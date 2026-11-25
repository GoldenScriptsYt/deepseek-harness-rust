//! Contract checks for the standalone Session query/search/export module.

use dsh_cli::{api, session_query_api};
use dsh_session_format::{Event, Header, HeaderTag, SESSION_FORMAT_VERSION};
use dsh_session_persistence::Store;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(label: &str) -> Self {
        for _ in 0..128 {
            let path = std::env::temp_dir().join(format!(
                "dsh-session-query-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create temporary test root: {error}"),
            }
        }
        panic!("could not allocate a unique temporary test root");
    }

    fn context(&self) -> std::sync::Arc<api::ApiContext> {
        api::ApiContext::new("http://127.0.0.1", "test", &self.0)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn event(seq: u64, time: u64, kind: &str, data: Value, surface: Option<Value>) -> Event {
    let mut extra = Map::new();
    extra.insert("seq".into(), json!(seq));
    extra.insert("time".into(), json!(time));
    if let Some(surface) = surface {
        extra.insert("surfaceOp".into(), surface);
    }
    Event {
        kind: kind.into(),
        data,
        extra,
    }
}

fn user_event(seq: u64, time: u64, text: &str) -> Event {
    event(
        seq,
        time,
        "user/message",
        json!({
            "source": {"kind": "user"},
            "content": [{"type": "text", "text": text}],
        }),
        Some(json!("append")),
    )
}

fn assistant_event(seq: u64, time: u64, text: &str) -> Event {
    event(
        seq,
        time,
        "assistant/message",
        json!({"message": {"role": "assistant", "content": [{"type": "text", "text": text}]}}),
        Some(json!("append")),
    )
}

fn add_session(
    root: &Path,
    id: &str,
    cwd: Option<&str>,
    created_at: u64,
    parent: Option<&str>,
    events: &[Event],
) -> PathBuf {
    let store = Store::new(root.join("sessions"));
    let mut session = store
        .create(Header {
            kind: HeaderTag::Session,
            version: SESSION_FORMAT_VERSION,
            id: id.into(),
            created_at,
            cwd: cwd.map(str::to_owned),
            parent_session: parent.map(str::to_owned),
            is_seeded: false,
            origin: parent.map(|_| dsh_session_format::Origin::Subagent),
            delegation_depth: u64::from(parent.is_some()),
            agent_preset: Some("standard".into()),
        })
        .unwrap();
    session.append(events).unwrap();
    let path = session.path().to_path_buf();
    drop(session);
    path
}

fn call(cx: &api::ApiContext, payload: Value) -> Value {
    session_query_api::list_sessions(cx, &payload).unwrap()
}

#[test]
fn list_uses_controller_metadata_and_omits_absent_cwd() {
    let temp = TempRoot::new("list");
    let cx = temp.context();
    add_session(
        &temp.0,
        "visible",
        Some("C:/work/a"),
        10,
        None,
        &[
            event(0, 11, "turn/start", json!({}), None),
            user_event(1, 100, "hello"),
            assistant_event(2, 101, "world"),
        ],
    );
    add_session(&temp.0, "legacy", None, 20, None, &[]);
    let value = call(&cx, json!({"args": {"request": {}}}));
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["sessionId"], "visible");
    assert_eq!(items[0]["updatedAt"], 100);
    assert_eq!(items[0]["blank"], false);
    assert_eq!(items[0]["formatStatus"], "current");
    assert_eq!(items[0]["running"], false);
    assert_eq!(items[0]["agentAvailable"], false);
    assert_eq!(items[0]["projections"]["kind"], "cached");
    assert_eq!(
        items[0]["projections"]["values"]["sessionListMetadata"]["lastPromptAt"],
        100
    );
    assert!(items[0].get("cwd").is_some());
    assert_eq!(value.as_object().unwrap().len(), 1);
}

#[test]
fn list_cwd_scope_is_additive_and_strict() {
    let temp = TempRoot::new("list-scope");
    let cx = temp.context();
    add_session(&temp.0, "a", Some("C:/work/a"), 1, None, &[]);
    add_session(&temp.0, "b", Some("C:/work/b"), 2, None, &[]);
    let value = session_query_api::list_sessions(&cx, &json!({"cwd": "C:/work/a"})).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 1);
    assert_eq!(value["items"][0]["sessionId"], "a");
    let error = session_query_api::list_sessions(&cx, &json!({"cwd": 4})).unwrap_err();
    assert_eq!(error.code, "gateway/bad-request");
}

#[test]
fn host_search_validation_matches_typescript() {
    let temp = TempRoot::new("host-validation");
    let cx = temp.context();
    for (query, message) in [
        ("", "session search query must not be empty"),
        ("   ", "session search query must not be empty"),
        ("a\0b", "session search query must not contain NUL"),
    ] {
        let error = session_query_api::search_sessions(&cx, &json!({"query": query})).unwrap_err();
        assert_eq!(error.code, "gateway/bad-request");
        assert_eq!(error.message, message);
    }
    let long = "😀".repeat(251);
    let error = session_query_api::search_sessions(&cx, &json!({"query": long})).unwrap_err();
    assert_eq!(
        error.message,
        "session search query must contain at most 500 UTF-16 code units"
    );
    let error = session_query_api::search_sessions(&cx, &json!({"query": 3})).unwrap_err();
    assert_eq!(error.code, "gateway/bad-request");
}

#[test]
fn host_search_filters_visibility_surface_and_duplicates() {
    let temp = TempRoot::new("host-search");
    let cx = temp.context();
    add_session(
        &temp.0,
        "current",
        Some("C:/work/a"),
        1,
        None,
        &[user_event(0, 1, "Needle answer")],
    );
    add_session(
        &temp.0,
        "hidden",
        None,
        2,
        None,
        &[user_event(0, 2, "Needle hidden")],
    );
    let mut replacement = event(
        1,
        4,
        "user/message",
        json!({"source": {"kind": "user"}, "content": [{"type": "text", "text": "replacement"}]}),
        Some(json!({"op": "replace", "startSeq": 0, "endSeq": 0})),
    );
    replacement
        .extra
        .insert("sourceEventSeqs".into(), json!([0]));
    add_session(
        &temp.0,
        "shadowed",
        Some("C:/work/a"),
        3,
        None,
        &[user_event(0, 3, "Needle old"), replacement],
    );
    let value = session_query_api::search_sessions(&cx, &json!({"query": "needle"})).unwrap();
    assert_eq!(
        value["items"],
        json!([{"sessionId": "current", "snippet": "Needle answer"}])
    );
    assert_eq!(value["hasMore"], false);
    let scoped =
        session_query_api::search_sessions(&cx, &json!({"query": "needle", "sessionId": "hidden"}))
            .unwrap();
    assert_eq!(scoped["items"], json!([]));

    add_session(
        &temp.0,
        "broken",
        Some("C:/work/a"),
        5,
        None,
        &[
            user_event(0, 5, "Needle old"),
            event(
                1,
                6,
                "user/message",
                json!({"source": {"kind": "user"}, "content": [{"type": "text", "text": "replacement"}]}),
                Some(json!({"op": "replace", "startSeq": 0, "endSeq": 0})),
            ),
        ],
    );
    let error = session_query_api::search_sessions(&cx, &json!({"query": "needle"})).unwrap_err();
    assert_eq!(error.code, "gateway/internal");
    assert_eq!(
        error.details["cause"]["code"],
        "SESSION_QUERY_INVALID_SURFACE"
    );
}

#[test]
fn host_search_has_twenty_one_lookahead_and_unicode_snippets() {
    let temp = TempRoot::new("host-page");
    let cx = temp.context();
    for index in 0..21 {
        add_session(
            &temp.0,
            &format!("s{index:02}"),
            Some("C:/work/a"),
            index,
            None,
            &[user_event(
                0,
                index,
                &format!("match {index} {}", "😀".repeat(300)),
            )],
        );
    }
    let value = session_query_api::search_sessions(&cx, &json!({"query": "match"})).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 20);
    assert_eq!(value["hasMore"], true);
    for item in value["items"].as_array().unwrap() {
        assert!(item["snippet"].as_str().unwrap().chars().count() <= 240);
    }
}

#[test]
fn query_paginates_with_bound_cursors_and_filters() {
    let temp = TempRoot::new("query-page");
    let cx = temp.context();
    for (id, cwd, created) in [("a", "C:/a", 1), ("b", "C:/a", 2), ("c", "C:/b", 3)] {
        add_session(
            &temp.0,
            id,
            Some(cwd),
            created,
            None,
            &[user_event(0, created, "common token")],
        );
    }
    let request = json!({
        "query": "common   token",
        "sessionFilters": [{"kind": "cwd", "values": ["C:/a"]}],
        "eventFilters": [{"kind": "surface", "values": ["current"]}],
        "limit": 1,
    });
    let first = session_query_api::query_sessions(&cx, &request).unwrap();
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert!(first.get("nextCursor").is_some());
    let cursor = first["nextCursor"].as_str().unwrap();
    let mut second_request = request.clone();
    second_request["cursor"] = json!(cursor);
    let second = session_query_api::query_sessions(&cx, &second_request).unwrap();
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert!(second.get("nextCursor").is_none());
    assert_eq!(first["items"][0]["bestMatch"]["surface"], "current");
    assert_eq!(first["items"][0]["live"], false);
    assert_eq!(first["items"][0]["persisted"], true);

    let mut mismatched = request;
    mismatched["cursor"] = json!(cursor);
    mismatched["query"] = json!("other");
    let error = session_query_api::query_sessions(&cx, &mismatched).unwrap_err();
    assert_eq!(error.code, "SESSION_QUERY_INVALID_CURSOR");
    let error =
        session_query_api::query_sessions(&cx, &json!({"query": "x", "limit": 101})).unwrap_err();
    assert_eq!(error.code, "SESSION_QUERY_INVALID_LIMIT");
    let error = session_query_api::query_sessions(
        &cx,
        &json!({"query": "x", "eventFilters": [{"kind": "text", "text": "x"}]}),
    )
    .unwrap_err();
    assert_eq!(error.code, "SESSION_QUERY_INVALID_FILTER");
}

#[test]
fn query_events_are_session_scoped_and_ranked() {
    let temp = TempRoot::new("query-events");
    let cx = temp.context();
    add_session(
        &temp.0,
        "a",
        Some("C:/a"),
        1,
        None,
        &[
            user_event(0, 10, "alpha beta alpha"),
            assistant_event(1, 11, "alpha"),
            event(
                2,
                12,
                "turn/end",
                json!({"reason": {"kind": "completed"}}),
                None,
            ),
        ],
    );
    add_session(
        &temp.0,
        "b",
        Some("C:/b"),
        2,
        None,
        &[user_event(0, 20, "alpha")],
    );
    let value = session_query_api::query_events(
        &cx,
        &json!({
            "sessionId": "a",
            "query": "alpha",
            "filters": [{"kind": "type", "values": ["user/message", "assistant/message"]}],
            "limit": 10,
        }),
    )
    .unwrap();
    assert_eq!(value["session"]["id"], "a");
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["seq"], 0);
    assert_eq!(items[1]["seq"], 1);
    assert!(items.iter().all(|item| item["sessionId"] == "a"));
}

#[test]
fn corrupt_logs_fail_loud_instead_of_disappearing() {
    let temp = TempRoot::new("corrupt");
    let cx = temp.context();
    let path = add_session(
        &temp.0,
        "bad",
        Some("C:/a"),
        1,
        None,
        &[user_event(0, 1, "needle")],
    );
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(b"not-json\n");
    fs::write(&path, bytes).unwrap();
    let error = session_query_api::query_sessions(&cx, &json!({"query": "needle"})).unwrap_err();
    assert_eq!(error.code, "SESSION_QUERY_CORRUPT_SESSION");
    assert_eq!(error.details["kind"], "corrupt");
    let error = session_query_api::search_sessions(&cx, &json!({"query": "needle"})).unwrap_err();
    assert_eq!(error.code, "gateway/internal");
    assert_eq!(
        error.details["cause"]["code"],
        "SESSION_QUERY_CORRUPT_SESSION"
    );
    let error = session_query_api::list_sessions(&cx, &json!({})).unwrap_err();
    assert_eq!(error.code, "SESSION_QUERY_CORRUPT_SESSION");
}

#[test]
fn torn_final_row_is_ignored_but_committed_bad_row_is_not() {
    let temp = TempRoot::new("torn");
    let cx = temp.context();
    let path = add_session(
        &temp.0,
        "torn",
        Some("C:/a"),
        1,
        None,
        &[user_event(0, 1, "needle")],
    );
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(b"{\"type\":\"turn/end\"");
    fs::write(&path, bytes).unwrap();
    let value = session_query_api::search_sessions(&cx, &json!({"query": "needle"})).unwrap();
    assert_eq!(value["items"][0]["sessionId"], "torn");
}

fn decode_base64(value: &str) -> Vec<u8> {
    fn digit(byte: u8) -> u8 {
        match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("invalid base64"),
        }
    }
    let bytes = value.as_bytes();
    let mut output = Vec::new();
    for chunk in bytes.chunks(4) {
        let a = digit(chunk[0]);
        let b = digit(chunk[1]);
        let c = if chunk.get(2).is_some_and(|byte| *byte != b'=') {
            digit(chunk[2])
        } else {
            0
        };
        let d = if chunk.get(3).is_some_and(|byte| *byte != b'=') {
            digit(chunk[3])
        } else {
            0
        };
        output.push((a << 2) | (b >> 4));
        if chunk.get(2).is_some_and(|byte| *byte != b'=') {
            output.push((b << 4) | (c >> 2));
        }
        if chunk.get(3).is_some_and(|byte| *byte != b'=') {
            output.push((c << 6) | d);
        }
    }
    output
}

fn stored_zip_entries(bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let mut output = BTreeMap::new();
    let mut offset = 0usize;
    while u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) == 0x04034b50 {
        let method = u16::from_le_bytes(bytes[offset + 8..offset + 10].try_into().unwrap());
        assert_eq!(method, 0);
        let size = u32::from_le_bytes(bytes[offset + 18..offset + 22].try_into().unwrap()) as usize;
        let name_size =
            u16::from_le_bytes(bytes[offset + 26..offset + 28].try_into().unwrap()) as usize;
        let extra_size =
            u16::from_le_bytes(bytes[offset + 28..offset + 30].try_into().unwrap()) as usize;
        let name = String::from_utf8(bytes[offset + 30..offset + 30 + name_size].to_vec()).unwrap();
        let start = offset + 30 + name_size + extra_size;
        output.insert(name, bytes[start..start + size].to_vec());
        offset = start + size;
    }
    assert_eq!(
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()),
        0x02014b50
    );
    output
}

#[test]
fn export_is_a_valid_stored_zip_with_lineage_and_missing_attachments() {
    let temp = TempRoot::new("export");
    let cx = temp.context();
    add_session(
        &temp.0,
        "root/a",
        Some("C:/a"),
        10,
        None,
        &[user_event(0, 11, "root [Image: x (1×1, 2 bytes)]")],
    );
    add_session(
        &temp.0,
        "child",
        Some("C:/a"),
        20,
        Some("root/a"),
        &[assistant_event(0, 21, "child")],
    );
    add_session(
        &temp.0,
        "grand",
        Some("C:/a"),
        30,
        Some("child"),
        &[assistant_event(0, 31, "grand")],
    );
    let value = session_query_api::export_session(
        &cx,
        &json!({
            "sessionId": "root/a",
            "includeDescendants": "true",
        }),
    )
    .unwrap();
    assert_eq!(value["filename"], "dsh-session-root_a.zip");
    assert_eq!(value["contentType"], "application/zip");
    assert_eq!(value["compression"], "stored");
    let entries = stored_zip_entries(&decode_base64(value["data"].as_str().unwrap()));
    assert!(entries.contains_key("session.v4.jsonl"));
    assert!(entries.contains_key("subagents/child/session.v4.jsonl"));
    assert!(entries.contains_key("subagents/grand/session.v4.jsonl"));
    assert!(String::from_utf8(entries["session.v4.jsonl"].clone())
        .unwrap()
        .ends_with('\n'));
    let error = session_query_api::export_session(
        &cx,
        &json!({"sessionId": "root/a", "includeDescendants": "yes"}),
    )
    .unwrap_err();
    assert_eq!(error.code, "gateway/bad-request");
    let error =
        session_query_api::export_session(&cx, &json!({"sessionId": "missing"})).unwrap_err();
    assert_eq!(error.code, "SESSION_QUERY_SESSION_NOT_FOUND");
}

#[test]
fn export_reports_attachment_references_it_cannot_materialize() {
    let temp = TempRoot::new("attachments");
    let cx = temp.context();
    add_session(
        &temp.0,
        "media",
        Some("C:/a"),
        1,
        None,
        &[event(
            0,
            1,
            "user/message",
            json!({
                "source": {"kind": "user"},
                "content": [{"type": "image", "attachment": {
                    "attachmentId": "img-1", "mediaType": "image/png", "name": "x.png",
                    "width": 1, "height": 1, "bytes": 2,
                }}],
            }),
            Some(json!("append")),
        )],
    );
    let value = session_query_api::export_session(&cx, &json!({"sessionId": "media"})).unwrap();
    let entries = stored_zip_entries(&decode_base64(value["data"].as_str().unwrap()));
    let missing = String::from_utf8(entries["attachments-missing.txt"].clone()).unwrap();
    assert!(missing.contains("media/img-1.png\tATTACHMENT_NOT_FOUND"));
    assert!(!entries.contains_key("media/img-1.png"));
}

#[test]
fn markdown_matches_title_turns_tools_and_final_reply_shape() {
    let temp = TempRoot::new("markdown");
    let cx = temp.context();
    add_session(
        &temp.0,
        "md",
        Some("C:/a"),
        1,
        None,
        &[
            event(0, 1, "session/title", json!({"title": "My\nTitle"}), None),
            event(1, 2, "turn/start", json!({}), None),
            user_event(2, 3, "hello\n\nworld"),
            assistant_event(3, 4, "earlier"),
            assistant_event(4, 5, "final\n```rust\nunfinished"),
            event(
                5,
                6,
                "turn/end",
                json!({"reason": {"kind": "completed"}}),
                None,
            ),
        ],
    );
    let value =
        session_query_api::export_markdown(&cx, &json!({"sessionId": "md", "sourcePath": ""}))
            .unwrap();
    assert_eq!(value["contentType"], "text/markdown; charset=utf-8");
    let content = value["content"].as_str().unwrap();
    assert!(content.starts_with("# My Title\n\n> hello\n>\n> world\n\n"));
    assert!(content.contains("<details><summary>1 previous message</summary>"));
    assert!(content.contains("**Assistant**\n\nearlier"));
    assert!(content.ends_with("final\n```rust\nunfinished\n```\n"));
    assert!(content.ends_with('\n') && !content.ends_with("\n\n"));
}
