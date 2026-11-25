// Workspace registry and read-only workspace file RPCs.
//
// The TypeScript controllers are the wire contract. Registry data extends the
// existing `rust-workspaces.json` shape so older CLI sessions remain readable.

use crate::api::{self, ApiContext};
use dsh_gateway::{Broadcaster, Gateway, RpcError};
#[cfg(test)]
use dsh_session_format::{Header, HeaderTag, SESSION_FORMAT_VERSION};
use dsh_session_persistence::Store;
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::collections::{hash_map::DefaultHasher, BinaryHeap, HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_LINES: usize = 5000;
const MAX_ENTRIES: usize = 2000;
const MAX_RESULTS: usize = 512;
const MAX_SEARCH_ENTRIES: usize = 100_000;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(30);
const WATCH_POLL: Duration = Duration::from_millis(100);
const VCS_NAMES: [&str; 6] = [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];

struct Core {
    home: PathBuf,
    feed: Broadcaster,
    mutation: Mutex<()>,
}

impl Core {
    fn file(&self) -> PathBuf {
        self.home.join("storages").join("rust-workspaces.json")
    }

    fn sessions(&self) -> Vec<SessionInfo> {
        Store::mixed(self.home.join("sessions"))
            .read_all()
            .into_iter()
            .map(|stored| SessionInfo {
                id: stored.header.id,
                cwd: stored.header.cwd,
                created_at: stored.header.created_at,
            })
            .collect()
    }

    fn publish(&self, frames: Vec<Value>) {
        for frame in frames {
            self.feed.publish(frame);
        }
    }
}

#[derive(Clone)]
struct SessionInfo {
    id: String,
    cwd: Option<String>,
    created_at: u64,
}

/// Register after `api::register_all`; workspace follow and session creation are
/// deliberately wrapped so the two controllers share one durable registry.
pub fn register_all(gw: &Gateway, cx: Arc<ApiContext>) {
    let core = Arc::new(Core {
        home: cx.home.clone(),
        feed: gw.broadcaster("workspace/follow"),
        mutation: Mutex::new(()),
    });

    let c = core.clone();
    let create_cx = cx.clone();
    gw.rpc("workspace/create", move |v| {
        let c = c.clone();
        let cx = create_cx.clone();
        async move {
            tokio::task::spawn_blocking(move || create_workspace(&c, &cx, &v))
                .await
                .map_err(|_| rpc_error("gateway/bad-request", "workspace task failed"))?
        }
    });
    let c = core.clone();
    let default_cx = cx.clone();
    gw.rpc("workspace/initializeDefault", move |v| {
        let c = c.clone();
        let cx = default_cx.clone();
        async move {
            tokio::task::spawn_blocking(move || initialize_default(&c, &cx, &v))
                .await
                .map_err(|_| rpc_error("gateway/bad-request", "workspace task failed"))?
        }
    });
    let c = core.clone();
    gw.rpc("workspace/rename", move |v| {
        let c = c.clone();
        async move { rename_workspace(&c, &v) }
    });
    let c = core.clone();
    gw.rpc("workspace/delete", move |v| {
        let c = c.clone();
        async move { delete_workspace(&c, &v) }
    });
    let c = core.clone();
    gw.rpc("workspace/insertBefore", move |v| {
        let c = c.clone();
        async move { insert_workspace_before(&c, &v) }
    });
    let c = core.clone();
    gw.rpc("workspace/insertSessionBefore", move |v| {
        let c = c.clone();
        async move { insert_session_before(&c, &v) }
    });
    let c = core.clone();
    let archive_cx = cx.clone();
    gw.rpc("workspace/archiveSession", move |v| {
        let c = c.clone();
        let cx = archive_cx.clone();
        async move { archive_session(&c, &cx, &v) }
    });
    let c = core.clone();
    gw.rpc("workspace/unarchiveSession", move |v| {
        let c = c.clone();
        async move { unarchive_session(&c, &v) }
    });
    let c = core.clone();
    gw.rpc("workspace/pinSession", move |v| {
        let c = c.clone();
        async move { pin_session(&c, &v) }
    });
    let c = core.clone();
    gw.rpc("workspace/unpinSession", move |v| {
        let c = c.clone();
        async move { unpin_session(&c, &v) }
    });
    let c = core.clone();
    gw.stream("workspace/follow", move |v, _| {
        follow_workspaces(c.clone(), &v)
    });

    let c = core.clone();
    let session_cx = cx.clone();
    gw.rpc("session/create", move |v| {
        let c = c.clone();
        let cx = session_cx.clone();
        async move {
            let result = api::create_session(&cx, &v)?;
            if let Some(session_id) = result.get("sessionId").and_then(Value::as_str) {
                match attach_created_session(&c, session_id) {
                    Ok(frames) => c.publish(frames),
                    Err(error) => eprintln!("[dsh-rs] workspace session attach failed: {error:?}"),
                }
            }
            Ok(result)
        }
    });

    macro_rules! file_rpc {
        ($name:literal, $handler:ident) => {{
            let c = core.clone();
            let cx = cx.clone();
            gw.rpc($name, move |v| {
                let c = c.clone();
                let cx = cx.clone();
                async move {
                    tokio::task::spawn_blocking(move || $handler(&c, &cx, &v))
                        .await
                        .map_err(|_| {
                            rpc_error("gateway/bad-request", "workspace file task failed")
                        })?
                }
            });
        }};
    }
    file_rpc!("workspaceFiles/read", file_read);
    file_rpc!("workspaceFiles/readBytes", file_read_bytes);
    file_rpc!("workspaceFiles/stat", file_stat);
    file_rpc!("workspaceFiles/list", file_list);
    file_rpc!("workspaceFiles/search", file_search);

    let c = core.clone();
    gw.stream("workspaceFiles/changes", move |v, _| {
        file_changes(c.clone(), &v)
    });
}

fn rpc_error(code: &str, message: impl Into<String>) -> RpcError {
    RpcError::new(code, message)
}

fn detailed_error(code: &str, message: impl Into<String>, details: Value) -> RpcError {
    let mut error = rpc_error(code, message);
    error.details = details;
    error
}

fn bad_request(message: impl Into<String>) -> RpcError {
    detailed_error("gateway/bad-request", message, json!({}))
}

fn request(payload: &Value) -> &Value {
    payload.get("args").unwrap_or(payload)
}

fn workspace_request(payload: &Value) -> &Value {
    let args = request(payload);
    args.get("request").unwrap_or(args)
}

fn string_field<'a>(value: &'a Value, name: &str) -> Result<&'a str, RpcError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad_request(format!("{name} must not be empty")))
}

fn optional_string<'a>(value: &'a Value, name: &str) -> Result<Option<&'a str>, RpcError> {
    match value.get(name) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(bad_request(format!("{name} must be a string"))),
    }
}

fn optional_bool(value: &Value, name: &str, default: bool) -> Result<bool, RpcError> {
    match value.get(name) {
        None => Ok(default),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(bad_request(format!("{name} must be a boolean"))),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn iso8601(ms: u64) -> String {
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doe - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms % 1000
    )
}

fn random_uuid() -> String {
    let mut hex = String::new();
    for _ in 0..2 {
        let mut hash = std::collections::hash_map::RandomState::new().build_hasher();
        hash.write_u128(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        hex.push_str(&format!("{:016x}", hash.finish()));
    }
    format!(
        "{}-{}-4{}-a{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[13..16],
        &hex[17..20],
        &hex[20..32]
    )
}

pub(crate) fn display_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    if text
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(r"\\?\UNC\"))
    {
        format!(r"\\{}", &text[8..])
    } else {
        text.strip_prefix(r"\\?\").unwrap_or(&text).to_string()
    }
}

#[cfg(windows)]
fn same_path(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(not(windows))]
fn same_path(left: &str, right: &str) -> bool {
    left == right
}

fn canonical_directory(path: &Path) -> Result<PathBuf, RpcError> {
    if !path.is_absolute() || path.as_os_str().to_string_lossy().contains('\0') {
        return Err(detailed_error(
            "workspace/invalid-path",
            "Workspace path must be fully qualified",
            json!({ "path": display_path(path) }),
        ));
    }
    let canonical = fs::canonicalize(path).map_err(|error| {
        detailed_error(
            "workspace/invalid-path",
            format!(
                "cannot create a Workspace at \"{}\": {error}",
                display_path(path)
            ),
            json!({ "path": display_path(path) }),
        )
    })?;
    if !canonical.is_dir() {
        return Err(detailed_error(
            "workspace/invalid-path",
            "Workspace path is not a directory",
            json!({ "path": display_path(path) }),
        ));
    }
    Ok(canonical)
}

fn workspace_title(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| display_path(path))
}

fn empty_registry() -> Value {
    json!({
        "workspaces": [],
        "workspaceIds": [],
        "archivedSessionIds": [],
        "pinnedSessionIds": [],
        "initialized": false
    })
}

fn read_document(core: &Core) -> Result<Value, RpcError> {
    let path = core.file();
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(empty_registry());
        }
        Err(error) => return Err(rpc_error("workspace/io", error.to_string())),
    };
    let mut document: Value = serde_json::from_str(&text).map_err(|error| {
        rpc_error(
            "workspace/io",
            format!("invalid workspace registry: {error}"),
        )
    })?;
    let object = document
        .as_object_mut()
        .ok_or_else(|| rpc_error("workspace/io", "workspace registry must be an object"))?;
    for field in [
        "workspaces",
        "workspaceIds",
        "archivedSessionIds",
        "pinnedSessionIds",
    ] {
        if !object.contains_key(field) {
            object.insert(field.to_string(), json!([]));
        }
        let values = object.get(field).and_then(Value::as_array).ok_or_else(|| {
            rpc_error(
                "workspace/io",
                format!("workspace registry {field} must be an array"),
            )
        })?;
        if field != "workspaces" && values.iter().any(|value| !value.is_string()) {
            return Err(rpc_error(
                "workspace/io",
                format!("workspace registry {field} must contain strings"),
            ));
        }
    }
    let workspaces = object.get("workspaces").and_then(Value::as_array).unwrap();
    for record in workspaces {
        for field in ["workspaceId", "path", "title", "createdAt", "updatedAt"] {
            if record.get(field).and_then(Value::as_str).is_none() {
                return Err(rpc_error(
                    "workspace/io",
                    format!("workspace record is missing string field {field}"),
                ));
            }
        }
        let ids: &[Value] = record
            .get("sessionIds")
            .map(|value| {
                value.as_array().ok_or_else(|| {
                    rpc_error("workspace/io", "workspace sessionIds must be an array")
                })
            })
            .transpose()?
            .map_or(&[][..], |ids| ids.as_slice());
        if ids.iter().any(|value| !value.is_string()) {
            return Err(rpc_error(
                "workspace/io",
                "workspace sessionIds must contain strings",
            ));
        }
    }
    if object
        .get("initialized")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(rpc_error(
            "workspace/io",
            "workspace registry initialized must be boolean",
        ));
    }
    if object
        .get("defaultWorkspaceId")
        .is_some_and(|value| !value.is_string())
    {
        return Err(rpc_error(
            "workspace/io",
            "workspace registry defaultWorkspaceId must be a string",
        ));
    }
    Ok(document)
}

fn atomic_write(path: &Path, document: &Value) -> Result<(), RpcError> {
    let parent = path
        .parent()
        .ok_or_else(|| rpc_error("workspace/io", "invalid registry path"))?;
    fs::create_dir_all(parent).map_err(|e| rpc_error("workspace/io", e.to_string()))?;
    let nonce = format!("{}-{}", std::process::id(), now_ms());
    let temp = parent.join(format!(".rust-workspaces.{nonce}.tmp"));
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(document)?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(|error| rpc_error("workspace/io", error.to_string()))
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn records(document: &Value) -> Vec<Value> {
    document["workspaces"]
        .as_array()
        .into_iter()
        .flatten()
        .cloned()
        .collect()
}

fn record_index(document: &Value, id: &str) -> Option<usize> {
    records(document)
        .iter()
        .position(|record| record["workspaceId"].as_str() == Some(id))
}

fn ordered_records(document: &Value) -> Vec<Value> {
    let all = records(document);
    let mut used = HashSet::new();
    let mut ordered = Vec::new();
    for id in string_array(document.get("workspaceIds")) {
        if let Some(record) = all.iter().find(|r| r["workspaceId"].as_str() == Some(&id)) {
            if used.insert(id) {
                ordered.push(record.clone());
            }
        }
    }
    for record in all {
        if let Some(id) = record["workspaceId"].as_str() {
            if used.insert(id.to_string()) {
                ordered.push(record);
            }
        }
    }
    ordered
}

fn workspace_ids(document: &Value) -> Vec<String> {
    ordered_records(document)
        .iter()
        .filter_map(|record| record["workspaceId"].as_str().map(str::to_string))
        .collect()
}

fn session_index(core: &Core) -> HashMap<String, SessionInfo> {
    core.sessions()
        .into_iter()
        .map(|session| (session.id.clone(), session))
        .collect()
}

fn member_is_valid(record: &Value, session: &SessionInfo) -> bool {
    let Some(cwd) = session.cwd.as_deref() else {
        return false;
    };
    match fs::canonicalize(cwd) {
        Ok(path) => same_path(&display_path(&path), record["path"].as_str().unwrap_or("")),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && Path::new(cwd).is_absolute() =>
        {
            true
        }
        Err(_) => false,
    }
}

fn workspace_view(core: &Core, record: &Value) -> Value {
    let sessions = session_index(core);
    let ids: Vec<String> = string_array(record.get("sessionIds"))
        .into_iter()
        .filter(|id| {
            sessions
                .get(id)
                .is_some_and(|session| member_is_valid(record, session))
        })
        .collect();
    json!({
        "workspaceId": record["workspaceId"],
        "path": record["path"],
        "title": record["title"],
        "sessionIds": ids,
        "createdAt": record["createdAt"],
        "updatedAt": record["updatedAt"]
    })
}

fn baseline(core: &Core, document: &Value) -> Value {
    json!({
        "type": "baseline",
        "value": {
            "items": ordered_records(document).iter().map(|record| workspace_view(core, record)).collect::<Vec<_>>(),
            "archivedSessionIds": string_array(document.get("archivedSessionIds")),
            "pinnedSessionIds": string_array(document.get("pinnedSessionIds"))
        }
    })
}

fn synchronize_document(document: &mut Value, core: &Core) {
    let sessions = session_index(core);
    let ordered = ordered_records(document);
    let mut stored = Vec::new();
    for mut record in ordered {
        let ids: Vec<String> = string_array(record.get("sessionIds"))
            .into_iter()
            .filter(|id| {
                sessions
                    .get(id)
                    .is_some_and(|session| member_is_valid(&record, session))
            })
            .collect();
        record["sessionIds"] = json!(ids);
        stored.push(record);
    }
    document["workspaceIds"] = json!(stored
        .iter()
        .filter_map(|r| r["workspaceId"].as_str())
        .collect::<Vec<_>>());
    document["workspaces"] = json!(stored);
}

fn bootstrap_document(core: &Core, mut document: Value) -> Result<Value, RpcError> {
    if document.get("initialized").and_then(Value::as_bool) == Some(true) {
        synchronize_document(&mut document, core);
        return Ok(document);
    }
    let sessions = core.sessions();
    let mut existing = records(&document);
    if existing.is_empty() {
        let mut groups: HashMap<String, (u64, PathBuf)> = HashMap::new();
        for session in &sessions {
            let Some(cwd) = session.cwd.as_deref() else {
                continue;
            };
            let Ok(path) = fs::canonicalize(cwd) else {
                continue;
            };
            if !path.is_dir() {
                continue;
            }
            let key = display_path(&path);
            groups
                .entry(key)
                .and_modify(|(newest, _)| *newest = (*newest).max(session.created_at))
                .or_insert_with(|| (session.created_at, path));
        }
        let mut grouped: Vec<_> = groups.into_iter().collect();
        grouped.sort_by(
            |(left_path, (left_newest, _)), (right_path, (right_newest, _))| {
                right_newest
                    .cmp(left_newest)
                    .then_with(|| left_path.cmp(right_path))
            },
        );
        for (key, (_, path)) in grouped {
            let now = iso8601(now_ms());
            existing.push(json!({
                "workspaceId": random_uuid(),
                "path": key,
                "title": workspace_title(&path),
                "sessionIds": [],
                "createdAt": now,
                "updatedAt": now
            }));
        }
    }
    document["workspaces"] = json!(existing);
    document["workspaceIds"] = json!(records(&document)
        .iter()
        .filter_map(|r| r["workspaceId"].as_str())
        .collect::<Vec<_>>());
    if document.get("archivedSessionIds").is_none() {
        document["archivedSessionIds"] = json!([]);
    }
    if document.get("pinnedSessionIds").is_none() {
        document["pinnedSessionIds"] = json!([]);
    }
    for record in records(&document) {
        let path = record["path"].as_str().unwrap_or("");
        let mut ids: Vec<&SessionInfo> = sessions
            .iter()
            .filter(|s| {
                s.cwd
                    .as_deref()
                    .and_then(|p| fs::canonicalize(p).ok())
                    .is_some_and(|p| same_path(&display_path(&p), path))
            })
            .collect();
        ids.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        if let Some(index) = record_index(&document, record["workspaceId"].as_str().unwrap_or("")) {
            document["workspaces"][index]["sessionIds"] =
                json!(ids.iter().map(|s| s.id.clone()).collect::<Vec<_>>());
        }
    }
    document["initialized"] = json!(true);
    synchronize_document(&mut document, core);
    Ok(document)
}

fn load(core: &Core) -> Result<Value, RpcError> {
    bootstrap_document(core, read_document(core)?)
}

fn save(core: &Core, document: &Value) -> Result<(), RpcError> {
    atomic_write(&core.file(), document)
}

fn require_workspace(document: &Value, id: &str) -> Result<usize, RpcError> {
    record_index(document, id).ok_or_else(|| {
        detailed_error(
            "workspace/not-found",
            format!("Workspace \"{id}\" not found"),
            json!({ "workspaceId": id }),
        )
    })
}

fn create_workspace(core: &Core, _cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let path = string_field(workspace_request(payload), "path")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = bootstrap_document(core, read_document(core)?)?;
    let canonical = canonical_directory(Path::new(path))?;
    let canonical = display_path(&canonical);
    if let Some(record) = records(&document)
        .into_iter()
        .find(|r| same_path(r["path"].as_str().unwrap_or(""), &canonical))
    {
        return Ok(json!({ "workspace": workspace_view(core, &record), "created": false }));
    }
    let now = iso8601(now_ms());
    let old_ids = workspace_ids(&document);
    let record = json!({
        "workspaceId": random_uuid(),
        "path": canonical,
        "title": workspace_title(Path::new(&canonical)),
        "sessionIds": [],
        "createdAt": now,
        "updatedAt": now
    });
    document["workspaces"]
        .as_array_mut()
        .unwrap()
        .insert(0, record.clone());
    let mut ids = vec![record["workspaceId"].as_str().unwrap().to_string()];
    ids.extend(old_ids);
    document["workspaceIds"] = json!(ids);
    synchronize_document(&mut document, core);
    save(core, &document)?;
    drop(_guard);
    core.publish(vec![
        json!({ "type": "upsert", "workspace": record }),
        json!({ "type": "order", "workspaceIds": workspace_ids(&document) }),
    ]);
    Ok(json!({ "workspace": record, "created": true }))
}

fn initialize_default(core: &Core, _cx: &ApiContext, _payload: &Value) -> Result<Value, RpcError> {
    let _guard = core.mutation.lock().unwrap();
    let mut document = bootstrap_document(core, read_document(core)?)?;
    if let Some(id) = document["defaultWorkspaceId"].as_str() {
        return match record_index(&document, id) {
            Some(index) => Ok(json!({
                "workspace": workspace_view(core, &document["workspaces"][index])
            })),
            None => Ok(Value::Null),
        };
    }
    if !workspace_ids(&document).is_empty()
        || !string_array(document.get("archivedSessionIds")).is_empty()
        || !core.sessions().is_empty()
    {
        return Ok(Value::Null);
    }
    let requested = default_workspace_path(&core.home)?;
    fs::create_dir_all(&requested).map_err(|error| rpc_error("workspace/io", error.to_string()))?;
    let canonical = canonical_directory(&requested)?;
    let now = iso8601(now_ms());
    let record = json!({
        "workspaceId": random_uuid(),
        "path": display_path(&canonical),
        "title": workspace_title(&requested),
        "sessionIds": [],
        "createdAt": now,
        "updatedAt": now
    });
    document["workspaces"] = json!([record.clone()]);
    document["workspaceIds"] = json!([record["workspaceId"]]);
    document["defaultWorkspaceId"] = record["workspaceId"].clone();
    save(core, &document)?;
    drop(_guard);
    core.publish(vec![
        json!({ "type": "upsert", "workspace": record.clone() }),
        json!({ "type": "order", "workspaceIds": [record["workspaceId"]] }),
    ]);
    Ok(json!({ "workspace": record }))
}

fn default_workspace_path(_home: &Path) -> Result<PathBuf, RpcError> {
    let unavailable = || rpc_error("workspace/io", "system Documents directory is unavailable");
    let configured = std::env::var_os("DSH_DOCUMENTS_DIRECTORY");
    let directory = if let Some(directory) = configured {
        PathBuf::from(directory)
    } else {
        #[cfg(windows)]
        let output = native_command_output(
            "powershell.exe",
            &[
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); [Environment]::GetFolderPath([Environment+SpecialFolder]::MyDocuments, [Environment+SpecialFolderOption]::DoNotVerify)",
            ],
        )?;
        #[cfg(target_os = "macos")]
        let output = native_command_output(
            "osascript",
            &[
                "-e",
                "POSIX path of (path to documents folder from user domain without folder creation)",
            ],
        )?;
        #[cfg(target_os = "linux")]
        let output = native_command_output("xdg-user-dir", &["DOCUMENTS"])?;
        #[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
        return Err(unavailable());
        let text = String::from_utf8(output).map_err(|_| unavailable())?;
        let text = text.trim_end_matches(['\r', '\n']);
        if text.is_empty() {
            return Err(unavailable());
        }
        #[cfg(target_os = "linux")]
        if Path::new(text).components().eq(_home.components()) {
            return Err(unavailable());
        }
        PathBuf::from(text)
    };
    if !directory.is_absolute() || directory.as_os_str().to_string_lossy().contains('\0') {
        return Err(rpc_error(
            "workspace/io",
            "Documents directory must be fully qualified",
        ));
    }
    Ok(directory.join("deepseek-harness").join("default-workspace"))
}

fn native_command_output(program: &str, arguments: &[&str]) -> Result<Vec<u8>, RpcError> {
    let mut child = Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| rpc_error("workspace/io", error.to_string()))?;
    let stdout = child.stdout.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let mut file = stdout;
        while let Ok(count) = file.read(&mut buffer) {
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
        bytes
    });
    let start = SystemTime::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if start.elapsed().unwrap_or_default() >= Duration::from_secs(5) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = reader.join();
                return Err(rpc_error(
                    "workspace/io",
                    "Documents directory lookup timed out",
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = reader.join();
                return Err(rpc_error("workspace/io", error.to_string()));
            }
        }
    };
    let output = reader.join().unwrap_or_default();
    if !status.success() {
        return Err(rpc_error(
            "workspace/io",
            "Documents directory lookup failed",
        ));
    }
    Ok(output)
}

fn rename_workspace(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let request = workspace_request(payload);
    let id = string_field(request, "workspaceId")?;
    let title = string_field(request, "title")?.trim();
    if title.is_empty() {
        return Err(bad_request("Workspace rename requires a non-blank title"));
    }
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let index = require_workspace(&document, id)?;
    let mut changed = false;
    if document["workspaces"][index]["title"].as_str() != Some(title) {
        if records(&document).iter().any(|record| {
            record["workspaceId"].as_str() != Some(id) && record["title"].as_str() == Some(title)
        }) {
            return Err(detailed_error(
                "workspace/name-conflict",
                format!("Workspace name '{title}' is already in use"),
                json!({ "name": title }),
            ));
        }
        document["workspaces"][index]["title"] = json!(title);
        document["workspaces"][index]["updatedAt"] = json!(iso8601(now_ms()));
        synchronize_document(&mut document, core);
        save(core, &document)?;
        changed = true;
    }
    let view = workspace_view(core, &document["workspaces"][index]);
    drop(_guard);
    if changed {
        core.publish(vec![json!({ "type": "upsert", "workspace": view.clone() })]);
    }
    Ok(json!({ "workspace": view }))
}

fn delete_workspace(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let id = string_field(workspace_request(payload), "workspaceId")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let index = require_workspace(&document, id)?;
    document["workspaces"].as_array_mut().unwrap().remove(index);
    synchronize_document(&mut document, core);
    let ids = workspace_ids(&document);
    document["workspaceIds"] = json!(ids);
    save(core, &document)?;
    drop(_guard);
    core.publish(vec![
        json!({ "type": "order", "workspaceIds": ids }),
        json!({ "type": "remove", "workspaceId": id }),
    ]);
    Ok(json!({ "deleted": true }))
}

fn insert_workspace_before(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let request = workspace_request(payload);
    let id = string_field(request, "workspaceId")?;
    let before = optional_string(request, "beforeWorkspaceId")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let mut ids = workspace_ids(&document);
    if !ids.iter().any(|candidate| candidate == id)
        || before.is_some_and(|anchor| !ids.iter().any(|candidate| candidate == anchor))
    {
        let missing = if !ids.iter().any(|candidate| candidate == id) {
            id
        } else {
            before.unwrap()
        };
        return Err(detailed_error(
            "workspace/not-found",
            format!("Workspace \"{missing}\" not found"),
            json!({ "workspaceId": missing }),
        ));
    }
    let mut changed = false;
    if before != Some(id) {
        let old_ids = ids.clone();
        ids.retain(|candidate| candidate != id);
        let at = before
            .map(|anchor| {
                ids.iter()
                    .position(|candidate| candidate == anchor)
                    .unwrap()
            })
            .unwrap_or(ids.len());
        ids.insert(at, id.to_string());
        if ids != old_ids {
            document["workspaceIds"] = json!(ids);
            synchronize_document(&mut document, core);
            save(core, &document)?;
            changed = true;
        }
    }
    drop(_guard);
    if changed {
        core.publish(vec![json!({ "type": "order", "workspaceIds": ids })]);
    }
    Ok(json!({ "workspaceIds": ids }))
}

fn insert_session_before(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let request = workspace_request(payload);
    let id = string_field(request, "workspaceId")?;
    let session_id = string_field(request, "sessionId")?;
    let before = optional_string(request, "beforeSessionId")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let index = require_workspace(&document, id)?;
    let view = workspace_view(core, &document["workspaces"][index]);
    let mut ids = string_array(view.get("sessionIds"));
    let mut details = json!({ "workspaceId": id, "sessionId": session_id });
    if let Some(before) = before {
        details["beforeSessionId"] = json!(before);
    }
    if !ids.iter().any(|candidate| candidate == session_id)
        || before.is_some_and(|anchor| !ids.iter().any(|candidate| candidate == anchor))
    {
        return Err(detailed_error(
            "workspace/move-invalid",
            "Session cannot be moved in this Workspace",
            details,
        ));
    }
    let mut changed = false;
    if before != Some(session_id) {
        let old_ids = ids.clone();
        ids.retain(|candidate| candidate != session_id);
        let at = before
            .map(|anchor| {
                ids.iter()
                    .position(|candidate| candidate == anchor)
                    .unwrap()
            })
            .unwrap_or(ids.len());
        ids.insert(at, session_id.to_string());
        if ids != old_ids {
            document["workspaces"][index]["sessionIds"] = json!(ids);
            document["workspaces"][index]["updatedAt"] = json!(iso8601(now_ms()));
            save(core, &document)?;
            changed = true;
        }
    }
    let view = workspace_view(core, &document["workspaces"][index]);
    drop(_guard);
    if changed {
        core.publish(vec![json!({ "type": "upsert", "workspace": view.clone() })]);
    }
    Ok(json!({ "workspace": view }))
}

fn attach_created_session(core: &Core, session_id: &str) -> Result<Vec<Value>, RpcError> {
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let sessions = session_index(core);
    let Some(session) = sessions.get(session_id) else {
        return Ok(Vec::new());
    };
    let Some(cwd) = session.cwd.as_deref() else {
        return Ok(Vec::new());
    };
    let Ok(path) = fs::canonicalize(cwd) else {
        return Ok(Vec::new());
    };
    let path = display_path(&path);
    let Some(index) = records(&document)
        .iter()
        .position(|record| same_path(record["path"].as_str().unwrap_or(""), &path))
    else {
        return Ok(Vec::new());
    };
    let mut ids = string_array(document["workspaces"][index].get("sessionIds"));
    if ids.iter().any(|id| id == session_id) {
        return Ok(Vec::new());
    }
    ids.insert(0, session_id.to_string());
    document["workspaces"][index]["sessionIds"] = json!(ids);
    document["workspaces"][index]["updatedAt"] = json!(iso8601(now_ms()));
    synchronize_document(&mut document, core);
    save(core, &document)?;
    let view = workspace_view(core, &document["workspaces"][index]);
    Ok(vec![json!({ "type": "upsert", "workspace": view })])
}

fn archive_session(core: &Core, cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = workspace_request(payload);
    let id = string_field(request, "sessionId")?;
    let stop = optional_bool(request, "stopActivity", false)?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let mut archived = string_array(document.get("archivedSessionIds"));
    if archived.iter().any(|candidate| candidate == id) {
        return Ok(json!({ "archivedSessionIds": archived }));
    }
    if !session_index(core).contains_key(id) {
        return Err(detailed_error(
            "session/not-found",
            format!("Session \"{id}\" not found"),
            json!({ "sessionId": id }),
        ));
    }
    if !stop {
        let state = api::session_state(cx, &json!({ "sessionId": id }))?;
        if state["running"].as_bool().unwrap_or(false) {
            return Err(detailed_error(
                "workspace/session-active",
                "Session has active work",
                json!({ "sessionId": id, "activity": [{ "kind": "turn" }] }),
            ));
        }
    }
    archived.push(id.to_string());
    document["archivedSessionIds"] = json!(archived);
    let pinned: Vec<String> = string_array(document.get("pinnedSessionIds"))
        .into_iter()
        .filter(|candidate| candidate != id)
        .collect();
    let removed_pin = pinned != string_array(document.get("pinnedSessionIds"));
    document["pinnedSessionIds"] = json!(pinned);
    save(core, &document)?;
    drop(_guard);
    let mut frames = vec![json!({ "type": "archived", "archivedSessionIds": archived })];
    if removed_pin {
        frames.push(json!({ "type": "pinned", "pinnedSessionIds": string_array(document.get("pinnedSessionIds")) }));
    }
    core.publish(frames);
    if stop {
        if let Err(error) = api::cancel_session(cx, &json!({ "sessionId": id })) {
            eprintln!("[dsh-rs] workspace session cancellation failed: {error:?}");
        }
    }
    Ok(json!({ "archivedSessionIds": archived }))
}

fn unarchive_session(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let id = string_field(workspace_request(payload), "sessionId")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let old = string_array(document.get("archivedSessionIds"));
    let ids: Vec<String> = old
        .iter()
        .filter(|candidate| *candidate != id)
        .cloned()
        .collect();
    if ids != old {
        document["archivedSessionIds"] = json!(ids);
        save(core, &document)?;
    }
    drop(_guard);
    if ids != old {
        core.publish(vec![
            json!({ "type": "archived", "archivedSessionIds": ids.clone() }),
        ]);
    }
    Ok(json!({ "archivedSessionIds": ids }))
}

fn pin_session(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let id = string_field(workspace_request(payload), "sessionId")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let old = string_array(document.get("pinnedSessionIds"));
    if old.iter().any(|candidate| candidate == id) {
        return Ok(json!({ "pinnedSessionIds": old }));
    }
    if string_array(document.get("archivedSessionIds"))
        .iter()
        .any(|candidate| candidate == id)
    {
        return Err(bad_request("An archived Session cannot be pinned"));
    }
    if !session_index(core).contains_key(id) {
        return Err(detailed_error(
            "session/not-found",
            format!("Session \"{id}\" not found"),
            json!({ "sessionId": id }),
        ));
    }
    let mut ids = vec![id.to_string()];
    ids.extend(old);
    document["pinnedSessionIds"] = json!(ids);
    save(core, &document)?;
    drop(_guard);
    core.publish(vec![
        json!({ "type": "pinned", "pinnedSessionIds": ids.clone() }),
    ]);
    Ok(json!({ "pinnedSessionIds": ids }))
}

fn unpin_session(core: &Core, payload: &Value) -> Result<Value, RpcError> {
    let id = string_field(workspace_request(payload), "sessionId")?;
    let _guard = core.mutation.lock().unwrap();
    let mut document = load(core)?;
    let old = string_array(document.get("pinnedSessionIds"));
    let ids: Vec<String> = old
        .iter()
        .filter(|candidate| *candidate != id)
        .cloned()
        .collect();
    if ids != old {
        document["pinnedSessionIds"] = json!(ids);
        save(core, &document)?;
    }
    drop(_guard);
    if ids != old {
        core.publish(vec![
            json!({ "type": "pinned", "pinnedSessionIds": ids.clone() }),
        ]);
    }
    Ok(json!({ "pinnedSessionIds": ids }))
}

struct FollowState {
    core: Arc<Core>,
    receiver: broadcast::Receiver<Value>,
    queue: VecDeque<Value>,
}

fn follow_workspaces(
    core: Arc<Core>,
    _payload: &Value,
) -> BoxStream<'static, Result<Value, RpcError>> {
    // Gateway invokes stream constructors synchronously. Subscribe before the
    // snapshot so a mutation during registry I/O cannot fall into a gap.
    let receiver = core.feed.subscribe();
    let document = match load(&core) {
        Ok(document) => document,
        Err(error) => {
            return stream::once(async move { Err(error) }).boxed();
        }
    };
    let queue = VecDeque::from([baseline(&core, &document)]);
    stream::unfold(
        FollowState {
            core,
            receiver,
            queue,
        },
        |mut state| async move {
            if let Some(frame) = state.queue.pop_front() {
                return Some((Ok(frame), state));
            }
            match state.receiver.recv().await {
                Ok(frame) => Some((Ok(frame), state)),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let document = match load(&state.core) {
                        Ok(document) => document,
                        Err(error) => return Some((Err(error), state)),
                    };
                    let mut replacement = baseline(&state.core, &document);
                    replacement["type"] = json!("replace");
                    Some((Ok(replacement), state))
                }
                Err(broadcast::error::RecvError::Closed) => None,
            }
        },
    )
    .boxed()
}

#[derive(Clone)]
struct FileScope {
    root: PathBuf,
}

fn file_scope(core: &Core, payload: &Value) -> Result<FileScope, RpcError> {
    let id = string_field(request(payload), "workspaceFileScopeId")?;
    let session = Store::mixed(core.home.join("sessions"))
        .read_all()
        .into_iter()
        .find(|stored| stored.header.id == id)
        .ok_or_else(|| {
            detailed_error(
                "gateway/lookup-not-found",
                "Workspace file Session was not found",
                json!({ "sessionId": id }),
            )
        })?;
    let requested_root = session
        .header
        .cwd
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| core.home.clone());
    let root = fs::canonicalize(&requested_root).map_err(|_| {
        detailed_error(
            "workspace-file/not-found",
            "Workspace root was not found",
            json!({ "path": display_path(&requested_root) }),
        )
    })?;
    let metadata = fs::metadata(&root).map_err(|_| {
        detailed_error(
            "workspace-file/not-found",
            "Workspace root was not found",
            json!({ "path": display_path(&requested_root) }),
        )
    })?;
    if !metadata.is_dir() {
        let kind = if metadata.is_file() { "file" } else { "other" };
        return Err(detailed_error(
            "workspace-file/not-directory",
            "Workspace root is not a directory",
            json!({ "path": display_path(&requested_root), "kind": kind }),
        ));
    }
    Ok(FileScope { root })
}

#[cfg(windows)]
fn contained(root: &Path, target: &Path) -> bool {
    let mut root = root.components();
    let mut target = target.components();
    loop {
        match (root.next(), target.next()) {
            (None, _) => return true,
            (Some(_), None) => return false,
            (Some(left), Some(right)) => {
                if !left
                    .as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy())
                {
                    return false;
                }
            }
        }
    }
}

#[cfg(not(windows))]
fn contained(root: &Path, target: &Path) -> bool {
    target.starts_with(root)
}

fn outside(path: &str) -> RpcError {
    detailed_error(
        "workspace-file/outside-workspace",
        format!("\"{path}\" is outside the workspace"),
        json!({ "path": path }),
    )
}

fn not_found(path: &str) -> RpcError {
    detailed_error(
        "workspace-file/not-found",
        format!("no entry at \"{path}\""),
        json!({ "path": path }),
    )
}

/// Resolve through the deepest existing ancestor. This preserves safe missing
/// watch targets while catching links in every existing component.
fn canonical_allow_missing(path: &Path) -> std::io::Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut suffix = Vec::new();
            let mut ancestor = path.to_path_buf();
            loop {
                match fs::canonicalize(&ancestor) {
                    Ok(mut canonical) => {
                        if !suffix.is_empty() && !canonical.is_dir() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::NotADirectory,
                                "path ancestor is not a directory",
                            ));
                        }
                        for component in suffix.into_iter().rev() {
                            canonical.push(component);
                        }
                        return Ok(canonical);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        let Some(name) = ancestor.file_name() else {
                            return Err(error);
                        };
                        suffix.push(name.to_os_string());
                        if !ancestor.pop() {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Err(error) => Err(error),
    }
}

fn requested_path(scope: &FileScope, raw: &str) -> Result<PathBuf, RpcError> {
    if raw.is_empty() || raw.contains('\0') {
        return Err(bad_request("path is required"));
    }
    let requested = Path::new(raw);
    Ok(if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        scope.root.join(requested)
    })
}

/// Resolve a file or watch target. Reads and file watches use filesystem read
/// authority, not workspace containment; only directory operations are confined.
fn resolve_path(
    scope: &FileScope,
    raw: &str,
    final_link_allowed: bool,
    missing_allowed: bool,
) -> Result<(PathBuf, PathBuf), RpcError> {
    let candidate = requested_path(scope, raw)?;
    match fs::symlink_metadata(&candidate) {
        Ok(metadata) if !final_link_allowed && !metadata.is_file() => {
            let kind = if metadata.file_type().is_symlink() {
                "symlink"
            } else {
                file_kind(&metadata)
            };
            return Err(detailed_error(
                "workspace-file/not-regular-file",
                format!("\"{raw}\" is a {kind}"),
                json!({ "path": raw, "kind": kind }),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && missing_allowed => {}
        Err(_) => return Err(not_found(raw)),
    }
    let canonical = canonical_allow_missing(&candidate).map_err(|_| not_found(raw))?;
    let exists = fs::metadata(&canonical).is_ok();
    if !exists && !missing_allowed {
        return Err(not_found(raw));
    }
    Ok((candidate, canonical))
}

fn file_kind(metadata: &fs::Metadata) -> &'static str {
    if metadata.is_file() {
        "file"
    } else if metadata.is_dir() {
        "directory"
    } else {
        "other"
    }
}

fn version_of(metadata: &fs::Metadata) -> String {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}:{}:{}", modified, metadata.len(), inode_of(metadata))
}

#[cfg(unix)]
fn inode_of(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ino()
}

#[cfg(not(unix))]
fn inode_of(metadata: &fs::Metadata) -> u64 {
    let _ = metadata;
    0
}

fn stat_value(path: &Path, metadata: &fs::Metadata) -> Value {
    json!({ "absolutePath": display_path(path), "version": version_of(metadata), "bytes": metadata.len() })
}

fn locate_file(scope: &FileScope, raw: &str) -> Result<(PathBuf, fs::Metadata, Value), RpcError> {
    let (_, canonical) = resolve_path(scope, raw, false, false)?;
    let metadata = fs::metadata(&canonical).map_err(|_| not_found(raw))?;
    if !metadata.is_file() {
        return Err(detailed_error(
            "workspace-file/not-regular-file",
            format!("\"{raw}\" is a {}", file_kind(&metadata)),
            json!({ "path": raw, "kind": file_kind(&metadata) }),
        ));
    }
    let stat = stat_value(&canonical, &metadata);
    Ok((canonical, metadata, stat))
}

fn safe_integer(number: &serde_json::Number) -> Option<u64> {
    let integer = number.as_u64().or_else(|| {
        let value = number.as_f64()?;
        (value.is_finite()
            && value.fract() == 0.0
            && value >= 0.0
            && value <= MAX_SAFE_INTEGER as f64)
            .then_some(value as u64)
    })?;
    (integer <= MAX_SAFE_INTEGER).then_some(integer)
}

fn optional_integer(value: &Value, name: &str, default: u64, min: u64) -> Result<u64, RpcError> {
    match value.get(name) {
        None => Ok(default),
        Some(Value::Null) => Err(bad_request(format!(
            "{name} must be a safe integer of at least {min}"
        ))),
        Some(Value::Number(number)) => safe_integer(number)
            .filter(|n| *n >= min)
            .ok_or_else(|| bad_request(format!("{name} must be a safe integer of at least {min}"))),
        Some(_) => Err(bad_request(format!(
            "{name} must be a safe integer of at least {min}"
        ))),
    }
}

fn not_text(raw: &str, message: impl Into<String>) -> RpcError {
    detailed_error("workspace-file/not-text", message, json!({ "path": raw }))
}

/// Decode one segment while retaining only an incomplete trailing UTF-8 sequence.
fn decode_utf8_segment(pending: &mut Vec<u8>, bytes: &[u8], raw: &str) -> Result<String, RpcError> {
    let mut combined = Vec::with_capacity(pending.len() + bytes.len());
    combined.extend_from_slice(pending);
    combined.extend_from_slice(bytes);
    pending.clear();
    match std::str::from_utf8(&combined) {
        Ok(text) => Ok(text.to_string()),
        Err(error) if error.error_len().is_none() => {
            let valid = error.valid_up_to();
            let trailing = combined.len() - valid;
            if trailing > 3 {
                return Err(not_text(raw, format!("\"{raw}\" is not UTF-8 text")));
            }
            pending.extend_from_slice(&combined[valid..]);
            Ok(std::str::from_utf8(&combined[..valid])
                .map_err(|_| not_text(raw, format!("\"{raw}\" is not UTF-8 text")))?
                .to_string())
        }
        Err(_) => Err(not_text(raw, format!("\"{raw}\" is not UTF-8 text"))),
    }
}

fn complete_page_line(
    lines: &mut Vec<String>,
    current: &mut String,
    bytes: &mut usize,
    raw: &str,
) -> Result<(), RpcError> {
    if !lines.is_empty() {
        *bytes = bytes.checked_add(1).ok_or_else(|| too_large(raw))?;
    }
    if *bytes > MAX_BYTES {
        return Err(too_large(raw));
    }
    lines.push(std::mem::take(current));
    Ok(())
}

fn text_page(
    path: &Path,
    raw: &str,
    offset: u64,
    limit: usize,
) -> Result<(String, usize, bool), RpcError> {
    let last = offset.saturating_add(limit as u64 - 1);
    let file = File::open(path).map_err(|_| not_found(raw))?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut pending_utf8 = Vec::new();
    let mut line_number = 1_u64;
    let mut current = String::new();
    let mut lines: Vec<String> = Vec::new();
    let mut bytes = 0_usize;
    let mut eof = true;
    'chunks: loop {
        let buffer = reader.fill_buf().map_err(|_| not_found(raw))?;
        if buffer.is_empty() {
            break;
        }
        let mut position = 0;
        while position < buffer.len() {
            if line_number > last {
                eof = false;
                break 'chunks;
            }
            let newline = buffer[position..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|at| position + at);
            let end = newline.unwrap_or(buffer.len());
            let segment = decode_utf8_segment(&mut pending_utf8, &buffer[position..end], raw)?;
            if line_number >= offset {
                bytes = bytes
                    .checked_add(segment.len())
                    .ok_or_else(|| too_large(raw))?;
                if bytes > MAX_BYTES {
                    return Err(too_large(raw));
                }
                current.push_str(&segment);
            }
            if let Some(newline) = newline {
                if line_number >= offset {
                    complete_page_line(&mut lines, &mut current, &mut bytes, raw)?;
                    if lines.len() > limit {
                        return Err(bad_request("line page exceeded its limit"));
                    }
                }
                line_number += 1;
                position = newline + 1;
            } else {
                position = buffer.len();
            }
        }
        reader.consume(position);
    }
    if !pending_utf8.is_empty() {
        return Err(not_text(raw, format!("\"{raw}\" is not UTF-8 text")));
    }
    if !current.is_empty() {
        complete_page_line(&mut lines, &mut current, &mut bytes, raw)?;
    }
    let text = lines.join("\n");
    if text.contains('\0') {
        return Err(not_text(raw, format!("\"{raw}\" contains NUL bytes")));
    }
    Ok((text, lines.len(), eof))
}

fn too_large(path: &str) -> RpcError {
    detailed_error(
        "workspace-file/too-large",
        format!("\"{path}\" exceeds the {MAX_BYTES} byte cap"),
        json!({ "path": path, "limit": MAX_BYTES }),
    )
}

fn file_too_large(path: &str) -> RpcError {
    detailed_error(
        "workspace-file/too-large",
        format!("\"{path}\" exceeds the {MAX_FILE_BYTES} byte full-file cap"),
        json!({ "path": path, "limit": MAX_FILE_BYTES }),
    )
}

fn file_read(core: &Core, _cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request(payload);
    let scope = file_scope(core, payload)?;
    let raw = string_field(request, "path")?;
    let range = request
        .get("range")
        .filter(|value| value.is_object())
        .ok_or_else(|| bad_request("range must be an object"))?;
    let offset = optional_integer(range, "offset", 1, 1)?;
    let limit = optional_integer(range, "limit", MAX_LINES as u64, 1)?;
    if limit > MAX_LINES as u64 {
        return Err(bad_request(format!("limit must be at most {MAX_LINES}")));
    }
    let (path, _, stat) = locate_file(&scope, raw)?;
    let (text, lines, eof) = text_page(&path, raw, offset, limit as usize)?;
    let mut result = stat;
    result["offset"] = json!(offset);
    result["text"] = json!(text);
    result["lines"] = json!(lines);
    result["eof"] = json!(eof);
    Ok(result)
}

fn file_read_bytes(core: &Core, _cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request(payload);
    let scope = file_scope(core, payload)?;
    let raw = string_field(request, "path")?;
    let options = request
        .get("options")
        .filter(|value| value.is_object())
        .ok_or_else(|| bad_request("options must be an object"))?;
    let range = options.get("range");
    let window = match range {
        None => None,
        Some(value) if value.is_object() => {
            let offset = optional_integer(value, "offset", 0, 0)?;
            let length = optional_integer(value, "length", MAX_BYTES as u64, 1)?;
            if offset
                .checked_add(length)
                .is_none_or(|end| end > MAX_SAFE_INTEGER)
            {
                return Err(bad_request("offset plus length must stay a safe integer"));
            }
            if length > MAX_BYTES as u64 {
                return Err(too_large(raw));
            }
            Some((offset, length))
        }
        Some(_) => return Err(bad_request("range must be an object")),
    };
    let base = optional_string(options, "baseFile")?;
    let resolved;
    let target = if let Some(base) = base {
        let normalized = raw.replace('\\', "/");
        if normalized.is_empty()
            || normalized.starts_with('/')
            || normalized.contains('\0')
            || has_uri_scheme(&normalized)
        {
            return Err(bad_request(
                "path must be relative when baseFile is provided",
            ));
        }
        let (base_path, _, _) = locate_file(&scope, base)?;
        resolved = base_path
            .parent()
            .unwrap_or(&scope.root)
            .join(Path::new(&normalized));
        display_path(&resolved)
    } else {
        raw.to_string()
    };
    let (path, metadata, stat) = locate_file(&scope, &target)?;
    let mut result = stat;
    if let Some((offset, length)) = window {
        let mut file = File::open(&path).map_err(|_| not_found(raw))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| not_found(raw))?;
        let mut data = Vec::with_capacity(length as usize);
        file.take(length)
            .read_to_end(&mut data)
            .map_err(|_| not_found(raw))?;
        result["offset"] = json!(offset);
        result["data"] = json!(data);
        result["eof"] = json!(offset + data.len() as u64 >= metadata.len());
    } else {
        if metadata.len() > MAX_FILE_BYTES {
            return Err(file_too_large(raw));
        }
        let mut data = Vec::with_capacity(metadata.len() as usize);
        File::open(&path)
            .and_then(|file| file.take(MAX_FILE_BYTES + 1).read_to_end(&mut data))
            .map_err(|_| not_found(raw))?;
        if data.len() as u64 > MAX_FILE_BYTES {
            return Err(file_too_large(raw));
        }
        result["offset"] = json!(0);
        result["data"] = json!(data);
        result["eof"] = json!(true);
    }
    Ok(result)
}

fn has_uri_scheme(path: &str) -> bool {
    let Some(colon) = path.find(':') else {
        return false;
    };
    let scheme = &path[..colon];
    !scheme.is_empty()
        && scheme
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic())
        && scheme
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
}

fn file_stat(core: &Core, _cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let scope = file_scope(core, payload)?;
    let raw = string_field(request(payload), "path")?;
    let (_, _, stat) = locate_file(&scope, raw)?;
    Ok(stat)
}

fn relative_wire_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn file_list(core: &Core, _cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request(payload);
    let scope = file_scope(core, payload)?;
    let raw = string_field(request, "path")?;
    let candidate = requested_path(&scope, raw)?;
    let entry = fs::symlink_metadata(&candidate).map_err(|_| not_found(raw))?;
    let final_link = entry.file_type().is_symlink();
    if !final_link && !entry.is_dir() {
        let kind = file_kind(&entry);
        return Err(detailed_error(
            "workspace-file/not-directory",
            format!("\"{raw}\" is a {kind}"),
            json!({ "path": raw, "kind": kind }),
        ));
    }
    let canonical = match canonical_allow_missing(&candidate) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && final_link => {
            return Err(detailed_error(
                "workspace-file/not-directory",
                format!("\"{raw}\" does not resolve to a directory"),
                json!({ "path": raw, "kind": "symlink" }),
            ));
        }
        Err(_) => return Err(not_found(raw)),
    };
    if !contained(&scope.root, &canonical) {
        return Err(outside(raw));
    }
    let metadata = match fs::metadata(&canonical) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && final_link => {
            return Err(detailed_error(
                "workspace-file/not-directory",
                format!("\"{raw}\" does not resolve to a directory"),
                json!({ "path": raw, "kind": "symlink" }),
            ));
        }
        Err(_) => return Err(not_found(raw)),
    };
    if !metadata.is_dir() {
        let kind = if final_link {
            "symlink"
        } else {
            file_kind(&metadata)
        };
        return Err(detailed_error(
            "workspace-file/not-directory",
            format!("\"{raw}\" does not resolve to a directory"),
            json!({ "path": raw, "kind": kind }),
        ));
    }
    let mut selected: BinaryHeap<(String, PathBuf)> = BinaryHeap::new();
    let mut total = 0_usize;
    let read_dir = fs::read_dir(&canonical).map_err(|_| not_found(raw))?;
    for entry in read_dir {
        let entry = entry.map_err(|_| not_found(raw))?;
        total += 1;
        let item = (
            entry.file_name().to_string_lossy().to_string(),
            entry.path(),
        );
        if selected.len() < MAX_ENTRIES {
            selected.push(item);
        } else if selected.peek().is_some_and(|largest| item < *largest) {
            selected.pop();
            selected.push(item);
        }
    }
    let mut children = Vec::with_capacity(selected.len());
    for (name, child) in selected {
        let value = match fs::metadata(&child) {
            Ok(metadata) => {
                let mut value = json!({ "name": name, "type": file_kind(&metadata) });
                if metadata.is_file() {
                    value["size"] = json!(metadata.len());
                }
                value
            }
            Err(_) => json!({ "name": name, "type": "other" }),
        };
        children.push(value);
    }
    children.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Ok(json!({
        "path": relative_wire_path(&scope.root, &canonical),
        "entries": children,
        "truncated": total > MAX_ENTRIES
    }))
}

fn exclusion_glob(path: &str) -> String {
    let mut escaped = String::new();
    for character in path.chars() {
        if matches!(character, '\\' | '*' | '?' | '[' | ']' | '{' | '}' | '!') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    format!(
        "--glob=!{}{escaped}",
        if path.contains('/') { "/" } else { "" }
    )
}

fn find_ripgrep() -> OsString {
    if let Some(path) = std::env::var_os("DSH_RIPGREP") {
        return path;
    }
    if let Some(paths) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&paths) {
            for name in ["rg.exe", "rg"] {
                let candidate = directory.join(name);
                if candidate.is_file() {
                    return candidate.into_os_string();
                }
            }
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        for ancestor in exe.ancestors().skip(1) {
            let modules = ancestor.join("node_modules").join(".pnpm");
            if let Ok(entries) = fs::read_dir(modules) {
                for entry in entries.flatten() {
                    if !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("@vscode+ripgrep-")
                    {
                        continue;
                    }
                    let candidate = entry
                        .path()
                        .join("node_modules")
                        .join("@vscode")
                        .join("ripgrep-win32-x64")
                        .join("bin")
                        .join("rg.exe");
                    if candidate.is_file() {
                        return candidate.into_os_string();
                    }
                }
            }
        }
    }
    OsString::from("rg")
}

struct SearchRun {
    paths: Vec<String>,
    incomplete: bool,
}

fn run_ripgrep(root: &Path, arguments: &[String]) -> Result<SearchRun, RpcError> {
    let mut child = Command::new(find_ripgrep())
        .args(arguments)
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            detailed_error(
                "workspace-file/search-failed",
                format!("could not launch workspace search: {error}"),
                json!({}),
            )
        })?;
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let capped = Arc::new(AtomicBool::new(false));
    let reader_capped = capped.clone();
    let output = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut paths = Vec::new();
        let mut listed = 0_usize;
        loop {
            let mut bytes = Vec::new();
            if reader.read_until(0, &mut bytes).map_err(|_| ())? == 0 {
                break;
            }
            if listed == MAX_SEARCH_ENTRIES {
                reader_capped.store(true, Ordering::Release);
                break;
            }
            bytes.pop();
            if let Ok(path) = String::from_utf8(bytes) {
                paths.push(path);
            }
            listed += 1;
        }
        Ok::<_, ()>(paths)
    });
    let diagnostics = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            match stderr.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    let remaining = 65_536_usize.saturating_sub(bytes.len());
                    bytes.extend_from_slice(&buffer[..count.min(remaining)]);
                }
                Err(_) => break,
            }
        }
        bytes
    });
    let start = SystemTime::now();
    let status = loop {
        if capped.load(Ordering::Acquire) {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if start.elapsed().unwrap_or_default() >= SEARCH_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = output.join();
                let _ = diagnostics.join();
                return Err(detailed_error(
                    "workspace-file/search-failed",
                    "workspace file search timed out",
                    json!({}),
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = output.join();
                let _ = diagnostics.join();
                return Err(detailed_error(
                    "workspace-file/search-failed",
                    error.to_string(),
                    json!({}),
                ));
            }
        }
    };
    let paths = output
        .join()
        .map_err(|_| {
            detailed_error(
                "workspace-file/search-failed",
                "could not read workspace search",
                json!({}),
            )
        })?
        .map_err(|_| {
            detailed_error(
                "workspace-file/search-failed",
                "could not read workspace search",
                json!({}),
            )
        })?;
    let stderr = diagnostics.join().unwrap_or_default();
    if let Some(status) = status {
        if !status.success() && status.code() != Some(1) && status.code() != Some(2) {
            return Err(detailed_error(
                "workspace-file/search-failed",
                format!(
                    "workspace search failed: {}",
                    String::from_utf8_lossy(&stderr)
                ),
                json!({}),
            ));
        }
    }
    Ok(SearchRun {
        paths,
        incomplete: capped.load(Ordering::Acquire)
            || status.is_some_and(|status| status.code() == Some(2)),
    })
}

fn file_search(core: &Core, _cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request(payload);
    let scope = file_scope(core, payload)?;
    let spec = request
        .get("request")
        .filter(|value| value.is_object())
        .ok_or_else(|| bad_request("request must be an object"))?;
    let query = spec
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if query.is_empty() {
        return Err(bad_request("query must not be blank"));
    }
    let excluded = spec
        .get("excludedPaths")
        .and_then(Value::as_array)
        .ok_or_else(|| bad_request("excludedPaths must be an array"))?;
    if excluded.iter().any(|value| !value.is_string()) {
        return Err(bad_request("excludedPaths must contain strings"));
    }
    if spec.get("includeHidden").and_then(Value::as_bool).is_none()
        || spec
            .get("respectGitignore")
            .and_then(Value::as_bool)
            .is_none()
    {
        return Err(bad_request(
            "includeHidden and respectGitignore are required",
        ));
    }
    let mut arguments = vec![
        "--no-config".to_string(),
        "--files".to_string(),
        "--sort=path".to_string(),
        "--null".to_string(),
        "--path-separator=/".to_string(),
    ];
    if spec["includeHidden"].as_bool() == Some(true) {
        arguments.push("--hidden".to_string());
    }
    if spec["respectGitignore"].as_bool() == Some(false) {
        arguments.push("--no-ignore".to_string());
    }
    for name in VCS_NAMES {
        arguments.push(exclusion_glob(name));
    }
    for value in excluded {
        arguments.push(exclusion_glob(value.as_str().unwrap()));
    }
    let run = run_ripgrep(&scope.root, &arguments)?;
    let needle = query.to_lowercase();
    let by_path = query.contains('/');
    let mut matches: HashMap<String, Value> = HashMap::new();
    for path in run.paths {
        let mut admit = |candidate: &str, kind: &str| {
            let name = candidate.rsplit('/').next().unwrap_or(candidate);
            if (if by_path { candidate } else { name })
                .to_lowercase()
                .contains(&needle)
            {
                matches.insert(
                    candidate.to_string(),
                    json!({ "path": candidate, "type": kind }),
                );
            }
        };
        admit(&path, "file");
        let mut end = path.rfind('/');
        while let Some(index) = end {
            admit(&path[..index], "directory");
            end = path[..index].rfind('/');
        }
    }
    let mut matches: Vec<Value> = matches.into_values().collect();
    matches.sort_by(|a, b| {
        let depth = |v: &Value| {
            v["path"]
                .as_str()
                .unwrap_or("")
                .bytes()
                .filter(|b| *b == b'/')
                .count()
        };
        depth(a)
            .cmp(&depth(b))
            .then_with(|| a["path"].as_str().cmp(&b["path"].as_str()))
    });
    let truncated = matches.len() > MAX_RESULTS;
    matches.truncate(MAX_RESULTS);
    Ok(json!({ "matches": matches, "truncated": truncated, "incomplete": run.incomplete }))
}

#[derive(Debug, PartialEq, Eq)]
enum Observation {
    Present { path: String, version: String },
    Absent { path: String },
}

fn observe(scope: &FileScope, canonical: &Path, raw: &str) -> Result<Observation, RpcError> {
    let resolved = match canonical_allow_missing(canonical) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Observation::Absent {
                path: display_path(canonical),
            });
        }
        Err(_) => return Err(not_found(raw)),
    };
    let metadata = match fs::metadata(&resolved) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Observation::Absent {
                path: display_path(&resolved),
            });
        }
        Err(_) => return Err(not_found(raw)),
    };
    if metadata.is_dir() && !contained(&scope.root, &resolved) {
        return Err(outside(raw));
    }
    let version = if metadata.is_dir() {
        let mut digest = 0_u128;
        for entry in fs::read_dir(&resolved).map_err(|_| not_found(raw))? {
            let entry = entry.map_err(|_| not_found(raw))?;
            let path = entry.path();
            let mut hash = DefaultHasher::new();
            hash.write(entry.file_name().to_string_lossy().as_bytes());
            if let Ok(metadata) = fs::metadata(&path) {
                hash.write_u64(inode_of(&metadata));
                hash.write_u64(metadata.len());
                if let Some(modified) = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                {
                    hash.write_u128(modified.as_nanos());
                }
            }
            digest = digest.wrapping_add(hash.finish() as u128);
        }
        format!("{digest:032x}")
    } else {
        version_of(&metadata)
    };
    Ok(Observation::Present {
        path: display_path(&resolved),
        version,
    })
}

fn file_changes(core: Arc<Core>, payload: &Value) -> BoxStream<'static, Result<Value, RpcError>> {
    let initial = (|| {
        let scope = file_scope(&core, payload)?;
        let raw = string_field(request(payload), "path")?.to_string();
        let (_, canonical) = resolve_path(&scope, &raw, true, true)?;
        let observation = observe(&scope, &canonical, &raw)?;
        Ok::<_, RpcError>((scope, canonical, raw, observation))
    })();
    match initial {
        Err(error) => stream::once(async move { Err(error) }).boxed(),
        Ok((scope, canonical, raw, observation)) => stream::unfold(Some((observation, true, scope, canonical, raw)), move |state| async move {
            let (mut previous, first, scope, canonical, raw) = state?;
            if first {
                return Some((Ok(json!({ "kind": "ready" })), Some((previous, false, scope, canonical, raw))));
            }
            let current = loop {
                tokio::time::sleep(WATCH_POLL).await;
                let observed = tokio::task::spawn_blocking({
                    let scope = scope.clone();
                    let canonical = canonical.clone();
                    let raw = raw.clone();
                    move || observe(&scope, &canonical, &raw)
                })
                .await;
                match observed {
                    Ok(Ok(current)) if current != previous => break current,
                    Ok(Ok(current)) => previous = current,
                    Ok(Err(error)) => return Some((Err(error), None)),
                    Err(_) => {
                        let error = detailed_error(
                            "workspace-file/watch-unsupported",
                            "workspace file observation failed",
                            json!({ "path": raw }),
                        );
                        return Some((Err(error), None));
                    }
                }
            };
            let frame = match &current {
                Observation::Present { path, version } => json!({ "kind": "change", "change": { "absolutePath": path, "version": version } }),
                Observation::Absent { path } => json!({ "kind": "change", "change": { "absolutePath": path, "absent": true } }),
            };
            Some((Ok(frame), Some((current, false, scope, canonical, raw))))
        }).boxed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    struct TempRoot(PathBuf);
    impl TempRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "dsh-wa-{label}-{}-{}",
                std::process::id(),
                &random_uuid()[..8]
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn core_at(root: &Path) -> Arc<Core> {
        let gateway = Gateway::new("test", root.to_string_lossy(), None);
        Arc::new(Core {
            home: root.to_path_buf(),
            feed: gateway.broadcaster("workspace/follow"),
            mutation: Mutex::new(()),
        })
    }

    fn context(root: &Path) -> Arc<ApiContext> {
        ApiContext::new("http://127.0.0.1", "test", root)
    }

    fn add_session(root: &Path, id: &str, cwd: &Path) {
        let store = Store::new(root.join("sessions"));
        let header = Header {
            kind: HeaderTag::Session,
            version: SESSION_FORMAT_VERSION,
            id: id.to_string(),
            created_at: now_ms(),
            cwd: Some(display_path(cwd)),
            parent_session: None,
            is_seeded: false,
            origin: None,
            delegation_depth: 0,
            agent_preset: Some("standard".to_string()),
        };
        drop(store.create(header).unwrap());
    }

    #[test]
    fn workspace_persistence_order_archive_and_pin() {
        let temp = TempRoot::new("registry");
        let one = temp.0.join("one");
        let two = temp.0.join("two");
        fs::create_dir(&one).unwrap();
        fs::create_dir(&two).unwrap();
        let core = core_at(&temp.0);
        let cx = context(&temp.0);
        let first = create_workspace(&core, &cx, &json!({ "path": display_path(&one) })).unwrap();
        let second = create_workspace(&core, &cx, &json!({ "path": display_path(&two) })).unwrap();
        assert_eq!(first["created"], true);
        assert_eq!(second["workspace"]["sessionIds"], json!([]));
        let one_id = first["workspace"]["workspaceId"].as_str().unwrap();
        let two_id = second["workspace"]["workspaceId"].as_str().unwrap();
        add_session(&temp.0, "s1", &one);
        add_session(&temp.0, "s2", &two);
        let attached = attach_created_session(&core, "s1").unwrap();
        assert_eq!(
            attached[0]["workspace"]["workspaceId"],
            first["workspace"]["workspaceId"]
        );
        attach_created_session(&core, "s2").unwrap();
        assert_eq!(
            pin_session(&core, &json!({ "sessionId": "s1" })).unwrap()["pinnedSessionIds"],
            json!(["s1"])
        );
        assert_eq!(
            pin_session(&core, &json!({ "sessionId": "s1" })).unwrap()["pinnedSessionIds"],
            json!(["s1"])
        );
        assert_eq!(
            archive_session(&core, &cx, &json!({ "sessionId": "s1" })).unwrap()
                ["archivedSessionIds"],
            json!(["s1"])
        );
        let document = load(&core).unwrap();
        assert_eq!(document["pinnedSessionIds"], json!([]));
        assert_eq!(
            workspace_ids(&document),
            vec![two_id.to_string(), one_id.to_string()]
        );
        let reloaded = core_at(&temp.0);
        assert_eq!(load(&reloaded).unwrap(), document);
    }

    #[test]
    fn file_wire_shapes_paging_binary_and_traversal() {
        let temp = TempRoot::new("files");
        let root = temp.0.join("workspace");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("lines.txt"), b"one\ntwo\n").unwrap();
        fs::write(root.join("bytes.bin"), [0_u8, 1, 2, 255]).unwrap();
        add_session(&temp.0, "scope", &root);
        let core = core_at(&temp.0);
        let cx = context(&temp.0);
        let page = file_read(&core, &cx, &json!({ "args": { "workspaceFileScopeId": "scope", "path": "lines.txt", "range": { "offset": 2, "limit": 1 } } })).unwrap();
        assert_eq!(page["text"], "two");
        assert_eq!(page["lines"], 1);
        assert_eq!(page["eof"], true);
        assert!(page["bytes"].is_number());
        let bytes = file_read_bytes(&core, &cx, &json!({ "args": { "workspaceFileScopeId": "scope", "path": "bytes.bin", "options": { "range": { "offset": 1, "length": 2 } } } })).unwrap();
        assert_eq!(bytes["data"], json!([1, 2]));
        assert_eq!(bytes["eof"], false);
        assert_eq!(
            file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "lines.txt" } })
            )
            .unwrap()["bytes"],
            8
        );
        assert_eq!(
            file_list(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "" } })
            )
            .unwrap_err()
            .details,
            json!({})
        );
        fs::write(temp.0.join("outside"), b"outside").unwrap();
        assert_eq!(
            file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "../outside" } }),
            )
            .unwrap()["bytes"],
            7
        );
        assert_eq!(
            file_list(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": ".." } }),
            )
            .unwrap_err()
            .code,
            "workspace-file/outside-workspace"
        );
    }

    #[cfg(windows)]
    #[test]
    fn junction_allows_file_reads_but_rejects_directory_escape() {
        let temp = TempRoot::new("junction");
        let root = temp.0.join("workspace");
        let outside = temp.0.join("outside");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("secret"), b"secret").unwrap();
        let link = root.join("link");
        let status = Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&outside)
            .status()
            .unwrap();
        if !status.success() {
            return;
        }
        add_session(&temp.0, "scope", &root);
        let core = core_at(&temp.0);
        let cx = context(&temp.0);
        assert_eq!(
            file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "link/secret" } }),
            )
            .unwrap()["bytes"],
            6
        );
        let error = file_list(
            &core,
            &cx,
            &json!({ "args": { "workspaceFileScopeId": "scope", "path": "link" } }),
        )
        .unwrap_err();
        assert_eq!(error.code, "workspace-file/outside-workspace");

        let link_error = file_stat(
            &core,
            &cx,
            &json!({ "args": { "workspaceFileScopeId": "scope", "path": "link" } }),
        )
        .unwrap_err();
        assert_eq!(link_error.code, "workspace-file/not-regular-file");
        assert_eq!(link_error.details["kind"], "symlink");

        let scope = FileScope {
            root: fs::canonicalize(&root).unwrap(),
        };
        assert!(observe(&scope, &scope.root, ".").is_ok());
        let later = root.join("later");
        assert!(matches!(
            observe(&scope, &later, "later"),
            Ok(Observation::Absent { .. })
        ));
        assert!(Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&later)
            .arg(&outside)
            .status()
            .unwrap()
            .success());
        assert_eq!(
            observe(&scope, &later, "later").unwrap_err().code,
            "workspace-file/outside-workspace"
        );
    }

    #[tokio::test]
    async fn follow_is_baseline_then_live() {
        let temp = TempRoot::new("follow");
        let root = temp.0.join("workspace");
        fs::create_dir(&root).unwrap();
        let core = core_at(&temp.0);
        let cx = context(&temp.0);
        create_workspace(&core, &cx, &json!({ "path": display_path(&root) })).unwrap();
        let mut receiver = core.feed.subscribe();
        let mut stream = follow_workspaces(core.clone(), &json!({}));
        let baseline = stream.next().await.unwrap().unwrap();
        assert_eq!(baseline["type"], "baseline");
        let other = temp.0.join("other");
        fs::create_dir(&other).unwrap();
        create_workspace(&core, &cx, &json!({ "path": display_path(&other) })).unwrap();
        assert_eq!(receiver.try_recv().unwrap()["type"], "upsert");
        assert_eq!(stream.next().await.unwrap().unwrap()["type"], "upsert");
        assert_eq!(stream.next().await.unwrap().unwrap()["type"], "order");
        for _ in 0..300 {
            core.feed
                .publish(json!({ "type": "order", "workspaceIds": [] }));
        }
        assert_eq!(stream.next().await.unwrap().unwrap()["type"], "replace");
    }

    #[test]
    fn text_paging_edges_and_caps() {
        let temp = TempRoot::new("text");
        let root = temp.0.join("workspace");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("empty"), "").unwrap();
        fs::write(root.join("blank"), "\n").unwrap();
        fs::write(root.join("tail"), "a\nb\nc").unwrap();
        fs::write(root.join("nul-byte"), b"ok\n\0bad").unwrap();
        fs::write(root.join("invalid"), [0xff, 0xfe]).unwrap();
        add_session(&temp.0, "scope", &root);
        let core = core_at(&temp.0);
        let cx = context(&temp.0);
        let read = |path: &str, range: Value| {
            file_read(&core, &cx, &json!({ "args": { "workspaceFileScopeId": "scope", "path": path, "range": range } })).unwrap()
        };
        assert_eq!(read("empty", json!({}))["lines"], 0);
        assert_eq!(read("blank", json!({}))["lines"], 1);
        assert_eq!(read("blank", json!({}))["text"], "");
        assert_eq!(
            read("tail", json!({ "offset": 2, "limit": 1 }))["eof"],
            false
        );
        assert_eq!(read("tail", json!({ "offset": 4 }))["lines"], 0);
        assert_eq!(read("tail", json!({ "offset": 4 }))["eof"], true);
        assert_eq!(
            read("nul-byte", json!({ "offset": 1, "limit": 1 }))["text"],
            "ok"
        );
        assert_eq!(
            file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "nul-byte", "range": {} } })
            )
            .unwrap_err()
            .code,
            "workspace-file/not-text"
        );
        assert_eq!(
            file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "invalid", "range": {} } })
            )
            .unwrap_err()
            .code,
            "workspace-file/not-text"
        );
        assert_eq!(file_read(&core, &cx, &json!({ "args": { "workspaceFileScopeId": "scope", "path": "empty", "range": { "limit": 5001 } } })).unwrap_err().code, "gateway/bad-request");
        fs::write(root.join("large"), vec![b'x'; MAX_BYTES + 1]).unwrap();
        assert_eq!(
            file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "large", "range": {} } })
            )
            .unwrap_err()
            .code,
            "workspace-file/too-large"
        );
    }

    #[tokio::test]
    async fn changes_emit_ready_creation_and_absence() {
        let temp = TempRoot::new("changes");
        let root = temp.0.join("workspace");
        fs::create_dir(&root).unwrap();
        add_session(&temp.0, "scope", &root);
        let core = core_at(&temp.0);
        let mut stream = file_changes(
            core,
            &json!({ "args": { "workspaceFileScopeId": "scope", "path": "later.txt" } }),
        );
        assert_eq!(stream.next().await.unwrap().unwrap()["kind"], "ready");
        fs::write(root.join("later.txt"), b"hello").unwrap();
        let created = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(created["kind"], "change");
        assert_eq!(created["change"]["absent"], Value::Null);
        fs::remove_file(root.join("later.txt")).unwrap();
        let removed = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(removed["change"]["absent"], true);
    }

    #[test]
    fn search_has_literal_exclusions_and_inferred_directories() {
        if Command::new(find_ripgrep())
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let temp = TempRoot::new("search");
        let root = temp.0.join("workspace");
        fs::create_dir_all(root.join("src").join("sub")).unwrap();
        fs::write(root.join("src").join("SrcAlpha.ts"), "").unwrap();
        fs::write(root.join("src").join("sub").join("Beta.ts"), "").unwrap();
        fs::write(root.join("src").join("sub").join("Gamma.txt"), "").unwrap();
        add_session(&temp.0, "scope", &root);
        let core = core_at(&temp.0);
        let cx = context(&temp.0);
        let result = file_search(&core, &cx, &json!({ "args": {
            "workspaceFileScopeId": "scope",
            "request": { "query": "SRC", "excludedPaths": ["sub"], "includeHidden": true, "respectGitignore": false }
        } })).unwrap();
        assert_eq!(
            result["matches"],
            json!([
                { "path": "src", "type": "directory" },
                { "path": "src/SrcAlpha.ts", "type": "file" }
            ])
        );
        assert_eq!(result["truncated"], false);
        assert_eq!(result["incomplete"], false);
    }
}
