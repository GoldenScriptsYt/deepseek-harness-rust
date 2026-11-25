//! Slash-command execution RPC for commands owned by the Rust Host.
//!
//! `/plan` owns the whole plan-mode surface: it logs `command/run`, commits or
//! queues the mode change, and logs `command/done`. A selection made during an
//! open turn stays pending in the log (`command/done` without `plan/mode`) and
//! is committed by the running Agent at the turn boundary.

use crate::api::{append_host_event, request_of, rpc_error, stored_session, ApiContext};
use crate::plan::plan_narration;
use dsh_gateway::{Gateway, RpcError};
use dsh_planning::fold_plan;
use rand::random;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Commands currently implemented by the Rust host, in display order.
pub(crate) fn descriptors() -> Value {
    json!([
        {"definitionId": "@deepseek-ai/dsh-command-feedback", "name": "feedback", "description": "Record feedback about this session", "input": {"hint": "<text>"}},
        {"definitionId": "@deepseek-ai/dsh-plan-mode", "name": "plan", "description": "Enter or leave plan mode", "input": {"hint": "[off|message]"}}
    ])
}

/// What one selection did, deciding the command's reported text.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Selection {
    Committed,
    Queued,
    Cancelled,
    Noop,
}

pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    gateway.rpc("commands/execute", move |payload| {
        let context = Arc::clone(&context);
        async move { execute(&context, &payload).await }
    });
}

pub(crate) async fn execute(context: &Arc<ApiContext>, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let session_id = request
        .get("agentId")
        .or_else(|| request.get("sessionId"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "agentId must not be empty"))?;
    let line = request
        .get("line")
        .and_then(Value::as_str)
        .ok_or_else(|| rpc_error("gateway/bad-request", "line must be a string"))?;
    let Some((name, raw_input)) = parse_command(line) else {
        return Ok(Value::Null);
    };
    if name != "plan" && name != "feedback" {
        return Ok(Value::Null);
    }
    let has_attachments = request
        .get("submittedAttachments")
        .or_else(|| request.get("attachments"))
        .and_then(Value::as_array)
        .is_some_and(|attachments| !attachments.is_empty());
    let message = raw_input.trim().to_string();
    if name == "feedback" {
        return execute_feedback(context, session_id, &message, has_attachments).await;
    }
    if has_attachments {
        return Err(rpc_error(
            "command/attachments",
            "Attachments are not supported with /plan yet; send the files in a separate message.",
        ));
    }
    let stored = stored_session(context, session_id)?;
    let logged = fold_plan(&stored.events).active;
    let active = message != "off";
    let command_id = command_id();
    let selection = select(context, session_id, logged, active);
    let text = notice(selection, active, logged);
    if context.session_running(session_id)
        && matches!(selection, Selection::Queued | Selection::Cancelled)
    {
        // A running turn owns the Session write lease: the command rows and the
        // mode change land together at the turn boundary.
        context.set_plan_pending(
            session_id,
            crate::api::PendingPlan {
                active,
                command_id: command_id.clone(),
                args: raw_input.to_string(),
                text: text.to_string(),
            },
        );
    } else {
        append_host_event(
            context,
            session_id,
            "command/run",
            json!({
                "commandId": command_id,
                "name": name,
                "args": raw_input,
                "source": {"kind": "user"},
            }),
        )
        .await?;
        if selection == Selection::Committed {
            append_host_event(context, session_id, "plan/mode", json!({"active": active})).await?;
            append_host_event(context, session_id, "user/message", plan_narration(active)).await?;
        }
        append_host_event(
            context,
            session_id,
            "command/done",
            json!({"commandId": command_id, "kind": "success", "text": text}),
        )
        .await?;
    }
    if active && !message.is_empty() {
        // The instruction becomes the next request under plan guidance.
        submit_message(context, session_id, &message, &format!("plan-{command_id}")).await?;
    }
    Ok(json!({
        "commandId": command_id,
        "result": {"kind": "success", "text": text},
    }))
}

async fn execute_feedback(
    context: &ApiContext,
    session_id: &str,
    message: &str,
    has_attachments: bool,
) -> Result<Value, RpcError> {
    // Resolve before logging: an unknown session must not create an identity or a command.
    stored_session(context, session_id)?;
    let id = command_id();
    append_host_event(
        context,
        session_id,
        "command/run",
        json!({
            "commandId": id, "name": "feedback", "source": {"kind": "user"}
        }),
    )
    .await?;
    let (kind, text) = if has_attachments {
        ("error", "/feedback does not accept attachments".to_owned())
    } else if message.is_empty() {
        (
            "error",
            "Feedback text is required. Usage: /feedback <text>".to_owned(),
        )
    } else {
        append_host_event(
            context,
            session_id,
            "feedback/record",
            json!({"text": message}),
        )
        .await?;
        (
            "success",
            format!(
                "Feedback recorded for session {session_id}\nAnonymous user: {}.",
                anonymous_id(&context.home)
            ),
        )
    };
    append_host_event(
        context,
        session_id,
        "command/done",
        json!({"commandId": id, "kind": kind, "text": text}),
    )
    .await?;
    Ok(json!({"commandId": id, "result": {"kind": kind, "text": text}}))
}

/// Reuse the Node host's per-home anonymous identifier when present.
fn anonymous_id(home: &std::path::Path) -> String {
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::{Mutex, OnceLock};
    static IDS: OnceLock<Mutex<HashMap<std::path::PathBuf, String>>> = OnceLock::new();
    let mut ids = IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    if let Some(id) = ids.get(home) {
        return id.clone();
    }
    let file = home.join(".anonymous-user-id");
    let read = || {
        std::fs::read_to_string(&file)
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| {
                s.len() == 36
                    && s.bytes().enumerate().all(|(i, c)| {
                        if [8, 13, 18, 23].contains(&i) {
                            c == b'-'
                        } else {
                            c.is_ascii_hexdigit()
                        }
                    })
            })
    };
    let id = read().unwrap_or_else(|| {
        let mut bytes = random::<[u8; 16]>();
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let created = format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
        let _ = std::fs::create_dir_all(home);
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&file) {
            Ok(mut output) => { let _ = writeln!(output, "{created}"); created }
            Err(_) => read().unwrap_or(created),
        }
    });
    ids.insert(home.to_path_buf(), id.clone());
    id
}

/// Commit, queue, cancel, or ignore one plan-mode selection.
fn select(context: &ApiContext, session_id: &str, logged: bool, active: bool) -> Selection {
    let target = context.plan_pending(session_id).unwrap_or(logged);
    if active == target {
        return Selection::Noop;
    }
    if context.session_running(session_id) {
        return if logged == active {
            Selection::Cancelled
        } else {
            Selection::Queued
        };
    }
    if logged == active {
        context.take_plan_pending(session_id);
        return Selection::Cancelled;
    }
    Selection::Committed
}

/// The visible result text for one selection.
fn notice(selection: Selection, active: bool, logged: bool) -> &'static str {
    match (selection, active, logged) {
        (Selection::Committed, true, _) => "Plan mode on. Use /plan off to leave.",
        (Selection::Committed, false, _) => "Plan mode off.",
        (Selection::Cancelled, false, false) => "Plan mode entry cancelled.",
        (Selection::Queued | Selection::Noop | Selection::Cancelled, true, _) => {
            "Entering plan mode (applies from the next step). Use /plan off to leave."
        }
        (_, false, true) => "Leaving plan mode (applies from the next step).",
        (_, false, false) => "Plan mode is already inactive.",
    }
}

/// Submit the instruction that accompanies `/plan <message>` as a normal turn.
///
/// The instruction goes through the same admission path as a prompt, so the
/// turn it starts runs under the plan guidance already in the log.
async fn submit_message(
    context: &Arc<ApiContext>,
    session_id: &str,
    message: &str,
    request_id: &str,
) -> Result<(), RpcError> {
    let payload = json!({"args": {"request": {
        "sessionId": session_id,
        "requestId": request_id,
        "mode": "queue",
        "content": [{"type": "text", "text": message}],
    }}});
    crate::api::prompt(Arc::clone(context), payload)
        .await
        .map(|_| ())
}

fn parse_command(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix('/')?;
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let name = &rest[..end];
    let mut bytes = name.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        || !bytes
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte))
    {
        return None;
    }
    Some((name, &rest[end..]))
}

fn command_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut bytes = random::<[u8; 16]>();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "cmd-{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}-{}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        NEXT.fetch_add(1, Ordering::Relaxed) + 1
    )
}

#[cfg(test)]
mod feedback_tests {
    use super::*;
    use dsh_session_format::{Header, HeaderTag, SESSION_FORMAT_VERSION};

    #[tokio::test]
    async fn feedback_command_is_discoverable_and_records_private_input_once() {
        let home = std::env::temp_dir().join(format!(
            "dsh-feedback-command-{}-{}",
            std::process::id(),
            random::<u64>()
        ));
        let cx = ApiContext::new("http://localhost", "test", &home);
        let id = "feedback-command";
        drop(
            cx.store()
                .create(Header {
                    kind: HeaderTag::Session,
                    version: SESSION_FORMAT_VERSION,
                    id: id.into(),
                    created_at: 1,
                    cwd: None,
                    parent_session: None,
                    is_seeded: false,
                    origin: None,
                    delegation_depth: 0,
                    agent_preset: None,
                })
                .unwrap(),
        );
        assert!(descriptors()
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["name"] == "feedback"));
        let outcome = execute(&cx, &json!({"agentId": id, "line": "/feedback   hello  "}))
            .await
            .unwrap();
        assert_eq!(outcome["result"]["kind"], "success");
        let events = stored_session(&cx, id).unwrap().events;
        assert_eq!(
            events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["command/run", "feedback/record", "command/done"]
        );
        assert!(events[0].data.get("args").is_none());
        assert_eq!(events[1].data, json!({"text": "hello"}));
        let identity = std::fs::read_to_string(home.join(".anonymous-user-id")).unwrap();
        assert!(outcome["result"]["text"]
            .as_str()
            .unwrap()
            .contains(identity.trim()));
        let empty = execute(&cx, &json!({"agentId": id, "line": "/feedback"}))
            .await
            .unwrap();
        assert_eq!(empty["result"]["kind"], "error");
        let events = stored_session(&cx, id).unwrap().events;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "feedback/record")
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(home);
    }
}
