//! Message feedback RPCs backed by the Session event log.

use crate::api::{append_host_event, request_of, rpc_error, ApiContext};
use dsh_gateway::{Gateway, RpcError};
use dsh_session_persistence::Stored;
use rand::random;
use serde_json::{json, Value};
use std::sync::Arc;

const MAX_NOTE_BYTES: usize = 8192;
const CATEGORIES: &[&str] = &[
    "task-result",
    "instruction-following",
    "product-interaction",
    "service-stability",
    "resource-cost",
    "security-privacy-permission",
    "other",
];

pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    let session_context = Arc::clone(&context);
    gateway.rpc("sessionFeedback/record", move |payload| {
        let context = Arc::clone(&session_context);
        async move { record_session_feedback(&context, &payload).await }
    });
    let list_context = Arc::clone(&context);
    gateway.rpc("messageFeedback/list", move |payload| {
        let context = Arc::clone(&list_context);
        async move {
            let session_id = required_session_id(&payload)?;
            tokio::task::spawn_blocking(move || list_feedback(&context, &session_id))
                .await
                .map_err(|error| rpc_error("gateway/internal", error.to_string()))?
        }
    });

    let put_context = Arc::clone(&context);
    gateway.rpc("messageFeedback/put", move |payload| {
        let context = Arc::clone(&put_context);
        async move { put_feedback(context, payload).await }
    });

    gateway.rpc("messageFeedback/delete", move |payload| {
        let context = Arc::clone(&context);
        async move { delete_feedback(context, payload).await }
    });
}

/// Record the log-only session feedback used by the session feedback plugin.
async fn record_session_feedback(context: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let session_id = required_text(request, "sessionId")?;
    let text = match request.get("text") {
        None => None,
        Some(Value::String(value)) => Some(value.trim()).filter(|value| !value.is_empty()),
        Some(_) => return Err(rpc_error("gateway/bad-request", "text must be a string")),
    };
    let category = match request.get("category") {
        None => None,
        Some(Value::String(value)) if CATEGORIES.contains(&value.as_str()) => Some(value.as_str()),
        Some(_) => {
            return Err(rpc_error(
                "gateway/bad-request",
                "category is not a supported feedback category",
            ))
        }
    };
    match context.store().metadata_by_id(session_id) {
        Ok(_) => {}
        Err(dsh_session_persistence::StoreError::NotFound(_)) => {
            return Ok(rejected(
                json!({"code": "session-not-found", "sessionId": session_id}),
            ))
        }
        Err(error) => return Err(rpc_error("session/io", error.to_string())),
    }
    let mut entry = serde_json::Map::new();
    if let Some(text) = text {
        entry.insert("text".into(), json!(text));
    }
    if let Some(category) = category {
        entry.insert("category".into(), json!(category));
    }
    append_host_event(context, session_id, "feedback/record", Value::Object(entry)).await?;
    Ok(success(json!({"recorded": true})))
}

fn list_feedback(context: &ApiContext, session_id: &str) -> Result<Value, RpcError> {
    let Some(stored) = stored_session(context, session_id) else {
        return Ok(rejected(json!({
            "code": "session-not-found",
            "sessionId": session_id,
        })));
    };
    Ok(success(
        json!({"items": current_items(&stored, session_id)}),
    ))
}

async fn put_feedback(context: Arc<ApiContext>, payload: Value) -> Result<Value, RpcError> {
    let request = request_of(&payload);
    let session_id = required_session_id(&payload)?;
    let message_id = required_text(request, "messageId")?;
    let rating = required_text(request, "rating")?;
    if !matches!(rating, "positive" | "negative") {
        return Err(rpc_error(
            "gateway/bad-request",
            "rating must be positive or negative",
        ));
    }
    let if_version = match request.get("ifVersion") {
        Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        _ => {
            return Err(rpc_error(
                "gateway/bad-request",
                "ifVersion must be a version string or null",
            ))
        }
    };
    let note = match request.get("note") {
        None => None,
        Some(Value::String(value)) => Some(value.as_str()),
        Some(_) => {
            return Err(rpc_error(
                "gateway/bad-request",
                "note must be a string when provided",
            ))
        }
    };
    if note.is_some_and(|value| value.trim().is_empty()) {
        return Ok(rejected(json!({"code": "note-blank"})));
    }
    if let Some(note) = note.filter(|value| value.len() > MAX_NOTE_BYTES) {
        return Ok(rejected(json!({
            "code": "note-too-large",
            "maxBytes": MAX_NOTE_BYTES,
            "actualBytes": note.len(),
        })));
    }
    let category = match request.get("category") {
        None => None,
        Some(Value::String(value)) => Some(value.as_str()),
        Some(_) => {
            return Err(rpc_error(
                "gateway/bad-request",
                "category must be a string when provided",
            ))
        }
    };
    if category.is_some_and(|value| !CATEGORIES.contains(&value)) {
        return Err(rpc_error(
            "gateway/bad-request",
            "category is not a supported feedback category",
        ));
    }

    let _write = context.feedback_write().await;
    let read_context = Arc::clone(&context);
    let read_id = session_id.clone();
    let stored = tokio::task::spawn_blocking(move || stored_session(&read_context, &read_id))
        .await
        .map_err(|error| rpc_error("gateway/internal", error.to_string()))?;
    let Some(stored) = stored else {
        return Ok(rejected(json!({
            "code": "session-not-found",
            "sessionId": session_id,
        })));
    };
    if !contains_assistant_message(&stored, message_id) {
        return Ok(rejected(json!({
            "code": "target-not-found",
            "sessionId": session_id,
            "messageId": message_id,
        })));
    }
    let items = current_items(&stored, &session_id);
    let existing = items
        .iter()
        .find(|item| item.get("messageId").and_then(Value::as_str) == Some(message_id));
    if if_version != existing.and_then(|item| item.get("version").and_then(Value::as_str)) {
        return Ok(rejected(json!({
            "code": "version-conflict",
            "current": existing.cloned().unwrap_or(Value::Null),
        })));
    }

    let now = now_ms();
    let mut item = json!({
        "messageId": message_id,
        "rating": rating,
        "version": new_version(),
        "createdAt": existing
            .and_then(|value| value.get("createdAt"))
            .and_then(Value::as_u64)
            .unwrap_or(now),
        "updatedAt": existing
            .and_then(|value| value.get("updatedAt"))
            .and_then(Value::as_u64)
            .map_or(now, |previous| now.max(previous)),
    });
    if let Some(note) = note {
        item["note"] = json!(note);
    }
    if let Some(category) = category {
        item["category"] = json!(category);
    }
    if existing.is_some_and(|previous| {
        previous.get("rating").and_then(Value::as_str) == Some(rating)
            && previous.get("note").and_then(Value::as_str) == note
            && previous.get("category").and_then(Value::as_str) == category
    }) {
        return Ok(success(existing.cloned().unwrap_or(Value::Null)));
    }

    append_host_event(
        &context,
        &session_id,
        "feedback/message-put",
        json!({"sessionId": session_id, "item": item}),
    )
    .await?;
    Ok(success(item))
}

async fn delete_feedback(context: Arc<ApiContext>, payload: Value) -> Result<Value, RpcError> {
    let request = request_of(&payload);
    let session_id = required_session_id(&payload)?;
    let message_id = required_text(request, "messageId")?;
    let version = required_text(request, "ifVersion")?;
    let _write = context.feedback_write().await;
    let read_context = Arc::clone(&context);
    let read_id = session_id.clone();
    let stored = tokio::task::spawn_blocking(move || stored_session(&read_context, &read_id))
        .await
        .map_err(|error| rpc_error("gateway/internal", error.to_string()))?;
    let Some(stored) = stored else {
        return Ok(rejected(json!({
            "code": "session-not-found",
            "sessionId": session_id,
        })));
    };
    let items = current_items(&stored, &session_id);
    let existing = items
        .iter()
        .find(|item| item.get("messageId").and_then(Value::as_str) == Some(message_id));
    if let Some(existing) = existing {
        if existing.get("version").and_then(Value::as_str) != Some(version) {
            return Ok(rejected(json!({
                "code": "version-conflict",
                "current": existing,
            })));
        }
        append_host_event(
            &context,
            &session_id,
            "feedback/message-delete",
            json!({"sessionId": session_id, "messageId": message_id}),
        )
        .await?;
    }
    Ok(success(json!({"absent": true})))
}

fn stored_session(context: &ApiContext, session_id: &str) -> Option<Stored> {
    context
        .store()
        .read_all()
        .into_iter()
        .find(|stored| stored.header.id == session_id)
}

fn current_items(stored: &Stored, session_id: &str) -> Vec<Value> {
    let mut items = Vec::new();
    for event in &stored.events {
        match event.kind.as_str() {
            "feedback/message-put"
                if event.data.get("sessionId").and_then(Value::as_str) == Some(session_id) =>
            {
                let Some(item) = event.data.get("item") else {
                    continue;
                };
                let id = item.get("messageId").and_then(Value::as_str);
                if let Some(index) = items.iter().position(|current: &Value| {
                    current.get("messageId").and_then(Value::as_str) == id
                }) {
                    items[index] = item.clone();
                } else {
                    items.push(item.clone());
                }
            }
            "feedback/message-delete"
                if event.data.get("sessionId").and_then(Value::as_str) == Some(session_id) =>
            {
                if let Some(id) = event.data.get("messageId").and_then(Value::as_str) {
                    items.retain(|item: &Value| {
                        item.get("messageId").and_then(Value::as_str) != Some(id)
                    });
                }
            }
            _ => {}
        }
    }
    items
}

fn contains_assistant_message(stored: &Stored, message_id: &str) -> bool {
    stored.events.iter().any(|event| {
        event.kind == "assistant/message"
            && event
                .extra
                .get("surfaceOp")
                .is_none_or(|surface_op| surface_op == "append")
            && event
                .data
                .get("message")
                .and_then(|message| message.get("id"))
                .and_then(Value::as_str)
                == Some(message_id)
    })
}

fn required_session_id(payload: &Value) -> Result<String, RpcError> {
    let request = request_of(payload);
    required_text(request, "sessionId").map(str::to_owned)
}

fn required_text<'a>(request: &'a Value, field: &str) -> Result<&'a str, RpcError> {
    request
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", format!("{field} must not be empty")))
}

fn success(value: Value) -> Value {
    json!({"ok": true, "value": value})
}

fn rejected(error: Value) -> Value {
    json!({"ok": false, "error": error})
}

fn new_version() -> String {
    let mut bytes = random::<[u8; 16]>();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod session_feedback_tests {
    use super::*;
    use dsh_session_format::{Header, HeaderTag, SESSION_FORMAT_VERSION};

    #[tokio::test]
    async fn records_trimmed_feedback_and_handles_unknown_sessions() {
        let home = std::env::temp_dir().join(format!(
            "dsh-session-feedback-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let cx = ApiContext::new("http://localhost", "test", &home);
        drop(
            cx.store()
                .create(Header {
                    kind: HeaderTag::Session,
                    version: SESSION_FORMAT_VERSION,
                    id: "feedback-session".into(),
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

        let result = record_session_feedback(
            &cx,
            &json!({"sessionId": "feedback-session", "text": "  useful  ", "category": "other"}),
        )
        .await
        .unwrap();
        assert_eq!(result, json!({"ok": true, "value": {"recorded": true}}));
        record_session_feedback(&cx, &json!({"sessionId": "feedback-session", "text": "  "}))
            .await
            .unwrap();
        let stored = crate::api::stored_session(&cx, "feedback-session").unwrap();
        assert_eq!(stored.events.len(), 2);
        assert_eq!(stored.events[0].kind, "feedback/record");
        assert_eq!(
            stored.events[0].data,
            json!({"text": "useful", "category": "other"})
        );
        assert_eq!(stored.events[1].data, json!({}));
        let missing = record_session_feedback(&cx, &json!({"sessionId": "missing"}))
            .await
            .unwrap();
        assert_eq!(
            missing,
            json!({"ok": false, "error": {"code": "session-not-found", "sessionId": "missing"}})
        );
        assert!(record_session_feedback(
            &cx,
            &json!({"sessionId": "feedback-session", "category": "invalid"})
        )
        .await
        .is_err());
        assert_eq!(
            crate::api::stored_session(&cx, "feedback-session")
                .unwrap()
                .events
                .len(),
            2
        );
        let _ = std::fs::remove_dir_all(home);
    }
}
