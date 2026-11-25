//! Durable Agent Team mode selection for Session roots.

use crate::api::{append_host_event, request_of, rpc_error, stored_session, ApiContext};
use dsh_gateway::{Gateway, RpcError};
use serde_json::{json, Value};
use std::sync::Arc;

pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    gateway.rpc("agentTeams/setEnabled", move |payload| {
        let context = Arc::clone(&context);
        async move { set_enabled(&context, &payload).await }
    });
}

async fn set_enabled(context: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let session_id = request
        .get("agentId")
        .or_else(|| request.get("sessionId"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "agentId must not be empty"))?;
    let enabled = request
        .get("enabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| rpc_error("gateway/bad-request", "enabled must be a boolean"))?;
    let stored = stored_session(context, session_id)?;
    if stored.header.origin == Some(dsh_session_format::Origin::Subagent)
        || stored.header.parent_session.is_some()
    {
        return Err(rpc_error(
            "agentTeams/lead-required",
            "Team mode can only be changed from the root Session",
        ));
    }
    if team_enabled(&stored.events) != enabled {
        append_host_event(
            context,
            session_id,
            "team/mode",
            json!({"enabled": enabled}),
        )
        .await?;
    }
    Ok(Value::Null)
}

fn team_enabled(events: &[dsh_session_format::Event]) -> bool {
    events
        .iter()
        .rev()
        .find(|event| event.kind == "team/mode")
        .and_then(|event| event.data.get("enabled"))
        .and_then(Value::as_bool)
        .or_else(|| {
            events
                .iter()
                .any(|event| event.kind == "team/task")
                .then_some(true)
        })
        .unwrap_or(false)
}
