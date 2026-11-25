//! Host RPCs for user questions: the pending list, the durable answer that
//! closes a continued call, the wait attachment, and the `$events` waterfall
//! outcome that answers a card this Host published.

use crate::api::{answer_question, request_of, rpc_error, ApiContext};
use crate::interaction::WaterfallOutcome;
use dsh_gateway::{Gateway, RpcError};
use futures_util::stream::{self, BoxStream};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// How often an attached wait re-reads whether it still runs.
const ATTACH_TICK: Duration = Duration::from_millis(20);

pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    gateway.rpc("userQuestions/pending", move |payload| {
        let request = request_of(&payload).clone();
        async move { pending(&request) }
    });
    gateway.rpc("userQuestions/answer", move |payload| {
        let context = context.clone();
        async move { answer(context, payload).await }
    });
    gateway.stream("userQuestions/attachWait", |payload, _cancel| {
        attach(&payload)
    });
    gateway.rpc("$events/result", move |payload| {
        let request = request_of(&payload).clone();
        async move { settle(&request) }
    });
}

/// Read the Session a question RPC names.
/// @param request - Request arguments.
/// @returns The Session id, or a bad-request error.
fn session_of(request: &Value) -> Result<String, RpcError> {
    if let Some(id) = request.get("sessionId").and_then(Value::as_str) {
        if !id.is_empty() {
            return Ok(id.to_owned());
        }
    }
    let agent = request.get("agent");
    for value in [
        agent.and_then(|agent| agent.as_str()),
        agent
            .and_then(|agent| agent.get("id"))
            .and_then(Value::as_str),
        agent
            .and_then(|agent| agent.get("sessionId"))
            .and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    {
        if !value.is_empty() {
            return Ok(value.to_owned());
        }
    }
    Err(rpc_error(
        "gateway/bad-request",
        "agent must name the Session that owns the question",
    ))
}

fn call_id_of(request: &Value) -> Result<&str, RpcError> {
    request
        .get("callId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "callId must not be empty"))
}

/// Settle one published waterfall with the Client's outcome.
fn settle(request: &Value) -> Result<Value, RpcError> {
    let hub = crate::interaction::hub()
        .ok_or_else(|| rpc_error("$events/unavailable", "this Host forwards no Client events"))?;
    let event_id = request
        .get("eventId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "eventId must not be empty"))?;
    let outcome = request
        .get("outcome")
        .ok_or_else(|| rpc_error("gateway/bad-request", "outcome is required"))?;
    let settled = match outcome.get("kind").and_then(Value::as_str) {
        Some("result") => {
            WaterfallOutcome::Value(outcome.get("value").cloned().unwrap_or(Value::Null))
        }
        Some("next") => WaterfallOutcome::Next,
        Some("rejected") => WaterfallOutcome::Rejected {
            message: outcome
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("the Client rejected the request")
                .to_string(),
            code: outcome
                .pointer("/error/code")
                .and_then(Value::as_str)
                .map(str::to_owned),
        },
        _ => {
            return Err(rpc_error(
                "gateway/bad-request",
                "outcome.kind must be result, next, or rejected",
            ))
        }
    };
    hub.settle(event_id, settled)
        .map_err(|error| rpc_error("$events/not-pending", error))?;
    Ok(Value::Null)
}

/// Every timed wait this Host can still take an answer for.
fn pending(_request: &Value) -> Result<Value, RpcError> {
    let hub = crate::interaction::hub()
        .ok_or_else(|| rpc_error("userQuestions/unavailable", "this Host asks no questions"))?;
    let mut questions = hub.pending_questions();
    questions.sort_by(|left, right| left["callId"].as_str().cmp(&right["callId"].as_str()));
    Ok(json!({ "questions": questions }))
}

/// Durably answer one continued timed question.
async fn answer(context: Arc<ApiContext>, payload: Value) -> Result<Value, RpcError> {
    answer_question(context, payload).await
}

/// Release one claim on a live timed wait.
struct Claim {
    hub: Arc<crate::interaction::WaterfallHub>,
    agent: String,
    call_id: String,
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.hub.release(&self.agent, &self.call_id);
    }
}

/// Attach one answer UI to a live timed wait.
///
/// The first frame carries the Host-computed remaining duration. The stream
/// stays open while the wait runs, so its claim keeps the Host timer paused, and
/// ends when the wait settles or the Client disconnects.
fn attach(payload: &Value) -> BoxStream<'static, Result<Value, RpcError>> {
    struct Attached {
        claim: Claim,
        first: Option<u64>,
    }

    let request = request_of(payload).clone();
    let Some(hub) = crate::interaction::hub() else {
        return Box::pin(stream::empty());
    };
    let agent = match session_of(&request) {
        Ok(agent) => agent,
        Err(error) => return crate::process_api::error_stream(error),
    };
    let call_id = match call_id_of(&request) {
        Ok(call_id) => call_id.to_owned(),
        Err(error) => return crate::process_api::error_stream(error),
    };
    if !hub.claim(&agent, &call_id) {
        return Box::pin(stream::empty());
    }
    let remaining = hub.remaining_ms(&agent, &call_id).unwrap_or(0);
    let start = Attached {
        claim: Claim {
            hub,
            agent,
            call_id,
        },
        first: Some(remaining),
    };
    Box::pin(stream::unfold(Some(start), |state| async move {
        let mut state = state?;
        if let Some(remaining) = state.first.take() {
            let frame = Ok(json!({ "remainingMs": remaining }));
            return Some((frame, Some(state)));
        }
        while state
            .claim
            .hub
            .is_waiting(&state.claim.agent, &state.claim.call_id)
        {
            tokio::time::sleep(ATTACH_TICK).await;
        }
        None
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_gateway() -> Gateway {
        Gateway::new("t", "h", None)
    }

    #[test]
    fn an_answer_without_a_call_id_is_rejected() {
        let error = call_id_of(&json!({ "answer": {} })).expect_err("callId is required");
        assert_eq!(error.code, "gateway/bad-request");
    }

    #[test]
    fn the_session_can_be_named_by_id_or_agent() {
        assert_eq!(session_of(&json!({"agent": "s1"})).unwrap(), "s1");
        assert_eq!(session_of(&json!({"agent": {"id": "s2"}})).unwrap(), "s2");
        assert_eq!(session_of(&json!({"sessionId": "s3"})).unwrap(), "s3");
        assert!(session_of(&json!({})).is_err());
    }

    #[tokio::test]
    async fn a_rejected_outcome_carries_its_code() {
        crate::interaction::install_with_gateway(&test_gateway());
        let hub = crate::interaction::hub().expect("installed hub");
        let (event_id, answer) = hub.publish("user-questions/request", "s1", json!({}));
        settle(&json!({
            "eventId": event_id,
            "outcome": {"kind": "rejected", "error": {"message": "too late", "code": crate::interaction::TIMED_OUT_CODE}}
        }))
        .expect("settled");
        match answer.await.expect("the waiter is still listening") {
            WaterfallOutcome::Rejected { message, code } => {
                assert_eq!(message, "too late");
                assert_eq!(code.as_deref(), Some(crate::interaction::TIMED_OUT_CODE));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn pending_lists_the_open_waits() {
        crate::interaction::install_with_gateway(&test_gateway());
        let value = pending(&json!({})).expect("the installed hub answers");
        assert!(value["questions"].is_array());
    }
    #[tokio::test]
    async fn an_attachment_reports_the_remaining_time_and_releases_its_claim() {
        use futures_util::StreamExt;

        crate::interaction::install_with_gateway(&test_gateway());
        let hub = crate::interaction::hub().expect("installed hub");
        let cancel = dsh_core::Cancel::default();
        let asking = {
            let hub = hub.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                hub.ask(
                    "s-attach",
                    json!([{"id": "q1", "question": "Which?"}]),
                    "call-attach",
                    Some(Duration::from_secs(5)),
                    &cancel,
                )
                .await
            })
        };
        for _ in 0..200 {
            if hub.is_waiting("s-attach", "call-attach") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(hub.is_waiting("s-attach", "call-attach"), "the wait opened");
        let mut frames = attach(&json!({"agent": "s-attach", "callId": "call-attach"}));
        let opening = frames.next().await.expect("one frame").expect("a frame");
        let remaining = opening["remainingMs"].as_u64().expect("milliseconds");
        assert!(
            (1..=5000).contains(&remaining),
            "unexpected remaining {remaining}"
        );
        drop(frames);
        assert!(
            hub.claim("s-attach", "call-attach"),
            "dropping the stream must release its claim"
        );
        hub.release("s-attach", "call-attach");
        cancel.cancel();
        assert!(
            asking.await.expect("the ask task finished").is_err(),
            "cancelling the turn ends the open wait"
        );
    }
}
