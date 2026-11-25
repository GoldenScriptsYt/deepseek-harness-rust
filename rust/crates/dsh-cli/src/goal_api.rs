//! Durable same-session Goals over the remote API: the `goals/*` methods.
//!
//! Lifecycle rules live in `dsh-planning` and are shared with the model-facing
//! goal tools; this module resolves remote arguments, folds the committed log,
//! and appends the resulting `goal/change`.

use crate::api::{append_host_event, now_ms, request_of, rpc_error, stored_session, ApiContext};
use dsh_gateway::{Gateway, RpcError};
use dsh_planning::{
    apply_to, clear_goal, complete_goal, create_goal, edit_goal, pause_goal, random_goal_id,
    resume_goal, GoalChange, GoalProjection, GoalRef, GoalRefusal, GoalTimeline,
};
use dsh_session_format::Event;
use serde_json::{json, Value};
use std::sync::Arc;

/// Register the Goal remote methods.
pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    let c = Arc::clone(&context);
    gateway.rpc("goals/get", move |payload| {
        let c = Arc::clone(&c);
        async move { get(&c, &payload) }
    });
    let c = Arc::clone(&context);
    gateway.rpc("goals/create", move |payload| {
        let c = Arc::clone(&c);
        async move { create(&c, &payload).await }
    });
    let c = Arc::clone(&context);
    gateway.rpc("goals/edit", move |payload| {
        let c = Arc::clone(&c);
        async move { edit(&c, &payload).await }
    });
    let c = Arc::clone(&context);
    gateway.rpc("goals/pause", move |payload| {
        let c = Arc::clone(&c);
        async move { pause(&c, &payload).await }
    });
    let c = Arc::clone(&context);
    gateway.rpc("goals/resume", move |payload| {
        let c = Arc::clone(&c);
        async move { resume(&c, &payload).await }
    });
    let c = Arc::clone(&context);
    gateway.rpc("goals/complete", move |payload| {
        let c = Arc::clone(&c);
        async move { complete(&c, &payload).await }
    });
    let c = Arc::clone(&context);
    gateway.rpc("goals/clear", move |payload| {
        let c = Arc::clone(&c);
        async move { clear(&c, &payload).await }
    });
}

/// Render one refusal with the stable code the client reads.
fn refusal(error: GoalRefusal) -> RpcError {
    rpc_error(error.code, error.message)
}

/// Fold the whole Session log, keeping the last valid Goal.
/// @param events - committed Session rows in order.
/// @returns The Goal timeline; a rejected row is skipped.
fn fold(events: &[Event]) -> GoalTimeline {
    let mut timeline = GoalTimeline::new();
    for event in events {
        let _ = timeline.observe(event);
    }
    timeline
}

/// Fold the whole Session log, refusing a corrupt Goal history.
/// @param events - committed Session rows in order.
/// @returns The Goal timeline or the durable corruption report.
fn fold_strict(events: &[Event]) -> Result<GoalTimeline, RpcError> {
    let mut timeline = GoalTimeline::new();
    for event in events {
        timeline
            .observe(event)
            .map_err(|error| rpc_error("GOAL_STATE_CORRUPT", error.to_string()))?;
    }
    Ok(timeline)
}

/// Render one durable Goal as the client-visible view.
/// @param projection - current durable Goal projection.
/// @param armed - process-local continuation activation for this revision.
/// @returns The `GoalView` fields the client reads.
fn view(projection: &GoalProjection, armed: bool) -> Value {
    // The remote view is the flattened snapshot; the durable projection keeps
    // its snapshot nested under `goal` for replay counters.
    let mut value = serde_json::to_value(&projection.goal).unwrap_or(Value::Null);
    value["roundsStarted"] = json!(projection.rounds_started);
    value["createdAt"] = json!(projection.created_at);
    value["updatedAt"] = json!(projection.updated_at);
    value["activation"] = json!(if armed { "armed" } else { "disarmed" });
    value
}

/// Positional remote arguments, also accepting a named request object.
/// @param payload - one gateway RPC payload.
/// @returns The argument list.
fn goal_args(payload: &Value) -> Vec<Value> {
    match request_of(payload) {
        Value::Array(args) => args.clone(),
        other => vec![other.clone()],
    }
}

/// Resolve the owning Session of a Goal request.
/// @param args - positional remote arguments.
/// @returns The Session identity.
fn session_argument(args: &[Value]) -> Result<String, RpcError> {
    args.first()
        .and_then(|first| {
            first.as_str().map(str::to_string).or_else(|| {
                ["sessionId", "agentId"]
                    .iter()
                    .find_map(|key| first.get(*key).and_then(Value::as_str).map(str::to_string))
            })
        })
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "goals requests require a sessionId"))
}

/// Resolve the compare-and-set Goal reference of a mutation.
/// @param args - positional remote arguments.
/// @returns The expected current revision.
fn ref_argument(args: &[Value]) -> Result<GoalRef, RpcError> {
    let value = args
        .get(1)
        .ok_or_else(|| rpc_error("gateway/bad-request", "goals mutations require a goal ref"))?;
    serde_json::from_value(value.clone())
        .map_err(|error| rpc_error("gateway/bad-request", format!("invalid goal ref: {error}")))
}

/// Read one request's optional round cap.
/// @param request - create or edit request object.
/// @returns The cap, `None` when absent, or the invalid-rounds error.
fn max_rounds_argument(request: &Value) -> Result<Option<u64>, RpcError> {
    match request.get("maxGoalRounds") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            rpc_error(
                "GOAL_INVALID_MAX_ROUNDS",
                "maxGoalRounds must be a positive safe integer",
            )
        }),
    }
}

/// Read the mutation time clamped past the current revision.
/// @param current - current durable Goal projection.
/// @returns Millisecond commit time.
fn mutation_time(current: Option<&GoalProjection>) -> u64 {
    current.map_or_else(now_ms, |current| now_ms().max(current.updated_at))
}

/// Commit one validated Goal change and return what the client reads next.
///
/// Activation is recorded by [`crate::api::note_goal_change`] when the appended
/// event publishes, so this function never sets it directly.
/// @param cx - running Host.
/// @param session_id - owning Session.
/// @param current - durable Goal the change was validated against.
/// @param change - whole-value mutation to append.
/// @returns The client-visible view, or the clear tombstone.
async fn commit(
    cx: &ApiContext,
    session_id: &str,
    current: Option<&GoalProjection>,
    change: GoalChange,
) -> Result<Value, RpcError> {
    let payload = change
        .to_payload()
        .map_err(|error| rpc_error("GOAL_STATE_CORRUPT", error.to_string()))?;
    let next = apply_to(current, &change)
        .map_err(|error| rpc_error("GOAL_STATE_CORRUPT", error.to_string()))?;
    let cleared = matches!(change, GoalChange::Clear(_));
    let reference = change.change_ref();
    append_host_event(cx, session_id, "goal/change", payload).await?;
    match next {
        Some(projection) => {
            let armed = cx.goal_armed(
                session_id,
                projection.goal.id.as_str(),
                projection.goal.revision,
            );
            Ok(view(&projection, armed))
        }
        None if cleared => Ok(json!({
            "id": reference.id.as_str(),
            "revision": reference.revision,
        })),
        None => Ok(Value::Null),
    }
}

/// `goals/get`: the current durable Goal with its live activation.
pub(crate) fn get(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let stored = stored_session(cx, &session_id)?;
    Ok(match fold(&stored.events).projection() {
        Some(projection) => {
            let armed = cx.goal_armed(
                &session_id,
                projection.goal.id.as_str(),
                projection.goal.revision,
            );
            view(projection, armed)
        }
        None => Value::Null,
    })
}

/// `goals/create`: create and arm a Goal, replacing only a completed one.
pub(crate) async fn create(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let request = args.get(1).cloned().unwrap_or_else(|| json!({}));
    let objective = request
        .get("objective")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let max_goal_rounds = max_rounds_argument(&request)?;
    let stored = stored_session(cx, &session_id)?;
    let timeline = fold_strict(&stored.events)?;
    let current = timeline.projection();
    let change = create_goal(
        current,
        random_goal_id(),
        objective,
        max_goal_rounds,
        now_ms(),
    )
    .map_err(refusal)?;
    let reference = change.change_ref();
    commit(cx, &session_id, current, change).await?;
    Ok(json!({"ref": {"id": reference.id.as_str(), "revision": reference.revision}}))
}

/// `goals/edit`: replace the objective and/or the round cap.
pub(crate) async fn edit(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let reference = ref_argument(&args)?;
    let request = args.get(2).cloned().unwrap_or_else(|| json!({}));
    let objective = request.get("objective").and_then(Value::as_str);
    let max_goal_rounds = max_rounds_argument(&request)?;
    let stored = stored_session(cx, &session_id)?;
    let timeline = fold_strict(&stored.events)?;
    let change = edit_goal(
        timeline.projection(),
        &reference,
        objective,
        max_goal_rounds,
        mutation_time(timeline.projection()),
    )
    .map_err(refusal)?;
    commit(cx, &session_id, timeline.projection(), change).await
}

/// `goals/pause`: stop automatic continuation.
pub(crate) async fn pause(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let reference = ref_argument(&args)?;
    let stored = stored_session(cx, &session_id)?;
    let timeline = fold_strict(&stored.events)?;
    let current = timeline.projection();
    let change = pause_goal(current, &reference, mutation_time(current)).map_err(refusal)?;
    commit(cx, &session_id, current, change).await
}

/// `goals/resume`: rearm a Goal the user asks to continue.
pub(crate) async fn resume(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let reference = ref_argument(&args)?;
    let stored = stored_session(cx, &session_id)?;
    let timeline = fold_strict(&stored.events)?;
    let current = timeline.projection();
    let armed = current.is_some_and(|projection| {
        cx.goal_armed(
            &session_id,
            projection.goal.id.as_str(),
            projection.goal.revision,
        )
    });
    let change =
        resume_goal(current, &reference, armed, mutation_time(current)).map_err(refusal)?;
    commit(cx, &session_id, current, change).await
}

/// `goals/complete`: retire the Goal.
pub(crate) async fn complete(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let reference = ref_argument(&args)?;
    let stored = stored_session(cx, &session_id)?;
    let timeline = fold_strict(&stored.events)?;
    let current = timeline.projection();
    let change = complete_goal(current, &reference, mutation_time(current)).map_err(refusal)?;
    commit(cx, &session_id, current, change).await
}

/// `goals/clear`: retain a durable tombstone and disarm the Goal.
pub(crate) async fn clear(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = goal_args(payload);
    let session_id = session_argument(&args)?;
    let reference = ref_argument(&args)?;
    let stored = stored_session(cx, &session_id)?;
    let timeline = fold_strict(&stored.events)?;
    let current = timeline.projection();
    let change = clear_goal(current, &reference, mutation_time(current)).map_err(refusal)?;
    commit(cx, &session_id, current, change).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_planning::phase_name;

    fn event(kind: &str, data: Value, seq: u64, time: u64) -> Event {
        let mut extra = serde_json::Map::new();
        extra.insert("seq".into(), json!(seq));
        extra.insert("time".into(), json!(time));
        Event {
            kind: kind.into(),
            data,
            extra,
        }
    }

    fn create_change() -> Value {
        json!({
            "kind": "goal/change", "version": 1, "operation": "create",
            "goal": {"id": "goal-1", "revision": 1, "objective": "ship it", "phase": "active", "maxGoalRounds": 4},
            "roundsStarted": 0, "createdAt": 100, "updatedAt": 100
        })
    }

    #[test]
    fn goal_arguments_accept_positional_and_named_requests() {
        assert_eq!(
            session_argument(&goal_args(&json!({"args": ["session-1"]}))).unwrap(),
            "session-1"
        );
        assert_eq!(
            session_argument(&goal_args(&json!({"args": {"sessionId": "session-2"}}))).unwrap(),
            "session-2"
        );
        assert!(session_argument(&goal_args(&json!({}))).is_err());
    }

    #[test]
    fn a_created_goal_reads_back_with_its_rounds_and_activation() {
        let timeline = fold(&[event("goal/change", create_change(), 0, 100)]);
        let projection = timeline.projection().expect("a current goal");
        assert_eq!(projection.goal.objective, "ship it");
        assert_eq!(projection.rounds_started, 0);
        let value = view(projection, true);
        assert_eq!(value["id"], "goal-1");
        assert_eq!(value["activation"], "armed");
        assert_eq!(value["roundsStarted"], 0);
    }

    #[test]
    fn admitted_goal_rounds_advance_the_projection() {
        let events = [
            event("goal/change", create_change(), 0, 100),
            event(
                "user/message",
                json!({
                    "content": [{"type": "text", "text": "continue"}],
                    "source": {"kind": "goal", "goalId": "goal-1", "revision": 1, "round": 1}
                }),
                1,
                101,
            ),
        ];
        let timeline = fold_strict(&events).expect("a valid history");
        assert_eq!(timeline.projection().unwrap().rounds_started, 1);
    }

    #[test]
    fn stale_references_are_named() {
        let timeline = fold(&[event("goal/change", create_change(), 0, 100)]);
        let current = timeline.projection().unwrap();
        let stale = GoalRef {
            id: current.goal.id.clone(),
            revision: 7,
        };
        let error = pause_goal(Some(current), &stale, 200).unwrap_err();
        assert_eq!(error.code, "GOAL_STALE_REVISION");
        assert_eq!(phase_name(current.goal.phase), "active");
    }

    #[test]
    fn rejected_rows_keep_the_last_valid_goal() {
        let mut timeline = GoalTimeline::new();
        timeline
            .observe(&event("goal/change", create_change(), 0, 100))
            .unwrap();
        let duplicate = event(
            "goal/change",
            json!({
                "kind": "goal/change", "version": 1, "operation": "create",
                "goal": {"id": "goal-2", "revision": 1, "objective": "second", "phase": "active", "maxGoalRounds": 2},
                "roundsStarted": 0, "createdAt": 200, "updatedAt": 200
            }),
            1,
            101,
        );
        assert!(timeline.observe(&duplicate).is_err());
        assert_eq!(timeline.projection().unwrap().goal.id.as_str(), "goal-1");
    }

    #[test]
    fn a_cleared_goal_leaves_a_tombstone_and_no_projection() {
        let events = [
            event("goal/change", create_change(), 0, 100),
            event(
                "goal/change",
                json!({
                    "kind": "goal/change", "version": 1, "operation": "clear",
                    "cleared": {"id": "goal-1", "revision": 2}, "clearedAt": 120
                }),
                1,
                120,
            ),
        ];
        let timeline = fold_strict(&events).expect("a valid history");
        assert!(timeline.projection().is_none());
    }
}
