//! `get_goal`, `create_goal`, and `update_goal` tools. Port of `goal/tool-goal`:
//! the model-facing view over the durable same-session Goal domain.
//!
//! Each tool validates against the committed log the Host hands to it and asks the
//! owning Agent to commit the resulting `goal/change`, so the durable order stays
//! `tool/call`, `goal/change`, `tool/result`.

use dsh_core::{BoxFuture, SessionFacts, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use dsh_planning::{
    block_goal, complete_goal, create_goal, edit_goal, pause_goal, phase_name, random_goal_id,
    resume_goal, GoalBlockReason, GoalChange, GoalPhase, GoalProjection, GoalRef, GoalRefusal,
    GoalTimeline,
};
use serde_json::{json, Value};

const CREATE_DESCRIPTION: &str = "Create a persisted goal that keeps this session working across automatic continuation rounds. Use it when the direct human request is a long-running objective, even if the user did not say \"goal\"; not for single-turn work.";
const GET_DESCRIPTION: &str =
    "Read the current session goal, including the id and revision that update_goal requires.";
const UPDATE_DESCRIPTION: &str = "Change the current goal: edit its objective or round cap, pause it, resume it, mark it complete, or report it blocked.";

/// One millisecond clock reading for a Goal mutation.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

/// Read the committed log a goal tool validates against.
fn facts(cx: &ToolContext) -> Result<&dyn SessionFacts, ToolOutput> {
    cx.session
        .as_deref()
        .ok_or_else(|| ToolOutput::err("Error: goal tools require a Host session log"))
}

/// Fold the committed Goal out of the Session log.
fn current_goal(facts: &dyn SessionFacts) -> Result<(Option<GoalProjection>, bool), ToolOutput> {
    let events = facts
        .events()
        .map_err(|error| ToolOutput::err(format!("Error: {error}")))?;
    let mut timeline = GoalTimeline::new();
    for event in &events {
        // A rejected historical change leaves the last valid Goal in place.
        let _ = timeline.observe(event);
    }
    let projection = timeline.projection().cloned();
    let armed = projection.as_ref().is_some_and(|projection| {
        facts.goal_armed(projection.goal.id.as_str(), projection.goal.revision)
    });
    Ok((projection, armed))
}

/// Render the compact model-facing Goal value.
fn goal_value(projection: Option<&GoalProjection>, armed: bool) -> Value {
    match projection {
        None => json!({"goal": Value::Null}),
        Some(projection) => {
            let mut goal = json!({
                "id": projection.goal.id.as_str(),
                "revision": projection.goal.revision,
                "objective": projection.goal.objective,
                "phase": phase_name(projection.goal.phase),
                "roundsStarted": projection.rounds_started,
                "maxGoalRounds": projection.goal.max_goal_rounds,
            });
            if let Some(reason) = &projection.goal.blocked_reason {
                goal["blockedReason"] = json!({"code": reason.code, "message": reason.message});
            }
            json!({
                "goal": goal,
                "activation": if armed { "armed" } else { "disarmed" },
            })
        }
    }
}

/// Render one refusal as the model-visible tool failure.
fn refusal(error: GoalRefusal) -> ToolOutput {
    ToolOutput::err(format!("Error: {}: {}", error.code, error.message))
}

/// Commit one validated change and describe the state it produces.
fn commit(
    cx: &ToolContext,
    current: Option<&GoalProjection>,
    change: GoalChange,
    armed: Option<bool>,
) -> Result<Value, ToolOutput> {
    let payload = change
        .to_payload()
        .map_err(|error| ToolOutput::err(format!("Error: {error}")))?;
    let next = dsh_planning::apply_to(current, &change)
        .map_err(|error| ToolOutput::err(format!("Error: {error}")))?;
    cx.events.append("goal/change", payload);
    let armed = match armed {
        Some(armed) => armed,
        None => current.is_some_and(|projection| {
            cx.session.as_deref().is_some_and(|facts| {
                facts.goal_armed(projection.goal.id.as_str(), projection.goal.revision)
            })
        }),
    };
    Ok(goal_value(next.as_ref(), armed))
}

/// `get_goal`: the current durable Goal and its live activation.
pub struct GetGoal;

impl Tool for GetGoal {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "get_goal".into(),
            description: GET_DESCRIPTION.into(),
            input_schema: json!({"type":"object","additionalProperties":false,"properties":{}}),
        }
    }

    fn call<'a>(&'a self, _args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let facts = match facts(cx) {
                Ok(facts) => facts,
                Err(error) => return error,
            };
            match current_goal(facts) {
                Ok((projection, armed)) => {
                    ToolOutput::ok(goal_value(projection.as_ref(), armed).to_string())
                }
                Err(error) => error,
            }
        })
    }
}

/// `create_goal`: start an armed Goal over an absent or completed one.
pub struct CreateGoal;

impl Tool for CreateGoal {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "create_goal".into(),
            description: CREATE_DESCRIPTION.into(),
            input_schema: json!({"type":"object","additionalProperties":false,"properties":{
                "objective":{"type":"string","description":"The durable objective the session keeps working toward."},
                "max_goal_rounds":{"type":"integer","description":"Maximum automatic continuation rounds before the goal stops."}
            },"required":["objective"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let facts = match facts(cx) {
                Ok(facts) => facts,
                Err(error) => return error,
            };
            let objective = args.get("objective").and_then(Value::as_str).unwrap_or("");
            let max_goal_rounds = match args.get("max_goal_rounds") {
                None | Some(Value::Null) => None,
                Some(value) => match value.as_u64() {
                    Some(rounds) => Some(rounds),
                    None => {
                        return ToolOutput::err("Error: max_goal_rounds must be a positive integer")
                    }
                },
            };
            let (current, _) = match current_goal(facts) {
                Ok(goal) => goal,
                Err(error) => return error,
            };
            let change = match create_goal(
                current.as_ref(),
                random_goal_id(),
                objective,
                max_goal_rounds,
                now_ms(),
            ) {
                Ok(change) => change,
                Err(error) => return refusal(error),
            };
            match commit(cx, current.as_ref(), change, Some(true)) {
                Ok(value) => ToolOutput::ok(value.to_string()),
                Err(error) => error,
            }
        })
    }
}

/// `update_goal`: edit, pause, resume, complete, or block the current Goal.
pub struct UpdateGoal;

impl Tool for UpdateGoal {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "update_goal".into(),
            description: UPDATE_DESCRIPTION.into(),
            input_schema: json!({"type":"object","additionalProperties":false,"properties":{
                "goal_id":{"type":"string","description":"The current goal id from get_goal."},
                "revision":{"type":"integer","description":"The current goal revision from get_goal."},
                "action":{"type":"string","enum":["edit","pause","resume","complete","blocked"],"description":"The lifecycle change to apply."},
                "objective":{"type":"string","description":"Replacement objective for an edit."},
                "max_goal_rounds":{"type":"integer","description":"Replacement round cap for an edit."},
                "blocked_reason":{"type":"string","description":"Why the goal cannot continue, for a blocked report."}
            },"required":["goal_id","revision","action"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let facts = match facts(cx) {
                Ok(facts) => facts,
                Err(error) => return error,
            };
            let goal_id = match args.get("goal_id").and_then(Value::as_str) {
                Some(id) if !id.trim().is_empty() => id.to_string(),
                _ => return ToolOutput::err("Error: goal_id must be a non-empty string"),
            };
            let revision = match args.get("revision").and_then(Value::as_u64) {
                Some(revision) => revision,
                None => return ToolOutput::err("Error: revision must be a positive integer"),
            };
            let action = match args.get("action").and_then(Value::as_str) {
                Some(action) => action.to_string(),
                None => return ToolOutput::err("Error: action must be a string"),
            };
            let reference = match dsh_planning::GoalId::new(goal_id) {
                Ok(id) => GoalRef { id, revision },
                Err(error) => return ToolOutput::err(format!("Error: {error}")),
            };
            let (current, armed) = match current_goal(facts) {
                Ok(goal) => goal,
                Err(error) => return error,
            };
            let now = now_ms();
            if let Some(current) = &current {
                if action == "resume" && current.goal.phase == GoalPhase::Paused {
                    return ToolOutput::err(
                        "Error: GOAL_TOOL_RESUME_PAUSED: a paused goal can only be resumed by the user",
                    );
                }
            }
            let (change, armed_after) = match action.as_str() {
                "edit" => {
                    let objective = args.get("objective").and_then(Value::as_str);
                    let max_goal_rounds = match args.get("max_goal_rounds") {
                        None | Some(Value::Null) => None,
                        Some(value) => match value.as_u64() {
                            Some(rounds) => Some(rounds),
                            None => {
                                return ToolOutput::err(
                                    "Error: max_goal_rounds must be a positive integer",
                                )
                            }
                        },
                    };
                    (
                        edit_goal(
                            current.as_ref(),
                            &reference,
                            objective,
                            max_goal_rounds,
                            now,
                        ),
                        None,
                    )
                }
                "pause" => (pause_goal(current.as_ref(), &reference, now), Some(false)),
                "resume" => (
                    resume_goal(current.as_ref(), &reference, armed, now),
                    Some(true),
                ),
                "complete" => (complete_goal(current.as_ref(), &reference, now), Some(false)),
                "blocked" => {
                    let reason = args
                        .get("blocked_reason")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if reason.trim().is_empty() {
                        return ToolOutput::err(
                            "Error: blocked_reason is required when blocking a goal",
                        );
                    }
                    (
                        block_goal(
                            current.as_ref(),
                            &reference,
                            GoalBlockReason {
                                code: "model-reported".into(),
                                message: reason.trim().to_string(),
                            },
                            now,
                        ),
                        Some(false),
                    )
                }
                other => {
                    return ToolOutput::err(format!(
                        "Error: action must be one of edit, pause, resume, complete, blocked (got {other:?})"
                    ))
                }
            };
            let change = match change {
                Ok(change) => change,
                Err(error) => return refusal(error),
            };
            match commit(cx, current.as_ref(), change, armed_after) {
                Ok(value) => ToolOutput::ok(value.to_string()),
                Err(error) => error,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_session_format::Event;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Log {
        events: Mutex<Vec<Event>>,
        armed: Mutex<bool>,
    }

    impl Log {
        fn new() -> Self {
            Self::default()
        }

        fn armed(self, armed: bool) -> Self {
            *self.armed.lock().unwrap() = armed;
            self
        }
    }

    impl SessionFacts for Log {
        fn events(&self) -> Result<Vec<Event>, String> {
            Ok(self.events.lock().unwrap().clone())
        }

        fn goal_armed(&self, _id: &str, _revision: u64) -> bool {
            *self.armed.lock().unwrap()
        }
    }

    /// Drive one tool call to completion on a single-threaded runtime.
    fn run(future: impl std::future::Future<Output = ToolOutput>) -> ToolOutput {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a test runtime")
            .block_on(future)
    }

    fn context(log: std::sync::Arc<Log>) -> ToolContext {
        ToolContext {
            session: Some(log),
            ..ToolContext::default()
        }
    }

    /// Apply the committed events to the fake log, mirroring the Agent.
    fn settle(log: &Log, cx: &ToolContext) {
        let mut events = log.events.lock().unwrap();
        for (kind, data) in cx.events.drain() {
            let seq = events.len() as u64;
            let mut extra = serde_json::Map::new();
            extra.insert("seq".into(), json!(seq));
            extra.insert("time".into(), json!(1000 + seq));
            events.push(Event { kind, data, extra });
        }
    }

    #[test]
    fn create_then_read_returns_the_compact_goal_value() {
        let log = std::sync::Arc::new(Log::new());
        let cx = context(log.clone());
        let created = CreateGoal.call(json!({"objective": "ship it", "max_goal_rounds": 5}), &cx);
        let created = run(created);
        assert!(!created.is_error, "{}", created.text);
        let value: Value = serde_json::from_str(&created.text).expect("JSON");
        assert_eq!(value["goal"]["objective"], "ship it");
        assert_eq!(value["goal"]["maxGoalRounds"], 5);
        assert_eq!(value["activation"], "armed");
        settle(&log, &cx);
        let read = run(GetGoal.call(json!({}), &cx));
        let value: Value = serde_json::from_str(&read.text).expect("JSON");
        assert_eq!(value["goal"]["objective"], "ship it");
    }

    #[test]
    fn a_second_create_reports_the_existing_goal() {
        let log = std::sync::Arc::new(Log::new());
        let cx = context(log.clone());
        let first = run(CreateGoal.call(json!({"objective": "one"}), &cx));
        assert!(!first.is_error);
        settle(&log, &cx);
        let second = run(CreateGoal.call(json!({"objective": "two"}), &cx));
        assert!(second.is_error);
        assert!(
            second.text.contains("GOAL_ALREADY_EXISTS"),
            "{}",
            second.text
        );
    }

    #[test]
    fn update_validation_covers_actions_and_references() {
        let log = std::sync::Arc::new(Log::new().armed(true));
        let cx = context(log.clone());
        let created = run(CreateGoal.call(json!({"objective": "ship it"}), &cx));
        let value: Value = serde_json::from_str(&created.text).expect("JSON");
        let id = value["goal"]["id"].as_str().unwrap().to_string();
        settle(&log, &cx);
        let unknown = run(UpdateGoal.call(
            json!({"goal_id": id, "revision": 9, "action": "pause"}),
            &cx,
        ));
        assert!(
            unknown.text.contains("GOAL_STALE_REVISION"),
            "{}",
            unknown.text
        );
        let bad_action = run(UpdateGoal.call(
            json!({"goal_id": id, "revision": 1, "action": "rewind"}),
            &cx,
        ));
        assert!(bad_action.is_error);
        let paused = run(UpdateGoal.call(
            json!({"goal_id": id, "revision": 1, "action": "pause"}),
            &cx,
        ));
        assert!(!paused.is_error, "{}", paused.text);
        let paused_value: Value = serde_json::from_str(&paused.text).expect("JSON");
        assert_eq!(paused_value["goal"]["phase"], "paused");
        assert_eq!(paused_value["activation"], "disarmed");
        settle(&log, &cx);
        let resume = run(UpdateGoal.call(
            json!({"goal_id": id, "revision": 2, "action": "resume"}),
            &cx,
        ));
        assert!(
            resume.text.contains("GOAL_TOOL_RESUME_PAUSED"),
            "{}",
            resume.text
        );
    }

    #[test]
    fn tools_refuse_to_run_without_a_session_log() {
        let cx = ToolContext::default();
        let output = run(GetGoal.call(json!({}), &cx));
        assert!(output.is_error);
        assert!(output.text.contains("Host session log"));
    }

    #[test]
    fn tool_names_match_the_model_facing_surface() {
        assert_eq!(GetGoal.def().name, "get_goal");
        assert_eq!(CreateGoal.def().name, "create_goal");
        assert_eq!(UpdateGoal.def().name, "update_goal");
    }

    #[test]
    fn clear_goal_is_available_to_host_callers() {
        // The model-facing surface has no clear action; the Host RPCs use this builder.
        let change = dsh_planning::clear_goal(
            None,
            &GoalRef {
                id: dsh_planning::GoalId::new("goal-1").unwrap(),
                revision: 1,
            },
            10,
        );
        assert_eq!(change.unwrap_err().code, "GOAL_NOT_FOUND");
    }
}
