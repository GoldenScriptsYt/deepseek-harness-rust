//! `exit_plan_mode` tool. Port of `plan/plan-mode`: present the plan for one
//! review decision, then leave plan mode or return the user's feedback.

use crate::ask_user_question::{AskOutcome, QuestionAsker, QuestionRequest};
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::sync::Arc;

/// Tool name the plan-mode guidance tells the model to call.
pub const EXIT_PLAN_MODE: &str = "exit_plan_mode";
/// Question id the review decision is keyed by.
pub const REVIEW_ID: &str = "plan-review";
/// Option that leaves plan mode.
pub const APPROVE_LABEL: &str = "Approve";
/// Option that keeps plan mode and returns feedback.
pub const KEEP_PLANNING_LABEL: &str = "Keep planning";

const DESCRIPTION: &str = "Present the complete plan for the user's review and leave plan mode when it is approved. \
    Available only while plan mode is active. Make this the only and final tool call in the response. \
    On rejection, revise the plan using the returned feedback and present it again.";

/// `exit_plan_mode`: ask the user to approve the plan and leave plan mode.
pub struct ExitPlanMode {
    asker: Arc<dyn QuestionAsker>,
}

impl ExitPlanMode {
    /// Build the tool around the Host's question channel.
    /// @param asker - Delivery of the review question.
    #[must_use]
    pub fn new(asker: Arc<dyn QuestionAsker>) -> Self {
        Self { asker }
    }
}

/// Whether a Session log says plan mode is active.
/// @param events - Committed Session events in log order.
/// @returns The last committed `plan/mode` selection, defaulting to inactive.
#[must_use]
pub fn plan_mode_active(events: &[dsh_session_format::Event]) -> bool {
    events
        .iter()
        .rev()
        .find(|event| event.kind == "plan/mode")
        .and_then(|event| event.data.get("active").and_then(Value::as_bool))
        .unwrap_or(false)
}

/// The review question presented for one plan.
/// @param plan - Complete plan markdown.
/// @param call_id - Tool call the review belongs to.
/// @returns The single-question request the client renders as a plan review.
#[must_use]
pub fn review_question(plan: &str, call_id: &str) -> Value {
    json!({
        "id": REVIEW_ID,
        "header": "Plan review",
        "question": "Approve this plan and leave plan mode?",
        "detail": plan,
        "options": [
            {"label": APPROVE_LABEL, "description": "Leave plan mode; the plan is carried out from the next step."},
            {"label": KEEP_PLANNING_LABEL, "description": "Stay in plan mode; feedback goes back to the model."}
        ],
        "intent": {"kind": "plan-review", "approve": APPROVE_LABEL, "callId": call_id}
    })
}

impl Tool for ExitPlanMode {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: EXIT_PLAN_MODE.into(),
            description: DESCRIPTION.into(),
            input_schema: json!({"type":"object","additionalProperties":false,"properties":{
                "plan":{"type":"string","description":"The complete plan, as markdown, starting with a # heading that names it."}},
                "required":["plan"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let plan = args.get("plan").and_then(Value::as_str).unwrap_or_default();
            if !plan.trim_start().starts_with('#') {
                return ToolOutput::err(format!(
                    "Error: {EXIT_PLAN_MODE} requires a non-empty markdown plan starting with a # heading"
                ));
            }
            let Some(session) = cx.session.as_ref() else {
                return ToolOutput::err(format!(
                    "Error: {EXIT_PLAN_MODE} requires a calling session"
                ));
            };
            let events = match session.events() {
                Ok(events) => events,
                Err(error) => return ToolOutput::err(format!("Error: {error}")),
            };
            if !plan_mode_active(&events) {
                return ToolOutput::err(format!(
                    "Error: {EXIT_PLAN_MODE} is only available in plan mode"
                ));
            }
            let question = review_question(plan, &cx.call_id);
            let questions = [question];
            let outcome = self
                .asker
                .ask(QuestionRequest {
                    questions: &questions,
                    cancel: &cx.cancel,
                    call_id: &cx.call_id,
                    timeout: None,
                })
                .await;
            let answer = match outcome {
                Ok(AskOutcome::Answered(answer)) => answer,
                Ok(AskOutcome::Pending) => {
                    return ToolOutput::err(format!(
                        "Error: the plan review for {EXIT_PLAN_MODE} did not return a decision"
                    ))
                }
                Err(error) => return ToolOutput::err(format!("Error: {error}")),
            };
            let item = answer
                .get("answers")
                .and_then(Value::as_array)
                .and_then(|answers| {
                    let mut matching = answers
                        .iter()
                        .filter(|item| item.get("id").and_then(Value::as_str) == Some(REVIEW_ID));
                    let first = matching.next()?;
                    matching.next().is_none().then_some(first)
                });
            let selected: Vec<&str> = item
                .and_then(|item| item.get("selected").and_then(Value::as_array))
                .map(|values| values.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let custom = item
                .and_then(|item| item.get("custom").and_then(Value::as_str))
                .unwrap_or_default();
            if selected.as_slice() != [APPROVE_LABEL] || !custom.is_empty() {
                let feedback = if custom.is_empty() {
                    "The user chose to keep planning; revise the plan and present it again."
                        .to_string()
                } else {
                    format!("The user chose to keep planning; their feedback: {custom}")
                };
                return ToolOutput::err(format!("Error: {feedback}"));
            }
            // The tool result narrates the transition, so the mode change is
            // log-only: it commits with this call's events, before the result.
            cx.events.append("plan/mode", json!({"active": false}));
            ToolOutput::ok(json!({"approved": true}).to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::Cancel;
    use dsh_session_format::Event;
    use std::sync::Mutex;

    struct Facts(Vec<Event>);

    impl dsh_core::SessionFacts for Facts {
        fn events(&self) -> Result<Vec<Event>, String> {
            Ok(self.0.clone())
        }
    }

    struct Fixed {
        answer: Value,
        seen: Mutex<Vec<(String, usize, bool)>>,
    }

    impl QuestionAsker for Fixed {
        fn ask<'a>(
            &'a self,
            request: QuestionRequest<'a>,
        ) -> BoxFuture<'a, Result<AskOutcome, String>> {
            self.seen.lock().unwrap().push((
                request.call_id.to_string(),
                request.questions.len(),
                request.timeout.is_none(),
            ));
            let answer = AskOutcome::Answered(self.answer.clone());
            Box::pin(async move { Ok(answer) })
        }
    }

    fn tool(fixed: &Arc<Fixed>) -> ExitPlanMode {
        let dynamic: Arc<dyn QuestionAsker> = fixed.clone();
        ExitPlanMode::new(dynamic)
    }

    fn approve() -> Value {
        json!({"answers":[{"id":"plan-review","selected":["Approve"]}]})
    }

    fn context(events: Vec<Event>) -> ToolContext {
        ToolContext {
            call_id: "call-plan".into(),
            session: Some(Arc::new(Facts(events))),
            ..ToolContext::default()
        }
    }

    fn mode_event(active: bool) -> Event {
        Event {
            kind: "plan/mode".into(),
            data: json!({"active": active}),
            extra: Default::default(),
        }
    }

    #[tokio::test]
    async fn an_approved_plan_leaves_plan_mode() {
        let fixed = Arc::new(Fixed {
            answer: approve(),
            seen: Mutex::new(Vec::new()),
        });
        let tool = tool(&fixed);
        let cx = context(vec![mode_event(true)]);
        let out = tool
            .call(json!({"plan": "# Ship it\n\nDo the work."}), &cx)
            .await;
        assert!(!out.is_error, "{}", out.text);
        assert_eq!(
            serde_json::from_str::<Value>(&out.text).unwrap(),
            json!({"approved": true})
        );
        let seen = fixed.seen.lock().unwrap();
        assert_eq!(seen[0].0, "call-plan");
        assert_eq!(seen[0].1, 1);
        assert!(seen[0].2, "the review waits indefinitely");
        let events = cx.events.drain();
        assert_eq!(events[0].0, "plan/mode");
        assert_eq!(events[0].1["active"], json!(false));
    }

    #[tokio::test]
    async fn keeping_planning_returns_the_feedback() {
        let fixed = Arc::new(Fixed {
            answer: json!({"answers":[{"id":"plan-review","selected":["Keep planning"],"custom":"cover rollback"}]}),
            seen: Mutex::new(Vec::new()),
        });
        let tool = tool(&fixed);
        let cx = context(vec![mode_event(true)]);
        let out = tool.call(json!({"plan": "# Plan\n"}), &cx).await;
        assert!(out.is_error);
        assert!(out.text.contains("cover rollback"), "{}", out.text);
        assert!(cx.events.drain().is_empty(), "the mode is unchanged");
    }

    #[tokio::test]
    async fn the_review_is_only_offered_in_plan_mode() {
        let fixed = Arc::new(Fixed {
            answer: approve(),
            seen: Mutex::new(Vec::new()),
        });
        let tool = tool(&fixed);
        let out = tool
            .call(
                json!({"plan": "# Plan\n"}),
                &context(vec![mode_event(false)]),
            )
            .await;
        assert!(out.is_error);
        assert!(out.text.contains("only available in plan mode"));
        assert!(fixed.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_plan_without_a_heading_is_rejected() {
        let fixed = Arc::new(Fixed {
            answer: approve(),
            seen: Mutex::new(Vec::new()),
        });
        let tool = tool(&fixed);
        let out = tool
            .call(
                json!({"plan": "just do it"}),
                &context(vec![mode_event(true)]),
            )
            .await;
        assert!(out.is_error);
        assert!(fixed.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn the_review_question_carries_the_plan_and_intent() {
        let question = review_question("# Plan\nbody", "call-3");
        assert_eq!(question["id"], json!(REVIEW_ID));
        assert_eq!(question["detail"], json!("# Plan\nbody"));
        assert_eq!(question["intent"]["kind"], json!("plan-review"));
        assert_eq!(question["intent"]["approve"], json!(APPROVE_LABEL));
        assert_eq!(question["options"][1]["label"], json!(KEEP_PLANNING_LABEL));
        let _ = Cancel::default();
    }
}
