//! `ask_user_question` tool. Port of `interaction/tool-ask-user`: the call blocks
//! until a client answers, or returns a pending result once a timed wait ends.

use dsh_core::{BoxFuture, Cancel, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::sync::Arc;

/// Request parameter whose presence marks a call as timed in the logged header.
pub const TIMED_WAIT_PARAMETER: &str = "timeout";
/// Foreground wait applied when the model omits `timeout`.
pub const DEFAULT_TIMEOUT_SECS: u32 = 120;
/// Largest accepted wait, in seconds.
pub const MAX_TIMEOUT_SECS: i64 = 2_147_483;

pub const SKIPPED_ANSWER_GUIDANCE: &str = "An answer item with empty selected and no custom means the user skipped that question: continue without it, do not ask it again, and do not treat the skip as permission.";

const DESCRIPTION: &str = concat!(
    "Ask brief, direct, self-contained questions about missing information, preferences, or decisions. ",
    "Use user-facing terms; assume no knowledge of background work or internal names. ",
    "Use distinct stable question IDs. An answer item with empty selected and no custom means the user skipped ",
    "that question: continue without it, do not ask it again, and do not treat the skip as permission. ",
    "A pending result instead means no answer batch arrived before the timeout and the user can still answer.",
);

/// Instruction the pending result carries in its `message` field.
pub const PENDING_NOTICE: &str = concat!(
    "No answer batch arrived before the timeout. This is pending, not a skipped answer. ",
    "Continue useful independent work. The user can still answer; their reply will be a user message ",
    "identified as answer_to_pending_question with this callId and the original questions. ",
    "Do not treat this as permission.",
);

/// One question set handed to the Host's interaction layer.
pub struct QuestionRequest<'a> {
    /// Validated `questions` array from the tool arguments.
    pub questions: &'a [Value],
    /// Cancellation of the calling turn.
    pub cancel: &'a Cancel,
    /// Tool call the questions belong to; keys the client card and its answer.
    pub call_id: &'a str,
    /// Foreground wait; `None` waits indefinitely.
    pub timeout: Option<std::time::Duration>,
}

/// First settlement of one question call.
#[derive(Debug, Clone)]
pub enum AskOutcome {
    /// The client submitted an answer batch.
    Answered(Value),
    /// The foreground wait ended; the questions stay answerable by `call_id`.
    Pending,
}

/// Host-provided delivery of one question set to a user and collection of the answer.
pub trait QuestionAsker: Send + Sync {
    /// Show the questions and wait for the first settlement.
    /// @param request - The question set to ask.
    /// @returns The client's answer payload or the pending marker, or failure text.
    fn ask<'a>(&'a self, request: QuestionRequest<'a>)
        -> BoxFuture<'a, Result<AskOutcome, String>>;
}

/// `ask_user_question`: ask one batch of questions and return its answer or pending marker.
pub struct AskUserQuestion {
    asker: Arc<dyn QuestionAsker>,
}

impl AskUserQuestion {
    /// Build the tool around one Host asker.
    /// @param asker - Delivery of question sets to the user.
    #[must_use]
    pub fn new(asker: Arc<dyn QuestionAsker>) -> Self {
        Self { asker }
    }

    /// Validate `timeout` and turn it into a duration; `-1` means indefinite.
    /// @param timeout - Raw `timeout` argument.
    /// @returns The foreground wait, or a model-visible rejection.
    pub fn timeout(timeout: Option<i64>) -> Result<Option<std::time::Duration>, String> {
        let seconds = timeout.unwrap_or(i64::from(DEFAULT_TIMEOUT_SECS));
        if seconds == -1 {
            return Ok(None);
        }
        if !(1..=MAX_TIMEOUT_SECS).contains(&seconds) {
            return Err(format!(
                "timeout must be -1 or a positive integer up to {MAX_TIMEOUT_SECS} seconds"
            ));
        }
        Ok(Some(std::time::Duration::from_secs(seconds as u64)))
    }

    fn questions(args: &Value) -> Result<&[Value], String> {
        let questions = args
            .get("questions")
            .and_then(Value::as_array)
            .ok_or_else(|| "questions must be an array".to_string())?;
        if questions.is_empty() {
            return Err("ask_user_question requires at least one question".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for question in questions {
            let id = question
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| "each question needs a non-empty string id".to_string())?;
            if !seen.insert(id) {
                return Err(format!(
                    "question id {id:?} must be unique within this call"
                ));
            }
            let text = question
                .get("question")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty());
            if text.is_none() {
                return Err(format!("question {id:?} needs non-empty text"));
            }
        }
        Ok(questions)
    }
}

/// Wire schema of the timed question tool.
///
/// The `timeout` property is what the `userQuestions` projection reads out of a
/// logged request header, so a timed call stays distinguishable from a legacy one.
/// @returns The tool input schema.
#[must_use]
pub fn ask_schema() -> Value {
    let mut schema = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "questions": {
                "type": "array",
                "description": "Questions to ask the user.",
                "items": {
                    "type": "object",
                    "additionalProperties": true,
                    "properties": {
                        "id": {"type": "string", "description": "Stable id for this question; echoed in the answer."},
                        "question": {"type": "string", "description": "The specific question to ask the user."},
                        "header": {"type": "string", "description": "Optional short heading for the question, such as \"Confirm\" or \"Choose Mode\"."},
                        "options": {
                            "type": "array",
                            "description": "Optional choices to show the user. If you recommend one, put it first and append \"(Recommended)\" to that label.",
                            "items": {
                                "type": "object",
                                "additionalProperties": true,
                                "properties": {
                                    "label": {"type": "string", "description": "Short user-facing option label."},
                                    "description": {"type": "string", "description": "One sentence explaining the tradeoff or impact."}
                                },
                                "required": ["label"]
                            }
                        },
                        "multi_select": {"type": "boolean", "description": "Whether the user may select more than one option. Defaults to false."}
                    },
                    "required": ["id", "question"]
                }
            }
        },
        "required": ["questions"]
    });
    schema["properties"][TIMED_WAIT_PARAMETER] = json!({
        "type": "integer",
        "description": "Wait seconds for the entire batch (default 120); omit unless the user specifies a duration. Use -1 only when an answer is required before proceeding."
    });
    schema
}

impl Tool for AskUserQuestion {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "ask_user_question".into(),
            description: DESCRIPTION.into(),
            input_schema: ask_schema(),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let questions = match Self::questions(&args) {
                Ok(questions) => questions,
                Err(error) => return ToolOutput::err(format!("Error: {error}")),
            };
            let timeout =
                match Self::timeout(args.get(TIMED_WAIT_PARAMETER).and_then(Value::as_i64)) {
                    Ok(timeout) => timeout,
                    Err(error) => return ToolOutput::err(format!("Error: {error}")),
                };
            let request = QuestionRequest {
                questions,
                cancel: &cx.cancel,
                call_id: &cx.call_id,
                timeout,
            };
            match self.asker.ask(request).await {
                Ok(AskOutcome::Answered(answer)) => ToolOutput::ok(answer.to_string()),
                Ok(AskOutcome::Pending) => ToolOutput::ok(
                    json!({"pending": true, "callId": cx.call_id, "message": PENDING_NOTICE})
                        .to_string(),
                ),
                Err(error) => ToolOutput::err(format!("Error: {error}")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::Cancel;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Fixed {
        outcome: Mutex<Option<Result<AskOutcome, String>>>,
        seen: Mutex<Vec<(String, Option<std::time::Duration>, usize)>>,
    }

    impl QuestionAsker for Fixed {
        fn ask<'a>(
            &'a self,
            request: QuestionRequest<'a>,
        ) -> BoxFuture<'a, Result<AskOutcome, String>> {
            self.seen.lock().unwrap().push((
                request.call_id.to_string(),
                request.timeout,
                request.questions.len(),
            ));
            let outcome = self.outcome.lock().unwrap().clone();
            Box::pin(async move { outcome.unwrap_or_else(|| Err("no outcome".into())) })
        }
    }

    fn asker(outcome: Result<AskOutcome, String>) -> (Arc<Fixed>, Arc<dyn QuestionAsker>) {
        let fixed = Arc::new(Fixed::default());
        *fixed.outcome.lock().unwrap() = Some(outcome);
        let dynamic: Arc<dyn QuestionAsker> = fixed.clone();
        (fixed, dynamic)
    }

    fn cx(call_id: &str) -> ToolContext {
        ToolContext {
            call_id: call_id.into(),
            ..ToolContext::default()
        }
    }

    #[tokio::test]
    async fn an_answer_batch_is_returned_verbatim() {
        let answer = json!({"answers":[{"id":"a","selected":["yes"]}]});
        let (fixed, asker) = asker(Ok(AskOutcome::Answered(answer.clone())));
        let tool = AskUserQuestion::new(asker);
        let out = tool
            .call(
                json!({"questions":[{"id":"a","question":"Proceed?"}]}),
                &cx("call-1"),
            )
            .await;
        assert!(!out.is_error);
        assert_eq!(serde_json::from_str::<Value>(&out.text).unwrap(), answer);
        let seen = fixed.seen.lock().unwrap();
        assert_eq!(seen[0].0, "call-1");
        assert_eq!(seen[0].1, Some(std::time::Duration::from_secs(120)));
        assert_eq!(seen[0].2, 1);
    }

    #[tokio::test]
    async fn a_pending_settlement_keeps_the_call_answerable() {
        let (_, asker) = asker(Ok(AskOutcome::Pending));
        let tool = AskUserQuestion::new(asker);
        let out = tool
            .call(
                json!({"questions":[{"id":"a","question":"Proceed?"}],"timeout":30}),
                &cx("call-9"),
            )
            .await;
        assert!(!out.is_error);
        let value: Value = serde_json::from_str(&out.text).unwrap();
        assert_eq!(value["pending"], json!(true));
        assert_eq!(value["callId"], json!("call-9"));
        assert_eq!(value["message"], json!(PENDING_NOTICE));
    }

    #[tokio::test]
    async fn negative_one_waits_indefinitely() {
        let (fixed, asker) = asker(Ok(AskOutcome::Answered(json!({"answers":[]}))));
        let tool = AskUserQuestion::new(asker);
        let out = tool
            .call(
                json!({"questions":[{"id":"a","question":"Proceed?"}],"timeout":-1}),
                &cx("call-2"),
            )
            .await;
        assert!(!out.is_error);
        assert_eq!(fixed.seen.lock().unwrap()[0].1, None);
    }

    #[tokio::test]
    async fn malformed_arguments_never_reach_the_asker() {
        let (fixed, asker) = asker(Ok(AskOutcome::Answered(json!({"answers":[]}))));
        let tool = AskUserQuestion::new(asker);
        for args in [
            json!({}),
            json!({"questions":[]}),
            json!({"questions":[{"id":"","question":"x"}]}),
            json!({"questions":[{"id":"a"}]}),
            json!({"questions":[{"id":"a","question":"x"},{"id":"a","question":"y"}]}),
            json!({"questions":[{"id":"a","question":"x"}],"timeout":0}),
            json!({"questions":[{"id":"a","question":"x"}],"timeout":2147484}),
        ] {
            let out = tool.call(args, &cx("call-3")).await;
            assert!(out.is_error, "expected rejection");
        }
        assert!(fixed.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_asker_failure_is_model_visible() {
        let (_, asker) = asker(Err("the user dismissed the question".to_string()));
        let tool = AskUserQuestion::new(asker);
        let out = tool
            .call(
                json!({"questions":[{"id":"ok","question":"Proceed?"}]}),
                &cx("call-4"),
            )
            .await;
        assert!(out.is_error);
        assert!(out.text.contains("dismissed"));
    }

    #[test]
    fn skipped_answer_guidance_is_part_of_the_description() {
        let tool = AskUserQuestion::new(asker(Ok(AskOutcome::Pending)).1);
        assert!(tool.def().description.contains(SKIPPED_ANSWER_GUIDANCE));
        let _ = Cancel::default();
    }
}
