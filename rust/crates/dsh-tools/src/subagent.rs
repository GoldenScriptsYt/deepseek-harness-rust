//! `subagent` tool. Port of `subagent/tool-subagent` delegation without the live
//! agent tree: the Host runs the child Agent to completion and returns its answer.

use dsh_core::{BoxFuture, Cancel, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::sync::Arc;

const DESCRIPTION: &str = "Delegate a self-contained task to a subagent (a separate agent that works in its own context) to offload focused, independent work — research, a scoped implementation, an analysis — so it does not consume this conversation's context. The subagent returns its result, not its intermediate steps. This tool runs the subagent to completion before returning.";

/// One delegation handed to the Host's subagent runner.
pub struct SubagentTask<'a> {
    /// Short display description of the delegated work.
    pub description: &'a str,
    /// The complete, self-contained task prompt.
    pub prompt: &'a str,
    /// Cancellation of the calling turn.
    pub cancel: &'a Cancel,
}

/// Host-provided execution of one delegated task in a separate Agent context.
pub trait SubagentRunner: Send + Sync {
    /// Run the task to completion.
    /// @param task - The delegation to execute.
    /// @returns The child's final answer, or the model-visible failure text.
    fn run<'a>(&'a self, task: SubagentTask<'a>) -> BoxFuture<'a, Result<String, String>>;
}

/// `subagent`: delegate one task to a child Agent with its own Session.
pub struct Subagent {
    runner: Arc<dyn SubagentRunner>,
}

impl Subagent {
    /// Build the tool around one Host runner.
    /// @param runner - Executor for delegated tasks.
    #[must_use]
    pub fn new(runner: Arc<dyn SubagentRunner>) -> Self {
        Self { runner }
    }
}

impl Tool for Subagent {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "subagent".into(),
            description: DESCRIPTION.into(),
            input_schema: json!({"type":"object","additionalProperties":false,"properties":{
                "description":{"type":"string","description":"A short (3-5 word) description of the delegated task, for display. Use the language of the user's current request."},
                "prompt":{"type":"string","description":"The complete, self-contained task for the subagent. It does not share this conversation's context, so include everything it needs."}
            },"required":["description","prompt"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let (Some(description), Some(prompt)) = (
                args.get("description").and_then(Value::as_str),
                args.get("prompt").and_then(Value::as_str),
            ) else {
                return ToolOutput::err(
                    "Error: description and prompt must both be non-empty strings".to_string(),
                );
            };
            if prompt.trim().is_empty() {
                return ToolOutput::err("Error: prompt must not be empty".to_string());
            }
            let task = SubagentTask {
                description,
                prompt,
                cancel: &cx.cancel,
            };
            match self.runner.run(task).await {
                Ok(text) => ToolOutput::ok(text),
                Err(error) => ToolOutput::err(format!("Error: {error}")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Recorder {
        seen: Mutex<Vec<(String, String)>>,
        answer: Result<String, String>,
    }

    impl SubagentRunner for Recorder {
        fn run<'a>(&'a self, task: SubagentTask<'a>) -> BoxFuture<'a, Result<String, String>> {
            Box::pin(async move {
                self.seen
                    .lock()
                    .unwrap()
                    .push((task.description.to_string(), task.prompt.to_string()));
                self.answer.clone()
            })
        }
    }

    fn recorder(answer: Result<&str, &str>) -> Arc<Recorder> {
        Arc::new(Recorder {
            seen: Mutex::new(Vec::new()),
            answer: answer.map(str::to_string).map_err(str::to_string),
        })
    }

    #[tokio::test]
    async fn delegates_the_prompt_and_returns_the_child_answer() {
        let recorder = recorder(Ok("the child answer"));
        let tool = Subagent::new(recorder.clone());
        let out = tool
            .call(
                json!({"description":"read the tree","prompt":"list every crate"}),
                &ToolContext::default(),
            )
            .await;
        assert_eq!(out, ToolOutput::ok("the child answer"));
        assert_eq!(
            recorder.seen.lock().unwrap().as_slice(),
            [("read the tree".to_string(), "list every crate".to_string())]
        );
    }

    #[tokio::test]
    async fn a_failed_delegation_is_a_tool_error() {
        let tool = Subagent::new(recorder(Err("model unavailable")));
        let out = tool
            .call(
                json!({"description":"x","prompt":"y"}),
                &ToolContext::default(),
            )
            .await;
        assert!(out.is_error);
        assert!(out.text.contains("model unavailable"), "{}", out.text);
    }

    #[tokio::test]
    async fn missing_arguments_never_reach_the_runner() {
        let recorder = recorder(Ok("unused"));
        let tool = Subagent::new(recorder.clone());
        let out = tool
            .call(json!({"description":"x"}), &ToolContext::default())
            .await;
        assert!(out.is_error);
        assert!(recorder.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn the_tool_matches_the_typescript_delegation_contract() {
        let recorder = recorder(Ok("unused"));
        let tool = Subagent::new(recorder);
        let def = tool.def();
        assert_eq!(def.name, "subagent");
        assert_eq!(
            def.input_schema["required"],
            json!(["description", "prompt"])
        );
    }
}
