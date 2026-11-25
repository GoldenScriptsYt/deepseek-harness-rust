//! `todo_write` tool. Port of `todo/tool-todo`: a whole-list replacement that
//! appends a `todo/write` snapshot to the calling Session.

use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use dsh_planning::{PlanningError, TodoItem, TodoStatus, TodoWrite};
use serde::Deserialize;
use serde_json::{json, Value};

const DESCRIPTION_HEAD: &str = "Record and update a task list to plan multi-step work and show progress; skip it for trivial single-step tasks. Add one todo per concrete step before you start. ";
const DESCRIPTION_PARALLEL: &str = "While work remains, keep the todos being worked on `in_progress`, several only when work runs in parallel. ";
const DESCRIPTION_SINGLE: &str = "While work remains, keep exactly one todo `in_progress`. ";
const DESCRIPTION_TAIL: &str = "Mark each todo `completed` as soon as it is done.";

/// The model-facing description for one activation.
/// @param allow_parallel - Whether several items may be `in_progress` at once.
/// @returns The composed tool description.
fn describe(allow_parallel: bool) -> String {
    let mut text = String::from(DESCRIPTION_HEAD);
    text.push_str(if allow_parallel {
        DESCRIPTION_PARALLEL
    } else {
        DESCRIPTION_SINGLE
    });
    text.push_str(DESCRIPTION_TAIL);
    text
}

/// One model-supplied item, before canonicalization.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawItem {
    content: String,
    status: String,
}

/// Validate and canonicalize the model's list the way the TypeScript tool does:
/// trimmed non-empty unique content, and at most one `in_progress` item unless
/// the deployment allows parallel work.
/// @param raw - The model-supplied items.
/// @param allow_parallel - Whether several items may be `in_progress` at once.
/// @returns The canonical list, or the model-visible argument error text.
fn to_todo_list(raw: &[RawItem], allow_parallel: bool) -> Result<Vec<TodoItem>, String> {
    let mut items = Vec::with_capacity(raw.len());
    let mut seen: Vec<&str> = Vec::with_capacity(raw.len());
    let mut active = 0usize;
    for (index, item) in raw.iter().enumerate() {
        let content = item.content.trim();
        if content.is_empty() {
            return Err(format!("todos[{index}].content must be a non-empty string"));
        }
        if seen.contains(&content) {
            return Err(format!("todos[{index}].content duplicates {content:?}"));
        }
        seen.push(content);
        let status = match item.status.as_str() {
            "pending" => TodoStatus::Pending,
            "in_progress" => TodoStatus::InProgress,
            "completed" => TodoStatus::Completed,
            other => {
                return Err(format!(
                    "todos[{index}].status must be one of pending, in_progress, completed (got {other:?})"
                ))
            }
        };
        if status == TodoStatus::InProgress {
            active += 1;
        }
        items.push(TodoItem {
            content: content.to_string(),
            status,
        });
    }
    if !allow_parallel && active > 1 {
        return Err(format!(
            "at most one task may be in_progress (got {active})"
        ));
    }
    Ok(items)
}

/// `todo_write`: replace the Session's whole todo list.
pub struct Todo {
    allow_parallel: bool,
}

impl Todo {
    /// Build the tool for one deployment policy.
    /// @param allow_parallel - Whether several items may be `in_progress` at once.
    #[must_use]
    pub fn new(allow_parallel: bool) -> Self {
        Self { allow_parallel }
    }
}

impl Default for Todo {
    fn default() -> Self {
        // The shipped web presets set `allowParallelInProgress: true`.
        Self {
            allow_parallel: true,
        }
    }
}

fn render_error(error: &PlanningError) -> ToolOutput {
    ToolOutput::err(format!("Error: {error}"))
}

impl Tool for Todo {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "todo_write".into(),
            description: describe(self.allow_parallel),
            input_schema: json!({"type":"object","additionalProperties":false,"properties":{
                "todos":{"type":"array","description":"The COMPLETE task list, replacing any previous list.","items":{
                    "type":"object","additionalProperties":false,"properties":{
                        "content":{"type":"string","description":"What the task is — a short imperative line."},
                        "status":{"type":"string","enum":["pending","in_progress","completed"],"description":"pending (not started) | in_progress (now) | completed (done)."}
                    },"required":["content","status"]}
                }
            },"required":["todos"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let raw: Vec<RawItem> =
                match serde_json::from_value(args.get("todos").cloned().unwrap_or(Value::Null)) {
                    Ok(items) => items,
                    Err(error) => {
                        return ToolOutput::err(format!("Error: invalid todos argument: {error}"));
                    }
                };
            let items = match to_todo_list(&raw, self.allow_parallel) {
                Ok(items) => items,
                Err(error) => return ToolOutput::err(format!("Error: {error}")),
            };
            let write = TodoWrite {
                todos: items.clone(),
            };
            if let Err(error) = write.validate_shape() {
                return render_error(&error);
            }
            match serde_json::to_value(&write) {
                Ok(payload) => cx.events.append("todo/write", payload),
                Err(error) => return ToolOutput::err(format!("Error: {error}")),
            }
            let count =
                |status: TodoStatus| items.iter().filter(|item| item.status == status).count();
            ToolOutput::ok(format!(
                "Updated todo list: {} pending, {} in progress, {} completed.",
                count(TodoStatus::Pending),
                count(TodoStatus::InProgress),
                count(TodoStatus::Completed)
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> ToolContext {
        ToolContext::default()
    }

    #[tokio::test]
    async fn whole_list_replacement_logs_the_canonical_snapshot() {
        let tool = Todo::default();
        let cx = ctx();
        let out = tool
            .call(
                json!({"todos":[{"content":"  write tests ","status":"in_progress"},{"content":"ship","status":"pending"}]}),
                &cx,
            )
            .await;
        assert!(!out.is_error, "{}", out.text);
        assert_eq!(
            out.text,
            "Updated todo list: 1 pending, 1 in progress, 0 completed."
        );
        let events = cx.events.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "todo/write");
        assert_eq!(
            events[0].1,
            json!({"todos":[{"content":"write tests","status":"in_progress"},{"content":"ship","status":"pending"}]})
        );
    }

    #[tokio::test]
    async fn invalid_lists_are_rejected_without_logging() {
        let tool = Todo::default();
        let cases = [
            (
                json!({"todos":[{"content":"   ","status":"pending"}]}),
                "Error: todos[0].content must be a non-empty string",
            ),
            (
                json!({"todos":[{"content":"a","status":"pending"},{"content":"a","status":"completed"}]}),
                "Error: todos[1].content duplicates \"a\"",
            ),
            (
                json!({"todos":[{"content":"a","status":"blocked"}]}),
                "Error: todos[0].status must be one of pending, in_progress, completed (got \"blocked\")",
            ),
        ];
        for (args, expected) in cases {
            let cx = ctx();
            let out = tool.call(args, &cx).await;
            assert!(out.is_error);
            assert_eq!(out.text, expected);
            assert!(cx.events.drain().is_empty());
        }
    }

    #[tokio::test]
    async fn single_active_discipline_rejects_two_in_progress_items() {
        let tool = Todo::new(false);
        let cx = ctx();
        let out = tool
            .call(
                json!({"todos":[{"content":"a","status":"in_progress"},{"content":"b","status":"in_progress"}]}),
                &cx,
            )
            .await;
        assert!(out.is_error);
        assert_eq!(
            out.text,
            "Error: at most one task may be in_progress (got 2)"
        );
        assert!(cx.events.drain().is_empty());
    }

    #[tokio::test]
    async fn the_logged_snapshot_replays_into_the_planning_projection() {
        let tool = Todo::default();
        let cx = ctx();
        let out = tool
            .call(
                json!({"todos":[{"content":"port planning","status":"in_progress"},{"content":"test","status":"completed"}]}),
                &cx,
            )
            .await;
        assert!(!out.is_error, "{}", out.text);
        let (kind, data) = cx.events.drain().remove(0);
        let event = dsh_session_format::Event {
            kind,
            data,
            extra: serde_json::Map::from_iter([
                ("seq".to_string(), json!(0)),
                ("time".to_string(), json!(1)),
            ]),
        };
        let limits = dsh_planning::PlanningLimits {
            allow_parallel_todos: true,
            ..dsh_planning::PlanningLimits::default()
        };
        let mut state = dsh_planning::PlanningState::new(limits);
        state.replay(&event).expect("the logged snapshot replays");
        assert_eq!(
            state.todos().expect("a written list"),
            &[
                TodoItem {
                    content: "port planning".into(),
                    status: TodoStatus::InProgress
                },
                TodoItem {
                    content: "test".into(),
                    status: TodoStatus::Completed
                }
            ]
        );
    }

    #[test]
    fn description_follows_the_deployment_policy() {
        assert!(describe(true).contains("several only when work runs in parallel"));
        assert!(describe(false).contains("keep exactly one todo `in_progress`"));
        assert_eq!(Todo::default().def().name, "todo_write");
    }
}
