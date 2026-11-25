use crate::util::normalized_nonempty;
use crate::{PlanningError, PlanningResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Status of one ordered todo item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
}

/// One model-facing todo. Items intentionally have no independent identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TodoItem {
    pub content: String,
    pub status: TodoStatus,
}

/// Whole-list `todo/write` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TodoWrite {
    pub todos: Vec<TodoItem>,
}

impl TodoWrite {
    /// Validate the shape every `todo/write` snapshot must keep: unique,
    /// trimmed, non-empty item content.
    /// @returns An error naming the first offending field.
    pub fn validate_shape(&self) -> PlanningResult<()> {
        let mut seen = BTreeSet::new();
        for item in &self.todos {
            normalized_nonempty(&item.content, "todo.content")?;
            if !seen.insert(item.content.as_str()) {
                return Err(PlanningError::invalid(
                    "todo.content",
                    "must be unique within the whole list",
                ));
            }
        }
        Ok(())
    }

    /// Validate the whole list against the deployment's concurrency policy.
    /// @param allow_parallel - Whether several items may be `in_progress` at once.
    /// @returns An error naming the first offending field.
    pub fn validate(&self, allow_parallel: bool) -> PlanningResult<()> {
        self.validate_shape()?;
        let active = self
            .todos
            .iter()
            .filter(|item| item.status == TodoStatus::InProgress)
            .count();
        if !allow_parallel && active > 1 {
            return Err(PlanningError::invalid(
                "todo.status",
                "at most one item may be in_progress",
            ));
        }
        Ok(())
    }
}

/// `None` means no list for the current turn; an empty vector is a written empty list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct TodoState {
    pub items: Option<Vec<TodoItem>>,
}

impl TodoState {
    pub(crate) fn validate(&self, max_items: usize, allow_parallel: bool) -> PlanningResult<()> {
        if let Some(items) = &self.items {
            if items.len() > max_items {
                return Err(PlanningError::LimitExceeded {
                    field: "todo items",
                    limit: max_items,
                });
            }
            TodoWrite {
                todos: items.clone(),
            }
            .validate(allow_parallel)?;
        }
        Ok(())
    }

    pub(crate) fn write(
        &mut self,
        write: &TodoWrite,
        max_items: usize,
        allow_parallel: bool,
    ) -> PlanningResult<()> {
        if write.todos.len() > max_items {
            return Err(PlanningError::LimitExceeded {
                field: "todo items",
                limit: max_items,
            });
        }
        write.validate(allow_parallel)?;
        self.items = Some(write.todos.clone());
        Ok(())
    }
}
