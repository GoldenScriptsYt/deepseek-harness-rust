use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque identity of one approval request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovalRequestId(String);

impl ApprovalRequestId {
    /// Validate an identifier read from a durable or caller-owned boundary.
    pub fn new(value: impl Into<String>) -> Result<Self, crate::InteractionError> {
        let value = value.into();
        if value.is_empty() {
            return Err(crate::InteractionError::validation(
                "approval request id is empty",
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the identifier without losing its opaque type.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApprovalRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Opaque identity of the tool call associated with an interaction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallId(String);

impl ToolCallId {
    /// Validate an identifier read from a durable or caller-owned boundary.
    pub fn new(value: impl Into<String>) -> Result<Self, crate::InteractionError> {
        let value = value.into();
        if value.is_empty() {
            return Err(crate::InteractionError::validation("tool call id is empty"));
        }
        Ok(Self(value))
    }

    /// Borrow the identifier without losing its opaque type.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ToolCallId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Durable approval request fields; runtime cancellation and display data are absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalRequest {
    pub id: ApprovalRequestId,
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<ToolCallId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// The closed approval outcomes defined by the TypeScript approval service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalOutcome {
    AllowedOnce,
    Rejected,
    Cancelled,
    Unavailable,
}

/// One selectable answer in a user question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuestionOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Durable user-question fields. Presentation-only intents are deliberately excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserQuestion {
    pub id: String,
    pub question: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<QuestionOption>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multi_select: Option<bool>,
}

/// A question request keyed by its originating tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserQuestionRequest {
    pub call_id: ToolCallId,
    pub questions: Vec<UserQuestion>,
}

/// Whether the tool can still return the answer in its foreground window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QuestionState {
    Open,
    Continued,
}

/// One answer in a complete batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuestionAnswer {
    pub id: String,
    pub selected: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom: Option<String>,
}

/// Complete structured answer batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserQuestionAnswer {
    pub answers: Vec<QuestionAnswer>,
}

/// Pending approval with the sequence that opened it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingApproval {
    pub asked_seq: u64,
    pub request: ApprovalRequest,
}

/// A retained approval settlement used to distinguish late answers from unknown ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettledApproval {
    pub decided_seq: u64,
    pub request: ApprovalRequest,
    pub outcome: ApprovalOutcome,
}

/// Pending question reconstructed from durable interaction events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingQuestion {
    pub asked_seq: u64,
    pub call_id: ToolCallId,
    pub questions: Vec<UserQuestion>,
    pub state: QuestionState,
}

/// A retained question settlement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettledQuestion {
    pub settled_seq: u64,
    pub call_id: ToolCallId,
    pub answers: Vec<QuestionAnswer>,
}

/// Deployment-owned upper bounds. Settled history is bounded separately from live work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionLimits {
    pub max_pending_approvals: usize,
    pub max_pending_questions: usize,
    pub max_recent_approvals: usize,
    pub max_recent_questions: usize,
}

impl Default for InteractionLimits {
    fn default() -> Self {
        Self {
            max_pending_approvals: 128,
            max_pending_questions: 128,
            max_recent_approvals: 1024,
            max_recent_questions: 1024,
        }
    }
}

impl InteractionLimits {
    pub(crate) fn validate(self) -> Result<(), crate::InteractionError> {
        if self.max_pending_approvals == 0 || self.max_pending_questions == 0 {
            return Err(crate::InteractionError::validation(
                "pending interaction limits must be greater than zero",
            ));
        }
        Ok(())
    }
}
