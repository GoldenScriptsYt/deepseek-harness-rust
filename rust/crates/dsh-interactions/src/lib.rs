//! Durable, tool-independent approval and user-question state.
//!
//! The owner is thread-safe and accepts caller-assigned Session sequence/time values. It never
//! owns timers, abort signals, channels, UI callbacks, or queued runtime replies. Append accepted
//! [`DurableEvent`] records through the owning Session, or recover the same state by ordered replay.

mod event;
mod snapshot;
mod state;
mod types;

pub use event::{
    ApprovalAsked, ApprovalDecided, DurableEvent, InteractionEventKind, InteractionPayload,
    QuestionAsked, QuestionContinued, QuestionSettled, MAX_SAFE_SESSION_INTEGER,
};
pub use snapshot::{ApprovalRecord, InteractionSnapshot, QuestionRecord, SNAPSHOT_VERSION};
pub use state::{
    ApprovalDecision, ApprovalResponse, InteractionState, QuestionResponse, ReplayOutcome,
};
pub use types::{
    ApprovalOutcome, ApprovalRequest, ApprovalRequestId, InteractionLimits, PendingApproval,
    PendingQuestion, QuestionAnswer, QuestionOption, QuestionState, SettledApproval,
    SettledQuestion, ToolCallId, UserQuestion, UserQuestionAnswer, UserQuestionRequest,
};

/// Typed failures from validation, bounds, recovery, and state ownership.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InteractionError {
    #[error("invalid interaction state: {0}")]
    Validation(String),
    #[error("{kind} capacity exceeded (limit {limit})")]
    Capacity { kind: &'static str, limit: usize },
    #[error("interaction state lock is poisoned")]
    Poisoned,
}

impl InteractionError {
    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub(crate) fn capacity(kind: &'static str, limit: usize) -> Self {
        Self::Capacity { kind, limit }
    }
}
