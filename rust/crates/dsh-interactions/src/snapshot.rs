use crate::types::{PendingApproval, PendingQuestion, SettledApproval, SettledQuestion};
use serde::{Deserialize, Serialize};

/// Current snapshot schema version.
pub const SNAPSHOT_VERSION: u32 = 1;

/// Serializable bounded interaction state. Configuration and runtime resources are not persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InteractionSnapshot {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_time: Option<u64>,
    pub approvals: Vec<ApprovalRecord>,
    pub questions: Vec<QuestionRecord>,
}

/// One pending or recently settled approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase", deny_unknown_fields)]
pub enum ApprovalRecord {
    Pending(PendingApproval),
    Settled(SettledApproval),
}

/// One pending or recently settled question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase", deny_unknown_fields)]
pub enum QuestionRecord {
    Pending(PendingQuestion),
    Settled(SettledQuestion),
}
