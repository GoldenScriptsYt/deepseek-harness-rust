use crate::{PlanningError, PlanningResult};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque Goal identity. The authoritative contract requires only non-emptiness.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GoalId(String);

impl GoalId {
    /// Validate and brand a Goal id.
    pub fn new(value: impl Into<String>) -> PlanningResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(PlanningError::invalid("goal.id", "must be non-empty"));
        }
        Ok(Self(value))
    }

    /// Borrow the stable identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque globally stable Schedule identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ScheduleId(String);

impl ScheduleId {
    /// Validate and brand a Schedule id without surrounding whitespace.
    pub fn new(value: impl Into<String>) -> PlanningResult<Self> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(PlanningError::invalid(
                "schedule.id",
                "must be non-empty without surrounding whitespace",
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the stable identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque Session identity used by Schedule destinations and receipts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    /// Validate and brand a non-empty Session id.
    pub fn new(value: impl Into<String>) -> PlanningResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(PlanningError::invalid("sessionId", "must be non-empty"));
        }
        Ok(Self(value))
    }

    /// Borrow the stable identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque delivered-message identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(String);

impl MessageId {
    /// Validate and brand a message id without surrounding whitespace.
    pub fn new(value: impl Into<String>) -> PlanningResult<Self> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(PlanningError::invalid(
                "messageId",
                "must be non-empty without surrounding whitespace",
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the stable identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque pairing id for `command/run` and `command/done`.
///
/// The authoritative command brand performs no validation, so this wrapper also
/// preserves arbitrary strings rather than inventing a stronger wire contract.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommandId(String);

impl CommandId {
    /// Brand an executor-minted command pairing id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the pairing identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GoalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for ScheduleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
