use thiserror::Error;

/// Result returned by planning validation, replay, and recovery.
pub type PlanningResult<T> = Result<T, PlanningError>;

/// Typed failures owned by the durable planning projection.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlanningError {
    /// An event or public value violated a durable contract.
    #[error("invalid planning {field}: {message}")]
    Invalid {
        /// Field or operation that failed validation.
        field: &'static str,
        /// Stable, actionable diagnostic.
        message: String,
    },
    /// A snapshot violated a durable invariant.
    #[error("corrupt planning snapshot at {field}: {message}")]
    Corrupt {
        /// Snapshot field that failed validation.
        field: &'static str,
        /// Stable, actionable diagnostic.
        message: String,
    },
    /// The snapshot uses an incompatible fold/state version.
    #[error("unsupported planning snapshot version {version}")]
    UnsupportedSnapshotVersion {
        /// Version found in the snapshot.
        version: u32,
    },
    /// A configured collection bound would be exceeded.
    #[error("planning {field} limit {limit} exceeded")]
    LimitExceeded {
        /// Bounded collection name.
        field: &'static str,
        /// Configured maximum.
        limit: usize,
    },
    /// A newer event skipped the next Session sequence.
    #[error("planning event sequence {actual} does not follow {last}")]
    InvalidSequence {
        /// Sequence supplied by the caller.
        actual: u64,
        /// Last reflected sequence, or `-1` for an empty projection.
        last: i128,
    },
    /// A newer event moved commit time backwards.
    #[error("planning event time {actual} precedes last reflected time {last}")]
    InvalidTime {
        /// Commit time supplied by the caller.
        actual: u64,
        /// Last reflected commit time.
        last: u64,
    },
    /// JSON serialization or deserialization failed.
    #[error("planning JSON error: {0}")]
    Json(String),
}

impl PlanningError {
    /// Machine-routable failure discriminator.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "PLANNING_INVALID",
            Self::Corrupt { .. } => "PLANNING_CORRUPT",
            Self::UnsupportedSnapshotVersion { .. } => "PLANNING_UNSUPPORTED_VERSION",
            Self::LimitExceeded { .. } => "PLANNING_LIMIT_EXCEEDED",
            Self::InvalidSequence { .. } => "PLANNING_INVALID_SEQUENCE",
            Self::InvalidTime { .. } => "PLANNING_INVALID_TIME",
            Self::Json(_) => "PLANNING_JSON_ERROR",
        }
    }

    pub(crate) fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            field,
            message: message.into(),
        }
    }

    pub(crate) fn json(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }

    pub(crate) fn into_corrupt(self) -> Self {
        match self {
            Self::Invalid { field, message } => Self::Corrupt { field, message },
            other => other,
        }
    }
}

/// Whether an event changed domain state, was stale, or only advanced the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// A relevant event was folded.
    Applied,
    /// A duplicate or stale sequence was ignored without changing the cursor.
    Ignored,
    /// An unrelated or intentionally inert event advanced the replay cursor.
    Unrelated,
}
