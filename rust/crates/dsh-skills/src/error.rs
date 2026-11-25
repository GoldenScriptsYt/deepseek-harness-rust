use std::fmt;
use std::io;
use std::path::PathBuf;

/// Stable machine-routing codes for skill failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SkillErrorCode {
    InvalidConfig,
    InvalidDefinition,
    ReadFailed,
    LimitExceeded,
    PathOutsideRoot,
    DuplicateName,
}

impl SkillErrorCode {
    /// Return the stable uppercase code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "SKILL_INVALID_CONFIG",
            Self::InvalidDefinition => "SKILL_INVALID_DEFINITION",
            Self::ReadFailed => "SKILL_READ_FAILED",
            Self::LimitExceeded => "SKILL_LIMIT_EXCEEDED",
            Self::PathOutsideRoot => "SKILL_PATH_OUTSIDE_ROOT",
            Self::DuplicateName => "SKILL_DUPLICATE_NAME",
        }
    }
}

impl fmt::Display for SkillErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The resource bound that rejected discovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LimitKind {
    FileBytes,
    TotalBytes,
    Roots,
    EntriesPerRoot,
    SkillFiles,
}

impl LimitKind {
    /// Return a stable diagnostic name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FileBytes => "file bytes",
            Self::TotalBytes => "total bytes",
            Self::Roots => "roots",
            Self::EntriesPerRoot => "entries per root",
            Self::SkillFiles => "skill files",
        }
    }
}

impl fmt::Display for LimitKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Typed discovery, validation, path-safety, and loading failures.
#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    #[error("invalid skill loader configuration: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("invalid skill definition at {path:?}: {reason}")]
    InvalidDefinition { path: PathBuf, reason: String },

    #[error("failed to read skill path {path:?}: {source}")]
    ReadFailed { path: PathBuf, source: io::Error },

    #[error("skill {kind} limit exceeded: {actual} > {limit}")]
    LimitExceeded {
        kind: LimitKind,
        limit: u64,
        actual: u64,
    },

    #[error("skill path {path:?} resolves outside root {root:?}")]
    PathOutsideRoot { path: PathBuf, root: PathBuf },

    #[error("duplicate skill name {name:?}: {duplicate:?} shadows {first:?}")]
    DuplicateName {
        name: String,
        first: PathBuf,
        duplicate: PathBuf,
    },
}

impl SkillError {
    /// Return the stable machine-routing code.
    pub const fn code(&self) -> SkillErrorCode {
        match self {
            Self::InvalidConfig { .. } => SkillErrorCode::InvalidConfig,
            Self::InvalidDefinition { .. } => SkillErrorCode::InvalidDefinition,
            Self::ReadFailed { .. } => SkillErrorCode::ReadFailed,
            Self::LimitExceeded { .. } => SkillErrorCode::LimitExceeded,
            Self::PathOutsideRoot { .. } => SkillErrorCode::PathOutsideRoot,
            Self::DuplicateName { .. } => SkillErrorCode::DuplicateName,
        }
    }
}
