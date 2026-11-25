use serde_yaml::Mapping;
use std::path::PathBuf;

/// Default maximum bytes read from one skill document.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 1024 * 1024;
/// Default maximum direct entries inspected in one root.
pub const DEFAULT_MAX_ENTRIES_PER_ROOT: usize = 4096;
/// Default maximum configured roots scanned by one discovery.
pub const DEFAULT_MAX_ROOTS: usize = 64;
/// Default maximum valid skill files inspected before duplicate resolution.
pub const DEFAULT_MAX_SKILL_FILES: usize = 4096;
/// Default aggregate bytes read across one discovery.
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

/// One configured skill root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    /// Directory containing flat Markdown skills and bundle directories.
    pub path: PathBuf,
    /// Provider-specific source label retained on summaries.
    pub source: String,
    /// Lower ranks are discovered first and win duplicate names.
    pub rank: i32,
    /// Whether the direct `.system` child is excluded.
    pub skip_system: bool,
}

impl SkillRoot {
    /// Construct a root with an explicit source label and precedence rank.
    pub fn new(path: impl Into<PathBuf>, source: impl Into<String>, rank: i32) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
            rank,
            skip_system: false,
        }
    }

    /// Exclude the direct `.system` child from this root.
    pub fn skipping_system(mut self) -> Self {
        self.skip_system = true;
        self
    }
}

/// Resource bounds applied to a complete discovery operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryLimits {
    /// Maximum UTF-8 bytes read from one skill file.
    pub max_file_bytes: u64,
    /// Maximum direct directory entries inspected per root, including ignored entries.
    pub max_entries_per_root: usize,
    /// Maximum number of configured roots.
    pub max_roots: usize,
    /// Maximum valid skill files parsed before duplicate resolution.
    pub max_skill_files: usize,
    /// Maximum aggregate bytes read across all roots.
    pub max_total_bytes: u64,
}

impl Default for DiscoveryLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_entries_per_root: DEFAULT_MAX_ENTRIES_PER_ROOT,
            max_roots: DEFAULT_MAX_ROOTS,
            max_skill_files: DEFAULT_MAX_SKILL_FILES,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
        }
    }
}

impl DiscoveryLimits {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.max_file_bytes == 0 {
            return Err("max_file_bytes must be greater than zero");
        }
        if self.max_entries_per_root == 0 {
            return Err("max_entries_per_root must be greater than zero");
        }
        if self.max_roots == 0 {
            return Err("max_roots must be greater than zero");
        }
        if self.max_skill_files == 0 {
            return Err("max_skill_files must be greater than zero");
        }
        if self.max_total_bytes == 0 {
            return Err("max_total_bytes must be greater than zero");
        }
        Ok(())
    }
}

/// Policy for two valid documents that declare the same skill name.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DuplicateNamePolicy {
    /// Keep the first document in root-rank and deterministic path order.
    #[default]
    FirstWins,
    /// Fail discovery with `SKILL_DUPLICATE_NAME`.
    Reject,
}

/// Resolved catalog visibility controls parsed from frontmatter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkillInvocationPolicy {
    /// Whether model-facing consumers may advertise and load the skill.
    pub model_invocable: bool,
    /// Whether human-facing command consumers may invoke the skill.
    pub user_invocable: bool,
}

/// The name and description projection safe for model-facing catalogs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelVisibleSkillSummary {
    /// Kebab-case skill identifier.
    pub name: String,
    /// Routing description without paths, source metadata, or instruction body.
    pub description: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SkillLocator {
    pub(crate) root_index: usize,
    pub(crate) path: PathBuf,
    pub(crate) resource_base: PathBuf,
}

/// Invocation-neutral metadata for one discovered skill.
#[derive(Clone, Debug, PartialEq)]
pub struct SkillSummary {
    /// Kebab-case identifier declared by frontmatter.
    pub name: String,
    /// Non-empty routing description declared by frontmatter.
    pub description: String,
    /// Optional non-empty routing guidance.
    pub when_to_use: Option<String>,
    /// Resolved model and user visibility controls.
    pub invocation: SkillInvocationPolicy,
    /// Source label inherited from the winning root.
    pub source: String,
    /// Precedence rank inherited from the winning root.
    pub rank: i32,
    /// Canonical regular-file path used for previews and loading.
    pub path: PathBuf,
    /// Discovered bundle path or flat root used to resolve resources.
    pub resource_base: PathBuf,
    pub(crate) locator: SkillLocator,
}

impl SkillSummary {
    /// Project this summary for a model catalog when model invocation is enabled.
    pub fn model_visible_summary(&self) -> Option<ModelVisibleSkillSummary> {
        self.invocation
            .model_invocable
            .then(|| ModelVisibleSkillSummary {
                name: self.name.clone(),
                description: self.description.clone(),
            })
    }
}

/// A complete skill document loaded from the current file contents.
#[derive(Clone, Debug, PartialEq)]
pub struct SkillDefinition {
    /// Current parsed metadata and the winning root's source information.
    pub summary: SkillSummary,
    /// Markdown instruction body with surrounding JavaScript whitespace trimmed.
    pub content: String,
    /// Optional YAML mapping from the `metadata` frontmatter field.
    pub metadata: Option<Mapping>,
}

impl SkillDefinition {
    /// Project the loaded definition for a model catalog when invocation is enabled.
    pub fn model_visible_summary(&self) -> Option<ModelVisibleSkillSummary> {
        self.summary.model_visible_summary()
    }
}

/// Classification for a non-fatal discovery diagnostic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SkillWarningCode {
    MissingFrontmatter,
    InvalidYaml,
    InvalidDefinition,
    InvalidInvocation,
    InvalidUtf8,
    SymlinkUnavailable,
    EntryDisappeared,
    RootReadFailed,
    DuplicateIgnored,
}

impl SkillWarningCode {
    /// Return the stable uppercase warning code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingFrontmatter => "SKILL_MISSING_FRONTMATTER",
            Self::InvalidYaml => "SKILL_INVALID_YAML",
            Self::InvalidDefinition => "SKILL_INVALID_DEFINITION",
            Self::InvalidInvocation => "SKILL_INVALID_INVOCATION",
            Self::InvalidUtf8 => "SKILL_INVALID_UTF8",
            Self::SymlinkUnavailable => "SKILL_SYMLINK_UNAVAILABLE",
            Self::EntryDisappeared => "SKILL_ENTRY_DISAPPEARED",
            Self::RootReadFailed => "SKILL_ROOT_READ_FAILED",
            Self::DuplicateIgnored => "SKILL_DUPLICATE_IGNORED",
        }
    }
}

/// A malformed, inaccessible, or shadowed entry omitted from the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillWarning {
    /// Stable warning classification.
    pub code: SkillWarningCode,
    /// Path associated with the diagnostic.
    pub path: PathBuf,
    /// Human-readable detail; models never receive this field automatically.
    pub message: String,
}
