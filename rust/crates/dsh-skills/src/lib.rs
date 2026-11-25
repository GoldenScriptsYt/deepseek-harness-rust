//! Bounded filesystem skill discovery and loading.
//!
//! The loader recognizes the TypeScript filesystem provider's two one-level
//! forms: `<root>/<bundle>/SKILL.md` and `<root>/<flat>.md`. Roots are scanned in
//! rank order, duplicate names are first-wins by default, and the resulting
//! catalog is sorted by skill name. Malformed documents are omitted and reported
//! as warnings; unsafe paths and exceeded resource limits fail the operation.
//!
//! Symbolic links may be followed only when their resolved file remains under
//! the canonical configured root. This crate owns discovery and loading only;
//! it does not render prompts, invoke skills, or watch for filesystem changes.

mod discovery;
mod error;
mod frontmatter;
mod types;

pub use discovery::{SkillCatalog, SkillLoader};
pub use error::{LimitKind, SkillError, SkillErrorCode};
pub use types::{
    DiscoveryLimits, DuplicateNamePolicy, ModelVisibleSkillSummary, SkillDefinition,
    SkillInvocationPolicy, SkillRoot, SkillSummary, SkillWarning, SkillWarningCode,
    DEFAULT_MAX_ENTRIES_PER_ROOT, DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_ROOTS,
    DEFAULT_MAX_SKILL_FILES, DEFAULT_MAX_TOTAL_BYTES,
};
