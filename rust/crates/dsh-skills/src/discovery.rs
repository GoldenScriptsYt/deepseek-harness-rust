use crate::error::{LimitKind, SkillError};
use crate::frontmatter::{self, ParsedSkill};
use crate::types::{
    DiscoveryLimits, DuplicateNamePolicy, ModelVisibleSkillSummary, SkillDefinition, SkillLocator,
    SkillRoot, SkillSummary, SkillWarning, SkillWarningCode,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Configures bounded skill discovery over an explicit set of roots.
#[derive(Clone, Debug)]
pub struct SkillLoader {
    roots: Vec<SkillRoot>,
    limits: DiscoveryLimits,
    duplicate_policy: DuplicateNamePolicy,
}

impl SkillLoader {
    /// Construct a loader with default resource limits and first-wins duplicates.
    pub fn new(roots: impl IntoIterator<Item = SkillRoot>) -> Self {
        Self {
            roots: roots.into_iter().collect(),
            limits: DiscoveryLimits::default(),
            duplicate_policy: DuplicateNamePolicy::FirstWins,
        }
    }

    /// Replace the default resource limits, rejecting zero-valued bounds.
    pub fn with_limits(mut self, limits: DiscoveryLimits) -> Result<Self, SkillError> {
        limits
            .validate()
            .map_err(|reason| SkillError::InvalidConfig { reason })?;
        self.limits = limits;
        Ok(self)
    }

    /// Select how duplicate skill names are resolved.
    pub fn with_duplicate_policy(mut self, policy: DuplicateNamePolicy) -> Self {
        self.duplicate_policy = policy;
        self
    }

    /// Discover and validate every configured root.
    ///
    /// Missing roots are empty observations. Malformed documents are omitted
    /// with warnings. A root I/O failure omits that root, preserves other roots,
    /// and marks the catalog incomplete. Path escapes and resource-limit
    /// violations fail the entire operation.
    pub fn discover(&self) -> Result<SkillCatalog, SkillError> {
        self.limits
            .validate()
            .map_err(|reason| SkillError::InvalidConfig { reason })?;
        if self.roots.len() > self.limits.max_roots {
            return Err(SkillError::LimitExceeded {
                kind: LimitKind::Roots,
                limit: self.limits.max_roots as u64,
                actual: self.roots.len() as u64,
            });
        }

        let mut ordered: Vec<(usize, &SkillRoot)> = self.roots.iter().enumerate().collect();
        ordered.sort_by_key(|(index, root)| (root.rank, *index));

        let mut canonical_roots = Vec::with_capacity(ordered.len());
        let mut warnings = Vec::new();
        let mut complete = true;
        for (_, root) in &ordered {
            if root.path.as_os_str().is_empty() || root.source.is_empty() {
                return Err(SkillError::InvalidConfig {
                    reason: "skill roots require a non-empty path and source",
                });
            }
            match canonicalize_root(&root.path) {
                Ok(path) => canonical_roots.push(path),
                Err(error) => {
                    warnings.push(SkillWarning {
                        code: SkillWarningCode::RootReadFailed,
                        path: root.path.clone(),
                        message: error.to_string(),
                    });
                    canonical_roots.push(None);
                    complete = false;
                }
            }
        }

        let mut winners: BTreeMap<String, SkillSummary> = BTreeMap::new();
        let mut state = ScanState {
            limits: &self.limits,
            total_bytes: 0,
            skill_files: 0,
            warnings: &mut warnings,
        };
        for (root_index, (_, root)) in ordered.into_iter().enumerate() {
            let discovered = match scan_root(
                root_index,
                canonical_roots[root_index].as_deref(),
                root,
                &mut state,
            ) {
                Ok(skills) => skills,
                Err(error) => match error {
                    SkillError::ReadFailed { path, source } => {
                        state.warnings.push(SkillWarning {
                            code: SkillWarningCode::RootReadFailed,
                            path,
                            message: source.to_string(),
                        });
                        complete = false;
                        continue;
                    }
                    other => return Err(other),
                },
            };
            for summary in discovered {
                if let Some(first) = winners.get(&summary.name) {
                    match self.duplicate_policy {
                        DuplicateNamePolicy::FirstWins => {
                            state.warnings.push(SkillWarning {
                                code: SkillWarningCode::DuplicateIgnored,
                                path: summary.path.clone(),
                                message: format!(
                                    "duplicate name {:?} is shadowed by {:?}",
                                    summary.name, first.path
                                ),
                            });
                        }
                        DuplicateNamePolicy::Reject => {
                            return Err(SkillError::DuplicateName {
                                name: summary.name,
                                first: first.path.clone(),
                                duplicate: summary.path,
                            });
                        }
                    }
                } else {
                    winners.insert(summary.name.clone(), summary);
                }
            }
        }

        Ok(SkillCatalog {
            skills: winners.into_values().collect(),
            warnings,
            complete,
            context: Arc::new(CatalogContext {
                roots: canonical_roots,
                limits: self.limits,
            }),
        })
    }
}

/// An immutable, deterministically ordered skill catalog.
#[derive(Clone, Debug)]
pub struct SkillCatalog {
    skills: Vec<SkillSummary>,
    warnings: Vec<SkillWarning>,
    complete: bool,
    context: Arc<CatalogContext>,
}

impl SkillCatalog {
    /// Return winning summaries sorted by kebab-case name.
    pub fn skills(&self) -> &[SkillSummary] {
        &self.skills
    }

    /// Return non-fatal diagnostics in deterministic discovery order.
    pub fn warnings(&self) -> &[SkillWarning] {
        &self.warnings
    }

    /// Return whether every configured root was read successfully.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Find a winning summary by its exact validated name.
    pub fn find(&self, name: &str) -> Option<&SkillSummary> {
        if !frontmatter::is_skill_name(name) {
            return None;
        }
        self.skills
            .binary_search_by(|summary| summary.name.as_str().cmp(name))
            .ok()
            .and_then(|index| self.skills.get(index))
    }

    /// Return only model-invocable name/description projections, in catalog order.
    pub fn model_visible_summaries(&self) -> Vec<ModelVisibleSkillSummary> {
        self.skills
            .iter()
            .filter_map(SkillSummary::model_visible_summary)
            .collect()
    }

    /// Re-read and validate the current body for a cataloged skill.
    ///
    /// Missing, non-regular, malformed, non-UTF-8, and name-changed files return
    /// `Ok(None)`, matching the TypeScript provider's stale-load behavior.
    pub fn load(&self, name: &str) -> Result<Option<SkillDefinition>, SkillError> {
        let Some(summary) = self.find(name) else {
            return Ok(None);
        };
        let locator = &summary.locator;
        let root = self.context.roots[locator.root_index]
            .as_deref()
            .ok_or_else(|| SkillError::ReadFailed {
                path: locator.path.clone(),
                source: io::Error::new(io::ErrorKind::NotFound, "skill root is unavailable"),
            })?;
        let mut state = ScanState {
            limits: &self.context.limits,
            total_bytes: 0,
            skill_files: 0,
            warnings: &mut Vec::new(),
        };
        let document = match read_document(&locator.path, root, &mut state)? {
            DocumentRead::Text(document) => document,
            DocumentRead::Missing | DocumentRead::NotRegular | DocumentRead::InvalidUtf8 => {
                return Ok(None);
            }
        };
        let parsed = match frontmatter::parse(&document.text) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(None),
        };
        if parsed.name != name {
            return Ok(None);
        }
        let ParsedSkill {
            name,
            description,
            when_to_use,
            invocation,
            metadata,
            content,
        } = parsed;
        let canonical_path = document.path;
        Ok(Some(SkillDefinition {
            summary: SkillSummary {
                name,
                description,
                when_to_use,
                invocation,
                source: summary.source.clone(),
                rank: summary.rank,
                path: canonical_path,
                resource_base: locator.resource_base.clone(),
                locator: locator.clone(),
            },
            content,
            metadata,
        }))
    }
}

#[derive(Clone, Debug)]
struct CatalogContext {
    roots: Vec<Option<PathBuf>>,
    limits: DiscoveryLimits,
}

struct ScanState<'a> {
    limits: &'a DiscoveryLimits,
    total_bytes: u64,
    skill_files: usize,
    warnings: &'a mut Vec<SkillWarning>,
}

struct DocumentText {
    path: PathBuf,
    text: String,
}

enum DocumentRead {
    Missing,
    NotRegular,
    InvalidUtf8,
    Text(DocumentText),
}

#[derive(Debug)]
struct RootEntry {
    name: String,
    path: PathBuf,
    kind: EntryKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryKind {
    Directory,
    File,
    Symlink,
    Other,
}

fn canonicalize_root(path: &Path) -> Result<Option<PathBuf>, io::Error> {
    let canonical = match fs::canonicalize(path) {
        Ok(path) => path,
        Err(error) if is_absent(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if fs::metadata(&canonical)?.is_dir() {
        Ok(Some(canonical))
    } else {
        Ok(None)
    }
}

fn scan_root(
    root_index: usize,
    canonical_root: Option<&Path>,
    root: &SkillRoot,
    state: &mut ScanState<'_>,
) -> Result<Vec<SkillSummary>, SkillError> {
    let Some(canonical_root) = canonical_root else {
        return Ok(Vec::new());
    };
    let entries = list_entries(canonical_root, root.skip_system, state)?;
    let mut skills = Vec::new();
    for entry in entries {
        let locator = match entry.kind {
            EntryKind::Directory => Some((entry.path.join("SKILL.md"), entry.path, true)),
            EntryKind::File if entry.name.ends_with(".md") => {
                Some((entry.path.clone(), canonical_root.to_path_buf(), false))
            }
            EntryKind::Symlink => match followed_entry_kind(&entry.path) {
                Ok(EntryKind::Directory) => Some((entry.path.join("SKILL.md"), entry.path, true)),
                Ok(EntryKind::File) if entry.name.ends_with(".md") => {
                    Some((entry.path.clone(), canonical_root.to_path_buf(), false))
                }
                Ok(_) => None,
                Err(error) => {
                    state.warnings.push(SkillWarning {
                        code: SkillWarningCode::SymlinkUnavailable,
                        path: entry.path,
                        message: error.to_string(),
                    });
                    None
                }
            },
            EntryKind::File | EntryKind::Other => None,
        };
        let Some((locator_path, resource_base, bundle)) = locator else {
            continue;
        };
        let document = match read_document(&locator_path, canonical_root, state)? {
            DocumentRead::Text(document) => document,
            DocumentRead::Missing => {
                if !bundle {
                    state.warnings.push(SkillWarning {
                        code: SkillWarningCode::EntryDisappeared,
                        path: locator_path,
                        message: "flat skill disappeared during discovery".to_owned(),
                    });
                }
                continue;
            }
            DocumentRead::NotRegular => continue,
            DocumentRead::InvalidUtf8 => {
                state.warnings.push(SkillWarning {
                    code: SkillWarningCode::InvalidUtf8,
                    path: locator_path,
                    message: "skill document is not valid UTF-8".to_owned(),
                });
                continue;
            }
        };
        let parsed = match frontmatter::parse(&document.text) {
            Ok(parsed) => parsed,
            Err(error) => {
                state.warnings.push(SkillWarning {
                    code: error.code.warning_code(),
                    path: locator_path,
                    message: error.message,
                });
                continue;
            }
        };
        state.skill_files += 1;
        if state.skill_files > state.limits.max_skill_files {
            return Err(SkillError::LimitExceeded {
                kind: LimitKind::SkillFiles,
                limit: state.limits.max_skill_files as u64,
                actual: state.skill_files as u64,
            });
        }
        skills.push(summary_from_parsed(
            parsed,
            document.path,
            root_index,
            locator_path,
            resource_base,
            &root.source,
            root.rank,
        ));
    }
    Ok(skills)
}

fn summary_from_parsed(
    parsed: ParsedSkill,
    canonical_path: PathBuf,
    root_index: usize,
    locator_path: PathBuf,
    resource_base: PathBuf,
    source: &str,
    rank: i32,
) -> SkillSummary {
    SkillSummary {
        name: parsed.name,
        description: parsed.description,
        when_to_use: parsed.when_to_use,
        invocation: parsed.invocation,
        source: source.to_owned(),
        rank,
        path: canonical_path,
        resource_base: resource_base.clone(),
        locator: SkillLocator {
            root_index,
            path: locator_path,
            resource_base,
        },
    }
}

fn list_entries(
    root: &Path,
    skip_system: bool,
    state: &mut ScanState<'_>,
) -> Result<Vec<RootEntry>, SkillError> {
    let directory = match fs::read_dir(root) {
        Ok(directory) => directory,
        Err(error) if is_absent(&error) => return Ok(Vec::new()),
        Err(error) => {
            return Err(SkillError::ReadFailed {
                path: root.to_path_buf(),
                source: error,
            });
        }
    };
    let mut entries = Vec::new();
    let mut count = 0usize;
    for entry in directory {
        let entry = entry.map_err(|error| SkillError::ReadFailed {
            path: root.to_path_buf(),
            source: error,
        })?;
        count += 1;
        if count > state.limits.max_entries_per_root {
            return Err(SkillError::LimitExceeded {
                kind: LimitKind::EntriesPerRoot,
                limit: state.limits.max_entries_per_root as u64,
                actual: count as u64,
            });
        }
        let file_name = entry.file_name();
        let name = match file_name.into_string() {
            Ok(name) if is_safe_entry_name(&name) => name,
            Ok(name) => {
                state.warnings.push(SkillWarning {
                    code: SkillWarningCode::InvalidDefinition,
                    path: entry.path(),
                    message: format!("unsafe directory entry name {name:?}"),
                });
                continue;
            }
            Err(file_name) => {
                state.warnings.push(SkillWarning {
                    code: SkillWarningCode::InvalidDefinition,
                    path: entry.path(),
                    message: format!("directory entry name is not UTF-8: {file_name:?}"),
                });
                continue;
            }
        };
        if skip_system && name == ".system" {
            continue;
        }
        let path = root.join(&name);
        let kind = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() => EntryKind::Directory,
            Ok(metadata) if metadata.is_file() => EntryKind::File,
            Ok(metadata) if metadata.file_type().is_symlink() => EntryKind::Symlink,
            Ok(_) => EntryKind::Other,
            Err(error) if is_absent(&error) => {
                state.warnings.push(SkillWarning {
                    code: SkillWarningCode::EntryDisappeared,
                    path,
                    message: "directory entry disappeared during discovery".to_owned(),
                });
                continue;
            }
            Err(error) => {
                return Err(SkillError::ReadFailed {
                    path,
                    source: error,
                });
            }
        };
        entries.push(RootEntry { name, path, kind });
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

fn followed_entry_kind(path: &Path) -> io::Result<EntryKind> {
    let metadata = fs::metadata(path)?;
    Ok(if metadata.is_dir() {
        EntryKind::Directory
    } else if metadata.is_file() {
        EntryKind::File
    } else {
        EntryKind::Other
    })
}

fn read_document(
    locator: &Path,
    canonical_root: &Path,
    state: &mut ScanState<'_>,
) -> Result<DocumentRead, SkillError> {
    let canonical = match fs::canonicalize(locator) {
        Ok(path) => path,
        Err(error) if is_absent(&error) => return Ok(DocumentRead::Missing),
        Err(error) => {
            return Err(SkillError::ReadFailed {
                path: locator.to_path_buf(),
                source: error,
            });
        }
    };
    ensure_contained(&canonical, canonical_root)?;

    let file = match File::open(&canonical) {
        Ok(file) => file,
        Err(error) if is_absent(&error) => return Ok(DocumentRead::Missing),
        Err(error) => {
            return Err(SkillError::ReadFailed {
                path: canonical,
                source: error,
            });
        }
    };
    let metadata = file.metadata().map_err(|error| SkillError::ReadFailed {
        path: canonical.clone(),
        source: error,
    })?;
    if !metadata.is_file() {
        return Ok(DocumentRead::NotRegular);
    }
    if metadata.len() > state.limits.max_file_bytes {
        return Err(SkillError::LimitExceeded {
            kind: LimitKind::FileBytes,
            limit: state.limits.max_file_bytes,
            actual: metadata.len(),
        });
    }
    let projected_total = state.total_bytes.saturating_add(metadata.len());
    if projected_total > state.limits.max_total_bytes {
        return Err(SkillError::LimitExceeded {
            kind: LimitKind::TotalBytes,
            limit: state.limits.max_total_bytes,
            actual: projected_total,
        });
    }

    let capacity = usize::try_from(metadata.len())
        .unwrap_or(0)
        .min(1024 * 1024);
    let mut bytes = Vec::with_capacity(capacity);
    file.take(state.limits.max_file_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| SkillError::ReadFailed {
            path: canonical.clone(),
            source: error,
        })?;
    if bytes.len() as u64 > state.limits.max_file_bytes {
        return Err(SkillError::LimitExceeded {
            kind: LimitKind::FileBytes,
            limit: state.limits.max_file_bytes,
            actual: bytes.len() as u64,
        });
    }
    let actual_total = state.total_bytes.saturating_add(bytes.len() as u64);
    if actual_total > state.limits.max_total_bytes {
        return Err(SkillError::LimitExceeded {
            kind: LimitKind::TotalBytes,
            limit: state.limits.max_total_bytes,
            actual: actual_total,
        });
    }
    state.total_bytes = actual_total;

    let reopened = fs::canonicalize(&canonical).map_err(|error| SkillError::ReadFailed {
        path: canonical.clone(),
        source: error,
    })?;
    if reopened != canonical {
        return Err(SkillError::PathOutsideRoot {
            path: reopened,
            root: canonical_root.to_path_buf(),
        });
    }
    ensure_contained(&reopened, canonical_root)?;

    match String::from_utf8(bytes) {
        Ok(text) => Ok(DocumentRead::Text(DocumentText {
            path: canonical,
            text,
        })),
        Err(_) => Ok(DocumentRead::InvalidUtf8),
    }
}

fn ensure_contained(path: &Path, root: &Path) -> Result<(), SkillError> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(SkillError::PathOutsideRoot {
            path: path.to_path_buf(),
            root: root.to_path_buf(),
        })
    }
}

fn is_safe_entry_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\\')
}

fn is_absent(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.kind() == io::ErrorKind::NotADirectory
}
