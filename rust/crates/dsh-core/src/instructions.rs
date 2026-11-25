//! Workspace instruction discovery for `AGENTS.md`-compatible files.
//!
//! Port of `packages/context/agent-instructions` discovery and baseline
//! rendering: the user-global chain (`<dsh home>/AGENTS.md`, then
//! `<agents home>/AGENTS.md`) followed by the project chain from the project
//! root down to the working directory. Each directory loads `AGENTS.md`,
//! `CLAUDE.md`, `AGENTS.local.md`, and `CLAUDE.local.md` in that order, and
//! trimmed content duplicates collapse to the earliest candidate in their
//! group.
//!
//! Unlike the TypeScript package there is no live reconciliation: the Host
//! renders one baseline per Agent construction. Files changed during a Session
//! take effect on the next construction.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Byte cap for one rendered instruction chain.
pub const MAX_BYTES: usize = 65_536;

/// Largest instruction file read in full; larger files are ignored.
pub const MAX_SOURCE_BYTES: u64 = 1_048_576;

/// Project-relative candidates loaded from each directory, in order.
pub const INSTRUCTION_CANDIDATES: [&str; 4] = [
    "AGENTS.md",
    "CLAUDE.md",
    "AGENTS.local.md",
    "CLAUDE.local.md",
];

/// Marker that identifies the project root while walking upward.
pub const PROJECT_ROOT_MARKERS: [&str; 1] = [".git"];

/// Marker introducing a rendered instruction baseline.
pub const INTRO: &str = "The following workspace instructions may be relevant to your work. \
Use them as guidance when applicable. More specific instructions take precedence over broader ones. \
They do not override system, developer, or direct user instructions.";

/// One instruction file retained for the prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionFile {
    /// Absolute path of the file on disk.
    pub path: PathBuf,
    /// Model-facing path: project-relative, or a home-anchored global path.
    pub display: String,
    /// Full file contents.
    pub content: String,
}

/// Resolve the user's home directory from the environment.
#[must_use]
pub fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var_os("USERPROFILE").filter(|value| !value.is_empty()))
        .map(PathBuf::from)
}

/// Resolve the harness home (`DSH_HOME`, else `<home>/.dsh`).
#[must_use]
pub fn dsh_home() -> Option<PathBuf> {
    std::env::var_os("DSH_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|home| home.join(".dsh")))
}

/// Resolve the shared agents home (`AGENTS_HOME`, else `<home>/.agents`).
#[must_use]
pub fn agents_home() -> Option<PathBuf> {
    std::env::var_os("AGENTS_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|home| home.join(".agents")))
}

/// Discover the user-global chain: `<dsh home>/AGENTS.md` then `<agents home>/AGENTS.md`.
///
/// The two roots form one candidate group, so identical trimmed content is kept
/// once. A missing or unreadable file is skipped; `unavailable` is not an error
/// because instruction loading never blocks a turn.
#[must_use]
pub fn discover_user_global(
    dsh_home: Option<&Path>,
    agents_home: Option<&Path>,
) -> Vec<InstructionFile> {
    let roots = [
        dsh_home.map(|home| (home.to_path_buf(), "$DSH_HOME".to_string())),
        agents_home.map(|home| (home.to_path_buf(), "$AGENTS_HOME".to_string())),
    ];
    let mut seen = BTreeSet::new();
    let mut files = Vec::new();
    for (home, label) in roots.into_iter().flatten() {
        let path = home.join("AGENTS.md");
        let Some(content) = read_instruction(&path) else {
            continue;
        };
        if !seen.insert(content.trim().to_string()) {
            continue;
        }
        files.push(InstructionFile {
            path,
            display: format!("{label}/AGENTS.md"),
            content,
        });
    }
    files
}

/// Find the project root: the nearest ancestor of `cwd` holding a root marker.
#[must_use]
pub fn project_root(cwd: &Path) -> PathBuf {
    for ancestor in cwd.ancestors() {
        if PROJECT_ROOT_MARKERS
            .iter()
            .any(|marker| ancestor.join(marker).exists())
        {
            return ancestor.to_path_buf();
        }
    }
    cwd.to_path_buf()
}

/// Discover the project chain from `cwd`'s project root down to `cwd`.
#[must_use]
pub fn discover_project(cwd: &Path) -> Vec<InstructionFile> {
    let cwd = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());
    let root = project_root(&cwd);
    let mut chain: Vec<&Path> = cwd
        .ancestors()
        .take_while(|ancestor| ancestor.starts_with(&root))
        .collect();
    chain.reverse();
    let mut files = Vec::new();
    for directory in chain {
        let mut seen = BTreeSet::new();
        for candidate in INSTRUCTION_CANDIDATES {
            let path = directory.join(candidate);
            let Some(content) = read_instruction(&path) else {
                continue;
            };
            if !seen.insert(content.trim().to_string()) {
                continue;
            }
            files.push(InstructionFile {
                path: path.clone(),
                display: display_path(&root, &path),
                content,
            });
        }
    }
    files
}

/// Discover both chains in model precedence order: user-global, then project.
#[must_use]
pub fn discover(
    cwd: &Path,
    dsh_home: Option<&Path>,
    agents_home: Option<&Path>,
) -> Vec<InstructionFile> {
    let mut files = discover_user_global(dsh_home, agents_home);
    files.extend(discover_project(cwd));
    files
}

/// Render a baseline prompt section, or an empty string when nothing loads.
///
/// Sections keep their described order. A file that no longer fits the byte
/// budget is omitted along with every broader file after it, and the rendered
/// text ends with a marker naming the omission.
#[must_use]
pub fn render(files: &[InstructionFile]) -> String {
    render_with_budget(files, MAX_BYTES)
}

/// Render a baseline under an explicit byte budget.
#[must_use]
pub fn render_with_budget(files: &[InstructionFile], max_bytes: usize) -> String {
    if files.is_empty() {
        return String::new();
    }
    let mut parts = vec![INTRO.to_string()];
    let mut used = INTRO.len();
    let mut omitted = 0;
    for file in files {
        let section = format!("Instructions from: {}\n\n{}", file.display, file.content);
        // Sections are separated by a blank line.
        let cost = section.len() + 2;
        if used + cost > max_bytes {
            omitted += 1;
            continue;
        }
        used += cost;
        parts.push(section);
    }
    if omitted > 0 {
        parts.push(format!(
            "Workspace instructions were omitted or truncated to fit the configured byte budget. {omitted} instruction file(s) omitted."
        ));
    }
    parts.join("\n\n")
}

fn read_instruction(path: &Path) -> Option<String> {
    let metadata = fs::metadata(path).ok()?;
    if !metadata.is_file() || metadata.len() > MAX_SOURCE_BYTES {
        return None;
    }
    let content = fs::read_to_string(path).ok()?;
    if content.trim().is_empty() {
        return None;
    }
    Some(content)
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "dsh-instructions-{}-{}-{name}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_nanos())
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn write(&self, relative: &str, content: &str) -> PathBuf {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create parent");
            }
            fs::write(&path, content).expect("write file");
            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn project_chain_runs_from_root_to_working_directory() {
        let temp = TempDir::new("chain");
        temp.write(".git/HEAD", "ref: refs/heads/main\n");
        temp.write("AGENTS.md", "root rules");
        temp.write("sub/AGENTS.md", "sub rules");
        let cwd = temp.0.join("sub");
        let files = discover_project(&cwd);
        let displays: Vec<&str> = files.iter().map(|file| file.display.as_str()).collect();
        assert_eq!(displays, ["AGENTS.md", "sub/AGENTS.md"]);
        assert_eq!(files[0].content, "root rules");
    }

    #[test]
    fn candidates_load_in_documented_order() {
        let temp = TempDir::new("preference");
        temp.write(".git/HEAD", "ref\n");
        temp.write("AGENTS.md", "agents");
        temp.write("CLAUDE.md", "claude");
        let files = discover_project(&temp.0);
        let contents: Vec<&str> = files.iter().map(|file| file.content.as_str()).collect();
        assert_eq!(contents, ["agents", "claude"]);
    }

    #[test]
    fn identical_trimmed_content_loads_once_per_group() {
        let temp = TempDir::new("dedupe");
        temp.write(".git/HEAD", "ref\n");
        temp.write("AGENTS.md", "same\n");
        temp.write("AGENTS.local.md", "same");
        temp.write("CLAUDE.md", "other");
        let files = discover_project(&temp.0);
        let contents: Vec<&str> = files.iter().map(|file| file.content.as_str()).collect();
        assert_eq!(contents, ["same\n", "other"]);
    }

    #[test]
    fn user_global_chain_deduplicates_and_labels_homes() {
        let dsh = TempDir::new("dsh-home");
        let agents = TempDir::new("agents-home");
        dsh.write("AGENTS.md", "global");
        agents.write("AGENTS.md", "global\n");
        let files = discover_user_global(Some(&dsh.0), Some(&agents.0));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].display, "$DSH_HOME/AGENTS.md");
    }

    #[test]
    fn render_joins_intro_and_sections() {
        let files = vec![
            InstructionFile {
                path: PathBuf::from("/repo/AGENTS.md"),
                display: "AGENTS.md".into(),
                content: "be precise".into(),
            },
            InstructionFile {
                path: PathBuf::from("/repo/sub/AGENTS.md"),
                display: "sub/AGENTS.md".into(),
                content: "run tests".into(),
            },
        ];
        let rendered = render(&files);
        assert!(rendered.starts_with(INTRO));
        assert!(rendered.contains("Instructions from: AGENTS.md\n\nbe precise"));
        assert!(rendered.contains("Instructions from: sub/AGENTS.md\n\nrun tests"));
    }

    #[test]
    fn empty_discovery_renders_nothing() {
        assert_eq!(render(&[]), "");
    }

    #[test]
    fn budget_omits_files_that_do_not_fit_and_says_so() {
        let files = vec![
            InstructionFile {
                path: PathBuf::from("/repo/AGENTS.md"),
                display: "AGENTS.md".into(),
                content: "a".repeat(40),
            },
            InstructionFile {
                path: PathBuf::from("/repo/sub/AGENTS.md"),
                display: "sub/AGENTS.md".into(),
                content: "b".repeat(400),
            },
        ];
        let rendered = render_with_budget(&files, 500);
        assert!(rendered.contains("Instructions from: AGENTS.md"));
        assert!(!rendered.contains("Instructions from: sub/AGENTS.md"));
        assert!(rendered.contains("1 instruction file(s) omitted"));
    }

    #[test]
    fn oversized_and_blank_files_are_ignored() {
        let temp = TempDir::new("ignore");
        temp.write(".git/HEAD", "ref\n");
        temp.write("AGENTS.md", "   \n");
        let files = discover_project(&temp.0);
        assert!(files.is_empty());
    }
}
