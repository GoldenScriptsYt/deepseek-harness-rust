use crate::fs_safety::{ensure_workspace_dir, io_error, resolve_checked};
use crate::str_arg;
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use globset::{Glob as GlobPattern, GlobMatcher};
use ignore::WalkBuilder;
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_GLOB_RESULTS: usize = 100;
const MAX_GREP_MATCHES: usize = 250;
const MAX_GREP_LINE_BYTES: usize = 2000;
const RAW_OUTPUT_MAX_BYTES: usize = 20_000_000;
const VCS_DIRECTORIES: &[&str] = &[".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];
const SPILL_DIRECTORY: &str = ".dsh-tool-spill";

pub struct Glob;
pub struct Grep;

/// A pattern without `/` matches the basename at any depth; otherwise it anchors to the
/// path relative to the search root.
fn matcher(pattern: &str) -> Result<(GlobMatcher, bool), String> {
    let basename_only = !pattern.contains('/') && !pattern.contains('\\');
    GlobPattern::new(pattern)
        .map(|glob| (glob.compile_matcher(), basename_only))
        .map_err(|error| format!("Error: [SEARCH_INVALID_PATTERN] invalid glob pattern: {error}"))
}

fn matches(matcher: &(GlobMatcher, bool), root: &Path, path: &Path) -> bool {
    if matcher.1 {
        path.file_name()
            .is_some_and(|name| matcher.0.is_match(Path::new(name)))
    } else {
        path.strip_prefix(root)
            .is_ok_and(|relative| matcher.0.is_match(relative))
    }
}

fn excluded(name: &str) -> bool {
    VCS_DIRECTORIES.contains(&name) || name == SPILL_DIRECTORY
}

fn walk(root: &Path) -> ignore::Walk {
    WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .filter_entry(|entry| !excluded(&entry.file_name().to_string_lossy()))
        .build()
}

fn preview_line(line: &str) -> String {
    if line.len() <= MAX_GREP_LINE_BYTES {
        return line.to_string();
    }
    let mut end = MAX_GREP_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{} (line truncated)", &line[..end])
}

fn validate_include(include: &str) -> Result<(), String> {
    if include.trim().is_empty() {
        return Err("Error: include must be a non-empty glob when given".into());
    }
    if include.starts_with('!') {
        return Err(
            "Error: include must be a positive glob filter; negated patterns are not supported"
                .into(),
        );
    }
    let mut depth = 0usize;
    for character in include.chars() {
        match character {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return Err("Error: include must be one glob, not a comma-separated list (use {a,b} alternation instead)".into()),
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug)]
struct GrepMatch {
    path: String,
    line: usize,
    text: String,
}

fn format_grep(matches: &[GrepMatch]) -> String {
    let mut groups: Vec<(&str, Vec<&GrepMatch>)> = Vec::new();
    let mut positions: HashMap<&str, usize> = HashMap::new();
    for found in matches {
        let position = match positions.get(found.path.as_str()) {
            Some(position) => *position,
            None => {
                let position = groups.len();
                positions.insert(found.path.as_str(), position);
                groups.push((&found.path, Vec::new()));
                position
            }
        };
        groups[position].1.push(found);
    }
    let mut sections = Vec::with_capacity(groups.len());
    for (path, group) in groups {
        let rows: Vec<String> = group
            .iter()
            .map(|found| format!("Line {}: {}", found.line, preview_line(&found.text)))
            .collect();
        sections.push(format!("{path}\n{}", rows.join("\n")));
    }
    sections.join("\n\n")
}

/// Save complete formatted output in a private, persistent workspace directory.
/// Failure is reported by the caller and never changes a successful search into an error.
fn spill(workspace: &Path, name: &str, content: &str) -> Result<PathBuf, String> {
    let directory = workspace.join(SPILL_DIRECTORY);
    ensure_workspace_dir(workspace, &directory).map_err(|error| error.to_string())?;
    let temporary = tempfile::Builder::new()
        .prefix(name)
        .suffix(".txt")
        .tempdir_in(&directory)
        .map_err(|error| format!("cannot create spill directory: {error}"))?;
    let path = temporary.path().join("result.txt");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| format!("cannot create spill file: {error}"))?;
    file.write_all(content.as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("cannot write spill file: {error}"))?;
    drop(file);
    let temporary = temporary.keep();
    Ok(temporary.join("result.txt"))
}

fn recovery(locator: Option<PathBuf>, label: &str, narrow: &str) -> String {
    match locator {
        Some(path) => format!(
            "Full {label} stored at: {}. Use read with this file_path to retrieve it.",
            path.display()
        ),
        None => format!("The complete {label} could not be saved; {narrow} to see more."),
    }
}

impl Tool for Glob {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "glob".into(),
            description: format!("Find files by glob pattern, including hidden and ignored files. Returns up to {MAX_GLOB_RESULTS} paths newest first and a recovery path when more exist."),
            input_schema: json!({"type":"object","properties":{
                "pattern":{"type":"string","description":"Non-empty glob pattern. A pattern with no separator matches the basename at any depth; include a separator to anchor the depth."},
                "path":{"type":"string","description":"Directory inside the workspace to search. Defaults to the workspace; relative paths resolve against it."}
            },"required":["pattern"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let pattern = match str_arg(&args, "pattern") {
                Ok(pattern) => pattern.to_string(),
                Err(error) => return ToolOutput::err(error),
            };
            if pattern.trim().is_empty() {
                return ToolOutput::err("Error: pattern must be a non-empty string");
            }
            let input = args
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or(".")
                .to_string();
            let checked = match resolve_checked(&cx.cwd, &input) {
                Ok(path) => path,
                Err(error) => return ToolOutput::err(error.render()),
            };
            let workspace = match fs::canonicalize(&cx.cwd) {
                Ok(path) => path,
                Err(error) => {
                    return ToolOutput::err(io_error("resolve workspace", &cx.cwd, error).render())
                }
            };
            let cancel = cx.cancel.clone();
            tokio::task::spawn_blocking(move || {
                let matcher = match matcher(&pattern) { Ok(matcher) => matcher, Err(error) => return ToolOutput::err(error) };
                let mut found: Vec<(SystemTime, String)> = Vec::new();
                let mut bytes = 0usize;
                for entry in walk(&checked.target).flatten() {
                    if cancel.is_cancelled() { return ToolOutput::err("Error: [SEARCH_ABORTED] search cancelled"); }
                    if !entry.file_type().is_some_and(|kind| kind.is_file()) || !matches(&matcher, &checked.target, entry.path()) { continue; }
                    let display = entry.path().display().to_string();
                    bytes += display.len();
                    if bytes > RAW_OUTPUT_MAX_BYTES {
                        return ToolOutput::err(format!("Error: [SEARCH_RAW_OUTPUT_OVERFLOW] glob exceeded the {RAW_OUTPUT_MAX_BYTES}-byte result cap; narrow pattern or path and retry"));
                    }
                    let modified = entry.metadata().ok().and_then(|metadata| metadata.modified().ok()).unwrap_or(UNIX_EPOCH);
                    found.push((modified, display));
                }
                found.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
                if found.is_empty() { return ToolOutput::ok("No files found"); }
                if found.len() <= MAX_GLOB_RESULTS { return ToolOutput::ok(found.into_iter().map(|entry| entry.1).collect::<Vec<_>>().join("\n")); }
                let total = found.len();
                let complete = found.iter().map(|entry| entry.1.as_str()).collect::<Vec<_>>().join("\n");
                let locator = spill(&workspace, "glob-results-", &complete).ok();
                let page: Vec<String> = found.into_iter().take(MAX_GLOB_RESULTS).map(|entry| entry.1).collect();
                ToolOutput::ok(format!("{}\n\n(Showing {} of {total} paths. {})", page.join("\n"), MAX_GLOB_RESULTS, recovery(locator, "sorted result", "narrow pattern or path")))
            }).await.unwrap_or_else(|error| ToolOutput::err(format!("Error: [SEARCH_FAILED] search task failed: {error}")))
        })
    }
}

impl Tool for Grep {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "grep".into(),
            description: format!("Search UTF-8 file contents with a regular expression. Returns up to {MAX_GREP_MATCHES} matches grouped by file, with each line preview capped at {MAX_GREP_LINE_BYTES} bytes."),
            input_schema: json!({"type":"object","properties":{
                "pattern":{"type":"string","description":"Non-empty regular expression to search for."},
                "path":{"type":"string","description":"File or directory inside the workspace to search. Defaults to the workspace; relative paths resolve against it."},
                "include":{"type":"string","description":"One positive glob filter for files to search, such as *.ts or *.{js,jsx}. Negation and comma-separated lists are not supported."}
            },"required":["pattern"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let pattern = match str_arg(&args, "pattern") {
                Ok(pattern) => pattern.to_string(),
                Err(error) => return ToolOutput::err(error),
            };
            if pattern.is_empty() {
                return ToolOutput::err("Error: pattern must be a non-empty string");
            }
            let input = args
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or(".")
                .to_string();
            let include = args
                .get("include")
                .and_then(Value::as_str)
                .filter(|include| !include.is_empty())
                .map(str::to_string);
            if let Some(include) = &include {
                if let Err(error) = validate_include(include) {
                    return ToolOutput::err(error);
                }
            }
            let checked = match resolve_checked(&cx.cwd, &input) {
                Ok(path) => path,
                Err(error) => return ToolOutput::err(error.render()),
            };
            let workspace = match fs::canonicalize(&cx.cwd) {
                Ok(path) => path,
                Err(error) => {
                    return ToolOutput::err(io_error("resolve workspace", &cx.cwd, error).render())
                }
            };
            let cancel = cx.cancel.clone();
            tokio::task::spawn_blocking(move || {
                let regex = match Regex::new(&pattern) { Ok(regex) => regex, Err(error) => return ToolOutput::err(format!("Error: [SEARCH_INVALID_PATTERN] invalid regular expression: {error}")) };
                let include = match include.as_deref().map(matcher).transpose() { Ok(include) => include, Err(error) => return ToolOutput::err(error) };
                let mut found: Vec<GrepMatch> = Vec::new();
                let mut bytes = 0usize;
                for entry in walk(&checked.target).flatten() {
                    if cancel.is_cancelled() { return ToolOutput::err("Error: [SEARCH_ABORTED] search cancelled"); }
                    if !entry.file_type().is_some_and(|kind| kind.is_file()) { continue; }
                    if include.as_ref().is_some_and(|matcher| !matches(matcher, &checked.target, entry.path())) { continue; }
                    let Ok(text) = fs::read_to_string(entry.path()) else { continue; };
                    for (index, line) in text.lines().enumerate() {
                        if !regex.is_match(line) { continue; }
                        bytes += line.len();
                        if bytes > RAW_OUTPUT_MAX_BYTES {
                            return ToolOutput::err(format!("Error: [SEARCH_RAW_OUTPUT_OVERFLOW] grep exceeded the {RAW_OUTPUT_MAX_BYTES}-byte result cap; narrow pattern, path, or include and retry"));
                        }
                        found.push(GrepMatch { path: entry.path().display().to_string(), line: index + 1, text: line.to_string() });
                    }
                }
                if found.is_empty() { return ToolOutput::ok("No matches found"); }
                let total = found.len();
                let complete = format_grep(&found);
                if total <= MAX_GREP_MATCHES {
                    return ToolOutput::ok(format!("Found {total} {}\n\n{complete}", if total == 1 { "match" } else { "matches" }));
                }
                let locator = spill(&workspace, "grep-results-", &complete).ok();
                let page = format_grep(&found[..MAX_GREP_MATCHES]);
                ToolOutput::ok(format!("Found {MAX_GREP_MATCHES} of {total} matches\n\n{page}\n\n({})", recovery(locator, "grep result", "narrow pattern, path, or include")))
            }).await.unwrap_or_else(|error| ToolOutput::err(format!("Error: [SEARCH_FAILED] search task failed: {error}")))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_safety::unique_root;
    use dsh_core::Cancel;

    fn cx(directory: &Path) -> ToolContext {
        ToolContext {
            cwd: directory.to_path_buf(),
            cancel: Cancel::default(),
            ..ToolContext::default()
        }
    }

    fn fixture(tag: &str) -> tempfile::TempDir {
        let root = unique_root(tag);
        fs::create_dir_all(root.path().join("src/deep")).unwrap();
        fs::create_dir_all(root.path().join(".git")).unwrap();
        fs::write(root.path().join("a.ts"), "const x = 1\nneedle here\n").unwrap();
        fs::write(root.path().join("src/b.ts"), "nothing\n").unwrap();
        fs::write(root.path().join("src/deep/c.js"), "needle again\n").unwrap();
        fs::write(root.path().join(".git/config"), "needle hidden\n").unwrap();
        root
    }

    #[tokio::test]
    async fn glob_basename_anchored_hidden_and_vcs_exclusion() {
        let root = fixture("glob");
        let all = Glob.call(json!({"pattern":"*.ts"}), &cx(root.path())).await;
        assert!(
            all.text.contains("a.ts") && all.text.contains("b.ts") && !all.text.contains("c.js")
        );
        let anchored = Glob
            .call(json!({"pattern":"src/**/*.js"}), &cx(root.path()))
            .await;
        assert!(anchored.text.contains("c.js") && !anchored.text.contains("a.ts"));
        assert_eq!(
            Glob.call(json!({"pattern":"*.rs"}), &cx(root.path()))
                .await
                .text,
            "No files found"
        );
        assert!(!Glob
            .call(json!({"pattern":"**/*"}), &cx(root.path()))
            .await
            .text
            .contains(".git"));
    }

    #[tokio::test]
    async fn grep_include_grouping_and_line_bounds() {
        let root = fixture("grep");
        let output = Grep
            .call(json!({"pattern":"needle"}), &cx(root.path()))
            .await;
        assert_eq!(
            output
                .text
                .lines()
                .filter(|line| line.starts_with("Line "))
                .count(),
            2
        );
        let ts = Grep
            .call(
                json!({"pattern":"needle","include":"*.ts"}),
                &cx(root.path()),
            )
            .await;
        assert!(ts.text.contains("a.ts\nLine 2: needle here"));
        assert!(
            Grep.call(json!({"pattern":"("}), &cx(root.path()))
                .await
                .is_error
        );
        assert!(
            Grep.call(
                json!({"pattern":"x","include":"*.ts,*.js"}),
                &cx(root.path())
            )
            .await
            .is_error
        );
        fs::write(root.path().join("long"), "needle ".repeat(1000)).unwrap();
        let long = Grep
            .call(json!({"pattern":"needle","path":"long"}), &cx(root.path()))
            .await;
        assert!(long.text.contains("(line truncated)"));
        assert!(long.text.len() < 3000);
    }

    #[tokio::test]
    async fn over_cap_results_spill_and_report_omissions() {
        let root = unique_root("spill");
        for index in 0..260 {
            fs::write(root.path().join(format!("{index}.txt")), "needle\n").unwrap();
        }
        let glob = Glob
            .call(json!({"pattern":"*.txt"}), &cx(root.path()))
            .await;
        assert!(glob.text.contains("Showing 100 of 260 paths"));
        assert!(glob.text.contains("Full sorted result stored at:"));
        let grep = Grep
            .call(json!({"pattern":"needle"}), &cx(root.path()))
            .await;
        assert!(grep.text.contains("Found 250 of 260 matches"));
        assert!(grep.text.contains("Full grep result stored at:"));
    }

    #[tokio::test]
    async fn search_rejects_workspace_escape() {
        let root = unique_root("search-safe");
        let output = Glob
            .call(json!({"pattern":"*","path":".."}), &cx(root.path()))
            .await;
        assert!(output.is_error && output.text.contains("FS_SANDBOX_DENIED"));
    }
}
