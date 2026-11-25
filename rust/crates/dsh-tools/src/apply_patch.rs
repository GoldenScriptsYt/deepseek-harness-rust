use crate::fs_patch::{apply_hunks, parse_patch, HunkResult, PatchHunk, PatchOperation};
use crate::fs_safety::{
    atomic_delete, atomic_write, decode_text, detect_crlf, normalize_crlf, read_regular,
    resolve_checked, restore_crlf, version, CheckedPath, FsCode, FsError, FsState, WriteOptions,
};
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Planned {
    index: usize,
    operation: PatchOperation,
    source: CheckedPath,
    destination: Option<CheckedPath>,
}

#[derive(Debug, Clone)]
struct Simulated {
    display: PathBuf,
    exists: bool,
    version: Option<crate::fs_safety::Version>,
    content: Option<String>,
    crlf: bool,
    touched: bool,
}

impl Simulated {
    fn absent(display: PathBuf) -> Self {
        Self {
            display,
            exists: false,
            version: None,
            content: None,
            crlf: false,
            touched: false,
        }
    }

    fn present(display: PathBuf, version: crate::fs_safety::Version) -> Self {
        Self {
            display,
            version: Some(version),
            exists: true,
            content: None,
            crlf: false,
            touched: false,
        }
    }
}

#[derive(Debug, Clone)]
struct Refusal {
    index: usize,
    operation: PatchOperation,
    path: PathBuf,
    error: FsError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Completed,
    Failed,
    Unknown,
    Skipped,
}

#[derive(Debug, Clone)]
struct Outcome {
    index: usize,
    operation: PatchOperation,
    path: PathBuf,
    destination: Option<PathBuf>,
    status: Status,
    failure: Option<FsError>,
    changed: Vec<PathBuf>,
    partial_move: bool,
}

/// Structured report retained by the tool; core's current ToolOutput cannot transport metadata.
#[derive(Debug, Clone)]
struct PatchReport {
    outcomes: Vec<Outcome>,
    cancelled: bool,
}

impl PatchReport {
    fn completed_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for kind in ["add", "update", "delete"] {
            for outcome in &self.outcomes {
                if outcome.status != Status::Completed {
                    continue;
                }
                let actual = if outcome.operation.kind() == "move" {
                    "update"
                } else {
                    outcome.operation.kind()
                };
                if actual != kind {
                    continue;
                }
                let marker = match kind {
                    "add" => 'A',
                    "delete" => 'D',
                    _ => 'M',
                };
                let path = outcome.destination.as_ref().unwrap_or(&outcome.path);
                lines.push(format!("{marker} {}", path.display()));
            }
        }
        lines
    }

    pub(crate) fn render(&self) -> String {
        let stopped = self.cancelled
            || self
                .outcomes
                .iter()
                .any(|outcome| outcome.status != Status::Completed);
        if !stopped {
            return if self.outcomes.is_empty() {
                "Success. No operations in patch.".into()
            } else {
                format!(
                    "Success. Updated the following files:\n{}",
                    self.completed_lines().join("\n")
                )
            };
        }
        let completed = self
            .outcomes
            .iter()
            .filter(|outcome| outcome.status == Status::Completed)
            .count();
        let mut lines = vec![format!(
            "Patch stopped after {} of {} operations.",
            completed,
            self.outcomes.len()
        )];
        if completed > 0 {
            lines.push("Completed:".into());
            lines.extend(self.completed_lines());
        }
        for (status, title) in [
            (Status::Failed, "Failed:"),
            (Status::Unknown, "Outcome unknown:"),
            (Status::Skipped, "Not executed:"),
        ] {
            let selected: Vec<&Outcome> = self
                .outcomes
                .iter()
                .filter(|outcome| outcome.status == status)
                .collect();
            if selected.is_empty() {
                continue;
            }
            lines.push(title.into());
            for outcome in selected {
                let label = format!(
                    "- operation {} ({} {}",
                    outcome.index + 1,
                    outcome.operation.kind(),
                    outcome.path.display()
                );
                if status == Status::Skipped {
                    lines.push(format!("{label})"));
                    continue;
                }
                let destination = outcome
                    .destination
                    .as_ref()
                    .map_or(String::new(), |path| format!(" -> {}", path.display()));
                let message = outcome
                    .failure
                    .as_ref()
                    .map_or("outcome unknown", |error| error.message.as_str());
                lines.push(format!(
                    "{label}{destination}, patch line {}): {message}",
                    outcome.operation.line()
                ));
                if status == Status::Failed && outcome.partial_move {
                    lines.push(format!(
                        "  destination {} was written; source {} remains",
                        outcome.destination.as_ref().unwrap().display(),
                        outcome.path.display()
                    ));
                }
            }
        }
        lines.join("\n")
    }

    /// Bounded presentation data without file contents or unbounded per-operation diagnostics.
    #[allow(dead_code)] // Consumed once tool results carry presentation metadata.
    pub(crate) fn presentation_metadata(&self) -> Value {
        let operations: Vec<Value> = self.outcomes.iter().take(100).map(|outcome| json!({
            "index": outcome.index,
            "kind": outcome.operation.kind(),
            "path": outcome.path.display().to_string().chars().take(1000).collect::<String>(),
            "destination": outcome.destination.as_ref().map(|path| path.display().to_string().chars().take(1000).collect::<String>()),
            "patchLine": outcome.operation.line(),
            "status": match outcome.status { Status::Completed => "completed", Status::Failed => "failed", Status::Unknown => "unknown", Status::Skipped => "skipped" },
            "code": outcome.failure.as_ref().map(|error| error.code.as_str()),
            "message": outcome.failure.as_ref().map(|error| error.message.chars().take(500).collect::<String>()),
            "hunkLine": outcome.failure.as_ref().and_then(|error| error.hunk_line),
            "affected": outcome.partial_move,
        })).collect();
        json!({ "stopped": self.cancelled || self.outcomes.iter().any(|outcome| outcome.status != Status::Completed), "operations": operations, "operationsOmitted": self.outcomes.len().saturating_sub(100) })
    }

    fn is_error(&self) -> bool {
        self.cancelled
            || self
                .outcomes
                .iter()
                .any(|outcome| outcome.status != Status::Completed)
    }
}

fn source_state<'a>(
    states: &'a mut HashMap<PathBuf, Simulated>,
    source: &CheckedPath,
) -> Result<&'a mut Simulated, FsError> {
    if !states.contains_key(&source.target) {
        match version(&source.target)? {
            Some(version) => {
                states.insert(
                    source.target.clone(),
                    Simulated::present(source.display.clone(), version),
                );
            }
            None => {
                states.insert(
                    source.target.clone(),
                    Simulated::absent(source.display.clone()),
                );
            }
        }
    }
    Ok(states.get_mut(&source.target).unwrap())
}

fn destination_state<'a>(
    states: &'a mut HashMap<PathBuf, Simulated>,
    destination: &CheckedPath,
) -> Result<&'a mut Simulated, FsError> {
    if !states.contains_key(&destination.target) {
        match version(&destination.target)? {
            Some(version) => {
                states.insert(
                    destination.target.clone(),
                    Simulated::present(destination.display.clone(), version),
                );
            }
            None => {
                states.insert(
                    destination.target.clone(),
                    Simulated::absent(destination.display.clone()),
                );
            }
        }
    }
    Ok(states.get_mut(&destination.target).unwrap())
}

fn require_source(state: &Simulated, display: &Path, verb: &str) -> Result<(), FsError> {
    if !state.exists {
        return Err(FsError::new(
            FsCode::NotFound,
            format!("cannot {verb} \"{}\": not found", display.display()),
        ));
    }
    Ok(())
}

fn load_content(state: &mut Simulated, operation: &str) -> Result<(), FsError> {
    if state.content.is_none() {
        let bytes = read_regular(&state.display, &state.display, operation, true)?;
        if bytes.contains(&0) {
            return Err(FsError::new(
                FsCode::NotText,
                format!(
                    "cannot {operation} \"{}\": binary file",
                    state.display.display()
                ),
            ));
        }
        let raw = decode_text(&bytes, &state.display, operation)?;
        state.crlf = detect_crlf(&raw);
        state.content = Some(normalize_crlf(&raw));
    }
    Ok(())
}

fn refuse_symlink_source(source: &CheckedPath, kind: &str) -> Result<(), FsError> {
    match fs::symlink_metadata(&source.display) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(FsError::new(FsCode::PatchSymlinkSource, format!("cannot {kind} \"{}\": path is a symbolic link — operate on its target path instead", source.display.display()))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(crate::fs_safety::io_error("inspect", &source.display, error)),
    }
}

fn preflight(planned: &[Planned]) -> Vec<Refusal> {
    let mut states: HashMap<PathBuf, Simulated> = HashMap::new();
    let mut refusals = Vec::new();
    for entry in planned {
        let mut failure_path = entry.source.display.clone();
        let result = (|| -> Result<(), FsError> {
            match &entry.operation {
                PatchOperation::Add { content, .. } => {
                    let state = destination_state(&mut states, &entry.source)?;
                    state.exists = true;
                    state.version = None;
                    state.content = Some(content.clone());
                    state.crlf = false;
                    state.touched = true;
                }
                PatchOperation::Delete { .. } => {
                    refuse_symlink_source(&entry.source, "delete")?;
                    let state = source_state(&mut states, &entry.source)?;
                    require_source(state, &entry.source.display, "delete")?;
                    state.exists = false;
                    state.version = None;
                    state.content = None;
                    state.touched = true;
                }
                PatchOperation::Update { hunks, move_to, .. } => {
                    if move_to.is_some()
                        && entry.source.target == entry.destination.as_ref().unwrap().target
                    {
                        return Err(FsError::new(
                            FsCode::SameTarget,
                            format!(
                                "cannot move \"{}\" to itself",
                                entry.source.display.display()
                            ),
                        ));
                    }
                    if move_to.is_some() {
                        refuse_symlink_source(&entry.source, "move")?;
                    }
                    let state = source_state(&mut states, &entry.source)?;
                    require_source(
                        state,
                        &entry.source.display,
                        if move_to.is_some() { "move" } else { "patch" },
                    )?;
                    load_content(state, "patch")?;
                    let result = apply_hunks(state.content.as_ref().unwrap(), hunks);
                    let after = match result {
                        HunkResult::Applied(after) => after,
                        HunkResult::Mismatch {
                            line,
                            stage,
                            pattern,
                        } => {
                            let preview = pattern.first().cloned().unwrap_or_default();
                            return Err(patch_mismatch(
                                &entry.source.display,
                                line,
                                stage,
                                &preview,
                            ));
                        }
                    };
                    let crlf = state.crlf;
                    if let Some(destination) = &entry.destination {
                        failure_path = destination.display.clone();
                        let target = destination_state(&mut states, destination)?;
                        target.exists = true;
                        target.version = None;
                        target.content = Some(after.clone());
                        target.crlf = crlf;
                        target.touched = true;
                    }
                    let state = states.get_mut(&entry.source.target).unwrap();
                    if move_to.is_some() {
                        state.exists = false;
                        state.version = None;
                        state.content = None;
                    } else {
                        state.content = Some(after);
                    }
                    state.touched = true;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            refusals.push(Refusal {
                index: entry.index,
                operation: entry.operation.clone(),
                path: failure_path,
                error,
            });
        }
    }
    refusals
}

fn render_refusals(refusals: &[Refusal]) -> String {
    let mut lines = vec![format!(
        "Error: [{}] Patch rejected before any change ({} problems):",
        refusals[0].error.code.as_str(),
        refusals.len()
    )];
    for refusal in refusals {
        lines.push(format!(
            "- operation {} ({} {}, patch line {}): {}",
            refusal.index + 1,
            refusal.operation.kind(),
            refusal.path.display(),
            refusal.operation.line(),
            refusal.error.message
        ));
    }
    lines.join("\n")
}

fn patch_mismatch(display: &Path, line: usize, stage: &'static str, pattern: &str) -> FsError {
    let detail = FsError::mismatch(line, stage, pattern);
    let mut error = FsError::new(
        FsCode::PatchMismatch,
        format!("cannot patch \"{}\": {}", display.display(), detail.message),
    );
    error.hunk_line = detail.hunk_line;
    error.stage = detail.stage;
    error
}

fn read_update(
    source: &CheckedPath,
    hunks: &[PatchHunk],
) -> Result<(String, bool, fs::Permissions, crate::fs_safety::Version), FsError> {
    let expected = version(&source.target)?.ok_or_else(|| {
        FsError::new(
            FsCode::StaleVersion,
            "file no longer exists; re-read and retry",
        )
    })?;
    let bytes = read_regular(&source.target, &source.display, "patch", true)?;
    if version(&source.target)? != Some(expected.clone()) {
        return Err(FsError::new(
            FsCode::StaleVersion,
            "file changed while it was being read; re-read and retry",
        ));
    }
    if bytes.contains(&0) {
        return Err(FsError::new(
            FsCode::NotText,
            format!("cannot patch \"{}\": binary file", source.display.display()),
        ));
    }
    let raw = decode_text(&bytes, &source.display, "patch")?;
    let crlf = detect_crlf(&raw);
    let after = match apply_hunks(&normalize_crlf(&raw), hunks) {
        HunkResult::Applied(after) => after,
        HunkResult::Mismatch {
            line,
            stage,
            pattern,
        } => {
            let preview = pattern.first().cloned().unwrap_or_default();
            return Err(patch_mismatch(&source.display, line, stage, &preview));
        }
    };
    let mode = fs::metadata(&source.target)
        .map_err(|error| crate::fs_safety::io_error("inspect", &source.display, error))?
        .permissions();
    Ok((restore_crlf(&after, crlf), crlf, mode, expected))
}

fn commit(cwd: &Path, planned: &[Planned], cancel: &dsh_core::Cancel) -> PatchReport {
    let mut outcomes: Vec<Outcome> = planned
        .iter()
        .map(|entry| Outcome {
            index: entry.index,
            operation: entry.operation.clone(),
            path: entry.source.display.clone(),
            destination: entry.destination.as_ref().map(|path| path.display.clone()),
            status: Status::Skipped,
            failure: None,
            changed: Vec::new(),
            partial_move: false,
        })
        .collect();
    let mut cancelled = false;
    for (position, entry) in planned.iter().enumerate() {
        if cancel.is_cancelled() {
            cancelled = true;
            break;
        }
        let result = (|| -> Result<Vec<PathBuf>, FsError> {
            match &entry.operation {
                PatchOperation::Add { content, .. } => {
                    let path = atomic_write(
                        cwd,
                        &entry.source.display.to_string_lossy(),
                        content.as_bytes(),
                        WriteOptions {
                            create_if_absent: false,
                            preserved_mode: None,
                            expected: None,
                        },
                    )?;
                    Ok(vec![path])
                }
                PatchOperation::Delete { .. } => Ok(vec![atomic_delete(
                    cwd,
                    &entry.source.display.to_string_lossy(),
                    None,
                )?]),
                PatchOperation::Update { hunks, .. } => {
                    let (content, _, mode, expected) = read_update(&entry.source, hunks)?;
                    if let Some(destination) = &entry.destination {
                        let input = destination.display.to_string_lossy();
                        let path = atomic_write(
                            cwd,
                            &input,
                            content.as_bytes(),
                            WriteOptions {
                                create_if_absent: false,
                                preserved_mode: Some(mode),
                                expected: None,
                            },
                        )?;
                        match atomic_delete(
                            cwd,
                            &entry.source.display.to_string_lossy(),
                            Some(expected),
                        ) {
                            Ok(_) => Ok(vec![path]),
                            Err(error) => {
                                outcomes[position].partial_move = true;
                                outcomes[position].changed.push(path.clone());
                                Err(error)
                            }
                        }
                    } else {
                        let input = entry.source.display.to_string_lossy();
                        let path = atomic_write(
                            cwd,
                            &input,
                            content.as_bytes(),
                            WriteOptions {
                                create_if_absent: false,
                                preserved_mode: Some(mode),
                                expected: Some(expected),
                            },
                        )?;
                        Ok(vec![path])
                    }
                }
            }
        })();
        match result {
            Ok(changed) => {
                outcomes[position].status = Status::Completed;
                outcomes[position].changed = changed;
            }
            Err(error) => {
                outcomes[position].status = Status::Failed;
                outcomes[position].failure = Some(error);
                cancelled = cancel.is_cancelled();
                break;
            }
        }
    }
    PatchReport {
        outcomes,
        cancelled,
    }
}

pub struct ApplyPatch {
    state: Arc<FsState>,
}

impl ApplyPatch {
    pub(crate) fn new(state: Arc<FsState>) -> Self {
        Self { state }
    }
}

impl Default for ApplyPatch {
    fn default() -> Self {
        Self {
            state: Arc::new(FsState::default()),
        }
    }
}

impl Tool for ApplyPatch {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "apply_patch".into(),
            description: "Apply a multi-file patch. Every operation is checked before any change; commits then run in patch order and stop at the first failure without rolling back earlier changes.".into(),
            input_schema: json!({"type":"object","properties":{
                "input":{"type":"string","description":"Patch wrapped in *** Begin Patch and *** End Patch. Use *** Add File: path with + lines, *** Delete File: path, or *** Update File: path with context (' '), removed ('-'), and added ('+') lines. Hunks may start with @@ or optional context. *** End of File anchors a hunk at EOF; *** Move to: path renames an updated file. The entire patch is validated before applying."}
            },"required":["input"]}),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            if cx.cancel.is_cancelled() {
                return ToolOutput::err(
                    FsError::new(FsCode::Aborted, "patch cancelled before execution").render(),
                );
            }
            let input = match crate::str_arg(&args, "input") {
                Ok(input) => input.to_string(),
                Err(error) => return ToolOutput::err(error),
            };
            let operations = match parse_patch(&input) {
                Ok(operations) => operations,
                Err(error) => return ToolOutput::err(error.render()),
            };
            if operations.is_empty() {
                return ToolOutput::ok("Success. No operations in patch.");
            }
            if let Some(target) = first_outside(cx, &operations) {
                if let Err(refusal) =
                    crate::approval::approve_target(cx, "apply_patch", &target).await
                {
                    return refusal;
                }
            }
            let cwd = cx.cwd.clone();
            let state = self.state.clone();
            let cancel = cx.cancel.clone();
            let result = tokio::task::spawn_blocking(
                move || -> Result<PatchReport, (Vec<Refusal>, Option<FsError>)> {
                    let mut planned = Vec::new();
                    let mut refusals = Vec::new();
                    for (index, operation) in operations.into_iter().enumerate() {
                        let operation_path = operation.path().to_string();
                        let resolve_result = (|| -> Result<Planned, FsError> {
                            let source = resolve_checked(&cwd, operation.path())?;
                            let destination = match &operation {
                                PatchOperation::Update {
                                    move_to: Some(path),
                                    ..
                                } => Some(resolve_checked(&cwd, path)?),
                                _ => None,
                            };
                            Ok(Planned {
                                index,
                                operation: operation.clone(),
                                source,
                                destination,
                            })
                        })();
                        match resolve_result {
                            Ok(entry) => planned.push(entry),
                            Err(error) => {
                                let path = PathBuf::from(&operation_path);
                                refusals.push(Refusal {
                                    index,
                                    operation,
                                    path,
                                    error,
                                });
                            }
                        }
                    }
                    refusals.extend(preflight(&planned));
                    refusals.sort_by_key(|refusal| refusal.index);
                    if !refusals.is_empty() {
                        return Err((refusals, None));
                    }
                    let targets: BTreeSet<PathBuf> = planned
                        .iter()
                        .flat_map(|entry| {
                            std::iter::once(entry.source.target.clone())
                                .chain(entry.destination.iter().map(|path| path.target.clone()))
                        })
                        .collect();
                    let _guards =
                        state.lock_targets_blocking(&targets.into_iter().collect::<Vec<_>>());
                    for entry in &planned {
                        let source = match resolve_checked(&cwd, entry.operation.path()) {
                            Ok(source) => source,
                            Err(error) => {
                                return Err((
                                    vec![Refusal {
                                        index: entry.index,
                                        operation: entry.operation.clone(),
                                        path: entry.source.display.clone(),
                                        error,
                                    }],
                                    None,
                                ))
                            }
                        };
                        if source.target != entry.source.target {
                            return Err((
                                vec![Refusal {
                                    index: entry.index,
                                    operation: entry.operation.clone(),
                                    path: entry.source.display.clone(),
                                    error: FsError::new(
                                        FsCode::StaleVersion,
                                        "source changed before the patch lock was acquired; retry",
                                    ),
                                }],
                                None,
                            ));
                        }
                        if let Some(destination) = &entry.destination {
                            let fresh = match resolve_checked(
                                &cwd,
                                destination.display.to_string_lossy().as_ref(),
                            ) {
                                Ok(fresh) => fresh,
                                Err(error) => {
                                    return Err((
                                        vec![Refusal {
                                            index: entry.index,
                                            operation: entry.operation.clone(),
                                            path: destination.display.clone(),
                                            error,
                                        }],
                                        None,
                                    ))
                                }
                            };
                            if fresh.target != destination.target {
                                return Err((
                                    vec![Refusal {
                                        index: entry.index,
                                        operation: entry.operation.clone(),
                                        path: destination.display.clone(),
                                        error: FsError::new(
                                            FsCode::StaleVersion,
                                            "destination changed before the patch lock was acquired; retry",
                                        ),
                                    }],
                                    None,
                                ));
                            }
                        }
                    }
                    let report = commit(&cwd, &planned, &cancel);
                    Ok(report)
                },
            )
            .await;
            match result {
                Err(error) => ToolOutput::err(format!("Error: apply_patch task failed: {error}")),
                Ok(Err((refusals, _))) => ToolOutput::err(render_refusals(&refusals)),
                Ok(Ok(report)) => {
                    let error = report.is_error();
                    let mut text = report.render();
                    if report.cancelled {
                        text.push_str("\nError: [FS_ABORTED] patch cancelled; re-read affected files before retrying");
                    }
                    ToolOutput {
                        text,
                        is_error: error,
                    }
                }
            }
        })
    }
}

/// First patch target that reaches outside the Session working directory.
/// @param cx - Calling tool context.
/// @param operations - Parsed patch operations.
/// @returns The resolved target path, or `None` when every target stays inside.
fn first_outside(cx: &ToolContext, operations: &[PatchOperation]) -> Option<PathBuf> {
    for operation in operations {
        let mut paths = vec![operation.path().to_string()];
        if let PatchOperation::Update {
            move_to: Some(path),
            ..
        } = operation
        {
            paths.push(path.clone());
        }
        for path in paths {
            let Ok(checked) = resolve_checked(&cx.cwd, &path) else {
                continue;
            };
            if !crate::approval::inside(&cx.cwd, &checked.target) {
                return Some(checked.target);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_safety::unique_root;
    use dsh_core::Cancel;

    fn cx(cwd: &Path) -> ToolContext {
        ToolContext {
            cwd: cwd.to_path_buf(),
            cancel: Cancel::default(),
            ..ToolContext::default()
        }
    }

    #[tokio::test]
    async fn preflight_prevents_every_write_and_commit_reports_partial_success() {
        let root = unique_root("patch");
        std::fs::write(root.path().join("a"), "one\n").unwrap();
        let tool = ApplyPatch::default();
        let rejected = tool.call(json!({"input":"*** Begin Patch\n*** Update File: a\n@@\n-one\n+two\n*** Update File: a\n@@\n-missing\n+x\n*** End Patch"}), &cx(root.path())).await;
        assert!(rejected.is_error && rejected.text.contains("before any change"));
        assert_eq!(
            std::fs::read_to_string(root.path().join("a")).unwrap(),
            "one\n"
        );
        let patch = "*** Begin Patch\n*** Update File: a\n@@\n-one\n+two\n*** Delete File: absent\n*** End Patch";
        let rejected = tool.call(json!({"input":patch}), &cx(root.path())).await;
        assert!(rejected.is_error && rejected.text.contains("before any change"));
        assert_eq!(
            std::fs::read_to_string(root.path().join("a")).unwrap(),
            "one\n"
        );

        let planned: Vec<Planned> = parse_patch(patch)
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(index, operation)| {
                let source = resolve_checked(root.path(), operation.path()).unwrap();
                Planned {
                    index,
                    operation,
                    source,
                    destination: None,
                }
            })
            .collect();
        let report = commit(root.path(), &planned, &Cancel::default());
        assert_eq!(report.outcomes[0].status, Status::Completed);
        assert_eq!(report.outcomes[1].status, Status::Failed);
        assert_eq!(
            report.outcomes[1].failure.as_ref().unwrap().code,
            FsCode::NotFound
        );
        assert!(report.render().contains("Patch stopped after 1 of 2"));
        assert!(!report.render().contains("rolled back"));
        assert_eq!(
            std::fs::read_to_string(root.path().join("a")).unwrap(),
            "two\n"
        );
    }

    #[tokio::test]
    async fn add_update_move_delete_and_crlf() {
        let root = unique_root("patch-ops");
        std::fs::write(root.path().join("a"), "one\r\ntwo\r\n").unwrap();
        let tool = ApplyPatch::default();
        let output = tool.call(json!({"input":"*** Begin Patch\n*** Add File: b\n+new\n*** Update File: a\n*** Move to: c\n@@\n-one\n+ONE\n*** Delete File: b\n*** End Patch"}), &cx(root.path())).await;
        assert!(!output.is_error, "{}", output.text);
        assert_eq!(
            std::fs::read_to_string(root.path().join("c")).unwrap(),
            "ONE\r\ntwo\r\n"
        );
        assert!(!root.path().join("a").exists() && !root.path().join("b").exists());
        assert!(output.text.contains("M ") && output.text.contains("D "));
    }

    #[tokio::test]
    async fn path_escape_and_symlink_delete_are_rejected() {
        let root = unique_root("patch-safe");
        let outside = unique_root("patch-outside");
        std::fs::write(outside.path().join("x"), "outside\n").unwrap();
        let tool = ApplyPatch::default();
        let escaped = tool.call(json!({"input":format!("*** Begin Patch\n*** Delete File: {}\n*** End Patch", outside.path().join("x").display())}), &cx(root.path())).await;
        assert!(escaped.is_error && escaped.text.contains("FS_SANDBOX_DENIED"));
        assert_eq!(
            std::fs::read_to_string(outside.path().join("x")).unwrap(),
            "outside\n"
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path().join("x"), root.path().join("link")).unwrap();
            let linked = tool
                .call(
                    json!({"input":"*** Begin Patch\n*** Delete File: link\n*** End Patch"}),
                    &cx(root.path()),
                )
                .await;
            assert!(linked.is_error && linked.text.contains("PATCH_SYMLINK_SOURCE"));
            assert!(root.path().join("link").is_symlink());
        }
    }
}
