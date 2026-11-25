use crate::fs_safety::{
    atomic_write, decode_text, edit_file, read_regular, resolve_checked, FsCode, FsError, FsState,
    WriteOptions,
};
use crate::str_arg;
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::sync::Arc;

const READ_LIMIT: usize = 2000;
const READ_MAX_LINE: usize = 2000;
const READ_MAX_BYTES: usize = 50 * 1024;

pub struct Read;
pub struct Write {
    state: Arc<FsState>,
}
pub struct Edit {
    state: Arc<FsState>,
}

impl Write {
    pub(crate) fn new(state: Arc<FsState>) -> Self {
        Self { state }
    }
}

impl Default for Write {
    fn default() -> Self {
        Self {
            state: Arc::new(FsState::default()),
        }
    }
}

impl Edit {
    pub(crate) fn new(state: Arc<FsState>) -> Self {
        Self { state }
    }
}

impl Default for Edit {
    fn default() -> Self {
        Self {
            state: Arc::new(FsState::default()),
        }
    }
}

/// Render a bounded, line-numbered window exactly as `formatReadOutput` does.
pub(crate) fn render_read(
    display: &str,
    text: &str,
    offset: usize,
    limit: usize,
) -> Result<String, String> {
    let mut lines: Vec<(usize, String)> = Vec::new();
    let (mut total, mut bytes, mut capped) = (0usize, 0usize, false);
    // `split('\n')` yields a final empty piece for a trailing newline; JS does the same
    // scan but does not count a terminating newline as a line, so drop it.
    let body = text.strip_suffix('\n').unwrap_or(text);
    let pieces: Vec<&str> = if text.is_empty() {
        vec![]
    } else {
        body.split('\n').collect()
    };
    for raw in pieces {
        let raw = raw.strip_suffix('\r').unwrap_or(raw);
        let wanted = !capped && total + 1 >= offset && lines.len() < limit;
        total += 1;
        if !wanted {
            continue;
        }
        let shown: String = if raw.chars().count() > READ_MAX_LINE {
            format!(
                "{}... (line truncated to {READ_MAX_LINE} chars)",
                raw.chars().take(READ_MAX_LINE).collect::<String>()
            )
        } else {
            raw.to_string()
        };
        let size = shown.len() + usize::from(!lines.is_empty());
        if bytes + size > READ_MAX_BYTES {
            capped = true;
            continue;
        }
        bytes += size;
        lines.push((total, shown));
    }
    if !capped && offset > total && !(total == 0 && offset == 1) {
        return Err(format!(
            "offset {offset} is out of range for \"{display}\" ({total} lines)"
        ));
    }
    let end = lines.last().map_or(offset.saturating_sub(1), |l| l.0);
    let footer = if capped {
        format!(
            "(Output capped. Showing lines {offset}-{end}. Use offset={} to continue.)",
            end + 1
        )
    } else if end < total {
        format!(
            "(Showing lines {offset}-{end} of {total}. Use offset={} to continue.)",
            end + 1
        )
    } else {
        format!("(End of file - total {total} lines)")
    };
    let body = if lines.is_empty() {
        footer
    } else {
        let joined: Vec<String> = lines.iter().map(|(n, t)| format!("{n}: {t}")).collect();
        format!("{}\n\n{footer}", joined.join("\n"))
    };
    Ok(format!(
        "<path>{display}</path>\n<type>file</type>\n<content>\n{body}\n</content>"
    ))
}

impl Tool for Read {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "read".into(),
            description: "Read a UTF-8 text file and return line-numbered content.".into(),
            input_schema: json!({"type":"object","properties":{
                "file_path":{"type":"string","description":"Path to read, resolved by the filesystem backend."},
                "offset":{"type":"number","description":"1-based first line to return. Defaults to 1."},
                "limit":{"type":"number","description":format!("Maximum number of lines to return. Defaults to {READ_LIMIT}.")}
            },"required":["file_path"]}),
        }
    }
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let p = match str_arg(&args, "file_path") {
                Ok(p) => p.to_string(),
                Err(e) => return ToolOutput::err(e),
            };
            let offset = args
                .get("offset")
                .and_then(Value::as_u64)
                .map_or(1, |o| o.max(1) as usize);
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .map_or(READ_LIMIT, |l| l.max(1) as usize);
            let cwd = cx.cwd.clone();
            tokio::task::spawn_blocking(move || {
                let checked = match resolve_checked(&cwd, &p) {
                    Ok(path) => path,
                    Err(error) => return ToolOutput::err(error.render()),
                };
                let display = checked.display.display().to_string();
                let bytes = match read_regular(&checked.target, &checked.display, "read", false) {
                    Ok(bytes) => bytes,
                    Err(error) => return ToolOutput::err(error.render()),
                };
                let text = match decode_text(&bytes, &checked.display, "read") {
                    Ok(text) => text,
                    Err(error) => return ToolOutput::err(error.render()),
                };
                match render_read(&display, &text, offset, limit) {
                    Ok(output) => ToolOutput::ok(output),
                    Err(error) => ToolOutput::err(format!("Error: {error}")),
                }
            })
            .await
            .unwrap_or_else(|error| ToolOutput::err(format!("Error: read task failed: {error}")))
        })
    }
}

impl Tool for Write {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "write".into(),
            description: "Create or fully replace a UTF-8 text file.".into(),
            input_schema: json!({"type":"object","properties":{
                "file_path":{"type":"string","description":"Path to write, resolved by the filesystem backend. Provide `file_path` before `content` in the arguments."},
                "content":{"type":"string","description":"Full UTF-8 text content to write."}
            },"required":["file_path","content"]}),
        }
    }
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let (p, content) = match (str_arg(&args, "file_path"), str_arg(&args, "content")) {
                (Ok(p), Ok(c)) => (p.to_string(), c.to_string()),
                (Err(e), _) | (_, Err(e)) => return ToolOutput::err(e),
            };
            let checked = match resolve_checked(&cx.cwd, &p) {
                Ok(path) => path,
                Err(error) => return ToolOutput::err(error.render()),
            };
            if !crate::approval::inside(&cx.cwd, &checked.target) {
                if let Err(refusal) =
                    crate::approval::approve_target(cx, "write", &checked.target).await
                {
                    return refusal;
                }
            }
            let cwd = cx.cwd.clone();
            let state = self.state.clone();
            let size = content.len();
            tokio::task::spawn_blocking(move || {
                let fresh = match resolve_checked(&cwd, &p) {
                    Ok(path) => path,
                    Err(error) => return ToolOutput::err(error.render()),
                };
                if fresh.target != checked.target {
                    return ToolOutput::err(
                        FsError::new(
                            FsCode::StaleVersion,
                            "target changed before the write lock was acquired; retry",
                        )
                        .render(),
                    );
                }
                let _guards = state.lock_targets_blocking(&[fresh.target]);
                match atomic_write(
                    &cwd,
                    &p,
                    content.as_bytes(),
                    WriteOptions {
                        create_if_absent: false,
                        preserved_mode: None,
                        expected: None,
                    },
                ) {
                    Ok(path) => ToolOutput::ok(format!("Wrote {size} bytes to {}", path.display())),
                    Err(error) => ToolOutput::err(error.render()),
                }
            })
            .await
            .unwrap_or_else(|error| ToolOutput::err(format!("Error: write task failed: {error}")))
        })
    }
}

impl Tool for Edit {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "edit".into(),
            description: "Edit an existing UTF-8 text file by replacing literal text.".into(),
            input_schema: json!({"type":"object","properties":{
                "file_path":{"type":"string","description":"Path to edit, resolved by the filesystem backend. Provide `file_path` before `old_string` and `new_string` in the arguments."},
                "old_string":{"type":"string","description":"Literal text to replace."},
                "new_string":{"type":"string","description":"Literal replacement text. Use an empty string to delete the match."},
                "replace_all":{"type":"boolean","description":"Replace all matches. Defaults to false; when false, old_string must appear exactly once."}
            },"required":["file_path","old_string","new_string"]}),
        }
    }
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let (p, old, new) = match (
                str_arg(&args, "file_path"),
                str_arg(&args, "old_string"),
                str_arg(&args, "new_string"),
            ) {
                (Ok(p), Ok(o), Ok(n)) => (p.to_string(), o.to_string(), n.to_string()),
                (Err(e), ..) | (_, Err(e), _) | (_, _, Err(e)) => return ToolOutput::err(e),
            };
            let all = args
                .get("replace_all")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if old.is_empty() {
                return ToolOutput::err(
                    "Error: [FS_EDIT_NOT_FOUND] old_string must be a non-empty string",
                );
            }
            if old == new {
                return ToolOutput::err("Error: old_string and new_string must differ");
            }
            let checked = match resolve_checked(&cx.cwd, &p) {
                Ok(path) => path,
                Err(error) => return ToolOutput::err(error.render()),
            };
            if !crate::approval::inside(&cx.cwd, &checked.target) {
                if let Err(refusal) =
                    crate::approval::approve_target(cx, "edit", &checked.target).await
                {
                    return refusal;
                }
            }
            let cwd = cx.cwd.clone();
            let state = self.state.clone();
            tokio::task::spawn_blocking(move || {
                let fresh = match resolve_checked(&cwd, &p) {
                    Ok(path) => path,
                    Err(error) => return ToolOutput::err(error.render()),
                };
                if fresh.target != checked.target {
                    return ToolOutput::err(
                        FsError::new(
                            FsCode::StaleVersion,
                            "target changed before the edit lock was acquired; retry",
                        )
                        .render(),
                    );
                }
                let _guards = state.lock_targets_blocking(&[fresh.target]);
                match edit_file(&cwd, &p, &old, &new, all) {
                    Ok((path, count)) => ToolOutput::ok(format!(
                        "Edited {} ({count} replacement{})",
                        path.display(),
                        if count == 1 { "" } else { "s" }
                    )),
                    Err(error) => ToolOutput::err(error.render()),
                }
            })
            .await
            .unwrap_or_else(|error| ToolOutput::err(format!("Error: edit task failed: {error}")))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_format_matches_typescript() {
        let out = render_read("/a.txt", "one\ntwo\nthree\n", 1, 2000).unwrap();
        assert_eq!(
            out,
            "<path>/a.txt</path>\n<type>file</type>\n<content>\n1: one\n2: two\n3: three\n\n(End of file - total 3 lines)\n</content>"
        );
    }

    #[test]
    fn read_windows_and_errors() {
        let out = render_read("/a", "a\nb\nc\nd\n", 2, 2).unwrap();
        assert!(out.contains("2: b\n3: c\n\n(Showing lines 2-3 of 4. Use offset=4 to continue.)"));
        assert!(render_read("/a", "a\n", 5, 10)
            .unwrap_err()
            .contains("out of range"));
        assert!(render_read("/e", "", 1, 10)
            .unwrap()
            .contains("(End of file - total 0 lines)"));
        assert!(render_read("/c", "a\r\nb\r\n", 1, 10)
            .unwrap()
            .contains("1: a\n2: b"));
    }

    fn cx(cwd: &std::path::Path) -> dsh_core::ToolContext {
        dsh_core::ToolContext {
            cwd: cwd.to_path_buf(),
            cancel: dsh_core::Cancel::default(),
            ..dsh_core::ToolContext::default()
        }
    }

    #[tokio::test]
    async fn read_preserves_utf8_bytes_crlf_and_nul() {
        let root = crate::fs_safety::unique_root("read-exact");
        std::fs::write(root.path().join("file"), "α\r\nβ\0γ\r\n").unwrap();
        let output = Read
            .call(json!({"file_path":"file"}), &cx(root.path()))
            .await;
        assert!(!output.is_error, "{}", output.text);
        assert!(output.text.contains("1: α\n2: β\0γ"));
        std::fs::write(root.path().join("invalid"), [0xff, 0xfe]).unwrap();
        let invalid = Read
            .call(json!({"file_path":"invalid"}), &cx(root.path()))
            .await;
        assert!(invalid.is_error && invalid.text.contains("FS_NOT_TEXT"));
    }

    #[tokio::test]
    async fn write_accepts_absolute_workspace_paths_and_creates_parents() {
        let root = crate::fs_safety::unique_root("write-absolute");
        let path = root.path().join("nested/deeper/file");
        let output = Write::default()
            .call(
                json!({"file_path":path.to_string_lossy(),"content":"exact\r\ncontent\0"}),
                &cx(root.path()),
            )
            .await;
        assert!(!output.is_error, "{}", output.text);
        assert_eq!(std::fs::read(path).unwrap(), b"exact\r\ncontent\0");
    }

    #[tokio::test]
    async fn write_and_edit_reject_workspace_escapes() {
        let parent = crate::fs_safety::unique_root("tool-containment");
        let workspace = parent.path().join("workspace");
        let outside = parent.path().join("outside");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("file"), "outside").unwrap();
        for path in [
            "../outside/file",
            outside.join("file").to_string_lossy().as_ref(),
        ] {
            let written = Write::default()
                .call(
                    json!({"file_path":path,"content":"changed"}),
                    &cx(&workspace),
                )
                .await;
            assert!(written.is_error && written.text.contains("FS_SANDBOX_DENIED"));
            let edited = Edit::default()
                .call(
                    json!({"file_path":path,"old_string":"outside","new_string":"changed"}),
                    &cx(&workspace),
                )
                .await;
            assert!(edited.is_error && edited.text.contains("FS_SANDBOX_DENIED"));
        }
        assert_eq!(
            std::fs::read_to_string(outside.join("file")).unwrap(),
            "outside"
        );
    }

    #[tokio::test]
    async fn concurrent_edits_are_serialized_without_stale_versions() {
        let root = crate::fs_safety::unique_root("edit-concurrency");
        let mut initial = String::from("start\n");
        for index in 0..12 {
            initial.push_str(&format!("line-{index}\n"));
        }
        std::fs::write(root.path().join("file"), initial).unwrap();
        let state = Arc::new(FsState::default());
        let edit = Arc::new(Edit::new(state));
        let mut tasks = Vec::new();
        for index in 0..12 {
            let edit = edit.clone();
            let cwd = root.path().to_path_buf();
            tasks.push(tokio::spawn(async move {
                edit.call(
                    json!({"file_path":"file","old_string":format!("\nline-{index}\n"),"new_string":format!("\nchanged-{index}\n")}),
                    &cx(&cwd),
                )
                .await
            }));
        }
        for task in tasks {
            let output = task.await.unwrap();
            assert!(!output.is_error, "{}", output.text);
        }
        let final_text = std::fs::read_to_string(root.path().join("file")).unwrap();
        assert!(final_text.starts_with("start\n"));
        for index in 0..12 {
            assert!(final_text.contains(&format!("changed-{index}\n")));
            assert!(!final_text.contains(&format!("line-{index}\n")));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn edit_tool_handles_crlf_ambiguity_binary_and_replace_all() {
        use std::os::unix::fs::PermissionsExt;
        let root = crate::fs_safety::unique_root("edit-tool");
        let path = root.path().join("file");
        std::fs::write(&path, b"same\r\nsame\r\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let edit = Edit::default();
        let ambiguous = edit
            .call(
                json!({"file_path":"file","old_string":"same","new_string":"x"}),
                &cx(root.path()),
            )
            .await;
        assert!(ambiguous.is_error && ambiguous.text.contains("FS_AMBIGUOUS_EDIT"));
        let replaced = edit
            .call(
                json!({"file_path":"file","old_string":"same","new_string":"x","replace_all":true}),
                &cx(root.path()),
            )
            .await;
        assert!(!replaced.is_error, "{}", replaced.text);
        assert_eq!(std::fs::read(&path).unwrap(), b"x\r\nx\r\n");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        std::fs::write(&path, b"before\0after").unwrap();
        let binary = edit
            .call(
                json!({"file_path":"file","old_string":"before","new_string":"x"}),
                &cx(root.path()),
            )
            .await;
        assert!(binary.is_error && binary.text.contains("FS_NOT_TEXT"));
        assert_eq!(std::fs::read(&path).unwrap(), b"before\0after");
    }
}
