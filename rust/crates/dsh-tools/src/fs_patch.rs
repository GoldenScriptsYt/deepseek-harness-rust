use crate::fs_safety::{FsCode, FsError};

const BEGIN: &str = "*** Begin Patch";
const END: &str = "*** End Patch";
const EOF: &str = "*** End of File";
const MOVE: &str = "*** Move to: ";
const ADD: &str = "*** Add File: ";
const DELETE: &str = "*** Delete File: ";
const UPDATE: &str = "*** Update File: ";

#[derive(Debug, Clone)]
pub(crate) struct PatchHunk {
    pub(crate) line: usize,
    pub(crate) context: Option<String>,
    pub(crate) old: Vec<String>,
    pub(crate) new: Vec<String>,
    pub(crate) eof: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PatchOperation {
    Add {
        path: String,
        line: usize,
        content: String,
    },
    Delete {
        path: String,
        line: usize,
    },
    Update {
        path: String,
        line: usize,
        hunks: Vec<PatchHunk>,
        move_to: Option<String>,
    },
}

impl PatchOperation {
    pub(crate) fn line(&self) -> usize {
        match self {
            Self::Add { line, .. } | Self::Delete { line, .. } | Self::Update { line, .. } => *line,
        }
    }

    pub(crate) fn path(&self) -> &str {
        match self {
            Self::Add { path, .. } | Self::Delete { path, .. } | Self::Update { path, .. } => path,
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Add { .. } => "add",
            Self::Delete { .. } => "delete",
            Self::Update {
                move_to: Some(_), ..
            } => "move",
            Self::Update { .. } => "update",
        }
    }
}

#[derive(Debug, Clone)]
struct InputLine {
    text: String,
    number: usize,
}

fn whitespace(c: char) -> bool {
    c.is_whitespace()
}

fn trim(value: &str) -> &str {
    value.trim_matches(whitespace)
}
fn trim_end(value: &str) -> &str {
    value.trim_end_matches(whitespace)
}

fn syntax(line: usize, reason: impl AsRef<str>) -> FsError {
    FsError::at_line(
        FsCode::PatchSyntax,
        line,
        format!("Invalid patch at line {line}: {}", reason.as_ref()),
    )
}

fn patch_lines(patch: &str) -> Result<Vec<InputLine>, FsError> {
    let leading: String = patch.chars().take_while(|c| whitespace(*c)).collect();
    let offset = leading.bytes().filter(|byte| *byte == b'\n').count();
    let trimmed = trim(patch);
    let mut lines: Vec<InputLine> = trimmed
        .split('\n')
        .enumerate()
        .map(|(index, text)| InputLine {
            text: text.to_string(),
            number: offset + index + 1,
        })
        .collect();
    if lines.is_empty() {
        lines.push(InputLine {
            text: String::new(),
            number: 1,
        });
    }
    let first = lines[0].text.clone();
    let last = lines.last().unwrap().text.clone();
    if matches!(first.as_str(), "<<EOF" | "<<'EOF'" | "<<\"EOF\"")
        && last.ends_with("EOF")
        && lines.len() >= 4
    {
        let retained = lines.len() - 2;
        lines = lines.into_iter().skip(1).take(retained).collect();
    }
    if trim(&lines[0].text) != BEGIN {
        return Err(syntax(lines[0].number, format!("Expected '{BEGIN}'")));
    }
    if trim(&lines.last().unwrap().text) != END {
        return Err(syntax(
            lines.last().unwrap().number,
            format!("Expected '{END}'"),
        ));
    }
    let last_index = lines.len() - 1;
    for (index, line) in lines.iter_mut().enumerate() {
        if index < last_index {
            line.text = line
                .text
                .strip_suffix('\r')
                .unwrap_or(&line.text)
                .to_string();
        }
    }
    Ok(lines)
}

fn empty_hunk(hunk: &PatchHunk) -> bool {
    hunk.old.is_empty() && hunk.new.is_empty()
}

fn ensure_update(operation: Option<&PatchOperation>, line: usize) -> Result<(), FsError> {
    let Some(PatchOperation::Update { path, hunks, .. }) = operation else {
        return Ok(());
    };
    let Some(hunk) = hunks.last() else {
        return Err(syntax(
            line,
            format!("Update for '{path}' requires at least one hunk"),
        ));
    };
    if empty_hunk(hunk) {
        return Err(syntax(
            line,
            "Update hunk must contain context, added, or removed lines",
        ));
    }
    Ok(())
}

fn read_update(operation: &mut PatchOperation, input: &InputLine) -> Result<(), FsError> {
    let PatchOperation::Update { hunks, move_to, .. } = operation else {
        unreachable!()
    };
    let text = trim_end(&input.text);
    let previous = hunks.last();
    let marker = text == "@@" || text.starts_with("@@ ");
    if previous.is_some_and(|hunk| hunk.eof) {
        if text.is_empty() {
            return Ok(());
        }
        if !marker {
            return Err(syntax(
                input.number,
                "Expected '@@' after '*** End of File'",
            ));
        }
    }
    if previous.is_none() && move_to.is_none() && text.starts_with(MOVE) {
        *move_to = Some(text[MOVE.len()..].to_string());
        return Ok(());
    }
    if marker {
        if previous.is_some_and(empty_hunk) {
            return Err(syntax(
                input.number,
                "Update hunk must contain context, added, or removed lines",
            ));
        }
        let context = text.strip_prefix("@@ ").map(str::to_string);
        hunks.push(PatchHunk {
            line: input.number,
            context,
            old: Vec::new(),
            new: Vec::new(),
            eof: false,
        });
        return Ok(());
    }
    if text == EOF {
        if let Some(hunk) = previous {
            if empty_hunk(hunk) {
                return Err(syntax(
                    input.number,
                    "Update hunk must contain context, added, or removed lines",
                ));
            }
            hunks.last_mut().unwrap().eof = true;
        }
        return Ok(());
    }
    let prefix = input.text.chars().next();
    if !input.text.is_empty() && !matches!(prefix, Some(' ') | Some('+') | Some('-')) {
        return Err(syntax(
            input.number,
            "Expected a context (' '), added ('+'), removed ('-') line, or '@@'",
        ));
    }
    if previous.is_none() {
        hunks.push(PatchHunk {
            line: input.number,
            context: None,
            old: Vec::new(),
            new: Vec::new(),
            eof: false,
        });
    }
    let content = input.text.chars().skip(1).collect::<String>();
    let hunk = hunks.last_mut().unwrap();
    match prefix {
        Some('+') => hunk.new.push(content),
        Some('-') => hunk.old.push(content),
        _ => {
            hunk.old.push(content.clone());
            hunk.new.push(content);
        }
    }
    Ok(())
}

pub(crate) fn parse_patch(patch: &str) -> Result<Vec<PatchOperation>, FsError> {
    let lines = patch_lines(patch)?;
    let last_index = lines.len() - 1;
    let mut operations: Vec<PatchOperation> = Vec::new();
    let mut ended = false;
    for (index, line) in lines.iter().enumerate().skip(1) {
        let operation = operations.last();
        let header = if operation.is_some_and(|op| matches!(op, PatchOperation::Update { .. }))
            && index != last_index
        {
            trim_end(&line.text)
        } else {
            trim(&line.text)
        };
        if ended && index != last_index {
            if !trim(&line.text).is_empty() {
                return Err(syntax(
                    line.number,
                    format!("Unexpected content after '{END}'"),
                ));
            }
            continue;
        }
        if header == END {
            ensure_update(operation, line.number)?;
            ended = true;
        } else if let Some(path) = header.strip_prefix(ADD) {
            ensure_update(operation, line.number)?;
            operations.push(PatchOperation::Add {
                path: path.to_string(),
                line: line.number,
                content: String::new(),
            });
        } else if let Some(path) = header.strip_prefix(DELETE) {
            ensure_update(operation, line.number)?;
            operations.push(PatchOperation::Delete {
                path: path.to_string(),
                line: line.number,
            });
        } else if let Some(path) = header.strip_prefix(UPDATE) {
            ensure_update(operation, line.number)?;
            operations.push(PatchOperation::Update {
                path: path.to_string(),
                line: line.number,
                hunks: Vec::new(),
                move_to: None,
            });
        } else if operations.is_empty() && header.starts_with("*** Environment ID:") {
            return Err(syntax(line.number, "Environment ID headers are unsupported; the caller selects the filesystem provider"));
        } else if let Some(PatchOperation::Add { content, .. }) =
            operations.last_mut().filter(|_| line.text.starts_with('+'))
        {
            content.push_str(&line.text[1..]);
            content.push('\n');
        } else if let Some(operation) = operations
            .last_mut()
            .filter(|op| matches!(op, PatchOperation::Update { .. }))
        {
            read_update(operation, line)?;
        } else {
            return Err(syntax(line.number, "Expected an Add File, Delete File, or Update File header; added content must start with +"));
        }
    }
    Ok(operations)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchPass {
    Exact,
    TrimEnd,
    Trim,
    Punctuation,
}

fn normalize(value: &str, pass: MatchPass) -> String {
    let trimmed = match pass {
        MatchPass::Exact => return value.to_string(),
        MatchPass::TrimEnd => value.trim_end_matches(whitespace),
        MatchPass::Trim | MatchPass::Punctuation => value.trim_matches(whitespace),
    }
    .to_string();
    if pass != MatchPass::Punctuation {
        return trimmed;
    }
    trimmed
        .chars()
        .map(|c| match c {
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{2018}'..='\u{201b}' => '\'',
            '\u{201c}'..='\u{201f}' => '"',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            _ => c,
        })
        .collect()
}

fn seek(lines: &[String], pattern: &[String], start: usize, eof: bool) -> Option<usize> {
    if pattern.is_empty() {
        return Some(start);
    }
    if pattern.len() > lines.len() {
        return None;
    }
    let last = lines.len() - pattern.len();
    let first = if eof { last } else { start };
    for pass in [
        MatchPass::Exact,
        MatchPass::TrimEnd,
        MatchPass::Trim,
        MatchPass::Punctuation,
    ] {
        let expected: Vec<String> = pattern.iter().map(|line| normalize(line, pass)).collect();
        for index in first..=last {
            if expected
                .iter()
                .enumerate()
                .all(|(offset, line)| normalize(&lines[index + offset], pass) == *line)
            {
                return Some(index);
            }
        }
    }
    None
}

#[derive(Debug)]
struct Replacement {
    start: usize,
    old_len: usize,
    new: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum HunkResult {
    Applied(String),
    Mismatch {
        line: usize,
        stage: &'static str,
        pattern: Vec<String>,
    },
}

pub(crate) fn apply_hunks(before: &str, hunks: &[PatchHunk]) -> HunkResult {
    let mut original: Vec<String> = before.split('\n').map(str::to_string).collect();
    if original.last().is_some_and(|line| line.is_empty()) {
        original.pop();
    }
    let mut replacements = Vec::new();
    let mut cursor = 0usize;
    for hunk in hunks {
        if let Some(context) = &hunk.context {
            let pattern = vec![context.clone()];
            let Some(found) = seek(&original, &pattern, cursor, false) else {
                return HunkResult::Mismatch {
                    line: hunk.line,
                    stage: "context",
                    pattern,
                };
            };
            cursor = found + 1;
        }
        if hunk.old.is_empty() {
            let start = if original.last().is_some_and(|line| line.is_empty()) {
                original.len() - 1
            } else {
                original.len()
            };
            replacements.push(Replacement {
                start,
                old_len: 0,
                new: hunk.new.clone(),
            });
            continue;
        }
        let mut pattern = hunk.old.clone();
        let mut new = hunk.new.clone();
        let mut found = seek(&original, &pattern, cursor, hunk.eof);
        if found.is_none() && pattern.last().is_some_and(|line| line.is_empty()) {
            pattern.pop();
            if new.last().is_some_and(|line| line.is_empty()) {
                new.pop();
            }
            found = seek(&original, &pattern, cursor, hunk.eof);
        }
        let Some(found) = found else {
            return HunkResult::Mismatch {
                line: hunk.line,
                stage: "oldLines",
                pattern: hunk.old.clone(),
            };
        };
        replacements.push(Replacement {
            start: found,
            old_len: pattern.len(),
            new,
        });
        cursor = found + pattern.len();
    }
    replacements.sort_by_key(|replacement| replacement.start);
    let mut lines = original;
    for replacement in replacements.into_iter().rev() {
        lines.splice(
            replacement.start..replacement.start + replacement.old_len,
            replacement.new,
        );
    }
    if lines.last().is_none_or(|line| !line.is_empty()) {
        lines.push(String::new());
    }
    HunkResult::Applied(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_tracks_original_lines_and_strict_markers() {
        let patch = "\n<<EOF\n*** Begin Patch\n*** Add File: a\n+one\n*** End Patch\nEOF\n";
        let operations = parse_patch(patch).unwrap();
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].line(), 4);
        match &operations[0] {
            PatchOperation::Add { content, .. } => assert_eq!(content, "one\n"),
            _ => panic!(),
        }
        let error =
            parse_patch("*** Begin Patch\n*** Add File: a\none\n*** End Patch").unwrap_err();
        assert_eq!(error.line, Some(3));
        assert_eq!(error.code, FsCode::PatchSyntax);
    }

    #[test]
    fn hunks_search_original_text_and_prefer_exact_matches() {
        let hunks = vec![PatchHunk {
            line: 2,
            context: None,
            old: vec!["a ".into()],
            new: vec!["b".into()],
            eof: false,
        }];
        match apply_hunks("a\na \na\n", &hunks) {
            HunkResult::Applied(text) => assert_eq!(text, "a\nb\na\n"),
            _ => panic!(),
        }
    }

    #[test]
    fn eof_and_empty_insertions_follow_tail_semantics() {
        let append = vec![PatchHunk {
            line: 2,
            context: None,
            old: Vec::new(),
            new: vec!["b".into()],
            eof: false,
        }];
        assert!(
            matches!(apply_hunks("a\n", &append), HunkResult::Applied(text) if text == "a\nb\n")
        );
        let eof = vec![PatchHunk {
            line: 2,
            context: Some("a".into()),
            old: vec!["a".into()],
            new: vec!["z".into()],
            eof: true,
        }];
        assert!(
            matches!(apply_hunks("x\na\nz\na\n", &eof), HunkResult::Applied(text) if text == "x\na\nz\nz\n")
        );
    }

    #[test]
    fn punctuation_and_trailing_empty_retry() {
        let hunk = vec![PatchHunk {
            line: 2,
            context: None,
            old: vec!["“a”".into()],
            new: vec!["b".into()],
            eof: false,
        }];
        assert!(
            matches!(apply_hunks("\"a\"\n", &hunk), HunkResult::Applied(text) if text == "b\n")
        );
        let hunk = vec![PatchHunk {
            line: 2,
            context: None,
            old: vec!["a".into(), "".into()],
            new: vec!["b".into(), "".into()],
            eof: false,
        }];
        assert!(matches!(apply_hunks("a", &hunk), HunkResult::Applied(text) if text == "b\n"));
    }
}
