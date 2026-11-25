//! On-disk layout of Session artifacts. Port of the path helpers in
//! `packages/session/session-persistence-jsonl/src/format.ts` and
//! `packages/session/session-format/src/filename.ts`. Names must match the TypeScript
//! implementation exactly or existing Sessions are not found.

use std::path::{Path, PathBuf};

/// Physical encoding of a JSONL artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Zstd,
    None,
}

impl Compression {
    fn suffix(self) -> &'static str {
        match self {
            Compression::Zstd => ".zstd",
            Compression::None => "",
        }
    }
}

fn is_safe_unit(unit: u16) -> bool {
    matches!(unit, 0x30..=0x39 | 0x41..=0x5A | 0x61..=0x7A | 0x2E | 0x5F | 0x2D)
}

fn escape_unit(out: &mut String, unit: u16) {
    out.push_str(&format!("~{unit:04X}"));
}

/// Escape a string into one safe path segment. Operates on UTF-16 code units like the
/// TypeScript original; `~` and every unit outside `[A-Za-z0-9._-]` becomes `~XXXX`.
/// Returns `None` for the empty string.
pub fn encode_segment(raw: &str) -> Option<String> {
    if raw.is_empty() {
        return None;
    }
    if raw == "." {
        return Some("~002E".into());
    }
    if raw == ".." {
        return Some("~002E~002E".into());
    }
    let mut out = String::new();
    for unit in raw.encode_utf16() {
        if is_safe_unit(unit) {
            out.push(unit as u8 as char);
        } else {
            escape_unit(&mut out, unit);
        }
    }
    Some(out)
}

/// Readable, bounded project directory key. Separators and `:` collapse to `-`.
/// Returns `None` for an empty path.
pub fn project_key(cwd: &str) -> Option<String> {
    if cwd.is_empty() {
        return None;
    }
    let mut readable = String::new();
    let mut separator_run = false;
    for unit in cwd.encode_utf16() {
        if matches!(unit, 0x2F | 0x5C | 0x3A) {
            if !separator_run {
                readable.push('-');
            }
            separator_run = true;
        } else if is_safe_unit(unit) {
            readable.push(unit as u8 as char);
            separator_run = false;
        } else {
            escape_unit(&mut readable, unit);
            separator_run = false;
        }
    }
    let trimmed = readable.trim_start_matches('-');
    let slug = if trimmed.is_empty() { "root" } else { trimmed };
    // Slice by UTF-16 units, which equal chars here because the key is ASCII.
    let bounded: String = slug.chars().take(251).collect();
    Some(format!("--{bounded}--"))
}

/// Project directory under `root`; `None` cwd selects `_no-cwd`.
pub fn project_dir(root: &Path, cwd: Option<&str>) -> Option<PathBuf> {
    match cwd {
        None => Some(root.join("_no-cwd")),
        Some(c) => project_key(c).map(|k| root.join(k)),
    }
}

/// Directory owned by one Session.
pub fn session_dir(root: &Path, cwd: Option<&str>, id: &str) -> Option<PathBuf> {
    Some(project_dir(root, cwd)?.join(encode_segment(id)?))
}

/// Raw log basename of one generation: `session.jsonl` for v0, `session.vN.jsonl` after.
pub fn session_format_log_filename(version: u64) -> String {
    if version == 0 {
        "session.jsonl".into()
    } else {
        format!("session.v{version}.jsonl")
    }
}

/// Parse a canonical raw log basename back to its generation. Leading zeros, `.v0`,
/// uppercase, temporary and compressed names are not canonical.
pub fn parse_session_format_log_filename(name: &str) -> Option<u64> {
    if name == "session.jsonl" {
        return Some(0);
    }
    let digits = name.strip_prefix("session.v")?.strip_suffix(".jsonl")?;
    if digits.is_empty() || digits.starts_with('0') || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let version: u64 = digits.parse().ok()?;
    // Number.isSafeInteger bound in the TypeScript reader.
    (version <= 9_007_199_254_740_991).then_some(version)
}

/// Filename of one generation for the selected encoding.
pub fn generation_log_filename(version: u64, compression: Compression) -> String {
    format!(
        "{}{}",
        session_format_log_filename(version),
        compression.suffix()
    )
}

/// Parse a generation filename for the selected encoding.
pub fn parse_generation_log_filename(name: &str, compression: Compression) -> Option<u64> {
    parse_session_format_log_filename(name.strip_suffix(compression.suffix())?)
}

/// Full path of one immutable generation.
pub fn generation_log_path(
    root: &Path,
    cwd: Option<&str>,
    id: &str,
    version: u64,
    compression: Compression,
) -> Option<PathBuf> {
    Some(session_dir(root, cwd, id)?.join(generation_log_filename(version, compression)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_escape_like_typescript() {
        assert_eq!(encode_segment("a~b/c").unwrap(), "a~007Eb~002Fc");
        assert_eq!(encode_segment("..").unwrap(), "~002E~002E");
        assert_eq!(encode_segment("é").unwrap(), "~00E9");
        // Astral characters are two UTF-16 units, each escaped.
        assert_eq!(encode_segment("😀").unwrap(), "~D83D~DE00");
        assert!(encode_segment("").is_none());
    }

    #[test]
    fn project_keys_collapse_separators() {
        assert_eq!(
            project_key("C:\\Users\\me\\proj").unwrap(),
            "--C-Users-me-proj--"
        );
        assert_eq!(project_key("/a//b").unwrap(), "--a-b--");
        assert_eq!(project_key("/").unwrap(), "--root--");
    }

    #[test]
    fn generation_names_round_trip() {
        assert_eq!(
            generation_log_filename(0, Compression::None),
            "session.jsonl"
        );
        assert_eq!(
            generation_log_filename(4, Compression::Zstd),
            "session.v4.jsonl.zstd"
        );
        assert_eq!(
            parse_generation_log_filename("session.v4.jsonl.zstd", Compression::Zstd),
            Some(4)
        );
        assert_eq!(
            parse_generation_log_filename("session.v4.jsonl", Compression::Zstd),
            None
        );
        assert_eq!(parse_session_format_log_filename("session.v04.jsonl"), None);
        assert_eq!(parse_session_format_log_filename("session.v0.jsonl"), None);
        assert_eq!(parse_session_format_log_filename("session.jsonl"), Some(0));
    }
}
