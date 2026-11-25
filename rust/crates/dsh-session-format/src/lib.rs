//! Reader and writer for the current (v4) Session JSONL log.
//!
//! Port of `packages/session/session-persistence-jsonl/src/format.ts`. A log is one
//! header line followed by event rows. A final row without a trailing newline is a torn
//! tail and is excluded from the committed prefix.

pub mod layout;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Session format version this crate reads and writes.
pub const SESSION_FORMAT_VERSION: u64 = 4;

/// Failure while reading a Session log.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("empty or header-less session log")]
    MissingHeader,
    #[error("malformed session header: {0}")]
    Header(String),
    #[error("session header uses retired policy baseline fields")]
    RetiredHeaderField,
    #[error("session format version {found} is not readable by this build (expected {expected})")]
    UnsupportedVersion { found: u64, expected: u64 },
    #[error("corrupt session log: unparsable committed event at line {0}")]
    Unparsable(usize),
    #[error("corrupt session log: invalid committed event at line {line}: {reason}")]
    InvalidEvent { line: usize, reason: String },
}

/// First record of a log. Unknown keys are refused, matching the TypeScript reader.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Header {
    #[serde(rename = "type")]
    pub kind: HeaderTag,
    pub version: u64,
    pub id: String,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session: Option<String>,
    pub is_seeded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    pub delegation_depth: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_preset: Option<String>,
}

/// The literal `"session"` header discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderTag {
    #[serde(rename = "session")]
    Session,
}

/// Session origin marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    Subagent,
}

/// One event row. Fields other than `type` and `data` (for example `surfaceOp`) are kept
/// verbatim in `extra`, so unknown additive fields survive a read/write cycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub data: Value,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Event {
    /// Position of this row in the log, as the producer stamped it.
    pub fn seq(&self) -> Option<u64> {
        self.extra.get("seq").and_then(Value::as_u64)
    }

    /// Commit time in milliseconds since the epoch, as the producer stamped it.
    pub fn time(&self) -> Option<u64> {
        self.extra.get("time").and_then(Value::as_u64)
    }
}

/// How a corrupt committed row is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Any corrupt row fails the scan.
    Strict,
    /// A corrupt row ends the committed prefix unless a later `turn/end` row exists.
    Recoverable,
}

/// Result of scanning a log buffer.
#[derive(Debug, Clone)]
pub struct Scan {
    pub header: Header,
    pub events: Vec<Event>,
    /// Byte offset just past the last committed row; safe to truncate to and append at.
    pub committed_bytes: usize,
}

/// Parse and validate the header line.
pub fn parse_header(line: &[u8]) -> Result<Header, FormatError> {
    let value: Value =
        serde_json::from_slice(line).map_err(|e| FormatError::Header(e.to_string()))?;
    if let Some(obj) = value.as_object() {
        if obj.contains_key("sandboxMode") || obj.contains_key("approvalPolicy") {
            return Err(FormatError::RetiredHeaderField);
        }
        if let Some(found) = obj.get("version").and_then(Value::as_u64) {
            if found != SESSION_FORMAT_VERSION {
                return Err(FormatError::UnsupportedVersion {
                    found,
                    expected: SESSION_FORMAT_VERSION,
                });
            }
        }
    }
    serde_json::from_value(value).map_err(|e| FormatError::Header(e.to_string()))
}

/// Scan a complete or torn log buffer into its header and committed event prefix.
pub fn scan(buffer: &[u8], recovery: Recovery) -> Result<Scan, FormatError> {
    let header_end = buffer
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(FormatError::MissingHeader)?;
    let header = parse_header(&buffer[..header_end])?;

    let mut events = Vec::new();
    let mut committed = header_end + 1;
    let mut pos = header_end + 1;
    let mut line_no = 0usize;
    let mut issue: Option<FormatError> = None;

    while let Some(rel) = buffer[pos..].iter().position(|&b| b == b'\n') {
        let end = pos + rel;
        let row = &buffer[pos..end];
        pos = end + 1;
        line_no += 1;

        let parsed: Result<Event, FormatError> = match serde_json::from_slice::<Value>(row) {
            Err(_) => Err(FormatError::Unparsable(line_no)),
            Ok(value) => {
                serde_json::from_value::<Event>(value).map_err(|e| FormatError::InvalidEvent {
                    line: line_no,
                    reason: e.to_string(),
                })
            }
        };

        if let Some(existing) = issue.take() {
            // After a recoverable corruption, rows are dropped, but a later turn/end proves
            // the damage was mid-log rather than a torn tail.
            let is_turn_end = matches!(&parsed, Ok(e) if e.kind == "turn/end");
            if is_turn_end {
                return Err(existing);
            }
            issue = Some(existing);
            continue;
        }

        match parsed {
            Ok(event) => {
                events.push(event);
                committed = pos;
            }
            Err(err) => match recovery {
                Recovery::Strict => return Err(err),
                Recovery::Recoverable => issue = Some(err),
            },
        }
    }

    Ok(Scan {
        header,
        events,
        committed_bytes: committed,
    })
}

/// Serialize a header as one newline-terminated JSONL record.
pub fn encode_header(header: &Header) -> String {
    let mut line = serde_json::to_string(header).expect("header serializes");
    line.push('\n');
    line
}

/// Serialize an event as one newline-terminated JSONL record.
pub fn encode_event(event: &Event) -> String {
    let mut line = serde_json::to_string(event).expect("event serializes");
    line.push('\n');
    line
}
