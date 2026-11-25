//! Read-only Session list, search, query, and export RPCs.
//!
//! `session/list` and `session/search` follow the TypeScript Session controller.
//! The query methods expose the Session query service contracts, while export
//! methods are headless JSON adaptations of the TypeScript HTTP download routes.

use crate::api::ApiContext;
use dsh_gateway::{Gateway, RpcError};
use dsh_session_format::layout::{encode_segment, project_dir, session_format_log_filename};
use dsh_session_format::{parse_header, Event, Header, SESSION_FORMAT_VERSION};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 100;
const SNIPPET_CHARS: usize = 240;
const HOST_SEARCH_LIMIT: usize = 20;
const HOST_QUERY_MAX_UTF16: usize = 500;
const MAX_PROJECTS: usize = 1_024;
const MAX_SESSIONS: usize = 20_000;
const MAX_EVENTS_PER_SESSION: usize = 100_000;
const MAX_SESSION_BYTES: u64 = 64 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 1024 * 1024;
const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_EXPORT_BYTES: usize = 128 * 1024 * 1024;
const MAX_ZIP_BYTES: usize = (MAX_EXPORT_BYTES / 4) * 3;
const MAX_MARKDOWN_BYTES: usize = 64 * 1024 * 1024;
const MAX_CURSOR_BYTES: usize = 16 * 1024;
const MAX_FILTER_VALUES: usize = 32_766;
const CURSOR_INSTANCE: &str = "dsh-cli-memory-v1";

/// Register the read-only Session query and export RPCs.
pub fn register_all(gw: &Gateway, cx: Arc<ApiContext>) {
    let c = cx.clone();
    gw.rpc("session/query/sessions", move |payload| {
        let c = c.clone();
        async move { blocking(c, payload, query_sessions).await }
    });
    let c = cx.clone();
    gw.rpc("session/query/events", move |payload| {
        let c = c.clone();
        async move { blocking(c, payload, query_events).await }
    });
    let c = cx.clone();
    gw.rpc("session/query/session", move |payload| {
        let c = c.clone();
        async move { blocking(c, payload, read_session).await }
    });
    let c = cx.clone();
    gw.rpc("session/export", move |payload| {
        let c = c.clone();
        async move { blocking(c, payload, export_session).await }
    });
    let c = cx;
    gw.rpc("session/markdown", move |payload| {
        let c = c.clone();
        async move { blocking(c, payload, export_markdown).await }
    });
}

async fn blocking<F>(cx: Arc<ApiContext>, payload: Value, operation: F) -> Result<Value, RpcError>
where
    F: FnOnce(&ApiContext, &Value) -> Result<Value, RpcError> + Send + 'static,
{
    match tokio::task::spawn_blocking(move || operation(&cx, &payload)).await {
        Ok(result) => result,
        Err(error) => Err(rpc_error(
            "gateway/internal",
            format!("session query worker failed: {error}"),
            json!({}),
        )),
    }
}

fn request_of(payload: &Value) -> &Value {
    let args = payload.get("args").unwrap_or(payload);
    args.get("request").unwrap_or(args)
}

fn rpc_error(code: &str, message: impl Into<String>, details: Value) -> RpcError {
    RpcError {
        code: code.into(),
        message: message.into(),
        details,
    }
}

fn query_error(code: &str, message: impl Into<String>) -> RpcError {
    rpc_error(code, message, json!({}))
}

fn bad_request(message: impl Into<String>) -> RpcError {
    rpc_error("gateway/bad-request", message, json!({}))
}

fn corrupt_error(id: &str, path: &Path, message: impl Into<String>) -> RpcError {
    rpc_error(
        "SESSION_QUERY_CORRUPT_SESSION",
        format!("corrupt session \"{id}\": {}", message.into()),
        json!({"sessionId": id, "path": path.display().to_string(), "kind": "corrupt"}),
    )
}

fn not_found(id: &str) -> RpcError {
    query_error(
        "SESSION_QUERY_SESSION_NOT_FOUND",
        format!("session \"{id}\" not found"),
    )
}

fn resource_error(message: impl Into<String>) -> RpcError {
    rpc_error("gateway/resource-exhausted", message, json!({}))
}

#[derive(Clone)]
struct Artifact {
    header: Header,
    path: PathBuf,
    size_bytes: u64,
    modified_ms: u64,
    modified_ns: u128,
}

struct Loaded {
    artifact: Artifact,
    events: Vec<Event>,
}

struct Budget {
    sessions: usize,
    bytes: u64,
}

impl Budget {
    fn new() -> Self {
        Self {
            sessions: 0,
            bytes: 0,
        }
    }

    fn count_session(&mut self) -> Result<(), RpcError> {
        self.sessions += 1;
        if self.sessions > MAX_SESSIONS {
            return Err(resource_error(format!(
                "session query exceeds the {MAX_SESSIONS}-Session bound"
            )));
        }
        Ok(())
    }

    fn add_bytes(&mut self, bytes: u64) -> Result<(), RpcError> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > MAX_TOTAL_BYTES {
            return Err(resource_error(format!(
                "session query exceeds the {MAX_TOTAL_BYTES}-byte scan bound"
            )));
        }
        Ok(())
    }
}

fn modified_ns(metadata: &fs::Metadata) -> u128 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_nanos())
}

fn sorted_directories(path: &Path) -> Result<Vec<PathBuf>, RpcError> {
    sorted_directories_bounded(path, MAX_PROJECTS)
}

fn directory_read_error(path: &Path, action: &str, error: std::io::Error) -> RpcError {
    rpc_error(
        "SESSION_QUERY_PERSISTENCE_FAILED",
        format!("session persistence {action} failed: {error}"),
        json!({"path": path.display().to_string()}),
    )
}

fn sorted_directories_bounded(path: &Path, maximum: usize) -> Result<Vec<PathBuf>, RpcError> {
    let mut output = Vec::new();
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(output),
        Err(error) => return Err(directory_read_error(path, "read", error)),
    };
    for entry in entries {
        let entry = entry.map_err(|error| directory_read_error(path, "read", error))?;
        if entry
            .file_type()
            .map_err(|error| {
                rpc_error(
                    "SESSION_QUERY_PERSISTENCE_FAILED",
                    format!("session persistence stat failed: {error}"),
                    json!({"path": entry.path().display().to_string()}),
                )
            })?
            .is_dir()
        {
            output.push(entry.path());
            if output.len() > maximum {
                return Err(resource_error(format!(
                    "session query exceeds the {maximum}-directory bound"
                )));
            }
        }
    }
    output.sort();
    Ok(output)
}

enum BoundedLine {
    Eof,
    Complete(Vec<u8>),
    Torn,
    TooLarge,
}

fn read_bounded_line<R: BufRead>(reader: &mut R, maximum: usize) -> std::io::Result<BoundedLine> {
    let mut line = Vec::new();
    let read = reader
        .by_ref()
        .take((maximum + 1) as u64)
        .read_until(b'\n', &mut line)?;
    if line.len() > maximum {
        return Ok(BoundedLine::TooLarge);
    }
    match read {
        0 => Ok(BoundedLine::Eof),
        _ if line.last() == Some(&b'\n') => Ok(BoundedLine::Complete(line)),
        _ => Ok(BoundedLine::Torn),
    }
}

fn file_open_error(path: &Path, error: std::io::Error) -> RpcError {
    directory_read_error(path, "read", error)
}

fn file_metadata_error(path: &Path, error: std::io::Error) -> RpcError {
    directory_read_error(path, "stat", error)
}

fn read_header(path: &Path) -> Result<(Header, u64), RpcError> {
    let file = File::open(path).map_err(|error| file_open_error(path, error))?;
    let metadata = file
        .metadata()
        .map_err(|error| file_metadata_error(path, error))?;
    let id = path
        .parent()
        .and_then(Path::file_name)
        .unwrap_or_default()
        .to_string_lossy();
    let mut reader = BufReader::new(file);
    let line = match read_bounded_line(&mut reader, MAX_HEADER_BYTES)
        .map_err(|error| file_open_error(path, error))?
    {
        BoundedLine::Complete(line) => line,
        BoundedLine::TooLarge => {
            return Err(resource_error(format!(
                "session header exceeds the {MAX_HEADER_BYTES}-byte bound"
            )))
        }
        _ => {
            return Err(corrupt_error(
                &id,
                path,
                "missing newline-terminated header",
            ))
        }
    };
    let header = parse_header(&line[..line.len() - 1])
        .map_err(|error| corrupt_error(&id, path, error.to_string()))?;
    if header.id.is_empty() {
        return Err(corrupt_error(&id, path, "header id must not be empty"));
    }
    Ok((header, metadata.len()))
}

fn inventory(
    root: &Path,
    cwd: Option<&str>,
    budget: &mut Budget,
) -> Result<Vec<Artifact>, RpcError> {
    let projects = if let Some(cwd) = cwd {
        if cwd.is_empty() {
            return Err(bad_request("cwd must not be empty"));
        }
        vec![project_dir(root, Some(cwd)).ok_or_else(|| bad_request("cwd is not encodable"))?]
    } else {
        sorted_directories(root)?
    };
    if projects.len() > MAX_PROJECTS {
        return Err(resource_error(format!(
            "session query exceeds the {MAX_PROJECTS}-project bound"
        )));
    }
    let filename = session_format_log_filename(SESSION_FORMAT_VERSION);
    let mut output = Vec::new();
    for project in projects {
        for directory in sorted_directories_bounded(&project, MAX_SESSIONS)? {
            let path = directory.join(&filename);
            let metadata = match fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let entries = fs::read_dir(&directory)
                        .map_err(|error| directory_read_error(&directory, "read", error))?;
                    let mut retired = false;
                    for entry in entries {
                        let entry = entry
                            .map_err(|error| directory_read_error(&directory, "read", error))?;
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        if name.starts_with("session.v")
                            && name.ends_with(".jsonl")
                            && name != filename.as_str()
                        {
                            retired = true;
                            break;
                        }
                    }
                    if retired {
                        let id = directory.file_name().unwrap_or_default().to_string_lossy();
                        return Err(rpc_error(
                            "session/migration-required",
                            format!(
                                "session \"{id}\" requires migration to v{SESSION_FORMAT_VERSION}"
                            ),
                            json!({"sessionId": id, "path": directory.display().to_string()}),
                        ));
                    }
                    continue;
                }
                Err(error) => {
                    return Err(rpc_error(
                        "SESSION_QUERY_PERSISTENCE_FAILED",
                        format!("session persistence stat failed: {error}"),
                        json!({"path": path.display().to_string()}),
                    ))
                }
            };
            if !metadata.is_file() {
                return Err(corrupt_error(
                    &directory.file_name().unwrap_or_default().to_string_lossy(),
                    &path,
                    "current Session log is not a regular file",
                ));
            }
            if metadata.len() > MAX_SESSION_BYTES {
                return Err(resource_error(format!(
                    "session log exceeds the {MAX_SESSION_BYTES}-byte bound"
                )));
            }
            let (header, _) = read_header(&path)?;
            if cwd.is_some_and(|scope| header.cwd.as_deref() != Some(scope)) {
                continue;
            }
            budget.count_session()?;
            budget.add_bytes(metadata.len())?;
            let expected_segment = encode_segment(&header.id);
            let expected_project = match header.cwd.as_deref() {
                None => Some(root.join("_no-cwd")),
                Some(value) => project_dir(root, Some(value)),
            };
            if expected_segment.as_deref() != directory.file_name().and_then(|name| name.to_str())
                || expected_project.as_deref() != Some(project.as_path())
                || header.cwd.as_ref().is_some_and(|value| value.is_empty())
            {
                return Err(corrupt_error(
                    &header.id,
                    &path,
                    "header identity does not match its project/session directory",
                ));
            }
            output.push(Artifact {
                header,
                path,
                size_bytes: metadata.len(),
                modified_ms: metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map_or(0, |duration| duration.as_millis() as u64),
                modified_ns: modified_ns(&metadata),
            });
        }
    }
    output.sort_by(|left, right| {
        right
            .modified_ms
            .cmp(&left.modified_ms)
            .then_with(|| left.header.id.cmp(&right.header.id))
            .then_with(|| left.path.cmp(&right.path))
    });
    Ok(output)
}

fn changed_error(id: &str) -> RpcError {
    rpc_error(
        "session/changed",
        format!("session \"{id}\" changed while it was being read"),
        json!({"sessionId": id}),
    )
}

fn scan_artifact<F>(artifact: &Artifact, mut visit: F) -> Result<(), RpcError>
where
    F: FnMut(usize, u64, &Event) -> Result<(), RpcError>,
{
    let id = &artifact.header.id;
    let file =
        File::open(&artifact.path).map_err(|error| file_open_error(&artifact.path, error))?;
    let before = file
        .metadata()
        .map_err(|error| file_metadata_error(&artifact.path, error))?;
    if before.len() != artifact.size_bytes || modified_ns(&before) != artifact.modified_ns {
        return Err(changed_error(id));
    }
    let mut reader = BufReader::new(file);
    let line = match read_bounded_line(&mut reader, MAX_HEADER_BYTES)
        .map_err(|error| file_open_error(&artifact.path, error))?
    {
        BoundedLine::Complete(line) => line,
        BoundedLine::TooLarge => {
            return Err(resource_error(format!(
                "session header exceeds the {MAX_HEADER_BYTES}-byte bound"
            )))
        }
        _ => return Err(corrupt_error(id, &artifact.path, "missing header")),
    };
    let header = parse_header(&line[..line.len() - 1])
        .map_err(|error| corrupt_error(id, &artifact.path, error.to_string()))?;
    if header != artifact.header {
        return Err(corrupt_error(
            id,
            &artifact.path,
            "header changed during listing",
        ));
    }
    let mut index = 0usize;
    loop {
        let line_offset = reader
            .stream_position()
            .map_err(|error| file_open_error(&artifact.path, error))?;
        let line = match read_bounded_line(&mut reader, MAX_LINE_BYTES)
            .map_err(|error| file_open_error(&artifact.path, error))?
        {
            BoundedLine::Eof => break,
            BoundedLine::Complete(line) => line,
            BoundedLine::TooLarge => {
                return Err(resource_error(format!(
                    "session event line exceeds the {MAX_LINE_BYTES}-byte bound"
                )))
            }
            BoundedLine::Torn => break,
        };
        let raw: Value = serde_json::from_slice(&line[..line.len() - 1]).map_err(|error| {
            corrupt_error(
                id,
                &artifact.path,
                format!("invalid event at line {}: {error}", index + 2),
            )
        })?;
        validate_raw_event_envelope(&raw, index)
            .map_err(|message| corrupt_error(id, &artifact.path, message))?;
        let event: Event = serde_json::from_value(raw).map_err(|error| {
            corrupt_error(
                id,
                &artifact.path,
                format!("invalid event at line {}: {error}", index + 2),
            )
        })?;
        if event.seq() != Some(index as u64) || event.time().is_none() {
            return Err(corrupt_error(
                id,
                &artifact.path,
                format!(
                    "event at line {} requires contiguous seq and numeric time",
                    index + 2
                ),
            ));
        }
        visit(index, line_offset, &event)?;
        index += 1;
        if index > MAX_EVENTS_PER_SESSION {
            return Err(resource_error(format!(
                "session \"{id}\" exceeds the {MAX_EVENTS_PER_SESSION}-event bound"
            )));
        }
    }
    let after = reader
        .get_ref()
        .metadata()
        .map_err(|error| file_metadata_error(&artifact.path, error))?;
    if before.len() != after.len() || modified_ns(&before) != modified_ns(&after) {
        return Err(changed_error(id));
    }
    Ok(())
}

fn validate_raw_event_envelope(value: &Value, index: usize) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err(format!(
            "event at seq {index} has an invalid event envelope"
        ));
    };
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "type" | "seq" | "time" | "data" | "surfaceOp" | "sourceEventSeqs" | "ignorable"
        )
    }) || !object.contains_key("data")
        || !object.get("type").is_some_and(Value::is_string)
        || object
            .get("ignorable")
            .is_some_and(|value| value != &Value::Bool(true))
    {
        return Err(format!(
            "event at seq {index} has an invalid event envelope"
        ));
    }
    Ok(())
}

fn load_artifact(artifact: Artifact) -> Result<Loaded, RpcError> {
    let mut events = Vec::new();
    scan_artifact(&artifact, |_, _, event| {
        events.push(event.clone());
        Ok(())
    })?;
    Ok(Loaded { artifact, events })
}

fn resolve_artifact<'a>(
    artifacts: &'a [Artifact],
    id: &str,
    cwd: Option<&str>,
) -> Result<&'a Artifact, RpcError> {
    let mut matches = artifacts.iter().filter(|artifact| {
        artifact.header.id == id
            && cwd.is_none_or(|scope| artifact.header.cwd.as_deref() == Some(scope))
    });
    let first = matches.next().ok_or_else(|| not_found(id))?;
    if matches.next().is_some() {
        return Err(query_error(
            "SESSION_QUERY_INVALID_FILTER",
            format!("session id \"{id}\" is ambiguous; supply cwd"),
        ));
    }
    Ok(first)
}

fn load_session(artifacts: &[Artifact], id: &str, cwd: Option<&str>) -> Result<Loaded, RpcError> {
    let artifact = resolve_artifact(artifacts, id, cwd)?.clone();
    load_artifact(artifact)
}

fn optional_cwd(request: &Value) -> Result<Option<String>, RpcError> {
    match request.get("cwd") {
        None => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(Value::String(_)) => Err(bad_request("cwd must not be empty")),
        Some(_) => Err(bad_request("cwd must be text")),
    }
}

fn required_session_id(request: &Value) -> Result<&str, RpcError> {
    request
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| bad_request("missing or invalid sessionId query parameter"))
}

#[derive(Default)]
struct ListMetadata {
    blank: bool,
    last_prompt_at: Option<u64>,
    title: Option<Value>,
    agent_preset: Option<String>,
    last_used: Option<Value>,
    pending: Option<Value>,
    count: usize,
}

impl ListMetadata {
    fn observe(&mut self, event: &Event) {
        self.count += 1;
        match event.kind.as_str() {
            "turn/start" => self.blank = false,
            "user/message" => {
                if event
                    .data
                    .get("source")
                    .and_then(|source| source.get("kind"))
                    .and_then(Value::as_str)
                    == Some("user")
                {
                    self.last_prompt_at = event.time();
                }
            }
            "session/title" => self.title = event.data.get("title").cloned(),
            "agent-preset/selected" => {
                self.agent_preset = event
                    .data
                    .get("agentPreset")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            "model/selection" => {
                let selected = event
                    .data
                    .get("provider")
                    .and_then(Value::as_str)
                    .filter(|provider| !provider.is_empty())
                    .and_then(|provider| {
                        selection_value(
                            provider,
                            event
                                .data
                                .get("model")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            event.data.get("reasoningEffort").and_then(Value::as_str),
                        )
                    });
                if selected.is_some() && selected != self.pending {
                    self.pending = selected;
                }
            }
            "request/header" => {
                let Some(header) = event.data.get("header") else {
                    return;
                };
                let Some(config) = header.get("config") else {
                    return;
                };
                let adapter_defaulted = header
                    .get("adapterDefaults")
                    .and_then(|defaults| defaults.get("reasoningEffort"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let effort = if adapter_defaulted {
                    None
                } else {
                    config.get("reasoningEffort").and_then(Value::as_str)
                };
                let Some(selected) = config
                    .get("provider")
                    .and_then(Value::as_str)
                    .filter(|provider| !provider.is_empty())
                    .and_then(|provider| {
                        selection_value(
                            provider,
                            config
                                .get("model")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            effort,
                        )
                    })
                else {
                    return;
                };
                if self.pending.as_ref() == Some(&selected) {
                    self.pending = None;
                }
                if self.last_used.as_ref() != Some(&selected) {
                    self.last_used = Some(selected);
                }
            }
            _ => {}
        }
    }
}

fn selection_value(provider: &str, model: &str, effort: Option<&str>) -> Option<Value> {
    if provider.is_empty() || model.is_empty() {
        return None;
    }
    let mut selection = json!({"provider": provider, "model": model});
    if let Some(effort) = effort.filter(|value| !value.is_empty()) {
        selection["reasoningEffort"] = json!(effort);
    }
    Some(selection)
}

fn list_row(header: &Header, metadata: ListMetadata) -> Value {
    let last_prompt_at = metadata.last_prompt_at;
    let updated_at = header.created_at.max(last_prompt_at.unwrap_or(0));
    let mut row = json!({
        "sessionId": header.id,
        "updatedAt": updated_at,
        "agentAvailable": false,
        "formatStatus": "current",
        "running": false,
        "blank": metadata.blank,
        "cwd": header.cwd,
        "projections": {
            "kind": "cached",
            "asOfSeq": metadata.count,
            "values": {
                "userQuestions": {"active": [], "settled": []},
                "sessionListMetadata": {"blank": metadata.blank, "lastPromptAt": last_prompt_at},
                "tokenUsage": {"uncachedInputTokens": 0, "outputTokens": 0, "cacheReadTokens": 0, "cacheWriteTokens": 0},
                "title": metadata.title,
                "goal": null,
                "agentPreset": metadata.agent_preset.clone().or_else(|| header.agent_preset.clone()).unwrap_or_else(|| "standard".into()),
                "workingDirectory": header.cwd,
                "modelSelection": {"lastUsed": metadata.last_used, "next": metadata.pending.or(metadata.last_used)},
                "permissions": {"currentValue": "workspace-write"},
                "inbox": {"next-turn": [], "next-step": []},
                "subagentCatalog": [],
                "subagentTiming": {"settledMs": 0},
                "subagent": null,
                "agentTeam": {"enabled": false, "tasks": []},
                "plan": {"active": false, "pending": false},
                "conversationContinuation": null
            }
        }
    });
    if let Some(parent) = &header.parent_session {
        row["parentSessionId"] = json!(parent);
    }
    if let Some(origin) = header.origin {
        row["origin"] = json!(origin);
    }
    row
}

/// Exact `session/list` response shape, with an optional headless `cwd` scope.
pub fn list_sessions(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    if request
        .get("cursor")
        .is_some_and(|value| !value.is_string())
    {
        return Err(bad_request("session list cursor must be text"));
    }
    let cwd = optional_cwd(request)?;
    let root = cx.home.join("sessions");
    let mut budget = Budget::new();
    let artifacts = inventory(&root, cwd.as_deref(), &mut budget)?;
    let mut rows = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        if artifact.header.cwd.is_none() {
            continue;
        }
        let mut metadata = ListMetadata {
            blank: true,
            ..ListMetadata::default()
        };
        scan_artifact(&artifact, |_, _, event| {
            metadata.observe(event);
            Ok(())
        })?;
        rows.push(list_row(&artifact.header, metadata));
    }
    rows.sort_by(|left, right| {
        right
            .get("updatedAt")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .cmp(&left.get("updatedAt").and_then(Value::as_u64).unwrap_or(0))
            .then_with(|| {
                left.get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .cmp(
                        right
                            .get("sessionId")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
            })
    });
    Ok(json!({"items": rows}))
}

fn normalize_host_query(value: Option<&Value>) -> Result<String, RpcError> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Err(bad_request("session search query must be text"));
    };
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(bad_request("session search query must not be empty"));
    }
    if normalized.encode_utf16().count() > HOST_QUERY_MAX_UTF16 {
        return Err(bad_request(format!(
            "session search query must contain at most {HOST_QUERY_MAX_UTF16} UTF-16 code units"
        )));
    }
    if normalized.contains('\0') {
        return Err(bad_request("session search query must not contain NUL"));
    }
    Ok(normalized.to_owned())
}

/// Exact fixed-page `session/search` response: 20 items plus one lookahead.
pub fn search_sessions(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let query = normalize_host_query(request.get("query"))?;
    let cwd = optional_cwd(request)?;
    let session_id = match request.get("sessionId") {
        None => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        Some(_) => return Err(bad_request("sessionId must not be empty")),
    };
    let result = host_search(
        &cx.home.join("sessions"),
        &query,
        cwd.as_deref(),
        session_id.as_deref(),
    );
    match result {
        Ok((items, has_more)) => Ok(json!({"items": items, "hasMore": has_more})),
        Err(error) => Err(rpc_error(
            "gateway/internal",
            format!("session search failed: {}", error.message),
            json!({"cause": {"code": error.code, "message": error.message, "details": error.details}}),
        )),
    }
}

#[derive(Clone)]
struct EventHit {
    session_id: String,
    seq: u64,
    kind: String,
    time: u64,
    surface: &'static str,
    snippet: String,
    match_count: usize,
    document_length: usize,
}

impl EventHit {
    fn wire(&self) -> Value {
        json!({
            "sessionId": self.session_id,
            "seq": self.seq,
            "type": self.kind,
            "time": self.time,
            "surface": self.surface,
            "snippet": self.snippet,
        })
    }

    fn host_wire(&self) -> Value {
        json!({
            "sessionId": self.session_id,
            "snippet": self.snippet,
        })
    }

    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        other
            .match_count
            .cmp(&self.match_count)
            .then_with(|| self.document_length.cmp(&other.document_length))
            .then_with(|| other.time.cmp(&self.time))
            .then_with(|| self.session_id.cmp(&other.session_id))
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

fn host_search(
    root: &Path,
    query: &str,
    cwd: Option<&str>,
    session_id: Option<&str>,
) -> Result<(Vec<Value>, bool), RpcError> {
    let mut budget = Budget::new();
    let artifacts = inventory(root, cwd, &mut budget)?;
    let visible: Vec<Artifact> = artifacts
        .into_iter()
        .filter(|artifact| {
            artifact.header.cwd.is_some() && session_id.is_none_or(|id| artifact.header.id == id)
        })
        .collect();
    if visible.is_empty() {
        return Ok((Vec::new(), false));
    }
    let matcher = Matcher::new(query);
    let mut best: HashMap<String, EventHit> = HashMap::new();
    for artifact in visible {
        let id = artifact.header.id.clone();
        let surface = build_surface_index(&artifact)?;
        let mut session_best: Option<EventHit> = None;
        scan_artifact(&artifact, |index, _, event| {
            if !matches!(event.kind.as_str(), "user/message" | "assistant/message")
                || surface.surfaces[index] != "current"
            {
                return Ok(());
            }
            let Some(text) = semantic_text(event) else {
                return Ok(());
            };
            let Some(found) = matcher.find(&text) else {
                return Ok(());
            };
            let hit = EventHit {
                session_id: id.clone(),
                seq: index as u64,
                kind: event.kind.clone(),
                time: event.time().unwrap_or(0),
                surface: "current",
                snippet: make_snippet(&text, found.start, SNIPPET_CHARS),
                match_count: found.count,
                document_length: text.chars().count(),
            };
            if session_best
                .as_ref()
                .is_none_or(|existing| hit.compare(existing).is_lt())
            {
                session_best = Some(hit);
            }
            Ok(())
        })?;
        if let Some(hit) = session_best {
            best.insert(id, hit);
        }
    }
    let mut hits: Vec<EventHit> = best.into_values().collect();
    hits.sort_by(EventHit::compare);
    let has_more = hits.len() > HOST_SEARCH_LIMIT;
    hits.truncate(HOST_SEARCH_LIMIT);
    Ok((hits.iter().map(EventHit::host_wire).collect(), has_more))
}

#[derive(Clone)]
struct Matcher {
    tokens: Vec<String>,
}

struct Found {
    start: usize,
    count: usize,
}

impl Matcher {
    fn new(query: &str) -> Self {
        Self {
            tokens: tokenize(query)
                .into_iter()
                .map(|token| token.text)
                .collect(),
        }
    }

    fn find(&self, text: &str) -> Option<Found> {
        if self.tokens.is_empty() {
            return None;
        }
        let tokens = tokenize(text);
        if tokens.len() < self.tokens.len() {
            return None;
        }
        let mut count = 0usize;
        let mut first = None;
        let mut index = 0usize;
        while index + self.tokens.len() <= tokens.len() {
            if tokens[index..index + self.tokens.len()]
                .iter()
                .zip(&self.tokens)
                .all(|(candidate, query)| candidate.text == *query)
            {
                count += 1;
                first.get_or_insert(tokens[index].start);
                index += self.tokens.len();
            } else {
                index += 1;
            }
        }
        count.gt(&0).then(|| Found {
            start: first.unwrap_or(0),
            count,
        })
    }
}

struct TextToken {
    text: String,
    start: usize,
}

fn tokenize(text: &str) -> Vec<TextToken> {
    let mut output = Vec::new();
    let mut current = String::new();
    let mut start = 0usize;
    for (offset, character) in text.char_indices() {
        if character.is_alphanumeric() {
            if current.is_empty() {
                start = offset;
            }
            current.extend(character.to_lowercase());
        } else if !current.is_empty() {
            output.push(TextToken {
                text: std::mem::take(&mut current),
                start,
            });
        }
    }
    if !current.is_empty() {
        output.push(TextToken {
            text: current,
            start,
        });
    }
    output
}

fn normalize_whitespace_with_match(text: &str, match_start: usize) -> (String, usize) {
    let mut output = String::new();
    let mut remapped = 0usize;
    let mut found = false;
    for (offset, character) in text.char_indices() {
        if offset >= match_start && !found {
            remapped = output.chars().count();
            found = true;
        }
        if character.is_whitespace() {
            if !output.is_empty() && !output.ends_with(' ') {
                output.push(' ');
            }
        } else {
            output.push(character);
        }
    }
    if !found {
        remapped = output.chars().count();
    }
    if output.ends_with(' ') {
        output.pop();
    }
    (output, remapped)
}

fn make_snippet(text: &str, match_start: usize, maximum: usize) -> String {
    let (normalized, remapped) = normalize_whitespace_with_match(text, match_start);
    let characters: Vec<char> = normalized.chars().collect();
    if characters.len() <= maximum {
        return normalized;
    }
    if maximum == 1 {
        return "…".into();
    }
    let remapped = remapped.min(characters.len().saturating_sub(1));
    let mut start = remapped.saturating_sub(maximum / 3);
    let prefix = start > 0;
    let mut suffix = true;
    let mut content_length = maximum.saturating_sub(usize::from(prefix) + usize::from(suffix));
    if content_length == 0 {
        start = remapped;
        suffix = false;
        content_length = maximum.saturating_sub(usize::from(prefix));
    } else if remapped >= start + content_length {
        start = remapped - content_length + 1;
    }
    let mut end = (start + content_length).min(characters.len());
    if end == characters.len() {
        suffix = false;
        content_length = maximum.saturating_sub(usize::from(prefix));
        start = end.saturating_sub(content_length);
        end = (start + content_length).min(characters.len());
    }
    format!(
        "{}{}{}",
        if prefix { "…" } else { "" },
        characters[start..end].iter().collect::<String>(),
        if suffix { "…" } else { "" },
    )
}

fn normalize_whitespace(text: &str) -> String {
    let mut output = String::new();
    for character in text.chars() {
        if character.is_whitespace() {
            if !output.is_empty() && !output.ends_with(' ') {
                output.push(' ');
            }
        } else {
            output.push(character);
        }
    }
    if output.ends_with(' ') {
        output.pop();
    }
    output
}

fn semantic_text(event: &Event) -> Option<String> {
    let parts: Vec<String> = match event.kind.as_str() {
        "user/message" => content_text(event.data.get("content")?),
        "assistant/message" => content_text(event.data.get("message")?.get("content")?),
        "tool/call" => vec_strings([
            event.data.get("name").and_then(Value::as_str),
            event.data.get("arguments").and_then(Value::as_str),
        ]),
        "tool/result" => {
            let mut parts = content_text(event.data.get("message")?.get("content")?);
            if let Some(error) = event.data.get("error") {
                if let Some(name) = error.get("name").and_then(Value::as_str) {
                    parts.push(name.to_owned());
                }
                if let Some(code) = error.get("code").and_then(Value::as_str) {
                    parts.push(code.to_owned());
                }
            }
            parts
        }
        "todo/write" => event
            .data
            .get("todos")
            .and_then(Value::as_array)
            .map(|todos| {
                todos
                    .iter()
                    .flat_map(|todo| {
                        [
                            todo.get("status").and_then(Value::as_str),
                            todo.get("content").and_then(Value::as_str),
                        ]
                    })
                    .flatten()
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        "turn/end" => turn_end_text(&event.data),
        _ => Vec::new(),
    };
    let joined = join_text(parts);
    (!joined.is_empty()).then_some(joined)
}

fn vec_strings<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Vec<String> {
    values.into_iter().flatten().map(str::to_owned).collect()
}

fn join_text(parts: Vec<String>) -> String {
    parts
        .into_iter()
        .map(|part| part.trim().to_owned())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn content_text(content: &Value) -> Vec<String> {
    content
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .flat_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => vec_strings([block.get("text").and_then(Value::as_str)]),
                    Some("tool-call") => vec_strings([
                        block.get("name").and_then(Value::as_str),
                        block.get("arguments").and_then(Value::as_str),
                    ]),
                    _ => Vec::new(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn turn_end_text(data: &Value) -> Vec<String> {
    match data
        .get("reason")
        .and_then(|reason| reason.get("kind"))
        .and_then(Value::as_str)
    {
        Some("error") => vec_strings([
            Some("error"),
            data.get("reason")
                .and_then(|reason| reason.get("error"))
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str),
        ]),
        Some("aborted") => vec!["aborted".into()],
        Some("max-tokens") => vec!["max-tokens".into()],
        Some("interrupted") => vec!["interrupted".into()],
        _ => Vec::new(),
    }
}

const SURFACE_TYPES: [&str; 5] = [
    "system/message",
    "developer/message",
    "user/message",
    "assistant/message",
    "tool/result",
];

fn is_known_event(kind: &str) -> bool {
    matches!(
        kind,
        "action-cards/decision"
            | "agent-preset/selected"
            | "agent/inbox/spliced"
            | "approval/asked"
            | "approval/decided"
            | "approval/policy"
            | "assistant/attempt"
            | "assistant/message"
            | "command/done"
            | "command/run"
            | "compaction/end"
            | "compaction/prune"
            | "compaction/start"
            | "compaction/summary"
            | "deliverables/presented"
            | "developer/message"
            | "feedback/message-delete"
            | "feedback/message-put"
            | "feedback/record"
            | "goal/change"
            | "hook/invoked"
            | "hook/result"
            | "image/offload"
            | "job/registered"
            | "job/settled"
            | "llm/retry"
            | "llm/retry-started"
            | "model/selection"
            | "permission/preset"
            | "plan/mode"
            | "request/context"
            | "request/header"
            | "sandbox/mode"
            | "schedule/change"
            | "session-log-deepseek/delivery-accepted"
            | "session/end-seed"
            | "session/title"
            | "session/title-llm-request"
            | "step/end"
            | "step/start"
            | "subagent/catalog"
            | "subagent/descriptor"
            | "subagent/model-selection-policy"
            | "system/message"
            | "team/member"
            | "team/message/delivered"
            | "team/message/queued"
            | "team/mode"
            | "team/task"
            | "todo/write"
            | "tool-workflow/agent-end"
            | "tool-workflow/agent-start"
            | "tool-workflow/run-end"
            | "tool-workflow/run-start"
            | "tool/call"
            | "tool/ptc-dispatch"
            | "tool/ptc-dispatch-start"
            | "tool/result"
            | "turn/end"
            | "turn/start"
            | "user/message"
            | "web/deepseek-search-llm-request"
            | "working-directory/change"
            | "workspace-root/change"
            | "workspace/changes"
    )
}

fn validate_surface_event_data(event: &Event, index: usize) -> Result<(), String> {
    let data = &event.data;
    if SURFACE_TYPES.contains(&event.kind.as_str()) && data.is_object() {
        let message = if event.kind == "user/message" {
            Some(data)
        } else {
            data.get("message")
        };
        if let Some(message) = message.and_then(Value::as_object) {
            let developer = message.get("role").and_then(Value::as_str) == Some("developer");
            if (event.kind == "developer/message") != developer {
                return Err(format!(
                    "developer/message at seq {index} and developer role must occur together"
                ));
            }
            if let Some(content) = message.get("content").and_then(Value::as_array) {
                let mut additions = false;
                for block in content {
                    let Some(block) = block.as_object() else {
                        continue;
                    };
                    match block.get("type").and_then(Value::as_str) {
                        Some("tool-addition" | "tool-removal") if !developer => {
                            return Err(format!(
                                "tool-change blocks at seq {index} require developer role"
                            ));
                        }
                        Some("tool-addition") => {
                            additions = true;
                            if block
                                .get("toolName")
                                .and_then(Value::as_str)
                                .is_none_or(str::is_empty)
                            {
                                return Err(format!(
                                    "tool-addition at seq {index} requires a nonempty toolName"
                                ));
                            }
                            if block.contains_key("tool") {
                                return Err(format!("tool-addition at seq {index} must omit inline tool definitions"));
                            }
                        }
                        Some("tool-removal")
                            if block
                                .get("toolName")
                                .and_then(Value::as_str)
                                .is_none_or(str::is_empty) =>
                        {
                            return Err(format!(
                                "tool-removal at seq {index} requires a nonempty toolName"
                            ));
                        }
                        _ => {}
                    }
                }
                if event.kind == "developer/message"
                    && additions
                        != event
                            .data
                            .as_object()
                            .is_some_and(|object| object.contains_key("headerSeq"))
                {
                    return Err(format!("developer/message at seq {index} requires headerSeq exactly when tool additions are present"));
                }
            }
        }
    }
    if event.kind == "request/header" {
        if !data.is_object() {
            return Err(format!(
                "request/header at seq {index} data must be an object"
            ));
        }
        let Some(header) = data.get("header").and_then(Value::as_object) else {
            return Err(format!(
                "request/header at seq {index} header must be an object"
            ));
        };
        if header.contains_key("system") {
            return Err(format!(
                "request/header at seq {index} must omit header.system; use system/message"
            ));
        }
        if header
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return Err(format!(
                "request/header at seq {index} must omit empty tools"
            ));
        }
        if header
            .get("adapterDefaults")
            .and_then(Value::as_object)
            .is_some_and(Map::is_empty)
        {
            return Err(format!(
                "request/header at seq {index} must omit empty adapterDefaults"
            ));
        }
    } else if event.kind == "tool/result" {
        if !data.is_object() {
            return Err(format!("tool/result at seq {index} data must be an object"));
        }
        if event.data.get("error").is_some()
            && data
                .get("message")
                .and_then(|message| message.get("isError"))
                .and_then(Value::as_bool)
                != Some(true)
        {
            return Err(format!(
                "tool/result at seq {index} error requires message.isError === true"
            ));
        }
    }
    Ok(())
}

struct SurfaceIndex {
    artifact: Artifact,
    offsets: Vec<u64>,
    surfaces: Vec<&'static str>,
    nodes: Vec<usize>,
    offloaded: HashMap<usize, Vec<usize>>,
}

impl SurfaceIndex {
    fn read_at(&self, seq: usize) -> Result<Event, String> {
        let mut file = File::open(&self.artifact.path).map_err(|error| error.to_string())?;
        file.seek(SeekFrom::Start(
            *self
                .offsets
                .get(seq)
                .ok_or_else(|| format!("missing event {seq}"))?,
        ))
        .map_err(|error| error.to_string())?;
        let mut reader = BufReader::new(file);
        let BoundedLine::Complete(line) =
            read_bounded_line(&mut reader, MAX_LINE_BYTES).map_err(|error| error.to_string())?
        else {
            return Err(format!("cannot reread event {seq}"));
        };
        serde_json::from_slice(&line[..line.len() - 1]).map_err(|error| error.to_string())
    }

    fn observe(&mut self, index: usize, offset: u64, event: &Event) -> Result<(), String> {
        self.offsets.push(offset);
        self.surfaces.push("log-only");
        validate_surface_event_data(event, index)?;
        if !SURFACE_TYPES.contains(&event.kind.as_str()) {
            let ignorable = !is_known_event(&event.kind)
                && event.extra.get("ignorable").and_then(Value::as_bool) == Some(true);
            if !ignorable
                && (event.extra.contains_key("surfaceOp")
                    || event.extra.contains_key("sourceEventSeqs"))
            {
                return Err(format!("session event \"{}\" at seq {index} is not surface-eligible and cannot carry surface metadata", event.kind));
            }
            if event.kind == "image/offload" {
                self.apply_offload(index, event)?;
            }
            return Ok(());
        }
        let sources = source_event_seqs(event, index)?;
        if event.kind == "assistant/message" && event.extra.contains_key("sourceEventSeqs") {
            return Err("assistant/message cannot carry sourceEventSeqs".into());
        }
        let Some(operation) = event.extra.get("surfaceOp") else {
            return Err(format!(
                "session event \"{}\" at seq {index} requires a surfaceOp marker",
                event.kind
            ));
        };
        if event.kind == "developer/message" {
            self.validate_developer_header(index, event)?;
        }
        if operation == "append" {
            self.nodes.push(index);
            self.surfaces[index] = "current";
            return Ok(());
        }
        let Some(object) = operation.as_object() else {
            return Err(format!("invalid replace surfaceOp at seq {index}"));
        };
        if object.len() != 3
            || object.get("op").and_then(Value::as_str) != Some("replace")
            || object.get("startSeq").and_then(Value::as_u64).is_none()
            || object.get("endSeq").and_then(Value::as_u64).is_none()
        {
            return Err(format!("invalid replace surfaceOp at seq {index}"));
        }
        let start = object["startSeq"].as_u64().unwrap() as usize;
        let end = object["endSeq"].as_u64().unwrap() as usize;
        if start >= index || end >= index {
            return Err(format!(
                "surface replace at seq {index} must reference earlier events"
            ));
        }
        let start_index = self
            .nodes
            .iter()
            .position(|seq| *seq == start)
            .ok_or_else(|| format!("surface replace: start seq {start} not found in surface"))?;
        let end_index = self
            .nodes
            .iter()
            .position(|seq| *seq == end)
            .ok_or_else(|| format!("surface replace: end seq {end} not found in surface"))?;
        if start_index > end_index {
            return Err(format!(
                "surface replace start seq {start} is after end seq {end}"
            ));
        }
        let shadowed: Vec<usize> = self.nodes[start_index..=end_index].to_vec();
        if shadowed.iter().any(|seq| !sources.contains(seq)) {
            return Err("surface replace sourceEventSeqs must include every shadowed node".into());
        }
        if event.kind == "tool/result" {
            if shadowed.len() != 1 || self.read_at(shadowed[0])?.kind != "tool/result" {
                return Err(
                    "tool/result surface replacement must rewrite one current tool/result".into(),
                );
            }
            if without_tool_result_content(&self.read_at(shadowed[0])?)
                != without_tool_result_content(event)
            {
                return Err("tool/result surface replacement may change only content".into());
            }
        }
        if start_index == 0
            && self.read_at(self.nodes[0])?.kind == "system/message"
            && (event.kind != "system/message" || shadowed.len() != 1)
        {
            return Err("surface replace: node 0 holds the system prompt and may be rewritten only by a system/message over exactly that one node".into());
        }
        for seq in shadowed {
            self.surfaces[seq] = "shadowed";
            self.offloaded.remove(&seq);
        }
        self.nodes.splice(start_index..=end_index, [index]);
        self.surfaces[index] = "current";
        Ok(())
    }

    fn validate_developer_header(&self, index: usize, event: &Event) -> Result<(), String> {
        let Some(header_seq) = event.data.get("headerSeq").and_then(Value::as_u64) else {
            return Ok(());
        };
        let header_seq = header_seq as usize;
        if header_seq >= index || self.read_at(header_seq)?.kind != "request/header" {
            return Err(
                "developer/message headerSeq must reference an earlier request/header".into(),
            );
        }
        let header_event = self.read_at(header_seq)?;
        let Some(content) = event
            .data
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
        else {
            return Ok(());
        };
        for block in content {
            if block.get("type").and_then(Value::as_str) != Some("tool-addition") {
                continue;
            }
            let name = block
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let definitions: Vec<&Value> = header_event
                .data
                .get("header")
                .and_then(|header| header.get("tools"))
                .and_then(Value::as_array)
                .map(|tools| {
                    tools
                        .iter()
                        .filter(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
                        .collect()
                })
                .unwrap_or_default();
            if definitions.len() != 1 {
                return Err(format!("developer/message tool-addition \"{name}\" must name exactly one tool in headerSeq {header_seq}"));
            }
            let definition = definitions[0];
            if !definition.get("description").is_some_and(Value::is_string)
                || !definition.get("parameters").is_some_and(Value::is_object)
                || definition
                    .get("deferLoading")
                    .is_some_and(|value| value != &Value::Bool(true))
            {
                return Err(format!("developer/message tool-addition \"{name}\" requires a complete tool definition in headerSeq {header_seq}"));
            }
        }
        Ok(())
    }

    fn apply_offload(&mut self, index: usize, event: &Event) -> Result<(), String> {
        if event
            .data
            .as_object()
            .is_none_or(|object| object.len() != 1)
            || event
                .data
                .get("targets")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty)
        {
            return Err("image/offload: data must contain a nonempty targets array".into());
        }
        let mut seen = HashSet::new();
        for target in event.data["targets"].as_array().unwrap() {
            if target.as_object().is_none_or(|object| object.len() != 2)
                || target.get("seq").and_then(Value::as_u64).is_none()
                || target
                    .get("imageIndexes")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
            {
                return Err(
                    "image/offload: each target must contain a seq and nonempty imageIndexes"
                        .into(),
                );
            }
            let seq = target["seq"].as_u64().unwrap() as usize;
            if !seen.insert(seq) {
                return Err(format!("image/offload: duplicate target seq {seq}"));
            }
            if !self.nodes.contains(&seq) {
                return Err(format!(
                    "image/offload: target seq {seq} is not a current surface node"
                ));
            }
            let source = self.read_at(seq)?;
            if source.kind != "user/message" && source.kind != "tool/result" {
                return Err(format!(
                    "image/offload: target seq {seq} must be user/message or tool/result"
                ));
            }
            let content = if source.kind == "user/message" {
                source.data.get("content")
            } else {
                source
                    .data
                    .get("message")
                    .and_then(|message| message.get("content"))
            };
            let images = content
                .and_then(Value::as_array)
                .map(|blocks| {
                    blocks
                        .iter()
                        .filter(|block| block.get("type").and_then(Value::as_str) == Some("image"))
                        .count()
                })
                .unwrap_or(0);
            let omitted = self.offloaded.entry(seq).or_default();
            let mut previous = None;
            for value in target["imageIndexes"].as_array().unwrap() {
                let Some(image) = value.as_u64() else {
                    return Err("image/offload: imageIndexes must be strictly increasing non-negative safe integers".into());
                };
                if previous.is_some_and(|previous| image <= previous) {
                    return Err("image/offload: imageIndexes must be strictly increasing non-negative safe integers".into());
                }
                previous = Some(image);
                let image = image as usize;
                if image >= images || omitted.contains(&image) {
                    return Err(format!("image/offload at seq {index}: image index {image} is missing or already offloaded"));
                }
                omitted.push(image);
            }
        }
        Ok(())
    }
}

fn without_tool_result_content(event: &Event) -> Value {
    let mut data = event.data.clone();
    if let Some(message) = data.get_mut("message").and_then(Value::as_object_mut) {
        message.insert("content".into(), Value::Null);
    }
    data
}

fn build_surface_index(artifact: &Artifact) -> Result<SurfaceIndex, RpcError> {
    let mut index = SurfaceIndex {
        artifact: artifact.clone(),
        offsets: Vec::new(),
        surfaces: Vec::new(),
        nodes: Vec::new(),
        offloaded: HashMap::new(),
    };
    scan_artifact(artifact, |seq, offset, event| {
        index
            .observe(seq, offset, event)
            .map_err(|message| query_error("SESSION_QUERY_INVALID_SURFACE", message))
    })?;
    Ok(index)
}

fn source_event_seqs(event: &Event, index: usize) -> Result<HashSet<usize>, String> {
    let Some(value) = event.extra.get("sourceEventSeqs") else {
        return Ok(HashSet::new());
    };
    let Some(values) = value.as_array() else {
        return Err(format!("sourceEventSeqs at seq {index} must be an array"));
    };
    if values.is_empty() {
        return Err("sourceEventSeqs must not be empty".into());
    }
    let mut output = HashSet::new();
    for value in values {
        let Some(seq) = value.as_u64().filter(|seq| *seq <= 9_007_199_254_740_991) else {
            return Err(format!(
                "sourceEventSeqs at seq {index} must contain non-negative integers"
            ));
        };
        let seq = seq as usize;
        if seq >= index || !output.insert(seq) {
            return Err(format!(
                "sourceEventSeqs at seq {index} must be unique earlier events"
            ));
        }
    }
    Ok(output)
}

#[derive(Clone)]
enum SessionFilter {
    Id(Vec<String>),
    Cwd(Vec<Option<String>>),
    CreatedAt { from: Option<f64>, to: Option<f64> },
    Parent(Vec<Option<String>>),
    Availability(Vec<String>),
}

#[derive(Clone)]
enum EventFilter {
    Seq { from: Option<f64>, to: Option<f64> },
    Time { from: Option<f64>, to: Option<f64> },
    Type(Vec<String>),
    Surface(Vec<String>),
    Text(String),
}

fn invalid_filter(detail: impl Into<String>) -> RpcError {
    query_error(
        "SESSION_QUERY_INVALID_FILTER",
        format!("session {}", detail.into()),
    )
}

fn unknown_filter_kind(filter: &Value) -> RpcError {
    let kind = filter.get("kind").and_then(Value::as_str);
    invalid_filter(format!(
        "unknown filter kind {}",
        kind.map(|value| format!("\"{value}\""))
            .unwrap_or_else(|| "(missing)".into())
    ))
}

fn filter_array<'a>(request: &'a Value, key: &str) -> Result<Vec<&'a Value>, RpcError> {
    match request.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => Ok(values.iter().collect()),
        Some(_) => Err(invalid_filter("filters must be an array")),
    }
}

fn string_values(
    filter: &Value,
    name: &str,
    nullable: bool,
) -> Result<Vec<Option<String>>, RpcError> {
    let Some(values) = filter.get("values").and_then(Value::as_array) else {
        return Err(invalid_filter(format!(
            "{name} filter values must be an array"
        )));
    };
    if values.len() > MAX_FILTER_VALUES {
        return Err(query_error("SESSION_QUERY_INVALID_FILTER", format!("session-search request exceeds SQLite's portable {MAX_FILTER_VALUES}-variable limit; reduce filter values")));
    }
    let mut output = Vec::new();
    for value in values {
        if let Some(value) = value.as_str() {
            output.push(Some(value.to_owned()));
        } else if nullable && value.is_null() {
            output.push(None);
        } else {
            return Err(invalid_filter(if nullable {
                format!("{name} filter values must be an array of strings or null")
            } else {
                format!("{name} filter values must be an array of strings")
            }));
        }
    }
    Ok(output)
}

fn range_filter(filter: &Value, name: &str) -> Result<(Option<f64>, Option<f64>), RpcError> {
    let bound = |key| -> Result<Option<f64>, RpcError> {
        match filter.get(key) {
            None => Ok(None),
            Some(value) => value
                .as_f64()
                .filter(|number| number.is_finite())
                .map(Some)
                .ok_or_else(|| invalid_filter(format!("{name} filter {key} must be finite"))),
        }
    };
    let from = bound("from")?;
    let to = bound("to")?;
    if from.is_some_and(|from| to.is_some_and(|to| from > to)) {
        return Err(invalid_filter(format!(
            "{name} filter from must be less than or equal to to"
        )));
    }
    Ok((from, to))
}

fn parse_session_filters(request: &Value) -> Result<Vec<SessionFilter>, RpcError> {
    let filters: Vec<SessionFilter> = filter_array(request, "sessionFilters")?
        .into_iter()
        .map(|filter| match filter.get("kind").and_then(Value::as_str) {
            Some("id") => Ok(SessionFilter::Id(
                string_values(filter, "id", false)?
                    .into_iter()
                    .map(Option::unwrap)
                    .collect(),
            )),
            Some("cwd") => Ok(SessionFilter::Cwd(string_values(filter, "cwd", true)?)),
            Some("created-at") => {
                let (from, to) = range_filter(filter, "created-at")?;
                Ok(SessionFilter::CreatedAt { from, to })
            }
            Some("parent") => Ok(SessionFilter::Parent(string_values(
                filter, "parent", true,
            )?)),
            Some("availability") => {
                let values: Vec<String> = string_values(filter, "availability", false)?
                    .into_iter()
                    .map(Option::unwrap)
                    .collect();
                if let Some(value) = values
                    .iter()
                    .find(|value| value.as_str() != "live" && value.as_str() != "persisted")
                {
                    return Err(invalid_filter(format!(
                        "availability filter contains unknown value \"{value}\""
                    )));
                }
                Ok(SessionFilter::Availability(values))
            }
            _ => Err(unknown_filter_kind(filter)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_filter_budget(&filters, &[], false)?;
    Ok(filters)
}

fn parse_event_filters(
    request: &Value,
    key: &str,
    allow_text: bool,
) -> Result<Vec<EventFilter>, RpcError> {
    let filters: Vec<EventFilter> = filter_array(request, key)?
        .into_iter()
        .map(|filter| {
            let kind = filter.get("kind").and_then(Value::as_str);
            match kind {
                Some("seq") => {
                    let (from, to) = range_filter(filter, "seq")?;
                    Ok(EventFilter::Seq { from, to })
                }
                Some("time") => {
                    let (from, to) = range_filter(filter, "time")?;
                    Ok(EventFilter::Time { from, to })
                }
                Some("type") => Ok(EventFilter::Type(
                    string_values(filter, "type", false)?
                        .into_iter()
                        .map(Option::unwrap)
                        .collect(),
                )),
                Some("surface") => {
                    let values: Vec<String> = string_values(filter, "surface", false)?
                        .into_iter()
                        .map(Option::unwrap)
                        .collect();
                    if let Some(value) = values.iter().find(|value| {
                        !matches!(value.as_str(), "current" | "shadowed" | "log-only")
                    }) {
                        return Err(invalid_filter(format!(
                            "surface filter contains unknown value \"{value}\""
                        )));
                    }
                    Ok(EventFilter::Surface(values))
                }
                Some("text") if allow_text => filter
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.trim().is_empty())
                    .map(|text| EventFilter::Text(text.to_owned()))
                    .ok_or_else(|| invalid_filter("text filter text must be a string")),
                Some("text") => Err(query_error(
                    "SESSION_QUERY_INVALID_FILTER",
                    "session-search metadata filters do not accept text clauses",
                )),
                _ => Err(unknown_filter_kind(filter)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(filters)
}

fn session_predicate_count(filter: &SessionFilter) -> usize {
    match filter {
        SessionFilter::Id(_) | SessionFilter::Cwd(_) | SessionFilter::Parent(_) => 1,
        SessionFilter::CreatedAt { from, to } => {
            usize::from(from.is_some()) + usize::from(to.is_some())
        }
        SessionFilter::Availability(values) => {
            usize::from(!(values.len() == 2 || values.is_empty()))
        }
    }
}

fn event_predicate_count(filter: &EventFilter) -> usize {
    match filter {
        EventFilter::Seq { from, to } | EventFilter::Time { from, to } => {
            usize::from(from.is_some()) + usize::from(to.is_some())
        }
        EventFilter::Type(_) | EventFilter::Surface(_) | EventFilter::Text(_) => 1,
    }
}

fn validate_filter_budget(
    session: &[SessionFilter],
    event: &[EventFilter],
    within_session: bool,
) -> Result<(), RpcError> {
    let predicates = session.iter().map(session_predicate_count).sum::<usize>()
        + event.iter().map(event_predicate_count).sum::<usize>()
        + usize::from(within_session);
    if predicates > 14 {
        return Err(query_error("SESSION_QUERY_INVALID_FILTER", "session-search request exceeds the supported SQLite FTS5 outer-predicate budget of 14; reduce filters"));
    }
    let bindings = session
        .iter()
        .map(|filter| match filter {
            SessionFilter::Id(values) => values.len(),
            SessionFilter::Cwd(values) | SessionFilter::Parent(values) => values.len(),
            SessionFilter::CreatedAt { from, to } => {
                usize::from(from.is_some()) + usize::from(to.is_some())
            }
            SessionFilter::Availability(_) => 0,
        })
        .sum::<usize>()
        + event
            .iter()
            .map(|filter| match filter {
                EventFilter::Seq { from, to } | EventFilter::Time { from, to } => {
                    usize::from(from.is_some()) + usize::from(to.is_some())
                }
                EventFilter::Type(values) | EventFilter::Surface(values) => values.len(),
                EventFilter::Text(_) => 1,
            })
            .sum::<usize>()
        + usize::from(within_session);
    if bindings > MAX_FILTER_VALUES {
        return Err(query_error("SESSION_QUERY_INVALID_FILTER", format!("session-search request exceeds SQLite's portable {MAX_FILTER_VALUES}-variable limit; reduce filter values")));
    }
    Ok(())
}

fn in_range(value: u64, from: Option<f64>, to: Option<f64>) -> bool {
    let value = value as f64;
    from.is_none_or(|from| value >= from) && to.is_none_or(|to| value <= to)
}

fn session_matches(header: &Header, filters: &[SessionFilter]) -> bool {
    filters.iter().all(|filter| match filter {
        SessionFilter::Id(values) => values.contains(&header.id),
        SessionFilter::Cwd(values) => values.contains(&header.cwd),
        SessionFilter::CreatedAt { from, to } => in_range(header.created_at, *from, *to),
        SessionFilter::Parent(values) => values.contains(&header.parent_session),
        SessionFilter::Availability(values) => values.iter().any(|value| value == "persisted"),
    })
}

fn event_matches(event: &Event, surface: &str, filters: &[EventFilter]) -> bool {
    filters.iter().all(|filter| match filter {
        EventFilter::Seq { from, to } => in_range(event.seq().unwrap_or(0), *from, *to),
        EventFilter::Time { from, to } => in_range(event.time().unwrap_or(0), *from, *to),
        EventFilter::Type(values) => values.contains(&event.kind),
        EventFilter::Surface(values) => values.iter().any(|value| value == surface),
        EventFilter::Text(text) => Matcher::new(text)
            .find(&semantic_text(event).unwrap_or_default())
            .is_some(),
    })
}

fn normalize_query(value: Option<&Value>) -> Result<String, RpcError> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Err(query_error(
            "SESSION_QUERY_INVALID_QUERY",
            "session-search query must be text",
        ));
    };
    let query = normalize_whitespace(value);
    if query.is_empty() {
        return Err(query_error(
            "SESSION_QUERY_INVALID_QUERY",
            "session-search query must contain non-whitespace text",
        ));
    }
    if query.contains('\0') {
        return Err(query_error(
            "SESSION_QUERY_INVALID_QUERY",
            "session-search query must not contain NUL",
        ));
    }
    Ok(query.replace(['\u{FDD0}', '\u{FDD1}'], "\u{FFFD}"))
}

fn normalize_limit(request: &Value) -> Result<usize, RpcError> {
    match request.get("limit") {
        None | Some(Value::Null) => Ok(DEFAULT_LIMIT),
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| (1..=MAX_LIMIT).contains(value))
            .ok_or_else(|| {
                query_error(
                    "SESSION_QUERY_INVALID_LIMIT",
                    format!("session-search limit must be an integer between 1 and {MAX_LIMIT}"),
                )
            }),
    }
}

fn cursor_text(request: &Value) -> Result<Option<&str>, RpcError> {
    match request.get("cursor") {
        None => Ok(None),
        Some(Value::String(value)) if value.len() <= MAX_CURSOR_BYTES => Ok(Some(value)),
        Some(Value::String(_)) => Err(query_error(
            "SESSION_QUERY_INVALID_CURSOR",
            "session-search cursor is invalid",
        )),
        Some(_) => Err(query_error(
            "SESSION_QUERY_INVALID_CURSOR",
            "session-search cursor must be text",
        )),
    }
}

fn stable_hash(text: &str) -> String {
    let mut first = 0xcbf29ce484222325u64;
    let mut second = 0x9e3779b97f4a7c15u64;
    for byte in text.as_bytes() {
        first ^= u64::from(*byte);
        first = first.wrapping_mul(0x100000001b3);
        second ^= u64::from(*byte);
        second = second.rotate_left(5).wrapping_mul(0xbf58476d1ce4e5b9);
    }
    format!("{first:016x}{second:016x}")
}

fn logical_header(header: &Header) -> Value {
    let mut value = serde_json::to_value(header).expect("header serializes");
    value.as_object_mut().expect("header object").remove("type");
    value
}

fn corpus_generation(artifacts: &[Artifact]) -> String {
    let mut identity = String::new();
    for artifact in artifacts {
        identity.push_str(&artifact.header.id);
        identity.push('\0');
        identity.push_str(&artifact.path.display().to_string());
        identity.push('\0');
        identity.push_str(&artifact.size_bytes.to_string());
        identity.push('\0');
        identity.push_str(&artifact.modified_ns.to_string());
        identity.push('\n');
    }
    stable_hash(&identity)
}

fn filter_identity(value: &Value) -> String {
    serde_json::to_string(value).expect("filter serializes")
}

fn canonical_filter_identity(mut value: Value) -> String {
    if let Some(values) = value.get_mut("values").and_then(Value::as_array_mut) {
        values.sort_by(|left, right| match (left.as_str(), right.as_str()) {
            (Some(left), Some(right)) => left.cmp(right),
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
    }
    filter_identity(&value)
}

fn canonical_session_filters(filters: &[SessionFilter]) -> String {
    let mut values: Vec<String> = filters
        .iter()
        .map(|filter| match filter {
            SessionFilter::Id(values) => {
                canonical_filter_identity(json!({"kind": "id", "values": values}))
            }
            SessionFilter::Cwd(values) => {
                canonical_filter_identity(json!({"kind": "cwd", "values": values}))
            }
            SessionFilter::CreatedAt { from, to } => {
                canonical_filter_identity(json!({"kind": "created-at", "from": from, "to": to}))
            }
            SessionFilter::Parent(values) => {
                canonical_filter_identity(json!({"kind": "parent", "values": values}))
            }
            SessionFilter::Availability(values) => {
                canonical_filter_identity(json!({"kind": "availability", "values": values}))
            }
        })
        .collect();
    values.sort();
    values.join("|")
}

fn canonical_event_filters(filters: &[EventFilter]) -> String {
    let mut values: Vec<String> = filters
        .iter()
        .map(|filter| match filter {
            EventFilter::Seq { from, to } => {
                canonical_filter_identity(json!({"kind": "seq", "from": from, "to": to}))
            }
            EventFilter::Time { from, to } => {
                canonical_filter_identity(json!({"kind": "time", "from": from, "to": to}))
            }
            EventFilter::Type(values) => {
                canonical_filter_identity(json!({"kind": "type", "values": values}))
            }
            EventFilter::Surface(values) => {
                canonical_filter_identity(json!({"kind": "surface", "values": values}))
            }
            EventFilter::Text(text) => {
                canonical_filter_identity(json!({"kind": "text", "text": text}))
            }
        })
        .collect();
    values.sort();
    values.join("|")
}

fn request_fingerprint(
    scope: &str,
    query: &str,
    session_id: Option<&str>,
    session_filters: &str,
    event_filters: &str,
    limit: usize,
) -> String {
    stable_hash(&filter_identity(&json!({
        "scope": scope,
        "sessionId": session_id,
        "query": query,
        "sessionFilters": session_filters,
        "eventFilters": event_filters,
        "limit": limit,
    })))
}

#[derive(Clone, Copy)]
struct Cursor {
    offset: usize,
}

fn encode_cursor(
    scope: &str,
    fingerprint: &str,
    generation: &str,
    offset: usize,
) -> Result<String, RpcError> {
    let payload = json!({
        "version": 1,
        "instance": CURSOR_INSTANCE,
        "scope": scope,
        "fingerprint": fingerprint,
        "generation": generation,
        "offset": offset,
    });
    let encoded = base64url_encode(&serde_json::to_vec(&payload).expect("cursor serializes"))?;
    Ok(format!("dshsq1.{encoded}"))
}

fn decode_cursor(
    value: &str,
    scope: &str,
    fingerprint: &str,
    generation: &str,
) -> Result<Cursor, RpcError> {
    let invalid = || {
        query_error(
            "SESSION_QUERY_INVALID_CURSOR",
            "session-search cursor is invalid",
        )
    };
    if value.len() > MAX_CURSOR_BYTES {
        return Err(invalid());
    }
    let encoded = value.strip_prefix("dshsq1.").ok_or_else(invalid)?;
    let bytes = base64url_decode(encoded).map_err(|_| invalid())?;
    let payload: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if payload.get("version").and_then(Value::as_u64) != Some(1)
        || payload.get("instance").and_then(Value::as_str) != Some(CURSOR_INSTANCE)
        || payload.get("scope").and_then(Value::as_str) != Some(scope)
        || payload.get("fingerprint").and_then(Value::as_str) != Some(fingerprint)
    {
        return Err(invalid());
    }
    if payload.get("generation").and_then(Value::as_str) != Some(generation) {
        return Err(query_error(
            "SESSION_QUERY_STALE_CURSOR",
            "session-search cursor is stale",
        ));
    }
    let offset = payload
        .get("offset")
        .and_then(Value::as_u64)
        .and_then(|offset| usize::try_from(offset).ok())
        .ok_or_else(invalid)?;
    Ok(Cursor { offset })
}

fn page_items(
    items: &[Value],
    cursor: Cursor,
    limit: usize,
    scope: &str,
    fingerprint: &str,
    generation: &str,
) -> Result<Value, RpcError> {
    let start = cursor.offset.min(items.len());
    let end = start.saturating_add(limit).min(items.len());
    let mut page = json!({"items": &items[start..end]});
    if end < items.len() {
        page["nextCursor"] = json!(encode_cursor(scope, fingerprint, generation, end)?);
    }
    Ok(page)
}

/// Cross-Session query-service search contract over the bounded memory index.
pub fn query_sessions(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let query = normalize_query(request.get("query"))?;
    let limit = normalize_limit(request)?;
    let cursor = cursor_text(request)?;
    let session_filters = parse_session_filters(request)?;
    let event_filters = parse_event_filters(request, "eventFilters", false)?;
    validate_filter_budget(&session_filters, &event_filters, false)?;
    let root = cx.home.join("sessions");
    let mut budget = Budget::new();
    let artifacts = inventory(&root, None, &mut budget)?;
    let generation = corpus_generation(&artifacts);
    let session_identity = canonical_session_filters(&session_filters);
    let event_identity = canonical_event_filters(&event_filters);
    let fingerprint = request_fingerprint(
        "sessions",
        &query,
        None,
        &session_identity,
        &event_identity,
        limit,
    );
    let cursor = cursor
        .map(|value| decode_cursor(value, "sessions", &fingerprint, &generation))
        .transpose()?
        .unwrap_or(Cursor { offset: 0 });
    let matcher = Matcher::new(&query);
    let mut hits: Vec<(Header, EventHit)> = Vec::new();
    for artifact in artifacts {
        if !session_matches(&artifact.header, &session_filters) {
            continue;
        }
        let surface = build_surface_index(&artifact)?;
        let id = artifact.header.id.clone();
        let mut best: Option<EventHit> = None;
        scan_artifact(&artifact, |index, _, event| {
            if !event_matches(event, surface.surfaces[index], &event_filters) {
                return Ok(());
            }
            let Some(text) = semantic_text(event) else {
                return Ok(());
            };
            let Some(found) = matcher.find(&text) else {
                return Ok(());
            };
            let hit = EventHit {
                session_id: id.clone(),
                seq: index as u64,
                kind: event.kind.clone(),
                time: event.time().unwrap_or(0),
                surface: surface.surfaces[index],
                snippet: make_snippet(&text, found.start, SNIPPET_CHARS),
                match_count: found.count,
                document_length: text.chars().count(),
            };
            if best
                .as_ref()
                .is_none_or(|existing| hit.compare(existing).is_lt())
            {
                best = Some(hit);
            }
            Ok(())
        })?;
        if let Some(best) = best {
            hits.push((artifact.header, best));
        }
    }
    hits.sort_by(|left, right| left.1.compare(&right.1));
    let items: Vec<Value> = hits
        .iter()
        .map(|(header, best)| {
            json!({
                "header": logical_header(header),
                "live": false,
                "persisted": true,
                "formatStatus": "current",
                "bestMatch": best.wire(),
            })
        })
        .collect();
    if cursor.offset > items.len() {
        return Err(query_error(
            "SESSION_QUERY_INVALID_CURSOR",
            "session-search cursor offset is out of range",
        ));
    }
    page_items(&items, cursor, limit, "sessions", &fingerprint, &generation)
}

/// Within-Session query-service search contract, including its header binding.
pub fn query_events(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let session_id = request
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            query_error(
                "SESSION_QUERY_INVALID_FILTER",
                "session-search session id must be text",
            )
        })?;
    let query = normalize_query(request.get("query"))?;
    let limit = normalize_limit(request)?;
    let cursor = cursor_text(request)?;
    let filters = parse_event_filters(request, "filters", false)?;
    validate_filter_budget(&[], &filters, true)?;
    let cwd = optional_cwd(request)?;
    let root = cx.home.join("sessions");
    let mut budget = Budget::new();
    let artifacts = inventory(&root, cwd.as_deref(), &mut budget)?;
    let generation = corpus_generation(&artifacts);
    let event_identity = canonical_event_filters(&filters);
    let fingerprint = request_fingerprint(
        "events",
        &query,
        Some(session_id),
        "",
        &event_identity,
        limit,
    );
    let cursor = cursor
        .map(|value| decode_cursor(value, "events", &fingerprint, &generation))
        .transpose()?
        .unwrap_or(Cursor { offset: 0 });
    let artifact = resolve_artifact(&artifacts, session_id, cwd.as_deref())?.clone();
    let surface = build_surface_index(&artifact)?;
    let matcher = Matcher::new(&query);
    let mut hits = Vec::new();
    scan_artifact(&artifact, |index, _, event| {
        if !event_matches(event, surface.surfaces[index], &filters) {
            return Ok(());
        }
        let Some(text) = semantic_text(event) else {
            return Ok(());
        };
        let Some(found) = matcher.find(&text) else {
            return Ok(());
        };
        hits.push(EventHit {
            session_id: session_id.to_owned(),
            seq: index as u64,
            kind: event.kind.clone(),
            time: event.time().unwrap_or(0),
            surface: surface.surfaces[index],
            snippet: make_snippet(&text, found.start, SNIPPET_CHARS),
            match_count: found.count,
            document_length: text.chars().count(),
        });
        Ok(())
    })?;
    hits.sort_by(EventHit::compare);
    let items: Vec<Value> = hits.iter().map(EventHit::wire).collect();
    if cursor.offset > items.len() {
        return Err(query_error(
            "SESSION_QUERY_INVALID_CURSOR",
            "session-search cursor offset is out of range",
        ));
    }
    let mut page = page_items(&items, cursor, limit, "events", &fingerprint, &generation)?;
    page["session"] = logical_header(&artifact.header);
    Ok(page)
}

/// Exact detached raw-log snapshot shape returned by `readSession`.
pub fn read_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let id = required_session_id(request)?;
    let cwd = optional_cwd(request)?;
    let root = cx.home.join("sessions");
    let mut budget = Budget::new();
    let artifacts = inventory(&root, cwd.as_deref(), &mut budget)?;
    let loaded = load_session(&artifacts, id, cwd.as_deref())?;
    Ok(json!({
        "session": logical_header(&loaded.artifact.header),
        "inheritedEventCount": 0,
        "events": loaded.events,
    }))
}

fn include_descendants(request: &Value) -> Result<bool, RpcError> {
    match request.get("includeDescendants") {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value == "true" => Ok(true),
        Some(Value::String(value)) if value == "false" => Ok(false),
        Some(_) => Err(bad_request("missing or invalid sessionId query parameter")),
    }
}

fn safe_session_segment(id: &str) -> String {
    id.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn descendants(root: &[Artifact], root_id: &str) -> Result<Vec<Artifact>, RpcError> {
    let mut children: HashMap<&str, Vec<&Artifact>> = HashMap::new();
    for artifact in root {
        if let Some(parent) = artifact.header.parent_session.as_deref() {
            children.entry(parent).or_default().push(artifact);
        }
    }
    for values in children.values_mut() {
        values.sort_by(|left, right| {
            left.header
                .created_at
                .cmp(&right.header.created_at)
                .then_with(|| left.header.id.cmp(&right.header.id))
        });
    }
    let mut output = Vec::new();
    let mut stack: Vec<&Artifact> = children.get(root_id).cloned().unwrap_or_default();
    let mut seen = HashSet::from([root_id.to_owned()]);
    while let Some(artifact) = stack.pop() {
        if !seen.insert(artifact.header.id.clone()) {
            return Err(query_error(
                "SESSION_QUERY_INVALID_LINEAGE",
                "session lineage contains a cycle or duplicate id",
            ));
        }
        output.push(artifact.clone());
        if let Some(next) = children.get(artifact.header.id.as_str()) {
            for child in next.iter().rev() {
                stack.push(child);
            }
        }
    }
    Ok(output)
}

#[derive(Clone)]
struct AttachmentRef {
    path: String,
    id: String,
}

fn collect_attachment_refs(content: &Value, output: &mut BTreeMap<String, AttachmentRef>) {
    let Some(blocks) = content.as_array() else {
        return;
    };
    for block in blocks {
        let Some(kind) = block.get("type").and_then(Value::as_str) else {
            continue;
        };
        if kind != "image" && kind != "file" {
            continue;
        }
        let Some(attachment) = block.get("attachment") else {
            continue;
        };
        let Some(id) = attachment.get("attachmentId").and_then(Value::as_str) else {
            continue;
        };
        let path = if kind == "image" {
            let extension = match attachment.get("mediaType").and_then(Value::as_str) {
                Some("image/png") => "png",
                Some("image/jpeg") => "jpg",
                Some("image/webp") => "webp",
                Some("image/gif") => "gif",
                _ => "bin",
            };
            format!("media/{id}.{extension}")
        } else {
            let digest = id.strip_prefix("sha256:").unwrap_or(id);
            let raw_name = attachment
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("file");
            let name: String = raw_name
                .chars()
                .map(|character| {
                    if character == '\\'
                        || character == '/'
                        || character.is_control()
                        || character == '\u{7f}'
                    {
                        '_'
                    } else {
                        character
                    }
                })
                .collect();
            let name = if matches!(name.as_str(), "" | "." | "..") {
                "file"
            } else {
                name.as_str()
            };
            format!(
                "files/{}/{digest}/{name}",
                digest.chars().take(2).collect::<String>()
            )
        };
        let key = if kind == "image" {
            format!("image\0{id}")
        } else {
            format!(
                "file\0{id}\0{}",
                attachment
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("file")
            )
        };
        output.insert(
            key,
            AttachmentRef {
                path,
                id: id.to_owned(),
            },
        );
    }
}

fn attachment_refs_in_event(event: &Event, output: &mut BTreeMap<String, AttachmentRef>) {
    let data = &event.data;
    match event.kind.as_str() {
        "user/message" | "tool/ptc-dispatch" => {
            collect_attachment_refs(data.get("content").unwrap_or(&Value::Null), output)
        }
        "system/message" | "developer/message" | "tool/result" | "team/message/queued" => {
            collect_attachment_refs(
                data.get("message")
                    .and_then(|message| message.get("content"))
                    .unwrap_or(&Value::Null),
                output,
            )
        }
        "agent/inbox/spliced" => {
            if let Some(messages) = data.get("inserted").and_then(Value::as_array) {
                for message in messages {
                    collect_attachment_refs(message.get("content").unwrap_or(&Value::Null), output);
                }
            }
        }
        "compaction/summary" => {
            collect_attachment_refs(data.get("summary").unwrap_or(&Value::Null), output);
            collect_attachment_refs(data.get("rawOutput").unwrap_or(&Value::Null), output);
        }
        "assistant/message" => {
            collect_attachment_refs(
                data.get("message")
                    .and_then(|message| message.get("content"))
                    .unwrap_or(&Value::Null),
                output,
            );
            if let Some(stream) = data.get("stream").and_then(Value::as_array) {
                for record in stream {
                    if record.get("type").and_then(Value::as_str) == Some("chunk")
                        && record
                            .get("chunk")
                            .and_then(|chunk| chunk.get("type"))
                            .and_then(Value::as_str)
                            == Some("block-end")
                    {
                        collect_attachment_refs(
                            record
                                .get("chunk")
                                .and_then(|chunk| chunk.get("block"))
                                .unwrap_or(&Value::Null),
                            output,
                        );
                    }
                }
            }
        }
        _ => {}
    }
}

/// Headless ZIP adaptation of `/api/session.export`; bytes are standard base64.
/// Entries use ZIP's stored method because this crate has no DEFLATE dependency.
pub fn export_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let id = required_session_id(request)?;
    let include_descendants = include_descendants(request)?;
    let cwd = optional_cwd(request)?;
    let root = cx.home.join("sessions");
    let mut budget = Budget::new();
    let artifacts = inventory(&root, cwd.as_deref(), &mut budget)?;
    let root_artifact = resolve_artifact(&artifacts, id, cwd.as_deref())?.clone();
    let filename = format!("dsh-session-{}.zip", safe_session_segment(id));
    let mut included = vec![root_artifact.clone()];
    if include_descendants {
        included.extend(descendants(&artifacts, id)?);
    }
    let mut paths = HashSet::new();
    let mut zip = ZipWriter::new()?;
    let mut attachments = BTreeMap::new();
    for (position, artifact) in included.iter().enumerate() {
        build_surface_index(artifact)?;
        let path = if position == 0 {
            session_format_log_filename(SESSION_FORMAT_VERSION)
        } else {
            format!(
                "subagents/{}/{}",
                safe_session_segment(&artifact.header.id),
                session_format_log_filename(SESSION_FORMAT_VERSION)
            )
        };
        if !paths.insert(path.clone()) {
            return Err(query_error(
                "SESSION_QUERY_INVALID_LINEAGE",
                "sanitized session ids collide in the export archive",
            ));
        }
        zip.begin(path)?;
        zip.write(dsh_session_format::encode_header(&artifact.header).as_bytes())?;
        scan_artifact(artifact, |_, _, event| {
            zip.write(dsh_session_format::encode_event(event).as_bytes())?;
            attachment_refs_in_event(event, &mut attachments);
            Ok(())
        })?;
        zip.end()?;
    }
    if !attachments.is_empty() {
        let mut missing = String::new();
        for reference in attachments.values() {
            missing.push_str(&format!(
                "{}\tATTACHMENT_NOT_FOUND: attachment store unavailable ({})\n",
                reference.path, reference.id
            ));
        }
        zip.begin("attachments-missing.txt".into())?;
        zip.write(missing.as_bytes())?;
        zip.end()?;
    }
    let zip = zip.finish()?;
    Ok(json!({
        "filename": filename,
        "contentType": "application/zip",
        "contentDisposition": format!("attachment; filename=\"{}\"", filename),
        "data": base64_standard_encode(&zip)?,
        "compression": "stored",
    }))
}

/// Headless Markdown adaptation of `/api/session.markdown`.
pub fn export_markdown(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let id = required_session_id(request)?;
    let cwd = optional_cwd(request)?;
    let source_override = match request.get("sourcePath") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return Err(bad_request("sourcePath must be text")),
    };
    let root = cx.home.join("sessions");
    let mut budget = Budget::new();
    let artifacts = inventory(&root, cwd.as_deref(), &mut budget)?;
    let loaded = load_session(&artifacts, id, cwd.as_deref())?;
    let source = source_override.unwrap_or_else(|| loaded.artifact.path.display().to_string());
    let content = format_session_markdown(&loaded.events, &source);
    if content.len() > MAX_MARKDOWN_BYTES {
        return Err(resource_error(format!(
            "session markdown exceeds the {MAX_MARKDOWN_BYTES}-byte content bound"
        )));
    }
    let response = json!({"contentType": "text/markdown; charset=utf-8", "content": content});
    let encoded_len = serde_json::to_vec(&response)
        .map_err(|error| resource_error(format!("session markdown serialization failed: {error}")))?
        .len();
    if encoded_len > MAX_EXPORT_BYTES {
        return Err(resource_error(format!(
            "session markdown exceeds the {MAX_EXPORT_BYTES}-byte encoded bound"
        )));
    }
    Ok(response)
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

struct ZipEntry {
    name: String,
    offset: u32,
    crc: u32,
    size: u32,
}

struct ZipWriter {
    output: Vec<u8>,
    central: Vec<u8>,
    entries: Vec<ZipEntry>,
    active: Option<(usize, u32, usize)>,
}

impl ZipWriter {
    fn new() -> Result<Self, RpcError> {
        Ok(Self {
            output: Vec::new(),
            central: Vec::new(),
            entries: Vec::new(),
            active: None,
        })
    }

    fn reserve(&mut self, additional: usize) -> Result<(), RpcError> {
        let prospective = self
            .output
            .len()
            .checked_add(additional)
            .and_then(|length| length.checked_add(self.central.len()))
            .and_then(|length| length.checked_add((self.entries.len() + 1).saturating_mul(46)))
            .and_then(|length| length.checked_add(22));
        if prospective.is_none_or(|length| length > MAX_ZIP_BYTES) {
            return Err(resource_error(format!(
                "session export exceeds the {MAX_EXPORT_BYTES}-byte encoded bound"
            )));
        }
        self.output
            .try_reserve(additional)
            .map_err(|_| resource_error("session export allocation failed"))
    }

    fn begin(&mut self, name: String) -> Result<(), RpcError> {
        if self.active.is_some() {
            return Err(resource_error("session export entry is already open"));
        }
        if self.entries.len() >= u16::MAX as usize || name.len() > u16::MAX as usize {
            return Err(resource_error(
                "session export has too many or overly long ZIP entries",
            ));
        }
        self.reserve(30 + name.len())?;
        u32::try_from(self.output.len())
            .map_err(|_| resource_error("session export is too large for ZIP"))?;
        let header_offset = self.output.len();
        push_u32(&mut self.output, 0x04034b50);
        push_u16(&mut self.output, 20);
        push_u16(&mut self.output, 0x0800);
        push_u16(&mut self.output, 0);
        push_u16(&mut self.output, 0);
        push_u16(&mut self.output, 0x0021);
        push_u32(&mut self.output, 0);
        push_u32(&mut self.output, 0);
        push_u32(&mut self.output, 0);
        push_u16(&mut self.output, name.len() as u16);
        push_u16(&mut self.output, 0);
        self.output.extend_from_slice(name.as_bytes());
        self.active = Some((header_offset, !0u32, 0usize));
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), RpcError> {
        let Some((_, mut crc, mut size)) = self.active else {
            return Err(resource_error("session export entry is not open"));
        };
        size = size
            .checked_add(bytes.len())
            .ok_or_else(|| resource_error("session export size overflow"))?;
        if size > u32::MAX as usize {
            return Err(resource_error("session export entry is too large for ZIP"));
        }
        self.reserve(bytes.len())?;
        crc = crc32_update(crc, bytes);
        self.active = Some((self.active.expect("active entry").0, crc, size));
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    fn end(&mut self) -> Result<(), RpcError> {
        let (offset, crc, size) = self
            .active
            .take()
            .ok_or_else(|| resource_error("session export entry is not open"))?;
        let size = u32::try_from(size)
            .map_err(|_| resource_error("session export entry is too large for ZIP"))?;
        self.output[offset + 14..offset + 18].copy_from_slice(&(!crc).to_le_bytes());
        self.output[offset + 18..offset + 22].copy_from_slice(&size.to_le_bytes());
        self.output[offset + 22..offset + 26].copy_from_slice(&size.to_le_bytes());
        let name = String::from_utf8(
            self.output[offset + 30
                ..offset
                    + 30
                    + u16::from_le_bytes(self.output[offset + 26..offset + 28].try_into().unwrap())
                        as usize]
                .to_vec(),
        )
        .map_err(|_| resource_error("session export entry name is not UTF-8"))?;
        self.entries.push(ZipEntry {
            name,
            offset: u32::try_from(offset).unwrap(),
            crc: !crc,
            size,
        });
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<u8>, RpcError> {
        if self.active.is_some() {
            return Err(resource_error("session export entry is still open"));
        }
        for entry in &self.entries {
            self.central
                .try_reserve(46 + entry.name.len())
                .map_err(|_| resource_error("session export allocation failed"))?;
            push_u32(&mut self.central, 0x02014b50);
            push_u16(&mut self.central, 20);
            push_u16(&mut self.central, 20);
            push_u16(&mut self.central, 0x0800);
            push_u16(&mut self.central, 0);
            push_u16(&mut self.central, 0);
            push_u16(&mut self.central, 0x0021);
            push_u32(&mut self.central, entry.crc);
            push_u32(&mut self.central, entry.size);
            push_u32(&mut self.central, entry.size);
            push_u16(&mut self.central, entry.name.len() as u16);
            push_u16(&mut self.central, 0);
            push_u16(&mut self.central, 0);
            push_u16(&mut self.central, 0);
            push_u16(&mut self.central, 0);
            push_u32(&mut self.central, 0);
            push_u32(&mut self.central, entry.offset);
            self.central.extend_from_slice(entry.name.as_bytes());
        }
        self.reserve(self.central.len() + 22)?;
        let central_offset = u32::try_from(self.output.len())
            .map_err(|_| resource_error("session export is too large for ZIP"))?;
        let central_size = u32::try_from(self.central.len())
            .map_err(|_| resource_error("session export is too large for ZIP"))?;
        self.output.extend_from_slice(&self.central);
        push_u32(&mut self.output, 0x06054b50);
        push_u16(&mut self.output, 0);
        push_u16(&mut self.output, 0);
        push_u16(&mut self.output, self.entries.len() as u16);
        push_u16(&mut self.output, self.entries.len() as u16);
        push_u32(&mut self.output, central_size);
        push_u32(&mut self.output, central_offset);
        push_u16(&mut self.output, 0);
        Ok(self.output)
    }
}

fn crc32_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    crc
}

fn base64_standard_encode(bytes: &[u8]) -> Result<String, RpcError> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let capacity = bytes
        .len()
        .checked_add(2)
        .and_then(|length| length.checked_div(3))
        .and_then(|length| length.checked_mul(4))
        .filter(|length| *length <= MAX_EXPORT_BYTES)
        .ok_or_else(|| resource_error("session export encoded size overflow"))?;
    let mut output = String::new();
    output
        .try_reserve(capacity)
        .map_err(|_| resource_error("session export allocation failed"))?;
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(ALPHABET[((value >> 18) & 63) as usize] as char);
        output.push(ALPHABET[((value >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[((value >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[(value & 63) as usize] as char
        } else {
            '='
        });
    }
    Ok(output)
}

fn base64url_encode(bytes: &[u8]) -> Result<String, RpcError> {
    Ok(base64_standard_encode(bytes)?
        .trim_end_matches('=')
        .replace('+', "-")
        .replace('/', "_"))
}

fn base64url_decode(value: &str) -> Result<Vec<u8>, ()> {
    if value.len() % 4 == 1
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(());
    }
    let mut output = Vec::with_capacity(value.len() * 3 / 4 + 2);
    let mut accumulator = 0u32;
    let mut bits = 0u32;
    for byte in value.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(()),
        };
        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((accumulator >> bits) as u8);
            accumulator &= (1u32 << bits).wrapping_sub(1);
        }
    }
    if bits != 0 && (accumulator & ((1 << bits) - 1)) != 0 {
        return Err(());
    }
    Ok(output)
}

#[derive(Clone)]
struct MarkdownTurn {
    segments: Vec<MarkdownSegment>,
    calls: HashMap<String, usize>,
}

#[derive(Clone)]
enum MarkdownSegment {
    User(String),
    Assistant { text: String, tool_calls: bool },
    Tool(MarkdownTool),
}

#[derive(Clone)]
struct MarkdownTool {
    name: String,
    arguments: String,
    result: Option<MarkdownToolResult>,
}

#[derive(Clone)]
struct MarkdownToolResult {
    text: String,
    is_error: bool,
    reason: Option<String>,
}

fn format_session_markdown(events: &[Event], source_path: &str) -> String {
    let title = events
        .iter()
        .rev()
        .find(|event| event.kind == "session/title")
        .and_then(|event| event.data.get("title").and_then(Value::as_str))
        .map(normalize_title)
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| "Untitled Session".into());
    let mut sections = vec![format!("# {title}")];
    if !source_path.is_empty() {
        sections.push(format!(
            "Session file:\n\n{}",
            fenced(source_path, "text", false)
        ));
    }
    for turn in collect_markdown_turns(events) {
        let final_index = turn
            .segments
            .iter()
            .rposition(|segment| {
                matches!(
                    segment,
                    MarkdownSegment::Assistant {
                        tool_calls: false,
                        ..
                    }
                )
            })
            .filter(|index| *index + 1 == turn.segments.len())
            .unwrap_or(usize::MAX);
        let end = if final_index == usize::MAX {
            turn.segments.len()
        } else {
            final_index
        };
        let mut pending = Vec::new();
        for segment in &turn.segments[..end] {
            match segment {
                MarkdownSegment::User(text) => {
                    if !pending.is_empty() {
                        sections.push(collapsible_section(&pending));
                        pending.clear();
                    }
                    sections.push(quote_markdown(text));
                }
                other => pending.push(other.clone()),
            }
        }
        if !pending.is_empty() {
            sections.push(collapsible_section(&pending));
        }
        if final_index != usize::MAX {
            if let MarkdownSegment::Assistant { text, .. } = &turn.segments[final_index] {
                let text = close_unfinished_fence(&trim_blank_edge_lines(text));
                if !text.is_empty() {
                    sections.push(text);
                }
            }
        }
    }
    format!("{}\n", sections.join("\n\n").trim_end())
}

fn normalize_title(title: &str) -> String {
    title
        .replace("\r\n", " ")
        .replace(['\r', '\n'], " ")
        .trim()
        .to_owned()
}

fn collect_markdown_turns(events: &[Event]) -> Vec<MarkdownTurn> {
    let mut turns = Vec::new();
    let mut current: Option<usize> = None;
    for event in events {
        match event.kind.as_str() {
            "turn/start" => {
                turns.push(MarkdownTurn {
                    segments: Vec::new(),
                    calls: HashMap::new(),
                });
                current = Some(turns.len() - 1);
            }
            "turn/end" => current = None,
            "user/message" => {
                if event.extra.get("surfaceOp").and_then(Value::as_str) != Some("append")
                    || event
                        .data
                        .get("source")
                        .and_then(|source| source.get("kind"))
                        .and_then(Value::as_str)
                        != Some("user")
                {
                    continue;
                }
                let text =
                    markdown_visible_content(event.data.get("content").unwrap_or(&Value::Null));
                if !text.is_empty() {
                    active_turn(&mut turns, &mut current)
                        .segments
                        .push(MarkdownSegment::User(text));
                }
            }
            "assistant/message" => {
                if event.extra.get("surfaceOp").and_then(Value::as_str) != Some("append") {
                    continue;
                }
                let Some(message) = event.data.get("message") else {
                    continue;
                };
                let text = markdown_visible_content(message.get("content").unwrap_or(&Value::Null));
                let tool_calls = message
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block.get("type").and_then(Value::as_str) == Some("tool-call")
                        })
                    });
                if !trim_blank_edge_lines(&text).is_empty() {
                    active_turn(&mut turns, &mut current)
                        .segments
                        .push(MarkdownSegment::Assistant { text, tool_calls });
                }
            }
            "tool/call" => {
                let name = event
                    .data
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let arguments = event
                    .data
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let call_id = scalar_id(event.data.get("callId").unwrap_or(&Value::Null));
                let turn = active_turn(&mut turns, &mut current);
                let index = turn.segments.len();
                turn.segments.push(MarkdownSegment::Tool(MarkdownTool {
                    name,
                    arguments,
                    result: None,
                }));
                turn.calls.insert(call_id, index);
            }
            "tool/result" => {
                if event.extra.get("surfaceOp").and_then(Value::as_str) != Some("append") {
                    continue;
                }
                let call_id = scalar_id(
                    event
                        .data
                        .get("message")
                        .and_then(|message| message.get("toolCallId"))
                        .unwrap_or(&Value::Null),
                );
                let turn = active_turn(&mut turns, &mut current);
                let index = *turn.calls.entry(call_id).or_insert_with(|| {
                    let index = turn.segments.len();
                    turn.segments.push(MarkdownSegment::Tool(MarkdownTool {
                        name: "unknown".into(),
                        arguments: String::new(),
                        result: None,
                    }));
                    index
                });
                let result = MarkdownToolResult {
                    text: markdown_visible_content(
                        event
                            .data
                            .get("message")
                            .and_then(|message| message.get("content"))
                            .unwrap_or(&Value::Null),
                    ),
                    is_error: event
                        .data
                        .get("message")
                        .and_then(|message| message.get("isError"))
                        .and_then(Value::as_bool)
                        == Some(true),
                    reason: event
                        .data
                        .get("error")
                        .and_then(|error| error.get("reason"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                };
                if let MarkdownSegment::Tool(tool) = &mut turn.segments[index] {
                    tool.result = Some(result);
                }
            }
            _ => {}
        }
    }
    turns
        .into_iter()
        .filter(|turn| !turn.segments.is_empty())
        .collect()
}

fn active_turn<'a>(
    turns: &'a mut Vec<MarkdownTurn>,
    current: &'a mut Option<usize>,
) -> &'a mut MarkdownTurn {
    if current.is_none() {
        turns.push(MarkdownTurn {
            segments: Vec::new(),
            calls: HashMap::new(),
        });
        *current = Some(turns.len() - 1);
    }
    &mut turns[current.expect("current turn")]
}

fn scalar_id(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn markdown_visible_content(content: &Value) -> String {
    content
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => block
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                        .map(str::to_owned),
                    Some("image") => block.get("attachment").map(image_label),
                    Some("file") => block.get("attachment").map(file_label),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n\n")
        })
        .unwrap_or_default()
}

fn label_name(value: Option<&str>) -> String {
    let normalized = value
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(']', "\\]")
        .trim()
        .to_owned();
    if normalized.is_empty() {
        "attachment".into()
    } else {
        normalized
    }
}

fn image_label(attachment: &Value) -> String {
    format!(
        "[Image: {} ({}×{}, {} bytes)]",
        label_name(attachment.get("name").and_then(Value::as_str)),
        attachment.get("width").and_then(Value::as_u64).unwrap_or(0),
        attachment
            .get("height")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        attachment.get("bytes").and_then(Value::as_u64).unwrap_or(0),
    )
}

fn file_label(attachment: &Value) -> String {
    format!(
        "[File: {} ({} bytes)]",
        label_name(attachment.get("name").and_then(Value::as_str)),
        attachment.get("bytes").and_then(Value::as_u64).unwrap_or(0),
    )
}

fn quote_markdown(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let normalized = normalized.trim_end_matches('\n');
    normalized
        .split('\n')
        .map(|line| {
            if line.is_empty() {
                ">".into()
            } else {
                format!("> {line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn collapsible_section(entries: &[MarkdownSegment]) -> String {
    let summary = if entries.len() == 1 {
        "1 previous message".to_owned()
    } else {
        format!("{} previous messages", entries.len())
    };
    let body = entries
        .iter()
        .map(render_markdown_entry)
        .collect::<Vec<_>>()
        .join("\n\n");
    format!("<details><summary>{summary}</summary>\n\n{body}\n\n</details>")
}

fn render_markdown_entry(entry: &MarkdownSegment) -> String {
    match entry {
        MarkdownSegment::Assistant { text, .. } => {
            format!(
                "**Assistant**\n\n{}",
                escape_wrapper_tags_outside_fences(&close_unfinished_fence(
                    &trim_blank_edge_lines(text)
                ))
            )
        }
        MarkdownSegment::Tool(tool) => {
            let heading = format!("**Tool: `{}`**", escape_wrapper_tags(&tool.name));
            let arguments = if tool.arguments.is_empty() {
                "(no arguments)".into()
            } else {
                let parsed = serde_json::from_str::<Value>(&tool.arguments)
                    .ok()
                    .and_then(|value| serde_json::to_string_pretty(&value).ok())
                    .unwrap_or_else(|| tool.arguments.clone());
                fenced(&parsed, "json", true)
            };
            let result_heading = if tool.result.as_ref().is_some_and(|result| result.is_error) {
                "**Result (error)**"
            } else {
                "**Result**"
            };
            let result_body = match &tool.result {
                None => "(no result recorded)".into(),
                Some(result)
                    if result
                        .reason
                        .as_ref()
                        .is_some_and(|reason| !reason.is_empty()) =>
                {
                    let mut body = format!(
                        "Error: {}",
                        escape_wrapper_tags(result.reason.as_deref().unwrap_or_default())
                    );
                    if !result.text.is_empty() {
                        body.push_str(&format!("\n\n{}", fenced(&result.text, "", true)));
                    }
                    body
                }
                Some(result) if result.text.is_empty() => "(no output)".into(),
                Some(result) => fenced(&result.text, "", true),
            };
            format!("{heading}\n\n{arguments}\n\n{result_heading}\n\n{result_body}")
        }
        MarkdownSegment::User(_) => String::new(),
    }
}

fn fenced(text: &str, language: &str, trim_trailing: bool) -> String {
    let content = if trim_trailing {
        text.trim_end_matches('\n')
    } else {
        text
    };
    let mut longest = 0usize;
    let mut run = 0usize;
    for byte in content.bytes() {
        if byte == b'`' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    let fence = "`".repeat((longest + 1).max(3));
    format!("{fence}{language}\n{content}\n{fence}")
}

fn trim_blank_edge_lines(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();
    let mut start = 0;
    let mut end = lines.len();
    while start < end && lines[start].trim().is_empty() {
        start += 1;
    }
    while end > start && lines[end - 1].trim().is_empty() {
        end -= 1;
    }
    lines[start..end].join("\n")
}

#[derive(Clone, Copy)]
struct Fence {
    character: char,
    length: usize,
}

fn opening_fence(line: &str) -> Option<Fence> {
    let trimmed = line.trim_start_matches(' ');
    let spaces = line.len() - trimmed.len();
    if spaces > 3 {
        return None;
    }
    let character = trimmed.chars().next()?;
    if character != '`' && character != '~' {
        return None;
    }
    let length = trimmed
        .chars()
        .take_while(|value| *value == character)
        .count();
    if length < 3 {
        return None;
    }
    let info = &trimmed[length..];
    if character == '`' && info.contains('`') {
        return None;
    }
    Some(Fence { character, length })
}

fn closes_fence(line: &str, fence: Fence) -> bool {
    let trimmed = line.trim_start_matches(' ');
    if line.len() - trimmed.len() > 3 {
        return false;
    }
    let marker_length = trimmed.trim_end_matches(fence.character).len();
    let marker = &trimmed[..marker_length];
    marker.trim().is_empty() && trimmed.len() - marker_length >= fence.length
}

fn scan_markdown(markdown: &str) -> (Vec<(String, bool)>, Option<Fence>) {
    let normalized = markdown.replace("\r\n", "\n").replace('\r', "\n");
    let mut output = Vec::new();
    let mut fence = None;
    let mut indented = false;
    for line in normalized.split('\n') {
        if let Some(active) = fence {
            output.push((line.to_owned(), true));
            if closes_fence(line, active) {
                fence = None;
            }
            continue;
        }
        if let Some(opening) = opening_fence(line) {
            fence = Some(opening);
            indented = false;
            output.push((line.to_owned(), true));
            continue;
        }
        if line.starts_with("    ") || line.starts_with('\t') {
            indented = true;
            output.push((line.to_owned(), true));
            continue;
        }
        output.push((line.to_owned(), indented && line.trim().is_empty()));
        if !line.trim().is_empty() {
            indented = false;
        }
    }
    (output, fence)
}

fn close_unfinished_fence(markdown: &str) -> String {
    let (_, fence) = scan_markdown(markdown);
    match fence {
        None => markdown.to_owned(),
        Some(fence) => format!(
            "{}\n{}",
            markdown.trim_end_matches('\n'),
            fence.character.to_string().repeat(fence.length)
        ),
    }
}

fn escape_wrapper_tags_outside_fences(markdown: &str) -> String {
    scan_markdown(markdown)
        .0
        .into_iter()
        .map(|(line, code)| {
            if code {
                line
            } else {
                escape_wrapper_tags(&line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn escape_wrapper_tags(text: &str) -> String {
    let mut output = String::new();
    let mut remaining = text;
    while let Some(index) = remaining.find('<') {
        output.push_str(&remaining[..index]);
        remaining = &remaining[index..];
        let rest = &remaining[1..];
        let name = rest
            .trim_start_matches('/')
            .split(|character: char| !character.is_ascii_alphanumeric())
            .next()
            .unwrap_or_default();
        if name.eq_ignore_ascii_case("details") || name.eq_ignore_ascii_case("summary") {
            let boundary = name.len() + usize::from(rest.starts_with('/'));
            let suffix = &rest[boundary..];
            if suffix.starts_with('/') || suffix.starts_with('>') || suffix.starts_with(' ') {
                output.push_str("&lt;");
            } else {
                output.push('<');
            }
            remaining = rest;
        } else {
            output.push('<');
            remaining = rest;
        }
    }
    output.push_str(remaining);
    output
}
