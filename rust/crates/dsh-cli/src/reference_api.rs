//! Workspace file and Session mention discovery RPCs.

use crate::api::{default_workspace_path, request_of, rpc_error, ApiContext};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use dsh_gateway::{Gateway, RpcError};
use dsh_session_persistence::{StoreError, Stored};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const FILE_RESULT_LIMIT: usize = 20;
const FILE_INDEX_LIMIT: usize = 50_000;
const SESSION_RESULT_LIMIT: usize = 50;
const EXCLUDED_DIRECTORIES: &[&str] = &[
    ".git",
    "node_modules",
    "dist",
    "build",
    "out",
    "coverage",
    "target",
    ".next",
    ".nuxt",
    ".turbo",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".gradle",
];

pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    let file_context = Arc::clone(&context);
    gateway.rpc("fileReferences/list", move |payload| {
        let context = Arc::clone(&file_context);
        async move { list_files(&context, payload).await }
    });

    gateway.rpc("sessionReferenceResolver/candidates", move |payload| {
        let context = Arc::clone(&context);
        async move { list_sessions(&context, payload).await }
    });
}

async fn list_files(context: &ApiContext, payload: Value) -> Result<Value, RpcError> {
    let request = request_of(&payload);
    let session_id = required_agent_id(request)?;
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let metadata = context
        .store()
        .metadata_by_id(session_id)
        .map_err(|error| store_error(session_id, error))?;
    let root = metadata
        .header
        .cwd
        .map(PathBuf::from)
        .unwrap_or_else(|| default_workspace_path(&context.home));
    let candidates = tokio::task::spawn_blocking(move || file_candidates(&root, &query))
        .await
        .map_err(|error| rpc_error("gateway/internal", error.to_string()))?;
    Ok(Value::Array(candidates))
}

async fn list_sessions(context: &ApiContext, payload: Value) -> Result<Value, RpcError> {
    let request = request_of(&payload);
    let session_id = required_agent_id(request)?.to_owned();
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let store = context.store();
    let candidates = tokio::task::spawn_blocking(move || {
        let all = store.read_all();
        let current = all
            .iter()
            .find(|stored| stored.header.id == session_id)
            .ok_or_else(|| {
                rpc_error(
                    "session/not-found",
                    format!("session {session_id:?} not found"),
                )
            })?;
        Ok::<_, RpcError>(session_candidates(&all, current, &query))
    })
    .await
    .map_err(|error| rpc_error("gateway/internal", error.to_string()))??;
    Ok(Value::Array(candidates))
}

fn file_candidates(root: &Path, raw_query: &str) -> Vec<Value> {
    let Ok(root) = root.canonicalize() else {
        return Vec::new();
    };
    if !root.is_dir() {
        return Vec::new();
    }
    let query = raw_query.replace('\\', "/");
    let mut candidates = if let Some(slash) = query.rfind('/') {
        let directory = &query[..slash];
        let fragment = &query[slash + 1..];
        list_directory(&root, directory, fragment)
    } else {
        let mut indexed = Vec::new();
        scan_workspace(&root, &mut indexed);
        indexed
            .into_iter()
            .filter(|(path, _)| {
                query.starts_with('.')
                    || query.contains("/.")
                    || !path.split('/').any(|part| part.starts_with('.'))
            })
            .collect::<Vec<_>>()
    };
    candidates.sort_by(|left, right| {
        let left_score = file_score(
            &left.0,
            left.1,
            if query.contains('/') {
                query.rsplit('/').next().unwrap_or_default()
            } else {
                &query
            },
        );
        let right_score = file_score(
            &right.0,
            right.1,
            if query.contains('/') {
                query.rsplit('/').next().unwrap_or_default()
            } else {
                &query
            },
        );
        right_score
            .cmp(&left_score)
            .then_with(|| (left.1 != "directory").cmp(&(right.1 != "directory")))
            .then_with(|| {
                if query.is_empty() || query.ends_with('/') {
                    Ordering::Equal
                } else {
                    left.0.len().cmp(&right.0.len())
                }
            })
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates
        .into_iter()
        .filter(|(path, kind)| {
            file_score(path, kind, query.rsplit('/').next().unwrap_or_default()).is_some()
        })
        .take(FILE_RESULT_LIMIT)
        .map(|(path, kind)| json!({"path": path, "kind": kind}))
        .collect()
}

fn list_directory(root: &Path, directory: &str, fragment: &str) -> Vec<(String, &'static str)> {
    if directory.starts_with('/')
        || directory.contains(':')
        || directory
            .split('/')
            .any(|part| part == ".." || EXCLUDED_DIRECTORIES.contains(&part))
    {
        return Vec::new();
    }
    let mut absolute = root.to_path_buf();
    for segment in directory
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
    {
        absolute.push(segment);
        let Ok(metadata) = fs::symlink_metadata(&absolute) else {
            return Vec::new();
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Vec::new();
        }
    }
    let Ok(entries) = fs::read_dir(&absolute) else {
        return Vec::new();
    };
    let prefix = if directory.is_empty() {
        String::new()
    } else {
        format!("{directory}/")
    };
    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') && !fragment.starts_with('.') {
            continue;
        }
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let candidate_kind = if kind.is_dir() && !EXCLUDED_DIRECTORIES.contains(&name.as_str()) {
            "directory"
        } else if kind.is_file() {
            "file"
        } else {
            continue;
        };
        let path = format!("{prefix}{name}");
        if file_score(&path, candidate_kind, fragment).is_some() {
            candidates.push((path, candidate_kind));
        }
    }
    candidates
}

fn scan_workspace(root: &Path, indexed: &mut Vec<(String, &'static str)>) {
    let mut directories = vec![(root.to_path_buf(), String::new())];
    let mut cursor = 0;
    while cursor < directories.len() && indexed.len() < FILE_INDEX_LIMIT {
        let (absolute, relative) = directories[cursor].clone();
        cursor += 1;
        let Ok(entries) = fs::read_dir(&absolute) else {
            continue;
        };
        let mut entries = entries.flatten().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            let path = if relative.is_empty() {
                name.clone()
            } else {
                format!("{relative}/{name}")
            };
            if kind.is_dir() {
                if EXCLUDED_DIRECTORIES.contains(&name.as_str()) {
                    continue;
                }
                indexed.push((path.clone(), "directory"));
                directories.push((entry.path(), path));
            } else if kind.is_file() {
                indexed.push((path, "file"));
            }
            if indexed.len() >= FILE_INDEX_LIMIT {
                break;
            }
        }
    }
}

fn file_score(path: &str, kind: &str, query: &str) -> Option<i32> {
    if query.is_empty() {
        return Some(0);
    }
    let lower = path.to_lowercase();
    let name = lower.rsplit('/').next().unwrap_or_default();
    let needle = query.to_lowercase();
    let directory_bonus = if kind == "directory" { 25 } else { 0 };
    let score = if name == needle {
        1000
    } else if name.starts_with(&needle) {
        900
    } else if name.contains(&needle) {
        700
    } else if lower.contains(&needle) {
        500
    } else {
        300 + subsequence_score(&lower, &needle)?
    };
    Some(score + directory_bonus)
}

fn subsequence_score(target: &str, query: &str) -> Option<i32> {
    let mut cursor = 0;
    let mut gap = 0;
    for character in query.chars() {
        let found = target[cursor..].find(character)? + cursor;
        gap += found - cursor;
        cursor = found + character.len_utf8();
    }
    Some((100 - gap as i32).max(0))
}

fn session_candidates(all: &[Stored], current: &Stored, query: &str) -> Vec<Value> {
    let mut candidates = all
        .iter()
        .filter(|stored| stored.header.id != current.header.id)
        .filter_map(|stored| {
            let label = stored
                .events
                .iter()
                .rev()
                .find(|event| event.kind == "session/title")
                .and_then(|event| event.data.get("title"))
                .and_then(Value::as_str)
                .filter(|title| !title.is_empty())
                .unwrap_or(&stored.header.id);
            let subagent_label = (stored.header.origin == Some(dsh_session_format::Origin::Subagent))
                .then(|| {
                    stored
                        .events
                        .iter()
                        .find(|event| event.kind == "subagent/descriptor")
                        .and_then(|event| event.data.get("label"))
                        .and_then(Value::as_str)
                        .filter(|label| !label.is_empty())
                })
                .flatten();
            let display_title = subagent_label.unwrap_or(label);
            let score = session_score(label, display_title, &stored.header.id, stored.header.cwd.as_deref(), query)?;
            let workspace_rank = match (&stored.header.cwd, &current.header.cwd) {
                (Some(candidate), Some(target)) if candidate == target => 0,
                (None, _) => 1,
                _ => 2,
            };
            let mention = format_session_mention(&stored.header.id, display_title);
            let mut candidate = json!({
                "sessionId": stored.header.id,
                "label": label,
                "displayTitle": display_title,
                "sameWorkspace": stored.header.cwd.is_some() && stored.header.cwd == current.header.cwd,
                "createdAt": stored.header.created_at,
                "mention": mention,
            });
            if let Some(cwd) = &stored.header.cwd {
                candidate["cwd"] = json!(cwd);
            }
            Some((
                workspace_rank,
                score,
                stored.header.created_at,
                stored.header.id.as_str(),
                candidate,
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.3.cmp(right.3))
    });
    candidates
        .into_iter()
        .take(SESSION_RESULT_LIMIT)
        .map(|candidate| candidate.4)
        .collect()
}

fn session_score(
    label: &str,
    display_title: &str,
    session_id: &str,
    cwd: Option<&str>,
    query: &str,
) -> Option<i32> {
    if query.is_empty() {
        return Some(0);
    }
    let needle = query.to_lowercase();
    let fuzzy = [label, display_title]
        .into_iter()
        .map(str::to_lowercase)
        .filter_map(|text| {
            if text == needle {
                Some(1000)
            } else if text.starts_with(&needle) {
                Some(900)
            } else if text.contains(&needle) {
                Some(700)
            } else {
                subsequence_score(&text, &needle).map(|score| 300 + score)
            }
        })
        .max();
    let substring = [Some(session_id), cwd]
        .into_iter()
        .flatten()
        .map(str::to_lowercase)
        .any(|text| text.contains(&needle));
    fuzzy.or_else(|| substring.then_some(500))
}

fn format_session_mention(session_id: &str, label: &str) -> String {
    let id_json = serde_json::to_vec(session_id).expect("Session identity is JSON string");
    let uri = URL_SAFE_NO_PAD.encode(id_json);
    let label = label.replace('\\', "\\\\").replace(']', "\\]");
    format!("@[{}](dsh-session:{})", label, uri)
}

fn required_agent_id(request: &Value) -> Result<&str, RpcError> {
    request
        .get("agentId")
        .or_else(|| request.get("sessionId"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "agentId must not be empty"))
}

fn store_error(session_id: &str, error: StoreError) -> RpcError {
    match error {
        StoreError::NotFound(_) => rpc_error(
            "session/not-found",
            format!("session {session_id:?} not found"),
        ),
        error => rpc_error("gateway/internal", error.to_string()),
    }
}
