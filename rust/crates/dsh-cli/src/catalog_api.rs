//! Session-scoped composer catalogs served by the Rust Host.

use crate::api::{default_workspace_path, home_dir, request_of, ApiContext};
use crate::workspace_api::display_path;
use dsh_gateway::{Gateway, RpcError};
use dsh_session_persistence::{SessionMetadata, StoreError};
use dsh_skills::{SkillLoader, SkillRoot, SkillWarningCode};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) const STANDARD_PRESET: &str = "standard";
const PRESET_ROSTER: &[&str] = &["standard", "ptc", "minimal", "cordis"];

pub(crate) fn supported_presets() -> &'static [&'static str] {
    PRESET_ROSTER
}

pub(crate) fn supports_preset(id: &str) -> bool {
    PRESET_ROSTER.contains(&id)
}

pub(crate) fn register_all(gateway: &Gateway, context: Arc<ApiContext>) {
    let schedule = context.clone();
    gateway.rpc("schedule/catalog", move |_| {
        let context = schedule.clone();
        async move { read_catalog(move || schedule_catalog(&context.home)).await }
    });
    gateway.rpc("agentPresets/list", |_| async {
        Ok(
            serde_json::from_str(include_str!("../assets/rpc/agentPresets_list.json"))
                .expect("captured Agent preset catalog is valid JSON"),
        )
    });
    gateway.rpc("agentPresets/read", |payload| async move {
        read_catalog(move || read_preset(&payload)).await
    });
    let presets = context.clone();
    gateway.rpc("agentPresets/select", move |payload| {
        let context = presets.clone();
        async move { read_catalog(move || select_preset(&context, &payload)).await }
    });
    let commands = context.clone();
    gateway.rpc("commands/list", move |payload| {
        let context = commands.clone();
        async move {
            read_catalog(move || {
                let id = required_text(&payload, "agentId")?;
                session_metadata(&context, id)?;
                Ok(crate::commands_api::descriptors())
            })
            .await
        }
    });
    gateway.rpc("skills/list", move |payload| {
        let context = context.clone();
        async move { read_catalog(move || list_skills(&context, &payload)).await }
    });
}

fn schedule_catalog(home: &Path) -> Result<Value, RpcError> {
    match std::fs::symlink_metadata(home.join("storages/schedule.json")) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!([])),
        Err(error) => Err(internal_error(format!(
            "Schedule storage could not be inspected: {error}"
        ))),
        Ok(_) => Err(internal_error(
            "Reading existing scheduled tasks is unavailable in the Rust Host".into(),
        )),
    }
}

async fn read_catalog(
    read: impl FnOnce() -> Result<Value, RpcError> + Send + 'static,
) -> Result<Value, RpcError> {
    tokio::task::spawn_blocking(read)
        .await
        .map_err(|error| internal_error(format!("catalog worker failed: {error}")))?
}

fn required_text<'a>(payload: &'a Value, field: &str) -> Result<&'a str, RpcError> {
    request_of(payload)
        .get(field)
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| RpcError {
            code: "gateway/bad-request".into(),
            message: format!("{field} must not be empty"),
            details: json!({}),
        })
}

fn session_metadata(context: &ApiContext, id: &str) -> Result<SessionMetadata, RpcError> {
    context
        .store()
        .metadata_by_id(id)
        .map_err(|error| match error {
            StoreError::NotFound(_) => RpcError {
                code: "session/not-found".into(),
                message: format!("session {id:?} not found"),
                details: json!({ "sessionId": id }),
            },
            error => internal_error(format!("session {id:?} could not be inspected: {error}")),
        })
}

/// Tools the Agent builder adds on top of the preset's own registry.
const HOST_ADDED_TOOLS: &[(&str, &[&str])] = &[
    ("goal", &["create_goal", "get_goal", "update_goal"]),
    ("subagent", &["subagent"]),
    ("web", &["web_fetch", "web_search"]),
];

/// One built-in preset's declared composition, rendered from the tools this Host runs.
///
/// The viewer shows what the selected mode actually enables, so the rows are
/// generated from the same registration the Agent builder performs.
/// @param payload - RPC payload naming the preset.
/// @returns `{agentPreset, name, content}` with `content` holding pretty JSON.
fn read_preset(payload: &Value) -> Result<Value, RpcError> {
    let id = payload
        .get("agentPreset")
        .or_else(|| payload.get("agentId"))
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError {
            code: "gateway/bad-request".into(),
            message: "missing agentPreset".into(),
            details: Value::Null,
        })?;
    let preset = dsh_tools::ToolPreset::parse(id).ok_or_else(|| RpcError {
        code: "agent-preset/not-found".into(),
        message: format!("Agent preset {id:?} is unavailable in the Rust Host"),
        details: json!({"agentPreset": id, "available": supported_presets()}),
    })?;
    let mut registry = dsh_core::ToolRegistry::default();
    dsh_tools::register_for_preset(&mut registry, preset);
    let mut groups: Vec<Value> = Vec::new();
    let mut base: Vec<String> = Vec::new();
    for name in registry.names() {
        let group = match name.as_str() {
            "bash" | "pwsh" => "shell",
            "read" | "write" | "edit" | "apply_patch" => "fs",
            "glob" | "grep" => "fs-search",
            "todo_write" => "todo",
            "terminal_*" => "terminal",
            other => other,
        };
        base.push(format!("{group}:{name}"));
    }
    groups.push(json!({"group": "preset", "tools": base}));
    if preset != dsh_tools::ToolPreset::Minimal {
        for (group, tools) in HOST_ADDED_TOOLS {
            groups.push(json!({"group": group, "tools": tools}));
        }
    }
    let name = match id {
        "standard" => "Standard",
        "ptc" => "Programmatic tool calling",
        "minimal" => "Minimal",
        "cordis" => "Creator",
        other => other,
    };
    Ok(json!({
        "agentPreset": id,
        "name": name,
        "content": serde_json::to_string_pretty(&groups)
            .unwrap_or_else(|error| format!("composition unavailable: {error}")),
    }))
}

fn select_preset(context: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let id = required_text(payload, "agentId")?;
    let preset = required_text(payload, "agentPreset")?;
    let session = session_metadata(context, id)?;
    if !supports_preset(preset) {
        return Err(RpcError {
            code: "agent-preset/not-found".into(),
            message: format!("Agent preset {preset:?} is unavailable in the Rust Host"),
            details: json!({"agentPreset": preset, "available": supported_presets()}),
        });
    }
    let mut started = false;
    let mut selected = session
        .header
        .agent_preset
        .clone()
        .unwrap_or_else(|| STANDARD_PRESET.to_owned());
    context
        .store()
        .read_streamed(session.header.cwd.as_deref(), id, |event| {
            started |= event.kind == "turn/start";
            if event.kind == "agent-preset/selected" {
                if let Some(value) = event.data.get("agentPreset").and_then(Value::as_str) {
                    selected = value.to_owned();
                }
            }
            Ok(())
        })
        .map_err(|error| {
            internal_error(format!("session {id:?} could not be inspected: {error}"))
        })?;
    if started {
        return Err(RpcError {
            code: "agent-preset/locked".into(),
            message: "This session has already started".into(),
            details: json!({"sessionId": id, "agentPreset": preset}),
        });
    }
    if selected == preset {
        return Ok(json!(preset));
    }
    if context.has_resident_agent(id) {
        return Err(RpcError {
            code: "agent-preset/locked".into(),
            message: "This blank Session already has a resident Agent".into(),
            details: json!({"sessionId": id, "agentPreset": preset}),
        });
    }
    crate::api::append_session_event(
        context,
        id,
        "agent-preset/selected",
        json!({"agentPreset": preset}),
    )?;
    context.emit("agent-preset/selected", vec![json!(id), json!(preset)]);
    Ok(json!(preset))
}

fn list_skills(context: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let id = required_text(payload, "sessionId")?;
    let session = session_metadata(context, id)?;
    let cwd = session
        .header
        .cwd
        .map(PathBuf::from)
        .unwrap_or_else(|| default_workspace_path(&context.home));
    let cwd = std::path::absolute(cwd).map_err(|error| {
        internal_error(format!("skill workspace could not be resolved: {error}"))
    })?;
    let catalog = SkillLoader::new(skill_roots(&cwd, &context.home))
        .discover()
        .map_err(|error| internal_error(format!("skill listing failed: {error}")))?;

    for warning in catalog.warnings() {
        if warning.code == SkillWarningCode::RootReadFailed {
            eprintln!(
                "[dsh-rs] skills: {}: {}",
                warning.path.display(),
                warning.message
            );
        }
    }
    let skills: Vec<_> = catalog
        .skills()
        .iter()
        .filter(|skill| skill.invocation.user_invocable)
        .map(|skill| {
            let mut entry = json!({
                "name": skill.name,
                "path": display_path(&skill.path),
                "description": skill.description,
                "modelInvocable": skill.invocation.model_invocable,
            });
            if let Some(when_to_use) = &skill.when_to_use {
                entry["whenToUse"] = json!(when_to_use);
            }
            entry
        })
        .collect();
    Ok(json!({ "skills": skills }))
}

fn skill_roots(cwd: &Path, dsh_home: &Path) -> Vec<SkillRoot> {
    let project = cwd
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists())
        .unwrap_or(cwd);
    let agents_home = std::env::var_os("DSH_AGENTS_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".agents"));
    let mut roots = vec![
        SkillRoot::new(project.join(".dsh/skills"), "project-dsh", 100),
        SkillRoot::new(project.join(".agents/skills"), "project-agents", 200),
        SkillRoot::new(dsh_home.join("skills"), "user-dsh", 400).skipping_system(),
        SkillRoot::new(agents_home.join("skills"), "user-agents", 500),
    ];
    if let Some(bundled) = std::env::var_os("DSH_BUNDLED_SKILL_DIR").filter(|path| !path.is_empty())
    {
        roots.push(SkillRoot::new(bundled, "bundled", 600));
    }
    roots
}

fn internal_error(message: String) -> RpcError {
    RpcError {
        code: "gateway/internal".into(),
        message,
        details: json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_preset_read_lists_the_tools_this_host_runs_for_it() {
        let standard = read_preset(&json!({"agentPreset": "standard"})).unwrap();
        assert_eq!(standard["name"], "Standard");
        let content = standard["content"].as_str().unwrap();
        assert!(content.contains("read"), "{content}");
        assert!(content.contains("subagent"), "{content}");
        assert!(content.contains("web_search"), "{content}");

        let minimal = read_preset(&json!({"agentPreset": "minimal"})).unwrap();
        let content = minimal["content"].as_str().unwrap();
        let shell = if cfg!(windows) { "pwsh" } else { "bash" };
        assert!(content.contains(shell), "{content}");
        assert!(!content.contains("subagent"), "{content}");
        assert!(!content.contains("read"), "{content}");
    }

    #[test]
    fn an_unknown_preset_read_names_the_available_modes() {
        let error = read_preset(&json!({"agentPreset": "nope"})).unwrap_err();
        assert_eq!(error.code, "agent-preset/not-found");
        assert_eq!(error.details["available"], json!(supported_presets()));
    }
}
