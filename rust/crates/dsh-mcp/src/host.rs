//! Host-side MCP server configuration, connection, and tool registration.
//!
//! One entry configures one stdio server. The TypeScript Host composes those
//! entries from `cordis.yml` plugin configs; this Host reads the same fields
//! from a JSON array in the file named by `DSH_MCP_SERVERS`, or inline when
//! the variable's value starts with `[`.

use dsh_core::{Tool, ToolRegistry};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::tools::McpTool;
use crate::{ClientOptions, McpClient, RequestOptions, StdioServerConfig};

/// Default per-call budget, matching the TypeScript `toolCallTimeoutMs`.
pub const DEFAULT_TOOL_CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// Default budget for transport closure and child reaping.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Environment variable naming the server configuration.
pub const CONFIG_ENV: &str = "DSH_MCP_SERVERS";
/// Longest accepted `serverName`, from the TypeScript configuration schema.
const MAX_SERVER_NAME_LENGTH: usize = 32;

/// One configured MCP server, using the TypeScript `Config` field names.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerSpec {
    /// Namespace for the server's public tool names.
    pub server_name: String,
    /// Executable to spawn for the stdio transport.
    #[serde(default)]
    pub command: Option<String>,
    /// Arguments passed to `command`.
    #[serde(default)]
    pub args: Vec<String>,
    /// Extra environment merged over the child's scrubbed environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Working directory for the child.
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    /// Per-call budget for `tools/call` and resource requests.
    #[serde(default)]
    pub tool_call_timeout_ms: Option<u64>,
    /// Budget for negotiation, connection, and the initial tool sync.
    #[serde(default)]
    pub startup_timeout_ms: Option<u64>,
    /// Budget for transport closure and pending local work.
    #[serde(default)]
    pub close_timeout_ms: Option<u64>,
    /// Reject Host startup when this server fails instead of starting without it.
    #[serde(default)]
    pub fail_on_startup_error: bool,
}

impl McpServerSpec {
    /// Validate the fields this Host reads.
    ///
    /// @returns Why the entry is unusable, or `Ok(())`.
    pub fn validate(&self) -> Result<(), String> {
        let name = &self.server_name;
        if name.is_empty() || name.chars().count() > MAX_SERVER_NAME_LENGTH {
            return Err(format!(
                "serverName \"{name}\" must be 1 to {MAX_SERVER_NAME_LENGTH} characters"
            ));
        }
        if !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        {
            return Err(format!(
                "serverName \"{name}\" may contain only letters, digits, and _ or -"
            ));
        }
        match self.command.as_deref() {
            Some(command) if !command.trim().is_empty() => Ok(()),
            _ => Err(format!(
                "server \"{name}\" needs a non-empty command for the stdio transport"
            )),
        }
    }

    fn tool_call_timeout(&self) -> Duration {
        self.tool_call_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_TOOL_CALL_TIMEOUT)
    }
}

/// Connected MCP servers and the tools they contributed.
pub struct McpHost {
    tools: Vec<Arc<dyn Tool>>,
    instructions: Vec<(String, String)>,
    failures: Vec<String>,
}

impl McpHost {
    /// Connect every configured server and list its tools.
    ///
    /// A server that fails to connect or list contributes no tools; its
    /// failure is reported by [`McpHost::failures`] unless it sets
    /// `failOnStartupError`, which turns the failure into an error.
    ///
    /// @param specs - Validated server entries.
    /// @returns The connected host.
    pub async fn connect(specs: Vec<McpServerSpec>) -> Result<Self, String> {
        let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
        let mut instructions: Vec<(String, String)> = Vec::new();
        let mut failures: Vec<String> = Vec::new();
        let mut seen: BTreeMap<String, ()> = BTreeMap::new();
        for spec in specs {
            spec.validate()?;
            if seen.insert(spec.server_name.clone(), ()).is_some() {
                return Err(format!(
                    "serverName \"{}\" is configured more than once",
                    spec.server_name
                ));
            }
            let name = spec.server_name.clone();
            let mut config = StdioServerConfig::new(spec.command.clone().unwrap_or_default());
            if !spec.args.is_empty() {
                config = config.args(spec.args.clone());
            }
            for (key, value) in &spec.env {
                config = config.env(key.clone(), value.clone());
            }
            if let Some(cwd) = &spec.cwd {
                config = config.cwd(cwd.clone());
            }
            let mut options = ClientOptions::default();
            if let Some(startup) = spec.startup_timeout_ms {
                options.startup_timeout = Duration::from_millis(startup);
            }
            if let Some(close) = spec.close_timeout_ms {
                options.close_timeout = Duration::from_millis(close);
            }
            options.request_timeout = spec.tool_call_timeout();
            let client = match McpClient::connect(config, options).await {
                Ok(client) => Arc::new(client),
                Err(error) => {
                    let failure = format!("mcp-client({name}): connection failed: {error}");
                    if spec.fail_on_startup_error {
                        return Err(failure);
                    }
                    failures.push(failure);
                    continue;
                }
            };
            let listed = match client
                .list_tools_with(&RequestOptions::timeout(spec.tool_call_timeout()))
                .await
            {
                Ok(listed) => listed,
                Err(error) => {
                    let failure = format!("mcp-client({name}): tools/list failed: {error}");
                    if spec.fail_on_startup_error {
                        return Err(failure);
                    }
                    failures.push(failure);
                    continue;
                }
            };
            if let Some(text) = client.instructions() {
                if !text.trim().is_empty() {
                    instructions.push((name.clone(), text.to_owned()));
                }
            }
            let timeout = spec.tool_call_timeout();
            let mut names: BTreeMap<String, ()> = BTreeMap::new();
            for listed_tool in &listed {
                let tool = McpTool::new(Arc::clone(&client), name.clone(), listed_tool, timeout);
                let public = tool.public_name().to_owned();
                if names.insert(public.clone(), ()).is_some() {
                    failures.push(format!(
                        "mcp-client({name}): server listed tool \"{}\" more than once",
                        listed_tool.name
                    ));
                    continue;
                }
                tools.push(Arc::new(tool));
            }
        }
        Ok(McpHost {
            tools,
            instructions,
            failures,
        })
    }

    /// Server instructions as system-prompt sections.
    ///
    /// Each connected server with nonblank instructions contributes one
    /// `### MCP server: <name>` section, in configuration order, matching the
    /// TypeScript Host's attribution.
    ///
    /// @returns The sections, or an empty string when no server sent instructions.
    pub fn prompt_sections(&self) -> String {
        self.instructions
            .iter()
            .map(|(name, text)| format!("### MCP server: {name}\n\n{text}"))
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    /// Tools contributed by every connected server.
    pub fn tools(&self) -> &[Arc<dyn Tool>] {
        &self.tools
    }

    /// Startup failures of servers that contributed no tools.
    pub fn failures(&self) -> &[String] {
        &self.failures
    }

    /// Add every contributed tool to a registry.
    ///
    /// @param registry - Registry the Agent will use.
    pub fn register(&self, registry: &mut ToolRegistry) {
        for tool in &self.tools {
            registry.register_shared(Arc::clone(tool));
        }
    }
}

/// Parse a JSON array of server entries.
///
/// @param json - Configuration text.
/// @returns The entries, or why the text is unusable.
pub fn parse_specs(json: &str) -> Result<Vec<McpServerSpec>, String> {
    let value: Value =
        serde_json::from_str(json).map_err(|error| format!("invalid MCP server JSON: {error}"))?;
    let entries = match value {
        Value::Array(entries) => entries,
        other => {
            return Err(format!(
                "MCP server configuration must be a JSON array, got {}",
                match other {
                    Value::Object(_) => "an object",
                    Value::String(_) => "a string",
                    _ => "a scalar",
                }
            ))
        }
    };
    entries
        .into_iter()
        .map(|entry| {
            serde_json::from_value(entry)
                .map_err(|error| format!("invalid MCP server entry: {error}"))
        })
        .collect()
}

/// Read server entries from a configuration file.
///
/// @param path - JSON file holding an array of entries.
/// @returns The entries, or why the file is unusable.
pub fn load_specs(path: &Path) -> Result<Vec<McpServerSpec>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    parse_specs(&text)
}

/// Server entries named by `DSH_MCP_SERVERS`, if any.
///
/// The value is a JSON file path, or inline JSON when it starts with `[`.
/// @returns The configured entries, or a readable configuration failure.
pub fn specs_from_env() -> Result<Option<Vec<McpServerSpec>>, String> {
    let Some(value) = std::env::var(CONFIG_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    if value.trim_start().starts_with('[') {
        return parse_specs(&value).map(Some);
    }
    load_specs(Path::new(&value)).map(Some)
}
