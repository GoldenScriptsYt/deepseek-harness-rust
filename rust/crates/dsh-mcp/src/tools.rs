//! MCP tool bridge: server-qualified names and the model-visible result text.
//!
//! Port of `packages/mcp/mcp-client/src/tools.ts` for eager loading over stdio.
//! An image, audio, or embedded-resource block keeps the TypeScript diagnostics
//! for a block the Native projection cannot admit: this Host has no attachment
//! store on the tool-call path, so only text reaches the model.

use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

use crate::{McpClient, McpError, RequestOptions};

/// DeepSeek function-name contract: at most 64 characters.
pub const MAX_PUBLIC_NAME_LENGTH: usize = 64;
/// Hex characters of the identity hash appended after lossy normalization.
const HASH_LENGTH: usize = 12;

/// Model-facing name of one MCP tool: `mcp__<server>__<raw>` when that fits the
/// function-name contract, otherwise the normalized, truncated form plus a
/// 12-hex-character SHA-256 identity suffix.
///
/// @param server_name - Configured server namespace.
/// @param raw_name - The server's own tool name, used verbatim on the wire.
/// @returns The stable public name.
pub fn public_tool_name(server_name: &str, raw_name: &str) -> String {
    let joined = format!("mcp__{server_name}__{raw_name}");
    let normalized: String = joined
        .chars()
        .map(|character| match character {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-' => character,
            _ => '_',
        })
        .collect();
    if normalized == joined && normalized.chars().count() <= MAX_PUBLIC_NAME_LENGTH {
        return normalized;
    }
    let digest = Sha256::digest(format!("{server_name}\u{0}{raw_name}").as_bytes());
    let identity = hex_prefix(&digest, HASH_LENGTH);
    let keep = MAX_PUBLIC_NAME_LENGTH - HASH_LENGTH - 1;
    let mut public = String::with_capacity(MAX_PUBLIC_NAME_LENGTH);
    public.extend(normalized.chars().take(keep));
    public.push('_');
    public.push_str(&identity);
    public
}

fn hex_prefix(bytes: &[u8], length: usize) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex.truncate(length);
    hex
}

/// Text one MCP content array contributes to the model, blocks joined by newlines.
///
/// @param content - The result's `content` array in protocol order.
/// @param tool_name - The server's raw tool name, named when nothing is visible.
/// @returns The model-visible text.
pub fn extract_text(content: &[Value], tool_name: &str) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(content.len());
    for block in content {
        let Some(object) = block.as_object() else {
            parts.push("[unsupported MCP content block: expected an object]".to_owned());
            continue;
        };
        let kind = object.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "text" => {
                if let Some(text) = object.get("text").and_then(Value::as_str) {
                    parts.push(text.to_owned());
                }
            }
            "image" => parts.push(format!(
                "[image unavailable: {}; this result was not admitted to durable model context; raw image data remains available to programmatic callers]",
                media_type(object),
            )),
            "resource_link" => {
                match (
                    object.get("name").and_then(Value::as_str),
                    object.get("uri").and_then(Value::as_str),
                ) {
                    (Some(name), Some(uri)) => parts.push(format!("Resource link: {name} ({uri})")),
                    _ => parts.push(
                        "[resource link unavailable: the MCP block is missing its name or URI]"
                            .to_owned(),
                    ),
                }
            }
            "audio" => parts.push(format!(
                "[audio result unsupported: {}; raw audio data remains available to programmatic callers]",
                media_type(object),
            )),
            "resource" => parts.push(
                "[embedded resource unsupported; raw resource data remains available to programmatic callers]"
                    .to_owned(),
            ),
            other => parts.push(format!(
                "[unsupported MCP content type: {}]",
                if other.is_empty() { "unknown" } else { other }
            )),
        }
    }
    if parts.is_empty() {
        return format!("({tool_name} returned no model-visible content)");
    }
    parts.join("\n")
}

fn media_type(block: &Map<String, Value>) -> &str {
    block
        .get("mimeType")
        .and_then(Value::as_str)
        .unwrap_or("unknown media type")
}

/// One MCP tool registered as a native tool under its public name.
pub struct McpTool {
    client: Arc<McpClient>,
    server_name: String,
    raw_name: String,
    public_name: String,
    description: String,
    input_schema: Value,
    timeout: Duration,
}

impl McpTool {
    /// Build the bridge for one listed tool.
    ///
    /// @param client - Connected server client.
    /// @param server_name - Configured server namespace.
    /// @param tool - The listed tool definition.
    /// @param timeout - Per-call budget for `tools/call`.
    /// @returns The bridge.
    pub fn new(
        client: Arc<McpClient>,
        server_name: impl Into<String>,
        tool: &crate::Tool,
        timeout: Duration,
    ) -> Self {
        let server_name = server_name.into();
        McpTool {
            client,
            public_name: public_tool_name(&server_name, &tool.name),
            raw_name: tool.name.clone(),
            description: tool.description.clone().unwrap_or_default(),
            input_schema: tool.input_schema.clone(),
            server_name,
            timeout,
        }
    }

    /// Public, model-facing name.
    pub fn public_name(&self) -> &str {
        &self.public_name
    }

    /// Configured server namespace.
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// The server's own tool name.
    pub fn raw_name(&self) -> &str {
        &self.raw_name
    }
}

impl Tool for McpTool {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: self.public_name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
        }
    }

    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let arguments = match args {
                Value::Object(map) => Value::Object(map),
                Value::Null => Value::Object(Map::new()),
                other => {
                    return ToolOutput::err(format!(
                        "MCP tool \"{}\" arguments must be a JSON object, got {}",
                        self.raw_name,
                        json_type(&other),
                    ))
                }
            };
            let options = RequestOptions::timeout(self.timeout);
            let call = self.client.call_tool(&self.raw_name, arguments, &options);
            let result = tokio::select! {
                result = call => result,
                () = cx.cancel.cancelled() => Err(McpError::cancelled("tools/call")),
            };
            match result {
                Ok(result) if result.is_error => {
                    ToolOutput::err(extract_text(&result.content, &self.raw_name))
                }
                Ok(result) => ToolOutput::ok(extract_text(&result.content, &self.raw_name)),
                Err(error) => ToolOutput::err(format!(
                    "MCP tool \"{}\" failed: {}",
                    self.raw_name,
                    describe(&error),
                )),
            }
        })
    }
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Failure text for one request: the JSON-RPC error when the server answered,
/// otherwise the classified kind and message.
///
/// @param error - The failed request.
/// @returns One line of model-visible text.
fn describe(error: &McpError) -> String {
    match error.rpc_error() {
        Some(rpc) => format!("JSON-RPC error {}: {}", rpc.code, rpc.message),
        None => format!("{}: {}", error.code(), error.message()),
    }
}
