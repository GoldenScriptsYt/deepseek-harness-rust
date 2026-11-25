use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{Implementation, MODERN_PROTOCOL_VERSION};

pub const SUPPORTED_LEGACY_VERSIONS: [&str; 5] = [
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
    "2024-10-07",
];

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ServerCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completions: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ServerCapabilities {
    pub fn supports_tools(&self) -> bool {
        self.tools.is_some()
    }

    pub fn supports_resources(&self) -> bool {
        self.resources.is_some()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ToolCapability {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ResourceCapability {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PromptCapability {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Tool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    #[serde(
        default,
        rename = "outputSchema",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Value>>,
    #[serde(default, rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CallToolParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub arguments: Map<String, Value>,
    #[serde(default, rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CallToolResult {
    pub content: Vec<Value>,
    #[serde(
        default,
        rename = "structuredContent",
        skip_serializing_if = "Option::is_none"
    )]
    pub structured_content: Option<Value>,
    #[serde(
        default,
        rename = "isError",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub is_error: bool,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Resource {
    pub name: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Value>>,
    #[serde(default, rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceContents {
    pub uri: String,
    #[serde(default, rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadResourceResult {
    pub contents: Vec<ResourceContents>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListToolsPage {
    pub tools: Vec<Tool>,
    #[serde(
        default,
        rename = "nextCursor",
        skip_serializing_if = "Option::is_none"
    )]
    pub next_cursor: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListResourcesPage {
    pub resources: Vec<Resource>,
    #[serde(
        default,
        rename = "nextCursor",
        skip_serializing_if = "Option::is_none"
    )]
    pub next_cursor: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerNotification {
    pub method: String,
    pub params: Option<Value>,
}

impl ServerNotification {
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            method: method.into(),
            params,
        }
    }

    pub fn progress_token(&self) -> Option<&Value> {
        if self.method != "notifications/progress" {
            return None;
        }
        self.params.as_ref()?.get("progressToken")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    pub server_info: Implementation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct DiscoveryResult {
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    pub server_info: Implementation,
    pub instructions: Option<String>,
}

pub(crate) fn ensure_modern_result(
    method: &str,
    mut result: Value,
) -> Result<Value, crate::McpError> {
    let object = result.as_object_mut().ok_or_else(|| {
        crate::McpError::invalid_result(format!("{method} result must be an object"))
    })?;
    match object.remove("resultType") {
        Some(Value::String(kind)) if kind == "complete" => Ok(result),
        Some(Value::String(kind)) => Err(crate::McpError::new(
            crate::ErrorKind::UnsupportedResult,
            format!("{method} returned unsupported result type {kind}"),
            Some(method),
            None,
            None,
        )),
        _ => Err(crate::McpError::invalid_result(format!(
            "{method} modern result is missing resultType"
        ))),
    }
}

pub(crate) fn parse_discovery(result: Value) -> Result<DiscoveryResult, crate::McpError> {
    let object = result.as_object().ok_or_else(|| {
        crate::McpError::invalid_result("server/discover result must be an object")
    })?;
    let supported_versions = object
        .get("supportedVersions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            crate::McpError::invalid_result("server/discover supportedVersions must be an array")
        })?;
    let version = supported_versions
        .iter()
        .filter_map(Value::as_str)
        .find(|version| *version == MODERN_PROTOCOL_VERSION)
        .ok_or_else(|| crate::McpError::invalid_result("server/discover did not offer modern MCP"))?
        .to_owned();
    let capabilities =
        serde_json::from_value(object.get("capabilities").cloned().ok_or_else(|| {
            crate::McpError::invalid_result("server/discover capabilities are missing")
        })?)
        .map_err(|error| {
            crate::McpError::invalid_result(format!("invalid server capabilities: {error}"))
        })?;
    let server_info = object
        .get("_meta")
        .and_then(|meta| meta.get("io.modelcontextprotocol/serverInfo"))
        .cloned()
        .unwrap_or_else(|| {
            serde_json::to_value(Implementation::new("unknown", "unknown")).unwrap()
        });
    let server_info = serde_json::from_value(server_info).map_err(|error| {
        crate::McpError::invalid_result(format!("invalid server info: {error}"))
    })?;
    let instructions = match object.get("instructions") {
        None => None,
        Some(Value::String(instructions)) => Some(instructions.clone()),
        Some(_) => {
            return Err(crate::McpError::invalid_result(
                "server/discover instructions must be a string",
            ));
        }
    };
    Ok(DiscoveryResult {
        protocol_version: version,
        capabilities,
        server_info,
        instructions,
    })
}

pub(crate) fn legacy_version_supported(version: &str) -> bool {
    SUPPORTED_LEGACY_VERSIONS.contains(&version)
}
