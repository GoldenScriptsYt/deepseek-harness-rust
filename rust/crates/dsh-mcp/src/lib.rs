//! Provider-neutral MCP JSON-RPC client foundation.
//!
//! The supported transport is newline-delimited JSON-RPC 2.0 over a directly
//! spawned child process's stdio. Streamable HTTP and SSE are not implemented.
//! Call [`McpClient::close`] to await child reaping and cleanup.

mod client;
mod config;
mod error;
mod host;
mod protocol;
mod runtime;
pub mod tools;

pub use client::McpClient;
pub use config::{
    ClientOptions, Implementation, RequestOptions, StdioServerConfig, VersionNegotiation,
    DEFAULT_MAX_BUFFERED_OUTPUT_BYTES, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_LIST_PAGES,
    DEFAULT_MAX_PENDING_REQUESTS, DEFAULT_MAX_STDERR_BYTES, DEFAULT_NOTIFICATION_CAPACITY,
    DEFAULT_PROTOCOL_VERSION, MODERN_PROTOCOL_VERSION,
};
pub use error::{ErrorKind, McpError, RpcError};
pub use host::{
    load_specs, parse_specs, specs_from_env, McpHost, McpServerSpec, CONFIG_ENV,
    DEFAULT_CLOSE_TIMEOUT, DEFAULT_TOOL_CALL_TIMEOUT,
};
pub use protocol::{
    CallToolParams, CallToolResult, ListResourcesPage, ListToolsPage, PromptCapability,
    ReadResourceResult, Resource, ResourceCapability, ResourceContents, ServerCapabilities,
    ServerNotification, Tool, ToolCapability, SUPPORTED_LEGACY_VERSIONS,
};
pub use runtime::{CloseReport, StderrSnapshot};
pub use tools::{extract_text, public_tool_name, McpTool};
