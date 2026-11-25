use std::fmt;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Configuration,
    Io,
    Protocol,
    Timeout,
    Cancelled,
    Closed,
    Rpc,
    CapabilityNotSupported,
    Overloaded,
    InvalidResult,
    UnsupportedResult,
    Serialization,
}

impl ErrorKind {
    pub fn code(self) -> &'static str {
        match self {
            Self::Configuration => "MCP_CONFIGURATION",
            Self::Io => "MCP_IO",
            Self::Protocol => "MCP_PROTOCOL",
            Self::Timeout => "MCP_TIMEOUT",
            Self::Cancelled => "MCP_CANCELLED",
            Self::Closed => "MCP_CONNECTION_CLOSED",
            Self::Rpc => "MCP_REQUEST_FAILED",
            Self::CapabilityNotSupported => "MCP_CAPABILITY_NOT_SUPPORTED",
            Self::Overloaded => "MCP_OVERLOADED",
            Self::InvalidResult => "MCP_INVALID_RESULT",
            Self::UnsupportedResult => "MCP_UNSUPPORTED_RESULT",
            Self::Serialization => "MCP_SERIALIZATION",
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl fmt::Display for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "JSON-RPC error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

#[derive(Clone, Debug)]
pub struct McpError {
    kind: ErrorKind,
    message: Box<str>,
    method: Option<Box<str>>,
    rpc: Option<Box<RpcError>>,
    source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl McpError {
    pub(crate) fn new(
        kind: ErrorKind,
        message: impl Into<String>,
        method: Option<&str>,
        rpc: Option<RpcError>,
        source: Option<Arc<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            message: message.into().into(),
            method: method.map(|method| method.into()),
            rpc: rpc.map(Box::new),
            source,
        }
    }

    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Configuration, message, None, None, None)
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Protocol, message, None, None, None)
    }

    pub(crate) fn invalid_result(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidResult, message, None, None, None)
    }

    pub(crate) fn closed(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Closed, message, None, None, None)
    }

    pub(crate) fn io(message: impl Into<String>, error: std::io::Error) -> Self {
        let source = Arc::new(error);
        Self::new(ErrorKind::Io, message, None, None, Some(source))
    }

    pub(crate) fn rpc(error: RpcError, method: Option<&str>) -> Self {
        let message = error.to_string();
        Self::new(ErrorKind::Rpc, message, method, Some(error), None)
    }

    pub(crate) fn timeout(method: &str) -> Self {
        Self::new(
            ErrorKind::Timeout,
            format!("request {method} timed out"),
            Some(method),
            None,
            None,
        )
    }

    pub(crate) fn cancelled(method: &str) -> Self {
        Self::new(
            ErrorKind::Cancelled,
            format!("request {method} was cancelled"),
            Some(method),
            None,
            None,
        )
    }

    pub(crate) fn write_timeout(method: &str) -> Self {
        Self::new(
            ErrorKind::Timeout,
            format!("timed out writing request {method}"),
            Some(method),
            None,
            None,
        )
    }

    pub(crate) fn with_method(mut self, method: &str) -> Self {
        if self.method.is_none() {
            self.method = Some(method.into());
        }
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn code(&self) -> &'static str {
        self.kind.code()
    }

    pub fn method(&self) -> Option<&str> {
        self.method.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn rpc_error(&self) -> Option<&RpcError> {
        self.rpc.as_deref()
    }
}

impl fmt::Display for McpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for McpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
    }
}
