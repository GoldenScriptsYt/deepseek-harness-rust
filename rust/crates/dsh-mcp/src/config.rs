use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub const DEFAULT_PROTOCOL_VERSION: &str = "2025-11-25";
pub const MODERN_PROTOCOL_VERSION: &str = "2026-07-28";
pub const DEFAULT_MAX_FRAME_BYTES: usize = 10 * 1024 * 1024;
pub const DEFAULT_MAX_BUFFERED_OUTPUT_BYTES: usize = 2 * DEFAULT_MAX_FRAME_BYTES;
pub const DEFAULT_MAX_STDERR_BYTES: usize = 1024 * 1024;
pub const DEFAULT_NOTIFICATION_CAPACITY: usize = 256;
pub const DEFAULT_MAX_PENDING_REQUESTS: usize = 128;
pub const DEFAULT_MAX_LIST_PAGES: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Implementation {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<serde_json::Value>>,
}

impl Implementation {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            title: None,
            description: None,
            icons: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VersionNegotiation {
    /// Probe modern discovery, then initialize a fresh child if needed.
    #[default]
    Auto,
    /// Use the initialize/initialized handshake directly.
    Legacy,
}

#[derive(Clone, Debug)]
pub struct StdioServerConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, OsString>,
    pub cwd: Option<PathBuf>,
}

impl StdioServerConfig {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<OsString>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub(crate) fn validate(&self) -> Result<(), crate::McpError> {
        if self.command.trim().is_empty() || self.command.contains('\0') {
            return Err(crate::McpError::configuration(
                "command must be non-empty and contain no NUL",
            ));
        }
        if self.args.iter().any(|arg| arg.contains('\0')) {
            return Err(crate::McpError::configuration(
                "arguments must not contain NUL",
            ));
        }
        for (key, value) in &self.env {
            if key.is_empty() || key.contains(['=', '\0']) {
                return Err(crate::McpError::configuration(
                    "environment variable names must be non-empty and contain neither '=' nor NUL",
                ));
            }
            if value.to_string_lossy().contains('\0') {
                return Err(crate::McpError::configuration(
                    "environment variable values must not contain NUL",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub client_info: Implementation,
    pub startup_timeout: Duration,
    pub request_timeout: Duration,
    pub close_timeout: Duration,
    /// Maximum encoded JSON frame, including its newline delimiter.
    pub max_frame_bytes: usize,
    /// Maximum bytes retained across the frame currently written and queued frames.
    pub max_buffered_output_bytes: usize,
    pub max_stderr_bytes: usize,
    pub notification_capacity: usize,
    pub max_pending_requests: usize,
    pub max_list_pages: usize,
    pub version_negotiation: VersionNegotiation,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            client_info: Implementation::new("dsh-mcp-client", env!("CARGO_PKG_VERSION")),
            startup_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            close_timeout: Duration::from_secs(5),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_buffered_output_bytes: DEFAULT_MAX_BUFFERED_OUTPUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            notification_capacity: DEFAULT_NOTIFICATION_CAPACITY,
            max_pending_requests: DEFAULT_MAX_PENDING_REQUESTS,
            max_list_pages: DEFAULT_MAX_LIST_PAGES,
            version_negotiation: VersionNegotiation::Auto,
        }
    }
}

impl ClientOptions {
    pub fn validate(&self) -> Result<(), crate::McpError> {
        if self.client_info.name.trim().is_empty() || self.client_info.version.trim().is_empty() {
            return Err(crate::McpError::configuration(
                "client name and version must be non-empty",
            ));
        }
        for (name, duration) in [
            ("startup timeout", self.startup_timeout),
            ("request timeout", self.request_timeout),
            ("close timeout", self.close_timeout),
        ] {
            if duration.is_zero() {
                return Err(crate::McpError::configuration(format!(
                    "{name} must be positive"
                )));
            }
            if std::time::Instant::now().checked_add(duration).is_none() {
                return Err(crate::McpError::configuration(format!(
                    "{name} is too large"
                )));
            }
        }
        if self.max_buffered_output_bytes < self.max_frame_bytes {
            return Err(crate::McpError::configuration(
                "maximum buffered output bytes must be at least the maximum frame bytes",
            ));
        }
        if self.max_frame_bytes > u32::MAX as usize
            || self.max_buffered_output_bytes > u32::MAX as usize
        {
            return Err(crate::McpError::configuration(
                "frame and buffered output limits must not exceed 4 GiB",
            ));
        }
        for (name, limit) in [
            ("maximum frame bytes", self.max_frame_bytes),
            (
                "maximum buffered output bytes",
                self.max_buffered_output_bytes,
            ),
            ("maximum stderr bytes", self.max_stderr_bytes),
            ("notification capacity", self.notification_capacity),
            ("maximum pending requests", self.max_pending_requests),
            ("maximum list pages", self.max_list_pages),
        ] {
            if limit == 0 {
                return Err(crate::McpError::configuration(format!(
                    "{name} must be positive"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    /// Overrides the client's default request timeout.
    pub timeout: Option<Duration>,
    /// Included as `params._meta.progressToken` when supported by the protocol era.
    /// Dropping the request future or exceeding its timeout sends `notifications/cancelled`.
    pub progress_token: Option<String>,
}

impl RequestOptions {
    pub fn timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            progress_token: None,
        }
    }
}
