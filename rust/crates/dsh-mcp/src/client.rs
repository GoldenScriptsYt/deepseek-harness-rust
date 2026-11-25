use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::broadcast;

use crate::protocol::{
    legacy_version_supported, parse_discovery, CallToolParams, CallToolResult, InitializeResult,
    ListResourcesPage, ListToolsPage, ReadResourceResult, Resource, ServerCapabilities, Tool,
};
use crate::runtime::{CloseReport, RuntimeHandle, StderrSnapshot};
use crate::{
    ClientOptions, ErrorKind, Implementation, McpError, RequestOptions, ServerNotification,
    StdioServerConfig, VersionNegotiation,
};

pub struct McpClient {
    inner: Arc<RuntimeHandle>,
    notifications: broadcast::Receiver<ServerNotification>,
    protocol_version: String,
    server_info: Implementation,
    capabilities: ServerCapabilities,
    instructions: Option<String>,
}

impl McpClient {
    pub async fn connect(
        config: StdioServerConfig,
        options: ClientOptions,
    ) -> Result<Self, McpError> {
        config.validate()?;
        options.validate()?;
        let options = Arc::new(options);
        if options.version_negotiation == VersionNegotiation::Auto {
            let (probe, notifications) = RuntimeHandle::spawn(
                &config,
                Arc::clone(&options),
                options.notification_capacity,
                crate::runtime::Era::Modern,
            )?;
            let probe_options = RequestOptions {
                timeout: Some(options.startup_timeout),
                progress_token: None,
            };
            let probe_result = async {
                let params = probe.protocol_params(
                    "server/discover",
                    Value::Object(Map::new()),
                    &probe_options,
                )?;
                let result = probe
                    .request("server/discover", params, &probe_options)
                    .await?;
                parse_discovery(result).map_err(|error| error.with_method("server/discover"))
            }
            .await;
            match probe_result {
                Ok(discovery) => {
                    return Ok(Self::from_parts(
                        probe,
                        notifications,
                        discovery.protocol_version,
                        discovery.capabilities,
                        discovery.server_info,
                        discovery.instructions,
                    ));
                }
                Err(error) if can_fall_back(&error) => {
                    reap_probe(&probe, "server/discover").await?;
                }
                Err(error) => return Err(fail_connect(&probe, error, "server/discover").await),
            }
        }
        let (runtime, notifications) = RuntimeHandle::spawn(
            &config,
            Arc::clone(&options),
            options.notification_capacity,
            crate::runtime::Era::Legacy,
        )?;
        match initialize(&runtime, &options).await {
            Ok(initialized) => Ok(Self::from_parts(
                runtime,
                notifications,
                initialized.protocol_version,
                initialized.capabilities,
                initialized.server_info,
                initialized.instructions,
            )),
            Err(error) => Err(fail_connect(&runtime, error, "initialize").await),
        }
    }

    fn from_parts(
        inner: RuntimeHandle,
        notifications: broadcast::Receiver<ServerNotification>,
        protocol_version: String,
        capabilities: ServerCapabilities,
        server_info: Implementation,
        instructions: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            notifications,
            protocol_version,
            server_info,
            capabilities,
            instructions,
        }
    }

    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }

    pub fn server_info(&self) -> &Implementation {
        &self.server_info
    }

    pub fn capabilities(&self) -> &ServerCapabilities {
        &self.capabilities
    }

    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ServerNotification> {
        self.notifications.resubscribe()
    }

    pub fn stderr_snapshot(&self) -> StderrSnapshot {
        self.inner.stderr()
    }

    pub async fn request(
        &self,
        method: &str,
        params: Value,
        options: &RequestOptions,
    ) -> Result<Value, McpError> {
        if method.trim().is_empty() {
            return Err(McpError::configuration("method must be non-empty"));
        }
        let params = self.inner.protocol_params(method, params, options)?;
        self.inner.request(method, params, options).await
    }

    pub async fn ping(&self, options: &RequestOptions) -> Result<(), McpError> {
        self.request("ping", Value::Object(Map::new()), options)
            .await
            .map(|_| ())
    }

    pub async fn list_tools_page(
        &self,
        cursor: Option<&str>,
        options: &RequestOptions,
    ) -> Result<ListToolsPage, McpError> {
        if !self.capabilities.supports_tools() {
            return Ok(ListToolsPage {
                tools: Vec::new(),
                next_cursor: None,
                extra: Map::new(),
            });
        }
        let mut params = Map::new();
        if let Some(cursor) = cursor {
            params.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
        }
        let result = self
            .request("tools/list", Value::Object(params), options)
            .await?;
        serde_json::from_value(result).map_err(|error| {
            McpError::invalid_result(format!("invalid tools/list result: {error}"))
                .with_method("tools/list")
        })
    }

    pub async fn list_tools(&self) -> Result<Vec<Tool>, McpError> {
        self.list_tools_with(&RequestOptions::default()).await
    }

    pub async fn list_tools_with(&self, options: &RequestOptions) -> Result<Vec<Tool>, McpError> {
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.max_list_pages() {
            let page = self.list_tools_page(cursor.as_deref(), options).await?;
            tools.extend(page.tools);
            match page.next_cursor {
                Some(next) => {
                    if !seen.insert(next.clone()) {
                        return Err(McpError::protocol(
                            "tools/list returned a repeated pagination cursor",
                        )
                        .with_method("tools/list"));
                    }
                    cursor = Some(next);
                }
                None => return Ok(tools),
            }
        }
        Err(McpError::protocol("tools/list exceeded the page limit").with_method("tools/list"))
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        options: &RequestOptions,
    ) -> Result<CallToolResult, McpError> {
        if !self.capabilities.supports_tools() {
            return Err(capability_error("tools", "tools/call"));
        }
        if name.trim().is_empty() {
            return Err(
                McpError::configuration("tool name must be non-empty").with_method("tools/call")
            );
        }
        let arguments = arguments.as_object().cloned().ok_or_else(|| {
            McpError::configuration("tool arguments must be a JSON object")
                .with_method("tools/call")
        })?;
        let params = CallToolParams {
            name: name.to_owned(),
            arguments,
            meta: None,
        };
        let params = serde_json::to_value(params).map_err(|error| {
            McpError::new(
                ErrorKind::Serialization,
                format!("failed to serialize tool call: {error}"),
                Some("tools/call"),
                None,
                None,
            )
        })?;
        let result = self.request("tools/call", params, options).await?;
        serde_json::from_value(result).map_err(|error| {
            McpError::invalid_result(format!("invalid tools/call result: {error}"))
                .with_method("tools/call")
        })
    }

    pub async fn list_resources_page(
        &self,
        cursor: Option<&str>,
        options: &RequestOptions,
    ) -> Result<ListResourcesPage, McpError> {
        if !self.capabilities.supports_resources() {
            return Ok(ListResourcesPage {
                resources: Vec::new(),
                next_cursor: None,
                extra: Map::new(),
            });
        }
        let mut params = Map::new();
        if let Some(cursor) = cursor {
            params.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
        }
        let result = self
            .request("resources/list", Value::Object(params), options)
            .await?;
        serde_json::from_value(result).map_err(|error| {
            McpError::invalid_result(format!("invalid resources/list result: {error}"))
                .with_method("resources/list")
        })
    }

    pub async fn list_resources(&self) -> Result<Vec<Resource>, McpError> {
        self.list_resources_with(&RequestOptions::default()).await
    }

    pub async fn list_resources_with(
        &self,
        options: &RequestOptions,
    ) -> Result<Vec<Resource>, McpError> {
        let mut resources = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.max_list_pages() {
            let page = self.list_resources_page(cursor.as_deref(), options).await?;
            resources.extend(page.resources);
            match page.next_cursor {
                Some(next) => {
                    if !seen.insert(next.clone()) {
                        return Err(McpError::protocol(
                            "resources/list returned a repeated pagination cursor",
                        )
                        .with_method("resources/list"));
                    }
                    cursor = Some(next);
                }
                None => return Ok(resources),
            }
        }
        Err(McpError::protocol("resources/list exceeded the page limit")
            .with_method("resources/list"))
    }

    pub async fn read_resource(
        &self,
        uri: &str,
        options: &RequestOptions,
    ) -> Result<ReadResourceResult, McpError> {
        if !self.capabilities.supports_resources() {
            return Err(capability_error("resources", "resources/read"));
        }
        if uri.is_empty() {
            return Err(McpError::configuration("resource URI must be non-empty")
                .with_method("resources/read"));
        }
        let params = serde_json::json!({ "uri": uri });
        let result = self.request("resources/read", params, options).await?;
        serde_json::from_value(result).map_err(|error| {
            McpError::invalid_result(format!("invalid resources/read result: {error}"))
                .with_method("resources/read")
        })
    }

    pub async fn close(self) -> CloseReport {
        self.inner.shutdown().await
    }
}

fn can_fall_back(error: &McpError) -> bool {
    matches!(
        error.kind(),
        ErrorKind::Rpc
            | ErrorKind::Closed
            | ErrorKind::Timeout
            | ErrorKind::InvalidResult
            | ErrorKind::UnsupportedResult
    )
}

async fn reap_probe(runtime: &RuntimeHandle, method: &str) -> Result<(), McpError> {
    let report = runtime.shutdown().await;
    if report.exit_status.is_some() && report.cleanup_error.is_none() {
        return Ok(());
    }
    Err(McpError::new(
        ErrorKind::Closed,
        format!(
            "failed to reap MCP probe (forced={}, timed_out={}): {}",
            report.forced,
            report.timed_out,
            report
                .cleanup_error
                .as_deref()
                .unwrap_or("child exit unknown")
        ),
        Some(method),
        None,
        None,
    ))
}

async fn fail_connect(runtime: &RuntimeHandle, error: McpError, operation: &str) -> McpError {
    match reap_probe(runtime, operation).await {
        Ok(()) => error,
        Err(cleanup) => cleanup,
    }
}

async fn initialize(
    runtime: &RuntimeHandle,
    options: &ClientOptions,
) -> Result<InitializeResult, McpError> {
    let params = serde_json::json!({
        "protocolVersion": crate::DEFAULT_PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": options.client_info,
    });
    let result = runtime
        .request(
            "initialize",
            params,
            &RequestOptions {
                timeout: Some(options.startup_timeout),
                progress_token: None,
            },
        )
        .await?;
    let initialized: InitializeResult = serde_json::from_value(result).map_err(|error| {
        McpError::invalid_result(format!("invalid initialize result: {error}"))
            .with_method("initialize")
    })?;
    if !legacy_version_supported(&initialized.protocol_version) {
        return Err(McpError::protocol(format!(
            "server selected unsupported protocol version {}",
            initialized.protocol_version
        ))
        .with_method("initialize"));
    }
    runtime.notify_initialized(options.startup_timeout).await?;
    Ok(initialized)
}

fn capability_error(capability: &str, method: &str) -> McpError {
    McpError::new(
        ErrorKind::CapabilityNotSupported,
        format!("server does not advertise {capability}"),
        Some(method),
        None,
        None,
    )
}
