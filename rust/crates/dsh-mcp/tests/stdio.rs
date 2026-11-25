use std::path::{Path, PathBuf};
use std::time::Duration;

use dsh_mcp::{
    CallToolResult, ClientOptions, ErrorKind, McpClient, RequestOptions, StdioServerConfig,
    VersionNegotiation,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

struct Fixture {
    client: McpClient,
    marker: PathBuf,
    _directory: TempDir,
}

impl Fixture {
    async fn close(self) -> dsh_mcp::CloseReport {
        self.client.close().await
    }

    async fn close_with_events(self) -> (dsh_mcp::CloseReport, Vec<String>) {
        let Self {
            client,
            marker,
            _directory,
        } = self;
        let report = client.close().await;
        let events = std::fs::read_to_string(marker)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect();
        (report, events)
    }
}

fn server_config(scenario: &str, marker: &Path) -> StdioServerConfig {
    StdioServerConfig::new(env!("CARGO_BIN_EXE_fake-mcp-server"))
        .env("DSH_FAKE_MCP_SCENARIO", scenario)
        .env("DSH_FAKE_MCP_MARKER", marker)
}

async fn fixture(
    scenario: &str,
    configure: impl FnOnce(&mut ClientOptions),
) -> Result<Fixture, dsh_mcp::McpError> {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("events.txt");
    let mut options = ClientOptions {
        startup_timeout: Duration::from_secs(3),
        request_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_secs(1),
        ..ClientOptions::default()
    };
    configure(&mut options);
    let client = McpClient::connect(server_config(scenario, &marker), options).await?;
    Ok(Fixture {
        client,
        marker,
        _directory: directory,
    })
}

fn request_options() -> RequestOptions {
    RequestOptions {
        timeout: Some(Duration::from_secs(2)),
        progress_token: None,
    }
}

fn text(result: &CallToolResult) -> Option<&str> {
    result.content[0].get("text").and_then(Value::as_str)
}

#[tokio::test]
async fn modern_handshake_listing_calling_resources_and_notifications() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("modern", |_| {}).await.unwrap();
        assert_eq!(fixture.client.protocol_version(), "2026-07-28");
        assert_eq!(fixture.client.server_info().name, "fake-modern");
        assert_eq!(fixture.client.instructions(), Some("deterministic fixture"));
        let mut notifications = fixture.client.subscribe();
        let tools = fixture.client.list_tools().await.unwrap();
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["echo", "add", "fail"]
        );
        assert!(tools[0].output_schema.is_some());
        let notification = notifications.recv().await.unwrap();
        assert_eq!(notification.method, "notifications/tools/list_changed");
        let result = fixture
            .client
            .call_tool("echo", json!({"message":"hello"}), &request_options())
            .await
            .unwrap();
        assert_eq!(text(&result), Some("echo"));
        assert_eq!(
            result.structured_content,
            Some(json!({"arguments":{"message":"hello"}}))
        );
        let resources = fixture.client.list_resources().await.unwrap();
        assert_eq!(resources[0].uri, "file:///test.txt");
        let contents = fixture
            .client
            .read_resource("file:///test.txt", &request_options())
            .await
            .unwrap();
        assert_eq!(
            contents.contents[0].text.as_deref(),
            Some("fixture contents")
        );
        fixture.client.ping(&request_options()).await.unwrap();
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert!(!report.forced);
        assert_eq!(events, ["start", "eof"]);
    })
    .await
    .expect("modern integration test timed out");
}

#[tokio::test]
async fn auto_negotiation_falls_back_to_fresh_legacy_child() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("legacy", |_| {}).await.unwrap();
        assert_eq!(fixture.client.protocol_version(), "2025-11-25");
        assert_eq!(fixture.client.server_info().name, "fake-legacy");
        assert_eq!(fixture.client.instructions(), Some("legacy fixture"));
        assert_eq!(fixture.client.list_tools().await.unwrap().len(), 3);
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert_eq!(events, ["start", "eof", "start", "initialized", "eof"]);
    })
    .await
    .expect("legacy integration test timed out");
}

#[tokio::test]
async fn explicit_legacy_mode_skips_discovery_probe() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("legacy", |options| {
            options.version_negotiation = VersionNegotiation::Legacy;
        })
        .await
        .unwrap();
        assert_eq!(fixture.client.protocol_version(), "2025-11-25");
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit());
        assert_eq!(events, ["start", "initialized", "eof"]);
    })
    .await
    .expect("explicit legacy test timed out");
}

#[tokio::test]
async fn rpc_errors_preserve_code_message_and_data() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("modern", |_| {}).await.unwrap();
        let error = fixture
            .client
            .call_tool("fail", json!({}), &request_options())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Rpc);
        assert_eq!(error.method(), Some("tools/call"));
        let rpc = error.rpc_error().unwrap();
        assert_eq!(rpc.code, -32602);
        assert_eq!(rpc.message, "invalid tool arguments");
        assert_eq!(rpc.data, Some(json!({"fixture":true})));
        let tool_error = fixture
            .client
            .call_tool("tool-error", json!({}), &request_options())
            .await
            .unwrap();
        assert!(tool_error.is_error);
        assert_eq!(text(&tool_error), Some("tool failed"));
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("error propagation test timed out");
}

#[tokio::test]
async fn malformed_stdout_fails_pending_request_and_reaps_child() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("malformed", |_| {}).await.unwrap();
        let error = fixture.client.list_tools().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(error.message().contains("malformed stdout JSON"));
        let report = fixture.close().await;
        assert!(report.exit_status.is_some(), "{report:?}");
        assert!(report.cleanup_error.is_none(), "{report:?}");
    })
    .await
    .expect("malformed output test timed out");
}

#[tokio::test]
async fn oversized_stdout_frame_is_bounded_and_rejected() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("oversized", |options| options.max_frame_bytes = 1024)
            .await
            .unwrap();
        let error = fixture.client.list_tools().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(error.message().contains("exceeds 1024 bytes"));
        let report = fixture.close().await;
        assert!(report.exit_status.is_some(), "{report:?}");
    })
    .await
    .expect("oversized frame test timed out");
}

#[tokio::test]
async fn timeout_sends_cancellation_and_late_response_is_ignored() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("cancel", |_| {}).await.unwrap();
        let mut notifications = fixture.client.subscribe();
        let error = fixture
            .client
            .call_tool(
                "slow",
                json!({}),
                &RequestOptions {
                    timeout: Some(Duration::from_millis(50)),
                    progress_token: Some("fixture-token".to_owned()),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout);
        let progress = notifications.recv().await.unwrap();
        assert_eq!(progress.method, "notifications/progress");
        assert_eq!(progress.progress_token(), Some(&json!("fixture-token")));
        let cancelled = notifications.recv().await.unwrap();
        assert_eq!(cancelled.method, "notifications/fixture/cancelled");
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("cancel:"))
                .count(),
            1,
            "{events:?}"
        );
    })
    .await
    .expect("cancellation test timed out");
}

#[tokio::test]
async fn stderr_is_drained_without_blocking_and_retention_is_bounded() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("stderr", |options| options.max_stderr_bytes = 64)
            .await
            .unwrap();
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        let snapshot = fixture.client.stderr_snapshot();
        assert_eq!(snapshot.text.len(), 64);
        assert!(snapshot.truncated);
        let report = fixture.close().await;
        assert!(report.clean_exit(), "{report:?}");
        assert_eq!(report.stderr.text.len(), 64);
        assert!(report.stderr.truncated);
    })
    .await
    .expect("stderr test timed out");
}

#[tokio::test]
async fn close_forces_stubborn_child_and_awaits_exit() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("stubborn", |options| {
            options.close_timeout = Duration::from_millis(50);
        })
        .await
        .unwrap();
        let report = fixture.close().await;
        assert!(report.forced, "{report:?}");
        assert!(report.exit_status.is_some(), "{report:?}");
        assert!(report.cleanup_error.is_none(), "{report:?}");
    })
    .await
    .expect("forced shutdown test timed out");
}

#[tokio::test]
async fn unsupported_negotiated_version_is_rejected() {
    timeout(TEST_TIMEOUT, async {
        let error = fixture("bad-version", |_| {}).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(error.message().contains("2099-01-01"));
    })
    .await
    .expect("bad version test timed out");
}

#[tokio::test]
async fn unadvertised_capabilities_are_gated() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("no-capabilities", |_| {}).await.unwrap();
        assert!(fixture.client.list_tools().await.unwrap().is_empty());
        assert!(fixture.client.list_resources().await.unwrap().is_empty());
        let error = fixture
            .client
            .call_tool("echo", json!({}), &request_options())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::CapabilityNotSupported);
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("capability test timed out");
}

#[tokio::test]
async fn repeated_pagination_cursor_is_rejected() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("repeat-cursor", |options| options.max_list_pages = 2)
            .await
            .unwrap();
        let error = fixture.client.list_tools().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(error.message().contains("repeated pagination cursor"));
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("pagination test timed out");
}

#[tokio::test]
async fn modern_input_required_result_is_structured_unsupported_result() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("input-required", |_| {}).await.unwrap();
        let error = fixture
            .client
            .call_tool("input-required", json!({}), &request_options())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedResult);
        assert!(error.message().contains("input_required"));
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("input required test timed out");
}

#[tokio::test]
async fn concurrent_requests_route_out_of_order_and_server_ping_is_answered() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("server-ping", |_| {}).await.unwrap();
        let mut notifications = fixture.client.subscribe();
        let options = request_options();
        let (held, released) = tokio::join!(
            fixture.client.call_tool("hold", json!({}), &options),
            fixture.client.call_tool("release", json!({}), &options),
        );
        assert_eq!(text(&held.unwrap()), Some("held"));
        assert_eq!(text(&released.unwrap()), Some("released"));
        assert!(fixture.client.ping(&options).await.is_ok());
        assert_eq!(
            notifications.recv().await.unwrap().method,
            "notifications/fixture/server-ping"
        );
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("concurrency test timed out");
}

#[tokio::test]
async fn dropping_request_future_sends_one_cancellation() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("cancel", |_| {}).await.unwrap();
        let mut notifications = fixture.client.subscribe();
        let options = RequestOptions {
            timeout: Some(Duration::from_secs(2)),
            progress_token: Some("dropped-token".to_owned()),
        };
        let mut request = Box::pin(fixture.client.call_tool("slow", json!({}), &options));
        let progress = timeout(Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    result = &mut request => panic!("request completed before drop: {result:?}"),
                    notification = notifications.recv() => {
                        let notification = notification.unwrap();
                        if notification.method == "notifications/progress" {
                            break notification;
                        }
                    }
                }
            }
        })
        .await
        .expect("progress notification was not delivered");
        assert_eq!(progress.progress_token(), Some(&json!("dropped-token")));
        drop(request);
        let cancelled = timeout(Duration::from_secs(2), notifications.recv())
            .await
            .expect("cancellation acknowledgement was not delivered")
            .unwrap();
        assert_eq!(cancelled.method, "notifications/fixture/cancelled");
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("cancel:"))
                .count(),
            1,
            "{events:?}"
        );
    })
    .await
    .expect("dropped request test timed out");
}

#[tokio::test]
async fn server_cancellation_fails_request_without_echoing_cancellation() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("server-cancel", |_| {}).await.unwrap();
        let mut notifications = fixture.client.subscribe();
        let error = fixture
            .client
            .call_tool("slow", json!({}), &request_options())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
        assert_eq!(error.method(), Some("tools/call"));
        let cancelled = notifications.recv().await.unwrap();
        assert_eq!(cancelled.method, "notifications/cancelled");
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert!(!events.iter().any(|event| event.starts_with("cancel:")));
    })
    .await
    .expect("server cancellation test timed out");
}

#[tokio::test]
async fn oversized_outgoing_frame_is_rejected_without_closing_connection() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("modern", |options| options.max_frame_bytes = 1024)
            .await
            .unwrap();
        let error = fixture
            .client
            .call_tool(
                "echo",
                json!({"message":"x".repeat(4096)}),
                &request_options(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert_eq!(error.method(), Some("tools/call"));
        assert!(error
            .message()
            .contains("outgoing frame exceeds 1024 bytes"));
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("outgoing frame limit test timed out");
}

#[tokio::test]
async fn blocked_write_deadline_fails_connection_and_reaps_child() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("blocked-write", |options| {
            options.max_frame_bytes = 4 * 1024 * 1024;
            options.close_timeout = Duration::from_millis(100);
        })
        .await
        .unwrap();
        fixture
            .client
            .call_tool("stop-reading", json!({}), &request_options())
            .await
            .unwrap();
        let blocked_options = RequestOptions::timeout(Duration::from_millis(400));
        let ping_options = request_options();
        let (blocked, queued) = tokio::join!(
            fixture.client.call_tool(
                "blocked",
                json!({"message":"x".repeat(1024 * 1024)}),
                &blocked_options,
            ),
            fixture.client.ping(&ping_options),
        );
        assert_eq!(blocked.unwrap_err().kind(), ErrorKind::Timeout);
        let error = queued.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.method(), Some("tools/call"));
        assert!(error
            .message()
            .contains("timed out writing request tools/call"));
        let report = fixture.close().await;
        assert!(report.forced, "{report:?}");
        assert!(report.timed_out, "{report:?}");
        assert!(report.exit_status.is_some(), "{report:?}");
        assert!(report.cleanup_error.is_none(), "{report:?}");
    })
    .await
    .expect("blocked write test timed out");
}

#[tokio::test]
async fn startup_timeout_reaps_probe_before_legacy_fallback() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("startup-timeout", |options| {
            options.startup_timeout = Duration::from_millis(75);
        })
        .await
        .unwrap();
        assert_eq!(fixture.client.protocol_version(), "2025-11-25");
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert_eq!(events, ["start", "eof", "start", "initialized", "eof"]);
    })
    .await
    .expect("startup timeout test timed out");
}

#[tokio::test]
async fn invalid_discovery_result_falls_back_to_fresh_legacy_child() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("invalid-discovery", |_| {}).await.unwrap();
        assert_eq!(fixture.client.protocol_version(), "2025-11-25");
        let (report, events) = fixture.close_with_events().await;
        assert!(report.clean_exit(), "{report:?}");
        assert_eq!(events, ["start", "eof", "start", "initialized", "eof"]);
    })
    .await
    .expect("invalid discovery test timed out");
}

#[tokio::test]
async fn malformed_discovery_does_not_fall_back_and_is_reaped() {
    timeout(TEST_TIMEOUT, async {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("events.txt");
        let error = McpClient::connect(
            server_config("malformed-discovery", &marker),
            ClientOptions {
                startup_timeout: Duration::from_secs(1),
                ..ClientOptions::default()
            },
        )
        .await
        .err()
        .expect("malformed discovery unexpectedly connected");
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert_eq!(error.method(), Some("server/discover"));
        let events = std::fs::read_to_string(marker).unwrap();
        assert_eq!(events.lines().collect::<Vec<_>>(), ["start", "eof"]);
    })
    .await
    .expect("malformed discovery test timed out");
}

#[tokio::test]
async fn invalid_response_envelopes_are_protocol_errors_and_reaped() {
    for scenario in ["ambiguous-response", "missing-response-result"] {
        timeout(TEST_TIMEOUT, async {
            let fixture = fixture(scenario, |_| {}).await.unwrap();
            let error = fixture.client.list_tools().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Protocol);
            assert!(error.message().contains("exactly one of result or error"));
            let report = fixture.close().await;
            assert!(report.exit_status.is_some(), "{report:?}");
            assert!(report.cleanup_error.is_none(), "{report:?}");
        })
        .await
        .expect("invalid response envelope test timed out");
    }
}

#[tokio::test]
async fn unknown_response_id_is_ignored_without_losing_next_response() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("unknown-response", |_| {}).await.unwrap();
        assert_eq!(fixture.client.list_tools().await.unwrap().len(), 3);
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("unknown response id test timed out");
}

#[tokio::test]
async fn missing_modern_result_type_is_invalid_but_connection_survives() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("missing-result-type", |_| {}).await.unwrap();
        let error = fixture.client.list_tools().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidResult);
        assert_eq!(error.method(), Some("tools/list"));
        assert!(fixture.client.ping(&request_options()).await.is_ok());
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("missing result type test timed out");
}

#[tokio::test]
async fn notification_backlog_is_bounded_and_reports_lag() {
    timeout(TEST_TIMEOUT, async {
        let fixture = fixture("modern", |options| options.notification_capacity = 1)
            .await
            .unwrap();
        let mut notifications = fixture.client.subscribe();
        assert_eq!(fixture.client.list_tools().await.unwrap().len(), 3);
        assert!(matches!(
            notifications.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));
        assert!(fixture.close().await.clean_exit());
    })
    .await
    .expect("notification backlog test timed out");
}

#[tokio::test]
async fn invalid_configuration_and_spawn_failure_are_structured() {
    let error = McpClient::connect(StdioServerConfig::new(""), ClientOptions::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    let error = McpClient::connect(
        StdioServerConfig::new("unused"),
        ClientOptions {
            request_timeout: Duration::MAX,
            ..ClientOptions::default()
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    let error = McpClient::connect(
        StdioServerConfig::new("dsh-mcp-command-that-does-not-exist"),
        ClientOptions::default(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
}
