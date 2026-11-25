//! Host-side registration: configuration, connection, and model-facing tools.

use dsh_core::{Cancel, ToolContext, ToolRegistry};
use dsh_mcp::{extract_text, parse_specs, public_tool_name, McpHost, McpServerSpec};
use serde_json::json;
use tempfile::TempDir;

fn spec(scenario: &str, marker: &std::path::Path) -> McpServerSpec {
    McpServerSpec {
        server_name: "fixture".to_owned(),
        command: Some(env!("CARGO_BIN_EXE_fake-mcp-server").to_owned()),
        env: [
            ("DSH_FAKE_MCP_SCENARIO".to_owned(), scenario.to_owned()),
            (
                "DSH_FAKE_MCP_MARKER".to_owned(),
                marker.display().to_string(),
            ),
        ]
        .into_iter()
        .collect(),
        tool_call_timeout_ms: Some(2_000),
        startup_timeout_ms: Some(3_000),
        close_timeout_ms: Some(1_000),
        ..McpServerSpec::default()
    }
}

fn context() -> ToolContext {
    ToolContext {
        cwd: std::env::temp_dir(),
        cancel: Cancel::default(),
        ..ToolContext::default()
    }
}

#[tokio::test]
async fn connects_lists_and_registers_server_qualified_tools() {
    let directory = TempDir::new().unwrap();
    let host = McpHost::connect(vec![spec("basic", &directory.path().join("events.txt"))])
        .await
        .unwrap();
    let mut registry = ToolRegistry::default();
    host.register(&mut registry);
    assert_eq!(
        registry.names(),
        vec![
            "mcp__fixture__add".to_owned(),
            "mcp__fixture__echo".to_owned(),
            "mcp__fixture__fail".to_owned(),
        ]
    );
    let echo = registry.get("mcp__fixture__echo").unwrap();
    assert_eq!(echo.def().description, "Echo");
    let result = echo.call(json!({"message":"hi"}), &context()).await;
    assert!(!result.is_error, "{result:?}");
    assert_eq!(result.text, "echo");
    let failure = registry
        .get("mcp__fixture__add")
        .unwrap()
        .call(json!({}), &context())
        .await;
    assert!(!failure.is_error, "{failure:?}");
    assert_eq!(failure.text, "3");
    assert!(host.failures().is_empty(), "{:?}", host.failures());
    assert_eq!(
        host.prompt_sections(),
        "### MCP server: fixture\n\ndeterministic fixture"
    );
}

#[tokio::test]
async fn server_rpc_failure_is_a_model_visible_tool_error() {
    let directory = TempDir::new().unwrap();
    let host = McpHost::connect(vec![spec("basic", &directory.path().join("events.txt"))])
        .await
        .unwrap();
    let mut registry = ToolRegistry::default();
    host.register(&mut registry);
    // `fail` is listed but rejected by the server, which must reach the model as
    // a failed tool result rather than a Rust error.
    let failure = registry
        .get("mcp__fixture__fail")
        .unwrap()
        .call(json!({}), &context())
        .await;
    assert!(failure.is_error, "{failure:?}");
    assert_eq!(
        failure.text,
        "MCP tool \"fail\" failed: JSON-RPC error -32602: invalid tool arguments"
    );
}

#[test]
fn configuration_rejects_invalid_entries() {
    let error = parse_specs("{\"serverName\":\"x\"}").unwrap_err();
    assert!(error.contains("must be a JSON array"), "{error}");
    let invalid = parse_specs("[{\"serverName\":\"bad name\",\"command\":\"x\"}]").unwrap();
    let error = invalid[0].validate().unwrap_err();
    assert!(error.contains("letters, digits"), "{error}");
    let missing = McpServerSpec {
        server_name: "ok".to_owned(),
        ..McpServerSpec::default()
    };
    assert!(missing
        .validate()
        .unwrap_err()
        .contains("non-empty command"));
    let long = McpServerSpec {
        server_name: "a".repeat(33),
        command: Some("x".to_owned()),
        ..McpServerSpec::default()
    };
    assert!(long.validate().unwrap_err().contains("1 to 32 characters"));
}

#[test]
fn public_names_are_stable_and_collision_free() {
    assert_eq!(
        public_tool_name("github", "create_issue"),
        "mcp__github__create_issue"
    );
    let lossy = public_tool_name("server", "create issue");
    assert!(lossy.starts_with("mcp__server__create_issue_"), "{lossy}");
    assert!(lossy.len() <= 64);
    assert_ne!(lossy, public_tool_name("server", "create-issue"));
    let long = public_tool_name("server", &"tool".repeat(30));
    assert_eq!(long.len(), 64);
    assert_eq!(long, public_tool_name("server", &"tool".repeat(30)));
}

#[test]
fn content_projection_names_every_unavailable_block() {
    let text = extract_text(
        &[
            json!({"type":"text","text":"first"}),
            json!({"type":"image","data":"AA==","mimeType":"image/png"}),
            json!({"type":"resource_link","name":"spec","uri":"file:///spec.md"}),
            json!({"type":"audio","data":"AA==","mimeType":"audio/wav"}),
            json!({"type":"resource","resource":{"uri":"file:///a"}}),
            json!({"type":"future"}),
            json!("oops"),
        ],
        "tool",
    );
    assert_eq!(
        text,
        [
            "first".to_owned(),
            "[image unavailable: image/png; this result was not admitted to durable model context; raw image data remains available to programmatic callers]".to_owned(),
            "Resource link: spec (file:///spec.md)".to_owned(),
            "[audio result unsupported: audio/wav; raw audio data remains available to programmatic callers]".to_owned(),
            "[embedded resource unsupported; raw resource data remains available to programmatic callers]".to_owned(),
            "[unsupported MCP content type: future]".to_owned(),
            "[unsupported MCP content block: expected an object]".to_owned(),
        ]
        .join("\n")
    );
    assert_eq!(
        extract_text(&[], "echo"),
        "(echo returned no model-visible content)"
    );
}
