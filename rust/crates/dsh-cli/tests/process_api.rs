//! Focused integration checks for the browser process API registration surface.

pub mod api {
    pub use dsh_cli::api::ApiContext;
}

#[path = "../src/process_api.rs"]
mod process_api;

use dsh_cli::api::ApiContext;
use dsh_gateway::Gateway;
use serde_json::{json, Value};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

struct TestHome(PathBuf);

impl TestHome {
    fn new() -> Self {
        use std::hash::{BuildHasher, Hasher};

        for _ in 0..16 {
            let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
            hasher.write_u128(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            );
            let path = std::env::temp_dir().join(format!(
                "dsh-process-api-integration-{}-{:016x}",
                std::process::id(),
                hasher.finish()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create process API test home: {error}"),
            }
        }
        panic!("could not allocate a unique process API test home");
    }
}

impl Drop for TestHome {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove process API test home");
    }
}

struct TestServer {
    addr: std::net::SocketAddr,
    cookie: String,
    next_request: AtomicU64,
    _gateway: Gateway,
    _home: TestHome,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl TestServer {
    async fn start() -> Self {
        let home = TestHome::new();
        let context = ApiContext::new("http://127.0.0.1", "test-token", &home.0);
        let gateway = Gateway::new("test-token", home.0.display().to_string(), None);
        process_api::register_all(&gateway, context);
        let (addr, serve) = gateway.bind("127.0.0.1:0").await.unwrap();
        let task = tokio::spawn(serve);
        let cookie = sign_in(addr, "/?token=test-token").await;
        Self {
            addr,
            cookie,
            next_request: AtomicU64::new(1),
            _gateway: gateway,
            _home: home,
            task: Some(task),
        }
    }

    async fn call(&self, method: &str, payload: Value) -> Value {
        let id = self.next_request.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "type": "client-request",
            "rpcId": format!("process-api-{id}"),
            "method": method,
            "payload": payload,
        })
        .to_string();
        let content_length = body.len().to_string();
        let response = http_request(
            self.addr,
            "POST",
            &format!("/api/{method}"),
            &[
                ("Cookie", self.cookie.as_str()),
                ("Content-Type", "application/json"),
                ("Content-Length", &content_length),
            ],
            Some(&body),
        )
        .await;
        serde_json::from_str(response_body(&response)).expect("Gateway JSON response")
    }

    async fn shutdown(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn http_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> String {
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }

    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        socket.read_to_string(&mut response),
    )
    .await
    .expect("Gateway response completes")
    .expect("read Gateway response");
    response
}

fn response_body(response: &str) -> &str {
    response.split_once("\r\n\r\n").unwrap_or_default().1
}

async fn sign_in(addr: std::net::SocketAddr, path: &str) -> String {
    let response = http_request(addr, "GET", path, &[], None).await;
    assert!(response.starts_with("HTTP/1.1 303"), "{response}");
    let headers = response.split_once("\r\n\r\n").unwrap_or_default().0;
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("set-cookie").then(|| {
                value
                    .trim()
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
        })
        .expect("sign-in cookie")
}

#[tokio::test]
async fn web_host_serves_session_query_and_composer_catalog_rpcs() {
    let home = TestHome::new();
    let dist = home.0.join("apps/web/dist");
    std::fs::create_dir_all(&dist).unwrap();
    std::fs::write(
        dist.join("index.html"),
        "<html><head></head><body></body></html>",
    )
    .unwrap();
    let started = dsh_cli::web::start(&home.0, &home.0, 0).await.unwrap();
    let (addr, query) = started
        .entry_url
        .strip_prefix("http://")
        .unwrap()
        .split_once('/')
        .unwrap();
    let addr = addr.parse().unwrap();
    let task = tokio::spawn(started.serve);
    let cookie = sign_in(addr, &format!("/{query}")).await;

    let response = call_host_rpc(
        addr,
        &cookie,
        "session/query/sessions",
        json!({"query": "needle"}),
    )
    .await;
    assert_eq!(response["ok"], true, "{response}");

    let project = home.0.join(format!("project-{}", "x".repeat(70)));
    std::fs::create_dir(&project).unwrap();
    let created = call_host_rpc(
        addr,
        &cookie,
        "session/create",
        json!({"args": {"request": {"cwd": project}}}),
    )
    .await;
    assert_eq!(created["ok"], true, "{created}");
    let session_id = created["value"]["sessionId"].as_str().unwrap();
    let commands = call_host_rpc(
        addr,
        &cookie,
        "commands/list",
        json!({"args": {"agentId": session_id}}),
    )
    .await;
    assert_eq!(commands, json!({"ok": true, "value": []}));

    let missing = call_host_rpc(
        addr,
        &cookie,
        "skills/list",
        json!({"args": {"request": {"sessionId": "missing-session"}}}),
    )
    .await;
    assert_eq!(missing["error"]["code"], "session/not-found");
    assert_eq!(missing["error"]["details"]["sessionId"], "missing-session");

    let roster = call_host_rpc(addr, &cookie, "agentPresets/list", json!({"args": {}})).await;
    assert_eq!(
        roster["value"],
        json!({"presets": [
            {"id": "standard", "order": 1, "isDefault": true},
            {"id": "ptc", "order": 2, "isDefault": false},
            {"id": "minimal", "order": 3, "isDefault": false},
            {"id": "cordis", "order": 4, "isDefault": false}
        ]})
    );
    let standard = call_host_rpc(
        addr,
        &cookie,
        "agentPresets/select",
        json!({"args": {"agentId": session_id, "agentPreset": "standard"}}),
    )
    .await;
    assert_eq!(standard, json!({"ok": true, "value": "standard"}));
    let minimal = call_host_rpc(
        addr,
        &cookie,
        "agentPresets/select",
        json!({"args": {"agentId": session_id, "agentPreset": "minimal"}}),
    )
    .await;
    assert_eq!(minimal, json!({"ok": true, "value": "minimal"}));
    let history = call_host_rpc(
        addr,
        &cookie,
        "session/page",
        json!({"args": {"address": {"kind": "session", "sessionId": session_id}}}),
    )
    .await;
    assert_eq!(
        history["value"]["records"][0]["event"]["type"], "agent-preset/selected",
        "{history}"
    );
    assert_eq!(
        history["value"]["records"][0]["event"]["data"]["agentPreset"],
        "minimal"
    );

    for preset in ["ptc", "cordis"] {
        let selected = call_host_rpc(
            addr,
            &cookie,
            "agentPresets/select",
            json!({"args": {"agentId": session_id, "agentPreset": preset}}),
        )
        .await;
        assert_eq!(selected, json!({"ok": true, "value": preset}));
    }
    let listed = call_host_rpc(addr, &cookie, "session/list", json!({"args": {}})).await;
    let row = listed["value"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["sessionId"] == session_id)
        .unwrap();
    assert_eq!(row["projections"]["values"]["agentPreset"], "cordis");

    let unknown = call_host_rpc(
        addr,
        &cookie,
        "agentPresets/select",
        json!({"args": {"agentId": session_id, "agentPreset": "missing"}}),
    )
    .await;
    assert_eq!(unknown["error"]["code"], "agent-preset/not-found");
    assert_eq!(
        unknown["error"]["details"]["available"]
            .as_array()
            .unwrap()
            .len(),
        4
    );

    let discovery = call_host_rpc(
        addr,
        &cookie,
        "llm/discoverModels",
        json!({"args": {"settingsNs": "llm-pi-ai", "request": {"provider": "amazon-bedrock"}}}),
    )
    .await;
    assert_eq!(discovery["error"]["code"], "llm/model-discovery-rejected");
    assert_eq!(
        discovery["error"]["details"],
        json!({"settingsNs": "llm-pi-ai", "provider": "amazon-bedrock"})
    );

    for (method, value) in [
        ("schedule/catalog", json!([])),
        (
            "pluginInventory/list",
            json!({"entries": [], "managementAvailable": false}),
        ),
        (
            "remoteConnection/pairingRequests",
            json!({"requests": [], "pollIntervalMs": 2000}),
        ),
    ] {
        let result = call_host_rpc(addr, &cookie, method, json!({"args": {}})).await;
        assert_eq!(result, json!({"ok": true, "value": value}), "{method}");
    }

    let registries = call_host_rpc(
        addr,
        &cookie,
        "pluginManager/registries",
        json!({"args": {}}),
    )
    .await;
    assert_eq!(registries["ok"], true);
    assert_eq!(registries["value"]["registry"], Value::Null);
    assert_eq!(
        registries["value"]["fallbackRegistries"],
        json!(["https://registry.npmmirror.com/"])
    );
    assert!(
        registries["value"]["resolved"].is_null()
            || registries["value"]["resolved"].as_str().is_some()
    );

    let schedule_dir = home.0.join("storages");
    std::fs::create_dir(&schedule_dir).unwrap();
    let schedule_file = schedule_dir.join("schedule.json");
    let saved_schedule =
        r#"{"unit":{"name":"schedule","version":2},"global":null,"tables":{"tasks":{}}}"#;
    std::fs::write(&schedule_file, saved_schedule).unwrap();
    let saved = call_host_rpc(addr, &cookie, "schedule/catalog", json!({"args": {}})).await;
    assert_eq!(saved["error"]["code"], "gateway/internal");
    assert_eq!(
        std::fs::read_to_string(schedule_file).unwrap(),
        saved_schedule
    );

    let credentials = call_host_rpc(
        addr,
        &cookie,
        "credentials/describe",
        json!({"args": {"refs": ["DEEPSEEK_API_KEY"]}}),
    )
    .await;
    assert_eq!(credentials["value"]["DEEPSEEK_API_KEY"]["writable"], false);
    let write = call_host_rpc(
        addr,
        &cookie,
        "credentials/set",
        json!({"args": {"ref": "DEEPSEEK_API_KEY", "value": "fixture-secret"}}),
    )
    .await;
    assert_eq!(write["error"]["code"], "credential/rejected");
    assert!(!write.to_string().contains("fixture-secret"));

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

async fn call_host_rpc(
    addr: std::net::SocketAddr,
    cookie: &str,
    method: &str,
    payload: Value,
) -> Value {
    let body = json!({
        "type": "client-request",
        "rpcId": method,
        "method": method,
        "payload": payload
    })
    .to_string();
    let response = http_request(
        addr,
        "POST",
        &format!("/api/{method}"),
        &[
            ("Cookie", cookie),
            ("Content-Type", "application/json"),
            ("Content-Length", &body.len().to_string()),
        ],
        Some(&body),
    )
    .await;
    let envelope: Value = serde_json::from_str(response_body(&response)).unwrap();
    envelope["result"].clone()
}

#[tokio::test]
async fn registers_terminal_rpc_surface_and_validates_wire_requests() {
    let mut server = TestServer::start().await;

    let environment = server
        .call(
            "terminal/environment",
            json!({ "args": { "sessionId": "session-a" } }),
        )
        .await;
    assert_eq!(environment["result"]["ok"], true);
    assert_eq!(environment["result"]["value"]["maxInputBytes"], 64 * 1024);
    assert_eq!(
        environment["result"]["value"]["maxBufferedBytes"],
        2 * 1024 * 1024
    );
    assert_eq!(environment["result"]["value"]["maxCols"], 500);
    assert_eq!(environment["result"]["value"]["maxRows"], 200);
    assert_eq!(environment["result"]["value"]["scrollback"], 1000);

    let list = server
        .call("terminal/list", json!({ "sessionId": "session-a" }))
        .await;
    assert_eq!(list["result"]["value"], json!([]));

    let invalid_create = server
        .call(
            "terminal/create",
            json!({
                "args": {
                    "request": {
                        "sessionId": "session-a",
                        "id": "terminal-a",
                        "cols": 80,
                        "rows": 24,
                        "shellPath": null,
                    }
                }
            }),
        )
        .await;
    assert_eq!(
        invalid_create["result"]["error"]["code"],
        "gateway/arguments-invalid"
    );

    let invalid_title = server
        .call(
            "terminal/rename",
            json!({
                "sessionId": "session-a",
                "id": "terminal-a",
                "title": format!("{} ", "x".repeat(120)),
            }),
        )
        .await;
    assert_eq!(
        invalid_title["result"]["error"]["code"],
        "gateway/arguments-invalid"
    );

    let missing_job = server
        .call(
            "job/kill",
            json!({ "args": { "sessionId": "session-a", "jobId": "bash-missing" } }),
        )
        .await;
    assert_eq!(missing_job["result"]["error"]["code"], "job/not-found");
    assert_eq!(
        missing_job["result"]["error"]["details"],
        json!({ "sessionId": "session-a", "jobId": "bash-missing" })
    );

    let close = server
        .call(
            "terminal/close",
            json!({ "args": { "sessionId": "session-a", "id": "missing" } }),
        )
        .await;
    assert_eq!(close["result"]["ok"], true);
    assert_eq!(close["result"]["value"], Value::Null);
    server.shutdown().await;
}

#[test]
fn temporary_homes_are_private_directories() {
    let first = TestHome::new();
    let second = TestHome::new();
    assert_ne!(first.0, second.0);
    assert!(first.0.is_dir());
    assert!(second.0.is_dir());
    assert_eq!(std::fs::read_dir(&first.0).unwrap().count(), 0);
}
