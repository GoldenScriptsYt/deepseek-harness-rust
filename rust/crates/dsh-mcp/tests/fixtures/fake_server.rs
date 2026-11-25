use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

fn main() {
    let scenario = std::env::var("DSH_FAKE_MCP_SCENARIO").unwrap_or_else(|_| "modern".to_owned());
    if scenario == "stderr" {
        eprintln!("{}", "s".repeat(8192));
    }
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut modern = false;
    let mut initialized = false;
    let mut held: HashMap<String, Value> = HashMap::new();
    marker("start");

    loop {
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                if scenario == "stubborn" {
                    thread::sleep(Duration::from_secs(60));
                    continue;
                }
                break;
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("fake server read error: {error}");
                std::process::exit(2);
            }
        }
        let message: Value = match serde_json::from_str(&line) {
            Ok(message) => message,
            Err(_) => continue,
        };
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let id = message.get("id").cloned();
        if method.is_empty() {
            if id.as_ref().and_then(Value::as_i64) == Some(999)
                && message
                    .get("result")
                    .is_some_and(|result| result.is_object())
            {
                marker("server-ping");
                notify(&mut stdout, "notifications/fixture/server-ping", json!({}));
            }
            continue;
        }
        if id.is_none() {
            if method == "notifications/initialized" {
                initialized = true;
                marker("initialized");
            } else if method == "notifications/cancelled" {
                let cancelled_id = message
                    .get("params")
                    .and_then(|params| params.get("requestId"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                marker(&format!("cancel:{cancelled_id}"));
                if scenario == "cancel" {
                    notify(
                        &mut stdout,
                        "notifications/fixture/cancelled",
                        json!({"requestId":cancelled_id}),
                    );
                    if let Some(late_id) = held.remove(cancelled_id) {
                        respond(
                            &mut stdout,
                            modern,
                            late_id,
                            Ok(json!({"content":[{"type":"text","text":"late"}]})),
                        );
                    }
                }
            }
            continue;
        }
        let id = id.unwrap();
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        let response = match method {
            "server/discover" => {
                if scenario == "legacy" || scenario == "bad-version" {
                    error(-32601, "Method not found")
                } else if scenario == "startup-timeout" {
                    continue;
                } else if !discovery_meta_valid(&params) {
                    error(-32602, "missing modern request metadata")
                } else if scenario == "invalid-discovery" {
                    modern = true;
                    Ok(json!({"supportedVersions":42}))
                } else {
                    modern = true;
                    Ok(json!({
                        "supportedVersions":["2026-07-28"],
                        "capabilities": if scenario == "no-capabilities" { json!({}) } else { json!({
                            "tools":{"listChanged":true},
                            "resources":{"subscribe":false,"listChanged":true}
                        }) },
                        "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"fake-modern","version":"1.2.3"}},
                        "instructions":"deterministic fixture",
                        "ttlMs":60000,
                        "cacheScope":"server"
                    }))
                }
            }
            "initialize" => {
                if scenario == "modern" {
                    error(-32601, "discovery expected")
                } else if params.get("protocolVersion").and_then(Value::as_str)
                    != Some("2025-11-25")
                {
                    error(-32602, "unsupported offered version")
                } else {
                    modern = false;
                    Ok(json!({
                        "protocolVersion": if scenario == "bad-version" { "2099-01-01" } else { "2025-11-25" },
                        "capabilities":{"tools":{},"resources":{}},
                        "serverInfo":{"name":"fake-legacy","version":"1.0.0"},
                        "instructions":"legacy fixture"
                    }))
                }
            }
            "ping" => Ok(json!({})),
            "tools/list" if !initialized && !modern => error(-32002, "not initialized"),
            "tools/list" => {
                notification(&mut stdout, "notifications/tools/list_changed", json!({}));
                let cursor = params.get("cursor").and_then(Value::as_str);
                match (scenario.as_str(), cursor) {
                    ("repeat-cursor", _) => Ok(json!({"tools":[],"nextCursor":"same"})),
                    (_, None) => Ok(json!({
                        "tools":[tool("echo", "Echo", true)],
                        "nextCursor":"page-2"
                    })),
                    (_, Some("page-2")) => Ok(json!({
                        "tools":[tool("add", "Add", false), tool("fail", "Fail", false)]
                    })),
                    _ => error(-32602, "invalid cursor"),
                }
            }
            "tools/call" if !initialized && !modern => error(-32002, "not initialized"),
            "tools/call" => {
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                match name {
                    "echo" => {
                        let arguments = params
                            .get("arguments")
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        Ok(json!({
                            "content":[{"type":"text","text":"echo"}],
                            "structuredContent":{"arguments":arguments}
                        }))
                    }
                    "add" => Ok(json!({
                        "content":[{"type":"text","text":"3"}],
                        "structuredContent":{"value":3}
                    })),
                    "fail" => error(-32602, "invalid tool arguments"),
                    "tool-error" => Ok(json!({
                        "content":[{"type":"text","text":"tool failed"}],
                        "isError":true
                    })),
                    "stop-reading" => Ok(tool_result("stop reading")),
                    "slow" => {
                        held.insert(id.as_str().unwrap_or_default().to_owned(), id.clone());
                        if scenario == "server-cancel" {
                            notify(
                                &mut stdout,
                                "notifications/cancelled",
                                json!({"requestId":id,"reason":"fixture cancelled request"}),
                            );
                        }
                        let progress_token = params
                            .get("_meta")
                            .and_then(|meta| meta.get("progressToken"))
                            .cloned()
                            .unwrap_or_else(|| json!("fixture-token"));
                        notify(
                            &mut stdout,
                            "notifications/progress",
                            json!({"progressToken":progress_token,"progress":1,"total":2}),
                        );
                        continue;
                    }
                    "input-required" => Ok(json!({
                        "resultType":"input_required",
                        "content":[{"type":"text","text":"more input required"}]
                    })),
                    "hold" => {
                        held.insert(id.as_str().unwrap_or_default().to_owned(), id.clone());
                        continue;
                    }
                    "release" => {
                        for (_, held_id) in held.drain() {
                            respond(&mut stdout, modern, held_id, Ok(tool_result("held")));
                        }
                        respond(&mut stdout, modern, id.clone(), Ok(tool_result("released")));
                        if scenario == "server-ping" {
                            write_message(
                                &mut stdout,
                                json!({"jsonrpc":"2.0","id":999,"method":"ping"}),
                            );
                        }
                        continue;
                    }
                    _ => error(-32602, "unknown tool"),
                }
            }
            "resources/list" => Ok(json!({
                "resources":[{
                    "name":"test",
                    "uri":"file:///test.txt",
                    "mimeType":"text/plain",
                    "description":"fixture resource"
                }]
            })),
            "resources/read" => {
                if params.get("uri").and_then(Value::as_str) == Some("file:///test.txt") {
                    Ok(json!({"contents":[{
                        "uri":"file:///test.txt",
                        "mimeType":"text/plain",
                        "text":"fixture contents"
                    }]}))
                } else {
                    error(-32002, "resource not found")
                }
            }
            _ => error(-32601, "Method not found"),
        };

        if scenario == "malformed-discovery" && method == "server/discover" {
            writeln!(stdout, "{{not-json").unwrap();
            stdout.flush().unwrap();
            continue;
        }
        if scenario == "malformed" && method == "tools/list" {
            writeln!(stdout, "{{not-json").unwrap();
            stdout.flush().unwrap();
            continue;
        }
        if scenario == "ambiguous-response" && method == "tools/list" {
            write_message(
                &mut stdout,
                json!({"jsonrpc":"2.0","id":id,"result":{},"error":{"code":-1,"message":"bad"}}),
            );
            continue;
        }
        if scenario == "missing-response-result" && method == "tools/list" {
            write_message(&mut stdout, json!({"jsonrpc":"2.0","id":id}));
            continue;
        }
        if scenario == "missing-result-type" && method == "tools/list" {
            write_message(
                &mut stdout,
                json!({"jsonrpc":"2.0","id":id,"result":{"tools":[]}}),
            );
            continue;
        }
        if scenario == "unknown-response" && method == "tools/list" {
            write_message(
                &mut stdout,
                json!({"jsonrpc":"2.0","id":"unknown-id","result":{"resultType":"complete","tools":[]}}),
            );
        }
        if scenario == "oversized" && method == "tools/list" {
            let response = json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":{"resultType":"complete","tools":[],"padding":"x".repeat(4096)}
            });
            writeln!(stdout, "{response}").unwrap();
            stdout.flush().unwrap();
            continue;
        }
        respond(&mut stdout, modern, id, response);
        if scenario == "blocked-write"
            && method == "tools/call"
            && params.get("name").and_then(Value::as_str) == Some("stop-reading")
        {
            marker("stop-reading");
            thread::sleep(Duration::from_secs(60));
        }
    }
    if scenario != "stubborn" {
        marker("eof");
    }
}

fn discovery_meta_valid(params: &Value) -> bool {
    let Some(meta) = params.get("_meta").and_then(Value::as_object) else {
        return false;
    };
    meta.contains_key("io.modelcontextprotocol/protocolVersion")
        && meta.contains_key("io.modelcontextprotocol/clientCapabilities")
        && meta.contains_key("io.modelcontextprotocol/clientInfo")
}

fn tool(name: &str, description: &str, has_output: bool) -> Value {
    let mut value = json!({
        "name":name,
        "description":description,
        "inputSchema":{"type":"object","properties":{},"additionalProperties":true},
        "annotations":{"readOnlyHint":true}
    });
    if has_output {
        value
            .as_object_mut()
            .unwrap()
            .insert("outputSchema".to_owned(), json!({"type":"object"}));
    }
    value
}

fn tool_result(text: &str) -> Value {
    json!({"content":[{"type":"text","text":text}]})
}

type FakeResult = Result<Value, (i64, &'static str)>;

fn error(code: i64, message: &'static str) -> FakeResult {
    Err((code, message))
}

fn respond(stdout: &mut impl Write, modern: bool, id: Value, result: FakeResult) {
    match result {
        Ok(mut result) => {
            if modern {
                if let Some(object) = result.as_object_mut() {
                    if !object.contains_key("resultType") {
                        object.insert(
                            "resultType".to_owned(),
                            Value::String("complete".to_owned()),
                        );
                    }
                }
            }
            write_message(stdout, json!({"jsonrpc":"2.0","id":id,"result":result}));
        }
        Err((code, message)) => write_message(
            stdout,
            json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message,"data":{"fixture":true}}}),
        ),
    }
}

fn write_message(stdout: &mut impl Write, message: Value) {
    writeln!(stdout, "{message}").unwrap();
    stdout.flush().unwrap();
}

fn notification(stdout: &mut impl Write, method: &str, params: Value) {
    notify(stdout, method, params);
}

fn notify(stdout: &mut impl Write, method: &str, params: Value) {
    write_message(
        stdout,
        json!({"jsonrpc":"2.0","method":method,"params":params}),
    );
}

fn marker(event: &str) {
    let Some(path) = std::env::var_os("DSH_FAKE_MCP_MARKER") else {
        return;
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{event}").unwrap();
    file.flush().unwrap();
}
