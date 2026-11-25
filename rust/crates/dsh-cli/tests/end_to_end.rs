//! Full path: CLI args, DeepSeek client, agent loop, real tools, session on disk.
//! Only the model server is mocked.

use dsh_cli::{run, Args};
use dsh_core::Cancel;
use dsh_llm_deepseek::client::Connection;
use dsh_session_format::{scan, Recovery};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn sse(events: &[Value]) -> String {
    let body: String = events
        .iter()
        .map(|e| format!("event: {}\ndata: {}\n\n", e["type"].as_str().unwrap(), e))
        .collect();
    format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}", body.len(), body)
}

/// Serve each canned response to one request, returning every request body received.
async fn serve(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let h = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for resp in responses {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut got = Vec::new();
            let mut buf = vec![0u8; 65536];
            let body = loop {
                let n = sock.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
                let s = String::from_utf8_lossy(&got).into_owned();
                if let Some(h) = s.find("\r\n\r\n") {
                    let len = s[..h]
                        .to_lowercase()
                        .split("content-length: ")
                        .nth(1)
                        .and_then(|r| r.split("\r\n").next())
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if got.len() >= h + 4 + len {
                        break serde_json::from_str::<Value>(&s[h + 4..h + 4 + len]).unwrap();
                    }
                }
            };
            bodies.push(body);
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        }
        bodies
    });
    (url, h)
}

#[tokio::test]
async fn runs_a_task_with_a_real_tool_and_persists_the_session() {
    let work = std::env::temp_dir().join(format!("dsh-e2e-work-{}", std::process::id()));
    let home = std::env::temp_dir().join(format!("dsh-e2e-home-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&work).unwrap();

    let tool_turn = sse(&[
        json!({"type":"message_start","message":{"usage":{"input_tokens":50}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Creating the file. "}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"write","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"hello.txt\","}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"content\":\"hi from rust\\n\"}"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":20}}),
        json!({"type":"message_stop"}),
    ]);
    let final_turn = sse(&[
        json!({"type":"message_start","message":{"usage":{"input_tokens":80}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"All done."}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}),
        json!({"type":"message_stop"}),
    ]);
    let (url, server) = serve(vec![tool_turn, final_turn]).await;

    let mut conn = Connection::new("sk-test");
    conn.base_url = url;
    let args = Args {
        task: "make hello.txt".into(),
        cwd: Some(work.clone()),
        home: Some(home.clone()),
        ..Args::default()
    };
    let mut out: Vec<u8> = Vec::new();
    let code = run(args, conn, Cancel::default(), &mut out).await.unwrap();
    let transcript = String::from_utf8(out).unwrap();

    assert_eq!(code, 0, "{transcript}");
    assert!(transcript.contains("Creating the file."), "{transcript}");
    assert!(transcript.contains("[tool: write]"), "{transcript}");
    assert!(transcript.contains("All done."), "{transcript}");
    // The tool really ran.
    assert_eq!(
        std::fs::read_to_string(work.join("hello.txt")).unwrap(),
        "hi from rust\n"
    );

    // The second request carried the tool result back to the model, correctly paired.
    let bodies = server.await.unwrap();
    let advertised: Vec<&str> = bodies[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    // The headless runner has no Host Session log, so the Goal tools stay unadvertised.
    // The presets enable the platform's shell: pwsh on Windows, bash elsewhere.
    let shell = if cfg!(windows) { "pwsh" } else { "bash" };
    let mut expected = vec![
        "apply_patch",
        shell,
        "edit",
        "glob",
        "grep",
        "read",
        "todo_write",
        "write",
    ];
    expected.sort_unstable();
    assert_eq!(advertised, expected);
    let msgs = bodies[1]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[1]["content"][1]["type"], "tool_use");
    assert_eq!(msgs[2]["content"][0]["type"], "tool_result");
    assert_eq!(msgs[2]["content"][0]["tool_use_id"], "toolu_1");

    // The persisted log is a valid v4 session.
    let log = std::fs::read_dir(home.join("sessions"))
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .path();
    let session_dir = std::fs::read_dir(&log)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .path();
    let bytes = std::fs::read(session_dir.join("session.v4.jsonl")).unwrap();
    let scanned = scan(&bytes, Recovery::Strict).unwrap();
    assert_eq!(
        scanned.events.last().unwrap().data["reason"]["kind"],
        "completed"
    );
    assert!(scanned.events.iter().any(|e| e.kind == "tool/result"));

    let _ = std::fs::remove_dir_all(work);
    let _ = std::fs::remove_dir_all(home);
}
