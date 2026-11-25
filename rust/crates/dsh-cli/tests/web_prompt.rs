//! A web prompt runs a real turn: accepted by the RPC, committed to the session log, and
//! published to the streams following that Session. Only the model server is mocked.

use dsh_cli::api::{self, ApiContext};
use dsh_gateway::Gateway;
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

/// Answer one request with `response`, returning the origin to point the client at.
async fn serve(response: String) -> (String, tokio::task::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut got = Vec::new();
        let mut buf = vec![0u8; 65536];
        let body = loop {
            let n = sock.read(&mut buf).await.unwrap();
            got.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&got).into_owned();
            if let Some(head) = text.find("\r\n\r\n") {
                let len = text[..head]
                    .to_lowercase()
                    .split("content-length: ")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if got.len() >= head + 4 + len {
                    break serde_json::from_str::<Value>(&text[head + 4..head + 4 + len]).unwrap();
                }
            }
        };
        sock.write_all(response.as_bytes()).await.unwrap();
        sock.shutdown().await.ok();
        body
    });
    (url, handle)
}

#[tokio::test]
async fn a_prompt_runs_a_turn_and_publishes_events_to_followers() {
    let home = std::env::temp_dir().join(format!("dsh-web-prompt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    let cwd = home.join("project");
    std::fs::create_dir_all(&cwd).unwrap();

    let reply = sse(&[
        json!({"type":"message_start","message":{"usage":{"input_tokens":12}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello from the mock."}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}),
        json!({"type":"message_stop"}),
    ]);
    let (origin, server) = serve(reply).await;

    std::env::set_var("DEEPSEEK_API_KEY", "sk-test");
    std::env::set_var("DEEPSEEK_BASE_URL", &origin);

    let cx = ApiContext::new("http://x", "t", &home);
    let gateway = Gateway::new("t", home.display().to_string(), None);
    let follow = gateway.broadcaster("session/follow");
    cx.attach_follow(follow.clone());

    let cwd_string = cwd.display().to_string();
    let created =
        api::create_session(&cx, &json!({"args": {"request": {"cwd": cwd_string}}})).unwrap();
    let id = created["sessionId"].as_str().unwrap().to_string();

    // Subscribe before prompting, exactly as an open follow stream does.
    let mut frames = follow.subscribe();
    let accepted = api::prompt(
        cx.clone(),
        json!({"args": {"request": {
            "requestId": "r1", "sessionId": id, "mode": "queue",
            "content": [{"type": "text", "text": "say hello"}],
        }}}),
    )
    .await
    .unwrap();
    assert_eq!(accepted, json!({"accepted": true}));

    let mut seen = Vec::new();
    let mut reply_text = String::new();
    loop {
        let next = tokio::time::timeout(std::time::Duration::from_secs(15), frames.recv()).await;
        let Ok(Ok(frame)) = next else { break };
        assert_eq!(frame["sessionId"], json!(id));
        let event = &frame["frame"]["event"];
        let kind = event["type"].as_str().unwrap_or_default().to_string();
        seen.push(kind.clone());
        if kind == "assistant/message" {
            reply_text = event["data"]["message"]["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            break;
        }
    }

    assert!(
        seen.iter().any(|kind| kind == "user/message"),
        "no user message in {seen:?}"
    );
    assert_eq!(reply_text, "Hello from the mock.");

    // The published events carry the wire envelope the client's journal adapter expects.
    // The turn is also durable: the log is a valid v4 session ending a completed turn.
    let request_body = server.await.unwrap();
    assert_eq!(
        request_body["messages"][0]["content"][0]["text"],
        "say hello"
    );
    let project = std::fs::read_dir(home.join("sessions"))
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .path();
    let session_dir = std::fs::read_dir(&project)
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
    for (index, event) in scanned.events.iter().enumerate() {
        assert_eq!(
            event.seq(),
            Some(index as u64),
            "row {index} ({}) has no matching seq",
            event.kind
        );
    }

    let _ = std::fs::remove_dir_all(&home);
}
