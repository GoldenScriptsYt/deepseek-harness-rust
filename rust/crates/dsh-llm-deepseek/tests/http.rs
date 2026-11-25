use dsh_llm::{ErrorCode, ImageMediaType};
use dsh_llm_deepseek::{
    client::Connection, files::ListFiles, files::UploadFile, listing::ModelListing, UploadedFile,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct MockResponse {
    status: u16,
    content_type: &'static str,
    headers: Vec<(&'static str, String)>,
    chunks: Vec<Vec<u8>>,
}

impl MockResponse {
    fn json(value: Value) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            headers: vec![],
            chunks: vec![serde_json::to_vec(&value).unwrap()],
        }
    }

    fn text(status: u16, text: &str) -> Self {
        Self {
            status,
            content_type: "text/plain",
            headers: vec![],
            chunks: vec![text.as_bytes().to_vec()],
        }
    }
}

/// One-request-per-connection loopback fixture with an OS-assigned port and awaited teardown.
struct MockServer {
    origin: String,
    task: JoinHandle<Vec<RecordedRequest>>,
}

impl MockServer {
    async fn start<F>(expected: usize, mut handler: F) -> Self
    where
        F: FnMut(&RecordedRequest) -> MockResponse + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(expected);
            for _ in 0..expected {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let response = handler(&request);
                write_response(&mut socket, response).await;
                requests.push(request);
            }
            requests
        });
        Self {
            origin: format!("http://{address}"),
            task,
        }
    }

    async fn finish(self) -> Vec<RecordedRequest> {
        self.task.await.unwrap()
    }

    async fn cancel(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn read_request(socket: &mut TcpStream) -> RecordedRequest {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 8192];
    let header_end = loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0, "connection closed before request headers");
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header_text = std::str::from_utf8(&bytes[..header_end - 4]).unwrap();
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap().to_string();
    let target = request_parts.next().unwrap().to_string();
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').unwrap();
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < header_end + length {
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0, "connection closed before request body");
        bytes.extend_from_slice(&buffer[..count]);
    }
    RecordedRequest {
        method,
        target,
        headers,
        body: bytes[header_end..header_end + length].to_vec(),
    }
}

async fn write_response(socket: &mut TcpStream, response: MockResponse) {
    let reason = match response.status {
        200 => "OK",
        307 => "Temporary Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Fixture",
    };
    let length: usize = response.chunks.iter().map(Vec::len).sum();
    let mut head = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {length}\r\nconnection: close\r\n",
        response.status, reason, response.content_type
    );
    for (name, value) in response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    socket.write_all(head.as_bytes()).await.unwrap();
    for chunk in response.chunks {
        socket.write_all(&chunk).await.unwrap();
    }
    socket.shutdown().await.unwrap();
}

fn file_value(id: &str) -> Value {
    json!({
        "id":id,"type":"file","mime_type":"image/png","size_bytes":3,
        "created_at":"2023-11-14T22:13:20.000Z","filename":"image.png"
    })
}

#[tokio::test]
async fn files_upload_list_retrieve_delete_use_messages_root_and_shared_auth() {
    let server = MockServer::start(4, |request| match (request.method.as_str(), request.target.as_str()) {
        ("POST", "/anthropic/v1/files") => MockResponse::json(file_value("file-api-one")),
        ("GET", "/anthropic/v1/files?after_id=file+before&limit=1000") => MockResponse::json(json!({
            "data":[file_value("file-api-one")],"first_id":"file-api-one","last_id":"file-api-one","has_more":false
        })),
        ("GET", "/anthropic/v1/files/file-api-one") => MockResponse::json(file_value("file-api-one")),
        ("DELETE", "/anthropic/v1/files/file-api-one") => MockResponse::json(json!({"id":"file-api-one","type":"file_deleted"})),
        _ => panic!("unexpected request: {} {}", request.method, request.target),
    }).await;
    let connection =
        Connection::new("fixture-key").with_base_url(format!("{}/anthropic", server.origin));
    let uploaded: Result<UploadedFile, _> = connection
        .upload_file(UploadFile {
            data: vec![1, 2, 3],
            media_type: ImageMediaType::Png,
            filename: "a\"b\r\nc.png".into(),
            expires_after_seconds: 3_600,
        })
        .await;
    let page_result = connection
        .list_files(ListFiles {
            after: Some("file before".into()),
            limit: Some(1000),
        })
        .await;
    let retrieved_result = connection.retrieve_file("file-api-one").await;
    let deleted_result = connection.delete_file("file-api-one").await;
    let requests = server.finish().await;

    let uploaded = uploaded.unwrap();
    assert_eq!(uploaded.file.id, "file-api-one");
    assert_eq!(uploaded.file.created_at, 1_700_000_000);
    assert_eq!(uploaded.expires_at, 1_700_003_600);
    assert_eq!(page_result.unwrap().data.len(), 1);
    assert_eq!(retrieved_result.unwrap().bytes, 3);
    deleted_result.unwrap();
    for request in &requests {
        assert_eq!(request.headers["x-api-key"], "fixture-key");
        assert_eq!(request.headers["anthropic-version"], "2023-06-01");
        assert_eq!(request.headers["anthropic-beta"], "files-api-2025-04-14");
        assert!(request.headers["user-agent"].starts_with("deepseek-harness/"));
        assert!(!request.headers.contains_key("authorization"));
    }
    let content_type = &requests[0].headers["content-type"];
    assert!(content_type.starts_with("multipart/form-data; boundary=----dsh-"));
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("name=\"expires_after[anchor]\"\r\n\r\ncreated_at"));
    assert!(body.contains("name=\"expires_after[seconds]\"\r\n\r\n3600"));
    assert!(body.contains("filename=\"a%22b%0D%0Ac.png\""));
    assert!(body.contains("Content-Type: image/png\r\n\r\n\u{1}\u{2}\u{3}\r\n--"));
}

#[tokio::test]
async fn files_table_driven_http_failures_keep_status_and_metadata() {
    let cases = [
        (401, ErrorCode::Auth),
        (403, ErrorCode::Auth),
        (429, ErrorCode::RateLimit),
        (500, ErrorCode::Server),
        (400, ErrorCode::FilesApi),
    ];
    for (status, code) in cases {
        let server = MockServer::start(1, move |_| MockResponse {
            status,
            content_type: "application/json",
            headers: vec![
                ("retry-after", "2".into()),
                ("x-request-id", format!("req-{status}")),
            ],
            chunks: vec![
                br#"{"error":{"type":"invalid_request_error","message":"fixture"}}"#.to_vec(),
            ],
        })
        .await;
        let connection =
            Connection::new("key").with_base_url(format!("{}/anthropic/v1", server.origin));
        let result = connection.retrieve_file("missing").await;
        let requests = server.finish().await;
        let error = result.unwrap_err();
        assert_eq!(error.code, code, "status {status}");
        assert_eq!(error.status, Some(status));
        assert_eq!(error.retry_after_ms, Some(2000));
        assert_eq!(
            error.request_id.as_deref(),
            Some(format!("req-{status}").as_str())
        );
        assert_eq!(requests[0].target, "/anthropic/v1/files/missing");
    }
}

#[tokio::test]
async fn files_malformed_success_is_invalid_response() {
    let server = MockServer::start(1, |_| MockResponse::text(200, "not-json")).await;
    let connection = Connection::new("key").with_base_url(server.origin.clone());
    let result = connection.retrieve_file("one").await;
    let requests = server.finish().await;
    let error = result.unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidResponse);
    assert_eq!(error.status, Some(200));
    assert!(error.message.contains("retrieve"));
    assert_eq!(requests[0].target, "/v1/files/one");
}

#[tokio::test]
async fn files_refuse_redirects_without_forwarding_credentials() {
    let destination =
        MockServer::start(0, |_| panic!("redirect destination must not be contacted")).await;
    let destination_origin = destination.origin.clone();
    let source = MockServer::start(1, move |_| MockResponse {
        status: 307,
        content_type: "text/plain",
        headers: vec![("location", format!("{destination_origin}/file-api-one"))],
        chunks: vec![],
    })
    .await;
    let connection = Connection::new("secret").with_base_url(source.origin.clone());
    let result = connection.retrieve_file("file-api-one").await;
    let source_requests = source.finish().await;
    let destination_requests = destination.finish().await;
    assert_eq!(result.unwrap_err().code, ErrorCode::Transport);
    assert_eq!(source_requests[0].headers["x-api-key"], "secret");
    assert!(destination_requests.is_empty());
}

#[tokio::test]
async fn listing_uses_origin_and_returns_provider_neutral_models() {
    let body = json!({"data":[
        {"id":"deepseek-flash","name":"Flash","input_modalities":["text","image"],"context_window":1048576,"max_output_tokens":393216,"api_capabilities":{"anthropic_messages":{"system_prompt_update":"in-history"}},"client_metadata":{"deepseek-harness":{"badge":{"text":"New","tone":"success"}}}},
        {"id":"hidden","api_capabilities":{}}
    ]});
    let server = MockServer::start(1, move |_| MockResponse::json(body.clone())).await;
    let connection = Connection::new("listing-key")
        .with_base_url(format!("{}/custom/anthropic/v1", server.origin));
    let result = connection.list_models("deepseek-official").await;
    let requests = server.finish().await;
    let models = result.unwrap();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].target, "/models");
    assert_eq!(requests[0].headers["accept"], "application/json");
    assert_eq!(requests[0].headers["x-api-key"], "listing-key");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].provider, "deepseek-official");
    assert_eq!(models[0].id, "deepseek-flash");
    assert_eq!(models[0].name, "Flash");
    assert_eq!(models[0].input_modalities.as_ref().unwrap().len(), 2);
    assert!(models[0].badge.is_some());
}

#[tokio::test]
async fn listing_table_driven_transport_and_decode_failures() {
    let cases = [
        (401, "unauthorized", ErrorCode::Http(401)),
        (307, "", ErrorCode::Transport),
        (200, "<html>", ErrorCode::InvalidResponse),
        (200, r#"{"data":[]}"#, ErrorCode::InvalidResponse),
    ];
    for (status, body, code) in cases {
        let server = MockServer::start(1, move |_| MockResponse::text(status, body)).await;
        let connection =
            Connection::new("key").with_base_url(format!("{}/anthropic", server.origin));
        let result: Result<ModelListing, _> = connection.model_listing().await;
        let requests = server.finish().await;
        let error = result.unwrap_err();
        assert_eq!(error.code, code, "status {status}, body {body}");
        if matches!(code, ErrorCode::Http(_)) {
            assert_eq!(error.status, Some(status));
        }
        assert_eq!(requests[0].target, "/models");
    }
}

#[tokio::test]
async fn listing_deadline_is_timeout_and_fixture_is_cancelled() {
    let server = MockServer::start(1, |_| MockResponse::json(json!({"data":[]}))).await;
    let connection = Connection::new("key").with_base_url(format!("{}/anthropic", server.origin));
    // A zero deadline deterministically expires before any network progress.
    let result = connection.model_listing_with_timeout(Duration::ZERO).await;
    assert_eq!(result.unwrap_err().code, ErrorCode::Timeout);
    server.cancel().await;
}
