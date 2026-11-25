//! Streaming HTTP transport with connection-owned client reuse.

use crate::files::MESSAGES_FILES_BETA;
use crate::sse::{parse_event, SseDecoder, SseItem};
use crate::translate::Translator;
use dsh_llm::{ErrorCode, LlmError, StreamChunk};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;

pub const PUBLIC_BASE_URL: &str = "https://api.deepseek.com/anthropic";
pub const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_millis(300_000);
const TOOL_CHANGES_BETA: &str = "mid-conversation-tool-changes-2026-07-01";

fn user_agent() -> String {
    let mut value = format!(
        "deepseek-harness/{} (+https://github.com/deepseek-ai/deepseek-harness)",
        env!("CARGO_PKG_VERSION")
    );
    if let Ok(suffix) = std::env::var("DSH_USER_AGENT_SUFFIX") {
        if !suffix.is_empty()
            && suffix.bytes().all(|c| {
                c.is_ascii_alphanumeric()
                    || matches!(
                        c,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    )
            })
        {
            value.push(' ');
            value.push_str(&suffix);
        }
    }
    value
}

/// Where and as whom requests are sent. Clones share one lazily built HTTP client.
#[derive(Debug, Clone)]
pub struct Connection {
    pub base_url: String,
    pub api_key: String,
    pub user_id: String,
    pub idle_timeout: Duration,
    client: Arc<OnceLock<Result<Arc<reqwest::Client>, LlmError>>>,
}

impl Connection {
    pub fn new(api_key: impl Into<String>) -> Self {
        Connection {
            base_url: PUBLIC_BASE_URL.into(),
            api_key: api_key.into(),
            user_id: "dsh-rust".into(),
            idle_timeout: DEFAULT_STREAM_IDLE_TIMEOUT,
            client: Arc::new(OnceLock::new()),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Shared, redirect-disabled client. Construction failures are cached, never panicked.
    pub fn client(&self) -> Result<Arc<reqwest::Client>, LlmError> {
        self.client
            .get_or_init(|| {
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .connect_timeout(Duration::from_secs(30))
                    .user_agent(user_agent())
                    .build()
                    .map(Arc::new)
                    .map_err(transport)
            })
            .clone()
    }
}

/// API root without duplicating an explicit `/v1`.
pub fn messages_api_root(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    let path = base.split_once("://").map_or(base, |(_, rest)| {
        rest.split_once('/').map_or("", |(_, p)| p)
    });
    if path.trim_end_matches('/').ends_with("v1") {
        base.to_string()
    } else {
        format!("{base}/v1")
    }
}

fn normalize_detail(value: &str) -> String {
    let mut output = String::new();
    let mut pending_space = false;
    for character in value.chars() {
        if character.is_whitespace() || matches!(character, '-' | '_') {
            pending_space = !output.is_empty();
        } else {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(character.to_ascii_lowercase());
        }
    }
    format!(" {output} ")
}

fn has_any(detail: &str, phrases: &[&str]) -> bool {
    phrases
        .iter()
        .any(|phrase| detail.contains(&normalize_detail(phrase)))
}

fn quota_exceeded(detail: &str) -> bool {
    has_any(
        detail,
        &[
            "insufficient quota",
            "insufficient balance",
            "insufficient credit",
            "insufficient credits",
            "quota exceeded",
            "quota exhausted",
            "quota reached",
            "usage limit exceeded",
            "usage limit exhausted",
            "usage limit reached",
            "balance exhausted",
            "balance depleted",
            "credit exhausted",
            "credit depleted",
            "credits exhausted",
            "credits depleted",
            "out of credit",
            "out of credits",
            "out of budget",
        ],
    ) || {
        let words: Vec<&str> = detail.split_whitespace().collect();
        words.windows(2).any(|pair| {
            matches!(
                pair[0].trim_matches(['(', ',', '.', ';', ':']),
                "exceeded" | "exceeds" | "exceed"
            ) && matches!(
                pair[1].trim_matches([')', ',', '.', ';', ':']),
                "quota" | "your" | "the" | "current"
            )
        })
    }
}

fn context_window_exceeded(detail: &str) -> bool {
    if has_any(
        detail,
        &[
            "maximum context length",
            "maximum context window",
            "max context length",
            "max context window",
            "maximum allowed context length",
            "maximum allowed context window",
            "maximum supported context length",
            "maximum supported context window",
        ],
    ) {
        return true;
    }
    let words: Vec<&str> = detail.split_whitespace().collect();
    for (index, word) in words.iter().enumerate() {
        let subject = word.trim_matches(['(', ',', '.', ';', ':']);
        if !matches!(
            subject,
            "request" | "prompt" | "input" | "message" | "messages"
        ) {
            continue;
        }
        let tail = &words[index + 1..];
        if tail.windows(2).any(|pair| {
            matches!(
                pair[0].trim_matches(['(', ',', '.', ';', ':']),
                "is" | "are"
            ) && matches!(pair[1].trim_matches(['(', ',', '.', ';', ':']), "too")
        }) {
            let text = tail.join(" ");
            if (text.contains("too large for") || text.contains("too long for"))
                && (text.contains("context")
                    || text.contains("for this model")
                    || text.contains("for the model"))
            {
                return true;
            }
        }
        if tail.len() <= 10 {
            let overflow = tail.iter().position(|word| {
                matches!(
                    word.trim_matches(['(', ',', '.', ';', ':']),
                    "exceeds" | "exceeded" | "overflow" | "overflows"
                )
            });
            if let Some(overflow) = overflow {
                let remaining = &tail[overflow + 1..];
                if remaining.len() <= 7 && remaining.iter().any(|word| word.contains("context")) {
                    return true;
                }
            }
            if tail
                .windows(3)
                .any(|window| window[0] == "is" && window[1] == "larger" && window[2] == "than")
                && tail.iter().any(|word| word.contains("context"))
            {
                return true;
            }
        }
    }
    false
}

/// Parse positive decimal seconds or an HTTP-date into milliseconds.
pub fn parse_retry_after(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    let decimal = match value.split_once('.') {
        Some((whole, fraction)) => {
            !whole.is_empty()
                && !fraction.is_empty()
                && !fraction.contains('.')
                && whole.bytes().all(|byte| byte.is_ascii_digit())
                && fraction.bytes().all(|byte| byte.is_ascii_digit())
        }
        None => !bytes.is_empty() && bytes.iter().all(u8::is_ascii_digit),
    };
    if decimal {
        let seconds = value.parse::<f64>().ok()?;
        let milliseconds = seconds * 1000.0;
        return (milliseconds.is_finite() && milliseconds > 0.0)
            .then(|| milliseconds.ceil() as u64);
    }
    let instant = httpdate::parse_http_date(value).ok()?;
    let delay = instant.duration_since(SystemTime::now()).ok()?;
    Some(delay.as_millis().min(u64::MAX as u128) as u64)
}

/// Retain the first provider-issued request id in TypeScript precedence order.
pub fn request_id(headers: &HeaderMap) -> Option<String> {
    ["request-id", "x-request-id", "x-deepseek-request-id"]
        .iter()
        .find_map(|name| {
            headers
                .get(*name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        })
}

/// Classify an HTTP or in-band provider error. Mirrors `providerError` in `transport.ts`.
pub fn provider_error(raw: &Value, status: Option<u16>, retry_after: Option<&str>) -> LlmError {
    let err = raw.get("error");
    let text = |key: &str| {
        err.and_then(|error| error.get(key))
            .and_then(Value::as_str)
            .unwrap_or("")
    };
    let message = match text("message") {
        "" => format!(
            "DeepSeek Messages request failed ({})",
            status.map_or_else(|| "stream error".into(), |status| status.to_string())
        ),
        message => message.to_string(),
    };
    let ty = text("type");
    let provider_detail = format!("{ty} {} {}", text("code"), message);
    let detail = normalize_detail(&provider_detail);
    let code = if status == Some(401)
        || status == Some(403)
        || matches!(ty, "authentication_error" | "permission_error")
    {
        ErrorCode::Auth
    } else if status == Some(402) || quota_exceeded(&detail) {
        ErrorCode::Quota
    } else if status == Some(429) || ty == "rate_limit_error" {
        ErrorCode::RateLimit
    } else if context_window_exceeded(&detail) {
        ErrorCode::ContextWindowExceeded
    } else if matches!(status, Some(400) | Some(413)) || ty == "invalid_request_error" {
        ErrorCode::InvalidRequest
    } else if status.is_some_and(|status| status >= 500)
        || matches!(ty, "api_error" | "overloaded_error")
    {
        ErrorCode::Server
    } else {
        status.map_or(ErrorCode::Server, ErrorCode::Http)
    };
    let mut error =
        LlmError::new(code, message).with_provider_detail(provider_detail.trim().to_string());
    if let Some(status) = status {
        error = error.with_status(status);
    }
    if let Some(delay) = retry_after.and_then(parse_retry_after) {
        error = error.with_retry_after(delay);
    }
    error
}

/// Classify a response and preserve its retry/request-id headers.
pub fn provider_response_error(raw: &Value, status: u16, headers: &HeaderMap) -> LlmError {
    let retry_after = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok());
    let mut error = provider_error(raw, Some(status), retry_after);
    if let Some(id) = request_id(headers) {
        error = error.with_request_id(id);
    }
    error
}

fn transport(error: impl std::fmt::Display) -> LlmError {
    LlmError::new(
        ErrorCode::Transport,
        format!("DeepSeek Messages transport failed: {error}"),
    )
}

fn request_transport(error: reqwest::Error) -> LlmError {
    if error.is_timeout() {
        LlmError::new(
            ErrorCode::Timeout,
            format!("DeepSeek Messages request timed out: {error}"),
        )
    } else {
        transport(error)
    }
}

fn contains_file_image_source(value: &Value) -> bool {
    match value {
        Value::Object(fields) => {
            fields.get("type").and_then(Value::as_str) == Some("image")
                && fields
                    .get("source")
                    .and_then(|source| source.get("type"))
                    .and_then(Value::as_str)
                    == Some("file")
                || fields.values().any(contains_file_image_source)
        }
        Value::Array(items) => items.iter().any(contains_file_image_source),
        _ => false,
    }
}

/// Send `body` and forward translated chunks to `tx` until the stream ends or fails.
/// The final error, if any, is sent as the last item. Dropping the receiver cancels.
pub async fn stream(
    conn: &Connection,
    body: &Value,
    session_id: Option<&str>,
    compaction: bool,
    tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
) {
    if let Err(error) = run(conn, body, session_id, compaction, &tx).await {
        let _ = tx.send(Err(error)).await;
    }
}

async fn run(
    conn: &Connection,
    body: &Value,
    session_id: Option<&str>,
    compaction: bool,
    tx: &mpsc::Sender<Result<StreamChunk, LlmError>>,
) -> Result<(), LlmError> {
    let client = conn.client()?;
    let uses_tool_changes = body["messages"].as_array().is_some_and(|messages| {
        messages.iter().any(|message| {
            message["content"].as_array().is_some_and(|content| {
                content.iter().any(|block| {
                    matches!(
                        block["type"].as_str(),
                        Some("tool_addition") | Some("tool_removal")
                    )
                })
            })
        })
    });
    let mut request = client
        .post(format!("{}/messages", messages_api_root(&conn.base_url)))
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .header("x-api-key", &conn.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("x-deepseek-harness-user-id", &conn.user_id)
        .body(body.to_string());
    let mut betas = Vec::new();
    if contains_file_image_source(body) {
        betas.push(MESSAGES_FILES_BETA);
    }
    if uses_tool_changes {
        betas.push(TOOL_CHANGES_BETA);
    }
    if !betas.is_empty() {
        request = request.header("anthropic-beta", betas.join(","));
    }
    if let Some(id) = session_id {
        request = request.header("x-deepseek-harness-session-id", id);
    }
    if compaction {
        request = request.header("x-deepseek-harness-compact", "1");
    }
    let response = request.send().await.map_err(request_transport)?;
    let status = response.status();
    if status.is_redirection() {
        return Err(transport("redirects are not allowed"));
    }
    if !status.is_success() {
        let headers = response.headers().clone();
        let text = response.text().await.map_err(request_transport)?;
        let raw: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        return Err(provider_response_error(&raw, status.as_u16(), &headers));
    }

    let mut decoder = SseDecoder::default();
    let mut translator = Translator::default();
    let mut bytes = response.bytes_stream();
    loop {
        let next = tokio::time::timeout(conn.idle_timeout, bytes.next())
            .await
            .map_err(|_| {
                LlmError::new(
                    ErrorCode::Timeout,
                    format!(
                        "DeepSeek Messages stream idle timeout after {} ms",
                        conn.idle_timeout.as_millis()
                    ),
                )
            })?;
        let Some(chunk) = next else {
            return Err(LlmError::new(
                ErrorCode::StreamClosed,
                "DeepSeek Messages stream ended before message_stop",
            ));
        };
        for item in decoder.push_bytes(&chunk.map_err(request_transport)?) {
            let SseItem::Frame { event, data } = item else {
                continue;
            };
            let event = parse_event(event.as_deref(), &data)?;
            let done = event["type"] == "message_stop";
            for chunk in translator.push(&event)? {
                if tx.send(Ok(chunk)).await.is_err() {
                    return Ok(());
                }
            }
            if let Some(failure) = translator.failure.take() {
                return Err(failure);
            }
            if done {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::UNIX_EPOCH;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn api_root_does_not_double_v1() {
        assert_eq!(
            messages_api_root("https://x.com/anthropic"),
            "https://x.com/anthropic/v1"
        );
        assert_eq!(
            messages_api_root("https://x.com/anthropic/"),
            "https://x.com/anthropic/v1"
        );
        assert_eq!(messages_api_root("https://x.com/v1"), "https://x.com/v1");
    }

    #[test]
    fn errors_classify_like_typescript() {
        let cases = [
            (
                json!({"error":{"type":"rate_limit_error","message":"slow"}}),
                Some(429),
                ErrorCode::RateLimit,
            ),
            (Value::Null, Some(401), ErrorCode::Auth),
            (Value::Null, Some(402), ErrorCode::Quota),
            (Value::Null, Some(503), ErrorCode::Server),
            (Value::Null, Some(400), ErrorCode::InvalidRequest),
            (Value::Null, Some(418), ErrorCode::Http(418)),
            (
                json!({"error":{"type":"overloaded_error","message":"busy"}}),
                None,
                ErrorCode::Server,
            ),
        ];
        for (raw, status, expected) in cases {
            assert_eq!(provider_error(&raw, status, None).code, expected);
        }
        let error = provider_error(
            &json!({"error":{"type":"invalid_request_error","message":"prompt is too long for this model"}}),
            Some(400),
            None,
        );
        assert_eq!(error.code, ErrorCode::ContextWindowExceeded);
        assert_eq!(
            provider_error(&Value::Null, Some(400), None).code,
            ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn quota_wording_is_not_a_rate_limit() {
        for message in [
            "insufficient balance",
            "usage limit reached",
            "out of credits",
            "current quota exceeded",
        ] {
            assert_eq!(
                provider_error(&json!({"error":{"message":message}}), Some(400), None).code,
                ErrorCode::Quota,
                "{message}"
            );
        }
        assert_eq!(
            provider_error(
                &json!({"error":{"message":"too many requests"}}),
                Some(400),
                None
            )
            .code,
            ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        assert_eq!(parse_retry_after("2.5"), Some(2500));
        for invalid in ["0", "-1", "+2", "2e3", ".5", "2.", "1.2.3", " 2"] {
            assert_eq!(parse_retry_after(invalid), None, "{invalid}");
        }
        let future = UNIX_EPOCH
            + Duration::from_secs(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 60,
            );
        let date = httpdate::fmt_http_date(future);
        let parsed = parse_retry_after(&date).unwrap();
        assert!((59_000..=60_000).contains(&parsed));
        assert_eq!(parse_retry_after("Thu, 01 Jan 1970 00:00:00 GMT"), None);
    }

    #[test]
    fn response_errors_keep_request_id_and_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert("x-deepseek-request-id", "req-1".parse().unwrap());
        headers.insert(RETRY_AFTER, "3".parse().unwrap());
        let error = provider_response_error(&Value::Null, 429, &headers);
        assert_eq!(error.request_id.as_deref(), Some("req-1"));
        assert_eq!(error.retry_after_ms, Some(3000));
    }

    #[test]
    fn cloned_connections_share_the_http_client() {
        let connection = Connection::new("key");
        let clone = connection.clone();
        let first = connection.client().unwrap();
        let second = clone.client().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    async fn serve_once(response: String) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0u8; 16384];
            let mut received = Vec::new();
            loop {
                let count = socket.read(&mut buffer).await.unwrap();
                received.extend_from_slice(&buffer[..count]);
                let text = String::from_utf8_lossy(&received);
                if let Some(header_end) = text.find("\r\n\r\n") {
                    let length = text[..header_end]
                        .to_lowercase()
                        .split("content-length: ")
                        .nth(1)
                        .and_then(|value| value.split("\r\n").next())
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if received.len() >= header_end + 4 + length {
                        break;
                    }
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.ok();
            String::from_utf8_lossy(&received).into_owned()
        });
        (format!("http://{address}"), server)
    }

    fn sse(events: &[Value]) -> String {
        let body: String = events
            .iter()
            .map(|event| {
                format!(
                    "event: {}\ndata: {}\n\n",
                    event["type"].as_str().unwrap(),
                    event
                )
            })
            .collect();
        format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}", body.len(), body)
    }

    #[tokio::test]
    async fn streams_a_full_turn_and_sends_auth_headers() {
        let (url, server) = serve_once(sse(&[
            json!({"type":"message_start","message":{"usage":{"input_tokens":4}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
            json!({"type":"message_stop"}),
        ])).await;
        let connection = Connection::new("sk-test").with_base_url(url);
        let (tx, mut rx) = mpsc::channel(32);
        stream(
            &connection,
            &json!({"model":"m","messages":[]}),
            Some("sess"),
            false,
            tx,
        )
        .await;
        let mut received = Vec::new();
        while let Some(item) = rx.recv().await {
            received.push(item.unwrap());
        }
        assert!(received
            .iter()
            .any(|chunk| matches!(chunk, StreamChunk::TextDelta { text, .. } if text == "hello")));
        assert!(matches!(received.last(), Some(StreamChunk::Finish { .. })));
        let request = server.await.unwrap().to_lowercase();
        assert!(request.starts_with("post /v1/messages"));
        assert!(request.contains("x-api-key: sk-test"));
        assert!(request.contains("x-deepseek-harness-session-id: sess"));
        assert!(request.contains("user-agent: deepseek-harness/"));
    }

    #[tokio::test]
    async fn file_sources_enable_files_beta_and_combine_tool_changes_beta() {
        let (url, server) = serve_once(sse(&[
            json!({"type":"message_start","message":{"usage":{"input_tokens":1}}}),
            json!({"type":"message_stop"}),
        ]))
        .await;
        let connection = Connection::new("k").with_base_url(url);
        let body = json!({"messages":[{"role":"user","content":[
            {"type":"tool_result","tool_use_id":"call-1","content":[
                {"type":"image","source":{"type":"file","file_id":"file-1"}}
            ]},
            {"type":"tool_addition"}
        ]}]});
        let (tx, mut rx) = mpsc::channel(4);
        stream(&connection, &body, None, false, tx).await;
        while rx.recv().await.is_some() {}
        let request = server.await.unwrap().to_lowercase();
        assert!(request.contains(&format!(
            "anthropic-beta: {},{}",
            MESSAGES_FILES_BETA, TOOL_CHANGES_BETA
        )));
    }

    #[tokio::test]
    async fn http_errors_are_classified() {
        let body = "{\"error\":{\"type\":\"authentication_error\",\"message\":\"bad key\"}}";
        let (url, _server) = serve_once(format!(
            "HTTP/1.1 401 Unauthorized\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ))
        .await;
        let connection = Connection::new("k").with_base_url(url);
        let (tx, mut rx) = mpsc::channel(4);
        stream(&connection, &json!({}), None, false, tx).await;
        let error = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(
            (error.code, error.status, error.message.as_str()),
            (ErrorCode::Auth, Some(401), "bad key")
        );
    }

    #[tokio::test]
    async fn redirects_are_transport_failures() {
        let (url, server) = serve_once(
            "HTTP/1.1 307 Temporary Redirect\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                .to_string(),
        )
        .await;
        let connection = Connection::new("k").with_base_url(url);
        let (tx, mut rx) = mpsc::channel(1);
        stream(&connection, &json!({}), None, false, tx).await;
        let error = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(error.code, ErrorCode::Transport);
        assert!(error.message.contains("redirects are not allowed"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn premature_eof_is_stream_closed() {
        let (url, _server) = serve_once(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 0\r\n\r\n"
                .into(),
        )
        .await;
        let connection = Connection::new("k").with_base_url(url);
        let (tx, mut rx) = mpsc::channel(4);
        stream(&connection, &json!({}), None, false, tx).await;
        assert_eq!(
            rx.recv().await.unwrap().unwrap_err().code,
            ErrorCode::StreamClosed
        );
    }
}
