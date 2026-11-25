//! DeepSeek web search through an Anthropic-compatible Messages call with the
//! native `web_search_20250305` server tool.

use crate::error::{WebError, WebErrorCode};
use dsh_core::Cancel;
use serde_json::{json, Value};
use std::time::Duration;

/// Default search endpoint, including `/v1`; `/messages` is appended.
pub const DEFAULT_SEARCH_BASE_URL: &str = "https://api.deepseek.com/anthropic/v1";
/// Default Anthropic-format model name.
pub const DEFAULT_SEARCH_MODEL: &str = "deepseek-v4-flash";
/// Default `anthropic-version` header value.
pub const DEFAULT_API_VERSION: &str = "2023-06-01";
/// Default generated-token bound.
pub const DEFAULT_MAX_TOKENS: u32 = 4096;
/// Default `web_search` server-tool uses per request.
pub const DEFAULT_MAX_USES: u32 = 5;
/// Default search deadline.
pub const DEFAULT_SEARCH_TIMEOUT: Duration = Duration::from_secs(60);
/// Attribution header sent on every request.
pub const DEFAULT_SEARCH_USER_AGENT: &str = "deepseek-harness/0.0.1";

/// Resolved search configuration.
#[derive(Clone, Debug)]
pub struct DeepSeekSearchConfig {
    /// Messages endpoint root; `/messages` is appended.
    pub base_url: String,
    /// API key sent as `x-api-key`.
    pub api_key: String,
    /// Anthropic-format model name.
    pub model: String,
    /// `anthropic-version` header value.
    pub api_version: String,
    /// Generated-token bound.
    pub max_tokens: u32,
    /// Server-tool uses per request.
    pub max_uses: u32,
    /// Per-search deadline.
    pub timeout: Duration,
}

impl DeepSeekSearchConfig {
    /// Build the shipped configuration around one API key.
    /// @param api_key - DeepSeek API key.
    #[must_use]
    pub fn new(api_key: impl Into<String>) -> Self {
        DeepSeekSearchConfig {
            base_url: DEFAULT_SEARCH_BASE_URL.to_string(),
            api_key: api_key.into(),
            model: DEFAULT_SEARCH_MODEL.to_string(),
            api_version: DEFAULT_API_VERSION.to_string(),
            max_tokens: DEFAULT_MAX_TOKENS,
            max_uses: DEFAULT_MAX_USES,
            timeout: DEFAULT_SEARCH_TIMEOUT,
        }
    }

    /// Resolved Messages endpoint.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("{}/messages", self.base_url.trim_end_matches('/'))
    }
}

/// One citeable search source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSource {
    /// Source URL.
    pub url: String,
    /// Page title when the provider supplies one.
    pub title: Option<String>,
    /// Provider snippet, or the citation excerpt for this URL.
    pub snippet: Option<String>,
    /// Provider summary text, when the provider returns a synthesized answer.
    pub summary: Option<String>,
    /// Page age, verbatim from the provider.
    pub published_at: Option<String>,
}

/// One search outcome.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WebSearchResult {
    /// Provider answer text, when any.
    pub content: Option<String>,
    /// Deduped sources in result order.
    pub sources: Vec<WebSource>,
    /// Whether the caller cut the source list.
    pub truncated: bool,
}

/// DeepSeek-backed search provider.
pub struct DeepSeekSearchProvider {
    client: reqwest::Client,
    config: DeepSeekSearchConfig,
}

impl DeepSeekSearchProvider {
    /// Build the provider with a redirect-refusing client.
    /// @param config - Resolved endpoint, key, and request bounds.
    /// @returns The provider, or a coded failure when the client cannot be built.
    pub fn new(config: DeepSeekSearchConfig) -> Result<Self, WebError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| {
                WebError::new(
                    WebErrorCode::ProviderError,
                    format!("cannot build the HTTP client: {error}"),
                )
            })?;
        Ok(DeepSeekSearchProvider { client, config })
    }

    /// Search one query.
    /// @param query - Search query.
    /// @param cancel - Turn cancellation shared with the tool call.
    /// @returns Deduped sources, or a coded failure.
    pub async fn search(&self, query: &str, cancel: &Cancel) -> Result<WebSearchResult, WebError> {
        if cancel.is_cancelled() {
            return Err(WebError::new(
                WebErrorCode::Aborted,
                "DeepSeek search aborted",
            ));
        }
        let deadline = self.config.timeout;
        tokio::select! {
            () = cancel.cancelled() => Err(WebError::new(WebErrorCode::Aborted, "DeepSeek search aborted")),
            outcome = tokio::time::timeout(deadline, self.dispatch(query)) => match outcome {
                Ok(result) => result,
                Err(_) => Err(WebError::new(WebErrorCode::FetchTimeout, "DeepSeek search timed out")),
            },
        }
    }

    /// The exact secret-free request body for one query.
    /// @param query - Search query.
    /// @returns The Messages request body.
    #[must_use]
    pub fn request_body(&self, query: &str) -> Value {
        json!({
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": format!("Perform a web search for the query: {query}")}],
            }],
            "tools": [{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": self.config.max_uses,
            }],
            "thinking": {"type": "disabled"},
        })
    }

    async fn dispatch(&self, query: &str) -> Result<WebSearchResult, WebError> {
        let endpoint = self.config.endpoint();
        let response = self
            .client
            .post(&endpoint)
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", &self.config.api_version)
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("user-agent", DEFAULT_SEARCH_USER_AGENT)
            .json(&self.request_body(query))
            .send()
            .await
            .map_err(|error| {
                WebError::new(
                    WebErrorCode::ProviderError,
                    format!("DeepSeek search request to {endpoint} failed: {error}"),
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            let detail = error_detail(&detail);
            return Err(WebError::new(
                WebErrorCode::ProviderError,
                format!("DeepSeek API error (HTTP {})", status.as_u16()) + &detail,
            ));
        }
        let body: Value = response.json().await.map_err(|error| {
            WebError::new(
                WebErrorCode::ProviderError,
                format!("DeepSeek search response is not JSON: {error}"),
            )
        })?;
        map_response(&body)
    }
}

/// Read one human-readable detail from a provider error body.
fn error_detail(body: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return String::new();
    };
    let detail = value
        .get("error")
        .and_then(|error| {
            error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.as_str())
        })
        .or_else(|| value.get("message").and_then(Value::as_str));
    match detail {
        Some(detail) if !detail.is_empty() => format!(": {detail}"),
        _ => String::new(),
    }
}

/// Map a Messages response to deduped sources.
/// @param body - Parsed response body.
/// @returns The normalized result, or a coded failure when no result block came back.
pub fn map_response(body: &Value) -> Result<WebSearchResult, WebError> {
    let blocks = body
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut cited: Vec<(String, String)> = Vec::new();
    for block in &blocks {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        let Some(citations) = block.get("citations").and_then(Value::as_array) else {
            continue;
        };
        for citation in citations {
            let url = citation
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let text = citation
                .get("cited_text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if url.is_empty() || text.is_empty() {
                continue;
            }
            if cited.iter().any(|(known, _)| known == url) {
                continue;
            }
            cited.push((url.to_string(), text.to_string()));
        }
    }
    let mut seen: Vec<String> = Vec::new();
    let mut sources = Vec::new();
    let mut any_result_block = false;
    for block in &blocks {
        if block.get("type").and_then(Value::as_str) != Some("web_search_tool_result") {
            continue;
        }
        any_result_block = true;
        let Some(items) = block.get("content").and_then(Value::as_array) else {
            continue;
        };
        for item in items {
            if item.get("type").and_then(Value::as_str) != Some("web_search_result") {
                continue;
            }
            let url = item.get("url").and_then(Value::as_str).unwrap_or_default();
            if url.is_empty() || seen.iter().any(|known| known == url) {
                continue;
            }
            seen.push(url.to_string());
            let non_empty = |key: &str| {
                item.get(key)
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            };
            let snippet = non_empty("snippet").or_else(|| {
                cited
                    .iter()
                    .find(|(known, _)| known == url)
                    .map(|(_, text)| text.clone())
            });
            sources.push(WebSource {
                url: url.to_string(),
                title: non_empty("title"),
                snippet,
                summary: None,
                published_at: non_empty("page_age"),
            });
        }
    }
    if !any_result_block {
        return Err(WebError::new(
            WebErrorCode::ProviderError,
            "DeepSeek returned no web_search_tool_result blocks; the request may not have triggered native web search",
        ));
    }
    Ok(WebSearchResult {
        content: None,
        sources,
        truncated: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Serve one canned JSON response and capture the request body.
    async fn serve(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buffer = vec![0u8; 65536];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let _ = tx.send(request);
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
        (format!("http://{address}"), rx)
    }

    fn provider(base: &str) -> DeepSeekSearchProvider {
        let mut config = DeepSeekSearchConfig::new("test-key");
        config.base_url = base.to_string();
        DeepSeekSearchProvider::new(config).unwrap()
    }

    #[tokio::test]
    async fn search_returns_deduped_sources_and_sends_the_native_tool() {
        let body = r#"{"content":[{"type":"text","text":"answer","citations":[{"url":"https://b.example","cited_text":"cited b"}]},{"type":"web_search_tool_result","content":[{"type":"web_search_result","url":"https://a.example","title":"A","snippet":"snip a","page_age":"2 days"},{"type":"web_search_result","url":"https://b.example","title":"B"},{"type":"web_search_result","url":"https://a.example","title":"A again"}]}]}"#;
        let (base, request) = serve(200, body).await;
        let result = provider(&base)
            .search("rust async", &Cancel::default())
            .await
            .unwrap();
        assert_eq!(result.sources.len(), 2);
        assert_eq!(result.sources[0].url, "https://a.example");
        assert_eq!(result.sources[0].snippet.as_deref(), Some("snip a"));
        assert_eq!(result.sources[0].published_at.as_deref(), Some("2 days"));
        assert_eq!(result.sources[1].snippet.as_deref(), Some("cited b"));
        let request = request.await.unwrap();
        assert!(request.contains("web_search_20250305"), "{request}");
        assert!(
            request.contains("Perform a web search for the query: rust async"),
            "{request}"
        );
        assert!(request.contains("x-api-key: test-key"), "{request}");
    }

    #[tokio::test]
    async fn a_response_without_result_blocks_is_an_error() {
        let (base, _) = serve(200, r#"{"content":[{"type":"text","text":"no search"}]}"#).await;
        let error = provider(&base)
            .search("q", &Cancel::default())
            .await
            .unwrap_err();
        assert_eq!(error.code, WebErrorCode::ProviderError);
        assert!(
            error.message.contains("no web_search_tool_result"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn a_http_failure_names_the_status_and_detail() {
        let (base, _) = serve(401, r#"{"error":{"message":"invalid api key"}}"#).await;
        let error = provider(&base)
            .search("q", &Cancel::default())
            .await
            .unwrap_err();
        assert_eq!(error.code, WebErrorCode::ProviderError);
        assert!(error.message.contains("HTTP 401"), "{}", error.message);
        assert!(
            error.message.contains("invalid api key"),
            "{}",
            error.message
        );
    }
}
