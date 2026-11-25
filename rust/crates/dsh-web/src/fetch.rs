//! Anonymous HTTP(S) fetch provider: manual same-origin redirects, byte and
//! character caps, declared-charset decoding.

use crate::error::{WebError, WebErrorCode};
use crate::policy::{
    classify_content_type, decode_body, decoder_for_charset, is_same_origin, parse_charset,
    validate_fetch_url, FetchableKind,
};
use dsh_core::Cancel;
use std::time::Duration;
use url::Url;

/// Default read cap, in bytes.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 5_000_000;
/// Default decoded body cap, in characters.
pub const DEFAULT_MAX_BODY_CHARS: usize = 100_000;
/// Default per-fetch deadline.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default same-origin redirect budget.
pub const DEFAULT_MAX_REDIRECTS: usize = 5;
/// Attribution header sent on every request.
pub const DEFAULT_USER_AGENT: &str = "deepseek-harness/0.0.1 (+https://github.com/deepseek-ai)";

const ACCEPT: &str = "text/html,application/xhtml+xml,text/*;q=0.9,application/json;q=0.8";

/// Resolved transport and response limits.
#[derive(Clone, Debug)]
pub struct HttpFetchLimits {
    /// Maximum response body size in bytes.
    pub max_response_bytes: usize,
    /// Maximum decoded body length in characters.
    pub max_body_chars: usize,
    /// Default fetch deadline.
    pub timeout: Duration,
    /// Maximum same-origin redirect hops.
    pub max_redirects: usize,
    /// `User-Agent` header value.
    pub user_agent: String,
}

impl Default for HttpFetchLimits {
    fn default() -> Self {
        HttpFetchLimits {
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_body_chars: DEFAULT_MAX_BODY_CHARS,
            timeout: DEFAULT_TIMEOUT,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            user_agent: DEFAULT_USER_AGENT.to_string(),
        }
    }
}

/// One decoded response body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebFetchBody {
    /// Classified body kind.
    pub kind: FetchableKind,
    /// Decoded text, capped by the limits.
    pub content: String,
}

/// One fetch outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebFetchResult {
    /// Final URL after same-origin redirects.
    pub url: String,
    /// Final HTTP status.
    pub status_code: u16,
    /// Decoded body.
    pub body: WebFetchBody,
    /// Whether the character cap cut the body.
    pub truncated: bool,
}

/// Anonymous HTTP(S) fetcher.
pub struct HttpFetchProvider {
    client: reqwest::Client,
    limits: HttpFetchLimits,
}

impl HttpFetchProvider {
    /// Build the provider with a redirect-disabled client.
    /// @param limits - Transport and response limits.
    /// @returns The provider, or a coded failure when the client cannot be built.
    pub fn new(limits: HttpFetchLimits) -> Result<Self, WebError> {
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
        Ok(HttpFetchProvider { client, limits })
    }

    /// Fetch one URL to a decoded body.
    /// @param url - Request URL.
    /// @param cancel - Turn cancellation shared with the tool call.
    /// @returns The decoded response, or a coded failure.
    pub async fn fetch(&self, url: &str, cancel: &Cancel) -> Result<WebFetchResult, WebError> {
        if cancel.is_cancelled() {
            return Err(WebError::new(WebErrorCode::Aborted, "web fetch aborted"));
        }
        let deadline = self.limits.timeout;
        tokio::select! {
            () = cancel.cancelled() => Err(WebError::new(WebErrorCode::Aborted, "web fetch aborted")),
            outcome = tokio::time::timeout(deadline, self.follow(url)) => match outcome {
                Ok(result) => result,
                Err(_) => Err(WebError::new(WebErrorCode::FetchTimeout, "web fetch timed out")),
            },
        }
    }

    async fn follow(&self, initial: &str) -> Result<WebFetchResult, WebError> {
        let mut current = validate_fetch_url(initial)?;
        let mut hops = 0usize;
        loop {
            let response = self
                .client
                .get(current.clone())
                .header("user-agent", &self.limits.user_agent)
                .header("accept", ACCEPT)
                .send()
                .await
                .map_err(network)?;
            let status = response.status().as_u16();
            if is_redirect(status) {
                if hops >= self.limits.max_redirects {
                    return Err(WebError::new(
                        WebErrorCode::RedirectBlocked,
                        format!(
                            "exceeded the maximum of {} redirects",
                            self.limits.max_redirects
                        ),
                    ));
                }
                let location = response
                    .headers()
                    .get("location")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let Some(location) = location else {
                    return Err(WebError::new(
                        WebErrorCode::ProviderError,
                        format!("redirect response (HTTP {status}) without a Location header"),
                    ));
                };
                let target = current.join(&location).map_err(|error| {
                    WebError::new(
                        WebErrorCode::InvalidUrl,
                        format!("invalid redirect target: {error}"),
                    )
                })?;
                let target = validate_fetch_url(target.as_str())?;
                if !is_same_origin(&target, &current) {
                    return Err(WebError::new(
                        WebErrorCode::RedirectBlocked,
                        format!("cross-origin redirect to {} is not followed automatically; retry against that URL directly", origin(&target)),
                    ));
                }
                current = target;
                hops += 1;
                continue;
            }
            return self.read(response, current).await;
        }
    }

    async fn read(
        &self,
        mut response: reqwest::Response,
        url: Url,
    ) -> Result<WebFetchResult, WebError> {
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let kind = classify_content_type(content_type.as_deref()).ok_or_else(|| {
            WebError::new(
                WebErrorCode::UnsupportedContentType,
                format!(
                    "unsupported content type \"{}\"",
                    content_type.as_deref().unwrap_or("unknown")
                ),
            )
        })?;
        let (encoding, strict) =
            decoder_for_charset(parse_charset(content_type.as_deref()).as_deref())?;
        if response
            .content_length()
            .is_some_and(|length| length as usize > self.limits.max_response_bytes)
        {
            return Err(WebError::new(
                WebErrorCode::FetchTooLarge,
                format!(
                    "response exceeds the maximum of {} bytes",
                    self.limits.max_response_bytes
                ),
            ));
        }
        let mut bytes: Vec<u8> = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(network)? {
            if bytes.len() + chunk.len() > self.limits.max_response_bytes {
                return Err(WebError::new(
                    WebErrorCode::FetchTooLarge,
                    format!(
                        "response exceeds the maximum of {} bytes",
                        self.limits.max_response_bytes
                    ),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let decoded = decode_body(encoding, strict, &bytes, false)?;
        let mut truncated = false;
        let content = if decoded.chars().count() > self.limits.max_body_chars {
            truncated = true;
            decoded.chars().take(self.limits.max_body_chars).collect()
        } else {
            decoded
        };
        Ok(WebFetchResult {
            url: url.to_string(),
            status_code: status,
            body: WebFetchBody { kind, content },
            truncated,
        })
    }
}

fn origin(url: &Url) -> String {
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().unwrap_or_default(),
        port
    )
}

fn is_redirect(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

fn network(error: reqwest::Error) -> WebError {
    if error.is_timeout() {
        return WebError::new(WebErrorCode::FetchTimeout, "web fetch timed out");
    }
    WebError::new(
        WebErrorCode::ProviderError,
        format!("web fetch failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Serve one canned response per accepted connection.
    async fn serve(body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request).await;
                let _ = socket.write_all(body.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{address}")
    }

    fn provider() -> HttpFetchProvider {
        HttpFetchProvider::new(HttpFetchLimits::default()).unwrap()
    }

    #[tokio::test]
    async fn a_text_response_is_decoded_and_capped() {
        let base = serve("HTTP/1.1 200 OK\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: 5\r\nconnection: close\r\n\r\nhello").await;
        let result = provider()
            .fetch(&format!("{base}/x"), &Cancel::default())
            .await
            .unwrap();
        assert_eq!(result.status_code, 200);
        assert_eq!(result.body.kind, FetchableKind::Text);
        assert_eq!(result.body.content, "hello");
        assert!(!result.truncated);

        let limits = HttpFetchLimits {
            max_body_chars: 2,
            ..HttpFetchLimits::default()
        };
        let capped = HttpFetchProvider::new(limits).unwrap();
        let result = capped
            .fetch(&format!("{base}/x"), &Cancel::default())
            .await
            .unwrap();
        assert_eq!(result.body.content, "he");
        assert!(result.truncated);
    }

    #[tokio::test]
    async fn an_unsupported_content_type_is_rejected() {
        let base = serve("HTTP/1.1 200 OK\r\ncontent-type: image/png\r\ncontent-length: 1\r\nconnection: close\r\n\r\nx").await;
        let error = provider()
            .fetch(&format!("{base}/x"), &Cancel::default())
            .await
            .unwrap_err();
        assert_eq!(error.code, WebErrorCode::UnsupportedContentType);
    }

    #[tokio::test]
    async fn a_cross_origin_redirect_is_refused() {
        let target = serve("HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok").await;
        let redirect = format!("HTTP/1.1 302 Found\r\nlocation: {target}/next\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
        let leaked: &'static str = Box::leak(redirect.into_boxed_str());
        let base = serve(leaked).await;
        let error = provider()
            .fetch(&format!("{base}/x"), &Cancel::default())
            .await
            .unwrap_err();
        assert_eq!(error.code, WebErrorCode::RedirectBlocked);
    }

    #[tokio::test]
    async fn a_same_origin_redirect_is_followed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for round in 0..2 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request).await;
                let response = if round == 0 {
                    format!("HTTP/1.1 302 Found\r\nlocation: http://{address}/final\r\ncontent-length: 0\r\nconnection: close\r\n\r\n", address = address)
                } else {
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 3\r\nconnection: close\r\n\r\nend".to_string()
                };
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        let result = provider()
            .fetch(&format!("http://{address}/start"), &Cancel::default())
            .await
            .unwrap();
        assert_eq!(result.body.content, "end");
        assert!(result.url.ends_with("/final"), "{}", result.url);
    }

    #[tokio::test]
    async fn an_oversized_body_is_rejected_before_reading() {
        let base = serve("HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 9999999\r\nconnection: close\r\n\r\n").await;
        let error = provider()
            .fetch(&format!("{base}/x"), &Cancel::default())
            .await
            .unwrap_err();
        assert_eq!(error.code, WebErrorCode::FetchTooLarge);
    }

    #[tokio::test]
    async fn a_cancelled_turn_never_dispatches() {
        let cancel = Cancel::default();
        cancel.cancel();
        let error = provider()
            .fetch("https://example.com", &cancel)
            .await
            .unwrap_err();
        assert_eq!(error.code, WebErrorCode::Aborted);
    }
}
