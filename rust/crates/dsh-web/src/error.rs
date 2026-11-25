//! Web failure codes shared by the fetch and search providers.

use std::fmt;

/// Stable failure codes the web tools report to the model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebErrorCode {
    /// The URL is malformed, unsupported, or over the length cap.
    InvalidUrl,
    /// The URL carries credentials or is not a fetchable destination.
    BlockedUrl,
    /// A redirect was refused: cross-origin, over the hop cap, or missing `Location`.
    RedirectBlocked,
    /// The declared or streamed body exceeds the byte cap.
    FetchTooLarge,
    /// The body is not a decodable text type.
    UnsupportedContentType,
    /// The fetch deadline fired.
    FetchTimeout,
    /// The caller cancelled the operation.
    Aborted,
    /// The provider failed or returned an unusable response.
    ProviderError,
    /// The provider has no credential to dispatch with.
    CredentialMissing,
}

impl WebErrorCode {
    /// Wire code carried in diagnostics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidUrl => "WEB_INVALID_URL",
            Self::BlockedUrl => "WEB_BLOCKED_URL",
            Self::RedirectBlocked => "WEB_REDIRECT_BLOCKED",
            Self::FetchTooLarge => "WEB_FETCH_TOO_LARGE",
            Self::UnsupportedContentType => "WEB_UNSUPPORTED_CONTENT_TYPE",
            Self::FetchTimeout => "WEB_FETCH_TIMEOUT",
            Self::Aborted => "WEB_ABORTED",
            Self::ProviderError => "WEB_PROVIDER_ERROR",
            Self::CredentialMissing => "WEB_PROVIDER_CREDENTIAL_MISSING",
        }
    }
}

/// One web operation failure with its stable code.
#[derive(Clone, Debug)]
pub struct WebError {
    /// Failure code the model sees.
    pub code: WebErrorCode,
    /// Actionable explanation.
    pub message: String,
}

impl WebError {
    /// Build a failure.
    /// @param code - Stable failure code.
    /// @param message - Model-facing explanation.
    #[must_use]
    pub fn new(code: WebErrorCode, message: impl Into<String>) -> Self {
        WebError {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for WebError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for WebError {}
