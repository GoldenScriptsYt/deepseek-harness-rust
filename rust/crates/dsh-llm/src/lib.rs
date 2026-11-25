//! Provider-neutral model types. Port of the data types in `packages/llm/llm`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub mod retry;

pub use retry::{RetryPolicy, RETRY_CODE_CHOICES};

/// Stable failure classes, matching the TypeScript `LlmError` codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    MalformedResponse,
    UnsupportedContent,
    ContentRefused,
    EmptyResponse,
    InvalidRequest,
    InvalidResponse,
    Timeout,
    Aborted,
    Transport,
    StreamClosed,
    FilesApi,
    MissingCredential,
    InvalidCredential,
    UnknownModel,
    ModelUnsupported,
    ClientUpgradeRequired,
    ImageOffloadRequired,
    Auth,
    AccountQuota,
    Quota,
    RateLimit,
    ContextWindowExceeded,
    Server,
    Unknown,
    /// An otherwise unclassified provider HTTP status.
    Http(u16),
}

impl ErrorCode {
    /// Failure classes exposed as retry-policy choices. Retryability is policy-selected.
    pub const fn is_retry_choice(self) -> bool {
        matches!(
            self,
            Self::EmptyResponse
                | Self::MalformedResponse
                | Self::Quota
                | Self::RateLimit
                | Self::Server
                | Self::StreamClosed
                | Self::Timeout
                | Self::Transport
        )
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&match self {
            Self::MalformedResponse => "MALFORMED_RESPONSE".to_string(),
            Self::UnsupportedContent => "UNSUPPORTED_CONTENT".to_string(),
            Self::ContentRefused => "CONTENT_REFUSED".to_string(),
            Self::EmptyResponse => "EMPTY_RESPONSE".to_string(),
            Self::InvalidRequest => "INVALID_REQUEST".to_string(),
            Self::InvalidResponse => "INVALID_RESPONSE".to_string(),
            Self::Timeout => "TIMEOUT".to_string(),
            Self::Aborted => "ABORTED".to_string(),
            Self::Transport => "TRANSPORT".to_string(),
            Self::StreamClosed => "STREAM_CLOSED".to_string(),
            Self::FilesApi => "FILES_API".to_string(),
            Self::MissingCredential => "MISSING_CREDENTIAL".to_string(),
            Self::InvalidCredential => "INVALID_CREDENTIAL".to_string(),
            Self::UnknownModel => "UNKNOWN_MODEL".to_string(),
            Self::ModelUnsupported => "MODEL_UNSUPPORTED".to_string(),
            Self::ClientUpgradeRequired => "CLIENT_UPGRADE_REQUIRED".to_string(),
            Self::ImageOffloadRequired => "IMAGE_OFFLOAD_REQUIRED".to_string(),
            Self::Auth => "AUTH".to_string(),
            Self::AccountQuota => "ACCOUNT_QUOTA".to_string(),
            Self::Quota => "QUOTA".to_string(),
            Self::RateLimit => "RATE_LIMIT".to_string(),
            Self::ContextWindowExceeded => "CONTEXT_WINDOW_EXCEEDED".to_string(),
            Self::Server => "SERVER".to_string(),
            Self::Unknown => "UNKNOWN".to_string(),
            Self::Http(status) => format!("HTTP_{status}"),
        })
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "MALFORMED_RESPONSE" => Self::MalformedResponse,
            "UNSUPPORTED_CONTENT" => Self::UnsupportedContent,
            "CONTENT_REFUSED" => Self::ContentRefused,
            "EMPTY_RESPONSE" => Self::EmptyResponse,
            "INVALID_REQUEST" => Self::InvalidRequest,
            "INVALID_RESPONSE" => Self::InvalidResponse,
            "TIMEOUT" => Self::Timeout,
            "ABORTED" => Self::Aborted,
            "TRANSPORT" => Self::Transport,
            "STREAM_CLOSED" => Self::StreamClosed,
            "FILES_API" => Self::FilesApi,
            "MISSING_CREDENTIAL" => Self::MissingCredential,
            "INVALID_CREDENTIAL" => Self::InvalidCredential,
            "UNKNOWN_MODEL" => Self::UnknownModel,
            "MODEL_UNSUPPORTED" => Self::ModelUnsupported,
            "CLIENT_UPGRADE_REQUIRED" => Self::ClientUpgradeRequired,
            "IMAGE_OFFLOAD_REQUIRED" => Self::ImageOffloadRequired,
            "AUTH" => Self::Auth,
            "ACCOUNT_QUOTA" => Self::AccountQuota,
            "QUOTA" => Self::Quota,
            "RATE_LIMIT" => Self::RateLimit,
            "CONTEXT_WINDOW_EXCEEDED" => Self::ContextWindowExceeded,
            "SERVER" => Self::Server,
            "UNKNOWN" => Self::Unknown,
            _ => value
                .strip_prefix("HTTP_")
                .and_then(|status| status.parse::<u16>().ok())
                .map(Self::Http)
                .unwrap_or(Self::Unknown),
        })
    }
}

/// A model-layer failure.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct LlmError {
    pub code: ErrorCode,
    pub message: String,
    /// HTTP status when the failure preceded streaming.
    pub status: Option<u16>,
    /// Provider-requested wait before retrying.
    pub retry_after_ms: Option<u64>,
    /// Opaque provider-issued request identifier.
    pub request_id: Option<String>,
    /// Provider type/code/message retained for routing and diagnostics.
    pub provider_detail: Option<String>,
    /// Additional oldest image occurrences that need offloading.
    pub offload_images: Option<u64>,
}

impl LlmError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        LlmError {
            code,
            message: message.into(),
            status: None,
            retry_after_ms: None,
            request_id: None,
            provider_detail: None,
            offload_images: None,
        }
    }

    pub fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }

    pub fn with_retry_after(mut self, milliseconds: u64) -> Self {
        self.retry_after_ms = Some(milliseconds);
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_provider_detail(mut self, detail: impl Into<String>) -> Self {
        self.provider_detail = Some(detail.into());
        self
    }

    pub fn with_offload_images(mut self, count: u64) -> Self {
        self.offload_images = Some(count);
        self
    }
}

/// Who authored a history message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    Developer,
    User,
    Assistant,
    Tool,
}

/// Raster media types accepted by the Messages image protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageMediaType {
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/webp")]
    Webp,
    #[serde(rename = "image/gif")]
    Gif,
}

impl ImageMediaType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
            Self::Gif => "image/gif",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "image/png" => Some(Self::Png),
            "image/jpeg" => Some(Self::Jpeg),
            "image/webp" => Some(Self::Webp),
            "image/gif" => Some(Self::Gif),
            _ => None,
        }
    }
}

/// Intrinsic dimensions of a normalized image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageDimensions {
    pub width: u64,
    pub height: u64,
}

/// Durable image metadata; bytes remain owned by the attachment service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageAttachmentRef {
    pub attachment_id: String,
    pub media_type: ImageMediaType,
    pub bytes: u64,
    pub width: u64,
    pub height: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_dimensions: Option<ImageDimensions>,
}

/// Exact immutable bytes prepared for one request, shared without copying image data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestImage {
    pub variant_id: String,
    pub attachment: ImageAttachmentRef,
    pub data: Arc<[u8]>,
    pub media_type: ImageMediaType,
    pub bytes: u64,
    pub width: u64,
    pub height: u64,
}

/// A durable verbatim file reference. Providers only receive its projected handle text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileAttachmentRef {
    pub attachment_id: String,
    pub name: String,
    pub bytes: u64,
}

/// One block of message content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Content {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
        /// Provider replay signature, kept so thinking can be sent back verbatim.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    Image {
        attachment: ImageAttachmentRef,
        /// Set after a durable request-limit offload decision.
        #[serde(default, skip_serializing_if = "is_false")]
        offloaded: bool,
    },
    File {
        attachment: FileAttachmentRef,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: String,
    },
    ToolAddition {
        name: String,
    },
    ToolRemoval {
        name: String,
    },
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// One provider-neutral history message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub role: Role,
    pub content: Vec<Content>,
    /// Set on `Role::Tool` messages: the call this result answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}

/// Why generation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FinishReason {
    Stop,
    ToolCalls,
    MaxTokens,
}

/// Token accounting for one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub total_tokens: u64,
}

/// Kind of a streamed content block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    Text,
    Reasoning,
    ToolCall,
}

/// One incremental stream event.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamChunk {
    BlockStart {
        index: usize,
        kind: BlockKind,
    },
    TextDelta {
        index: usize,
        text: String,
    },
    ReasoningDelta {
        index: usize,
        text: String,
    },
    ToolCallDelta {
        index: usize,
        id: Option<String>,
        name: Option<String>,
        arguments_delta: String,
    },
    BlockEnd {
        index: usize,
    },
    Usage(TokenUsage),
    /// End of stream. `replay` holds one entry per block, in order (reasoning signatures).
    Finish {
        reason: FinishReason,
        replay: Vec<Option<String>>,
    },
}

/// A callable tool as presented to a model.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Provider model modality vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelModality {
    Text,
    Image,
}

/// Selector badge display emphasis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelBadgeTone {
    Neutral,
    Info,
    Success,
    Warning,
}

/// Literal or English-required localized badge text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ModelBadgeText {
    Literal(String),
    Localized(BTreeMap<String, String>),
}

/// Provider-owned selector label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelBadge {
    pub text: ModelBadgeText,
    pub tone: ModelBadgeTone,
}

/// Display metadata for one registered provider model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelInfo {
    pub provider: String,
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_modalities: Option<Vec<ModelModality>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub badge: Option<ModelBadge>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn error_codes_keep_dynamic_http_status_as_a_string() {
        assert_eq!(
            serde_json::to_value(ErrorCode::StreamClosed).unwrap(),
            json!("STREAM_CLOSED")
        );
        assert_eq!(
            serde_json::to_value(ErrorCode::Http(418)).unwrap(),
            json!("HTTP_418")
        );
        assert_eq!(
            serde_json::from_value::<ErrorCode>(json!("HTTP_429")).unwrap(),
            ErrorCode::Http(429)
        );
        assert_eq!(
            serde_json::from_value::<ErrorCode>(json!("not-a-code")).unwrap(),
            ErrorCode::Unknown
        );
    }

    #[test]
    fn image_content_round_trips_without_request_bytes() {
        let content = Content::Image {
            attachment: ImageAttachmentRef {
                attachment_id: "sha256:a".into(),
                media_type: ImageMediaType::Png,
                bytes: 3,
                width: 1,
                height: 1,
                name: None,
                original_dimensions: None,
            },
            offloaded: false,
        };
        let restored: Content =
            serde_json::from_value(serde_json::to_value(&content).unwrap()).unwrap();
        assert_eq!(restored, content);
    }
}
