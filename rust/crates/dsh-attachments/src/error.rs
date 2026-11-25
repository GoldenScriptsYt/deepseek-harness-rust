use image::ImageError;
use serde_json::Error as JsonError;
use std::io;

/// Stable machine-routing codes for attachment failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AttachmentErrorCode {
    TooManyImages,
    ImagesTooLarge,
    UnsupportedImageType,
    InvalidImageBase64,
    InvalidImage,
    ImageTypeMismatch,
    ImageTooLarge,
    ImageTooManyPixels,
    ImageDimensionTooLarge,
    InvalidFileBase64,
    FileTooLarge,
    InvalidAttachmentRef,
    AttachmentCorrupt,
    AttachmentWriteFailed,
    AttachmentNotFound,
    AttachmentReadFailed,
    AttachmentConfigInvalid,
    AttachmentRootInvalid,
}

impl AttachmentErrorCode {
    /// Return the stable uppercase protocol code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TooManyImages => "TOO_MANY_IMAGES",
            Self::ImagesTooLarge => "IMAGES_TOO_LARGE",
            Self::UnsupportedImageType => "UNSUPPORTED_IMAGE_TYPE",
            Self::InvalidImageBase64 => "INVALID_IMAGE_BASE64",
            Self::InvalidImage => "INVALID_IMAGE",
            Self::ImageTypeMismatch => "IMAGE_TYPE_MISMATCH",
            Self::ImageTooLarge => "IMAGE_TOO_LARGE",
            Self::ImageTooManyPixels => "IMAGE_TOO_MANY_PIXELS",
            Self::ImageDimensionTooLarge => "IMAGE_DIMENSION_TOO_LARGE",
            Self::InvalidFileBase64 => "INVALID_FILE_BASE64",
            Self::FileTooLarge => "FILE_TOO_LARGE",
            Self::InvalidAttachmentRef => "INVALID_ATTACHMENT_REF",
            Self::AttachmentCorrupt => "ATTACHMENT_CORRUPT",
            Self::AttachmentWriteFailed => "ATTACHMENT_WRITE_FAILED",
            Self::AttachmentNotFound => "ATTACHMENT_NOT_FOUND",
            Self::AttachmentReadFailed => "ATTACHMENT_READ_FAILED",
            Self::AttachmentConfigInvalid => "ATTACHMENT_CONFIG_INVALID",
            Self::AttachmentRootInvalid => "ATTACHMENT_ROOT_INVALID",
        }
    }
}

/// Structured attachment validation, receipt, and storage failures.
#[derive(Debug, thiserror::Error)]
pub enum AttachmentError {
    #[error("invalid attachment store limits: {reason}")]
    InvalidLimits { reason: &'static str },

    #[error("attachment storage root is unsafe: {reason}")]
    InvalidRoot { reason: &'static str },

    #[error("unable to open attachment storage: {source}")]
    StoreOpenFailed { source: io::Error },

    #[error("attachment reference is invalid")]
    InvalidAttachmentRef,

    #[error("attachment receipt does not exist")]
    ReceiptNotFound,

    #[error("attachment receipt has been revoked")]
    ReceiptRevoked,

    #[error("stored attachment is corrupt: {reason}")]
    AttachmentCorrupt { reason: &'static str },

    #[error("attachment receipt data is invalid: {source}")]
    ReceiptDataInvalid { source: JsonError },

    #[error("unable to encode attachment receipt: {source}")]
    ReceiptEncodingFailed { source: JsonError },

    #[error("stored attachment does not exist: {id}")]
    AttachmentNotFound { id: String },

    #[error("unable to read attachment: {source}")]
    AttachmentReadFailed { source: io::Error },

    #[error("unable to {operation} attachment: {source}")]
    AttachmentWriteFailed {
        operation: &'static str,
        source: io::Error,
    },

    #[error("image upload is not canonical base64")]
    InvalidImageBase64,

    #[error("file upload is not canonical base64")]
    InvalidFileBase64,

    #[error("file exceeds the configured {limit}-byte limit")]
    FileTooLarge { limit: u64 },

    #[error("image exceeds the configured {limit}-byte limit")]
    ImageTooLarge { limit: u64 },

    #[error("image batch exceeds the configured {limit}-image limit")]
    TooManyImages { limit: usize },

    #[error("image batch exceeds the configured {limit}-byte limit")]
    ImagesTooLarge { limit: u64 },

    #[error("image media type {media_type} is not supported")]
    UnsupportedImageType { media_type: String },

    #[error("image data is invalid: {reason}")]
    InvalidImageData { reason: &'static str },

    #[error("image format {format} is not supported")]
    UnsupportedImageFormat { format: String },

    #[error("unable to decode image: {source}")]
    InvalidImage { source: ImageError },

    #[error("declared image type {declared} does not match detected type {detected}")]
    ImageTypeMismatch {
        declared: crate::ImageMediaType,
        detected: crate::ImageMediaType,
    },

    #[error("image {width}x{height} exceeds the configured {limit}-pixel limit")]
    ImageTooManyPixels { width: u32, height: u32, limit: u64 },

    #[error("image dimension {width}x{height} exceeds the configured {limit}-pixel side limit")]
    ImageDimensionTooLarge { width: u32, height: u32, limit: u32 },
}

impl AttachmentError {
    /// Return the stable machine-routing code for this failure.
    pub const fn code(&self) -> AttachmentErrorCode {
        match self {
            Self::InvalidLimits { .. } => AttachmentErrorCode::AttachmentConfigInvalid,
            Self::InvalidRoot { .. } => AttachmentErrorCode::AttachmentRootInvalid,
            Self::StoreOpenFailed { .. } => AttachmentErrorCode::AttachmentRootInvalid,
            Self::InvalidAttachmentRef => AttachmentErrorCode::InvalidAttachmentRef,
            Self::ReceiptNotFound => AttachmentErrorCode::AttachmentNotFound,
            Self::ReceiptRevoked => AttachmentErrorCode::AttachmentNotFound,
            Self::AttachmentCorrupt { .. } | Self::ReceiptDataInvalid { .. } => {
                AttachmentErrorCode::AttachmentCorrupt
            }
            Self::ReceiptEncodingFailed { .. } => AttachmentErrorCode::AttachmentWriteFailed,
            Self::AttachmentNotFound { .. } => AttachmentErrorCode::AttachmentNotFound,
            Self::AttachmentReadFailed { .. } => AttachmentErrorCode::AttachmentReadFailed,
            Self::AttachmentWriteFailed { .. } => AttachmentErrorCode::AttachmentWriteFailed,
            Self::InvalidImageBase64 => AttachmentErrorCode::InvalidImageBase64,
            Self::InvalidFileBase64 => AttachmentErrorCode::InvalidFileBase64,
            Self::FileTooLarge { .. } => AttachmentErrorCode::FileTooLarge,
            Self::ImageTooLarge { .. } => AttachmentErrorCode::ImageTooLarge,
            Self::TooManyImages { .. } => AttachmentErrorCode::TooManyImages,
            Self::ImagesTooLarge { .. } => AttachmentErrorCode::ImagesTooLarge,
            Self::UnsupportedImageType { .. } => AttachmentErrorCode::UnsupportedImageType,
            Self::InvalidImageData { .. }
            | Self::UnsupportedImageFormat { .. }
            | Self::InvalidImage { .. } => AttachmentErrorCode::InvalidImage,
            Self::ImageTypeMismatch { .. } => AttachmentErrorCode::ImageTypeMismatch,
            Self::ImageTooManyPixels { .. } => AttachmentErrorCode::ImageTooManyPixels,
            Self::ImageDimensionTooLarge { .. } => AttachmentErrorCode::ImageDimensionTooLarge,
        }
    }

    /// Whether the caller can correct the proposed image content or batch.
    pub const fn is_image_admission_error(&self) -> bool {
        matches!(
            self.code(),
            AttachmentErrorCode::TooManyImages
                | AttachmentErrorCode::ImagesTooLarge
                | AttachmentErrorCode::UnsupportedImageType
                | AttachmentErrorCode::InvalidImageBase64
                | AttachmentErrorCode::InvalidImage
                | AttachmentErrorCode::ImageTypeMismatch
                | AttachmentErrorCode::ImageTooLarge
                | AttachmentErrorCode::ImageTooManyPixels
                | AttachmentErrorCode::ImageDimensionTooLarge
        )
    }
}
