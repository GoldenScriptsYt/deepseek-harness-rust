use crate::AttachmentError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Default maximum encoded bytes for one verbatim file.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// Default maximum encoded bytes for one submitted image.
pub const DEFAULT_MAX_IMAGE_BYTES: u64 = 50 * 1024 * 1024;
/// Default maximum images in one prompt.
pub const DEFAULT_MAX_IMAGES_PER_MESSAGE: usize = 20;
/// Default maximum aggregate image bytes in one prompt.
pub const DEFAULT_MAX_MESSAGE_IMAGE_BYTES: u64 = 200 * 1024 * 1024;
/// Default maximum decoded pixels in one image.
pub const DEFAULT_MAX_IMAGE_PIXELS: u64 = 64_000_000;
/// Default maximum intrinsic width or height of one image.
pub const DEFAULT_MAX_IMAGE_DIMENSION: u32 = 65_535;

/// Deployment-resolved upload and prompt limits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentLimits {
    pub max_file_bytes: u64,
    pub max_image_bytes: u64,
    pub max_images_per_message: usize,
    pub max_message_image_bytes: u64,
    pub max_image_pixels: u64,
    pub max_image_dimension: u32,
}

impl Default for AttachmentLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_image_bytes: DEFAULT_MAX_IMAGE_BYTES,
            max_images_per_message: DEFAULT_MAX_IMAGES_PER_MESSAGE,
            max_message_image_bytes: DEFAULT_MAX_MESSAGE_IMAGE_BYTES,
            max_image_pixels: DEFAULT_MAX_IMAGE_PIXELS,
            max_image_dimension: DEFAULT_MAX_IMAGE_DIMENSION,
        }
    }
}

impl AttachmentLimits {
    pub(crate) fn validate(&self) -> Result<(), AttachmentError> {
        if self.max_file_bytes == 0 || usize::try_from(self.max_file_bytes).is_err() {
            return Err(AttachmentError::InvalidLimits {
                reason: "max_file_bytes must be a positive platform-sized integer",
            });
        }
        if self.max_image_bytes == 0 || usize::try_from(self.max_image_bytes).is_err() {
            return Err(AttachmentError::InvalidLimits {
                reason: "max_image_bytes must be a positive platform-sized integer",
            });
        }
        if self.max_images_per_message == 0 {
            return Err(AttachmentError::InvalidLimits {
                reason: "max_images_per_message must be positive",
            });
        }
        if self.max_message_image_bytes == 0 {
            return Err(AttachmentError::InvalidLimits {
                reason: "max_message_image_bytes must be positive",
            });
        }
        if self.max_image_pixels == 0 {
            return Err(AttachmentError::InvalidLimits {
                reason: "max_image_pixels must be positive",
            });
        }
        if self.max_image_dimension == 0 {
            return Err(AttachmentError::InvalidLimits {
                reason: "max_image_dimension must be positive",
            });
        }
        Ok(())
    }
}

/// Raster formats accepted by the attachment store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Return the MIME string used on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
            Self::Gif => "image/gif",
        }
    }
}

impl std::fmt::Display for ImageMediaType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ImageMediaType {
    type Error = AttachmentError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "image/png" => Ok(Self::Png),
            "image/jpeg" => Ok(Self::Jpeg),
            "image/webp" => Ok(Self::Webp),
            "image/gif" => Ok(Self::Gif),
            _ => Err(AttachmentError::UnsupportedImageType {
                media_type: value.to_owned(),
            }),
        }
    }
}

impl FromStr for ImageMediaType {
    type Err = AttachmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

/// Intrinsic image dimensions in pixels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageDimensions {
    pub width: u32,
    pub height: u32,
}

/// Durable serializable reference to one immutable image object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageAttachmentRef {
    pub attachment_id: String,
    pub media_type: ImageMediaType,
    pub bytes: u64,
    pub width: u32,
    pub height: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_dimensions: Option<ImageDimensions>,
}

/// Durable serializable reference to one verbatim file object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileAttachmentRef {
    pub attachment_id: String,
    pub name: String,
    pub bytes: u64,
}

/// Verified stored image bytes and their reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredImageAttachment {
    pub reference: ImageAttachmentRef,
    pub data: Vec<u8>,
}

/// One decoded image proposed for storage.
#[derive(Clone, Copy, Debug)]
pub struct SaveImageAttachment<'a> {
    pub data: &'a [u8],
    pub media_type: ImageMediaType,
    pub name: Option<&'a str>,
}

/// Attachment named by an active or revoked durable upload receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum StagedAttachment {
    File {
        attachment: FileAttachmentRef,
    },
    Image {
        attachment: ImageAttachmentRef,
        encoded_bytes: u64,
    },
}

/// Lifecycle state persisted with an upload receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReceiptState {
    Active,
    Revoked,
}

/// Durable authority for one uploaded file or image.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UploadReceipt {
    pub id: String,
    pub state: ReceiptState,
    pub attachment: StagedAttachment,
}

/// Browser prompt input before receipt resolution and image persistence.
#[derive(Clone, Debug)]
pub enum PromptContentPart<'a> {
    Text {
        text: &'a str,
    },
    Image {
        media_type: ImageMediaType,
        data: &'a str,
        name: Option<&'a str>,
    },
    FileReceipt {
        receipt_id: &'a str,
    },
    File {
        attachment: FileAttachmentRef,
    },
    StoredImage {
        attachment: ImageAttachmentRef,
        encoded_bytes: u64,
    },
}

/// Prompt content whose uploads have become durable references.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum AdmittedPromptContentPart {
    Text { text: String },
    Image { attachment: ImageAttachmentRef },
    File { attachment: FileAttachmentRef },
}
