//! Durable, provider-neutral attachment storage and prompt admission.
//!
//! Objects are content-addressed beneath an explicit root. Upload receipts are
//! separately persisted, checksummed, and revocable; revocation never deletes
//! shared immutable objects.

mod error;
mod fs;
mod image;
mod store;
mod types;

pub use error::{AttachmentError, AttachmentErrorCode};
pub use store::AttachmentStore;
pub use types::{
    AdmittedPromptContentPart, AttachmentLimits, FileAttachmentRef, ImageAttachmentRef,
    ImageDimensions, ImageMediaType, PromptContentPart, ReceiptState, SaveImageAttachment,
    StagedAttachment, StoredImageAttachment, UploadReceipt, DEFAULT_MAX_FILE_BYTES,
    DEFAULT_MAX_IMAGES_PER_MESSAGE, DEFAULT_MAX_IMAGE_BYTES, DEFAULT_MAX_IMAGE_DIMENSION,
    DEFAULT_MAX_IMAGE_PIXELS, DEFAULT_MAX_MESSAGE_IMAGE_BYTES,
};
