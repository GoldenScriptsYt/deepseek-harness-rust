//! DeepSeek Messages-protocol adapter core. Port of `packages/llm/llm-deepseek`.

pub mod client;
pub mod files;
pub mod listing;
pub mod serialize;
pub mod sse;
pub mod translate;

pub use files::{
    FileObject, FilePage, ListFiles, UploadFile, UploadedFile, DEFAULT_FILES_API_TIMEOUT,
    DEFAULT_FILES_API_TIMEOUT_MS, MAX_FILE_EXPIRY_SECONDS, MAX_FILE_UPLOAD_BYTES,
    MAX_STORED_FILE_BYTES, MAX_STORED_FILE_COUNT, MESSAGES_FILES_BETA, MIN_FILE_EXPIRY_SECONDS,
};
pub use listing::{
    model_listing_url, parse_model_listing, ClientRequirement, ListedBadge, ListedCapabilities,
    ListedFacts, ListingEntry, ModelListing, SystemPromptUpdate, ToolUpdate,
    DEFAULT_LISTING_TIMEOUT, DEFAULT_LISTING_TIMEOUT_MS, MAX_BADGE_TEXT_LENGTH,
};
