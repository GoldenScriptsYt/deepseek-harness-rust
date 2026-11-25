//! Web search and fetch providers with their model-facing tools.
//!
//! `dsh-web` owns URL and content-type policy, the bounded HTML-to-markdown
//! conversion, the anonymous HTTP fetcher, the DeepSeek-backed search provider,
//! and the `web_search` / `web_fetch` tool registrations.

pub mod error;
pub mod fetch;
pub mod html;
pub mod policy;
pub mod search;
pub mod tools;

pub use error::{WebError, WebErrorCode};
pub use fetch::{HttpFetchLimits, HttpFetchProvider, WebFetchBody, WebFetchResult};
pub use html::html_to_markdown;
pub use policy::FetchableKind;
pub use search::{DeepSeekSearchConfig, DeepSeekSearchProvider, WebSearchResult, WebSource};
pub use tools::{
    format_fetch_output, format_search_output, register_web_tools, WebFetchTool, WebSearchTool,
    WebToolConfig, EXTERNAL_WEB_CONTENT_NOTICE,
};
