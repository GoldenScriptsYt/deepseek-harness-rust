//! DeepSeek `GET /models` discovery and per-entry decoding.

use crate::client::{request_id, Connection};
use dsh_llm::{
    ErrorCode, LlmError, ModelBadge, ModelBadgeText, ModelBadgeTone, ModelInfo, ModelModality,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use unicode_segmentation::UnicodeSegmentation;

pub const APP_PRODUCT: &str = "deepseek-harness";
pub const MAX_BADGE_TEXT_LENGTH: usize = 12;
pub const DEFAULT_LISTING_TIMEOUT: Duration = Duration::from_millis(20_000);
pub const DEFAULT_LISTING_TIMEOUT_MS: u64 = 20_000;

/// Client requirement declared under this build's product token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequirement {
    None,
    MinVersion(String),
    Invalid,
}

/// Messages system prompt update mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemPromptUpdate {
    InHistory,
    LeadingOnly,
}

/// Messages tool declaration update mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolUpdate {
    InHistory,
    AdditionOnly,
    LeadingOnly,
}

/// Capabilities a usable listing entry declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedFacts {
    pub input_modalities: Vec<ModelModality>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub system_prompt_update: Option<SystemPromptUpdate>,
    pub tool_update: Option<ToolUpdate>,
}

/// Why the Messages transport cannot drive a listed model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedCapabilities {
    Usable { facts: ListedFacts },
    Unusable { reason: String },
}

impl ListedCapabilities {
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Usable { .. })
    }
}

/// A valid selector badge, or why this build cannot display it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedBadge {
    Badge(ModelBadge),
    Invalid { reason: String },
}

/// One decoded listing entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListingEntry {
    pub id: String,
    pub name: Option<String>,
    pub requirement: ClientRequirement,
    pub capabilities: ListedCapabilities,
    pub badge: Option<ListedBadge>,
}

/// A decoded listing with at least one entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelListing {
    pub entries: Vec<ListingEntry>,
    pub skipped: usize,
}

fn object(value: &Value) -> Option<&serde_json::Map<String, Value>> {
    value.as_object()
}

/// `/models` at the origin of a Messages base URL; credentials never move to another origin.
pub fn model_listing_url(base_url: &str) -> Result<String, LlmError> {
    let url = reqwest::Url::parse(base_url).map_err(|error| {
        LlmError::new(
            ErrorCode::InvalidRequest,
            format!("DeepSeek Messages base URL is invalid: {error}"),
        )
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(LlmError::new(
            ErrorCode::InvalidRequest,
            "DeepSeek Messages base URL must use HTTP or HTTPS",
        ));
    }
    Ok(format!("{}/models", url.origin().ascii_serialization()))
}

fn known_modalities(value: Option<&Value>) -> Option<Vec<ModelModality>> {
    let values = value?.as_array()?;
    let mut output = Vec::new();
    for modality in [ModelModality::Text, ModelModality::Image] {
        let wire = match modality {
            ModelModality::Text => "text",
            ModelModality::Image => "image",
        };
        if let Some(position) = values.iter().position(|value| value.as_str() == Some(wire)) {
            output.push((position, modality));
        }
    }
    output.sort_by_key(|(position, _)| *position);
    Some(output.into_iter().map(|(_, modality)| modality).collect())
}

fn capacity(value: Option<&Value>) -> Result<Option<u64>, ()> {
    match value {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|capacity| *capacity > 0 && *capacity <= 9_007_199_254_740_991)
            .map(Some)
            .ok_or(()),
    }
}

fn requirement_of(entry: &serde_json::Map<String, Value>) -> ClientRequirement {
    let Some(value) = entry.get("client_requirements") else {
        return ClientRequirement::None;
    };
    let Some(requirements) = object(value) else {
        return ClientRequirement::Invalid;
    };
    let Some(declared) = requirements.get(APP_PRODUCT) else {
        return ClientRequirement::None;
    };
    let Some(own) = object(declared) else {
        return ClientRequirement::Invalid;
    };
    match own.get("min_version") {
        None => ClientRequirement::None,
        Some(Value::String(version)) if !version.is_empty() => {
            ClientRequirement::MinVersion(version.clone())
        }
        Some(_) => ClientRequirement::Invalid,
    }
}

fn language_id(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(first) = parts.next() else {
        return false;
    };
    if first.len() < 2 || first.len() > 8 || !first.bytes().all(|byte| byte.is_ascii_lowercase()) {
        return false;
    }
    parts.all(|part| {
        !part.is_empty()
            && part.len() <= 8
            && part
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

fn default_ignorable(character: char) -> bool {
    matches!(character as u32,
        0x00ad | 0x034f | 0x061c | 0x115f..=0x1160 | 0x17b4..=0x17b5 | 0x180b..=0x180f
        | 0x200b..=0x200f | 0x202a..=0x202e | 0x2060..=0x206f | 0x3164 | 0xfe00..=0xfe0f
        | 0xfeff | 0xffa0 | 0xfff0..=0xfff8 | 0x1bca0..=0x1bca3 | 0x1d173..=0x1d17a
        | 0xe0000..=0xe0fff)
}

fn badge_text(value: &Value) -> Option<String> {
    let text = value.as_str()?.trim();
    let unreadable = value.as_str()?.chars().any(|character| {
        character.is_control()
            || matches!(
                character,
                '\u{2028}' | '\u{2029}' | '\u{061c}' | '\u{200e}' | '\u{200f}'
            )
            || matches!(character as u32, 0x202a..=0x202e | 0x2066..=0x2069)
    });
    let valid = (1..=MAX_BADGE_TEXT_LENGTH).contains(&text.graphemes(true).count())
        && text
            .chars()
            .any(|character| !character.is_whitespace() && !default_ignorable(character))
        && !unreadable;
    valid.then(|| text.to_string())
}

fn localized_badge_text(value: &Value) -> Option<ModelBadgeText> {
    if value.is_string() {
        return badge_text(value).map(ModelBadgeText::Literal);
    }
    let variants = object(value)?;
    let mut translations = BTreeMap::new();
    for (language, value) in variants {
        if !language_id(language) {
            return None;
        }
        translations.insert(language.clone(), badge_text(value)?);
    }
    translations
        .contains_key("en")
        .then_some(ModelBadgeText::Localized(translations))
}

fn badge_of(entry: &serde_json::Map<String, Value>) -> Option<ListedBadge> {
    const OWN: &str = "client_metadata[\"deepseek-harness\"]";
    let invalid_text = || {
        ListedBadge::Invalid {
        reason: format!("{OWN}.badge.text is not a string or an object of lowercase language ids including \"en\", each text 1 to {MAX_BADGE_TEXT_LENGTH} characters on one line"),
    }
    };
    let metadata = match entry.get("client_metadata") {
        None | Some(Value::Null) => return None,
        Some(Value::Object(metadata)) => metadata,
        Some(_) => {
            return Some(ListedBadge::Invalid {
                reason: "client_metadata is not an object".into(),
            })
        }
    };
    let own = match metadata.get(APP_PRODUCT) {
        None | Some(Value::Null) => return None,
        Some(Value::Object(own)) => own,
        Some(_) => {
            return Some(ListedBadge::Invalid {
                reason: format!("{OWN} is not an object"),
            })
        }
    };
    let declared = match own.get("badge") {
        None | Some(Value::Null) => return None,
        Some(Value::Object(declared)) => declared,
        Some(_) => {
            return Some(ListedBadge::Invalid {
                reason: format!("{OWN}.badge is not an object"),
            })
        }
    };
    let Some(text) = declared.get("text").and_then(localized_badge_text) else {
        return Some(invalid_text());
    };
    let tone = match declared.get("tone").and_then(Value::as_str) {
        Some("info") => ModelBadgeTone::Info,
        Some("success") => ModelBadgeTone::Success,
        Some("warning") => ModelBadgeTone::Warning,
        _ => ModelBadgeTone::Neutral,
    };
    Some(ListedBadge::Badge(ModelBadge { text, tone }))
}

fn capabilities_of(entry: &serde_json::Map<String, Value>) -> ListedCapabilities {
    let messages = entry
        .get("api_capabilities")
        .and_then(object)
        .and_then(|capabilities| capabilities.get("anthropic_messages"))
        .and_then(object);
    let Some(messages) = messages else {
        return ListedCapabilities::Unusable {
            reason: "declares no api_capabilities.anthropic_messages".into(),
        };
    };
    let outputs = match entry.get("output_modalities") {
        None => Some(vec![ModelModality::Text]),
        Some(value) => known_modalities(Some(value)),
    };
    if !outputs.is_some_and(|modalities| modalities.contains(&ModelModality::Text)) {
        return ListedCapabilities::Unusable {
            reason: "declares no text output".into(),
        };
    }
    let inputs = match entry.get("input_modalities") {
        None => Some(vec![ModelModality::Text]),
        Some(value) => known_modalities(Some(value)),
    };
    let Some(inputs) = inputs.filter(|modalities| !modalities.is_empty()) else {
        return ListedCapabilities::Unusable {
            reason: "declares no input modality DeepSeek Harness supports".into(),
        };
    };
    let (Ok(context_window), Ok(max_output_tokens)) = (
        capacity(entry.get("context_window")),
        capacity(entry.get("max_output_tokens")),
    ) else {
        return ListedCapabilities::Unusable {
            reason: "declares an invalid context_window or max_output_tokens".into(),
        };
    };
    let system_prompt_update = match messages.get("system_prompt_update") {
        None | Some(Value::Null) => None,
        Some(Value::String(mode)) => Some(if mode == "in-history" {
            SystemPromptUpdate::InHistory
        } else {
            SystemPromptUpdate::LeadingOnly
        }),
        Some(_) => Some(SystemPromptUpdate::LeadingOnly),
    };
    let tool_update = match messages.get("tool_update") {
        None | Some(Value::Null) => None,
        Some(Value::String(mode)) if mode == "in-history" => Some(ToolUpdate::InHistory),
        Some(Value::String(mode)) if mode == "addition-only" => Some(ToolUpdate::AdditionOnly),
        Some(_) => Some(ToolUpdate::LeadingOnly),
    };
    ListedCapabilities::Usable {
        facts: ListedFacts {
            input_modalities: inputs,
            context_window,
            max_output_tokens,
            system_prompt_update,
            tool_update,
        },
    }
}

/// Decode one `/models` body entry by entry.
pub fn parse_model_listing(body: &Value) -> Result<ModelListing, LlmError> {
    let rows = object(body)
        .and_then(|body| body.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LlmError::new(
                ErrorCode::InvalidResponse,
                "DeepSeek model listing has no \"data\" array",
            )
        })?;
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let Some(entry) = object(row) else { continue };
        let Some(id) = entry
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        if !seen.insert(id.to_string()) {
            continue;
        }
        entries.push(ListingEntry {
            id: id.to_string(),
            name: entry
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .map(str::to_string),
            requirement: requirement_of(entry),
            capabilities: capabilities_of(entry),
            badge: badge_of(entry),
        });
    }
    if entries.is_empty() {
        return Err(LlmError::new(
            ErrorCode::InvalidResponse,
            "DeepSeek model listing contains no model entry",
        ));
    }
    Ok(ModelListing {
        skipped: rows.len() - entries.len(),
        entries,
    })
}

fn model_info(provider: &str, entry: &ListingEntry) -> Option<ModelInfo> {
    let ListedCapabilities::Usable { facts } = &entry.capabilities else {
        return None;
    };
    Some(ModelInfo {
        provider: provider.to_string(),
        id: entry.id.clone(),
        name: entry.name.clone().unwrap_or_else(|| entry.id.clone()),
        description: None,
        input_modalities: Some(facts.input_modalities.clone()),
        badge: match &entry.badge {
            Some(ListedBadge::Badge(badge)) => Some(badge.clone()),
            _ => None,
        },
    })
}

impl Connection {
    /// Request and decode one listing with the default twenty-second deadline.
    pub async fn model_listing(&self) -> Result<ModelListing, LlmError> {
        self.model_listing_with_timeout(DEFAULT_LISTING_TIMEOUT)
            .await
    }

    /// Request and decode one listing. Redirects are refused by the shared client.
    pub async fn model_listing_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ModelListing, LlmError> {
        let url = model_listing_url(&self.base_url)?;
        let request = self
            .client()?
            .get(&url)
            .header("accept", "application/json")
            .header("x-api-key", &self.api_key);
        let response = tokio::time::timeout(timeout, async {
            let response = request.send().await.map_err(|error| {
                LlmError::new(
                    ErrorCode::Transport,
                    format!("could not reach {url}: {error}"),
                )
            })?;
            let status = response.status();
            if status.is_redirection() {
                return Err(LlmError::new(
                    ErrorCode::Transport,
                    format!("could not reach {url}: redirects are not allowed"),
                ));
            }
            let headers = response.headers().clone();
            let body = response.bytes().await.map_err(|error| {
                LlmError::new(
                    ErrorCode::Transport,
                    format!("could not read {url}: {error}"),
                )
            })?;
            if !status.is_success() {
                let mut error = LlmError::new(
                    ErrorCode::Http(status.as_u16()),
                    format!("{url} answered HTTP {}", status.as_u16()),
                )
                .with_status(status.as_u16());
                if let Some(id) = request_id(&headers) {
                    error = error.with_request_id(id);
                }
                return Err(error);
            }
            let value: Value = serde_json::from_slice(&body).map_err(|_| {
                LlmError::new(
                    ErrorCode::InvalidResponse,
                    format!("{url} did not answer with JSON"),
                )
            })?;
            parse_model_listing(&value)
        })
        .await
        .map_err(|_| {
            LlmError::new(
                ErrorCode::Timeout,
                format!(
                    "DeepSeek model listing request to {url} timed out after {} ms",
                    timeout.as_millis()
                ),
            )
        })?;
        response
    }

    /// Return provider-neutral metadata for usable entries, in service order.
    /// Client requirements and unusable entries remain available through [`Self::model_listing`].
    pub async fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, LlmError> {
        Ok(self
            .model_listing()
            .await?
            .entries
            .iter()
            .filter_map(|entry| model_info(provider, entry))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn only(row: Value) -> ListingEntry {
        let mut row = row.as_object().unwrap().clone();
        row.insert("id".into(), json!("m"));
        parse_model_listing(&json!({"data":[row]}))
            .unwrap()
            .entries
            .remove(0)
    }

    #[test]
    fn listing_url_uses_origin_not_messages_path() {
        assert_eq!(
            model_listing_url("https://gateway.example/custom/route/v1").unwrap(),
            "https://gateway.example/models"
        );
        assert_eq!(
            model_listing_url("http://127.0.0.1:1234/anthropic").unwrap(),
            "http://127.0.0.1:1234/models"
        );
        assert!(model_listing_url("ftp://example.com/anthropic").is_err());
    }

    #[test]
    fn decodes_entries_and_skips_missing_or_duplicate_ids() {
        let listing = parse_model_listing(&json!({"data":[null, 1, {"id":""}, {"id":"a","name":"A","api_capabilities":{"anthropic_messages":{}}}, {"id":"a"}]})).unwrap();
        assert_eq!(listing.skipped, 4);
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].name.as_deref(), Some("A"));
        for invalid in [
            json!(null),
            json!([]),
            json!({}),
            json!({"data":{}}),
            json!({"data":[]}),
            json!({"data":[{"id":""}]}),
        ] {
            assert_eq!(
                parse_model_listing(&invalid).unwrap_err().code,
                ErrorCode::InvalidResponse
            );
        }
    }

    #[test]
    fn defaults_and_narrows_modalities_and_capacities() {
        let entry = only(json!({"api_capabilities":{"anthropic_messages":{}}}));
        let ListedCapabilities::Usable { facts } = entry.capabilities else {
            panic!()
        };
        assert_eq!(facts.input_modalities, vec![ModelModality::Text]);
        assert_eq!(facts.context_window, None);
        let entry = only(
            json!({"input_modalities":["image","audio","text","image"],"context_window":100,"max_output_tokens":20,"api_capabilities":{"anthropic_messages":{"system_prompt_update":"future","tool_update":"addition-only"}}}),
        );
        let ListedCapabilities::Usable { facts } = entry.capabilities else {
            panic!()
        };
        assert_eq!(
            facts.input_modalities,
            vec![ModelModality::Image, ModelModality::Text]
        );
        assert_eq!(facts.context_window, Some(100));
        assert_eq!(facts.max_output_tokens, Some(20));
        assert_eq!(
            facts.system_prompt_update,
            Some(SystemPromptUpdate::LeadingOnly)
        );
        assert_eq!(facts.tool_update, Some(ToolUpdate::AdditionOnly));
    }

    #[test]
    fn unusable_rows_remain_visible_with_reasons() {
        let cases = [
            (json!({}), "declares no api_capabilities.anthropic_messages"),
            (
                json!({"api_capabilities":{"anthropic_messages":{}},"output_modalities":["image"]}),
                "declares no text output",
            ),
            (
                json!({"api_capabilities":{"anthropic_messages":{}},"input_modalities":["audio"]}),
                "declares no input modality DeepSeek Harness supports",
            ),
            (
                json!({"api_capabilities":{"anthropic_messages":{}},"context_window":0}),
                "declares an invalid context_window or max_output_tokens",
            ),
            (
                json!({"api_capabilities":{"anthropic_messages":{}},"max_output_tokens":1.5}),
                "declares an invalid context_window or max_output_tokens",
            ),
        ];
        for (row, reason) in cases {
            let entry = only(row);
            assert!(!entry.capabilities.is_usable());
            assert_eq!(
                entry.capabilities,
                ListedCapabilities::Unusable {
                    reason: reason.into()
                }
            );
        }
    }

    #[test]
    fn reads_requirements_and_update_modes() {
        assert_eq!(only(json!({"api_capabilities":{"anthropic_messages":{}},"client_requirements":{APP_PRODUCT:{"min_version":"0.2.0-alpha.1"}}})).requirement, ClientRequirement::MinVersion("0.2.0-alpha.1".into()));
        for value in [json!(null), json!([]), json!("0.2.0")] {
            assert_eq!(only(json!({"api_capabilities":{"anthropic_messages":{}},"client_requirements":value})).requirement, ClientRequirement::Invalid);
        }
        let entry = only(
            json!({"api_capabilities":{"anthropic_messages":{"system_prompt_update":null,"tool_update":null}}}),
        );
        let ListedCapabilities::Usable { facts } = entry.capabilities else {
            panic!()
        };
        assert_eq!(facts.system_prompt_update, None);
        assert_eq!(facts.tool_update, None);
    }

    #[test]
    fn badge_validation_preserves_localized_text_and_graphemes() {
        let entry = only(
            json!({"api_capabilities":{"anthropic_messages":{}},"client_metadata":{APP_PRODUCT:{"badge":{"text":{"en":" New ","zh":"新"},"tone":"info"}}}}),
        );
        let Some(ListedBadge::Badge(badge)) = entry.badge else {
            panic!()
        };
        assert_eq!(badge.tone, ModelBadgeTone::Info);
        assert!(
            matches!(badge.text, ModelBadgeText::Localized(text) if text["en"] == "New" && text["zh"] == "新")
        );
        let longest = "十".repeat(MAX_BADGE_TEXT_LENGTH);
        assert!(matches!(only(json!({"api_capabilities":{"anthropic_messages":{}},"client_metadata":{APP_PRODUCT:{"badge":{"text":longest}}}})).badge, Some(ListedBadge::Badge(_))));
        assert!(matches!(only(json!({"api_capabilities":{"anthropic_messages":{}},"client_metadata":{APP_PRODUCT:{"badge":{"text":"👨‍👩‍👧".repeat(MAX_BADGE_TEXT_LENGTH)}}}})).badge, Some(ListedBadge::Badge(_))));
        assert!(matches!(only(json!({"api_capabilities":{"anthropic_messages":{}},"client_metadata":{APP_PRODUCT:{"badge":{"text":"line\nbreak"}}}})).badge, Some(ListedBadge::Invalid { .. })));
        assert!(matches!(only(json!({"api_capabilities":{"anthropic_messages":{}},"client_metadata":{APP_PRODUCT:{"badge":{"text":{"zh":"新"}}}}} )).badge, Some(ListedBadge::Invalid { .. })));
    }

    #[test]
    fn badge_diagnostics_do_not_hide_usable_models() {
        let entry =
            only(json!({"api_capabilities":{"anthropic_messages":{}},"client_metadata":"bad"}));
        assert!(entry.capabilities.is_usable());
        assert!(
            matches!(entry.badge, Some(ListedBadge::Invalid { reason }) if reason == "client_metadata is not an object")
        );
    }
}
