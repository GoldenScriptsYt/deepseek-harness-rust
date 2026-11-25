//! OpenAI Chat Completions route for imported provider profiles.

use crate::model_routes::ProviderProfile;
use base64::Engine;
use dsh_attachments::{
    AttachmentError, AttachmentStore, FileAttachmentRef as StoredFileRef,
    ImageAttachmentRef as StoredImageRef, ImageDimensions as StoredImageDimensions,
    ImageMediaType as StoredImageMediaType,
};
use dsh_core::{BoxFuture, Model};
use dsh_llm::{
    BlockKind, Content, ErrorCode, FinishReason, LlmError, Message, Role, StreamChunk, TokenUsage,
    ToolDef,
};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::sync::mpsc;

const MAX_ERROR_BODY_CHARS: usize = 1200;

/// One OpenAI-compatible chat-completions model route.
pub struct OpenAiModel {
    provider: String,
    model: String,
    api_key: String,
    base_url: String,
    profile: ProviderProfile,
    max_tokens: u64,
    reasoning_effort: Option<String>,
    attachments: Option<AttachmentStore>,
    client: Client,
}

impl OpenAiModel {
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        api_key: String,
        profile: ProviderProfile,
        model_profile: &Value,
        reasoning_effort: Option<String>,
        attachments: Option<AttachmentStore>,
    ) -> Result<Self, LlmError> {
        let base_url = profile
            .base_url
            .as_deref()
            .filter(|url| !url.trim().is_empty())
            .ok_or_else(|| LlmError::new(ErrorCode::InvalidRequest, "provider baseURL is missing"))?
            .trim_end_matches('/')
            .to_owned();
        let parsed = reqwest::Url::parse(&base_url).map_err(|_| {
            LlmError::new(
                ErrorCode::InvalidRequest,
                "provider baseURL is not a valid URL",
            )
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(LlmError::new(
                ErrorCode::InvalidRequest,
                "provider baseURL must use HTTP or HTTPS",
            ));
        }
        let max_tokens = model_profile["maxTokens"]
            .as_u64()
            .or_else(|| std::env::var("DSH_MAX_TOKENS").ok()?.parse().ok())
            .unwrap_or(32_000);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|_| {
                LlmError::new(ErrorCode::Transport, "provider HTTP client could not start")
            })?;
        Ok(Self {
            provider: provider.into(),
            model: model.into(),
            api_key,
            base_url,
            profile,
            max_tokens,
            reasoning_effort,
            attachments,
            client,
        })
    }

    fn endpoint(&self) -> String {
        if self.base_url.ends_with("/chat/completions") {
            self.base_url.clone()
        } else {
            format!("{}/chat/completions", self.base_url)
        }
    }

    fn body(&self, history: &[Message], tools: &[ToolDef]) -> Result<Value, LlmError> {
        let messages = self.messages(history)?;
        let mut body = json!({
            "model": self.model,
            "messages": messages,
            "stream": true,
        });
        let cap_field = self
            .profile
            .compat
            .as_ref()
            .and_then(|compat| compat["maxTokensField"].as_str())
            .unwrap_or("max_tokens");
        body[cap_field] = json!(self.max_tokens);
        if self
            .profile
            .compat
            .as_ref()
            .is_some_and(|compat| compat["supportsReasoningEffort"] == true)
        {
            if let Some(effort) = self.reasoning_effort.as_deref() {
                body["reasoning_effort"] = json!(effort);
            }
        }
        if !tools.is_empty() {
            body["tools"] = json!(tools
                .iter()
                .map(|tool| json!({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                    }
                }))
                .collect::<Vec<_>>());
        }
        Ok(body)
    }

    fn messages(&self, history: &[Message]) -> Result<Vec<Value>, LlmError> {
        let mut messages = Vec::with_capacity(history.len());
        for message in history {
            let compat = self.profile.compat.as_ref();
            let mut text = String::new();
            let mut reasoning = String::new();
            let mut parts = Vec::new();
            let mut tool_calls = Vec::new();
            for block in &message.content {
                match block {
                    Content::Text { text: value } => {
                        text.push_str(value);
                        parts.push(json!({"type": "text", "text": value}));
                    }
                    Content::Reasoning { text: value, .. } => reasoning.push_str(value),
                    Content::Image {
                        attachment,
                        offloaded,
                    } => {
                        if *offloaded {
                            text.push_str("[image omitted to fit request limits]");
                            parts.push(json!({"type": "text", "text": "[image omitted to fit request limits]"}));
                            continue;
                        }
                        let store = self.attachments.as_ref().ok_or_else(|| {
                            LlmError::new(
                                ErrorCode::FilesApi,
                                "image attachment storage is unavailable",
                            )
                        })?;
                        let reference = stored_image_ref(attachment)?;
                        let stored = store.read_image(&reference).map_err(attachment_error)?;
                        let encoded = base64::engine::general_purpose::STANDARD.encode(stored.data);
                        let url =
                            format!("data:{};base64,{encoded}", attachment.media_type.as_str());
                        parts.push(json!({
                            "type": "image_url",
                            "image_url": {"url": url, "detail": "high"},
                        }));
                    }
                    Content::File { attachment } => {
                        let message_text = if let Some(store) = &self.attachments {
                            let reference = StoredFileRef {
                                attachment_id: attachment.attachment_id.clone(),
                                name: attachment.name.clone(),
                                bytes: attachment.bytes,
                            };
                            let path = store
                                .file_readonly_path(&reference)
                                .map_err(attachment_error)?;
                            format!("[File {} ({} bytes) is available at {}. Use the file tools to read it.]", attachment.name, attachment.bytes, path.display())
                        } else {
                            format!(
                                "[File {} ({} bytes) is attached; file access is unavailable.]",
                                attachment.name, attachment.bytes
                            )
                        };
                        text.push_str(&message_text);
                        parts.push(json!({"type": "text", "text": message_text}));
                    }
                    Content::ToolCall {
                        id,
                        name,
                        arguments,
                    } => tool_calls.push(json!({
                        "id": id,
                        "type": "function",
                        "function": {"name": name, "arguments": arguments},
                    })),
                    Content::ToolAddition { .. } | Content::ToolRemoval { .. } => {}
                }
            }
            let role = match message.role {
                Role::System => "system",
                Role::Developer if compat.is_some_and(|c| c["supportsDeveloperRole"] == false) => {
                    "system"
                }
                Role::Developer => "developer",
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "tool",
            };
            let mut entry = json!({"role": role});
            if message.role == Role::Tool {
                entry["content"] = json!(text);
                if let Some(id) = &message.tool_call_id {
                    entry["tool_call_id"] = json!(id);
                }
                if message.is_error == Some(true) {
                    entry["content"] = json!(format!("Tool error: {text}"));
                }
            } else if message.role == Role::User
                && parts.iter().any(|part| part["type"] == "image_url")
            {
                entry["content"] = json!(parts);
            } else {
                entry["content"] = if text.is_empty() {
                    Value::Null
                } else {
                    json!(text)
                };
            }
            if !reasoning.is_empty()
                && compat.is_some_and(|value| value["thinkingFormat"] == "deepseek")
            {
                entry["reasoning_content"] = json!(reasoning);
            }
            if !tool_calls.is_empty() {
                entry["tool_calls"] = json!(tool_calls);
            }
            messages.push(entry);
        }
        Ok(messages)
    }
}

impl Model for OpenAiModel {
    fn provider(&self) -> String {
        self.provider.clone()
    }
    fn model(&self) -> String {
        self.model.clone()
    }

    fn stream<'a>(
        &'a self,
        history: &'a [Message],
        tools: &'a [ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move { self.stream_inner(history, tools, tx).await })
    }
}

impl OpenAiModel {
    async fn stream_inner(
        &self,
        history: &[Message],
        tools: &[ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) {
        let body = match self.body(history, tools) {
            Ok(body) => body,
            Err(error) => {
                let _ = tx.send(Err(error)).await;
                return;
            }
        };
        let request = self
            .client
            .post(self.endpoint())
            .header(header::ACCEPT, "text/event-stream")
            .header(header::AUTHORIZATION, format!("Bearer {}", self.api_key))
            .json(&body)
            .send()
            .await;
        let response = match request {
            Ok(response) => response,
            Err(error) => {
                let code = if error.is_timeout() {
                    ErrorCode::Timeout
                } else {
                    ErrorCode::Transport
                };
                let _ = tx
                    .send(Err(LlmError::new(
                        code,
                        "provider request failed before streaming",
                    )))
                    .await;
                return;
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let _ = tx.send(Err(http_error(status, &body, &self.api_key))).await;
            return;
        }

        let mut stream = response.bytes_stream();
        let mut line = Vec::new();
        let mut state = StreamState::default();
        let mut finished = false;
        while let Some(next) = stream.next().await {
            if tx.is_closed() {
                return;
            }
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(_) => {
                    let _ = tx
                        .send(Err(LlmError::new(
                            ErrorCode::StreamClosed,
                            "provider stream ended unexpectedly",
                        )))
                        .await;
                    return;
                }
            };
            for byte in chunk {
                if byte == b'\n' {
                    let current = std::mem::take(&mut line);
                    if process_sse_line(&current, &self.api_key, &mut state, &tx).await {
                        finished = true;
                        break;
                    }
                } else if byte != b'\r' {
                    line.push(byte);
                }
            }
            if finished {
                break;
            }
        }
        if !finished && !line.is_empty() {
            let _ = process_sse_line(&line, &self.api_key, &mut state, &tx).await;
        }
        if !finished && !tx.is_closed() {
            state.finish(&tx, FinishReason::Stop).await;
        }
    }
}

#[derive(Default)]
struct StreamState {
    text_started: bool,
    reasoning_started: bool,
    tools_started: BTreeSet<usize>,
}

impl StreamState {
    async fn finish(
        &mut self,
        tx: &mpsc::Sender<Result<StreamChunk, LlmError>>,
        reason: FinishReason,
    ) {
        if self.text_started {
            let _ = tx.send(Ok(StreamChunk::BlockEnd { index: 0 })).await;
        }
        if self.reasoning_started {
            let _ = tx.send(Ok(StreamChunk::BlockEnd { index: 1 })).await;
        }
        for index in std::mem::take(&mut self.tools_started) {
            let _ = tx
                .send(Ok(StreamChunk::BlockEnd { index: 2 + index }))
                .await;
        }
        let _ = tx
            .send(Ok(StreamChunk::Finish {
                reason,
                replay: vec![],
            }))
            .await;
    }
}

async fn process_sse_line(
    line: &[u8],
    api_key: &str,
    state: &mut StreamState,
    tx: &mpsc::Sender<Result<StreamChunk, LlmError>>,
) -> bool {
    let Ok(line) = std::str::from_utf8(line) else {
        return false;
    };
    let Some(data) = line.strip_prefix("data:").map(str::trim) else {
        return false;
    };
    if data == "[DONE]" {
        state.finish(tx, FinishReason::Stop).await;
        return true;
    }
    let Ok(event) = serde_json::from_str::<Value>(data) else {
        let _ = tx
            .send(Err(LlmError::new(
                ErrorCode::MalformedResponse,
                "provider sent an invalid stream event",
            )))
            .await;
        return true;
    };
    if let Some(error) = event.get("error") {
        let message = error["message"]
            .as_str()
            .unwrap_or("provider reported a stream error");
        let _ = tx
            .send(Err(LlmError::new(
                ErrorCode::Server,
                safe_message(message, api_key),
            )))
            .await;
        return true;
    }
    if let Some(usage) = event.get("usage") {
        let input = usage["prompt_tokens"].as_u64().unwrap_or(0);
        let output = usage["completion_tokens"].as_u64().unwrap_or(0);
        let total = usage["total_tokens"].as_u64().unwrap_or(input + output);
        let _ = tx
            .send(Ok(StreamChunk::Usage(TokenUsage {
                input_tokens: input,
                output_tokens: output,
                total_tokens: total,
                ..TokenUsage::default()
            })))
            .await;
    }
    let choice = &event["choices"][0];
    let delta = &choice["delta"];
    if let Some(text) = delta["content"].as_str().filter(|text| !text.is_empty()) {
        if !state.text_started {
            state.text_started = true;
            if tx
                .send(Ok(StreamChunk::BlockStart {
                    index: 0,
                    kind: BlockKind::Text,
                }))
                .await
                .is_err()
            {
                return true;
            }
        }
        if tx
            .send(Ok(StreamChunk::TextDelta {
                index: 0,
                text: text.to_owned(),
            }))
            .await
            .is_err()
        {
            return true;
        }
    }
    let reasoning = delta["reasoning_content"]
        .as_str()
        .or_else(|| delta["reasoning"].as_str());
    if let Some(text) = reasoning.filter(|text| !text.is_empty()) {
        if !state.reasoning_started {
            state.reasoning_started = true;
            if tx
                .send(Ok(StreamChunk::BlockStart {
                    index: 1,
                    kind: BlockKind::Reasoning,
                }))
                .await
                .is_err()
            {
                return true;
            }
        }
        if tx
            .send(Ok(StreamChunk::ReasoningDelta {
                index: 1,
                text: text.to_owned(),
            }))
            .await
            .is_err()
        {
            return true;
        }
    }
    if let Some(calls) = delta["tool_calls"].as_array() {
        for call in calls {
            let index = call["index"].as_u64().unwrap_or(0) as usize;
            if state.tools_started.insert(index)
                && tx
                    .send(Ok(StreamChunk::BlockStart {
                        index: 2 + index,
                        kind: BlockKind::ToolCall,
                    }))
                    .await
                    .is_err()
            {
                return true;
            }
            let function = &call["function"];
            let id = call["id"].as_str().map(str::to_owned);
            let name = function["name"].as_str().map(str::to_owned);
            let arguments = function["arguments"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            if tx
                .send(Ok(StreamChunk::ToolCallDelta {
                    index: 2 + index,
                    id,
                    name,
                    arguments_delta: arguments,
                }))
                .await
                .is_err()
            {
                return true;
            }
        }
    }
    let finish = match choice["finish_reason"].as_str() {
        Some("tool_calls") | Some("function_call") => Some(FinishReason::ToolCalls),
        Some("length") => Some(FinishReason::MaxTokens),
        Some(_) => Some(FinishReason::Stop),
        None => None,
    };
    if let Some(reason) = finish {
        state.finish(tx, reason).await;
        return true;
    }
    false
}

fn http_error(status: StatusCode, body: &str, api_key: &str) -> LlmError {
    let code = match status.as_u16() {
        401 | 403 => ErrorCode::Auth,
        408 => ErrorCode::Timeout,
        429 => ErrorCode::RateLimit,
        400 | 404 | 422 => ErrorCode::InvalidRequest,
        500..=599 => ErrorCode::Server,
        status => ErrorCode::Http(status),
    };
    let detail = safe_message(body, api_key);
    LlmError::new(
        code,
        format!("provider returned HTTP {}: {detail}", status.as_u16()),
    )
    .with_status(status.as_u16())
}

fn safe_message(message: &str, api_key: &str) -> String {
    let scrubbed = if api_key.is_empty() {
        message.to_owned()
    } else {
        message.replace(api_key, "[redacted]")
    };
    let mut limited: String = scrubbed.chars().take(MAX_ERROR_BODY_CHARS).collect();
    if scrubbed.chars().count() > MAX_ERROR_BODY_CHARS {
        limited.push('…');
    }
    limited
}

fn stored_image_ref(reference: &dsh_llm::ImageAttachmentRef) -> Result<StoredImageRef, LlmError> {
    let original_dimensions = reference
        .original_dimensions
        .as_ref()
        .map(|dimensions| {
            Ok::<_, LlmError>(StoredImageDimensions {
                width: u32::try_from(dimensions.width).map_err(|_| invalid_attachment())?,
                height: u32::try_from(dimensions.height).map_err(|_| invalid_attachment())?,
            })
        })
        .transpose()?;
    Ok(StoredImageRef {
        attachment_id: reference.attachment_id.clone(),
        media_type: match reference.media_type {
            dsh_llm::ImageMediaType::Png => StoredImageMediaType::Png,
            dsh_llm::ImageMediaType::Jpeg => StoredImageMediaType::Jpeg,
            dsh_llm::ImageMediaType::Webp => StoredImageMediaType::Webp,
            dsh_llm::ImageMediaType::Gif => StoredImageMediaType::Gif,
        },
        bytes: reference.bytes,
        width: u32::try_from(reference.width).map_err(|_| invalid_attachment())?,
        height: u32::try_from(reference.height).map_err(|_| invalid_attachment())?,
        name: reference.name.clone(),
        original_dimensions,
    })
}

fn invalid_attachment() -> LlmError {
    LlmError::new(ErrorCode::FilesApi, "image attachment metadata is invalid")
}

fn attachment_error(error: AttachmentError) -> LlmError {
    LlmError::new(ErrorCode::FilesApi, error.to_string())
}
