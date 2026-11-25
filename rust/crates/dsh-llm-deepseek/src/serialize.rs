//! Provider-neutral history to a DeepSeek Messages request body.

use base64::Engine;
use dsh_llm::{
    Content, ErrorCode, FileAttachmentRef, ImageAttachmentRef, LlmError, Message, RequestImage,
    Role,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// Reasoning effort; `None` in [`Options`] disables thinking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effort {
    Low,
    High,
    Max,
}

impl Effort {
    fn wire(self) -> &'static str {
        match self {
            Effort::Low => "low",
            Effort::High => "high",
            Effort::Max => "max",
        }
    }
}

/// A callable tool declaration.
#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub defer_loading: bool,
}

/// Request parameters outside the history.
#[derive(Debug, Clone)]
pub struct Options {
    pub model: String,
    pub max_tokens: u64,
    pub effort: Option<Effort>,
    pub temperature: Option<f64>,
    pub stop_sequences: Vec<String>,
    pub tools: Vec<ToolSpec>,
    /// Route keeps later system updates in message history instead of the `system` field.
    pub system_in_history: bool,
}

fn invalid(m: &str) -> LlmError {
    LlmError::new(ErrorCode::InvalidRequest, format!("DeepSeek Messages {m}"))
}

fn unsupported(what: &str) -> LlmError {
    LlmError::new(
        ErrorCode::UnsupportedContent,
        format!("DeepSeek Messages cannot represent {what}"),
    )
}

fn tool_input(raw: &str) -> Value {
    match serde_json::from_str::<Value>(raw) {
        Ok(v @ Value::Object(_)) => v,
        _ => Value::Object(Map::new()),
    }
}

struct Wire {
    role: &'static str,
    content: Vec<Value>,
}

fn text_block(t: &str) -> Value {
    json!({"type":"text","text":t})
}

/// Prepared request images and optional durable Files API references.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PreparedImages {
    /// Exact request bytes keyed by durable attachment id.
    pub versions: BTreeMap<String, RequestImage>,
    /// `None` selects inline base64; `Some` requires a file id for every retained image.
    pub file_ids: Option<BTreeMap<String, String>>,
    /// Execution-world read-only paths used in model-visible image handles.
    pub readonly_paths: BTreeMap<String, String>,
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".into())
}

fn image_identity(reference: &ImageAttachmentRef) -> String {
    match &reference.name {
        Some(name) => format!("{} ({})", quoted(name), reference.attachment_id),
        None => reference.attachment_id.clone(),
    }
}

fn normalized_access_text(reference: &ImageAttachmentRef, path: &str) -> String {
    let extension = match reference.media_type {
        dsh_llm::ImageMediaType::Png => ".png",
        dsh_llm::ImageMediaType::Jpeg => ".jpg",
        dsh_llm::ImageMediaType::Webp => ".webp",
        dsh_llm::ImageMediaType::Gif => ".gif",
    };
    format!(
        " Normalized copy (read-only; may be resized or re-encoded): {} ({}x{}px, {}). Source dimensions, format, and byte size may differ. Copy to a writable path ending in {extension} before editing.",
        quoted(path), reference.width, reference.height, reference.media_type.as_str()
    )
}

/// Stable model-facing handle for one exact request image.
pub fn request_image_handle_text(
    reference: &ImageAttachmentRef,
    version: &RequestImage,
    readonly_path: Option<&str>,
) -> String {
    let preview = format!(
        "Image {}; request preview {}x{}px.",
        image_identity(reference),
        version.width,
        version.height
    );
    match readonly_path {
        Some(path) => preview + &normalized_access_text(reference, path),
        None => format!("{preview} It may be resized or re-encoded; source dimensions, format, and byte size may differ."),
    }
}

/// Stable model-facing placeholder for one request-limit image omission.
pub fn offloaded_image_text(reference: &ImageAttachmentRef, readonly_path: Option<&str>) -> String {
    let identity = format!(
        "image omitted to fit request image limits; {}.",
        image_identity(reference)
    );
    match readonly_path {
        Some(path) => format!("[{identity}{}]", normalized_access_text(reference, path)),
        None => format!("[{identity} No local normalized image path is available; ask the user to attach it again if needed.]"),
    }
}

/// Stable model-facing handle for a durable verbatim file reference.
pub fn file_handle_text(reference: &FileAttachmentRef, readonly_path: Option<&str>) -> String {
    let digest_source = reference
        .attachment_id
        .strip_prefix("sha256:")
        .unwrap_or(&reference.attachment_id);
    let digest: String = digest_source.chars().take(8).collect();
    let identity = format!(
        "File {} ({} bytes, sha256:{digest})",
        quoted(&reference.name),
        reference.bytes
    );
    match readonly_path {
        Some(path) => format!("[{identity}: verbatim read-only copy saved at {}. Read that path with your file tools when its contents are needed; copy it to a writable location before modifying it. When delegating file work, include this saved path in the delegation prompt; only subagents sharing this execution environment can read it.]", quoted(path)),
        None => format!("[{identity} was uploaded, but the current execution environment cannot access a readable path. Report that limitation if its contents are needed; do not claim to have read it.]"),
    }
}

fn image_blocks(
    block: &ImageAttachmentRef,
    offloaded: bool,
    prepared: Option<&PreparedImages>,
) -> Result<Vec<Value>, LlmError> {
    let path = prepared.and_then(|prepared| {
        prepared
            .readonly_paths
            .get(&block.attachment_id)
            .map(String::as_str)
    });
    if offloaded {
        return Ok(vec![text_block(&offloaded_image_text(block, path))]);
    }
    let prepared = prepared.ok_or_else(|| invalid("request image is missing"))?;
    let version = prepared
        .versions
        .get(&block.attachment_id)
        .ok_or_else(|| invalid("request image is missing"))?;
    if version.attachment.attachment_id != block.attachment_id
        || version.data.len() as u64 != version.bytes
    {
        return Err(invalid(
            "prepared request image metadata does not match its bytes",
        ));
    }
    let mut blocks = vec![text_block(&request_image_handle_text(block, version, path))];
    match &prepared.file_ids {
        None => blocks.push(json!({
            "type":"image",
            "source":{
                "type":"base64",
                "media_type":version.media_type.as_str(),
                "data":base64::engine::general_purpose::STANDARD.encode(version.data.as_ref())
            }
        })),
        Some(file_ids) => {
            let file_id = file_ids
                .get(&block.attachment_id)
                .ok_or_else(|| invalid("request file id is missing"))?;
            if file_id.is_empty() {
                return Err(invalid("request file id is missing"));
            }
            blocks.push(json!({"type":"image","source":{"type":"file","file_id":file_id}}));
        }
    }
    Ok(blocks)
}

fn input_blocks(
    content: &[Content],
    prepared: Option<&PreparedImages>,
) -> Result<Vec<Value>, LlmError> {
    let mut blocks = Vec::new();
    for content in content {
        match content {
            Content::Text { text } if !text.is_empty() => blocks.push(text_block(text)),
            Content::Text { .. } | Content::Reasoning { .. } | Content::ToolCall { .. } => {}
            Content::Image {
                attachment,
                offloaded,
            } => blocks.extend(image_blocks(attachment, *offloaded, prepared)?),
            Content::File { attachment } => {
                let path = prepared.and_then(|prepared| {
                    prepared
                        .readonly_paths
                        .get(&attachment.attachment_id)
                        .map(String::as_str)
                });
                blocks.push(text_block(&file_handle_text(attachment, path)));
            }
            Content::ToolAddition { .. } | Content::ToolRemoval { .. } => {
                return Err(unsupported("user/tool-result content tool-change"))
            }
        }
    }
    Ok(blocks)
}

/// Build the JSON request body for text-only `history`.
pub fn serialize(options: &Options, history: &[Message]) -> Result<Value, LlmError> {
    serialize_prepared(options, history, None)
}

/// Build a request using exact prepared image bytes, Files references, and read-only paths.
pub fn serialize_prepared(
    options: &Options,
    history: &[Message],
    prepared: Option<&PreparedImages>,
) -> Result<Value, LlmError> {
    let mut messages: Vec<Wire> = Vec::new();
    let mut system_parts: Vec<String> = Vec::new();
    let mut updates: Vec<Wire> = Vec::new();

    fn flush(messages: &mut Vec<Wire>, updates: &mut Vec<Wire>) -> Result<(), LlmError> {
        if updates.is_empty() {
            return Ok(());
        }
        if messages.last().map(|m| m.role) != Some("user") {
            return Err(unsupported(
                "system update without a preceding user or tool-result turn",
            ));
        }
        messages.append(updates);
        Ok(())
    }

    for m in history {
        match m.role {
            Role::Developer => {
                let mut blocks = Vec::new();
                for c in &m.content {
                    match c {
                        Content::Text { text } if !text.is_empty() => blocks.push(text_block(text)),
                        Content::Text { .. } => {}
                        Content::ToolAddition { name } => blocks.push(json!({"type":"tool_addition","tool":{"type":"tool_reference","name":name}})),
                        Content::ToolRemoval { name } => blocks.push(json!({"type":"tool_removal","tool":{"type":"tool_reference","name":name}})),
                        _ => return Err(unsupported("developer content")),
                    }
                }
                if !blocks.is_empty() {
                    updates.push(Wire {
                        role: "system",
                        content: blocks,
                    });
                }
                continue;
            }
            Role::System => {
                let mut texts = Vec::new();
                for c in &m.content {
                    match c {
                        Content::Text { text } => texts.push(text.as_str()),
                        _ => return Err(unsupported("non-text system content")),
                    }
                }
                let joined = texts.join("\n\n");
                if joined.is_empty() {
                    continue;
                }
                if options.system_in_history && !messages.is_empty() {
                    updates.push(Wire {
                        role: "system",
                        content: vec![text_block(&joined)],
                    });
                } else {
                    system_parts.push(joined);
                }
                continue;
            }
            _ => {}
        }

        let mut blocks: Vec<Value> = Vec::new();
        match m.role {
            Role::Assistant => {
                for c in &m.content {
                    match c {
                        Content::Text { text } if !text.is_empty() => blocks.push(text_block(text)),
                        Content::Text { .. } => {}
                        Content::Reasoning { text, signature } => {
                            let mut b = json!({"type":"thinking","thinking":text});
                            if let Some(s) = signature {
                                b["signature"] = json!(s);
                            }
                            blocks.push(b);
                        }
                        Content::ToolCall { id, name, arguments } => {
                            blocks.push(json!({"type":"tool_use","id":id,"name":name,"input":tool_input(arguments)}))
                        }
                        _ => return Err(unsupported("assistant content")),
                    }
                }
            }
            Role::Tool => {
                let inner = input_blocks(&m.content, prepared)?;
                let id = m
                    .tool_call_id
                    .clone()
                    .ok_or_else(|| invalid("tool result has no call id"))?;
                let mut b = json!({"type":"tool_result","tool_use_id":id,"content":inner});
                if let Some(e) = m.is_error {
                    b["is_error"] = json!(e);
                }
                blocks.push(b);
            }
            Role::User => blocks = input_blocks(&m.content, prepared)?,
            Role::System | Role::Developer => unreachable!(),
        }
        // Empty tool results still carry their call id; other empty turns are skipped.
        if blocks.is_empty() {
            continue;
        }
        let wire_role = if m.role == Role::Assistant {
            "assistant"
        } else {
            "user"
        };
        if wire_role == "assistant" {
            flush(&mut messages, &mut updates)?;
        }
        match messages.last_mut() {
            Some(last) if last.role == wire_role => last.content.extend(blocks),
            _ => messages.push(Wire {
                role: wire_role,
                content: blocks,
            }),
        }
    }
    flush(&mut messages, &mut updates)?;

    // Tool pairing: every tool_use is answered immediately by a following user turn.
    let mut pending: Vec<String> = Vec::new();
    for msg in &mut messages {
        match msg.role {
            "assistant" => {
                let mut seen = Vec::new();
                for b in &msg.content {
                    if b["type"] == "tool_use" {
                        let id = b["id"].as_str().unwrap_or_default().to_string();
                        if seen.contains(&id) {
                            return Err(invalid("duplicate tool call id"));
                        }
                        seen.push(id);
                    }
                }
                pending = seen;
            }
            "user" => {
                for b in &msg.content {
                    if b["type"] == "tool_result" {
                        let id = b["tool_use_id"].as_str().unwrap_or_default();
                        match pending.iter().position(|p| p == id) {
                            Some(i) => {
                                pending.remove(i);
                            }
                            None => return Err(invalid("tool result has no matching call")),
                        }
                    }
                }
                if !pending.is_empty() {
                    return Err(invalid("tool calls need immediate results"));
                }
                // Results first, otherwise in original order.
                msg.content.sort_by_key(|b| b["type"] != "tool_result");
            }
            _ => {}
        }
    }
    if !pending.is_empty() {
        return Err(invalid("history ends with unresolved tools"));
    }

    let mut body = Map::new();
    body.insert("model".into(), json!(options.model));
    body.insert("stream".into(), json!(true));
    body.insert("max_tokens".into(), json!(options.max_tokens));
    body.insert(
        "messages".into(),
        Value::Array(
            messages
                .into_iter()
                .map(|m| json!({"role":m.role,"content":m.content}))
                .collect(),
        ),
    );
    if !system_parts.is_empty() {
        body.insert("system".into(), json!(system_parts.join("\n\n")));
    }
    match options.effort {
        Some(e) => {
            body.insert("thinking".into(), json!({"type":"enabled"}));
            body.insert("output_config".into(), json!({"effort": e.wire()}));
        }
        None => {
            body.insert("thinking".into(), json!({"type":"disabled"}));
        }
    }
    if let Some(t) = options.temperature {
        body.insert("temperature".into(), json!(t));
    }
    if !options.stop_sequences.is_empty() {
        body.insert("stop_sequences".into(), json!(options.stop_sequences));
    }
    if !options.tools.is_empty() {
        let tools: Vec<Value> = options
            .tools
            .iter()
            .map(|t| {
                let mut v = json!({"name":t.name,"description":t.description,"input_schema":t.input_schema});
                if t.defer_loading {
                    v["defer_loading"] = json!(true);
                }
                v
            })
            .collect();
        body.insert("tools".into(), Value::Array(tools));
    }
    Ok(Value::Object(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn opts() -> Options {
        Options {
            model: "m".into(),
            max_tokens: 100,
            effort: None,
            temperature: None,
            stop_sequences: vec![],
            tools: vec![],
            system_in_history: false,
        }
    }
    fn msg(role: Role, content: Vec<Content>) -> Message {
        Message {
            role,
            content,
            tool_call_id: None,
            is_error: None,
        }
    }
    fn text(t: &str) -> Content {
        Content::Text { text: t.into() }
    }
    fn call(id: &str) -> Content {
        Content::ToolCall {
            id: id.into(),
            name: "bash".into(),
            arguments: "{\"c\":1}".into(),
        }
    }
    fn result(id: &str) -> Message {
        Message {
            role: Role::Tool,
            content: vec![text("ok")],
            tool_call_id: Some(id.into()),
            is_error: None,
        }
    }

    #[test]
    fn system_is_joined_and_tool_turns_pair() {
        let body = serialize(
            &opts(),
            &[
                msg(Role::System, vec![text("a")]),
                msg(Role::System, vec![text("b")]),
                msg(Role::User, vec![text("hi")]),
                msg(Role::Assistant, vec![call("1"), call("2")]),
                result("2"),
                result("1"),
                msg(Role::User, vec![text("next")]),
            ],
        )
        .unwrap();
        assert_eq!(body["system"], "a\n\nb");
        let m = body["messages"].as_array().unwrap();
        assert_eq!(m.len(), 3);
        // Tool results and the following user text merge into one user turn, results first.
        assert_eq!(m[2]["content"].as_array().unwrap().len(), 3);
        assert_eq!(m[2]["content"][0]["type"], "tool_result");
        assert_eq!(m[2]["content"][2]["type"], "text");
        assert_eq!(body["thinking"]["type"], "disabled");
    }

    #[test]
    fn rejects_unpaired_tools() {
        let e = serialize(
            &opts(),
            &[
                msg(Role::User, vec![text("x")]),
                msg(Role::Assistant, vec![call("1")]),
            ],
        )
        .unwrap_err();
        assert!(e.message.contains("unresolved"));
        let e = serialize(&opts(), &[msg(Role::User, vec![text("x")]), result("9")]).unwrap_err();
        assert!(e.message.contains("no matching call"));
        let e = serialize(
            &opts(),
            &[
                msg(Role::User, vec![text("x")]),
                msg(Role::Assistant, vec![call("1"), call("1")]),
            ],
        )
        .unwrap_err();
        assert!(e.message.contains("duplicate"));
    }

    #[test]
    fn thinking_effort_and_signature() {
        let mut o = opts();
        o.effort = Some(Effort::High);
        let body = serialize(
            &o,
            &[
                msg(Role::User, vec![text("x")]),
                msg(
                    Role::Assistant,
                    vec![
                        Content::Reasoning {
                            text: "t".into(),
                            signature: Some("s".into()),
                        },
                        text("y"),
                    ],
                ),
            ],
        )
        .unwrap();
        assert_eq!(body["output_config"]["effort"], "high");
        assert_eq!(body["messages"][1]["content"][0]["signature"], "s");
    }

    #[test]
    fn developer_updates_follow_user_turn() {
        let ok = serialize(
            &opts(),
            &[
                msg(Role::User, vec![text("x")]),
                msg(
                    Role::Developer,
                    vec![Content::ToolAddition {
                        name: "grep".into(),
                    }],
                ),
            ],
        )
        .unwrap();
        assert_eq!(ok["messages"][1]["role"], "system");
        assert!(serialize(&opts(), &[msg(Role::Developer, vec![text("x")])]).is_err());
    }

    fn image_content() -> Content {
        Content::Image {
            attachment: dsh_llm::ImageAttachmentRef {
                attachment_id: "sha256:aaaaaaaaaaaaaaaa".into(),
                media_type: dsh_llm::ImageMediaType::Png,
                bytes: 3,
                width: 2,
                height: 1,
                name: Some("cat.png".into()),
                original_dimensions: None,
            },
            offloaded: false,
        }
    }

    fn prepared() -> PreparedImages {
        let Content::Image { attachment, .. } = image_content() else {
            unreachable!()
        };
        PreparedImages {
            versions: BTreeMap::from([(
                attachment.attachment_id.clone(),
                RequestImage {
                    variant_id: "sha256:bbbb".into(),
                    attachment,
                    data: Arc::from([1u8, 2, 3]),
                    media_type: dsh_llm::ImageMediaType::Png,
                    bytes: 3,
                    width: 4,
                    height: 3,
                },
            )]),
            file_ids: None,
            readonly_paths: BTreeMap::from([(
                "sha256:aaaaaaaaaaaaaaaa".into(),
                "/workspace/cat.png".into(),
            )]),
        }
    }

    #[test]
    fn serializes_inline_images_with_exact_handle_text() {
        let prepared = prepared();
        let body = serialize_prepared(
            &opts(),
            &[msg(
                Role::User,
                vec![text("before"), image_content(), text("after")],
            )],
            Some(&prepared),
        )
        .unwrap();
        let blocks = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[1]["type"], "text");
        assert_eq!(blocks[1]["text"], "Image \"cat.png\" (sha256:aaaaaaaaaaaaaaaa); request preview 4x3px. Normalized copy (read-only; may be resized or re-encoded): \"/workspace/cat.png\" (2x1px, image/png). Source dimensions, format, and byte size may differ. Copy to a writable path ending in .png before editing.");
        assert_eq!(
            blocks[2]["source"],
            json!({"type":"base64","media_type":"image/png","data":"AQID"})
        );
    }

    #[test]
    fn serializes_file_references_and_requires_complete_file_ids() {
        let mut prepared = prepared();
        prepared.file_ids = Some(BTreeMap::from([(
            "sha256:aaaaaaaaaaaaaaaa".into(),
            "file-1".into(),
        )]));
        let body = serialize_prepared(
            &opts(),
            &[msg(Role::User, vec![image_content()])],
            Some(&prepared),
        )
        .unwrap();
        assert_eq!(
            body["messages"][0]["content"][1]["source"],
            json!({"type":"file","file_id":"file-1"})
        );
        prepared.file_ids = Some(BTreeMap::new());
        let error = serialize_prepared(
            &opts(),
            &[msg(Role::User, vec![image_content()])],
            Some(&prepared),
        )
        .unwrap_err();
        assert!(error.message.contains("file id is missing"));
    }

    #[test]
    fn images_work_inside_tool_results_and_offload_to_text() {
        let prepared = prepared();
        let history = [
            msg(Role::Assistant, vec![call("1")]),
            Message {
                role: Role::Tool,
                content: vec![image_content()],
                tool_call_id: Some("1".into()),
                is_error: None,
            },
        ];
        let body = serialize_prepared(&opts(), &history, Some(&prepared)).unwrap();
        assert_eq!(body["messages"][1]["content"][0]["type"], "tool_result");
        assert_eq!(
            body["messages"][1]["content"][0]["content"][1]["type"],
            "image"
        );
        let Content::Image { attachment, .. } = image_content() else {
            unreachable!()
        };
        let offloaded = Content::Image {
            attachment,
            offloaded: true,
        };
        let body = serialize_prepared(&opts(), &history, Some(&prepared)).unwrap();
        assert!(body["messages"][1]["content"][0]["content"][1]["type"] == "image");
        let mut history = history;
        history[1].content = vec![offloaded];
        let body = serialize_prepared(&opts(), &history, None).unwrap();
        assert_eq!(
            body["messages"][1]["content"][0]["content"][0]["type"],
            "text"
        );
        assert!(body["messages"][1]["content"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("[image omitted"));
    }

    #[test]
    fn rejects_missing_images_and_unsupported_roles_before_serialization() {
        let error = serialize_prepared(&opts(), &[msg(Role::User, vec![image_content()])], None)
            .unwrap_err();
        assert!(error.message.contains("image is missing"));
        let error = serialize_prepared(
            &opts(),
            &[msg(Role::Assistant, vec![image_content()])],
            Some(&prepared()),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::UnsupportedContent);
    }

    #[test]
    fn durable_files_project_to_handle_text_not_native_file_blocks() {
        let file = Content::File {
            attachment: FileAttachmentRef {
                attachment_id: "sha256:0123456789abcdef".into(),
                name: "notes.txt".into(),
                bytes: 42,
            },
        };
        let body = serialize(&opts(), &[msg(Role::User, vec![file])]).unwrap();
        let text = body["messages"][0]["content"][0]["text"].as_str().unwrap();
        assert!(text.starts_with("[File \"notes.txt\" (42 bytes, sha256:01234567)"));
        assert!(text.contains("cannot access a readable path"));
    }
}
