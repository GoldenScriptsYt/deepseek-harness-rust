//! Messages stream events to provider-neutral chunks. Port of `translate.ts`.

use dsh_llm::{BlockKind, ErrorCode, FinishReason, LlmError, StreamChunk, TokenUsage};
use serde_json::Value;
use std::collections::BTreeMap;

fn malformed(detail: &str) -> LlmError {
    LlmError::new(
        ErrorCode::MalformedResponse,
        format!("DeepSeek Messages stream: {detail}"),
    )
}

struct Block {
    kind: BlockKind,
    json: String,
    signature: Option<String>,
    closed: bool,
}

/// Stateful translator. Feed each parsed SSE event to [`Translator::push`].
#[derive(Default)]
pub struct Translator {
    blocks: BTreeMap<usize, Block>,
    usage: TokenUsage,
    started: bool,
    reason: Option<FinishReason>,
    refused: bool,
    refusal_details: Option<Value>,
    finished: bool,
    /// Set when the stream ended in a failure that follows already-returned chunks
    /// (a refusal delivers final usage first). Callers drain it after each `push`.
    pub failure: Option<LlmError>,
}

fn index_of(event: &Value) -> Result<usize, LlmError> {
    event
        .get("index")
        .and_then(Value::as_u64)
        .map(|i| i as usize)
        .ok_or_else(|| malformed("invalid block index"))
}

fn update_usage(usage: &mut TokenUsage, raw: &Value) -> Result<(), LlmError> {
    for (wire, slot) in [
        ("input_tokens", &mut usage.input_tokens),
        ("output_tokens", &mut usage.output_tokens),
        ("cache_read_input_tokens", &mut usage.cache_read_tokens),
        ("cache_creation_input_tokens", &mut usage.cache_write_tokens),
    ] {
        match raw.get(wire) {
            None | Some(Value::Null) => {}
            Some(v) => {
                *slot = v
                    .as_u64()
                    .ok_or_else(|| malformed(&format!("invalid {wire}")))?
            }
        }
    }
    Ok(())
}

fn refusal(details: Option<&Value>) -> LlmError {
    let mut msg = String::from("DeepSeek refused to answer under its content safety review");
    if let Some(e) = details
        .and_then(|d| d.get("explanation"))
        .and_then(Value::as_str)
    {
        if !e.is_empty() {
            msg.push_str(": ");
            msg.push_str(e);
        }
    }
    LlmError::new(ErrorCode::ContentRefused, msg)
}

impl Translator {
    /// Translate one event, returning the chunks it produces.
    pub fn push(&mut self, event: &Value) -> Result<Vec<StreamChunk>, LlmError> {
        let ty = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut out = Vec::new();
        if self.finished {
            return Err(malformed("event after message_stop"));
        }
        match ty {
            "message_start" => {
                if self.started {
                    return Err(malformed("duplicate message_start"));
                }
                self.started = true;
                if let Some(u) = event.pointer("/message/usage") {
                    update_usage(&mut self.usage, u)?;
                }
                return Ok(out);
            }
            "content_block_start"
            | "content_block_delta"
            | "content_block_stop"
            | "message_delta"
            | "message_stop" => {}
            // Anthropic permits additional event types such as ping.
            _ => return Ok(out),
        }
        if !self.started {
            return Err(malformed("event precedes message_start"));
        }

        match ty {
            "content_block_start" => {
                let index = index_of(event)?;
                if self.blocks.contains_key(&index) || self.reason.is_some() {
                    return Err(malformed("duplicate or late block"));
                }
                let cb = event.get("content_block").unwrap_or(&Value::Null);
                match cb.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        self.blocks.insert(
                            index,
                            Block {
                                kind: BlockKind::Text,
                                json: String::new(),
                                signature: None,
                                closed: false,
                            },
                        );
                        out.push(StreamChunk::BlockStart {
                            index,
                            kind: BlockKind::Text,
                        });
                        let t = cb.get("text").and_then(Value::as_str).unwrap_or("");
                        if !t.is_empty() {
                            out.push(StreamChunk::TextDelta {
                                index,
                                text: t.into(),
                            });
                        }
                    }
                    Some("thinking") => {
                        let sig = cb
                            .get("signature")
                            .and_then(Value::as_str)
                            .map(String::from);
                        self.blocks.insert(
                            index,
                            Block {
                                kind: BlockKind::Reasoning,
                                json: String::new(),
                                signature: sig,
                                closed: false,
                            },
                        );
                        out.push(StreamChunk::BlockStart {
                            index,
                            kind: BlockKind::Reasoning,
                        });
                        let t = cb.get("thinking").and_then(Value::as_str).unwrap_or("");
                        if !t.is_empty() {
                            out.push(StreamChunk::ReasoningDelta {
                                index,
                                text: t.into(),
                            });
                        }
                    }
                    Some("tool_use") => {
                        let id = cb.get("id").and_then(Value::as_str).unwrap_or("");
                        let name = cb.get("name").and_then(Value::as_str).unwrap_or("");
                        if id.is_empty() || name.is_empty() {
                            return Err(malformed("empty tool identity"));
                        }
                        self.blocks.insert(
                            index,
                            Block {
                                kind: BlockKind::ToolCall,
                                json: String::new(),
                                signature: None,
                                closed: false,
                            },
                        );
                        out.push(StreamChunk::BlockStart {
                            index,
                            kind: BlockKind::ToolCall,
                        });
                        out.push(StreamChunk::ToolCallDelta {
                            index,
                            id: Some(id.into()),
                            name: Some(name.into()),
                            arguments_delta: String::new(),
                        });
                    }
                    other => {
                        return Err(LlmError::new(
                            ErrorCode::UnsupportedContent,
                            format!(
                                "DeepSeek Messages does not support response block {}",
                                other.unwrap_or("?")
                            ),
                        ))
                    }
                }
            }
            "content_block_delta" => {
                let index = index_of(event)?;
                let block = match self.blocks.get_mut(&index) {
                    Some(b) if !b.closed => b,
                    _ => return Err(malformed("delta/stop without an open block")),
                };
                let delta = event.get("delta").unwrap_or(&Value::Null);
                let field = |k: &str| delta.get(k).and_then(Value::as_str).map(String::from);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") if block.kind == BlockKind::Text => {
                        out.push(StreamChunk::TextDelta {
                            index,
                            text: field("text").ok_or_else(|| malformed("invalid text"))?,
                        });
                    }
                    Some("thinking_delta") if block.kind == BlockKind::Reasoning => {
                        out.push(StreamChunk::ReasoningDelta {
                            index,
                            text: field("thinking").ok_or_else(|| malformed("invalid thinking"))?,
                        });
                    }
                    Some("signature_delta") if block.kind == BlockKind::Reasoning => {
                        let s = field("signature").ok_or_else(|| malformed("invalid signature"))?;
                        block.signature.get_or_insert_with(String::new).push_str(&s);
                    }
                    Some("input_json_delta") if block.kind == BlockKind::ToolCall => {
                        let p = field("partial_json")
                            .ok_or_else(|| malformed("invalid partial_json"))?;
                        block.json.push_str(&p);
                        out.push(StreamChunk::ToolCallDelta {
                            index,
                            id: None,
                            name: None,
                            arguments_delta: p,
                        });
                    }
                    other => {
                        return Err(malformed(&format!(
                            "unsupported delta {}",
                            other.unwrap_or("?")
                        )))
                    }
                }
            }
            "content_block_stop" => {
                let index = index_of(event)?;
                match self.blocks.get_mut(&index) {
                    Some(b) if !b.closed => b.closed = true,
                    _ => return Err(malformed("delta/stop without an open block")),
                }
                out.push(StreamChunk::BlockEnd { index });
            }
            "message_delta" => {
                let reason = event.pointer("/delta/stop_reason").and_then(Value::as_str);
                match reason {
                    Some("refusal") => self.refused = true,
                    Some("end_turn") | Some("stop_sequence") => {
                        self.reason = Some(FinishReason::Stop)
                    }
                    Some("tool_use") => self.reason = Some(FinishReason::ToolCalls),
                    Some("max_tokens") => self.reason = Some(FinishReason::MaxTokens),
                    Some(other) => {
                        return Err(malformed(&format!("unsupported stop reason {other}")))
                    }
                    None => {}
                }
                if let Some(u) = event.get("usage") {
                    update_usage(&mut self.usage, u)?;
                }
                if self.refused {
                    self.refusal_details = event.pointer("/delta/stop_details").cloned();
                }
            }
            "message_stop" => {
                self.finished = true;
                self.usage.total_tokens = self.usage.input_tokens
                    + self.usage.output_tokens
                    + self.usage.cache_read_tokens
                    + self.usage.cache_write_tokens;
                if self.refused {
                    out.push(StreamChunk::Usage(self.usage));
                    self.failure = Some(refusal(self.refusal_details.as_ref()));
                    return Ok(out);
                }
                let reason = match self.reason {
                    Some(r) if self.blocks.values().all(|b| b.closed) => r,
                    _ => {
                        return Err(malformed(
                            "message_stop without settled blocks and stop reason",
                        ))
                    }
                };
                if self.blocks.is_empty() && reason == FinishReason::Stop {
                    return Err(LlmError::new(
                        ErrorCode::EmptyResponse,
                        "DeepSeek Messages returned no content",
                    ));
                }
                if reason != FinishReason::MaxTokens {
                    for b in self
                        .blocks
                        .values()
                        .filter(|b| b.kind == BlockKind::ToolCall)
                    {
                        let ok =
                            serde_json::from_str::<Value>(&b.json).is_ok_and(|v| v.is_object());
                        if !ok {
                            return Err(malformed("tool input is invalid JSON"));
                        }
                    }
                }
                out.push(StreamChunk::Usage(self.usage));
                let replay = self.blocks.values().map(|b| b.signature.clone()).collect();
                out.push(StreamChunk::Finish { reason, replay });
            }
            _ => unreachable!(),
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(events: Vec<Value>) -> Result<Vec<StreamChunk>, LlmError> {
        let mut t = Translator::default();
        let mut all = Vec::new();
        for e in events {
            all.extend(t.push(&e)?);
        }
        Ok(all)
    }

    fn start() -> Value {
        json!({"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":2}}})
    }

    #[test]
    fn text_turn() {
        let chunks = run(vec![
            start(),
            json!({"type":"ping"}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}),
            json!({"type":"message_stop"}),
        ])
        .unwrap();
        assert!(matches!(&chunks[1], StreamChunk::TextDelta { text, .. } if text == "hi"));
        match &chunks[chunks.len() - 2] {
            StreamChunk::Usage(u) => {
                assert_eq!(
                    (
                        u.input_tokens,
                        u.output_tokens,
                        u.cache_read_tokens,
                        u.total_tokens
                    ),
                    (10, 3, 2, 15)
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            chunks.last().unwrap(),
            StreamChunk::Finish {
                reason: FinishReason::Stop,
                ..
            }
        ));
    }

    #[test]
    fn thinking_signature_is_replayed_and_tool_args_assemble() {
        let chunks = run(vec![
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hm"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t1","name":"bash","input":{}}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"ls\"}"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            json!({"type":"message_stop"}),
        ])
        .unwrap();
        match chunks.last().unwrap() {
            StreamChunk::Finish { reason, replay } => {
                assert_eq!(*reason, FinishReason::ToolCalls);
                assert_eq!(replay, &vec![Some("sig".to_string()), None]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_protocol_violations() {
        assert!(run(vec![json!({"type":"content_block_stop","index":0})]).is_err());
        assert!(run(vec![start(), start()]).is_err());
        let e = run(vec![
            start(),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_stop"}),
        ])
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::EmptyResponse);
        let e = run(vec![
            start(),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"a","name":"b","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{bad"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            json!({"type":"message_stop"}),
        ])
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::MalformedResponse);
    }

    #[test]
    fn refusal_surfaces_usage_then_fails() {
        let mut t = Translator::default();
        t.push(&start()).unwrap();
        t.push(&json!({"type":"message_delta","delta":{"stop_reason":"refusal"}}))
            .unwrap();
        let chunks = t.push(&json!({"type":"message_stop"})).unwrap();
        assert!(matches!(chunks.last(), Some(StreamChunk::Usage(_))));
        assert_eq!(t.failure.unwrap().code, ErrorCode::ContentRefused);
    }
}
