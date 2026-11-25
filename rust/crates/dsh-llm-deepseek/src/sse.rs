//! Incremental SSE framing with stream-safe UTF-8 and CRLF handling.

use crate::client::provider_error;
use dsh_llm::{ErrorCode, LlmError};
use serde_json::Value;

/// One decoded item from the byte stream.
#[derive(Debug, Clone, PartialEq)]
pub enum SseItem {
    /// A `:` comment line (heartbeat).
    Comment,
    Frame {
        event: Option<String>,
        data: String,
    },
}

/// Feed bytes or text; complete frames come out as they terminate.
#[derive(Default)]
pub struct SseDecoder {
    buf: String,
    utf8_carry: Vec<u8>,
    pending_cr: bool,
    started: bool,
}

impl SseDecoder {
    /// Feed arbitrary network bytes. Incomplete UTF-8 and CRLF boundaries are retained.
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Vec<SseItem> {
        self.utf8_carry.extend_from_slice(chunk);
        let mut decoded = String::new();
        loop {
            match std::str::from_utf8(&self.utf8_carry) {
                Ok(text) => {
                    decoded.push_str(text);
                    self.utf8_carry.clear();
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    // `valid_up_to` is always a UTF-8 boundary.
                    decoded.push_str(std::str::from_utf8(&self.utf8_carry[..valid]).unwrap());
                    if let Some(len) = error.error_len() {
                        decoded.push('\u{fffd}');
                        self.utf8_carry.drain(..valid + len);
                    } else {
                        self.utf8_carry.drain(..valid);
                        break;
                    }
                }
            }
        }
        for character in decoded.chars() {
            if !self.started {
                self.started = true;
                if character == '\u{feff}' {
                    continue;
                }
            }
            if self.pending_cr {
                self.pending_cr = false;
                if character == '\n' {
                    continue;
                }
            }
            if character == '\r' {
                self.buf.push('\n');
                self.pending_cr = true;
            } else {
                self.buf.push(character);
            }
        }
        self.take_frames()
    }

    /// Feed text chunks while preserving the same boundary behavior as [`Self::push_bytes`].
    pub fn push(&mut self, chunk: &str) -> Vec<SseItem> {
        self.push_bytes(chunk.as_bytes())
    }

    fn take_frames(&mut self) -> Vec<SseItem> {
        let mut out = Vec::new();
        while let Some(end) = self.buf.find("\n\n") {
            let block: String = self.buf.drain(..end + 2).collect();
            let mut event = None;
            let mut data: Vec<&str> = Vec::new();
            let mut comment = false;
            for line in block.lines() {
                if line.is_empty() {
                    continue;
                }
                if line.starts_with(':') {
                    comment = true;
                } else if let Some(v) = line.strip_prefix("event:") {
                    event = Some(v.strip_prefix(' ').unwrap_or(v).to_string());
                } else if let Some(v) = line.strip_prefix("data:") {
                    data.push(v.strip_prefix(' ').unwrap_or(v));
                }
            }
            if comment {
                out.push(SseItem::Comment);
            }
            if !data.is_empty() {
                out.push(SseItem::Frame {
                    event,
                    data: data.join("\n"),
                });
            }
        }
        out
    }
}

/// Validate one frame as a typed JSON event. `error` events use provider classification.
pub fn parse_event(event: Option<&str>, data: &str) -> Result<Value, LlmError> {
    let malformed = |m: &str| LlmError::new(ErrorCode::MalformedResponse, m);
    let value: Value = serde_json::from_str(data)
        .map_err(|_| malformed("DeepSeek Messages SSE contains invalid JSON"))?;
    let ty = value.get("type").and_then(Value::as_str);
    match ty {
        Some(t) if event.is_none_or(|e| e == t) => {
            if t == "error" {
                return Err(provider_error(&value, None, None));
            }
            Ok(value)
        }
        _ => Err(malformed("DeepSeek Messages SSE event type mismatch")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frames_split_across_chunks_and_tail_is_dropped() {
        let mut d = SseDecoder::default();
        assert!(d.push("event: ping\ndata: {\"ty").is_empty());
        let items = d.push("pe\":\"ping\"}\n\n: hb\n\ndata: {\"type\":\"x\"");
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], SseItem::Frame { event: Some(e), .. } if e == "ping"));
        assert_eq!(items[1], SseItem::Comment);
    }

    #[test]
    fn crlf_is_normalized_even_when_split_between_calls() {
        for chunks in [vec!["data: x\r", "\n\r", "\n"], vec!["data: x\r\n", "\r\n"]] {
            let mut d = SseDecoder::default();
            let items: Vec<_> = chunks.into_iter().flat_map(|chunk| d.push(chunk)).collect();
            assert_eq!(
                items,
                vec![SseItem::Frame {
                    event: None,
                    data: "x".into()
                }]
            );
        }
    }

    #[test]
    fn utf8_can_split_at_every_byte_boundary() {
        let wire = "data: {\"type\":\"message_stop\",\"text\":\"猫\"}\r\n\r\n".as_bytes();
        for split in 0..=wire.len() {
            let mut d = SseDecoder::default();
            let mut items = d.push_bytes(&wire[..split]);
            items.extend(d.push_bytes(&wire[split..]));
            assert_eq!(items.len(), 1, "split {split}");
            if let SseItem::Frame { data, .. } = &items[0] {
                assert_eq!(serde_json::from_str::<Value>(data).unwrap()["text"], "猫");
            }
        }
    }

    #[test]
    fn leading_bom_is_ignored_at_every_byte_boundary() {
        let wire = "\u{feff}data: {\"type\":\"message_stop\",\"text\":\"猫\"}\r\n\r\n".as_bytes();
        for split in 0..=wire.len() {
            let mut decoder = SseDecoder::default();
            let mut items = decoder.push_bytes(&wire[..split]);
            items.extend(decoder.push_bytes(&wire[split..]));
            assert_eq!(items.len(), 1, "split {split}");
            if let SseItem::Frame { data, .. } = &items[0] {
                assert_eq!(serde_json::from_str::<Value>(data).unwrap()["text"], "猫");
            }
        }
    }

    #[test]
    fn invalid_utf8_is_replaced_without_losing_the_frame() {
        let mut d = SseDecoder::default();
        let mut wire = b"data: ".to_vec();
        wire.extend_from_slice(&[0xff]);
        wire.extend_from_slice(b"\n\n");
        assert_eq!(
            d.push_bytes(&wire),
            vec![SseItem::Frame {
                event: None,
                data: "\u{fffd}".into()
            }]
        );
    }

    #[test]
    fn event_name_must_match_type() {
        assert!(parse_event(Some("a"), "{\"type\":\"b\"}").is_err());
        assert!(parse_event(None, "{\"type\":\"b\"}").is_ok());
        assert!(parse_event(None, "not json").is_err());
    }

    #[test]
    fn in_band_errors_keep_provider_classification() {
        let error = parse_event(
            None,
            &json!({"type":"error","error":{"type":"rate_limit_error","message":"slow"}})
                .to_string(),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::RateLimit);
        assert_eq!(error.message, "slow");
    }
}
