//! Serializable Assistant stream frames, compact records, and reconnect baselines.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Failure while compacting or reading Assistant stream data.
#[derive(Debug, thiserror::Error)]
pub enum AssistantStreamError {
    #[error("assistant stream time must be a non-negative safe integer, got {0}")]
    InvalidTime(u64),
    #[error("{label} index must be a non-negative safe integer, got {value}")]
    InvalidIndex { label: &'static str, value: u64 },
    #[error("invalid assistant stream chunk: {reason}")]
    InvalidChunk { reason: String },
    #[error("invalid assistant stream {record}: {reason}")]
    InvalidRecord {
        record: &'static str,
        reason: String,
    },
    #[error("assistant stream chunk is not JSON-serializable: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// One model chunk paired with its original timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimedStreamChunk {
    pub time: u64,
    pub chunk: Value,
}

/// Lossless compact representation of one model-stream attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum AssistantStreamRecord {
    TextChunks {
        time0: u64,
        index: u64,
        dt: Vec<i64>,
        texts: Vec<String>,
    },
    ReasoningChunks {
        time0: u64,
        index: u64,
        dt: Vec<i64>,
        texts: Vec<String>,
    },
    ToolCallChunks {
        time0: u64,
        index: u64,
        dt: Vec<i64>,
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        args: Vec<String>,
    },
    Chunk {
        time: u64,
        chunk: Value,
    },
}

/// Durable event selected by a committed Assistant stream terminal frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssistantSettlementEventType {
    #[serde(rename = "assistant/message")]
    Message,
    #[serde(rename = "assistant/attempt")]
    Attempt,
}

/// Why one Assistant stream ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum AssistantStreamOutcome {
    Committed {
        #[serde(rename = "eventType")]
        event_type: AssistantSettlementEventType,
        seq: u64,
    },
    Abandoned,
}

/// Browser wire form of one process-local Assistant frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SessionAssistantStreamFrame {
    Start {
        #[serde(rename = "attemptId")]
        attempt_id: String,
        revision: u64,
        #[serde(rename = "startedAfterSeq")]
        started_after_seq: i64,
        turn: u64,
        step: u64,
    },
    Chunk {
        #[serde(rename = "attemptId")]
        attempt_id: String,
        revision: u64,
        index: u64,
        time: u64,
        chunk: Value,
    },
    End {
        #[serde(rename = "attemptId")]
        attempt_id: String,
        revision: u64,
        index: u64,
        outcome: AssistantStreamOutcome,
    },
}

/// One active Assistant attempt in a reconnect opening snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionAssistantStreamAttempt {
    #[serde(rename = "attemptId")]
    pub attempt_id: String,
    /// Last durable Session seq observed at start; `-1` means no durable row existed.
    pub started_after_seq: i64,
    pub turn: u64,
    pub step: u64,
    pub next_index: u64,
    pub stream: Vec<AssistantStreamRecord>,
}

/// Complete process-local Assistant state at one follow opening.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionAssistantStreamBaseline {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_attempt: Option<SessionAssistantStreamAttempt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunKind {
    Text,
    Reasoning,
    Tool,
}

#[derive(Debug, Clone)]
struct MutableRun {
    kind: RunKind,
    time0: u64,
    index: u64,
    dt: Vec<i64>,
    texts: Vec<String>,
    id: String,
    name: Option<String>,
    args: Vec<String>,
    last_time: u64,
}

#[derive(Debug, Clone)]
enum MutableRecord {
    Run(MutableRun),
    Raw { time: u64, chunk: Value },
}

/// Incrementally compacts one attempt without retaining a second raw-chunk list.
#[derive(Debug, Clone, Default)]
pub struct AssistantStreamAccumulator {
    records: Vec<MutableRecord>,
}

impl AssistantStreamAccumulator {
    /// Add one timed chunk and return its detached wire value.
    pub fn push(
        &mut self,
        value: TimedStreamChunk,
    ) -> Result<TimedStreamChunk, AssistantStreamError> {
        safe_time(value.time)?;
        let object = value
            .chunk
            .as_object()
            .ok_or_else(|| invalid_chunk("expected a JSON object"))?;
        let kind = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_chunk("expected a string type"))?;
        let timed = TimedStreamChunk {
            time: value.time,
            chunk: value.chunk.clone(),
        };
        match kind {
            "text-delta" | "reasoning-delta" => {
                let index = chunk_index(object, kind)?;
                let text = chunk_string(object, "text", kind)?;
                self.push_run(
                    RunKind::if_bool(kind == "text-delta"),
                    index,
                    text,
                    timed.time,
                );
            }
            "tool-call-delta" => {
                let index = chunk_index(object, kind)?;
                let id = chunk_string(object, "id", kind)?;
                let name = if object.contains_key("name") {
                    Some(chunk_string(object, "name", kind)?)
                } else {
                    None
                };
                let arguments = chunk_string(object, "argumentsDelta", kind)?;
                if id.is_empty() || name.as_deref() == Some("") {
                    self.records.push(MutableRecord::Raw {
                        time: timed.time,
                        chunk: timed.chunk.clone(),
                    });
                } else {
                    self.push_tool_run(index, id, name, arguments, timed.time);
                }
            }
            "block-start" | "block-end" | "usage" | "finish" => {
                self.records.push(MutableRecord::Raw {
                    time: timed.time,
                    chunk: timed.chunk.clone(),
                });
            }
            other => return Err(invalid_chunk(format!("unsupported type {other:?}"))),
        }
        Ok(timed)
    }

    /// Return a detached compact stream suitable for a durable event or baseline.
    pub fn snapshot(&self) -> Vec<AssistantStreamRecord> {
        self.records
            .iter()
            .map(|record| match record {
                MutableRecord::Raw { time, chunk } => AssistantStreamRecord::Chunk {
                    time: *time,
                    chunk: chunk.clone(),
                },
                MutableRecord::Run(run) => match run.kind {
                    RunKind::Text => AssistantStreamRecord::TextChunks {
                        time0: run.time0,
                        index: run.index,
                        dt: run.dt.clone(),
                        texts: run.texts.clone(),
                    },
                    RunKind::Reasoning => AssistantStreamRecord::ReasoningChunks {
                        time0: run.time0,
                        index: run.index,
                        dt: run.dt.clone(),
                        texts: run.texts.clone(),
                    },
                    RunKind::Tool => AssistantStreamRecord::ToolCallChunks {
                        time0: run.time0,
                        index: run.index,
                        dt: run.dt.clone(),
                        id: run.id.clone(),
                        name: run.name.clone(),
                        args: run.args.clone(),
                    },
                },
            })
            .collect()
    }

    fn push_run(&mut self, kind: RunKind, index: u64, text: String, time: u64) {
        let append = match self.records.last() {
            Some(MutableRecord::Run(previous)) => {
                previous.kind == kind
                    && previous.index == index
                    && safe_gap(previous.last_time, time).is_some()
            }
            _ => false,
        };
        if append {
            let Some(MutableRecord::Run(previous)) = self.records.last_mut() else {
                unreachable!()
            };
            previous
                .dt
                .push(safe_gap(previous.last_time, time).expect("checked above"));
            previous.texts.push(text);
            previous.last_time = time;
        } else {
            self.records.push(MutableRecord::Run(MutableRun {
                kind,
                time0: time,
                index,
                dt: Vec::new(),
                texts: vec![text],
                id: String::new(),
                name: None,
                args: Vec::new(),
                last_time: time,
            }));
        }
    }

    fn push_tool_run(
        &mut self,
        index: u64,
        id: String,
        name: Option<String>,
        args: String,
        time: u64,
    ) {
        let append = match self.records.last() {
            Some(MutableRecord::Run(previous)) => {
                previous.kind == RunKind::Tool
                    && previous.index == index
                    && previous.id == id
                    && previous.name == name
                    && safe_gap(previous.last_time, time).is_some()
            }
            _ => false,
        };
        if append {
            let Some(MutableRecord::Run(previous)) = self.records.last_mut() else {
                unreachable!()
            };
            previous
                .dt
                .push(safe_gap(previous.last_time, time).expect("checked above"));
            previous.args.push(args);
            previous.last_time = time;
        } else {
            self.records.push(MutableRecord::Run(MutableRun {
                kind: RunKind::Tool,
                time0: time,
                index,
                dt: Vec::new(),
                texts: Vec::new(),
                id,
                name,
                args: vec![args],
                last_time: time,
            }));
        }
    }
}

impl RunKind {
    fn if_bool(text: bool) -> Self {
        if text {
            Self::Text
        } else {
            Self::Reasoning
        }
    }
}

/// Expand compact records into the exact timed chunk sequence.
pub fn expand_assistant_stream(
    stream: &[AssistantStreamRecord],
) -> Result<Vec<TimedStreamChunk>, AssistantStreamError> {
    let mut chunks = Vec::new();
    for record in stream {
        match record {
            AssistantStreamRecord::Chunk { time, chunk } => {
                safe_time(*time)?;
                if !chunk.is_object() {
                    return Err(invalid_record("chunk", "raw chunk must be a JSON object"));
                }
                chunks.push(TimedStreamChunk {
                    time: *time,
                    chunk: chunk.clone(),
                });
            }
            AssistantStreamRecord::TextChunks {
                time0,
                index,
                dt,
                texts,
            } => {
                expand_run(
                    RunKind::Text,
                    *time0,
                    *index,
                    dt,
                    texts,
                    "",
                    None,
                    &mut chunks,
                )?;
            }
            AssistantStreamRecord::ReasoningChunks {
                time0,
                index,
                dt,
                texts,
            } => {
                expand_run(
                    RunKind::Reasoning,
                    *time0,
                    *index,
                    dt,
                    texts,
                    "",
                    None,
                    &mut chunks,
                )?;
            }
            AssistantStreamRecord::ToolCallChunks {
                time0,
                index,
                dt,
                id,
                name,
                args,
            } => {
                if id.is_empty() {
                    return Err(invalid_record("tool-call-chunks", "id must be non-empty"));
                }
                if name.as_deref() == Some("") {
                    return Err(invalid_record(
                        "tool-call-chunks",
                        "name must be non-empty when present",
                    ));
                }
                expand_run(
                    RunKind::Tool,
                    *time0,
                    *index,
                    dt,
                    args,
                    id,
                    name.as_deref(),
                    &mut chunks,
                )?;
            }
        }
    }
    Ok(chunks)
}

#[allow(clippy::too_many_arguments)]
fn expand_run(
    kind: RunKind,
    time0: u64,
    index: u64,
    dt: &[i64],
    members: &[String],
    id: &str,
    name: Option<&str>,
    output: &mut Vec<TimedStreamChunk>,
) -> Result<(), AssistantStreamError> {
    let label = match kind {
        RunKind::Text => "text-chunks",
        RunKind::Reasoning => "reasoning-chunks",
        RunKind::Tool => "tool-call-chunks",
    };
    safe_time(time0)?;
    safe_index(index, label)?;
    if members.is_empty() {
        return Err(invalid_record(label, "members must be non-empty"));
    }
    if dt.len() != members.len() - 1 {
        return Err(invalid_record(
            label,
            "dt length must be one less than its members",
        ));
    }
    if dt.iter().any(|gap| gap.unsigned_abs() > MAX_SAFE_INTEGER) {
        return Err(invalid_record(label, "dt must contain safe integers"));
    }
    let mut time = i128::from(time0);
    for (member_index, member) in members.iter().enumerate() {
        if member_index > 0 {
            time += i128::from(dt[member_index - 1]);
        }
        if time < 0 || time > i128::from(MAX_SAFE_INTEGER) {
            return Err(invalid_record(
                label,
                "member times must stay non-negative safe integers",
            ));
        }
        let chunk = match kind {
            RunKind::Text => json!({ "type": "text-delta", "index": index, "text": member }),
            RunKind::Reasoning => {
                json!({ "type": "reasoning-delta", "index": index, "text": member })
            }
            RunKind::Tool => {
                let mut value = json!({
                    "type": "tool-call-delta",
                    "index": index,
                    "id": id,
                    "argumentsDelta": member,
                });
                if let Some(name) = name {
                    value["name"] = Value::String(name.to_owned());
                }
                value
            }
        };
        output.push(TimedStreamChunk {
            time: time as u64,
            chunk,
        });
    }
    Ok(())
}

#[derive(Debug)]
struct ActiveAttempt {
    attempt_id: String,
    started_after_seq: i64,
    turn: u64,
    step: u64,
    stream: AssistantStreamAccumulator,
    next_index: u64,
}

/// Folds dense Assistant frames into one cached reconnect baseline.
#[derive(Debug, Default)]
pub struct SessionAssistantStreamAccumulator {
    active_attempt: Option<ActiveAttempt>,
    revision: u64,
    snapshot_value: Arc<SessionAssistantStreamBaseline>,
    dirty: bool,
}

impl SessionAssistantStreamAccumulator {
    /// Fold one trusted frame using the durable cursor observed with it.
    pub fn accept(
        &mut self,
        frame: SessionAssistantStreamFrame,
        durable_cursor: i64,
    ) -> Result<(), AssistantStreamError> {
        if matches!(
            &frame,
            SessionAssistantStreamFrame::Start { revision: 1, .. }
        ) && self.revision != 0
        {
            self.active_attempt = None;
            self.revision = 0;
        }
        if self.revision.checked_add(1) != Some(frame_revision(&frame)) {
            self.active_attempt = None;
            self.revision = frame_revision(&frame);
            self.dirty = true;
            return Ok(());
        }
        self.revision = frame_revision(&frame);
        match frame {
            SessionAssistantStreamFrame::Start {
                attempt_id,
                turn,
                step,
                ..
            } => {
                self.active_attempt = Some(ActiveAttempt {
                    attempt_id,
                    started_after_seq: durable_cursor,
                    turn,
                    step,
                    stream: AssistantStreamAccumulator::default(),
                    next_index: 0,
                });
            }
            SessionAssistantStreamFrame::Chunk {
                attempt_id,
                index,
                time,
                chunk,
                ..
            } => {
                let accepted = match self.active_attempt.as_ref() {
                    Some(attempt) => {
                        attempt.attempt_id == attempt_id && index == attempt.next_index
                    }
                    None => false,
                };
                if !accepted {
                    self.active_attempt = None;
                } else if let Some(attempt) = self.active_attempt.as_mut() {
                    if let Err(error) = attempt.stream.push(TimedStreamChunk { time, chunk }) {
                        self.active_attempt = None;
                        self.dirty = true;
                        return Err(error);
                    }
                    attempt.next_index += 1;
                }
            }
            SessionAssistantStreamFrame::End { .. } => self.active_attempt = None,
        }
        self.dirty = true;
        Ok(())
    }

    /// Return the identity-stable baseline for the latest accepted revision.
    pub fn snapshot(&mut self) -> Arc<SessionAssistantStreamBaseline> {
        if !self.dirty {
            return Arc::clone(&self.snapshot_value);
        }
        let active_attempt =
            self.active_attempt
                .as_ref()
                .map(|attempt| SessionAssistantStreamAttempt {
                    attempt_id: attempt.attempt_id.clone(),
                    started_after_seq: attempt.started_after_seq,
                    turn: attempt.turn,
                    step: attempt.step,
                    next_index: attempt.next_index,
                    stream: attempt.stream.snapshot(),
                });
        self.snapshot_value = Arc::new(SessionAssistantStreamBaseline {
            revision: self.revision,
            active_attempt,
        });
        self.dirty = false;
        Arc::clone(&self.snapshot_value)
    }
}

fn frame_revision(frame: &SessionAssistantStreamFrame) -> u64 {
    match frame {
        SessionAssistantStreamFrame::Start { revision, .. }
        | SessionAssistantStreamFrame::Chunk { revision, .. }
        | SessionAssistantStreamFrame::End { revision, .. } => *revision,
    }
}

fn safe_time(value: u64) -> Result<(), AssistantStreamError> {
    if value > MAX_SAFE_INTEGER {
        Err(AssistantStreamError::InvalidTime(value))
    } else {
        Ok(())
    }
}

fn safe_index(value: u64, label: &'static str) -> Result<(), AssistantStreamError> {
    if value > MAX_SAFE_INTEGER {
        Err(AssistantStreamError::InvalidIndex { label, value })
    } else {
        Ok(())
    }
}

fn safe_gap(previous: u64, next: u64) -> Option<i64> {
    let gap = i128::from(next) - i128::from(previous);
    if gap.unsigned_abs() <= u128::from(MAX_SAFE_INTEGER) {
        Some(gap as i64)
    } else {
        None
    }
}

fn chunk_index(
    object: &serde_json::Map<String, Value>,
    label: &str,
) -> Result<u64, AssistantStreamError> {
    let value = object
        .get("index")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_chunk(format!("{label} index must be a non-negative integer")))?;
    safe_index(value, "chunk")?;
    Ok(value)
}

fn chunk_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
    label: &str,
) -> Result<String, AssistantStreamError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_chunk(format!("{label} {field} must be a string")))
}

fn invalid_chunk(reason: impl Into<String>) -> AssistantStreamError {
    AssistantStreamError::InvalidChunk {
        reason: reason.into(),
    }
}

fn invalid_record(record: &'static str, reason: impl Into<String>) -> AssistantStreamError {
    AssistantStreamError::InvalidRecord {
        record,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn timed(time: u64, chunk: Value) -> TimedStreamChunk {
        TimedStreamChunk { time, chunk }
    }

    fn start(revision: u64, attempt_id: &str) -> SessionAssistantStreamFrame {
        SessionAssistantStreamFrame::Start {
            attempt_id: attempt_id.into(),
            revision,
            started_after_seq: -1,
            turn: 1,
            step: 2,
        }
    }

    fn chunk(
        revision: u64,
        attempt_id: &str,
        index: u64,
        time: u64,
        text: &str,
    ) -> SessionAssistantStreamFrame {
        SessionAssistantStreamFrame::Chunk {
            attempt_id: attempt_id.into(),
            revision,
            index,
            time,
            chunk: json!({"type":"text-delta","index":0,"text":text}),
        }
    }

    #[test]
    fn wire_types_use_the_typescript_field_names() {
        let frame = start(1, "session:1");
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({
                "type":"start",
                "attemptId":"session:1",
                "revision":1,
                "startedAfterSeq":-1,
                "turn":1,
                "step":2,
            })
        );
        assert_eq!(
            serde_json::from_value::<SessionAssistantStreamFrame>(
                serde_json::to_value(&frame).unwrap()
            )
            .unwrap(),
            frame
        );

        let end = SessionAssistantStreamFrame::End {
            attempt_id: "session:1".into(),
            revision: 2,
            index: 1,
            outcome: AssistantStreamOutcome::Committed {
                event_type: AssistantSettlementEventType::Message,
                seq: 9,
            },
        };
        assert_eq!(
            serde_json::to_value(end).unwrap(),
            json!({
                "type":"end",
                "attemptId":"session:1",
                "revision":2,
                "index":1,
                "outcome":{"kind":"committed","eventType":"assistant/message","seq":9},
            })
        );
        assert_eq!(
            serde_json::to_value(SessionAssistantStreamBaseline::default()).unwrap(),
            json!({"revision":0})
        );
    }

    #[test]
    fn compaction_is_lossless_and_respects_run_boundaries() {
        let input = vec![
            timed(10, json!({"type":"text-delta","index":2,"text":"a"})),
            timed(12, json!({"type":"text-delta","index":2,"text":"b"})),
            timed(13, json!({"type":"reasoning-delta","index":0,"text":"why"})),
            timed(
                15,
                json!({"type":"tool-call-delta","index":1,"id":"c1","name":"echo","argumentsDelta":"{"}),
            ),
            timed(
                17,
                json!({"type":"tool-call-delta","index":1,"id":"c1","name":"echo","argumentsDelta":"}"}),
            ),
            timed(
                18,
                json!({"type":"tool-call-delta","index":1,"id":"c1","argumentsDelta":"x"}),
            ),
            timed(
                20,
                json!({"type":"tool-call-delta","index":1,"id":"","name":"echo","argumentsDelta":"raw"}),
            ),
            timed(
                21,
                json!({"type":"block-start","index":3,"blockType":"text"}),
            ),
            timed(
                22,
                json!({"type":"usage","usage":{"inputTokens":1,"outputTokens":2}}),
            ),
            timed(23, json!({"type":"finish","reason":{"kind":"stop"}})),
        ];
        let mut accumulator = AssistantStreamAccumulator::default();
        for value in &input {
            assert_eq!(accumulator.push(value.clone()).unwrap(), *value);
        }
        let snapshot = accumulator.snapshot();
        assert_eq!(snapshot.len(), 8);
        assert!(
            matches!(snapshot[0], AssistantStreamRecord::TextChunks { ref dt, ref texts, .. }
            if dt == &vec![2] && texts == &vec!["a".to_string(), "b".to_string()])
        );
        assert!(
            matches!(snapshot[2], AssistantStreamRecord::ToolCallChunks { ref name, .. }
            if name == &Some("echo".into()))
        );
        assert!(
            matches!(snapshot[3], AssistantStreamRecord::ToolCallChunks { ref name, .. }
            if name.is_none())
        );
        assert!(matches!(snapshot[4], AssistantStreamRecord::Chunk { .. }));
        assert_eq!(expand_assistant_stream(&snapshot).unwrap(), input);

        let mut rebuilt = AssistantStreamAccumulator::default();
        for value in expand_assistant_stream(&snapshot).unwrap() {
            rebuilt.push(value).unwrap();
        }
        assert_eq!(rebuilt.snapshot(), snapshot);
    }

    #[test]
    fn durable_expansion_rejects_malformed_records() {
        let valid = AssistantStreamRecord::TextChunks {
            time0: 10,
            index: 0,
            dt: vec![2],
            texts: vec!["a".into(), "b".into()],
        };
        assert_eq!(
            expand_assistant_stream(std::slice::from_ref(&valid)).unwrap(),
            vec![
                timed(10, json!({"type":"text-delta","index":0,"text":"a"})),
                timed(12, json!({"type":"text-delta","index":0,"text":"b"})),
            ]
        );

        let text_record = |time0, index, dt, texts| AssistantStreamRecord::TextChunks {
            time0,
            index,
            dt,
            texts,
        };
        let invalid = vec![
            text_record(10, 0, vec![2], vec![]),
            text_record(10, 0, vec![], vec!["a".into(), "b".into()]),
            text_record(
                10,
                MAX_SAFE_INTEGER + 1,
                vec![2],
                vec!["a".into(), "b".into()],
            ),
            text_record(
                MAX_SAFE_INTEGER + 1,
                0,
                vec![2],
                vec!["a".into(), "b".into()],
            ),
            text_record(1, 0, vec![-2], vec!["a".into(), "b".into()]),
            text_record(10, 0, vec![i64::MIN], vec!["a".into(), "b".into()]),
            AssistantStreamRecord::ToolCallChunks {
                time0: 1,
                index: 0,
                dt: vec![],
                id: String::new(),
                name: None,
                args: vec!["{}".into()],
            },
            AssistantStreamRecord::ToolCallChunks {
                time0: 1,
                index: 0,
                dt: vec![],
                id: "c".into(),
                name: Some(String::new()),
                args: vec!["{}".into()],
            },
            AssistantStreamRecord::Chunk {
                time: 1,
                chunk: json!(null),
            },
        ];
        for record in invalid {
            assert!(expand_assistant_stream(&[record]).is_err());
        }
        assert!(serde_json::from_value::<AssistantStreamRecord>(json!({
            "type":"text-chunks","time0":1,"index":0,"dt":[],"texts":["x"],"extra":true,
        }))
        .is_err());
    }

    #[test]
    fn invalid_live_chunks_do_not_mutate_the_accumulator() {
        let mut accumulator = AssistantStreamAccumulator::default();
        assert!(accumulator
            .push(timed(1, json!({"type":"text-delta","index":-1,"text":"x"})))
            .is_err());
        assert!(accumulator
            .push(timed(1, json!({"type":"tool-call-delta","index":0,"id":"c","name":null,"argumentsDelta":"{}"})))
            .is_err());
        assert!(accumulator
            .push(timed(
                MAX_SAFE_INTEGER + 1,
                json!({"type":"finish","reason":{"kind":"stop"}})
            ))
            .is_err());
        assert!(accumulator.snapshot().is_empty());
    }

    #[test]
    fn baseline_uses_durable_cursor_dense_indexes_and_cached_identity() {
        let mut accumulator = SessionAssistantStreamAccumulator::default();
        let empty = accumulator.snapshot();
        assert!(Arc::ptr_eq(&empty, &accumulator.snapshot()));

        accumulator.accept(start(1, "s:1"), 41).unwrap();
        let opened = accumulator.snapshot();
        assert_eq!(opened.revision, 1);
        assert_eq!(
            opened.active_attempt.as_ref().unwrap().started_after_seq,
            41
        );
        assert!(Arc::ptr_eq(&opened, &accumulator.snapshot()));

        accumulator
            .accept(chunk(2, "s:1", 0, 10, "hello"), 41)
            .unwrap();
        let streamed = accumulator.snapshot();
        assert!(!Arc::ptr_eq(&opened, &streamed));
        let active = streamed.active_attempt.as_ref().unwrap();
        assert_eq!(active.next_index, 1);
        assert_eq!(active.stream.len(), 1);

        accumulator
            .accept(chunk(3, "s:1", 2, 11, "gap"), 41)
            .unwrap();
        assert_eq!(accumulator.snapshot().revision, 3);
        assert!(accumulator.snapshot().active_attempt.is_none());

        accumulator.accept(start(4, "s:2"), 44).unwrap();
        accumulator
            .accept(chunk(5, "wrong", 0, 12, "wrong attempt"), 44)
            .unwrap();
        assert!(accumulator.snapshot().active_attempt.is_none());

        accumulator.accept(start(9, "s:3"), 48).unwrap();
        let gap = accumulator.snapshot();
        assert_eq!(gap.revision, 9);
        assert!(gap.active_attempt.is_none());

        accumulator.accept(start(1, "s:4"), 50).unwrap();
        let reset = accumulator.snapshot();
        assert_eq!(reset.revision, 1);
        assert_eq!(reset.active_attempt.as_ref().unwrap().attempt_id, "s:4");
        assert_eq!(reset.active_attempt.as_ref().unwrap().started_after_seq, 50);

        accumulator
            .accept(
                SessionAssistantStreamFrame::End {
                    attempt_id: "s:4".into(),
                    revision: 2,
                    index: 0,
                    outcome: AssistantStreamOutcome::Abandoned,
                },
                50,
            )
            .unwrap();
        assert!(accumulator.snapshot().active_attempt.is_none());
    }

    #[test]
    fn recorded_max_token_stream_is_a_compaction_fixed_point() {
        let fixture =
            include_str!("../../../../snapshots/sdk/max-tokens-continue/session.v4.jsonl");
        let event: Value = fixture
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|row| {
                row["type"] == "assistant/message"
                    && row["data"]["stream"].as_array().is_some_and(|records| {
                        records
                            .iter()
                            .any(|record| record["type"] == "tool-call-chunks")
                    })
            })
            .expect("recorded max-token assistant event");
        assert_eq!(
            event["data"]["message"]["content"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let records: Vec<AssistantStreamRecord> =
            serde_json::from_value(event["data"]["stream"].clone()).unwrap();
        assert_eq!(records.len(), 7);
        let expanded = expand_assistant_stream(&records).unwrap();
        let mut accumulator = AssistantStreamAccumulator::default();
        for value in expanded {
            accumulator.push(value).unwrap();
        }
        assert_eq!(accumulator.snapshot(), records);
    }
}
