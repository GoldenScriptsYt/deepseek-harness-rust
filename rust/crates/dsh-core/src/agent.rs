//! Agent loop: turns of steps, each one model request plus its tool calls, logged as
//! current-format Session events. Port of `core/agent-loop` and `core/session` for the
//! text, reasoning and tool path.

use crate::assistant_stream::{
    AssistantSettlementEventType, AssistantStreamAccumulator, AssistantStreamError,
    AssistantStreamOutcome, SessionAssistantStreamFrame, TimedStreamChunk,
};
use crate::bus::Bus;
use crate::compaction;
use crate::tools::{
    BoxFuture, Cancel, EventSink, SessionFacts, ToolContext, ToolOutput, ToolRegistry,
};
use dsh_llm::{
    BlockKind, Content, ErrorCode, FinishReason, LlmError, Message, Role, StreamChunk, TokenUsage,
    ToolDef,
};
use dsh_session_format::Event;
use dsh_session_persistence::{Session, StoreError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

/// A model route. The DeepSeek adapter implements this; tests use scripted fakes.
pub trait Model: Send + Sync {
    fn provider(&self) -> String;
    fn model(&self) -> String;
    /// Replace the active route when this is a switchable model wrapper.
    fn replace_route(&self, _next: Arc<dyn Model>) -> bool {
        false
    }
    /// Stream one response to `tx`. A failure is sent as the last item. Dropping `tx` ends it.
    fn stream<'a>(
        &'a self,
        history: &'a [Message],
        tools: &'a [ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) -> BoxFuture<'a, ()>;
}

/// Observable activity: legacy live chunks, Assistant stream frames, and durable events.
#[derive(Debug, Clone)]
pub enum AgentEvent {
    Chunk(StreamChunk),
    AssistantStream(SessionAssistantStreamFrame),
    Logged(Event),
}

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    AssistantStream(#[from] AssistantStreamError),
    #[error("invalid agent inbox: {0}")]
    Inbox(String),
    #[error("agent admission requires an active run")]
    AdmissionUnavailable,
    #[error("agent admission queue is full")]
    AdmissionQueueFull,
}

/// Pending list selected by an inbox operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InboxTarget {
    NextTurn,
    NextStep,
}

impl InboxTarget {
    fn as_str(self) -> &'static str {
        match self {
            Self::NextTurn => "next-turn",
            Self::NextStep => "next-step",
        }
    }
}

/// User content and source metadata before a durable message identity is assigned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInput {
    pub content: Vec<Content>,
    pub source: Value,
}

impl UserInput {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![Content::Text { text: text.into() }],
            source: json!({ "kind": "user" }),
        }
    }

    pub fn with_source(content: Vec<Content>, source: Value) -> Self {
        Self { content, source }
    }
}

/// A pending user message in the current Session JSONL representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxMessage {
    pub id: String,
    #[serde(flatten)]
    pub message: Message,
    pub source: Value,
}

impl InboxMessage {
    pub fn content(&self) -> &[Content] {
        &self.message.content
    }

    pub fn source(&self) -> &Value {
        &self.source
    }

    pub fn rpc_id(&self) -> Option<&str> {
        self.source.get("rpcId").and_then(Value::as_str)
    }

    fn into_parts(self) -> (String, Message, Value) {
        (self.id, self.message, self.source)
    }

    fn from_parts(id: String, message: Message, source: Value) -> Self {
        Self {
            id,
            message,
            source,
        }
    }

    fn validate(&self) -> Result<(), CoreError> {
        if self.id.is_empty() {
            return Err(CoreError::Inbox("message id must not be empty".into()));
        }
        if self.message.role != Role::User {
            return Err(CoreError::Inbox(format!(
                "message \"{}\" is not user input",
                self.id
            )));
        }
        if !self.source.is_object() || self.source.get("kind").and_then(Value::as_str).is_none() {
            return Err(CoreError::Inbox(format!(
                "message \"{}\" has an invalid source",
                self.id
            )));
        }
        Ok(())
    }
}

/// Prompt admission fields supplied by an RPC client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptRequest {
    pub request_id: String,
    pub content: Vec<Content>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_time_zone: Option<String>,
}

impl PromptRequest {
    pub fn new(
        request_id: impl Into<String>,
        content: Vec<Content>,
        client_time_zone: Option<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            content,
            client_time_zone,
        }
    }

    pub fn text(request_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new(request_id, vec![Content::Text { text: text.into() }], None)
    }
}

/// Placement exposed by a Session queue snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueuePlacement {
    Queued,
    Steering,
    Context,
}

/// Serializable projection matching `SessionQueuedItem`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionQueuedItem {
    pub id: String,
    pub placement: QueuePlacement,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_id: Option<String>,
    pub content: Vec<Content>,
}

/// Recovered pending input. Field names serialize as `next-turn` and `next-step`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InboxState {
    #[serde(rename = "next-turn")]
    pub next_turn: Vec<InboxMessage>,
    #[serde(rename = "next-step")]
    pub next_step: Vec<InboxMessage>,
}

impl InboxState {
    fn list(&self, target: InboxTarget) -> &Vec<InboxMessage> {
        match target {
            InboxTarget::NextTurn => &self.next_turn,
            InboxTarget::NextStep => &self.next_step,
        }
    }

    fn list_mut(&mut self, target: InboxTarget) -> &mut Vec<InboxMessage> {
        match target {
            InboxTarget::NextTurn => &mut self.next_turn,
            InboxTarget::NextStep => &mut self.next_step,
        }
    }

    fn has_pending(&self) -> bool {
        !self.next_turn.is_empty() || !self.next_step.is_empty()
    }

    fn locate(&self, id: &str) -> Option<(InboxTarget, usize)> {
        for target in [InboxTarget::NextTurn, InboxTarget::NextStep] {
            if let Some(index) = self
                .list(target)
                .iter()
                .position(|message| message.id == id)
            {
                return Some((target, index));
            }
        }
        None
    }

    fn apply(
        &mut self,
        target: InboxTarget,
        start: usize,
        removed_count: usize,
        inserted: &[InboxMessage],
    ) -> Result<Vec<InboxMessage>, CoreError> {
        let length = self.list(target).len();
        if start > length || removed_count > length - start {
            return Err(CoreError::Inbox("inbox splice is out of range".into()));
        }
        for message in inserted {
            message.validate()?;
        }
        let removed: Vec<_> = self.list(target)[start..start + removed_count].to_vec();
        let mut candidate = self.list(target).clone();
        candidate.splice(start..start + removed_count, inserted.iter().cloned());
        let mut ids = BTreeSet::new();
        for message in candidate.iter().chain(if target == InboxTarget::NextTurn {
            self.next_step.iter()
        } else {
            self.next_turn.iter()
        }) {
            if !ids.insert(message.id.clone()) {
                return Err(CoreError::Inbox(format!(
                    "message \"{}\" is already pending",
                    message.id
                )));
            }
        }
        *self.list_mut(target) = candidate;
        Ok(removed)
    }

    pub fn recover(events: &[Event]) -> Result<Self, CoreError> {
        let mut state = Self::default();
        for event in events
            .iter()
            .filter(|event| event.kind == "agent/inbox/spliced")
        {
            let target = match event.data.get("target").and_then(Value::as_str) {
                Some("next-turn") => InboxTarget::NextTurn,
                Some("next-step") => InboxTarget::NextStep,
                _ => return Err(CoreError::Inbox("splice has an invalid target".into())),
            };
            let start = event
                .data
                .get("start")
                .and_then(Value::as_u64)
                .filter(|value| *value <= MAX_SAFE_INTEGER)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| CoreError::Inbox("splice has an invalid start".into()))?;
            let removed_count = event.data.get("removedCount").map_or(Ok(0usize), |value| {
                value
                    .as_u64()
                    .filter(|value| *value <= MAX_SAFE_INTEGER)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| CoreError::Inbox("splice has an invalid removedCount".into()))
            })?;
            let inserted: Vec<InboxMessage> =
                serde_json::from_value(event.data.get("inserted").cloned().unwrap_or(Value::Null))
                    .map_err(|error| {
                        CoreError::Inbox(format!("splice has invalid inserted messages: {error}"))
                    })?;
            state.apply(target, start, removed_count, &inserted)?;
        }
        Ok(state)
    }

    pub fn queue_snapshot(&self) -> Value {
        let item = |message: &InboxMessage, placement| SessionQueuedItem {
            id: message.id.clone(),
            placement,
            rpc_id: message.rpc_id().map(str::to_owned),
            content: message.content().to_vec(),
        };
        serde_json::to_value(
            self.next_turn
                .iter()
                .map(|message| item(message, QueuePlacement::Queued))
                .chain(self.next_step.iter().map(|message| {
                    item(
                        message,
                        if message.source.get("kind").and_then(Value::as_str) == Some("user") {
                            QueuePlacement::Steering
                        } else {
                            QueuePlacement::Context
                        },
                    )
                }))
                .collect::<Vec<_>>(),
        )
        .expect("queue items serialize")
    }
}

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// How a turn ended; mirrors the `turn/end` reason kinds.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEnd {
    Completed,
    MaxTokens,
    Aborted,
    Error(LlmError),
    StepLimit,
}

pub struct AgentConfig {
    pub system_prompt: String,
    pub cwd: PathBuf,
    pub max_steps: u32,
    /// Context compaction; `None` disables checkpoint summarization.
    pub compaction: Option<compaction::CompactionConfig>,
    /// Committed Session facts handed to tools that read durable planning state.
    pub session: Option<Arc<dyn SessionFacts>>,
}

pub struct Agent {
    model: Arc<dyn Model>,
    tools: ToolRegistry,
    session: Session,
    history: Vec<Message>,
    config: AgentConfig,
    pub bus: Bus<AgentEvent>,
    turn: u64,
    msg_counter: u64,
    context_logged: bool,
    /// Turn that already compacted, so one turn spends at most one summarization call.
    compacted_turn: Option<u64>,
    assistant_attempt_counter: u64,
    assistant_stream_revision: u64,
    inbox: InboxState,
    seen_request_ids: BTreeSet<String>,
    admission_tx: mpsc::Sender<AdmissionCommand>,
    admission_rx: mpsc::Receiver<AdmissionCommand>,
    admission_running: Arc<Mutex<bool>>,
    /// Deployment guidance appended to the system prompt while it applies.
    prompt_extra: String,
}

/// Cloneable admission handle for mutating pending input while an Agent run is active.
///
/// Commands execute on the Agent task, so Session writes, message-ID allocation, request-ID
/// deduplication, and Bus publication remain serialized with the active turn. A successful
/// acknowledgement means the mutation's durable event has been appended and published.
#[derive(Clone)]
pub struct AgentAdmission {
    tx: mpsc::Sender<AdmissionCommand>,
    running: Arc<Mutex<bool>>,
}

impl AgentAdmission {
    fn send(&self, command: AdmissionCommand) -> Result<(), CoreError> {
        let running = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*running {
            return Err(CoreError::AdmissionUnavailable);
        }
        self.tx.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => CoreError::AdmissionQueueFull,
            mpsc::error::TrySendError::Closed(_) => CoreError::AdmissionUnavailable,
        })
    }

    async fn request<T: Send>(
        &self,
        command: AdmissionCommand,
        reply: oneshot::Receiver<Result<T, CoreError>>,
    ) -> Result<T, CoreError> {
        self.send(command)?;
        reply.await.map_err(|_| CoreError::AdmissionUnavailable)?
    }

    /// Append input, returning its stable ID or `None` for an accepted duplicate RPC ID.
    pub async fn enqueue(
        &self,
        input: UserInput,
        target: InboxTarget,
    ) -> Result<Option<String>, CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::Enqueue {
                input,
                target,
                reply,
            },
            response,
        )
        .await
    }

    /// Admit an RPC prompt with durable request-ID idempotency and timezone metadata.
    pub async fn submit_prompt(
        &self,
        request: PromptRequest,
        target: InboxTarget,
    ) -> Result<Option<String>, CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::SubmitPrompt {
                request,
                target,
                reply,
            },
            response,
        )
        .await
    }

    /// Replace pending content in place, preserving its ID and complete source.
    pub async fn replace(&self, id: &str, content: Vec<Content>) -> Result<bool, CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::Replace {
                id: id.to_owned(),
                content,
                reply,
            },
            response,
        )
        .await
    }

    /// Remove one pending message wherever it occurs.
    pub async fn remove(&self, id: &str) -> Result<bool, CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::Remove {
                id: id.to_owned(),
                reply,
            },
            response,
        )
        .await
    }

    /// Move one queued turn into live steering while preserving its identity and source.
    pub async fn steer(&self, id: &str) -> Result<Option<InboxMessage>, CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::Steer {
                id: id.to_owned(),
                reply,
            },
            response,
        )
        .await
    }

    /// Apply JavaScript-style splice coordinates and durably record the normalized mutation.
    pub async fn splice(
        &self,
        target: InboxTarget,
        start: i64,
        delete_count: i64,
        inserted: Vec<InboxMessage>,
    ) -> Result<Vec<InboxMessage>, CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::Splice {
                target,
                start,
                delete_count,
                inserted,
                reply,
            },
            response,
        )
        .await
    }

    /// Clear steering first and queued turns second, recording explicit cancellation.
    pub async fn clear(&self) -> Result<(), CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(AdmissionCommand::Clear { reply }, response)
            .await
    }

    /// Cancel active work; optionally retain all still-pending inbox messages.
    pub async fn cancel(&self, cancel: &Cancel, keep_inbox: bool) -> Result<(), CoreError> {
        let (reply, response) = oneshot::channel();
        self.request(
            AdmissionCommand::Cancel {
                cancel: cancel.clone(),
                keep_inbox,
                reply,
            },
            response,
        )
        .await
    }
}

type EnqueueReply = oneshot::Sender<Result<Option<String>, CoreError>>;
type MutationReply = oneshot::Sender<Result<bool, CoreError>>;
type SpliceReply = oneshot::Sender<Result<Vec<InboxMessage>, CoreError>>;
type SteerReply = oneshot::Sender<Result<Option<InboxMessage>, CoreError>>;
type UnitReply = oneshot::Sender<Result<(), CoreError>>;

enum AdmissionCommand {
    Enqueue {
        input: UserInput,
        target: InboxTarget,
        reply: EnqueueReply,
    },
    SubmitPrompt {
        request: PromptRequest,
        target: InboxTarget,
        reply: EnqueueReply,
    },
    Replace {
        id: String,
        content: Vec<Content>,
        reply: MutationReply,
    },
    Remove {
        id: String,
        reply: MutationReply,
    },
    Steer {
        id: String,
        reply: SteerReply,
    },
    Splice {
        target: InboxTarget,
        start: i64,
        delete_count: i64,
        inserted: Vec<InboxMessage>,
        reply: SpliceReply,
    },
    Clear {
        reply: UnitReply,
    },
    Cancel {
        cancel: Cancel,
        keep_inbox: bool,
        reply: UnitReply,
    },
}

impl AdmissionCommand {
    fn execute(self, agent: &mut Agent) {
        match self {
            Self::Enqueue {
                input,
                target,
                reply,
            } => {
                let _ = reply.send(agent.enqueue(input, target));
            }
            Self::SubmitPrompt {
                request,
                target,
                reply,
            } => {
                let _ = reply.send(agent.submit_prompt(request, target));
            }
            Self::Replace { id, content, reply } => {
                let _ = reply.send(agent.replace(&id, content));
            }
            Self::Remove { id, reply } => {
                let _ = reply.send(agent.remove(&id));
            }
            Self::Steer { id, reply } => {
                let _ = reply.send(agent.steer(&id));
            }
            Self::Splice {
                target,
                start,
                delete_count,
                inserted,
                reply,
            } => {
                let _ = reply.send(agent.splice(target, start, delete_count, inserted));
            }
            Self::Clear { reply } => {
                let _ = reply.send(agent.clear());
            }
            Self::Cancel {
                cancel,
                keep_inbox,
                reply,
            } => {
                let _ = reply.send(agent.cancel(&cancel, keep_inbox));
            }
        }
    }
}

struct AdmissionRun<'a> {
    agent: &'a mut Agent,
}

impl<'a> AdmissionRun<'a> {
    fn new(agent: &'a mut Agent) -> Result<Self, CoreError> {
        agent.begin_admissions()?;
        Ok(Self { agent })
    }
}

impl Drop for AdmissionRun<'_> {
    fn drop(&mut self) {
        self.agent.stop_admissions();
    }
}

/// Rebuild model history from a Session's durable events.
///
/// A `compaction/summary` shadows the message events it names; the next
/// message event after it (the checkpoint replacement) takes their place.
pub fn derive_history(events: &[Event]) -> Vec<Message> {
    history_with_seqs(events)
        .into_iter()
        .map(|(_, message)| message)
        .collect()
}

/// Rebuild model history together with the seq that produced each message.
pub fn history_with_seqs(events: &[Event]) -> Vec<(u64, Message)> {
    let mut out: Vec<(u64, Message)> = Vec::new();
    let mut shadowed: Option<Vec<u64>> = None;
    for (index, e) in events.iter().enumerate() {
        if e.kind == "compaction/summary" {
            shadowed = Some(
                e.data
                    .get("shadowedSeqs")
                    .and_then(Value::as_array)
                    .map(|seqs| seqs.iter().filter_map(Value::as_u64).collect())
                    .unwrap_or_default(),
            );
            continue;
        }
        let msg = match e.kind.as_str() {
            "system/message" | "assistant/message" | "tool/result" => e.data.get("message"),
            "user/message" => Some(&e.data),
            _ => None,
        };
        if let Some(m) = msg.and_then(|v| serde_json::from_value::<Message>(v.clone()).ok()) {
            let seq = e.seq().unwrap_or(index as u64);
            // A `surfaceOp` replace names the nodes it rewrites, whatever replaced them.
            let replaced = shadowed.take().or_else(|| {
                let operation = e.extra.get("surfaceOp")?;
                if operation.get("op").and_then(Value::as_str) != Some("replace") {
                    return None;
                }
                Some(
                    e.extra
                        .get("sourceEventSeqs")
                        .and_then(Value::as_array)
                        .map(|seqs| seqs.iter().filter_map(Value::as_u64).collect())
                        .unwrap_or_default(),
                )
            });
            match replaced {
                // A replacement occupies the surface position of the span it shadows.
                Some(shadowed) => {
                    let position = out
                        .iter()
                        .position(|(seq, _)| shadowed.contains(seq))
                        .unwrap_or(out.len());
                    out.retain(|(seq, _)| !shadowed.contains(seq));
                    out.insert(position.min(out.len()), (seq, m));
                }
                None => out.push((seq, m)),
            }
        }
    }
    out
}

#[derive(Default)]
struct Assembler {
    parts: BTreeMap<usize, Part>,
    order: Vec<usize>,
}

struct Part {
    kind: BlockKind,
    text: String,
    id: String,
    name: String,
    closed: bool,
}

impl Assembler {
    fn push(&mut self, c: &StreamChunk) {
        match c {
            StreamChunk::BlockStart { index, kind } => {
                self.ensure(*index, *kind);
            }
            StreamChunk::TextDelta { index, text }
            | StreamChunk::ReasoningDelta { index, text } => {
                let kind = if matches!(c, StreamChunk::TextDelta { .. }) {
                    BlockKind::Text
                } else {
                    BlockKind::Reasoning
                };
                let part = self.ensure(*index, kind);
                if !part.closed {
                    part.text.push_str(text);
                }
            }
            StreamChunk::ToolCallDelta {
                index,
                id,
                name,
                arguments_delta,
            } => {
                let part = self.ensure(*index, BlockKind::ToolCall);
                if !part.closed {
                    if let Some(id) = id {
                        part.id = id.clone();
                    }
                    if let Some(name) = name.as_ref().filter(|name| !name.is_empty()) {
                        part.name = name.clone();
                    }
                    part.text.push_str(arguments_delta);
                }
            }
            StreamChunk::BlockEnd { index } => {
                if let Some(part) = self.parts.get_mut(index) {
                    part.closed = true;
                }
            }
            StreamChunk::Usage(_) | StreamChunk::Finish { .. } => {}
        }
    }

    fn ensure(&mut self, index: usize, kind: BlockKind) -> &mut Part {
        if !self.parts.contains_key(&index) {
            self.order.push(index);
            self.parts.insert(
                index,
                Part {
                    kind,
                    text: String::new(),
                    id: String::new(),
                    name: String::new(),
                    closed: false,
                },
            );
        }
        self.parts.get_mut(&index).expect("part was inserted")
    }

    fn block_value(&self, index: usize) -> Option<Value> {
        let part = self.parts.get(&index)?;
        Some(match part.kind {
            BlockKind::Text => json!({ "type": "text", "text": part.text }),
            BlockKind::Reasoning => json!({ "type": "reasoning", "text": part.text }),
            BlockKind::ToolCall => json!({
                "type": "tool-call",
                "id": if part.id.is_empty() { format!("call-{index}") } else { part.id.clone() },
                "name": part.name,
                "arguments": part.text,
            }),
        })
    }

    /// `replay` pairs with blocks in first-seen order; interruption drops empty blocks and tools.
    fn finish(
        &self,
        replay: &[Option<String>],
        interrupted: bool,
        reason: FinishReason,
    ) -> Vec<Content> {
        let mut out = Vec::new();
        for (position, index) in self.order.iter().enumerate() {
            let Some(part) = self.parts.get(index) else {
                continue;
            };
            if interrupted && part.text.trim().is_empty() {
                continue;
            }
            match part.kind {
                BlockKind::Text => out.push(Content::Text {
                    text: part.text.clone(),
                }),
                BlockKind::Reasoning => out.push(Content::Reasoning {
                    text: part.text.clone(),
                    signature: replay.get(position).cloned().flatten(),
                }),
                BlockKind::ToolCall if !interrupted && reason != FinishReason::MaxTokens => {
                    out.push(Content::ToolCall {
                        id: if part.id.is_empty() {
                            format!("call-{index}")
                        } else {
                            part.id.clone()
                        },
                        name: part.name.clone(),
                        arguments: part.text.clone(),
                    });
                }
                BlockKind::ToolCall => {}
            }
        }
        out
    }
}

struct LiveAttempt {
    id: String,
    index: u64,
    stream: AssistantStreamAccumulator,
}

impl LiveAttempt {
    fn new(id: String) -> Self {
        Self {
            id,
            index: 0,
            stream: AssistantStreamAccumulator::default(),
        }
    }

    fn start(
        &self,
        revision: u64,
        started_after_seq: i64,
        turn: u64,
        step: u64,
    ) -> SessionAssistantStreamFrame {
        SessionAssistantStreamFrame::Start {
            attempt_id: self.id.clone(),
            revision,
            started_after_seq,
            turn,
            step,
        }
    }

    fn push(&mut self, value: TimedStreamChunk) -> Result<TimedStreamChunk, AssistantStreamError> {
        let timed = self.stream.push(value)?;
        self.index += 1;
        Ok(timed)
    }

    fn chunk_frame(&self, timed: TimedStreamChunk, revision: u64) -> SessionAssistantStreamFrame {
        SessionAssistantStreamFrame::Chunk {
            attempt_id: self.id.clone(),
            revision,
            index: self.index - 1,
            time: timed.time,
            chunk: timed.chunk,
        }
    }

    fn end(&self, revision: u64, outcome: AssistantStreamOutcome) -> SessionAssistantStreamFrame {
        SessionAssistantStreamFrame::End {
            attempt_id: self.id.clone(),
            revision,
            index: self.index,
            outcome,
        }
    }

    fn stream_value(&self) -> Value {
        serde_json::to_value(self.stream.snapshot()).expect("assistant stream records serialize")
    }
}

fn block_kind_value(kind: BlockKind) -> &'static str {
    match kind {
        BlockKind::Text => "text",
        BlockKind::Reasoning => "reasoning",
        BlockKind::ToolCall => "tool-call",
    }
}

fn finish_reason_value(reason: FinishReason) -> Value {
    json!({ "kind": match reason {
        FinishReason::Stop => "stop",
        FinishReason::ToolCalls => "tool-calls",
        FinishReason::MaxTokens => "max-tokens",
    } })
}

fn replay_state_value(replay: &[Option<String>]) -> Option<Value> {
    (!replay.iter().all(Option::is_none)).then(|| {
        json!({
            "response": null,
            "blocks": replay,
        })
    })
}

/// Adapt the Rust provider chunk vocabulary to the current TypeScript wire vocabulary.
/// Rust has no authoritative block-end payload or response-level replay metadata, so
/// block ends use the accumulated block and replay responses are represented as null.
fn stream_chunk_value(chunk: &StreamChunk, assembler: &Assembler) -> Value {
    match chunk {
        StreamChunk::BlockStart { index, kind } => json!({
            "type": "block-start",
            "index": index,
            "blockType": block_kind_value(*kind),
        }),
        StreamChunk::TextDelta { index, text } => json!({
            "type": "text-delta",
            "index": index,
            "text": text,
        }),
        StreamChunk::ReasoningDelta { index, text } => json!({
            "type": "reasoning-delta",
            "index": index,
            "text": text,
        }),
        StreamChunk::ToolCallDelta {
            index,
            id,
            name,
            arguments_delta,
        } => {
            let mut value = json!({
                "type": "tool-call-delta",
                "index": index,
                "id": id.clone().unwrap_or_default(),
                "argumentsDelta": arguments_delta,
            });
            if let Some(name) = name {
                value["name"] = json!(name);
            }
            value
        }
        StreamChunk::BlockEnd { index } => json!({
            "type": "block-end",
            "index": index,
            "block": assembler.block_value(*index),
        }),
        StreamChunk::Usage(usage) => json!({
            "type": "usage",
            "usage": usage,
        }),
        StreamChunk::Finish { reason, replay } => {
            let mut value = json!({
                "type": "finish",
                "reason": finish_reason_value(*reason),
            });
            if let Some(state) = replay_state_value(replay) {
                value["replayState"] = state;
            }
            value
        }
    }
}

fn error_code(code: ErrorCode) -> Value {
    serde_json::to_value(code).unwrap_or(Value::Null)
}

fn recover_request_ids(events: &[Event]) -> BTreeSet<String> {
    let inherited_event_count = events
        .iter()
        .find(|event| event.kind == "session/end-seed")
        .and_then(|event| event.seq())
        .map_or(0, |seq| seq + 1);
    let mut requests = BTreeSet::new();
    for event in events {
        if event.seq().is_some_and(|seq| seq < inherited_event_count) {
            continue;
        }
        let messages: Vec<&Value> = match event.kind.as_str() {
            "agent/inbox/spliced" => event
                .data
                .get("inserted")
                .and_then(Value::as_array)
                .map_or_else(Vec::new, |messages| messages.iter().collect()),
            "user/message" => vec![&event.data],
            _ => Vec::new(),
        };
        for message in messages {
            let Some(source) = message.get("source") else {
                continue;
            };
            if !matches!(
                source.get("kind").and_then(Value::as_str),
                Some("user" | "retry")
            ) {
                continue;
            }
            if let Some(request_id) = source.get("rpcId").and_then(Value::as_str) {
                requests.insert(request_id.to_owned());
            }
        }
    }
    requests
}

fn recover_message_counter(events: &[Event]) -> u64 {
    events
        .iter()
        .flat_map(|event| {
            let direct = event.data.get("id").and_then(Value::as_str);
            let nested = event.data.pointer("/message/id").and_then(Value::as_str);
            let inserted = event
                .data
                .get("inserted")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|message| message.get("id").and_then(Value::as_str));
            direct
                .into_iter()
                .chain(nested)
                .chain(inserted)
                .filter_map(|id| id.strip_prefix("msg-"))
                .filter_map(|id| id.parse::<u64>().ok())
        })
        .max()
        .unwrap_or(0)
}

impl Agent {
    pub fn new(
        model: Arc<dyn Model>,
        tools: ToolRegistry,
        session: Session,
        config: AgentConfig,
    ) -> Self {
        Self::try_new(model, tools, session, config)
            .expect("agent session contains a valid durable inbox")
    }

    /// Construct an agent, rejecting malformed durable inbox events.
    pub fn try_new(
        model: Arc<dyn Model>,
        tools: ToolRegistry,
        session: Session,
        config: AgentConfig,
    ) -> Result<Self, CoreError> {
        let history = derive_history(&session.events);
        let inbox = InboxState::recover(&session.events)?;
        let seen_request_ids = recover_request_ids(&session.events);
        let turn = session
            .events
            .iter()
            .filter(|e| e.kind == "turn/start")
            .filter_map(|e| e.data.get("turn").and_then(Value::as_u64))
            .max()
            .unwrap_or(0);
        let msg_counter = recover_message_counter(&session.events);
        let context_logged = session.events.iter().any(|e| e.kind == "request/context");
        let (admission_tx, admission_rx) = mpsc::channel(64);
        Ok(Agent {
            model,
            tools,
            session,
            history,
            config,
            bus: Bus::default(),
            turn,
            msg_counter,
            context_logged,
            compacted_turn: None,
            assistant_attempt_counter: 0,
            assistant_stream_revision: 0,
            inbox,
            seen_request_ids,
            admission_tx,
            admission_rx,
            admission_running: Arc::new(Mutex::new(false)),
            prompt_extra: String::new(),
        })
    }

    /// Clone this handle before starting a run, then use it to admit or mutate pending input.
    pub fn admission(&self) -> AgentAdmission {
        AgentAdmission {
            tx: self.admission_tx.clone(),
            running: Arc::clone(&self.admission_running),
        }
    }

    /// Replace the conditional system-prompt guidance appended to the base prompt.
    ///
    /// The next turn logs a `system/message` that replaces the previous prompt
    /// whenever the effective text changes, so guidance stays reconstructable
    /// from the log.
    /// @param text - guidance that currently applies, or an empty string for none.
    pub fn set_prompt_extra(&mut self, text: impl Into<String>) {
        self.prompt_extra = text.into();
    }

    /// The complete system prompt for the next request.
    fn effective_system_prompt(&self) -> String {
        let extra = self.prompt_extra.trim();
        if extra.is_empty() {
            return self.config.system_prompt.clone();
        }
        if self.config.system_prompt.trim().is_empty() {
            return extra.to_owned();
        }
        format!("{}\n\n{extra}", self.config.system_prompt)
    }

    fn begin_admissions(&self) -> Result<(), CoreError> {
        let mut running = self
            .admission_running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *running {
            return Err(CoreError::Inbox("an Agent run is already active".into()));
        }
        *running = true;
        Ok(())
    }

    fn close_admissions_if_idle(&mut self) -> bool {
        let mut running = self
            .admission_running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.admission_rx.is_empty() {
            return false;
        }
        *running = false;
        true
    }

    fn stop_admissions(&mut self) {
        {
            let mut running = self
                .admission_running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *running = false;
        }
        self.drain_admissions();
    }

    fn drain_admissions(&mut self) {
        while let Ok(command) = self.admission_rx.try_recv() {
            command.execute(self);
        }
    }

    pub fn history(&self) -> &[Message] {
        &self.history
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Change the route used by this resident Agent's next model request.
    pub fn switch_model_route(&self, next: Arc<dyn Model>) -> bool {
        self.model.replace_route(next)
    }

    /// Persist and publish a model selection through this Agent's owned Session lease.
    pub fn record_model_selection(
        &mut self,
        provider: &str,
        model: &str,
        reasoning_effort: Option<&str>,
    ) -> Result<(Event, usize), CoreError> {
        let mut data = json!({"provider": provider, "model": model});
        if let Some(effort) = reasoning_effort {
            data["reasoningEffort"] = json!(effort);
        }
        self.record_session_event("model/selection", data)
    }

    /// Append and publish a Host-owned Session event through this Agent's write lease.
    pub fn record_session_event(
        &mut self,
        kind: &str,
        data: Value,
    ) -> Result<(Event, usize), CoreError> {
        let seq = self.session.events.len();
        let event = Event {
            kind: kind.into(),
            data,
            extra: Map::from_iter([("seq".into(), json!(seq)), ("time".into(), json!(now_ms()))]),
        };
        self.session.append(std::slice::from_ref(&event))?;
        self.bus.emit(&AgentEvent::Logged(event.clone()));
        Ok((event, seq))
    }

    /// A serializable copy of both pending lists.
    pub fn inbox_state(&self) -> InboxState {
        self.inbox.clone()
    }

    /// Current Session queue projection, in queued-then-steering order.
    pub fn queue_items(&self) -> Vec<SessionQueuedItem> {
        let item = |message: &InboxMessage, placement| SessionQueuedItem {
            id: message.id.clone(),
            placement,
            rpc_id: message.rpc_id().map(str::to_owned),
            content: message.content().to_vec(),
        };
        self.inbox
            .next_turn
            .iter()
            .map(|message| item(message, QueuePlacement::Queued))
            .chain(self.inbox.next_step.iter().map(|message| {
                item(
                    message,
                    if message.source.get("kind").and_then(Value::as_str) == Some("user") {
                        QueuePlacement::Steering
                    } else {
                        QueuePlacement::Context
                    },
                )
            }))
            .collect()
    }

    /// Serialize the current queue exactly as a Session `queued` snapshot.
    pub fn queue_snapshot(&self) -> Value {
        serde_json::to_value(self.queue_items()).expect("queue items serialize")
    }

    /// Append input, returning its stable ID or `None` for an accepted duplicate RPC ID.
    pub fn enqueue(
        &mut self,
        input: UserInput,
        target: InboxTarget,
    ) -> Result<Option<String>, CoreError> {
        let request_id = input
            .source
            .get("rpcId")
            .and_then(Value::as_str)
            .filter(|_| {
                matches!(
                    input.source.get("kind").and_then(Value::as_str),
                    Some("user" | "retry")
                )
            })
            .map(str::to_owned);
        if request_id
            .as_ref()
            .is_some_and(|request_id| self.seen_request_ids.contains(request_id))
        {
            return Ok(None);
        }
        let id = self.next_id();
        let message = InboxMessage::from_parts(
            id.clone(),
            Message {
                role: Role::User,
                content: input.content,
                tool_call_id: None,
                is_error: None,
            },
            input.source,
        );
        let length = self.inbox.list(target).len() as i64;
        self.mutate_inbox(target, length, 0, vec![message], false)?;
        if let Some(request_id) = request_id {
            self.seen_request_ids.insert(request_id);
        }
        Ok(Some(id))
    }

    /// Admit an RPC prompt with durable request-ID idempotency and timezone metadata.
    pub fn submit_prompt(
        &mut self,
        request: PromptRequest,
        target: InboxTarget,
    ) -> Result<Option<String>, CoreError> {
        if request.request_id.is_empty() {
            return Err(CoreError::Inbox("request id must not be empty".into()));
        }
        let mut source = json!({
            "kind": if request.content.is_empty() { "retry" } else { "user" },
            "rpcId": request.request_id,
        });
        if let Some(time_zone) = request.client_time_zone {
            source["clientTimeZone"] = json!(time_zone);
        }
        self.enqueue(UserInput::with_source(request.content, source), target)
    }

    /// Replace pending content in place, preserving its ID and complete source.
    pub fn replace(&mut self, id: &str, content: Vec<Content>) -> Result<bool, CoreError> {
        let Some((target, index)) = self.inbox.locate(id) else {
            return Ok(false);
        };
        let mut replacement = self.inbox.list(target)[index].clone();
        replacement.message.content = content;
        self.mutate_inbox(target, index as i64, 1, vec![replacement], true)?;
        Ok(true)
    }

    /// Remove one pending message wherever it occurs.
    pub fn remove(&mut self, id: &str) -> Result<bool, CoreError> {
        let Some((target, index)) = self.inbox.locate(id) else {
            return Ok(false);
        };
        self.mutate_inbox(target, index as i64, 1, Vec::new(), true)?;
        Ok(true)
    }

    /// Move one queued turn into live steering while preserving its identity and source.
    pub fn steer(&mut self, id: &str) -> Result<Option<InboxMessage>, CoreError> {
        let Some((InboxTarget::NextTurn, index)) = self.inbox.locate(id) else {
            return Ok(None);
        };
        let message = self.inbox.next_turn[index].clone();
        self.mutate_inbox(InboxTarget::NextTurn, index as i64, 1, Vec::new(), true)?;
        let length = self.inbox.next_step.len() as i64;
        self.mutate_inbox(
            InboxTarget::NextStep,
            length,
            0,
            vec![message.clone()],
            false,
        )?;
        Ok(Some(message))
    }

    /// Apply JavaScript-style splice coordinates and durably record the normalized mutation.
    pub fn splice(
        &mut self,
        target: InboxTarget,
        start: i64,
        delete_count: i64,
        inserted: Vec<InboxMessage>,
    ) -> Result<Vec<InboxMessage>, CoreError> {
        self.mutate_inbox(target, start, delete_count, inserted, true)
    }

    /// Clear steering first and queued turns second, recording explicit cancellation.
    pub fn clear(&mut self) -> Result<(), CoreError> {
        for target in [InboxTarget::NextStep, InboxTarget::NextTurn] {
            let length = self.inbox.list(target).len() as i64;
            if length > 0 {
                self.mutate_inbox(target, 0, length, Vec::new(), true)?;
            }
        }
        Ok(())
    }

    /// Cancel active work; optionally retain all still-pending inbox messages.
    pub fn cancel(&mut self, cancel: &Cancel, keep_inbox: bool) -> Result<(), CoreError> {
        let result = if keep_inbox { Ok(()) } else { self.clear() };
        cancel.cancel();
        result
    }

    fn mutate_inbox(
        &mut self,
        target: InboxTarget,
        start: i64,
        delete_count: i64,
        inserted: Vec<InboxMessage>,
        discard_removed: bool,
    ) -> Result<Vec<InboxMessage>, CoreError> {
        let length = self.inbox.list(target).len() as i64;
        let actual_start = if start < 0 {
            length.saturating_add(start).max(0)
        } else {
            start.min(length)
        };
        let actual_delete_count = delete_count.max(0).min(length.saturating_sub(actual_start));
        if actual_delete_count == 0 && inserted.is_empty() {
            return Ok(Vec::new());
        }
        let mut next = self.inbox.clone();
        let removed = next.apply(
            target,
            actual_start as usize,
            actual_delete_count as usize,
            &inserted,
        )?;
        let mut data = json!({
            "target": target.as_str(),
            "start": actual_start,
            "inserted": inserted,
        });
        if actual_delete_count > 0 {
            data["removedCount"] = json!(actual_delete_count);
        }
        if discard_removed && actual_delete_count > 0 {
            data["outcome"] = json!("canceled");
        }
        let mut extra = Map::new();
        extra.insert("seq".into(), json!(self.session.events.len()));
        extra.insert("time".into(), json!(now_ms()));
        let event = Event {
            kind: "agent/inbox/spliced".into(),
            data,
            extra,
        };
        self.session.append(std::slice::from_ref(&event))?;
        self.inbox = next;
        self.bus.emit(&AgentEvent::Logged(event));
        Ok(removed)
    }

    fn claim(&mut self, target: InboxTarget) -> Result<Vec<InboxMessage>, CoreError> {
        let mut claimed = Vec::new();
        let steering_count = self.inbox.next_step.len() as i64;
        if steering_count > 0 {
            claimed.extend(self.mutate_inbox(
                InboxTarget::NextStep,
                0,
                steering_count,
                Vec::new(),
                false,
            )?);
        }
        if target == InboxTarget::NextTurn && !self.inbox.next_turn.is_empty() {
            claimed.extend(self.mutate_inbox(InboxTarget::NextTurn, 0, 1, Vec::new(), false)?);
        }
        Ok(claimed)
    }

    /// Run at most one inbox turn. Steering is claimed before the single queued turn.
    pub async fn run_next_turn(&mut self, cancel: &Cancel) -> Result<Option<TurnEnd>, CoreError> {
        let run = AdmissionRun::new(self)?;
        let result = run.agent.run_next_turn_inner(cancel).await;
        drop(run);
        result
    }

    async fn run_next_turn_inner(&mut self, cancel: &Cancel) -> Result<Option<TurnEnd>, CoreError> {
        loop {
            self.drain_admissions();
            if cancel.is_cancelled() {
                return Ok(None);
            }
            if !self.inbox.has_pending() {
                if self.close_admissions_if_idle() {
                    return Ok(None);
                }
                continue;
            }
            let mut ends = self.run_one_turn(None, cancel).await?;
            return Ok(ends.pop());
        }
    }

    /// Drain pending turns in FIFO order until the inbox is empty or cancellation stops work.
    pub async fn run_pending(&mut self, cancel: &Cancel) -> Result<Vec<TurnEnd>, CoreError> {
        let run = AdmissionRun::new(self)?;
        let result = run.agent.run_pending_inner(cancel).await;
        drop(run);
        result
    }

    async fn run_pending_inner(&mut self, cancel: &Cancel) -> Result<Vec<TurnEnd>, CoreError> {
        let mut ends = Vec::new();
        loop {
            self.drain_admissions();
            if cancel.is_cancelled() {
                break;
            }
            if !self.inbox.has_pending() {
                if self.close_admissions_if_idle() {
                    break;
                }
                continue;
            }
            let turn_ends = self.run_one_turn(None, cancel).await?;
            let stop = turn_ends.iter().any(|end| {
                matches!(
                    end,
                    TurnEnd::Aborted | TurnEnd::Error(_) | TurnEnd::StepLimit
                )
            });
            ends.extend(turn_ends);
            if stop {
                break;
            }
        }
        Ok(ends)
    }

    fn next_id(&mut self) -> String {
        self.msg_counter += 1;
        format!("msg-{}", self.msg_counter)
    }

    fn next_assistant_revision(&mut self) -> u64 {
        self.assistant_stream_revision += 1;
        self.assistant_stream_revision
    }

    fn durable_cursor(&self) -> i64 {
        self.session
            .events
            .len()
            .checked_sub(1)
            .map_or(-1, |seq| seq as i64)
    }

    fn settle_assistant_stream(
        &mut self,
        live: &LiveAttempt,
        event_type: AssistantSettlementEventType,
        data: Value,
        extra: &[(&str, Value)],
    ) -> Result<usize, CoreError> {
        let result = self.log(
            match event_type {
                AssistantSettlementEventType::Message => "assistant/message",
                AssistantSettlementEventType::Attempt => "assistant/attempt",
            },
            data,
            extra,
        );
        let outcome = match &result {
            Ok(seq) => AssistantStreamOutcome::Committed {
                event_type,
                seq: *seq as u64,
            },
            Err(_) => AssistantStreamOutcome::Abandoned,
        };
        let revision = self.next_assistant_revision();
        self.bus
            .emit(&AgentEvent::AssistantStream(live.end(revision, outcome)));
        result
    }

    /// Summarize and shadow earlier history when it exceeds the configured budget.
    ///
    /// `compaction/start` opens the transaction, `compaction/summary` records the
    /// checkpoint and its shadowed span, the following `user/message` replaces that
    /// span, and `compaction/end` closes it. A failed summarization is recorded and
    /// leaves the history untouched, so the turn proceeds with the original context.
    async fn compact_if_needed(&mut self, turn: u64, cancel: &Cancel) -> Result<(), CoreError> {
        let Some(config) = self.config.compaction else {
            return Ok(());
        };
        if self.compacted_turn == Some(turn) {
            return Ok(());
        }
        if compaction::estimate_tokens(&self.history) <= config.threshold_tokens {
            return Ok(());
        }
        let split = compaction::split_index(&self.history, config.keep_recent_messages);
        if split == 0 {
            return Ok(());
        }
        let seqs = history_with_seqs(&self.session.events);
        let shadowed: Vec<u64> = seqs.iter().take(split).map(|(seq, _)| *seq).collect();
        let (Some(start), Some(end)) = (shadowed.first().copied(), shadowed.last().copied()) else {
            return Ok(());
        };
        let compaction_id = format!(
            "{}:compaction:{}",
            self.session.header.id,
            self.session
                .events
                .iter()
                .filter(|e| e.kind == "compaction/start")
                .count()
                + 1
        );
        let shadowed_messages: Vec<Message> = self.history[..split].to_vec();
        let shadowed_token_count = compaction::estimate_tokens(&shadowed_messages);
        let request = compaction::summarization_request(&shadowed_messages);
        self.log(
            "compaction/start",
            json!({"compactionId": compaction_id, "turn": turn}),
            &[],
        )?;
        let summary = match self.collect_summary(request, cancel).await {
            Ok(text) if !text.trim().is_empty() => text,
            Ok(_) => {
                self.log(
                    "compaction/end",
                    json!({
                        "compactionId": compaction_id,
                        "turn": turn,
                        "error": "summarization returned no text",
                    }),
                    &[],
                )?;
                return Ok(());
            }
            Err(error) => {
                self.log(
                    "compaction/end",
                    json!({
                        "compactionId": compaction_id,
                        "turn": turn,
                        "error": error.to_string(),
                    }),
                    &[],
                )?;
                return Ok(());
            }
        };
        let summary_content = vec![Content::Text {
            text: summary.clone(),
        }];
        let summary_seq = self.log(
            "compaction/summary",
            json!({
                "compactionId": compaction_id,
                "summary": summary_content,
                "rawOutput": summary_content,
                "llmStreamCall": true,
                "shadowedRange": {"start": start, "end": end},
                "shadowedSeqs": shadowed,
                "shadowedTokenCount": shadowed_token_count,
                "provider": self.model.provider(),
                "model": self.model.model(),
            }),
            &[],
        )?;
        let checkpoint = Message {
            role: Role::User,
            content: vec![Content::Text {
                text: compaction::frame_summary(&summary),
            }],
            tool_call_id: None,
            is_error: None,
        };
        let data = self.message_value(
            &checkpoint,
            json!({"kind": "compaction", "compactionId": compaction_id}),
        );
        let mut source_seqs = vec![json!(start), json!(summary_seq)];
        source_seqs.extend(shadowed.iter().map(|seq| json!(seq)));
        self.log(
            "user/message",
            data,
            &[
                ("sourceEventSeqs", Value::Array(source_seqs)),
                (
                    "surfaceOp",
                    json!({"op": "replace", "startSeq": start, "endSeq": end}),
                ),
            ],
        )?;
        self.log(
            "compaction/end",
            json!({"compactionId": compaction_id, "turn": turn}),
            &[],
        )?;
        self.history = derive_history(&self.session.events);
        self.compacted_turn = Some(turn);
        Ok(())
    }

    /// Run one summarization request and collect its text output.
    async fn collect_summary(
        &mut self,
        request: Vec<Message>,
        cancel: &Cancel,
    ) -> Result<String, LlmError> {
        let (tx, mut rx) = mpsc::channel(64);
        let model = self.model.clone();
        let task = tokio::spawn(async move { model.stream(&request, &[], tx).await });
        let mut text = String::new();
        let mut failure: Option<LlmError> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                item = rx.recv() => match item {
                    None => break,
                    Some(Ok(StreamChunk::TextDelta { text: delta, .. })) => text.push_str(&delta),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        failure = Some(error);
                        break;
                    }
                },
            }
        }
        task.abort();
        match failure {
            Some(error) => Err(error),
            None => Ok(text),
        }
    }
    /// Append one durable event; returns its sequence number.
    fn log(
        &mut self,
        kind: &str,
        data: Value,
        extra: &[(&str, Value)],
    ) -> Result<usize, CoreError> {
        let seq = self.session.events.len();
        let mut map = Map::new();
        for (k, v) in extra {
            map.insert((*k).into(), v.clone());
        }
        // Every log row carries its position and commit time; readers reject rows without them.
        map.insert("seq".into(), json!(seq));
        map.insert("time".into(), json!(now_ms()));
        let event = Event {
            kind: kind.into(),
            data,
            extra: map,
        };
        self.session.append(std::slice::from_ref(&event))?;
        self.bus.emit(&AgentEvent::Logged(event));
        Ok(seq)
    }

    /// Log the effective system prompt when it changed since the last logged one.
    ///
    /// The first message is appended; later changes rewrite that node through
    /// `surfaceOp`, which keeps one system prompt in the derived history.
    fn refresh_system_message(&mut self, turn: u64, step: u32) -> Result<(), CoreError> {
        let text = self.effective_system_prompt();
        if text.is_empty() {
            return Ok(());
        }
        let has_system = self
            .history
            .first()
            .is_some_and(|message| message.role == Role::System);
        let same = has_system
            && self.history.first().is_some_and(|message| {
                matches!(message.content.first(), Some(Content::Text { text: current }) if *current == text)
            });
        if same {
            return Ok(());
        }
        let previous_seq = history_with_seqs(&self.session.events)
            .into_iter()
            .find(|(_, message)| message.role == Role::System)
            .map(|(seq, _)| seq);
        let message = Message {
            role: Role::System,
            content: vec![Content::Text { text }],
            tool_call_id: None,
            is_error: None,
        };
        let data = json!({"turn": turn, "step": step, "message": self.message_value(&message, json!({"kind":"system-prompt"}))});
        let extra = match previous_seq {
            Some(seq) => vec![
                ("sourceEventSeqs", json!([seq])),
                (
                    "surfaceOp",
                    json!({"op": "replace", "startSeq": seq, "endSeq": seq}),
                ),
            ],
            None => vec![("surfaceOp", json!("append"))],
        };
        self.log("system/message", data, &extra)?;
        if has_system {
            self.history[0] = message;
        } else {
            self.history.insert(0, message);
        }
        Ok(())
    }

    fn message_value(&mut self, msg: &Message, source: Value) -> Value {
        let mut v = serde_json::to_value(msg).expect("message serializes");
        v["source"] = source;
        v["id"] = json!(self.next_id());
        v
    }

    fn model_source(&self) -> Value {
        json!({"kind":"model","provider":self.model.provider(),"model":self.model.model()})
    }

    fn model_source_with_replay(&self, replay: &[Option<String>]) -> Value {
        let mut source = self.model_source();
        if let Some(state) = replay_state_value(replay) {
            source["replayState"] = state;
        }
        source
    }

    fn admit_user_message(
        &mut self,
        message: InboxMessage,
        turn: u64,
        step: u32,
    ) -> Result<(), CoreError> {
        let (id, message, source) = message.into_parts();
        let mut data = serde_json::to_value(&message).expect("message serializes");
        data["source"] = source;
        data["id"] = json!(id);
        data["turn"] = json!(turn);
        data["step"] = json!(step);
        self.log("user/message", data, &[("surfaceOp", json!("append"))])?;
        self.history.push(message);
        Ok(())
    }

    /// Run one turn for `user_text` to completion, preserving the legacy event sequence.
    pub async fn run_turn(
        &mut self,
        user_text: &str,
        cancel: &Cancel,
    ) -> Result<TurnEnd, CoreError> {
        let run = AdmissionRun::new(self)?;
        run.agent.drain_admissions();
        let input = InboxMessage::from_parts(
            run.agent.next_id(),
            Message {
                role: Role::User,
                content: vec![Content::Text {
                    text: user_text.into(),
                }],
                tool_call_id: None,
                is_error: None,
            },
            json!({ "kind": "user" }),
        );
        let result = run.agent.run_one_turn(Some(input), cancel).await;
        drop(run);
        result.map(|mut ends| ends.pop().expect("one turn was requested"))
    }

    async fn run_one_turn(
        &mut self,
        initial: Option<InboxMessage>,
        cancel: &Cancel,
    ) -> Result<Vec<TurnEnd>, CoreError> {
        self.drain_admissions();
        self.turn += 1;
        let turn = self.turn;
        self.log("turn/start", json!({"turn": turn}), &[])?;
        let defs = self.tools.defs();
        let cx = ToolContext {
            cwd: self.config.cwd.clone(),
            cancel: cancel.clone(),
            events: EventSink::default(),
            session: self.config.session.clone(),
            call_id: String::new(),
        };
        let append = [("surfaceOp", json!("append"))];

        let mut initial = initial;
        let mut sticky_max_tokens = false;
        let mut step = 0u32;
        let end = loop {
            self.drain_admissions();
            if step > 0 && cancel.is_cancelled() {
                self.log(
                    "turn/end",
                    json!({"turn": turn, "reason": {"kind":"aborted","reason":{"kind":"user"}}}),
                    &[],
                )?;
                return Ok(vec![TurnEnd::Aborted]);
            }
            step += 1;
            let claimed = self.claim(if step == 1 && initial.is_none() {
                InboxTarget::NextTurn
            } else {
                InboxTarget::NextStep
            })?;
            self.log("step/start", json!({"turn": turn, "step": step}), &[])?;

            if step == 1 {
                self.refresh_system_message(turn, step)?;
            }
            if let Some(message) = initial.take() {
                self.admit_user_message(message, turn, step)?;
            }
            for message in claimed {
                self.admit_user_message(message, turn, step)?;
            }
            self.compact_if_needed(turn, cancel).await?;
            if !self.context_logged {
                self.context_logged = true;
                let data = json!({"provider": self.model.provider(), "model": self.model.model()});
                self.log("request/context", data, &[])?;
            }

            // Model request, cancellable. The opening frame precedes every chunk.
            self.assistant_attempt_counter += 1;
            let attempt_id = format!(
                "{}:{}",
                self.session.header.id, self.assistant_attempt_counter
            );
            let mut live = LiveAttempt::new(attempt_id);
            let start_revision = self.next_assistant_revision();
            let started_after_seq = self.durable_cursor();
            self.bus.emit(&AgentEvent::AssistantStream(live.start(
                start_revision,
                started_after_seq,
                turn,
                u64::from(step),
            )));

            let (tx, mut rx) = mpsc::channel(64);
            let (model, hist, tdefs) = (self.model.clone(), self.history.clone(), defs.clone());
            let task = tokio::spawn(async move { model.stream(&hist, &tdefs, tx).await });
            let mut asm = Assembler::default();
            let mut finish: Option<(FinishReason, Vec<Option<String>>)> = None;
            let mut usage: Option<TokenUsage> = None;
            let mut failure: Option<LlmError> = None;
            let mut aborted = false;
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { aborted = true; break; }
                    command = self.admission_rx.recv() => {
                        if let Some(command) = command {
                            command.execute(self);
                        }
                    }
                    item = rx.recv() => match item {
                        None => break,
                        Some(Ok(c)) => {
                            asm.push(&c);
                            let wire = stream_chunk_value(&c, &asm);
                            let timed = match live.push(TimedStreamChunk { time: now_ms(), chunk: wire }) {
                                Ok(timed) => timed,
                                Err(error) => {
                                    failure = Some(LlmError::new(
                                        ErrorCode::MalformedResponse,
                                        format!("cannot record assistant stream chunk: {error}"),
                                    ));
                                    break;
                                }
                            };
                            self.bus.emit(&AgentEvent::Chunk(c.clone()));
                            let revision = self.next_assistant_revision();
                            self.bus.emit(&AgentEvent::AssistantStream(live.chunk_frame(timed, revision)));
                            match c {
                                StreamChunk::Usage(value) => usage = Some(value),
                                StreamChunk::Finish { reason, replay } => finish = Some((reason, replay)),
                                _ => {}
                            }
                        }
                        Some(Err(e)) => { failure = Some(e); break; }
                    }
                }
            }
            task.abort();
            self.drain_admissions();
            aborted |= cancel.is_cancelled();

            if failure.is_none() && !aborted && finish.is_none() {
                failure = Some(LlmError::new(
                    ErrorCode::MalformedResponse,
                    "model stream ended without a finish",
                ));
            }
            if aborted || failure.is_some() {
                let interrupted_reason = finish
                    .as_ref()
                    .map_or(FinishReason::Stop, |(reason, _)| *reason);
                let replay = finish
                    .as_ref()
                    .map_or_else(Vec::new, |(_, replay)| replay.clone());
                let content = asm.finish(&replay, true, interrupted_reason);
                if !content.is_empty() {
                    let msg = Message {
                        role: Role::Assistant,
                        content,
                        tool_call_id: None,
                        is_error: None,
                    };
                    let src = self.model_source_with_replay(&replay);
                    let mut data = json!({
                        "turn": turn,
                        "step": step,
                        "message": self.message_value(&msg, src),
                        "interrupted": true,
                        "stream": live.stream_value(),
                    });
                    if let Some(usage) = usage {
                        data["usage"] = json!(usage);
                    }
                    self.settle_assistant_stream(
                        &live,
                        AssistantSettlementEventType::Message,
                        data,
                        &append,
                    )?;
                    self.history.push(msg);
                } else {
                    let data = json!({"turn": turn, "step": step, "stream": live.stream_value()});
                    self.settle_assistant_stream(
                        &live,
                        AssistantSettlementEventType::Attempt,
                        data,
                        &[],
                    )?;
                }
                self.log("step/end", json!({"turn": turn, "step": step}), &[])?;
                let (end, reason) = match failure {
                    Some(e) => {
                        let r = json!({"kind":"error","error":{"message":e.message,"code":error_code(e.code)}});
                        (TurnEnd::Error(e), r)
                    }
                    None => (
                        TurnEnd::Aborted,
                        json!({"kind":"aborted","reason":{"kind":"user"}}),
                    ),
                };
                self.log("turn/end", json!({"turn": turn, "reason": reason}), &[])?;
                return Ok(vec![end]);
            }

            let (reason, replay) = finish.expect("checked above");
            let content = asm.finish(&replay, false, reason);
            let calls: Vec<(String, String, String)> = content
                .iter()
                .filter_map(|c| match c {
                    Content::ToolCall {
                        id,
                        name,
                        arguments,
                    } => Some((id.clone(), name.clone(), arguments.clone())),
                    _ => None,
                })
                .collect();
            let msg = Message {
                role: Role::Assistant,
                content,
                tool_call_id: None,
                is_error: None,
            };
            let src = self.model_source_with_replay(&replay);
            let mut data = json!({
                "turn": turn,
                "step": step,
                "message": self.message_value(&msg, src),
                "stream": live.stream_value(),
            });
            if let Some(usage) = usage {
                data["usage"] = json!(usage);
            }
            self.settle_assistant_stream(
                &live,
                AssistantSettlementEventType::Message,
                data,
                &append,
            )?;
            self.history.push(msg);

            let mut turn_aborted = false;
            for (id, name, args) in &calls {
                let call_seq = self.log("tool/call", json!({"turn": turn, "step": step, "callId": id, "name": name, "arguments": args}), &[])?;
                let sink = EventSink::default();
                let (out, err_meta) = if turn_aborted || cancel.is_cancelled() {
                    turn_aborted = true;
                    (
                        ToolOutput::err("Error: tool call aborted"),
                        Some(json!({"name":"AbortError","code":"ABORTED"})),
                    )
                } else {
                    match self.tools.get(name).cloned() {
                        None => (
                            ToolOutput::err(format!("Error: unknown tool \"{name}\"")),
                            None,
                        ),
                        Some(tool) => {
                            let parsed: Result<Value, _> = serde_json::from_str(args);
                            match parsed {
                                Err(e) => (
                                    ToolOutput::err(format!("Error: invalid tool arguments: {e}")),
                                    None,
                                ),
                                Ok(v) => {
                                    let call_cx = ToolContext {
                                        cwd: cx.cwd.clone(),
                                        cancel: cx.cancel.clone(),
                                        events: sink.clone(),
                                        session: cx.session.clone(),
                                        call_id: id.clone(),
                                    };
                                    let call = tool.call(v, &call_cx);
                                    tokio::pin!(call);
                                    loop {
                                        tokio::select! {
                                            biased;
                                            _ = cancel.cancelled() => {
                                                turn_aborted = true;
                                                break (
                                                    ToolOutput::err("Error: tool call aborted"),
                                                    Some(json!({"name":"AbortError","code":"ABORTED"})),
                                                );
                                            }
                                            command = self.admission_rx.recv() => {
                                                if let Some(command) = command {
                                                    command.execute(self);
                                                }
                                            }
                                            o = &mut call => break (o, None),
                                        }
                                    }
                                }
                            }
                        }
                    }
                };
                // Commit the state the tool asked to record, in call order and
                // before the result, so replay sees `tool/call`, its events,
                // then `tool/result`.
                for (kind, data) in sink.drain() {
                    self.log(&kind, data, &[])?;
                }
                let result = Message {
                    role: Role::Tool,
                    content: vec![Content::Text {
                        text: out.text.clone(),
                    }],
                    tool_call_id: Some(id.clone()),
                    is_error: Some(out.is_error),
                };
                let mut data = json!({"turn": turn, "step": step, "message": self.message_value(&result, json!({"kind":"tool","callId":id}))});
                if let Some(m) = err_meta {
                    data["error"] = m;
                }
                self.log(
                    "tool/result",
                    data,
                    &[
                        ("sourceEventSeqs", json!([call_seq])),
                        ("surfaceOp", json!("append")),
                    ],
                )?;
                self.history.push(result);
            }
            self.drain_admissions();
            self.log("step/end", json!({"turn": turn, "step": step}), &[])?;
            self.drain_admissions();

            if turn_aborted || cancel.is_cancelled() {
                self.log(
                    "turn/end",
                    json!({"turn": turn, "reason": {"kind":"aborted","reason":{"kind":"user"}}}),
                    &[],
                )?;
                return Ok(vec![TurnEnd::Aborted]);
            }
            let step_end = match reason {
                FinishReason::ToolCalls if !calls.is_empty() => {
                    if step >= self.config.max_steps {
                        let r = json!({"kind":"error","error":{"message":"step limit reached","code":"STEP_LIMIT"}});
                        self.log("turn/end", json!({"turn": turn, "reason": r}), &[])?;
                        return Ok(vec![TurnEnd::StepLimit]);
                    }
                    None
                }
                FinishReason::MaxTokens => Some(TurnEnd::MaxTokens),
                _ => Some(TurnEnd::Completed),
            };
            if !self.inbox.next_step.is_empty() {
                sticky_max_tokens |= matches!(&step_end, Some(TurnEnd::MaxTokens));
                continue;
            }
            let Some(step_end) = step_end else {
                continue;
            };
            break if sticky_max_tokens && matches!(step_end, TurnEnd::Completed) {
                TurnEnd::MaxTokens
            } else {
                step_end
            };
        };
        let reason = match end {
            TurnEnd::MaxTokens => json!({"kind":"max-tokens"}),
            _ => json!({"kind":"completed"}),
        };
        self.log("turn/end", json!({"turn": turn, "reason": reason}), &[])?;
        Ok(vec![end])
    }
}

/// Commit time for a new log row, in milliseconds since the epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembler_uses_first_seen_order_and_ignores_closed_stragglers() {
        let mut assembler = Assembler::default();
        for chunk in [
            StreamChunk::BlockStart {
                index: 5,
                kind: BlockKind::Text,
            },
            StreamChunk::TextDelta {
                index: 5,
                text: "first".into(),
            },
            StreamChunk::ReasoningDelta {
                index: 2,
                text: "second".into(),
            },
            StreamChunk::BlockEnd { index: 5 },
            StreamChunk::TextDelta {
                index: 5,
                text: " straggler".into(),
            },
        ] {
            assembler.push(&chunk);
        }
        assert_eq!(
            assembler.finish(&[None, Some("sig".into())], false, FinishReason::Stop,),
            vec![
                Content::Text {
                    text: "first".into()
                },
                Content::Reasoning {
                    text: "second".into(),
                    signature: Some("sig".into())
                },
            ]
        );
    }

    #[test]
    fn assembler_preserves_tool_identity_and_applies_finish_drop_rules() {
        let mut assembler = Assembler::default();
        for chunk in [
            StreamChunk::ToolCallDelta {
                index: 3,
                id: Some("call-1".into()),
                name: Some("echo".into()),
                arguments_delta: "{".into(),
            },
            StreamChunk::ToolCallDelta {
                index: 3,
                id: None,
                name: None,
                arguments_delta: "}".into(),
            },
            StreamChunk::TextDelta {
                index: 0,
                text: "answer".into(),
            },
        ] {
            assembler.push(&chunk);
        }
        assert_eq!(
            assembler.finish(&[], false, FinishReason::ToolCalls),
            vec![
                Content::ToolCall {
                    id: "call-1".into(),
                    name: "echo".into(),
                    arguments: "{}".into(),
                },
                Content::Text {
                    text: "answer".into()
                },
            ]
        );
        assert_eq!(
            assembler.finish(&[], false, FinishReason::MaxTokens),
            vec![Content::Text {
                text: "answer".into()
            }]
        );
        assert_eq!(
            assembler.finish(&[], true, FinishReason::Stop),
            vec![Content::Text {
                text: "answer".into()
            }]
        );
    }

    #[test]
    fn receipt_recovery_ignores_inherited_events_before_end_seed() {
        let event = |seq: Option<u64>, kind: &str, data: Value| Event {
            kind: kind.into(),
            data,
            extra: seq
                .map(|seq| Map::from_iter([("seq".into(), json!(seq))]))
                .unwrap_or_default(),
        };
        let inserted = |id: &str, rpc_id: &str| {
            json!([{
                "id": id,
                "role": "user",
                "content": [{"type": "text", "text": rpc_id}],
                "source": {"kind": "user", "rpcId": rpc_id},
            }])
        };
        let events = vec![
            event(
                Some(0),
                "agent/inbox/spliced",
                json!({"inserted": inserted("msg-1", "inherited")}),
            ),
            event(
                Some(1),
                "user/message",
                json!({
                    "id": "msg-2",
                    "role": "user",
                    "content": [],
                    "source": {"kind": "retry", "rpcId": "inherited-retry"},
                }),
            ),
            event(Some(2), "session/end-seed", json!({})),
            event(
                Some(3),
                "agent/inbox/spliced",
                json!({"inserted": inserted("msg-3", "current")}),
            ),
            event(
                Some(4),
                "user/message",
                json!({
                    "id": "msg-4",
                    "role": "user",
                    "content": [],
                    "source": {"kind": "retry", "rpcId": "current-retry"},
                }),
            ),
            event(
                Some(5),
                "user/message",
                json!({
                    "id": "msg-5",
                    "role": "user",
                    "content": [],
                    "source": {"kind": "system", "rpcId": "ignored-kind"},
                }),
            ),
            event(
                None,
                "agent/inbox/spliced",
                json!({"inserted": inserted("msg-6", "unsequenced")}),
            ),
        ];

        let recovered = recover_request_ids(&events);
        let expected = BTreeSet::from([
            "current".into(),
            "current-retry".into(),
            "unsequenced".into(),
        ]);
        assert_eq!(recovered, expected);
    }

    #[test]
    fn wire_adapter_supplies_block_end_and_replay_shapes() {
        let mut assembler = Assembler::default();
        assembler.push(&StreamChunk::TextDelta {
            index: 4,
            text: "hello".into(),
        });
        assert_eq!(
            stream_chunk_value(&StreamChunk::BlockEnd { index: 4 }, &assembler),
            json!({
                "type":"block-end",
                "index":4,
                "block":{"type":"text","text":"hello"},
            })
        );
        assert_eq!(
            stream_chunk_value(
                &StreamChunk::ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments_delta: "{}".into(),
                },
                &assembler,
            ),
            json!({
                "type":"tool-call-delta",
                "index":1,
                "id":"",
                "argumentsDelta":"{}",
            })
        );
        assert_eq!(
            stream_chunk_value(
                &StreamChunk::Finish {
                    reason: FinishReason::MaxTokens,
                    replay: vec![None, Some("sig".into())],
                },
                &assembler,
            ),
            json!({
                "type":"finish",
                "reason":{"kind":"max-tokens"},
                "replayState":{"response":null,"blocks":[null,"sig"]},
            })
        );
    }
}
