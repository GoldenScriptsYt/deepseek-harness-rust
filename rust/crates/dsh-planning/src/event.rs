use crate::goal::{GoalChange, GoalMessageSource};
use crate::plan::{CommandDone, CommandRun, PlanMode};
use crate::schedule::ScheduleChange;
use crate::todo::TodoWrite;
use crate::util::{exact_keys, field, object, validate_safe};
use crate::{PlanningError, PlanningResult};
use dsh_session_format::Event as SessionEvent;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

/// Largest integer represented exactly by the JavaScript Session protocol.
pub const MAX_SAFE_SESSION_INTEGER: u64 = 9_007_199_254_740_991;

/// Event discriminators consumed by the planning projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanningEventKind {
    #[serde(rename = "goal/change")]
    GoalChange,
    #[serde(rename = "user/message")]
    UserMessage,
    #[serde(rename = "command/run")]
    CommandRun,
    #[serde(rename = "command/done")]
    CommandDone,
    #[serde(rename = "plan/mode")]
    PlanMode,
    #[serde(rename = "todo/write")]
    TodoWrite,
    #[serde(rename = "turn/start")]
    TurnStart,
    #[serde(rename = "schedule/entry/change")]
    ScheduleEntryChange,
}

impl PlanningEventKind {
    /// Stable Session event name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GoalChange => "goal/change",
            Self::UserMessage => "user/message",
            Self::CommandRun => "command/run",
            Self::CommandDone => "command/done",
            Self::PlanMode => "plan/mode",
            Self::TodoWrite => "todo/write",
            Self::TurnStart => "turn/start",
            Self::ScheduleEntryChange => "schedule/entry/change",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "goal/change" => Self::GoalChange,
            "user/message" => Self::UserMessage,
            "command/run" => Self::CommandRun,
            "command/done" => Self::CommandDone,
            "plan/mode" => Self::PlanMode,
            "todo/write" => Self::TodoWrite,
            "turn/start" => Self::TurnStart,
            "schedule/entry/change" => Self::ScheduleEntryChange,
            _ => return None,
        })
    }
}

/// Strict `turn/start` payload needed to clear per-turn todo state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStart {
    pub turn: u64,
}

impl TurnStart {
    fn validate(&self) -> PlanningResult<()> {
        validate_safe(self.turn, "turn/start.turn")?;
        Ok(())
    }
}

/// Typed payload of one planning-relevant Session event.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Public payload shape; values are short-lived.
pub enum PlanningPayload {
    GoalChange(GoalChange),
    /// Complete original `user/message` data whose source identifies a Goal round.
    /// Non-Goal messages are deliberately represented by `None` at the event boundary.
    UserMessage(Value),
    CommandRun(CommandRun),
    CommandDone(CommandDone),
    PlanMode(PlanMode),
    TodoWrite(TodoWrite),
    TurnStart(TurnStart),
    /// Rust-local current Schedule entry mutation.
    ScheduleEntryChange(ScheduleChange),
}

impl From<GoalChange> for PlanningPayload {
    fn from(change: GoalChange) -> Self {
        Self::GoalChange(change)
    }
}

impl PlanningPayload {
    /// Event discriminator for this payload.
    pub const fn kind(&self) -> PlanningEventKind {
        match self {
            Self::GoalChange(_) => PlanningEventKind::GoalChange,
            Self::UserMessage(_) => PlanningEventKind::UserMessage,
            Self::CommandRun(_) => PlanningEventKind::CommandRun,
            Self::CommandDone(_) => PlanningEventKind::CommandDone,
            Self::PlanMode(_) => PlanningEventKind::PlanMode,
            Self::TodoWrite(_) => PlanningEventKind::TodoWrite,
            Self::TurnStart(_) => PlanningEventKind::TurnStart,
            Self::ScheduleEntryChange(_) => PlanningEventKind::ScheduleEntryChange,
        }
    }

    /// Validate and retain a complete user-message body carrying Goal attribution.
    pub fn user_message(data: Value) -> PlanningResult<Self> {
        goal_source(&data)?.ok_or_else(|| {
            PlanningError::invalid("user/message.source", "must identify a Goal round")
        })?;
        Ok(Self::UserMessage(data))
    }

    pub(crate) fn validate(&self) -> PlanningResult<()> {
        match self {
            Self::GoalChange(change) => change.validate(),
            Self::UserMessage(data) => Self::user_message(data.clone()).map(|_| ()),
            Self::CommandRun(command) => command.validate(),
            Self::CommandDone(command) => command.validate(),
            Self::PlanMode(_) => Ok(()),
            Self::TodoWrite(write) => write.validate_shape(),
            Self::TurnStart(turn) => turn.validate(),
            Self::ScheduleEntryChange(change) => change.validate(),
        }
    }

    pub(crate) fn to_value(&self) -> PlanningResult<Value> {
        self.validate()?;
        match self {
            Self::GoalChange(change) => change.to_value(),
            Self::UserMessage(data) => Ok(data.clone()),
            Self::CommandRun(command) => command.to_value(),
            Self::CommandDone(command) => command.to_value(),
            Self::PlanMode(mode) => serde_json::to_value(mode).map_err(PlanningError::json),
            Self::TodoWrite(write) => serde_json::to_value(write).map_err(PlanningError::json),
            Self::TurnStart(turn) => serde_json::to_value(turn).map_err(PlanningError::json),
            Self::ScheduleEntryChange(change) => change.to_value(),
        }
    }

    fn from_parts(kind: PlanningEventKind, data: Value) -> PlanningResult<Self> {
        let payload = match kind {
            PlanningEventKind::GoalChange => Self::GoalChange(GoalChange::from_value(data)?),
            PlanningEventKind::UserMessage => Self::user_message(data)?,
            PlanningEventKind::CommandRun => Self::CommandRun(CommandRun::from_value(data)?),
            PlanningEventKind::CommandDone => Self::CommandDone(CommandDone::from_value(data)?),
            PlanningEventKind::PlanMode => Self::PlanMode(decode_plan_mode(data)?),
            PlanningEventKind::TodoWrite => Self::TodoWrite(decode_todo_write(data)?),
            PlanningEventKind::TurnStart => Self::TurnStart(decode_turn_start(data)?),
            PlanningEventKind::ScheduleEntryChange => {
                Self::ScheduleEntryChange(ScheduleChange::from_value(data)?)
            }
        };
        payload.validate()?;
        Ok(payload)
    }
}

fn decode_plan_mode(value: Value) -> PlanningResult<PlanMode> {
    let map = object(value, "plan/mode")?;
    exact_keys(&map, &["active"], &[], "plan/mode")?;
    Ok(PlanMode {
        active: field(&map, "active", "plan/mode")?,
    })
}

fn decode_todo_write(value: Value) -> PlanningResult<TodoWrite> {
    let map = object(value, "todo/write")?;
    exact_keys(&map, &["todos"], &[], "todo/write")?;
    let write = TodoWrite {
        todos: field(&map, "todos", "todo/write")?,
    };
    write.validate_shape()?;
    Ok(write)
}

fn decode_turn_start(value: Value) -> PlanningResult<TurnStart> {
    let map = object(value, "turn/start")?;
    exact_keys(&map, &["turn"], &[], "turn/start")?;
    let turn = TurnStart {
        turn: field(&map, "turn", "turn/start")?,
    };
    turn.validate()?;
    Ok(turn)
}

fn goal_source(data: &Value) -> PlanningResult<Option<GoalMessageSource>> {
    let map = object(data.clone(), "user/message")?;
    let source = map
        .get("source")
        .ok_or_else(|| PlanningError::invalid("user/message", "missing required source"))?;
    GoalMessageSource::from_source(source)
}

/// Validated planning event with Session-owned sequence and commit time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanningEvent {
    payload: PlanningPayload,
    seq: u64,
    time: u64,
}

impl PlanningEvent {
    /// Construct a typed event, validating both payload and Session safe integers.
    pub fn new(payload: PlanningPayload, seq: u64, time: u64) -> PlanningResult<Self> {
        validate_metadata(seq, time)?;
        payload.validate()?;
        Ok(Self { payload, seq, time })
    }

    /// Sequence assigned by the owning Session.
    pub const fn seq(&self) -> u64 {
        self.seq
    }

    /// Commit time in Unix epoch milliseconds.
    pub const fn time(&self) -> u64 {
        self.time
    }

    /// Borrow the typed payload.
    pub const fn payload(&self) -> &PlanningPayload {
        &self.payload
    }

    /// Consume the event and return its typed payload.
    pub fn into_payload(self) -> PlanningPayload {
        self.payload
    }

    /// Encode the generic Session v4 envelope without losing the original user-message body.
    pub fn to_session_event(&self) -> PlanningResult<SessionEvent> {
        let mut extra = Map::new();
        extra.insert("seq".into(), Value::from(self.seq));
        extra.insert("time".into(), Value::from(self.time));
        Ok(SessionEvent {
            kind: self.payload.kind().as_str().into(),
            data: self.payload.to_value()?,
            extra,
        })
    }

    /// Decode a known planning event. Unknown kinds, non-Goal user messages, and
    /// frozen historical `schedule/change` events return `None`.
    ///
    /// Metadata and known payloads are validated before callers inspect sequence
    /// staleness, so a malformed known event cannot hide behind a duplicate seq.
    pub fn from_session_event(event: &SessionEvent) -> PlanningResult<Option<Self>> {
        let (seq, time) = session_metadata(event)?;
        let Some(kind) = PlanningEventKind::parse(&event.kind) else {
            return Ok(None);
        };
        if kind == PlanningEventKind::UserMessage && goal_source(&event.data)?.is_none() {
            return Ok(None);
        }
        let payload = PlanningPayload::from_parts(kind, event.data.clone())?;
        Ok(Some(Self { payload, seq, time }))
    }
}

pub(crate) fn validate_metadata(seq: u64, time: u64) -> PlanningResult<()> {
    validate_safe(seq, "Session.seq")?;
    validate_safe(time, "Session.time")?;
    Ok(())
}

pub(crate) fn session_metadata(event: &SessionEvent) -> PlanningResult<(u64, u64)> {
    let seq = event
        .extra
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            PlanningError::invalid("Session.seq", "is required as a non-negative integer")
        })?;
    let time = event
        .extra
        .get("time")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            PlanningError::invalid("Session.time", "is required as a non-negative integer")
        })?;
    validate_metadata(seq, time)?;
    Ok((seq, time))
}

impl<'de> Deserialize<'de> for PlanningEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let event = SessionEvent::deserialize(deserializer)?;
        Self::from_session_event(&event)
            .map_err(serde::de::Error::custom)?
            .ok_or_else(|| serde::de::Error::custom("event is unrelated to planning"))
    }
}

impl Serialize for PlanningEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_session_event()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
