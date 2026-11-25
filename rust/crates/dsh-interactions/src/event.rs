use crate::types::{
    ApprovalOutcome, ApprovalRequest, ApprovalRequestId, QuestionAnswer, QuestionState, ToolCallId,
    UserQuestion,
};
use crate::InteractionError;
use dsh_session_format::Event as SessionEvent;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Largest integer represented exactly by the JavaScript Session protocol.
pub const MAX_SAFE_SESSION_INTEGER: u64 = 9_007_199_254_740_991;

/// Stable discriminators for durable interaction records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InteractionEventKind {
    #[serde(rename = "approval/asked")]
    ApprovalAsked,
    #[serde(rename = "approval/decided")]
    ApprovalDecided,
    #[serde(rename = "question/asked")]
    QuestionAsked,
    #[serde(rename = "question/continued")]
    QuestionContinued,
    #[serde(rename = "question/settled")]
    QuestionSettled,
}

impl InteractionEventKind {
    /// Stable Session event discriminator.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ApprovalAsked => "approval/asked",
            Self::ApprovalDecided => "approval/decided",
            Self::QuestionAsked => "question/asked",
            Self::QuestionContinued => "question/continued",
            Self::QuestionSettled => "question/settled",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "approval/asked" => Some(Self::ApprovalAsked),
            "approval/decided" => Some(Self::ApprovalDecided),
            "question/asked" => Some(Self::QuestionAsked),
            "question/continued" => Some(Self::QuestionContinued),
            "question/settled" => Some(Self::QuestionSettled),
            _ => None,
        }
    }
}

/// Payload of `approval/asked`, matching the authoritative durable fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalAsked {
    pub id: ApprovalRequestId,
    pub tool_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<ToolCallId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl From<ApprovalRequest> for ApprovalAsked {
    fn from(request: ApprovalRequest) -> Self {
        Self {
            id: request.id,
            tool_name: request.tool_name,
            call_id: request.call_id,
            reason: request.reason,
        }
    }
}

/// Payload of `approval/decided`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApprovalDecided {
    pub id: ApprovalRequestId,
    pub outcome: ApprovalOutcome,
}

/// Normalized durable question request. Adapters must map real tool events to this record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuestionAsked {
    pub call_id: ToolCallId,
    pub questions: Vec<UserQuestion>,
    pub state: QuestionState,
}

/// Notification that a foreground question remains answerable in a later turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuestionContinued {
    pub call_id: ToolCallId,
}

/// Complete answer batch that settles a question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuestionSettled {
    pub call_id: ToolCallId,
    pub answers: Vec<QuestionAnswer>,
}

/// Typed interaction payload independent of its Session envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InteractionPayload {
    ApprovalAsked(ApprovalAsked),
    ApprovalDecided(ApprovalDecided),
    QuestionAsked(QuestionAsked),
    QuestionContinued(QuestionContinued),
    QuestionSettled(QuestionSettled),
}

impl InteractionPayload {
    pub fn kind(&self) -> InteractionEventKind {
        match self {
            Self::ApprovalAsked(_) => InteractionEventKind::ApprovalAsked,
            Self::ApprovalDecided(_) => InteractionEventKind::ApprovalDecided,
            Self::QuestionAsked(_) => InteractionEventKind::QuestionAsked,
            Self::QuestionContinued(_) => InteractionEventKind::QuestionContinued,
            Self::QuestionSettled(_) => InteractionEventKind::QuestionSettled,
        }
    }

    fn data(&self) -> EventData {
        match self {
            Self::ApprovalAsked(value) => EventData::ApprovalAsked(value.clone()),
            Self::ApprovalDecided(value) => EventData::ApprovalDecided(value.clone()),
            Self::QuestionAsked(value) => EventData::QuestionAsked(value.clone()),
            Self::QuestionContinued(value) => EventData::QuestionContinued(value.clone()),
            Self::QuestionSettled(value) => EventData::QuestionSettled(value.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum EventData {
    ApprovalAsked(ApprovalAsked),
    ApprovalDecided(ApprovalDecided),
    QuestionAsked(QuestionAsked),
    QuestionContinued(QuestionContinued),
    QuestionSettled(QuestionSettled),
}

/// One interaction record encoded with the Session v4 `type`, `data`, `seq`, and `time` fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableEvent {
    #[serde(rename = "type")]
    kind: InteractionEventKind,
    data: EventData,
    seq: u64,
    time: u64,
}

impl DurableEvent {
    /// Encode a typed payload with caller-owned Session metadata.
    pub fn new(payload: InteractionPayload, seq: u64, time: u64) -> Result<Self, InteractionError> {
        validate_session_integer(seq, "seq")?;
        validate_session_integer(time, "time")?;
        let event = Self {
            kind: payload.kind(),
            data: payload.data(),
            seq,
            time,
        };
        event.validate_kind()?;
        Ok(event)
    }

    /// Sequence assigned by the owning Session.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Commit time in Unix epoch milliseconds.
    pub fn time(&self) -> u64 {
        self.time
    }

    /// Interaction discriminator.
    pub fn kind(&self) -> InteractionEventKind {
        self.kind
    }

    /// Validate and return the typed payload.
    pub fn payload(&self) -> Result<InteractionPayload, InteractionError> {
        self.validate_kind()?;
        Ok(match &self.data {
            EventData::ApprovalAsked(value) => InteractionPayload::ApprovalAsked(value.clone()),
            EventData::ApprovalDecided(value) => InteractionPayload::ApprovalDecided(value.clone()),
            EventData::QuestionAsked(value) => InteractionPayload::QuestionAsked(value.clone()),
            EventData::QuestionContinued(value) => {
                InteractionPayload::QuestionContinued(value.clone())
            }
            EventData::QuestionSettled(value) => InteractionPayload::QuestionSettled(value.clone()),
        })
    }

    /// Convert to the generic event type used by `dsh-session-format`.
    pub fn to_session_event(&self) -> Result<SessionEvent, InteractionError> {
        let payload = self.payload()?;
        let data = serde_json::to_value(payload.data()).map_err(|error| {
            InteractionError::validation(format!("interaction event does not serialize: {error}"))
        })?;
        let mut extra = Map::new();
        extra.insert("seq".to_owned(), Value::from(self.seq));
        extra.insert("time".to_owned(), Value::from(self.time));
        let kind = payload.kind().as_str().to_owned();
        Ok(SessionEvent { kind, data, extra })
    }

    /// Parse a known interaction event from a Session row; unrelated event kinds return `None`.
    pub fn from_session_event(event: &SessionEvent) -> Result<Option<Self>, InteractionError> {
        let Some(kind) = InteractionEventKind::parse(&event.kind) else {
            return Ok(None);
        };
        let seq = event.seq().ok_or_else(|| {
            InteractionError::validation("interaction Session event is missing a valid seq")
        })?;
        let time = event.time().ok_or_else(|| {
            InteractionError::validation("interaction Session event is missing a valid time")
        })?;
        let data = serde_json::from_value(event.data.clone()).map_err(|error| {
            InteractionError::validation(format!("malformed {} data: {error}", event.kind))
        })?;
        let decoded = Self {
            kind,
            data,
            seq,
            time,
        };
        decoded.validate_kind()?;
        Ok(Some(decoded))
    }

    fn validate_kind(&self) -> Result<(), InteractionError> {
        validate_session_integer(self.seq, "seq")?;
        validate_session_integer(self.time, "time")?;
        let matches_kind = matches!(
            (self.kind, &self.data),
            (
                InteractionEventKind::ApprovalAsked,
                EventData::ApprovalAsked(_)
            ) | (
                InteractionEventKind::ApprovalDecided,
                EventData::ApprovalDecided(_)
            ) | (
                InteractionEventKind::QuestionAsked,
                EventData::QuestionAsked(_)
            ) | (
                InteractionEventKind::QuestionContinued,
                EventData::QuestionContinued(_)
            ) | (
                InteractionEventKind::QuestionSettled,
                EventData::QuestionSettled(_)
            )
        );
        if !matches_kind {
            return Err(InteractionError::validation(
                "interaction event discriminator does not match its data",
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_session_integer(value: u64, field: &str) -> Result<(), InteractionError> {
    if value > MAX_SAFE_SESSION_INTEGER {
        return Err(InteractionError::validation(format!(
            "{field} exceeds the Session safe-integer range"
        )));
    }
    Ok(())
}
