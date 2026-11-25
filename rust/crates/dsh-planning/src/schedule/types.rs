use super::time::{
    canonical_cron, canonical_instant, canonical_local_time, canonical_rrule, time_zone,
};
use crate::ids::{MessageId, ScheduleId, SessionId};
use crate::util::{
    deserialize_value, exact_keys, field, object, optional_field, string_field, validate_positive,
    validate_safe,
};
use crate::{PlanningError, PlanningResult};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

const MAX_TITLE_LENGTH: usize = 120;
const MIN_EVERY_SECONDS: u64 = 60;

/// Stable discriminator for all current Schedule timing records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    After,
    At,
    Every,
    Daily,
    Weekly,
    Cron,
    Rrule,
}

impl ScheduleKind {
    /// Durable record discriminator.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::After => "after",
            Self::At => "at",
            Self::Every => "every",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Cron => "cron",
            Self::Rrule => "rrule",
        }
    }
}

fn validate_id(id: &ScheduleId) -> PlanningResult<()> {
    if id.as_str().is_empty() || id.as_str().trim() != id.as_str() {
        return Err(PlanningError::invalid(
            "schedule.id",
            "must be non-empty without surrounding whitespace",
        ));
    }
    Ok(())
}

fn validate_title(title: &str) -> PlanningResult<()> {
    if title.is_empty() || title.trim() != title {
        return Err(PlanningError::invalid(
            "schedule.title",
            "must be non-empty and already trimmed",
        ));
    }
    if title.encode_utf16().count() > MAX_TITLE_LENGTH {
        return Err(PlanningError::invalid(
            "schedule.title",
            "must be at most 120 JavaScript characters",
        ));
    }
    Ok(())
}

fn validate_prompt(prompt: &str) -> PlanningResult<()> {
    if prompt.is_empty() || prompt.trim() != prompt {
        return Err(PlanningError::invalid(
            "schedule.prompt",
            "must be non-empty and already trimmed",
        ));
    }
    Ok(())
}

fn validate_header(
    id: &ScheduleId,
    title: &str,
    prompt: &str,
    scheduled_at: &str,
) -> PlanningResult<()> {
    validate_id(id)?;
    validate_title(title)?;
    validate_prompt(prompt)?;
    canonical_instant(scheduled_at)
}

/// Delayed one-shot Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AfterScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub after_seconds: u64,
    pub scheduled_at: String,
}

impl AfterScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::After {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the after record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)?;
        validate_positive(self.after_seconds, "schedule.afterSeconds")?;
        Ok(())
    }
}

/// Absolute one-shot Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AtScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub scheduled_at: String,
}

impl AtScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::At {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the at record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)
    }
}

/// Fixed-rate recurring Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EveryScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub every_seconds: u64,
    pub scheduled_at: String,
}

impl EveryScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::Every {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the every record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)?;
        validate_safe(self.every_seconds, "schedule.everySeconds")?;
        if self.every_seconds < MIN_EVERY_SECONDS
            || self
                .every_seconds
                .checked_mul(1_000)
                .is_none_or(|millis| millis > crate::MAX_SAFE_SESSION_INTEGER)
        {
            return Err(PlanningError::invalid(
                "schedule.everySeconds",
                "must be at least 60 and have a safe millisecond interval",
            ));
        }
        Ok(())
    }
}

/// Daily wall-clock Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DailyScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub time: String,
    pub time_zone: String,
    pub scheduled_at: String,
}

impl DailyScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::Daily {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the daily record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)?;
        canonical_local_time(&self.time)?;
        time_zone(&self.time_zone).map(|_| ())
    }
}

/// Weekly wall-clock Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WeeklyScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub time: String,
    pub time_zone: String,
    pub weekdays: Vec<u8>,
    pub scheduled_at: String,
}

impl WeeklyScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::Weekly {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the weekly record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)?;
        canonical_local_time(&self.time)?;
        time_zone(&self.time_zone).map(|_| ())?;
        if self.weekdays.is_empty() || self.weekdays.len() > 7 {
            return Err(PlanningError::invalid(
                "schedule.weekdays",
                "must contain one through seven ISO weekdays",
            ));
        }
        let mut previous = 0;
        for weekday in &self.weekdays {
            if !(1..=7).contains(weekday) || *weekday <= previous {
                return Err(PlanningError::invalid(
                    "schedule.weekdays",
                    "must be unique ascending ISO weekdays from 1 through 7",
                ));
            }
            previous = *weekday;
        }
        Ok(())
    }
}

/// Five-field Vixie Cron Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub expression: String,
    pub time_zone: String,
    pub scheduled_at: String,
}

impl CronScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::Cron {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the cron record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)?;
        time_zone(&self.time_zone).map(|_| ())?;
        canonical_cron(&self.expression)
    }
}

/// Bounded RFC 5545 DTSTART/RRULE Schedule record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RRuleScheduleRecord {
    pub id: ScheduleId,
    pub kind: ScheduleKind,
    pub title: String,
    pub prompt: String,
    pub rrule: String,
    pub scheduled_at: String,
}

impl RRuleScheduleRecord {
    fn validate(&self) -> PlanningResult<()> {
        if self.kind != ScheduleKind::Rrule {
            return Err(PlanningError::invalid(
                "schedule.kind",
                "must match the rrule record variant",
            ));
        }
        validate_header(&self.id, &self.title, &self.prompt, &self.scheduled_at)?;
        canonical_rrule(&self.rrule)
    }
}

/// One validated current Schedule timing record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleRecord {
    After(AfterScheduleRecord),
    At(AtScheduleRecord),
    Every(EveryScheduleRecord),
    Daily(DailyScheduleRecord),
    Weekly(WeeklyScheduleRecord),
    Cron(CronScheduleRecord),
    Rrule(RRuleScheduleRecord),
}

impl ScheduleRecord {
    /// Stable timing discriminator.
    pub const fn kind(&self) -> ScheduleKind {
        match self {
            Self::After(_) => ScheduleKind::After,
            Self::At(_) => ScheduleKind::At,
            Self::Every(_) => ScheduleKind::Every,
            Self::Daily(_) => ScheduleKind::Daily,
            Self::Weekly(_) => ScheduleKind::Weekly,
            Self::Cron(_) => ScheduleKind::Cron,
            Self::Rrule(_) => ScheduleKind::Rrule,
        }
    }

    /// Globally stable task identity.
    pub const fn id(&self) -> &ScheduleId {
        match self {
            Self::After(record) => &record.id,
            Self::At(record) => &record.id,
            Self::Every(record) => &record.id,
            Self::Daily(record) => &record.id,
            Self::Weekly(record) => &record.id,
            Self::Cron(record) => &record.id,
            Self::Rrule(record) => &record.id,
        }
    }

    /// Committed next or final UTC occurrence.
    pub fn scheduled_at(&self) -> &str {
        match self {
            Self::After(record) => &record.scheduled_at,
            Self::At(record) => &record.scheduled_at,
            Self::Every(record) => &record.scheduled_at,
            Self::Daily(record) => &record.scheduled_at,
            Self::Weekly(record) => &record.scheduled_at,
            Self::Cron(record) => &record.scheduled_at,
            Self::Rrule(record) => &record.scheduled_at,
        }
    }

    pub(crate) fn validate(&self) -> PlanningResult<()> {
        match self {
            Self::After(record) => record.validate(),
            Self::At(record) => record.validate(),
            Self::Every(record) => record.validate(),
            Self::Daily(record) => record.validate(),
            Self::Weekly(record) => record.validate(),
            Self::Cron(record) => record.validate(),
            Self::Rrule(record) => record.validate(),
        }
    }

    fn to_value(&self) -> PlanningResult<Value> {
        self.validate()?;
        match self {
            Self::After(record) => serde_json::to_value(record),
            Self::At(record) => serde_json::to_value(record),
            Self::Every(record) => serde_json::to_value(record),
            Self::Daily(record) => serde_json::to_value(record),
            Self::Weekly(record) => serde_json::to_value(record),
            Self::Cron(record) => serde_json::to_value(record),
            Self::Rrule(record) => serde_json::to_value(record),
        }
        .map_err(PlanningError::json)
    }

    fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "schedule.record")?;
        let kind: String = string_field(&map, "kind", "schedule.record")?;
        let record = match kind.as_str() {
            "after" => {
                exact_keys(
                    &map,
                    &[
                        "id",
                        "kind",
                        "title",
                        "prompt",
                        "afterSeconds",
                        "scheduledAt",
                    ],
                    &[],
                    "schedule.after",
                )?;
                Self::After(
                    serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?,
                )
            }
            "at" => {
                exact_keys(
                    &map,
                    &["id", "kind", "title", "prompt", "scheduledAt"],
                    &[],
                    "schedule.at",
                )?;
                Self::At(serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?)
            }
            "every" => {
                exact_keys(
                    &map,
                    &[
                        "id",
                        "kind",
                        "title",
                        "prompt",
                        "everySeconds",
                        "scheduledAt",
                    ],
                    &[],
                    "schedule.every",
                )?;
                Self::Every(
                    serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?,
                )
            }
            "daily" => {
                exact_keys(
                    &map,
                    &[
                        "id",
                        "kind",
                        "title",
                        "prompt",
                        "time",
                        "timeZone",
                        "scheduledAt",
                    ],
                    &[],
                    "schedule.daily",
                )?;
                Self::Daily(
                    serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?,
                )
            }
            "weekly" => {
                exact_keys(
                    &map,
                    &[
                        "id",
                        "kind",
                        "title",
                        "prompt",
                        "time",
                        "timeZone",
                        "weekdays",
                        "scheduledAt",
                    ],
                    &[],
                    "schedule.weekly",
                )?;
                Self::Weekly(
                    serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?,
                )
            }
            "cron" => {
                exact_keys(
                    &map,
                    &[
                        "id",
                        "kind",
                        "title",
                        "prompt",
                        "expression",
                        "timeZone",
                        "scheduledAt",
                    ],
                    &[],
                    "schedule.cron",
                )?;
                Self::Cron(serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?)
            }
            "rrule" => {
                exact_keys(
                    &map,
                    &["id", "kind", "title", "prompt", "rrule", "scheduledAt"],
                    &[],
                    "schedule.rrule",
                )?;
                Self::Rrule(
                    serde_json::from_value(Value::Object(map)).map_err(PlanningError::json)?,
                )
            }
            _ => return Err(PlanningError::invalid("schedule.kind", "is invalid")),
        };
        record.validate()?;
        Ok(record)
    }
}

impl Serialize for ScheduleRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ScheduleRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_value(deserialize_value(deserializer).map_err(D::Error::custom)?)
            .map_err(D::Error::custom)
    }
}

/// Values used when a Schedule delivery creates a Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleCreateSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_preset: Option<String>,
}

impl ScheduleCreateSpec {
    fn validate(&self) -> PlanningResult<()> {
        for value in [&self.cwd, &self.agent_preset].into_iter().flatten() {
            if value.is_empty() {
                return Err(PlanningError::invalid(
                    "schedule.create",
                    "values must be non-empty when present",
                ));
            }
        }
        Ok(())
    }

    fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "schedule.create")?;
        exact_keys(&map, &[], &["cwd", "agentPreset"], "schedule.create")?;
        let spec = Self {
            cwd: optional_field(&map, "cwd", "schedule.create")?,
            agent_preset: optional_field(&map, "agentPreset", "schedule.create")?,
        };
        spec.validate()?;
        Ok(spec)
    }
}

impl<'de> Deserialize<'de> for ScheduleCreateSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_value(deserialize_value(deserializer).map_err(D::Error::custom)?)
            .map_err(D::Error::custom)
    }
}

/// Durable destination of future Schedule deliveries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScheduleTarget {
    Session {
        #[serde(rename = "sessionId")]
        session_id: SessionId,
    },
    NewOnce {
        #[serde(rename = "sessionId")]
        session_id: SessionId,
        create: ScheduleCreateSpec,
    },
    New {
        create: ScheduleCreateSpec,
    },
}

impl ScheduleTarget {
    fn validate(&self) -> PlanningResult<()> {
        match self {
            Self::Session { session_id } | Self::NewOnce { session_id, .. } => {
                validate_session_id(session_id)
            }
            Self::New { create } => create.validate(),
        }
    }
}

/// Active, paused, and ended entries remain durable; only active entries imply a future obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleStatus {
    Active,
    Paused,
    Ended,
}

/// Durable inbox acknowledgment, not model execution completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryReceipt {
    pub scheduled_at: String,
    pub delivered_at: String,
    pub message_id: MessageId,
    pub target_session_id: SessionId,
}

impl DeliveryReceipt {
    fn validate(&self) -> PlanningResult<()> {
        canonical_instant(&self.scheduled_at)?;
        canonical_instant(&self.delivered_at)?;
        validate_message_id(&self.message_id)?;
        validate_session_id(&self.target_session_id)
    }
}

fn validate_session_id(id: &SessionId) -> PlanningResult<()> {
    if id.as_str().is_empty() {
        return Err(PlanningError::invalid(
            "schedule.sessionId",
            "must be non-empty",
        ));
    }
    Ok(())
}

fn validate_message_id(id: &MessageId) -> PlanningResult<()> {
    if id.as_str().is_empty() || id.as_str().trim() != id.as_str() {
        return Err(PlanningError::invalid(
            "schedule.messageId",
            "must be non-empty without surrounding whitespace",
        ));
    }
    Ok(())
}

/// Rust-local durable Schedule entry: current record, destination, status, and latest receipt.
///
/// Runtime execution flags, delivery queues, and the Host's full retained delivery
/// history are deliberately absent. The caller owns any persistence or runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleEntry {
    pub record: ScheduleRecord,
    pub target: ScheduleTarget,
    pub involved_session_ids: Vec<SessionId>,
    pub status: ScheduleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_delivery: Option<DeliveryReceipt>,
}

/// Descriptive alias for consumers that use the TypeScript catalog terminology.
pub type ScheduleCatalogEntry = ScheduleEntry;

impl ScheduleEntry {
    pub(crate) fn validate(&self, max_involved: usize) -> PlanningResult<()> {
        self.record.validate()?;
        self.target.validate()?;
        if self.involved_session_ids.len() > max_involved {
            return Err(PlanningError::LimitExceeded {
                field: "schedule involved Session ids",
                limit: max_involved,
            });
        }
        for id in &self.involved_session_ids {
            validate_session_id(id)?;
        }
        if let Some(created_at) = &self.created_at {
            canonical_instant(created_at)?;
        }
        if let Some(updated_at) = &self.updated_at {
            canonical_instant(updated_at)?;
        }
        if matches!((&self.created_at, &self.updated_at), (Some(created), Some(updated)) if updated < created)
        {
            return Err(PlanningError::invalid(
                "schedule.updatedAt",
                "cannot precede schedule.createdAt",
            ));
        }
        if let Some(receipt) = &self.last_delivery {
            receipt.validate()?;
        }
        Ok(())
    }

    fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "schedule.entry")?;
        exact_keys(
            &map,
            &["record", "target", "involvedSessionIds", "status"],
            &["createdAt", "updatedAt", "lastDelivery"],
            "schedule.entry",
        )?;
        let entry = Self {
            record: field(&map, "record", "schedule.entry")?,
            target: field(&map, "target", "schedule.entry")?,
            involved_session_ids: field(&map, "involvedSessionIds", "schedule.entry")?,
            status: field(&map, "status", "schedule.entry")?,
            created_at: optional_field(&map, "createdAt", "schedule.entry")?,
            updated_at: optional_field(&map, "updatedAt", "schedule.entry")?,
            last_delivery: optional_field(&map, "lastDelivery", "schedule.entry")?,
        };
        entry.validate(usize::MAX)?;
        Ok(entry)
    }
}

impl<'de> Deserialize<'de> for ScheduleEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_value(deserialize_value(deserializer).map_err(D::Error::custom)?)
            .map_err(D::Error::custom)
    }
}

/// Whole-value Rust-local `schedule/entry/change` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Public payload shape; values are short-lived.
pub enum ScheduleChange {
    Put(ScheduleEntry),
    Delete(ScheduleId),
}

/// Descriptive alias for the local Schedule mutation payload.
pub type ScheduleEntryChange = ScheduleChange;

impl ScheduleChange {
    pub(crate) const VERSION: u8 = 1;

    pub(crate) fn validate(&self) -> PlanningResult<()> {
        match self {
            Self::Put(entry) => entry.validate(usize::MAX),
            Self::Delete(id) => {
                validate_id(id)?;
                Ok(())
            }
        }
    }

    pub(crate) fn to_value(&self) -> PlanningResult<Value> {
        self.validate()?;
        let mut map = Map::new();
        map.insert("version".into(), Value::from(Self::VERSION));
        match self {
            Self::Put(entry) => {
                map.insert("operation".into(), Value::String("put".into()));
                map.insert(
                    "entry".into(),
                    serde_json::to_value(entry).map_err(PlanningError::json)?,
                );
            }
            Self::Delete(id) => {
                map.insert("operation".into(), Value::String("delete".into()));
                map.insert(
                    "id".into(),
                    serde_json::to_value(id).map_err(PlanningError::json)?,
                );
            }
        }
        Ok(Value::Object(map))
    }

    pub(crate) fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "schedule/entry/change")?;
        let version: u8 = field(&map, "version", "schedule/entry/change")?;
        if version != Self::VERSION {
            return Err(PlanningError::invalid(
                "schedule/entry/change.version",
                "is unsupported",
            ));
        }
        let operation: String = string_field(&map, "operation", "schedule/entry/change")?;
        let change = match operation.as_str() {
            "put" => {
                exact_keys(
                    &map,
                    &["version", "operation", "entry"],
                    &[],
                    "schedule/entry/change",
                )?;
                Self::Put(field(&map, "entry", "schedule/entry/change")?)
            }
            "delete" => {
                exact_keys(
                    &map,
                    &["version", "operation", "id"],
                    &[],
                    "schedule/entry/change",
                )?;
                Self::Delete(field(&map, "id", "schedule/entry/change")?)
            }
            _ => {
                return Err(PlanningError::invalid(
                    "schedule/entry/change.operation",
                    "is invalid",
                ))
            }
        };
        change.validate()?;
        Ok(change)
    }
}

impl<'de> Deserialize<'de> for ScheduleChange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_value(deserialize_value(deserializer).map_err(D::Error::custom)?)
            .map_err(D::Error::custom)
    }
}
