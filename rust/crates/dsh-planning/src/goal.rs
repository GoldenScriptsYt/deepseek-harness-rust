use crate::ids::GoalId;
use crate::util::{
    deserialize_value, exact_keys, field, normalized_nonempty, object, string_field,
    validate_positive, validate_safe,
};
use crate::{PlanningError, PlanningResult};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

/// Lifecycle phase of the current Goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalPhase {
    Active,
    Paused,
    Blocked,
    Complete,
}

/// Canonical explanation for a blocked Goal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalBlockReason {
    pub code: String,
    pub message: String,
}

impl GoalBlockReason {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        let bytes = self.code.as_bytes();
        let kebab = !bytes.is_empty()
            && bytes[0].is_ascii_lowercase()
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            && !self.code.contains("--")
            && !self.code.ends_with('-');
        if !kebab {
            return Err(PlanningError::invalid(
                "goal.blockedReason.code",
                "must be lower-kebab-case",
            ));
        }
        normalized_nonempty(&self.message, "goal.blockedReason.message")
    }
}

/// Revisioned Goal snapshot carried by every non-clear `goal/change`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalSnapshot {
    pub id: GoalId,
    pub revision: u64,
    pub objective: String,
    pub phase: GoalPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<GoalBlockReason>,
    pub max_goal_rounds: u64,
}

impl GoalSnapshot {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        if self.id.as_str().is_empty() {
            return Err(PlanningError::invalid("goal.id", "must be non-empty"));
        }
        validate_positive(self.revision, "goal.revision")?;
        validate_positive(self.max_goal_rounds, "goal.maxGoalRounds")?;
        normalized_nonempty(&self.objective, "goal.objective")?;
        match (&self.phase, &self.blocked_reason) {
            (GoalPhase::Blocked, Some(reason)) => reason.validate(),
            (GoalPhase::Blocked, None) => Err(PlanningError::invalid(
                "goal.blockedReason",
                "is required exactly for a blocked Goal",
            )),
            (_, None) => Ok(()),
            (_, Some(_)) => Err(PlanningError::invalid(
                "goal.blockedReason",
                "is forbidden unless the Goal is blocked",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for GoalSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = deserialize_value(deserializer).map_err(D::Error::custom)?;
        let map = object(value, "goal").map_err(D::Error::custom)?;
        let phase: GoalPhase = field(&map, "phase", "goal").map_err(D::Error::custom)?;
        let required: &[&str] = if phase == GoalPhase::Blocked {
            &[
                "id",
                "revision",
                "objective",
                "phase",
                "blockedReason",
                "maxGoalRounds",
            ]
        } else {
            &["id", "revision", "objective", "phase", "maxGoalRounds"]
        };
        exact_keys(&map, required, &[], "goal").map_err(D::Error::custom)?;
        let id = GoalId::new(string_field(&map, "id", "goal").map_err(D::Error::custom)?)
            .map_err(D::Error::custom)?;
        let revision = field(&map, "revision", "goal").map_err(D::Error::custom)?;
        let objective = string_field(&map, "objective", "goal").map_err(D::Error::custom)?;
        let blocked_reason: Option<GoalBlockReason> = if phase == GoalPhase::Blocked {
            Some(field(&map, "blockedReason", "goal").map_err(D::Error::custom)?)
        } else {
            None
        };
        let max_goal_rounds = field(&map, "maxGoalRounds", "goal").map_err(D::Error::custom)?;
        let snapshot = Self {
            id,
            revision,
            objective,
            phase,
            blocked_reason,
            max_goal_rounds,
        };
        snapshot.validate().map_err(D::Error::custom)?;
        Ok(snapshot)
    }
}

/// Stable Goal identity at one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalRef {
    pub id: GoalId,
    pub revision: u64,
}

impl GoalRef {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        if self.id.as_str().is_empty() {
            return Err(PlanningError::invalid("goal.ref.id", "must be non-empty"));
        }
        validate_positive(self.revision, "goal.ref.revision")?;
        Ok(())
    }
}

/// Current Goal plus the counters needed to admit continuation rounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalProjection {
    pub goal: GoalSnapshot,
    pub rounds_started: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl GoalProjection {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        self.goal.validate()?;
        validate_safe(self.rounds_started, "goal.roundsStarted")?;
        validate_safe(self.created_at, "goal.createdAt")?;
        validate_safe(self.updated_at, "goal.updatedAt")?;
        if self.updated_at < self.created_at {
            return Err(PlanningError::invalid(
                "goal.updatedAt",
                "cannot precede goal.createdAt",
            ));
        }
        if self.rounds_started > self.goal.max_goal_rounds {
            return Err(PlanningError::invalid(
                "goal.roundsStarted",
                "cannot exceed goal.maxGoalRounds",
            ));
        }
        Ok(())
    }
}

/// Non-clear Goal mutation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalSnapshotOperation {
    Create,
    Edit,
    Pause,
    Resume,
    Complete,
    Block,
}

impl GoalSnapshotOperation {
    /// Stable durable operation spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Edit => "edit",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Complete => "complete",
            Self::Block => "block",
        }
    }
}

/// Complete Goal operation vocabulary, including the clear tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalOperation {
    Create,
    Edit,
    Pause,
    Resume,
    Complete,
    Block,
    Clear,
}

/// Fields shared by all whole-snapshot Goal mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalSnapshotChange {
    pub operation: GoalSnapshotOperation,
    pub goal: GoalSnapshot,
    pub rounds_started: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Revisioned clear tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalClearChange {
    pub cleared: GoalRef,
    pub cleared_at: u64,
}

/// Strict version-1 `goal/change` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalChange {
    Snapshot(GoalSnapshotChange),
    Clear(GoalClearChange),
}

impl GoalChange {
    pub(crate) const VERSION: u8 = 1;

    /// Operation represented by this whole-value mutation.
    pub const fn operation(&self) -> GoalOperation {
        match self {
            Self::Snapshot(change) => match change.operation {
                GoalSnapshotOperation::Create => GoalOperation::Create,
                GoalSnapshotOperation::Edit => GoalOperation::Edit,
                GoalSnapshotOperation::Pause => GoalOperation::Pause,
                GoalSnapshotOperation::Resume => GoalOperation::Resume,
                GoalSnapshotOperation::Complete => GoalOperation::Complete,
                GoalSnapshotOperation::Block => GoalOperation::Block,
            },
            Self::Clear(_) => GoalOperation::Clear,
        }
    }

    /// Identity established by this mutation.
    pub fn change_ref(&self) -> GoalRef {
        match self {
            Self::Snapshot(change) => GoalRef {
                id: change.goal.id.clone(),
                revision: change.goal.revision,
            },
            Self::Clear(change) => change.cleared.clone(),
        }
    }

    pub(crate) fn validate(&self) -> PlanningResult<()> {
        match self {
            Self::Snapshot(change) => {
                change.goal.validate()?;
                validate_safe(change.rounds_started, "goal.roundsStarted")?;
                validate_safe(change.created_at, "goal.createdAt")?;
                validate_safe(change.updated_at, "goal.updatedAt")?;
                if change.updated_at < change.created_at {
                    return Err(PlanningError::invalid(
                        "goal.updatedAt",
                        "cannot precede goal.createdAt",
                    ));
                }
                if change.rounds_started > change.goal.max_goal_rounds {
                    return Err(PlanningError::invalid(
                        "goal.roundsStarted",
                        "cannot exceed goal.maxGoalRounds",
                    ));
                }
                Ok(())
            }
            Self::Clear(change) => {
                change.cleared.validate()?;
                validate_safe(change.cleared_at, "goal.clearedAt")?;
                Ok(())
            }
        }
    }

    /// Encode this mutation as the durable `goal/change` payload.
    /// @returns The validated Session event payload.
    pub fn to_payload(&self) -> PlanningResult<Value> {
        self.to_value()
    }

    pub(crate) fn to_value(&self) -> PlanningResult<Value> {
        self.validate()?;
        let value = match self {
            Self::Snapshot(change) => {
                let mut map = Map::new();
                map.insert("kind".into(), Value::String("goal/change".into()));
                map.insert("version".into(), Value::from(Self::VERSION));
                map.insert(
                    "operation".into(),
                    Value::String(change.operation.as_str().into()),
                );
                map.insert(
                    "goal".into(),
                    serde_json::to_value(&change.goal).map_err(crate::PlanningError::json)?,
                );
                map.insert("roundsStarted".into(), Value::from(change.rounds_started));
                map.insert("createdAt".into(), Value::from(change.created_at));
                map.insert("updatedAt".into(), Value::from(change.updated_at));
                Value::Object(map)
            }
            Self::Clear(change) => {
                let mut map = Map::new();
                map.insert("kind".into(), Value::String("goal/change".into()));
                map.insert("version".into(), Value::from(Self::VERSION));
                map.insert("operation".into(), Value::String("clear".into()));
                map.insert(
                    "cleared".into(),
                    serde_json::to_value(&change.cleared).map_err(crate::PlanningError::json)?,
                );
                map.insert("clearedAt".into(), Value::from(change.cleared_at));
                Value::Object(map)
            }
        };
        Ok(value)
    }
}

impl<'de> Deserialize<'de> for GoalChange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = deserialize_value(deserializer).map_err(D::Error::custom)?;
        Self::from_value(value).map_err(D::Error::custom)
    }
}

impl GoalChange {
    pub(crate) fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "goal/change")?;
        let kind: String = field(&map, "kind", "goal/change")?;
        if kind != "goal/change" {
            return Err(PlanningError::invalid(
                "goal/change.kind",
                "must be goal/change",
            ));
        }
        let version: u8 = field(&map, "version", "goal/change")?;
        if version != Self::VERSION {
            return Err(PlanningError::invalid(
                "goal/change.version",
                format!("unsupported version {version}"),
            ));
        }
        let operation: String = field(&map, "operation", "goal/change")?;
        if operation == "clear" {
            exact_keys(
                &map,
                &["kind", "version", "operation", "cleared", "clearedAt"],
                &[],
                "goal/change",
            )?;
            let change = Self::Clear(GoalClearChange {
                cleared: field(&map, "cleared", "goal/change")?,
                cleared_at: field(&map, "clearedAt", "goal/change")?,
            });
            change.validate()?;
            return Ok(change);
        }
        let operation = match operation.as_str() {
            "create" => GoalSnapshotOperation::Create,
            "edit" => GoalSnapshotOperation::Edit,
            "pause" => GoalSnapshotOperation::Pause,
            "resume" => GoalSnapshotOperation::Resume,
            "complete" => GoalSnapshotOperation::Complete,
            "block" => GoalSnapshotOperation::Block,
            _ => {
                return Err(PlanningError::invalid(
                    "goal/change.operation",
                    "is invalid",
                ))
            }
        };
        exact_keys(
            &map,
            &[
                "kind",
                "version",
                "operation",
                "goal",
                "roundsStarted",
                "createdAt",
                "updatedAt",
            ],
            &[],
            "goal/change",
        )?;
        let change = Self::Snapshot(GoalSnapshotChange {
            operation,
            goal: field(&map, "goal", "goal/change")?,
            rounds_started: field(&map, "roundsStarted", "goal/change")?,
            created_at: field(&map, "createdAt", "goal/change")?,
            updated_at: field(&map, "updatedAt", "goal/change")?,
        });
        change.validate()?;
        Ok(change)
    }
}

/// Goal attribution read from a `user/message` source object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalMessageSource {
    pub goal_id: GoalId,
    pub revision: u64,
    pub round: u64,
}

impl GoalMessageSource {
    pub(crate) fn from_source(value: &Value) -> PlanningResult<Option<Self>> {
        let map = value
            .as_object()
            .ok_or_else(|| PlanningError::invalid("user/message.source", "must be an object"))?;
        let kind = map.get("kind").and_then(Value::as_str).ok_or_else(|| {
            PlanningError::invalid("user/message.source.kind", "must be a string")
        })?;
        if kind != "goal" {
            return Ok(None);
        }
        let id = GoalId::new(string_field(map, "goalId", "user/message.source")?)?;
        let revision = field(map, "revision", "user/message.source")?;
        let round = field(map, "round", "user/message.source")?;
        validate_positive(revision, "user/message.source.revision")?;
        validate_positive(round, "user/message.source.round")?;
        Ok(Some(Self {
            goal_id: id,
            revision,
            round,
        }))
    }
}

/// Strict durable Goal projection, including lifetime id-reuse protection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalState {
    pub current: Option<GoalProjection>,
    pub seen_goal_ids: BTreeSet<GoalId>,
    pub last_ref: Option<GoalRef>,
}

impl GoalState {
    /// The current Goal, if one exists beyond a clear tombstone.
    /// @returns The current projection.
    #[must_use]
    pub fn projection(&self) -> Option<&GoalProjection> {
        self.current.as_ref()
    }

    pub(crate) fn validate(&self) -> PlanningResult<()> {
        for id in &self.seen_goal_ids {
            if id.as_str().is_empty() {
                return Err(PlanningError::invalid(
                    "goal.seenGoalIds",
                    "contains an empty id",
                ));
            }
        }
        if let Some(current) = &self.current {
            current.validate()?;
            if !self.seen_goal_ids.contains(&current.goal.id) {
                return Err(PlanningError::invalid(
                    "goal.seenGoalIds",
                    "must contain the current Goal id",
                ));
            }
        }
        if let Some(reference) = &self.last_ref {
            reference.validate()?;
            if !self.seen_goal_ids.contains(&reference.id) {
                return Err(PlanningError::invalid(
                    "goal.lastRef",
                    "must name a retained Goal id",
                ));
            }
            if let Some(current) = &self.current {
                if current.goal.id != reference.id || current.goal.revision != reference.revision {
                    return Err(PlanningError::invalid(
                        "goal.lastRef",
                        "must match the current Goal revision",
                    ));
                }
            }
        } else if self.current.is_some() {
            return Err(PlanningError::invalid(
                "goal.lastRef",
                "is required for a current Goal",
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(&mut self, change: &GoalChange) -> PlanningResult<()> {
        change.validate()?;
        match change {
            GoalChange::Clear(clear) => {
                let current = self.current.as_ref().ok_or_else(|| {
                    PlanningError::invalid("goal.clear", "requires a current Goal")
                })?;
                require_next_revision(
                    &current.goal,
                    &clear.cleared.id,
                    clear.cleared.revision,
                    "clear",
                )?;
                if clear.cleared_at < current.updated_at {
                    return Err(PlanningError::invalid(
                        "goal.clearedAt",
                        "cannot precede the current Goal update",
                    ));
                }
                self.current = None;
                self.last_ref = Some(clear.cleared.clone());
            }
            GoalChange::Snapshot(change) => {
                if change.operation == GoalSnapshotOperation::Create {
                    if change.goal.revision != 1
                        || change.goal.phase != GoalPhase::Active
                        || change.rounds_started != 0
                        || self
                            .current
                            .as_ref()
                            .is_some_and(|goal| goal.goal.phase != GoalPhase::Complete)
                        || self.seen_goal_ids.contains(&change.goal.id)
                    {
                        return Err(PlanningError::invalid(
                            "goal.create",
                            "requires a fresh active revision-one Goal with zero rounds",
                        ));
                    }
                } else {
                    let current = self.current.as_ref().ok_or_else(|| {
                        PlanningError::invalid("goal.mutation", "requires a current Goal")
                    })?;
                    validate_transition(current, change)?;
                }
                if change.operation == GoalSnapshotOperation::Create {
                    self.seen_goal_ids.insert(change.goal.id.clone());
                }
                self.current = Some(GoalProjection {
                    goal: change.goal.clone(),
                    rounds_started: change.rounds_started,
                    created_at: change.created_at,
                    updated_at: change.updated_at,
                });
                self.last_ref = Some(GoalRef {
                    id: change.goal.id.clone(),
                    revision: change.goal.revision,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn admit_round(&mut self, source: &GoalMessageSource) -> PlanningResult<()> {
        let current = self.current.as_ref().ok_or_else(|| {
            PlanningError::invalid("goal.round", "requires an active current Goal")
        })?;
        if current.goal.phase != GoalPhase::Active
            || source.goal_id != current.goal.id
            || source.revision != current.goal.revision
            || source.round != current.rounds_started + 1
            || source.round > current.goal.max_goal_rounds
        {
            return Err(PlanningError::invalid(
                "goal.round",
                "is not the next admitted round of the active Goal",
            ));
        }
        if let Some(current) = &mut self.current {
            current.rounds_started = source.round;
        }
        Ok(())
    }
}

fn require_next_revision(
    current: &GoalSnapshot,
    next_id: &GoalId,
    next_revision: u64,
    operation: &str,
) -> PlanningResult<()> {
    let expected = current.revision.checked_add(1).ok_or_else(|| {
        PlanningError::invalid(
            "goal.revision",
            "cannot advance beyond the safe-integer range",
        )
    })?;
    if next_id != &current.id || next_revision != expected {
        return Err(PlanningError::invalid(
            "goal.revision",
            format!("{operation} must advance the current Goal by exactly one revision"),
        ));
    }
    Ok(())
}

fn validate_transition(
    current: &GoalProjection,
    change: &GoalSnapshotChange,
) -> PlanningResult<()> {
    let next = &change.goal;
    require_next_revision(
        &current.goal,
        &next.id,
        next.revision,
        change.operation.as_str(),
    )?;
    if change.created_at != current.created_at
        || change.updated_at < current.updated_at
        || change.rounds_started != current.rounds_started
    {
        return Err(PlanningError::invalid(
            "goal.mutation",
            "does not preserve current counters and timestamps",
        ));
    }
    let definition_same = next.objective == current.goal.objective
        && next.max_goal_rounds == current.goal.max_goal_rounds;
    match change.operation {
        GoalSnapshotOperation::Edit => {
            if next.phase != current.goal.phase
                || next.blocked_reason != current.goal.blocked_reason
            {
                return Err(PlanningError::invalid(
                    "goal.edit",
                    "cannot change phase or blocker",
                ));
            }
        }
        GoalSnapshotOperation::Pause => {
            if !definition_same
                || current.goal.phase != GoalPhase::Active
                || next.phase != GoalPhase::Paused
            {
                return Err(PlanningError::invalid(
                    "goal.pause",
                    "has an invalid phase transition",
                ));
            }
        }
        GoalSnapshotOperation::Resume => {
            if !definition_same
                || !matches!(
                    current.goal.phase,
                    GoalPhase::Active | GoalPhase::Paused | GoalPhase::Blocked
                )
                || next.phase != GoalPhase::Active
                || current.rounds_started >= next.max_goal_rounds
            {
                return Err(PlanningError::invalid(
                    "goal.resume",
                    "has an invalid phase transition or exhausted round budget",
                ));
            }
        }
        GoalSnapshotOperation::Complete => {
            if !definition_same
                || current.goal.phase == GoalPhase::Complete
                || next.phase != GoalPhase::Complete
            {
                return Err(PlanningError::invalid(
                    "goal.complete",
                    "has an invalid phase transition",
                ));
            }
        }
        GoalSnapshotOperation::Block => {
            if !definition_same
                || current.goal.phase != GoalPhase::Active
                || next.phase != GoalPhase::Blocked
            {
                return Err(PlanningError::invalid(
                    "goal.block",
                    "has an invalid phase transition",
                ));
            }
        }
        GoalSnapshotOperation::Create => {
            return Err(PlanningError::invalid(
                "goal.create",
                "cannot mutate a current Goal",
            ));
        }
    }
    Ok(())
}
