use crate::event::{session_metadata, PlanningEvent, PlanningPayload};
use crate::goal::GoalState;
use crate::ids::ScheduleId;
use crate::plan::PlanState;
use crate::schedule::ScheduleChange;
use crate::todo::TodoState;
use crate::util::validate_safe;
use crate::{PlanningError, PlanningResult, ReplayOutcome};
use dsh_session_format::Event as SessionEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Borrow;
use std::collections::BTreeMap;

/// Current durable planning snapshot schema version.
pub const SNAPSHOT_VERSION: u32 = 1;

/// Inclusive Session replay watermark. `None` is the pre-event cursor `-1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReplayCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_time: Option<u64>,
}

impl ReplayCursor {
    fn validate(&self) -> PlanningResult<()> {
        match (self.last_seq, self.last_time) {
            (None, None) => Ok(()),
            (Some(seq), Some(time)) => {
                validate_safe(seq, "planning.cursor.lastSeq")?;
                validate_safe(time, "planning.cursor.lastTime")?;
                Ok(())
            }
            _ => Err(PlanningError::invalid(
                "planning.cursor",
                "lastSeq and lastTime must both be present or both be absent",
            )),
        }
    }
}

/// Serializable durable state. Runtime limits are intentionally not snapshotted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanningSnapshot {
    pub version: u32,
    pub cursor: ReplayCursor,
    pub goal: GoalState,
    pub plan: PlanState,
    pub todo: TodoState,
    pub schedule: crate::schedule::ScheduleState,
}

/// Caller-owned bounds for collections that the durable contracts permit bounding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanningLimits {
    #[serde(default = "default_max_todo_items")]
    pub max_todo_items: usize,
    #[serde(default = "default_max_schedule_entries")]
    pub max_schedule_entries: usize,
    #[serde(default = "default_max_involved_session_ids")]
    pub max_involved_session_ids: usize,
    #[serde(default)]
    pub allow_parallel_todos: bool,
}

fn default_max_todo_items() -> usize {
    128
}

fn default_max_schedule_entries() -> usize {
    1_024
}

fn default_max_involved_session_ids() -> usize {
    1_024
}

impl Default for PlanningLimits {
    fn default() -> Self {
        Self {
            max_todo_items: default_max_todo_items(),
            max_schedule_entries: default_max_schedule_entries(),
            max_involved_session_ids: default_max_involved_session_ids(),
            allow_parallel_todos: false,
        }
    }
}

/// Deterministic aggregate projection over the generic Session event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanningState {
    goal: GoalState,
    plan: PlanState,
    todo: TodoState,
    schedule: crate::schedule::ScheduleState,
    cursor: ReplayCursor,
    limits: PlanningLimits,
}

impl Default for PlanningState {
    fn default() -> Self {
        Self::new(PlanningLimits::default())
    }
}

impl PlanningState {
    /// Create an empty projection with explicit durable collection limits.
    pub fn new(limits: PlanningLimits) -> Self {
        Self {
            goal: GoalState::default(),
            plan: PlanState::default(),
            todo: TodoState::default(),
            schedule: crate::schedule::ScheduleState::default(),
            cursor: ReplayCursor::default(),
            limits,
        }
    }

    /// Restore a validated checkpoint under caller-owned limits.
    pub fn from_snapshot(
        snapshot: PlanningSnapshot,
        limits: PlanningLimits,
    ) -> PlanningResult<Self> {
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(PlanningError::UnsupportedSnapshotVersion {
                version: snapshot.version,
            });
        }
        let state = Self {
            goal: snapshot.goal,
            plan: snapshot.plan,
            todo: snapshot.todo,
            schedule: snapshot.schedule,
            cursor: snapshot.cursor,
            limits,
        };
        state.validate().map_err(PlanningError::into_corrupt)?;
        Ok(state)
    }

    /// Restore a checkpoint and fold its contiguous suffix.
    pub fn recover<I>(
        snapshot: PlanningSnapshot,
        suffix: I,
        limits: PlanningLimits,
    ) -> PlanningResult<Self>
    where
        I: IntoIterator,
        I::Item: Borrow<SessionEvent>,
    {
        let mut state = Self::from_snapshot(snapshot, limits)?;
        state.replay_events(suffix)?;
        Ok(state)
    }

    /// Parse a checkpoint JSON value and validate it completely.
    pub fn from_snapshot_value(value: Value, limits: PlanningLimits) -> PlanningResult<Self> {
        let snapshot = serde_json::from_value(value).map_err(|error| PlanningError::Corrupt {
            field: "snapshot",
            message: error.to_string(),
        })?;
        Self::from_snapshot(snapshot, limits)
    }

    /// Restore from checkpoint JSON.
    pub fn from_json(json: &str, limits: PlanningLimits) -> PlanningResult<Self> {
        let value = serde_json::from_str(json).map_err(|error| PlanningError::Corrupt {
            field: "snapshot",
            message: error.to_string(),
        })?;
        Self::from_snapshot_value(value, limits)
    }

    /// Produce a detached checkpoint.
    pub fn snapshot(&self) -> PlanningSnapshot {
        PlanningSnapshot {
            version: SNAPSHOT_VERSION,
            cursor: self.cursor,
            goal: self.goal.clone(),
            plan: self.plan.clone(),
            todo: self.todo.clone(),
            schedule: self.schedule.clone(),
        }
    }

    /// Encode checkpoint JSON. Valid in-memory state always serializes.
    pub fn to_json(&self) -> PlanningResult<String> {
        serde_json::to_string(&self.snapshot()).map_err(PlanningError::json)
    }

    /// Encode a checkpoint as a JSON value.
    pub fn to_snapshot_value(&self) -> PlanningResult<Value> {
        serde_json::to_value(self.snapshot()).map_err(PlanningError::json)
    }

    /// Borrow the inclusive replay watermark.
    pub const fn cursor(&self) -> ReplayCursor {
        self.cursor
    }

    /// Active collection and admission limits.
    pub const fn limits(&self) -> PlanningLimits {
        self.limits
    }

    /// Reconfigure bounds without losing state, rejecting an already oversized checkpoint.
    pub fn set_limits(&mut self, limits: PlanningLimits) -> PlanningResult<()> {
        let previous = self.limits;
        self.limits = limits;
        if let Err(error) = self.validate() {
            self.limits = previous;
            return Err(error.into_corrupt());
        }
        Ok(())
    }

    /// Borrow durable Goal state, including lifetime ID admission.
    pub const fn goal(&self) -> &GoalState {
        &self.goal
    }

    /// Borrow durable plan-mode state.
    pub const fn plan(&self) -> &PlanState {
        &self.plan
    }

    /// Borrow durable ordered todo state.
    pub const fn todo(&self) -> &TodoState {
        &self.todo
    }

    /// Borrow durable Schedule entries.
    pub const fn schedule(&self) -> &crate::schedule::ScheduleState {
        &self.schedule
    }

    /// Current Goal projection, if one exists.
    pub fn goal_projection(&self) -> Option<&crate::GoalProjection> {
        self.goal.current.as_ref()
    }

    /// Cropped plan-mode view.
    pub fn plan_projection(&self) -> crate::PlanProjection {
        self.plan.projection()
    }

    /// Ordered current-turn todos; `None` means no written list this turn.
    pub fn todos(&self) -> Option<&[crate::TodoItem]> {
        self.todo.items.as_deref()
    }

    /// Stable-id-indexed Schedule entries.
    pub fn schedule_entries(&self) -> &BTreeMap<ScheduleId, crate::ScheduleEntry> {
        &self.schedule.entries
    }

    /// Fold one generic Session event atomically.
    ///
    /// Policy: decode known payloads first; ignore any `seq <= lastSeq`; otherwise
    /// require exactly `lastSeq + 1`; reject decreasing commit time while allowing
    /// equal time. Valid unknown and inert events advance the cursor. A failure
    /// leaves every domain and the cursor unchanged.
    pub fn replay(&mut self, event: &SessionEvent) -> PlanningResult<ReplayOutcome> {
        let (seq, time) = session_metadata(event)?;
        let decoded = PlanningEvent::from_session_event(event)?;
        if self.cursor.last_seq.is_some_and(|last| seq <= last) {
            return Ok(ReplayOutcome::Ignored);
        }
        let expected = match self.cursor.last_seq {
            Some(last) => last
                .checked_add(1)
                .ok_or_else(|| PlanningError::InvalidSequence {
                    actual: seq,
                    last: i128::from(last),
                })?,
            None => 0,
        };
        if seq != expected {
            return Err(PlanningError::InvalidSequence {
                actual: seq,
                last: expected as i128 - 1,
            });
        }
        if let Some(last) = self.cursor.last_time {
            if time < last {
                return Err(PlanningError::InvalidTime { actual: time, last });
            }
        }

        let mut candidate = self.clone();
        let outcome = match decoded {
            Some(event) => candidate.apply_payload(event.into_payload())?,
            None => ReplayOutcome::Unrelated,
        };
        candidate.validate()?;
        candidate.cursor = ReplayCursor {
            last_seq: Some(seq),
            last_time: Some(time),
        };
        *self = candidate;
        Ok(outcome)
    }

    /// Fold an ordered event slice or iterator.
    pub fn replay_events<I>(&mut self, events: I) -> PlanningResult<Vec<ReplayOutcome>>
    where
        I: IntoIterator,
        I::Item: Borrow<SessionEvent>,
    {
        events
            .into_iter()
            .map(|event| self.replay(event.borrow()))
            .collect()
    }

    fn apply_payload(&mut self, payload: PlanningPayload) -> PlanningResult<ReplayOutcome> {
        let relevant = match &payload {
            PlanningPayload::GoalChange(_) => true,
            PlanningPayload::UserMessage(_) => true,
            PlanningPayload::CommandRun(command) => {
                command.name == "plan" && command.args.is_some()
            }
            PlanningPayload::CommandDone(command) => {
                self.plan
                    .running
                    .as_ref()
                    .map(|running| &running.command_id)
                    == Some(&command.command_id)
            }
            PlanningPayload::PlanMode(_) => true,
            PlanningPayload::TodoWrite(_) => true,
            PlanningPayload::TurnStart(_) => true,
            PlanningPayload::ScheduleEntryChange(_) => true,
        };
        match payload {
            PlanningPayload::GoalChange(change) => self.goal.apply(&change)?,
            PlanningPayload::UserMessage(data) => {
                let source =
                    crate::goal::GoalMessageSource::from_source(data.get("source").ok_or_else(
                        || PlanningError::invalid("user/message.source", "is required"),
                    )?)?
                    .ok_or_else(|| {
                        PlanningError::invalid("user/message.source", "must identify a Goal round")
                    })?;
                self.goal.admit_round(&source)?;
            }
            PlanningPayload::CommandRun(command) => self.plan.apply_run(&command),
            PlanningPayload::CommandDone(command) => self.plan.apply_done(&command),
            PlanningPayload::PlanMode(mode) => self.plan.apply_mode(&mode),
            PlanningPayload::TodoWrite(write) => {
                self.todo.write(
                    &write,
                    self.limits.max_todo_items,
                    self.limits.allow_parallel_todos,
                )?;
            }
            PlanningPayload::TurnStart(_) => self.todo.items = None,
            PlanningPayload::ScheduleEntryChange(ScheduleChange::Put(entry)) => {
                self.schedule.apply(
                    &ScheduleChange::Put(entry),
                    self.limits.max_schedule_entries,
                    self.limits.max_involved_session_ids,
                )?;
            }
            PlanningPayload::ScheduleEntryChange(ScheduleChange::Delete(id)) => {
                self.schedule.apply(
                    &ScheduleChange::Delete(id),
                    self.limits.max_schedule_entries,
                    self.limits.max_involved_session_ids,
                )?;
            }
        }
        Ok(if relevant {
            ReplayOutcome::Applied
        } else {
            ReplayOutcome::Unrelated
        })
    }

    fn validate(&self) -> PlanningResult<()> {
        self.cursor.validate()?;
        self.goal.validate()?;
        self.plan.validate()?;
        self.todo
            .validate(self.limits.max_todo_items, self.limits.allow_parallel_todos)?;
        self.schedule.validate(
            self.limits.max_schedule_entries,
            self.limits.max_involved_session_ids,
        )
    }
}
