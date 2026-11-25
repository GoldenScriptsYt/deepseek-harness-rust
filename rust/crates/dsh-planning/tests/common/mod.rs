#![allow(dead_code)]

use dsh_planning::{
    CommandDone, CommandDoneKind, CommandId, CommandRun, GoalBlockReason, GoalChange,
    GoalClearChange, GoalId, GoalPhase, GoalSnapshot, GoalSnapshotChange, GoalSnapshotOperation,
    PlanningEvent, PlanningPayload, PlanningState, ReplayOutcome, ScheduleEntry, ScheduleId,
    SessionId,
};
use dsh_session_format::Event as SessionEvent;
use serde_json::{Map, Value};

pub fn raw_event(kind: &str, data: Value, seq: u64, time: u64) -> SessionEvent {
    let mut extra = Map::new();
    extra.insert("seq".into(), Value::from(seq));
    extra.insert("time".into(), Value::from(time));
    SessionEvent {
        kind: kind.into(),
        data,
        extra,
    }
}

pub fn event(payload: impl Into<PlanningPayload>, seq: u64, time: u64) -> SessionEvent {
    PlanningEvent::new(payload.into(), seq, time)
        .expect("valid typed event")
        .to_session_event()
        .expect("encodable typed event")
}

pub fn apply(
    state: &mut PlanningState,
    payload: impl Into<PlanningPayload>,
    seq: u64,
    time: u64,
) -> ReplayOutcome {
    state
        .replay(&event(payload, seq, time))
        .expect("valid replay")
}

pub fn goal(
    id: &str,
    revision: u64,
    phase: GoalPhase,
    objective: &str,
    max_rounds: u64,
) -> GoalSnapshot {
    let blocked_reason = (phase == GoalPhase::Blocked).then(|| GoalBlockReason {
        code: "needs-input".into(),
        message: "Input is required.".into(),
    });
    GoalSnapshot {
        id: GoalId::new(id).unwrap(),
        revision,
        objective: objective.into(),
        phase,
        blocked_reason,
        max_goal_rounds: max_rounds,
    }
}

pub fn snapshot_change(
    operation: GoalSnapshotOperation,
    goal: GoalSnapshot,
    rounds_started: u64,
    created_at: u64,
    updated_at: u64,
) -> GoalChange {
    GoalChange::Snapshot(GoalSnapshotChange {
        operation,
        goal,
        rounds_started,
        created_at,
        updated_at,
    })
}

pub fn create_goal(id: &str, max_rounds: u64, created_at: u64, updated_at: u64) -> GoalChange {
    snapshot_change(
        GoalSnapshotOperation::Create,
        goal(id, 1, GoalPhase::Active, "Ship the feature", max_rounds),
        0,
        created_at,
        updated_at,
    )
}

pub fn clear_goal(id: &str, revision: u64, cleared_at: u64) -> GoalChange {
    GoalChange::Clear(GoalClearChange {
        cleared: dsh_planning::GoalRef {
            id: GoalId::new(id).unwrap(),
            revision,
        },
        cleared_at,
    })
}

pub fn command_run(id: &str, name: &str, args: Option<&str>) -> PlanningPayload {
    PlanningPayload::CommandRun(CommandRun {
        command_id: CommandId::new(id),
        name: name.into(),
        args: args.map(str::to_owned),
    })
}

pub fn command_done(
    id: &str,
    kind: CommandDoneKind,
    text: Option<&str>,
    source_event_seq: Option<u64>,
) -> PlanningPayload {
    PlanningPayload::CommandDone(CommandDone {
        command_id: CommandId::new(id),
        kind,
        text: text.map(str::to_owned),
        source_event_seq,
    })
}

pub fn at_entry(id: &str, status: dsh_planning::ScheduleStatus) -> ScheduleEntry {
    ScheduleEntry {
        record: dsh_planning::ScheduleRecord::At(dsh_planning::AtScheduleRecord {
            id: ScheduleId::new(id).unwrap(),
            kind: dsh_planning::ScheduleKind::At,
            title: "Check in".into(),
            prompt: "Report progress.".into(),
            scheduled_at: "2026-11-10T09:00:00.000Z".into(),
        }),
        target: dsh_planning::ScheduleTarget::Session {
            session_id: SessionId::new("session-1").unwrap(),
        },
        involved_session_ids: vec![SessionId::new("session-1").unwrap()],
        status,
        created_at: Some("2026-11-01T00:00:00.000Z".into()),
        updated_at: Some("2026-11-01T00:00:00.000Z".into()),
        last_delivery: None,
    }
}

pub fn put(entry: ScheduleEntry) -> PlanningPayload {
    PlanningPayload::ScheduleEntryChange(dsh_planning::ScheduleChange::Put(entry))
}
