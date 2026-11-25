//! Deterministic durable state for DSH goals, plan mode, todos, and schedules.
//!
//! The aggregate projection consumes the generic Session event envelope from
//! `dsh-session-format`. It performs no I/O and starts no timers, deliveries, or
//! Agent work. Snapshots contain only validated data and a replay watermark.
//!
//! Authoritative TypeScript events are `goal/change`, goal-attributed
//! `user/message`, `command/run`, `command/done`, `plan/mode`, `todo/write`, and
//! `turn/start`. Historical `schedule/change` events remain inert. Because the
//! current TypeScript Schedule store is Host-owned rather than a Session
//! projection, this crate defines the explicitly Rust-local
//! `schedule/entry/change` whole-value event for its bounded entry collection.

mod error;
mod event;
mod goal;
mod goal_mutation;
mod goal_timeline;
mod ids;
mod plan;
mod schedule;
mod state;
mod todo;
mod util;

pub use error::{PlanningError, PlanningResult, ReplayOutcome};
pub use event::{
    PlanningEvent, PlanningEventKind, PlanningPayload, TurnStart, MAX_SAFE_SESSION_INTEGER,
};
pub use goal::{
    GoalBlockReason, GoalChange, GoalClearChange, GoalMessageSource, GoalOperation, GoalPhase,
    GoalProjection, GoalRef, GoalSnapshot, GoalSnapshotChange, GoalSnapshotOperation, GoalState,
};
pub use goal_mutation::{
    apply_to, block_goal, clear_goal, complete_goal, create_goal, edit_goal, pause_goal,
    phase_name, random_goal_id, resume_goal, GoalRefusal, DEFAULT_MAX_GOAL_ROUNDS,
};
pub use goal_timeline::GoalTimeline;
pub use ids::{CommandId, GoalId, MessageId, ScheduleId, SessionId};
pub use plan::{
    fold_plan, CommandDone, CommandDoneKind, CommandRun, PlanMode, PlanProjection, PlanState,
    RunningPlanCommand,
};
pub use schedule::{
    AfterScheduleRecord, AtScheduleRecord, CronScheduleRecord, DailyScheduleRecord,
    DeliveryReceipt, EveryScheduleRecord, RRuleScheduleRecord, ScheduleCatalogEntry,
    ScheduleChange, ScheduleCreateSpec, ScheduleEntry, ScheduleEntryChange, ScheduleKind,
    ScheduleRecord, ScheduleState, ScheduleStatus, ScheduleTarget, WeeklyScheduleRecord,
};
pub use state::{PlanningLimits, PlanningSnapshot, PlanningState, ReplayCursor, SNAPSHOT_VERSION};
pub use todo::{TodoItem, TodoState, TodoStatus, TodoWrite};
