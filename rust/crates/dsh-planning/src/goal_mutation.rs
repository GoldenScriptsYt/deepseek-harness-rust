//! Validated Goal mutations shared by the Host RPCs and the model-facing tools.
//!
//! Each builder reads the current projection, enforces the lifecycle rules, and
//! returns the `goal/change` payload to append. The durable fold re-checks the
//! same invariants, so a builder bug surfaces as a rejected replay rather than a
//! corrupt projection.

use crate::error::PlanningError;
use crate::goal::{
    GoalBlockReason, GoalChange, GoalClearChange, GoalPhase, GoalProjection, GoalRef, GoalSnapshot,
    GoalSnapshotChange, GoalSnapshotOperation,
};
use crate::ids::GoalId;
use crate::MAX_SAFE_SESSION_INTEGER;

/// Round cap a create request inherits when the caller names none.
pub const DEFAULT_MAX_GOAL_ROUNDS: u64 = 256;

/// A refused Goal mutation carrying the stable code the client and the model read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalRefusal {
    /// Stable failure code, such as `GOAL_STALE_REVISION`.
    pub code: &'static str,
    /// Actionable diagnostic.
    pub message: String,
}

impl GoalRefusal {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for GoalRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

/// Stable durable spelling of one phase.
/// @param phase - Goal phase.
/// @returns The durable lowercase name.
#[must_use]
pub fn phase_name(phase: GoalPhase) -> &'static str {
    match phase {
        GoalPhase::Active => "active",
        GoalPhase::Paused => "paused",
        GoalPhase::Blocked => "blocked",
        GoalPhase::Complete => "complete",
    }
}

/// Validate and normalize one objective.
fn resolve_objective(objective: &str) -> Result<String, GoalRefusal> {
    let trimmed = objective.trim();
    if trimmed.is_empty() {
        return Err(GoalRefusal::new(
            "GOAL_INVALID_OBJECTIVE",
            "goal objective must be a non-empty string",
        ));
    }
    Ok(trimmed.to_string())
}

/// Validate one round cap against the durable safe-integer range.
fn resolve_max_rounds(rounds: u64) -> Result<u64, GoalRefusal> {
    if !(1..=MAX_SAFE_SESSION_INTEGER).contains(&rounds) {
        return Err(GoalRefusal::new(
            "GOAL_INVALID_MAX_ROUNDS",
            "maxGoalRounds must be a positive safe integer",
        ));
    }
    Ok(rounds)
}

/// Reject stale or absent current-state references.
fn expect_current<'a>(
    current: Option<&'a GoalProjection>,
    reference: &GoalRef,
) -> Result<&'a GoalProjection, GoalRefusal> {
    let current = current.ok_or_else(|| GoalRefusal::new("GOAL_NOT_FOUND", "no current goal"))?;
    if reference.id != current.goal.id || reference.revision != current.goal.revision {
        return Err(GoalRefusal::new(
            "GOAL_STALE_REVISION",
            format!(
                "stale goal ref {:?} revision {}; current is {:?} revision {}",
                reference.id.as_str(),
                reference.revision,
                current.goal.id.as_str(),
                current.goal.revision
            ),
        ));
    }
    Ok(current)
}

/// Render the stable invalid-transition refusal.
fn transition_refusal(
    current: &GoalSnapshot,
    operation: &str,
    allowed: &[GoalPhase],
) -> GoalRefusal {
    let allowed: Vec<&str> = allowed.iter().map(|phase| phase_name(*phase)).collect();
    GoalRefusal::new(
        "GOAL_INVALID_TRANSITION",
        format!(
            "cannot {operation} goal {:?} from phase {:?}; expected {}",
            current.id.as_str(),
            phase_name(current.phase),
            allowed.join(" or ")
        ),
    )
}

/// Mutation time clamped across backward wall-clock movement.
fn mutation_time(current: Option<&GoalProjection>, now: u64) -> u64 {
    current.map_or(now, |current| now.max(current.updated_at))
}

/// Build a create change over an absent or completed Goal.
///
/// @param current - current projection, if any.
/// @param id - fresh Goal identity.
/// @param objective - requested objective.
/// @param max_goal_rounds - requested cap, or `None` for the deployment default.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn create_goal(
    current: Option<&GoalProjection>,
    id: GoalId,
    objective: &str,
    max_goal_rounds: Option<u64>,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    let objective = resolve_objective(objective)?;
    let max_goal_rounds = match max_goal_rounds {
        Some(rounds) => resolve_max_rounds(rounds)?,
        None => DEFAULT_MAX_GOAL_ROUNDS,
    };
    if let Some(current) = current {
        if current.goal.phase != GoalPhase::Complete {
            return Err(GoalRefusal::new(
                "GOAL_ALREADY_EXISTS",
                format!(
                    "goal {:?} already exists with phase {:?}",
                    current.goal.id.as_str(),
                    phase_name(current.goal.phase)
                ),
            ));
        }
    }
    let now = mutation_time(current, now);
    Ok(GoalChange::Snapshot(GoalSnapshotChange {
        operation: GoalSnapshotOperation::Create,
        goal: GoalSnapshot {
            id,
            revision: 1,
            objective,
            phase: GoalPhase::Active,
            blocked_reason: None,
            max_goal_rounds,
        },
        rounds_started: 0,
        created_at: now,
        updated_at: now,
    }))
}

/// Build an edit change for the objective and/or the round cap.
///
/// @param current - current projection, if any.
/// @param reference - expected revision.
/// @param objective - replacement objective, if supplied.
/// @param max_goal_rounds - replacement cap, if supplied.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn edit_goal(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    objective: Option<&str>,
    max_goal_rounds: Option<u64>,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    if objective.is_none() && max_goal_rounds.is_none() {
        return Err(GoalRefusal::new(
            "GOAL_INVALID_EDIT",
            "goal edit requires objective and/or maxGoalRounds",
        ));
    }
    let current = expect_current(current, reference)?;
    let mut goal = current.goal.clone();
    goal.revision = current.goal.revision + 1;
    if let Some(objective) = objective {
        goal.objective = resolve_objective(objective)?;
    }
    if let Some(rounds) = max_goal_rounds {
        goal.max_goal_rounds = resolve_max_rounds(rounds)?;
    }
    Ok(GoalChange::Snapshot(GoalSnapshotChange {
        operation: GoalSnapshotOperation::Edit,
        goal,
        rounds_started: current.rounds_started,
        created_at: current.created_at,
        updated_at: mutation_time(Some(current), now),
    }))
}

/// Build a transition change for pause, resume, complete, or block.
fn transition(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    operation: GoalSnapshotOperation,
    allowed: &[GoalPhase],
    phase: GoalPhase,
    blocked_reason: Option<GoalBlockReason>,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    let current = expect_current(current, reference)?;
    if !allowed.contains(&current.goal.phase) {
        return Err(transition_refusal(
            &current.goal,
            operation.as_str(),
            allowed,
        ));
    }
    Ok(GoalChange::Snapshot(GoalSnapshotChange {
        operation,
        goal: GoalSnapshot {
            id: current.goal.id.clone(),
            revision: current.goal.revision + 1,
            objective: current.goal.objective.clone(),
            phase,
            blocked_reason,
            max_goal_rounds: current.goal.max_goal_rounds,
        },
        rounds_started: current.rounds_started,
        created_at: current.created_at,
        updated_at: mutation_time(Some(current), now),
    }))
}

/// Build a pause change.
///
/// @param current - current projection, if any.
/// @param reference - expected revision.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn pause_goal(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    transition(
        current,
        reference,
        GoalSnapshotOperation::Pause,
        &[GoalPhase::Active],
        GoalPhase::Paused,
        None,
        now,
    )
}

/// Build a resume change, refusing a paused Goal the model may not rearm and an
/// exhausted round budget.
///
/// @param current - current projection, if any.
/// @param reference - expected revision.
/// @param armed - whether automatic continuation is already armed for this revision.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn resume_goal(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    armed: bool,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    let current_projection = expect_current(current, reference)?;
    if current_projection.goal.phase == GoalPhase::Active && armed {
        return Err(GoalRefusal::new(
            "GOAL_INVALID_TRANSITION",
            format!(
                "goal {:?} is already active and armed",
                current_projection.goal.id.as_str()
            ),
        ));
    }
    if current_projection.rounds_started >= current_projection.goal.max_goal_rounds {
        return Err(GoalRefusal::new(
            "GOAL_INVALID_TRANSITION",
            format!(
                "goal {:?} exhausted {} goal rounds; increase maxGoalRounds before resuming",
                current_projection.goal.id.as_str(),
                current_projection.goal.max_goal_rounds
            ),
        ));
    }
    transition(
        current,
        reference,
        GoalSnapshotOperation::Resume,
        &[GoalPhase::Active, GoalPhase::Paused, GoalPhase::Blocked],
        GoalPhase::Active,
        None,
        now,
    )
}

/// Build a complete change.
///
/// @param current - current projection, if any.
/// @param reference - expected revision.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn complete_goal(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    transition(
        current,
        reference,
        GoalSnapshotOperation::Complete,
        &[GoalPhase::Active, GoalPhase::Paused, GoalPhase::Blocked],
        GoalPhase::Complete,
        None,
        now,
    )
}

/// Build a block change.
///
/// @param current - current projection, if any.
/// @param reference - expected revision.
/// @param reason - reported blocking condition.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn block_goal(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    reason: GoalBlockReason,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    if reason.message.trim().is_empty() {
        return Err(GoalRefusal::new(
            "GOAL_INVALID_BLOCK_REASON",
            "blocked_reason must be a non-empty string",
        ));
    }
    transition(
        current,
        reference,
        GoalSnapshotOperation::Block,
        &[GoalPhase::Active],
        GoalPhase::Blocked,
        Some(reason),
        now,
    )
}

/// Build a clear change over the current Goal.
///
/// @param current - current projection, if any.
/// @param reference - expected revision.
/// @param now - commit time in milliseconds.
/// @returns The change to append, or the refusal.
pub fn clear_goal(
    current: Option<&GoalProjection>,
    reference: &GoalRef,
    now: u64,
) -> Result<GoalChange, GoalRefusal> {
    let current = expect_current(current, reference)?;
    Ok(GoalChange::Clear(GoalClearChange {
        cleared: GoalRef {
            id: current.goal.id.clone(),
            revision: current.goal.revision + 1,
        },
        cleared_at: mutation_time(Some(current), now),
    }))
}

/// Validate one built change by applying it to the current projection.
///
/// @param current - current projection, if any.
/// @param change - change to check.
/// @returns The projection the change produces, or the durable rejection.
pub fn apply_to(
    current: Option<&GoalProjection>,
    change: &GoalChange,
) -> Result<Option<GoalProjection>, PlanningError> {
    let mut state = crate::goal::GoalState {
        current: current.cloned(),
        ..crate::goal::GoalState::default()
    };
    if let Some(current) = current {
        state.seen_goal_ids.insert(current.goal.id.clone());
    }
    state.apply(change)?;
    Ok(state.current.clone())
}

/// Generate a deployment Goal identity.
/// @returns A fresh `goal-<hex>` identity.
#[must_use]
pub fn random_goal_id() -> GoalId {
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let span = format!("{nanos:032x}{counter:016x}");
    GoalId::new(format!(
        "goal-{}-{}-{}-{}-{}",
        &span[0..8],
        &span[8..12],
        &span[12..16],
        &span[16..20],
        &span[20..32]
    ))
    .expect("generated Goal identities are well formed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn active() -> GoalProjection {
        let change = create_goal(None, GoalId::new("goal-1").unwrap(), "ship it", Some(4), 10)
            .expect("a fresh goal");
        apply_to(None, &change).expect("applies").expect("a goal")
    }

    fn reference(projection: &GoalProjection) -> GoalRef {
        GoalRef {
            id: projection.goal.id.clone(),
            revision: projection.goal.revision,
        }
    }

    #[test]
    fn create_refuses_an_unfinished_goal_and_accepts_a_completed_one() {
        let projection = active();
        let refusal = create_goal(
            Some(&projection),
            GoalId::new("goal-2").unwrap(),
            "again",
            None,
            20,
        )
        .unwrap_err();
        assert_eq!(refusal.code, "GOAL_ALREADY_EXISTS");
        let completed = complete_goal(Some(&projection), &reference(&projection), 20).unwrap();
        let completed = apply_to(Some(&projection), &completed).unwrap().unwrap();
        let change = create_goal(
            Some(&completed),
            GoalId::new("goal-2").unwrap(),
            "again",
            None,
            30,
        )
        .unwrap();
        let next = apply_to(Some(&completed), &change).unwrap().unwrap();
        assert_eq!(next.goal.id.as_str(), "goal-2");
        assert_eq!(next.goal.max_goal_rounds, DEFAULT_MAX_GOAL_ROUNDS);
    }

    #[test]
    fn stale_references_and_missing_goals_are_named() {
        let projection = active();
        let stale = GoalRef {
            id: projection.goal.id.clone(),
            revision: 9,
        };
        assert_eq!(
            pause_goal(Some(&projection), &stale, 20).unwrap_err().code,
            "GOAL_STALE_REVISION"
        );
        assert_eq!(
            pause_goal(None, &reference(&projection), 20)
                .unwrap_err()
                .code,
            "GOAL_NOT_FOUND"
        );
    }

    #[test]
    fn pause_resume_and_complete_follow_the_lifecycle() {
        let projection = active();
        let paused = pause_goal(Some(&projection), &reference(&projection), 20).unwrap();
        let paused_projection = apply_to(Some(&projection), &paused).unwrap().unwrap();
        assert_eq!(phase_name(paused_projection.goal.phase), "paused");
        assert_eq!(
            pause_goal(Some(&paused_projection), &reference(&paused_projection), 30)
                .unwrap_err()
                .code,
            "GOAL_INVALID_TRANSITION"
        );
        let refused = resume_goal(
            Some(&paused_projection),
            &reference(&paused_projection),
            false,
            30,
        )
        .unwrap();
        let resumed = apply_to(Some(&paused_projection), &refused)
            .unwrap()
            .unwrap();
        assert_eq!(phase_name(resumed.goal.phase), "active");
        assert_eq!(
            resume_goal(Some(&resumed), &reference(&resumed), true, 40)
                .unwrap_err()
                .code,
            "GOAL_INVALID_TRANSITION"
        );
        let completed = complete_goal(Some(&resumed), &reference(&resumed), 40).unwrap();
        let completed = apply_to(Some(&resumed), &completed).unwrap().unwrap();
        assert_eq!(phase_name(completed.goal.phase), "complete");
        assert_eq!(
            complete_goal(Some(&completed), &reference(&completed), 50)
                .unwrap_err()
                .code,
            "GOAL_INVALID_TRANSITION"
        );
    }

    #[test]
    fn resume_refuses_an_exhausted_round_budget() {
        let projection = active();
        let mut exhausted = projection.clone();
        exhausted.rounds_started = projection.goal.max_goal_rounds;
        let paused = pause_goal(Some(&projection), &reference(&projection), 20).unwrap();
        let mut paused = apply_to(Some(&projection), &paused).unwrap().unwrap();
        paused.rounds_started = exhausted.rounds_started;
        let refusal = resume_goal(Some(&paused), &reference(&paused), false, 30);
        assert!(refusal.is_err());
    }

    #[test]
    fn edits_require_a_field_and_preserve_creation() {
        let projection = active();
        assert_eq!(
            edit_goal(Some(&projection), &reference(&projection), None, None, 20)
                .unwrap_err()
                .code,
            "GOAL_INVALID_EDIT"
        );
        let change = edit_goal(
            Some(&projection),
            &reference(&projection),
            Some(" ship it well "),
            Some(6),
            20,
        )
        .unwrap();
        let edited = apply_to(Some(&projection), &change).unwrap().unwrap();
        assert_eq!(edited.goal.objective, "ship it well");
        assert_eq!(edited.goal.revision, 2);
        assert_eq!(edited.created_at, projection.created_at);
    }

    #[test]
    fn clear_retains_a_tombstone_and_block_records_its_reason() {
        let projection = active();
        let blocked = block_goal(
            Some(&projection),
            &reference(&projection),
            GoalBlockReason {
                code: "model-reported".into(),
                message: "the gateway is down".into(),
            },
            20,
        )
        .unwrap();
        let blocked = apply_to(Some(&projection), &blocked).unwrap().unwrap();
        assert_eq!(phase_name(blocked.goal.phase), "blocked");
        let cleared = clear_goal(Some(&blocked), &reference(&blocked), 30).unwrap();
        match &cleared {
            GoalChange::Clear(clear) => {
                assert_eq!(clear.cleared.revision, blocked.goal.revision + 1);
                assert_eq!(clear.cleared_at, 30);
            }
            other => panic!("expected a clear tombstone, got {other:?}"),
        }
        assert!(apply_to(Some(&blocked), &cleared).unwrap().is_none());
    }

    #[test]
    fn empty_objectives_and_round_caps_are_refused_with_their_codes() {
        assert_eq!(
            create_goal(None, GoalId::new("goal-1").unwrap(), "   ", None, 10)
                .unwrap_err()
                .code,
            "GOAL_INVALID_OBJECTIVE"
        );
        assert_eq!(
            create_goal(None, GoalId::new("goal-1").unwrap(), "x", Some(0), 10)
                .unwrap_err()
                .code,
            "GOAL_INVALID_MAX_ROUNDS"
        );
    }

    #[test]
    fn generated_identities_are_distinct_and_prefixed() {
        let first = random_goal_id();
        let second = random_goal_id();
        assert_ne!(first, second);
        assert!(first.as_str().starts_with("goal-"));
        assert_eq!(first.as_str().len(), 41);
    }

    #[test]
    fn the_built_change_serializes_to_the_wire_payload() {
        let projection = active();
        let change = pause_goal(Some(&projection), &reference(&projection), 20).unwrap();
        let payload = change.to_payload().unwrap();
        assert_eq!(payload["kind"], json!("goal/change"));
        assert_eq!(payload["operation"], json!("pause"));
        assert_eq!(payload["goal"]["phase"], json!("paused"));
    }
}
