//! Goal-only fold of a committed Session log.
//!
//! Consumers that need just the current Goal — the Host's projection value and
//! the model-facing goal tools — fold through this type instead of replaying the
//! whole planning aggregate, so an unrelated planning domain cannot fail them.

use crate::error::{PlanningError, PlanningResult, ReplayOutcome};
use crate::event::{PlanningEvent, PlanningPayload};
use crate::goal::{GoalMessageSource, GoalProjection, GoalState};
use dsh_session_format::Event as SessionEvent;

/// Current Goal folded from goal-owned Session events.
#[derive(Debug, Clone, Default)]
pub struct GoalTimeline {
    state: GoalState,
}

impl GoalTimeline {
    /// Build an empty timeline.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one committed Session event; kinds outside the Goal domain are ignored.
    ///
    /// A rejected change leaves the previous Goal in place, matching a reader that
    /// keeps the last value it could validate.
    /// @param event - committed Session row, in log order.
    /// @returns Whether the Goal fold changed.
    pub fn observe(&mut self, event: &SessionEvent) -> PlanningResult<ReplayOutcome> {
        if event.kind != "goal/change" && event.kind != "user/message" {
            return Ok(ReplayOutcome::Unrelated);
        }
        let Some(decoded) = PlanningEvent::from_session_event(event)? else {
            return Ok(ReplayOutcome::Unrelated);
        };
        let mut candidate = self.clone();
        let outcome = match decoded.into_payload() {
            PlanningPayload::GoalChange(change) => {
                candidate.state.apply(&change)?;
                ReplayOutcome::Applied
            }
            PlanningPayload::UserMessage(data) => {
                let source =
                    GoalMessageSource::from_source(data.get("source").ok_or_else(|| {
                        PlanningError::invalid("user/message.source", "is required")
                    })?)?
                    .ok_or_else(|| {
                        PlanningError::invalid("user/message.source", "must identify a Goal round")
                    })?;
                candidate.state.admit_round(&source)?;
                ReplayOutcome::Applied
            }
            _ => ReplayOutcome::Unrelated,
        };
        *self = candidate;
        Ok(outcome)
    }

    /// The current Goal.
    /// @returns The projection, or `None` before a Goal exists and after a clear.
    #[must_use]
    pub fn projection(&self) -> Option<&GoalProjection> {
        self.state.projection()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn row(kind: &str, data: Value, seq: u64) -> SessionEvent {
        SessionEvent {
            kind: kind.into(),
            data,
            extra: serde_json::Map::from_iter([
                ("seq".to_string(), json!(seq)),
                ("time".to_string(), json!(100 + seq)),
            ]),
        }
    }

    fn create() -> Value {
        json!({
            "kind": "goal/change", "version": 1, "operation": "create",
            "goal": {"id": "goal-1", "revision": 1, "objective": "ship it", "phase": "active", "maxGoalRounds": 4},
            "roundsStarted": 0, "createdAt": 10, "updatedAt": 10
        })
    }

    #[test]
    fn unrelated_and_plain_user_events_leave_the_goal_alone() {
        let mut timeline = GoalTimeline::new();
        timeline.observe(&row("goal/change", create(), 0)).unwrap();
        assert_eq!(
            timeline.observe(&row("assistant/message", json!({"content": []}), 1)),
            Ok(ReplayOutcome::Unrelated)
        );
        assert_eq!(
            timeline.observe(&row(
                "user/message",
                json!({"content": [], "source": {"kind": "user"}}),
                2
            )),
            Ok(ReplayOutcome::Unrelated)
        );
        assert_eq!(timeline.projection().unwrap().rounds_started, 0);
    }

    #[test]
    fn admitted_rounds_and_clears_move_the_timeline() {
        let mut timeline = GoalTimeline::new();
        timeline.observe(&row("goal/change", create(), 0)).unwrap();
        timeline
            .observe(&row(
                "user/message",
                json!({
                    "content": [{"type": "text", "text": "continue"}],
                    "source": {"kind": "goal", "goalId": "goal-1", "revision": 1, "round": 1}
                }),
                1,
            ))
            .unwrap();
        assert_eq!(timeline.projection().unwrap().rounds_started, 1);
        let clear = json!({
            "kind": "goal/change", "version": 1, "operation": "clear",
            "cleared": {"id": "goal-1", "revision": 2}, "clearedAt": 20
        });
        timeline.observe(&row("goal/change", clear, 2)).unwrap();
        assert!(timeline.projection().is_none());
    }

    #[test]
    fn a_rejected_change_keeps_the_previous_goal() {
        let mut timeline = GoalTimeline::new();
        timeline.observe(&row("goal/change", create(), 0)).unwrap();
        let duplicate = create();
        assert!(timeline.observe(&row("goal/change", duplicate, 1)).is_err());
        assert_eq!(timeline.projection().unwrap().goal.revision, 1);
    }
}
