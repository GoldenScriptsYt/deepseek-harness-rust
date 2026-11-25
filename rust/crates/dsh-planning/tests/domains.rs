mod common;

use common::*;
use dsh_planning::{
    CommandDoneKind, GoalPhase, GoalSnapshotOperation, PlanningError, PlanningEvent,
    PlanningLimits, PlanningPayload, PlanningState, ReplayOutcome, TodoItem, TodoStatus, TodoWrite,
};
use serde_json::json;

#[test]
fn goal_lifecycle_and_clear_preserve_revision_and_creation_metadata() {
    let mut state = PlanningState::default();
    assert_eq!(
        apply(&mut state, create_goal("goal-1", 3, 100, 100), 0, 100),
        ReplayOutcome::Applied
    );

    let operations = [
        (
            GoalSnapshotOperation::Edit,
            goal("goal-1", 2, GoalPhase::Active, "Ship reliably", 3),
            101,
        ),
        (
            GoalSnapshotOperation::Pause,
            goal("goal-1", 3, GoalPhase::Paused, "Ship reliably", 3),
            102,
        ),
        (
            GoalSnapshotOperation::Resume,
            goal("goal-1", 4, GoalPhase::Active, "Ship reliably", 3),
            103,
        ),
        (
            GoalSnapshotOperation::Block,
            goal("goal-1", 5, GoalPhase::Blocked, "Ship reliably", 3),
            104,
        ),
        (
            GoalSnapshotOperation::Resume,
            goal("goal-1", 6, GoalPhase::Active, "Ship reliably", 3),
            105,
        ),
        (
            GoalSnapshotOperation::Complete,
            goal("goal-1", 7, GoalPhase::Complete, "Ship reliably", 3),
            106,
        ),
    ];
    for (index, (operation, next, time)) in operations.into_iter().enumerate() {
        apply(
            &mut state,
            snapshot_change(operation, next, 0, 100, time),
            index as u64 + 1,
            time,
        );
        let current = state.goal_projection().unwrap();
        assert_eq!(current.created_at, 100);
        assert_eq!(current.updated_at, time);
        assert_eq!(current.rounds_started, 0);
    }

    apply(&mut state, clear_goal("goal-1", 8, 107), 7, 107);
    assert!(state.goal_projection().is_none());
    assert_eq!(state.goal().last_ref.as_ref().unwrap().revision, 8);
    assert_eq!(
        state.goal().seen_goal_ids,
        [dsh_planning::GoalId::new("goal-1").unwrap()].into()
    );
}

#[test]
fn goal_rejects_revision_gaps_invalid_transitions_and_mutated_counters_atomically() {
    let mut state = PlanningState::default();
    apply(&mut state, create_goal("goal-1", 2, 10, 10), 0, 10);

    let cases = [
        snapshot_change(
            GoalSnapshotOperation::Edit,
            goal("goal-1", 3, GoalPhase::Active, "Changed", 2),
            0,
            10,
            11,
        ),
        snapshot_change(
            GoalSnapshotOperation::Edit,
            goal("goal-1", 2, GoalPhase::Paused, "Changed", 2),
            0,
            10,
            11,
        ),
        snapshot_change(
            GoalSnapshotOperation::Edit,
            goal("goal-1", 2, GoalPhase::Active, "Changed", 2),
            1,
            10,
            11,
        ),
        snapshot_change(
            GoalSnapshotOperation::Edit,
            goal("goal-1", 2, GoalPhase::Active, "Changed", 2),
            0,
            9,
            11,
        ),
        snapshot_change(
            GoalSnapshotOperation::Pause,
            goal("goal-1", 2, GoalPhase::Paused, "Ship the feature", 2),
            0,
            10,
            9,
        ),
    ];
    for change in cases {
        let before = state.snapshot();
        let error = match PlanningEvent::new(change.into(), 1, 20) {
            Err(error) => error,
            Ok(event) => state
                .replay(&event.to_session_event().unwrap())
                .expect_err("invalid Goal mutation must fail"),
        };
        assert_eq!(error.code(), "PLANNING_INVALID");
        assert_eq!(state.snapshot(), before);
    }

    apply(
        &mut state,
        snapshot_change(
            GoalSnapshotOperation::Pause,
            goal("goal-1", 2, GoalPhase::Paused, "Ship the feature", 2),
            0,
            10,
            11,
        ),
        1,
        11,
    );
    let before = state.snapshot();
    assert!(state
        .replay(&event(
            snapshot_change(
                GoalSnapshotOperation::Pause,
                goal("goal-1", 3, GoalPhase::Paused, "Ship the feature", 2),
                0,
                10,
                12,
            ),
            2,
            12,
        ))
        .is_err());
    assert_eq!(state.snapshot(), before);
}

#[test]
fn goal_creation_requires_fresh_active_revision_one_and_cannot_replace_active_goal() {
    let invalid = [
        goal("bad", 2, GoalPhase::Active, "Objective", 2),
        goal("bad", 1, GoalPhase::Paused, "Objective", 2),
        goal("bad", 1, GoalPhase::Active, " Objective ", 2),
    ];
    for next in invalid {
        let change = snapshot_change(GoalSnapshotOperation::Create, next, 0, 1, 1);
        let rejected = match PlanningEvent::new(change.into(), 0, 1) {
            Err(_) => true,
            Ok(valid) => PlanningState::default()
                .replay(&valid.to_session_event().unwrap())
                .is_err(),
        };
        assert!(rejected);
    }

    let mut state = PlanningState::default();
    apply(&mut state, create_goal("one", 2, 1, 1), 0, 1);
    let before = state.snapshot();
    assert!(state
        .replay(&event(create_goal("two", 2, 1, 2), 1, 2))
        .is_err());
    assert_eq!(state.snapshot(), before);

    apply(
        &mut state,
        snapshot_change(
            GoalSnapshotOperation::Complete,
            goal("one", 2, GoalPhase::Complete, "Ship the feature", 2),
            0,
            1,
            2,
        ),
        1,
        2,
    );
    apply(&mut state, create_goal("two", 2, 3, 3), 2, 3);
    assert_eq!(state.goal_projection().unwrap().goal.id.as_str(), "two");
}

#[test]
fn goal_admits_only_next_round_at_current_revision_and_within_budget() {
    let mut state = PlanningState::default();
    apply(&mut state, create_goal("goal-1", 2, 0, 0), 0, 0);
    let message = |revision, round| {
        PlanningPayload::user_message(json!({
            "id": "message-1",
            "role": "user",
            "content": [{"type": "text", "text": "Continue"}],
            "source": {"kind": "goal", "goalId": "goal-1", "revision": revision, "round": round}
        }))
        .unwrap()
    };
    apply(&mut state, message(1, 1), 1, 1);
    assert_eq!(state.goal_projection().unwrap().rounds_started, 1);

    apply(
        &mut state,
        snapshot_change(
            GoalSnapshotOperation::Edit,
            goal("goal-1", 2, GoalPhase::Active, "Refined", 2),
            1,
            0,
            2,
        ),
        2,
        2,
    );
    for (offset, revision, round) in [(1, 1, 2), (4, 3, 1), (5, 2, 3), (6, 3, 2)] {
        let before = state.snapshot();
        assert!(state
            .replay(&event(message(revision, round), 3, 3 + offset))
            .is_err());
        assert_eq!(state.snapshot(), before);
    }
    apply(&mut state, message(2, 2), 3, 10);
    assert_eq!(state.goal_projection().unwrap().rounds_started, 2);

    let before = state.snapshot();
    assert!(state.replay(&event(message(2, 3), 4, 11)).is_err());
    assert_eq!(state.snapshot(), before);
}

#[test]
fn non_goal_messages_and_unknown_events_advance_without_mutating_goal() {
    let mut state = PlanningState::default();
    apply(&mut state, create_goal("goal-1", 1, 0, 0), 0, 0);
    let before = state.snapshot();
    let unrelated = raw_event(
        "user/message",
        json!({
            "id": "message-1",
            "role": "user",
            "content": "Hello",
            "source": {"kind": "future-source", "extension": {"kept": true}}
        }),
        1,
        1,
    );
    assert_eq!(state.replay(&unrelated).unwrap(), ReplayOutcome::Unrelated);
    assert_eq!(state.goal(), &before.goal);
    assert_eq!(state.cursor().last_seq, Some(1));

    assert!(PlanningPayload::user_message(json!({
        "id": "message-2",
        "role": "user",
        "content": "Hello",
        "source": {"kind": "future-source"}
    }))
    .is_err());
    assert!(PlanningPayload::user_message(json!({"source": null})).is_err());
}

#[test]
fn cleared_goal_ids_cannot_be_reused_but_new_goal_can_follow_clear() {
    let mut state = PlanningState::default();
    apply(&mut state, create_goal("same", 2, 1, 1), 0, 1);
    apply(&mut state, clear_goal("same", 2, 2), 1, 2);
    let before = state.snapshot();
    assert!(state
        .replay(&event(create_goal("same", 2, 3, 3), 2, 3))
        .is_err());
    assert_eq!(state.snapshot(), before);
    apply(&mut state, create_goal("new", 2, 3, 3), 2, 3);
    assert_eq!(state.goal().seen_goal_ids.len(), 2);
}

#[test]
fn plan_command_selection_precedence_settlement_and_explicit_mode() {
    let mut state = PlanningState::default();
    assert_eq!(
        apply(&mut state, command_run("a", "plan", None), 0, 0),
        ReplayOutcome::Unrelated
    );
    assert!(state.plan().running.is_none());

    apply(&mut state, command_run("b", "plan", Some(" off ")), 1, 1);
    assert!(!state.plan_projection().pending);
    assert!(!state.plan().running.as_ref().unwrap().wanted);
    apply(&mut state, command_run("c", "plan", None), 2, 2);
    assert_eq!(
        state.plan().running.as_ref().unwrap().command_id.as_str(),
        "b"
    );

    apply(
        &mut state,
        command_done("b", CommandDoneKind::Error, Some("Rejected"), None),
        3,
        3,
    );
    assert!(!state.plan_projection().pending);
    assert!(state.plan().wanted.is_none());

    apply(&mut state, command_run("d", "plan", Some("on")), 4, 4);
    apply(
        &mut state,
        command_done("d", CommandDoneKind::Success, None, Some(4)),
        5,
        5,
    );
    assert!(state.plan_projection().pending);
    assert_eq!(state.plan().wanted, Some(true));
    apply(
        &mut state,
        PlanningPayload::PlanMode(dsh_planning::PlanMode { active: true }),
        6,
        6,
    );
    assert!(state.plan_projection().active);
    assert!(!state.plan_projection().pending);
    assert!(state.plan().entered);

    apply(
        &mut state,
        PlanningPayload::PlanMode(dsh_planning::PlanMode { active: false }),
        7,
        7,
    );
    assert!(state.plan().entered);
    assert!(!state.plan().active);

    apply(&mut state, command_run("e", "plan", Some("off")), 8, 8);
    apply(
        &mut state,
        command_done("e", CommandDoneKind::Success, None, None),
        9,
        9,
    );
    assert!(state.plan().wanted.is_none());
    assert!(!state.plan_projection().pending);
}

#[test]
fn malformed_command_events_are_rejected_even_when_unrelated_to_plan() {
    let invalid = [
        json!({"commandId": "x", "name": "Plan", "source": {"kind": "user"}}),
        json!({"commandId": "x", "name": "plan", "args": null, "source": {"kind": "user"}}),
        json!({"commandId": "x", "name": "plan", "source": {"kind": "agent"}}),
        json!({"commandId": "x", "name": "plan", "source": {"kind": "user"}, "extra": 1}),
    ];
    for data in invalid {
        assert!(dsh_planning::PlanningEvent::from_session_event(&raw_event(
            "command/run",
            data,
            0,
            0,
        ))
        .is_err());
    }
    assert!(PlanningEvent::new(
        command_done("x", CommandDoneKind::Error, Some(" "), None),
        0,
        0,
    )
    .is_err());
    assert!(PlanningEvent::new(
        command_done("x", CommandDoneKind::Error, Some("failure"), Some(0)),
        0,
        0,
    )
    .is_err());
}

#[test]
fn todos_replace_whole_ordered_lists_and_clear_only_at_turn_start() {
    let mut state = PlanningState::default();
    let first = TodoWrite {
        todos: vec![
            TodoItem {
                content: "First".into(),
                status: TodoStatus::InProgress,
            },
            TodoItem {
                content: "Second".into(),
                status: TodoStatus::Pending,
            },
            TodoItem {
                content: "Third".into(),
                status: TodoStatus::Completed,
            },
        ],
    };
    apply(&mut state, PlanningPayload::TodoWrite(first.clone()), 0, 0);
    assert_eq!(state.todos().unwrap(), first.todos);

    let replacement = TodoWrite {
        todos: vec![TodoItem {
            content: "Only".into(),
            status: TodoStatus::Pending,
        }],
    };
    apply(
        &mut state,
        PlanningPayload::TodoWrite(replacement.clone()),
        1,
        1,
    );
    assert_eq!(state.todos().unwrap(), replacement.todos);

    assert_eq!(
        state
            .replay(&raw_event("turn/end", json!({}), 2, 2))
            .unwrap(),
        ReplayOutcome::Unrelated
    );
    assert_eq!(state.todos().unwrap(), replacement.todos);
    apply(
        &mut state,
        PlanningPayload::TurnStart(dsh_planning::TurnStart { turn: 3 }),
        3,
        3,
    );
    assert!(state.todos().is_none());

    apply(
        &mut state,
        PlanningPayload::TodoWrite(TodoWrite { todos: vec![] }),
        4,
        4,
    );
    assert_eq!(state.todos(), Some([].as_slice()));
}

#[test]
fn todo_shape_and_configured_bounds_are_enforced_atomically() {
    let invalid = [
        TodoWrite {
            todos: vec![TodoItem {
                content: " duplicate ".into(),
                status: TodoStatus::Pending,
            }],
        },
        TodoWrite {
            todos: vec![
                TodoItem {
                    content: "Same".into(),
                    status: TodoStatus::Pending,
                },
                TodoItem {
                    content: "Same".into(),
                    status: TodoStatus::Completed,
                },
            ],
        },
        TodoWrite {
            todos: vec![TodoItem {
                content: " ".into(),
                status: TodoStatus::Pending,
            }],
        },
    ];
    for write in invalid {
        assert!(PlanningEvent::new(PlanningPayload::TodoWrite(write), 0, 0).is_err());
    }

    let parallel = TodoWrite {
        todos: vec![
            TodoItem {
                content: "One".into(),
                status: TodoStatus::InProgress,
            },
            TodoItem {
                content: "Two".into(),
                status: TodoStatus::InProgress,
            },
        ],
    };
    let mut state = PlanningState::default();
    let before = state.snapshot();
    assert!(state
        .replay(&event(PlanningPayload::TodoWrite(parallel.clone()), 0, 0))
        .is_err());
    assert_eq!(state.snapshot(), before);

    let mut permissive = PlanningState::new(PlanningLimits {
        allow_parallel_todos: true,
        ..PlanningLimits::default()
    });
    apply(&mut permissive, PlanningPayload::TodoWrite(parallel), 0, 0);
    assert_eq!(permissive.todos().unwrap().len(), 2);

    let mut bounded = PlanningState::new(PlanningLimits {
        max_todo_items: 1,
        ..PlanningLimits::default()
    });
    let error = bounded
        .replay(&event(
            PlanningPayload::TodoWrite(TodoWrite {
                todos: vec![
                    TodoItem {
                        content: "One".into(),
                        status: TodoStatus::Pending,
                    },
                    TodoItem {
                        content: "Two".into(),
                        status: TodoStatus::Pending,
                    },
                ],
            }),
            0,
            0,
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        PlanningError::LimitExceeded { limit: 1, .. }
    ));
}
