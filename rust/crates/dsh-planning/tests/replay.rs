mod common;

use common::*;
use dsh_planning::{
    PlanningError, PlanningLimits, PlanningPayload, PlanningSnapshot, PlanningState, ReplayCursor,
    ReplayOutcome, TodoItem, TodoStatus, TodoWrite, SNAPSHOT_VERSION,
};
use serde_json::{json, Value};

#[test]
fn empty_snapshot_json_round_trip_and_default_limits() {
    let state = PlanningState::default();
    assert_eq!(state.cursor(), ReplayCursor::default());
    assert!(state.goal_projection().is_none());
    assert!(state.todos().is_none());
    assert!(state.schedule_entries().is_empty());
    assert!(!state.plan_projection().active);
    assert!(!state.plan_projection().pending);
    assert_eq!(state.limits(), PlanningLimits::default());

    let json = state.to_json().unwrap();
    let restored = PlanningState::from_json(&json, PlanningLimits::default()).unwrap();
    assert_eq!(restored, state);
    assert_eq!(restored.to_json().unwrap(), json);
    assert_eq!(
        state.to_snapshot_value().unwrap()["version"],
        Value::from(SNAPSHOT_VERSION)
    );
}

#[test]
fn duplicate_and_older_sequences_are_ignored_without_advancing_cursor() {
    let mut state = PlanningState::default();
    let first = event(create_goal("one", 2, 10, 10), 0, 10);
    assert_eq!(state.replay(&first).unwrap(), ReplayOutcome::Applied);
    let before = state.snapshot();

    let mut duplicate = first.clone();
    duplicate.extra.insert("time".into(), json!(999));
    assert_eq!(state.replay(&duplicate).unwrap(), ReplayOutcome::Ignored);
    assert_eq!(state.snapshot(), before);

    let older = raw_event("future/event", json!({"anything": true}), 0, 1);
    assert_eq!(state.replay(&older).unwrap(), ReplayOutcome::Ignored);
    assert_eq!(state.snapshot(), before);
}

#[test]
fn sequence_gaps_backward_time_and_equal_time_policy_are_explicit() {
    let mut state = PlanningState::default();
    let error = state
        .replay(&raw_event("future/event", json!({}), 1, 10))
        .unwrap_err();
    assert!(matches!(
        error,
        PlanningError::InvalidSequence {
            actual: 1,
            last: -1
        }
    ));
    assert_eq!(state.cursor(), ReplayCursor::default());

    assert_eq!(
        state
            .replay(&raw_event("future/event", json!({}), 0, 10))
            .unwrap(),
        ReplayOutcome::Unrelated
    );
    let before = state.snapshot();
    let error = state
        .replay(&raw_event("future/event", json!({}), 2, 10))
        .unwrap_err();
    assert!(matches!(
        error,
        PlanningError::InvalidSequence { actual: 2, last: 0 }
    ));
    assert_eq!(state.snapshot(), before);

    let error = state
        .replay(&raw_event("future/event", json!({}), 1, 9))
        .unwrap_err();
    assert!(matches!(
        error,
        PlanningError::InvalidTime {
            actual: 9,
            last: 10
        }
    ));
    assert_eq!(state.snapshot(), before);

    assert_eq!(
        state
            .replay(&raw_event("future/event", json!({}), 1, 10))
            .unwrap(),
        ReplayOutcome::Unrelated
    );
    assert_eq!(
        state.cursor(),
        ReplayCursor {
            last_seq: Some(1),
            last_time: Some(10)
        }
    );
}

#[test]
fn unknown_and_historical_schedule_events_advance_the_watermark() {
    let mut state = PlanningState::default();
    let events = [
        raw_event("future/event", json!({"arbitrary": [1, null]}), 0, 0),
        raw_event(
            "schedule/change",
            json!({
                "version": 1,
                "operation": "create",
                "id": "legacy",
                "record": {"kind": "legacy"}
            }),
            1,
            1,
        ),
        raw_event("turn/end", json!({}), 2, 2),
    ];
    let outcomes = state.replay_events(&events).unwrap();
    assert_eq!(outcomes, vec![ReplayOutcome::Unrelated; 3]);
    assert_eq!(state.cursor().last_seq, Some(2));
    assert_eq!(state.cursor().last_time, Some(2));
    assert!(state.schedule_entries().is_empty());
}

#[test]
fn malformed_known_stale_events_do_not_hide_behind_duplicate_sequence() {
    let mut state = PlanningState::default();
    apply(&mut state, create_goal("one", 2, 0, 0), 0, 0);
    let before = state.snapshot();
    let malformed = raw_event(
        "command/run",
        json!({"commandId": "x", "name": "PLAN", "source": {"kind": "user"}}),
        0,
        100,
    );
    assert!(state.replay(&malformed).is_err());
    assert_eq!(state.snapshot(), before);

    let malformed_goal = raw_event(
        "goal/change",
        json!({"kind": "goal/change", "version": 99, "operation": "clear"}),
        0,
        100,
    );
    assert!(state.replay(&malformed_goal).is_err());
    assert_eq!(state.snapshot(), before);
}

#[test]
fn metadata_and_safe_integer_validation_precede_replay() {
    let mut state = PlanningState::default();
    let mut extra = serde_json::Map::new();
    extra.insert(
        "seq".into(),
        json!(dsh_planning::MAX_SAFE_SESSION_INTEGER + 1),
    );
    extra.insert("time".into(), json!(0));
    let unsafe_seq = dsh_session_format::Event {
        kind: "future/event".into(),
        data: json!({}),
        extra,
    };
    assert_eq!(
        state.replay(&unsafe_seq).unwrap_err().code(),
        "PLANNING_INVALID"
    );

    for data in [
        raw_event("future/event", json!({}), 0, 0),
        raw_event("future/event", json!({}), 0, 0),
    ] {
        let mut invalid = data;
        invalid.extra.remove("time");
        assert!(state.replay(&invalid).is_err());
    }
    assert_eq!(state.cursor(), ReplayCursor::default());

    let mut fractional = raw_event("future/event", json!({}), 0, 0);
    fractional.extra.insert("seq".into(), json!(0.0));
    assert!(state.replay(&fractional).is_err());
}

#[test]
fn rejected_payload_leaves_domains_and_cursor_unchanged() {
    let mut state = PlanningState::new(PlanningLimits {
        max_schedule_entries: 0,
        ..PlanningLimits::default()
    });
    apply(&mut state, create_goal("one", 2, 0, 0), 0, 0);
    apply(
        &mut state,
        PlanningPayload::TodoWrite(TodoWrite {
            todos: vec![TodoItem {
                content: "Keep".into(),
                status: TodoStatus::Completed,
            }],
        }),
        1,
        1,
    );
    let before = state.snapshot();
    let error = state
        .replay(&event(
            put(at_entry("schedule-1", dsh_planning::ScheduleStatus::Active)),
            2,
            2,
        ))
        .unwrap_err();
    assert_eq!(error.code(), "PLANNING_LIMIT_EXCEEDED");
    assert_eq!(state.snapshot(), before);
}

#[test]
fn checkpoint_plus_contiguous_suffix_matches_full_replay() {
    let events = vec![
        event(create_goal("one", 3, 10, 10), 0, 10),
        event(
            PlanningPayload::user_message(json!({
                "id": "m1",
                "role": "user",
                "content": "Start",
                "source": {"kind": "goal", "goalId": "one", "revision": 1, "round": 1}
            }))
            .unwrap(),
            1,
            11,
        ),
        event(command_run("plan-1", "plan", Some("on")), 2, 12),
        event(
            command_done(
                "plan-1",
                dsh_planning::CommandDoneKind::Success,
                None,
                Some(2),
            ),
            3,
            13,
        ),
        event(
            PlanningPayload::TodoWrite(TodoWrite {
                todos: vec![TodoItem {
                    content: "Verify".into(),
                    status: TodoStatus::InProgress,
                }],
            }),
            4,
            14,
        ),
        event(
            put(at_entry("schedule-1", dsh_planning::ScheduleStatus::Paused)),
            5,
            15,
        ),
    ];

    let mut full = PlanningState::default();
    full.replay_events(&events).unwrap();

    let mut prefix = PlanningState::default();
    prefix.replay_events(&events[..3]).unwrap();
    let checkpoint_json = prefix.to_json().unwrap();
    let checkpoint = prefix.snapshot();
    let mut recovered =
        PlanningState::recover(checkpoint, &events[3..], PlanningLimits::default()).unwrap();
    assert_eq!(recovered, full);

    let mut duplicate_suffix = events[3..].to_vec();
    duplicate_suffix.insert(0, events[2].clone());
    let outcomes = recovered.replay_events(&duplicate_suffix).unwrap();
    assert_eq!(outcomes[0], ReplayOutcome::Ignored);
    assert_eq!(recovered, full);

    let restored = PlanningState::from_json(&checkpoint_json, PlanningLimits::default()).unwrap();
    assert_eq!(restored, prefix);
}

#[test]
fn snapshot_versions_cursor_invariants_and_unknown_keys_are_rejected() {
    let state = PlanningState::default();
    let mut value = state.to_snapshot_value().unwrap();
    value["version"] = json!(2);
    let error = PlanningState::from_snapshot_value(value, PlanningLimits::default()).unwrap_err();
    assert!(matches!(
        error,
        PlanningError::UnsupportedSnapshotVersion { version: 2 }
    ));

    let mut snapshot = state.snapshot();
    snapshot.cursor.last_seq = Some(0);
    let error = PlanningState::from_snapshot(snapshot, PlanningLimits::default()).unwrap_err();
    assert_eq!(error.code(), "PLANNING_CORRUPT");

    let mut value = state.to_snapshot_value().unwrap();
    value["unexpected"] = json!(true);
    assert!(PlanningState::from_snapshot_value(value, PlanningLimits::default()).is_err());

    let mut value = state.to_snapshot_value().unwrap();
    value["cursor"]["lastSeq"] = json!(dsh_planning::MAX_SAFE_SESSION_INTEGER + 1);
    value["cursor"]["lastTime"] = json!(0);
    assert_eq!(
        PlanningState::from_snapshot_value(value, PlanningLimits::default())
            .unwrap_err()
            .code(),
        "PLANNING_CORRUPT"
    );
}

#[test]
fn restore_and_reconfiguration_enforce_collection_bounds() {
    let mut state = PlanningState::default();
    apply(
        &mut state,
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
    );
    let snapshot = state.snapshot();
    let too_small = PlanningLimits {
        max_todo_items: 1,
        ..PlanningLimits::default()
    };
    assert_eq!(
        PlanningState::from_snapshot(snapshot.clone(), too_small)
            .unwrap_err()
            .code(),
        "PLANNING_LIMIT_EXCEEDED"
    );

    let before = state.snapshot();
    assert!(state.set_limits(too_small).is_err());
    assert_eq!(state.snapshot(), before);
    assert_eq!(state.limits(), PlanningLimits::default());
    state
        .set_limits(PlanningLimits {
            max_todo_items: 2,
            ..PlanningLimits::default()
        })
        .unwrap();
}

#[test]
fn snapshot_rejects_internal_invariants_even_when_types_are_constructible() {
    let mut snapshot: PlanningSnapshot = PlanningState::default().snapshot();
    snapshot.plan.active = true;
    snapshot.plan.entered = false;
    assert_eq!(
        PlanningState::from_snapshot(snapshot, PlanningLimits::default())
            .unwrap_err()
            .code(),
        "PLANNING_CORRUPT"
    );
}
