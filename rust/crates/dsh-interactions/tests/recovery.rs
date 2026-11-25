mod common;

use common::{answers, approval, call_id, limits, question_request};
use dsh_interactions::{
    ApprovalOutcome, ApprovalRequestId, ApprovalResponse, DurableEvent, InteractionError,
    InteractionEventKind, InteractionLimits, InteractionPayload, InteractionState,
    QuestionResponse, QuestionState, ReplayOutcome, ToolCallId,
};
use dsh_session_format::Event as SessionEvent;
use serde_json::{json, Value};

fn event(payload: InteractionPayload, seq: u64, time: u64) -> DurableEvent {
    DurableEvent::new(payload, seq, time).unwrap()
}

#[test]
fn ordered_event_replay_matches_live_transitions_and_snapshot_recovery() {
    let live = InteractionState::new(limits()).unwrap();
    let approval_request = approval("approval");
    let approval_id = approval_request.id.clone();
    let question_request = question_request("call", &["q1"]);
    let question_id = question_request.call_id.clone();
    let mut events = vec![
        live.open_approval(approval_request, 0, 10).unwrap(),
        live.open_question(question_request, QuestionState::Open, 1, 10)
            .unwrap(),
    ];
    match live.continue_question(&question_id, 2, 10).unwrap() {
        QuestionResponse::Continued { event, .. } => events.push(event),
        other => panic!("unexpected continuation: {other:?}"),
    }
    match live
        .decide_approval(&approval_id, ApprovalOutcome::Cancelled, 3, 10)
        .unwrap()
    {
        ApprovalResponse::Accepted(decision) => events.push(decision.event),
        other => panic!("unexpected approval: {other:?}"),
    }
    match live
        .submit_answers(&question_id, answers(&["q1"]), 4, 10)
        .unwrap()
    {
        QuestionResponse::Answered { event, .. } => events.push(event),
        other => panic!("unexpected answer: {other:?}"),
    }

    let replayed = InteractionState::new(limits()).unwrap();
    for record in &events {
        assert_eq!(
            replayed.replay(record.clone()).unwrap(),
            ReplayOutcome::Applied
        );
    }
    assert_eq!(replayed.snapshot().unwrap(), live.snapshot().unwrap());

    let restored = InteractionState::from_snapshot(
        serde_json::from_value(serde_json::to_value(live.snapshot().unwrap()).unwrap()).unwrap(),
        limits(),
    )
    .unwrap();
    assert_eq!(restored.snapshot().unwrap(), live.snapshot().unwrap());
}

#[test]
fn duplicate_and_late_replay_records_do_not_change_settlements() {
    let state = InteractionState::new(limits()).unwrap();
    let request = approval("approval");
    let id = request.id.clone();
    let asked = event(InteractionPayload::ApprovalAsked(request.into()), 1, 10);
    let decided = event(
        InteractionPayload::ApprovalDecided(dsh_interactions::ApprovalDecided {
            id: id.clone(),
            outcome: ApprovalOutcome::Rejected,
        }),
        2,
        10,
    );
    state.replay(asked.clone()).unwrap();
    state.replay(decided).unwrap();
    let before = state.snapshot().unwrap();
    assert_eq!(
        state
            .replay(event(asked.payload().unwrap(), 3, 10))
            .unwrap(),
        ReplayOutcome::Ignored
    );
    assert_eq!(
        state
            .replay(event(
                InteractionPayload::ApprovalDecided(dsh_interactions::ApprovalDecided {
                    id,
                    outcome: ApprovalOutcome::AllowedOnce,
                }),
                4,
                10
            ))
            .unwrap(),
        ReplayOutcome::Ignored
    );
    assert_eq!(
        state.recent_approvals().unwrap(),
        before
            .approvals
            .iter()
            .filter_map(|record| {
                match record {
                    dsh_interactions::ApprovalRecord::Settled(value) => Some(value.clone()),
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
    );
    assert!(state.pending_approvals().unwrap().is_empty());
}

#[test]
fn malformed_serialized_events_are_rejected() {
    let malformed = [
        json!({
            "type": "approval/decided",
            "data": {"id": "one", "outcome": "approved"},
            "seq": 1,
            "time": 1
        }),
        json!({
            "type": "approval/asked",
            "data": {"id": "one", "toolName": "shell", "runtimeSignal": {}},
            "seq": 1,
            "time": 1
        }),
        json!({
            "type": "approval/asked",
            "data": {"id": "question", "questions": [], "state": "open"},
            "seq": 1,
            "time": 1
        }),
        json!({
            "type": "question/continued",
            "data": {"callId": "call"},
            "seq": "1",
            "time": 1
        }),
    ];
    for value in malformed {
        assert!(serde_json::from_value::<DurableEvent>(value).is_err());
    }
}

#[test]
fn semantically_malformed_recovery_is_atomic() {
    let state = InteractionState::new(limits()).unwrap();
    let request = question_request("call", &["q1", "q2"]);
    let id = request.call_id.clone();
    state
        .replay(event(
            InteractionPayload::QuestionAsked(dsh_interactions::QuestionAsked {
                call_id: id.clone(),
                questions: request.questions,
                state: QuestionState::Open,
            }),
            1,
            10,
        ))
        .unwrap();
    let before = state.snapshot().unwrap();
    let invalid = event(
        InteractionPayload::QuestionSettled(dsh_interactions::QuestionSettled {
            call_id: id,
            answers: answers(&["q1"]).answers,
        }),
        2,
        10,
    );
    assert!(matches!(
        state.replay(invalid),
        Err(InteractionError::Validation(_))
    ));
    assert_eq!(state.snapshot().unwrap(), before);

    let empty_id = event(
        InteractionPayload::ApprovalAsked(dsh_interactions::ApprovalAsked {
            id: ApprovalRequestId::new("")
                .unwrap_or_else(|_| serde_json::from_value(json!("")).unwrap()),
            tool_name: "shell".to_owned(),
            call_id: None,
            reason: None,
        }),
        2,
        10,
    );
    assert!(matches!(
        state.replay(empty_id),
        Err(InteractionError::Validation(_))
    ));
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn malformed_snapshots_fail_closed() {
    let state = InteractionState::new(limits()).unwrap();
    state.open_approval(approval("one"), 1, 10).unwrap();
    let valid = serde_json::to_value(state.snapshot().unwrap()).unwrap();

    let mut unsupported = valid.clone();
    unsupported["version"] = json!(2);
    assert!(InteractionState::from_snapshot(
        serde_json::from_value(unsupported).unwrap(),
        limits()
    )
    .is_err());

    let mut duplicate = valid.clone();
    duplicate["approvals"] = json!([valid["approvals"][0], valid["approvals"][0]]);
    assert!(
        InteractionState::from_snapshot(serde_json::from_value(duplicate).unwrap(), limits())
            .is_err()
    );

    let mut missing_cursor = valid.clone();
    missing_cursor.as_object_mut().unwrap().remove("lastSeq");
    assert!(InteractionState::from_snapshot(
        serde_json::from_value(missing_cursor).unwrap(),
        limits()
    )
    .is_err());

    let mut oversized = valid.clone();
    oversized["approvals"]
        .as_array_mut()
        .unwrap()
        .push(valid["approvals"][0].clone());
    assert!(InteractionState::from_snapshot(
        serde_json::from_value(oversized).unwrap(),
        InteractionLimits {
            max_pending_approvals: 1,
            ..limits()
        }
    )
    .is_err());

    let mut future_record = valid;
    future_record["approvals"][0]["askedSeq"] = json!(2);
    assert!(InteractionState::from_snapshot(
        serde_json::from_value(future_record).unwrap(),
        limits()
    )
    .is_err());
}

#[test]
fn session_rows_require_metadata_and_ignore_unrelated_kinds() {
    let state = InteractionState::new(limits()).unwrap();
    let unrelated = SessionEvent {
        kind: "turn/start".to_owned(),
        data: json!({"turn": 1}),
        extra: serde_json::Map::new(),
    };
    assert_eq!(state.replay_session_event(&unrelated).unwrap(), None);

    let missing_seq = SessionEvent {
        kind: "approval/asked".to_owned(),
        data: json!({"id": "one", "toolName": "shell"}),
        extra: serde_json::Map::new(),
    };
    assert!(state.replay_session_event(&missing_seq).is_err());

    let mut extra = serde_json::Map::new();
    extra.insert("seq".to_owned(), json!(0));
    extra.insert("time".to_owned(), json!(0));
    extra.insert("futureAdditiveField".to_owned(), json!(true));
    let row = SessionEvent {
        kind: "approval/asked".to_owned(),
        data: json!({"id": "one", "toolName": "shell"}),
        extra,
    };
    assert_eq!(
        state.replay_session_event(&row).unwrap(),
        Some(ReplayOutcome::Applied)
    );
}

#[test]
fn sequence_time_and_safe_integer_validation_is_deterministic() {
    let state = InteractionState::new(limits()).unwrap();
    state.open_approval(approval("zero"), 0, 10).unwrap();
    assert!(state.open_approval(approval("equal"), 0, 10).is_err());
    assert!(state.open_approval(approval("backward"), 1, 9).is_err());
    assert!(state
        .open_approval(
            approval("unsafe"),
            dsh_interactions::MAX_SAFE_SESSION_INTEGER + 1,
            10
        )
        .is_err());
    assert_eq!(state.pending_approvals().unwrap().len(), 1);
}

#[test]
fn recovery_enforces_pending_bounds_without_partial_mutation() {
    let state = InteractionState::new(InteractionLimits {
        max_pending_questions: 1,
        ..limits()
    })
    .unwrap();
    for (seq, call) in [(1, "one"), (2, "two")] {
        let request = question_request(call, &["q1"]);
        let result = state.replay(event(
            InteractionPayload::QuestionAsked(dsh_interactions::QuestionAsked {
                call_id: request.call_id,
                questions: request.questions,
                state: QuestionState::Open,
            }),
            seq,
            0,
        ));
        if seq == 1 {
            assert_eq!(result.unwrap(), ReplayOutcome::Applied);
        } else {
            assert!(matches!(result, Err(InteractionError::Capacity { .. })));
        }
    }
    assert_eq!(state.pending_questions().unwrap().len(), 1);
    assert_eq!(state.snapshot().unwrap().last_seq, Some(1));
}

#[test]
fn snapshot_json_contains_no_runtime_resources() {
    let state = InteractionState::new(limits()).unwrap();
    state.open_approval(approval("one"), 1, 10).unwrap();
    let value: Value = serde_json::to_value(state.snapshot().unwrap()).unwrap();
    for forbidden in ["signal", "timer", "claims", "queue", "callback", "wait"] {
        assert!(!value.to_string().contains(forbidden));
    }
}

#[test]
fn opaque_ids_validate_at_deserialized_boundaries() {
    assert!(serde_json::from_value::<ApprovalRequestId>(json!("")).is_ok());
    assert!(serde_json::from_value::<ToolCallId>(json!("")).is_ok());
    assert!(ApprovalRequestId::new("").is_err());
    assert!(ToolCallId::new("").is_err());
    assert_eq!(call_id("call").as_str(), "call");
    assert_eq!(
        InteractionEventKind::QuestionAsked,
        serde_json::from_value(json!("question/asked")).unwrap()
    );
}
