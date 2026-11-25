mod common;

use common::{answer, answers, approval, approval_id, call_id, limits, question_request};
use dsh_interactions::{
    ApprovalOutcome, ApprovalResponse, InteractionError, InteractionEventKind, InteractionPayload,
    InteractionState, QuestionResponse, QuestionState, UserQuestionAnswer,
};
use serde_json::json;

#[test]
fn approvals_allow_once_reject_cancel_and_fail_closed() {
    let state = InteractionState::new(limits()).unwrap();
    for (index, outcome) in [
        ApprovalOutcome::AllowedOnce,
        ApprovalOutcome::Rejected,
        ApprovalOutcome::Cancelled,
        ApprovalOutcome::Unavailable,
    ]
    .into_iter()
    .enumerate()
    {
        let request = approval(&format!("approval-{index}"));
        let id = request.id.clone();
        let asked = state.open_approval(request, index as u64 * 2, 10).unwrap();
        assert_eq!(asked.kind(), InteractionEventKind::ApprovalAsked);
        match state
            .decide_approval(&id, outcome, index as u64 * 2 + 1, 10)
            .unwrap()
        {
            ApprovalResponse::Accepted(decision) => {
                assert_eq!(decision.event.kind(), InteractionEventKind::ApprovalDecided);
                assert_eq!(decision.settled.outcome, outcome);
            }
            other => panic!("expected accepted approval, got {other:?}"),
        }
    }
    assert!(state.pending_approvals().unwrap().is_empty());
    assert_eq!(state.recent_approvals().unwrap().len(), 2);
}

#[test]
fn first_approval_response_wins_and_unknown_ids_are_distinct() {
    let state = InteractionState::new(limits()).unwrap();
    let request = approval("one");
    let id = request.id.clone();
    state.open_approval(request, 0, 0).unwrap();
    assert!(matches!(
        state.decide_approval(&id, ApprovalOutcome::Rejected, 1, 0),
        Ok(ApprovalResponse::Accepted(_))
    ));
    assert_eq!(
        state.decide_approval(&id, ApprovalOutcome::AllowedOnce, 0, 0),
        Ok(ApprovalResponse::AlreadySettled)
    );
    assert_eq!(
        state.decide_approval(&approval_id("missing"), ApprovalOutcome::Cancelled, 0, 0),
        Ok(ApprovalResponse::Unknown)
    );
    let recent = state.recent_approvals().unwrap();
    assert_eq!(recent[0].outcome, ApprovalOutcome::Rejected);
    assert!(state.pending_approvals().unwrap().is_empty());
}

#[test]
fn duplicate_open_and_pending_capacity_do_not_replace_existing_work() {
    let state = InteractionState::new(limits()).unwrap();
    state.open_approval(approval("one"), 1, 1).unwrap();
    assert!(matches!(
        state.open_approval(approval("one"), 2, 2),
        Err(InteractionError::Validation(_))
    ));
    state.open_approval(approval("two"), 2, 2).unwrap();
    assert!(matches!(
        state.open_approval(approval("three"), 3, 3),
        Err(InteractionError::Capacity { limit: 2, .. })
    ));
    assert_eq!(state.pending_approvals().unwrap().len(), 2);
}

#[test]
fn questions_continue_then_submit_exactly_once() {
    let state = InteractionState::new(limits()).unwrap();
    let request = question_request("call-one", &["q1", "q2"]);
    let id = request.call_id.clone();
    state
        .open_question(request, QuestionState::Open, 0, 100)
        .unwrap();
    match state.continue_question(&id, 1, 100).unwrap() {
        QuestionResponse::Continued { event, question } => {
            assert_eq!(event.kind(), InteractionEventKind::QuestionContinued);
            assert_eq!(question.state, QuestionState::Continued);
        }
        other => panic!("expected continued question, got {other:?}"),
    }
    assert_eq!(
        state.continue_question(&id, 0, 0),
        Ok(QuestionResponse::AlreadyContinued)
    );
    match state
        .submit_answers(&id, answers(&["q2", "q1"]), 2, 100)
        .unwrap()
    {
        QuestionResponse::Answered { event, settled } => {
            assert_eq!(event.kind(), InteractionEventKind::QuestionSettled);
            assert_eq!(settled.answers.len(), 2);
        }
        other => panic!("expected answered question, got {other:?}"),
    }
    assert_eq!(
        state.submit_answers(&id, answers(&["q1", "q2"]), 0, 0),
        Ok(QuestionResponse::AlreadySettled)
    );
    assert_eq!(
        state.continue_question(&id, 0, 0),
        Ok(QuestionResponse::AlreadySettled)
    );
    assert_eq!(
        state.submit_answers(&call_id("missing"), answers(&["q1"]), 0, 0),
        Ok(QuestionResponse::Unknown)
    );
}

#[test]
fn open_questions_can_settle_in_the_foreground() {
    let state = InteractionState::new(limits()).unwrap();
    let request = question_request("foreground", &["q1"]);
    let id = request.call_id.clone();
    state
        .open_question(request, QuestionState::Open, 1, 1)
        .unwrap();
    assert!(matches!(
        state.submit_answers(&id, answers(&["q1"]), 2, 1),
        Ok(QuestionResponse::Answered { .. })
    ));
}

#[test]
fn continued_questions_have_no_deadline_in_persisted_state() {
    let state = InteractionState::new(limits()).unwrap();
    let request = question_request("timed", &["q1"]);
    let id = request.call_id.clone();
    state
        .open_question(request, QuestionState::Open, 1, 50)
        .unwrap();
    state.continue_question(&id, 2, 60).unwrap();
    let restored = InteractionState::from_snapshot(state.snapshot().unwrap(), limits()).unwrap();
    assert!(matches!(
        restored.submit_answers(&id, answers(&["q1"]), 3, 60),
        Ok(QuestionResponse::Answered { .. })
    ));
}

#[test]
fn malformed_answer_batches_leave_pending_state_and_cursor_unchanged() {
    let state = InteractionState::new(limits()).unwrap();
    let request = question_request("call", &["q1", "q2"]);
    let id = request.call_id.clone();
    state
        .open_question(request, QuestionState::Open, 1, 10)
        .unwrap();
    let before = state.snapshot().unwrap();
    let invalid = [
        answers(&[]),
        answers(&["q1"]),
        answers(&["q1", "q2", "extra"]),
        UserQuestionAnswer {
            answers: vec![answer("q1"), answer("q1")],
        },
    ];
    for batch in invalid {
        assert!(matches!(
            state.submit_answers(&id, batch, 2, 10),
            Err(InteractionError::Validation(_))
        ));
        assert_eq!(state.snapshot().unwrap(), before);
    }
    assert!(matches!(
        state.open_question(question_request("empty", &[]), QuestionState::Open, 2, 10),
        Err(InteractionError::Validation(_))
    ));
    assert!(matches!(
        state.open_question(
            question_request("duplicates", &["same", "same"]),
            QuestionState::Open,
            2,
            10
        ),
        Err(InteractionError::Validation(_))
    ));
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn recent_history_is_bounded_and_evicts_the_oldest_settlement() {
    let state = InteractionState::new(dsh_interactions::InteractionLimits {
        max_recent_approvals: 1,
        ..limits()
    })
    .unwrap();
    for (index, name) in ["old", "new"].into_iter().enumerate() {
        let request = approval(name);
        let id = request.id.clone();
        state.open_approval(request, index as u64 * 2, 0).unwrap();
        state
            .decide_approval(&id, ApprovalOutcome::AllowedOnce, index as u64 * 2 + 1, 0)
            .unwrap();
    }
    let recent = state.recent_approvals().unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].request.id.as_str(), "new");
    assert_eq!(
        state.decide_approval(&approval_id("old"), ApprovalOutcome::Rejected, 0, 0),
        Ok(ApprovalResponse::Unknown)
    );
}

#[test]
fn durable_events_use_session_envelopes_and_typed_json_data() {
    let state = InteractionState::new(limits()).unwrap();
    let asked = state.open_approval(approval("approval"), 7, 1234).unwrap();
    let value = serde_json::to_value(&asked).unwrap();
    assert_eq!(
        value,
        json!({
            "type": "approval/asked",
            "data": {
                "id": "approval",
                "toolName": "shell",
                "callId": "call-approval",
                "reason": "needs permission"
            },
            "seq": 7,
            "time": 1234
        })
    );
    let session = asked.to_session_event().unwrap();
    assert_eq!(session.seq(), Some(7));
    assert_eq!(session.time(), Some(1234));
    assert_eq!(
        dsh_interactions::DurableEvent::from_session_event(&session)
            .unwrap()
            .unwrap()
            .payload()
            .unwrap(),
        asked.payload().unwrap()
    );

    let question = state
        .open_question(
            question_request("question-call", &["q1"]),
            QuestionState::Open,
            8,
            1234,
        )
        .unwrap();
    let value = serde_json::to_value(&question).unwrap();
    assert_eq!(value["data"]["callId"], "question-call");
    assert_eq!(value["data"]["questions"][0]["multiSelect"], false);
    assert_eq!(value["data"]["state"], "open");
    assert!(matches!(
        question.payload().unwrap(),
        InteractionPayload::QuestionAsked(_)
    ));
}
