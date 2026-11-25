mod common;

use common::{answers, approval, limits, question_request};
use dsh_interactions::{
    ApprovalOutcome, ApprovalResponse, InteractionState, QuestionResponse, QuestionState,
};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn concurrent_approval_responders_have_exactly_one_decision_slot() {
    const RESPONDERS: usize = 24;
    let state = Arc::new(InteractionState::new(limits()).unwrap());
    let request = approval("shared");
    let id = request.id.clone();
    state.open_approval(request, 0, 0).unwrap();
    let barrier = Arc::new(Barrier::new(RESPONDERS + 1));
    let mut workers = Vec::new();
    for index in 0..RESPONDERS {
        let state = Arc::clone(&state);
        let id = id.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            state.decide_approval(
                &id,
                if index % 2 == 0 {
                    ApprovalOutcome::AllowedOnce
                } else {
                    ApprovalOutcome::Rejected
                },
                100 + index as u64,
                1,
            )
        }));
    }
    barrier.wait();
    let mut accepted = 0;
    let mut late = 0;
    for worker in workers {
        match worker.join().unwrap().unwrap() {
            ApprovalResponse::Accepted(_) => accepted += 1,
            ApprovalResponse::AlreadySettled => late += 1,
            other => panic!("unexpected approval response: {other:?}"),
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(late, RESPONDERS - 1);
    assert!(state.pending_approvals().unwrap().is_empty());
    assert_eq!(state.recent_approvals().unwrap().len(), 1);
}

#[test]
fn concurrent_question_responders_accept_one_complete_batch() {
    const RESPONDERS: usize = 24;
    let state = Arc::new(InteractionState::new(limits()).unwrap());
    let request = question_request("shared", &["q1", "q2"]);
    let id = request.call_id.clone();
    state
        .open_question(request, QuestionState::Open, 0, 0)
        .unwrap();
    let barrier = Arc::new(Barrier::new(RESPONDERS + 1));
    let mut workers = Vec::new();
    for index in 0..RESPONDERS {
        let state = Arc::clone(&state);
        let id = id.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            let ids = if index % 2 == 0 {
                ["q1", "q2"]
            } else {
                ["q2", "q1"]
            };
            state.submit_answers(&id, answers(&ids), 100 + index as u64, 1)
        }));
    }
    barrier.wait();
    let mut accepted = 0;
    let mut late = 0;
    for worker in workers {
        match worker.join().unwrap().unwrap() {
            QuestionResponse::Answered { .. } => accepted += 1,
            QuestionResponse::AlreadySettled => late += 1,
            other => panic!("unexpected question response: {other:?}"),
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(late, RESPONDERS - 1);
    assert!(state.pending_questions().unwrap().is_empty());
    assert_eq!(state.recent_questions().unwrap().len(), 1);
}
