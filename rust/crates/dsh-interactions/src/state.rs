use crate::event::{validate_session_integer, DurableEvent, InteractionPayload};
use crate::types::{
    ApprovalOutcome, ApprovalRequest, ApprovalRequestId, InteractionLimits, PendingApproval,
    PendingQuestion, QuestionAnswer, QuestionState, SettledApproval, SettledQuestion, ToolCallId,
    UserQuestion, UserQuestionAnswer, UserQuestionRequest,
};
use crate::{InteractionError, InteractionSnapshot};
use dsh_session_format::Event as SessionEvent;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

/// An accepted approval response and the durable record to append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalDecision {
    pub event: DurableEvent,
    pub settled: SettledApproval,
}

/// Result of an approval response. Only `Accepted` creates a durable decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalResponse {
    Accepted(Box<ApprovalDecision>),
    AlreadySettled,
    Unknown,
}

/// Result of a question response or foreground continuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuestionResponse {
    Answered {
        event: DurableEvent,
        settled: SettledQuestion,
    },
    Continued {
        event: DurableEvent,
        question: PendingQuestion,
    },
    AlreadyContinued,
    AlreadySettled,
    Unknown,
}

/// Whether replay changed state or ignored a duplicate/late record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayOutcome {
    Applied,
    Ignored,
}

#[derive(Debug)]
struct Inner {
    pending_approvals: BTreeMap<ApprovalRequestId, PendingApproval>,
    recent_approvals: BTreeMap<ApprovalRequestId, SettledApproval>,
    pending_questions: BTreeMap<ToolCallId, PendingQuestion>,
    recent_questions: BTreeMap<ToolCallId, SettledQuestion>,
    last_seq: Option<u64>,
    last_time: Option<u64>,
}

/// Thread-safe owner of approval and question state. Persist only snapshots or durable events.
///
/// Callers assign ids that remain unique for the Session lifetime. Retained settlements are
/// bounded, so a response for an evicted id is reported as `Unknown`, not as a late response.
#[derive(Debug)]
pub struct InteractionState {
    limits: InteractionLimits,
    inner: Mutex<Inner>,
}

impl InteractionState {
    /// Create an empty owner with explicit deployment bounds.
    pub fn new(limits: InteractionLimits) -> Result<Self, InteractionError> {
        limits.validate()?;
        Ok(Self {
            limits,
            inner: Mutex::new(Inner {
                pending_approvals: BTreeMap::new(),
                recent_approvals: BTreeMap::new(),
                pending_questions: BTreeMap::new(),
                recent_questions: BTreeMap::new(),
                last_seq: None,
                last_time: None,
            }),
        })
    }

    /// Restore a validated snapshot. Runtime timers, claims, and queues are never reconstructed.
    pub fn from_snapshot(
        snapshot: InteractionSnapshot,
        limits: InteractionLimits,
    ) -> Result<Self, InteractionError> {
        if snapshot.version != crate::SNAPSHOT_VERSION {
            return Err(InteractionError::validation(format!(
                "unsupported interaction snapshot version {}",
                snapshot.version
            )));
        }
        let state = Self::new(limits)?;
        let mut inner = state.lock()?;
        inner.last_seq = snapshot.last_seq;
        inner.last_time = snapshot.last_time;
        if let Some(seq) = inner.last_seq {
            validate_session_integer(seq, "lastSeq")?;
        }
        if let Some(time) = inner.last_time {
            validate_session_integer(time, "lastTime")?;
        }
        if inner.last_seq.is_some() != inner.last_time.is_some() {
            return Err(InteractionError::validation(
                "snapshot lastSeq and lastTime must either both be present or both be absent",
            ));
        }
        let last_seq = match inner.last_seq {
            Some(seq) => seq,
            None if snapshot.approvals.is_empty() && snapshot.questions.is_empty() => 0,
            None => {
                return Err(InteractionError::validation(
                    "nonempty snapshot is missing lastSeq",
                ));
            }
        };

        let mut sequences = BTreeSet::new();
        for record in snapshot.approvals {
            match record {
                crate::ApprovalRecord::Pending(pending) => {
                    validate_approval_request(&pending.request)?;
                    validate_record_seq(&mut sequences, pending.asked_seq, last_seq)?;
                    if inner.pending_approvals.contains_key(&pending.request.id)
                        || inner.recent_approvals.contains_key(&pending.request.id)
                    {
                        return Err(InteractionError::validation(format!(
                            "duplicate approval id {} in snapshot",
                            pending.request.id
                        )));
                    }
                    inner
                        .pending_approvals
                        .insert(pending.request.id.clone(), pending);
                }
                crate::ApprovalRecord::Settled(settled) => {
                    validate_approval_request(&settled.request)?;
                    validate_record_seq(&mut sequences, settled.decided_seq, last_seq)?;
                    if inner.pending_approvals.contains_key(&settled.request.id)
                        || inner.recent_approvals.contains_key(&settled.request.id)
                    {
                        return Err(InteractionError::validation(format!(
                            "duplicate approval id {} in snapshot",
                            settled.request.id
                        )));
                    }
                    inner
                        .recent_approvals
                        .insert(settled.request.id.clone(), settled);
                }
            }
        }
        for record in snapshot.questions {
            match record {
                crate::QuestionRecord::Pending(pending) => {
                    validate_question_request(&pending.questions)?;
                    validate_record_seq(&mut sequences, pending.asked_seq, last_seq)?;
                    if inner.pending_questions.contains_key(&pending.call_id)
                        || inner.recent_questions.contains_key(&pending.call_id)
                    {
                        return Err(InteractionError::validation(format!(
                            "duplicate question call id {} in snapshot",
                            pending.call_id
                        )));
                    }
                    inner
                        .pending_questions
                        .insert(pending.call_id.clone(), pending);
                }
                crate::QuestionRecord::Settled(settled) => {
                    validate_answer_shape(&settled.answers)?;
                    validate_record_seq(&mut sequences, settled.settled_seq, last_seq)?;
                    if inner.pending_questions.contains_key(&settled.call_id)
                        || inner.recent_questions.contains_key(&settled.call_id)
                    {
                        return Err(InteractionError::validation(format!(
                            "duplicate question call id {} in snapshot",
                            settled.call_id
                        )));
                    }
                    inner
                        .recent_questions
                        .insert(settled.call_id.clone(), settled);
                }
            }
        }
        if inner.pending_approvals.len() > limits.max_pending_approvals
            || inner.pending_questions.len() > limits.max_pending_questions
            || inner.recent_approvals.len() > limits.max_recent_approvals
            || inner.recent_questions.len() > limits.max_recent_questions
        {
            return Err(InteractionError::validation(
                "snapshot exceeds the configured interaction bounds",
            ));
        }
        drop(inner);
        Ok(state)
    }

    /// Open an approval and return the `approval/asked` record to append durably.
    pub fn open_approval(
        &self,
        request: ApprovalRequest,
        seq: u64,
        time: u64,
    ) -> Result<DurableEvent, InteractionError> {
        validate_approval_request(&request)?;
        let event = DurableEvent::new(
            InteractionPayload::ApprovalAsked(request.clone().into()),
            seq,
            time,
        )?;
        let mut inner = self.lock()?;
        validate_next_position(&inner, seq, time)?;
        if inner.pending_approvals.contains_key(&request.id)
            || inner.recent_approvals.contains_key(&request.id)
        {
            return Err(InteractionError::validation(format!(
                "duplicate approval request id {}",
                request.id
            )));
        }
        if inner.pending_approvals.len() >= self.limits.max_pending_approvals {
            return Err(InteractionError::capacity(
                "pending approvals",
                self.limits.max_pending_approvals,
            ));
        }
        inner.pending_approvals.insert(
            request.id.clone(),
            PendingApproval {
                asked_seq: seq,
                request,
            },
        );
        inner.last_seq = Some(seq);
        inner.last_time = Some(time);
        Ok(event)
    }

    /// Claim the approval's single decision slot. Duplicate and late answers do not mutate state.
    pub fn decide_approval(
        &self,
        id: &ApprovalRequestId,
        outcome: ApprovalOutcome,
        seq: u64,
        time: u64,
    ) -> Result<ApprovalResponse, InteractionError> {
        if id.as_str().is_empty() {
            return Err(InteractionError::validation("approval request id is empty"));
        }
        let event = DurableEvent::new(
            InteractionPayload::ApprovalDecided(crate::ApprovalDecided {
                id: id.clone(),
                outcome,
            }),
            seq,
            time,
        )?;
        let mut inner = self.lock()?;
        if inner.recent_approvals.contains_key(id) {
            return Ok(ApprovalResponse::AlreadySettled);
        }
        if !inner.pending_approvals.contains_key(id) {
            return Ok(ApprovalResponse::Unknown);
        }
        validate_next_position(&inner, seq, time)?;
        let Some(pending) = inner.pending_approvals.remove(id) else {
            return Ok(ApprovalResponse::Unknown);
        };
        let settled = SettledApproval {
            decided_seq: seq,
            request: pending.request,
            outcome,
        };
        insert_recent(
            &mut inner.recent_approvals,
            id.clone(),
            settled.clone(),
            self.limits.max_recent_approvals,
            |record| record.decided_seq,
        );
        inner.last_seq = Some(seq);
        inner.last_time = Some(time);
        Ok(ApprovalResponse::Accepted(Box::new(ApprovalDecision {
            event,
            settled,
        })))
    }

    /// Open a timed question. `Open` permits a foreground answer; `Continued` has no deadline.
    pub fn open_question(
        &self,
        request: UserQuestionRequest,
        state: QuestionState,
        seq: u64,
        time: u64,
    ) -> Result<DurableEvent, InteractionError> {
        validate_question_request(&request.questions)?;
        let event = DurableEvent::new(
            InteractionPayload::QuestionAsked(crate::QuestionAsked {
                call_id: request.call_id.clone(),
                questions: request.questions.clone(),
                state,
            }),
            seq,
            time,
        )?;
        let mut inner = self.lock()?;
        validate_next_position(&inner, seq, time)?;
        if inner.pending_questions.contains_key(&request.call_id)
            || inner.recent_questions.contains_key(&request.call_id)
        {
            return Err(InteractionError::validation(format!(
                "duplicate question call id {}",
                request.call_id
            )));
        }
        if inner.pending_questions.len() >= self.limits.max_pending_questions {
            return Err(InteractionError::capacity(
                "pending questions",
                self.limits.max_pending_questions,
            ));
        }
        inner.pending_questions.insert(
            request.call_id.clone(),
            PendingQuestion {
                asked_seq: seq,
                call_id: request.call_id,
                questions: request.questions,
                state,
            },
        );
        inner.last_seq = Some(seq);
        inner.last_time = Some(time);
        Ok(event)
    }

    /// Mark a foreground question continued. The caller owns the clock; continued work never expires here.
    pub fn continue_question(
        &self,
        call_id: &ToolCallId,
        seq: u64,
        time: u64,
    ) -> Result<QuestionResponse, InteractionError> {
        if call_id.as_str().is_empty() {
            return Err(InteractionError::validation("tool call id is empty"));
        }
        let event = DurableEvent::new(
            InteractionPayload::QuestionContinued(crate::QuestionContinued {
                call_id: call_id.clone(),
            }),
            seq,
            time,
        )?;
        let mut inner = self.lock()?;
        if inner.recent_questions.contains_key(call_id) {
            return Ok(QuestionResponse::AlreadySettled);
        }
        let Some(pending) = inner.pending_questions.get(call_id) else {
            return Ok(QuestionResponse::Unknown);
        };
        if pending.state == QuestionState::Continued {
            return Ok(QuestionResponse::AlreadyContinued);
        }
        validate_next_position(&inner, seq, time)?;
        let question = PendingQuestion {
            state: QuestionState::Continued,
            ..pending.clone()
        };
        inner
            .pending_questions
            .insert(call_id.clone(), question.clone());
        inner.last_seq = Some(seq);
        inner.last_time = Some(time);
        Ok(QuestionResponse::Continued { event, question })
    }

    /// Settle a question with exactly one answer per requested question.
    pub fn submit_answers(
        &self,
        call_id: &ToolCallId,
        answer: UserQuestionAnswer,
        seq: u64,
        time: u64,
    ) -> Result<QuestionResponse, InteractionError> {
        if call_id.as_str().is_empty() {
            return Err(InteractionError::validation("tool call id is empty"));
        }
        let event = DurableEvent::new(
            InteractionPayload::QuestionSettled(crate::QuestionSettled {
                call_id: call_id.clone(),
                answers: answer.answers.clone(),
            }),
            seq,
            time,
        )?;
        let mut inner = self.lock()?;
        if inner.recent_questions.contains_key(call_id) {
            return Ok(QuestionResponse::AlreadySettled);
        }
        let Some(pending) = inner.pending_questions.get(call_id) else {
            return Ok(QuestionResponse::Unknown);
        };
        validate_answer_coverage(&pending.questions, &answer.answers)?;
        validate_next_position(&inner, seq, time)?;
        let settled = SettledQuestion {
            settled_seq: seq,
            call_id: call_id.clone(),
            answers: answer.answers,
        };
        inner.pending_questions.remove(call_id);
        insert_recent(
            &mut inner.recent_questions,
            call_id.clone(),
            settled.clone(),
            self.limits.max_recent_questions,
            |record| record.settled_seq,
        );
        inner.last_seq = Some(seq);
        inner.last_time = Some(time);
        Ok(QuestionResponse::Answered { event, settled })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, InteractionError> {
        self.inner.lock().map_err(|_| InteractionError::Poisoned)
    }

    /// Pending approvals ordered by opaque id.
    pub fn pending_approvals(&self) -> Result<Vec<PendingApproval>, InteractionError> {
        Ok(self.lock()?.pending_approvals.values().cloned().collect())
    }

    /// Retained settled approvals ordered by opaque id.
    pub fn recent_approvals(&self) -> Result<Vec<SettledApproval>, InteractionError> {
        Ok(self.lock()?.recent_approvals.values().cloned().collect())
    }

    /// Pending questions ordered by tool-call id.
    pub fn pending_questions(&self) -> Result<Vec<PendingQuestion>, InteractionError> {
        Ok(self.lock()?.pending_questions.values().cloned().collect())
    }

    /// Retained settled questions ordered by tool-call id.
    pub fn recent_questions(&self) -> Result<Vec<SettledQuestion>, InteractionError> {
        Ok(self.lock()?.recent_questions.values().cloned().collect())
    }

    /// Capture bounded state without locks, clocks, channels, or other runtime resources.
    pub fn snapshot(&self) -> Result<InteractionSnapshot, InteractionError> {
        let inner = self.lock()?;
        Ok(InteractionSnapshot {
            version: crate::SNAPSHOT_VERSION,
            last_seq: inner.last_seq,
            last_time: inner.last_time,
            approvals: inner
                .pending_approvals
                .values()
                .cloned()
                .map(crate::ApprovalRecord::Pending)
                .chain(
                    inner
                        .recent_approvals
                        .values()
                        .cloned()
                        .map(crate::ApprovalRecord::Settled),
                )
                .collect(),
            questions: inner
                .pending_questions
                .values()
                .cloned()
                .map(crate::QuestionRecord::Pending)
                .chain(
                    inner
                        .recent_questions
                        .values()
                        .cloned()
                        .map(crate::QuestionRecord::Settled),
                )
                .collect(),
        })
    }

    /// Apply one typed durable record in append order. Duplicate/late records are ignored.
    pub fn replay(&self, event: DurableEvent) -> Result<ReplayOutcome, InteractionError> {
        let payload = event.payload()?;
        let mut inner = self.lock()?;
        validate_next_position(&inner, event.seq(), event.time())?;
        let outcome = match payload {
            InteractionPayload::ApprovalAsked(asked) => {
                let request = ApprovalRequest {
                    id: asked.id,
                    tool_name: asked.tool_name,
                    call_id: asked.call_id,
                    reason: asked.reason,
                };
                validate_approval_request(&request)?;
                if inner.pending_approvals.contains_key(&request.id)
                    || inner.recent_approvals.contains_key(&request.id)
                {
                    ReplayOutcome::Ignored
                } else {
                    if inner.pending_approvals.len() >= self.limits.max_pending_approvals {
                        return Err(InteractionError::capacity(
                            "pending approvals",
                            self.limits.max_pending_approvals,
                        ));
                    }
                    inner.pending_approvals.insert(
                        request.id.clone(),
                        PendingApproval {
                            asked_seq: event.seq(),
                            request,
                        },
                    );
                    ReplayOutcome::Applied
                }
            }
            InteractionPayload::ApprovalDecided(decided) => {
                if decided.id.as_str().is_empty() {
                    return Err(InteractionError::validation("approval request id is empty"));
                }
                if let Some(pending) = inner.pending_approvals.remove(&decided.id) {
                    insert_recent(
                        &mut inner.recent_approvals,
                        decided.id,
                        SettledApproval {
                            decided_seq: event.seq(),
                            request: pending.request,
                            outcome: decided.outcome,
                        },
                        self.limits.max_recent_approvals,
                        |record| record.decided_seq,
                    );
                    ReplayOutcome::Applied
                } else {
                    ReplayOutcome::Ignored
                }
            }
            InteractionPayload::QuestionAsked(asked) => {
                validate_question_request(&asked.questions)?;
                if inner.pending_questions.contains_key(&asked.call_id)
                    || inner.recent_questions.contains_key(&asked.call_id)
                {
                    ReplayOutcome::Ignored
                } else {
                    if inner.pending_questions.len() >= self.limits.max_pending_questions {
                        return Err(InteractionError::capacity(
                            "pending questions",
                            self.limits.max_pending_questions,
                        ));
                    }
                    inner.pending_questions.insert(
                        asked.call_id.clone(),
                        PendingQuestion {
                            asked_seq: event.seq(),
                            call_id: asked.call_id,
                            questions: asked.questions,
                            state: asked.state,
                        },
                    );
                    ReplayOutcome::Applied
                }
            }
            InteractionPayload::QuestionContinued(continued) => {
                if continued.call_id.as_str().is_empty() {
                    return Err(InteractionError::validation("tool call id is empty"));
                }
                if let Some(pending) = inner.pending_questions.get_mut(&continued.call_id) {
                    if pending.state == QuestionState::Open {
                        pending.state = QuestionState::Continued;
                        ReplayOutcome::Applied
                    } else {
                        ReplayOutcome::Ignored
                    }
                } else {
                    ReplayOutcome::Ignored
                }
            }
            InteractionPayload::QuestionSettled(settled) => {
                if settled.call_id.as_str().is_empty() {
                    return Err(InteractionError::validation("tool call id is empty"));
                }
                validate_answer_shape(&settled.answers)?;
                if let Some(pending) = inner.pending_questions.get(&settled.call_id) {
                    validate_answer_coverage(&pending.questions, &settled.answers)?;
                    inner.pending_questions.remove(&settled.call_id);
                    insert_recent(
                        &mut inner.recent_questions,
                        settled.call_id.clone(),
                        SettledQuestion {
                            settled_seq: event.seq(),
                            call_id: settled.call_id,
                            answers: settled.answers,
                        },
                        self.limits.max_recent_questions,
                        |record| record.settled_seq,
                    );
                    ReplayOutcome::Applied
                } else {
                    ReplayOutcome::Ignored
                }
            }
        };
        inner.last_seq = Some(event.seq());
        inner.last_time = Some(event.time());
        Ok(outcome)
    }

    /// Replay a known interaction row from the generic Session format.
    pub fn replay_session_event(
        &self,
        event: &SessionEvent,
    ) -> Result<Option<ReplayOutcome>, InteractionError> {
        match DurableEvent::from_session_event(event)? {
            Some(event) => self.replay(event).map(Some),
            None => Ok(None),
        }
    }
}

fn insert_recent<K: Ord + Clone, V>(
    records: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    limit: usize,
    sequence: impl Fn(&V) -> u64,
) {
    if limit == 0 {
        return;
    }
    records.insert(key, value);
    if records.len() > limit {
        let mut ordered: Vec<(u64, K)> = records
            .iter()
            .map(|(key, record)| (sequence(record), key.clone()))
            .collect();
        ordered.sort_by_key(|(seq, _)| *seq);
        let excess = ordered.len() - limit;
        for (_, key) in ordered.into_iter().take(excess) {
            records.remove(&key);
        }
    }
}

fn validate_next_position(inner: &Inner, seq: u64, time: u64) -> Result<(), InteractionError> {
    validate_session_integer(seq, "seq")?;
    validate_session_integer(time, "time")?;
    if let Some(last) = inner.last_seq {
        if seq <= last {
            return Err(InteractionError::validation(format!(
                "interaction event seq {seq} is not greater than {last}"
            )));
        }
    }
    if let Some(last) = inner.last_time {
        if time < last {
            return Err(InteractionError::validation(format!(
                "interaction event time {time} precedes {last}"
            )));
        }
    }
    Ok(())
}

fn validate_record_seq(
    sequences: &mut BTreeSet<u64>,
    seq: u64,
    last_seq: u64,
) -> Result<(), InteractionError> {
    validate_session_integer(seq, "record seq")?;
    if seq > last_seq {
        return Err(InteractionError::validation(
            "snapshot record sequence follows lastSeq",
        ));
    }
    if !sequences.insert(seq) {
        return Err(InteractionError::validation(format!(
            "duplicate record sequence {seq} in snapshot"
        )));
    }
    Ok(())
}

fn validate_approval_request(request: &ApprovalRequest) -> Result<(), InteractionError> {
    if request.id.as_str().is_empty() || request.tool_name.is_empty() {
        return Err(InteractionError::validation(
            "approval request id and toolName must be nonempty",
        ));
    }
    if request
        .call_id
        .as_ref()
        .is_some_and(|id| id.as_str().is_empty())
    {
        return Err(InteractionError::validation("approval call id is empty"));
    }
    Ok(())
}

fn validate_question_request(questions: &[UserQuestion]) -> Result<(), InteractionError> {
    if questions.is_empty() {
        return Err(InteractionError::validation("question batch is empty"));
    }
    let mut ids = BTreeSet::new();
    for question in questions {
        if question.id.is_empty() || question.question.is_empty() {
            return Err(InteractionError::validation(
                "question id and question text must be nonempty",
            ));
        }
        if !ids.insert(question.id.as_str()) {
            return Err(InteractionError::validation(format!(
                "duplicate question id {}",
                question.id
            )));
        }
    }
    Ok(())
}

fn validate_answer_shape(answers: &[QuestionAnswer]) -> Result<(), InteractionError> {
    let mut ids = BTreeSet::new();
    for answer in answers {
        if answer.id.is_empty() {
            return Err(InteractionError::validation("answer question id is empty"));
        }
        if !ids.insert(answer.id.as_str()) {
            return Err(InteractionError::validation(format!(
                "duplicate answer question id {}",
                answer.id
            )));
        }
    }
    Ok(())
}

fn validate_answer_coverage(
    questions: &[UserQuestion],
    answers: &[QuestionAnswer],
) -> Result<(), InteractionError> {
    validate_answer_shape(answers)?;
    let requested: BTreeSet<&str> = questions.iter().map(|item| item.id.as_str()).collect();
    let answered: BTreeSet<&str> = answers.iter().map(|item| item.id.as_str()).collect();
    if requested != answered || questions.len() != answers.len() {
        return Err(InteractionError::validation(
            "answer batch must name every requested question exactly once",
        ));
    }
    Ok(())
}
