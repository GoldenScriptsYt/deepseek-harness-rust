//! Host-side interactions: the `$events` waterfall that carries questions and
//! approvals to a Client, the timed waits a continuing question leaves behind,
//! and the in-process registry headless runs answer through.

use dsh_core::{BoxFuture, Cancel};
use dsh_gateway::{Broadcaster, Gateway};
use dsh_tools::{
    ApprovalOutcome, ApprovalPolicy, ApprovalRequest, Approver, AskOutcome, AskUserQuestion,
    ExitPlanMode, QuestionAsker, QuestionRequest,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Remote Event that asks a Client one question set.
pub const QUESTION_EVENT: &str = "user-questions/request";
/// Remote Event that asks a Client to allow one tool call.
pub const APPROVAL_EVENT: &str = "approval/request";
/// Code a Client rejects a timed question with at its own deadline.
pub const TIMED_OUT_CODE: &str = "ASK_TIMED_OUT";
/// How often a timed wait re-reads its remaining budget.
const TIMER_TICK: Duration = Duration::from_millis(20);

struct Pending {
    questions: Value,
    answer: Option<tokio::sync::oneshot::Sender<Value>>,
}

/// Process-wide question registry the Host installs at startup.
#[derive(Default)]
pub struct InteractionRegistry {
    pending: Mutex<HashMap<String, Pending>>,
    next: Mutex<u64>,
}

impl InteractionRegistry {
    /// Reserve an id and register one pending question set.
    /// @param questions - The validated `questions` array.
    /// @returns The minted call id and the receiver that resolves with the answer.
    fn reserve(&self, questions: Value) -> (String, tokio::sync::oneshot::Receiver<Value>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let id = {
            let mut next = self.next.lock().unwrap_or_else(|error| error.into_inner());
            *next += 1;
            format!("question-{next}")
        };
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                id.clone(),
                Pending {
                    questions,
                    answer: Some(tx),
                },
            );
        (id, rx)
    }

    /// Answer one pending question set.
    /// @param id - Call id from the pending row.
    /// @param answer - Client answer payload.
    /// @returns `()` when the blocked call was released, or a readable reason.
    pub fn answer(&self, id: &str, answer: Value) -> Result<(), String> {
        let sender = {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let row = pending
                .get_mut(id)
                .ok_or_else(|| format!("no pending question {id}"))?;
            row.answer
                .take()
                .ok_or_else(|| format!("question {id} was already answered"))?
        };
        if sender.send(answer).is_err() {
            return Err(format!("question {id} is no longer waiting"));
        }
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(id);
        Ok(())
    }

    /// Every question still waiting for an answer.
    /// @returns `{questions:[...]}` rows keyed by call id.
    pub fn pending(&self) -> Vec<Value> {
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut rows: Vec<Value> = pending
            .iter()
            .map(|(id, row)| json!({"callId": id, "questions": row.questions}))
            .collect();
        rows.sort_by(|left, right| left["callId"].as_str().cmp(&right["callId"].as_str()));
        rows
    }

    /// Drop one pending question, for example when its turn is cancelled.
    /// @param id - Call id to retire.
    pub fn retire(&self, id: &str) {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(id);
    }

    /// Ask one question set and wait for the answer.
    /// @param request - Questions, cancellation, and the call that asks.
    /// @returns The answer payload, or why the question ended without one.
    async fn ask(&self, request: QuestionRequest<'_>) -> Result<AskOutcome, String> {
        let questions = Value::Array(request.questions.to_vec());
        let (id, rx) = self.reserve(questions);
        let cancelled = async {
            request.cancel.cancelled().await;
        };
        tokio::select! {
            answered = rx => {
                self.retire(&id);
                answered
                    .map(AskOutcome::Answered)
                    .map_err(|_| "the user did not answer the question".to_string())
            }
            () = cancelled => {
                self.retire(&id);
                Err("the question was cancelled before it was answered".to_string())
            }
        }
    }
}

/// How one Client answered a forwarded waterfall.
#[derive(Debug, Clone, PartialEq)]
pub enum WaterfallOutcome {
    /// The Client handled the request and returned this value.
    Value(Value),
    /// The Client delegated back to the Host chain.
    Next,
    /// The Client rejected the request with a readable message.
    Rejected {
        /// Message the Client reported.
        message: String,
        /// Optional machine code, such as the timed-question deadline.
        code: Option<String>,
    },
}

/// One timed question set the Host is still waiting on.
struct Wait {
    /// Session the question belongs to.
    agent: String,
    /// Assistant tool call that asked.
    call_id: String,
    /// The validated `questions` array the Client renders.
    questions: Value,
    /// Remaining budget while no Client holds the card.
    remaining: Duration,
    /// When the current unpaused stretch began.
    ticking_since: Option<Instant>,
    /// Open cards that paused the Host timer.
    claims: usize,
}

impl Wait {
    /// Budget left right now.
    fn remaining(&self) -> Duration {
        match self.ticking_since {
            Some(since) => self.remaining.saturating_sub(since.elapsed()),
            None => self.remaining,
        }
    }

    /// Remaining milliseconds as the wire reports them.
    fn remaining_ms(&self) -> u64 {
        let left = self.remaining();
        if left.is_zero() {
            0
        } else {
            u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
        }
    }
}

/// Host-to-Client waterfall carrier over the `$events` downlink.
///
/// A question blocks its tool call, so the request must reach the Client while
/// the call is still open. The Client answers through `$events/result`, keyed by
/// the event id published here.
pub struct WaterfallHub {
    events: Broadcaster,
    pending: Mutex<HashMap<String, tokio::sync::oneshot::Sender<WaterfallOutcome>>>,
    waits: Mutex<HashMap<String, Wait>>,
    next: AtomicU64,
}

impl WaterfallHub {
    /// Build the hub over the Gateway's downlink publisher.
    /// @param events - The `$events` broadcaster.
    #[must_use]
    pub fn new(events: Broadcaster) -> Self {
        Self {
            events,
            pending: Mutex::new(HashMap::new()),
            waits: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }
    }

    /// Stable key of one Agent call.
    fn key(agent: &str, call_id: &str) -> String {
        format!("{agent}{}{call_id}", '\0')
    }

    /// Publish one waterfall and hand back the id the Client must echo.
    /// @param event - Remote Event name, such as `user-questions/request`.
    /// @param agent - Agent id that scopes the request.
    /// @param request - Request fields the Client receives.
    /// @returns The published event id and the receiver the answer settles.
    pub(crate) fn publish(
        &self,
        event: &str,
        agent: &str,
        request: Value,
    ) -> (String, tokio::sync::oneshot::Receiver<WaterfallOutcome>) {
        let event_id = format!(
            "remote-{}-{}",
            std::process::id(),
            self.next.fetch_add(1, Ordering::Relaxed)
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(event_id.clone(), tx);
        self.events.publish(json!({
            "type": "waterfall",
            "event": event,
            "eventId": event_id,
            "agentId": agent,
            "request": request,
        }));
        (event_id, rx)
    }

    /// Withdraw one published waterfall nobody will answer.
    /// @param event_id - Event id to retire.
    fn abandon(&self, event_id: &str) {
        self.events
            .publish(json!({ "type": "cancel", "eventId": event_id }));
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(event_id);
    }

    /// Ask one question set and wait for the Client's answer.
    ///
    /// A timed ask records the wait before publishing, so `userQuestions/attachWait`
    /// can count it down and a later Client can still answer it. Reaching the
    /// deadline cancels the card and reports `AskOutcome::Pending`.
    /// @param agent - Session the question belongs to.
    /// @param questions - The validated `questions` array.
    /// @param call_id - Assistant tool call that asks.
    /// @param timeout - Budget, or `None` to wait indefinitely.
    /// @param cancel - Cancellation of the calling turn.
    /// @returns The answer, a pending marker, or why the question ended.
    pub async fn ask(
        &self,
        agent: &str,
        questions: Value,
        call_id: &str,
        timeout: Option<Duration>,
        cancel: &Cancel,
    ) -> Result<AskOutcome, String> {
        let mut request = json!({ "questions": questions });
        if !call_id.is_empty() {
            let mut wait = json!({ "callId": call_id });
            let timed = timeout.is_some();
            if timed {
                wait["timed"] = json!(true);
            }
            request["wait"] = wait;
        }
        let key = Self::key(agent, call_id);
        let (event_id, mut rx) = self.publish(QUESTION_EVENT, agent, request);
        if let Some(limit) = timeout {
            self.open_wait(agent, call_id, questions, limit);
        }
        let mut ticker = tokio::time::interval(TIMER_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if self.remaining_ms(agent, call_id) == Some(0) {
                self.close_wait(&key);
                self.abandon(&event_id);
                return Ok(AskOutcome::Pending);
            }
            tokio::select! {
                answer = &mut rx => {
                    self.close_wait(&key);
                    return match answer {
                        Ok(WaterfallOutcome::Value(value)) => Ok(AskOutcome::Answered(value)),
                        Ok(WaterfallOutcome::Rejected { code, .. })
                            if code.as_deref() == Some(TIMED_OUT_CODE) => Ok(AskOutcome::Pending),
                        Ok(WaterfallOutcome::Rejected { message, .. }) => Err(message),
                        Ok(WaterfallOutcome::Next) => {
                            Err("no Client answered the question".to_string())
                        }
                        Err(_) => {
                            Err("the Client disconnected before answering".to_string())
                        }
                    };
                }
                () = cancel.cancelled() => {
                    self.close_wait(&key);
                    self.abandon(&event_id);
                    return Err("the question was cancelled before it was answered".to_string());
                }
                _ = ticker.tick() => {}
            }
        }
    }

    /// Record one timed wait before the Client can see the card.
    fn open_wait(&self, agent: &str, call_id: &str, questions: Value, limit: Duration) {
        if call_id.is_empty() {
            return;
        }
        self.waits
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                Self::key(agent, call_id),
                Wait {
                    agent: agent.to_string(),
                    call_id: call_id.to_string(),
                    questions,
                    remaining: limit,
                    ticking_since: Some(Instant::now()),
                    claims: 0,
                },
            );
    }

    /// Forget one wait, settling its remaining budget as of now.
    fn close_wait(&self, key: &str) {
        let mut waits = self.waits.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(mut wait) = waits.remove(key) {
            wait.remaining = wait.remaining();
        }
    }

    /// Remaining budget of one timed wait.
    /// @param agent - Session the question belongs to.
    /// @param call_id - Assistant tool call that asked.
    /// @returns Remaining milliseconds, or `None` when no wait is open.
    #[must_use]
    pub fn remaining_ms(&self, agent: &str, call_id: &str) -> Option<u64> {
        let waits = self.waits.lock().unwrap_or_else(|error| error.into_inner());
        waits
            .get(&Self::key(agent, call_id))
            .map(Wait::remaining_ms)
    }

    /// Whether one timed wait is still open.
    /// @param agent - Session the question belongs to.
    /// @param call_id - Assistant tool call that asked.
    /// @returns True while a Client may still answer that call.
    #[must_use]
    pub fn is_waiting(&self, agent: &str, call_id: &str) -> bool {
        self.waits
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&Self::key(agent, call_id))
    }

    /// Pause the Host timer while one Client shows the card.
    /// @param agent - Session the question belongs to.
    /// @param call_id - Assistant tool call that asked.
    /// @returns True when a wait was there to claim.
    pub fn claim(&self, agent: &str, call_id: &str) -> bool {
        let mut waits = self.waits.lock().unwrap_or_else(|error| error.into_inner());
        let Some(wait) = waits.get_mut(&Self::key(agent, call_id)) else {
            return false;
        };
        if wait.claims == 0 {
            wait.remaining = wait.remaining();
            wait.ticking_since = None;
        }
        wait.claims += 1;
        true
    }

    /// Resume the Host timer once no Client shows the card.
    /// @param agent - Session the question belongs to.
    /// @param call_id - Assistant tool call that asked.
    pub fn release(&self, agent: &str, call_id: &str) {
        let mut waits = self.waits.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(wait) = waits.get_mut(&Self::key(agent, call_id)) {
            wait.claims = wait.claims.saturating_sub(1);
            if wait.claims == 0 {
                wait.ticking_since = Some(Instant::now());
            }
        }
    }

    /// Every timed wait a Client may still answer.
    /// @returns `{callId, questions, remainingMs}` rows in ask order.
    #[must_use]
    pub fn pending_questions(&self) -> Vec<Value> {
        let waits = self.waits.lock().unwrap_or_else(|error| error.into_inner());
        waits
            .values()
            .map(|wait| {
                json!({
                    "agent": wait.agent,
                    "callId": wait.call_id,
                    "questions": wait.questions,
                    "remainingMs": wait.remaining_ms(),
                })
            })
            .collect()
    }

    /// Settle one published waterfall with a Client outcome.
    /// @param event_id - Event id the Client echoes.
    /// @param outcome - The Client's outcome.
    /// @returns `()` when a waiting call was released, or a readable reason.
    pub fn settle(&self, event_id: &str, outcome: WaterfallOutcome) -> Result<(), String> {
        let sender = {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            pending
                .remove(event_id)
                .ok_or_else(|| format!("no pending waterfall {event_id}"))?
        };
        sender
            .send(outcome)
            .map_err(|_| format!("waterfall {event_id} is no longer waiting"))
    }

    /// Ask one Client to allow one tool call.
    /// @param agent - Session the call belongs to.
    /// @param request - Tool name, call id, and what the action would touch.
    /// @param cancel - Cancellation of the calling turn.
    /// @returns What the Client decided, or `Unavailable` when nobody answered.
    async fn approve(&self, agent: &str, request: Value, cancel: &Cancel) -> ApprovalOutcome {
        let (event_id, mut rx) = self.publish(APPROVAL_EVENT, agent, request);
        let outcome: Result<WaterfallOutcome, ()> = tokio::select! {
            answer = &mut rx => answer.map_err(|_| ()),
            () = cancel.cancelled() => Err(()),
        };
        match outcome {
            Ok(WaterfallOutcome::Value(value)) => ApprovalOutcome::parse(value.as_str()),
            Ok(_) => ApprovalOutcome::Unavailable,
            Err(()) => {
                self.abandon(&event_id);
                ApprovalOutcome::Cancelled
            }
        }
    }
}

#[cfg(test)]
impl WaterfallHub {
    fn pending_is_empty(&self) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    }

    fn first_pending(&self) -> Option<String> {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .next()
            .cloned()
    }
}

/// Tool-facing asker backed by the installed registry and downlink hub.
pub struct HostQuestionAsker {
    registry: Option<Arc<InteractionRegistry>>,
    hub: Option<Arc<WaterfallHub>>,
    agent: String,
}

impl QuestionAsker for HostQuestionAsker {
    fn ask<'a>(
        &'a self,
        request: QuestionRequest<'a>,
    ) -> BoxFuture<'a, Result<AskOutcome, String>> {
        Box::pin(async move {
            if let Some(hub) = self.hub.as_ref() {
                let questions = Value::Array(request.questions.to_vec());
                return hub
                    .ask(
                        &self.agent,
                        questions,
                        request.call_id,
                        request.timeout,
                        request.cancel,
                    )
                    .await;
            }
            match self.registry.as_ref() {
                Some(registry) => registry.ask(request).await,
                None => Err("no Client can answer questions".to_string()),
            }
        })
    }
}

/// Tool-facing approver backed by the same downlink waterfall.
pub struct HostApprover {
    hub: Arc<WaterfallHub>,
    agent: String,
}

impl Approver for HostApprover {
    fn request<'a>(&'a self, request: ApprovalRequest<'a>) -> BoxFuture<'a, ApprovalOutcome> {
        Box::pin(async move {
            if request.cancel.is_cancelled() {
                return ApprovalOutcome::Cancelled;
            }
            self.hub
                .approve(
                    &self.agent,
                    json!({
                        "toolName": request.tool_name,
                        "callId": request.call_id,
                        "reason": request.reason,
                    }),
                    request.cancel,
                )
                .await
        })
    }
}

static INSTALLED: OnceLock<Arc<InteractionRegistry>> = OnceLock::new();
static HUB: OnceLock<Arc<WaterfallHub>> = OnceLock::new();

/// Install the Host's registry. Later calls keep the first installation.
/// @returns The registry the Host serves.
pub fn install() -> Arc<InteractionRegistry> {
    INSTALLED
        .get_or_init(|| Arc::new(InteractionRegistry::default()))
        .clone()
}

/// Install the registry and the downlink hub that forwards questions to Clients.
/// @param gateway - Running Gateway whose `$events` stream carries the waterfall.
/// @returns The registry the Host serves.
pub fn install_with_gateway(gateway: &Gateway) -> Arc<InteractionRegistry> {
    HUB.get_or_init(|| Arc::new(WaterfallHub::new(gateway.event_broadcaster())));
    install()
}

/// The installed registry, when the Host started one.
/// @returns The process-wide registry.
pub fn installed() -> Option<Arc<InteractionRegistry>> {
    INSTALLED.get().cloned()
}

/// The installed downlink hub, when the Host serves a web client.
/// @returns The process-wide waterfall carrier.
pub fn hub() -> Option<Arc<WaterfallHub>> {
    HUB.get().cloned()
}

/// Register the interactive tools for one Agent and install their approver.
///
/// `ask_user_question` and `exit_plan_mode` are advertised only where a Client
/// can answer: a headless run has nobody to ask, and a blocked turn there would
/// only time out. The approval seam is installed for every launch, because a
/// `never` policy rejects without a Client.
/// @param reg - Registry that receives the tools.
/// @param agent - Session id the questions are scoped to.
pub fn register_tools(reg: &mut dsh_core::ToolRegistry, agent: &str) {
    let Some(hub) = hub() else {
        return;
    };
    let asker: Arc<dyn QuestionAsker> = Arc::new(HostQuestionAsker {
        registry: installed(),
        hub: Some(hub.clone()),
        agent: agent.to_string(),
    });
    reg.register(AskUserQuestion::new(asker.clone()));
    reg.register(ExitPlanMode::new(asker));
    dsh_tools::install_approver(Arc::new(HostApprover {
        hub,
        agent: agent.to_string(),
    }));
    dsh_tools::set_default_policy(ApprovalPolicy::Ask);
}
#[cfg(test)]
mod tests {
    use super::*;

    fn hub() -> Arc<WaterfallHub> {
        Arc::new(WaterfallHub::new(Broadcaster::default()))
    }

    fn questions() -> Value {
        json!([{"id": "mode", "question": "Which mode?"}])
    }

    async fn answer_when_published(hub: &Arc<WaterfallHub>, answer: Value) {
        loop {
            if let Some(event_id) = hub.first_pending() {
                hub.settle(&event_id, WaterfallOutcome::Value(answer))
                    .expect("settled");
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn an_answer_releases_the_blocked_call() {
        let registry = Arc::new(InteractionRegistry::default());
        let questions = vec![json!({"id":"mode","question":"Which mode?"})];
        let cancel = Cancel::default();
        let waiter = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let request = QuestionRequest {
                    questions: &questions,
                    cancel: &cancel,
                    call_id: "call-1",
                    timeout: None,
                };
                registry.ask(request).await
            })
        };
        loop {
            if let Some(row) = registry.pending().first() {
                let id = row["callId"].as_str().unwrap_or_default().to_string();
                assert_eq!(row["questions"][0]["id"], json!("mode"));
                registry
                    .answer(
                        &id,
                        json!({"answers":[{"id":"mode","selected":["Standard"]}]}),
                    )
                    .expect("answer is accepted");
                break;
            }
            tokio::task::yield_now().await;
        }
        let outcome = waiter.await.expect("waiter joins").expect("answered");
        match outcome {
            AskOutcome::Answered(answer) => {
                assert_eq!(answer["answers"][0]["selected"][0], json!("Standard"));
            }
            AskOutcome::Pending => panic!("the answer is not pending"),
        }
        assert!(registry.pending().is_empty());
    }

    #[tokio::test]
    async fn answering_an_unknown_question_is_an_error() {
        let registry = InteractionRegistry::default();
        assert!(registry.answer("question-9", json!({})).is_err());
    }

    #[tokio::test]
    async fn the_hub_returns_the_client_answer() {
        let hub = hub();
        let waiter = {
            let hub = hub.clone();
            tokio::spawn(async move {
                hub.ask("session-1", questions(), "call-1", None, &Cancel::default())
                    .await
            })
        };
        answer_when_published(&hub, json!({"answers":[]})).await;
        let outcome = waiter.await.expect("joins").expect("answered");
        assert!(matches!(outcome, AskOutcome::Answered(_)));
        assert!(hub.pending_is_empty());
    }

    #[tokio::test]
    async fn the_published_request_names_the_call_and_whether_it_is_timed() {
        let events = Broadcaster::default();
        let hub = Arc::new(WaterfallHub::new(events.clone()));
        let mut feed = events.subscribe();
        let waiter = {
            let hub = hub.clone();
            tokio::spawn(async move {
                hub.ask(
                    "session-1",
                    questions(),
                    "call-7",
                    Some(Duration::from_secs(30)),
                    &Cancel::default(),
                )
                .await
            })
        };
        let frame = loop {
            let frame = feed.recv().await.expect("a frame");
            if frame["event"] == json!(QUESTION_EVENT) {
                break frame;
            }
        };
        assert_eq!(frame["agentId"], json!("session-1"));
        assert_eq!(frame["request"]["wait"]["callId"], json!("call-7"));
        assert_eq!(frame["request"]["wait"]["timed"], json!(true));
        hub.settle(
            frame["eventId"].as_str().expect("id"),
            WaterfallOutcome::Value(json!({"answers":[]})),
        )
        .expect("settled");
        assert!(waiter.await.expect("joins").is_ok());
    }

    #[tokio::test]
    async fn a_timed_question_expires_into_a_pending_marker() {
        let hub = hub();
        let started = Instant::now();
        let outcome = hub
            .ask(
                "session-1",
                questions(),
                "call-2",
                Some(Duration::from_millis(60)),
                &Cancel::default(),
            )
            .await
            .expect("expires");
        assert!(matches!(outcome, AskOutcome::Pending));
        assert!(started.elapsed() >= Duration::from_millis(60));
        assert!(hub.pending_is_empty(), "the card is withdrawn");
    }

    #[tokio::test]
    async fn a_client_claim_pauses_the_host_timer() {
        let hub = hub();
        let waiter = {
            let hub = hub.clone();
            tokio::spawn(async move {
                hub.ask(
                    "session-1",
                    questions(),
                    "call-3",
                    Some(Duration::from_millis(80)),
                    &Cancel::default(),
                )
                .await
            })
        };
        loop {
            if hub.is_waiting("session-1", "call-3") {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(hub.claim("session-1", "call-3"));
        tokio::time::sleep(Duration::from_millis(160)).await;
        let remaining = hub
            .remaining_ms("session-1", "call-3")
            .expect("still waiting");
        assert!(
            remaining > 50,
            "the timer is paused, not expiring: {remaining}"
        );
        answer_when_published(&hub, json!({"answers":[]})).await;
        hub.release("session-1", "call-3");
        assert!(matches!(
            waiter.await.expect("joins").expect("answered"),
            AskOutcome::Answered(_)
        ));
    }

    #[tokio::test]
    async fn a_client_deadline_rejection_reads_as_pending() {
        let hub = hub();
        let waiter = {
            let hub = hub.clone();
            tokio::spawn(async move {
                hub.ask(
                    "session-1",
                    questions(),
                    "call-4",
                    Some(Duration::from_secs(30)),
                    &Cancel::default(),
                )
                .await
            })
        };
        loop {
            if let Some(event_id) = hub.first_pending() {
                hub.settle(
                    &event_id,
                    WaterfallOutcome::Rejected {
                        message: "the question timed out".into(),
                        code: Some(TIMED_OUT_CODE.into()),
                    },
                )
                .expect("settled");
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            waiter.await.expect("joins").expect("pending"),
            AskOutcome::Pending
        ));
    }

    #[tokio::test]
    async fn a_cancelled_turn_cancels_the_published_card() {
        let hub = hub();
        let cancel = Cancel::default();
        let waiter = {
            let hub = hub.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                hub.ask("session-1", questions(), "call-5", None, &cancel)
                    .await
            })
        };
        while hub.first_pending().is_none() {
            tokio::task::yield_now().await;
        }
        cancel.cancel();
        assert!(waiter.await.expect("joins").is_err());
        assert!(hub.pending_is_empty());
    }

    #[tokio::test]
    async fn the_approver_reports_what_the_client_decided() {
        let hub = hub();
        let approver = HostApprover {
            hub: hub.clone(),
            agent: "session-1".into(),
        };
        let waiter = {
            let cancel = Cancel::default();
            tokio::spawn(async move {
                approver
                    .request(ApprovalRequest {
                        tool_name: "write",
                        call_id: "call-9",
                        reason: "outside the workspace",
                        cancel: &cancel,
                    })
                    .await
            })
        };
        loop {
            if let Some(event_id) = hub.first_pending() {
                hub.settle(&event_id, WaterfallOutcome::Value(json!("allowed-once")))
                    .expect("settled");
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(waiter.await.expect("joins"), ApprovalOutcome::AllowedOnce);
    }

    #[tokio::test]
    async fn an_unanswered_approval_fails_closed() {
        let hub = hub();
        let approver = HostApprover {
            hub: hub.clone(),
            agent: "session-1".into(),
        };
        let waiter = {
            let cancel = Cancel::default();
            tokio::spawn(async move {
                approver
                    .request(ApprovalRequest {
                        tool_name: "write",
                        call_id: "call-9",
                        reason: "outside the workspace",
                        cancel: &cancel,
                    })
                    .await
            })
        };
        loop {
            if let Some(event_id) = hub.first_pending() {
                hub.settle(&event_id, WaterfallOutcome::Next)
                    .expect("settled");
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(waiter.await.expect("joins"), ApprovalOutcome::Unavailable);
    }
}
