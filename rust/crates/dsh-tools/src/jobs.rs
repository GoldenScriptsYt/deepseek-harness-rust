//! Bounded, owner-scoped background jobs.
//!
//! The registry mirrors the TypeScript jobs contracts: admission is synchronous,
//! output has absolute byte offsets, model reads consume, `read_at` does not,
//! and settlement is first-wins. It does not infer that a settled producer has
//! released its operating-system resources; producers own that lifecycle.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type CancelHook = Box<dyn FnMut(Option<String>) -> Result<(), JobError> + Send>;

/// A stable client-visible job error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobError {
    pub code: &'static str,
    pub message: String,
}

impl JobError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for JobError {}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

/// Cooperative cancellation that does not own or implicitly kill a job.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    inner: Arc<TokenState>,
}

#[derive(Debug, Default)]
struct TokenState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

/// Exact identity and liveness for one terminal/job-owning session.
#[derive(Debug, Clone)]
pub struct Owner {
    inner: Arc<OwnerState>,
}

#[derive(Debug)]
struct OwnerState {
    id: String,
    live: AtomicBool,
}

impl PartialEq for Owner {
    fn eq(&self, other: &Self) -> bool {
        self.same(other)
    }
}

impl Eq for Owner {}

impl Owner {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(OwnerState {
                id: id.into(),
                live: AtomicBool::new(true),
            }),
        }
    }

    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub fn is_live(&self) -> bool {
        self.inner.live.load(Ordering::SeqCst)
    }

    /// Mark this owner dead. Call the relevant registries' `dispose_owner`
    /// before or immediately after this to release owned resources.
    pub fn dispose(&self) {
        self.inner.live.store(false, Ordering::SeqCst);
    }

    fn key(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Running,
    Stopping,
    Completed,
    Killed,
    Failed,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Killed | Self::Failed)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Completed => "completed",
            Self::Killed => "killed",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobChannel {
    Stdout,
    Stderr,
    Log,
}

impl JobChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::Log => "log",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementCause {
    Producer,
    Kill,
    Teardown,
}

impl SettlementCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Kill => "kill",
            Self::Teardown => "teardown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutcome {
    pub status: JobStatus,
    pub detail: Option<String>,
    pub exit_code: Option<i32>,
    pub result: Option<String>,
}

impl JobOutcome {
    pub fn completed() -> Self {
        Self {
            status: JobStatus::Completed,
            detail: None,
            exit_code: None,
            result: None,
        }
    }

    pub fn killed(detail: impl Into<String>) -> Self {
        Self {
            status: JobStatus::Killed,
            detail: Some(detail.into()),
            exit_code: None,
            result: None,
        }
    }

    pub fn failed(detail: impl Into<String>) -> Self {
        Self {
            status: JobStatus::Failed,
            detail: Some(detail.into()),
            exit_code: None,
            result: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutputChunk {
    /// Absolute UTF-8 byte offset of `text`.
    pub offset: u64,
    pub channel: JobChannel,
    pub text: String,
    /// Bytes before this chunk were evicted from the bounded ring.
    pub gap_before: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutputMetadata {
    pub total: u64,
    pub earliest: u64,
    pub spill_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobView {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub owner: Option<Owner>,
    pub status: JobStatus,
    pub progress: Option<String>,
    pub detail: Option<String>,
    pub exit_code: Option<i32>,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub output_limit_bytes: Option<u64>,
    pub output: JobOutputMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRead {
    pub chunks: Vec<JobOutputChunk>,
    pub lossy: bool,
    pub result: Option<String>,
    pub job: JobView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutputRead {
    pub chunks: Vec<JobOutputChunk>,
    pub next: u64,
    pub lossy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEvent {
    Registered(JobView),
    Progress(JobView),
    Stopping(JobView),
    Settled {
        job: JobView,
        cause: SettlementCause,
        awaited: bool,
    },
    Removed(JobView),
    Output {
        id: String,
        owner: Option<Owner>,
        total: u64,
    },
}

#[derive(Debug, Clone)]
pub enum JobEventFilter {
    All,
    /// An owner sees its own jobs and unowned jobs. `None` sees only unowned.
    Owner(Option<Owner>),
}

impl JobEvent {
    fn visible(&self, filter: &JobEventFilter) -> bool {
        match filter {
            JobEventFilter::All => true,
            JobEventFilter::Owner(None) => self.owner().is_none(),
            JobEventFilter::Owner(Some(caller)) => {
                self.owner().map(|owner| caller.same(owner)).unwrap_or(true)
            }
        }
    }

    fn owner(&self) -> Option<&Owner> {
        match self {
            Self::Registered(view)
            | Self::Progress(view)
            | Self::Stopping(view)
            | Self::Removed(view) => view.owner.as_ref(),
            Self::Settled { job, .. } => job.owner.as_ref(),
            Self::Output { owner, .. } => owner.as_ref(),
        }
    }
}

/// A bounded subscription. Lagged events are skipped rather than blocking
/// producers; use non-consuming `read_at` to recover missed output.
pub struct JobSubscription {
    receiver: broadcast::Receiver<JobEvent>,
    filter: JobEventFilter,
}

impl JobSubscription {
    pub async fn recv(&mut self) -> Result<JobEvent, JobError> {
        loop {
            let event = self
                .receiver
                .recv()
                .await
                .map_err(|_| JobError::new("EVENTS_CLOSED", "job event service is closed"))?;
            if event.visible(&self.filter) {
                return Ok(event);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobRegistryConfig {
    pub max_jobs: usize,
    pub max_concurrent_jobs_per_owner: usize,
    pub retain_bytes: usize,
    pub settled_retain_bytes: usize,
    pub pump_poll_ms: u64,
    pub event_capacity: usize,
    pub max_event_subscribers: usize,
    pub teardown_grace: Duration,
}

impl Default for JobRegistryConfig {
    fn default() -> Self {
        Self {
            max_jobs: 1024,
            max_concurrent_jobs_per_owner: 10,
            retain_bytes: 262_144,
            settled_retain_bytes: 16_384,
            pump_poll_ms: 150,
            event_capacity: 256,
            max_event_subscribers: 64,
            teardown_grace: Duration::from_secs(2),
        }
    }
}

#[derive(Clone)]
pub struct JobRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    config: JobRegistryConfig,
    state: Mutex<RegistryState>,
    events: Mutex<Option<broadcast::Sender<JobEvent>>>,
    next_admission: AtomicU64,
}

#[derive(Default)]
struct RegistryState {
    jobs: HashMap<String, Arc<Record>>,
    reservations: HashSet<(usize, u64)>,
    disposing: bool,
}

type HashMap<K, V> = std::collections::HashMap<K, V>;

struct Record {
    registry: Arc<Inner>,
    pump_poll_ms: u64,
    spec: RecordSpec,
    state: Mutex<JobState>,
    settled: Notify,
    settle_lock: Mutex<()>,
    committed: AtomicBool,
    cancel: Mutex<Option<CancelHook>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

struct RecordSpec {
    id: String,
    kind: String,
    label: String,
    owner: Option<Owner>,
    output_limit_bytes: Option<u64>,
    ring: Mutex<OutputRing>,
    spills: Mutex<Vec<String>>,
}

struct JobState {
    status: JobStatus,
    progress: Option<String>,
    detail: Option<String>,
    exit_code: Option<i32>,
    started_at: u64,
    finished_at: Option<u64>,
    model_cursor: u64,
    result_pending: Option<String>,
    result_delivered: bool,
    waiters: usize,
}

impl Record {
    fn view(&self) -> JobView {
        let state = lock(&self.state);
        let ring = lock(&self.spec.ring);
        JobView {
            id: self.spec.id.clone(),
            kind: self.spec.kind.clone(),
            label: self.spec.label.clone(),
            owner: self.spec.owner.clone(),
            status: state.status,
            progress: state.progress.clone(),
            detail: state.detail.clone(),
            exit_code: state.exit_code,
            started_at: state.started_at,
            finished_at: state.finished_at,
            output_limit_bytes: self.spec.output_limit_bytes,
            output: JobOutputMetadata {
                total: ring.total,
                earliest: ring.earliest(),
                spill_paths: lock(&self.spec.spills).clone(),
            },
        }
    }

    fn append_raw(&self, text: &str, channel: JobChannel, gap_before: bool) {
        if text.is_empty() {
            return;
        }
        let mut ring = lock(&self.spec.ring);
        let total = ring.append(text, channel, gap_before) + text.len() as u64;
        drop(ring);
        if self.committed.load(Ordering::SeqCst) {
            emit(
                self,
                JobEvent::Output {
                    id: self.spec.id.clone(),
                    owner: self.spec.owner.clone(),
                    total,
                },
            );
        }
    }
}

/// Producer-facing handle. Writes are staged and become visible at commit.
#[derive(Clone)]
pub struct JobHandle {
    id: String,
    record: Arc<Record>,
}

impl JobHandle {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn append(&self, text: impl AsRef<str>, channel: JobChannel) {
        self.record.append_raw(text.as_ref(), channel, false);
    }

    pub fn update_progress(&self, line: impl Into<String>) {
        lock(&self.record.state).progress = Some(line.into());
        if self.record.committed.load(Ordering::SeqCst) {
            emit(&self.record, JobEvent::Progress(self.record.view()));
        }
    }
}

/// Hooks returned by a successful starter.
pub struct JobHooks {
    cancel: Box<dyn FnMut(Option<String>) -> Result<(), JobError> + Send>,
    done: BoxFuture<JobOutcome>,
}

impl JobHooks {
    pub fn new<F>(
        cancel: impl FnMut(Option<String>) -> Result<(), JobError> + Send + 'static,
        done: F,
    ) -> Self
    where
        F: Future<Output = JobOutcome> + Send + 'static,
    {
        Self {
            cancel: Box::new(cancel),
            done: Box::pin(done),
        }
    }
}

/// Specification for one producer. The starter runs only after preflight.
pub struct JobSpec {
    kind: String,
    label: String,
    owner: Option<Owner>,
    output_limit_bytes: Option<u64>,
    sources: Vec<Box<dyn JobOutputSource>>,
    run: Box<dyn FnOnce(JobHandle) -> Result<JobHooks, JobError> + Send>,
}

impl JobSpec {
    pub fn new(
        kind: impl Into<String>,
        label: impl Into<String>,
        run: impl FnOnce(JobHandle) -> Result<JobHooks, JobError> + Send + 'static,
    ) -> Self {
        Self {
            kind: kind.into(),
            label: label.into(),
            owner: None,
            output_limit_bytes: None,
            sources: Vec::new(),
            run: Box::new(run),
        }
    }

    pub fn owner(mut self, owner: Owner) -> Self {
        self.owner = Some(owner);
        self
    }

    pub fn output_limit_bytes(mut self, bytes: u64) -> Self {
        self.output_limit_bytes = Some(bytes);
        self
    }

    pub fn source(mut self, source: Box<dyn JobOutputSource>) -> Self {
        self.sources.push(source);
        self
    }
}

/// Pull output source with one registry-owned cursor.
pub trait JobOutputSource: Send {
    fn read(&mut self, from: u64) -> Result<JobSourceRead, JobError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSourceRead {
    pub text: String,
    pub next_offset: u64,
    pub lossy: bool,
    pub spill_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OutputRing {
    chunks: VecDeque<JobOutputChunk>,
    total: u64,
    bytes: usize,
    limit: usize,
}

impl OutputRing {
    fn new(limit: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            total: 0,
            bytes: 0,
            limit,
        }
    }

    fn earliest(&self) -> u64 {
        self.chunks.front().map_or(self.total, |chunk| chunk.offset)
    }

    fn append(&mut self, text: &str, channel: JobChannel, gap_before: bool) -> u64 {
        let offset = self.total;
        self.total += text.len() as u64;
        self.chunks.push_back(JobOutputChunk {
            offset,
            channel,
            text: text.to_owned(),
            gap_before,
        });
        self.bytes += text.len();
        while self.bytes > self.limit && self.chunks.len() > 1 {
            let removed = self.chunks.pop_front().expect("nonempty");
            self.bytes -= removed.text.len();
        }
        self.trim_front_chunk();
        offset
    }

    /// Keeps the longest UTF-8-safe suffix of an oversized sole chunk.
    /// The chunk's offset advances so absolute read offsets stay correct.
    fn trim_front_chunk(&mut self) {
        if self.bytes <= self.limit {
            return;
        }
        let chunk = self.chunks.front_mut().expect("nonempty");
        let mut boundary = chunk.text.len().saturating_sub(self.limit);
        while !chunk.text.is_char_boundary(boundary) {
            boundary += 1;
        }
        chunk.text.drain(..boundary);
        chunk.offset += boundary as u64;
        chunk.gap_before = true;
        self.bytes = chunk.text.len();
    }

    fn trim(&mut self, limit: usize) {
        self.limit = limit;
        while self.bytes > limit && self.chunks.len() > 1 {
            let removed = self.chunks.pop_front().expect("nonempty");
            self.bytes -= removed.text.len();
        }
        self.trim_front_chunk();
    }

    fn read_from(&self, from: u64) -> JobOutputRead {
        JobOutputRead {
            chunks: self
                .chunks
                .iter()
                .filter(|chunk| chunk.offset + chunk.text.len() as u64 > from)
                .cloned()
                .collect(),
            next: self.total,
            lossy: from < self.earliest(),
        }
    }
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self::new(JobRegistryConfig::default())
    }
}

impl JobRegistry {
    pub fn new(config: JobRegistryConfig) -> Self {
        assert!(config.max_jobs > 0, "max_jobs must be positive");
        assert!(
            config.max_concurrent_jobs_per_owner > 0,
            "max_concurrent_jobs_per_owner must be positive"
        );
        assert!(config.retain_bytes > 0, "retain_bytes must be positive");
        assert!(
            config.settled_retain_bytes <= config.retain_bytes,
            "settled retention must not exceed active retention"
        );
        assert!(config.pump_poll_ms > 0, "pump_poll_ms must be positive");
        assert!(config.event_capacity > 0, "event_capacity must be positive");
        let (events, _) = broadcast::channel(config.event_capacity);
        Self {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(RegistryState::default()),
                events: Mutex::new(Some(events)),
                next_admission: AtomicU64::new(0),
            }),
        }
    }

    pub fn subscribe(&self, filter: JobEventFilter) -> Result<JobSubscription, JobError> {
        let events = lock(&self.inner.events);
        let sender = events
            .as_ref()
            .ok_or_else(|| JobError::new("SERVICE_DISPOSING", "job service is disposing"))?;
        if sender.receiver_count() >= self.inner.config.max_event_subscribers {
            return Err(JobError::new(
                "EVENT_LIMIT",
                "maximum job event subscribers reached",
            ));
        }
        Ok(JobSubscription {
            receiver: sender.subscribe(),
            filter,
        })
    }

    pub fn start(&self, spec: JobSpec) -> Result<String, JobError> {
        if spec.kind.is_empty() {
            return Err(JobError::new("INVALID_JOB", "kind must not be empty"));
        }
        if spec.label.is_empty() {
            return Err(JobError::new("INVALID_JOB", "label must not be empty"));
        }
        if let Some(owner) = &spec.owner {
            if !owner.is_live() {
                return Err(JobError::new("OWNER_NOT_LIVE", "job owner is not live"));
            }
        }

        let reservation_id = self.inner.next_admission.fetch_add(1, Ordering::Relaxed);
        let owner_key = spec.owner.as_ref().map_or(0, Owner::key);
        let mut reservation = Admission {
            inner: self.inner.clone(),
            key: (owner_key, reservation_id),
            active: true,
        };
        {
            let mut state = lock(&self.inner.state);
            if state.disposing {
                return Err(JobError::new(
                    "SERVICE_DISPOSING",
                    "job service is disposing",
                ));
            }
            let owner_active = state
                .jobs
                .values()
                .filter(|record| {
                    !lock(&record.state).status.is_terminal()
                        && record.spec.owner.as_ref().map_or(0, Owner::key) == owner_key
                })
                .count()
                + state
                    .reservations
                    .iter()
                    .filter(|(key, _)| *key == owner_key)
                    .count();
            if owner_active >= self.inner.config.max_concurrent_jobs_per_owner {
                return Err(JobError::new(
                    "OWNER_CAPACITY",
                    "maximum concurrent jobs for owner reached",
                ));
            }
            if state.jobs.len() + state.reservations.len() >= self.inner.config.max_jobs {
                return Err(JobError::new(
                    "CAPACITY",
                    "maximum retained or pending jobs reached",
                ));
            }
            state.reservations.insert(reservation.key);
        }

        let mut id = loop {
            let candidate = format!("{}-{}", spec.kind, random_hex_id());
            if !lock(&self.inner.state).jobs.contains_key(&candidate) {
                break candidate;
            }
        };
        if id.len() > 256 {
            return Err(JobError::new(
                "INVALID_JOB",
                "kind is too long for a job id",
            ));
        }
        let pump_poll_ms = self.inner.config.pump_poll_ms;
        let record = Arc::new(Record {
            registry: self.inner.clone(),
            pump_poll_ms,
            spec: RecordSpec {
                id: id.clone(),
                kind: spec.kind,
                label: spec.label,
                owner: spec.owner,
                output_limit_bytes: spec.output_limit_bytes,
                ring: Mutex::new(OutputRing::new(self.inner.config.retain_bytes)),
                spills: Mutex::new(Vec::new()),
            },
            state: Mutex::new(JobState {
                status: JobStatus::Running,
                progress: None,
                detail: None,
                exit_code: None,
                started_at: now_ms(),
                finished_at: None,
                model_cursor: 0,
                result_pending: None,
                result_delivered: false,
                waiters: 0,
            }),
            settled: Notify::new(),
            settle_lock: Mutex::new(()),
            committed: AtomicBool::new(false),
            cancel: Mutex::new(None),
            task: Mutex::new(None),
        });
        let handle = JobHandle {
            id: id.clone(),
            record: record.clone(),
        };
        let hooks = (spec.run)(handle)?;
        *lock(&record.cancel) = Some(hooks.cancel);

        {
            let mut state = lock(&self.inner.state);
            if state.disposing {
                return Err(JobError::new(
                    "SERVICE_DISPOSING",
                    "job service is disposing",
                ));
            }
            if let Some(owner) = &record.spec.owner {
                if !owner.is_live() {
                    return Err(JobError::new("OWNER_NOT_LIVE", "job owner is not live"));
                }
            }
            state.jobs.insert(id.clone(), record.clone());
            state.reservations.remove(&reservation.key);
            reservation.active = false;
            record.committed.store(true, Ordering::SeqCst);
            emit(&record, JobEvent::Registered(record.view()));
        }

        let sources = spec.sources;
        let task_record = record.clone();
        let task = tokio::spawn(async move {
            pump_and_settle(task_record, hooks.done, sources).await;
        });
        *lock(&record.task) = Some(task);
        id = record.spec.id.clone();
        Ok(id)
    }

    pub fn list(&self, caller: Option<&Owner>) -> Result<Vec<JobView>, JobError> {
        let inner = self.inner.clone();
        let state = lock(&inner.state);
        ensure_live(caller)?;
        let mut views: Vec<_> = state
            .jobs
            .values()
            .filter(|record| visible(record, caller))
            .map(|record| record.view())
            .collect();
        views.sort_by(|a, b| (a.started_at, &a.id).cmp(&(b.started_at, &b.id)));
        Ok(views)
    }

    pub fn get(&self, id: &str, caller: Option<&Owner>) -> Result<JobView, JobError> {
        let record = self.expect(id, caller)?;
        Ok(record.view())
    }

    /// Consuming model read. A settled result is returned exactly once.
    pub fn read(&self, id: &str, caller: Option<&Owner>) -> Result<JobRead, JobError> {
        let record = self.expect(id, caller)?;
        let mut state = lock(&record.state);
        let read = {
            let ring = lock(&record.spec.ring);
            ring.read_from(state.model_cursor)
        };
        state.model_cursor = read.next;
        let result = if state.status.is_terminal() && !state.result_delivered {
            state.result_delivered = true;
            state.result_pending.take()
        } else {
            None
        };
        let unread = lock(&record.spec.ring).total - state.model_cursor;
        if state.status.is_terminal() {
            lock(&record.spec.ring)
                .trim(self.inner.config.settled_retain_bytes.max(unread as usize));
        }
        drop(state);
        Ok(JobRead {
            chunks: read.chunks,
            lossy: read.lossy,
            result,
            job: record.view(),
        })
    }

    /// Non-consuming read at an absolute UTF-8 byte offset.
    pub fn read_at(
        &self,
        id: &str,
        from: u64,
        caller: Option<&Owner>,
    ) -> Result<JobOutputRead, JobError> {
        let record = self.expect(id, caller)?;
        let state = lock(&record.state);
        let read = lock(&record.spec.ring).read_from(from);
        if state.status.is_terminal() {
            lock(&record.spec.ring).trim(self.inner.config.settled_retain_bytes);
        }
        Ok(read)
    }

    pub fn kill(
        &self,
        id: &str,
        caller: Option<&Owner>,
        reason: Option<String>,
    ) -> Result<&'static str, JobError> {
        let record = self.expect(id, caller)?;
        let _settle = lock(&record.settle_lock);
        if lock(&record.state).status.is_terminal() {
            return Ok("already-finished");
        }
        let cancel = lock(&record.cancel).take();
        if let Some(mut cancel) = cancel {
            if let Err(error) = cancel(reason.clone()) {
                *lock(&record.cancel) = Some(cancel);
                return Err(error);
            }
        }
        {
            let mut state = lock(&record.state);
            if !state.status.is_terminal() {
                state.status = JobStatus::Stopping;
                state.detail = reason;
                drop(state);
                emit(&record, JobEvent::Stopping(record.view()));
            }
        }
        Ok("requested")
    }

    /// Wait without cancelling the producer. A timeout returns its live view.
    pub async fn wait(
        &self,
        id: &str,
        timeout: Duration,
        caller: Option<&Owner>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobView, JobError> {
        let record = self.expect(id, caller)?;
        if timeout.is_zero() {
            return Ok(record.view());
        }
        let notified = record.settled.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let registration = {
            let mut state = lock(&record.state);
            if state.status.is_terminal() {
                drop(state);
                return Ok(record.view());
            }
            state.waiters += 1;
            WaitRegistration {
                record: record.clone(),
            }
        };
        // Every branch settles the wait, and `notified` was armed before the
        // terminal check above, so a settlement cannot be missed.
        let view = tokio::select! {
            biased;
            _ = &mut notified => Ok(registration.record.view()),
            _ = sleep(timeout) => Ok(registration.record.view()),
            _ = async {
                if let Some(token) = cancellation {
                    token.cancelled().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                // A settlement that raced cancellation wins.
                if lock(&registration.record.state).status.is_terminal() {
                    Ok(registration.record.view())
                } else {
                    Err(JobError::new("CANCELLED", "job wait was cancelled"))
                }
            }
        };
        drop(registration);
        view
    }

    pub fn remove(&self, id: &str, caller: Option<&Owner>) -> Result<JobView, JobError> {
        let record = self.expect(id, caller)?;
        if !lock(&record.state).status.is_terminal() {
            return Err(JobError::new("JOB_ACTIVE", "active jobs cannot be removed"));
        }
        let mut state = lock(&self.inner.state);
        let removed = state
            .jobs
            .remove(id)
            .ok_or_else(|| JobError::new("NO_JOB", "job no longer exists"))?;
        let view = removed.view();
        emit(&removed, JobEvent::Removed(view.clone()));
        Ok(view)
    }

    pub async fn dispose_owner(&self, owner: &Owner) -> Result<(), JobError> {
        owner.dispose();
        let records: Vec<_> = {
            let state = lock(&self.inner.state);
            state
                .jobs
                .values()
                .filter(|record| {
                    record
                        .spec
                        .owner
                        .as_ref()
                        .map(|candidate| owner.same(candidate))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        };
        self.teardown_records(records).await?;
        let mut state = lock(&self.inner.state);
        state.jobs.retain(|_, record| {
            record
                .spec
                .owner
                .as_ref()
                .map(|candidate| !owner.same(candidate))
                .unwrap_or(true)
        });
        Ok(())
    }

    pub async fn dispose(&self) -> Result<(), JobError> {
        {
            let mut state = lock(&self.inner.state);
            state.disposing = true;
        }
        let records: Vec<_> = lock(&self.inner.state).jobs.values().cloned().collect();
        self.teardown_records(records).await?;
        lock(&self.inner.state).jobs.clear();
        lock(&self.inner.events).take();
        Ok(())
    }

    fn expect(&self, id: &str, caller: Option<&Owner>) -> Result<Arc<Record>, JobError> {
        ensure_live(caller)?;
        let state = lock(&self.inner.state);
        let record = state
            .jobs
            .get(id)
            .cloned()
            .ok_or_else(|| JobError::new("NO_JOB", format!("unknown job {id}")))?;
        if !visible(&record, caller) {
            return Err(JobError::new("FOREIGN_JOB", "job belongs to another owner"));
        }
        Ok(record)
    }

    async fn teardown_records(&self, records: Vec<Arc<Record>>) -> Result<(), JobError> {
        for record in &records {
            stop_for_teardown(record);
        }
        let tasks: Vec<_> = records
            .iter()
            .filter_map(|record| lock(&record.task).take().map(|task| (record.clone(), task)))
            .collect();
        let deadline = Instant::now() + self.inner.config.teardown_grace;
        for (record, mut task) in tasks {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remaining, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
                let _settle = lock(&record.settle_lock);
                settle_locked(
                    &self.inner,
                    &record,
                    JobOutcome::failed(
                        "producer did not settle during teardown; producer resources may be orphaned",
                    ),
                    SettlementCause::Teardown,
                );
            }
        }
        Ok(())
    }
}

fn ensure_live(caller: Option<&Owner>) -> Result<(), JobError> {
    if caller.map(|owner| !owner.is_live()).unwrap_or(false) {
        return Err(JobError::new("OWNER_NOT_LIVE", "job owner is not live"));
    }
    Ok(())
}

fn visible(record: &Record, caller: Option<&Owner>) -> bool {
    match (&record.spec.owner, caller) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(owner), Some(caller)) => owner.same(caller),
    }
}

struct Admission {
    inner: Arc<Inner>,
    key: (usize, u64),
    active: bool,
}

impl Drop for Admission {
    fn drop(&mut self) {
        if self.active {
            lock(&self.inner.state).reservations.remove(&self.key);
        }
    }
}

struct WaitRegistration {
    record: Arc<Record>,
}

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        let mut state = lock(&self.record.state);
        state.waiters = state.waiters.saturating_sub(1);
    }
}

async fn pump_and_settle(
    record: Arc<Record>,
    mut done: BoxFuture<JobOutcome>,
    mut sources: Vec<Box<dyn JobOutputSource>>,
) {
    let mut cursors = vec![0_u64; sources.len()];
    let mut exhausted = vec![false; sources.len()];
    let mut deadline = Instant::now();
    loop {
        let mut active = false;
        for (index, source) in sources.iter_mut().enumerate() {
            if exhausted[index] {
                continue;
            }
            active = true;
            match source.read(cursors[index]) {
                Ok(read) => {
                    cursors[index] = read.next_offset;
                    let spill = read.spill_path.unwrap_or_default();
                    if !spill.is_empty() {
                        let mut paths = lock(&record.spec.spills);
                        if !paths.iter().any(|path| path == &spill) {
                            paths.push(spill);
                        }
                    }
                    record.append_raw(&read.text, JobChannel::Log, read.lossy);
                }
                Err(_) => exhausted[index] = true,
            }
        }
        if !active {
            break;
        }
        tokio::select! {
            biased;
            outcome = &mut done => {
                finish_producer(record, outcome, sources, &mut cursors, &mut exhausted).await;
                return;
            }
            _ = sleep_until(deadline) => {}
        }
        deadline = Instant::now() + Duration::from_millis(record_poll_ms(&record));
    }
    let outcome = done.await;
    finish_producer(record, outcome, sources, &mut cursors, &mut exhausted).await;
}

async fn sleep_until(deadline: Instant) {
    tokio::time::sleep_until(deadline).await;
}

async fn finish_producer(
    record: Arc<Record>,
    outcome: JobOutcome,
    mut sources: Vec<Box<dyn JobOutputSource>>,
    cursors: &mut [u64],
    exhausted: &mut [bool],
) {
    for (index, source) in sources.iter_mut().enumerate() {
        if exhausted[index] {
            continue;
        }
        if let Ok(read) = source.read(cursors[index]) {
            cursors[index] = read.next_offset;
            if let Some(path) = read.spill_path {
                let mut paths = lock(&record.spec.spills);
                if !paths.iter().any(|existing| existing == &path) {
                    paths.push(path);
                }
            }
            record.append_raw(&read.text, JobChannel::Log, read.lossy);
        }
    }
    settle(&record, outcome, SettlementCause::Producer);
}

fn record_poll_ms(record: &Record) -> u64 {
    record.pump_poll_ms
}

fn settle(record: &Arc<Record>, outcome: JobOutcome, cause: SettlementCause) {
    let _settle = lock(&record.settle_lock);
    settle_locked(&record.registry, record, outcome, cause);
}

fn settle_locked(
    registry: &Arc<Inner>,
    record: &Arc<Record>,
    mut outcome: JobOutcome,
    cause: SettlementCause,
) {
    if !matches!(
        outcome.status,
        JobStatus::Completed | JobStatus::Killed | JobStatus::Failed
    ) {
        outcome = JobOutcome::failed("producer returned a non-terminal outcome");
    }
    {
        let mut state = lock(&record.state);
        if state.status.is_terminal() {
            return;
        }
        let unread = lock(&record.spec.ring).total - state.model_cursor;
        lock(&record.spec.ring).trim(registry.config.settled_retain_bytes.max(unread as usize));
        if outcome.status == JobStatus::Killed {
            if let Some(reason) = &state.detail {
                outcome.detail = Some(match &outcome.detail {
                    Some(detail) if !detail.is_empty() => format!("{detail}: {reason}"),
                    _ => reason.clone(),
                });
            }
        }
        state.status = outcome.status;
        state.detail = outcome.detail;
        state.exit_code = outcome.exit_code;
        state.finished_at = Some(now_ms());
        state.result_pending = outcome.result;
        state.progress = None;
        let awaited = state.waiters > 0;
        drop(state);
        lock(&record.cancel).take();
        record.settled.notify_waiters();
        emit(
            record,
            JobEvent::Settled {
                job: record.view(),
                cause,
                awaited,
            },
        );
        emit(
            record,
            JobEvent::Output {
                id: record.spec.id.clone(),
                owner: record.spec.owner.clone(),
                total: lock(&record.spec.ring).total,
            },
        );
    }
}

fn stop_for_teardown(record: &Arc<Record>) {
    let _settle = lock(&record.settle_lock);
    if lock(&record.state).status.is_terminal() {
        return;
    }
    let cancel = lock(&record.cancel).take();
    if let Some(mut cancel) = cancel {
        if let Err(error) = cancel(Some("service teardown".to_owned())) {
            settle_locked(
                &record.registry,
                record,
                JobOutcome::failed(format!(
                    "producer cancellation failed ({error}); producer resources may be orphaned"
                )),
                SettlementCause::Teardown,
            );
        }
    }
}

fn emit(record: &Record, event: JobEvent) {
    if let Some(sender) = lock(&record.registry.events).as_ref() {
        let _ = sender.send(event);
    }
}

fn random_hex_id() -> String {
    let mut bytes = [0_u8; 8];
    fill_random(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    let mut id = String::with_capacity(16);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut id, "{byte:02x}").expect("writing to String cannot fail");
    }
    id
}

#[cfg(windows)]
fn fill_random(bytes: &mut [u8]) {
    use windows_sys::Win32::Security::Cryptography::{
        BCryptGenRandom, BCRYPT_USE_SYSTEM_PREFERRED_RNG,
    };
    let status = unsafe {
        BCryptGenRandom(
            std::ptr::null_mut(),
            bytes.as_mut_ptr(),
            bytes.len() as u32,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    assert_eq!(
        status, 0,
        "BCryptGenRandom failed with NTSTATUS {status:#x}"
    );
}

#[cfg(unix)]
fn fill_random(bytes: &mut [u8]) {
    use std::io::Read;
    let mut random = std::fs::File::open("/dev/urandom").expect("open /dev/urandom");
    random.read_exact(bytes).expect("read /dev/urandom");
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::oneshot;

    struct Source {
        reads: AtomicUsize,
        text: &'static str,
    }

    impl JobOutputSource for Source {
        fn read(&mut self, from: u64) -> Result<JobSourceRead, JobError> {
            if from == 0 && self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(JobSourceRead {
                    text: self.text.to_owned(),
                    next_offset: self.text.len() as u64,
                    lossy: false,
                    spill_path: Some("spill.log".to_owned()),
                });
            }
            Ok(JobSourceRead {
                text: String::new(),
                next_offset: from,
                lossy: false,
                spill_path: None,
            })
        }
    }

    #[test]
    fn ring_is_utf8_safe_bounded_and_non_consuming() {
        let mut ring = OutputRing::new(5);
        ring.append("éééééé", JobChannel::Stdout, false);
        assert_eq!(ring.total, 12);
        assert_eq!(ring.earliest(), 8);
        let read = ring.read_from(0);
        assert!(read.lossy);
        assert_eq!(read.chunks[0].text, "éé");
        assert_eq!(read.next, 12);
        assert_eq!(ring.read_from(2).chunks, read.chunks);
    }

    #[tokio::test]
    async fn ownership_cursor_result_and_first_wins_settlement() {
        let registry = JobRegistry::new(JobRegistryConfig {
            pump_poll_ms: 1,
            ..JobRegistryConfig::default()
        });
        let owner = Owner::new("owner-a");
        let foreign = Owner::new("owner-b");
        let (done_tx, done_rx) = oneshot::channel();
        let id = registry
            .start(
                JobSpec::new("test", "persistent", move |handle| {
                    handle.append("hello", JobChannel::Stdout);
                    handle.update_progress("working");
                    Ok(JobHooks::new(|_| Ok(()), async move {
                        done_rx.await.unwrap_or_else(|_| JobOutcome::completed())
                    }))
                })
                .owner(owner.clone()),
            )
            .unwrap();
        assert!(registry.get(&id, None).is_err());
        assert!(registry.get(&id, Some(&foreign)).is_err());
        let waiter = {
            let registry = registry.clone();
            let id = id.clone();
            let owner = owner.clone();
            tokio::spawn(async move {
                registry
                    .wait(&id, Duration::from_secs(1), Some(&owner), None)
                    .await
                    .unwrap()
            })
        };
        done_tx
            .send(JobOutcome {
                result: Some("answer".to_owned()),
                ..JobOutcome::completed()
            })
            .unwrap();
        assert_eq!(waiter.await.unwrap().status, JobStatus::Completed);
        let first = registry.read(&id, Some(&owner)).unwrap();
        assert_eq!(first.chunks[0].text, "hello");
        assert_eq!(first.result.as_deref(), Some("answer"));
        assert_eq!(registry.read(&id, Some(&owner)).unwrap().result, None);
        assert_eq!(
            registry.kill(&id, Some(&owner), None).unwrap(),
            "already-finished"
        );
        registry.dispose().await.unwrap();
    }

    #[tokio::test]
    async fn pull_source_is_drained_after_settlement() {
        let registry = JobRegistry::new(JobRegistryConfig {
            pump_poll_ms: 1,
            ..JobRegistryConfig::default()
        });
        let (done_tx, done_rx) = oneshot::channel();
        let id = registry
            .start(
                JobSpec::new("pull", "pull", move |_| {
                    Ok(JobHooks::new(
                        |_| Ok(()),
                        async move { done_rx.await.unwrap() },
                    ))
                })
                .source(Box::new(Source {
                    reads: AtomicUsize::new(0),
                    text: "tail",
                })),
            )
            .unwrap();
        done_tx.send(JobOutcome::completed()).unwrap();
        registry
            .wait(&id, Duration::from_secs(1), None, None)
            .await
            .unwrap();
        let view = registry.get(&id, None).unwrap();
        assert_eq!(view.output.spill_paths, ["spill.log"]);
        let read = registry.read_at(&id, 0, None).unwrap();
        assert_eq!(read.chunks[0].text, "tail");
        registry.dispose().await.unwrap();
    }

    #[tokio::test]
    async fn throwing_cancel_does_not_change_state() {
        let registry = JobRegistry::default();
        let id = registry
            .start(JobSpec::new("test", "cancel", move |_| {
                Ok(JobHooks::new(
                    |_| Err(JobError::new("CANCEL_FAILED", "no")),
                    std::future::pending::<JobOutcome>(),
                ))
            }))
            .unwrap();
        assert!(registry.kill(&id, None, Some("stop".into())).is_err());
        assert_eq!(registry.get(&id, None).unwrap().status, JobStatus::Running);
        registry.dispose().await.unwrap();
    }

    #[test]
    fn ids_have_the_contract_shape() {
        let id = random_hex_id();
        assert_eq!(id.len(), 16);
        assert!(id
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()));
        assert_eq!(id.as_bytes()[12], b'4');
    }
}
