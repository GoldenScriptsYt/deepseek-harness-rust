//! Persistent owner-scoped terminal sessions and managed child processes.
//!
//! Registry code is platform independent and is tested with a fake backend.
//! The local backend uses a real PTY: POSIX `openpty`/controlling terminals and
//! Windows ConPTY plus a kill-on-close Job Object. Ordinary managed processes
//! use process groups on POSIX and Job Objects on Windows.

use crate::jobs::{BoxFuture, CancellationToken, Owner};
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::task::JoinHandle;

pub(crate) const TEARDOWN_GRACE: Duration = Duration::from_secs(2);

/// Stable terminal error code and diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalError {
    pub code: &'static str,
    pub message: String,
}

impl TerminalError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for TerminalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for TerminalError {}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalSignal {
    Sigint,
    Sigterm,
    Sigkill,
    Sigtstp,
    Sighup,
}

impl TerminalSignal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sigint => "SIGINT",
            Self::Sigterm => "SIGTERM",
            Self::Sigkill => "SIGKILL",
            Self::Sigtstp => "SIGTSTP",
            Self::Sighup => "SIGHUP",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalSessionStatus {
    Running,
    Exited {
        exit_code: Option<i32>,
        signal: Option<TerminalSignal>,
    },
}

impl TerminalSessionStatus {
    pub fn is_exited(&self) -> bool {
        matches!(self, Self::Exited { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalWaitReason {
    StdinRead,
    InferredIdle,
    Timeout,
    SessionExit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalSize {
    pub cols: u16,
    pub rows: u16,
}

impl TerminalSize {
    pub fn new(cols: u16, rows: u16) -> Result<Self, TerminalError> {
        if cols == 0 || rows == 0 {
            return Err(TerminalError::new(
                "INVALID_SIZE",
                "terminal dimensions must be positive",
            ));
        }
        Ok(Self { cols, rows })
    }
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self {
            cols: 160,
            rows: 40,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TerminalCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
}

impl TerminalCommand {
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TerminalSpawnRequest {
    pub backend: String,
    pub name: Option<String>,
    pub command: TerminalCommand,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub size: TerminalSize,
}

impl TerminalSpawnRequest {
    pub fn new(backend: impl Into<String>, command: TerminalCommand) -> Self {
        Self {
            backend: backend.into(),
            name: None,
            command,
            cwd: None,
            env: Vec::new(),
            size: TerminalSize::default(),
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    pub fn size(mut self, size: TerminalSize) -> Self {
        self.size = size;
        self
    }
}

#[derive(Debug, Clone)]
pub struct TerminalSpawnResult {
    pub id: String,
    pub name: String,
    pub backend: String,
    pub pid: Option<u32>,
    pub motd: String,
    pub session: TerminalSessionSnapshot,
}

#[derive(Debug, Clone)]
pub struct TerminalSendRequest {
    pub text: String,
    pub submit: bool,
    pub timeout: Duration,
    pub cancellation: Option<CancellationToken>,
}

impl TerminalSendRequest {
    pub fn new(text: impl Into<String>, submit: bool) -> Self {
        Self {
            text: text.into(),
            submit,
            timeout: Duration::from_secs(30),
            cancellation: None,
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSendResult {
    pub viewport: String,
    pub wait_reason: TerminalWaitReason,
    pub session_status: TerminalSessionStatus,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalReadResult {
    pub text: String,
    pub total_lines: usize,
    pub line_begin: usize,
    pub line_end: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSignalResult {
    pub signal: TerminalSignal,
    pub target: u32,
}

#[derive(Debug, Clone)]
pub struct TerminalSessionSnapshot {
    pub id: String,
    pub name: String,
    pub backend: String,
    pub pid: Option<u32>,
    pub status: TerminalSessionStatus,
    pub created_at: u64,
    pub active_send: bool,
    pub closing: bool,
    pub cleanup_error: Option<String>,
}

/// Backend contract. Implementations own PTY/process mechanics and readiness.
pub trait TerminalBackend: Send + Sync {
    fn kind(&self) -> &str;
    fn spawn(
        &self,
        spec: TerminalBackendSpawnSpec,
    ) -> BoxFuture<Result<Arc<dyn TerminalBackendSession>, TerminalError>>;
}

#[derive(Debug, Clone)]
pub struct TerminalBackendSpawnSpec {
    pub owner: Owner,
    pub session_id: String,
    pub cancellation: CancellationToken,
    pub command: TerminalCommand,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub size: TerminalSize,
}

pub trait TerminalSendOperation: Send + Sync {
    fn done(self: Arc<Self>) -> BoxFuture<Result<TerminalSendResult, TerminalError>>;
    fn read_output(self: Arc<Self>) -> TerminalReadResult;
    fn cancel(self: Arc<Self>) -> bool;
}

/// Reusable send promise plus a consuming output cursor.
#[derive(Clone)]
pub struct TerminalSendOperationHandle(Arc<dyn TerminalSendOperation>);

impl TerminalSendOperationHandle {
    pub async fn done(&self) -> Result<TerminalSendResult, TerminalError> {
        self.0.clone().done().await
    }

    pub fn read_output(&self) -> TerminalReadResult {
        self.0.clone().read_output()
    }

    pub fn cancel(&self) -> bool {
        self.0.clone().cancel()
    }
}

pub trait TerminalBackendSession: Send + Sync {
    fn pid(&self) -> Option<u32>;
    fn start_send(
        &self,
        request: TerminalSendRequest,
    ) -> Result<TerminalSendOperationHandle, TerminalError>;
    fn read(&self, offset: Option<u64>, count: usize) -> TerminalReadResult;
    fn signal(
        &self,
        signal: TerminalSignal,
    ) -> BoxFuture<Result<TerminalSignalResult, TerminalError>>;
    fn resize(&self, cols: u16, rows: u16) -> BoxFuture<Result<(), TerminalError>>;
    fn status(&self) -> TerminalSessionStatus;
    fn close(&self, reason: String) -> BoxFuture<Result<(), TerminalError>>;
}

#[derive(Debug, Clone)]
pub struct TerminalRegistryConfig {
    pub max_sessions: usize,
    pub max_pending_spawns: usize,
    pub teardown_grace: Duration,
}

impl Default for TerminalRegistryConfig {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            max_pending_spawns: 32,
            teardown_grace: TEARDOWN_GRACE,
        }
    }
}

pub struct TerminalRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    config: TerminalRegistryConfig,
    state: Mutex<RegistryState>,
    next_id: AtomicU64,
}

struct RegistryState {
    backends: HashMap<String, Arc<dyn TerminalBackend>>,
    sessions: HashMap<String, Arc<SessionRecord>>,
    pending: HashMap<String, Arc<PendingSpawn>>,
    next_name: u64,
    disposing: bool,
}

struct SessionRecord {
    registry: std::sync::Weak<RegistryInner>,
    id: String,
    name: String,
    owner: Owner,
    backend: String,
    session: Arc<dyn TerminalBackendSession>,
    created_at: u64,
    active_send: Mutex<Option<Arc<SendState>>>,
    closing: Mutex<Option<Arc<CloseState>>>,
    cleanup_error: Mutex<Option<String>>,
}

struct SendState {
    finished: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

struct CloseState {
    finished: Notify,
    result: Mutex<Option<Result<(), TerminalError>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

struct PendingSpawn {
    owner: Owner,
    name: String,
    backend: String,
    cancellation: CancellationToken,
    result: Mutex<Option<Result<Arc<dyn TerminalBackendSession>, TerminalError>>>,
    finished: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Default for TerminalRegistry {
    fn default() -> Self {
        Self::new(TerminalRegistryConfig::default())
    }
}

impl TerminalRegistry {
    pub fn new(config: TerminalRegistryConfig) -> Self {
        assert!(config.max_sessions > 0, "max_sessions must be positive");
        assert!(
            config.max_pending_spawns > 0,
            "max_pending_spawns must be positive"
        );
        Self {
            inner: Arc::new(RegistryInner {
                config,
                state: Mutex::new(RegistryState {
                    backends: HashMap::new(),
                    sessions: HashMap::new(),
                    pending: HashMap::new(),
                    next_name: 0,
                    disposing: false,
                }),
                next_id: AtomicU64::new(0),
            }),
        }
    }

    pub fn register_backend(&self, backend: Arc<dyn TerminalBackend>) -> Result<(), TerminalError> {
        if backend.kind().is_empty() {
            return Err(TerminalError::new(
                "INVALID_BACKEND",
                "backend kind must not be empty",
            ));
        }
        let mut state = lock(&self.inner.state);
        if state.disposing {
            return Err(TerminalError::new(
                "SERVICE_DISPOSING",
                "terminal service is disposing",
            ));
        }
        if state.backends.contains_key(backend.kind()) {
            return Err(TerminalError::new(
                "DUPLICATE_BACKEND",
                "backend is already registered",
            ));
        }
        state.backends.insert(backend.kind().to_owned(), backend);
        Ok(())
    }

    pub fn list_backends(&self) -> Vec<String> {
        let mut kinds: Vec<_> = lock(&self.inner.state).backends.keys().cloned().collect();
        kinds.sort();
        kinds
    }

    pub async fn spawn(
        &self,
        owner: &Owner,
        request: TerminalSpawnRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<TerminalSpawnResult, TerminalError> {
        if !owner.is_live() {
            return Err(TerminalError::new(
                "OWNER_NOT_LIVE",
                "terminal owner is not live",
            ));
        }
        let (id, name, pending) = {
            let mut state = lock(&self.inner.state);
            if state.disposing {
                return Err(TerminalError::new(
                    "SERVICE_DISPOSING",
                    "terminal service is disposing",
                ));
            }
            if state.sessions.len() + state.pending.len() >= self.inner.config.max_sessions {
                return Err(TerminalError::new(
                    "CAPACITY",
                    "maximum terminal sessions reached",
                ));
            }
            if state.pending.len() >= self.inner.config.max_pending_spawns {
                return Err(TerminalError::new(
                    "CAPACITY",
                    "maximum pending terminal spawns reached",
                ));
            }
            let backend = state
                .backends
                .get(&request.backend)
                .cloned()
                .ok_or_else(|| {
                    TerminalError::new("NO_BACKEND", "terminal backend is not registered")
                })?;
            let id = format!(
                "pty-{}",
                self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1
            );
            let name = match request.name.as_deref() {
                Some(name) if !name.is_empty() => name.to_owned(),
                Some(_) => {
                    return Err(TerminalError::new(
                        "DUPLICATE_NAME",
                        "name must not be empty",
                    ))
                }
                None => {
                    state.next_name += 1;
                    format!("terminal-{}", state.next_name)
                }
            };
            if state
                .sessions
                .values()
                .any(|record| record.owner.same(owner) && record.name == name)
                || state
                    .pending
                    .values()
                    .any(|item| item.owner.same(owner) && item.name == name)
            {
                return Err(TerminalError::new(
                    "DUPLICATE_NAME",
                    "terminal name is already reserved",
                ));
            }
            let token = CancellationToken::new();
            let pending = Arc::new(PendingSpawn {
                owner: owner.clone(),
                name: name.clone(),
                backend: request.backend.clone(),
                cancellation: token.clone(),
                result: Mutex::new(None),
                finished: Notify::new(),
                task: Mutex::new(None),
            });
            let task_pending = pending.clone();
            let spec = TerminalBackendSpawnSpec {
                owner: owner.clone(),
                session_id: id.clone(),
                cancellation: token,
                command: request.command,
                cwd: request.cwd,
                env: request.env,
                size: request.size,
            };
            let task = tokio::spawn(async move {
                let result = backend.spawn(spec).await;
                *lock(&task_pending.result) = Some(result);
                task_pending.finished.notify_waiters();
            });
            *lock(&pending.task) = Some(task);
            state.pending.insert(id.clone(), pending.clone());
            (id, name, pending)
        };

        let notified = pending.finished.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let result = loop {
            if lock(&pending.result).is_some() {
                break lock(&pending.result).take().expect("result present");
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = async {
                    if let Some(token) = cancellation { token.cancelled().await; }
                    else { std::future::pending::<()>().await; }
                } => {
                    pending.cancellation.cancel();
                    if !wait_for_pending(&pending, self.inner.config.teardown_grace).await {
                        abort_pending(&pending).await;
                        break Err(TerminalError::new("CANCELLED", "terminal spawn was cancelled"));
                    }
                }
            }
            if self.inner_state_disposing() {
                pending.cancellation.cancel();
                if !wait_for_pending(&pending, self.inner.config.teardown_grace).await {
                    abort_pending(&pending).await;
                    break Err(TerminalError::new(
                        "SERVICE_DISPOSING",
                        "terminal service is disposing",
                    ));
                }
            }
        };
        lock(&pending.task).take();
        lock(&self.inner.state).pending.remove(&id);

        let session = result?;
        let cancelled = cancellation
            .map(CancellationToken::is_cancelled)
            .unwrap_or(false)
            || pending.cancellation.is_cancelled();
        if cancelled || self.inner_state_disposing() || !owner.is_live() {
            let reason = if cancelled {
                "spawn cancelled"
            } else {
                "spawn rolled back"
            };
            if let Err(error) = timeout_close(
                &session,
                reason.to_owned(),
                self.inner.config.teardown_grace,
            )
            .await
            {
                let record = self.retain_cleanup_failure(
                    &id,
                    &name,
                    owner,
                    &pending.backend,
                    session,
                    error,
                );
                return Err(TerminalError::new(
                    "CLEANUP_FAILED",
                    format!(
                        "spawn rollback failed; session {} retained ({})",
                        record.id,
                        lock(&record.cleanup_error)
                            .as_deref()
                            .unwrap_or("cleanup failed")
                    ),
                ));
            }
            return Err(TerminalError::new(
                if cancelled {
                    "CANCELLED"
                } else {
                    "SERVICE_DISPOSING"
                },
                reason,
            ));
        }

        let record = Arc::new(SessionRecord {
            registry: Arc::downgrade(&self.inner),
            id: id.clone(),
            name: name.clone(),
            owner: owner.clone(),
            backend: pending.backend.clone(),
            session,
            created_at: now_ms(),
            active_send: Mutex::new(None),
            closing: Mutex::new(None),
            cleanup_error: Mutex::new(None),
        });
        // The guard must not live across the rollback's await.
        let published = {
            let mut state = lock(&self.inner.state);
            let open = !state.disposing && owner.is_live();
            if open {
                state.sessions.insert(id.clone(), record.clone());
            }
            open
        };
        if !published {
            if let Err(error) = timeout_close(
                &record.session,
                "publication rolled back".to_owned(),
                self.inner.config.teardown_grace,
            )
            .await
            {
                self.retain_cleanup_failure(
                    &id,
                    &name,
                    owner,
                    &pending.backend,
                    record.session.clone(),
                    error,
                );
            }
            return Err(TerminalError::new(
                "SERVICE_DISPOSING",
                "terminal service stopped before publication",
            ));
        }
        Ok(TerminalSpawnResult {
            session: record.snapshot(),
            id: record.id.clone(),
            name: record.name.clone(),
            backend: record.backend.clone(),
            pid: record.session.pid(),
            motd: record.session.read(None, 40).text,
        })
    }

    fn inner_state_disposing(&self) -> bool {
        lock(&self.inner.state).disposing
    }

    fn retain_cleanup_failure(
        &self,
        id: &str,
        name: &str,
        owner: &Owner,
        backend: &str,
        session: Arc<dyn TerminalBackendSession>,
        error: TerminalError,
    ) -> Arc<SessionRecord> {
        let record = Arc::new(SessionRecord {
            registry: Arc::downgrade(&self.inner),
            id: id.to_owned(),
            name: name.to_owned(),
            owner: owner.clone(),
            backend: backend.to_owned(),
            session,
            created_at: now_ms(),
            active_send: Mutex::new(None),
            closing: Mutex::new(None),
            cleanup_error: Mutex::new(Some(error.to_string())),
        });
        lock(&self.inner.state)
            .sessions
            .insert(record.id.clone(), record.clone());
        record
    }

    pub fn has_owner_activity(&self, owner: &Owner) -> bool {
        let state = lock(&self.inner.state);
        state.pending.values().any(|item| item.owner.same(owner))
            || state
                .sessions
                .values()
                .any(|record| record.owner.same(owner))
    }

    pub fn list(&self, owner: &Owner) -> Result<Vec<TerminalSessionSnapshot>, TerminalError> {
        self.ensure_owner(owner)?;
        let state = lock(&self.inner.state);
        let mut sessions: Vec<_> = state
            .sessions
            .values()
            .filter(|record| record.owner.same(owner))
            .map(|record| record.snapshot())
            .collect();
        sessions.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(sessions)
    }

    pub fn start_send(
        &self,
        owner: &Owner,
        id: &str,
        request: TerminalSendRequest,
    ) -> Result<TerminalSendOperationHandle, TerminalError> {
        let record = self.expect(owner, id)?;
        if lock(&record.cleanup_error).is_some()
            || record.session.status().is_exited()
            || record.is_closing()
        {
            return Err(TerminalError::new(
                "SESSION_EXITED",
                "terminal session is not writable",
            ));
        }
        let mut active = lock(&record.active_send);
        if active.is_some() {
            return Err(TerminalError::new(
                "SEND_ACTIVE",
                "terminal send is already active",
            ));
        }
        let operation = record.session.start_send(request)?;
        let state = Arc::new(SendState {
            finished: Notify::new(),
            task: Mutex::new(None),
        });
        let task_state = state.clone();
        let task_record = record.clone();
        let task_operation = operation.clone();
        let task = tokio::spawn(async move {
            let _ = task_operation.done().await;
            let mut active = lock(&task_record.active_send);
            if active
                .as_ref()
                .map(|item| Arc::ptr_eq(item, &task_state))
                .unwrap_or(false)
            {
                *active = None;
            }
            task_state.finished.notify_waiters();
        });
        *lock(&state.task) = Some(task);
        *active = Some(state);
        Ok(operation)
    }

    pub fn read(
        &self,
        owner: &Owner,
        id: &str,
        offset: Option<u64>,
        count: usize,
    ) -> Result<TerminalReadResult, TerminalError> {
        Ok(self.expect(owner, id)?.session.read(offset, count))
    }

    pub async fn signal(
        &self,
        owner: &Owner,
        id: &str,
        signal: TerminalSignal,
    ) -> Result<TerminalSignalResult, TerminalError> {
        let record = self.expect(owner, id)?;
        if record.session.status().is_exited() || record.is_closing() {
            return Err(TerminalError::new(
                "SESSION_EXITED",
                "terminal session has exited",
            ));
        }
        record.session.signal(signal).await
    }

    pub async fn resize(
        &self,
        owner: &Owner,
        id: &str,
        cols: u16,
        rows: u16,
    ) -> Result<(), TerminalError> {
        let record = self.expect(owner, id)?;
        if record.session.status().is_exited() || record.is_closing() {
            return Err(TerminalError::new(
                "SESSION_EXITED",
                "terminal session has exited",
            ));
        }
        TerminalSize::new(cols, rows)?;
        record.session.resize(cols, rows).await
    }

    pub async fn kill(
        &self,
        owner: &Owner,
        id: &str,
        reason: Option<String>,
    ) -> Result<bool, TerminalError> {
        let record = self.expect(owner, id)?;
        if record.session.status().is_exited()
            && !record.is_closing()
            && lock(&record.cleanup_error).is_none()
        {
            return Ok(false);
        }
        close_record(
            &record,
            reason.unwrap_or_else(|| "session killed".to_owned()),
            self.inner.config.teardown_grace,
        )
        .await?;
        Ok(true)
    }

    pub async fn dispose_owner(&self, owner: &Owner) -> Result<(), TerminalError> {
        owner.dispose();
        let pending: Vec<_> = {
            let state = lock(&self.inner.state);
            state
                .pending
                .values()
                .filter(|item| item.owner.same(owner))
                .cloned()
                .collect()
        };
        for item in &pending {
            item.cancellation.cancel();
        }
        let deadline = Instant::now() + self.inner.config.teardown_grace;
        for item in pending {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !wait_for_pending(&item, remaining).await {
                abort_pending(&item).await;
            }
            lock(&self.inner.state)
                .pending
                .retain(|_, candidate| !Arc::ptr_eq(candidate, &item));
        }
        let records: Vec<_> = {
            let state = lock(&self.inner.state);
            state
                .sessions
                .values()
                .filter(|record| record.owner.same(owner))
                .cloned()
                .collect()
        };
        self.close_many(records).await
    }

    pub async fn dispose(&self) -> Result<(), TerminalError> {
        {
            let mut state = lock(&self.inner.state);
            state.disposing = true;
        }
        let (pending, records): (Vec<_>, Vec<_>) = {
            let state = lock(&self.inner.state);
            (
                state.pending.values().cloned().collect(),
                state.sessions.values().cloned().collect(),
            )
        };
        for item in &pending {
            item.cancellation.cancel();
        }
        let deadline = Instant::now() + self.inner.config.teardown_grace;
        let mut errors = Vec::new();
        for item in pending {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !wait_for_pending(&item, remaining).await {
                abort_pending(&item).await;
            }
            let settled = lock(&item.result).take();
            if let Some(Ok(session)) = settled {
                if let Err(error) =
                    timeout_close(&session, "service disposal".to_owned(), Duration::ZERO).await
                {
                    errors.push(error);
                }
            }
        }
        lock(&self.inner.state).pending.clear();
        if let Err(error) = self.close_many(records).await {
            errors.push(error);
        }
        lock(&self.inner.state).sessions.clear();
        lock(&self.inner.state).backends.clear();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(TerminalError::new(
                "CLEANUP_FAILED",
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            ))
        }
    }

    async fn close_many(&self, records: Vec<Arc<SessionRecord>>) -> Result<(), TerminalError> {
        let mut errors = Vec::new();
        for record in records {
            if let Err(error) = close_record(
                &record,
                "owner disposed".to_owned(),
                self.inner.config.teardown_grace,
            )
            .await
            {
                errors.push(format!("{}: {error}", record.id));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(TerminalError::new("CLEANUP_FAILED", errors.join("; ")))
        }
    }

    fn ensure_owner(&self, owner: &Owner) -> Result<(), TerminalError> {
        if !owner.is_live() {
            return Err(TerminalError::new(
                "OWNER_NOT_LIVE",
                "terminal owner is not live",
            ));
        }
        Ok(())
    }

    fn expect(&self, owner: &Owner, id: &str) -> Result<Arc<SessionRecord>, TerminalError> {
        self.ensure_owner(owner)?;
        let record = lock(&self.inner.state)
            .sessions
            .get(id)
            .cloned()
            .ok_or_else(|| TerminalError::new("NO_SESSION", "terminal session does not exist"))?;
        if !record.owner.same(owner) {
            return Err(TerminalError::new(
                "FOREIGN_SESSION",
                "terminal session belongs to another owner",
            ));
        }
        Ok(record)
    }
}

impl SessionRecord {
    fn is_closing(&self) -> bool {
        lock(&self.closing).is_some()
    }

    fn snapshot(&self) -> TerminalSessionSnapshot {
        TerminalSessionSnapshot {
            id: self.id.clone(),
            name: self.name.clone(),
            backend: self.backend.clone(),
            pid: self.session.pid(),
            status: self.session.status(),
            created_at: self.created_at,
            active_send: lock(&self.active_send).is_some(),
            closing: self.is_closing(),
            cleanup_error: lock(&self.cleanup_error).clone(),
        }
    }
}

async fn wait_for_pending(pending: &PendingSpawn, timeout: Duration) -> bool {
    let notified = pending.finished.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    // Checked after arming so a completion in between cannot be missed.
    if lock(&pending.result).is_some() {
        return true;
    }
    tokio::select! {
        _ = &mut notified => lock(&pending.result).is_some(),
        _ = tokio::time::sleep(timeout) => lock(&pending.result).is_some(),
    }
}

async fn abort_pending(pending: &PendingSpawn) {
    let task = lock(&pending.task).take();
    if let Some(task) = task {
        task.abort();
        let _ = task.await;
    }
    if lock(&pending.result).is_none() {
        *lock(&pending.result) = Some(Err(TerminalError::new(
            "CANCELLED",
            "terminal spawn task was aborted",
        )));
    }
    pending.finished.notify_waiters();
}

async fn timeout_close(
    session: &Arc<dyn TerminalBackendSession>,
    reason: String,
    timeout: Duration,
) -> Result<(), TerminalError> {
    match tokio::time::timeout(timeout, session.close(reason)).await {
        Ok(result) => result,
        Err(_) => Err(TerminalError::new(
            "CLEANUP_FAILED",
            "terminal close exceeded teardown grace",
        )),
    }
}

async fn close_record(
    record: &Arc<SessionRecord>,
    reason: String,
    grace: Duration,
) -> Result<(), TerminalError> {
    let state = {
        let mut closing = lock(&record.closing);
        if let Some(existing) = &*closing {
            existing.clone()
        } else {
            let state = Arc::new(CloseState {
                finished: Notify::new(),
                result: Mutex::new(None),
                task: Mutex::new(None),
            });
            let task_state = state.clone();
            let session = record.session.clone();
            let task_reason = reason.clone();
            let task = tokio::spawn(async move {
                let result = session.close(task_reason).await;
                *lock(&task_state.result) = Some(result);
                task_state.finished.notify_waiters();
            });
            *lock(&state.task) = Some(task);
            *closing = Some(state.clone());
            state
        }
    };
    let notified = state.finished.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    // Checked after arming: a fast close may already have published its result.
    let already_finished = lock(&state.result).is_some();
    if !already_finished && tokio::time::timeout(grace, &mut notified).await.is_err() {
        let task = lock(&state.task).take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        *lock(&record.closing) = None;
        *lock(&record.cleanup_error) = Some("terminal close exceeded teardown grace".to_owned());
        return Err(TerminalError::new(
            "CLEANUP_FAILED",
            "terminal close exceeded teardown grace",
        ));
    }
    let result = lock(&state.result).clone().unwrap_or_else(|| {
        Err(TerminalError::new(
            "CLEANUP_FAILED",
            "terminal close returned no result",
        ))
    });
    lock(&state.task).take();
    let mut state_lock = lock(&record.closing);
    if state_lock
        .as_ref()
        .map(|item| Arc::ptr_eq(item, &state))
        .unwrap_or(false)
    {
        *state_lock = None;
    }
    drop(state_lock);
    match result {
        Ok(()) => {
            lock(&record.cleanup_error).take();
            if let Some(registry) = record.registry.upgrade() {
                lock(&registry.state).sessions.remove(&record.id);
            }
            Ok(())
        }
        Err(error) => {
            *lock(&record.cleanup_error) = Some(error.to_string());
            Err(error)
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalTerminalConfig {
    pub scrollback_lines: usize,
    pub scrollback_max_bytes: usize,
    pub max_read_bytes: usize,
    pub poll_interval: Duration,
    pub idle_silence: Duration,
    pub timeout: Duration,
    pub dispose_grace: Duration,
    pub write_timeout: Duration,
}

impl Default for LocalTerminalConfig {
    fn default() -> Self {
        Self {
            scrollback_lines: 10_000,
            scrollback_max_bytes: 4 * 1024 * 1024,
            max_read_bytes: 256 * 1024,
            poll_interval: Duration::from_millis(50),
            idle_silence: Duration::from_secs(3),
            timeout: Duration::from_secs(30),
            dispose_grace: Duration::from_secs(3),
            write_timeout: Duration::from_secs(2),
        }
    }
}

pub struct LocalTerminalBackend {
    config: LocalTerminalConfig,
}

impl LocalTerminalBackend {
    pub fn new(config: LocalTerminalConfig) -> Result<Self, TerminalError> {
        if config.scrollback_lines == 0
            || config.scrollback_max_bytes == 0
            || config.max_read_bytes == 0
            || config.poll_interval.is_zero()
            || config.idle_silence.is_zero()
            || config.timeout.is_zero()
            || config.dispose_grace.is_zero()
            || config.write_timeout.is_zero()
        {
            return Err(TerminalError::new(
                "INVALID_CONFIG",
                "local terminal limits and timings must be positive",
            ));
        }
        if config.max_read_bytes > config.scrollback_max_bytes {
            return Err(TerminalError::new(
                "INVALID_CONFIG",
                "max_read_bytes exceeds scrollback_max_bytes",
            ));
        }
        Ok(Self { config })
    }

    pub fn shell() -> Result<Self, TerminalError> {
        #[cfg(windows)]
        let command = TerminalCommand::new("powershell.exe", ["-NoLogo", "-NoProfile"]);
        #[cfg(unix)]
        let command = TerminalCommand::new("/bin/bash", ["--noprofile", "--norc", "-i"]);
        let _ = command;
        Self::new(LocalTerminalConfig::default())
    }
}

impl TerminalBackend for LocalTerminalBackend {
    fn kind(&self) -> &str {
        "local"
    }

    fn spawn(
        &self,
        spec: TerminalBackendSpawnSpec,
    ) -> BoxFuture<Result<Arc<dyn TerminalBackendSession>, TerminalError>> {
        let config = self.config.clone();
        Box::pin(async move {
            if spec.cancellation.is_cancelled() {
                return Err(TerminalError::new(
                    "CANCELLED",
                    "terminal spawn was cancelled",
                ));
            }
            let process = ManagedProcess::spawn(ProcessSpawnSpec {
                command: spec.command,
                cwd: spec.cwd,
                env: spec.env,
                size: spec.size,
            })
            .map_err(|error| TerminalError::new("INIT_REJECTED", error.to_string()))?;
            let session: Arc<dyn TerminalBackendSession> =
                Arc::new(LocalSession::start(process, config.clone()));
            let startup = match session.start_send(TerminalSendRequest {
                text: String::new(),
                submit: false,
                timeout: config.timeout,
                cancellation: Some(spec.cancellation.clone()),
            }) {
                Ok(startup) => startup,
                Err(error) => {
                    let _ = timeout_close(
                        &session,
                        "startup rejected".to_owned(),
                        config.dispose_grace,
                    )
                    .await;
                    return Err(error);
                }
            };
            let startup_result = tokio::select! {
                result = startup.done() => result,
                _ = spec.cancellation.cancelled() => {
                    startup.cancel();
                    let _ = timeout_close(&session, "startup cancelled".to_owned(), config.dispose_grace).await;
                    return Err(TerminalError::new("CANCELLED", "terminal startup was cancelled"));
                }
            };
            let failure = match &startup_result {
                Err(error) => Some(error.clone()),
                Ok(result) if result.wait_reason == TerminalWaitReason::SessionExit => Some(
                    TerminalError::new("STARTUP_EXITED", "shell exited during startup"),
                ),
                Ok(result) if result.wait_reason == TerminalWaitReason::Timeout => {
                    Some(TerminalError::new(
                        "STARTUP_TIMEOUT",
                        "shell did not become ready before timeout",
                    ))
                }
                Ok(_) => None,
            };
            if let Some(error) = failure {
                let _ = timeout_close(&session, "startup failed".to_owned(), config.dispose_grace)
                    .await;
                return Err(error);
            }
            let _ = startup.read_output();
            Ok(session)
        })
    }
}

struct LocalSession {
    inner: Arc<LocalInner>,
}

struct LocalInner {
    config: LocalTerminalConfig,
    process: ManagedProcess,
    output: Mutex<BoundedTextBuffer>,
    last_output: Mutex<Instant>,
    status: Mutex<TerminalSessionStatus>,
    closing: AtomicBool,
    active: Mutex<Option<Arc<LocalSend>>>,
    reader_done: watch::Sender<bool>,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    waiter_task: Mutex<Option<JoinHandle<()>>>,
}

impl LocalSession {
    fn start(process: ManagedProcess, config: LocalTerminalConfig) -> Self {
        let mut output_rx = process
            .take_output()
            .expect("terminal output is taken once");
        let mut exit = process.exit.clone();
        let (reader_done, _) = watch::channel(false);
        let inner = Arc::new(LocalInner {
            output: Mutex::new(BoundedTextBuffer::new(
                config.scrollback_lines,
                config.scrollback_max_bytes,
            )),
            config,
            process,
            last_output: Mutex::new(Instant::now()),
            status: Mutex::new(TerminalSessionStatus::Running),
            closing: AtomicBool::new(false),
            active: Mutex::new(None),
            reader_done,
            reader_task: Mutex::new(None),
            waiter_task: Mutex::new(None),
        });
        // Background tasks hold weak references so dropping an unclosed session
        // still reaches ManagedProcess::drop and kills the process tree.
        let reader = Arc::downgrade(&inner);
        let reader_task = tokio::spawn(async move {
            let mut decoder = Utf8Decoder::default();
            let mut sanitizer = TerminalSanitizer::default();
            while let Some(bytes) = output_rx.recv().await {
                let Some(inner) = reader.upgrade() else {
                    return;
                };
                let text = decoder.push(&bytes);
                let clean = sanitizer.push(&text);
                if !clean.is_empty() {
                    inner.append_output(&clean);
                }
            }
            if let Some(inner) = reader.upgrade() {
                inner.reader_done.send_replace(true);
            }
        });
        *lock(&inner.reader_task) = Some(reader_task);
        let waiter = Arc::downgrade(&inner);
        let waiter_task = tokio::spawn(async move {
            let code = exit
                .wait_for(Option::is_some)
                .await
                .ok()
                .and_then(|value| *value);
            let Some(inner) = waiter.upgrade() else {
                return;
            };
            // ConPTY keeps its output pipe open until the console is closed.
            inner.process.release_terminal().await;
            let mut done = inner.reader_done.subscribe();
            let drained =
                tokio::time::timeout(inner.config.dispose_grace, done.wait_for(|value| *value))
                    .await;
            drop(drained);
            if !inner.closing.load(Ordering::SeqCst) {
                *lock(&inner.status) = TerminalSessionStatus::Exited {
                    exit_code: code,
                    signal: None,
                };
            }
            inner.settle_active(TerminalWaitReason::SessionExit);
        });
        *lock(&inner.waiter_task) = Some(waiter_task);
        Self { inner }
    }
}

impl LocalInner {
    fn status(&self) -> TerminalSessionStatus {
        lock(&self.status).clone()
    }

    fn append_output(&self, text: &str) {
        lock(&self.output).append(text);
        *lock(&self.last_output) = Instant::now();
        let active = lock(&self.active).clone();
        if let Some(operation) = active {
            lock(&operation.output).append(text);
        }
    }

    fn clear_active(&self, operation: &Arc<LocalSend>) {
        let mut active = lock(&self.active);
        if active
            .as_ref()
            .is_some_and(|item| Arc::ptr_eq(item, operation))
        {
            *active = None;
        }
    }

    fn finish_send(&self, operation: &Arc<LocalSend>, reason: TerminalWaitReason) {
        self.clear_active(operation);
        operation.finish(reason, self.status());
    }

    fn fail_send(&self, operation: &Arc<LocalSend>, error: TerminalError) {
        self.clear_active(operation);
        operation.finish_result(Err(error));
    }

    fn settle_active(&self, reason: TerminalWaitReason) {
        let active = lock(&self.active).clone();
        if let Some(operation) = active {
            self.finish_send(&operation, reason);
        }
    }

    async fn close(&self) -> Result<(), TerminalError> {
        if self.closing.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let result = self.process.terminate(self.config.dispose_grace).await;
        if result.is_err() {
            // Leave the session closable so a retry can still reap the process.
            self.closing.store(false, Ordering::SeqCst);
            return result;
        }
        {
            let mut status = lock(&self.status);
            if !status.is_exited() {
                *status = TerminalSessionStatus::Exited {
                    exit_code: None,
                    signal: Some(TerminalSignal::Sigterm),
                };
            }
        }
        self.settle_active(TerminalWaitReason::SessionExit);
        let deadline = Instant::now() + self.config.dispose_grace;
        let tasks = [
            lock(&self.reader_task).take(),
            lock(&self.waiter_task).take(),
        ];
        for mut task in tasks.into_iter().flatten() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remaining, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
        result
    }

    async fn run_send(&self, operation: Arc<LocalSend>, request: TerminalSendRequest) {
        let input = format!("{}{}", request.text, if request.submit { "\r" } else { "" });
        if !input.is_empty() {
            if let Err(error) = self
                .process
                .write_all(input.as_bytes(), self.config.write_timeout)
                .await
            {
                self.fail_send(
                    &operation,
                    TerminalError::new("WRITE_FAILED", error.to_string()),
                );
                return;
            }
        }
        let deadline = Instant::now() + request.timeout;
        let mut interrupted = false;
        loop {
            if operation.is_finished() {
                return;
            }
            if self.closing.load(Ordering::SeqCst) || self.status().is_exited() {
                self.finish_send(&operation, TerminalWaitReason::SessionExit);
                return;
            }
            tokio::select! {
                biased;
                _ = operation.cancel_notify.notified() => {}
                _ = async {
                    match &request.cancellation {
                        Some(token) if !interrupted => token.cancelled().await,
                        _ => std::future::pending::<()>().await,
                    }
                } => {}
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }
            let cancel_requested = operation.cancel_requested.load(Ordering::SeqCst)
                || request
                    .cancellation
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled);
            if cancel_requested && !interrupted {
                interrupted = true;
                if let Err(error) = self.process.signal_foreground(TerminalSignal::Sigint).await {
                    self.fail_send(&operation, error);
                    return;
                }
            }
            let last_output = *lock(&self.last_output);
            if last_output.elapsed() >= self.config.idle_silence {
                self.finish_send(&operation, TerminalWaitReason::InferredIdle);
                return;
            }
            if Instant::now() >= deadline {
                self.finish_send(&operation, TerminalWaitReason::Timeout);
                return;
            }
        }
    }
}

impl TerminalBackendSession for LocalSession {
    fn pid(&self) -> Option<u32> {
        Some(self.inner.process.pid)
    }

    fn start_send(
        &self,
        request: TerminalSendRequest,
    ) -> Result<TerminalSendOperationHandle, TerminalError> {
        let inner = &self.inner;
        if inner.closing.load(Ordering::SeqCst) || inner.status().is_exited() {
            return Err(TerminalError::new(
                "SESSION_EXITED",
                "terminal session has exited",
            ));
        }
        if request
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(TerminalError::new(
                "CANCELLED",
                "terminal send was cancelled before start",
            ));
        }
        let operation = Arc::new(LocalSend::new(inner.config.clone()));
        {
            let mut active = lock(&inner.active);
            if active.is_some() {
                return Err(TerminalError::new(
                    "SEND_ACTIVE",
                    "terminal send is already active",
                ));
            }
            // Idle detection measures silence from the start of this send.
            *lock(&inner.last_output) = Instant::now();
            *active = Some(operation.clone());
        }
        let worker = inner.clone();
        let worker_operation = operation.clone();
        let task = tokio::spawn(async move {
            worker.run_send(worker_operation, request).await;
        });
        *lock(&operation.task) = Some(task);
        Ok(TerminalSendOperationHandle(operation))
    }

    fn read(&self, offset: Option<u64>, count: usize) -> TerminalReadResult {
        let buffer = lock(&self.inner.output);
        buffer.read_lines(offset.unwrap_or(0), count, self.inner.config.max_read_bytes)
    }

    fn signal(
        &self,
        signal: TerminalSignal,
    ) -> BoxFuture<Result<TerminalSignalResult, TerminalError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            if inner.status().is_exited() {
                return Err(TerminalError::new(
                    "SESSION_EXITED",
                    "terminal session has exited",
                ));
            }
            inner.process.signal_foreground(signal).await
        })
    }

    fn resize(&self, cols: u16, rows: u16) -> BoxFuture<Result<(), TerminalError>> {
        let inner = self.inner.clone();
        Box::pin(async move { inner.process.resize(cols, rows) })
    }

    fn status(&self) -> TerminalSessionStatus {
        self.inner.status()
    }

    fn close(&self, _reason: String) -> BoxFuture<Result<(), TerminalError>> {
        let inner = self.inner.clone();
        Box::pin(async move { inner.close().await })
    }
}

struct LocalSend {
    config: LocalTerminalConfig,
    output: Mutex<BoundedTextBuffer>,
    cursor: Mutex<u64>,
    result: watch::Sender<Option<Result<TerminalSendResult, TerminalError>>>,
    cancel_requested: AtomicBool,
    cancel_notify: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl LocalSend {
    fn new(config: LocalTerminalConfig) -> Self {
        let (result, _) = watch::channel(None);
        Self {
            output: Mutex::new(BoundedTextBuffer::new(
                config.scrollback_lines,
                config.scrollback_max_bytes,
            )),
            cursor: Mutex::new(0),
            config,
            result,
            cancel_requested: AtomicBool::new(false),
            cancel_notify: Notify::new(),
            task: Mutex::new(None),
        }
    }

    fn is_finished(&self) -> bool {
        self.result.borrow().is_some()
    }

    fn finish(&self, reason: TerminalWaitReason, status: TerminalSessionStatus) {
        let truncated = lock(&self.output).truncated;
        self.finish_result(Ok(TerminalSendResult {
            viewport: self.viewport(),
            wait_reason: reason,
            session_status: status,
            truncated,
        }));
    }

    /// First settlement wins; later results are ignored.
    fn finish_result(&self, result: Result<TerminalSendResult, TerminalError>) {
        self.result.send_if_modified(|slot| {
            if slot.is_some() {
                return false;
            }
            *slot = Some(result);
            true
        });
    }

    fn viewport(&self) -> String {
        lock(&self.output).tail(
            self.config.scrollback_lines.min(40),
            self.config.max_read_bytes,
        )
    }
}

impl TerminalSendOperation for LocalSend {
    fn done(self: Arc<Self>) -> BoxFuture<Result<TerminalSendResult, TerminalError>> {
        Box::pin(async move {
            let mut result = self.result.subscribe();
            let settled = result
                .wait_for(Option::is_some)
                .await
                .ok()
                .and_then(|value| (*value).clone());
            settled.unwrap_or_else(|| {
                Err(TerminalError::new(
                    "SESSION_EXIT",
                    "send task ended without a result",
                ))
            })
        })
    }

    fn read_output(self: Arc<Self>) -> TerminalReadResult {
        let output = lock(&self.output);
        let mut cursor = lock(&self.cursor);
        let start = *cursor;
        *cursor = output.total;
        output.read_bytes(start, self.config.max_read_bytes)
    }

    fn cancel(self: Arc<Self>) -> bool {
        if self.is_finished() {
            return false;
        }
        self.cancel_requested.store(true, Ordering::SeqCst);
        self.cancel_notify.notify_one();
        true
    }
}

#[derive(Debug)]
struct BoundedTextBuffer {
    text: String,
    total: u64,
    truncated: bool,
    max_lines: usize,
    max_bytes: usize,
}

impl BoundedTextBuffer {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            text: String::new(),
            total: 0,
            truncated: false,
            max_lines,
            max_bytes,
        }
    }

    fn append(&mut self, text: &str) {
        self.text.push_str(text);
        self.total += text.len() as u64;
        while self.text.len() > self.max_bytes {
            let mut boundary = self.text.len() - self.max_bytes;
            while !self.text.is_char_boundary(boundary) {
                boundary += 1;
            }
            if boundary >= self.text.len() {
                self.text.clear();
                break;
            }
            self.text.drain(..boundary);
            self.truncated = true;
        }
        let lines = self.text.bytes().filter(|byte| *byte == b'\n').count()
            + usize::from(!self.text.is_empty() && !self.text.ends_with('\n'));
        if lines > self.max_lines {
            let remove_lines = lines - self.max_lines;
            let mut seen = 0;
            let mut boundary = 0;
            for (index, byte) in self.text.bytes().enumerate() {
                if byte == b'\n' {
                    seen += 1;
                    if seen == remove_lines {
                        boundary = index + 1;
                        break;
                    }
                }
            }
            self.text.drain(..boundary);
            self.truncated = true;
        }
    }

    fn line_count(&self) -> usize {
        if self.text.is_empty() {
            0
        } else {
            self.text.split('\n').count()
        }
    }

    fn read_lines(&self, offset: u64, count: usize, max_bytes: usize) -> TerminalReadResult {
        let lines: Vec<_> = if self.text.is_empty() {
            Vec::new()
        } else {
            self.text.split('\n').collect()
        };
        let total_lines = lines.len();
        let offset = (offset.min(total_lines as u64)) as usize;
        if offset >= total_lines {
            return TerminalReadResult {
                text: String::new(),
                total_lines,
                line_begin: offset,
                line_end: offset,
                truncated: self.truncated,
            };
        }
        let end = total_lines - offset;
        let start = end.saturating_sub(count);
        let joined = lines[start..end].join("\n");
        let text = utf8_tail(&joined, max_bytes);
        let clipped = text.len() < joined.len();
        TerminalReadResult {
            text,
            total_lines,
            line_begin: start,
            line_end: end,
            truncated: self.truncated || clipped,
        }
    }

    fn read_bytes(&self, from: u64, max_bytes: usize) -> TerminalReadResult {
        let kept_start = self.total - self.text.len() as u64;
        let text = if from < kept_start {
            String::new()
        } else {
            utf8_tail(&self.text[(from - kept_start) as usize..], max_bytes)
        };
        let line_begin = self.text[..self.text.len().saturating_sub(text.len())]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
        TerminalReadResult {
            text,
            total_lines: self.line_count(),
            line_begin,
            line_end: self.line_count(),
            truncated: self.truncated || from < kept_start,
        }
    }

    fn tail(&self, lines: usize, max_bytes: usize) -> String {
        let text: Vec<_> = if self.text.is_empty() {
            Vec::new()
        } else {
            self.text.split('\n').collect()
        };
        utf8_tail(
            &text[text.len().saturating_sub(lines)..].join("\n"),
            max_bytes,
        )
    }
}

fn utf8_tail(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut boundary = text.len() - max_bytes;
    while !text.is_char_boundary(boundary) {
        boundary += 1;
    }
    text[boundary..].to_owned()
}

#[derive(Default)]
struct Utf8Decoder {
    carry: Vec<u8>,
}

impl Utf8Decoder {
    fn push(&mut self, bytes: &[u8]) -> String {
        self.carry.extend_from_slice(bytes);
        let mut output = String::new();
        let mut consumed = 0;
        while consumed < self.carry.len() {
            match std::str::from_utf8(&self.carry[consumed..]) {
                Ok(text) => {
                    output.push_str(text);
                    consumed = self.carry.len();
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid > 0 {
                        output.push_str(
                            std::str::from_utf8(&self.carry[consumed..consumed + valid])
                                .expect("validated"),
                        );
                        consumed += valid;
                    }
                    if error.error_len().is_none() {
                        break;
                    }
                    output.push('\u{fffd}');
                    consumed += error.error_len().expect("invalid sequence length");
                }
            }
        }
        self.carry.drain(..consumed);
        if self.carry.len() > 3 {
            self.carry.clear();
        }
        output
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum SanitizerState {
    #[default]
    Normal,
    Escape,
    Csi,
    Osc,
    OscEscape,
}

#[derive(Default)]
struct TerminalSanitizer {
    state: SanitizerState,
}

impl TerminalSanitizer {
    fn push(&mut self, text: &str) -> String {
        let mut output = String::new();
        for ch in text.chars() {
            match self.state {
                SanitizerState::Normal => match ch {
                    '\x1b' => self.state = SanitizerState::Escape,
                    '\r' => {}
                    '\n' | '\t' => output.push(ch),
                    ch if !ch.is_control() => output.push(ch),
                    _ => {}
                },
                SanitizerState::Escape => match ch {
                    '[' => self.state = SanitizerState::Csi,
                    ']' => self.state = SanitizerState::Osc,
                    _ => self.state = SanitizerState::Normal,
                },
                SanitizerState::Csi => {
                    if ('\x40'..='\x7e').contains(&ch) {
                        self.state = SanitizerState::Normal;
                    }
                }
                SanitizerState::Osc => match ch {
                    '\x07' => self.state = SanitizerState::Normal,
                    '\x1b' => self.state = SanitizerState::OscEscape,
                    _ => {}
                },
                SanitizerState::OscEscape => {
                    self.state = if ch == '\\' {
                        SanitizerState::Normal
                    } else {
                        SanitizerState::Osc
                    }
                }
            }
        }
        output
    }
}

#[derive(Debug, Clone)]
struct ProcessSpawnSpec {
    command: TerminalCommand,
    cwd: Option<PathBuf>,
    env: Vec<(String, String)>,
    size: TerminalSize,
}

type WriteCommand = (Vec<u8>, oneshot::Sender<io::Result<()>>);

/// A process attached to a PTY. Blocking PTY I/O and the exit wait run on
/// dedicated threads so they cannot pin the Tokio blocking pool or stall
/// runtime shutdown.
struct ManagedProcess {
    pid: u32,
    core: platform::PlatformCore,
    writer: Mutex<Option<std_mpsc::SyncSender<WriteCommand>>>,
    output: Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    exit: watch::Receiver<Option<i32>>,
}

impl ManagedProcess {
    fn spawn(spec: ProcessSpawnSpec) -> io::Result<Self> {
        let parts = platform::spawn(&spec)?;
        let platform::PtyParts {
            pid,
            core,
            reader,
            writer,
            waiter,
        } = parts;
        let (exit_tx, exit) = watch::channel(None);
        let threads = spawn_reader(reader).and_then(|output| Ok((output, spawn_writer(writer)?)));
        let (output, writer) = match threads {
            Ok(value) => value,
            Err(error) => {
                core.force_kill();
                return Err(error);
            }
        };
        let waited = std::thread::Builder::new()
            .name("dsh-pty-wait".to_owned())
            .spawn(move || {
                exit_tx.send_replace(Some(platform::wait(waiter)));
            });
        if let Err(error) = waited {
            core.force_kill();
            return Err(error);
        }
        Ok(Self {
            pid,
            core,
            writer: Mutex::new(Some(writer)),
            output: Mutex::new(Some(output)),
            exit,
        })
    }

    fn take_output(&self) -> Option<mpsc::Receiver<Vec<u8>>> {
        lock(&self.output).take()
    }

    async fn write_all(&self, bytes: &[u8], timeout: Duration) -> io::Result<()> {
        let sender = lock(&self.writer)
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "terminal input is closed"))?;
        let (reply, done) = oneshot::channel();
        sender
            .try_send((bytes.to_vec(), reply))
            .map_err(|error| match error {
                std_mpsc::TrySendError::Full(_) => {
                    io::Error::new(io::ErrorKind::WouldBlock, "terminal input queue is full")
                }
                std_mpsc::TrySendError::Disconnected(_) => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "terminal input is closed")
                }
            })?;
        match tokio::time::timeout(timeout, done).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "terminal writer stopped",
            )),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "process write timed out",
            )),
        }
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<(), TerminalError> {
        self.core.resize(cols, rows)
    }

    async fn signal_foreground(
        &self,
        signal: TerminalSignal,
    ) -> Result<TerminalSignalResult, TerminalError> {
        // ConPTY turns an ETX byte on its input into CTRL_C_EVENT for the
        // attached foreground processes.
        #[cfg(windows)]
        if signal == TerminalSignal::Sigint {
            self.write_all(&[3], Duration::from_secs(2))
                .await
                .map_err(|error| TerminalError::new("SIGNAL_FAILED", error.to_string()))?;
            return Ok(TerminalSignalResult {
                signal,
                target: self.pid,
            });
        }
        let target = self.core.signal(signal)?;
        Ok(TerminalSignalResult { signal, target })
    }

    async fn wait_code(&self, timeout: Duration) -> Option<i32> {
        let mut exit = self.exit.clone();
        let waited = tokio::time::timeout(timeout, exit.wait_for(Option::is_some))
            .await
            .ok()
            .and_then(|result| result.ok().and_then(|value| *value));
        waited.or_else(|| *self.exit.borrow())
    }

    async fn release_terminal(&self) {
        self.core.release_terminal().await;
    }

    async fn terminate(&self, grace: Duration) -> Result<(), TerminalError> {
        self.core.request_stop();
        if self.wait_code(grace).await.is_none() {
            self.core.force_kill();
            if self.wait_code(grace).await.is_none() {
                return Err(TerminalError::new(
                    "CLEANUP_FAILED",
                    "managed process was not reaped before teardown grace",
                ));
            }
        }
        self.core
            .reap_group(grace)
            .await
            .map_err(|error| TerminalError::new("CLEANUP_FAILED", error.to_string()))?;
        self.release_terminal().await;
        lock(&self.writer).take();
        Ok(())
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if self.exit.borrow().is_none() {
            self.core.force_kill();
        }
    }
}

fn spawn_reader(mut file: File) -> io::Result<mpsc::Receiver<Vec<u8>>> {
    let (sender, receiver) = mpsc::channel(16);
    std::thread::Builder::new()
        .name("dsh-pty-read".to_owned())
        .spawn(move || {
            let mut bytes = vec![0_u8; 8192];
            loop {
                match file.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(count) => {
                        if sender.blocking_send(bytes[..count].to_vec()).is_err() {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    // EIO (POSIX) and a broken pipe (Windows) both mean the PTY closed.
                    Err(_) => break,
                }
            }
        })?;
    Ok(receiver)
}

fn spawn_writer(mut file: File) -> io::Result<std_mpsc::SyncSender<WriteCommand>> {
    let (sender, receiver) = std_mpsc::sync_channel::<WriteCommand>(16);
    std::thread::Builder::new()
        .name("dsh-pty-write".to_owned())
        .spawn(move || {
            while let Ok((bytes, reply)) = receiver.recv() {
                let result = file.write_all(&bytes).and_then(|()| file.flush());
                let _ = reply.send(result);
            }
        })?;
    Ok(sender)
}

fn is_credential_key(key: &str) -> bool {
    let upper = key.to_uppercase();
    ["KEY", "SECRET", "TOKEN", "PASSWORD"]
        .iter()
        .any(|word| upper.contains(word))
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    use std::process::{Child, Command, Stdio};

    pub(super) type Waiter = Child;

    pub(super) struct PtyParts {
        pub(super) pid: u32,
        pub(super) core: PlatformCore,
        pub(super) reader: File,
        pub(super) writer: File,
        pub(super) waiter: Waiter,
    }

    pub(super) struct PlatformCore {
        pid: i32,
        master: File,
    }

    impl PlatformCore {
        pub(super) fn resize(&self, cols: u16, rows: u16) -> Result<(), TerminalError> {
            let size = libc::winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            let result =
                unsafe { libc::ioctl(self.master.as_raw_fd(), libc::TIOCSWINSZ as _, &size) };
            if result < 0 {
                return Err(TerminalError::new(
                    "RESIZE_FAILED",
                    io::Error::last_os_error().to_string(),
                ));
            }
            Ok(())
        }

        pub(super) fn signal(&self, signal: TerminalSignal) -> Result<u32, TerminalError> {
            let foreground = unsafe { libc::tcgetpgrp(self.master.as_raw_fd()) };
            let group = if foreground > 0 { foreground } else { self.pid };
            if unsafe { libc::kill(-group, signal_number(signal)) } < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(TerminalError::new("SIGNAL_FAILED", error.to_string()));
                }
            }
            Ok(group as u32)
        }

        pub(super) fn request_stop(&self) {
            unsafe { libc::kill(-self.pid, libc::SIGTERM) };
        }

        pub(super) fn force_kill(&self) {
            unsafe { libc::kill(-self.pid, libc::SIGKILL) };
        }

        /// Kills and waits out descendants that outlived the session leader.
        pub(super) async fn reap_group(&self, grace: Duration) -> io::Result<()> {
            if unsafe { libc::kill(-self.pid, 0) } != 0 {
                return Ok(());
            }
            self.force_kill();
            let deadline = Instant::now() + grace;
            while unsafe { libc::kill(-self.pid, 0) } == 0 {
                if Instant::now() >= deadline {
                    return Err(io::Error::other(
                        "POSIX process group did not become quiescent",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        }

        pub(super) async fn release_terminal(&self) {}
    }

    pub(super) fn wait(mut child: Waiter) -> i32 {
        match child.wait() {
            Ok(status) => status
                .code()
                .or_else(|| status.signal().map(|signal| 128 + signal))
                .unwrap_or(-1),
            Err(_) => -1,
        }
    }

    pub(super) fn spawn(spec: &ProcessSpawnSpec) -> io::Result<PtyParts> {
        let (master, slave) = open_pty(spec.size)?;
        let mut command = Command::new(&spec.command.program);
        command.args(&spec.command.args);
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        for (key, _) in std::env::vars_os() {
            if is_credential_key(&key.to_string_lossy()) {
                command.env_remove(key);
            }
        }
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        command
            .env("TERM", "dumb")
            .env("PAGER", "cat")
            .env("GIT_PAGER", "cat");
        command.stdin(Stdio::from(slave.try_clone()?));
        command.stdout(Stdio::from(slave.try_clone()?));
        command.stderr(Stdio::from(slave));
        // SAFETY: the closure only makes async-signal-safe calls. Stdio is
        // already installed, so fd 0 is the PTY slave.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn()?;
        let pid = child.id();
        let master = File::from(master);
        let reader = master.try_clone()?;
        let writer = master.try_clone()?;
        Ok(PtyParts {
            pid,
            core: PlatformCore {
                pid: pid as i32,
                master,
            },
            reader,
            writer,
            waiter: child,
        })
    }

    fn open_pty(size: TerminalSize) -> io::Result<(OwnedFd, OwnedFd)> {
        let mut master = -1;
        let mut slave = -1;
        let mut window = libc::winsize {
            ws_row: size.rows,
            ws_col: size.cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let result = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut window,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openpty succeeded and returned two descriptors we now own.
        let (master, slave) =
            unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
        for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
            if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok((master, slave))
    }

    fn signal_number(signal: TerminalSignal) -> i32 {
        match signal {
            TerminalSignal::Sigint => libc::SIGINT,
            TerminalSignal::Sigterm => libc::SIGTERM,
            TerminalSignal::Sigkill => libc::SIGKILL,
            TerminalSignal::Sigtstp => libc::SIGTSTP,
            TerminalSignal::Sighup => libc::SIGHUP,
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use std::path::Path;
    use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Console::{
        ClosePseudoConsole, CreatePseudoConsole, ResizePseudoConsole, COORD, HPCON,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::Threading::{
        CreateProcessW, DeleteProcThreadAttributeList, GetExitCodeProcess,
        InitializeProcThreadAttributeList, ResumeThread, TerminateProcess,
        UpdateProcThreadAttribute, WaitForSingleObject, CREATE_SUSPENDED,
        CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT, INFINITE, PROCESS_INFORMATION,
        PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, STARTF_USESTDHANDLES, STARTUPINFOEXW, STARTUPINFOW,
    };

    pub(super) type Waiter = OwnedHandle;

    pub(super) struct PtyParts {
        pub(super) pid: u32,
        pub(super) core: PlatformCore,
        pub(super) reader: File,
        pub(super) writer: File,
        pub(super) waiter: Waiter,
    }

    pub(super) struct PlatformCore {
        pid: u32,
        job: OwnedHandle,
        console: Mutex<Option<HPCON>>,
    }

    impl PlatformCore {
        pub(super) fn resize(&self, cols: u16, rows: u16) -> Result<(), TerminalError> {
            let console = *lock(&self.console);
            let console = console.ok_or_else(|| {
                TerminalError::new("SESSION_EXITED", "terminal console is closed")
            })?;
            let result = unsafe { ResizePseudoConsole(console, coord(cols, rows)) };
            if result < 0 {
                return Err(TerminalError::new(
                    "RESIZE_FAILED",
                    format!("ResizePseudoConsole HRESULT {result:#x}"),
                ));
            }
            Ok(())
        }

        pub(super) fn signal(&self, signal: TerminalSignal) -> Result<u32, TerminalError> {
            match signal {
                TerminalSignal::Sigterm | TerminalSignal::Sigkill => {
                    if unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) } == 0 {
                        return Err(TerminalError::new(
                            "SIGNAL_FAILED",
                            io::Error::last_os_error().to_string(),
                        ));
                    }
                    Ok(self.pid)
                }
                _ => Err(TerminalError::new(
                    "UNSUPPORTED",
                    "Windows terminals do not support this signal",
                )),
            }
        }

        pub(super) fn request_stop(&self) {
            self.force_kill();
        }

        pub(super) fn force_kill(&self) {
            unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) };
        }

        /// The job object already covers every descendant.
        pub(super) async fn reap_group(&self, _grace: Duration) -> io::Result<()> {
            Ok(())
        }

        /// ClosePseudoConsole can block until output drains, so it runs off the executor.
        pub(super) async fn release_terminal(&self) {
            let console = lock(&self.console).take();
            if let Some(console) = console {
                let _ = tokio::task::spawn_blocking(move || unsafe { ClosePseudoConsole(console) })
                    .await;
            }
        }
    }

    impl Drop for PlatformCore {
        fn drop(&mut self) {
            let console = lock(&self.console).take();
            if let Some(console) = console {
                unsafe { ClosePseudoConsole(console) };
            }
        }
    }

    /// Closes the console if spawning fails before ownership reaches the core.
    struct ConsoleGuard(HPCON);

    impl Drop for ConsoleGuard {
        fn drop(&mut self) {
            unsafe { ClosePseudoConsole(self.0) };
        }
    }

    struct AttributeList(Vec<usize>);

    impl AttributeList {
        fn pseudoconsole(console: HPCON) -> io::Result<Self> {
            let mut bytes = 0;
            unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut bytes) };
            let words = bytes.div_ceil(std::mem::size_of::<usize>());
            let mut storage = vec![0_usize; words];
            if unsafe {
                InitializeProcThreadAttributeList(storage.as_mut_ptr().cast(), 1, 0, &mut bytes)
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            let mut list = Self(storage);
            // The attribute value is the HPCON itself, not a pointer to it.
            let updated = unsafe {
                UpdateProcThreadAttribute(
                    list.as_mut_ptr(),
                    0,
                    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
                    console as *const core::ffi::c_void,
                    std::mem::size_of::<HPCON>(),
                    std::ptr::null_mut(),
                    std::ptr::null(),
                )
            };
            if updated == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(list)
        }

        fn as_mut_ptr(&mut self) -> *mut core::ffi::c_void {
            self.0.as_mut_ptr().cast()
        }
    }

    impl Drop for AttributeList {
        fn drop(&mut self) {
            unsafe { DeleteProcThreadAttributeList(self.as_mut_ptr()) };
        }
    }

    fn coord(cols: u16, rows: u16) -> COORD {
        COORD {
            X: cols.min(i16::MAX as u16) as i16,
            Y: rows.min(i16::MAX as u16) as i16,
        }
    }

    pub(super) fn wait(process: Waiter) -> i32 {
        if unsafe { WaitForSingleObject(process.as_raw_handle(), INFINITE) } != WAIT_OBJECT_0 {
            return -1;
        }
        let mut code = 0_u32;
        if unsafe { GetExitCodeProcess(process.as_raw_handle(), &mut code) } == 0 {
            return -1;
        }
        code as i32
    }

    pub(super) fn spawn(spec: &ProcessSpawnSpec) -> io::Result<PtyParts> {
        let job = create_job()?;
        let (input_read, input_write) = pipe()?;
        let (output_read, output_write) = pipe()?;
        let mut raw_console: HPCON = 0;
        let result = unsafe {
            CreatePseudoConsole(
                coord(spec.size.cols, spec.size.rows),
                input_read.as_raw_handle(),
                output_write.as_raw_handle(),
                0,
                &mut raw_console,
            )
        };
        if result < 0 {
            return Err(io::Error::other(format!(
                "CreatePseudoConsole HRESULT {result:#x}"
            )));
        }
        let console = ConsoleGuard(raw_console);
        let mut attributes = AttributeList::pseudoconsole(console.0)?;
        // SAFETY: STARTUPINFOEXW is a plain C struct for which all-zero is valid.
        let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
        startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        // Explicitly empty stdio stops a redirected parent's handles from
        // overriding the pseudoconsole in the child.
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
        startup.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
        startup.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
        startup.lpAttributeList = attributes.as_mut_ptr();
        let (process, pid) = create_process(spec, &startup, &job)?;
        drop(attributes);
        // ConPTY duplicated its ends; holding them would prevent EOF.
        drop(input_read);
        drop(output_write);
        let waiter = process;
        let raw_console = console.0;
        std::mem::forget(console);
        Ok(PtyParts {
            pid,
            core: PlatformCore {
                pid,
                job,
                console: Mutex::new(Some(raw_console)),
            },
            reader: File::from(output_read),
            writer: File::from(input_write),
            waiter,
        })
    }

    fn pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
        let mut read = std::ptr::null_mut();
        let mut write = std::ptr::null_mut();
        if unsafe { CreatePipe(&mut read, &mut write, std::ptr::null(), 64 * 1024) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreatePipe succeeded and returned two handles we now own.
        Ok(unsafe {
            (
                OwnedHandle::from_raw_handle(read),
                OwnedHandle::from_raw_handle(write),
            )
        })
    }

    fn create_job() -> io::Result<OwnedHandle> {
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateJobObjectW succeeded and returned a handle we now own.
        let job = unsafe { OwnedHandle::from_raw_handle(handle) };
        // SAFETY: this is a plain C struct for which all-zero is valid.
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let result = unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const info).cast(),
                std::mem::size_of_val(&info) as u32,
            )
        };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    /// Starts suspended so the process is in the job before it can run or fork.
    fn create_process(
        spec: &ProcessSpawnSpec,
        startup: &STARTUPINFOEXW,
        job: &OwnedHandle,
    ) -> io::Result<(OwnedHandle, u32)> {
        let program = resolve_program(&spec.command.program)?;
        let application = wide(&program.to_string_lossy());
        let mut command = wide(&format_command_line(&program, &spec.command.args));
        let cwd = spec.cwd.as_ref().map(|path| wide(&path.to_string_lossy()));
        let environment = environment_block(&spec.env);
        // SAFETY: PROCESS_INFORMATION is a plain C struct for which all-zero is valid.
        let mut info: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
        let flags = CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT;
        let created = unsafe {
            CreateProcessW(
                application.as_ptr(),
                command.as_mut_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                0,
                flags,
                environment.as_ptr().cast(),
                cwd.as_ref()
                    .map_or(std::ptr::null(), |value| value.as_ptr()),
                std::ptr::from_ref(startup).cast::<STARTUPINFOW>(),
                &mut info,
            )
        };
        if created == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateProcessW succeeded and returned two handles we now own.
        let (process, thread) = unsafe {
            (
                OwnedHandle::from_raw_handle(info.hProcess),
                OwnedHandle::from_raw_handle(info.hThread),
            )
        };
        if unsafe { AssignProcessToJobObject(job.as_raw_handle(), process.as_raw_handle()) } == 0 {
            let error = io::Error::last_os_error();
            unsafe { TerminateProcess(process.as_raw_handle(), 1) };
            return Err(error);
        }
        if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
            let error = io::Error::last_os_error();
            unsafe { TerminateJobObject(job.as_raw_handle(), 1) };
            return Err(error);
        }
        Ok((process, info.dwProcessId))
    }

    fn resolve_program(program: &Path) -> io::Result<PathBuf> {
        if program.exists() {
            return Ok(program.to_owned());
        }
        let name = program
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "program has no file name"))?
            .to_string_lossy()
            .into_owned();
        if let Some(paths) = std::env::var_os("PATH") {
            for directory in std::env::split_paths(&paths) {
                for candidate in [directory.join(&name), directory.join(format!("{name}.exe"))] {
                    if candidate.is_file() {
                        return Ok(candidate);
                    }
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("executable not found: {}", program.display()),
        ))
    }

    /// Builds a sorted, double-NUL-terminated block without credential variables.
    fn environment_block(extra: &[(String, String)]) -> Vec<u16> {
        let mut values: HashMap<String, String> = std::env::vars_os()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().to_uppercase(),
                    value.to_string_lossy().into_owned(),
                )
            })
            .filter(|(key, _)| !is_credential_key(key))
            .collect();
        for (key, value) in extra {
            values.insert(key.to_uppercase(), value.clone());
        }
        let mut entries: Vec<_> = values.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut block = Vec::new();
        for (key, value) in entries {
            block.extend(wide(&format!("{key}={value}")));
        }
        if block.is_empty() {
            block.push(0);
        }
        block.push(0);
        block
    }

    fn format_command_line(program: &Path, args: &[String]) -> String {
        let mut output = quote_argument(&program.to_string_lossy());
        for argument in args {
            output.push(' ');
            output.push_str(&quote_argument(argument));
        }
        output
    }

    fn quote_argument(argument: &str) -> String {
        if !argument.is_empty() && !argument.chars().any(|ch| ch.is_whitespace() || ch == '"') {
            return argument.to_owned();
        }
        let mut output = String::from("\"");
        let mut slashes = 0;
        for ch in argument.chars() {
            match ch {
                '\\' => slashes += 1,
                '"' => {
                    output.push_str(&"\\".repeat(slashes * 2 + 1));
                    output.push('"');
                    slashes = 0;
                }
                _ => {
                    output.push_str(&"\\".repeat(slashes));
                    slashes = 0;
                    output.push(ch);
                }
            }
        }
        output.push_str(&"\\".repeat(slashes * 2));
        output.push('"');
        output
    }

    fn wide(value: &str) -> Vec<u16> {
        value.encode_utf16().chain(std::iter::once(0)).collect()
    }
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

    struct FakeSession {
        pid: u32,
        status: Mutex<TerminalSessionStatus>,
        output: Mutex<BoundedTextBuffer>,
        active: AtomicBool,
        closes: AtomicUsize,
    }

    impl FakeSession {
        fn new(pid: u32) -> Self {
            Self {
                pid,
                status: Mutex::new(TerminalSessionStatus::Running),
                output: Mutex::new(BoundedTextBuffer::new(100, 1024)),
                active: AtomicBool::new(false),
                closes: AtomicUsize::new(0),
            }
        }
    }

    impl TerminalBackendSession for FakeSession {
        fn pid(&self) -> Option<u32> {
            Some(self.pid)
        }
        fn start_send(
            &self,
            request: TerminalSendRequest,
        ) -> Result<TerminalSendOperationHandle, TerminalError> {
            if self.active.swap(true, Ordering::SeqCst) {
                return Err(TerminalError::new("SEND_ACTIVE", "active"));
            }
            let session = Arc::new(Self::new(self.pid));
            let output = lock(&session.output);
            drop(output);
            let operation = Arc::new(FakeSend {
                output: Mutex::new(BoundedTextBuffer::new(100, 1024)),
                result: Mutex::new(None),
                notify: Notify::new(),
                session: session.clone(),
            });
            *lock(&operation.result) = Some(Ok(TerminalSendResult {
                viewport: request.text.clone(),
                wait_reason: TerminalWaitReason::InferredIdle,
                session_status: TerminalSessionStatus::Running,
                truncated: false,
            }));
            operation.notify.notify_waiters();
            Ok(TerminalSendOperationHandle(operation))
        }
        fn read(&self, offset: Option<u64>, count: usize) -> TerminalReadResult {
            lock(&self.output).read_lines(offset.unwrap_or(0), count, 256)
        }
        fn signal(
            &self,
            signal: TerminalSignal,
        ) -> BoxFuture<Result<TerminalSignalResult, TerminalError>> {
            let target = self.pid;
            Box::pin(async move { Ok(TerminalSignalResult { signal, target }) })
        }
        fn resize(&self, _cols: u16, _rows: u16) -> BoxFuture<Result<(), TerminalError>> {
            Box::pin(async { Ok(()) })
        }
        fn status(&self) -> TerminalSessionStatus {
            lock(&self.status).clone()
        }
        fn close(&self, _reason: String) -> BoxFuture<Result<(), TerminalError>> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            *lock(&self.status) = TerminalSessionStatus::Exited {
                exit_code: Some(0),
                signal: None,
            };
            Box::pin(async { Ok(()) })
        }
    }

    struct FakeSend {
        output: Mutex<BoundedTextBuffer>,
        result: Mutex<Option<Result<TerminalSendResult, TerminalError>>>,
        notify: Notify,
        session: Arc<FakeSession>,
    }
    impl TerminalSendOperation for FakeSend {
        fn done(self: Arc<Self>) -> BoxFuture<Result<TerminalSendResult, TerminalError>> {
            Box::pin(async move {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if lock(&self.result).is_none() {
                    notified.await;
                }
                lock(&self.result).clone().unwrap()
            })
        }
        fn read_output(self: Arc<Self>) -> TerminalReadResult {
            lock(&self.output).read_bytes(0, 256)
        }
        fn cancel(self: Arc<Self>) -> bool {
            self.session.active.store(false, Ordering::SeqCst);
            true
        }
    }

    struct FakeBackend {
        sessions: Mutex<Vec<Arc<FakeSession>>>,
    }
    impl FakeBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                sessions: Mutex::new(Vec::new()),
            })
        }
    }
    impl TerminalBackend for FakeBackend {
        fn kind(&self) -> &str {
            "fake"
        }
        fn spawn(
            &self,
            _spec: TerminalBackendSpawnSpec,
        ) -> BoxFuture<Result<Arc<dyn TerminalBackendSession>, TerminalError>> {
            let session = Arc::new(FakeSession::new(123));
            lock(&self.sessions).push(session.clone());
            let session: Arc<dyn TerminalBackendSession> = session;
            Box::pin(async move { Ok(session) })
        }
    }

    fn request() -> TerminalSpawnRequest {
        TerminalSpawnRequest::new("fake", TerminalCommand::new("fake", Vec::<String>::new()))
    }

    #[tokio::test]
    async fn registry_fences_owners_names_and_active_sends() {
        let registry = TerminalRegistry::default();
        let backend = FakeBackend::new();
        registry.register_backend(backend.clone()).unwrap();
        let owner = Owner::new("a");
        let foreign = Owner::new("b");
        let spawned = registry
            .spawn(&owner, request().name("one"), None)
            .await
            .unwrap();
        assert_eq!(spawned.id, "pty-1");
        assert!(registry
            .spawn(&owner, request().name("one"), None)
            .await
            .is_err());
        assert!(registry
            .signal(&foreign, &spawned.id, TerminalSignal::Sigint)
            .await
            .is_err());
        let operation = registry
            .start_send(&owner, &spawned.id, TerminalSendRequest::new("x", true))
            .unwrap();
        assert_eq!(operation.done().await.unwrap().viewport, "x");
        // The registry monitor clears the slot asynchronously; wait for snapshot.
        for _ in 0..20 {
            if !registry.list(&owner).unwrap()[0].active_send {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!registry.list(&owner).unwrap()[0].active_send);
        assert!(registry.kill(&owner, &spawned.id, None).await.unwrap());
        assert!(registry.list(&owner).unwrap().is_empty());
        registry.dispose().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_unpublished_spawn_is_rolled_back() {
        let registry = TerminalRegistry::default();
        let backend = FakeBackend::new();
        registry.register_backend(backend.clone()).unwrap();
        let owner = Owner::new("a");
        let token = CancellationToken::new();
        token.cancel();
        assert!(registry
            .spawn(&owner, request(), Some(&token))
            .await
            .is_err());
        assert!(!registry.has_owner_activity(&owner));
        registry.dispose().await.unwrap();
    }

    #[test]
    fn bounded_buffer_preserves_utf8_and_newest_relative_lines() {
        let mut buffer = BoundedTextBuffer::new(2, 9);
        buffer.append("one\ntwo\néé");
        assert_eq!(buffer.line_count(), 2);
        buffer.append("\nthree");
        let read = buffer.read_lines(0, 1, 256);
        assert_eq!(read.line_begin, 1);
        assert_eq!(read.line_end, 2);
        assert!(read.text.contains("three"));
        assert!(buffer.text.is_char_boundary(0));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_real_conpty_spawn_write_resize_and_kill() {
        let backend = LocalTerminalBackend::new(LocalTerminalConfig {
            poll_interval: Duration::from_millis(5),
            idle_silence: Duration::from_millis(400),
            timeout: Duration::from_secs(3),
            dispose_grace: Duration::from_secs(2),
            ..LocalTerminalConfig::default()
        })
        .unwrap();
        let registry = TerminalRegistry::default();
        registry.register_backend(Arc::new(backend)).unwrap();
        let owner = Owner::new("windows-test");
        let spawned = registry
            .spawn(
                &owner,
                TerminalSpawnRequest::new(
                    "local",
                    TerminalCommand::new("cmd.exe", ["/D", "/Q", "/K"]),
                )
                .size(TerminalSize::new(100, 30).unwrap()),
                None,
            )
            .await
            .unwrap();
        registry.resize(&owner, &spawned.id, 120, 40).await.unwrap();
        let sent = registry
            .start_send(
                &owner,
                &spawned.id,
                TerminalSendRequest::new("echo dsh-terminal-ready", true)
                    .timeout(Duration::from_secs(3)),
            )
            .unwrap();
        let result = sent.done().await.unwrap();
        assert_eq!(result.wait_reason, TerminalWaitReason::InferredIdle);
        let output = sent.read_output().text;
        assert!(output.contains("dsh-terminal-ready"), "output: {output:?}");
        registry
            .signal(&owner, &spawned.id, TerminalSignal::Sigint)
            .await
            .unwrap();
        assert!(registry.kill(&owner, &spawned.id, None).await.unwrap());
        registry.dispose().await.unwrap();
    }
}
