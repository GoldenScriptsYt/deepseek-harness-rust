//! Browser-facing terminal and background-job controllers.
//!
//! The TypeScript API owns the wire contract. This module deliberately keeps all
//! registries in the route closures: there is no process-global terminal or job
//! state, and a detached browser follower never owns a process.

use crate::api::ApiContext;
use dsh_gateway::RpcError;
use futures_util::{
    future::{self, join_all, Future},
    stream::{self, BoxStream, Stream, StreamExt},
};
use serde_json::{json, Value};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_TERMINALS: usize = 8;
const MAX_COLS: u16 = 500;
const MAX_ROWS: u16 = 200;
const SCROLLBACK: usize = 1000;
const MAX_FOLLOWER_BYTES: usize = 2 * 1024 * 1024;
const MAX_INPUT_BYTES: usize = 64 * 1024;
const DISPOSE_GRACE: Duration = Duration::from_millis(1000);
const OBSERVE_FLUSH: Duration = Duration::from_millis(100);
const OBSERVE_FRAME_BYTES: usize = 64 * 1024;
const KILL_REASON: &str = "cancelled by the user";
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

fn rpc_error(code: &str, message: impl Into<String>, details: Value) -> RpcError {
    RpcError {
        code: code.to_owned(),
        message: message.into(),
        details,
    }
}

fn invalid(message: impl Into<String>) -> RpcError {
    rpc_error("gateway/arguments-invalid", message, json!({}))
}

fn unavailable(message: &str) -> RpcError {
    rpc_error("terminal/unavailable", message, json!({}))
}

fn internal(message: impl Into<String>) -> RpcError {
    rpc_error("gateway/invocation-failed", message, json!({}))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn request(payload: &Value) -> &Value {
    let args = payload.get("args").unwrap_or(payload);
    args.get("request").unwrap_or(args)
}

fn string_field<'a>(value: &'a Value, name: &str) -> Result<&'a str, RpcError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{name} must be a string")))
}

fn session_id(payload: &Value) -> Result<String, RpcError> {
    let args = payload.get("args").unwrap_or(payload);
    let req = request(payload);
    for value in [req, args] {
        if let Some(id) = value.get("sessionId").and_then(Value::as_str) {
            if !id.is_empty() {
                return Ok(id.to_owned());
            }
        }
        if let Some(id) = value.get("session_id").and_then(Value::as_str) {
            if !id.is_empty() {
                return Ok(id.to_owned());
            }
        }
    }
    for value in [args.get("agent"), req.get("agent"), args.get("session")] {
        let Some(value) = value else { continue };
        for key in ["id", "sessionId", "session_id"] {
            if let Some(id) = value.get(key).and_then(Value::as_str) {
                if !id.is_empty() {
                    return Ok(id.to_owned());
                }
            }
        }
        if let Some(session) = value.get("session") {
            for key in ["id", "sessionId", "session_id"] {
                if let Some(id) = session.get(key).and_then(Value::as_str) {
                    if !id.is_empty() {
                        return Ok(id.to_owned());
                    }
                }
            }
        }
    }
    Err(invalid("sessionId is required"))
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn terminal_id(payload: &Value) -> Result<String, RpcError> {
    let id = string_field(request(payload), "id")?;
    if !valid_id(id) {
        return Err(invalid("Invalid terminal identity"));
    }
    Ok(id.to_owned())
}

fn dimension(value: &Value, name: &str, minimum: u16, maximum: u16) -> Result<u16, RpcError> {
    let number = value
        .get(name)
        .and_then(Value::as_u64)
        .filter(|number| *number <= u64::from(u16::MAX))
        .ok_or_else(|| invalid(format!("{name} must be an integer")))?;
    let number = number as u16;
    if number < minimum || number > maximum {
        return Err(invalid(format!(
            "{name} must be between {minimum} and {maximum}"
        )));
    }
    Ok(number)
}

fn dimensions(value: &Value) -> Result<(u16, u16), RpcError> {
    Ok((
        dimension(value, "cols", 2, MAX_COLS)?,
        dimension(value, "rows", 1, MAX_ROWS)?,
    ))
}

fn sequence(value: &Value) -> Result<u64, RpcError> {
    value
        .get("sequence")
        .and_then(Value::as_u64)
        .filter(|sequence| *sequence > 0 && *sequence <= MAX_SAFE_INTEGER)
        .ok_or_else(|| invalid("sequence must be a positive safe integer"))
}

fn terminal_title(value: &Value) -> Result<String, RpcError> {
    let raw = string_field(value, "title")?;
    if raw.encode_utf16().count() > 120 {
        return Err(invalid("Terminal title must contain 1–120 characters"));
    }
    let title = raw.trim().to_owned();
    if title.is_empty() {
        return Err(invalid("Terminal title must contain 1–120 characters"));
    }
    Ok(title)
}

fn encoded_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

/// Register the complete client-used terminal and job Remote surface.
pub fn register_all(gw: &dsh_gateway::Gateway, cx: Arc<ApiContext>) -> Arc<Controller> {
    let controller = Arc::new(Controller::new(cx));

    {
        let controller = controller.clone();
        gw.rpc("terminal/environment", move |payload| {
            let controller = controller.clone();
            async move {
                let session = session_id(&payload)?;
                controller.environment(&session)
            }
        });
    }
    {
        let controller = controller.clone();
        gw.rpc("terminal/shells", move |payload| {
            let controller = controller.clone();
            async move {
                let session = session_id(&payload)?;
                controller.shells(&session).await
            }
        });
    }
    {
        let controller = controller.clone();
        gw.rpc("terminal/list", move |payload| {
            let controller = controller.clone();
            async move {
                let session = session_id(&payload)?;
                controller.terminal_list(&session)
            }
        });
    }
    {
        let controller = controller.clone();
        gw.stream("terminal/watch", move |payload, _input| {
            let controller = controller.clone();
            controller.terminal_watch(&payload)
        });
    }
    {
        let controller = controller.clone();
        gw.rpc("terminal/create", move |payload| {
            let controller = controller.clone();
            async move { controller.create(&payload).await }
        });
    }
    {
        let controller = controller.clone();
        gw.stream("terminal/retain", move |payload, _input| {
            let controller = controller.clone();
            controller.retain(&payload)
        });
    }
    {
        let controller = controller.clone();
        gw.stream("terminal/attach", move |payload, input| {
            let controller = controller.clone();
            controller.attach(&payload, input)
        });
    }
    {
        let controller = controller.clone();
        gw.rpc("terminal/rename", move |payload| {
            let controller = controller.clone();
            async move {
                let session = session_id(&payload)?;
                let id = terminal_id(&payload)?;
                let mut title = terminal_title(request(&payload))?;
                let terminal = controller.terminal(&session, &id)?;
                if lock(&terminal.info).state != "running" {
                    lock(&terminal.info).title = title.clone();
                    controller.changed();
                    title.shrink_to_fit();
                    return Ok(Value::Null);
                }
                let (reply, result) = oneshot::channel();
                terminal
                    .command(Command::Rename {
                        title: title.clone(),
                        reply,
                    })
                    .await?;
                result
                    .await
                    .map_err(|_| internal("Terminal rename task ended unexpectedly"))??;
                title.shrink_to_fit();
                Ok(Value::Null)
            }
        });
    }
    {
        let controller = controller.clone();
        gw.rpc("terminal/close", move |payload| {
            let controller = controller.clone();
            async move {
                let session = session_id(&payload)?;
                let id = terminal_id(&payload)?;
                controller.close(&session, &id).await
            }
        });
    }
    {
        let controller = controller.clone();
        gw.stream("job/list", move |payload, _input| {
            let controller = controller.clone();
            controller.job_list(&payload)
        });
    }
    {
        let controller = controller.clone();
        gw.stream("job/follow", move |payload, _input| {
            let controller = controller.clone();
            controller.job_follow(&payload)
        });
    }
    {
        let controller = controller.clone();
        gw.rpc("job/kill", move |payload| {
            let controller = controller.clone();
            async move { controller.kill_job(&payload) }
        });
    }
    controller
}

struct AllocationCancellation {
    pending: Arc<PendingCreate>,
    armed: bool,
}

impl Drop for AllocationCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.pending.cancelled.store(true, Ordering::Release);
        }
    }
}

#[derive(Default)]
struct SessionState {
    terminals: HashMap<String, TerminalHandle>,
    pending: HashMap<String, Arc<PendingCreate>>,
    closed: HashSet<String>,
}

struct PendingCreate {
    notify: watch::Sender<Option<Result<Value, RpcError>>>,
    cancelled: AtomicBool,
}

struct PendingCleanup {
    state: Arc<Mutex<State>>,
    session: String,
    id: String,
    pending: Arc<PendingCreate>,
}

impl Drop for PendingCleanup {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        if let Some(owner) = state.sessions.get_mut(&self.session) {
            if owner
                .pending
                .get(&self.id)
                .is_some_and(|pending| Arc::ptr_eq(pending, &self.pending))
            {
                owner.pending.remove(&self.id);
            }
        }
    }
}

type RemovalQueue = Arc<Mutex<VecDeque<Value>>>;

struct State {
    sessions: HashMap<String, SessionState>,
    jobs: Vec<Job>,
    observers: HashMap<String, Vec<(u64, RemovalQueue)>>,
    next_observer: u64,
    disposed: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            sessions: HashMap::new(),
            jobs: Vec::new(),
            observers: HashMap::new(),
            next_observer: 1,
            disposed: false,
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        for job in &self.jobs {
            if job.is_live() {
                if let Some(cancel) = &job.cancel {
                    cancel(KILL_REASON.to_owned());
                }
            }
        }
    }
}

/// Owns the terminal and job RPC state for one Host.
#[derive(Clone)]
pub struct Controller {
    context: Arc<ApiContext>,
    state: Arc<Mutex<State>>,
    revision: watch::Sender<u64>,
}

impl Controller {
    fn new(context: Arc<ApiContext>) -> Self {
        let (revision, _) = watch::channel(0);
        Self {
            context,
            state: Arc::new(Mutex::new(State::default())),
            revision,
        }
    }

    fn changed(&self) {
        self.revision.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }

    fn session_cwd(&self, session: &str) -> Result<PathBuf, RpcError> {
        let stored = dsh_session_persistence::Store::mixed(self.context.home.join("sessions"))
            .read_all()
            .into_iter()
            .find(|stored| stored.header.id == session)
            .and_then(|stored| stored.header.cwd)
            .map(PathBuf::from);
        let cwd = stored.unwrap_or_else(|| self.context.home.clone());
        Ok(cwd)
    }

    fn environment(&self, session: &str) -> Result<Value, RpcError> {
        if lock(&self.state).disposed {
            return Err(unavailable("Terminal controller disposed"));
        }
        let cwd = self.session_cwd(session)?;
        Ok(json!({
            "cwd": cwd.to_string_lossy(),
            "maxInputBytes": MAX_INPUT_BYTES,
            "maxBufferedBytes": MAX_FOLLOWER_BYTES,
            "maxCols": MAX_COLS,
            "maxRows": MAX_ROWS,
            "scrollback": SCROLLBACK,
        }))
    }

    async fn shells(&self, session: &str) -> Result<Value, RpcError> {
        if lock(&self.state).disposed {
            return Err(unavailable("Terminal controller disposed"));
        }
        let cwd = self.session_cwd(session)?;
        let shells = discover_shells(&cwd).await?;
        Ok(serde_json::to_value(shells).unwrap_or(Value::Null))
    }

    fn terminal_list(&self, session: &str) -> Result<Value, RpcError> {
        let state = lock(&self.state);
        let mut output = Vec::new();
        if let Some(owner) = state.sessions.get(session) {
            output.extend(
                owner
                    .terminals
                    .values()
                    .map(|terminal| terminal.info_value()),
            );
        }
        output.sort_by_key(|info| info["id"].as_str().unwrap_or_default().to_owned());
        Ok(Value::Array(output))
    }

    fn terminal(&self, session: &str, id: &str) -> Result<TerminalHandle, RpcError> {
        let state = lock(&self.state);
        let owner = state
            .sessions
            .get(session)
            .ok_or_else(|| unavailable("Terminal no longer exists in this Session"))?;
        if owner.closed.contains(id) {
            return Err(unavailable("Terminal was closed in this Session"));
        }
        owner
            .terminals
            .get(id)
            .cloned()
            .ok_or_else(|| unavailable("Terminal no longer exists in this Session"))
    }

    async fn create(&self, payload: &Value) -> Result<Value, RpcError> {
        if lock(&self.state).disposed {
            return Err(unavailable("Terminal controller disposed"));
        }
        let session = session_id(payload)?;
        let req = request(payload);
        let id = terminal_id(payload)?;
        let (cols, rows) = dimensions(req)?;
        let selected = match req.get("shellPath") {
            None => None,
            Some(Value::String(path)) => Some(path.clone()),
            Some(_) => return Err(invalid("shellPath must be a string")),
        };
        if selected
            .as_ref()
            .is_some_and(|path| path.is_empty() || path.len() > 4096)
        {
            return Err(invalid("shellPath is invalid"));
        }
        let existing_pending = {
            let mut state = lock(&self.state);
            let owner = state.sessions.entry(session.clone()).or_default();
            if owner.closed.contains(&id) {
                return Err(unavailable("Terminal was closed in this Session"));
            }
            if let Some(terminal) = owner.terminals.get(&id) {
                return Ok(terminal.info_value());
            }
            owner.pending.get(&id).cloned()
        };
        if let Some(pending) = existing_pending {
            let mut result = pending.notify.subscribe();
            let current = result.borrow_and_update().clone();
            if let Some(result) = current {
                return result;
            }
            loop {
                if result.changed().await.is_err() {
                    return Err(internal("Terminal allocation task ended unexpectedly"));
                }
                let current = result.borrow_and_update().clone();
                if let Some(result) = current {
                    return result;
                }
            }
        }
        let pending = {
            let mut state = lock(&self.state);
            let owner = state.sessions.entry(session.clone()).or_default();
            if owner.closed.contains(&id) {
                return Err(unavailable("Terminal was closed in this Session"));
            }
            if let Some(terminal) = owner.terminals.get(&id) {
                return Ok(terminal.info_value());
            }
            if owner.pending.contains_key(&id) {
                return Err(internal("Terminal allocation changed while joining"));
            }
            if owner.terminals.len() + owner.pending.len() >= MAX_TERMINALS {
                return Err(rpc_error(
                    "terminal/limit-reached",
                    "Session terminal limit reached",
                    json!({ "limit": MAX_TERMINALS }),
                ));
            }
            let (notify, _) = watch::channel(None);
            let pending = Arc::new(PendingCreate {
                notify,
                cancelled: AtomicBool::new(false),
            });
            owner.pending.insert(id.clone(), pending.clone());
            pending
        };
        let (result_tx, result_rx) = oneshot::channel();
        let this = self.clone();
        let allocation_pending = Arc::clone(&pending);
        let task_pending = pending.clone();
        tokio::spawn(async move {
            let result = this
                .allocate(session, id, cols, rows, selected, task_pending)
                .await;
            let _ = result_tx.send(result.clone());
            allocation_pending.notify.send_replace(Some(result));
        });
        let mut guard = AllocationCancellation {
            pending: pending.clone(),
            armed: true,
        };
        let result = result_rx
            .await
            .map_err(|_| internal("Terminal allocation task ended unexpectedly"))?;
        guard.armed = false;
        drop(guard);
        if pending.cancelled.load(Ordering::Acquire) && result.is_err() {
            return Err(invalid("Terminal allocation cancelled"));
        }
        result
    }

    async fn allocate(
        &self,
        session: String,
        id: String,
        cols: u16,
        rows: u16,
        selected: Option<String>,
        pending: Arc<PendingCreate>,
    ) -> Result<Value, RpcError> {
        AllocationController {
            context: self.context.clone(),
            state: self.state.clone(),
            revision: self.revision.clone(),
        }
        .allocate(session, id, cols, rows, selected, pending)
        .await
    }

    async fn close(&self, session: &str, id: &str) -> Result<Value, RpcError> {
        let pending = {
            let mut state = lock(&self.state);
            let owner = state.sessions.entry(session.to_owned()).or_default();
            owner.closed.insert(id.to_owned());
            owner.pending.get(id).cloned()
        };
        if let Some(pending) = pending {
            let mut result = pending.notify.subscribe();
            while result.borrow_and_update().is_none() {
                if result.changed().await.is_err() {
                    break;
                }
            }
        }
        let terminal = {
            let state = lock(&self.state);
            state
                .sessions
                .get(session)
                .and_then(|owner| owner.terminals.get(id))
                .cloned()
        };
        if let Some(terminal) = terminal {
            if lock(&terminal.info).state != "running" {
                lock(&self.state)
                    .sessions
                    .get_mut(session)
                    .and_then(|owner| owner.terminals.remove(id));
            } else {
                let (reply, result) = oneshot::channel();
                terminal.command(Command::Close { reply }).await?;
                result
                    .await
                    .map_err(|_| internal("Terminal close task ended unexpectedly"))??;
                lock(&self.state)
                    .sessions
                    .get_mut(session)
                    .and_then(|owner| owner.terminals.remove(id));
            }
        }
        self.changed();
        Ok(Value::Null)
    }

    fn terminal_watch(&self, payload: &Value) -> BoxStream<'static, Result<Value, RpcError>> {
        let session = match session_id(payload) {
            Ok(session) => session,
            Err(error) => return error_stream(error),
        };
        let revision = self.revision.subscribe();
        let view = self.view();
        let initial = view.terminal_rows(&session);
        stream::unfold(
            Some((session, view, initial.clone(), revision, true)),
            move |state| async move {
                let (session, view, mut last, mut revision, first) = state?;
                loop {
                    if first {
                        return Some((
                            Ok(Value::Array(last.clone())),
                            Some((session, view, last, revision, false)),
                        ));
                    }
                    if revision.changed().await.is_err() {
                        return None;
                    }
                    let next = view.terminal_rows(&session);
                    if next != last {
                        last = next.clone();
                        return Some((
                            Ok(Value::Array(next)),
                            Some((session, view, last, revision, false)),
                        ));
                    }
                }
            },
        )
        .boxed()
    }

    fn retain(&self, payload: &Value) -> BoxStream<'static, Result<Value, RpcError>> {
        let result = (|| {
            let session = session_id(payload)?;
            let id = terminal_id(payload)?;
            let terminal = self.terminal(&session, &id)?;
            let alive = terminal.alive.subscribe();
            Ok::<_, RpcError>((terminal, alive))
        })();
        match result {
            Err(error) => error_stream(error),
            Ok((terminal, alive)) => stream::once(future::ready(Ok(json!({ "type": "retained" }))))
                .chain(stream::unfold(
                    (terminal, alive),
                    |(terminal, mut alive)| async move {
                        if !*alive.borrow_and_update() {
                            return None;
                        }
                        if alive.changed().await.is_err() || !*alive.borrow() {
                            return None;
                        }
                        let _ = lock(&terminal.info).state;
                        None
                    },
                ))
                .boxed(),
        }
    }

    fn attach(
        &self,
        payload: &Value,
        input: mpsc::Receiver<Value>,
    ) -> BoxStream<'static, Result<Value, RpcError>> {
        match (|| {
            let session = session_id(payload)?;
            let id = terminal_id(payload)?;
            let terminal = self.terminal(&session, &id)?;
            Ok::<_, RpcError>(AttachmentStream::opening(terminal, input))
        })() {
            Ok(stream) => stream.boxed(),
            Err(error) => error_stream(error),
        }
    }

    fn job_list(&self, payload: &Value) -> BoxStream<'static, Result<Value, RpcError>> {
        let session = match session_id(payload) {
            Ok(session) => session,
            Err(error) => return error_stream(error),
        };
        let revision = self.revision.subscribe();
        let view = self.view();
        let rows = view.job_rows(&session);
        let mut signature = rows.clone();
        for row in &mut signature {
            if let Some(object) = row.as_object_mut() {
                object.remove("output");
            }
        }
        let initial = json!({ "type": "rows", "jobs": rows });
        stream::unfold(
            Some((session, view, initial, signature, revision, true)),
            move |state| async move {
                let (session, view, initial, mut signature, mut revision, first) = state?;
                if first {
                    return Some((
                        Ok(initial),
                        Some((session, view, Value::Null, signature, revision, false)),
                    ));
                }
                loop {
                    if revision.changed().await.is_err() {
                        return None;
                    }
                    tokio::time::sleep(OBSERVE_FLUSH).await;
                    let rows = view.job_rows(&session);
                    let mut next_signature = rows.clone();
                    for row in &mut next_signature {
                        if let Some(object) = row.as_object_mut() {
                            object.remove("output");
                        }
                    }
                    if next_signature != signature {
                        signature = next_signature;
                        return Some((
                            Ok(json!({ "type": "rows", "jobs": rows })),
                            Some((session, view, Value::Null, signature, revision, false)),
                        ));
                    }
                }
            },
        )
        .boxed()
    }

    fn job_follow(&self, payload: &Value) -> BoxStream<'static, Result<Value, RpcError>> {
        let req = request(payload);
        let opened = (|| {
            let job_id = string_field(req, "jobId")?.to_owned();
            if job_id.is_empty() || job_id.len() > 256 {
                return Err(invalid("jobId is invalid"));
            }
            let session = match req.get("sessionId") {
                None => None,
                Some(Value::String(session)) if !session.is_empty() => Some(session.clone()),
                _ => return Err(invalid("sessionId must be a nonempty string")),
            };
            let from = match req.get("from") {
                None => None,
                Some(value) => Some(
                    value
                        .as_u64()
                        .filter(|offset| *offset <= MAX_SAFE_INTEGER)
                        .ok_or_else(|| {
                            invalid("invalid observe offset: expected a non-negative safe integer")
                        })?,
                ),
            };
            let revision = self.revision.subscribe();
            let (observer, removals) = self.observe_removal(&job_id);
            let state = lock(&self.state);
            let position = state
                .jobs
                .iter()
                .position(|job| job.id == job_id && job.visible(session.as_deref()))
                .ok_or_else(|| {
                    rpc_error(
                        "job/not-found",
                        "Job is not visible to this Session",
                        json!({ "sessionId": session, "jobId": job_id }),
                    )
                })?;
            let job = state.jobs[position].clone();
            let cursor = from.unwrap_or(job.earliest);
            Ok::<_, RpcError>(FollowState {
                id: job_id,
                session,
                cursor,
                revision,
                removals,
                _observer: observer,
                opened: false,
                pending: VecDeque::new(),
                view: self.view(),
            })
        })();
        match opened {
            Err(error) => error_stream(error),
            Ok(state) => stream::unfold(Some(state), |state| async move {
                let mut state = state?;
                if !state.opened {
                    state.opened = true;
                    return match state.view.job(&state.id, state.session.as_deref()) {
                        Ok(job) => Some((
                            Ok(json!({ "type": "opened", "job": job, "from": state.cursor })),
                            Some(state),
                        )),
                        Err(error) => Some((Err(error), None)),
                    };
                }
                loop {
                    if let Some(frame) = state.pending.pop_front() {
                        state.cursor = frame["next"].as_u64().unwrap_or(state.cursor);
                        return Some((Ok(frame), Some(state)));
                    }
                    if let Some(removed) = state
                        .removals
                        .lock()
                        .ok()
                        .and_then(|mut queue| queue.pop_front())
                    {
                        return Some((Ok(json!({ "type": "status", "job": removed })), None));
                    }
                    let (chunks, next, lossy, job) =
                        match state
                            .view
                            .read_job(&state.id, state.session.as_deref(), state.cursor)
                        {
                            Ok(read) => read,
                            Err(error) => return Some((Err(error), None)),
                        };
                    if !chunks.is_empty() || lossy {
                        state.pending.extend(output_frames(chunks, next, lossy));
                        continue;
                    }
                    state.cursor = next;
                    if job_terminal(&job)
                        && state.cursor >= job["output"]["total"].as_u64().unwrap_or(0)
                    {
                        return Some((Ok(json!({ "type": "status", "job": job })), None));
                    }
                    if state.revision.changed().await.is_err() {
                        return None;
                    }
                    tokio::time::sleep(OBSERVE_FLUSH).await;
                }
            })
            .boxed(),
        }
    }

    fn observe_removal(&self, id: &str) -> (Observer, RemovalQueue) {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let mut state = lock(&self.state);
        let token = state.next_observer;
        state.next_observer = state.next_observer.wrapping_add(1);
        state
            .observers
            .entry(id.to_owned())
            .or_default()
            .push((token, queue.clone()));
        (
            Observer {
                id: id.to_owned(),
                token,
                state: self.state.clone(),
            },
            queue,
        )
    }

    fn kill_job(&self, payload: &Value) -> Result<Value, RpcError> {
        let req = request(payload);
        let session = session_id(payload)?;
        let id = string_field(req, "jobId")?;
        if id.is_empty() || id.len() > 256 {
            return Err(invalid("jobId is invalid"));
        }
        let (outcome, cancel) = {
            let mut state = lock(&self.state);
            let job = state
                .jobs
                .iter_mut()
                .find(|job| job.id == id && job.visible(Some(&session)))
                .ok_or_else(|| {
                    rpc_error(
                        "job/not-found",
                        "Job is not visible to this Session",
                        json!({ "sessionId": session, "jobId": id }),
                    )
                })?;
            if !job.is_live() {
                ("already-finished", None)
            } else {
                job.status = "stopping";
                job.kill_reason = Some(KILL_REASON.to_owned());
                ("requested", job.cancel.clone())
            }
        };
        if let Some(cancel) = cancel {
            cancel(KILL_REASON.to_owned());
            self.changed();
        }
        Ok(json!({ "outcome": outcome }))
    }

    /// Close terminals and request cancellation of live jobs before releasing Host state.
    pub async fn dispose(&self) {
        let (terminals, jobs) = {
            let mut state = lock(&self.state);
            if state.disposed {
                return;
            }
            state.disposed = true;
            for owner in state.sessions.values_mut() {
                owner.closed.extend(owner.terminals.keys().cloned());
                for pending in owner.pending.values() {
                    pending.cancelled.store(true, Ordering::Release);
                }
            }
            let terminals: Vec<_> = state
                .sessions
                .values()
                .flat_map(|owner| owner.terminals.values().cloned())
                .collect();
            let jobs: Vec<_> = state
                .jobs
                .iter()
                .filter(|job| job.is_live())
                .filter_map(|job| job.cancel.clone())
                .collect();
            (terminals, jobs)
        };
        for cancel in jobs {
            cancel(KILL_REASON.to_owned());
        }
        let _ = join_all(terminals.into_iter().map(|terminal| async move {
            let (reply, result) = oneshot::channel();
            if terminal.command(Command::Close { reply }).await.is_ok() {
                let _ = result.await;
            }
        }))
        .await;
        let mut state = lock(&self.state);
        state.sessions.clear();
        state.jobs.clear();
        state.observers.clear();
        self.changed();
    }
}

#[derive(Clone)]
struct ControllerView {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for ControllerView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControllerView")
            .finish_non_exhaustive()
    }
}

impl ControllerView {
    fn terminal_rows(&self, session: &str) -> Vec<Value> {
        let state = lock(&self.state);
        let mut terminals: Vec<_> = state
            .sessions
            .get(session)
            .into_iter()
            .flat_map(|owner| owner.terminals.values())
            .map(TerminalHandle::info_value)
            .collect();
        terminals.sort_by_key(|info| info["id"].as_str().unwrap_or_default().to_owned());
        terminals
    }

    fn job_rows(&self, session: &str) -> Vec<Value> {
        let state = lock(&self.state);
        let mut jobs: Vec<_> = state
            .jobs
            .iter()
            .filter(|job| job.visible(Some(session)))
            .map(Job::view)
            .collect();
        jobs.sort_by_key(|job| job["id"].as_str().unwrap_or_default().to_owned());
        jobs
    }

    fn job(&self, id: &str, session: Option<&str>) -> Result<Value, RpcError> {
        let state = lock(&self.state);
        state
            .jobs
            .iter()
            .find(|job| job.id == id && job.visible(session))
            .map(Job::view)
            .ok_or_else(|| {
                rpc_error(
                    "job/not-found",
                    "Job is not visible to this Session",
                    json!({ "sessionId": session, "jobId": id }),
                )
            })
    }

    fn read_job(
        &self,
        id: &str,
        session: Option<&str>,
        cursor: u64,
    ) -> Result<(Vec<Value>, u64, bool, Value), RpcError> {
        let state = lock(&self.state);
        let job = state
            .jobs
            .iter()
            .find(|job| job.id == id && job.visible(session))
            .ok_or_else(|| {
                rpc_error(
                    "job/not-found",
                    "Job is not visible to this Session",
                    json!({ "sessionId": session, "jobId": id }),
                )
            })?;
        let lossy = cursor < job.earliest;
        let read_cursor = cursor.max(job.earliest);
        let mut chunks = Vec::new();
        let mut next = read_cursor;
        for chunk in &job.chunks {
            let end = chunk.at + chunk.text.len() as u64;
            if end <= read_cursor {
                continue;
            }
            if chunk.at >= job.total {
                break;
            }
            chunks.push(chunk.value());
            next = end;
        }
        Ok((chunks, next, lossy, job.view()))
    }
}

impl Controller {
    fn view(&self) -> ControllerView {
        ControllerView {
            state: self.state.clone(),
        }
    }
}

struct Observer {
    id: String,
    token: u64,
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for Observer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Observer")
            .field("id", &self.id)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        if let Some(observers) = state.observers.get_mut(&self.id) {
            observers.retain(|(token, _)| *token != self.token);
            if observers.is_empty() {
                state.observers.remove(&self.id);
            }
        }
    }
}

pub(crate) fn error_stream(error: RpcError) -> BoxStream<'static, Result<Value, RpcError>> {
    stream::once(future::ready(Err(error))).boxed()
}

fn output_frames(chunks: Vec<Value>, next: u64, lossy: bool) -> Vec<Value> {
    let mut output = Vec::new();
    let mut batch = Vec::new();
    let mut bytes = 0usize;
    let mut flagged = lossy;
    for chunk in chunks {
        bytes += chunk["text"].as_str().map_or(0, str::len);
        batch.push(chunk);
        if bytes >= OBSERVE_FRAME_BYTES {
            let end = batch
                .last()
                .and_then(|chunk| chunk["at"].as_u64())
                .unwrap_or(next)
                + batch
                    .last()
                    .and_then(|chunk| chunk["text"].as_str())
                    .map_or(0, |text| text.len() as u64);
            let mut frame = json!({ "type": "output", "chunks": batch, "next": end });
            if flagged {
                frame["lossy"] = Value::Bool(true);
            }
            output.push(frame);
            batch = Vec::new();
            bytes = 0;
            flagged = false;
        }
    }
    if !batch.is_empty() || flagged {
        let mut frame = json!({ "type": "output", "chunks": batch, "next": next });
        if flagged {
            frame["lossy"] = Value::Bool(true);
        }
        output.push(frame);
    }
    output
}

fn job_terminal(job: &Value) -> bool {
    matches!(
        job["status"].as_str(),
        Some("completed" | "killed" | "failed")
    )
}

#[derive(Clone)]
struct Chunk {
    at: u64,
    text: String,
    channel: Option<&'static str>,
    gap_before: bool,
}

impl Chunk {
    fn value(&self) -> Value {
        let mut value = json!({ "at": self.at, "text": self.text });
        if let Some(channel) = self.channel {
            value["channel"] = Value::String(channel.to_owned());
        }
        if self.gap_before {
            value["gapBefore"] = Value::Bool(true);
        }
        value
    }
}

type CancelJob = Arc<dyn Fn(String) + Send + Sync>;

#[derive(Clone)]
struct Job {
    id: String,
    kind: String,
    label: String,
    owner: Option<String>,
    output_limit: Option<u64>,
    status: &'static str,
    progress: Option<String>,
    detail: Option<String>,
    exit_code: Option<i32>,
    started_at: u64,
    finished_at: Option<u64>,
    chunks: VecDeque<Chunk>,
    earliest: u64,
    total: u64,
    kill_reason: Option<String>,
    cancel: Option<CancelJob>,
}

impl Job {
    fn is_live(&self) -> bool {
        matches!(self.status, "running" | "stopping")
    }

    fn visible(&self, session: Option<&str>) -> bool {
        self.owner.is_none() || self.owner.as_deref() == session
    }

    fn view(&self) -> Value {
        let mut value = json!({
            "id": self.id,
            "kind": self.kind,
            "label": self.label,
            "status": self.status,
            "startedAt": self.started_at,
            "output": { "total": self.total, "earliest": self.earliest },
        });
        if let Some(owner) = &self.owner {
            value["owner"] = Value::String(owner.clone());
        }
        if let Some(limit) = self.output_limit {
            value["outputLimitBytes"] = json!(limit);
        }
        if let Some(progress) = &self.progress {
            value["progress"] = Value::String(progress.clone());
        }
        if let Some(detail) = &self.detail {
            value["detail"] = Value::String(detail.clone());
        }
        if let Some(code) = self.exit_code {
            value["exitCode"] = json!(code);
        }
        if let Some(finished) = self.finished_at {
            value["finishedAt"] = json!(finished);
        }
        value
    }
}

#[derive(Debug)]
struct FollowState {
    id: String,
    session: Option<String>,
    cursor: u64,
    revision: watch::Receiver<u64>,
    removals: RemovalQueue,
    _observer: Observer,
    opened: bool,
    pending: VecDeque<Value>,
    view: ControllerView,
}

#[derive(Clone, Debug)]
struct TerminalInfo {
    id: String,
    title: String,
    shell_path: String,
    shell_args: Vec<String>,
    shell_name: String,
    cwd: String,
    cols: u16,
    rows: u16,
    state: &'static str,
    exit_code: Option<i32>,
    error: Option<String>,
}

impl TerminalInfo {
    fn value(&self) -> Value {
        let mut value = json!({
            "id": self.id,
            "title": self.title,
            "shell": { "path": self.shell_path, "args": self.shell_args, "name": self.shell_name },
            "cwd": self.cwd,
            "cols": self.cols,
            "rows": self.rows,
            "state": self.state,
            "exitCode": self.exit_code,
            "origin": "user",
            "control": "user",
        });
        if let Some(error) = &self.error {
            value["error"] = Value::String(error.clone());
        }
        value
    }
}

#[derive(Clone)]
struct TerminalHandle {
    info: Arc<Mutex<TerminalInfo>>,
    commands: mpsc::Sender<Command>,
    alive: watch::Sender<bool>,
    control: Arc<ProcessControl>,
    lifetime: Arc<()>,
}

impl TerminalHandle {
    fn info_value(&self) -> Value {
        lock(&self.info).value()
    }

    async fn command(&self, command: Command) -> Result<(), RpcError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| unavailable("Terminal is no longer running"))
    }
}

impl Drop for TerminalHandle {
    fn drop(&mut self) {
        if Arc::strong_count(&self.lifetime) == 1 && lock(&self.info).state == "running" {
            self.alive.send_replace(false);
            self.control.terminate();
        }
    }
}

type AttachmentResult = Result<(u64, Value, FollowerReader, watch::Sender<bool>), RpcError>;

enum Command {
    Detach {
        attachment: u64,
    },
    Attach {
        reply: oneshot::Sender<AttachmentResult>,
    },
    Write {
        attachment: u64,
        sequence: u64,
        data: String,
        reply: oneshot::Sender<Result<(), RpcError>>,
    },
    Resize {
        attachment: u64,
        sequence: u64,
        cols: u16,
        rows: u16,
        reply: oneshot::Sender<Result<(), RpcError>>,
    },
    Rename {
        title: String,
        reply: oneshot::Sender<Result<(), RpcError>>,
    },
    Close {
        reply: oneshot::Sender<Result<(), RpcError>>,
    },
}

#[derive(Default)]
struct FollowerQueue {
    frames: VecDeque<QueuedFrame>,
    bytes: usize,
    finished: bool,
    closed: bool,
    error: Option<RpcError>,
}

struct QueuedFrame {
    value: Value,
    bytes: usize,
}

#[derive(Clone)]
struct FollowerReader {
    queue: Arc<Mutex<FollowerQueue>>,
    notify: Arc<tokio::sync::Notify>,
}

impl FollowerReader {
    async fn next(&mut self) -> Option<Result<Value, RpcError>> {
        loop {
            let notified = self.notify.notified();
            {
                let mut queue = lock(&self.queue);
                if let Some(error) = queue.error.take() {
                    queue.closed = true;
                    return Some(Err(error));
                }
                if let Some(frame) = queue.frames.pop_front() {
                    queue.bytes -= frame.bytes;
                    return Some(Ok(frame.value));
                }
                if queue.finished || queue.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}

struct Follower {
    queue: Arc<Mutex<FollowerQueue>>,
    notify: Arc<tokio::sync::Notify>,
}

impl Follower {
    fn new() -> (Self, FollowerReader) {
        let queue = Arc::new(Mutex::new(FollowerQueue::default()));
        let notify = Arc::new(tokio::sync::Notify::new());
        let reader = FollowerReader {
            queue: queue.clone(),
            notify: notify.clone(),
        };
        (Self { queue, notify }, reader)
    }

    fn push(&mut self, value: Value) {
        let mut queue = lock(&self.queue);
        if queue.closed || queue.finished || queue.error.is_some() {
            return;
        }
        let bytes = encoded_len(&value);
        if queue.bytes + bytes > MAX_FOLLOWER_BYTES {
            queue.frames.clear();
            queue.bytes = 0;
            queue.error = Some(rpc_error(
                "gateway/invocation-failed",
                "Terminal output consumer exceeded its buffer; reconnect to recover the current screen",
                json!({}),
            ));
        } else {
            queue.bytes += bytes;
            queue.frames.push_back(QueuedFrame { value, bytes });
        }
        drop(queue);
        self.notify.notify_one();
    }

    fn fail(&mut self, error: RpcError) {
        let mut queue = lock(&self.queue);
        if queue.finished || queue.closed || queue.error.is_some() {
            return;
        }
        queue.frames.clear();
        queue.bytes = 0;
        queue.error = Some(error);
        drop(queue);
        self.notify.notify_one();
    }

    fn finish(&mut self) {
        lock(&self.queue).finished = true;
        self.notify.notify_one();
    }

    fn close(&mut self) {
        let mut queue = lock(&self.queue);
        queue.closed = true;
        queue.frames.clear();
        queue.bytes = 0;
        queue.error = None;
        drop(queue);
        self.notify.notify_one();
    }
}

struct AttachmentStream {
    terminal: TerminalHandle,
    input: mpsc::Receiver<Value>,
    opening: Option<(oneshot::Receiver<AttachmentResult>, watch::Sender<bool>)>,
    inner: Option<BoxStream<'static, Result<Value, RpcError>>>,
    cancel: Option<watch::Sender<bool>>,
}

impl AttachmentStream {
    fn opening(terminal: TerminalHandle, input: mpsc::Receiver<Value>) -> Self {
        Self {
            terminal,
            input,
            opening: None,
            inner: None,
            cancel: None,
        }
    }
}

impl Stream for AttachmentStream {
    type Item = Result<Value, RpcError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(inner) = self.inner.as_mut() {
                return Pin::new(inner).poll_next(cx);
            }
            if self.opening.is_none() {
                let (reply, rx) = oneshot::channel();
                let (cancel, _) = watch::channel(false);
                if self
                    .terminal
                    .commands
                    .try_send(Command::Attach { reply })
                    .is_err()
                {
                    return Poll::Ready(Some(Err(unavailable("Terminal is no longer running"))));
                }
                self.opening = Some((rx, cancel.clone()));
                self.cancel = Some(cancel);
            }
            let (rx, _) = self.opening.as_mut().expect("opening initialized");
            let opened = match Pin::new(rx).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(opened) => opened,
            };
            self.opening = None;
            match opened {
                Err(_) => {
                    return Poll::Ready(Some(Err(unavailable(
                        "Terminal closed during attachment",
                    ))));
                }
                Ok(Err(error)) => return Poll::Ready(Some(Err(error))),
                Ok(Ok((attachment, snapshot, reader, cancel))) => {
                    self.cancel = Some(cancel);
                    let terminal = self.terminal.clone();
                    let input = std::mem::replace(&mut self.input, mpsc::channel(1).1);
                    let inner = stream::once(future::ready(Ok(snapshot)))
                        .chain(stream::unfold(Some(AttachLoop { terminal, attachment, input, reader, expected: 1, input_open: true }), |state| async move {
                            let mut state = state?;
                            loop {
                                tokio::select! {
                                    biased;
                                    frame = state.reader.next(), if !state.reader_done() => {
                                        match frame {
                                            Some(frame) => return Some((frame, Some(state))),
                                            None => return None,
                                        }
                                    }
                                    command = state.input.recv(), if state.input_open => {
                                        match command {
                                            None => { state.input_open = false; continue }
                                            Some(value) => {
                                                let (expected, command) = match parse_input(&value, state.expected) {
                                                    Ok(parsed) => parsed,
                                                    Err(error) => return Some((Err(error), None)),
                                                };
                                                state.expected = expected;
                                                let result = match command {
                                                    ParsedInput::Write { sequence, data } => state.terminal.command(Command::Write { attachment: state.attachment, sequence, data, reply: oneshot::channel().0 }).await,
                                                    ParsedInput::Resize { sequence, cols, rows } => state.terminal.command(Command::Resize { attachment: state.attachment, sequence, cols, rows, reply: oneshot::channel().0 }).await,
                                                };
                                                if let Err(error) = result { return Some((Err(error), None)) }
                                            }
                                        }
                                    }
                                }
                            }
                        }))
                        .boxed();
                    self.inner = Some(inner);
                }
            }
        }
    }
}

impl Drop for AttachmentStream {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.send_replace(true);
        }
    }
}

struct AttachLoop {
    terminal: TerminalHandle,
    attachment: u64,
    input: mpsc::Receiver<Value>,
    reader: FollowerReader,
    expected: u64,
    input_open: bool,
}

impl AttachLoop {
    fn reader_done(&self) -> bool {
        let queue = lock(&self.reader.queue);
        queue.closed || (queue.finished && queue.frames.is_empty())
    }
}

enum ParsedInput {
    Write { sequence: u64, data: String },
    Resize { sequence: u64, cols: u16, rows: u16 },
}

fn parse_input(value: &Value, expected: u64) -> Result<(u64, ParsedInput), RpcError> {
    let seq = sequence(value)?;
    if seq != expected {
        return Err(invalid("Terminal input sequence must be consecutive"));
    }
    match value.get("type").and_then(Value::as_str) {
        Some("write") => {
            let data = string_field(value, "data")?;
            if data.len() > MAX_INPUT_BYTES {
                return Err(invalid("Terminal input exceeds maxInputBytes"));
            }
            if value.get("typed").is_some_and(|typed| !typed.is_boolean()) {
                return Err(invalid("typed must be a boolean"));
            }
            Ok((
                expected + 1,
                ParsedInput::Write {
                    sequence: seq,
                    data: data.to_owned(),
                },
            ))
        }
        Some("resize") => {
            let (cols, rows) = dimensions(value)?;
            Ok((
                expected + 1,
                ParsedInput::Resize {
                    sequence: seq,
                    cols,
                    rows,
                },
            ))
        }
        _ => Err(invalid("Unknown terminal input type")),
    }
}

async fn discover_shells(cwd: &Path) -> Result<Vec<Value>, RpcError> {
    let default = default_shell(cwd).await?;
    let candidates = ["zsh", "bash", "fish", "pwsh", "powershell", "cmd"];
    let mut output = vec![default.clone()];
    let mut kinds = HashSet::new();
    kinds.insert(shell_kind(default["path"].as_str().unwrap_or_default()));
    for candidate in candidates {
        if let Ok(profile) = resolve_shell(cwd, candidate).await {
            let path = profile["path"].as_str().unwrap_or_default();
            if kinds.insert(shell_kind(path)) {
                output.push(profile);
            }
        }
    }
    Ok(output)
}

fn shell_kind(path: &str) -> String {
    let name = Path::new(path)
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_ascii_lowercase();
    name.trim_end_matches(".exe").to_owned()
}

async fn default_shell(cwd: &Path) -> Result<Value, RpcError> {
    #[cfg(windows)]
    {
        resolve_shell(cwd, "cmd").await
    }
    #[cfg(not(windows))]
    {
        resolve_shell(cwd, "/bin/sh").await
    }
}

async fn resolve_shell(cwd: &Path, shell: &str) -> Result<Value, RpcError> {
    let executable = find_executable(cwd, shell)
        .await
        .ok_or_else(|| invalid("Selected shell is not available in this execution environment"))?;
    let name = shell_kind(&executable);
    let args = if cfg!(windows) {
        if name == "pwsh" || name == "powershell" {
            vec!["-NoLogo".to_owned()]
        } else {
            Vec::new()
        }
    } else if name == "sh" {
        vec!["-i".to_owned()]
    } else if name == "pwsh" || name == "powershell" {
        vec!["-NoLogo".to_owned()]
    } else {
        vec!["-i".to_owned()]
    };
    Ok(json!({ "path": executable, "args": args, "name": name }))
}

async fn find_executable(cwd: &Path, command: &str) -> Option<String> {
    let path = Path::new(command);
    let absolute = if path.is_absolute() {
        Some(path.to_path_buf())
    } else if command.contains('/') || (cfg!(windows) && command.contains('\\')) {
        Some(cwd.join(path))
    } else {
        let path_value = safe_env("PATH")?;
        #[cfg(not(windows))]
        let extensions = [""];
        #[cfg(windows)]
        let extensions = safe_env("PATHEXT")
            .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".to_owned())
            .split(';')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for directory in std::env::split_paths(&path_value) {
            for extension in &extensions {
                let candidate = directory.join(format!("{command}{extension}"));
                if executable_file(&candidate).await {
                    return Some(candidate.to_string_lossy().into_owned());
                }
            }
        }
        None
    }?;
    if executable_file(&absolute).await {
        Some(absolute.to_string_lossy().into_owned())
    } else {
        None
    }
}

async fn executable_file(path: &Path) -> bool {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(_) => return false,
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        path.extension().is_some_and(|extension| {
            matches!(
                extension.to_string_lossy().to_ascii_lowercase().as_str(),
                "exe" | "com" | "bat" | "cmd"
            )
        })
    }
}

fn safe_env(name: &str) -> Option<String> {
    std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
}

fn scrubbed_environment(session: &str) -> Vec<(String, String)> {
    let mut environment: Vec<_> = std::env::vars_os()
        .filter_map(|(key, value)| {
            let key = key.to_string_lossy();
            let upper = key.to_ascii_uppercase();
            if ["KEY", "SECRET", "TOKEN", "PASSWORD"]
                .iter()
                .any(|needle| upper.contains(needle))
            {
                None
            } else {
                Some((key.into_owned(), value.to_string_lossy().into_owned()))
            }
        })
        .collect();
    set_environment(&mut environment, "DSH_SESSION_ID", session);
    set_environment(&mut environment, "TERM", "xterm-256color");
    environment
}

fn set_environment(environment: &mut Vec<(String, String)>, key: &str, value: &str) {
    environment.retain(|(existing, _)| !existing.eq_ignore_ascii_case(key));
    environment.push((key.to_owned(), value.to_owned()));
}

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}

struct ProcessControl {
    pid: u32,
    killed: AtomicBool,
    term_sent: AtomicBool,
    finished: AtomicBool,
    #[cfg(windows)]
    job: Arc<JobHandle>,
}

impl ProcessControl {
    fn signal(&self, _force: bool) {
        // A Windows Job Object remains safe to terminate after its root process
        // is reaped, which also cleans up surviving descendants. A POSIX numeric
        // process-group ID does not carry that identity guarantee.
        if self.finished.load(Ordering::Acquire) && !cfg!(windows) {
            return;
        }
        self.killed.store(true, Ordering::Release);
        let _ = self.pid;
        #[cfg(unix)]
        {
            const SIGTERM: i32 = 15;
            const SIGKILL: i32 = 9;
            unsafe {
                kill(-(self.pid as i32), if _force { SIGKILL } else { SIGTERM });
                kill(self.pid as i32, if _force { SIGKILL } else { SIGTERM });
            }
        }
        #[cfg(windows)]
        {
            terminate_job(&self.job);
        }
    }

    fn terminate(&self) {
        if !self.term_sent.swap(true, Ordering::AcqRel) {
            self.signal(false);
        }
    }

    fn force(&self) {
        self.signal(true);
    }
}

struct OwnedProcess {
    pty: Pty,
    control: Arc<ProcessControl>,
    exit: oneshot::Receiver<Result<i32, String>>,
    finished: bool,
}

impl OwnedProcess {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.pty.write(data).await
    }

    async fn resize(&mut self, cols: u16, rows: u16) -> io::Result<()> {
        self.pty.resize(cols, rows).await
    }

    fn finish(&mut self) {
        self.pty.finish();
    }

    async fn terminate_and_wait(&mut self) -> Result<(), String> {
        if !self.finished {
            let needs_term = !self.control.term_sent.swap(true, Ordering::AcqRel);
            if needs_term {
                self.control.signal(false);
            }
            let result = tokio::time::timeout(DISPOSE_GRACE, &mut self.exit).await;
            match result {
                Ok(Ok(Ok(_))) => self.finished = true,
                Ok(Ok(Err(error))) => return Err(error),
                Ok(Err(_)) => return Err("process wait task ended".to_owned()),
                Err(_) => {
                    self.control.force();
                    let result = tokio::time::timeout(DISPOSE_GRACE, &mut self.exit).await;
                    match result {
                        Ok(Ok(Ok(_))) => self.finished = true,
                        _ => return Err("process did not exit after forced termination".to_owned()),
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if self.finished {
            self.control.finished.store(true, Ordering::Release);
        } else {
            self.control.terminate();
            self.control.force();
        }
        self.pty.finish();
    }
}

enum Pty {
    #[cfg(unix)]
    Posix(posix::PosixPty),
    #[cfg(windows)]
    Windows(WindowsPty),
}

impl Pty {
    async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Posix(pty) => pty.read(buffer).await,
            #[cfg(windows)]
            Self::Windows(pty) => pty.read(buffer).await,
        }
    }

    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Posix(pty) => pty.write(data).await,
            #[cfg(windows)]
            Self::Windows(pty) => pty.write(data).await,
        }
    }

    async fn resize(&mut self, cols: u16, rows: u16) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Posix(pty) => pty.resize(cols, rows),
            #[cfg(windows)]
            Self::Windows(pty) => pty.resize(cols, rows),
        }
    }

    fn finish(&mut self) {
        match self {
            #[cfg(unix)]
            Self::Posix(_) => {}
            #[cfg(windows)]
            Self::Windows(pty) => pty.finish(),
        }
    }
}

#[cfg(unix)]
mod posix {
    use super::*;
    use std::os::fd::{FromRawFd, OwnedFd};
    use tokio::io::Interest;

    #[cfg(target_os = "linux")]
    const O_NOCTTY: i32 = 0x100;
    #[cfg(target_os = "macos")]
    const O_NOCTTY: i32 = 0x20000;
    #[cfg(target_os = "linux")]
    const O_NONBLOCK: i32 = 0x800;
    #[cfg(target_os = "macos")]
    const O_NONBLOCK: i32 = 0x4;
    #[cfg(target_os = "linux")]
    const TIOCSCTTY: u64 = 0x540E;
    #[cfg(target_os = "macos")]
    const TIOCSCTTY: u64 = 0x20007461;
    #[cfg(target_os = "macos")]
    const TIOCSWINSZ: u64 = 0x80087467;
    #[cfg(not(target_os = "macos"))]
    const TIOCSWINSZ: u64 = 0x5414;
    const F_SETFL: i32 = 4;
    const F_GETFL: i32 = 3;
    const F_SETFD: i32 = 2;
    const FD_CLOEXEC: i32 = 1;
    #[cfg(target_os = "linux")]
    const O_CLOEXEC: i32 = 0x80000;
    #[cfg(target_os = "macos")]
    const O_CLOEXEC: i32 = 0x1000000;

    #[repr(C)]
    struct WinSize {
        rows: u16,
        cols: u16,
        xpixel: u16,
        ypixel: u16,
    }

    unsafe extern "C" {
        fn posix_openpt(flags: i32) -> i32;
        fn grantpt(fd: i32) -> i32;
        fn unlockpt(fd: i32) -> i32;
        fn ptsname(fd: i32) -> *mut i8;
        fn open(path: *const i8, flags: i32) -> i32;
        fn close(fd: i32) -> i32;
        fn fcntl(fd: i32, command: i32, ...) -> i32;
        fn setsid() -> i32;
        fn dup2(old: i32, new: i32) -> i32;
        fn ioctl(fd: i32, request: u64, ...) -> i32;
    }

    pub(super) struct PosixPty {
        master: tokio::io::unix::AsyncFd<File>,
    }

    impl PosixPty {
        pub(super) async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            match self
                .master
                .async_io(Interest::READABLE, |mut file| file.read(buffer))
                .await
            {
                Err(error) if error.raw_os_error() == Some(5) => Ok(0),
                result => result,
            }
        }

        pub(super) async fn write(&mut self, mut data: &[u8]) -> io::Result<()> {
            while !data.is_empty() {
                let written = self
                    .master
                    .async_io(Interest::WRITABLE, |mut file| file.write(data))
                    .await?;
                if written == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "PTY closed"));
                }
                data = &data[written..];
            }
            Ok(())
        }

        pub(super) fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
            let size = WinSize {
                rows,
                cols,
                xpixel: 0,
                ypixel: 0,
            };
            let result = unsafe { ioctl(self.master.as_raw_fd(), TIOCSWINSZ, &size) };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }

    pub(super) fn spawn(
        program: &Path,
        args: &[String],
        cwd: &Path,
        cols: u16,
        rows: u16,
        environment: Vec<(String, String)>,
    ) -> io::Result<(tokio::process::Child, PosixPty, Arc<ProcessControl>)> {
        unsafe {
            let master = posix_openpt(2 | O_NOCTTY | O_CLOEXEC);
            if master < 0 {
                return Err(io::Error::last_os_error());
            }
            let master = OwnedFd::from_raw_fd(master);
            if grantpt(master.as_raw_fd()) != 0 || unlockpt(master.as_raw_fd()) != 0 {
                return Err(io::Error::last_os_error());
            }
            let slave_name = ptsname(master.as_raw_fd());
            if slave_name.is_null() {
                return Err(io::Error::last_os_error());
            }
            let name = std::ffi::CStr::from_ptr(slave_name).to_bytes();
            let mut path = Vec::with_capacity(name.len() + 1);
            path.extend_from_slice(name);
            path.push(0);
            let slave = open(path.as_ptr().cast(), 2 | O_NOCTTY | O_CLOEXEC);
            if slave < 0 {
                return Err(io::Error::last_os_error());
            }
            let slave = OwnedFd::from_raw_fd(slave);
            if fcntl(master.as_raw_fd(), F_SETFD, FD_CLOEXEC) < 0
                || fcntl(slave.as_raw_fd(), F_SETFD, FD_CLOEXEC) < 0
            {
                return Err(io::Error::last_os_error());
            }
            let size = WinSize {
                rows,
                cols,
                xpixel: 0,
                ypixel: 0,
            };
            if ioctl(slave.as_raw_fd(), TIOCSWINSZ, &size) < 0 {
                return Err(io::Error::last_os_error());
            }
            let flags = master_flags(master.as_raw_fd());
            if flags < 0 || fcntl(master.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) < 0 {
                return Err(io::Error::last_os_error());
            }
            let master_file = File::from(master);
            // The registered File owns the descriptor and keeps it open until dropped.
            let async_master = tokio::io::unix::AsyncFd::register(master_file)?;
            let slave_fd = slave.as_raw_fd();
            let mut command = tokio::process::Command::new(program);
            command.args(args);
            command.current_dir(cwd);
            command.env_clear().envs(environment);
            command.stdin(std::process::Stdio::null());
            command.stdout(std::process::Stdio::null());
            command.stderr(std::process::Stdio::null());
            command.pre_exec(move || {
                if setsid() < 0 {
                    return Err(io::Error::last_os_error());
                }
                if ioctl(slave_fd, TIOCSCTTY, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                if dup2(slave_fd, 0) < 0 || dup2(slave_fd, 1) < 0 || dup2(slave_fd, 2) < 0 {
                    return Err(io::Error::last_os_error());
                }
                if slave_fd > 2 {
                    close(slave_fd);
                }
                Ok(())
            });
            command.kill_on_drop(true);
            let child = command.spawn()?;
            let pid = child.id().unwrap_or(0);
            let control = Arc::new(ProcessControl {
                pid,
                killed: AtomicBool::new(false),
                term_sent: AtomicBool::new(false),
                finished: AtomicBool::new(false),
            });
            Ok((
                child,
                PosixPty {
                    master: async_master,
                },
                control,
            ))
        }
    }

    fn master_flags(fd: i32) -> i32 {
        unsafe { fcntl(fd, F_GETFL) }
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::ffi::c_void;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use std::os::windows::process::ExitStatusExt;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SecurityAttributes {
        length: u32,
        inherit: i32,
    }

    #[repr(C)]
    struct StartupInfoW {
        cb: u32,
        reserved: *mut u16,
        desktop: *mut u16,
        title: *mut u16,
        x: u32,
        y: u32,
        x_size: u32,
        y_size: u32,
        x_chars: u32,
        y_chars: u32,
        fill: u32,
        flags: u32,
        show: u16,
        reserved_size: u16,
        reserved2: *mut u8,
        std_input: RawHandle,
        std_output: RawHandle,
        std_error: RawHandle,
    }

    #[repr(C)]
    struct StartupInfoExW {
        startup: StartupInfoW,
        attributes: *mut c_void,
    }

    #[repr(C)]
    struct IoCounters {
        read_operations: u64,
        write_operations: u64,
        other_operations: u64,
        read_bytes: u64,
        write_bytes: u64,
        other_bytes: u64,
    }

    #[repr(C)]
    struct BasicLimitInformation {
        per_process_time: i64,
        per_job_time: i64,
        flags: u32,
        minimum_working_set: usize,
        maximum_working_set: usize,
        active_process_limit: u32,
        affinity: usize,
        priority: u32,
        scheduling: u32,
    }

    #[repr(C)]
    struct ExtendedLimitInformation {
        basic: BasicLimitInformation,
        io: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory: usize,
        peak_job_memory: usize,
    }

    pub(super) struct JobHandle(OwnedHandle);
    impl AsRawHandle for JobHandle {
        fn as_raw_handle(&self) -> RawHandle {
            self.0.as_raw_handle()
        }
    }

    pub(super) fn terminate_job(job: &JobHandle) {
        unsafe {
            TerminateJobObject(job.0.as_raw_handle(), 1);
        }
    }

    unsafe extern "system" {
        fn CreatePipe(
            read: *mut RawHandle,
            write: *mut RawHandle,
            attributes: *const SecurityAttributes,
            size: u32,
        ) -> i32;
        fn SetHandleInformation(handle: RawHandle, mask: u32, flags: u32) -> i32;
        fn CreatePseudoConsole(
            size: Coord,
            input: RawHandle,
            output: RawHandle,
            flags: u32,
            console: *mut *mut c_void,
        ) -> i32;
        fn ClosePseudoConsole(console: *mut c_void) -> i32;
        fn ResizePseudoConsole(console: *mut c_void, size: Coord) -> i32;
        fn CreateJobObjectW(attributes: *const SecurityAttributes, name: *const u16) -> RawHandle;
        fn SetInformationJobObject(
            job: RawHandle,
            class: i32,
            info: *const ExtendedLimitInformation,
            length: u32,
        ) -> i32;
        fn AssignProcessToJobObject(job: RawHandle, process: RawHandle) -> i32;
        fn TerminateJobObject(job: RawHandle, code: u32) -> i32;
        fn InitializeProcThreadAttributeList(
            list: *mut c_void,
            count: u32,
            flags: u32,
            size: *mut usize,
        ) -> i32;
        fn UpdateProcThreadAttribute(
            list: *mut c_void,
            flags: u32,
            attribute: usize,
            value: *const c_void,
            size: usize,
            previous: *mut c_void,
            return_size: *mut usize,
        ) -> i32;
        fn DeleteProcThreadAttributeList(list: *mut c_void);
        fn CreateProcessW(
            application: *const u16,
            command: *mut u16,
            process_attributes: *const SecurityAttributes,
            thread_attributes: *const SecurityAttributes,
            inherit: i32,
            flags: u32,
            environment: *const c_void,
            cwd: *const u16,
            startup: *const StartupInfoW,
            process: *mut RawHandle,
            thread: *mut RawHandle,
        ) -> i32;
        fn ResumeThread(thread: RawHandle) -> u32;
        fn WaitForSingleObject(handle: RawHandle, milliseconds: u32) -> u32;
        fn GetExitCodeProcess(process: RawHandle, code: *mut u32) -> i32;
        fn GetProcessId(process: RawHandle) -> u32;
        fn CloseHandle(handle: RawHandle) -> i32;
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Coord {
        x: i16,
        y: i16,
    }

    pub(super) struct Child {
        handle: OwnedHandle,
        pid: u32,
    }

    impl Child {
        pub(super) fn id(&self) -> u32 {
            self.pid
        }
    }

    pub(super) async fn wait_child(child: Child) -> io::Result<std::process::ExitStatus> {
        let handle = child.handle.try_clone()?;
        tokio::task::spawn_blocking(move || unsafe {
            if WaitForSingleObject(handle.as_raw_handle(), u32::MAX) != 0 {
                return Err(io::Error::last_os_error());
            }
            let mut code = 0u32;
            if GetExitCodeProcess(handle.as_raw_handle(), &mut code) == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(std::process::ExitStatus::from_raw(code))
        })
        .await
        .map_err(|error| io::Error::other(error.to_string()))?
    }

    struct ConsoleGuard(Option<*mut c_void>);

    impl Drop for ConsoleGuard {
        fn drop(&mut self) {
            if let Some(console) = self.0.take() {
                unsafe { ClosePseudoConsole(console) };
            }
        }
    }

    struct AttributeGuard {
        storage: Vec<u64>,
        list: *mut c_void,
    }

    impl Drop for AttributeGuard {
        fn drop(&mut self) {
            unsafe { DeleteProcThreadAttributeList(self.list) };
        }
    }

    pub(super) struct WindowsPty {
        reader: Arc<Mutex<File>>,
        writer: Arc<Mutex<File>>,
        console: *mut c_void,
        _job: Arc<JobHandle>,
        finished: bool,
    }

    // The pseudo-console is only closed by the owning terminal task. Concurrent
    // PTY I/O uses the cloned file handles, never this raw pointer.
    unsafe impl Send for WindowsPty {}

    impl Drop for WindowsPty {
        fn drop(&mut self) {
            self.finish();
        }
    }

    impl WindowsPty {
        pub(super) fn finish(&mut self) {
            if !self.finished {
                self.finished = true;
                if !self.console.is_null() {
                    unsafe { ClosePseudoConsole(self.console) };
                    self.console = std::ptr::null_mut();
                }
            }
        }

        pub(super) async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let reader = self.reader.clone();
            let length = buffer.len();
            tokio::task::spawn_blocking(move || {
                let mut reader = reader.lock().unwrap_or_else(|error| error.into_inner());
                let mut bytes = vec![0u8; length];
                let count = reader.read(&mut bytes)?;
                bytes.truncate(count);
                Ok::<(usize, Vec<u8>), io::Error>((count, bytes))
            })
            .await
            .map_err(|error| io::Error::other(error.to_string()))?
            .map(|(count, bytes)| {
                buffer[..count].copy_from_slice(&bytes);
                count
            })
        }

        pub(super) async fn write(&mut self, data: &[u8]) -> io::Result<()> {
            let writer = self.writer.clone();
            let mut bytes = data.to_vec();
            tokio::task::spawn_blocking(move || {
                let mut writer = writer.lock().unwrap_or_else(|error| error.into_inner());
                writer.write_all(&bytes)?;
                writer.flush()?;
                bytes.clear();
                Ok::<(), io::Error>(())
            })
            .await
            .map_err(|error| io::Error::other(error.to_string()))?
        }

        pub(super) fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
            if self.finished {
                return Err(io::Error::new(io::ErrorKind::NotConnected, "ConPTY closed"));
            }
            let result = unsafe {
                ResizePseudoConsole(
                    self.console,
                    Coord {
                        x: cols as i16,
                        y: rows as i16,
                    },
                )
            };
            if result >= 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }

    pub(super) fn spawn(
        program: &Path,
        args: &[String],
        cwd: &Path,
        cols: u16,
        rows: u16,
        environment: Vec<(String, String)>,
    ) -> io::Result<(Child, WindowsPty, Arc<ProcessControl>)> {
        unsafe {
            let attributes = SecurityAttributes {
                length: std::mem::size_of::<SecurityAttributes>() as u32,
                inherit: 1,
            };
            let mut input_read = std::ptr::null_mut();
            let mut input_write = std::ptr::null_mut();
            let mut output_read = std::ptr::null_mut();
            let mut output_write = std::ptr::null_mut();
            if CreatePipe(&mut input_read, &mut input_write, &attributes, 0) == 0
                || CreatePipe(&mut output_read, &mut output_write, &attributes, 0) == 0
            {
                let error = stage_error("CreatePipe");
                for handle in [input_read, input_write, output_read, output_write] {
                    if !handle.is_null() {
                        CloseHandle(handle);
                    }
                }
                return Err(error);
            }
            if SetHandleInformation(input_write, 1, 0) == 0
                || SetHandleInformation(output_read, 1, 0) == 0
            {
                let error = stage_error("SetHandleInformation");
                for handle in [input_read, input_write, output_read, output_write] {
                    if !handle.is_null() {
                        CloseHandle(handle);
                    }
                }
                return Err(error);
            }
            let input_read = OwnedHandle::from_raw_handle(input_read);
            let input_write = OwnedHandle::from_raw_handle(input_write);
            let output_read = OwnedHandle::from_raw_handle(output_read);
            let output_write = OwnedHandle::from_raw_handle(output_write);
            let mut console = std::ptr::null_mut();
            let result = CreatePseudoConsole(
                Coord {
                    x: cols as i16,
                    y: rows as i16,
                },
                input_read.as_raw_handle(),
                output_write.as_raw_handle(),
                0,
                &mut console,
            );
            if result < 0 {
                let error = io::Error::from_raw_os_error(result);
                return Err(io::Error::new(
                    error.kind(),
                    format!("CreatePseudoConsole failed: {error}"),
                ));
            }
            let mut console = ConsoleGuard(Some(console));
            let job_raw = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job_raw.is_null() {
                return Err(stage_error("CreateJobObjectW"));
            }
            let job = Arc::new(JobHandle(OwnedHandle::from_raw_handle(job_raw)));
            let mut limits: ExtendedLimitInformation = std::mem::zeroed();
            limits.basic.flags = 0x2000;
            if SetInformationJobObject(
                job_raw,
                9,
                &limits,
                std::mem::size_of::<ExtendedLimitInformation>() as u32,
            ) == 0
            {
                return Err(stage_error("SetInformationJobObject"));
            }
            let mut attribute_size = 0usize;
            InitializeProcThreadAttributeList(std::ptr::null_mut(), 2, 0, &mut attribute_size);
            if attribute_size == 0 {
                return Err(stage_error("InitializeProcThreadAttributeList(size)"));
            }
            let storage = vec![0u64; attribute_size.div_ceil(8) + 1];
            let mut attribute_guard = AttributeGuard {
                storage,
                list: std::ptr::null_mut(),
            };
            attribute_guard.list = attribute_guard.storage.as_mut_ptr().cast();
            if InitializeProcThreadAttributeList(attribute_guard.list, 2, 0, &mut attribute_size)
                == 0
            {
                return Err(stage_error("InitializeProcThreadAttributeList(allocate)"));
            }
            let console_value = console.0.expect("pseudo-console created");
            let inherited = [input_read.as_raw_handle(), output_write.as_raw_handle()];
            if UpdateProcThreadAttribute(
                attribute_guard.list,
                0,
                0x20016,
                (&console_value as *const *mut c_void).cast(),
                std::mem::size_of::<*mut c_void>(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ) == 0
                || UpdateProcThreadAttribute(
                    attribute_guard.list,
                    0,
                    0x00020002,
                    inherited.as_ptr().cast(),
                    std::mem::size_of::<[RawHandle; 2]>(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                ) == 0
            {
                return Err(stage_error("UpdateProcThreadAttribute"));
            }
            let mut startup: StartupInfoExW = std::mem::zeroed();
            startup.startup.cb = std::mem::size_of::<StartupInfoExW>() as u32;
            startup.attributes = attribute_guard.list;
            let application = wide(program.to_string_lossy().as_ref());
            let mut command_line = wide(command_line(program, args).as_str());
            let mut environment_block = environment_block(&environment);
            let directory = wide(cwd.to_string_lossy().as_ref());
            let mut process = std::ptr::null_mut();
            let mut thread = std::ptr::null_mut();
            let created = CreateProcessW(
                application.as_ptr(),
                command_line.as_mut_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                1,
                0x00080004,
                environment_block.as_mut_ptr().cast(),
                directory.as_ptr(),
                (&startup.startup as *const StartupInfoW).cast(),
                &mut process,
                &mut thread,
            );
            if created == 0 {
                let error = stage_error("CreateProcessW");
                for handle in [process, thread] {
                    if !handle.is_null() {
                        CloseHandle(handle);
                    }
                }
                return Err(error);
            }
            let process = OwnedHandle::from_raw_handle(process);
            let thread = OwnedHandle::from_raw_handle(thread);
            if AssignProcessToJobObject(job_raw, process.as_raw_handle()) == 0 {
                let error = stage_error("AssignProcessToJobObject");
                TerminateProcess(process.as_raw_handle(), 1);
                WaitForSingleObject(process.as_raw_handle(), u32::MAX);
                return Err(error);
            }
            if ResumeThread(thread.as_raw_handle()) == u32::MAX {
                let error = stage_error("ResumeThread");
                TerminateJobObject(job_raw, 1);
                WaitForSingleObject(process.as_raw_handle(), u32::MAX);
                return Err(error);
            }
            let pid = GetProcessId(process.as_raw_handle());
            let child = Child {
                handle: process,
                pid,
            };
            let control = Arc::new(ProcessControl {
                pid,
                killed: AtomicBool::new(false),
                term_sent: AtomicBool::new(false),
                finished: AtomicBool::new(false),
                job: job.clone(),
            });
            let pty = WindowsPty {
                reader: Arc::new(Mutex::new(File::from(output_read))),
                writer: Arc::new(Mutex::new(File::from(input_write))),
                console: console.0.take().expect("pseudo-console created"),
                _job: job,
                finished: false,
            };
            Ok((child, pty, control))
        }
    }

    unsafe extern "system" {
        fn TerminateProcess(process: RawHandle, code: u32) -> i32;
    }

    fn stage_error(stage: &str) -> io::Error {
        let error = io::Error::last_os_error();
        io::Error::new(error.kind(), format!("{stage}: {error}"))
    }

    fn wide(value: &str) -> Vec<u16> {
        value.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn command_line(program: &Path, args: &[String]) -> String {
        let mut values = vec![program.to_string_lossy().into_owned()];
        values.extend(args.iter().cloned());
        values
            .iter()
            .map(|value| {
                if !value.is_empty()
                    && !value
                        .chars()
                        .any(|character| character.is_whitespace() || character == '"')
                {
                    value.clone()
                } else {
                    let mut output = String::from("\"");
                    let mut slashes = 0usize;
                    for character in value.chars() {
                        if character == '\\' {
                            slashes += 1;
                            continue;
                        }
                        if character == '"' {
                            output.push_str(&"\\".repeat(slashes * 2 + 1));
                            output.push('"');
                            slashes = 0;
                        } else {
                            output.push_str(&"\\".repeat(slashes));
                            output.push(character);
                            slashes = 0;
                        }
                    }
                    output.push_str(&"\\".repeat(slashes * 2));
                    output.push('"');
                    output
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn environment_block(environment: &[(String, String)]) -> Vec<u16> {
        let mut entries: Vec<_> = environment
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        entries.sort_by_key(|entry| entry.to_ascii_uppercase());
        let mut output = String::new();
        for entry in entries {
            output.push_str(&entry);
            output.push('\0');
        }
        output.push('\0');
        wide(&output)
    }
}

#[cfg(unix)]
use posix::spawn as spawn_pty;
#[cfg(windows)]
use windows::{spawn as spawn_pty, terminate_job, JobHandle, WindowsPty};

fn spawn_process(
    program: &Path,
    args: &[String],
    cwd: &Path,
    cols: u16,
    rows: u16,
    session: &str,
) -> Result<OwnedProcess, String> {
    let environment = scrubbed_environment(session);
    let (child, pty, control) = spawn_pty(program, args, cwd, cols, rows, environment)
        .map_err(|error| error.to_string())?;
    #[cfg(unix)]
    let mut child = child;
    let pid = child.id();
    let control_wait = control.clone();
    let (exit_tx, exit_rx) = oneshot::channel();
    tokio::spawn(async move {
        #[cfg(unix)]
        let status = child.wait().await;
        #[cfg(windows)]
        let status = windows::wait_child(child).await;
        control_wait.finished.store(true, Ordering::Release);
        let result = match status {
            Ok(status) => {
                #[cfg(unix)]
                let signal = {
                    use std::os::unix::process::ExitStatusExt;
                    status.signal()
                };
                #[cfg(windows)]
                let signal: Option<i32> = None;
                if control_wait.killed.load(Ordering::Acquire) || signal.is_some() {
                    Ok(-1)
                } else {
                    Ok(status.code().unwrap_or(-1))
                }
            }
            Err(error) => Err(error.to_string()),
        };
        let _ = pid;
        let _ = exit_tx.send(result);
    });
    Ok(OwnedProcess {
        pty: Pty::new(pty),
        control,
        exit: exit_rx,
        finished: false,
    })
}

impl Pty {
    fn new(#[cfg(unix)] pty: posix::PosixPty, #[cfg(windows)] pty: windows::WindowsPty) -> Self {
        #[cfg(unix)]
        {
            Self::Posix(pty)
        }
        #[cfg(windows)]
        {
            Self::Windows(pty)
        }
    }
}

#[derive(Clone)]
struct AllocationController {
    context: Arc<ApiContext>,
    state: Arc<Mutex<State>>,
    revision: watch::Sender<u64>,
}

impl AllocationController {
    async fn allocate(
        &self,
        session: String,
        id: String,
        cols: u16,
        rows: u16,
        selected: Option<String>,
        pending: Arc<PendingCreate>,
    ) -> Result<Value, RpcError> {
        let _cleanup = PendingCleanup {
            state: self.state.clone(),
            session: session.clone(),
            id: id.clone(),
            pending: pending.clone(),
        };
        let result = self
            .allocate_inner(&session, &id, cols, rows, selected.as_deref(), &pending)
            .await;
        result
    }

    async fn allocate_inner(
        &self,
        session: &str,
        id: &str,
        cols: u16,
        rows: u16,
        selected: Option<&str>,
        pending: &PendingCreate,
    ) -> Result<Value, RpcError> {
        let cwd = self.context.home.clone();
        let cwd =
            std::fs::canonicalize(&cwd).map_err(|_| invalid("Harness home is unavailable"))?;
        // Resolve the persisted Session directory when one exists.
        let persisted = dsh_session_persistence::Store::mixed(self.context.home.join("sessions"))
            .read_all()
            .into_iter()
            .find(|stored| stored.header.id == session)
            .and_then(|stored| stored.header.cwd)
            .map(PathBuf::from);
        let cwd = if let Some(persisted) = persisted {
            persisted
                .canonicalize()
                .map_err(|_| invalid("Session working directory does not exist"))?
        } else {
            cwd
        };
        let shells = discover_shells(&cwd).await?;
        let shell = if let Some(selected) = selected {
            shells
                .iter()
                .find(|candidate| candidate["path"].as_str() == Some(selected))
                .cloned()
                .ok_or_else(|| {
                    invalid("Selected shell is not available in this execution environment")
                })?
        } else {
            shells.first().cloned().ok_or_else(|| {
                invalid("Selected shell is not available in this execution environment")
            })?
        };
        if pending.cancelled.load(Ordering::Acquire) {
            return Err(invalid("Terminal allocation cancelled"));
        }
        let program = PathBuf::from(shell["path"].as_str().unwrap_or_default());
        let args: Vec<String> = shell["args"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let mut process = spawn_process(&program, &args, &cwd, cols, rows, session)
            .map_err(|error| invalid(format!("Terminal could not be started: {error}")))?;
        if pending.cancelled.load(Ordering::Acquire) {
            let error = match process.terminate_and_wait().await {
                Ok(()) => invalid("Terminal allocation cancelled"),
                Err(error) => invalid(format!("Terminal allocation cancelled: {error}")),
            };
            if let Some(owner) = lock(&self.state).sessions.get_mut(session) {
                owner.closed.insert(id.to_owned());
            }
            return Err(error);
        }
        let info = TerminalInfo {
            id: id.to_owned(),
            title: shell["name"].as_str().unwrap_or("shell").to_owned(),
            shell_path: program.to_string_lossy().into_owned(),
            shell_args: args,
            shell_name: shell["name"].as_str().unwrap_or("shell").to_owned(),
            cwd: cwd.to_string_lossy().into_owned(),
            cols,
            rows,
            state: "running",
            exit_code: None,
            error: None,
        };
        let (commands, command_rx) = mpsc::channel(64);
        let (alive, alive_rx) = watch::channel(true);
        let shared_info = Arc::new(Mutex::new(info.clone()));
        let terminal = TerminalHandle {
            info: shared_info.clone(),
            commands: commands.clone(),
            alive: alive.clone(),
            control: process.control.clone(),
            lifetime: Arc::new(()),
        };
        let revision = self.revision.clone();
        let fallback_info = info.clone();
        let session = session.to_owned();
        let id = id.to_owned();
        let cancelled = {
            let mut state = lock(&self.state);
            if state.disposed {
                true
            } else {
                let owner = state.sessions.entry(session.clone()).or_default();
                let cancelled =
                    owner.closed.contains(&id) || pending.cancelled.load(Ordering::Acquire);
                if !cancelled {
                    owner.terminals.insert(id.clone(), terminal);
                    owner.pending.remove(&id);
                }
                cancelled
            }
        };
        if cancelled {
            let error = match process.terminate_and_wait().await {
                Ok(()) => invalid("Terminal allocation cancelled"),
                Err(error) => invalid(format!("Terminal allocation cancelled: {error}")),
            };
            return Err(error);
        }
        tokio::spawn(async move {
            let _ = run_terminal(
                command_rx,
                commands,
                alive.clone(),
                alive_rx,
                process,
                info,
                shared_info,
                revision,
            )
            .await;
        });
        self.revision
            .send_modify(|value| *value = value.wrapping_add(1));
        Ok(lock(&self.state)
            .sessions
            .get(&session)
            .and_then(|owner| owner.terminals.get(&id))
            .map(TerminalHandle::info_value)
            .unwrap_or_else(|| fallback_info.value()))
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_terminal(
    mut commands: mpsc::Receiver<Command>,
    command_sender: mpsc::Sender<Command>,
    alive_tx: watch::Sender<bool>,
    mut alive: watch::Receiver<bool>,
    mut process: OwnedProcess,
    mut info: TerminalInfo,
    shared_info: Arc<Mutex<TerminalInfo>>,
    revision: watch::Sender<u64>,
) -> Result<(), String> {
    let mut screen = Screen::new(info.cols as usize, info.rows as usize);
    let mut output_sequence = 0u64;
    let mut attachments: HashMap<u64, Follower> = HashMap::new();
    let mut writable: Option<u64> = None;
    let mut process_exited = false;
    let mut output_open = true;
    let mut close_requested = false;
    let mut force_sent = false;
    let mut next_attachment = 1u64;
    let mut close_reply = None;
    let mut close_deadline: Option<Pin<Box<tokio::time::Sleep>>> = None;
    let mut cleanup_error = None;
    let mut exit_code = None;
    let mut exit_error = None;
    let mut buffer = [0u8; 8192];
    let mut decoder = Utf8Decoder::default();
    let result = loop {
        if !*alive.borrow() && !close_requested {
            close_requested = true;
            process.control.terminate();
            close_deadline = Some(Box::pin(tokio::time::sleep(DISPOSE_GRACE)));
        }
        if process_exited && !output_open {
            break Ok(());
        }
        tokio::select! {
            biased;
            command = commands.recv() => {
                let Some(command) = command else {
                    close_requested = true;
                    process.control.terminate();
                    close_deadline = Some(Box::pin(tokio::time::sleep(DISPOSE_GRACE)));
                    continue;
                };
                match command {
                    Command::Attach { reply } => {
                        if info.state != "running" {
                            let _ = reply.send(Err(unavailable("Terminal is no longer running")));
                            continue;
                        }
                        let attachment = next_attachment;
                        next_attachment = next_attachment.wrapping_add(1);
                        let (follower, reader) = Follower::new();
                        let old = writable.replace(attachment);
                        attachments.insert(attachment, follower);
                        if old.is_some() {
                            broadcast_state(&mut attachments, &info, Some(attachment));
                        }
                        let (cancel, mut cancel_rx) = watch::channel(false);
                        let cleanup_sender = command_sender.clone();
                        let mut cleanup_alive = alive.clone();
                        tokio::spawn(async move {
                            loop {
                                if *cancel_rx.borrow_and_update() {
                                    let _ = cleanup_sender.send(Command::Detach { attachment }).await;
                                    return;
                                }
                                tokio::select! {
                                    _ = cancel_rx.changed() => {}
                                    _ = cleanup_alive.changed() => return,
                                }
                            }
                        });
                        let _ = reply.send(Ok((attachment, json!({
                            "type": "snapshot", "sequence": output_sequence,
                            "screen": screen.serialize(), "info": info.value(), "writable": true,
                        }), reader, cancel)));
                    }
                    Command::Detach { attachment } => {
                        if let Some(mut follower) = attachments.remove(&attachment) {
                            follower.close();
                        }
                        if writable == Some(attachment) {
                            writable = None;
                            broadcast_state(&mut attachments, &info, writable);
                        }
                    }
                    Command::Write { attachment, sequence, data, reply } => {
                        let refusal = if writable != Some(attachment) {
                            Some("read-only")
                        } else if info.state != "running" {
                            Some("not-running")
                        } else {
                            None
                        };
                        if let Some(refusal) = refusal {
                            acknowledge(&mut attachments, attachment, sequence, Some(refusal));
                            let _ = reply.send(Ok(()));
                            continue;
                        }
                        match process.write(data.as_bytes()).await {
                            Ok(()) => acknowledge(&mut attachments, attachment, sequence, None),
                            Err(error) => {
                                if let Some(follower) = attachments.get_mut(&attachment) {
                                    follower.fail(internal(format!("Terminal write failed: {error}")));
                                }
                                let _ = reply.send(Err(internal(format!("Terminal write failed: {error}"))));
                            }
                        }
                    }
                    Command::Resize { attachment, sequence, cols, rows, reply } => {
                        let refusal = if writable != Some(attachment) {
                            Some("read-only")
                        } else if info.state != "running" {
                            Some("not-running")
                        } else {
                            None
                        };
                        if let Some(refusal) = refusal {
                            acknowledge(&mut attachments, attachment, sequence, Some(refusal));
                            let _ = reply.send(Ok(()));
                            continue;
                        }
                        match process.resize(cols, rows).await {
                            Ok(()) => {
                                info.cols = cols;
                                info.rows = rows;
                                *lock(&shared_info) = info.clone();
                                screen.resize(cols as usize, rows as usize);
                                broadcast_state(&mut attachments, &info, writable);
                                acknowledge(&mut attachments, attachment, sequence, None);
                                let _ = reply.send(Ok(()));
                                changed(&revision);
                            }
                            Err(error) => {
                                if let Some(follower) = attachments.get_mut(&attachment) { follower.fail(internal(format!("Terminal resize failed: {error}"))) }
                                let _ = reply.send(Err(internal(format!("Terminal resize failed: {error}"))));
                            }
                        }
                    }
                    Command::Rename { title, reply } => {
                        info.title = title;
                        *lock(&shared_info) = info.clone();
                        broadcast_state(&mut attachments, &info, writable);
                        changed(&revision);
                        let _ = reply.send(Ok(()));
                    }
                    Command::Close { reply } => {
                        close_reply = Some(reply);
                        if !close_requested {
                            close_requested = true;
                            process.control.terminate();
                            close_deadline = Some(Box::pin(tokio::time::sleep(DISPOSE_GRACE)));
                        }
                    }
                }
            }
            read = process.pty.read(&mut buffer), if output_open => {
                match read {
                    Ok(0) => output_open = false,
                    Ok(count) => {
                        let text = decoder.push(&buffer[..count]);
                        if !text.is_empty() {
                            screen.feed(&text);
                            output_sequence = output_sequence.saturating_add(1);
                            broadcast_output(&mut attachments, output_sequence, &text);
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => break Err(format!("Terminal output failed: {error}")),
                }
            }
            exit = &mut process.exit, if !process_exited => {
                process_exited = true;
                match exit {
                    Ok(Ok(code)) => exit_code = if code < 0 { None } else { Some(code) },
                    Ok(Err(error)) => exit_error = Some(error),
                    Err(_) => exit_error = Some("process wait task ended".to_owned()),
                }
                process.finished = true;
                process.control.finished.store(true, Ordering::Release);
            }
            _ = async { close_deadline.as_mut().unwrap().await }, if close_deadline.is_some() => {
                close_deadline = None;
                if !process_exited {
                    if !force_sent {
                        force_sent = true;
                        process.control.force();
                        close_deadline = Some(Box::pin(tokio::time::sleep(DISPOSE_GRACE)));
                    } else {
                        cleanup_error = Some("process did not exit after forced termination".to_owned());
                        break Err("Terminal cleanup failed".to_owned());
                    }
                }
            }
            _ = alive.changed(), if !close_requested => {}
        }
    };
    if let Err(error) = &result {
        if let Err(cleanup) = process.terminate_and_wait().await {
            cleanup_error = Some(cleanup);
        }
        info.state = "failed";
        info.error = Some(error.clone());
        exit_code = None;
    }
    if let Some(error) = cleanup_error {
        info.state = "failed";
        info.error = Some(error);
        exit_code = None;
    }
    *lock(&shared_info) = info.clone();
    let final_text = decoder.finish();
    if !final_text.is_empty() {
        screen.feed(&final_text);
        output_sequence = output_sequence.saturating_add(1);
        broadcast_output(&mut attachments, output_sequence, &final_text);
    }
    if info.state == "running" {
        finish_terminal(
            &mut info,
            &mut attachments,
            output_sequence,
            &mut screen,
            &decoder,
            exit_code,
            exit_error.clone(),
        );
    } else {
        broadcast_state(&mut attachments, &info, writable);
    }
    *lock(&shared_info) = info.clone();
    for follower in attachments.values_mut() {
        follower.finish()
    }
    process.finish();
    alive_tx.send_replace(false);
    changed(&revision);
    if let Some(reply) = close_reply {
        let result = if info.state == "failed" {
            Err(internal(
                info.error
                    .clone()
                    .unwrap_or_else(|| "Terminal cleanup failed".to_owned()),
            ))
        } else {
            Ok(())
        };
        let _ = reply.send(result);
    }
    Ok(())
}

fn changed(revision: &watch::Sender<u64>) {
    revision.send_modify(|value| *value = value.wrapping_add(1));
}

fn finish_terminal(
    info: &mut TerminalInfo,
    attachments: &mut HashMap<u64, Follower>,
    _sequence: u64,
    _screen: &mut Screen,
    _decoder: &Utf8Decoder,
    exit_code: Option<i32>,
    error: Option<String>,
) {
    if let Some(error) = error {
        info.state = "failed";
        info.error = Some(error);
        info.exit_code = None;
    } else {
        info.state = "exited";
        info.exit_code = exit_code;
        info.error = None;
    }
    broadcast_state(attachments, info, None);
}

fn broadcast_output(attachments: &mut HashMap<u64, Follower>, sequence: u64, data: &str) {
    for follower in attachments.values_mut() {
        follower.push(json!({ "type": "output", "sequence": sequence, "data": data }));
    }
}

fn broadcast_state(
    attachments: &mut HashMap<u64, Follower>,
    info: &TerminalInfo,
    writable: Option<u64>,
) {
    for (id, follower) in attachments.iter_mut() {
        follower.push(
            json!({ "type": "state", "info": info.value(), "writable": writable == Some(*id) }),
        );
    }
}

fn acknowledge(
    attachments: &mut HashMap<u64, Follower>,
    attachment: u64,
    sequence: u64,
    refusal: Option<&str>,
) {
    if let Some(follower) = attachments.get_mut(&attachment) {
        let mut frame = json!({ "type": "ack", "sequence": sequence });
        if let Some(refusal) = refusal {
            frame["refusal"] = Value::String(refusal.to_owned())
        }
        follower.push(frame);
    }
}

#[derive(Default)]
struct Utf8Decoder {
    pending: Vec<u8>,
}

impl Utf8Decoder {
    fn push(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut output = String::new();
        while !self.pending.is_empty() {
            match std::str::from_utf8(&self.pending) {
                Ok(text) => {
                    output.push_str(text);
                    self.pending.clear();
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid > 0 {
                        output.push_str(
                            std::str::from_utf8(&self.pending[..valid]).unwrap_or_default(),
                        );
                        self.pending.drain(..valid);
                    }
                    match error.error_len() {
                        Some(_) => {
                            output.push('\u{FFFD}');
                            self.pending.remove(0);
                        }
                        None => break,
                    }
                }
            }
        }
        output
    }

    fn finish(&mut self) -> String {
        let output = String::from_utf8_lossy(&self.pending).into_owned();
        self.pending.clear();
        output
    }
}

struct Screen {
    cols: usize,
    rows: usize,
    history: VecDeque<Vec<char>>,
    lines: Vec<Vec<char>>,
    row: usize,
    col: usize,
    saved: (usize, usize),
    escape: Escape,
    csi: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Escape {
    Text,
    Csi,
    Osc,
    OscTerminator,
    Single,
}

impl Screen {
    fn new(cols: usize, rows: usize) -> Self {
        Self {
            cols,
            rows,
            history: VecDeque::new(),
            lines: (0..rows).map(|_| Vec::new()).collect(),
            row: 0,
            col: 0,
            saved: (0, 0),
            escape: Escape::Text,
            csi: String::new(),
        }
    }

    fn resize(&mut self, cols: usize, rows: usize) {
        self.cols = cols;
        self.rows = rows;
        while self.lines.len() < rows {
            self.lines.push(Vec::new())
        }
        self.lines.truncate(rows);
        self.row = self.row.min(rows.saturating_sub(1));
        for line in &mut self.lines {
            line.truncate(cols)
        }
        self.col = self.col.min(cols.saturating_sub(1));
    }

    fn serialize(&self) -> String {
        self.history
            .iter()
            .chain(self.lines.iter())
            .map(|line| line.iter().collect::<String>().trim_end().to_owned())
            .collect::<Vec<_>>()
            .join("\r\n")
    }

    fn feed(&mut self, text: &str) {
        for character in text.chars() {
            match self.escape {
                Escape::Text => match character {
                    '\x1B' => self.escape = Escape::Single,
                    '\r' => self.col = 0,
                    '\n' => self.newline(),
                    '\u{8}' => self.col = self.col.saturating_sub(1),
                    '\t' => self.col = (self.col + 8) & !7,
                    character if character >= ' ' && character != '\u{7F}' => self.put(character),
                    _ => {}
                },
                Escape::Single => match character {
                    '[' => {
                        self.escape = Escape::Csi;
                        self.csi.clear()
                    }
                    ']' => self.escape = Escape::Osc,
                    '7' => self.saved = (self.row, self.col),
                    '8' => {
                        let (row, col) = self.saved;
                        self.row = row;
                        self.col = col;
                        self.escape = Escape::Text
                    }
                    _ => self.escape = Escape::Text,
                },
                Escape::Csi => {
                    if ('\x40'..='\x7E').contains(&character) {
                        let parameters = std::mem::take(&mut self.csi);
                        self.apply_csi(&parameters, character);
                        self.escape = Escape::Text;
                    } else if self.csi.len() < 256 {
                        self.csi.push(character)
                    } else {
                        self.csi.clear();
                        self.escape = Escape::Text;
                    }
                }
                Escape::Osc => {
                    if character == '\x07' {
                        self.escape = Escape::Text
                    } else if character == '\x1B' {
                        self.escape = Escape::OscTerminator
                    }
                }
                Escape::OscTerminator => {
                    self.escape = if character == '\\' {
                        Escape::Text
                    } else {
                        Escape::Osc
                    }
                }
            }
        }
    }

    fn put(&mut self, character: char) {
        while self.lines[self.row].len() <= self.col {
            self.lines[self.row].push(' ')
        }
        self.lines[self.row][self.col] = character;
        self.col += 1;
        if self.col >= self.cols {
            self.col = 0;
            self.newline();
        }
    }

    fn newline(&mut self) {
        if self.row + 1 >= self.rows {
            self.history.push_back(std::mem::take(&mut self.lines[0]));
            while self.history.len() > SCROLLBACK {
                self.history.pop_front();
            }
            self.lines.remove(0);
            self.lines.push(Vec::new());
        } else {
            self.row += 1
        }
    }

    fn apply_csi(&mut self, parameters: &str, final_char: char) {
        let numbers: Vec<usize> = parameters
            .split(';')
            .filter(|value| !value.is_empty())
            .map(|value| value.parse().unwrap_or(0))
            .collect();
        let first = numbers.first().copied().unwrap_or(1).max(1);
        match final_char {
            'A' => self.row = self.row.saturating_sub(first),
            'B' => self.row = (self.row + first).min(self.rows - 1),
            'C' => self.col = (self.col + first).min(self.cols - 1),
            'D' => self.col = self.col.saturating_sub(first),
            'E' => {
                self.row = (self.row + first).min(self.rows - 1);
                self.col = 0
            }
            'F' => {
                self.row = self.row.saturating_sub(first);
                self.col = 0
            }
            'G' => self.col = (first - 1).min(self.cols - 1),
            'H' | 'f' => {
                self.row = (first - 1).min(self.rows - 1);
                self.col = (numbers.get(1).copied().unwrap_or(1).max(1) - 1).min(self.cols - 1)
            }
            'J' => match numbers.first().copied().unwrap_or(0) {
                2 | 3 => {
                    for line in &mut self.lines {
                        line.clear()
                    }
                }
                _ => {}
            },
            'K' => {
                let count = numbers.first().copied().unwrap_or(0);
                if count == 2 {
                    self.lines[self.row].clear()
                } else if count == 0 {
                    self.lines[self.row].truncate(self.col)
                }
            }
            's' => self.saved = (self.row, self.col),
            'u' => {
                let (row, col) = self.saved;
                self.row = row.min(self.rows - 1);
                self.col = col.min(self.cols - 1)
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    fn controller(home: PathBuf) -> Arc<Controller> {
        Arc::new(Controller::new(ApiContext::new(
            "http://127.0.0.1",
            "test-token",
            home,
        )))
    }

    static NEXT_HOME: AtomicU64 = AtomicU64::new(1);

    struct TestHome(PathBuf);

    impl TestHome {
        fn new() -> Self {
            use std::hash::{BuildHasher, Hasher};

            for _ in 0..16 {
                let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
                hasher.write_u128(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos(),
                );
                let path = std::env::temp_dir().join(format!(
                    "dsh-process-api-test-{}-{}-{:016x}",
                    std::process::id(),
                    NEXT_HOME.fetch_add(1, Ordering::Relaxed),
                    hasher.finish()
                ));
                match std::fs::create_dir(&path) {
                    Ok(()) => return Self(path),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("create test home: {error}"),
                }
            }
            panic!("could not allocate a unique test home");
        }
    }

    impl Drop for TestHome {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove test home");
        }
    }

    fn harmless_shell() -> (PathBuf, Vec<String>) {
        #[cfg(windows)]
        {
            let program = std::env::var_os("COMSPEC").unwrap_or_else(|| "cmd.exe".into());
            (
                PathBuf::from(program),
                vec![
                    "/d".to_owned(),
                    "/c".to_owned(),
                    "set /p unused=".to_owned(),
                ],
            )
        }
        #[cfg(unix)]
        {
            (
                PathBuf::from("/bin/sh"),
                vec!["-c".to_owned(), "read ignored".to_owned()],
            )
        }
    }

    #[test]
    fn decoder_preserves_bom_split_utf8_and_replacement() {
        let mut decoder = Utf8Decoder::default();
        assert_eq!(decoder.push(&[0xEF, 0xBB]), "");
        assert_eq!(decoder.push(&[0xBF, b'A', 0xE2, 0x82]), "\u{FEFF}A");
        assert_eq!(decoder.push(&[0xAC]), "€");
        assert_eq!(decoder.push(&[0xF0, 0x9F]), "");
        assert_eq!(decoder.finish(), "\u{FFFD}");
    }

    #[test]
    fn validates_wire_arguments() {
        assert!(valid_id("terminal-1_A"));
        assert!(!valid_id("../bad"));
        assert!(dimensions(&json!({ "cols": 2, "rows": 1 })).is_ok());
        assert!(dimensions(&json!({ "cols": 1, "rows": 1 })).is_err());
        assert!(parse_input(&json!({ "type": "write", "sequence": 2, "data": "x" }), 1).is_err());
        assert!(parse_input(
            &json!({ "type": "write", "sequence": 1, "data": &"x".repeat(MAX_INPUT_BYTES + 1) }),
            1
        )
        .is_err());
        assert!(parse_input(
            &json!({ "type": "write", "sequence": MAX_SAFE_INTEGER, "data": "x" }),
            MAX_SAFE_INTEGER
        )
        .is_ok());
        assert_eq!(
            terminal_title(&json!({ "title": "  shell  " })).unwrap(),
            "shell"
        );
        assert!(terminal_title(&json!({ "title": "   " })).is_err());
        assert!(terminal_title(&json!({ "title": format!("{} ", "x".repeat(120)) })).is_err());
        assert!(terminal_title(&json!({ "title": "𠀀".repeat(61) })).is_err());
    }

    #[tokio::test]
    async fn follower_counts_encoded_bytes_and_finishes_gracefully() {
        let (mut follower, mut reader) = Follower::new();
        let frame = json!({ "type": "output", "sequence": 1, "data": "€" });
        follower.push(frame.clone());
        assert_eq!(follower.queue.lock().unwrap().bytes, encoded_len(&frame));
        follower.finish();
        assert_eq!(reader.next().await.unwrap().unwrap(), frame);
        assert!(reader.next().await.is_none());
    }

    fn spawn_harmless_for_test(
        program: &Path,
        args: &[String],
        cwd: &Path,
    ) -> Option<OwnedProcess> {
        match spawn_process(program, args, cwd, 80, 24, "test-session") {
            Ok(process) => Some(process),
            // Some locked-down Windows hosts deny even anonymous pipe creation.
            // Keep the child tests active on hosts where ConPTY is permitted.
            Err(error) if cfg!(windows) && error.contains("os error 998") => {
                eprintln!("skipping ConPTY child test: {error}");
                None
            }
            Err(error) => panic!("spawn harmless child: {error}"),
        }
    }

    #[tokio::test]
    async fn harmless_child_output_is_drained_after_exit() {
        let cwd = std::env::temp_dir();
        #[cfg(windows)]
        let (program, args) = {
            let executable = std::env::var_os("COMSPEC").unwrap_or_else(|| "cmd.exe".into());
            (
                PathBuf::from(executable),
                vec![
                    "/d".to_owned(),
                    "/c".to_owned(),
                    "echo process-api-marker".to_owned(),
                ],
            )
        };
        #[cfg(unix)]
        let (program, args) = (
            PathBuf::from("/bin/sh"),
            vec!["-c".to_owned(), "printf 'process-api-marker\\n'".to_owned()],
        );
        let Some(mut process) = spawn_harmless_for_test(&program, &args, &cwd) else {
            return;
        };
        let mut output = String::new();
        let mut decoder = Utf8Decoder::default();
        let mut buffer = [0u8; 4096];
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let count = process
                    .pty
                    .read(&mut buffer)
                    .await
                    .expect("read child output");
                if count == 0 {
                    break;
                }
                output.push_str(&decoder.push(&buffer[..count]));
            }
            let exit = (&mut process.exit).await.expect("child exit result");
            assert_eq!(exit.expect("child wait"), 0);
        })
        .await
        .expect("child output and exit complete");
        output.push_str(&decoder.finish());
        assert!(output.contains("process-api-marker"), "{output:?}");
        process.finished = true;
        process.finish();
    }

    #[tokio::test]
    async fn harmless_child_is_terminated_and_reaped() {
        let (program, args) = harmless_shell();
        let Some(mut process) = spawn_harmless_for_test(&program, &args, &std::env::temp_dir())
        else {
            return;
        };
        tokio::time::timeout(Duration::from_secs(10), process.terminate_and_wait())
            .await
            .expect("termination completes")
            .expect("terminate harmless child");
        assert!(process.finished);
        assert!(process.control.finished.load(Ordering::Acquire));
        process.finish();
    }

    #[tokio::test]
    async fn terminal_output_exit_retention_close_and_shell_validation() {
        let home = TestHome::new();
        let controller = controller(home.0.clone());
        let rejected = std::env::current_exe().expect("current executable");
        let invalid = json!({
            "sessionId": "session-test", "id": "terminal-test", "cols": 80, "rows": 24,
            "shellPath": rejected.to_string_lossy(),
        });
        assert!(controller.create(&invalid).await.is_err());

        let payload = json!({
            "sessionId": "session-test", "id": "terminal-test", "cols": 80, "rows": 24,
        });
        let created = match controller.create(&payload).await {
            Ok(created) => created,
            Err(error) if cfg!(windows) && error.message.contains("os error 998") => {
                eprintln!("skipping ConPTY terminal test: {error:?}");
                return;
            }
            Err(error) => panic!("create terminal: {error:?}"),
        };
        assert_eq!(created["state"], "running");
        let (input_tx, input_rx) = mpsc::channel(4);
        let mut attachment = controller.attach(&payload, input_rx);
        let snapshot = tokio::time::timeout(Duration::from_secs(10), attachment.next())
            .await
            .expect("attach completes")
            .expect("attachment opens")
            .expect("snapshot");
        assert_eq!(snapshot["type"], "snapshot");
        assert_eq!(snapshot["writable"], true);
        let command = if cfg!(windows) {
            "echo process-api-marker& exit\r\n"
        } else {
            "printf 'process-api-marker\\n'; exit\r\n"
        };
        input_tx
            .send(json!({ "type": "write", "sequence": 1, "data": command }))
            .await
            .expect("send terminal input");
        let mut saw_marker = false;
        let mut saw_exit = false;
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(frame) = attachment.next().await {
                let frame = frame.expect("terminal frame");
                if frame["type"] == "output"
                    && frame["data"]
                        .as_str()
                        .is_some_and(|data| data.contains("process-api-marker"))
                {
                    saw_marker = true;
                }
                if frame["type"] == "state" && frame["info"]["state"] == "exited" {
                    saw_exit = true;
                    break;
                }
            }
        })
        .await
        .expect("terminal exits");
        assert!(saw_marker, "marker output was not delivered");
        assert!(saw_exit, "final terminal state was not delivered");
        let listed = controller
            .terminal_list("session-test")
            .expect("list terminals");
        assert_eq!(listed.as_array().map(Vec::len), Some(1));
        assert_eq!(listed[0]["state"], "exited");
        controller
            .close("session-test", "terminal-test")
            .await
            .expect("close terminal");
        assert!(controller.create(&payload).await.is_err());
        controller.dispose().await;
    }

    #[test]
    fn pending_allocation_guard_releases_capacity_on_drop() {
        let state = Arc::new(Mutex::new(State::default()));
        let session = "session-test".to_owned();
        let id = "terminal-test".to_owned();
        let pending = Arc::new(PendingCreate {
            notify: watch::channel(None).0,
            cancelled: AtomicBool::new(false),
        });
        lock(&state)
            .sessions
            .entry(session.clone())
            .or_default()
            .pending
            .insert(id.clone(), pending.clone());
        {
            let _cleanup = PendingCleanup {
                state: state.clone(),
                session: session.clone(),
                id: id.clone(),
                pending,
            };
        }
        assert!(lock(&state).sessions[&session].pending.is_empty());
    }

    fn job_fixture() -> Job {
        Job {
            id: "bash-test".to_owned(),
            kind: "bash".to_owned(),
            label: "test".to_owned(),
            owner: Some("session-a".to_owned()),
            output_limit: None,
            status: "running",
            progress: None,
            detail: None,
            exit_code: None,
            started_at: 1,
            finished_at: None,
            chunks: VecDeque::new(),
            earliest: 0,
            total: 0,
            kill_reason: None,
            cancel: None,
        }
    }

    #[tokio::test]
    async fn job_reads_resume_at_chunk_boundaries_and_fence_sessions() {
        let home = TestHome::new();
        let controller = controller(home.0.clone());
        let mut job = job_fixture();
        job.chunks.push_back(Chunk {
            at: 0,
            text: "abcd".to_owned(),
            channel: Some("stdout"),
            gap_before: false,
        });
        job.chunks.push_back(Chunk {
            at: 4,
            text: "efgh".to_owned(),
            channel: Some("stderr"),
            gap_before: false,
        });
        job.total = 8;
        lock(&controller.state).jobs.push(job);

        let (chunks, next, lossy, _) = controller
            .view()
            .read_job("bash-test", Some("session-a"), 3)
            .unwrap();
        assert_eq!(chunks[0]["at"], 0);
        assert_eq!(chunks[0]["text"], "abcd");
        assert_eq!(chunks[1]["at"], 4);
        assert_eq!(chunks[1]["text"], "efgh");
        assert_eq!(next, 8);
        assert!(!lossy);

        let error = controller
            .view()
            .read_job("bash-test", Some("session-b"), 0)
            .unwrap_err();
        assert_eq!(error.code, "job/not-found");

        for payload in [
            json!({ "jobId": "bash-test", "sessionId": null }),
            json!({ "jobId": "bash-test", "from": null }),
            json!({ "jobId": "bash-test", "from": -1 }),
        ] {
            let error = controller
                .job_follow(&payload)
                .next()
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, "gateway/arguments-invalid");
        }
    }

    #[tokio::test]
    async fn job_kill_is_session_fenced_and_preserves_the_cancel_reason() {
        let home = TestHome::new();
        let controller = controller(home.0.clone());
        let calls = Arc::new(AtomicU64::new(0));
        let mut job = job_fixture();
        let cancel_calls = calls.clone();
        job.cancel = Some(Arc::new(move |reason| {
            assert_eq!(reason, KILL_REASON);
            cancel_calls.fetch_add(1, Ordering::AcqRel);
        }));
        lock(&controller.state).jobs.push(job);

        let payload = json!({ "sessionId": "session-a", "jobId": "bash-test" });
        assert_eq!(
            controller.kill_job(&payload).unwrap(),
            json!({ "outcome": "requested" })
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(lock(&controller.state).jobs[0].status, "stopping");
        assert_eq!(
            lock(&controller.state).jobs[0].kill_reason.as_deref(),
            Some(KILL_REASON)
        );

        let foreign = json!({ "sessionId": "session-b", "jobId": "bash-test" });
        assert_eq!(
            controller.kill_job(&foreign).unwrap_err().code,
            "job/not-found"
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);

        lock(&controller.state).jobs[0].status = "completed";
        assert_eq!(
            controller.kill_job(&payload).unwrap(),
            json!({ "outcome": "already-finished" })
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);
    }
}
