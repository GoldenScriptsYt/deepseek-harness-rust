use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{broadcast, mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::protocol::ensure_modern_result;
use crate::{
    ClientOptions, ErrorKind, McpError, RequestOptions, RpcError, ServerNotification,
    StdioServerConfig,
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Era {
    Legacy,
    Modern,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum RequestId {
    Number(i64),
    String(String),
}

impl RequestId {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Number(number) => number.as_i64().map(Self::Number),
            Value::String(string) => Some(Self::String(string.clone())),
            _ => None,
        }
    }

    fn to_value(&self) -> Value {
        match self {
            Self::Number(number) => Value::from(*number),
            Self::String(string) => Value::String(string.clone()),
        }
    }
}

struct PendingRequest {
    method: String,
    response: oneshot::Sender<Result<Value, McpError>>,
}

enum CommandMessage {
    Request {
        id: Option<RequestId>,
        method: String,
        frame: Vec<u8>,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<Value, McpError>>,
    },
}

struct CloseCommand {
    response: oneshot::Sender<CloseReport>,
}

pub(crate) struct RuntimeHandle {
    commands: mpsc::Sender<CommandMessage>,
    cancellations: mpsc::Sender<RequestId>,
    cancellation_overflow: Arc<AtomicBool>,
    cancellation_wakeup: Arc<Notify>,
    close: mpsc::Sender<CloseCommand>,
    task: Mutex<Option<JoinHandle<CloseReport>>>,
    shutdown_lock: tokio::sync::Mutex<()>,
    report: Mutex<Option<CloseReport>>,
    stderr: Arc<Mutex<StderrCapture>>,
    output_budget: Arc<Semaphore>,
    options: Arc<ClientOptions>,
    era: Era,
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        let (response, _) = oneshot::channel();
        // The actor owns the child and reaps it after this wake-up. Aborting the
        // task here would drop tokio::process::Child without awaiting exit.
        let _ = self.close.try_send(CloseCommand { response });
    }
}

impl RuntimeHandle {
    pub(crate) fn spawn(
        config: &StdioServerConfig,
        options: Arc<ClientOptions>,
        notification_capacity: usize,
        era: Era,
    ) -> Result<(Self, broadcast::Receiver<ServerNotification>), McpError> {
        config.validate()?;
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.creation_flags(0x0800_0000);
        }
        command.env_clear();
        for key in SAFE_INHERITED_ENV {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in &config.env {
            command.env(key, value);
        }
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().map_err(|error| {
            McpError::io(
                format!("failed to start {}: {error}", config.command),
                error,
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::closed("child stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::closed("child stdout was not piped"))?;
        let child_stderr = child
            .stderr
            .take()
            .ok_or_else(|| McpError::closed("child stderr was not piped"))?;
        let reader = FrameReader::new(stdout, options.max_frame_bytes);
        let stderr = Arc::new(Mutex::new(StderrCapture::new(options.max_stderr_bytes)));
        let stderr_task = tokio::spawn(drain_stderr(child_stderr, Arc::clone(&stderr)));
        let (command_tx, command_rx) = mpsc::channel(options.max_pending_requests);
        let output_budget = Arc::new(Semaphore::new(options.max_buffered_output_bytes));
        // A request sends at most one cancellation, and completed requests
        // disarm their guard. This therefore bounds queued cancellations by the
        // number of pending requests without an unbounded control channel.
        let (cancel_tx, cancel_rx) = mpsc::channel(options.max_pending_requests);
        let cancellation_overflow = Arc::new(AtomicBool::new(false));
        let cancellation_wakeup = Arc::new(Notify::new());
        let (close_tx, close_rx) = mpsc::channel(1);
        let (notification_tx, notification_rx) = broadcast::channel(notification_capacity);
        let task_options = Arc::clone(&options);
        let actor_stderr = Arc::clone(&stderr);
        let actor_budget = Arc::clone(&output_budget);
        let actor_cancellation_overflow = Arc::clone(&cancellation_overflow);
        let actor_cancellation_wakeup = Arc::clone(&cancellation_wakeup);
        let task = tokio::spawn(async move {
            run_actor(
                child,
                stdin,
                reader,
                stderr_task,
                actor_stderr,
                command_rx,
                cancel_rx,
                actor_cancellation_overflow,
                actor_cancellation_wakeup,
                close_rx,
                notification_tx,
                task_options,
                actor_budget,
                era,
            )
            .await
        });
        Ok((
            Self {
                commands: command_tx,
                cancellations: cancel_tx,
                cancellation_overflow,
                cancellation_wakeup,
                close: close_tx,
                task: Mutex::new(Some(task)),
                shutdown_lock: tokio::sync::Mutex::new(()),
                report: Mutex::new(None),
                stderr,
                output_budget,
                options,
                era,
            },
            notification_rx,
        ))
    }

    pub(crate) async fn request(
        &self,
        method: &str,
        params: Value,
        request_options: &RequestOptions,
    ) -> Result<Value, McpError> {
        let timeout = request_options
            .timeout
            .unwrap_or(self.options.request_timeout);
        if timeout.is_zero() {
            return Err(
                McpError::configuration("request timeout must be positive").with_method(method)
            );
        }
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            McpError::configuration("request timeout is too large").with_method(method)
        })?;
        let id = RequestId::String(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed).to_string());
        let mut permit = acquire_output(
            &self.output_budget,
            self.options.max_frame_bytes,
            deadline,
            method,
        )
        .await?;
        let frame = encode_frame(
            json!({"jsonrpc":"2.0","id":id.to_value(),"method":method,"params":params}),
            self.options.max_frame_bytes,
            Some(method),
        )?;
        if let Some(unused) = permit.split(frame.len()) {
            drop(unused);
        }
        let (response_tx, response_rx) = oneshot::channel();
        let mut guard = CancelGuard::disarmed(
            self.cancellations.clone(),
            Arc::clone(&self.cancellation_overflow),
            Arc::clone(&self.cancellation_wakeup),
            id.clone(),
        );
        tokio::time::timeout_at(
            deadline,
            self.commands.send(CommandMessage::Request {
                id: Some(id),
                method: method.to_owned(),
                frame,
                deadline,
                permit,
                response: response_tx,
            }),
        )
        .await
        .map_err(|_| McpError::timeout(method))?
        .map_err(|_| McpError::closed("MCP runtime is closed"))?;
        guard.arm();
        let result = tokio::time::timeout_at(deadline, response_rx).await;
        match result {
            Ok(Ok(result)) => {
                guard.disarm();
                result
            }
            Ok(Err(_)) => {
                guard.disarm();
                Err(McpError::closed("MCP runtime closed before responding"))
            }
            // Keep the guard armed so its drop emits exactly one cancellation.
            Err(_) => Err(McpError::timeout(method)),
        }
    }

    pub(crate) fn protocol_params(
        &self,
        method: &str,
        mut params: Value,
        request_options: &RequestOptions,
    ) -> Result<Value, McpError> {
        if params.is_null() {
            params = Value::Object(Map::new());
        }
        let object = params.as_object_mut().ok_or_else(|| {
            McpError::configuration(format!("{method} parameters must be an object"))
        })?;
        let mut meta = object
            .entry("_meta")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object()
            .cloned()
            .ok_or_else(|| McpError::configuration("_meta must be an object"))?;
        if let Some(token) = &request_options.progress_token {
            meta.insert("progressToken".to_owned(), Value::String(token.clone()));
        }
        if self.era == Era::Modern {
            meta.insert(
                "io.modelcontextprotocol/protocolVersion".to_owned(),
                Value::String(crate::MODERN_PROTOCOL_VERSION.to_owned()),
            );
            meta.insert(
                "io.modelcontextprotocol/clientCapabilities".to_owned(),
                Value::Object(Map::new()),
            );
            meta.insert(
                "io.modelcontextprotocol/clientInfo".to_owned(),
                serde_json::to_value(&self.options.client_info).map_err(|error| {
                    McpError::new(
                        ErrorKind::Serialization,
                        format!("failed to serialize client info: {error}"),
                        Some(method),
                        None,
                        None,
                    )
                })?,
            );
        }
        if !meta.is_empty() {
            object.insert("_meta".to_owned(), Value::Object(meta));
        } else {
            object.remove("_meta");
        }
        Ok(params)
    }

    pub(crate) async fn notify_initialized(
        &self,
        startup_timeout: Duration,
    ) -> Result<(), McpError> {
        let method = "notifications/initialized";
        let deadline = Instant::now()
            .checked_add(startup_timeout)
            .ok_or_else(|| McpError::configuration("startup timeout is too large"))?;
        let mut permit = acquire_output(
            &self.output_budget,
            self.options.max_frame_bytes,
            deadline,
            method,
        )
        .await?;
        let frame = encode_frame(
            json!({"jsonrpc":"2.0","method":method}),
            self.options.max_frame_bytes,
            Some(method),
        )?;
        if let Some(unused) = permit.split(frame.len()) {
            drop(unused);
        }
        let (response_tx, response_rx) = oneshot::channel();
        tokio::time::timeout_at(
            deadline,
            self.commands.send(CommandMessage::Request {
                id: None,
                method: method.to_owned(),
                frame,
                deadline,
                permit,
                response: response_tx,
            }),
        )
        .await
        .map_err(|_| McpError::write_timeout(method))?
        .map_err(|_| McpError::closed("MCP runtime is closed"))?;
        tokio::time::timeout_at(deadline, response_rx)
            .await
            .map_err(|_| McpError::write_timeout(method))?
            .map_err(|_| McpError::closed("MCP runtime closed before initialization"))?
            .map(|_| ())
    }

    pub(crate) fn max_list_pages(&self) -> usize {
        self.options.max_list_pages
    }

    pub(crate) fn stderr(&self) -> StderrSnapshot {
        self.stderr
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot()
    }

    pub(crate) async fn shutdown(&self) -> CloseReport {
        let _serial = self.shutdown_lock.lock().await;
        let mut task = self
            .task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let cached_report = self
            .report
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(report) = cached_report {
            if let Some(task) = task.as_mut() {
                let _ = task.await;
            }
            return report;
        }
        let Some(mut task) = task else {
            return self.fallback_close_report("MCP runtime task is unavailable", false);
        };
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self.close.try_send(CloseCommand {
            response: response_tx,
        });
        let wait_limit = self
            .options
            .close_timeout
            .saturating_mul(3)
            .saturating_add(Duration::from_millis(100));
        let deadline = Instant::now()
            .checked_add(wait_limit)
            .unwrap_or_else(Instant::now);
        let outcome = tokio::time::timeout_at(deadline, response_rx).await;
        let report = match outcome {
            Ok(Ok(report)) => {
                let _ = task.await;
                report
            }
            Ok(Err(_)) => match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(report)) => report,
                Ok(Err(error)) => {
                    task.abort();
                    let _ = task.await;
                    self.fallback_close_report(&runtime_task_error(error), false)
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    self.fallback_close_report("MCP runtime did not finish shutdown", true)
                }
            },
            Err(_) => {
                task.abort();
                let _ = task.await;
                self.fallback_close_report("MCP runtime did not finish shutdown", true)
            }
        };
        *self
            .report
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(report.clone());
        report
    }

    fn fallback_close_report(&self, cleanup_error: &str, timed_out: bool) -> CloseReport {
        CloseReport {
            exit_status: None,
            forced: false,
            timed_out,
            stderr: self.stderr(),
            cleanup_error: Some(cleanup_error.to_owned()),
        }
    }
}

fn runtime_task_error(error: tokio::task::JoinError) -> String {
    if error.is_panic() {
        "MCP runtime task panicked".to_owned()
    } else {
        "MCP runtime task ended without a close report".to_owned()
    }
}

async fn acquire_output(
    budget: &Arc<Semaphore>,
    bytes: usize,
    deadline: Instant,
    method: &str,
) -> Result<OwnedSemaphorePermit, McpError> {
    let bytes = u32::try_from(bytes)
        .map_err(|_| McpError::configuration("outgoing frame is too large to buffer"))?;
    tokio::time::timeout_at(deadline, Arc::clone(budget).acquire_many_owned(bytes))
        .await
        .map_err(|_| McpError::timeout(method))?
        .map_err(|_| McpError::closed("MCP output budget is closed"))
}

struct CancelGuard {
    sender: mpsc::Sender<RequestId>,
    overflow: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    id: RequestId,
    armed: bool,
}

impl CancelGuard {
    fn disarmed(
        sender: mpsc::Sender<RequestId>,
        overflow: Arc<AtomicBool>,
        wakeup: Arc<Notify>,
        id: RequestId,
    ) -> Self {
        Self {
            sender,
            overflow,
            wakeup,
            id,
            armed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Err(mpsc::error::TrySendError::Full(_)) = self.sender.try_send(self.id.clone()) {
                // Never silently lose a cancellation. The actor observes this
                // bounded failure signal and fails pending work deterministically.
                self.overflow.store(true, Ordering::Release);
                self.wakeup.notify_one();
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StderrSnapshot {
    pub text: String,
    pub truncated: bool,
}

struct StderrCapture {
    bytes: Vec<u8>,
    limit: usize,
    truncated: bool,
}

impl StderrCapture {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            truncated: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        let remaining = self.limit.saturating_sub(self.bytes.len());
        let retained = remaining.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..retained]);
        if retained < bytes.len() {
            self.truncated = true;
        }
    }

    fn snapshot(&self) -> StderrSnapshot {
        let mut text = String::from_utf8_lossy(&self.bytes).into_owned();
        let truncated = self.truncated || text.len() > self.limit;
        if text.len() > self.limit {
            let mut end = self.limit;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
        }
        StderrSnapshot { text, truncated }
    }
}

async fn drain_stderr(
    mut reader: ChildStderr,
    capture: Arc<Mutex<StderrCapture>>,
) -> io::Result<()> {
    let mut buffer = [0; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        capture
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(&buffer[..count]);
    }
}

struct FrameReader<R> {
    reader: R,
    pending: Vec<u8>,
    max_bytes: usize,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    fn new(reader: R, max_bytes: usize) -> Self {
        Self {
            reader,
            pending: Vec::new(),
            max_bytes,
        }
    }

    async fn next_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(position) = self.pending.iter().position(|byte| *byte == b'\n') {
                if position + 1 > self.max_bytes {
                    return Err(frame_too_large(self.max_bytes));
                }
                let mut frame: Vec<u8> = self.pending.drain(..=position).collect();
                frame.pop();
                if frame.last() == Some(&b'\r') {
                    frame.pop();
                }
                if frame.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Ok(Some(frame));
            }
            if self.pending.len() >= self.max_bytes {
                return Err(frame_too_large(self.max_bytes));
            }
            let read_limit = (self.max_bytes - self.pending.len()).min(8192);
            let mut chunk = [0; 8192];
            let count = self.reader.read(&mut chunk[..read_limit]).await?;
            if count == 0 {
                return if self.pending.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "child closed stdout with an incomplete JSON frame",
                    ))
                };
            }
            self.pending.extend_from_slice(&chunk[..count]);
        }
    }
}

fn frame_too_large(max_bytes: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("stdout frame exceeds {max_bytes} bytes"),
    )
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8192)),
            limit,
            exceeded: false,
        }
    }
}

impl io::Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("outgoing frame exceeds {} bytes", self.limit),
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_frame(
    message: Value,
    max_bytes: usize,
    method: Option<&str>,
) -> Result<Vec<u8>, McpError> {
    let mut buffer = BoundedBuffer::new(max_bytes);
    if let Err(error) = serde_json::to_writer(&mut buffer, &message) {
        if buffer.exceeded {
            return Err(McpError::new(
                ErrorKind::Protocol,
                format!("outgoing frame exceeds {max_bytes} bytes"),
                method,
                None,
                None,
            ));
        }
        return Err(McpError::new(
            ErrorKind::Serialization,
            format!("failed to serialize JSON-RPC message: {error}"),
            method,
            None,
            None,
        ));
    }
    if buffer.bytes.len() >= max_bytes {
        return Err(McpError::new(
            ErrorKind::Protocol,
            format!("outgoing frame exceeds {max_bytes} bytes"),
            method,
            None,
            None,
        ));
    }
    buffer.bytes.push(b'\n');
    Ok(buffer.bytes)
}

/// One resumable step of delivering an outgoing frame.
#[derive(Debug)]
enum WriteStep {
    /// The child accepted this many more bytes of the frame.
    Accepted(usize),
    /// Every byte was accepted and the flush completed.
    Flushed,
}

/// Perform the next step of delivering one frame.
///
/// Each step is a single `write` or `flush` call, so a future dropped between
/// polls loses no progress: a pipe write either accepts bytes and reports their
/// count, or accepts nothing and waits. The actor keeps the accepted count in
/// [`CurrentWrite`], which is what lets its `select!` loop interrupt a frame
/// and resume it instead of writing it a second time.
async fn write_step<W>(
    writer: &mut W,
    frame: &OutboundFrame,
    written: usize,
    deadline: Instant,
) -> Result<WriteStep, McpError>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let method = frame.method.as_str();
    let pending = if written < frame.bytes.len() {
        tokio::time::timeout_at(deadline, writer.write(&frame.bytes[written..]))
            .await
            .map(|result| result.map(WriteStep::Accepted))
    } else {
        tokio::time::timeout_at(deadline, writer.flush())
            .await
            .map(|result| result.map(|()| WriteStep::Flushed))
    };
    match pending {
        Ok(Ok(WriteStep::Accepted(0))) => Err(McpError::io(
            format!("failed to write child stdin for {method}: write accepted no bytes"),
            io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer"),
        )
        .with_method(method)),
        Ok(Ok(step)) => Ok(step),
        Ok(Err(error)) => Err(McpError::io(
            format!("failed to write child stdin for {method}: {error}"),
            error,
        )
        .with_method(method)),
        Err(_) => Err(McpError::write_timeout(method)),
    }
}

enum WriteCompletion {
    None,
    Notification(oneshot::Sender<Result<Value, McpError>>),
}

struct OutboundFrame {
    bytes: Vec<u8>,
    method: String,
    deadline: Instant,
    request_id: Option<RequestId>,
    permit: Option<OwnedSemaphorePermit>,
    completion: WriteCompletion,
}

impl OutboundFrame {
    fn len(&self) -> usize {
        self.bytes.len()
    }
}

struct CurrentWrite {
    frame: OutboundFrame,
    /// Bytes of `frame` the child has already accepted.
    written: usize,
}

enum ActorEvent {
    Wrote(Result<WriteStep, McpError>),
    Frame(io::Result<Option<Vec<u8>>>),
}

fn queue_frame(
    queue: &mut VecDeque<OutboundFrame>,
    buffered_bytes: &mut usize,
    mut frame: OutboundFrame,
    limit: usize,
    budget: &Arc<Semaphore>,
) -> Result<(), McpError> {
    if frame.permit.is_none() {
        let bytes = u32::try_from(frame.len())
            .map_err(|_| McpError::configuration("outgoing frame is too large to buffer"))?;
        frame.permit = Some(
            Arc::clone(budget)
                .try_acquire_many_owned(bytes)
                .map_err(|_| {
                    McpError::new(
                        ErrorKind::Overloaded,
                        format!("buffered outgoing frames exceed {limit} bytes"),
                        Some(&frame.method),
                        None,
                        None,
                    )
                })?,
        );
    }
    if buffered_bytes.saturating_add(frame.len()) > limit {
        return Err(McpError::new(
            ErrorKind::Overloaded,
            format!("buffered outgoing frames exceed {limit} bytes"),
            Some(&frame.method),
            None,
            None,
        ));
    }
    *buffered_bytes += frame.len();
    queue.push_back(frame);
    Ok(())
}

fn remove_queued_request(
    queue: &mut VecDeque<OutboundFrame>,
    buffered_bytes: &mut usize,
    id: &RequestId,
) {
    let before = *buffered_bytes;
    *buffered_bytes = 0;
    queue.retain(|frame| {
        if frame.request_id.as_ref() == Some(id) {
            false
        } else {
            *buffered_bytes += frame.len();
            true
        }
    });
    debug_assert!(*buffered_bytes <= before);
}

fn cancellation_frame(
    id: &RequestId,
    max_frame_bytes: usize,
    close_timeout: Duration,
) -> Result<OutboundFrame, McpError> {
    let method = "notifications/cancelled";
    let deadline = Instant::now()
        .checked_add(close_timeout)
        .ok_or_else(|| McpError::configuration("cancellation write timeout is too large"))?;
    Ok(OutboundFrame {
        bytes: encode_frame(
            json!({"jsonrpc":"2.0","method":method,"params":{"requestId":id.to_value(),"reason":"Request cancelled by client"}}),
            max_frame_bytes,
            Some(method),
        )?,
        method: method.to_owned(),
        deadline,
        request_id: None,
        permit: None,
        completion: WriteCompletion::None,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_actor(
    mut child: Child,
    mut stdin: ChildStdin,
    mut reader: FrameReader<ChildStdout>,
    stderr_task: JoinHandle<io::Result<()>>,
    stderr_capture: Arc<Mutex<StderrCapture>>,
    mut commands: mpsc::Receiver<CommandMessage>,
    mut cancellations: mpsc::Receiver<RequestId>,
    cancellation_overflow: Arc<AtomicBool>,
    cancellation_wakeup: Arc<Notify>,
    mut close_rx: mpsc::Receiver<CloseCommand>,
    notifications: broadcast::Sender<ServerNotification>,
    options: Arc<ClientOptions>,
    output_budget: Arc<Semaphore>,
    era: Era,
) -> CloseReport {
    let mut pending: HashMap<RequestId, PendingRequest> = HashMap::new();
    // Cancellation and request delivery use separate bounded channels. Preserve
    // cancellations that win that race so a late queued frame is never sent.
    let mut early_cancellations: HashSet<RequestId> = HashSet::new();
    let mut outbound: VecDeque<OutboundFrame> = VecDeque::new();
    let mut buffered_bytes: usize = 0;
    let mut current_write: Option<CurrentWrite> = None;
    let mut fatal = None;
    let mut close_response = None;

    'actor: loop {
        if cancellation_overflow.load(Ordering::Acquire) {
            fatal = Some(McpError::new(
                ErrorKind::Overloaded,
                "cancellation queue overflowed",
                None,
                None,
                None,
            ));
            break 'actor;
        }
        if current_write.is_none() {
            if let Some(frame) = outbound.pop_front() {
                current_write = Some(CurrentWrite { frame, written: 0 });
            }
        }
        let write_enabled = current_write.is_some();
        let write_result = async {
            let current = current_write.as_mut().expect("write is enabled");
            let (frame, written) = (&current.frame, current.written);
            write_step(&mut stdin, frame, written, frame.deadline).await
        };
        let frame = tokio::select! {
            biased;
            command = close_rx.recv() => {
                if let Some(command) = command {
                    close_response = Some(command.response);
                }
                break 'actor;
            }
            _ = cancellation_wakeup.notified() => {
                if cancellation_overflow.swap(false, Ordering::AcqRel) {
                    fatal = Some(McpError::new(
                        ErrorKind::Overloaded,
                        "cancellation queue overflowed",
                        None,
                        None,
                        None,
                    ));
                    break 'actor;
                }
                continue;
            }
            id = cancellations.recv() => {
                match id {
                    Some(id) => {
                        if let Some(request) = pending.remove(&id) {
                            let _ = request.response.send(Err(McpError::cancelled(&request.method)));
                            remove_queued_request(&mut outbound, &mut buffered_bytes, &id);
                            let frame = match cancellation_frame(
                                &id,
                                options.max_frame_bytes,
                                options.close_timeout,
                            ) {
                                Ok(frame) => frame,
                                Err(error) => {
                                    fatal = Some(error);
                                    break 'actor;
                                }
                            };
                            if let Err(error) = queue_frame(
                                &mut outbound,
                                &mut buffered_bytes,
                                frame,
                                options.max_buffered_output_bytes,
                                &output_budget,
                            ) {
                                fatal = Some(error);
                                break 'actor;
                            }
                        } else if early_cancellations.len() < options.max_pending_requests {
                            early_cancellations.insert(id);
                        } else {
                            fatal = Some(McpError::new(
                                ErrorKind::Overloaded,
                                "too many cancellations arrived before their requests",
                                None,
                                None,
                                None,
                            ));
                            break 'actor;
                        }
                    }
                    None => {
                        fatal = Some(McpError::closed("client cancellation channel closed"));
                        break 'actor;
                    }
                }
                continue;
            }
            write_result = write_result, if write_enabled => {
                ActorEvent::Wrote(write_result)
            }
            frame = reader.next_frame() => ActorEvent::Frame(frame),
            command = commands.recv() => {
                match command {
                    Some(CommandMessage::Request { id, method, frame, deadline, permit, response }) => {
                        if id
                            .as_ref()
                            .is_some_and(|id| early_cancellations.remove(id))
                        {
                            let _ = response.send(Err(McpError::cancelled(&method)));
                            continue;
                        }
                        if id.is_some() && pending.len() >= options.max_pending_requests {
                            let _ = response.send(Err(McpError::new(
                                ErrorKind::Overloaded,
                                "maximum pending requests reached",
                                Some(&method),
                                None,
                                None,
                            )));
                            continue;
                        }
                        let frame_len = frame.len();
                        if buffered_bytes.saturating_add(frame_len)
                            > options.max_buffered_output_bytes
                        {
                            let error = McpError::new(
                                ErrorKind::Overloaded,
                                format!(
                                    "buffered outgoing frames exceed {} bytes",
                                    options.max_buffered_output_bytes
                                ),
                                Some(&method),
                                None,
                                None,
                            );
                            let _ = response.send(Err(error.clone()));
                            if id.is_none() {
                                fatal = Some(error);
                                break 'actor;
                            }
                            continue;
                        }
                        match id {
                            Some(id) => {
                                let frame = OutboundFrame {
                                    bytes: frame,
                                    method: method.clone(),
                                    deadline,
                                    request_id: Some(id.clone()),
                                    permit: Some(permit),
                                    completion: WriteCompletion::None,
                                };
                                queue_frame(
                                    &mut outbound,
                                    &mut buffered_bytes,
                                    frame,
                                    options.max_buffered_output_bytes,
                                    &output_budget,
                                )
                                .expect("outgoing capacity checked");
                                pending.insert(id, PendingRequest { method, response });
                            }
                            None => {
                                let frame = OutboundFrame {
                                    bytes: frame,
                                    method: method.clone(),
                                    deadline,
                                    request_id: None,
                                    permit: Some(permit),
                                    completion: WriteCompletion::Notification(response),
                                };
                                queue_frame(
                                    &mut outbound,
                                    &mut buffered_bytes,
                                    frame,
                                    options.max_buffered_output_bytes,
                                    &output_budget,
                                )
                                .expect("outgoing capacity checked");
                            }
                        }
                    }
                    None => {
                        fatal = Some(McpError::closed("client request channel closed"));
                        break 'actor;
                    }
                }
                continue;
            }
        };
        let frame = match frame {
            ActorEvent::Wrote(write_result) => {
                let current = current_write.as_mut().expect("write completed");
                match write_result {
                    Ok(WriteStep::Accepted(count)) => {
                        current.written += count;
                        continue;
                    }
                    Ok(WriteStep::Flushed) => {}
                    Err(error) => {
                        fatal = Some(error);
                        break 'actor;
                    }
                }
                let current = current_write.take().expect("write completed");
                buffered_bytes = buffered_bytes.saturating_sub(current.frame.len());
                if let WriteCompletion::Notification(response) = current.frame.completion {
                    let _ = response.send(Ok(Value::Null));
                }
                continue;
            }
            ActorEvent::Frame(frame) => frame,
        };
        match frame {
            Ok(Some(frame)) => {
                let message = match serde_json::from_slice::<Value>(&frame) {
                    Ok(message) => message,
                    Err(error) => {
                        fatal = Some(McpError::protocol(format!(
                            "malformed stdout JSON: {error}"
                        )));
                        break 'actor;
                    }
                };
                if let Err(error) = handle_message(
                    message,
                    era,
                    &mut pending,
                    &mut outbound,
                    &mut buffered_bytes,
                    &notifications,
                    &options,
                    &output_budget,
                ) {
                    fatal = Some(error);
                    break 'actor;
                }
            }
            Ok(None) => {
                fatal = Some(McpError::closed("child closed stdout"));
                break 'actor;
            }
            Err(error) => {
                let kind = match error.kind() {
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
                        ErrorKind::Protocol
                    }
                    _ => ErrorKind::Io,
                };
                let message = if kind == ErrorKind::Protocol {
                    error.to_string()
                } else {
                    format!("failed to read child stdout: {error}")
                };
                fatal = Some(McpError::new(
                    kind,
                    message,
                    None,
                    None,
                    Some(Arc::new(error)),
                ));
                break 'actor;
            }
        }
    }

    if close_response.is_some() {
        for (_, request) in pending.drain() {
            let _ = request.response.send(Err(
                McpError::closed("connection closed").with_method(&request.method)
            ));
        }
    } else if let Some(error) = fatal {
        for (_, request) in pending.drain() {
            let _ = request
                .response
                .send(Err(error.clone().with_method(&request.method)));
        }
    } else {
        for (_, request) in pending.drain() {
            let _ = request.response.send(Err(
                McpError::closed("connection closed").with_method(&request.method)
            ));
        }
    }
    current_write.take();
    outbound.clear();
    drop(stdin);
    let close_report = shutdown_child(&mut child, stderr_task, &stderr_capture, &options).await;
    if let Some(response) = close_response {
        let _ = response.send(close_report.clone());
    }
    close_report
}

#[allow(clippy::too_many_arguments)]
fn handle_message(
    message: Value,
    era: Era,
    pending: &mut HashMap<RequestId, PendingRequest>,
    outbound: &mut VecDeque<OutboundFrame>,
    buffered_bytes: &mut usize,
    notifications: &broadcast::Sender<ServerNotification>,
    options: &ClientOptions,
    output_budget: &Arc<Semaphore>,
) -> Result<(), McpError> {
    let object = message
        .as_object()
        .ok_or_else(|| McpError::protocol("JSON-RPC message must be an object"))?;
    if object.get("jsonrpc") != Some(&Value::String("2.0".to_owned())) {
        return Err(McpError::protocol(
            "JSON-RPC message must specify version 2.0",
        ));
    }
    if let Some(method_value) = object.get("method") {
        let method = method_value
            .as_str()
            .ok_or_else(|| McpError::protocol("JSON-RPC method must be a string"))?;
        if object.contains_key("result") || object.contains_key("error") {
            return Err(McpError::protocol(
                "JSON-RPC request or notification must not contain result or error",
            ));
        }
        if object
            .get("params")
            .is_some_and(|params| !params.is_object() && !params.is_array())
        {
            return Err(McpError::protocol(
                "JSON-RPC params must be an object or array",
            ));
        }
        if object.contains_key("id") {
            let id_value = object.get("id").cloned().unwrap_or(Value::Null);
            let id = RequestId::from_value(&id_value).ok_or_else(|| {
                McpError::protocol("server request id must be a string or integer")
            })?;
            let response = if method == "ping" {
                json!({"jsonrpc":"2.0","id":id.to_value(),"result":{}})
            } else {
                json!({"jsonrpc":"2.0","id":id.to_value(),"error":{
                    "code":-32601,
                    "message":"Method not found"
                }})
            };
            let deadline = Instant::now()
                .checked_add(options.request_timeout)
                .ok_or_else(|| McpError::configuration("server response timeout is too large"))?;
            let frame = OutboundFrame {
                bytes: encode_frame(response, options.max_frame_bytes, Some(method))?,
                method: method.to_owned(),
                deadline,
                request_id: None,
                permit: None,
                completion: WriteCompletion::None,
            };
            queue_frame(
                outbound,
                buffered_bytes,
                frame,
                options.max_buffered_output_bytes,
                output_budget,
            )?;
            return Ok(());
        }
        let notification = ServerNotification::new(method, object.get("params").cloned());
        if method == "notifications/cancelled" {
            let id = notification
                .params
                .as_ref()
                .and_then(|params| params.get("requestId"))
                .and_then(RequestId::from_value)
                .ok_or_else(|| {
                    McpError::protocol(
                        "notifications/cancelled requestId must be a string or integer",
                    )
                })?;
            if let Some(request) = pending.remove(&id) {
                let _ = request
                    .response
                    .send(Err(McpError::cancelled(&request.method)));
            }
        }
        let _ = notifications.send(notification);
        return Ok(());
    }
    let id_value = object
        .get("id")
        .cloned()
        .ok_or_else(|| McpError::protocol("JSON-RPC response is missing id"))?;
    let id = RequestId::from_value(&id_value)
        .ok_or_else(|| McpError::protocol("JSON-RPC response id must be a string or integer"))?;
    let response = match (object.contains_key("result"), object.contains_key("error")) {
        (true, false) => {
            let result = object
                .get("result")
                .cloned()
                .ok_or_else(|| McpError::protocol("JSON-RPC response is missing result"))?;
            Response::Result(result)
        }
        (false, true) => {
            let value = object
                .get("error")
                .cloned()
                .ok_or_else(|| McpError::protocol("JSON-RPC response is missing error"))?;
            let error = serde_json::from_value::<RpcError>(value)
                .map_err(|error| McpError::protocol(format!("invalid JSON-RPC error: {error}")))?;
            Response::Error(error)
        }
        _ => {
            return Err(McpError::protocol(
                "JSON-RPC response must contain exactly one of result or error",
            ));
        }
    };
    let Some(request) = pending.remove(&id) else {
        return Ok(());
    };
    let result = match response {
        Response::Error(error) => Err(McpError::rpc(error, Some(&request.method))),
        Response::Result(result) if era == Era::Modern => {
            ensure_modern_result(&request.method, result)
                .map_err(|error| error.with_method(&request.method))
        }
        Response::Result(result) => Ok(result),
    };
    let _ = request.response.send(result);
    Ok(())
}

enum Response {
    Result(Value),
    Error(RpcError),
}

async fn shutdown_child(
    child: &mut Child,
    mut stderr_task: JoinHandle<io::Result<()>>,
    stderr_capture: &Arc<Mutex<StderrCapture>>,
    options: &ClientOptions,
) -> CloseReport {
    let mut forced = false;
    let mut timed_out = false;
    let mut cleanup_error = None;
    let exit_status = match tokio::time::timeout(options.close_timeout, child.wait()).await {
        Ok(Ok(status)) => Some(status),
        Ok(Err(error)) => {
            record_cleanup_error(
                &mut cleanup_error,
                format!("failed to wait for child exit: {error}"),
            );
            None
        }
        Err(_) => {
            forced = true;
            timed_out = true;
            if let Err(error) = child.start_kill() {
                record_cleanup_error(&mut cleanup_error, format!("failed to kill child: {error}"));
            }
            match tokio::time::timeout(options.close_timeout, child.wait()).await {
                Ok(Ok(status)) => Some(status),
                Ok(Err(error)) => {
                    record_cleanup_error(
                        &mut cleanup_error,
                        format!("failed to wait after killing child: {error}"),
                    );
                    None
                }
                Err(_) => {
                    record_cleanup_error(
                        &mut cleanup_error,
                        "child did not exit after forced termination".to_owned(),
                    );
                    None
                }
            }
        }
    };
    match tokio::time::timeout(options.close_timeout, &mut stderr_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => record_cleanup_error(
            &mut cleanup_error,
            format!("failed to drain child stderr: {error}"),
        ),
        Ok(Err(error)) => record_cleanup_error(
            &mut cleanup_error,
            format!("child stderr task failed: {error}"),
        ),
        Err(_) => {
            stderr_task.abort();
            let _ = stderr_task.await;
            record_cleanup_error(
                &mut cleanup_error,
                "stderr remained open after child exit".to_owned(),
            );
        }
    }
    let stderr = stderr_capture
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .snapshot();
    CloseReport {
        exit_status,
        forced,
        timed_out,
        stderr,
        cleanup_error,
    }
}

fn record_cleanup_error(cleanup_error: &mut Option<String>, message: String) {
    if let Some(existing) = cleanup_error {
        existing.push_str("; ");
        existing.push_str(&message);
    } else {
        *cleanup_error = Some(message);
    }
}

#[derive(Clone, Debug)]
pub struct CloseReport {
    pub exit_status: Option<std::process::ExitStatus>,
    pub forced: bool,
    pub timed_out: bool,
    pub stderr: StderrSnapshot,
    pub cleanup_error: Option<String>,
}

impl CloseReport {
    pub fn clean_exit(&self) -> bool {
        self.exit_status.is_some_and(|status| status.success())
            && !self.forced
            && !self.timed_out
            && self.cleanup_error.is_none()
    }
}

const SAFE_INHERITED_ENV: &[&str] = &[
    "PATH",
    "PATHEXT",
    "HOME",
    "LOGNAME",
    "USER",
    "USERNAME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "TEMP",
    "TMP",
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "COMSPEC",
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "PROCESSOR_ARCHITECTURE",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROGRAMFILES(W6432)",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
];

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    fn outgoing(method: &str, len: usize) -> OutboundFrame {
        OutboundFrame {
            bytes: vec![b'x'; len],
            method: method.to_owned(),
            deadline: Instant::now() + Duration::from_secs(1),
            request_id: None,
            permit: None,
            completion: WriteCompletion::None,
        }
    }

    #[tokio::test]
    async fn loopback_reader_handles_fragments_crlf_and_blank_lines() {
        let (mut writer, reader) = duplex(64);
        let mut reader = FrameReader::new(reader, 128);
        for part in ["\n", "{\"a\":", "1}\r", "\n{\"b\":2}\n"] {
            writer.write_all(part.as_bytes()).await.unwrap();
            writer.flush().await.unwrap();
        }
        assert_eq!(
            reader.next_frame().await.unwrap(),
            Some(b"{\"a\":1}".to_vec())
        );
        assert_eq!(
            reader.next_frame().await.unwrap(),
            Some(b"{\"b\":2}".to_vec())
        );
        drop(writer);
        assert_eq!(reader.next_frame().await.unwrap(), None);
    }

    #[tokio::test]
    async fn loopback_reader_rejects_oversize_before_newline_without_overreading() {
        let (mut writer, reader) = duplex(64);
        let mut reader = FrameReader::new(reader, 32);
        let writer = tokio::spawn(async move {
            let _ = writer.write_all(&[b'x'; 33]).await;
        });
        let error = reader.next_frame().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds 32 bytes"));
        assert_eq!(reader.pending.len(), 32);
        drop(reader);
        let _ = writer.await;
    }

    #[tokio::test]
    async fn loopback_reader_accepts_exact_limit_and_rejects_limit_plus_newline() {
        let frame = b"{\"ok\":true}";
        let (mut writer, reader) = duplex(64);
        let mut reader = FrameReader::new(reader, frame.len() + 1);
        writer.write_all(frame).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        assert_eq!(reader.next_frame().await.unwrap(), Some(frame.to_vec()));

        let (mut writer, reader) = duplex(64);
        let mut reader = FrameReader::new(reader, frame.len());
        writer.write_all(frame).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        let error = reader.next_frame().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.pending.len(), frame.len());
    }

    #[tokio::test]
    async fn loopback_reader_rejects_incomplete_frame_at_eof() {
        let (mut writer, reader) = duplex(64);
        writer.write_all(b"{}").await.unwrap();
        drop(writer);
        let mut reader = FrameReader::new(reader, 32);
        let error = reader.next_frame().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn outgoing_frame_limit_includes_delimiter_and_counts_utf8_bytes() {
        let message = json!({"text":"é"});
        let mut exact = serde_json::to_vec(&message).unwrap();
        exact.push(b'\n');
        assert_eq!(
            encode_frame(message.clone(), exact.len(), Some("test")).unwrap(),
            exact
        );
        let error = encode_frame(message, exact.len() - 1, Some("test")).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert_eq!(error.method(), Some("test"));
    }

    #[tokio::test(start_paused = true)]
    async fn loopback_write_timeout_is_bounded() {
        let (mut writer, reader) = duplex(1);
        writer.write_all(b"x").await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let error = write_step(&mut writer, &outgoing("tools/call", 1), 0, deadline)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.method(), Some("tools/call"));
        drop(reader);
    }

    #[test]
    fn buffered_output_limit_retains_only_accepted_frames() {
        let budget = Arc::new(Semaphore::new(10));
        let mut queue = VecDeque::new();
        let mut buffered = 0;
        queue_frame(&mut queue, &mut buffered, outgoing("first", 6), 10, &budget).unwrap();
        let error = queue_frame(
            &mut queue,
            &mut buffered,
            outgoing("second", 5),
            10,
            &budget,
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Overloaded);
        assert_eq!(buffered, 6);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn cancelling_queued_request_releases_its_bytes() {
        let id = RequestId::String("request".to_owned());
        let budget = Arc::new(Semaphore::new(10));
        let mut queue = VecDeque::new();
        let mut buffered = 0;
        let mut frame = outgoing("tools/call", 6);
        frame.request_id = Some(id.clone());
        queue_frame(&mut queue, &mut buffered, frame, 10, &budget).unwrap();
        remove_queued_request(&mut queue, &mut buffered, &id);
        assert!(queue.is_empty());
        assert_eq!(buffered, 0);
        assert_eq!(budget.available_permits(), 10);
    }

    #[test]
    fn stderr_snapshot_stays_within_byte_limit_for_invalid_utf8() {
        let mut capture = StderrCapture::new(4);
        capture.push(&[0xff, 0xff, 0xff, 0xff, b'x']);
        let snapshot = capture.snapshot();
        assert!(snapshot.text.len() <= 4);
        assert!(snapshot.truncated);
    }

    #[test]
    fn protocol_rejects_ambiguous_response_and_routes_valid_error() {
        let options = ClientOptions::default();
        let id = RequestId::String("request".to_owned());
        let (response, _) = oneshot::channel();
        let mut pending = HashMap::from([(
            id.clone(),
            PendingRequest {
                method: "tools/call".to_owned(),
                response,
            },
        )]);
        let mut queue = VecDeque::new();
        let mut buffered = 0;
        let (notifications, _) = broadcast::channel(1);
        let error = handle_message(
            json!({"jsonrpc":"2.0","id":id.to_value(),"result":{},"error":{"code":-1,"message":"bad"}}),
            Era::Legacy,
            &mut pending,
            &mut queue,
            &mut buffered,
            &notifications,
            &options,
            &Arc::new(Semaphore::new(1024)),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert_eq!(pending.len(), 1);

        let (response, mut receiver) = oneshot::channel();
        pending.insert(
            id.clone(),
            PendingRequest {
                method: "tools/call".to_owned(),
                response,
            },
        );
        handle_message(
            json!({"jsonrpc":"2.0","id":id.to_value(),"error":{"code":-32602,"message":"invalid","data":{"x":1}}}),
            Era::Legacy,
            &mut pending,
            &mut queue,
            &mut buffered,
            &notifications,
            &options,
            &Arc::new(Semaphore::new(1024)),
        )
        .unwrap();
        let error = receiver.try_recv().unwrap().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Rpc);
        assert_eq!(error.method(), Some("tools/call"));
        assert_eq!(error.rpc_error().unwrap().data, Some(json!({"x":1})));
    }
}
