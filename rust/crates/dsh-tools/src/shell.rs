use crate::str_arg;
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_OUTPUT_CHARS: usize = 30_000;

/// The platform shell a `Shell` tool runs commands through.
#[derive(Clone, Copy)]
enum Program {
    /// POSIX `bash -c`.
    Bash,
    /// Windows PowerShell `-Command`.
    Pwsh,
}

/// One shell tool: its model-visible name and the interpreter behind it.
#[derive(Clone, Copy)]
struct Shell {
    name: &'static str,
    description: &'static str,
    command_description: &'static str,
    program: Program,
}

impl Shell {
    const fn bash() -> Self {
        Self {
            name: "bash",
            description:
                "Execute a shell command in the workspace and return its output and exit status.",
            command_description: "The bash command to execute.",
            program: Program::Bash,
        }
    }

    const fn pwsh() -> Self {
        Self {
            name: "pwsh",
            description: "Execute a PowerShell command in the workspace and return its output and exit status.",
            command_description: "The PowerShell command to execute.",
            program: Program::Pwsh,
        }
    }
}

/// `bash`: the POSIX shell tool of presets that run on macOS and Linux.
pub struct Bash;

/// `pwsh`: the Windows shell tool; the Standard preset enables it there instead of `bash`.
pub struct Pwsh;

impl Tool for Bash {
    fn def(&self) -> ToolDef {
        Shell::bash().def()
    }
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Shell::bash().run(args, cx)
    }
}

impl Tool for Pwsh {
    fn def(&self) -> ToolDef {
        Shell::pwsh().def()
    }
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Shell::pwsh().run(args, cx)
    }
}

fn on_path(name: &str) -> bool {
    let exts: &[&str] = if cfg!(windows) { &["", ".exe"] } else { &[""] };
    std::env::var_os("PATH").is_some_and(|p| {
        std::env::split_paths(&p)
            .any(|d| exts.iter().any(|e| d.join(format!("{name}{e}")).is_file()))
    })
}

/// Build the interpreter invocation for one command.
fn shell_command(program: Program, command: &str) -> Command {
    match program {
        Program::Pwsh => {
            let executable = if on_path("pwsh") {
                "pwsh"
            } else {
                "powershell"
            };
            let mut c = Command::new(executable);
            c.args(["-NoProfile", "-NonInteractive", "-Command", command]);
            c
        }
        Program::Bash if cfg!(windows) && !on_path("bash") => shell_command(Program::Pwsh, command),
        Program::Bash => {
            let mut c = Command::new("bash");
            c.args(["-c", command]);
            c
        }
    }
}

const CAPTURE_BYTES: usize = MAX_OUTPUT_CHARS * 4;
const TEARDOWN_GRACE: Duration = Duration::from_secs(2);

#[derive(Default)]
struct Capture {
    bytes: Vec<u8>,
    seen: usize,
}

impl Capture {
    fn push(&mut self, chunk: &[u8]) {
        self.seen += chunk.len();
        let remaining = CAPTURE_BYTES.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }

    fn text(&self) -> (String, bool) {
        (
            String::from_utf8_lossy(&self.bytes).into_owned(),
            self.seen > self.bytes.len(),
        )
    }
}

async fn capture<R: AsyncRead + Unpin>(
    mut reader: R,
    output: Arc<Mutex<Capture>>,
) -> std::io::Result<()> {
    let mut buffer = [0u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(count) => output
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(&buffer[..count]),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn stop_readers(
    stdout_task: &mut Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    stderr_task: &mut Option<tokio::task::JoinHandle<std::io::Result<()>>>,
) {
    let stdout = stdout_task.take();
    let stderr = stderr_task.take();
    if let Some(task) = &stdout {
        task.abort();
    }
    if let Some(task) = &stderr {
        task.abort();
    }
    let _ = tokio::time::timeout(TEARDOWN_GRACE, async {
        if let Some(task) = stdout {
            let _ = task.await;
        }
        if let Some(task) = stderr {
            let _ = task.await;
        }
    })
    .await;
}

fn sensitive_environment(name: &std::ffi::OsStr) -> bool {
    let upper = name.to_string_lossy().to_uppercase();
    ["KEY", "SECRET", "TOKEN", "PASSWORD"]
        .iter()
        .any(|marker| upper.contains(marker))
}

fn scrub_environment(command: &mut Command) {
    command.env_clear();
    for (name, value) in std::env::vars_os() {
        if sensitive_environment(&name) {
            continue;
        }
        command.env(name, value);
    }
}

fn bounded_body(text: String, already_truncated: bool, reserve: usize) -> (String, bool) {
    let budget = MAX_OUTPUT_CHARS.saturating_sub(reserve);
    let count = text.chars().count();
    if count <= budget && !already_truncated {
        return (text, false);
    }
    let footer = "\n... (output truncated)";
    let keep = budget.saturating_sub(footer.chars().count());
    (
        format!("{}{footer}", text.chars().take(keep).collect::<String>()),
        true,
    )
}

impl Shell {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: self.name.into(),
            description: self.description.into(),
            input_schema: json!({"type":"object","properties":{
                "description":{"type":"string","description":"Clear, concise description of what this command does in active voice, 5-10 words (shown in the UI)."},
                "command":{"type":"string","description":self.command_description},
                "timeoutMs":{"type":"number","description":"Timeout in milliseconds."}
            },"required":["description","command"]}),
        }
    }
    fn run<'a>(self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            let command = match str_arg(&args, "command") {
                Ok(c) => c,
                Err(e) => return ToolOutput::err(e),
            };
            let timeout = Duration::from_millis(
                args.get("timeoutMs")
                    .and_then(Value::as_u64)
                    .unwrap_or(DEFAULT_TIMEOUT_MS),
            );
            let mut builder = shell_command(self.program, command);
            scrub_environment(&mut builder);
            let mut child = match builder
                .current_dir(&cx.cwd)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
            {
                Ok(child) => child,
                Err(error) => {
                    return ToolOutput::err(format!("Error: failed to start shell: {error}"))
                }
            };
            let stdout = Arc::new(Mutex::new(Capture::default()));
            let stderr = Arc::new(Mutex::new(Capture::default()));
            let mut stdout_task = Some(tokio::spawn(capture(
                child.stdout.take().unwrap(),
                stdout.clone(),
            )));
            let mut stderr_task = Some(tokio::spawn(capture(
                child.stderr.take().unwrap(),
                stderr.clone(),
            )));
            let waited = tokio::select! {
                biased;
                _ = cx.cancel.cancelled() => {
                    let _ = child.start_kill();
                    let _ = tokio::time::timeout(TEARDOWN_GRACE, child.wait()).await;
                    stop_readers(&mut stdout_task, &mut stderr_task).await;
                    return ToolOutput::err("Error: command cancelled");
                }
                result = tokio::time::timeout(timeout, child.wait()) => result,
            };
            let status = match waited {
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = tokio::time::timeout(TEARDOWN_GRACE, child.wait()).await;
                    stop_readers(&mut stdout_task, &mut stderr_task).await;
                    return ToolOutput::err(format!(
                        "Error: command timed out after {} ms",
                        timeout.as_millis()
                    ));
                }
                Ok(Err(error)) => {
                    stop_readers(&mut stdout_task, &mut stderr_task).await;
                    return ToolOutput::err(format!("Error: command failed: {error}"));
                }
                Ok(Ok(status)) => status,
            };
            let collect = async {
                let (mut out_done, mut err_done) = (false, false);
                let mut capture_failed = false;
                while !out_done || !err_done {
                    tokio::select! {
                        result = stdout_task.as_mut().unwrap(), if !out_done => {
                            out_done = true;
                            capture_failed |= !matches!(result, Ok(Ok(())));
                        }
                        result = stderr_task.as_mut().unwrap(), if !err_done => {
                            err_done = true;
                            capture_failed |= !matches!(result, Ok(Ok(())));
                        }
                    }
                }
                capture_failed
            };
            match tokio::time::timeout(TEARDOWN_GRACE, collect).await {
                Ok(true) => {
                    return ToolOutput::err(
                        "Error: command output capture failed; captured output is incomplete",
                    )
                }
                Err(_) => {
                    stop_readers(&mut stdout_task, &mut stderr_task).await;
                    return ToolOutput::err("Error: command output did not close after shell exit; captured output is incomplete");
                }
                Ok(false) => {}
            }
            let (out, out_truncated) = stdout
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .text();
            let (err, err_truncated) = stderr
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .text();
            let mut text = out;
            if !err.is_empty() {
                if !text.is_empty() && !text.ends_with('\n') {
                    text.push('\n');
                }
                text.push_str(&err);
            }
            match status.code() {
                Some(0) => {
                    let (text, _) = bounded_body(text, out_truncated || err_truncated, 0);
                    ToolOutput::ok(text)
                }
                code => {
                    let status = code.map_or("terminated by signal".to_string(), |code| {
                        format!("exit code {code}")
                    });
                    let suffix = format!("\nCommand failed: {status}");
                    let (mut text, _) =
                        bounded_body(text, out_truncated || err_truncated, suffix.chars().count());
                    if text.is_empty() {
                        text = format!("Command failed: {status}");
                    } else {
                        text.push_str(&suffix);
                    }
                    ToolOutput::err(text)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_core::Cancel;

    fn cx() -> ToolContext {
        ToolContext {
            cwd: std::env::temp_dir(),
            cancel: Cancel::default(),
            ..ToolContext::default()
        }
    }

    #[tokio::test]
    async fn runs_and_reports_status() {
        let ok = Bash.call(json!({"command":"echo hello"}), &cx()).await;
        assert!(!ok.is_error && ok.text.contains("hello"));
        let bad = Bash.call(json!({"command":"exit 3"}), &cx()).await;
        assert!(bad.is_error && bad.text.contains("exit code 3"));
    }

    #[tokio::test]
    async fn output_is_bounded_while_the_producer_drains() {
        // The tool prefers bash whenever it is on PATH, so the Windows branch follows the
        // same choice instead of assuming PowerShell.
        #[cfg(windows)]
        let command = if on_path("bash") {
            "printf 'x%.0s' {1..200000}"
        } else {
            "'x' * 200000"
        };
        #[cfg(not(windows))]
        let command = "yes x | head -c 200000";
        let output = Bash
            .call(json!({"command":command,"timeoutMs":10000}), &cx())
            .await;
        assert!(!output.is_error, "{}", output.text);
        assert!(output.text.chars().count() <= MAX_OUTPUT_CHARS);
        assert!(output.text.contains("output truncated"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn the_windows_shell_tool_runs_a_powershell_command() {
        assert_eq!(Pwsh.def().name, "pwsh");
        let ok = Pwsh
            .call(json!({"command":"Write-Output hello"}), &cx())
            .await;
        assert!(!ok.is_error, "{}", ok.text);
        assert!(ok.text.contains("hello"), "{}", ok.text);
        let bad = Pwsh.call(json!({"command":"exit 3"}), &cx()).await;
        assert!(bad.is_error, "{}", bad.text);
        assert!(bad.text.contains("exit code 3"), "{}", bad.text);
    }

    #[test]
    fn credential_environment_names_are_scrubbed() {
        for name in ["API_KEY", "CLIENT_SECRET", "AUTH_TOKEN", "DB_PASSWORD"] {
            assert!(sensitive_environment(std::ffi::OsStr::new(name)));
        }
        assert!(!sensitive_environment(std::ffi::OsStr::new("PATH")));
        assert!(!sensitive_environment(std::ffi::OsStr::new("HOME")));
    }

    #[tokio::test]
    async fn timeout_and_cancel_stop_the_command() {
        let t = Bash
            .call(json!({"command":"sleep 30","timeoutMs":200}), &cx())
            .await;
        assert!(t.is_error && t.text.contains("timed out"));
        let c = cx();
        let c2 = c.cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            c2.cancel();
        });
        let r = Bash.call(json!({"command":"sleep 30"}), &c).await;
        assert!(r.is_error && r.text.contains("cancelled"));
    }
}
