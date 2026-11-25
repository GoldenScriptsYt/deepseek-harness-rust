//! Supervises the `dsh web` Host sidecar: launch, readiness URL, shutdown.

use std::io::{BufRead, BufReader};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[cfg(windows)]
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
    SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};

/// The Host prints this prefix once its listener is bound and authenticated.
const READY_PREFIX: &str = "dsh web: ";

/// A cold `dsh web` boot compiles TypeScript through tsx before listening.
const BOOT_TIMEOUT: Duration = Duration::from_secs(180);

/// A running Host child process and the token URL it published.
pub struct HostProcess {
    child: Child,
    /// Closing the app handle kills the Host and all descendants, even on a crash.
    #[cfg(windows)]
    job: Option<OwnedHandle>,
    exited: bool,
    /// Ready URL including the per-launch credential.
    pub url: String,
}

/// Replace the credential in a log line so it never reaches a terminal or log file.
fn redact(line: &str) -> String {
    match line.split_once("token=") {
        Some((head, _)) => format!("{head}token=<redacted>"),
        None => line.to_string(),
    }
}

impl HostProcess {
    /// Spawn a Host from `repo_root` and block until it reports a ready URL.
    ///
    /// A named profile uses the legacy TypeScript Host so its Cordis bundles can
    /// run. With no profile, retain the Web profile compatibility behavior.
    pub fn spawn(repo_root: &Path, profile: Option<&str>) -> Result<Self, String> {
        let patch = std::env::var("DSH_NODE_PATCH").ok();
        let session_patch = write_plugin_session_overlay()?;
        let args = host_args(profile, patch.as_deref(), &session_patch)?;
        let mut child = Command::new("node")
            .args(args)
            .current_dir(repo_root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("failed to launch the dsh Host: {error}"))?;

        #[cfg(windows)]
        let job = match create_kill_on_close_job(&child) {
            Ok(job) => Some(job),
            Err(error) => {
                eprintln!("[dsh-host] Windows process job unavailable; using normal shutdown cleanup: {error}");
                None
            }
        };

        let Some(stdout) = child.stdout.take() else {
            terminate_child(&mut child);
            return Err("the dsh Host has no stdout pipe".to_string());
        };
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut reported = false;
            // Keep draining after readiness: closing the pipe would give the Host an
            // EPIPE on its next write and abort a healthy session.
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                eprintln!("[dsh-host] {}", redact(&line));
                if reported {
                    continue;
                }
                if let Some(url) = line.strip_prefix(READY_PREFIX) {
                    if sender.send(url.trim().to_string()).is_ok() {
                        reported = true;
                    }
                }
            }
        });

        if let Some(stderr) = child.stderr.take() {
            thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    eprintln!("[dsh-host] {}", redact(&line));
                }
            });
        }

        match receiver.recv_timeout(BOOT_TIMEOUT) {
            Ok(url) => Ok(Self {
                child,
                #[cfg(windows)]
                job,
                exited: false,
                url,
            }),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let mut failed = Self {
                    child,
                    #[cfg(windows)]
                    job,
                    exited: false,
                    url: String::new(),
                };
                failed.shutdown();
                Err(format!(
                    "the dsh Host did not report a ready URL within {}s",
                    BOOT_TIMEOUT.as_secs()
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let mut failed = Self {
                    child,
                    #[cfg(windows)]
                    job,
                    exited: false,
                    url: String::new(),
                };
                failed.shutdown();
                Err("the dsh Host exited before reporting a ready URL".to_string())
            }
        }
    }

    /// Stop the Host and everything it spawned.
    pub fn shutdown(&mut self) {
        if self.exited {
            return;
        }
        terminate_child(&mut self.child);
        #[cfg(windows)]
        drop(self.job.take());
        self.exited = true;
    }
}

/// Place the Node Host in a kill-on-close job so crashes cannot leave its listener orphaned.
#[cfg(windows)]
fn create_kill_on_close_job(child: &Child) -> std::io::Result<OwnedHandle> {
    let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if raw.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: CreateJobObjectW returned a valid handle owned by this function.
    let job = unsafe { OwnedHandle::from_raw_handle(raw) };
    // SAFETY: The Windows API structure accepts a zero-initialized limit record.
    let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let configured = unsafe {
        SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&raw const limits).cast(),
            std::mem::size_of_val(&limits) as u32,
        )
    };
    if configured == 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { AssignProcessToJobObject(job.as_raw_handle(), child.as_raw_handle()) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(job)
}

fn host_args(
    profile: Option<&str>,
    patch: Option<&str>,
    session_patch: &Path,
) -> Result<Vec<String>, String> {
    let mut args = vec![
        "--import".to_owned(),
        "tsx/esm".to_owned(),
        "apps/cli/src/bin.ts".to_owned(),
    ];
    match profile {
        Some(profile) if valid_profile_name(profile) => {
            args.extend(["--profile".to_owned(), profile.to_owned()]);
        }
        Some(_) => {
            return Err("DSH_NODE_PROFILE must be one profile name without path separators".into());
        }
        None => args.push("web".to_owned()),
    }
    if let Some(path) = patch {
        if path.is_empty() {
            return Err("DSH_NODE_PATCH must name a patch file".into());
        }
        args.extend(["--patch".to_owned(), path.to_owned()]);
    }
    args.extend([
        "--patch".to_owned(),
        session_patch.to_string_lossy().into_owned(),
    ]);
    // The compatibility Host can run beside another Harness instance. A
    // fixed 3080 bind leaves its WebServer inactive on EADDRINUSE; dependent
    // plugins such as the directory picker then stay pending while the UI may
    // connect to the other process. Keep this app flag after launcher flags,
    // then let this launch own an OS-assigned port.
    args.extend(["--port".to_owned(), "0".to_owned()]);
    args.push("--no-open".to_owned());
    Ok(args)
}

/// Keep the legacy Cordis Host away from the Rust Host's session files.
///
/// The two runtimes use different physical session encodings. This is the
/// separate session folder selected for plugin mode, applied as the last
/// profile overlay so even a user plugin patch cannot point it back at the
/// Rust Host's root.
fn write_plugin_session_overlay() -> Result<PathBuf, String> {
    let home = std::path::absolute(dsh_cli::web::default_home())
        .map_err(|error| format!("failed to resolve the DSH home for plugin sessions: {error}"))?;
    let session_root = home.join("plugin-sessions");
    std::fs::create_dir_all(&session_root)
        .map_err(|error| format!("failed to create the plugin session folder: {error}"))?;

    let root = session_root
        .to_str()
        .ok_or_else(|| "the plugin session folder path is not valid Unicode".to_owned())?;
    let root = serde_json::to_string(root)
        .map_err(|error| format!("failed to encode the plugin session folder path: {error}"))?;
    // This is a native desktop window even though the compatibility runtime
    // uses the Web profile for its Cordis UI. Match the account API identity
    // used by the native desktop client instead of sending `web` requests.
    let desktop_platform = match std::env::consts::OS {
        "windows" => "win32",
        "macos" => "darwin",
        _ => "null",
    };
    let contents = format!(
        "- id: deepseek-account\n  config:\n    desktopPlatform: {desktop_platform}\n\
         - id: session-persistence-jsonl\n  config:\n    root: {root}\n"
    );
    let overlay = home.join("plugin-sessions.patch.yml");
    std::fs::write(&overlay, contents)
        .map_err(|error| format!("failed to write the plugin session overlay: {error}"))?;
    Ok(overlay)
}

fn valid_profile_name(profile: &str) -> bool {
    use std::path::Component;

    if profile.is_empty()
        || profile
            .chars()
            .any(|character| matches!(character, '/' | '\\'))
    {
        return false;
    }
    let mut components = Path::new(profile).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

/// Terminate a child and reap it, killing its full tree on Windows.
fn terminate_child(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    #[cfg(windows)]
    // The Host may own agent subprocesses; /T takes the whole tree.
    let killed = Command::new("taskkill")
        .args(["/PID", &child.id().to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    #[cfg(not(windows))]
    let killed = child.kill().is_ok();

    if !killed {
        let _ = child.kill();
    }
    let _ = child.wait();
}

impl Drop for HostProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::{host_args, redact, HostProcess, READY_PREFIX};
    use ::dsh_cli;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::process::Command;
    use std::time::Duration;

    struct TestGateway {
        serve: Option<tauri::async_runtime::JoinHandle<std::io::Result<()>>>,
        entry_url: String,
        port: u16,
        _resources: tempfile::TempDir,
    }

    impl TestGateway {
        async fn start() -> Self {
            let resources = tempfile::tempdir().expect("temporary web resources");
            let dist = resources.path().join("apps/web/dist");
            std::fs::create_dir_all(&dist).expect("web dist directory");
            std::fs::write(
                dist.join("index.html"),
                "<!doctype html><title>Host test</title>",
            )
            .expect("web shell fixture");
            let started = dsh_cli::web::start(resources.path(), resources.path(), 0)
                .await
                .expect("gateway start");
            let url = tauri::Url::parse(&started.entry_url).expect("gateway URL");
            let port = url.port().expect("ephemeral gateway port");
            Self {
                serve: Some(tauri::async_runtime::spawn(started.serve)),
                entry_url: started.entry_url,
                port,
                _resources: resources,
            }
        }

        fn request(&self, target: &str) -> (u16, Vec<(String, String)>, Vec<u8>) {
            let mut stream = TcpStream::connect(("127.0.0.1", self.port)).expect("gateway connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .expect("write timeout");
            write!(
                stream,
                "GET {target} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
                self.port
            )
            .expect("gateway request");
            let mut response = Vec::new();
            stream.read_to_end(&mut response).expect("gateway response");
            let header_end = response
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
                .expect("response headers");
            let header_text =
                std::str::from_utf8(&response[..header_end]).expect("header encoding");
            let mut lines = header_text.split("\r\n");
            let status = lines
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|status| status.parse().ok())
                .expect("HTTP status");
            let headers = lines
                .map(|line| {
                    let (name, value) = line.split_once(':').expect("response header");
                    (name.to_ascii_lowercase(), value.trim().to_string())
                })
                .collect();
            (status, headers, response[header_end + 4..].to_vec())
        }

        async fn shutdown(&mut self) {
            if let Some(serve) = self.serve.take() {
                serve.abort();
                let _ = serve.await;
            }
        }
    }

    #[test]
    fn gateway_entry_url_is_loopback_with_one_ephemeral_token() {
        let mut gateway = tauri::async_runtime::block_on(TestGateway::start());
        let url = tauri::Url::parse(&gateway.entry_url).expect("entry URL");
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_ne!(url.port(), Some(0));
        assert_eq!(url.path(), "/");
        assert_eq!(url.username(), "");
        assert!(url.password().is_none());
        assert!(url.fragment().is_none());
        let parameters: Vec<_> = url.query_pairs().collect();
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters[0].0, "token");
        let token = parameters[0].1.as_bytes();
        assert_eq!(token.len(), 48);
        assert!(token
            .iter()
            .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(byte) }));
        tauri::async_runtime::block_on(gateway.shutdown());
    }

    #[test]
    fn gateway_authenticates_with_an_http_only_strict_cookie() {
        let mut gateway = tauri::async_runtime::block_on(TestGateway::start());
        let token = tauri::Url::parse(&gateway.entry_url)
            .expect("entry URL")
            .query_pairs()
            .find(|(name, _)| name == "token")
            .expect("launch token")
            .1
            .into_owned();
        let (status, headers, body) = gateway.request(&format!("/?token={token}"));
        assert_eq!(status, 303);
        assert!(headers
            .iter()
            .any(|(name, value)| { name == "location" && value == "./" }));
        let cookie = headers
            .iter()
            .find(|(name, _)| name == "set-cookie")
            .map(|(_, value)| value.as_str())
            .expect("authentication cookie");
        assert!(cookie.starts_with("dsh-auth-rs="));
        let lowercase = cookie.to_ascii_lowercase();
        assert!(lowercase.contains("httponly"));
        assert!(lowercase.contains("samesite=strict"));
        assert!(!body
            .windows(token.len())
            .any(|window| window == token.as_bytes()));

        let (status, _, _) = gateway.request("/");
        assert_eq!(status, 401);
        tauri::async_runtime::block_on(gateway.shutdown());
    }

    fn spawn_long_running_child() -> HostProcess {
        #[cfg(windows)]
        let child = Command::new("cmd")
            .args(["/D", "/Q", "/C", "ping -t 127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("long-running child");
        #[cfg(not(windows))]
        let child = Command::new("sleep")
            .arg("60")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("long-running child");
        HostProcess {
            child,
            #[cfg(windows)]
            job: None,
            exited: false,
            url: String::new(),
        }
    }

    #[test]
    fn host_shutdown_terminates_and_reaps_the_child_once() {
        let mut host = spawn_long_running_child();
        let pid = host.child.id();
        host.shutdown();
        assert!(host.exited);
        assert!(matches!(host.child.try_wait(), Ok(Some(_))));
        // A second call must not target a recycled PID.
        host.shutdown();
        assert_ne!(pid, 0);
    }

    #[test]
    fn host_drop_terminates_the_child() {
        let host = spawn_long_running_child();
        let pid = host.child.id();
        drop(host);
        #[cfg(windows)]
        {
            let output = Command::new("tasklist")
                .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
                .output()
                .expect("tasklist");
            let listing = String::from_utf8_lossy(&output.stdout);
            assert!(!listing.contains(&format!("\"{pid}\"")));
        }
        #[cfg(not(windows))]
        let _ = pid;
    }

    #[test]
    fn redacts_host_tokens_without_touching_other_output() {
        assert_eq!(
            redact(&format!(
                "{READY_PREFIX}http://127.0.0.1:3000/?token=secret"
            )),
            "dsh web: http://127.0.0.1:3000/?token=<redacted>"
        );
        assert_eq!(redact("ordinary Host output"), "ordinary Host output");
    }

    #[test]
    fn redaction_removes_everything_after_the_credential() {
        let output = redact("token=secret&other=also-secret");
        assert_eq!(output, "token=<redacted>");
        assert!(!output.contains("secret"));
    }

    #[test]
    fn host_args_select_a_named_profile_without_shell_interpolation() {
        assert_eq!(
            host_args(
                Some("superagent"),
                None,
                Path::new("C:\\Example\\plugin-sessions.patch.yml")
            )
            .unwrap(),
            [
                "--import",
                "tsx/esm",
                "apps/cli/src/bin.ts",
                "--profile",
                "superagent",
                "--patch",
                "C:\\Example\\plugin-sessions.patch.yml",
                "--port",
                "0",
                "--no-open",
            ]
        );
        assert_eq!(
            host_args(
                None,
                None,
                Path::new("C:\\Example\\plugin-sessions.patch.yml")
            )
            .unwrap(),
            [
                "--import",
                "tsx/esm",
                "apps/cli/src/bin.ts",
                "web",
                "--patch",
                "C:\\Example\\plugin-sessions.patch.yml",
                "--port",
                "0",
                "--no-open",
            ]
        );
    }

    #[test]
    fn host_args_reject_profile_path_components() {
        for profile in ["", ".", "..", "../web", "web\\..\\superagent"] {
            assert!(
                host_args(Some(profile), None, Path::new("plugin-sessions.patch.yml")).is_err(),
                "{profile:?}"
            );
        }
    }

    #[test]
    fn host_args_pass_a_patch_as_one_argument() {
        let args = host_args(
            Some("web"),
            Some("C:\\Example folder\\patch.yml"),
            Path::new("C:\\Example folder\\plugin-sessions.patch.yml"),
        )
        .unwrap();
        assert_eq!(
            &args[5..],
            [
                "--patch",
                "C:\\Example folder\\patch.yml",
                "--patch",
                "C:\\Example folder\\plugin-sessions.patch.yml",
                "--port",
                "0",
                "--no-open"
            ]
        );
        assert!(host_args(
            Some("web"),
            Some(""),
            Path::new("plugin-sessions.patch.yml")
        )
        .is_err());
    }
}
