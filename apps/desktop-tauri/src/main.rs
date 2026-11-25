//! Native desktop application: one WebView2 window over the in-process Rust Host.
//!
//! The Host (`dsh-cli`'s web gateway) runs on this process's async runtime on a loopback
//! port; no Node process is started. Application exit aborts and awaits the gateway task.
//! Setting `DSH_NODE_HOST` selects the TypeScript Host compatibility sidecar. Setting
//! `DSH_NODE_PROFILE` also selects it and boots that named profile (needs a checkout and Node),
//! which lets existing Cordis bundles run inside the Rust desktop carrier.

// A release build is a GUI program: no console window.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod deep_link;
mod host;
mod updater;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tauri::menu::{Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{
    AppHandle, Listener, Manager, RunEvent, WebviewUrl, WebviewWindow, WebviewWindowBuilder,
};
use tauri_plugin_deep_link::DeepLink;
use tauri_plugin_updater::UpdaterExt;

use deep_link::Action;
use host::HostProcess;

const MAIN_WINDOW: &str = "main";
const DEEP_LINK_EVENT: &str = "deep-link://new-url";
const TRAY_OPEN: &str = "tray-open";
const TRAY_CHECK_UPDATES: &str = "tray-check-updates";
const TRAY_QUIT: &str = "tray-quit";

/// Owns the in-process gateway task and closes the spawn-versus-exit race.
struct EmbeddedHostState {
    slot: Mutex<Option<tauri::async_runtime::JoinHandle<()>>>,
    exiting: AtomicBool,
}

impl EmbeddedHostState {
    fn new() -> Self {
        Self {
            slot: Mutex::new(None),
            exiting: AtomicBool::new(false),
        }
    }

    /// Spawn and install the gateway while shutdown is excluded from the slot.
    fn spawn<F>(&self, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut slot = self.slot.lock().expect("embedded host slot poisoned");
        if self.exiting.load(Ordering::Acquire) {
            return;
        }
        *slot = Some(tauri::async_runtime::spawn(task));
    }

    fn shutdown(&self) {
        self.exiting.store(true, Ordering::Release);
        let task = self
            .slot
            .lock()
            .expect("embedded host slot poisoned")
            .take();
        if let Some(task) = task {
            task.abort();
            // Cancellation is the normal Exit path; awaiting it releases the listener.
            let _ = tauri::async_runtime::block_on(task);
        }
    }
}

/// Owns the legacy Node sidecar and closes the spawn-versus-exit race.
struct NodeHostState {
    slot: Mutex<Option<HostProcess>>,
    exiting: AtomicBool,
}

impl NodeHostState {
    fn new() -> Self {
        Self {
            slot: Mutex::new(None),
            exiting: AtomicBool::new(false),
        }
    }

    /// Store a spawned Host unless application shutdown has already begun.
    fn install(&self, process: HostProcess) -> bool {
        let mut slot = self.slot.lock().expect("host slot poisoned");
        if self.exiting.load(Ordering::Acquire) {
            false
        } else {
            *slot = Some(process);
            true
        }
    }

    fn shutdown(&self) {
        self.exiting.store(true, Ordering::Release);
        if let Some(mut process) = self.slot.lock().expect("host slot poisoned").take() {
            process.shutdown();
        }
    }
}

/// Deep links received before the Host publishes its URL wait here.
#[derive(Default)]
struct DeepLinkState {
    ready: AtomicBool,
    pending: Mutex<Vec<tauri::Url>>,
}

/// Locate the checkout that owns `apps/cli/src/bin.ts` (Node fallback only).
fn repo_root() -> Result<PathBuf, String> {
    if let Some(explicit) = std::env::var_os("DSH_REPO_ROOT") {
        return Ok(PathBuf::from(explicit));
    }
    let mut dir = std::env::current_dir().map_err(|error| error.to_string())?;
    loop {
        if dir.join("apps/cli/src/bin.ts").is_file() {
            return Ok(dir);
        }
        if !dir.pop() {
            return Err(
                "could not locate the dsh checkout: apps/cli/src/bin.ts is not an ancestor of the current directory"
                    .to_string(),
            );
        }
    }
}

/// Render a message in the window instead of leaving the loading page up.
fn show_message(window: &WebviewWindow, heading: &str, message: &str) {
    eprintln!("desktop-tauri: {heading}: {message}");
    // Pass values as JSON string literals and assign them through textContent so
    // Host output can never be executed as script.
    let heading = serde_json::to_string(heading).unwrap_or_else(|_| "\"\"".to_string());
    let literal = serde_json::to_string(message).unwrap_or_else(|_| "\"\"".to_string());
    let script = format!(
        "document.body.replaceChildren();\
         const heading = document.createElement('pre');\
         heading.textContent = {heading};\
         heading.style.cssText = 'font:600 13px ui-monospace,monospace;padding:2rem 2rem 0;white-space:pre-wrap;color:#c0392b';\
         const detail = document.createElement('pre');\
         detail.textContent = {literal};\
         detail.style.cssText = 'font:13px ui-monospace,monospace;padding:0.75rem 2rem;white-space:pre-wrap';\
         document.body.append(heading, detail);"
    );
    let _ = window.eval(&script);
}

/// Render a boot failure in the window.
fn show_failure(window: &WebviewWindow, message: &str) {
    show_message(window, "Host boot failed", message);
}

fn open(window: &WebviewWindow, target: &str) {
    match target.parse::<tauri::Url>() {
        Ok(url) => {
            if window.navigate(url).is_err() {
                show_failure(
                    window,
                    "could not open the local Host; restart DeepSeek Harness",
                );
            }
        }
        Err(_) => show_failure(window, "the local Host supplied an invalid URL"),
    }
}

fn focus_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window(MAIN_WINDOW) {
        let _ = window.unminimize();
        let _ = window.show();
        let _ = window.set_focus();
    }
}

/// Handle future plugin events as well as second-instance arguments forwarded by it.
fn install_deep_link_listener(app: &AppHandle) {
    let handle = app.clone();
    app.listen(DEEP_LINK_EVENT, move |event| {
        match serde_json::from_str::<Vec<tauri::Url>>(event.payload()) {
            Ok(urls) => dispatch_deep_links(&handle, urls),
            Err(error) => eprintln!("desktop-tauri: invalid deep-link event payload: {error}"),
        }
    });
}

fn dispatch_deep_links(app: &AppHandle, urls: Vec<tauri::Url>) {
    focus_window(app);
    let Some(window) = app.get_webview_window(MAIN_WINDOW) else {
        eprintln!("desktop-tauri: cannot handle a deep link before the main window exists");
        return;
    };
    let state = app.state::<Arc<DeepLinkState>>().inner().clone();
    for url in urls {
        match deep_link::parse(&url) {
            Ok(Action::Open) => {}
            Ok(Action::Navigate(target)) => {
                let mut pending = state
                    .pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if state.ready.load(Ordering::Acquire) {
                    open(&window, target.as_str());
                } else {
                    pending.push(target);
                }
            }
            Err(error) => eprintln!("desktop-tauri: rejected deep link: {error}"),
        }
    }
}

/// Publish the Host URL, then release queued deep links in arrival order.
fn mark_host_ready(window: &WebviewWindow, state: &DeepLinkState) {
    let mut pending = state
        .pending
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    for target in pending.drain(..) {
        open(window, target.as_str());
    }
    state.ready.store(true, Ordering::Release);
}

fn updater_configured(app: &AppHandle) -> bool {
    app.config()
        .plugins
        .0
        .get("updater")
        .is_some_and(updater::configured)
}

/// Check only. Portable packaging cannot install Tauri's installer-based updates yet.
fn check_for_updates(app: &AppHandle, menu_item: MenuItem<tauri::Wry>) {
    let Some(window) = app.get_webview_window(MAIN_WINDOW) else {
        let _ = menu_item.set_enabled(true);
        return;
    };
    focus_window(app);
    if !updater_configured(app) {
        show_message(
            &window,
            "Updates unavailable",
            "No update endpoint and verification key are configured for this portable build.",
        );
        let _ = menu_item.set_enabled(true);
        return;
    }

    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let result = match handle.updater() {
            Ok(updater) => match updater.check().await {
                Ok(None) => Ok("DeepSeek Harness is up to date.".to_string()),
                Ok(Some(update)) => Ok(format!(
                    "Version {} is available. Automatic installation is not enabled for this portable build.",
                    update.version
                )),
                Err(error) => Err(format!("could not check for updates: {error}")),
            },
            Err(error) => Err(format!("could not initialize the updater: {error}")),
        };
        match result {
            Ok(message) => show_message(&window, "DeepSeek Harness update", &message),
            Err(error) => show_failure(&window, &error),
        }
        let _ = menu_item.set_enabled(true);
    });
}

fn setup_tray(app: &AppHandle) -> tauri::Result<()> {
    let open_item = MenuItem::with_id(app, TRAY_OPEN, "Open DeepSeek Harness", true, None::<&str>)?;
    let update_item = MenuItem::with_id(
        app,
        TRAY_CHECK_UPDATES,
        "Check for updates",
        updater_configured(app),
        None::<&str>,
    )?;
    let quit_item = MenuItem::with_id(app, TRAY_QUIT, "Quit", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&open_item, &update_item, &quit_item])?;
    let event_update_item = update_item.clone();
    let tray_handle = app.clone();

    let tray = TrayIconBuilder::with_id("deepseek-harness")
        .icon(tauri::include_image!("icons/icon.png"))
        .tooltip("DeepSeek Harness")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(move |app, event| match event.id().as_ref() {
            TRAY_OPEN => focus_window(app),
            TRAY_CHECK_UPDATES => {
                let _ = event_update_item.set_enabled(false);
                check_for_updates(app, event_update_item.clone());
            }
            TRAY_QUIT => app.exit(0),
            other => eprintln!("desktop-tauri: unknown tray menu action: {other}"),
        })
        .on_tray_icon_event(move |_tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                focus_window(&tray_handle);
            }
        })
        .build(app)?;

    // Dropping a TrayIcon removes it, so application state owns its lifetime.
    app.manage(tray);
    Ok(())
}

/// Run the Rust Host on the app's async runtime and point the window at it.
fn start_embedded(
    window: WebviewWindow,
    deep_link_state: Arc<DeepLinkState>,
    host_state: Arc<EmbeddedHostState>,
) {
    host_state.spawn(async move {
        let Some(resources) = dsh_cli::web::locate_resources() else {
            show_failure(
                &window,
                "cannot find the web client: expected a `resources` folder beside the executable (or set DSH_REPO_ROOT)",
            );
            return;
        };
        let home = dsh_cli::web::default_home();
        match dsh_cli::web::start(&resources, &home, 0).await {
            Ok(started) => {
                open(&window, &started.entry_url);
                mark_host_ready(&window, &deep_link_state);
                if let Err(error) = started.serve.await {
                    show_failure(&window, &format!("the Host stopped: {error}"));
                }
            }
            Err(error) => show_failure(&window, &format!("the Host failed to start: {error}")),
        }
    });
}

fn start_node(
    window: WebviewWindow,
    host_state: Arc<NodeHostState>,
    deep_link_state: Arc<DeepLinkState>,
) {
    let root = match repo_root() {
        Ok(root) => root,
        Err(error) => {
            show_failure(&window, &error);
            return;
        }
    };
    let profile = match std::env::var("DSH_NODE_PROFILE") {
        Ok(profile) => Some(profile),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => {
            show_failure(
                &window,
                &format!("DSH_NODE_PROFILE is not valid Unicode: {error}"),
            );
            return;
        }
    };
    std::thread::spawn(
        move || match HostProcess::spawn(&root, profile.as_deref()) {
            Ok(process) => {
                let target = process.url.clone();
                if host_state.install(process) {
                    open(&window, &target);
                    mark_host_ready(&window, &deep_link_state);
                }
            }
            Err(error) => show_failure(&window, &error),
        },
    );
}

fn main() {
    let node_state = Arc::new(NodeHostState::new());
    let boot_state = Arc::clone(&node_state);
    let exit_state = Arc::clone(&node_state);
    let embedded_state = Arc::new(EmbeddedHostState::new());
    let embedded_exit_state = Arc::clone(&embedded_state);
    let deep_link_state = Arc::new(DeepLinkState::default());

    let app = tauri::Builder::default()
        // The single-instance plugin must be first. Its deep-link feature forwards
        // second-instance arguments into the deep-link plugin registered below.
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            focus_window(app);
        }))
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .setup(move |app| {
            app.manage(Arc::clone(&deep_link_state));
            let builder =
                WebviewWindowBuilder::new(app, MAIN_WINDOW, WebviewUrl::App("index.html".into()))
                    // Keep the one-time token URL out of persistent WebView2 history/cache.
                    .incognito(true)
                    .title("DeepSeek Harness")
                    .inner_size(1280.0, 832.0)
                    .min_inner_size(720.0, 480.0);
            #[cfg(windows)]
            let builder = builder
                .decorations(false)
                .initialization_script(include_str!("window_chrome.js"));
            let window = builder.build()?;

            install_deep_link_listener(app.handle());
            let deep_link = app.state::<DeepLink<tauri::Wry>>();
            match deep_link.get_current() {
                Ok(Some(urls)) => dispatch_deep_links(app.handle(), urls),
                Ok(None) => {}
                Err(error) => {
                    eprintln!("desktop-tauri: could not read startup deep links: {error}")
                }
            }
            #[cfg(any(windows, target_os = "linux"))]
            if let Err(error) = deep_link.register_all() {
                eprintln!("desktop-tauri: could not register dsh-app://: {error}");
            }

            setup_tray(app.handle())?;
            if std::env::var_os("DSH_NODE_HOST").is_some()
                || std::env::var_os("DSH_NODE_PROFILE").is_some()
            {
                start_node(
                    window,
                    Arc::clone(&boot_state),
                    Arc::clone(&deep_link_state),
                );
            } else {
                start_embedded(
                    window,
                    Arc::clone(&deep_link_state),
                    Arc::clone(&embedded_state),
                );
            }
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("desktop-tauri: failed to build the application");

    app.run(move |_app, event| {
        if let RunEvent::Exit = event {
            embedded_exit_state.shutdown();
            exit_state.shutdown();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::sync::mpsc;
    use std::time::Duration;

    struct DropSignal(mpsc::Sender<()>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn embedded_host_shutdown_awaits_task_teardown() {
        let state = EmbeddedHostState::new();
        let (started_tx, started_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        state.spawn(async move {
            let _signal = DropSignal(dropped_tx);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });

        assert_eq!(started_rx.recv_timeout(Duration::from_secs(5)), Ok(()));
        state.shutdown();
        assert_eq!(dropped_rx.recv_timeout(Duration::from_secs(5)), Ok(()));
    }

    #[test]
    fn embedded_host_does_not_spawn_after_shutdown() {
        let state = EmbeddedHostState::new();
        let started = Arc::new(AtomicBool::new(false));
        state.shutdown();

        let task_started = Arc::clone(&started);
        state.spawn(async move {
            task_started.store(true, Ordering::Release);
        });

        assert!(!started.load(Ordering::Acquire));
    }

    #[test]
    fn native_config_registers_the_protocol_and_keeps_updates_disabled() {
        let config: Value = serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        assert_eq!(config["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            config["plugins"]["deep-link"]["desktop"]["schemes"],
            serde_json::json!(["dsh-app"])
        );
        assert_eq!(config["bundle"]["active"], false);
        assert!(!updater::configured(&config["plugins"]["updater"]));
    }

    #[test]
    fn tray_action_ids_are_distinct() {
        let ids = [TRAY_OPEN, TRAY_CHECK_UPDATES, TRAY_QUIT];
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            ids.len()
        );
    }
}
