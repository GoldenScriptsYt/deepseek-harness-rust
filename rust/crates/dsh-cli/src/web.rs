//! `dsh-rs web` and the desktop app: serve the web client from the Rust gateway.

use dsh_gateway::Gateway;
use serde_json::{json, Value};
use std::future::Future;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// A checkout, or a packaged `resources` folder with the same layout, holding the built
/// web client: `apps/web/dist` plus each plugin package's manifest and client file.
pub fn find_repo(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|d| d.join("apps/web/dist/index.html").is_file())
        .map(Path::to_path_buf)
}

/// Where the web client lives for an installed app: `resources/` beside the executable,
/// else `DSH_REPO_ROOT`, else a checkout above the executable or working directory.
pub fn locate_resources() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("DSH_REPO_ROOT") {
        return Some(PathBuf::from(explicit));
    }
    let exe = std::env::current_exe().ok();
    let beside = exe
        .as_ref()
        .and_then(|e| e.parent())
        .map(|d| d.join("resources"));
    if let Some(r) = beside.filter(|r| r.join("apps/web/dist/index.html").is_file()) {
        return Some(r);
    }
    exe.as_deref()
        .and_then(find_repo)
        .or_else(|| std::env::current_dir().ok().and_then(|d| find_repo(&d)))
}

/// The Harness home: `DSH_HOME`, else `~/.dsh`.
pub fn default_home() -> PathBuf {
    std::env::var_os("DSH_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("USERPROFILE")
                .or_else(|| std::env::var_os("HOME"))
                .map(|h| PathBuf::from(h).join(".dsh"))
        })
        .unwrap_or_else(|| PathBuf::from(".dsh"))
}

/// The profile and installation anchors used by the TypeScript plugin manager.
///
/// Both a checkout and the portable package keep the CLI manifest at
/// `apps/cli/package.json`; `package_app.py` writes a published-style metadata anchor there.
/// Source Web launches use `<home>/profiles/web`. The portable native package marks its
/// installation anchor as a Desktop launch and therefore uses `<home>/profiles/desktop`.
pub(crate) fn host_plugin_manager_config(
    repo: &Path,
    home: &Path,
) -> crate::plugin_manager::BundleListConfig {
    let repo = absolute_path(repo);
    let home = absolute_path(home);
    let install_anchor = repo.join("apps").join("cli").join("package.json");
    let profile_name = host_profile_name(&install_anchor);
    crate::plugin_manager::BundleListConfig::new(
        home.join("profiles").join(profile_name),
        install_anchor,
    )
}

/// Register the read-only bundle catalog without making plugin metadata a Host boot requirement.
pub(crate) fn register_host_plugin_manager(gateway: &Gateway, repo: &Path, home: &Path) {
    let config = host_plugin_manager_config(repo, home);
    let profile_manifest = config.profile_dir.join("package.json");
    let profile_name = config
        .profile_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("web");
    if let Err(error) = ensure_host_profile(&config.profile_dir, &profile_manifest, profile_name) {
        eprintln!(
            "[dsh-rs] plugin manager: profile manifest {} is unavailable: {error}; \
             pluginManager/listBundles will report a structured error",
            profile_manifest.display()
        );
    }
    if let Err(error) = validate_manifest_object(&config.install_anchor) {
        eprintln!(
            "[dsh-rs] plugin manager: installation manifest {} is unavailable: {error}; \
             pluginManager/listBundles will report a structured error",
            config.install_anchor.display()
        );
    }

    // Missing bundle packages, compatibility files, and patch files are handled per row by
    // list_bundles. They must not prevent the static Web Host from starting.
    crate::plugin_manager::register_plugin_manager(gateway, config);
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .map(|directory| directory.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

fn host_profile_name(install_anchor: &Path) -> &'static str {
    let manifest: Value = std::fs::read(install_anchor)
        .ok()
        .and_then(|document| serde_json::from_slice(&document).ok())
        .unwrap_or(Value::Null);
    match manifest
        .pointer("/dsh/host/profile")
        .and_then(Value::as_str)
    {
        Some("desktop") => "desktop",
        _ => "web",
    }
}

fn ensure_host_profile(
    profile_dir: &Path,
    manifest_path: &Path,
    profile_name: &str,
) -> std::io::Result<()> {
    std::fs::create_dir_all(profile_dir)?;
    let manifest = json!({
        "name": format!("dsh-profile-{profile_name}"),
        "private": true,
        "dependencies": {},
        "dsh": {
            "profile": {
                "bundles": [
                    "@deepseek-ai/dsh-base",
                    "@deepseek-ai/dsh-web-app"
                ]
            }
        }
    });
    let mut document = serde_json::to_vec_pretty(&manifest).map_err(std::io::Error::other)?;
    document.push(b'\n');

    use std::io::Write;
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(manifest_path)
    {
        Ok(mut file) => file.write_all(&document),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            validate_manifest_object(manifest_path)
        }
        Err(error) => Err(error),
    }
}

fn validate_manifest_object(path: &Path) -> std::io::Result<()> {
    let document: Value = serde_json::from_slice(&std::fs::read(path)?)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    if document.is_object() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "manifest root must be a JSON object",
        ))
    }
}

/// Unguessable launch token for the sign-in URL.
pub fn new_token() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::new();
    for _ in 0..3 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out
}

/// A running Host: the URL a browser opens once to sign in, and the server future to drive.
pub struct Started<F> {
    pub entry_url: String,
    pub serve: F,
}

/// Bind `127.0.0.1:<port>` (0 picks a free port), set up the web shell and endpoints for the
/// client at `repo`, and return the sign-in URL with the future that serves requests.
pub async fn start(
    repo: &Path,
    home: &Path,
    port: u16,
) -> std::io::Result<Started<impl Future<Output = std::io::Result<()>>>> {
    let token = new_token();
    let gw = Gateway::new(token.clone(), home.display().to_string(), None);
    let (addr, serve) = gw.bind(&format!("127.0.0.1:{port}")).await?;
    let origin = format!("http://{addr}");
    gw.with_web_shell(repo)?;
    crate::interaction::install_with_gateway(&gw);
    let cx = crate::api::ApiContext::new(origin.clone(), token, home);
    crate::api::register_all(&gw, cx.clone());
    crate::catalog_api::register_all(&gw, cx.clone());
    crate::workspace_api::register_all(&gw, cx.clone());
    crate::session_query_api::register_all(&gw, cx.clone());
    let processes = crate::process_api::register_all(&gw, cx);
    register_host_plugin_manager(&gw, repo, home);
    gw.on_unhandled(|method, _| {
        eprintln!("[dsh-rs] unported rpc: {method}");
    });
    Ok(Started {
        entry_url: gw.entry_url(&origin),
        serve: async move {
            let result = serve.await;
            processes.dispose().await;
            result
        },
    })
}
