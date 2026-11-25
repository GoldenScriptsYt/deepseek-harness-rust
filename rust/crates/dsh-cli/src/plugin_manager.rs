//! Read-only Rust port of the TypeScript plugin manager's bundle catalog.
//!
//! Authoritative behavior: `packages/boot/plugin-manager/src/index.ts` (`readBundles`),
//! `official-install-target.ts`, `install-spec.ts`, app-boot's profile/package metadata readers,
//! and `packages/util/official-bundles/src/catalog.generated.ts`. No plugin JavaScript is
//! evaluated. Bundle listing stays offline; the registry endpoint reads pnpm's resolved config.

use dsh_gateway::{Gateway, RpcError};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::error::Error as StdError;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const RPC_METHOD: &str = "pluginManager/listBundles";
const CATALOG_JSON: &str = include_str!("../assets/plugin_manager/official-bundles.json");
const DEFAULT_FALLBACK_REGISTRIES: &[&str] = &["https://registry.npmmirror.com/"];
const LISTING_DIAGNOSTIC: &str = "dsh.bundle.listedByDefault must be a boolean";
const MAX_ICON_BYTES: u64 = 256 * 1024;
const NODE_PLATFORMS: &[&str] = &[
    "aix", "android", "darwin", "freebsd", "haiku", "linux", "openbsd", "sunos", "win32", "cygwin",
    "netbsd",
];
const OPTIONAL_BUNDLES: &[&str] = &[
    "@deepseek-ai/dsh-cua-bundle",
    "@deepseek-ai/dsh-cua-mcp",
    "@deepseek-ai/dsh-timeline",
    "@deepseek-ai/dsh-experimental-session-search",
    "@deepseek-ai/dsh-experimental-ralph-bundle",
    "@deepseek-ai/dsh-experimental-terminal-bundle",
    "@deepseek-ai/dsh-experimental-badge-skill-bundle",
    "@deepseek-ai/dsh-experimental-session-titles-bundle",
    "@deepseek-ai/dsh-experimental-voice-input-bundle",
    "@deepseek-ai/dsh-experimental-cot-translation-bundle",
    "@deepseek-ai/dsh-experimental-auto-review",
    "@deepseek-ai/dsh-experimental-inspector-profile",
    "@deepseek-ai/dsh-experimental-liveshare",
    "@deepseek-ai/dsh-experimental-plugin-market-bundle",
    "@deepseek-ai/dsh-experimental-tool-worktree",
    "@deepseek-ai/dsh-experimental-forge-bundle",
    "@deepseek-ai/dsh-deepseek-chat-import",
];
const PROTECTED_PLUGINS: &[&str] = &[
    "@deepseek-ai/dsh-plugin-manager",
    "@deepseek-ai/cordis-plugin-loader",
    "@deepseek-ai/cordis-plugin-include",
    "@deepseek-ai/dsh-api-gateway",
    "@deepseek-ai/dsh-host-webserver",
    "@deepseek-ai/dsh-client-modules",
    "@deepseek-ai/dsh-client-ui-plugin-manager",
    "@deepseek-ai/dsh-host-plugin-inventory",
    "@deepseek-ai/dsh-typert-registry",
    "@deepseek-ai/dsh-api-remotes",
    "@deepseek-ai/dsh-api-remote-access",
    "@deepseek-ai/cordis-plugin-timer",
    "@deepseek-ai/dsh-client-connection",
    "@deepseek-ai/dsh-host-frontend-static",
    "@deepseek-ai/dsh-tools",
    "@deepseek-ai/dsh-hmr",
];

/// Filesystem and Host facts needed to read one profile's bundle inventory.
#[derive(Clone, Debug)]
pub struct BundleListConfig {
    /// Directory containing the profile's package.json.
    pub profile_dir: PathBuf,
    /// Absolute package.json path of the running DSH installation.
    pub install_anchor: PathBuf,
    /// Exact running DSH version, normally this crate's Cargo package version.
    pub runtime_version: String,
    /// Node Host platform; defaults to the compiled Host platform.
    pub platform: Option<String>,
    /// Whether a Linux Host is WSL; only relevant when `platform` is `linux`.
    pub wsl: bool,
    /// Bundles the running manager already knows carry protected management rows.
    pub protected_bundles: Vec<String>,
    /// Live Loader entry ids by declared patch row id. Omit when no Loader is attached.
    pub live_entries: BTreeMap<String, String>,
}

impl BundleListConfig {
    /// Configure a profile using the Rust package's authoritative runtime version.
    pub fn new(profile_dir: impl Into<PathBuf>, install_anchor: impl Into<PathBuf>) -> Self {
        Self {
            profile_dir: profile_dir.into(),
            install_anchor: install_anchor.into(),
            runtime_version: env!("CARGO_PKG_VERSION").to_owned(),
            platform: None,
            wsl: false,
            protected_bundles: Vec::new(),
            live_entries: BTreeMap::new(),
        }
    }
}

/// A profile or installation manifest could not be read. Per-bundle failures are
/// returned as `BundleInfo.error` instead; this error stops the whole inventory read.
#[derive(Debug)]
pub struct PluginManagerError {
    code: &'static str,
    path: PathBuf,
    message: String,
    source: Option<io::Error>,
}

impl PluginManagerError {
    fn io(code: &'static str, path: &Path, action: &str, source: io::Error) -> Self {
        Self {
            code,
            path: path.to_path_buf(),
            message: format!("dsh: failed to {action} {}: {source}", path.display()),
            source: Some(source),
        }
    }

    fn data(code: &'static str, path: &Path, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.to_path_buf(),
            message: message.into(),
            source: None,
        }
    }

    /// Stable machine-readable failure discriminant.
    pub fn code(&self) -> &'static str {
        self.code
    }
    /// Manifest or catalog file whose read failed.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl std::fmt::Display for PluginManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for PluginManagerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Register `pluginManager/listBundles` for later use by the main API.
///
/// Each request re-reads the profile, installation, package manifests, metadata,
/// and patches. The handler never writes files or invokes pnpm.
pub fn register_plugin_manager(gateway: &Gateway, config: BundleListConfig) {
    let profile_dir = config.profile_dir.clone();
    gateway.rpc(RPC_METHOD, move |_payload| {
        let config = config.clone();
        async move { list_bundles(&config).map(Value::Array).map_err(rpc_error) }
    });
    gateway.rpc("pluginManager/registries", move |_| {
        let profile_dir = profile_dir.clone();
        async move {
            let resolved = read_profile_registry(&profile_dir).await;
            Ok(registry_options(resolved))
        }
    });
}

fn registry_options(resolved: Option<String>) -> Value {
    json!({
        "registry": null,
        "fallbackRegistries": DEFAULT_FALLBACK_REGISTRIES,
        "resolved": resolved
    })
}

async fn read_profile_registry(profile_dir: &Path) -> Option<String> {
    let mut command = profile_registry_command();
    command
        .current_dir(profile_dir)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .ok()?
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_registry_stdout(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(windows)]
fn profile_registry_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new("cmd");
    command.args(["/D", "/S", "/C", "pnpm config get registry"]);
    command
}

#[cfg(not(windows))]
fn profile_registry_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new("pnpm");
    command.args(["config", "get", "registry"]);
    command
}

fn parse_registry_stdout(stdout: &str) -> Option<String> {
    public_registry_url(stdout.lines().last()?.trim())
}

fn public_registry_url(value: &str) -> Option<String> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return None;
    }
    let (scheme, rest) = value.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let host = rest[..authority_end].rsplit('@').next()?;
    if host.is_empty() {
        return None;
    }
    let suffix = &rest[authority_end..];
    let suffix = if suffix.is_empty() || suffix.starts_with('?') || suffix.starts_with('#') {
        format!("/{suffix}")
    } else {
        suffix.to_owned()
    };
    Some(format!("{scheme}://{host}{suffix}"))
}

fn rpc_error(error: PluginManagerError) -> RpcError {
    RpcError {
        code: "gateway/internal".into(),
        message: error.to_string(),
        details: json!({"cause": {"name": "PluginManagerError", "code": error.code(), "path": error.path()}}),
    }
}

/// Build the TypeScript-compatible `BundleInfo[]` from bundled Official metadata
/// and current local files. Names retain first-seen order across selection,
/// profile dependencies, installation dependencies, and the Official catalog.
pub fn list_bundles(config: &BundleListConfig) -> Result<Vec<Value>, PluginManagerError> {
    let runtime = SemVer::parse(&config.runtime_version).ok_or_else(|| {
        PluginManagerError::data(
            "PLUGIN_MANAGER_RUNTIME_VERSION_INVALID",
            &config.install_anchor,
            format!(
                "Invalid dsh runtime version: {:?}; expected a semantic version",
                config.runtime_version
            ),
        )
    })?;
    let profile_path = config.profile_dir.join("package.json");
    let profile = read_required_object(
        &profile_path,
        "PLUGIN_MANAGER_PROFILE_READ_FAILED",
        "read profile manifest",
    )?;
    let installation = read_required_object(
        &config.install_anchor,
        "PLUGIN_MANAGER_PROFILE_READ_FAILED",
        "read installation manifest",
    )?;
    let selected = string_array(
        profile.pointer("/dsh/profile/bundles"),
        &profile_path,
        "dsh.profile.bundles",
    )?;
    let recorded = string_map(
        profile.get("dependencies"),
        &profile_path,
        "dependencies",
        "PLUGIN_MANAGER_PROFILE_READ_FAILED",
    )?;
    let shipped = string_map(
        installation.get("dependencies"),
        &config.install_anchor,
        "dependencies",
        "PLUGIN_MANAGER_PROFILE_READ_FAILED",
    )?;
    let exemptions = read_exemptions(&config.profile_dir.join("compatibility.json"));
    let catalog = catalog_entries()?;
    let platform = config.platform.clone().unwrap_or_else(default_platform);
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for name in selected
        .iter()
        .chain(recorded.keys())
        .chain(shipped.keys())
        .chain(catalog.iter().map(|entry| &entry.name))
    {
        if seen.insert(name.to_owned()) {
            names.push(name.to_owned());
        }
    }

    let mut bundles = Vec::new();
    for name in names {
        if let Some(bundle) = read_bundle(
            config,
            &name,
            &selected,
            &recorded,
            &shipped,
            &exemptions,
            &catalog,
            &platform,
            &runtime,
        )? {
            bundles.push(bundle);
        }
    }
    Ok(bundles)
}

#[derive(Clone)]
struct OfficialEntry {
    name: String,
    meta: Value,
    platforms: Option<Vec<String>>,
}

fn catalog_entries() -> Result<Vec<OfficialEntry>, PluginManagerError> {
    let path = Path::new("assets/plugin_manager/official-bundles.json");
    let text = CATALOG_JSON;
    let document: Value = serde_json::from_str(text).map_err(|error| {
        PluginManagerError::data("PLUGIN_MANAGER_CATALOG_INVALID", path, error.to_string())
    })?;
    let entries = document
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            PluginManagerError::data(
                "PLUGIN_MANAGER_CATALOG_INVALID",
                path,
                "catalog entries must be an array",
            )
        })?;
    let mut unique = HashSet::new();
    entries
        .iter()
        .map(|entry| {
            let name = entry
                .get("packageName")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    PluginManagerError::data(
                        "PLUGIN_MANAGER_CATALOG_INVALID",
                        path,
                        "catalog packageName must be a string",
                    )
                })?;
            if !unique.insert(name) {
                return Err(PluginManagerError::data(
                    "PLUGIN_MANAGER_CATALOG_INVALID",
                    path,
                    format!("duplicate catalog package {name}"),
                ));
            }
            let meta = entry.get("meta").cloned().ok_or_else(|| {
                PluginManagerError::data(
                    "PLUGIN_MANAGER_CATALOG_INVALID",
                    path,
                    "catalog meta must be present",
                )
            })?;
            let platforms = match entry.get("platforms") {
                None => None,
                Some(value) => Some(
                    value
                        .as_array()
                        .and_then(|items| {
                            items
                                .iter()
                                .map(Value::as_str)
                                .map(|value| value.map(str::to_owned))
                                .collect::<Option<Vec<String>>>()
                        })
                        .ok_or_else(|| {
                            PluginManagerError::data(
                                "PLUGIN_MANAGER_CATALOG_INVALID",
                                path,
                                "catalog platforms must be strings",
                            )
                        })?,
                ),
            };
            Ok(OfficialEntry {
                name: name.to_owned(),
                meta,
                platforms,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn read_bundle(
    config: &BundleListConfig,
    name: &str,
    selected: &[String],
    recorded: &Map<String, Value>,
    shipped: &Map<String, Value>,
    exemptions: &Map<String, Value>,
    catalog: &[OfficialEntry],
    platform: &str,
    runtime: &SemVer,
) -> Result<Option<Value>, PluginManagerError> {
    let installed = recorded.contains_key(name);
    let optional = OPTIONAL_BUNDLES.contains(&name);
    let enabled = selected.iter().any(|candidate| candidate == name);
    let is_shipped = shipped.contains_key(name);
    let offered = catalog.iter().find(|entry| entry.name == name);
    let official = optional || offered.is_some();
    let mut target = Map::new();
    let mut target_error = None;
    if offered.is_some() {
        match official_install_target(config, name, runtime) {
            Ok(value) => {
                target.insert("installTarget".into(), value);
            }
            Err(error) => {
                target_error = Some(operation_error(error));
            }
        }
    }
    let catalog_meta = offered.map(|entry| ("meta".to_owned(), entry.meta.clone()));

    if offered.is_some() && !installed && !is_shipped {
        let platform_error = entry_platform_error(
            offered.and_then(|entry| entry.platforms.as_deref()),
            platform,
            config.wsl,
        );
        let error = platform_error
            .clone()
            .or(target_error.clone())
            .or_else(|| enabled.then(|| json!({"code":"not-bundle"})));
        let mut bundle = base_bundle(
            name,
            official,
            "missing",
            enabled,
            installed,
            optional,
            BundleManagement::Writable { removable: enabled },
        );
        if let Some((key, value)) = catalog_meta {
            bundle.insert(key, value);
        }
        bundle.extend(target);
        if matches!(
            platform_error
                .as_ref()
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str),
            Some("unsupported-platform")
        ) {
            bundle.insert("listedByDefault".into(), Value::Bool(false));
        }
        if let Some(error) = error {
            bundle.insert("error".into(), error);
        }
        return Ok(Some(Value::Object(bundle)));
    }

    let owned = installed && !is_shipped;
    let removable = owned || (enabled && !installed && !is_shipped);
    let read_only = config
        .protected_bundles
        .iter()
        .any(|candidate| candidate == name);
    let management = if read_only {
        BundleManagement::ReadOnly
    } else {
        BundleManagement::Writable { removable }
    };
    let source_of = |manifest_name: Option<&str>| -> Result<Option<String>, String> {
        if !owned {
            return Ok(None);
        }
        let recorded = recorded[name]
            .as_str()
            .ok_or_else(|| format!("dependencies[{name:?}] must be a string"))?;
        Ok(Some(dependency_spec(
            name,
            recorded,
            &config.profile_dir,
            manifest_name.unwrap_or(name),
        )))
    };
    let availability = if is_shipped {
        "installation"
    } else {
        "profile"
    };
    let mut listing = Map::new();
    let resolved = match resolve_bundle_dir(name, &config.install_anchor, &config.profile_dir) {
        Ok(directory) => directory,
        Err(error) => {
            if is_shipped
                && !installed
                && !enabled
                && !optional
                && offered.is_none()
                && shipped
                    .get(name)
                    .and_then(Value::as_str)
                    .is_some_and(|spec| spec.starts_with("workspace:"))
            {
                return Ok(None);
            }
            let mut bundle = base_bundle(
                name,
                official,
                if owned { "profile" } else { "missing" },
                enabled,
                installed,
                optional,
                management,
            );
            if let Some((key, value)) = catalog_meta {
                bundle.insert(key, value);
            }
            bundle.extend(target);
            let source_error = apply_source(&mut bundle, source_of(None));
            let error = if error.starts_with("dsh: cannot resolve profile bundle") {
                json!({"code":"not-bundle"})
            } else {
                operation_error(error)
            };
            bundle.insert(
                "error".into(),
                target_error
                    .or_else(|| source_error.map(operation_error))
                    .unwrap_or(error),
            );
            return Ok(Some(Value::Object(bundle)));
        }
    };
    let manifest_path = resolved.join("package.json");
    let manifest = match read_object_value(&manifest_path) {
        Ok(manifest) => manifest,
        Err(error) => {
            if enabled || installed || optional || offered.is_some() {
                let mut bundle = base_bundle(
                    name, official, "missing", enabled, installed, optional, management,
                );
                if let Some((key, value)) = catalog_meta {
                    bundle.insert(key, value);
                }
                bundle.extend(target);
                let source_error = apply_source(&mut bundle, source_of(None));
                bundle.insert(
                    "error".into(),
                    target_error
                        .or_else(|| source_error.map(operation_error))
                        .unwrap_or_else(|| operation_error(error)),
                );
                return Ok(Some(Value::Object(bundle)));
            }
            return Ok(None);
        }
    };
    let patch = manifest.pointer("/dsh/bundle/patch");
    if patch.is_none() {
        if !enabled && offered.is_none() {
            return Ok(None);
        }
        let mut bundle = base_bundle(
            name,
            official,
            availability,
            enabled,
            installed,
            optional,
            management,
        );
        if let Some((key, value)) = catalog_meta {
            bundle.insert(key, value);
        }
        bundle.extend(target);
        let manifest_name = manifest.get("name").and_then(Value::as_str);
        let source_error = apply_source(&mut bundle, source_of(manifest_name));
        bundle.insert(
            "error".into(),
            target_error
                .or_else(|| source_error.map(operation_error))
                .unwrap_or_else(|| json!({"code":"not-bundle"})),
        );
        return Ok(Some(Value::Object(bundle)));
    }

    let version = manifest
        .get("version")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let listed = manifest.pointer("/dsh/bundle/listedByDefault");
    let listing_diagnostic = match listed {
        None | Some(Value::Bool(_)) => None,
        Some(_) => Some(LISTING_DIAGNOSTIC),
    };
    if let Some(Value::Bool(value)) = listed {
        listing.insert("listedByDefault".into(), Value::Bool(*value));
    }
    let compatibility = match evaluate_compatibility(&manifest, exemptions, runtime) {
        Ok(compatibility) => compatibility,
        Err(error) => {
            let mut bundle = base_bundle(
                name,
                official,
                availability,
                enabled,
                installed,
                optional,
                management,
            );
            bundle.extend(listing);
            if let Some((key, value)) = catalog_meta {
                bundle.insert(key, value);
            }
            bundle.extend(target);
            if let Some(value) = &version {
                bundle.insert("version".into(), Value::String(value.clone()));
            }
            let source_error = apply_source(&mut bundle, source_of(None));
            bundle.insert(
                "error".into(),
                target_error
                    .or_else(|| source_error.map(operation_error))
                    .unwrap_or_else(|| operation_error(error)),
            );
            return Ok(Some(Value::Object(bundle)));
        }
    };
    if let Some(issue) = compatibility {
        if !issue["exempted"].as_bool().unwrap_or(false) {
            let mut bundle = base_bundle(
                name,
                official,
                availability,
                enabled,
                installed,
                optional,
                management,
            );
            bundle.extend(listing);
            if let Some((key, value)) = catalog_meta {
                bundle.insert(key, value);
            }
            bundle.extend(target);
            let source_error = apply_source(&mut bundle, source_of(None));
            if let Some(value) = &version {
                bundle.insert("version".into(), Value::String(value.clone()));
            }
            bundle.insert(
                "error".into(),
                target_error
                    .or_else(|| source_error.map(operation_error))
                    .unwrap_or_else(
                        || json!({"code":"incompatible-version", "incompatible":[issue]}),
                    ),
            );
            return Ok(Some(Value::Object(bundle)));
        }
    }
    let platform_error = manifest_platform_error(
        manifest.pointer("/dsh/bundle/platforms"),
        platform,
        config.wsl,
    );
    if matches!(
        platform_error
            .as_ref()
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str),
        Some("unsupported-platform")
    ) {
        listing.clear();
        listing.insert("listedByDefault".into(), Value::Bool(false));
    }
    let mut bundle = base_bundle(
        name,
        official,
        availability,
        enabled,
        installed,
        optional,
        management,
    );
    bundle.extend(listing);
    bundle.extend(target);
    if let Some(value) = &version {
        bundle.insert("version".into(), Value::String(value.clone()));
    }
    if let Some(description) = manifest
        .get("description")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        bundle.insert("description".into(), Value::String(description.to_owned()));
    }
    let mut meta = if let Some(entry) = offered {
        entry.meta.clone()
    } else {
        read_plugin_meta(
            manifest.get("name").and_then(Value::as_str).unwrap_or(name),
            &resolved,
        )
        .unwrap_or_default()
    };
    if let Some(diagnostic) = listing_diagnostic {
        let error = match meta.get("error").and_then(Value::as_str) {
            Some(previous) => format!("{previous}; {diagnostic}"),
            None => diagnostic.to_owned(),
        };
        insert_object(&mut meta, "error", Value::String(error));
    }
    if !meta.as_object().is_some_and(|fields| fields.is_empty()) {
        bundle.insert("meta".into(), meta);
    }
    let manifest_name = manifest.get("name").and_then(Value::as_str);
    let source_error = apply_source(&mut bundle, source_of(manifest_name));
    if let Some(error) = source_error {
        bundle.insert(
            "error".into(),
            target_error.unwrap_or_else(|| operation_error(error)),
        );
        return Ok(Some(Value::Object(bundle)));
    }
    let patch_paths = match patch_paths(&resolved, patch.expect("bundle patch checked above")) {
        Ok(paths) => paths,
        Err(error) => {
            if enabled || installed || optional || offered.is_some() {
                bundle.insert(
                    "error".into(),
                    target_error.unwrap_or_else(|| operation_error(error)),
                );
                bundle.insert("rows".into(), Value::Array(Vec::new()));
                bundle.insert("overrides".into(), Value::Array(Vec::new()));
                return Ok(Some(Value::Object(bundle)));
            }
            return Ok(None);
        }
    };
    let (rows, overrides) = match declared_rows(&patch_paths, &resolved, config) {
        Ok(rows) => rows,
        Err(error) => {
            bundle.insert(
                "error".into(),
                target_error.unwrap_or_else(|| operation_error(error)),
            );
            return Ok(Some(Value::Object(bundle)));
        }
    };
    let protected = config
        .protected_bundles
        .iter()
        .any(|candidate| candidate == name)
        || rows
            .iter()
            .any(|row| PROTECTED_PLUGINS.contains(&row["moduleName"].as_str().unwrap_or("")));
    if protected && !read_only {
        bundle.insert(
            "readOnlyReason".into(),
            Value::String("management-required".into()),
        );
        bundle.insert("removable".into(), Value::Bool(false));
    }
    bundle.insert("rows".into(), Value::Array(rows));
    bundle.insert(
        "overrides".into(),
        Value::Array(overrides.into_iter().map(Value::String).collect()),
    );
    if let Some(error) = platform_error.or(target_error) {
        bundle.insert("error".into(), error);
    }
    if let Some(update) = update_kind(&bundle) {
        bundle.insert("update".into(), Value::String(update.into()));
    }
    Ok(Some(Value::Object(bundle)))
}

#[derive(Clone, Copy)]
enum BundleManagement {
    Writable { removable: bool },
    ReadOnly,
}

fn base_bundle(
    name: &str,
    official: bool,
    availability: &str,
    enabled: bool,
    installed: bool,
    optional: bool,
    management: BundleManagement,
) -> Map<String, Value> {
    let mut bundle = Map::new();
    bundle.insert("name".into(), Value::String(name.to_owned()));
    bundle.insert("official".into(), Value::Bool(official));
    bundle.insert(
        "availability".into(),
        Value::String(availability.to_owned()),
    );
    bundle.insert("enabled".into(), Value::Bool(enabled));
    bundle.insert("installed".into(), Value::Bool(installed));
    bundle.insert("optional".into(), Value::Bool(optional));
    bundle.insert(
        "removable".into(),
        Value::Bool(matches!(
            management,
            BundleManagement::Writable { removable: true }
        )),
    );
    if matches!(management, BundleManagement::ReadOnly) {
        bundle.insert(
            "readOnlyReason".into(),
            Value::String("management-required".into()),
        );
    }
    bundle.insert("rows".into(), Value::Array(Vec::new()));
    bundle.insert("overrides".into(), Value::Array(Vec::new()));
    bundle
}

fn apply_source(
    bundle: &mut Map<String, Value>,
    source: Result<Option<String>, String>,
) -> Option<String> {
    match source {
        Ok(Some(source)) => {
            bundle.insert("source".into(), Value::String(source));
            None
        }
        Ok(None) => None,
        Err(error) => Some(error),
    }
}

fn insert_object(object: &mut Value, key: &str, value: Value) {
    if !object.is_object() {
        *object = Value::Object(Map::new());
    }
    object
        .as_object_mut()
        .expect("object")
        .insert(key.to_owned(), value);
}

fn operation_error(diagnostic: impl Into<String>) -> Value {
    json!({"code":"operation-error", "diagnostic":diagnostic.into()})
}

fn read_required_object(
    path: &Path,
    code: &'static str,
    action: &str,
) -> Result<Value, PluginManagerError> {
    let text = fs::read_to_string(path)
        .map_err(|error| PluginManagerError::io(code, path, action, error))?;
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        PluginManagerError::data(
            code,
            path,
            format!("dsh: failed to {action} {}: {error}", path.display()),
        )
    })?;
    if !value.is_object() {
        return Err(PluginManagerError::data(
            code,
            path,
            format!("dsh: {action} {} must hold a JSON object", path.display()),
        ));
    }
    Ok(value)
}

fn read_object_value(path: &Path) -> Result<Value, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|error| format!("invalid JSON in {}: {error}", path.display()))?;
    if !value.is_object() {
        return Err(format!("{} must hold a JSON object", path.display()));
    }
    Ok(value)
}

fn string_array(
    value: Option<&Value>,
    path: &Path,
    field: &str,
) -> Result<Vec<String>, PluginManagerError> {
    match value {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_owned).ok_or_else(|| {
                    PluginManagerError::data(
                        "PLUGIN_MANAGER_PROFILE_READ_FAILED",
                        path,
                        format!("{field} must contain only strings"),
                    )
                })
            })
            .collect(),
        Some(_) => Err(PluginManagerError::data(
            "PLUGIN_MANAGER_PROFILE_READ_FAILED",
            path,
            format!("{field} must be an array"),
        )),
    }
}

fn string_map(
    value: Option<&Value>,
    path: &Path,
    field: &str,
    code: &'static str,
) -> Result<Map<String, Value>, PluginManagerError> {
    match value {
        None => Ok(Map::new()),
        Some(value @ Value::Object(_)) => Ok(value.as_object().expect("object").clone()),
        Some(_) => Err(PluginManagerError::data(
            code,
            path,
            format!("{field} must be an object"),
        )),
    }
}

fn read_exemptions(path: &Path) -> Map<String, Value> {
    let Ok(text) = fs::read_to_string(path) else {
        return Map::new();
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return Map::new();
    };
    let Value::Object(entries) = value else {
        return Map::new();
    };
    entries
        .into_iter()
        .filter(|(key, versions)| {
            let Some((name, version)) = key.rsplit_once('@') else {
                return false;
            };
            valid_package_name(name)
                && SemVer::parse(version).is_some()
                && versions.as_array().is_some_and(|items| {
                    items.iter().all(|item| {
                        item.as_str()
                            .is_some_and(|value| SemVer::parse(value).is_some())
                    })
                })
        })
        .collect()
}

fn default_platform() -> String {
    if cfg!(target_os = "windows") {
        "win32"
    } else if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "android") {
        "android"
    } else {
        std::env::consts::OS
    }
    .to_owned()
}

fn valid_platforms(platforms: &[String]) -> bool {
    !platforms.is_empty()
        && platforms
            .iter()
            .all(|value| value == "wsl" || NODE_PLATFORMS.contains(&value.as_str()))
}

fn manifest_platform_error(value: Option<&Value>, platform: &str, wsl: bool) -> Option<Value> {
    let value = value?;
    let Some(items) = value.as_array() else {
        return Some(operation_error(
            "dsh.bundle.platforms must be a nonempty list of Node platform names or wsl",
        ));
    };
    let Some(platforms) = items
        .iter()
        .map(Value::as_str)
        .map(|value| value.map(str::to_owned))
        .collect::<Option<Vec<String>>>()
    else {
        return Some(operation_error(
            "dsh.bundle.platforms must be a nonempty list of Node platform names or wsl",
        ));
    };
    entry_platform_error(Some(&platforms), platform, wsl)
}

fn entry_platform_error(platforms: Option<&[String]>, platform: &str, wsl: bool) -> Option<Value> {
    let platforms = platforms?;
    if !valid_platforms(platforms) {
        return Some(operation_error(
            "dsh.bundle.platforms must be a nonempty list of Node platform names or wsl",
        ));
    }
    (!(platforms.iter().any(|value| value == platform)
        || wsl && platform == "linux" && platforms.iter().any(|value| value == "wsl")))
    .then(|| json!({"code":"unsupported-platform"}))
}

fn resolve_bundle_dir(
    name: &str,
    install_anchor: &Path,
    profile_dir: &Path,
) -> Result<PathBuf, String> {
    for anchor in [install_anchor, &profile_dir.join("package.json")] {
        if let Some(directory) = package_dir_from_anchor(anchor, name) {
            return Ok(directory);
        }
    }
    Err(format!("dsh: cannot resolve profile bundle {name:?} from the dsh installation or {}; run 'dsh plugin --profile {} install' if its dependency is not installed",
        profile_dir.display(), profile_dir.file_name().and_then(OsStr::to_str).unwrap_or("")))
}

fn package_dir_from_anchor(anchor: &Path, name: &str) -> Option<PathBuf> {
    let mut directory = anchor.parent()?;
    loop {
        let candidate = directory.join("node_modules").join(name);
        if candidate.join("package.json").exists() {
            return Some(candidate);
        }
        directory = directory.parent()?;
    }
}

fn official_install_target(
    config: &BundleListConfig,
    name: &str,
    _runtime: &SemVer,
) -> Result<Value, String> {
    let anchor = fs::canonicalize(&config.install_anchor).map_err(|error| {
        format!(
            "failed to resolve installation {}: {error}",
            config.install_anchor.display()
        )
    })?;
    let installation = read_object_value(&anchor)?;
    let source = installation.get("name").and_then(Value::as_str) == Some("@deepseek-ai/dsh")
        && installation
            .get("dependencies")
            .and_then(Value::as_object)
            .is_some_and(|dependencies| {
                dependencies.values().any(|value| {
                    value
                        .as_str()
                        .is_some_and(|value| value.starts_with("workspace:"))
                })
            });
    if !source {
        return Ok(
            json!({"spec":format!("{name}@{}", config.runtime_version), "version":config.runtime_version}),
        );
    }
    let root = anchor
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .ok_or_else(|| "Official bundle: incomplete source DSH checkout".to_owned())?;
    if anchor != root.join("apps/cli/package.json") {
        return Err(format!("Official bundle {name}: source DSH installation {} requires its complete workspace checkout", anchor.display()));
    }
    let root_manifest = root.join("package.json");
    if !root_manifest.is_file()
        || read_object_value(&root_manifest)?
            .get("name")
            .and_then(Value::as_str)
            != Some("@deepseek-ai/dsh-root")
    {
        return Err(format!("Official bundle {name}: source DSH installation {} requires its complete workspace checkout", anchor.display()));
    }
    let mut matches = Vec::new();
    let groups = fs::read_dir(root.join("packages"))
        .map_err(|error| format!("failed to read source checkout packages: {error}"))?;
    for group in groups {
        let group = group
            .map_err(|error| format!("failed to read source checkout packages: {error}"))?
            .path();
        if !group.is_dir() {
            continue;
        }
        let packages = fs::read_dir(&group)
            .map_err(|error| format!("failed to read {}: {error}", group.display()))?;
        for package in packages {
            let file = package
                .map_err(|error| format!("failed to read {}: {error}", group.display()))?
                .path()
                .join("package.json");
            if file.is_file() {
                let manifest = read_object_value(&file)?;
                if manifest.get("name").and_then(Value::as_str) == Some(name) {
                    matches.push((file, manifest));
                }
            }
        }
    }
    if matches.len() != 1 {
        return Err(format!("Official bundle {name}: expected one workspace package in {}, found {}; restore the checkout and run pnpm install", root.display(), matches.len()));
    }
    let (file, manifest) = &matches[0];
    let directory = fs::canonicalize(file.parent().expect("manifest parent"))
        .map_err(|error| format!("failed to resolve {}: {error}", file.display()))?;
    let bundle = manifest.pointer("/dsh/bundle").ok_or_else(|| {
        format!(
            "Official bundle {name}: {} must declare a public bundle at DSH version {}",
            file.display(),
            config.runtime_version
        )
    })?;
    if manifest.get("version").and_then(Value::as_str) != Some(config.runtime_version.as_str())
        || manifest.get("private").and_then(Value::as_bool) == Some(true)
        || bundle.get("patch").is_none()
    {
        return Err(format!(
            "Official bundle {name}: {} must declare a public bundle at DSH version {}",
            file.display(),
            config.runtime_version
        ));
    }
    let main = manifest
        .get("main")
        .and_then(Value::as_str)
        .map(|value| resolve_path(&directory, value));
    if !main.as_ref().is_some_and(|path| path.is_file()) {
        return Err(format!("Official bundle {name}: built runtime is missing in {}; run pnpm install and pnpm run build in {}", directory.display(), root.display()));
    }
    for patch in patch_paths(&directory, &bundle["patch"])? {
        if !patch.is_file() {
            return Err(format!(
                "Official bundle {name}: bundle patch {} is missing; restore the checkout",
                patch.display()
            ));
        }
    }
    Ok(
        json!({"spec":format!("link:{}", display_path(&directory)), "version":config.runtime_version}),
    )
}

fn display_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{rest}")
    } else {
        text.strip_prefix(r"\\?\").unwrap_or(&text).to_owned()
    }
}

fn patch_paths(directory: &Path, patch: &Value) -> Result<Vec<PathBuf>, String> {
    let files = match patch {
        Value::String(file) => vec![file.clone()],
        Value::Array(files) => files
            .iter()
            .map(|file| {
                file.as_str().map(str::to_owned).ok_or_else(|| {
                    "dsh.bundle.patch must be a file path or a list of file paths".to_owned()
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("dsh.bundle.patch must be a file path or a list of file paths".to_owned()),
    };
    Ok(files
        .into_iter()
        .map(|file| resolve_path(directory, &file))
        .collect())
}

fn dependency_spec(name: &str, recorded: &str, profile_dir: &Path, package_name: &str) -> String {
    if let Some((protocol, path)) = recorded
        .split_once(':')
        .filter(|(protocol, _)| *protocol == "file" || *protocol == "link")
    {
        let home = || -> PathBuf {
            std::env::var_os("USERPROFILE")
                .or_else(|| std::env::var_os("HOME"))
                .map(PathBuf::from)
                .unwrap_or_default()
        };
        let expanded = if path == "~" {
            home()
        } else {
            path.strip_prefix("~/")
                .or_else(|| path.strip_prefix("~\\"))
                .map(|path| home().join(path))
                .unwrap_or_else(|| PathBuf::from(path))
        };
        let absolute = absolute_path(profile_dir, &expanded);
        let spec = format!("{protocol}:{}", absolute.to_string_lossy());
        return if package_name == name {
            spec
        } else {
            format!("{name}@{spec}")
        };
    }
    if non_registry_value(recorded) {
        let spec = strip_http_user_info(recorded);
        return if package_name == name {
            spec
        } else {
            format!("{name}@{spec}")
        };
    }
    format!("{name}@{recorded}")
}

fn non_registry_value(value: &str) -> bool {
    let protocol = value
        .split_once(':')
        .map(|(protocol, _)| protocol.to_ascii_lowercase());
    if let Some(protocol) = protocol {
        if !matches!(protocol.as_str(), "npm" | "jsr" | "workspace" | "catalog") {
            return true;
        }
    }
    value
        .split_once('@')
        .is_some_and(|(_, rest)| rest.contains(':') && !rest.contains('/'))
}

fn strip_http_user_info(value: &str) -> String {
    let Some(index) = value.find("://") else {
        return value.to_owned();
    };
    let scheme = &value[..index];
    if !scheme.eq_ignore_ascii_case("http")
        && !scheme.eq_ignore_ascii_case("https")
        && !scheme.eq_ignore_ascii_case("git+http")
        && !scheme.eq_ignore_ascii_case("git+https")
    {
        return value.to_owned();
    }
    let start = index + 3;
    let Some(end) = value[start..].find('/') else {
        return value.to_owned();
    };
    let authority = &value[start..start + end];
    let Some(at) = authority.rfind('@') else {
        return value.to_owned();
    };
    format!(
        "{}{}{}",
        &value[..start],
        &value[start + at + 1..start + end],
        &value[start + end..]
    )
}

fn absolute_path(base: &Path, path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else if cfg!(windows)
        && matches!(
            path.components().next(),
            Some(Component::RootDir | Component::Prefix(_))
        )
    {
        base.components()
            .next()
            .map(|component| {
                PathBuf::from(component.as_os_str())
                    .join(path.to_string_lossy().trim_start_matches(['\\', '/']))
            })
            .unwrap_or_else(|| path.to_path_buf())
    } else {
        base.join(path)
    };
    lexical_absolute(&joined)
}

fn resolve_path(base: &Path, path: &str) -> PathBuf {
    absolute_path(base, Path::new(path))
}

fn lexical_absolute(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
}

fn update_kind(bundle: &Map<String, Value>) -> Option<&'static str> {
    if bundle["installed"].as_bool() != Some(true)
        || bundle["availability"].as_str() != Some("profile")
        || bundle["official"].as_bool() == Some(true)
    {
        return None;
    }
    let source = bundle.get("source").and_then(Value::as_str)?;
    SemVer::parse(bundle.get("version").and_then(Value::as_str)?)?;
    if pinned_git_origin(source).is_some() {
        return Some("git");
    }
    let name = bundle.get("name").and_then(Value::as_str)?;
    let range = source.strip_prefix(name)?.strip_prefix('@')?;
    if !range.contains(':') {
        Some("registry")
    } else {
        None
    }
}

fn pinned_git_origin(spec: &str) -> Option<String> {
    let spec = spec.trim();
    let location = spec.split_once('#').map_or(spec, |(location, _)| location);
    let lower = location.to_ascii_lowercase();
    let tarball = lower.ends_with(".tgz") || lower.ends_with(".tar.gz");
    let shorthand = ["github:", "gitlab:", "bitbucket:", "gist:"]
        .iter()
        .any(|prefix| lower.starts_with(prefix));
    let git = shorthand
        || lower.starts_with("git://")
        || lower.starts_with("git+")
        || lower.starts_with("git@")
        || ((lower.starts_with("http://") || lower.starts_with("https://")) && !tarball);
    if !git {
        return None;
    }
    let (location, fragment) = spec.split_once('#')?;
    let mut path = None;
    let mut commits = 0;
    for parameter in fragment.split('&') {
        if let Some(value) = parameter.strip_prefix("path:") {
            if path.replace(value).is_some() {
                return None;
            }
        } else {
            commits += 1;
            let commit = parameter;
            if commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return None;
            }
        }
    }
    if commits != 1 {
        return None;
    }
    let mut location = location.to_owned();
    let shorthand_hosts = [
        ("github:", "github.com"),
        ("gitlab:", "gitlab.com"),
        ("bitbucket:", "bitbucket.org"),
        ("gist:", "gist.github.com"),
    ];
    if let Some((prefix, host)) = shorthand_hosts
        .iter()
        .find(|(prefix, _)| lower.starts_with(prefix))
    {
        location = format!("{host}/{}", &location[prefix.len()..]);
    }
    if let Some(index) = location.find("://") {
        location = location[index + 3..].to_owned();
    }
    if let Some(index) = location.find('@') {
        if !location[..index].contains(['/', ':']) {
            location = location[index + 1..].to_owned();
        }
    }
    if !location.contains('/') {
        if let Some(index) = location.find(':') {
            if !location[index + 1..].starts_with(|ch: char| ch.is_ascii_digit()) {
                location.replace_range(index..index + 1, "/");
            }
        }
    }
    let location = location
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .to_ascii_lowercase();
    Some(format!(
        "{location}#{}",
        path.unwrap_or("").trim_matches('/')
    ))
}

fn valid_package_name(name: &str) -> bool {
    let valid_segment = |segment: &str| {
        let mut chars = segment.chars();
        chars
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric())
            && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '~' | '-'))
    };
    if let Some((scope, name)) = name
        .strip_prefix('@')
        .and_then(|value| value.split_once('/'))
    {
        name.len() <= 214 && valid_segment(scope) && valid_segment(name)
    } else {
        name.len() <= 214 && valid_segment(name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SemVer {
    major: u64,
    minor: u64,
    patch: u64,
    prerelease: Vec<String>,
    text: String,
}

impl SemVer {
    fn parse(value: &str) -> Option<Self> {
        let (version, build) = value
            .split_once('+')
            .map_or((value, None), |(version, build)| (version, Some(build)));
        if build.is_some_and(|build| {
            build.split('.').any(|part| {
                part.is_empty()
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        }) {
            return None;
        }
        let (numbers, prerelease) = version.split_once('-').unwrap_or((version, ""));
        let mut numbers = numbers.split('.');
        let major = numbers.next()?;
        let minor = numbers.next()?;
        let patch = numbers.next()?;
        if numbers.next().is_some()
            || [major, minor, patch].iter().any(|part| {
                part.is_empty()
                    || !part.bytes().all(|byte| byte.is_ascii_digit())
                    || (part.len() > 1 && part.starts_with('0'))
            })
        {
            return None;
        }
        let major = major.parse().ok()?;
        let minor = minor.parse().ok()?;
        let patch = patch.parse().ok()?;
        let prerelease = if prerelease.is_empty() {
            Vec::new()
        } else {
            let parts = prerelease.split('.').map(str::to_owned).collect::<Vec<_>>();
            if parts.iter().any(|part| {
                part.is_empty()
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    || (part.bytes().all(|byte| byte.is_ascii_digit())
                        && part.len() > 1
                        && part.starts_with('0'))
            }) {
                return None;
            }
            parts
        };
        Some(Self {
            major,
            minor,
            patch,
            prerelease,
            text: value.to_owned(),
        })
    }

    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.major, self.minor, self.patch)
            .cmp(&(other.major, other.minor, other.patch))
            .then_with(
                || match (self.prerelease.is_empty(), other.prerelease.is_empty()) {
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => self
                        .prerelease
                        .iter()
                        .zip(&other.prerelease)
                        .map(
                            |(left, right)| match (left.parse::<u64>(), right.parse::<u64>()) {
                                (Ok(left), Ok(right)) => left.cmp(&right),
                                _ => left.cmp(right),
                            },
                        )
                        .find(|order| *order != std::cmp::Ordering::Equal)
                        .unwrap_or_else(|| self.prerelease.len().cmp(&other.prerelease.len())),
                },
            )
    }
}

#[derive(Clone, Debug)]
enum Comparator {
    Op(&'static str, SemVer),
}

fn evaluate_compatibility(
    manifest: &Value,
    exemptions: &Map<String, Value>,
    runtime: &SemVer,
) -> Result<Option<Value>, String> {
    let mut requirements = Map::new();
    if let Some(engines) = manifest.get("engines") {
        if !engines.is_object() {
            return Err("Plugin manifest engines must be an object".into());
        }
        if let Some(range) = engines.get("dsh") {
            let range = range
                .as_str()
                .ok_or_else(|| "Plugin manifest engines.dsh must be a string".to_owned())?;
            if !satisfies(runtime, range) {
                requirements.insert("engines.dsh".into(), Value::String(range.to_owned()));
            }
            return exemption_issue(manifest, exemptions, runtime, requirements);
        }
    }
    if let Some(peers) = manifest.get("peerDependencies") {
        if !peers.is_object() {
            return Err("Plugin manifest peerDependencies must be an object".into());
        }
        for (name, range) in peers.as_object().expect("object") {
            let range = range.as_str().ok_or_else(|| {
                format!("Plugin manifest peerDependencies[{name:?}] must be a string")
            })?;
            if name != "@deepseek-ai/dsh" && !name.starts_with("@deepseek-ai/dsh-") {
                continue;
            }
            let effective = if matches!(range, "workspace:^" | "workspace:~" | "workspace:*") {
                runtime.text.as_str()
            } else {
                range
            };
            if !satisfies(runtime, effective) {
                requirements.insert(name.clone(), Value::String(range.to_owned()));
            }
        }
    }
    if requirements.is_empty() {
        Ok(None)
    } else {
        exemption_issue(manifest, exemptions, runtime, requirements)
    }
}

fn exemption_issue(
    manifest: &Value,
    exemptions: &Map<String, Value>,
    runtime: &SemVer,
    requirements: Map<String, Value>,
) -> Result<Option<Value>, String> {
    if requirements.is_empty() {
        return Ok(None);
    }
    let name = manifest
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            "Plugin manifest name must be a non-empty string when dsh requirements are unmet"
                .to_owned()
        })?;
    let version = manifest
        .get("version")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            "Plugin manifest version must be a non-empty string when dsh requirements are unmet"
                .to_owned()
        })?;
    let exempted = exemptions
        .get(&format!("{name}@{version}"))
        .and_then(Value::as_array)
        .is_some_and(|versions| {
            versions
                .iter()
                .any(|value| value.as_str() == Some(runtime.text.as_str()))
        });
    Ok(Some(
        json!({"name":name, "version":version, "runtimeVersion":runtime.text, "requirements":requirements, "exempted":exempted}),
    ))
}

fn satisfies(runtime: &SemVer, range: &str) -> bool {
    if range.trim().is_empty() {
        return false;
    }
    range
        .split("||")
        .any(|alternative| satisfies_alternative(runtime, alternative.trim()).unwrap_or(false))
}

fn satisfies_alternative(runtime: &SemVer, alternative: &str) -> Option<bool> {
    if let Some((lower, upper)) = alternative.split_once(" - ") {
        return Some(partial_lower(runtime, lower.trim()) && partial_upper(runtime, upper.trim()));
    }
    let normalized = alternative.replace(',', " ");
    let mut comparators = Vec::new();
    for token in normalized.split_whitespace() {
        if let Some(rest) = token.strip_prefix('^') {
            let lower = SemVer::parse(rest).or_else(|| partial_version(rest))?;
            let upper = if lower.major > 0 {
                SemVer {
                    major: lower.major + 1,
                    minor: 0,
                    patch: 0,
                    prerelease: Vec::new(),
                    text: String::new(),
                }
            } else if lower.minor > 0 || !rest.trim_start_matches(['v', '=']).starts_with("0.0.") {
                SemVer {
                    major: 0,
                    minor: lower.minor + 1,
                    patch: 0,
                    prerelease: Vec::new(),
                    text: String::new(),
                }
            } else {
                SemVer {
                    major: 0,
                    minor: 0,
                    patch: lower.patch + 1,
                    prerelease: Vec::new(),
                    text: String::new(),
                }
            };
            comparators.push(Comparator::Op(">=", lower.clone()));
            comparators.push(Comparator::Op("<", upper));
        } else if let Some(rest) = token.strip_prefix('~') {
            let lower = SemVer::parse(rest).or_else(|| partial_version(rest))?;
            comparators.push(Comparator::Op(">=", lower.clone()));
            comparators.push(Comparator::Op(
                "<",
                SemVer {
                    major: lower.major,
                    minor: lower.minor + 1,
                    patch: 0,
                    prerelease: Vec::new(),
                    text: String::new(),
                },
            ));
        } else if let Some(rest) = token.strip_prefix(">=") {
            comparators.push(Comparator::Op(
                ">=",
                SemVer::parse(rest).or_else(|| partial_version(rest))?,
            ));
        } else if let Some(rest) = token.strip_prefix("<=") {
            comparators.push(Comparator::Op(
                "<=",
                SemVer::parse(rest).or_else(|| partial_version(rest))?,
            ));
        } else if let Some(rest) = token.strip_prefix('>') {
            comparators.push(Comparator::Op(
                ">",
                SemVer::parse(rest).or_else(|| partial_version(rest))?,
            ));
        } else if let Some(rest) = token.strip_prefix('<') {
            comparators.push(Comparator::Op(
                "<",
                SemVer::parse(rest).or_else(|| partial_version(rest))?,
            ));
        } else if let Some(rest) = token.strip_prefix('=') {
            comparators.push(Comparator::Op(
                "=",
                SemVer::parse(rest).or_else(|| partial_version(rest))?,
            ));
        } else if matches!(token, "*" | "x" | "X") {
            continue;
        } else if !partial_match(runtime, token) {
            return Some(false);
        }
    }
    Some(comparators.iter().all(|comparator| match comparator {
        Comparator::Op(op, version) => match *op {
            ">=" => runtime.cmp(version) != std::cmp::Ordering::Less,
            "<=" => runtime.cmp(version) != std::cmp::Ordering::Greater,
            ">" => runtime.cmp(version) == std::cmp::Ordering::Greater,
            "<" => runtime.cmp(version) == std::cmp::Ordering::Less,
            "=" => runtime.cmp(version) == std::cmp::Ordering::Equal,
            _ => false,
        },
    }))
}

fn partial_version(value: &str) -> Option<SemVer> {
    let mut parts = value
        .trim_start_matches(['v', '='])
        .split(['.', '-', '*', 'x', 'X']);
    let major = parts.next()?;
    if major.is_empty() {
        return None;
    }
    let major = major.parse().ok()?;
    let minor = parts.next().unwrap_or("0");
    let patch = parts.next().unwrap_or("0");
    if minor == "*" || minor == "x" || minor == "X" || patch == "*" || patch == "x" || patch == "X"
    {
        return Some(SemVer {
            major,
            minor: 0,
            patch: 0,
            prerelease: Vec::new(),
            text: String::new(),
        });
    }
    Some(SemVer {
        major,
        minor: minor.parse().ok()?,
        patch: patch.parse().ok()?,
        prerelease: Vec::new(),
        text: String::new(),
    })
}

fn partial_match(runtime: &SemVer, value: &str) -> bool {
    let value = value.trim().trim_start_matches(['v', '=']);
    if value.is_empty() {
        return false;
    }
    if let Some(exact) = SemVer::parse(value) {
        return runtime.cmp(&exact) == std::cmp::Ordering::Equal;
    }
    partial_components(value).is_some_and(|parts| {
        parts
            .iter()
            .zip([runtime.major, runtime.minor, runtime.patch])
            .all(|(part, actual)| *part == actual)
    })
}

fn partial_lower(runtime: &SemVer, value: &str) -> bool {
    let value = value.trim().trim_start_matches(['v', '=']);
    let base = value.split('-').next().unwrap_or(value);
    let Some(parts) = partial_components(base) else {
        return false;
    };
    for (part, actual) in parts
        .iter()
        .zip([runtime.major, runtime.minor, runtime.patch])
    {
        match part.cmp(&actual) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }
    }
    value.contains('-')
        && parts.len() == 3
        && SemVer::parse(value).is_some_and(|lower| runtime.cmp(&lower) != std::cmp::Ordering::Less)
}

fn partial_upper(runtime: &SemVer, value: &str) -> bool {
    let value = value.trim().trim_start_matches(['v', '=']);
    if let Some(exact) = SemVer::parse(value) {
        return runtime.cmp(&exact) != std::cmp::Ordering::Greater;
    }
    let base = value.split('-').next().unwrap_or(value);
    let Some(parts) = partial_components(base) else {
        return false;
    };
    let upper = match parts.as_slice() {
        [] => return true,
        [major] => SemVer {
            major: major.saturating_add(1),
            minor: 0,
            patch: 0,
            prerelease: Vec::new(),
            text: String::new(),
        },
        [major, minor] => SemVer {
            major: *major,
            minor: minor.saturating_add(1),
            patch: 0,
            prerelease: Vec::new(),
            text: String::new(),
        },
        [major, minor, patch] => SemVer {
            major: *major,
            minor: *minor,
            patch: *patch,
            prerelease: Vec::new(),
            text: String::new(),
        },
        _ => return false,
    };
    if parts.len() == 3 {
        runtime.cmp(&upper) != std::cmp::Ordering::Greater
    } else {
        runtime.cmp(&upper) == std::cmp::Ordering::Less
    }
}

fn partial_components(value: &str) -> Option<Vec<u64>> {
    let mut components = Vec::new();
    let mut wildcard = false;
    for part in value.split('.') {
        if wildcard {
            return None;
        }
        if matches!(part, "*" | "x" | "X") {
            wildcard = true;
            continue;
        }
        if part.is_empty()
            || !part.bytes().all(|byte| byte.is_ascii_digit())
            || (part.len() > 1 && part.starts_with('0'))
        {
            return None;
        }
        components.push(part.parse().ok()?);
    }
    Some(components)
}

fn read_plugin_meta(specifier: &str, base_dir: &Path) -> Result<Value, String> {
    let Some(package_name) = bare_package_name(specifier) else {
        return Ok(Value::Null);
    };
    let result = (|| -> Result<Value, String> {
        let package_dir = resolve_metadata_package(specifier, package_name, base_dir)?;
        let manifest_path = exported_resource(&package_dir, "package.json")?;
        let manifest = match manifest_path {
            Some(path) => Some(read_object_value(&path)?),
            None => None,
        };
        let english = exported_resource(&package_dir, "locale/en.json")?;
        let dictionaries = match english {
            Some(path) if path.is_file() => read_locale_dictionaries(&package_dir, &path)?,
            _ => Map::new(),
        };
        let fallback_title = manifest
            .as_ref()
            .and_then(|manifest| nonempty_string(manifest.get("name")));
        let fallback_description = manifest
            .as_ref()
            .and_then(|manifest| nonempty_string(manifest.get("description")));
        let title = localized_text(&dictionaries, "title", fallback_title, specifier);
        let description = localized_text(&dictionaries, "description", fallback_description, "");
        let mut meta = Map::new();
        if let Some(title) = &title {
            meta.insert("title".into(), title.clone());
        }
        if let Some(description) = &description {
            meta.insert("description".into(), description.clone());
        }
        match metadata_icon(&package_dir, specifier, package_name, manifest.as_ref()) {
            Ok(icon) => {
                if let Some(icon) = icon {
                    meta.insert("icon".into(), Value::String(icon));
                }
            }
            Err(error) => {
                if meta.is_empty() {
                    return Ok(
                        json!({"error":format!("Plugin metadata for {specifier}: {error}")}),
                    );
                }
                meta.insert(
                    "error".into(),
                    Value::String(format!("Plugin metadata for {specifier}: {error}")),
                );
            }
        }
        if meta.is_empty() {
            return Ok(Value::Null);
        }
        Ok(Value::Object(meta))
    })();
    match result {
        Ok(meta) => Ok(meta),
        Err(error) => Ok(json!({"error":format!("Plugin metadata for {specifier}: {error}")})),
    }
}

fn bare_package_name(specifier: &str) -> Option<&str> {
    if specifier.is_empty()
        || specifier.starts_with(['.', '/', '\\', '#'])
        || specifier.contains(':')
    {
        return None;
    }
    if let Some(rest) = specifier.strip_prefix('@') {
        let first = rest.find('/')?;
        let end = rest[first + 1..]
            .find('/')
            .map(|index| first + 1 + index)
            .unwrap_or(rest.len());
        Some(&specifier[..end + 1])
    } else {
        Some(specifier.split('/').next()?)
    }
}

fn resolve_metadata_package(
    _specifier: &str,
    package_name: &str,
    base_dir: &Path,
) -> Result<PathBuf, String> {
    if base_dir.join("package.json").is_file() {
        if let Ok(manifest) = read_object_value(&base_dir.join("package.json")) {
            if manifest.get("name").and_then(Value::as_str) == Some(package_name) {
                return Ok(base_dir.to_path_buf());
            }
        }
    }
    package_dir_from_anchor(&base_dir.join("package.json"), package_name).ok_or_else(|| {
        format!(
            "cannot resolve package {package_name} from {}",
            base_dir.display()
        )
    })
}

fn exported_resource(package_dir: &Path, resource: &str) -> Result<Option<PathBuf>, String> {
    let request = format!("./{resource}");
    let manifest = read_object_value(&package_dir.join("package.json"))?;
    let Some(exports) = manifest.get("exports") else {
        return Ok(Some(resolve_path(package_dir, resource)));
    };
    let Some(target) = export_target(exports, &request) else {
        return Ok(None);
    };
    if target.starts_with("../") || target == ".." {
        return Err(format!("package resource {request} escapes its package"));
    }
    if Path::new(&target).is_absolute() {
        return Err(format!("package resource {request} must be relative"));
    }
    Ok(Some(resolve_path(package_dir, &target)))
}

fn export_target(value: &Value, request: &str) -> Option<String> {
    match value {
        Value::String(target) => Some(target.to_owned()),
        Value::Array(values) => values
            .iter()
            .find_map(|value| export_target(value, request)),
        Value::Object(object) => {
            if let Some(target) = object.get(request) {
                return export_target(target, request);
            }
            for (key, value) in object {
                if key.starts_with('.') {
                    let Some((prefix, suffix)) = key.split_once('*') else {
                        continue;
                    };
                    if request.starts_with(prefix) && request.ends_with(suffix) {
                        let middle = &request[prefix.len()..request.len() - suffix.len()];
                        return substitute_export(value, middle);
                    }
                }
            }
            object
                .get("default")
                .or_else(|| object.get("import"))
                .or_else(|| object.get("node"))
                .or_else(|| object.get("require"))
                .and_then(|value| export_target(value, request))
        }
        _ => None,
    }
}

fn substitute_export(value: &Value, middle: &str) -> Option<String> {
    match value {
        Value::String(target) => Some(target.replace('*', middle)),
        Value::Array(values) => values
            .iter()
            .find_map(|value| substitute_export(value, middle)),
        Value::Object(object) => object
            .get("default")
            .or_else(|| object.get("import"))
            .and_then(|value| substitute_export(value, middle)),
        _ => None,
    }
}

fn read_locale_dictionaries(
    package_dir: &Path,
    english: &Path,
) -> Result<Map<String, Value>, String> {
    let directory = english
        .parent()
        .ok_or_else(|| "English locale has no directory".to_owned())?;
    let mut dictionaries = Map::new();
    for entry in fs::read_dir(directory).map_err(|error| {
        format!(
            "failed to read locale directory {}: {error}",
            directory.display()
        )
    })? {
        let entry = entry.map_err(|error| format!("failed to read locale directory: {error}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(language) = name.strip_suffix(".json") else {
            continue;
        };
        if !valid_language(language) {
            return Err(format!(
                "locale/{name} must use a language id as its filename"
            ));
        }
        let language = language.to_ascii_lowercase();
        if dictionaries.contains_key(&language) {
            return Err(format!("locale/{name} duplicates locale {language}"));
        }
        let path = exported_resource(package_dir, &format!("locale/{name}"))?
            .ok_or_else(|| format!("locale/{name} is not exported"))?;
        if path.parent() != Some(directory) {
            return Err(format!(
                "locale/{name} must share the English locale directory {}",
                directory.display()
            ));
        }
        let value = read_object_value(&path)?;
        let meta = value.get("meta");
        if meta.is_some_and(|meta| !meta.is_object()) {
            return Err(format!("{}: meta must be an object", path.display()));
        }
        let mut fields = Map::new();
        for field in ["title", "description"] {
            if let Some(text) = meta
                .and_then(|meta| meta.get(field))
                .filter(|value| !value.is_null())
            {
                let text = text
                    .as_str()
                    .filter(|text| !text.trim().is_empty())
                    .ok_or_else(|| {
                        format!(
                            "{}: meta.{field} must be a non-empty string",
                            path.display()
                        )
                    })?;
                fields.insert(field.into(), Value::String(text.to_owned()));
            }
        }
        dictionaries.insert(language, Value::Object(fields));
    }
    Ok(dictionaries)
}

fn valid_language(value: &str) -> bool {
    let mut parts = value.split('-');
    parts.next().is_some_and(|part| {
        (2..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_alphabetic())
    }) && parts.all(|part| {
        (1..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

fn nonempty_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn localized_text(
    dictionaries: &Map<String, Value>,
    field: &str,
    fallback: Option<&str>,
    final_fallback: &str,
) -> Option<Value> {
    let mut translations = Map::new();
    for (language, fields) in dictionaries {
        if let Some(text) = fields.get(field) {
            translations.insert(language.clone(), text.clone());
        }
    }
    if translations.is_empty() {
        return fallback.map(|text| Value::String(text.to_owned()));
    }
    if dictionaries.len() == 1 {
        if let Some(text) = translations.get("en") {
            return Some(text.clone());
        }
    }
    let english = translations
        .get("en")
        .cloned()
        .unwrap_or_else(|| Value::String(fallback.unwrap_or(final_fallback).to_owned()));
    let mut result = Map::new();
    result.insert("en".into(), english.clone());
    for language in dictionaries.keys() {
        result
            .entry(language.clone())
            .or_insert_with(|| english.clone());
    }
    result.extend(translations);
    Some(Value::Object(result))
}

fn metadata_icon(
    package_dir: &Path,
    specifier: &str,
    package_name: &str,
    manifest: Option<&Value>,
) -> Result<Option<String>, String> {
    let declared = manifest.and_then(|manifest| manifest.get("icon"));
    let target = if let Some(icon) = declared {
        let icon = icon
            .as_str()
            .filter(|icon| !icon.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "{}: icon must be a non-empty string",
                    package_dir.join("package.json").display()
                )
            })?;
        if Path::new(icon).is_absolute() || icon.contains(':') || icon.starts_with('\\') {
            return Err(format!(
                "{}: icon must be a relative file path",
                package_dir.join("package.json").display()
            ));
        }
        Some(resolve_path(package_dir, icon))
    } else {
        let relative = specifier
            .strip_prefix(package_name)
            .unwrap_or_default()
            .trim_start_matches('/');
        match exported_resource(package_dir, &format!("{relative}icon"))? {
            Some(path) if path.is_file() => Some(path),
            Some(_) | None => return Ok(None),
        }
    };
    let Some(target) = target else {
        return Ok(None);
    };
    let canonical =
        fs::canonicalize(&target).map_err(|error| format!("{}: {error}", target.display()))?;
    let root = fs::canonicalize(package_dir)
        .map_err(|error| format!("{}: {error}", package_dir.display()))?;
    if !canonical.starts_with(&root) {
        return Err(format!(
            "{}: icon must remain inside its package directory",
            target.display()
        ));
    }
    let media = match target
        .extension()
        .and_then(OsStr::to_str)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        _ => {
            return Err(format!(
                "{}: icon must be SVG, PNG, JPEG, or WebP",
                target.display()
            ))
        }
    };
    let metadata =
        fs::metadata(&canonical).map_err(|error| format!("{}: {error}", target.display()))?;
    if !metadata.is_file() {
        return Err(format!("{}: icon must be a regular file", target.display()));
    }
    if metadata.len() > MAX_ICON_BYTES {
        return Err(format!("{}: icon exceeds 256 KiB", target.display()));
    }
    let bytes = fs::read(&canonical).map_err(|error| format!("{}: {error}", target.display()))?;
    if bytes.len() as u64 > MAX_ICON_BYTES {
        return Err(format!("{}: icon exceeds 256 KiB", target.display()));
    }
    Ok(Some(format!("data:{media};base64,{}", base64(&bytes))))
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (chunk[0] as u32) << 16
            | (chunk.get(1).copied().unwrap_or(0) as u32) << 8
            | chunk.get(2).copied().unwrap_or(0) as u32;
        output.push(ALPHABET[(value >> 18) as usize & 63] as char);
        output.push(ALPHABET[(value >> 12) as usize & 63] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[(value >> 6) as usize & 63] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[value as usize & 63] as char
        } else {
            '='
        });
    }
    output
}

fn declared_rows(
    paths: &[PathBuf],
    directory: &Path,
    config: &BundleListConfig,
) -> Result<(Vec<Value>, Vec<String>), String> {
    let mut patches = Vec::new();
    for path in paths {
        let text = fs::read_to_string(path)
            .map_err(|error| format!("failed to read bundle patch {}: {error}", path.display()))?;
        let value = parse_patch(&text)
            .map_err(|error| format!("invalid bundle patch {}: {error}", path.display()))?;
        let Value::Array(items) = value else {
            return Err(format!("{} must contain a patch list", path.display()));
        };
        patches.extend(items);
    }
    let mut rows = Vec::new();
    let mut positions = BTreeMap::new();
    for patch in &patches {
        if let Some(insert) = patch.get("insert").and_then(Value::as_array) {
            collect_inserted(insert, directory, config, &mut rows, &mut positions);
        }
    }
    let declared = rows
        .iter()
        .filter_map(|row| row["rowId"].as_str())
        .collect::<HashSet<_>>();
    let mut overrides = Vec::new();
    let mut seen = HashSet::new();
    for patch in &patches {
        if patch.get("insert").is_none() {
            if let Some(id) = patch.get("id").and_then(Value::as_str) {
                if !declared.contains(id) && seen.insert(id.to_owned()) {
                    overrides.push(id.to_owned());
                }
            }
        }
    }
    Ok((rows, overrides))
}

fn collect_inserted(
    items: &[Value],
    directory: &Path,
    config: &BundleListConfig,
    rows: &mut Vec<Value>,
    positions: &mut BTreeMap<String, usize>,
) {
    for item in items {
        if let Some(children) = item.get("insert").and_then(Value::as_array) {
            collect_inserted(children, directory, config, rows, positions);
        }
        let Some(row_id) = item.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(module_name) = item.get("name").and_then(Value::as_str) else {
            continue;
        };
        if item.get("group").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let module_name = module_name.to_owned();
        let mut row = Map::new();
        row.insert("rowId".into(), Value::String(row_id.to_owned()));
        row.insert("moduleName".into(), Value::String(module_name.clone()));
        if let Some(meta) = read_plugin_meta(&module_name, directory)
            .ok()
            .filter(|meta| !meta.is_null())
        {
            row.insert("meta".into(), meta);
        }
        if let Some(entry) = config.live_entries.get(row_id) {
            row.insert("entryId".into(), Value::String(entry.clone()));
        }
        let value = Value::Object(row);
        if let Some(position) = positions.get(row_id) {
            rows[*position] = value;
        } else {
            positions.insert(row_id.to_owned(), rows.len());
            rows.push(value);
        }
    }
}

#[cfg(test)]
fn file_url(path: &Path) -> String {
    let text = path.to_string_lossy().replace('\\', "/");
    if text.starts_with("//") {
        return format!("file:{text}");
    }
    let mut output = String::from("file:///");
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~:".contains(&byte) {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

/// Parse the data-only YAML subset used by Cordis patch lists. JavaScript tags
/// remain inert strings; no expression is evaluated. JSON patch lists work too.
fn parse_patch(text: &str) -> Result<Value, String> {
    let text = text.trim_start_matches('\u{feff}');
    if text.trim_start().starts_with('[') {
        return serde_json::from_str(text.trim()).map_err(|error| error.to_string());
    }
    let lines = text
        .lines()
        .enumerate()
        .map(|(index, line)| yaml_line(line, index + 1))
        .collect::<Result<Vec<_>, _>>()?;
    let mut parser = YamlParser { lines, index: 0 };
    if parser.lines.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    let value = parser.block(0)?;
    if parser.index != parser.lines.len() {
        return Err(format!(
            "unexpected indentation at line {}",
            parser.lines[parser.index].number
        ));
    }
    Ok(value)
}

#[derive(Debug)]
struct YamlLine {
    number: usize,
    indent: usize,
    text: String,
}

fn yaml_line(line: &str, number: usize) -> Result<YamlLine, String> {
    let mut indent = 0;
    for ch in line.chars() {
        if ch == ' ' {
            indent += 1;
        } else if ch == '\t' {
            return Err(format!("tabs cannot indent YAML at line {number}"));
        } else {
            break;
        }
    }
    let content = &line[indent..];
    let mut quote = None;
    let mut escaped = false;
    let mut end = content.len();
    for (index, ch) in content.char_indices() {
        if let Some(active) = quote {
            if active == '"' && escaped {
                escaped = false;
                continue;
            }
            if active == '"' && ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == active {
                quote = None;
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch == '#' && (index == 0 || content.as_bytes()[index - 1].is_ascii_whitespace()) {
            end = index;
            break;
        }
    }
    let text = content[..end].trim_end().to_owned();
    if text.is_empty() || text == "---" || text == "..." {
        return Ok(YamlLine {
            number,
            indent: usize::MAX,
            text: String::new(),
        });
    }
    Ok(YamlLine {
        number,
        indent,
        text,
    })
}

struct YamlParser {
    lines: Vec<YamlLine>,
    index: usize,
}

impl YamlParser {
    fn block(&mut self, indent: usize) -> Result<Value, String> {
        while self
            .lines
            .get(self.index)
            .is_some_and(|line| line.indent == usize::MAX)
        {
            self.index += 1;
        }
        let Some(line) = self.lines.get(self.index) else {
            return Ok(Value::Null);
        };
        if line.indent != indent {
            return Err(format!("unexpected indentation at line {}", line.number));
        }
        if line.text == "-" || line.text.starts_with("- ") {
            self.sequence(indent)
        } else {
            self.mapping(indent)
        }
    }

    fn sequence(&mut self, indent: usize) -> Result<Value, String> {
        let mut items = Vec::new();
        loop {
            while self
                .lines
                .get(self.index)
                .is_some_and(|line| line.indent == usize::MAX)
            {
                self.index += 1;
            }
            let Some(line) = self.lines.get(self.index) else {
                break;
            };
            if line.indent != indent || !(line.text == "-" || line.text.starts_with("- ")) {
                break;
            }
            let text = line.text[1..].trim_start().to_owned();
            let number = line.number;
            self.index += 1;
            if text.is_empty() {
                items.push(self.nested(indent)?);
            } else if let Some((key, value)) =
                split_mapping(&text).map(|(key, value)| (key.to_owned(), value.to_owned()))
            {
                let mut map = Map::new();
                map.insert(key, self.mapping_value(&value, indent + 2, number)?);
                self.mapping_tail(&mut map, indent + 2)?;
                items.push(Value::Object(map));
            } else {
                items.push(YamlParser::scalar(&text)?);
            }
        }
        Ok(Value::Array(items))
    }

    fn mapping(&mut self, indent: usize) -> Result<Value, String> {
        let mut map = Map::new();
        self.mapping_tail(&mut map, indent)?;
        Ok(Value::Object(map))
    }

    fn mapping_tail(&mut self, map: &mut Map<String, Value>, indent: usize) -> Result<(), String> {
        loop {
            while self
                .lines
                .get(self.index)
                .is_some_and(|line| line.indent == usize::MAX)
            {
                self.index += 1;
            }
            let Some(line) = self.lines.get(self.index) else {
                break;
            };
            if line.indent != indent || line.text == "-" || line.text.starts_with("- ") {
                break;
            }
            let Some((key, value)) =
                split_mapping(&line.text).map(|(key, value)| (key.to_owned(), value.to_owned()))
            else {
                return Err(format!("expected a mapping at line {}", line.number));
            };
            let number = line.number;
            self.index += 1;
            map.insert(key, self.mapping_value(&value, indent, number)?);
        }
        Ok(())
    }

    fn mapping_value(&mut self, text: &str, indent: usize, number: usize) -> Result<Value, String> {
        if text.is_empty() {
            return self.nested(indent);
        }
        if text == "|" || text == ">" {
            let mut values = Vec::new();
            while self
                .lines
                .get(self.index)
                .is_some_and(|line| line.indent != usize::MAX && line.indent > indent)
            {
                values.push(self.lines[self.index].text.clone());
                self.index += 1;
            }
            return Ok(Value::String(values.join(if text == ">" {
                " "
            } else {
                "\n"
            })));
        }
        Self::scalar(text).map_err(|error| format!("line {number}: {error}"))
    }

    fn nested(&mut self, indent: usize) -> Result<Value, String> {
        while self
            .lines
            .get(self.index)
            .is_some_and(|line| line.indent == usize::MAX)
        {
            self.index += 1;
        }
        let nested_indent = self.lines.get(self.index).map(|line| line.indent);
        if nested_indent.is_some_and(|nested| nested > indent) {
            self.block(nested_indent.expect("nested indentation"))
        } else {
            Ok(Value::Null)
        }
    }

    fn scalar(text: &str) -> Result<Value, String> {
        let text = text.trim();
        if text.starts_with('"') {
            return serde_json::from_str(text).map_err(|error| error.to_string());
        }
        if text.starts_with('\'') {
            if !text.ends_with('\'') || text.len() < 2 {
                return Err("unterminated quoted string".into());
            }
            return Ok(Value::String(text[1..text.len() - 1].replace("''", "'")));
        }
        if text.starts_with("!!js ") || text.starts_with('!') {
            return Ok(Value::String(text.to_owned()));
        }
        if let Ok(value) = serde_json::from_str::<Value>(text) {
            return Ok(value);
        }
        Ok(Value::String(text.to_owned()))
    }
}

fn split_mapping(text: &str) -> Option<(&str, &str)> {
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in text.char_indices() {
        if let Some(active) = quote {
            if active == '"' && escaped {
                escaped = false;
                continue;
            }
            if active == '"' && ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == active {
                quote = None;
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch == ':'
            && (index + 1 == text.len() || text.as_bytes()[index + 1].is_ascii_whitespace())
        {
            return Some((text[..index].trim(), text[index + 1..].trim()));
        }
    }
    None
}

#[cfg(test)]
mod tests;
