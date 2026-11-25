//! Imported OpenAI-compatible provider profiles from the installed desktop app.

use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

const PI_AI_NS: &str = "llm-pi-ai";
const DEFAULT_MODEL_NS: &str = "agent-default-model";

/// Provider settings fields consumed by the Rust OpenAI-compatible route.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderProfile {
    pub display_name: Option<String>,
    pub api_key_env: Option<String>,
    pub api: Option<String>,
    #[serde(rename = "baseURL")]
    pub base_url: Option<String>,
    #[serde(default)]
    pub models: Vec<Value>,
    pub compat: Option<Value>,
}

impl ProviderProfile {
    pub fn name(&self, id: &str) -> String {
        self.display_name.clone().unwrap_or_else(|| id.to_owned())
    }

    pub fn model(&self, id: &str) -> Option<&Value> {
        self.models
            .iter()
            .find(|model| model["id"].as_str() == Some(id))
    }
}

/// Homes to search for the user's own profile, in priority order.
///
/// A build can run under its own `DSH_HOME` (the portable package, a test
/// harness, a second install) while the models the user configured live in the
/// standard `~/.dsh`. Reading both keeps their providers available in every
/// build instead of only in the one that happened to create that home.
fn profile_homes(home: &Path) -> Vec<std::path::PathBuf> {
    let mut homes = vec![home.to_path_buf()];
    // A scratch home is a test harness, not a shipped build: it must not
    // pick up the developer's own credentials or providers.
    if home.starts_with(std::env::temp_dir()) {
        return homes;
    }
    let standard = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(std::path::PathBuf::from)
        .map(|home| home.join(".dsh"));
    if let Some(standard) = standard {
        if !homes.contains(&standard) {
            homes.push(standard);
        }
    }
    homes
}

/// Read the original Cordis profile as Rust namespace defaults, without importing credentials.
pub fn legacy_profile_defaults(home: &Path) -> BTreeMap<String, Value> {
    profile_homes(home)
        .into_iter()
        .find_map(|home| read_profile_defaults(&home))
        .unwrap_or_default()
}

fn read_profile_defaults(home: &Path) -> Option<BTreeMap<String, Value>> {
    let path = home.join("profiles/desktop/cordis.patch.yml");
    let text = std::fs::read_to_string(path).ok()?;
    let document = serde_yaml::from_str::<serde_yaml::Value>(&text).ok()?;
    let entries = document.as_sequence()?;

    let mut defaults = BTreeMap::new();
    for entry in entries {
        let Some(id) = yaml_text(entry.get("id")) else {
            continue;
        };
        let Some(config) = entry.get("config") else {
            continue;
        };
        match id {
            "llm-pi-ai" => {
                if let Some(providers) = config.get("providers").and_then(yaml_json) {
                    defaults.insert(PI_AI_NS.into(), json!({"providers": providers}));
                }
            }
            "agent-default-model" => {
                if let Some(selection) = yaml_json(config) {
                    defaults.insert(DEFAULT_MODEL_NS.into(), selection);
                }
            }
            _ => {}
        }
    }
    if defaults.is_empty() {
        return None;
    }
    Some(defaults)
}

/// Environment variables take precedence; the installed app's managed refs are the fallback.
/// Values are returned only to the request adapter and are never included in diagnostics.
pub fn resolve_credential(home: &Path, reference: &str) -> Option<String> {
    if let Ok(value) = std::env::var(reference) {
        if !value.is_empty() {
            return Some(value);
        }
    }
    profile_homes(home).into_iter().find_map(|home| {
        let text = std::fs::read_to_string(home.join(".credentials.yaml")).ok()?;
        let document = serde_yaml::from_str::<serde_yaml::Value>(&text).ok()?;
        document
            .get("refs")?
            .get(reference)?
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

/// Decode the effective `llm-pi-ai` settings into provider routes.
pub fn providers(settings: &Value) -> BTreeMap<String, ProviderProfile> {
    let Some(providers) = settings["providers"].as_object() else {
        return BTreeMap::new();
    };
    providers
        .iter()
        .filter_map(|(id, profile)| {
            serde_json::from_value::<ProviderProfile>(profile.clone())
                .ok()
                .map(|profile| (id.clone(), profile))
        })
        .collect()
}

/// Model catalog shaped for `session/modelCatalog`; duplicate profile rows collapse by model id.
pub fn model_catalog(
    settings: &Value,
    default_selection: &Value,
    mut credential_configured: impl FnMut(&str) -> bool,
) -> Value {
    let providers = providers(settings);
    let mut groups = Vec::new();
    for (id, profile) in providers {
        if profile.api.as_deref() != Some("openai-completions") {
            continue;
        }
        if !profile
            .api_key_env
            .as_deref()
            .is_some_and(&mut credential_configured)
        {
            continue;
        }
        let mut seen = HashSet::new();
        let models: Vec<Value> = profile
            .models
            .iter()
            .filter_map(|model| {
                let model_id = model["id"].as_str()?;
                if !seen.insert(model_id.to_owned()) {
                    return None;
                }
                let name = model["name"].as_str().unwrap_or(model_id);
                Some(json!({"id": model_id, "name": name}))
            })
            .collect();
        if !models.is_empty() {
            groups.push(json!({"id": id, "name": profile.name(&id), "models": models}));
        }
    }
    let routable: Vec<String> = groups
        .iter()
        .filter_map(|group| group["id"].as_str().map(str::to_owned))
        .collect();
    let mut selected = Map::new();
    if let Some(provider) = default_selection["provider"].as_str() {
        selected.insert("provider".into(), json!(provider));
    }
    if let Some(model) = default_selection["model"].as_str() {
        selected.insert("model".into(), json!(model));
    }
    if let Some(effort) = default_selection["reasoningEffort"].as_str() {
        selected.insert("reasoningEffort".into(), json!(effort));
    }
    json!({
        "default": selected,
        "routableProviders": routable,
        "groups": groups,
        "failures": [],
    })
}

/// Provider list shaped for `llm/listProviders`.
pub fn provider_list(settings: &Value) -> Value {
    let mut entries = vec![
        json!({"id": "deepseek-official", "name": "DeepSeek API"}),
        json!({"id": "deepseek-account", "name": "DeepSeek Account"}),
    ];
    for (id, profile) in providers(settings) {
        if profile.api.as_deref() == Some("openai-completions") {
            entries.push(json!({"id": id, "name": profile.name(&id)}));
        }
    }
    json!(entries)
}

/// One provider's configured models in discovery response shape.
pub fn discovered_models(profile: &ProviderProfile) -> Vec<Value> {
    let mut seen = HashSet::new();
    profile
        .models
        .iter()
        .filter_map(|model| {
            let id = model["id"].as_str()?;
            if !seen.insert(id.to_owned()) {
                return None;
            }
            let mut entry = json!({"id": id});
            for field in ["name", "contextWindow", "maxTokens"] {
                if let Some(value) = model.get(field) {
                    entry[field] = value.clone();
                }
            }
            if let Some(input) = model["input"].as_array() {
                entry["inputModalities"] = json!(input);
            }
            Some(entry)
        })
        .collect()
}

fn yaml_text(value: Option<&serde_yaml::Value>) -> Option<&str> {
    value.and_then(serde_yaml::Value::as_str)
}

fn yaml_json(value: &serde_yaml::Value) -> Option<Value> {
    serde_json::to_value(value).ok()
}
