//! Updater configuration checks shared by the tray and contract tests.

use serde_json::Value;
use tauri_plugin_updater::Config;

/// Whether an endpoint and a non-empty verification key have been supplied.
///
/// This only checks configuration presence. Cryptographic validation happens when the
/// updater plugin builds and checks an update; failures are reported to the user.
pub fn configured(settings: &Value) -> bool {
    serde_json::from_value::<Config>(settings.clone())
        .is_ok_and(|config| !config.endpoints.is_empty() && !config.pubkey.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shipped_scaffold_is_disabled() {
        let config: Value = serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        let settings = &config["plugins"]["updater"];
        let parsed: Config = serde_json::from_value(settings.clone()).unwrap();
        assert!(parsed.endpoints.is_empty());
        assert!(parsed.pubkey.is_empty());
        assert!(!configured(settings));
    }

    #[test]
    fn both_endpoint_and_key_are_required() {
        assert!(!configured(&json!({ "pubkey": "test-key" })));
        assert!(!configured(&json!({
            "pubkey": "",
            "endpoints": ["https://updates.example.com/manifest.json"]
        })));
        assert!(!configured(&json!({
            "pubkey": "test-key",
            "endpoints": ["not a URL"]
        })));
        assert!(configured(&json!({
            "pubkey": "test-key",
            "endpoints": ["https://updates.example.com/manifest.json"]
        })));
    }
}
