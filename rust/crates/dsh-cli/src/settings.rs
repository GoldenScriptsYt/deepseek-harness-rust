//! User settings: namespaces with a schema, defaults (`base`), user overrides (`user`) and a
//! revision. Port of the read/write half of `packages/settings/settings`, including validation
//! against the captured Schemastery schema before a mutation is persisted.
//!
//! The namespace catalog (schemas, defaults, `applies`, `secrets`) is the TypeScript Host's
//! `settings/describe`, captured in `assets/rpc/settings_describe.json`. Only user
//! overrides and revisions are stored, in `<home>/settings.rust.json`.

use dsh_gateway::RpcError;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

#[path = "settings_schema.rs"]
mod settings_schema;

pub struct SettingsStore {
    file: PathBuf,
    templates: Vec<Value>,
    state: Mutex<BTreeMap<String, (Value, u64)>>,
}

/// Recursively overlay `user` on `base`; non-object values replace.
fn merge(base: &Value, user: &Value) -> Value {
    match (base, user) {
        (Value::Object(b), Value::Object(u)) => {
            let mut out = b.clone();
            for (k, v) in u {
                let merged = out.get(k).map_or_else(|| v.clone(), |bv| merge(bv, v));
                out.insert(k.clone(), merged);
            }
            Value::Object(out)
        }
        (_, u) => u.clone(),
    }
}

fn set_path(root: &mut Value, path: &[String], value: Value) -> Result<(), String> {
    let Some((head, rest)) = path.split_first() else {
        *root = value;
        return Ok(());
    };
    if let Some(array) = root.as_array_mut() {
        let index = settings_schema::array_index(head)
            .ok_or_else(|| format!("invalid settings array index \"{head}\""))?;
        if index > array.len() || (index == array.len() && !rest.is_empty()) {
            return Err(format!("settings array index \"{head}\" is out of range"));
        }
        if index == array.len() {
            array.push(value);
        } else {
            set_path(&mut array[index], rest, value)?;
        }
        return Ok(());
    }
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }
    let child = root
        .as_object_mut()
        .expect("object")
        .entry(head.clone())
        .or_insert(Value::Null);
    set_path(child, rest, value)
}

/// Remove `path`, pruning objects left empty by the removal.
fn unset_path(root: &mut Value, path: &[String]) -> Result<(), String> {
    let Some((head, rest)) = path.split_first() else {
        return Ok(());
    };
    if let Some(array) = root.as_array_mut() {
        let index = settings_schema::array_index(head)
            .ok_or_else(|| format!("invalid settings array index \"{head}\""))?;
        if index >= array.len() {
            return Err(format!("settings array index \"{head}\" is out of range"));
        }
        if rest.is_empty() {
            array.remove(index);
        } else {
            unset_path(&mut array[index], rest)?;
        }
        return Ok(());
    }
    let Some(obj) = root.as_object_mut() else {
        return Ok(());
    };
    if rest.is_empty() {
        obj.remove(head);
        return Ok(());
    }
    if let Some(child) = obj.get_mut(head) {
        unset_path(child, rest)?;
        if child.as_object().is_some_and(Map::is_empty) {
            obj.remove(head);
        }
    }
    Ok(())
}

fn rejected(ns: &str, message: impl Into<String>) -> RpcError {
    let mut error = RpcError::new("settings/rejected", message);
    error.details = json!({ "ns": ns });
    error
}

impl SettingsStore {
    pub fn open(file: PathBuf, catalog: &Value) -> Self {
        Self::open_with_defaults(file, catalog, &BTreeMap::new())
    }

    /// Open settings while overlaying profile-derived defaults onto namespace bases.
    /// Explicit Rust user values remain higher priority than imported profile values.
    pub fn open_with_defaults(
        file: PathBuf,
        catalog: &Value,
        defaults: &BTreeMap<String, Value>,
    ) -> Self {
        let templates = catalog["namespaces"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut template| {
                if let Some(default) = template["ns"].as_str().and_then(|ns| defaults.get(ns)) {
                    template["base"] = merge(&template["base"], default);
                }
                template
            })
            .collect();
        let mut state = BTreeMap::new();
        if let Some(doc) = std::fs::read_to_string(&file)
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        {
            for (ns, v) in doc.as_object().into_iter().flatten() {
                state.insert(
                    ns.clone(),
                    (v["user"].clone(), v["revision"].as_u64().unwrap_or(0)),
                );
            }
        }
        SettingsStore {
            file,
            templates,
            state: Mutex::new(state),
        }
    }

    /// Read a namespace's effective value without exposing its internal template.
    pub fn value(&self, ns: &str) -> Option<Value> {
        let state = self.state.lock().unwrap();
        self.templates
            .iter()
            .find(|template| template["ns"] == ns)
            .map(|template| self.entry(template, &state)["value"].clone())
    }

    fn entry(&self, template: &Value, state: &BTreeMap<String, (Value, u64)>) -> Value {
        let ns = template["ns"].as_str().unwrap_or_default();
        let (user, revision) = state.get(ns).cloned().unwrap_or((json!({}), 0));
        let user = if user.is_object() { user } else { json!({}) };
        let mut e = template.clone();
        e["value"] = merge(&template["base"], &user);
        e["user"] = user;
        e["revision"] = json!(revision);
        e
    }

    /// The `settings/describe` result.
    pub fn describe(&self) -> Value {
        let state = self.state.lock().unwrap();
        let namespaces: Vec<Value> = self
            .templates
            .iter()
            .map(|t| self.entry(t, &state))
            .collect();
        json!({"writable": true, "hasDocument": true, "namespaces": namespaces})
    }

    /// Materialize the Rust settings file if needed and open it in the system text editor.
    pub fn open_document(&self) -> Result<Value, RpcError> {
        if !self.file.exists() {
            let state = self.state.lock().unwrap();
            self.persist(&state)
                .map_err(|error| RpcError::new("settings/document-open-failed", error))?;
        }
        open_settings_file(&self.file)
            .map_err(|error| RpcError::new("settings/document-open-failed", error))?;
        Ok(json!({"opened": true}))
    }

    /// Apply `ops` to one namespace if its revision is still `expected`.
    pub fn mutate(&self, ns: &str, ops: &[Value], expected: u64) -> Result<Value, RpcError> {
        let Some(template) = self.templates.iter().find(|t| t["ns"] == ns) else {
            return Err(rejected(
                ns,
                format!("settings namespace \"{ns}\" does not exist"),
            ));
        };
        let schema = settings_schema::Schema::new(&template["schema"])
            .map_err(|error| rejected(ns, error))?;
        let mut state = self.state.lock().unwrap();
        let (mut user, revision) = state.get(ns).cloned().unwrap_or((json!({}), 0));
        if revision != expected {
            let mut e = RpcError::new(
                "settings/conflict",
                format!("settings namespace \"{ns}\" changed since it was read (expected revision {expected}, now {revision})"),
            );
            e.details = json!({"ns": ns, "expected": expected, "actual": revision});
            return Err(e);
        }
        if !user.is_object() {
            user = json!({});
        }
        for op in ops {
            let kind = op["op"]
                .as_str()
                .ok_or_else(|| rejected(ns, "settings operation must have a string op"))?;
            let path: Vec<String> = op["path"]
                .as_array()
                .ok_or_else(|| rejected(ns, "settings operation path must be an array of strings"))?
                .iter()
                .map(|part| {
                    part.as_str().map(String::from).ok_or_else(|| {
                        rejected(ns, "settings operation path must be an array of strings")
                    })
                })
                .collect::<Result<_, _>>()?;
            schema
                .validate_path(&path)
                .map_err(|error| rejected(ns, error))?;
            match kind {
                "set" => {
                    let value = op
                        .get("value")
                        .ok_or_else(|| rejected(ns, "settings set operation must have a value"))?;
                    set_path(&mut user, &path, value.clone())
                        .map_err(|error| rejected(ns, error))?;
                    if !user.is_object() {
                        return Err(rejected(ns, "settings root must be an object"));
                    }
                }
                "unset" if path.is_empty() => user = template["base"].clone(),
                "unset" => unset_path(&mut user, &path).map_err(|error| rejected(ns, error))?,
                other => return Err(rejected(ns, format!("unsupported settings op {other:?}"))),
            }
        }
        if !user.is_object() {
            return Err(rejected(ns, "settings root must be an object"));
        }
        let effective = merge(&template["base"], &user);
        schema
            .validate_value(&effective)
            .map_err(|error| rejected(ns, error))?;

        let mut next = state.clone();
        next.insert(ns.to_string(), (user, revision + 1));
        self.persist(&next).map_err(|error| rejected(ns, error))?;
        *state = next;
        Ok(self.entry(template, &state))
    }

    fn persist(&self, state: &BTreeMap<String, (Value, u64)>) -> Result<(), String> {
        let doc: Map<String, Value> = state
            .iter()
            .map(|(k, (u, r))| (k.clone(), json!({"user": u, "revision": r})))
            .collect();
        let document =
            serde_json::to_string_pretty(&Value::Object(doc)).map_err(|error| error.to_string())?;
        if let Some(dir) = self
            .file
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
        }
        std::fs::write(&self.file, document).map_err(|error| error.to_string())
    }
}

#[cfg(windows)]
fn open_settings_file(path: &std::path::Path) -> Result<(), String> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "shell32")]
    unsafe extern "system" {
        fn ShellExecuteW(
            window: *mut c_void,
            operation: *const u16,
            file: *const u16,
            parameters: *const u16,
            directory: *const u16,
            show: i32,
        ) -> *mut c_void;
    }

    let operation: Vec<u16> = "open\0".encode_utf16().collect();
    let target: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            operation.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
        )
    } as isize;
    (result > 32)
        .then_some(())
        .ok_or_else(|| "No text editor could open the Rust settings file".to_owned())
}

#[cfg(not(windows))]
fn open_settings_file(path: &std::path::Path) -> Result<(), String> {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    std::process::Command::new(program)
        .arg(path)
        .spawn()
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn catalog() -> Value {
        serde_json::from_str(include_str!("../assets/rpc/settings_describe.json"))
            .expect("captured settings catalog is JSON")
    }

    fn test_root(tag: &str) -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "dsh-settings-{tag}-{}-{nonce}-{id}",
            std::process::id()
        ))
    }

    fn store(tag: &str) -> (SettingsStore, TestDir) {
        let root = test_root(tag);
        std::fs::create_dir_all(&root).expect("create private test directory");
        let file = root.join("settings.json");
        (SettingsStore::open(file, &catalog()), TestDir(root))
    }

    fn rejected(error: RpcError, ns: &str) {
        assert_eq!(error.code, "settings/rejected");
        assert_eq!(error.details, json!({ "ns": ns }));
    }

    #[test]
    fn captured_catalog_defaults_are_valid() {
        for template in catalog()["namespaces"].as_array().expect("namespaces") {
            let schema =
                settings_schema::Schema::new(&template["schema"]).expect("valid schema graph");
            schema
                .validate_value(&template["base"])
                .unwrap_or_else(|error| panic!("{}: {error}", template["ns"]));
        }
    }

    #[test]
    fn rust_desktop_enables_the_coding_tool_presets_by_default() {
        let (settings, _dir) = store("coding-tools-default");
        let described = settings.describe();
        let account = described["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|namespace| namespace["ns"] == "ui-settings-account")
            .expect("account settings namespace");
        assert!(account["value"]["developerTools"].as_bool().unwrap());
        assert!(account["base"]["developerTools"].as_bool().unwrap());
    }

    #[test]
    fn set_unset_merge_with_defaults_and_bump_revision() {
        let (s, _dir) = store("ops");
        let e = s
            .mutate(
                "connection",
                &[
                    json!({"op":"set","path":["recovery","backoffBaseMs"],"value":750}),
                    json!({"op":"set","path":["recovery","backoffFactor"],"value":1.5}),
                ],
                0,
            )
            .unwrap();
        assert_eq!(e["revision"], 1);
        assert_eq!(
            e["user"],
            json!({"recovery": {"backoffBaseMs": 750, "backoffFactor": 1.5}})
        );
        assert_eq!(e["value"]["recovery"]["backoffBaseMs"], 750);
        assert_eq!(e["value"]["recovery"]["backoffFactor"], 1.5);
        assert_eq!(e["value"]["recovery"]["backoffMaxMs"], 10000);
        let e = s
            .mutate(
                "connection",
                &[json!({"op":"unset","path":["recovery","backoffBaseMs"]})],
                1,
            )
            .unwrap();
        assert_eq!(e["user"], json!({"recovery": {"backoffFactor": 1.5}}));
        assert_eq!(e["value"]["recovery"]["backoffBaseMs"], 500);
    }

    #[test]
    fn stale_revision_conflicts_and_namespace_failures_match_rpc_codes() {
        let (s, _dir) = store("conflict");
        s.mutate(
            "connection",
            &[json!({"op":"set","path":["recovery","backoffBaseMs"],"value":750})],
            0,
        )
        .unwrap();
        let e = s
            .mutate(
                "connection",
                &[json!({"op":"set","path":["recovery","backoffBaseMs"],"value":800})],
                0,
            )
            .unwrap_err();
        assert_eq!(e.code, "settings/conflict");
        assert_eq!(
            e.details,
            json!({"ns":"connection","expected":0,"actual":1})
        );
        rejected(s.mutate("nope", &[], 0).unwrap_err(), "nope");
        rejected(
            s.mutate("connection", &[json!({"op":"bogus","path":[]})], 1)
                .unwrap_err(),
            "connection",
        );
        assert_eq!(
            s.describe()["namespaces"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["ns"] == "connection")
                .unwrap()["revision"],
            1
        );
    }

    #[test]
    fn rejects_unknown_keys_wrong_types_and_invalid_enums_without_committing() {
        let (s, dir) = store("validation");
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":["missing"],"value":true})],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":["enabled"],"value":"yes"})],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        rejected(
            s.mutate(
                "web-deepseek-api",
                &[json!({"op":"set","path":["searchBilling"],"value":"invalid"})],
                0,
            )
            .unwrap_err(),
            "web-deepseek-api",
        );
        rejected(
            s.mutate(
                "ui-chat",
                &[json!({"op":"set","path":["transcriptView"],"value":"invalid"})],
                0,
            )
            .unwrap_err(),
            "ui-chat",
        );
        rejected(
            s.mutate(
                "connection",
                &[json!({"op":"set","path":["recovery","backoffBaseMs"],"value":1.5})],
                0,
            )
            .unwrap_err(),
            "connection",
        );
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[json!({"op":"unset","path":["missing"]})],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        assert!(!dir.0.join("settings.json").exists());
        let described = s.describe();
        for entry in described["namespaces"].as_array().unwrap() {
            assert_eq!(entry["revision"], 0);
            assert_eq!(entry["user"], json!({}));
        }
        let e = s
            .mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":["enabled"],"value":false})],
                0,
            )
            .unwrap();
        assert_eq!(e["revision"], 1);
        assert_eq!(e["value"]["enabled"], false);
    }

    #[test]
    fn rejects_malformed_operations_and_invalid_roots() {
        let (s, _dir) = store("operations");
        for op in [
            json!({"op":"set","path":"enabled","value":false}),
            json!({"op":"set","path":[1],"value":false}),
            json!({"op":"set","path":["enabled"]}),
            json!({"path":["enabled"],"value":false}),
        ] {
            rejected(
                s.mutate("session-log-deepseek", &[op], 0).unwrap_err(),
                "session-log-deepseek",
            );
        }
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":[],"value":false})],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[
                    json!({"op":"set","path":[],"value":false}),
                    json!({"op":"set","path":["enabled"],"value":false}),
                ],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":["enabled","nested"],"value":false})],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        let e = s
            .mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":[],"value":{"enabled":false}})],
                0,
            )
            .unwrap();
        assert_eq!(e["user"], json!({"enabled": false}));
        let e = s
            .mutate(
                "session-log-deepseek",
                &[json!({"op":"unset","path":[]})],
                1,
            )
            .unwrap();
        assert_eq!(e["user"], json!({"enabled": true}));
        assert_eq!(e["value"]["enabled"], true);
    }

    #[test]
    fn array_paths_set_and_unset_elements() {
        let (s, _dir) = store("arrays");
        s.mutate(
            "llm-deepseek",
            &[
                json!({"op":"set","path":["retryPolicy","retryableCodes"],"value":["TIMEOUT","SERVER"]}),
                json!({"op":"set","path":["retryPolicy","mode"],"value":"normal"}),
            ],
            0,
        )
        .unwrap();
        let e = s.mutate(
            "llm-deepseek",
            &[json!({"op":"set","path":["retryPolicy","retryableCodes","0"],"value":"RATE_LIMIT"})],
            1,
        )
        .unwrap();
        assert_eq!(
            e["value"]["retryPolicy"]["retryableCodes"],
            json!(["RATE_LIMIT", "SERVER"])
        );
        let e = s
            .mutate(
                "llm-deepseek",
                &[json!({"op":"unset","path":["retryPolicy","retryableCodes","1"]})],
                2,
            )
            .unwrap();
        assert_eq!(
            e["value"]["retryPolicy"]["retryableCodes"],
            json!(["RATE_LIMIT"])
        );
        rejected(s.mutate("llm-deepseek", &[json!({"op":"set","path":["retryPolicy","retryableCodes","01"],"value":"SERVER"})], 3).unwrap_err(), "llm-deepseek");
        rejected(
            s.mutate(
                "llm-deepseek",
                &[json!({"op":"unset","path":["retryPolicy","retryableCodes","1"]})],
                3,
            )
            .unwrap_err(),
            "llm-deepseek",
        );
    }

    #[test]
    fn validates_required_nested_values_and_locale_pattern() {
        let (s, _dir) = store("metadata");
        rejected(
            s.mutate(
                "llm-deepseek",
                &[json!({"op":"set","path":["models"],"value":[{"name":"missing id"}]})],
                0,
            )
            .unwrap_err(),
            "llm-deepseek",
        );
        rejected(
            s.mutate(
                "locale",
                &[json!({"op":"set","path":["preference"],"value":"e"})],
                0,
            )
            .unwrap_err(),
            "locale",
        );
        let e = s
            .mutate(
                "locale",
                &[json!({"op":"set","path":["preference"],"value":"en-US"})],
                0,
            )
            .unwrap();
        assert_eq!(e["value"]["preference"], "en-US");
    }

    #[test]
    fn overrides_survive_a_restart() {
        let (s, dir) = store("persist");
        s.mutate(
            "connection",
            &[json!({"op":"set","path":["recovery","backoffBaseMs"],"value":900})],
            0,
        )
        .unwrap();
        let again = SettingsStore::open(dir.0.join("settings.json"), &catalog());
        let d = again.describe();
        let entry = d["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["ns"] == "connection")
            .unwrap();
        assert_eq!(entry["value"]["recovery"]["backoffBaseMs"], 900);
        assert_eq!(entry["revision"], 1);
    }

    #[test]
    fn persistence_failure_does_not_advance_memory_or_revision() {
        let root = test_root("io");
        std::fs::create_dir_all(&root).expect("create private test directory");
        let _dir = TestDir(root.clone());
        std::fs::write(root.join("blocked"), "not a directory").expect("create blocking file");
        let s = SettingsStore::open(root.join("blocked/settings.json"), &catalog());
        rejected(
            s.mutate(
                "session-log-deepseek",
                &[json!({"op":"set","path":["enabled"],"value":false})],
                0,
            )
            .unwrap_err(),
            "session-log-deepseek",
        );
        let described = s.describe();
        let entry = described["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["ns"] == "session-log-deepseek")
            .unwrap();
        assert_eq!(entry["revision"], 0);
        assert_eq!(entry["user"], json!({}));
    }
}
