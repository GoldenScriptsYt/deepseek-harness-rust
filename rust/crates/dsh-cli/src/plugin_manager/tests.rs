use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let base = std::env::temp_dir();
        for _ in 0..100 {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = base.join(format!(
                "dsh-plugin-manager-{}-{nanos}-{sequence}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create private test root {}: {error}", path.display()),
            }
        }
        panic!("could not allocate a private test root");
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    root: TempRoot,
    profile: PathBuf,
    install: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = TempRoot::new();
        let profile = root.path().join("profile");
        let install = root.path().join("installation/package.json");
        fs::create_dir_all(&profile).unwrap();
        fs::create_dir_all(install.parent().unwrap()).unwrap();
        write_json(
            &profile.join("package.json"),
            &json!({"name":"profile", "dependencies":{}}),
        );
        write_json(&install, &json!({"name":"installation", "dependencies":{}}));
        Self {
            root,
            profile,
            install,
        }
    }

    fn config(&self) -> BundleListConfig {
        let mut config = BundleListConfig::new(&self.profile, &self.install);
        config.runtime_version = "0.2.3".into();
        config.platform = Some("linux".into());
        config
    }

    fn save_profile(&self, manifest: Value) {
        write_json(&self.profile.join("package.json"), &manifest);
    }

    fn bundle(&self, name: &str, manifest: Value, installation: bool) -> PathBuf {
        let anchor = if installation {
            self.install.parent().unwrap()
        } else {
            &self.profile
        };
        let directory = anchor.join("node_modules").join(name);
        fs::create_dir_all(&directory).unwrap();
        write_json(&directory.join("package.json"), &manifest);
        directory
    }
}

fn write_json(path: &Path, value: &Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn write_text(path: &Path, value: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, value).unwrap();
}

fn by_name<'a>(bundles: &'a [Value], name: &str) -> &'a Value {
    bundles
        .iter()
        .find(|bundle| bundle["name"] == name)
        .unwrap_or_else(|| panic!("missing bundle {name}: {bundles:#?}"))
}

fn bundle_manifest(name: &str, version: &str) -> Value {
    json!({"name":name, "version":version, "main":"lib/index.js", "dsh":{"bundle":{"patch":"./cordis.patch.yml"}}})
}

#[test]
fn registry_options_match_the_plugin_manager_fallback() {
    assert_eq!(
        super::registry_options(None),
        json!({
            "registry": null,
            "fallbackRegistries": ["https://registry.npmmirror.com/"],
            "resolved": null
        })
    );
}

#[test]
fn registry_resolution_takes_the_last_line_and_hides_inline_credentials() {
    assert_eq!(
        super::parse_registry_stdout(
            "Ignored pnpm notice\nhttps://user:secret@registry.example.test/team"
        ),
        Some("https://registry.example.test/team".to_owned())
    );
    assert_eq!(
        super::parse_registry_stdout("https://registry.example.test\n"),
        Some("https://registry.example.test/".to_owned())
    );
    assert_eq!(super::parse_registry_stdout("registry unavailable\n"), None);
}

#[test]
fn official_catalog_is_offline_exact_and_localized() {
    let fixture = Fixture::new();
    let bundles = list_bundles(&fixture.config()).unwrap();
    assert_eq!(bundles.len(), 2);
    assert_eq!(bundles[0]["name"], "@deepseek-ai/dsh-subagent-claude-code");
    assert_eq!(bundles[1]["name"], "@deepseek-ai/dsh-subagent-codex");
    for bundle in &bundles {
        assert_eq!(bundle["official"], true);
        assert_eq!(bundle["availability"], "missing");
        assert_eq!(bundle["installed"], false);
        assert_eq!(bundle["enabled"], false);
        assert_eq!(bundle["removable"], false);
        assert_eq!(bundle["rows"], json!([]));
        assert_eq!(bundle["overrides"], json!([]));
        assert_eq!(bundle["installTarget"]["version"], "0.2.3");
        assert_eq!(
            bundle["installTarget"]["spec"],
            format!("{}@0.2.3", bundle["name"].as_str().unwrap())
        );
        assert_eq!(bundle["meta"]["title"].as_object().unwrap().len(), 8);
        assert_eq!(bundle["meta"]["description"].as_object().unwrap().len(), 8);
        assert_eq!(
            bundle["meta"]["title"]["en"],
            if bundle["name"].as_str().unwrap().contains("codex") {
                "Codex subagent"
            } else {
                "Claude Code subagent"
            }
        );
        assert!(bundle["meta"]["icon"]
            .as_str()
            .unwrap()
            .starts_with("data:image/svg+xml;base64,"));
        assert!(bundle.get("version").is_none());
        assert!(bundle.get("error").is_none());
    }
    let catalog: Value = serde_json::from_str(CATALOG_JSON).unwrap();
    assert_eq!(
        catalog["provenance"]["generator"],
        "scripts/gen-official-bundle-catalog.ts"
    );
    assert_eq!(catalog["entries"].as_array().unwrap().len(), 2);
}

#[test]
fn inventory_order_deduplicates_and_installation_owns_copies() {
    let fixture = Fixture::new();
    fixture.save_profile(json!({"name":"profile", "dsh":{"profile":{"bundles":["z","a","z"]}}, "dependencies":{"b":"1", "a":"1"}}));
    write_json(
        &fixture.install,
        &json!({"name":"installation", "dependencies":{"i":"1", "b":"1"}}),
    );
    let bundles = list_bundles(&fixture.config()).unwrap();
    let names = bundles
        .iter()
        .map(|bundle| bundle["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        &names[..6],
        &[
            "z",
            "a",
            "b",
            "i",
            "@deepseek-ai/dsh-subagent-claude-code",
            "@deepseek-ai/dsh-subagent-codex"
        ]
    );
    assert_eq!(by_name(&bundles, "z")["availability"], "missing");
    assert_eq!(by_name(&bundles, "z")["removable"], true);
    assert_eq!(by_name(&bundles, "a")["availability"], "profile");
    assert_eq!(by_name(&bundles, "a")["removable"], true);
    assert_eq!(by_name(&bundles, "b")["availability"], "missing");
    assert_eq!(by_name(&bundles, "b")["removable"], false);
    assert_eq!(by_name(&bundles, "i")["installed"], false);
}

#[test]
fn installation_copy_wins_and_optional_bundle_stays_protected() {
    let fixture = Fixture::new();
    fixture.save_profile(json!({"name":"profile", "dependencies":{"shadowed":"1"}}));
    write_json(
        &fixture.install,
        &json!({"name":"installation", "dependencies":{"shadowed":"1"}}),
    );
    fixture.bundle("shadowed", bundle_manifest("shadowed", "1.0.0"), false);
    let installation_copy = fixture.bundle("shadowed", bundle_manifest("shadowed", "2.0.0"), true);
    write_text(&installation_copy.join("cordis.patch.yml"), "[]\n");
    let bundles = list_bundles(&fixture.config()).unwrap();
    let shadowed = by_name(&bundles, "shadowed");
    assert_eq!(shadowed["version"], "2.0.0");
    assert_eq!(shadowed["availability"], "installation");
    assert_eq!(shadowed["installed"], true);
    assert_eq!(shadowed["removable"], false);

    let optional = "@deepseek-ai/dsh-cua-bundle";
    fixture.save_profile(json!({"name":"profile", "dsh":{"profile":{"bundles":[optional]}}}));
    let bundles = list_bundles(&fixture.config()).unwrap();
    let bundle = by_name(&bundles, optional);
    assert_eq!(bundle["official"], true);
    assert_eq!(bundle["optional"], true);
    assert_eq!(bundle["enabled"], true);
    assert_eq!(bundle["error"]["code"], "not-bundle");
    assert_eq!(bundle["removable"], true);
}

#[test]
fn profile_bundle_rows_overrides_metadata_and_update_are_data_only() {
    let fixture = Fixture::new();
    let name = "@example/third-party";
    fixture.save_profile(json!({"name":"profile", "dsh":{"profile":{"bundles":[name]}}, "dependencies":{name:"^1.2.0"}}));
    let directory = fixture.bundle(
        name,
        json!({
            "name":name, "version":"1.2.3", "description":"A bundle", "main":"index.js",
            "dsh":{"bundle":{"patch":"./cordis.patch.yml", "listedByDefault":true}}
        }),
        false,
    );
    write_text(
        &directory.join("cordis.patch.yml"),
        r#"
- insert:
    - id: first
      name: ./first.js
    - id: duplicate
      name: ./old.js
    - id: container
      name: ./group.js
      group: true
      insert:
        - id: child
          name: child-plugin
    - name: ./anonymous.js
    - id: duplicate
      name: ./new.js
    - id: inert
      name: inert-plugin
      config: !!js process.exit(1)
- id: override
  value: 1
- id: first
  value: 2
- id: override
  value: 3
- insert:
    - id: management
      name: '@deepseek-ai/dsh-plugin-manager'
"#,
    );
    let mut config = fixture.config();
    config.live_entries.insert("first".into(), "entry-1".into());
    let bundles = list_bundles(&config).unwrap();
    let bundle = by_name(&bundles, name);
    assert_eq!(bundle["source"], format!("{name}@^1.2.0"));
    assert_eq!(bundle["update"], "registry");
    assert_eq!(bundle["description"], "A bundle");
    assert!(bundle["meta"].get("error").is_none());
    assert_eq!(bundle["listedByDefault"], true);
    assert_eq!(bundle["removable"], false);
    assert_eq!(bundle["readOnlyReason"], "management-required");
    let rows = bundle["rows"].as_array().unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row["rowId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "duplicate", "child", "inert", "management"]
    );
    assert_eq!(rows[0]["entryId"], "entry-1");
    assert_eq!(rows[1]["moduleName"], "./new.js");
    assert_eq!(bundle["overrides"], json!(["override"]));
}

#[test]
fn local_paths_aliases_and_git_origins_are_normalized() {
    let fixture = Fixture::new();
    let expected = lexical_absolute(&fixture.profile.join("../linked"));
    assert_eq!(
        dependency_spec("alias", "file:../linked", &fixture.profile, "real"),
        format!("alias@file:{}", expected.display())
    );
    assert_eq!(
        dependency_spec("real", "link:../linked", &fixture.profile, "real"),
        format!("link:{}", expected.display())
    );
    assert_eq!(
        dependency_spec(
            "pkg",
            "https://name:secret@example.com/repo.git#abc",
            &fixture.profile,
            "pkg"
        ),
        "https://example.com/repo.git#abc"
    );
    let commit = "0123456789abcdef0123456789abcdef01234567";
    assert_eq!(
        pinned_git_origin(&format!("github:owner/repo#{commit}")).unwrap(),
        "github.com/owner/repo#"
    );
    assert_eq!(
        pinned_git_origin(&format!(
            "git+https://github.com/Owner/Repo.git#path:packages/a&{commit}"
        ))
        .unwrap(),
        "github.com/owner/repo#packages/a"
    );
    assert!(pinned_git_origin("github:owner/repo#main").is_none());
    assert_eq!(
        file_url(Path::new("C:/some dir/file.js")),
        "file:///C:/some%20dir/file.js"
    );
}

#[test]
fn localized_meta_supports_exports_icons_and_unexported_resources() {
    let fixture = Fixture::new();
    let name = "localized";
    fixture.save_profile(json!({"name":"profile", "dependencies":{name:"1"}}));
    let directory = fixture.bundle(
        name,
        json!({
            "name":name, "version":"1.0.0", "icon":"./icon.svg",
            "exports":{"./package.json":"./package.json", "./locale/*.json":"./locale/*.json"},
            "dsh":{"bundle":{"patch":"./patch.json"}}
        }),
        false,
    );
    write_text(
        &directory.join("locale/en.json"),
        r#"{"meta":{"title":"English","description":"Description"}}"#,
    );
    write_text(
        &directory.join("locale/fr.json"),
        r#"{"meta":{"title":"Français"}}"#,
    );
    write_text(
        &directory.join("icon.svg"),
        "<svg xmlns='http://www.w3.org/2000/svg'/>",
    );
    write_text(&directory.join("patch.json"), "[]");
    let bundles = list_bundles(&fixture.config()).unwrap();
    let meta = &by_name(&bundles, name)["meta"];
    assert_eq!(meta["title"]["en"], "English");
    assert_eq!(meta["title"]["fr"], "Français");
    assert_eq!(meta["description"]["en"], "Description");
    assert_eq!(meta["description"]["fr"], "Description");
    assert!(meta["icon"]
        .as_str()
        .unwrap()
        .starts_with("data:image/svg+xml;base64,"));
    assert!(meta.get("error").is_none());
}

#[test]
fn literal_locale_and_icon_errors_preserve_display_text() {
    let fixture = Fixture::new();
    let name = "broken-icon";
    fixture.save_profile(json!({"name":"profile", "dependencies":{name:"1"}}));
    let directory = fixture.bundle(name, json!({
        "name":name, "version":"1.0.0", "icon":"./missing.svg", "dsh":{"bundle":{"patch":"./patch.json"}}
    }), false);
    write_text(
        &directory.join("locale/en.json"),
        r#"{"meta":{"title":"Readable title"}}"#,
    );
    write_text(&directory.join("patch.json"), "[]");
    let bundles = list_bundles(&fixture.config()).unwrap();
    let meta = &by_name(&bundles, name)["meta"];
    assert_eq!(meta["title"], "Readable title");
    assert!(meta["error"].as_str().unwrap().contains("missing.svg"));
}

#[test]
fn invalid_listing_and_broken_patch_are_contained_per_bundle() {
    let fixture = Fixture::new();
    let name = "diagnostic";
    fixture.save_profile(
        json!({"name":"profile", "dsh":{"profile":{"bundles":[name]}}, "dependencies":{name:"1"}}),
    );
    let directory = fixture.bundle(name, json!({"name":name, "version":"1.0.0", "dsh":{"bundle":{"patch":"./missing.yml", "listedByDefault":"yes"}}}), false);
    let _ = directory;
    let bundles = list_bundles(&fixture.config()).unwrap();
    let bundle = by_name(&bundles, name);
    assert_eq!(
        bundle["meta"]["error"],
        "dsh.bundle.listedByDefault must be a boolean"
    );
    assert_eq!(bundle["error"]["code"], "operation-error");
    assert_eq!(bundle["rows"], json!([]));
    assert_eq!(bundle["overrides"], json!([]));
    assert!(bundle["error"]["diagnostic"]
        .as_str()
        .unwrap()
        .contains("missing.yml"));
}

#[test]
fn platform_and_compatibility_failures_match_bundle_info() {
    let fixture = Fixture::new();
    let platform = "platformed";
    fixture.save_profile(json!({"name":"profile", "dependencies":{platform:"1", "incompatible":"1", "malformed":"1"}}));
    let directory = fixture.bundle(platform, json!({"name":platform, "version":"1.0.0", "dsh":{"bundle":{"patch":"./patch.json", "platforms":["darwin"], "listedByDefault":true}}}), false);
    write_text(&directory.join("patch.json"), "[]");
    let directory = fixture.bundle("incompatible", json!({"name":"incompatible", "version":"1.0.0", "engines":{"dsh":"^9.0.0"}, "dsh":{"bundle":{"patch":"./patch.json", "listedByDefault":true}}}), false);
    write_text(&directory.join("patch.json"), "[]");
    let directory = fixture.bundle("malformed", json!({"name":"malformed", "version":"1.0.0", "dsh":{"bundle":{"patch":"./patch.json", "platforms":["linux", 1]}}}), false);
    write_text(&directory.join("patch.json"), "[]");
    let bundles = list_bundles(&fixture.config()).unwrap();
    assert_eq!(
        by_name(&bundles, platform)["error"]["code"],
        "unsupported-platform"
    );
    assert_eq!(by_name(&bundles, platform)["listedByDefault"], false);
    assert_eq!(
        by_name(&bundles, "incompatible")["error"]["code"],
        "incompatible-version"
    );
    assert_eq!(by_name(&bundles, "incompatible")["listedByDefault"], true);
    assert_eq!(by_name(&bundles, "incompatible")["rows"], json!([]));
    assert_eq!(
        by_name(&bundles, "malformed")["error"]["code"],
        "operation-error"
    );
}

#[test]
fn wsl_and_exact_version_exemptions_are_honored() {
    let fixture = Fixture::new();
    let name = "wsl-bundle";
    fixture.save_profile(json!({"name":"profile", "dependencies":{name:"1"}}));
    let directory = fixture.bundle(name, json!({"name":name, "version":"1.0.0", "engines":{"dsh":"^9"}, "dsh":{"bundle":{"patch":"./patch.json", "platforms":["wsl"]}}}), false);
    write_text(&directory.join("patch.json"), "[]");
    write_json(
        &fixture.profile.join("compatibility.json"),
        &json!({format!("{name}@1.0.0"):["0.2.3"]}),
    );
    let mut config = fixture.config();
    config.wsl = true;
    let bundles = list_bundles(&config).unwrap();
    assert!(by_name(&bundles, name).get("error").is_none());
    config.wsl = false;
    assert_eq!(
        by_name(&list_bundles(&config).unwrap(), name)["error"]["code"],
        "unsupported-platform"
    );
}

#[test]
fn source_checkout_target_requires_complete_workspace() {
    let fixture = Fixture::new();
    let root = fixture.root.path().join("source");
    let install = root.join("apps/cli/package.json");
    fs::create_dir_all(install.parent().unwrap()).unwrap();
    write_json(
        &install,
        &json!({"name":"@deepseek-ai/dsh", "version":"0.2.3", "dependencies":{"workspace-pkg":"workspace:*"}}),
    );
    let mut config = fixture.config();
    config.install_anchor = install.clone();
    let bundles = list_bundles(&config).unwrap();
    assert_eq!(bundles[0]["error"]["code"], "operation-error");
    assert!(bundles[0]["installTarget"].is_null());

    write_json(
        &root.join("package.json"),
        &json!({"name":"@deepseek-ai/dsh-root"}),
    );
    let directory = root.join("packages/subagent/claude-code");
    fs::create_dir_all(&directory).unwrap();
    write_json(
        &directory.join("package.json"),
        &bundle_manifest("@deepseek-ai/dsh-subagent-claude-code", "0.2.3"),
    );
    write_text(&directory.join("lib/index.js"), "");
    write_text(&directory.join("cordis.patch.yml"), "[]\n");
    let bundles = list_bundles(&config).unwrap();
    let target =
        by_name(&bundles, "@deepseek-ai/dsh-subagent-claude-code")["installTarget"].clone();
    assert_eq!(
        target["spec"],
        format!(
            "link:{}",
            display_path(&fs::canonicalize(&directory).unwrap())
        )
    );
    assert_eq!(
        by_name(&bundles, "@deepseek-ai/dsh-subagent-codex")["error"]["code"],
        "operation-error"
    );
}

#[test]
fn top_level_manifest_errors_are_typed_and_rpc_errors_are_structured() {
    let fixture = Fixture::new();
    write_text(&fixture.profile.join("package.json"), "not json");
    let error = list_bundles(&fixture.config()).unwrap_err();
    assert_eq!(error.code(), "PLUGIN_MANAGER_PROFILE_READ_FAILED");
    assert_eq!(error.path(), fixture.profile.join("package.json"));
    let rpc = rpc_error(error);
    assert_eq!(rpc.code, "gateway/internal");
    assert_eq!(rpc.details["cause"]["name"], "PluginManagerError");
    assert_eq!(
        rpc.details["cause"]["code"],
        "PLUGIN_MANAGER_PROFILE_READ_FAILED"
    );

    write_json(
        &fixture.profile.join("package.json"),
        &json!({"dependencies":[]}),
    );
    let error = list_bundles(&fixture.config()).unwrap_err();
    assert_eq!(error.code(), "PLUGIN_MANAGER_PROFILE_READ_FAILED");
    write_json(&fixture.profile.join("package.json"), &json!({}));
    for installation in [json!({"dependencies":"bad"}), json!(null)] {
        write_json(&fixture.install, &installation);
        let error = list_bundles(&fixture.config()).unwrap_err();
        assert_eq!(error.code(), "PLUGIN_MANAGER_PROFILE_READ_FAILED");
        assert_eq!(error.path(), fixture.install);
    }
    write_text(&fixture.install, "not json");
    let error = list_bundles(&fixture.config()).unwrap_err();
    assert_eq!(error.code(), "PLUGIN_MANAGER_PROFILE_READ_FAILED");
    assert_eq!(error.path(), fixture.install);
}

#[test]
fn semver_ranges_cover_wildcards_carets_tilde_and_hyphens() {
    let version = SemVer::parse("0.2.3").unwrap();
    for range in [
        "*",
        "x",
        "0.2.3",
        "=0.2.3",
        "^0.2.0",
        "~0.2.0",
        ">=0.1.0 <1",
        "0.2.x",
        "0.1 - 0.3",
    ] {
        assert!(satisfies(&version, range), "expected {range} to match");
    }
    for range in ["", "0.2.2", "^0.3.0", "~0.3.0", "0.3 - 1", ">=1"] {
        assert!(!satisfies(&version, range), "expected {range} not to match");
    }
    assert!(satisfies(&SemVer::parse("0.0.3").unwrap(), "^0.0.3"));
    assert!(!satisfies(&SemVer::parse("0.0.4").unwrap(), "^0.0.3"));
}

#[test]
fn yaml_patch_parser_is_inert_and_reports_real_line_numbers() {
    let value = parse_patch("- insert:\n    - id: row\n      name: './row.js'\n      config: !!js globalThis.compromised = true\n- id: override\n").unwrap();
    assert_eq!(value[0]["insert"][0]["id"], "row");
    assert_eq!(
        value[0]["insert"][0]["config"],
        "!!js globalThis.compromised = true"
    );
    assert_eq!(value[1]["id"], "override");
    assert!(parse_patch("- insert:\n\t- id: bad\n")
        .unwrap_err()
        .to_string()
        .contains("line 2"));
}

#[test]
fn yaml_patch_parser_preserves_lines_and_unicode_byte_boundaries() {
    let value = parse_patch(
        "\u{feff}\n# café\n\n- insert:\n    - id: naïve\n      name: './café.js' # 注釈\n",
    )
    .unwrap();
    assert_eq!(value[0]["insert"][0]["id"], "naïve");
    assert_eq!(value[0]["insert"][0]["name"], "./café.js");

    let error = parse_patch(
        "\u{feff}\n# café\n\n- insert:\n    - id: naïve\n      name: './café.js'\n\t- id: bad\n",
    )
    .unwrap_err();
    assert!(error.to_string().contains("line 7"), "{error}");
}

#[test]
fn host_startup_registers_catalog_and_degrades_missing_bundle_resources() {
    let root = TempRoot::new();
    let repo = root.path().join("resources");
    let home = root.path().join("home");
    write_json(
        &repo.join("apps/cli/package.json"),
        &json!({
            "name": "@deepseek-ai/dsh",
            "version": "0.2.3",
            "private": true,
            "dependencies": {}
        }),
    );

    let config = crate::web::host_plugin_manager_config(&repo, &home);
    assert_eq!(config.profile_dir, home.join("profiles/web"));
    assert_eq!(config.install_anchor, repo.join("apps/cli/package.json"));
    assert!(config.install_anchor.is_absolute());

    let token = "host-startup-test";
    let gateway = Gateway::new(token, home.display().to_string(), None);
    crate::web::register_host_plugin_manager(&gateway, &repo, &home);

    let profile: Value =
        serde_json::from_slice(&fs::read(config.profile_dir.join("package.json")).unwrap())
            .unwrap();
    assert_eq!(profile["name"], "dsh-profile-web");
    assert_eq!(profile["private"], true);
    assert_eq!(
        profile["dsh"]["profile"]["bundles"],
        json!(["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app"])
    );

    let bundles = list_bundles(&config).unwrap();
    assert_eq!(bundles.len(), 4);
    assert_eq!(
        by_name(&bundles, "@deepseek-ai/dsh-base")["error"]["code"],
        "not-bundle"
    );
    assert_eq!(
        by_name(&bundles, "@deepseek-ai/dsh-web-app")["error"]["code"],
        "not-bundle"
    );
    assert_eq!(
        by_name(&bundles, "@deepseek-ai/dsh-subagent-claude-code")["installTarget"]["spec"],
        "@deepseek-ai/dsh-subagent-claude-code@0.2.3"
    );
}

#[test]
fn packaged_native_anchor_selects_the_desktop_profile() {
    let root = TempRoot::new();
    let repo = root.path().join("resources");
    let home = root.path().join("home");
    write_json(
        &repo.join("apps/cli/package.json"),
        &json!({
            "name": "@deepseek-ai/dsh",
            "dependencies": {},
            "dsh": {"host": {"profile": "desktop"}}
        }),
    );
    let config = crate::web::host_plugin_manager_config(&repo, &home);
    assert_eq!(config.profile_dir, home.join("profiles/desktop"));

    let gateway = Gateway::new("desktop-profile-test", home.display().to_string(), None);
    crate::web::register_host_plugin_manager(&gateway, &repo, &home);
    let profile: Value =
        serde_json::from_slice(&fs::read(config.profile_dir.join("package.json")).unwrap())
            .unwrap();
    assert_eq!(profile["name"], "dsh-profile-desktop");
    assert!(!home.join("profiles/web/package.json").exists());
}

#[test]
fn host_startup_preserves_an_existing_profile_manifest() {
    let root = TempRoot::new();
    let repo = root.path().join("resources");
    let home = root.path().join("home");
    write_json(
        &repo.join("apps/cli/package.json"),
        &json!({"name":"@deepseek-ai/dsh", "dependencies":{}}),
    );
    let profile_path = home.join("profiles/web/package.json");
    let existing = "{\n  \"name\": \"user-profile\",\n  \"dependencies\": {}\n}\n";
    write_text(&profile_path, existing);

    let gateway = Gateway::new("profile-test", home.display().to_string(), None);
    crate::web::register_host_plugin_manager(&gateway, &repo, &home);
    crate::web::register_host_plugin_manager(&gateway, &repo, &home);

    assert_eq!(fs::read_to_string(profile_path).unwrap(), existing);
}

#[test]
fn host_startup_missing_installation_manifest_degrades_to_structured_error() {
    let root = TempRoot::new();
    let repo = root.path().join("resources");
    let home = root.path().join("home");
    let config = crate::web::host_plugin_manager_config(&repo, &home);
    let gateway = Gateway::new("missing-install-test", home.display().to_string(), None);
    crate::web::register_host_plugin_manager(&gateway, &repo, &home);

    assert!(config.profile_dir.join("package.json").is_file());
    let error = list_bundles(&config).unwrap_err();
    assert_eq!(error.code(), "PLUGIN_MANAGER_PROFILE_READ_FAILED");
    assert_eq!(error.path(), repo.join("apps/cli/package.json"));
    let rpc = rpc_error(error);
    assert_eq!(rpc.code, "gateway/internal");
    assert_eq!(
        rpc.details["cause"]["path"],
        config.install_anchor.display().to_string()
    );
}
