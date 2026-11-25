//! Web client shell: the HTML page, boot manifest and plugin bundles the browser loads.
//! Port of `host/frontend-static` and the Host half of `client/modules`.
//!
//! The Vite build supplies `index.html`; the Host injects a boot prelude after `<head>`
//! and a theme prelude after `<body>`. Plugin code is each package's built `lib/client.js`,
//! concatenated per batch with its source-map comment replaced.

use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const HEAD_PREFIX: &str = include_str!("../assets/head-prefix.html");
const HEAD_SUFFIX: &str = include_str!("../assets/head-suffix.html");
const BODY_PREFIX: &str = include_str!("../assets/body-prefix.html");
/// Boot graph captured from the TypeScript Host's `web` profile. Entry order and the
/// inject/external/immediately flags come from the plugin graph; revisions are recomputed.
pub const CAPTURED_BOOT: &str = include_str!("../assets/web-boot.captured.json");

fn html_attr(url: &str) -> String {
    url.replace('&', "&amp;")
}

/// Render the page the browser receives, given the Vite `index.html` and the boot graph.
pub fn render_index(dist_index: &str, boot: &Value) -> String {
    let batches = boot["batches"].as_array().cloned().unwrap_or_default();
    let mut head = String::from(HEAD_PREFIX);
    for b in batches.iter().filter(|b| b["phase"] == "application") {
        head.push_str(&format!(
            "<link rel=\"preload\" as=\"script\" href=\"{}\">",
            html_attr(b["url"].as_str().unwrap_or_default())
        ));
    }
    if let Some(b) = batches.iter().find(|b| b["phase"] == "bootstrap") {
        head.push_str(&format!(
            "<script src=\"{}\"></script>",
            html_attr(b["url"].as_str().unwrap_or_default())
        ));
    }
    head.push_str(&format!(
        "<script>globalThis[\"__DSH_BOOT__\"] = {}</script>",
        boot
    ));
    head.push_str(HEAD_SUFFIX);

    let with_head = match dist_index.find("<head>") {
        Some(i) => {
            let end = i + "<head>".len();
            format!("{}{}{}", &dist_index[..end], head, &dist_index[end..])
        }
        None => dist_index.to_string(),
    };
    match with_head.find("<body>") {
        Some(i) => {
            let end = i + "<body>".len();
            format!("{}{}{}", &with_head[..end], BODY_PREFIX, &with_head[end..])
        }
        None => with_head,
    }
}

/// Concatenate plugin bundles for one combo request. Each source loses its trailing
/// `sourceMappingURL` comment and gains `;\n`; one comment naming the combo closes the file.
pub fn combo_script(sources: &[String], source_map_url: &str) -> String {
    let mut out = String::new();
    for s in sources {
        let body = match s.rfind("//# sourceMappingURL=") {
            Some(i) if !s[i..].trim_end().contains('\n') => &s[..i],
            _ => s.as_str(),
        };
        out.push_str(body);
        out.push_str(";\n");
    }
    out.push_str("//# sourceMappingURL=");
    out.push_str(source_map_url);
    out.push('\n');
    out
}

/// Parse the combo request query `?<id>/client.js,<id>/client.js&rev=<rev>` into package ids.
pub fn parse_combo(raw_query: &str) -> Option<(Vec<String>, Option<String>)> {
    let q = raw_query.strip_prefix('?')?;
    let (list, rev) = match q.split_once("&rev=") {
        Some((l, r)) => (l, Some(r.to_string())),
        None => (q, None),
    };
    let ids: Option<Vec<String>> = list
        .split(',')
        .map(|p| p.strip_suffix("/client.js").map(String::from))
        .collect();
    ids.filter(|v| !v.is_empty()).map(|v| (v, rev))
}

/// The browser entry a package declares for `./client`: a string export, else the
/// `browser` then `default` condition, else `lib/client.js`.
pub fn client_file(manifest: &Value) -> String {
    let rel = match &manifest["exports"]["./client"] {
        Value::String(s) => Some(s.as_str()),
        Value::Object(o) => o
            .get("browser")
            .or_else(|| o.get("default"))
            .and_then(Value::as_str),
        _ => None,
    };
    rel.unwrap_or("./lib/client.js")
        .trim_start_matches("./")
        .to_string()
}

/// Package name to its browser client file, found by scanning `packages/*/*/package.json`.
pub fn scan_packages(repo: &Path) -> HashMap<String, PathBuf> {
    let mut map = HashMap::new();
    for group in std::fs::read_dir(repo.join("packages"))
        .into_iter()
        .flatten()
        .flatten()
    {
        for pkg in std::fs::read_dir(group.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            let manifest = pkg.path().join("package.json");
            let Ok(text) = std::fs::read_to_string(&manifest) else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            if let Some(name) = v["name"].as_str() {
                let file = pkg.path().join(client_file(&v));
                if file.is_file() {
                    map.insert(name.to_string(), file);
                }
            }
        }
    }
    map
}

/// Recompute entry, batch and boot revisions from the plugin files on disk. Entries whose
/// package or `lib/client.js` is missing are dropped from the graph and from their batches.
pub fn build_boot(captured: &Value, packages: &HashMap<String, PathBuf>) -> Value {
    let mut boot = captured.clone();
    let mut revs: HashMap<String, String> = HashMap::new();
    let mut entries = Vec::new();
    for e in captured["entries"].as_array().cloned().unwrap_or_default() {
        let Some(id) = e["id"].as_str() else { continue };
        let Some(file) = packages.get(id) else {
            continue;
        };
        let Ok(bytes) = std::fs::read(file) else {
            continue;
        };
        let rev = revision(&bytes);
        let mut e = e.clone();
        e["rev"] = Value::String(rev.clone());
        e["url"] = Value::String(format!("plugins/??{id}/client.js&rev={rev}"));
        revs.insert(id.to_string(), rev);
        entries.push(e);
    }
    let mut batches = Vec::new();
    for b in captured["batches"].as_array().cloned().unwrap_or_default() {
        let ids: Vec<String> = b["entries"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .filter(|id| revs.contains_key(*id))
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        if ids.is_empty() {
            continue;
        }
        let rev = revision(
            ids.iter()
                .map(|i| format!("{i}:{}", revs[i]))
                .collect::<Vec<_>>()
                .join("|")
                .as_bytes(),
        );
        let list: Vec<String> = ids.iter().map(|i| format!("{i}/client.js")).collect();
        let mut b = b.clone();
        b["entries"] = Value::Array(ids.iter().cloned().map(Value::String).collect());
        b["rev"] = Value::String(rev.clone());
        b["url"] = Value::String(format!("plugins/??{}&rev={rev}", list.join(",")));
        batches.push(b);
    }
    boot["rev"] = Value::String(revision(
        entries
            .iter()
            .filter_map(|e| e["rev"].as_str())
            .collect::<Vec<_>>()
            .join("|")
            .as_bytes(),
    ));
    boot["entries"] = Value::Array(entries);
    boot["batches"] = Value::Array(batches);
    boot
}

/// 12-hex-digit revision of some bytes (FNV-1a 64, folded). Revisions are cache keys only.
pub fn revision(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:012x}", h & 0xffff_ffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combo_requests_parse() {
        let (ids, rev) = parse_combo("?@a/x/client.js,@b/y/client.js&rev=abc123").unwrap();
        assert_eq!(ids, ["@a/x", "@b/y"]);
        assert_eq!(rev.as_deref(), Some("abc123"));
        assert!(parse_combo("?@a/x/other.js").is_none());
        assert!(parse_combo("nope").is_none());
        assert!(parse_combo("?").is_none());
    }

    #[test]
    fn combo_replaces_source_map_comment() {
        let out = combo_script(
            &[
                "a();\n//# sourceMappingURL=client.js.map".into(),
                "b()".into(),
            ],
            "??x/client.js.map&rev=1",
        );
        assert_eq!(
            out,
            "a();\n;\nb();\n//# sourceMappingURL=??x/client.js.map&rev=1\n"
        );
    }

    #[test]
    fn boot_is_rebuilt_from_files_and_drops_missing_packages() {
        let root = std::env::temp_dir().join(format!("dsh-shell-boot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("a/lib")).unwrap();
        std::fs::write(root.join("a/lib/client.js"), "a()").unwrap();
        let packages = HashMap::from([("@x/a".to_string(), root.join("a/lib/client.js"))]);
        let captured = serde_json::json!({
            "rev": "old", "release": {"version": "1"},
            "entries": [{"id":"@x/a","url":"old","rev":"old","inject":["@x/b"]}, {"id":"@x/gone","url":"old","rev":"old"}],
            "batches": [{"phase":"application","url":"old","rev":"old","entries":["@x/a","@x/gone"]}, {"phase":"application","url":"old","rev":"old","entries":["@x/gone"]}]
        });
        let boot = build_boot(&captured, &packages);
        assert_eq!(boot["entries"].as_array().unwrap().len(), 1);
        assert_eq!(boot["entries"][0]["inject"], serde_json::json!(["@x/b"]));
        let rev = boot["entries"][0]["rev"].as_str().unwrap();
        assert_eq!(
            boot["entries"][0]["url"],
            format!("plugins/??@x/a/client.js&rev={rev}")
        );
        assert_eq!(boot["batches"].as_array().unwrap().len(), 1);
        assert_eq!(boot["batches"][0]["entries"], serde_json::json!(["@x/a"]));
        assert!(boot["batches"][0]["url"]
            .as_str()
            .unwrap()
            .starts_with("plugins/??@x/a/client.js&rev="));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn client_file_follows_the_export_conditions() {
        use serde_json::json;
        assert_eq!(client_file(&json!({})), "lib/client.js");
        assert_eq!(
            client_file(&json!({"exports":{"./client":"./lib/c.js"}})),
            "lib/c.js"
        );
        assert_eq!(
            client_file(
                &json!({"exports":{"./client":{"node":"./lib/client.node.js","browser":"./lib/client.browser.js","default":"./lib/d.js"}}})
            ),
            "lib/client.browser.js"
        );
        assert_eq!(
            client_file(
                &json!({"exports":{"./client":{"types":"./t.d.ts","default":"./lib/d.js"}}})
            ),
            "lib/d.js"
        );
    }

    #[test]
    fn revision_is_stable_and_content_sensitive() {
        assert_eq!(revision(b"abc"), revision(b"abc"));
        assert_ne!(revision(b"abc"), revision(b"abd"));
        assert_eq!(revision(b"").len(), 12);
    }
}
