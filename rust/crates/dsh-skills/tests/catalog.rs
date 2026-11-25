use dsh_skills::{
    DiscoveryLimits, DuplicateNamePolicy, LimitKind, SkillError, SkillErrorCode, SkillLoader,
    SkillRoot, SkillWarningCode, DEFAULT_MAX_FILE_BYTES,
};
use serde_yaml::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/catalog")
        .join(path)
}

fn document(name: &str, description: &str, body: &str) -> String {
    format!("---\nname: {name}\ndescription: {description}\n---\n\n{body}\n")
}

fn write_skill(root: &Path, file_name: &str, name: &str, description: &str, body: &str) {
    fs::create_dir_all(root).unwrap();
    fs::write(root.join(file_name), document(name, description, body)).unwrap();
}

fn write_bundle(root: &Path, directory: &str, name: &str, description: &str, body: &str) {
    let bundle = root.join(directory);
    fs::create_dir_all(&bundle).unwrap();
    fs::write(bundle.join("SKILL.md"), document(name, description, body)).unwrap();
}

fn names(catalog: &dsh_skills::SkillCatalog) -> Vec<&str> {
    catalog
        .skills()
        .iter()
        .map(|skill| skill.name.as_str())
        .collect()
}

#[test]
fn fixture_catalog_validates_orders_deduplicates_and_projects_model_summaries() {
    let catalog = SkillLoader::new([
        SkillRoot::new(fixture("fallback"), "fallback", 200),
        SkillRoot::new(fixture("preferred"), "preferred", 100).skipping_system(),
    ])
    .discover()
    .unwrap();

    assert!(catalog.is_complete());
    assert_eq!(
        names(&catalog),
        [
            "alpha",
            "flat-rich",
            "model-only",
            "shared",
            "unicode-skill",
            "user-only",
            "zeta",
        ]
    );
    let shared = catalog.find("shared").unwrap();
    assert_eq!(shared.description, "Preferred shared description.");
    assert_eq!(shared.source, "preferred");
    assert_eq!(shared.rank, 100);
    assert_eq!(
        shared.path,
        fs::canonicalize(fixture("preferred/shared/SKILL.md")).unwrap()
    );
    assert_eq!(
        shared.resource_base,
        fs::canonicalize(fixture("preferred/shared")).unwrap()
    );
    assert!(catalog.find("nested-ignored").is_none());
    assert!(catalog.find("hidden-system").is_none());
    assert!(catalog.find("Bad_Name").is_none());

    let alpha = catalog.load("alpha").unwrap().unwrap();
    assert_eq!(alpha.content, "Alpha body.");
    assert_eq!(
        alpha.summary.when_to_use.as_deref(),
        Some("Use alpha for fixture checks.")
    );
    assert_eq!(
        alpha
            .metadata
            .as_ref()
            .unwrap()
            .get(Value::String("owner".into())),
        Some(&Value::String("tests".into()))
    );

    let flat = catalog.load("flat-rich").unwrap().unwrap();
    assert_eq!(
        flat.summary.description,
        "Includes a ---- marker that is not a delimiter.\n"
    );
    assert_eq!(flat.content, "Flat body.");
    assert!(flat.summary.invocation.model_invocable);
    assert!(flat.summary.invocation.user_invocable);

    assert!(
        catalog
            .find("model-only")
            .unwrap()
            .invocation
            .model_invocable
    );
    assert!(
        !catalog
            .find("model-only")
            .unwrap()
            .invocation
            .user_invocable
    );
    assert!(
        !catalog
            .find("user-only")
            .unwrap()
            .invocation
            .model_invocable
    );
    assert!(catalog.find("user-only").unwrap().invocation.user_invocable);

    let visible = catalog.model_visible_summaries();
    assert_eq!(
        visible
            .iter()
            .map(|skill| skill.name.as_str())
            .collect::<Vec<_>>(),
        [
            "alpha",
            "flat-rich",
            "model-only",
            "shared",
            "unicode-skill",
            "zeta"
        ]
    );
    assert!(visible
        .iter()
        .all(|skill| skill.description.contains("instructions")
            || skill.description.contains("marker")
            || skill.description.contains("Model")
            || skill.description.contains("Preferred")
            || skill.description.contains("Résumé")
            || skill.description.contains("Flat")));

    for code in [
        SkillWarningCode::InvalidDefinition,
        SkillWarningCode::MissingFrontmatter,
        SkillWarningCode::InvalidYaml,
        SkillWarningCode::InvalidInvocation,
        SkillWarningCode::DuplicateIgnored,
    ] {
        assert!(
            catalog
                .warnings()
                .iter()
                .any(|warning| warning.code == code),
            "missing {code:?}"
        );
    }
}

#[test]
fn duplicate_rejection_reports_both_canonical_paths() {
    let error = SkillLoader::new([
        SkillRoot::new(fixture("fallback"), "fallback", 200),
        SkillRoot::new(fixture("preferred"), "preferred", 100),
    ])
    .with_duplicate_policy(DuplicateNamePolicy::Reject)
    .discover()
    .unwrap_err();

    assert_eq!(error.code(), SkillErrorCode::DuplicateName);
    match error {
        SkillError::DuplicateName {
            name,
            first,
            duplicate,
        } => {
            assert_eq!(name, "shared");
            assert_eq!(
                first,
                fs::canonicalize(fixture("preferred/shared/SKILL.md")).unwrap()
            );
            assert_eq!(
                duplicate,
                fs::canonicalize(fixture("fallback/shared/SKILL.md")).unwrap()
            );
        }
        other => panic!("expected duplicate error, got {other:?}"),
    }
}

#[test]
fn duplicate_priority_uses_rank_then_root_order_then_entry_order() {
    let temporary = TempDir::new().unwrap();
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    write_skill(&first, "z.md", "shared", "first z", "z");
    write_skill(&first, "a.md", "shared", "first a", "a");
    write_skill(&second, "a.md", "shared", "second", "second");

    let catalog = SkillLoader::new([
        SkillRoot::new(&second, "second", 10),
        SkillRoot::new(&first, "first", 10),
    ])
    .discover()
    .unwrap();
    assert_eq!(catalog.find("shared").unwrap().description, "second");

    let catalog = SkillLoader::new([
        SkillRoot::new(&first, "first", 20),
        SkillRoot::new(&second, "second", 10),
    ])
    .discover()
    .unwrap();
    assert_eq!(catalog.find("shared").unwrap().description, "second");

    let catalog = SkillLoader::new([
        SkillRoot::new(&first, "first", 10),
        SkillRoot::new(&second, "second", 10),
    ])
    .discover()
    .unwrap();
    assert_eq!(catalog.find("shared").unwrap().description, "first a");
    assert_eq!(
        catalog.find("shared").unwrap().path,
        fs::canonicalize(first.join("a.md")).unwrap()
    );
}

#[test]
fn loading_rereads_current_body_and_rejects_stale_or_missing_definitions() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    write_skill(&root, "current.md", "current", "First", "First body.");
    let catalog = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap();

    let loaded = catalog.load("current").unwrap().unwrap();
    assert_eq!(loaded.content, "First body.");
    assert_eq!(loaded.summary.description, "First");

    write_skill(&root, "current.md", "current", "Second", "Second body.");
    let loaded = catalog.load("current").unwrap().unwrap();
    assert_eq!(loaded.content, "Second body.");
    assert_eq!(loaded.summary.description, "Second");

    write_skill(&root, "current.md", "renamed", "Renamed", "Renamed body.");
    assert!(catalog.load("current").unwrap().is_none());
    assert!(catalog.load("Bad_Name").unwrap().is_none());

    fs::remove_file(root.join("current.md")).unwrap();
    assert!(catalog.load("current").unwrap().is_none());
    assert!(catalog.load("unknown").unwrap().is_none());
}

#[test]
fn crlf_boolean_spellings_optional_fields_and_javascript_trim_are_ported() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("crlf.md"),
        "---\r\nname: crlf\r\ndescription: CRLF\r\nwhenToUse: \r\ndisable-model-invocation: false\r\nuser-invocable: true\r\nmetadata:\r\n  marker: \"----\"\r\n---\r\n\r\n\u{FEFF}Body.\u{3000}\r\n",
    )
    .unwrap();
    fs::write(
        root.join("nel.md"),
        "---\nname: nel\ndescription: NEL is not JavaScript whitespace\n---\n\u{0085}Body.\u{0085}\n",
    )
    .unwrap();
    fs::write(
        root.join("numeric.md"),
        "---\nname: numeric\ndescription: Numeric booleans\ndisable-model-invocation: 1.0\nuser-invocable: 0\n---\nNumeric.\n",
    )
    .unwrap();

    let catalog = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap();
    let crlf = catalog.load("crlf").unwrap().unwrap();
    assert_eq!(crlf.content, "Body.");
    assert_eq!(crlf.summary.when_to_use, None);
    assert!(crlf.summary.invocation.model_invocable);
    assert!(crlf.summary.invocation.user_invocable);
    assert_eq!(
        crlf.metadata.unwrap().get(Value::String("marker".into())),
        Some(&Value::String("----".into()))
    );
    assert_eq!(
        catalog.load("nel").unwrap().unwrap().content,
        "\u{0085}Body.\u{0085}"
    );
    let numeric = catalog.load("numeric").unwrap().unwrap();
    assert!(!numeric.summary.invocation.model_invocable);
    assert!(!numeric.summary.invocation.user_invocable);
}

#[test]
fn invalid_utf8_is_omitted_without_hiding_valid_siblings() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    write_skill(&root, "valid.md", "valid", "Valid", "Valid body.");
    fs::write(
        root.join("invalid.md"),
        b"---\nname: invalid\ndescription: Invalid UTF-8\n---\n\xff\n",
    )
    .unwrap();

    let catalog = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap();
    assert!(catalog.is_complete());
    assert_eq!(names(&catalog), ["valid"]);
    assert!(catalog
        .warnings()
        .iter()
        .any(|warning| warning.code == SkillWarningCode::InvalidUtf8));
}

#[cfg(unix)]
#[test]
fn non_utf8_entry_names_are_bounded_and_reported() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    fs::create_dir_all(&root).unwrap();
    write_skill(&root, "valid.md", "valid", "Valid", "Valid body.");
    fs::write(root.join(OsStr::from_bytes(b"invalid-\xff.md")), "ignored").unwrap();

    let catalog = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap();
    assert_eq!(names(&catalog), ["valid"]);
    assert!(catalog.warnings().iter().any(|warning| {
        warning.code == SkillWarningCode::InvalidDefinition && warning.message.contains("not UTF-8")
    }));
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_symlink(target: &Path, link: &Path) -> io::Result<()> {
    if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    }
}

fn symlink_or_skip(target: &Path, link: &Path) -> bool {
    match create_symlink(target, link) {
        Ok(()) => true,
        Err(error)
            if error.kind() == io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(1314) =>
        {
            eprintln!("skipping symlink assertion: host denied symlink creation: {error}");
            false
        }
        Err(error) => panic!("failed to create test symlink: {error}"),
    }
}

#[test]
fn in_root_symlinks_publish_canonical_files_and_keep_discovered_resource_roots() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    write_bundle(&root, "real", "linked-skill", "Linked", "Linked body.");
    write_skill(&root, "z-flat.md", "flat-skill", "Flat", "Flat body.");
    let linked_resource = root.join("real/resources");
    fs::create_dir_all(&linked_resource).unwrap();
    fs::write(linked_resource.join("reference.txt"), "Linked resource.").unwrap();
    if !symlink_or_skip(&root.join("real"), &root.join("a-linked"))
        || !symlink_or_skip(&root.join("z-flat.md"), &root.join("a-flat.md"))
    {
        return;
    }

    let catalog = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap();
    let linked = catalog.find("linked-skill").unwrap();
    assert_eq!(
        linked.path,
        fs::canonicalize(root.join("real/SKILL.md")).unwrap()
    );
    assert_eq!(
        linked.resource_base,
        fs::canonicalize(root.join("a-linked")).unwrap()
    );
    assert_eq!(
        catalog.load("linked-skill").unwrap().unwrap().content,
        "Linked body."
    );
    assert_eq!(
        fs::read_to_string(linked.resource_base.join("resources/reference.txt")).unwrap(),
        "Linked resource."
    );
    assert_eq!(
        fs::canonicalize(linked.resource_base.as_path()).unwrap(),
        fs::canonicalize(root.join("real")).unwrap()
    );

    let flat = catalog.find("flat-skill").unwrap();
    assert_eq!(flat.path, fs::canonicalize(root.join("z-flat.md")).unwrap());
    assert_eq!(flat.resource_base, fs::canonicalize(&root).unwrap());
    assert_eq!(
        catalog.load("flat-skill").unwrap().unwrap().content,
        "Flat body."
    );
}

#[test]
fn external_symlinks_and_broken_links_cannot_escape_the_root() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    let outside = temporary.path().join("outside");
    write_bundle(&outside, "escaped", "escaped", "Escaped", "Escaped body.");
    write_skill(
        &outside,
        "escaped.md",
        "escaped-flat",
        "Escaped flat",
        "Escaped flat body.",
    );
    fs::create_dir_all(&root).unwrap();
    if !symlink_or_skip(&outside.join("escaped"), &root.join("escape"))
        || !symlink_or_skip(&outside.join("escaped.md"), &root.join("escape.md"))
        || !symlink_or_skip(&outside.join("missing"), &root.join("broken"))
    {
        return;
    }

    let error = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap_err();
    assert_eq!(error.code(), SkillErrorCode::PathOutsideRoot);
    assert!(matches!(error, SkillError::PathOutsideRoot { .. }));
}

#[test]
fn explicit_root_symlinks_and_dot_segments_are_canonicalized_before_scanning() {
    let temporary = TempDir::new().unwrap();
    let actual = temporary.path().join("actual");
    let nested = temporary.path().join("nested");
    write_skill(&actual, "safe.md", "safe", "Safe", "Safe body.");
    fs::create_dir_all(&nested).unwrap();
    if !symlink_or_skip(&actual, &temporary.path().join("alias")) {
        return;
    }

    for root in [
        temporary.path().join("alias"),
        nested.join("..").join("actual"),
    ] {
        let catalog = SkillLoader::new([SkillRoot::new(root, "custom", 300)])
            .discover()
            .unwrap();
        assert_eq!(names(&catalog), ["safe"]);
        assert_eq!(
            catalog.find("safe").unwrap().path,
            fs::canonicalize(actual.join("safe.md")).unwrap()
        );
    }
}

#[test]
fn missing_and_non_directory_roots_are_confirmed_empty_observations() {
    let temporary = TempDir::new().unwrap();
    let file = temporary.path().join("file");
    fs::write(&file, "not a directory").unwrap();
    let catalog = SkillLoader::new([
        SkillRoot::new(temporary.path().join("missing"), "missing", 1),
        SkillRoot::new(file, "file", 2),
    ])
    .discover()
    .unwrap();
    assert!(catalog.is_complete());
    assert!(catalog.skills().is_empty());
    assert!(catalog.warnings().is_empty());
}

#[test]
fn file_and_total_byte_limits_accept_exact_sizes_and_reject_one_more() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    let text = document("exact", "Exact", "café 日本語 🚀");
    write_skill(&root, "exact.md", "exact", "Exact", "café 日本語 🚀");
    assert_eq!(fs::read_to_string(root.join("exact.md")).unwrap(), text);
    let exact = text.len() as u64;

    let limits = DiscoveryLimits {
        max_file_bytes: exact,
        max_total_bytes: exact,
        ..DiscoveryLimits::default()
    };
    let catalog = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .with_limits(limits)
        .unwrap()
        .discover()
        .unwrap();
    assert_eq!(names(&catalog), ["exact"]);

    for (kind, limits) in [
        (
            LimitKind::FileBytes,
            DiscoveryLimits {
                max_file_bytes: exact - 1,
                ..limits
            },
        ),
        (
            LimitKind::TotalBytes,
            DiscoveryLimits {
                max_total_bytes: exact - 1,
                ..limits
            },
        ),
    ] {
        let error = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
            .with_limits(limits)
            .unwrap()
            .discover()
            .unwrap_err();
        assert_eq!(error.code(), SkillErrorCode::LimitExceeded);
        assert!(matches!(error, SkillError::LimitExceeded { kind: actual, .. } if actual == kind));
    }
}

#[test]
fn entry_skill_file_and_root_count_limits_cover_the_complete_scan() {
    let temporary = TempDir::new().unwrap();
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    write_skill(&first, "one.md", "one", "One", "One");
    write_skill(&first, "two.md", "two", "Two", "Two");
    fs::write(first.join("ignored.txt"), "ignored").unwrap();
    write_skill(&second, "three.md", "three", "Three", "Three");

    let exact = DiscoveryLimits {
        max_entries_per_root: 3,
        max_skill_files: 3,
        max_roots: 2,
        ..DiscoveryLimits::default()
    };
    let catalog = SkillLoader::new([
        SkillRoot::new(&first, "first", 1),
        SkillRoot::new(&second, "second", 2),
    ])
    .with_limits(exact)
    .unwrap()
    .discover()
    .unwrap();
    assert_eq!(names(&catalog), ["one", "three", "two"]);

    for (kind, limits) in [
        (
            LimitKind::EntriesPerRoot,
            DiscoveryLimits {
                max_entries_per_root: 2,
                ..exact
            },
        ),
        (
            LimitKind::SkillFiles,
            DiscoveryLimits {
                max_skill_files: 1,
                ..exact
            },
        ),
        (
            LimitKind::Roots,
            DiscoveryLimits {
                max_roots: 1,
                ..exact
            },
        ),
    ] {
        let error = SkillLoader::new([
            SkillRoot::new(&first, "first", 1),
            SkillRoot::new(&second, "second", 2),
        ])
        .with_limits(limits)
        .unwrap()
        .discover()
        .unwrap_err();
        assert_eq!(error.code(), SkillErrorCode::LimitExceeded);
        assert!(matches!(error, SkillError::LimitExceeded { kind: actual, .. } if actual == kind));
    }
}

#[test]
fn zero_limits_are_rejected_as_invalid_configuration() {
    let default = DiscoveryLimits::default();
    let cases = [
        DiscoveryLimits {
            max_file_bytes: 0,
            ..default
        },
        DiscoveryLimits {
            max_total_bytes: 0,
            ..default
        },
        DiscoveryLimits {
            max_entries_per_root: 0,
            ..default
        },
        DiscoveryLimits {
            max_skill_files: 0,
            ..default
        },
        DiscoveryLimits {
            max_roots: 0,
            ..default
        },
    ];
    for limits in cases {
        let error = SkillLoader::new(Vec::new())
            .with_limits(limits)
            .unwrap_err();
        assert_eq!(error.code(), SkillErrorCode::InvalidConfig);
    }
    let error = SkillLoader::new([SkillRoot::new("", "custom", 1)])
        .discover()
        .unwrap_err();
    assert_eq!(error.code(), SkillErrorCode::InvalidConfig);
}

#[test]
fn default_file_limit_rejects_a_large_document_before_catalog_publication() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("skills");
    let body = "x".repeat(DEFAULT_MAX_FILE_BYTES as usize);
    write_skill(&root, "large.md", "large", "Large", &body);

    let error = SkillLoader::new([SkillRoot::new(&root, "custom", 300)])
        .discover()
        .unwrap_err();
    assert_eq!(error.code(), SkillErrorCode::LimitExceeded);
    assert!(
        matches!(error, SkillError::LimitExceeded { kind: LimitKind::FileBytes, limit, .. }
        if limit == DEFAULT_MAX_FILE_BYTES)
    );
}
