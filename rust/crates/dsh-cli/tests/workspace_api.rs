pub mod api {
    pub use dsh_cli::api::*;
}

mod workspace_api {
    include!("../src/workspace_api.rs");

    #[cfg(test)]
    mod integration {
        use super::*;

        struct TempRoot(PathBuf);

        impl TempRoot {
            fn new(label: &str) -> Self {
                let path = std::env::temp_dir().join(format!(
                    "dsh-workspace-api-{label}-{}-{}",
                    std::process::id(),
                    &random_uuid()[..8]
                ));
                fs::create_dir_all(&path).unwrap();
                Self(path)
            }
        }

        impl Drop for TempRoot {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }

        fn core_at(root: &Path) -> Arc<Core> {
            let gateway = Gateway::new("test", root.to_string_lossy(), None);
            Arc::new(Core {
                home: root.to_path_buf(),
                feed: gateway.broadcaster("workspace/follow"),
                mutation: Mutex::new(()),
            })
        }

        fn context(root: &Path) -> Arc<ApiContext> {
            ApiContext::new("http://127.0.0.1", "test", root)
        }

        fn add_session(root: &Path, id: &str, cwd: &Path) {
            let store = Store::new(root.join("sessions"));
            let header = Header {
                kind: HeaderTag::Session,
                version: SESSION_FORMAT_VERSION,
                id: id.to_string(),
                created_at: now_ms(),
                cwd: Some(display_path(cwd)),
                parent_session: None,
                is_seeded: false,
                origin: None,
                delegation_depth: 0,
                agent_preset: Some("standard".to_string()),
            };
            drop(store.create(header).unwrap());
        }

        fn setup(label: &str) -> (TempRoot, Arc<Core>, Arc<ApiContext>, PathBuf) {
            let temp = TempRoot::new(label);
            let workspace = temp.0.join("workspace");
            fs::create_dir(&workspace).unwrap();
            add_session(&temp.0, "scope", &workspace);
            let core = core_at(&temp.0);
            let cx = context(&temp.0);
            (temp, core, cx, workspace)
        }

        fn keys(value: &Value) -> HashSet<&str> {
            value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect()
        }

        #[test]
        fn read_stat_and_bytes_allow_authorized_outside_files() {
            let (temp, core, cx, workspace) = setup("outside-files");
            let outside = temp.0.join("outside.txt");
            fs::write(&outside, b"one\ntwo\n").unwrap();

            let stat = file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "../outside.txt" } }),
            )
            .unwrap();
            assert_eq!(stat["bytes"], 8);
            assert_eq!(
                display_path(&fs::canonicalize(&outside).unwrap()),
                stat["absolutePath"]
            );

            let page = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "../outside.txt", "range": { "offset": 2, "limit": 1 } } }),
            )
            .unwrap();
            assert_eq!(page["text"], "two");
            assert_eq!(page["lines"], 1);
            assert_eq!(page["eof"], true);

            let bytes = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": display_path(&outside), "options": { "range": { "offset": 1, "length": 2 } } } }),
            )
            .unwrap();
            assert_eq!(bytes["data"], json!([b'n', b'e']));
            assert_eq!(bytes["eof"], false);
            assert!(!workspace.join("outside.txt").exists());
        }

        #[cfg(unix)]
        #[test]
        fn file_operations_reject_a_final_symlink_even_inside_workspace() {
            let (_temp, core, cx, workspace) = setup("final-link");
            fs::write(workspace.join("real.txt"), b"hello").unwrap();
            std::os::unix::fs::symlink(workspace.join("real.txt"), workspace.join("link.txt"))
                .unwrap();

            let error = file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "link.txt" } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "workspace-file/not-regular-file");
            assert_eq!(error.details["kind"], "symlink");
        }

        #[cfg(unix)]
        #[test]
        fn list_reports_link_targets_and_dangling_links() {
            let (_temp, core, cx, workspace) = setup("listing");
            fs::create_dir(workspace.join("dir")).unwrap();
            fs::write(workspace.join("real.txt"), b"x").unwrap();
            std::os::unix::fs::symlink(workspace.join("real.txt"), workspace.join("to-file"))
                .unwrap();
            std::os::unix::fs::symlink(workspace.join("dir"), workspace.join("to-dir")).unwrap();
            std::os::unix::fs::symlink(workspace.join("missing"), workspace.join("dangling"))
                .unwrap();

            let listing = file_list(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "." } }),
            )
            .unwrap();
            assert_eq!(listing["path"], "");
            assert_eq!(
                listing["entries"],
                json!([
                    { "name": "dangling", "type": "other" },
                    { "name": "dir", "type": "directory" },
                    { "name": "real.txt", "type": "file", "size": 1 },
                    { "name": "to-dir", "type": "directory" },
                    { "name": "to-file", "type": "file", "size": 1 }
                ])
            );

            let error = file_list(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "dangling" } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "workspace-file/not-directory");
            assert_eq!(error.details["kind"], "symlink");
        }

        #[test]
        fn list_rejects_parent_absolute_and_link_traversal() {
            let (temp, core, cx, _workspace) = setup("traversal");
            let outside = temp.0.join("outside");
            fs::create_dir(&outside).unwrap();
            fs::write(outside.join("secret.txt"), b"secret").unwrap();

            for path in [json!(".."), json!(display_path(&outside))] {
                let error = file_list(
                    &core,
                    &cx,
                    &json!({ "args": { "workspaceFileScopeId": "scope", "path": path } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "workspace-file/outside-workspace");
            }

            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(&outside, _workspace.join("escape")).unwrap();
                let error = file_list(
                    &core,
                    &cx,
                    &json!({ "args": { "workspaceFileScopeId": "scope", "path": "escape" } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "workspace-file/outside-workspace");
            }
        }

        #[test]
        fn search_is_workspace_scoped_and_literal() {
            if Command::new(find_ripgrep())
                .arg("--version")
                .output()
                .is_err()
            {
                return;
            }
            let (_temp, core, cx, workspace) = setup("search");
            fs::create_dir_all(workspace.join("src").join("sub")).unwrap();
            fs::write(workspace.join("src").join("SrcAlpha.ts"), "").unwrap();
            fs::write(workspace.join("src").join("sub").join("Beta.ts"), "").unwrap();
            fs::write(workspace.join(".hidden.txt"), "").unwrap();

            let result = file_search(
                &core,
                &cx,
                &json!({ "args": {
                    "workspaceFileScopeId": "scope",
                    "request": { "query": "SRC", "excludedPaths": ["sub"], "includeHidden": true, "respectGitignore": false }
                } }),
            )
            .unwrap();
            assert_eq!(
                result["matches"],
                json!([
                    { "path": "src", "type": "directory" },
                    { "path": "src/SrcAlpha.ts", "type": "file" }
                ])
            );
            assert_eq!(result["truncated"], false);
            assert_eq!(result["incomplete"], false);
            assert_eq!(
                keys(&result),
                HashSet::from(["matches", "truncated", "incomplete"])
            );
        }

        #[test]
        fn text_reads_validate_skipped_lines_and_split_utf8() {
            let (_temp, core, cx, workspace) = setup("text-validation");
            fs::write(workspace.join("invalid.txt"), b"ok\n\xffbad\nend").unwrap();
            let error = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "invalid.txt", "range": { "offset": 2, "limit": 1 } } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "workspace-file/not-text");

            let mut split = vec![b'a'; 65_534];
            split.push(b'\n');
            split.extend_from_slice("侧\nlast".as_bytes());
            fs::write(workspace.join("split.txt"), split).unwrap();
            let page = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "split.txt", "range": { "offset": 2, "limit": 1 } } }),
            )
            .unwrap();
            assert_eq!(page["text"], "侧");
            assert_eq!(page["eof"], false);
        }

        #[tokio::test]
        async fn watches_allow_outside_files_but_reject_outside_directories() {
            let (temp, core, _cx, _workspace) = setup("watch-scope");
            let outside = temp.0.join("watched.txt");
            fs::write(&outside, b"one").unwrap();
            let mut stream = file_changes(
                core,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "../watched.txt" } }),
            );
            assert_eq!(stream.next().await.unwrap().unwrap()["kind"], "ready");
            fs::write(&outside, b"second").unwrap();
            let change = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(change["kind"], "change");
            assert_eq!(change["change"]["absent"], Value::Null);

            let (temp, core, _cx, _workspace) = setup("watch-directory");
            let mut stream = file_changes(
                core,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": ".." } }),
            );
            let error = stream.next().await.unwrap().unwrap_err();
            assert_eq!(error.code, "workspace-file/outside-workspace");
            assert!(temp.0.is_dir());
        }

        #[test]
        fn full_file_cap_does_not_limit_byte_windows() {
            let (_temp, core, cx, workspace) = setup("full-file-cap");
            let path = workspace.join("large.bin");
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.set_len(MAX_FILE_BYTES + 1).unwrap();
            drop(file);

            let error = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "large.bin", "options": {} } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "workspace-file/too-large");
            assert_eq!(error.details["limit"], MAX_FILE_BYTES);

            let window = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "large.bin", "options": { "range": { "length": 1 } } } }),
            )
            .unwrap();
            assert_eq!(window["data"], json!([0]));
            assert_eq!(window["eof"], false);
        }

        #[test]
        fn registry_corruption_is_preserved_and_default_is_not_recreated() {
            let temp = TempRoot::new("registry-safety");
            let core = core_at(&temp.0);
            let cx = context(&temp.0);
            fs::create_dir_all(temp.0.join("storages")).unwrap();
            let registry = temp.0.join("storages").join("rust-workspaces.json");
            fs::write(&registry, b"{broken").unwrap();
            let workspace = temp.0.join("workspace");
            fs::create_dir(&workspace).unwrap();
            let error = create_workspace(&core, &cx, &json!({ "path": display_path(&workspace) }))
                .unwrap_err();
            assert_eq!(error.code, "workspace/io");
            assert_eq!(fs::read(&registry).unwrap(), b"{broken");

            let mut document = empty_registry();
            document["initialized"] = json!(true);
            document["defaultWorkspaceId"] = json!("deleted-default");
            save(&core, &document).unwrap();
            assert_eq!(
                initialize_default(&core, &cx, &json!({})).unwrap(),
                Value::Null
            );
            assert_eq!(
                load(&core).unwrap()["defaultWorkspaceId"],
                "deleted-default"
            );
        }

        #[test]
        fn pin_checks_already_pinned_then_archived_before_existence() {
            let temp = TempRoot::new("pin-order");
            let core = core_at(&temp.0);
            let mut document = empty_registry();
            document["initialized"] = json!(true);
            document["archivedSessionIds"] = json!(["archived"]);
            document["pinnedSessionIds"] = json!(["pinned"]);
            save(&core, &document).unwrap();
            assert_eq!(
                pin_session(&core, &json!({ "sessionId": "pinned" })).unwrap()["pinnedSessionIds"],
                json!(["pinned"])
            );
            assert_eq!(
                pin_session(&core, &json!({ "sessionId": "archived" }))
                    .unwrap_err()
                    .code,
                "gateway/bad-request"
            );
            assert_eq!(
                pin_session(&core, &json!({ "sessionId": "missing" }))
                    .unwrap_err()
                    .code,
                "session/not-found"
            );
        }

        #[test]
        fn numeric_windows_match_javascript_safe_integer_validation() {
            let (_temp, core, cx, workspace) = setup("safe-integers");
            fs::write(workspace.join("file.txt"), b"hello").unwrap();
            let too_big = json!(9_007_199_254_740_992_u64);

            let error = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "range": { "offset": too_big } } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");

            let error = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "options": { "range": { "offset": too_big, "length": 1 } } } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");

            let past_eof = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "range": { "offset": 9_007_199_254_740_991_u64, "limit": 2 } } }),
            )
            .unwrap();
            assert_eq!(past_eof["offset"], 9_007_199_254_740_991_u64);
            assert_eq!(past_eof["lines"], 0);
            assert_eq!(past_eof["eof"], true);

            let error = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "options": { "range": { "offset": 9_007_199_254_740_991_u64, "length": 1 } } } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");

            let integral_floats = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "range": { "offset": 1.0, "limit": 1.0 } } }),
            )
            .unwrap();
            assert_eq!(integral_floats["text"], "hello");

            let bytes = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "options": { "range": { "offset": -0.0, "length": 2.0 } } } }),
            )
            .unwrap();
            assert_eq!(bytes["offset"], 0);
            assert_eq!(bytes["data"], json!([b'h', b'e']));

            for range in [
                json!({ "offset": 1.5 }),
                json!({ "offset": -1.0 }),
                json!({ "offset": null }),
            ] {
                let error = file_read(
                    &core,
                    &cx,
                    &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "range": range } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "gateway/bad-request");
                assert_eq!(error.details, json!({}));
            }
        }

        #[test]
        fn file_response_shapes_and_required_request_objects() {
            let (_temp, core, cx, workspace) = setup("wire-shapes");
            fs::write(workspace.join("file.txt"), b"one\ntwo").unwrap();

            let stat = file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt" } }),
            )
            .unwrap();
            assert_eq!(
                keys(&stat),
                HashSet::from(["absolutePath", "version", "bytes"])
            );

            let text = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "range": {} } }),
            )
            .unwrap();
            assert_eq!(
                keys(&text),
                HashSet::from([
                    "absolutePath",
                    "version",
                    "bytes",
                    "offset",
                    "text",
                    "lines",
                    "eof"
                ])
            );

            let bytes = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt", "options": {} } }),
            )
            .unwrap();
            assert_eq!(
                keys(&bytes),
                HashSet::from(["absolutePath", "version", "bytes", "offset", "data", "eof"])
            );

            let listing = file_list(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "." } }),
            )
            .unwrap();
            assert_eq!(
                keys(&listing),
                HashSet::from(["path", "entries", "truncated"])
            );

            let error = file_read(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt" } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");
            assert_eq!(error.details, json!({}));

            let error = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "file.txt" } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");
            assert_eq!(error.details, json!({}));

            let error = file_search(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope" } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");
            assert_eq!(error.details, json!({}));
        }

        #[test]
        fn scope_root_and_file_kind_errors_match_the_wire_unions() {
            let temp = TempRoot::new("scope-errors");
            let root_file = temp.0.join("not-a-directory");
            fs::write(&root_file, b"x").unwrap();
            add_session(&temp.0, "file-scope", &root_file);
            let core = core_at(&temp.0);
            let cx = context(&temp.0);

            let error = file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "file-scope", "path": "." } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "workspace-file/not-directory");
            assert_eq!(
                error.details,
                json!({ "path": display_path(&fs::canonicalize(&root_file).unwrap()), "kind": "file" })
            );

            let error = file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "missing", "path": "." } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/lookup-not-found");
            assert_eq!(error.details, json!({ "sessionId": "missing" }));

            let (_temp, core, cx, _workspace) = setup("kind-errors");
            let error = file_stat(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "." } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "workspace-file/not-regular-file");
            assert_eq!(error.details, json!({ "path": ".", "kind": "directory" }));

            #[cfg(unix)]
            {
                use std::os::unix::net::UnixListener;

                let socket = _workspace.join("socket");
                let _listener = UnixListener::bind(&socket).unwrap();
                let error = file_stat(
                    &core,
                    &cx,
                    &json!({ "args": { "workspaceFileScopeId": "scope", "path": "socket" } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "workspace-file/not-regular-file");
                assert_eq!(error.details, json!({ "path": "socket", "kind": "other" }));

                let error = file_list(
                    &core,
                    &cx,
                    &json!({ "args": { "workspaceFileScopeId": "scope", "path": "socket" } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "workspace-file/not-directory");
                assert_eq!(error.details, json!({ "path": "socket", "kind": "other" }));

                std::os::unix::fs::symlink(_workspace.join("socket"), _workspace.join("link"))
                    .unwrap();
                let error = file_list(
                    &core,
                    &cx,
                    &json!({ "args": { "workspaceFileScopeId": "scope", "path": "link" } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "workspace-file/not-directory");
                assert_eq!(error.details, json!({ "path": "link", "kind": "symlink" }));
            }
        }

        #[test]
        fn directory_cap_keeps_the_first_lexical_entries() {
            let (_temp, core, cx, workspace) = setup("listing-cap");
            for index in 0..MAX_ENTRIES + 5 {
                fs::write(workspace.join(format!("file-{index:04}")), "").unwrap();
            }

            let listing = file_list(
                &core,
                &cx,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "." } }),
            )
            .unwrap();
            let entries = listing["entries"].as_array().unwrap();
            assert_eq!(entries.len(), MAX_ENTRIES);
            assert_eq!(listing["truncated"], true);
            assert_eq!(entries[0]["name"], "file-0000");
            assert_eq!(entries[MAX_ENTRIES - 1]["name"], "file-1999");
        }

        #[test]
        fn base_file_resolution_normalizes_separators_and_validates_types() {
            let (_temp, core, cx, workspace) = setup("base-file");
            fs::create_dir(workspace.join("nested")).unwrap();
            fs::write(workspace.join("nested").join("base.txt"), "base").unwrap();
            fs::write(workspace.join("nested").join("near.txt"), b"near").unwrap();
            fs::write(workspace.join("near.txt"), b"root").unwrap();

            let bytes = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": {
                    "workspaceFileScopeId": "scope",
                    "path": ".\\near.txt",
                    "options": { "baseFile": "nested/base.txt" }
                } }),
            )
            .unwrap();
            assert_eq!(bytes["data"], json!(b"near"));
            assert_eq!(
                bytes["absolutePath"],
                display_path(&fs::canonicalize(workspace.join("nested").join("near.txt")).unwrap())
            );

            for base in [json!(null), json!(1), json!(true)] {
                let error = file_read_bytes(
                    &core,
                    &cx,
                    &json!({ "args": {
                        "workspaceFileScopeId": "scope",
                        "path": "near.txt",
                        "options": { "baseFile": base }
                    } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "gateway/bad-request");
                assert_eq!(error.details, json!({}));
            }

            let error = file_read_bytes(
                &core,
                &cx,
                &json!({ "args": {
                    "workspaceFileScopeId": "scope",
                    "path": display_path(&workspace.join("near.txt")),
                    "options": { "baseFile": "nested/base.txt" }
                } }),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request");
            assert_eq!(error.details, json!({}));

            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(
                    workspace.join("nested").join("near.txt"),
                    workspace.join("nested").join("link.txt"),
                )
                .unwrap();
                let error = file_read_bytes(
                    &core,
                    &cx,
                    &json!({ "args": {
                        "workspaceFileScopeId": "scope",
                        "path": "link.txt",
                        "options": { "baseFile": "nested/base.txt" }
                    } }),
                )
                .unwrap_err();
                assert_eq!(error.code, "workspace-file/not-regular-file");
                assert_eq!(error.details["kind"], "symlink");
            }
        }

        #[test]
        fn optional_workspace_fields_reject_wrong_json_types() {
            let temp = TempRoot::new("optional-fields");
            let one = temp.0.join("one");
            let two = temp.0.join("two");
            fs::create_dir(&one).unwrap();
            fs::create_dir(&two).unwrap();
            let core = core_at(&temp.0);
            let cx = context(&temp.0);
            let first =
                create_workspace(&core, &cx, &json!({ "path": display_path(&one) })).unwrap();
            let second =
                create_workspace(&core, &cx, &json!({ "path": display_path(&two) })).unwrap();
            let one_id = first["workspace"]["workspaceId"].as_str().unwrap();
            let two_id = second["workspace"]["workspaceId"].as_str().unwrap();

            for anchor in [json!(null), json!(1), json!(false)] {
                let error = insert_workspace_before(
                    &core,
                    &json!({ "workspaceId": one_id, "beforeWorkspaceId": anchor }),
                )
                .unwrap_err();
                assert_eq!(error.code, "gateway/bad-request");
                assert_eq!(error.details, json!({}));
            }
            assert_eq!(
                insert_workspace_before(
                    &core,
                    &json!({ "workspaceId": one_id, "beforeWorkspaceId": two_id }),
                )
                .unwrap()["workspaceIds"],
                json!([one_id, two_id])
            );
            assert_eq!(
                insert_workspace_before(&core, &json!({ "workspaceId": one_id })).unwrap()
                    ["workspaceIds"],
                json!([two_id, one_id])
            );

            add_session(&temp.0, "s1", &one);
            add_session(&temp.0, "s2", &one);
            attach_created_session(&core, "s1").unwrap();
            attach_created_session(&core, "s2").unwrap();
            for anchor in [json!(null), json!(1), json!(false)] {
                let error = insert_session_before(
                    &core,
                    &json!({ "workspaceId": one_id, "sessionId": "s1", "beforeSessionId": anchor }),
                )
                .unwrap_err();
                assert_eq!(error.code, "gateway/bad-request");
                assert_eq!(error.details, json!({}));
            }
            assert_eq!(
                insert_session_before(
                    &core,
                    &json!({ "workspaceId": one_id, "sessionId": "s1", "beforeSessionId": "s2" }),
                )
                .unwrap()["workspace"]["sessionIds"],
                json!(["s1", "s2"])
            );

            for stop in [json!(null), json!(0), json!("false")] {
                let error = archive_session(
                    &core,
                    &cx,
                    &json!({ "sessionId": "s1", "stopActivity": stop }),
                )
                .unwrap_err();
                assert_eq!(error.code, "gateway/bad-request");
                assert_eq!(error.details, json!({}));
            }
        }

        #[test]
        fn workspace_lifecycle_and_session_sets_are_idempotent() {
            let temp = TempRoot::new("lifecycle");
            let one = temp.0.join("one");
            let two = temp.0.join("two");
            fs::create_dir(&one).unwrap();
            fs::create_dir(&two).unwrap();
            let core = core_at(&temp.0);
            let cx = context(&temp.0);
            let first = create_workspace(
                &core,
                &cx,
                &json!({ "args": { "request": { "path": display_path(&one) } } }),
            )
            .unwrap();
            let second =
                create_workspace(&core, &cx, &json!({ "path": display_path(&two) })).unwrap();
            let one_id = first["workspace"]["workspaceId"].as_str().unwrap();
            let two_id = second["workspace"]["workspaceId"].as_str().unwrap();

            let renamed = rename_workspace(
                &core,
                &json!({ "workspaceId": one_id, "title": "  Renamed  " }),
            )
            .unwrap();
            assert_eq!(renamed["workspace"]["title"], "Renamed");
            assert_eq!(
                keys(&renamed["workspace"]),
                HashSet::from([
                    "workspaceId",
                    "path",
                    "title",
                    "sessionIds",
                    "createdAt",
                    "updatedAt"
                ])
            );
            let error =
                rename_workspace(&core, &json!({ "workspaceId": two_id, "title": "Renamed" }))
                    .unwrap_err();
            assert_eq!(error.code, "workspace/name-conflict");
            assert_eq!(error.details, json!({ "name": "Renamed" }));

            assert_eq!(
                insert_workspace_before(
                    &core,
                    &json!({ "workspaceId": one_id, "beforeWorkspaceId": two_id })
                )
                .unwrap()["workspaceIds"],
                json!([one_id, two_id])
            );
            assert_eq!(
                insert_workspace_before(
                    &core,
                    &json!({ "workspaceId": one_id, "beforeWorkspaceId": one_id })
                )
                .unwrap()["workspaceIds"],
                json!([one_id, two_id])
            );
            assert_eq!(
                delete_workspace(&core, &json!({ "workspaceId": two_id })).unwrap(),
                json!({ "deleted": true })
            );
            let error = delete_workspace(&core, &json!({ "workspaceId": two_id })).unwrap_err();
            assert_eq!(error.code, "workspace/not-found");
            assert_eq!(error.details, json!({ "workspaceId": two_id }));
            assert_eq!(
                initialize_default(&core, &cx, &json!({})).unwrap(),
                Value::Null
            );

            add_session(&temp.0, "s1", &one);
            attach_created_session(&core, "s1").unwrap();
            assert_eq!(
                pin_session(&core, &json!({ "sessionId": "s1" })).unwrap()["pinnedSessionIds"],
                json!(["s1"])
            );
            assert_eq!(
                unpin_session(&core, &json!({ "sessionId": "s1" })).unwrap()["pinnedSessionIds"],
                json!([])
            );
            assert_eq!(
                unpin_session(&core, &json!({ "sessionId": "s1" })).unwrap()["pinnedSessionIds"],
                json!([])
            );
            assert_eq!(
                archive_session(&core, &cx, &json!({ "sessionId": "s1" })).unwrap()
                    ["archivedSessionIds"],
                json!(["s1"])
            );
            assert_eq!(
                archive_session(&core, &cx, &json!({ "sessionId": "s1" })).unwrap()
                    ["archivedSessionIds"],
                json!(["s1"])
            );
            assert_eq!(
                unarchive_session(&core, &json!({ "sessionId": "s1" })).unwrap()
                    ["archivedSessionIds"],
                json!([])
            );
            assert_eq!(
                unarchive_session(&core, &json!({ "sessionId": "s1" })).unwrap()
                    ["archivedSessionIds"],
                json!([])
            );
        }

        #[test]
        fn owned_session_survives_a_missing_fully_qualified_cwd_spelling() {
            let temp = TempRoot::new("missing-cwd");
            let workspace = temp.0.join("workspace");
            fs::create_dir_all(workspace.join("nested")).unwrap();
            let core = core_at(&temp.0);
            let cx = context(&temp.0);
            let created =
                create_workspace(&core, &cx, &json!({ "path": display_path(&workspace) })).unwrap();
            let spelling = workspace.join("nested").join("..");
            add_session(&temp.0, "owned", &spelling);
            attach_created_session(&core, "owned").unwrap();
            fs::remove_dir_all(&workspace).unwrap();
            assert!(fs::canonicalize(&spelling).is_err());

            let document = load(&core).unwrap();
            let view = workspace_view(&core, &document["workspaces"][0]);
            assert_eq!(view["workspaceId"], created["workspace"]["workspaceId"]);
            assert_eq!(view["sessionIds"], json!(["owned"]));
        }

        #[test]
        fn register_all_smoke_and_windows_unc_display() {
            let temp = TempRoot::new("registration");
            let gateway = Gateway::new("test", temp.0.to_string_lossy(), None);
            register_all(&gateway, context(&temp.0));

            #[cfg(windows)]
            {
                assert_eq!(
                    display_path(Path::new(r"\\?\UNC\server\share\folder")),
                    r"\\server\share\folder"
                );
                assert_eq!(display_path(Path::new(r"\\?\C:\folder")), r"C:\folder");
            }
        }

        #[tokio::test]
        async fn directory_watches_report_direct_entry_and_kind_changes() {
            let (_temp, core, _cx, workspace) = setup("directory-watch");
            let mut stream = file_changes(
                core.clone(),
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "." } }),
            );
            assert_eq!(stream.next().await.unwrap().unwrap()["kind"], "ready");

            let child = workspace.join("child");
            fs::write(&child, b"one").unwrap();
            let created = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(created["kind"], "change");
            assert_eq!(
                created["change"]["absolutePath"],
                display_path(&fs::canonicalize(&workspace).unwrap())
            );
            assert_eq!(created["change"]["absent"], Value::Null);

            fs::write(&child, b"a longer value").unwrap();
            let modified = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(modified["kind"], "change");
            assert_eq!(modified["change"]["absent"], Value::Null);

            fs::remove_file(&child).unwrap();
            let removed = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(removed["change"]["absent"], Value::Null);
            assert_eq!(
                removed["change"]["absolutePath"],
                display_path(&fs::canonicalize(&workspace).unwrap())
            );
            drop(stream);

            fs::write(&child, b"file").unwrap();
            let mut stream = file_changes(
                core,
                &json!({ "args": { "workspaceFileScopeId": "scope", "path": "child" } }),
            );
            assert_eq!(stream.next().await.unwrap().unwrap()["kind"], "ready");
            fs::remove_file(&child).unwrap();
            fs::create_dir(&child).unwrap();
            let changed_kind = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(changed_kind["kind"], "change");
            assert_eq!(changed_kind["change"]["absent"], Value::Null);
            fs::remove_dir(&child).unwrap();
            let absent = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(absent["change"]["absent"], true);
        }
    }
}
