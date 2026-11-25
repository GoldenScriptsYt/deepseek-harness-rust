use super::*;
use dsh_session_format::{scan, HeaderTag, Recovery};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;

struct TempRoot {
    path: PathBuf,
    _directory: tempfile::TempDir,
}

impl TempRoot {
    fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("dsh-session-store-")
            .tempdir()
            .unwrap();
        let path = directory.path().to_path_buf();
        Self {
            path,
            _directory: directory,
        }
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn header(id: &str) -> Header {
    Header {
        kind: HeaderTag::Session,
        version: SESSION_FORMAT_VERSION,
        id: id.into(),
        created_at: 1,
        cwd: Some("/work/proj".into()),
        parent_session: None,
        is_seeded: false,
        origin: None,
        delegation_depth: 0,
        agent_preset: None,
    }
}

fn event(kind: &str) -> Event {
    Event {
        kind: kind.into(),
        data: json!({"turn": 1}),
        extra: Default::default(),
    }
}

fn collect_files(dir: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, output);
        } else if path
            .file_name()
            .is_some_and(|name| name == "session.v4.jsonl")
        {
            output.push(path);
        }
    }
}

#[test]
fn create_append_reopen() {
    let root = TempRoot::new();
    let store = Store::new(&root.path);
    let mut session = store.create(header("s1")).unwrap();
    session
        .append(&[event("turn/start"), event("turn/end")])
        .unwrap();
    drop(session);
    let reopened = store.open(Some("/work/proj"), "s1").unwrap();
    assert_eq!(reopened.events.len(), 2);
    assert!(matches!(
        store.create(header("s1")),
        Err(StoreError::Exists(_))
    ));
    assert_eq!(store.list(Some("/work/proj")).unwrap(), vec!["s1"]);
    assert_eq!(
        store.metadata(Some("/work/proj"), "s1").unwrap().header.id,
        "s1"
    );
}

#[test]
fn mixed_store_reads_and_appends_to_each_original_encoding() {
    let root = TempRoot::new();
    let plain = Store::new(&root.path);
    let zstd = Store::with_compression(&root.path, Compression::Zstd);
    let mut first = plain.create(header("plain")).unwrap();
    first.append(&[event("turn/start")]).unwrap();
    drop(first);
    let mut second = zstd.create(header("compressed")).unwrap();
    second.append(&[event("turn/start")]).unwrap();
    let original = second.path().to_path_buf();
    drop(second);

    let mixed = Store::mixed(&root.path);
    assert_eq!(mixed.list(Some("/work/proj")).unwrap().len(), 2);
    assert_eq!(mixed.list_metadata(Some("/work/proj")).unwrap().len(), 2);
    assert_eq!(mixed.metadata_by_id("compressed").unwrap().path, original);
    let mut opened = mixed.open(Some("/work/proj"), "compressed").unwrap();
    assert_eq!(opened.events.len(), 1);
    opened.append(&[event("turn/end")]).unwrap();
    drop(opened);
    assert_eq!(
        zstd.open(Some("/work/proj"), "compressed")
            .unwrap()
            .events
            .len(),
        2
    );
    assert!(!original.with_extension("").exists());
    let mut events = Vec::new();
    mixed
        .read_streamed(Some("/work/proj"), "compressed", |event| {
            events.push(event);
            Ok(())
        })
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(mixed.read_all().len(), 2);
    assert!(matches!(
        mixed.create(header("compressed")),
        Err(StoreError::Exists(_))
    ));
}

#[test]
fn mixed_store_rejects_duplicate_or_newer_generations() {
    let root = TempRoot::new();
    let plain = Store::new(&root.path);
    let zstd = Store::with_compression(&root.path, Compression::Zstd);
    drop(plain.create(header("duplicate")).unwrap());
    drop(zstd.create(header("duplicate")).unwrap());
    let mixed = Store::mixed(&root.path);
    assert!(matches!(
        mixed.open(Some("/work/proj"), "duplicate"),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        mixed.list_metadata(Some("/work/proj")),
        Err(StoreError::Corrupt(_))
    ));

    let separate = TempRoot::new();
    drop(Store::new(&separate.path).create(header("newer")).unwrap());
    let dir = session_dir(&separate.path, Some("/work/proj"), "newer").unwrap();
    fs::write(dir.join("session.v5.jsonl.zstd"), b"future").unwrap();
    assert!(matches!(
        Store::mixed(&separate.path).open(Some("/work/proj"), "newer"),
        Err(StoreError::MigrationRequired { found: 5, .. })
    ));
}

#[test]
fn open_truncates_torn_tail_and_appends_cleanly() {
    let root = TempRoot::new();
    let store = Store::new(&root.path);
    let mut session = store.create(header("s2")).unwrap();
    session.append(&[event("turn/start")]).unwrap();
    let path = session.path().to_path_buf();
    drop(session);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{\"type\":\"turn/en")
        .unwrap();

    let mut session = store.open(Some("/work/proj"), "s2").unwrap();
    assert_eq!(session.events.len(), 1);
    session.append(&[event("turn/end")]).unwrap();
    drop(session);
    let again = store.open(Some("/work/proj"), "s2").unwrap();
    assert_eq!(again.events.len(), 2);
}

#[test]
fn older_generation_requires_migration() {
    let root = TempRoot::new();
    let dir = session_dir(&root.path, Some("/work/proj"), "s3").unwrap();
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("session.v3.jsonl"), b"x\n").unwrap();
    let error = Store::new(&root.path)
        .open(Some("/work/proj"), "s3")
        .err()
        .unwrap();
    assert!(matches!(
        error,
        StoreError::MigrationRequired { found: 3, .. }
    ));
}

#[test]
fn read_all_lists_sessions_across_projects_newest_first() {
    let root = TempRoot::new();
    let store = Store::new(&root.path);
    let mut first = store.create(header("a")).unwrap();
    first.append(&[event("turn/start")]).unwrap();
    let mut other = header("b");
    other.cwd = Some("/other".into());
    store.create(other).unwrap();
    drop(first);
    let a_path = store.metadata(Some("/work/proj"), "a").unwrap().path;
    let b_path = store.metadata(Some("/other"), "b").unwrap().path;
    let times = fs::FileTimes::new()
        .set_accessed(UNIX_EPOCH)
        .set_modified(UNIX_EPOCH + Duration::from_secs(10));
    fs::File::options()
        .write(true)
        .open(&a_path)
        .unwrap()
        .set_times(times)
        .unwrap();
    fs::File::options()
        .write(true)
        .open(&b_path)
        .unwrap()
        .set_times(times)
        .unwrap();
    fs::create_dir_all(root.path.join("--junk--").join("x")).unwrap();

    let all = store.read_all();
    assert_eq!(
        all.iter()
            .map(|stored| stored.header.id.as_str())
            .collect::<Vec<_>>(),
        ["b", "a"]
    );
    assert_eq!(all[1].events.len(), 1);
}

#[test]
fn missing_session_is_not_found() {
    let root = TempRoot::new();
    let error = Store::new(&root.path).open(None, "nope").err().unwrap();
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[test]
fn listing_reads_only_headers_and_cache_tracks_revision() {
    let root = TempRoot::new();
    let store = Store::new(&root.path);
    let mut session = store.create(header("cached")).unwrap();
    let first = store.metadata(Some("/work/proj"), "cached").unwrap();
    session.append(&[event("turn/start")]).unwrap();
    let second = store.metadata(Some("/work/proj"), "cached").unwrap();
    assert_ne!(first.revision, second.revision);
    assert!(second.size_bytes > first.size_bytes);
    let path = session.path().to_path_buf();
    drop(session);

    let bytes = fs::read(&path).unwrap();
    let header_end = bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    let mut corrupt = bytes[..header_end].to_vec();
    corrupt.extend_from_slice(b"not json\n");
    fs::write(&path, corrupt).unwrap();
    let listed = store.list_metadata(Some("/work/proj")).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].header.id, "cached");
    assert_eq!(store.list(Some("/work/proj")).unwrap(), vec!["cached"]);
    assert!(store.read_all()[0].events.is_empty());
}

#[test]
fn streamed_read_delivers_events_and_propagates_callback_errors() {
    let root = TempRoot::new();
    let store = Store::new(&root.path);
    let mut session = store.create(header("stream")).unwrap();
    let large = event("turn/start");
    let mut large = large;
    large.data = json!({"text": "x".repeat(200_000)});
    session.append(&[large.clone(), event("turn/end")]).unwrap();
    drop(session);

    let mut seen = Vec::new();
    let summary = store
        .read_streamed(Some("/work/proj"), "stream", |event| {
            seen.push(event.kind.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(summary.event_count, 2);
    assert_eq!(seen, ["turn/start", "turn/end"]);
    let error = store
        .read_streamed(Some("/work/proj"), "stream", |_| {
            Err(StoreError::NotFound("callback stopped".into()))
        })
        .err()
        .unwrap();
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[test]
fn compressed_create_append_reopen_and_listing() {
    let root = TempRoot::new();
    let store = Store::with_compression(&root.path, Compression::Zstd);
    let mut session = store.create(header("zstd")).unwrap();
    assert!(session.path().ends_with("session.v4.jsonl.zstd"));
    session.append(&[event("turn/start")]).unwrap();
    session.append(&[event("turn/end")]).unwrap();
    let listed = store.list_metadata(Some("/work/proj")).unwrap();
    assert_eq!(listed[0].header.id, "zstd");
    assert_eq!(
        listed[0].size_bytes,
        fs::metadata(session.path()).unwrap().len()
    );
    drop(session);

    let reopened = store.open(Some("/work/proj"), "zstd").unwrap();
    assert_eq!(
        reopened
            .events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        ["turn/start", "turn/end"]
    );
    drop(reopened);
    assert_eq!(store.read_all()[0].events.len(), 2);
    assert!(Store::new(&root.path)
        .list_metadata(Some("/work/proj"))
        .unwrap()
        .is_empty());
}

#[test]
fn compressed_listing_does_not_decode_corrupt_event_checksum() {
    let root = TempRoot::new();
    let store = Store::with_compression(&root.path, Compression::Zstd);
    let mut session = store.create(header("bad-body")).unwrap();
    session.append(&[event("turn/start")]).unwrap();
    let path = session.path().to_path_buf();
    drop(session);
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xFF;
    fs::write(&path, bytes).unwrap();

    assert_eq!(
        store.list_metadata(Some("/work/proj")).unwrap()[0]
            .header
            .id,
        "bad-body"
    );
    assert!(store.open(Some("/work/proj"), "bad-body").is_err());
    assert!(store.read_all().is_empty());
}

#[test]
fn compressed_torn_frame_recovers_complete_rows_and_republishes_atomically() {
    let root = TempRoot::new();
    let store = Store::with_compression(&root.path, Compression::Zstd);
    let mut session = store.create(header("torn-zstd")).unwrap();
    session.append(&[event("turn/start")]).unwrap();
    let path = session.path().to_path_buf();
    let first = fs::read(&path).unwrap();
    let first_len = first.len();
    session.append(&[event("turn/end")]).unwrap();
    let complete = fs::read(&path).unwrap();
    drop(session);
    assert!(complete.len() > first.len());
    let mut torn = first;
    torn.extend_from_slice(&complete[first_len..complete.len() - 2]);
    fs::write(&path, torn).unwrap();

    let mut reopened = store.open(Some("/work/proj"), "torn-zstd").unwrap();
    assert_eq!(reopened.events.len(), 2);
    reopened.append(&[event("turn/end")]).unwrap();
    drop(reopened);
    assert_eq!(
        store
            .open(Some("/work/proj"), "torn-zstd")
            .unwrap()
            .events
            .len(),
        3
    );
    assert_eq!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count(),
        0
    );
}

#[test]
fn committed_corruption_followed_by_turn_end_is_not_recovered() {
    let root = TempRoot::new();
    let store = Store::new(&root.path);
    let session = store.create(header("corrupt-midlog")).unwrap();
    let path = session.path().to_path_buf();
    drop(session);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"not json\n{\"type\":\"turn/end\",\"data\":{}}\n")
        .unwrap();

    assert!(matches!(
        store.open(Some("/work/proj"), "corrupt-midlog"),
        Err(StoreError::Format(FormatError::Unparsable(1)))
    ));
}

fn assert_append_rollback(compression: Compression) {
    let root = TempRoot::new();
    let store = Store::with_compression(&root.path, compression);
    let mut session = store.create(header("rollback")).unwrap();
    session.append(&[event("turn/start")]).unwrap();
    let before = fs::read(session.path()).unwrap();
    for (failure, operation) in [
        (TestFailure::Write, AppendOperation::Write),
        (TestFailure::Sync, AppendOperation::Sync),
    ] {
        session.failure = Some(failure);
        let error = session.append(&[event("turn/end")]).err().unwrap();
        assert!(
            matches!(error, StoreError::Append { operation: actual, rollback: None, .. } if actual == operation)
        );
        assert_eq!(fs::read(session.path()).unwrap(), before);
        assert_eq!(session.events.len(), 1);
    }
    session.append(&[event("turn/end")]).unwrap();
    assert_eq!(session.events.len(), 2);
    drop(session);
    assert_eq!(
        store
            .open(Some("/work/proj"), "rollback")
            .unwrap()
            .events
            .len(),
        2
    );
}

#[test]
fn plaintext_append_rolls_back_write_and_sync_failures() {
    assert_append_rollback(Compression::None);
}

#[test]
fn compressed_append_rolls_back_write_and_sync_failures() {
    assert_append_rollback(Compression::Zstd);
}

#[test]
fn write_lease_excludes_writers_but_not_readers_and_releases_on_drop() {
    let root = TempRoot::new();
    let first = Store::new(&root.path);
    let second = Store::new(&root.path);
    let session = first.create(header("leased")).unwrap();
    assert!(matches!(
        second.open(Some("/work/proj"), "leased"),
        Err(StoreError::AlreadyOwned(_))
    ));
    assert_eq!(
        second
            .metadata(Some("/work/proj"), "leased")
            .unwrap()
            .header
            .id,
        "leased"
    );
    let count = second
        .read_streamed(Some("/work/proj"), "leased", |_| Ok(()))
        .unwrap()
        .event_count;
    assert_eq!(count, 0);
    #[cfg(unix)]
    {
        let dir = session_dir(&root.path, Some("/work/proj"), "leased").unwrap();
        let lock = dir.join("session.lock");
        assert!(lock.exists());
        let times = fs::FileTimes::new().set_modified(UNIX_EPOCH);
        fs::File::options()
            .write(true)
            .open(&lock)
            .unwrap()
            .set_times(times)
            .unwrap();
        assert!(matches!(
            second.open(Some("/work/proj"), "leased"),
            Err(StoreError::AlreadyOwned(_))
        ));
    }
    drop(session);
    assert!(second.open(Some("/work/proj"), "leased").is_ok());
}

#[test]
fn compressed_fixture_corpus_round_trips_through_streaming_codec() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../snapshots");
    let mut files = Vec::new();
    collect_files(&root, &mut files);
    assert_eq!(files.len(), 319, "recorded fixture corpus changed");
    let temp = TempRoot::new();
    let path = temp.path.join("session.v4.jsonl.zstd");
    for file in files {
        let bytes = fs::read(&file).unwrap();
        let expected = scan(&bytes, Recovery::Strict)
            .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
        let mut plain_events = Vec::new();
        let plain = crate::codec::read_plaintext(
            &file,
            &mut crate::codec::CollectEvents(&mut plain_events),
        )
        .unwrap();
        assert_eq!(plain.header, expected.header, "{}", file.display());
        assert_eq!(plain_events, expected.events, "{}", file.display());

        let mut artifact =
            crate::codec::compress_frame(encode_header(&expected.header).as_bytes()).unwrap();
        let mut body = String::new();
        for event in &expected.events {
            body.push_str(&encode_event(event));
        }
        if !body.is_empty() {
            artifact.extend(crate::codec::compress_frame(body.as_bytes()).unwrap());
        }
        fs::write(&path, artifact).unwrap();
        let mut actual_events = Vec::new();
        let actual =
            crate::codec::read_zstd(&path, &mut crate::codec::CollectEvents(&mut actual_events))
                .unwrap();
        assert_eq!(actual.header, expected.header, "{}", file.display());
        assert_eq!(actual_events, expected.events, "{}", file.display());
        assert!(!actual.repair_required, "{}", file.display());
    }
}
