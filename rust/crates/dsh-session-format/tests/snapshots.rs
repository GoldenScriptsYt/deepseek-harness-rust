//! Reads every recorded v4 Session in `snapshots/` and checks the codec round-trips it.

use dsh_session_format::{encode_event, encode_header, scan, Recovery};
use serde_json::Value;
use std::path::{Path, PathBuf};

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.file_name().is_some_and(|n| n == "session.v4.jsonl") {
            out.push(path);
        }
    }
}

#[test]
fn every_recorded_v4_session_round_trips() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../snapshots");
    let mut files = Vec::new();
    collect(&root, &mut files);
    assert!(
        files.len() > 100,
        "expected the recorded snapshot corpus, found {}",
        files.len()
    );

    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        let scanned =
            scan(&bytes, Recovery::Strict).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        assert_eq!(
            scanned.committed_bytes,
            bytes.len(),
            "{}: whole file committed",
            file.display()
        );

        let mut rewritten = encode_header(&scanned.header);
        for event in &scanned.events {
            rewritten.push_str(&encode_event(event));
        }
        // Compare as JSON values per line: key order and whitespace are not the contract.
        let original: Vec<Value> = String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let again: Vec<Value> = rewritten
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(original, again, "{}: round trip differs", file.display());
    }
}

#[test]
fn torn_tail_is_not_committed() {
    let log = b"{\"type\":\"session\",\"version\":4,\"id\":\"s\",\"createdAt\":0,\"isSeeded\":false,\"delegationDepth\":0}\n{\"type\":\"turn/start\",\"data\":{\"turn\":1}}\n{\"type\":\"turn/en";
    let scanned = scan(log, Recovery::Strict).unwrap();
    assert_eq!(scanned.events.len(), 1);
    assert_eq!(&log[scanned.committed_bytes..], b"{\"type\":\"turn/en");
}

#[test]
fn refuses_other_versions_and_retired_fields() {
    let v5 = b"{\"type\":\"session\",\"version\":5,\"id\":\"s\",\"createdAt\":0,\"isSeeded\":false,\"delegationDepth\":0}\n";
    assert!(scan(v5, Recovery::Strict).is_err());
    let retired = b"{\"type\":\"session\",\"version\":4,\"id\":\"s\",\"createdAt\":0,\"isSeeded\":false,\"delegationDepth\":0,\"sandboxMode\":\"x\"}\n";
    assert!(scan(retired, Recovery::Strict).is_err());
}

#[test]
fn recoverable_mode_drops_corrupt_tail_but_strict_fails() {
    let log = b"{\"type\":\"session\",\"version\":4,\"id\":\"s\",\"createdAt\":0,\"isSeeded\":false,\"delegationDepth\":0}\n{\"type\":\"turn/start\",\"data\":{}}\nnot json\n";
    assert!(scan(log, Recovery::Strict).is_err());
    let scanned = scan(log, Recovery::Recoverable).unwrap();
    assert_eq!(scanned.events.len(), 1);
}
