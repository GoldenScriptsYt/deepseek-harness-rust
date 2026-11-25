//! File-backed v4 Session store with streaming reads and optional Zstandard frames.
//!
//! Plaintext remains the default. Compressed artifacts use concatenated,
//! independently checksummed frames: one header frame followed by one frame per
//! durable append. Write handles hold a kernel lease until they are dropped.

mod codec;
mod lease;

pub use dsh_session_format::layout::Compression;

use codec::{read_plaintext, read_zstd, read_zstd_header, CallbackEvents, CollectEvents};
use dsh_session_format::layout::{
    encode_segment, generation_log_filename, parse_generation_log_filename, project_dir,
    session_dir,
};
use dsh_session_format::{
    encode_event, encode_header, Event, FormatError, Header, SESSION_FORMAT_VERSION,
};
use lease::WriteLease;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error("session path is not encodable (empty id or cwd)")]
    BadPath,
    #[error("session {0} already exists")]
    Exists(String),
    #[error("session {0} not found")]
    NotFound(String),
    #[error("session {0} is owned by another writer")]
    AlreadyOwned(String),
    #[error("session {id} has only generation v{found}; migration to v{expected} is required")]
    MigrationRequired {
        id: String,
        found: u64,
        expected: u64,
    },
    #[error("corrupt session artifact: {0}")]
    Corrupt(String),
    #[error("session {id} append {operation} failed: {source}")]
    Append {
        id: String,
        operation: AppendOperation,
        source: Box<StoreError>,
        #[source]
        rollback: Option<Box<StoreError>>,
    },
    #[error("session {id} publication failed: {source}")]
    Publication {
        id: String,
        source: Box<StoreError>,
        #[source]
        cleanup: Option<Box<StoreError>>,
    },
}

/// The physical operation that failed during an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOperation {
    Write,
    Sync,
}

impl std::fmt::Display for AppendOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Write => "write",
            Self::Sync => "sync",
        })
    }
}

/// Opaque stat-derived artifact identity used to guard the header cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRevision {
    pub size_bytes: u64,
    pub modified: SystemTime,
}

/// Header-only listing metadata. Event bodies are never decoded for this path.
#[derive(Debug, Clone)]
pub struct SessionMetadata {
    pub header: Header,
    pub path: PathBuf,
    pub modified_ms: u64,
    pub size_bytes: u64,
    pub revision: ArtifactRevision,
}

/// Summary of a streamed read.
#[derive(Debug, Clone)]
pub struct ReadSummary {
    pub header: Header,
    pub event_count: usize,
    pub modified_ms: u64,
}

#[derive(Clone)]
struct CachedHeader {
    revision: ArtifactRevision,
    header: Header,
}

/// A Session store rooted at one directory.
pub struct Store {
    root: PathBuf,
    compression: Compression,
    mixed: bool,
    headers: Mutex<HashMap<PathBuf, CachedHeader>>,
}

impl Store {
    /// Construct a plaintext store, preserving the original API and artifacts.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_compression(root, Compression::None)
    }

    /// Construct a store for one physical encoding.
    pub fn with_compression(root: impl Into<PathBuf>, compression: Compression) -> Self {
        Self {
            root: root.into(),
            compression,
            mixed: false,
            headers: Mutex::new(HashMap::new()),
        }
    }

    /// Read existing plaintext and zstd Sessions without changing their encoding.
    /// New Sessions use `compression`; duplicate encodings fail closed.
    pub fn with_mixed_compression(root: impl Into<PathBuf>, compression: Compression) -> Self {
        let mut store = Self::with_compression(root, compression);
        store.mixed = true;
        store
    }

    /// Read both encodings while retaining plaintext for newly created Sessions.
    pub fn mixed(root: impl Into<PathBuf>) -> Self {
        Self::with_mixed_compression(root, Compression::None)
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }

    fn dir(&self, header_cwd: Option<&str>, id: &str) -> Result<PathBuf, StoreError> {
        session_dir(&self.root, header_cwd, id).ok_or(StoreError::BadPath)
    }

    fn current_path(&self, dir: &Path) -> PathBuf {
        dir.join(generation_log_filename(
            SESSION_FORMAT_VERSION,
            self.compression,
        ))
    }

    fn existing_path(&self, dir: &Path, id: &str) -> Result<(PathBuf, Compression), StoreError> {
        if !self.mixed {
            return Ok((self.current_path(dir), self.compression));
        }
        let plain = newest_generation(dir, Compression::None)?;
        let zstd = newest_generation(dir, Compression::Zstd)?;
        let highest = match (plain, zstd) {
            (None, None) => return Err(StoreError::NotFound(id.into())),
            (Some(a), Some(b)) if a == b => {
                return Err(StoreError::Corrupt(format!(
                    "session {id:?} has duplicate encodings for generation v{a}"
                )))
            }
            (Some(a), Some(b)) if a > b => (a, Compression::None),
            (Some(_), Some(b)) => (b, Compression::Zstd),
            (Some(a), None) => (a, Compression::None),
            (None, Some(b)) => (b, Compression::Zstd),
        };
        if highest.0 != SESSION_FORMAT_VERSION {
            return Err(StoreError::MigrationRequired {
                id: id.into(),
                found: highest.0,
                expected: SESSION_FORMAT_VERSION,
            });
        }
        Ok((
            dir.join(generation_log_filename(highest.0, highest.1)),
            highest.1,
        ))
    }

    fn missing(&self, dir: &Path, id: &str) -> StoreError {
        match newest_generation(dir, self.compression) {
            Ok(Some(found)) if found != SESSION_FORMAT_VERSION => StoreError::MigrationRequired {
                id: id.into(),
                found,
                expected: SESSION_FORMAT_VERSION,
            },
            _ => StoreError::NotFound(id.into()),
        }
    }

    /// Create and durably publish a header-only Session.
    pub fn create(&self, header: Header) -> Result<Session, StoreError> {
        let dir = self.dir(header.cwd.as_deref(), &header.id)?;
        let path = self.current_path(&dir);
        if path.exists()
            || (self.mixed
                && (newest_generation(&dir, Compression::None)?.is_some()
                    || newest_generation(&dir, Compression::Zstd)?.is_some()))
        {
            return Err(StoreError::Exists(header.id));
        }
        let lease = WriteLease::acquire(&dir, &header.id)?;
        if path.exists()
            || (self.mixed
                && (newest_generation(&dir, Compression::None)?.is_some()
                    || newest_generation(&dir, Compression::Zstd)?.is_some()))
        {
            return Err(StoreError::Exists(header.id));
        }
        publish_artifact(&dir, &path, &header, &[], self.compression)?;
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        Ok(Session {
            header,
            events: Vec::new(),
            path,
            file,
            compression: self.compression,
            _lease: lease,
            #[cfg(test)]
            failure: None,
        })
    }

    /// Open one Session and repair a torn physical tail while holding its write lease.
    pub fn open(&self, cwd: Option<&str>, id: &str) -> Result<Session, StoreError> {
        let dir = self.dir(cwd, id)?;
        let lease = WriteLease::acquire(&dir, id)?;
        let (path, compression) = self.existing_path(&dir, id)?;
        let mut events = Vec::new();
        let decoded = match compression {
            Compression::None => read_plaintext(&path, &mut CollectEvents(&mut events)),
            Compression::Zstd => read_zstd(&path, &mut CollectEvents(&mut events)),
        };
        let decoded = match decoded {
            Ok(decoded) => decoded,
            Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(self.missing(&dir, id));
            }
            Err(error) => return Err(error),
        };
        if decoded.repair_required {
            if compression == Compression::Zstd {
                // Atomic republication preserves events recovered from a torn frame;
                // a crash during repair leaves the original artifact for retry.
                publish_artifact(&dir, &path, &decoded.header, &events, compression)?;
            } else {
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(decoded.committed_bytes)?;
                file.sync_all()?;
            }
        }
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Session {
            header: decoded.header,
            events,
            path,
            file,
            compression,
            _lease: lease,
            #[cfg(test)]
            failure: None,
        })
    }

    /// Header-only metadata for one current-generation Session.
    pub fn metadata(&self, cwd: Option<&str>, id: &str) -> Result<SessionMetadata, StoreError> {
        let dir = self.dir(cwd, id)?;
        let (path, compression) = self.existing_path(&dir, id)?;
        match self.cached_header(&path, compression) {
            Ok(metadata) => Ok(metadata),
            Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                Err(self.missing(&dir, id))
            }
            Err(error) => Err(error),
        }
    }

    /// Look up a current Session by ID across projects without reading event bodies.
    pub fn metadata_by_id(&self, id: &str) -> Result<SessionMetadata, StoreError> {
        let segment = encode_segment(id).ok_or(StoreError::BadPath)?;
        let projects = match fs::read_dir(&self.root) {
            Ok(projects) => projects,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound(id.into()));
            }
            Err(error) => return Err(error.into()),
        };
        let mut found = None;
        for project in projects {
            let project = project?;
            if !project.file_type()?.is_dir() {
                continue;
            }
            let dir = project.path().join(&segment);
            match fs::symlink_metadata(&dir) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => continue,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            }
            let (path, compression) = match self.existing_path(&dir, id) {
                Ok(path) => path,
                Err(StoreError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            match self.cached_header(&path, compression) {
                Ok(metadata) if metadata.header.id == id => {
                    if found.replace(metadata).is_some() {
                        return Err(StoreError::Corrupt(format!("ambiguous session id {id:?}")));
                    }
                }
                Ok(_) => {}
                Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        found.ok_or_else(|| StoreError::NotFound(id.into()))
    }

    /// List current Sessions under one project without decoding any event body.
    pub fn list_metadata(&self, cwd: Option<&str>) -> Result<Vec<SessionMetadata>, StoreError> {
        let project = project_dir(&self.root, cwd).ok_or(StoreError::BadPath)?;
        let previous = self
            .headers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut current = HashMap::new();
        let mut output = Vec::new();
        match fs::read_dir(&project) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }
                    let id = entry.file_name().to_string_lossy().into_owned();
                    let (path, compression) = match self.existing_path(&entry.path(), &id) {
                        Ok(path) => path,
                        Err(StoreError::NotFound(_) | StoreError::MigrationRequired { .. }) => {
                            continue
                        }
                        Err(error) => return Err(error),
                    };
                    let metadata = match fs::metadata(&path) {
                        Ok(metadata) => metadata,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                        Err(error) => return Err(error.into()),
                    };
                    let revision = revision(&metadata);
                    let header = match previous
                        .get(&path)
                        .filter(|entry| entry.revision == revision)
                    {
                        Some(cached) => cached.header.clone(),
                        None => match compression {
                            Compression::None => read_plaintext_header(&path)?,
                            Compression::Zstd => read_zstd_header(&path)?.0,
                        },
                    };
                    current.insert(
                        path.clone(),
                        CachedHeader {
                            revision: revision.clone(),
                            header: header.clone(),
                        },
                    );
                    output.push(SessionMetadata {
                        header,
                        path,
                        modified_ms: modified_ms(&metadata),
                        size_bytes: metadata.len(),
                        revision,
                    });
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        *self
            .headers
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = current;
        output.sort_by(|left, right| {
            right
                .modified_ms
                .cmp(&left.modified_ms)
                .then_with(|| left.header.id.cmp(&right.header.id))
        });
        Ok(output)
    }

    fn cached_header(
        &self,
        path: &Path,
        compression: Compression,
    ) -> Result<SessionMetadata, StoreError> {
        let metadata = fs::metadata(path)?;
        let revision = revision(&metadata);
        let mut cache = self
            .headers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(cached) = cache.get(path).filter(|entry| entry.revision == revision) {
            return Ok(SessionMetadata {
                header: cached.header.clone(),
                path: path.to_path_buf(),
                modified_ms: modified_ms(&metadata),
                size_bytes: metadata.len(),
                revision,
            });
        }
        let header = match compression {
            Compression::None => read_plaintext_header(path)?,
            Compression::Zstd => read_zstd_header(path)?.0,
        };
        cache.insert(
            path.to_path_buf(),
            CachedHeader {
                revision: revision.clone(),
                header: header.clone(),
            },
        );
        Ok(SessionMetadata {
            header,
            path: path.to_path_buf(),
            modified_ms: modified_ms(&metadata),
            size_bytes: metadata.len(),
            revision,
        })
    }

    /// Escaped directory names of readable current Sessions under one project.
    pub fn list(&self, cwd: Option<&str>) -> Result<Vec<String>, StoreError> {
        let project = project_dir(&self.root, cwd).ok_or(StoreError::BadPath)?;
        let mut names = Vec::new();
        match fs::read_dir(&project) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }
                    let id = entry.file_name().to_string_lossy().into_owned();
                    let (path, compression) = match self.existing_path(&entry.path(), &id) {
                        Ok(path) => path,
                        Err(StoreError::NotFound(_) | StoreError::MigrationRequired { .. }) => {
                            continue
                        }
                        Err(error) => return Err(error),
                    };
                    if self.cached_header(&path, compression).is_ok() {
                        names.push(entry.file_name().to_string_lossy().into_owned());
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        names.sort();
        Ok(names)
    }

    /// Stream one Session's committed events without buffering its file.
    pub fn read_streamed<F>(
        &self,
        cwd: Option<&str>,
        id: &str,
        mut visit: F,
    ) -> Result<ReadSummary, StoreError>
    where
        F: FnMut(Event) -> Result<(), StoreError>,
    {
        let dir = self.dir(cwd, id)?;
        let (path, compression) = self.existing_path(&dir, id)?;
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(self.missing(&dir, id))
            }
            Err(error) => return Err(error.into()),
        };
        let mut sink = CallbackEvents(&mut visit);
        let decoded = match compression {
            Compression::None => read_plaintext(&path, &mut sink)?,
            Compression::Zstd => read_zstd(&path, &mut sink)?,
        };
        Ok(ReadSummary {
            header: decoded.header,
            event_count: decoded.event_count,
            modified_ms: modified_ms(&metadata),
        })
    }

    /// Visit current Sessions one at a time. Each fully materialized `Stored`
    /// value is released when the callback returns, so traversal never buffers
    /// every artifact or every event in the root simultaneously.
    pub fn for_each_session<F>(&self, mut visit: F)
    where
        F: FnMut(Stored),
    {
        for project in fs::read_dir(&self.root).into_iter().flatten().flatten() {
            if !project.path().is_dir() {
                continue;
            }
            for dir in fs::read_dir(project.path()).into_iter().flatten().flatten() {
                if !dir.path().is_dir() {
                    continue;
                }
                let id = dir.file_name().to_string_lossy().into_owned();
                let Ok((path, compression)) = self.existing_path(&dir.path(), &id) else {
                    continue;
                };
                let Ok(metadata) = fs::metadata(&path) else {
                    continue;
                };
                let mut events = Vec::new();
                let decoded = match compression {
                    Compression::None => read_plaintext(&path, &mut CollectEvents(&mut events)),
                    Compression::Zstd => read_zstd(&path, &mut CollectEvents(&mut events)),
                };
                let Ok(decoded) = decoded else { continue };
                visit(Stored {
                    header: decoded.header,
                    events,
                    modified_ms: modified_ms(&metadata),
                });
            }
        }
    }

    /// Compatibility API returning all current Sessions, newest first.
    /// Use `list_metadata` for listing or `read_streamed` to avoid retaining
    /// every event of every Session.
    pub fn read_all(&self) -> Vec<Stored> {
        let mut output = Vec::new();
        self.for_each_session(|stored| output.push(stored));
        output.sort_by_key(|entry| std::cmp::Reverse(entry.modified_ms));
        output
    }
}

/// A Session read for listing: header, committed events and last-write time.
pub struct Stored {
    pub header: Header,
    pub events: Vec<Event>,
    pub modified_ms: u64,
}

/// An open Session with exclusive write ownership and an append handle at EOF.
pub struct Session {
    pub header: Header,
    pub events: Vec<Event>,
    path: PathBuf,
    file: File,
    compression: Compression,
    _lease: WriteLease,
    #[cfg(test)]
    failure: Option<TestFailure>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestFailure {
    Write,
    Sync,
}

impl Session {
    /// Append one independently durable batch, restoring the pre-append physical
    /// length if either the write or its sync fails.
    pub fn append(&mut self, events: &[Event]) -> Result<(), StoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut batch = String::new();
        for event in events {
            batch.push_str(&encode_event(event));
        }
        let bytes = match self.compression {
            Compression::None => batch.into_bytes(),
            Compression::Zstd => codec::compress_frame(batch.as_bytes())?,
        };
        let before = self.file.metadata()?.len();
        self.file.seek(SeekFrom::End(0))?;
        let result = (|| -> Result<(), StoreError> {
            #[cfg(test)]
            if self.failure == Some(TestFailure::Write) {
                self.failure = None;
                return Err(StoreError::Append {
                    id: self.header.id.clone(),
                    operation: AppendOperation::Write,
                    source: Box::new(io::Error::other("injected write failure").into()),
                    rollback: None,
                });
            }
            self.file
                .write_all(&bytes)
                .map_err(|error| StoreError::Append {
                    id: self.header.id.clone(),
                    operation: AppendOperation::Write,
                    source: Box::new(error.into()),
                    rollback: None,
                })?;
            #[cfg(test)]
            if self.failure == Some(TestFailure::Sync) {
                self.failure = None;
                return Err(StoreError::Append {
                    id: self.header.id.clone(),
                    operation: AppendOperation::Sync,
                    source: Box::new(io::Error::other("injected sync failure").into()),
                    rollback: None,
                });
            }
            self.file.sync_data().map_err(|error| StoreError::Append {
                id: self.header.id.clone(),
                operation: AppendOperation::Sync,
                source: Box::new(error.into()),
                rollback: None,
            })?;
            Ok(())
        })();
        if let Err(mut error) = result {
            let rollback = (|| -> io::Result<()> {
                self.file.set_len(before)?;
                self.file.sync_all()
            })();
            if let StoreError::Append { rollback: slot, .. } = &mut error {
                *slot = rollback.err().map(|error| Box::new(error.into()));
            }
            return Err(error);
        }
        self.events.extend_from_slice(events);
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// This Session's identity, as carried by the header and every event frame.
    pub fn id(&self) -> &str {
        &self.header.id
    }
}

fn read_plaintext_header(path: &Path) -> Result<Header, StoreError> {
    let file = File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 || line.last() != Some(&b'\n') {
        return Err(FormatError::MissingHeader.into());
    }
    line.pop();
    Ok(dsh_session_format::parse_header(&line)?)
}

fn revision(metadata: &fs::Metadata) -> ArtifactRevision {
    ArtifactRevision {
        size_bytes: metadata.len(),
        modified: metadata.modified().unwrap_or(UNIX_EPOCH),
    }
}

fn modified_ms(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn newest_generation(dir: &Path, compression: Compression) -> Result<Option<u64>, StoreError> {
    let mut best = None;
    match fs::read_dir(dir) {
        Ok(entries) => {
            for entry in entries {
                let name = entry?.file_name().to_string_lossy().into_owned();
                if let Some(version) = parse_generation_log_filename(&name, compression) {
                    best = Some(best.map_or(version, |current: u64| current.max(version)));
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(best)
}

fn staging_file(dir: &Path, final_name: &str) -> Result<(PathBuf, File), StoreError> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    loop {
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = dir.join(format!(
            ".{final_name}.{}-{sequence}.tmp",
            std::process::id()
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn write_artifact(
    file: &mut File,
    header: &Header,
    events: &[Event],
    compression: Compression,
) -> Result<(), StoreError> {
    let header = encode_header(header);
    match compression {
        Compression::None => file.write_all(header.as_bytes())?,
        Compression::Zstd => file.write_all(&codec::compress_frame(header.as_bytes())?)?,
    }
    if !events.is_empty() {
        let mut batch = String::new();
        for event in events {
            batch.push_str(&encode_event(event));
        }
        match compression {
            Compression::None => file.write_all(batch.as_bytes())?,
            Compression::Zstd => file.write_all(&codec::compress_frame(batch.as_bytes())?)?,
        }
    }
    file.sync_all()?;
    Ok(())
}

fn publish_artifact(
    dir: &Path,
    path: &Path,
    header: &Header,
    events: &[Event],
    compression: Compression,
) -> Result<(), StoreError> {
    let final_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "artifact filename is not UTF-8",
            )
        })?;
    let (staging, mut file) = staging_file(dir, final_name)?;
    let mut published = false;
    let result = (|| -> Result<(), StoreError> {
        write_artifact(&mut file, header, events, compression)?;
        rename_artifact(&staging, path)?;
        published = true;
        #[cfg(unix)]
        File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if let Err(source) = result {
        if !published {
            if let Err(error) = fs::remove_file(&staging) {
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(StoreError::Publication {
                        id: header.id.clone(),
                        source: Box::new(source),
                        cleanup: Some(Box::new(error.into())),
                    });
                }
            }
        }
        return Err(StoreError::Publication {
            id: header.id.clone(),
            source: Box::new(source),
            cleanup: None,
        });
    }
    Ok(())
}

#[cfg(windows)]
fn rename_artifact(staging: &Path, path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    fn wide(path: &Path) -> io::Result<Vec<u16>> {
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "artifact path has no filename")
        })?;
        // Canonicalizing the directory supplies the extended-length prefix
        // required by MoveFileExW without dereferencing the artifact itself.
        let path = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()?
            .join(name);
        Ok(path.as_os_str().encode_wide().chain(Some(0)).collect())
    }

    let staging = wide(staging)?;
    let target = wide(path)?;

    // SAFETY: both paths are NUL-terminated UTF-16 buffers.
    if unsafe {
        MoveFileExW(
            staging.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        ) == 0
    } {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn rename_artifact(staging: &Path, path: &Path) -> io::Result<()> {
    fs::rename(staging, path)
}

#[cfg(test)]
mod tests;
