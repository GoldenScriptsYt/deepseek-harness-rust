use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FsCode {
    NotFound,
    NotRegularFile,
    NotText,
    PermissionDenied,
    IoError,
    Aborted,
    EditNotFound,
    AmbiguousEdit,
    StaleVersion,
    NotObserved,
    SandboxDenied,
    PatchSyntax,
    PatchMismatch,
    PatchSymlinkSource,
    SameTarget,
}

impl FsCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "FS_NOT_FOUND",
            Self::NotRegularFile => "FS_NOT_REGULAR_FILE",
            Self::NotText => "FS_NOT_TEXT",
            Self::PermissionDenied => "FS_PERMISSION_DENIED",
            Self::IoError => "FS_IO_ERROR",
            Self::Aborted => "FS_ABORTED",
            Self::EditNotFound => "FS_EDIT_NOT_FOUND",
            Self::AmbiguousEdit => "FS_AMBIGUOUS_EDIT",
            Self::StaleVersion => "FS_STALE_VERSION",
            Self::NotObserved => "FS_NOT_OBSERVED",
            Self::SandboxDenied => "FS_SANDBOX_DENIED",
            Self::PatchSyntax => "PATCH_SYNTAX",
            Self::PatchMismatch => "FS_PATCH_MISMATCH",
            Self::PatchSymlinkSource => "PATCH_SYMLINK_SOURCE",
            Self::SameTarget => "FS_SAME_TARGET",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FsError {
    pub(crate) code: FsCode,
    pub(crate) message: String,
    pub(crate) line: Option<usize>,
    pub(crate) hunk_line: Option<usize>,
    pub(crate) stage: Option<&'static str>,
}

impl FsError {
    pub(crate) fn new(code: FsCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            line: None,
            hunk_line: None,
            stage: None,
        }
    }

    pub(crate) fn at_line(code: FsCode, line: usize, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            line: Some(line),
            hunk_line: None,
            stage: None,
        }
    }

    pub(crate) fn mismatch(line: usize, stage: &'static str, pattern: &str) -> Self {
        let preview: String = pattern.chars().take(80).collect();
        let ellipsis = if pattern.chars().count() > 80 {
            "…"
        } else {
            ""
        };
        Self {
            code: FsCode::PatchMismatch,
            message: format!(
                "hunk at patch line {line} ({stage}) does not match — {preview}{ellipsis}"
            ),
            line: None,
            hunk_line: Some(line),
            stage: Some(stage),
        }
    }

    pub(crate) fn render(&self) -> String {
        format!("Error: [{}] {}", self.code.as_str(), self.message)
    }

    #[allow(dead_code)] // Consumed once tool results carry presentation metadata.
    pub(crate) fn metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.code.as_str(),
            "code": self.code.as_str(),
            "message": self.message,
            "line": self.line,
            "hunkLine": self.hunk_line,
            "stage": self.stage,
        })
    }
}

impl fmt::Display for FsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for FsError {}

pub(crate) fn io_error(operation: &str, display: &Path, error: io::Error) -> FsError {
    let code = match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => FsCode::NotFound,
        io::ErrorKind::PermissionDenied => FsCode::PermissionDenied,
        _ => FsCode::IoError,
    };
    FsError::new(
        code,
        format!("cannot {operation} \"{}\": {error}", display.display()),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct CheckedPath {
    pub(crate) display: PathBuf,
    pub(crate) target: PathBuf,
}

fn lexical_absolute(cwd: &Path, input: &str) -> Result<PathBuf, FsError> {
    if input.trim().is_empty() {
        return Err(FsError::new(
            FsCode::NotFound,
            "path must be a non-empty string",
        ));
    }
    let expanded = if input == "~" || input.starts_with("~/") || input.starts_with("~\\") {
        let home = ["HOME", "USERPROFILE"]
            .iter()
            .find_map(std::env::var_os)
            .map(PathBuf::from);
        match home {
            Some(home) if input == "~" => home,
            Some(home) => home.join(&input[2..]),
            None => {
                return Err(FsError::new(
                    FsCode::NotFound,
                    "cannot expand home directory",
                ))
            }
        }
    } else {
        PathBuf::from(input)
    };
    let joined = if expanded.is_absolute() {
        expanded
    } else {
        cwd.join(expanded)
    };
    let mut result = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::Prefix(prefix) => result.push(prefix.as_os_str()),
            Component::RootDir => result.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() {
                    return Err(FsError::new(
                        FsCode::SandboxDenied,
                        "path traverses above the workspace",
                    ));
                }
            }
            Component::Normal(part) => result.push(part),
        }
    }
    if result.as_os_str().is_empty() {
        result.push(".");
    }
    Ok(result)
}

fn canonical_workspace(cwd: &Path) -> Result<PathBuf, FsError> {
    fs::canonicalize(cwd).map_err(|e| io_error("resolve workspace", cwd, e))
}

fn canonical_missing(path: &Path) -> Result<PathBuf, FsError> {
    let mut suffix = Vec::new();
    let mut cursor = path.to_path_buf();
    loop {
        match fs::canonicalize(&cursor) {
            Ok(found) => {
                let mut target = found;
                for part in suffix.iter().rev() {
                    target.push(part);
                }
                return Ok(target);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let name = cursor
                    .file_name()
                    .ok_or_else(|| FsError::new(FsCode::NotFound, "path does not exist"))?;
                suffix.push(name.to_os_string());
                if !cursor.pop() {
                    return Err(io_error("resolve", path, e));
                }
            }
            Err(e) => return Err(io_error("resolve", path, e)),
        }
    }
}

fn contained(workspace: &Path, target: &Path) -> bool {
    target.starts_with(workspace)
}

/// Resolve through the filesystem and require canonical identity to remain in the workspace.
/// Missing suffixes are joined to their nearest existing canonical ancestor.
pub(crate) fn resolve_checked(cwd: &Path, input: &str) -> Result<CheckedPath, FsError> {
    let workspace = canonical_workspace(cwd)?;
    let display = lexical_absolute(&workspace, input)?;
    let target = canonical_missing(&display)?;
    if !contained(&workspace, &target) {
        return Err(FsError::new(
            FsCode::SandboxDenied,
            format!(
                "path \"{}\" resolves outside the workspace \"{}\"",
                display.display(),
                workspace.display()
            ),
        ));
    }
    Ok(CheckedPath { display, target })
}

pub(crate) fn ensure_workspace_dir(workspace: &Path, directory: &Path) -> Result<(), FsError> {
    if !contained(workspace, directory) {
        return Err(FsError::new(
            FsCode::SandboxDenied,
            "directory is outside the workspace",
        ));
    }
    let relative = directory
        .strip_prefix(workspace)
        .map_err(|_| FsError::new(FsCode::SandboxDenied, "directory is outside the workspace"))?;
    let mut cursor = workspace.to_path_buf();
    for part in relative.components() {
        match part {
            Component::Normal(name) => cursor.push(name),
            Component::CurDir => continue,
            _ => {
                return Err(FsError::new(
                    FsCode::SandboxDenied,
                    "invalid directory path",
                ))
            }
        }
        match fs::symlink_metadata(&cursor) {
            Ok(md) => {
                if md.file_type().is_symlink() {
                    let canonical =
                        fs::canonicalize(&cursor).map_err(|e| io_error("resolve", &cursor, e))?;
                    if !contained(workspace, &canonical) {
                        return Err(FsError::new(
                            FsCode::SandboxDenied,
                            "directory symlink resolves outside the workspace",
                        ));
                    }
                } else if !md.is_dir() {
                    return Err(FsError::new(
                        FsCode::NotRegularFile,
                        format!("\"{}\" is not a directory", cursor.display()),
                    ));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&cursor).map_err(|e| io_error("create directory", &cursor, e))?;
            }
            Err(e) => return Err(io_error("inspect", &cursor, e)),
        }
    }
    Ok(())
}

#[derive(Debug)]
struct LockEntry {
    lock: Arc<AsyncMutex<()>>,
    users: usize,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FsState {
    locks: Arc<Mutex<BTreeMap<PathBuf, LockEntry>>>,
}

pub(crate) struct MutationGuard {
    guard: Option<OwnedMutexGuard<()>>,
    locks: Arc<Mutex<BTreeMap<PathBuf, LockEntry>>>,
    key: PathBuf,
    lock: Arc<AsyncMutex<()>>,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        self.guard.take();
        let mut map = self.locks.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = map.get_mut(&self.key) {
            entry.users -= 1;
            if entry.users == 0 && Arc::ptr_eq(&entry.lock, &self.lock) {
                map.remove(&self.key);
            }
        }
    }
}

impl FsState {
    pub(crate) fn lock_targets_blocking(&self, targets: &[PathBuf]) -> Vec<MutationGuard> {
        let keys: BTreeSet<&PathBuf> = targets.iter().collect();
        let mut guards = Vec::with_capacity(keys.len());
        for key in keys {
            let lock = {
                let mut map = self.locks.lock().unwrap_or_else(|error| error.into_inner());
                let entry = map.entry(key.clone()).or_insert_with(|| LockEntry {
                    lock: Arc::new(AsyncMutex::new(())),
                    users: 0,
                });
                entry.users += 1;
                entry.lock.clone()
            };
            guards.push(MutationGuard {
                guard: Some(lock.clone().blocking_lock_owned()),
                locks: self.locks.clone(),
                key: key.clone(),
                lock,
            });
        }
        guards
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Version {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) len: u64,
    pub(crate) modified: SystemTime,
}

#[cfg(unix)]
impl Version {
    fn of(metadata: &Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            len: metadata.len(),
            modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        }
    }
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Version {
    pub(crate) serial: u64,
    pub(crate) index: u64,
    pub(crate) len: u64,
    pub(crate) modified: SystemTime,
}

pub(crate) fn version(path: &Path) -> Result<Option<Version>, FsError> {
    let metadata = match fs::metadata(path) {
        Ok(md) => md,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error("inspect", path, e)),
    };
    if !metadata.is_file() {
        return Err(FsError::new(
            FsCode::NotRegularFile,
            format!("\"{}\" is not a regular file", path.display()),
        ));
    }
    #[cfg(unix)]
    {
        Ok(Some(Version::of(&metadata)))
    }
    #[cfg(windows)]
    {
        Version::of_path(path, &metadata)
            .map(Some)
            .map_err(|e| io_error("inspect", path, e))
    }
}

#[cfg(windows)]
impl Version {
    fn of_path(path: &Path, metadata: &Metadata) -> Result<Self, io::Error> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS,
            FILE_READ_ATTRIBUTES,
        };
        let file = OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .share_mode(0x7)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?;
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
        let error = if ok == 0 {
            Some(io::Error::last_os_error())
        } else {
            None
        };
        if let Some(error) = error {
            return Err(error);
        }
        let value = ((info.ftLastWriteTime.dwHighDateTime as u64) << 32)
            | info.ftLastWriteTime.dwLowDateTime as u64;
        let modified = SystemTime::UNIX_EPOCH
            + std::time::Duration::from_secs((value / 10_000_000).saturating_sub(11_644_473_600))
            + std::time::Duration::from_nanos((value % 10_000_000) * 100);
        let _ = metadata;
        Ok(Self {
            serial: info.dwVolumeSerialNumber as u64,
            index: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
            len: metadata.len(),
            modified,
        })
    }
}

pub(crate) fn read_regular(
    path: &Path,
    display: &Path,
    operation: &str,
    reject_binary: bool,
) -> Result<Vec<u8>, FsError> {
    let metadata = fs::metadata(path).map_err(|e| io_error(operation, display, e))?;
    if !metadata.is_file() {
        return Err(FsError::new(
            FsCode::NotRegularFile,
            format!(
                "cannot {operation} \"{}\": not a regular file",
                display.display()
            ),
        ));
    }
    let bytes = fs::read(path).map_err(|e| io_error(operation, display, e))?;
    if reject_binary && bytes.iter().take(4096).any(|byte| *byte == 0) {
        return Err(FsError::new(
            FsCode::NotText,
            format!("cannot {operation} \"{}\": binary file", display.display()),
        ));
    }
    Ok(bytes)
}

pub(crate) fn decode_text(
    bytes: &[u8],
    display: &Path,
    operation: &str,
) -> Result<String, FsError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        FsError::new(
            FsCode::NotText,
            format!(
                "cannot {operation} \"{}\": invalid UTF-8 text",
                display.display()
            ),
        )
    })
}

pub(crate) fn normalize_crlf(text: &str) -> String {
    text.replace("\r\n", "\n")
}

pub(crate) fn detect_crlf(text: &str) -> bool {
    let sample: String = text.chars().take(4096).collect();
    let crlf = sample.matches("\r\n").count();
    let lf = sample.matches('\n').count() - crlf;
    crlf > lf
}

pub(crate) fn restore_crlf(text: &str, crlf: bool) -> String {
    if crlf {
        normalize_crlf(text).replace('\n', "\r\n")
    } else {
        text.to_string()
    }
}

pub(crate) fn apply_literal(
    text: &str,
    old: &str,
    new: &str,
    all: bool,
    display: &Path,
) -> Result<(String, usize), FsError> {
    let old = normalize_crlf(old);
    let new = normalize_crlf(new);
    if old.is_empty() {
        return Err(FsError::new(
            FsCode::EditNotFound,
            "old_string must be a non-empty string",
        ));
    }
    let count = text.matches(&old).count();
    if count == 0 {
        return Err(FsError::new(
            FsCode::EditNotFound,
            format!("old_string was not found in \"{}\"", display.display()),
        ));
    }
    if count > 1 && !all {
        return Err(FsError::new(FsCode::AmbiguousEdit, format!("old_string matched {count} times in \"{}\"; provide a more specific old_string or set replace_all to true", display.display())));
    }
    let updated = if all {
        text.replace(&old, &new)
    } else {
        text.replacen(&old, &new, 1)
    };
    Ok((updated, count))
}

pub(crate) struct Staging {
    pub(crate) directory: PathBuf,
    pub(crate) file: PathBuf,
}

impl Staging {
    pub(crate) fn create(target: &Path) -> Result<Self, FsError> {
        let parent = target
            .parent()
            .ok_or_else(|| FsError::new(FsCode::IoError, "target has no parent directory"))?;
        let name = target
            .file_name()
            .ok_or_else(|| FsError::new(FsCode::NotRegularFile, "target has no file name"))?;
        let directory = tempfile::Builder::new()
            .prefix(&format!(".{}.", name.to_string_lossy()))
            .suffix(".tmpdir")
            .tempdir_in(parent)
            .map_err(|error| {
                FsError::new(
                    FsCode::IoError,
                    format!(
                        "cannot create staging directory for \"{}\": {error}",
                        target.display()
                    ),
                )
            })?;
        let file = directory.path().join("content");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let handle = options
            .open(&file)
            .map_err(|e| io_error("create staging file", target, e))?;
        drop(handle);
        let directory = directory.keep();
        Ok(Self { directory, file })
    }

    pub(crate) fn cleanup(&self) {
        // This private directory belongs to the operation. Cleanup after publication is best-effort:
        // a committed write must not be reported as failed because its empty staging directory remains.
        let _ = fs::remove_dir_all(&self.directory);
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WriteOptions {
    pub(crate) create_if_absent: bool,
    pub(crate) preserved_mode: Option<fs::Permissions>,
    pub(crate) expected: Option<Version>,
}

/// Publish complete, synced content without exposing a partially written destination.
/// The caller holds all affected target locks and rechecks containment immediately before this call.
pub(crate) fn atomic_write(
    cwd: &Path,
    input: &str,
    content: &[u8],
    options: WriteOptions,
) -> Result<PathBuf, FsError> {
    let initial = resolve_checked(cwd, input)?;
    let workspace = canonical_workspace(cwd)?;
    if let Some(parent) = initial.target.parent() {
        ensure_workspace_dir(&workspace, parent)?;
    }
    let checked = resolve_checked(cwd, input)?;
    if checked.target != initial.target {
        return Err(FsError::new(
            FsCode::StaleVersion,
            "target changed while preparing the write; re-read and retry",
        ));
    }
    let before = version(&checked.target)?;
    if let Some(expected) = options.expected {
        if before.as_ref() != Some(&expected) {
            return Err(FsError::new(
                FsCode::StaleVersion,
                "file changed since it was read; re-read and retry",
            ));
        }
    }
    if options.create_if_absent && before.is_some() {
        return Err(FsError::new(
            FsCode::NotObserved,
            "cannot overwrite an existing file without reading it first; read the file and retry",
        ));
    }
    let mode = options.preserved_mode.clone().or_else(|| {
        fs::metadata(&checked.target)
            .ok()
            .map(|md| md.permissions())
    });
    let staging = Staging::create(&checked.target)?;
    #[cfg(windows)]
    if before.is_some() {
        crate::fs_safety::windows::copy_dacl(&checked.target, &staging.file)?;
    }
    let result = (|| -> Result<(), FsError> {
        let mut file = File::options()
            .write(true)
            .truncate(true)
            .open(&staging.file)
            .map_err(|e| io_error("open staging file", &checked.display, e))?;
        file.write_all(content)
            .map_err(|e| io_error("write staging file", &checked.display, e))?;
        file.sync_all()
            .map_err(|e| io_error("sync staging file", &checked.display, e))?;
        drop(file);
        if let Some(mode) = mode {
            fs::set_permissions(&staging.file, mode)
                .map_err(|e| io_error("preserve permissions", &checked.display, e))?;
        }
        let fresh = resolve_checked(cwd, input)?;
        if fresh.target != checked.target || version(&fresh.target)? != before {
            return Err(FsError::new(
                FsCode::StaleVersion,
                "target changed before publication; re-read and retry",
            ));
        }
        publish(&checked.target, &staging.file, before.is_some())?;
        Ok(())
    })();
    if result.is_err() {
        staging.cleanup();
    }
    result?;
    Ok(checked.display)
}

fn publish(target: &Path, staged: &Path, replacing: bool) -> Result<(), FsError> {
    if replacing {
        #[cfg(windows)]
        {
            return crate::fs_safety::windows::replace_file(target, staged);
        }
        #[cfg(unix)]
        {
            return fs::rename(staged, target).map_err(|e| io_error("publish", target, e));
        }
    }
    match fs::hard_link(staged, target) {
        Ok(()) => Ok(()),
        Err(error) if replacing || error.kind() != io::ErrorKind::AlreadyExists => {
            Err(io_error("publish", target, error))
        }
        Err(_) => Err(FsError::new(
            FsCode::NotObserved,
            format!(
                "cannot write \"{}\": destination appeared during publication; re-read and retry",
                target.display()
            ),
        )),
    }
}

pub(crate) fn atomic_delete(
    cwd: &Path,
    input: &str,
    expected: Option<Version>,
) -> Result<PathBuf, FsError> {
    let checked = resolve_checked(cwd, input)?;
    let raw = fs::symlink_metadata(&checked.display)
        .map_err(|e| io_error("delete", &checked.display, e))?;
    if raw.file_type().is_symlink() {
        return Err(FsError::new(FsCode::PatchSymlinkSource, format!("cannot delete \"{}\": path is a symbolic link — operate on its target path instead", checked.display.display())));
    }
    let before = version(&checked.target)?;
    if before.is_none() {
        return Err(FsError::new(
            FsCode::StaleVersion,
            "file no longer exists; re-read and retry",
        ));
    }
    if expected
        .as_ref()
        .is_some_and(|expected| expected != before.as_ref().unwrap())
    {
        return Err(FsError::new(
            FsCode::StaleVersion,
            "file changed since it was read; re-read and retry",
        ));
    }
    let fresh = resolve_checked(cwd, input)?;
    if fresh.target != checked.target || version(&fresh.target)? != before {
        return Err(FsError::new(
            FsCode::StaleVersion,
            "file changed before deletion; re-read and retry",
        ));
    }
    fs::remove_file(&checked.display).map_err(|e| io_error("delete", &checked.display, e))?;
    Ok(checked.display)
}

pub(crate) fn edit_file(
    cwd: &Path,
    input: &str,
    old: &str,
    new: &str,
    all: bool,
) -> Result<(PathBuf, usize), FsError> {
    let checked = resolve_checked(cwd, input)?;
    let expected = version(&checked.target)?.ok_or_else(|| {
        FsError::new(
            FsCode::StaleVersion,
            "file no longer exists; re-read and retry",
        )
    })?;
    let bytes = read_regular(&checked.target, &checked.display, "edit", true)?;
    if version(&checked.target)? != Some(expected.clone()) {
        return Err(FsError::new(
            FsCode::StaleVersion,
            "file changed while it was being read; re-read and retry",
        ));
    }
    if bytes.contains(&0) {
        return Err(FsError::new(
            FsCode::NotText,
            format!("cannot edit \"{}\": binary file", checked.display.display()),
        ));
    }
    let raw = decode_text(&bytes, &checked.display, "edit")?;
    let crlf = detect_crlf(&raw);
    let (updated, count) = apply_literal(&normalize_crlf(&raw), old, new, all, &checked.display)?;
    let mode = fs::metadata(&checked.target)
        .map_err(|e| io_error("inspect", &checked.display, e))?
        .permissions();
    let display = atomic_write(
        cwd,
        input,
        restore_crlf(&updated, crlf).as_bytes(),
        WriteOptions {
            create_if_absent: false,
            preserved_mode: Some(mode),
            expected: Some(expected),
        },
    )?;
    Ok((display, count))
}

#[cfg(test)]
pub(crate) fn unique_root(tag: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("dsh-tools-{tag}-"))
        .tempdir()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_workspace_paths_are_allowed_but_siblings_are_not() {
        let parent = unique_root("containment");
        let workspace = parent.path().join("workspace");
        let sibling = parent.path().join("workspace-other");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&sibling).unwrap();
        fs::write(workspace.join("inside"), "inside").unwrap();
        fs::write(sibling.join("outside"), "outside").unwrap();

        let absolute = resolve_checked(
            &workspace,
            workspace.join("nested/inside").to_string_lossy().as_ref(),
        )
        .unwrap();
        assert_eq!(
            absolute.target,
            fs::canonicalize(&workspace).unwrap().join("nested/inside")
        );
        assert_eq!(
            resolve_checked(&workspace, "../workspace-other/outside")
                .unwrap_err()
                .code,
            FsCode::SandboxDenied
        );
        assert_eq!(
            resolve_checked(&workspace, &sibling.join("outside").to_string_lossy())
                .unwrap_err()
                .code,
            FsCode::SandboxDenied
        );
        assert_eq!(
            fs::read_to_string(sibling.join("outside")).unwrap(),
            "outside"
        );

        #[cfg(windows)]
        {
            let alias = workspace.to_string_lossy().to_uppercase();
            assert!(resolve_checked(&workspace, &alias).is_ok());
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_parents_and_final_components_cannot_escape() {
        let parent = unique_root("symlink-containment");
        let workspace = parent.path().join("workspace");
        let outside = parent.path().join("outside");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("file"), "outside").unwrap();
        std::os::unix::fs::symlink(&outside, workspace.join("linked-directory")).unwrap();
        std::os::unix::fs::symlink(outside.join("file"), workspace.join("linked-file")).unwrap();

        for path in ["linked-directory/file", "linked-file"] {
            assert_eq!(
                resolve_checked(&workspace, path).unwrap_err().code,
                FsCode::SandboxDenied
            );
        }
        assert_eq!(fs::read_to_string(outside.join("file")).unwrap(), "outside");
    }

    #[cfg(unix)]
    #[test]
    fn in_workspace_symlink_writes_and_edits_affect_the_target() {
        let root = unique_root("symlink-target");
        fs::write(root.path().join("target"), "before\n").unwrap();
        std::os::unix::fs::symlink("target", root.path().join("link")).unwrap();
        atomic_write(
            root.path(),
            "link",
            b"written\n",
            WriteOptions {
                create_if_absent: false,
                preserved_mode: None,
                expected: None,
            },
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(root.path().join("target")).unwrap(),
            "written\n"
        );
        edit_file(root.path(), "link", "written", "edited", false).unwrap();
        assert_eq!(
            fs::read_to_string(root.path().join("target")).unwrap(),
            "edited\n"
        );
        assert!(fs::symlink_metadata(root.path().join("link"))
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn staging_directories_are_private_and_cleaned_up() {
        let root = unique_root("staging-cleanup");
        let target = root.path().join("target");
        let staging = Staging::create(&target).unwrap();
        fs::write(&staging.file, b"complete").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&staging.directory)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(&staging.file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let directory = staging.directory.clone();
        drop(staging);
        assert!(!directory.exists());
        assert!(!target.exists());
    }

    #[cfg(unix)]
    #[test]
    fn edits_preserve_posix_mode_and_line_endings() {
        use std::os::unix::fs::PermissionsExt;
        let root = unique_root("mode-crlf");
        let path = root.path().join("file");
        fs::write(&path, b"one\r\ntwo\r\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        edit_file(root.path(), "file", "one", "ONE", false).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"ONE\r\ntwo\r\n");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[test]
    fn edits_reject_binary_ambiguity_and_missing_text_without_writing() {
        let root = unique_root("edit-errors");
        let path = root.path().join("file");
        fs::write(&path, b"same\nsame\n").unwrap();
        assert_eq!(
            edit_file(root.path(), "file", "same", "x", false)
                .unwrap_err()
                .code,
            FsCode::AmbiguousEdit
        );
        assert_eq!(fs::read(&path).unwrap(), b"same\nsame\n");
        edit_file(root.path(), "file", "same", "x", true).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"x\nx\n");
        fs::write(&path, b"before\0after").unwrap();
        assert_eq!(
            edit_file(root.path(), "file", "before", "x", false)
                .unwrap_err()
                .code,
            FsCode::NotText
        );
        assert_eq!(fs::read(&path).unwrap(), b"before\0after");
        fs::write(&path, b"plain text\n").unwrap();
        assert_eq!(
            edit_file(root.path(), "file", "absent", "x", false)
                .unwrap_err()
                .code,
            FsCode::EditNotFound
        );
    }
}

#[cfg(windows)]
pub(crate) mod windows {
    use super::{io_error, FsError};
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use windows_sys::Win32::Security::{
        GetFileSecurityW, SetFileSecurityW, DACL_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::Storage::FileSystem::ReplaceFileW;

    fn wide(path: &Path) -> Vec<u16> {
        let text: Vec<u16> = path.as_os_str().encode_wide().collect();
        if text.starts_with(&[92, 92, 63, 92]) {
            if !text.ends_with(&[0]) {
                let mut result = text;
                result.push(0);
                return result;
            }
            return text;
        }
        let mut result = Vec::with_capacity(text.len() + 8);
        result.extend_from_slice(&[92, 92, 63, 92]);
        if text.starts_with(&[92, 92]) {
            result.extend_from_slice(&[85, 78, 67, 92]);
            result.extend_from_slice(&text[2..]);
        } else {
            result.extend_from_slice(&text);
        }
        result.push(0);
        result
    }

    fn read_dacl(path: &Path) -> Result<Vec<u8>, FsError> {
        let text = wide(path);
        let mut needed = 0u32;
        unsafe {
            GetFileSecurityW(
                text.as_ptr(),
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                0,
                &mut needed,
            );
        }
        if needed == 0 {
            return Err(io_error(
                "read permissions",
                path,
                io::Error::last_os_error(),
            ));
        }
        let mut bytes = vec![0u8; needed as usize];
        if unsafe {
            GetFileSecurityW(
                text.as_ptr(),
                DACL_SECURITY_INFORMATION,
                bytes.as_mut_ptr() as _,
                bytes.len() as u32,
                &mut needed,
            )
        } == 0
        {
            return Err(io_error(
                "read permissions",
                path,
                io::Error::last_os_error(),
            ));
        }
        bytes.truncate(needed as usize);
        Ok(bytes)
    }

    pub(crate) fn copy_dacl(source: &Path, destination: &Path) -> Result<(), FsError> {
        let bytes = read_dacl(source)?;
        let text = wide(destination);
        if unsafe {
            SetFileSecurityW(
                text.as_ptr(),
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                bytes.as_ptr() as _,
            )
        } == 0
        {
            return Err(io_error(
                "preserve permissions",
                destination,
                io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    pub(crate) fn replace_file(target: &Path, staged: &Path) -> Result<(), FsError> {
        let target_text = wide(target);
        let staged_text = wide(staged);
        if unsafe {
            ReplaceFileW(
                target_text.as_ptr(),
                staged_text.as_ptr(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io_error("replace", target, io::Error::last_os_error()));
        }
        Ok(())
    }
}
