use crate::AttachmentError;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use tempfile::NamedTempFile;

pub(crate) const RECEIPT_MAX_BYTES: u64 = 64 * 1024;

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

pub(crate) fn valid_receipt_id(id: &str) -> bool {
    let Some(hex) = id.strip_prefix("receipt-") else {
        return false;
    };
    hex.len() == 32
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn ensure_directory(
    root: &Path,
    path: &Path,
    operation: &'static str,
) -> Result<(), AttachmentError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| AttachmentError::InvalidRoot {
            reason: "managed directory escapes the storage root",
        })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Err(AttachmentError::InvalidRoot {
                reason: "managed directory contains a non-normal path component",
            });
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(AttachmentError::InvalidRoot {
                    reason: "managed directory is a symlink or non-directory",
                });
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(|source| {
                    AttachmentError::AttachmentWriteFailed { operation, source }
                })?;
                sync_directory(
                    current
                        .parent()
                        .ok_or_else(|| AttachmentError::InvalidRoot {
                            reason: "managed directory has no parent",
                        })?,
                )
                .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
            }
            Err(source) => {
                return Err(AttachmentError::AttachmentWriteFailed { operation, source });
            }
        }
        let canonical = fs::canonicalize(&current)
            .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
        if !canonical.starts_with(root) {
            return Err(AttachmentError::InvalidRoot {
                reason: "managed directory resolves outside the storage root",
            });
        }
    }
    set_private_permissions(path)
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })
}

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

pub(crate) fn atomic_write_new(
    path: &Path,
    data: &[u8],
    operation: &'static str,
) -> Result<bool, AttachmentError> {
    let parent = path.parent().ok_or_else(|| AttachmentError::InvalidRoot {
        reason: "publication target has no parent",
    })?;
    let mut temporary = NamedTempFile::new_in(parent)
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
    temporary
        .write_all(data)
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
    match temporary.persist_noclobber(path) {
        Ok(file) => {
            drop(file);
            sync_directory(parent)
                .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
            Ok(true)
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(AttachmentError::AttachmentWriteFailed {
            operation,
            source: error.error,
        }),
    }
}

pub(crate) fn atomic_replace(
    path: &Path,
    data: &[u8],
    operation: &'static str,
) -> Result<(), AttachmentError> {
    let parent = path.parent().ok_or_else(|| AttachmentError::InvalidRoot {
        reason: "publication target has no parent",
    })?;
    let mut temporary = NamedTempFile::new_in(parent)
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
    temporary
        .write_all(data)
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
    temporary
        .persist(path)
        .map_err(|error| AttachmentError::AttachmentWriteFailed {
            operation,
            source: error.error,
        })?;
    sync_directory(parent)
        .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })
}

pub(crate) fn atomic_link(
    source: &Path,
    target: &Path,
    operation: &'static str,
) -> Result<bool, AttachmentError> {
    match fs::hard_link(source, target) {
        Ok(()) => {
            sync_directory(
                target
                    .parent()
                    .ok_or_else(|| AttachmentError::InvalidRoot {
                        reason: "link target has no parent",
                    })?,
            )
            .map_err(|source| AttachmentError::AttachmentWriteFailed { operation, source })?;
            Ok(true)
        }
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(source) => Err(AttachmentError::AttachmentWriteFailed { operation, source }),
    }
}

pub(crate) fn verify_file(
    path: &Path,
    expected_bytes: u64,
    expected_digest: &str,
) -> Result<(), AttachmentError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            AttachmentError::AttachmentCorrupt {
                reason: "existing attachment object is missing",
            }
        } else {
            AttachmentError::AttachmentReadFailed { source }
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != expected_bytes
    {
        return Err(AttachmentError::AttachmentCorrupt {
            reason: "existing attachment object type or length is invalid",
        });
    }
    let mut file =
        File::open(path).map_err(|source| AttachmentError::AttachmentReadFailed { source })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|source| AttachmentError::AttachmentReadFailed { source })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let digest = hasher.finalize();
    let mut actual = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(actual, "{byte:02x}");
    }
    if actual != expected_digest {
        return Err(AttachmentError::AttachmentCorrupt {
            reason: "existing attachment object digest is invalid",
        });
    }
    Ok(())
}

pub(crate) fn read_bounded(path: &Path, maximum: u64) -> io::Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "attachment is not a regular bounded file",
        ));
    }
    let file = File::open(path)?;
    let mut data = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut data)?;
    if data.len() as u64 > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "attachment exceeded its read limit",
        ));
    }
    Ok(Some(data))
}

pub(crate) fn object_path(root: &Path, directory: &str, digest: &str) -> PathBuf {
    root.join(directory).join(&digest[..2]).join(digest)
}
