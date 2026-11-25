use crate::StoreError;
use std::fs;
#[cfg(unix)]
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

const LEASE_FILENAME: &str = "session.lock";

/// Kernel-owned write ownership. There is intentionally no age or expiry: a
/// live but wedged writer keeps ownership until it closes or its process dies.
pub(crate) struct WriteLease {
    #[cfg(unix)]
    file: File,
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
}

impl WriteLease {
    pub(crate) fn acquire(dir: &Path, id: &str) -> Result<Self, StoreError> {
        fs::create_dir_all(dir)?;
        #[cfg(unix)]
        {
            use fs2::FileExt;
            use std::os::unix::fs::MetadataExt;
            let path = dir.join(LEASE_FILENAME);
            for _ in 0..3 {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path)?;
                if let Err(error) = file.try_lock_exclusive() {
                    if error.kind() == io::ErrorKind::WouldBlock {
                        return Err(StoreError::AlreadyOwned(id.into()));
                    }
                    return Err(error.into());
                }
                let held = file.metadata()?;
                match fs::metadata(&path) {
                    Ok(current) if held.dev() == current.dev() && held.ino() == current.ino() => {
                        return Ok(Self { file });
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Err(StoreError::AlreadyOwned(id.into()))
        }
        #[cfg(windows)]
        {
            use sha2::{Digest, Sha256};
            use windows_sys::Win32::Foundation::{
                CloseHandle, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
            };
            use windows_sys::Win32::System::Threading::{CreateSemaphoreW, WaitForSingleObject};

            // Match the TypeScript semaphore namespace while canonicalizing aliases
            // before case-folding, as Win32 itself does for object names.
            let canonical = fs::canonicalize(dir)?;
            let lock_path = canonical.join(LEASE_FILENAME);
            let folded = lock_path.as_os_str().to_string_lossy().to_lowercase();
            let digest = Sha256::digest(folded.as_bytes());
            let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
            let name: Vec<u16> = format!("Local\\dsh-session-lock-{hex}")
                .encode_utf16()
                .chain(Some(0))
                .collect();
            // SAFETY: the name is NUL-terminated and the handle is retained for
            // the lease lifetime. A zero count means immediate contention.
            let handle = unsafe { CreateSemaphoreW(std::ptr::null(), 1, 1, name.as_ptr()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error().into());
            }
            // SAFETY: `handle` was just successfully created.
            let waited = unsafe { WaitForSingleObject(handle, 0) };
            if waited == WAIT_OBJECT_0 {
                return Ok(Self { handle });
            }
            // SAFETY: closing an owned semaphore handle is always valid.
            unsafe { CloseHandle(handle) };
            if waited == WAIT_TIMEOUT {
                Err(StoreError::AlreadyOwned(id.into()))
            } else if waited == WAIT_FAILED {
                Err(io::Error::last_os_error().into())
            } else {
                Err(io::Error::other(format!("unexpected semaphore wait result {waited}")).into())
            }
        }
    }
}

// A kernel HANDLE can be moved between threads; ownership remains exclusive.
#[cfg(windows)]
unsafe impl Send for WriteLease {}

impl Drop for WriteLease {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = &self.file; // Closing the descriptor releases flock.
        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::CloseHandle;
            use windows_sys::Win32::System::Threading::ReleaseSemaphore;
            // SAFETY: this handle is owned and dropped exactly once.
            unsafe {
                ReleaseSemaphore(self.handle, 1, std::ptr::null_mut());
                CloseHandle(self.handle);
            }
        }
    }
}
