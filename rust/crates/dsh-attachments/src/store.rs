use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rand::rngs::OsRng;
use rand::TryRngCore;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::fs::{
    atomic_link, atomic_replace, atomic_write_new, ensure_directory, object_path, read_bounded,
    sha256_hex, valid_receipt_id, verify_file, RECEIPT_MAX_BYTES,
};
use crate::image::{inspect_image_bytes, ImageMetadata};
use crate::{
    AdmittedPromptContentPart, AttachmentError, AttachmentLimits, FileAttachmentRef,
    ImageAttachmentRef, ImageMediaType, PromptContentPart, ReceiptState, SaveImageAttachment,
    StagedAttachment, StoredImageAttachment, UploadReceipt,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReceiptRecord {
    version: u32,
    receipt: UploadReceipt,
    checksum: String,
}

struct PreparedImage {
    digest: String,
    metadata: ImageMetadata,
}

enum PlannedPart<'a> {
    Text(&'a str),
    RawImage {
        media_type: ImageMediaType,
        data: &'a str,
        name: Option<&'a str>,
    },
    File(FileAttachmentRef),
    StoredImage {
        attachment: ImageAttachmentRef,
    },
}

struct DecodedImage<'a> {
    data: Vec<u8>,
    media_type: ImageMediaType,
    name: Option<&'a str>,
}

/// Durable, content-addressed attachment storage with revocable upload receipts.
#[derive(Clone, Debug)]
pub struct AttachmentStore {
    root: PathBuf,
    limits: AttachmentLimits,
}

impl AttachmentStore {
    /// Open or create a private attachment root and validate deployment limits.
    pub fn open(root: impl AsRef<Path>, limits: AttachmentLimits) -> Result<Self, AttachmentError> {
        limits.validate()?;
        let supplied = root.as_ref();
        if supplied.as_os_str().is_empty() {
            return Err(AttachmentError::InvalidRoot {
                reason: "storage root is empty",
            });
        }
        match fs::symlink_metadata(supplied) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(AttachmentError::InvalidRoot {
                    reason: "storage root is a symlink or non-directory",
                });
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(supplied)
                    .map_err(|source| AttachmentError::StoreOpenFailed { source })?;
            }
            Err(source) => {
                return Err(AttachmentError::StoreOpenFailed { source });
            }
        }
        let canonical = fs::canonicalize(supplied)
            .map_err(|source| AttachmentError::StoreOpenFailed { source })?;
        ensure_directory(&canonical, &canonical, "open attachment store")?;
        let store = Self {
            root: canonical,
            limits,
        };
        for directory in ["receipts", "images", "file-objects", "files"] {
            ensure_directory(
                &store.root,
                &store.root.join(directory),
                "open attachment store",
            )?;
        }
        Ok(store)
    }

    /// Absolute canonical storage root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Limits applied by this store instance.
    pub const fn limits(&self) -> &AttachmentLimits {
        &self.limits
    }

    /// Store one bounded verbatim file and issue a distinct durable receipt.
    pub fn save_file(
        &self,
        data: &[u8],
        name: Option<&str>,
    ) -> Result<UploadReceipt, AttachmentError> {
        self.check_file_size(data.len())?;
        let digest = sha256_hex(data);
        let attachment = self.commit_file(data, name, digest)?;
        self.issue_receipt(StagedAttachment::File { attachment })
    }

    /// Decode canonical base64 and store one bounded verbatim file.
    pub fn admit_encoded_file(
        &self,
        data: &str,
        name: Option<&str>,
    ) -> Result<UploadReceipt, AttachmentError> {
        let decoded = decode_canonical_base64(data, self.limits.max_file_bytes, false)?;
        self.save_file(&decoded, name)
    }

    /// Validate and store one supported image, then issue a durable receipt.
    pub fn save_image(
        &self,
        input: SaveImageAttachment<'_>,
    ) -> Result<UploadReceipt, AttachmentError> {
        let receipts = self.save_images(std::slice::from_ref(&input))?;
        receipts
            .into_iter()
            .next()
            .ok_or(AttachmentError::InvalidImageData {
                reason: "validated image batch unexpectedly produced no receipt",
            })
    }

    /// Decode canonical base64 and store one supported image.
    pub fn admit_encoded_image(
        &self,
        media_type: ImageMediaType,
        data: &str,
        name: Option<&str>,
    ) -> Result<UploadReceipt, AttachmentError> {
        let decoded = decode_canonical_base64(data, self.limits.max_image_bytes, true)?;
        self.save_image(SaveImageAttachment {
            data: &decoded,
            media_type,
            name,
        })
    }

    /// Validate an entire image batch before publishing any member.
    pub fn save_images(
        &self,
        inputs: &[SaveImageAttachment<'_>],
    ) -> Result<Vec<UploadReceipt>, AttachmentError> {
        let total = inputs.iter().try_fold(0_u64, |total, input| {
            total.checked_add(input.data.len() as u64).ok_or_else(|| {
                AttachmentError::ImagesTooLarge {
                    limit: self.limits.max_message_image_bytes,
                }
            })
        })?;
        self.assert_image_batch(inputs.len(), total)?;
        let prepared = inputs
            .iter()
            .map(|input| self.prepare_image(input))
            .collect::<Result<Vec<_>, _>>()?;
        let mut receipts = Vec::with_capacity(inputs.len());
        for (input, prepared) in inputs.iter().zip(prepared) {
            let attachment = self.commit_image(input, &prepared)?;
            receipts.push(self.issue_receipt(StagedAttachment::Image {
                attachment,
                encoded_bytes: input.data.len() as u64,
            })?);
        }
        Ok(receipts)
    }

    /// Look up a receipt, including its revoked state, and verify its checksum.
    pub fn receipt(&self, id: &str) -> Result<UploadReceipt, AttachmentError> {
        if !valid_receipt_id(id) {
            return Err(AttachmentError::InvalidAttachmentRef);
        }
        ensure_directory(
            &self.root,
            self.root.join("receipts").as_path(),
            "read receipt",
        )?;
        let path = self.receipt_path(id);
        let data = read_bounded(&path, RECEIPT_MAX_BYTES)
            .map_err(|source| AttachmentError::AttachmentReadFailed { source })?
            .ok_or(AttachmentError::ReceiptNotFound)?;
        let record: ReceiptRecord = serde_json::from_slice(&data)
            .map_err(|source| AttachmentError::ReceiptDataInvalid { source })?;
        if record.version != 1 || record.receipt.id != id {
            return Err(AttachmentError::AttachmentCorrupt {
                reason: "receipt version or identifier is invalid",
            });
        }
        let expected = receipt_checksum(&record.receipt)?;
        if record.checksum != expected {
            return Err(AttachmentError::AttachmentCorrupt {
                reason: "receipt checksum is invalid",
            });
        }
        validate_staged_reference(&record.receipt.attachment)?;
        Ok(record.receipt)
    }

    /// Resolve an active receipt to its durable attachment.
    pub fn resolve_receipt(&self, id: &str) -> Result<StagedAttachment, AttachmentError> {
        let receipt = self.receipt(id)?;
        if receipt.state == ReceiptState::Revoked {
            return Err(AttachmentError::ReceiptRevoked);
        }
        Ok(receipt.attachment)
    }

    /// Idempotently revoke a receipt without deleting shared immutable objects.
    pub fn revoke(&self, id: &str) -> Result<(), AttachmentError> {
        let mut receipt = self.receipt(id)?;
        if receipt.state == ReceiptState::Revoked {
            return Ok(());
        }
        receipt.state = ReceiptState::Revoked;
        let record = ReceiptRecord {
            version: 1,
            checksum: receipt_checksum(&receipt)?,
            receipt,
        };
        let mut data = serde_json::to_vec(&record)
            .map_err(|source| AttachmentError::ReceiptEncodingFailed { source })?;
        data.push(b'\n');
        atomic_replace(&self.receipt_path(id), &data, "replace receipt")
    }

    /// Durable read-only alias for a validated verbatim file reference.
    pub fn file_readonly_path(
        &self,
        reference: &FileAttachmentRef,
    ) -> Result<PathBuf, AttachmentError> {
        let digest = validate_file_reference(reference)?;
        if reference.bytes > self.limits.max_file_bytes {
            return Err(AttachmentError::FileTooLarge {
                limit: self.limits.max_file_bytes,
            });
        }
        let path = self.file_alias_path(reference, &digest);
        ensure_directory(
            &self.root,
            path.parent().ok_or(AttachmentError::InvalidAttachmentRef)?,
            "read file path",
        )?;
        Ok(path)
    }

    /// Read and integrity-check one verbatim file reference.
    pub fn read_file(&self, reference: &FileAttachmentRef) -> Result<Vec<u8>, AttachmentError> {
        let digest = validate_file_reference(reference)?;
        if reference.bytes > self.limits.max_file_bytes {
            return Err(AttachmentError::FileTooLarge {
                limit: self.limits.max_file_bytes,
            });
        }
        let path = self.file_alias_path(reference, &digest);
        ensure_directory(
            &self.root,
            path.parent().ok_or(AttachmentError::InvalidAttachmentRef)?,
            "read file",
        )?;
        let data = read_attachment(&path, self.limits.max_file_bytes, &reference.attachment_id)?;
        if data.len() as u64 != reference.bytes || sha256_hex(&data) != digest {
            return Err(AttachmentError::AttachmentCorrupt {
                reason: "file byte count or digest does not match its reference",
            });
        }
        Ok(data)
    }

    /// Durable read-only object path for a validated image reference.
    pub fn image_readonly_path(
        &self,
        reference: &ImageAttachmentRef,
    ) -> Result<PathBuf, AttachmentError> {
        let digest = validate_image_reference(reference)?;
        if reference.bytes == 0 || reference.bytes > self.limits.max_image_bytes {
            return Err(AttachmentError::InvalidAttachmentRef);
        }
        if reference.width == 0
            || reference.height == 0
            || reference.width > self.limits.max_image_dimension
            || reference.height > self.limits.max_image_dimension
        {
            return Err(AttachmentError::InvalidAttachmentRef);
        }
        let path = object_path(&self.root, "images", &digest);
        ensure_directory(
            &self.root,
            path.parent().ok_or(AttachmentError::InvalidAttachmentRef)?,
            "read image path",
        )?;
        Ok(path)
    }

    /// Read and integrity-check one image, re-deriving format and dimensions.
    pub fn read_image(
        &self,
        reference: &ImageAttachmentRef,
    ) -> Result<StoredImageAttachment, AttachmentError> {
        let digest = validate_image_reference(reference)?;
        if reference.bytes == 0 || reference.bytes > self.limits.max_image_bytes {
            return Err(AttachmentError::InvalidAttachmentRef);
        }
        if reference.width == 0
            || reference.height == 0
            || reference.width > self.limits.max_image_dimension
            || reference.height > self.limits.max_image_dimension
        {
            return Err(AttachmentError::InvalidAttachmentRef);
        }
        let path = object_path(&self.root, "images", &digest);
        ensure_directory(
            &self.root,
            path.parent().ok_or(AttachmentError::InvalidAttachmentRef)?,
            "read image",
        )?;
        let data = read_attachment(&path, self.limits.max_image_bytes, &reference.attachment_id)?;
        if data.len() as u64 != reference.bytes || sha256_hex(&data) != digest {
            return Err(AttachmentError::AttachmentCorrupt {
                reason: "image byte count or digest does not match its reference",
            });
        }
        let metadata = inspect_image_bytes(&data, &self.limits, false)?;
        if metadata.media_type != reference.media_type
            || metadata.width != reference.width
            || metadata.height != reference.height
        {
            return Err(AttachmentError::AttachmentCorrupt {
                reason: "image metadata does not match its reference",
            });
        }
        Ok(StoredImageAttachment {
            reference: reference.clone(),
            data,
        })
    }

    /// Resolve file receipts and replace raw images with durable references in order.
    pub fn admit_prompt_content(
        &self,
        content: &[PromptContentPart<'_>],
    ) -> Result<Vec<AdmittedPromptContentPart>, AttachmentError> {
        let mut planned = Vec::with_capacity(content.len());
        let mut image_count = 0_usize;
        let mut estimated_bytes = 0_u64;
        for part in content {
            match part {
                PromptContentPart::Text { text } => planned.push(PlannedPart::Text(text)),
                PromptContentPart::Image {
                    media_type,
                    data,
                    name,
                } => {
                    image_count =
                        image_count
                            .checked_add(1)
                            .ok_or(AttachmentError::TooManyImages {
                                limit: self.limits.max_images_per_message,
                            })?;
                    estimated_bytes = estimated_bytes.saturating_add(estimated_base64_bytes(data));
                    planned.push(PlannedPart::RawImage {
                        media_type: *media_type,
                        data,
                        name: *name,
                    });
                }
                PromptContentPart::FileReceipt { receipt_id } => {
                    match self.resolve_receipt(receipt_id)? {
                        StagedAttachment::File { attachment } => {
                            planned.push(PlannedPart::File(attachment))
                        }
                        StagedAttachment::Image {
                            attachment,
                            encoded_bytes,
                        } => {
                            image_count = image_count.checked_add(1).ok_or(
                                AttachmentError::TooManyImages {
                                    limit: self.limits.max_images_per_message,
                                },
                            )?;
                            estimated_bytes = estimated_bytes.checked_add(encoded_bytes).ok_or(
                                AttachmentError::ImagesTooLarge {
                                    limit: self.limits.max_message_image_bytes,
                                },
                            )?;
                            planned.push(PlannedPart::StoredImage { attachment });
                        }
                    }
                }
                PromptContentPart::File { attachment } => {
                    validate_file_reference(attachment)?;
                    planned.push(PlannedPart::File(attachment.clone()));
                }
                PromptContentPart::StoredImage {
                    attachment,
                    encoded_bytes,
                } => {
                    validate_image_reference(attachment)?;
                    image_count =
                        image_count
                            .checked_add(1)
                            .ok_or(AttachmentError::TooManyImages {
                                limit: self.limits.max_images_per_message,
                            })?;
                    estimated_bytes = estimated_bytes.checked_add(*encoded_bytes).ok_or(
                        AttachmentError::ImagesTooLarge {
                            limit: self.limits.max_message_image_bytes,
                        },
                    )?;
                    planned.push(PlannedPart::StoredImage {
                        attachment: attachment.clone(),
                    });
                }
            }
        }
        self.assert_image_batch(image_count, estimated_bytes)?;

        let mut decoded = Vec::new();
        let mut actual_bytes = 0_u64;
        for part in &planned {
            if let PlannedPart::RawImage {
                media_type,
                data,
                name,
            } = part
            {
                let bytes = decode_canonical_base64(data, self.limits.max_image_bytes, true)?;
                actual_bytes = actual_bytes.checked_add(bytes.len() as u64).ok_or(
                    AttachmentError::ImagesTooLarge {
                        limit: self.limits.max_message_image_bytes,
                    },
                )?;
                decoded.push(DecodedImage {
                    data: bytes,
                    media_type: *media_type,
                    name: *name,
                });
            }
        }
        self.assert_image_batch(image_count, actual_bytes)?;
        let prepared = decoded
            .iter()
            .map(|image| {
                self.prepare_image(&SaveImageAttachment {
                    data: &image.data,
                    media_type: image.media_type,
                    name: image.name,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut admitted = Vec::with_capacity(planned.len());
        let mut image_index = 0_usize;
        for part in planned {
            match part {
                PlannedPart::Text(text) => admitted.push(AdmittedPromptContentPart::Text {
                    text: text.to_owned(),
                }),
                PlannedPart::File(attachment) => {
                    admitted.push(AdmittedPromptContentPart::File { attachment });
                }
                PlannedPart::StoredImage { attachment, .. } => {
                    admitted.push(AdmittedPromptContentPart::Image { attachment });
                }
                PlannedPart::RawImage {
                    media_type, name, ..
                } => {
                    let input = SaveImageAttachment {
                        data: &decoded[image_index].data,
                        media_type,
                        name,
                    };
                    let attachment = self.commit_image(&input, &prepared[image_index])?;
                    image_index += 1;
                    admitted.push(AdmittedPromptContentPart::Image { attachment });
                }
            }
        }
        Ok(admitted)
    }

    fn check_file_size(&self, bytes: usize) -> Result<(), AttachmentError> {
        if bytes as u64 > self.limits.max_file_bytes {
            return Err(AttachmentError::FileTooLarge {
                limit: self.limits.max_file_bytes,
            });
        }
        Ok(())
    }

    fn assert_image_batch(&self, count: usize, bytes: u64) -> Result<(), AttachmentError> {
        if count > self.limits.max_images_per_message {
            return Err(AttachmentError::TooManyImages {
                limit: self.limits.max_images_per_message,
            });
        }
        if bytes > self.limits.max_message_image_bytes {
            return Err(AttachmentError::ImagesTooLarge {
                limit: self.limits.max_message_image_bytes,
            });
        }
        Ok(())
    }

    fn prepare_image(
        &self,
        input: &SaveImageAttachment<'_>,
    ) -> Result<PreparedImage, AttachmentError> {
        if input.data.len() as u64 > self.limits.max_image_bytes {
            return Err(AttachmentError::ImageTooLarge {
                limit: self.limits.max_image_bytes,
            });
        }
        let metadata = inspect_image_bytes(input.data, &self.limits, true)?;
        if metadata.media_type != input.media_type {
            return Err(AttachmentError::ImageTypeMismatch {
                declared: input.media_type,
                detected: metadata.media_type,
            });
        }
        Ok(PreparedImage {
            digest: sha256_hex(input.data),
            metadata,
        })
    }

    fn commit_image(
        &self,
        input: &SaveImageAttachment<'_>,
        prepared: &PreparedImage,
    ) -> Result<ImageAttachmentRef, AttachmentError> {
        let reference = ImageAttachmentRef {
            attachment_id: format!("sha256:{}", prepared.digest),
            media_type: prepared.metadata.media_type,
            bytes: input.data.len() as u64,
            width: prepared.metadata.width,
            height: prepared.metadata.height,
            name: image_display_name(input.name),
            original_dimensions: None,
        };
        let path = object_path(&self.root, "images", &prepared.digest);
        self.publish_object(&path, input.data, &prepared.digest, "write image")?;
        Ok(reference)
    }

    fn commit_file(
        &self,
        data: &[u8],
        name: Option<&str>,
        digest: String,
    ) -> Result<FileAttachmentRef, AttachmentError> {
        let reference = FileAttachmentRef {
            attachment_id: format!("sha256:{digest}"),
            name: file_leaf_name(name),
            bytes: data.len() as u64,
        };
        let object = object_path(&self.root, "file-objects", &digest);
        self.publish_object(&object, data, &digest, "write file")?;
        let alias = self.file_alias_path(&reference, &digest);
        ensure_directory(
            &self.root,
            alias
                .parent()
                .ok_or(AttachmentError::InvalidAttachmentRef)?,
            "write file alias",
        )?;
        match fs::symlink_metadata(&alias) {
            Ok(_) => verify_file(&alias, data.len() as u64, &digest)?,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                if !atomic_link(&object, &alias, "write file alias")? {
                    verify_file(&alias, data.len() as u64, &digest)?;
                }
            }
            Err(source) => {
                return Err(AttachmentError::AttachmentReadFailed { source });
            }
        }
        Ok(reference)
    }

    fn publish_object(
        &self,
        path: &Path,
        data: &[u8],
        digest: &str,
        operation: &'static str,
    ) -> Result<(), AttachmentError> {
        ensure_directory(
            &self.root,
            path.parent().ok_or(AttachmentError::InvalidAttachmentRef)?,
            operation,
        )?;
        match fs::symlink_metadata(path) {
            Ok(_) => verify_file(path, data.len() as u64, digest)?,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                if !atomic_write_new(path, data, operation)? {
                    verify_file(path, data.len() as u64, digest)?;
                }
            }
            Err(source) => return Err(AttachmentError::AttachmentReadFailed { source }),
        }
        Ok(())
    }

    fn receipt_path(&self, id: &str) -> PathBuf {
        self.root.join("receipts").join(format!("{id}.json"))
    }

    fn file_alias_path(&self, reference: &FileAttachmentRef, digest: &str) -> PathBuf {
        self.root
            .join("files")
            .join(&digest[..2])
            .join(digest)
            .join(&reference.name)
    }

    fn issue_receipt(
        &self,
        attachment: StagedAttachment,
    ) -> Result<UploadReceipt, AttachmentError> {
        for _ in 0..32 {
            let mut random = [0_u8; 16];
            OsRng.try_fill_bytes(&mut random).map_err(|error| {
                AttachmentError::AttachmentWriteFailed {
                    operation: "generate receipt identifier",
                    source: io::Error::other(error),
                }
            })?;
            let mut id = String::from("receipt-");
            for byte in random {
                use std::fmt::Write as _;
                let _ = write!(id, "{byte:02x}");
            }
            let receipt = UploadReceipt {
                id: id.clone(),
                state: ReceiptState::Active,
                attachment: attachment.clone(),
            };
            let record = ReceiptRecord {
                version: 1,
                checksum: receipt_checksum(&receipt)?,
                receipt: receipt.clone(),
            };
            let mut data = serde_json::to_vec(&record)
                .map_err(|source| AttachmentError::ReceiptEncodingFailed { source })?;
            data.push(b'\n');
            if atomic_write_new(&self.receipt_path(&id), &data, "create receipt")? {
                return Ok(receipt);
            }
        }
        Err(AttachmentError::AttachmentWriteFailed {
            operation: "create receipt",
            source: io::Error::new(
                io::ErrorKind::AlreadyExists,
                "unable to allocate a unique receipt identifier",
            ),
        })
    }
}

fn receipt_checksum(receipt: &UploadReceipt) -> Result<String, AttachmentError> {
    let data = serde_json::to_vec(receipt)
        .map_err(|source| AttachmentError::ReceiptEncodingFailed { source })?;
    Ok(sha256_hex(&data))
}

fn decode_canonical_base64(
    data: &str,
    maximum: u64,
    image: bool,
) -> Result<Vec<u8>, AttachmentError> {
    let maximum_encoded = maximum
        .saturating_add(2)
        .saturating_div(3)
        .saturating_mul(4);
    if data.len() as u64 > maximum_encoded {
        return Err(if image {
            AttachmentError::ImageTooLarge { limit: maximum }
        } else {
            AttachmentError::FileTooLarge { limit: maximum }
        });
    }
    if image && data.is_empty() {
        return Err(AttachmentError::InvalidImageBase64);
    }
    let decoded = BASE64.decode(data).map_err(|_| {
        if image {
            AttachmentError::InvalidImageBase64
        } else {
            AttachmentError::InvalidFileBase64
        }
    })?;
    if BASE64.encode(&decoded).as_bytes() != data.as_bytes() {
        return Err(if image {
            AttachmentError::InvalidImageBase64
        } else {
            AttachmentError::InvalidFileBase64
        });
    }
    if decoded.len() as u64 > maximum {
        return Err(if image {
            AttachmentError::ImageTooLarge { limit: maximum }
        } else {
            AttachmentError::FileTooLarge { limit: maximum }
        });
    }
    Ok(decoded)
}

fn estimated_base64_bytes(data: &str) -> u64 {
    (data.len() as u64).saturating_mul(3).saturating_div(4)
}

fn parse_digest(id: &str) -> Result<String, AttachmentError> {
    let Some(digest) = id.strip_prefix("sha256:") else {
        return Err(AttachmentError::InvalidAttachmentRef);
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AttachmentError::InvalidAttachmentRef);
    }
    Ok(digest.to_owned())
}

fn validate_file_reference(reference: &FileAttachmentRef) -> Result<String, AttachmentError> {
    let digest = parse_digest(&reference.attachment_id)?;
    if reference.name != file_leaf_name(Some(&reference.name)) {
        return Err(AttachmentError::InvalidAttachmentRef);
    }
    Ok(digest)
}

fn validate_image_reference(reference: &ImageAttachmentRef) -> Result<String, AttachmentError> {
    parse_digest(&reference.attachment_id)
}

fn validate_staged_reference(attachment: &StagedAttachment) -> Result<(), AttachmentError> {
    match attachment {
        StagedAttachment::File { attachment } => {
            validate_file_reference(attachment)?;
        }
        StagedAttachment::Image { attachment, .. } => {
            validate_image_reference(attachment)?;
        }
    }
    Ok(())
}

fn read_attachment(path: &Path, maximum: u64, id: &str) -> Result<Vec<u8>, AttachmentError> {
    match read_bounded(path, maximum) {
        Ok(Some(data)) => Ok(data),
        Ok(None) => Err(AttachmentError::AttachmentNotFound { id: id.to_owned() }),
        Err(source) if source.kind() == io::ErrorKind::InvalidData => {
            Err(AttachmentError::AttachmentCorrupt {
                reason: "stored attachment is not a regular bounded file",
            })
        }
        Err(source) => Err(AttachmentError::AttachmentReadFailed { source }),
    }
}

fn file_leaf_name(value: Option<&str>) -> String {
    let Some(value) = value else {
        return "file".to_owned();
    };
    let start = value.rfind(['/', '\\']).map_or(0, |index| {
        index + value[index..].chars().next().map_or(1, char::len_utf8)
    });
    let leaf = &value[start..];
    let clean: String = leaf
        .chars()
        .filter(|character| !matches!(character, '\u{0000}'..='\u{001f}' | '\u{007f}'))
        .map(|character| {
            if matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*') {
                '_'
            } else {
                character
            }
        })
        .collect();
    let clean = clean.trim().trim_end_matches(['.', ' ']);
    let clean = if is_windows_device_name(clean) {
        format!("_{clean}")
    } else {
        clean.to_owned()
    };
    let mut bytes = 0_usize;
    let prefix: String = clean
        .chars()
        .take_while(|character| {
            bytes += character.len_utf8();
            bytes <= 255
        })
        .collect();
    let prefix = prefix.trim_end_matches(['.', ' ']);
    if prefix.is_empty() || prefix == "." || prefix == ".." {
        "file".to_owned()
    } else {
        prefix.to_owned()
    }
}

fn is_windows_device_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or(name)
        .trim_end_matches(['.', ' ']);
    let lower = stem.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "con"
            | "prn"
            | "aux"
            | "nul"
            | "com1"
            | "com2"
            | "com3"
            | "com4"
            | "com5"
            | "com6"
            | "com7"
            | "com8"
            | "com9"
            | "lpt1"
            | "lpt2"
            | "lpt3"
            | "lpt4"
            | "lpt5"
            | "lpt6"
            | "lpt7"
            | "lpt8"
            | "lpt9"
    )
}

fn image_display_name(value: Option<&str>) -> Option<String> {
    let value = value?;
    let start = value.rfind(['/', '\\']).map_or(0, |index| {
        index + value[index..].chars().next().map_or(1, char::len_utf8)
    });
    let clean: String = value[start..]
        .chars()
        .filter(|character| !matches!(character, '\u{0000}'..='\u{001f}' | '\u{007f}'))
        .collect();
    let clean = clean.trim();
    let mut bytes = 0_usize;
    let prefix: String = clean
        .chars()
        .take_while(|character| {
            bytes += character.len_utf8();
            bytes <= 255
        })
        .collect();
    if prefix.is_empty() {
        None
    } else {
        Some(prefix)
    }
}
