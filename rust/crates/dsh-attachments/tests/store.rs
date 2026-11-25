use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use dsh_attachments::{
    AdmittedPromptContentPart, AttachmentError, AttachmentErrorCode, AttachmentLimits,
    AttachmentStore, FileAttachmentRef, ImageAttachmentRef, ImageMediaType, PromptContentPart,
    ReceiptState, SaveImageAttachment, StagedAttachment, UploadReceipt,
};
use std::fs;
use std::io::Cursor;
use std::path::Path;
use tempfile::TempDir;

fn image_bytes(format: image::ImageFormat, width: u32, height: u32) -> Vec<u8> {
    let raster = image::RgbaImage::from_fn(width, height, |x, y| {
        image::Rgba([(x % 256) as u8, (y % 256) as u8, 40, 255])
    });
    let mut output = Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(raster)
        .write_to(&mut output, format)
        .unwrap();
    output.into_inner()
}

fn png(width: u32, height: u32) -> Vec<u8> {
    image_bytes(image::ImageFormat::Png, width, height)
}

fn file_ref(receipt: &UploadReceipt) -> FileAttachmentRef {
    match &receipt.attachment {
        StagedAttachment::File { attachment } => attachment.clone(),
        StagedAttachment::Image { .. } => panic!("expected a file receipt"),
    }
}

fn image_ref(receipt: &UploadReceipt) -> ImageAttachmentRef {
    match &receipt.attachment {
        StagedAttachment::Image { attachment, .. } => attachment.clone(),
        StagedAttachment::File { .. } => panic!("expected an image receipt"),
    }
}

fn receipt_count(root: &Path) -> usize {
    fs::read_dir(root.join("receipts"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
        })
        .count()
}

fn image_object_count(root: &Path) -> usize {
    fs::read_dir(root.join("images"))
        .unwrap()
        .filter_map(Result::ok)
        .count()
}

fn assert_code<T: std::fmt::Debug>(result: Result<T, AttachmentError>, code: AttachmentErrorCode) {
    match result {
        Ok(value) => panic!("expected {code:?}, got success: {value:?}"),
        Err(error) => assert_eq!(error.code(), code, "unexpected error: {error}"),
    }
}

fn digest(reference: &ImageAttachmentRef) -> &str {
    reference.attachment_id.strip_prefix("sha256:").unwrap()
}

#[test]
fn file_roundtrip_reopen_distinct_receipts_and_revocation() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let store = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    let first = store
        .save_file(b"same bytes", Some(r"C:\client\report.txt"))
        .unwrap();
    let second = store.save_file(b"same bytes", Some("other.txt")).unwrap();
    let first_ref = file_ref(&first);
    let second_ref = file_ref(&second);

    assert_eq!(first_ref.name, "report.txt");
    assert_eq!(first_ref.attachment_id, second_ref.attachment_id);
    assert_ne!(first.id, second.id);
    assert_eq!(first.state, ReceiptState::Active);
    assert_eq!(store.read_file(&first_ref).unwrap(), b"same bytes");

    store.revoke(&first.id).unwrap();
    store.revoke(&first.id).unwrap();
    assert_eq!(
        store.receipt(&first.id).unwrap().state,
        ReceiptState::Revoked
    );
    assert_code(
        store.resolve_receipt(&first.id),
        AttachmentErrorCode::AttachmentNotFound,
    );
    assert_eq!(store.read_file(&second_ref).unwrap(), b"same bytes");

    drop(store);
    let reopened = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    assert_eq!(reopened.receipt(&second.id).unwrap(), second);
    assert_eq!(
        reopened.resolve_receipt(&second.id).unwrap(),
        second.attachment
    );
    assert_eq!(reopened.read_file(&second_ref).unwrap(), b"same bytes");
}

#[test]
fn images_roundtrip_in_all_supported_formats() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let store = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    let cases = [
        (image::ImageFormat::Png, ImageMediaType::Png),
        (image::ImageFormat::Jpeg, ImageMediaType::Jpeg),
        (image::ImageFormat::WebP, ImageMediaType::Webp),
        (image::ImageFormat::Gif, ImageMediaType::Gif),
    ];
    let mut saved = Vec::new();
    for (format, media_type) in cases {
        let data = image_bytes(format, 3, 2);
        let receipt = store
            .save_image(SaveImageAttachment {
                data: &data,
                media_type,
                name: Some("picture"),
            })
            .unwrap();
        let reference = image_ref(&receipt);
        assert_eq!(reference.media_type, media_type);
        assert_eq!((reference.width, reference.height), (3, 2));
        assert_eq!(reference.bytes, data.len() as u64);
        assert_eq!(reference.name.as_deref(), Some("picture"));
        let stored = store.read_image(&reference).unwrap();
        assert_eq!(stored.reference, reference);
        assert_eq!(stored.data, data);
        saved.push((receipt.id, reference, data));
    }
    drop(store);
    let reopened = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    for (id, reference, data) in saved {
        assert_eq!(reopened.receipt(&id).unwrap().state, ReceiptState::Active);
        assert_eq!(reopened.read_image(&reference).unwrap().data, data);
    }
}

#[test]
fn prompt_admission_preserves_order_and_resolves_file_and_image_receipts() {
    let temporary = TempDir::new().unwrap();
    let store = AttachmentStore::open(temporary.path(), AttachmentLimits::default()).unwrap();
    let file = store.save_file(b"prompt file", Some("prompt.txt")).unwrap();
    let stored = store
        .save_image(SaveImageAttachment {
            data: &png(2, 2),
            media_type: ImageMediaType::Png,
            name: None,
        })
        .unwrap();
    let raw = png(4, 3);
    let encoded = BASE64.encode(&raw);
    let admitted = store
        .admit_prompt_content(&[
            PromptContentPart::Text { text: "before" },
            PromptContentPart::Image {
                media_type: ImageMediaType::Png,
                data: &encoded,
                name: Some("raw.png"),
            },
            PromptContentPart::FileReceipt {
                receipt_id: &file.id,
            },
            PromptContentPart::FileReceipt {
                receipt_id: &stored.id,
            },
            PromptContentPart::Text { text: "after" },
        ])
        .unwrap();

    assert_eq!(admitted.len(), 5);
    assert!(matches!(&admitted[0], AdmittedPromptContentPart::Text { text } if text == "before"));
    let AdmittedPromptContentPart::Image {
        attachment: raw_ref,
    } = &admitted[1]
    else {
        panic!("raw image was not converted to a durable reference");
    };
    assert_eq!(raw_ref.name.as_deref(), Some("raw.png"));
    assert_eq!((raw_ref.width, raw_ref.height), (4, 3));
    assert_eq!(store.read_image(raw_ref).unwrap().data, raw);
    assert!(
        matches!(&admitted[2], AdmittedPromptContentPart::File { attachment } if attachment == &file_ref(&file))
    );
    assert!(
        matches!(&admitted[3], AdmittedPromptContentPart::Image { attachment } if attachment == &image_ref(&stored))
    );
    assert!(matches!(&admitted[4], AdmittedPromptContentPart::Text { text } if text == "after"));
}

#[test]
fn traversal_and_unknown_receipts_are_rejected() {
    let temporary = TempDir::new().unwrap();
    let store = AttachmentStore::open(temporary.path(), AttachmentLimits::default()).unwrap();
    let receipt = store.save_file(b"safe", Some("safe.txt")).unwrap();
    let valid = file_ref(&receipt);

    let mut traversal = valid.clone();
    traversal.attachment_id = "sha256:../../escape".to_owned();
    assert_code(
        store.read_file(&traversal),
        AttachmentErrorCode::InvalidAttachmentRef,
    );
    let mut traversal = valid.clone();
    traversal.name = "../escape".to_owned();
    assert_code(
        store.read_file(&traversal),
        AttachmentErrorCode::InvalidAttachmentRef,
    );
    let mut image = image_ref(
        &store
            .save_image(SaveImageAttachment {
                data: &png(1, 1),
                media_type: ImageMediaType::Png,
                name: None,
            })
            .unwrap(),
    );
    image.attachment_id = "sha256:../../escape".to_owned();
    assert_code(
        store.read_image(&image),
        AttachmentErrorCode::InvalidAttachmentRef,
    );
    assert_code(
        store.receipt("../escape"),
        AttachmentErrorCode::InvalidAttachmentRef,
    );
    assert_code(
        store.receipt("receipt-00000000000000000000000000000000"),
        AttachmentErrorCode::AttachmentNotFound,
    );
}

#[cfg(unix)]
#[test]
fn managed_directory_symlink_cannot_escape_root() {
    use std::os::unix::fs::symlink;

    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let outside = temporary.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let store = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    fs::remove_dir(root.join("images")).unwrap();
    symlink(&outside, root.join("images")).unwrap();
    assert_code(
        store.save_image(SaveImageAttachment {
            data: &png(1, 1),
            media_type: ImageMediaType::Png,
            name: None,
        }),
        AttachmentErrorCode::AttachmentRootInvalid,
    );
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}

#[test]
fn corrupt_and_oversize_stored_objects_are_detected() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let store = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    let file = store.save_file(b"corrupt file", Some("file.txt")).unwrap();
    let file_reference = file_ref(&file);
    let file_digest = file_reference
        .attachment_id
        .strip_prefix("sha256:")
        .unwrap();
    let alias = root
        .join("files")
        .join(&file_digest[..2])
        .join(file_digest)
        .join(&file_reference.name);
    fs::write(&alias, b"CORRUPT FILE").unwrap();
    assert_code(
        store.read_file(&file_reference),
        AttachmentErrorCode::AttachmentCorrupt,
    );

    let data = png(2, 2);
    let image = store
        .save_image(SaveImageAttachment {
            data: &data,
            media_type: ImageMediaType::Png,
            name: None,
        })
        .unwrap();
    let image_reference = image_ref(&image);
    let path = root
        .join("images")
        .join(&digest(&image_reference)[..2])
        .join(digest(&image_reference));
    fs::write(&path, vec![0_u8; data.len()]).unwrap();
    assert_code(
        store.read_image(&image_reference),
        AttachmentErrorCode::AttachmentCorrupt,
    );

    let exact_root = temporary.path().join("exact-limits");
    let exact = AttachmentStore::open(
        &exact_root,
        AttachmentLimits {
            max_file_bytes: 3,
            ..AttachmentLimits::default()
        },
    )
    .unwrap();
    let exact_file = exact.save_file(b"abc", Some("abc.txt")).unwrap();
    let exact_ref = file_ref(&exact_file);
    let exact_digest = exact_ref.attachment_id.strip_prefix("sha256:").unwrap();
    let exact_alias = exact_root
        .join("files")
        .join(&exact_digest[..2])
        .join(exact_digest)
        .join(&exact_ref.name);
    fs::write(&exact_alias, b"abcd").unwrap();
    assert_code(
        exact.read_file(&exact_ref),
        AttachmentErrorCode::AttachmentCorrupt,
    );
}

#[test]
fn mime_mismatch_and_invalid_image_data_are_rejected_without_writes() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let store = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    assert_code(
        store.save_image(SaveImageAttachment {
            data: &png(2, 2),
            media_type: ImageMediaType::Jpeg,
            name: None,
        }),
        AttachmentErrorCode::ImageTypeMismatch,
    );
    assert_code(
        store.save_image(SaveImageAttachment {
            data: b"not an image",
            media_type: ImageMediaType::Png,
            name: None,
        }),
        AttachmentErrorCode::InvalidImage,
    );
    assert_eq!(receipt_count(store.root()), 0);
    assert_eq!(image_object_count(store.root()), 0);
}

#[test]
fn base64_is_canonical_bounded_and_empty_files_are_valid() {
    let temporary = TempDir::new().unwrap();
    let store = AttachmentStore::open(temporary.path(), AttachmentLimits::default()).unwrap();
    let empty = store.admit_encoded_file("", Some("empty.bin")).unwrap();
    assert_eq!(file_ref(&empty).bytes, 0);
    assert_eq!(
        store.read_file(&file_ref(&empty)).unwrap(),
        Vec::<u8>::new()
    );
    assert_code(
        store.admit_encoded_file("Zg", None),
        AttachmentErrorCode::InvalidFileBase64,
    );
    assert_code(
        store.admit_encoded_file("Zh==", None),
        AttachmentErrorCode::InvalidFileBase64,
    );
    assert_code(
        store.admit_encoded_image(ImageMediaType::Png, "", None),
        AttachmentErrorCode::InvalidImageBase64,
    );

    let bounded = AttachmentStore::open(
        temporary.path().join("bounded"),
        AttachmentLimits {
            max_file_bytes: 4,
            max_image_bytes: 4,
            ..AttachmentLimits::default()
        },
    )
    .unwrap();
    assert_code(
        bounded.save_file(b"12345", None),
        AttachmentErrorCode::FileTooLarge,
    );
    assert_code(
        bounded.admit_encoded_file(&BASE64.encode(b"12345"), None),
        AttachmentErrorCode::FileTooLarge,
    );
    assert_code(
        bounded.save_image(SaveImageAttachment {
            data: b"12345",
            media_type: ImageMediaType::Png,
            name: None,
        }),
        AttachmentErrorCode::ImageTooLarge,
    );
    assert_code(
        bounded.admit_encoded_image(ImageMediaType::Png, &BASE64.encode(b"123456"), None),
        AttachmentErrorCode::ImageTooLarge,
    );
}

#[test]
fn image_dimension_and_pixel_limits_are_enforced() {
    let temporary = TempDir::new().unwrap();
    let dimension = AttachmentStore::open(
        temporary.path().join("dimension"),
        AttachmentLimits {
            max_image_dimension: 2,
            ..AttachmentLimits::default()
        },
    )
    .unwrap();
    assert_code(
        dimension.save_image(SaveImageAttachment {
            data: &png(3, 2),
            media_type: ImageMediaType::Png,
            name: None,
        }),
        AttachmentErrorCode::ImageDimensionTooLarge,
    );
    let pixels = AttachmentStore::open(
        temporary.path().join("pixels"),
        AttachmentLimits {
            max_image_pixels: 5,
            ..AttachmentLimits::default()
        },
    )
    .unwrap();
    assert_code(
        pixels.save_image(SaveImageAttachment {
            data: &png(3, 2),
            media_type: ImageMediaType::Png,
            name: None,
        }),
        AttachmentErrorCode::ImageTooManyPixels,
    );
}

#[test]
fn batch_validation_finishes_before_any_write() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let store = AttachmentStore::open(
        &root,
        AttachmentLimits {
            max_images_per_message: 2,
            ..AttachmentLimits::default()
        },
    )
    .unwrap();
    let valid = png(2, 2);
    assert_code(
        store.save_images(&[
            SaveImageAttachment {
                data: &valid,
                media_type: ImageMediaType::Png,
                name: None,
            },
            SaveImageAttachment {
                data: b"invalid",
                media_type: ImageMediaType::Png,
                name: None,
            },
        ]),
        AttachmentErrorCode::InvalidImage,
    );
    assert_eq!(receipt_count(store.root()), 0);
    assert_eq!(image_object_count(store.root()), 0);
}

#[test]
fn prompt_limit_counts_stored_images_before_writing_raw_images() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let data = png(2, 2);
    let store = AttachmentStore::open(
        &root,
        AttachmentLimits {
            max_message_image_bytes: data.len() as u64,
            ..AttachmentLimits::default()
        },
    )
    .unwrap();
    let stored = store
        .save_image(SaveImageAttachment {
            data: &data,
            media_type: ImageMediaType::Png,
            name: None,
        })
        .unwrap();
    let encoded = BASE64.encode(&data);
    assert_code(
        store.admit_prompt_content(&[
            PromptContentPart::Image {
                media_type: ImageMediaType::Png,
                data: &encoded,
                name: None,
            },
            PromptContentPart::FileReceipt {
                receipt_id: &stored.id,
            },
        ]),
        AttachmentErrorCode::ImagesTooLarge,
    );
    assert_eq!(receipt_count(store.root()), 1);
    assert_eq!(image_object_count(store.root()), 1);
}

#[test]
fn receipt_corruption_is_detected() {
    let temporary = TempDir::new().unwrap();
    let store = AttachmentStore::open(temporary.path(), AttachmentLimits::default()).unwrap();
    let receipt = store.save_file(b"receipt", None).unwrap();
    let path = store
        .root()
        .join("receipts")
        .join(format!("{}.json", receipt.id));
    let data = fs::read_to_string(&path)
        .unwrap()
        .replace("\"active\"", "\"revoked\"");
    fs::write(&path, data).unwrap();
    assert_code(
        store.receipt(&receipt.id),
        AttachmentErrorCode::AttachmentCorrupt,
    );
}

#[test]
fn atomic_alias_failure_preserves_target_and_issues_no_receipt() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("attachments");
    let store = AttachmentStore::open(&root, AttachmentLimits::default()).unwrap();
    let first = store.save_file(b"atomic", Some("same.txt")).unwrap();
    let reference = file_ref(&first);
    let file_digest = reference.attachment_id.strip_prefix("sha256:").unwrap();
    let alias = root
        .join("files")
        .join(&file_digest[..2])
        .join(file_digest)
        .join(&reference.name);
    fs::remove_file(&alias).unwrap();
    fs::create_dir(&alias).unwrap();

    assert_code(
        store.save_file(b"atomic", Some("same.txt")),
        AttachmentErrorCode::AttachmentCorrupt,
    );
    assert!(alias.is_dir());
    assert_eq!(receipt_count(store.root()), 1);
    assert_eq!(
        store.receipt(&first.id).unwrap().state,
        ReceiptState::Active
    );
}

#[test]
fn file_names_are_cross_platform_safe_and_bounded() {
    let temporary = TempDir::new().unwrap();
    let store = AttachmentStore::open(temporary.path(), AttachmentLimits::default()).unwrap();
    let receipt = store
        .save_file(b"name", Some(r"C:\client\CON.txt"))
        .unwrap();
    let reference = file_ref(&receipt);
    assert_eq!(reference.name, "_CON.txt");
    assert!(!reference.name.contains(['/', '\\']));

    let long_name = "é".repeat(200);
    let receipt = store.save_file(b"long", Some(&long_name)).unwrap();
    let reference = file_ref(&receipt);
    assert!(reference.name.len() <= 255);
    assert!(reference.name.chars().all(|character| character != '\u{0}'));
}
