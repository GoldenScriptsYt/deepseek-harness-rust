use crate::{AttachmentError, AttachmentLimits, ImageMediaType};
use image::{ImageDecoder, ImageFormat, ImageReader, Limits};
use std::io::Cursor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ImageMetadata {
    pub media_type: ImageMediaType,
    pub width: u32,
    pub height: u32,
}

fn media_type(format: ImageFormat) -> Result<ImageMediaType, AttachmentError> {
    match format {
        ImageFormat::Png => Ok(ImageMediaType::Png),
        ImageFormat::Jpeg => Ok(ImageMediaType::Jpeg),
        ImageFormat::WebP => Ok(ImageMediaType::Webp),
        ImageFormat::Gif => Ok(ImageMediaType::Gif),
        _ => Err(AttachmentError::UnsupportedImageFormat {
            format: format!("{format:?}"),
        }),
    }
}

pub(crate) fn inspect_image_bytes(
    data: &[u8],
    limits: &AttachmentLimits,
    decode_pixels: bool,
) -> Result<ImageMetadata, AttachmentError> {
    if data.is_empty() {
        return Err(AttachmentError::InvalidImageData {
            reason: "image is empty",
        });
    }
    let reader = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|source| AttachmentError::InvalidImage {
            source: image::ImageError::IoError(source),
        })?;
    let format = reader
        .format()
        .ok_or_else(|| AttachmentError::InvalidImageData {
            reason: "image format is unknown",
        })?;
    let detected = media_type(format)?;
    let mut decoder = reader
        .into_decoder()
        .map_err(|source| AttachmentError::InvalidImage { source })?;
    let (encoded_width, encoded_height) = decoder.dimensions();
    let orientation = decoder
        .orientation()
        .map_err(|source| AttachmentError::InvalidImage { source })?;
    let transposed = matches!(
        orientation,
        image::metadata::Orientation::Rotate90
            | image::metadata::Orientation::Rotate270
            | image::metadata::Orientation::Rotate90FlipH
            | image::metadata::Orientation::Rotate270FlipH
    );
    let (width, height) = if transposed {
        (encoded_height, encoded_width)
    } else {
        (encoded_width, encoded_height)
    };
    if width == 0 || height == 0 {
        return Err(AttachmentError::InvalidImageData {
            reason: "image dimensions are zero",
        });
    }
    if width > limits.max_image_dimension || height > limits.max_image_dimension {
        return Err(AttachmentError::ImageDimensionTooLarge {
            width,
            height,
            limit: limits.max_image_dimension,
        });
    }
    let pixels = u64::from(width) * u64::from(height);
    if pixels > limits.max_image_pixels {
        return Err(AttachmentError::ImageTooManyPixels {
            width,
            height,
            limit: limits.max_image_pixels,
        });
    }
    if decode_pixels {
        let allocation_limit = limits
            .max_image_pixels
            .saturating_mul(16)
            .saturating_add(data.len() as u64)
            .saturating_add(64 * 1024);
        let mut reader = ImageReader::new(Cursor::new(data))
            .with_guessed_format()
            .map_err(|source| AttachmentError::InvalidImage {
                source: image::ImageError::IoError(source),
            })?;
        let mut decoder_limits = Limits::default();
        decoder_limits.max_image_width = Some(limits.max_image_dimension);
        decoder_limits.max_image_height = Some(limits.max_image_dimension);
        decoder_limits.max_alloc = Some(allocation_limit);
        reader.limits(decoder_limits);
        reader
            .decode()
            .map_err(|source| AttachmentError::InvalidImage { source })?;
    }
    Ok(ImageMetadata {
        media_type: detected,
        width,
        height,
    })
}
