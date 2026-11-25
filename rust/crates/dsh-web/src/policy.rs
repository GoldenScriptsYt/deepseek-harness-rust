//! URL, content-type, and charset policy for the HTTP fetch provider. Network-free.

use crate::error::{WebError, WebErrorCode};
use encoding_rs::Encoding;
use url::Url;

/// Maximum accepted request URL length.
pub const MAX_URL_LENGTH: usize = 2048;

/// The body kinds this provider decodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchableKind {
    /// `text/html` and XHTML.
    Html,
    /// Other text and structured text types.
    Text,
}

/// Parse a URL and enforce the network-independent restrictions.
/// @param input - Raw URL string from the tool arguments.
/// @returns The parsed URL, or a coded failure.
pub fn parse_fetch_url(input: &str) -> Result<Url, WebError> {
    let url = Url::parse(input).map_err(|error| {
        WebError::new(
            WebErrorCode::InvalidUrl,
            format!("invalid URL: {input} ({error})"),
        )
    })?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(WebError::new(
            WebErrorCode::InvalidUrl,
            format!(
                "unsupported URL scheme \"{}://\" (only http and https are allowed)",
                url.scheme()
            ),
        ));
    }
    if !url.username().is_empty() || url.password().is_some_and(|password| !password.is_empty()) {
        return Err(WebError::new(
            WebErrorCode::BlockedUrl,
            "credentials in URLs are not allowed",
        ));
    }
    Ok(url)
}

/// Validate a request URL against the complete pre-network policy.
/// @param input - Raw URL string from the tool arguments.
/// @returns The parsed URL, or a coded failure.
pub fn validate_fetch_url(input: &str) -> Result<Url, WebError> {
    if input.len() > MAX_URL_LENGTH {
        return Err(WebError::new(
            WebErrorCode::InvalidUrl,
            format!("URL exceeds the maximum length of {MAX_URL_LENGTH}"),
        ));
    }
    parse_fetch_url(input)
}

/// Whether two URLs share scheme, host, and port.
/// @param left - One URL.
/// @param right - The other URL.
/// @returns Whether a redirect between them stays on one origin.
#[must_use]
pub fn is_same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

/// Classify a response `Content-Type` into a decodable body kind.
/// @param content_type - Raw header value, or `None` when absent.
/// @returns The decodable kind, or `None` for an unsupported type.
#[must_use]
pub fn classify_content_type(content_type: Option<&str>) -> Option<FetchableKind> {
    let mime = content_type
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if mime == "text/html" || mime == "application/xhtml+xml" {
        return Some(FetchableKind::Html);
    }
    if mime.starts_with("text/")
        || mime == "application/json"
        || mime == "application/xml"
        || mime.ends_with("+json")
        || mime.ends_with("+xml")
    {
        return Some(FetchableKind::Text);
    }
    None
}

/// Extract the lower-cased `charset` parameter from a `Content-Type`.
/// @param content_type - Raw header value, or `None` when absent.
/// @returns The declared charset, or `None` when the header declares none.
#[must_use]
pub fn parse_charset(content_type: Option<&str>) -> Option<String> {
    let header = content_type?;
    let lower = header.to_ascii_lowercase();
    let index = lower.find("charset")?;
    let after = &header[index + "charset".len()..];
    let after = after.trim_start();
    let after = after.strip_prefix('=')?.trim_start();
    let value = match after.strip_prefix('"') {
        Some(quoted) => quoted.split('"').next().unwrap_or_default(),
        None => after.split(';').next().unwrap_or_default().trim(),
    };
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_ascii_lowercase())
    }
}

/// Resolve the declared charset, defaulting to UTF-8.
///
/// A malformed declaration containing a UTF-8 label resolves to UTF-8 with
/// strict decoding; any other unsupported declaration is rejected.
/// @param charset - Declared charset parameter, or `None` to default.
/// @returns The encoding and whether decoding must be strict.
pub fn decoder_for_charset(charset: Option<&str>) -> Result<(&'static Encoding, bool), WebError> {
    let declared = charset.unwrap_or("utf-8");
    if let Some(encoding) = Encoding::for_label(declared.as_bytes()) {
        return Ok((encoding, false));
    }
    let utf8_label = declared
        .split(|character: char| character == ',' || character.is_whitespace())
        .any(|label| {
            !label.is_empty() && Encoding::for_label(label.as_bytes()) == Some(encoding_rs::UTF_8)
        });
    if utf8_label {
        return Ok((encoding_rs::UTF_8, true));
    }
    Err(WebError::new(
        WebErrorCode::UnsupportedContentType,
        format!("unsupported charset \"{declared}\""),
    ))
}

/// Decode one body with the resolved encoding.
///
/// A strict decoder reports invalid bytes as an unsupported content type unless
/// the byte cap already cut the body, which permits an incomplete final character.
/// @param encoding - Resolved encoding.
/// @param strict - Whether the declaration required a strict decoder.
/// @param bytes - Response bytes, already capped.
/// @param truncated - Whether the byte cap cut the body.
/// @returns The decoded text.
pub fn decode_body(
    encoding: &'static Encoding,
    strict: bool,
    bytes: &[u8],
    truncated: bool,
) -> Result<String, WebError> {
    if strict && !truncated {
        return match encoding_rs::UTF_8.decode_without_bom_handling_and_without_replacement(bytes) {
            Some(text) => Ok(text.into_owned()),
            None => Err(WebError::new(
                WebErrorCode::UnsupportedContentType,
                "response body is not valid UTF-8 for the malformed charset declaration",
            )),
        };
    }
    let (text, _, _) = encoding.decode(bytes);
    Ok(text.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_are_limited_to_http_and_https_without_credentials() {
        assert!(validate_fetch_url("https://example.com/a").is_ok());
        let file = validate_fetch_url("file:///etc/passwd").unwrap_err();
        assert_eq!(file.code, WebErrorCode::InvalidUrl);
        let credentials = validate_fetch_url("https://user:pass@example.com/").unwrap_err();
        assert_eq!(credentials.code, WebErrorCode::BlockedUrl);
        let long = format!("https://example.com/{}", "a".repeat(MAX_URL_LENGTH));
        assert_eq!(
            validate_fetch_url(&long).unwrap_err().code,
            WebErrorCode::InvalidUrl
        );
    }

    #[test]
    fn same_origin_ignores_path_and_compares_the_default_port() {
        let left = Url::parse("https://example.com/a").unwrap();
        assert!(is_same_origin(
            &left,
            &Url::parse("https://example.com:443/b").unwrap()
        ));
        assert!(!is_same_origin(
            &left,
            &Url::parse("http://example.com/b").unwrap()
        ));
        assert!(!is_same_origin(
            &left,
            &Url::parse("https://other.example.com/b").unwrap()
        ));
    }

    #[test]
    fn content_types_split_into_html_text_and_unsupported() {
        assert_eq!(
            classify_content_type(Some("text/html; charset=utf-8")),
            Some(FetchableKind::Html)
        );
        assert_eq!(
            classify_content_type(Some("application/xhtml+xml")),
            Some(FetchableKind::Html)
        );
        assert_eq!(
            classify_content_type(Some("text/plain")),
            Some(FetchableKind::Text)
        );
        assert_eq!(
            classify_content_type(Some("application/ld+json")),
            Some(FetchableKind::Text)
        );
        assert_eq!(classify_content_type(Some("image/png")), None);
        assert_eq!(classify_content_type(None), None);
    }

    #[test]
    fn charset_parameters_resolve_to_encodings() {
        assert_eq!(
            parse_charset(Some("text/html; charset=\"ISO-8859-1\"")),
            Some("iso-8859-1".into())
        );
        assert_eq!(parse_charset(Some("text/plain")), None);
        let (encoding, strict) = decoder_for_charset(None).unwrap();
        assert_eq!(encoding, encoding_rs::UTF_8);
        assert!(!strict);
        let (encoding, strict) = decoder_for_charset(Some("utf-8, utf8")).unwrap();
        assert_eq!(encoding, encoding_rs::UTF_8);
        assert!(strict);
        assert_eq!(
            decoder_for_charset(Some("x-unknown")).unwrap_err().code,
            WebErrorCode::UnsupportedContentType
        );
    }

    #[test]
    fn strict_decoding_reports_invalid_utf8_but_tolerates_a_capped_tail() {
        let bytes = [0x61, 0xff, 0x62];
        assert!(decode_body(encoding_rs::UTF_8, true, &bytes, false).is_err());
        assert!(decode_body(encoding_rs::UTF_8, true, &[0x61, 0xe2, 0x82], true).is_ok());
        assert!(decode_body(encoding_rs::UTF_8, false, &bytes, false).is_ok());
    }
}
