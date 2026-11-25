//! DeepSeek Messages Files API helpers using the connection-owned HTTP client.

use crate::client::{messages_api_root, parse_retry_after, request_id};
use dsh_llm::{ErrorCode, ImageMediaType, LlmError};
use reqwest::header::{HeaderMap, CONTENT_TYPE, RETRY_AFTER};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const MESSAGES_FILES_BETA: &str = "files-api-2025-04-14";
pub const MIN_FILE_EXPIRY_SECONDS: u64 = 3_600;
pub const MAX_FILE_EXPIRY_SECONDS: u64 = 2_592_000;
pub const MAX_FILE_UPLOAD_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_STORED_FILE_COUNT: u64 = 10_000;
pub const MAX_STORED_FILE_BYTES: u64 = 25 * 1024 * 1024 * 1024;
pub const DEFAULT_FILES_API_TIMEOUT: Duration = Duration::from_millis(20_000);
pub const DEFAULT_FILES_API_TIMEOUT_MS: u64 = 20_000;
static UPLOAD_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// One immutable uploaded image.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadFile {
    pub data: Vec<u8>,
    pub media_type: ImageMediaType,
    pub filename: String,
    pub expires_after_seconds: u64,
}

/// Validated provider file metadata.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileObject {
    pub id: String,
    pub bytes: u64,
    pub created_at: u64,
    pub filename: String,
}

/// Upload metadata plus the locally computed reuse deadline.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadedFile {
    pub file: FileObject,
    pub expires_at: u64,
}

/// One provider-ordered Files page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilePage {
    pub data: Vec<FileObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_id: Option<String>,
    pub has_more: bool,
}

/// Pagination for [`crate::client::Connection::list_files`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListFiles {
    pub after: Option<String>,
    pub limit: Option<u64>,
}

fn invalid_response(operation: &str) -> LlmError {
    LlmError::new(
        ErrorCode::InvalidResponse,
        format!("DeepSeek Files API returned an invalid {operation} response."),
    )
}

fn transport(error: impl std::fmt::Display) -> LlmError {
    LlmError::new(
        ErrorCode::Transport,
        format!("DeepSeek Files API transport failed: {error}"),
    )
}

fn request_transport(error: reqwest::Error) -> LlmError {
    if error.is_timeout() {
        LlmError::new(
            ErrorCode::Timeout,
            format!("DeepSeek Files API request timed out: {error}"),
        )
    } else {
        transport(error)
    }
}

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

fn is_safe_integer(value: u64) -> bool {
    value <= MAX_SAFE_INTEGER
}

fn parse_file_object(value: &Value, operation: &str) -> Result<FileObject, LlmError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_response(operation))?;
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| invalid_response(operation))?;
    if object.get("type").and_then(Value::as_str) != Some("file")
        || !object.get("mime_type").is_some_and(Value::is_string)
        || !object
            .get("size_bytes")
            .is_some_and(|value| value.as_u64().is_some_and(is_safe_integer))
        || object
            .get("filename")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(invalid_response(operation));
    }
    let created_at = object
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(parse_created_at)
        .ok_or_else(|| invalid_response(operation))?;
    Ok(FileObject {
        id: id.to_string(),
        bytes: object["size_bytes"].as_u64().unwrap(),
        created_at,
        filename: object["filename"].as_str().unwrap().to_string(),
    })
}

fn parse_created_at(value: &str) -> Option<u64> {
    if let Ok(instant) = httpdate::parse_http_date(value) {
        return instant
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs());
    }
    // Files-compatible gateways sometimes return ISO-8601 rather than RFC 7231.
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b'T' | b't' | b' '))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let number = |range: std::ops::Range<usize>| value.get(range)?.parse::<i64>().ok();
    let year = number(0..4)?;
    let month = number(5..7)?;
    let day = number(8..10)?;
    let hour = number(11..13)?;
    let minute = number(14..16)?;
    let second = number(17..19)?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day < 1 || day > days_in_month {
        return None;
    }
    let mut index = 19;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if start == index {
            return None;
        }
    }
    let offset = match bytes.get(index..) {
        Some([b'Z' | b'z']) => 0,
        Some([sign @ (b'+' | b'-'), _, _, b':', _, _]) => {
            let hours = value.get(index + 1..index + 3)?.parse::<i64>().ok()?;
            let minutes = value.get(index + 4..index + 6)?.parse::<i64>().ok()?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            (hours * 3600 + minutes * 60) * if *sign == b'+' { 1 } else { -1 }
        }
        Some([]) => 0,
        _ => return None,
    };
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset;
    u64::try_from(seconds)
        .ok()
        .filter(|value| is_safe_integer(*value))
}

fn files_url(connection: &crate::client::Connection) -> Result<reqwest::Url, LlmError> {
    reqwest::Url::parse(&format!(
        "{}/files",
        messages_api_root(&connection.base_url)
    ))
    .map_err(|error| {
        LlmError::new(
            ErrorCode::InvalidRequest,
            format!("DeepSeek Files API base URL is invalid: {error}"),
        )
    })
}

fn file_url(connection: &crate::client::Connection, id: &str) -> Result<reqwest::Url, LlmError> {
    if id.is_empty() {
        return Err(LlmError::new(
            ErrorCode::InvalidRequest,
            "DeepSeek Files API file id must not be empty",
        ));
    }
    let mut url = files_url(connection)?;
    url.path_segments_mut()
        .map_err(|_| {
            LlmError::new(
                ErrorCode::InvalidRequest,
                "DeepSeek Files API base URL cannot be a prefix",
            )
        })?
        .push(id);
    Ok(url)
}

fn apply_headers(
    request: reqwest::RequestBuilder,
    connection: &crate::client::Connection,
) -> reqwest::RequestBuilder {
    request
        .header("x-api-key", &connection.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", MESSAGES_FILES_BETA)
}

async fn request_json(
    request: reqwest::RequestBuilder,
    operation: &str,
) -> Result<Value, LlmError> {
    tokio::time::timeout(DEFAULT_FILES_API_TIMEOUT, async move {
        let response = request.send().await.map_err(request_transport)?;
        if response.status().is_redirection() {
            return Err(transport("redirects are not allowed"));
        }
        successful_json(response, operation).await
    })
    .await
    .map_err(|_| {
        LlmError::new(
            ErrorCode::Timeout,
            "DeepSeek Files API request timed out after 20000 ms",
        )
    })?
}

async fn successful_json(response: reqwest::Response, operation: &str) -> Result<Value, LlmError> {
    let status = response.status();
    let headers = response.headers().clone();
    if !status.is_success() {
        let raw = response.json::<Value>().await.unwrap_or(Value::Null);
        return Err(files_response_error(&raw, status.as_u16(), &headers));
    }
    response
        .json::<Value>()
        .await
        .map_err(|_| invalid_response(operation).with_status(status.as_u16()))
}

fn files_response_error(raw: &Value, status: u16, headers: &HeaderMap) -> LlmError {
    let fields = raw.get("error").and_then(Value::as_object);
    let text = |name: &str| {
        fields
            .and_then(|fields| fields.get(name))
            .and_then(Value::as_str)
    };
    let detail = [text("code"), text("type"), text("message")]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    let message = text("message")
        .map(str::to_string)
        .unwrap_or_else(|| format!("DeepSeek Files API error (HTTP {status})"));
    let code = match status {
        401 | 403 => ErrorCode::Auth,
        429 => ErrorCode::RateLimit,
        500.. => ErrorCode::Server,
        _ => ErrorCode::FilesApi,
    };
    let mut error = LlmError::new(code, message)
        .with_status(status)
        .with_provider_detail(detail);
    if let Some(delay) = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after)
    {
        error = error.with_retry_after(delay);
    }
    if let Some(id) = request_id(headers) {
        error = error.with_request_id(id);
    }
    error
}

/// Whether a Files failure permits one bounded cleanup-and-upload recovery.
pub fn is_files_quota_error(error: &LlmError) -> bool {
    error.code == ErrorCode::FilesApi
        && error.provider_detail.as_deref().is_some_and(|detail| {
            let detail = detail.to_lowercase();
            [
                "quota",
                "storage",
                "stored files",
                "file count",
                "too many files",
            ]
            .iter()
            .any(|term| detail.contains(term))
        })
}

fn multipart(input: &UploadFile) -> (Vec<u8>, String) {
    let sequence = UPLOAD_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let boundary = format!("----dsh-{}-{timestamp}-{sequence}", std::process::id());
    let filename = input
        .filename
        .replace('"', "%22")
        .replace('\r', "%0D")
        .replace('\n', "%0A");
    let head = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"expires_after[anchor]\"\r\n\r\ncreated_at\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"expires_after[seconds]\"\r\n\r\n{}\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {}\r\n\r\n",
        input.expires_after_seconds, input.media_type.as_str()
    );
    let tail = format!("\r\n--{boundary}--\r\n");
    let mut body = Vec::with_capacity(head.len() + input.data.len() + tail.len());
    body.extend_from_slice(head.as_bytes());
    body.extend_from_slice(&input.data);
    body.extend_from_slice(tail.as_bytes());
    (body, boundary)
}

impl crate::client::Connection {
    /// Upload one image with explicit expiry. Input bounds are checked before transport.
    pub async fn upload_file(&self, input: UploadFile) -> Result<UploadedFile, LlmError> {
        if input.data.len() > MAX_FILE_UPLOAD_BYTES {
            return Err(LlmError::new(
                ErrorCode::InvalidRequest,
                "DeepSeek Files API upload exceeds 128 MiB.",
            ));
        }
        if !(MIN_FILE_EXPIRY_SECONDS..=MAX_FILE_EXPIRY_SECONDS)
            .contains(&input.expires_after_seconds)
        {
            return Err(LlmError::new(
                ErrorCode::InvalidRequest,
                "DeepSeek file expiry must be between 3600 and 2592000 seconds.",
            ));
        }
        let (body, boundary) = multipart(&input);
        let request = apply_headers(self.client()?.post(files_url(self)?), self)
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body);
        let file = parse_file_object(&request_json(request, "upload").await?, "upload")?;
        let expires_at = file
            .created_at
            .checked_add(input.expires_after_seconds)
            .ok_or_else(|| invalid_response("upload"))?;
        Ok(UploadedFile { file, expires_at })
    }

    /// List one provider-ordered page of files.
    pub async fn list_files(&self, options: ListFiles) -> Result<FilePage, LlmError> {
        let mut url = files_url(self)?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(after) = &options.after {
                query.append_pair("after_id", after);
            }
            if let Some(limit) = options.limit {
                query.append_pair("limit", &limit.to_string());
            }
        }
        let request = apply_headers(self.client()?.get(url), self);
        let value = request_json(request, "list").await?;
        let object = value.as_object().ok_or_else(|| invalid_response("list"))?;
        if !object.get("has_more").is_some_and(Value::is_boolean)
            || !object.get("data").is_some_and(Value::is_array)
            || !matches!(
                object.get("first_id"),
                None | Some(Value::Null) | Some(Value::String(_))
            )
            || !matches!(
                object.get("last_id"),
                None | Some(Value::Null) | Some(Value::String(_))
            )
        {
            return Err(invalid_response("list"));
        }
        let cursor = |name: &str| match object.get(name) {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.clone()),
            _ => unreachable!(),
        };
        let data = object["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| parse_file_object(value, "list"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FilePage {
            data,
            first_id: cursor("first_id"),
            last_id: cursor("last_id"),
            has_more: object["has_more"].as_bool().unwrap(),
        })
    }

    /// Retrieve one provider file's metadata.
    pub async fn retrieve_file(&self, id: &str) -> Result<FileObject, LlmError> {
        let request = apply_headers(self.client()?.get(file_url(self, id)?), self);
        parse_file_object(&request_json(request, "retrieve").await?, "retrieve")
    }

    /// Delete one provider file and validate the returned identity.
    pub async fn delete_file(&self, id: &str) -> Result<(), LlmError> {
        let request = apply_headers(self.client()?.delete(file_url(self, id)?), self);
        let value = request_json(request, "delete").await?;
        if value.get("id").and_then(Value::as_str) != Some(id)
            || value.get("type").and_then(Value::as_str) != Some("file_deleted")
        {
            return Err(invalid_response("delete"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn file_expiry_bounds_are_inclusive() {
        assert!(parse_created_at("Sun, 06 Nov 1994 08:49:37 GMT").is_some());
        assert_eq!(
            parse_created_at("2023-11-14T22:13:20.000Z"),
            Some(1_700_000_000)
        );
        assert_eq!(
            parse_created_at("2023-11-14T23:13:20+01:00"),
            Some(1_700_000_000)
        );
        assert_eq!(parse_created_at("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_created_at("1969-12-31T23:59:59Z"), None);
        assert_eq!(parse_created_at("2023-02-29T00:00:00Z"), None);
    }

    #[test]
    fn validates_file_metadata() {
        let valid = json!({"id":"file-1","type":"file","mime_type":"image/png","size_bytes":3,"created_at":"2023-11-14T22:13:20.000Z","filename":"a.png"});
        assert_eq!(parse_file_object(&valid, "retrieve").unwrap().bytes, 3);
        for invalid in [json!(null), json!([]), json!({}), json!({"id":1})] {
            assert_eq!(
                parse_file_object(&invalid, "retrieve").unwrap_err().code,
                ErrorCode::InvalidResponse
            );
        }
        for size in [
            json!(MAX_SAFE_INTEGER + 1),
            json!(u64::MAX),
            json!(-1),
            json!(1.5),
        ] {
            let mut invalid = valid.clone();
            invalid["size_bytes"] = size;
            assert_eq!(
                parse_file_object(&invalid, "retrieve").unwrap_err().code,
                ErrorCode::InvalidResponse
            );
        }
        let mut largest = valid;
        largest["size_bytes"] = json!(MAX_SAFE_INTEGER);
        assert_eq!(
            parse_file_object(&largest, "retrieve").unwrap().bytes,
            MAX_SAFE_INTEGER
        );
    }

    #[test]
    fn files_error_classification_and_quota_detection() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "2".parse().unwrap());
        for (status, expected) in [
            (401, ErrorCode::Auth),
            (403, ErrorCode::Auth),
            (429, ErrorCode::RateLimit),
            (500, ErrorCode::Server),
            (400, ErrorCode::FilesApi),
        ] {
            let error = files_response_error(&Value::Null, status, &headers);
            assert_eq!(error.code, expected);
            assert_eq!(error.status, Some(status));
            assert_eq!(error.retry_after_ms, Some(2000));
        }
        let error = files_response_error(
            &json!({"error":{"code":"file_quota","message":"storage quota exceeded"}}),
            400,
            &HeaderMap::new(),
        );
        assert!(is_files_quota_error(&error));
    }
}
