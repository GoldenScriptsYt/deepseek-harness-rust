//! Validation for the `dsh-app` protocol. Navigation is limited to the loopback Host.

use std::net::IpAddr;

use tauri::Url;

/// A validated request from the operating system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Focus the existing window without changing its page.
    Open,
    /// Focus the window and navigate to this loopback Host URL.
    Navigate(Url),
}

/// Parse `dsh-app://open` or `dsh-app://open?url=<encoded-loopback-url>`.
///
/// External URLs, credentials, duplicate parameters, and unknown parameters are rejected.
/// Error text deliberately excludes the supplied URL because it may contain a Host token.
pub fn parse(url: &Url) -> Result<Action, String> {
    if url.scheme() != "dsh-app" {
        return Err("the deep link does not use the dsh-app scheme".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("the deep link must not contain URL credentials".into());
    }
    if url.host_str() != Some("open") {
        return Err("the deep link host must be open".into());
    }
    if !url.path().is_empty() && url.path() != "/" {
        return Err("the deep link must not contain a path".into());
    }

    let mut target = None;
    for (key, value) in url.query_pairs() {
        if key != "url" {
            return Err("the deep link contains an unsupported query parameter".into());
        }
        if target.is_some() {
            return Err("the deep link contains more than one url parameter".into());
        }
        let parsed = Url::parse(&value).map_err(|_| "the deep link target is not a valid URL")?;
        if !is_loopback_http(&parsed) {
            return Err("the deep link target must be an HTTP(S) URL on loopback".into());
        }
        target = Some(parsed);
    }

    Ok(target.map_or(Action::Open, Action::Navigate))
}

fn is_loopback_http(url: &Url) -> bool {
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return false;
    }
    url.host_str().is_some_and(|host| {
        if host == "localhost" {
            return true;
        }
        let address = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        address
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_text(value: &str) -> Result<Action, String> {
        parse(&Url::parse(value).unwrap())
    }

    #[test]
    fn open_without_a_target_only_focuses_the_window() {
        assert_eq!(parse_text("dsh-app://open").unwrap(), Action::Open);
        assert_eq!(parse_text("dsh-app://open/").unwrap(), Action::Open);
    }

    #[test]
    fn accepts_encoded_loopback_host_targets() {
        for host in ["127.0.0.1:3000", "localhost:3000", "[::1]:3000"] {
            let target = format!("http://{host}/?token=secret#session");
            let link = format!("dsh-app://open?url={}", urlencoding(&target));
            let action = parse_text(&link).unwrap();
            let Action::Navigate(parsed) = action else {
                panic!("expected a navigation action");
            };
            assert_eq!(parsed.as_str(), target);
        }
    }

    #[test]
    fn rejects_external_or_non_http_targets() {
        for target in [
            "https://example.com/",
            "file:///C:/Windows/System32/calc.exe",
            "javascript:alert(1)",
            "http://127.0.0.1.example.com/",
            "http://user:pass@127.0.0.1:3000/",
        ] {
            let link = format!("dsh-app://open?url={}", urlencoding(target));
            assert!(parse_text(&link).is_err(), "accepted {target}");
        }
    }

    #[test]
    fn rejects_ambiguous_or_unsupported_requests() {
        for link in [
            "https://open/",
            "dsh-app://quit",
            "dsh-app://user@open",
            "dsh-app://open/session",
            "dsh-app://open?next=https%3A%2F%2Fexample.com",
            "dsh-app://open?url=http%3A%2F%2F127.0.0.1&url=http%3A%2F%2Flocalhost",
            "dsh-app://open?url=not-a-url",
        ] {
            assert!(parse_text(link).is_err(), "accepted {link}");
        }
    }

    fn urlencoding(value: &str) -> String {
        let mut output = String::new();
        for byte in value.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                output.push(char::from(byte));
            } else {
                output.push_str(&format!("%{byte:02X}"));
            }
        }
        output
    }
}
