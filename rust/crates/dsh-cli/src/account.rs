//! DeepSeek Platform browser sign-in for the Rust Host.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use dsh_gateway::{Gateway, RawGetRequest, RawGetResponse, RpcError};
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use rand::RngCore;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
#[cfg(not(windows))]
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

const PLATFORM_ORIGIN: &str = "https://platform.deepseek.com";
const GRANT_FILE: &str = "account-grant.dpapi";
const MAX_RESPONSE_BYTES: usize = 65_536;
const ATTEMPT_LIMIT: Duration = Duration::from_secs(600);

/// Persistent Platform grant; this value is encrypted before it reaches disk.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct AccountGrant {
    version: u8,
    issuer: String,
    token: String,
    device_id: String,
}

#[derive(Clone)]
struct SignInAttempt {
    id: String,
    phase: String,
    error_code: Option<String>,
    authorize_url: Option<String>,
    expires_at: Option<u64>,
    callback_origin: String,
    login_source: String,
    locale: String,
    headers: Vec<(String, String)>,
    state: String,
    verifier: String,
    device_id: String,
    authorize_id: Option<String>,
}

#[derive(Default)]
struct AccountState {
    grant: Option<AccountGrant>,
    attempt: Option<SignInAttempt>,
}

struct Inner {
    home: PathBuf,
    state: Mutex<AccountState>,
    updates: dsh_gateway::Broadcaster,
    http: Client,
}

/// Cloneable account state and OAuth controller shared by HTTP routes.
#[derive(Clone)]
pub(crate) struct AccountController(Arc<Inner>);

impl AccountController {
    /// Load the OS-protected account grant and initialize the live account stream.
    pub(crate) fn new(home: &Path, updates: dsh_gateway::Broadcaster) -> Self {
        let grant = load_grant(home);
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("the standard HTTP client configuration is valid");
        AccountController(Arc::new(Inner {
            home: home.to_path_buf(),
            state: Mutex::new(AccountState {
                grant,
                attempt: None,
            }),
            updates,
            http,
        }))
    }

    /// Return the account-safe projection; tokens and PKCE material never enter this value.
    pub(crate) fn view(&self) -> Value {
        let state = self.0.state.lock().unwrap();
        account_view(&state)
    }

    /// Subscribe before reading the snapshot so no account update can be lost at stream startup.
    pub(crate) fn watch(&self) -> BoxStream<'static, Result<Value, RpcError>> {
        let receiver = self.0.updates.subscribe();
        let snapshot = json!({"type":"snapshot","state":self.view()});
        stream::once(async move { Ok(snapshot) })
            .chain(live_updates(receiver))
            .boxed()
    }

    pub(crate) async fn start_sign_in(&self, payload: Value) -> Result<Value, RpcError> {
        let args = payload.get("args").unwrap_or(&payload);
        let client = args
            .get("client")
            .or_else(|| args.get(0))
            .unwrap_or(&Value::Null);
        let callback_origin = args
            .get("callbackOrigin")
            .or_else(|| args.get(1))
            .and_then(Value::as_str)
            .ok_or_else(|| rpc_error("gateway/bad-request", "Missing loopback callback origin"))?;
        let callback_origin = validate_callback_origin(callback_origin)
            .ok_or_else(|| rpc_error("gateway/bad-request", "Invalid loopback callback origin"))?;
        let login_source = match args
            .get("loginSource")
            .or_else(|| args.get(2))
            .and_then(Value::as_str)
        {
            Some("desktop") => "desktop",
            Some("web") => "web",
            _ => return Err(rpc_error("gateway/bad-request", "Invalid sign-in source")),
        };
        let locale = wire_locale(client.get("locale").and_then(Value::as_str).unwrap_or("en"));
        let version = valid_client_version(client.get("version").and_then(Value::as_str));
        let timezone = client
            .get("timezoneOffsetSeconds")
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .clamp(-86_400, 86_400)
            .to_string();
        let headers = platform_headers(&version, &locale, &timezone);
        let verifier = random_code();
        let state_token = random_code();
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let device_id = self
            .0
            .state
            .lock()
            .unwrap()
            .grant
            .as_ref()
            .map(|grant| grant.device_id.clone())
            .unwrap_or_else(random_uuid);
        let attempt = SignInAttempt {
            id: random_uuid(),
            phase: "initializing".into(),
            error_code: None,
            authorize_url: None,
            expires_at: None,
            callback_origin,
            login_source: login_source.into(),
            locale: locale.into(),
            headers,
            state: state_token,
            verifier,
            device_id,
            authorize_id: None,
        };
        {
            let mut state = self.0.state.lock().unwrap();
            if state
                .attempt
                .as_ref()
                .is_some_and(|current| is_active(&current.phase))
            {
                return Ok(account_view(&state));
            }
            state.attempt = Some(attempt.clone());
        }
        self.publish("snapshot");

        let redirect_uri = format!("{}/oauth/callback", attempt.callback_origin);
        let response = self
            .post_platform(
                "auth_init",
                json!({
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                    "state": attempt.state,
                    "redirect_uri": redirect_uri,
                    "locale": attempt.locale,
                    "login_source": attempt.login_source,
                }),
                &attempt.headers,
            )
            .await;
        let data = match response {
            Ok(data) => data,
            Err(code) => {
                self.fail_attempt(&attempt.id, code);
                return Err(rpc_error(
                    "account/sign-in-failed",
                    "DeepSeek sign-in could not be started",
                ));
            }
        };
        let Some(authorize_id) = data
            .get("authorize_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
        else {
            self.fail_attempt(&attempt.id, "protocol");
            return Err(rpc_error(
                "account/sign-in-failed",
                "DeepSeek returned an invalid sign-in response",
            ));
        };
        let Some(authorize_url) = data
            .get("authorize_url")
            .and_then(Value::as_str)
            .and_then(|url| validate_platform_url(url, "/dsh/authorize"))
        else {
            let _ = self.cancel_authorization(&attempt, &authorize_id).await;
            self.fail_attempt(&attempt.id, "protocol");
            return Err(rpc_error(
                "account/sign-in-failed",
                "DeepSeek returned an invalid sign-in link",
            ));
        };
        let expires_in = data
            .get("expires_in")
            .and_then(Value::as_u64)
            .filter(|seconds| *seconds > 0);
        let Some(expires_in) = expires_in else {
            let _ = self.cancel_authorization(&attempt, &authorize_id).await;
            self.fail_attempt(&attempt.id, "protocol");
            return Err(rpc_error(
                "account/sign-in-failed",
                "DeepSeek returned an invalid sign-in expiry",
            ));
        };
        let expires_at = now_ms()
            .saturating_add(expires_in.saturating_mul(1000))
            .min(now_ms().saturating_add(ATTEMPT_LIMIT.as_millis() as u64));
        let attempt_is_waiting = {
            let mut state = self.0.state.lock().unwrap();
            if let Some(current) = state
                .attempt
                .as_mut()
                .filter(|current| current.id == attempt.id && current.phase == "initializing")
            {
                current.phase = "waiting-browser".into();
                current.authorize_url = Some(authorize_url.to_string());
                current.expires_at = Some(expires_at);
                current.authorize_id = Some(authorize_id.clone());
                true
            } else {
                false
            }
        };
        if !attempt_is_waiting {
            let _ = self.cancel_authorization(&attempt, &authorize_id).await;
            return Ok(self.view());
        }
        self.publish("snapshot");
        if login_source == "desktop" && open_in_browser(authorize_url).is_err() {
            let _ = self.cancel_authorization(&attempt, &authorize_id).await;
            self.fail_attempt(&attempt.id, "network");
            return Err(rpc_error(
                "account/sign-in-failed",
                "The sign-in page could not be opened",
            ));
        }
        Ok(self.view())
    }

    pub(crate) fn cancel_sign_in(&self, payload: &Value) -> Result<Value, RpcError> {
        let args = payload.get("args").unwrap_or(payload);
        let attempt_id = args
            .get("attemptId")
            .or_else(|| args.get(0))
            .and_then(Value::as_str)
            .ok_or_else(|| rpc_error("gateway/bad-request", "Missing sign-in attempt id"))?;
        let mut cancelled = None;
        {
            let mut state = self.0.state.lock().unwrap();
            if let Some(attempt) = state
                .attempt
                .as_mut()
                .filter(|attempt| attempt.id == attempt_id && is_active(&attempt.phase))
            {
                attempt.phase = "cancelled".into();
                attempt.authorize_url = None;
                cancelled = Some(attempt.clone());
            }
        }
        if let Some(attempt) = cancelled {
            self.publish("snapshot");
            if let Some(authorize_id) = attempt.authorize_id.as_deref() {
                let controller = self.clone();
                let attempt = attempt.clone();
                let authorize_id = authorize_id.to_owned();
                tokio::spawn(async move {
                    let _ = controller
                        .cancel_authorization(&attempt, &authorize_id)
                        .await;
                });
            }
        }
        Ok(self.view())
    }

    async fn cancel_authorization(
        &self,
        attempt: &SignInAttempt,
        authorize_id: &str,
    ) -> Result<(), &'static str> {
        self.post_platform(
            "auth_cancel",
            json!({
                "authorize_id": authorize_id,
                "code_verifier": attempt.verifier,
            }),
            &attempt.headers,
        )
        .await
        .map(|_| ())
    }

    /// Read the signed-in Platform profile; credentials remain inside the Rust Host.
    pub(crate) async fn get_profile(&self, payload: Value) -> Value {
        let Some(grant) = self.0.state.lock().unwrap().grant.clone() else {
            return Value::Null;
        };
        let headers = client_headers(&payload);
        let data = match self
            .get_account_data("/auth-api/v0/users/current", &grant.token, &headers)
            .await
        {
            Ok(data) => data,
            Err(_) => return json!({"status":"failed"}),
        };
        let Some(object) = data.as_object() else {
            return json!({"status":"failed"});
        };
        let identity = object.get("id_profile").and_then(Value::as_object);
        let identity_text = |key: &str| {
            identity
                .and_then(|profile| profile.get(key))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
        };
        let text = |key: &str| {
            object
                .get(key)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
        };
        let contact = text("mobile")
            .or_else(|| text("mobile_number"))
            .or_else(|| text("email"));
        json!({
            "status":"ready",
            "value": {
                "id": text("id"),
                "name": identity_text("name"),
                "contact": contact,
                "avatarUrl": identity_text("picture"),
            }
        })
    }

    /// Read recharge and bonus wallets independently from the profile.
    pub(crate) async fn get_balance(&self, payload: Value) -> Value {
        let Some(grant) = self.0.state.lock().unwrap().grant.clone() else {
            return Value::Null;
        };
        let headers = client_headers(&payload);
        let data = match self
            .get_account_data("/api/v0/users/get_user_summary", &grant.token, &headers)
            .await
        {
            Ok(data) => data,
            Err(_) => return json!({"status":"failed"}),
        };
        let wallets = |field: &str| -> Option<Vec<Value>> {
            data.get(field)?
                .as_array()?
                .iter()
                .map(|row| {
                    let currency = row.get("currency")?.as_str()?;
                    let balance = row.get("balance")?.as_str()?;
                    (matches!(currency, "CNY" | "USD") && valid_decimal(balance))
                        .then(|| json!({"currency":currency,"balance":balance}))
                })
                .collect()
        };
        let (Some(value), Some(bonus_wallets)) =
            (wallets("normal_wallets"), wallets("bonus_wallets"))
        else {
            return json!({"status":"failed"});
        };
        json!({"status":"ready","value":value,"bonusWallets":bonus_wallets})
    }

    async fn get_account_data(
        &self,
        path: &str,
        token: &str,
        headers: &[(String, String)],
    ) -> Result<Value, &'static str> {
        let url = format!("{PLATFORM_ORIGIN}{path}");
        let mut request = self.0.http.get(url).header("x-dsh-auth-token", token);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let mut response = request.send().await.map_err(|_| "no-response")?;
        if !response.status().is_success() {
            eprintln!("[deepseek-account] request failed {{ path: '{path}', status: {}, errorCode: 'network' }}", response.status().as_u16());
            return Err("network");
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk))
                    if bytes.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES =>
                {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(Some(_)) => return Err("protocol"),
                Ok(None) => break,
                Err(_) => return Err("network"),
            }
        }
        let envelope: Value = serde_json::from_slice(&bytes).map_err(|_| "protocol")?;
        if envelope["code"].as_i64() != Some(0) || envelope["data"]["biz_code"].as_i64() != Some(0)
        {
            return Err("protocol");
        }
        envelope["data"].get("biz_data").cloned().ok_or("protocol")
    }

    pub(crate) fn sign_out(&self) -> Result<Value, RpcError> {
        remove_grant(&self.0.home).map_err(|_| {
            rpc_error(
                "account/storage",
                "DeepSeek sign-out could not clear the saved account",
            )
        })?;
        {
            let mut state = self.0.state.lock().unwrap();
            state.grant = None;
            state.attempt = None;
        }
        self.publish("credentials-changed");
        Ok(self.view())
    }

    fn publish(&self, update_type: &str) {
        self.0
            .updates
            .publish(json!({"type":update_type,"state":self.view()}));
    }

    fn fail_attempt(&self, id: &str, code: &str) {
        let changed = {
            let mut state = self.0.state.lock().unwrap();
            state
                .attempt
                .as_mut()
                .filter(|attempt| attempt.id == id)
                .map(|attempt| {
                    attempt.phase = if code == "expired" {
                        "expired"
                    } else {
                        "failed"
                    }
                    .into();
                    attempt.error_code = Some(code.to_owned());
                    attempt.authorize_url = None;
                })
                .is_some()
        };
        if changed {
            self.publish("snapshot");
        }
    }

    async fn post_platform(
        &self,
        path: &str,
        body: Value,
        headers: &[(String, String)],
    ) -> Result<Value, &'static str> {
        let mut request = self
            .0
            .http
            .post(format!("{PLATFORM_ORIGIN}/auth-api/v0/dsh/{path}"))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(_) => {
                eprintln!("[deepseek-account] request failed {{ path: '/auth-api/v0/dsh/{path}', errorCode: 'no-response' }}");
                return Err("no-response");
            }
        };
        if !response.status().is_success() {
            eprintln!("[deepseek-account] request failed {{ path: '/auth-api/v0/dsh/{path}', status: {}, errorCode: 'network' }}", response.status().as_u16());
            return Err("network");
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk))
                    if bytes.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES =>
                {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(Some(_)) => return Err("protocol"),
                Ok(None) => break,
                Err(_) => return Err("network"),
            }
        }
        let envelope: Value = serde_json::from_slice(&bytes).map_err(|_| "protocol")?;
        if envelope["code"].as_i64() != Some(0) || envelope["data"]["biz_code"].as_i64() != Some(0)
        {
            return Err("protocol");
        }
        envelope
            .get("data")
            .and_then(|data| data.get("biz_data"))
            .cloned()
            .ok_or("protocol")
    }

    async fn callback(&self, request: RawGetRequest) -> RawGetResponse {
        let mut code = None;
        let mut received_state = None;
        let mut duplicate = false;
        for (key, value) in form_urlencoded::parse(request.query.as_bytes()) {
            match key.as_ref() {
                "code" if code.is_none() => code = Some(value.into_owned()),
                "state" if received_state.is_none() => received_state = Some(value.into_owned()),
                "code" | "state" => duplicate = true,
                _ => {}
            }
        }
        let (Some(code), Some(received_state)) = (code, received_state) else {
            return callback_response(400, "Invalid sign-in response");
        };
        if duplicate || code.is_empty() || code.len() > 4096 {
            return callback_response(400, "Invalid sign-in response");
        }
        let (attempt, expired) = {
            let mut state = self.0.state.lock().unwrap();
            let Some(attempt) = state.attempt.as_mut() else {
                return callback_response(410, "This sign-in attempt has expired");
            };
            if attempt.phase != "waiting-browser"
                || !constant_time_eq(&received_state, &attempt.state)
            {
                return callback_response(400, "Invalid sign-in response");
            }
            if attempt.expires_at.is_some_and(|expiry| now_ms() >= expiry) {
                attempt.phase = "expired".into();
                attempt.error_code = Some("expired".into());
                attempt.authorize_url = None;
                (None, true)
            } else {
                attempt.phase = "exchanging".into();
                (Some(attempt.clone()), false)
            }
        };
        if expired {
            self.publish("snapshot");
            return callback_response(410, "This sign-in attempt has expired");
        }
        let Some(attempt) = attempt else {
            return callback_response(410, "This sign-in attempt has expired");
        };
        self.publish("snapshot");
        let redirect_uri = format!("{}/oauth/callback", attempt.callback_origin);
        let device_model = if cfg!(target_os = "windows") {
            "win32-x64"
        } else if cfg!(target_os = "macos") {
            "darwin-arm64"
        } else {
            "linux-x64"
        };
        let os_version = std::env::consts::OS;
        let exchange = self
            .post_platform(
                "auth_exchange",
                json!({
                    "code": code,
                    "code_verifier": attempt.verifier,
                    "redirect_uri": redirect_uri,
                    "device_id": attempt.device_id,
                    "device_model": device_model,
                    "os_version": os_version,
                }),
                &attempt.headers,
            )
            .await;
        let data = match exchange {
            Ok(data) => data,
            Err(error) => {
                self.fail_attempt(&attempt.id, error);
                return callback_response(502, "DeepSeek sign-in could not be completed");
            }
        };
        let Some(token) = data.get("token").and_then(Value::as_str).filter(|token| {
            !token.is_empty()
                && token.len() <= 8192
                && token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        }) else {
            self.fail_attempt(&attempt.id, "protocol");
            return callback_response(502, "DeepSeek returned an invalid account response");
        };
        let Some(authorized_url) = data
            .get("authorized_url")
            .and_then(Value::as_str)
            .and_then(|url| validate_platform_url(url, "/dsh/authorized"))
        else {
            self.fail_attempt(&attempt.id, "protocol");
            return callback_response(502, "DeepSeek returned an invalid account response");
        };
        let grant = AccountGrant {
            version: 1,
            issuer: PLATFORM_ORIGIN.into(),
            token: token.into(),
            device_id: attempt.device_id.clone(),
        };
        if store_grant(&self.0.home, &grant).is_err() {
            self.fail_attempt(&attempt.id, "storage");
            return callback_response(500, "DeepSeek account could not be saved securely");
        }
        {
            let mut state = self.0.state.lock().unwrap();
            if !state
                .attempt
                .as_ref()
                .is_some_and(|current| current.id == attempt.id && current.phase == "exchanging")
            {
                drop(state);
                let _ = remove_grant(&self.0.home);
                return callback_response(410, "This sign-in attempt has expired");
            }
            state.grant = Some(grant);
            let current = state
                .attempt
                .as_mut()
                .expect("the attempt was checked above");
            current.phase = "succeeded".into();
            current.error_code = None;
            current.authorize_url = None;
        }
        self.publish("credentials-changed");
        let mut completion = match Url::parse(authorized_url) {
            Ok(url) => url,
            Err(_) => {
                return callback_response(502, "DeepSeek returned an invalid account response")
            }
        };
        completion
            .query_pairs_mut()
            .append_pair("login_source", &attempt.login_source);
        RawGetResponse {
            status: 302,
            location: Some(completion.to_string()),
            body: String::new(),
        }
    }
}

/// Register account RPCs, the state stream, and the validated browser return path.
pub(crate) fn register_all(gateway: &Gateway, account: AccountController) {
    let current = account.clone();
    gateway.stream("account/watch", move |_payload, _uplink| current.watch());
    let current = account.clone();
    gateway.rpc("account/startSignIn", move |payload| {
        let account = current.clone();
        async move { account.start_sign_in(payload).await }
    });
    let current = account.clone();
    gateway.rpc("account/cancelSignIn", move |payload| {
        let account = current.clone();
        async move { account.cancel_sign_in(&payload) }
    });
    let current = account.clone();
    gateway.rpc("account/signOut", move |_| {
        let account = current.clone();
        async move { account.sign_out() }
    });
    let current = account.clone();
    gateway.rpc("account/getProfile", move |payload| {
        let account = current.clone();
        async move { Ok(account.get_profile(payload).await) }
    });
    let current = account.clone();
    gateway.rpc("account/getBalance", move |payload| {
        let account = current.clone();
        async move { Ok(account.get_balance(payload).await) }
    });
    gateway.rpc("account/hasRunningAccountTasks", |_| async {
        // Account-backed model routing is not mounted in the Rust Host yet.
        Ok(json!(false))
    });
    gateway.raw_get("/oauth/callback", move |request| {
        let account = account.clone();
        async move { account.callback(request).await }
    });
}

fn live_updates(
    receiver: broadcast::Receiver<Value>,
) -> BoxStream<'static, Result<Value, RpcError>> {
    stream::unfold(receiver, |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(frame) => return Some((Ok(frame), receiver)),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
    .boxed()
}

fn account_view(state: &AccountState) -> Value {
    let attempt = state.attempt.as_ref().map(|attempt| {
        let mut view = json!({"id":attempt.id,"phase":attempt.phase});
        if let Some(url) = attempt.authorize_url.as_ref() {
            view["authorizeUrl"] = json!(url);
        }
        if let Some(expiry) = attempt.expires_at {
            view["expiresAt"] = json!(expiry);
        }
        if let Some(code) = attempt.error_code.as_ref() {
            view["errorCode"] = json!(code);
        }
        view
    });
    json!({
        "status": if state.grant.is_some() { "credential-stored" } else { "signed-out" },
        "attempt": attempt,
        "links": { "usageUrl": "https://platform.deepseek.com/usage", "topUpUrl": "https://platform.deepseek.com/top_up" }
    })
}

fn callback_response(status: u16, body: &str) -> RawGetResponse {
    RawGetResponse {
        status,
        location: None,
        body: body.into(),
    }
}

fn rpc_error(code: &str, message: &str) -> RpcError {
    RpcError::new(code, message)
}

fn is_active(phase: &str) -> bool {
    matches!(
        phase,
        "initializing" | "waiting-browser" | "exchanging" | "committing"
    )
}

fn wire_locale(locale: &str) -> &'static str {
    if locale.to_ascii_lowercase().split(['-', '_']).next() == Some("zh") {
        "zh_CN"
    } else {
        "en_US"
    }
}

fn valid_client_version(value: Option<&str>) -> String {
    let version = value.unwrap_or("0.2.3");
    if version.len() <= 64
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
    {
        version.to_owned()
    } else {
        "0.2.3".into()
    }
}

fn platform_headers(version: &str, locale: &str, timezone: &str) -> Vec<(String, String)> {
    let platform = if cfg!(target_os = "windows") {
        "desktop-win"
    } else if cfg!(target_os = "macos") {
        "desktop-mac"
    } else {
        "web"
    };
    vec![
        ("x-client-bundle-id".into(), String::new()),
        ("x-client-platform".into(), platform.into()),
        ("x-client-version".into(), version.into()),
        ("x-client-locale".into(), locale.into()),
        ("x-client-timezone-offset".into(), timezone.into()),
    ]
}

fn client_headers(payload: &Value) -> Vec<(String, String)> {
    let args = payload.get("args").unwrap_or(payload);
    let client = args.get("client").or_else(|| args.get(0)).unwrap_or(args);
    let version = valid_client_version(client.get("version").and_then(Value::as_str));
    let locale = wire_locale(client.get("locale").and_then(Value::as_str).unwrap_or("en"));
    let timezone = client
        .get("timezoneOffsetSeconds")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .clamp(-86_400, 86_400)
        .to_string();
    platform_headers(&version, locale, &timezone)
}

fn valid_decimal(value: &str) -> bool {
    if value.is_empty() || value.len() > 128 {
        return false;
    }
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map_or((value, None), |(mantissa, exponent)| {
            (mantissa, Some(exponent))
        });
    if let Some(exponent) = exponent {
        let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return false;
        }
    }
    let mantissa = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let mut parts = mantissa.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    parts.next().is_none()
        && (whole.bytes().any(|byte| byte.is_ascii_digit())
            || fraction.is_some_and(|fraction| fraction.bytes().any(|byte| byte.is_ascii_digit())))
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && fraction.is_none_or(|fraction| fraction.bytes().all(|byte| byte.is_ascii_digit()))
}

fn validate_callback_origin(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let host = url.host_str()?;
    if url.scheme() != "http"
        || !matches!(host, "localhost" | "127.0.0.1" | "::1")
        || url.port().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    Some(url.origin().ascii_serialization())
}

fn validate_platform_url<'a>(value: &'a str, path: &str) -> Option<&'a str> {
    let url = Url::parse(value).ok()?;
    if url.scheme() != "https"
        || url.host_str() != Some("platform.deepseek.com")
        || url.port().is_some()
        || url.username() != ""
        || url.password().is_some()
        || url.path() != path
        || url.fragment().is_some()
    {
        return None;
    }
    Some(value)
}

fn random_code() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn random_uuid() -> String {
    let mut bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15])
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let a = left.as_bytes();
    let b = right.as_bytes();
    let mut difference = a.len() ^ b.len();
    for index in 0..a.len().max(b.len()) {
        difference |= usize::from(
            a.get(index).copied().unwrap_or_default() ^ b.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

#[cfg(windows)]
fn open_in_browser(url: &str) -> Result<(), ()> {
    use std::ffi::c_void;
    #[link(name = "shell32")]
    unsafe extern "system" {
        fn ShellExecuteW(
            window: *mut c_void,
            operation: *const u16,
            file: *const u16,
            parameters: *const u16,
            directory: *const u16,
            show: i32,
        ) -> *mut c_void;
    }
    let operation: Vec<u16> = "open\0".encode_utf16().collect();
    let target: Vec<u16> = format!("{url}\0").encode_utf16().collect();
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            operation.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
        )
    } as isize;
    (result > 32).then_some(()).ok_or(())
}

#[cfg(not(windows))]
fn open_in_browser(url: &str) -> Result<(), ()> {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    Command::new(program)
        .arg(url)
        .spawn()
        .map(|_| ())
        .map_err(|_| ())
}

fn grant_path(home: &Path) -> PathBuf {
    home.join(GRANT_FILE)
}

fn load_grant(home: &Path) -> Option<AccountGrant> {
    let protected = std::fs::read(grant_path(home)).ok()?;
    let clear = secure_storage::unprotect(&protected).ok()?;
    let grant: AccountGrant = serde_json::from_slice(&clear).ok()?;
    (grant.version == 1 && grant.issuer == PLATFORM_ORIGIN && !grant.token.is_empty())
        .then_some(grant)
}

fn store_grant(home: &Path, grant: &AccountGrant) -> Result<(), ()> {
    std::fs::create_dir_all(home).map_err(|_| ())?;
    let clear = serde_json::to_vec(grant).map_err(|_| ())?;
    let protected = secure_storage::protect(&clear)?;
    let path = grant_path(home);
    let temporary = home.join(format!(
        ".account-grant-{}-{}.tmp",
        std::process::id(),
        random_uuid()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|_| ())?;
    use std::io::Write;
    if file.write_all(&protected).is_err() || file.sync_all().is_err() {
        let _ = std::fs::remove_file(&temporary);
        return Err(());
    }
    drop(file);
    if path.exists() {
        std::fs::remove_file(&path).map_err(|_| ())?;
    }
    std::fs::rename(&temporary, &path).map_err(|_| {
        let _ = std::fs::remove_file(&temporary);
        ()
    })
}

fn remove_grant(home: &Path) -> Result<(), ()> {
    match std::fs::remove_file(grant_path(home)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(()),
    }
}

#[cfg(windows)]
mod secure_storage {
    use std::ffi::c_void;
    use std::ptr;

    #[repr(C)]
    struct DataBlob {
        size: u32,
        data: *mut u8,
    }

    const UI_FORBIDDEN: u32 = 0x1;
    const ENTROPY: &str = "DeepSeek Harness account grant v1\0";

    #[link(name = "crypt32")]
    unsafe extern "system" {
        fn CryptProtectData(
            input: *const DataBlob,
            description: *const u16,
            entropy: *const DataBlob,
            reserved: *mut c_void,
            prompt: *const c_void,
            flags: u32,
            output: *mut DataBlob,
        ) -> i32;
        fn CryptUnprotectData(
            input: *const DataBlob,
            description: *mut *mut u16,
            entropy: *const DataBlob,
            reserved: *mut c_void,
            prompt: *const c_void,
            flags: u32,
            output: *mut DataBlob,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
    }

    fn entropy_blob(entropy: &mut [u16]) -> DataBlob {
        DataBlob {
            size: (entropy.len() * std::mem::size_of::<u16>()) as u32,
            data: entropy.as_mut_ptr().cast(),
        }
    }

    unsafe fn copy_and_free(blob: DataBlob) -> Result<Vec<u8>, ()> {
        if blob.data.is_null() || blob.size == 0 {
            return Err(());
        }
        let copy = std::slice::from_raw_parts(blob.data, blob.size as usize).to_vec();
        let _ = LocalFree(blob.data.cast());
        Ok(copy)
    }

    pub(super) fn protect(clear: &[u8]) -> Result<Vec<u8>, ()> {
        let data = DataBlob {
            size: u32::try_from(clear.len()).map_err(|_| ())?,
            data: clear.as_ptr().cast_mut(),
        };
        let mut entropy: Vec<u16> = ENTROPY.encode_utf16().collect();
        let entropy = entropy_blob(&mut entropy);
        let mut output = DataBlob {
            size: 0,
            data: ptr::null_mut(),
        };
        let ok = unsafe {
            CryptProtectData(
                &data,
                ptr::null(),
                &entropy,
                ptr::null_mut(),
                ptr::null(),
                UI_FORBIDDEN,
                &mut output,
            )
        };
        if ok == 0 {
            return Err(());
        }
        unsafe { copy_and_free(output) }
    }

    pub(super) fn unprotect(protected: &[u8]) -> Result<Vec<u8>, ()> {
        let data = DataBlob {
            size: u32::try_from(protected.len()).map_err(|_| ())?,
            data: protected.as_ptr().cast_mut(),
        };
        let mut entropy: Vec<u16> = ENTROPY.encode_utf16().collect();
        let entropy = entropy_blob(&mut entropy);
        let mut output = DataBlob {
            size: 0,
            data: ptr::null_mut(),
        };
        let ok = unsafe {
            CryptUnprotectData(
                &data,
                ptr::null_mut(),
                &entropy,
                ptr::null_mut(),
                ptr::null(),
                UI_FORBIDDEN,
                &mut output,
            )
        };
        if ok == 0 {
            return Err(());
        }
        unsafe { copy_and_free(output) }
    }
}

#[cfg(not(windows))]
mod secure_storage {
    pub(super) fn protect(_clear: &[u8]) -> Result<Vec<u8>, ()> {
        Err(())
    }
    pub(super) fn unprotect(_protected: &[u8]) -> Result<Vec<u8>, ()> {
        Err(())
    }
}
