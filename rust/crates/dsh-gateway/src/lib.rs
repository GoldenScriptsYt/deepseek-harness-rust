//! Web gateway. Port of `packages/api/gateway`, `host/authentication-token` and the static
//! parts of `host/webserver`: JSON RPC under `/api/<method>`, one WebSocket mux carrying
//! logical streams at `/api/remote.mux`, launch-token sign-in, and static file serving.
//!
//! Wire formats are those documented in `api/gateway/src/stream-protocol.ts` and observed
//! from the TypeScript Host.

use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream::BoxStream;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};

pub mod shell;

pub const MUX_PATH: &str = "/api/remote.mux";
pub const EVENTS_ENDPOINT: &str = "$events";
const COOKIE_NAME: &str = "dsh-auth-rs";
const COOKIE_MAX_AGE_SECS: u64 = 30 * 24 * 60 * 60;

/// A failure returned to the client in the RPC result.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
    pub code: String,
    pub message: String,
    pub details: Value,
}

impl RpcError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        RpcError {
            code: code.into(),
            message: message.into(),
            details: json!({}),
        }
    }
}

type RpcFuture = Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>;
type RpcFn = Arc<dyn Fn(Value) -> RpcFuture + Send + Sync>;
type RawPostFuture = Pin<Box<dyn Future<Output = Value> + Send>>;
type RawPostFn = Arc<dyn Fn(RawPostRequest) -> RawPostFuture + Send + Sync>;
type RawGetFuture = Pin<Box<dyn Future<Output = RawGetResponse> + Send>>;
type RawGetFn = Arc<dyn Fn(RawGetRequest) -> RawGetFuture + Send + Sync>;
/// Opens a server stream: receives the open payload and the client's uplink items.
type StreamFn = Arc<
    dyn Fn(Value, mpsc::Receiver<Value>) -> BoxStream<'static, Result<Value, RpcError>>
        + Send
        + Sync,
>;
type UnhandledRpc = Arc<dyn Fn(&str, &str) + Send + Sync>;

/// Mutable route tables, shared so features can register after the server starts.
#[derive(Default)]
struct Routes {
    rpc: HashMap<String, RpcFn>,
    raw_posts: HashMap<String, RawPostRoute>,
    raw_gets: HashMap<String, RawGetFn>,
    streams: HashMap<String, StreamFn>,
}

#[derive(Clone)]
struct RawPostRoute {
    max_body_bytes: usize,
    handler: RawPostFn,
}

/// Authenticated byte request delivered to one registered raw POST route.
pub struct RawPostRequest {
    pub query: String,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

/// Query string delivered to one narrow unauthenticated loopback GET route.
pub struct RawGetRequest {
    pub query: String,
}

/// Bounded response from a registered unauthenticated loopback GET route.
pub struct RawGetResponse {
    pub status: u16,
    pub location: Option<String>,
    pub body: String,
}

struct Shared {
    token: String,
    home: String,
    static_root: Option<PathBuf>,
    web: Mutex<Option<Arc<WebShell>>>,
    unhandled: Mutex<Option<UnhandledRpc>>,
    routes: Mutex<Routes>,
    sessions: Mutex<HashSet<String>>,
    topics: Mutex<HashMap<String, Broadcaster>>,
}

impl Shared {
    fn event_broadcaster(&self) -> Broadcaster {
        self.topics
            .lock()
            .unwrap()
            .get(EVENTS_ENDPOINT)
            .expect("the $events topic is initialized with the gateway")
            .clone()
    }
}

/// Frames a subscriber may fall behind by before the oldest are dropped for it.
const BROADCAST_CAPACITY: usize = 256;

/// Live frames for one stream endpoint. Every open stream of that endpoint receives
/// each published frame; a subscriber that falls further than [`BROADCAST_CAPACITY`]
/// behind loses the oldest frames instead of blocking the publisher.
#[derive(Clone)]
pub struct Broadcaster(broadcast::Sender<Value>);

impl Default for Broadcaster {
    fn default() -> Self {
        Self(broadcast::channel(BROADCAST_CAPACITY).0)
    }
}

impl Broadcaster {
    /// Deliver one frame to every open stream of this endpoint.
    pub fn publish(&self, frame: Value) {
        // No subscriber is not an error: frames published while nobody listens are dropped.
        let _ = self.0.send(frame);
    }

    /// Join the live frames published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.0.subscribe()
    }
}

/// Rendered page plus the packages whose `lib/client.js` the plugin route serves.
struct WebShell {
    dist: PathBuf,
    boot: Value,
    index: String,
    packages: HashMap<String, PathBuf>,
}

/// Cloneable handle for registering endpoints and building the router.
#[derive(Clone)]
pub struct Gateway {
    shared: Arc<Shared>,
}

impl Gateway {
    pub fn new(
        token: impl Into<String>,
        home: impl Into<String>,
        static_root: Option<PathBuf>,
    ) -> Self {
        Gateway {
            shared: Arc::new(Shared {
                token: token.into(),
                home: home.into(),
                static_root,
                web: Mutex::new(None),
                unhandled: Mutex::new(None),
                topics: Mutex::new(HashMap::from([(
                    EVENTS_ENDPOINT.to_string(),
                    Broadcaster(broadcast::channel(BROADCAST_CAPACITY).0),
                )])),
                routes: Mutex::default(),
                sessions: Mutex::default(),
            }),
        }
    }

    /// Observe calls to methods nobody registered (method, raw body). They still answer 404.
    pub fn on_unhandled(&self, f: impl Fn(&str, &str) + Send + Sync + 'static) {
        *self.shared.unhandled.lock().unwrap() = Some(Arc::new(f));
    }

    /// Register a unary RPC. `method` is the path after `/api/`, e.g. `session/list`.
    pub fn rpc<F, Fut>(&self, method: &str, f: F)
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, RpcError>> + Send + 'static,
    {
        self.shared
            .routes
            .lock()
            .unwrap()
            .rpc
            .insert(method.into(), Arc::new(move |v| Box::pin(f(v))));
    }

    /// Register an authenticated raw-body POST route beneath `/api`.
    ///
    /// The body is collected only up to `max_body_bytes`; oversized requests
    /// return HTTP 413 before reaching the feature handler.
    pub fn raw_post<F, Fut>(&self, path: &str, max_body_bytes: usize, f: F)
    where
        F: Fn(RawPostRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Value> + Send + 'static,
    {
        self.shared.routes.lock().unwrap().raw_posts.insert(
            path.to_owned(),
            RawPostRoute {
                max_body_bytes,
                handler: Arc::new(move |request| Box::pin(f(request))),
            },
        );
    }

    /// Register one narrow unauthenticated GET route, intended for loopback OAuth callbacks.
    /// Every request is still bound to the gateway's loopback listener and must be validated by its handler.
    pub fn raw_get<F, Fut>(&self, path: &str, f: F)
    where
        F: Fn(RawGetRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = RawGetResponse> + Send + 'static,
    {
        self.shared.routes.lock().unwrap().raw_gets.insert(
            path.to_owned(),
            Arc::new(move |request| Box::pin(f(request))),
        );
    }

    /// The broadcaster for one stream endpoint, created on first use.
    pub fn broadcaster(&self, endpoint: &str) -> Broadcaster {
        let mut topics = self.shared.topics.lock().unwrap();
        topics
            .entry(endpoint.to_string())
            .or_insert_with(|| Broadcaster(broadcast::channel(BROADCAST_CAPACITY).0))
            .clone()
    }

    /// Return the persistent publisher for `$events` downlink items.
    ///
    /// Publish complete `emit`, `waterfall`, or `cancel` values from
    /// `RemoteEventDownlinkFrame`; the Gateway owns each opening `ready` value.
    /// Values published without subscribers are dropped. A subscriber that
    /// exceeds the bounded queue loses its oldest values and keeps the remaining
    /// values in publication order.
    pub fn event_broadcaster(&self) -> Broadcaster {
        self.shared.event_broadcaster()
    }

    /// Register a server stream endpoint carried over the mux socket.
    pub fn stream<F>(&self, endpoint: &str, f: F)
    where
        F: Fn(Value, mpsc::Receiver<Value>) -> BoxStream<'static, Result<Value, RpcError>>
            + Send
            + Sync
            + 'static,
    {
        self.shared
            .routes
            .lock()
            .unwrap()
            .streams
            .insert(endpoint.into(), Arc::new(f));
    }

    /// Serve the web client from a checkout: `apps/web/dist` as static files, a rendered
    /// index with the boot graph, and `/plugins/` bundles built from each `lib/client.js`.
    pub fn with_web_shell(&self, repo: &Path) -> std::io::Result<()> {
        let dist = repo.join("apps/web/dist");
        let template = std::fs::read_to_string(dist.join("index.html"))?;
        let packages = shell::scan_packages(repo);
        let captured: Value =
            serde_json::from_str(shell::CAPTURED_BOOT).map_err(std::io::Error::other)?;
        let boot = shell::build_boot(&captured, &packages);
        let index = shell::render_index(&template, &boot);
        *self.shared.web.lock().unwrap() = Some(Arc::new(WebShell {
            dist,
            boot,
            index,
            packages,
        }));
        Ok(())
    }

    /// The boot graph the page was rendered with, once the web shell is set up.
    pub fn boot_graph(&self) -> Option<Value> {
        self.shared
            .web
            .lock()
            .unwrap()
            .as_ref()
            .map(|w| w.boot.clone())
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/plugins/", get(plugins))
            .route("/plugins/{*rest}", get(plugin_asset))
            .route(MUX_PATH, get(mux))
            .route("/api/session/uploadFileBinary", post(raw_upload))
            .route("/api/{*method}", post(rpc))
            .route("/oauth/callback", get(raw_get))
            .route("/", get(index))
            .route("/{*path}", get(static_file))
            .with_state(self.shared.clone())
    }

    /// Bind `addr` and serve until the future is dropped. Returns the bound address first.
    pub async fn bind(
        &self,
        addr: &str,
    ) -> std::io::Result<(
        std::net::SocketAddr,
        impl Future<Output = std::io::Result<()>>,
    )> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        let app = self.router();
        Ok((local, async move { axum::serve(listener, app).await }))
    }

    /// The URL a browser opens once to sign in.
    pub fn entry_url(&self, origin: &str) -> String {
        format!("{origin}/?token={}", self.shared.token)
    }
}

async fn raw_upload(State(s): State<Arc<Shared>>, request: Request<Body>) -> Response {
    if !authenticated(&s, request.headers()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let path = request.uri().path().to_owned();
    let route = s.routes.lock().unwrap().raw_posts.get(&path).cloned();
    let Some(route) = route else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let query = request.uri().query().unwrap_or_default().to_owned();
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = match to_bytes(request.into_body(), route.max_body_bytes).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body exceeds the upload limit",
            )
                .into_response()
        }
    };
    let value = (route.handler)(RawPostRequest {
        query,
        content_type,
        body: body.to_vec(),
    })
    .await;
    axum::Json(value).into_response()
}

async fn raw_get(State(s): State<Arc<Shared>>, request: Request<Body>) -> Response {
    let path = request.uri().path().to_owned();
    let handler = s.routes.lock().unwrap().raw_gets.get(&path).cloned();
    let Some(handler) = handler else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let response = handler(RawGetRequest {
        query: request.uri().query().unwrap_or_default().to_owned(),
    })
    .await;
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8");
    if let Some(location) = response.location {
        let Ok(location) = HeaderValue::from_str(&location) else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid callback response",
            )
                .into_response();
        };
        builder = builder.header(header::LOCATION, location);
    }
    builder.body(Body::from(response.body)).unwrap_or_else(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "callback response failed",
        )
            .into_response()
    })
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

fn random_id() -> String {
    // 256 bits from the OS-seeded hasher; adequate for a loopback session cookie.
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::new();
    for _ in 0..4 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out
}

fn cookie_value(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|p| {
        let (k, v) = p.trim().split_once('=')?;
        (k == COOKIE_NAME).then(|| v.to_string())
    })
}

fn authenticated(shared: &Shared, headers: &HeaderMap) -> bool {
    cookie_value(headers).is_some_and(|c| shared.sessions.lock().unwrap().contains(&c))
}

async fn index(
    State(s): State<Arc<Shared>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Some(token) = q.get("token") {
        if constant_time_eq(token, &s.token) {
            let id = random_id();
            s.sessions.lock().unwrap().insert(id.clone());
            let cookie = format!("{COOKIE_NAME}={id}; Max-Age={COOKIE_MAX_AGE_SECS}; Path=/; HttpOnly; SameSite=Strict");
            return (
                StatusCode::SEE_OTHER,
                [
                    (header::LOCATION, "./".to_string()),
                    (header::SET_COOKIE, cookie),
                ],
            )
                .into_response();
        }
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    if !authenticated(&s, &headers) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let web = s.web.lock().unwrap().clone();
    if let Some(web) = web {
        let mut r = Response::new(Body::from(web.index.clone()));
        r.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        return r;
    }
    serve_file(&s, "index.html").await
}

/// Split `<package>/assets/<path>` where the package may be scoped (`@scope/name`).
pub fn parse_asset_path(rest: &str) -> Option<(&str, &str)> {
    let split = if rest.starts_with('@') {
        let first = rest.find('/')?;
        rest[first + 1..].find('/')? + first + 1
    } else {
        rest.find('/')?
    };
    let (package, tail) = rest.split_at(split);
    let path = tail.strip_prefix("/assets/")?;
    (!package.is_empty() && !path.is_empty()).then_some((package, path))
}

/// Walk up from a package's client file to the directory holding its `package.json`.
fn package_dir(client_file: &Path) -> Option<PathBuf> {
    client_file
        .ancestors()
        .skip(1)
        .find(|d| d.join("package.json").is_file())
        .map(Path::to_path_buf)
}

async fn plugin_asset(
    State(s): State<Arc<Shared>>,
    headers: HeaderMap,
    UrlPath(rest): UrlPath<String>,
) -> Response {
    if !authenticated(&s, &headers) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let not_found = || (StatusCode::NOT_FOUND, "not found").into_response();
    let web = s.web.lock().unwrap().clone();
    let (Some(web), Some((package, path))) = (web, parse_asset_path(&rest)) else {
        return not_found();
    };
    let (Some(file), Some(rel)) = (web.packages.get(package), safe_relative(path)) else {
        return not_found();
    };
    let Some(dir) = package_dir(file) else {
        return not_found();
    };
    match tokio::fs::read(dir.join("lib/assets").join(rel)).await {
        Ok(bytes) => {
            let mut r = Response::new(Body::from(bytes));
            r.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(content_type(path)),
            );
            r.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            );
            r
        }
        Err(_) => not_found(),
    }
}

async fn plugins(
    State(s): State<Arc<Shared>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    if !authenticated(&s, &headers) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let web = s.web.lock().unwrap().clone();
    // The combo URL is `/plugins/??a/client.js&rev=x`: the query string itself begins with `?`.
    let raw = uri.query().unwrap_or_default().to_string();
    let (Some(web), Some((ids, rev))) = (web, shell::parse_combo(&raw)) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let mut sources = Vec::new();
    for id in &ids {
        let Some(file) = web.packages.get(id) else {
            return (StatusCode::NOT_FOUND, "not found").into_response();
        };
        match tokio::fs::read_to_string(file).await {
            Ok(text) => sources.push(text),
            Err(_) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        }
    }
    let list: Vec<String> = ids.iter().map(|i| format!("{i}/client.js")).collect();
    let map_url = format!(
        "??{}.map{}",
        list.join(","),
        rev.map(|r| format!("&rev={r}")).unwrap_or_default()
    );
    let mut r = Response::new(Body::from(shell::combo_script(&sources, &map_url)));
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/javascript; charset=utf-8"),
    );
    r.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    r
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "wasm" => "application/wasm",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Reject traversal: every component must be a plain name.
fn safe_relative(rel: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for part in rel.split('/') {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.contains('\\')
            || part.contains(':')
        {
            return None;
        }
        out.push(part);
    }
    Some(out)
}

async fn serve_file(s: &Shared, rel: &str) -> Response {
    let web_dist = s.web.lock().unwrap().as_ref().map(|w| w.dist.clone());
    let root = web_dist.or_else(|| s.static_root.clone());
    let (Some(root), Some(rel)) = (root, safe_relative(rel)) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let full = root.join(&rel);
    match tokio::fs::read(&full).await {
        Ok(bytes) => {
            let ct = content_type(&full.to_string_lossy());
            let mut r = Response::new(Body::from(bytes));
            r.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static(ct));
            r
        }
        Err(_) => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn static_file(
    State(s): State<Arc<Shared>>,
    headers: HeaderMap,
    UrlPath(path): UrlPath<String>,
) -> Response {
    if !authenticated(&s, &headers) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    serve_file(&s, &path).await
}

fn error_value(e: &RpcError) -> Value {
    json!({"code": e.code, "message": e.message, "details": e.details})
}

async fn rpc(
    State(s): State<Arc<Shared>>,
    headers: HeaderMap,
    UrlPath(method): UrlPath<String>,
    body: String,
) -> Response {
    if !authenticated(&s, &headers) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let handler = s.routes.lock().unwrap().rpc.get(&method).cloned();
    let Some(handler) = handler else {
        let hook = s.unhandled.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(&method, &body);
        }
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let respond = |rpc_id: &str, result: Value| {
        axum::Json(json!({"type":"server-response","rpcId":rpc_id,"result":result})).into_response()
    };
    let bad = |why: &str| {
        respond(
            "invalid-request",
            json!({"ok":false,"error":{"code":"gateway/bad-request","message":"invalid client-request message","details":{"reason":why}}}),
        )
    };

    let Ok(msg) = serde_json::from_str::<Value>(&body) else {
        return bad("not JSON");
    };
    let valid = msg.get("type") == Some(&json!("client-request"))
        && msg.get("rpcId").is_some_and(Value::is_string)
        && msg.get("method").is_some_and(Value::is_string)
        && msg.get("payload").is_some();
    if !valid {
        return bad("missing type, rpcId, method or payload");
    }
    let rpc_id = msg["rpcId"].as_str().unwrap_or_default().to_string();
    if msg["method"].as_str() != Some(method.as_str()) {
        return bad("method does not match the request path");
    }
    let result = match handler(msg["payload"].clone()).await {
        Ok(v) => json!({"ok":true,"value":v}),
        Err(e) => json!({"ok":false,"error":error_value(&e)}),
    };
    respond(&rpc_id, result)
}

async fn mux(State(s): State<Arc<Shared>>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if !authenticated(&s, &headers) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    ws.on_upgrade(move |socket| run_mux(socket, s))
}

fn exact_keys(m: &Map<String, Value>, keys: &[&str]) -> bool {
    m.len() == keys.len() && keys.iter().all(|k| m.contains_key(*k))
}

fn valid_id(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// One decoded browser-to-Host mux message.
#[derive(Debug, PartialEq)]
pub enum ClientMessage {
    Open {
        stream_id: String,
        endpoint: String,
        payload: Value,
    },
    Item {
        stream_id: String,
        value: Option<Value>,
    },
    End {
        stream_id: String,
    },
    Cancel {
        stream_id: String,
    },
}

/// Parse and validate one mux text message, mirroring `parseRemoteStreamClientMessage`.
pub fn parse_client_message(text: &str) -> Option<ClientMessage> {
    let v: Value = serde_json::from_str(text).ok()?;
    let m = v.as_object()?;
    let id = valid_id(m.get("streamId"))?;
    match m.get("type")?.as_str()? {
        "cancel" if exact_keys(m, &["type", "streamId"]) => {
            Some(ClientMessage::Cancel { stream_id: id })
        }
        "end" if exact_keys(m, &["type", "streamId"]) => Some(ClientMessage::End { stream_id: id }),
        "item" if exact_keys(m, &["type", "streamId"]) => Some(ClientMessage::Item {
            stream_id: id,
            value: None,
        }),
        "item" if exact_keys(m, &["type", "streamId", "value"]) => Some(ClientMessage::Item {
            stream_id: id,
            value: m.get("value").cloned(),
        }),
        "open" if exact_keys(m, &["type", "streamId", "endpoint", "payload"]) => {
            let endpoint = m
                .get("endpoint")?
                .as_str()
                .filter(|e| !e.is_empty())?
                .to_string();
            Some(ClientMessage::Open {
                stream_id: id,
                endpoint,
                payload: m.get("payload")?.clone(),
            })
        }
        _ => None,
    }
}

struct Live {
    /// `None` for Gateway-owned streams, whose uplink items are discarded.
    uplink: Option<mpsc::Sender<Value>>,
    task: tokio::task::JoinHandle<()>,
}

async fn run_event_stream(
    stream_id: String,
    home: String,
    mut events: broadcast::Receiver<Value>,
    out: mpsc::Sender<String>,
) {
    let ready = json!({"type":"item","streamId":stream_id,"value":{"type":"ready","clientId":random_id(),"host":{"home":home}}});
    if out.send(ready.to_string()).await.is_err() {
        return;
    }
    loop {
        match events.recv().await {
            Ok(value) => {
                let message = json!({"type":"item","streamId":stream_id,"value":value});
                if out.send(message.to_string()).await.is_err() {
                    return;
                }
            }
            // Tokio has already discarded the oldest queued values; the tail stays FIFO.
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => {
                let _ = out
                    .send(json!({"type":"end","streamId":stream_id}).to_string())
                    .await;
                return;
            }
        }
    }
}

async fn run_mux(socket: WebSocket, s: Arc<Shared>) {
    let (mut sink, mut source) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
    let writer = tokio::spawn(async move {
        while let Some(t) = out_rx.recv().await {
            if sink.send(Message::Text(t.into())).await.is_err() {
                break;
            }
        }
    });
    let mut live: HashMap<String, Live> = HashMap::new();

    while let Some(Ok(frame)) = source.next().await {
        let Message::Text(text) = frame else {
            if matches!(frame, Message::Close(_)) {
                break;
            }
            continue;
        };
        match parse_client_message(&text) {
            None => break, // protocol violation closes the connection
            Some(ClientMessage::Open {
                stream_id,
                endpoint,
                payload,
            }) => {
                if live.contains_key(&stream_id) {
                    break;
                }
                let out = out_tx.clone();
                let sid = stream_id.clone();
                if endpoint == EVENTS_ENDPOINT {
                    // Subscribe before ready can reach the socket so publications during
                    // opening remain queued for this subscriber.
                    let events = s.event_broadcaster().subscribe();
                    let home = s.home.clone();
                    let task = tokio::spawn(async move {
                        run_event_stream(sid, home, events, out).await;
                    });
                    live.insert(stream_id, Live { uplink: None, task });
                    continue;
                }
                let handler = s.routes.lock().unwrap().streams.get(&endpoint).cloned();
                let Some(handler) = handler else {
                    let hook = s.unhandled.lock().unwrap().clone();
                    if let Some(hook) = hook {
                        hook(&format!("stream {endpoint}"), &payload.to_string());
                    }
                    let e = json!({"type":"error","streamId":sid,"error":{"code":"gateway/invocation-unavailable","message":format!("no stream endpoint {endpoint}"),"details":{}}});
                    let _ = out.send(e.to_string()).await;
                    continue;
                };
                let (up_tx, up_rx) = mpsc::channel(64);
                let mut items = handler(payload, up_rx);
                let task = tokio::spawn(async move {
                    while let Some(item) = items.next().await {
                        let msg = match item {
                            Ok(v) => json!({"type":"item","streamId":sid,"value":v}),
                            Err(e) => {
                                let _ = out.send(json!({"type":"error","streamId":sid,"error":error_value(&e)}).to_string()).await;
                                return;
                            }
                        };
                        if out.send(msg.to_string()).await.is_err() {
                            return;
                        }
                    }
                    let _ = out
                        .send(json!({"type":"end","streamId":sid}).to_string())
                        .await;
                });
                live.insert(
                    stream_id,
                    Live {
                        uplink: Some(up_tx),
                        task,
                    },
                );
            }
            Some(ClientMessage::Item { stream_id, value }) => {
                if let Some(l) = live.get(&stream_id) {
                    let Some(uplink) = &l.uplink else {
                        continue;
                    };
                    if uplink.try_send(value.unwrap_or(Value::Null)).is_err() {
                        // Uplink overflow fails that stream only.
                        if let Some(l) = live.remove(&stream_id) {
                            l.task.abort();
                        }
                        let e = json!({"type":"error","streamId":stream_id,"error":{"code":"gateway/uplink-overflow","message":"uplink buffer full","details":{}}});
                        let _ = out_tx.send(e.to_string()).await;
                    }
                }
            }
            Some(ClientMessage::End { stream_id }) => {
                // Half-close: dropping the sender ends the handler's uplink.
                if let Some(l) = live.get_mut(&stream_id) {
                    if let Some(uplink) = &mut l.uplink {
                        let (dead, _) = mpsc::channel(1);
                        *uplink = dead;
                    }
                }
            }
            Some(ClientMessage::Cancel { stream_id }) => {
                if let Some(l) = live.remove(&stream_id) {
                    l.task.abort();
                }
            }
        }
    }
    for (_, l) in live {
        l.task.abort();
    }
    writer.abort();
}

/// Directory holding the built web client, if the checkout has one.
pub fn default_static_root(repo: &Path) -> Option<PathBuf> {
    let dist = repo.join("apps/web/dist");
    dist.join("index.html").is_file().then_some(dist)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mux_messages_strictly() {
        assert_eq!(
            parse_client_message(
                r#"{"type":"open","streamId":"1","endpoint":"$events","payload":{"args":{}}}"#
            ),
            Some(ClientMessage::Open {
                stream_id: "1".into(),
                endpoint: "$events".into(),
                payload: json!({"args":{}})
            })
        );
        assert_eq!(
            parse_client_message(r#"{"type":"item","streamId":"1"}"#),
            Some(ClientMessage::Item {
                stream_id: "1".into(),
                value: None
            })
        );
        assert_eq!(
            parse_client_message(r#"{"type":"cancel","streamId":"1"}"#),
            Some(ClientMessage::Cancel {
                stream_id: "1".into()
            })
        );
        assert!(parse_client_message(r#"{"type":"cancel","streamId":""}"#).is_none());
        assert!(parse_client_message(r#"{"type":"cancel","streamId":"1","extra":1}"#).is_none());
        assert!(parse_client_message(
            r#"{"type":"open","streamId":"1","endpoint":"","payload":1}"#
        )
        .is_none());
        assert!(parse_client_message("nope").is_none());
    }

    #[tokio::test]
    async fn event_stream_orders_ready_before_backlog_and_preserves_tail_after_lag() {
        let (sender, _receiver) = broadcast::channel(2);
        let events = Broadcaster(sender);
        let subscribed = events.subscribe();
        events.publish(json!({"type":"emit","event":"fixture/changed","args":[0]}));
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let task = tokio::spawn(run_event_stream(
            "events".into(),
            "/fixture".into(),
            subscribed,
            out_tx,
        ));

        let ready = next_json(&mut out_rx).await;
        assert_eq!(ready["type"], "item");
        assert_eq!(ready["streamId"], "events");
        assert_eq!(ready["value"]["type"], "ready");
        assert_eq!(ready["value"]["host"]["home"], "/fixture");
        assert_eq!(next_json(&mut out_rx).await["value"]["args"], json!([0]));

        for value in 1..=3 {
            events.publish(json!({"type":"emit","event":"fixture/changed","args":[value]}));
        }
        assert_eq!(next_json(&mut out_rx).await["value"]["args"], json!([2]));
        assert_eq!(next_json(&mut out_rx).await["value"]["args"], json!([3]));

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    async fn next_json(receiver: &mut mpsc::Receiver<String>) -> Value {
        let text = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
            .await
            .expect("event stream produced a frame")
            .expect("event stream output stayed open");
        serde_json::from_str(&text).unwrap()
    }

    #[test]
    fn asset_paths_split_scoped_and_plain_packages() {
        assert_eq!(
            parse_asset_path("@deepseek-ai/dsh-x/assets/icons/a.png"),
            Some(("@deepseek-ai/dsh-x", "icons/a.png"))
        );
        assert_eq!(
            parse_asset_path("plain/assets/b.svg"),
            Some(("plain", "b.svg"))
        );
        assert_eq!(parse_asset_path("@scope/assets/chunk.js"), None);
        assert_eq!(parse_asset_path("@scope/name/assets/"), None);
        assert_eq!(parse_asset_path("nothing"), None);
    }

    #[test]
    fn path_traversal_is_rejected() {
        assert!(safe_relative("assets/a.js").is_some());
        assert!(safe_relative("../secret").is_none());
        assert!(safe_relative("a/../b").is_none());
        assert!(safe_relative("a\\b").is_none());
        assert!(safe_relative("C:/x").is_none());
    }

    #[test]
    fn constant_time_compare() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
    }
}
