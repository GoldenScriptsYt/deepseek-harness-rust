//! RPC endpoints of the web client. Static catalogs are the TypeScript Host's own answers,
//! captured once in `assets/rpc/`; the rest are implemented here as they are ported.

use crate::settings::SettingsStore;
use base64::Engine;
use dsh_attachments::{
    AdmittedPromptContentPart, AttachmentError, AttachmentLimits, AttachmentStore,
    ImageAttachmentRef as StoredImageRef, ImageDimensions as StoredImageDimensions,
    ImageMediaType as StoredImageMediaType, PromptContentPart, SaveImageAttachment,
    StagedAttachment, DEFAULT_MAX_FILE_BYTES,
};
use dsh_core::{
    Agent, AgentEvent, Cancel, InboxState, InboxTarget, PromptRequest as CorePromptRequest,
    SessionAssistantStreamAccumulator, SessionAssistantStreamFrame, TurnEnd, UserInput,
};
use dsh_gateway::{Broadcaster, Gateway, RawPostRequest, RpcError};
use dsh_llm::Content;
use dsh_llm_deepseek::serialize::Effort;
use dsh_session_format::{Event, Header, HeaderTag, SESSION_FORMAT_VERSION};
use dsh_session_persistence::{Session, Store, StoreError, Stored};
use futures_util::stream::BoxStream;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

const FILE_UPLOAD_PATH: &str = "/api/session/uploadFileBinary";

/// What the endpoints need to know about the running Host.
pub struct ApiContext {
    /// `http://127.0.0.1:<port>`.
    pub origin: String,
    pub token: String,
    pub home: PathBuf,
    pub(crate) account: crate::account::AccountController,
    workspaces: Mutex<()>,
    settings: SettingsStore,
    /// Live `session/follow` frames; attached by `register_all` once the gateway exists.
    follow: Mutex<Option<Broadcaster>>,
    /// Host-wide projection changes; attached by `register_all` once the gateway exists.
    control: Mutex<Option<Broadcaster>>,
    /// Cordis-compatible Host events carried by `$events`.
    events: Mutex<Option<Broadcaster>>,
    /// Reconnect baselines for Assistant streams owned by live Agents.
    assistant_streams: Mutex<HashMap<String, SessionAssistantStreamAccumulator>>,
    /// Sessions whose turn is running, with the handle that cancels it.
    running: Mutex<HashMap<String, Cancel>>,
    /// Resident Agents retain their durable inbox between turns.
    agents: Mutex<HashMap<String, Arc<AsyncMutex<Agent>>>>,
    /// Serialize compare-and-set message feedback mutations.
    feedback_write: AsyncMutex<()>,
    /// Process-local Goal continuation activation: Goal id, revision, and armed flag.
    goal_activation: GoalActivation,
    /// Plan-mode selections awaiting the next turn boundary.
    plan_pending: Mutex<HashMap<String, PendingPlan>>,
    /// Connected MCP servers, connected once on first use.
    mcp: AsyncMutex<Option<Arc<dsh_mcp::McpHost>>>,
}

/// A plan-mode selection whose command rows await the turn boundary.
#[derive(Debug, Clone)]
pub(crate) struct PendingPlan {
    pub(crate) active: bool,
    pub(crate) command_id: String,
    pub(crate) args: String,
    pub(crate) text: String,
}

impl ApiContext {
    pub fn new(
        origin: impl Into<String>,
        token: impl Into<String>,
        home: impl Into<PathBuf>,
    ) -> Arc<Self> {
        let home = home.into();
        let catalog = captured(include_str!("../assets/rpc/settings_describe.json"));
        let imported = crate::model_routes::legacy_profile_defaults(&home);
        let settings = SettingsStore::open_with_defaults(
            PathBuf::from(&home).join("settings.rust.json"),
            &catalog,
            &imported,
        );
        let account_updates = Broadcaster::default();
        let account = crate::account::AccountController::new(&home, account_updates);
        Arc::new(ApiContext {
            origin: origin.into(),
            token: token.into(),
            home,
            account,
            workspaces: Mutex::new(()),
            settings,
            follow: Mutex::new(None),
            control: Mutex::new(None),
            events: Mutex::new(None),
            assistant_streams: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            agents: Mutex::new(HashMap::new()),
            feedback_write: AsyncMutex::new(()),
            goal_activation: Arc::new(Mutex::new(HashMap::new())),
            plan_pending: Mutex::new(HashMap::new()),
            mcp: AsyncMutex::new(None),
        })
    }

    /// Connected MCP servers from `DSH_MCP_SERVERS`.
    ///
    /// The servers connect once per Host process and stay connected for its
    /// lifetime; a configuration failure starts the Host without them and is
    /// reported once on stderr.
    /// @returns The connected host, or `None` when no server is configured.
    pub(crate) async fn mcp_host(&self) -> Option<Arc<dsh_mcp::McpHost>> {
        let mut slot = self.mcp.lock().await;
        if let Some(host) = slot.as_ref() {
            return Some(Arc::clone(host));
        }
        let specs = match dsh_mcp::specs_from_env() {
            Ok(Some(specs)) => specs,
            Ok(None) => return None,
            Err(error) => {
                eprintln!("dsh: MCP servers disabled: {error}");
                return None;
            }
        };
        match dsh_mcp::McpHost::connect(specs).await {
            Ok(host) => {
                for failure in host.failures() {
                    eprintln!("dsh: {failure}");
                }
                let host = Arc::new(host);
                *slot = Some(Arc::clone(&host));
                Some(host)
            }
            Err(error) => {
                eprintln!("dsh: MCP servers disabled: {error}");
                None
            }
        }
    }

    /// Publish Session events to the streams following this Host.
    pub fn attach_follow(&self, broadcaster: Broadcaster) {
        *self.follow.lock().unwrap() = Some(broadcaster);
    }

    fn follow(&self) -> Option<Broadcaster> {
        self.follow.lock().unwrap().clone()
    }

    /// Attach the Host-wide projection-change publisher.
    pub fn attach_control(&self, broadcaster: Broadcaster) {
        *self.control.lock().unwrap() = Some(broadcaster);
    }

    fn control(&self) -> Option<Broadcaster> {
        self.control.lock().unwrap().clone()
    }

    /// Attach the `$events` publisher used by Session lifecycle notifications.
    pub fn attach_events(&self, broadcaster: Broadcaster) {
        *self.events.lock().unwrap() = Some(broadcaster);
    }

    pub(crate) fn emit(&self, event: &str, args: Vec<Value>) {
        if let Some(events) = self.events.lock().unwrap().clone() {
            events.publish(json!({"type": "emit", "event": event, "args": args}));
        }
    }

    fn is_running(&self, session_id: &str) -> bool {
        self.running.lock().unwrap().contains_key(session_id)
    }

    pub(crate) fn resident_agent(&self, session_id: &str) -> Option<Arc<AsyncMutex<Agent>>> {
        self.agents.lock().unwrap().get(session_id).cloned()
    }

    /// Record whether this Session's live process may continue the goal automatically.
    /// @param session_id - owning Session.
    /// @param id - Goal identity the activation belongs to.
    /// @param revision - exact Goal revision.
    /// @param armed - whether automatic continuation is armed.
    /// @returns Whether the activation actually changed.
    pub(crate) fn set_goal_activation(
        &self,
        session_id: &str,
        id: &str,
        revision: u64,
        armed: bool,
    ) -> bool {
        let mut activations = self.goal_activation.lock().unwrap();
        let next = (id.to_string(), revision, armed);
        if activations.get(session_id) == Some(&next) {
            return false;
        }
        activations.insert(session_id.to_string(), next);
        true
    }

    /// Whether automatic continuation is armed for this exact Goal revision.
    /// @param session_id - owning Session.
    /// @param id - Goal identity.
    /// @param revision - exact Goal revision.
    pub(crate) fn goal_armed(&self, session_id: &str, id: &str, revision: u64) -> bool {
        self.goal_activation
            .lock()
            .unwrap()
            .get(session_id)
            .is_some_and(|(known_id, known_revision, armed)| {
                *armed && known_id == id && *known_revision == revision
            })
    }
    /// Whether this Session has a running turn that defers plan-mode commits.
    /// @param session_id - owning Session.
    pub(crate) fn session_running(&self, session_id: &str) -> bool {
        self.running.lock().unwrap().contains_key(session_id)
    }

    /// Record a plan-mode selection to commit at the next turn boundary.
    ///
    /// A running turn owns the Session write lease, so the selection's command
    /// rows are logged with the mode change once the turn settles.
    /// @param session_id - owning Session.
    /// @param pending - the selection and the command rows it still owes.
    pub(crate) fn set_plan_pending(&self, session_id: &str, pending: PendingPlan) {
        self.plan_pending
            .lock()
            .unwrap()
            .insert(session_id.to_string(), pending);
    }

    /// Take the pending plan-mode selection, if any.
    /// @param session_id - owning Session.
    pub(crate) fn take_plan_pending(&self, session_id: &str) -> Option<PendingPlan> {
        self.plan_pending.lock().unwrap().remove(session_id)
    }

    /// Read the pending selection's target state without clearing it.
    /// @param session_id - owning Session.
    pub(crate) fn plan_pending(&self, session_id: &str) -> Option<bool> {
        self.plan_pending
            .lock()
            .unwrap()
            .get(session_id)
            .map(|pending| pending.active)
    }

    pub(crate) async fn feedback_write(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.feedback_write.lock().await
    }

    pub(crate) fn has_resident_agent(&self, session_id: &str) -> bool {
        self.resident_agent(session_id).is_some()
    }

    pub(crate) fn setting_value(&self, ns: &str) -> Value {
        self.settings.value(ns).unwrap_or_else(|| json!({}))
    }

    pub(crate) fn credential_value(&self, reference: &str) -> Option<String> {
        crate::model_routes::resolve_credential(&self.home, reference)
    }

    /// Fold one live Assistant frame into this Session's reconnect baseline.
    pub fn accept_assistant_stream(
        &self,
        session_id: &str,
        frame: SessionAssistantStreamFrame,
        durable_cursor: i64,
    ) {
        let mut streams = self.assistant_streams.lock().unwrap();
        let stream = streams.entry(session_id.to_string()).or_default();
        if let Err(error) = stream.accept(frame, durable_cursor) {
            eprintln!("[dsh-rs] invalid assistant stream for {session_id}: {error}");
        }
    }

    /// Current Assistant stream baseline for one Session.
    pub fn assistant_stream_baseline(&self, session_id: &str) -> Value {
        let mut streams = self.assistant_streams.lock().unwrap();
        streams
            .get_mut(session_id)
            .map(|stream| {
                serde_json::to_value(stream.snapshot()).expect("assistant baseline serializes")
            })
            .unwrap_or_else(|| json!({"revision": 0}))
    }

    /// Claim a Session's single turn slot; the handle cancels that turn.
    fn claim_turn(&self, session_id: &str) -> Option<Cancel> {
        let mut running = self.running.lock().unwrap();
        if running.contains_key(session_id) {
            return None;
        }
        let cancel = Cancel::default();
        running.insert(session_id.to_string(), cancel.clone());
        Some(cancel)
    }

    fn release_turn(&self, session_id: &str) {
        let removed = self.running.lock().unwrap().remove(session_id).is_some();
        self.assistant_streams.lock().unwrap().remove(session_id);
        if removed {
            self.emit("api-session/status", vec![json!(session_id), json!(false)]);
        }
    }

    /// Admit a cancellation to the running turn, if any. Cancelling an idle Session is
    /// accepted and does nothing, so a client may ask defensively.
    fn cancel_turn(&self, session_id: &str) {
        if let Some(cancel) = self.running.lock().unwrap().get(session_id) {
            cancel.cancel();
        }
    }

    pub(crate) fn store(&self) -> Store {
        Store::mixed(self.home.join("sessions"))
    }

    /// Committed Session facts handed to durable-state tools for one Session.
    /// @param session_id - Session the tools run for.
    /// @returns A live handle the tool reads during its call.
    pub(crate) fn session_facts(&self, session_id: &str) -> Arc<dyn dsh_core::SessionFacts> {
        Arc::new(HostSessionFacts {
            sessions: self.home.join("sessions"),
            session_id: session_id.to_string(),
            activation: Arc::clone(&self.goal_activation),
        })
    }

    fn attachments(&self) -> Result<AttachmentStore, RpcError> {
        AttachmentStore::open(self.home.join("attachments"), AttachmentLimits::default())
            .map_err(attachment_rpc_error)
    }
}

/// Process-local Goal continuation activation: Goal id, revision, and armed flag.
type GoalActivation = Arc<Mutex<HashMap<String, (String, u64, bool)>>>;

/// Live Session facts for tools that read durable state mid-turn.
struct HostSessionFacts {
    sessions: PathBuf,
    session_id: String,
    activation: GoalActivation,
}

impl dsh_core::SessionFacts for HostSessionFacts {
    fn events(&self) -> Result<Vec<Event>, String> {
        Store::mixed(self.sessions.clone())
            .read_all()
            .into_iter()
            .find(|stored| stored.header.id == self.session_id)
            .map(|stored| stored.events)
            .ok_or_else(|| format!("session {:?} not found", self.session_id))
    }

    fn goal_armed(&self, id: &str, revision: u64) -> bool {
        self.activation
            .lock()
            .unwrap()
            .get(&self.session_id)
            .is_some_and(|(known_id, known_revision, armed)| {
                *armed && known_id == id && *known_revision == revision
            })
    }
}

fn attachment_rpc_error(error: AttachmentError) -> RpcError {
    rpc_error_details(
        "session/attachment-invalid",
        error.to_string(),
        json!({ "reason": error.code().as_str() }),
    )
}

fn upload_query_value(query: &str, key: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .find_map(|(candidate, value)| (candidate == key).then(|| value.into_owned()))
}

fn raw_upload_error(error: RpcError) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": error.code,
            "message": error.message,
            "details": error.details,
        },
    })
}

/// Store one authenticated raw file or prompt-image upload from the web client.
fn upload_binary(cx: &ApiContext, request: RawPostRequest) -> Value {
    let content_type = request
        .content_type
        .as_deref()
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some("application/octet-stream") {
        return raw_upload_error(rpc_error(
            "gateway/arguments-invalid",
            "content type must be application/octet-stream",
        ));
    }
    if upload_query_value(&request.query, "sessionId")
        .is_none_or(|session_id| session_id.is_empty())
    {
        return raw_upload_error(rpc_error("gateway/bad-request", "sessionId is required"));
    }

    let name = upload_query_value(&request.query, "name");
    let stored = (|| -> Result<Value, RpcError> {
        let attachments = cx.attachments()?;
        if let Some(media_type) = upload_query_value(&request.query, "mediaType") {
            let media_type = StoredImageMediaType::try_from(media_type.as_str())
                .map_err(attachment_rpc_error)?;
            let receipt = attachments
                .save_image(SaveImageAttachment {
                    data: &request.body,
                    media_type,
                    name: name.as_deref(),
                })
                .map_err(attachment_rpc_error)?;
            let receipt_id = receipt.id;
            match receipt.attachment {
                StagedAttachment::Image { attachment, .. } => {
                    Ok(json!({ "receiptId": receipt_id, "image": attachment }))
                }
                StagedAttachment::File { .. } => {
                    Err(attachment_rpc_error(AttachmentError::InvalidAttachmentRef))
                }
            }
        } else {
            let receipt = attachments
                .save_file(&request.body, name.as_deref())
                .map_err(attachment_rpc_error)?;
            let receipt_id = receipt.id;
            match receipt.attachment {
                StagedAttachment::File { attachment } => {
                    Ok(json!({ "receiptId": receipt_id, "file": attachment }))
                }
                StagedAttachment::Image { .. } => {
                    Err(attachment_rpc_error(AttachmentError::InvalidAttachmentRef))
                }
            }
        }
    })();

    match stored {
        Ok(value) => json!({ "ok": true, "value": value }),
        Err(error) => raw_upload_error(error),
    }
}

fn captured(text: &'static str) -> Value {
    serde_json::from_str(text).expect("captured RPC response is valid JSON")
}

/// First frame of every boot-time follow stream; each stays open afterwards.
pub fn baselines(cx: &ApiContext, boot: Option<Value>) -> Vec<(&'static str, Value)> {
    vec![
        (
            "remoteAccess/follow",
            json!({"type": "baseline", "owner": true}),
        ),
        (
            "pluginEvents/follow",
            json!({"type": "baseline", "graph": boot.unwrap_or(Value::Null)}),
        ),
        ("connectorAccounts/watch", json!({"accounts": []})),
        (
            "account/watch",
            json!({"type": "snapshot", "state": cx.account.view()}),
        ),
        (
            "sessionStatus/follow",
            json!({"type": "baseline", "items": []}),
        ),
        (
            "workspace/follow",
            json!({"type": "baseline", "value": {"items": list_workspaces(cx), "archivedSessionIds": [], "pinnedSessionIds": []}}),
        ),
        (
            "session/control",
            json!({"type": "baseline", "value": {"projections": {}}}),
        ),
    ]
}

pub fn register_all(gw: &Gateway, cx: Arc<ApiContext>) {
    cx.attach_events(gw.event_broadcaster());
    crate::account::register_all(gw, cx.account.clone());
    let upload_context = cx.clone();
    gw.raw_post(
        FILE_UPLOAD_PATH,
        usize::try_from(DEFAULT_MAX_FILE_BYTES).expect("default upload limit fits usize"),
        move |request| {
            let context = upload_context.clone();
            async move { upload_binary(&context, request) }
        },
    );
    crate::feedback_api::register_all(gw, cx.clone());
    crate::team_api::register_all(gw, cx.clone());
    crate::interaction_api::register_all(gw, cx.clone());
    crate::goal_api::register_all(gw, cx.clone());
    crate::reference_api::register_all(gw, cx.clone());
    crate::commands_api::register_all(gw, cx.clone());
    gw.rpc("session/canOpenWorkspacePath", |_| async {
        Ok(json!(false))
    });
    for (endpoint, first) in baselines(&cx, gw.boot_graph()) {
        if endpoint == "account/watch" {
            continue;
        }
        gw.stream(endpoint, move |_payload, _uplink| {
            use futures_util::StreamExt;
            futures_util::stream::once(std::future::ready(Ok(first.clone())))
                .chain(futures_util::stream::pending())
                .boxed()
        });
    }

    let statics: [(&str, &'static str); 5] = [
        (
            "permissionPresets/catalog",
            include_str!("../assets/rpc/permissionPresets_catalog.json"),
        ),
        (
            "web/listProviders",
            include_str!("../assets/rpc/web_listProviders.json"),
        ),
        (
            "dynamicCordisRunner/inventory",
            include_str!("../assets/rpc/dynamicCordisRunner_inventory.json"),
        ),
        (
            "dynamicCordisRunner/syncInspectManifest",
            include_str!("../assets/rpc/dynamicCordisRunner_syncInspectManifest.json"),
        ),
        (
            "llm/listConfigurableProviders",
            include_str!("../assets/rpc/llm_listConfigurableProviders.json"),
        ),
    ];
    for (method, text) in statics {
        gw.rpc(method, move |_| async move { Ok(captured(text)) });
    }
    let c = cx.clone();
    gw.rpc("llm/listProviders", move |_| {
        let c = c.clone();
        async move {
            Ok(crate::model_routes::provider_list(
                &c.setting_value("llm-pi-ai"),
            ))
        }
    });
    let c = cx.clone();
    gw.rpc("session/modelCatalog", move |_| {
        let c = c.clone();
        async move {
            let settings = c.setting_value("llm-pi-ai");
            let default = c.setting_value("agent-default-model");
            Ok(crate::model_routes::model_catalog(
                &settings,
                &default,
                |reference| c.credential_value(reference).is_some(),
            ))
        }
    });
    let c = cx.clone();
    gw.rpc("llm/discoverModels", move |payload| {
        let c = c.clone();
        async move {
            let args = payload.get("args").unwrap_or(&payload);
            let settings_ns = args["settingsNs"]
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| rpc_error("gateway/bad-request", "settingsNs must not be empty"))?;
            let request = &args["request"];
            let provider = request["provider"]
                .as_str()
                .filter(|provider| !provider.is_empty())
                .ok_or_else(|| rpc_error("gateway/bad-request", "request.provider is required"))?;
            if settings_ns != "llm-pi-ai" {
                return Err(rpc_error_details(
                    "llm/model-discovery-rejected",
                    "The Rust Host only discovers models for the llm-pi-ai settings namespace",
                    json!({"settingsNs": settings_ns}),
                ));
            }
            let settings = c.setting_value("llm-pi-ai");
            let profile = crate::model_routes::providers(&settings)
                .remove(provider)
                .ok_or_else(|| {
                    rpc_error_details(
                        "llm/model-discovery-rejected",
                        format!("Provider {provider:?} is not configured"),
                        json!({"settingsNs": settings_ns, "provider": provider}),
                    )
                })?;
            if profile.api.as_deref() != Some("openai-completions") {
                return Err(rpc_error_details(
                    "llm/model-discovery-rejected",
                    format!("Provider {provider:?} uses an unsupported API protocol"),
                    json!({"settingsNs": settings_ns, "provider": provider}),
                ));
            }
            if profile
                .api_key_env
                .as_deref()
                .is_some_and(|reference| c.credential_value(reference).is_none())
            {
                return Err(rpc_error_details(
                    "llm/model-discovery-rejected",
                    format!("Provider {provider:?} has no configured credential"),
                    json!({"settingsNs": settings_ns, "provider": provider}),
                ));
            }
            Ok(json!(crate::model_routes::discovered_models(&profile)))
        }
    });
    gw.rpc("pluginInventory/list", |_| async {
        Ok(json!({"entries": [], "managementAvailable": false}))
    });

    let c = cx.clone();
    gw.rpc("settings/describe", move |_| {
        let c = c.clone();
        async move { Ok(c.settings.describe()) }
    });
    let c = cx.clone();
    gw.rpc("settings/openSettingsDocument", move |_| {
        let c = c.clone();
        async move { c.settings.open_document() }
    });
    let c = cx.clone();
    gw.rpc("settings/mutate", move |payload| {
        let c = c.clone();
        async move {
            let a = &payload["args"];
            let ns = a["ns"].as_str().ok_or_else(|| {
                RpcError::new("gateway/arguments-invalid", "settings/mutate: missing ns")
            })?;
            let ops = a["ops"].as_array().cloned().unwrap_or_default();
            let expected = a["expectedRevision"].as_u64().unwrap_or(0);
            c.settings.mutate(ns, &ops, expected)
        }
    });
    let c = cx.clone();
    gw.rpc("remoteConnection/status", move |_| {
        let origin = c.origin.clone();
        async move {
            Ok(json!({"url": origin, "availability": "unreachable-host", "loginUrlAllowed": true, "ticket": "none", "pollIntervalMs": 1000}))
        }
    });
    gw.rpc("remoteConnection/pairingRequests", |_| async {
        Ok(json!({"requests": [], "pollIntervalMs": 2000}))
    });
    let c = cx.clone();
    gw.rpc("remoteConnection/loginUrl", move |_| {
        let url = format!("{}/?token={}", c.origin, c.token);
        async move { Ok(json!(url)) }
    });
    let c = cx.clone();
    gw.rpc("credentials/describe", move |payload| {
        let c = c.clone();
        async move {
            let mut out = serde_json::Map::new();
            let refs = request_of(&payload)["refs"]
                .as_array()
                .filter(|refs| refs.len() <= 64)
                .ok_or_else(|| {
                    rpc_error(
                        "gateway/bad-request",
                        "Expected at most 64 credential references",
                    )
                })?;
            for item in refs {
                let r = credential_reference(item)?;
                let configured = c.credential_value(r).is_some();
                out.insert(
                    r.into(),
                    json!({"configured": configured, "writable": false}),
                );
            }
            Ok(Value::Object(out))
        }
    });
    for method in ["credentials/set", "credentials/unset"] {
        gw.rpc(method, |payload| async move {
            let reference = credential_reference(&request_of(&payload)["ref"])?;
            Err(rpc_error_details(
                "credential/rejected",
                "This Host reads credentials from the existing local store or launch environment",
                json!({"ref": reference}),
            ))
        });
    }
    let c = cx.clone();
    gw.rpc("session/list", move |_| {
        let c = c.clone();
        async move {
            let stored = c.store().read_all();
            let running = c.running.lock().unwrap().clone();
            Ok(json!({"items": session_items_with_running(&stored, &running)}))
        }
    });
    let c = cx.clone();
    gw.rpc("session/create", move |payload| {
        let c = c.clone();
        async move { create_session(&c, &payload) }
    });
    let follow = gw.broadcaster("session/follow");
    cx.attach_follow(follow.clone());
    let event_follow = follow.clone();
    let c = cx.clone();
    gw.stream("session/follow", move |payload, _uplink| {
        use futures_util::StreamExt;
        let session_id = payload_session_id(&payload);
        // Subscribe before reading the snapshot, then drop frames the snapshot already
        // carries: the reverse order loses anything logged between the two.
        let receiver = event_follow.subscribe();
        match follow_snapshot(&c, &payload) {
            Ok(snapshot) => {
                let cursor = snapshot["cursor"].as_i64().unwrap_or(-1);
                let assistant_revision = snapshot
                    .get("assistantStream")
                    .and_then(|baseline| baseline.get("revision"))
                    .and_then(Value::as_u64);
                futures_util::stream::once(std::future::ready(Ok(snapshot)))
                    .chain(live_frames(
                        receiver,
                        session_id,
                        cursor,
                        assistant_revision,
                    ))
                    .boxed()
            }
            Err(error) => futures_util::stream::once(std::future::ready(Err(error))).boxed(),
        }
    });
    let control = gw.broadcaster("session/control");
    cx.attach_control(control.clone());
    let c = cx.clone();
    gw.stream("session/control", move |_payload, _uplink| {
        use futures_util::StreamExt;
        let receiver = control.subscribe();
        let baseline = control_baseline(&c);
        futures_util::stream::once(std::future::ready(Ok(baseline)))
            .chain(live_control_frames(receiver))
            .boxed()
    });
    gw.stream("session/observeEvent", move |payload, _uplink| {
        use futures_util::StreamExt;
        let session_id = match address_session_id(&payload) {
            Ok(session_id) => session_id,
            Err(error) => {
                return futures_util::stream::once(std::future::ready(Err(error))).boxed()
            }
        };
        let receiver = follow.subscribe();
        futures_util::stream::once(std::future::ready(Ok(json!({"type": "ready"}))))
            .chain(live_frames(receiver, Some(session_id), -1, None))
            .boxed()
    });
    let control = cx.control();
    gw.stream("session/observeProjections", move |payload, _uplink| {
        use futures_util::StreamExt;
        let session_id = match request_session_id(&payload) {
            Ok(session_id) => session_id.to_string(),
            Err(error) => {
                return futures_util::stream::once(std::future::ready(Err(error))).boxed()
            }
        };
        let Some(receiver) = control.as_ref().map(Broadcaster::subscribe) else {
            return futures_util::stream::once(std::future::ready(Ok(json!({"type": "ready"}))))
                .chain(futures_util::stream::pending())
                .boxed();
        };
        futures_util::stream::once(std::future::ready(Ok(json!({"type": "ready"}))))
            .chain(live_control_frames(receiver).filter_map(move |frame| {
                let belongs = frame
                    .as_ref()
                    .ok()
                    .and_then(|frame| frame.get("sessionId"))
                    .and_then(Value::as_str)
                    == Some(session_id.as_str());
                std::future::ready(belongs.then_some(frame))
            }))
            .boxed()
    });
    let c = cx.clone();
    gw.stream("session/observeState", move |payload, _uplink| {
        use futures_util::StreamExt;
        let session_id = match request_session_id(&payload) {
            Ok(session_id) => session_id.to_string(),
            Err(error) => {
                return futures_util::stream::once(std::future::ready(Err(error))).boxed()
            }
        };
        match session_state(&c, &json!({"sessionId": session_id})) {
            Ok(state) => state_frames(c.clone(), session_id, state),
            Err(error) => futures_util::stream::once(std::future::ready(Err(error))).boxed(),
        }
    });
    let c = cx.clone();
    gw.rpc("session/prompt", move |payload| {
        let c = c.clone();
        async move { prompt(c, payload).await }
    });
    let c = cx.clone();
    gw.rpc("session/updateQueue", move |payload| {
        let c = c.clone();
        async move { update_queue(&c, payload).await }
    });
    let c = cx.clone();
    gw.rpc("session/attachment", move |payload| {
        let c = c.clone();
        async move { attachment(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/cancel", move |payload| {
        let c = c.clone();
        async move { cancel_session(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/page", move |payload| {
        let c = c.clone();
        async move { session_page(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/events", move |payload| {
        let c = c.clone();
        async move { session_page(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/rename", move |payload| {
        let c = c.clone();
        async move { rename_session(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/selectModel", move |payload| {
        let c = c.clone();
        async move { select_model(&c, &payload).await }
    });
    let c = cx.clone();
    gw.rpc("session/fork", move |payload| {
        let c = c.clone();
        async move { fork_session(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/state", move |payload| {
        let c = c.clone();
        async move { session_state(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/wait", move |payload| {
        let c = c.clone();
        async move { wait_session(&c, &payload).await }
    });
    let c = cx.clone();
    gw.rpc("session/projections", move |payload| {
        let c = c.clone();
        async move { session_projections(&c, &payload) }
    });
    let c = cx.clone();
    gw.rpc("session/search", move |payload| {
        let c = c.clone();
        async move { search_sessions(&c, &payload) }
    });
    gw.rpc("directoryPicker/list", |payload| async move {
        list_directory(&payload)
    });
    gw.rpc("directoryPicker/createDirectory", |payload| async move {
        create_directory(&payload)
    });
    gw.rpc("directoryPicker/pick", |_| async move {
        // This Host renders no OS dialog on a display, so a native choice cannot be made
        // here; the browse picker drives `list` and `createDirectory` instead.
        Err(rpc_error(
            "directory-picker/unavailable",
            "this Host opens no native directory dialog; use the browse picker",
        ))
    });
    let c = cx;
    gw.rpc("workspace/initializeDefault", move |_| {
        let c = c.clone();
        async move { initialize_default(&c).map(|w| json!({"workspace": w})) }
    });
}

fn is_user_message(kind: &str) -> bool {
    kind == "user/message"
}

fn credential_reference(value: &Value) -> Result<&str, RpcError> {
    value
        .as_str()
        .filter(|name| {
            let mut bytes = name.bytes();
            bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
        .ok_or_else(|| rpc_error("gateway/bad-request", "Invalid credential reference"))
}

fn selection_value(provider: &str, model: &str, effort: Option<&str>) -> Option<Value> {
    if provider.is_empty() || model.is_empty() {
        return None;
    }
    let mut selection = json!({"provider": provider, "model": model});
    if let Some(effort) = effort.filter(|effort| !effort.is_empty()) {
        selection["reasoningEffort"] = json!(effort);
    }
    Some(selection)
}

fn model_selection_values(events: &[Event]) -> Value {
    let mut last_used: Option<Value> = None;
    let mut pending: Option<Value> = None;
    for event in events {
        match event.kind.as_str() {
            "model/selection" => {
                let selected =
                    event
                        .data
                        .get("provider")
                        .and_then(Value::as_str)
                        .and_then(|provider| {
                            selection_value(
                                provider,
                                event
                                    .data
                                    .get("model")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default(),
                                event.data.get("reasoningEffort").and_then(Value::as_str),
                            )
                        });
                if selected.is_some() && selected != pending {
                    pending = selected;
                }
            }
            "request/header" => {
                let Some(header) = event.data.get("header") else {
                    continue;
                };
                let Some(config) = header.get("config") else {
                    continue;
                };
                let adapter_defaulted = header
                    .get("adapterDefaults")
                    .and_then(|defaults| defaults.get("reasoningEffort"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let effort = if adapter_defaulted {
                    None
                } else {
                    config.get("reasoningEffort").and_then(Value::as_str)
                };
                let Some(selected) =
                    config
                        .get("provider")
                        .and_then(Value::as_str)
                        .and_then(|provider| {
                            selection_value(
                                provider,
                                config
                                    .get("model")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default(),
                                effort,
                            )
                        })
                else {
                    continue;
                };
                if pending.as_ref() == Some(&selected) {
                    pending = None;
                }
                if last_used.as_ref() != Some(&selected) {
                    last_used = Some(selected);
                }
            }
            _ => {}
        }
    }
    json!({"lastUsed": last_used, "next": pending.or(last_used)})
}

/// Client-visible projection values for one stored Session.
fn projection_values(s: &Stored) -> Value {
    let blank = !s.events.iter().any(|e| is_user_message(&e.kind));
    let title = s
        .events
        .iter()
        .rev()
        .find(|e| e.kind == "session/title")
        .and_then(|e| e.data.get("title").cloned())
        .unwrap_or(Value::Null);
    let cwd = s.header.cwd.clone();
    let model_selection = model_selection_values(&s.events);
    let team_enabled = s
        .events
        .iter()
        .rev()
        .find(|event| event.kind == "team/mode")
        .and_then(|event| event.data.get("enabled"))
        .and_then(Value::as_bool)
        .or_else(|| {
            s.events
                .iter()
                .any(|event| event.kind == "team/task")
                .then_some(true)
        })
        .unwrap_or(false);
    let plan = dsh_planning::fold_plan(&s.events).projection();
    let inbox = InboxState::recover(&s.events).unwrap_or_default();
    json!({
        "todos": todos_projection(&s.events),
        "userQuestions": crate::user_questions::fold(&s.events),
        "sessionListMetadata": {"blank": blank, "lastPromptAt": null},
        "tokenUsage": {"uncachedInputTokens": 0, "outputTokens": 0, "cacheReadTokens": 0, "cacheWriteTokens": 0},
        "title": title,
        "goal": goal_projection(&s.events),
        "agentPreset": selected_agent_preset_from_events(&s.header.agent_preset, &s.events),
        "workingDirectory": cwd,
        "modelSelection": model_selection,
        "permissions": {"currentValue": "workspace-write"},
        "inbox": inbox,
        "subagentCatalog": [],
        "subagentTiming": {"settledMs": 0},
        "subagent": null,
        "agentTeam": {"enabled": team_enabled, "tasks": []},
        "plan": {"active": plan.active, "pending": plan.pending},
        "conversationContinuation": null
    })
}

/// The `goal` projection: the durable Goal snapshot plus its admitted round count,
/// or null before a Goal exists and after one is cleared.
/// @param events - Committed Session rows in order.
/// @returns The projection value the client reads.
fn goal_projection(events: &[Event]) -> Value {
    let mut value: Option<Value> = None;
    for event in events {
        match event.kind.as_str() {
            "goal/change" => {
                if event.data.get("operation").and_then(Value::as_str) == Some("clear") {
                    value = None;
                    continue;
                }
                let Some(goal) = event.data.get("goal") else {
                    continue;
                };
                value = Some(json!({
                    "goal": goal,
                    "roundsStarted": event.data.get("roundsStarted").cloned().unwrap_or(json!(0)),
                    "createdAt": event.data.get("createdAt").cloned().unwrap_or(Value::Null),
                    "updatedAt": event.data.get("updatedAt").cloned().unwrap_or(Value::Null),
                }));
            }
            "user/message" => {
                let Some(source) = event.data.get("source") else {
                    continue;
                };
                if source.get("kind").and_then(Value::as_str) != Some("goal") {
                    continue;
                }
                let Some(current) = value.as_mut() else {
                    continue;
                };
                let admitted = current["goal"]["id"]
                    == *source.get("goalId").unwrap_or(&Value::Null)
                    && current["goal"]["revision"]
                        == *source.get("revision").unwrap_or(&Value::Null);
                let round = source.get("round").and_then(Value::as_u64);
                let next_round = current["roundsStarted"].as_u64().map(|rounds| rounds + 1);
                if admitted && round.is_some() && round == next_round {
                    current["roundsStarted"] = json!(round);
                }
            }
            _ => {}
        }
    }
    value.unwrap_or(Value::Null)
}
/// The `todos` projection: the latest `todo/write` list after the most recent
/// `turn/start`, or null before a write in this turn.
/// @param events - Committed Session rows in order.
/// @returns The whole current list, or null.
fn todos_projection(events: &[Event]) -> Value {
    let mut value = Value::Null;
    for event in events {
        match event.kind.as_str() {
            "todo/write" => value = event.data.get("todos").cloned().unwrap_or(Value::Null),
            "turn/start" => value = Value::Null,
            _ => {}
        }
    }
    value
}

/// One `session/list` row per stored Session, shaped like the TypeScript Host's.
pub fn session_items(all: &[Stored]) -> Vec<Value> {
    session_items_with_running(all, &HashMap::new())
}

/// One list row per Session with the Host's current turn-slot state.
pub fn session_items_with_running(all: &[Stored], running: &HashMap<String, Cancel>) -> Vec<Value> {
    all.iter()
        .map(|s| {
            let is_running = running.contains_key(&s.header.id);
            let mut row = json!({
                "sessionId": s.header.id,
                "updatedAt": s.modified_ms,
                "agentAvailable": is_running,
                "formatStatus": "current",
                "running": is_running,
                "blank": !s.events.iter().any(|e| is_user_message(&e.kind)),
                "cwd": s.header.cwd.clone(),
                "projections": {
                    "kind": "cached",
                    "asOfSeq": s.events.len(),
                    "values": projection_values(s)
                }
            });
            if let Some(parent) = &s.header.parent_session {
                row["parentSessionId"] = json!(parent);
            }
            if let Some(origin) = s.header.origin {
                row["origin"] = json!(origin);
            }
            row
        })
        .collect()
}

/// One committed row as the browser wire envelope: `seq` and `time` are explicit
/// fields there, while the log keeps them alongside the row's own additions.
fn wire_event(event: &dsh_session_format::Event, index: usize) -> Value {
    let mut value = json!({
        "type": event.kind,
        "seq": event.seq().unwrap_or(index as u64),
        "time": event.time().unwrap_or(0),
        "data": event.data,
    });
    for (key, extra) in &event.extra {
        if key != "seq" && key != "time" {
            value[key] = extra.clone();
        }
    }
    value
}

/// The opening frame of `session/follow`: the Session header, its history records up to
/// the requested message budget, and the projection baseline at that cut.
pub fn follow_snapshot(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = payload.get("args").unwrap_or(payload);
    let request = args.get("request").unwrap_or(args);
    let session_id = request
        .get("address")
        .and_then(|a| a.get("sessionId"))
        .or_else(|| request.get("sessionId"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            rpc_error(
                "gateway/bad-request",
                "session/follow requires an address.sessionId",
            )
        })?;
    let budget = request
        .get("maxMessages")
        .and_then(Value::as_u64)
        .unwrap_or(500) as usize;

    let stored = cx
        .store()
        .read_all()
        .into_iter()
        .find(|s| s.header.id == session_id)
        .ok_or_else(|| {
            rpc_error(
                "session/not-found",
                format!("session \"{session_id}\" not found"),
            )
        })?;

    let total = stored.events.len();
    let start = total.saturating_sub(budget);
    let records: Vec<Value> = stored.events[start..]
        .iter()
        .enumerate()
        .map(|(offset, event)| json!({"type": "event", "event": wire_event(event, start + offset)}))
        .collect();

    // The wire header is the stored header without its discriminator.
    let mut header =
        serde_json::to_value(&stored.header).map_err(|e| rpc_error("session/io", e.to_string()))?;
    if let Some(fields) = header.as_object_mut() {
        fields.remove("type");
    }

    let mut snapshot = json!({
        "type": "snapshot",
        "header": header,
        "cursor": total as i64 - 1,
        "records": records,
        "hasMore": start > 0,
        "projections": {"asOfSeq": total, "values": projection_values(&stored)}
    });
    if request.get("assistantStream").and_then(Value::as_bool) == Some(true) {
        snapshot["assistantStream"] = cx.assistant_stream_baseline(session_id);
    }
    Ok(snapshot)
}

pub(crate) fn stored_session(cx: &ApiContext, session_id: &str) -> Result<Stored, RpcError> {
    cx.store()
        .read_all()
        .into_iter()
        .find(|stored| stored.header.id == session_id)
        .ok_or_else(|| {
            rpc_error(
                "session/not-found",
                format!("session \"{session_id}\" not found"),
            )
        })
}

fn request_session_id(payload: &Value) -> Result<&str, RpcError> {
    request_of(payload)
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "sessionId must not be empty"))
}

fn address_session_id(payload: &Value) -> Result<String, RpcError> {
    let request = request_of(payload);
    let address = request.get("address").ok_or_else(|| {
        rpc_error(
            "gateway/bad-request",
            "session history request requires an address",
        )
    })?;
    match address.get("kind").and_then(Value::as_str) {
        Some("session") | None => address
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .ok_or_else(|| rpc_error("gateway/bad-request", "session address requires sessionId")),
        Some("subagent") => address
            .get("childSessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                rpc_error(
                    "gateway/bad-request",
                    "subagent address requires childSessionId",
                )
            }),
        Some(kind) => Err(rpc_error(
            "gateway/bad-request",
            format!("unknown Session address kind \"{kind}\""),
        )),
    }
}

fn session_cursor(stored: &Stored) -> i64 {
    stored.events.len().saturating_sub(1) as i64
}

fn is_append_surface(event: &Event) -> bool {
    event.extra.get("surfaceOp").is_none_or(|op| op == "append")
}

fn event_seq(event: &Event, index: usize) -> u64 {
    event.seq().unwrap_or(index as u64)
}

/// Append one Host-owned durable event and publish it to current followers.
pub(crate) fn append_session_event(
    cx: &ApiContext,
    session_id: &str,
    kind: &str,
    data: Value,
) -> Result<(Event, usize), RpcError> {
    if cx.running.lock().unwrap().contains_key(session_id) {
        return Err(rpc_error(
            "session/agent-busy",
            format!("session \"{session_id}\" has a running turn"),
        ));
    }
    let stored = stored_session(cx, session_id)?;
    let before = projection_values(&stored);
    let mut session = cx
        .store()
        .open(stored.header.cwd.as_deref(), session_id)
        .map_err(|error| rpc_error("session/io", error.to_string()))?;
    let seq = session.events.len();
    let mut extra = serde_json::Map::new();
    extra.insert("seq".into(), json!(seq));
    extra.insert("time".into(), json!(now_ms()));
    let event = Event {
        kind: kind.into(),
        data,
        extra,
    };
    session
        .append(std::slice::from_ref(&event))
        .map_err(|error| rpc_error("session/io", error.to_string()))?;
    let after = projection_values(&Stored {
        header: session.header.clone(),
        events: session.events.clone(),
        modified_ms: 0,
    });
    publish_session_event(cx, session_id, &event, seq, &before, &after);
    Ok((event, seq))
}

fn publish_session_event(
    cx: &ApiContext,
    session_id: &str,
    event: &Event,
    seq: usize,
    before: &Value,
    after: &Value,
) {
    note_goal_change(cx, session_id, event);
    if let Some(follow) = cx.follow() {
        follow.publish(json!({
            "sessionId": session_id,
            "seq": seq,
            "frame": {"type": "event", "event": wire_event(event, seq)},
        }));
    }
    let changes = projection_changes(before, after);
    if !changes.is_empty() {
        if let Some(control) = cx.control() {
            control.publish(json!({
                "type": "projection", "sessionId": session_id, "seq": seq, "changes": changes,
            }));
        }
    }
}

/// Track process-local Goal continuation activation from one committed change.
///
/// Every committed `goal/change` passes here — Host RPCs and model tool calls
/// alike — so activation has one owner and the client sees one edge per real
/// activation change.
/// @param cx - running Host.
/// @param session_id - owning Session.
/// @param event - the committed event.
fn note_goal_change(cx: &ApiContext, session_id: &str, event: &Event) {
    if event.kind != "goal/change" {
        return;
    }
    let Some(operation) = event.data.get("operation").and_then(Value::as_str) else {
        return;
    };
    let (id, revision, cleared, armed) = if operation == "clear" {
        let Some(cleared) = event.data.get("cleared") else {
            return;
        };
        let (Some(id), Some(revision)) = (
            cleared.get("id").and_then(Value::as_str),
            cleared.get("revision").and_then(Value::as_u64),
        ) else {
            return;
        };
        (id.to_string(), revision, true, false)
    } else {
        let Some(goal) = event.data.get("goal") else {
            return;
        };
        let (Some(id), Some(revision)) = (
            goal.get("id").and_then(Value::as_str),
            goal.get("revision").and_then(Value::as_u64),
        ) else {
            return;
        };
        let armed = match operation {
            "create" | "resume" => true,
            "pause" | "complete" | "block" => false,
            // An edit keeps the activation the previous revision carried.
            "edit" => cx.goal_armed(session_id, id, revision.saturating_sub(1)),
            _ => return,
        };
        (id.to_string(), revision, false, armed)
    };
    let prior = cx.goal_armed(session_id, &id, revision.saturating_sub(1));
    if !cx.set_goal_activation(session_id, &id, revision, armed) || prior == armed {
        return;
    }
    let mut payload = json!({"sessionId": session_id});
    if !cleared {
        payload["goal"] = json!({
            "id": id,
            "revision": revision,
            "activation": if armed { "armed" } else { "disarmed" },
        });
    }
    cx.emit("goal/activation-changed", vec![payload]);
}

/// Model-visible continuation prompt for one same-session Goal round.
/// @param goal - exact active Goal revision being admitted.
/// @param round - next positive round number.
/// @returns The instruction a Goal round reads in Session history.
fn goal_round_prompt(goal: &dsh_planning::GoalSnapshot, round: u64) -> String {
    let objective = serde_json::to_string(&goal.objective).unwrap_or_default();
    format!(
        "<goal_round>\nObjective: {objective}\nRound: {round}/{}\n\n\
Continue working toward the objective in this same session. Treat the current workspace, \
tool results, and durable session state as authoritative; inspect them instead of assuming \
earlier narration is still current. Make concrete progress and verify the result. Before \
claiming completion, gather evidence that the whole objective is achieved, read the current \
goal, and mark it complete. If work remains, leave the goal active for the next round. Follow \
the configured goal-tool policy before reporting a blocker.\n</goal_round>",
        goal.max_goal_rounds
    )
}

/// Queue the next automatic Goal round for a quiescent Agent.
///
/// A Goal that exhausted its round budget is blocked here, so the next human turn
/// sees why continuation stopped.
/// @param cx - running Host.
/// @param session_id - Session the Agent owns.
/// @param agent - quiescent Agent holding the Session write lease.
/// @returns Whether another turn was queued.
/// What the Host does at quiescence for the current Goal.
enum GoalRoundPlan {
    /// Queue one more automatic round.
    Continue { content: String, source: Value },
    /// Retire a Goal that spent its whole round budget.
    Block(dsh_planning::GoalChange),
    /// Leave the Agent idle.
    Idle,
}

/// Decide the next automatic Goal round from durable state and live activation.
/// @param cx - running Host.
/// @param session_id - owning Session.
/// @param projection - current durable Goal.
/// @returns The round to queue, the retirement to record, or idle.
fn plan_goal_round(
    cx: &ApiContext,
    session_id: &str,
    projection: &dsh_planning::GoalProjection,
) -> GoalRoundPlan {
    if projection.goal.phase != dsh_planning::GoalPhase::Active {
        return GoalRoundPlan::Idle;
    }
    if !cx.goal_armed(
        session_id,
        projection.goal.id.as_str(),
        projection.goal.revision,
    ) {
        return GoalRoundPlan::Idle;
    }
    let reference = dsh_planning::GoalRef {
        id: projection.goal.id.clone(),
        revision: projection.goal.revision,
    };
    if projection.rounds_started >= projection.goal.max_goal_rounds {
        let reason = dsh_planning::GoalBlockReason {
            code: "round-limit".into(),
            message: format!(
                "Goal reached its configured limit of {} rounds.",
                projection.goal.max_goal_rounds
            ),
        };
        return match dsh_planning::block_goal(Some(projection), &reference, reason, now_ms()) {
            Ok(change) => GoalRoundPlan::Block(change),
            // A refused retirement leaves the Goal paused by its own rules.
            Err(_) => GoalRoundPlan::Idle,
        };
    }
    let round = projection.rounds_started + 1;
    GoalRoundPlan::Continue {
        content: goal_round_prompt(&projection.goal, round),
        source: json!({
            "kind": "goal",
            "goalId": projection.goal.id.as_str(),
            "revision": projection.goal.revision,
            "round": round,
        }),
    }
}

/// Apply one Goal round decision to a quiescent Agent.
/// @param cx - running Host.
/// @param session_id - Session the Agent owns.
/// @param agent - quiescent Agent holding the Session write lease.
/// @returns Whether another turn was queued.
fn drive_goal_round(cx: &ApiContext, session_id: &str, agent: &mut Agent) -> bool {
    let mut timeline = dsh_planning::GoalTimeline::new();
    for event in &agent.session().events {
        let _ = timeline.observe(event);
    }
    let Some(projection) = timeline.projection().cloned() else {
        return false;
    };
    match plan_goal_round(cx, session_id, &projection) {
        GoalRoundPlan::Continue { content, source } => agent
            .enqueue(
                dsh_core::UserInput::with_source(vec![Content::Text { text: content }], source),
                InboxTarget::NextTurn,
            )
            .is_ok(),
        GoalRoundPlan::Block(change) => {
            if let Ok(payload) = change.to_payload() {
                let _ = agent.record_session_event("goal/change", payload);
            }
            false
        }
        GoalRoundPlan::Idle => false,
    }
}

/// `session/page` and its deprecated `session/events` alias.
pub fn session_page(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let session_id = address_session_id(payload)?;
    let stored = stored_session(cx, &session_id)?;
    let request = request_of(payload);
    let cursor = match request.get("throughSeq") {
        Some(value) => value.as_i64().filter(|seq| *seq >= -1).ok_or_else(|| {
            rpc_error(
                "gateway/bad-request",
                "throughSeq must be an integer greater than or equal to -1",
            )
        })?,
        None => session_cursor(&stored),
    };
    if cursor > session_cursor(&stored) {
        return Err(rpc_error(
            "gateway/bad-request",
            format!(
                "session history cut {cursor} exceeds {}",
                session_cursor(&stored)
            ),
        ));
    }
    let before_seq = match request.get("beforeSeq") {
        Some(value) => value.as_i64().filter(|seq| *seq >= 0).ok_or_else(|| {
            rpc_error(
                "gateway/bad-request",
                "beforeSeq must be a non-negative safe integer",
            )
        })?,
        None => cursor + 1,
    };
    let max_messages = match request.get("maxMessages") {
        Some(value) => value
            .as_u64()
            .and_then(|count| usize::try_from(count).ok())
            .filter(|count| *count > 0)
            .ok_or_else(|| {
                rpc_error(
                    "gateway/bad-request",
                    "maxMessages must be a positive safe integer",
                )
            })?,
        None => 50,
    };
    let turn_window = request.get("turnWindow");
    let min_messages = turn_window
        .and_then(|window| window.get("minMessages"))
        .and_then(Value::as_u64)
        .map(usize::try_from)
        .transpose()
        .map_err(|_| {
            rpc_error(
                "gateway/bad-request",
                "turnWindow.minMessages must be a positive safe integer",
            )
        })?
        .unwrap_or(0);
    let min_turns = turn_window
        .and_then(|window| window.get("minTurns"))
        .and_then(Value::as_u64)
        .filter(|count| *count > 0);
    if turn_window.is_some() {
        if min_messages == 0 || min_messages > max_messages {
            return Err(rpc_error(
                "gateway/bad-request",
                "turnWindow.minMessages must be a positive safe integer no greater than maxMessages",
            ));
        }
        if min_turns.is_none() {
            return Err(rpc_error(
                "gateway/bad-request",
                "turnWindow.minTurns must be a positive safe integer",
            ));
        }
    }

    let end = (cursor + 1).min(before_seq).max(0) as usize;
    let mut count = 0usize;
    let mut turns = 0u64;
    let mut cut = 0usize;
    for index in (0..end).rev() {
        let event = &stored.events[index];
        if turn_window.is_some() && event.kind == "turn/start" {
            turns += 1;
            if count >= min_messages && turns >= min_turns.unwrap_or(0) {
                cut = index;
                break;
            }
        }
        if !matches!(event.kind.as_str(), "user/message" | "assistant/message")
            || !is_append_surface(event)
        {
            continue;
        }
        count += 1;
        let mut group_start = event_seq(event, index) as usize;
        if let Some(sources) = event.extra.get("sourceEventSeqs").and_then(Value::as_array) {
            for source in sources.iter().filter_map(Value::as_u64) {
                group_start = group_start.min(source as usize);
            }
        }
        if count >= max_messages {
            cut = group_start.min(end);
            break;
        }
    }
    let records: Vec<Value> = stored.events[cut..end]
        .iter()
        .enumerate()
        .map(|(offset, event)| json!({"type": "event", "event": wire_event(event, cut + offset)}))
        .collect();
    Ok(json!({"cursor": cursor, "records": records, "hasMore": cut > 0}))
}

/// `session/rename`: normalize and append a user-owned title event.
pub fn rename_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let session_id = request_session_id(payload)?;
    let raw = request_of(payload)
        .get("title")
        .and_then(Value::as_str)
        .ok_or_else(|| rpc_error("gateway/bad-request", "session/rename requires title"))?;
    let title = normalize_session_title(raw, 80);
    if title.is_empty() {
        return Err(rpc_error(
            "session/title-invalid",
            "session title must contain visible characters",
        ));
    }
    let (_, seq) = append_session_event(
        cx,
        session_id,
        "session/title",
        json!({"title": title, "messageSeqs": [], "source": {"kind": "user"}}),
    )?;
    Ok(json!({"title": title, "seq": seq}))
}

/// `session/selectModel`: update the resident Agent's model route and persist the choice.
pub async fn select_model(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let session_id = request_session_id(payload)?;
    let request = request_of(payload);
    let provider = request
        .get("provider")
        .and_then(Value::as_str)
        .filter(|provider| !provider.is_empty())
        .ok_or_else(|| {
            rpc_error(
                "gateway/bad-request",
                "session/selectModel requires provider",
            )
        })?;
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "session/selectModel requires model"))?;
    let effort = request.get("reasoningEffort").and_then(Value::as_str);
    if effort.is_some_and(|value| !matches!(value, "low" | "high" | "max")) {
        return Err(rpc_error(
            "session/model-unavailable",
            "reasoningEffort must be low, high, or max",
        ));
    }
    let mut selected = json!({"provider": provider, "model": model});
    if let Some(effort) = effort {
        selected["reasoningEffort"] = json!(effort);
    }
    let route = build_model_route(
        cx,
        provider,
        model,
        effort,
        session_id,
        Some(cx.attachments()?),
    )?;
    if let Some(resident) = cx.resident_agent(session_id) {
        let mut agent = resident.lock().await;
        let before = projection_values(&Stored {
            header: agent.session().header.clone(),
            events: agent.session().events.clone(),
            modified_ms: 0,
        });
        if !agent.switch_model_route(route) {
            return Err(rpc_error(
                "session/model-unavailable",
                "this Agent does not support changing its model route",
            ));
        }
        let (event, seq) = agent
            .record_model_selection(provider, model, effort)
            .map_err(|error| rpc_error("session/io", error.to_string()))?;
        let after = projection_values(&Stored {
            header: agent.session().header.clone(),
            events: agent.session().events.clone(),
            modified_ms: 0,
        });
        publish_session_event(cx, session_id, &event, seq, &before, &after);
    } else {
        append_session_event(cx, session_id, "model/selection", selected.clone())?;
    }
    Ok(json!({"selected": selected}))
}

/// `session/fork`: copy an exact inclusive prefix and repair an open tail for the child.
pub fn fork_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let source_id = request
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "session/fork requires sessionId"))?;
    let requested = request.get("atSeq");
    let boundary = match requested {
        Some(value) => value
            .as_u64()
            .and_then(|seq| usize::try_from(seq).ok())
            .ok_or_else(|| {
                rpc_error(
                    "gateway/bad-request",
                    "atSeq must be a non-negative safe integer",
                )
            })?,
        None => {
            let stored = stored_session(cx, source_id)?;
            latest_completed_boundary(&stored.events).ok_or_else(|| RpcError {
                code: "session/fork-unavailable".into(),
                message: format!("session \"{source_id}\" has no completed turn to fork from"),
                details: json!({"sessionId": source_id}),
            })?
        }
    };
    let stored = stored_session(cx, source_id)?;
    if boundary >= stored.events.len()
        || stored.events[boundary].seq() != Some(boundary as u64)
        || stored.events[..=boundary]
            .iter()
            .enumerate()
            .any(|(index, event)| event.seq() != Some(index as u64))
    {
        let last = stored.events.last().and_then(Event::seq);
        return Err(RpcError {
            code: "session/fork-unavailable".into(),
            message: format!(
                "event {boundary} does not exist in session \"{source_id}\" (last seq: {})",
                last.map_or_else(|| "none".into(), |seq| seq.to_string()),
            ),
            details: json!({"sessionId": source_id}),
        });
    }
    let seed = build_fork_seed(&stored.events, boundary);
    let cwd = stored.header.cwd.clone();
    let child_id = fresh_session_id();
    let child = Header {
        kind: HeaderTag::Session,
        version: SESSION_FORMAT_VERSION,
        id: child_id.clone(),
        created_at: now_ms(),
        cwd,
        parent_session: None,
        is_seeded: true,
        origin: None,
        delegation_depth: 0,
        agent_preset: stored.header.agent_preset.clone(),
    };
    let mut session = cx
        .store()
        .create(child)
        .map_err(|error| fork_write_error(source_id, error))?;
    session
        .append(&seed)
        .map_err(|error| fork_write_error(source_id, error))?;
    publish_session_added(cx, &session);
    Ok(json!({"sessionId": child_id}))
}

fn fork_write_error(source_id: &str, error: StoreError) -> RpcError {
    match error {
        StoreError::MigrationRequired { .. } => RpcError {
            code: "session/migration-required".into(),
            message: format!("session \"{source_id}\" requires migration"),
            details: json!({"sessionId": source_id}),
        },
        error => rpc_error(
            "gateway/internal",
            format!("failed to fork session \"{source_id}\": {error}"),
        ),
    }
}

fn fresh_session_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("session-{nanos:x}-{:x}", std::process::id())
}

fn latest_completed_boundary(events: &[Event]) -> Option<usize> {
    let mut boundary = events.iter().rposition(|event| event.kind == "turn/end")?;
    for (index, event) in events.iter().enumerate().skip(boundary + 1) {
        if event.kind == "turn/start"
            || event.kind == "agent/inbox/spliced"
            || (event.kind == "user/message"
                && event.extra.get("surfaceOp") == Some(&json!("append")))
        {
            break;
        }
        if event.seq() == Some(index as u64) {
            boundary = index;
        }
    }
    Some(boundary)
}

#[derive(Clone)]
struct PendingForkCall {
    id: String,
    turn: u64,
    step: u64,
    call_seq: Option<u64>,
}

fn build_fork_seed(events: &[Event], boundary: usize) -> Vec<Event> {
    let mut seed = events[..=boundary].to_vec();
    let time = events[boundary].time().unwrap_or(0);
    seed.push(stored_event(
        "session/end-seed",
        json!({"inherited": true}),
        (boundary + 1) as u64,
        time,
    ));
    let mut open_turn = None;
    let mut open_step = None;
    let mut pending: Vec<PendingForkCall> = Vec::new();
    for (index, event) in seed.iter().enumerate() {
        match event.kind.as_str() {
            "turn/start" => {
                open_turn = event.data.get("turn").and_then(Value::as_u64);
                open_step = None;
                pending.clear();
            }
            "turn/end" => {
                open_turn = None;
                open_step = None;
                pending.clear();
            }
            "step/start" => open_step = event.data.get("step").and_then(Value::as_u64),
            "step/end" => {
                open_step = None;
                pending.clear();
            }
            "assistant/message" => {
                let Some(turn) = event.data.get("turn").and_then(Value::as_u64) else {
                    continue;
                };
                let Some(step) = event.data.get("step").and_then(Value::as_u64) else {
                    continue;
                };
                for block in event
                    .data
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if block.get("type").and_then(Value::as_str) == Some("tool-call") {
                        if let Some(id) = block.get("id").and_then(Value::as_str) {
                            if !pending.iter().any(|call| call.id == id) {
                                pending.push(PendingForkCall {
                                    id: id.into(),
                                    turn,
                                    step,
                                    call_seq: None,
                                });
                            }
                        }
                    }
                }
            }
            "tool/call" => {
                let Some(call_id) = event.data.get("callId").and_then(Value::as_str) else {
                    continue;
                };
                if let Some(call) = pending.iter_mut().find(|call| call.id == call_id) {
                    call.call_seq = Some(index as u64);
                }
            }
            "tool/result" => {
                let Some(call_id) = event
                    .data
                    .get("message")
                    .and_then(|message| message.get("source"))
                    .and_then(|source| source.get("callId"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let turn = event.data.get("turn").and_then(Value::as_u64);
                let step = event.data.get("step").and_then(Value::as_u64);
                if event.extra.get("surfaceOp") == Some(&json!("append")) {
                    pending.retain(|call| {
                        call.id != call_id || Some(call.turn) != turn || Some(call.step) != step
                    });
                }
            }
            _ => {}
        }
    }
    if open_turn.is_none() {
        return seed;
    }
    let mut seq = seed.len() as u64;
    for call in pending {
        let started = call.call_seq.is_some();
        let text = if started {
            "The history inherited by this branch records this tool call starting but does not include its result. The parent session may have completed it after the fork point. Decide whether to retry from the tool semantics: retry only if the operation is read-only or idempotent; if it may have side effects, first verify external state or ask the user. Do not retry blindly."
        } else {
            "The history inherited by this branch has no record of this tool call starting. The parent session may have executed it after the fork point. Decide whether to retry from the tool semantics: retry only if the operation is read-only or idempotent; if it may have side effects, first verify external state or ask the user. Do not retry blindly."
        };
        let message = json!({
            "id": format!("forked-tool-result-{}-{seq}", call.id),
            "role": "tool",
            "toolCallId": call.id,
            "isError": true,
            "source": {"kind": "tool", "callId": call.id},
            "content": [{"type": "text", "text": text}],
        });
        let mut event = stored_event(
            "tool/result",
            json!({
                "turn": call.turn,
                "step": call.step,
                "message": message,
                "error": if started {
                    json!({"name": "ToolOutcomeUnknownError", "code": "TOOL_OUTCOME_UNKNOWN"})
                } else {
                    json!({"name": "ToolNotStartedError", "code": "TOOL_NOT_STARTED"})
                },
            }),
            seq,
            time,
        );
        event.extra.insert("surfaceOp".into(), json!("append"));
        if let Some(call_seq) = call.call_seq {
            event
                .extra
                .insert("sourceEventSeqs".into(), json!([call_seq]));
        }
        seed.push(event);
        seq += 1;
    }
    if let (Some(turn), Some(step)) = (open_turn, open_step) {
        seed.push(stored_event(
            "step/end",
            json!({"turn": turn, "step": step}),
            seq,
            time,
        ));
        seq += 1;
    }
    if let Some(turn) = open_turn {
        seed.push(stored_event(
            "turn/end",
            json!({"turn": turn, "reason": {"kind": "forked"}}),
            seq,
            time,
        ));
    }
    seed
}

fn stored_event(kind: &str, data: Value, seq: u64, time: u64) -> Event {
    Event {
        kind: kind.into(),
        data,
        extra: serde_json::Map::from_iter([
            ("seq".into(), json!(seq)),
            ("time".into(), json!(time)),
        ]),
    }
}

/// `session/state`: one passive execution-state sample.
pub fn session_state(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let session_id = request_session_id(payload)?;
    let stored = stored_session(cx, session_id)?;
    let running = cx.is_running(session_id);
    let queue = if cx.resident_agent(session_id).is_some() {
        InboxState::recover(&stored.events)
            .map(|inbox| inbox.queue_snapshot())
            .map_err(|error| rpc_error("gateway/internal", error.to_string()))?
    } else {
        Value::Null
    };
    Ok(json!({
        "cursor": session_cursor(&stored),
        "active": running,
        "running": running,
        "queue": queue,
        "pendingInput": [],
    }))
}

/// `session/wait`: wait for quiescence without cancelling the running turn.
pub async fn wait_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let session_id = request_session_id(payload)?.to_string();
    let request = request_of(payload);
    let timeout_ms = request
        .get("timeoutMs")
        .and_then(Value::as_u64)
        .unwrap_or(30_000);
    if timeout_ms == 0 || timeout_ms > i32::MAX as u64 {
        return Err(rpc_error(
            "gateway/bad-request",
            "timeoutMs must be a positive supported timer delay",
        ));
    }
    let after_seq = request
        .get("afterSeq")
        .and_then(Value::as_i64)
        .unwrap_or(-1);
    if after_seq < -1 {
        return Err(rpc_error(
            "gateway/bad-request",
            "afterSeq must be an integer greater than or equal to -1",
        ));
    }
    let max_messages = request
        .get("maxMessages")
        .and_then(Value::as_u64)
        .unwrap_or(50);
    if max_messages == 0 {
        return Err(rpc_error(
            "gateway/bad-request",
            "maxMessages must be a positive safe integer",
        ));
    }
    let started = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);
    let (reason, state) = loop {
        let state = session_state(cx, &json!({"sessionId": session_id}))?;
        if !state["running"].as_bool().unwrap_or(false) {
            break ("idle", state);
        }
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            break ("timeout", state);
        }
        tokio::time::sleep((timeout - elapsed).min(std::time::Duration::from_millis(20))).await;
    };
    let stored = stored_session(cx, &session_id)?;
    let cursor = state["cursor"].as_i64().unwrap_or(session_cursor(&stored));
    let output = assistant_output(&stored, after_seq, cursor, max_messages as usize);
    Ok(json!({"reason": reason, "state": state, "output": output}))
}

fn assistant_output(
    stored: &Stored,
    after_seq: i64,
    through_seq: i64,
    max_messages: usize,
) -> Value {
    let mut records = Vec::new();
    if through_seq >= 0 {
        for index in (0..=through_seq as usize).rev() {
            if index as i64 <= after_seq {
                continue;
            }
            let Some(event) = stored.events.get(index) else {
                continue;
            };
            if event.kind != "assistant/message" || !is_append_surface(event) {
                continue;
            }
            if records.len() == max_messages {
                return json!({"records": records, "truncated": true});
            }
            records.push(json!({"type": "event", "event": wire_event(event, index)}));
        }
    }
    json!({"records": records, "truncated": false})
}

/// `session/projections`: exact values when present, null when the Session is absent.
pub fn session_projections(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let session_id = request_session_id(payload)?;
    let Ok(stored) = stored_session(cx, session_id) else {
        return Ok(Value::Null);
    };
    Ok(json!({
        "kind": "sequenced",
        "asOfSeq": stored.events.len(),
        "values": projection_values(&stored),
    }))
}

/// `session/search`: bounded literal search over current user and Assistant messages.
pub fn search_sessions(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let query = request_of(payload)
        .get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .ok_or_else(|| {
            rpc_error(
                "gateway/bad-request",
                "session search query must not be empty",
            )
        })?;
    if query.encode_utf16().count() > 500 {
        return Err(rpc_error(
            "gateway/bad-request",
            "session search query must contain at most 500 UTF-16 code units",
        ));
    }
    if query.contains('\0') {
        return Err(rpc_error(
            "gateway/bad-request",
            "session search query must not contain NUL",
        ));
    }
    let needle = query.to_lowercase();
    let mut items = Vec::new();
    for stored in cx.store().read_all() {
        let Some(snippet) = stored.events.iter().rev().find_map(|event| {
            if !matches!(event.kind.as_str(), "user/message" | "assistant/message")
                || !is_append_surface(event)
            {
                return None;
            }
            let text = message_text(event)?;
            text.to_lowercase()
                .contains(&needle)
                .then(|| search_snippet(&text, query))
        }) else {
            continue;
        };
        items.push(json!({"sessionId": stored.header.id, "snippet": snippet}));
        if items.len() > 20 {
            break;
        }
    }
    let has_more = items.len() > 20;
    items.truncate(20);
    Ok(json!({"items": items, "hasMore": has_more}))
}

fn message_text(event: &Event) -> Option<String> {
    let message = if event.kind == "assistant/message" {
        event.data.get("message")?
    } else {
        &event.data
    };
    let mut parts = Vec::new();
    for block in message.get("content")?.as_array()? {
        if block.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = block.get("text").and_then(Value::as_str) {
                parts.push(text);
            }
        }
    }
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn search_snippet(text: &str, query: &str) -> String {
    let normalized: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let chars: Vec<char> = normalized.chars().collect();
    if chars.is_empty() {
        return String::new();
    }
    let lower: String = normalized.chars().flat_map(char::to_lowercase).collect();
    let needle = query.to_lowercase();
    let match_byte = lower.find(&needle).unwrap_or(0);
    let match_char = lower[..match_byte].chars().count().min(chars.len());
    let start = match_char.saturating_sub(80);
    let end = (match_char + query.chars().count() + 80)
        .min(chars.len())
        .min(start + 240);
    chars[start..end].iter().collect()
}

fn normalize_session_title(input: &str, max_bytes: usize) -> String {
    let mut chars = input.chars().peekable();
    let mut cleaned = String::new();
    while let Some(ch) = chars.next() {
        match ch {
            '\u{1b}' => match chars.next() {
                Some('[') => consume_control_sequence(&mut chars, false),
                Some(']') => consume_control_sequence(&mut chars, true),
                Some(next) if ('@'..='_').contains(&next) => {}
                _ => {}
            },
            '\u{9b}' => consume_control_sequence(&mut chars, false),
            '\u{9d}' => consume_control_sequence(&mut chars, true),
            ch if ch.is_control() => {}
            '\u{200b}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206f}'
            | '\u{feff}' => {}
            ch if ch.is_whitespace() => {
                if !cleaned.ends_with(' ') {
                    cleaned.push(' ');
                }
            }
            ch => cleaned.push(ch),
        }
    }
    let mut used = 0usize;
    let mut out = String::new();
    for ch in cleaned.trim().chars() {
        let width = ch.len_utf8();
        if used + width > max_bytes {
            break;
        }
        out.push(ch);
        used += width;
    }
    out.trim_end().to_string()
}

fn consume_control_sequence(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, osc: bool) {
    while let Some(ch) = chars.next() {
        if osc && ch == '\u{7}' {
            break;
        }
        if osc && ch == '\u{1b}' && chars.peek() == Some(&'\\') {
            chars.next();
            break;
        }
        if !osc && ('@'..='~').contains(&ch) {
            break;
        }
    }
}

/// The Session a follow payload addresses, when it names one.
fn payload_session_id(payload: &Value) -> Option<String> {
    let args = payload.get("args").unwrap_or(payload);
    let request = args.get("request").unwrap_or(args);
    request
        .get("address")
        .and_then(|a| a.get("sessionId"))
        .or_else(|| request.get("sessionId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Frames published for one Session after `after_seq`. A subscriber that falls behind the
/// broadcast capacity loses the oldest frames rather than stalling the publisher; the
/// Session log remains the authority, so a client can re-open to recover a missed frame.
fn live_frames(
    receiver: broadcast::Receiver<Value>,
    session_id: Option<String>,
    after_seq: i64,
    assistant_after_revision: Option<u64>,
) -> BoxStream<'static, Result<Value, RpcError>> {
    use futures_util::StreamExt;
    futures_util::stream::unfold(
        (receiver, session_id, after_seq, assistant_after_revision),
        |(mut receiver, wanted, after, assistant_after)| async move {
            loop {
                match receiver.recv().await {
                    Ok(value) => {
                        let belongs = wanted.as_deref().is_none_or(|id| {
                            value.get("sessionId").and_then(Value::as_str) == Some(id)
                        });
                        if !belongs {
                            continue;
                        }
                        let Some(frame) = value.get("frame") else {
                            continue;
                        };
                        let visible = if frame.get("type").and_then(Value::as_str)
                            == Some("assistant-stream")
                        {
                            assistant_after.is_some_and(|revision| {
                                value
                                    .get("assistantRevision")
                                    .and_then(Value::as_u64)
                                    .is_some_and(|current| current > revision)
                            })
                        } else {
                            value.get("seq").and_then(Value::as_i64).unwrap_or(-1) > after
                        };
                        if visible {
                            return Some((
                                Ok(frame.clone()),
                                (receiver, wanted, after, assistant_after),
                            ));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    )
    .boxed()
}

fn projection_changes(before: &Value, after: &Value) -> Vec<Value> {
    after
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, value)| before.get(*key) != Some(*value))
        .map(|(key, value)| json!({"key": key, "val": value}))
        .collect()
}

fn publish_projection_change(
    cx: &ApiContext,
    session_id: &str,
    previous: &Mutex<Value>,
    seq: usize,
) {
    let Some(stored) = cx
        .store()
        .read_all()
        .into_iter()
        .find(|stored| stored.header.id == session_id)
    else {
        return;
    };
    let after = projection_values(&stored);
    let mut previous = previous.lock().unwrap();
    let changes = projection_changes(&previous, &after);
    *previous = after;
    if changes.is_empty() {
        return;
    }
    if let Some(control) = cx.control() {
        control.publish(json!({
            "type": "projection", "sessionId": session_id, "seq": seq, "changes": changes,
        }));
    }
}

fn control_baseline(cx: &ApiContext) -> Value {
    let projections: serde_json::Map<String, Value> = cx
        .store()
        .read_all()
        .into_iter()
        .map(|stored| {
            let values = projection_values(&stored);
            (
                stored.header.id,
                json!({"kind": "sequenced", "asOfSeq": stored.events.len(), "values": values}),
            )
        })
        .collect();
    json!({"type": "baseline", "value": {"projections": Value::Object(projections)}})
}

fn live_control_frames(
    receiver: broadcast::Receiver<Value>,
) -> BoxStream<'static, Result<Value, RpcError>> {
    use futures_util::StreamExt;
    futures_util::stream::unfold(receiver, |mut receiver| async move {
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

fn state_frames(
    cx: Arc<ApiContext>,
    session_id: String,
    state: Value,
) -> BoxStream<'static, Result<Value, RpcError>> {
    use futures_util::StreamExt;
    let updates = futures_util::stream::unfold(
        (cx, session_id, state.clone()),
        |(cx, session_id, mut last)| async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
            interval.tick().await;
            loop {
                interval.tick().await;
                let Ok(next) = session_state(&cx, &json!({"sessionId": session_id})) else {
                    return None;
                };
                if next != last {
                    last = next.clone();
                    return Some((Ok(next), (cx, session_id, last)));
                }
            }
        },
    );
    futures_util::stream::once(std::future::ready(Ok(state)))
        .chain(updates)
        .boxed()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptMode {
    Queue,
    Steer,
}

#[derive(Debug)]
struct PromptRequest {
    request_id: String,
    session_id: String,
    mode: PromptMode,
    content: Vec<Value>,
    client_time_zone: Option<String>,
}

fn rpc_error_details(code: &str, message: impl Into<String>, details: Value) -> RpcError {
    RpcError {
        details,
        ..rpc_error(code, message)
    }
}

fn required_string<'a>(object: &'a Value, field: &str) -> Result<&'a str, RpcError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            rpc_error(
                "gateway/arguments-invalid",
                format!("{field} must be a non-empty string"),
            )
        })
}

fn required_u64(object: &Value, field: &str) -> Result<u64, RpcError> {
    object.get(field).and_then(Value::as_u64).ok_or_else(|| {
        rpc_error(
            "gateway/arguments-invalid",
            format!("{field} must be a non-negative safe integer"),
        )
    })
}

/// The plain text of a prompt, joined from its text content parts.
fn prompt_text(parts: &[Value]) -> Option<String> {
    let mut text = String::new();
    for part in parts {
        if part.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        let Some(part) = part.get("text").and_then(Value::as_str) else {
            continue;
        };
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(part);
    }
    (!text.trim().is_empty()).then_some(text)
}

fn time_zone_shape(value: &str) -> bool {
    if value == "UTC" {
        return true;
    }
    let mut segments = value.split('/');
    let Some(first) = segments.next() else {
        return false;
    };
    if first.is_empty()
        || !first.as_bytes()[0].is_ascii_alphabetic()
        || !first
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'+' | b'-' | b'.'))
    {
        return false;
    }
    let mut count = 1;
    for segment in segments {
        if segment.is_empty()
            || !segment.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'+' | b'-' | b'.')
            })
        {
            return false;
        }
        count += 1;
    }
    count >= 2
}

fn canonical_time_zone(value: &str) -> Result<String, RpcError> {
    if !time_zone_shape(value) {
        return Err(rpc_error_details(
            "session/invalid-time-zone",
            "clientTimeZone must be UTC or a valid IANA Area/Location name",
            json!({ "value": value }),
        ));
    }
    match <chrono_tz::Tz as std::str::FromStr>::from_str(value) {
        Ok(zone) if zone.name() == "UTC" || time_zone_shape(zone.name()) => {
            Ok(zone.name().to_string())
        }
        _ => Err(rpc_error_details(
            "session/invalid-time-zone",
            "clientTimeZone must be UTC or a valid IANA Area/Location name",
            json!({ "value": value }),
        )),
    }
}

fn parse_prompt_request(payload: &Value) -> Result<PromptRequest, RpcError> {
    let args = payload.get("args").unwrap_or(payload);
    let request = args.get("request").unwrap_or(args);
    let request_id = required_string(request, "requestId")?.to_string();
    let session_id = required_string(request, "sessionId")?.to_string();
    let mode = match request.get("mode").and_then(Value::as_str) {
        Some("queue") => PromptMode::Queue,
        Some("steer") => PromptMode::Steer,
        _ => {
            return Err(rpc_error(
                "gateway/arguments-invalid",
                "mode must be queue or steer",
            ))
        }
    };
    let content = request
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| rpc_error("gateway/arguments-invalid", "content must be an array"))?
        .clone();
    let mut has_attachment = false;
    for (index, part) in content.iter().enumerate() {
        match part.get("type").and_then(Value::as_str) {
            Some("text") => {
                if part.get("text").and_then(Value::as_str).is_none() {
                    return Err(rpc_error(
                        "gateway/arguments-invalid",
                        format!("content[{index}].text must be a string"),
                    ));
                }
            }
            Some("image") => {
                let media_type = required_string(part, "mediaType").map_err(|error| {
                    rpc_error_details(
                        &error.code,
                        format!("content[{index}].mediaType must be a string"),
                        json!({}),
                    )
                })?;
                if !matches!(
                    media_type,
                    "image/png" | "image/jpeg" | "image/webp" | "image/gif"
                ) {
                    return Err(rpc_error_details(
                        "session/attachment-invalid",
                        "unsupported image media type",
                        json!({ "reason": "UNSUPPORTED_IMAGE_MEDIA_TYPE" }),
                    ));
                }
                required_string(part, "data").map_err(|error| {
                    rpc_error_details(
                        &error.code,
                        format!("content[{index}].data must be a non-empty string"),
                        json!({}),
                    )
                })?;
                if let Some(name) = part.get("name") {
                    if !name.is_string() {
                        return Err(rpc_error(
                            "gateway/arguments-invalid",
                            format!("content[{index}].name must be a string"),
                        ));
                    }
                }
                has_attachment = true;
            }
            Some("file") => {
                required_string(part, "receiptId").map_err(|error| {
                    rpc_error_details(
                        &error.code,
                        format!("content[{index}].receiptId must be a non-empty string"),
                        json!({}),
                    )
                })?;
                has_attachment = true;
            }
            _ => {
                return Err(rpc_error(
                    "gateway/arguments-invalid",
                    format!("content[{index}].type must be text, image, or file"),
                ))
            }
        }
    }
    let text = prompt_text(&content);
    if !content.is_empty() && text.is_none() && !has_attachment {
        return Err(rpc_error(
            "gateway/bad-request",
            "prompt content must include non-whitespace text or an attachment",
        ));
    }
    let client_time_zone = request
        .get("clientTimeZone")
        .map(|value| {
            value.as_str().ok_or_else(|| {
                rpc_error(
                    "gateway/arguments-invalid",
                    "clientTimeZone must be a string",
                )
            })
        })
        .transpose()?
        .map(canonical_time_zone)
        .transpose()?;
    Ok(PromptRequest {
        request_id,
        session_id,
        mode,
        content,
        client_time_zone,
    })
}

fn has_prompt_request(events: &[dsh_session_format::Event], request_id: &str) -> bool {
    let inherited_event_count = events
        .iter()
        .find(|event| event.kind == "session/end-seed")
        .and_then(|event| event.seq())
        .map_or(0, |seq| seq + 1);
    events.iter().any(|event| {
        if event.seq().is_some_and(|seq| seq < inherited_event_count) {
            return false;
        }
        let messages = match event.kind.as_str() {
            "agent/inbox/spliced" => event
                .data
                .get("inserted")
                .and_then(Value::as_array)
                .map(Vec::as_slice),
            "user/message" => Some(std::slice::from_ref(&event.data)),
            _ => None,
        };
        messages.is_some_and(|messages| {
            messages.iter().any(|message| {
                let source = message.get("source");
                matches!(
                    source
                        .and_then(|source| source.get("kind"))
                        .and_then(Value::as_str),
                    Some("user" | "retry")
                ) && source
                    .and_then(|source| source.get("rpcId"))
                    .and_then(Value::as_str)
                    == Some(request_id)
            })
        })
    })
}

/// `session/prompt`: durably admit one user message and drain the resident Agent's inbox.
pub async fn prompt(cx: Arc<ApiContext>, payload: Value) -> Result<Value, RpcError> {
    let request = parse_prompt_request(&payload)?;
    let session_id = request.session_id.as_str();
    let stored = stored_session(&cx, session_id)?;
    if has_prompt_request(&stored.events, &request.request_id) {
        return Ok(json!({"accepted": true}));
    }
    let inbox = InboxState::recover(&stored.events)
        .map_err(|error| rpc_error("gateway/internal", error.to_string()))?;
    if request.content.is_empty()
        && (request.mode != PromptMode::Queue
            || !continuation_eligible(&stored.events)
            || !inbox.next_turn.is_empty()
            || inbox
                .next_step
                .iter()
                .any(|message| message.source["kind"] == "user"))
    {
        return Err(continuation_unavailable());
    }
    let content = if request.content.is_empty() {
        Vec::new()
    } else {
        admit_prompt_content(&cx, &request.content)?
    };
    let cwd = stored.header.cwd.clone().unwrap_or_else(|| ".".to_string());
    let resident = ensure_resident_agent(&cx, session_id, &cwd).await?;
    let core_request = CorePromptRequest {
        request_id: request.request_id,
        content,
        client_time_zone: request.client_time_zone,
    };
    let target = match request.mode {
        PromptMode::Queue => InboxTarget::NextTurn,
        PromptMode::Steer => InboxTarget::NextStep,
    };
    loop {
        if let Some(cancel) = cx.claim_turn(session_id) {
            cx.emit("api-session/status", vec![json!(session_id), json!(true)]);
            let admitted = {
                let mut agent = resident.lock().await;
                match agent.submit_prompt(core_request.clone(), target) {
                    Ok(id) => id,
                    Err(error) => {
                        drop(agent);
                        cx.release_turn(session_id);
                        return Err(core_rpc_error(error));
                    }
                }
            };
            if admitted.is_none() {
                cx.release_turn(session_id);
                return Ok(json!({"accepted": true}));
            }
            let id = session_id.to_string();
            let session = cx.clone();
            tokio::spawn(async move {
                let outcome = run_resident(&session, id.clone(), resident, &cancel).await;
                session.release_turn(&id);
                if let Err(error) = outcome {
                    session.emit("api-session/error", vec![json!(id), json!(error)]);
                    eprintln!("[dsh-rs] turn failed for {id}: {error}");
                }
            });
            return Ok(json!({"accepted": true}));
        }
        let admission = resident.lock().await.admission();
        match admission.submit_prompt(core_request.clone(), target).await {
            Ok(_) => return Ok(json!({"accepted": true})),
            Err(dsh_core::CoreError::AdmissionUnavailable) => continue,
            Err(error) => return Err(core_rpc_error(error)),
        }
    }
}

/// `userQuestions/answer`: durably answer one continued timed question.
///
/// Only a call the `userQuestions` fold lists as `continued` is answerable, so a
/// plan review or an approval — which ask under their own tool names — can never
/// be answered through this path. The reply is steered into the Session as a
/// user message whose source names the call; that message is also the record
/// that closes the question in the fold.
/// @param cx - Host context that owns the resident Agent.
/// @param payload - RPC payload with the Session, call id, and answer batch.
/// @returns Whether the question was still continued.
pub(crate) async fn answer_question(
    cx: Arc<ApiContext>,
    payload: Value,
) -> Result<Value, RpcError> {
    let args = request_of(&payload);
    let session_id = args
        .get("sessionId")
        .and_then(Value::as_str)
        .or_else(|| args.get("agent").and_then(Value::as_str))
        .or_else(|| args.pointer("/agent/id").and_then(Value::as_str))
        .or_else(|| args.pointer("/agent/sessionId").and_then(Value::as_str))
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "agent must name the Session"))?
        .to_string();
    let call_id = args
        .get("callId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| rpc_error("gateway/bad-request", "callId must not be empty"))?;
    let answer = args
        .get("answer")
        .ok_or_else(|| rpc_error("gateway/bad-request", "answer is required"))?;
    let stored = stored_session(&cx, &session_id)?;
    let view = crate::user_questions::fold(&stored.events);
    let Some(questions) = crate::user_questions::answerable(&view, call_id) else {
        return Ok(json!(false));
    };
    let answers = answer
        .get("answers")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| bad_answer("answer.answers must be an array"))?;
    let mut answered = std::collections::HashSet::new();
    for item in &answers {
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_answer("every answer item needs an id"))?;
        if !answered.insert(id.to_string()) {
            return Err(bad_answer("each question may be answered once"));
        }
    }
    if answers.len() != questions.len()
        || !questions.iter().all(|question| {
            question
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| answered.contains(id))
        })
    {
        return Err(bad_answer(&format!(
            "the answer batch for {call_id} must name each of its {} questions exactly once",
            questions.len()
        )));
    }
    let inbox = InboxState::recover(&stored.events)
        .map_err(|error| rpc_error("gateway/internal", error.to_string()))?;
    let queued = |message: &dsh_core::InboxMessage| {
        message.source()["kind"] == json!("user-question-reply")
            && message.source()["callId"] == json!(call_id)
    };
    if inbox.next_turn.iter().any(queued) || inbox.next_step.iter().any(queued) {
        return Err(rpc_error_details(
            "userQuestions/reply-queued",
            "a reply is already queued for this question",
            json!({ "reason": "REPLY_QUEUED" }),
        ));
    }
    let (text, source) = crate::user_questions::reply_message(call_id, &questions, &answers);
    let input = UserInput::with_source(vec![Content::Text { text }], source);
    let cwd = stored.header.cwd.clone().unwrap_or_else(|| ".".to_string());
    let resident = ensure_resident_agent(&cx, &session_id, &cwd).await?;
    if let Some(cancel) = cx.claim_turn(&session_id) {
        cx.emit("api-session/status", vec![json!(session_id), json!(true)]);
        let admitted = {
            let mut agent = resident.lock().await;
            agent.enqueue(input, InboxTarget::NextStep)
        };
        if let Err(error) = admitted {
            cx.release_turn(&session_id);
            return Err(core_rpc_error(error));
        }
        let id = session_id.clone();
        let session = cx.clone();
        tokio::spawn(async move {
            let outcome = run_resident(&session, id.clone(), resident, &cancel).await;
            session.release_turn(&id);
            if let Err(error) = outcome {
                session.emit("api-session/error", vec![json!(id), json!(error)]);
                eprintln!("[dsh-rs] turn failed for {id}: {error}");
            }
        });
        return Ok(json!(true));
    }
    // A running turn drains next-step input at its next step.
    let mut agent = resident.lock().await;
    if let Err(error) = agent.enqueue(input, InboxTarget::NextStep) {
        return Err(core_rpc_error(error));
    }
    Ok(json!(true))
}

fn bad_answer(message: &str) -> RpcError {
    rpc_error_details(
        "userQuestions/bad-answer",
        message,
        json!({ "reason": "BAD_ANSWER" }),
    )
}

fn continuation_unavailable() -> RpcError {
    rpc_error_details(
        "session/agent-busy",
        "generation continuation is unavailable",
        json!({ "reason": "NO_CONTINUABLE_FAILURE_OR_PENDING_INPUT" }),
    )
}

fn continuation_eligible(events: &[Event]) -> bool {
    let mut admitted = false;
    let mut failed_attempt = false;
    let mut ending = false;
    for event in events {
        match event.kind.as_str() {
            "turn/start" => {
                admitted = false;
                failed_attempt = false;
                ending = false;
            }
            "step/start" | "assistant/message" => failed_attempt = false,
            "user/message" if is_append_surface(event) => {
                let source = event.data.get("source");
                let kind = source
                    .and_then(|source| source.get("kind"))
                    .and_then(Value::as_str);
                let empty = event
                    .data
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty);
                if !admitted && (kind == Some("user") && !empty || kind == Some("retry") && empty) {
                    admitted = true;
                }
            }
            "assistant/attempt" => failed_attempt = true,
            "turn/end" => {
                let continuable = event
                    .data
                    .get("reason")
                    .and_then(|reason| reason.get("kind"))
                    .and_then(Value::as_str)
                    == Some("error")
                    && event
                        .data
                        .pointer("/reason/error/code")
                        .and_then(Value::as_str)
                        .is_some_and(|code| {
                            matches!(
                                code,
                                "RATE_LIMIT"
                                    | "SERVER"
                                    | "TIMEOUT"
                                    | "TRANSPORT"
                                    | "EMPTY_RESPONSE"
                                    | "STREAM_CLOSED"
                            )
                        });
                ending = continuable && admitted && failed_attempt;
                admitted = false;
                failed_attempt = false;
            }
            _ => {}
        }
    }
    ending
}

fn core_rpc_error(error: dsh_core::CoreError) -> RpcError {
    match error {
        dsh_core::CoreError::AdmissionUnavailable => rpc_error(
            "session/busy",
            "the Session's Agent is changing execution state",
        ),
        dsh_core::CoreError::AdmissionQueueFull => rpc_error_details(
            "session/busy",
            "the Session's admission queue is full",
            json!({ "reason": "ADMISSION_QUEUE_FULL" }),
        ),
        other => rpc_error("session/agent-busy", other.to_string()),
    }
}

async fn ensure_resident_agent(
    cx: &ApiContext,
    session_id: &str,
    cwd: &str,
) -> Result<Arc<AsyncMutex<Agent>>, RpcError> {
    if let Some(agent) = cx.resident_agent(session_id) {
        return Ok(agent);
    }
    if let Some(agent) = cx.agents.lock().unwrap().get(session_id) {
        return Ok(Arc::clone(agent));
    }
    let session = cx
        .store()
        .open(Some(cwd), session_id)
        .map_err(|error| rpc_error("session/not-found", error.to_string()))?;
    let selection = session
        .events
        .iter()
        .rev()
        .find(|event| event.kind == "model/selection")
        .map(|event| event.data.clone());
    let defaults = cx.setting_value("agent-default-model");
    let selected_provider = selection
        .as_ref()
        .and_then(|selection| selection.get("provider"))
        .and_then(Value::as_str)
        .or_else(|| defaults["provider"].as_str())
        .unwrap_or("deepseek-official");
    let selected_model = selection
        .as_ref()
        .and_then(|selection| selection.get("model"))
        .and_then(Value::as_str)
        .or_else(|| defaults["model"].as_str())
        .filter(|model| !model.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            rpc_error(
                "session/model-unavailable",
                "the selected provider has no default model",
            )
        })?;
    let effort = selection
        .as_ref()
        .and_then(|selection| selection.get("reasoningEffort"))
        .and_then(Value::as_str)
        .or_else(|| defaults["reasoningEffort"].as_str());
    let preset = selected_agent_preset(&session);
    let preset_tools = dsh_tools::ToolPreset::parse(&preset).ok_or_else(|| {
        rpc_error_details(
            "agent-preset/not-found",
            format!("Agent preset {preset:?} is unavailable in the Rust Host"),
            json!({"agentPreset": preset, "available": crate::catalog_api::supported_presets()}),
        )
    })?;
    let attachments = Some(cx.attachments()?);
    let route = build_model_route(
        cx,
        selected_provider,
        &selected_model,
        effort,
        session_id,
        attachments,
    )?;
    let model = crate::switchable_model(route);
    let session_facts = Some(cx.session_facts(session_id));
    let mcp = cx.mcp_host().await;
    let subagent: Option<Arc<dyn dsh_tools::SubagentRunner>> =
        Some(Arc::new(crate::HostSubagentRunner::new(
            Arc::clone(&model),
            cx.home.join("sessions"),
            PathBuf::from(&cwd),
            session_id,
            session.header.delegation_depth,
        )));
    let agent = crate::try_build_agent_with_model(
        model,
        session,
        PathBuf::from(cwd),
        preset_tools,
        session_facts,
        mcp.as_deref(),
        subagent,
    )
    .map_err(|error| rpc_error("session/agent-busy", error.to_string()))?;
    let agent = Arc::new(AsyncMutex::new(agent));
    cx.agents
        .lock()
        .unwrap()
        .insert(session_id.to_string(), Arc::clone(&agent));
    Ok(agent)
}

fn build_model_route(
    cx: &ApiContext,
    provider: &str,
    model: &str,
    effort: Option<&str>,
    session_id: &str,
    attachments: Option<AttachmentStore>,
) -> Result<Arc<dyn dsh_core::Model>, RpcError> {
    let effort = match effort {
        None => None,
        Some("low") => Some(Effort::Low),
        Some("high") => Some(Effort::High),
        Some("max") => Some(Effort::Max),
        Some(value) => {
            return Err(rpc_error(
                "session/model-unavailable",
                format!("unsupported reasoning effort {value:?}"),
            ))
        }
    };
    if provider == "deepseek-official" {
        let key = cx.credential_value("DEEPSEEK_API_KEY").ok_or_else(|| {
            rpc_error(
                "credentials/unavailable",
                "no DeepSeek credential: configure DEEPSEEK_API_KEY",
            )
        })?;
        return Ok(crate::deepseek_model(
            crate::connection_from_env(key),
            session_id,
            model.to_owned(),
            effort,
            attachments,
        ));
    }

    let settings = cx.setting_value("llm-pi-ai");
    let profile = crate::model_routes::providers(&settings)
        .remove(provider)
        .ok_or_else(|| {
            rpc_error(
                "session/model-unavailable",
                format!("provider {provider:?} is not configured in this Host"),
            )
        })?;
    if profile.api.as_deref() != Some("openai-completions") {
        return Err(rpc_error(
            "session/model-unavailable",
            format!("provider {provider:?} does not use a supported chat protocol"),
        ));
    }
    let model_profile = profile.model(model).cloned().ok_or_else(|| {
        rpc_error(
            "session/model-unavailable",
            format!("model {model:?} is not configured for provider {provider:?}"),
        )
    })?;
    let reference = profile.api_key_env.as_deref().ok_or_else(|| {
        rpc_error(
            "credentials/unavailable",
            format!("provider {provider:?} has no configured credential reference"),
        )
    })?;
    let key = cx.credential_value(reference).ok_or_else(|| {
        rpc_error(
            "credentials/unavailable",
            format!("no credential is configured for provider {provider:?}"),
        )
    })?;
    let model = crate::openai_model::OpenAiModel::new(
        provider,
        model,
        key,
        profile,
        &model_profile,
        effort.map(|effort| {
            match effort {
                Effort::Low => "low",
                Effort::High => "high",
                Effort::Max => "max",
            }
            .to_owned()
        }),
        attachments,
    )
    .map_err(|error| rpc_error("session/model-unavailable", error.to_string()))?;
    Ok(Arc::new(model))
}

/// Resolve the latest durable preset selection, falling back to the Session header.
pub(crate) fn selected_agent_preset(session: &Session) -> String {
    selected_agent_preset_from_events(&session.header.agent_preset, &session.events)
}

fn selected_agent_preset_from_events(header: &Option<String>, events: &[Event]) -> String {
    events
        .iter()
        .rev()
        .find(|event| event.kind == "agent-preset/selected")
        .and_then(|event| event.data.get("agentPreset"))
        .and_then(Value::as_str)
        .or(header.as_deref())
        .unwrap_or(crate::catalog_api::STANDARD_PRESET)
        .to_owned()
}

fn admit_prompt_content(cx: &ApiContext, content: &[Value]) -> Result<Vec<Content>, RpcError> {
    let parts = content
        .iter()
        .map(|part| {
            let value = match part.get("type").and_then(Value::as_str) {
                Some("text") => PromptContentPart::Text {
                    text: part.get("text").and_then(Value::as_str).unwrap_or_default(),
                },
                Some("image") => PromptContentPart::Image {
                    media_type: match part.get("mediaType").and_then(Value::as_str) {
                        Some("image/png") => StoredImageMediaType::Png,
                        Some("image/jpeg") => StoredImageMediaType::Jpeg,
                        Some("image/webp") => StoredImageMediaType::Webp,
                        Some("image/gif") => StoredImageMediaType::Gif,
                        _ => {
                            return Err(attachment_rpc_error(
                                AttachmentError::UnsupportedImageType {
                                    media_type: "unsupported".to_string(),
                                },
                            ))
                        }
                    },
                    data: part.get("data").and_then(Value::as_str).unwrap_or_default(),
                    name: part.get("name").and_then(Value::as_str),
                },
                Some("file") => PromptContentPart::FileReceipt {
                    receipt_id: part
                        .get("receiptId")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                },
                _ => {
                    return Err(rpc_error(
                        "gateway/arguments-invalid",
                        "unsupported prompt content",
                    ))
                }
            };
            Ok(value)
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    cx.attachments()?
        .admit_prompt_content(&parts)
        .map_err(attachment_rpc_error)?
        .into_iter()
        .map(|part| match part {
            AdmittedPromptContentPart::Text { text } => Ok(Content::Text { text }),
            AdmittedPromptContentPart::Image { attachment } => Ok(Content::Image {
                attachment: dsh_llm::ImageAttachmentRef {
                    attachment_id: attachment.attachment_id,
                    media_type: match attachment.media_type {
                        StoredImageMediaType::Png => dsh_llm::ImageMediaType::Png,
                        StoredImageMediaType::Jpeg => dsh_llm::ImageMediaType::Jpeg,
                        StoredImageMediaType::Webp => dsh_llm::ImageMediaType::Webp,
                        StoredImageMediaType::Gif => dsh_llm::ImageMediaType::Gif,
                    },
                    bytes: attachment.bytes,
                    width: u64::from(attachment.width),
                    height: u64::from(attachment.height),
                    name: attachment.name,
                    original_dimensions: None,
                },
                offloaded: false,
            }),
            AdmittedPromptContentPart::File { attachment } => Ok(Content::File {
                attachment: dsh_llm::FileAttachmentRef {
                    attachment_id: attachment.attachment_id,
                    name: attachment.name,
                    bytes: attachment.bytes,
                },
            }),
        })
        .collect()
}

/// Apply the plan-mode state the next request runs under.
///
/// A selection queued during the previous turn is already the selected state,
/// so the guidance follows it before the durable commit lands.
/// @param cx - running Host.
/// @param session_id - owning Session.
/// @param agent - quiescent Agent holding the Session write lease.
fn refresh_plan_mode(cx: &ApiContext, session_id: &str, agent: &mut Agent) {
    let state = dsh_planning::fold_plan(&agent.session().events);
    let pending = cx.plan_pending(session_id);
    agent.set_prompt_extra(crate::plan::plan_guidance(&state, pending));
}

/// Commit a plan-mode selection that waited for the turn boundary.
/// @param cx - running Host.
/// @param session_id - owning Session.
/// @param agent - quiescent Agent holding the Session write lease.
fn commit_plan_pending(cx: &ApiContext, session_id: &str, agent: &mut Agent) {
    let Some(pending) = cx.take_plan_pending(session_id) else {
        return;
    };
    if dsh_planning::fold_plan(&agent.session().events).active == pending.active {
        return;
    }
    // The selection was queued while a turn held the write lease, so its
    // command rows land here, before the mode change they belong to.
    let command = json!({
        "commandId": pending.command_id,
        "name": "plan",
        "args": pending.args,
        "source": {"kind": "user"},
    });
    if agent.record_session_event("command/run", command).is_err() {
        return;
    }
    let done = json!({
        "commandId": pending.command_id,
        "kind": "success",
        "text": pending.text,
    });
    if agent.record_session_event("command/done", done).is_err() {
        return;
    }
    if agent
        .record_session_event("plan/mode", json!({"active": pending.active}))
        .is_ok()
    {
        let _ =
            agent.record_session_event("user/message", crate::plan::plan_narration(pending.active));
    }
}

/// Run all durable pending turns, publishing each committed event as it lands.
async fn run_resident(
    cx: &Arc<ApiContext>,
    session_id: String,
    resident: Arc<AsyncMutex<Agent>>,
    cancel: &Cancel,
) -> Result<(), String> {
    let mut agent = resident.lock().await;
    let initial_projection = projection_values(&Stored {
        header: agent.session().header.clone(),
        events: agent.session().events.clone(),
        modified_ms: 0,
    });
    let durable_cursor = Arc::new(Mutex::new(
        agent.session().events.len().saturating_sub(1) as i64
    ));
    let follow = cx.follow();
    let publishing = session_id.clone();
    let publishing_cx = cx.clone();
    let projection = Mutex::new(initial_projection);
    let cursor = durable_cursor;
    let _listener = agent.bus.on(move |event| {
        let AgentEvent::AssistantStream(frame) = event else {
            let AgentEvent::Logged(logged) = event else {
                return;
            };
            let index = logged.seq().unwrap_or(0) as usize;
            *cursor.lock().unwrap() = index as i64;
            if let Some(broadcaster) = &follow {
                broadcaster.publish(json!({
                    "sessionId": publishing,
                    "seq": index,
                    "frame": {"type": "event", "event": wire_event(logged, index)},
                }));
            }
            if logged.kind == "user/message"
                && logged
                    .data
                    .get("source")
                    .and_then(|source| source.get("kind"))
                    .and_then(Value::as_str)
                    == Some("user")
            {
                publishing_cx.emit(
                    "api-session/activity",
                    vec![json!(publishing), json!(logged.time().unwrap_or(0))],
                );
            }
            note_goal_change(&publishing_cx, &publishing, logged);
            publish_projection_change(&publishing_cx, &publishing, &projection, index);
            return;
        };
        let cursor = *cursor.lock().unwrap();
        let wire = serde_json::to_value(frame).expect("assistant stream frame serializes");
        let revision = wire.get("revision").and_then(Value::as_u64).unwrap_or(0);
        publishing_cx.accept_assistant_stream(&publishing, frame.clone(), cursor);
        if let Some(broadcaster) = &follow {
            broadcaster.publish(json!({
                "sessionId": publishing,
                "seq": cursor,
                "assistantRevision": revision,
                "frame": {"type": "assistant-stream", "frame": wire},
            }));
        }
    });
    // Plan guidance and Goal continuation both follow durable state between
    // turns: the guidance refreshes before each turn, and a selection made
    // during a turn commits once the turn boundary is reached.
    let mut ends = Vec::new();
    loop {
        commit_plan_pending(cx, &session_id, &mut agent);
        refresh_plan_mode(cx, &session_id, &mut agent);
        let round_ends = agent
            .run_pending(cancel)
            .await
            .map_err(|error| error.to_string())?;
        let stopped = round_ends.iter().any(|end| {
            matches!(
                end,
                TurnEnd::Aborted | TurnEnd::Error(_) | TurnEnd::StepLimit
            )
        });
        ends.extend(round_ends);
        if stopped || cancel.is_cancelled() || !drive_goal_round(cx, &session_id, &mut agent) {
            break;
        }
    }
    commit_plan_pending(cx, &session_id, &mut agent);
    drop(agent);
    ends.into_iter()
        .find_map(|end| match end {
            TurnEnd::Error(error) => Some(error.to_string()),
            TurnEnd::StepLimit => Some("step limit reached".to_string()),
            _ => None,
        })
        .map_or(Ok(()), Err)
}

/// `session/updateQueue`: edit, remove, or promote one durable pending message.
pub async fn update_queue(cx: &ApiContext, payload: Value) -> Result<Value, RpcError> {
    let request = request_of(&payload);
    let session_id = required_string(request, "sessionId")?;
    let item_id = required_string(request, "itemId")?;
    let action = request
        .get("action")
        .ok_or_else(|| rpc_error("gateway/bad-request", "action is required"))?;
    let kind = action.get("kind").and_then(Value::as_str).ok_or_else(|| {
        rpc_error(
            "gateway/arguments-invalid",
            "action.kind must be edit, remove, or steer",
        )
    })?;
    let stored = stored_session(cx, session_id)?;
    let inbox = InboxState::recover(&stored.events)
        .map_err(|error| rpc_error("gateway/internal", error.to_string()))?;
    let queued = inbox
        .queue_snapshot()
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item["id"].as_str() == Some(item_id))
                .cloned()
        })
        .ok_or_else(|| rpc_error("session/queue-item-not-found", "queue item was not found"))?;
    let cwd = stored.header.cwd.clone().unwrap_or_else(|| ".".to_string());
    let resident = ensure_resident_agent(cx, session_id, &cwd).await?;
    let running = cx.is_running(session_id);
    if kind == "steer" {
        if !running || queued["placement"].as_str() != Some("queued") {
            return Err(rpc_error(
                "session/steer-unavailable",
                "queued item cannot be steered",
            ));
        }
        let admission = resident.lock().await.admission();
        let moved = admission.steer(item_id).await.map_err(core_rpc_error)?;
        if moved.is_none() {
            return Err(rpc_error(
                "session/queue-item-not-found",
                "queue item was not found",
            ));
        }
    } else {
        let content = if kind == "edit" {
            let parts = action
                .get("content")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    rpc_error("gateway/arguments-invalid", "edit content must be an array")
                })?;
            if parts
                .iter()
                .any(|part| part.get("type").and_then(Value::as_str) != Some("text"))
            {
                return Err(rpc_error_details(
                    "session/attachment-invalid",
                    "queue edits accept text content only",
                    json!({ "reason": "QUEUE_EDIT_NON_TEXT" }),
                ));
            }
            if parts.is_empty()
                || !parts.iter().any(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.trim().is_empty())
                })
            {
                return Err(rpc_error(
                    "gateway/bad-request",
                    "queue edit content must include non-whitespace text",
                ));
            }
            parts
                .iter()
                .map(|part| Content::Text {
                    text: part
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                })
                .collect()
        } else if kind == "remove" {
            Vec::new()
        } else {
            return Err(rpc_error(
                "gateway/arguments-invalid",
                "action.kind must be edit, remove, or steer",
            ));
        };
        let changed = if running {
            let admission = resident.lock().await.admission();
            if kind == "edit" {
                admission
                    .replace(item_id, content)
                    .await
                    .map_err(core_rpc_error)?
            } else {
                admission.remove(item_id).await.map_err(core_rpc_error)?
            }
        } else {
            let mut agent = resident.lock().await;
            if kind == "edit" {
                agent.replace(item_id, content).map_err(core_rpc_error)?
            } else {
                agent.remove(item_id).map_err(core_rpc_error)?
            }
        };
        if !changed {
            return Err(rpc_error(
                "session/queue-item-not-found",
                "queue item was not found",
            ));
        }
    }
    Ok(json!({"accepted": true}))
}

/// `session/attachment`: read a supplied durable image reference within the Session scope.
pub fn attachment(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    required_string(request, "sessionId")?;
    let reference = request
        .get("attachment")
        .ok_or_else(|| rpc_error("gateway/arguments-invalid", "attachment is required"))?;
    let media_type = match required_string(reference, "mediaType")? {
        "image/png" => StoredImageMediaType::Png,
        "image/jpeg" => StoredImageMediaType::Jpeg,
        "image/webp" => StoredImageMediaType::Webp,
        "image/gif" => StoredImageMediaType::Gif,
        _ => {
            return Err(attachment_rpc_error(
                AttachmentError::UnsupportedImageType {
                    media_type: "unsupported".to_string(),
                },
            ))
        }
    };
    let reference = StoredImageRef {
        attachment_id: required_string(reference, "attachmentId")?.to_string(),
        media_type,
        bytes: required_u64(reference, "bytes")?,
        width: u32::try_from(required_u64(reference, "width")?)
            .map_err(|_| attachment_rpc_error(AttachmentError::InvalidAttachmentRef))?,
        height: u32::try_from(required_u64(reference, "height")?)
            .map_err(|_| attachment_rpc_error(AttachmentError::InvalidAttachmentRef))?,
        name: reference
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string),
        original_dimensions: reference.get("originalDimensions").and_then(|dimensions| {
            Some(StoredImageDimensions {
                width: u32::try_from(dimensions.get("width")?.as_u64()?).ok()?,
                height: u32::try_from(dimensions.get("height")?.as_u64()?).ok()?,
            })
        }),
    };
    let stored = cx
        .attachments()?
        .read_image(&reference)
        .map_err(attachment_rpc_error)?;
    let value = serde_json::to_value(&stored.reference).expect("attachment reference serializes");
    Ok(json!({
        "attachment": value,
        "data": base64::engine::general_purpose::STANDARD.encode(stored.data),
    }))
}

/// `session/cancel`: stop the Session's running turn and settle it as aborted.
pub fn cancel_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let session_id = request
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| rpc_error("gateway/bad-request", "session/cancel requires sessionId"))?;
    cx.cancel_turn(session_id);
    Ok(json!({"accepted": true}))
}

/// The account's home directory, the listing root when a request names no path.
pub(crate) fn home_dir() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Child directories reported for one level before the level is marked truncated.
const DIRECTORY_ENTRY_LIMIT: usize = 1000;

/// The argument object of an RPC payload, whichever envelope shape it uses.
pub(crate) fn request_of(payload: &Value) -> &Value {
    let args = payload.get("args").unwrap_or(payload);
    args.get("request").unwrap_or(args)
}

fn relative_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', ':', '\0'])
}

/// `directoryPicker/list`: one directory level plus its ancestry, for the browse picker.
pub fn list_directory(payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let requested = request
        .get("path")
        .and_then(Value::as_str)
        .map(str::to_string);
    let path = match requested {
        Some(path) => PathBuf::from(path),
        None => home_dir(),
    };
    if !path.is_dir() {
        return Err(rpc_error(
            "directory-picker/not-found",
            format!("{} is not a directory", path.display()),
        ));
    }

    // Ancestors run from this directory up to the root; crumbs read root first.
    let mut chain: Vec<PathBuf> = path.ancestors().map(Path::to_path_buf).collect();
    chain.reverse();
    let crumbs: Vec<Value> = chain
        .iter()
        .map(|crumb| {
            let name = crumb.file_name().map_or_else(
                || crumb.display().to_string(),
                |n| n.to_string_lossy().into_owned(),
            );
            json!({"name": name, "path": crumb.display().to_string(), "hidden": false})
        })
        .collect();

    let mut children: Vec<(String, PathBuf)> = Vec::new();
    let listing = std::fs::read_dir(&path)
        .map_err(|error| rpc_error("directory-picker/io", error.to_string()))?;
    for entry in listing {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        // Follows symlinks, so a link to a directory is offered like the directory itself.
        if !entry.path().is_dir() {
            continue;
        }
        children.push((
            entry.file_name().to_string_lossy().into_owned(),
            entry.path(),
        ));
    }
    children.sort_by(|a, b| a.0.cmp(&b.0));
    let truncated = children.len() > DIRECTORY_ENTRY_LIMIT;
    children.truncate(DIRECTORY_ENTRY_LIMIT);
    let entries: Vec<Value> = children
        .iter()
        .map(|(name, child)| json!({"name": name, "path": child.display().to_string(), "hidden": name.starts_with('.')}))
        .collect();

    Ok(json!({
        // Report the normalized path: crumbs and entries use it too, so a client can compare
        // them, and a request written with the other separator style still matches.
        "path": path.display().to_string(),
        "home": home_dir().display().to_string(),
        "crumbs": crumbs,
        "entries": entries,
        "truncated": truncated,
    }))
}

/// `directoryPicker/createDirectory`: make one child directory and return its absolute path.
pub fn create_directory(payload: &Value) -> Result<Value, RpcError> {
    let request = request_of(payload);
    let parent = request.get("path").and_then(Value::as_str).ok_or_else(|| {
        rpc_error(
            "gateway/bad-request",
            "directoryPicker/createDirectory requires path",
        )
    })?;
    let name = request.get("name").and_then(Value::as_str).ok_or_else(|| {
        rpc_error(
            "gateway/bad-request",
            "directoryPicker/createDirectory requires name",
        )
    })?;
    if !relative_name(name) {
        return Err(rpc_error(
            "gateway/bad-request",
            format!("\"{name}\" is not a single directory name"),
        ));
    }
    let parent = PathBuf::from(parent);
    if !parent.is_dir() {
        return Err(rpc_error(
            "directory-picker/not-found",
            format!("{} is not a directory", parent.display()),
        ));
    }
    let created = parent.join(name);
    std::fs::create_dir(&created)
        .map_err(|error| rpc_error("directory-picker/io", error.to_string()))?;
    Ok(json!(created.display().to_string()))
}

pub(crate) fn random_uuid() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut hex = String::new();
    for _ in 0..2 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        hex.push_str(&format!("{:016x}", h.finish()));
    }
    format!(
        "{}-{}-4{}-a{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[13..16],
        &hex[17..20],
        &hex[20..32]
    )
}

/// Civil date from days since the Unix epoch (Howard Hinnant's algorithm).
fn iso8601(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms % 1000
    )
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The workspace a first-run UI starts in: `<Documents>/deepseek-harness/default-workspace`.
pub(crate) fn default_workspace_path(home: &Path) -> PathBuf {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(|h| {
            PathBuf::from(h)
                .join("Documents")
                .join("deepseek-harness")
                .join("default-workspace")
        })
        .unwrap_or_else(|| home.join("default-workspace"))
}

pub(crate) fn rpc_error(code: &str, message: impl Into<String>) -> RpcError {
    RpcError::new(code, message)
}

/// Append an event through a resident Agent when it owns the Session lease.
pub(crate) async fn append_host_event(
    cx: &ApiContext,
    session_id: &str,
    kind: &str,
    data: Value,
) -> Result<(Event, usize), RpcError> {
    if let Some(resident) = cx.resident_agent(session_id) {
        let mut agent = resident.lock().await;
        let before = projection_values(&Stored {
            header: agent.session().header.clone(),
            events: agent.session().events.clone(),
            modified_ms: 0,
        });
        let (event, seq) = agent
            .record_session_event(kind, data)
            .map_err(|error| rpc_error("session/io", error.to_string()))?;
        let after = projection_values(&Stored {
            header: agent.session().header.clone(),
            events: agent.session().events.clone(),
            modified_ms: 0,
        });
        publish_session_event(cx, session_id, &event, seq, &before, &after);
        return Ok((event, seq));
    }
    append_session_event(cx, session_id, kind, data)
}

/// Preset a Session runs when the request names none.
fn default_preset() -> String {
    crate::catalog_api::STANDARD_PRESET.to_owned()
}

/// `session/create`: create, or reuse the blank Session, a new conversation runs in.
///
/// `workspaceId` and `cwd` are exclusive. A requested `sessionId` is reused only while the
/// stored Session holds no conversation; otherwise the new Session takes a fresh identity,
/// so the returned id can differ from the requested one.
pub fn create_session(cx: &ApiContext, payload: &Value) -> Result<Value, RpcError> {
    let args = payload.get("args").unwrap_or(payload);
    let request = args.get("request").unwrap_or(args);
    let workspace_id = request.get("workspaceId").and_then(Value::as_str);
    let cwd_argument = request.get("cwd").and_then(Value::as_str);
    if workspace_id.is_some() && cwd_argument.is_some() {
        return Err(rpc_error(
            "gateway/bad-request",
            "session.create accepts workspaceId or cwd, not both",
        ));
    }

    let cwd = match workspace_id {
        Some(wanted) => list_workspaces(cx)
            .into_iter()
            .find(|w| w.get("workspaceId").and_then(Value::as_str) == Some(wanted))
            .and_then(|w| w.get("path").and_then(Value::as_str).map(str::to_string))
            .ok_or_else(|| {
                rpc_error(
                    "workspace/not-found",
                    format!("workspace \"{wanted}\" not found"),
                )
            })?,
        None => match cwd_argument {
            Some(cwd) => cwd.to_string(),
            None => initialize_default(cx)?["path"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        },
    };

    let requested_preset = request
        .get("agentPreset")
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(preset) = requested_preset.as_deref() {
        if !crate::catalog_api::supports_preset(preset) {
            return Err(rpc_error_details(
                "agent-preset/not-found",
                format!("Agent preset {preset:?} is unavailable in the Rust Host"),
                json!({"agentPreset": preset, "available": crate::catalog_api::supported_presets()}),
            ));
        }
    }
    let store = cx.store();
    let mut id = format!("session-{}", random_uuid());

    if let Some(requested) = request.get("sessionId").and_then(Value::as_str) {
        match store.open(Some(&cwd), requested) {
            Ok(mut existing) if !existing.events.iter().any(|e| is_user_message(&e.kind)) => {
                let current = selected_agent_preset(&existing);
                let preset = requested_preset.unwrap_or(current.clone());
                if preset != current {
                    let before = projection_values(&Stored {
                        header: existing.header.clone(),
                        events: existing.events.clone(),
                        modified_ms: 0,
                    });
                    let seq = existing.events.len() as u64;
                    let event = stored_event(
                        "agent-preset/selected",
                        json!({"agentPreset": preset}),
                        seq,
                        now_ms(),
                    );
                    existing
                        .append(std::slice::from_ref(&event))
                        .map_err(|error| rpc_error("session/io", error.to_string()))?;
                    let after = projection_values(&Stored {
                        header: existing.header.clone(),
                        events: existing.events.clone(),
                        modified_ms: 0,
                    });
                    publish_session_event(cx, requested, &event, seq as usize, &before, &after);
                    cx.emit(
                        "agent-preset/selected",
                        vec![json!(requested), json!(preset)],
                    );
                }
                publish_session_added(cx, &existing);
                return Ok(json!({"sessionId": requested, "agentPreset": preset}));
            }
            // It already holds a conversation, so it is not reusable.
            Ok(_) => {}
            Err(StoreError::NotFound(_)) => id = requested.to_string(),
            Err(error) => return Err(rpc_error("session/io", error.to_string())),
        }
    }

    let preset = requested_preset.unwrap_or_else(default_preset);
    let session = store
        .create(Header {
            kind: HeaderTag::Session,
            version: SESSION_FORMAT_VERSION,
            id: id.clone(),
            created_at: now_ms(),
            cwd: Some(cwd),
            parent_session: None,
            is_seeded: false,
            origin: None,
            delegation_depth: 0,
            agent_preset: Some(preset.clone()),
        })
        .map_err(|error| match error {
            StoreError::Exists(_) => {
                rpc_error("session/exists", format!("session \"{id}\" already exists"))
            }
            other => rpc_error("session/io", other.to_string()),
        })?;
    publish_session_added(cx, &session);
    Ok(json!({"sessionId": id, "agentPreset": preset}))
}

fn publish_session_added(cx: &ApiContext, session: &Session) {
    let stored = Stored {
        header: session.header.clone(),
        events: session.events.clone(),
        modified_ms: now_ms(),
    };
    let rows = session_items_with_running(&[stored], &HashMap::new());
    if let Some(row) = rows.into_iter().next() {
        cx.emit("api-session/added", vec![row]);
    }
}

/// Every stored workspace with the Sessions whose working directory is its path.
pub fn list_workspaces(cx: &ApiContext) -> Vec<Value> {
    let file = cx.home.join("storages").join("rust-workspaces.json");
    let doc: Value = std::fs::read_to_string(file)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_else(|| json!({"workspaces": []}));
    let sessions = cx.store().read_all();
    doc["workspaces"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|w| {
            let mut w = w.clone();
            let ids: Vec<&str> = sessions
                .iter()
                .filter(|s| s.header.cwd.as_deref() == w["path"].as_str())
                .map(|s| s.header.id.as_str())
                .collect();
            w["sessionIds"] = json!(ids);
            w
        })
        .collect()
}

/// Create (once) and describe the default workspace; its Sessions are those whose
/// working directory is the workspace path.
pub fn initialize_default(cx: &ApiContext) -> Result<Value, RpcError> {
    let _guard = cx.workspaces.lock().unwrap();
    let path = default_workspace_path(&cx.home);
    std::fs::create_dir_all(&path).map_err(|e| {
        rpc_error(
            "workspace/io",
            format!("cannot create {}: {e}", path.display()),
        )
    })?;
    let file = cx.home.join("storages").join("rust-workspaces.json");
    let mut doc: Value = std::fs::read_to_string(&file)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_else(|| json!({"workspaces": []}));
    let path_str = path.display().to_string();
    let existing = doc["workspaces"]
        .as_array()
        .and_then(|a| a.iter().find(|w| w["path"] == path_str))
        .cloned();
    let mut ws = match existing {
        Some(w) => w,
        None => {
            let now = iso8601(now_ms());
            let w = json!({
                "workspaceId": random_uuid(),
                "path": path_str,
                "title": path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default(),
                "createdAt": now,
                "updatedAt": now
            });
            doc["workspaces"]
                .as_array_mut()
                .expect("array")
                .push(w.clone());
            if let Some(dir) = file.parent() {
                std::fs::create_dir_all(dir)
                    .map_err(|e| rpc_error("workspace/io", e.to_string()))?;
            }
            std::fs::write(
                &file,
                serde_json::to_string_pretty(&doc).expect("serializes"),
            )
            .map_err(|e| rpc_error("workspace/io", e.to_string()))?;
            w
        }
    };
    let ids: Vec<String> = cx
        .store()
        .read_all()
        .into_iter()
        .filter(|s| s.header.cwd.as_deref() == Some(path_str.as_str()))
        .map(|s| s.header.id)
        .collect();
    ws["sessionIds"] = json!(ids);
    Ok(ws)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_responses_parse() {
        for text in [
            include_str!("../assets/rpc/permissionPresets_catalog.json"),
            include_str!("../assets/rpc/agentPresets_list.json"),
            include_str!("../assets/rpc/web_listProviders.json"),
            include_str!("../assets/rpc/settings_describe.json"),
            include_str!("../assets/rpc/dynamicCordisRunner_inventory.json"),
            include_str!("../assets/rpc/dynamicCordisRunner_syncInspectManifest.json"),
            include_str!("../assets/rpc/session_modelCatalog.json"),
            include_str!("../assets/rpc/llm_listProviders.json"),
            include_str!("../assets/rpc/llm_listConfigurableProviders.json"),
        ] {
            serde_json::from_str::<Value>(text).unwrap();
        }
    }

    #[test]
    fn host_store_finds_plain_and_compressed_sessions() {
        use dsh_session_persistence::Compression;
        let (home, _) = home("mixed-store");
        let sessions = home.join("sessions");
        for (id, encoding) in [
            ("plain", Compression::None),
            ("compressed", Compression::Zstd),
        ] {
            let store = Store::with_compression(&sessions, encoding);
            drop(
                store
                    .create(Header {
                        kind: HeaderTag::Session,
                        version: SESSION_FORMAT_VERSION,
                        id: id.into(),
                        created_at: 1,
                        cwd: Some("/workspace".into()),
                        parent_session: None,
                        is_seeded: false,
                        origin: None,
                        delegation_depth: 0,
                        agent_preset: None,
                    })
                    .unwrap(),
            );
        }
        let cx = ApiContext::new("http://localhost", "test", &home);
        let store = cx.store();
        assert_eq!(store.list_metadata(Some("/workspace")).unwrap().len(), 2);
        store.open(Some("/workspace"), "compressed").unwrap();
        assert_eq!(session_items(&store.read_all()).len(), 2);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn iso_dates_match_the_epoch_and_a_known_instant() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        // The TypeScript Host's workspace createdAt from the capture.
        assert_eq!(iso8601(1_795_422_909_074), "2026-11-23T08:35:09.074Z");
        assert_eq!(iso8601(951_782_400_000), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn todos_projection_keeps_only_the_current_turns_latest_write() {
        let event = |kind: &str, data: Value| Event {
            kind: kind.into(),
            data,
            extra: serde_json::Map::new(),
        };
        let item = json!([{"content":"a","status":"pending"}]);
        assert_eq!(todos_projection(&[]), Value::Null);
        assert_eq!(
            todos_projection(&[event("todo/write", json!({"todos": item}))]),
            item
        );
        assert_eq!(
            todos_projection(&[
                event("todo/write", json!({"todos": item})),
                event("turn/start", json!({"turn": 2})),
            ]),
            Value::Null
        );
        assert_eq!(
            todos_projection(&[
                event("todo/write", json!({"todos": item})),
                event("turn/start", json!({"turn": 2})),
                event("todo/write", json!({"todos": []})),
            ]),
            json!([])
        );
    }

    #[test]
    fn the_goal_projection_matches_the_replayed_planning_state() {
        let row = |kind: &str, data: Value, seq: u64| {
            let mut extra = serde_json::Map::new();
            extra.insert("seq".into(), json!(seq));
            extra.insert("time".into(), json!(1000 + seq));
            Event {
                kind: kind.into(),
                data,
                extra,
            }
        };
        let create = json!({
            "kind": "goal/change", "version": 1, "operation": "create",
            "goal": {"id": "goal-1", "revision": 1, "objective": "ship it", "phase": "active", "maxGoalRounds": 4},
            "roundsStarted": 0, "createdAt": 100, "updatedAt": 100
        });
        let round = json!({
            "content": [{"type": "text", "text": "continue"}],
            "source": {"kind": "goal", "goalId": "goal-1", "revision": 1, "round": 1}
        });
        let edit = json!({
            "kind": "goal/change", "version": 1, "operation": "edit",
            "goal": {"id": "goal-1", "revision": 2, "objective": "ship it well", "phase": "active", "maxGoalRounds": 6},
            "roundsStarted": 1, "createdAt": 100, "updatedAt": 200
        });
        let pause = json!({
            "kind": "goal/change", "version": 1, "operation": "pause",
            "goal": {"id": "goal-1", "revision": 3, "objective": "ship it well", "phase": "paused", "maxGoalRounds": 6},
            "roundsStarted": 1, "createdAt": 100, "updatedAt": 300
        });
        let events = vec![
            row("goal/change", create, 0),
            row("user/message", round, 1),
            row("goal/change", edit, 2),
            row("goal/change", pause, 3),
        ];
        let limits = dsh_planning::PlanningLimits {
            allow_parallel_todos: true,
            ..dsh_planning::PlanningLimits::default()
        };
        let mut state = dsh_planning::PlanningState::new(limits);
        for event in &events {
            state.replay(event).expect("the fixture replays");
        }
        let expected = serde_json::to_value(state.goal_projection().expect("a current goal"))
            .expect("serializes");
        assert_eq!(goal_projection(&events), expected);
        let clear = json!({
            "kind": "goal/change", "version": 1, "operation": "clear",
            "cleared": {"id": "goal-1", "revision": 4}, "clearedAt": 400
        });
        let mut cleared = events;
        cleared.push(row("goal/change", clear, 4));
        assert_eq!(goal_projection(&cleared), Value::Null);
    }
    #[test]
    fn boot_streams_start_with_the_baselines_the_client_expects() {
        let home = std::env::temp_dir().join(format!("dsh-api-base-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let cx = ApiContext::new("http://x", "t", &home);
        let b: std::collections::BTreeMap<_, _> = baselines(&cx, Some(json!({"rev": "r"})))
            .into_iter()
            .collect();
        assert_eq!(b.len(), 7);
        assert_eq!(
            b["remoteAccess/follow"],
            json!({"type": "baseline", "owner": true})
        );
        assert_eq!(b["pluginEvents/follow"]["graph"]["rev"], "r");
        assert_eq!(b["account/watch"]["state"]["status"], "signed-out");
        assert_eq!(b["workspace/follow"]["value"]["items"], json!([]));
        initialize_default(&cx).unwrap();
        let after: std::collections::BTreeMap<_, _> = baselines(&cx, None).into_iter().collect();
        assert_eq!(
            after["workspace/follow"]["value"]["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn uuids_are_well_formed_and_distinct() {
        let (a, b) = (random_uuid(), random_uuid());
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        assert_eq!(a.matches('-').count(), 4);
    }

    #[test]
    fn default_workspace_is_created_once_and_lists_its_sessions() {
        let home = std::env::temp_dir().join(format!("dsh-api-home-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let cx = ApiContext::new("http://x", "t", &home);
        let first = initialize_default(&cx).unwrap();
        let second = initialize_default(&cx).unwrap();
        assert_eq!(first["workspaceId"], second["workspaceId"]);
        assert_eq!(first["sessionIds"], json!([]));
        assert!(Path::new(first["path"].as_str().unwrap()).is_dir());
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn prompt_text_joins_text_parts_and_rejects_empty() {
        let joined = json!([{"type": "text", "text": "a"}, {"type": "image"}, {"type": "text", "text": "b"}]);
        assert_eq!(
            prompt_text(joined.as_array().unwrap()),
            Some("a\nb".to_string())
        );
        let blank = json!([{"type": "text", "text": "   "}]);
        assert_eq!(prompt_text(blank.as_array().unwrap()), None);
        assert!(prompt_text(&[]).is_none());
    }

    #[test]
    fn prompt_boundary_validates_ids_content_and_time_zones() {
        let request = json!({"request": {
            "requestId": "r1", "sessionId": "s1", "mode": "queue",
            "content": [{"type": "text", "text": "hello"}],
            "clientTimeZone": "America/New_York",
        }});
        let parsed = parse_prompt_request(&request).unwrap();
        assert_eq!(parsed.request_id, "r1");
        assert_eq!(parsed.session_id, "s1");
        assert_eq!(parsed.mode, PromptMode::Queue);
        assert_eq!(parsed.client_time_zone.as_deref(), Some("America/New_York"));

        for value in ["", " PST", "PST", "+00:00", "America/No_Such_Zone"] {
            let error = parse_prompt_request(&json!({"request": {
                "requestId": "r1", "sessionId": "s1", "mode": "queue",
                "content": [{"type": "text", "text": "hello"}],
                "clientTimeZone": value,
            }}))
            .unwrap_err();
            assert_eq!(error.code, "session/invalid-time-zone");
            assert_eq!(error.details["value"], value);
        }
        let error = parse_prompt_request(&json!({"request": {
            "requestId": "r1", "sessionId": "s1", "mode": "queue",
            "content": [{"type": "text", "text": "   "}],
        }}))
        .unwrap_err();
        assert_eq!(error.code, "gateway/bad-request");
        let error = parse_prompt_request(&json!({"request": {
            "requestId": "r1", "sessionId": "s1", "mode": "queue",
            "content": [{"type": "image", "mediaType": "image/svg+xml", "data": "x"}],
        }}))
        .unwrap_err();
        assert_eq!(error.code, "session/attachment-invalid");

        parse_prompt_request(&json!({"request": {
            "requestId": "r1", "sessionId": "s1", "mode": "queue", "content": [],
        }}))
        .unwrap();
        let attached = parse_prompt_request(&json!({"request": {
            "requestId": "r1", "sessionId": "s1", "mode": "steer",
            "content": [
                {"type": "image", "mediaType": "image/png", "data": "AA=="},
                {"type": "file", "receiptId": "receipt-1"},
            ],
        }}))
        .unwrap();
        assert_eq!(attached.mode, PromptMode::Steer);
        assert_eq!(attached.content.len(), 2);
    }

    #[test]
    fn prompt_receipts_ignore_inherited_copies_and_include_claimed_inputs() {
        let events: Vec<dsh_session_format::Event> = serde_json::from_value(json!([
            {"type": "user/message", "seq": 0, "time": 1,
             "data": {"source": {"kind": "user", "rpcId": "r1"}}},
            {"type": "session/end-seed", "seq": 1, "time": 2,
             "data": {"inherited": true}},
            {"type": "agent/inbox/spliced", "seq": 2, "time": 3,
             "data": {"inserted": [{"source": {"kind": "user", "rpcId": "r2"}}]}},
            {"type": "agent/inbox/spliced", "seq": 3, "time": 4,
             "data": {"inserted": [{"source": {"kind": "retry", "rpcId": "r3"}}]}},
        ]))
        .unwrap();
        assert!(!has_prompt_request(&events, "r1"));
        assert!(has_prompt_request(&events, "r2"));
        assert!(has_prompt_request(&events, "r3"));
        assert!(!has_prompt_request(&events, "r4"));
    }

    #[test]
    fn continuation_uses_the_last_turn_boundary() {
        let mut events: Vec<Event> = serde_json::from_value(json!([
            {"type": "turn/start", "seq": 0, "time": 1, "data": {"turn": 1}},
            {"type": "user/message", "seq": 1, "time": 2, "surfaceOp": "append",
             "data": {"source": {"kind": "user"}, "content": [{"type": "text", "text": "go"}]}},
            {"type": "assistant/attempt", "seq": 2, "time": 3, "data": {}},
            {"type": "turn/end", "seq": 3, "time": 4,
             "data": {"reason": {"kind": "error", "error": {"code": "RATE_LIMIT"}}}},
        ]))
        .unwrap();
        assert!(continuation_eligible(&events));
        events.push(
            serde_json::from_value(json!({
                "type": "turn/start", "seq": 4, "time": 5, "data": {"turn": 2}
            }))
            .unwrap(),
        );
        assert!(!continuation_eligible(&events));
    }

    #[tokio::test]
    async fn queue_edits_and_removals_are_durable() {
        let (home, cwd) = home("queue");
        let cx = ApiContext::new("http://x", "t", &home);
        let created = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();
        let id = created["sessionId"].as_str().unwrap();
        let stored = cx.store().open(Some(&cwd), id).unwrap();
        let mut agent = crate::build_agent(
            dsh_llm_deepseek::client::Connection::new("sk-test"),
            id,
            stored,
            PathBuf::from(&cwd),
        );
        let first = agent
            .enqueue(dsh_core::UserInput::text("first"), InboxTarget::NextTurn)
            .unwrap()
            .unwrap();
        let second = agent
            .enqueue(dsh_core::UserInput::text("second"), InboxTarget::NextTurn)
            .unwrap()
            .unwrap();
        cx.agents
            .lock()
            .unwrap()
            .insert(id.to_string(), Arc::new(AsyncMutex::new(agent)));

        let accepted = update_queue(
            &cx,
            json!({"sessionId": id, "itemId": first, "action": {
                "kind": "edit", "content": [{"type": "text", "text": "edited"}]
            }}),
        )
        .await
        .unwrap();
        assert_eq!(accepted, json!({"accepted": true}));
        let state = session_state(&cx, &json!({"sessionId": id})).unwrap();
        assert_eq!(state["queue"][0]["id"], json!(first));
        assert_eq!(state["queue"][0]["content"][0]["text"], json!("edited"));

        let error = update_queue(
            &cx,
            json!({"sessionId": id, "itemId": first, "action": {
                "kind": "edit", "content": [{"type": "image"}]
            }}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "session/attachment-invalid");
        assert_eq!(error.details["reason"], "QUEUE_EDIT_NON_TEXT");

        update_queue(
            &cx,
            json!({"sessionId": id, "itemId": second, "action": {"kind": "remove"}}),
        )
        .await
        .unwrap();
        let error = update_queue(
            &cx,
            json!({"sessionId": id, "itemId": first, "action": {"kind": "steer"}}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "session/steer-unavailable");

        drop(cx.agents.lock().unwrap().remove(id).unwrap());
        let reopened = cx.store().open(Some(&cwd), id).unwrap();
        let inbox = InboxState::recover(&reopened.events).unwrap();
        assert_eq!(inbox.next_turn.len(), 1);
        assert_eq!(inbox.next_turn[0].id, first);
        assert_eq!(
            inbox.next_turn[0].message.content[0],
            Content::Text {
                text: "edited".into()
            }
        );
        let _ = std::fs::remove_dir_all(home);
    }

    #[test]
    fn attachment_reads_exact_stored_image_bytes() {
        use base64::engine::general_purpose::STANDARD;
        use dsh_attachments::StagedAttachment;

        let (home, _) = home("attachment");
        let cx = ApiContext::new("http://x", "t", &home);
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(image::RgbaImage::new(1, 1))
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        let bytes = png.into_inner();
        let encoded = STANDARD.encode(&bytes);
        let receipt = cx
            .attachments()
            .unwrap()
            .admit_encoded_image(StoredImageMediaType::Png, &encoded, Some("pixel.png"))
            .unwrap();
        let StagedAttachment::Image {
            attachment: reference,
            ..
        } = receipt.attachment
        else {
            panic!("image admission produced an image receipt")
        };
        let value = attachment(
            &cx,
            &json!({"sessionId": "permission-scope", "attachment": reference}),
        )
        .unwrap();
        assert_eq!(value["data"], json!(encoded));
        assert_eq!(
            STANDARD.decode(value["data"].as_str().unwrap()).unwrap(),
            bytes
        );
        assert_eq!(value["attachment"]["name"], json!("pixel.png"));
        let _ = std::fs::remove_dir_all(home);
    }

    #[test]
    fn a_session_holds_one_turn_slot() {
        let (home, _) = home("turnslot");
        let cx = ApiContext::new("http://x", "t", &home);
        let first = cx.claim_turn("s1").expect("first claim wins");
        assert!(cx.claim_turn("s1").is_none(), "a second turn is refused");
        assert!(cx.claim_turn("s2").is_some());
        // A cancellation reaches the turn the claim handed out.
        cx.cancel_turn("s1");
        assert!(first.is_cancelled());
        cx.release_turn("s1");
        assert!(cx.claim_turn("s1").is_some());
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn live_frames_deliver_this_session_after_the_cursor() {
        use futures_util::StreamExt;
        let gateway = Gateway::new("t", "h", None);
        let broadcaster = gateway.broadcaster("probe");
        let mut frames = live_frames(broadcaster.subscribe(), Some("s1".into()), 2, None);
        // Replay of the snapshot cut, another Session, then the frame that belongs here.
        broadcaster.publish(json!({"sessionId": "s1", "seq": 2, "frame": {"n": 2}}));
        broadcaster.publish(json!({"sessionId": "s2", "seq": 3, "frame": {"n": 3}}));
        broadcaster.publish(json!({"sessionId": "s1", "seq": 3, "frame": {"n": 3}}));
        let frame = frames.next().await.unwrap().unwrap();
        assert_eq!(frame, json!({"n": 3}));
    }

    #[tokio::test]
    async fn assistant_frames_use_the_snapshot_revision_and_keep_publication_order() {
        use futures_util::StreamExt;
        let gateway = Gateway::new("t", "h", None);
        let broadcaster = gateway.broadcaster("assistant-probe");
        let assistant = |revision| SessionAssistantStreamFrame::Chunk {
            attempt_id: "attempt-1".into(),
            revision,
            index: revision - 1,
            time: 10 + revision,
            chunk: json!({"type": "text-delta", "index": revision - 1, "text": "x"}),
        };
        let mut frames = live_frames(broadcaster.subscribe(), Some("s1".into()), 0, Some(1));
        broadcaster.publish(json!({
            "sessionId": "s1", "seq": 0, "assistantRevision": 1,
            "frame": {"type": "assistant-stream", "frame": assistant(1)},
        }));
        broadcaster.publish(json!({"sessionId": "s1", "seq": 1, "frame": {"n": 1}}));
        broadcaster.publish(json!({
            "sessionId": "s1", "seq": 1, "assistantRevision": 2,
            "frame": {"type": "assistant-stream", "frame": assistant(2)},
        }));
        broadcaster.publish(json!({
            "sessionId": "s2", "seq": 2, "assistantRevision": 3,
            "frame": {"type": "assistant-stream", "frame": assistant(3)},
        }));
        assert_eq!(frames.next().await.unwrap().unwrap(), json!({"n": 1}));
        assert_eq!(
            frames.next().await.unwrap().unwrap(),
            json!({"type": "assistant-stream", "frame": assistant(2)})
        );
    }

    #[test]
    fn follow_includes_an_assistant_baseline_only_when_requested() {
        let (home, cwd) = home("assistant-baseline");
        let cx = ApiContext::new("http://x", "t", &home);
        seed_session(&cx, &cwd, "session-assistant", 1);
        cx.accept_assistant_stream(
            "session-assistant",
            SessionAssistantStreamFrame::Start {
                attempt_id: "attempt-1".into(),
                revision: 1,
                started_after_seq: 0,
                turn: 1,
                step: 1,
            },
            0,
        );
        cx.accept_assistant_stream(
            "session-assistant",
            SessionAssistantStreamFrame::Chunk {
                attempt_id: "attempt-1".into(),
                revision: 2,
                index: 0,
                time: 10,
                chunk: json!({"type": "text-delta", "index": 0, "text": "hello"}),
            },
            0,
        );
        let request = |opted_in| {
            json!({"args": {"request": {
                "address": {"kind": "session", "sessionId": "session-assistant"},
                "assistantStream": opted_in,
            }}})
        };
        assert_eq!(
            follow_snapshot(&cx, &request(false)).unwrap()["assistantStream"],
            Value::Null
        );
        let snapshot = follow_snapshot(&cx, &request(true)).unwrap();
        assert_eq!(snapshot["assistantStream"]["revision"], 2);
        assert_eq!(snapshot["assistantStream"]["activeAttempt"]["nextIndex"], 1);
        assert_eq!(
            snapshot["assistantStream"]["activeAttempt"]["stream"][0]["type"],
            "text-chunks"
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn session_lifecycle_values_travel_through_the_host_event_stream() {
        let (home, cwd) = home("session-events");
        let cx = ApiContext::new("http://x", "t", &home);
        let gateway = Gateway::new("t", "h", None);
        cx.attach_events(gateway.event_broadcaster());
        let mut events = gateway.event_broadcaster().subscribe();
        let created = create_session(&cx, &json!({"cwd": cwd})).unwrap();
        let id = created["sessionId"].as_str().unwrap();
        let added = events.recv().await.unwrap();
        assert_eq!(added["type"], "emit");
        assert_eq!(added["event"], "api-session/added");
        assert_eq!(added["args"][0]["sessionId"], id);

        cx.emit("api-session/status", vec![json!(id), json!(true)]);
        assert_eq!(events.recv().await.unwrap()["args"], json!([id, true]));
        cx.emit("api-session/activity", vec![json!(id), json!(123)]);
        assert_eq!(
            events.recv().await.unwrap()["event"],
            "api-session/activity"
        );
        cx.emit("api-session/error", vec![json!(id), json!("failed")]);
        assert_eq!(events.recv().await.unwrap()["args"], json!([id, "failed"]));
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn lists_one_directory_level_with_crumbs_and_hidden_flag() {
        let (home, _) = home("listdir");
        let root = home.join("tree");
        std::fs::create_dir_all(root.join("beta")).unwrap();
        std::fs::create_dir_all(root.join("alpha")).unwrap();
        std::fs::create_dir_all(root.join(".hidden")).unwrap();
        std::fs::write(root.join("not-a-dir.txt"), "x").unwrap();

        let listing =
            list_directory(&json!({"args": {"path": root.display().to_string()}})).unwrap();
        assert_eq!(listing["path"], root.display().to_string());
        let names: Vec<&str> = listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, [".hidden", "alpha", "beta"]);
        assert_eq!(listing["entries"][0]["hidden"], true);
        assert_eq!(listing["entries"][1]["hidden"], false);
        assert_eq!(listing["truncated"], false);
        // Crumbs run root first and end at the listed directory.
        let crumbs = listing["crumbs"].as_array().unwrap();
        assert_eq!(crumbs.last().unwrap()["path"], root.display().to_string());
        assert_eq!(crumbs.len(), root.ancestors().count());
        // The reported path is normalized, so it matches the last crumb exactly even when the
        // request spelled it with the other separator style.
        assert_eq!(
            listing["path"].as_str().unwrap(),
            crumbs.last().unwrap()["path"].as_str().unwrap()
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn a_normalized_listing_matches_its_crumbs_for_either_separator() {
        let (home, _) = home("listnorm");
        let root = home.join("tree");
        std::fs::create_dir_all(&root).unwrap();
        let forward = root.display().to_string().replace('\\', "/");
        let listing = list_directory(&json!({"args": {"path": forward}})).unwrap();
        let crumbs = listing["crumbs"].as_array().unwrap();
        assert_eq!(listing["path"], crumbs.last().unwrap()["path"]);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn listing_a_missing_directory_reports_not_found() {
        let error =
            list_directory(&json!({"args": {"path": "Z:/definitely/not/here"}})).unwrap_err();
        assert_eq!(error.code, "directory-picker/not-found");
    }

    #[test]
    fn creates_one_directory_and_refuses_path_segments() {
        let (home, _) = home("createdir");
        let root = home.join("tree");
        std::fs::create_dir_all(&root).unwrap();
        let parent = root.display().to_string();

        let created =
            create_directory(&json!({"args": {"path": parent, "name": "fresh"}})).unwrap();
        assert_eq!(created, json!(root.join("fresh").display().to_string()));
        assert!(root.join("fresh").is_dir());

        for bad in ["..", "a/b", "a\\b", "C:evil", ""] {
            let error = create_directory(
                &json!({"args": {"path": root.display().to_string(), "name": bad}}),
            )
            .unwrap_err();
            assert_eq!(error.code, "gateway/bad-request", "{bad}");
        }
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn session_rows_report_blank_title_and_cwd() {
        use dsh_session_format::Event;
        let header = Header {
            kind: HeaderTag::Session,
            version: 4,
            id: "s1".into(),
            created_at: 0,
            cwd: Some("/w".into()),
            parent_session: None,
            is_seeded: false,
            origin: None,
            delegation_depth: 0,
            agent_preset: None,
        };
        let ev = |k: &str, d: Value| Event {
            kind: k.into(),
            data: d,
            extra: Default::default(),
        };
        let blank = Stored {
            header: header.clone(),
            events: vec![ev("turn/start", json!({}))],
            modified_ms: 5,
        };
        let used = Stored {
            header,
            events: vec![
                ev("user/message", json!({})),
                ev("session/title", json!({"title": "Hello"})),
            ],
            modified_ms: 9,
        };
        let rows = session_items(&[blank, used]);
        assert_eq!(rows[0]["blank"], true);
        assert_eq!(rows[1]["blank"], false);
        assert_eq!(rows[1]["projections"]["values"]["title"], "Hello");
        assert_eq!(rows[1]["cwd"], "/w");
        assert_eq!(rows[1]["updatedAt"], 9);
    }

    /// A fresh home and the cwd a test creates Sessions under.
    fn home(name: &str) -> (std::path::PathBuf, String) {
        let home = std::env::temp_dir().join(format!("dsh-api-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let cwd = home.join("project").display().to_string();
        (home, cwd)
    }

    #[tokio::test]
    async fn a_delegated_task_runs_as_a_durable_child_session() {
        use dsh_llm::{BlockKind, FinishReason, StreamChunk};

        /// Answers every request with one fixed text block.
        struct ChildModel;

        impl dsh_core::Model for ChildModel {
            fn provider(&self) -> String {
                "fake".into()
            }
            fn model(&self) -> String {
                "m1".into()
            }
            fn stream<'a>(
                &'a self,
                _history: &'a [dsh_llm::Message],
                _tools: &'a [dsh_llm::ToolDef],
                tx: tokio::sync::mpsc::Sender<Result<StreamChunk, dsh_llm::LlmError>>,
            ) -> dsh_core::BoxFuture<'a, ()> {
                Box::pin(async move {
                    let _ = tx
                        .send(Ok(StreamChunk::BlockStart {
                            index: 0,
                            kind: BlockKind::Text,
                        }))
                        .await;
                    let _ = tx
                        .send(Ok(StreamChunk::TextDelta {
                            index: 0,
                            text: "child answer".into(),
                        }))
                        .await;
                    let _ = tx.send(Ok(StreamChunk::BlockEnd { index: 0 })).await;
                    let _ = tx
                        .send(Ok(StreamChunk::Finish {
                            reason: FinishReason::Stop,
                            replay: vec![None],
                        }))
                        .await;
                })
            }
        }

        let (home, cwd) = home("subagent-delegate");
        let cx = ApiContext::new("http://x", "t", &home);
        let created = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();
        let id = created["sessionId"].as_str().unwrap().to_string();
        let runner = crate::HostSubagentRunner::new(
            Arc::new(ChildModel),
            cx.home.join("sessions"),
            std::path::PathBuf::from(&cwd),
            &id,
            0,
        );

        use dsh_tools::SubagentRunner as _;
        let answer = runner
            .run(dsh_tools::SubagentTask {
                description: "answer briefly",
                prompt: "say the words",
                cancel: &Cancel::default(),
            })
            .await
            .unwrap();
        assert_eq!(answer, "child answer");

        let stored = cx.store().read_all();
        let child = stored
            .iter()
            .find(|stored| stored.header.parent_session.as_deref() == Some(id.as_str()))
            .expect("a child Session was created");
        assert_eq!(
            child.header.origin,
            Some(dsh_session_format::Origin::Subagent)
        );
        assert_eq!(child.header.delegation_depth, 1);
        assert_eq!(child.header.agent_preset.as_deref(), Some("standard"));
        let kinds: Vec<&str> = child
            .events
            .iter()
            .map(|event| event.kind.as_str())
            .collect();
        assert!(kinds.contains(&"user/message"), "{kinds:?}");
        assert!(kinds.contains(&"assistant/message"), "{kinds:?}");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn create_session_writes_a_blank_session_at_the_requested_cwd() {
        let (home, cwd) = home("create");
        let cx = ApiContext::new("http://x", "t", &home);
        let value = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();

        let id = value["sessionId"].as_str().unwrap();
        assert!(id.starts_with("session-"), "{id}");
        assert_eq!(value["agentPreset"], "standard");

        let opened = cx.store().open(Some(&cwd), id).unwrap();
        assert_eq!(opened.header.cwd.as_deref(), Some(cwd.as_str()));
        assert_eq!(opened.header.agent_preset.as_deref(), Some("standard"));
        assert!(opened.events.is_empty());

        let rows = session_items(&cx.store().read_all());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["blank"], true);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn create_session_accepts_a_bare_request_without_the_args_envelope() {
        let (home, cwd) = home("bare");
        let cx = ApiContext::new("http://x", "t", &home);
        let value = create_session(&cx, &json!({"cwd": cwd, "agentPreset": "ptc"})).unwrap();
        assert_eq!(value["agentPreset"], "ptc");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn create_session_rejects_workspace_and_cwd_together() {
        let (home, cwd) = home("both");
        let cx = ApiContext::new("http://x", "t", &home);
        let error =
            create_session(&cx, &json!({"args": {"workspaceId": "w1", "cwd": cwd}})).unwrap_err();
        assert_eq!(error.code, "gateway/bad-request");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn create_session_reports_an_unknown_workspace() {
        let (home, _) = home("nows");
        let cx = ApiContext::new("http://x", "t", &home);
        let error = create_session(&cx, &json!({"args": {"workspaceId": "missing"}})).unwrap_err();
        assert_eq!(error.code, "workspace/not-found");
        let _ = std::fs::remove_dir_all(&home);
    }

    /// Append `count` rows to one Session, stamped the way the agent loop stamps them.
    fn seed_session(cx: &ApiContext, cwd: &str, id: &str, count: usize) {
        create_session(
            cx,
            &json!({"args": {"request": {"cwd": cwd, "sessionId": id}}}),
        )
        .unwrap();
        let mut session = cx.store().open(Some(cwd), id).unwrap();
        for seq in 0..count {
            let mut extra = serde_json::Map::new();
            extra.insert("seq".into(), json!(seq));
            extra.insert("time".into(), json!(1_000 + seq));
            let event = dsh_session_format::Event {
                kind: "user/message".into(),
                data: json!({"text": format!("m{seq}")}),
                extra,
            };
            session.append(std::slice::from_ref(&event)).unwrap();
        }
        drop(session);
    }

    #[test]
    fn follow_snapshot_opens_with_the_header_history_and_projections() {
        let (home, cwd) = home("follow");
        let cx = ApiContext::new("http://x", "t", &home);
        seed_session(&cx, &cwd, "session-a", 2);

        let frame = follow_snapshot(&cx, &json!({"args": {"request": {"address": {"kind": "session", "sessionId": "session-a"}}}})).unwrap();
        assert_eq!(frame["type"], "snapshot");
        assert_eq!(frame["header"]["id"], "session-a");
        assert_eq!(frame["header"]["version"], 4);
        assert_eq!(frame["header"]["cwd"], cwd);
        assert_eq!(frame["header"]["agentPreset"], "standard");
        // The wire header carries no log discriminator.
        assert_eq!(frame["header"].get("type"), None);
        assert_eq!(frame["cursor"], 1);
        assert_eq!(frame["hasMore"], false);
        assert_eq!(frame["records"].as_array().unwrap().len(), 2);
        assert_eq!(frame["records"][0]["type"], "event");
        assert_eq!(frame["records"][0]["event"]["seq"], 0);
        assert_eq!(frame["records"][1]["event"]["time"], 1_001);
        assert_eq!(frame["records"][1]["event"]["data"]["text"], "m1");
        assert_eq!(frame["projections"]["asOfSeq"], 2);
        assert_eq!(
            frame["projections"]["values"]["sessionListMetadata"]["blank"],
            false
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn follow_snapshot_cuts_history_to_the_message_budget() {
        let (home, cwd) = home("followbudget");
        let cx = ApiContext::new("http://x", "t", &home);
        seed_session(&cx, &cwd, "session-b", 5);
        let frame = follow_snapshot(
            &cx,
            &json!({"args": {"request": {
            "address": {"kind": "session", "sessionId": "session-b"}, "maxMessages": 2}}}),
        )
        .unwrap();
        let records = frame["records"].as_array().unwrap();
        assert_eq!(records.len(), 2);
        // The retained rows keep their own seq rather than being renumbered.
        assert_eq!(records[0]["event"]["seq"], 3);
        assert_eq!(records[1]["event"]["seq"], 4);
        assert_eq!(frame["hasMore"], true);
        assert_eq!(frame["cursor"], 4);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn follow_snapshot_reports_an_unknown_session() {
        let (home, _) = home("followmissing");
        let cx = ApiContext::new("http://x", "t", &home);
        let error = follow_snapshot(
            &cx,
            &json!({"args": {"request": {"address": {"sessionId": "nope"}}}}),
        )
        .unwrap_err();
        assert_eq!(error.code, "session/not-found");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn create_session_reuses_a_blank_session_but_not_one_holding_a_conversation() {
        use dsh_session_format::Event;
        let (home, cwd) = home("reuse");
        let cx = ApiContext::new("http://x", "t", &home);
        let request = |id: &str, preset: Option<&str>| {
            let mut request = json!({"cwd": cwd, "sessionId": id});
            if let Some(preset) = preset {
                request["agentPreset"] = json!(preset);
            }
            json!({"args": {"request": request}})
        };

        let first = create_session(&cx, &request("session-fixed", None)).unwrap();
        assert_eq!(first["sessionId"], "session-fixed");
        let again = create_session(&cx, &request("session-fixed", Some("minimal"))).unwrap();
        assert_eq!(again["sessionId"], "session-fixed");
        assert_eq!(again["agentPreset"], "minimal");
        let persisted = cx.store().open(Some(&cwd), "session-fixed").unwrap();
        assert_eq!(selected_agent_preset(&persisted), "minimal");
        assert_eq!(persisted.events[0].kind, "agent-preset/selected");
        drop(persisted);

        let mut session = cx.store().open(Some(&cwd), "session-fixed").unwrap();
        session
            .append(&[Event {
                kind: "user/message".into(),
                data: json!({"text": "hi"}),
                extra: Default::default(),
            }])
            .unwrap();
        drop(session);

        let fresh = create_session(&cx, &request("session-fixed", None)).unwrap();
        assert_ne!(fresh["sessionId"], "session-fixed");
        assert!(fresh["sessionId"].as_str().unwrap().starts_with("session-"));
        let _ = std::fs::remove_dir_all(&home);
    }

    fn append_seed(cx: &ApiContext, cwd: &str, id: &str, events: Vec<Event>) {
        create_session(
            cx,
            &json!({"args": {"request": {"cwd": cwd, "sessionId": id}}}),
        )
        .unwrap();
        let mut session = cx.store().open(Some(cwd), id).unwrap();
        let events: Vec<Event> = events
            .into_iter()
            .enumerate()
            .map(|(seq, mut event)| {
                event.extra.insert("seq".into(), json!(seq));
                event.extra.insert("time".into(), json!(1_000 + seq));
                event
            })
            .collect();
        session.append(&events).unwrap();
    }

    fn user_event(text: &str) -> Event {
        let mut event = stored_event(
            "user/message",
            json!({"id": "message-user", "role": "user", "content": [{"type": "text", "text": text}]}),
            0,
            0,
        );
        event.extra.insert("surfaceOp".into(), json!("append"));
        event
    }

    fn assistant_event(text: &str) -> Event {
        let mut event = stored_event(
            "assistant/message",
            json!({"turn": 1, "step": 1, "message": {"id": "message-assistant", "role": "assistant", "content": [{"type": "text", "text": text}]}}),
            0,
            0,
        );
        event.extra.insert("surfaceOp".into(), json!("append"));
        event
    }

    #[test]
    fn session_page_defaults_to_fifty_and_honors_a_complete_turn_window() {
        let (home, cwd) = home("page");
        let cx = ApiContext::new("http://x", "t", &home);
        let mut events = vec![stored_event("turn/start", json!({"turn": 1}), 0, 0)];
        for index in 0..30 {
            events.push(user_event(&format!("first {index}")));
        }
        events.push(stored_event(
            "turn/end",
            json!({"turn": 1, "reason": {"kind": "completed"}}),
            0,
            0,
        ));
        events.push(stored_event("turn/start", json!({"turn": 2}), 0, 0));
        for index in 0..30 {
            events.push(user_event(&format!("second {index}")));
        }
        let mut assistant = assistant_event("answer");
        assistant.data["turn"] = json!(2);
        events.push(assistant);
        events.push(stored_event(
            "turn/end",
            json!({"turn": 2, "reason": {"kind": "completed"}}),
            0,
            0,
        ));
        append_seed(&cx, &cwd, "session-page", events);

        let page = session_page(
            &cx,
            &json!({"args": {"request": {
            "address": {"kind": "session", "sessionId": "session-page"}}}}),
        )
        .unwrap();
        assert!(page["records"].as_array().unwrap().len() > 50);
        assert_eq!(
            page["records"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|record| matches!(
                    record["event"]["type"].as_str(),
                    Some("user/message" | "assistant/message")
                ))
                .count(),
            50
        );
        assert_eq!(page["hasMore"], true);

        let window = session_page(
            &cx,
            &json!({"args": {"request": {
            "address": {"kind": "session", "sessionId": "session-page"},
            "maxMessages": 50, "turnWindow": {"minMessages": 1, "minTurns": 1}}}}),
        )
        .unwrap();
        let records = window["records"].as_array().unwrap();
        assert_eq!(records[0]["event"]["type"], "turn/start");
        assert_eq!(records[0]["event"]["data"]["turn"], 2);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn rename_model_state_wait_and_search_use_durable_session_events() {
        let (home, cwd) = home("commands");
        // Selection builds a live route, which requires a configured credential.
        std::env::set_var("DEEPSEEK_API_KEY", "sk-test");
        let cx = ApiContext::new("http://x", "t", &home);
        append_seed(
            &cx,
            &cwd,
            "session-commands",
            vec![assistant_event("needle answer")],
        );

        let renamed = rename_session(
            &cx,
            &json!({"args": {"request": {
            "sessionId": "session-commands", "title": "  A\u{1b}[31m title\u{1b}[0m  "}}}),
        )
        .unwrap();
        assert_eq!(renamed["title"], "A title");
        assert_eq!(renamed["seq"], 1);

        let selected = select_model(&cx, &json!({"args": {"request": {
            "sessionId": "session-commands", "provider": "deepseek-official", "model": "deepseek-v4-flash", "reasoningEffort": "high"}}})).await.unwrap();
        assert_eq!(selected["selected"]["reasoningEffort"], "high");
        let projections =
            session_projections(&cx, &json!({"sessionId": "session-commands"})).unwrap();
        assert_eq!(
            projections["values"]["modelSelection"]["next"]["model"],
            "deepseek-v4-flash"
        );
        assert_eq!(
            projections["values"]["modelSelection"]["lastUsed"],
            Value::Null
        );

        let state = session_state(&cx, &json!({"sessionId": "session-commands"})).unwrap();
        assert_eq!(state["cursor"], 2);
        assert_eq!(state["running"], false);
        assert_eq!(state["queue"], Value::Null);

        let idle = wait_session(
            &cx,
            &json!({"sessionId": "session-commands", "afterSeq": -1}),
        )
        .await
        .unwrap();
        assert_eq!(idle["reason"], "idle");
        assert_eq!(idle["output"]["records"].as_array().unwrap().len(), 1);

        let found = search_sessions(&cx, &json!({"query": "NEEDLE"})).unwrap();
        assert_eq!(found["items"][0]["sessionId"], "session-commands");
        assert_eq!(found["items"][0]["snippet"], "needle answer");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn search_snippets_are_bounded_to_two_hundred_forty_code_points() {
        let text = format!("{}needle{}", "a".repeat(500), "b".repeat(500));
        let snippet = search_snippet(&text, "needle");
        assert!(snippet.chars().count() <= 240);
        assert!(snippet.contains("needle"));
    }

    #[test]
    fn fork_repairs_a_started_tool_call_without_mutating_the_source() {
        let (home, cwd) = home("forkstarted");
        let cx = ApiContext::new("http://x", "t", &home);
        let assistant = stored_event(
            "assistant/message",
            json!({"turn": 1, "step": 1, "message": {"role": "assistant", "content": [{"type": "tool-call", "id": "call-1", "name": "read", "arguments": {}}]}}),
            0,
            0,
        );
        append_seed(
            &cx,
            &cwd,
            "session-source",
            vec![
                stored_event("turn/start", json!({"turn": 1}), 0, 0),
                stored_event("step/start", json!({"turn": 1, "step": 1}), 0, 0),
                assistant,
                stored_event(
                    "tool/call",
                    json!({"turn": 1, "step": 1, "callId": "call-1", "name": "read"}),
                    0,
                    0,
                ),
            ],
        );

        let forked = fork_session(
            &cx,
            &json!({"args": {"request": {"sessionId": "session-source", "atSeq": 3}}}),
        )
        .unwrap();
        let child_id = forked["sessionId"].as_str().unwrap();
        let child = cx.store().open(Some(&cwd), child_id).unwrap();
        assert!(child.header.is_seeded);
        assert_eq!(child.events.len(), 8);
        assert_eq!(child.events[4].kind, "session/end-seed");
        assert_eq!(child.events[5].kind, "tool/result");
        assert_eq!(
            child.events[5].data["error"]["code"],
            "TOOL_OUTCOME_UNKNOWN"
        );
        assert_eq!(child.events[5].extra["sourceEventSeqs"], json!([3]));
        assert_eq!(child.events[6].kind, "step/end");
        assert_eq!(child.events[7].data["reason"]["kind"], "forked");
        assert_eq!(
            cx.store()
                .open(Some(&cwd), "session-source")
                .unwrap()
                .events
                .len(),
            4
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn fork_defaults_to_the_latest_completed_prefix_and_marks_unstarted_calls() {
        let (home, cwd) = home("forkdefault");
        let cx = ApiContext::new("http://x", "t", &home);
        let assistant = stored_event(
            "assistant/message",
            json!({"turn": 1, "step": 1, "message": {"role": "assistant", "content": [{"type": "tool-call", "id": "call-2", "name": "read", "arguments": {}}]}}),
            0,
            0,
        );
        append_seed(
            &cx,
            &cwd,
            "session-source",
            vec![
                stored_event("turn/start", json!({"turn": 1}), 0, 0),
                user_event("hello"),
                assistant,
                stored_event(
                    "turn/end",
                    json!({"turn": 1, "reason": {"kind": "completed"}}),
                    0,
                    0,
                ),
                stored_event("session/title", json!({"title": "Fork me"}), 0, 0),
            ],
        );
        let forked = fork_session(&cx, &json!({"sessionId": "session-source"})).unwrap();
        let child = cx
            .store()
            .open(Some(&cwd), forked["sessionId"].as_str().unwrap())
            .unwrap();
        assert_eq!(child.events.len(), 6);
        assert_eq!(child.events[4].kind, "session/title");
        assert_eq!(child.events[5].kind, "session/end-seed");

        let cut = fork_session(&cx, &json!({"sessionId": "session-source", "atSeq": 0})).unwrap();
        let child = cx
            .store()
            .open(Some(&cwd), cut["sessionId"].as_str().unwrap())
            .unwrap();
        assert_eq!(child.events[1].kind, "session/end-seed");
        assert_eq!(child.events[2].kind, "turn/end");

        let call_cut =
            fork_session(&cx, &json!({"sessionId": "session-source", "atSeq": 2})).unwrap();
        let child = cx
            .store()
            .open(Some(&cwd), call_cut["sessionId"].as_str().unwrap())
            .unwrap();
        assert_eq!(child.events[4].data["error"]["code"], "TOOL_NOT_STARTED");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn the_goal_lifecycle_moves_the_projection_activation_and_log() {
        let (home, cwd) = home("goal-lifecycle");
        let cx = ApiContext::new("http://x", "t", &home);
        let gateway = Gateway::new("t", "h", None);
        cx.attach_events(gateway.event_broadcaster());
        let mut events = gateway.event_broadcaster().subscribe();
        let created = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();
        let id = created["sessionId"].as_str().unwrap().to_string();
        assert_eq!(events.recv().await.unwrap()["event"], "api-session/added");

        let created = crate::goal_api::create(
            &cx,
            &json!({"args": [id, {"objective": "ship the goal flow", "maxGoalRounds": 6}]}),
        )
        .await
        .unwrap();
        let goal_id = created["ref"]["id"].as_str().unwrap().to_string();
        assert_eq!(created["ref"]["revision"], 1);

        // The client hears one activation edge, and the projection carries the Goal.
        let edge = events.recv().await.unwrap();
        assert_eq!(edge["event"], "goal/activation-changed");
        assert_eq!(edge["args"][0]["sessionId"], json!(id));
        assert_eq!(edge["args"][0]["goal"]["activation"], "armed");
        let view = crate::goal_api::get(&cx, &json!({"args": [id]})).unwrap();
        assert_eq!(view["objective"], "ship the goal flow");
        assert_eq!(view["maxGoalRounds"], 6);
        assert_eq!(view["activation"], "armed");
        let stored = stored_session(&cx, &id).unwrap();
        let projections = projection_values(&stored);
        assert_eq!(
            projections["goal"]["goal"]["objective"],
            "ship the goal flow"
        );
        assert_eq!(projections["goal"]["goal"]["phase"], "active");

        // Pause disarms and moves the phase; a stale ref is refused by code.
        let paused =
            crate::goal_api::pause(&cx, &json!({"args": [id, {"id": goal_id, "revision": 1}]}))
                .await
                .unwrap();
        assert_eq!(paused["phase"], "paused");
        assert_eq!(paused["activation"], "disarmed");
        assert_eq!(
            events.recv().await.unwrap()["args"][0]["goal"]["activation"],
            "disarmed"
        );
        let stale =
            crate::goal_api::complete(&cx, &json!({"args": [id, {"id": goal_id, "revision": 1}]}))
                .await
                .unwrap_err();
        assert_eq!(stale.code, "GOAL_STALE_REVISION");

        // A tool-owned change appended through the Host moves activation too.
        let stored = stored_session(&cx, &id).unwrap();
        let current = {
            let mut timeline = dsh_planning::GoalTimeline::new();
            for event in &stored.events {
                let _ = timeline.observe(event);
            }
            timeline.projection().cloned().expect("a current goal")
        };
        let resume = dsh_planning::resume_goal(
            Some(&current),
            &dsh_planning::GoalRef {
                id: current.goal.id.clone(),
                revision: current.goal.revision,
            },
            false,
            9_000,
        )
        .unwrap();
        let payload = resume.to_payload().unwrap();
        append_host_event(&cx, &id, "goal/change", payload)
            .await
            .unwrap();
        assert!(cx.goal_armed(&id, goal_id.as_str(), 3));
        assert_eq!(
            events.recv().await.unwrap()["args"][0]["goal"]["activation"],
            "armed"
        );

        // Clear leaves a tombstone the view reads as no Goal at all.
        let cleared =
            crate::goal_api::clear(&cx, &json!({"args": [id, {"id": goal_id, "revision": 3}]}))
                .await
                .unwrap();
        assert_eq!(cleared["revision"], 4);
        assert_eq!(
            crate::goal_api::get(&cx, &json!({"args": [id]})).unwrap(),
            Value::Null
        );
        let stored = stored_session(&cx, &id).unwrap();
        let changes: Vec<u64> = stored
            .events
            .iter()
            .filter(|event| event.kind == "goal/change")
            .map(|event| event.extra["seq"].as_u64().unwrap())
            .collect();
        assert_eq!(changes.len(), 4, "create, pause, resume, clear");
        assert_eq!(changes, (0..4).collect::<Vec<u64>>(), "dense seqs");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn the_plan_command_commits_the_mode_and_keeps_pending_out_of_the_log() {
        let (home, cwd) = home("plan-command");
        let cx = ApiContext::new("http://x", "t", &home);
        let gateway = Gateway::new("t", "h", None);
        cx.attach_events(gateway.event_broadcaster());
        let created = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();
        let id = created["sessionId"].as_str().unwrap().to_string();

        let entered =
            crate::commands_api::execute(&cx, &json!({"args": {"agentId": id, "line": "/plan"}}))
                .await
                .unwrap();
        assert_eq!(
            entered["result"]["text"],
            "Plan mode on. Use /plan off to leave."
        );
        let stored = stored_session(&cx, &id).unwrap();
        let kinds: Vec<&str> = stored
            .events
            .iter()
            .map(|event| event.kind.as_str())
            .collect();
        assert_eq!(
            kinds,
            vec!["command/run", "plan/mode", "user/message", "command/done"],
            "one command, one committed mode, one narration"
        );
        let plan = projection_values(&stored)["plan"].clone();
        assert_eq!(plan, json!({"active": true, "pending": false}));
        let notice = stored.events[2].data["source"].clone();
        assert_eq!(notice["kind"], "plan-mode");
        assert_eq!(notice["form"], "notice");
        assert_eq!(
            notice["summary"],
            "The user switched this session to plan mode."
        );

        // Selecting the state the Session already has logs nothing new.
        let repeated =
            crate::commands_api::execute(&cx, &json!({"args": {"agentId": id, "line": "/plan"}}))
                .await
                .unwrap();
        assert_eq!(
            repeated["result"]["text"],
            "Entering plan mode (applies from the next step). Use /plan off to leave."
        );
        let stored = stored_session(&cx, &id).unwrap();
        assert_eq!(
            stored
                .events
                .iter()
                .filter(|event| event.kind == "plan/mode")
                .count(),
            1,
            "a repeated selection commits no second mode change"
        );

        // The exit commits and the entry flag survives for guidance.
        let exited = crate::commands_api::execute(
            &cx,
            &json!({"args": {"agentId": id, "line": "/plan off"}}),
        )
        .await
        .unwrap();
        assert_eq!(exited["result"]["text"], "Plan mode off.");
        let stored = stored_session(&cx, &id).unwrap();
        let state = dsh_planning::fold_plan(&stored.events);
        assert!(!state.active);
        assert!(
            state.entered,
            "the first entry keeps guidance after an exit"
        );
        assert_eq!(
            projection_values(&stored)["plan"],
            json!({"active": false, "pending": false})
        );

        let again = crate::commands_api::execute(
            &cx,
            &json!({"args": {"agentId": id, "line": "/plan off"}}),
        )
        .await
        .unwrap();
        assert_eq!(again["result"]["text"], "Plan mode is already inactive.");

        // Attachments belong to an instruction, never to the exit.
        let refused = crate::commands_api::execute(
            &cx,
            &json!({"args": {
                "agentId": id,
                "line": "/plan off",
                "submittedAttachments": [{"id": "a-1"}],
            }}),
        )
        .await
        .unwrap_err();
        assert_eq!(refused.code, "command/attachments");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn a_plan_selection_during_a_turn_waits_for_the_boundary() {
        let (home, cwd) = home("plan-queued");
        let cx = ApiContext::new("http://x", "t", &home);
        let created = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();
        let id = created["sessionId"].as_str().unwrap().to_string();
        // Hold a live turn slot the way the resident driver does.
        let cancel = Cancel::default();
        cx.running.lock().unwrap().insert(id.clone(), cancel);

        let queued =
            crate::commands_api::execute(&cx, &json!({"args": {"agentId": id, "line": "/plan"}}))
                .await
                .unwrap();
        assert_eq!(
            queued["result"]["text"],
            "Entering plan mode (applies from the next step). Use /plan off to leave."
        );
        let stored = stored_session(&cx, &id).unwrap();
        assert!(
            stored
                .events
                .iter()
                .all(|event| event.kind != "command/run" && event.kind != "plan/mode"),
            "a running turn leaves the log alone until the boundary"
        );
        assert_eq!(
            projection_values(&stored)["plan"],
            json!({"active": false, "pending": false})
        );
        assert_eq!(cx.plan_pending(&id), Some(true));

        // A second selection replaces the queued one.
        let cancelled = crate::commands_api::execute(
            &cx,
            &json!({"args": {"agentId": id, "line": "/plan off"}}),
        )
        .await
        .unwrap();
        assert_eq!(cancelled["result"]["text"], "Plan mode entry cancelled.");
        assert_eq!(cx.plan_pending(&id), Some(false));
        cx.running.lock().unwrap().remove(&id);
        let _ = std::fs::remove_dir_all(&home);
    }

    /// Admit one durable user message the way the Host does, as pending input.
    /// @param cx - running Host.
    /// @param id - owning Session.
    /// @param message_id - the admitted message identity.
    /// @param text - user text.
    async fn admit(cx: &ApiContext, id: &str, message_id: &str, text: &str) {
        let stored = stored_session(cx, id).unwrap();
        let start = InboxState::recover(&stored.events)
            .unwrap()
            .queue_snapshot()
            .as_array()
            .map_or(0, Vec::len);
        append_host_event(
            cx,
            id,
            "agent/inbox/spliced",
            json!({
                "target": "next-turn",
                "start": start,
                "inserted": [{
                    "id": message_id,
                    "role": "user",
                    "content": [{"type": "text", "text": text}],
                    "source": {"kind": "user", "rpcId": message_id},
                }],
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_queued_selection_commits_at_the_turn_boundary_with_its_guidance() {
        use dsh_llm::{BlockKind, FinishReason, StreamChunk};

        /// Replies with one fixed text answer per request.
        struct Scripted {
            text: &'static str,
            prompts: Mutex<Vec<String>>,
        }

        impl dsh_core::Model for Scripted {
            fn provider(&self) -> String {
                "fake".into()
            }
            fn model(&self) -> String {
                "m1".into()
            }
            fn stream<'a>(
                &'a self,
                history: &'a [dsh_llm::Message],
                _tools: &'a [dsh_llm::ToolDef],
                tx: tokio::sync::mpsc::Sender<Result<StreamChunk, dsh_llm::LlmError>>,
            ) -> dsh_core::BoxFuture<'a, ()> {
                let system = history
                    .first()
                    .filter(|message| matches!(message.role, dsh_llm::Role::System))
                    .map(|message| {
                        message
                            .content
                            .iter()
                            .filter_map(|content| match content {
                                dsh_llm::Content::Text { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<String>()
                    })
                    .unwrap_or_default();
                self.prompts.lock().unwrap().push(system);
                let text = self.text;
                Box::pin(async move {
                    let _ = tx
                        .send(Ok(StreamChunk::BlockStart {
                            index: 0,
                            kind: BlockKind::Text,
                        }))
                        .await;
                    let _ = tx
                        .send(Ok(StreamChunk::TextDelta {
                            index: 0,
                            text: text.into(),
                        }))
                        .await;
                    let _ = tx.send(Ok(StreamChunk::BlockEnd { index: 0 })).await;
                    let _ = tx
                        .send(Ok(StreamChunk::Finish {
                            reason: FinishReason::Stop,
                            replay: vec![None],
                        }))
                        .await;
                })
            }
        }

        let (home, cwd) = home("plan-boundary");
        let cx = ApiContext::new("http://x", "t", &home);
        let created = create_session(&cx, &json!({"args": {"request": {"cwd": cwd}}})).unwrap();
        let id = created["sessionId"].as_str().unwrap().to_string();

        // One admitted user turn, then the selection queued while it was open.
        admit(&cx, &id, "msg-1", "plan the port").await;
        let session = cx.store().open(Some(&cwd), &id).unwrap();
        let scripted = Arc::new(Scripted {
            text: "the plan",
            prompts: Mutex::new(Vec::new()),
        });
        let mut agent = crate::try_build_agent_with_model(
            scripted.clone(),
            session,
            std::path::PathBuf::from(&cwd),
            dsh_tools::ToolPreset::Standard,
            Some(cx.session_facts(&id)),
            None,
            None,
        )
        .unwrap();
        cx.set_plan_pending(
            &id,
            PendingPlan {
                active: true,
                command_id: "cmd-queued-1".into(),
                args: String::new(),
                text: "Entering plan mode (applies from the next step). Use /plan off to leave."
                    .into(),
            },
        );

        // The turn runs under the queued state, then the boundary commits it.
        refresh_plan_mode(&cx, &id, &mut agent);
        let ends = agent.run_pending(&Cancel::default()).await.unwrap();
        assert!(matches!(ends.as_slice(), [TurnEnd::Completed]), "{ends:?}");
        commit_plan_pending(&cx, &id, &mut agent);
        assert_eq!(cx.plan_pending(&id), None);
        // Release the Session write lease before the Host reads it back.
        drop(agent);

        let stored = stored_session(&cx, &id).unwrap();
        let kinds: Vec<&str> = stored
            .events
            .iter()
            .map(|event| event.kind.as_str())
            .collect();
        let tail: Vec<&str> = kinds[kinds.len() - 4..].to_vec();
        assert_eq!(
            tail,
            vec!["command/run", "command/done", "plan/mode", "user/message"],
            "the queued command lands with its mode change"
        );
        assert_eq!(
            projection_values(&stored)["plan"],
            json!({ "active": true, "pending": false })
        );
        let prompt = scripted.prompts.lock().unwrap()[0].clone();
        assert!(
            prompt.contains("The following instructions apply only while plan mode is active."),
            "the queued state already frames the turn"
        );
        assert!(
            prompt.contains("Plan mode is active. Follow the plan-mode instructions."),
            "the guidance states the active policy"
        );

        // The next turn rewrites the one system message instead of adding another.
        // A queued exit changes the guidance, so the next turn rewrites the
        // single system message instead of adding a second one.
        admit(&cx, &id, "msg-2", "continue").await;
        cx.set_plan_pending(
            &id,
            PendingPlan {
                active: false,
                command_id: "cmd-queued-2".into(),
                args: " off".into(),
                text: "Leaving plan mode (applies from the next step).".into(),
            },
        );
        let session = cx.store().open(Some(&cwd), &id).unwrap();
        let mut agent = crate::try_build_agent_with_model(
            scripted.clone(),
            session,
            std::path::PathBuf::from(&cwd),
            dsh_tools::ToolPreset::Standard,
            Some(cx.session_facts(&id)),
            None,
            None,
        )
        .unwrap();
        commit_plan_pending(&cx, &id, &mut agent);
        refresh_plan_mode(&cx, &id, &mut agent);
        let ends = agent.run_pending(&Cancel::default()).await.unwrap();
        assert!(matches!(ends.as_slice(), [TurnEnd::Completed]), "{ends:?}");
        drop(agent);
        let stored = stored_session(&cx, &id).unwrap();
        let systems: Vec<&dsh_session_format::Event> = stored
            .events
            .iter()
            .filter(|event| event.kind == "system/message")
            .collect();
        assert_eq!(systems.len(), 2, "the second turn rewrites the prompt");
        assert_eq!(systems[1].extra["surfaceOp"]["op"], "replace");
        assert_eq!(
            systems[1].extra["sourceEventSeqs"],
            json!([systems[0].seq()])
        );
        let history = dsh_core::derive_history(&stored.events);
        let prompts: Vec<&dsh_llm::Message> = history
            .iter()
            .filter(|message| matches!(message.role, dsh_llm::Role::System))
            .collect();
        assert_eq!(prompts.len(), 1, "one system prompt survives the rewrite");
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn plan_guidance_follows_the_folded_state() {
        let fresh = dsh_planning::PlanState::default();
        assert_eq!(crate::plan::plan_guidance(&fresh, None), "");
        let entered = dsh_planning::fold_plan(&[dsh_session_format::Event {
            kind: "plan/mode".into(),
            data: json!({"active": true}),
            extra: json!({"seq": 0, "time": 0}).as_object().cloned().unwrap(),
        }]);
        assert!(entered.active && entered.entered);
        let active = crate::plan::plan_guidance(&entered, None);
        assert!(
            active.starts_with("The following instructions apply only while plan mode is active.")
        );
        assert!(active.contains("exit_plan_mode"));
        assert!(active.ends_with("Plan mode is active. Follow the plan-mode instructions."));
        let inactive = crate::plan::plan_guidance(&entered, Some(false));
        assert!(inactive
            .ends_with("Plan mode is inactive. The plan-mode instructions no longer apply."));
        assert_eq!(
            crate::plan::plan_guidance(&fresh, Some(true)),
            crate::plan::plan_guidance(&entered, None),
            "a pending entry already frames the next request"
        );
    }

    #[test]
    fn the_goal_round_prompt_matches_the_model_facing_text() {
        let goal = dsh_planning::GoalSnapshot {
            id: dsh_planning::GoalId::new("goal-1").unwrap(),
            revision: 1,
            objective: "ship it".into(),
            phase: dsh_planning::GoalPhase::Active,
            blocked_reason: None,
            max_goal_rounds: 6,
        };
        assert_eq!(
            goal_round_prompt(&goal, 2),
            concat!(
                "<goal_round>\n",
                "Objective: \"ship it\"\n",
                "Round: 2/6\n\n",
                "Continue working toward the objective in this same session. Treat the current ",
                "workspace, tool results, and durable session state as authoritative; inspect them ",
                "instead of assuming earlier narration is still current. Make concrete progress and ",
                "verify the result. Before claiming completion, gather evidence that the whole ",
                "objective is achieved, read the current goal, and mark it complete. If work remains, ",
                "leave the goal active for the next round. Follow the configured goal-tool policy ",
                "before reporting a blocker.\n",
                "</goal_round>"
            )
        );
    }

    #[test]
    fn plan_goal_round_queues_blocks_or_stays_idle() {
        let (home, _) = home("goal-rounds");
        let cx = ApiContext::new("http://x", "t", &home);
        let change = dsh_planning::create_goal(
            None,
            dsh_planning::GoalId::new("goal-1").unwrap(),
            "ship it",
            Some(2),
            10,
        )
        .unwrap();
        let projection = dsh_planning::apply_to(None, &change)
            .unwrap()
            .expect("a goal");

        assert!(matches!(
            plan_goal_round(&cx, "session-1", &projection),
            GoalRoundPlan::Idle
        ));
        assert!(cx.set_goal_activation("session-1", "goal-1", 1, true));
        match plan_goal_round(&cx, "session-1", &projection) {
            GoalRoundPlan::Continue { content, source } => {
                assert!(content.contains("Round: 1/2"));
                assert_eq!(source["kind"], "goal");
                assert_eq!(source["round"], 1);
                assert_eq!(source["goalId"], "goal-1");
            }
            _ => panic!("an armed active goal continues"),
        }

        let mut spent = projection.clone();
        spent.rounds_started = 2;
        match plan_goal_round(&cx, "session-1", &spent) {
            GoalRoundPlan::Block(change) => {
                let payload = change.to_payload().unwrap();
                assert_eq!(payload["operation"], "block");
                assert_eq!(payload["goal"]["phase"], "blocked");
                assert_eq!(payload["goal"]["blockedReason"]["code"], "round-limit");
            }
            _ => panic!("a spent goal retires"),
        }

        let completed = dsh_planning::complete_goal(
            Some(&projection),
            &dsh_planning::GoalRef {
                id: projection.goal.id.clone(),
                revision: 1,
            },
            20,
        )
        .unwrap();
        let completed = dsh_planning::apply_to(Some(&projection), &completed)
            .unwrap()
            .unwrap();
        assert!(matches!(
            plan_goal_round(&cx, "session-1", &completed),
            GoalRoundPlan::Idle
        ));
        let _ = std::fs::remove_dir_all(&home);
    }
}
