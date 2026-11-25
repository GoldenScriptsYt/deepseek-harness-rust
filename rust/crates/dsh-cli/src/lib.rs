//! Headless runner: one task, one turn, streamed to stdout, persisted as a Session.

mod account;
pub mod api;
mod catalog_api;
mod commands_api;
mod feedback_api;
mod goal_api;
pub mod interaction;
mod interaction_api;
pub mod model_routes;
pub mod openai_model;
mod plan;
pub mod plugin_manager;
pub mod process_api;
mod reference_api;
pub mod session_query_api;
pub mod settings;
mod team_api;
pub mod user_questions;
pub mod web;
pub mod workspace_api;

use dsh_attachments::{
    AttachmentError, AttachmentStore, FileAttachmentRef as StoredFileRef,
    ImageAttachmentRef as StoredImageRef,
};
use dsh_core::{Agent, AgentConfig, AgentEvent, BoxFuture, Cancel, Model, ToolRegistry, TurnEnd};
use dsh_llm::{Content, ErrorCode, LlmError, Message, RequestImage, StreamChunk, ToolDef};
use dsh_llm_deepseek::client::{self, Connection};
use dsh_llm_deepseek::serialize::{serialize_prepared, Effort, Options, PreparedImages, ToolSpec};
use dsh_session_format::{Header, HeaderTag, SESSION_FORMAT_VERSION};
use dsh_session_persistence::Store;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub const DEFAULT_MODEL: &str = "deepseek-flash";

/// Wrap a provider route so a resident Agent can change models without losing its history.
pub fn switchable_model(route: Arc<dyn Model>) -> Arc<dyn Model> {
    Arc::new(SwitchableModel {
        route: Arc::new(RwLock::new(route)),
    })
}

struct SwitchableModel {
    route: Arc<RwLock<Arc<dyn Model>>>,
}

impl SwitchableModel {
    fn current(&self) -> Arc<dyn Model> {
        Arc::clone(
            &self
                .route
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl Model for SwitchableModel {
    fn provider(&self) -> String {
        self.current().provider()
    }

    fn model(&self) -> String {
        self.current().model()
    }

    fn replace_route(&self, next: Arc<dyn Model>) -> bool {
        *self
            .route
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        true
    }

    fn stream<'a>(
        &'a self,
        history: &'a [Message],
        tools: &'a [ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) -> BoxFuture<'a, ()> {
        let route = self.current();
        Box::pin(async move { route.stream(history, tools, tx).await })
    }
}

/// A route to the DeepSeek Messages API, from `DEEPSEEK_API_KEY` and `DEEPSEEK_BASE_URL`.
pub fn connection_from_env(key: impl Into<String>) -> Connection {
    let mut conn = Connection::new(key.into());
    conn.base_url =
        std::env::var("DEEPSEEK_BASE_URL").unwrap_or_else(|_| client::PUBLIC_BASE_URL.into());
    conn
}

/// Build the agent a Session runs with: the DeepSeek route, every registered tool, the
/// open Session, and the shared system prompt.
pub fn build_agent(
    conn: Connection,
    session_id: &str,
    session: dsh_session_persistence::Session,
    cwd: PathBuf,
) -> Agent {
    build_agent_with_selection(conn, session_id, session, cwd, None, None)
}

/// Build an Agent with a durable Session selection overriding environment defaults.
pub fn build_agent_with_selection(
    conn: Connection,
    session_id: &str,
    session: dsh_session_persistence::Session,
    cwd: PathBuf,
    selected_model: Option<String>,
    selected_effort: Option<Effort>,
) -> Agent {
    try_build_agent_with_selection(
        conn,
        session_id,
        session,
        cwd,
        selected_model,
        selected_effort,
    )
    .expect("agent session contains a valid durable inbox")
}

/// Build an Agent without panicking when durable inbox recovery rejects the Session.
pub fn try_build_agent_with_selection(
    conn: Connection,
    session_id: &str,
    session: dsh_session_persistence::Session,
    cwd: PathBuf,
    selected_model: Option<String>,
    selected_effort: Option<Effort>,
) -> Result<Agent, dsh_core::CoreError> {
    try_build_agent_with_attachments(
        conn,
        session_id,
        session,
        cwd,
        selected_model,
        selected_effort,
        None,
    )
}

/// Build an Agent with durable attachment bytes available to model requests.
pub fn try_build_agent_with_attachments(
    conn: Connection,
    session_id: &str,
    session: dsh_session_persistence::Session,
    cwd: PathBuf,
    selected_model: Option<String>,
    selected_effort: Option<Effort>,
    attachments: Option<AttachmentStore>,
) -> Result<Agent, dsh_core::CoreError> {
    try_build_agent_with_preset_and_attachments(
        conn,
        session_id,
        session,
        cwd,
        AgentSelection {
            model: selected_model,
            effort: selected_effort,
            attachments,
        },
        dsh_tools::ToolPreset::Standard,
    )
}

/// Durable model selection and attachments one Agent builder carries.
#[derive(Default)]
pub struct AgentSelection {
    /// Model id from the Session's last `model/selection`, if any.
    pub model: Option<String>,
    /// Reasoning effort from the same selection.
    pub effort: Option<Effort>,
    /// Attachment store available to model requests.
    pub attachments: Option<AttachmentStore>,
}

/// Build an Agent with the tools selected by its durable preset.
pub fn try_build_agent_with_preset_and_attachments(
    conn: Connection,
    session_id: &str,
    session: dsh_session_persistence::Session,
    cwd: PathBuf,
    selection: AgentSelection,
    preset: dsh_tools::ToolPreset,
) -> Result<Agent, dsh_core::CoreError> {
    let model = deepseek_model(
        conn,
        session_id,
        selection
            .model
            .unwrap_or_else(|| std::env::var("DSH_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.into())),
        selection.effort,
        selection.attachments,
    );
    try_build_agent_with_model(model, session, cwd, preset, None, None, None)
}

/// Compaction settings from the environment; `DSH_COMPACTION=off` disables it.
///
/// The Agent's session log is the only source of truth for history, so the
/// threshold is a deployment choice rather than a protocol constant.
fn compaction_config() -> Option<dsh_core::CompactionConfig> {
    if std::env::var("DSH_COMPACTION").is_ok_and(|value| value.eq_ignore_ascii_case("off")) {
        return None;
    }
    let mut config = dsh_core::CompactionConfig::default();
    if let Some(value) = std::env::var("DSH_COMPACTION_THRESHOLD_TOKENS")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        config.threshold_tokens = value;
    }
    if let Some(value) = std::env::var("DSH_COMPACTION_KEEP_MESSAGES")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        config.keep_recent_messages = value;
    }
    Some(config)
}
/// Build one DeepSeek Messages route, used both at Agent creation and model switching.
pub(crate) fn deepseek_model(
    conn: Connection,
    session_id: &str,
    model: String,
    effort: Option<Effort>,
    attachments: Option<AttachmentStore>,
) -> Arc<dyn Model> {
    let options = Options {
        model,
        max_tokens: std::env::var("DSH_MAX_TOKENS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32_000),
        effort,
        temperature: None,
        stop_sequences: vec![],
        tools: vec![],
        system_in_history: false,
    };
    Arc::new(DeepSeekModel {
        conn,
        options,
        session_id: session_id.to_string(),
        attachments,
    })
}

/// Web providers this Host can offer, from the deployment environment.
///
/// The fetcher needs no credential. The search provider is offered only when
/// `DEEPSEEK_API_KEY` is set, mirroring the TypeScript Host, which registers
/// `web_search` only while its provider is usable.
/// @returns The fetch provider and the optional search provider.
fn web_tool_providers() -> (
    Option<Arc<dsh_web::HttpFetchProvider>>,
    Option<Arc<dsh_web::DeepSeekSearchProvider>>,
) {
    let fetcher = dsh_web::HttpFetchProvider::new(dsh_web::HttpFetchLimits::default())
        .ok()
        .map(Arc::new);
    let searcher = std::env::var("DEEPSEEK_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
        .and_then(|key| {
            let mut config = dsh_web::DeepSeekSearchConfig::new(key);
            if let Some(base) = std::env::var("DSH_WEB_SEARCH_BASE_URL")
                .ok()
                .filter(|base| !base.trim().is_empty())
            {
                config.base_url = base;
            }
            dsh_web::DeepSeekSearchProvider::new(config)
                .ok()
                .map(Arc::new)
        });
    (fetcher, searcher)
}

/// Finish an Agent build around any provider implementation of [`Model`].
pub(crate) fn try_build_agent_with_model(
    model: Arc<dyn Model>,
    session: dsh_session_persistence::Session,
    cwd: PathBuf,
    preset: dsh_tools::ToolPreset,
    session_facts: Option<Arc<dyn dsh_core::SessionFacts>>,
    mcp: Option<&dsh_mcp::McpHost>,
    subagent: Option<Arc<dyn dsh_tools::SubagentRunner>>,
) -> Result<Agent, dsh_core::CoreError> {
    let mut tools = ToolRegistry::default();
    dsh_tools::register_for_preset(&mut tools, preset);
    if session_facts.is_some() {
        dsh_tools::register_goal_tools(&mut tools);
    }
    if preset != dsh_tools::ToolPreset::Minimal {
        if let Some(runner) = subagent {
            dsh_tools::register_subagent_tools(&mut tools, runner);
        }
    }
    if preset != dsh_tools::ToolPreset::Minimal {
        let (fetcher, searcher) = web_tool_providers();
        if let Some(fetcher) = fetcher {
            dsh_web::register_web_tools(
                &mut tools,
                dsh_web::WebToolConfig::default(),
                fetcher,
                searcher,
            );
        }
    }
    if preset != dsh_tools::ToolPreset::Minimal {
        crate::interaction::register_tools(&mut tools, session.id());
    }
    if let Some(mcp) = mcp {
        mcp.register(&mut tools);
    }
    let model_name = model.model();
    let persona = match preset {
        dsh_tools::ToolPreset::Minimal => format!(
            "You are a coding agent powered by the {model_name} model. This is Minimal mode: the {shell} tool is the only tool available; use it for workspace inspection and changes.",
            shell = if cfg!(windows) { "pwsh" } else { "bash" }
        ),
        dsh_tools::ToolPreset::Cordis => format!(
            "You are a coding agent powered by the {model_name} model. You are in Creator mode. Help the user customize DeepSeek Harness. Use cordis_inspect to check the available Host APIs and tool schemas before relying on them. Create and verify plugin or preset source in the workspace, and do not claim a plugin is active unless the Host confirms it."
        ),
        dsh_tools::ToolPreset::Standard | dsh_tools::ToolPreset::Ptc => format!(
            "You are a coding agent powered by the {model_name} model."
        ),
    };
    let instructions = dsh_core::workspace_instructions(&cwd);
    let tool_names = tools.names();
    let mut system_prompt = dsh_core::assemble_agent_prompt(&persona, &instructions, &tool_names);
    if preset != dsh_tools::ToolPreset::Minimal {
        system_prompt.push_str("\n\n");
        system_prompt.push_str(match dsh_tools::default_policy() {
            dsh_tools::ApprovalPolicy::Ask => dsh_tools::ASK_SENTENCE,
            dsh_tools::ApprovalPolicy::Never => dsh_tools::NEVER_SENTENCE,
        });
    }
    if let Some(mcp) = mcp {
        let sections = mcp.prompt_sections();
        if !sections.is_empty() {
            system_prompt.push_str("\n\n");
            system_prompt.push_str(&sections);
        }
    }
    Agent::try_new(
        model,
        tools,
        session,
        AgentConfig {
            system_prompt,
            cwd,
            max_steps: 100,
            compaction: compaction_config(),
            session: session_facts,
        },
    )
}

/// Runs delegated tasks as child Sessions of the calling Session.
///
/// The child inherits the parent route and the Standard composition, runs one
/// turn to completion, and keeps its own Session file so its transcript stays
/// inspectable after the parent's tool result is the only thing the parent model sees.
pub struct HostSubagentRunner {
    model: Arc<dyn Model>,
    sessions: PathBuf,
    cwd: PathBuf,
    parent: String,
    depth: u64,
}

impl HostSubagentRunner {
    /// Build the runner for one parent Session.
    /// @param model - The parent's model route, inherited by the child.
    /// @param sessions - Session store root.
    /// @param cwd - Workspace the child works in.
    /// @param parent - Parent Session identity.
    /// @param depth - The parent's delegation depth.
    #[must_use]
    pub fn new(
        model: Arc<dyn Model>,
        sessions: PathBuf,
        cwd: PathBuf,
        parent: impl Into<String>,
        depth: u64,
    ) -> Self {
        Self {
            model,
            sessions,
            cwd,
            parent: parent.into(),
            depth,
        }
    }
}

/// The last assistant text in one Agent history.
/// @param history - Provider-neutral messages of the finished child turn.
/// @returns The concatenated text of the final assistant message.
fn final_answer(history: &[Message]) -> String {
    let Some(message) = history
        .iter()
        .rev()
        .find(|message| message.role == dsh_llm::Role::Assistant)
    else {
        return String::new();
    };
    message
        .content
        .iter()
        .filter_map(|content| match content {
            Content::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

impl dsh_tools::SubagentRunner for HostSubagentRunner {
    fn run<'a>(
        &'a self,
        task: dsh_tools::SubagentTask<'a>,
    ) -> BoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |duration| duration.as_nanos());
            let id = format!("{}-sub-{nanos:x}", self.parent);
            let session = Store::mixed(&self.sessions)
                .create(Header {
                    kind: HeaderTag::Session,
                    version: SESSION_FORMAT_VERSION,
                    id: id.clone(),
                    created_at: u64::try_from(nanos / 1_000_000).unwrap_or(0),
                    cwd: Some(self.cwd.to_string_lossy().into_owned()),
                    parent_session: Some(self.parent.clone()),
                    is_seeded: false,
                    origin: Some(dsh_session_format::Origin::Subagent),
                    delegation_depth: self.depth + 1,
                    agent_preset: Some("standard".into()),
                })
                .map_err(|error| format!("failed to create the child Session: {error}"))?;
            let mut agent = try_build_agent_with_model(
                Arc::clone(&self.model),
                session,
                self.cwd.clone(),
                dsh_tools::ToolPreset::Standard,
                None,
                None,
                None,
            )
            .map_err(|error| error.to_string())?;
            match agent.run_turn(task.prompt, task.cancel).await {
                Ok(TurnEnd::Completed) => {}
                Ok(other) => {
                    return Err(format!("the subagent turn ended with {other:?}"));
                }
                Err(error) => return Err(error.to_string()),
            }
            let answer = final_answer(agent.history());
            if answer.trim().is_empty() {
                return Err(format!(
                    "the subagent finished \"{}\" without a text answer",
                    task.description
                ));
            }
            Ok(answer)
        })
    }
}

/// DeepSeek Messages route as an agent [`Model`].
pub struct DeepSeekModel {
    pub conn: Connection,
    pub options: Options,
    pub session_id: String,
    pub attachments: Option<AttachmentStore>,
}

impl DeepSeekModel {
    /// Translate retired Flash model names to the current DeepSeek API model ID.
    ///
    /// DeepSeek's current vision-capable Flash model is exposed as `deepseek-flash`.
    /// Older app profiles can still carry versioned IDs; sending those directly can
    /// select an obsolete text-only route even though the user attached an image.
    fn api_model_id(&self) -> &str {
        let model = self.options.model.as_str();
        if matches!(model, "deepseek-v4-flash" | "deepseek-v4-flash-vision-exp")
            || model.starts_with("deepseek-v4.1-flash")
        {
            "deepseek-flash"
        } else {
            model
        }
    }

    fn prepared_images(&self, history: &[Message]) -> Result<Option<PreparedImages>, LlmError> {
        let Some(store) = &self.attachments else {
            return Ok(None);
        };
        let mut prepared = PreparedImages::default();
        for message in history {
            for content in &message.content {
                match content {
                    Content::Image {
                        attachment,
                        offloaded,
                    } => {
                        let reference = stored_image_ref(attachment).map_err(files_api_error)?;
                        let path = store
                            .image_readonly_path(&reference)
                            .map_err(files_api_error)?;
                        prepared
                            .readonly_paths
                            .insert(attachment.attachment_id.clone(), path.display().to_string());
                        if !offloaded {
                            let stored = store.read_image(&reference).map_err(files_api_error)?;
                            prepared.versions.insert(
                                attachment.attachment_id.clone(),
                                RequestImage {
                                    variant_id: attachment.attachment_id.clone(),
                                    attachment: attachment.clone(),
                                    data: Arc::from(stored.data),
                                    media_type: attachment.media_type,
                                    bytes: attachment.bytes,
                                    width: attachment.width,
                                    height: attachment.height,
                                },
                            );
                        }
                    }
                    Content::File { attachment } => {
                        let reference = StoredFileRef {
                            attachment_id: attachment.attachment_id.clone(),
                            name: attachment.name.clone(),
                            bytes: attachment.bytes,
                        };
                        let path = store
                            .file_readonly_path(&reference)
                            .map_err(files_api_error)?;
                        prepared
                            .readonly_paths
                            .insert(attachment.attachment_id.clone(), path.display().to_string());
                    }
                    _ => {}
                }
            }
        }
        Ok(Some(prepared))
    }
}

fn stored_image_ref(
    reference: &dsh_llm::ImageAttachmentRef,
) -> Result<StoredImageRef, AttachmentError> {
    let original_dimensions = reference
        .original_dimensions
        .as_ref()
        .map(|dimensions| {
            Ok::<_, AttachmentError>(dsh_attachments::ImageDimensions {
                width: u32::try_from(dimensions.width)
                    .map_err(|_| AttachmentError::InvalidAttachmentRef)?,
                height: u32::try_from(dimensions.height)
                    .map_err(|_| AttachmentError::InvalidAttachmentRef)?,
            })
        })
        .transpose()?;
    Ok(StoredImageRef {
        attachment_id: reference.attachment_id.clone(),
        media_type: match reference.media_type {
            dsh_llm::ImageMediaType::Png => dsh_attachments::ImageMediaType::Png,
            dsh_llm::ImageMediaType::Jpeg => dsh_attachments::ImageMediaType::Jpeg,
            dsh_llm::ImageMediaType::Webp => dsh_attachments::ImageMediaType::Webp,
            dsh_llm::ImageMediaType::Gif => dsh_attachments::ImageMediaType::Gif,
        },
        bytes: reference.bytes,
        width: u32::try_from(reference.width).map_err(|_| AttachmentError::InvalidAttachmentRef)?,
        height: u32::try_from(reference.height)
            .map_err(|_| AttachmentError::InvalidAttachmentRef)?,
        name: reference.name.clone(),
        original_dimensions,
    })
}

fn files_api_error(error: AttachmentError) -> LlmError {
    LlmError::new(ErrorCode::FilesApi, error.to_string())
}

impl Model for DeepSeekModel {
    fn provider(&self) -> String {
        "deepseek-official".into()
    }
    fn model(&self) -> String {
        self.options.model.clone()
    }
    fn stream<'a>(
        &'a self,
        history: &'a [Message],
        tools: &'a [ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut options = self.options.clone();
            options.model = self.api_model_id().to_owned();
            options.tools = tools
                .iter()
                .map(|t| ToolSpec {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    input_schema: t.input_schema.clone(),
                    defer_loading: false,
                })
                .collect();
            let body = self
                .prepared_images(history)
                .and_then(|prepared| serialize_prepared(&options, history, prepared.as_ref()));
            match body {
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
                Ok(body) => {
                    client::stream(&self.conn, &body, Some(&self.session_id), false, tx).await
                }
            }
        })
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Args {
    pub task: String,
    pub cwd: Option<PathBuf>,
    pub model: Option<String>,
    pub resume: Option<String>,
    pub effort: Option<String>,
    pub max_steps: Option<u32>,
    pub home: Option<PathBuf>,
}

impl Args {
    pub fn parse(argv: &[String]) -> Result<Args, String> {
        let mut a = Args::default();
        let mut words = Vec::new();
        let mut it = argv.iter();
        while let Some(arg) = it.next() {
            let mut value = |name: &str| {
                it.next()
                    .cloned()
                    .ok_or_else(|| format!("{name} needs a value"))
            };
            match arg.as_str() {
                "--cwd" => a.cwd = Some(value("--cwd")?.into()),
                "--model" => a.model = Some(value("--model")?),
                "--resume" => a.resume = Some(value("--resume")?),
                "--effort" => a.effort = Some(value("--effort")?),
                "--home" => a.home = Some(value("--home")?.into()),
                "--max-steps" => {
                    a.max_steps = Some(
                        value("--max-steps")?
                            .parse()
                            .map_err(|_| "--max-steps must be a number".to_string())?,
                    )
                }
                s if s.starts_with("--") => return Err(format!("unknown option {s}")),
                s => words.push(s.to_string()),
            }
        }
        a.task = words.join(" ");
        if a.task.is_empty() {
            return Err("usage: dsh-rs [--cwd DIR] [--model M] [--effort low|high|max] [--resume ID] [--max-steps N] <task>".into());
        }
        Ok(a)
    }
}

fn effort(s: &str) -> Result<Option<Effort>, String> {
    match s {
        "off" | "none" => Ok(None),
        "low" => Ok(Some(Effort::Low)),
        "high" => Ok(Some(Effort::High)),
        "max" => Ok(Some(Effort::Max)),
        other => Err(format!("unknown effort \"{other}\"")),
    }
}

fn dsh_home(args: &Args) -> PathBuf {
    args.home
        .clone()
        .or_else(|| std::env::var_os("DSH_HOME").map(PathBuf::from))
        .or_else(|| {
            std::env::var_os("USERPROFILE")
                .or_else(|| std::env::var_os("HOME"))
                .map(|h| PathBuf::from(h).join(".dsh"))
        })
        .unwrap_or_else(|| PathBuf::from(".dsh"))
}

fn new_session_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("rs-{nanos:x}-{:x}", std::process::id())
}

/// Run `args` against `conn`, writing the transcript to `out`. Returns the process exit code.
pub async fn run(
    args: Args,
    conn: Connection,
    cancel: Cancel,
    out: &mut (dyn Write + Send),
) -> Result<i32, String> {
    let cwd = match &args.cwd {
        Some(c) => c.clone(),
        None => std::env::current_dir().map_err(|e| e.to_string())?,
    };
    let cwd_str = cwd.display().to_string();
    let store = Store::mixed(dsh_home(&args).join("sessions"));
    let session_id = args.resume.clone().unwrap_or_else(new_session_id);
    let session = match &args.resume {
        Some(id) => store.open(Some(&cwd_str), id).map_err(|e| e.to_string())?,
        None => store
            .create(Header {
                kind: HeaderTag::Session,
                version: SESSION_FORMAT_VERSION,
                id: session_id.clone(),
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as u64),
                cwd: Some(cwd_str.clone()),
                parent_session: None,
                is_seeded: false,
                origin: None,
                delegation_depth: 0,
                agent_preset: None,
            })
            .map_err(|e| e.to_string())?,
    };

    let max_tokens = std::env::var("DSH_MAX_TOKENS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32_000);
    let options = Options {
        model: args.model.clone().unwrap_or_else(|| DEFAULT_MODEL.into()),
        max_tokens,
        effort: effort(args.effort.as_deref().unwrap_or("off"))?,
        temperature: None,
        stop_sequences: vec![],
        tools: vec![],
        system_in_history: false,
    };
    let model = Arc::new(DeepSeekModel {
        conn,
        options,
        session_id: session_id.clone(),
        attachments: None,
    });
    let mut tools = ToolRegistry::default();
    dsh_tools::register_all(&mut tools);
    let persona = format!(
        "You are a coding agent powered by the {} model.",
        model.model()
    );
    let instructions = dsh_core::workspace_instructions(&cwd);
    let system_prompt = dsh_core::assemble_agent_prompt(&persona, &instructions, &tools.names());
    let mut agent = Agent::new(
        model,
        tools,
        session,
        AgentConfig {
            system_prompt,
            cwd,
            max_steps: args.max_steps.unwrap_or(100),
            compaction: compaction_config(),
            session: None,
        },
    );

    let (ltx, lrx) = std::sync::mpsc::channel::<String>();
    let _reg = agent.bus.on(move |e| match e {
        AgentEvent::Chunk(StreamChunk::TextDelta { text, .. }) => {
            let _ = ltx.send(text.clone());
        }
        AgentEvent::Logged(ev) if ev.kind == "tool/call" => {
            let name = ev.data["name"].as_str().unwrap_or("?");
            let _ = ltx.send(format!("\n[tool: {name}]\n"));
        }
        _ => {}
    });

    let end = {
        let turn = agent.run_turn(&args.task, &cancel);
        tokio::pin!(turn);
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(20));
        loop {
            tokio::select! {
                r = &mut turn => break r.map_err(|e| e.to_string())?,
                _ = ticker.tick() => { while let Ok(s) = lrx.try_recv() { let _ = out.write_all(s.as_bytes()); let _ = out.flush(); } }
            }
        }
    };
    while let Ok(s) = lrx.try_recv() {
        let _ = out.write_all(s.as_bytes());
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "session: {session_id}");
    Ok(match end {
        TurnEnd::Completed | TurnEnd::MaxTokens => 0,
        TurnEnd::Aborted => 130,
        TurnEnd::Error(e) => {
            let _ = writeln!(out, "error: {e}");
            1
        }
        TurnEnd::StepLimit => {
            let _ = writeln!(out, "error: step limit reached");
            1
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn prepared_images_supply_exact_bytes_to_deepseek_requests() {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use dsh_attachments::{ImageMediaType, SaveImageAttachment, StagedAttachment};
        use dsh_llm::Role;
        use serde_json::json;

        let root = std::env::temp_dir().join(format!("dsh-prepared-image-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
            1,
            1,
            image::Rgba([0, 0, 0, 255]),
        ))
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
        let bytes = png.into_inner();
        let encoded = STANDARD.encode(&bytes);
        let store = AttachmentStore::open(&root, Default::default()).unwrap();
        let receipt = store
            .save_image(SaveImageAttachment {
                data: &bytes,
                media_type: ImageMediaType::Png,
                name: Some("pixel.png"),
            })
            .unwrap();
        let StagedAttachment::Image { attachment, .. } = receipt.attachment else {
            panic!("image admission produced an image receipt")
        };
        let attachment: dsh_llm::ImageAttachmentRef = serde_json::to_value(&attachment)
            .and_then(serde_json::from_value)
            .unwrap();
        let history = vec![Message {
            role: Role::User,
            content: vec![Content::Image {
                attachment: attachment.clone(),
                offloaded: false,
            }],
            tool_call_id: None,
            is_error: None,
        }];
        let options = Options {
            model: DEFAULT_MODEL.into(),
            max_tokens: 128,
            effort: None,
            temperature: None,
            stop_sequences: Vec::new(),
            tools: Vec::new(),
            system_in_history: false,
        };
        let model = DeepSeekModel {
            conn: Connection::new("sk-test"),
            options: options.clone(),
            session_id: "s".into(),
            attachments: Some(store),
        };
        let prepared = model.prepared_images(&history).unwrap().unwrap();
        assert_eq!(
            prepared.versions[&attachment.attachment_id].data.as_ref(),
            bytes
        );
        assert!(PathBuf::from(&prepared.readonly_paths[&attachment.attachment_id]).is_file());
        let request = serialize_prepared(&options, &history, Some(&prepared)).unwrap();
        assert_eq!(
            request["messages"][0]["content"][1]["source"]["data"],
            json!(encoded)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn parses_options_and_task_words() {
        let a = Args::parse(&v(&[
            "--model",
            "m",
            "--max-steps",
            "3",
            "fix",
            "the",
            "bug",
        ]))
        .unwrap();
        assert_eq!(
            (a.task.as_str(), a.model.as_deref(), a.max_steps),
            ("fix the bug", Some("m"), Some(3))
        );
        assert!(Args::parse(&v(&[])).is_err());
        assert!(Args::parse(&v(&["--nope", "x"])).is_err());
        assert!(Args::parse(&v(&["--model"])).is_err());
    }
}
