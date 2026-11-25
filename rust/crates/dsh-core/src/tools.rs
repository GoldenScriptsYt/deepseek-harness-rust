//! Tool trait and registry. Port of `core/tools`.

use dsh_llm::ToolDef;
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Cooperative cancellation shared by a turn and everything it spawns.
#[derive(Clone)]
pub struct Cancel {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
}

impl Default for Cancel {
    fn default() -> Self {
        let (tx, rx) = watch::channel(false);
        Cancel {
            tx: Arc::new(tx),
            rx,
        }
    }
}

impl Cancel {
    pub fn cancel(&self) {
        let _ = self.tx.send(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolves when cancelled; pending forever otherwise.
    pub async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Durable Session events a tool asks the owning Agent to commit.
///
/// A tool that owns durable state records its transition here; the Agent commits
/// each queued event with the Session's `seq` and `time` right after the call
/// returns, so the log order stays `tool/call`, the tool's events,
/// `tool/result`.
#[derive(Clone, Default)]
pub struct EventSink {
    events: Arc<std::sync::Mutex<Vec<(String, Value)>>>,
}

impl EventSink {
    /// Queue one event for the owning Agent to commit after the call returns.
    /// @param kind - Session event discriminator, such as `todo/write`.
    /// @param data - Event payload.
    pub fn append(&self, kind: impl Into<String>, data: Value) {
        if let Ok(mut events) = self.events.lock() {
            events.push((kind.into(), data));
        }
    }

    /// Take every queued event in call order, leaving the sink empty.
    /// @returns The queued events.
    pub fn drain(&self) -> Vec<(String, Value)> {
        self.events
            .lock()
            .map(|mut events| std::mem::take(&mut *events))
            .unwrap_or_default()
    }
}

/// Session facts a durable-state tool may read without owning the Session.
///
/// The Host implements this over the committed log so a tool can validate a
/// compare-and-set transition before it asks the Agent to commit the event.
pub trait SessionFacts: Send + Sync {
    /// Committed Session events, in log order.
    /// @returns The events, or a readable reason they are unavailable.
    fn events(&self) -> Result<Vec<dsh_session_format::Event>, String>;

    /// Whether automatic continuation is armed for one Goal revision.
    /// @param _id - Goal identity.
    /// @param _revision - exact Goal revision.
    /// @returns The process-local activation; tools that never read it keep the default.
    fn goal_armed(&self, _id: &str, _revision: u64) -> bool {
        false
    }
}

/// Environment handed to every tool call.
#[derive(Clone, Default)]
pub struct ToolContext {
    pub cwd: PathBuf,
    pub cancel: Cancel,
    pub events: EventSink,
    /// Committed Session facts, when the caller supplies them.
    pub session: Option<Arc<dyn SessionFacts>>,
    /// Identity of the assistant tool call running this tool, empty outside a call.
    pub call_id: String,
}

/// Result of a tool call. Tool failures are results, not Rust errors: the model sees them.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutput {
    pub text: String,
    pub is_error: bool,
}

impl ToolOutput {
    pub fn ok(text: impl Into<String>) -> Self {
        ToolOutput {
            text: text.into(),
            is_error: false,
        }
    }
    pub fn err(text: impl Into<String>) -> Self {
        ToolOutput {
            text: text.into(),
            is_error: true,
        }
    }
}

pub trait Tool: Send + Sync {
    fn def(&self) -> ToolDef;
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput>;
}

/// Registered tools by name. Registration returns the previous tool if the name was taken.
#[derive(Default, Clone)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn register(&mut self, tool: impl Tool + 'static) -> Option<Arc<dyn Tool>> {
        self.tools.insert(tool.def().name, Arc::new(tool))
    }

    /// Register a tool already shared with another registry.
    ///
    /// @param tool - The shared tool.
    /// @returns The tool previously registered under the same name.
    pub fn register_shared(&mut self, tool: Arc<dyn Tool>) -> Option<Arc<dyn Tool>> {
        self.tools.insert(tool.def().name, tool)
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn Tool>> {
        self.tools.get(name)
    }

    pub fn defs(&self) -> Vec<ToolDef> {
        self.tools.values().map(|t| t.def()).collect()
    }

    /// Registered tool names in ascending order.
    pub fn names(&self) -> Vec<String> {
        self.tools.keys().cloned().collect()
    }
}
