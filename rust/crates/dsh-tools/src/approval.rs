//! Approval seam: one action that needs wider access than the Session has.
//!
//! A tool that is about to act outside the Session's working directory asks the
//! Host first. The Host answers through the same `$events` waterfall the
//! questions use, audits the decision in the Session log, and fails closed when
//! nobody can answer.

use dsh_core::{BoxFuture, Cancel, ToolContext, ToolOutput};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::{Arc, OnceLock};

/// System-prompt text while every request is rejected.
pub const NEVER_SENTENCE: &str = "Approvals are disabled for this session: every request for wider access is rejected automatically. Do not ask the user for permissions and never claim you were granted any; work inside the access the session already has.";

/// System-prompt text while a client can grant one action.
pub const ASK_SENTENCE: &str = "Actions that reach outside the session's working directory are reviewed first: the Host asks the user, and a rejection, cancellation, or silence means the action did not run.";

/// Effective approval policy of one Session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalPolicy {
    /// A client decides each request; silence fails closed.
    Ask,
    /// Every request is rejected before any client is consulted.
    Never,
}

impl ApprovalPolicy {
    /// Parse a logged or configured policy.
    /// @param value - `ask` or `never`.
    /// @returns The matching policy, or `None` for an unknown value.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "ask" => Some(Self::Ask),
            "never" => Some(Self::Never),
            _ => None,
        }
    }

    /// Wire name of this policy.
    /// @returns `ask` or `never`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Never => "never",
        }
    }

    /// Prompt sentence that tells the model what this policy does.
    /// @returns The deployment-owned sentence.
    #[must_use]
    pub fn sentence(self) -> &'static str {
        match self {
            Self::Ask => ASK_SENTENCE,
            Self::Never => NEVER_SENTENCE,
        }
    }
}

/// Closed set of answers to one approval request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalOutcome {
    /// The user allowed this one action.
    AllowedOnce,
    /// The user refused.
    Rejected,
    /// The turn was cancelled before an answer arrived.
    Cancelled,
    /// No answerer existed; the request fails closed.
    Unavailable,
}

impl ApprovalOutcome {
    /// Normalize a client answer; anything outside the vocabulary fails closed.
    /// @param value - Client answer, when it is a string.
    /// @returns The matching outcome.
    #[must_use]
    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("allowed-once") => Self::AllowedOnce,
            Some("rejected") => Self::Rejected,
            Some("cancelled") => Self::Cancelled,
            _ => Self::Unavailable,
        }
    }

    /// Wire name of this outcome.
    /// @returns The client-vocabulary string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllowedOnce => "allowed-once",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Unavailable => "unavailable",
        }
    }
}

/// One decision a client is asked to make.
pub struct ApprovalRequest<'a> {
    /// Tool whose action needs the decision.
    pub tool_name: &'a str,
    /// Tool call that triggered it.
    pub call_id: &'a str,
    /// What the action would touch, in the user's terms.
    pub reason: &'a str,
    /// Cancellation of the calling turn.
    pub cancel: &'a Cancel,
}

/// Host-side delivery of one approval request.
pub trait Approver: Send + Sync {
    /// Ask one client and return its closed outcome.
    /// @param request - The pending decision.
    /// @returns The outcome; implementations fail closed.
    fn request<'a>(&'a self, request: ApprovalRequest<'a>) -> BoxFuture<'a, ApprovalOutcome>;
}

static INSTALLED: OnceLock<Arc<dyn Approver>> = OnceLock::new();
static DEFAULT_POLICY: OnceLock<ApprovalPolicy> = OnceLock::new();

/// Install the Host's approver. Later calls keep the first installation.
/// @param approver - Delivery of approval requests to a client.
pub fn install_approver(approver: Arc<dyn Approver>) {
    let _ = INSTALLED.set(approver);
}

/// Set the policy used before a Session logs its own.
/// @param policy - Deployment default.
pub fn set_default_policy(policy: ApprovalPolicy) {
    let _ = DEFAULT_POLICY.set(policy);
}

/// The installed approver, when this Host can ask a client.
/// @returns The process-wide approver.
#[must_use]
pub fn installed() -> Option<Arc<dyn Approver>> {
    INSTALLED.get().cloned()
}

/// Deployment default policy: `never` unless the Host configured otherwise.
/// @returns The default policy.
#[must_use]
pub fn default_policy() -> ApprovalPolicy {
    DEFAULT_POLICY
        .get()
        .copied()
        .unwrap_or(ApprovalPolicy::Never)
}

/// Policy a Session's log selects, falling back to the deployment default.
/// @param events - Committed Session events in log order.
/// @returns The effective policy.
#[must_use]
pub fn policy_of(events: &[dsh_session_format::Event]) -> ApprovalPolicy {
    events
        .iter()
        .rev()
        .find(|event| event.kind == "approval/policy")
        .and_then(|event| event.data.get("policy").and_then(Value::as_str))
        .and_then(ApprovalPolicy::parse)
        .unwrap_or_else(default_policy)
}

/// Whether one absolute target stays inside the Session working directory.
/// @param root - Session working directory.
/// @param target - Resolved absolute target path.
/// @returns True when the target is the directory itself or below it.
#[must_use]
pub fn inside(root: impl AsRef<Path>, target: &Path) -> bool {
    let root_path = root.as_ref();
    let root = root_path
        .canonicalize()
        .unwrap_or_else(|_| root_path.to_path_buf());
    let target = target
        .canonicalize()
        .unwrap_or_else(|_| target.to_path_buf());
    target.starts_with(&root)
}

/// Ask before touching a target outside the Session working directory.
///
/// A target inside the Session runs without a question, so a Session that stays
/// inside its working directory never sees an approval prompt.
/// @param cx - Calling tool context.
/// @param tool - Tool name shown to the user.
/// @param target - Resolved absolute target path.
/// @returns `Ok(())` when the action may run, otherwise the model-visible refusal.
pub async fn approve_target(cx: &ToolContext, tool: &str, target: &Path) -> Result<(), ToolOutput> {
    if inside(&cx.cwd, target) {
        return Ok(());
    }
    let reason = format!(
        "`{}` is outside the session working directory `{}`",
        target.display(),
        cx.cwd.display()
    );
    require_approval(cx, tool, &reason).await
}

/// Ask before acting outside the Session's access.
///
/// The audit pair (`approval/asked`, `approval/decided`) is recorded through the
/// call's event sink, so it commits with the rest of the call's events.
/// @param cx - Calling tool context.
/// @param tool - Tool name shown to the user.
/// @param reason - What the action would touch.
/// @returns `Ok(())` when this one action may run, otherwise the model-visible refusal.
pub async fn require_approval(
    cx: &ToolContext,
    tool: &str,
    reason: &str,
) -> Result<(), ToolOutput> {
    let Some(approver) = installed() else {
        return Ok(());
    };
    let policy = cx
        .session
        .as_ref()
        .and_then(|session| session.events().ok())
        .map_or_else(default_policy, |events| policy_of(&events));
    require_approval_with(approver.as_ref(), policy, cx, tool, reason).await
}

/// Ask one explicit approver under one explicit policy.
/// @param approver - Answerer for this request.
/// @param policy - Effective policy of the Session.
/// @param cx - Calling tool context.
/// @param tool - Tool name shown to the user.
/// @param reason - What the action would touch.
/// @returns `Ok(())` when this one action may run, otherwise the refusal.
pub async fn require_approval_with(
    approver: &dyn Approver,
    policy: ApprovalPolicy,
    cx: &ToolContext,
    tool: &str,
    reason: &str,
) -> Result<(), ToolOutput> {
    let id = format!(
        "approval-{}",
        if cx.call_id.is_empty() {
            "call"
        } else {
            &cx.call_id
        }
    );
    let mut asked = json!({"id": id, "toolName": tool, "reason": reason});
    if !cx.call_id.is_empty() {
        asked["callId"] = json!(cx.call_id);
    }
    cx.events.append("approval/asked", asked);
    let outcome = if policy == ApprovalPolicy::Never {
        ApprovalOutcome::Rejected
    } else if cx.cancel.is_cancelled() {
        ApprovalOutcome::Cancelled
    } else {
        approver
            .request(ApprovalRequest {
                tool_name: tool,
                call_id: &cx.call_id,
                reason,
                cancel: &cx.cancel,
            })
            .await
    };
    cx.events.append(
        "approval/decided",
        json!({"id": id, "outcome": outcome.as_str()}),
    );
    match outcome {
        ApprovalOutcome::AllowedOnce => Ok(()),
        ApprovalOutcome::Rejected => Err(ToolOutput::err(format!(
            "Error: the user did not approve {tool} on {reason}. The action did not run; do not retry it without new information."
        ))),
        ApprovalOutcome::Cancelled => Err(ToolOutput::err(format!(
            "Error: the approval request for {tool} on {reason} was cancelled; the action did not run."
        ))),
        ApprovalOutcome::Unavailable => Err(ToolOutput::err(format!(
            "Error: no approval answerer was available for {tool} on {reason}; the request failed closed and the action did not run."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Fixed {
        outcome: Mutex<ApprovalOutcome>,
        asked: Mutex<Vec<String>>,
    }

    impl Approver for Fixed {
        fn request<'a>(&'a self, request: ApprovalRequest<'a>) -> BoxFuture<'a, ApprovalOutcome> {
            self.asked
                .lock()
                .unwrap()
                .push(request.tool_name.to_string());
            let outcome = *self.outcome.lock().unwrap();
            Box::pin(async move { outcome })
        }
    }

    fn installed(outcome: ApprovalOutcome) -> (Arc<Fixed>, Arc<dyn Approver>) {
        let fixed = Arc::new(Fixed {
            outcome: Mutex::new(outcome),
            asked: Mutex::new(Vec::new()),
        });
        let dynamic: Arc<dyn Approver> = fixed.clone();
        (fixed, dynamic)
    }

    #[test]
    fn outcomes_fail_closed_for_anything_outside_the_vocabulary() {
        assert_eq!(
            ApprovalOutcome::parse(Some("allowed-once")),
            ApprovalOutcome::AllowedOnce
        );
        assert_eq!(
            ApprovalOutcome::parse(Some("rejected")),
            ApprovalOutcome::Rejected
        );
        assert_eq!(
            ApprovalOutcome::parse(Some("cancelled")),
            ApprovalOutcome::Cancelled
        );
        assert_eq!(
            ApprovalOutcome::parse(Some("yolo")),
            ApprovalOutcome::Unavailable
        );
        assert_eq!(ApprovalOutcome::parse(None), ApprovalOutcome::Unavailable);
    }

    fn policy_event(policy: &str) -> dsh_session_format::Event {
        dsh_session_format::Event {
            kind: "approval/policy".into(),
            data: json!({ "policy": policy }),
            extra: Default::default(),
        }
    }

    #[test]
    fn a_logged_policy_overrides_the_deployment_default() {
        let mut events = Vec::new();
        events.push(policy_event("ask"));
        assert_eq!(policy_of(&events), ApprovalPolicy::Ask);
        events.push(policy_event("never"));
        assert_eq!(policy_of(&events), ApprovalPolicy::Never);
    }

    #[tokio::test]
    async fn a_granted_request_reports_nothing_and_is_audited() {
        let (fixed, approver) = installed(ApprovalOutcome::AllowedOnce);
        let cx = ToolContext {
            call_id: "call-7".into(),
            ..ToolContext::default()
        };
        require_approval_with(
            approver.as_ref(),
            ApprovalPolicy::Ask,
            &cx,
            "write",
            "outside the workspace",
        )
        .await
        .expect("granted");
        assert_eq!(fixed.asked.lock().unwrap().as_slice(), ["write"]);
        let events = cx.events.drain();
        assert_eq!(events[0].0, "approval/asked");
        assert_eq!(events[1].0, "approval/decided");
        assert_eq!(events[1].1["outcome"], json!("allowed-once"));
    }

    #[tokio::test]
    async fn a_rejection_is_model_visible_and_does_not_run() {
        let (fixed, approver) = installed(ApprovalOutcome::Rejected);
        let cx = ToolContext::default();
        let refused = require_approval_with(
            approver.as_ref(),
            ApprovalPolicy::Ask,
            &cx,
            "write",
            "outside the workspace",
        )
        .await
        .expect_err("refused");
        assert_eq!(fixed.asked.lock().unwrap().len(), 1);
        assert!(refused.is_error);
        assert!(refused.text.contains("did not approve"), "{}", refused.text);
    }

    #[tokio::test]
    async fn the_never_policy_rejects_without_asking_anyone() {
        let (fixed, approver) = installed(ApprovalOutcome::AllowedOnce);
        let cx = ToolContext::default();
        let refused = require_approval_with(
            approver.as_ref(),
            ApprovalPolicy::Never,
            &cx,
            "write",
            "outside the workspace",
        )
        .await
        .expect_err("the never policy rejects");
        assert!(refused.is_error);
        assert!(fixed.asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unavailable_answerer_fails_closed() {
        let (_, approver) = installed(ApprovalOutcome::Unavailable);
        let cx = ToolContext::default();
        let refused = require_approval_with(
            approver.as_ref(),
            ApprovalPolicy::Ask,
            &cx,
            "write",
            "outside the workspace",
        )
        .await
        .expect_err("fails closed");
        assert!(refused.text.contains("failed closed"), "{}", refused.text);
    }

    #[test]
    fn only_paths_below_the_working_directory_are_inside() {
        let root = std::env::temp_dir().join(format!("approval-scope-{}", std::process::id()));
        let nested = root.join("src");
        std::fs::create_dir_all(&nested).expect("workspace");
        assert!(inside(&root, &nested));
        assert!(inside(&root, &root));
        assert!(!inside(&nested, &root));
        let _ = std::fs::remove_dir_all(&root);
    }
}
