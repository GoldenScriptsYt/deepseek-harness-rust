//! Plan-mode guidance and notices owned by the Rust Host.
//!
//! The guidance text is the shipped deployment section from the standard agent
//! preset; it enters the system prompt only while the Session has entered plan
//! mode, and the log records each change.

use dsh_planning::PlanState;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};

/// Deployment-owned plan guidance, matching the shipped preset section.
pub(crate) const PLAN_SECTION: &str = "\
You are in plan mode. Stay in plan mode until the user approves your plan through exit_plan_mode or switches the session mode. Imperative language to implement changes means plan the implementation, not execute it. A user's conversational agreement — including an answer confirming something you asked — approves nothing and does not end plan mode; fold the confirmed decision into the plan and submit it through exit_plan_mode.\
\
Explore first. Use non-mutating reads, searches, static analysis, and checks to ground the plan in the actual repository. Do not edit or write files, change configuration, run formatters or code generation that rewrites tracked files, commit, or otherwise carry out the plan. Prefer existing functions and patterns over new machinery.\
\
The tool catalog stays the same across modes for request-cache stability. These plan-mode rules override any later tool description or guidance that suggests using mutation tools; those tools remain listed to keep the tool catalog unchanged. Do not use todo_write to track this planning phase: it tracks implementation after an approved plan, while the plan itself belongs in exit_plan_mode.\
\
Resolve discoverable facts by inspection. Use ask_user_question only for user-owned choices or material ambiguity that inspection cannot answer. Do not ask the user where code lives or how current behavior works when you can find out.\
\
Make the plan decision-complete: state the goal and success criteria; group implementation changes by subsystem; identify public API, schema, and data-flow changes; cover edge cases, failure modes, tests, acceptance criteria, and explicit assumptions. Keep it concise enough to review but detailed enough that another engineer can implement it without making design decisions.\
\
When ready, call exit_plan_mode with the complete plan markdown, starting with a # title. Make exit_plan_mode the only and final tool call in that assistant response: it presents the plan for approval, and implementation begins only in a later step after approval. Do not paste the final plan as a plain reply or ask \"should I proceed?\" through prose or ask_user_question. If review rejects it, incorporate the feedback and present again. If the review channel is unavailable or aborted, stay in plan mode and ask the user to switch modes manually; do not proceed with implementation.";

/// Conditional system-prompt guidance for the current plan-mode state.
///
/// `pending` is the selection awaiting the next turn boundary, which the
/// upcoming request already follows.
/// @param state - folded plan-mode state.
/// @param pending - selection awaiting commit, when present.
/// @returns Text to append to the system prompt, or an empty string.
pub(crate) fn plan_guidance(state: &PlanState, pending: Option<bool>) -> String {
    let active = pending.unwrap_or(state.active);
    if !state.entered && !active {
        return String::new();
    }
    let status = if active {
        "Plan mode is active. Follow the plan-mode instructions."
    } else {
        "Plan mode is inactive. The plan-mode instructions no longer apply."
    };
    format!("The following instructions apply only while plan mode is active.\n\n{PLAN_SECTION}\n\n{status}")
}

/// The user-visible notice accompanying a committed selection.
/// @param active - state the Session moved to.
/// @returns The `user/message` payload to log.
pub(crate) fn plan_narration(active: bool) -> Value {
    let text = if active {
        "The user switched this session to plan mode."
    } else {
        "The user switched this session back to the default mode."
    };
    json!({
        "id": message_id(),
        "role": "user",
        "content": [{"type": "text", "text": text}],
        "source": {"kind": "plan-mode", "form": "notice", "summary": text},
    })
}

/// A Host-owned plan message identity.
pub(crate) fn message_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("msg-plan-{}", NEXT.fetch_add(1, Ordering::Relaxed) + 1)
}
