//! Context compaction: checkpoint summarization and log-faithful history replacement.
//!
//! Port of `packages/compaction/compaction` events and the `compaction-basic`
//! flow. A compaction shadows a span of conversation messages, asks the routed
//! model for a structured checkpoint, records `compaction/start`,
//! `compaction/summary`, and `compaction/end`, and appends the replacement
//! `user/message` between the last two. `derive_history` then rebuilds the same
//! shorter transcript from the log.

use dsh_llm::{Content, Message};

/// Tag opening the structured summary inside the replacement message.
pub const SUMMARY_OPEN_TAG: &str = "<compacted-summary>";

/// Tag closing the structured summary inside the replacement message.
pub const SUMMARY_CLOSE_TAG: &str = "</compacted-summary>";

/// Framing that presents the replacement message as established background.
pub const CHECKPOINT_PREAMBLE: &str = "This is an automatically generated checkpoint condensing an earlier span of the conversation to free up context. Treat the captured context as established background and build on it without restating it. Continue the task directly from the messages that follow, without acknowledging this checkpoint.";

/// The summarization directive appended after the replayed conversation.
pub const COMPACTION_INSTRUCTION: &str = concat!(
    "You are now acting as a compaction engine for this AI coding assistant. ",
    "Condense the conversation ABOVE into a structured checkpoint that lets another model resume the work with no loss of essential context.\n\n",
    "Output EXACTLY the Markdown structure below: keep every section, in order. Use terse bullets, not prose paragraphs. ",
    "Write \"(none)\" for an empty section — never drop a section.\n\n",
    "## Primary Request and Intent\n",
    "- [the user's original and evolving goals; quote verbatim where the exact wording matters]\n\n",
    "## Key Technical Concepts\n",
    "- [technologies, frameworks, patterns, and conventions in play]\n\n",
    "## Files and Code\n",
    "- [exact path: why it matters, key changes or snippets]\n\n",
    "## Errors and Fixes\n",
    "- [error: how it was resolved, plus any related user feedback]\n\n",
    "## Pending Jobs\n",
    "- [explicitly requested work not yet completed]\n\n",
    "## Current Work\n",
    "- [precisely what was in progress at this checkpoint]\n\n",
    "## Next Step\n",
    "- [the single next action, directly in line with the most recent request, or \"(none)\"]\n\n",
    "## Critical Context\n",
    "- [decisions and their rationale, constraints, user preferences, open questions, data needed to continue]\n\n",
    "Rules:\n",
    "- Write concise English engineering prose. Preserve exact file paths, commands, error strings, identifiers, numeric values, function signatures, and syntax fragments.\n",
    "- Capture user feedback and explicit instructions faithfully, especially corrections.\n",
    "- Do NOT mention this summarization request or that the context was compacted.\n",
    "- Output only the checkpoint text: do not call any tool or take any other action.\n",
    "- If the conversation already contains a <compacted-summary> block, it is a PRIOR checkpoint. ",
    "Do not copy it forward verbatim: preserve still-true facts, drop stale ones, and merge newer information into a single consolidated summary under the same structure.",
);

/// Compaction tuning for one Agent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionConfig {
    /// Estimated prompt tokens above which a turn compacts before requesting.
    pub threshold_tokens: usize,
    /// Trailing messages kept verbatim; earlier complete user turns are summarized.
    pub keep_recent_messages: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            threshold_tokens: 96_000,
            keep_recent_messages: 12,
        }
    }
}

/// Estimate prompt tokens with the fixed four-characters-per-token heuristic.
///
/// The heuristic matches the TypeScript token meter's estimator: it is
/// deliberately conservative and independent of the provider's reported usage,
/// so the same log yields the same decision on replay.
#[must_use]
pub fn estimate_tokens(messages: &[Message]) -> usize {
    messages.iter().map(estimate_message_tokens).sum()
}

/// Estimate one message's tokens with the fixed heuristic.
#[must_use]
pub fn estimate_message_tokens(message: &Message) -> usize {
    message
        .content
        .iter()
        .map(|content| match content {
            Content::Text { text } => text.chars().count(),
            Content::Reasoning { text, .. } => text.chars().count(),
            Content::ToolCall {
                name, arguments, ..
            } => name.chars().count() + arguments.to_string().chars().count(),
            _ => 0,
        })
        .sum::<usize>()
        .div_ceil(4)
}

/// Choose the history index where the compacted span ends.
///
/// The kept suffix holds at least `keep_recent_messages` messages and starts at
/// a user message, so a kept turn never begins with a tool result whose call
/// was summarized. Returns `0` when no such boundary exists.
#[must_use]
pub fn split_index(messages: &[Message], keep_recent_messages: usize) -> usize {
    if messages.len() <= keep_recent_messages {
        return 0;
    }
    let upper = messages.len() - keep_recent_messages;
    (1..=upper)
        .rev()
        .find(|index| {
            messages[*index].role == dsh_llm::Role::User && suffix_is_balanced(&messages[*index..])
        })
        .unwrap_or(0)
}

/// Whether every tool result in the suffix answers a tool call in the suffix.
fn suffix_is_balanced(messages: &[Message]) -> bool {
    let calls: std::collections::BTreeSet<&str> = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::ToolCall { id, .. } => Some(id.as_str()),
            _ => None,
        })
        .collect();
    messages
        .iter()
        .filter_map(|message| message.tool_call_id.as_deref())
        .all(|id| calls.contains(id))
}

/// Wrap summarized text in the durable checkpoint framing.
#[must_use]
pub fn frame_summary(summary: &str) -> String {
    format!(
        "{CHECKPOINT_PREAMBLE}\n\n{SUMMARY_OPEN_TAG}\n{}\n{SUMMARY_CLOSE_TAG}",
        summary.trim()
    )
}

/// Build the summarization request: replayed conversation then the directive.
#[must_use]
pub fn summarization_request(shadowed: &[Message]) -> Vec<Message> {
    let mut request = Vec::with_capacity(shadowed.len() + 1);
    request.extend_from_slice(shadowed);
    request.push(Message {
        role: dsh_llm::Role::User,
        content: vec![Content::Text {
            text: COMPACTION_INSTRUCTION.to_string(),
        }],
        tool_call_id: None,
        is_error: None,
    });
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsh_llm::Role;

    fn text(role: Role, text: &str) -> Message {
        Message {
            role,
            content: vec![Content::Text {
                text: text.to_string(),
            }],
            tool_call_id: None,
            is_error: None,
        }
    }

    #[test]
    fn estimates_four_characters_per_token() {
        let message = text(Role::User, "abcdefgh");
        assert_eq!(estimate_message_tokens(&message), 2);
    }

    #[test]
    fn split_keeps_the_requested_recent_messages_at_a_user_boundary() {
        let messages = vec![
            text(Role::User, "one"),
            text(Role::Assistant, "answer"),
            text(Role::User, "two"),
            text(Role::Assistant, "answer"),
            text(Role::User, "three"),
        ];
        assert_eq!(split_index(&messages, 2), 2);
        assert_eq!(split_index(&messages, 3), 2);
        assert_eq!(split_index(&messages, 5), 0);
    }

    #[test]
    fn split_never_starts_the_kept_suffix_with_a_tool_result() {
        let messages = vec![
            text(Role::User, "one"),
            text(Role::Assistant, "call"),
            text(Role::User, "two"),
            Message {
                role: Role::Tool,
                content: vec![Content::Text {
                    text: "result".into(),
                }],
                tool_call_id: Some("call-1".into()),
                is_error: None,
            },
        ];
        assert_eq!(split_index(&messages, 1), 0);
        assert_eq!(split_index(&messages, 2), 0);
    }

    #[test]
    fn frame_wraps_summary_in_checkpoint_tags() {
        let framed = frame_summary("  ## Current Work\n- porting  ");
        assert!(framed.starts_with(CHECKPOINT_PREAMBLE));
        assert!(framed
            .contains("<compacted-summary>\n## Current Work\n- porting\n</compacted-summary>"));
    }

    #[test]
    fn summarization_request_appends_the_directive_last() {
        let request = summarization_request(&[text(Role::User, "work")]);
        assert_eq!(request.len(), 2);
        assert_eq!(request[0].role, Role::User);
        assert_eq!(request[1].role, Role::User);
        assert!(request[1].content.iter().any(|content| matches!(content,
            Content::Text { text } if text == COMPACTION_INSTRUCTION)));
    }
}
