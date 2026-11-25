//! Agent loop, tools and event bus. Port of `packages/core/*`.

pub mod agent;
pub mod assistant_stream;
pub mod bus;
pub mod compaction;
pub mod instructions;
pub mod system_prompt;
pub mod tools;

pub use agent::{
    derive_history, history_with_seqs, Agent, AgentAdmission, AgentConfig, AgentEvent, CoreError,
    InboxMessage, InboxState, InboxTarget, Model, PromptRequest, QueuePlacement, SessionQueuedItem,
    TurnEnd, UserInput,
};
pub use assistant_stream::{
    expand_assistant_stream, AssistantSettlementEventType, AssistantStreamAccumulator,
    AssistantStreamError, AssistantStreamOutcome, AssistantStreamRecord,
    SessionAssistantStreamAccumulator, SessionAssistantStreamAttempt,
    SessionAssistantStreamBaseline, SessionAssistantStreamFrame, TimedStreamChunk,
};
pub use compaction::{
    estimate_tokens, frame_summary, split_index, CompactionConfig, CHECKPOINT_PREAMBLE,
    COMPACTION_INSTRUCTION, SUMMARY_CLOSE_TAG, SUMMARY_OPEN_TAG,
};
pub use system_prompt::{
    assemble_agent_prompt, tool_guidance, workspace_instructions, PromptSection,
    SystemPromptAssembler, SystemPromptAssembly, SystemPromptError, ToolPromptSection, IDENTITY,
};
pub use tools::{
    BoxFuture, Cancel, EventSink, SessionFacts, Tool, ToolContext, ToolOutput, ToolRegistry,
};
