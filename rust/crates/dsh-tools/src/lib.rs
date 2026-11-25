//! Built-in tools. Argument names and `read` output follow the TypeScript tools
//! (`fs/tool-fs`, `fs/tool-fs-search`, `shell/tool-bash`) so recorded model behavior carries over.

mod apply_patch;
mod approval;
mod ask_user_question;
mod cordis;
mod fs_patch;
mod fs_safety;
mod fs_tools;
mod goal;
mod jobs;
mod plan_review;
mod search;
mod shell;
mod subagent;
mod terminal;
mod todo;

pub use apply_patch::ApplyPatch;
pub use approval::{
    approve_target, default_policy, inside, install_approver, policy_of, require_approval,
    require_approval_with, set_default_policy, ApprovalOutcome, ApprovalPolicy, ApprovalRequest,
    Approver, ASK_SENTENCE, NEVER_SENTENCE,
};
pub use ask_user_question::{
    ask_schema, AskOutcome, AskUserQuestion, QuestionAsker, QuestionRequest, DEFAULT_TIMEOUT_SECS,
    MAX_TIMEOUT_SECS, PENDING_NOTICE, SKIPPED_ANSWER_GUIDANCE, TIMED_WAIT_PARAMETER,
};
pub use cordis::CordisInspect;
pub use fs_tools::{Edit, Read, Write};
pub use goal::{CreateGoal, GetGoal, UpdateGoal};
pub use jobs::{
    CancellationToken, JobChannel, JobError, JobEvent, JobEventFilter, JobHandle, JobHooks,
    JobOutcome, JobOutputChunk, JobOutputMetadata, JobOutputRead, JobOutputSource, JobRead,
    JobRegistry, JobRegistryConfig, JobSourceRead, JobSpec, JobStatus, JobSubscription, JobView,
    Owner, SettlementCause,
};
pub use plan_review::{plan_mode_active, review_question, ExitPlanMode};
pub use search::{Glob, Grep};
pub use shell::{Bash, Pwsh};
pub use subagent::{Subagent, SubagentRunner, SubagentTask};
pub use terminal::{
    LocalTerminalBackend, LocalTerminalConfig, TerminalBackend, TerminalBackendSession,
    TerminalBackendSpawnSpec, TerminalCommand, TerminalError, TerminalReadResult, TerminalRegistry,
    TerminalRegistryConfig, TerminalSendOperation, TerminalSendOperationHandle,
    TerminalSendRequest, TerminalSendResult, TerminalSessionSnapshot, TerminalSessionStatus,
    TerminalSignal, TerminalSignalResult, TerminalSize, TerminalSpawnRequest, TerminalSpawnResult,
    TerminalWaitReason,
};
pub use todo::Todo;

use dsh_core::ToolRegistry;
use fs_safety::FsState;
use std::sync::Arc;

/// Agent tool composition selected by a Session preset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolPreset {
    /// Minimal terminal-first tool composition.
    Minimal,
    /// Standard coding tools.
    Standard,
    /// Standard tools used by the PTC composition until its runtime is ported.
    Ptc,
    /// Standard tools used by the Cordis development composition.
    Cordis,
}

impl ToolPreset {
    /// Parse a preset id from a durable Session or RPC request.
    /// @param id - Durable or requested preset id.
    /// @returns The matching Rust tool composition, or `None` when this Host cannot run it.
    pub fn parse(id: &str) -> Option<Self> {
        match id {
            "minimal" => Some(Self::Minimal),
            "standard" => Some(Self::Standard),
            "ptc" => Some(Self::Ptc),
            "cordis" => Some(Self::Cordis),
            _ => None,
        }
    }
}

/// Register every built-in tool. Mutating tools share one owner for target locks.
pub fn register_all(reg: &mut ToolRegistry) {
    let state = Arc::new(FsState::default());
    reg.register(Read);
    reg.register(Write::new(state.clone()));
    reg.register(Edit::new(state.clone()));
    reg.register(Glob);
    reg.register(Grep);
    reg.register(ApplyPatch::new(state));
    register_shell(reg);
    reg.register(Todo::default());
}

/// Register the model-facing Goal tools.
///
/// The Host adds these only when it can hand the tools a committed Session log;
/// a deployment without one (the headless runner) must not advertise them.
/// @param reg - Registry that receives the Goal tools.
/// Register the platform's shell tool: `pwsh` on Windows, `bash` elsewhere.
///
/// The presets enable exactly one of the two, matching the platform-disabled
/// pair the TypeScript compositions declare, so the model never sees a shell
/// name whose commands this Host would run through a different interpreter.
/// @param reg - Registry that receives the platform shell tool.
pub fn register_shell(reg: &mut ToolRegistry) {
    #[cfg(windows)]
    reg.register(Pwsh);
    #[cfg(not(windows))]
    reg.register(Bash);
}

pub fn register_goal_tools(reg: &mut ToolRegistry) {
    reg.register(GetGoal);
    reg.register(CreateGoal);
    reg.register(UpdateGoal);
}

/// Register the delegation tool when the Host can run a child Agent.
///
/// The built-in delegation tool is model-visible only in compositions whose
/// runtime can start a child Session, so a Host without one omits it entirely.
/// @param reg - Registry that receives `subagent`.
/// @param runner - Executor for delegated tasks.
pub fn register_subagent_tools(reg: &mut ToolRegistry, runner: Arc<dyn SubagentRunner>) {
    reg.register(Subagent::new(runner));
}

/// Register tools for one supported Agent preset.
/// @param reg - Registry that receives the selected preset's tools.
/// @param preset - Agent preset selected for this Session.
/// @returns `()` after registering its Rust tool composition.
pub fn register_for_preset(reg: &mut ToolRegistry, preset: ToolPreset) {
    if preset == ToolPreset::Minimal {
        // The built-in Minimal preset works through a terminal, rather than
        // loading Standard's file-management tools.
        register_shell(reg);
    } else {
        register_all(reg);
        if preset == ToolPreset::Cordis {
            let tools = reg.clone();
            reg.register(CordisInspect::new(tools));
        }
    }
}

pub(crate) fn str_arg<'a>(args: &'a serde_json::Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("Error: missing or non-string argument \"{key}\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn minimal_preset_exposes_only_the_terminal_tool() {
        let mut tools = ToolRegistry::default();
        register_for_preset(&mut tools, ToolPreset::Minimal);
        let names: BTreeSet<_> = tools.defs().into_iter().map(|tool| tool.name).collect();
        let shell = if cfg!(windows) { "pwsh" } else { "bash" };
        assert_eq!(names, BTreeSet::from([shell.to_owned()]));
    }

    #[test]
    fn non_minimal_presets_keep_the_standard_rust_toolset() {
        let mut expected = ToolRegistry::default();
        register_all(&mut expected);
        let expected: BTreeSet<_> = expected.defs().into_iter().map(|tool| tool.name).collect();

        for preset in [ToolPreset::Standard, ToolPreset::Ptc] {
            let mut actual = ToolRegistry::default();
            register_for_preset(&mut actual, preset);
            let actual: BTreeSet<_> = actual.defs().into_iter().map(|tool| tool.name).collect();
            assert_eq!(actual, expected, "{preset:?}");
        }

        let mut cordis = ToolRegistry::default();
        register_for_preset(&mut cordis, ToolPreset::Cordis);
        let cordis: BTreeSet<_> = cordis.defs().into_iter().map(|tool| tool.name).collect();
        assert!(cordis.contains("cordis_inspect"));
        assert!(expected.is_subset(&cordis));
    }
}
