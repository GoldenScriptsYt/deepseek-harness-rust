use crate::ids::CommandId;
use crate::util::{
    deserialize_value, exact_keys, field, object, optional_field, string_field, validate_safe,
};
use crate::{PlanningError, PlanningResult};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

/// Strict `command/run` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandRun {
    pub command_id: CommandId,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
}

impl CommandRun {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        let bytes = self.name.as_bytes();
        if bytes.is_empty()
            || !bytes[0].is_ascii_lowercase()
            || !bytes.iter().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(*byte, b'-' | b'_')
            })
        {
            return Err(PlanningError::invalid(
                "command/run.name",
                "must be a lowercase command name without a leading slash",
            ));
        }
        Ok(())
    }

    pub(crate) fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "command/run")?;
        exact_keys(
            &map,
            &["commandId", "name", "source"],
            &["args"],
            "command/run",
        )?;
        let source = map
            .get("source")
            .ok_or_else(|| PlanningError::invalid("command/run", "missing source"))?;
        let source = object(source.clone(), "command/run.source")?;
        exact_keys(&source, &["kind"], &[], "command/run.source")?;
        let kind: String = field(&source, "kind", "command/run.source")?;
        if kind != "user" {
            return Err(PlanningError::invalid(
                "command/run.source.kind",
                "must be user",
            ));
        }
        let command = Self {
            command_id: field(&map, "commandId", "command/run")?,
            name: string_field(&map, "name", "command/run")?,
            args: optional_field(&map, "args", "command/run")?,
        };
        command.validate()?;
        Ok(command)
    }

    pub(crate) fn to_value(&self) -> PlanningResult<Value> {
        self.validate()?;
        let mut map = Map::new();
        map.insert(
            "commandId".into(),
            serde_json::to_value(&self.command_id).map_err(crate::PlanningError::json)?,
        );
        map.insert("name".into(), Value::String(self.name.clone()));
        if let Some(args) = &self.args {
            map.insert("args".into(), Value::String(args.clone()));
        }
        let mut source = Map::new();
        source.insert("kind".into(), Value::String("user".into()));
        map.insert("source".into(), Value::Object(source));
        Ok(Value::Object(map))
    }
}

impl<'de> Deserialize<'de> for CommandRun {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_value(deserialize_value(deserializer).map_err(D::Error::custom)?)
            .map_err(D::Error::custom)
    }
}

/// Terminal outcome of one command execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandDoneKind {
    Success,
    Error,
}

/// Strict `command/done` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandDone {
    pub command_id: CommandId,
    pub kind: CommandDoneKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_event_seq: Option<u64>,
}

impl CommandDone {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        match self.kind {
            CommandDoneKind::Success => {
                if let Some(seq) = self.source_event_seq {
                    validate_safe(seq, "command/done.sourceEventSeq")?;
                }
            }
            CommandDoneKind::Error => {
                if self.source_event_seq.is_some() {
                    return Err(PlanningError::invalid(
                        "command/done.sourceEventSeq",
                        "is forbidden for an error outcome",
                    ));
                }
                let text = self.text.as_deref().ok_or_else(|| {
                    PlanningError::invalid("command/done.text", "is required for an error outcome")
                })?;
                if text.trim().is_empty() {
                    return Err(PlanningError::invalid(
                        "command/done.text",
                        "must be non-empty for an error outcome",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn from_value(value: Value) -> PlanningResult<Self> {
        let map = object(value, "command/done")?;
        exact_keys(
            &map,
            &["commandId", "kind"],
            &["text", "sourceEventSeq"],
            "command/done",
        )?;
        let kind: String = string_field(&map, "kind", "command/done")?;
        let kind = match kind.as_str() {
            "success" => CommandDoneKind::Success,
            "error" => CommandDoneKind::Error,
            _ => return Err(PlanningError::invalid("command/done.kind", "is invalid")),
        };
        let command = Self {
            command_id: field(&map, "commandId", "command/done")?,
            kind,
            text: optional_field(&map, "text", "command/done")?,
            source_event_seq: optional_field(&map, "sourceEventSeq", "command/done")?,
        };
        command.validate()?;
        Ok(command)
    }

    pub(crate) fn to_value(&self) -> PlanningResult<Value> {
        self.validate()?;
        let mut map = Map::new();
        map.insert(
            "commandId".into(),
            serde_json::to_value(&self.command_id).map_err(crate::PlanningError::json)?,
        );
        map.insert(
            "kind".into(),
            Value::String(
                match self.kind {
                    CommandDoneKind::Success => "success",
                    CommandDoneKind::Error => "error",
                }
                .into(),
            ),
        );
        if let Some(text) = &self.text {
            map.insert("text".into(), Value::String(text.clone()));
        }
        if let Some(seq) = self.source_event_seq {
            map.insert("sourceEventSeq".into(), Value::from(seq));
        }
        Ok(Value::Object(map))
    }
}

impl<'de> Deserialize<'de> for CommandDone {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_value(deserialize_value(deserializer).map_err(D::Error::custom)?)
            .map_err(D::Error::custom)
    }
}

/// Whole-value `plan/mode` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanMode {
    pub active: bool,
}

/// One `/plan` command awaiting settlement or mode commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunningPlanCommand {
    pub command_id: CommandId,
    pub wanted: bool,
}

/// Checkpoint-safe plan-mode unit state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanState {
    pub active: bool,
    pub wanted: Option<bool>,
    pub running: Option<RunningPlanCommand>,
    pub entered: bool,
}

impl PlanState {
    pub(crate) fn validate(&self) -> PlanningResult<()> {
        if self.active && !self.entered {
            return Err(PlanningError::invalid(
                "plan.entered",
                "must remain true while plan mode is active",
            ));
        }
        Ok(())
    }

    pub(crate) fn apply_run(&mut self, command: &CommandRun) {
        if command.name == "plan" {
            if let Some(args) = &command.args {
                self.running = Some(RunningPlanCommand {
                    command_id: command.command_id.clone(),
                    wanted: args.trim() != "off",
                });
            }
        }
    }

    pub(crate) fn apply_done(&mut self, command: &CommandDone) {
        if let Some(running) = self.running.take() {
            self.wanted =
                if command.kind == CommandDoneKind::Success && running.wanted != self.active {
                    Some(running.wanted)
                } else {
                    None
                };
        }
    }

    pub(crate) fn apply_mode(&mut self, mode: &PlanMode) {
        self.active = mode.active;
        self.wanted = None;
        self.entered |= mode.active;
    }

    /// Cropped client view. A running selection takes precedence over a settled one.
    pub fn projection(&self) -> PlanProjection {
        let wanted = self
            .running
            .as_ref()
            .map(|running| running.wanted)
            .or(self.wanted);
        PlanProjection {
            active: self.active,
            pending: wanted.is_some_and(|wanted| wanted != self.active),
        }
    }
}

/// Browser-safe plan view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanProjection {
    pub active: bool,
    pub pending: bool,
}
/// Fold the plan-mode unit state from committed Session events.
///
/// Command payloads that fail validation leave the state unchanged, so one
/// malformed row cannot hide a later valid mode change.
///
/// @param events - Committed Session rows in order.
/// @returns The logged state, pending selection, and first-entry flag.
#[must_use]
pub fn fold_plan(events: &[dsh_session_format::Event]) -> PlanState {
    let mut state = PlanState::default();
    for event in events {
        match event.kind.as_str() {
            "command/run" => {
                if let Ok(command) = CommandRun::from_value(event.data.clone()) {
                    state.apply_run(&command);
                }
            }
            "command/done" => {
                if let Ok(command) = CommandDone::from_value(event.data.clone()) {
                    state.apply_done(&command);
                }
            }
            "plan/mode" => {
                // A row written by an older build may not decode; the fold keeps
                // the state it already has rather than failing the whole replay.
                if let Ok(mode) = serde_json::from_value::<PlanMode>(event.data.clone()) {
                    state.apply_mode(&mode);
                }
            }
            _ => {}
        }
    }
    state
}
#[cfg(test)]
mod tests {
    use super::*;
    use dsh_session_format::Event;

    fn event(seq: u64, kind: &str, data: Value) -> Event {
        serde_json::from_value(serde_json::json!({
            "type": kind,
            "data": data,
            "extra": {"seq": seq, "time": seq},
            "time": seq,
        }))
        .expect("event parses")
    }

    fn run(seq: u64, command_id: &str, args: &str) -> Event {
        event(
            seq,
            "command/run",
            serde_json::json!({
                "commandId": command_id,
                "name": "plan",
                "args": args,
                "source": {"kind": "user"},
            }),
        )
    }

    fn done(seq: u64, command_id: &str) -> Event {
        event(
            seq,
            "command/done",
            serde_json::json!({"commandId": command_id, "kind": "success"}),
        )
    }

    #[test]
    fn a_settled_selection_without_a_mode_commit_stays_pending() {
        let state = fold_plan(&[run(0, "cmd-1", ""), done(1, "cmd-1")]);
        assert!(!state.active);
        assert_eq!(state.wanted, Some(true));
        assert_eq!(
            state.projection(),
            PlanProjection {
                active: false,
                pending: true
            }
        );
    }

    #[test]
    fn a_mode_commit_clears_the_pending_selection_and_keeps_the_entry() {
        let events = [
            run(0, "cmd-1", ""),
            done(1, "cmd-1"),
            event(2, "plan/mode", serde_json::json!({"active": true})),
        ];
        let state = fold_plan(&events);
        assert!(state.active && state.entered);
        assert_eq!(
            state.projection(),
            PlanProjection {
                active: true,
                pending: false
            }
        );

        let exits = [
            run(3, "cmd-2", " off"),
            done(4, "cmd-2"),
            event(5, "plan/mode", serde_json::json!({"active": false})),
        ];
        let mut all = events.to_vec();
        all.extend(exits);
        let state = fold_plan(&all);
        assert!(!state.active, "the exit commits");
        assert!(state.entered, "guidance stays after the first entry");
    }

    #[test]
    fn a_repeated_selection_keeps_the_logged_state() {
        let state = fold_plan(&[run(0, "cmd-1", "off"), done(1, "cmd-1")]);
        assert_eq!(state.wanted, None);
        assert_eq!(
            state.projection(),
            PlanProjection {
                active: false,
                pending: false
            }
        );
    }

    #[test]
    fn malformed_rows_leave_the_state_unchanged() {
        let events = [
            event(0, "command/run", serde_json::json!({"name": "plan"})),
            event(1, "plan/mode", serde_json::json!({"active": "yes"})),
        ];
        let state = fold_plan(&events);
        assert_eq!(state, PlanState::default());
    }
}
