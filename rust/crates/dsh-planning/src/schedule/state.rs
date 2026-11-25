use super::types::{ScheduleChange, ScheduleEntry};
use crate::ids::ScheduleId;
use crate::{PlanningError, PlanningResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Deterministic, bounded Schedule entry collection keyed by stable task id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScheduleState {
    pub entries: BTreeMap<ScheduleId, ScheduleEntry>,
}

impl ScheduleState {
    pub(crate) fn validate(&self, max_entries: usize, max_involved: usize) -> PlanningResult<()> {
        if self.entries.len() > max_entries {
            return Err(PlanningError::LimitExceeded {
                field: "schedule entries",
                limit: max_entries,
            });
        }
        for (key, entry) in &self.entries {
            if key != entry.record.id() {
                return Err(PlanningError::invalid(
                    "schedule.entries",
                    "map key must equal the contained record id",
                ));
            }
            entry.validate(max_involved)?;
        }
        Ok(())
    }

    pub(crate) fn apply(
        &mut self,
        change: &ScheduleChange,
        max_entries: usize,
        max_involved: usize,
    ) -> PlanningResult<()> {
        match change {
            ScheduleChange::Put(entry) => {
                entry.validate(max_involved)?;
                let id = entry.record.id().clone();
                if !self.entries.contains_key(&id) && self.entries.len() >= max_entries {
                    return Err(PlanningError::LimitExceeded {
                        field: "schedule entries",
                        limit: max_entries,
                    });
                }
                self.entries.insert(id, entry.clone());
            }
            ScheduleChange::Delete(id) => {
                if id.as_str().is_empty() || id.as_str().trim() != id.as_str() {
                    return Err(PlanningError::invalid(
                        "schedule.id",
                        "must be non-empty without surrounding whitespace",
                    ));
                }
                self.entries.remove(id);
            }
        }
        Ok(())
    }
}
