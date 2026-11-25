use crate::{PlanningError, PlanningResult, MAX_SAFE_SESSION_INTEGER};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

pub(crate) fn validate_safe(value: u64, field: &'static str) -> PlanningResult<u64> {
    if value > MAX_SAFE_SESSION_INTEGER {
        return Err(PlanningError::invalid(
            field,
            "exceeds the Session safe-integer range",
        ));
    }
    Ok(value)
}

pub(crate) fn validate_positive(value: u64, field: &'static str) -> PlanningResult<u64> {
    if value == 0 {
        return Err(PlanningError::invalid(field, "must be positive"));
    }
    validate_safe(value, field)
}

pub(crate) fn object(value: Value, field: &'static str) -> PlanningResult<Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| PlanningError::invalid(field, "must be an object"))
}

pub(crate) fn exact_keys(
    value: &Map<String, Value>,
    required: &[&str],
    optional: &[&str],
    field: &'static str,
) -> PlanningResult<()> {
    for key in required {
        if !value.contains_key(*key) {
            return Err(PlanningError::invalid(
                field,
                format!("missing required key {key}"),
            ));
        }
    }
    for key in value.keys() {
        if !required.contains(&key.as_str()) && !optional.contains(&key.as_str()) {
            return Err(PlanningError::invalid(
                field,
                format!("contains unknown key {key}"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn field<T: DeserializeOwned>(
    value: &Map<String, Value>,
    key: &str,
    context: &'static str,
) -> PlanningResult<T> {
    let raw = value
        .get(key)
        .ok_or_else(|| PlanningError::invalid(context, format!("missing required key {key}")))?;
    serde_json::from_value(raw.clone())
        .map_err(|error| PlanningError::invalid(context, format!("{key} is malformed: {error}")))
}

pub(crate) fn optional_field<T: DeserializeOwned>(
    value: &Map<String, Value>,
    key: &str,
    context: &'static str,
) -> PlanningResult<Option<T>> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::Null) => Err(PlanningError::invalid(
            context,
            format!("{key} must be absent rather than null"),
        )),
        Some(raw) => serde_json::from_value(raw.clone()).map_err(|error| {
            PlanningError::invalid(context, format!("{key} is malformed: {error}"))
        }),
    }
}

pub(crate) fn string_field(
    value: &Map<String, Value>,
    key: &str,
    context: &'static str,
) -> PlanningResult<String> {
    field(value, key, context)
}

pub(crate) fn normalized_nonempty(value: &str, field: &'static str) -> PlanningResult<()> {
    if value.is_empty() || value.trim() != value {
        return Err(PlanningError::invalid(
            field,
            "must be non-empty and already trimmed",
        ));
    }
    Ok(())
}

pub(crate) fn deserialize_value<'de, D>(deserializer: D) -> PlanningResult<Value>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer)
        .map_err(|error| PlanningError::invalid("wire", error.to_string()))
}
