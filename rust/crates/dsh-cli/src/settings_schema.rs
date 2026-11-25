//! Validation for the serialized Schemastery graphs captured by `settings/describe`.

use serde_json::{Map, Value};

const LOCALE_PATTERN: &str = "^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$";
const MAX_DEPTH: usize = 128;

pub(super) struct Schema<'a> {
    uid: &'a Value,
    refs: &'a Map<String, Value>,
}

impl<'a> Schema<'a> {
    pub(super) fn new(source: &'a Value) -> Result<Self, String> {
        let uid = source
            .get("uid")
            .ok_or_else(|| "settings schema has no root uid".to_string())?;
        let refs = source
            .get("refs")
            .and_then(Value::as_object)
            .ok_or_else(|| "settings schema has no refs".to_string())?;
        if !refs.contains_key(&reference_key(uid)) {
            return Err(format!("settings schema root {uid} is not defined"));
        }
        Ok(Self { uid, refs })
    }

    pub(super) fn validate_value(&self, value: &Value) -> Result<(), String> {
        self.validate_node(self.uid, value, "$", 0)
    }

    pub(super) fn validate_path(&self, path: &[String]) -> Result<(), String> {
        self.validate_node_path(self.uid, path, 0)
    }

    fn node(&self, reference: &Value) -> Result<&'a Value, String> {
        self.refs
            .get(&reference_key(reference))
            .ok_or_else(|| format!("settings schema reference {reference} is not defined"))
    }

    fn validate_node(
        &self,
        reference: &Value,
        value: &Value,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        if depth > MAX_DEPTH {
            return Err(format!(
                "settings schema nesting exceeds {MAX_DEPTH} at {path}"
            ));
        }
        let node = self.node(reference)?;
        let kind = node["type"]
            .as_str()
            .ok_or_else(|| format!("settings schema node {reference} has no type"))?;
        let meta = node.get("meta").unwrap_or(&Value::Null);
        let required = meta["required"].as_bool().unwrap_or(false);

        if value.is_null() && !required && kind != "const" {
            return Ok(());
        }

        let result = match kind {
            "any" => Ok(()),
            "const" => {
                let expected = node
                    .get("value")
                    .ok_or_else(|| format!("settings const {reference} has no value"))?;
                if values_equal(value, expected) {
                    Ok(())
                } else {
                    Err(type_error(path, &format!("constant {expected}"), value))
                }
            }
            "boolean" => {
                if value.is_boolean() {
                    Ok(())
                } else {
                    Err(type_error(path, "boolean", value))
                }
            }
            "string" => self.validate_string(value, meta, path),
            "number" => self.validate_number(value, meta, path),
            "object" => self.validate_object(reference, node, value, path, depth),
            "array" => self.validate_array(node, value, meta, path, depth),
            "dict" => self.validate_dict(node, value, path, depth),
            "union" => {
                let list = node
                    .get("list")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("settings union {reference} has no list"))?;
                for alternative in list {
                    if self
                        .validate_node(alternative, value, path, depth + 1)
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                Err(format!(
                    "{path} does not match any allowed value: {}",
                    value
                ))
            }
            other => Err(format!(
                "{path} uses unsupported settings schema type \"{other}\""
            )),
        };

        // Captured `loose` unions are editable enums; invalid values must not fall back silently.
        result
    }

    fn validate_string(&self, value: &Value, meta: &Value, path: &str) -> Result<(), String> {
        let Some(text) = value.as_str() else {
            return Err(type_error(path, "string", value));
        };
        if let Some(pattern) = meta.get("pattern") {
            let source = pattern["source"].as_str().unwrap_or_default();
            let flags = pattern["flags"].as_str().unwrap_or_default();
            if source != LOCALE_PATTERN || flags != "u" {
                return Err(format!("{path} uses an unsupported string pattern"));
            }
            if !valid_locale(text) {
                return Err(format!("{path} must match {source}"));
            }
        }
        let length = text.encode_utf16().count();
        check_range(length as f64, meta, "string length", path)?;
        Ok(())
    }

    fn validate_number(&self, value: &Value, meta: &Value, path: &str) -> Result<(), String> {
        let Some(number) = value.as_f64() else {
            return Err(type_error(path, "number", value));
        };
        if !number.is_finite() {
            return Err(type_error(path, "finite number", value));
        }
        check_range(number, meta, "number", path)?;
        if let Some(step) = meta.get("step").and_then(Value::as_f64) {
            let minimum = meta.get("min").and_then(Value::as_f64).unwrap_or(0.0);
            if step <= 0.0 || !is_multiple_of(number, minimum, step) {
                return Err(format!("{path} must be a multiple of {step}"));
            }
        }
        Ok(())
    }

    fn validate_object(
        &self,
        reference: &Value,
        node: &Value,
        value: &Value,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        let Some(object) = value.as_object() else {
            return Err(type_error(path, "object", value));
        };
        let fields = node
            .get("dict")
            .and_then(Value::as_object)
            .ok_or_else(|| format!("settings object {reference} has no fields"))?;
        for key in object.keys() {
            if !fields.contains_key(key) {
                return Err(format!("unknown settings key \"{key}\" at {path}"));
            }
        }
        for (key, child) in fields {
            let child_path = format!("{path}.{key}");
            match object.get(key) {
                Some(value) => self.validate_node(child, value, &child_path, depth + 1)?,
                None => {
                    let child_node = self.node(child)?;
                    if child_node["meta"]["required"].as_bool().unwrap_or(false) {
                        return Err(format!("missing required settings value at {child_path}"));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_array(
        &self,
        node: &Value,
        value: &Value,
        meta: &Value,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        let Some(array) = value.as_array() else {
            return Err(type_error(path, "array", value));
        };
        check_range(array.len() as f64, meta, "array length", path)?;
        let inner = node
            .get("inner")
            .ok_or_else(|| "settings array has no item schema".to_string())?;
        for (index, value) in array.iter().enumerate() {
            self.validate_node(inner, value, &format!("{path}[{index}]"), depth + 1)?;
        }
        Ok(())
    }

    fn validate_dict(
        &self,
        node: &Value,
        value: &Value,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        let Some(object) = value.as_object() else {
            return Err(type_error(path, "object", value));
        };
        let inner = node
            .get("inner")
            .ok_or_else(|| "settings dictionary has no value schema".to_string())?;
        let key_schema = node
            .get("sKey")
            .ok_or_else(|| "settings dictionary has no key schema".to_string())?;
        for (key, value) in object {
            self.validate_node(
                key_schema,
                &Value::String(key.clone()),
                &format!("{path} key"),
                depth + 1,
            )?;
            self.validate_node(inner, value, &format!("{path}.{key}"), depth + 1)?;
        }
        Ok(())
    }

    fn validate_node_path(
        &self,
        reference: &Value,
        path: &[String],
        depth: usize,
    ) -> Result<(), String> {
        if depth > MAX_DEPTH {
            return Err(format!("settings path nesting exceeds {MAX_DEPTH}"));
        }
        let Some((head, rest)) = path.split_first() else {
            return Ok(());
        };
        let node = self.node(reference)?;
        match node["type"].as_str() {
            Some("any") => Ok(()),
            Some("object") => {
                let child = node
                    .get("dict")
                    .and_then(Value::as_object)
                    .and_then(|fields| fields.get(head))
                    .ok_or_else(|| {
                        format!("unknown settings key \"{head}\" at {}", path.join("."))
                    })?;
                self.validate_node_path(child, rest, depth + 1)
            }
            Some("dict") => {
                let key_schema = node
                    .get("sKey")
                    .ok_or_else(|| "settings dictionary has no key schema".to_string())?;
                self.validate_node(
                    key_schema,
                    &Value::String(head.clone()),
                    &format!("{} key", path.join(".")),
                    depth + 1,
                )?;
                let inner = node
                    .get("inner")
                    .ok_or_else(|| "settings dictionary has no value schema".to_string())?;
                self.validate_node_path(inner, rest, depth + 1)
            }
            Some("array") => {
                array_index(head).ok_or_else(|| {
                    format!(
                        "invalid settings array index \"{head}\" at {}",
                        path.join(".")
                    )
                })?;
                let inner = node
                    .get("inner")
                    .ok_or_else(|| "settings array has no item schema".to_string())?;
                self.validate_node_path(inner, rest, depth + 1)
            }
            Some("union") => {
                let list = node
                    .get("list")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "settings union has no list".to_string())?;
                for alternative in list {
                    if self
                        .validate_node_path(alternative, path, depth + 1)
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                Err(format!("unknown settings path \"{}\"", path.join(".")))
            }
            _ => Err(format!(
                "settings path \"{}\" crosses a scalar value",
                path.join(".")
            )),
        }
    }
}

fn reference_key(reference: &Value) -> String {
    reference
        .as_u64()
        .map_or_else(|| reference.to_string(), |value| value.to_string())
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        _ => left == right,
    }
}

fn type_error(path: &str, expected: &str, value: &Value) -> String {
    format!("expected {expected} at {path}, got {}", value.get_type())
}

fn check_range(value: f64, meta: &Value, description: &str, path: &str) -> Result<(), String> {
    if let Some(max) = meta.get("max").and_then(Value::as_f64) {
        if value > max {
            return Err(format!("{path} {description} must be <= {max}"));
        }
    }
    if let Some(min) = meta.get("min").and_then(Value::as_f64) {
        if value < min {
            return Err(format!("{path} {description} must be >= {min}"));
        }
    }
    Ok(())
}

fn is_multiple_of(value: f64, minimum: f64, step: f64) -> bool {
    if step.fract() == 0.0 && minimum.fract() == 0.0 {
        return (value - minimum) % step == 0.0;
    }
    let quotient = (value - minimum) / step;
    quotient.is_finite() && quotient.fract().abs() <= f64::EPSILON * quotient.abs().max(1.0) * 4.0
}

fn valid_locale(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(language) = parts.next() else {
        return false;
    };
    if !(2..=8).contains(&language.len())
        || !language.bytes().all(|byte| byte.is_ascii_alphabetic())
    {
        return false;
    }
    parts.all(|part| {
        (1..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

pub(super) fn array_index(segment: &str) -> Option<usize> {
    if segment.is_empty()
        || (segment.len() > 1 && segment.starts_with('0'))
        || !segment.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    segment.parse().ok()
}

trait ValueKind {
    fn get_type(&self) -> &'static str;
}

impl ValueKind for Value {
    fn get_type(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }
}
