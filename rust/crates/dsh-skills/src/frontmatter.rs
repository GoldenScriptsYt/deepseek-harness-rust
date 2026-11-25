use crate::types::{SkillInvocationPolicy, SkillWarningCode};
use serde_yaml::{Mapping, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrontmatterErrorCode {
    MissingFrontmatter,
    InvalidYaml,
    InvalidDefinition,
    InvalidInvocation,
}

impl FrontmatterErrorCode {
    pub(crate) fn warning_code(self) -> SkillWarningCode {
        match self {
            Self::MissingFrontmatter => SkillWarningCode::MissingFrontmatter,
            Self::InvalidYaml => SkillWarningCode::InvalidYaml,
            Self::InvalidDefinition => SkillWarningCode::InvalidDefinition,
            Self::InvalidInvocation => SkillWarningCode::InvalidInvocation,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FrontmatterError {
    pub(crate) code: FrontmatterErrorCode,
    pub(crate) message: String,
}

impl FrontmatterError {
    fn new(code: FrontmatterErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ParsedSkill {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) when_to_use: Option<String>,
    pub(crate) invocation: SkillInvocationPolicy,
    pub(crate) metadata: Option<Mapping>,
    pub(crate) content: String,
}

pub(crate) fn parse(raw: &str) -> Result<ParsedSkill, FrontmatterError> {
    let first_line_end = raw.find('\n').ok_or_else(|| {
        FrontmatterError::new(
            FrontmatterErrorCode::MissingFrontmatter,
            "missing opening line",
        )
    })?;
    let first_line = raw[..first_line_end]
        .strip_suffix('\r')
        .unwrap_or(&raw[..first_line_end]);
    if first_line != "---" {
        return Err(FrontmatterError::new(
            FrontmatterErrorCode::MissingFrontmatter,
            "first line must be exactly `---`",
        ));
    }

    let yaml_start = first_line_end + 1;
    let (yaml_end, body_start) = closing_delimiter(raw, yaml_start).ok_or_else(|| {
        FrontmatterError::new(
            FrontmatterErrorCode::MissingFrontmatter,
            "missing closing `---` line",
        )
    })?;
    let parsed: Value = serde_yaml::from_str(&raw[yaml_start..yaml_end]).map_err(|error| {
        FrontmatterError::new(FrontmatterErrorCode::InvalidYaml, error.to_string())
    })?;
    let data = parsed.as_mapping().ok_or_else(|| {
        FrontmatterError::new(
            FrontmatterErrorCode::InvalidDefinition,
            "frontmatter must be a YAML mapping",
        )
    })?;

    let name = required_string(data, "name")?;
    if !is_skill_name(name) {
        return Err(FrontmatterError::new(
            FrontmatterErrorCode::InvalidDefinition,
            format!("invalid kebab-case skill name {name:?}"),
        ));
    }
    let description = required_string(data, "description")?.to_owned();
    let when_to_use = optional_string(data, "whenToUse").map(str::to_owned);
    let invocation = parse_invocation(data)?;
    let metadata = data
        .get(Value::String("metadata".to_owned()))
        .and_then(Value::as_mapping)
        .cloned();

    Ok(ParsedSkill {
        name: name.to_owned(),
        description,
        when_to_use,
        invocation,
        metadata,
        content: raw[body_start..]
            .trim_matches(is_javascript_whitespace)
            .to_owned(),
    })
}

fn closing_delimiter(raw: &str, mut line_start: usize) -> Option<(usize, usize)> {
    while line_start <= raw.len() {
        let remainder = &raw[line_start..];
        let newline = remainder.find('\n');
        let line_end = line_start + newline.unwrap_or(remainder.len());
        let line = raw[line_start..line_end]
            .strip_suffix('\r')
            .unwrap_or(&raw[line_start..line_end]);
        if line == "---" {
            return Some((
                line_start,
                newline.map_or(raw.len(), |offset| line_start + offset + 1),
            ));
        }
        let newline = newline?;
        line_start += newline + 1;
    }
    None
}

fn mapping_value<'a>(data: &'a Mapping, key: &str) -> Option<&'a Value> {
    data.get(Value::String(key.to_owned()))
}

fn required_string<'a>(data: &'a Mapping, key: &str) -> Result<&'a str, FrontmatterError> {
    mapping_value(data, key)
        .and_then(|value| match value {
            Value::String(value) => Some(value.as_str()),
            _ => None,
        })
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            FrontmatterError::new(
                FrontmatterErrorCode::InvalidDefinition,
                format!("frontmatter requires a non-empty string {key:?}"),
            )
        })
}

fn optional_string<'a>(data: &'a Mapping, key: &str) -> Option<&'a str> {
    mapping_value(data, key)
        .and_then(|value| match value {
            Value::String(value) => Some(value.as_str()),
            _ => None,
        })
        .filter(|value| !value.is_empty())
}

fn parse_invocation(data: &Mapping) -> Result<SkillInvocationPolicy, FrontmatterError> {
    reject_legacy_key(data, "disableModelInvocation", "disable-model-invocation")?;
    reject_legacy_key(data, "modelInvocable", "disable-model-invocation")?;
    reject_legacy_key(data, "userInvocable", "user-invocable")?;

    let disable_model_invocation = frontmatter_boolean(data, "disable-model-invocation")?;
    let user_invocable = frontmatter_boolean(data, "user-invocable")?;
    Ok(SkillInvocationPolicy {
        model_invocable: disable_model_invocation != Some(true),
        user_invocable: user_invocable != Some(false),
    })
}

fn reject_legacy_key(
    data: &Mapping,
    legacy: &str,
    canonical: &str,
) -> Result<(), FrontmatterError> {
    if mapping_value(data, legacy).is_some() {
        return Err(FrontmatterError::new(
            FrontmatterErrorCode::InvalidInvocation,
            format!("frontmatter field {legacy:?} is unsupported; use {canonical:?}"),
        ));
    }
    Ok(())
}

fn frontmatter_boolean(data: &Mapping, key: &str) -> Result<Option<bool>, FrontmatterError> {
    let Some(value) = mapping_value(data, key) else {
        return Ok(None);
    };
    let parsed = match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) => {
            if value.as_f64() == Some(1.0) {
                Some(true)
            } else if value.as_f64() == Some(0.0) {
                Some(false)
            } else {
                None
            }
        }
        Value::String(value) => match value.to_ascii_lowercase().as_str() {
            "true" | "yes" | "on" | "1" => Some(true),
            "false" | "no" | "off" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    };
    match parsed {
        Some(value) => Ok(Some(value)),
        None => Err(FrontmatterError::new(
            FrontmatterErrorCode::InvalidInvocation,
            format!("frontmatter field {key:?} must be a boolean"),
        )),
    }
}

pub(crate) fn is_skill_name(name: &str) -> bool {
    !name.is_empty()
        && name.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
}

fn is_javascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

#[cfg(test)]
mod tests {
    use super::{is_javascript_whitespace, is_skill_name, parse, FrontmatterErrorCode};

    fn error_code(raw: &str) -> FrontmatterErrorCode {
        parse(raw).unwrap_err().code
    }

    #[test]
    fn validates_kebab_case_names_with_ascii_lowercase_and_digits_only() {
        for valid in ["a", "0", "skill-0", "alpha-beta-123", "a1-b2-c3"] {
            assert!(is_skill_name(valid), "{valid:?}");
        }
        for invalid in [
            "",
            "-alpha",
            "alpha-",
            "alpha--beta",
            "Alpha",
            "alpha_beta",
            "alpha.beta",
            "alpha beta",
            "café",
            "日本語",
            "a/b",
        ] {
            assert!(!is_skill_name(invalid), "{invalid:?}");
        }
    }

    #[test]
    fn frontmatter_delimiters_must_be_exact_lines() {
        for raw in [
            "---",
            "\n---\nname: a\ndescription: b\n---\n",
            "--- \nname: a\ndescription: b\n---\n",
            "---\nname: a\ndescription: b\n",
            "---\nname: a\ndescription: b\n----\n",
            "---\nname: a\ndescription: b\n ---\n",
        ] {
            assert_eq!(
                error_code(raw),
                FrontmatterErrorCode::MissingFrontmatter,
                "{raw:?}"
            );
        }

        let parsed =
            parse("---\r\nname: a\r\ndescription: b\r\nmetadata:\r\n  marker: ----\r\n---")
                .unwrap();
        assert_eq!(parsed.name, "a");
        assert_eq!(parsed.description, "b");
        assert_eq!(parsed.content, "");
        assert!(parsed.metadata.is_some());
    }

    #[test]
    fn parses_block_scalars_without_treating_indented_markers_as_delimiters() {
        let parsed = parse(
            "---\nname: block\ndescription: |-\n  first line\n  ----\n  last line\nwhenToUse: >-\n  route with\n  folded guidance\n---\nBody.\n",
        )
        .unwrap();
        assert_eq!(parsed.name, "block");
        assert_eq!(parsed.description, "first line\n----\nlast line");
        assert_eq!(
            parsed.when_to_use.as_deref(),
            Some("route with folded guidance")
        );
        assert_eq!(parsed.content, "Body.");
    }

    #[test]
    fn rejects_malformed_yaml_and_non_mapping_documents() {
        assert_eq!(
            error_code("---\nname: [unterminated\n---\n"),
            FrontmatterErrorCode::InvalidYaml
        );
        for raw in [
            "---\n- name: a\n- description: b\n---\n",
            "---\nscalar\n---\n",
            "---\n---\n",
        ] {
            assert_eq!(
                error_code(raw),
                FrontmatterErrorCode::InvalidDefinition,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn required_strings_reject_missing_empty_and_wrongly_typed_values() {
        for raw in [
            "---\ndescription: b\n---\n",
            "---\nname: a\n---\n",
            "---\nname: ''\ndescription: b\n---\n",
            "---\nname: a\ndescription: ''\n---\n",
            "---\nname: 1\ndescription: b\n---\n",
            "---\nname: a\ndescription: true\n---\n",
            "---\nname: a\ndescription: [b]\n---\n",
        ] {
            assert_eq!(
                error_code(raw),
                FrontmatterErrorCode::InvalidDefinition,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn optional_fields_ignore_empty_or_wrongly_typed_values() {
        let parsed = parse(
            "---\nname: optional\ndescription: d\nwhenToUse: 12\nmetadata: [not, a, mapping]\n---\n",
        )
        .unwrap();
        assert_eq!(parsed.when_to_use, None);
        assert_eq!(parsed.metadata, None);

        let parsed = parse(
            "---\nname: optional\ndescription: d\nwhenToUse:\nmetadata: {owner: tests}\n---\n",
        )
        .unwrap();
        assert_eq!(parsed.when_to_use, None);
        assert!(parsed.metadata.is_some());
    }

    #[test]
    fn invocation_booleans_match_typescript_string_and_numeric_spellings() {
        for value in ["true", "TRUE", "yes", "YeS", "on", "ON", "1"] {
            let parsed = parse(&format!(
                "---\nname: boolean\ndescription: d\ndisable-model-invocation: \"{value}\"\n---\n"
            ))
            .unwrap();
            assert!(!parsed.invocation.model_invocable, "{value:?}");
            assert!(parsed.invocation.user_invocable, "{value:?}");
        }
        for value in ["false", "FALSE", "no", "No", "off", "OFF", "0"] {
            let parsed = parse(&format!(
                "---\nname: boolean\ndescription: d\nuser-invocable: \"{value}\"\n---\n"
            ))
            .unwrap();
            assert!(parsed.invocation.model_invocable, "{value:?}");
            assert!(!parsed.invocation.user_invocable, "{value:?}");
        }
        for (value, model, user) in [
            ("1", false, true),
            ("1.0", false, true),
            ("0", true, false),
            ("-0", true, false),
        ] {
            let parsed = parse(&format!(
                "---\nname: boolean\ndescription: d\ndisable-model-invocation: {value}\nuser-invocable: {value}\n---\n"
            ))
            .unwrap();
            assert_eq!(parsed.invocation.model_invocable, model, "{value:?}");
            assert_eq!(parsed.invocation.user_invocable, user, "{value:?}");
        }
        let parsed = parse(
            "---\nname: boolean\ndescription: d\ndisable-model-invocation: true\nuser-invocable: false\n---\n",
        )
        .unwrap();
        assert!(!parsed.invocation.model_invocable);
        assert!(!parsed.invocation.user_invocable);
    }

    #[test]
    fn invocation_rejects_invalid_values_and_legacy_field_names() {
        for value in [
            "2",
            "-1",
            "0.5",
            ".nan",
            "maybe",
            "\"\"",
            "\" true \"",
            "[true]",
            "{value: true}",
            "null",
        ] {
            let raw = format!(
                "---\nname: boolean\ndescription: d\ndisable-model-invocation: {value}\n---\n"
            );
            assert_eq!(
                error_code(&raw),
                FrontmatterErrorCode::InvalidInvocation,
                "{value:?}"
            );
        }
        for key in ["disableModelInvocation", "modelInvocable", "userInvocable"] {
            let raw = format!("---\nname: boolean\ndescription: d\n{key}: false\n---\n");
            assert_eq!(
                error_code(&raw),
                FrontmatterErrorCode::InvalidInvocation,
                "{key:?}"
            );
        }
    }

    #[test]
    fn body_trimming_uses_javascript_whitespace_not_rust_whitespace() {
        let javascript_space = "\u{0009}\u{000A}\u{000B}\u{000C}\u{000D}\u{0020}\u{00A0}\u{1680}\u{2000}\u{2007}\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}\u{FEFF}";
        for character in javascript_space.chars() {
            assert!(
                is_javascript_whitespace(character),
                "U+{:04X}",
                character as u32
            );
        }
        for character in ['\u{0085}', '\u{180E}', '\u{200B}', '\u{2060}', '\u{1F600}'] {
            assert!(
                !is_javascript_whitespace(character),
                "U+{:04X}",
                character as u32
            );
        }
        let parsed = parse(&format!(
            "---\nname: trim\ndescription: d\n---\n{javascript_space}Body.{javascript_space}"
        ))
        .unwrap();
        assert_eq!(parsed.content, "Body.");
        let parsed = parse("---\nname: trim\ndescription: d\n---\n\u{0085}Body.\u{0085}").unwrap();
        assert_eq!(parsed.content, "\u{0085}Body.\u{0085}");
    }
}
