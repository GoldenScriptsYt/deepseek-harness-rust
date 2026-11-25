//! Read-only Cordis API inspection for the Rust development preset.

use crate::ToolRegistry;
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput};
use dsh_llm::ToolDef;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::OnceLock;

const API_CATALOG: &str = include_str!("../assets/cordis-api-catalog.json");
static CATALOG: OnceLock<Value> = OnceLock::new();

/// Inspect the generated Cordis Host API catalog and this Agent's Rust tools.
pub struct CordisInspect {
    tools: ToolRegistry,
}

impl CordisInspect {
    /// Keep a snapshot of the tools available beside the inspector.
    /// @param tools - Agent tool registry used for the Tool provider query.
    /// @returns A read-only Cordis inspector.
    pub fn new(tools: ToolRegistry) -> Self {
        Self { tools }
    }
}

impl Tool for CordisInspect {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "cordis_inspect".into(),
            description: "Inspect the Cordis Host Service and Event catalogs before editing harness plugins. Omit query to list providers; then query an exact Service, Event, Host runtime version, or this Agent's tool schemas. This tool is read-only.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "object",
                        "properties": {
                            "platform": {"type": "string", "enum": ["host", "client"]},
                            "provider": {"type": "string"},
                            "method": {"type": "string"},
                            "input": {"type": "object"}
                        },
                        "required": ["platform", "provider", "method"],
                        "additionalProperties": false
                    }
                },
                "additionalProperties": false
            }),
        }
    }

    fn call<'a>(&'a self, args: Value, _cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            match inspect(&self.tools, args.get("query")) {
                Ok(value) => ToolOutput::ok(
                    serde_json::to_string_pretty(&value)
                        .expect("Cordis inspection result serializes"),
                ),
                Err(error) => ToolOutput::err(error),
            }
        })
    }
}

fn inspect(tools: &ToolRegistry, query: Option<&Value>) -> Result<Value, String> {
    let Some(query) = query else {
        return Ok(json!({"providers": [
            {"id": "Service", "description": "Generated Host Service directory and exact method contracts."},
            {"id": "Event", "description": "Generated Host Event directory and exact listener contracts."},
            {"id": "Runtime", "description": "The running Rust Host version."},
            {"id": "Tool", "description": "Tool schemas available to this Agent."}
        ]}));
    };
    let platform = query
        .get("platform")
        .and_then(Value::as_str)
        .ok_or_else(|| "query.platform must be host or client".to_owned())?;
    if platform != "host" {
        return Err("Client Cordis API inspection requires a connected Client runtime".into());
    }
    let provider = query
        .get("provider")
        .and_then(Value::as_str)
        .ok_or_else(|| "query.provider must be a provider id".to_owned())?;
    let method = query
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "query.method must be a method name".to_owned())?;
    let input = query.get("input").filter(|value| value.is_object());
    if query.get("input").is_some() && input.is_none() {
        return Err("query.input must be an object".into());
    }
    match (provider, method) {
        ("Service", "listService") => query_service(input.and_then(|input| input.get("service"))),
        ("Event", "listEvents") => query_event(input.and_then(|input| input.get("event"))),
        ("Runtime", "getVersion") => Ok(json!({"version": env!("CARGO_PKG_VERSION")})),
        ("Tool", "listTools") => Ok(json!({
            "tools": tools.defs().into_iter().map(|tool| json!({
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema
            })).collect::<Vec<_>>()
        })),
        ("Service", _) => Err(format!("unknown Service inspect method {method:?}")),
        ("Event", _) => Err(format!("unknown Event inspect method {method:?}")),
        ("Runtime", _) => Err(format!("unknown Runtime inspect method {method:?}")),
        ("Tool", _) => Err(format!("unknown Tool inspect method {method:?}")),
        _ => Err(format!("unknown Host Cordis provider {provider:?}")),
    }
}

fn query_service(key: Option<&Value>) -> Result<Value, String> {
    let catalog = catalog();
    let services = catalog["services"]
        .as_array()
        .ok_or_else(|| "generated Service catalog is invalid".to_owned())?;
    let Some(key) = key else {
        return Ok(json!({
            "mode": "catalog",
            "services": services.iter().map(|service| json!({
                "key": service["key"],
                "description": service["summary"],
                "methods": service["methods"].as_array().into_iter().flatten().map(|method| json!({"signature": method["signature"]})).collect::<Vec<_>>()
            })).collect::<Vec<_>>()
        }));
    };
    let key = key
        .as_str()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| "input.service must be a non-empty Service key".to_owned())?;
    let service = services
        .iter()
        .find(|service| service["key"].as_str() == Some(key))
        .ok_or_else(|| format!("no catalogued Service named {key:?}"))?;
    let methods = service["methods"]
        .as_array()
        .ok_or_else(|| format!("Service {key:?} has invalid method metadata"))?;
    let signatures = methods
        .iter()
        .filter_map(|method| method["signature"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    Ok(json!({
        "mode": "service",
        "service": {
            "key": key,
            "description": service["description"],
            "access": {
                "optional": {
                    "expression": format!("ctx.get({})", serde_json::to_string(key).expect("Service key serializes")),
                    "requiresUndefinedCheck": true
                },
                "hardDependency": {
                    "inject": [key],
                    "expression": if is_js_identifier(key) { format!("ctx.{key}") } else { format!("ctx[{}]", serde_json::to_string(key).expect("Service key serializes")) }
                }
            },
            "methods": methods
        },
        "referencedTypes": referenced_types(catalog, &signatures)
    }))
}

fn query_event(name: Option<&Value>) -> Result<Value, String> {
    let catalog = catalog();
    let events = catalog["events"]
        .as_array()
        .ok_or_else(|| "generated Event catalog is invalid".to_owned())?;
    let events = events
        .iter()
        .filter(|event| {
            !event["name"]
                .as_str()
                .unwrap_or_default()
                .starts_with("cordis/")
        })
        .collect::<Vec<_>>();
    let Some(name) = name else {
        return Ok(json!({
            "mode": "catalog",
            "events": events.iter().map(|event| json!({
                "name": event["name"],
                "description": event["summary"],
                "mode": event["mode"],
                "signature": event["signature"]
            })).collect::<Vec<_>>()
        }));
    };
    let name = name
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "input.event must be a non-empty Event name".to_owned())?;
    let event = events
        .into_iter()
        .find(|event| event["name"].as_str() == Some(name))
        .ok_or_else(|| format!("no catalogued Event named {name:?}"))?;
    let signature = event["signature"]
        .as_str()
        .ok_or_else(|| format!("Event {name:?} has invalid signature metadata"))?;
    Ok(json!({
        "mode": "event",
        "event": {
            "name": name,
            "description": event["description"],
            "mode": event["mode"],
            "signature": signature,
            "parameters": event["parameters"]
        },
        "referencedTypes": referenced_types(catalog, &[signature.to_owned()])
    }))
}

fn referenced_types(catalog: &Value, seeds: &[String]) -> Vec<Value> {
    let Some(types) = catalog["types"].as_array() else {
        return Vec::new();
    };
    let mut included = HashSet::new();
    let mut frontier = seeds.to_vec();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for entry in types {
            let Some(name) = entry["name"].as_str() else {
                continue;
            };
            if included.contains(name) {
                continue;
            }
            if !frontier.iter().any(|text| contains_identifier(text, name)) {
                continue;
            }
            included.insert(name.to_owned());
            if let Some(declaration) = entry["declaration"].as_str() {
                next.push(declaration.to_owned());
            }
        }
        frontier = next;
    }
    types
        .iter()
        .filter(|entry| {
            entry["name"]
                .as_str()
                .is_some_and(|name| included.contains(name))
        })
        .cloned()
        .collect()
}

fn contains_identifier(text: &str, identifier: &str) -> bool {
    text.split(|character: char| {
        !(character.is_alphanumeric() || character == '_' || character == '$')
    })
    .any(|token| token == identifier)
}

fn is_js_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first == '$' || first.is_ascii_alphabetic())
        && chars.all(|character| {
            character == '_' || character == '$' || character.is_ascii_alphanumeric()
        })
}

fn catalog() -> &'static Value {
    CATALOG.get_or_init(|| {
        serde_json::from_str(API_CATALOG).expect("generated Cordis API catalog is valid JSON")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cordis_inspect_lists_read_only_host_providers() {
        let result = inspect(&ToolRegistry::default(), None).unwrap();
        let providers = result["providers"].as_array().unwrap();
        assert_eq!(providers.len(), 4);
        assert!(providers.iter().any(|provider| provider["id"] == "Service"));
        assert!(providers.iter().any(|provider| provider["id"] == "Event"));
    }

    #[test]
    fn cordis_inspect_returns_exact_service_and_referenced_types() {
        let query = json!({
            "platform": "host",
            "provider": "Service",
            "method": "listService",
            "input": {"service": "tools"}
        });
        let result = inspect(&ToolRegistry::default(), Some(&query)).unwrap();
        assert_eq!(result["mode"], "service");
        assert_eq!(result["service"]["key"], "tools");
        assert!(!result["service"]["methods"].as_array().unwrap().is_empty());
    }

    #[test]
    fn cordis_inspect_returns_event_directory_and_rejects_client_runtime_queries() {
        let directory = json!({"platform":"host","provider":"Event","method":"listEvents"});
        let result = inspect(&ToolRegistry::default(), Some(&directory)).unwrap();
        assert!(result["events"].as_array().unwrap().len() > 10);

        let client = json!({"platform":"client","provider":"Tool","method":"listTools"});
        assert!(inspect(&ToolRegistry::default(), Some(&client))
            .unwrap_err()
            .contains("connected Client runtime"));
    }
}
