//! Fold of `ask_user_question` events into the answerable question set.
//!
//! The Rust Host advertises one `ask_user_question` schema and it always
//! declares the timed `timeout` parameter, so every logged call of that tool is
//! answerable after a wait. `exit_plan_mode` and approvals ask through the same
//! waterfall but under their own tool names, so they never enter this fold and
//! can never be answered as a continued question.

use dsh_session_format::Event;
use serde_json::{json, Map, Value};

/// Tool whose calls this fold tracks.
pub const ASK_USER_QUESTION_TOOL: &str = "ask_user_question";

/// Read the question batch out of one logged arguments value.
/// @param arguments - The `tool/call` arguments text.
/// @returns The questions in service vocabulary, or `None` when unreadable.
#[must_use]
pub fn questions_of(arguments: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(arguments).ok()?;
    let questions = parsed.get("questions")?.as_array()?;
    if questions.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(questions.len());
    for question in questions {
        let id = question.get("id")?.as_str()?;
        let text = question.get("question")?.as_str()?;
        if id.is_empty() {
            return None;
        }
        let mut item = Map::new();
        item.insert("id".into(), json!(id));
        item.insert("question".into(), json!(text));
        if let Some(header) = question.get("header").and_then(Value::as_str) {
            item.insert("header".into(), json!(header));
        }
        if let Some(options) = question.get("options").and_then(Value::as_array) {
            let mut mapped = Vec::with_capacity(options.len());
            for option in options {
                let label = option.get("label")?.as_str()?;
                let mut entry = Map::new();
                entry.insert("label".into(), json!(label));
                if let Some(description) = option.get("description").and_then(Value::as_str) {
                    entry.insert("description".into(), json!(description));
                }
                mapped.push(Value::Object(entry));
            }
            item.insert("options".into(), Value::Array(mapped));
        }
        if let Some(multi) = question.get("multi_select").and_then(Value::as_bool) {
            item.insert("multiSelect".into(), json!(multi));
        }
        out.push(Value::Object(item));
    }
    Some(out)
}

/// Text of one recorded message, when it has one.
fn text_of(container: &Value) -> Option<&str> {
    container
        .get("content")
        .and_then(Value::as_array)
        .and_then(|blocks| blocks.iter().find_map(|block| block.get("text")?.as_str()))
}

/// Whether one recorded result is the pending marker a timed wait returns.
fn is_pending_result(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|parsed| parsed.get("pending").and_then(Value::as_bool))
        == Some(true)
}

/// Answers carried by one recorded text.
fn answers_of(text: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(text).ok()?;
    let answers = parsed.get("answers")?.as_array()?;
    let mut out = Vec::with_capacity(answers.len());
    for answer in answers {
        let id = answer.get("id")?.as_str()?;
        let selected = answer.get("selected")?.as_array()?;
        if selected.iter().any(|value| !value.is_string()) {
            return None;
        }
        let mut item = Map::new();
        item.insert("id".into(), json!(id));
        item.insert("selected".into(), Value::Array(selected.clone()));
        if let Some(custom) = answer.get("custom").and_then(Value::as_str) {
            item.insert("custom".into(), json!(custom));
        }
        out.push(Value::Object(item));
    }
    Some(out)
}

/// The empty view shared by every Session with no answerable question.
#[must_use]
pub fn empty() -> Value {
    json!({"active": [], "settled": []})
}

/// Fold a whole Session log into its question state.
/// @param events - Session events in append order.
/// @returns `active` calls in ask order and `settled` ones in settlement order.
#[must_use]
pub fn fold(events: &[Event]) -> Value {
    let mut active: Vec<Value> = Vec::new();
    let mut settled: Vec<Value> = Vec::new();
    for event in events {
        match event.kind.as_str() {
            "tool/call" => {
                if event.data.get("name").and_then(Value::as_str) != Some(ASK_USER_QUESTION_TOOL) {
                    continue;
                }
                let Some(call_id) = event.data.get("callId").and_then(Value::as_str) else {
                    continue;
                };
                let arguments = event.data.get("arguments").and_then(Value::as_str);
                let Some(questions) = arguments.and_then(questions_of) else {
                    continue;
                };
                active.retain(|item| item["callId"] != json!(call_id));
                active.push(json!({
                    "callId": call_id,
                    "questions": questions,
                    "state": "open",
                }));
            }
            "tool/result" => {
                let message = event.data.get("message");
                let Some(call_id) = message
                    .and_then(|message| message.get("toolCallId"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if !active.iter().any(|item| item["callId"] == json!(call_id)) {
                    continue;
                }
                let text = message.and_then(text_of).unwrap_or_default();
                let is_error = message
                    .and_then(|message| message.get("isError"))
                    .and_then(Value::as_bool)
                    == Some(true);
                let code = event.data.pointer("/error/code").and_then(Value::as_str);
                if is_pending_result(text) || code == Some("TOOL_OUTCOME_UNKNOWN") {
                    for item in &mut active {
                        if item["callId"] == json!(call_id) {
                            item["state"] = json!("continued");
                        }
                    }
                    continue;
                }
                let answers = if is_error { None } else { answers_of(text) };
                active.retain(|item| item["callId"] != json!(call_id));
                if let Some(answers) = answers {
                    settled.push(json!({ "callId": call_id, "answers": answers }));
                }
            }
            "user/message" => {
                let source = event.data.get("source");
                if source
                    .and_then(|source| source.get("kind"))
                    .and_then(Value::as_str)
                    != Some("user-question-reply")
                {
                    continue;
                }
                let Some(call_id) = source
                    .and_then(|source| source.get("callId"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if !active.iter().any(|item| item["callId"] == json!(call_id)) {
                    continue;
                }
                let answers = text_of(&event.data)
                    .and_then(answers_of)
                    .unwrap_or_default();
                active.retain(|item| item["callId"] != json!(call_id));
                settled.push(json!({ "callId": call_id, "answers": answers }));
            }
            _ => {}
        }
    }
    json!({ "active": active, "settled": settled })
}

/// Whether a folded view still lets this call be answered.
/// @param view - Folded `active`/`settled` value.
/// @param call_id - Call the Client answers.
/// @returns The questions of that call when it is `continued`.
#[must_use]
pub fn answerable(view: &Value, call_id: &str) -> Option<Vec<Value>> {
    view.get("active")?
        .as_array()?
        .iter()
        .find(|item| item["callId"] == json!(call_id) && item["state"] == json!("continued"))
        .and_then(|item| item["questions"].as_array().cloned())
}

/// The steered message that answers one continued question.
/// @param call_id - Call the reply answers.
/// @param questions - Questions the call asked.
/// @param answers - One answer item per question.
/// @returns The user message content and source the Agent records.
#[must_use]
pub fn reply_message(call_id: &str, questions: &[Value], answers: &[Value]) -> (String, Value) {
    let text = json!({
        "kind": "answer_to_pending_question",
        "tool": ASK_USER_QUESTION_TOOL,
        "callId": call_id,
        "questions": questions,
        "answers": answers,
    });
    (
        text.to_string(),
        json!({"kind": "user-question-reply", "callId": call_id, "outcome": "answered"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: &str, data: Value) -> Event {
        Event {
            kind: kind.into(),
            data,
            extra: Default::default(),
        }
    }

    fn call(id: &str) -> Event {
        event(
            "tool/call",
            json!({
                "callId": id,
                "name": "ask_user_question",
                "arguments": json!({"questions":[{"id":"q1","question":"Which mode?","header":"Mode",
                    "options":[{"label":"Fast","description":"Less detail"}],"multi_select":false}]}).to_string(),
            }),
        )
    }

    fn result(id: &str, body: Value) -> Event {
        event(
            "tool/result",
            json!({"message":{"toolCallId":id,"isError":false,
                "content":[{"type":"text","text":body.to_string()}]}}),
        )
    }

    #[test]
    fn a_question_opens_until_its_result_settles_it() {
        let events = vec![
            call("call-1"),
            result(
                "call-1",
                json!({"answers":[{"id":"q1","selected":["Fast"]}]}),
            ),
        ];
        let view = fold(&events);
        assert_eq!(view["active"], json!([]));
        assert_eq!(view["settled"][0]["callId"], json!("call-1"));
        assert_eq!(
            view["settled"][0]["answers"][0]["selected"][0],
            json!("Fast")
        );
        assert!(
            answerable(&view, "call-1").is_none(),
            "settled calls are not answerable"
        );
    }

    #[test]
    fn the_pending_marker_continues_the_call() {
        let view = fold(&[
            call("call-2"),
            result(
                "call-2",
                json!({"pending":true,"callId":"call-2","message":"waiting"}),
            ),
        ]);
        assert_eq!(view["active"][0]["state"], json!("continued"));
        let questions = answerable(&view, "call-2").expect("continued");
        assert_eq!(questions[0]["multiSelect"], json!(false));
        assert_eq!(view["settled"], json!([]));
    }

    #[test]
    fn a_plain_failure_closes_the_call_without_settling_it() {
        let mut failure = result("call-3", json!({"message":"boom"}));
        failure.data["message"]["isError"] = json!(true);
        let view = fold(&[call("call-3"), failure]);
        assert_eq!(view["active"], json!([]));
        assert_eq!(view["settled"], json!([]));
    }

    #[test]
    fn a_late_reply_settles_the_continued_call() {
        let (text, source) = reply_message(
            "call-4",
            &[json!({"id":"q1","question":"Which mode?"})],
            &[json!({"id":"q1","selected":["Fast"]})],
        );
        let events = vec![
            call("call-4"),
            result("call-4", json!({"pending":true,"callId":"call-4"})),
            event(
                "user/message",
                json!({"source": source, "content":[{"type":"text","text":text}]}),
            ),
        ];
        let view = fold(&events);
        assert_eq!(view["active"], json!([]));
        assert_eq!(view["settled"][0]["answers"][0]["id"], json!("q1"));
    }

    #[test]
    fn other_tools_asking_through_the_waterfall_stay_out() {
        let view = fold(&[event(
            "tool/call",
            json!({"callId":"call-plan","name":"exit_plan_mode","arguments":"{\"plan\":\"# Plan\"}"}),
        )]);
        assert_eq!(view, empty());
    }

    #[test]
    fn unreadable_arguments_never_open_a_card() {
        let view = fold(&[event(
            "tool/call",
            json!({"callId":"call-x","name":"ask_user_question","arguments":"{not json"}),
        )]);
        assert_eq!(view, empty());
    }
}
