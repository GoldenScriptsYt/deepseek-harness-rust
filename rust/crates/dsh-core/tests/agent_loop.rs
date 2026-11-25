use dsh_core::*;
use dsh_llm::*;
use dsh_session_format::{Header, HeaderTag, SESSION_FORMAT_VERSION};
use dsh_session_persistence::Store;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, Barrier, Notify};

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

type Script = Vec<Result<StreamChunk, LlmError>>;

/// Replays one scripted response per request.
struct Scripted {
    script: Mutex<Vec<Script>>,
    seen: Mutex<Vec<usize>>,
    seen_messages: Mutex<Vec<Vec<Message>>>,
    hang: bool,
}

impl Scripted {
    fn new(script: Vec<Script>) -> Self {
        Scripted {
            script: Mutex::new(script),
            seen: Mutex::new(vec![]),
            seen_messages: Mutex::new(vec![]),
            hang: false,
        }
    }
}

impl Model for Scripted {
    fn provider(&self) -> String {
        "fake".into()
    }
    fn model(&self) -> String {
        "m1".into()
    }
    fn stream<'a>(
        &'a self,
        history: &'a [Message],
        _t: &'a [ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) -> BoxFuture<'a, ()> {
        self.seen.lock().unwrap().push(history.len());
        self.seen_messages.lock().unwrap().push(history.to_vec());
        let items = {
            let mut s = self.script.lock().unwrap();
            if s.is_empty() {
                vec![]
            } else {
                s.remove(0)
            }
        };
        let hang = self.hang;
        Box::pin(async move {
            for i in items {
                let _ = tx.send(i).await;
            }
            if hang {
                std::future::pending::<()>().await;
            }
        })
    }
}

struct DeferredModel {
    scripts: Mutex<VecDeque<Script>>,
    seen_messages: Mutex<Vec<Vec<Message>>>,
    started: mpsc::UnboundedSender<usize>,
    gates: Mutex<VecDeque<oneshot::Receiver<()>>>,
}

impl DeferredModel {
    fn new(
        scripts: Vec<Script>,
        gated_calls: usize,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<usize>,
        Vec<oneshot::Sender<()>>,
    ) {
        let (started, started_rx) = mpsc::unbounded_channel();
        let mut gates = VecDeque::new();
        let mut releases = Vec::new();
        for _ in 0..gated_calls {
            let (release, gate) = oneshot::channel();
            gates.push_back(gate);
            releases.push(release);
        }
        (
            Arc::new(Self {
                scripts: Mutex::new(scripts.into()),
                seen_messages: Mutex::new(Vec::new()),
                started,
                gates: Mutex::new(gates),
            }),
            started_rx,
            releases,
        )
    }
}

impl Model for DeferredModel {
    fn provider(&self) -> String {
        "fake".into()
    }

    fn model(&self) -> String {
        "deferred".into()
    }

    fn stream<'a>(
        &'a self,
        history: &'a [Message],
        _tools: &'a [ToolDef],
        tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
    ) -> BoxFuture<'a, ()> {
        let index = self.seen_messages.lock().unwrap().len();
        self.seen_messages.lock().unwrap().push(history.to_vec());
        let _ = self.started.send(index);
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let gate = self.gates.lock().unwrap().pop_front();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            for item in script {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        })
    }
}

fn request_user_texts_from(model: &DeferredModel) -> Vec<Vec<String>> {
    model
        .seen_messages
        .lock()
        .unwrap()
        .iter()
        .map(|messages| {
            messages
                .iter()
                .filter(|message| message.role == Role::User)
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    Content::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect()
        })
        .collect()
}

async fn submit_concurrently(
    admission: dsh_core::agent::AgentAdmission,
    requests: Vec<PromptRequest>,
) -> Vec<Result<Option<String>, CoreError>> {
    let barrier = Arc::new(Barrier::new(requests.len()));
    let mut tasks = Vec::new();
    for request in requests {
        let admission = admission.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            admission
                .submit_prompt(request, InboxTarget::NextTurn)
                .await
        }));
    }
    let mut responses = Vec::with_capacity(tasks.len());
    for task in tasks {
        responses.push(
            tokio::time::timeout(TEST_TIMEOUT, task)
                .await
                .expect("concurrent admission does not deadlock")
                .unwrap(),
        );
    }
    responses
}

fn text_reply(t: &str) -> Script {
    vec![
        Ok(StreamChunk::BlockStart {
            index: 0,
            kind: BlockKind::Text,
        }),
        Ok(StreamChunk::TextDelta {
            index: 0,
            text: t.into(),
        }),
        Ok(StreamChunk::BlockEnd { index: 0 }),
        Ok(StreamChunk::Finish {
            reason: FinishReason::Stop,
            replay: vec![None],
        }),
    ]
}

fn tool_reply(id: &str, name: &str, args: &str) -> Script {
    vec![
        Ok(StreamChunk::BlockStart {
            index: 0,
            kind: BlockKind::ToolCall,
        }),
        Ok(StreamChunk::ToolCallDelta {
            index: 0,
            id: Some(id.into()),
            name: Some(name.into()),
            arguments_delta: String::new(),
        }),
        Ok(StreamChunk::ToolCallDelta {
            index: 0,
            id: None,
            name: None,
            arguments_delta: args.into(),
        }),
        Ok(StreamChunk::BlockEnd { index: 0 }),
        Ok(StreamChunk::Finish {
            reason: FinishReason::ToolCalls,
            replay: vec![None],
        }),
    ]
}

struct Echo;
impl Tool for Echo {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "echo".into(),
            description: "echo".into(),
            input_schema: json!({"type":"object"}),
        }
    }
    fn call<'a>(&'a self, args: Value, _cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move { ToolOutput::ok(format!("echo:{}", args["v"])) })
    }
}

struct Recorder;
impl Tool for Recorder {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "record".into(),
            description: "record".into(),
            input_schema: json!({"type":"object"}),
        }
    }
    fn call<'a>(&'a self, args: Value, cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            cx.events.append("note/write", json!({"note": args["v"]}));
            if args["fail"] == json!(true) {
                return ToolOutput::err("Error: recording failed");
            }
            ToolOutput::ok("recorded")
        })
    }
}

struct Sleeper;
impl Tool for Sleeper {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "sleep".into(),
            description: String::new(),
            input_schema: json!({}),
        }
    }
    fn call<'a>(&'a self, _a: Value, _cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            ToolOutput::ok("late")
        })
    }
}

#[derive(Clone)]
struct DeferredTool {
    started: mpsc::UnboundedSender<()>,
    gate: Arc<tokio::sync::Mutex<Option<oneshot::Receiver<()>>>>,
    calls: Arc<AtomicUsize>,
}

impl DeferredTool {
    fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<()>, oneshot::Sender<()>) {
        let (started, started_rx) = mpsc::unbounded_channel();
        let (release, gate) = oneshot::channel();
        (
            Arc::new(Self {
                started,
                gate: Arc::new(tokio::sync::Mutex::new(Some(gate))),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            started_rx,
            release,
        )
    }
}

impl Tool for DeferredTool {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "defer".into(),
            description: "deferred".into(),
            input_schema: json!({"type": "object"}),
        }
    }

    fn call<'a>(&'a self, _args: Value, _cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _ = self.started.send(());
            let gate = self.gate.lock().await.take();
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            ToolOutput::ok("deferred tool done")
        })
    }
}

fn session(tag: &str) -> (Store, dsh_session_persistence::Session, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("dsh-core-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = Store::new(&root);
    let s = store
        .create(Header {
            kind: HeaderTag::Session,
            version: SESSION_FORMAT_VERSION,
            id: "s".into(),
            created_at: 0,
            cwd: Some("/w".into()),
            parent_session: None,
            is_seeded: false,
            origin: None,
            delegation_depth: 0,
            agent_preset: None,
        })
        .unwrap();
    (store, s, root)
}

fn config() -> AgentConfig {
    AgentConfig {
        system_prompt: "sys".into(),
        cwd: ".".into(),
        max_steps: 10,
        compaction: None,
        session: None,
    }
}

fn agent(model: Scripted, s: dsh_session_persistence::Session) -> Agent {
    agent_with(Arc::new(model), s, config())
}

fn agent_with(
    model: Arc<Scripted>,
    s: dsh_session_persistence::Session,
    config: AgentConfig,
) -> Agent {
    let mut tools = ToolRegistry::default();
    tools.register(Echo);
    tools.register(Recorder);
    tools.register(Sleeper);
    Agent::new(model, tools, s, config)
}

#[tokio::test]
async fn a_tool_records_its_own_durable_events_before_its_result() {
    let (store, s, root) = session("tool-events");
    let model = Arc::new(Scripted::new(vec![
        tool_reply("c1", "record", r#"{"v":"a"}"#),
        text_reply("done"),
    ]));
    let mut agent = agent_with(model, s, config());
    agent.run_turn("go", &Cancel::default()).await.unwrap();
    let kinds = kinds(&agent);
    let call = kinds.iter().position(|k| k == "tool/call").expect("call");
    let note = kinds
        .iter()
        .position(|k| k == "note/write")
        .expect("tool event");
    let result = kinds
        .iter()
        .position(|k| k == "tool/result")
        .expect("result");
    assert!(call < note && note < result, "{kinds:?}");
    let events = &agent.session().events;
    assert_eq!(events[note].data, json!({"note":"a"}));
    assert_eq!(events[note].extra.get("seq"), Some(&json!(note)));
    assert!(events[note].extra.get("time").is_some());
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn a_failed_tool_call_still_commits_what_it_recorded() {
    let (store, s, root) = session("tool-events-failure");
    let model = Arc::new(Scripted::new(vec![
        tool_reply("c1", "record", r#"{"v":"a","fail":true}"#),
        text_reply("done"),
    ]));
    let mut agent = agent_with(model, s, config());
    agent.run_turn("go", &Cancel::default()).await.unwrap();
    let kinds = kinds(&agent);
    assert!(kinds.iter().any(|k| k == "note/write"), "{kinds:?}");
    let result = agent
        .session()
        .events
        .iter()
        .find(|e| e.kind == "tool/result")
        .expect("result");
    assert_eq!(result.data["message"]["isError"], json!(true));
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}

fn kinds(a: &Agent) -> Vec<String> {
    a.session().events.iter().map(|e| e.kind.clone()).collect()
}

fn logged_user_texts(a: &Agent) -> Vec<String> {
    a.session()
        .events
        .iter()
        .filter(|event| event.kind == "user/message")
        .filter_map(|event| event.data.get("content").and_then(Value::as_array))
        .flatten()
        .filter_map(|block| block.get("text").and_then(Value::as_str).map(str::to_owned))
        .collect()
}

fn request_user_texts(model: &Scripted) -> Vec<Vec<String>> {
    model
        .seen_messages
        .lock()
        .unwrap()
        .iter()
        .map(|messages| {
            messages
                .iter()
                .filter(|message| message.role == Role::User)
                .flat_map(|message| &message.content)
                .filter_map(|content| match content {
                    Content::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect()
        })
        .collect()
}

#[derive(Debug, Clone)]
enum Observed {
    Chunk,
    Assistant(SessionAssistantStreamFrame),
    Logged(dsh_session_format::Event),
}

fn observe(
    a: &Agent,
) -> (
    Arc<Mutex<Vec<Observed>>>,
    dsh_core::bus::Registration<AgentEvent>,
) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let registration = a.bus.on(move |event| {
        let observed = match event {
            AgentEvent::Chunk(_) => Observed::Chunk,
            AgentEvent::AssistantStream(frame) => Observed::Assistant(frame.clone()),
            AgentEvent::Logged(event) => Observed::Logged(event.clone()),
        };
        sink.lock().unwrap().push(observed);
    });
    (events, registration)
}

#[tokio::test]
async fn tool_turn_logs_v4_events_and_resumes() {
    let (store, s, root) = session("tool");
    let mut a = agent(
        Scripted::new(vec![
            tool_reply("c1", "echo", "{\"v\":7}"),
            text_reply("done"),
        ]),
        s,
    );
    let logged = Arc::new(Mutex::new(0usize));
    let l = logged.clone();
    let _reg = a.bus.on(move |e| {
        if matches!(e, AgentEvent::Logged(_)) {
            *l.lock().unwrap() += 1
        }
    });

    let end = a.run_turn("hi", &Cancel::default()).await.unwrap();
    assert_eq!(end, TurnEnd::Completed);
    assert_eq!(
        kinds(&a),
        [
            "turn/start",
            "step/start",
            "system/message",
            "user/message",
            "request/context",
            "assistant/message",
            "tool/call",
            "tool/result",
            "step/end",
            "step/start",
            "assistant/message",
            "step/end",
            "turn/end",
        ]
    );
    assert_eq!(*logged.lock().unwrap(), 13);
    // Every committed row carries the position and commit time the reader requires.
    for (index, event) in a.session().events.iter().enumerate() {
        assert_eq!(
            event.seq(),
            Some(index as u64),
            "row {index} ({}) has no matching seq",
            event.kind
        );
        assert!(
            event.time().is_some_and(|t| t > 0),
            "row {index} ({}) has no commit time",
            event.kind
        );
    }
    let result = &a.session().events[7];
    assert_eq!(result.data["message"]["content"][0]["text"], "echo:7");
    assert_eq!(result.extra["sourceEventSeqs"], json!([6]));
    assert_eq!(a.session().events[12].data["reason"]["kind"], "completed");

    // The log it wrote is a valid session the reader accepts, and history derives from it.
    drop(a);
    let reopened = store.open(Some("/w"), "s").unwrap();
    let roles: Vec<_> = derive_history(&reopened.events)
        .iter()
        .map(|m| m.role)
        .collect();
    assert_eq!(
        roles,
        [
            Role::System,
            Role::User,
            Role::Assistant,
            Role::Tool,
            Role::Assistant
        ]
    );

    // A resumed agent continues turn numbering and sends the prior history.
    let model = Arc::new(Scripted::new(vec![text_reply("again")]));
    let mut a2 = Agent::new(model.clone(), ToolRegistry::default(), reopened, config());
    a2.run_turn("more", &Cancel::default()).await.unwrap();
    assert_eq!(*model.seen.lock().unwrap(), vec![6]);
    assert_eq!(
        a2.session()
            .events
            .iter()
            .filter(|e| e.kind == "system/message")
            .count(),
        1
    );
    assert_eq!(
        a2.session()
            .events
            .iter()
            .rev()
            .find(|e| e.kind == "turn/start")
            .unwrap()
            .data["turn"],
        2
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cancel_during_tool_aborts_with_paired_results() {
    let (_st, s, root) = session("abort");
    let mut a = agent(Scripted::new(vec![tool_reply("c1", "sleep", "{}")]), s);
    let cancel = Cancel::default();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        c2.cancel();
    });
    assert_eq!(a.run_turn("go", &cancel).await.unwrap(), TurnEnd::Aborted);
    let res = a
        .session()
        .events
        .iter()
        .find(|e| e.kind == "tool/result")
        .unwrap();
    assert_eq!(res.data["message"]["isError"], true);
    assert_eq!(res.data["error"]["code"], "ABORTED");
    let last = a.session().events.last().unwrap();
    assert_eq!(
        last.data["reason"],
        json!({"kind":"aborted","reason":{"kind":"user"}})
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cancel_during_stream_keeps_partial_text() {
    let (_st, s, root) = session("stream");
    let partial = vec![
        Ok(StreamChunk::BlockStart {
            index: 0,
            kind: BlockKind::Text,
        }),
        Ok(StreamChunk::TextDelta {
            index: 0,
            text: "partial".into(),
        }),
    ];
    let mut m = Scripted::new(vec![partial]);
    m.hang = true;
    let mut a = agent(m, s);
    let cancel = Cancel::default();
    let streamed = Arc::new(Notify::new());
    let signal = streamed.clone();
    let _reg = a.bus.on(move |event| {
        if let AgentEvent::AssistantStream(SessionAssistantStreamFrame::Chunk { chunk, .. }) = event
        {
            if chunk["type"] == "text-delta" {
                signal.notify_one();
            }
        }
    });
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let end = a.run_turn("go", &task_cancel).await?;
        Ok::<_, CoreError>((end, a))
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), streamed.notified())
        .await
        .expect("partial text chunk was emitted");
    cancel.cancel();
    let (end, a) = task.await.unwrap().unwrap();
    assert_eq!(end, TurnEnd::Aborted);
    let msg = a
        .session()
        .events
        .iter()
        .find(|e| e.kind == "assistant/message")
        .unwrap();
    assert_eq!(msg.data["interrupted"], true);
    assert_eq!(msg.data["message"]["content"][0]["text"], "partial");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_failure_ends_turn_with_error() {
    let (_st, s, root) = session("fail");
    let mut a = agent(
        Scripted::new(vec![vec![Err(LlmError::new(ErrorCode::Auth, "bad key"))]]),
        s,
    );
    let end = a.run_turn("go", &Cancel::default()).await.unwrap();
    assert!(matches!(end, TurnEnd::Error(ref e) if e.code == ErrorCode::Auth));
    let last = a.session().events.last().unwrap();
    assert_eq!(
        last.data["reason"],
        json!({"kind":"error","error":{"message":"bad key","code":"AUTH"}})
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn unknown_tool_and_bad_args_are_error_results() {
    let (_st, s, root) = session("unknown");
    let mut a = agent(
        Scripted::new(vec![
            tool_reply("c1", "nope", "{}"),
            tool_reply("c2", "echo", "{bad"),
            text_reply("ok"),
        ]),
        s,
    );
    assert_eq!(
        a.run_turn("go", &Cancel::default()).await.unwrap(),
        TurnEnd::Completed
    );
    let results: Vec<_> = a
        .session()
        .events
        .iter()
        .filter(|e| e.kind == "tool/result")
        .collect();
    assert!(results[0].data["message"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("unknown tool"));
    assert!(results[1].data["message"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("invalid tool arguments"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn assistant_frames_are_dense_and_end_after_durable_settlement() {
    let (_st, s, root) = session("assistant-frames");
    let mut a = agent(Scripted::new(vec![text_reply("hello")]), s);
    let (events, _reg) = observe(&a);
    assert_eq!(
        a.run_turn("go", &Cancel::default()).await.unwrap(),
        TurnEnd::Completed
    );

    let observed = events.lock().unwrap();
    let frames: Vec<_> = observed
        .iter()
        .filter_map(|event| match event {
            Observed::Assistant(frame) => Some(frame),
            _ => None,
        })
        .collect();
    assert_eq!(frames.len(), 6);
    assert_eq!(
        frames
            .iter()
            .map(|frame| match frame {
                SessionAssistantStreamFrame::Start { revision, .. }
                | SessionAssistantStreamFrame::Chunk { revision, .. }
                | SessionAssistantStreamFrame::End { revision, .. } => *revision,
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6]
    );
    assert!(matches!(
        frames[0],
        SessionAssistantStreamFrame::Start {
            started_after_seq: 4,
            ..
        }
    ));
    let indexes: Vec<_> = frames
        .iter()
        .filter_map(|frame| match frame {
            SessionAssistantStreamFrame::Chunk { index, .. } => Some(*index),
            SessionAssistantStreamFrame::End { index, .. } => Some(*index),
            _ => None,
        })
        .collect();
    assert_eq!(indexes, vec![0, 1, 2, 3, 4]);
    assert!(matches!(
        frames.last().unwrap(),
        SessionAssistantStreamFrame::End {
            outcome: AssistantStreamOutcome::Committed {
                event_type: AssistantSettlementEventType::Message,
                seq: 5,
            },
            ..
        }
    ));
    let logged_position = observed
        .iter()
        .position(
            |event| matches!(event, Observed::Logged(event) if event.kind == "assistant/message"),
        )
        .unwrap();
    let end_position = observed
        .iter()
        .position(|event| {
            matches!(
                event,
                Observed::Assistant(SessionAssistantStreamFrame::End { .. })
            )
        })
        .unwrap();
    assert!(logged_position < end_position);
    assert_eq!(
        observed
            .iter()
            .filter(|event| matches!(event, Observed::Chunk))
            .count(),
        4
    );

    let durable = a
        .session()
        .events
        .iter()
        .find(|event| event.kind == "assistant/message")
        .unwrap();
    let records: Vec<AssistantStreamRecord> =
        serde_json::from_value(durable.data["stream"].clone()).unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(expand_assistant_stream(&records).unwrap().len(), 4);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn assistant_revisions_continue_across_attempts() {
    let (_st, s, root) = session("assistant-attempts");
    let mut a = agent(
        Scripted::new(vec![
            tool_reply("c1", "echo", "{\"v\":1}"),
            text_reply("done"),
        ]),
        s,
    );
    let (events, _reg) = observe(&a);
    assert_eq!(
        a.run_turn("go", &Cancel::default()).await.unwrap(),
        TurnEnd::Completed
    );
    let observed = events.lock().unwrap();
    let frames: Vec<_> = observed
        .iter()
        .filter_map(|event| match event {
            Observed::Assistant(frame) => Some(frame),
            _ => None,
        })
        .collect();
    assert_eq!(
        frames
            .iter()
            .map(|frame| match frame {
                SessionAssistantStreamFrame::Start { revision, .. }
                | SessionAssistantStreamFrame::Chunk { revision, .. }
                | SessionAssistantStreamFrame::End { revision, .. } => *revision,
            })
            .collect::<Vec<_>>(),
        (1..=13).collect::<Vec<_>>()
    );
    let attempts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match frame {
            SessionAssistantStreamFrame::Start { attempt_id, .. } => Some(attempt_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(attempts, vec!["s:1", "s:2"]);
    let committed: Vec<_> = frames
        .iter()
        .filter_map(|frame| match frame {
            SessionAssistantStreamFrame::End {
                outcome: AssistantStreamOutcome::Committed { seq, .. },
                ..
            } => Some(*seq),
            _ => None,
        })
        .collect();
    assert_eq!(committed, vec![5, 10]);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn empty_model_failure_settles_as_an_assistant_attempt() {
    let (_st, s, root) = session("assistant-failure");
    let mut a = agent(
        Scripted::new(vec![vec![Err(LlmError::new(ErrorCode::Auth, "bad key"))]]),
        s,
    );
    let (events, _reg) = observe(&a);
    assert!(matches!(
        a.run_turn("go", &Cancel::default()).await.unwrap(),
        TurnEnd::Error(ref error) if error.code == ErrorCode::Auth
    ));
    let attempt = a
        .session()
        .events
        .iter()
        .find(|event| event.kind == "assistant/attempt")
        .unwrap();
    assert_eq!(attempt.data["stream"], json!([]));
    let observed = events.lock().unwrap();
    let ends: Vec<_> = observed
        .iter()
        .filter_map(|event| match event {
            Observed::Assistant(SessionAssistantStreamFrame::End { outcome, .. }) => Some(outcome),
            _ => None,
        })
        .collect();
    assert_eq!(ends.len(), 1);
    assert!(matches!(
        ends[0],
        AssistantStreamOutcome::Committed {
            event_type: AssistantSettlementEventType::Attempt,
            ..
        }
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn max_token_completion_keeps_stream_but_drops_tool_calls() {
    let (_st, s, root) = session("assistant-max-tokens");
    let script = vec![
        Ok(StreamChunk::BlockStart {
            index: 0,
            kind: BlockKind::Text,
        }),
        Ok(StreamChunk::TextDelta {
            index: 0,
            text: "prefix".into(),
        }),
        Ok(StreamChunk::BlockEnd { index: 0 }),
        Ok(StreamChunk::BlockStart {
            index: 1,
            kind: BlockKind::ToolCall,
        }),
        Ok(StreamChunk::ToolCallDelta {
            index: 1,
            id: Some("cut".into()),
            name: Some("echo".into()),
            arguments_delta: "{\"v\":".into(),
        }),
        Ok(StreamChunk::Finish {
            reason: FinishReason::MaxTokens,
            replay: vec![None, None],
        }),
    ];
    let mut a = agent(Scripted::new(vec![script]), s);
    assert_eq!(
        a.run_turn("go", &Cancel::default()).await.unwrap(),
        TurnEnd::MaxTokens
    );
    let message = a
        .session()
        .events
        .iter()
        .find(|event| event.kind == "assistant/message")
        .unwrap();
    assert_eq!(
        message.data["message"]["content"],
        json!([{"type":"text","text":"prefix"}])
    );
    assert!(!a
        .session()
        .events
        .iter()
        .any(|event| event.kind == "tool/call"));
    let records: Vec<AssistantStreamRecord> =
        serde_json::from_value(message.data["stream"].clone()).unwrap();
    assert!(records.iter().any(|record| matches!(
        record,
        AssistantStreamRecord::ToolCallChunks { id, .. } if id == "cut"
    )));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn inbox_queue_drains_fifo_in_separate_turns() {
    let (_store, session, root) = session("inbox-fifo");
    let model = Arc::new(Scripted::new(vec![
        text_reply("first reply"),
        text_reply("second reply"),
        text_reply("third reply"),
    ]));
    let mut agent = Agent::new(model.clone(), ToolRegistry::default(), session, config());
    let first = agent
        .enqueue(UserInput::text("first"), InboxTarget::NextTurn)
        .unwrap()
        .unwrap();
    let second = agent
        .enqueue(UserInput::text("second"), InboxTarget::NextTurn)
        .unwrap()
        .unwrap();
    assert_eq!(agent.queue_items()[0].id, first);
    assert_eq!(agent.queue_items()[1].id, second);
    let third = agent
        .enqueue(UserInput::text("third"), InboxTarget::NextTurn)
        .unwrap()
        .unwrap();
    assert_eq!(agent.queue_items()[2].id, third);

    let ends = agent.run_pending(&Cancel::default()).await.unwrap();
    assert_eq!(ends, vec![TurnEnd::Completed; 3]);
    assert_eq!(logged_user_texts(&agent), ["first", "second", "third"]);
    assert_eq!(
        request_user_texts(&model),
        vec![
            vec!["first".to_owned()],
            vec!["first".to_owned(), "second".to_owned()],
            vec!["first".to_owned(), "second".to_owned(), "third".to_owned()],
        ]
    );
    assert_eq!(
        agent
            .session()
            .events
            .iter()
            .filter(|event| event.kind == "turn/start")
            .count(),
        3
    );
    assert!(agent.queue_items().is_empty());
    assert_eq!(agent.run_next_turn(&Cancel::default()).await.unwrap(), None);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn inbox_claims_ordered_steering_before_one_queued_turn() {
    let (_store, session, root) = session("inbox-steering");
    let model = Arc::new(Scripted::new(vec![
        tool_reply("c1", "echo", "{\"v\":1}"),
        text_reply("done"),
        text_reply("tail reply"),
    ]));
    let mut tools = ToolRegistry::default();
    tools.register(Echo);
    let mut agent = Agent::new(model.clone(), tools, session, config());
    agent
        .enqueue(UserInput::text("queued"), InboxTarget::NextTurn)
        .unwrap();
    agent
        .enqueue(UserInput::text("steer one"), InboxTarget::NextStep)
        .unwrap();
    agent
        .enqueue(UserInput::text("steer two"), InboxTarget::NextStep)
        .unwrap();
    agent
        .enqueue(UserInput::text("tail"), InboxTarget::NextTurn)
        .unwrap();

    assert_eq!(
        agent.run_pending(&Cancel::default()).await.unwrap(),
        vec![TurnEnd::Completed, TurnEnd::Completed]
    );
    assert_eq!(
        request_user_texts(&model),
        vec![
            vec![
                "steer one".to_owned(),
                "steer two".to_owned(),
                "queued".to_owned()
            ],
            vec![
                "steer one".to_owned(),
                "steer two".to_owned(),
                "queued".to_owned()
            ],
            vec![
                "steer one".to_owned(),
                "steer two".to_owned(),
                "queued".to_owned(),
                "tail".to_owned()
            ],
        ]
    );
    assert_eq!(
        logged_user_texts(&agent),
        ["steer one", "steer two", "queued", "tail"]
    );
    let claims: Vec<_> = agent
        .session()
        .events
        .iter()
        .filter(|event| {
            event.kind == "agent/inbox/spliced"
                && event.data.get("removedCount").is_some()
                && event.data.get("outcome").is_none()
        })
        .map(|event| event.data["target"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(claims, ["next-step", "next-turn", "next-turn"]);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn inbox_edit_remove_and_request_ids_preserve_identity() {
    let (_store, session, root) = session("inbox-edit");
    let mut agent = agent(Scripted::new(vec![]), session);
    let first = agent
        .submit_prompt(
            PromptRequest::new(
                "rpc-1",
                vec![Content::Text {
                    text: "original".into(),
                }],
                Some("Europe/Paris".into()),
            ),
            InboxTarget::NextTurn,
        )
        .unwrap()
        .unwrap();
    let second = agent
        .submit_prompt(
            PromptRequest::text("rpc-2", "remove me"),
            InboxTarget::NextStep,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        agent.queue_snapshot(),
        json!([
            {
                "id": first,
                "placement": "queued",
                "rpcId": "rpc-1",
                "content": [{"type":"text","text":"original"}]
            },
            {
                "id": second,
                "placement": "steering",
                "rpcId": "rpc-2",
                "content": [{"type":"text","text":"remove me"}]
            }
        ])
    );
    let splices = agent
        .session()
        .events
        .iter()
        .filter(|event| event.kind == "agent/inbox/spliced")
        .count();
    assert_eq!(
        agent
            .submit_prompt(
                PromptRequest::text("rpc-1", "duplicate"),
                InboxTarget::NextTurn
            )
            .unwrap(),
        None
    );
    assert_eq!(
        agent
            .session()
            .events
            .iter()
            .filter(|event| event.kind == "agent/inbox/spliced")
            .count(),
        splices
    );

    assert!(agent
        .replace(
            &first,
            vec![Content::Text {
                text: "edited".into(),
            }],
        )
        .unwrap());
    let edited = &agent.inbox_state().next_turn[0];
    assert_eq!(edited.id, first);
    assert_eq!(edited.rpc_id(), Some("rpc-1"));
    assert_eq!(edited.source()["clientTimeZone"], "Europe/Paris");
    assert!(agent.remove(&second).unwrap());
    assert!(!agent.remove(&second).unwrap());
    assert!(agent.remove(&first).unwrap());
    assert_eq!(
        agent
            .submit_prompt(
                PromptRequest::text("rpc-1", "retired"),
                InboxTarget::NextTurn
            )
            .unwrap(),
        None
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn inbox_recovers_pending_state_and_retired_request_ids() {
    let (store, session, root) = session("inbox-recovery");
    let mut agent = agent(Scripted::new(vec![]), session);
    let kept = agent
        .submit_prompt(
            PromptRequest::text("rpc-kept", "before edit"),
            InboxTarget::NextTurn,
        )
        .unwrap()
        .unwrap();
    let removed = agent
        .submit_prompt(
            PromptRequest::text("rpc-removed", "remove"),
            InboxTarget::NextTurn,
        )
        .unwrap()
        .unwrap();
    agent
        .replace(
            &kept,
            vec![Content::Text {
                text: "kept".into(),
            }],
        )
        .unwrap();
    agent.remove(&removed).unwrap();
    agent
        .enqueue(
            UserInput::with_source(
                vec![Content::Text {
                    text: "context".into(),
                }],
                json!({"kind":"context"}),
            ),
            InboxTarget::NextStep,
        )
        .unwrap();
    let snapshot = agent.queue_snapshot();
    assert_eq!(snapshot[0]["placement"], "queued");
    assert_eq!(snapshot[1]["placement"], "context");
    assert!(snapshot[1].get("rpcId").is_none());
    drop(agent);

    let reopened = store.open(Some("/w"), "s").unwrap();
    let model = Arc::new(Scripted::new(vec![text_reply("done")]));
    let mut recovered = Agent::new(model, ToolRegistry::default(), reopened, config());
    assert_eq!(recovered.queue_snapshot(), snapshot);
    assert_eq!(
        recovered
            .submit_prompt(
                PromptRequest::text("rpc-kept", "duplicate"),
                InboxTarget::NextTurn
            )
            .unwrap(),
        None
    );
    assert_eq!(
        recovered.run_pending(&Cancel::default()).await.unwrap(),
        vec![TurnEnd::Completed]
    );
    assert_eq!(logged_user_texts(&recovered), ["context", "kept"]);
    drop(recovered);

    let reopened = store.open(Some("/w"), "s").unwrap();
    let mut recovered = Agent::new(
        Arc::new(Scripted::new(vec![])),
        ToolRegistry::default(),
        reopened,
        config(),
    );
    assert_eq!(
        recovered
            .submit_prompt(
                PromptRequest::text("rpc-kept", "duplicate after admission"),
                InboxTarget::NextTurn
            )
            .unwrap(),
        None
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn inbox_cancellation_retains_tail_and_clear_is_durable() {
    let (store, active_session, _root) = session("inbox-cancel-retain");
    let model = Arc::new(Scripted::new(vec![text_reply("tail reply")]));
    let mut hanging = Scripted::new(vec![]);
    hanging.hang = true;
    let hanging = Arc::new(hanging);
    let mut agent = Agent::new(
        hanging.clone(),
        ToolRegistry::default(),
        active_session,
        config(),
    );
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    agent
        .enqueue(UserInput::text("tail"), InboxTarget::NextTurn)
        .unwrap();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move { agent.run_pending(&task_cancel).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while hanging.seen.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        vec![TurnEnd::Aborted]
    );

    let reopened = store.open(Some("/w"), "s").unwrap();
    let mut recovered = Agent::new(model, ToolRegistry::default(), reopened, config());
    assert_eq!(recovered.queue_items().len(), 1);
    assert_eq!(
        recovered.queue_items()[0].content[0],
        Content::Text {
            text: "tail".into()
        }
    );
    assert_eq!(
        recovered.run_pending(&Cancel::default()).await.unwrap(),
        vec![TurnEnd::Completed]
    );
    assert_eq!(logged_user_texts(&recovered), ["active", "tail"]);
    drop(recovered);

    let (_store, clear_session, root) = session("inbox-cancel-clear");
    let mut agent = Agent::new(
        Arc::new(Scripted::new(vec![])),
        ToolRegistry::default(),
        clear_session,
        config(),
    );
    agent
        .enqueue(UserInput::text("queued"), InboxTarget::NextTurn)
        .unwrap();
    agent
        .enqueue(UserInput::text("steering"), InboxTarget::NextStep)
        .unwrap();
    let cancel = Cancel::default();
    agent.cancel(&cancel, false).unwrap();
    assert!(cancel.is_cancelled());
    assert!(agent.queue_items().is_empty());
    let cleared: Vec<_> = agent
        .session()
        .events
        .iter()
        .filter(|event| event.kind == "agent/inbox/spliced" && event.data["outcome"] == "canceled")
        .collect();
    assert_eq!(cleared.len(), 2);
    assert_eq!(cleared[0].data["target"], "next-step");
    assert_eq!(cleared[1].data["target"], "next-turn");
    assert!(cleared
        .iter()
        .all(|event| event.data["outcome"] == "canceled"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn admission_during_blocked_model_is_acknowledged_after_durable_publish() {
    let (store, session, root) = session("admission-blocked-model");
    let (model, mut started, mut releases) = DeferredModel::new(
        vec![text_reply("first reply"), text_reply("second reply")],
        1,
    );
    let mut agent = Agent::new(model.clone(), ToolRegistry::default(), session, config());
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    let admission = agent.admission();
    let (observed, _registration) = observe(&agent);
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let ends = agent.run_pending(&task_cancel).await?;
        Ok::<_, CoreError>((ends, agent))
    });

    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("first model call starts")
            .unwrap(),
        0
    );
    assert_eq!(request_user_texts_from(&model), vec![vec!["active"]]);

    let admitted = tokio::time::timeout(
        TEST_TIMEOUT,
        admission.submit_prompt(
            PromptRequest::new(
                "rpc-queued",
                vec![Content::Text {
                    text: "queued".into(),
                }],
                Some("Europe/Paris".into()),
            ),
            InboxTarget::NextTurn,
        ),
    )
    .await
    .expect("admission is acknowledged while the model is blocked")
    .unwrap();
    assert_eq!(admitted.as_deref(), Some("msg-3"));

    let durable = {
        let mut events = Vec::new();
        store
            .read_streamed(Some("/w"), "s", |event| {
                events.push(event);
                Ok(())
            })
            .unwrap();
        events
    };
    let admission_event = durable
        .iter()
        .find(|event| {
            event.kind == "agent/inbox/spliced"
                && event.data["inserted"][0]["source"]["rpcId"] == "rpc-queued"
        })
        .expect("admission is durable before acknowledgement");
    assert_eq!(
        admission_event.data,
        json!({
            "target": "next-turn",
            "start": 0,
            "inserted": [{
                "id": "msg-3",
                "role": "user",
                "content": [{"type": "text", "text": "queued"}],
                "source": {
                    "kind": "user",
                    "rpcId": "rpc-queued",
                    "clientTimeZone": "Europe/Paris",
                },
            }],
        })
    );
    assert!(durable.iter().all(|event| {
        event
            .seq()
            .is_some_and(|seq| seq < admission_event.seq().unwrap())
            || event.kind != "user/message"
            || event.data["source"]["rpcId"] != "rpc-queued"
    }));
    let published = observed
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| match event {
            Observed::Logged(event) if *event == *admission_event => Some(event.clone()),
            _ => None,
        })
        .expect("admission is published before acknowledgement");
    assert_eq!(published.extra["seq"], admission_event.extra["seq"]);

    releases.remove(0).send(()).unwrap();
    let (ends, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("queued admission drains after the active turn")
        .unwrap()
        .unwrap();
    assert_eq!(ends, vec![TurnEnd::Completed, TurnEnd::Completed]);
    assert_eq!(logged_user_texts(&agent), ["active", "queued"]);
    assert_eq!(
        request_user_texts_from(&model),
        vec![vec!["active"], vec!["active", "queued"]]
    );
    assert!(agent.queue_items().is_empty());
    assert_eq!(
        agent
            .session()
            .events
            .iter()
            .filter(|event| {
                event.kind == "agent/inbox/spliced"
                    && event.data["inserted"][0]["source"]["rpcId"] == "rpc-queued"
            })
            .count(),
        1
    );
    assert_eq!(
        agent
            .session()
            .events
            .iter()
            .filter(|event| {
                event.kind == "user/message" && event.data["source"]["rpcId"] == "rpc-queued"
            })
            .count(),
        1
    );
    assert!(matches!(
        admission
            .submit_prompt(PromptRequest::text("idle", "idle"), InboxTarget::NextTurn)
            .await,
        Err(CoreError::AdmissionUnavailable)
    ));
    drop(agent);
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn concurrent_steering_keeps_next_step_order_ahead_of_queued_tail() {
    let (_store, session, root) = session("admission-steering");
    let (model, mut started, mut releases) = DeferredModel::new(
        vec![
            tool_reply("c1", "echo", "{\"v\":1}"),
            text_reply("done"),
            text_reply("steered"),
            text_reply("tail reply"),
        ],
        2,
    );
    let mut tools = ToolRegistry::default();
    tools.register(Echo);
    let mut agent = Agent::new(model.clone(), tools, session, config());
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    agent
        .enqueue(UserInput::text("tail"), InboxTarget::NextTurn)
        .unwrap();
    let admission = agent.admission();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let ends = agent.run_pending(&task_cancel).await?;
        Ok::<_, CoreError>((ends, agent))
    });

    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("first model call starts")
            .unwrap(),
        0
    );
    for (request_id, text) in [("steer-1", "steer one"), ("steer-2", "steer two")] {
        tokio::time::timeout(
            TEST_TIMEOUT,
            admission.submit_prompt(PromptRequest::text(request_id, text), InboxTarget::NextStep),
        )
        .await
        .expect("steering is acknowledged during the model await")
        .unwrap()
        .expect("new steering is admitted");
    }
    assert_eq!(request_user_texts_from(&model), vec![vec!["active"]]);

    releases.remove(0).send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("second step starts after the tool")
            .unwrap(),
        1
    );
    assert_eq!(
        request_user_texts_from(&model),
        vec![vec!["active"], vec!["active", "steer one", "steer two"]]
    );
    tokio::time::timeout(
        TEST_TIMEOUT,
        admission.submit_prompt(
            PromptRequest::text("steer-3", "steer three"),
            InboxTarget::NextStep,
        ),
    )
    .await
    .expect("late steering is acknowledged during the second model await")
    .unwrap()
    .expect("new steering is admitted");

    releases.remove(0).send(()).unwrap();
    let (ends, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("steering and queued work drain without deadlock")
        .unwrap()
        .unwrap();
    assert_eq!(ends, vec![TurnEnd::Completed, TurnEnd::Completed]);
    assert_eq!(
        logged_user_texts(&agent),
        ["active", "steer one", "steer two", "steer three", "tail"]
    );
    assert_eq!(
        request_user_texts_from(&model),
        vec![
            vec!["active"],
            vec!["active", "steer one", "steer two"],
            vec!["active", "steer one", "steer two", "steer three"],
            vec!["active", "steer one", "steer two", "steer three", "tail",],
        ]
    );
    assert!(agent.queue_items().is_empty());
    drop(agent);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn queued_item_can_move_into_live_steering_without_losing_identity() {
    let (_store, session, root) = session("admission-promote-queued");
    let (model, mut started, mut releases) = DeferredModel::new(
        vec![tool_reply("c1", "echo", "{\"v\":1}"), text_reply("done")],
        2,
    );
    let mut tools = ToolRegistry::default();
    tools.register(Echo);
    let mut agent = Agent::new(model.clone(), tools, session, config());
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    let queued = agent
        .enqueue(UserInput::text("queued"), InboxTarget::NextTurn)
        .unwrap()
        .unwrap();
    let admission = agent.admission();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let ends = agent.run_pending(&task_cancel).await?;
        Ok::<_, CoreError>((ends, agent))
    });

    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("first model call starts")
            .unwrap(),
        0
    );
    let moved = tokio::time::timeout(TEST_TIMEOUT, admission.steer(&queued))
        .await
        .expect("queued promotion is acknowledged during the model await")
        .unwrap()
        .expect("queued item exists");
    assert_eq!(moved.id, queued);
    assert!(moved.source["kind"] == "user");

    releases.remove(0).send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("second model call starts after the tool")
            .unwrap(),
        1
    );
    releases.remove(0).send(()).unwrap();
    let (ends, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("promoted steering drains")
        .unwrap()
        .unwrap();
    assert_eq!(ends, vec![TurnEnd::Completed]);
    assert_eq!(logged_user_texts(&agent), ["active", "queued"]);
    assert_eq!(
        request_user_texts_from(&model),
        vec![vec!["active"], vec!["active", "queued"]]
    );
    assert!(agent.queue_items().is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn concurrent_admissions_serialize_deduplication_and_message_ids() {
    const CONCURRENCY: usize = 12;
    let (_store, session, root) = session("admission-concurrent-ids");
    let mut scripts = vec![text_reply("active reply")];
    for _ in 0..=CONCURRENCY {
        scripts.push(text_reply("queued reply"));
    }
    let (model, mut started, mut releases) = DeferredModel::new(scripts, 1);
    let mut agent = Agent::new(model, ToolRegistry::default(), session, config());
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    let admission = agent.admission();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let ends = agent.run_pending(&task_cancel).await?;
        Ok::<_, CoreError>((ends, agent))
    });
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("first model call starts")
            .unwrap(),
        0
    );

    let identical = submit_concurrently(
        admission.clone(),
        (0..CONCURRENCY)
            .map(|index| PromptRequest::text("same-rpc", format!("copy {index}")))
            .collect(),
    )
    .await;
    let accepted: Vec<_> = identical.into_iter().filter_map(Result::unwrap).collect();
    assert_eq!(accepted, vec!["msg-3"]);

    let distinct = submit_concurrently(
        admission,
        (0..CONCURRENCY)
            .map(|index| {
                PromptRequest::text(format!("unique-rpc-{index}"), format!("unique {index}"))
            })
            .collect(),
    )
    .await;
    let mut allocated: Vec<u64> = distinct
        .into_iter()
        .map(|response| response.unwrap().unwrap())
        .map(|id| id.strip_prefix("msg-").unwrap().parse().unwrap())
        .collect();
    allocated.sort_unstable();
    assert_eq!(
        allocated,
        (4..=(CONCURRENCY + 3) as u64).collect::<Vec<_>>()
    );

    releases.remove(0).send(()).unwrap();
    let (ends, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("all admitted work drains without deadlock")
        .unwrap()
        .unwrap();
    assert_eq!(ends, vec![TurnEnd::Completed; CONCURRENCY + 2]);
    assert_eq!(logged_user_texts(&agent).len(), CONCURRENCY + 2);
    assert!(agent.queue_items().is_empty());

    let mut inserted_ids: Vec<String> = agent
        .session()
        .events
        .iter()
        .filter(|event| event.kind == "agent/inbox/spliced")
        .flat_map(|event| event.data["inserted"].as_array().into_iter().flatten())
        .filter_map(|message| message["id"].as_str().map(str::to_owned))
        .collect();
    inserted_ids.sort_by_key(|id| id.strip_prefix("msg-").unwrap().parse::<u64>().unwrap());
    let expected_ids = std::iter::once("msg-1".to_owned())
        .chain((3..=(CONCURRENCY + 3)).map(|number| format!("msg-{number}")))
        .collect::<Vec<_>>();
    assert_eq!(inserted_ids, expected_ids);
    assert_eq!(
        agent
            .session()
            .events
            .iter()
            .filter(|event| {
                event.kind == "agent/inbox/spliced"
                    && event.data["inserted"][0]["source"]["rpcId"] == "same-rpc"
            })
            .count(),
        1
    );
    assert_eq!(
        agent
            .session()
            .events
            .iter()
            .filter(|event| {
                event.kind == "user/message" && event.data["source"]["rpcId"] == "same-rpc"
            })
            .count(),
        1
    );
    for index in 0..CONCURRENCY {
        assert_eq!(
            agent
                .session()
                .events
                .iter()
                .filter(|event| {
                    event.kind == "user/message"
                        && event.data["source"]["rpcId"] == format!("unique-rpc-{index}")
                })
                .count(),
            1
        );
    }
    drop(agent);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn admission_can_edit_remove_and_splice_pending_items_during_model_await() {
    let (_store, session, root) = session("admission-mutations");
    let (model, mut started, mut releases) = DeferredModel::new(
        vec![
            text_reply("active reply"),
            text_reply("edited reply"),
            text_reply("new reply"),
        ],
        1,
    );
    let mut agent = Agent::new(model.clone(), ToolRegistry::default(), session, config());
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    let edited_id = agent
        .submit_prompt(
            PromptRequest::new(
                "rpc-edited",
                vec![Content::Text {
                    text: "original".into(),
                }],
                Some("America/New_York".into()),
            ),
            InboxTarget::NextTurn,
        )
        .unwrap()
        .unwrap();
    let removed_id = agent
        .submit_prompt(
            PromptRequest::text("rpc-removed", "remove"),
            InboxTarget::NextTurn,
        )
        .unwrap()
        .unwrap();
    let spliced_id = agent
        .submit_prompt(
            PromptRequest::text("rpc-spliced", "splice"),
            InboxTarget::NextTurn,
        )
        .unwrap()
        .unwrap();
    let admission = agent.admission();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let ends = agent.run_pending(&task_cancel).await?;
        Ok::<_, CoreError>((ends, agent))
    });
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("first model call starts")
            .unwrap(),
        0
    );

    assert!(tokio::time::timeout(
        TEST_TIMEOUT,
        admission.replace(
            &edited_id,
            vec![Content::Text {
                text: "edited".into()
            }],
        ),
    )
    .await
    .expect("replace is acknowledged during the model await")
    .unwrap());
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, admission.remove(&removed_id))
            .await
            .expect("remove is acknowledged during the model await")
            .unwrap()
    );
    let removed = tokio::time::timeout(
        TEST_TIMEOUT,
        admission.splice(InboxTarget::NextTurn, 1, 1, Vec::new()),
    )
    .await
    .expect("splice is acknowledged during the model await")
    .unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].id, spliced_id);
    let new_id = tokio::time::timeout(
        TEST_TIMEOUT,
        admission.submit_prompt(PromptRequest::text("rpc-new", "new"), InboxTarget::NextTurn),
    )
    .await
    .expect("insert is acknowledged during the model await")
    .unwrap()
    .unwrap();
    assert_eq!(new_id, "msg-6");

    releases.remove(0).send(()).unwrap();
    let (ends, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("mutated queue drains without deadlock")
        .unwrap()
        .unwrap();
    assert_eq!(ends, vec![TurnEnd::Completed; 3]);
    assert_eq!(logged_user_texts(&agent), ["active", "edited", "new"]);
    assert_eq!(
        request_user_texts_from(&model),
        vec![
            vec!["active"],
            vec!["active", "edited"],
            vec!["active", "edited", "new"],
        ]
    );
    let replacement = agent
        .session()
        .events
        .iter()
        .find(|event| {
            event.kind == "agent/inbox/spliced"
                && event.data["inserted"][0]["id"] == edited_id
                && event.data["inserted"][0]["content"][0]["text"] == "edited"
        })
        .unwrap();
    assert_eq!(replacement.data["removedCount"], 1);
    assert_eq!(replacement.data["outcome"], "canceled");
    assert_eq!(
        replacement.data["inserted"][0]["source"]["rpcId"],
        "rpc-edited"
    );
    assert_eq!(
        replacement.data["inserted"][0]["source"]["clientTimeZone"],
        "America/New_York"
    );
    let canceled: Vec<_> = agent
        .session()
        .events
        .iter()
        .filter(|event| event.kind == "agent/inbox/spliced" && event.data["outcome"] == "canceled")
        .collect();
    assert_eq!(canceled.len(), 3);
    let admitted = agent
        .session()
        .events
        .iter()
        .find(|event| {
            event.kind == "agent/inbox/spliced"
                && event.data["inserted"][0]["source"]["rpcId"] == "rpc-new"
        })
        .unwrap();
    assert_eq!(admitted.data["start"], 1);
    assert!(admitted.data.get("removedCount").is_none());
    assert!(admitted.data.get("outcome").is_none());
    assert!(agent.queue_items().is_empty());
    drop(agent);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn admission_cancellation_retains_durable_pending_tail() {
    let (store, session, root) = session("admission-cancel-retain");
    let (model, mut started, _releases) = DeferredModel::new(
        vec![text_reply("blocked reply"), text_reply("tail reply")],
        1,
    );
    let mut agent = Agent::new(model.clone(), ToolRegistry::default(), session, config());
    agent
        .enqueue(UserInput::text("active"), InboxTarget::NextTurn)
        .unwrap();
    agent
        .enqueue(UserInput::text("tail"), InboxTarget::NextTurn)
        .unwrap();
    let admission = agent.admission();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let ends = agent.run_pending(&task_cancel).await?;
        Ok::<_, CoreError>((ends, agent))
    });
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, started.recv())
            .await
            .expect("first model call starts")
            .unwrap(),
        0
    );

    tokio::time::timeout(TEST_TIMEOUT, admission.cancel(&cancel, true))
        .await
        .expect("cancellation is acknowledged during the model await")
        .unwrap();
    assert!(cancel.is_cancelled());
    let (ends, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("cancelled run stops")
        .unwrap()
        .unwrap();
    assert_eq!(ends, vec![TurnEnd::Aborted]);
    assert_eq!(agent.queue_items().len(), 1);
    assert_eq!(
        agent.queue_items()[0].content,
        vec![Content::Text {
            text: "tail".into()
        }]
    );
    assert!(!agent
        .session()
        .events
        .iter()
        .any(|event| event.kind == "agent/inbox/spliced" && event.data["outcome"] == "canceled"));
    drop(agent);

    let reopened = store.open(Some("/w"), "s").unwrap();
    let mut recovered = Agent::new(model, ToolRegistry::default(), reopened, config());
    assert_eq!(recovered.queue_items().len(), 1);
    assert_eq!(
        recovered.run_pending(&Cancel::default()).await.unwrap(),
        vec![TurnEnd::Completed]
    );
    assert_eq!(logged_user_texts(&recovered), ["active", "tail"]);
    assert!(recovered.queue_items().is_empty());
    drop(recovered);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn admission_during_blocked_tool_does_not_restart_the_tool() {
    let (_store, session, root) = session("admission-blocked-tool");
    let (model, _started, _releases) =
        DeferredModel::new(vec![tool_reply("c1", "defer", "{}"), text_reply("done")], 0);
    let (tool, mut tool_started, release_tool) = DeferredTool::new();
    let mut tools = ToolRegistry::default();
    tools.register((*tool).clone());
    let mut agent = Agent::new(model.clone(), tools, session, config());
    let admission = agent.admission();
    let cancel = Cancel::default();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let end = agent.run_turn("active", &task_cancel).await?;
        Ok::<_, CoreError>((end, agent))
    });
    tokio::time::timeout(TEST_TIMEOUT, tool_started.recv())
        .await
        .expect("tool call starts")
        .unwrap();

    tokio::time::timeout(
        TEST_TIMEOUT,
        admission.submit_prompt(
            PromptRequest::text("tool-steer", "steer"),
            InboxTarget::NextStep,
        ),
    )
    .await
    .expect("steering is acknowledged during the tool await")
    .unwrap()
    .expect("new steering is admitted");
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    release_tool.send(()).unwrap();

    let (end, agent) = tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("tool and run finish without deadlock")
        .unwrap()
        .unwrap();
    assert_eq!(end, TurnEnd::Completed);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    let result = agent
        .session()
        .events
        .iter()
        .find(|event| event.kind == "tool/result")
        .unwrap();
    assert_eq!(
        result.data["message"]["content"][0]["text"],
        "deferred tool done"
    );
    assert_eq!(result.data["message"]["isError"], false);
    assert_eq!(
        request_user_texts_from(&model),
        vec![vec!["active"], vec!["active", "steer"]]
    );
    assert!(agent.queue_items().is_empty());
    drop(agent);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn inbox_rejects_invalid_durable_splices() {
    use dsh_session_format::Event;

    let (_store, mut invalid_session, root) = session("inbox-invalid-range");
    invalid_session
        .append(&[Event {
            kind: "agent/inbox/spliced".into(),
            data: json!({"target":"next-turn","start":1,"removedCount":1,"inserted":[]}),
            extra: serde_json::Map::new(),
        }])
        .unwrap();
    let error = Agent::try_new(
        Arc::new(Scripted::new(vec![])),
        ToolRegistry::default(),
        invalid_session,
        config(),
    )
    .err()
    .expect("out-of-range splice must fail recovery");
    assert!(error.to_string().contains("out of range"));
    let _ = std::fs::remove_dir_all(root);

    let (_store, mut duplicate_session, root) = session("inbox-invalid-duplicate");
    let message = json!({
        "id":"same",
        "role":"user",
        "content":[{"type":"text","text":"pending"}],
        "source":{"kind":"user"}
    });
    duplicate_session
        .append(&[
            Event {
                kind: "agent/inbox/spliced".into(),
                data: json!({"target":"next-turn","start":0,"inserted":[message]}),
                extra: serde_json::Map::new(),
            },
            Event {
                kind: "agent/inbox/spliced".into(),
                data: json!({"target":"next-step","start":0,"inserted":[message]}),
                extra: serde_json::Map::new(),
            },
        ])
        .unwrap();
    let error = Agent::try_new(
        Arc::new(Scripted::new(vec![])),
        ToolRegistry::default(),
        duplicate_session,
        config(),
    )
    .err()
    .expect("duplicate pending IDs must fail recovery");
    assert!(error.to_string().contains("already pending"));
    let _ = std::fs::remove_dir_all(root);
}
#[tokio::test]
async fn compaction_shadows_earlier_turns_and_replaces_them_with_a_checkpoint() {
    let (store, s, root) = session("compaction");
    let mut config = config();
    config.compaction = Some(CompactionConfig {
        threshold_tokens: 4,
        keep_recent_messages: 3,
    });
    let model = Arc::new(Scripted::new(vec![
        text_reply("a1"),
        text_reply("a2"),
        text_reply("## Current Work\n- porting"),
        text_reply("a3"),
    ]));
    let mut a = agent_with(model.clone(), s, config);

    a.run_turn("one", &Cancel::default()).await.unwrap();
    a.run_turn("two", &Cancel::default()).await.unwrap();
    let end = a.run_turn("three", &Cancel::default()).await.unwrap();
    assert_eq!(end, TurnEnd::Completed);

    let kinds = kinds(&a);
    let start = kinds
        .iter()
        .position(|kind| kind == "compaction/start")
        .expect("compaction/start logged");
    assert_eq!(
        &kinds[start..start + 4],
        [
            "compaction/start",
            "compaction/summary",
            "user/message",
            "compaction/end",
        ]
    );
    let summary = a
        .session()
        .events
        .iter()
        .find(|event| event.kind == "compaction/summary")
        .expect("summary row");
    assert_eq!(summary.data["shadowedSeqs"], json!([2, 3, 5]));
    assert_eq!(
        summary.data["shadowedRange"],
        json!({ "start": 2, "end": 5 })
    );
    assert_eq!(summary.data["llmStreamCall"], json!(true));
    assert_eq!(summary.data["provider"], json!("fake"));
    assert_eq!(summary.data["model"], json!("m1"));

    let history = derive_history(&a.session().events);
    assert_eq!(history.len(), 5, "checkpoint replaces the shadowed span");
    let checkpoint = match &history[0].content[0] {
        Content::Text { text } => text.clone(),
        other => panic!("unexpected checkpoint content: {other:?}"),
    };
    assert!(checkpoint.starts_with(CHECKPOINT_PREAMBLE));
    assert!(checkpoint
        .contains("<compacted-summary>\n## Current Work\n- porting\n</compacted-summary>"));

    // The summarization call replays the shadowed span and ends with the directive.
    {
        let seen = model.seen_messages.lock().unwrap();
        assert_eq!(seen[2].len(), 4);
        assert!(matches!(&seen[2][3].content[0],
            Content::Text { text } if text == COMPACTION_INSTRUCTION));
    }
    // The next real request carries the checkpoint instead of the shadowed span.
    let texts = request_user_texts(&model);
    assert_eq!(texts[3].len(), 3, "texts[3] = {texts:?}");
    assert!(texts[3][0].starts_with(CHECKPOINT_PREAMBLE));
    assert_eq!(&texts[3][1..], ["two", "three"]);
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn failed_compaction_is_recorded_and_leaves_history_intact() {
    let (store, s, root) = session("compaction-failure");
    let mut config = config();
    config.compaction = Some(CompactionConfig {
        threshold_tokens: 4,
        keep_recent_messages: 3,
    });
    let model = Arc::new(Scripted::new(vec![
        text_reply("a1"),
        text_reply("a2"),
        vec![Err(LlmError::new(ErrorCode::MalformedResponse, "boom"))],
        text_reply("a3"),
    ]));
    let mut a = agent_with(model.clone(), s, config);

    a.run_turn("one", &Cancel::default()).await.unwrap();
    a.run_turn("two", &Cancel::default()).await.unwrap();
    let end = a.run_turn("three", &Cancel::default()).await.unwrap();
    assert_eq!(end, TurnEnd::Completed);

    let kinds = kinds(&a);
    let start = kinds
        .iter()
        .position(|kind| kind == "compaction/start")
        .expect("compaction/start logged");
    assert_eq!(
        &kinds[start..start + 2],
        ["compaction/start", "compaction/end"]
    );
    let end_row = &a.session().events[start + 1];
    assert!(end_row.data["error"]
        .as_str()
        .is_some_and(|error| error.contains("boom")));
    assert!(!kinds.iter().any(|kind| kind == "compaction/summary"));
    assert_eq!(derive_history(&a.session().events).len(), 7);
    drop(store);
    let _ = std::fs::remove_dir_all(root);
}
