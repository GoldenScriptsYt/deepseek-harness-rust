# Rust port status

`PORTING.md` owns the method and the per-crate status. This file owns what is ported, what is left, and the rules that keep the port worth having. TypeScript sizes are source lines in `packages/*/*/src`, excluding tests; the whole tree is about 520k lines, about 100k of it experimental.

## Ported

| Area | Rust crate | Verified by |
|---|---|---|
| Session log format v4, path layout | `dsh-session-format` | all 319 recorded `session.v4.jsonl` round-trip |
| `Event::seq()` / `Event::time()` accessors | `dsh-session-format` | unit tests |
| File session store: create, open, append, torn-tail repair, list | `dsh-session-persistence` | unit tests |
| Zstd persistence, independent checksummed frames, torn-frame recovery, write leases | `dsh-session-persistence` | unit tests, including 319 recorded fixtures |
| DeepSeek Messages client: SSE, stream translation, request serialization, images, Files API, retry classification, model listing | `dsh-llm`, `dsh-llm-deepseek` | unit tests, loopback integration tests |
| Imported OpenAI Chat Completions routes: seven desktop providers, 90 unique models, legacy credential reads, model catalog and selection | `dsh-cli` | Tauri carrier build; live provider requests not sent |
| Agent loop, event bus, tool registry, abort, resume, Assistant stream frames | `dsh-core` | unit and integration tests |
| Log row `seq`/`time` stamping | `dsh-core` | a test asserts every committed row carries both |
| Tools: read, write, edit, glob, grep, bash, apply_patch | `dsh-tools` | 24 tests, `read` output byte-exact |
| Headless runner | `dsh-cli` (`dsh-rs`) | end-to-end test against a mock model server |
| Web gateway: auth, RPC, WebSocket mux, static files, plugin bundles | `dsh-gateway` | tests; page byte-identical to the TypeScript Host's apart from the documented picker deviation |
| Live-frame broadcaster (bounded, drop-oldest for a lagging subscriber) | `dsh-gateway` | unit test |
| Web boot API: schema-validated settings describe/mutate, session list, default workspace, catalogs, credentials status, 7 boot streams (baseline frames) | `dsh-cli` | the real web UI boots and renders |
| `session/create`, `session/list`, `session/follow`, `session/page`, `session/events` | `dsh-cli` | unit tests, live mux probe, wire checks |
| `session/control`, `session/observeEvent`, `session/observeProjections`, `session/observeState` | `dsh-cli` | unit tests; event and projection streams use broadcaster-backed live frames |
| `session/rename`, `session/selectModel`, `session/fork`, `session/state`, `session/wait`, `session/projections`, `session/search` | `dsh-cli` | unit tests; search is a bounded literal scan, not the TypeScript query index |
| Session query, raw history, ZIP export, and Markdown export RPCs | `dsh-cli` | Query and export contract tests, plus Web Host HTTP registration coverage |
| Plugin bundle catalog, profile setup, and `pluginManager/listBundles` startup registration | `dsh-cli` | 19 focused catalog/startup tests; missing resources degrade to structured RPC errors |
| `session/follow` opening snapshot: header, bounded history records, projection baseline | `dsh-cli` | unit tests, live mux probe |
| `session/follow` live event frames after the snapshot cursor | `dsh-cli` | subscription-before-snapshot ordering is tested |
| Assistant `start`/`chunk`/`end` frames, durable compact records, reconnect baseline | `dsh-core`, `dsh-cli` | frame ordering, gap recovery, and baseline tests |
| `session/prompt`: durable admission, queued prompts, live steering, cancellation, ordered text/image/file content, continuation eligibility | `dsh-cli`, `dsh-core`, `dsh-attachments`, `dsh-llm-deepseek` | 63 CLI library tests, 22 Agent-loop tests, 13 attachment tests; end-to-end tests are being rerun |
| `session/updateQueue` and `session/attachment` readback | `dsh-cli` | durable edit/remove/recovery and exact stored-image readback tests |
| Resident Agent runtime with durable inbox snapshots | `dsh-cli`, `dsh-core` | admission-channel tests include moving a queued item into live steering without losing identity |
| Content-addressed attachment store and DeepSeek image preparation | `dsh-attachments`, `dsh-cli`, `dsh-llm-deepseek` | deterministic tests use generated PNG bytes without a live API call |
| System prompt: identity, preset persona, ordered tool guidance, deterministic assembly | `dsh-core`, `dsh-cli` | 14 focused tests; both Host Agent builders assemble it per preset |
| Interactions: blocking `ask_user_question` over the `$events` waterfall | `dsh-tools`, `dsh-cli` | 3 tool tests, 7 registry and hub tests, 3 RPC tests; the Client card answers through `$events/result` |
| Workspace instructions (`AGENTS.md`/`CLAUDE.md` project chain, `$DSH_HOME` and `$AGENTS_HOME` user chain, dedupe, byte budget) | `dsh-core` | 8 focused tests; injected into the system message by both Host Agent builders |
| Context compaction: checkpoint summarization, shadowed-span replacement, log-faithful history rebuild | `dsh-core`, `dsh-cli` | 5 module tests, 2 Agent-loop tests (success and failure); `DSH_COMPACTION`, `DSH_COMPACTION_THRESHOLD_TOKENS`, `DSH_COMPACTION_KEEP_MESSAGES` |
| Session skill catalog and filesystem discovery | `dsh-cli`, `dsh-skills` | Authenticated HTTP fixture verifies project precedence, user fallback, and invocation visibility |
| Composer command catalog and execution boundary | `dsh-cli` | `commands/list` returns the empty Rust Host catalog; `commands/execute` implements `/plan`, `/plan off`, and `/plan <message>`, and returns no match for every other command |
| Plan mode: `plan/mode` commits, narration, pending selections, conditional system-prompt guidance | `dsh-planning`, `dsh-cli`, `dsh-core` | 4 fold tests, 3 Host tests (committed, queued, guidance), and a two-turn test that asserts the boundary commit, the one-rewritten `system/message`, and the single derived system prompt |
| Goal domain: `goals/*` RPCs, `get_goal`/`create_goal`/`update_goal` tools, process-local activation, automatic rounds with a round-limit block | `dsh-planning`, `dsh-tools`, `dsh-cli` | 12 mutation tests, 6 tool tests, 7 RPC tests, and an end-to-end lifecycle test (create, pause, tool-owned resume, clear) |
| Web search and fetch: URL/content-type/charset policy, same-origin redirects, byte and character caps, bounded HTML→markdown conversion, DeepSeek Anthropic-Messages search, `web_search`/`web_fetch` output text | `dsh-web`, `dsh-cli` | 30 tests against loopback servers; the Host registers `web_fetch` for every non-Minimal preset and `web_search` only with a DeepSeek key |
| Message feedback list, put, and delete | `dsh-cli` | Session-log event fold with compare-and-set versions |
| Agent Team mode selection | `dsh-cli` | Durable `team/mode` event and live projection update; Team task tools are not ported |
| File and Session mention candidates | `dsh-cli` | Bounded workspace path search and Session title/descriptor lookup |
| `session/cancel` | `dsh-cli` | unit test; the claim hands out the `Cancel` the RPC triggers |
| `directoryPicker/list`, `directoryPicker/createDirectory` | `dsh-cli` | unit tests, wire check |
| `directoryPicker/pick` | `dsh-cli` | returns `directory-picker/unavailable`; the Host opens no OS dialog |
| Browse picker in the boot graph | `dsh-gateway` | `shell_golden` asserts the deviation is the only difference |
| Native app: Tauri window over the in-process Host | `apps/desktop-tauri` | launched; UI renders; session list, Plugins, prompt composer |

Never run against the live DeepSeek API (no key was available); the prompt path is verified against a mock model server instead.

## Handoff: current state (2026-11-23)

Electron parity remains open across all four finish-line conditions. The Rust workspace, Tauri carrier, and Rust CI changes are still local.

- On Windows with Rust 1.90.0, formatting and warning-denying Clippy pass, all 466 workspace tests pass, and the Tauri carrier builds with `--locked` and opens with the embedded Rust Host. Three tests that had gone stale were repaired: a missing `.await` in the model-selection test, the model-discovery error details (now include `provider`), and the bounded-output shell test, which now picks its command from the same bash/PowerShell choice the tool makes instead of assuming PowerShell on Windows.
- Wire surface: the last measured inventory predates the account and settings-document RPC work in this branch. Recount before using the old method totals. Remaining stubs include `credentials/set|unset` and `directoryPicker/pick` rejecting, `session/canOpenWorkspacePath` always false, `schedule/catalog` empty, and `remoteConnection/status` reporting `unreachable-host`.
- Planning replay and recurrence fixtures now pass all 34 tests. The goal test accepts validation during event construction or replay and checks that rejection leaves the state unchanged. The recurrence fixture uses a monthly rule that exceeds the candidate limit.
- The Web Host serves `session/query/sessions`, `session/query/events`, `session/query/session`, `session/export`, and `session/markdown`. These handlers read plaintext v4 logs. The existing list and search handlers still serve the web client, including live running state and inbox projections on list rows.
- Workspace commands accept the client's named `args.request` envelope, including the directory selected in the desktop workspace picker.
- Agent preset selection lists `standard`, `ptc`, `minimal`, and `cordis`; selections persist as `agent-preset/selected` and survive Session reloads. The Rust desktop settings default enables Developer Tools, so PTC and Minimal appear in the mode picker. Minimal now exposes a terminal command tool and a terminal-first prompt, but its persistent shell and `read_image` are not ported. Compaction runs for every preset through the shared Agent config. PTC still exposes Standard's direct tools instead of `run_code`. Creator adds the generated Host Service/Event catalog, tool schemas, and Creator-specific guidance, but dynamic Cordis plugin activation and the plugin authoring tools remain unported; client-side Cordis inspection requires a connected Client runtime.
- Boot and settings calls for plugin inventory and pending pairing requests report the Rust Host's current empty state. `pluginManager/registries` reports the configured npmmirror fallback and queries pnpm's resolved profile registry with a bounded timeout; it returns `null` when pnpm cannot answer. Configured imported providers return their profile model lists from `llm/discoverModels`; discovery for arbitrary draft endpoints remains unported.
- The automation page opens with an empty schedule catalog on a fresh Host. If a TypeScript schedule store exists, the Rust Host reports the migration gap and preserves the file. Credential settings read environment values and existing `.credentials.yaml` references, and remain read-only.
- Windows Session publication uses extended-length paths for its write-through rename. The authenticated Host test creates a Session in a long workspace path.
- The browser job controller has no production job producer. `dsh-tools::JobRegistry` owns the tested output cap and still needs Host and model-tool integration. `TerminalRegistry` also needs that integration.
- Rust 1.90.0 checks all workspace targets, including the browser terminal controller and the `dsh-tools` POSIX PTY backend, for Linux x86-64 and macOS arm64. Native platform runs still need verification.
- The Goal and plan-mode domains landed on the Rust Host. Goals answer `goals/get|create|edit|pause|resume|complete|clear`, expose `get_goal`, `create_goal`, and `update_goal` to the model, keep activation process-local, and drive automatic rounds up to the configured cap. Plan mode answers `/plan`, `/plan off`, and `/plan <message>`, logs `plan/mode` plus a narration `user/message` only on a real edge, queues a selection made during an open turn and commits it at the turn boundary, and appends conditional guidance to the system prompt; the prompt is re-logged as one `system/message` rewritten through `surfaceOp`, and history rebuild honors that replacement. Gaps: `blockedAfterConsecutiveRounds` is not enforced by `update_goal` (the tool cannot see message sources), `exit_plan_mode` and its review need user questions, `/plan` refuses attachments instead of forwarding them, and `/plan <message>` admission has no Host test because the resident Agent holds the Session write lease.

Priority for the port after the workspace is green: (1) live DeepSeek end-to-end run with a real key, (2) API key storage and account logout revocation, (3) approvals and user questions (which also unblock `exit_plan_mode` plan review), (4) skills injected into the model prompt, (5) workspace, terminal and job controllers, (7) subagents, todo driver, schedule delivery, (8) sandbox, MCP HTTP transports and on-demand tool loading, (9) session migrations and SQLite, (10) installer, updater, tray, single instance, deep links, signing, macOS and Linux. The sections below list the full inventory.

Earlier focused suites include 98 `dsh-cli` library tests, 22 Agent-loop tests, 13 attachment tests, 23 skills tests, and the planning (50), interaction (21), and MCP (41) foundation suites. The workspace runs 545 tests in 52 binaries. `dsh-tools` now has 38 tests, including the Cordis inspector. Release binary 4.4 MB (`dsh-rs`), 5.7 MB with the carrier; Host RSS 13 MB idle, 29.5 MB with the window. The Node Host alone was 223 MB.

## Left

### 1. Chat and the web client surface

Closest to the user, so it comes first.

- Plugin catalog registration is wired into Host startup. Regenerate the portable resources and supply the bundle packages, compatibility metadata, and patches before expecting the packaged Plugins page to list working bundles. Installation, removal, enable/disable, updates, reload, registry access, and the plugin runtime remain.
- The embedded Rust Host does not load TypeScript Cordis plugins. The Tauri carrier accepts `DSH_NODE_PROFILE=<name>` to run an existing Cordis profile through its Node compatibility sidecar; `superagent` resolves to the installed `dsh-superagent` package and its custom presets in this environment. `apps/desktop-tauri/run-with-plugins.ps1` launches the latest debug carrier with that profile. The carrier stays Rust while the profile Host and plugins execute in Node. With the variable unset, the embedded Rust Host activates no profile plugin code.
- Regenerate the Rust Cordis inspection catalog with `node --import tsx/esm rust/tools/gen_cordis_api_catalog.mjs` after changing `packages/extensions/tool-cordis/src/api-catalog.ts`.
- Live increments on the remaining streams: `workspace/follow` upsert, `sessionStatus/follow` items, `pluginEvents/follow` graph updates, and Cordis events beyond the currently published Session activity/status/error values. The gateway can publish now; each endpoint needs its frame shapes.
- Remaining Session methods: `initializeDefaultModel`, `openWorkspacePath`, `workspacePathApplications`. `canOpenWorkspacePath` is implemented and returns false because this Host has no native path opener.
- Queue and attachment integration needs real Host coverage for steering during a live stream, cancellation with inbox retention, request-ID idempotency, cold queue mutation, receipt binding/retirement, and model image-modality validation. Authenticated HTTP upload transport is being ported separately.
- Remaining controllers: `workspace-controller`, `workspace-files`, `terminal-controller`, `job-controller`, `remotes`, `remote-access`.
- Credentials and account: API key storage, account notifications, server-side logout revocation, and recovery from expired account grants.
- Interactions: approvals. Blocking `ask_user_question` is ported — the tool forwards
  `user-questions/request` over the `$events` downlink and the Client answers through
  `$events/result`; timed mode, the `userQuestions` projection, and plan-review intents
  remain.
- The Rust Host still registers no general slash-command plugins, Agent Team task operations, or message-feedback UI commands beyond the feedback RPCs listed above. Unsupported commands return the normal no-match result.

### 2. Agent features missing from `dsh-core` and `dsh-tools`

| Feature | TypeScript source | Lines |
|---|---|---|
| Tool presentation, concurrency, output spill | `core/tools`, `spill` | 3.8k |
| Subagents | `subagent/subagent` | 5.1k |
| Team / shared tasks | `team` | |
| Todos and schedules | `todo`, `schedule` | 3.6k (schedule) |
| Loop and tool guards | `guard` | |
| Hooks, approvals, permission presets | `core`, `credentials` | |
| File observation policy, sandbox | `fs-sandbox`, `sandbox` | |
| Persistent terminals, background jobs | `terminal`, `jobs`, `shell/*-persistent` | |
| PTC (programmatic tool calling) | `ptc-runtime`, `core/tool-executor-ptc` | 4.1k |
| LSP integration | `lsp` | |
| MCP client: HTTP transports, OAuth, reconnect supervision, on-demand tool loading, resources, and prompts | `mcp/mcp-client` | 3.4k (stdio transport, tool bridge, and server-qualified registration are done) |
| Skill invocation, model prompt and tool integration, and catalog invalidation | `dsh-skills`, `dsh-core`, `dsh-cli` | |
| Host slash command registration and execution | `interaction/commands` | |
| Browser use, computer use | `browser-use`, `computer-use` | |
| Image normalization and attachment-service preparation, catalog version gating, token meter (LLM side) | `llm/*`, `attachment` | |
| Other provider protocols and full pi-ai parity (Bedrock, provider-native auth, dynamic catalog/discovery, etc.) | `llm/llm-pi-ai` | 4.7k |
| SSH execution providers | `ssh` | |
| Attachments, deliverables | `attachment`, `deliverables` | |

### 3. Sessions and storage

- SQLite session backend (`session-persistence-sqlite`).
- Generation migration plumbing (`session-persistence-jsonl`) beyond the current v4 reader/writer.
- Migrations v0 to v3 to v4 (`session-format-v0-to-v1` through `v3-to-v4`, about 6.4k lines). Deferred with evidence: every Session on the development machine is v4 (80 of 80 current-generation logs), the recorded corpus is v4, and each edge is a frozen decoder whose fixtures are inline TypeScript objects rather than data files, so a Rust port has no oracle here. Upgrade path: open an old Session once through the Node Host, which rewrites it as v4; the Rust Host then reads it. A v1-v3 log is skipped by listing and reported as not found by id.
- Production projection/cache lifecycle (`session-projection*`, `session-stats`, `session-title*`, `session-turn-outline`) and indexed session query/export (`session-query`). The current Host rebuilds a small projection set from stored events and uses bounded scans for search and query.
- Non-session storage, identity, telemetry (`storage`, `identity`, OTel).

### 4. Host composition

- Profiles and bundles (`bundle/*`, `boot/*`, `apps/cli`): the Rust Host is composed in code, not from `cordis.yml`. A loader for the same YAML keys is undecided.
- Plugin market (`boot/plugin-manager`, `plugin-market`), installation/removal/update operations, extensions and runtime self-modification (`extensions/*`, about 26k lines). Bundle listing and Host startup registration are implemented, but the Rust Host supports no TypeScript plugins; changing that is a decision, not an oversight.
- `acp`, `sdk`, `api/*` surfaces for the Python and TypeScript SDKs, and their snapshot gates.

### 5. Native app

Done: window, in-process Host, portable folder, browse directory picker.

Open: installer (MSI/NSIS), single-instance, tray, auto-update and the mandatory-update policy, `dsh-app://` protocol and the Electron preload bridges, resources embedded in the exe, code signing (`apps/desktop/windows-signing`), macOS and Linux builds.

### 6. Experimental

`packages/experimental/*` (about 100k lines: liveshare, inspector, client-ui-forge, webworker-runtime, timeline, cua, forge), the plugin-market generator, and the website.

### 7. Verification gaps

- No run against the live DeepSeek API; there is no Rust equivalent of `pnpm run test:e2e`.
- The repo's recorded-session snapshots (`pnpm run test:snapshot`) are used for the session log format only; replaying whole sessions through the Rust agent loop is not set up.
- The Rust CI jobs added in `.github/workflows/` have not yet been exercised on this change set.
- The boot graph (`assets/web-boot.captured.json`) is a capture of the TypeScript `web` profile. It needs regenerating when client plugins are added or removed; `shell_golden` fails when the rendered page drifts for any other reason.

## Method

For each unported call: start the Host (`rust/tools/restart_host.ps1`), read the `[dsh-rs] unported rpc` log, capture the TypeScript Host's answer with `rust/tools/capture_rpc.py` / `capture_streams.mjs`, implement, test.

Browser automation against this page is unreliable. `rust/tools/follow_probe.mjs <cookie> <sessionId> [endpoint]` drives one stream over the real mux socket and prints every frame; that is how both the opening snapshot and the populated history were verified.

A new wire shape gets a golden expectation, never a guess. When the Rust Host must differ from the TypeScript Host on purpose, the golden states the deviation and asserts it is the only one — see `crates/dsh-gateway/tests/shell_golden.rs` and the browse picker.

## Engineering guide for whoever ports the rest

The reason for the port is lower memory and higher speed. Measured so far: the Rust Host plus Tauri shell is a 5.7 MB exe at about 30 MB RSS before the webview; the Node Host alone was 223 MB. The webview (WebView2, about 500 MB for its process tree) is Chromium and cannot be reduced from Rust, so every saving has to come from the Host. Do not regress that number: measure RSS after each feature, and after a long session, not only at idle.

### Memory

- **Do not hold whole sessions in memory.** `Store::read_all` loads every event of every session; it is fine for tests and wrong for a real home directory. Next step: index only what listing needs (id, cwd, mtime, title, blank, last prompt) into a small cache file or SQLite table, and read events on demand with a streaming reader (`BufRead::lines`), not `fs::read` into one buffer.
- **Agent history is the big one.** `Agent` keeps `Vec<Message>` and clones it for every model request (`self.history.clone()` in `dsh-core/src/agent.rs`). Replace with `Arc<[Message]>` snapshots or pass a borrowed slice into the request builder; serialize to the request body once, straight into a reused `Vec<u8>`.
- **Stream, never accumulate.** SSE chunks go from the socket to the UI frame without intermediate `String` copies of the whole response. Tool output over a cap goes to disk (the TypeScript `spill` package does this) and the model gets a head, a tail and a path.
- **Bounded channels everywhere.** Every `mpsc` has a capacity and a stated policy when full (backpressure, drop oldest, or fail the stream, like the gateway's `uplink-overflow`). No `unbounded_channel`. The live-frame broadcast follows this: 256 frames, oldest dropped for a lagging subscriber, and its stream skips the gap.
- **Cap per-session caches** (LRU by bytes, not by count). Drop projections for sessions no client has open.
- **Share, don't clone, large values**: `Arc<str>` / `Arc<[u8]>` / `bytes::Bytes` for file contents, tool output and bundle sources. The plugin route reads about 8 MB of bundles per request: cache them as `Bytes` once, and serve with `Content-Encoding: gzip` or brotli precompressed at startup (the TypeScript Host does gzip).
- **Prefer `serde_json::value::RawValue`** where a payload is only passed through (event `data`, RPC results), so it is not parsed into a tree and re-serialized. `Event.data: Value` is the current exception; switch it when the log reader becomes a hot path.
- **Release profile is already tuned** (`lto`, `codegen-units = 1`, `panic = "abort"`, `strip`). Keep dependencies lean: check `cargo tree -d` and `cargo bloat --release` before adding a crate, and turn off default features (for example `reqwest` without default TLS features if rustls is chosen).

### Speed

- **Do the work off the async executor.** File scans (`glob`, `grep`, `read_all`), zstd, and large JSON parsing go in `spawn_blocking` or a dedicated rayon pool, as `glob` and `grep` already do. Never run a blocking call inside an `async fn` on the request path. `directoryPicker/list` and `session/prompt`'s store reads are still blocking on the request path.
- **One HTTP client.** `client::run` builds a `reqwest::Client` per request; build one per process and reuse it for connection pooling and TLS session reuse.
- **Batch durable writes.** `Session::append` calls `sync_data` per call. Group events per step (the loop already appends one at a time), `fsync` at turn boundaries and on `turn/end`, and keep torn-tail repair as the safety net. The TypeScript store batches live writes (`LIVE_WRITE_BATCH_MAX_DELAY_MS`).
- **Run independent tools concurrently**, as the TypeScript loop does for tools marked concurrency-safe (`glob`, `grep`, reads). The Rust loop currently runs tool calls one at a time.
- **Avoid per-event allocation on hot paths**: reuse buffers in the SSE decoder (it currently `replace`s and `drain`s a `String`), and parse frames from `&[u8]` with `memchr`.
- **Zero-copy static serving**: serve `apps/web/dist` and plugin files with `tokio::fs` streaming or memory-mapped files, with `ETag`/`Cache-Control: immutable` (assets already carry content hashes).
- **Compile-time, not runtime, dispatch** where the set is closed (event kinds, RPC methods): an `enum` plus `match` instead of `HashMap<String, Box<dyn Fn>>` for the hot RPCs; keep the map only for plugin-style extension points.

### Correctness rules that protect the savings

- Keep wire formats byte-compatible with the TypeScript Host until it is retired; the golden tests (`shell_golden`, the 319 session fixtures) are the safety net. Add a golden test for every new wire shape instead of guessing it.
- A deliberate deviation is stated in its golden, not hidden by re-recording the expectation.
- Cancellation must free memory too: dropping a turn drops its channels, tasks and buffers (`task.abort()` plus dropping the receiver). Test it.
- Every new feature ships with a measurement: RSS before and after, and wall time for the operation. A feature that adds more than a few MB at idle needs a reason.
- Profile before optimizing: `cargo flamegraph`, `tokio-console`, and `dhat` for allocation counts. Fix what the profile shows, not what looks slow.

### Finish line

The Rust Host replaces the Node Host for the desktop and web product when all of these hold:

- A conversation runs end to end with tools, approvals, and live updates in the unmodified web client.
- Two consecutive releases of recorded Sessions replay byte-identically through the Rust loop.
- The desktop carrier installs, updates, and survives an interrupted write on each shipped platform.
- Measured idle RSS stays under 40 MB with the webview excluded.
