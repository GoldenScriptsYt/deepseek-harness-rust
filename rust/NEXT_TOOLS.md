# Missing model-facing tools: port plan

Ordered by dependency. Each entry names the TypeScript source, the Rust landing spot, the wire methods the client calls, and the test that proves it.

## 1. Interactions: `ask_user_question` and approvals

Everything else in this list that needs a human answer depends on this one.

- TS: `packages/interaction/user-questions/src/*`, `user-approval`, `tool-ask-user`.
- Rust: new crate `dsh-interaction` (question registry keyed by `(session, callId)`, waiter map, timeout, continuation).
- Wire: `userQuestions/attachWait`, `userQuestions/answer`, `userQuestions/projection`, event `user-questions/request`.
- Tool: `dsh-tools/src/ask_user_question.rs` with intents `question`, `plan-review`, `approval`.
- Blocks until answered or cancelled through `ToolContext.cancel`.
- Test: a Host test answers through the RPC and the tool returns the chosen option; a cancelled turn retires the waiter.

## 2. `exit_plan_mode` and plan review

- Depends on item 1's `plan-review` intent.
- TS: `packages/plan/*`, tool schema in `docs/tool-catalog.md` (`exit_plan_mode`, line ~659).
- Rust: `dsh-tools/src/plan.rs` calling the plan registry already ported in `dsh-planning` (`plan.rs`), rejecting entry outside plan mode.
- Wire: reuse `commands/execute` `/plan` state plus the `plan` projection already served.
- Test: entering plan mode, calling the tool, and accepting the review clears pending state and logs `plan/exit`.

## 3. Agent task tools

- `list_agents`, `send_message`, `wait_agent`, `interrupt_agent`.
- Requires a task registry: today `HostSubagentRunner` is synchronous, so there is no handle to address.
- Rust: `dsh-tools/src/agents.rs` + a `TaskRegistry` in `dsh-cli` mapping task id → child session id, status, and a `JoinHandle`; `subagent` gains an async mode that returns a task id.
- Wire: `taskStatus` RPC already exists on the client (`task_status`/`task_cancel` remote calls).
- Test: start a task, `list_agents` shows it running, `wait_agent` returns its result, `interrupt_agent` cancels.

## 4. Schedule tools and the Automation page

- `schedule_create`, `schedule_edit`, `schedule_list`, `schedule_view` plus `schedule/catalog|edit|history|deleteInactive`.
- TS: `packages/schedule/tool-schedule/src/index.ts`; schemas in `docs/tool-catalog.md` line ~1648.
- Rust: `dsh-planning/src/schedule.rs` (store: `<home>/schedule/tasks.json`, RFC 5545 subset parser, next-occurrence in TZID).
- Delivery: the Host tick that already drives Goal rounds also drains due occurrences into the target Session's durable inbox.
- Client rows: mirror `ui-schedule/src/client/task-record.ts`.
- Test: create a daily rule, tick past one occurrence, assert one durable message; edit/delete/history round-trip.

## 5. PTC `run_code`

- TS: `packages/ptc-runtime` (4.1k lines) and `core/tool-executor-ptc`.
- Rust: embed QuickJS (`rquickjs`) rather than Node; expose `gw.rpc`-shaped tool calls, `console`, and a bounded 64 KB return.
- Advisory-only until the sandbox lands: refuse without `DSH_PTC=1`.
- Test: a script calls two tools and returns their combination; a timeout and an oversized return both fail cleanly.

## 6. `workflow`, `present`, office tools

- `workflow`: `packages/workflow` — step state machine over tool calls, durable per-step log.
- `present`: render a deliverable and hand the client a resource URL; needs `deliverables`.
- Office (`tool-office`): docx/xlsx/pptx read and write; port last, it is mostly format libraries.

## 7. Plugin-manager and market tools

- `pluginManager/*` and `pluginMarket/*` RPCs used by the Plugins page: list bundles (done), install/remove/toggle/update, market index search, entry detail.
- Blocked on the decision recorded in `PORTING.md`: the Rust Host loads no TypeScript plugins.
- Interim: serve the read-only catalog plus explicit `plugin-manager/unsupported` errors instead of `unported rpc` log lines.

## Wire surface

Every method above is already logged by the Host as `[dsh-rs] unported rpc` when the client asks; use `rust/tools/capture_rpc.py` against the TypeScript Host for the golden answer before implementing.
