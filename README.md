# Agent Hub MCP

> 📖 中文项目总结（架构图、工具清单、安全设计、快速上手）：[`docs/PROJECT_SUMMARY.zh.md`](docs/PROJECT_SUMMARY.zh.md) ｜ 事故复盘：[`docs/INCIDENT_CROSS_TALK_AND_RELAY_ARCHITECTURE.md`](docs/INCIDENT_CROSS_TALK_AND_RELAY_ARCHITECTURE.md)

A local MCP server that gives one orchestrator (Hermes Agent) **read and write access to four separate desktop AI apps** — Codex/ChatGPT Desktop, DeepSeek Harness, Antigravity, and VS Code Cline — plus a zero-token watchdog that pushes phone notifications when a session finishes, stalls, or needs approval.

It exists to solve one specific problem: these apps each keep their own private session database, and none of them exposes an API for "send this text into *that existing conversation*". Copy-pasting between them by hand does not scale. Doing it by scraping the DOM does not stay correct.

---

## The problem it solves

Running several desktop agents at once means four databases, four session-ID formats, four notions of "status", and — for the Electron apps — **a single window with a single composer**.

The naive approach is a background script that connects to the debug port and types into whatever input box is on screen. That approach has a failure mode with real consequences:

> The script was supposed to deliver a report into conversation *A*. The window happened to be showing conversation *B*. It injected the report into *B* and pressed send. Then it read *B*'s reply back and delivered it to the wrong agent.
>
> Nothing errored. Both messages were "delivered successfully."

So the design goal here is not "make sending work". It is **"make sending into the wrong conversation impossible, and make a failed send loud rather than silent."**

---

## What is covered

| App | Read path | Write path |
|---|---|---|
| **Codex / ChatGPT Desktop** | `~/.codex/state_5.sqlite`, `thread_history_1.sqlite`, `goals_1.sqlite` | Electron CDP on `:9222` (opt-in), app mutex + React-Fiber routing + two-phase session locking |
| **DeepSeek Harness (DSH)** | `session_projcache` + `session.v4.jsonl.zstd` content search | Native `dsh-agent-hub-bridge` Cordis plugin over authenticated `127.0.0.1:43917` |
| **Antigravity** | `conversation_summaries.db`, `conversations_fts`, `transcript*.jsonl` | Electron CDP on `:9224` (opt-in), same mutex + locking protocol |
| **VS Code Cline** | `~/.cline/data/db/sessions.db`, `session-search.db`, task `messages.json`, VS Code `globalStorage` | `local.agent-hub-cline-bridge` extension on `127.0.0.1:43918`, with Cline Hub WebSocket fallback |

Every app supports: list sessions, read a session verbatim, search across message bodies, send a message, create a new session, and interrupt a running turn. Read-only for all four; **write is opt-in per app** and each write goes through the verification protocol below.

---

## Safety model — three layers

**1. App-level cross-process mutex.** Codex and Antigravity are single-window Electron clients. Only one send transaction may hold an app's operation lock at a time, enforced across *processes* with `fcntl` locks in the lock directory. Concurrent requests queue instead of racing for the same DOM.

**2. Route verification before injection.** A send is refused unless the *actually active* session matches the requested one:

```
acquire app mutex
  → read active session id from React Fiber router (fallback: sidebar DOM)
  → if mismatch: navigate to /c/<target_id>, poll until the route settles
  → final gate: re-read the active id; still mismatched → abort, inject nothing
  → verify composer has no unsaved draft
  → inject text, dispatch send
  → confirm the message mounted as a user turn
release mutex
```

A mismatch raises `SessionMismatchError` and returns the *actual* session that was on screen, so the caller can see what happened. It never falls back to "send into whatever is visible", and it never opens a new conversation as a workaround.

**3. Audit log.** Every send/relay/approve/cancel/create writes an entry with target app, requested and *verified* session id, verified title, character count, text hash, mode, and outcome. The hash makes duplicate delivery detectable; a 60-second duplicate guard is applied by default.

---

## Exposed MCP tools (24)

### Read, summary, binding
1. `hub_overview(limit_per_app=5)`
2. `hub_list_sessions(app="all", active_only=False, include_archived=False, include_subagents=False, limit=20)`
3. `hub_search_sessions(query, app="all", limit=15)` — full-text and message-body search across all four apps
4. `hub_read_session(app, session_id, tail_messages=10, max_chars_per_message=6000, include_tools=False)` — exact IDs, bound aliases, or fuzzy phrase matching
5. `hub_summarize_session(app, session_id, tail_messages=16)` — structured completion summary cards
6. `hub_handover_session(app, session_id, target_hint="", tail_messages=16)` — lossless cross-agent handover packets
7. `hub_bind_task(alias, app, session_id, note="")`
8. `hub_list_bindings()`
9. `hub_unbind_task(alias)`

### Write, approval, cross-agent relay
10. `hub_writer_status()` — live write-bridge availability per app
11. `hub_send_message(app, session_id, message, sender_tag="[via Hermes]", mode="queue", model="", provider="", reasoning_effort="", dry_run=False, allow_duplicate=False, watch=False, ...)`
12. `hub_relay_message(source_app, source_session_id, target_app, target_session_id, source_message_index=None, source_role="assistant", extract_code_block=False, override_text="", prefix_instruction="", mode="queue", dry_run=True, ...)` — one-shot verbatim relay; **dry-run by default**
13. `hub_create_session(app, title="", workspace="", initial_message="", ...)` — all four apps
14. `hub_respond_approval(app, session_id, decision="yes", feedback="", dry_run=False)`
15. `hub_cancel_session(app, session_id)`
16. `hub_audit_recent(limit=20, app="all", action="")`

### Centralised bi-directional relay (`relay_states.sqlite`)
17. `hub_relay_start(source_app, source_session_id, target_app, target_session_id, name="", bidirectional=True, settle_seconds=20, min_hop_interval_seconds=15, max_hops=20, notify_targets="weixin", notify_on_hop=False, notify_on_milestone=True, ...)`
18. `hub_relay_list(include_disabled=False, include_recent_tx=5)`
19. `hub_relay_stop(relay_id_or_name, delete=False)`
20. `hub_relay_tick(relay_id="", dry_run=True)`

Relay state lives in SQLite with idempotent transaction IDs, a settle window, a minimum hop interval, and a hop cap — so a formatting difference or a mid-stream read cannot trigger an infinite ping-pong loop or a notification storm.

### Zero-token watchdog
21. `hub_watch_add(app, session_id, alias="", notify_targets="qqbot", stall_minutes=15, notify_on="all", verify_agent=False, suppress_routine=False)`
22. `hub_watch_list()`
23. `hub_watch_remove(watch_id_or_alias)`
24. `hub_watch_check_now(dry_run=True, verify_agent=False, suppress_routine=False)`

The watchdog polls session status on a timer, fires only on *transitions* (completed / waiting_approval / waiting_user / error / interrupted / stalled), and de-duplicates with per-event cooldowns. Optional Level-2 verification shells out to the Hermes CLI and asks a cheap model to classify the session's real state before deciding whether a notification is worth sending.

---

## Install

Requires macOS (the watchdog uses `launchd`; the adapters themselves are portable) and Python ≥ 3.11.

```bash
git clone https://github.com/NanqiShen/agent-hub-mcp.git
cd agent-hub-mcp
python3 -m unittest discover -s tests      # 47 tests, no network, no app required
```

Register with an MCP client (stdio transport):

```json
{
  "mcpServers": {
    "agent-hub": {
      "command": "python3",
      "args": ["-m", "agent_hub.server"],
      "cwd": "/absolute/path/to/agent-hub-mcp"
    }
  }
}
```

The two write bridges need separate installs — a VS Code extension (`vscode_cline_bridge/`, prebuilt `.vsix` included) and a DSH Cordis plugin (`dsh_plugin/`). Both bind to loopback and require a token file that you create yourself.

## Configuration

All paths and ports are overridable; nothing machine-specific is baked in.

| Variable | Purpose |
|---|---|
| `AGENT_HUB_CODEX_CDP_PORT` / `AGENT_HUB_ANTIGRAVITY_CDP_PORT` | Electron debug ports (default `9222` / `9224`) |
| `AGENT_HUB_DSH_BRIDGE_URL` / `AGENT_HUB_DSH_BRIDGE_TOKEN_PATH` | DSH bridge endpoint and its bearer token file |
| `AGENT_HUB_CLINE_BRIDGE_URL` / `_TOKEN_PATH` / `_STATE_PATH` | Cline bridge endpoint and token |
| `AGENT_HUB_WATCHES_PATH` / `AGENT_HUB_BINDINGS_PATH` / `AGENT_HUB_AUDIT_PATH` / `AGENT_HUB_RELAY_DB_PATH` | State file locations |
| `AGENT_HUB_LOCK_DIR` / `AGENT_HUB_CLINE_HUB_LOCK` | Mutex directory |
| `AGENT_HUB_HERMES_BIN` | Hermes CLI used for Level-2 verification |
| `AGENT_HUB_WATCHER_VERIFY_*` | Model, provider, reasoning effort, timeout, and enable flags for Level-2 verification |

Tokens are read from files at runtime and are never logged, audited, or written to this repository.

## Tests

```bash
python3 -m unittest discover -s tests -v     # 47 tests
python3 scripts/verify_writers.py --status   # probe live write-bridge readiness
```

No network and no running desktop app are required — the adapters and writers are tested against in-process fakes that replay real DOM/database shapes.

## Known limitations

- **Electron writes are inherently racy with the user.** If you click into the window mid-send you may race the injection. The mutex protects agent-vs-agent, not agent-vs-human. Prefer `dry_run=True` when the app is in active use.
- **CDP is opt-in and port-based.** A crashed or restarted app changes its target list; `hub_writer_status()` reports readiness precisely so a caller can check before sending.
- **Session-ID formats differ per app** and are treated as opaque strings. There is no cross-app ID mapping.
- **Level-2 verification costs a model call.** With it off, the watchdog is purely transition-based and can be fooled by long tool-call stalls that look like idle.
- **The relay is polling, not event-driven.** `settle_seconds` and `min_hop_interval_seconds` trade latency for safety by design.

## Provenance

This code was written by an AI agent (Antigravity / Gemini) working under human direction, in response to a documented cross-talk incident. The incident write-up and the resulting architecture requirements are in [`docs/INCIDENT_CROSS_TALK_AND_RELAY_ARCHITECTURE.md`](docs/INCIDENT_CROSS_TALK_AND_RELAY_ARCHITECTURE.md) — it is worth reading before changing the write path.

Session IDs, project names, and machine paths in that document and in the test fixtures have been replaced with synthetic placeholders.

## License

MIT — see [LICENSE](LICENSE).
