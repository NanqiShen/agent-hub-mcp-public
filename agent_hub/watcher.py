"""Zero-token local watchdog store, transition evaluator, and launchd daemon."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Iterator
import uuid

from .adapters import AntigravityAdapter, ClineAdapter, CodexAdapter, DshAdapter
from .models import (
    AppAdapter,
    STATUS_ERROR,
    STATUS_IDLE,
    STATUS_INTERRUPTED,
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
    STATUS_WAITING_USER,
    SessionSummary,
    clean_single_line,
    truncate_text,
)
from .summary import (
    BG_LABEL_RUNNING,
    BG_LABEL_SUBMITTED,
    detect_background_job,
    extract_task_completion_summary,
    format_completion_card,
    format_handover_packet,
)


DEFAULT_WATCHES_PATH = Path.home() / ".hermes" / "agent-hub" / "watches.json"
# Resolve the Hermes CLI without baking in a machine-specific absolute path.
# Override with AGENT_HUB_HERMES_BIN, otherwise fall back to ~/.local/bin/hermes
# and finally to whatever `hermes` resolves to on PATH.
DEFAULT_HERMES_BIN = os.environ.get("AGENT_HUB_HERMES_BIN") or str(
    Path.home() / ".local" / "bin" / "hermes"
)

ALL_NOTIFY_EVENTS: tuple[str, ...] = (
    "completed",
    "waiting_approval",
    "waiting_user",
    "error",
    "interrupted",
    "stalled",
)

DEFAULT_VERIFY_EVENTS: tuple[str, ...] = ("completed", "stalled", "error")
DEFAULT_DAEMON_DEBOUNCE_SECONDS = 25
DEBOUNCE_EVENTS: tuple[str, ...] = ("error", "completed")
DEFAULT_FALLBACK_CHANNELS: dict[str, str] = {
    "weixin": "qqbot",
    "qqbot": "weixin",
}

_TRANSIENT_CLASSIFICATIONS: frozenset[str] = frozenset(
    {
        "transient_error",
        "self_correcting",
        "tool_error_recovering",
        "transient_tool_error",
        "false_alarm",
        "recovering",
    }
)

_EVENT_LABELS: dict[str, str] = {
    "completed": "回合完毕 / 等待输入",
    "waiting_approval": "等待审批",
    "waiting_user": "等待你回复",
    "error": "运行出错",
    "interrupted": "任务中断",
    "stalled": "疑似卡滞",
}

_FENCED_JSON_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE)

_RUNTIME_WATCH_KEYS: tuple[str, ...] = (
    "title",
    "last_status",
    "last_status_detail",
    "last_updated_at_ms",
    "last_notified_event",
    "last_notified_at",
    "last_notified_by_event",
    "last_suppressed_by_event",
    "last_stalled_updated_at_ms",
    "pending_transition",
    "last_verification",
    "last_delivery_error",
)

logger = logging.getLogger("agent_hub.watcher")


def _resolve_watches_path(path: Path | None = None) -> Path:
    if path is not None:
        return path
    env_path = os.environ.get("AGENT_HUB_WATCHES_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_WATCHES_PATH


def parse_notify_targets(raw: str | list[str] | None) -> list[str]:
    """Normalize target string/list (defaulting to ['qqbot'])."""
    if isinstance(raw, list):
        items = [str(x).strip() for x in raw if str(x).strip()]
        return items or ["qqbot"]
    txt = str(raw or "").strip()
    if not txt:
        return ["qqbot"]
    items = [part.strip() for part in txt.split(",") if part.strip()]
    return items or ["qqbot"]


def parse_notify_events(raw: str | list[str] | None) -> list[str]:
    """Normalize notify_on event list ('all' expands to ALL_NOTIFY_EVENTS)."""
    if isinstance(raw, list):
        vals = [str(x).strip().lower() for x in raw if str(x).strip()]
    else:
        txt = str(raw or "all").strip().lower()
        if txt in ("", "all", "*"):
            return list(ALL_NOTIFY_EVENTS)
        vals = [p.strip().lower() for p in txt.split(",") if p.strip()]
    valid = [v for v in vals if v in ALL_NOTIFY_EVENTS]
    return valid or list(ALL_NOTIFY_EVENTS)


def parse_verify_events(raw: str | list[str] | None) -> list[str]:
    """Normalize verify_events list (defaults to DEFAULT_VERIFY_EVENTS)."""
    if raw is None:
        return list(DEFAULT_VERIFY_EVENTS)
    if isinstance(raw, list):
        vals = [str(x).strip().lower() for x in raw if str(x).strip()]
        if not vals:
            return list(DEFAULT_VERIFY_EVENTS)
    else:
        txt = str(raw).strip().lower()
        if txt in ("", "default"):
            return list(DEFAULT_VERIFY_EVENTS)
        if txt in ("all", "*"):
            return list(ALL_NOTIFY_EVENTS)
        if txt in ("none", "off"):
            return []
        vals = [p.strip().lower() for p in txt.split(",") if p.strip()]
    return [v for v in vals if v in ALL_NOTIFY_EVENTS]


class WatchStore:
    """Process-safe flock-protected atomic JSON store for persistent zero-token session watches."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = _resolve_watches_path(path)
        self._lock_path = self.path.parent / f".{self.path.name}.lock"

    @contextmanager
    def _file_lock(self, *, exclusive: bool = True) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        with self._lock_path.open("a+", encoding="utf-8") as lock_fh:
            try:
                os.chmod(self._lock_path, 0o600)
            except OSError:
                pass
            fcntl.flock(lock_fh.fileno(), mode)
            try:
                yield
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def _read_unlocked(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if isinstance(data, dict) and isinstance(data.get("watches"), list):
            return [dict(w) for w in data["watches"] if isinstance(w, dict)]
        return []

    def _write_unlocked(self, watches: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        payload = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "watches": watches,
        }
        fd, tmp_name = tempfile.mkstemp(prefix=".watches-", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
            os.chmod(tmp_name, 0o600)
            os.replace(tmp_name, self.path)
        finally:
            if os.path.exists(tmp_name):
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    def list_all(self) -> list[dict[str, Any]]:
        with self._file_lock(exclusive=False):
            return self._read_unlocked()

    def save_all(self, watches: list[dict[str, Any]]) -> None:
        with self._file_lock(exclusive=True):
            self._write_unlocked(watches)

    def upsert(self, watch: dict[str, Any]) -> dict[str, Any]:
        """Insert or update watch keyed uniquely by (app, session_id)."""
        with self._file_lock(exclusive=True):
            watches = self._read_unlocked()
            app = str(watch["app"])
            sid = str(watch["session_id"])
            for idx, existing in enumerate(watches):
                if existing.get("app") == app and existing.get("session_id") == sid:
                    merged = dict(existing)
                    merged.update(watch)
                    if "pending_transition" in watch and watch["pending_transition"] is None:
                        merged.pop("pending_transition", None)
                    merged["watch_id"] = existing.get("watch_id") or merged.get("watch_id") or f"w_{uuid.uuid4().hex[:8]}"
                    watches[idx] = merged
                    self._write_unlocked(watches)
                    return merged

            new_watch = {k: v for k, v in watch.items() if not (k == "pending_transition" and v is None)}
            new_watch.setdefault("watch_id", f"w_{uuid.uuid4().hex[:8]}")
            new_watch.setdefault(
                "created_at",
                datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            )
            watches.append(new_watch)
            self._write_unlocked(watches)
            return new_watch

    def remove(self, key: str) -> dict[str, Any] | None:
        """Remove a watch by watch_id, alias (case-insensitive), or session_id."""
        target = (key or "").strip()
        if not target:
            return None
        t_low = target.lower()
        with self._file_lock(exclusive=True):
            watches = self._read_unlocked()
            for idx, w in enumerate(watches):
                if (
                    str(w.get("watch_id", "")).lower() == t_low
                    or str(w.get("alias", "")).strip().lower() == t_low
                    or str(w.get("session_id", "")) == target
                ):
                    removed = watches.pop(idx)
                    self._write_unlocked(watches)
                    return removed
            return None

    def update_entries(
        self,
        updates_by_id: dict[str, dict[str, Any]],
        *,
        clear_keys_by_id: dict[str, set[str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Incrementally merge runtime watch updates by watch_id under exclusive flock.

        Preserves concurrent watch_add / watch_remove mutations performed while a slow
        poll cycle (such as Level-2 agent verification) was in flight.
        """
        if not updates_by_id and not clear_keys_by_id:
            return self.list_all()
        clears = clear_keys_by_id or {}
        with self._file_lock(exclusive=True):
            current = self._read_unlocked()
            changed = False
            for idx, existing in enumerate(current):
                wid = str(existing.get("watch_id") or "")
                if not wid:
                    continue
                patch = updates_by_id.get(wid)
                rm_keys = clears.get(wid, set())
                if patch is None and not rm_keys:
                    continue
                merged = dict(existing)
                for rk in rm_keys:
                    if rk in merged:
                        merged.pop(rk, None)
                        changed = True
                if patch:
                    for key, val in patch.items():
                        if key in ("last_notified_by_event", "last_suppressed_by_event") and isinstance(val, dict):
                            cur_map = merged.get(key)
                            new_map = dict(cur_map) if isinstance(cur_map, dict) else {}
                            for ev, ts in val.items():
                                try:
                                    prev_ts = float(new_map.get(ev) or 0)
                                    cur_ts = float(ts or 0)
                                    if cur_ts >= prev_ts:
                                        new_map[ev] = ts
                                except (TypeError, ValueError):
                                    new_map[ev] = ts
                            if new_map != merged.get(key):
                                merged[key] = new_map
                                changed = True
                        else:
                            if merged.get(key) != val:
                                merged[key] = val
                                changed = True
                current[idx] = merged
            if changed:
                self._write_unlocked(current)
            return current


def evaluate_watch(
    watch: dict[str, Any],
    live_meta: SessionSummary,
    *,
    now_ts: float | None = None,
) -> tuple[str | None, str]:
    """Detect whether a watched session crossed a notifiable transition (per-event cooldown)."""
    now = time.time() if now_ts is None else float(now_ts)
    prev_status = str(watch.get("last_status") or STATUS_IDLE)
    cur_status = live_meta.status
    notify_on = set(parse_notify_events(watch.get("notify_on")))
    if watch.get("cooldown_minutes") is not None:
        cooldown_min = max(0, int(watch["cooldown_minutes"]))
    else:
        cooldown_min = max(1, int(watch.get("stall_minutes") or 15))
    cooldown_s = cooldown_min * 60

    event_type: str | None = None
    reason = ""

    if prev_status == STATUS_RUNNING and cur_status == STATUS_IDLE:
        event_type = "completed"
        reason = "running -> idle"
    elif cur_status == STATUS_WAITING_APPROVAL and prev_status != STATUS_WAITING_APPROVAL:
        event_type = "waiting_approval"
        reason = live_meta.status_detail or "awaiting approval"
    elif cur_status == STATUS_WAITING_USER and prev_status != STATUS_WAITING_USER:
        event_type = "waiting_user"
        reason = live_meta.status_detail or "awaiting user reply"
    elif cur_status == STATUS_ERROR and prev_status != STATUS_ERROR:
        event_type = "error"
        reason = live_meta.status_detail or "session entered error state"
    elif cur_status == STATUS_INTERRUPTED and prev_status != STATUS_INTERRUPTED:
        event_type = "interrupted"
        reason = live_meta.status_detail or "session interrupted"
    elif cur_status == STATUS_RUNNING and live_meta.updated_at_ms > 0:
        stall_min = max(1, int(watch.get("stall_minutes") or 15))
        idle_ms = int(now * 1000) - int(live_meta.updated_at_ms)
        if idle_ms >= stall_min * 60_000:
            sent_by_ev = watch.get("last_notified_by_event")
            last_stalled_notif = (
                float(sent_by_ev.get("stalled") or 0)
                if isinstance(sent_by_ev, dict)
                else (
                    float(watch.get("last_notified_at") or 0)
                    if str(watch.get("last_notified_event") or "") == "stalled"
                    else 0.0
                )
            )
            last_stalled_ts = int(watch.get("last_stalled_updated_at_ms") or 0)
            already_alerted = (
                last_stalled_ts == int(live_meta.updated_at_ms)
                and last_stalled_notif > 0
                and (now - last_stalled_notif) < cooldown_s
            )
            if not already_alerted:
                event_type = "stalled"
                mins = int(idle_ms // 60_000)
                reason = f"no activity for {mins}m (threshold {stall_min}m)"

    if event_type is None or event_type not in notify_on:
        return None, ""

    # Cooldown is strictly isolated by event_type so high-priority waiting_approval /
    # waiting_user / error alerts are never silenced by earlier completed / stalled notices.
    if cooldown_s > 0:
        sent_by_event = watch.get("last_notified_by_event")
        last_same_at = 0.0
        if isinstance(sent_by_event, dict) and sent_by_event.get(event_type):
            last_same_at = float(sent_by_event.get(event_type) or 0)
        elif str(watch.get("last_notified_event") or "") == event_type:
            last_same_at = float(watch.get("last_notified_at") or 0)

        if last_same_at > 0 and (now - last_same_at) < cooldown_s:
            return None, ""

        sup_by_event = watch.get("last_suppressed_by_event")
        if isinstance(sup_by_event, dict):
            last_sup_at = float(sup_by_event.get(event_type) or 0)
            if last_sup_at > 0 and (now - last_sup_at) < cooldown_s:
                return None, ""

    return event_type, reason


def format_watch_notification(
    watch: dict[str, Any],
    event_type: str,
    live_meta: SessionSummary,
    *,
    reason: str = "",
    snippet: str = "",
    label_override: str = "",
    verdict: dict[str, Any] | None = None,
    completion_summary: dict[str, Any] | None = None,
) -> str:
    """Format a compact Chinese notification ready for QQ/Weixin push."""
    label = (label_override or "").strip()
    if not label and isinstance(verdict, dict) and verdict.get("ok"):
        label = str(verdict.get("label") or "").strip()
    if not label and event_type in ("completed", "stalled") and snippet:
        bg = detect_background_job(snippet)
        if bg["matched"]:
            label = str(bg["label"])
    if not label:
        label = _EVENT_LABELS.get(event_type, event_type)

    if event_type == "completed" and completion_summary is not None:
        return format_completion_card(
            completion_summary,
            watch=watch,
            live_meta=live_meta,
            label=label,
            reason=reason,
            verdict=verdict,
        )

    prev_status = str(watch.get("last_status") or STATUS_IDLE)
    alias = str(watch.get("alias") or "").strip()
    display_name = f"{alias} ({live_meta.title})" if alias and alias != live_meta.title else live_meta.title
    detail = live_meta.status_detail or reason
    detail_suffix = f" ({detail})" if detail else ""
    lines = [
        f"[Agent Hub 盯盘] {label} | {live_meta.app}「{display_name}」",
        f"状态: {prev_status} -> {live_meta.status}{detail_suffix}",
    ]
    verdict_summary = (
        str(verdict.get("summary") or "").strip()
        if isinstance(verdict, dict) and verdict.get("ok")
        else ""
    )
    if verdict_summary:
        lines.append(f"研判结论: {verdict_summary}")
    elif snippet:
        lines.append(f"最新进展: {snippet}")
    return "\n".join(lines)


def build_verification_prompt(ctx: dict[str, Any]) -> str:
    """Build a compact fact-check prompt for on-demand Level-2 session verification."""
    watch = ctx.get("watch") or {}
    live_meta = ctx.get("live_meta")
    if isinstance(live_meta, dict):
        app = str(live_meta.get("app") or watch.get("app") or "")
        sid = str(live_meta.get("session_id") or watch.get("session_id") or "")
        title = str(live_meta.get("title") or watch.get("title") or "")
        workspace = str(live_meta.get("workspace") or "")
        cur_status = str(live_meta.get("status") or "")
        detail = str(live_meta.get("status_detail") or "")
    else:
        app = str(getattr(live_meta, "app", "") or watch.get("app") or "")
        sid = str(getattr(live_meta, "session_id", "") or watch.get("session_id") or "")
        title = str(getattr(live_meta, "title", "") or watch.get("title") or "")
        workspace = str(getattr(live_meta, "workspace", "") or "")
        cur_status = str(getattr(live_meta, "status", "") or "")
        detail = str(getattr(live_meta, "status_detail", "") or "")

    alias = str(watch.get("alias") or "")
    prev_status = str(watch.get("last_status") or STATUS_IDLE)
    event_type = str(ctx.get("event_type") or "")
    reason = str(ctx.get("reason") or "")
    snippet = str(ctx.get("snippet") or "")
    bg = ctx.get("background_job") or {}
    context_messages = ctx.get("context_messages") or []

    lines = [
        "你是 Agent Hub 盯盘系统的 Level-2 事实核验员。请仅做只读事实研判，严禁修改任何文件，严禁向会话或聊天软件发送消息。",
        f"目标会话: app={app}, session_id={sid}, alias={alias or '-'}, title={title}",
        f"工作区: {workspace or '-'}",
        f"初筛事件: {event_type} ({prev_status} -> {cur_status}; detail={detail or '-'}; reason={reason or '-'})",
    ]
    if bg.get("matched"):
        lines.append(
            f"规则初筛命中后台作业信号: kind={bg.get('kind')}, job_ids={bg.get('job_ids') or []}, signals={bg.get('signals') or []}"
        )
    if snippet:
        lines.append(f"最新摘要: {snippet}")

    if context_messages:
        lines.append("最近消息片段（由旧到新）：")
        for msg in context_messages[-3:]:
            if not isinstance(msg, dict):
                continue
            role = str(msg.get("role") or "unknown")
            body, _ = truncate_text(str(msg.get("content") or "").strip(), 900)
            if body:
                lines.append(f"[{role}] {body}")

    lines.extend(
        [
            "",
            "注意：如果只是单个命令或工具返回非零退出码/报错（如 HTTP 400、脚本报错、除零等），而模型正在自主排错、重试或继续推进而非整个会话崩溃，请务必将 classification 设为 \"transient_error\" 且 needs_attention 设为 false。",
            "请判断该会话当前真实所处阶段，并严格只输出一个 JSON 对象（不要输出多余解释）：",
            '{"classification":"completed|background_running|still_running|transient_error|needs_user|error","needs_attention":true/false,"label":"简短中文标签，如【阶段汇报·后台作业运行中】或【回合完毕 / 等待输入】","summary":"1-2句高置信度中文结论，说明真实进度、作业ID或是否需要人工介入"}',
        ]
    )
    return "\n".join(lines)


def parse_verification_output(raw_text: str) -> dict[str, Any]:
    """Parse Level-2 verifier output (raw JSON, fenced JSON, or plain-text fallback)."""
    text = str(raw_text or "").strip()
    if not text:
        return {"ok": False, "error": "empty verifier output"}

    parsed: dict[str, Any] | None = None
    candidates: list[str] = [text]
    fenced = _FENCED_JSON_RE.search(text)
    if fenced:
        candidates.append(fenced.group(1).strip())
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        candidates.append(text[start : end + 1])

    for cand in candidates:
        try:
            obj = json.loads(cand)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            parsed = obj
            break

    if parsed is not None:
        classification = str(parsed.get("classification") or "unknown").strip().lower() or "unknown"
        if classification in _TRANSIENT_CLASSIFICATIONS:
            classification = "transient_error"
        raw_needs = parsed.get("needs_attention")
        if isinstance(raw_needs, bool):
            needs_attention = raw_needs
        elif isinstance(raw_needs, str) and raw_needs.strip().lower() in ("true", "false"):
            needs_attention = raw_needs.strip().lower() == "true"
        else:
            needs_attention = classification not in ("background_running", "still_running", "transient_error")

        label = clean_single_line(str(parsed.get("label") or ""), 40)
        summary = clean_single_line(str(parsed.get("summary") or ""), 260)
        if not summary and label:
            summary = label
        return {
            "ok": True,
            "classification": classification,
            "needs_attention": needs_attention,
            "label": label,
            "summary": summary,
        }

    return {
        "ok": True,
        "classification": "unknown",
        "needs_attention": True,
        "label": "",
        "summary": clean_single_line(text, 240),
    }


def default_hermes_verify(
    ctx: dict[str, Any],
    *,
    model: str = "",
    reasoning: str = "low",
    toolsets: str = "terminal",
    provider: str = "",
    timeout: int = 90,
    hermes_bin: str = "",
) -> dict[str, Any]:
    """Run an on-demand single-turn `hermes -z` verification pass; fail-open on errors."""
    bin_path = (hermes_bin or "").strip() or (
        DEFAULT_HERMES_BIN if Path(DEFAULT_HERMES_BIN).exists() else "hermes"
    )
    prompt = build_verification_prompt(ctx)
    cmd: list[str] = [bin_path]
    if model.strip():
        cmd.extend(["-m", model.strip()])
    if provider.strip():
        cmd.extend(["--provider", provider.strip()])
    if reasoning.strip():
        cmd.extend(["--reasoning", reasoning.strip()])
    if toolsets.strip():
        cmd.extend(["-t", toolsets.strip()])
    cmd.extend(["-z", prompt])

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(5, int(timeout)),
        )
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc), "source": "hermes-z"}

    if proc.returncode != 0:
        err = clean_single_line(proc.stderr or proc.stdout or f"exit {proc.returncode}", 200)
        return {"ok": False, "error": err, "source": "hermes-z"}

    verdict = parse_verification_output(proc.stdout or "")
    verdict["source"] = "hermes-z"
    return verdict


def default_hermes_send(target: str, message: str) -> tuple[bool, str]:
    """Deliver notification via local Hermes CLI without waking any LLM."""
    hermes_bin = DEFAULT_HERMES_BIN if Path(DEFAULT_HERMES_BIN).exists() else "hermes"
    try:
        proc = subprocess.run(
            [hermes_bin, "send", "-t", target, message],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if proc.returncode == 0:
            return True, (proc.stdout or "sent").strip()
        err = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()
        return False, err
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


NotifierFn = Callable[[str, str], tuple[bool, str]]
AgentVerifierFn = Callable[[dict[str, Any]], dict[str, Any]]


class WatchdogRunner:
    """Polls registered watches read-only and dispatches notifications on state transitions."""

    def __init__(
        self,
        adapters: dict[str, AppAdapter] | None = None,
        store: WatchStore | None = None,
        notifier: NotifierFn | None = None,
        *,
        verifier: AgentVerifierFn | None = None,
        verify_agent: bool = False,
        verify_events: tuple[str, ...] | list[str] | None = DEFAULT_VERIFY_EVENTS,
        suppress_routine: bool = False,
        verify_model: str = "",
        verify_reasoning: str = "low",
        verify_toolsets: str = "terminal",
        verify_provider: str = "",
        verify_timeout: int = 90,
        debounce_seconds: int = 0,
    ) -> None:
        self.adapters: dict[str, AppAdapter] = adapters or {
            "codex": CodexAdapter(),
            "dsh": DshAdapter(),
            "antigravity": AntigravityAdapter(),
            "cline": ClineAdapter(),
        }
        self.store = store or WatchStore()
        self.notifier = notifier or default_hermes_send
        self.verify_agent = bool(verify_agent)
        if verify_events is None:
            self.verify_events: tuple[str, ...] = DEFAULT_VERIFY_EVENTS
        else:
            self.verify_events = tuple(v for v in verify_events if v in ALL_NOTIFY_EVENTS)
        self.suppress_routine = bool(suppress_routine)
        self.verify_model = (verify_model or "").strip()
        self.verify_reasoning = (verify_reasoning or "low").strip()
        self.verify_toolsets = (verify_toolsets or "").strip()
        self.verify_provider = (verify_provider or "").strip()
        self.verify_timeout = max(5, int(verify_timeout))
        self.debounce_seconds = max(0, int(debounce_seconds))
        if verifier is not None:
            self.verifier: AgentVerifierFn = verifier
        else:
            self.verifier = lambda ctx: default_hermes_verify(
                ctx,
                model=self.verify_model,
                reasoning=self.verify_reasoning,
                toolsets=self.verify_toolsets,
                provider=self.verify_provider,
                timeout=self.verify_timeout,
            )

    def _recent_context(self, adapter: AppAdapter, session_id: str) -> tuple[str, list[dict[str, Any]]]:
        try:
            rd = adapter.read_session(
                session_id,
                tail_messages=10,
                max_chars_per_message=2400,
                include_tools=True,
            )
            msgs = [m for m in (rd.get("messages") or []) if isinstance(m, dict)]
            if msgs:
                non_tool = [m for m in msgs if m.get("role") != "tool"]
                last = non_tool[-1] if non_tool else msgs[-1]
                snippet = clean_single_line(f"{last.get('role', '')}: {last.get('content', '')}", 160)
                return snippet, msgs
        except Exception:  # noqa: BLE001
            pass
        return "", []

    def _latest_snippet(self, adapter: AppAdapter, session_id: str) -> str:
        snippet, _ = self._recent_context(adapter, session_id)
        return snippet

    @staticmethod
    def _build_fallback_targets(watch: dict[str, Any], primary_targets: list[str]) -> list[str]:
        tried = set(primary_targets)
        candidates: list[str] = []
        raw_explicit = watch.get("fallback_targets")
        if raw_explicit:
            candidates.extend(parse_notify_targets(raw_explicit))
        for tgt in primary_targets:
            mapped = DEFAULT_FALLBACK_CHANNELS.get(tgt)
            if mapped:
                candidates.append(mapped)
        deduped: list[str] = []
        for fb in candidates:
            if fb and fb not in tried and fb not in deduped:
                deduped.append(fb)
        return deduped

    def _deliver_with_fallback(
        self,
        watch: dict[str, Any],
        app: str,
        sid: str,
        event_type: str,
        text: str,
        targets: list[str],
    ) -> tuple[bool, list[dict[str, Any]]]:
        deliveries: list[dict[str, Any]] = []
        for tgt in targets:
            try:
                ok, info = self.notifier(tgt, text)
            except Exception as exc:  # noqa: BLE001
                ok, info = False, str(exc)
            deliveries.append({"target": tgt, "ok": bool(ok), "detail": info, "fallback": False})
            if not ok:
                logger.warning("Watch delivery failed for %s:%s (%s) via %s: %s", app, sid, event_type, tgt, info)

        any_delivered = any(d["ok"] for d in deliveries)
        if not any_delivered:
            for fb in self._build_fallback_targets(watch, targets):
                logger.warning(
                    "Attempting fallback delivery for %s:%s (%s) via '%s' after primary failure",
                    app,
                    sid,
                    event_type,
                    fb,
                )
                try:
                    fb_ok, fb_info = self.notifier(fb, text)
                except Exception as exc:  # noqa: BLE001
                    fb_ok, fb_info = False, str(exc)
                deliveries.append({"target": fb, "ok": bool(fb_ok), "detail": fb_info, "fallback": True})
                if fb_ok:
                    any_delivered = True
                    break
                logger.warning(
                    "Fallback watch delivery failed for %s:%s (%s) via %s: %s",
                    app,
                    sid,
                    event_type,
                    fb,
                    fb_info,
                )
        return any_delivered, deliveries

    def poll_once(self, *, dry_run: bool = False, now_ts: float | None = None) -> dict[str, Any]:
        """Run one poll cycle across all enabled watches."""
        now = time.time() if now_ts is None else float(now_ts)
        watches = self.store.list_all()
        checked = 0
        events: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        runtime_updates: dict[str, dict[str, Any]] = {}
        clear_keys_by_id: dict[str, set[str]] = {}
        fallback_unkeyed_dirty = False

        def _stage_update(w_obj: dict[str, Any], key: str, value: Any) -> None:
            nonlocal fallback_unkeyed_dirty
            w_obj[key] = value
            wid = str(w_obj.get("watch_id") or "")
            if wid:
                runtime_updates.setdefault(wid, {})[key] = value
                clear_keys_by_id.get(wid, set()).discard(key)
            else:
                fallback_unkeyed_dirty = True

        def _stage_clear(w_obj: dict[str, Any], key: str) -> None:
            nonlocal fallback_unkeyed_dirty
            had = key in w_obj
            w_obj.pop(key, None)
            wid = str(w_obj.get("watch_id") or "")
            if wid:
                clear_keys_by_id.setdefault(wid, set()).add(key)
                if wid in runtime_updates:
                    runtime_updates[wid].pop(key, None)
            elif had:
                fallback_unkeyed_dirty = True

        for w in watches:
            if not w.get("enabled", True):
                continue
            checked += 1
            app = str(w.get("app", ""))
            sid = str(w.get("session_id", ""))
            adapter = self.adapters.get(app)
            if adapter is None:
                errors.append({"watch_id": w.get("watch_id"), "error": f"Unknown app '{app}'"})
                continue

            try:
                meta = adapter.get_session_meta(sid)
            except Exception as exc:  # noqa: BLE001
                errors.append({"watch_id": w.get("watch_id"), "error": str(exc)})
                continue

            if meta is None:
                errors.append({"watch_id": w.get("watch_id"), "error": f"Session '{sid}' not found in '{app}'"})
                continue

            event_type, reason = evaluate_watch(w, meta, now_ts=now)
            eff_debounce = (
                max(0, int(w["debounce_seconds"]))
                if w.get("debounce_seconds") is not None
                else self.debounce_seconds
            )
            if event_type is None:
                if w.get("pending_transition") and not dry_run:
                    _stage_clear(w, "pending_transition")
            elif event_type in DEBOUNCE_EVENTS and eff_debounce > 0:
                pending = w.get("pending_transition")
                if (
                    isinstance(pending, dict)
                    and pending.get("event") == event_type
                    and pending.get("status") == meta.status
                ):
                    first_seen = float(pending.get("first_seen_at") or now)
                    if (now - first_seen) < eff_debounce:
                        if not dry_run:
                            new_pending = dict(pending)
                            new_pending["count"] = int(pending.get("count") or 1) + 1
                            _stage_update(w, "pending_transition", new_pending)
                        continue
                    if not dry_run:
                        _stage_clear(w, "pending_transition")
                else:
                    if not dry_run:
                        _stage_update(
                            w,
                            "pending_transition",
                            {
                                "event": event_type,
                                "status": meta.status,
                                "first_seen_at": now,
                                "count": 1,
                            },
                        )
                    continue
            elif w.get("pending_transition") and not dry_run:
                _stage_clear(w, "pending_transition")

            delivery_blocked = False
            if event_type is not None:
                snippet, context_messages = self._recent_context(adapter, sid)
                combined_parts = [meta.preview or "", snippet] + [
                    str(m.get("content") or "") for m in context_messages
                ]
                bg_info = detect_background_job("\n".join(p for p in combined_parts if p))
                if bg_info["matched"] and event_type in ("completed", "stalled"):
                    effective_label = str(bg_info["label"])
                else:
                    effective_label = _EVENT_LABELS.get(event_type, event_type)

                watch_verify = w.get("verify_agent")
                use_verify = bool(watch_verify) if watch_verify is not None else self.verify_agent
                watch_verify_events = (
                    tuple(parse_verify_events(w.get("verify_events")))
                    if w.get("verify_events")
                    else self.verify_events
                )

                verdict: dict[str, Any] | None = None
                suppressed = False
                if use_verify and event_type in watch_verify_events:
                    ctx = {
                        "watch": dict(w),
                        "event_type": event_type,
                        "live_meta": meta,
                        "reason": reason,
                        "snippet": snippet,
                        "context_messages": context_messages,
                        "background_job": bg_info,
                    }
                    try:
                        verdict = self.verifier(ctx)
                        if not isinstance(verdict, dict):
                            verdict = {"ok": False, "error": "invalid verifier return type"}
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Level-2 verifier failed for %s:%s (%s): %s", app, sid, event_type, exc)
                        verdict = {"ok": False, "error": str(exc)}

                    if verdict.get("ok"):
                        if str(verdict.get("label") or "").strip():
                            effective_label = str(verdict["label"]).strip()
                        watch_suppress = w.get("suppress_routine")
                        use_suppress = bool(watch_suppress) if watch_suppress is not None else self.suppress_routine
                        if verdict.get("classification") == "transient_error":
                            suppressed = True
                        elif use_suppress and verdict.get("needs_attention") is False:
                            suppressed = True

                completion_summary: dict[str, Any] | None = None
                render_summary_card = False
                if event_type == "completed":
                    completion_summary = extract_task_completion_summary(
                        session_meta=meta,
                        messages=context_messages,
                    )
                    verdict_cls = str(verdict.get("classification") or "") if isinstance(verdict, dict) and verdict.get("ok") else ""
                    if verdict_cls not in ("background_running", "still_running"):
                        render_summary_card = True

                text = format_watch_notification(
                    w,
                    event_type,
                    meta,
                    reason=reason,
                    snippet=snippet,
                    label_override=effective_label,
                    verdict=verdict,
                    completion_summary=completion_summary if render_summary_card else None,
                )
                targets = parse_notify_targets(w.get("notify_targets"))
                deliveries: list[dict[str, Any]] = []
                any_delivered = False

                if not dry_run:
                    if not suppressed:
                        any_delivered, deliveries = self._deliver_with_fallback(
                            w, app, sid, event_type, text, targets
                        )
                        if any_delivered:
                            _stage_update(w, "last_notified_event", event_type)
                            _stage_update(w, "last_notified_at", now)
                            sent_map = dict(w.get("last_notified_by_event")) if isinstance(w.get("last_notified_by_event"), dict) else {}
                            sent_map[event_type] = now
                            _stage_update(w, "last_notified_by_event", sent_map)
                            if event_type == "stalled":
                                _stage_update(w, "last_stalled_updated_at_ms", meta.updated_at_ms)
                            if "last_delivery_error" in w:
                                _stage_clear(w, "last_delivery_error")
                        else:
                            # Hold transition open so the next cycle retries instead of dropping alerts.
                            delivery_blocked = True
                            logger.error(
                                "All notification channels failed for %s:%s (%s); preserving last_status='%s' for retry: %s",
                                app,
                                sid,
                                event_type,
                                w.get("last_status"),
                                deliveries,
                            )
                            _stage_update(
                                w,
                                "last_delivery_error",
                                {
                                    "at": datetime.fromtimestamp(now, timezone.utc).astimezone().isoformat(timespec="seconds"),
                                    "event": event_type,
                                    "deliveries": deliveries,
                                },
                            )
                    else:
                        logger.info(
                            "Suppressed %s alert for %s:%s (%s)",
                            event_type,
                            app,
                            sid,
                            verdict.get("summary", "") if isinstance(verdict, dict) else "",
                        )
                        sup_map = dict(w.get("last_suppressed_by_event")) if isinstance(w.get("last_suppressed_by_event"), dict) else {}
                        sup_map[event_type] = now
                        _stage_update(w, "last_suppressed_by_event", sup_map)
                        if event_type == "stalled":
                            _stage_update(w, "last_stalled_updated_at_ms", meta.updated_at_ms)

                    if verdict is not None:
                        _stage_update(
                            w,
                            "last_verification",
                            {
                                "at": datetime.fromtimestamp(now, timezone.utc).astimezone().isoformat(timespec="seconds"),
                                "event": event_type,
                                "ok": bool(verdict.get("ok")),
                                "classification": verdict.get("classification", ""),
                                "needs_attention": verdict.get("needs_attention"),
                                "suppressed": suppressed,
                                "summary": verdict.get("summary", ""),
                                **({"error": verdict["error"]} if verdict.get("error") else {}),
                            },
                        )

                ev_entry: dict[str, Any] = {
                    "watch_id": w.get("watch_id"),
                    "alias": w.get("alias", ""),
                    "app": app,
                    "session_id": sid,
                    "title": meta.title,
                    "event": event_type,
                    "label": effective_label,
                    "reason": reason,
                    "targets": targets,
                    "dry_run": dry_run,
                    "suppressed": suppressed,
                    "delivered": any_delivered if not dry_run and not suppressed else None,
                    "background_job": bg_info,
                    "message": text,
                    "deliveries": deliveries,
                }
                if completion_summary is not None:
                    ev_entry["completion_summary"] = completion_summary
                if verdict is not None:
                    ev_entry["verification"] = verdict
                events.append(ev_entry)

            if not dry_run and not delivery_blocked:
                if w.get("last_status") != meta.status:
                    _stage_update(w, "last_status", meta.status)
                if w.get("last_status_detail") != meta.status_detail:
                    _stage_update(w, "last_status_detail", meta.status_detail)
                if w.get("last_updated_at_ms") != meta.updated_at_ms:
                    _stage_update(w, "last_updated_at_ms", meta.updated_at_ms)
                if w.get("title") != meta.title:
                    _stage_update(w, "title", meta.title)

        if not dry_run:
            if fallback_unkeyed_dirty:
                self.store.save_all(watches)
            elif runtime_updates or clear_keys_by_id:
                self.store.update_entries(runtime_updates, clear_keys_by_id=clear_keys_by_id)

        return {
            "checked_count": checked,
            "event_count": len(events),
            "events": events,
            "errors": errors,
        }


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )
    env_verify = os.environ.get("AGENT_HUB_WATCHER_VERIFY_AGENT", "").strip().lower() in ("1", "true", "yes", "on")
    env_suppress = os.environ.get("AGENT_HUB_WATCHER_SUPPRESS_ROUTINE", "").strip().lower() in ("1", "true", "yes", "on")
    ap = argparse.ArgumentParser(description="Zero-token Agent Hub session watchdog daemon")
    ap.add_argument("--interval", type=int, default=25, help="Poll interval in seconds (default 25)")
    ap.add_argument("--once", action="store_true", help="Run one poll cycle and exit")
    ap.add_argument("--dry-run", action="store_true", help="Evaluate watches without sending or mutating state")
    ap.add_argument(
        "--verify-agent",
        action="store_true",
        default=env_verify,
        help="Enable Level-2 on-demand `hermes -z` verification on key state transitions",
    )
    ap.add_argument(
        "--verify-events",
        default=os.environ.get("AGENT_HUB_WATCHER_VERIFY_EVENTS", ",".join(DEFAULT_VERIFY_EVENTS)),
        help="Comma-separated events that trigger Level-2 verification (default: completed,stalled,error)",
    )
    ap.add_argument(
        "--verify-model",
        default=os.environ.get("AGENT_HUB_WATCHER_VERIFY_MODEL", ""),
        help="Optional model override passed to `hermes -m ... -z` during Level-2 verification",
    )
    ap.add_argument(
        "--verify-provider",
        default=os.environ.get("AGENT_HUB_WATCHER_VERIFY_PROVIDER", ""),
        help="Optional provider override passed to `hermes --provider ... -z` during Level-2 verification",
    )
    ap.add_argument(
        "--verify-toolsets",
        default=os.environ.get("AGENT_HUB_WATCHER_VERIFY_TOOLSETS", "terminal"),
        help="Toolset filter for the Level-2 `hermes -z` call (default: terminal; keeps MCP servers out)",
    )
    ap.add_argument(
        "--verify-reasoning",
        default=os.environ.get("AGENT_HUB_WATCHER_VERIFY_REASONING", "low"),
        help="Reasoning level for Level-2 `hermes -z` verification (default: low)",
    )
    ap.add_argument(
        "--verify-timeout",
        type=int,
        default=int(os.environ.get("AGENT_HUB_WATCHER_VERIFY_TIMEOUT", "90") or 90),
        help="Timeout in seconds for Level-2 `hermes -z` verification (default: 90)",
    )
    ap.add_argument(
        "--suppress-routine",
        action="store_true",
        default=env_suppress,
        help="Suppress push notifications when Level-2 verifier reports needs_attention=false",
    )
    ap.add_argument(
        "--debounce-seconds",
        type=int,
        default=int(os.environ.get("AGENT_HUB_WATCHER_DEBOUNCE_SECONDS", str(DEFAULT_DAEMON_DEBOUNCE_SECONDS)) or DEFAULT_DAEMON_DEBOUNCE_SECONDS),
        help="Seconds a transient error/completed state must persist across consecutive polls (default: 25)",
    )
    ap.add_argument(
        "--no-relay",
        action="store_true",
        help="Disable centralized bi-directional relay broker ticks inside the watcher loop",
    )
    args = ap.parse_args(argv)

    runner = WatchdogRunner(
        verify_agent=bool(args.verify_agent),
        verify_events=parse_verify_events(args.verify_events),
        suppress_routine=bool(args.suppress_routine),
        verify_model=args.verify_model,
        verify_reasoning=args.verify_reasoning,
        verify_toolsets=args.verify_toolsets,
        verify_provider=args.verify_provider,
        verify_timeout=args.verify_timeout,
        debounce_seconds=args.debounce_seconds,
    )
    relay_broker = None
    if not args.no_relay:
        from .relay import RelayBroker
        from .server import HubService

        relay_service = HubService(adapters=runner.adapters, watches=runner.store)
        relay_broker = RelayBroker(relay_service, notifier=runner.notifier)

    if args.once:
        res = runner.poll_once(dry_run=args.dry_run)
        if relay_broker is not None:
            res["relay"] = relay_broker.poll_once(dry_run=args.dry_run)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    stop_requested = False

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        logger.info("Received signal %s; shutting down watcher...", signum)
        stop_requested = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    interval = max(5, int(args.interval))
    logger.info(
        "Agent Hub watchdog started (interval=%ss, debounce=%ss, verify_agent=%s, suppress_routine=%s, relay=%s, store=%s)",
        interval,
        runner.debounce_seconds,
        runner.verify_agent,
        runner.suppress_routine,
        relay_broker is not None,
        runner.store.path,
    )
    while not stop_requested:
        try:
            res = runner.poll_once(dry_run=args.dry_run)
            if res["events"]:
                logger.info("Triggered %d watch event(s): %s", len(res["events"]), [e["event"] for e in res["events"]])
            if res["errors"]:
                logger.warning("Watch poll errors: %s", res["errors"])
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unexpected watcher cycle failure: %s", exc)

        if relay_broker is not None:
            try:
                rres = relay_broker.poll_once(dry_run=args.dry_run)
                if rres.get("actions"):
                    logger.info(
                        "Relay cycle actions: %s",
                        [(a.get("relay_id"), a.get("direction"), a.get("status")) for a in rres["actions"]],
                    )
                if rres.get("errors"):
                    logger.warning("Relay poll errors: %s", rres["errors"])
            except Exception as exc:  # noqa: BLE001
                logger.exception("Unexpected relay cycle failure: %s", exc)

        # Sleep in short increments for prompt signal exit
        slept = 0
        while not stop_requested and slept < interval:
            time.sleep(min(1, interval - slept))
            slept += 1

    logger.info("Agent Hub watchdog stopped cleanly.")


if __name__ == "__main__":
    main()
