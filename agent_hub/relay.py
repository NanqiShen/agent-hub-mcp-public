"""Centralized bi-directional cross-agent relay store and idempotent broker."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable
import uuid

from .models import STATUS_IDLE, STATUS_RUNNING, clean_single_line, truncate_text
from .summary import extract_task_completion_summary, format_handover_packet
from .watcher import DEFAULT_FALLBACK_CHANNELS, default_hermes_send, parse_notify_targets


DEFAULT_RELAY_DB_PATH = Path.home() / ".hermes" / "agent-hub" / "relay_states.sqlite"

DEFAULT_MILESTONE_PATTERNS: tuple[str, ...] = (
    r"终审",
    r"裁决",
    r"验收通过",
    r"全部\s*PASS",
    r"\bALL\s+PASS\b",
    r"\bAPPROVED\b",
    r"已批准",
    r"驳回",
    r"\bREJECTED\b",
    r"阻断",
    r"\bBLOCKED\b",
    r"\bFAILED\b",
    r"阶段关闭",
    r"结题",
    r"需人工",
    r"人工确认",
)

_RELAY_HEADER_RE = re.compile(r"^\[(?:via\s+Hermes|Agent\s+Hub\s+Relay)[^\]]*\]\s*$", re.IGNORECASE)
_RELAY_ECHO_RE = re.compile(r"^\s*\[(?:via\s+Hermes\s*\|\s*relayed|Agent\s+Hub\s+Relay\b)", re.IGNORECASE)
_CODE_BLOCK_RE = re.compile(r"```[^\n]*\n(.*?)```", re.DOTALL)

logger = logging.getLogger("agent_hub.relay")

NotifierFn = Callable[[str, str], tuple[bool, str]]


def _resolve_relay_db_path(path: Path | None = None) -> Path:
    if path is not None:
        return path
    env_path = os.environ.get("AGENT_HUB_RELAY_DB_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_RELAY_DB_PATH


def _utc_iso(ts: float) -> str:
    if not ts or ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().isoformat(timespec="seconds")
    except (OSError, OverflowError, ValueError):
        return ""


def canonical_message_hash(text: str) -> str:
    """Compute whitespace-tolerant canonical SHA-256 hash, stripping leading relay/Hermes headers."""
    lines = str(text or "").replace("\r\n", "\n").splitlines()
    idx = 0
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped or _RELAY_HEADER_RE.match(stripped):
            idx += 1
            continue
        break
    normalized = "\n".join(line.rstrip() for line in lines[idx:]).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def is_relay_echo(text: str) -> bool:
    """Return True when a message starts with an Agent Hub / Hermes relay attribution header."""
    for line in str(text or "").splitlines():
        if line.strip():
            return bool(_RELAY_ECHO_RE.match(line))
    return False


def match_patterns(text: str, patterns: list[str] | tuple[str, ...]) -> str:
    """Return the first matched keyword/regex snippet in text, or empty string."""
    body = str(text or "")
    if not body.strip() or not patterns:
        return ""
    for pat in patterns:
        p = str(pat or "").strip()
        if not p:
            continue
        try:
            m = re.search(p, body, re.IGNORECASE)
            if m:
                return clean_single_line(m.group(0), 60)
        except re.error:
            if p.lower() in body.lower():
                return clean_single_line(p, 60)
    return ""


def _extract_main_code_block(text: str) -> str:
    blocks = [m.group(1).strip() for m in _CODE_BLOCK_RE.finditer(text or "") if m.group(1).strip()]
    if not blocks:
        return text
    return max(enumerate(blocks), key=lambda pair: (len(pair[1]), pair[0]))[1]


def format_relay_notification(
    link: dict[str, Any],
    *,
    event_kind: str,
    direction: str,
    source_label: str,
    target_label: str,
    matched_signal: str = "",
    snippet: str = "",
    error: str = "",
) -> str:
    """Format a concise Chinese notification card for milestone / circuit-breaker / mismatch events."""
    name = str(link.get("name") or link.get("relay_id") or "relay")
    dir_arrow = f"{source_label} -> {target_label}"
    if event_kind == "milestone_stop":
        header = f"[Agent Hub 中继] 【中继自动暂停·达成终止条件】{name}"
    elif event_kind == "milestone":
        header = f"[Agent Hub 中继] 【中继节点达成】{name}"
    elif event_kind == "max_hops":
        header = f"[Agent Hub 中继] 【中继自动暂停·达到最大轮次】{name}"
    elif event_kind == "session_mismatch":
        header = f"[Agent Hub 中继] 【串台拦截保护·已阻断发送】{name}"
    elif event_kind == "error":
        header = f"[Agent Hub 中继] 【中继投递异常】{name}"
    else:
        header = f"[Agent Hub 中继] 【跨端流转完成】{name}"

    lines = [header, f"流向: {dir_arrow} ({direction})"]
    if matched_signal:
        lines.append(f"命中节点: {matched_signal}")
    if error:
        lines.append(f"阻断原因: {clean_single_line(error, 220)}")
    if snippet:
        lines.append(f"内容摘要: {clean_single_line(snippet, 220)}")
    return "\n".join(lines)


class RelayStore:
    """SQLite WAL-backed store for bi-directional relay links, idempotent transactions, and alert dedupe."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = _resolve_relay_db_path(path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        con = sqlite3.connect(str(self.path), timeout=10.0, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=10000")
        con.execute("PRAGMA synchronous=NORMAL")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
        return con

    def _ensure_schema(self) -> None:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS relay_links (
                    relay_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    source_app TEXT NOT NULL,
                    source_session_id TEXT NOT NULL,
                    source_title TEXT NOT NULL DEFAULT '',
                    target_app TEXT NOT NULL,
                    target_session_id TEXT NOT NULL,
                    target_title TEXT NOT NULL DEFAULT '',
                    bidirectional INTEGER NOT NULL DEFAULT 1,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    policy_json TEXT NOT NULL DEFAULT '{}',
                    state_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS relay_transactions (
                    tx_id TEXT PRIMARY KEY,
                    relay_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    source_app TEXT NOT NULL,
                    source_session_id TEXT NOT NULL,
                    target_app TEXT NOT NULL,
                    target_session_id TEXT NOT NULL,
                    source_msg_index INTEGER,
                    content_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    audit_id TEXT NOT NULL DEFAULT '',
                    verified_session_id TEXT NOT NULL DEFAULT '',
                    milestone_matched TEXT NOT NULL DEFAULT '',
                    notified INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_relay_tx_lookup
                ON relay_transactions (relay_id, direction, content_hash, status)
                """
            )
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_relay_tx_recent
                ON relay_transactions (created_at DESC)
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS relay_notifications (
                    notif_key TEXT PRIMARY KEY,
                    relay_id TEXT NOT NULL,
                    event_kind TEXT NOT NULL,
                    targets_json TEXT NOT NULL DEFAULT '[]',
                    delivered INTEGER NOT NULL DEFAULT 0,
                    detail TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL
                )
                """
            )
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    @staticmethod
    def _row_to_link(row: tuple[Any, ...]) -> dict[str, Any]:
        (
            relay_id,
            name,
            source_app,
            source_sid,
            source_title,
            target_app,
            target_sid,
            target_title,
            bidirectional,
            enabled,
            policy_json,
            state_json,
            created_at,
            updated_at,
        ) = row
        try:
            policy = json.loads(policy_json) if policy_json else {}
        except json.JSONDecodeError:
            policy = {}
        try:
            state = json.loads(state_json) if state_json else {}
        except json.JSONDecodeError:
            state = {}
        return {
            "relay_id": str(relay_id),
            "name": str(name or ""),
            "source_app": str(source_app),
            "source_session_id": str(source_sid),
            "source_title": str(source_title or ""),
            "target_app": str(target_app),
            "target_session_id": str(target_sid),
            "target_title": str(target_title or ""),
            "bidirectional": bool(bidirectional),
            "enabled": bool(enabled),
            "policy": policy if isinstance(policy, dict) else {},
            "state": state if isinstance(state, dict) else {},
            "created_at": float(created_at or 0),
            "updated_at": float(updated_at or 0),
            "created_at_iso": _utc_iso(float(created_at or 0)),
            "updated_at_iso": _utc_iso(float(updated_at or 0)),
        }

    def upsert_link(
        self,
        *,
        name: str,
        source_app: str,
        source_session_id: str,
        source_title: str,
        target_app: str,
        target_session_id: str,
        target_title: str,
        bidirectional: bool = True,
        enabled: bool = True,
        policy: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        relay_id: str = "",
    ) -> dict[str, Any]:
        now = time.time()
        clean_name = name.strip()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing_id = ""
            if relay_id.strip():
                row = con.execute("SELECT relay_id FROM relay_links WHERE relay_id = ?", (relay_id.strip(),)).fetchone()
                if row:
                    existing_id = str(row[0])
            if not existing_id and clean_name:
                row = con.execute("SELECT relay_id FROM relay_links WHERE lower(name) = lower(?)", (clean_name,)).fetchone()
                if row:
                    existing_id = str(row[0])
            if not existing_id:
                row = con.execute(
                    """
                    SELECT relay_id FROM relay_links
                    WHERE (source_app = ? AND source_session_id = ? AND target_app = ? AND target_session_id = ?)
                       OR (source_app = ? AND source_session_id = ? AND target_app = ? AND target_session_id = ?)
                    LIMIT 1
                    """,
                    (
                        source_app,
                        source_session_id,
                        target_app,
                        target_session_id,
                        target_app,
                        target_session_id,
                        source_app,
                        source_session_id,
                    ),
                ).fetchone()
                if row:
                    existing_id = str(row[0])

            final_id = existing_id or f"r_{uuid.uuid4().hex[:8]}"
            pol_s = json.dumps(policy or {}, ensure_ascii=False)
            st_s = json.dumps(state or {}, ensure_ascii=False)

            if existing_id:
                con.execute(
                    """
                    UPDATE relay_links
                    SET name = ?, source_app = ?, source_session_id = ?, source_title = ?,
                        target_app = ?, target_session_id = ?, target_title = ?,
                        bidirectional = ?, enabled = ?, policy_json = ?, state_json = ?, updated_at = ?
                    WHERE relay_id = ?
                    """,
                    (
                        clean_name or final_id,
                        source_app,
                        source_session_id,
                        source_title,
                        target_app,
                        target_session_id,
                        target_title,
                        1 if bidirectional else 0,
                        1 if enabled else 0,
                        pol_s,
                        st_s,
                        now,
                        final_id,
                    ),
                )
            else:
                con.execute(
                    """
                    INSERT INTO relay_links (
                        relay_id, name, source_app, source_session_id, source_title,
                        target_app, target_session_id, target_title,
                        bidirectional, enabled, policy_json, state_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        final_id,
                        clean_name or final_id,
                        source_app,
                        source_session_id,
                        source_title,
                        target_app,
                        target_session_id,
                        target_title,
                        1 if bidirectional else 0,
                        1 if enabled else 0,
                        pol_s,
                        st_s,
                        now,
                        now,
                    ),
                )
            row = con.execute("SELECT * FROM relay_links WHERE relay_id = ?", (final_id,)).fetchone()
            con.execute("COMMIT")
            assert row is not None
            return self._row_to_link(row)
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    def get_link(self, relay_id: str) -> dict[str, Any] | None:
        con = self._connect()
        try:
            row = con.execute("SELECT * FROM relay_links WHERE relay_id = ?", (relay_id,)).fetchone()
            return self._row_to_link(row) if row else None
        finally:
            con.close()

    def find_link(self, key: str) -> dict[str, Any] | None:
        q = (key or "").strip()
        if not q:
            return None
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM relay_links WHERE lower(relay_id) = lower(?) OR lower(name) = lower(?) LIMIT 1",
                (q, q),
            ).fetchone()
            return self._row_to_link(row) if row else None
        finally:
            con.close()

    def list_links(self, *, include_disabled: bool = False) -> list[dict[str, Any]]:
        con = self._connect()
        try:
            sql = "SELECT * FROM relay_links"
            if not include_disabled:
                sql += " WHERE enabled = 1"
            sql += " ORDER BY updated_at DESC"
            rows = con.execute(sql).fetchall()
            return [self._row_to_link(r) for r in rows]
        finally:
            con.close()

    def set_enabled(self, key: str, enabled: bool) -> dict[str, Any] | None:
        q = (key or "").strip()
        if not q:
            return None
        now = time.time()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT relay_id FROM relay_links WHERE lower(relay_id) = lower(?) OR lower(name) = lower(?) LIMIT 1",
                (q, q),
            ).fetchone()
            if not row:
                con.execute("COMMIT")
                return None
            rid = str(row[0])
            con.execute(
                "UPDATE relay_links SET enabled = ?, updated_at = ? WHERE relay_id = ?",
                (1 if enabled else 0, now, rid),
            )
            updated = con.execute("SELECT * FROM relay_links WHERE relay_id = ?", (rid,)).fetchone()
            con.execute("COMMIT")
            return self._row_to_link(updated) if updated else None
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    def delete_link(self, key: str) -> dict[str, Any] | None:
        q = (key or "").strip()
        if not q:
            return None
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM relay_links WHERE lower(relay_id) = lower(?) OR lower(name) = lower(?) LIMIT 1",
                (q, q),
            ).fetchone()
            if not row:
                con.execute("COMMIT")
                return None
            link = self._row_to_link(row)
            con.execute("DELETE FROM relay_links WHERE relay_id = ?", (link["relay_id"],))
            con.execute("COMMIT")
            return link
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    def update_state(
        self,
        relay_id: str,
        state_patch: dict[str, Any],
        *,
        enabled: bool | None = None,
        clear_keys: set[str] | None = None,
    ) -> dict[str, Any] | None:
        now = time.time()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT state_json, enabled FROM relay_links WHERE relay_id = ?", (relay_id,)).fetchone()
            if not row:
                con.execute("COMMIT")
                return None
            try:
                cur_state = json.loads(row[0]) if row[0] else {}
            except json.JSONDecodeError:
                cur_state = {}
            if not isinstance(cur_state, dict):
                cur_state = {}
            for k in (clear_keys or set()):
                cur_state.pop(k, None)
            cur_state.update(state_patch)
            next_enabled = int(row[1]) if enabled is None else (1 if enabled else 0)
            con.execute(
                "UPDATE relay_links SET state_json = ?, enabled = ?, updated_at = ? WHERE relay_id = ?",
                (json.dumps(cur_state, ensure_ascii=False), next_enabled, now, relay_id),
            )
            updated = con.execute("SELECT * FROM relay_links WHERE relay_id = ?", (relay_id,)).fetchone()
            con.execute("COMMIT")
            return self._row_to_link(updated) if updated else None
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    @staticmethod
    def tx_id_for(
        relay_id: str,
        direction: str,
        source_app: str,
        source_session_id: str,
        content_hash: str,
    ) -> str:
        raw = f"{relay_id}|{direction}|{source_app}:{source_session_id}|{content_hash}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def has_sent_tx(self, relay_id: str, direction: str, content_hash: str) -> bool:
        con = self._connect()
        try:
            row = con.execute(
                """
                SELECT 1 FROM relay_transactions
                WHERE relay_id = ? AND direction = ? AND content_hash = ? AND status = 'sent'
                LIMIT 1
                """,
                (relay_id, direction, content_hash),
            ).fetchone()
            return row is not None
        finally:
            con.close()

    def record_tx(self, tx: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        relay_id = str(tx["relay_id"])
        direction = str(tx["direction"])
        source_app = str(tx["source_app"])
        source_sid = str(tx["source_session_id"])
        chash = str(tx["content_hash"])
        tx_id = str(tx.get("tx_id") or self.tx_id_for(relay_id, direction, source_app, source_sid, chash))
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                """
                INSERT INTO relay_transactions (
                    tx_id, relay_id, direction, source_app, source_session_id,
                    target_app, target_session_id, source_msg_index, content_hash,
                    status, audit_id, verified_session_id, milestone_matched, notified,
                    error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tx_id) DO UPDATE SET
                    status = excluded.status,
                    audit_id = excluded.audit_id,
                    verified_session_id = excluded.verified_session_id,
                    milestone_matched = excluded.milestone_matched,
                    notified = excluded.notified,
                    error = excluded.error,
                    updated_at = excluded.updated_at
                """,
                (
                    tx_id,
                    relay_id,
                    direction,
                    source_app,
                    source_sid,
                    str(tx["target_app"]),
                    str(tx["target_session_id"]),
                    tx.get("source_msg_index"),
                    chash,
                    str(tx["status"]),
                    str(tx.get("audit_id") or ""),
                    str(tx.get("verified_session_id") or ""),
                    str(tx.get("milestone_matched") or ""),
                    1 if tx.get("notified") else 0,
                    str(tx.get("error") or ""),
                    now,
                    now,
                ),
            )
            con.execute("COMMIT")
            return {**tx, "tx_id": tx_id, "updated_at": now}
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    def recent_transactions(self, *, relay_id: str = "", limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        con = self._connect()
        try:
            if relay_id.strip():
                rows = con.execute(
                    """
                    SELECT tx_id, relay_id, direction, source_app, source_session_id,
                           target_app, target_session_id, source_msg_index, content_hash,
                           status, audit_id, verified_session_id, milestone_matched,
                           notified, error, created_at, updated_at
                    FROM relay_transactions
                    WHERE relay_id = ?
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (relay_id.strip(), limit),
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT tx_id, relay_id, direction, source_app, source_session_id,
                           target_app, target_session_id, source_msg_index, content_hash,
                           status, audit_id, verified_session_id, milestone_matched,
                           notified, error, created_at, updated_at
                    FROM relay_transactions
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "tx_id": r[0],
                        "relay_id": r[1],
                        "direction": r[2],
                        "source_app": r[3],
                        "source_session_id": r[4],
                        "target_app": r[5],
                        "target_session_id": r[6],
                        "source_msg_index": r[7],
                        "content_hash": r[8],
                        "status": r[9],
                        "audit_id": r[10],
                        "verified_session_id": r[11],
                        "milestone_matched": r[12],
                        "notified": bool(r[13]),
                        "error": r[14],
                        "created_at": r[15],
                        "updated_at": r[16],
                        "updated_at_iso": _utc_iso(float(r[16] or 0)),
                    }
                )
            return out
        finally:
            con.close()

    @staticmethod
    def notif_key_for(relay_id: str, event_kind: str, dedupe_token: str) -> str:
        raw = f"{relay_id}|{event_kind}|{dedupe_token}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    def has_notified(self, notif_key: str) -> bool:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT 1 FROM relay_notifications WHERE notif_key = ? AND delivered = 1 LIMIT 1",
                (notif_key,),
            ).fetchone()
            return row is not None
        finally:
            con.close()

    def record_notification(
        self,
        notif_key: str,
        relay_id: str,
        event_kind: str,
        targets: list[str],
        delivered: bool,
        detail: str = "",
    ) -> None:
        now = time.time()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                """
                INSERT INTO relay_notifications (
                    notif_key, relay_id, event_kind, targets_json, delivered, detail, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(notif_key) DO UPDATE SET
                    delivered = MAX(relay_notifications.delivered, excluded.delivered),
                    detail = excluded.detail,
                    created_at = excluded.created_at
                """,
                (
                    notif_key,
                    relay_id,
                    event_kind,
                    json.dumps(targets, ensure_ascii=False),
                    1 if delivered else 0,
                    detail,
                    now,
                ),
            )
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()


class RelayBroker:
    """Centralized, idempotent, mutex-enforced bi-directional cross-agent message relay broker."""

    def __init__(
        self,
        service: Any,
        *,
        store: RelayStore | None = None,
        notifier: NotifierFn | None = None,
    ) -> None:
        self.service = service
        self.store = store or getattr(service, "relays", None) or RelayStore()
        self.notifier = notifier or default_hermes_send

    def _read_endpoint(
        self,
        app: str,
        session_id: str,
        title: str = "",
        *,
        tail_messages: int = 10,
    ) -> tuple[dict[str, Any] | None, str | None]:
        adapter = self.service.adapters.get(app)
        if adapter is not None:
            try:
                meta = adapter.get_session_meta(session_id)
            except Exception as exc:  # noqa: BLE001
                meta = None
                logger.warning("Relay adapter get_session_meta failed for %s:%s: %s", app, session_id, exc)
            if meta is not None:
                rd = adapter.read_session(
                    meta.session_id,
                    tail_messages=tail_messages,
                    max_chars_per_message=16000,
                    include_tools=False,
                )
                if "error" not in rd:
                    return rd, None

        writer = self.service.writers.get(app)
        if app == "codex" and writer is not None and hasattr(writer, "read_live_session"):
            try:
                wst = writer.status()
            except Exception as exc:  # noqa: BLE001
                wst = {"ready": False, "detail": str(exc)}
            if wst.get("ready"):
                try:
                    live = writer.read_live_session(
                        session_id,
                        target_title=title,
                        tail_messages=tail_messages,
                        max_chars_per_message=16000,
                    )
                    return live, None
                except Exception as exc:  # noqa: BLE001
                    return None, str(exc)

        return None, f"Session '{session_id}' not found in '{app}'"

    @staticmethod
    def _latest_assistant_message(rd: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(rd, dict):
            return None
        msgs = rd.get("messages") or []
        for m in reversed(msgs):
            if not isinstance(m, dict):
                continue
            if str(m.get("role") or "").lower() != "assistant":
                continue
            content = str(m.get("content") or "").strip()
            if not content:
                continue
            if is_relay_echo(content):
                continue
            return m
        return None

    def create_or_update_link(
        self,
        source_app: str,
        source_session_id: str,
        target_app: str,
        target_session_id: str,
        *,
        name: str = "",
        bidirectional: bool = True,
        settle_seconds: int = 20,
        min_hop_interval_seconds: int = 15,
        max_hops: int = 20,
        extract_code_block: bool = False,
        use_handover_summary: bool = False,
        prefix_a_to_b: str = "",
        prefix_b_to_a: str = "",
        trigger_patterns: list[str] | None = None,
        milestone_patterns: list[str] | None = None,
        stop_patterns: list[str] | None = None,
        notify_targets: str = "weixin",
        notify_on_hop: bool = False,
        notify_on_milestone: bool = True,
        mode: str = "queue",
        model_a: str = "",
        provider_a: str = "",
        reasoning_effort_a: str = "",
        model_b: str = "",
        provider_b: str = "",
        reasoning_effort_b: str = "",
        relay_existing: bool = False,
    ) -> dict[str, Any]:
        """Register or update a bi-directional or uni-directional relay link with baseline hash snapshotting."""
        src_app, src_sid, src_err = self.service._resolve_target(source_app, source_session_id)
        if src_err or src_app is None or src_sid is None:
            return {"error": f"Source error: {src_err}"}

        dst_app, dst_sid, dst_err = self.service._resolve_target(target_app, target_session_id)
        if dst_err or dst_app is None or dst_sid is None:
            return {"error": f"Target error: {dst_err}"}

        if src_app == dst_app and src_sid == dst_sid:
            return {"error": "Refusing self-loop relay link where source and target are identical."}

        for label, sid in (("source", src_sid), ("target", dst_sid)):
            if sid in ("active", "vscode-cline-active"):
                return {
                    "error": (
                        f"Refusing relay link with placeholder {label}_session_id '{sid}'; "
                        "pass a concrete verified session_id."
                    )
                }

        src_rd, src_read_err = self._read_endpoint(src_app, src_sid)
        if src_rd is None:
            return {"error": f"Cannot verify source endpoint ({src_app}:{src_sid}): {src_read_err}"}
        dst_rd, dst_read_err = self._read_endpoint(dst_app, dst_sid)
        if dst_rd is None:
            return {"error": f"Cannot verify target endpoint ({dst_app}:{dst_sid}): {dst_read_err}"}

        src_title = str((src_rd.get("session") or {}).get("title") or src_sid)
        dst_title = str((dst_rd.get("session") or {}).get("title") or dst_sid)

        last_hash_a = ""
        last_hash_b = ""
        if not relay_existing:
            latest_a = self._latest_assistant_message(src_rd)
            latest_b = self._latest_assistant_message(dst_rd)
            if latest_a is not None:
                last_hash_a = canonical_message_hash(str(latest_a.get("content") or ""))
            if latest_b is not None:
                last_hash_b = canonical_message_hash(str(latest_b.get("content") or ""))

        policy = {
            "settle_seconds": max(0, int(settle_seconds)),
            "min_hop_interval_seconds": max(0, int(min_hop_interval_seconds)),
            "max_hops": max(1, int(max_hops)),
            "extract_code_block": bool(extract_code_block),
            "use_handover_summary": bool(use_handover_summary),
            "prefix_a_to_b": str(prefix_a_to_b or "").strip(),
            "prefix_b_to_a": str(prefix_b_to_a or "").strip(),
            "trigger_patterns": [str(p).strip() for p in (trigger_patterns or []) if str(p).strip()],
            "milestone_patterns": (
                [str(p).strip() for p in milestone_patterns if str(p).strip()]
                if milestone_patterns is not None
                else list(DEFAULT_MILESTONE_PATTERNS)
            ),
            "stop_patterns": [str(p).strip() for p in (stop_patterns or []) if str(p).strip()],
            "notify_targets": parse_notify_targets(notify_targets),
            "notify_on_hop": bool(notify_on_hop),
            "notify_on_milestone": bool(notify_on_milestone),
            "mode": (mode or "queue").strip() or "queue",
            "model_a": str(model_a or "").strip(),
            "provider_a": str(provider_a or "").strip(),
            "reasoning_effort_a": str(reasoning_effort_a or "").strip(),
            "model_b": str(model_b or "").strip(),
            "provider_b": str(provider_b or "").strip(),
            "reasoning_effort_b": str(reasoning_effort_b or "").strip(),
        }
        state = {
            "last_relayed_hash_a": last_hash_a,
            "last_relayed_hash_b": last_hash_b,
            "hop_count": 0,
            "last_hop_at": 0.0,
        }

        link = self.store.upsert_link(
            name=name.strip() or f"{src_app}->{dst_app}",
            source_app=src_app,
            source_session_id=src_sid,
            source_title=src_title,
            target_app=dst_app,
            target_session_id=dst_sid,
            target_title=dst_title,
            bidirectional=bool(bidirectional),
            enabled=True,
            policy=policy,
            state=state,
        )
        return {"ok": True, "relay": link}

    def _deliver_alert(
        self,
        link: dict[str, Any],
        *,
        event_kind: str,
        dedupe_token: str,
        text: str,
    ) -> dict[str, Any] | None:
        relay_id = str(link["relay_id"])
        notif_key = self.store.notif_key_for(relay_id, event_kind, dedupe_token)
        if self.store.has_notified(notif_key):
            return None

        policy = link.get("policy") or {}
        targets = parse_notify_targets(policy.get("notify_targets") or "weixin")
        deliveries: list[dict[str, Any]] = []
        for tgt in targets:
            try:
                ok, detail = self.notifier(tgt, text)
            except Exception as exc:  # noqa: BLE001
                ok, detail = False, str(exc)
            deliveries.append({"target": tgt, "ok": bool(ok), "detail": detail, "fallback": False})

        any_ok = any(d["ok"] for d in deliveries)
        if not any_ok:
            tried = set(targets)
            for tgt in targets:
                fb = DEFAULT_FALLBACK_CHANNELS.get(tgt)
                if not fb or fb in tried:
                    continue
                tried.add(fb)
                try:
                    fb_ok, fb_detail = self.notifier(fb, text)
                except Exception as exc:  # noqa: BLE001
                    fb_ok, fb_detail = False, str(exc)
                deliveries.append({"target": fb, "ok": bool(fb_ok), "detail": fb_detail, "fallback": True})
                if fb_ok:
                    any_ok = True
                    break

        self.store.record_notification(
            notif_key,
            relay_id,
            event_kind,
            targets,
            any_ok,
            detail=json.dumps(deliveries, ensure_ascii=False),
        )
        return {
            "relay_id": relay_id,
            "event_kind": event_kind,
            "notif_key": notif_key,
            "delivered": any_ok,
            "targets": targets,
            "message": text,
            "deliveries": deliveries,
        }

    def _build_hop_payload(
        self,
        link: dict[str, Any],
        *,
        direction: str,
        src_app: str,
        src_sid: str,
        src_title: str,
        dst_app: str,
        dst_title: str,
        src_rd: dict[str, Any],
        latest_msg: dict[str, Any],
    ) -> str:
        policy = link.get("policy") or {}
        raw_body = str(latest_msg.get("content") or "").strip()

        if policy.get("use_handover_summary"):
            adapter = self.service.adapters.get(src_app)
            summary = extract_task_completion_summary(
                adapter,
                src_sid,
                session_meta=src_rd.get("session"),
                messages=src_rd.get("messages"),
            )
            if isinstance(summary, dict) and "error" not in summary:
                body = format_handover_packet(summary)
            else:
                body = raw_body
        else:
            body = raw_body

        if policy.get("extract_code_block"):
            body = _extract_main_code_block(body)

        body, _ = truncate_text(body, 14000)
        relay_name = str(link.get("name") or link.get("relay_id"))
        header = f"[Agent Hub Relay | {relay_name} | {src_app}:{src_title} -> {dst_app}:{dst_title}]"
        prefix_key = "prefix_a_to_b" if direction == "a_to_b" else "prefix_b_to_a"
        prefix = str(policy.get(prefix_key) or "").strip()
        if prefix:
            return f"{header}\n{prefix}\n\n{body}".strip()
        return f"{header}\n\n{body}".strip()

    def poll_once(
        self,
        *,
        relay_id: str = "",
        dry_run: bool = False,
        now_ts: float | None = None,
    ) -> dict[str, Any]:
        """Evaluate active relay links and execute at most settled, verified, idempotent hops."""
        now = time.time() if now_ts is None else float(now_ts)
        if relay_id.strip():
            found = self.store.find_link(relay_id.strip())
            links = [found] if (found and found.get("enabled", True)) else []
        else:
            links = self.store.list_links(include_disabled=False)

        actions: list[dict[str, Any]] = []
        notifications: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for link in links:
            rid = str(link["relay_id"])
            policy = link.get("policy") or {}
            state = dict(link.get("state") or {})
            hop_count = int(state.get("hop_count") or 0)
            max_hops = max(1, int(policy.get("max_hops") or 20))
            min_interval = max(0, int(policy.get("min_hop_interval_seconds") or 0))
            settle_s = max(0, int(policy.get("settle_seconds") or 0))
            last_hop_at = float(state.get("last_hop_at") or 0)

            if hop_count >= max_hops:
                if not dry_run:
                    self.store.update_state(rid, {}, enabled=False)
                    msg = format_relay_notification(
                        link,
                        event_kind="max_hops",
                        direction="paused",
                        source_label=f"{link['source_app']}:{link['source_title'] or link['source_session_id']}",
                        target_label=f"{link['target_app']}:{link['target_title'] or link['target_session_id']}",
                        matched_signal=f"hop_count={hop_count}/{max_hops}",
                    )
                    notif = self._deliver_alert(
                        link,
                        event_kind="max_hops",
                        dedupe_token=str(max_hops),
                        text=msg,
                    )
                    if notif:
                        notifications.append(notif)
                actions.append({"relay_id": rid, "status": "paused_max_hops", "hop_count": hop_count})
                continue

            directions: list[tuple[str, str, str, str, str, str, str, str, str]] = [
                (
                    "a_to_b",
                    link["source_app"],
                    link["source_session_id"],
                    link["source_title"],
                    link["target_app"],
                    link["target_session_id"],
                    link["target_title"],
                    "last_relayed_hash_a",
                    "pending_a",
                )
            ]
            if link.get("bidirectional"):
                directions.append(
                    (
                        "b_to_a",
                        link["target_app"],
                        link["target_session_id"],
                        link["target_title"],
                        link["source_app"],
                        link["source_session_id"],
                        link["source_title"],
                        "last_relayed_hash_b",
                        "pending_b",
                    )
                )

            link_stopped = False
            for (
                direction,
                src_app,
                src_sid,
                src_title,
                dst_app,
                dst_sid,
                dst_title,
                hash_key,
                pending_key,
            ) in directions:
                if link_stopped:
                    break

                if min_interval > 0 and last_hop_at > 0 and (now - last_hop_at) < min_interval:
                    continue

                src_rd, src_err = self._read_endpoint(src_app, src_sid, src_title)
                if src_rd is None:
                    errors.append({"relay_id": rid, "direction": direction, "error": src_err})
                    continue

                src_sess = src_rd.get("session") or {}
                src_status = str(src_sess.get("status") or STATUS_IDLE)
                live_src_title = str(src_sess.get("title") or src_title or src_sid)
                if src_status != STATUS_IDLE or src_rd.get("is_generating"):
                    continue

                latest = self._latest_assistant_message(src_rd)
                if latest is None:
                    continue

                raw_content = str(latest.get("content") or "").strip()
                chash = canonical_message_hash(raw_content)
                if not chash or chash == str(state.get(hash_key) or "") or self.store.has_sent_tx(rid, direction, chash):
                    if state.get(pending_key) and not dry_run:
                        self.store.update_state(rid, {}, clear_keys={pending_key})
                        state.pop(pending_key, None)
                    continue

                # Debounce check: require message hash stability across settle_seconds before dispatch.
                if settle_s > 0:
                    pend = state.get(pending_key)
                    if isinstance(pend, dict) and pend.get("hash") == chash:
                        first_seen = float(pend.get("first_seen_at") or now)
                        if (now - first_seen) < settle_s:
                            if not dry_run:
                                new_pend = dict(pend)
                                new_pend["count"] = int(pend.get("count") or 1) + 1
                                state[pending_key] = new_pend
                                self.store.update_state(rid, {pending_key: new_pend})
                            actions.append({"relay_id": rid, "direction": direction, "status": "debouncing", "content_hash": chash})
                            continue
                    else:
                        new_pend = {
                            "hash": chash,
                            "index": latest.get("index"),
                            "first_seen_at": now,
                            "count": 1,
                        }
                        if not dry_run:
                            state[pending_key] = new_pend
                            self.store.update_state(rid, {pending_key: new_pend})
                        actions.append({"relay_id": rid, "direction": direction, "status": "debouncing", "content_hash": chash})
                        continue

                trig_pats = policy.get("trigger_patterns") or []
                if trig_pats and not match_patterns(raw_content, trig_pats):
                    if not dry_run:
                        state[hash_key] = chash
                        state.pop(pending_key, None)
                        self.store.update_state(rid, {hash_key: chash}, clear_keys={pending_key})
                    actions.append({"relay_id": rid, "direction": direction, "status": "filtered_no_trigger", "content_hash": chash})
                    continue

                # Defer cleanly if destination is currently executing a turn.
                dst_rd, _ = self._read_endpoint(dst_app, dst_sid, dst_title, tail_messages=2)
                if dst_rd is not None:
                    dst_sess = dst_rd.get("session") or {}
                    live_dst_title = str(dst_sess.get("title") or dst_title or dst_sid)
                    if str(dst_sess.get("status") or "") == STATUS_RUNNING or dst_rd.get("is_generating"):
                        actions.append({"relay_id": rid, "direction": direction, "status": "deferred_target_running", "content_hash": chash})
                        continue
                else:
                    live_dst_title = dst_title or dst_sid

                final_msg = self._build_hop_payload(
                    link,
                    direction=direction,
                    src_app=src_app,
                    src_sid=src_sid,
                    src_title=live_src_title,
                    dst_app=dst_app,
                    dst_title=live_dst_title,
                    src_rd=src_rd,
                    latest_msg=latest,
                )

                if dry_run:
                    actions.append(
                        {
                            "relay_id": rid,
                            "direction": direction,
                            "status": "dry_run_ready",
                            "source_app": src_app,
                            "source_session_id": src_sid,
                            "target_app": dst_app,
                            "target_session_id": dst_sid,
                            "content_hash": chash,
                            "preview": clean_single_line(final_msg, 220),
                        }
                    )
                    continue

                dst_suffix = "b" if direction == "a_to_b" else "a"
                send_res = self.service.send_message(
                    dst_app,
                    dst_sid,
                    final_msg,
                    sender_tag="",
                    mode=str(policy.get("mode") or "queue"),
                    model=str(policy.get(f"model_{dst_suffix}") or ""),
                    provider=str(policy.get(f"provider_{dst_suffix}") or ""),
                    reasoning_effort=str(policy.get(f"reasoning_effort_{dst_suffix}") or ""),
                    dry_run=False,
                    allow_duplicate=False,
                    source_meta={
                        "relay_id": rid,
                        "relay_direction": direction,
                        "source_app": src_app,
                        "source_session_id": src_sid,
                        "source_title": live_src_title,
                        "source_message_index": latest.get("index"),
                    },
                )

                if "error" in send_res:
                    err_code = str(send_res.get("error_code") or "")
                    err_msg = str(send_res.get("error") or "send failed")
                    tx_status = "aborted_mismatch" if err_code == "session_mismatch" else "failed"
                    self.store.record_tx(
                        {
                            "relay_id": rid,
                            "direction": direction,
                            "source_app": src_app,
                            "source_session_id": src_sid,
                            "target_app": dst_app,
                            "target_session_id": dst_sid,
                            "source_msg_index": latest.get("index"),
                            "content_hash": chash,
                            "status": tx_status,
                            "error": err_msg,
                        }
                    )
                    errors.append({"relay_id": rid, "direction": direction, "error_code": err_code, "error": err_msg})
                    ev_kind = "session_mismatch" if err_code == "session_mismatch" else "error"
                    alert_txt = format_relay_notification(
                        link,
                        event_kind=ev_kind,
                        direction=direction,
                        source_label=f"{src_app}:{live_src_title}",
                        target_label=f"{dst_app}:{live_dst_title}",
                        error=err_msg,
                    )
                    notif = self._deliver_alert(
                        link,
                        event_kind=ev_kind,
                        dedupe_token=f"{direction}:{chash[:12]}",
                        text=alert_txt,
                    )
                    if notif:
                        notifications.append(notif)
                    actions.append({"relay_id": rid, "direction": direction, "status": tx_status, "error": err_msg})
                    continue

                verified_dst_sid = str(send_res.get("verified_session_id") or send_res.get("session_id") or dst_sid)
                audit_id = str(send_res.get("audit_id") or "")

                stop_hit = match_patterns(raw_content, policy.get("stop_patterns") or [])
                milestone_hit = stop_hit or match_patterns(raw_content, policy.get("milestone_patterns") or [])

                notif_sent = False
                if stop_hit:
                    notif_txt = format_relay_notification(
                        link,
                        event_kind="milestone_stop",
                        direction=direction,
                        source_label=f"{src_app}:{live_src_title}",
                        target_label=f"{dst_app}:{live_dst_title}",
                        matched_signal=stop_hit,
                        snippet=raw_content,
                    )
                    notif = self._deliver_alert(
                        link,
                        event_kind="milestone_stop",
                        dedupe_token=f"{direction}:{chash[:16]}",
                        text=notif_txt,
                    )
                    if notif:
                        notifications.append(notif)
                        notif_sent = bool(notif.get("delivered"))
                    link_stopped = True
                elif milestone_hit and policy.get("notify_on_milestone", True):
                    notif_txt = format_relay_notification(
                        link,
                        event_kind="milestone",
                        direction=direction,
                        source_label=f"{src_app}:{live_src_title}",
                        target_label=f"{dst_app}:{live_dst_title}",
                        matched_signal=milestone_hit,
                        snippet=raw_content,
                    )
                    notif = self._deliver_alert(
                        link,
                        event_kind="milestone",
                        dedupe_token=f"{direction}:{chash[:16]}",
                        text=notif_txt,
                    )
                    if notif:
                        notifications.append(notif)
                        notif_sent = bool(notif.get("delivered"))
                elif policy.get("notify_on_hop", False):
                    notif_txt = format_relay_notification(
                        link,
                        event_kind="hop",
                        direction=direction,
                        source_label=f"{src_app}:{live_src_title}",
                        target_label=f"{dst_app}:{live_dst_title}",
                        snippet=raw_content,
                    )
                    notif = self._deliver_alert(
                        link,
                        event_kind="hop",
                        dedupe_token=f"{direction}:{chash[:16]}",
                        text=notif_txt,
                    )
                    if notif:
                        notifications.append(notif)
                        notif_sent = bool(notif.get("delivered"))

                hop_count += 1
                last_hop_at = now
                state[hash_key] = chash
                state["hop_count"] = hop_count
                state["last_hop_at"] = last_hop_at
                state.pop(pending_key, None)

                self.store.record_tx(
                    {
                        "relay_id": rid,
                        "direction": direction,
                        "source_app": src_app,
                        "source_session_id": src_sid,
                        "target_app": dst_app,
                        "target_session_id": verified_dst_sid,
                        "source_msg_index": latest.get("index"),
                        "content_hash": chash,
                        "status": "sent",
                        "audit_id": audit_id,
                        "verified_session_id": verified_dst_sid,
                        "milestone_matched": milestone_hit,
                        "notified": notif_sent,
                    }
                )
                self.store.update_state(
                    rid,
                    {
                        hash_key: chash,
                        "hop_count": hop_count,
                        "last_hop_at": last_hop_at,
                    },
                    enabled=False if link_stopped else None,
                    clear_keys={pending_key},
                )
                actions.append(
                    {
                        "relay_id": rid,
                        "direction": direction,
                        "status": "sent",
                        "audit_id": audit_id,
                        "verified_session_id": verified_dst_sid,
                        "content_hash": chash,
                        "milestone_matched": milestone_hit,
                        "hop_count": hop_count,
                        "auto_stopped": link_stopped,
                    }
                )

        return {
            "checked_links": len(links),
            "actions_count": len(actions),
            "actions": actions,
            "notifications": notifications,
            "errors": errors,
        }
