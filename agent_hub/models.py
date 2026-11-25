"""Shared data models, unified statuses, and safe read-only helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any, Protocol


VALID_APPS: tuple[str, ...] = ("codex", "dsh", "antigravity", "cline")

STATUS_RUNNING = "running"
STATUS_WAITING_APPROVAL = "waiting_approval"
STATUS_WAITING_USER = "waiting_user"
STATUS_ERROR = "error"
STATUS_INTERRUPTED = "interrupted"
STATUS_IDLE = "idle"

ACTIVE_STATUSES: frozenset[str] = frozenset(
    {
        STATUS_RUNNING,
        STATUS_WAITING_APPROVAL,
        STATUS_WAITING_USER,
        STATUS_ERROR,
    }
)


@dataclass(slots=True)
class SessionSummary:
    """Normalized metadata summary for a session in any supported desktop agent app."""

    app: str
    session_id: str
    title: str
    status: str
    status_detail: str = ""
    workspace: str = ""
    updated_at_ms: int = 0
    preview: str = ""
    is_pinned: bool = False
    archived: bool = False
    is_subagent: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self, bound_aliases: list[str] | None = None) -> dict[str, Any]:
        """Serialize summary to a JSON-friendly dict."""
        data: dict[str, Any] = {
            "app": self.app,
            "session_id": self.session_id,
            "title": self.title,
            "status": self.status,
            "status_detail": self.status_detail,
            "workspace": self.workspace,
            "updated_at": format_local_iso(self.updated_at_ms),
            "updated_at_ms": self.updated_at_ms,
            "preview": self.preview,
            "is_pinned": self.is_pinned,
            "archived": self.archived,
        }
        if bound_aliases:
            data["bound_aliases"] = list(bound_aliases)
        if self.extra:
            data["extra"] = self.extra
        return data


@dataclass(slots=True)
class SessionMessage:
    """Normalized conversation turn/message extracted from a session transcript."""

    index: int
    role: str
    content: str
    timestamp: str = ""
    truncated: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize message to a JSON-friendly dict."""
        data: dict[str, Any] = {
            "index": self.index,
            "role": self.role,
            "content": self.content,
        }
        if self.timestamp:
            data["timestamp"] = self.timestamp
        if self.truncated:
            data["truncated"] = True
        if self.extra:
            data["extra"] = self.extra
        return data


class AppAdapter(Protocol):
    """Protocol implemented by every desktop agent read-only adapter."""

    app_name: str

    def list_sessions(
        self,
        *,
        active_only: bool = False,
        include_archived: bool = False,
        include_subagents: bool = False,
        limit: int = 20,
    ) -> list[SessionSummary]: ...

    def search_sessions(self, query: str, *, limit: int = 15) -> list[SessionSummary]: ...

    def read_session(
        self,
        session_id: str,
        *,
        tail_messages: int = 10,
        max_chars_per_message: int = 6000,
        include_tools: bool = False,
    ) -> dict[str, Any]: ...

    def get_session_meta(self, session_id: str) -> SessionSummary | None: ...


def connect_sqlite_ro(path: Path) -> sqlite3.Connection:
    """Open a SQLite database strictly in read-only URI mode."""
    if not path.exists():
        raise FileNotFoundError(f"SQLite file not found: {path}")
    return sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5.0)


def clean_single_line(text: str | None, max_len: int = 140) -> str:
    """Collapse whitespace/newlines to a single readable line."""
    if not text:
        return ""
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 1].rstrip() + "…"


def truncate_text(text: str | None, max_chars: int = 6000) -> tuple[str, bool]:
    """Preserve head and tail when truncating long messages."""
    raw = str(text or "")
    if max_chars <= 0 or len(raw) <= max_chars:
        return raw, False
    if max_chars < 200:
        return raw[:max_chars], True
    head_len = int(max_chars * 0.72)
    tail_len = max_chars - head_len - 48
    if tail_len < 40:
        return raw[:max_chars], True
    omitted = len(raw) - head_len - tail_len
    marker = f"\n\n... [truncated {omitted} chars] ...\n\n"
    return raw[:head_len] + marker + raw[-tail_len:], True


def parse_iso_or_epoch_ms(val: Any) -> int:
    """Normalize seconds epoch, milliseconds epoch, or ISO-8601 string into UTC ms."""
    if val is None or val == "":
        return 0
    if isinstance(val, (int, float)):
        num = float(val)
        if num <= 0:
            return 0
        # Heuristic: values below 1e11 are seconds; >= 1e11 are milliseconds.
        return int(num * 1000) if num < 100_000_000_000 else int(num)
    s = str(val).strip()
    if not s:
        return 0
    if s.isdigit():
        return parse_iso_or_epoch_ms(int(s))
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def format_local_iso(ms: int) -> str:
    """Format UTC millisecond timestamp as local ISO-8601 string."""
    if not ms or ms <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone().isoformat(timespec="seconds")
    except (OSError, OverflowError, ValueError):
        return ""


def fts5_safe_phrase(query: str) -> str:
    """Quote an arbitrary user search string as an FTS5 phrase literal."""
    cleaned = query.replace('"', '""').strip()
    return f'"{cleaned}"'


def extract_text_blocks(blocks: Any) -> str:
    """Extract joined text from heterogeneous content block lists or strings."""
    if isinstance(blocks, str):
        return blocks
    if not isinstance(blocks, list):
        return ""
    parts: list[str] = []
    for item in blocks:
        if isinstance(item, str):
            if item:
                parts.append(item)
        elif isinstance(item, dict):
            btype = item.get("type", "")
            if btype in ("", "text", "input_text", "output_text"):
                txt = item.get("text")
                if isinstance(txt, str) and txt:
                    parts.append(txt)
    return "\n".join(parts)
