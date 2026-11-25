"""Append-only send audit log and 60-second duplicate-send guard."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any
import uuid


DEFAULT_AUDIT_PATH = Path.home() / ".hermes" / "agent-hub" / "send_audit.jsonl"


def _resolve_audit_path(path: Path | None = None) -> Path:
    if path is not None:
        return path
    env_path = os.environ.get("AGENT_HUB_AUDIT_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_AUDIT_PATH


class AuditLog:
    """Records outbound writes/relays and guards against duplicate sends."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = _resolve_audit_path(path)

    @staticmethod
    def content_hash(text: str) -> str:
        """Compute deterministic SHA-256 hex digest of normalized message body."""
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _read_tail_entries(self, max_lines: int = 200) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            lines = self.path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return []
        out: list[dict[str, Any]] = []
        for raw in lines[-max_lines:]:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    out.append(obj)
            except json.JSONDecodeError:
                continue
        return out

    def check_duplicate(
        self,
        target_app: str,
        target_session_id: str,
        text_hash: str,
        *,
        window_seconds: int = 60,
    ) -> dict[str, Any] | None:
        """Return recent matching audit entry if identical content was sent within window_seconds."""
        now = time.time()
        for entry in reversed(self._read_tail_entries()):
            if entry.get("status") != "sent" or entry.get("dry_run"):
                continue
            ts = float(entry.get("ts_epoch") or 0)
            if ts <= 0 or (now - ts) > window_seconds:
                continue
            if (
                entry.get("target_app") == target_app
                and entry.get("target_session_id") == target_session_id
                and entry.get("text_hash") == text_hash
            ):
                return entry
        return None

    def record(self, entry: dict[str, Any]) -> dict[str, Any]:
        """Assign ID + timestamp and append one audit event to send_audit.jsonl."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass

        enriched = dict(entry)
        enriched.setdefault("audit_id", uuid.uuid4().hex[:12])
        enriched.setdefault("ts_epoch", round(time.time(), 3))
        enriched.setdefault(
            "timestamp",
            datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        )
        line = json.dumps(enriched, ensure_ascii=False) + "\n"
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)
        return enriched

    def recent(
        self,
        *,
        limit: int = 20,
        app: str = "all",
        action: str = "",
    ) -> list[dict[str, Any]]:
        """Return newest-first audit log entries optionally filtered by target app or action."""
        n = max(1, min(int(limit), 100))
        app_filter = (app or "all").strip().lower()
        act_filter = (action or "").strip().lower()
        entries = self._read_tail_entries(max_lines=max(n * 8, 200))
        out: list[dict[str, Any]] = []
        for entry in reversed(entries):
            if app_filter not in ("", "all", "*") and str(entry.get("target_app") or "").lower() != app_filter:
                continue
            if act_filter and str(entry.get("action") or "").lower() != act_filter:
                continue
            out.append(entry)
            if len(out) >= n:
                break
        return out
