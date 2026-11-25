"""Persistent friendly task alias bindings for exact session locking."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Any


DEFAULT_BINDINGS_PATH = Path.home() / ".hermes" / "agent-hub" / "bindings.json"


def _resolve_bindings_path(path: Path | None = None) -> Path:
    if path is not None:
        return path
    env_path = os.environ.get("AGENT_HUB_BINDINGS_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_BINDINGS_PATH


class BindingStore:
    """Thread-safe / atomic JSON store mapping human task aliases to (app, session_id)."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = _resolve_bindings_path(path)

    def _load_raw(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if isinstance(data, dict) and isinstance(data.get("bindings"), dict):
            return dict(data["bindings"])
        if isinstance(data, dict):
            # Support flat dict format if edited manually
            return {k: v for k, v in data.items() if isinstance(v, dict) and "session_id" in v}
        return {}

    def _save_raw(self, bindings: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "bindings": bindings,
        }
        fd, tmp_name = tempfile.mkstemp(prefix=".bindings-", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
            os.replace(tmp_name, self.path)
        finally:
            if os.path.exists(tmp_name):
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    def bind(
        self,
        alias: str,
        app: str,
        session_id: str,
        *,
        title: str = "",
        workspace: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        """Create or update a task alias binding."""
        clean_alias = alias.strip()
        if not clean_alias:
            raise ValueError("alias must not be empty")
        bindings = self._load_raw()
        entry = {
            "alias": clean_alias,
            "app": app,
            "session_id": session_id,
            "title": title,
            "workspace": workspace,
            "note": note.strip(),
            "bound_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        }
        bindings[clean_alias] = entry
        self._save_raw(bindings)
        return entry

    def unbind(self, alias: str) -> bool:
        """Delete a task alias binding. Returns True if it existed."""
        clean_alias = alias.strip()
        bindings = self._load_raw()
        if clean_alias not in bindings:
            # Case-insensitive fallback match
            lower_map = {k.lower(): k for k in bindings}
            matched = lower_map.get(clean_alias.lower())
            if not matched:
                return False
            clean_alias = matched
        del bindings[clean_alias]
        self._save_raw(bindings)
        return True

    def list_all(self) -> dict[str, dict[str, Any]]:
        """Return all stored bindings keyed by alias."""
        return self._load_raw()

    def resolve(self, key: str) -> dict[str, Any] | None:
        """Resolve an alias (exact or case-insensitive, stripping optional 'alias:' prefix)."""
        q = key.strip()
        if q.lower().startswith("alias:"):
            q = q[6:].strip()
        if not q:
            return None
        bindings = self._load_raw()
        if q in bindings:
            return bindings[q]
        q_low = q.lower()
        for alias, entry in bindings.items():
            if alias.lower() == q_low:
                return entry
        return None

    def aliases_by_session(self) -> dict[tuple[str, str], list[str]]:
        """Map (app, session_id) -> list of bound aliases."""
        mapping: dict[tuple[str, str], list[str]] = {}
        for alias, entry in self._load_raw().items():
            pair = (str(entry.get("app", "")), str(entry.get("session_id", "")))
            mapping.setdefault(pair, []).append(alias)
        return mapping
