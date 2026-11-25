"""Read-only adapter for Cline (~/.cline DBs + VS Code globalStorage fallback)."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time
from typing import Any
import urllib.request

from ..models import (
    ACTIVE_STATUSES,
    STATUS_ERROR,
    STATUS_IDLE,
    STATUS_INTERRUPTED,
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
    STATUS_WAITING_USER,
    SessionMessage,
    SessionSummary,
    clean_single_line,
    connect_sqlite_ro,
    extract_text_blocks,
    format_local_iso,
    fts5_safe_phrase,
    parse_iso_or_epoch_ms,
    truncate_text,
)


_MSG_CACHE_LIMIT = 64
_RECENT_ACTIVITY_GRACE_MS = 60_000
_APPROVAL_ASKS = frozenset({"command", "tool", "use_mcp_server", "browser_action_launch", "command_output"})
_USER_ASKS = frozenset(
    {"followup", "plan_mode_respond", "mistake_limit_reached", "api_req_failed", "resume_task", "resume_completed_task"}
)


class ClineAdapter:
    """Reads Cline sessions.db, session-search.db, and VS Code globalStorage tasks."""

    app_name = "cline"

    def __init__(
        self,
        cline_dir: Path | None = None,
        vscode_tasks_dir: Path | None = None,
    ) -> None:
        self._use_default_paths = cline_dir is None and vscode_tasks_dir is None
        self.cline_dir = cline_dir or (Path.home() / ".cline")
        self.sessions_db = self.cline_dir / "data" / "db" / "sessions.db"
        self.search_db = self.cline_dir / "data" / "db" / "session-search.db"
        self.vscode_tasks_dir = vscode_tasks_dir or (
            Path.home()
            / "Library"
            / "Application Support"
            / "Code"
            / "User"
            / "globalStorage"
            / "saoudrizwan.claude-dev"
            / "tasks"
        )
        self._msg_cache: dict[tuple[str, int, int], list[str]] = {}

    @staticmethod
    def _map_status(raw_status: str | None) -> tuple[str, str]:
        st = (raw_status or "").strip().lower()
        if st in ("waiting_approval", "awaiting_approval", "approval_required", "needs_approval"):
            return STATUS_WAITING_APPROVAL, st
        if st in ("running", "active", "in_progress", "started"):
            return STATUS_RUNNING, st
        if st in ("failed", "error", "errored"):
            return STATUS_ERROR, st
        if st in ("waiting", "waiting_user", "paused", "clarify"):
            return STATUS_WAITING_USER, st
        if st in ("interrupted", "cancelled", "canceled", "aborted"):
            return STATUS_INTERRUPTED, st
        return STATUS_IDLE, st if st and st != "completed" else ""

    @staticmethod
    def _latest_file_mtime_ms(*paths: str | None) -> int:
        best = 0
        for raw in paths:
            if not raw:
                continue
            try:
                p = Path(raw)
                if p.exists():
                    ms = int(p.stat().st_mtime * 1000)
                    if ms > best:
                        best = ms
            except OSError:
                continue
        return best

    @classmethod
    def _resolve_status(
        cls,
        raw_status: str | None,
        *,
        ended_at: Any = None,
        exit_code: Any = None,
        file_mtime_ms: int = 0,
        now_ms: int = 0,
    ) -> tuple[str, str, dict[str, Any]]:
        status, detail = cls._map_status(raw_status)
        if status != STATUS_ERROR:
            return status, detail, {}

        ended_str = str(ended_at or "").strip()
        ended_ms = parse_iso_or_epoch_ms(ended_str)
        exit_int: int | None = None
        if exit_code is not None and str(exit_code).strip() != "":
            try:
                exit_int = int(exit_code)
            except (TypeError, ValueError):
                exit_int = None

        has_ended = bool(ended_str)
        has_nonzero_exit = exit_int is not None and exit_int != 0
        cur_now_ms = now_ms if now_ms > 0 else int(time.time() * 1000)
        raw_label = detail or "failed"

        if ended_ms > 0 and file_mtime_ms > ended_ms + 2000:
            return (
                STATUS_RUNNING,
                f"transient_error ({raw_label})",
                {"transient_error": True, "raw_status": raw_label},
            )
        if not has_ended and not has_nonzero_exit:
            return (
                STATUS_RUNNING,
                f"transient_error ({raw_label})",
                {"transient_error": True, "raw_status": raw_label},
            )
        if not has_nonzero_exit and file_mtime_ms > 0 and (cur_now_ms - file_mtime_ms) <= _RECENT_ACTIVITY_GRACE_MS:
            return (
                STATUS_RUNNING,
                f"transient_error ({raw_label})",
                {"transient_error": True, "raw_status": raw_label},
            )

        err_detail = f"{raw_label} (exit_code={exit_int})" if has_nonzero_exit else raw_label
        flags: dict[str, Any] = {"confirmed_terminal": True}
        if has_ended:
            flags["ended_at"] = ended_str
        if exit_int is not None:
            flags["exit_code"] = exit_int
        return STATUS_ERROR, err_detail, flags

    @classmethod
    def _classify_ask(cls, ask_name: str, text_hint: str = "") -> tuple[str, str]:
        ask = (ask_name or "").strip().lower()
        hint = clean_single_line(text_hint, 90)
        suffix = f": {hint}" if hint else (f": {ask}" if ask else "")
        if ask in _USER_ASKS:
            return STATUS_WAITING_USER, f"awaiting user{suffix}"
        return STATUS_WAITING_APPROVAL, f"awaiting approval{suffix}"

    @classmethod
    def _inspect_ui_or_messages_approval(cls, paths: dict[str, str]) -> tuple[str, str] | None:
        ui_raw = paths.get("ui_messages_path")
        if ui_raw:
            ui_path = Path(ui_raw)
            if ui_path.exists():
                try:
                    items = json.loads(ui_path.read_text(encoding="utf-8"))
                    if isinstance(items, list) and items:
                        last = items[-1]
                        if isinstance(last, dict) and last.get("type") == "ask" and not last.get("partial"):
                            return cls._classify_ask(str(last.get("ask") or ""), str(last.get("text") or ""))
                except (OSError, json.JSONDecodeError):
                    pass

        msg_raw = paths.get("messages_path")
        if msg_raw:
            msg_path = Path(msg_raw)
            if msg_path.exists():
                try:
                    data = json.loads(msg_path.read_text(encoding="utf-8"))
                    raw_msgs = data.get("messages") if isinstance(data, dict) else data
                    if isinstance(raw_msgs, list) and raw_msgs:
                        last = raw_msgs[-1]
                        if isinstance(last, dict):
                            if last.get("type") == "ask" or last.get("approval_pending") is True or last.get("ask"):
                                return cls._classify_ask(
                                    str(last.get("ask") or "tool"),
                                    extract_text_blocks(last.get("content")) or str(last.get("text") or ""),
                                )
                except (OSError, json.JSONDecodeError):
                    pass
        return None

    @classmethod
    def _last_round_completed(cls, paths: dict[str, str]) -> bool:
        """Return True when the newest persisted message is a finished assistant turn.

        Used to distinguish a stale reconciler exit (process died after the round's
        final answer, e.g. an extension-host restart) from a real mid-task failure.
        Pending asks, partial records, and user-last records keep the error state.
        """
        for key in ("messages_path", "ui_messages_path"):
            raw = paths.get(key)
            if not raw:
                continue
            p = Path(raw)
            if not p.exists():
                continue
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            items = data.get("messages") if isinstance(data, dict) else data
            if not isinstance(items, list) or not items:
                continue
            for it in reversed(items):
                if not isinstance(it, dict) or it.get("partial") is True:
                    continue
                if it.get("type") == "ask" or it.get("ask") or it.get("approval_pending") is True:
                    return False
                if it.get("type") == "say":
                    return str(it.get("say") or "text").strip().lower() in ("", "text", "completion_result")
                role = str(it.get("role") or "").strip().lower()
                if role:
                    return role == "assistant"
            return False
        return False

    def _probe_bridge_pending_asks(self) -> dict[str, tuple[str, str]]:
        """Query local VS Code Cline bridge /health for live in-memory pending ask state."""
        if not self._use_default_paths:
            return {}
        base_dir = Path.home() / ".hermes" / "agent-hub"
        tok_path = base_dir / "cline-bridge.token"
        if not tok_path.exists():
            return {}
        try:
            token = tok_path.read_text(encoding="utf-8").strip()
        except OSError:
            return {}
        if not token:
            return {}

        ports: list[int] = []
        reg_dir = base_dir / "cline-bridges"
        if reg_dir.exists():
            for pfile in reg_dir.glob("*.json"):
                try:
                    p = int(json.loads(pfile.read_text(encoding="utf-8")).get("port") or 0)
                    if p > 0 and p not in ports:
                        ports.append(p)
                except Exception:  # noqa: BLE001
                    pass
        state_file = base_dir / "cline-bridge.json"
        if state_file.exists():
            try:
                p = int(json.loads(state_file.read_text(encoding="utf-8")).get("port") or 0)
                if p > 0 and p not in ports:
                    ports.append(p)
            except Exception:  # noqa: BLE001
                pass
        if not ports:
            ports.append(43918)

        out: dict[str, tuple[str, str]] = {}
        for port in ports[:4]:
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/health",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    method="GET",
                )
                with urllib.request.urlopen(req, timeout=0.3) as resp:
                    health = json.loads(resp.read().decode("utf-8"))
                tid = str(health.get("activeTaskId") or "").strip()
                pending = health.get("pendingAsk")
                if tid and isinstance(pending, dict):
                    out[tid] = self._classify_ask(str(pending.get("ask") or ""), str(pending.get("text") or ""))
            except Exception:  # noqa: BLE001
                continue
        return out

    @staticmethod
    def _parse_metadata_title(metadata_json: str | None) -> str:
        if not metadata_json:
            return ""
        try:
            obj = json.loads(metadata_json)
            if isinstance(obj, dict):
                return str(obj.get("title") or obj.get("name") or obj.get("summary") or "")
        except json.JSONDecodeError:
            pass
        return ""

    def _load_sessions_map(self) -> dict[str, tuple[SessionSummary, dict[str, str]]]:
        """Merge sessions.db and session-search.db keyed by session_id."""
        by_id: dict[str, tuple[SessionSummary, dict[str, str]]] = {}

        # 1. Indexed search DB (titles + older/archived sessions)
        if self.search_db.exists():
            con = connect_sqlite_ro(self.search_db)
            try:
                rows = con.execute(
                    """
                    SELECT i.session_id, i.title, i.indexed_at, i.document_count,
                           MAX(s.started_at) AS last_started,
                           MAX(s.workspace_root) AS ws
                    FROM indexed_sessions i
                    LEFT JOIN session_search s ON s.session_id = i.session_id
                    GROUP BY i.session_id
                    """
                ).fetchall()
                for sid, title, indexed_at, doc_cnt, last_started, ws in rows:
                    ts_ms = parse_iso_or_epoch_ms(last_started or indexed_at)
                    summary = SessionSummary(
                        app=self.app_name,
                        session_id=str(sid),
                        title=clean_single_line(title or sid, 120),
                        status=STATUS_IDLE,
                        workspace=str(ws or ""),
                        updated_at_ms=ts_ms,
                        preview=clean_single_line(title, 160),
                        extra={"indexed_docs": int(doc_cnt or 0)},
                    )
                    by_id[str(sid)] = (summary, {})
            finally:
                con.close()

        # 2. Live/recent sessions.db (takes precedence on status, workspace, paths)
        if self.sessions_db.exists():
            con = connect_sqlite_ro(self.sessions_db)
            try:
                rows = con.execute(
                    """
                    SELECT session_id, source, status, cwd, workspace_root,
                           prompt, metadata_json, messages_path, transcript_path,
                           started_at, updated_at, is_subagent, model, provider,
                           ended_at, exit_code
                    FROM sessions
                    """
                ).fetchall()
                for (
                    sid,
                    source,
                    raw_status,
                    cwd,
                    ws_root,
                    prompt,
                    meta_json,
                    mp,
                    tp,
                    started_at,
                    updated_at,
                    is_subagent,
                    model,
                    provider,
                    ended_at,
                    exit_code,
                ) in rows:
                    sid_str = str(sid)
                    file_mtime_ms = self._latest_file_mtime_ms(mp, tp)
                    status, detail, status_flags = self._resolve_status(
                        raw_status,
                        ended_at=ended_at,
                        exit_code=exit_code,
                        file_mtime_ms=file_mtime_ms,
                    )
                    paths = {
                        "messages_path": str(mp or ""),
                        "transcript_path": str(tp or ""),
                        "ui_messages_path": str(self.vscode_tasks_dir / sid_str / "ui_messages.json"),
                    }
                    if (
                        status == STATUS_ERROR
                        and status_flags.get("confirmed_terminal")
                        and self._last_round_completed(paths)
                    ):
                        # Process exited after the round's final assistant message
                        # (e.g. extension-host restart); the task itself is not in error.
                        status = STATUS_IDLE
                        detail = f"回合已结束 · 进程收尾退出（{detail}）"
                        status_flags = {"stale_terminated": True}
                    existing_title = by_id[sid_str][0].title if sid_str in by_id else ""
                    meta_title = self._parse_metadata_title(meta_json)
                    title = clean_single_line(meta_title or existing_title or prompt or sid_str, 120)
                    ts_ms = max(parse_iso_or_epoch_ms(updated_at or started_at), file_mtime_ms)
                    extra: dict[str, Any] = {}
                    if source:
                        extra["source"] = source
                    if model:
                        extra["model"] = f"{provider}/{model}" if provider else model
                    if status_flags:
                        extra.update(status_flags)
                    summary = SessionSummary(
                        app=self.app_name,
                        session_id=sid_str,
                        title=title,
                        status=status,
                        status_detail=detail,
                        workspace=str(ws_root or cwd or ""),
                        updated_at_ms=ts_ms,
                        preview=clean_single_line(prompt or title, 160),
                        is_subagent=bool(is_subagent),
                        extra=extra,
                    )
                    if not status_flags.get("confirmed_terminal") and status != STATUS_INTERRUPTED:
                        ask_hit = self._inspect_ui_or_messages_approval(paths)
                        if ask_hit is not None:
                            status, detail = ask_hit
                            summary = SessionSummary(
                                app=self.app_name,
                                session_id=sid_str,
                                title=title,
                                status=status,
                                status_detail=detail,
                                workspace=str(ws_root or cwd or ""),
                                updated_at_ms=ts_ms,
                                preview=clean_single_line(prompt or title, 160),
                                is_subagent=bool(is_subagent),
                                extra=extra,
                            )
                    by_id[sid_str] = (summary, paths)
            finally:
                con.close()

        # 3. VS Code globalStorage fallback (only if task directory contains ui_messages.json)
        if self.vscode_tasks_dir.exists():
            for tdir in self.vscode_tasks_dir.iterdir():
                if not tdir.is_dir() or tdir.name in by_id:
                    continue
                ui_file = tdir / "ui_messages.json"
                if not ui_file.exists():
                    continue
                title = tdir.name
                preview = ""
                task_status = STATUS_IDLE
                task_detail = ""
                try:
                    ui_data = json.loads(ui_file.read_text(encoding="utf-8"))
                    if isinstance(ui_data, list):
                        for item in ui_data:
                            if isinstance(item, dict) and item.get("text"):
                                title = clean_single_line(item["text"], 120)
                                preview = clean_single_line(item["text"], 160)
                                break
                        last = ui_data[-1] if ui_data else None
                        if isinstance(last, dict) and last.get("type") == "ask" and not last.get("partial"):
                            task_status, task_detail = self._classify_ask(
                                str(last.get("ask") or ""),
                                str(last.get("text") or ""),
                            )
                except (OSError, json.JSONDecodeError):
                    pass
                summary = SessionSummary(
                    app=self.app_name,
                    session_id=tdir.name,
                    title=title,
                    status=task_status,
                    status_detail=task_detail,
                    updated_at_ms=int(ui_file.stat().st_mtime * 1000),
                    preview=preview,
                    extra={"source": "vscode_globalStorage"},
                )
                by_id[tdir.name] = (summary, {"ui_messages_path": str(ui_file)})

        for live_sid, (live_st, live_detail) in self._probe_bridge_pending_asks().items():
            if live_sid in by_id:
                s, paths = by_id[live_sid]
                s.status = live_st
                s.status_detail = live_detail
            else:
                s = SessionSummary(
                    app=self.app_name,
                    session_id=live_sid,
                    title=live_sid,
                    status=live_st,
                    status_detail=live_detail,
                    updated_at_ms=int(time.time() * 1000),
                    extra={"source": "vscode_cline_bridge"},
                )
                by_id[live_sid] = (s, {"ui_messages_path": str(self.vscode_tasks_dir / live_sid / "ui_messages.json")})

        return by_id

    def list_sessions(
        self,
        *,
        active_only: bool = False,
        include_archived: bool = False,
        include_subagents: bool = False,
        limit: int = 20,
    ) -> list[SessionSummary]:
        merged = self._load_sessions_map()
        items = [summary for summary, _ in merged.values()]
        if not include_subagents:
            items = [s for s in items if not s.is_subagent]
        if active_only:
            items = [s for s in items if s.status in ACTIVE_STATUSES]
        items.sort(key=lambda s: s.updated_at_ms, reverse=True)
        return items[:limit]

    def _read_from_transcript(self, path: Path) -> list[str]:
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return []
        texts: list[str] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("{"):
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        body = obj.get("text") or extract_text_blocks(obj.get("content"))
                        if isinstance(body, str) and body.strip():
                            texts.append(body.strip())
                        continue
                except json.JSONDecodeError:
                    pass
            texts.append(line)
        return texts

    def _extract_recent_file_texts(self, paths: dict[str, str], *, max_messages: int = 60) -> list[str]:
        candidates = [
            ("messages", paths.get("messages_path")),
            ("ui", paths.get("ui_messages_path")),
            ("transcript", paths.get("transcript_path")),
        ]
        for kind, raw_path in candidates:
            if not raw_path:
                continue
            p = Path(raw_path)
            if not p.exists():
                continue
            try:
                st = p.stat()
            except OSError:
                continue
            cache_key = (str(p), st.st_mtime_ns, st.st_size)
            cached = self._msg_cache.get(cache_key)
            if cached is not None:
                return cached
            if kind == "messages":
                texts = [m.content for m in self._read_from_messages_json(p, include_tools=False) if m.content]
            elif kind == "ui":
                texts = [m.content for m in self._read_from_ui_messages(p, include_tools=False) if m.content]
            else:
                texts = self._read_from_transcript(p)
            recent = texts[-max_messages:]
            if len(self._msg_cache) >= _MSG_CACHE_LIMIT:
                oldest = next(iter(self._msg_cache))
                self._msg_cache.pop(oldest, None)
            self._msg_cache[cache_key] = recent
            return recent
        return []

    @staticmethod
    def _text_matches(haystacks: list[str], q_low: str, q_collapsed: str) -> bool:
        for text in haystacks:
            low = text.lower()
            if q_low in low:
                return True
            if q_collapsed and q_collapsed in " ".join(low.split()):
                return True
        return False

    def search_sessions(self, query: str, *, limit: int = 15) -> list[SessionSummary]:
        q = query.strip()
        if not q:
            return self.list_sessions(limit=limit)

        merged = self._load_sessions_map()
        matched_ids: set[str] = set()
        q_low = q.lower()
        q_collapsed = " ".join(q_low.split())

        for sid, (summary, _) in merged.items():
            if (
                q_low in sid.lower()
                or q_low in summary.title.lower()
                or q_low in summary.workspace.lower()
                or q_low in summary.preview.lower()
            ):
                matched_ids.add(sid)

        if len(matched_ids) < limit and self.search_db.exists():
            db_limit = max(limit * 3, 30)
            con = connect_sqlite_ro(self.search_db)
            try:
                try:
                    fts_rows = con.execute(
                        "SELECT DISTINCT session_id FROM session_search WHERE session_search MATCH ? LIMIT ?",
                        (fts5_safe_phrase(q), db_limit),
                    ).fetchall()
                    for (sid,) in fts_rows:
                        matched_ids.add(str(sid))
                except sqlite3.OperationalError:
                    pass

                if len(matched_ids) < limit:
                    like = f"%{q}%"
                    like_rows = con.execute(
                        """
                        SELECT DISTINCT session_id
                        FROM session_search
                        WHERE content LIKE ? OR title LIKE ? OR workspace_root LIKE ?
                        LIMIT ?
                        """,
                        (like, like, like, db_limit),
                    ).fetchall()
                    for (sid,) in like_rows:
                        matched_ids.add(str(sid))
            finally:
                con.close()

        if len(matched_ids) < limit:
            unmatched = [
                (sid, summary, paths)
                for sid, (summary, paths) in merged.items()
                if sid not in matched_ids
            ]
            unmatched.sort(key=lambda item: item[1].updated_at_ms, reverse=True)
            for sid, _, paths in unmatched[:40]:
                texts = self._extract_recent_file_texts(paths)
                if texts and self._text_matches(texts, q_low, q_collapsed):
                    matched_ids.add(sid)
                    if len(matched_ids) >= limit:
                        break

        results = [merged[sid][0] for sid in matched_ids if sid in merged]
        results.sort(key=lambda s: s.updated_at_ms, reverse=True)
        return results[:limit]

    def get_session_meta(self, session_id: str) -> SessionSummary | None:
        merged = self._load_sessions_map()
        pair = merged.get(session_id)
        return pair[0] if pair else None

    def _read_from_messages_json(self, path: Path, *, include_tools: bool) -> list[SessionMessage]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        raw_msgs = data.get("messages") if isinstance(data, dict) else data
        if not isinstance(raw_msgs, list):
            return []
        out: list[SessionMessage] = []
        for idx, m in enumerate(raw_msgs):
            if not isinstance(m, dict):
                continue
            role = str(m.get("role") or "assistant")
            if role == "tool" and not include_tools:
                continue
            content = m.get("content")
            parts: list[str] = []
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                text_body = extract_text_blocks(content)
                if text_body:
                    parts.append(text_body)
                if include_tools:
                    for blk in content:
                        if isinstance(blk, dict) and blk.get("type") in ("tool_use", "tool-call", "tool_result"):
                            tname = blk.get("name") or blk.get("type")
                            parts.append(f"[{blk.get('type')}: {tname}]")
            full_text = "\n".join(p for p in parts if p).strip()
            if not full_text:
                continue
            ts = format_local_iso(parse_iso_or_epoch_ms(m.get("ts")))
            out.append(SessionMessage(index=idx, role=role, content=full_text, timestamp=ts))
        return out

    def _read_from_ui_messages(self, path: Path, *, include_tools: bool) -> list[SessionMessage]:
        try:
            items = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(items, list):
            return []
        out: list[SessionMessage] = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            say = str(item.get("say") or "")
            ask = str(item.get("ask") or "")
            if say == "user_feedback" or (not say and not ask):
                role = "user"
            elif ask and not include_tools and ask not in ("followup", "completion_result"):
                continue
            else:
                role = "assistant"
            ts = format_local_iso(parse_iso_or_epoch_ms(item.get("ts")))
            out.append(SessionMessage(index=idx, role=role, content=text, timestamp=ts))
        return out

    def read_session(
        self,
        session_id: str,
        *,
        tail_messages: int = 10,
        max_chars_per_message: int = 6000,
        include_tools: bool = False,
    ) -> dict[str, Any]:
        merged = self._load_sessions_map()
        pair = merged.get(session_id)
        if pair is None:
            return {"error": f"Cline session '{session_id}' not found"}
        meta, paths = pair

        messages: list[SessionMessage] = []
        mp = paths.get("messages_path")
        if mp and Path(mp).exists():
            messages = self._read_from_messages_json(Path(mp), include_tools=include_tools)

        if not messages and paths.get("ui_messages_path") and Path(paths["ui_messages_path"]).exists():
            messages = self._read_from_ui_messages(Path(paths["ui_messages_path"]), include_tools=include_tools)

        if not messages and self.search_db.exists():
            con = connect_sqlite_ro(self.search_db)
            try:
                rows = con.execute(
                    """
                    SELECT ordinal, role, started_at, content
                    FROM session_search
                    WHERE session_id = ?
                      AND role IN ('user', 'assistant')
                    ORDER BY ordinal DESC
                    LIMIT ?
                    """,
                    (session_id, max(tail_messages, 10)),
                ).fetchall()
                for ordinal, role, started_at, content in reversed(rows):
                    if content and str(content).strip():
                        messages.append(
                            SessionMessage(
                                index=int(ordinal or 0),
                                role=str(role),
                                content=str(content),
                                timestamp=format_local_iso(parse_iso_or_epoch_ms(started_at)),
                            )
                        )
            finally:
                con.close()

        selected = messages[-max(1, tail_messages) :]
        out_msgs: list[dict[str, Any]] = []
        for msg in selected:
            body, was_trunc = truncate_text(msg.content, max_chars_per_message)
            msg.content = body
            msg.truncated = was_trunc
            out_msgs.append(msg.to_dict())

        return {
            "session": meta.to_dict(),
            "message_count_returned": len(out_msgs),
            "messages": out_msgs,
        }
