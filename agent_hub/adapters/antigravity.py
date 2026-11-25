"""Read-only adapter for Google Antigravity App local sessions."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any
from urllib.parse import unquote, urlparse

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
    format_local_iso,
    fts5_safe_phrase,
    parse_iso_or_epoch_ms,
    truncate_text,
)


class AntigravityAdapter:
    """Reads Antigravity conversation_summaries.db and brain transcript JSONL files."""

    app_name = "antigravity"

    def __init__(self, antigravity_dir: Path | None = None) -> None:
        self.base_dir = antigravity_dir or (Path.home() / ".gemini" / "antigravity")
        self.db_path = self.base_dir / "conversation_summaries.db"
        self.brain_dir = self.base_dir / "brain"

    @staticmethod
    def _parse_workspace(workspace_uris: str | None) -> str:
        if not workspace_uris:
            return ""
        raw = str(workspace_uris).strip()
        uris: list[str] = []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                uris = [str(u) for u in parsed if u]
        except json.JSONDecodeError:
            uris = [raw]
        cleaned: list[str] = []
        for uri in uris:
            if uri.startswith("file://"):
                cleaned.append(unquote(urlparse(uri).path))
            else:
                cleaned.append(uri)
        return ", ".join(cleaned)

    @staticmethod
    def _has_parent_column(con: sqlite3.Connection) -> bool:
        try:
            cols = {row[1] for row in con.execute("PRAGMA table_info(conversation_summaries)").fetchall()}
            return "parent_conversation_id" in cols
        except sqlite3.Error:
            return False

    @staticmethod
    def _derive_status(raw_status: str | None, not_fully_idle: int | None, killed: int | None) -> tuple[str, str]:
        st = (raw_status or "").strip().upper()
        if "APPROVAL" in st:
            return STATUS_WAITING_APPROVAL, str(raw_status or "awaiting_approval")
        if "WAITING" in st:
            return STATUS_WAITING_USER, str(raw_status or "waiting_user")
        if "ERROR" in st or "FAIL" in st:
            return STATUS_ERROR, str(raw_status or "error")
        if st == "CASCADE_RUN_STATUS_RUNNING" or not_fully_idle:
            return STATUS_RUNNING, str(raw_status or "not_fully_idle")
        if killed:
            return STATUS_INTERRUPTED, "killed"
        return STATUS_IDLE, ""

    def _inspect_transcript_tail_status(self, session_id: str) -> tuple[str, str] | None:
        tpath = self.brain_dir / session_id / ".system_generated" / "logs" / "transcript.jsonl"
        if not tpath.exists():
            return None
        try:
            size = tpath.stat().st_size
            read_offset = max(0, size - 16384)
            with tpath.open("rb") as fh:
                if read_offset > 0:
                    fh.seek(read_offset)
                raw = fh.read().decode("utf-8", errors="replace")
        except OSError:
            return None

        lines = raw.splitlines()
        if read_offset > 0 and len(lines) > 1:
            lines = lines[1:]

        steps: list[dict[str, Any]] = []
        for raw_ln in lines[-20:]:
            s = raw_ln.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    steps.append(obj)
            except json.JSONDecodeError:
                continue

        if not steps:
            return None

        for step in reversed(steps):
            stype = str(step.get("type") or "").upper()
            st = str(step.get("status") or "").upper()

            if stype == "USER_INPUT":
                return None

            if st in ("WAITING_FOR_APPROVAL", "PENDING_APPROVAL", "AWAITING_APPROVAL", "WAITING_APPROVAL"):
                return STATUS_WAITING_APPROVAL, clean_single_line(step.get("content") or step.get("status"), 120) or "awaiting approval"
            if st in ("WAITING", "WAITING_FOR_USER", "AWAITING_USER", "WAITING_USER"):
                return STATUS_WAITING_USER, clean_single_line(step.get("content") or step.get("status"), 120) or "awaiting user reply"

            if stype == "ERROR_MESSAGE" or st == "ERROR":
                return STATUS_ERROR, clean_single_line(step.get("content") or st, 140) or "step error"

            if stype == "PLANNER_RESPONSE":
                for tc in (step.get("tool_calls") or []):
                    if not isinstance(tc, dict):
                        continue
                    tname = str(tc.get("name") or "")
                    if tname == "ask_question":
                        return STATUS_WAITING_USER, "awaiting ask_question reply"
                    args = tc.get("arguments")
                    if isinstance(args, dict):
                        ameta = args.get("ArtifactMetadata")
                        if isinstance(ameta, dict) and ameta.get("RequestFeedback") is True:
                            return STATUS_WAITING_APPROVAL, "awaiting plan/artifact approval"
                if st == "DONE":
                    return None

        return None

    def _row_to_summary(self, row: tuple[Any, ...]) -> SessionSummary:
        cid, title, preview, step_count, last_mod, ws_uris, raw_status, not_idle, killed = row[:9]
        parent_cid = str(row[9] or "").strip() if len(row) > 9 else ""
        status, detail = self._derive_status(raw_status, not_idle, killed)
        tail_hit = self._inspect_transcript_tail_status(str(cid))
        if tail_hit is not None:
            t_status, t_detail = tail_hit
            if t_status in (STATUS_WAITING_APPROVAL, STATUS_WAITING_USER):
                if status in (STATUS_IDLE, STATUS_RUNNING):
                    status, detail = t_status, t_detail
            elif t_status == STATUS_ERROR and not killed:
                if status in (STATUS_IDLE, STATUS_RUNNING):
                    status, detail = t_status, t_detail
        ts_ms = parse_iso_or_epoch_ms(last_mod)
        extra: dict[str, Any] = {"step_count": int(step_count)} if step_count is not None else {}
        if parent_cid:
            extra["parent_conversation_id"] = parent_cid
        return SessionSummary(
            app=self.app_name,
            session_id=str(cid),
            title=clean_single_line(title or preview or cid, 120),
            status=status,
            status_detail=detail,
            workspace=self._parse_workspace(ws_uris),
            updated_at_ms=ts_ms,
            preview=clean_single_line(preview, 160),
            is_subagent=bool(parent_cid),
            extra=extra,
        )

    def list_sessions(
        self,
        *,
        active_only: bool = False,
        include_archived: bool = False,
        include_subagents: bool = False,
        limit: int = 20,
    ) -> list[SessionSummary]:
        if not self.db_path.exists():
            return []
        con = connect_sqlite_ro(self.db_path)
        try:
            has_parent = self._has_parent_column(con)
            parent_expr = "parent_conversation_id" if has_parent else "'' AS parent_conversation_id"
            clauses: list[str] = []
            if not include_subagents and has_parent:
                clauses.append("(parent_conversation_id IS NULL OR parent_conversation_id = '')")
            where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            fetch_n = max(limit * 3, 25) if active_only else limit
            rows = con.execute(
                f"""
                SELECT conversation_id, title, preview, step_count, last_modified_time,
                       workspace_uris, status, not_fully_idle, killed, {parent_expr}
                FROM conversation_summaries
                {where_sql}
                ORDER BY last_modified_time DESC
                LIMIT ?
                """,
                (fetch_n,),
            ).fetchall()
            out = [self._row_to_summary(r) for r in rows]
            if active_only:
                out = [s for s in out if s.status in ACTIVE_STATUSES][:limit]
            return out
        finally:
            con.close()

    def _transcript_matches(self, tpath: Path, q_low: str, q_collapsed: str) -> bool:
        try:
            lines = tpath.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return False
        need_full: set[int] = set()
        for line_no, raw_line in enumerate(lines):
            if not raw_line.strip():
                continue
            try:
                step = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if step.get("type") not in ("USER_INPUT", "PLANNER_RESPONSE"):
                continue
            text = str(step.get("content") or "")
            low = text.lower()
            if q_low in low or (q_collapsed and q_collapsed in " ".join(low.split())):
                return True
            if "content" in (step.get("truncated_fields") or []):
                need_full.add(line_no)
        if need_full:
            full_path = tpath.parent / "transcript_full.jsonl"
            full_map = self._hydrate_truncated_lines(full_path, need_full)
            for hydrated in full_map.values():
                text = str(hydrated.get("content") or "")
                low = text.lower()
                if q_low in low or (q_collapsed and q_collapsed in " ".join(low.split())):
                    return True
        return False

    def search_sessions(self, query: str, *, limit: int = 15) -> list[SessionSummary]:
        if not self.db_path.exists() and not self.brain_dir.exists():
            return []
        q = query.strip()
        if not q:
            return self.list_sessions(limit=limit)

        by_id: dict[str, SessionSummary] = {}
        con: sqlite3.Connection | None = None
        if self.db_path.exists():
            con = connect_sqlite_ro(self.db_path)
        try:
            if con is not None:
                # 1. Fast FTS5 match across indexed conversation chunks
                try:
                    fts_rows = con.execute(
                        """
                        SELECT DISTINCT cs.conversation_id, cs.title, cs.preview, cs.step_count,
                               cs.last_modified_time, cs.workspace_uris, cs.status,
                               cs.not_fully_idle, cs.killed
                        FROM conversations_fts f
                        JOIN conversation_summaries cs ON cs.conversation_id = f.conversation_id
                        WHERE conversations_fts MATCH ?
                        ORDER BY cs.last_modified_time DESC
                        LIMIT ?
                        """,
                        (fts5_safe_phrase(q), limit),
                    ).fetchall()
                    for r in fts_rows:
                        s = self._row_to_summary(r)
                        by_id[s.session_id] = s
                except sqlite3.OperationalError:
                    pass

                # 2. Substring LIKE fallback on summaries (handles CJK substrings & IDs)
                if len(by_id) < limit:
                    like = f"%{q}%"
                    like_rows = con.execute(
                        """
                        SELECT conversation_id, title, preview, step_count, last_modified_time,
                               workspace_uris, status, not_fully_idle, killed
                        FROM conversation_summaries
                        WHERE conversation_id LIKE ?
                           OR COALESCE(title, '') LIKE ?
                           OR COALESCE(preview, '') LIKE ?
                           OR COALESCE(workspace_uris, '') LIKE ?
                        ORDER BY last_modified_time DESC
                        LIMIT ?
                        """,
                        (like, like, like, like, limit),
                    ).fetchall()
                    for r in like_rows:
                        s = self._row_to_summary(r)
                        by_id.setdefault(s.session_id, s)

                # 3. Substring LIKE fallback on FTS conversation bodies (CJK & phrase body match)
                if len(by_id) < limit:
                    like = f"%{q}%"
                    try:
                        body_rows = con.execute(
                            """
                            SELECT DISTINCT cs.conversation_id, cs.title, cs.preview, cs.step_count,
                                   cs.last_modified_time, cs.workspace_uris, cs.status,
                                   cs.not_fully_idle, cs.killed
                            FROM conversations_fts f
                            JOIN conversation_summaries cs ON cs.conversation_id = f.conversation_id
                            WHERE COALESCE(f.user_body, '') LIKE ?
                               OR COALESCE(f.agent_body, '') LIKE ?
                            ORDER BY cs.last_modified_time DESC
                            LIMIT ?
                            """,
                            (like, like, limit),
                        ).fetchall()
                        for r in body_rows:
                            s = self._row_to_summary(r)
                            by_id.setdefault(s.session_id, s)
                    except sqlite3.OperationalError:
                        pass

            # 4. Recent brain transcript JSONL fallback for unindexed/recent turns
            if len(by_id) < limit and self.brain_dir.exists():
                q_low = q.lower()
                q_collapsed = " ".join(q_low.split())
                tfiles: list[tuple[float, str, Path]] = []
                for tpath in self.brain_dir.glob("*/.system_generated/logs/transcript.jsonl"):
                    cid = tpath.parent.parent.parent.name
                    if cid in by_id:
                        continue
                    try:
                        mtime = tpath.stat().st_mtime
                    except OSError:
                        continue
                    tfiles.append((mtime, cid, tpath))
                tfiles.sort(key=lambda item: item[0], reverse=True)
                for mtime, cid, tpath in tfiles[:20]:
                    if self._transcript_matches(tpath, q_low, q_collapsed):
                        meta = None
                        if con is not None:
                            row = con.execute(
                                """
                                SELECT conversation_id, title, preview, step_count, last_modified_time,
                                       workspace_uris, status, not_fully_idle, killed
                                FROM conversation_summaries
                                WHERE conversation_id = ?
                                LIMIT 1
                                """,
                                (cid,),
                            ).fetchone()
                            if row is not None:
                                meta = self._row_to_summary(row)
                        if meta is None:
                            meta = SessionSummary(
                                app=self.app_name,
                                session_id=cid,
                                title=cid,
                                status=STATUS_IDLE,
                                updated_at_ms=int(mtime * 1000),
                            )
                        by_id[cid] = meta
                        if len(by_id) >= limit:
                            break

            results = list(by_id.values())
            results.sort(key=lambda s: s.updated_at_ms, reverse=True)
            return results[:limit]
        finally:
            if con is not None:
                con.close()

    def get_session_meta(self, session_id: str) -> SessionSummary | None:
        if not self.db_path.exists():
            return None
        con = connect_sqlite_ro(self.db_path)
        try:
            row = con.execute(
                """
                SELECT conversation_id, title, preview, step_count, last_modified_time,
                       workspace_uris, status, not_fully_idle, killed
                FROM conversation_summaries
                WHERE conversation_id = ?
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_summary(row)
        finally:
            con.close()

    def _hydrate_truncated_lines(self, full_path: Path, target_lines: set[int]) -> dict[int, dict[str, Any]]:
        if not target_lines or not full_path.exists():
            return {}
        loaded: dict[int, dict[str, Any]] = {}
        max_idx = max(target_lines)
        with full_path.open("r", encoding="utf-8", errors="replace") as fh:
            for line_no, raw_line in enumerate(fh):
                if line_no in target_lines:
                    try:
                        loaded[line_no] = json.loads(raw_line)
                    except json.JSONDecodeError:
                        pass
                if line_no >= max_idx:
                    break
        return loaded

    def read_session(
        self,
        session_id: str,
        *,
        tail_messages: int = 10,
        max_chars_per_message: int = 6000,
        include_tools: bool = False,
    ) -> dict[str, Any]:
        meta = self.get_session_meta(session_id)
        logs_dir = self.brain_dir / session_id / ".system_generated" / "logs"
        transcript_path = logs_dir / "transcript.jsonl"
        full_path = logs_dir / "transcript_full.jsonl"

        if meta is None and not transcript_path.exists():
            return {"error": f"Antigravity session '{session_id}' not found"}

        if meta is None:
            meta = SessionSummary(
                app=self.app_name,
                session_id=session_id,
                title=session_id,
                status=STATUS_IDLE,
                updated_at_ms=int(transcript_path.stat().st_mtime * 1000),
            )

        selected_entries: list[tuple[int, dict[str, Any]]] = []
        if transcript_path.exists():
            with transcript_path.open("r", encoding="utf-8", errors="replace") as fh:
                for line_no, raw_line in enumerate(fh):
                    if not raw_line.strip():
                        continue
                    try:
                        step = json.loads(raw_line)
                    except json.JSONDecodeError:
                        continue
                    stype = step.get("type")
                    if stype not in ("USER_INPUT", "PLANNER_RESPONSE"):
                        continue
                    has_content = bool(str(step.get("content") or "").strip())
                    has_tools = bool(include_tools and step.get("tool_calls"))
                    if has_content or has_tools:
                        selected_entries.append((line_no, step))

        tail_entries = selected_entries[-max(1, tail_messages) :]
        need_full = {
            line_no
            for line_no, step in tail_entries
            if "content" in (step.get("truncated_fields") or [])
        }
        full_map = self._hydrate_truncated_lines(full_path, need_full)

        out_msgs: list[dict[str, Any]] = []
        for line_no, step in tail_entries:
            hydrated = full_map.get(line_no, step)
            stype = hydrated.get("type")
            role = "user" if stype == "USER_INPUT" else "assistant"
            text = str(hydrated.get("content") or "")
            if include_tools and hydrated.get("tool_calls"):
                tc_names = [
                    str(tc.get("name", "tool"))
                    for tc in hydrated.get("tool_calls", [])
                    if isinstance(tc, dict)
                ]
                if tc_names:
                    prefix = f"[tool_calls: {', '.join(tc_names)}]"
                    text = f"{prefix}\n{text}".strip() if text else prefix

            ts = format_local_iso(parse_iso_or_epoch_ms(hydrated.get("created_at")))
            step_idx = int(hydrated.get("step_index") or line_no)
            body, was_trunc = truncate_text(text, max_chars_per_message)
            msg = SessionMessage(index=step_idx, role=role, content=body, timestamp=ts, truncated=was_trunc)
            out_msgs.append(msg.to_dict())

        return {
            "session": meta.to_dict(),
            "message_count_returned": len(out_msgs),
            "messages": out_msgs,
        }
