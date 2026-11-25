"""Read-only adapter for OpenAI Codex desktop & CLI local storage."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any

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
    parse_iso_or_epoch_ms,
    truncate_text,
)

_APPROVAL_ITEM_STATUSES = frozenset(
    {"pending", "waiting_approval", "requires_approval", "awaiting_approval", "needsapproval", "needs_approval"}
)


class CodexAdapter:
    """Reads Codex session titles, turn statuses, goals, and messages read-only."""

    app_name = "codex"

    def __init__(self, codex_dir: Path | None = None) -> None:
        self.codex_dir = codex_dir or (Path.home() / ".codex")
        self.state_db = self.codex_dir / "state_5.sqlite"
        self.history_db = self.codex_dir / "thread_history_1.sqlite"
        self.goals_db = self.codex_dir / "goals_1.sqlite"

    def _open_optional(self, path: Path) -> sqlite3.Connection | None:
        if not path.exists():
            return None
        return connect_sqlite_ro(path)

    @staticmethod
    def _pick_title(row: tuple[Any, ...]) -> str:
        # row: id, title, name, preview, first_user_message
        sid, title, name, preview, first_msg = row[0], row[1], row[2], row[3], row[4]
        for candidate in (name, title, preview, first_msg, sid):
            cleaned = clean_single_line(candidate, max_len=120)
            if cleaned:
                return cleaned
        return str(sid)

    @staticmethod
    def _summarize_error(error_json: str | None) -> str:
        if not error_json:
            return "turn failed"
        try:
            obj = json.loads(error_json)
            if isinstance(obj, dict):
                msg = obj.get("message") or obj.get("codexErrorInfo") or ""
                if msg:
                    return clean_single_line(str(msg), max_len=160)
        except json.JSONDecodeError:
            pass
        return clean_single_line(error_json, max_len=160)

    def _derive_thread_status(
        self,
        con_hist: sqlite3.Connection | None,
        con_goals: sqlite3.Connection | None,
        thread_id: str,
    ) -> tuple[str, str]:
        status = STATUS_IDLE
        details: list[str] = []

        if con_hist is not None:
            turn_row = con_hist.execute(
                """
                SELECT status, error_json, started_at, completed_at
                FROM thread_turns
                WHERE thread_id = ?
                ORDER BY rollout_ordinal DESC
                LIMIT 1
                """,
                (thread_id,),
            ).fetchone()
            if turn_row:
                t_status, err_json, _, _ = turn_row
                if t_status in ("waitingApproval", "waiting_approval"):
                    status = STATUS_WAITING_APPROVAL
                    details.append("awaiting approval")
                elif t_status == "inProgress":
                    status = STATUS_RUNNING
                    recent_items = con_hist.execute(
                        """
                        SELECT item_type, item_json
                        FROM thread_items
                        WHERE thread_id = ?
                        ORDER BY rollout_ordinal DESC
                        LIMIT 8
                        """,
                        (thread_id,),
                    ).fetchall()
                    for itype, ij in recent_items:
                        if itype in ("approvalRequest", "permissionRequest"):
                            status = STATUS_WAITING_APPROVAL
                            detail_txt = ""
                            try:
                                obj = json.loads(ij)
                                detail_txt = clean_single_line(
                                    obj.get("command") or obj.get("tool") or obj.get("reason") or "",
                                    80,
                                )
                            except json.JSONDecodeError:
                                pass
                            details.append(f"awaiting approval: {detail_txt}".strip(": ") if detail_txt else "awaiting approval")
                            break
                        if itype == "userInputRequest":
                            status = STATUS_WAITING_USER
                            details.append("awaiting user input")
                            break
                        if itype in ("commandExecution", "fileChange", "mcpToolCall"):
                            try:
                                obj = json.loads(ij)
                                item_st = str(obj.get("status") or "").strip().lower()
                                ap_st = str(obj.get("approvalStatus") or obj.get("approval_status") or "").strip().lower()
                                req_ap = obj.get("requiresApproval") is True
                                if item_st in _APPROVAL_ITEM_STATUSES or ap_st in ("pending", "waiting", "awaiting") or req_ap:
                                    status = STATUS_WAITING_APPROVAL
                                    hint = clean_single_line(
                                        obj.get("command") or obj.get("tool") or obj.get("name") or itype,
                                        80,
                                    )
                                    details.append(f"awaiting approval ({itype}: {hint})")
                                    break
                            except json.JSONDecodeError:
                                pass
                        elif itype == "agentMessage":
                            try:
                                obj = json.loads(ij)
                                questions = obj.get("questions")
                                if questions:
                                    status = STATUS_WAITING_USER
                                    details.append("awaiting user answer")
                            except json.JSONDecodeError:
                                pass
                            break
                    if status == STATUS_RUNNING and recent_items:
                        latest_type = recent_items[0][0]
                        details.append(f"inProgress ({latest_type})")
                elif t_status == "failed":
                    status = STATUS_ERROR
                    details.append(self._summarize_error(err_json))
                elif t_status == "interrupted":
                    status = STATUS_INTERRUPTED
                    details.append("turn interrupted")
                elif t_status == "completed":
                    status = STATUS_IDLE

        if con_goals is not None:
            goal_row = con_goals.execute(
                """
                SELECT status, objective
                FROM thread_goals
                WHERE thread_id = ?
                ORDER BY updated_at_ms DESC
                LIMIT 1
                """,
                (thread_id,),
            ).fetchone()
            if goal_row:
                g_status, g_obj = goal_row
                if g_status in ("active", "blocked"):
                    details.append(f"goal:{g_status} {clean_single_line(g_obj, 60)}")

        return status, "; ".join(d for d in details if d)

    def _row_to_summary(
        self,
        row: tuple[Any, ...],
        con_hist: sqlite3.Connection | None,
        con_goals: sqlite3.Connection | None,
    ) -> SessionSummary:
        (
            tid,
            title,
            name,
            preview,
            first_msg,
            cwd,
            updated_at,
            updated_at_ms,
            recency_at_ms,
            archived,
            is_pinned,
            source,
            thread_source,
            parent_id,
            model,
            model_provider,
            reasoning_effort,
        ) = row
        ts_ms = parse_iso_or_epoch_ms(updated_at_ms or recency_at_ms or updated_at)
        status, detail = self._derive_thread_status(con_hist, con_goals, tid)
        is_sub = bool(parent_id) or (thread_source == "subagent")
        extra: dict[str, Any] = {}
        if model:
            extra["model"] = model
        if model_provider:
            extra["provider"] = model_provider
        if reasoning_effort:
            extra["reasoning_effort"] = reasoning_effort
        if source:
            extra["source"] = source
        if is_sub:
            extra["is_subagent"] = True
        return SessionSummary(
            app=self.app_name,
            session_id=str(tid),
            title=self._pick_title(row),
            status=status,
            status_detail=detail,
            workspace=str(cwd or ""),
            updated_at_ms=ts_ms,
            preview=clean_single_line(preview or first_msg, max_len=160),
            is_pinned=bool(is_pinned),
            archived=bool(archived),
            is_subagent=is_sub,
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
        if not self.state_db.exists():
            return []
        con_state = connect_sqlite_ro(self.state_db)
        con_hist = self._open_optional(self.history_db)
        con_goals = self._open_optional(self.goals_db)
        try:
            clauses: list[str] = []
            if not include_archived:
                clauses.append("archived = 0")
            if not include_subagents:
                clauses.append("(thread_source IS NULL OR thread_source != 'subagent')")
                clauses.append("attached_thread_parent_id IS NULL")
            where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            fetch_n = max(limit * 4, 80) if active_only else limit
            rows = con_state.execute(
                f"""
                SELECT id, title, name, preview, first_user_message, cwd,
                       updated_at, updated_at_ms, recency_at_ms, archived,
                       is_pinned, source, thread_source, attached_thread_parent_id,
                       model, model_provider, reasoning_effort
                FROM threads
                {where_sql}
                ORDER BY COALESCE(updated_at_ms, updated_at * 1000, 0) DESC
                LIMIT ?
                """,
                (fetch_n,),
            ).fetchall()

            results: list[SessionSummary] = []
            for row in rows:
                summary = self._row_to_summary(row, con_hist, con_goals)
                if active_only and summary.status not in ACTIVE_STATUSES:
                    continue
                results.append(summary)
                if len(results) >= limit:
                    break
            return results
        finally:
            con_state.close()
            if con_hist is not None:
                con_hist.close()
            if con_goals is not None:
                con_goals.close()

    def search_sessions(self, query: str, *, limit: int = 15) -> list[SessionSummary]:
        if not self.state_db.exists():
            return []
        q = query.strip()
        if not q:
            return self.list_sessions(limit=limit)

        con_state = connect_sqlite_ro(self.state_db)
        con_hist = self._open_optional(self.history_db)
        con_goals = self._open_optional(self.goals_db)
        try:
            like = f"%{q}%"
            rows = con_state.execute(
                """
                SELECT id, title, name, preview, first_user_message, cwd,
                       updated_at, updated_at_ms, recency_at_ms, archived,
                       is_pinned, source, thread_source, attached_thread_parent_id,
                       model, model_provider, reasoning_effort
                FROM threads
                WHERE (
                    id LIKE ?
                    OR COALESCE(name, '') LIKE ?
                    OR COALESCE(title, '') LIKE ?
                    OR COALESCE(preview, '') LIKE ?
                    OR COALESCE(first_user_message, '') LIKE ?
                    OR COALESCE(cwd, '') LIKE ?
                )
                  AND (thread_source IS NULL OR thread_source != 'subagent')
                  AND attached_thread_parent_id IS NULL
                ORDER BY archived ASC, COALESCE(updated_at_ms, updated_at * 1000, 0) DESC
                LIMIT ?
                """,
                (like, like, like, like, like, like, limit),
            ).fetchall()

            seen_ids: set[str] = set()
            results: list[SessionSummary] = []
            for row in rows:
                seen_ids.add(str(row[0]))
                results.append(self._row_to_summary(row, con_hist, con_goals))

            # Fast indexed fallback across recent 40 non-archived threads' message bodies
            if len(results) < limit and con_hist is not None:
                q_low = q.lower()
                candidates = con_state.execute(
                    """
                    SELECT id, title, name, preview, first_user_message, cwd,
                           updated_at, updated_at_ms, recency_at_ms, archived,
                           is_pinned, source, thread_source, attached_thread_parent_id,
                           model, model_provider, reasoning_effort
                    FROM threads
                    WHERE archived = 0
                      AND (thread_source IS NULL OR thread_source != 'subagent')
                      AND attached_thread_parent_id IS NULL
                    ORDER BY COALESCE(updated_at_ms, updated_at * 1000, 0) DESC
                    LIMIT 40
                    """
                ).fetchall()
                for row in candidates:
                    tid = str(row[0])
                    if tid in seen_ids:
                        continue
                    items = con_hist.execute(
                        """
                        SELECT item_json
                        FROM thread_items
                        WHERE thread_id = ?
                          AND item_type IN ('userMessage', 'agentMessage')
                        ORDER BY rollout_ordinal DESC
                        LIMIT 40
                        """,
                        (tid,),
                    ).fetchall()
                    if any(q_low in (ij or "").lower() for (ij,) in items):
                        seen_ids.add(tid)
                        results.append(self._row_to_summary(row, con_hist, con_goals))
                        if len(results) >= limit:
                            break

            return results
        finally:
            con_state.close()
            if con_hist is not None:
                con_hist.close()
            if con_goals is not None:
                con_goals.close()

    def get_session_meta(self, session_id: str) -> SessionSummary | None:
        if not self.state_db.exists():
            return None
        con_state = connect_sqlite_ro(self.state_db)
        con_hist = self._open_optional(self.history_db)
        con_goals = self._open_optional(self.goals_db)
        try:
            row = con_state.execute(
                """
                SELECT id, title, name, preview, first_user_message, cwd,
                       updated_at, updated_at_ms, recency_at_ms, archived,
                       is_pinned, source, thread_source, attached_thread_parent_id,
                       model, model_provider, reasoning_effort
                FROM threads
                WHERE id = ?
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_summary(row, con_hist, con_goals)
        finally:
            con_state.close()
            if con_hist is not None:
                con_hist.close()
            if con_goals is not None:
                con_goals.close()

    def _parse_item(self, itype: str, item_json: str, created_ms: int, idx: int, *, include_tools: bool) -> SessionMessage | None:
        try:
            obj = json.loads(item_json)
        except json.JSONDecodeError:
            return None
        if not isinstance(obj, dict):
            return None

        ts = format_local_iso(parse_iso_or_epoch_ms(created_ms))
        if itype == "userMessage":
            text = extract_text_blocks(obj.get("content"))
            if not text.strip():
                return None
            return SessionMessage(index=idx, role="user", content=text, timestamp=ts)

        if itype == "agentMessage":
            text = str(obj.get("text") or "")
            questions = obj.get("questions")
            if questions:
                q_str = json.dumps(questions, ensure_ascii=False)
                text = f"{text}\n\n[questions]: {q_str}".strip()
            if not text.strip():
                return None
            extra = {"phase": obj["phase"]} if obj.get("phase") else {}
            return SessionMessage(index=idx, role="assistant", content=text, timestamp=ts, extra=extra)

        if not include_tools:
            return None

        if itype == "commandExecution":
            cmd = clean_single_line(obj.get("command"), 180)
            st = obj.get("status", "")
            code = obj.get("exitCode")
            out = clean_single_line(obj.get("aggregatedOutput"), 400)
            body = f"[command status={st} exit={code}] {cmd}"
            if out:
                body += f"\noutput: {out}"
            return SessionMessage(index=idx, role="tool", content=body, timestamp=ts)

        if itype == "fileChange":
            changes = obj.get("changes") or []
            paths = [str(c.get("path", "")) for c in changes[:8] if isinstance(c, dict)]
            return SessionMessage(
                index=idx,
                role="tool",
                content=f"[fileChange status={obj.get('status', '')}] {', '.join(paths)}",
                timestamp=ts,
            )

        if itype == "plan":
            plan_txt = obj.get("text") or json.dumps(obj, ensure_ascii=False)
            return SessionMessage(index=idx, role="tool", content=f"[plan]\n{plan_txt}", timestamp=ts)

        if itype in ("mcpToolCall", "collabAgentToolCall"):
            tool_name = obj.get("tool") or obj.get("name") or itype
            return SessionMessage(
                index=idx,
                role="tool",
                content=f"[{itype}: {tool_name} status={obj.get('status', '')}]",
                timestamp=ts,
            )

        return None

    def read_session(
        self,
        session_id: str,
        *,
        tail_messages: int = 10,
        max_chars_per_message: int = 6000,
        include_tools: bool = False,
    ) -> dict[str, Any]:
        meta = self.get_session_meta(session_id)
        if meta is None:
            return {"error": f"Codex session '{session_id}' not found"}

        messages: list[SessionMessage] = []
        if self.history_db.exists():
            con_hist = connect_sqlite_ro(self.history_db)
            try:
                allowed = ["'userMessage'", "'agentMessage'"]
                if include_tools:
                    allowed.extend(["'commandExecution'", "'fileChange'", "'plan'", "'mcpToolCall'", "'collabAgentToolCall'"])
                fetch_limit = max(tail_messages * 12, 80)
                rows = con_hist.execute(
                    f"""
                    SELECT rollout_ordinal, item_type, item_json, created_at_ms
                    FROM thread_items
                    WHERE thread_id = ?
                      AND item_type IN ({', '.join(allowed)})
                    ORDER BY rollout_ordinal DESC
                    LIMIT ?
                    """,
                    (session_id, fetch_limit),
                ).fetchall()
                for ordinal, itype, ij, created_ms in reversed(rows):
                    msg = self._parse_item(itype, ij, created_ms or 0, int(ordinal or 0), include_tools=include_tools)
                    if msg is not None:
                        messages.append(msg)
            finally:
                con_hist.close()

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
