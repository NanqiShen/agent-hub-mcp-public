"""Read-only adapter for DeepSeek Harness (DSH Desktop) local sessions."""

from __future__ import annotations

from compression import zstd
import json
from pathlib import Path
from typing import Any

from ..models import (
    ACTIVE_STATUSES,
    STATUS_ERROR,
    STATUS_IDLE,
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
    STATUS_WAITING_USER,
    SessionMessage,
    SessionSummary,
    clean_single_line,
    extract_text_blocks,
    format_local_iso,
    parse_iso_or_epoch_ms,
    truncate_text,
)


_SYNTHETIC_USER_KINDS = frozenset({"agent-instructions", "runtime-context", "skill-catalog", "time-context"})
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_TRANSCRIPT_CACHE_LIMIT = 64


class DshAdapter:
    """Reads DSH Desktop projcache metadata and zstd-compressed JSONL transcripts."""

    app_name = "dsh"

    def __init__(self, harness_dir: Path | None = None) -> None:
        self.harness_dir = harness_dir or (
            Path.home() / "Library" / "Application Support" / "dsh-desktop" / "harness"
        )
        self.projcache_dir = self.harness_dir / "storages" / "session_projcache" / "sessions"
        self.sessions_dir = self.harness_dir / "sessions"
        self._transcript_cache: dict[tuple[str, int, int], list[str]] = {}

    @staticmethod
    def _row_val(rows: dict[str, Any], key: str, default: Any = None) -> Any:
        entry = rows.get(key)
        if isinstance(entry, dict) and "val" in entry:
            return entry["val"]
        return default

    def _derive_status(self, rows: dict[str, Any]) -> tuple[str, str]:
        uq = self._row_val(rows, "userQuestions", {}) or {}
        active_q = (uq.get("questions") or {}).get("active") or []
        if active_q:
            first_q = clean_single_line(str(active_q[0]), 120) if active_q else ""
            return STATUS_WAITING_USER, f"awaiting answer: {first_q}".strip()

        goal = self._row_val(rows, "goal", {}) or {}
        if goal.get("failure"):
            return STATUS_ERROR, clean_single_line(str(goal.get("failure")), 160)

        approval = self._row_val(rows, "approvalState", {}) or {}
        if approval.get("pending") or approval.get("waiting"):
            return STATUS_WAITING_APPROVAL, "awaiting tool/permission approval"

        stats = self._row_val(rows, "sessionStats", {}) or {}
        pending_calls = stats.get("pendingCalls") or {}
        open_step = stats.get("openStep")
        turn_open = bool(approval.get("turnOpen"))

        if pending_calls or open_step is not None or turn_open:
            parts: list[str] = []
            if pending_calls:
                parts.append(f"pending_calls={len(pending_calls)}")
            if isinstance(open_step, dict):
                parts.append(f"turn={open_step.get('turn')} step={open_step.get('step')}")
            elif turn_open:
                parts.append("turnOpen=true")
            return STATUS_RUNNING, ", ".join(parts)

        return STATUS_IDLE, ""

    def _parse_projcache_file(self, path: Path) -> tuple[SessionSummary, dict[str, Any]] | None:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        record = raw.get("record") or {}
        if not isinstance(record, dict):
            return None
        identity = record.get("identity") or {}
        rows = record.get("rows") or {}
        if not isinstance(rows, dict):
            rows = {}

        session_id = path.stem
        title_val = self._row_val(rows, "title", "")
        turns = ((self._row_val(rows, "turnOutline", {}) or {}).get("turns") or [])
        first_prompt = turns[0].get("prompt", "") if turns and isinstance(turns[0], dict) else ""
        title = clean_single_line(title_val or first_prompt or session_id, 120)

        workspace = str(self._row_val(rows, "workingDirectory", "") or identity.get("cwd") or "")
        meta = self._row_val(rows, "sessionListMetadata", {}) or {}
        ts_candidates: list[int] = [parse_iso_or_epoch_ms(meta.get("lastPromptAt")) if isinstance(meta, dict) else 0]
        try:
            ts_candidates.append(int(path.stat().st_mtime * 1000))
        except OSError:
            pass
        if turns and isinstance(turns[-1], dict):
            last_t = turns[-1]
            for ts_key in ("updatedAt", "completedAt", "endedAt", "timestamp", "startedAt", "createdAt"):
                if last_t.get(ts_key):
                    ts_candidates.append(parse_iso_or_epoch_ms(last_t.get(ts_key)))
        stats = self._row_val(rows, "sessionStats", {}) or {}
        if isinstance(stats, dict):
            open_step = stats.get("openStep")
            if isinstance(open_step, dict):
                for ts_key in ("startTime", "firstTokenTime"):
                    if open_step.get(ts_key):
                        ts_candidates.append(parse_iso_or_epoch_ms(open_step.get(ts_key)))
            pending_calls = stats.get("pendingCalls")
            if isinstance(pending_calls, dict):
                for call_ts in pending_calls.values():
                    if call_ts:
                        ts_candidates.append(parse_iso_or_epoch_ms(call_ts))
        ts_ms = max(ts_candidates, default=0)

        preview = ""
        if turns and isinstance(turns[-1], dict):
            last_t = turns[-1]
            preview = clean_single_line(last_t.get("response") or last_t.get("prompt") or "", 160)

        status, detail = self._derive_status(rows)
        model_sel = self._row_val(rows, "modelSelection", {}) or {}
        extra: dict[str, Any] = {}
        if isinstance(model_sel, dict):
            sel = model_sel.get("pending") or model_sel.get("lastUsed") or model_sel
            if isinstance(sel, dict):
                if sel.get("model"):
                    extra["model"] = sel.get("model")
                if sel.get("provider"):
                    extra["provider"] = sel.get("provider")
                if sel.get("reasoningEffort"):
                    extra["reasoning_effort"] = sel.get("reasoningEffort")
        if turns:
            extra["turns"] = len(turns)

        summary = SessionSummary(
            app=self.app_name,
            session_id=session_id,
            title=title,
            status=status,
            status_detail=detail,
            workspace=workspace,
            updated_at_ms=ts_ms,
            preview=preview,
            extra=extra,
        )
        return summary, rows

    def list_sessions(
        self,
        *,
        active_only: bool = False,
        include_archived: bool = False,  # unused by DSH; kept for interface uniformity
        include_subagents: bool = False,
        limit: int = 20,
    ) -> list[SessionSummary]:
        if not self.projcache_dir.exists():
            return []
        items: list[SessionSummary] = []
        for path in self.projcache_dir.glob("*.json"):
            parsed = self._parse_projcache_file(path)
            if parsed is None:
                continue
            summary, _ = parsed
            if active_only and summary.status not in ACTIVE_STATUSES:
                continue
            items.append(summary)
        items.sort(key=lambda s: s.updated_at_ms, reverse=True)
        return items[:limit]

    def _build_zstd_index(self) -> dict[str, Path]:
        index: dict[str, Path] = {}
        if not self.sessions_dir.exists():
            return index
        for zpath in self.sessions_dir.glob("*/*/session.v4.jsonl.zstd"):
            dir_name = zpath.parent.name
            index[dir_name] = zpath
            if dir_name.startswith("session-"):
                index[dir_name[len("session-") :]] = zpath
            else:
                index[f"session-{dir_name}"] = zpath
        return index

    @staticmethod
    def _decompress_zstd_tail(zpath: Path, max_comp_bytes: int = 2_000_000) -> str:
        try:
            file_size = zpath.stat().st_size
            if file_size <= max_comp_bytes:
                return zstd.decompress(zpath.read_bytes()).decode("utf-8", errors="replace")
            with zpath.open("rb") as fh:
                fh.seek(max(0, file_size - max_comp_bytes))
                chunk = fh.read()
            pos = 0
            while True:
                idx = chunk.find(_ZSTD_MAGIC, pos)
                if idx == -1:
                    break
                try:
                    return zstd.decompress(chunk[idx:]).decode("utf-8", errors="replace")
                except zstd.ZstdError:
                    pos = idx + 4
            return zstd.decompress(zpath.read_bytes()).decode("utf-8", errors="replace")
        except (OSError, zstd.ZstdError):
            return ""

    def _extract_recent_message_texts(self, zpath: Path, *, max_messages: int = 60) -> list[str]:
        try:
            st = zpath.stat()
        except OSError:
            return []
        cache_key = (str(zpath), st.st_mtime_ns, st.st_size)
        cached = self._transcript_cache.get(cache_key)
        if cached is not None:
            return cached

        raw = self._decompress_zstd_tail(zpath)
        if not raw:
            return []

        texts: list[str] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            etype = obj.get("type", "")
            data = obj.get("data") or {}
            if etype == "user/message":
                src = data.get("source")
                src_kind = src.get("kind", "") if isinstance(src, dict) else ""
                if src_kind in _SYNTHETIC_USER_KINDS:
                    continue
                text = extract_text_blocks(data.get("content")).strip()
                if text:
                    texts.append(text)
            elif etype == "assistant/message":
                msg_obj = data.get("message") or {}
                text = self._extract_assistant_blocks(msg_obj.get("content"), include_tools=False).strip()
                if text:
                    texts.append(text)

        recent = texts[-max_messages:]
        if len(self._transcript_cache) >= _TRANSCRIPT_CACHE_LIMIT:
            oldest = next(iter(self._transcript_cache))
            self._transcript_cache.pop(oldest, None)
        self._transcript_cache[cache_key] = recent
        return recent

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
        if not self.projcache_dir.exists() and not self.sessions_dir.exists():
            return []
        q = query.strip().lower()
        if not q:
            return self.list_sessions(limit=limit)
        q_collapsed = " ".join(q.split())

        candidates: list[tuple[SessionSummary, dict[str, Any]]] = []
        seen_sids: set[str] = set()
        if self.projcache_dir.exists():
            for path in self.projcache_dir.glob("*.json"):
                parsed = self._parse_projcache_file(path)
                if parsed is None:
                    continue
                summary, rows = parsed
                candidates.append((summary, rows))
                seen_sids.add(summary.session_id)
                if summary.session_id.startswith("session-"):
                    seen_sids.add(summary.session_id[len("session-") :])
                else:
                    seen_sids.add(f"session-{summary.session_id}")

        zstd_index = self._build_zstd_index()
        seen_zpaths: set[Path] = set()
        for zpath in zstd_index.values():
            if zpath in seen_zpaths:
                continue
            seen_zpaths.add(zpath)
            dir_name = zpath.parent.name
            if dir_name in seen_sids:
                continue
            try:
                ts_ms = int(zpath.stat().st_mtime * 1000)
            except OSError:
                ts_ms = 0
            candidates.append(
                (
                    SessionSummary(
                        app=self.app_name,
                        session_id=dir_name,
                        title=dir_name,
                        status=STATUS_IDLE,
                        updated_at_ms=ts_ms,
                    ),
                    {},
                )
            )
            seen_sids.add(dir_name)

        matched: list[SessionSummary] = []
        unmatched: list[SessionSummary] = []
        for summary, rows in candidates:
            hit = (
                q in summary.session_id.lower()
                or q in summary.title.lower()
                or q in summary.workspace.lower()
                or q in summary.preview.lower()
            )
            if not hit and rows:
                turns = ((self._row_val(rows, "turnOutline", {}) or {}).get("turns") or [])
                for t in turns:
                    if not isinstance(t, dict):
                        continue
                    if q in str(t.get("prompt", "")).lower() or q in str(t.get("response", "")).lower():
                        hit = True
                        break
            if hit:
                matched.append(summary)
            else:
                unmatched.append(summary)

        if len(matched) < limit and zstd_index:
            unmatched.sort(key=lambda s: s.updated_at_ms, reverse=True)
            scanned = 0
            for summary in unmatched:
                zpath = zstd_index.get(summary.session_id)
                if zpath is None:
                    continue
                scanned += 1
                if self._text_matches(self._extract_recent_message_texts(zpath), q, q_collapsed):
                    matched.append(summary)
                    if len(matched) >= limit:
                        break
                if scanned >= 40:
                    break

        matched.sort(key=lambda s: s.updated_at_ms, reverse=True)
        return matched[:limit]

    def _find_projcache(self, session_id: str) -> Path | None:
        if not self.projcache_dir.exists():
            return None
        candidates = [session_id]
        if session_id.startswith("session-"):
            candidates.append(session_id[len("session-") :])
        else:
            candidates.append(f"session-{session_id}")
        for cand in candidates:
            p = self.projcache_dir / f"{cand}.json"
            if p.exists():
                return p
        return None

    def _find_zstd_log(self, session_id: str) -> Path | None:
        if not self.sessions_dir.exists():
            return None
        candidates = [session_id]
        if session_id.startswith("session-"):
            candidates.append(session_id[len("session-") :])
        else:
            candidates.append(f"session-{session_id}")
        for cand in candidates:
            hits = list(self.sessions_dir.glob(f"*/{cand}/session.v4.jsonl.zstd"))
            if hits:
                return hits[0]
        return None

    def get_session_meta(self, session_id: str) -> SessionSummary | None:
        p = self._find_projcache(session_id)
        if p is not None:
            parsed = self._parse_projcache_file(p)
            if parsed is not None:
                return parsed[0]
        zpath = self._find_zstd_log(session_id)
        if zpath is None:
            return None
        return SessionSummary(
            app=self.app_name,
            session_id=session_id,
            title=session_id,
            status=STATUS_IDLE,
            updated_at_ms=int(zpath.stat().st_mtime * 1000),
        )

    @staticmethod
    def _extract_assistant_blocks(content: Any, *, include_tools: bool) -> str:
        if isinstance(content, str):
            return content
        if not isinstance(content, list):
            return ""
        parts: list[str] = []
        for blk in content:
            if not isinstance(blk, dict):
                continue
            btype = blk.get("type", "")
            if btype == "text":
                txt = blk.get("text", "")
                if txt:
                    parts.append(txt)
            elif include_tools and btype == "tool-call":
                name = blk.get("name", "unknown")
                args = clean_single_line(json.dumps(blk.get("arguments", {}), ensure_ascii=False), 200)
                parts.append(f"[tool-call: {name} args={args}]")
        return "\n".join(parts)

    @classmethod
    def _parse_jsonl_messages(cls, raw: str, *, include_tools: bool) -> list[SessionMessage]:
        messages: list[SessionMessage] = []
        for idx, line in enumerate(raw.splitlines()):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            etype = obj.get("type", "")
            data = obj.get("data") or {}
            seq = int(obj.get("seq") or idx)
            ts = format_local_iso(parse_iso_or_epoch_ms(obj.get("time")))

            if etype == "user/message":
                src = data.get("source")
                src_kind = src.get("kind", "") if isinstance(src, dict) else ""
                if src_kind in _SYNTHETIC_USER_KINDS:
                    continue
                text = extract_text_blocks(data.get("content"))
                if text.strip():
                    messages.append(SessionMessage(index=seq, role="user", content=text, timestamp=ts))
            elif etype == "assistant/message":
                msg_obj = data.get("message") or {}
                text = cls._extract_assistant_blocks(msg_obj.get("content"), include_tools=include_tools)
                if text.strip():
                    messages.append(SessionMessage(index=seq, role="assistant", content=text, timestamp=ts))
            elif include_tools and etype == "tool/result":
                tool_name = data.get("name") or data.get("toolName") or "tool"
                out = clean_single_line(json.dumps(data.get("result", data), ensure_ascii=False), 320)
                messages.append(
                    SessionMessage(index=seq, role="tool", content=f"[tool/result: {tool_name}] {out}", timestamp=ts)
                )
        return messages

    def _read_messages_from_zstd(
        self,
        zpath: Path,
        *,
        tail_messages: int,
        include_tools: bool,
        tail_budget_bytes: int = 2_000_000,
    ) -> list[SessionMessage]:
        need = max(1, int(tail_messages))
        try:
            file_size = zpath.stat().st_size
        except OSError:
            return []

        if file_size <= tail_budget_bytes:
            try:
                raw = zstd.decompress(zpath.read_bytes()).decode("utf-8", errors="replace")
            except (OSError, zstd.ZstdError):
                return []
            return self._parse_jsonl_messages(raw, include_tools=include_tools)

        # Fast path: decompress tail frames first without unpacking huge multi-MB logs.
        tail_raw = self._decompress_zstd_tail(zpath, max_comp_bytes=tail_budget_bytes)
        messages = self._parse_jsonl_messages(tail_raw, include_tools=include_tools) if tail_raw else []
        if len(messages) >= need:
            return messages

        larger_budget = tail_budget_bytes * 3
        if larger_budget < file_size:
            wider_raw = self._decompress_zstd_tail(zpath, max_comp_bytes=larger_budget)
            wider_msgs = self._parse_jsonl_messages(wider_raw, include_tools=include_tools) if wider_raw else []
            if len(wider_msgs) >= need:
                return wider_msgs
            if len(wider_msgs) > len(messages):
                messages = wider_msgs

        try:
            full_raw = zstd.decompress(zpath.read_bytes()).decode("utf-8", errors="replace")
            return self._parse_jsonl_messages(full_raw, include_tools=include_tools)
        except (OSError, zstd.ZstdError):
            return messages

    def read_session(
        self,
        session_id: str,
        *,
        tail_messages: int = 10,
        max_chars_per_message: int = 6000,
        include_tools: bool = False,
        tail_budget_bytes: int = 2_000_000,
    ) -> dict[str, Any]:
        meta = self.get_session_meta(session_id)
        if meta is None:
            return {"error": f"DSH session '{session_id}' not found"}

        messages: list[SessionMessage] = []
        zpath = self._find_zstd_log(meta.session_id) or self._find_zstd_log(session_id)
        if zpath is not None:
            messages = self._read_messages_from_zstd(
                zpath,
                tail_messages=tail_messages,
                include_tools=include_tools,
                tail_budget_bytes=tail_budget_bytes,
            )
        else:
            # Fallback to turnOutline inside projcache if zstd file is absent
            pc = self._find_projcache(session_id)
            if pc is not None:
                parsed = self._parse_projcache_file(pc)
                if parsed is not None:
                    _, rows = parsed
                    turns = (self._row_val(rows, "turnOutline", {}) or {}).get("turns") or []
                    for t in turns:
                        if not isinstance(t, dict):
                            continue
                        tnum = int(t.get("turn") or 0)
                        if t.get("prompt"):
                            messages.append(SessionMessage(index=tnum * 2, role="user", content=str(t["prompt"])))
                        if t.get("response"):
                            messages.append(SessionMessage(index=tnum * 2 + 1, role="assistant", content=str(t["response"])))

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
