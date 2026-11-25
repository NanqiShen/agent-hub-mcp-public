"""Structured task completion summary card and cross-agent handover packet extraction."""

from __future__ import annotations

import re
from typing import Any

from .models import AppAdapter, STATUS_IDLE, SessionSummary, clean_single_line


BG_LABEL_SUBMITTED = "【已提交后台作业】"
BG_LABEL_RUNNING = "【阶段汇报·后台作业运行中】"

_BG_SUBMITTED_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("submitted_batch_job", re.compile(r"\bSubmitted\s+batch\s+job\s+\d+", re.IGNORECASE)),
    ("sbatch", re.compile(r"\bsbatch\b", re.IGNORECASE)),
    ("cn_submitted_job", re.compile(r"已提交[^\n。；;]{0,24}(?:Slurm\s*)?(?:作业|任务|批处理|后台|job)", re.IGNORECASE)),
    ("cn_submitted_job_le", re.compile(r"提交了[^\n。；;]{0,24}(?:Slurm\s*)?(?:作业|任务|批处理|后台|job)", re.IGNORECASE)),
    ("en_submitted_job", re.compile(r"\bsubmitted\s+(?:a\s+)?(?:slurm|batch|background)\s+(?:job|task)s?\b", re.IGNORECASE)),
)

_BG_RUNNING_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("cn_running_background", re.compile(r"正在后台")),
    ("cn_background_active", re.compile(r"后台(?:作业|任务|进程)[^\n。；;]{0,12}(?:运行中|执行中|排队中|进行中)")),
    ("cn_waiting_result", re.compile(r"等待[^\n。；;]{0,24}(?:出结果|跑完|运行完成|执行完成|作业完成|结束)")),
    ("en_running_background", re.compile(r"\brunning\s+in\s+(?:the\s+)?background\b", re.IGNORECASE)),
    ("en_background_active", re.compile(r"\bbackground\s+(?:job|task|process)\s+(?:is\s+)?(?:running|pending|queued)\b", re.IGNORECASE)),
    ("job_state_active", re.compile(r"(?:作业|job)\s*(?:状态|state|status)\s*[:：=]?\s*(?:RUNNING|PENDING|PD\b)", re.IGNORECASE)),
)

_BG_FINISHED_OVERRIDE_RE = re.compile(
    r"(?:作业|任务|job)\s*\d*\s*(?:已|已经)?(?:全部)?(?:运行|执行)?(?:完成|结束|跑完|成功)",
    re.IGNORECASE,
)

_JOB_ID_RE = re.compile(
    r"(?:Submitted\s+batch\s+job|Slurm\s*(?:作业|任务|job)|(?:作业|job)\s*(?:ID|编号|号)?)\s*[:：=#]?\s*(\d{4,12})",
    re.IGNORECASE,
)

_FENCE_BLOCK_RE = re.compile(r"```.*?```", re.DOTALL)
_HERMES_TAG_LINE_RE = re.compile(r"^\[via\s+Hermes[^\]]*\]\s*$", re.IGNORECASE)
_INLINE_HERMES_TAG_RE = re.compile(r"^\[via\s+Hermes[^\]]*\]\s*", re.IGNORECASE)
_MD_HEADING_PREFIX_RE = re.compile(r"^#{1,6}\s*")
_MD_BULLET_PREFIX_RE = re.compile(r"^(?:[-*+•]\s+|\d+[.)]\s+)")

_FILLER_PREFIX_RE = re.compile(
    r"^(?:好的|收到|没问题|明白了|了解|我先|我来|让我|接下来|下面我将|首先让我|现在我将|"
    r"ok|okay|sure|got it|let me|i will|i'll|next,|now let|first,)\b",
    re.IGNORECASE,
)

_ACTION_VERB_RE = re.compile(
    r"(?:新增|添加|修改|修复|更新|重构|实现|编写|补全|优化|部署|安装|创建|删除|移除|配置|清理|拆分|接入|启用|加固|"
    r"added|modified|fixed|updated|refactored|implemented|created|wrote|built|installed|configured|removed|enabled)",
    re.IGNORECASE,
)

_VERIFICATION_RE = re.compile(
    r"(?:\bRan\s+\d+\s+tests?\b|\b\d+\s+passed\b|\b\d+\s+failed\b|\b\d+/\d+\s*(?:passed|通过)|\bpytest\b|\bunittest\b|"
    r"\bnpm\s+test\b|\bverify_writers\b|测试通过|验证通过|全量测试|全部通过|编译通过|构建成功|无报错|"
    r"exit(?:_code)?\s*[:=]\s*0\b|ready\s*[:=]\s*true|returncode\s*[:=]\s*0)",
    re.IGNORECASE,
)

_ARTIFACT_RE = re.compile(
    r"(?:\.vsix\b|Built\s+[^\s]+|\bgit\s+commit\b|\bcommit\s+[0-9a-f]{7,12}\b|"
    r"产出|产物|生成了|已生成|已导出|已保存|已安装包|打包|artifact|deliverable|"
    r"http://127\.0\.0\.1:\d+)",
    re.IGNORECASE,
)

_FAILURE_ISSUE_RE = re.compile(
    r"(?:\bFAILED\b|\bAssertionError\b|\bTraceback\b|waiting_approval|需要手动|需人工|需重启|尚待|未完成|"
    r"\bTODO\b|\bFIXME\b|\bblocked\b|失败|报错)",
    re.IGNORECASE,
)
_RESOLVED_ISSUE_RE = re.compile(
    r"(?:已修复|修复了|已解决|解决了|避免|防止|不再|prevent|avoid|resolved|fixed\b|0\s*failures|0\s*errors)",
    re.IGNORECASE,
)

_FILE_PATH_RE = re.compile(
    r"(?<![a-zA-Z0-9_://])"
    r"((?:/(?:Users|dshare|flash|tmp|opt|etc|Applications)/[^\s`'\"),;:\]>]+)|"
    r"(?:[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.@-]+)+\.(?:py|js|mjs|cjs|ts|tsx|jsx|sh|bash|zsh|md|json|jsonl|yaml|yml|toml|sql|r|R|csv|tsv|pdf|png|svg|vsix|txt|log|html|css|plist|conf))|"
    r"(?:[A-Za-z0-9_.-]+\.(?:py|js|mjs|ts|tsx|sh|md|json|yaml|yml|sql|R|csv|tsv|pdf|png|svg|vsix)))",
)

_CODEX_FILECHANGE_RE = re.compile(r"^\[fileChange\s+status=([^\]]*)\]\s*(.+)$")
_CODEX_CMD_RE = re.compile(r"^\[command\s+status=([^\s\]]+)\s+exit=([^\]]*)\]\s*(.+)$")
_TOOL_CALL_TAG_RE = re.compile(r"^\[(tool-call|tool_use|tool_calls|tool/result)[:\s]+([^\]]+)\]\s*(.*)$")


def detect_background_job(text: str) -> dict[str, Any]:
    """Detect async/Slurm background job submissions or active background work in text."""
    body = str(text or "")
    if not body.strip():
        return {"matched": False, "kind": "", "label": "", "job_ids": [], "signals": []}

    running_signals = [name for name, pat in _BG_RUNNING_PATTERNS if pat.search(body)]
    submitted_signals = [name for name, pat in _BG_SUBMITTED_PATTERNS if pat.search(body)]

    if not running_signals and not submitted_signals:
        return {"matched": False, "kind": "", "label": "", "job_ids": [], "signals": []}

    if submitted_signals and not running_signals and _BG_FINISHED_OVERRIDE_RE.search(body):
        return {"matched": False, "kind": "", "label": "", "job_ids": [], "signals": []}

    job_ids: list[str] = []
    seen_ids: set[str] = set()
    for match in _JOB_ID_RE.finditer(body):
        jid = match.group(1)
        if jid not in seen_ids:
            seen_ids.add(jid)
            job_ids.append(jid)

    if running_signals:
        kind = "running_background"
        label = BG_LABEL_RUNNING
    else:
        kind = "submitted"
        label = BG_LABEL_SUBMITTED

    signals = running_signals + [s for s in submitted_signals if s not in running_signals]
    return {
        "matched": True,
        "kind": kind,
        "label": label,
        "job_ids": job_ids,
        "signals": signals,
    }


def _meta_to_dict(meta: SessionSummary | dict[str, Any] | None) -> dict[str, Any]:
    if meta is None:
        return {}
    if isinstance(meta, SessionSummary):
        return meta.to_dict()
    if isinstance(meta, dict):
        return dict(meta)
    return {
        "app": str(getattr(meta, "app", "") or ""),
        "session_id": str(getattr(meta, "session_id", "") or ""),
        "title": str(getattr(meta, "title", "") or ""),
        "workspace": str(getattr(meta, "workspace", "") or ""),
        "status": str(getattr(meta, "status", "") or ""),
        "status_detail": str(getattr(meta, "status_detail", "") or ""),
        "updated_at": str(getattr(meta, "updated_at", "") or ""),
        "preview": str(getattr(meta, "preview", "") or ""),
    }


def _strip_hermes_prefix(text: str) -> str:
    lines = str(text or "").replace("\r\n", "\n").splitlines()
    out: list[str] = []
    skipping_header = True
    for line in lines:
        stripped = line.strip()
        if skipping_header:
            if not stripped:
                continue
            if _HERMES_TAG_LINE_RE.match(stripped):
                continue
            cleaned = _INLINE_HERMES_TAG_RE.sub("", stripped).strip()
            skipping_header = False
            if cleaned:
                out.append(cleaned)
            continue
        out.append(line)
    return "\n".join(out).strip()


def _extract_objective(user_text: str, assistant_texts: list[str], meta: dict[str, Any]) -> str:
    cleaned = _strip_hermes_prefix(user_text)
    if cleaned:
        lines = [ln.strip() for ln in cleaned.splitlines() if ln.strip()]
        picked_parts: list[str] = []
        for ln in lines:
            if ln.startswith("```") or ln in ("---", "==="):
                continue
            ln_clean = _MD_HEADING_PREFIX_RE.sub("", ln)
            ln_clean = _MD_BULLET_PREFIX_RE.sub("", ln_clean).replace("**", "").strip()
            if not ln_clean:
                continue
            picked_parts.append(ln_clean)
            if len(" | ".join(picked_parts)) >= 90 or (len(ln_clean) >= 24 and not ln_clean.endswith(("：", ":"))):
                break
        if picked_parts:
            return clean_single_line(" — ".join(picked_parts) if len(picked_parts) > 1 and picked_parts[0].endswith(("：", ":")) else picked_parts[0], 220)

    for txt in assistant_texts:
        for ln in txt.splitlines():
            m = re.match(r"^(?:[-*+]\s*)?(?:\*\*)?(?:任务目标|本轮目标|目标|Objective|Goal)(?:\*\*)?\s*[:：]\s*(.+)$", ln.strip(), re.IGNORECASE)
            if m and m.group(1).strip():
                return clean_single_line(m.group(1).strip().replace("**", ""), 220)

    fallback = str(meta.get("title") or meta.get("preview") or "").strip()
    return clean_single_line(fallback, 220) if fallback else "（未识别到明确任务目标）"


def _add_unique(items: list[str], candidate: str, *, max_len: int = 160) -> None:
    norm = clean_single_line(candidate.replace("**", ""), max_len=max_len)
    if not norm or len(norm) < 3:
        return
    key = re.sub(r"[\s`*_.:：;；,，-]+", "", norm).lower()
    if not key:
        return
    for existing in items:
        ek = re.sub(r"[\s`*_.:：;；,，-]+", "", existing).lower()
        if key == ek or (len(key) >= 12 and (key in ek or ek in key)):
            return
    items.append(norm)


def _extract_file_paths(text: str, *, limit: int = 12) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for match in _FILE_PATH_RE.finditer(text or ""):
        cand = match.group(1).rstrip(".,;:)]}>'\"")
        if not cand or "://" in cand or cand.startswith("...") or cand.endswith("/..."):
            continue
        low = cand.lower()
        if low in ("e.g.", "i.e.", "v1.0", "v2.0") or low.startswith(("http", "www.")):
            continue
        if cand not in seen:
            seen.add(cand)
            paths.append(cand)
            if len(paths) >= limit:
                break
    return paths


def _is_meaningful_command(cmd: str) -> bool:
    c = (cmd or "").strip().lower()
    if not c:
        return False
    noisy_prefixes = ("ls ", "ls\n", "cat ", "head ", "tail ", "rg ", "grep ", "find ", "pwd", "echo ", "wc ")
    if c == "ls" or c.startswith(noisy_prefixes):
        return False
    return True


def extract_task_completion_summary(
    adapter: AppAdapter | None = None,
    session_id: str = "",
    *,
    session_meta: SessionSummary | dict[str, Any] | None = None,
    messages: list[dict[str, Any]] | None = None,
    tail_messages: int = 16,
    max_chars_per_message: int = 4000,
    max_actions: int = 6,
    max_artifacts: int = 6,
) -> dict[str, Any]:
    """Extract a structured task completion summary & handover packet from recent messages."""
    meta = _meta_to_dict(session_meta)
    raw_messages: list[dict[str, Any]] = []

    if messages is not None:
        raw_messages = [dict(m) for m in messages if isinstance(m, dict)]
    elif adapter is not None and session_id:
        rd = adapter.read_session(
            session_id,
            tail_messages=tail_messages,
            max_chars_per_message=max_chars_per_message,
            include_tools=True,
        )
        if "error" in rd and not meta:
            return {"error": rd["error"]}
        if isinstance(rd.get("session"), dict):
            meta = _meta_to_dict(rd["session"])
        raw_messages = [dict(m) for m in (rd.get("messages") or []) if isinstance(m, dict)]
        if "error" in rd and not raw_messages:
            return {"error": rd["error"]}

    # Slice to the last substantive round: last user turn -> end of transcript.
    last_user_idx: int | None = None
    for idx in range(len(raw_messages) - 1, -1, -1):
        m = raw_messages[idx]
        if str(m.get("role") or "").lower() == "user" and str(m.get("content") or "").strip():
            last_user_idx = idx
            break

    # If the very last message is a trailing user turn with zero assistant/tool responses after it,
    # fall back to the previous completed round when available.
    if last_user_idx is not None and last_user_idx == len(raw_messages) - 1 and last_user_idx > 0:
        for prev_idx in range(last_user_idx - 1, -1, -1):
            if str(raw_messages[prev_idx].get("role") or "").lower() == "user" and str(raw_messages[prev_idx].get("content") or "").strip():
                last_user_idx = prev_idx
                break

    round_msgs = raw_messages[last_user_idx:] if last_user_idx is not None else raw_messages
    trigger_user_text = (
        str(raw_messages[last_user_idx].get("content") or "")
        if last_user_idx is not None
        else ""
    )

    assistant_msgs = [
        str(m.get("content") or "").strip()
        for m in round_msgs
        if str(m.get("role") or "").lower() == "assistant" and str(m.get("content") or "").strip()
    ]
    tool_msgs = [
        str(m.get("content") or "").strip()
        for m in round_msgs
        if str(m.get("role") or "").lower() == "tool" and str(m.get("content") or "").strip()
    ]

    objective = _extract_objective(trigger_user_text, assistant_msgs, meta)

    actions: list[str] = []
    verification: list[str] = []
    artifacts: list[str] = []
    open_issues: list[str] = []
    modified_files: list[str] = []

    def _record_files(text: str) -> None:
        for fp in _extract_file_paths(text):
            if fp not in modified_files and len(modified_files) < 12:
                modified_files.append(fp)

    # 1. Inspect explicit tool records first (highest signal for real side effects & verification).
    for tbody in tool_msgs:
        _record_files(tbody)
        for line in tbody.splitlines():
            s = line.strip()
            if not s:
                continue
            fc_m = _CODEX_FILECHANGE_RE.match(s)
            if fc_m:
                _add_unique(actions, f"文件变更 ({fc_m.group(1)}): {fc_m.group(2)}")
                continue
            cmd_m = _CODEX_CMD_RE.match(s)
            if cmd_m:
                st, ex, cmd_txt = cmd_m.group(1).strip(), cmd_m.group(2).strip(), cmd_m.group(3).strip()
                if _VERIFICATION_RE.search(cmd_txt) or _VERIFICATION_RE.search(tbody):
                    _add_unique(verification, f"执行校验 `{clean_single_line(cmd_txt, 80)}` (status={st}, exit={ex})")
                elif _is_meaningful_command(cmd_txt):
                    _add_unique(actions, f"执行命令 `{clean_single_line(cmd_txt, 80)}` (exit={ex})")
                continue
            tc_m = _TOOL_CALL_TAG_RE.match(s)
            if tc_m:
                tname = tc_m.group(2).strip()
                rest = tc_m.group(3).strip()
                if any(k in tname.lower() for k in ("write", "edit", "replace", "apply", "patch", "run_command", "bash", "execute")):
                    detail = f": {clean_single_line(rest, 90)}" if rest else ""
                    _add_unique(actions, f"工具调用 `{tname}`{detail}")
            if _VERIFICATION_RE.search(s):
                _add_unique(verification, s)

    # 2. Inspect assistant responses (stripping fenced code blocks for prose bullets, keeping fenced metrics out of noise).
    for atxt in assistant_msgs:
        _record_files(atxt)
        prose = _FENCE_BLOCK_RE.sub("\n", atxt)
        section_hint = ""
        for raw_ln in prose.splitlines():
            s = raw_ln.strip()
            if not s:
                continue
            if s.startswith("#"):
                heading_txt = _MD_HEADING_PREFIX_RE.sub("", s).replace("**", "").strip()
                section_hint = heading_txt.lower()
                if heading_txt in ("任务目标", "【任务目标】", "实质变更与动作", "【实质变更与动作】", "验证结果与产物", "【验证结果与产物】"):
                    continue
                if len(heading_txt) >= 8 and _ACTION_VERB_RE.search(heading_txt):
                    _add_unique(actions, heading_txt)
                continue

            is_bullet = bool(_MD_BULLET_PREFIX_RE.match(s))
            clean_ln = _MD_BULLET_PREFIX_RE.sub("", s).strip()
            if not clean_ln or len(clean_ln) < 5:
                continue
            if clean_ln.replace("*", "").strip() in ("【任务目标】", "【实质变更与动作】", "【验证结果与产物】", "任务目标", "实质变更与动作", "验证结果与产物"):
                continue

            # Classify line into verification / artifact / issue / action
            if _VERIFICATION_RE.search(clean_ln):
                _add_unique(verification, clean_ln)
                if "fail" in clean_ln.lower() and not _RESOLVED_ISSUE_RE.search(clean_ln):
                    _add_unique(open_issues, clean_ln)
                continue

            if _ARTIFACT_RE.search(clean_ln) or any(k in section_hint for k in ("产物", "交付", "output", "artifact")):
                _add_unique(artifacts, clean_ln)
                continue

            if _FAILURE_ISSUE_RE.search(clean_ln) and not _RESOLVED_ISSUE_RE.search(clean_ln):
                _add_unique(open_issues, clean_ln)

            if _FILLER_PREFIX_RE.match(clean_ln) and not _ACTION_VERB_RE.search(clean_ln):
                continue

            if is_bullet or _ACTION_VERB_RE.search(clean_ln) or any(k in section_hint for k in ("变更", "改动", "动作", "实现", "完成", "summary", "change", "done")):
                _add_unique(actions, clean_ln)

    combined_text = "\n".join(assistant_msgs + tool_msgs)
    bg_info = detect_background_job(combined_text)

    if len(actions) > max_actions:
        actions = actions[:max_actions]

    # Fallbacks to avoid empty sections while never fabricating test assertions.
    last_excerpt = clean_single_line(assistant_msgs[-1], 240) if assistant_msgs else ""
    if not actions:
        if modified_files:
            actions = [f"涉及/修改文件: {', '.join(modified_files[:4])}"]
        elif last_excerpt:
            actions = [f"完成本轮处理并输出结论: {last_excerpt}"]
        else:
            actions = ["已完成本轮推理与交互（未检测到独立工具修改）"]

    if modified_files and len(artifacts) < max_artifacts:
        file_summary = f"涉及文件: {', '.join(f'`{p}`' for p in modified_files[:5])}"
        if not any(modified_files[0] in a for a in artifacts):
            _add_unique(artifacts, file_summary, max_len=200)

    combined_results: list[str] = []
    for item in verification + artifacts:
        _add_unique(combined_results, item, max_len=160)
    combined_results = combined_results[:max_artifacts]
    if not combined_results:
        if bg_info.get("matched"):
            job_suffix = f" (job_ids={','.join(bg_info['job_ids'])})" if bg_info.get("job_ids") else ""
            combined_results = [f"{bg_info['label']}{job_suffix}"]
        else:
            combined_results = ["本轮未检测到显式测试或构建输出，请按需核查产出。"]

    summary: dict[str, Any] = {
        "app": str(meta.get("app") or ""),
        "session_id": str(meta.get("session_id") or session_id or ""),
        "title": str(meta.get("title") or meta.get("session_id") or session_id or ""),
        "workspace": str(meta.get("workspace") or ""),
        "status": str(meta.get("status") or STATUS_IDLE),
        "status_detail": str(meta.get("status_detail") or ""),
        "updated_at": str(meta.get("updated_at") or ""),
        "objective": objective,
        "actions": actions,
        "verification": verification[:max_artifacts],
        "artifacts": artifacts[:max_artifacts],
        "results": combined_results,
        "modified_files": modified_files,
        "open_issues": open_issues[:4],
        "background_job": bg_info,
        "last_assistant_excerpt": last_excerpt,
    }
    summary["markdown_card"] = format_completion_card(summary)
    summary["handover_prompt"] = format_handover_packet(summary)
    return summary


def format_completion_card(
    summary: dict[str, Any],
    *,
    watch: dict[str, Any] | None = None,
    live_meta: SessionSummary | None = None,
    label: str = "",
    reason: str = "",
    verdict: dict[str, Any] | None = None,
) -> str:
    """Format a compact, mobile-friendly Markdown completion summary card."""
    app = str(getattr(live_meta, "app", "") or summary.get("app") or "")
    title = str(getattr(live_meta, "title", "") or summary.get("title") or summary.get("session_id") or "")
    cur_status = str(getattr(live_meta, "status", "") or summary.get("status") or STATUS_IDLE)
    detail = str(getattr(live_meta, "status_detail", "") or summary.get("status_detail") or reason or "")

    alias = str((watch or {}).get("alias") or "").strip()
    display_name = f"{alias} ({title})" if alias and alias != title else title

    lines: list[str] = []
    if watch is not None or live_meta is not None or label:
        prev_status = str((watch or {}).get("last_status") or STATUS_IDLE)
        eff_label = (label or "").strip() or "回合完毕 / 等待输入"
        detail_suffix = f" ({detail})" if detail else ""
        lines.append(f"[Agent Hub 盯盘] {eff_label} | {app}「{display_name}」")
        lines.append(f"状态: {prev_status} -> {cur_status}{detail_suffix}")
        verdict_summary = (
            str(verdict.get("summary") or "").strip()
            if isinstance(verdict, dict) and verdict.get("ok")
            else ""
        )
        if verdict_summary:
            lines.append(f"研判结论: {verdict_summary}")
        lines.append("")
    else:
        ws = str(summary.get("workspace") or "").strip()
        ws_part = f" | `{ws}`" if ws else ""
        lines.append(f"### 履约总结卡片 | {app}「{display_name}」{ws_part}")
        lines.append("")

    objective = str(summary.get("objective") or "（未识别到明确任务目标）")
    actions = list(summary.get("actions") or [])
    results = list(summary.get("results") or summary.get("verification") or summary.get("artifacts") or [])

    lines.append("**【任务目标】**")
    lines.append(objective)
    lines.append("")
    lines.append("**【实质变更与动作】**")
    for item in actions[:6]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("**【验证结果与产物】**")
    for item in results[:6]:
        lines.append(f"- {item}")

    open_issues = list(summary.get("open_issues") or [])
    if open_issues:
        lines.append("")
        lines.append("**【待确认/风险】**")
        for item in open_issues[:3]:
            lines.append(f"- {item}")

    return "\n".join(lines).strip()


def format_handover_packet(summary: dict[str, Any], *, target_hint: str = "") -> str:
    """Format a structured lossless cross-agent handover Markdown packet."""
    app = str(summary.get("app") or "")
    sid = str(summary.get("session_id") or "")
    title = str(summary.get("title") or sid)
    workspace = str(summary.get("workspace") or "")
    status = str(summary.get("status") or "")

    lines = [
        "# 跨端任务交接包 (Agent Hub Handover)",
        f"- **来源会话**: `{app}:{sid}` 「{title}」 (status={status})",
    ]
    if workspace:
        lines.append(f"- **工作区**: `{workspace}`")
    if target_hint.strip():
        lines.append(f"- **交接目标要求**: {target_hint.strip()}")

    lines.extend(["", "## 【任务目标】", str(summary.get("objective") or "（未识别）"), "", "## 【已完成变更与动作】"])
    for item in (summary.get("actions") or ["（无记录）"]):
        lines.append(f"- {item}")

    lines.extend(["", "## 【验证结果与产物】"])
    for item in (summary.get("results") or summary.get("verification") or ["（无显式测试输出）"]):
        lines.append(f"- {item}")

    modified_files = list(summary.get("modified_files") or [])
    if modified_files:
        lines.extend(["", "## 【涉及文件清单】"])
        for fp in modified_files:
            lines.append(f"- `{fp}`")

    open_issues = list(summary.get("open_issues") or [])
    if open_issues:
        lines.extend(["", "## 【未决事项与风险】"])
        for iss in open_issues:
            lines.append(f"- {iss}")

    if summary.get("last_assistant_excerpt"):
        lines.extend(["", "## 【最近结论摘录】", str(summary["last_assistant_excerpt"])])

    return "\n".join(lines).strip()
