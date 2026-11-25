"""Unit and integration tests for agent_hub adapters, writers, audit guard, and MCP server."""

from __future__ import annotations

import asyncio
from compression import zstd
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
import unittest
from unittest.mock import patch

from agent_hub.adapters import AntigravityAdapter, ClineAdapter, CodexAdapter, DshAdapter
from agent_hub.audit import AuditLog
from agent_hub.bindings import BindingStore
from agent_hub.models import (
    STATUS_ERROR,
    STATUS_IDLE,
    STATUS_INTERRUPTED,
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
    STATUS_WAITING_USER,
    SessionSummary,
    VALID_APPS,
)
from agent_hub.relay import RelayBroker, RelayStore, canonical_message_hash, is_relay_echo
from agent_hub.server import HubService, _extract_last_code_block
from agent_hub.watcher import (
    BG_LABEL_RUNNING,
    BG_LABEL_SUBMITTED,
    WatchStore,
    WatchdogRunner,
    default_hermes_verify,
    detect_background_job,
    parse_verification_output,
)
from agent_hub.writers import AntigravityWriter, ClineWriter, CodexWriter, DshWriter, SessionMismatchError
from agent_hub.writers.cdp_writer import CdpWriter


VALID_STATUSES = {
    STATUS_RUNNING,
    STATUS_WAITING_APPROVAL,
    STATUS_WAITING_USER,
    STATUS_ERROR,
    STATUS_INTERRUPTED,
    STATUS_IDLE,
}


class _FakeWriter:
    """In-memory writer double for deterministic send/relay/create unit tests."""

    def __init__(self, app_name: str, *, ready: bool = True, supports_create: bool = False) -> None:
        self.app_name = app_name
        self.ready = ready
        self.calls: list[dict] = []
        if supports_create:
            self.create_session = self._create_session  # type: ignore[method-assign]

    def status(self) -> dict:
        if self.ready:
            return {"app": self.app_name, "ready": True, "transport": "fake"}
        return {"app": self.app_name, "ready": False, "transport": "fake", "detail": "fake offline"}

    def send_message(self, session_id: str, text: str, *, mode: str = "queue", target_title: str = "") -> dict:
        self.calls.append({"action": "send", "session_id": session_id, "text": text, "mode": mode, "title": target_title})
        return {"ok": True, "mode": mode, "sessionId": session_id, "requestId": f"req-{len(self.calls)}"}

    def _create_session(self, *, cwd: str = "", title: str = "", agent_preset: str = "") -> dict:
        self.calls.append({"action": "create", "cwd": cwd, "title": title})
        return {"ok": True, "sessionId": "fake-created-session-1", "cwd": cwd or "/tmp"}


class TestLiveReadOnlyAdapters(unittest.TestCase):
    """Verify all 4 live read-only adapters against local macOS app data."""

    def test_all_four_adapters_list_fast_and_valid(self) -> None:
        adapters = {
            "codex": CodexAdapter(),
            "dsh": DshAdapter(),
            "antigravity": AntigravityAdapter(),
            "cline": ClineAdapter(),
        }
        t0 = time.monotonic()
        for app_name, adapter in adapters.items():
            sessions = adapter.list_sessions(limit=5)
            self.assertGreater(len(sessions), 0, f"{app_name} returned 0 sessions")
            for s in sessions:
                self.assertEqual(s.app, app_name)
                self.assertTrue(s.session_id)
                self.assertTrue(s.title)
                self.assertIn(s.status, VALID_STATUSES)
                self.assertGreater(s.updated_at_ms, 0)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 2.0, f"Listing all 4 apps took too long: {elapsed:.2f}s")

    def test_read_session_returns_messages_for_each_app(self) -> None:
        adapters = {
            "codex": CodexAdapter(),
            "dsh": DshAdapter(),
            "antigravity": AntigravityAdapter(),
            "cline": ClineAdapter(),
        }
        for app_name, adapter in adapters.items():
            sessions = adapter.list_sessions(limit=10)
            self.assertGreater(len(sessions), 0)
            found_msgs = False
            for s in sessions[:5]:
                res = adapter.read_session(s.session_id, tail_messages=4, max_chars_per_message=500)
                self.assertNotIn("error", res)
                self.assertEqual(res["session"]["session_id"], s.session_id)
                if res["messages"]:
                    found_msgs = True
                    for m in res["messages"]:
                        self.assertIn(m["role"], ("user", "assistant", "tool"))
                        self.assertTrue(m["content"])
                        self.assertLessEqual(len(m["content"]), 520)
                    break
            self.assertTrue(found_msgs, f"{app_name} had no readable messages in top 5 sessions")

    def test_search_sessions_ascii_cjk_and_punctuation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            svc = HubService(
                bindings=BindingStore(Path(td) / "bindings.json"),
                audit=AuditLog(Path(td) / "audit.jsonl"),
            )
            for q in ("python", "测试", 'foo-bar:baz/123 "quoted"'):
                res = svc.search_sessions(q, app="all", limit=10)
                self.assertNotIn("error", res)
                self.assertNotIn("adapter_errors", res, f"Query {q!r} raised adapter errors: {res}")

    def test_antigravity_truncation_hydration(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            logs = base / "brain" / "conv-1" / ".system_generated" / "logs"
            logs.mkdir(parents=True)
            (logs / "transcript.jsonl").write_text(
                json.dumps(
                    {
                        "step_index": 0,
                        "type": "USER_INPUT",
                        "content": "short...",
                        "truncated_fields": ["content"],
                        "created_at": "2026-11-23T09:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (logs / "transcript_full.jsonl").write_text(
                json.dumps(
                    {
                        "step_index": 0,
                        "type": "USER_INPUT",
                        "content": "FULL_UNTRUNCATED_PROMPT_BODY",
                        "created_at": "2026-11-23T09:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            adapter = AntigravityAdapter(antigravity_dir=base)
            res = adapter.read_session("conv-1", tail_messages=5)
            self.assertEqual(len(res["messages"]), 1)
            self.assertEqual(res["messages"][0]["content"], "FULL_UNTRUNCATED_PROMPT_BODY")


class TestBindingsAuditWriteAndRelay(unittest.TestCase):
    """Verify BindingStore, AuditLog duplicate guard, send, relay, and create paths."""

    def test_bind_resolve_list_unbind_in_temp_store(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bpath = Path(td) / "bindings.json"
            svc = HubService(bindings=BindingStore(bpath), audit=AuditLog(Path(td) / "audit.jsonl"))

            codex_top = svc.list_sessions(app="codex", limit=1)["sessions"][0]
            sid = codex_top["session_id"]

            bind_res = svc.bind_task("测试pipeline", "codex", sid, note="unit test")
            self.assertTrue(bind_res.get("ok"))
            self.assertTrue(bpath.exists())

            listed = svc.list_bindings()
            self.assertEqual(listed["count"], 1)
            self.assertEqual(listed["bindings"][0]["alias"], "测试pipeline")
            self.assertIn(listed["bindings"][0]["live_status"], VALID_STATUSES)

            read1 = svc.read_session("alias", "测试pipeline", tail_messages=2)
            self.assertEqual(read1["session"]["session_id"], sid)
            self.assertIn("测试pipeline", read1["session"]["bound_aliases"])

            read2 = svc.read_session("all", "alias:测试pipeline", tail_messages=2)
            self.assertEqual(read2["session"]["session_id"], sid)

            sres = svc.search_sessions("测试pipeline", app="all", limit=5)
            self.assertGreaterEqual(sres["count"], 1)
            self.assertEqual(sres["sessions"][0]["session_id"], sid)
            self.assertEqual(sres["sessions"][0].get("matched_by"), "binding:测试pipeline")

            ov = svc.overview(limit_per_app=2)
            self.assertEqual(set(ov["apps"].keys()), set(VALID_APPS))
            self.assertEqual(len(ov["bindings"]), 1)

            ub = svc.unbind_task("测试pipeline")
            self.assertTrue(ub["ok"])
            self.assertEqual(svc.list_bindings()["count"], 0)

    def test_audit_duplicate_guard_and_allow_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            audit = AuditLog(Path(td) / "audit.jsonl")
            h = audit.content_hash("hello world")
            self.assertIsNone(audit.check_duplicate("dsh", "s1", h))
            rec = audit.record(
                {
                    "action": "send",
                    "status": "sent",
                    "dry_run": False,
                    "target_app": "dsh",
                    "target_session_id": "s1",
                    "text_hash": h,
                }
            )
            self.assertTrue(rec["audit_id"])
            dup = audit.check_duplicate("dsh", "s1", h, window_seconds=60)
            self.assertIsNotNone(dup)
            self.assertEqual(dup["audit_id"], rec["audit_id"])
            self.assertIsNone(audit.check_duplicate("dsh", "s2", h, window_seconds=60))

    def test_send_relay_and_create_with_fake_writers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fake_dsh = _FakeWriter("dsh", ready=True, supports_create=True)
            fake_codex = _FakeWriter("codex", ready=False)
            svc = HubService(
                bindings=BindingStore(Path(td) / "bindings.json"),
                audit=AuditLog(Path(td) / "audit.jsonl"),
                writers={
                    "dsh": fake_dsh,
                    "codex": fake_codex,
                    "antigravity": _FakeWriter("antigravity", ready=False),
                    "cline": _FakeWriter("cline", ready=False),
                },
            )

            ws = svc.writer_status()
            self.assertEqual(set(ws["writers"].keys()), set(VALID_APPS))
            self.assertTrue(ws["writers"]["dsh"]["ready"])
            self.assertFalse(ws["writers"]["codex"]["ready"])

            dsh_top = svc.list_sessions(app="dsh", limit=1)["sessions"][0]["session_id"]
            codex_sessions = svc.list_sessions(app="codex", limit=5)["sessions"]
            codex_src = None
            for cs in codex_sessions:
                rd = svc.read_session("codex", cs["session_id"], tail_messages=5)
                if any(m["role"] == "assistant" for m in rd.get("messages", [])):
                    codex_src = cs["session_id"]
                    break
            self.assertIsNotNone(codex_src)

            svc.bind_task("目标DSH", "dsh", dsh_top)
            svc.bind_task("来源Codex", "codex", codex_src)

            # 1. dry_run preview has zero side effects
            dry = svc.send_message("alias", "目标DSH", "测试预览", dry_run=True)
            self.assertTrue(dry["ok"])
            self.assertTrue(dry["dry_run"])
            self.assertEqual(len(fake_dsh.calls), 0)

            # 2. real send via fake writer + duplicate suppression + allow_duplicate override
            sent = svc.send_message("alias", "目标DSH", "执行一步", dry_run=False)
            self.assertTrue(sent["ok"])
            self.assertEqual(len(fake_dsh.calls), 1)
            self.assertIn("[via Hermes]", fake_dsh.calls[0]["text"])

            dup_err = svc.send_message("alias", "目标DSH", "执行一步", dry_run=False)
            self.assertIn("error", dup_err)

            forced = svc.send_message("alias", "目标DSH", "执行一步", dry_run=False, allow_duplicate=True)
            self.assertTrue(forced["ok"])
            self.assertEqual(len(fake_dsh.calls), 2)

            # 3. unready writer returns clean error
            codex_err = svc.send_message("alias", "来源Codex", "hi", dry_run=False)
            self.assertIn("error", codex_err)

            # 4. relay dry_run & self-loop rejection & real relay
            self_loop = svc.relay_message("dsh", dsh_top, "dsh", dsh_top, dry_run=True)
            self.assertIn("error", self_loop)

            relay_dry = svc.relay_message("alias", "来源Codex", "alias", "目标DSH", dry_run=True)
            self.assertTrue(relay_dry["ok"])
            self.assertTrue(relay_dry["dry_run"])
            self.assertIn("[via Hermes | relayed from codex:", relay_dry["message_preview"])

            relay_sent = svc.relay_message(
                "alias",
                "来源Codex",
                "alias",
                "目标DSH",
                prefix_instruction="请按如下内容继续：",
                dry_run=False,
            )
            self.assertTrue(relay_sent["ok"])
            self.assertEqual(len(fake_dsh.calls), 3)
            self.assertIn("请按如下内容继续：", fake_dsh.calls[2]["text"])

            # 5. create_session dry_run and unsupported app
            c_dry = svc.create_session("dsh", title="Smoke", dry_run=True)
            self.assertTrue(c_dry["ok"])
            c_unsup = svc.create_session("codex", title="Unsupported", dry_run=False)
            self.assertIn("error", c_unsup)

    def test_extract_last_code_block(self) -> None:
        sample = "说明如下：\n```text\nshort\n```\n最终提示词：\n```markdown\nLINE1\nLINE2\n```\n结束"
        body, extracted = _extract_last_code_block(sample)
        self.assertTrue(extracted)
        self.assertEqual(body, "LINE1\nLINE2")

        plain, ok = _extract_last_code_block("no code fences here")
        self.assertFalse(ok)
        self.assertEqual(plain, "no code fences here")

    def test_invalid_inputs_return_clean_errors(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            svc = HubService(bindings=BindingStore(Path(td) / "bindings.json"), audit=AuditLog(Path(td) / "audit.jsonl"))
            self.assertIn("error", svc.list_sessions(app="nonexistent"))
            self.assertIn("error", svc.read_session("codex", "does-not-exist-999"))
            self.assertIn("error", svc.read_session("alias", "unbound-alias"))
            self.assertIn("error", svc.bind_task("x", "codex", "does-not-exist-999"))


class _FakeAdapter:
    """In-memory adapter double for driving watchdog state transitions."""

    def __init__(self, app_name: str, summary: SessionSummary, last_msg: str = "step output") -> None:
        self.app_name = app_name
        self.summary = summary
        self.last_msg = last_msg

    def list_sessions(self, **kwargs):  # noqa: ANN003
        return [self.summary]

    def search_sessions(self, query: str, **kwargs):  # noqa: ANN003
        q = (query or "").strip().lower()
        if not q:
            return [self.summary]
        s = self.summary
        if q in s.session_id.lower() or q in s.title.lower() or q in s.preview.lower() or q in self.last_msg.lower():
            return [s]
        return []

    def get_session_meta(self, session_id: str) -> SessionSummary | None:
        return self.summary if session_id == self.summary.session_id else None

    def read_session(self, session_id: str, **kwargs):  # noqa: ANN003
        return {
            "session": self.summary.to_dict(),
            "message_count_returned": 1,
            "messages": [{"index": 1, "role": "assistant", "content": self.last_msg}],
        }


class TestWatchdog(unittest.TestCase):
    """Verify WatchStore CRUD, all state transitions, cooldown, and HubService watch tools."""

    def test_watch_store_crud_and_hub_service_tools(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bstore = BindingStore(Path(td) / "bindings.json")
            wstore = WatchStore(Path(td) / "watches.json")
            astore = AuditLog(Path(td) / "audit.jsonl")
            fresh_meta = SessionSummary(
                app="codex",
                session_id="s-crud-1",
                title="Fresh Codex Task",
                status=STATUS_RUNNING,
                updated_at_ms=int(time.time() * 1000),
            )
            svc = HubService(
                adapters={"codex": _FakeAdapter("codex", fresh_meta)},
                bindings=bstore,
                audit=astore,
                watches=wstore,
            )

            sid = "s-crud-1"
            add1 = svc.watch_add("codex", sid, alias="主pipeline", notify_targets="qqbot,weixin", stall_minutes=10)
            self.assertTrue(add1["ok"])
            wid = add1["watch"]["watch_id"]

            # Upserting same (app, session_id) updates without duplicate entries
            add2 = svc.watch_add("codex", sid, alias="主pipeline", notify_targets="qqbot", stall_minutes=20)
            self.assertTrue(add2["ok"])
            self.assertEqual(add2["watch"]["watch_id"], wid)

            wl = svc.watch_list()
            self.assertEqual(wl["count"], 1)
            self.assertEqual(wl["watches"][0]["stall_minutes"], 20)
            self.assertEqual(wl["watches"][0]["live_status"], STATUS_RUNNING)

            chk = svc.watch_check_now(dry_run=True)
            self.assertEqual(chk["checked_count"], 1)
            self.assertEqual(chk["event_count"], 0)

            rm = svc.watch_remove("主pipeline")
            self.assertTrue(rm["ok"])
            self.assertEqual(svc.watch_list()["count"], 0)

    def test_watchdog_transitions_dedupe_and_stall_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []

            def fake_notifier(target: str, msg: str) -> tuple[bool, str]:
                sent.append((target, msg))
                return True, "ok"

            now = 2_000_000_000.0
            meta = SessionSummary(
                app="codex",
                session_id="s-watch-1",
                title="Watch Target",
                status=STATUS_RUNNING,
                updated_at_ms=int(now * 1000),
            )
            fake_adapter = _FakeAdapter("codex", meta, last_msg="running step 5")
            runner = WatchdogRunner(adapters={"codex": fake_adapter}, store=wstore, notifier=fake_notifier)

            wstore.upsert(
                {
                    "alias": "测试任务",
                    "app": "codex",
                    "session_id": "s-watch-1",
                    "title": "Watch Target",
                    "notify_targets": ["qqbot"],
                    "stall_minutes": 10,
                    "cooldown_minutes": 10,
                    "notify_on": ["completed", "waiting_approval", "waiting_user", "error", "interrupted", "stalled"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                    "last_updated_at_ms": int(now * 1000),
                }
            )

            # 1. Healthy running -> no event
            r0 = runner.poll_once(now_ts=now + 30)
            self.assertEqual(r0["event_count"], 0)

            # 2. Stalled after 11m -> fires once; immediate repeat inside cooldown suppressed
            r1 = runner.poll_once(now_ts=now + 11 * 60)
            self.assertEqual(r1["event_count"], 1)
            self.assertEqual(r1["events"][0]["event"], "stalled")
            self.assertEqual(len(sent), 1)

            r2 = runner.poll_once(now_ts=now + 12 * 60)
            self.assertEqual(r2["event_count"], 0)

            # 3. waiting_user -> fires once (past 10m cooldown)
            meta.status = STATUS_WAITING_USER
            meta.updated_at_ms = int((now + 22 * 60) * 1000)
            r3 = runner.poll_once(now_ts=now + 22 * 60)
            self.assertEqual(r3["events"][0]["event"], "waiting_user")

            # 4. waiting_approval -> fires once (past 10m cooldown)
            meta.status = STATUS_WAITING_APPROVAL
            r4 = runner.poll_once(now_ts=now + 33 * 60)
            self.assertEqual(r4["events"][0]["event"], "waiting_approval")

            # 5. error -> fires once, then stays silent on next tick
            meta.status = STATUS_ERROR
            meta.status_detail = "boom"
            r5 = runner.poll_once(now_ts=now + 44 * 60)
            self.assertEqual(r5["events"][0]["event"], "error")
            r5b = runner.poll_once(now_ts=now + 44 * 60 + 25)
            self.assertEqual(r5b["event_count"], 0)

            # 6. Back to running -> completed (running -> idle) fires once, then idle stays silent
            meta.status = STATUS_RUNNING
            meta.status_detail = ""
            meta.updated_at_ms = int((now + 55 * 60) * 1000)
            runner.poll_once(now_ts=now + 55 * 60)
            meta.status = STATUS_IDLE
            r6 = runner.poll_once(now_ts=now + 56 * 60)
            self.assertEqual(r6["events"][0]["event"], "completed")
            self.assertIn("回合完毕", sent[-1][1])
            r6b = runner.poll_once(now_ts=now + 56 * 60 + 25)
            self.assertEqual(r6b["event_count"], 0)

    def test_dsh_timestamp_uses_max_of_last_prompt_mtime_and_stats(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            harness = Path(td)
            pc_dir = harness / "storages" / "session_projcache" / "sessions"
            pc_dir.mkdir(parents=True)

            now = time.time()
            old_prompt_ms = int((now - 1800) * 1000)  # 30m ago
            fresh_stat_ms = int((now - 15) * 1000)    # 15s ago
            pc_path = pc_dir / "session-dsh-ts.json"
            pc_path.write_text(
                json.dumps(
                    {
                        "record": {
                            "identity": {"cwd": "/tmp/dsh"},
                            "rows": {
                                "title": {"val": "Long DSH Run"},
                                "sessionListMetadata": {"val": {"lastPromptAt": old_prompt_ms}},
                                "sessionStats": {
                                    "val": {
                                        "openStep": {"turn": 1, "step": 12, "startTime": fresh_stat_ms},
                                        "pendingCalls": {},
                                    }
                                },
                                "turnOutline": {
                                    "val": {
                                        "turns": [{"turn": 1, "prompt": "Run heavy matrix job", "response": ""}]
                                    }
                                },
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            # Backdate file mtime to match old_prompt_ms to prove sessionStats timestamp also lifts ts_ms
            os.utime(pc_path, (now - 1800, now - 1800))

            adapter = DshAdapter(harness_dir=harness)
            meta = adapter.get_session_meta("session-dsh-ts")
            self.assertIsNotNone(meta)
            self.assertEqual(meta.status, STATUS_RUNNING)
            self.assertGreaterEqual(meta.updated_at_ms, fresh_stat_ms)

            # Also verify mtime lifts ts_ms when sessionStats/turnOutline have no timestamps
            pc_mtime_path = pc_dir / "session-dsh-mtime.json"
            pc_mtime_path.write_text(
                json.dumps(
                    {
                        "record": {
                            "identity": {"cwd": "/tmp/dsh"},
                            "rows": {
                                "title": {"val": "Mtime Active DSH"},
                                "sessionListMetadata": {"val": {"lastPromptAt": old_prompt_ms}},
                                "sessionStats": {"val": {"openStep": None, "pendingCalls": {"c1": 0}}},
                                "approvalState": {"val": {"turnOpen": True}},
                                "turnOutline": {"val": {"turns": [{"turn": 1, "prompt": "Compile", "response": ""}]}},
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            os.utime(pc_mtime_path, (now - 20, now - 20))
            meta_mtime = adapter.get_session_meta("session-dsh-mtime")
            self.assertIsNotNone(meta_mtime)
            self.assertGreater(meta_mtime.updated_at_ms, old_prompt_ms)

            # Neither should trigger a false 'stalled' alert with stall_minutes=15
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []
            runner = WatchdogRunner(
                adapters={"dsh": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
            )
            wstore.upsert(
                {
                    "alias": "DSH长跑",
                    "app": "dsh",
                    "session_id": "session-dsh-ts",
                    "stall_minutes": 15,
                    "cooldown_minutes": 15,
                    "notify_targets": ["weixin"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            wstore.upsert(
                {
                    "alias": "DSH文件活跃",
                    "app": "dsh",
                    "session_id": "session-dsh-mtime",
                    "stall_minutes": 15,
                    "cooldown_minutes": 15,
                    "notify_targets": ["weixin"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            res = runner.poll_once(now_ts=now)
            self.assertEqual(res["event_count"], 0)
            self.assertEqual(sent, [])

    def test_background_job_detection_refines_labels(self) -> None:
        bg_sub = detect_background_job("已提交 Slurm 作业 21356050（sbatch run_all.sh），可通过 squeue 查看。")
        self.assertTrue(bg_sub["matched"])
        self.assertEqual(bg_sub["kind"], "submitted")
        self.assertEqual(bg_sub["label"], BG_LABEL_SUBMITTED)
        self.assertEqual(bg_sub["job_ids"], ["21356050"])

        bg_run = detect_background_job("Submitted batch job 21356050，当前正在后台运行，等待集群出结果。")
        self.assertTrue(bg_run["matched"])
        self.assertEqual(bg_run["kind"], "running_background")
        self.assertEqual(bg_run["label"], BG_LABEL_RUNNING)

        bg_done = detect_background_job("此前通过 sbatch 提交的 Slurm 作业 21356050 已全部运行完成，结果表已生成。")
        self.assertFalse(bg_done["matched"])

        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []
            meta = SessionSummary(
                app="codex",
                session_id="s-bg-1",
                title="Cluster Job",
                status=STATUS_IDLE,
                updated_at_ms=2_000_000_000_000,
            )
            adapter = _FakeAdapter("codex", meta, last_msg="Submitted batch job 21356050，已提交 Slurm 作业，正在后台运行，等待出结果。")
            runner = WatchdogRunner(
                adapters={"codex": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
            )
            wstore.upsert(
                {
                    "alias": "集群实验",
                    "app": "codex",
                    "session_id": "s-bg-1",
                    "notify_targets": ["weixin"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            res = runner.poll_once(now_ts=2_000_000_100.0)
            self.assertEqual(res["event_count"], 1)
            self.assertEqual(res["events"][0]["label"], BG_LABEL_RUNNING)
            self.assertIn(BG_LABEL_RUNNING, sent[0][1])
            self.assertNotIn("任务完成", sent[0][1])

            # Explicit completion turns back to neutral Turn Complete label (past 15m cooldown)
            adapter.last_msg = "此前通过 sbatch 提交的 Slurm 作业 21356050 已全部运行完成。"
            meta.status = STATUS_RUNNING
            meta.updated_at_ms = 2_000_001_900_000
            runner.poll_once(now_ts=2_000_001_900.0)
            meta.status = STATUS_IDLE
            res2 = runner.poll_once(now_ts=2_000_002_000.0)
            self.assertEqual(res2["events"][0]["label"], "回合完毕 / 等待输入")
            self.assertIn("回合完毕 / 等待输入", sent[1][1])

    def test_two_stage_evaluator_verifies_suppresses_and_fails_open(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []
            verify_calls: list[dict] = []
            now = 2_000_000_000.0

            meta = SessionSummary(
                app="dsh",
                session_id="s-eval-1",
                title="Slurm Eval",
                status=STATUS_IDLE,
                updated_at_ms=int(now * 1000),
            )
            adapter = _FakeAdapter("dsh", meta, last_msg="Submitted batch job 21356050 via sbatch")

            # Case A: Verified alert replaces snippet with high-confidence summary
            def verifier_a(ctx: dict) -> dict:
                verify_calls.append(ctx)
                return {
                    "ok": True,
                    "classification": "background_running",
                    "needs_attention": True,
                    "label": BG_LABEL_RUNNING,
                    "summary": "当前仅是提交了 Slurm 作业 21356050，任务并未结束，预计耗时 10 分钟",
                }

            runner_a = WatchdogRunner(
                adapters={"dsh": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
                verifier=verifier_a,
                verify_agent=True,
                suppress_routine=False,
            )
            wstore.upsert(
                {
                    "alias": "核验任务",
                    "app": "dsh",
                    "session_id": "s-eval-1",
                    "notify_targets": ["weixin"],
                    "stall_minutes": 10,
                    "cooldown_minutes": 10,
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                    "last_updated_at_ms": int(now * 1000),
                }
            )
            ra = runner_a.poll_once(now_ts=now + 10)
            self.assertEqual(len(verify_calls), 1)
            self.assertEqual(len(sent), 1)
            self.assertIn("研判结论: 当前仅是提交了 Slurm 作业 21356050，任务并未结束，预计耗时 10 分钟", sent[0][1])
            self.assertNotIn("最新进展:", sent[0][1])
            self.assertFalse(ra["events"][0]["suppressed"])

            # Case B: Routine suppression on stall + cooldown avoids re-invoking LLM every 25s
            verify_calls.clear()
            sent.clear()
            meta.status = STATUS_RUNNING
            meta.updated_at_ms = int((now + 60) * 1000)
            runner_b = WatchdogRunner(
                adapters={"dsh": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
                verifier=lambda ctx: (
                    verify_calls.append(ctx)
                    or {
                        "ok": True,
                        "classification": "background_running",
                        "needs_attention": False,
                        "label": BG_LABEL_RUNNING,
                        "summary": "后台 Slurm 作业正常运算中，无需人工介入",
                    }
                ),
                verify_agent=True,
                suppress_routine=True,
            )
            runner_b.poll_once(now_ts=now + 70)
            rb1 = runner_b.poll_once(now_ts=now + 12 * 60)
            self.assertEqual(len(verify_calls), 1)
            self.assertEqual(len(sent), 0)
            self.assertTrue(rb1["events"][0]["suppressed"])
            stored = wstore.list_all()[0]
            self.assertEqual(stored["last_verification"]["suppressed"], True)
            self.assertEqual(stored["last_verification"]["classification"], "background_running")

            # Subsequent poll 25s later does NOT re-call verifier due to stall cooldown
            rb2 = runner_b.poll_once(now_ts=now + 12 * 60 + 25)
            self.assertEqual(rb2["event_count"], 0)
            self.assertEqual(len(verify_calls), 1)

            # Case C: Fail-open when verifier raises an exception
            sent.clear()
            meta.status = STATUS_IDLE
            runner_c = WatchdogRunner(
                adapters={"dsh": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
                verifier=lambda _ctx: (_ for _ in ()).throw(RuntimeError("llm down")),
                verify_agent=True,
                suppress_routine=True,
            )
            rc = runner_c.poll_once(now_ts=now + 13 * 60)
            self.assertEqual(len(sent), 1)
            self.assertFalse(rc["events"][0]["suppressed"])
            self.assertFalse(rc["events"][0]["verification"]["ok"])

            # Case D: parse_verification_output & default_hermes_verify CLI flags
            fenced = parse_verification_output(
                '思考完成。\n```json\n{"classification":"background_running","needs_attention":false,"label":"【阶段汇报·后台作业运行中】","summary":"Slurm 21356050 仍在运行"}\n```'
            )
            self.assertTrue(fenced["ok"])
            self.assertFalse(fenced["needs_attention"])
            self.assertEqual(fenced["summary"], "Slurm 21356050 仍在运行")

            plain = parse_verification_output("纯文本回退研判结论")
            self.assertTrue(plain["ok"])
            self.assertTrue(plain["needs_attention"])
            self.assertEqual(plain["summary"], "纯文本回退研判结论")

            with patch("agent_hub.watcher.subprocess.run") as mock_run:
                mock_run.return_value = subprocess.CompletedProcess(
                    args=["hermes"],
                    returncode=0,
                    stdout='{"classification":"completed","needs_attention":true,"label":"【回合完毕】","summary":"已完成"}',
                    stderr="",
                )
                verdict = default_hermes_verify(
                    {"watch": {}, "event_type": "completed", "live_meta": meta},
                    model="gemini-2.5-flash",
                    reasoning="low",
                    hermes_bin="/usr/local/bin/hermes",
                )
                self.assertTrue(verdict["ok"])
                self.assertEqual(verdict["source"], "hermes-z")
                called_cmd = mock_run.call_args[0][0]
                self.assertEqual(called_cmd[:5], ["/usr/local/bin/hermes", "-m", "gemini-2.5-flash", "--reasoning", "low"])
                self.assertEqual(called_cmd[5:7], ["-t", "terminal"])
                self.assertEqual(called_cmd[7], "-z")

    def test_global_cooldown_blocks_same_and_alternating_events(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []
            t0 = 2_000_000_000.0
            meta = SessionSummary(
                app="cline",
                session_id="s-cool-1",
                title="Cooldown Test",
                status=STATUS_ERROR,
                status_detail="tool failed",
                updated_at_ms=int(t0 * 1000),
            )
            adapter = _FakeAdapter("cline", meta, last_msg="step failed")
            runner = WatchdogRunner(
                adapters={"cline": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
            )
            wstore.upsert(
                {
                    "alias": "冷却测试",
                    "app": "cline",
                    "session_id": "s-cool-1",
                    "notify_targets": ["weixin"],
                    "stall_minutes": 15,
                    "cooldown_minutes": 15,
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )

            # 1. running -> error fires at t0
            r0 = runner.poll_once(now_ts=t0)
            self.assertEqual(r0["event_count"], 1)
            self.assertEqual(r0["events"][0]["event"], "error")
            self.assertEqual(len(sent), 1)

            # Recover to running at t0 + 60s
            meta.status = STATUS_RUNNING
            meta.status_detail = ""
            runner.poll_once(now_ts=t0 + 60)

            # 2. Same-event repeat (running -> error) at t0 + 2m IS blocked by per-event cooldown
            meta.status = STATUS_ERROR
            r1 = runner.poll_once(now_ts=t0 + 120)
            self.assertEqual(r1["event_count"], 0)
            self.assertEqual(len(sent), 1)

            # 3. Distinct event (completed) at t0 + 3m MUST NOT be masked by prior error cooldown
            meta.status = STATUS_RUNNING
            runner.poll_once(now_ts=t0 + 150)
            meta.status = STATUS_IDLE
            r2 = runner.poll_once(now_ts=t0 + 180)
            self.assertEqual(r2["event_count"], 1)
            self.assertEqual(r2["events"][0]["event"], "completed")
            self.assertEqual(len(sent), 2)

            # 4. High-priority waiting_approval at t0 + 4m MUST NOT be masked by completed cooldown
            meta.status = STATUS_WAITING_APPROVAL
            meta.status_detail = "awaiting command confirm"
            r3 = runner.poll_once(now_ts=t0 + 240)
            self.assertEqual(r3["event_count"], 1)
            self.assertEqual(r3["events"][0]["event"], "waiting_approval")
            self.assertEqual(len(sent), 3)

            # 5. Same-event repeat waiting_approval within cooldown IS suppressed
            meta.status = STATUS_RUNNING
            runner.poll_once(now_ts=t0 + 270)
            meta.status = STATUS_WAITING_APPROVAL
            r3b = runner.poll_once(now_ts=t0 + 300)
            self.assertEqual(r3b["event_count"], 0)
            self.assertEqual(len(sent), 3)

            # 6. After 16m (>15m cooldown), running -> error fires cleanly again
            meta.status = STATUS_RUNNING
            meta.updated_at_ms = int((t0 + 15 * 60 + 30) * 1000)
            runner.poll_once(now_ts=t0 + 15 * 60 + 30)
            meta.status = STATUS_ERROR
            r4 = runner.poll_once(now_ts=t0 + 16 * 60)
            self.assertEqual(r4["event_count"], 1)
            self.assertEqual(r4["events"][0]["event"], "error")
            self.assertEqual(len(sent), 4)

    def test_debounce_suppresses_transient_flapping_and_confirms_persistent_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []
            t0 = 2_000_000_000.0
            meta = SessionSummary(
                app="cline",
                session_id="s-flap-1",
                title="Flap Test",
                status=STATUS_ERROR,
                status_detail="http 400",
                updated_at_ms=int(t0 * 1000),
            )
            runner = WatchdogRunner(
                adapters={"cline": _FakeAdapter("cline", meta)},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
                debounce_seconds=25,
            )
            wstore.upsert(
                {
                    "alias": "震荡防抖",
                    "app": "cline",
                    "session_id": "s-flap-1",
                    "notify_targets": ["weixin"],
                    "cooldown_minutes": 15,
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )

            # t0: first error observed -> held in pending_transition, 0 alerts
            r0 = runner.poll_once(now_ts=t0)
            self.assertEqual(r0["event_count"], 0)
            self.assertEqual(len(sent), 0)
            stored0 = wstore.list_all()[0]
            self.assertEqual(stored0["last_status"], STATUS_RUNNING)
            self.assertEqual(stored0["pending_transition"]["event"], "error")

            # t0 + 20: bounced back to running -> pending_transition cleared, 0 alerts
            meta.status = STATUS_RUNNING
            meta.status_detail = ""
            r1 = runner.poll_once(now_ts=t0 + 20)
            self.assertEqual(r1["event_count"], 0)
            self.assertEqual(len(sent), 0)
            self.assertNotIn("pending_transition", wstore.list_all()[0])

            # t0 + 50: persistent error starts -> pending
            meta.status = STATUS_ERROR
            r2 = runner.poll_once(now_ts=t0 + 50)
            self.assertEqual(r2["event_count"], 0)
            self.assertEqual(len(sent), 0)

            # t0 + 75 (>= 25s): still error -> confirmed and alerted once
            r3 = runner.poll_once(now_ts=t0 + 75)
            self.assertEqual(r3["event_count"], 1)
            self.assertEqual(r3["events"][0]["event"], "error")
            self.assertEqual(len(sent), 1)
            self.assertNotIn("pending_transition", wstore.list_all()[0])

    def test_cline_adapter_distinguishes_transient_tool_error_from_crash(self) -> None:
        from datetime import datetime, timezone

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cline_dir = root / "cline"
            db_dir = cline_dir / "data" / "db"
            db_dir.mkdir(parents=True)
            msg_dir = cline_dir / "data" / "sessions"
            msg_dir.mkdir(parents=True)

            now = time.time()
            old_ts = now - 7200
            old_iso = datetime.fromtimestamp(old_ts, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            recent_iso = datetime.fromtimestamp(now - 10, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

            m1 = msg_dir / "s1.json"
            m2 = msg_dir / "s2.json"
            m3 = msg_dir / "s3.json"
            m4 = msg_dir / "s4.json"
            for p in (m1, m2, m3, m4):
                p.write_text('{"messages":[]}', encoding="utf-8")

            # m2 fresh (now > old_ts ended_at); m3 at now-10 (within 60s grace, matching ended_at); m4 older than old_ts
            os.utime(m2, (now - 2, now - 2))
            os.utime(m3, (now - 10, now - 10))
            os.utime(m4, (old_ts - 60, old_ts - 60))

            con = sqlite3.connect(db_dir / "sessions.db")
            con.execute(
                """
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY, source TEXT, pid INTEGER, started_at TEXT,
                    ended_at TEXT, exit_code INTEGER, status TEXT, status_message TEXT,
                    cwd TEXT, workspace_root TEXT, prompt TEXT, metadata_json TEXT,
                    messages_path TEXT, transcript_path TEXT, updated_at TEXT,
                    is_subagent INTEGER, model TEXT, provider TEXT
                )
                """
            )
            rows = [
                ("s-null-end", "vscode", 101, old_iso, None, None, "failed", "", "/tmp", "/tmp", "p1", "{}", str(m1), "", recent_iso, 0, "m", "p"),
                ("s-newer-msg", "vscode", 102, old_iso, old_iso, 0, "failed", "", "/tmp", "/tmp", "p2", "{}", str(m2), "", old_iso, 0, "m", "p"),
                ("s-grace-win", "vscode", 103, old_iso, recent_iso, None, "failed", "", "/tmp", "/tmp", "p3", "{}", str(m3), "", recent_iso, 0, "m", "p"),
                ("s-true-crash", "vscode", 104, old_iso, old_iso, 1, "failed", "", "/tmp", "/tmp", "p4", "{}", str(m4), "", old_iso, 0, "m", "p"),
            ]
            con.executemany(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            con.commit()
            con.close()

            adapter = ClineAdapter(cline_dir=cline_dir, vscode_tasks_dir=root / "vsc_missing")
            s1 = adapter.get_session_meta("s-null-end")
            s2 = adapter.get_session_meta("s-newer-msg")
            s3 = adapter.get_session_meta("s-grace-win")
            s4 = adapter.get_session_meta("s-true-crash")

            for s in (s1, s2, s3):
                self.assertIsNotNone(s)
                self.assertEqual(s.status, STATUS_RUNNING)
                self.assertTrue(s.extra.get("transient_error"))

            self.assertIsNotNone(s4)
            self.assertEqual(s4.status, STATUS_ERROR)
            self.assertTrue(s4.extra.get("confirmed_terminal"))
            self.assertIn("exit_code=1", s4.status_detail)

    def test_level2_silences_transient_self_correcting_error(self) -> None:
        norm = parse_verification_output(
            '{"classification":"self_correcting","label":"【自主修正中】","summary":"命令返回400，模型正在重试"}'
        )
        self.assertTrue(norm["ok"])
        self.assertEqual(norm["classification"], "transient_error")
        self.assertFalse(norm["needs_attention"])

        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            sent: list[tuple[str, str]] = []
            verify_calls: list[dict] = []
            t0 = 2_000_000_000.0
            meta = SessionSummary(
                app="cline",
                session_id="s-l2-trans",
                title="Self Correct",
                status=STATUS_ERROR,
                status_detail="failed",
                updated_at_ms=int(t0 * 1000),
            )
            adapter = _FakeAdapter("cline", meta, last_msg="HTTP 400 error, retrying with fixed query...")
            runner = WatchdogRunner(
                adapters={"cline": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
                verifier=lambda ctx: (
                    verify_calls.append(ctx)
                    or {
                        "ok": True,
                        "classification": "transient_error",
                        "needs_attention": False,
                        "label": "【自主修正中】",
                        "summary": "只是单步工具返回非零后模型正在自主修正，无需打扰",
                    }
                ),
                verify_agent=True,
                suppress_routine=False,  # transient_error silences unconditionally!
            )
            wstore.upsert(
                {
                    "alias": "自主修正",
                    "app": "cline",
                    "session_id": "s-l2-trans",
                    "notify_targets": ["weixin"],
                    "cooldown_minutes": 15,
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )

            # 1. Transient error -> silenced unconditionally, NOT pushed, suppressed cooldown recorded
            r0 = runner.poll_once(now_ts=t0)
            self.assertEqual(r0["event_count"], 1)
            self.assertTrue(r0["events"][0]["suppressed"])
            self.assertEqual(len(sent), 0)
            self.assertEqual(len(verify_calls), 1)
            stored = wstore.list_all()[0]
            self.assertEqual(stored["last_verification"]["classification"], "transient_error")
            self.assertEqual(stored["last_suppressed_by_event"]["error"], t0)
            self.assertEqual(stored.get("last_notified_at", 0), 0)

            # 2. Repeat running -> error 2m later does NOT re-call LLM verifier due to suppressed cooldown
            meta.status = STATUS_RUNNING
            runner.poll_once(now_ts=t0 + 60)
            meta.status = STATUS_ERROR
            r1 = runner.poll_once(now_ts=t0 + 120)
            self.assertEqual(r1["event_count"], 0)
            self.assertEqual(len(verify_calls), 1)

            # 3. Genuine waiting_user 3m later IS NOT blocked because no real alert was sent
            meta.status = STATUS_WAITING_USER
            meta.status_detail = "confirm delete?"
            r2 = runner.poll_once(now_ts=t0 + 180)
            self.assertEqual(r2["event_count"], 1)
            self.assertEqual(r2["events"][0]["event"], "waiting_user")
            self.assertEqual(len(sent), 1)

    def test_send_message_watch_true_aligns_with_verified_session_id(self) -> None:
        class _VerifyingFakeWriter:
            app_name = "antigravity"

            def __init__(self, verified_sid: str, verified_title: str) -> None:
                self.verified_sid = verified_sid
                self.verified_title = verified_title
                self.calls: list[dict] = []

            def status(self) -> dict:
                return {"app": "antigravity", "ready": True, "transport": "fake"}

            def send_message(self, session_id: str, text: str, **kwargs) -> dict:  # noqa: ANN003
                self.calls.append({"session_id": session_id, "text": text, **kwargs})
                return {
                    "ok": True,
                    "sessionId": self.verified_sid,
                    "verified_session_id": self.verified_sid,
                    "verified_title": self.verified_title,
                    "mode": kwargs.get("mode", "queue"),
                }

        class _MultiFakeAdapter:
            def __init__(self, app_name: str, items: dict[str, tuple[SessionSummary, str]]) -> None:
                self.app_name = app_name
                self.items = items

            def list_sessions(self, **kwargs):  # noqa: ANN003
                return [s for s, _ in self.items.values()]

            def search_sessions(self, query: str, **kwargs):  # noqa: ANN003
                return [s for s, _ in self.items.values() if query.lower() in s.title.lower()]

            def get_session_meta(self, session_id: str) -> SessionSummary | None:
                pair = self.items.get(session_id)
                return pair[0] if pair else None

            def read_session(self, session_id: str, **kwargs):  # noqa: ANN003
                pair = self.items.get(session_id)
                if pair is None:
                    return {"error": "not found"}
                return {
                    "session": pair[0].to_dict(),
                    "message_count_returned": 1,
                    "messages": [{"index": 1, "role": "assistant", "content": pair[1]}],
                }

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            wstore = WatchStore(td_path / "watches.json")
            ag_req = SessionSummary(app="antigravity", session_id="ag-req-1", title="Requested AG", status=STATUS_IDLE, updated_at_ms=1000)
            ag_ver = SessionSummary(app="antigravity", session_id="ag-verified-2", title="Verified AG Title", status=STATUS_RUNNING, updated_at_ms=2000)
            codex_src = SessionSummary(app="codex", session_id="cx-src-1", title="Codex Source", status=STATUS_IDLE, updated_at_ms=1500)

            ag_writer = _VerifyingFakeWriter("ag-verified-2", "Verified AG Title")
            svc = HubService(
                adapters={
                    "antigravity": _MultiFakeAdapter("antigravity", {"ag-req-1": (ag_req, "ok"), "ag-verified-2": (ag_ver, "running")}),
                    "codex": _MultiFakeAdapter("codex", {"cx-src-1": (codex_src, "Codex final instruction")}),
                    "dsh": _MultiFakeAdapter("dsh", {}),
                    "cline": _MultiFakeAdapter("cline", {}),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=wstore,
                writers={
                    "antigravity": ag_writer,
                    "codex": _FakeWriter("codex", ready=False),
                    "dsh": _FakeWriter("dsh", ready=False),
                    "cline": _FakeWriter("cline", ready=False),
                },
            )

            # Dry-run plans watch without registering
            dry = svc.send_message("antigravity", "ag-req-1", "preview", dry_run=True, watch=True, notify_targets="weixin")
            self.assertTrue(dry["ok"])
            self.assertTrue(dry["watch_planned"])
            self.assertEqual(dry["notify_targets"], ["weixin"])
            self.assertEqual(svc.watch_list()["count"], 0)

            # Real send aligns watch to verified_session_id
            sent_res = svc.send_message(
                "antigravity",
                "ag-req-1",
                "执行对齐测试",
                watch=True,
                watch_alias="强对齐任务",
                notify_targets="weixin",
            )
            self.assertTrue(sent_res["ok"])
            self.assertEqual(sent_res["verified_session_id"], "ag-verified-2")
            self.assertEqual(sent_res["verified_title"], "Verified AG Title")
            self.assertTrue(sent_res["watch"]["ok"])
            self.assertEqual(sent_res["watch"]["watch"]["session_id"], "ag-verified-2")
            self.assertEqual(sent_res["watch"]["watch"]["notify_targets"], ["weixin"])
            self.assertEqual(sent_res["watch"]["watch"]["alias"], "强对齐任务")

            wl = svc.watch_list()
            self.assertEqual(wl["count"], 1)
            self.assertEqual(wl["watches"][0]["session_id"], "ag-verified-2")

            # relay_message with watch=True forwards and updates watch on verified_session_id
            relayed = svc.relay_message(
                "codex",
                "cx-src-1",
                "antigravity",
                "ag-req-1",
                dry_run=False,
                watch=True,
                watch_alias="强对齐任务",
                notify_targets="qqbot,weixin",
            )
            self.assertTrue(relayed["ok"])
            self.assertEqual(relayed["verified_session_id"], "ag-verified-2")
            self.assertTrue(relayed["watch"]["ok"])
            self.assertEqual(relayed["watch"]["watch"]["notify_targets"], ["qqbot", "weixin"])

    def test_watch_add_rejects_nonexistent_ghost_session(self) -> None:
        class _MentioningAdapter:
            app_name = "antigravity"

            def __init__(self) -> None:
                self.real = SessionSummary(app="antigravity", session_id="real-session-1", title="Real Session", status=STATUS_IDLE, updated_at_ms=1000)

            def list_sessions(self, **kwargs):  # noqa: ANN003
                return [self.real]

            def search_sessions(self, query: str, **kwargs):  # noqa: ANN003
                # Simulate another transcript mentioning the ghost UUID
                return [self.real] if "99999999-1111-2222-3333-444444444444" in query else []

            def get_session_meta(self, session_id: str) -> SessionSummary | None:
                return self.real if session_id == self.real.session_id else None

            def read_session(self, session_id: str, **kwargs):  # noqa: ANN003
                return {"session": self.real.to_dict(), "messages": []}

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            svc = HubService(
                adapters={
                    "antigravity": _MentioningAdapter(),
                    "codex": _FakeAdapter("codex", SessionSummary(app="codex", session_id="cx-real-1", title="Codex", status=STATUS_IDLE)),
                    "dsh": _FakeAdapter("dsh", SessionSummary(app="dsh", session_id="dsh-real-1", title="DSH", status=STATUS_IDLE)),
                    "cline": _FakeAdapter("cline", SessionSummary(app="cline", session_id="cl-real-1", title="Cline", status=STATUS_IDLE)),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=WatchStore(td_path / "watches.json"),
            )

            for app_name, ghost_id in (
                ("antigravity", "00000000-0000-0000-0000-000000000000"),
                ("codex", "ghost-session-id"),
                ("cline", "active"),
                ("all", "99999999-1111-2222-3333-444444444444"),
            ):
                res = svc.watch_add(app_name, ghost_id)
                self.assertIn("error", res, f"Expected error for ghost watch ({app_name}, {ghost_id}), got {res}")
                self.assertNotIn("ok", res)

            self.assertEqual(svc.watch_list()["count"], 0)


class TestVscodeClineHttpBridge(unittest.TestCase):
    """Verify ClineWriter HTTP bridge integration against an ephemeral loopback HTTPServer."""

    def test_cline_writer_and_hub_service_http_bridge(self) -> None:
        expected_token = "a" * 64
        received: list[tuple[str, dict]] = []

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def _reply(self, code: int, body: dict) -> None:
                raw = json.dumps(body).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _authorized(self) -> bool:
                return self.headers.get("Authorization") == f"Bearer {expected_token}"

            def do_GET(self) -> None:  # noqa: N802
                if not self._authorized():
                    self._reply(401, {"ok": False, "error": "unauthorized"})
                    return
                if self.path == "/health":
                    self._reply(
                        200,
                        {
                            "ok": True,
                            "plugin": "agent-hub-cline-bridge",
                            "clineInstalled": True,
                            "clineActive": True,
                            "hasStartNewTask": True,
                            "hasSendMessage": True,
                            "workspaces": ["/tmp/demo-ws"],
                        },
                    )
                    return
                self._reply(404, {"ok": False})

            def do_POST(self) -> None:  # noqa: N802
                if not self._authorized():
                    self._reply(401, {"ok": False, "error": "unauthorized"})
                    return
                length = int(self.headers.get("Content-Length") or "0")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                received.append((self.path, payload))
                action = "startNewTask" if self.path == "/new-task" else "sendMessage"
                self._reply(200, {"ok": True, "action": action, "workspaces": ["/tmp/demo-ws"]})

        srv = HTTPServer(("127.0.0.1", 0), _Handler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            port = srv.server_address[1]
            with tempfile.TemporaryDirectory() as td:
                td_path = Path(td)
                tok_file = td_path / "cline-bridge.token"
                tok_file.write_text(expected_token + "\n", encoding="utf-8")
                state_file = td_path / "cline-bridge.json"
                state_file.write_text(json.dumps({"port": port}), encoding="utf-8")

                writer = ClineWriter(
                    token_path=tok_file,
                    state_path=state_file,
                    lock_path=td_path / "missing.json",
                )
                st = writer.status()
                self.assertTrue(st["ready"])
                self.assertEqual(st["transport"], "vscode-cline-api")

                # Reject wrong bearer token cleanly
                bad_tok = td_path / "bad.token"
                bad_tok.write_text("b" * 64, encoding="utf-8")
                bad_writer = ClineWriter(
                    token_path=bad_tok,
                    state_path=state_file,
                    lock_path=td_path / "missing.json",
                )
                self.assertFalse(bad_writer.status()["ready"])

                svc = HubService(
                    bindings=BindingStore(td_path / "bindings.json"),
                    audit=AuditLog(td_path / "audit.jsonl"),
                    watches=WatchStore(td_path / "watches.json"),
                    writers={
                        "dsh": _FakeWriter("dsh", ready=False),
                        "codex": _FakeWriter("codex", ready=False),
                        "antigravity": _FakeWriter("antigravity", ready=False),
                        "cline": writer,
                    },
                )

                created = svc.create_session("cline", title="Demo", initial_message="跑一下测试")
                self.assertTrue(created["ok"])
                self.assertEqual(created["session_id"], "vscode-cline-active")
                self.assertEqual(received[0][0], "/new-task")
                self.assertIn("[via Hermes]", received[0][1]["text"])
                self.assertEqual(len(received), 1)  # verify no double-send

                sent = svc.send_message("cline", "active", "继续下一步", dry_run=False)
                self.assertTrue(sent["ok"])
                self.assertEqual(received[1][0], "/send")
                self.assertIn("[via Hermes]", received[1][1]["text"])
        finally:
            srv.shutdown()
            srv.server_close()

    def test_cline_writer_passes_session_id_and_propagates_500(self) -> None:
        expected_token = "c" * 64
        received: list[tuple[str, dict]] = []
        mode_state = {"fail_send": False}

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def _reply(self, code: int, body: dict) -> None:
                raw = json.dumps(body).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/health":
                    self._reply(
                        200,
                        {
                            "ok": True,
                            "plugin": "agent-hub-cline-bridge",
                            "clineInstalled": True,
                            "clineActive": True,
                            "hasStartNewTask": True,
                            "hasSendMessage": True,
                            "hasController": True,
                            "hasShowTaskWithId": True,
                            "activeTaskId": None,
                            "workspaces": ["/tmp/ws"],
                        },
                    )
                    return
                self._reply(404, {"ok": False})

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or "0")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                received.append((self.path, payload))
                if mode_state["fail_send"]:
                    self._reply(500, {"ok": False, "error": "No active task to send message to"})
                    return
                self._reply(
                    200,
                    {
                        "ok": True,
                        "action": "sendMessage",
                        "sessionId": payload.get("sessionId"),
                        "activatedSession": True,
                        "workspaces": ["/tmp/ws"],
                    },
                )

        srv = HTTPServer(("127.0.0.1", 0), _Handler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            port = srv.server_address[1]
            with tempfile.TemporaryDirectory() as td:
                td_path = Path(td)
                tok = td_path / "cline.token"
                tok.write_text(expected_token + "\n", encoding="utf-8")
                st = td_path / "cline.json"
                st.write_text(json.dumps({"port": port}), encoding="utf-8")

                writer = ClineWriter(token_path=tok, state_path=st, lock_path=td_path / "missing.json")
                status = writer.status()
                self.assertTrue(status["hasController"])
                self.assertTrue(status["hasShowTaskWithId"])

                res = writer.send_message("1795000000000_tstsd", "继续")
                self.assertTrue(res["ok"])
                self.assertEqual(res["sessionId"], "1795000000000_tstsd")
                self.assertTrue(res["activatedSession"])
                self.assertEqual(received[-1][1]["sessionId"], "1795000000000_tstsd")

                mode_state["fail_send"] = True
                with self.assertRaises(RuntimeError) as ctx:
                    writer.send_message("1795000000000_tstsd", "再次发送")
                self.assertIn("HTTP 500", str(ctx.exception))
                self.assertIn("No active task", str(ctx.exception))

                audit_log = AuditLog(td_path / "audit.jsonl")
                svc = HubService(
                    adapters={
                        "cline": _FakeAdapter(
                            "cline",
                            SessionSummary(app="cline", session_id="1795000000000_tstsd", title="PRJ_A", status=STATUS_IDLE),
                        ),
                        "dsh": _FakeWriter("dsh", ready=False),
                        "codex": _FakeWriter("codex", ready=False),
                        "antigravity": _FakeWriter("antigravity", ready=False),
                    },
                    bindings=BindingStore(td_path / "bindings.json"),
                    audit=audit_log,
                    watches=WatchStore(td_path / "watches.json"),
                    writers={
                        "cline": writer,
                        "dsh": _FakeWriter("dsh", ready=False),
                        "codex": _FakeWriter("codex", ready=False),
                        "antigravity": _FakeWriter("antigravity", ready=False),
                    },
                )
                svc_err = svc.send_message("cline", "1795000000000_tstsd", "走Hub路径测试500")
                self.assertIn("error", svc_err)
                self.assertIn("No active task", svc_err["error"])
                audit_lines = (td_path / "audit.jsonl").read_text(encoding="utf-8").splitlines()
                self.assertEqual(json.loads(audit_lines[-1])["status"], "error")
        finally:
            srv.shutdown()
            srv.server_close()

    def test_vscode_cline_bridge_js_auto_activation_and_anti_drop(self) -> None:
        ext_path = Path(__file__).resolve().parent.parent / "vscode_cline_bridge" / "extension.js"
        node_script = r"""
const assert = require('node:assert/strict');
const http = require('node:http');
const Module = require('node:module');

const events = [];
const state = {
  hasResolvedView: false,
  knownSessions: new Set(['sess-1', 'sess-2']),
  emitResumeErrorOnSend: false,
};

function makeController() {
  return {
    task: null,
    async clearTask() {
      events.push('clearTask');
      this.task = null;
    },
    async initTask(prompt) {
      events.push('initTask:' + prompt);
      const msgs = [];
      this.task = {
        ulid: 'new-task-99',
        taskId: 'new-task-99',
        messageStateHandler: { messages: msgs },
        async handleWebviewAskResponse(kind, text) {
          events.push('ask:' + kind + ':' + text);
        },
      };
    },
    async postStateToWebview() {
      events.push('postState');
    },
    async showTaskWithId(id) {
      events.push('show:' + id);
      if (!state.knownSessions.has(id)) {
        throw new Error('Task not found in history: ' + id);
      }
      const msgs = [];
      this.task = {
        ulid: id,
        taskId: id,
        messageStateHandler: { messages: msgs },
        async handleWebviewAskResponse(kind, text) {
          events.push('send:' + id + ':' + text);
          if (state.emitResumeErrorOnSend) {
            msgs.push({ type: 'say', say: 'error', text: 'Failed to resume task: missing api key' });
          }
        },
      };
    },
  };
}

// Replicate Cline 4.1.27 iQu(t) closure hiding `t` inside closure scope
function iQu(t) {
  return {
    startNewTask: async (r) => {
      await t.clearTask();
      await t.postStateToWebview();
      await t.initTask(r);
    },
    sendMessage: async (r) => {
      if (t.task) {
        await t.task.handleWebviewAskResponse('messageResponse', r || '');
      }
    },
    pressPrimaryButton: async () => {
      if (t.task) {
        await t.task.handleWebviewAskResponse('yesButtonClicked', '');
      }
    },
  };
}

const controller = makeController();
const clineApi = iQu(controller);

const origLoad = Module._load;
Module._load = function(request, parent, isMain) {
  if (request === 'vscode') {
    return {
      workspace: { workspaceFolders: [{ uri: { fsPath: '/tmp/ws-node-test' } }] },
      extensions: { getExtension: () => ({ isActive: true, exports: clineApi }) },
      commands: {
        executeCommand: async (cmd) => {
          events.push('cmd:' + cmd);
          if (cmd === 'claude-dev.SidebarProvider.focus' && !state.hasResolvedView) {
            state.hasResolvedView = true;
            await controller.clearTask();
          }
        },
      },
    };
  }
  return origLoad.apply(this, arguments);
};

const bridge = require(process.argv[1]);
const { extractControllerViaInspector, createRequestHandler } = bridge._internals;

async function reqJson(port, method, route, token, body) {
  const rawBody = body ? JSON.stringify(body) : null;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: '127.0.0.1',
        port,
        path: route,
        method,
        headers: {
          Authorization: 'Bearer ' + token,
          ...(rawBody ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(rawBody) } : {}),
        },
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) });
        });
      }
    );
    req.on('error', reject);
    if (rawBody) req.write(rawBody);
    req.end();
  });
}

(async () => {
  // (a) Inspector extracts closed-over controller directly from api.sendMessage
  const extracted = await extractControllerViaInspector(clineApi.sendMessage);
  assert.equal(extracted, controller, 'Inspector should extract exact closed-over controller');

  const token = 'test-token-123456';
  let boundPort = 0;
  const handler = createRequestHandler({
    token,
    getPort: () => boundPort,
    getClineApiImpl: async () => ({ installed: true, active: true, api: clineApi }),
    executeCommandImpl: async (cmd) => {
      events.push('focus');
      if (cmd === 'claude-dev.SidebarProvider.focus' && !state.hasResolvedView) {
        state.hasResolvedView = true;
        await controller.clearTask();
      }
    },
    getWorkspacesImpl: () => ['/tmp/ws-node-test'],
    sidebarFocusDelayMs: 5,
    postActivateSettleMs: 5,
    waitForActiveTaskTimeoutMs: 500,
  });

  const srv = http.createServer(handler);
  await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
  boundPort = srv.address().port;

  try {
    // (h) /health exposes controller diagnostics
    const h0 = await reqJson(boundPort, 'GET', '/health', token);
    assert.equal(h0.status, 200);
    assert.equal(h0.body.hasController, true);
    assert.equal(h0.body.hasShowTaskWithId, true);
    assert.equal(h0.body.activeTaskId, null);

    // (f) Without sessionId (and with 'active') when controller.task == null -> HTTP 500, no send
    const rNoSid = await reqJson(boundPort, 'POST', '/send', token, { text: 'dropped?' });
    assert.equal(rNoSid.status, 500);
    assert.match(rNoSid.body.error, /No active Cline task/);

    const rSentinel = await reqJson(boundPort, 'POST', '/send', token, { sessionId: 'active', text: 'dropped?' });
    assert.equal(rSentinel.status, 500);
    assert.equal(events.filter((e) => e.startsWith('send:')).length, 0);

    // (b) Explicit sessionId when controller.task == null -> focus (triggers clearTask) BEFORE showTaskWithId -> sendMessage -> 200
    events.length = 0;
    const r1 = await reqJson(boundPort, 'POST', '/send', token, { sessionId: 'sess-1', text: 'msg1' });
    assert.equal(r1.status, 200);
    assert.equal(r1.body.ok, true);
    assert.equal(r1.body.activatedSession, true);
    assert.equal(r1.body.sessionId, 'sess-1');
    assert.deepEqual(events, ['focus', 'clearTask', 'show:sess-1', 'send:sess-1:msg1']);

    const h1 = await reqJson(boundPort, 'GET', '/health', token);
    assert.equal(h1.body.activeTaskId, 'sess-1');

    // (c) Explicit sessionId matching active task -> skips showTaskWithId, sends directly
    events.length = 0;
    const r2 = await reqJson(boundPort, 'POST', '/send', token, { sessionId: 'sess-1', text: 'msg2' });
    assert.equal(r2.status, 200);
    assert.equal(r2.body.activatedSession, false);
    assert.deepEqual(events, ['send:sess-1:msg2']);

    // (d) Different sessionId -> switches via showTaskWithId -> 200
    events.length = 0;
    const r3 = await reqJson(boundPort, 'POST', '/send', token, { sessionId: 'sess-2', text: 'msg3' });
    assert.equal(r3.status, 200);
    assert.equal(r3.body.activatedSession, true);
    assert.equal(r3.body.sessionId, 'sess-2');
    assert.deepEqual(events, ['focus', 'show:sess-2', 'send:sess-2:msg3']);

    // (e) Nonexistent sessionId -> showTaskWithId throws -> HTTP 500, no sendMessage
    events.length = 0;
    const rMissing = await reqJson(boundPort, 'POST', '/send', token, { sessionId: 'missing-task', text: 'msg4' });
    assert.equal(rMissing.status, 500);
    assert.match(rMissing.body.error, /Task not found in history: missing-task/);
    assert.equal(events.filter((e) => e.startsWith('send:')).length, 0);

    // (g) Swallowed 'Failed to resume task' error emitted into messageStateHandler -> HTTP 500
    state.emitResumeErrorOnSend = true;
    const rResumeErr = await reqJson(boundPort, 'POST', '/send', token, { sessionId: 'sess-2', text: 'msg5' });
    assert.equal(rResumeErr.status, 500);
    assert.match(rResumeErr.body.error, /Failed to resume task: missing api key/);
    state.emitResumeErrorOnSend = false;

    // /new-task returns created sessionId
    const rNew = await reqJson(boundPort, 'POST', '/new-task', token, { text: 'brand new' });
    assert.equal(rNew.status, 200);
    assert.equal(rNew.body.sessionId, 'new-task-99');
  } finally {
    srv.close();
  }
})();
"""
        proc = subprocess.run(
            ["node", "-e", node_script, str(ext_path)],
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"Node bridge integration test failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
        )


class TestMcpStdioHandshake(unittest.TestCase):
    """Verify real MCP stdio protocol handshake discovers all 24 tools."""

    def test_mcp_stdio_lists_all_24_tools_and_calls_overview_and_watch_list(self) -> None:
        from mcp.client.session import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client

        project_root = str(Path(__file__).resolve().parent.parent)
        expected_tools = {
            "hub_overview",
            "hub_list_sessions",
            "hub_search_sessions",
            "hub_read_session",
            "hub_summarize_session",
            "hub_handover_session",
            "hub_bind_task",
            "hub_list_bindings",
            "hub_unbind_task",
            "hub_writer_status",
            "hub_send_message",
            "hub_relay_message",
            "hub_create_session",
            "hub_respond_approval",
            "hub_cancel_session",
            "hub_audit_recent",
            "hub_relay_start",
            "hub_relay_list",
            "hub_relay_stop",
            "hub_relay_tick",
            "hub_watch_add",
            "hub_watch_list",
            "hub_watch_remove",
            "hub_watch_check_now",
        }

        async def _probe() -> None:
            with tempfile.TemporaryDirectory() as td:
                env = dict(os.environ)
                env["PYTHONPATH"] = project_root
                env["AGENT_HUB_BINDINGS_PATH"] = str(Path(td) / "bindings.json")
                env["AGENT_HUB_AUDIT_PATH"] = str(Path(td) / "audit.jsonl")
                env["AGENT_HUB_WATCHES_PATH"] = str(Path(td) / "watches.json")
                env["AGENT_HUB_RELAY_DB_PATH"] = str(Path(td) / "relay_states.sqlite")
                params = StdioServerParameters(
                    command=sys.executable,
                    args=["-m", "agent_hub.server"],
                    env=env,
                )
                async with stdio_client(params) as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        tools_resp = await session.list_tools()
                        tools_by_name = {t.name: t for t in tools_resp.tools}
                        self.assertEqual(set(tools_by_name.keys()), expected_tools)

                        for tool_name in ("hub_send_message", "hub_relay_message", "hub_create_session"):
                            props = (tools_by_name[tool_name].input_schema or {}).get("properties") or {}
                            for param in ("model", "provider", "reasoning_effort"):
                                self.assertIn(param, props, f"{tool_name} missing {param} in MCP schema")
                        relay_props = (tools_by_name["hub_relay_message"].input_schema or {}).get("properties") or {}
                        self.assertIn("mode", relay_props)
                        create_props = (tools_by_name["hub_create_session"].input_schema or {}).get("properties") or {}
                        self.assertIn("agent_preset", create_props)
                        self.assertIn("watch", create_props)

                        ov_resp = await session.call_tool("hub_overview", {"limit_per_app": 1})
                        self.assertFalse(ov_resp.is_error)
                        parsed = json.loads(ov_resp.content[0].text)
                        self.assertEqual(set(parsed["apps"].keys()), set(VALID_APPS))

                        ws_resp = await session.call_tool("hub_writer_status", {})
                        self.assertFalse(ws_resp.is_error)
                        ws_parsed = json.loads(ws_resp.content[0].text)
                        self.assertEqual(set(ws_parsed["writers"].keys()), set(VALID_APPS))

                        wl_resp = await session.call_tool("hub_watch_list", {})
                        self.assertFalse(wl_resp.is_error)
                        wl_parsed = json.loads(wl_resp.content[0].text)
                        self.assertEqual(wl_parsed["count"], 0)

                        ar_resp = await session.call_tool("hub_audit_recent", {"limit": 5})
                        self.assertFalse(ar_resp.is_error)
                        ar_parsed = json.loads(ar_resp.content[0].text)
                        self.assertEqual(ar_parsed["count"], 0)

                        rl_resp = await session.call_tool("hub_relay_list", {})
                        self.assertFalse(rl_resp.is_error)
                        rl_parsed = json.loads(rl_resp.content[0].text)
                        self.assertEqual(rl_parsed["count"], 0)

        asyncio.run(_probe())


class TestContentPenetrationSearchAndFuzzyResolve(unittest.TestCase):
    """Verify DSH/Cline/Antigravity body search and HubService._resolve_target fuzzy locking."""

    def test_dsh_zstd_body_search_nested_model_and_tail_frames(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            harness = Path(td)
            pc_dir = harness / "storages" / "session_projcache" / "sessions"
            pc_dir.mkdir(parents=True)
            s_dir = harness / "sessions" / "ws-olink" / "session-dsh-1"
            s_dir.mkdir(parents=True)

            (pc_dir / "session-dsh-1.json").write_text(
                json.dumps(
                    {
                        "record": {
                            "identity": {"cwd": "/tmp/olink"},
                            "rows": {
                                "title": {"val": "General Work"},
                                "sessionListMetadata": {"val": {"lastPromptAt": "2026-11-24T01:00:00Z"}},
                                "modelSelection": {
                                    "val": {
                                        "lastUsed": {
                                            "provider": "deepseek-official",
                                            "model": "deepseek-reasoner",
                                            "reasoningEffort": "high",
                                        }
                                    }
                                },
                                "turnOutline": {"val": {"turns": []}},
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            events = [
                {
                    "seq": 1,
                    "type": "user/message",
                    "time": "2026-11-24T01:00:01Z",
                    "data": {"source": {"kind": "agent-instructions"}, "content": [{"type": "text", "text": "SECRET_SYNTHETIC_NO_HIT"}]},
                },
                {
                    "seq": 2,
                    "type": "user/message",
                    "time": "2026-11-24T01:00:02Z",
                    "data": {"source": {"kind": "user"}, "content": [{"type": "text", "text": "请分析 Olink 蛋白组学数据中的队列偏差"}]},
                },
                {
                    "seq": 3,
                    "type": "assistant/message",
                    "time": "2026-11-24T01:00:03Z",
                    "data": {"message": {"content": [{"type": "text", "text": "已完成 蛋白组学 PCA 质控校验"}]}},
                },
            ]
            compressed_frames = b"".join(zstd.compress((json.dumps(ev, ensure_ascii=False) + "\n").encode("utf-8")) for ev in events)
            (s_dir / "session.v4.jsonl.zstd").write_bytes(compressed_frames)

            # Large (>2MB) multi-frame file to exercise tail-frame decompression path
            big_dir = harness / "sessions" / "ws-big" / "session-dsh-big"
            big_dir.mkdir(parents=True)
            pad_line = (json.dumps({"seq": 1, "type": "step/start", "data": {"blob": os.urandom(900).hex()}}) + "\n").encode("utf-8")
            # Raw random hex compresses poorly (~450B per frame) so ~5000 small frames exceed 2.1MB cleanly
            pad_frame = zstd.compress(pad_line)
            repeat_cnt = (2_150_000 // len(pad_frame)) + 2
            tail_ev = json.dumps(
                {
                    "seq": 9999,
                    "type": "user/message",
                    "time": "2026-11-24T01:05:00Z",
                    "data": {"source": {"kind": "user"}, "content": [{"type": "text", "text": "Here is TAIL_FRAME_NEEDLE_OK inside multi-frame zstd."}]},
                }
            ).encode("utf-8") + b"\n"
            (big_dir / "session.v4.jsonl.zstd").write_bytes((pad_frame * repeat_cnt) + zstd.compress(tail_ev))

            adapter = DshAdapter(harness_dir=harness)
            listed = adapter.list_sessions(limit=5)
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0].extra["model"], "deepseek-reasoner")
            self.assertEqual(listed[0].extra["provider"], "deepseek-official")
            self.assertEqual(listed[0].extra["reasoning_effort"], "high")

            self.assertEqual([s.session_id for s in adapter.search_sessions("Olink 蛋白组学")], ["session-dsh-1"])
            self.assertEqual([s.session_id for s in adapter.search_sessions("PCA 质控")], ["session-dsh-1"])
            self.assertEqual(adapter.search_sessions("SECRET_SYNTHETIC_NO_HIT"), [])
            big_hits = adapter.search_sessions("TAIL_FRAME_NEEDLE_OK")
            self.assertEqual([s.session_id for s in big_hits], ["session-dsh-big"])

    def test_cline_body_search_across_messages_ui_and_fts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cline_dir = root / "cline"
            db_dir = cline_dir / "data" / "db"
            db_dir.mkdir(parents=True)
            msg_dir = cline_dir / "data" / "sessions" / "s-cline-msg"
            msg_dir.mkdir(parents=True)
            vsc_dir = root / "vsc_tasks"
            ui_dir = vsc_dir / "t-ui-1"
            ui_dir.mkdir(parents=True)

            msg_file = msg_dir / "s-cline-msg.messages.json"
            msg_file.write_text(
                json.dumps(
                    {
                        "messages": [
                            {"role": "user", "content": "Open project", "ts": 1_760_000_000_000},
                            {"role": "assistant", "content": "Found unique_messages_json_token in workspace.", "ts": 1_760_000_005_000},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (ui_dir / "ui_messages.json").write_text(
                json.dumps(
                    [
                        {"say": "user_feedback", "text": "Task UI Header", "ts": 1_760_000_001_000},
                        {"say": "text", "text": "Confirmed later_ui_body_phrase finished cleanly.", "ts": 1_760_000_002_000},
                    ]
                ),
                encoding="utf-8",
            )

            s_con = sqlite3.connect(db_dir / "sessions.db")
            s_con.execute(
                """
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY, source TEXT, pid INTEGER, started_at TEXT,
                    ended_at TEXT, exit_code INTEGER, status TEXT, status_message TEXT,
                    cwd TEXT, workspace_root TEXT, prompt TEXT, metadata_json TEXT,
                    messages_path TEXT, transcript_path TEXT, updated_at TEXT,
                    is_subagent INTEGER, model TEXT, provider TEXT
                )
                """
            )
            s_con.execute(
                """
                INSERT INTO sessions VALUES (
                    's-cline-msg', 'vscode', 123, '2026-11-24T01:00:00Z',
                    NULL, NULL, 'completed', '',
                    '/tmp/ws', '/tmp/ws', 'Open project', '{"title":"Cline Generic"}',
                    ?, '', '2026-11-24T01:01:00Z', 0, 'deepseek-chat', 'deepseek'
                )
                """,
                (str(msg_file),),
            )
            s_con.commit()
            s_con.close()

            sr_con = sqlite3.connect(db_dir / "session-search.db")
            sr_con.execute(
                """
                CREATE TABLE indexed_sessions (
                    session_id TEXT PRIMARY KEY, source_revision TEXT, indexed_at TEXT,
                    document_count INTEGER, index_version INTEGER, title TEXT
                )
                """
            )
            sr_con.execute(
                """
                CREATE VIRTUAL TABLE session_search USING fts5(
                    session_id UNINDEXED, document_id UNINDEXED, ordinal UNINDEXED,
                    role UNINDEXED, started_at UNINDEXED, workspace_root, title, content
                )
                """
            )
            sr_con.execute("INSERT INTO indexed_sessions VALUES ('s-cline-fts', 'r1', '2026-11-23T01:00:00Z', 1, 1, 'Archived FTS')")
            sr_con.execute(
                "INSERT INTO session_search VALUES ('s-cline-fts', 'd1', 0, 'assistant', '2026-11-23T01:00:00Z', '/tmp/fts', 'Archived FTS', 'Details with indexed_fts_body_phrase here')"
            )
            sr_con.commit()
            sr_con.close()

            adapter = ClineAdapter(cline_dir=cline_dir, vscode_tasks_dir=vsc_dir)
            self.assertEqual([s.session_id for s in adapter.search_sessions("unique_messages_json_token")], ["s-cline-msg"])
            self.assertEqual([s.session_id for s in adapter.search_sessions("later_ui_body_phrase")], ["t-ui-1"])
            self.assertEqual([s.session_id for s in adapter.search_sessions("indexed_fts_body_phrase")], ["s-cline-fts"])

    def test_antigravity_cjk_body_and_transcript_search(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            con = sqlite3.connect(base / "conversation_summaries.db")
            con.execute(
                """
                CREATE TABLE conversation_summaries (
                    conversation_id TEXT PRIMARY KEY, title TEXT, preview TEXT,
                    step_count INTEGER, last_modified_time TEXT, workspace_uris TEXT,
                    status TEXT, not_fully_idle INTEGER, killed INTEGER
                )
                """
            )
            con.execute(
                """
                CREATE VIRTUAL TABLE conversations_fts USING fts5(
                    conversation_id UNINDEXED, title, workspace, project, user_body, agent_body
                )
                """
            )
            con.execute(
                "INSERT INTO conversation_summaries VALUES ('conv-ag-fts', 'Generic Title', 'Short preview', 3, '2026-11-24T01:00:00Z', '[]', 'IDLE', 0, 0)"
            )
            con.execute(
                "INSERT INTO conversations_fts VALUES ('conv-ag-fts', 'Generic Title', '', '', '问题', '这里包含这是仅存于正文的中文关键句内容')"
            )
            con.commit()
            con.close()

            logs = base / "brain" / "conv-ag-live" / ".system_generated" / "logs"
            logs.mkdir(parents=True)
            (logs / "transcript.jsonl").write_text(
                json.dumps(
                    {
                        "step_index": 0,
                        "type": "USER_INPUT",
                        "content": "truncated...",
                        "truncated_fields": ["content"],
                        "created_at": "2026-11-24T02:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (logs / "transcript_full.jsonl").write_text(
                json.dumps(
                    {
                        "step_index": 0,
                        "type": "USER_INPUT",
                        "content": "刚写入的实时轨迹正文需要被穿透命中",
                        "created_at": "2026-11-24T02:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            adapter = AntigravityAdapter(antigravity_dir=base)
            self.assertEqual([s.session_id for s in adapter.search_sessions("中文关键句")], ["conv-ag-fts"])
            self.assertEqual([s.session_id for s in adapter.search_sessions("实时轨迹正文")], ["conv-ag-live"])

    def test_global_fuzzy_resolve_target_and_guards(self) -> None:
        class _SearchableAdapter:
            def __init__(self, app_name: str, items: list[tuple[SessionSummary, str]]) -> None:
                self.app_name = app_name
                self.items = items

            def list_sessions(self, **kwargs):  # noqa: ANN003
                return [s for s, _ in self.items]

            def search_sessions(self, query: str, *, limit: int = 15):
                q = query.strip().lower()
                hits = []
                for s, body in self.items:
                    if q in s.session_id.lower() or q in s.title.lower() or q in s.preview.lower() or q in body.lower():
                        hits.append(s)
                return hits[:limit]

            def get_session_meta(self, session_id: str) -> SessionSummary | None:
                for s, _ in self.items:
                    if s.session_id == session_id:
                        return s
                return None

            def read_session(self, session_id: str, **kwargs):  # noqa: ANN003
                for s, body in self.items:
                    if s.session_id == session_id:
                        return {
                            "session": s.to_dict(),
                            "message_count_returned": 1,
                            "messages": [{"index": 1, "role": "assistant", "content": body}],
                        }
                return {"error": f"{self.app_name} session '{session_id}' not found"}

        with tempfile.TemporaryDirectory() as td:
            dsh_sum = SessionSummary(app="dsh", session_id="dsh-exact-1", title="DSH Pipeline", status=STATUS_IDLE, updated_at_ms=2000)
            codex_sum = SessionSummary(app="codex", session_id="codex-exact-1", title="Codex Review", status=STATUS_IDLE, updated_at_ms=1000)
            adapters = {
                "dsh": _SearchableAdapter("dsh", [(dsh_sum, "完成 Olink 队列偏差 蛋白组学 PCA 质控 分析")]),
                "codex": _SearchableAdapter("codex", [(codex_sum, "Reviewed Rust parser performance")]),
                "antigravity": _SearchableAdapter("antigravity", []),
                "cline": _SearchableAdapter("cline", []),
            }
            fake_dsh = _FakeWriter("dsh", ready=True, supports_create=True)
            svc = HubService(
                adapters=adapters,
                bindings=BindingStore(Path(td) / "bindings.json"),
                audit=AuditLog(Path(td) / "audit.jsonl"),
                watches=WatchStore(Path(td) / "watches.json"),
                writers={
                    "dsh": fake_dsh,
                    "codex": _FakeWriter("codex", ready=True),
                    "antigravity": _FakeWriter("antigravity", ready=True),
                    "cline": _FakeWriter("cline", ready=True, supports_create=True),
                },
            )

            # Multi-word sentence intersection resolves across app="all"
            rd = svc.read_session("all", "Olink 蛋白组学")
            self.assertEqual(rd["session"]["session_id"], "dsh-exact-1")

            # Fuzzy send dry-run and watch_add lock onto matching session
            dry = svc.send_message("all", "PCA 质控", "继续下一步", dry_run=True)
            self.assertTrue(dry["ok"])
            self.assertEqual(dry["target_app"], "dsh")
            self.assertEqual(dry["target_session_id"], "dsh-exact-1")

            w = svc.watch_add("all", "Rust parser")
            self.assertTrue(w["ok"])
            self.assertEqual(w["watch"]["app"], "codex")
            self.assertEqual(w["watch"]["session_id"], "codex-exact-1")

            # Explicit alias never falls back to fuzzy search; Cline 'active' sentinel never hijacked
            self.assertIn("error", svc.read_session("alias", "PCA 质控"))
            self.assertEqual(svc._resolve_target("cline", "active"), ("cline", "active", None))
            self.assertIn("error", svc.read_session("dsh", "does-not-exist-999"))
            self.assertIn("error", svc.read_session("all", "no-such-phrase-anywhere"))


class TestModelPassthroughAndCdpCreate(unittest.TestCase):
    """Verify model/provider/reasoning passthrough, DSH bridge, and Codex/Antigravity CDP create_session."""

    def test_dsh_and_cline_model_provider_reasoning_passthrough(self) -> None:
        received: list[tuple[str, dict]] = []
        expected_tok = "t" * 64

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A002
                return

            def do_GET(self) -> None:  # noqa: N802
                raw = json.dumps(
                    {
                        "ok": True,
                        "pid": 1234,
                        "hasStartNewTask": True,
                        "hasSendMessage": True,
                        "workspaces": ["/tmp/ws"],
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or "0")
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                received.append((self.path, payload))
                body: dict = {"ok": True, "workspaces": ["/tmp/ws"]}
                if self.path == "/create":
                    body["sessionId"] = "dsh-new-1"
                    body["cwd"] = payload.get("cwd", "/tmp/ws")
                elif self.path == "/send":
                    body["requestId"] = f"rpc-{len(received)}"
                    body["mode"] = payload.get("mode", "queue")
                elif self.path == "/new-task":
                    body["action"] = "startNewTask"
                raw = json.dumps(body).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        srv = HTTPServer(("127.0.0.1", 0), _Handler)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            url = f"http://127.0.0.1:{srv.server_address[1]}"
            with tempfile.TemporaryDirectory() as td:
                td_path = Path(td)
                tok = td_path / "tok"
                tok.write_text(expected_tok + "\n", encoding="utf-8")
                st = td_path / "state.json"
                st.write_text(json.dumps({"port": srv.server_address[1]}), encoding="utf-8")

                dsh_writer = DshWriter(base_url=url, token_path=tok)
                cline_writer = ClineWriter(token_path=tok, state_path=st, lock_path=td_path / "missing.json")
                dsh_sum = SessionSummary(app="dsh", session_id="dsh-s1", title="DSH S1", status=STATUS_IDLE, updated_at_ms=1000)
                cline_sum = SessionSummary(app="cline", session_id="cline-s1", title="Cline S1", status=STATUS_IDLE, updated_at_ms=1000)

                svc = HubService(
                    adapters={
                        "dsh": _FakeAdapter("dsh", dsh_sum),
                        "cline": _FakeAdapter("cline", cline_sum),
                        "codex": _FakeAdapter("codex", SessionSummary(app="codex", session_id="c1", title="C1", status=STATUS_IDLE)),
                        "antigravity": _FakeAdapter("antigravity", SessionSummary(app="antigravity", session_id="a1", title="A1", status=STATUS_IDLE)),
                    },
                    bindings=BindingStore(td_path / "bindings.json"),
                    audit=AuditLog(td_path / "audit.jsonl"),
                    watches=WatchStore(td_path / "watches.json"),
                    writers={
                        "dsh": dsh_writer,
                        "cline": cline_writer,
                        "codex": _FakeWriter("codex", ready=False),
                        "antigravity": _FakeWriter("antigravity", ready=False),
                    },
                )

                created_dsh = svc.create_session(
                    "dsh",
                    title="Research Run",
                    workspace="/tmp/ws",
                    initial_message="Start analysis",
                    model="deepseek-reasoner",
                    provider="deepseek",
                    reasoning_effort="high",
                    agent_preset="research",
                )
                self.assertTrue(created_dsh["ok"])
                self.assertEqual(received[0][0], "/create")
                self.assertEqual(received[0][1]["model"], "deepseek-reasoner")
                self.assertEqual(received[0][1]["provider"], "deepseek")
                self.assertEqual(received[0][1]["reasoningEffort"], "high")
                self.assertEqual(received[0][1]["agentPreset"], "research")
                self.assertEqual(received[1][0], "/send")

                sent_dsh = svc.send_message(
                    "dsh",
                    "dsh-s1",
                    "Follow up",
                    model="deepseek-chat",
                    provider="deepseek",
                    reasoning_effort="low",
                )
                self.assertTrue(sent_dsh["ok"])
                self.assertEqual(received[2][1]["model"], "deepseek-chat")
                self.assertEqual(received[2][1]["reasoningEffort"], "low")

                # 60s duplicate guard still active
                dup = svc.send_message("dsh", "dsh-s1", "Follow up", model="deepseek-chat")
                self.assertIn("error", dup)

                created_cline = svc.create_session(
                    "cline",
                    initial_message="Check tests",
                    model="claude-3-7-sonnet",
                    provider="anthropic",
                    reasoning_effort="medium",
                )
                self.assertTrue(created_cline["ok"])
                self.assertEqual(received[3][0], "/new-task")
                self.assertEqual(received[3][1]["model"], "claude-3-7-sonnet")
                self.assertEqual(received[3][1]["provider"], "anthropic")
                self.assertEqual(received[3][1]["reasoningEffort"], "medium")

                sent_cline = svc.send_message(
                    "cline",
                    "cline-s1",
                    "Continue",
                    model="o3-mini",
                    provider="openai",
                    reasoning_effort="high",
                )
                self.assertTrue(sent_cline["ok"])
                self.assertEqual(received[4][0], "/send")
                self.assertEqual(received[4][1]["model"], "o3-mini")
                self.assertEqual(received[4][1]["provider"], "openai")
                self.assertEqual(received[4][1]["reasoningEffort"], "high")
        finally:
            srv.shutdown()
            srv.server_close()

    def test_codex_and_antigravity_cdp_create_model_and_draft_guard(self) -> None:
        calls: list[dict] = []
        state = {"draft_present": False, "href": "https://127.0.0.1:52123/c/conv-new-uuid-42", "clicked_new": True}

        class _FakeSimpleWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                return False

            def send_text(self, raw: str) -> None:
                calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = calls[-1]
                cid = last["id"]
                method = last["method"]
                if method == "Runtime.evaluate":
                    expr = (last.get("params") or {}).get("expression", "")
                    val: object = {"ok": True}
                    if expr == "window.location.href":
                        val = state["href"]
                    elif "composer_found" in expr:
                        val = {"composer_found": True, "has_unsaved_draft": state["draft_present"]}
                    elif "findNewChatBtn" in expr:
                        if state["draft_present"]:
                            val = {"ok": False, "error": "draft-present", "message": "Composer has unsaved user draft"}
                        else:
                            val = {"ok": True, "clicked": state["clicked_new"], "method": "selector" if state["clicked_new"] else "shortcut-needed"}
                    elif "collectMenuItems" in expr:
                        val = {"applied": True, "already_selected": False, "model_clicked": "o3-mini", "effort_clicked": "High"}
                    elif "window.__hermes_active_composer" in expr and "sendBtn" in expr:
                        val = {"ok": True, "method": "button"}
                    elif "window.__hermes_active_composer" in expr:
                        if state["draft_present"]:
                            val = {"ok": False, "error": "draft-present", "message": "Composer has unsaved user draft"}
                        else:
                            val = {"ok": True, "isContentEditable": True}
                    elif "switch" in expr or "preComposer" in expr:
                        if state["draft_present"]:
                            val = {"ok": False, "error": "draft-present", "message": "Composer has unsaved user draft"}
                        else:
                            val = {"ok": True, "switched": False}
                    return json.dumps({"id": cid, "result": {"result": {"value": val}}})
                return json.dumps({"id": cid, "result": {}})

        with (
            tempfile.TemporaryDirectory() as td,
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _FakeSimpleWS),
            patch.object(CdpWriter, "_discover_endpoint", return_value=(9224, [{"title": "App", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/devtools/page/1"}])),
        ):
            td_path = Path(td)
            codex_w = CodexWriter()
            ag_w = AntigravityWriter()
            svc = HubService(
                adapters={
                    "codex": _FakeAdapter("codex", SessionSummary(app="codex", session_id="c1", title="C1", status=STATUS_IDLE)),
                    "antigravity": _FakeAdapter("antigravity", SessionSummary(app="antigravity", session_id="a1", title="A1", status=STATUS_IDLE)),
                    "dsh": _FakeAdapter("dsh", SessionSummary(app="dsh", session_id="d1", title="D1", status=STATUS_IDLE)),
                    "cline": _FakeAdapter("cline", SessionSummary(app="cline", session_id="cl1", title="CL1", status=STATUS_IDLE)),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=WatchStore(td_path / "watches.json"),
                writers={
                    "codex": codex_w,
                    "antigravity": ag_w,
                    "dsh": _FakeWriter("dsh", ready=False),
                    "cline": _FakeWriter("cline", ready=False),
                },
            )

            # 1. Antigravity create_session extracts /c/<uuid>, selects model+effort, and sends initial_message
            ag_res = svc.create_session(
                "antigravity",
                title="Smoke AG",
                initial_message="Hello AG",
                model="gemini-2.5-pro",
                reasoning_effort="xhigh",
            )
            self.assertTrue(ag_res["ok"])
            self.assertEqual(ag_res["session_id"], "conv-new-uuid-42")
            self.assertIn("initial_send", ag_res)
            self.assertTrue(any(c["method"] == "Input.insertText" and "[via Hermes]" in c["params"]["text"] for c in calls))
            self.assertTrue(any("extra high" in (c.get("params") or {}).get("expression", "") for c in calls))

            # 2. Codex create_session without initial_message (title only) does NOT send title as message
            calls.clear()
            state["href"] = "app://-/index.html"
            state["clicked_new"] = False  # exercise keyboard shortcut fallback
            codex_open = svc.create_session("codex", title="Just Open", initial_message="", model="o3-mini", reasoning_effort="high")
            self.assertTrue(codex_open["ok"])
            self.assertTrue(codex_open["session_id"].startswith("codex-cdp-"))
            self.assertEqual(codex_open["created"]["new_chat_method"], "shortcut")
            self.assertNotIn("initial_send", codex_open)
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))
            self.assertTrue(any(c["method"] == "Input.dispatchKeyEvent" for c in calls))

            # 3. Draft protection aborts both create_session and send_message safely
            calls.clear()
            state["draft_present"] = True
            with self.assertRaises(RuntimeError) as ctx1:
                codex_w.create_session(initial_message="Should abort")
            self.assertIn("draft-present", str(ctx1.exception))
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))

            err_send = svc.send_message("antigravity", "a1", "Should also abort")
            self.assertIn("error", err_send)
            self.assertIn("draft-present", err_send["error"])

    def test_cdp_send_aborts_with_session_mismatch_and_zero_injection(self) -> None:
        calls: list[dict] = []
        state: dict[str, Any] = {
            "app": "antigravity",
            "pathname": "/c/wrong-other-session",
            "href": "https://127.0.0.1:52123/c/wrong-other-session",
            "title": "Wrong Session",
            "codex_sid": "wrong-thread-999",
            "switch_clicked": True,
        }

        class _FakeSimpleWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                return False

            def send_text(self, raw: str) -> None:
                calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = calls[-1]
                cid = last["id"]
                method = last["method"]
                if method == "Runtime.evaluate":
                    expr = (last.get("params") or {}).get("expression", "")
                    if "__agentHubReadSessionIdentity" in expr:
                        if state["app"] == "antigravity":
                            val = {
                                "sessionId": state["pathname"].removeprefix("/c/") if state["pathname"].startswith("/c/") else "",
                                "title": state["title"],
                                "href": state["href"],
                                "pathname": state["pathname"],
                                "source": "pathname" if state["pathname"].startswith("/c/") else "none",
                            }
                        else:
                            val = {
                                "sessionId": state["codex_sid"],
                                "title": state["title"],
                                "href": "app://-/index.html",
                                "pathname": "/index.html",
                                "source": "sidebar-active-chatgpt",
                            }
                        return json.dumps({"id": cid, "result": {"result": {"value": val}}})
                    if "findBySessionId" in expr:
                        return json.dumps(
                            {
                                "id": cid,
                                "result": {
                                    "result": {
                                        "value": {
                                            "ok": True,
                                            "switched": state["switch_clicked"],
                                            "switchMethod": "sidebar-title" if state["switch_clicked"] else "not-found",
                                        }
                                    }
                                },
                            }
                        )
                    if "preComposer" in expr or "composer-not-found" in expr:
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})
                return json.dumps({"id": cid, "result": {}})

        with (
            tempfile.TemporaryDirectory() as td,
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _FakeSimpleWS),
            patch.object(
                CdpWriter,
                "_discover_endpoint",
                return_value=(9224, [{"title": "Wrong Session", "url": "https://127.0.0.1:52123/c/wrong-other-session", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/devtools/page/1"}]),
            ),
        ):
            td_path = Path(td)
            ag_w = AntigravityWriter()
            ag_w.switch_verify_timeout_s = 0.08
            ag_w.switch_poll_interval_s = 0.02

            # 1. Antigravity stuck on wrong session -> SessionMismatchError, zero injection
            with self.assertRaises(SessionMismatchError) as ctx:
                ag_w.send_message("target-ag-123456", "绝对不能注入到错误会话", target_title="Target Session")
            self.assertEqual(ctx.exception.expected_session_id, "target-ag-123456")
            self.assertEqual(ctx.exception.actual_session_id, "wrong-other-session")
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))
            self.assertFalse(any("__hermes_active_composer" in (c.get("params") or {}).get("expression", "") for c in calls))

            # 2. Antigravity on blank '/' page -> SessionMismatchError, zero injection
            calls.clear()
            state["pathname"] = "/"
            state["href"] = "https://127.0.0.1:52123/"
            state["switch_clicked"] = False
            svc = HubService(
                adapters={
                    "antigravity": _FakeAdapter("antigravity", SessionSummary(app="antigravity", session_id="target-ag-123456", title="Target AG", status=STATUS_IDLE)),
                    "codex": _FakeAdapter("codex", SessionSummary(app="codex", session_id="cccccccc-3333-4333-8333-333333333333", title="Branch · Class B", status=STATUS_IDLE)),
                    "dsh": _FakeAdapter("dsh", SessionSummary(app="dsh", session_id="d1", title="D1", status=STATUS_IDLE)),
                    "cline": _FakeAdapter("cline", SessionSummary(app="cline", session_id="c1", title="C1", status=STATUS_IDLE)),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=WatchStore(td_path / "watches.json"),
                writers={
                    "antigravity": ag_w,
                    "codex": _FakeWriter("codex", ready=False),
                    "dsh": _FakeWriter("dsh", ready=False),
                    "cline": _FakeWriter("cline", ready=False),
                },
            )
            res = svc.send_message("antigravity", "target-ag-123456", "不应注入", watch=True)
            self.assertEqual(res.get("error_code"), "session_mismatch")
            self.assertIn("error", res)
            self.assertEqual(svc.watch_list()["count"], 0)
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))
            self.assertFalse(any("__hermes_active_composer" in (c.get("params") or {}).get("expression", "") for c in calls))

            # 3. Codex DOM mismatch -> SessionMismatchError, zero injection
            calls.clear()
            state["app"] = "codex"
            state["codex_sid"] = "wrong-thread-999"
            state["switch_clicked"] = True
            codex_w = CodexWriter()
            codex_w.switch_verify_timeout_s = 0.08
            codex_w.switch_poll_interval_s = 0.02
            with self.assertRaises(SessionMismatchError) as cx_ctx:
                codex_w.send_message("cccccccc-3333-4333-8333-333333333333", "不应注入到错误Codex")
            self.assertEqual(cx_ctx.exception.actual_session_id, "wrong-thread-999")
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))
            self.assertFalse(any("__hermes_active_composer" in (c.get("params") or {}).get("expression", "") for c in calls))

    def test_cdp_send_verifies_switch_and_returns_verified_session_id(self) -> None:
        calls: list[dict] = []
        state: dict[str, Any] = {
            "app": "antigravity",
            "pathname": "/c/other-session-111",
            "href": "https://127.0.0.1:52123/c/other-session-111",
            "title": "Other Session",
            "poll_count": 0,
            "codex_sid": "other-thread-000",
        }

        class _FakeSimpleWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                return False

            def send_text(self, raw: str) -> None:
                calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = calls[-1]
                cid = last["id"]
                method = last["method"]
                if method == "Runtime.evaluate":
                    expr = (last.get("params") or {}).get("expression", "")
                    if "__agentHubReadSessionIdentity" in expr:
                        if state["app"] == "antigravity":
                            if state["switched"] and state["poll_count"] >= 1:
                                state["pathname"] = "/c/target-ag-123456"
                                state["href"] = "https://127.0.0.1:52123/c/target-ag-123456"
                                state["title"] = "Agent Hub Hardening"
                            if state["switched"]:
                                state["poll_count"] += 1
                            sid = state["pathname"].removeprefix("/c/")
                            val = {
                                "sessionId": sid,
                                "title": state["title"],
                                "href": state["href"],
                                "pathname": state["pathname"],
                                "source": "pathname",
                            }
                        else:
                            if state["switched"] and state["poll_count"] >= 1:
                                state["codex_sid"] = "cccccccc-3333-4333-8333-333333333333"
                                state["title"] = "Branch · Class B"
                            if state["switched"]:
                                state["poll_count"] += 1
                            val = {
                                "sessionId": state["codex_sid"],
                                "title": state["title"],
                                "href": "app://-/index.html",
                                "pathname": "/index.html",
                                "source": "sidebar-active-thread",
                            }
                        return json.dumps({"id": cid, "result": {"result": {"value": val}}})
                    if "findBySessionId" in expr:
                        state["switched"] = True
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "switched": True, "switchMethod": "id-href"}}}})
                    if "window.__hermes_active_composer" in expr and "sendBtn" in expr:
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "method": "button"}}}})
                    if "window.__hermes_active_composer" in expr:
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "isContentEditable": True}}}})
                    if "preComposer" in expr or "composer-not-found" in expr:
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})
                return json.dumps({"id": cid, "result": {}})

        with (
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _FakeSimpleWS),
            patch.object(
                CdpWriter,
                "_discover_endpoint",
                return_value=(9224, [{"title": "App", "url": "https://127.0.0.1:52123/c/other-session-111", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/devtools/page/1"}]),
            ),
        ):
            # 1. Antigravity transitions after 1 poll and succeeds with verified_session_id
            state["switched"] = False
            ag_w = AntigravityWriter()
            ag_w.switch_verify_timeout_s = 0.5
            ag_w.switch_poll_interval_s = 0.01
            ag_res = ag_w.send_message("target-ag-123456", "准确投递给 Antigravity", target_title="Agent Hub Hardening")
            self.assertTrue(ag_res["ok"])
            self.assertTrue(ag_res["switched"])
            self.assertEqual(ag_res["verified_session_id"], "target-ag-123456")
            self.assertEqual(ag_res["verified_title"], "Agent Hub Hardening")
            self.assertTrue(any(c["method"] == "Input.insertText" and "Antigravity" in c["params"]["text"] for c in calls))

            # 2. Codex transitions after 1 poll and succeeds with verified_session_id
            calls.clear()
            state.update({"app": "codex", "switched": False, "poll_count": 0, "title": "Other Codex"})
            codex_w = CodexWriter()
            codex_w.switch_verify_timeout_s = 0.5
            codex_w.switch_poll_interval_s = 0.01
            cx_res = codex_w.send_message("cccccccc-3333-4333-8333-333333333333", "准确投递给 Codex", target_title="Branch · Class B")
            self.assertTrue(cx_res["ok"])
            self.assertTrue(cx_res["switched"])
            self.assertEqual(cx_res["verified_session_id"], "cccccccc-3333-4333-8333-333333333333")
            self.assertEqual(cx_res["verified_title"], "Branch · Class B")
            self.assertTrue(any(c["method"] == "Input.insertText" and "Codex" in c["params"]["text"] for c in calls))


class TestEvolutionSprintFeatures(unittest.TestCase):
    """Verify completion summary cards, watchdog hardening, approval statuses, DSH tail reads, and remote approval."""

    def test_completion_summary_card_and_handover_tools(self) -> None:
        from agent_hub.watcher import extract_task_completion_summary

        msgs = [
            {"index": 1, "role": "user", "content": "旧轮次需求：先搭个雏形"},
            {"index": 2, "role": "assistant", "content": "已完成雏形并验证旧版本。"},
            {
                "index": 3,
                "role": "user",
                "content": "[via Hermes]\n\n请修复 watcher 冷却逻辑并生成 VSIX 包，然后跑全量单测",
            },
            {
                "index": 4,
                "role": "tool",
                "content": "[fileChange status=completed] agent_hub/watcher.py, vscode_cline_bridge/extension.js",
            },
            {
                "index": 5,
                "role": "tool",
                "content": "[command status=completed exit=0] python -m unittest discover -s tests -v",
            },
            {
                "index": 6,
                "role": "assistant",
                "content": (
                    "收到，已处理完成。\n\n"
                    "## 实质改动\n"
                    "- 修复 `agent_hub/watcher.py` 按事件类型隔离冷却时间并增加 flock 并发锁\n"
                    "- 在 `vscode_cline_bridge/extension.js` 新增 `/approve` 与 `/cancel` 路由\n"
                    "- Built `vscode_cline_bridge/agent-hub-cline-bridge-0.1.0.vsix` (8265 bytes)\n\n"
                    "## 验证结果\n"
                    "- Ran 40 tests in 11.2s OK\n"
                ),
            },
        ]

        summary = extract_task_completion_summary(
            session_meta=SessionSummary(
                app="codex",
                session_id="s-sum-1",
                title="Watcher Hardening",
                workspace="/tmp/agent-hub-mcp",
                status=STATUS_IDLE,
                updated_at_ms=2_000_000_000_000,
            ),
            messages=msgs,
        )
        self.assertIn("修复 watcher 冷却逻辑", summary["objective"])
        self.assertNotIn("[via Hermes]", summary["objective"])
        self.assertTrue(any("watcher.py" in a for a in summary["actions"]))
        self.assertTrue(any("Ran 40 tests" in v or "exit=0" in v for v in summary["verification"]))
        self.assertIn("agent_hub/watcher.py", summary["modified_files"])
        self.assertIn("**【任务目标】**", summary["markdown_card"])
        self.assertIn("**【实质变更与动作】**", summary["markdown_card"])
        self.assertIn("**【验证结果与产物】**", summary["markdown_card"])
        self.assertIn("# 跨端任务交接包", summary["handover_prompt"])

        # Do NOT fabricate test results when none appear in the round
        plain_sum = extract_task_completion_summary(
            session_meta=SessionSummary(app="dsh", session_id="d-plain", title="Plan Only", status=STATUS_IDLE),
            messages=[
                {"index": 1, "role": "user", "content": "梳理架构方案"},
                {"index": 2, "role": "assistant", "content": "- 建议拆分三阶段路线图\n- 明确风险边界"},
            ],
        )
        self.assertEqual(plain_sum["verification"], [])
        self.assertIn("未检测到显式测试或构建输出", plain_sum["results"][0])

        # Watchdog pushes structured card on `completed` and watch aliases resolve in HubService
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            meta = SessionSummary(
                app="codex",
                session_id="s-sum-1",
                title="Watcher Hardening",
                workspace="/tmp/agent-hub-mcp",
                status=STATUS_IDLE,
                updated_at_ms=2_000_000_000_000,
            )

            class _RichFakeAdapter(_FakeAdapter):
                def read_session(self, session_id: str, **kwargs):  # noqa: ANN003
                    return {
                        "session": self.summary.to_dict(),
                        "message_count_returned": len(msgs),
                        "messages": msgs,
                    }

            adapter = _RichFakeAdapter("codex", meta)
            wstore = WatchStore(td_path / "watches.json")
            wstore.upsert(
                {
                    "alias": "汇报测试",
                    "app": "codex",
                    "session_id": "s-sum-1",
                    "notify_targets": ["qqbot"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            sent: list[tuple[str, str]] = []
            runner = WatchdogRunner(
                adapters={"codex": adapter},
                store=wstore,
                notifier=lambda t, m: (sent.append((t, m)) or (True, "ok")),
            )
            poll_res = runner.poll_once(now_ts=2_000_000_050.0)
            self.assertEqual(poll_res["event_count"], 1)
            pushed = sent[0][1]
            self.assertIn("回合完毕 / 等待输入", pushed)
            self.assertIn("**【任务目标】**", pushed)
            self.assertIn("**【实质变更与动作】**", pushed)
            self.assertIn("**【验证结果与产物】**", pushed)
            self.assertIn("completion_summary", poll_res["events"][0])

            svc = HubService(
                adapters={
                    "codex": adapter,
                    "dsh": _FakeAdapter("dsh", meta),
                    "antigravity": _FakeAdapter("antigravity", meta),
                    "cline": _FakeAdapter("cline", meta),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=wstore,
            )
            sum_via_watch_alias = svc.summarize_session("alias", "汇报测试")
            self.assertTrue(sum_via_watch_alias["ok"])
            self.assertEqual(sum_via_watch_alias["session_id"], "s-sum-1")
            ho = svc.handover_session("all", "汇报测试", target_hint="交给 DSH 复核")
            self.assertTrue(ho["ok"])
            self.assertIn("交给 DSH 复核", ho["handover_prompt"])

    def test_watchdog_channel_auto_fallback_and_anti_loss(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            now = 2_000_000_000.0
            meta = SessionSummary(
                app="dsh",
                session_id="s-fb-1",
                title="Fallback Test",
                status=STATUS_IDLE,
                updated_at_ms=int(now * 1000),
            )
            adapter = _FakeAdapter("dsh", meta, last_msg="完成所有计算。")
            attempts: list[str] = []
            fail_cfg = {"weixin": True, "qqbot": True}

            def mock_send(target: str, _msg: str) -> tuple[bool, str]:
                attempts.append(target)
                if fail_cfg.get(target):
                    return False, f"{target} failed (ret=-2)"
                return True, f"sent via {target}"

            wstore.upsert(
                {
                    "alias": "防丢降级",
                    "app": "dsh",
                    "session_id": "s-fb-1",
                    "notify_targets": ["weixin"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            runner = WatchdogRunner(adapters={"dsh": adapter}, store=wstore, notifier=mock_send)

            # Case 1: Both weixin and fallback qqbot fail -> MUST NOT advance last_status or last_notified_at
            r_fail = runner.poll_once(now_ts=now + 10)
            self.assertEqual(r_fail["event_count"], 1)
            self.assertFalse(r_fail["events"][0]["delivered"])
            self.assertEqual(attempts, ["weixin", "qqbot"])
            stored_fail = wstore.list_all()[0]
            self.assertEqual(stored_fail["last_status"], STATUS_RUNNING)
            self.assertEqual(stored_fail.get("last_notified_at", 0), 0)
            self.assertEqual(stored_fail.get("last_notified_event", ""), "")
            self.assertIn("last_delivery_error", stored_fail)

            # Case 2: Next poll with qqbot healthy retries and succeeds via auto-fallback
            attempts.clear()
            fail_cfg["qqbot"] = False
            r_ok = runner.poll_once(now_ts=now + 35)
            self.assertEqual(r_ok["event_count"], 1)
            self.assertTrue(r_ok["events"][0]["delivered"])
            self.assertEqual(attempts, ["weixin", "qqbot"])
            self.assertEqual(
                [d["target"] for d in r_ok["events"][0]["deliveries"]],
                ["weixin", "qqbot"],
            )
            self.assertTrue(r_ok["events"][0]["deliveries"][1]["fallback"])
            stored_ok = wstore.list_all()[0]
            self.assertEqual(stored_ok["last_status"], STATUS_IDLE)
            self.assertEqual(stored_ok["last_notified_event"], "completed")
            self.assertGreater(stored_ok["last_notified_at"], 0)
            self.assertNotIn("last_delivery_error", stored_ok)

    def test_watch_store_flock_concurrent_merge_preserves_external_edits(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wstore = WatchStore(Path(td) / "watches.json")
            now = 2_000_000_000.0
            m1 = SessionSummary(app="codex", session_id="s-lock-1", title="Lock 1", status=STATUS_IDLE, updated_at_ms=int(now * 1000))
            m_rm = SessionSummary(app="codex", session_id="s-lock-rm", title="Removed", status=STATUS_RUNNING, updated_at_ms=int(now * 1000))

            wstore.upsert(
                {
                    "watch_id": "w-1",
                    "alias": "保留任务",
                    "app": "codex",
                    "session_id": "s-lock-1",
                    "notify_targets": ["weixin"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            wstore.upsert(
                {
                    "watch_id": "w-rm",
                    "alias": "将被移除",
                    "app": "codex",
                    "session_id": "s-lock-rm",
                    "notify_targets": ["weixin"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )

            entered = threading.Event()
            release = threading.Event()

            def slow_verifier(_ctx: dict) -> dict:
                entered.set()
                release.wait(timeout=5.0)
                return {
                    "ok": True,
                    "classification": "completed",
                    "needs_attention": True,
                    "label": "回合完毕 / 等待输入",
                    "summary": "并发核验完成",
                }

            class _MapAdapter:
                app_name = "codex"

                def get_session_meta(self, sid: str) -> SessionSummary | None:
                    return {"s-lock-1": m1, "s-lock-rm": m_rm}.get(sid)

                def read_session(self, sid: str, **kwargs):  # noqa: ANN003
                    return {"session": m1.to_dict(), "message_count_returned": 1, "messages": [{"index": 1, "role": "assistant", "content": "done"}]}

            runner = WatchdogRunner(
                adapters={"codex": _MapAdapter()},
                store=wstore,
                notifier=lambda t, m: (True, "ok"),
                verifier=slow_verifier,
                verify_agent=True,
            )

            poll_result: dict = {}

            def _run_poll() -> None:
                poll_result["res"] = runner.poll_once(now_ts=now + 10)

            t = threading.Thread(target=_run_poll)
            t.start()
            self.assertTrue(entered.wait(timeout=3.0))

            # Concurrently mutate WatchStore while poll_once is waiting inside Level-2 verifier
            wstore.upsert(
                {
                    "watch_id": "w-1",
                    "alias": "保留任务",
                    "app": "codex",
                    "session_id": "s-lock-1",
                    "notify_targets": ["qqbot", "weixin"],
                    "enabled": True,
                }
            )
            wstore.upsert(
                {
                    "watch_id": "w-new",
                    "alias": "并发新增",
                    "app": "codex",
                    "session_id": "s-lock-new",
                    "notify_targets": ["qqbot"],
                    "enabled": True,
                    "last_status": STATUS_RUNNING,
                }
            )
            wstore.remove("w-rm")
            release.set()
            t.join(timeout=5.0)

            after = {w["watch_id"]: w for w in wstore.list_all()}
            self.assertIn("w-new", after)
            self.assertNotIn("w-rm", after)
            self.assertIn("w-1", after)
            self.assertEqual(after["w-1"]["notify_targets"], ["qqbot", "weixin"])
            self.assertEqual(after["w-1"]["last_status"], STATUS_IDLE)
            self.assertEqual(after["w-1"]["last_verification"]["summary"], "并发核验完成")

    def test_four_app_approval_status_and_antigravity_subagents(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)

            # 1. Codex waiting_approval on inProgress commandExecution / approvalRequest
            cx_dir = root / "codex"
            cx_dir.mkdir(parents=True)
            st_con = sqlite3.connect(cx_dir / "state_5.sqlite")
            st_con.execute(
                """
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY, title TEXT, name TEXT, preview TEXT, first_user_message TEXT,
                    cwd TEXT, updated_at INTEGER, updated_at_ms INTEGER, recency_at_ms INTEGER,
                    archived INTEGER, is_pinned INTEGER, source TEXT, thread_source TEXT,
                    attached_thread_parent_id TEXT, model TEXT, model_provider TEXT, reasoning_effort TEXT
                )
                """
            )
            st_con.executemany(
                "INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'desktop', NULL, NULL, 'o3', 'openai', 'high')",
                [
                    ("cx-ap-cmd", "Cmd Approval", "Cmd Approval", "p", "u", "/tmp", 1700000000, 1700000000000, 1700000000000),
                    ("cx-ap-req", "Req Approval", "Req Approval", "p", "u", "/tmp", 1700000001, 1700000001000, 1700000001000),
                ],
            )
            st_con.commit()
            st_con.close()

            h_con = sqlite3.connect(cx_dir / "thread_history_1.sqlite")
            h_con.execute("CREATE TABLE thread_turns (thread_id TEXT, rollout_ordinal INTEGER, status TEXT, error_json TEXT, started_at TEXT, completed_at TEXT)")
            h_con.execute("CREATE TABLE thread_items (thread_id TEXT, rollout_ordinal INTEGER, item_type TEXT, item_json TEXT, created_at_ms INTEGER)")
            h_con.executemany(
                "INSERT INTO thread_turns VALUES (?, 1, 'inProgress', NULL, '2026-11-24T00:00:00Z', NULL)",
                [("cx-ap-cmd",), ("cx-ap-req",)],
            )
            h_con.execute(
                "INSERT INTO thread_items VALUES ('cx-ap-cmd', 2, 'commandExecution', ?, 1700000000000)",
                (json.dumps({"command": "rm -rf build/", "status": "waiting_approval"}),),
            )
            h_con.execute(
                "INSERT INTO thread_items VALUES ('cx-ap-req', 2, 'approvalRequest', ?, 1700000001000)",
                (json.dumps({"tool": "run_command", "reason": "needs confirm"}),),
            )
            h_con.commit()
            h_con.close()

            cx_adapter = CodexAdapter(codex_dir=cx_dir)
            self.assertEqual(cx_adapter.get_session_meta("cx-ap-cmd").status, STATUS_WAITING_APPROVAL)
            self.assertEqual(cx_adapter.get_session_meta("cx-ap-req").status, STATUS_WAITING_APPROVAL)

            # 2. Cline waiting_approval / waiting_user across ui_messages.json and messages.json
            cl_dir = root / "cline"
            (cl_dir / "data" / "db").mkdir(parents=True)
            (cl_dir / "data" / "sessions").mkdir(parents=True)
            vsc_dir = root / "vsc_tasks"
            (vsc_dir / "task-ui-cmd").mkdir(parents=True)
            (vsc_dir / "task-ui-cmd" / "ui_messages.json").write_text(
                json.dumps(
                    [
                        {"type": "say", "say": "text", "text": "Initial task"},
                        {"type": "ask", "ask": "command", "text": "sbatch job.sh"},
                    ]
                ),
                encoding="utf-8",
            )
            (vsc_dir / "task-ui-follow").mkdir(parents=True)
            (vsc_dir / "task-ui-follow" / "ui_messages.json").write_text(
                json.dumps(
                    [
                        {"type": "say", "say": "text", "text": "Question"},
                        {"type": "ask", "ask": "followup", "text": "选哪个参数？"},
                    ]
                ),
                encoding="utf-8",
            )
            msg_ap_path = cl_dir / "data" / "sessions" / "s-msg-ap.json"
            msg_ap_path.write_text(
                json.dumps({"messages": [{"role": "assistant", "type": "ask", "ask": "tool", "approval_pending": True, "text": "write file"}]}),
                encoding="utf-8",
            )
            cl_con = sqlite3.connect(cl_dir / "data" / "db" / "sessions.db")
            cl_con.execute(
                """
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY, source TEXT, pid INTEGER, started_at TEXT,
                    ended_at TEXT, exit_code INTEGER, status TEXT, status_message TEXT,
                    cwd TEXT, workspace_root TEXT, prompt TEXT, metadata_json TEXT,
                    messages_path TEXT, transcript_path TEXT, updated_at TEXT,
                    is_subagent INTEGER, model TEXT, provider TEXT
                )
                """
            )
            cl_con.execute(
                "INSERT INTO sessions VALUES ('s-msg-ap', 'vscode', 1, '2026-11-24T00:00:00Z', NULL, NULL, 'running', '', '/tmp', '/tmp', 'p', '{}', ?, '', '2026-11-24T00:01:00Z', 0, 'm', 'p')",
                (str(msg_ap_path),),
            )
            cl_con.commit()
            cl_con.close()

            cl_adapter = ClineAdapter(cline_dir=cl_dir, vscode_tasks_dir=vsc_dir)
            self.assertEqual(cl_adapter.get_session_meta("task-ui-cmd").status, STATUS_WAITING_APPROVAL)
            self.assertEqual(cl_adapter.get_session_meta("task-ui-follow").status, STATUS_WAITING_USER)
            self.assertEqual(cl_adapter.get_session_meta("s-msg-ap").status, STATUS_WAITING_APPROVAL)

            # 3. Antigravity approval, ask_question, error vs recovered-DONE, and subagent filter
            ag_dir = root / "antigravity"
            ag_dir.mkdir(parents=True)
            ag_con = sqlite3.connect(ag_dir / "conversation_summaries.db")
            ag_con.execute(
                """
                CREATE TABLE conversation_summaries (
                    conversation_id TEXT PRIMARY KEY, title TEXT, preview TEXT,
                    step_count INTEGER, last_modified_time TEXT, workspace_uris TEXT,
                    status TEXT, not_fully_idle INTEGER, killed INTEGER,
                    parent_conversation_id TEXT
                )
                """
            )
            ag_con.executemany(
                "INSERT INTO conversation_summaries VALUES (?, ?, ?, 5, ?, '[]', 'CASCADE_RUN_STATUS_IDLE', 0, 0, ?)",
                [
                    ("ag-req-ap", "Plan Feedback", "p", "2026-11-24T05:00:00Z", ""),
                    ("ag-ask-q", "Ask User", "p", "2026-11-24T04:00:00Z", ""),
                    ("ag-err", "Step Error", "p", "2026-11-24T03:00:00Z", ""),
                    ("ag-recovered", "Recovered", "p", "2026-11-24T02:00:00Z", ""),
                    ("ag-sub", "Subagent", "p", "2026-11-24T01:00:00Z", "ag-req-ap"),
                ],
            )
            ag_con.commit()
            ag_con.close()

            def _write_transcript(cid: str, entries: list[dict]) -> None:
                p = ag_dir / "brain" / cid / ".system_generated" / "logs" / "transcript.jsonl"
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")

            _write_transcript(
                "ag-req-ap",
                [
                    {
                        "type": "PLANNER_RESPONSE",
                        "status": "DONE",
                        "content": "Plan ready",
                        "tool_calls": [{"name": "write_file", "arguments": {"ArtifactMetadata": {"RequestFeedback": True}}}],
                    }
                ],
            )
            _write_transcript(
                "ag-ask-q",
                [{"type": "PLANNER_RESPONSE", "status": "DONE", "content": "Pick one", "tool_calls": [{"name": "ask_question"}]}],
            )
            _write_transcript("ag-err", [{"type": "ERROR_MESSAGE", "status": "DONE", "content": "Quota exhausted"}])
            _write_transcript(
                "ag-recovered",
                [
                    {"type": "GENERIC", "status": "ERROR", "content": "transient"},
                    {"type": "PLANNER_RESPONSE", "status": "DONE", "content": "Recovered cleanly"},
                ],
            )

            ag_adapter = AntigravityAdapter(antigravity_dir=ag_dir)
            self.assertEqual(ag_adapter.get_session_meta("ag-req-ap").status, STATUS_WAITING_APPROVAL)
            self.assertEqual(ag_adapter.get_session_meta("ag-ask-q").status, STATUS_WAITING_USER)
            self.assertEqual(ag_adapter.get_session_meta("ag-err").status, STATUS_ERROR)
            self.assertEqual(ag_adapter.get_session_meta("ag-recovered").status, STATUS_IDLE)
            listed_default = [s.session_id for s in ag_adapter.list_sessions(limit=20, include_subagents=False)]
            self.assertNotIn("ag-sub", listed_default)
            listed_with_sub = [s.session_id for s in ag_adapter.list_sessions(limit=20, include_subagents=True)]
            self.assertIn("ag-sub", listed_with_sub)

    def test_dsh_read_session_tail_frames_and_budget_escalation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            harness = Path(td)
            zdir = harness / "sessions" / "ws" / "session-big-tail"
            zdir.mkdir(parents=True)
            zpath = zdir / "session.v4.jsonl.zstd"

            # Build 20 independent zstd frames (realistic append-only DSH session log)
            frames = bytearray()
            for i in range(20):
                line = json.dumps(
                    {
                        "seq": i + 1,
                        "type": "user/message" if i % 2 == 0 else "assistant/message",
                        "time": f"2026-11-24T01:{i:02d}:00Z",
                        "data": (
                            {"content": [{"type": "text", "text": f"turn-{i} " + ("x" * 180)}]}
                            if i % 2 == 0
                            else {"message": {"content": [{"type": "text", "text": f"turn-{i} " + ("y" * 180)}]}}
                        ),
                    }
                ) + "\n"
                frames.extend(zstd.compress(line.encode("utf-8")))
            zpath.write_bytes(bytes(frames))

            pc_dir = harness / "storages" / "session_projcache" / "sessions"
            pc_dir.mkdir(parents=True)
            (pc_dir / "session-big-tail.json").write_text(
                json.dumps({"record": {"identity": {"cwd": "/tmp"}, "rows": {"title": {"val": "Big Tail"}}}}),
                encoding="utf-8",
            )

            adapter = DshAdapter(harness_dir=harness)
            # Tail budget covers final ~3-4 frames -> returns latest 2 without full decompression
            r_small = adapter.read_session("session-big-tail", tail_messages=2, tail_budget_bytes=280)
            self.assertEqual([m["index"] for m in r_small["messages"]], [19, 20])

            # Larger tail_messages=10 triggers automatic budget escalation / full fallback cleanly
            r_escalate = adapter.read_session("session-big-tail", tail_messages=10, tail_budget_bytes=280)
            self.assertEqual(len(r_escalate["messages"]), 10)
            self.assertEqual(r_escalate["messages"][-1]["index"], 20)

    def test_remote_approval_cancel_and_audit_recent_across_bridges(self) -> None:
        # 1. Node integration test for Cline extension.js /approve, /cancel, /new-task, and port registry
        ext_path = Path(__file__).resolve().parent.parent / "vscode_cline_bridge" / "extension.js"
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            node_script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const Module = require('node:module');
const path = require('node:path');

const tmpDir = process.argv[2];
process.env.AGENT_HUB_CLINE_BRIDGE_STATE_PATH = path.join(tmpDir, 'cline-bridge.json');
process.env.AGENT_HUB_CLINE_BRIDGE_TOKEN_PATH = path.join(tmpDir, 'cline-bridge.token');

const events = [];
const state = {
  controller: {
    task: {
      ulid: 'task-ap-1',
      taskId: 'task-ap-1',
      messageStateHandler: {
        messages: [{ type: 'ask', ask: 'command', text: 'pytest -q', ts: 123456 }],
      },
      async handleWebviewAskResponse(kind, text) {
        events.push('resp:' + kind + ':' + text);
      },
    },
    async showTaskWithId(id) {
      if (id !== 'task-ap-1') throw new Error('missing ' + id);
    },
    async initTask(prompt) {
      events.push('init:' + prompt);
    },
    async clearTask() {},
    async postStateToWebview() {},
    async cancelTask() {
      events.push('cancelTask');
    },
  },
};
const clineApi = {
  startNewTask: async (prompt) => {
    setTimeout(() => {
      state.controller.task = {
        ulid: 'async-task-77',
        taskId: 'async-task-77',
        messageStateHandler: { messages: [] },
        async handleWebviewAskResponse() {},
      };
    }, 30);
    events.push('startNew:' + prompt);
  },
  sendMessage: async (msg) => {
    if (state.controller.task) await state.controller.task.handleWebviewAskResponse('messageResponse', msg);
  },
  pressPrimaryButton: async () => {
    events.push('primary');
    if (state.controller.task) await state.controller.task.handleWebviewAskResponse('yesButtonClicked', '');
  },
  pressSecondaryButton: async () => {
    events.push('secondary');
    if (state.controller.task) await state.controller.task.handleWebviewAskResponse('noButtonClicked', '');
  },
};
// Ensure inspector closes over state.controller
clineApi.sendMessage = ((t) => async (r) => { if (t.task) await t.task.handleWebviewAskResponse('messageResponse', r); })(state.controller);

const origLoad = Module._load;
Module._load = function(request) {
  if (request === 'vscode') {
    return {
      workspace: { workspaceFolders: [{ uri: { fsPath: '/flash/grp/xxllab/shennq' } }] },
      extensions: { getExtension: () => ({ isActive: true, exports: clineApi }) },
      commands: { executeCommand: async () => {} },
    };
  }
  return origLoad.apply(this, arguments);
};

const bridge = require(process.argv[1]);
const { createRequestHandler } = bridge._internals;

async function reqJson(port, method, route, token, body) {
  const rawBody = body ? JSON.stringify(body) : null;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: '127.0.0.1', port, path: route, method,
        headers: {
          Authorization: 'Bearer ' + token,
          ...(rawBody ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(rawBody) } : {}),
        },
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) }));
      }
    );
    req.on('error', reject);
    if (rawBody) req.write(rawBody);
    req.end();
  });
}

(async () => {
  const tok = 'd'.repeat(64);
  let boundPort = 0;
  const handler = createRequestHandler({
    token: tok,
    getPort: () => boundPort,
    getClineApiImpl: async () => ({ installed: true, active: true, api: clineApi }),
    executeCommandImpl: async () => {},
    getWorkspacesImpl: () => ['/flash/grp/xxllab/shennq'],
    sidebarFocusDelayMs: 5,
    postActivateSettleMs: 5,
    waitForActiveTaskTimeoutMs: 500,
  });
  const srv = http.createServer(handler);
  await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
  boundPort = srv.address().port;
  try {
    const h = await reqJson(boundPort, 'GET', '/health', tok);
    assert.equal(h.status, 200);
    assert.equal(h.body.hasPressPrimaryButton, true);
    assert.equal(h.body.hasCancelTask, true);
    assert.equal(h.body.pendingAsk.ask, 'command');

    const apYes = await reqJson(boundPort, 'POST', '/approve', tok, { sessionId: 'task-ap-1', decision: 'yes' });
    assert.equal(apYes.status, 200);
    assert.equal(apYes.body.decision, 'yes');
    assert.ok(events.includes('primary'));

    const apNo = await reqJson(boundPort, 'POST', '/approve', tok, { sessionId: 'task-ap-1', decision: 'no', feedback: '改为只读命令' });
    assert.equal(apNo.status, 200);
    assert.ok(events.includes('resp:noButtonClicked:改为只读命令'));

    const apBad = await reqJson(boundPort, 'POST', '/approve', tok, { sessionId: 'wrong-id', decision: 'yes' });
    assert.equal(apBad.status, 500);
    assert.equal(apBad.body.error_code, 'session_mismatch');

    const can = await reqJson(boundPort, 'POST', '/cancel', tok, { sessionId: 'task-ap-1' });
    assert.equal(can.status, 200);
    assert.ok(events.includes('cancelTask'));

    // Test /new-task waits for async task assignment
    state.controller.task = null;
    const nt = await reqJson(boundPort, 'POST', '/new-task', tok, { text: 'delayed task' });
    assert.equal(nt.status, 200);
    assert.equal(nt.body.sessionId, 'async-task-77');
  } finally {
    srv.close();
  }
})();
"""
            proc = subprocess.run(["node", "-e", node_script, str(ext_path), td], capture_output=True, text=True, timeout=20)
            self.assertEqual(proc.returncode, 0, f"Node approval/cancel test failed:\n{proc.stdout}\n{proc.stderr}")

            # 2. ClineWriter multi-window workspace routing + SessionMismatchError
            reg_dir = td_path / "cline-bridges"
            reg_dir.mkdir(parents=True)
            (reg_dir / "port-43918.json").write_text(json.dumps({"port": 43918, "workspaces": ["/tmp/ws-a"]}), encoding="utf-8")
            (reg_dir / "port-43919.json").write_text(json.dumps({"port": 43919, "workspaces": ["/flash/grp/xxllab/shennq"]}), encoding="utf-8")
            (td_path / "cline-bridge.token").write_text("x" * 64 + "\n", encoding="utf-8")
            cw = ClineWriter(token_path=td_path / "cline-bridge.token", state_path=td_path / "cline-bridge.json")
            self.assertEqual(
                [e["port"] for e in cw._candidate_bridge_entries(workspace="/flash/grp/xxllab/shennq")][0],
                43919,
            )

        # 3. CdpWriter splash filter, respond_approval (mismatch guard + click), cancel_session
        codex_w = CodexWriter()
        raw_targets = [
            {"type": "page", "url": "data:text/html;charset=utf-8,<svg>splash</svg>", "title": "data:text/html", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/1"},
            {"type": "page", "url": "app://-/index.html?initialRoute=%2Favatar-overlay", "title": "Overlay", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/2"},
            {"type": "page", "url": "app://-/index.html", "title": "ChatGPT", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/3"},
        ]
        with patch.object(CdpWriter, "_fetch_targets", return_value=raw_targets):
            port, pages = codex_w._discover_endpoint()
            self.assertEqual(port, 9222)
            self.assertEqual([p["title"] for p in pages], ["ChatGPT"])

        cdp_calls: list[dict] = []
        cdp_state = {"sid": "ag-ap-1", "clicked": ""}

        class _FakeApprovalWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, *args):  # noqa: ANN002
                return False

            def send_text(self, raw: str) -> None:
                cdp_calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = cdp_calls[-1]
                cid = last["id"]
                expr = (last.get("params") or {}).get("expression", "")
                if "__agentHubReadSessionIdentity" in expr:
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {
                                "result": {
                                    "value": {
                                        "sessionId": cdp_state["sid"],
                                        "title": "AG Task",
                                        "href": f"https://127.0.0.1:52123/c/{cdp_state['sid']}",
                                        "pathname": f"/c/{cdp_state['sid']}",
                                        "source": "pathname",
                                    }
                                }
                            },
                        }
                    )
                if "findBySessionId" in expr:
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "switched": False}}}})
                if "__agentHubRespondApproval" in expr:
                    cdp_state["clicked"] = "Approve"
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "clicked_label": "Approve"}}}})
                if "__agentHubCancelSession" in expr:
                    cdp_state["clicked"] = "Stop"
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "clicked_label": "Stop"}}}})
                return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})

        with (
            tempfile.TemporaryDirectory() as td,
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _FakeApprovalWS),
            patch.object(CdpWriter, "_discover_endpoint", return_value=(9224, [{"title": "Antigravity", "url": "https://127.0.0.1:52123/c/ag-ap-1", "webSocketDebuggerUrl": "ws://127.0.0.1:9224/p1"}])),
        ):
            ag_w = AntigravityWriter()
            ag_w.switch_verify_timeout_s = 0.05
            ag_w.switch_poll_interval_s = 0.01
            ok_ap = ag_w.respond_approval("ag-ap-1", "同意", target_title="AG Task")
            self.assertEqual(ok_ap["decision"], "yes")
            self.assertEqual(ok_ap["clicked_label"], "Approve")

            # Mismatch refuses click
            cdp_calls.clear()
            cdp_state["sid"] = "different-session-9"
            with self.assertRaises(SessionMismatchError):
                ag_w.respond_approval("ag-ap-1", "yes", target_title="AG Task")
            self.assertFalse(any("__agentHubRespondApproval" in (c.get("params") or {}).get("expression", "") for c in cdp_calls))

            # Cancel succeeds when matched
            cdp_state["sid"] = "ag-ap-1"
            ok_can = ag_w.cancel_session("ag-ap-1", target_title="AG Task")
            self.assertEqual(ok_can["clicked_label"], "Stop")

            # 4. HubService respond_approval, cancel_session, audit_recent, create_session(watch=True), relay_message(model=...)
            td_path = Path(td)
            meta_ag = SessionSummary(app="antigravity", session_id="ag-ap-1", title="AG Task", status=STATUS_WAITING_APPROVAL, updated_at_ms=2000)
            meta_dsh = SessionSummary(app="dsh", session_id="dsh-1", title="DSH 1", status=STATUS_RUNNING, updated_at_ms=1500)

            class _FullFakeWriter(_FakeWriter):
                def respond_approval(self, sid: str, decision: str = "yes", **kwargs):  # noqa: ANN003
                    self.calls.append({"action": "approve", "sid": sid, "decision": decision, **kwargs})
                    return {"ok": True, "sessionId": sid, "verified_session_id": sid, "decision": decision}

                def cancel_session(self, sid: str, **kwargs):  # noqa: ANN003
                    self.calls.append({"action": "cancel", "sid": sid, **kwargs})
                    return {"ok": True, "sessionId": sid, "verified_session_id": sid}

            dsh_fw = _FullFakeWriter("dsh", ready=True, supports_create=True)
            ag_adapter = _FakeAdapter("antigravity", meta_ag)
            dsh_adapter = _FakeAdapter("dsh", meta_dsh)

            # Ensure watch_add sees created fake-created-session-1 when create_session(watch=True) runs
            orig_dsh_meta = dsh_adapter.get_session_meta
            dsh_adapter.get_session_meta = lambda s: (
                SessionSummary(app="dsh", session_id="fake-created-session-1", title="NewWatched", status=STATUS_RUNNING, updated_at_ms=2000)
                if s == "fake-created-session-1"
                else orig_dsh_meta(s)
            )

            svc = HubService(
                adapters={"antigravity": ag_adapter, "dsh": dsh_adapter, "codex": _FakeAdapter("codex", meta_dsh), "cline": _FakeAdapter("cline", meta_dsh)},
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=WatchStore(td_path / "watches.json"),
                writers={"antigravity": ag_w, "dsh": dsh_fw, "codex": _FakeWriter("codex"), "cline": _FakeWriter("cline")},
            )
            dry_ap = svc.respond_approval("antigravity", "ag-ap-1", "yes", dry_run=True)
            self.assertTrue(dry_ap["dry_run"])
            real_ap = svc.respond_approval("antigravity", "ag-ap-1", "同意", feedback="ok")
            self.assertTrue(real_ap["ok"])
            self.assertEqual(real_ap["decision"], "yes")

            can_res = svc.cancel_session("dsh", "dsh-1")
            self.assertTrue(can_res["ok"])

            cr_watch = svc.create_session("dsh", title="NewWatched", initial_message="Run", watch=True, watch_alias="新建盯盘")
            self.assertTrue(cr_watch["ok"])
            self.assertTrue(cr_watch["watch"]["ok"])
            self.assertEqual(cr_watch["watch"]["watch"]["alias"], "新建盯盘")

            rel = svc.relay_message("antigravity", "ag-ap-1", "dsh", "dsh-1", dry_run=False, model="deepseek-reasoner", mode="steer")
            self.assertTrue(rel["ok"])
            self.assertEqual(rel["mode"], "steer")

            aud_all = svc.audit_recent(limit=10)
            self.assertGreaterEqual(aud_all["count"], 4)
            aud_ap = svc.audit_recent(limit=10, app="antigravity", action="respond_approval")
            self.assertEqual(aud_ap["count"], 1)
            self.assertEqual(aud_ap["entries"][0]["status"], "approved")


class TestCrossTalkProtectionAndMutex(unittest.TestCase):
    """Verify incident anti-cross-talk Fiber routing, title gate, cross-process mutex, and live DOM reads."""

    def test_incident_replay_class_b_vs_prja_fiber_nav_and_title_guard(self) -> None:
        proja_sid = "aaaaaaaa-1111-4111-8111-111111111111"
        challenge_sid = "bbbbbbbb-2222-4222-8222-222222222222"
        calls: list[dict] = []
        state: dict[str, Any] = {
            "active_sid": challenge_sid,
            "doc_title": "Branch · Class B",
            "pathname": f"/g/g-p-zuoye/c/{challenge_sid}",
            "consistent": True,
            "conflict": "",
            "mode": "stuck",  # 'stuck' | 'title_contradict' | 'conflict' | 'fiber_ok'
            "poll_after_switch": 0,
        }

        class _IncidentWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, *args):  # noqa: ANN002
                return False

            def send_text(self, raw: str) -> None:
                calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = calls[-1]
                cid = last["id"]
                expr = (last.get("params") or {}).get("expression", "")
                if "__agentHubReadSessionIdentity" in expr:
                    if state["mode"] == "fiber_ok" and state.get("switched"):
                        state["poll_after_switch"] += 1
                        if state["poll_after_switch"] >= 1:
                            state["active_sid"] = proja_sid
                            state["doc_title"] = "Analytics Project A"
                            state["pathname"] = f"/g/g-p-proja/c/{proja_sid}"
                    val = {
                        "sessionId": state["active_sid"],
                        "title": state["doc_title"],
                        "docTitle": state["doc_title"],
                        "href": "app://-/index.html",
                        "pathname": state["pathname"],
                        "source": "router-fiber",
                        "consistent": state["consistent"],
                        "conflict": state["conflict"],
                    }
                    return json.dumps({"id": cid, "result": {"result": {"value": val}}})
                if "findBySessionId" in expr:
                    state["switched"] = True
                    if state["mode"] == "title_contradict":
                        # Simulate router ID changing while document.title lags on old session!
                        state["active_sid"] = proja_sid
                        state["doc_title"] = "Branch · Class B"
                    elif state["mode"] == "conflict":
                        state["active_sid"] = proja_sid
                        state["doc_title"] = "Analytics Project A"
                        state["consistent"] = False
                        state["conflict"] = f"router={proja_sid} vs sidebar={challenge_sid}"
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {
                                "result": {
                                    "value": {
                                        "ok": True,
                                        "switched": True,
                                        "switchMethod": "fiber-router-navigate",
                                        "navigatedTo": f"/g/g-p-proja/c/{proja_sid}",
                                    }
                                }
                            },
                        }
                    )
                if "__agentHubVerifyPostSend" in expr:
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {
                                "result": {
                                    "value": {
                                        "composer_cleared": True,
                                        "is_generating": True,
                                        "message_appeared": True,
                                    }
                                }
                            },
                        }
                    )
                if "window.__hermes_active_composer" in expr and "sendBtn" in expr:
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "method": "button"}}}})
                if "window.__hermes_active_composer" in expr:
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "isContentEditable": True}}}})
                if "preComposer" in expr or "composer-not-found" in expr:
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})
                if "composer_found" in expr:
                    return json.dumps({"id": cid, "result": {"result": {"value": {"composer_found": True, "has_unsaved_draft": False}}}})
                return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})

        with (
            tempfile.TemporaryDirectory() as td,
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _IncidentWS),
            patch.object(
                CdpWriter,
                "_discover_endpoint",
                return_value=(9222, [{"title": "Branch · Class B", "url": "app://-/index.html", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/p1"}]),
            ),
        ):
            td_path = Path(td)
            codex_w = CodexWriter()
            codex_w._lock_dir = td_path
            codex_w.switch_verify_timeout_s = 0.08
            codex_w.switch_poll_interval_s = 0.02

            # Case A: Stuck on Class B -> must abort with SessionMismatchError, zero injection
            with self.assertRaises(SessionMismatchError) as ctx_a:
                codex_w.send_message(proja_sid, "PRJ_A Stage 3 验收报告", target_title="Analytics Project A")
            self.assertEqual(ctx_a.exception.actual_session_id, challenge_sid)
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))

            # Case B1: Router ID updated but document.title still contradicts -> Fail Closed!
            calls.clear()
            state.update({"active_sid": challenge_sid, "doc_title": "Branch · Class B", "consistent": True, "conflict": "", "mode": "title_contradict", "switched": False})
            with self.assertRaises(SessionMismatchError) as ctx_b1:
                codex_w.send_message(proja_sid, "PRJ_A Stage 3 验收报告", target_title="Analytics Project A")
            self.assertIn("contradiction", str(ctx_b1.exception))
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))

            # Case B2: Router vs Sidebar conflict -> Fail Closed!
            calls.clear()
            state.update({"active_sid": challenge_sid, "doc_title": "Branch · Class B", "consistent": True, "conflict": "", "mode": "conflict", "switched": False})
            with self.assertRaises(SessionMismatchError) as ctx_b2:
                codex_w.send_message(proja_sid, "PRJ_A Stage 3 验收报告", target_title="Analytics Project A")
            self.assertIn("inconsistent", str(ctx_b2.exception))
            self.assertFalse(any(c["method"] == "Input.insertText" for c in calls))

            # Case C: Smooth Fiber navigate from Class B -> PRJ_A + HubService cloud UUID support
            calls.clear()
            state.update({"active_sid": challenge_sid, "doc_title": "Branch · Class B", "consistent": True, "conflict": "", "mode": "fiber_ok", "switched": False, "poll_after_switch": 0})
            codex_w.switch_verify_timeout_s = 0.5
            svc = HubService(
                adapters={
                    "codex": _FakeAdapter("codex", SessionSummary(app="codex", session_id="other-local", title="Other", status=STATUS_IDLE)),
                    "dsh": _FakeAdapter("dsh", SessionSummary(app="dsh", session_id="d1", title="D1", status=STATUS_IDLE)),
                    "antigravity": _FakeAdapter("antigravity", SessionSummary(app="antigravity", session_id="a1", title="A1", status=STATUS_IDLE)),
                    "cline": _FakeAdapter("cline", SessionSummary(app="cline", session_id="c1", title="C1", status=STATUS_IDLE)),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=WatchStore(td_path / "watches.json"),
                relays=RelayStore(td_path / "relay.sqlite"),
                writers={
                    "codex": codex_w,
                    "dsh": _FakeWriter("dsh", ready=False),
                    "antigravity": _FakeWriter("antigravity", ready=False),
                    "cline": _FakeWriter("cline", ready=False),
                },
            )
            res = svc.send_message("codex", proja_sid, "PRJ_A Stage 3 验收报告")
            self.assertTrue(res["ok"])
            self.assertEqual(res["verified_session_id"], proja_sid)
            self.assertEqual(res["verified_title"], "Analytics Project A")
            self.assertEqual(res["bridge_result"]["switch_method"], "fiber-router-navigate")
            self.assertTrue(res["bridge_result"]["message_appeared"])
            self.assertTrue(any(c["method"] == "Input.insertText" and "PRJ_A Stage 3" in c["params"]["text"] for c in calls))

    def test_concurrent_app_mutex_serializes_threads_and_cross_process_flock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)

            # 1. Thread-level serialization verification
            spans: list[tuple[str, float, float]] = []

            class _SlowWS:
                def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                    self.reqs: list[dict] = []

                def __enter__(self):
                    return self

                def __exit__(self, *args):  # noqa: ANN002
                    return False

                def send_text(self, raw: str) -> None:
                    self.reqs.append(json.loads(raw))

                def recv_text(self) -> str:
                    last = self.reqs[-1]
                    cid = last["id"]
                    expr = (last.get("params") or {}).get("expression", "")
                    if "__agentHubReadSessionIdentity" in expr:
                        # Identify current session from which thread called? Just return a matching sentinel per test
                        return json.dumps(
                            {
                                "id": cid,
                                "result": {
                                    "result": {
                                        "value": {
                                            "sessionId": active_holder["sid"],
                                            "title": "Concurrent",
                                            "docTitle": "Concurrent",
                                            "href": "app://-/index.html",
                                            "pathname": f"/c/{active_holder['sid']}",
                                            "source": "router-fiber",
                                            "consistent": True,
                                        }
                                    }
                                },
                            }
                        )
                    if "window.__hermes_active_composer" in expr and "sendBtn" in expr:
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "method": "button"}}}})
                    if "window.__hermes_active_composer" in expr:
                        return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "isContentEditable": True}}}})
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})

            active_holder = {"sid": ""}
            codex_w = CodexWriter()
            codex_w._lock_dir = td_path
            codex_w.lock_timeout_s = 5.0

            def _wrapped_inject(self_w, ws, text):  # noqa: ANN001
                t0 = time.monotonic()
                time.sleep(0.10)
                t1 = time.monotonic()
                spans.append((text, t0, t1))
                return {"ok": True, "method": "button"}

            results: list[dict] = []
            errors: list[Exception] = []

            def _worker(sid: str, msg: str) -> None:
                try:
                    # Wait briefly after mutex entry is checked via _discover_endpoint hook
                    res = codex_w.send_message(sid, msg)
                    results.append(res)
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

            def _discover_for_thread():
                # Only called while _acquire_app_mutex() is held!
                active_holder["sid"] = "sess-1" if not spans else "sess-2"
                return (9222, [{"title": "Concurrent", "url": "app://-/index.html", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/p1"}])

            with (
                patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _SlowWS),
                patch.object(CdpWriter, "_discover_endpoint", side_effect=_discover_for_thread),
                patch.object(CdpWriter, "_cdp_inject_and_send", _wrapped_inject),
            ):
                t1 = threading.Thread(target=_worker, args=("sess-1", "msg-1"))
                t2 = threading.Thread(target=_worker, args=("sess-2", "msg-2"))
                t1.start()
                time.sleep(0.02)
                t2.start()
                t1.join(timeout=5.0)
                t2.join(timeout=5.0)

            self.assertEqual(errors, [])
            self.assertEqual(len(results), 2)
            spans.sort(key=lambda item: item[1])
            self.assertLessEqual(spans[0][2], spans[1][1] + 0.001, f"Concurrent CDP sections overlapped: {spans}")

            # 2. Cross-process fcntl.flock contention verification
            lock_file = td_path / ".cdp-mutex-codex.lock"
            child_code = (
                "import fcntl, os, sys, time\n"
                "fd = os.open(sys.argv[1], os.O_CREAT | os.O_RDWR, 0o600)\n"
                "fcntl.flock(fd, fcntl.LOCK_EX)\n"
                "sys.stdout.write('LOCKED\\n'); sys.stdout.flush()\n"
                "time.sleep(0.35)\n"
                "fcntl.flock(fd, fcntl.LOCK_UN); os.close(fd)\n"
            )
            proc = subprocess.Popen([sys.executable, "-c", child_code, str(lock_file)], stdout=subprocess.PIPE, text=True)
            try:
                assert proc.stdout is not None
                line = proc.stdout.readline().strip()
                self.assertEqual(line, "LOCKED")

                fast_w = CodexWriter()
                fast_w._lock_dir = td_path
                fast_w.lock_timeout_s = 0.12
                with self.assertRaises(TimeoutError):
                    with fast_w._acquire_app_mutex():
                        pass

                patient_w = CodexWriter()
                patient_w._lock_dir = td_path
                patient_w.lock_timeout_s = 2.0
                with patient_w._acquire_app_mutex():
                    entered = True
                self.assertTrue(entered)
            finally:
                proc.wait(timeout=5.0)

    def test_cdp_read_live_session_verifies_identity_and_extracts_messages(self) -> None:
        calls: list[dict] = []
        state = {"sid": "aaaaaaaa-1111-4111-8111-111111111111", "title": "Analytics Project A"}

        class _LiveReadWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, *args):  # noqa: ANN002
                return False

            def send_text(self, raw: str) -> None:
                calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = calls[-1]
                cid = last["id"]
                expr = (last.get("params") or {}).get("expression", "")
                if "__agentHubReadSessionIdentity" in expr:
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {
                                "result": {
                                    "value": {
                                        "sessionId": state["sid"],
                                        "title": state["title"],
                                        "docTitle": state["title"],
                                        "href": "app://-/index.html",
                                        "pathname": f"/c/{state['sid']}",
                                        "source": "router-fiber",
                                        "consistent": True,
                                    }
                                }
                            },
                        }
                    )
                if "findBySessionId" in expr:
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "switched": False, "switchMethod": "not-found"}}}})
                if "__agentHubReadLiveMessages" in expr:
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {
                                "result": {
                                    "value": {
                                        "is_generating": False,
                                        "messages": [
                                            {"index": 1, "role": "user", "content": "请审核 P0"},
                                            {"index": 2, "role": "assistant", "content": "终审裁决：P0 已关闭"},
                                        ],
                                    }
                                }
                            },
                        }
                    )
                return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})

        with (
            tempfile.TemporaryDirectory() as td,
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _LiveReadWS),
            patch.object(
                CdpWriter,
                "_discover_endpoint",
                return_value=(9222, [{"title": "Analytics Project A", "url": "app://-/index.html", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/p1"}]),
            ),
        ):
            cw = CodexWriter()
            cw._lock_dir = Path(td)
            cw.switch_verify_timeout_s = 0.05
            live = cw.read_live_session("aaaaaaaa-1111-4111-8111-111111111111", target_title="Analytics Project A")
            self.assertEqual(live["verified_session_id"], "aaaaaaaa-1111-4111-8111-111111111111")
            self.assertEqual(len(live["messages"]), 2)
            self.assertEqual(live["messages"][-1]["content"], "终审裁决：P0 已关闭")

            with self.assertRaises(SessionMismatchError):
                cw.read_live_session("bbbbbbbb-2222-4222-8222-222222222222", target_title="Branch · Class B")

    def test_cdp_read_live_session_uses_fiber_and_restores_previous_session(self) -> None:
        prev_sid = "dddddddd-4444-4444-8444-444444444444"
        target_sid = "aaaaaaaa-1111-4111-8111-111111111111"
        calls: list[dict] = []
        state: dict[str, Any] = {"active": prev_sid, "switches": []}

        class _FiberReadWS:
            def __init__(self, url: str, timeout: float = 15.0) -> None:  # noqa: ARG002
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, *args):  # noqa: ANN002
                return False

            def send_text(self, raw: str) -> None:
                calls.append(json.loads(raw))

            def recv_text(self) -> str:
                last = calls[-1]
                cid = last["id"]
                expr = (last.get("params") or {}).get("expression", "")
                if "__agentHubReadSessionIdentity" in expr:
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {"result": {"value": {
                                "sessionId": state["active"],
                                "title": "Analytics Project B" if state["active"] == prev_sid else "Analytics Project A",
                                "docTitle": "Analytics Project B" if state["active"] == prev_sid else "Analytics Project A",
                                "href": "app://-/index.html",
                                "pathname": f"/c/{state['active']}",
                                "source": "router-fiber",
                                "consistent": True,
                            }}},
                        }
                    )
                if "findBySessionId" in expr:
                    m = re.search(r'"sessionId":\s*"([^"]+)"', expr)
                    sid = m.group(1) if m else ""
                    state["switches"].append(sid)
                    state["active"] = sid
                    return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True, "switched": True, "switchMethod": "fiber-router-navigate"}}}})
                if "__agentHubReadLiveMessages" in expr:
                    return json.dumps(
                        {
                            "id": cid,
                            "result": {"result": {"value": {
                                "source": "fiber",
                                "is_generating": False,
                                "messages": [
                                    {"index": 1, "role": "user", "content": "请审核 P0"},
                                    {
                                        "index": 2,
                                        "role": "assistant",
                                        "content": "完整正文 answered fully \ue200genui\ue202{\"widget\":1}\ue201 end",
                                    },
                                ],
                            }}},
                        }
                    )
                return json.dumps({"id": cid, "result": {"result": {"value": {"ok": True}}}})

        with (
            tempfile.TemporaryDirectory() as td,
            patch("agent_hub.writers.cdp_writer.SimpleWebSocket", _FiberReadWS),
            patch.object(
                CdpWriter,
                "_discover_endpoint",
                return_value=(9222, [{"title": "Analytics Project B", "url": "app://-/index.html", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/p1"}]),
            ),
        ):
            cw = CodexWriter()
            cw._lock_dir = Path(td)
            cw.switch_verify_timeout_s = 0.3
            cw.switch_poll_interval_s = 0.01
            live = cw.read_live_session(target_sid, target_title="Analytics Project A", tail_messages=5)

        self.assertEqual(live["verified_session_id"], target_sid)
        self.assertEqual(live["session"]["extra"]["extraction"], "fiber")
        self.assertEqual(len(live["messages"]), 2)
        body = live["messages"][1]["content"]
        self.assertIn("answered fully", body)
        self.assertIn("end", body)
        self.assertNotIn("\ue200", body)
        self.assertNotIn("genui", body)
        # Switched to the target first, then restored the previously active session.
        self.assertEqual(state["switches"], [target_sid, prev_sid])
        self.assertEqual(state["active"], prev_sid)

    def test_default_hermes_verify_cmd_uses_provider_toolsets_and_timeout(self) -> None:
        from agent_hub.watcher import default_hermes_verify

        captured: dict[str, Any] = {}

        class _P:
            returncode = 0
            stdout = '{"classification":"completed","needs_attention":false,"label":"回合完毕","summary":"done"}'
            stderr = ""

        def _fake_run(cmd, **kwargs):  # noqa: ANN001, ANN003
            captured["cmd"] = list(cmd)
            captured["kwargs"] = dict(kwargs)
            return _P()

        with patch("agent_hub.watcher.subprocess.run", side_effect=_fake_run):
            verdict = default_hermes_verify(
                {"watch": {"app": "cline", "session_id": "s1"}, "event_type": "error"},
                model="deepseek-v4.1-flash",
                provider="custom:qqbot-deepseek",
                toolsets="terminal",
                timeout=90,
            )

        self.assertTrue(verdict["ok"])
        cmd = captured["cmd"]
        self.assertIn("-m", cmd)
        self.assertIn("deepseek-v4.1-flash", cmd)
        self.assertIn("--provider", cmd)
        self.assertIn("custom:qqbot-deepseek", cmd)
        self.assertIn("-t", cmd)
        self.assertIn("terminal", cmd)
        self.assertIn("-z", cmd)
        self.assertEqual(captured["kwargs"].get("timeout"), 90)

    def test_cline_stale_terminal_exit_after_completed_round_is_idle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cl_dir = Path(td) / "cline"
            (cl_dir / "data" / "db").mkdir(parents=True)
            (cl_dir / "data" / "sessions").mkdir(parents=True)
            vsc_dir = Path(td) / "vsc_tasks"
            vsc_dir.mkdir(parents=True)

            msg_done = cl_dir / "data" / "sessions" / "s-done.json"
            msg_done.write_text(
                json.dumps({"messages": [
                    {"role": "user", "content": "do work"},
                    {"role": "assistant", "content": "全部交付完成\n\n# 执行完毕"},
                ]}),
                encoding="utf-8",
            )
            msg_ask = cl_dir / "data" / "sessions" / "s-ask.json"
            msg_ask.write_text(
                json.dumps({"messages": [
                    {"role": "assistant", "content": "working"},
                    {"role": "assistant", "type": "ask", "ask": "api_req_failed", "text": "API error"},
                ]}),
                encoding="utf-8",
            )

            con = sqlite3.connect(cl_dir / "data" / "db" / "sessions.db")
            con.execute(
                "CREATE TABLE sessions (session_id TEXT PRIMARY KEY, source TEXT, pid INTEGER, started_at TEXT, ended_at TEXT, exit_code INTEGER, status TEXT, status_message TEXT, cwd TEXT, workspace_root TEXT, prompt TEXT, metadata_json TEXT, messages_path TEXT, transcript_path TEXT, updated_at TEXT, is_subagent INTEGER, model TEXT, provider TEXT)"
            )
            con.executemany(
                "INSERT INTO sessions VALUES (?, 'vscode', 1, '2026-11-25T10:00:00Z', '2030-01-01T00:00:00Z', 1, 'failed', '', '/tmp', '/tmp', 'p', '{}', ?, '', '2026-11-25T11:38:00Z', 0, 'm', 'p')",
                [("s-done", str(msg_done)), ("s-ask", str(msg_ask))],
            )
            con.commit()
            con.close()

            ad = ClineAdapter(cline_dir=cl_dir, vscode_tasks_dir=vsc_dir)
            done_meta = ad.get_session_meta("s-done")
            self.assertIsNotNone(done_meta)
            self.assertEqual(done_meta.status, STATUS_IDLE)
            self.assertIn("回合已结束", done_meta.status_detail)
            self.assertTrue(done_meta.extra.get("stale_terminated"))

            ask_meta = ad.get_session_meta("s-ask")
            self.assertIsNotNone(ask_meta)
            self.assertEqual(ask_meta.status, STATUS_ERROR)


class TestNativeRelayBroker(unittest.TestCase):
    """Verify SQLite RelayStore, idempotent transactions, debounce, echo suppression, and closed-loop broker."""

    def test_relay_store_crud_idempotent_tx_and_notification_dedupe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "relay_states.sqlite"
            store = RelayStore(db_path)

            link = store.upsert_link(
                name="prja-loop",
                source_app="cline",
                source_session_id="cline-prja-1",
                source_title="Cline PRJ_A",
                target_app="codex",
                target_session_id="aaaaaaaa-1111-4111-8111-111111111111",
                target_title="Analytics Project A",
                bidirectional=True,
                policy={"settle_seconds": 20, "max_hops": 10},
                state={"last_relayed_hash_a": "abc", "hop_count": 0},
            )
            self.assertTrue(link["relay_id"].startswith("r_"))
            self.assertEqual(len(store.list_links()), 1)

            chash = canonical_message_hash("[via Hermes]\n\nStage 3 PASS  \r\n")
            self.assertEqual(chash, canonical_message_hash("Stage 3 PASS"))
            self.assertTrue(is_relay_echo("[Agent Hub Relay | prja-loop | cline -> codex]\n\nhello"))
            self.assertTrue(is_relay_echo("[via Hermes | relayed from codex:x]\n\nhello"))
            self.assertFalse(is_relay_echo("普通输出内容"))

            self.assertFalse(store.has_sent_tx(link["relay_id"], "a_to_b", chash))
            store.record_tx(
                {
                    "relay_id": link["relay_id"],
                    "direction": "a_to_b",
                    "source_app": "cline",
                    "source_session_id": "cline-prja-1",
                    "target_app": "codex",
                    "target_session_id": "aaaaaaaa-1111-4111-8111-111111111111",
                    "source_msg_index": 3,
                    "content_hash": chash,
                    "status": "sent",
                    "audit_id": "aud-1",
                    "verified_session_id": "aaaaaaaa-1111-4111-8111-111111111111",
                }
            )
            self.assertTrue(store.has_sent_tx(link["relay_id"], "a_to_b", chash))
            self.assertEqual(len(store.recent_transactions(relay_id=link["relay_id"])), 1)

            nkey = store.notif_key_for(link["relay_id"], "milestone", chash[:12])
            self.assertFalse(store.has_notified(nkey))
            store.record_notification(nkey, link["relay_id"], "milestone", ["weixin"], True, "ok")
            self.assertTrue(store.has_notified(nkey))

            paused = store.set_enabled("prja-loop", False)
            self.assertFalse(paused["enabled"])
            self.assertEqual(len(store.list_links(include_disabled=False)), 0)
            self.assertEqual(len(store.list_links(include_disabled=True)), 1)

            deleted = store.delete_link("prja-loop")
            self.assertIsNotNone(deleted)
            self.assertEqual(len(store.list_links(include_disabled=True)), 0)

    def test_bidirectional_relay_broker_closed_loop_debounce_and_milestone_alerts(self) -> None:
        class _ScriptedEndpointAdapter:
            def __init__(self, app_name: str, sid: str, title: str, initial_text: str) -> None:
                self.app_name = app_name
                self.summary = SessionSummary(app=app_name, session_id=sid, title=title, status=STATUS_IDLE, updated_at_ms=1000)
                self.messages: list[dict[str, Any]] = [{"index": 1, "role": "assistant", "content": initial_text}]

            def list_sessions(self, **kwargs):  # noqa: ANN003
                return [self.summary]

            def search_sessions(self, query: str, **kwargs):  # noqa: ANN003
                return []

            def get_session_meta(self, session_id: str) -> SessionSummary | None:
                return self.summary if session_id == self.summary.session_id else None

            def read_session(self, session_id: str, **kwargs):  # noqa: ANN003
                return {
                    "session": self.summary.to_dict(),
                    "message_count_returned": len(self.messages),
                    "messages": list(self.messages),
                }

        class _ScriptedWriter:
            def __init__(self, app_name: str, *, mismatch_on_send: bool = False) -> None:
                self.app_name = app_name
                self.mismatch_on_send = mismatch_on_send
                self.sent: list[dict[str, Any]] = []

            def status(self) -> dict[str, Any]:
                return {"app": self.app_name, "ready": True, "transport": "scripted"}

            def send_message(self, session_id: str, text: str, *, target_title: str = "", mode: str = "queue") -> dict[str, Any]:
                if self.mismatch_on_send:
                    raise SessionMismatchError(
                        app_name=self.app_name,
                        expected_session_id=session_id,
                        expected_title=target_title,
                        actual_session_id="bbbbbbbb-2222-4222-8222-222222222222",
                        actual_title="Branch · Class B",
                        reason="mock mismatch",
                    )
                self.sent.append({"session_id": session_id, "text": text, "mode": mode})
                return {
                    "ok": True,
                    "sessionId": session_id,
                    "verified_session_id": session_id,
                    "verified_title": target_title,
                    "mode": mode,
                }

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            cline_ep = _ScriptedEndpointAdapter("cline", "cline-prja-1", "Cline PRJ_A", "旧历史消息不应被重放")
            codex_ep = _ScriptedEndpointAdapter("codex", "aaaaaaaa-1111-4111-8111-111111111111", "Analytics Project A", "旧审阅记录")
            cline_w = _ScriptedWriter("cline")
            codex_w = _ScriptedWriter("codex")
            pushes: list[tuple[str, str]] = []

            svc = HubService(
                adapters={
                    "cline": cline_ep,
                    "codex": codex_ep,
                    "dsh": _FakeAdapter("dsh", SessionSummary(app="dsh", session_id="d1", title="D1", status=STATUS_IDLE)),
                    "antigravity": _FakeAdapter("antigravity", SessionSummary(app="antigravity", session_id="a1", title="A1", status=STATUS_IDLE)),
                },
                bindings=BindingStore(td_path / "bindings.json"),
                audit=AuditLog(td_path / "audit.jsonl"),
                watches=WatchStore(td_path / "watches.json"),
                relays=RelayStore(td_path / "relay_states.sqlite"),
                writers={
                    "cline": cline_w,
                    "codex": codex_w,
                    "dsh": _FakeWriter("dsh", ready=False),
                    "antigravity": _FakeWriter("antigravity", ready=False),
                },
            )
            broker = RelayBroker(svc, store=svc.relays, notifier=lambda t, m: (pushes.append((t, m)) or (True, "ok")))

            reg = broker.create_or_update_link(
                "cline",
                "cline-prja-1",
                "codex",
                "aaaaaaaa-1111-4111-8111-111111111111",
                name="prja-loop",
                bidirectional=True,
                settle_seconds=20,
                min_hop_interval_seconds=0,
                max_hops=5,
                notify_targets="weixin",
                notify_on_hop=False,
                notify_on_milestone=True,
                stop_patterns=[r"阶段关闭"],
            )
            self.assertTrue(reg["ok"])
            rid = reg["relay"]["relay_id"]

            # 1. Baseline snapshot: immediate poll does nothing
            t0 = 2_000_000_000.0
            r_base = broker.poll_once(now_ts=t0)
            self.assertEqual(r_base["actions_count"], 0)
            self.assertEqual(len(codex_w.sent), 0)

            # 2. Cline produces new report -> debouncing, then changes mid-stream (resets timer), then settles -> silent dispatch!
            cline_ep.messages.append({"index": 2, "role": "assistant", "content": "Stage 3 初步草稿"})
            r_d1 = broker.poll_once(now_ts=t0 + 10)
            self.assertEqual(r_d1["actions"][0]["status"], "debouncing")

            cline_ep.messages.append({"index": 3, "role": "assistant", "content": "Stage 3 完整报告：12/12 校验完成"})
            r_d2 = broker.poll_once(now_ts=t0 + 20)
            self.assertEqual(r_d2["actions"][0]["status"], "debouncing")
            self.assertEqual(len(codex_w.sent), 0)

            r_d3 = broker.poll_once(now_ts=t0 + 35)
            self.assertEqual(r_d3["actions"][0]["status"], "debouncing")  # only 15s since t0+20
            self.assertEqual(len(codex_w.sent), 0)

            r_send1 = broker.poll_once(now_ts=t0 + 42)
            self.assertEqual(r_send1["actions"][0]["status"], "sent")
            self.assertEqual(len(codex_w.sent), 1)
            self.assertIn("Stage 3 完整报告", codex_w.sent[0]["text"])
            self.assertEqual(len(pushes), 0)  # routine hop is SILENT!

            # 3. Codex replies routinely -> B->A silent hop, zero pushes
            codex_ep.messages.append({"index": 2, "role": "assistant", "content": "请补充 B1 运行细节数据"})
            broker.poll_once(now_ts=t0 + 50)
            r_send2 = broker.poll_once(now_ts=t0 + 75)
            self.assertEqual(r_send2["actions"][0]["direction"], "b_to_a")
            self.assertEqual(r_send2["actions"][0]["status"], "sent")
            self.assertEqual(len(cline_w.sent), 1)
            self.assertEqual(len(pushes), 0)

            # 4. Echo suppression: relayed header message is never bounced back
            cline_ep.messages.append({"index": 4, "role": "assistant", "content": "[Agent Hub Relay | prja-loop | codex -> cline]\n\necho"})
            r_echo = broker.poll_once(now_ts=t0 + 80)
            self.assertEqual(r_echo["actions_count"], 0)

            # 5. Codex emits milestone + stop_pattern -> relays once, pushes 1 formatted Weixin card, auto-pauses!
            codex_ep.messages.append({"index": 3, "role": "assistant", "content": "终审裁决：Stage 3 验收通过，ALL PASS，阶段关闭。"})
            broker.poll_once(now_ts=t0 + 90)
            r_mile = broker.poll_once(now_ts=t0 + 115)
            self.assertEqual(r_mile["actions"][0]["status"], "sent")
            self.assertTrue(r_mile["actions"][0]["auto_stopped"])
            self.assertEqual(len(cline_w.sent), 2)
            self.assertEqual(len(pushes), 1)
            self.assertEqual(pushes[0][0], "weixin")
            self.assertIn("中继自动暂停", pushes[0][1])

            # Subsequent polls do nothing (link disabled, zero duplicate notifications)
            r_after = broker.poll_once(now_ts=t0 + 150)
            self.assertEqual(r_after["checked_links"], 0)
            self.assertEqual(len(pushes), 1)

            # 6. SessionMismatchError during relay -> aborted_mismatch tx + alert card + zero target injection
            codex_w.mismatch_on_send = True
            svc.relays.set_enabled(rid, True)
            cline_ep.messages.append({"index": 5, "role": "assistant", "content": "新的实验请求需要复核"})
            broker.poll_once(now_ts=t0 + 160)
            r_mm = broker.poll_once(now_ts=t0 + 185)
            self.assertEqual(r_mm["actions"][0]["status"], "aborted_mismatch")
            self.assertEqual(len(pushes), 2)
            self.assertIn("串台拦截保护", pushes[1][1])
            txs = svc.relays.recent_transactions(relay_id=rid, limit=10)
            self.assertTrue(any(tx["status"] == "aborted_mismatch" for tx in txs))

    def test_watcher_cli_once_runs_relay_tick_safely(self) -> None:
        from agent_hub.watcher import main as watcher_main

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with (
                patch.dict(
                    os.environ,
                    {
                        "AGENT_HUB_WATCHES_PATH": str(td_path / "watches.json"),
                        "AGENT_HUB_RELAY_DB_PATH": str(td_path / "relay_states.sqlite"),
                    },
                ),
                patch("builtins.print") as mock_print,
            ):
                watcher_main(["--once", "--dry-run"])
                self.assertEqual(mock_print.call_count, 1)
                payload = json.loads(mock_print.call_args[0][0])
                self.assertIn("checked_count", payload)
                self.assertIn("relay", payload)
                self.assertEqual(payload["relay"]["checked_links"], 0)


if __name__ == "__main__":
    unittest.main()
