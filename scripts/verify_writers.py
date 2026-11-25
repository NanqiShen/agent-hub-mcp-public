#!/usr/bin/env python3
"""Safe verification helper to inspect write-bridge readiness and smoke-test individual writers."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agent_hub.audit import AuditLog
from agent_hub.bindings import BindingStore
from agent_hub.models import VALID_APPS
from agent_hub.server import HubService
from agent_hub.watcher import WatchStore


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect and smoke-test Agent Hub desktop write bridges.")
    ap.add_argument("--status", action="store_true", help="Print live status of all 4 writers.")
    ap.add_argument(
        "--send",
        choices=VALID_APPS,
        help="Run a safe throwaway smoke-send test for the given app (never targets real work threads).",
    )
    ap.add_argument(
        "--session-id",
        default="",
        help="Optional explicit throwaway session_id for codex/antigravity CDP tests.",
    )
    args = ap.parse_args()

    if not args.status and not args.send:
        args.status = True

    with tempfile.TemporaryDirectory() as td:
        svc = HubService(
            bindings=BindingStore(Path(td) / "bindings.json"),
            audit=AuditLog(Path(td) / "audit.jsonl"),
            watches=WatchStore(Path(td) / "watches.json"),
        )

        if args.status:
            print(json.dumps(svc.writer_status(), ensure_ascii=False, indent=2))
            if not args.send:
                return 0

        app = args.send
        ws = svc.writer_status()["writers"].get(app, {})
        if not ws.get("ready"):
            print(json.dumps({"ok": False, "app": app, "error": "writer not ready", "writer_status": ws}, ensure_ascii=False, indent=2))
            return 1

        marker = f"AGENT_HUB_{app.upper()}_WRITE_OK"
        prompt = f"请只回复 {marker}，不要调用任何工具。"

        if app in ("dsh", "cline"):
            res = svc.create_session(
                app,
                title=f"[AgentHub Smoke Test - {app}]",
                initial_message=prompt,
                dry_run=False,
            )
            print(json.dumps(res, ensure_ascii=False, indent=2))
            return 0 if res.get("ok") else 1

        # For codex / antigravity CDP: require explicit throwaway session_id or safely pick an [AgentHub Smoke Test] thread
        sid = args.session_id.strip()
        if not sid:
            hits = svc.search_sessions("AgentHub Smoke Test", app=app, limit=5).get("sessions") or []
            if not hits:
                print(
                    json.dumps(
                        {
                            "ok": False,
                            "app": app,
                            "error": (
                                f"Refusing to guess a live {app} work thread. Create a throwaway thread titled "
                                f"'AgentHub Smoke Test' in {app} or pass --session-id <throwaway_id>."
                            ),
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 2
            sid = hits[0]["session_id"]

        res = svc.send_message(app, sid, prompt, dry_run=False)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if res.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
