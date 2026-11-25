"""Opt-in Electron CDP write adapters for Antigravity App and Codex (ChatGPT Desktop)."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Any, Iterator
import urllib.error
from urllib.parse import urlparse
import urllib.request

from ._ws import SimpleWebSocket


DEFAULT_LOCK_DIR = Path.home() / ".hermes" / "agent-hub"


class SessionMismatchError(RuntimeError):
    """Raised when the active desktop UI session does not match the requested session_id."""

    def __init__(
        self,
        *,
        app_name: str,
        expected_session_id: str,
        actual_session_id: str = "",
        expected_title: str = "",
        actual_url: str = "",
        actual_title: str = "",
        reason: str = "",
    ) -> None:
        self.app_name = app_name
        self.expected_session_id = expected_session_id
        self.actual_session_id = actual_session_id
        self.expected_title = expected_title
        self.actual_url = actual_url
        self.actual_title = actual_title
        self.reason = reason
        msg = (
            f"{app_name} session mismatch: expected '{expected_session_id}'"
            + (f" ({expected_title})" if expected_title else "")
            + f", got '{actual_session_id or 'none'}' "
            + f"(url={actual_url or '-'}, title={actual_title or '-'})"
        )
        if reason:
            msg += f"; {reason}"
        msg += " — aborted before text injection to prevent cross-talk or unintended new session creation."
        super().__init__(msg)


_EFFORT_ALIASES: dict[str, list[str]] = {
    "none": ["none", "off", "无", "关闭"],
    "minimal": ["minimal", "min", "极低"],
    "low": ["low", "低", "fast", "快"],
    "medium": ["medium", "med", "mid", "中", "standard", "标准"],
    "high": ["high", "高"],
    "xhigh": ["extra high", "extra-high", "xhigh", "x-high", "超高", "极高"],
    "max": ["max", "maximum", "最高", "最大"],
    "ultra": ["ultra", "极限"],
}

_SESSION_URL_RE = re.compile(r"/(?:c|t)/([A-Za-z0-9_-]{6,})")
_ANTIGRAVITY_PATH_RE = re.compile(r"^/c/([A-Za-z0-9_-]{6,})/?$")
_CODEX_PATH_RE = re.compile(r"^(?:/g/[^/]+)?/(?:c|t|local)/([A-Za-z0-9_-]{6,})/?$")
_UUID_LIKE_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_GENERIC_TITLES = frozenset(
    {"", "chatgpt", "codex", "antigravity", "new chat", "new conversation", "新聊天", "新对话", "home", "index"}
)
_GENERIC_TITLE_TOKENS = frozenset({"session", "thread", "chat", "task", "conversation", "对话", "会话", "任务"})

_JS_DOM_PRELUDE = """
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const visible = (el) => {
  if (!el) return false;
  if (el.closest('[hidden], [aria-hidden="true"], [inert]')) return false;
  if (typeof el.checkVisibility === 'function' && !el.checkVisibility()) return false;
  const r = el.getBoundingClientRect();
  const st = window.getComputedStyle(el);
  return r.width > 0 && r.height > 0 && st.visibility !== 'hidden' && st.display !== 'none';
};
const findComposer = () => {
  const selectors = [
    '[aria-label="Message input"]',
    'div.ProseMirror[aria-label*="询问"]',
    'div.ProseMirror[aria-label*="Ask" i]',
    'textarea[placeholder*="Ask"]',
    'textarea[placeholder*="Message"]',
    'textarea[placeholder*="message"]',
    'textarea',
    '[role="textbox"][contenteditable="true"]',
    '[contenteditable="true"]'
  ];
  for (const sel of selectors) {
    const nodes = [...document.querySelectorAll(sel)].filter(visible);
    if (nodes.length > 0) return nodes[nodes.length - 1];
  }
  return null;
};
const readDraft = (el) => {
  if (!el) return '';
  return (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT')
    ? (el.value || '')
    : (el.innerText || el.textContent || '');
};
"""


def _discover_pid_loopback_ports(process_regex: str) -> list[int]:
    """Return TCP LISTEN ports owned by processes matching process_regex (macOS lsof)."""
    try:
        ps_out = subprocess.run(
            ["ps", "-ax", "-o", "pid=,command="],
            text=True,
            capture_output=True,
            timeout=2.5,
        ).stdout
    except Exception:  # noqa: BLE001
        return []

    pattern = re.compile(process_regex)
    pids = [
        line.strip().split(None, 1)[0]
        for line in ps_out.splitlines()
        if pattern.search(line) and "Helper" not in line
    ]
    if not pids:
        return []

    ports: list[int] = []
    try:
        lsof_out = subprocess.run(
            ["lsof", "-nP", "-a", "-iTCP", "-sTCP:LISTEN", "-p", ",".join(pids[:4])],
            text=True,
            capture_output=True,
            timeout=2.5,
        ).stdout
    except Exception:  # noqa: BLE001
        return []

    for line in lsof_out.splitlines():
        m = re.search(r"(?:127\.0\.0\.1|localhost|\*):(\d+)\s+\(LISTEN\)", line)
        if m:
            p = int(m.group(1))
            if p not in ports:
                ports.append(p)
    return ports


class CdpWriter:
    """Thread-safe Electron CDP writer with draft protection and optional session switching."""

    def __init__(
        self,
        *,
        app_name: str,
        env_port_var: str,
        default_port: int,
        process_regex: str,
        launcher_hint: str,
        new_chat_selectors: list[str] | None = None,
        new_chat_texts: list[str] | None = None,
        model_trigger_selectors: list[str] | None = None,
        reasoning_trigger_selectors: list[str] | None = None,
        new_chat_shortcut: dict[str, Any] | None = None,
        switch_verify_timeout_s: float = 5.0,
        switch_poll_interval_s: float = 0.15,
        create_url_poll_timeout_s: float = 3.0,
        lock_dir: Path | None = None,
        lock_timeout_s: float = 30.0,
    ) -> None:
        self.app_name = app_name
        self.env_port_var = env_port_var
        self.default_port = default_port
        self.process_regex = process_regex
        self.launcher_hint = launcher_hint
        self.new_chat_selectors = list(new_chat_selectors or [])
        self.new_chat_texts = list(new_chat_texts or [])
        self.model_trigger_selectors = list(model_trigger_selectors or [])
        self.reasoning_trigger_selectors = list(reasoning_trigger_selectors or [])
        self.new_chat_shortcut = dict(new_chat_shortcut or {"key": "n", "code": "KeyN", "modifiers": 4})
        self.switch_verify_timeout_s = max(0.05, float(switch_verify_timeout_s))
        self.switch_poll_interval_s = max(0.01, float(switch_poll_interval_s))
        self.create_url_poll_timeout_s = max(0.05, float(create_url_poll_timeout_s))
        if lock_dir is not None:
            self._lock_dir = lock_dir
        else:
            env_lock_dir = os.environ.get("AGENT_HUB_LOCK_DIR", "").strip()
            self._lock_dir = Path(env_lock_dir).expanduser() if env_lock_dir else DEFAULT_LOCK_DIR
        self.lock_timeout_s = max(0.05, float(lock_timeout_s))
        self._lock = threading.Lock()
        self._msg_id = 0

    @contextmanager
    def _acquire_app_mutex(self) -> Iterator[None]:
        """Acquire both an in-process thread lock and a cross-process OS flock for this Electron app."""
        deadline = time.monotonic() + self.lock_timeout_s
        if not self._lock.acquire(timeout=self.lock_timeout_s):
            raise TimeoutError(f"{self.app_name} app mutex timed out after {self.lock_timeout_s:.1f}s (thread lock)")
        fd: int | None = None
        try:
            self._lock_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(self._lock_dir, 0o700)
            except OSError:
                pass
            lock_path = self._lock_dir / f".cdp-mutex-{self.app_name}.lock"
            fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                os.chmod(lock_path, 0o600)
            except OSError:
                pass
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"{self.app_name} app mutex timed out after {self.lock_timeout_s:.1f}s (cross-process flock)"
                        ) from exc
                    time.sleep(0.05)
            yield
        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
                try:
                    os.close(fd)
                except OSError:
                    pass
            self._lock.release()

    def _candidate_ports(self) -> list[int]:
        ports: list[int] = []
        env_raw = os.environ.get(self.env_port_var, "").strip()
        if env_raw.isdigit():
            ports.append(int(env_raw))
        if self.default_port not in ports:
            ports.append(self.default_port)
        for p in _discover_pid_loopback_ports(self.process_regex):
            if p not in ports:
                ports.append(p)
        return ports

    @staticmethod
    def _fetch_targets(port: int) -> list[dict[str, Any]] | None:
        url = f"http://127.0.0.1:{port}/json/list"
        try:
            with urllib.request.urlopen(url, timeout=1.2) as resp:
                if resp.status != 200:
                    return None
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
                if isinstance(data, list):
                    return data
        except Exception:  # noqa: BLE001
            return None
        return None

    @staticmethod
    def _is_usable_page(target: dict[str, Any]) -> bool:
        if target.get("type") != "page" or not target.get("webSocketDebuggerUrl"):
            return False
        url = str(target.get("url") or "").strip().lower()
        if url in ("", "about:blank") or url.startswith("data:") or "overlay" in url:
            return False
        title_low = str(target.get("title") or "").strip().lower()
        if title_low.startswith("data:"):
            return False
        return True

    def _discover_endpoint(self) -> tuple[int | None, list[dict[str, Any]]]:
        for port in self._candidate_ports():
            targets = self._fetch_targets(port)
            if targets is None:
                continue
            pages = [t for t in targets if self._is_usable_page(t)]
            if pages:
                pages.sort(
                    key=lambda t: (
                        0 if t.get("title") in ("Antigravity", "ChatGPT") or "chatgpt.com" in t.get("url", "") else 1,
                    )
                )
                return port, pages
        return None, []

    def _probe_dom_readiness(self, ws_url: str) -> dict[str, Any]:
        """Run a fast, strictly read-only DOM probe for composer visibility and unsaved drafts."""
        script = """
        (() => {
          const visible = (el) => {
            if (!el) return false;
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            return r.width > 0 && r.height > 0 && st.visibility !== 'hidden' && st.display !== 'none';
          };
          const selectors = [
            '[aria-label="Message input"]',
            'div.ProseMirror[aria-label*="询问"]',
            'div.ProseMirror[aria-label*="Ask" i]',
            'textarea[placeholder*="Ask"]',
            'textarea[placeholder*="Message"]',
            'textarea[placeholder*="message"]',
            'textarea',
            '[role="textbox"][contenteditable="true"]',
            '[contenteditable="true"]'
          ];
          let composer = null;
          for (const sel of selectors) {
            const nodes = [...document.querySelectorAll(sel)].filter(visible);
            if (nodes.length > 0) { composer = nodes[nodes.length - 1]; break; }
          }
          const raw = composer
            ? ((composer.tagName === 'TEXTAREA' || composer.tagName === 'INPUT')
                ? (composer.value || '')
                : (composer.innerText || composer.textContent || ''))
            : '';
          return {
            composer_found: Boolean(composer),
            has_unsaved_draft: raw.trim().length > 0
          };
        })()
        """
        with SimpleWebSocket(ws_url, timeout=2.0) as ws:
            res = self._cdp_eval(ws, script, await_promise=False)
        return res if isinstance(res, dict) else {}

    def status(self) -> dict[str, Any]:
        """Check whether an Electron CDP page target is currently reachable and probe composer DOM."""
        port, pages = self._discover_endpoint()
        if port is not None:
            info: dict[str, Any] = {
                "app": self.app_name,
                "ready": True,
                "transport": "electron-cdp",
                "endpoint": f"http://127.0.0.1:{port}",
                "page_count": len(pages),
                "page_titles": [str(p.get("title") or "") for p in pages[:6]],
            }
            try:
                probe = self._probe_dom_readiness(str(pages[0]["webSocketDebuggerUrl"]))
                info["composer_found"] = bool(probe.get("composer_found"))
                info["has_unsaved_draft"] = bool(probe.get("has_unsaved_draft"))
            except Exception as exc:  # noqa: BLE001
                info["dom_probe_error"] = str(exc)
            return info
        return {
            "app": self.app_name,
            "ready": False,
            "transport": "electron-cdp",
            "detail": (
                f"No active CDP page target found for {self.app_name}. "
                f"When ready, relaunch via: {self.launcher_hint}"
            ),
        }

    def _cdp_call(self, ws: SimpleWebSocket, method: str, params: dict[str, Any] | None = None) -> Any:
        self._msg_id += 1
        call_id = self._msg_id
        ws.send_text(
            json.dumps(
                {
                    "id": call_id,
                    "method": method,
                    "params": params or {},
                }
            )
        )
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            raw = ws.recv_text()
            msg = json.loads(raw)
            if msg.get("id") != call_id:
                continue
            if "error" in msg:
                raise RuntimeError(f"CDP error: {msg['error']}")
            return msg.get("result")
        raise TimeoutError(f"CDP {method} timed out")

    def _cdp_eval(self, ws: SimpleWebSocket, expression: str, *, await_promise: bool = True) -> Any:
        self._msg_id += 1
        call_id = self._msg_id
        ws.send_text(
            json.dumps(
                {
                    "id": call_id,
                    "method": "Runtime.evaluate",
                    "params": {
                        "expression": expression,
                        "awaitPromise": await_promise,
                        "returnByValue": True,
                    },
                }
            )
        )
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            raw = ws.recv_text()
            msg = json.loads(raw)
            if msg.get("id") != call_id:
                continue
            if "error" in msg:
                raise RuntimeError(f"CDP error: {msg['error']}")
            result = (msg.get("result") or {}).get("result") or {}
            if (msg.get("result") or {}).get("exceptionDetails"):
                raise RuntimeError(f"CDP exception: {(msg['result']['exceptionDetails']).get('text')}")
            return result.get("value")
        raise TimeoutError("CDP Runtime.evaluate timed out")

    @staticmethod
    def _normalize_effort_aliases(reasoning_effort: str) -> list[str]:
        raw = reasoning_effort.strip().lower()
        if not raw:
            return []
        compact = re.sub(r"[\s_-]+", "", raw)
        if compact in ("extrahigh", "xhigh"):
            return _EFFORT_ALIASES["xhigh"]
        if raw in _EFFORT_ALIASES:
            return _EFFORT_ALIASES[raw]
        if compact in _EFFORT_ALIASES:
            return _EFFORT_ALIASES[compact]
        return [raw]

    def _apply_model_config_via_cdp(
        self,
        ws: SimpleWebSocket,
        *,
        model: str = "",
        provider: str = "",
        reasoning_effort: str = "",
    ) -> dict[str, Any] | None:
        model_clean = model.strip()
        provider_clean = provider.strip()
        effort_clean = reasoning_effort.strip()
        if not model_clean and not provider_clean and not effort_clean:
            return None

        model_tokens = [t for t in re.split(r"[-_\s/]+", model_clean.lower()) if t]
        provider_tokens = [t for t in re.split(r"[-_\s/]+", provider_clean.lower()) if t]
        effort_aliases = self._normalize_effort_aliases(effort_clean)
        cfg = json.dumps(
            {
                "modelRaw": model_clean.lower(),
                "modelTokens": model_tokens,
                "providerTokens": provider_tokens,
                "effortRaw": effort_clean.lower(),
                "effortAliases": effort_aliases,
                "modelSelectors": self.model_trigger_selectors,
                "reasoningSelectors": self.reasoning_trigger_selectors,
            }
        )
        script = f"""
        (async () => {{
          const cfg = {cfg};
          {_JS_DOM_PRELUDE}
          const esc = () => document.dispatchEvent(new KeyboardEvent('keydown', {{
            key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true
          }}));
          const elText = (el) => ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || '') + ' ' + (el.getAttribute('title') || '')).trim().toLowerCase();
          const hasEffort = (txt, aliases) => {{
            if (!aliases || aliases.length === 0) return false;
            const wantsXHigh = aliases.some(a => a === 'extra high' || a === 'extra-high' || a === 'xhigh' || a === '超高' || a === '极高');
            const clean = wantsXHigh ? txt : txt.replace(/extra[\\s_-]*high|x[\\s_-]*high|超高|极高/gi, ' ');
            for (const alias of aliases) {{
              if (/^[a-z0-9 .-]+$/.test(alias)) {{
                const re = new RegExp('(?:^|[^a-z0-9])' + alias.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&') + '(?:[^a-z0-9]|$)', 'i');
                if (re.test(clean)) return true;
              }} else {{
                if (clean.includes(alias)) return true;
              }}
            }}
            return false;
          }};
          const findTrigger = (selectors, fallbackRe) => {{
            for (const sel of selectors) {{
              const hit = [...document.querySelectorAll(sel)].filter(visible)[0];
              if (hit) return hit;
            }}
            return [...document.querySelectorAll('button, [role="button"], [role="combobox"], select')]
              .filter(visible)
              .find((el) => fallbackRe.test(elText(el))) || null;
          }};
          const collectMenuItems = () => {{
            const sels = [
              '[role="menuitem"]',
              '[role="menuitemradio"]',
              '[role="option"]',
              '[role="radio"]',
              '[data-radix-collection-item]',
              '[role="menu"] button',
              '[role="listbox"] button',
              '[role="dialog"] button',
              '[data-radix-popper-content-wrapper] button'
            ];
            const seen = new Set();
            const out = [];
            for (const sel of sels) {{
              for (const el of document.querySelectorAll(sel)) {{
                if (!seen.has(el) && visible(el)) {{
                  seen.add(el);
                  out.push(el);
                }}
              }}
            }}
            return out;
          }};

          const modelBtn = findTrigger(cfg.modelSelectors, /(select model|选择.*模型|model)/i);
          const reasoningBtn = findTrigger(cfg.reasoningSelectors, /(思考强度|reasoning|effort|thinking)/i);
          const currentSummary = ((modelBtn ? elText(modelBtn) : '') + ' ' + (reasoningBtn ? elText(reasoningBtn) : '')).trim();

          const modelAlready = cfg.modelTokens.length === 0 || cfg.modelTokens.every(t => currentSummary.includes(t));
          const effortAlready = cfg.effortAliases.length === 0 || hasEffort(currentSummary, cfg.effortAliases);
          if (modelAlready && effortAlready && currentSummary) {{
            return {{ applied: true, already_selected: true, current: currentSummary.slice(0, 120) }};
          }}

          let modelClicked = '';
          let effortClicked = '';

          if ((!modelAlready || (!effortAlready && !reasoningBtn)) && modelBtn) {{
            modelBtn.click();
            await sleep(250);
            const items = collectMenuItems();
            let bestItem = null;
            let bestScore = 0;
            for (const item of items) {{
              const txt = elText(item);
              if (!txt) continue;
              let score = 0;
              if (cfg.modelTokens.length > 0) {{
                let hits = 0;
                for (const t of cfg.modelTokens) {{
                  if (txt.includes(t)) hits += 1;
                }}
                if (hits === 0) continue;
                score += hits * 10;
                if (hits === cfg.modelTokens.length) score += 20;
                if (cfg.modelRaw && txt.includes(cfg.modelRaw)) score += 15;
              }}
              if (cfg.providerTokens.length > 0 && cfg.providerTokens.every(t => txt.includes(t))) {{
                score += 5;
              }}
              if (cfg.effortAliases.length > 0 && hasEffort(txt, cfg.effortAliases)) {{
                score += 12;
              }}
              if (score > bestScore) {{
                bestScore = score;
                bestItem = item;
              }}
            }}
            if (bestItem && bestScore > 0) {{
              modelClicked = (bestItem.innerText || bestItem.getAttribute('aria-label') || '').trim().slice(0, 120);
              if (cfg.effortAliases.length > 0 && hasEffort(elText(bestItem), cfg.effortAliases)) {{
                effortClicked = modelClicked;
              }}
              bestItem.click();
              await sleep(200);
            }} else {{
              esc();
              await sleep(100);
            }}
          }}

          if (cfg.effortAliases.length > 0 && !effortClicked) {{
            const targetReasoningBtn = reasoningBtn || modelBtn;
            if (targetReasoningBtn) {{
              const afterText = elText(targetReasoningBtn);
              if (hasEffort(afterText, cfg.effortAliases)) {{
                effortClicked = afterText.slice(0, 120);
              }} else {{
                targetReasoningBtn.click();
                await sleep(250);
                const items = collectMenuItems();
                let bestEff = null;
                let bestEffScore = 0;
                for (const item of items) {{
                  const txt = elText(item);
                  if (!txt || !hasEffort(txt, cfg.effortAliases)) continue;
                  let score = 10;
                  if (cfg.modelTokens.length > 0 && cfg.modelTokens.every(t => txt.includes(t))) score += 10;
                  if (score > bestEffScore) {{
                    bestEffScore = score;
                    bestEff = item;
                  }}
                }}
                if (bestEff) {{
                  effortClicked = (bestEff.innerText || bestEff.getAttribute('aria-label') || '').trim().slice(0, 120);
                  bestEff.click();
                  await sleep(200);
                }} else {{
                  esc();
                  await sleep(100);
                }}
              }}
            }}
          }}

          return {{
            applied: Boolean(modelClicked || effortClicked),
            already_selected: false,
            ...(modelClicked ? {{ model_clicked: modelClicked }} : {{}}),
            ...(effortClicked ? {{ effort_clicked: effortClicked }} : {{}}),
            had_model_trigger: Boolean(modelBtn),
            had_reasoning_trigger: Boolean(reasoningBtn)
          }};
        }})()
        """
        try:
            res = self._cdp_eval(ws, script, await_promise=True)
            if isinstance(res, dict):
                return res
            return {"applied": False, "warning": f"unexpected model selection result: {res}"}
        except Exception as exc:  # noqa: BLE001
            return {"applied": False, "warning": str(exc)}

    def _cdp_inject_and_send(self, ws: SimpleWebSocket, text: str) -> dict[str, Any]:
        payload = json.dumps({"text": text})
        prep_script = f"""
        (() => {{
          const cfg = {payload};
          {_JS_DOM_PRELUDE}
          const composer = findComposer();
          if (!composer) {{
            return {{ ok: false, error: 'composer-not-found', message: 'Could not locate visible chat input element via CDP.' }};
          }}
          if (readDraft(composer).trim().length > 0) {{
            return {{ ok: false, error: 'draft-present', message: 'Target session composer has unsaved draft; aborted.' }};
          }}
          composer.focus();
          const isContentEditable = composer.tagName !== 'TEXTAREA' && composer.tagName !== 'INPUT';
          if (!isContentEditable) {{
            const proto = composer.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
            const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
            if (setter) setter.call(composer, cfg.text);
            else composer.value = cfg.text;
            composer.dispatchEvent(new Event('input', {{ bubbles: true }}));
          }}
          window.__hermes_active_composer = composer;
          return {{ ok: true, isContentEditable }};
        }})()
        """
        prep = self._cdp_eval(ws, prep_script, await_promise=False)
        if not isinstance(prep, dict) or not prep.get("ok"):
            raise RuntimeError(f"{self.app_name} CDP send prep failed: {prep}")

        if prep.get("isContentEditable"):
            self._cdp_call(ws, "Input.insertText", {"text": text})

        time.sleep(0.15)
        click_script = f"""
        (() => {{
          {_JS_DOM_PRELUDE}
          const composer = window.__hermes_active_composer || document.activeElement;
          const sendBtn = [...document.querySelectorAll('button[aria-label*="Send" i], button[aria-label*="发送"], button[data-testid*="send" i], button.agy-send-button, button[type="submit"]')]
            .filter((b) => visible(b) && !b.disabled)[0];
          if (sendBtn) {{
            sendBtn.click();
          }} else if (composer) {{
            composer.dispatchEvent(new KeyboardEvent('keydown', {{ key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }}));
          }}
          return {{ ok: true, method: sendBtn ? 'button' : 'enter' }};
        }})()
        """
        res = self._cdp_eval(ws, click_script, await_promise=True)
        if not isinstance(res, dict) or not res.get("ok"):
            raise RuntimeError(f"{self.app_name} CDP send failed: {res}")

        time.sleep(0.25)
        probe_lines = [
            ln.strip()
            for ln in text.splitlines()
            if ln.strip() and not ln.strip().startswith(("[via ", "[Agent Hub Relay"))
        ]
        probe_text = (probe_lines[0][:28] if probe_lines else text.strip()[:28])
        verify_cfg = json.dumps({"probe": probe_text})
        verify_script = f"""
        (() => {{
          // __agentHubVerifyPostSend
          const cfg = {verify_cfg};
          {_JS_DOM_PRELUDE}
          const comp = findComposer();
          const draft = comp ? readDraft(comp).trim() : '';
          const stopBtn = [...document.querySelectorAll('button[aria-label*="Stop" i], button[aria-label*="停止"], button[data-testid*="stop" i]')]
            .filter(visible)[0];
          let messageAppeared = false;
          const needle = String(cfg.probe || '').trim().toLowerCase();
          if (needle) {{
            const turnEls = [...document.querySelectorAll(
              '[data-testid="user-message-content"], [data-testid="user-message"], [class*="user-message"], [data-message-author-role="user"]'
            )].filter(visible);
            messageAppeared = turnEls.some((el) => (el.innerText || el.textContent || '').toLowerCase().includes(needle));
            if (!messageAppeared) {{
              const mainEl = document.querySelector('main');
              if (mainEl) {{
                messageAppeared = (mainEl.innerText || '').toLowerCase().includes(needle);
              }}
            }}
          }}
          return {{ composer_cleared: draft.length === 0, is_generating: Boolean(stopBtn), message_appeared: messageAppeared }};
        }})()
        """
        try:
            verify = self._cdp_eval(ws, verify_script, await_promise=False)
            if isinstance(verify, dict) and "composer_cleared" in verify:
                res["composer_cleared"] = bool(verify.get("composer_cleared"))
                res["is_generating"] = bool(verify.get("is_generating"))
                if "message_appeared" in verify:
                    res["message_appeared"] = bool(verify.get("message_appeared"))
        except Exception:  # noqa: BLE001
            pass
        return res

    def create_session(
        self,
        *,
        cwd: str = "",
        title: str = "",
        initial_message: str = "",
        model: str = "",
        provider: str = "",
        reasoning_effort: str = "",
    ) -> dict[str, Any]:
        """Trigger a new conversation/thread via Electron CDP, optionally configure model and send initial_message."""
        with self._acquire_app_mutex():
            port, pages = self._discover_endpoint()
            if port is None or not pages:
                raise ConnectionError(
                    f"{self.app_name} CDP is not active. Relaunch with {self.launcher_hint} to enable GUI session creation."
                )

            ws_url = str(pages[0]["webSocketDebuggerUrl"])
            ws_name = Path(cwd.strip()).name.lower() if cwd.strip() else ""
            cfg = json.dumps(
                {
                    "workspaceName": ws_name,
                    "newChatSelectors": self.new_chat_selectors,
                    "newChatTexts": self.new_chat_texts,
                }
            )
            new_chat_script = f"""
            (async () => {{
              const cfg = {cfg};
              {_JS_DOM_PRELUDE}
              const preComposer = findComposer();
              if (preComposer && readDraft(preComposer).trim().length > 0) {{
                return {{ ok: false, error: 'draft-present', message: 'Composer has unsaved user draft; aborted to avoid overwriting.' }};
              }}

              const findNewChatBtn = () => {{
                const allBtns = [...document.querySelectorAll('a, button, [role="button"]')].filter(visible);
                if (cfg.workspaceName) {{
                  const wsHit = allBtns.find((el) => {{
                    const hay = ((el.getAttribute('aria-label') || '') + ' ' + (el.getAttribute('title') || '')).toLowerCase();
                    return hay.includes(cfg.workspaceName) && (hay.includes('新建') || hay.includes('new'));
                  }});
                  if (wsHit) return {{ el: wsHit, method: 'workspace-button' }};
                }}
                for (const sel of cfg.newChatSelectors) {{
                  const hit = [...document.querySelectorAll(sel)].filter(visible)[0];
                  if (hit) return {{ el: hit, method: 'selector' }};
                }}
                for (const el of allBtns) {{
                  const txt = (el.innerText || '').trim().toLowerCase();
                  const aria = (el.getAttribute('aria-label') || '').trim().toLowerCase();
                  const hay = txt || aria;
                  if (!hay || hay.length > 80) continue;
                  if (cfg.newChatTexts.some(t => hay === t || txt === t || aria.includes(t))) {{
                    return {{ el, method: 'text' }};
                  }}
                }}
                return null;
              }};

              let found = findNewChatBtn();
              if (!found) {{
                const sidebarToggle = [...document.querySelectorAll('[data-testid="sidebar-toggle"], button[aria-label*="Sidebar" i], button[aria-label*="侧边栏"]')]
                  .filter(visible)[0];
                if (sidebarToggle) {{
                  sidebarToggle.click();
                  await sleep(250);
                  found = findNewChatBtn();
                  if (found) found.method = 'sidebar-' + found.method;
                }}
              }}

              if (found && found.el) {{
                found.el.click();
                await sleep(450);
                return {{ ok: true, clicked: true, method: found.method }};
              }}
              return {{ ok: true, clicked: false, method: 'shortcut-needed' }};
            }})()
            """

            send_res: dict[str, Any] | None = None
            model_selection: dict[str, Any] | None = None
            with SimpleWebSocket(ws_url, timeout=15.0) as ws:
                prep = self._cdp_eval(ws, new_chat_script, await_promise=True)
                if not isinstance(prep, dict) or not prep.get("ok"):
                    raise RuntimeError(f"{self.app_name} CDP create_session failed: {prep}")

                new_chat_method = str(prep.get("method") or "button")
                if not prep.get("clicked"):
                    sc = self.new_chat_shortcut
                    key = str(sc.get("key") or "n")
                    code = str(sc.get("code") or f"Key{key.upper()}")
                    mods = int(sc.get("modifiers") or 4)
                    vk = int(sc.get("windowsVirtualKeyCode") or ord(key.upper()))
                    key_params = {
                        "key": key,
                        "code": code,
                        "modifiers": mods,
                        "windowsVirtualKeyCode": vk,
                        "nativeVirtualKeyCode": vk,
                    }
                    self._cdp_call(ws, "Input.dispatchKeyEvent", {"type": "keyDown", **key_params})
                    self._cdp_call(ws, "Input.dispatchKeyEvent", {"type": "keyUp", **key_params})
                    new_chat_method = "shortcut"
                    time.sleep(0.45)

                model_selection = self._apply_model_config_via_cdp(
                    ws,
                    model=model,
                    provider=provider,
                    reasoning_effort=reasoning_effort,
                )

                session_id = ""
                if initial_message.strip():
                    send_res = self._cdp_inject_and_send(ws, initial_message)
                    deadline = time.monotonic() + self.create_url_poll_timeout_s
                    while True:
                        href = self._cdp_eval(ws, "window.location.href", await_promise=False)
                        m = _SESSION_URL_RE.search(str(href or ""))
                        if m:
                            session_id = m.group(1)
                            break
                        if time.monotonic() >= deadline:
                            break
                        time.sleep(self.switch_poll_interval_s)
                else:
                    href = self._cdp_eval(ws, "window.location.href", await_promise=False)
                    m = _SESSION_URL_RE.search(str(href or ""))
                    session_id = m.group(1) if m else ""

                if not session_id:
                    session_id = f"{self.app_name}-cdp-{int(time.time() * 1000)}"

            out: dict[str, Any] = {
                "ok": True,
                "sessionId": session_id,
                "transport": "electron-cdp",
                "port": port,
                "cwd": cwd,
                "title": title,
                "new_chat_method": new_chat_method,
                "sent_initial_message": bool(initial_message.strip()),
            }
            if send_res is not None:
                out["send_method"] = send_res.get("method", "")
            if model_selection is not None:
                out["modelSelection"] = model_selection
            return out

    @staticmethod
    def _normalize_session_id(raw: Any) -> str:
        s = str(raw or "").strip()
        for prefix in ("chatgpt:conversation:", "chatgpt:", "local:"):
            if s.startswith(prefix):
                s = s[len(prefix) :].strip()
        return s

    @staticmethod
    def _normalize_title_text(raw: str) -> str:
        s = str(raw or "").strip().lower()
        s = re.sub(r"\s*-\s*(?:antigravity|chatgpt|codex)$", "", s, flags=re.IGNORECASE)
        s = s.strip("….").strip()
        return " ".join(s.split())

    @classmethod
    def _titles_contradict(cls, expected_title: str, actual_title: str) -> bool:
        """Return True only when both titles are meaningful and clearly refer to different sessions."""
        exp = cls._normalize_title_text(expected_title)
        act = cls._normalize_title_text(actual_title)
        if exp in _GENERIC_TITLES or act in _GENERIC_TITLES:
            return False
        if _UUID_LIKE_RE.match(exp) or _UUID_LIKE_RE.match(act):
            return False
        if exp == act or exp in act or act in exp:
            return False

        def _tokens(val: str) -> set[str]:
            toks = set(re.findall(r"[a-z0-9]{3,}|[\u4e00-\u9fff]{2,}", val))
            return {t for t in toks if t not in _GENERIC_TITLE_TOKENS}

        exp_toks = _tokens(exp)
        act_toks = _tokens(act)
        if not exp_toks or not act_toks:
            return False
        return len(exp_toks & act_toks) == 0

    def _read_active_session_identity(self, ws: SimpleWebSocket) -> dict[str, Any]:
        """Inspect the currently active UI session identity via read-only CDP & React Fiber evaluation."""
        if self.app_name == "antigravity":
            script = """
            (() => {
              // __agentHubReadSessionIdentity
              const href = String(window.location.href || '');
              let pathname = String(window.location.pathname || '');
              try {
                if (!pathname && href) pathname = new URL(href).pathname || '';
              } catch {}
              const m = pathname.match(/^\\/c\\/([A-Za-z0-9_-]{6,})\\/?$/);
              const sessionId = m ? m[1] : '';
              const docTitle = String(document.title || '').replace(/\\s*-\\s*Antigravity$/i, '').trim();
              let title = '';
              if (sessionId) {
                const row = [...document.querySelectorAll('[data-testid="conversation-row-sidebar"][data-cascade-id]')]
                  .find((el) => el.getAttribute('data-cascade-id') === sessionId)
                  || document.querySelector('[data-testid="conversation-row-sidebar"][data-selected="true"]');
                if (row) {
                  title = (row.innerText || row.textContent || '').trim().split('\\n')[0].trim();
                }
              }
              if (!title) title = docTitle;
              return { sessionId, title, docTitle, href, pathname, source: sessionId ? 'pathname' : 'none', consistent: true, conflict: '' };
            })()
            """
        else:
            script = """
            (() => {
              // __agentHubReadSessionIdentity
              const href = String(window.location.href || '');
              let pathname = String(window.location.pathname || '');
              try {
                if (!pathname && href) pathname = new URL(href).pathname || '';
              } catch {}
              const norm = (v) => String(v || '').trim()
                .replace(/^chatgpt:conversation:/, '')
                .replace(/^chatgpt:/, '')
                .replace(/^local:/, '')
                .trim();
              const isHidden = (el) => {
                if (!el) return true;
                if (el.closest('[hidden], [aria-hidden="true"], [inert]')) return true;
                if (typeof el.checkVisibility === 'function' && !el.checkVisibility()) return true;
                const st = window.getComputedStyle(el);
                return st.display === 'none' || st.visibility === 'hidden';
              };
              const pathRe = /^(?:\\/g\\/[^/]+)?\\/(?:c|t|local)\\/([A-Za-z0-9_-]{6,})\\/?$/;
              const docTitle = String(document.title || '').trim();

              let routerPathname = '';
              let routerSid = '';
              const root = document.getElementById('root') || document.getElementById('__next') || document.body?.firstElementChild;
              const fk = root ? Object.keys(root).find((k) => k.startsWith('__reactContainer$') || k.startsWith('__reactFiber$')) : null;
              let rootFiber = fk ? root[fk] : null;
              if (rootFiber && rootFiber.current) rootFiber = rootFiber.current;
              if (rootFiber) {
                const visited = new Set();
                const q = [rootFiber];
                let steps = 0;
                while (q.length && steps < 3000) {
                  const f = q.shift();
                  if (!f || visited.has(f)) continue;
                  visited.add(f);
                  steps += 1;
                  const p = f.memoizedProps;
                  const v = p && typeof p === 'object' ? p.value : null;
                  if (v && typeof v === 'object' && v.router && typeof v.router.navigate === 'function') {
                    const rp = String(v.router.state?.location?.pathname || v.location?.pathname || '');
                    const rm = rp.match(pathRe);
                    if (rm) {
                      routerPathname = rp;
                      routerSid = norm(rm[1]);
                      break;
                    }
                  }
                  if (f.child) q.push(f.child);
                  if (f.sibling) q.push(f.sibling);
                }
              }

              const pm = pathname.match(pathRe);
              const pathnameSid = pm ? norm(pm[1]) : '';

              let sidebarSid = '';
              let sidebarTitle = '';
              let sidebarSource = '';
              const activeThread = document.querySelector(
                '[data-app-action-sidebar-thread-active="true"], [data-app-action-sidebar-thread-selected="true"]'
              );
              if (activeThread) {
                const sid = norm(activeThread.getAttribute('data-app-action-sidebar-thread-id'));
                if (sid) {
                  sidebarSid = sid;
                  sidebarSource = 'sidebar-active-thread';
                  sidebarTitle = (
                    activeThread.getAttribute('data-app-action-sidebar-thread-title')
                    || activeThread.getAttribute('aria-label')
                    || activeThread.innerText
                    || ''
                  ).trim();
                }
              }
              if (!sidebarSid) {
                const activeChatgpt = document.querySelector(
                  '[data-sidebar-archive-chatgpt-id][aria-current="page"], [data-sidebar-chatgpt-conversation-key][aria-current="page"]'
                );
                if (activeChatgpt) {
                  const sid = norm(
                    activeChatgpt.getAttribute('data-sidebar-archive-chatgpt-id')
                    || activeChatgpt.getAttribute('data-sidebar-chatgpt-conversation-key')
                  );
                  if (sid) {
                    sidebarSid = sid;
                    sidebarSource = 'sidebar-active-chatgpt';
                    sidebarTitle = (
                      activeChatgpt.getAttribute('aria-label')
                      || activeChatgpt.innerText
                      || ''
                    ).trim();
                  }
                }
              }

              let domSid = '';
              let domSource = '';
              const domQueries = [
                ['[data-above-composer-conversation-id]', 'data-above-composer-conversation-id', 'composer-above-dom'],
                ['[data-map-composer-conversation]', 'data-map-composer-conversation', 'composer-map-dom'],
                ['main [data-thread-id], [data-active="true"][data-thread-id]', 'data-thread-id', 'main-dom'],
                ['main [data-conversation-id], [data-active="true"][data-conversation-id]', 'data-conversation-id', 'main-dom'],
              ];
              for (const [sel, attr, src] of domQueries) {
                for (const el of document.querySelectorAll(sel)) {
                  if (isHidden(el)) continue;
                  const sid = norm(el.getAttribute(attr));
                  if (sid) {
                    domSid = sid;
                    domSource = src;
                    break;
                  }
                }
                if (domSid) break;
              }

              let sessionId = '';
              let source = 'none';
              if (routerSid) {
                sessionId = routerSid;
                source = 'router-fiber';
              } else if (pathnameSid) {
                sessionId = pathnameSid;
                source = 'pathname';
              } else if (sidebarSid) {
                sessionId = sidebarSid;
                source = sidebarSource;
              } else if (domSid) {
                sessionId = domSid;
                source = domSource;
              }

              let consistent = true;
              let conflict = '';
              if (routerSid && sidebarSid && routerSid !== sidebarSid) {
                consistent = false;
                conflict = 'router=' + routerSid + ' vs sidebar=' + sidebarSid;
              } else if (routerSid && pathnameSid && routerSid !== pathnameSid) {
                consistent = false;
                conflict = 'router=' + routerSid + ' vs pathname=' + pathnameSid;
              }

              return {
                sessionId,
                title: sidebarTitle || docTitle,
                docTitle,
                href,
                pathname: routerPathname || pathname,
                source,
                consistent,
                conflict,
              };
            })()
            """

        raw = self._cdp_eval(ws, script, await_promise=False)
        data = raw if isinstance(raw, dict) else {}
        url = str(data.get("href") or "")
        pathname = str(data.get("pathname") or "")
        if not pathname and url:
            try:
                pathname = urlparse(url).path or ""
            except ValueError:
                pathname = ""
        sid = self._normalize_session_id(data.get("sessionId"))
        if not sid and pathname:
            pat = _ANTIGRAVITY_PATH_RE if self.app_name == "antigravity" else _CODEX_PATH_RE
            m = pat.match(pathname)
            if m:
                sid = self._normalize_session_id(m.group(1))
        return {
            "session_id": sid,
            "title": str(data.get("title") or "").strip(),
            "doc_title": str(data.get("docTitle") or data.get("title") or "").strip(),
            "url": url,
            "pathname": pathname,
            "source": str(data.get("source") or ("pathname" if sid else "none")),
            "consistent": bool(data.get("consistent", True)),
            "conflict": str(data.get("conflict") or "").strip(),
        }

    def _matches_target_session(
        self,
        identity: dict[str, Any],
        expected_session_id: str,
        *,
        expected_title: str = "",
    ) -> bool:
        if not identity.get("consistent", True):
            return False
        expected = self._normalize_session_id(expected_session_id)
        if not expected:
            return False
        actual_sid = self._normalize_session_id(identity.get("session_id"))
        if self.app_name == "antigravity":
            pathname = str(identity.get("pathname") or "").rstrip("/")
            matched = pathname == f"/c/{expected}" and actual_sid == expected
        else:
            matched = actual_sid == expected
        if not matched:
            return False
        if expected_title:
            actual_title = str(identity.get("doc_title") or identity.get("title") or "").strip()
            if self._titles_contradict(expected_title, actual_title):
                return False
        return True

    def _select_page(self, pages: list[dict[str, Any]], expected_sid: str, target_title: str) -> dict[str, Any]:
        target = pages[0]
        matched_page = False
        for p in pages:
            pu = str(p.get("url") or "")
            if f"/c/{expected_sid}" in pu or f"/t/{expected_sid}" in pu:
                target = p
                matched_page = True
                break
        if not matched_page and target_title:
            tl = target_title.lower()
            for p in pages:
                pt = str(p.get("title", "")).strip().lower()
                if pt and (tl in pt or pt in tl):
                    target = p
                    break
        return target

    def _ensure_target_session(
        self,
        ws: SimpleWebSocket,
        expected_sid: str,
        target_title: str = "",
        *,
        raw_session_id: str = "",
    ) -> tuple[dict[str, Any], bool, str]:
        """Verify active UI session matches expected_sid or switch via sidebar/Fiber navigate, failing closed."""
        js_payload = json.dumps(
            {
                "targetTitle": target_title,
                "sessionId": expected_sid,
                "rawSessionId": raw_session_id or expected_sid,
            }
        )
        switch_script = f"""
        (async () => {{
          const cfg = {js_payload};
          {_JS_DOM_PRELUDE}
          const norm = (v) => String(v || '').trim()
            .replace(/^chatgpt:conversation:/, '')
            .replace(/^chatgpt:/, '')
            .replace(/^local:/, '')
            .trim();
          const isNewChatButton = (el) => {{
            const hay = ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || '') + ' ' + (el.getAttribute('title') || '')).toLowerCase();
            return /(?:^|\\b)(?:new chat|new thread|new conversation)(?:$|\\b)|新聊天|新线程|新对话|新建对话|新建聊天/.test(hay);
          }};
          const findBySessionId = (sid) => {{
            if (!sid) return null;
            for (const a of document.querySelectorAll('a[href]')) {{
              if (!visible(a) || isNewChatButton(a)) continue;
              const href = String(a.getAttribute('href') || '');
              if (href === '/c/' + sid || href.startsWith('/c/' + sid + '?') || href.endsWith('/c/' + sid) || href.includes('/' + sid)) {{
                return {{ el: a, method: 'id-href' }};
              }}
            }}
            const idAttrs = [
              'data-cascade-id',
              'data-app-action-sidebar-thread-id',
              'data-sidebar-archive-chatgpt-id',
              'data-sidebar-chatgpt-conversation-key',
              'data-thread-id',
              'data-conversation-id',
            ];
            for (const attr of idAttrs) {{
              for (const el of document.querySelectorAll('[' + attr + ']')) {{
                if (!visible(el) || isNewChatButton(el)) continue;
                if (norm(el.getAttribute(attr)) === sid) {{
                  return {{ el, method: 'id-' + attr }};
                }}
              }}
            }}
            return null;
          }};
          const fiberNavigate = (sid, rawSid) => {{
            if (!sid) return null;
            const root = document.getElementById('root') || document.getElementById('__next') || document.body?.firstElementChild;
            const fk = root ? Object.keys(root).find((k) => k.startsWith('__reactContainer$') || k.startsWith('__reactFiber$')) : null;
            let rootFiber = fk ? root[fk] : null;
            if (rootFiber && rootFiber.current) rootFiber = rootFiber.current;
            if (!rootFiber) return null;
            let routerNav = null;
            let navPush = null;
            let propsNav = null;
            let scopedPath = '';
            const visited = new Set();
            const q = [rootFiber];
            let steps = 0;
            while (q.length && steps < 4000) {{
              const f = q.shift();
              if (!f || visited.has(f)) continue;
              visited.add(f);
              steps += 1;
              const p = f.memoizedProps;
              if (p && typeof p === 'object') {{
                if (typeof p.navigate === 'function' && !propsNav) propsNav = p.navigate;
                const v = p.value;
                if (v && typeof v === 'object') {{
                  if (v.router && typeof v.router.navigate === 'function' && !routerNav) routerNav = v.router;
                  if (v.navigator && typeof v.navigator.push === 'function' && !navPush) navPush = v.navigator;
                  const lp = String(v.location?.pathname || v.router?.state?.location?.pathname || '');
                  if (lp && (lp.endsWith('/' + sid) || lp.includes('/c/' + sid) || lp.includes('/local/' + sid))) {{
                    scopedPath = lp;
                  }}
                }}
              }}
              if (f.child) q.push(f.child);
              if (f.sibling) q.push(f.sibling);
            }}
            const defaultPath = String(rawSid || '').startsWith('local:') ? ('/local/' + sid) : ('/c/' + sid);
            const targetPath = scopedPath || defaultPath;
            if (routerNav) {{
              routerNav.navigate(targetPath);
              return {{ method: 'fiber-router-navigate', navigatedTo: targetPath }};
            }}
            if (navPush) {{
              navPush.push(targetPath);
              return {{ method: 'fiber-navigator-push', navigatedTo: targetPath }};
            }}
            if (propsNav) {{
              propsNav(targetPath);
              return {{ method: 'fiber-props-navigate', navigatedTo: targetPath }};
            }}
            return null;
          }};

          let hit = findBySessionId(cfg.sessionId);
          if (hit && hit.el) {{
            hit.el.click();
            await sleep(250);
            return {{ ok: true, switched: true, switchMethod: hit.method }};
          }}

          const navRes = fiberNavigate(cfg.sessionId, cfg.rawSessionId);
          if (navRes) {{
            await sleep(250);
            return {{ ok: true, switched: true, switchMethod: navRes.method, navigatedTo: navRes.navigatedTo }};
          }}

          const sidebarToggle = [...document.querySelectorAll('[data-testid="sidebar-toggle"], button[aria-label*="Sidebar" i], button[aria-label*="侧边栏"]')]
            .filter(visible)[0];
          if (sidebarToggle) {{
            sidebarToggle.click();
            await sleep(250);
            hit = findBySessionId(cfg.sessionId);
            if (hit) hit.method = 'sidebar-' + hit.method;
          }}

          if (!hit && cfg.targetTitle) {{
            const needle = cfg.targetTitle.trim().toLowerCase();
            if (needle) {{
              const scoped = [...document.querySelectorAll(
                '[data-testid="conversation-row-sidebar"], a[href^="/c/"], [data-sidebar-cycle-item], [data-app-action-sidebar-thread-row], aside a, aside button, nav a, nav button'
              )].filter((el) => {{
                if (!visible(el) || isNewChatButton(el)) return false;
                const txt = (el.innerText || el.getAttribute('aria-label') || '').trim().toLowerCase();
                return txt && txt.length < 180 && (txt === needle || txt.includes(needle) || (txt.length >= 4 && needle.includes(txt)));
              }});
              if (scoped.length > 0) {{
                hit = {{ el: scoped[0], method: 'sidebar-title' }};
              }}
            }}
          }}

          if (hit && hit.el) {{
            hit.el.click();
            await sleep(250);
            return {{ ok: true, switched: true, switchMethod: hit.method }};
          }}
          return {{ ok: true, switched: false, switchMethod: 'not-found' }};
        }})()
        """

        identity = self._read_active_session_identity(ws)
        switched = False
        switch_method = "already-active"

        if not self._matches_target_session(identity, expected_sid, expected_title=target_title):
            sw_res = self._cdp_eval(ws, switch_script, await_promise=True)
            if not isinstance(sw_res, dict) or not sw_res.get("ok"):
                raise RuntimeError(f"{self.app_name} CDP switch failed: {sw_res}")
            switched = bool(sw_res.get("switched"))
            switch_method = str(sw_res.get("switchMethod") or ("clicked" if switched else "not-found"))

            if switched:
                deadline = time.monotonic() + self.switch_verify_timeout_s
                while True:
                    identity = self._read_active_session_identity(ws)
                    if self._matches_target_session(identity, expected_sid, expected_title=target_title):
                        # Double-confirm stability gate to ensure React Router did not bounce back.
                        time.sleep(min(self.switch_poll_interval_s, 0.15))
                        identity = self._read_active_session_identity(ws)
                        if self._matches_target_session(identity, expected_sid, expected_title=target_title):
                            break
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(self.switch_poll_interval_s)
            else:
                identity = self._read_active_session_identity(ws)

            if not self._matches_target_session(identity, expected_sid, expected_title=target_title):
                conflict = str(identity.get("conflict") or "").strip()
                actual_title = str(identity.get("doc_title") or identity.get("title") or "").strip()
                if conflict:
                    reason = f"inconsistent UI state ({conflict})"
                elif self._normalize_session_id(identity.get("session_id")) == expected_sid and target_title and self._titles_contradict(target_title, actual_title):
                    reason = f"title contradiction: expected '{target_title}' vs actual '{actual_title}'"
                elif switched:
                    reason = (
                        f"session switch did not stabilize on target within {self.switch_verify_timeout_s:.1f}s "
                        f"(switch_method={switch_method})"
                    )
                else:
                    reason = "could not locate target session via sidebar or Fiber router"
                raise SessionMismatchError(
                    app_name=self.app_name,
                    expected_session_id=expected_sid,
                    expected_title=target_title,
                    actual_session_id=str(identity.get("session_id") or ""),
                    actual_url=str(identity.get("url") or ""),
                    actual_title=actual_title,
                    reason=reason,
                )
        return identity, switched, switch_method

    def send_message(
        self,
        session_id: str,
        text: str,
        *,
        target_title: str = "",
        mode: str = "queue",
        model: str = "",
        provider: str = "",
        reasoning_effort: str = "",
    ) -> dict[str, Any]:
        """Inject text into the verified target Electron session via CDP with fail-fast session guard."""
        expected_sid = self._normalize_session_id(session_id)
        if not expected_sid:
            raise ValueError(f"{self.app_name} send_message requires a non-empty session_id")

        with self._acquire_app_mutex():
            port, pages = self._discover_endpoint()
            if port is None or not pages:
                raise ConnectionError(
                    f"{self.app_name} CDP is not active. Relaunch with {self.launcher_hint} to enable GUI sends."
                )

            target = self._select_page(pages, expected_sid, target_title)
            ws_url = str(target["webSocketDebuggerUrl"])

            pre_draft_script = f"""
            (() => {{
              {_JS_DOM_PRELUDE}
              const preComposer = findComposer();
              if (preComposer && readDraft(preComposer).trim().length > 0) {{
                return {{ ok: false, error: 'draft-present', message: 'Composer has unsaved user draft; aborted to avoid overwriting.' }};
              }}
              return {{ ok: true }};
            }})()
            """

            post_switch_composer_script = f"""
            (() => {{
              {_JS_DOM_PRELUDE}
              const composer = findComposer();
              if (!composer) {{
                return {{ ok: false, error: 'composer-not-found', message: 'Could not locate visible chat input element via CDP.' }};
              }}
              if (readDraft(composer).trim().length > 0) {{
                return {{ ok: false, error: 'draft-present', message: 'Target session composer has unsaved draft; aborted.' }};
              }}
              return {{ ok: true }};
            }})()
            """

            with SimpleWebSocket(ws_url, timeout=15.0) as ws:
                pre_draft = self._cdp_eval(ws, pre_draft_script, await_promise=False)
                if not isinstance(pre_draft, dict) or not pre_draft.get("ok"):
                    raise RuntimeError(f"{self.app_name} CDP send prep failed: {pre_draft}")

                _, switched, switch_method = self._ensure_target_session(
                    ws,
                    expected_sid,
                    target_title,
                    raw_session_id=session_id,
                )

                composer_check = self._cdp_eval(ws, post_switch_composer_script, await_promise=False)
                if not isinstance(composer_check, dict) or not composer_check.get("ok"):
                    raise RuntimeError(f"{self.app_name} CDP send prep failed: {composer_check}")

                model_selection = self._apply_model_config_via_cdp(
                    ws,
                    model=model,
                    provider=provider,
                    reasoning_effort=reasoning_effort,
                )

                final_identity = self._read_active_session_identity(ws)
                if not self._matches_target_session(final_identity, expected_sid, expected_title=target_title):
                    raise SessionMismatchError(
                        app_name=self.app_name,
                        expected_session_id=expected_sid,
                        expected_title=target_title,
                        actual_session_id=str(final_identity.get("session_id") or ""),
                        actual_url=str(final_identity.get("url") or ""),
                        actual_title=str(final_identity.get("doc_title") or final_identity.get("title") or ""),
                        reason=final_identity.get("conflict") or "active session drifted prior to text injection",
                    )

                res = self._cdp_inject_and_send(ws, text)
                res["switched"] = switched
                verified_sid = expected_sid
                verified_title = str(final_identity.get("title") or target_title or "").strip()

            out = {
                "ok": True,
                "transport": "electron-cdp",
                "port": port,
                "mode": mode,
                "sessionId": verified_sid,
                "verified_session_id": verified_sid,
                "verified_title": verified_title,
                "verified_url": str(final_identity.get("url") or ""),
                "verified_source": str(final_identity.get("source") or ""),
                "switch_method": switch_method,
                **res,
            }
            if model_selection is not None:
                out["modelSelection"] = model_selection
            return out

    _PUA_BLOCK_RE = re.compile(r"[\ue000-\ue2ff][\s\S]*?\ue201")
    _PUA_CHAR_RE = re.compile(r"[\ue000-\uf8ff]")

    @classmethod
    def _clean_live_text(cls, text: str) -> str:
        """Strip ChatGPT private-use widget markers and collapse excess blank lines."""
        cleaned = cls._PUA_BLOCK_RE.sub(" ", str(text or ""))
        cleaned = cls._PUA_CHAR_RE.sub("", cleaned)
        cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def read_live_session(
        self,
        session_id: str,
        *,
        target_title: str = "",
        tail_messages: int = 10,
        max_chars_per_message: int = 8000,
    ) -> dict[str, Any]:
        """Switch to and verify target Electron session under mutex, then read full turns.

        Extraction prefers the React Fiber conversation model (mp.turns with
        contentReferences/fallbackMarkdown), which carries the complete assistant
        markdown even when the DOM truncates or collapses long responses, and falls
        back to rendered DOM nodes only when Fiber data is unavailable. The
        previously active session is restored in a finally block so automated
        cross-session reads never leave the desktop parked away from the user's
        working conversation.
        """
        expected_sid = self._normalize_session_id(session_id)
        if not expected_sid:
            raise ValueError(f"{self.app_name} read_live_session requires a non-empty session_id")

        with self._acquire_app_mutex():
            port, pages = self._discover_endpoint()
            if port is None or not pages:
                raise ConnectionError(
                    f"{self.app_name} CDP is not active. Relaunch with {self.launcher_hint} to read live UI sessions."
                )

            target = self._select_page(pages, expected_sid, target_title)
            ws_url = str(target["webSocketDebuggerUrl"])
            read_cfg = json.dumps({"tailMessages": max(1, int(tail_messages)), "sessionId": expected_sid})
            dom_read_script = _READ_LIVE_SESSION_SCRIPT.replace("__READ_CFG__", read_cfg).replace(
                "__JS_DOM_PRELUDE__", _JS_DOM_PRELUDE
            )

            with SimpleWebSocket(ws_url, timeout=15.0) as ws:
                try:
                    prev_identity = self._read_active_session_identity(ws)
                except Exception:  # noqa: BLE001
                    prev_identity = {}
                identity, switched, switch_method = self._ensure_target_session(
                    ws,
                    expected_sid,
                    target_title,
                    raw_session_id=session_id,
                )
                try:
                    # Brief retry so the React tree finishes mounting the switched
                    # conversation before extraction; refuse stale turns from the
                    # previous conversation by conversationId match.
                    data: dict[str, Any] = {}
                    deadline = time.monotonic() + 2.0
                    while True:
                        raw = self._cdp_eval(ws, dom_read_script, await_promise=False)
                        data = raw if isinstance(raw, dict) else {}
                        if data.get("source") == "fiber" or time.monotonic() >= deadline:
                            break
                        time.sleep(0.15)
                finally:
                    if switched:
                        prev_sid = self._normalize_session_id(prev_identity.get("session_id"))
                        prev_title = str(prev_identity.get("doc_title") or prev_identity.get("title") or "").strip()
                        if prev_sid and prev_sid != expected_sid:
                            try:
                                self._ensure_target_session(ws, prev_sid, prev_title)
                            except Exception:  # noqa: BLE001
                                pass

            raw_msgs = data.get("messages") or []
            out_msgs: list[dict[str, Any]] = []
            for m in raw_msgs:
                if not isinstance(m, dict):
                    continue
                content = self._clean_live_text(str(m.get("content") or ""))
                truncated = False
                if max_chars_per_message > 0 and len(content) > max_chars_per_message:
                    content = content[:max_chars_per_message]
                    truncated = True
                if not content:
                    continue
                entry: dict[str, Any] = {
                    "index": int(m.get("index") or (len(out_msgs) + 1)),
                    "role": str(m.get("role") or "assistant"),
                    "content": content,
                }
                if truncated:
                    entry["truncated"] = True
                out_msgs.append(entry)

            verified_title = str(identity.get("title") or target_title or expected_sid).strip()
            is_generating = bool(data.get("is_generating"))
            return {
                "session": {
                    "app": self.app_name,
                    "session_id": expected_sid,
                    "title": verified_title,
                    "status": "running" if is_generating else "idle",
                    "status_detail": "generating in UI" if is_generating else "",
                    "workspace": "",
                    "updated_at": "",
                    "updated_at_ms": int(time.time() * 1000),
                    "preview": out_msgs[-1]["content"][:140] if out_msgs else "",
                    "extra": {"source": "live-cdp", "port": port, "extraction": str(data.get("source") or "dom")},
                },
                "message_count_returned": len(out_msgs),
                "messages": out_msgs,
                "is_generating": is_generating,
                "verified_session_id": expected_sid,
                "verified_title": verified_title,
                "switched": switched,
                "switch_method": switch_method,
            }

    @staticmethod
    def _normalize_decision(decision: str) -> str:
        raw = (decision or "").strip().lower()
        if raw in ("yes", "y", "approve", "approved", "allow", "accept", "proceed", "confirm", "同意", "允许", "批准", "确认", "放行"):
            return "yes"
        if raw in ("no", "n", "reject", "rejected", "deny", "denied", "decline", "cancel", "拒绝", "不同意", "不允许", "驳回"):
            return "no"
        raise ValueError(f"Invalid approval decision '{decision}'; expected 'yes' or 'no'.")

    def respond_approval(
        self,
        session_id: str,
        decision: str = "yes",
        *,
        target_title: str = "",
        feedback: str = "",
    ) -> dict[str, Any]:
        """Verify target Electron session and click the matching approval/deny button via CDP."""
        expected_sid = self._normalize_session_id(session_id)
        if not expected_sid:
            raise ValueError(f"{self.app_name} respond_approval requires a non-empty session_id")
        norm_decision = self._normalize_decision(decision)

        with self._acquire_app_mutex():
            port, pages = self._discover_endpoint()
            if port is None or not pages:
                raise ConnectionError(
                    f"{self.app_name} CDP is not active. Relaunch with {self.launcher_hint} to enable GUI approval."
                )

            target = self._select_page(pages, expected_sid, target_title)
            ws_url = str(target["webSocketDebuggerUrl"])
            payload = json.dumps({"decision": norm_decision})
            click_approval_script = f"""
            (async () => {{
              // __agentHubRespondApproval
              const cfg = {payload};
              {_JS_DOM_PRELUDE}
              const inSidebarOrNav = (el) => Boolean(
                el.closest('aside, nav, [data-testid="conversation-row-sidebar"], [data-sidebar-cycle-item]')
              );
              const labelOf = (el) => (
                ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || '') + ' ' + (el.getAttribute('title') || '') + ' ' + (el.getAttribute('data-testid') || ''))
                .replace(/\\s+/g, ' ')
                .trim()
              );
              const yesTestIds = ['approve', 'allow', 'accept', 'proceed', 'confirm'];
              const noTestIds = ['deny', 'reject', 'decline'];
              const yesTextRe = /\\b(approve|allow(?: once| always)?|accept|proceed|confirm|run command|continue|yes)\\b|批准|允许|同意|确认|放行|继续执行/i;
              const noTextRe = /\\b(deny|reject|decline|don't allow|do not allow|cancel|no)\\b|拒绝|不允许|驳回|取消/i;

              const btns = [...document.querySelectorAll('button, [role="button"]')].filter((el) => (
                visible(el) && !el.disabled && el.getAttribute('aria-disabled') !== 'true' && !inSidebarOrNav(el)
              ));
              let best = null;
              let bestScore = 0;
              for (const el of btns) {{
                const full = labelOf(el);
                if (!full || full.length > 140) continue;
                const tid = String(el.getAttribute('data-testid') || '').toLowerCase();
                const txt = String(el.innerText || el.getAttribute('aria-label') || '').replace(/\\s+/g, ' ').trim();
                let score = 0;
                if (cfg.decision === 'yes') {{
                  if (yesTestIds.some((k) => tid.includes(k))) score += 25;
                  if (yesTextRe.test(txt)) score += 15;
                  if (/^(approve|allow|accept|proceed|confirm|yes|批准|允许|同意|确认|放行)$/i.test(txt)) score += 20;
                  if (noTextRe.test(txt) && !yesTextRe.test(txt)) score = 0;
                }} else {{
                  if (noTestIds.some((k) => tid.includes(k))) score += 25;
                  if (noTextRe.test(txt)) score += 15;
                  if (/^(deny|reject|decline|no|拒绝|不允许|驳回)$/i.test(txt)) score += 20;
                  if (yesTextRe.test(txt) && !noTextRe.test(txt)) score = 0;
                }}
                if (score > bestScore) {{
                  bestScore = score;
                  best = {{ el, label: txt || tid || 'button', score }};
                }}
              }}
              if (!best) {{
                return {{ ok: false, error: 'approval-button-not-found', candidate_count: btns.length }};
              }}
              best.el.click();
              await sleep(200);
              return {{ ok: true, clicked_label: best.label, score: best.score }};
            }})()
            """

            with SimpleWebSocket(ws_url, timeout=15.0) as ws:
                _, switched, switch_method = self._ensure_target_session(ws, expected_sid, target_title)
                final_identity = self._read_active_session_identity(ws)
                if not self._matches_target_session(final_identity, expected_sid):
                    raise SessionMismatchError(
                        app_name=self.app_name,
                        expected_session_id=expected_sid,
                        actual_session_id=str(final_identity.get("session_id") or ""),
                        actual_url=str(final_identity.get("url") or ""),
                        actual_title=str(final_identity.get("title") or ""),
                        reason="active session drifted prior to approval click",
                    )

                click_res = self._cdp_eval(ws, click_approval_script, await_promise=True)
                if not isinstance(click_res, dict) or not click_res.get("ok"):
                    raise RuntimeError(
                        f"{self.app_name} no visible '{norm_decision}' approval button found in verified session '{expected_sid}': {click_res}"
                    )

            verified_title = str(final_identity.get("title") or target_title or "").strip()
            return {
                "ok": True,
                "action": "respondApproval",
                "decision": norm_decision,
                "transport": "electron-cdp",
                "port": port,
                "sessionId": expected_sid,
                "verified_session_id": expected_sid,
                "verified_title": verified_title,
                "switched": switched,
                "switch_method": switch_method,
                "clicked_label": click_res.get("clicked_label", ""),
                **({"feedback_note": "GUI button click only; feedback text was not injected"} if feedback.strip() else {}),
            }

    def cancel_session(self, session_id: str, *, target_title: str = "") -> dict[str, Any]:
        """Verify target Electron session and click the active Stop/Cancel button via CDP."""
        expected_sid = self._normalize_session_id(session_id)
        if not expected_sid:
            raise ValueError(f"{self.app_name} cancel_session requires a non-empty session_id")

        with self._acquire_app_mutex():
            port, pages = self._discover_endpoint()
            if port is None or not pages:
                raise ConnectionError(
                    f"{self.app_name} CDP is not active. Relaunch with {self.launcher_hint} to enable GUI cancel."
                )

            target = self._select_page(pages, expected_sid, target_title)
            ws_url = str(target["webSocketDebuggerUrl"])
            stop_script = f"""
            (async () => {{
              // __agentHubCancelSession
              {_JS_DOM_PRELUDE}
              const stopBtn = [...document.querySelectorAll('button[aria-label*="Stop" i], button[aria-label*="停止"], button[data-testid*="stop" i]')]
                .filter((b) => visible(b) && !b.disabled)[0];
              if (!stopBtn) {{
                return {{ ok: false, error: 'stop-button-not-found' }};
              }}
              const lbl = (stopBtn.getAttribute('aria-label') || stopBtn.innerText || 'stop').trim();
              stopBtn.click();
              await sleep(200);
              return {{ ok: true, clicked_label: lbl }};
            }})()
            """
            with SimpleWebSocket(ws_url, timeout=15.0) as ws:
                _, switched, switch_method = self._ensure_target_session(ws, expected_sid, target_title)
                final_identity = self._read_active_session_identity(ws)
                if not self._matches_target_session(final_identity, expected_sid):
                    raise SessionMismatchError(
                        app_name=self.app_name,
                        expected_session_id=expected_sid,
                        actual_session_id=str(final_identity.get("session_id") or ""),
                        actual_url=str(final_identity.get("url") or ""),
                        actual_title=str(final_identity.get("title") or ""),
                        reason="active session drifted prior to cancel click",
                    )
                stop_res = self._cdp_eval(ws, stop_script, await_promise=True)
                if not isinstance(stop_res, dict) or not stop_res.get("ok"):
                    raise RuntimeError(f"{self.app_name} active Stop button not found for session '{expected_sid}': {stop_res}")

            return {
                "ok": True,
                "action": "cancelSession",
                "transport": "electron-cdp",
                "port": port,
                "sessionId": expected_sid,
                "verified_session_id": expected_sid,
                "verified_title": str(final_identity.get("title") or target_title or "").strip(),
                "switched": switched,
                "switch_method": switch_method,
                "clicked_label": stop_res.get("clicked_label", ""),
            }


class AntigravityWriter(CdpWriter):
    """CDP writer targeting Antigravity.app."""

    def __init__(self) -> None:
        super().__init__(
            app_name="antigravity",
            env_port_var="AGENT_HUB_ANTIGRAVITY_CDP_PORT",
            default_port=9224,
            process_regex=r"/Applications/Antigravity\.app/Contents/MacOS/Antigravity\b",
            launcher_hint="~/.hermes/scripts/launch-antigravity-cdp.sh",
            new_chat_selectors=[
                '[data-testid="new-conversation-button"]',
                'a[aria-label*="New Conversation" i]',
                'button[aria-label*="New Conversation" i]',
                '[data-testid*="new-chat" i]',
            ],
            new_chat_texts=["new conversation", "new chat", "新对话", "新建对话"],
            model_trigger_selectors=[
                '[data-testid="model-selector-trigger"]',
                'button[aria-label*="Select model" i]',
                'button[aria-label*="model" i]',
            ],
            reasoning_trigger_selectors=[
                '[data-testid*="reasoning" i]',
                '[data-testid*="thinking" i]',
                '[data-testid*="effort" i]',
                'button[aria-label*="thinking" i]',
                'button[aria-label*="effort" i]',
            ],
            new_chat_shortcut={"key": "L", "code": "KeyL", "modifiers": 12, "windowsVirtualKeyCode": 76},
        )


class CodexWriter(CdpWriter):
    """CDP writer targeting Codex (ChatGPT.app Aperitif)."""

    def __init__(self) -> None:
        super().__init__(
            app_name="codex",
            env_port_var="AGENT_HUB_CODEX_CDP_PORT",
            default_port=9222,
            process_regex=r"/Applications/ChatGPT\.app/Contents/MacOS/",
            launcher_hint="~/.hermes/scripts/launch-codex-cdp.sh",
            new_chat_selectors=[
                '[data-testid*="new-chat" i]',
                '[data-testid*="new-thread" i]',
                'button[aria-label*="新聊天"]',
                'button[aria-label*="新建"][aria-label*="聊天"]',
                'button[aria-label*="New chat" i]',
                'button[aria-label*="New thread" i]',
            ],
            new_chat_texts=["新聊天", "新线程", "new chat", "new thread", "new conversation"],
            model_trigger_selectors=[
                '[data-testid*="model-switch" i]',
                '[data-testid*="model-select" i]',
                'button[aria-label*="选择 ChatGPT 模型"]',
                'button[aria-label*="选择"][aria-label*="模型"]',
                'button[aria-label*="Select model" i]',
                'button[aria-label*="model" i]',
            ],
            reasoning_trigger_selectors=[
                '[data-testid*="reasoning" i]',
                '[data-testid*="effort" i]',
                'button[aria-label*="思考"]',
                'button[aria-label*="reasoning" i]',
                'button[aria-label*="effort" i]',
            ],
            new_chat_shortcut={"key": "n", "code": "KeyN", "modifiers": 4, "windowsVirtualKeyCode": 78},
        )


_READ_LIVE_SESSION_SCRIPT = r"""
(() => {
  // __agentHubReadLiveMessages
  const cfg = __READ_CFG__;
  __JS_DOM_PRELUDE__
  const cleanText = (t) => String(t || '')
    .replace(/[\ue000-\ue2ff][\s\S]*?\ue201/g, ' ')
    .replace(/[\ue000-\uf8ff]/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  const refText = (r) => {
    if (!r || typeof r !== 'object') return '';
    const dil = r.model_dil_v2;
    if (dil && typeof dil.fallbackMarkdown === 'string' && dil.fallbackMarkdown.trim()) return dil.fallbackMarkdown;
    if (typeof r.matched_text === 'string' && r.matched_text.trim()) return r.matched_text;
    if (typeof r.text === 'string' && r.text.trim()) return r.text;
    return '';
  };
  const expandAssistant = (it) => {
    let content = typeof it.content === 'string' ? it.content : '';
    const refs = Array.isArray(it.contentReferences) ? it.contentReferences : [];
    const ordered = refs
      .filter((r) => r && typeof r === 'object')
      .sort((a, b) => Number(a.start_idx || 0) - Number(b.start_idx || 0));
    const bodies = [];
    let seq = 0;
    content = content.replace(/::chatgpt-content-reference\{[^}]*\}/g, () => {
      const r = ordered[seq++] || null;
      const body = r ? refText(r) : '';
      if (!body) return '';
      bodies.push(body);
      return '\n' + body + '\n';
    });
    const seenHeads = new Set(bodies.map((b) => b.slice(0, 120)));
    for (const r of ordered) {
      const body = refText(r);
      if (!body) continue;
      const head = body.slice(0, 120);
      if (seenHeads.has(head)) continue;
      if (body.length > 80 && content.includes(body.slice(0, 80))) continue;
      seenHeads.add(head);
      content = content ? (content + '\n\n' + body) : body;
    }
    return cleanText(content);
  };
  const expandUser = (it) => {
    if (typeof it.message === 'string') return cleanText(it.message);
    const msg = it.message;
    if (msg && typeof msg === 'object') {
      const c = msg.content;
      if (typeof c === 'string') return cleanText(c);
      if (Array.isArray(c)) return cleanText(c.map((x) => (x && typeof x.text === 'string' ? x.text : '')).join('\n'));
    }
    if (typeof it.authoredPrompt === 'string') return cleanText(it.authoredPrompt);
    return '';
  };
  const readFiberTurns = (expectedId) => {
    const rootEl = document.getElementById('root') || document.body;
    if (!rootEl) return null;
    const fk = Object.keys(rootEl).find((k) => k.startsWith('__reactContainer$') || k.startsWith('__reactFiber$'));
    let rf = fk ? rootEl[fk] : null;
    if (rf && rf.current) rf = rf.current;
    if (!rf) return null;
    const visited = new Set();
    const q = [rf];
    let steps = 0;
    while (q.length && steps < 15000) {
      const f = q.shift();
      steps += 1;
      if (!f || visited.has(f)) continue;
      visited.add(f);
      const mp = f.memoizedProps;
      if (mp && typeof mp === 'object' && Array.isArray(mp.turns) && mp.turns.length > 0) {
        const cid = mp.turns[0] && mp.turns[0].conversationId;
        if (!expectedId || cid === expectedId) return mp.turns;
      }
      if (f.child) q.push(f.child);
      if (f.sibling) q.push(f.sibling);
    }
    return null;
  };
  const stopBtn = [...document.querySelectorAll('button[aria-label*="Stop" i], button[aria-label*="停止"], button[data-testid*="stop" i]')]
    .filter(visible)[0];
  const messages = [];
  let source = 'dom';
  const turns = readFiberTurns(cfg.sessionId);
  if (turns) {
    for (const t of turns) {
      const items = (t && t.turn && Array.isArray(t.turn.items)) ? t.turn.items : [];
      for (const it of items) {
        if (!it || typeof it !== 'object') continue;
        if (it.type === 'user-message') {
          const txt = expandUser(it);
          if (txt) messages.push({ role: 'user', content: txt });
        } else if (it.type === 'assistant-message') {
          if (it.completed === false) continue;
          const txt = expandAssistant(it);
          if (txt) messages.push({ role: 'assistant', content: txt });
        }
      }
    }
  }
  if (messages.length > 0) {
    source = 'fiber';
  } else {
    const pushMsg = (role, rawText) => {
      let txt = String(rawText || '').trim();
      if (!txt) return;
      if (role === 'user') txt = txt.replace(/^(?:你说：|You said)\s*/i, '').trim();
      if (!txt) return;
      messages.push({ role, content: txt });
    };
    const chatTurns = [...document.querySelectorAll('[data-content-search-turn-key]')].filter(visible);
    if (chatTurns.length > 0) {
      for (const turn of chatTurns) {
        const userEl = turn.querySelector('[class*="user-message"], [data-message-author-role="user"]');
        const userText = userEl ? (userEl.innerText || '').trim() : '';
        if (userText) pushMsg('user', userText);
        const assistEls = [...turn.querySelectorAll('[data-message-author-role="assistant"], [class*="markdown"], .prose, [data-testid="chatgpt-writing-block"]')]
          .filter((el) => visible(el) && !el.closest('[class*="user-message"], [data-message-author-role="user"]'));
        const topAssist = assistEls.filter((el) => !assistEls.some((other) => other !== el && other.contains(el)));
        if (topAssist.length > 0) {
          const joined = topAssist.map((el) => (el.innerText || '').trim()).filter(Boolean).join('\n\n');
          if (joined) pushMsg('assistant', joined);
        } else {
          const fullTxt = (turn.innerText || '').trim();
          if (fullTxt && userText && fullTxt.length > userText.length + 4) {
            const rest = fullTxt.replace(userText, '').trim();
            if (rest) pushMsg('assistant', rest);
          } else if (fullTxt && !userText) {
            pushMsg('assistant', fullTxt);
          }
        }
      }
    } else {
      const turnNodes = [...document.querySelectorAll(
        '[data-testid="user-message-content"], [data-message-author-role="user"], [data-message-author-role="assistant"], [data-testid="assistant-message"]'
      )].filter(visible);
      for (const el of turnNodes) {
        const tid = String(el.getAttribute('data-testid') || '');
        const roleAttr = String(el.getAttribute('data-message-author-role') || '');
        const role = (roleAttr === 'user' || tid.includes('user')) ? 'user' : 'assistant';
        pushMsg(role, el.innerText || el.textContent || '');
      }
    }
  }
  return {
    source,
    is_generating: Boolean(stopBtn),
    messages: messages.slice(-cfg.tailMessages).map((m, i) => ({ index: i + 1, role: m.role, content: m.content })),
  };
})()
"""
