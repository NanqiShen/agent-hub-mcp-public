'use strict';

const vscode = require('vscode');
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

let inspector = null;
try {
  inspector = require('node:inspector');
} catch {
  inspector = null;
}

const MAX_BODY_BYTES = 1024 * 1024;
const CLINE_EXTENSION_ID = 'saoudrizwan.claude-dev';
const controllerCache = new WeakMap();

let activeServer = null;
let boundPort = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getActiveTaskId(controller) {
  if (!controller || !controller.task) return null;
  const raw = controller.task.taskId || controller.task.ulid || controller.task.sessionId;
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  return trimmed || null;
}

function getPendingAsk(controller) {
  const msgs =
    controller &&
    controller.task &&
    controller.task.messageStateHandler &&
    Array.isArray(controller.task.messageStateHandler.messages)
      ? controller.task.messageStateHandler.messages
      : null;
  if (!msgs || msgs.length === 0) return null;
  const last = msgs[msgs.length - 1];
  if (!last || last.type !== 'ask' || last.partial) return null;
  return {
    ask: typeof last.ask === 'string' ? last.ask : '',
    text: String(last.text || '').slice(0, 300),
    ts: last.ts ?? null,
  };
}

function isClineController(obj) {
  return Boolean(
    obj &&
      typeof obj === 'object' &&
      typeof obj.showTaskWithId === 'function' &&
      (typeof obj.initTask === 'function' ||
        typeof obj.clearTask === 'function' ||
        typeof obj.postStateToWebview === 'function')
  );
}

async function extractControllerViaInspector(fn) {
  if (typeof fn !== 'function' || !inspector || typeof inspector.Session !== 'function') {
    return null;
  }

  const suffix = crypto.randomBytes(8).toString('hex');
  const probeKey = `__ahb_probe_${suffix}`;
  const outKey = `__ahb_out_${suffix}`;
  const session = new inspector.Session();
  const post = (method, params = {}) =>
    new Promise((resolve, reject) => {
      session.post(method, params, (err, res) => {
        if (err) reject(err);
        else resolve(res);
      });
    });

  globalThis[probeKey] = fn;
  try {
    session.connect();
    const evalRes = await post('Runtime.evaluate', {
      expression: `globalThis[${JSON.stringify(probeKey)}]`,
    });
    const fnObjId = evalRes && evalRes.result && evalRes.result.objectId;
    if (!fnObjId) return null;

    const props = await post('Runtime.getProperties', { objectId: fnObjId });
    const scopesProp = (props.internalProperties || []).find((p) => p.name === '[[Scopes]]');
    const scopesObjId = scopesProp && scopesProp.value && scopesProp.value.objectId;
    if (!scopesObjId) return null;

    const scopeList = await post('Runtime.getProperties', { objectId: scopesObjId });
    const checkDecl = `function() {
      var check = function(o) {
        return Boolean(
          o &&
          typeof o === 'object' &&
          typeof o.showTaskWithId === 'function' &&
          (typeof o.initTask === 'function' ||
            typeof o.clearTask === 'function' ||
            typeof o.postStateToWebview === 'function')
        );
      };
      if (check(this)) {
        globalThis[${JSON.stringify(outKey)}] = this;
        return true;
      }
      if (this && typeof this === 'object' && check(this.controller)) {
        globalThis[${JSON.stringify(outKey)}] = this.controller;
        return true;
      }
      return false;
    }`;

    for (const scope of scopeList.result || []) {
      if (!scope || !scope.value || !scope.value.objectId) continue;
      if (scope.name === 'Global' || scope.description === 'Global') continue;

      const vars = await post('Runtime.getProperties', { objectId: scope.value.objectId });
      for (const v of vars.result || []) {
        const objId = v && v.value && v.value.type === 'object' && v.value.objectId;
        if (!objId) continue;
        try {
          const callRes = await post('Runtime.callFunctionOn', {
            objectId: objId,
            functionDeclaration: checkDecl,
            returnByValue: true,
          });
          if (callRes && callRes.result && callRes.result.value === true) {
            const candidate = globalThis[outKey];
            if (isClineController(candidate)) {
              return candidate;
            }
          }
        } catch {
          // Skip inaccessible proxies or disposed handles.
        }
      }
    }
    return null;
  } catch {
    return null;
  } finally {
    delete globalThis[probeKey];
    delete globalThis[outKey];
    try {
      session.disconnect();
    } catch {
      // Ignore disconnect errors.
    }
  }
}

async function getClineController(api) {
  if (!api || typeof api !== 'object') return null;

  if (isClineController(api)) return api;
  if (isClineController(api._controller)) return api._controller;
  if (isClineController(api.controller)) return api.controller;

  const cached = controllerCache.get(api);
  if (isClineController(cached)) return cached;

  for (const fn of [api.sendMessage, api.startNewTask, api.pressPrimaryButton]) {
    if (typeof fn !== 'function') continue;
    const found = await extractControllerViaInspector(fn);
    if (isClineController(found)) {
      controllerCache.set(api, found);
      return found;
    }
  }
  return null;
}

async function waitForActiveTask(controller, targetSessionId, timeoutMs = 3000) {
  const deadline = Date.now() + Math.max(0, Number(timeoutMs) || 0);
  while (true) {
    if (targetSessionId) {
      if (getActiveTaskId(controller) === targetSessionId) return true;
    } else if (controller && controller.task) {
      return true;
    }
    if (Date.now() >= deadline) return false;
    await sleep(50);
  }
}

function stateDirPath() {
  return path.join(os.homedir(), '.hermes', 'agent-hub');
}

function tokenFilePath() {
  const override = (process.env.AGENT_HUB_CLINE_BRIDGE_TOKEN_PATH || '').trim();
  return override || path.join(stateDirPath(), 'cline-bridge.token');
}

function stateFilePath() {
  const override = (process.env.AGENT_HUB_CLINE_BRIDGE_STATE_PATH || '').trim();
  return override || path.join(stateDirPath(), 'cline-bridge.json');
}

function portStateFilePath(port) {
  return path.join(path.dirname(stateFilePath()), 'cline-bridges', `port-${port}.json`);
}

function loadOrCreateToken() {
  const filePath = tokenFilePath();
  const dirPath = path.dirname(filePath);
  fs.mkdirSync(dirPath, { recursive: true, mode: 0o700 });
  try {
    fs.chmodSync(dirPath, 0o700);
  } catch {
    // Best-effort permission hardening.
  }

  if (fs.existsSync(filePath)) {
    const existing = fs.readFileSync(filePath, 'utf8').trim();
    if (existing.length >= 32) {
      try {
        fs.chmodSync(filePath, 0o600);
      } catch {
        // Ignore.
      }
      return existing;
    }
  }

  const token = crypto.randomBytes(32).toString('hex');
  fs.writeFileSync(filePath, `${token}\n`, { encoding: 'utf8', mode: 0o600 });
  try {
    fs.chmodSync(filePath, 0o600);
  } catch {
    // Ignore.
  }
  return token;
}

function verifyBearer(authHeader, expectedToken) {
  if (typeof authHeader !== 'string') return false;
  const prefix = 'Bearer ';
  if (!authHeader.startsWith(prefix)) return false;
  const supplied = Buffer.from(authHeader.slice(prefix.length).trim(), 'utf8');
  const expected = Buffer.from(expectedToken, 'utf8');
  if (supplied.length !== expected.length) return false;
  return crypto.timingSafeEqual(supplied, expected);
}

function sendJson(res, status, payload) {
  if (res.destroyed || res.writableEnded) return;
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(Buffer.byteLength(body)),
  });
  res.end(body);
}

async function readJsonBody(req) {
  const declared = Number(req.headers['content-length']);
  if (Number.isFinite(declared) && declared > MAX_BODY_BYTES) {
    const err = new Error('payload-too-large');
    err.status = 413;
    throw err;
  }
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > MAX_BODY_BYTES) {
      const err = new Error('payload-too-large');
      err.status = 413;
      throw err;
    }
    chunks.push(buf);
  }
  const raw = Buffer.concat(chunks, total).toString('utf8').trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    const err = new Error('invalid-json');
    err.status = 400;
    throw err;
  }
}

function currentWorkspaces() {
  return (vscode.workspace.workspaceFolders || []).map((f) => f.uri.fsPath || f.uri.toString());
}

async function getClineApi({ activateIfNeeded = true } = {}) {
  const ext = vscode.extensions.getExtension(CLINE_EXTENSION_ID);
  if (!ext) {
    return { installed: false, active: false, api: null };
  }
  let api = ext.isActive ? ext.exports : null;
  if (!ext.isActive && activateIfNeeded) {
    api = await ext.activate();
  } else if (ext.isActive && !api) {
    api = ext.exports;
  }
  return {
    installed: true,
    active: Boolean(ext.isActive),
    api: api || null,
  };
}

async function applyModelConfig(api, { model, provider, reasoningEffort }) {
  const reqModel = typeof model === 'string' ? model.trim() : '';
  const reqProvider = typeof provider === 'string' ? provider.trim() : '';
  const reqEffort = typeof reasoningEffort === 'string' ? reasoningEffort.trim() : '';
  if (!reqModel && !reqProvider && !reqEffort) return null;

  const requested = {
    ...(reqProvider ? { provider: reqProvider } : {}),
    ...(reqModel ? { model: reqModel } : {}),
    ...(reqEffort ? { reasoningEffort: reqEffort } : {}),
  };

  try {
    if (api && typeof api.updateSettings === 'function') {
      await api.updateSettings(requested);
      return { applied: true, method: 'api.updateSettings', requested };
    }
    if (api && typeof api.setModel === 'function' && reqModel) {
      await api.setModel(reqModel);
      return { applied: true, method: 'api.setModel', requested };
    }
  } catch (err) {
    return {
      applied: false,
      requested,
      warning: err && err.message ? err.message : String(err),
    };
  }

  return {
    applied: false,
    requested,
    note: 'forwarded to Cline bridge; Cline VS Code extension API v4 does not expose setModel',
  };
}

function writeStateFile(port, { writePortRegistry = true } = {}) {
  const filePath = stateFilePath();
  const dirPath = path.dirname(filePath);
  fs.mkdirSync(dirPath, { recursive: true, mode: 0o700 });
  const payload = {
    port,
    pid: process.pid,
    workspaces: currentWorkspaces(),
    startedAt: new Date().toISOString(),
  };
  const jsonStr = `${JSON.stringify(payload, null, 2)}\n`;
  fs.writeFileSync(filePath, jsonStr, { encoding: 'utf8', mode: 0o600 });
  try {
    fs.chmodSync(filePath, 0o600);
  } catch {
    // Ignore.
  }

  if (writePortRegistry) {
    const portFile = portStateFilePath(port);
    const bridgesDir = path.dirname(portFile);
    fs.mkdirSync(bridgesDir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(portFile, jsonStr, { encoding: 'utf8', mode: 0o600 });
    try {
      fs.chmodSync(portFile, 0o600);
    } catch {
      // Ignore.
    }
  }
}

function clearStateFileIfOwned() {
  const candidates = [stateFilePath()];
  if (boundPort) {
    candidates.push(portStateFilePath(boundPort));
  }
  for (const filePath of candidates) {
    try {
      if (!fs.existsSync(filePath)) continue;
      const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      if (data && data.pid === process.pid) {
        fs.unlinkSync(filePath);
      }
    } catch {
      // Ignore cleanup errors.
    }
  }
}

function listenOnPortRange(server, startPort, endPort) {
  return new Promise((resolve, reject) => {
    let port = startPort;
    const tryNext = () => {
      const onError = (err) => {
        server.removeListener('listening', onListening);
        if (err && err.code === 'EADDRINUSE' && port < endPort) {
          port += 1;
          tryNext();
          return;
        }
        reject(err);
      };
      const onListening = () => {
        server.removeListener('error', onError);
        resolve(port);
      };
      server.once('error', onError);
      server.once('listening', onListening);
      server.listen(port, '127.0.0.1');
    };
    tryNext();
  });
}

function makeMismatchError(message, expectedSessionId, actualSessionId) {
  const err = new Error(message);
  err.status = 500;
  err.errorCode = 'session_mismatch';
  err.expectedSessionId = expectedSessionId || '';
  err.actualSessionId = actualSessionId || '';
  return err;
}

async function ensureTargetSession(
  controller,
  sessionId,
  {
    focusSidebar = false,
    executeCommandImpl,
    sidebarFocusDelayMs = 250,
    postActivateSettleMs = 100,
    waitForActiveTaskTimeoutMs = 3000,
  }
) {
  const isExplicitSession = Boolean(
    sessionId && sessionId !== 'active' && sessionId !== 'vscode-cline-active'
  );
  let activeTaskId = getActiveTaskId(controller);
  let activatedSession = false;

  if (isExplicitSession && activeTaskId !== sessionId) {
    // Focus sidebar BEFORE loading task so VscodeWebviewProvider's initial clearTask() doesn't wipe it out.
    try {
      await executeCommandImpl('claude-dev.SidebarProvider.focus');
    } catch {
      // Non-fatal if sidebar view is already open or hidden.
    }
    if (sidebarFocusDelayMs > 0) {
      await sleep(sidebarFocusDelayMs);
    }

    if (getActiveTaskId(controller) !== sessionId) {
      if (typeof controller.showTaskWithId !== 'function') {
        const err = new Error('Cline controller does not expose showTaskWithId');
        err.status = 500;
        throw err;
      }
      try {
        await controller.showTaskWithId(sessionId);
      } catch (showErr) {
        const msg = showErr && showErr.message ? showErr.message : String(showErr);
        throw makeMismatchError(msg, sessionId, getActiveTaskId(controller));
      }
      const ready = await waitForActiveTask(controller, sessionId, waitForActiveTaskTimeoutMs);
      const loadedTaskId = getActiveTaskId(controller);
      if (!ready || loadedTaskId !== sessionId) {
        throw makeMismatchError(
          `Failed to activate Cline session "${sessionId}" (activeTaskId=${loadedTaskId || 'none'})`,
          sessionId,
          loadedTaskId
        );
      }
      if (postActivateSettleMs > 0) {
        await sleep(postActivateSettleMs);
      }
      activatedSession = true;
      activeTaskId = loadedTaskId;
    }
  } else if (isExplicitSession && activeTaskId === sessionId) {
    if (focusSidebar) {
      try {
        await executeCommandImpl('claude-dev.SidebarProvider.focus');
      } catch {
        // Ignore.
      }
    }
  } else {
    if (focusSidebar) {
      try {
        await executeCommandImpl('claude-dev.SidebarProvider.focus');
      } catch {
        // Ignore.
      }
    }
    if (!controller.task) {
      const err = new Error(
        'No active Cline task to send message to (t.task is null); pass sessionId to auto-activate a historical task'
      );
      err.status = 500;
      throw err;
    }
  }

  return { isExplicitSession, activeTaskId, activatedSession };
}

function createRequestHandler({
  token,
  getPort = () => boundPort,
  getClineApiImpl = getClineApi,
  executeCommandImpl = (cmd, ...args) => vscode.commands.executeCommand(cmd, ...args),
  getWorkspacesImpl = currentWorkspaces,
  sidebarFocusDelayMs = 250,
  postActivateSettleMs = 100,
  waitForActiveTaskTimeoutMs = 3000,
} = {}) {
  return async (req, res) => {
    try {
      if (!verifyBearer(req.headers.authorization, token)) {
        sendJson(res, 401, { ok: false, error: 'unauthorized' });
        return;
      }

      const activePort = typeof getPort === 'function' ? getPort() : boundPort;
      const url = new URL(req.url || '/', `http://127.0.0.1:${activePort || 43918}`);
      const pathname = url.pathname;

      if (req.method === 'GET' && pathname === '/health') {
        const info = await getClineApiImpl({ activateIfNeeded: false });
        const api = info.api || {};
        let controller = null;
        if (info.active && info.api) {
          try {
            controller = await getClineController(info.api);
          } catch {
            controller = null;
          }
        }
        sendJson(res, 200, {
          ok: true,
          plugin: 'agent-hub-cline-bridge',
          port: activePort,
          pid: process.pid,
          clineInstalled: info.installed,
          clineActive: info.active,
          hasStartNewTask: typeof api.startNewTask === 'function',
          hasSendMessage: typeof api.sendMessage === 'function',
          hasPressPrimaryButton: typeof api.pressPrimaryButton === 'function',
          hasPressSecondaryButton: typeof api.pressSecondaryButton === 'function',
          hasController: Boolean(controller),
          hasShowTaskWithId: Boolean(controller && typeof controller.showTaskWithId === 'function'),
          hasCancelTask: Boolean(
            (controller && typeof controller.cancelTask === 'function') ||
              typeof api.cancelTask === 'function'
          ),
          activeTaskId: getActiveTaskId(controller),
          pendingAsk: getPendingAsk(controller),
          workspaces: getWorkspacesImpl(),
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/new-task') {
        const body = await readJsonBody(req);
        const text = typeof body.text === 'string' ? body.text.trim() : '';
        const focusSidebar = body.focusSidebar !== false;
        const model = typeof body.model === 'string' ? body.model.trim() : '';
        const provider = typeof body.provider === 'string' ? body.provider.trim() : '';
        const reasoningEffort = typeof body.reasoningEffort === 'string' ? body.reasoningEffort.trim() : '';
        if (!text) {
          sendJson(res, 400, { ok: false, error: 'non-empty text is required' });
          return;
        }
        const info = await getClineApiImpl({ activateIfNeeded: true });
        if (!info.installed || !info.api || typeof info.api.startNewTask !== 'function') {
          sendJson(res, 503, { ok: false, error: 'Cline extension API startNewTask is unavailable' });
          return;
        }
        if (focusSidebar) {
          try {
            await executeCommandImpl('claude-dev.SidebarProvider.focus');
          } catch {
            // Non-fatal if sidebar view is already open or hidden.
          }
        }
        const modelSelection = await applyModelConfig(info.api, { model, provider, reasoningEffort });
        await info.api.startNewTask(text);
        let sessionId = null;
        try {
          const controller = await getClineController(info.api);
          if (controller) {
            await waitForActiveTask(controller, null, waitForActiveTaskTimeoutMs);
            sessionId = getActiveTaskId(controller);
          }
        } catch {
          sessionId = null;
        }
        sendJson(res, 200, {
          ok: true,
          action: 'startNewTask',
          ...(sessionId ? { sessionId } : {}),
          port: activePort,
          workspaces: getWorkspacesImpl(),
          ...(modelSelection ? { modelSelection } : {}),
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/send') {
        const body = await readJsonBody(req);
        const text = typeof body.text === 'string' ? body.text.trim() : '';
        const sessionId = typeof body.sessionId === 'string' ? body.sessionId.trim() : '';
        const focusSidebar = body.focusSidebar === true;
        const model = typeof body.model === 'string' ? body.model.trim() : '';
        const provider = typeof body.provider === 'string' ? body.provider.trim() : '';
        const reasoningEffort = typeof body.reasoningEffort === 'string' ? body.reasoningEffort.trim() : '';
        if (!text) {
          sendJson(res, 400, { ok: false, error: 'non-empty text is required' });
          return;
        }
        const info = await getClineApiImpl({ activateIfNeeded: true });
        if (!info.installed || !info.api || typeof info.api.sendMessage !== 'function') {
          sendJson(res, 503, { ok: false, error: 'Cline extension API sendMessage is unavailable' });
          return;
        }

        const controller = await getClineController(info.api);
        if (!controller) {
          sendJson(res, 500, {
            ok: false,
            error: 'Unable to access Cline internal controller to verify or activate task; refusing silent drop',
          });
          return;
        }

        const { isExplicitSession, activatedSession } = await ensureTargetSession(controller, sessionId, {
          focusSidebar,
          executeCommandImpl,
          sidebarFocusDelayMs,
          postActivateSettleMs,
          waitForActiveTaskTimeoutMs,
        });

        const modelSelection = await applyModelConfig(info.api, { model, provider, reasoningEffort });

        const preSendId = getActiveTaskId(controller);
        if (!controller.task || (isExplicitSession && preSendId !== sessionId)) {
          sendJson(res, 500, {
            ok: false,
            ...(isExplicitSession
              ? {
                  error_code: 'session_mismatch',
                  expectedSessionId: sessionId,
                  actualSessionId: preSendId || '',
                }
              : {}),
            error: isExplicitSession
              ? `Target Cline session "${sessionId}" is not active prior to sendMessage (activeTaskId=${preSendId || 'none'})`
              : 'No active Cline task prior to sendMessage (t.task is null)',
          });
          return;
        }

        const prevMsgs =
          controller.task &&
          controller.task.messageStateHandler &&
          Array.isArray(controller.task.messageStateHandler.messages)
            ? controller.task.messageStateHandler.messages
            : null;
        const prevMsgCount = prevMsgs ? prevMsgs.length : -1;

        await info.api.sendMessage(text);

        if (!controller.task) {
          sendJson(res, 500, {
            ok: false,
            error: 'Cline task became inactive during sendMessage',
          });
          return;
        }

        const postMsgs =
          controller.task &&
          controller.task.messageStateHandler &&
          Array.isArray(controller.task.messageStateHandler.messages)
            ? controller.task.messageStateHandler.messages
            : null;
        if (postMsgs && prevMsgCount >= 0 && postMsgs.length > prevMsgCount) {
          const added = postMsgs.slice(prevMsgCount);
          const resumeErr = added.find(
            (m) =>
              m &&
              m.type === 'say' &&
              m.say === 'error' &&
              String(m.text || '').includes('Failed to resume task')
          );
          if (resumeErr) {
            sendJson(res, 500, {
              ok: false,
              error: String(resumeErr.text || 'Failed to resume task'),
            });
            return;
          }
        }

        sendJson(res, 200, {
          ok: true,
          action: 'sendMessage',
          sessionId: getActiveTaskId(controller) || (isExplicitSession ? sessionId : null),
          activatedSession,
          port: activePort,
          workspaces: getWorkspacesImpl(),
          ...(modelSelection ? { modelSelection } : {}),
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/approve') {
        const body = await readJsonBody(req);
        const sessionId = typeof body.sessionId === 'string' ? body.sessionId.trim() : '';
        const rawDecision = typeof body.decision === 'string' ? body.decision.trim().toLowerCase() : 'yes';
        const decision = ['yes', 'y', 'approve', 'allow'].includes(rawDecision)
          ? 'yes'
          : ['no', 'n', 'reject', 'deny'].includes(rawDecision)
            ? 'no'
            : '';
        const feedback = typeof body.feedback === 'string' ? body.feedback.trim() : '';
        const focusSidebar = body.focusSidebar === true;
        if (!decision) {
          sendJson(res, 400, { ok: false, error: 'decision must be yes or no' });
          return;
        }

        const info = await getClineApiImpl({ activateIfNeeded: true });
        if (!info.installed || !info.api) {
          sendJson(res, 503, { ok: false, error: 'Cline extension is unavailable' });
          return;
        }
        const controller = await getClineController(info.api);
        if (!controller) {
          sendJson(res, 500, { ok: false, error: 'Unable to access Cline internal controller for /approve' });
          return;
        }

        const { isExplicitSession, activatedSession } = await ensureTargetSession(controller, sessionId, {
          focusSidebar,
          executeCommandImpl,
          sidebarFocusDelayMs,
          postActivateSettleMs,
          waitForActiveTaskTimeoutMs,
        });

        const preId = getActiveTaskId(controller);
        if (!controller.task || (isExplicitSession && preId !== sessionId)) {
          throw makeMismatchError(
            `Target Cline session "${sessionId}" is not active prior to approval`,
            sessionId,
            preId
          );
        }

        const responseType = decision === 'yes' ? 'yesButtonClicked' : 'noButtonClicked';
        if (feedback && typeof controller.task.handleWebviewAskResponse === 'function') {
          await controller.task.handleWebviewAskResponse(responseType, feedback, []);
        } else if (decision === 'yes' && typeof info.api.pressPrimaryButton === 'function') {
          await info.api.pressPrimaryButton();
        } else if (decision === 'no' && typeof info.api.pressSecondaryButton === 'function') {
          await info.api.pressSecondaryButton();
        } else if (typeof controller.task.handleWebviewAskResponse === 'function') {
          await controller.task.handleWebviewAskResponse(responseType, feedback, []);
        } else {
          sendJson(res, 503, { ok: false, error: 'Cline approval button API is unavailable' });
          return;
        }

        sendJson(res, 200, {
          ok: true,
          action: 'respondApproval',
          decision,
          sessionId: getActiveTaskId(controller) || (isExplicitSession ? sessionId : null),
          activatedSession,
          port: activePort,
          workspaces: getWorkspacesImpl(),
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/cancel') {
        const body = await readJsonBody(req);
        const sessionId = typeof body.sessionId === 'string' ? body.sessionId.trim() : '';
        const focusSidebar = body.focusSidebar === true;
        const info = await getClineApiImpl({ activateIfNeeded: true });
        if (!info.installed || !info.api) {
          sendJson(res, 503, { ok: false, error: 'Cline extension is unavailable' });
          return;
        }
        const controller = await getClineController(info.api);
        if (!controller) {
          sendJson(res, 500, { ok: false, error: 'Unable to access Cline internal controller for /cancel' });
          return;
        }

        const { isExplicitSession, activatedSession } = await ensureTargetSession(controller, sessionId, {
          focusSidebar,
          executeCommandImpl,
          sidebarFocusDelayMs,
          postActivateSettleMs,
          waitForActiveTaskTimeoutMs,
        });

        const preId = getActiveTaskId(controller);
        if (isExplicitSession && preId !== sessionId) {
          throw makeMismatchError(
            `Target Cline session "${sessionId}" is not active prior to cancel`,
            sessionId,
            preId
          );
        }

        if (typeof controller.cancelTask === 'function') {
          await controller.cancelTask();
        } else if (typeof info.api.cancelTask === 'function') {
          await info.api.cancelTask();
        } else {
          sendJson(res, 501, { ok: false, error: 'Cline controller does not expose cancelTask' });
          return;
        }

        sendJson(res, 200, {
          ok: true,
          action: 'cancelTask',
          sessionId: getActiveTaskId(controller) || (isExplicitSession ? sessionId : null),
          activatedSession,
          port: activePort,
          workspaces: getWorkspacesImpl(),
        });
        return;
      }

      sendJson(res, 404, { ok: false, error: `Unknown route ${req.method} ${pathname}` });
    } catch (err) {
      const status = Number.isInteger(err && err.status) ? err.status : 500;
      sendJson(res, status, {
        ok: false,
        ...(err && err.errorCode ? { error_code: err.errorCode } : {}),
        ...(err && err.expectedSessionId ? { expectedSessionId: err.expectedSessionId } : {}),
        ...(err && err.actualSessionId !== undefined ? { actualSessionId: err.actualSessionId } : {}),
        error: err && err.message ? err.message : String(err),
      });
    }
  };
}

async function activate(context) {
  const token = loadOrCreateToken();
  const startPort = Number(process.env.AGENT_HUB_CLINE_BRIDGE_PORT || 43918);

  const server = http.createServer(
    createRequestHandler({
      token,
      getPort: () => boundPort || startPort,
    })
  );

  server.on('error', () => {
    // Handled during listenOnPortRange; avoid crashing extension host on async socket error.
  });

  boundPort = await listenOnPortRange(server, startPort, startPort + 7);
  activeServer = server;
  writeStateFile(boundPort);

  context.subscriptions.push({
    dispose: () => deactivate(),
  });
}

function deactivate() {
  clearStateFileIfOwned();
  if (activeServer) {
    try {
      activeServer.closeAllConnections?.();
      activeServer.close();
    } catch {
      // Ignore close errors.
    }
    activeServer = null;
  }
  boundPort = null;
}

module.exports = {
  activate,
  deactivate,
  _internals: {
    extractControllerViaInspector,
    getClineController,
    getActiveTaskId,
    getPendingAsk,
    isClineController,
    waitForActiveTask,
    createRequestHandler,
  },
};
