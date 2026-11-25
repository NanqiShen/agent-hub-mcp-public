import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';

export const name = 'dsh-agent-hub-bridge';
export const inject = ['connection', 'typertGateway'];

const MAX_BODY_BYTES = 1024 * 1024;

function tokenFilePath() {
  const override = process.env.AGENT_HUB_DSH_BRIDGE_TOKEN_PATH?.trim();
  if (override) return override;
  return path.join(os.homedir(), '.hermes', 'agent-hub', 'dsh-bridge.token');
}

function loadOrCreateToken() {
  const filePath = tokenFilePath();
  const dirPath = path.dirname(filePath);
  mkdirSync(dirPath, { recursive: true, mode: 0o700 });
  try {
    chmodSync(dirPath, 0o700);
  } catch {
    // Best-effort permission hardening.
  }

  if (existsSync(filePath)) {
    const existing = readFileSync(filePath, 'utf8').trim();
    if (existing.length >= 32) {
      try {
        chmodSync(filePath, 0o600);
      } catch {
        // Ignore if immutable.
      }
      return existing;
    }
  }

  const token = randomBytes(32).toString('hex');
  writeFileSync(filePath, `${token}\n`, { encoding: 'utf8', mode: 0o600 });
  try {
    chmodSync(filePath, 0o600);
  } catch {
    // Best-effort permission hardening.
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
  return timingSafeEqual(supplied, expected);
}

function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  for (const child of Object.values(value)) deepFreeze(child);
  return Object.freeze(value);
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

function serializeError(error) {
  if (!error) return { code: 'unknown', message: 'Unknown error' };
  const failure = error.failure;
  if (failure && typeof failure === 'object') {
    return {
      code: String(failure.code || error.code || 'rpc-error'),
      message: String(failure.message || error.message || 'RPC error'),
      details: failure.details || {},
    };
  }
  return {
    code: String(error.code || 'internal'),
    message: error instanceof Error ? error.message : String(error),
  };
}

export function apply(ctx) {
  const log = typeof ctx?.logger === 'function' ? ctx.logger(name) : (ctx?.logger ?? console);
  const port = Number(process.env.AGENT_HUB_DSH_BRIDGE_PORT || 43917);
  const token = loadOrCreateToken();
  const peer = ctx.connection.createPeer({
    principal: { kind: 'owner' },
    interactions: 'none',
  });

  const invokeGateway = (namespace, method, args) => (
    ctx.typertGateway.invoke({ namespace, method, args, peer })
  );

  async function resolveModelTarget(sessionId, { provider, model }) {
    const reqProvider = typeof provider === 'string' ? provider.trim() : '';
    const reqModel = typeof model === 'string' ? model.trim() : '';
    const current = ctx.get?.('agents')?.get?.(sessionId)?.model?.current?.();

    let resolvedProvider = reqProvider;
    let resolvedModel = reqModel;
    const llm = ctx.get?.('llm');

    if (llm && typeof llm.listProviders === 'function' && typeof llm.listModels === 'function' && (reqModel || reqProvider)) {
      try {
        const providers = await llm.listProviders();
        const provLow = reqProvider.toLowerCase();
        const modelLow = reqModel.toLowerCase();

        const orderedProviders = [...(providers || [])].sort((a, b) => {
          const aId = String(a?.id || '').toLowerCase();
          const bId = String(b?.id || '').toLowerCase();
          const aScore = provLow ? (aId === provLow ? 2 : (aId.includes(provLow) ? 1 : 0)) : 0;
          const bScore = provLow ? (bId === provLow ? 2 : (bId.includes(provLow) ? 1 : 0)) : 0;
          return bScore - aScore;
        });

        const matchingProviders = provLow
          ? orderedProviders.filter((p) => {
              const pid = String(p?.id || '').toLowerCase();
              const pname = String(p?.name || '').toLowerCase();
              return pid === provLow || pid.includes(provLow) || pname.includes(provLow);
            })
          : orderedProviders;

        const searchProviders = matchingProviders.length > 0 ? matchingProviders : orderedProviders;
        if (reqModel) {
          let bestHit = null;
          let bestScore = -1;
          for (const p of searchProviders) {
            const pid = String(p?.id || '');
            if (!pid) continue;
            let models = [];
            try {
              models = await llm.listModels(pid);
            } catch {
              continue;
            }
            for (const m of models || []) {
              const mid = String(m?.id || '');
              const mname = String(m?.name || '');
              const midLow = mid.toLowerCase();
              const mnameLow = mname.toLowerCase();
              let score = -1;
              if (mid === reqModel) score = 5;
              else if (midLow === modelLow || mnameLow === modelLow) score = 4;
              else if (midLow.includes(modelLow)) score = 3;
              else if (mnameLow.includes(modelLow)) score = 2;
              if (score > 0 && provLow && pid.toLowerCase().includes(provLow)) {
                score += 2;
              }
              if (score > bestScore) {
                bestScore = score;
                bestHit = { provider: pid, model: mid };
              }
            }
          }
          if (bestHit) {
            resolvedProvider = bestHit.provider;
            resolvedModel = bestHit.model;
          } else if (!resolvedProvider && matchingProviders.length > 0) {
            resolvedProvider = String(matchingProviders[0].id || '');
          }
        } else if (matchingProviders.length > 0) {
          resolvedProvider = String(matchingProviders[0].id || resolvedProvider);
        }
      } catch (lookupErr) {
        log?.warn?.('[dsh-agent-hub-bridge] llm model resolution fallback:', lookupErr?.message ?? lookupErr);
      }
    }

    if (!resolvedProvider && current?.provider) resolvedProvider = String(current.provider);
    if (!resolvedModel && current?.model) resolvedModel = String(current.model);
    return { provider: resolvedProvider, model: resolvedModel };
  }

  async function applyModelSelection(sessionId, { provider, model, reasoningEffort }) {
    const reqProvider = typeof provider === 'string' ? provider.trim() : '';
    const reqModel = typeof model === 'string' ? model.trim() : '';
    const reqEffort = typeof reasoningEffort === 'string' ? reasoningEffort.trim() : '';
    if (!reqProvider && !reqModel && !reqEffort) return null;

    const requested = {
      ...(reqProvider ? { provider: reqProvider } : {}),
      ...(reqModel ? { model: reqModel } : {}),
      ...(reqEffort ? { reasoningEffort: reqEffort } : {}),
    };

    try {
      const resolved = await resolveModelTarget(sessionId, { provider: reqProvider, model: reqModel });
      if (!resolved.provider || !resolved.model) {
        return {
          applied: false,
          requested,
          warning: 'Unable to resolve provider and model for session.selectModel',
        };
      }
      const selectReq = {
        sessionId,
        provider: resolved.provider,
        model: resolved.model,
        ...(reqEffort ? { reasoningEffort: reqEffort } : {}),
      };
      const res = await invokeGateway('session', 'selectModel', { request: selectReq });
      return {
        applied: true,
        selected: res?.selected ?? selectReq,
      };
    } catch (selectErr) {
      log?.warn?.('[dsh-agent-hub-bridge] session.selectModel non-fatal error:', selectErr?.message ?? selectErr);
      return {
        applied: false,
        requested,
        warning: serializeError(selectErr),
      };
    }
  }

  const server = http.createServer(async (req, res) => {
    try {
      if (!verifyBearer(req.headers.authorization, token)) {
        sendJson(res, 401, { ok: false, error: { code: 'unauthorized', message: 'Invalid or missing bearer token' } });
        return;
      }

      const url = new URL(req.url || '/', `http://127.0.0.1:${port}`);
      const pathname = url.pathname;

      if (req.method === 'GET' && pathname === '/health') {
        sendJson(res, 200, {
          ok: true,
          plugin: name,
          port,
          pid: process.pid,
        });
        return;
      }

      if (req.method === 'GET' && pathname === '/sessions') {
        const data = await invokeGateway('session', 'list', { _request: {} });
        sendJson(res, 200, { ok: true, data });
        return;
      }

      if (req.method === 'POST' && pathname === '/send') {
        const body = await readJsonBody(req);
        const sessionId = typeof body.sessionId === 'string' ? body.sessionId.trim() : '';
        const text = typeof body.text === 'string' ? body.text : '';
        const mode = typeof body.mode === 'string' && body.mode.trim() ? body.mode.trim() : 'queue';
        const model = typeof body.model === 'string' ? body.model.trim() : '';
        const provider = typeof body.provider === 'string' ? body.provider.trim() : '';
        const reasoningEffort = typeof body.reasoningEffort === 'string' ? body.reasoningEffort.trim() : '';
        const rpcId = typeof body.requestId === 'string' && body.requestId.trim()
          ? body.requestId.trim()
          : `agent-hub-${randomUUID()}`;

        if (!sessionId || !text.trim()) {
          sendJson(res, 400, { ok: false, error: { code: 'bad-request', message: 'sessionId and non-empty text are required' } });
          return;
        }

        const modelSelection = await applyModelSelection(sessionId, { provider, model, reasoningEffort });

        if (mode === 'steer') {
          try {
            const agents = ctx.get?.('agents');
            const agent = agents?.get?.(sessionId);
            if (agent?.status === 'running' && typeof agent.inject === 'function') {
              agent.inject(deepFreeze({
                id: randomUUID(),
                role: 'user',
                content: [{ type: 'text', text }],
                source: { kind: 'user', rpcId },
              }));
              sendJson(res, 200, {
                ok: true,
                mode: 'steer',
                sessionId,
                requestId: rpcId,
                ...(modelSelection ? { modelSelection } : {}),
              });
              return;
            }
          } catch (steerErr) {
            log?.warn?.('[dsh-agent-hub-bridge] steer fallback to queue:', steerErr?.message ?? steerErr);
          }
        }

        const result = await invokeGateway('session', 'prompt', {
          request: {
            requestId: rpcId,
            sessionId,
            mode: 'queue',
            content: [{ type: 'text', text }],
            clientTimeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
          },
        });
        sendJson(res, 200, {
          ok: true,
          mode: 'queue',
          sessionId,
          requestId: rpcId,
          ...(modelSelection ? { modelSelection } : {}),
          result,
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/create') {
        const body = await readJsonBody(req);
        const defaultCwd = path.join(os.homedir(), 'Documents', 'deepseek-harness-default-workspace');
        const cwd = typeof body.cwd === 'string' && body.cwd.trim() ? body.cwd.trim() : defaultCwd;
        if (
          process.platform === 'darwin' &&
          (/^\/(?:dshare|flash|gpfs|scratch)\b/i.test(cwd) || /^\/home\/[^/]+/i.test(cwd))
        ) {
          sendJson(res, 400, {
            ok: false,
            error: {
              code: 'remote-path-not-local',
              message: `Refusing to create local macOS DSH workspace at remote Linux path: ${cwd}`,
            },
          });
          return;
        }
        const title = typeof body.title === 'string' ? body.title.trim() : '';
        const rawPreset = typeof body.agentPreset === 'string' ? body.agentPreset.trim() : (typeof body.preset === 'string' ? body.preset.trim() : '');
        const agentPreset = rawPreset || undefined;
        const model = typeof body.model === 'string' ? body.model.trim() : '';
        const provider = typeof body.provider === 'string' ? body.provider.trim() : '';
        const reasoningEffort = typeof body.reasoningEffort === 'string' ? body.reasoningEffort.trim() : '';

        mkdirSync(cwd, { recursive: true });
        const created = await invokeGateway('session', 'create', {
          request: {
            cwd,
            ...(agentPreset ? { agentPreset } : {}),
          },
        });
        const sessionId = created?.sessionId;
        if (typeof sessionId !== 'string' || !sessionId) {
          sendJson(res, 502, {
            ok: false,
            error: { code: 'invalid-create-response', message: 'Host session.create returned no sessionId' },
            created,
          });
          return;
        }

        let renamed = false;
        if (title) {
          try {
            await invokeGateway('session', 'rename', {
              request: { sessionId, title },
            });
            renamed = true;
          } catch (renameErr) {
            log?.warn?.('[dsh-agent-hub-bridge] session.rename non-fatal error:', renameErr?.message ?? renameErr);
          }
        }

        const modelSelection = await applyModelSelection(sessionId, { provider, model, reasoningEffort });

        sendJson(res, 200, {
          ok: true,
          sessionId,
          cwd,
          title,
          renamed,
          ...(agentPreset ? { agentPreset } : {}),
          ...(modelSelection ? { modelSelection } : {}),
          created,
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/select-model') {
        const body = await readJsonBody(req);
        const sessionId = typeof body.sessionId === 'string' ? body.sessionId.trim() : '';
        const model = typeof body.model === 'string' ? body.model.trim() : '';
        const provider = typeof body.provider === 'string' ? body.provider.trim() : '';
        const reasoningEffort = typeof body.reasoningEffort === 'string' ? body.reasoningEffort.trim() : '';
        if (!sessionId || (!model && !provider && !reasoningEffort)) {
          sendJson(res, 400, {
            ok: false,
            error: { code: 'bad-request', message: 'sessionId and at least one of model/provider/reasoningEffort are required' },
          });
          return;
        }
        const modelSelection = await applyModelSelection(sessionId, { provider, model, reasoningEffort });
        sendJson(res, 200, { ok: true, sessionId, modelSelection });
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
        if (!sessionId || !decision) {
          sendJson(res, 400, {
            ok: false,
            error: { code: 'bad-request', message: 'sessionId and valid decision (yes/no) are required' },
          });
          return;
        }

        const agents = ctx.get?.('agents');
        const agent = agents?.get?.(sessionId);
        const approvedBool = decision === 'yes';

        if (agent && typeof agent.respondApproval === 'function') {
          const result = await agent.respondApproval({ approved: approvedBool, decision, feedback });
          sendJson(res, 200, { ok: true, action: 'respondApproval', method: 'agent.respondApproval', sessionId, decision, result });
          return;
        }
        if (agent && typeof agent.resolveApproval === 'function') {
          const result = await agent.resolveApproval(approvedBool, feedback);
          sendJson(res, 200, { ok: true, action: 'respondApproval', method: 'agent.resolveApproval', sessionId, decision, result });
          return;
        }

        try {
          const result = await invokeGateway('session', 'respondApproval', {
            request: { sessionId, approved: approvedBool, decision, ...(feedback ? { feedback } : {}) },
          });
          sendJson(res, 200, { ok: true, action: 'respondApproval', method: 'gateway.session.respondApproval', sessionId, decision, result });
          return;
        } catch (gwErr) {
          log?.warn?.('[dsh-agent-hub-bridge] session.respondApproval unavailable:', gwErr?.message ?? gwErr);
        }

        sendJson(res, 409, {
          ok: false,
          error: {
            code: 'approval-hook-unavailable',
            message:
              'DSH Desktop plugin host did not expose an active in-process approval responder for this session; ' +
              'please confirm in DSH Desktop UI or reply via queue prompt if it is a user question.',
          },
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/cancel') {
        const body = await readJsonBody(req);
        const sessionId = typeof body.sessionId === 'string' ? body.sessionId.trim() : '';
        if (!sessionId) {
          sendJson(res, 400, { ok: false, error: { code: 'bad-request', message: 'sessionId is required' } });
          return;
        }
        const result = await invokeGateway('session', 'cancel', {
          request: { sessionId },
        });
        sendJson(res, 200, { ok: true, sessionId, result });
        return;
      }

      sendJson(res, 404, { ok: false, error: { code: 'not-found', message: `Unknown route ${req.method} ${pathname}` } });
    } catch (error) {
      const status = Number.isInteger(error?.status) ? error.status : 500;
      sendJson(res, status, { ok: false, error: serializeError(error) });
    }
  });

  server.on('error', (err) => {
    log?.error?.(`[dsh-agent-hub-bridge] HTTP server error on 127.0.0.1:${port}:`, err?.message ?? err);
  });

  server.listen(port, '127.0.0.1', () => {
    log?.info?.(`[dsh-agent-hub-bridge] listening on http://127.0.0.1:${port}`);
  });

  ctx.effect?.(() => () => {
    try {
      server.closeAllConnections?.();
      server.close();
    } catch {
      // Ignore close errors during disposal.
    }
    try {
      peer?.dispose?.();
    } catch {
      // Ignore peer disposal errors.
    }
  }, 'dsh-agent-hub-bridge: http server');
}
