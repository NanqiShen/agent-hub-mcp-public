# Agent Hub MCP —— 项目内容总结（中文）

> 生成于 2026-11-25。此文档概括仓库里都有什么、怎么运转、怎么验证。
> 仓库地址：`github.com/NanqiShen/agent-hub-mcp`（私有）。

## 一句话定位

**Agent Hub 是一个本地 MCP 服务器 + 原生桥接层 + 零 token 守护进程**，让一个中枢 Agent（Hermes）可以直接读取、检索、写入、跨端转发、盯盘四个桌面 AI 应用的真实会话：**ChatGPT Desktop（Codex）、DeepSeek Harness（DSH）、Antigravity、VS Code Cline**。

## 解决什么问题

多桌面 Agent 并行干活时，人要在四个窗口之间手动复制粘贴。Agent Hub 把「读会话、发消息、批权限、跨端交卷、状态盯盘」全部做成工具与后台循环，中枢 Agent 可以：

- 查看任意应用任意会话的完整对话与状态（running / idle / waiting_approval / error / stalled）
- 把一条消息注入指定会话（带路由核验与防串台机制）
- 把 A 应用的结论**逐字**转给 B 应用，并自动往返（双向中继）
- 监听状态跃迁并用手机推送汇报（零 token 定时巡检）

## 架构总览

```
Hermes (MCP client, stdio)
        │
        ▼
agent_hub/server.py  ── 24 个 MCP 工具（读/写/中继/盯盘/审批/审计）
        │
        ├── adapters/          只读层（SQLite / JSONL / FTS）
        │     codex.py dsh.py antigravity.py cline.py
        │
        ├── writers/           写入层（受控、可核验）
        │     cdp_writer.py    Codex / Antigravity（Electron CDP 9222/9224）
        │     dsh_writer.py    DSH（Cordis 插件 HTTP 127.0.0.1:43917）
        │     cline_writer.py  Cline（VS Code 扩展 127.0.0.1:43918 + WS 兜底）
        │
        ├── relay.py           双向中继循环（SQLite 事务、防回声、里程碑告警）
        ├── watcher.py         零 token 守护进程（LaunchAgent，25s 巡检）
        ├── summary.py         会话完成摘要卡片
        ├── bindings.py        任务别名绑定
        └── audit.py           全部发送/中继/审批动作审计
```

## 四大应用的读/写通道

| 应用 | 读取通道 | 写入通道 |
|---|---|---|
| **ChatGPT Desktop (codex)** | `~/.codex/state_5.sqlite`、`thread_history_1.sqlite`、`goals_1.sqlite`；CDP 9222 实时读取（React Fiber 优先） | CDP 注入 + 两阶段路由核验（Electron 9222） |
| **DSH Desktop** | `session_projcache`、`session.v4.jsonl.zstd` 全文检索 | Cordis 插件 `dsh-agent-hub-bridge`（带令牌的 127.0.0.1:43917） |
| **Antigravity** | `conversation_summaries.db`、FTS、`transcript*.jsonl` | CDP 注入（Electron 9224），同 Codex 防护 |
| **VS Code Cline** | `~/.cline/data/db/sessions.db`、任务 `messages.json`、VS Code globalStorage | 自研 VS Code 扩展 `agent-hub-cline-bridge`（127.0.0.1:43918），Cline Hub WS 兜底 |

## 24 个 MCP 工具（分组）

1. **读 / 摘要 / 绑定（9）**：`hub_overview`、`hub_list_sessions`、`hub_search_sessions`（跨 4 应用全文）、`hub_read_session`、`hub_summarize_session`（完成摘要卡）、`hub_handover_session`（无损跨端交接包）、`hub_bind_task` / `hub_list_bindings` / `hub_unbind_task`（任务别名）
2. **写 / 审批 / 单次转发（7）**：`hub_writer_status`、`hub_send_message`（60s 重复保护、干跑、审计）、`hub_relay_message`（默认 dry-run）、`hub_create_session`、`hub_respond_approval`、`hub_cancel_session`、`hub_audit_recent`
3. **双向中继循环（4）**：`hub_relay_start`（注册链接，防抖 20s、最小间隔 15s、最大跳数 20、里程碑推送）、`hub_relay_list`、`hub_relay_stop`、`hub_relay_tick`（即时评估，可 dry-run）
4. **零 token 守护（4）**：`hub_watch_add`、`hub_watch_list`、`hub_watch_remove`、`hub_watch_check_now`
5. 另兼容 MCP prompts / resources 接口（`list_prompts`、`read_resource` 等）

## 防串台与安全设计（核心卖点）

- **跨进程 app 互斥锁**：同一桌面应用的写操作串行，杜绝两个进程同时操控一个窗口。
- **两阶段会话路由核验**：发送前按会话 ID/标题双重确认路由身份，注入后二次核验页面身份与消息落地（`verified_session_id`、`message_appeared`、`composer_cleared`）。
- **React Fiber 实时读取（新）**：ChatGPT Desktop 的 DOM 只渲染片段（实测 5669 chars vs 完整 13820 chars）。改为遍历 React Fiber，按 `contentReferences → model_dil_v2.fallbackMarkdown / matched_text` 拼出完整正文，DOM 仅作兜底；并清洗 PUA widget 标记（`U+E200…U+E201`）。
- **conversationId 严格匹配（新）**：切换会话后树里可能还挂着上一个会话的 turns；读取时要求 `turns[0].conversationId == 目标会话`，不匹配则短轮询等渲染，绝不回退到旧会话内容。
- **切窗自动回切（新）**：任何临时切窗读取，`finally` 里切回操作前所处会话，用户桌面不被打扰。
- **Cline 收尾退出纠偏（新）**：Cline 回合完成后进程收尾 `exit_code=1` 曾被误判 error；现在检查末条为非 `partial` 的助手消息，判定为 `idle · 回合已结束`，不阻断中继。
- **令牌与凭据**：桥接令牌只存本地文件、不入库不入日志；仓库无任何真实密钥（已扫描）。

## 后台守护（零 token）

- `ai.hermes.agent-hub-watcher`（LaunchAgent，25s 间隔）：轮询 watches.json 中登记的会话，状态跃迁（completed / waiting_approval / waiting_user / error / stalled）时推送；可选 Level-2 AI 核验（默认 `deepseek-v4.1-flash` + `-t terminal` + 90s 超时，实测 9.8s，替代曾超时 55s 的默认模型组合）。
- **双向中继**：watcher 每 tick 顺带执行 `RelayBroker`，按 SQLite 状态机去重、防回声、限速；里程碑命中（裁决/通过/驳回/结题等）时推微信。

## 数据与状态文件

- `~/.hermes/agent-hub/watches.json` — 盯盘登记（list）
- `~/.hermes/agent-hub/relay_states.sqlite` — 中继链接（`relay_links`）与事务（`relay_transactions`）
- `~/.hermes/agent-hub/bindings.json` — 任务别名
- `~/.hermes/agent-hub/send_audit.jsonl` / `audit.jsonl` — 动作审计

## 测试与验证

- 单元测试：`python -m unittest discover -s tests -v`（当前 **47 项全绿**）
- 写入通道自检：`python scripts/verify_writers.py --status`
- 实测覆盖：CDP 切窗 → Fiber 全文读取 → conversationId 隔离 → 自动回切；中继 dry-run 基线；Cline 收尾退出 → idle。

## 目录结构

```
agent-hub-mcp/
├── agent_hub/           # 核心包（server/adapters/writers/relay/watcher…）
├── tests/               # 47 个单元测试
├── dsh_plugin/          # DSH Cordis 桥插件（index.mjs）
├── vscode_cline_bridge/ # Cline VS Code 扩展源码 + 打包 .vsix
├── scripts/             # verify_writers.py 等
├── docs/                # 架构/事故复盘文挡
└── README.md            # 英文主文档（含全部 24 个工具签名）
```

## 快速上手

```bash
# 1. 依赖：Python 3.12+（本机用 3.14），四个桌面应用
# 2. 启动 MCP 服务（stdio，由 MCP 客户端拉起）
PYTHONPATH=/path/to/agent-hub-mcp python -m agent_hub.server
# 3. 安装 Cline 桥扩展（可选写入能力）
code --install-extension vscode_cline_bridge/agent-hub-cline-bridge-0.1.0.vsix
# 4. 安装 DSH 插件（可选写入能力）
#    将 dsh_plugin/ 放入 DSH Cordis 插件目录并启用 dsh-agent-hub-bridge
# 5. 启动守护进程
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/ai.hermes.agent-hub-watcher.plist
```

## 已知限制

- 依赖各桌面应用的数据布局（升级可能改 schema，需适配层跟进）。
- CDP 写入要求应用以调试端口启动（提供 `launch-codex-cdp.sh` / `launch-antigravity-cdp.sh`）。
- 中继是「轮到它说话」的自动往返，不做语义裁判；跳数上限防死循环。
- 平台差异：微信推送依赖 Hermes 网关的 context_token，过期需用户发条消息刷新。
