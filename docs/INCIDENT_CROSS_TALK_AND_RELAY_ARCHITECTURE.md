# Agent Hub 跨端/跨会话“串台”事故复盘与自动消息中继（Relay）架构重构需求

**日期**：2026-11-25  
**触发人**：the maintainer  
**报告与对接方**：Hermes -> Antigravity (Agent Hub MCP)  
**涉及端**：ChatGPT Desktop (Electron/CDP on 9222), VS Code Cline (43918), DSH (43917), Antigravity (9224)

---

## 一、事故背景与现场还原

在 2026-11-25 上午的多任务并发运行中：
1. **多任务并行场景**：
   - **Task 1 (Analytics Project A)**：VS Code Cline 在 the HPC cluster上完成了 Stage 3 验收（12/12 交叉校验全部 PASS，产出 `P1Z_STAGE3_FINAL_ACCEPTANCE_CN.md`），需要将报告汇总注入 ChatGPT 会话 `Analytics Project A`（`aaaaaaaa-1111-4111-8111-111111111111`），并接收 ChatGPT 终审意见反哺回 Cline。
   - **Task 2 (Class B 空间预测)**：DSH 与 ChatGPT 会话「Branch · Class B」（`bbbbbbbb-2222-4222-8222-222222222222`）进行学术指导研讨，Slurm 全量作业 `<slurm-job-id>` 在 the HPC cluster运行。
   - **Task 3 (Work Pipeline 6-arm)**：Codex 会话 `eeeeeeee-5555-4555-8555-555555555555` 在集群上推进 C/B′/B″ Duplex 恢复作业。

2. **严重“串台”事故现场**：
   - 外部临时编写的后台守护脚本（`relay_supervisor.py`）试图将 Cline 的 PRJ_A 报告发送给 ChatGPT 桌面端。
   - 但当时 ChatGPT 桌面端（单一 Electron 窗口）刚刚被用于Class B研讨，停留在「Branch · Class B」页面。
   - 守护脚本在没有严密校验当前页面会话 ID 的情况下，直接抓取 ProseMirror 输入框注入了 PRJ_A 的长篇对账报告并触发了发送！
   - 随后，脚本误将Class B中生成的文字当成 PRJ_A 的裁决内容读取并向 Cline 注入，甚至引发了重复状态判定的连环微信通知。
   - **the maintainer严正指出**：
     > “你怎么把PRJ_A发到分支Class B里面了，串台了！你忽略重新回到正轨运行就好，这个错误发给anti的agenthub让它帮你调整一下，找一个合适的架构，自动来回发消息还是需要继续做的”

---

## 二、当前单 Electron 窗口架构的核心缺陷分析

1. **单实例 Electron 窗口与多会话路由抢占（Single Window Resource Contention）**：
   - ChatGPT Desktop（Aperitif）默认只运行一个主窗口实例（监听 9222 端口）。
   - 页面内部使用 React Router 管理会话（URL 保持在 `app://-/index.html`，内部路由通过 `/c/<conversation_id>` 渲染）。
   - 当多个后台 Agent 任务并发访问不同会话时，它们在争夺同一个 DOM 树和唯一的全局 Composer（输入框）。任何一方切换路由，都会破坏另一方的上下文。

2. **外部独立守护进程各自为政，缺乏全局调度锁（No Centralized Concurrency Control）**：
   - 如果由业务端自己写脚本连接 9222 端口直接 `eval` DOM，完全绕过了 Agent Hub 的安全防护层与审计锁，造成裸写竞争。
   - 即使 Agent Hub 的 `CdpWriter` 实现了 `SessionMismatchError`，但如果外部脚本不走 Agent Hub，该保护便形同虚设；且即使走 Agent Hub，单窗口也无法同时为两个会话提供写服务。

3. **DOM 异步导航与状态机延迟（Asynchronous Navigation & State Lag）**：
   - 通过调用 React Fiber 的 `memoizedProps.navigate('/c/<id>')` 切换会话时，React 渲染组件树、挂载虚拟滚动（Virtualized turns）需要 100~500ms 的时间。
   - 若在此期间立即读取或写入，极易读到旧会话残留的 DOM 节点或向旧 Composer 派发事件。

4. **哈希防抖与消息去重不足（Debounce & Deduplication Fragility）**：
   - 简单的文本截断比对或单次哈希比对在遇到富文本格式转换、流式生成未结束等情况时，会导致状态机误判，引发循环交接和通知轰炸。

---

## 三、the maintainer提出的核心目标与重构需求

1. **“自动来回发消息还是需要继续做的”**：
   - 跨 Agent 双向闭环（如 Cline ⇄ ChatGPT, DSH ⇄ Codex）是核心刚需，严禁因噎废食全面退回纯人工手动。
   - 必须在底层架构上彻底解决“串台”隐患，实现稳定可靠的自动化双向流动。

2. **架构重构核心要求**：
   - **安全红线**：绝对杜绝跨会话误投（Zero Cross-Talk Tolerance）。宁可发送失败报错，绝不允许发到非目标会话。
   - **中心化调度**：由 Agent Hub 统一提供常驻的 Message Relay 机制，而不是由业务层各自挂散装脚本。
   - **严密事务契约（Two-Phase Commit / Session Locking）**：所有跨端投递必须包含“会话锁定 -> 路由切换 -> 严格二次核验 -> 文本注入 -> 发送确认 -> 释放锁”的完整生命周期。

---

## 四、建议 Antigravity 设计并落地的重构方案

### 方案 A：Agent Hub 内置集中式 Relay 服务与全局会话互斥锁（推荐首选）

在 `agent-hub-mcp` 体系内新增 `hub_relay_service` 或在 `server.py` / `cdp_writer.py` 扩充：

1. **App 级全局操作互斥锁（App-Level Write Mutex）**：
   - 针对 `codex` / `chatgpt`（9222）和 `antigravity`（9224）这类单窗口 Electron 客户端，维护严格的并发互斥队列。
   - 同一时刻只允许一个发信事务持有该 App 的操作锁。

2. **会话切换与强一致性验证协议（Strict Route Verification Protocol）**：
   ```python
   def atomic_send_to_electron_session(app, target_session_id, message_text):
       with app_lock:
           # 1. 检查当前活跃 session_id（从 React Fiber router 或 DOM 属性获取）
           active_sid = get_active_session_id_from_dom()
           
           # 2. 如果不匹配，执行 Fiber navigate
           if active_sid != target_session_id:
               execute_react_navigate(f"/c/{target_session_id}")
               # 轮询等待路由稳定（最多 5s）
               wait_until_session_matches(target_session_id, timeout=5.0)
           
           # 3. 最终门禁强校验（Gate Check）
           current_sid, current_title = get_verified_dom_identity()
           if current_sid != target_session_id:
               raise FatalSessionMismatchError(
                   f"ABORTED: Refusing to send! Target was {target_session_id}, but UI is {current_sid} ({current_title})"
               )
           
           # 4. 检查输入框无未保存草稿
           assert_composer_clean()
           
           # 5. 注入文本并派发发送
           trigger_prosemirror_send(message_text)
           
           # 6. 验证新消息已作为 user-turn 挂载上屏
           verify_message_appeared(message_text)
   ```

3. **内置双向中继守护模块（Agent-Hub Native Bi-Directional Relay）**：
   - 统一接口：例如 `hub_relay_start(source={"app": "cline", "id": "..."}, target={"app": "codex", "id": "..."}, policy={...})`。
   - 内置轮询与 Watcher 触发，当 Source 产出特定交付物/报告时，由 Agent Hub 安全原子化地注入 Target；当 Target 回复完成时，提取裁决反哺 Source。
   - 所有状态保存在 `~/.hermes/agent-hub/relay_states.sqlite`，自带幂等事务 ID，杜绝重复交接与通知抖动。

### 方案 B：探索多窗口 / 无头（Headless）会话隔离

- 如果 Electron 支持多窗口（如 `target.createTarget({url: ...})` 打开独立 Tab/Window），使 PRJ_A、Class B、Work Pipeline 各自拥有独立的 CDP Page Target，彻底消除共享同一个页面的争抢。
- 若无法多窗口，则方案 A 的全局排队与严格门禁是最佳工程解。

---

## 五、验收标准

1. **防串台冒烟测试**：
   - 故意将 ChatGPT 窗口切至「Branch · Class B」，调用接口向 `Analytics Project A` 发送消息；
   - 必须观察到系统自动平滑切换至 PRJ_A 会话，验证成功后再注入，若切换失败必须立即抛错阻断，Class B中严禁出现任何误发字符。
2. **并发竞争测试**：
   - 两个任务同时尝试向两个不同会话发送消息，排队机制必须保证前一个会话发送完毕且页面完全稳定后，才切换并发送后一个，互不污染。
3. **闭环中继稳定性**：
   - 自动来回流转过程静默稳定，微信仅在重要节点（如验收通过/驳回）推送一次格式化卡片，无高频重复刷屏。
