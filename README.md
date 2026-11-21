# Multimodal-DAG Orchestrator v0.8

多模型分层 DAG 并发协同调度总台（支持 **单 Key 极简冷启动降级、高并发信号量限流与熔断、跨模态全局风格锚点、HITL 人工审批门禁、语义缓存、流式落盘与零 OOM ZIP 打包导出**）。

## 核心特性（V2 → v0.8 演进）

1. **单 Key 30 秒极简冷启动 (`quickstart.py` & `config/quickstart.minimal.yaml`)**
   - 启动时并发探活所有已配置 Provider，缺失多模态/专用模型 Key 时自动按能力降级链回退至主 LLM 节点，绝不中断流水线。
   - 内置启动环境预检（`PreflightChecker`），自动扫描 `ffmpeg`、`npx` 与可用磁盘空间并给出降级提示。
2. **高并发限流治理与连接池复用 (`core/connectors.py` & `PooledMultiProtocolConnector`)**
   - 全局 + 单 Provider 双层异步信号量（Semaphore）控制，配合 Full Jitter 指数退避与节点级熔断器（Circuit Breaker），有效抵御 `429 Rate Limit` 雪崩。
3. **上下文瘦身与 Output Schema 自愈 (`SubtaskContract`)**
   - 上游节点产物自动提炼为 `<=200` 字摘要 + 结构化契约字段注入下游，避免多层并行上下文指数膨胀；内置 JSON 提取与非结构化文本自愈包装。
4. **局部重跑级联同步与 SSE 断点续接 (`server/app.py`)**
   - 支持 `auto`（自动标脏并级联重跑下游）、`mark_dirty`、`isolated` 三种局部重跑策略。
   - SSE 事件流绑定递增 `seq_id`，前端刷新后携带 `last_seq_id` 自动回放历史并接管实时进度。
5. **生产级安全与治理闭环（v0.4 → v0.8 15 项深度修缮）**
   - **预算硬熔断 (`BudgetGuard`)**：项目级 Token 计费与费用上限拦截。
   - **Kahn 拓扑判环 (`validate_acyclic`) & 崩溃恢复 (`recover_from_crash`)**：杜绝动态子图死循环与进程重启后的僵尸节点。
   - **流式落盘 (`DiskBackedArtifactStore`) & 分块 ZIP 导出**：大体积多模态文件落盘引用 + 写锁防覆盖 + 72h 自动 GC，内存峰值恒定低位。
   - **MCP 安全沙箱 (`MCPSandbox`) & Key 脱敏 (`SecretSanitizer`)**：防止路径穿越、危险命令注入及配置日志明文泄露。
   - **跨模态全局锚点 (`GlobalStyleBible`) & 语义缓存 (`SemanticDAGCache`) & 关键路径调度**：统一音画时长/视觉规范，缓存未改动子任务，优先调度主干节点。
   - **HITL 人工审批门禁 & 零编译可视化工作台 (`frontend/index.html`)**：支持高成本节点暂停审批修改 Prompt、关键路径金色高亮与 Trace 耗时/缓存瀑布图。

## 快速开始 (Quick Start)

```bash
# 1. 安装依赖
pip install -e .

# 2. 配置至少一个 API Key
cp .env.example .env
export PRIMARY_API_KEY="sk-your-key"

# 3. 一键启动控制面与可视化工作台
python quickstart.py
```

启动后浏览器将自动打开 `http://127.0.0.1:8088`。
