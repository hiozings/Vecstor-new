# Interactive SQL Shell Implementation Plan

> **For agentic workers:** 实施时使用 superpowers:executing-plans 与 test-driven-development。用户已批准实施；本文件记录任务与验收。

**Goal:** 用户在终端内通过多行 SQL 和界面命令完成选库、导入、持久 CRUD、普通与向量查询。

**Architecture:** prompt_toolkit/Rich 客户端通过计算服务访问存储端托管 DuckDB；语句级事务、持久操作 ID 和跨 RPC 读租约保证一致性；P2 兼容接口保持可用。

**Tech Stack:** 现有 DuckDB、SQLGlot、gRPC、NumPy、Conda；拟增加 prompt_toolkit/Rich 与项目 console-script。

**Spec:** `docs/design/interactive-sql-shell.md`。

## Global Constraints

- 本计划属于 P2.5；完成本阶段不标记 P3/P4 完成。
- 终端和计算服务不直接打开存储数据库文件，所有访问走 RPC。
- 回车续行，词法状态之外的分号才结束语句；AST 仍交给 SQLGlot。
- 每请求携带 database_id；不得使用影响所有会话的全局 current_database。
- 单语句事务和同事务操作日志；写入超时不得假定回滚或自动以新 ID 重发。
- 多 RPC 向量查询使用同一 read_token，旧查询可完成，新查询读到已提交写入。
- 不变更生成代码而不更新 proto；保留 P1/P2 基线，新增 v2 不改变旧字段语义。
- 安装依赖前检查已有环境，经 Conda 锁定；按 AGENTS 约束取得 Git 操作许可并在新分支开发。

## Review Focus

- 分号在字符串、嵌套注释、dollar-quoted 文本中，或同次粘贴包含多句：不误执行、不漏句。
- 两客户端切库与查询交错：不跨库读取、写入或污染补全缓存。
- 写入成功但响应丢失：持久 operation_id 防止重复插入，结果未知可查询。
- 扫描与向量读取之间发生 UPDATE/DELETE：同一读事务不能混用版本；租约释放不泄漏。
- 通用 SQL 入口包含嵌套外部函数、额外语句或无效向量：拒绝，失败后数据库和 revision 不变。

## Task 1 语义契约与协议

文件：修改 `proto/vecstor/rpc/services.proto` 或新增同包 v2 proto；新增 `src/vecstor/query/statements.py`、`tests/unit/test_statements.py`、协议测试。

接口：`classify_statement(sql: str) -> StatementPlan`；计划包含 statement_kind、目标表、受控 AST、查询类型。v2 消息确定 DatabaseHandle、ReadLease、OperationStatus、Column、Cell、ResultBatch。

- [x] 为 DDL/DML/普通 SELECT/向量 SELECT/库命令编写支持与拒绝用例，先运行失败测试。
- [x] 固定 SQL 表格中列出的范围，支持内联数组及距离别名；P1 旧解析器保留兼容，不直接放宽全部 AST。
- [x] 编译协议并测试 int64、NULL、向量数组、影响行数和结果结束帧，拒绝不支持类型。
- [x] 验证：`python -m pytest tests/unit/test_statements.py tests/unit/test_sql_protocol.py -q` 全通过。

## Task 2 数据库目录与持久读写

文件：新增 `src/vecstor/storage/catalog.py`、`database.py`、`operations.py`；修改服务启动入口；新增对应单元/集成测试。

接口：`DatabaseCatalog(data_root)`、`list_databases()`、`open_database(name, create=False)`；`execute_statement(database_id, operation_id, plan)` 在单事务中写入业务数据、revision 和请求结果。

- [x] 先测试空目录启动、逻辑名称选择、目录越界拒绝、选库失败保持原库、不同库同名表、重启持久化。
- [x] 建立每库执行队列和连接生命周期；存储启动不要求 --dataset，兼容旧只读启动模式。
- [x] 实现 CREATE/DROP TABLE、INSERT VALUES、UPDATE、DELETE、普通 SELECT；普通表基础类型与向量表约束分别校验。
- [x] 验证失败写入回滚、重复 ID、维度错误、NaN/Inf、整型精度、零影响行数、相同 operation_id 幂等及不同请求摘要拒绝。
- [x] 验证：重启数据库后数据、revision 和已提交操作状态保留。失败/未知状态的解释与文档一致。

## Task 3 RPC 会话路由与查询快照

文件：新增 `src/vecstor/storage/read_leases.py`；修改 `compute/engine.py`、`compute/service.py`、`rpc/client.py`；新增数据库/操作/SQL v2 服务适配。

接口：`begin_read(database_id, table_id, deadline) -> ReadLease`、`scan_metadata(read_token, ...)`、`fetch_vectors(read_token, ids)`、`end_read(read_token)`。客户端选择库后显式传 database_id，不依赖全局状态。

- [x] 先测试两个客户端选不同库、同时查询/写入、读到一半发生更新、token 超时/取消、服务重启失效。
- [x] 普通 SQL 路由到存储端；向量 SQL 固定读事务并沿用计算侧精确 Top-K；支持自定义向量表名及距离投影。
- [x] 新增通用结果流及 GetOperationStatus；整个语句传播总 deadline，保留背压下的向量读取执行容量。
- [x] 验证旧读保持旧结果，写提交后新读获得新结果；回滚不改变可见内容；结果流中断不显示“完整成功”。

## Task 4 数据选择与导入

文件：新增 `src/vecstor/storage/imports.py`；扩展目录 RPC；新增 `tests/integration/test_database_import.py`。

接口：`list_datasets() -> source descriptors`、`import_npz(database_id, source_id, table, operation_id)`。仅处理 data-root/imports 内托管源，保留原文件。

- [x] 先测试列出 NPZ、正确导入、坏清单、维度错误、重复目标表、导入中断和 operation_id 重试。
- [x] 在一个事务中导入完整元数据与向量，失败不留下半张表；数据库切换不需要重启服务。
- [x] 验证导入后的向量查询与原 NPZ 的 P1 结果一致。

## Task 5 终端与分句器

文件：新增 `src/vecstor/shell/{app,buffer,commands,render,completion}.py`；修改 `pyproject.toml`、`cli.py`、环境声明/锁定；新增单元与 PTY 测试。

接口：`StatementBuffer.feed(text: str) -> list[str]` 与 pending/reset；`run_shell(client)`；界面保存当前 DatabaseHandle。`vecstor` 默认进入 shell，`vecstor serve` 管理服务入口，旧脚本继续工作。

- [x] 先测试换行与分句状态表：引号转义、注释嵌套、dollar quote、多句粘贴、纯注释、错误后未执行文本保留。
- [x] 使用 Conda 检查并安装 prompt_toolkit/Rich，锁定版本；配置 console-script，仅安装项目入口，不覆盖现有依赖。
- [x] 实现提示符、帮助/选库/导入命令、补全、历史、表格/JSON、耗时和影响行数；输入中断、读取取消和写入未知状态分开处理。
- [x] 使用真实 PTY 模拟 Enter、Ctrl-C、Ctrl-D、中文和多行粘贴，验证不会提前执行 SQL，且报错后可继续使用。

## Task 6 全链路验收与文档

文件：新增 `scripts/check_shell.py`、`tests/integration/test_shell_workflow.py` 和验收记录；更新 README、AGENTS、CHANGELOG。

- [x] 终端执行示例全流程，包含两数据库、多表、CRUD、普通查询、内联向量、导入、退出和重连。
- [x] 并发写入/读取、服务重启、响应丢失与重复请求、租约到期和取消测试通过。
- [x] 在 Landlock 下禁止计算访问整个 data-root，仍可选库、CRUD 和查询；停止存储后所有操作明确失败。
- [x] 运行全套 P1/P2/P2.5 回归，独立代码审查并修复；生成协议与源文件一致；更新操作说明与测试证据。

## 建议实施顺序和工作量

Task 1 → 2 → 3 → 4 → 5 → 6。终端静态原型与分句测试可在协议明确后提前进行，但不能把界面演示当作后端 CRUD 已完成。

按现有代码规模估计约 8–12 个开发日，主要不确定性为事务快照、写入结果确认和协议兼容；这不是已测得的工时承诺。建议先交付“可选库的只读终端”，再交付“持久 CRUD＋一致性”，最终统一验收。P3 开始时间据实际进度调整，P4 的重复工作由本阶段复用。

## 完成记录 2026-10-12

- Git 预检：P2 分支工作区含前阶段实现和规划；159 项基线复验通过。按用户授权提交为 ee60ad9，创建 p25-interactive-sql；不推送或合并。
- Task 1 complete：分类器 87 项测试、类型化协议测试通过；先观察未实现失败再实现。旧解析器与 v1 消息保留。
- Task 2 complete：持久 CRUD、双库、重启、成功/失败操作摘要、回滚、读租约与取消测试通过。
- Task 3 complete：真实 v2 RPC 的普通/向量查询、大整数/NULL、同序结果、错误和会话隔离通过。
- Task 4 complete：托管导入、重复目标拒绝、源移走后的幂等重试，以及 5 组导入向量与 P1 对照通过。
- Task 5 complete：console-script、31 项终端模块测试（含真实 PTY）通过，依赖按 Conda 锁定。
- Task 6 complete：全套 319 passed；10000 行真实 PTY 终端验收通过，重启持久化、Landlock 文件拒绝和停存储 UNAVAILABLE 均有证据。
- 独立审查问题已修复：运行中 SQL 超时未中断、失败操作 ID 可重绑、结果流未查行宽、切库/列名补全缓存；回归先失败再通过。额外修复导入重试依赖原文件问题。
- Ruling: 事务/租约/操作日志合并到 database.py，目录/源导入合并到 catalog.py，不创建空 operations/read_leases/imports 模块 — 保持当前单线程队列的清晰所有权 — 后续复杂度增长时可再拆分。
- Ruling: 建库按名字幂等复用，操作结果分别通过 .open/USE 选择 — 应对建库响应丢失且不覆盖文件 — 属于 VecStor 数据库命令语义，非完整 DuckDB CLI 兼容。
- Ruling: 普通结果物化后分批传输，并在 fetchmany 检查取消/期限 — 控制原型实现复杂度 — 大结果仍占存储内存，需后续游标优化。
- 开发验收时：本阶段改动保留在功能分支工作区，上一阶段提交已完成。随后用户明确要求合并 main 并推送，按新的授权执行 Git 集成。
- 详细证据：docs/validation/p25-sql-shell.md。
