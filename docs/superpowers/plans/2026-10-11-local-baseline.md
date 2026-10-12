# P1 Local Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 实现可重复生成数据、严格 SQL 校验、精确混合查询与独立真值验证。

**Architecture:** 本地 Dataset 持有只读数组副本，调用者不得主动解除只读标志；SQLGlot 解析受限语法，DuckDB 执行参数化元数据过滤；NumPy 执行分块平方 L2 与确定性 Top-K。CLI 暴露生成、查询与验收演示。

**Tech Stack:** 现有 Conda vecstor 环境，不新增依赖。

**Spec:** `docs/design/data-and-query-spec.md`。

## Global Constraints

- float32 向量输入，float64 距离累加，按 `(distance, id)` 升序。
- 不直接执行用户原始 SQL；不读取外部数据库或文件函数。
- 当前仅 P1 本地基线，不实现 P2 网络协议和 P3 ANN。
- 不覆盖已有生成文件，不修改 Conda 环境和原始申报材料。

## Review Focus

- AST 中未支持的子句被静默丢弃：在解析测试中拒绝额外子句、嵌套函数与多语句。
- K 边界并列与非连续 ID：在精确执行测试中用手工数据验证。
- float32 转换溢出、整型截断：在 Dataset 和查询输入测试中拒绝。
- 生成与加载不一致：测试空数据、清单错误、非 pickle 数组和文件往返。
- 引擎与真值共享代码导致漏检：演示独立布尔过滤与全矩阵排序，另加手算预期。

## Task 1 数据模型和可重复数据

Files: `src/vecstor/data.py`, `src/vecstor/datasets.py`, `tests/unit/test_data.py`。
Interfaces: `Dataset(ids, categories, timestamps, values, vectors)`；`generate_dataset(rows=10000, dimension=128, seed=20261011, distribution='independent') -> Dataset`；`save_dataset(dataset, path, metadata=None)` / `load_dataset(path) -> Dataset`。

- [x] 先写校验、生成、文件往返测试；运行 `conda run -n vecstor python -m pytest tests/unit/test_data.py -q` 观察未实现失败。
- [x] 实现数据校验、生成及快照；同一命令应全部 PASS。

## Task 2 查询解析和安全过滤

Files: `src/vecstor/query/parser.py`, `tests/unit/test_query.py`。
Interfaces: `parse_query(sql) -> QueryPlan`，含 projection、limit、filter_sql、filter_params，过滤 SQL 固定选出内部行号和元数据。

- [x] 先写合法查询和不支持语法/类型/注入测试，运行对应 pytest 观察失败。
- [x] 实现 AST 白名单、类型校验和字面量绑定，验证测试通过。

## Task 3 精确执行器

Files: `src/vecstor/query/engine.py`, `tests/unit/test_engine.py`。
Interfaces: `LocalEngine(dataset, block_size=4096)` 支持 context manager；`execute(sql, query_vector) -> QueryResult`，`QueryResult.to_dict()` 可 JSON 序列化。

- [x] 手工 fixture 验证条件优先级、精确距离、并列、空表、少于 K、非法向量；运行测试观察失败。
- [x] 实现 DuckDB 元数据过滤与 NumPy 块式精确 Top-K，运行全部测试应 PASS。

## Task 4 CLI 和独立真值验收

Files: `src/vecstor/cli.py`, `src/vecstor/evaluation.py`, `scripts/vecstor.py`, `tests/integration/test_cli.py`。
Interfaces: CLI `generate --output`、`query --dataset --sql --vector`、`demo --output-dir`；错误退出码 2；默认演示 10000 行、128 维。

- [x] 先写 subprocess 测试验证产物、查询结果与失败行为，观察失败。
- [x] 实现独立真值与 CLI，运行全套 pytest；分别执行 independent/correlated 演示。
- [x] 更新 README、AGENTS、CHANGELOG；复核结果与规范，记录实际检查。

## 执行记录

- 用户已明确授权按所提约定和顺序实施，在当前会话直接执行。
- 当前目录不是可用 Git 仓库，无法创建 worktree 或提交；保留文件级记录，不初始化或修改隐藏 Git 目录。
- 细化决定：本地精确基线用 NumPy float64 累加保证排序语义；Faiss/GPU 留作后续优化路径。代价是本阶段不提供 GPU 加速业务查询，但能提供独立正确性参照。

## 完成记录 2026-10-12

- Task 1 complete：先观察 32 项缺失实现失败，再实现并通过；随后补充空数据和转换溢出等测试。
- Task 2 complete：先观察 63 项缺失实现失败，再通过解析测试；后续修复极端 Decimal 指数输入。
- Task 3 complete：先观察 23 项缺失实现失败，再通过精确执行测试；手工与独立随机真值一致。
- Task 4 complete：先观察 8 项 CLI/验收缺失失败，再完成 CLI 和独立真值；最终全套 133 passed。
- 独立代码审查：三个重要问题均经失败回归测试后修复，最终整套通过；没有未处理的重要问题。
- 细化决定：category 拒绝 NUL，避免存储改变字符串；代价是初版不支持含 U+0000 的类别。
- 只读数组提供普通写保护，文档明确调用者不得主动解除；更强隔离留待需要时实现。
- 审查未覆盖的范围已界定：P2 网络/版本协议、ANN/GPU 业务执行、面向恶意输入的服务隔离和独立安装包均不属于本次 P1；这些是后续工作，不作为当前已完成能力。
- 2026-10-12 复验：independent/correlated 各 10000 行、128 维、12 条查询全部通过；证据见 docs/validation/p1-local-baseline.md。
- 当前目录无可用 Git 元数据，未提交；本计划与 CHANGELOG 保留执行记录。
