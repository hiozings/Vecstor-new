# ANN Recall@10 0.95 Optimization Implementation Plan

> **For agentic workers:** 后续实施使用 superpowers:executing-plans 或经用户选择的 subagent-driven-development。以下均为待实施任务；本次只完成分析、参数诊断和计划，不修改业务默认行为。

**Goal:** 在独立留出查询上，使每个规定工作负载分组的平均 Recall@10≥0.95，并以最低可行延迟满足现有索引资源约束。

**Architecture:** 先以现有 CPU IVFFlat 和 SearchOptions 校准搜索范围/候选预算，再接入与索引快照匹配的质量配置；不改变存储权威、RPC、读租约和 float64 真值语义。质量下限、候选充足和资源上限分别建模；P4 自动策略选择与替代索引单列后续项。

**Tech Stack:** 现有 Conda Python、Faiss CPU、NumPy、DuckDB、SQLGlot、gRPC、pytest；不新增依赖。

**Spec:** `docs/design/ann-recall-optimization.md`。

## Global Constraints

- 用户确认无查询延迟硬上限；每个规模/分布/过滤比例组平均 Recall@10≥0.95，不能用跨组平均掩盖差组。
- 默认 exact/filter-first 和原始平方 L2/float64、距离/id 并列语义保持不变；ANN 结果不因样本召回率高而标为 exact。
- 精确小集合、预算回退、真正 ANN 分开统计；每查询 10 个邻居不是 10 个独立质量样本。
- 默认 256 MiB/8 条目索引估算预算、1 构建任务、60 秒租约、1024 行/1 MiB 向量批次及 4 个共享扫描名额保留。预算不等于进程 RSS 硬限。
- 所有数据/向量/索引匹配同一租约；质量配置也必须匹配 database/table/revision/schema/dimension/metric/build fingerprint，不用旧版本校准结果冒充新版本质量。
- 不根据同一留出集反复调参；没有真值时不在线声称观测到了 Recall。
- 当前只是规划；实施前按 AGENTS 获得 Git 操作许可，在用户批准的新功能分支工作。此计划不授权推送/合并。

## Review Focus

- 小选择率由于回退全部精确而“平均召回达标”：必须列真正 ANN 数量和覆盖范围，不能冒称 ANN 优化通过。
- 候选已满但桶探测不足：质量下限不能被 enough_candidates 跳过。
- profile 在 UPDATE、其他表写入、DROP 重建或不同建索引参数后仍被使用：同租约实际 IndexKey 校验并明确失效。
- 提高 L 导致后过滤迟于直接精确几个数量级：同质量比较并记录存储过滤、RPC 等待和负载，不把 RPC 等待直接解释为网络耗时。
- 复用 IDSelector 时数组被回收、条件污染其他查询或错误省略过滤：选择器只在本次查询中持有，完整集合优化必须有同快照覆盖证明。

## 文件职责

- `benchmarks/p3/calibrate.py`：正式离线校准、留出评估及可重放原始结果，替代本次 results/ 内一次性诊断脚本。
- `configs/quality/`：小型、版本化校准配置示例；不保存数据集或大型结果。实际配置含快照身份，只可在对应环境生成使用。
- `query/quality_policy.py`：配置 schema、校准范围与 IndexKey 匹配、参数边界；不在这里读取存储文件。
- `compute/vector_execution.py`：在现有租约和索引固定后解析质量预算，执行与统计；普通 SQL 不变。
- `index/faiss_backend.py`：查询内选择器复用和探桶统计，不能修改共享 index.nprobe。
- `query/search_options.py`、v2 proto/client/shell：可选质量配置标识和展示；旧字段编号/含义不变。
- 测试和文档随每个任务更新；本计划不创建空模块。

## Task 1：把质量评估变成独立可复现实验

**Files:** 新建 `benchmarks/p3/calibrate.py`；扩展 `benchmarks/p3/README.md`；新增 `tests/integration/test_recall_calibration.py`。

**Interfaces:** `calibrate(dataset, calibration_queries, options_grid, workload) -> CalibrationResult`；`evaluate(dataset, heldout_queries, frozen_config) -> QualityReport`。结果含 query_id/seed、真值与召回 ID、实际模式、参数、完整回退原因；配置选择只接收 calibration 结果。

- [x] 先写小样本回归：查询 seed 不重叠；更换 heldout 数据不能改变所选参数；空真值独立统计；实际 exact 不计入真正 ANN 的均值。
- [x] 运行新增测试，确认缺少校准接口而失败。
- [x] 实现至少 200 校准/500 留出查询每组；1 万/10 万×128、两类既有分布、五档通过率、K=10；另加入带簇负载，真实 embedding 在确认来源和许可后单列。
- [x] nprobe 网格覆盖 8→nlist，选择 calibration mean≥0.97 的最小值作为初始余量规则；发布前要求 heldout group mean≥0.95，并报告查询级 bootstrap 95% CI、最差值、回退/不足比例。
- [x] 将本次“L 64→4096、nprobe固定时召回不升”的消融、global required_L 分布及 nprobe=nlist 对照纳入脚本。全桶对照单独标明，不当作 ANN 加速结果。
- [x] 验证新测试及原实验工具；输出原始数据、源代码/依赖/索引版本和配置选择记录。

## Task 2：使用现有接口验证高召回配置与路径边界

**Files:** `configs/quality/` 校准产物、`docs/p3-search.md`、新验证记录；此任务不必修改执行协议。

**Interfaces:** 校准产物映射至现有 `SearchOptions(mode='approx', path=..., nprobe=..., candidates=..., max_candidates=..., max_rounds=..., fallback=...)`。

- [x] 先写/运行真实 RPC 用例，证明默认 nprobe=8 的困难查询召回不满足目标，同时正确返回 K；该质量断言应失败，不把它认定为 SQL 错误。
- [x] 在冻结的校准配置上验证两路径；当前数据起点为 1 万行 nprobe≈96、10 万行非小集合 nprobe≈224—240，具体以 Task 1 输出为准，不按行数硬编码普适参数。
- [x] 后过滤 L 由校准 required_L 的高分位数加余量确定；上限/轮次有界，取消和 deadline 不变。先不要增加自动 WHERE 预扫描或改为无限扩展。
- [x] M≤256 保留精确；低通过率明确推荐先过滤，强制后过滤实验单独报告数万候选成本。此阶段先写使用建议，不提前交付 P4 自适应优化器。
- [x] 验证所有冻结配置的留出质量；失败则记录失败并重新划分留出集，不能在相同评估集上无痕调参。

## Task 3：接入显式的质量配置与停止规则

**Files:** 新建 `src/vecstor/query/quality_policy.py`；修改 search_options/vector_execution、v2 proto、sql_client、shell 及服务配置入口；新增 unit/integration 测试。

**Interfaces:**

- `QualityProfile`：profile_id、完整 snapshot/index identity、校准数据/查询摘要、适用工作负载说明、`nprobe_floor`、`initial_candidates`、`max_candidates`、`max_rounds`、实测统计。质量数字是离线证据，不是运行时保证。
- `resolve_quality(profile, actual_index_key, request_options) -> QualityBudget`：检查版本与参数冲突；不查询存储目录、不执行 WHERE。
- `SearchOptions.quality_profile_id: str | None`：默认 None，保留原行为。`.search profile <id>|off` 显式选择；不增加名为“保证召回率”的开关。

- [x] 先写失败测试：候选已满但未达到 quality floor 不得以高质量成功；显式 nprobe 低于 floor、用户预算低于 profile 所需预算应报明确冲突，不静默忽略约束。
- [x] 增加缺失/错误版本 profile 测试，包括其他表写入、同名表重建、不同 nlist；默认允许有标识的精确回退，strict/error 模式明确失败。已开始的旧读仍使用匹配旧 profile。
- [x] 实现经过 schema 校验的计算侧只读配置加载；配置来自显式路径，在施加 Landlock 前读取和校验，不能借此打开存储路径。进入查询后以已固定的实际 IndexKey 原子匹配。
- [x] ANN 从 nprobe_floor 开始，减少无效早期轮次；在质量下限满足后，候选不足才继续扩展。已有显式更高 nprobe 可以保留；超出 nlist 拒绝。
- [x] v2 增量字段携带可选 profile_id；同步生成 proto。计划展示 requested/actual 参数、profile identity、校准范围、是否全桶及回退原因，不写虚假的 online Recall。
- [x] 通过旧客户端兼容、会话独立、版本/租约、严格预算、超时/取消和真实终端测试；默认 exact 不受影响。

首版 profile 可以只支持已校准的显式路径，不要求自动识别任意过滤分布。未知工作负载必须提示未校准范围；不能把一个数据集上的配置描述为普适 0.95 保证。

## Task 4：降低高质量路径成本并独立验收

**Files:** `index/faiss_backend.py`、`compute/vector_execution.py`、对应单元/并发测试；扩展校准脚本与 `scripts/check_p3.py` 的质量模式覆盖。

**Interfaces:** `PreparedSelector` 由本次租约合法 ID 构建并持有数组；backend search 接收该对象或 None，禁止修改共享索引。统计分离 selector_build_ms/native_search_ms；无法可靠逐请求归因的 Faiss 全局计数只在单线程基准采集，在线标 unknown。

- [x] 先写失败测试：跨轮仅构建一次 selector；两个并发查询不同过滤条件不会互相污染；取消后释放；完整快照覆盖条件不成立时不得省略 selector。
- [x] 实现查询内复用与严格证明后的全量匹配快路；继续 float64 重排，不改变精确标签条件。
- [x] 在相同 heldout Recall 下比较 nlist/nprobe 配对；单独评估训练迭代数/样本数。不将更长训练或更大 nlist 预设为有效。
- [x] 记录端到端 P50/P95、CPU 时间、RPC/字节、瞬时查询 RSS 和构建峰值；不能只比较 kernel 时间或用已有构建 VmHWM 推断查询内存增量。
- [x] 对后过滤高成本先拆分 SQL 解析、绑定、DuckDB 执行与 RPC 等待；仅在确认瓶颈后考虑租约内有界谓词计划复用/批处理，保留共享扫描名额。独立于召回改动开关，便于消融。
- [x] 完整业务回归、真实 PTY、Landlock、并发 1/4/8；归档质量和成本结果，更新 AGENTS/CHANGELOG/README。用户未给延迟硬上限，不能为了更低延迟牺牲规定组的质量。

## 可选后续：Flat/HNSW 对照

只有上述配置成本不可接受时再单列方案：同质量比较 native Flat 与 HNSWFlat，保留 P1 真值；验证 ID、过滤支持、float32 极值/并列、删除/重建和版本一致性，实测内存与构建成本。GPU 属于执行加速评估，PQ 属于压缩权衡，都不作为当前漏桶问题的默认解法。

## 2026-10-13 初始分析记录（历史）

- 读取 P3 360 次原始结果、相关执行器/索引/数据生成代码，并核对 Faiss 官方资料。
- 运行 17280 条离线参数探测、独立校准/留出评估、候选量消融与查询级置信区间计算。
- 在克隆数据库上完成 360 次真实 gRPC 成本对照；未修改默认配置或产品代码。
- 分析结果见设计文档；以上 Task 1—4 尚未实施，不将诊断脚本当作已交付的质量配置功能。

2026-10-14 Task 1：60 组正式评估通过，每组 200 校准/500 留出，最低平均 Recall@10=0.9702；435 项全套通过。先行修复阻塞长实验的 SWIG 选择器所有权泄漏，审计源码等价变更后恢复已冻结配置。Task 2—4 继续实施。

2026-10-14 Task 2：60 组、720 次真实 RPC 返回 ID 与冻结结果一致，距离校验通过，所有进程已回收；436 项回归通过。绑定实际 IndexKey 的配置保存在验收产物，并提供单组示例。

2026-10-14 Task 3：质量配置服务/终端/协议已接入；720 次加载配置的隔离 RPC 对照通过，6 个数据根读取均拒绝，进程回收；全桶标识不改变 approximate 标签。

2026-10-14 Task 4：选择器复用、全量快路、候选批量绑定和租约内有界计划缓存已实现。96 组 RPC 成本消融、12 组训练参数探索及最终 720 次隔离对照完成；独立审查的两个恢复校验问题已修复，详见 [最终验收](../../validation/ann-recall-optimization.md)。

最终完整回归：`conda run --no-capture-output -n vecstor python -m pytest -q`，473 passed in 48.20s（2026-10-14）；包括真实 PTY、gRPC、版本/租约、并发和隔离。
