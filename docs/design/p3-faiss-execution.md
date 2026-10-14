# P3 Faiss 近似检索与混合执行设计

日期：2026-10-13。状态：CPU 路径已实现，最终验证证据见 [P3 验证记录](../validation/p3-faiss.md)。本文保留设计决策及约束，使用方式见 [P3 使用说明](../p3-search.md)。

## 交付目标与边界

1. 接入 Faiss CPU `IndexIVFFlat`，支持按快照构建、复用计算侧内存索引。
2. 同一向量 SQL 可显式选择 filter-first 或 search-first 近似路径。
3. 实现候选扩展、资源边界、精确回退和明确失败行为。
4. 提供 EXPLAIN、EXPLAIN ANALYZE 及终端实际执行计划展示。
5. 保持默认精确查询、持久 CRUD、会话选库、类型化结果和 Landlock 隔离兼容。

P3 必做 CPU ANN。GPU 后端单列可选扩展，需先验证过滤参数和 ID 行为，不因安装了 GPU Faiss 就宣称业务支持。P4 再实现权威索引持久化/分发、增量更新、数据缓存和自适应代价模型。P3 不增加自动选择路径；明确指定路径便于做对照实验。

## 实施前代码约束

- `compute/sql_service.py::_vector` 目前是完整过滤、分批读取所有合法向量、NumPy float64 精确排序，保留为基线和回退实现。
- `storage/database.py::_begin_read` 返回 database_id、table_id、revision、schema_version、dimension，跨 RPC 依靠 read_token 保持同一事务。
- revision 是数据库级的：修改其他表也会让已有索引版本不再匹配。这会多建索引，但 P3 接受这种保守失效，不擅自改为表级版本。
- 现有 ScanMetadata 会评估完整 WHERE，不适合直接冒充 search-first 的候选过滤。
- 当前 ResultFrame 只有 execution_path 字符串，无法表达检索轮次、精确/近似状态和回退原因。
- 有 4 MiB 消息上限、1 MiB 向量批次预算、共享扫描名额限制及 60 秒读租约上限，新接口继续遵循。

## 查询语义与终端选项

保持 SQL 不变：

```sql
SELECT id, category, vector_distance(vector, [0.1, 0.2, 0.3]) AS distance
FROM items
WHERE value >= 10
ORDER BY distance
LIMIT 10;
```

新增会话命令：

```text
.index build items
.index list
.search mode approx
.search path filter-first
.search nprobe 8
.search candidates 64
.search max-candidates 4096
.search max-rounds 6
.search fallback exact
.plan on
```

- `.search` 不带参数显示当前配置，`.search reset` 恢复精确默认值。
- 默认 mode=exact、path=filter-first，走现有精确实现，不依赖索引。
- 近似模式才支持两种路径。exact 与显式 search-first 组合报参数错误，不暗中改变其含义；reset 可一次恢复合法默认配置。
- `.search` 配置只作用于当前会话后续向量查询，不影响普通 SQL、CRUD 或其他终端；每条请求复制选项，运行中不受后续设置改变。
- 近似模式的输出必须在开始和完成时明确标识，不能因为返回了 K 行或做了精确重排就标成 exact。
- 最终候选始终按原始 float32 向量、float64 平方 L2 与 id 重排。返回距离沿用 P1 的数值语义；近似性来自候选遗漏。
- 在召回到的集合中按 `(distance,id)` 排序；近似模式不保证找到全部并列候选，Recall 的真值仍遵循 P1 的确定性 ID 规则。

设置 fallback=exact（默认）允许索引缺失、过期、预算不足等情况回到精确基线；fallback=error 用于测量严格 ANN 工作负载，不能就地返回不足 K 的不完整成功结果。SQL 分句器仍仅处理边界，选项通过 RPC 字段传输，不往 SQL 中拼接隐藏提示。

## 索引生命周期

### 快照键与构建

索引键包含 `(database_id, table_id, revision, schema_version, dimension, metric, build_fingerprint)`。指纹包括算法、nlist、训练 seed、采样策略和 Faiss 版本；nprobe 属于查询参数，不改变索引键。

同一快照可有不同构建配置。registry 为每个快照维护当前活动 IndexKey，最后一次成功构建成为后续查询的活动索引；查询开始时解析并固定该 IndexKey，构建新配置不会替换运行中查询持有的对象。执行计划记录实际指纹，不根据当前默认 nlist 猜测已构建索引。

`.index build <table>` 是计算服务的显式操作：BeginRead → 通过新增 RPC 分批导出该租约内的全部 ID/向量 → 训练并 add_with_ids → 核对行数/唯一 ID/版本 → 发布只读内存索引 → EndRead。不读取服务端文件，也不通过伪造一个“无条件向量查询”来导出数据。

构建时不使用用户 WHERE。库名或表名不作为独立版本标识，DROP 后重建同名表必须依靠不同 table_id 区分。

本地已用已安装 Faiss 1.12.0 做只读能力探针，确认 CPU IVFFlat 的 `SearchParametersIVF(nprobe=..., sel=IDSelectorBatch(...))` 和大于 2^53 的 int64 自定义 ID 可用，设置查询参数不会修改 index.nprobe。它不是 P3 业务验收。

### 初始配置

| 配置 | P3 初始值/规则 |
| --- | --- |
| 距离 | METRIC_L2，输入 float32 |
| 小表策略 | N < 1024 不训练 IVF，明确使用精确路径 |
| nlist 默认 | `min(256, isqrt(N), max(1, N//40))` |
| 训练数据 | 固定 seed=20261013，从同一快照最多采样 100000 行 |
| 训练顺序 | 导出按 id 稳定排序，确定采样结果后训练 |
| 初始 nprobe | `min(8,nlist)`；可显式配置并记录实际值 |
| 计算侧驻留预算 | 默认 256 MiB、最多 8 个索引条目；均可配置 |
| 同时构建 | 1 个；相同键已就绪则复用，不重复训练 |
| 构建 deadline | 默认 45 秒，最多 60 秒，不超过读租约期限 |

这些是项目的初始工程参数，不是 Faiss 推荐参数或性能保证。nlist/训练样本合法性需验证；显式不合法配置报错，不静默改写用户参数。

构建使用独立工作进程，避免不可中断的 Faiss train 长时间占用 gRPC 服务线程。父进程持有读租约和 deadline，超时/取消时停止构建进程、清理缓冲并拒绝发布；通过进程内存/管道传输，不能在隔离计算进程中借用存储目录临时落盘。只在完整验证后原子加入 registry。

工作进程使用 spawn/新解释器启动，不在已运行 gRPC/BLAS 线程的进程中 fork 后直接执行 Faiss。禁止因工作进程启动失败降级到不能终止的同步训练。

训练样本、导出缓冲、序列化传递和索引副本均计入预估构建预算；记录构建峰值 RSS 与逻辑索引字节。预估不等于精确 native 内存硬上限，无法满足预算时拒绝构建，不宣称超内存能力。

### 复用与失效

- 每条查询先取得租约，再查找完全匹配的索引；禁止使用最新索引去查询旧租约或反过来。
- 写入提交后新查询看到新 revision，旧索引不再用于它。没有新索引时默认精确回退；用户再次 `.index build`，不在普通查询里偷偷训练。
- 已经持有旧租约和旧索引的查询可完成；索引有引用计数，仍被查询使用的条目不能清理。
- `.index list` 显示版本、算法、行数、维度、内存和引用状态；`.index clear [table]` 只清理无活动引用的派生条目。
- 预算不足时先回收无引用的旧版本；仍不足则拒绝构建，不驱逐正在使用的版本。P3 不实现自适应缓存策略。
- 计算服务重启后 registry 为空，查询精确回退，允许重新构建；存储重启使旧 token 失效，不凭本地索引绕开存储确认。

## 两条执行路径

### Filter-first

1. BeginRead 固定版本，ScanMetadata 计算合法 ID 集合及其元数据，得到 M。
2. M=0 立即返回精确空结果；M≤256 默认直接精确计算，记录 small_filtered_set。
3. M 较大且匹配索引存在时，使用合法 int64 ID 构建 IDSelectorBatch，传入每次搜索自己的 SearchParametersIVF。
4. Faiss 返回候选后去掉 -1 填充，核对所有 ID 都在合法集合中；再从同一租约取原始向量、float64 重排。
5. 候选不足时扩展 nprobe 和候选上限；达到预算仍不足则精确回退或明确报错。

fallback=error 时关闭小合法集合的精确捷径：非空集合仍尝试有效 ANN 索引，无法执行则报错。M=0 可直接返回并标记 exact_empty，不计入 ANN 检索质量样本。

本路径不为每个 WHERE 重建 Faiss 索引。合法 ID 集合的构建成本和元数据传输量必须计入统计。IDSelector 所需数组必须在 native search 结束前保持存活。

### Search-first

1. BeginRead 固定版本并选择完全匹配索引；不预先完整执行 WHERE。
2. 对全表 IVF 检索 L 个候选，去掉 -1，跨轮次维护已检查 ID 集合。
3. 新增 FilterCandidates RPC，在同一读事务中只检查新候选 ID 是否满足原 SQL 的 WHERE，返回匹配的元数据。
4. 只读取通过过滤的新候选向量，合并后 float64 重排 Top-K。
5. 不足 K 时增加 L 和 nprobe，再检查新增 ID；不会反复拉取已检查向量。

不能把现有 ScanMetadata 全量 WHERE 再与候选求交后声称是 search-first。FilterCandidates 要同时保证候选真实存在、过滤已完整执行，不能把未知 ID 当成“不匹配过滤”静默丢弃。

Search-first 通常不知道全体符合条件的行数，计划中显示 examined_candidates 和 matched_candidates，不能把后者冒充全局匹配数或真实选择率。

## 候选扩展和终止规则

以 N 为全表行数、K 为请求数量、K'=min(K,N)。默认唯一候选预算 `B=min(N,max(4096,8*K'))`，初始 `L=min(B,max(64,4*K'))`。filter-first 可进一步把预算限制到 M。用户显式候选预算优先，并在计划里显示实际约束。

每轮同时将 L 和 nprobe 翻倍，分别受 B/N 和 nlist 限制；最多 6 轮。候选预算按跨轮次唯一 ID 计数，不按最后一轮的 L 或重复次数统计；新候选超过剩余预算时截断检查并记录 candidate_budget。

成功计划中的终止原因：enough_candidates、candidate_budget、round_budget、no_progress；取消和 deadline 以明确 RPC 错误返回，不生成成功计划。新增 ID 为零且参数还能扩大时继续扩大；参数已不能扩大才判定 no_progress。

- 得到至少 K 个合法候选后可结束近似检索并重排；这不是召回率保证。
- 无法填满 K 时默认走完整精确过滤，确保真正不足 K 的情况下返回全部合法记录，而不是因为 ANN 漏检而少返回。
- fallback=error 时，候选预算/轮次耗尽返回 RESOURCE_EXHAUSTED；缺失匹配索引返回 FAILED_PRECONDITION。
- 取消或总 deadline 已到不再开启回退、不重置计时，直接失败；损坏响应仍返回 DATA_LOSS。
- nprobe=nlist 或候选精确重排本身不能作为 exact 标志。只有现有完整 float64 精确路径完成，才标 actual_mode=exact。

## 数值与资源边界

Faiss 使用 float32 距离，P1 允许的某些很大但有限的 float32 值可能导致其内部距离溢出。构建时记录最大绝对分量；以 float64 保守估计距离范围。若训练或查询的平方距离上界超过 `0.25 * finfo(float32).max`，拒绝 ANN 路径并按策略精确回退，不能改变 P1 合法输入的意义。

索引中的每个真实 ID 必须唯一且非负，-1 只允许作为 Faiss 填充。索引输入和 RPC 结果需验证行数、版本、维度、大小与有限值；模型输出或索引文件不能作为信任源跳过这些校验。

CPU 索引发布后只读。nprobe/selector 使用局部参数，禁止并发修改共享 index.nprobe。引用计数、构建发布和 registry 操作加锁；不持锁执行长时间网络 RPC。

## 协议扩展

保持 v2 已有字段编号和默认精确行为，新增字段/RPC：

| 位置 | 新增内容 |
| --- | --- |
| SQLRequest | SearchOptions：mode、path、nprobe、候选/轮次预算、fallback |
| ReadLease | optional row_count；区分旧服务未提供与真实空表 |
| SQLStorage | StreamVectorSnapshot：同一 token 的全表 ID/向量分页导出 |
| SQLStorage | FilterCandidates：token、原向量 SQL、候选 ID；返回 checked_count、匹配行和租约标识 |
| SQLCompute | BuildIndex、ListIndexes、ClearIndexes、GetSearchCapabilities |
| ResultFrame COMPLETE | QueryExecutionPlan，含实际模式、路径、参数、统计和回退信息 |

新终端开启 ANN 前检查能力；旧客户端不发 SearchOptions 时始终精确，旧服务不支持新能力时明确提示，不假装设置已生效。v1 P2 只读接口本阶段不接入 ANN，原测试保留。

StreamVectorSnapshot 及候选过滤复用存储队列、批次限制和流名额；不要因增加导出流再引入线程饥饿。read_token 到期、库/table 身份不一致、响应中断均不可用旧缓存掩盖。

## 执行计划展示

```sql
EXPLAIN SELECT id FROM items
WHERE value >= 10
ORDER BY vector_distance(vector, [0.1,0.2,0.3]) LIMIT 10;

EXPLAIN ANALYZE SELECT id FROM items
WHERE value >= 10
ORDER BY vector_distance(vector, [0.1,0.2,0.3]) LIMIT 10;
```

EXPLAIN 只解析、读取租约元信息和检查 registry，不训练索引、不执行 ANN、不拉取向量，也不完整计算 WHERE；未知的匹配数显示 unknown。EXPLAIN ANALYZE 执行查询后返回实际计划和统计，不重复执行第二遍查询。首版仅支持向量 SELECT 的这两种形式，其他语句明确拒绝。

`.plan on` 在普通向量查询后显示实际计划；JSON 输出保留完整结构。计划至少包括：

- requested/effective mode 和 path，database/table/revision/schema/index 标识。
- 索引状态、nlist、nprobe、构建参数、候选预算与请求 K。
- 元数据扫描数、唯一候选检查数、匹配候选数、向量读取数和返回数。
- 每轮 L/nprobe/新增与累计候选/通过数量/停止原因。
- RPC 次数、Protobuf 负载字节、计算总耗时、RPC 等待、Faiss 检索及重排耗时。
- fallback_reason、exact 保证来源或 approximate 标志。

离线没有真值时不在计划里捏造 Recall。`.timer` 的终端显示耗时与计算服务耗时分别标注。显式构建的时间/数据传输单独记录，冷启动与热索引结果分开，不隐藏预处理成本。

## 验收

1. 原 P1/P2/P2.5 测试保持通过；未设置 ANN 的查询逐项一致。
2. 两条路径真实经过 Faiss：filter-first 验证 selector；search-first 在无回退情况下不得调用全量 ScanMetadata。
3. 构造前几轮近邻全被过滤的负载，必须观察到扩展轮次和新增候选；预算不足回退得到精确结果或明确报错。
4. 并发 UPDATE/DELETE/DROP 重建、其他表写入、计算/存储重启、租约到期和取消，均不得混用索引版本。
5. 大整数/不连续 ID、空表、M<K、重复距离、近似距离精度、巨大有限分量、错误批次和资源预算都有测试。
6. Landlock 下完成索引构建、两路径查询、更新后回退及重建，计算进程仍不能读取 data-root。
7. 10000 和 100000 行、128 维，两类分布、五档通过率、K=10/100，输出 Recall@K、返回数量和计划；至少三次重复用于描述波动，P3 不承诺性能提升。

Recall 使用相同过滤条件、快照和 P1 float64 真值，分母为 min(K,M)，空真值单独统计。报告区分真正 ANN 结果与精确回退，不能让大量回退掩盖 ANN 召回不足。Recall@10 ≥0.95 保留为调参目标，默认 nprobe=8 不构成普遍保证。

## 参考

- [Faiss 每查询搜索参数](https://github.com/facebookresearch/faiss/wiki/Setting-search-parameters-for-one-query)：局部 SearchParameters 与选择器。
- [Faiss 参数/过滤测试](https://github.com/facebookresearch/faiss/blob/main/tests/test_search_params.py)：IDSelector 与查询参数组合。
- [Faiss GPU 限制](https://github.com/facebookresearch/faiss/wiki/Faiss-on-the-GPU)：GPU API 不完全等同 CPU，持久化前需转 CPU。
- [pgvector 过滤与迭代扫描](https://github.com/pgvector/pgvector#iterative-index-scans)：候选不足继续扫描的参考；本项目不直接移植 PostgreSQL 内部代码。

## 实现映射

实际执行器位于 `compute/vector_execution.py`；租约校验与统计集中在 `compute/lease_io.py`，显式构建调度在 `compute/index_manager.py`。没有创建独立 `query/execution_plan.py`：计划使用版本化 JSON 结构，经 QueryExecutionPlan 传递，codec 校验对象和有限数值。预算采用 256 MiB 索引序列化字节估算，构建估算加驻留量上限为其两倍；这不是 native 内存硬限。实验和审查修复记录见验证文档。
