# GPU ANN 检索与 SQL 设备选择设计

日期：2026-10-15。状态：用户于2026-10-15确认并进入实施，实际验证及裁定见 [GPU验收](../validation/gpu-ann.md)。用户已确认首期只做 GPU ANN 检索，保留 CPU float64 精确基线与重排。

## 目标与选型

用户可在每条向量 SQL 中选择 CPU 或 GPU，执行计划如实显示实际设备；现有 SQL、CPU 双路径、数据版本、读租约与存算隔离保持兼容。CPU 构建/训练，显式创建 GPU IVFFlat 副本；SQL 解析、过滤 RPC、候选融合与 float64 重排仍在 CPU。暂不做 GPU 训练、GPU 精确模式、FP16/PQ、多 GPU、自动设备选择或权威索引持久化。

比较三种方式：

| 方式 | 评价 |
| --- | --- |
| 从现有 CPU IVFFlat 克隆 GPU 副本 | 推荐：保持相同训练样本、质心和版本，便于比较候选与召回；代价是 CPU/GPU 双份驻留 |
| GPU 独立训练与检索 | 改动更大，训练差异影响既有质量证据，超出首期范围 |
| 查询时临时建 GPU 索引 | 冷启动和传输成本难预测，违反显式构建约定，不采用 |

首期保证 CPU 两路径不退化，新增 **GPU search-first**。GPU filter-first 需要本机版本确实支持正确的过滤选择器；不以“全局检索后过滤”伪装为先过滤。首期未通过选择器验证时，该组合明确不支持，后续另立扩展任务。

## SQL 与命令约定

```sql
SELECT id, category, vector_distance(vector, [0,0,0]) AS distance
FROM items WHERE value < 1000
ORDER BY distance LIMIT 10
SETTINGS device = 'gpu', mode = 'approx', path = 'search-first';
```

同一语句改为 `device='cpu'` 即选择 CPU。`SETTINGS` 是 VecStor 方言扩展，不是 DuckDB 原生能力。原 SQLGlot DuckDB 方言拒绝该尾部语法；已扩展其 tokenizer/parser 和 AST，仅放开这一个向量查询扩展，不改用宽松的通用方言解析全部 SQL。

未带 SETTINGS 的 SQL 保持原行为。参数优先级为“本条 SQL > 会话 > 原默认值”；合并后整体校验，不修改会话。指定 device 本身不隐式改变 mode/path，因而 `device='gpu'` 与默认 exact 冲突时应提示同时指定 approx/search-first。完整示例包含三项，避免依赖会话状态。

允许的键：device(cpu/gpu)、gpu_id(首期仅0)、device_fallback(error/cpu)，以及现有 mode、path、nprobe、candidates、max_candidates、max_rounds、fallback、quality_profile_id。只接受单个字面量；重复键、未知键、表达式、错误类型、普通 SQL/CRUD 上的 SETTINGS 均拒绝。保持原 65536 字符、4096 AST 节点边界。EXPLAIN/ANALYZE 支持同一语法。SETTINGS 在向存储提交前从已校验 AST 移除，不传入 DuckDB，也不改变质量配置用于匹配的 metadata SQL。

备选“第三个 vector_distance 参数”会把执行策略混入距离语义；注释 hint 当前会被剥离，旧服务可能静默忽略，均不推荐。新增客户端需要服务能力确认和完成帧设备确认；旧服务不支持时必须失败，不能忽略 device。

显式准备 GPU 副本：

```text
.index build items --device gpu --gpu-id 0
.index list
```

该操作若缺少当前 CPU 索引，先走现有 CPU 构建，再克隆；已有匹配 CPU 索引则复用。CPU 构建成功但 GPU 克隆失败时，保留可用 CPU 索引，返回清楚的部分状态，不发布 GPU ready。查询不隐藏训练或克隆。重启后显式重建；`.index clear` 可按设备清理，默认兼容清理当前表全部无引用副本。

## GPU 能力与数值边界

Faiss 官方文档列出 GPU k/nprobe 上限 2048；这里 k 指一次 GPU 搜索请求的候选数 L，不只是 SQL LIMIT。现有低通过率先检索可能需要数万候选，因此不能直接搬用 CPU 参数。实际版本必须以独立子进程探测验证，防止 native 断言影响主进程。见 [GPU 官方说明](https://github.com/facebookresearch/faiss/wiki/Faiss-on-the-GPU)。

首期采取明确能力边界：每轮 L 和 nprobe 不超过已验证上限；max_candidates 大于单轮上限本身不自动降低用户预算，但下一轮需要越界时返回 gpu_candidate_limit，按显式设备回退策略处理。若质量配置初始候选/nprobe 下限超过能力，则执行前拒绝或显式回退。不能静默截断参数后宣称满足质量预算。分片检索与大候选 GPU 合并不纳入首期。

保留 float32 GPU 存储和计算，不启用半精度；保留完整非负 int64 ID，包括大于 2^53 的 ID。用同一 CPU 索引副本、同 nprobe 对比，允许 GPU float32 舍入造成候选差异；最终返回距离仍由现有 CPU float64 重排器计算，不承诺 CPU/GPU ANN ID 逐项完全相同。浮点极值继续执行现有安全边界校验。

GPU filter-first 的组件探测必须核对过滤后 ID 全部合法、稀疏 int64 ID、空集合、不同并发谓词；仅 API 接受参数不等于正确支持。未通过则能力列表公布 false，业务不启用。

## 生命周期、资源与并发

计算侧维护逻辑 IndexKey 不变，物理副本增加 `(IndexKey, backend, device_id, precision, worker_generation)` 身份，CPU/GPU 不互相覆盖。租约固定版本，查询持引用；写入后新查询不使用旧副本，旧读引用结束才回收。GPU 副本保存完整逻辑身份，不要求 CPU 副本永远驻留，但克隆/回退使用的 CPU handle 必须持引用且匹配同一租约。

采用每 GPU 一个通过新解释器启动的常驻 worker，单 owner 线程创建、使用和释放 Faiss GPU 索引与 StandardGpuResources。Faiss 官方明确 GPU 索引及资源不支持普通多线程共享，见 [线程安全说明](https://github.com/facebookresearch/faiss/wiki/Threads-and-asynchronous-calls)。CPU gRPC 并发不直接进入同一 GPU 资源。

匿名管道传递有上限的命令、CPU 索引字节、查询和结果；GPU worker 不读取 storage data-root。父进程在施加 Landlock 后启动 worker；子进程继承限制，验收需覆盖子进程读拒绝。每任务有 request_id、deadline、worker_generation，响应核对身份/行数/ID/有限值。队列建议最多8个等待任务、每设备1个活动任务，超额非阻塞拒绝；具体上限做配置。

排队取消移除对应请求；运行中的 CUDA 内核不能假装已取消。超时/取消需终止并回收该 GPU worker，增大 generation、失效其全部 GPU 副本、明确失败其排队请求；CPU 服务继续可用。新 worker 启动后索引仍需显式构建。构建/克隆失败、OOM、worker 崩溃不留下 ready 句柄。正常结果返回前等待 GPU 完成，避免遗漏异步 CUDA 错误。

保留 CPU 256 MiB/8 条目逻辑限制；新增 GPU 索引逻辑预算建议512 MiB、scratch建议128 MiB、最多8副本，均可配置，另留256 MiB设备余量。创建前检查估算与设备可用空间，克隆期间计入峰值和预留；无引用副本才能淘汰。总 VRAM 含 context/allocator/库开销，不能把逻辑预算宣称为硬显存上限；实际 OOM 必须处理，必要时回收 worker。当前8 GiB显卡能力按运行时检测，不硬编码机型。

## 回退和质量配置

`device_fallback='error'` 为默认；明确要求 GPU 时，无设备、无GPU副本、不支持路径、候选越界、OOM均不能悄悄用 CPU。`device_fallback='cpu'` 允许同一读租约转 CPU ANN；若还触发既有质量/候选不足，才按原 fallback=exact/error 处理。取消、deadline、数据损坏不可被回退吞掉；明确 GPU 且禁止设备回退时，小集合捷径也不能偷偷转 CPU，空结果单独记录为未执行设备搜索。

quality profile 升级为带后端证据的版本；旧格式只可用于 CPU。GPU 配置需绑定 backend、precision、Faiss版本、逻辑索引身份和已测设备类别，不复用 CPU 的 0.95 结论。GPU 不兼容的配置必须拒绝或按明确策略转到对应 CPU 配置，不能把 CPU profile 标成 GPU 已应用。

最终计划显示 requested_device、actual_device、GPU型号/编号、backend、device_fallback_reason、worker_generation，以及 CPU重排说明。若最终精确回退，actual_device=cpu；若先GPU尝试再回退，应保留尝试轨迹。EXPLAIN 只看能力/版本/驻留元信息，不创建 context、克隆或查询。GPU排队、H2D/D2H、同步搜索、CPU重排、RPC分别统计；未实测kernel时间为null，walltime不冒充kernel时间。

## 验收范围

1. 现有473项回归继续通过；CPU-only机器可运行原服务，GPU专属测试明确skip，不能报告GPU成功。
2. 真GPU组件、真实SQL终端、两库/两会话、旧客户端/服务拒绝忽略参数、版本/租约、写入失效、重启、显存不足与worker取消/恢复测试。
3. 真实Landlock证据同时包括计算进程及GPU worker无法读存储目录、GPU查询成功。
4. 单独校准GPU，使用1万/10万×128维、独立/相关/带簇、五档过滤率、K=10/100；每组200校准/500独立留出，K=10支持组平均Recall≥0.95。超过能力组标明确不支持或CPU回退，不能纳入纯GPU达标分母或宣传“全部GPU已达标”。
5. 同质量、相同数据/查询比较CPU/GPU；冷克隆与热查询分别记录，并发1/4/8、至少3次重复，报告P50/P95、吞吐、RPC、CPU/RSS、VRAM、克隆耗时、设备回退率；不预设加速倍数。纯GPU覆盖不足也是本阶段必须报告的结论。
