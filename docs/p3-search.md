# P3 CPU Faiss 混合检索

P3 为 **v2 托管数据库**增加计算侧 CPU `IndexIVFFlat` 内存索引、两种近似路径和执行计划。默认仍为精确查询，旧 P1/P2 接口保持原有行为。安装状态与业务能力不同：本阶段使用已安装 Faiss 的 CPU API，未接入 GPU 业务调度，也未新增依赖。

## 从终端使用

按 [SQL 终端说明](sql-shell.md) 启动存储、计算和终端，选择已导入向量表的数据库。以下假设 `items` 为三维向量表，至少 1024 行：

```text
.open demo
.index build items
.index list
.search mode approx
.search path filter-first
.plan on
```

```sql
SELECT id, category, vector_distance(vector, [0,0,0]) AS distance
FROM items WHERE value < 1000
ORDER BY distance LIMIT 10;
```

切换后对同一 SQL 执行：

```text
.search path search-first
.search candidates 64
.search max-candidates 4096
.search max-rounds 6
.search fallback exact
```

`.search` 显示设置；`.search reset` 恢复 `exact + filter-first`。普通 SQL/CRUD 不受这些设置影响；其他终端保留自己的设置。回车续行、分号提交规则不变。`vector_distance` 的首参数必须是向量列；内联查询数组的维数必须匹配该表。

`.index build items --nlist 32 --timeout 45` 可显式指定桶数和构建超时；`nlist` 不得超过数据及训练样本数。默认 `nlist=min(256,isqrt(N),max(1,N//40))`，训练最多使用 100000 行，固定 seed=20261013。N<1024 返回 `exact_only / small_table`，不训练 IVF。

`.search nprobe 8` 设置每次搜索的初始桶数；不得大于实际索引的 nlist。未显式设置时使用 `min(8,nlist)`。较大的 nprobe 通常增加检索工作量，实际召回率需要离线测量。

`.index clear [items]` 清理无活动读引用的索引；返回 `removed` 和 `busy`。索引只驻留在计算进程内，重启后需重新构建。

## 路径和正确性

| 请求 | 执行行为 |
| --- | --- |
| exact/filter-first（默认） | 完整过滤，分批拉取所有合法向量，float64 平方 L2 精确 Top-K |
| approx/filter-first | 先取得合法 ID，用 Faiss IDSelectorBatch 限制检索，再获取候选原始向量重排 |
| approx/search-first | 先 ANN，再用专用候选过滤 RPC 评估新候选；无回退时不调用全量 ScanMetadata |

先过滤的空集合直接返回 `exact_empty`。默认对不超过 256 条的非空合法集合走精确捷径（匹配索引可用时记录 `small_filtered_set`）；严格模式 `fallback=error` 关闭该捷径。两条 ANN 路径都按原始向量 float64 距离、再按 id 重排；**精确重排不保证全局精确召回**。

初始候选量默认为 `min(B,max(64,4*min(K,N)))`，唯一候选预算 B 默认为 `min(N,max(4096,8*min(K,N)))`，先过滤还受合法集合大小限制。每轮同时扩大候选量和 nprobe，最多默认 6 轮（可设置 1—32）。跨轮去重，只检查新增候选，只获取通过过滤的向量。

不足 K、索引缺失或过期时，默认完整精确回退，最终实际模式为 exact；合法记录不足 K 则返回全部合法记录。`.search fallback error` 改为明确失败：索引缺失/过期等返回 FAILED_PRECONDITION，候选或轮次预算耗尽返回 RESOURCE_EXHAUSTED，绝不将不完整 ANN 结果当成成功。超时、取消、响应损坏不被回退吞掉。

## 计划和统计

```sql
EXPLAIN SELECT id FROM items WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10;

EXPLAIN ANALYZE SELECT id FROM items WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10;
```

EXPLAIN 只获取读租约元信息、查看索引状态，不训练、扫描 WHERE 或拉取向量；未知的匹配数为 null。ANALYZE 执行一次查询，返回实际计划 JSON 行。目前仅支持向量 SELECT 的 EXPLAIN。

`.plan on` 展示完整结构；`.plan off` 仍保留实际模式及回退提示。`.mode json` 保留 `execution_plan`。主要字段：

- `requested_mode/path`、`actual_mode/path`、`fallback_reason` 与 `stop_reason`。
- database/table ID、revision、schema、维数与实际索引构建指纹。
- `rounds` 中各轮 L/nprobe、新增和累计唯一候选、累计匹配数。
- `counts` 中 RPC 次数、Protobuf 负载字节、元数据返回数、向量获取数和各接口次数。
- `compute_elapsed_ms`、`rpc_wait_ms`、`faiss_search_ms`、`rerank_ms`。

`metadata_rows_scanned=null` 表示未测量 DuckDB 的物理扫描行数；候选过滤返回的数量不是全表选择率。`rpc_payload_bytes` 只累计存储 RPC 的序列化请求/响应，不代表 TCP 流量。计划不在线计算或伪造 Recall。客户端计时和计算侧计时作用域不同。

## 数据版本、资源和隔离

一次向量查询或构建只使用同一个 MVCC 读租约。索引键含 database_id、table_id、revision、schema_version、dimension、metric、构建指纹；查询固定当时匹配的索引并持有引用。写入后的新查询不会使用旧 revision，已经开始的旧查询可完成；DROP 重建同名表不会复用旧 table_id 的索引。revision 仍为数据库级，修改其他表也会保守失效。

构建只能显式触发：通过 RPC 按 ID 导出完整快照，在新解释器子进程训练，完整校验后发布。计算服务及构建子进程无需存储目录权限；不通过共享路径读取 DuckDB/NPZ。取消/超时会终止构建子进程并拒绝发布。构建默认 45 秒、最多 60 秒，仍受读租约期限约束。

计算服务可设置：

```bash
vecstor serve compute --storage 127.0.0.1:50051 \
  --index-memory-mib 256 --index-max-entries 8 \
  --isolate-storage-dir data/catalog
```

默认最多一个构建、8 个驻留索引、256 MiB 索引序列化字节估算；构建工作集估算与驻留索引合计不得超过驻留预算的两倍。该估算不是 native RSS 硬限制，不能据此宣称超内存检索。构建报告单独记录 worker 峰值 RSS 和采样范围。仍被引用的索引不可驱逐。

存储端继续使用每库 SQL 队列、8 工作线程和共享 4 个扫描/候选过滤名额，保留向量读取容量。向量批次至多 1024 行/1 MiB，消息上限 4 MiB。服务停止、租约过期、错误版本或损坏批次必须失败。

## 复现

从项目根目录，在 Conda `vecstor` 中运行，输出目录必须不存在：

```bash
python -m pytest -q
python scripts/check_p3.py --output-dir results/p3-check
python benchmarks/p3/run.py --output-dir results/p3-matrix
```

`check_p3` 使用真实 PTY 与 Landlock，覆盖索引构建、双路径、候选扩展、P1 对照、写入失效/重建、计算重启及停存储。实验默认 10000/100000 行、128 维、独立/相关两类分布、五档选择率、K=10/100、三种模式、各重复三次，保存原始计划、真值/结果 ID、召回、延迟、内存及汇总；小规模调试可用 `--sizes 2048 --dimension 8 --repeats 1`。

实验为本机串行、预建热索引，构建另计；P50/P95 每组仅三个样本，不能解释为生产性能保证。原始结果位于 Git 忽略的 results/，验证结论见 [P3 验证记录](validation/p3-faiss.md)。P4 再实现持久索引分发、批量增量维护、缓存和自适应策略；GPU 业务路径仍未实现。

## 显式质量配置（2026-10-14）

计算服务可在启动时加载计算侧自己的配置文件：

```bash
vecstor serve compute --storage 127.0.0.1:50051 \
  --quality-profiles configs/quality/recall95-example.json
```

文件至多 1 MiB/128 个配置，在 Landlock 前读取并校验；不能放在被隔离的 storage data-root 内。配置只读驻留于内存，不从存储目录取配置，不在查询时训练或重新校准。

终端先选择数据库、显式构建匹配索引，再选择配置：

```text
.search mode approx
.search profiles
.search profile q100000-independent-0p01-filter-first
.plan on
```

示例只适用于配套验收数据根 `results/recall-rpc-validation-20261014/100000-independent/storage` 中的 `quality` 数据库、`items` 表、`value < 1000`、K=10 和先过滤路径；查询向量为 128 维且分量在 [0,1]。它不是任意数据集的通用默认值。重新导入会产生不同身份；自己的数据必须重新独立校准并通过真实 RPC 验证来导出对应文件。

配置匹配完整 IndexKey、AST 校验后生成的 metadata SQL、K/路径和查询分量范围。查询向量可在校准范围内变化，投影可变化；不同过滤表达式即使语义等价也可能需要单独配置。数据 revision、同名表身份或构建参数改变后旧配置失效。已开始的旧读可以继续使用匹配旧快照的配置。

配置将 nprobe/candidates/唯一候选预算提升至已校准下限；用户显式设置低于下限会返回 INVALID_ARGUMENT，不能以凑满 K 跳过质量下限。版本/范围/配置缺失按 fallback=exact/error 返回有记录的精确回退或 FAILED_PRECONDITION。小集合精确捷径、取消/期限和数据损坏行为保留。

`.search profile off` 关闭配置，随后使用当前手动参数，未设置时仍为原 ANN 默认参数；`.search reset` 恢复默认精确模式。配置只作用于当前会话向量请求。新客户端会验证服务确认了 quality_profile，防止旧服务忽略新增字段后返回不满足要求的 ANN。

执行计划新增 requested_id/state、配置版本、离线样本数量/均值和 effective_options。离线平均 Recall 不是当前查询的在线 Recall 或单条保证；未在实际工作负载校准的输入不能据此声称已经达到 0.95。

正式加载与隔离验收：

```bash
python benchmarks/p3/validate_quality.py --calibration-dir results/recall-calibration-20261014 \
  --existing-rpc-dir results/recall-rpc-validation-20261014 \
  --output-dir results/recall-profile-validation
```

候选过滤默认批量绑定 ID，并在单条读租约内缓存一份有大小上限的谓词计划；可在存储启动时使用 `--candidate-binding legacy` 或 `--no-candidate-plan-cache` 做独立成本对照。执行计划新增 `selector_build_ms`、`native_search_ms` 与全量选择器快路统计。优化保持默认 exact、版本和租约行为；[最终质量与资源记录](validation/ann-recall-optimization.md) 给出适用范围及局限。
