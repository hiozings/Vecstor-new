# P4 有界数据缓存与按需传输

状态：T1—T8已按用户选择的代表性实验范围完成。审查修复后838项完整真实GPU回归、630次配置/33744条计时查询通过；性能收益和退化见[验收记录](validation/p4-cache.md)。设计见[缓存与传输设计](design/p4-data-cache-and-transfer.md)，实施记录见[八任务计划](superpowers/plans/2026-10-19-p4-data-cache-and-transfer.md)。

2026-10-23追加优化：查询统计改为O(1)快照，完整真实GPU回归841项通过；CPU串行/容量前后对照见[快照验收](validation/cache-stats-snapshot.md)。

## 启动与请求控制

默认不启用缓存，元数据仍走full路径。已按[持久索引说明](p4-indexes.md)准备存储和发布工件后，可启动：

```bash
vecstor serve compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50052 \
  --index-source published --timeout 60 --data-cache-mib 64 \
  --metadata-projection projected --fused-candidates
vecstor --compute 127.0.0.1:50052 --timeout 60
```

```text
.open demo
.index load items
.search mode approx
.search path search-first
.search data-cache on
.search metadata-projection projected
.cache stats
.cache clear
```

`.cache clear`只清所选库的计算侧数据缓存，不删除数据、持久工件或驻留索引。另一个节点的缓存不受清理影响。`.search reset`复位会话选项，不清缓存；`default`继承进程启动配置，恢复默认会话不等于关闭管理员已启用的缓存。

SQL可逐请求覆盖，例如：

```sql
SELECT id FROM items
ORDER BY vector_distance(vector, [0.1, 0.2]) LIMIT 10
SETTINGS data_cache='off', metadata_projection='full';
```

查询向量维度须与实际表一致。缓存支持exact及CPU/GPU重排；GPU仍仅approx/search-first，维持原有设备/质量限制。`candidate_data_path='separate'|'fused'`独立控制数据路径；开启缓存不会自动切换执行策略。显式fused需要启动`--fused-candidates`。

| 参数/选项 | 默认 | 语义 |
| --- | --- | --- |
| --data-cache-mib | 0 | 缓存逻辑容量；显式on在0容量下拒绝 |
| --data-cache-max-entries | 65536 | 与容量同时限制 |
| --data-cache-scan-admission | off | exact全量扫描默认只查缓存不填充；on用于允许扫描预热 |
| --metadata-projection | full | projected只返回所需元数据，SELECT *仍含全部列 |
| data_cache | default | 请求级default/off/on，off不进行向量缓存lookup/填充，仍读取用量统计 |
| metadata_projection | default | 请求级default/full/projected |

projected不会省去距离计算必需的向量，也不减少WHERE必须读取的过滤字段。fused在开启缓存或projected时只返回未命中且通过过滤的向量；即使全部命中，仍远程执行过滤和全部候选存在性校验。旧关闭/full/fused路径保留。

## 一致性、容量与故障

键绑定存储进程epoch、数据库、表、revision、schema、维度和ID。每条查询仍需远程读租约；写入/其他表写入、DROP重建、存储重启不会错误命中旧数据。旧查询在有效租约下继续使用旧版本；过期、取消、损坏流或不完整候选返回均失败，不用缓存掩盖错误。

缓存不持久化，计算重启后为空，工件预加载也不会填它。数据epoch不会改变既有IndexKey；若通过离线备份恢复重用了原有数据revision，local模式索引须由运维清理重建或重启计算进程；published模式仍按权威工件身份显式加载。缓存不是存储离线服务，新的BeginRead或过滤无法连接存储时必须失败。

条目保存独立不可变向量字节；LRU记账每scope1024字节、每行512字节管理额度加向量长度。64MiB/128维最多约65535行，10万行不能全放下。记账值不是Python分配器RSS硬限；索引、DuckDB、RPC、旧有全量元数据物化和查询工作集另计。新增缓存批工作区为16MiB/4槽，超额请求明确记录bypass并执行原网络读取，不等待其他查询下载。

clear会使已在途但尚未填充的结果跳过插入；已有请求仍可返回完整已验证答案。global admission generation会保守使其他库的在途填充也跳过一次，但不会清除其他库已有条目。

## 计划和统计

执行计划的data_cache区分lookup/hit/miss、实际served-from-cache、网络vectors_fetched、bypass、入缓存行数和淘汰。fused在过滤前查缓存，hit_rows可包含后来被WHERE拒绝的行；不应把它们当作实际节省的向量读取。拆分重试lookup单列，避免逻辑候选双计。

rpc_payload_bytes为实际Protobuf序列化字节，cache_payload_bytes_avoided仅为命中向量裸字节推算。首次按需存储能力检查计入RPC/字节/等待，启动握手不算查询RPC。lookup/copy等计时可能嵌套，不相加作为总时间。EXPLAIN不触碰缓存；ANALYZE只执行一次。

## 验证入口

```bash
python scripts/check_p4_cache.py --output-dir results/p4-cache-new --rows 10000
python scripts/check_p4_cache.py --output-dir results/p4-cache-gpu-new --rows 10000 --require-gpu
python benchmarks/p4/data_reads.py --output-dir results/p4-data-reads-new --tier representative
```

目录必须不存在，失败保留report及已执行证据，finally回收进程。完整实验入口为correctness/costs/gpu；代表性矩阵不等同于完整200查询矩阵。实验流程、样本及局限见[实验说明](../benchmarks/p4/README.md)。

2026-10-23已将每请求describe改为常数时间进程统计快照，复用既有标量，避免遍历缓存。显式`.cache stats`的分库统计仍按需遍历；并发锁等待、逐行复制和LRU成本仍存在。针对既有CPU退化场景的前后对照见[快照验收](validation/cache-stats-snapshot.md)，默认不自动开启优化，不能把传输减少等同于普遍提速。
