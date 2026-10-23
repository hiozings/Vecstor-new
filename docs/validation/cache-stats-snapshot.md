# 查询缓存统计快照优化验收（2026-10-23）

## 改动与语义

旧 `CachedLeaseReader.describe()` 为获取两个进程总量调用 `stats(database_id)`，在缓存全局锁内扫描全部条目、建立列表和集合。现在调用 `process_snapshot()`：只复制已有标量和容器长度，返回独立字典；锁内计算时间和临时空间为 O(1)，不随驻留向量条数增长。锁等待仍可能受并发准入、清理或显式管理统计影响，并非无锁或固定墙钟耗时。

管理接口 `stats(database_id)` 仍按需遍历，分库统计及 process 字段保持原契约；工作区、版本隔离、LRU、准入、逻辑预算和请求局部命中计数均未改动。没有新增分库冗余记账，没有更改默认开关或协议。

实现提交：`644ea19`；前测业务源码与原缓存验收 `9362353` 相同，分支起点 `2564d53`。本次不是全部 P4 验收。

## 范围与复现

本机 Intel Core i9-14900HX，RTX 4060 Laptop GPU 8188 MiB、驱动615.78.08；Linux 7.2.11-1-cachyos/glibc2.44、Python3.12.15、NumPy1.26.4，复用锁定Conda环境。GPU仅用于完整回归，本次性能对照均CPU。

前测在业务改动前独立完成，后测与测试串行运行。复用既有代表性 manifest：case 6（10万行、128维、独立分布、100%过滤率、K10、并发1、八种缓存/投影/fused组合，冷/暖/独立/热点各20查询）；case 31/32（相同数据、K100、8/64 MiB、projected/separate，缓存开/关，冷/暖各200查询）。每种配置重复三次，单轮36配置/6720计时查询。数据 seed=20261016，查询 seed=20261019；全部 CPU search-first，nprobe 覆盖 nlist。每个配置使用新计算进程、预载同源索引、预热会话后清空应用缓存；不清 OS 页缓存。

```bash
# 分别在改动前与实现提交上运行；目录必须不存在。
env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  conda run --no-capture-output -n vecstor python benchmarks/p4/data_reads.py \
  --output-dir results/cache-stats-20261023/before \
  --tier representative --case-ids 6 31 32
# after 使用同一命令，仅将输出目录改为 after。
env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  conda run --no-capture-output -n vecstor python benchmarks/p4/cache_stats.py \
  --output-dir results/cache-stats-20261023/micro
env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 \
  conda run --no-capture-output -n vecstor python -m pytest -q \
  --junitxml=results/cache-stats-20261023/pytest.xml
```

原始产物在 `results/cache-stats-20261023/`（Git 忽略）：before/after 含配置、环境、源码 SHA256、每查询答案与计划、资源、数据/查询文件和服务日志；`compare.py` 逐查询验证输入摘要、答案、召回、实际模式/路径/设备、候选轮次、RPC/字节/向量数与缓存记账相同，输出所有阶段及重复的 `comparison.json`。前后工件身份不同，不将身份字符串强行当作可比条件；输入文件及逻辑结果必须一致。

## 独立统计成本

同一进程内比较保留的旧管理统计与新快照，每规模两方法随机顺序、3次重复、每次100调用，5次预热；使用公开 batch 接口填充缓存，返回值在计时后逐次验证一致。下面是三次重复 P50 的中位数，单位微秒；原始纳秒样本及 P95 全部保留于 `micro/report.json`。

| 驻留条目 | 旧 stats P50 µs | process_snapshot P50 µs |
| ---: | ---: | ---: |
| 0 | 1.302 | 0.376 |
| 8191 | 2914.394 | 0.454 |
| 42785 | 16480.628 | 0.435 |
| 65535 | 30029.084 | 0.450 |

该微基准验证删除遍历的直接成本，不代表整条查询的加速倍数。零向量内容不参与统计运算；微基准没有模拟 RPC、过滤、重排或并发竞争。

## 回归与端到端对照

前后各36次配置、6720条计时查询，共72次/13440条，三次重复；6720对输入与答案及上述计数逐条一致。数据与查询NPZ逐字节一致；零跳过、不完整结果或清理后残留进程，所有运行通过Landlock目录/文件拒读。两轮分别454.61/396.18秒。前后只改动两个业务源码文件；后测源码SHA与完整回归使用的源码清单一致。

以下为warm阶段，数值为三次重复的各自P50/P95再取中位数，单位ms。全部阶段及单次重复保留于comparison.json。

| case/容量 | 缓存/投影/候选路径 | 前 P50/P95 | 后 P50/P95 | P50变化 |
| --- | --- | ---: | ---: | ---: |
| 6 / 64 MiB | off/full/fused | 39.20/49.65 | 38.50/48.75 | -1.8% |
| 6 / 64 MiB | off/full/separate | 33.48/56.94 | 36.17/48.20 | +8.0% |
| 6 / 64 MiB | off/projected/fused | 28.12/45.36 | 30.15/37.10 | +7.2% |
| 6 / 64 MiB | off/projected/separate | 44.85/63.66 | 31.31/43.62 | -30.2% |
| 6 / 64 MiB | on/full/fused | 39.07/50.34 | 39.27/41.82 | +0.5% |
| 6 / 64 MiB | on/full/separate | 39.66/47.87 | 33.73/40.86 | -15.0% |
| 6 / 64 MiB | on/projected/fused | 38.79/45.35 | 35.35/39.50 | -8.9% |
| 6 / 64 MiB | on/projected/separate | 36.86/41.44 | 30.39/38.77 | -17.5% |
| 31 / 8 MiB | off/projected/separate | 56.41/71.41 | 52.39/69.63 | -7.1% |
| 31 / 8 MiB | on/projected/separate | 59.36/82.67 | 49.46/67.75 | -16.7% |
| 32 / 64 MiB | off/projected/separate | 54.82/71.14 | 52.40/68.15 | -4.4% |
| 32 / 64 MiB | on/projected/separate | 63.79/85.30 | 41.31/48.25 | -35.2% |

64 MiB、K100开启缓存的P50下降35.2%、P95由85.30降至48.25ms；同组关闭缓存P50也下降4.4%。开启/关闭缓存P50比值由1.164变为0.788，即本次暖态从慢于关闭缓存转为快于关闭缓存。8 MiB对应比值由1.052变为0.944，但仍有换入换出：命中率0.1890、每查询读取324.395条向量；64 MiB命中率1、暖态不读向量。缓存条数/容量（8191/8388608字节、42785/43812864字节）及三次重复8 MiB暖态淘汰总量194637均保持相同。

容量组开启缓存时，计算进程峰值RSS前/后分别为8 MiB组425.86/412.16 MiB、64 MiB组426.17/426.45 MiB（各三次重复取最大值）。RSS包含索引、解释器及工作区，并非向量缓存预算；本次不宣称进程内存下降。

不能把端到端变化全部归因于本次优化：串行小样本off/projected/separate自身P50下降30.2%，on/full/fused反而增加0.5%；即使不遍历，缓存复制和不同RPC路径也仍有成本。容量组较稳定且与微基准方向一致，但当前顺序前后测不提供统计显著或普遍提速保证。

新增三项测试在实现前全部失败（缓存开/关describe遍历断言两项、缺少快照接口一项）；实现后62项缓存内核、并发、执行与控制定向回归通过，21.14秒。随后无PYTHONPATH、`VECSTOR_REQUIRE_GPU=1`完整回归841项通过，406.34秒；JUnit确认零失败、错误和跳过。后测、完整回归与最终业务源码SHA逐项一致。单次独立最终只读审查无Critical/Important/Minor发现，未追加第二次审查。审查时尚未结束的全套回归现已通过；未测的并发/GPU性能不外推。最终证据索引为`acceptance.json`，审查摘要为`review.md`。

## 解释边界

前/后轮按时间顺序运行，变体顺序固定随机种子，各轮保留缓存关闭控制；仍受调度、热状态和时间漂移影响。case 6 每阶段只有20条不同查询，分位数仅作描述，不声称统计显著。只针对既有 CPU 串行与容量退化，不替代并发4/8、GPU性能、更多数据分布或全部200查询矩阵。逐行复制、LRU锁操作及缓存换入换出仍有成本，不承诺所有暖态均快于关闭缓存。
