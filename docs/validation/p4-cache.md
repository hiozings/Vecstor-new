# P4 有界数据缓存与传输优化验收（2026-10-19）

状态：**缓存与传输子阶段T1—T8按用户选择的代表性实验范围完成**。审查修复后完整回归838项通过；CPU/GPU代表性38场景、630次配置、33744条计时查询全部通过。原完整200查询矩阵仍未运行。

## 范围和环境

实现计算侧按storage epoch/database/table/revision/schema/dimension/ID隔离的原始向量LRU、miss批量读取、按需元数据、选择性fused响应、明确开关/完成确认，以及读租约/清理/并发/重启故障保护。默认cache关闭、metadata full；exact全量扫描默认lookup-only，不自动填充。

用户已明确选择本轮运行**代表性正式矩阵**，保留完整correctness/costs/gpu入口。未执行原200查询全矩阵，不能据此宣布全部实验空间或整个P4完成；自适应策略、增量索引维护、跨节点缓存同步仍未实现。

硬件为Intel Core i9-14900HX（32逻辑CPU）、RTX4060 Laptop 8188MiB、驱动615.78.08、CachyOS Linux7.2.9-2；任务结束后的只读环境快照在results/p4-cache-final-20261019/environment.json。环境沿用[迁移记录](../environment.md)：Conda vecstor、锁定Python/Faiss/DuckDB/gRPC、RTX4060 Laptop，未安装新依赖或改驱动。所有回归/实验清除PYTHONPATH；真实GPU检查不以CPU-only替代。实现分支codex/p4-data-cache-transfer；审查修复业务/实验版本为9362353。

## 证据入口

| 路径（相对于项目根） | 实际状态 |
| --- | --- |
| results/p4-cache-10k-20261019 | 5f546c4生命周期：24项判据通过，9.99秒，无残留进程 |
| results/p4-cache-100k-20261019 | 5f546c4生命周期：24项判据通过，60.38秒，无残留进程 |
| results/p4-cache-gpu-20261019 | 5f546c4生命周期：26项判据通过，10.94秒，真实GPU和工件拒读，无残留进程 |
| results/p4-cache-data-gpu-20261019 | 审查前bee1270：48次配置/1920计时查询通过，267.93秒；保留历史，不作为最终修复版本结果 |
| results/p4-cache-data-cpu-20261019-final | 9362353：582次配置、31824条计时查询通过，2223.60秒，无跳过/残留进程 |
| results/p4-cache-data-gpu-20261019-final | 9362353：48次配置、1920条计时查询通过，271.05秒，无跳过/残留进程 |
| results/p4-cache-final-20261019 | 838项完整回归、C01—C15索引、summary.json、完整汇总表及原始记录核对脚本 |

首次正式入口因scripts/vecstor.py遮蔽包，在创建实验目录之前失败；独立CLI测试复现并按现有脚本的源码路径引导方式修复。未通过设置PYTHONPATH掩盖问题。审查前阶段全套819通过（354.97秒）；这也不是最终HEAD完整回归。

最终命令：

```bash
env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 \
  conda run --no-capture-output -n vecstor python -m pytest -q \
  --junitxml=results/p4-cache-final-20261019/pytest.xml
```

最终完整回归 **838通过，414.25秒，零失败/零跳过**。其JUnit已逐项核对下表用例；正式报告和逐查询原始记录另经summarize.py核对630个配置组合、33744条equivalent记录、零incomplete及全部缓存记账上限。两个最终实验的源码SHA集合与回归冻结集合完全相同。

协议已通过独立临时目录重新生成并逐字节核对四个生成文件；正式业务源码不被此检查改写。

## C01—C15证据映射

以下C01—C14均由最终JUnit和注明版本的生命周期报告确认通过；C15按用户授权的代表性范围通过。机器可读索引为results/p4-cache-final-20261019/acceptance-matrix.json。

| 编号 | 判据 | 主要测试/脚本 |
| --- | --- | --- |
| C01 | 关闭路径、普通CRUD与确认兼容 | test_request_off_does_not_touch_warm_cache_and_full_overrides_projected；test_cache_session_settings_do_not_require_vector_ack_for_crud；既有P1—P4回归 |
| C02 | 冷/暖/部分命中，实际RPC和字节减少 | test_cold_warm_queries_save_actual_vector_rpc；test_partial_hits_across_larger_candidate_budget；test_copy_owns_only_one_vector；正式生命周期 |
| C03 | 命中仍远程过滤/验证所有ID | test_other_predicate_still_filters_cached_vectors；test_cached_unknown_id_never_skips_storage_existence_check |
| C04 | 写入、跨库、DROP重建与旧读 | test_write_and_other_table_change_cannot_reuse_cached_revision；test_drop_recreate_and_second_database_do_not_hit_old_scope；test_old_warm_query_remains_bound_across_write |
| C05 | epoch隔离、计算重启、双节点冷热 | test_storage_restart_changes_epoch_even_same_revision；test_live_compute_rejects_same_revision_bytes_after_storage_restore；正式生命周期two_node_warm_cold/restart_empty_cache/storage_epoch_invalidates_resident |
| C06 | 同revision工件切换、损坏不能绕过 | test_new_artifact_same_revision_reuses_data_but_corrupt_never_bypassed；既有P4工件确认回归 |
| C07 | 预算/LRU/所有权、clear、并发、工作区 | test_budget_and_lru；test_entry_limit_and_empty_scope_release；test_clear_during_download_cannot_refill；test_parallel_miss_is_not_single_flight；test_workspace_exhaustion_bypasses；test_clear_while_vector_rpc_in_flight_cannot_refill |
| C08 | 字段/ID/交集/向量/流完整性 | test_corrupt_projected_candidates_rejected；test_incomplete_or_invalid_scan_rejected；test_corrupt_vector_rpc_cannot_poison_cache |
| C09 | 全命中也受TTL/取消/完成期限约束 | test_cache_deadline_uses_send_time_and_granted_ttl；test_warm_cache_does_not_allow_expired_final_result；test_warm_query_actual_rpc_cancellation |
| C10 | id-only/别名/*/过滤/排序等价 | test_projection_keeps_predicate_and_source_aliases；test_projection_star_and_aliases_keep_all_output_fields；test_no_vector_projection_does_not_materialize_candidate_lists；正式8开关对照 |
| C11 | fused交集、拆分、容量隔离 | test_fused_cold_warm_partial_only_returns_missing_vectors；test_projected_separate_wide_candidates_split_before_topk；test_projected_separate_single_oversized_metadata_fails；test_projected_candidates_share_slots_but_vectors_remain_available |
| C12 | 扫描准入、两ANN路径与精确回退 | test_exact_scan_lookup_only_and_admission_switch；test_exact_warm_scan_with_sufficient_capacity；既有P3两路径/候选预算回归；代表性correctness-probe |
| C13 | EXPLAIN/ANALYZE、请求统计与能力 | test_explain_never_touches_cache_and_analyze_executes_once；test_parallel_request_stats_do_not_use_global_deltas；test_lazy_storage_capability_rpc_is_accounted；test_cache_completion_ack_and_instance_invalidation |
| C14 | PTY、独立节点、Landlock、真实GPU | test_cache_acceptance_real_terminal_nodes_and_restarts；test_gpu_cached_projected_fused_and_cpu_fallback；三个正式生命周期报告 |
| C15 | 独立开关、配对实验和真实资源 | test_data_read_smoke_pairs_variants_and_keeps_raw_resources；test_proxy_preserves_read_responses_and_delay；test_full_hotset_contains_160_requests_across_all_twenty_hot_queries；最终代表性CPU/GPU报告（通过，完整tier仍NOT_RUN） |

同revision恢复的exact测试证明原始数据缓存隔离；不把它用作仍驻留local ANN的恢复证明。正常published恢复要求数据库和权威工件来自一致备份。进程重启与注入故障不等同于断电耐久性证明。

## 代表性矩阵与测量方法

manifest共38个场景、630次配置运行、33744条计时查询；CPU编号0—35，GPU编号36—37。每个配置三次重复。主CPU成本为1万/10万×独立/相关×并发1/4/8，各阶段20查询、8个cache/projection/separate-fused组合。另有exact/filter-first每阶段1查询的正确性探针、选择性负载、每阶段200查询的8/64MiB容量组、0/1/5ms单向延迟，以及GPU两个原已支持冻结负载。

每个变体启动新计算进程、加载同一权威工件；客户端预热一次后清应用缓存，然后按cold/warm/independent/hotset序列执行。cold不代表冷OS页缓存/冷索引。warm重复cold查询，independent使用不同seed，hotset中80%请求来自20条热点池；20样本阶段仅有16次热点请求，不能在单阶段覆盖20个热点，完整200样本生成已验证160次热点请求覆盖全部20条。

配置顺序固定seed随机化。参考进程在计时前退出，避免其额外CPU/GPU索引影响测量；答案和独立NumPy真值事先生成。正确性核对在吞吐计时之外，逐查询保留结果/计划、过滤/float64距离、与关闭优化基线的答案及实际模式一致性。CPU稀疏search-first仍可能精确回退，报告实际数量，不把回退结果称为纯ANN性能。

GPU先对对应新工件重新验证500条冻结留出，再带绑定工件的v3质量配置执行对照；既有留出复用属于一致性回归，不是新泛化证据。CPU合成数据与GPU冻结校准数据不同，不能把两张成本表解释为同负载CPU/GPU速度比较。

RPC字节为实际Protobuf序列化大小；命中向量裸字节推算单列，不等于网卡流量。lookup/copy等计时可能嵌套，不可相加。延迟代理仅增加转发前和首响应前的固定传播延迟，不模拟带宽、拥塞或真实跨机环境；BeginRead往返包含存储执行。0ms代理用于区分代理本身开销。

RSS/CPU每5ms采样，可能漏峰。新compute进程的资源可按变体观察；storage跨变体复用，其CPU值为累计生命周期计数，不直接当作变体CPU差。代理和实验驱动共进程，资源项明确含驱动成本。缓存记账容量和工作区预算不是整个进程RSS硬限。

### 最终结果：减少传输，不代表所有负载更快

以下均为warm阶段；P50/P95是三次重复各自分位数再取中位数，字节/RPC是按实际请求数加权平均。20查询成本组只有20个不同输入、重复三次，分位数为描述性结果，不声称统计显著或普遍提速。完整六种对照汇总见results/p4-cache-final-20261019/summary-tables.md，所有八种组合原始记录仍保留。

CPU：10万行、独立分布、100%通过率、K10。每格P50/P95，单位ms：

| 并发 | off/full/separate | on/full/separate | on/projected/separate | off/full/fused | on/projected/fused |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 32.57 / 48.64 | 36.96 / 47.74 | 36.49 / 44.40 | 42.71 / 55.57 | 29.13 / 46.52 |
| 4 | 61.82 / 82.11 | 40.29 / 67.71 | 40.06 / 49.21 | 112.47 / 147.19 | 38.99 / 57.91 |
| 8 | 113.93 / 119.59 | 89.59 / 111.87 | 83.26 / 104.63 | 212.11 / 232.62 | 92.76 / 112.99 |

对应separate路径，缓存关闭/full为4 RPC、39456存储Protobuf字节/请求；缓存开启/full为3 RPC、6195字节；缓存开启/projected为3 RPC、4412字节，三种并发下warm向量读取均从64行降为0。串行cache/full的P50仍从32.57升至36.96ms；减少RPC或字节不能保证延迟下降。fused旧路径与selective新路径的SQL读取形态不同，不能把其差异全部归因于网络往返。

GPU：10万行、独立分布、并发1，两个冻结支持负载：

| 通过率/K | 配置 | P50/P95 ms | RPC/请求 | 存储Protobuf字节/请求 |
| --- | --- | ---: | ---: | ---: |
| 100%/100 | off/full/separate | 41.85 / 55.79 | 4 | 75112 |
| 同上 | on/full/separate | 38.55 / 48.51 | 3 | 8723 |
| 同上 | on/projected/separate | 35.29 / 48.36 | 3 | 5147 |
| 同上 | on/projected/fused | 42.57 / 48.59 | 3 | 5147 |
| 10%/10 | off/full/separate | 43.82 / 50.76 | 4 | 19105 |
| 同上 | on/full/separate | 40.49 / 53.51 | 3 | 5962 |
| 同上 | on/projected/separate | 39.12 / 44.90 | 3 | 5266 |
| 同上 | on/projected/fused | 41.96 / 50.86 | 3 | 5921 |

缓存开启后上述warm向量读取均为0。窄过滤fused的lookup命中率约0.0982，因为查找分母包含最终未通过WHERE的候选；这不等于只有9.82%的实际重排向量来自缓存。两个完整500留出工件验证平均Recall@K分别为100%/K100的0.98266、10%/K10的0.98560，均无精确回退/不完整；成本阶段v3质量配置实际应用。它们不是全部GPU负载覆盖或逐查询召回保证。

### 容量与延迟边界

CPU10万行、K100、projected/separate、并发1；每阶段200查询，三次重复：

| 容量/缓存 | warm P50/P95 ms | 读取向量/请求 | lookup命中率 | warm淘汰总数（3次） |
| --- | ---: | ---: | ---: | ---: |
| 8MiB/off | 54.44 / 69.54 | 400 | — | 0 |
| 8MiB/on | 63.51 / 86.29 | 324.39 | 0.1890 | 194637 |
| 64MiB/off | 56.83 / 74.94 | 400 | — | 0 |
| 64MiB/on | 63.03 / 84.34 | 0 | 1.0000 | 0 |

8MiB最终8191条、记账8388608字节；64MiB最终42785条、记账43812864字节（约41.78MiB）。64MiB可容纳这组ANN热工作集，warm存储字节由215478降为8280/请求，但P50仍退化。每条查询的全缓存统计遍历、逐行复制和锁操作是潜在成本，当前没有独立消融把退化全部归因于其中某一项。exact全表扫描的工作集更大，不能从ANN热工作集推断64MiB能容纳整个10万行表；产品默认exact不填缓存。

延迟代理组固定10万行、K10、projected/separate、并发1：

| 注入单向时延 | off warm P50/P95 ms | on warm P50/P95 ms | BeginRead往返中位数ms |
| --- | ---: | ---: | ---: |
| 0ms | 50.20 / 75.81 | 49.64 / 64.42 | 3.55 |
| 1ms | 61.88 / 72.31 | 51.39 / 66.40 | 7.87 |
| 5ms | 110.96 / 116.97 | 86.65 / 91.74 | 19.32 |

注入组都经同一代理，0ms代理并非直连，因此不把它与直接连接的P50差异归因于缓存。RPC字节统计仍是计算端逻辑存储调用，不把代理下游重复传输相加冒充网卡流量。相同200-query完整延迟矩阵未运行。

### 资源、回退与未覆盖范围

CPU各配置compute采样峰值范围216.2—432.5MiB、storage353.1—506.3MiB；GPU对应compute393.6—430.4MiB、storage423.9—439.3MiB、GPU worker705.3—707.6MiB。整卡显存采样最大766.9MiB（包含桌面等程序）。新增批工作区最大记账16MiB，全部缓存记账检查未超配置上限。

这里是每配置采样峰值的范围，不能相加为同步峰值。采样不含此前导入/构建/参考校验的全部峰值，GPU worker在加载取得PID后才监测；不能与索引生命周期的全程峰值直接比较。每个配置仅一个计算进程，多会话并发1/4/8；双节点一致性另由生命周期验收证明，不用本表宣称双节点吞吐扩展。

33744条计时查询中有960条请求approx但实际精确回退，均来自低选择性search-first组；这些不是纯ANN性能样本。所有配置与关闭优化的冻结基线答案/实际模式相同，过滤与float64距离一致，零不完整结果。exact/filter-first探针每阶段仅1条查询，不输出P95、不作为广泛延迟结论。

未运行：原全部200查询correctness/costs/gpu tier，特别是1%/50%过滤率的正式成本组合、更广泛GPU并发/查询集合，以及真实跨机、带宽/拥塞和冷OS页缓存实验。主成本只有20条不同查询、三次重复；容量组200条不能替代其他组样本。变体顺序随机化但场景按固定顺序运行，仍有调度、热状态及时间漂移限制。保持默认关闭，不根据这些结果自动启用全部优化。

## 审查修复、取舍及待优化项

一次独立只读审查：无Critical，1项Important和3项Minor；其中projected separate大响应缺少二分为功能问题。该问题及协议标记、热点生成偏差均已先失败复现后修复，27项定向及后续838项完整回归通过，未追加第二次独立审查。后两项按实际研究影响提升为必须修复：不能把新RPC标成legacy，也不能把16热点负载称为20热点。

保留一项非阻断优化：每请求describe仍为了process计数调用完整选库stats，在全局锁内遍历缓存；最终实验包含此开销。后续可增加常数时间process快照，不在本轮隐藏这项潜在退化。

实施决策及风险：

1. 在用户授权的新分支复用当前目录，保留editable注册与既有结果；并发用户改动须继续精确暂存。
2. 独立内核阶段以定向检查推进，集成阶段和最终版本运行完整真实GPU回归；不能用定向通过替代最终全套。
3. TopK内部None保留旧full急切物化，显式bool启用projected保留K行逻辑；三态接口防止SELECT *与旧路径混淆。
4. 二分前释放失败父批工作区，失败lookup次数单列，成功逻辑候选不双计；拆分本身有真实额外RPC/lookup成本。
5. 参考节点退出、会话预热后清缓存、验证移出吞吐计时；结果只描述应用冷/暖缓存，不能称冷启动性能。
6. 预先计算数据ID位置，避免每查询在实验驱动重建10万行映射而污染吞吐。
7. local ANN在离线恢复复用revision后仍须显式清理重建或重启；忽略该既有前提可能用到旧local索引。epoch隔离只作用于本期数据缓存，不改变IndexKey。
8. published恢复须保持权威数据库与工件一致；人工改写文件不在epoch完整性保证内。
9. 完整200查询tier明确NOT_RUN，用户选择的代表性结果不能支持全矩阵结论。
10. 不扩大为RSS硬限、自动策略、增量维护、TLS/鉴权；仍遵循单存储、可信网络和显式资源边界。
