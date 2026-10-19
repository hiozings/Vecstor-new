# P4 有界数据缓存与传输优化验收（2026-10-19）

状态：最终完整回归838项通过；修复后代表性实验进行中。T1—T7已验证；本页只在证据完整后更新为完成，不把审查前实验当作最终版本结果。

## 范围和环境

实现计算侧按storage epoch/database/table/revision/schema/dimension/ID隔离的原始向量LRU、miss批量读取、按需元数据、选择性fused响应、明确开关/完成确认，以及读租约/清理/并发/重启故障保护。默认cache关闭、metadata full；exact全量扫描默认lookup-only，不自动填充。

用户已明确选择本轮运行**代表性正式矩阵**，保留完整correctness/costs/gpu入口。未执行原200查询全矩阵，不能据此宣布全部实验空间或整个P4完成；自适应策略、增量索引维护、跨节点缓存同步仍未实现。

环境沿用[迁移记录](../environment.md)：Conda vecstor、锁定Python/Faiss/DuckDB/gRPC、RTX4060 Laptop，未安装新依赖或改驱动。所有回归/实验清除PYTHONPATH；真实GPU检查不以CPU-only替代。实现分支codex/p4-data-cache-transfer；审查修复业务/实验版本为9362353。

## 证据入口

| 路径（相对于项目根） | 实际状态 |
| --- | --- |
| results/p4-cache-10k-20261019 | 5f546c4生命周期：24项判据通过，9.99秒，无残留进程 |
| results/p4-cache-100k-20261019 | 5f546c4生命周期：24项判据通过，60.38秒，无残留进程 |
| results/p4-cache-gpu-20261019 | 5f546c4生命周期：26项判据通过，10.94秒，真实GPU和工件拒读，无残留进程 |
| results/p4-cache-data-gpu-20261019 | 审查前bee1270：48次配置/1920计时查询通过，267.93秒；保留历史，不作为最终修复版本结果 |
| results/p4-cache-final-20261019 | 最终完整回归、C01—C15证据索引和代表性实验汇总（进行中） |

首次正式入口因scripts/vecstor.py遮蔽包，在创建实验目录之前失败；独立CLI测试复现并按现有脚本的源码路径引导方式修复。未通过设置PYTHONPATH掩盖问题。审查前阶段全套819通过（354.97秒）；这也不是最终HEAD完整回归。

最终命令：

```bash
env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 \
  conda run --no-capture-output -n vecstor python -m pytest -q \
  --junitxml=results/p4-cache-final-20261019/pytest.xml
```

最终完整回归 **838通过，414.25秒，零失败/零跳过**。其JUnit已核对上述用例；C15仍须等待正式实验完成，不以工具smoke测试代替性能证据。

协议已通过独立临时目录重新生成并逐字节核对四个生成文件；正式业务源码不被此检查改写。

## C01—C15证据映射

矩阵最终须由真实JUnit和正式报告确认；以下列出判据与对应测试，不能只凭测试名认为已经通过。

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
| C15 | 独立开关、配对实验和真实资源 | test_data_read_smoke_pairs_variants_and_keeps_raw_resources；test_proxy_preserves_read_responses_and_delay；最终代表性CPU/GPU报告（进行中） |

同revision恢复的exact测试证明原始数据缓存隔离；不把它用作仍驻留local ANN的恢复证明。正常published恢复要求数据库和权威工件来自一致备份。进程重启与注入故障不等同于断电耐久性证明。

## 代表性矩阵与测量方法

manifest共38个场景、630次配置运行、33744条计时查询；CPU编号0—35，GPU编号36—37。每个配置三次重复。主CPU成本为1万/10万×独立/相关×并发1/4/8，各阶段20查询、8个cache/projection/separate-fused组合。另有exact/filter-first每阶段1查询的正确性探针、选择性负载、每阶段200查询的8/64MiB容量组、0/1/5ms单向延迟，以及GPU两个原已支持冻结负载。

每个变体启动新计算进程、加载同一权威工件；客户端预热一次后清应用缓存，然后按cold/warm/independent/hotset序列执行。cold不代表冷OS页缓存/冷索引。warm重复cold查询，independent使用不同seed，hotset中80%请求来自20条热点池；20样本阶段仅有16次热点请求，不能在单阶段覆盖20个热点，完整200样本生成已验证160次热点请求覆盖全部20条。

配置顺序固定seed随机化。参考进程在计时前退出，避免其额外CPU/GPU索引影响测量；答案和独立NumPy真值事先生成。正确性核对在吞吐计时之外，逐查询保留结果/计划、过滤/float64距离、与关闭优化基线的答案及实际模式一致性。CPU稀疏search-first仍可能精确回退，报告实际数量，不把回退结果称为纯ANN性能。

GPU先对对应新工件重新验证500条冻结留出，再带绑定工件的v3质量配置执行对照；既有留出复用属于一致性回归，不是新泛化证据。CPU合成数据与GPU冻结校准数据不同，不能把两张成本表解释为同负载CPU/GPU速度比较。

RPC字节为实际Protobuf序列化大小；命中向量裸字节推算单列，不等于网卡流量。lookup/copy等计时可能嵌套，不可相加。延迟代理仅增加转发前和首响应前的固定传播延迟，不模拟带宽、拥塞或真实跨机环境；BeginRead往返包含存储执行。0ms代理用于区分代理本身开销。

RSS/CPU每5ms采样，可能漏峰。新compute进程的资源可按变体观察；storage跨变体复用，其CPU值为累计生命周期计数，不直接当作变体CPU差。代理和实验驱动共进程，资源项明确含驱动成本。缓存记账容量和工作区预算不是整个进程RSS硬限。

正式结果将在修复后CPU/GPU报告都完成后写入本节；不提前采用审查前GPU结果填充最终表格。

## 审查修复、取舍及待优化项

一次独立只读审查：无Critical，指出projected separate大响应缺少二分。该问题及协议标记、热点生成偏差均已先失败复现后修复，27项定向验证通过。后两项按实际研究影响提升为必须修复：不能把新RPC标成legacy，也不能把16热点负载称为20热点。

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
