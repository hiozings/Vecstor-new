# P4 自适应查询策略选择实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有MVCC/持久工件/质量预算边界内，实现显式CPU `path=auto`，依据过滤比例、候选、传输与缓存成本选择具体执行路径，并完成独立正确性、质量和性能验收。

**Architecture:** 同租约绑定索引 → 逐候选资格解析 → 冻结成本策略＋有界本地观测＋O(1)scope快照 → 纯决策函数 → 既有具体路径；不新增存储统计RPC，不在线训练/试跑，不改变默认模式。

**Tech Stack:** 复用Conda Python3.12、NumPy1.26、DuckDB1.5.6、SQLGlot30.21、gRPC1.83、Faiss1.12和pytest；不新增运行时依赖。

**Spec:** [详细设计](../../design/p4-adaptive-query-strategy.md)，尤其第4节语义表、第5节输入来源、第6节风险/成本选择规则和第8—9节验收。**状态：2026-11-03起经用户授权实施，进度见末尾。** 当前已有基线为快照优化后841项回归；不能把它记作本计划通过。

## Global Constraints

- 默认CPU exact/filter-first；固定path不进规划或观测，策略服务启用时仅O(1)活跃向量计数增减；v1和普通SQL保持原语义。
- auto ANN首期只支持CPU、published已加载工件；无查询时训练/索引加载/全扫描统计探测；auto额外存储数据RPC=0。
- exact不能变ANN；approx/fallback=error不能主动exact；显式设备、质量、预算、投影、缓存和fused设置不得被成本模型放宽。
- 质量profile逐路径解析；nprobe=nlist不证明完整；每请求仍有租约、实际存储过滤和完整流/版本校验。
- `scope_snapshot`和规划不得遍历向量缓存/历史；历史256条×8KiB逻辑记账、TTL300秒；文件≤2MiB/64 workload/SQL≤64KiB；逻辑记账不是RSS硬限。
- 风险门槛建议1%（≥500个独立校准向量的95% Wilson上界）；切换间距max(2ms,参考下界10%)；auto FF临时工作集建议64MiB。均须原样记入配置/报告，后续修改需新策略版本和新留出验证。
- 超时/取消/DATA_LOSS不能转成成功；规划/反馈计入总deadline；EXPLAIN无扫描、缓存触碰或反馈，ANALYZE/shadow均执行一次。
- 无PYTHONPATH，完整回归要求真实GPU；性能串行独占运行，数据/参数/查询冻结、至少3重复；未测组标识清楚，不承诺预设提升倍数。
- 实施前按AGENTS核对Git授权和当前改动；新功能使用codex/分支，精确暂存。2026-11-03用户已授权实施、检查Git、创建功能分支和分阶段提交；不合并或推送。

## Review Focus

1. 同过滤比例但候选通过率相反：T4测试q≠s，候选耗尽风险不能被低延迟掩盖。
2. 单路径质量合格但按输入选择后组合质量下降：T7/T8单独冻结并评估完整选择策略，不能合并路径均值证明。
3. scope存在缓存但不属于当前向量/路径，或clear与请求完成交错：T2/T5复用证据必须匹配查询摘要、预算和起止generation。
4. 模型文件不匹配网络/线程/并发，但质量profile有效：T1/T3/T6分别验证只失效成本、准确活跃计数及退化原因。
5. 会话auto请求没有SQL SETTINGS、旧服务或同地址重启：T6能力及完成帧双重确认，不能只覆盖SQL显式设置。

## 文件职责与接口约定

下表新文件均为待创建，不为目录树提前创建空模块。`compute/`、`query/`等相对`src/vecstor/`。

| 文件 | 职责/任务 |
| --- | --- |
| 新`compute/planner_types.py`、`query/adaptive_policy.py` | 冻结数据契约、文件解析、部署指纹和模型覆盖；T1 |
| `cache/vector_cache.py`；新`compute/planner_observations.py` | O(1)scope快照/loss序号、固定schema有界历史；T2 |
| 新`compute/planning_inputs.py`；`query/quality_policy.py` | 同租约候选资格、公开profile单项查询、统计来源/预算解析；T3 |
| 新`compute/adaptive_planner.py` | 候选轮次/传输/缓存成本、风险分层、确定性选择；T4 |
| `compute/vector_execution.py`、`compute/data_reader.py`、`query/search_options.py` | 执行接入、内部auto契约、实际计划/有界反馈；T5 |
| `query/search_options.py/execution_settings.py`、`compute/sql_service.py/service.py`、`rpc/server.py/sql_client.py`、`shell/app.py/commands.py/completion.py` | 启动/请求/会话/能力/完成帧和活跃计数；T6 |
| 新`benchmarks/p4/adaptive.py/adaptive_cases.py/adaptive_costs.py` | 校准、冻结策略、配对成本/质量/消融、恢复运行；T7 |
| 新`scripts/check_p4_adaptive.py`、`rpc/p4_adaptive_acceptance.py` | PTY、双节点、重启/故障/Landlock验收；T8 |
| 新`docs/p4-adaptive.md`、`docs/validation/p4-adaptive.md`；README/AGENTS/CHANGELOG | 使用、证据、局限；T8 |

不需新增proto字段或RPC：现有path是字符串，能力和执行计划使用既有JSON承载。若实施中证据要求变更proto，先记录理由并生成全部pb2模块，禁止手改生成文件或改变旧字段含义。

T1定义以下冻结对象（tuple/不可变映射；无存储文件句柄/可变数组）。所有signature中的`context`指该类型，不是gRPC context：

- `DeploymentContext(hardware_fingerprint, network_class, active_vector_requests, concurrency_bucket)`；bucket∈{1,4,8,None}对应1、2—4、5—8、unknown。
- `PlanCandidate(candidate_id, mode, path, options: SearchOptions, quality_description, execution_fingerprint)`；path必须具体；quality_description缺失仅用于exact。
- `PlanningInputs(scope: DataScope, artifact_identity, table_rows, dimension, k, metadata_sql_hash, query_hash, candidates: tuple[PlanCandidate,...], reference_id, statistics, cache_snapshot, context: DeploymentContext, policy_id, policy_hash, remaining_ms)`；统计带source/known/区间，不用0表示unknown。
- `CandidateEstimate(candidate_id, eligible, reason, mean_ms, lower_ms, upper_ms, rpc_calls, payload_bytes, vectors_reranked, vectors_missed, workspace_bytes, failure_upper, components)`；unknown数值为None；components有界、最多32轮。
- `PlannerDecision(reference_id, proposed_id, executed_id, reason, estimates: tuple[CandidateEstimate,...], policy_id, policy_hash, mode)`。
- `ResolvedPlan(candidate: PlanCandidate, decision: PlannerDecision)`；现有执行器用candidate.options，原始请求保留在trace。

数据来源接口：`scope_snapshot(scope)->Mapping`；`ObservationStore.peek(key, now)->Observation|None`不更新LRU（给EXPLAIN），`get(key, now)`只在实际auto规划中触及该键；`record(observation, now)`固定大小单条更新，满时移除一条；`note_budget_failure(key, now)`仅记风险。基数卡片和完整查询复用卡片用不同key kind，共享256条上限。

依赖：T1→T2/T3→T4→T5→T6→T7→T8。每任务在完成契约后提交；不必为准备文件、CLI参数再拆无功能任务。下面命令均在项目根目录、vecstor环境、`env -u PYTHONPATH`下运行；涉及gRPC/GPU按项目要求使用可访问设备和localhost的环境。

## Task 1: 不可变契约和版本绑定策略文件

**Files:** 新`src/vecstor/compute/planner_types.py`、`src/vecstor/query/adaptive_policy.py`；新`tests/unit/test_adaptive_policy.py`。

**Interfaces:** 产出上述冻结类型；`AdaptivePolicy.load(path)->AdaptivePolicy`、`lookup(scope, artifact, metadata_sql, k, context)->PolicyMatch`。PolicyMatch区分cost匹配、基数匹配和profile映射，不能混成一个布尔值；`describe()`提供有限只读元信息。

- [x] RED：`test_policy_rejects_oversize_nonfinite_negative_and_duplicate`覆盖2MiB/64 workload/64KiB边界、NaN/Inf/负系数及重复ID；`test_cost_context_mismatch_does_not_erase_quality_mapping`验证网络/线程/并发不符仅失效成本；`test_persistent_stats_rebind_epoch_only_after_identity_match`验证epoch变化和revision/工件变化的差别；`test_planner_inputs_are_detached_and_frozen`禁止修改嵌套对象。
- [x] 运行`python -m pytest -q tests/unit/test_adaptive_policy.py`，预期因新接口缺失失败，记录失败原因。
- [x] 实现解析及有限schema。成本模型固定特征列由schema列举（公共调用、扫描M/N、候选U、各类RPC/B、selector、ANN轮次/探桶、rerank、cache copy/admit），禁止任意表达式执行；保存特征单位、非负系数、训练覆盖、残差和各输入统计来源。部署指纹缺失为unknown。
- [x] 重跑该文件，预期全通过；用1份最小合法fixture和所有边界fixture验证，不发布假实测系数。
- [x] 提交契约/解析/测试，建议`feat: define versioned adaptive planning policy contracts`。

## Task 2: O(1)scope快照和有界观测

**Files:** 修改`src/vecstor/cache/vector_cache.py`；新`src/vecstor/compute/planner_observations.py`、`tests/unit/test_planner_observations.py`；扩展`tests/unit/test_cache_process_snapshot.py`。

**Interfaces:** `VersionedVectorCache.scope_snapshot(scope)`返回scope条数/payload及process预算、活动工作区、admission_generation、loss_generation；不暴露内部dict。`ObservationStore(max_entries=256, entry_charge=8192, ttl_seconds=300)`及前述get/peek/record/note_budget_failure。`make_reuse_observation(inputs, trace, before, after)`返回标量证据或None，不保存ID/向量/结果。

- [x] RED：`test_scope_snapshot_without_entry_or_scope_iteration`用禁止遍历容器覆盖空/其他库/65535条；`test_loss_generation_tracks_evict_clear_close_not_hits`验证序号；`test_reuse_requires_same_query_budget_path_and_generations`覆盖clear在完成前后、淘汰、scan admission off、bypass和fused未匹配lookup；`test_history_bounds_ttl_peek_and_concurrent_eviction`覆盖257项、TTL=300边界、EXPLAIN peek无副作用及并发。
- [x] 运行新测试及快照测试，预期新接口失败；保留上一阶段process_snapshot已有测试。
- [x] 实现只用已有_scopes计数推导scope payload，loss序号在同锁更新。观测固定有界字段、查询摘要由已验证float32规范字节计算；record只处理一个键，满时一条LRU淘汰；get仅懒删除当前过期键。复用证据核验实际重排向量、准入/重复插入/服务数，不用全局hit差值。
- [x] 运行`python -m pytest -q tests/unit/test_planner_observations.py tests/unit/test_cache_process_snapshot.py tests/unit/test_vector_data_cache.py tests/unit/test_data_cache_concurrency.py`，预期全通过，原process统计字段不变。
- [x] 提交，建议`feat: add bounded version-scoped planner observations`。

## Task 3: 候选资格和同租约输入解析

**Files:** 新`src/vecstor/compute/planning_inputs.py`、`tests/unit/test_planning_inputs.py`；修改`src/vecstor/query/quality_policy.py`，扩展现有quality测试。

**Interfaces:** `QualityCatalog.describe_profile(profile_id)->Mapping|None`为公开O(1)单项访问；`qualify_candidates(plan, options, lease, pinned_cpu_entry, quality_catalog, policy_match)->tuple[PlanCandidate,...]`；`collect_planning_inputs(..., cache_snapshot, observation_store, deployment_context, remaining_ms)->PlanningInputs`。只接收已经打开的lease和已持引用entry，不自己调用RPC/registry.acquire。

- [x] RED：`test_exact_auto_never_qualifies_ann`；`test_error_fallback_and_fused_prune_exact`；`test_each_path_resolves_its_own_profile`；`test_explicit_profile_budget_conflict_is_not_cost_fallback`；`test_optional_profile_conflict_excludes_only_that_candidate`（全冲突INVALID_ARGUMENT）；`test_unknown_cardinality_does_not_scan_or_count`；`test_sf_matches_do_not_become_table_cardinality`；`test_corrupt_artifact_is_not_optimizer_downgrade`。
- [x] 运行`python -m pytest -q tests/unit/test_planning_inputs.py`，预期缺少接口失败。
- [x] 实现第4—5节资格与统计优先级。先构造具体path options再调用现有resolve；显式quality只映射其一条路径；不把policy profile映射直接当已校验证据。统计有效性与成本有效性分开；N缺失时不猜测。未知/冲突统计保守退化，但执行实际过滤不受影响。
- [x] 运行新文件及`tests/unit/test_quality_policy.py tests/unit/test_artifact_quality_policy.py`，预期全通过；用stub断言没有新增网络或跨层内部文件读取。
- [x] 提交，建议`feat: qualify adaptive candidates against leases and quality profiles`。

## Task 4: 纯成本估算、风险分层与确定性决策

**Files:** 新`src/vecstor/compute/adaptive_planner.py`、`tests/unit/test_adaptive_planner.py`。

**Interfaces:** `estimate_candidate(candidate, inputs, policy)->CandidateEstimate`；`choose_plan(inputs, policy, mode='apply')->PlannerDecision`，mode也支持shadow；无RPC、无数据库、无全局可变状态。候选最多exact/FF/SF三项，轮数≤32。

- [ ] RED：`test_low_selectivity_high_candidate_risk_prefers_filter`和`test_high_selectivity_prefers_search`使用确定性合成系数验证路径交叉；`test_equal_s_opposite_q_changes_choice`；`test_cache_reuse_reduces_miss_bytes_not_filter_rpcs`；`test_partial_misses_do_not_use_global_ceil_as_exact_rpc_count`；`test_rpc_wait_and_nested_faiss_times_are_not_double_counted`。
- [ ] 补RED：`test_error_fallback_risk_tier_precedes_latency`让10ms高风险SF对30ms低风险FF，必须选FF；all unknown时参考路径且reason明确；`test_explicit_budget_and_small_filtered_exact_branch_are_modelled`；`test_uncertain_interval_uses_reference`；`test_deadline_and_memory_exclusions`；`test_shadow_only_changes_executed_id`。
- [ ] 运行`python -m pytest -q tests/unit/test_adaptive_planner.py`，观察预期失败。
- [ ] 实现原候选扩展规则的有界预测，分别计U/M/V/miss/调用布局；fallback=error执行风险分层，fallback=exact计算已花费＋后续exact。以经验区间和差距规则选择；unknown不置零、不凭预测空集合返回答案。超deadline不伪造成功，预测超时不延长实际deadline。
- [ ] 重跑新测试并加入随机输入不变量检查（非负、有限、显式上限、选择只来自eligible、输入相同决定相同）；预期全通过。
- [ ] 提交，建议`feat: select CPU query paths with bounded cost and risk models`。

## Task 5: 执行器接入、实际计划和一次执行

**Files:** 修改`src/vecstor/compute/vector_execution.py`、`data_reader.py`、`sql_service.py`及`src/vecstor/query/search_options.py`；新`tests/integration/test_adaptive_execution.py`、`tests/integration/test_adaptive_failures.py`。

**Interfaces:** `execute_vector`新增可选`adaptive_policy=None, observations=None, deployment_context=None, adaptive_mode='apply'`注入，固定path不读取它们；lease/CPU引用取得后解析auto，得到ResolvedPlan再走现有具体分支。原requested options与resolved options分别记录；不把path='auto'传入ANN循环。反馈函数由执行器在完成校验后调用，总deadline仍有效。 此任务同时加入SearchOptions的auto组合校验，但SQL服务在完整planner尚未注入时明确拒绝auto（adaptive_planner_unavailable），不提前公布能力。T5集成测试直接调用注入式execute_vector连接真实存储RPC，固定参考查询可经既有客户端；完整客户端auto端到端在T6验证，避免依赖尚未开放的入口。

- [ ] RED：`test_auto_matches_selected_fixed_path_and_exact_truth`覆盖三种选择；`test_fixed_path_never_calls_planner_or_history`；`test_explain_peeks_without_scan_search_or_observation_update`；`test_analyze_and_shadow_execute_once`；`test_estimated_zero_still_checks_storage`；`test_failure_does_not_record_fast_success`（预算错误单独标记风险）。
- [ ] 补RED：`test_clear_during_query_cannot_publish_warm_reuse_hint`、`test_cancel_corrupt_rpc_and_expired_lease_preserve_errors`；模型预计FF小集合但实际不同只能改变成本预测，不能截断真实结果。
- [ ] 运行新文件，预期缺少auto入口/执行计划失败。
- [ ] 接入规划、planner_ms/feedback_ms、完整adaptive诊断；原质量/设备/fallback顺序的抽取仅限复用需要。固定路径保留原行为；EXPLAIN缺失质量仍能显示排除原因但不执行；ANALYZE/实际执行的硬错误保持原语义。
- [ ] 运行`python -m pytest -q tests/integration/test_adaptive_execution.py tests/integration/test_adaptive_failures.py tests/integration/test_data_cache_execution.py tests/integration/test_quality_profiles_rpc.py`，预期全通过，额外规划数据RPC=0。
- [ ] 提交，建议`feat: execute adaptive plans with traceable decisions and unchanged fallbacks`。

## Task 6: 用户入口、部署上下文和能力确认

**Files:** 修改T6职责表所列query/compute/rpc/shell文件；新`tests/integration/test_adaptive_controls.py`；扩展`tests/unit/test_search_options.py`、`tests/unit/test_execution_settings.py`。

**Interfaces:** 启动`--adaptive-policy`、`--adaptive-mode apply|shadow`、`--adaptive-network-class`；`.search path auto`及SQL SETTINGS path='auto'复用原SearchOptions.path字符串，无新请求proto字段。`SQLComputeService`提供finally安全的活跃向量计数和只读部署快照，auto规划前读取；质量与成本上下文匹配由T3处理。

- [ ] RED：验证auto+exact、auto+approx/CPU、auto+GPU拒绝、fused+auto只选SF、默认不变；`test_session_auto_checks_capability_without_settings`；`test_old_server_or_restarted_instance_cannot_fake_auto_ack`；`test_missing_adaptive_completion_is_error`；`test_ordinary_sql_does_not_require_adaptive_ack`。
- [ ] RED：`test_planned_exact_keeps_requested_quality_ack`覆盖内部profile清空但完成帧仍确认原requested_id；`test_active_count_includes_fixed_and_returns_to_zero_on_all_exits`，用屏障协调1个auto与2个fixed，快照=3/bucket4；失败、取消、EXPLAIN结束不泄漏；`test_missing_network_label_and_bad_policy_fail_startup`；`test_context_mismatch_keeps_quality_but_disables_cost`。
- [ ] 运行对应单元/集成文件，记录失败；再在T5内部契约与未就绪拒绝保护上实现参数/加载/Landlock顺序、SQL与会话覆盖、补全/帮助及能力缓存失效。策略启用才启用额外活跃计数，fixed无规划和观测写入。
- [ ] 重跑上述测试，外加现有`test_data_cache_controls.py`和GPU执行测试；预期原GPU固定路径不受影响；同地址重启需重新确认策略ID/mode。
- [ ] 在此集成点运行一次`VECSTOR_REQUIRE_GPU=1 python -m pytest -q`完整回归，预期零失败/跳过；随后提交，建议`feat: expose opt-in adaptive CPU planning with capability acknowledgements`。

## Task 7: 独立校准、冻结策略和配对实验工具

**Files:** 新`benchmarks/p4/adaptive.py`、`adaptive_cases.py`、`adaptive_costs.py`、`tests/integration/test_adaptive_benchmark.py`、`tests/unit/test_adaptive_costs.py`；复用已有`data_reads.py`/延迟代理/工件质量工具，不修改已完成实验的历史结果。

**Interfaces:** `build_policy(calibration_dir, output_path)`只消费完整已验证训练报告；`run_adaptive_matrix(output_dir, tier, policy_path, case_ids=None, repeats=3)`；CLI分`prepare`（通过RPC准备数据/工件/质量与成本校准）、`evaluate`（冻结策略后的质量/成本）、`ablate`。每个产物manifest有完整源码/输入hash及已完成单元，输出目录不覆盖；恢复必须匹配manifest，不能拼接不同源码结果。

- [ ] RED：`test_cost_fit_prunes_redundancy_and_rejects_unusable_models`、`test_dataset_query_splits_and_frozen_policy_hash`、`test_repeat_count_is_not_independent_risk_sample_count`、`test_unsupported_quality_cell_is_not_exact_success`、`test_small_exact_probe_has_no_p95`、`test_resume_rejects_changed_manifest`。
- [ ] smoke测试使用1024行/4维、2查询，不承诺质量/尾延迟；检查至少3重复、固定/auto同参数、实际执行只一次、正确性在计时区间外校验、资源与源码记录、所有进程回收。先运行新文件，观察缺少入口失败。
- [ ] 实现有界特征提取：优先现有trace的互不重叠时间/计数；不能从rpc_wait减出“真实网络RTT”。先合并常量及线性依赖列，再用归一化非负最小二乘拟合，列出保留列映射、秩/残差/覆盖失败，未达要求的格子不导出模型。风险分层用独立尝试总数和错误计数，不使用只保留成功的样本。额外输入维度未被样本辨识时标unknown。
- [ ] 准备组件质量校准/留出、成本拟合128向量（96拟合/32内部验证；相对绝对误差中位数≤20%、p90≤40%才发布该桶；覆盖固定路径/既定缓存开关及冷暖条件，未变化的特征不得假称已拟合）、最终策略留出500（200计时），最终集合与其他集合摘要严格不重叠。可复用已冻结且匹配工件的组件profile，必须校验原始证据；auto整体质量仍独立验证。
- [ ] 运行上述单元及smoke集成测试，预期全通过；验证9组正式CPU/最多32400 ANN及auto请求、270条exact探针的manifest计数，full exact200入口默认关闭。原始记录保留预测/实际、错误、资格排除及NOT_RUN。
- [ ] 提交工具与测试，建议`bench: add frozen-policy adaptive quality and CPU cost comparisons`。

## Task 8: 双节点/重启验收、正式实验、审查及交付

**Files:** 新`scripts/check_p4_adaptive.py`、`src/vecstor/rpc/p4_adaptive_acceptance.py`、`tests/integration/test_p4_adaptive_acceptance.py`、`docs/p4-adaptive.md`、`docs/validation/p4-adaptive.md`；更新本计划、设计状态、AGENTS、README、CHANGELOG。

**Interfaces:** `run(output_dir, rows=10000)->report`，输出A01—A12证据映射、节点各自策略/工件/epoch与实际路径；使用已有ServiceProcess、SQL客户端和Landlock探针，不共享目录读取存储文件。

- [ ] RED：小规模进程验收测试覆盖PTY设置auto、两个节点不同缓存选不同路径但同版本、持旧租约并发写、新工件发布、计算重启和存储同端口重启、失败/取消与资源清理；运行`python -m pytest -q tests/integration/test_p4_adaptive_acceptance.py`观察失败。
- [ ] 实现脚本并重跑，预期全通过；独立运行1万/10万生命周期，输出目录必须不存在；不把统计重建等同于索引重新训练。
- [ ] 先各类10次pilot，报告预计工时和数据量，再执行设计§9首批矩阵、500向量完整策略质量验收及规划微基准；若模型或质量不达标，保留失败证据，修改策略版本后使用新的留出集，禁止在原评估集反复调参。
- [ ] 对路径交叉点和容量场景按分批矩阵补并发/延迟/独立热点及四项成本输入消融；未执行部分逐格NOT_RUN。对比固定exact/FF/SF、s阈值、完整auto；成功/失败和exact回退分开统计。
- [ ] 完成一次独立最终只读审查。阻断问题先RED再修复；业务源码改变后重跑受影响实验/完整回归，不能沿用旧SHA结果。无改动时不重复跑同一套昂贵矩阵。
- [ ] 最终运行`env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 conda run --no-capture-output -n vecstor python -m pytest -q --junitxml=results/p4-adaptive-final/pytest.xml`；目录事先创建但不得覆盖旧证据。预期全部通过、零跳过，记录真实数量，不预填841。
- [ ] 汇总质量和性能护栏：planner＋feedback串行P95≤0.5ms且≤该组P50的2%；auto每格P50/最佳固定≤1.10、P95≤1.20；明显差距≥20%且证据充分组低成本选择≥90%。未过则标未通过策略发布验收，不把功能通过混为性能通过。
- [ ] 更新用户说明、路径/设备支持矩阵、统计失效/模型上下文、复现入口和局限；确认源码/策略/结果SHA一致后分阶段提交。没有另行授权，不合并、不推送、不默认启用auto。

## 交付检查与当前记录

- 设计§1—4→T1/T3/T5/T6；§5→T2/T3；§6→T1/T4/T7；§7→T5/T6；A01—A12→T1—T8对应测试；§9→T7/T8。接口类型及策略/历史上限在各任务保持一致。
- 2026-10-23：已核对最新O(1)快照、执行器、质量解析、租约/存储过滤、客户端确认和历史成本。一次独立设计可实施性审查提出“fallback=error风险优先级”和“部署上下文来源”两个问题，已在设计§5.5/§6及T1/T4/T6落实。
- 当前未实现任何auto业务入口，未跑本计划测试或校准；设计中的阈值、样本和任务均是待实施约定，不构成已通过的结果。

## 实施记录（2026-11-03）

- T1：不可变契约、策略严格解析与版本/部署匹配完成；13项测试先因缺少模块失败，后全部通过（0.05秒）。本任务不开放auto入口；完整初始回归仍在运行。

- 初始真实GPU全套基线841通过（544.58秒、零跳过、无PYTHONPATH）。T2新增scope快照/丢失序号和256条有界观测；四项scope测试先失败，观测/缓存/T1合计65项通过（0.36秒）。同时补齐T1可变数组拒绝测试，先失败后修复。

- T3：同租约候选资格、逐路径质量解析、基数来源及缓存/风险观测输入完成；31项规划/既有质量测试通过（0.43秒）。补充缺失索引且显式fused时仍保留既有exact回退，失败复现后修复；此为不可执行ANN的结构性回退，不是成本驱动主动exact。
