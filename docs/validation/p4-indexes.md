# P4 索引持久化与版本发布验收（2026-10-18）

本记录覆盖P4的九任务**索引子阶段**：CPU权威工件、原子发布、读租约绑定、双计算节点RPC加载、批量写入后完整重建、零训练恢复、GPU重克隆、v3质量绑定和显式回收。数据缓存、自适应策略、表级版本、增量维护和多存储共识仍未实现。

## 环境、版本与复现

迁移环境为CachyOS/Linux 7.2.9-2-cachyos、Miniforge/Conda 26.7.2、RTX 4060 Laptop 8188MiB、驱动615.78.08、Landlock ABI10。78个Conda包与精确锁一致，Python3.12.15/Faiss1.12.0/DuckDB1.5.6等版本未升级。只以 `python -m pip install --no-deps --no-build-isolation -e .` 补注册项目；补注册后不设置PYTHONPATH的当时完整真实GPU回归632项通过，详见[环境迁移记录](../environment.md)。

实施分支为`codex/p4-index-publication`；各任务分阶段提交。核心规模/GPU验收先于内存观测修正；成本与500留出质量验证的源码版本为`f3a00c90d79ca3cfbe183097eadfa4e9d35b00df`，各目录还保存源码摘要。其后审查修复了目录同步重试、删除表回收和上传清理重试，额外fsync可能影响耗时，以下成本不能当作修复后源码的重新测量。

最终回归命令（需真实GPU和localhost权限；后补测试可用同环境运行tests/integration/test_p4_preload_processes.py）：

```bash
env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 \
  conda run --no-capture-output -n vecstor python -m pytest -q \
  --junitxml=results/p4-final-20261018/pytest.xml
```

最终完整回归 **687通过，277.35秒，零失败/零跳过**；其后仅增加四项独立进程测试，**4通过，8.05秒**（supplement.xml），业务源码不变。合计691项测试证据，未将两次运行描述成一次691项全套运行。A01—A20机器可读索引为results/p4-final-20261018/acceptance-matrix.json，已逐项核对真实测试结果。四个pb2/grpc生成模块已重新生成并逐字节一致。规模、成本和工件质量的复现入口见[实验说明](../../benchmarks/p4/README.md)，所有输出目录必须新建，原始产物受Git忽略。

## 实际证据

| 原始目录（项目根下） | 实际结果和范围 |
| --- | --- |
| results/p4-indexes-10k-20261018 | 1万×128，36个生命周期判据通过，9.854秒 |
| results/p4-indexes-100k-20261018 | 保留失败：PTY导入默认15秒超时并回滚，未进入索引发布；进程已清理 |
| results/p4-indexes-100k-20261018-retry1 | 10万×128，36个判据通过，64.426秒；管理导入显式180秒，发布/加载仍≤60秒 |
| results/p4-indexes-gpu-20261018 | 1万×128，38个判据通过，10.593秒，真实GPU及工件拒读验证 |
| results/p4-lifecycle-costs-20261018 | 12次完整生命周期，1万/10万、独立/相关分布、每组3次；624个计时查询全部通过 |
| results/p4-quality-20261018 | CPU/GPU各500条完整冻结留出；新v3配置在重启预加载后实际应用 |
| results/p4-final-20261018 | 最终完整pytest日志、JUnit及A01—A20证据索引 |

规模脚本实际保存config/environment/source-sha256、事件、查询答案、隔离记录、服务日志和综合report；工件/恢复/回收身份收在report中。确定性故障证据来自pytest/JUnit和测试专用SIGKILL服务，不冒充规模脚本本身执行了全部故障矩阵。成功规模运行清理后无残余服务进程。

## A01—A20证据对应

下表中的测试名对应最终JUnit。每项按具体判据结合独立进程验收和确定性回归，不将一个脚本成功视为全部矩阵成功。

| 编号 | 判据 | 证据 |
| --- | --- | --- |
| A01 | 两节点同工件/答案，RPC加载与文件拒读 | 三个规模报告；test_two_nodes_same_artifact_same_answers |
| A02 | 重放、异请求冲突、提交后响应丢失 | test_same_operation_replay_and_conflict；test_commit_response_lost_is_recoverable_and_idempotent；test_replay_after_data_write_does_not_retrain |
| A03 | 批量更新revision窗口不读旧索引 | 规模报告batch-update、strict failure、exact fallback、新版本答案；test_write_gap_never_serves_stale_ann |
| A04 | 旧读跨新head保持自身版本 | test_old_query_completes_after_new_head；test_gc_protects_old_lease_and_download_pin |
| A05 | 同revision换配置，不用旧active | test_same_revision_head_switch_ignores_local_active |
| A06 | 其他表写入、DROP重建、CAS冲突 | test_commit_after_other_table_write_aborts；test_drop_recreate_same_name_changes_identity；test_stale_head_generation_cannot_overwrite_new_publication |
| A07 | 发布/下载准入保留读取容量 | test_second_publication_rejected_without_poisoning_first；test_download_admission_preserves_vector_capacity |
| A08 | 单计算节点重启，另一节点继续 | 规模报告restart_without_training/other_node_survives_restart；test_load_never_calls_build_worker |
| A09 | 多计算节点及全进程恢复 | 规模报告full_restart_same_artifact-A/B；test_storage_restart_recovers_committed_only；test_both_compute_processes_restart_with_storage_alive |
| A10 | 存储故障后旧token失效、恢复已提交状态 | 规模报告storage_stop_fails_closed/old_token_invalid；test_old_token_invalid_after_restart |
| A11 | 上传/验证/COMMIT前后实际kill | test_publication_crash_windows_recover_exact_commit_status四个窗口；测试专用服务在对象安装后、目录提交前及提交后截断进程 |
| A12 | 摘要/格式/结构损坏拒绝 | test_recovery_reports_corrupt_without_publishing_orphans；test_valid_native_payload_with_forged_snapshot_digest_rejected；test_unknown_format_or_faiss_version_rejected |
| A13 | 异常流、超大int64、非法ID | test_invalid_artifact_stream_never_returns_success七种流故障；test_large_int64_and_signed_zero_preserved；test_invalid_snapshot_input_is_rejected |
| A14 | 预算/磁盘/取消后无半成品或泄漏 | test_quota_reserved_before_upload；test_fsync_failure_never_marks_prepared；test_failed_directory_barrier_must_be_retried；test_abort_retries_failed_unlink_without_leaking_reservation；test_verifier_cancellation_reaps_process；test_maintenance_budget_released_after_cancel |
| A15 | GC引用保护、退休后kill、固定集合/墓碑 | test_gc_protects_old_lease_and_download_pin；test_gc_crash_after_retire_resumes_fixed_set；test_prune_replay_keeps_original_set_and_tombstone；test_deleted_key_tombstone_rejects_different_native_bytes；test_dropped_table_retained_only_until_references_release |
| A16 | 已提交损坏后ANN拒绝、exact可用 | test_recovery_marks_missing_blob_corrupt；test_corrupt_publication_errors_but_exact_works |
| A17 | GPU同来源、实际kill后重克隆与隔离 | GPU规模报告；test_gpu_restore_clones_loaded_cpu_without_training；test_dead_worker_reload_preserves_published_cpu；test_gpu_fallback_cannot_choose_other_artifact |
| A18 | local旧质量兼容、published工件质量约束 | test_v2_profile_rejected_only_in_published_mode；test_same_key_wrong_artifact_profile_rejected；test_frozen_faiss_version_must_match_published_backend；500条CPU/GPU工件验证 |
| A19 | 只读检查、EXPLAIN/ANALYZE、普通CRUD | test_inspect_is_read_only；test_explain_does_not_scan_or_fetch_and_analyze_reports_real_work；test_published_mode_rejects_local_build_and_keeps_crud |
| A20 | preload错误不ready、正确加载无训练 | test_preload_failure_prevents_ready；test_preload_rechecks_head_after_loading；test_preload_has_no_training_or_snapshot_export；test_preload_config_respects_size_and_storage_boundary；test_preload_fault_never_reports_ready（缺文件/损坏/预算） |

旧租约、下载pin、同名重建与回收使用真实DuckDB/RPC及同步屏障。崩溃覆盖进程SIGKILL和注入文件系统错误，不等同于断电、控制器故障或远程文件系统耐久性证明。

## 生命周期成本

固定seed=20261016、128维、CPU、独立/相关分布，每组3次。下表为各阶段3次中位数，单位毫秒。加载只下载/校验已发布工件，恢复包括新计算进程启动及预加载；这些范围不等同于重新构建，因此不计算“提速倍数”。

| 行数/分布 | 更新后发布 | A加载 | B加载 | A启动预加载 | 提交后至两节点ready |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1万/独立 | 565.80 | 79.95 | 87.54 | 246.85 | 1393.97 |
| 1万/相关 | 528.61 | 84.80 | 76.99 | 244.00 | 1343.07 |
| 10万/独立 | 3666.27 | 274.44 | 272.41 | 445.39 | 11354.93 |
| 10万/相关 | 3661.40 | 272.60 | 279.26 | 424.49 | 11533.14 |

提交后至ready包含验收脚本在窗口内执行严格失败/精确回退等探针，不是系统自动更新SLA。系统没有后台自动发布或加载，实际可见性窗口取决于管理操作。

10万独立分布的发布阶段中位数：快照导出2319.81ms、native训练571.56ms、build总计3190.84ms、上传79.41ms、Finalize RPC279.38ms，其中验证164.06ms、落盘22.25ms。各分量可能嵌套，中位数不能相加。目录事务未单独计时，storage_catalog_commit_ms保持null。单工件payload为1万5,252,139字节、10万52,133,259字节；网络统计为Protobuf序列化字节，不是网卡字节。

查询使用一个固定向量、全nprobe、热工件；每客户端4条计时查询，并发1/4/8分别4/16/32条，每组3次。以下是每次P50/P95的跨重复中位数（毫秒），小样本P95仅为描述性观测：

| 行数/分布 | 并发1 P50/P95 | 并发4 P50/P95 | 并发8 P50/P95 |
| --- | ---: | ---: | ---: |
| 1万/独立 | 31.74 / 43.87 | 46.22 / 72.48 | 86.81 / 98.19 |
| 1万/相关 | 38.28 / 52.24 | 45.80 / 72.97 | 102.85 / 129.75 |
| 10万/独立 | 27.62 / 40.96 | 53.48 / 73.81 | 114.21 / 139.70 |
| 10万/相关 | 44.53 / 51.12 | 56.84 / 83.01 | 105.45 / 130.53 |

各进程RSS/CPU、索引目录大小每5ms采样，可能漏峰。1万的storage峰值范围419.6—427.2MiB、A296.4—316.6MiB、B249.8—268.0MiB；10万分别1138.7—1146.6MiB、698.0—742.8MiB、461.9—497.0MiB。这里是整个进程RSS，不能当作索引驻留量或相加为同步峰值。两版本索引目录峰值分别10,505,852和104,268,096逻辑字节。

verifier的exec后VmHWM为1万182.7—183.4MiB、10万365.5—366.1MiB。原ru_maxrss会带入父进程exec前峰值，报告同时保留原值与口径；旧build-worker指标不改名冒充同口径。初次100k报告的约1.19GB旧verifier计数不是其验证阶段新增RSS。预算为保守工作集估计，不是硬RSS限额。此次成本矩阵为CPU，GPU显存栏为null；GPU功能/质量已单独验证。没有清空OS页缓存，新进程不能称为冷OS缓存，也不支持一般ANN性能优越结论。

## 工件质量验证

从既有校准中固定1万独立分布、100%通过率、K10、search-first各一个CPU/GPU负载，核对200条校准、500条完整留出集合、float32查询身份、实际数据内容、Faiss版本及已选构建/检索参数，未重新调参。

| 后端 | 完整留出数 | 平均Recall@10 | 结果 |
| --- | ---: | ---: | --- |
| CPU | 500 | 0.9702 | 通过，导出v3并在重启加载后实际应用 |
| GPU | 500 | 0.9728 | 通过，来源绑定相同CPU工件并验证GPU身份 |

通过要求包括零精确回退、零结果不完整、过滤和float64距离一致、工件/head不变。详细逐查询记录和新质量文件位于results/p4-quality-20261018/cpu及gpu。复用旧留出集只证明这两个已支持负载的工件一致性，不是新泛化实验，不重写原GPU60组的36通过/21不支持/3完整性失败，也不承诺任意数据或每条查询召回。

## 审查修复与限制

独立只读审查未发现Critical，提出两项Important，均通过先失败回归后修复：已存在对象和祖先目录须重试fsync屏障；keep-last只保留仍存在的table_id，DROP表在有效引用释放后可显式回收。另补充磁盘unlink暂时失败后的上传清理重试，防止容量永久占用；32项相关回归通过。未搁置审查中的Important项。

默认local/exact不变；published需显式发布/加载，完整CPU索引仍驻留内存。计算端原生解码不能强制中断且原生崩溃可影响进程；仅可信工件部署。单存储权威、无TLS/鉴权、无断电证明、无实时无间隙切换；写入至发布/加载的窗口必须明确处理。后续P4数据缓存/自动策略和P5更广性能实验仍需另行实施。
