# P4 索引持久化与版本发布实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 执行方式由用户在实施前选择；本次仅编写设计与计划，不启动实现或子代理。

**Goal:** 交付存储侧权威 CPU 索引工件、原子版本发布、双计算节点显式加载与重启恢复，并用真实 RPC/PTY/隔离及故障注入证明一致性。

**Architecture:** 计算侧复用现有 worker 显式构建；存储侧独立校验后先落盘工件、再通过 DuckDB 短事务提交发布目录与幂等结果。读租约同时固定数据版本和权威工件；计算节点从 RPC 加载，GPU 从同工件克隆。

**Tech Stack:** 现有 Conda Python 3.12.15、DuckDB 1.5.6、Faiss 1.12.0、NumPy 1.26.4、gRPC 1.83.1、Protobuf 7.35.1、pytest；不新增依赖。

**Spec:** [P4 设计提案](../../design/p4-index-publication.md)。状态：2026-10-16 已形成提案，尚未经实施验收；任务均未开始。用户要求本轮不修改业务代码，不将本计划视为实施或 Git 操作授权。

## 全局约束

- 保留数据库级 revision、完整 IndexKey；索引目录事务不得调用递增 revision 的 `Database._write`。
- 默认 `--index-source local`，默认 CPU/exact/filter-first；published 显式启用，旧 BuildIndex 在 local 模式保持兼容。
- published 查询只可使用租约固定的 `(IndexKey, artifact_id)`；本地 active key 不构成发布权威。
- 发布/加载总期限每次最多 60 秒，默认 45 秒；每库 16 个租约上限不变；取消/期限不能被回退吞掉。
- 1 MiB chunk、4 MiB 消息、64 KiB 描述符、256 MiB 单工件、2 GiB 存储工件+staging 默认预算、256 MiB 磁盘余量。
- 存储 8 RPC worker、4 共享重操作名额、最多 2 工件重操作、1 verifier；每库 1 发布，计算 CPU 构建/加载合计 1 任务；全部非阻塞准入。
- CPU 驻留默认 256 MiB/8 条目；加载/构建估算工作集加驻留不超过预算两倍；verifier 默认 512 MiB 估算预算。GPU 原预算和单 owner 约束保持。
- 计算/worker 不得直接访问存储 data-root；首期没有计算侧磁盘工件缓存或隐式查询加载/训练。
- 数据提交到索引发布/加载存在可观测窗口；新数据缺索引时精确回退或严格失败，不读旧索引。
- GPU 仍只支持 approx/search-first、gpu_id=0、单次 L/nprobe≤2048；质量与设备回退分别控制。
- 质量 v1/v2 保留 local 语义；published 配置使用 v3 绑定工件摘要，不能仅改身份字段迁移证据。
- 任一 proto 改动后运行生成脚本，不手改 pb2；文档与日志只记真实执行结果。
- 实施前按 AGENTS 取得 Git 操作同意，创建功能分支；建议名 `codex/p4-index-publication`。各任务提交点仅在取得相应授权后执行，不自动合并/推送。

## 审查重点及测试归属

1. COMMIT 成功后丢响应/取消，不能改记失败或重复发布：T2/T4/T9。
2. 同 revision、不同构建指纹发布，旧本地 active/GPU CPU 回退不能混用：T5/T6/T7。
3. 上传工件结构合法、ID一致但向量错误，不能只凭文件 hash 发布：T1/T3/T4。
4. 旧读/慢下载与 GC、DROP 重建同名表竞争，不能提前删或串用对象：T2/T8/T9。
5. 重启后查询成功实际偷偷训练，或不匹配 v3 证据仍显示质量通过：T7/T8/T9。

## 文件与依赖图

| 单元 | 新增文件（实施时创建） | 修改的现有文件 |
| --- | --- | --- |
| 工件类型、编码、规范摘要 | `src/vecstor/index/artifact.py` | `index/faiss_backend.py`、`index/types.py`（只复用/必要导出） |
| 权威目录及事务 | `src/vecstor/storage/index_catalog.py` | `storage/database.py`、`storage/catalog.py` |
| 不可变文件、验证进程 | `src/vecstor/storage/index_artifacts.py`、`src/vecstor/index/artifact_worker.py` | 复用 `index/build_worker.py` 的管道生命周期模式 |
| 存储 RPC 和协调 | `src/vecstor/storage/index_service.py` | `storage/sql_service.py`、`proto/vecstor/rpc/sql.proto`、生成模块 |
| 计算发布/加载客户端 | `src/vecstor/compute/index_publication.py` | `compute/index_manager.py`、`index/registry.py`、`compute/sql_service.py` |
| 执行绑定 | 无新通用调度模块 | `compute/lease_io.py`、`compute/vector_execution.py`、`index/gpu_registry.py` |
| GPU/质量 | `benchmarks/p4/validate_artifact_quality.py` | `index/gpu_backend.py`、`query/quality_policy.py`、能力输出 |
| 运维与恢复 | `src/vecstor/compute/index_recovery.py` | `rpc/server.py`、`compute/service.py`、`rpc/sql_client.py`、`shell/commands.py`、`isolation.py` |
| 验收 | `src/vecstor/rpc/p4_index_acceptance.py`、`scripts/check_p4_indexes.py`、`benchmarks/p4/index_lifecycle.py`、`benchmarks/p4/README.md` | 复用现有 ServiceProcess/TerminalProcess |

依赖顺序：T1 → T2/T3（接口先固定）→ T4 → T5 → T6 → T7/T8 → T9。这是依赖关系，不自动授权并行代理。每个任务独立完成失败测试→实现→通过测试→审查；不要一次改完所有层后才测试。

## Task 1：固定工件格式、规范内容摘要与兼容性

**Files:** 新增 `src/vecstor/index/artifact.py`、`tests/unit/test_index_artifact.py`；修改 `src/vecstor/index/faiss_backend.py`；复用 `tests/unit/test_faiss_index.py`。

**Interfaces:**

- `ArtifactDescriptor`：设计3.2字段的 frozen dataclass；`to_dict()`、`from_dict(value)` 严格字段校验。
- `SnapshotHasher(row_count: int, dimension: int)`：`add(ids: np.ndarray, vectors: np.ndarray) -> None`，`finish() -> str`；只接受全局递增且唯一 int64 ID、float32 固定维度、精确总数。
- `encode_artifact(handle: FaissIndex, key: IndexKey, config: BuildConfig, snapshot_sha256: str) -> tuple[ArtifactDescriptor, bytes]`。
- `validate_artifact(payload: bytes, descriptor: ArtifactDescriptor) -> FaissIndex`：长度/hash/版本预检，再验证原生结构和规范内容摘要；无训练。
- `ArtifactError(code: str, reason: str)`：code 为设计中的 gRPC 状态名，由边界层映射，不在底层调用 context.abort。
- 协调结果类型同文件定义：`PublicationRecord`（operation_id/operation_kind/request_intent/intent_digest/request_digest/key/expected_generation/state/descriptor/generation/error；PRUNE的key和descriptor为null，额外结果保存PruneResult）、`PublishedLease`（既有 lease 字段及 publication_version/generation/descriptor/state）、`PruneResult`（candidate/protected/deleted ID 与字节）、`ArtifactInspection`（descriptor/generation/cpu_ready/gpu_ready/state）。字段不可省略时使用显式 null，不靠缺键猜状态。

- [x] 写 `test_digest_is_independent_of_batch_partition`、`test_large_int64_and_signed_zero_preserved`、`test_descriptor_id_covers_key_and_payload`、`test_valid_ids_wrong_vectors_rejected`、`test_unknown_format_or_faiss_version_rejected`，按设计规范断言。

```python
def test_digest_is_independent_of_batch_partition(snapshot):
    # snapshot fixture: 2048行、4维、唯一ID>2**53，含-0.0；在本测试文件定义。
    ids, vectors = snapshot
    one = SnapshotHasher(len(ids), 4)
    one.add(ids, vectors)
    many = SnapshotHasher(len(ids), 4)
    for start in range(0, len(ids), 37):
        many.add(ids[start:start + 37], vectors[start:start + 37])
    assert one.finish() == many.finish()
```

- [x] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_index_artifact.py`，先观察缺实现或断言失败，保存失败摘要。
- [x] 实现规范 raw Faiss payload 与 descriptor；向量摘要从 IVFFlat 倒排代码提取，不要求现有索引启用 DirectMap，不更改旧 NPZ 管道封装。
- [x] 运行上述测试及 `tests/unit/test_faiss_index.py`；证明错误向量即使 hash 重算也被权威 snapshot digest 拒绝，限制大小在 native 读取之前检查。
- [x] 审查工件格式/身份值并记录本任务结果；有 Git 授权时提交 `feat: define immutable CPU index artifacts`。

## Task 2：目录事务、发布 CAS 与读租约绑定

**Files:** 新增 `src/vecstor/storage/index_catalog.py`、`tests/unit/test_index_catalog.py`；修改 `storage/database.py`、`storage/catalog.py`；扩展 `tests/unit/test_catalog.py`。

**Interfaces:** `IndexCatalog(connection, store)` 只在 Database 队列调用，store 由 T3 提供，测试使用明确定义的内存 fake。

- `initialize() -> None`：内部表迁移和未知 schema 拒绝。
- `begin(lease: PublishedLease, operation_id: str, config: BuildConfig, expected_generation: int, request_intent: dict) -> PublicationRecord`；request_intent字段严格遵循设计4.3，服务端自行计算摘要。
- `bind_upload(operation_id, descriptor) -> PublicationRecord`；`set_phase(operation_id, phase) -> None`；`fail(operation_id, code, reason) -> PublicationRecord`（不能覆盖 COMMITTED）。
- `commit(operation_id: str, prepared: ArtifactDescriptor, lease_token: str, check: Callable[[], None]) -> PublicationRecord`：仅检查已准备对象、当前 revision/head、完整 key、租约；原子落目录/结果。
- `get_operation(operation_id: str) -> PublicationRecord | None`；`list_artifacts(table_id: str | None, limit: int, cursor: str) -> tuple[list[dict], str]`；`resolve_head(connection, snapshot: tuple) -> tuple[ArtifactDescriptor | None, int, str]`。
- `Database._record_snapshot_digest(token, digest, row_count) -> None`；`_begin_read` 同一事务读取数据与 head，租约内部增加完成收据，不把内部状态直接发给客户端。

- [ ] 写失败测试，覆盖 `test_publication_does_not_advance_data_revision`、`test_old_lease_keeps_head_on_same_revision_switch`、`test_same_operation_replay_and_conflict`、`test_commit_after_other_table_write_aborts`、`test_drop_recreate_same_name_changes_identity`、`test_unknown_catalog_schema_rejected`。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_index_catalog.py`，确认失败来自缺实现。
- [ ] 实现设计3.3表与短事务；head CAS 使用该 snapshot 的旧 generation（不存在为0），成功从数据库级发布序列分配新值；其他 table 发布不无故冲突。
- [ ] 实现每库1发布准入、操作摘要和终态重放；同 key 工件冲突、墓碑约束；小于1024行发布明确 FAILED_PRECONDITION，不发布空 IVF。
- [ ] 将目录接入 Database 生命周期、租约及清理；租约结束将未提交发布终止并释放每库名额，COMMITTED不受影响。不要修改现有数据事务结果和 operation_id 命名空间。内存 fake 模拟对象已准备、pin 获取和丢失的不同状态。
- [ ] 运行新测试及 `tests/unit/test_catalog.py tests/integration/test_p3_storage.py`；断言旧接口不变、发布事务失败不会推进 revision/generation、COMMITTED 不被取消覆盖。
- [ ] 审查线性化点和锁序；授权时提交 `feat: bind published index heads to read leases`。

## Task 3：不可变对象存储、校验子进程与容量账本

**Files:** 新增 `src/vecstor/storage/index_artifacts.py`、`src/vecstor/index/artifact_worker.py`、`tests/unit/test_index_artifact_store.py`、`tests/integration/test_index_artifact_worker.py`。

**Interfaces:**

- `ArtifactStore(root: Path, max_artifact_bytes=256*1024**2, max_total_bytes=2*1024**3, min_free_bytes=256*1024**2)`。
- `begin_upload(operation: tuple[str, str], descriptor: ArtifactDescriptor) -> None`；`append(operation: tuple[str, str], offset: int, chunk: bytes) -> None`；`seal(operation: tuple[str, str]) -> None`。operation统一为(database_id, operation_id)，防止跨库同ID冲突。
- `validate_and_install(operation: tuple[str, str], snapshot_digest: str, deadline: float, cancelled: Callable[[], bool]) -> ArtifactDescriptor`。
- `abort_upload(operation: tuple[str, str]) -> None`；`pin(descriptor: ArtifactDescriptor) -> ContextManager[BinaryIO]`；`prepared(descriptor) -> bool`（短锁内，无大文件hash）。
- `recover(committed: list[ArtifactDescriptor]) -> dict`：manifest/长度/hash核验，返回损坏/孤儿/临时文件清单，不直接改 DB。
- `delete_retired(descriptor: ArtifactDescriptor) -> int`；`reserved_bytes` / `stored_bytes` / `transfer_pins` 只读统计；具体 GC 选择交 T8。
- `verify_in_worker(path: Path, descriptor: ArtifactDescriptor, deadline: float, cancelled: Callable[[], bool], max_working_bytes=512*1024**2) -> dict`：仅 storage 调用；新解释器只读 staging 文件，返回实际结构、内容摘要、RSS及耗时。

- [ ] 写失败测试：`test_reject_symlink_and_path_escape`、`test_chunk_limit_and_offset`、`test_quota_reserved_before_upload`、`test_fsync_failure_never_marks_prepared`、`test_existing_object_never_overwritten`、`test_verifier_cancellation_reaps_process`、`test_digest_checked_before_native_read`。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_index_artifact_store.py tests/integration/test_index_artifact_worker.py`，记录失败。
- [ ] 实现同文件系统 staging→objects 安装，文件/目录 fsync，manifest和payload不可变；以真实成功落盘产生 prepared 状态，不能用调用者布尔值跳过验证。
- [ ] 实现 verifier 一次 communicate 专用线程、deadline终止/回收，失败返回明确错误；内存估算包括 payload/原生/排序副本，单任务准入。
- [ ] 测试注入 write/fsync/rename/worker exit 各失败点；断言容量预留释放、临时文件清理、已存在对象不变、已准备对象在重启可识别。
- [ ] 运行新测试；授权时提交 `feat: persist and validate bounded index artifacts`。

## Task 4：增量协议与存储发布/分发 RPC

**Files:** 修改 `proto/vecstor/rpc/sql.proto`、生成模块、`storage/sql_service.py`、`storage/catalog.py`；新增 `storage/index_service.py`、`tests/integration/test_index_publication_rpc.py`、`tests/unit/test_index_artifact_protocol.py`。

**Interfaces:** `IndexPublicationService(catalog, store, stream_slots)` 实现设计5.1七个RPC；SQLStorageService 显式委托相应方法，复用原 `_stream_slots`。

- 新增类型化消息 `IndexIdentity`（IndexKey字段）、`IndexBuildConfig`、`IndexArtifactDescriptor`、`IndexPublicationRecord`、`BeginIndexPublicationRequest`、`IndexUploadFrame`、`FinalizeIndexPublicationRequest`、`IndexPublicationRequest`、`ListPublishedIndexesRequest/Response`、`DownloadIndexArtifactRequest`、`IndexArtifactFrame`、`PruneIndexArtifactsRequest/Response`。
- upload frame 采用 oneof header/chunk/trailer；download frame 采用 oneof descriptor/chunk/complete。chunk 为 offset/bytes，trailer/complete 固定 total_size/payload_sha256；身份与操作/租约固定在头部且后续帧必须匹配。
- ReadLease 增量字段及 SnapshotBatch 最终摘要字段见设计5.1；在源文件确认未使用号码后顺序分配，不复用旧号码。
- `DatabaseCatalog.record_snapshot_digest(...)`、`begin_index_publication(...)`、`commit_index_publication(...)`、`get_index_publication(...)` 仅委托 Database.call；新错误类型独立映射，不沿用把所有 OSError 当 NOT_FOUND 的 `_abort`。

- [ ] 写协议失败测试：旧字段解码不变；缺final/尾随帧/重复offset/超4MiB/错误身份拒绝；最终 snapshot digest 必须来自完整导出。
- [ ] 写真实RPC失败测试：上传不可见、Finalize后可下载、同operation重放、revision改变拒绝、结构合法但内容错、commit后故意丢响应仍查得COMMITTED；下载只能取租约固定工件。
- [ ] 运行新测试确认失败；修改proto并执行 `conda run -n vecstor python scripts/generate_proto.py`。
- [ ] 实现新能力 `index_publication_version=1` 与RPC编排；上传按完整流seal，Finalize调用T3后T2；同key已有有效工件可跳过训练/上传/新收据，但必须校验它仍READY并执行相同CAS。Prune在本任务只定义协议并明确返回UNIMPLEMENTED，T8才接入真实回收，期间不宣称支持回收。
- [ ] 在完整Snapshot导出正常结束时通过Database队列保存收据，取消/过期时不保存；租约内部状态不与SQLStorageService `_leases` 重复维护。
- [ ] 实现非阻塞2工件/1verifier/4共享名额，所有异常路径释放；数据库锁不得包住流等待、子进程或文件I/O。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_index_artifact_protocol.py tests/integration/test_index_publication_rpc.py tests/integration/test_p3_storage.py tests/integration/test_fused_candidates.py`；额外断言满载时FetchVectors/EndRead可完成。
- [ ] 核对proto重新生成结果稳定、旧客户端仍可用；授权时提交 `feat: publish and distribute index artifacts over RPC`。

## Task 5：计算侧显式发布、加载与引用管理

**Files:** 新增 `src/vecstor/compute/index_publication.py`、`tests/unit/test_index_publication_manager.py`、`tests/integration/test_index_artifact_loading.py`；修改 `compute/index_manager.py`、`index/registry.py`、`compute/sql_service.py`、proto及生成模块。

**Interfaces:**

- `IndexPublicationManager(stub, registry, build_from_lease, maintenance_slot, gpu_registry, gpu_worker)`。
- `publish(request, context) -> PublicationRecord`；`load(request, context) -> dict`；`inspect(request, context) -> ArtifactInspection`。
- `IndexManager.build_from_lease(io: LeaseIO, config: BuildConfig) -> tuple[FaissIndex, dict]`：沿用调用者期限/租约，不发布registry、不释放调用者lease；旧 `_build_cpu` 包装后保持原行为。
- `IndexRegistry.acquire_exact(key: IndexKey, artifact_id: str) -> ContextManager[IndexEntry | None]`；`publish_artifact(descriptor: ArtifactDescriptor, handle: FaissIndex, table: str, statistics: dict, check: Callable) -> IndexEntry`；IndexEntry 增加可选descriptor（local为None）。
- `PublishIndexRequest/Response`、`LoadIndexRequest/Response`、`InspectPublishedIndexRequest/Response` 与代理RPC加入 SQLCompute，字段见设计5.2。

- [ ] 写失败测试：`test_load_never_calls_build_worker`、`test_wrong_expected_artifact_rejected`、`test_partial_download_never_activates`、`test_pinned_local_conflict_is_not_overwritten`、`test_publish_success_survives_local_load_failure`、`test_maintenance_budget_released_after_cancel`。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_index_publication_manager.py tests/integration/test_index_artifact_loading.py` 记录失败。
- [ ] 提取租约构建函数，复用总deadline与CPU维护名额；原显式CPU/GPU BuildIndex路径回归。发布优先查询op状态并比较原始请求意图/已有key，避免响应丢失后重新训练；测试同ID在数据推进后重试仍返回原成功，不同参数仍拒绝。
- [ ] 实现分块下载缓冲和完整校验、预算估算与最终原子本地发布；本地操作报告 `trained=false`、下载字节、artifact_id、来源generation。客户端拿到完整流之前不能暴露可查询对象。此任务LoadIndex只实现CPU，GPU请求明确不支持，T7再接入克隆。
- [ ] 实现按完整工件身份获取，pin计数/预算与旧registry兼容；同key不同或无绑定对象显式冲突，禁止原地修改被引用句柄。
- [ ] 运行新测试及 `tests/unit/test_index_registry.py tests/unit/test_index_manager.py tests/integration/test_p3_execution.py`；新proto再生成并检查。
- [ ] 审查总deadline传播、子进程数和错误结果；授权时提交 `feat: load published indexes without retraining`。

## Task 6：published 查询模式与双节点切换安全性

**Files:** 修改 `compute/lease_io.py`、`compute/vector_execution.py`、`compute/sql_service.py`、`compute/service.py`；新增 `tests/integration/test_p4_index_consistency.py`；扩展 `tests/unit/test_p3_lease_io.py`。

**Interfaces:** `execute_vector(..., index_source='local')`；LeaseIO 在 published 模式验证 publication_version 和 descriptor；查询trace新增字段遵循设计8。服务配置在构造时固定，普通SQL不携带索引管理选项。

- [ ] 编写双计算服务共享一个storage fixture，固定两节点独立registry；用Event阻塞旧查询已pin之后的FetchVectors。
- [ ] 写 `test_two_nodes_same_artifact_same_answers`、`test_write_gap_never_serves_stale_ann`、`test_same_revision_head_switch_ignores_local_active`、`test_old_query_completes_after_new_head`、`test_inspect_is_read_only`、`test_corrupt_publication_errors_but_exact_works`。

```python
def test_same_revision_head_switch_ignores_local_active(cluster):
    # cluster fixture在本文件定义：publish(config)、load(node)、query(node)、inspect(node)。
    first = cluster.publish(nlist=1)
    cluster.load('A'); cluster.load('B')
    second = cluster.publish(nlist=2)
    assert second.key.revision == first.key.revision
    assert second.generation > first.generation
    assert cluster.query_error('B', fallback='error') == 'published_index_not_loaded'
    cluster.load('B')
    result = cluster.query('B', fallback='error')
    assert result['execution_plan']['artifact_id'] == second.descriptor.artifact_id
```

- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/integration/test_p4_index_consistency.py` 记录失败。
- [ ] 接入published按租约descriptor精确选择，区分未发布/未加载/损坏；保持默认exact与已有显式设备回退规则。无匹配CPU/GPU时不得挑本地同snapshot其他key。
- [ ] EXPLAIN仅元信息、ANALYZE仅一次；client完成帧对publication能力与实际artifact字段做一致性检查，普通CRUD不受影响。
- [ ] 运行新测试及 `tests/integration/test_p3_execution.py tests/unit/test_gpu_review_regressions.py`；检查A/B屏障、旧读答案和新读版本。
- [ ] 授权时提交 `feat: enforce lease-bound published indexes across compute nodes`。

## Task 7：GPU 工件来源与质量配置 v3

**Files:** 修改 `index/gpu_registry.py`、`index/gpu_backend.py`、`compute/index_publication.py`、`query/quality_policy.py`、`compute/sql_service.py`；新增 `tests/unit/test_artifact_quality_policy.py`、`tests/integration/test_p4_gpu_restore.py`、`benchmarks/p4/validate_artifact_quality.py`。

**Interfaces:**

- `GpuRegistry.acquire_exact(key, artifact_id)`：先 `_sync()` 清理旧worker generation，再按工件获取；统计中保存artifact_id。
- `IndexPublicationManager.load(device='gpu')`：pin CPU artifact，调用现有submit_clone，验证generation及来源，发布GPU descriptor；失败保留CPU。
- `QualityCatalog.resolve(..., execution=None, artifact: ArtifactDescriptor | None=None, index_source='local')`：local v1/v2行为不变；published必须v3且三个artifact_identity字段全匹配。
- `validate_artifact_quality.run(calibration_dir: Path, compute_address: str, database: str, table: str, output_dir: Path, device: str) -> dict`：读取既有冻结配置/完整留出集，校验真实数据指纹与快照绑定，验证当前发布工件；不重调参、不创建新训练；输出v3配置与逐查询证据。

- [ ] 写失败测试：`test_same_key_wrong_artifact_profile_rejected`、`test_v2_profile_rejected_only_in_published_mode`、`test_gpu_restore_clones_loaded_cpu_without_training`、`test_gpu_fallback_cannot_choose_other_artifact`、`test_dead_worker_reload_preserves_published_cpu`。
- [ ] 运行单元测试确认失败；实现v3严格字段校验、CPU gpu_model=`not_applicable`、能力版本与明确失败原因。
- [ ] 实现GPU load复用既有worker协议，绑定来源artifact且维持generation校验，不扩展GPU检索范围或取消模型。
- [ ] 实现质量验证工具，正式证据至少200校准/500留出，零不完整/零精确回退，固定已有质量门槛；输入数据身份/参数/摘要不一致则拒绝，已见留出标记一致性回归。工具不能仅复制旧summary再写新hash。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_artifact_quality_policy.py tests/unit/test_quality_policy.py tests/unit/test_gpu_quality.py`；真实GPU运行 `VECSTOR_REQUIRE_GPU=1 conda run -n vecstor python -m pytest -q tests/integration/test_p4_gpu_restore.py tests/integration/test_gpu_execution.py`。
- [ ] 新工具先以小fixture验证“样本不足不导出”，正式500条工件证据随T9归档；授权时提交 `feat: bind restored GPU replicas and quality profiles to artifacts`。

## Task 8：启动恢复、旧版本回收与终端命令

**Files:** 新增 `compute/index_recovery.py`、`tests/integration/test_p4_index_recovery.py`、`tests/integration/test_p4_index_gc.py`、`tests/unit/test_index_preload.py`；修改 `storage/index_catalog.py`、`storage/index_artifacts.py`、`rpc/server.py`、`rpc/sql_client.py`、`shell/commands.py`、`isolation.py`、服务构造参数。

**Interfaces:**

- `IndexCatalog.recover(store_report: dict) -> dict`：旧epoch非终态PUBLISH→ABORTED，PRUNE继续已固定集合，损坏对象→CORRUPT，不改变data revision；全部库引用收集完成后核对文件，发生在ready前。
- `IndexCatalog.begin_prune(operation_id: str | None, keep_last: int, dry_run: bool) -> PruneResult`、`finish_prune(operation_id: str, deleted_ids: list[str]) -> PruneResult`：dry-run可不带ID且不写记录；execute事务保护选择/标记并持久保存固定集合，T3负责删除；同ID重放不能纳入新工件。
- `read_preload(path: Path, storage_root: Path | None) -> list[dict]`：1MiB/128条限制、重复目标及data-root检查；在Landlock前调用。
- `preload_indexes(manager, targets: list[dict], deadline: float) -> list[ArtifactInspection]`：串行加载，每目标≤60秒，总≤300秒，最后复查head；任何失败不发ready。
- SessionClient 新增 `publish_index`、`load_index`、`inspect_published_index`、`list_published_indexes`、`index_publication_status`、`prune_index_artifacts`，参数对应T4/T5请求；publish/prune保存并显示operation_id。

- [ ] 写失败测试：`test_storage_restart_recovers_committed_only`、`test_recovery_marks_missing_blob_corrupt`、`test_old_token_invalid_after_restart`、`test_preload_has_no_training_or_snapshot_export`、`test_preload_failure_prevents_ready`。
- [ ] 写GC失败测试：旧租约/下载pin保护、同revision多配置保留、quota不足不删保护对象、RETIRED后kill重启继续清理、墓碑阻止不同字节复活、prune重放保持原集合。
- [ ] 运行上述三个测试文件确认失败。
- [ ] 实现启动核对与崩溃清理，按设计锁序实现mark/delete/finalize GC；不在全局锁内等待数据库/网络，不把存储旧索引当数据时光回溯。
- [ ] 实现`--index-source`、`--index-preload`及工件预算参数；启动preload在服务ready前完成。读取配置的允许目录与现有quality配置规则一致。
- [ ] 实现设计8的终端命令及帮助；调整`.index`参数解析，显式拒绝重复/未知键，保持旧build/list/clear。prune默认dry-run，execute必须operation_id。
- [ ] 扩展隔离探针：A/B进程及GPU worker拒绝读取存储payload与manifest，同时网络加载成功；不得扩大存储目录权限。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/unit/test_index_preload.py tests/integration/test_p4_index_recovery.py tests/integration/test_p4_index_gc.py tests/unit/test_shell_ui.py tests/integration/test_shell_workflow.py`；增加真实PTY新命令流程。
- [ ] 授权时提交 `feat: recover and prune published index versions explicitly`。

## Task 9：双节点、崩溃、规模验收与交付文档

**Files:** 新增 `rpc/p4_index_acceptance.py`、`scripts/check_p4_indexes.py`、`benchmarks/p4/index_lifecycle.py`、`benchmarks/p4/README.md`、`tests/integration/test_p4_index_acceptance.py`、`tests/integration/test_p4_index_crashes.py`、`docs/p4-indexes.md`、`docs/validation/p4-indexes.md`；更新README、用户说明、AGENTS、CHANGELOG。

**Interfaces:**

- `run_acceptance(output_dir: Path, rows=10000, dimension=128, seed=20261016, require_gpu=False) -> dict`：输出A01—A20矩阵；失败同样落报告；output必须不存在。
- `run_lifecycle_benchmark(output_dir: Path, sizes=(10000,100000), repeats=3, concurrency=(1,4,8)) -> dict`：构建/发布/加载/恢复分开计时，复用固定工件与查询，不混入参数调优。
- 测试专用故障注入模块放在 `tests/`，通过受控子进程与Event/pipe建立commit前后barrier；生产脚本不支持网络触发任意kill点。

- [ ] 先写小规模验收工具测试，校验output防覆盖、每场景有判据、失败报告与进程回收；写COMMIT前后kill的确定性集成测试，观察失败。
- [ ] 实现A01—A20矩阵，至少一次由真实PTY执行publish/status/load；A/B进程独立，存储唯一。使用明确屏障固定旧查询和发布时序，不靠sleep碰运气。
- [ ] 实现恢复反训练验证：测试进程训练入口必失败，计数 `build_worker_started=0` / `snapshot_calls=0` / `artifact_download_bytes>0`；分别覆盖A单重启、A/B重启、存储及全部进程重启。
- [ ] 实现对象落盘后commit前、commit后响应前、GC标记后删除前的kill检查；断言原子发布与幂等状态，不把文件目录存在当发布成功。
- [ ] 运行 `conda run -n vecstor python -m pytest -q tests/integration/test_p4_index_acceptance.py tests/integration/test_p4_index_crashes.py`，通过后按设计9执行1万、10万和真实GPU验收，各使用新输出目录。
- [ ] 运行生命周期成本工具，保存三次重复和资源/时间定义；运行T7的至少一组CPU、一个已支持GPU负载的完整500留出工件验证。将支持范围与原全矩阵分开，不宣称所有负载重验。
- [ ] 运行完整业务回归：`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 conda run --no-capture-output -n vecstor python -m pytest -q`。真实GPU/localhost受沙箱限制时按项目约定请求沙箱外运行；CPU-only skip不能计GPU通过。
- [ ] 独立审查重点为本计划5项风险及A01—A20覆盖；按执行方式调用审查技能，修复须补失败回归。审查后按变更范围重验，性能测量与最终源码不同则保留版本区别。
- [ ] 写验证文档真实结果，列出尚未通过的矩阵；同步目录、恢复/发布命令和限制。保留原始证据包及源码SHA，不向Git提交大型结果/用户数据。
- [ ] 授权时提交 `test: validate published index lifecycle across two compute nodes`；是否合并/推送另按用户授权办理。

## 里程碑与停线条件

| 里程碑 | 完成任务 | 可演示结果 | 不达标时处理 |
| --- | --- | --- | --- |
| M1 格式与权威发布 | T1—T4 | 单存储发布、幂等、损坏拒绝、崩溃后目录恢复 | 不接入查询，先修持久性/身份问题 |
| M2 双节点安全使用 | T5—T6 | A/B 同工件；旧读跨切换；未加载节点明确回退/失败 | 暂缓GPU与性能优化 |
| M3 恢复与运维 | T7—T8 | 零训练恢复、GPU重克隆、v3证据、受保护回收 | 任一错误工件激活或旧读提前回收均阻止验收 |
| M4 可复现交付 | T9 | 完整矩阵、成本、隔离、回归、文档 | 不以部分通过替代P4索引子阶段完成 |

建议以里程碑安排约2—3周实施窗口，实际取决于审查与故障测试；这只是工作量估计，不替代AGENTS正式排期。60秒租约与10万规模是首个需要实测的容量门槛；若失败，单列长作业设计再评审，不直接改长租约。数据缓存/自动策略在本子阶段之后单独设计。

## 设计覆盖检查与本轮状态

| 设计章节 | 责任任务 |
| --- | --- |
| 1—2 范围/现有接入 | 全局约束、T5、T6 |
| 3 身份/格式/目录 | T1、T2、T3 |
| 4 发布/取消/崩溃 | T2、T3、T4、T9 |
| 5 协议/资源/错误 | T3、T4、T5 |
| 6 查询/双节点 | T5、T6、T9 |
| 7 恢复/GC | T8、T9 |
| 8 质量/操作/计划 | T7、T8 |
| 9 验收/成本/证据 | T9，并由各任务前置回归覆盖 |
| 10 兼容/限制 | 全局约束、T1、T7、T9文档 |

本轮完成设计与计划编写、文档链接/一致性自查及项目文档索引更新；未修改业务代码、proto、测试或依赖，未运行新的业务回归，未执行Git操作。上述命令与测试名均为实施目标，不是已有测试通过记录。

## 实施记录（2026-10-17）

- 已获授权按任务实施、建分支与分阶段提交；分支codex/p4-index-publication，未合并/推送，用户.gitignore修改未纳入。
- 实施前完整真实GPU基线565通过（138.96秒）。T1工件与原Faiss测试52通过（2.07秒）；T2—T9待实施，P4尚未验收。
