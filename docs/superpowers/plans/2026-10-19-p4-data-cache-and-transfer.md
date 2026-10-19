# P4 有界数据缓存与传输优化实施计划

> 执行时使用executing-plans，按任务推进、每任务先失败测试再实现；如用户另选代理执行方式再调整。**本文件是2026-10-19规划交付，不是业务实施记录。2026-10-19已按用户授权开始实施：T1—T7完成，T8待实施。** 后续Git检查、建分支及提交须有适用于本子阶段的用户授权，不把旧索引阶段授权自动扩为合并/推送授权。

**Goal:** 在既有MVCC/工件身份约束下交付有界向量缓存、按需元数据与选择性fused读取，保留独立开关，用真实双节点/故障/资源及配对实验验证。

**Architecture:** 计算进程私有VersionedVectorCache → CachedLeaseReader → 现有LeaseIO网络校验；存储增加投影读取能力，数据库队列仍拥有租约与SQL；不缓存元数据/谓词结果，不改变ANN和质量参数。

**Tech Stack:** 现有Conda Python3.12、NumPy1.26、DuckDB1.5.6、SQLGlot30.21、gRPC1.83、Faiss1.12；不新增运行时依赖。

**Spec:** [详细设计](../../design/p4-data-cache-and-transfer.md)。规划依据为现有读取代码、G2结果和P4成本原始报告；本轮未运行新业务测试或缓存实验。

## 全局决策

- 默认cache关闭（0MiB），建议显式64MiB；65536条目、scope1024字节、entry512字节+payload的逻辑记账；新增工作区16MiB/4槽，每批4MiB。
- 原始vector按storage_epoch/database/table/revision/schema/dimension/dtype/id键隔离；不含索引构建参数。每请求必须BeginRead和存储过滤。
- 不新增single-flight、预取、自动切路径、Arrow、批调度或增量索引。旧v1和普通SQL不接缓存。
- cache、字段投影、已有fused独立开关；explicit请求必须能力与完成帧确认。旧服务器只允许原始关闭路径。
- 每个RPC单元完整校验后才能入缓存；不吞DATA_LOSS/取消/超时。clear后的旧在途数据不重新插入。
- 共享4存储重操作槽，1024行/1MiB向量、4MiB消息上限不提高；只对明确超大响应二分，单行超大失败。
- 无PYTHONPATH执行；性能实验串行，记录最终运行源码与后续修改差异。不得把下面的预期PASS写成实际通过。

## 文件职责与依赖

表中cache/compute/storage/query/rpc/shell前缀均相对于`src/vecstor/`；proto/tests/scripts/benchmarks/docs相对于项目根。

| 文件 | 职责 | 任务 |
| --- | --- | --- |
| 新cache/types.py、cache/vector_cache.py、cache/__init__.py | scope/预算/批内工作区、线程安全LRU与复制所有权 | T1 |
| proto/vecstor/rpc/sql.proto及生成模块 | epoch/TTL、新投影消息/RPC、新请求选项与本地缓存管理接口 | T2/T6 |
| 新query/read_projection.py | 从VectorPlan推导来源字段；白名单投影AST | T3 |
| storage/database.py/catalog.py/sql_service.py | epoch/TTL、投影SQL、选择性向量读取及有界帧 | T2/T3 |
| 新rpc/projected_reads.py | 新帧和可选字段/向量覆盖校验 | T3 |
| 新compute/data_reader.py | 数据缓存与LeaseIO整合；separate/fused miss组装 | T4/T5 |
| compute/lease_io.py | 原校验保留，新增保守本地租约期限/新网络接口 | T2/T3 |
| compute/vector_execution.py | 读适配器、按需TopK物化、实际统计 | T4/T5 |
| compute/sql_service.py/service.py、rpc/server.py | 服务缓存生命周期、启动配置与能力 | T4/T6 |
| query/search_options.py/execution_settings.py、rpc/sql_client.py | 不可变选项、兼容和确认 | T6 |
| 新shell/cache_commands.py；shell/commands.py/completion.py/app.py | .cache控制、.search帮助/补全 | T6 |
| 新scripts/check_p4_cache.py、rpc/p4_cache_acceptance.py | 真实PTY/双节点/隔离及故障判据 | T7 |
| 新benchmarks/p4/data_reads.py、tests/support/delayed_storage.py | 配对缓存/投影/延迟实验；延迟代理仅测试用途 | T8 |
| 新docs/p4-cache.md、validation/p4-cache.md | 使用/证据/限制；同步README、AGENTS、CHANGELOG | T8 |

依赖：T1→T2→T3→T4→T5→T6→T7→T8。T1可单独测试但尚不改变查询；T2/T3新协议不改变旧路径；T4/T5先经测试构造参数启用，T6才暴露完整用户开关。每个任务完成后更新实施记录，并在授权范围内提交。

## Task 1：版本化有界缓存内核

**Files:** 新cache/types.py、vector_cache.py、__init__.py；新tests/unit/test_vector_data_cache.py、test_data_cache_concurrency.py。

**Interfaces:**

- frozen `DataScope(storage_epoch: str, database_id: str, table_id: str, revision: int, schema_version: str, dimension: int, dtype: str='float32-le')`。
- frozen `CacheConfig(max_bytes: int=0, max_entries: int=65536, workspace_bytes: int=16777216, workspace_slots: int=4, scan_admission: bool=False)`，reject bool/负数/无效槽；维度和ID同现有契约。
- `VersionedVectorCache(config: CacheConfig)`；`batch(scope: DataScope, ids: tuple[int,...], check: Callable[[],None]) -> ContextManager[LookupBatch | None]`。None表示disabled/工作区无名额，调用者不等待。
- `LookupBatch`公开readonly ids、missing_ids、generation和请求局部计数；`finish(matched_ids: tuple[int,...], remote_ids: tuple[int,...], remote_vectors: np.ndarray, *, admit: bool, check: Callable[[],None]) -> np.ndarray`。remote_ids必须精确等于matched_ids中的miss，按matched顺序；输出匹配顺序float32数组。separate的matched_ids就是输入ids。进入finish前远端协议已校验，finish仍检查集合/形状/有限值。返回数组由查询拥有，不引用缓存；退出context归还4MiB工作区名额。
- `stats(database_id: str) -> dict`只读总容量和所选库占用；`clear(database_id: str) -> dict`回收选库所有scope并推进全局admission_generation；无跨请求计数差。

- [x] 写`test_budget_and_lru`：dimension=4、max_bytes=2080恰容纳1个scope+2条(512+16)；第三条淘汰最久未用、touch改变次序；条目上限独立约束。`test_large_entry_bypasses`验证max_bytes=1024不能容纳记录，结果仍可返回。
- [x] 写`test_copy_owns_only_one_vector`：从1MiB响应插入一行，删除响应后缓存只保留16字节payload；改变输出不影响下一次读取；负零字节及id=2**53+1保持。`test_conflicting_same_key_is_data_loss`不得覆盖。
- [x] 写同步Event测试`test_clear_during_download_cannot_refill`、`test_parallel_miss_is_not_single_flight`、`test_workspace_exhaustion_bypasses`：4个上下文占满，第5个立即None；结束/异常后容量复原；进程记账不越限。
- [x] 运行失败测试：`conda run -n vecstor python -m pytest -q tests/unit/test_vector_data_cache.py tests/unit/test_data_cache_concurrency.py`；初始应因缺模块/接口失败，记录原因。
- [x] 实现上述接口及LRU，scope空后释放1024额度，不把RPC放入锁内；finish只缓存匹配且校验成功的remote行，已缓存未匹配行不进入结果。
- [x] 同命令通过后检查C02/C07、维护AGENTS新cache目录和CHANGELOG；提交建议`feat: add bounded revision scoped vector data cache`。

## Task 2：存储epoch、保守租约期限和协议契约

**Files:** sql.proto及四个生成模块、storage/sql_service.py、compute/lease_io.py；新tests/unit/test_data_read_protocol.py，tests/integration/test_data_cache_epoch.py。

**Interfaces:**

- ReadLease字段13 storage_epoch、14 effective_ttl_seconds；epoch归SQLStorageService实例，每启动生成一次uuid4，不持久化、不依赖ArtifactStore存在。
- 声明设计§7两个新Projected RPC及消息；明确字段号1—8/optional存在性、最后帧complete。实现前新RPC返回UNIMPLEMENTED；`data_read_version=1`只在T3真正实现后公布；epoch能力T2可独立公布。
- `LeaseIO.open()`继续返回ReadLease；新增`configure_cache_lease() -> DataScope`校验epoch/实际TTL，使用open已记录的t0收紧deadline。关闭缓存的旧服务不要求新字段。每请求不能再次BeginRead替换过期租约。

- [x] 写`test_storage_restart_changes_epoch_even_same_revision`：同数据库重启后revision不变、epoch不同；备份恢复相同database/table/revision但向量内容不同的受控fixture也不得命中旧scope。
- [x] 写`test_cache_deadline_uses_send_time_and_granted_ttl`，受控单调时钟t0=100、返回时101、TTL=2，本地截止102，不能变成103；0/NaN/超过请求TTL拒绝。全部hit也检查取消/到期。
- [x] 写optional字段roundtrip测试：id=0/value=0能区分缺失；旧ReadLease字段序列化语义不变，新proto未实现接口明确拒绝。
- [x] 定向测试先失败；修改proto并运行`conda run -n vecstor python scripts/generate_proto.py`，不手改生成文件。实现epoch/TTL，再运行上述单元/集成测试及原test_p3_lease_io.py、test_p4_index_recovery.py。
- [x] 通过后提交建议`feat: bind data cache reads to storage epochs and lease deadlines`；此时新投影能力仍未开放。

## Task 3：投影元数据与选择性候选RPC

**Files:** 新query/read_projection.py、rpc/projected_reads.py；storage三层及LeaseIO；新tests/unit/test_read_projection.py、test_projected_read_frames.py、tests/integration/test_projected_storage.py。

**Interfaces:**

- `required_metadata_fields(plan: VectorPlan, projected: bool) -> tuple[str,...]`：canonical id/category/timestamp/value，id永远在首，full返回全部；不能改plan.metadata_sql。
- `project_metadata_sql(metadata_sql: str, fields: tuple[str,...]) -> str`：SQLGlot AST替换SELECT，仅接受规范字段集合；禁止字符串拼接用户列名。
- Database新增`_scan_projected(token,sql,fields)`和`_read_projected_candidates(token,sql,ids,fields,vector_ids)`，Catalog对应封装；rows为按fields顺序tuple；候选返回`(rows, returned_vector_ids, float32_vectors)`，字段列id固定首位。
- `validate_projected_row(row, fields) -> dict`；`decode_projected_candidates(reply, lease, checked_ids, fields, vector_ids) -> tuple[list[dict], list[int], np.ndarray]`；scan校验器保持total/offset/complete状态，结束时必须验证完成。
- LeaseIO增加`scan_projected(sql,fields)`、`read_projected_candidates(sql,ids,fields,vector_ids)`；所有调用经原call/stream记账及_remaining检查。

- [x] 写字段/AST测试：`SELECT id AS x,distance`只回id；WHERE category照常有效；SELECT *返回完整，未知字段/乱序/重复拒绝；质量metadata_sql原值不变。
- [x] 写真实SQL/RPC测试：checked_ids=[3,1,2]，matched=[3,2]，vector_ids=[1,2]，返回metadata=[3,2]、vector_ids=[2]；未命中的未知ID依然失败；vector_ids必须是checked_ids的有序子集。
- [x] 写流损坏测试：漏optional、额外字段、wrong revision/token、错vector顺序、漏交集ID、超大/非有限payload、缺/重复COMPLETE；无部分成功。
- [x] 写大category拆帧及单行4MiB失败测试、1MiB向量限制、共享4重操作槽满仍可Fetch/EndRead；大字段不能靠多发未经验证帧绕限。
- [x] 先观察上述测试失败；实现新SQL及RPC，计划marker包括fields和选择性向量模式，仍每租约至多一份64KiB源SQL/生成SQL缓存。不要把动态ID具体值塞进SQL计划键或SQL文本。
- [x] 通过`test_projected_storage.py test_projected_read_frames.py test_read_projection.py test_fused_candidates.py test_p3_storage.py`后公布data_read_version=1；提交建议`feat: add projected and selective vector read RPCs`。

## Task 4：separate路径缓存接入与统计

**Files:** 新compute/data_reader.py；compute/vector_execution.py、sql_service.py/service.py；新tests/integration/test_data_cache_execution.py，扩展test_p3_lease_io.py。

**Interfaces:**

- `CachedLeaseReader(io: LeaseIO, cache: VersionedVectorCache | None, fields: tuple[str,...], projected: bool, cache_enabled: bool)`。
- `fetch(ids: list[int], *, scan: bool=False) -> np.ndarray`：批内cache.batch，remote缺失分批原io.fetch，然后finish；scan且scan_admission=False仅lookup。bypass沿原fetch，不伪装缓存已服务。
- `scan(sql) -> Iterator[list[dict]]`、`filter(sql,ids) -> list[dict]`按projected选择新/旧RPC；T4仅接separate。`describe() -> dict`返回请求局部统计。
- execute_vector新增可选data_cache参数（默认None），服务拥有一个cache、close清理；io.open成功后、任何缓存访问前调用configure_cache_lease，cache关闭无需新epoch；索引build/publication不传cache。计划字段按设计§8。

- [x] 先写cold/warm/partial真实RPC测试，固定64条：暖读取vectors_fetched=0，vectors_served_from_cache=64，rpc_calls减少1，结果及工件身份一致；部分hit保留调用方顺序，cache off不touchLRU。
- [x] 写C03/C04/C06测试：不同WHERE共享向量但结果不同；旧租约跨写入、新revision/他表写入/新table_id隔离；同revision换artifact仍可用数据但不能用旧ANN；CORRUPT不可因缓存成功而回退。
- [x] 写exact默认lookup-only、scan-admission开关、精确回退、不足K/空结果测试；snapshot/publish/load从不填缓存。
- [x] 写EXPLAIN无lookup/touch、ANALYZE一次、并发计数局部、全命中TTL/取消、clear竞态测试；只读取已通过RPC校验的向量。
- [x] 先RED后接入；运行新execution测试、test_p4_index_consistency.py、test_p3_execution.py、test_index_artifact_loading.py。通过后提交建议`feat: reuse validated vectors across leased queries`。

## Task 5：fused缓存交互及按需输出物化

**Files:** data_reader.py、vector_execution.py；新tests/integration/test_cached_fused_reads.py、tests/unit/test_projected_topk.py。

**Interfaces:**

- `CachedLeaseReader.filter_fetch(sql: str, ids: list[int]) -> tuple[list[dict], np.ndarray]`：先获取batch并得missing_ids；新RPC传全部ids、fields、missing_ids；完整校验后batch.finish(matched_ids,remote_ids,vectors)。cache off+full走原io.filter_fetch，projected无cache仍走新RPC。
- 计数hit_rows为lookup命中、vectors_served_from_cache只计通过WHERE的命中；请求local计数随递归拆分只累计实际执行，不能二分父节点和子节点双计lookup。
- `TopK(..., materialize_vector: bool | None=None)`增加可选参数；优化模式只保留选中K行vector拷贝，最终输出一次转换；旧full默认路径保持。

- [x] 写全miss/全hit/partial、缓存命中但WHERE失败、多个候选轮次无重复、精确回退、GPU→CPU同工件回退测试。暖fused仍1次过滤RPC但vector裸字节为0。
- [x] 写完整交集响应被破坏时，即使坏ID在缓存中也DATA_LOSS；超4MiB二分与单行失败；workspace满不得偷偷改requested fused为separate，只跳过缓存并记录bypass。
- [x] 写TopK id/distance/alias/*/vector投影、同距离id排序、K大于合法数；无vector投影不创建候选vector列表，有vector仅为保留行拷贝且不挂住整批buffer。
- [x] 运行失败测试→实现→同测试及test_fused_candidates.py、test_gpu_execution.py、test_p4_gpu_restore.py。GPU必须真实运行，CPU-only不能算此任务GPU通过。
- [x] 通过后提交建议`perf: fetch only uncached candidate vectors and required output fields`。

## Task 6：用户配置、能力兼容和本地缓存管理

**Files:** SearchOptions/execution_settings、sql.proto/生成文件、server/service、sql_client、shell各入口；新tests/unit/test_data_cache_settings.py、tests/integration/test_data_cache_controls.py。

**Interfaces:**

- SearchOptions新增data_cache='default'、metadata_projection='default'；proto字段13/14。CLI参数及默认值按设计§5/7；`.search`/SETTINGS覆盖会话，普通SQL不能携带向量新参数。
- GetDataCacheStats/ClearDataCache请求固定新增专用`DataCacheRequest { string database_id=1; }`；返回ControlResponse，type/version明确为data_cache_version=1，stats区分process与database。
- `SessionClient.data_cache_stats() -> dict`、`clear_data_cache() -> dict`只调用计算服务RPC；shell/cache_commands.py解析`.cache stats|clear`，不支持隐式清存储。
- 向量完成帧确认requested/effective cache和projection、data_read_version；cache因workspace bypass属于已执行策略而非协议不支持，不能被客户端误拒绝。

- [x] 写合法/非法/重复SETTINGS、启动0MiB+request on拒绝、off不touch、旧计算忽略选项拒绝、旧存储缺epoch/读取能力拒绝、新服务关闭模式兼容。
- [x] 写热会话server_instance_id改变后能力重新获取、普通CRUD成功确认不被cache选项污染；clear当前库不清其他库/索引/持久数据，未选库拒绝。
- [x] RED→实现并生成proto；运行新settings/controls及test_execution_settings.py、test_capability_cache.py、test_shell_ui.py。测试后补help/补全，提交建议`feat: expose independent cache and projection controls`。

## Task 7：真实双节点、故障和隔离验收

**Files:** 新rpc/p4_cache_acceptance.py、scripts/check_p4_cache.py、tests/integration/test_p4_cache_acceptance.py、test_data_cache_recovery.py。

**Interfaces:** `run_cache_acceptance(output_dir: Path, rows: int=10000, dimension: int=128, require_gpu: bool=False) -> dict`；output必须不存在，异常也写报告和清进程。报告只对实际运行项标PASS，关联pytest故障证据；不虚构覆盖矩阵。

- [x] 先写小fixture验收测试：真实PTY `.cache stats/clear`、SQL SETTINGS、source摘要、每场景判据、错误退出不残留进程、输出不覆盖；初始缺工具失败。
- [x] 实现存储+独立A/B进程，A暖/B冷同租约快照答案；写入后新revision/旧在途读；同revision换工件；A重启空cache但published preload不训练；存储重启epoch变化且老cache不可命中。
- [x] 用Event/管道固定暂停点，验证命中后取消、clear期间下载、DROP重建、受控同revision备份恢复、缓存损坏协议拒绝；存储停机不能从cache离线开始查询。
- [x] GPU worker及A/B对data-root/工件文件Landlock实际拒读，缓存命中不构成直接读文件；真实GPU至少一个支持负载验证，不扩大既有GPU质量范围。
- [x] 定向测试通过后运行新目录1万、10万、--require-gpu验收；保存C01—C14映射、PID/隔离/逐查询/计数/失败记录；提交建议`test: validate cached reads across revisions and process restarts`。

## Task 8：配对成本、消融与最终交付

**Files:** 新benchmarks/p4/data_reads.py、tests/support/delayed_storage.py、tests/integration/test_p4_data_read_benchmark.py；新docs/p4-cache.md、validation/p4-cache.md；更新AGENTS/CHANGELOG/README/benchmarks/p4/README.md。

**Interfaces:**

- `run_data_read_matrix(output_dir: Path, *, tier: str='smoke', seed: int=20261019, repeats: int=3) -> dict`，tier=smoke/correctness/costs/gpu；场景与样本固定见设计§9。smoke使用1024行、4维、2查询，仍3重复；仅验证工具，不代表正式成本。
- 测试代理在loopback透明转发SQLStorage RPC，只给读取RPC在转发前及首响应前各加delay_ms=0/1/5；后续stream帧不重复加传播延迟，其他方法不加延迟。保持deadline剩余预算、取消和流完整性，异常不重试。正式运行前probe验证实际RTT和两端结果相同，代理不写入生产server参数。

- [ ] 写smoke测试校验随机化配对、3重复、cold先clear、warm不clear、所有开关/参数/seed持久化、原始查询记录/资源非空、失败不算成功、样本不足不输出“提速”。
- [ ] 实现报告，复用既有资源采样工具但保留作用域：5ms RSS/CPU、cache记账/工作区、适用GPU显存、实际Protobuf和原始计时；向量裸字节推算单列。
- [ ] 跑correctness/costs/gpu三个tier串行；基线与优化运行源码相同、参数与质量相同；按设计§9明确规模/独立相关/过滤/K/并发/冷暖/独立查询/热点/容量/延迟组合。记录全部退化和未支持负载，不能用P4原固定向量成本代替新基线。
- [ ] 运行`env -u PYTHONPATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1 conda run --no-capture-output -n vecstor python -m pytest -q --junitxml=results/p4-cache-final-YYYYMMDD/pytest.xml`，输出目录在执行时新建。通过定义为无失败且要求GPU项无跳过，记录实际数量，不能照抄历史691。
- [ ] 一次整体独立审查，重点见下文；Critical/Important先失败回归后修复，再按影响运行验证。性能测量若早于热路径修改必须重测受影响组或明确旧版本不可用于当前性能结论。
- [ ] 协议再次生成逐字节检查、文档链接/状态自审、C01—C15逐项关联真实JUnit/报告；只有全部必需判据通过才标缓存子阶段完成。
- [ ] 更新所有文档及实测限制；在已授权功能分支提交建议`test: document bounded data cache and transfer ablations`。合并/推送另按用户授权处理，自适应策略仍未实施。

## Review Focus

1. 同revision存储重启/回滚复用旧cache：T2/T7的epoch与受控备份恢复，不能仅测新revision。
2. cache hit绕开未知候选/WHERE/损坏工件检查：T3/T4/T5，特别是命中却被过滤的向量计数。
3. bytes view挂住大缓冲、clear后旧下载重新插入、工作区容量不退还：T1并发/所有权测试，T7真实取消。
4. projected漏字段零值误判、SELECT *漏vector、分帧截断或fused漏交集：T3/T5协议和答案测试。
5. 普通CRUD已提交却因cache确认被报失败，或并发全局差值伪造每请求命中：T4/T6控制与统计测试。

## 覆盖与里程碑

| 设计章节 | 任务 | 完成门槛 |
| --- | --- | --- |
| §1—3范围/成本 | T8 | 新同版本基线与原成本区别清楚 |
| §4身份/期限 | T2/T4/T7 | 旧scope不串用、所有hit也不能越TTL |
| §5预算/并发 | T1/T4/T6 | 记账/条目/工作区上限、clear竞态通过 |
| §6读取/投影 | T3/T4/T5 | 等价答案、miss子集、字段/流完整性 |
| §7协议/入口 | T2/T3/T6 | 显式选项能力确认，旧关闭路径兼容 |
| §8指标 | T4/T6/T8 | 请求局部计数、真实RPC与推算分列 |
| §9矩阵 | T7/T8 | C01—C15证据，无未运行项冒充PASS |
| §10局限 | T8 | 默认开关、内存重复/物化/旧部署边界明确 |

M1=T1—T3协议与预算基础；M2=T4—T6可使用的独立优化；M3=T7故障与双节点；M4=T8实验及最终交付。若M2收益不明显，仍完成正确性和失败记录，不扩大到自动策略“补救”。

本计划建议延续由同一执行者逐任务实施、最终一次独立审查的方式，因为scope/协议/计数接口高度相互依赖；本轮只完成文档，不启动实现或审查代理。

## 2026-10-19实验范围调整

用户已明确选择“先跑代表性正式矩阵，保留全量复现入口”。T8保留correctness/costs/gpu完整tier，新增representative tier执行本轮代表性实验；三次重复和独立开关仍保留，样本数、数据规模/分布、负载、并发/冷热、容量/延迟及GPU覆盖逐项写入配置和报告。未执行的全量组合明确标记NOT_RUN，不能宣称原200查询全矩阵通过。
