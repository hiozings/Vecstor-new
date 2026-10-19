# P4 有界数据缓存与传输优化设计

日期：2026-10-19。状态：**实施中：T1缓存内核及T2租约协议完成，查询与投影读取尚未接入**。初始规划轮交付设计与实施计划；2026-10-19起按用户授权逐任务实施，不新增依赖。实施入口见[八任务计划](../superpowers/plans/2026-10-19-p4-data-cache-and-transfer.md)。

## 1. 目标与范围

在已完成的持久工件、双节点读租约和零训练恢复基础上，减少重复向量读取与不必要字段传输，同时维持原查询答案、候选集合、float64重排、版本和故障语义。成功意味着可独立启停、有容量边界、实际少传数据且可解释退化，不以预设延迟提升比例验收。

本期实施计算节点私有的内存向量缓存、只请求未命中ID、元数据按投影返回、fused路径选择性返回未缓存向量、统计和验收工具。沿用现有1024行/1MiB批量读取，不新增Arrow Flight、压缩、异步预取或跨请求合批。保留数据库级revision；不缓存谓词结果、元数据、Top-K答案、索引工件或GPU显存，不改变ANN参数、质量配置或设备策略。自适应路径选择、增量索引维护、跨节点缓存同步和超内存检索另行设计。

普通SQL/CRUD与v1查询接口不接缓存；目标是v2向量查询。默认关闭缓存和按需字段优化，默认local、CPU exact/filter-first、已有fused开关语义保持。

## 2. 现有代码与证据

### 2.1 实际接入点

| 位置 | 已有行为 | 本期变化 |
| --- | --- | --- |
| compute/lease_io.py | 每请求BeginRead/EndRead；scan/filter/fetch/filter_fetch均校验token/revision、顺序和完整性 | 新的独立读取适配层复用这些边界；缓存仅覆盖向量读取 |
| compute/vector_execution.py | exact逐批scan→fetch；filter-first ANN先保留允许行；search-first按候选轮次filter→fetch或fused | 接入按需读取，不改候选预算和路径；TopK只为最终输出转换向量 |
| storage/database.py::_scan | 用metadata_sql读取四个元数据字段，返回完整匹配集合 | projected路径只替换SELECT列表；保留WHERE/参数/租约 |
| storage/database.py::_filter_candidates | 验证全部候选存在，再按绑定ID过滤；每租约缓存一份SQL计划 | 新路径仍检查全部ID，计划marker加入字段集合和向量选择模式 |
| storage/sql_service.py | 8线程，4个共享重操作名额，FetchVectors保留容量；消息4MiB | 新RPC复用同4名额；分帧受序列化字节约束 |
| query/statements.py::VectorPlan | projection含输出名→来源字段；metadata_sql经SQLGlot生成 | 从来源字段求必需元数据；不得解析字符串或从SQL文本猜字段 |
| query/search_options.py / execution_settings.py | 不可变请求选项；SQL SETTINGS覆盖会话 | 增加缓存和字段策略选项，明确能力和完成帧确认 |
| compute/sql_service.py / rpc/sql_client.py | 能力握手、server_instance_id、热会话缓存 | 新选项不能被旧服务忽略；普通SQL不误加向量确认要求 |

当前scan在数据库队列内物化全部匹配元数据后再分批发送；本期不把它改成游标分页。因此本设计的有界保证属于**新增缓存及其批内临时内存**，不宣称整个查询/服务RSS有硬上限。filter-first ANN的allowed_rows、现有seen集合和Faiss驻留也仍需单独计量。

### 2.2 已测成本及推论边界

本轮直接读取 `results/p4-lifecycle-costs-20261018/*/report.json` 的逐查询计划。来源版本f3a00c9，1万/10万×128、独立分布、每组3次，固定向量、全nprobe、热索引；下表的RPC等待/native/字节为合并原始样本中位数，不与“各次P50中位数”混用。

| 行数 | 并发 | 样本 | RPC等待ms | native ms | RPC/查询 | Protobuf字节/查询 | 读取向量/查询 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1万 | 1 | 12 | 21.85 | 0.52 | 4 | 39102 | 64 |
| 1万 | 4 | 48 | 35.85 | 0.34 | 4 | 39102 | 64 |
| 1万 | 8 | 96 | 39.70 | 0.29 | 4 | 39102 | 64 |
| 10万 | 1 | 12 | 18.70 | 3.66 | 4 | 39106 | 64 |
| 10万 | 4 | 48 | 41.10 | 3.86 | 4 | 39106 | 64 |
| 10万 | 8 | 96 | 47.06 | 3.29 | 4 | 39106 | 64 |

64×128×4=32768字节是这类请求的向量裸数据量；全部命中时理论上可省一次FetchVectors和这些裸数据，但BeginRead、过滤和EndRead仍需网络。RPC等待含服务执行/排队、序列化及传输，不能解释成纯网络延迟。固定向量重复使该负载特别容易命中，不能据此预测独立查询命中率。

[GPU G2证据](../validation/gpu-improvements.md)中4→3次RPC并未普遍提速：10万/10%/K10并发8的GPU P50为142.92→168.38ms；10万/100%/K100为216.68→183.30ms。合并读取也改变存储SQL形态，不能全部归因于少一次往返。新系统上的[索引成本](../validation/p4-indexes.md)与旧系统G2数据不是同机同版本对照；本期必须重新建立配对基线。

## 3. 方案选择

| 方案 | 收益 | 代价 | 决策 |
| --- | --- | --- | --- |
| 按版本+ID缓存原始float32向量 | 不绑定SQL或候选顺序；exact/CPU/GPU重排共用；身份容易验证 | 每行对象开销、冷扫描污染、仍需过滤RPC | **首期采用** |
| 缓存整批Fetch响应或整条过滤结果 | 批级管理简单、同SQL重复可能少更多RPC | 候选集合/顺序不同则命中低；过滤结果完整性、内存与失效更复杂 | 暂缓 |
| 缓存整表/从Faiss重构向量替代RPC | 可减少大量读取 | 与索引重复内存、绕开权威读取契约，易把索引当权威数据 | 不采用 |

先落地缓存，再实现字段投影和选择性fused返回；两个优化单独开关、分别消融。没有收益也保留原始结果，默认不因“少RPC”自动打开fused。

## 4. 缓存身份与一致性

### 4.1 Scope

`DataScope(storage_epoch, database_id, table_id, revision, schema_version, dimension, dtype='float32-le')`，每条记录键为`(scope,id)`。id必须非负int64，禁止转float。原始向量与构建参数/工件身份无关，故不加入artifact_id或publication_generation：同revision换索引配置可以复用向量，**查询使用的索引仍必须通过已有租约工件验证**，不能由缓存绕过CORRUPT、缺工件或质量绑定。

存储进程启动生成新的随机`storage_epoch`，新增到BeginRead响应；缓存开启必须取得该字段。不能仅使用地址或持久database_id：存储恢复旧备份后，原revision数值可能再次出现。旧服务无epoch时，缓存关闭路径仍兼容；显式开启缓存则FAILED_PRECONDITION，不静默启用不安全键。epoch只参与数据缓存，不变更既有IndexKey和工件格式。

每次查询照常远程BeginRead，固定scope；WHERE由该租约在存储侧执行。缓存命中不能跳过ScanMetadata/FilterCandidates的过滤和候选存在性校验，不缓存“不存在”或“不匹配”。不同SQL可共用同一向量字节；不能共用WHERE结果。

写入使新查询使用新revision，旧条目不会命中；正在运行的旧租约允许读旧scope。不同表写入同样保守失效。DROP重建由新table_id隔离。旧scope可按LRU回收，无需全局广播或等待其他节点；命中率可不同，答案及工件身份必须相同。

### 4.2 期限与故障

BeginRead新增`effective_ttl_seconds`，由存储返回实际授予TTL。计算在发送BeginRead之前记单调时刻t0，缓存使用截止时间取`min(query_deadline, t0+effective_ttl_seconds)`；要求0<TTL≤请求TTL≤60。新字段用于保守本地期限，不比较两机绝对时钟。等待、查缓存、合并结果和最终完成帧之前检查取消与期限，过期不能只凭内存完成。

每请求至少执行一次存储过滤；缓存不提供离线查询能力。若存储在最后一次有效数据读取后退出，现有查询在租约期限内可能完成，EndRead仍是best effort；不承诺“服务故障瞬间已取得数据的请求全部失败”。故障前已完成的RPC不作废；后续新BeginRead或过滤失败必须向上传播。

只允许完整通过版本/ID/长度/有限值校验的RPC单元进入缓存。截断stream可保留其此前已验证的独立向量批，但失败查询不能返回部分成功。未知ID、错revision、错epoch、DATA_LOSS不得转换成cache miss并重试掩盖错误。缓存优化不新增读重试。

## 5. 容量、所有权与并发

配置拟定：`--data-cache-mib 0`默认关闭；显式开启建议64MiB；`--data-cache-max-entries 65536`；`--data-cache-scan-admission off|on`默认off。与CPU索引256MiB、GPU预算独立，报告总RSS；不自动挪用索引预算。

记录保存独立复制的不可变`bytes`，一条恰为dimension×4，不保存整个RPC缓冲区的view，以免一行引用挂住1MiB。LRU按记录淘汰，读/写命中更新顺序。记账契约为每活跃scope1024字节、每记录512字节管理额度加payload字节；同时限制条目数。名称使用`accounted_bytes`而非实际RSS；这些固定管理额度是可复现预算模型，不是Python allocator精确硬限。空scope立即删除。128维、64MiB默认建议值最多约65535行（还需scope开销）；10万行全驻留需约97.7MiB，不能称64MiB容纳10万行。

`put`在锁内检查/淘汰再发布，`accounted_bytes≤budget`始终成立。单条超过容量则bypass，不报查询失败。已有key再次出现相同字节为幂等；不同字节是`DATA_LOSS/cache_content_conflict`，不覆盖。

不引入single-flight、不让查询等另一请求的网络结果。并发相同miss允许重复RPC，统计重复插入；首期以减少锁/取消耦合为代价接受冷并发重复流量。

缓存读取将命中向量复制到批内工作区，不向调用者暴露缓存对象或在请求中长期pin。锁内仅查找/LRU/复制/记账，锁外RPC、距离计算和协议解码。缓存可在拷贝完成后安全淘汰。新增工作区预算16MiB、最多4个活跃批、每批预留4MiB（命中/返回/组装缓冲），两者都非阻塞准入；超额请求直接走原未缓存读取并记录`workspace_busy`。输入批仍≤1MiB向量/1024行，获取工作区后才复制。现有TopK/allowed_rows/响应解码等内存单列，不把工作区上限当进程硬限。

exact的全量scan默认允许lookup但禁止新插入，避免一次顺序扫描挤掉热点；filter-first ANN和search-first的实际重排向量可插入。显式scan-admission=on用于exact冷暖实验；此选项不影响答案。索引构建快照、publish/load/preload不填充数据缓存，也不从缓存替代发布收据。

清理拟提供计算服务本地`.cache stats`、`.cache clear`，要求已选库，仅清该database_id；不修改存储/索引/数据revision。返回process总容量及选库占用，不能混称选库容量。clear递增全局admission_generation；正在下载的旧代次结果仍可服务当前请求，但不得重新填充缓存。该保守代次会使其他库在途插入也跳过一次，避免维护无界库代次表。进程重启自然为空，双节点各自管理。

## 6. 读取和字段传输

### 6.1 separate与批量miss

1. 存储过滤返回有序元数据ID；检查合法性和完整性。
2. 为本批申请工作区，按原ID顺序查scope缓存，收集miss。
3. 按min(1024, floor(1MiB/(dimension×4)))分批调用原FetchVectors；全部响应检查通过后，按原位置组合。
4. 已命中行不再传输；全命中时本批不调用FetchVectors。零行无向量RPC。
5. 更新请求计数、按策略插入；重排始终使用完整相同float32数组和CPU float64累加。

exact即使查询不投影vector也需要全部合法向量计算距离，不可能只读取最终K的向量。过滤字段仍在存储端读取；字段投影只减少返回数据，不代表WHERE物理扫描变少。

### 6.2 projected元数据

从`VectorPlan.projection`来源字段取category/timestamp/value，加永远必需的id，按固定顺序`id,category,timestamp,value`生成字段集合；distance在计算端生成，vector由向量路径获取。`SELECT *`要求四个元数据字段加vector，不能做成id-only。别名和WHERE列分别处理；WHERE中的category未投影时仍参与存储过滤，但无需回传。

存储在已校验AST上替换SELECT列表，不改predicate，不改质量配置使用的原metadata_sql身份。新RPC使用存在性可检查的typed optional字段，id=0、timestamp=0、value=0不可误作缺字段。行的字段存在集合必须与请求精确相同，拒绝漏字段/额外字段；响应回显字段集合。

TopK在各批挑出保留项后仅保留K行需要的metadata/距离。只有投影包含vector时保留选中K行的独立float32拷贝，最终输出时才tolist；无vector投影时不为每个候选构建128个Python float。最终id距离排序、输出列名/类型保持不变。该物化优化仅在projected开关打开时使用，以便与旧路径做完整对照。

### 6.3 fused与部分命中

维持用户显式的candidate_data_path规则，不根据命中率自动改为separate。新候选读取RPC接受完整`checked_ids`和`vector_ids`（待补向量ID子集），存储验证所有checked_ids存在并执行WHERE，再仅为`matched_ids∩vector_ids`读取向量。缓存命中未通过WHERE的ID不参与重排。

返回的vector_ids必须等于checked_ids原序下的上述交集，vector字节顺序逐项对应；漏一个都DATA_LOSS。不允许用缓存掩盖错误响应。全命中也要远程过滤，只是vector_ids为空；全miss与现有fused结果等价。新存储实现选择向量列的方式必须显式限定缺失ID，不能传了子集却在内部读取全部匹配vector再丢弃。

该实现可能用“存在性检查→投影过滤→缺失向量查询”三个同租约SQL，原fused为不同SQL形态，新增执行成本必须独立记录。没有性能证据前不取代旧fused：缓存off且projection=full仍走旧RPC。缓存on或projected请求使用新RPC；指标标出legacy/selective协议，消融解释这项交互。

新候选响应过4MiB时，只对明确`projected_response_too_large`做有界二分拆分，最多1024输入带来的11层，保留顺序。单ID仍超限明确RESOURCE_EXHAUSTED，不无限重试。新扫描RPC按实际ByteSize拆帧，同时满足行上限；单行超限明确失败。旧RPC行为不变。

## 7. 协议与控制面（均为拟新增）

只在sql.proto增加字段/消息/RPC，旧字段语义不变，运行generate_proto.py同步四个生成模块。

- ReadLease新增`string storage_epoch=13`、`double effective_ttl_seconds=14`。
- SearchOptions新增两个string字段（当前最大字段号12，分别使用13、14）：`data_cache='default'|'off'|'on'`、`metadata_projection='default'|'full'|'projected'`。协议缺省空串解码为default。
- 新消息`ProjectedRow`：`optional int64 id=1`、`optional string category=2`、`optional int64 timestamp=3`、`optional double value=4`，所有字段值继续原数据契约。
- `ProjectedScanRequest`：database_id/token/sql/batch_rows/repeated string fields（1—5）。`ProjectedMetadataBatch`：token/revision/total_rows/offset/fields/rows/complete（1—7）；最后一帧complete=true，空结果一帧，流结束核对total和最后标记，禁止重复最终帧。
- `ProjectedCandidateRequest`：database_id/token/sql/checked_ids/fields/vector_ids（1—6）。`ProjectedCandidateResult`：token/revision/checked_ids/fields/rows/dimension/vector_ids/vectors_f32_le（1—8），unary响应；separate仅vector_ids空，fused为miss子集。
- SQLStorage新增`ScanProjectedMetadata` stream和`ReadProjectedCandidates` unary。FetchVectors继续复用。新接口同样校验租约和预算、共享4扫描槽；不占用专用FetchVectors容量。
- 存储capabilities新增`data_read_version=1`、`data_cache_epoch_version=1`；计算GetSearchCapabilities新增`data_cache_version=1`、`metadata_projection_version=1`、cache配置/可用状态。新RPC请求不得依赖服务端忽略未知字段来“兼容”。
- SQLCompute新增GetDataCacheStats/ClearDataCache（请求database_id，响应沿用ControlResponse JSON），不透传存储Control。CLI为`.cache stats|clear`。

启动参数`--metadata-projection full|projected`默认full；缓存是否默认打开由data-cache-mib>0决定。逐请求off不查不写缓存；on在配置容量0时拒绝；default继承启动配置。显式full覆盖启动projected。会话`.search`及SQL SETTINGS支持同名字符串选项；`.reset`清选项，不清缓存。拟例：`SETTINGS data_cache='on', metadata_projection='projected'`。

启动配置或请求显式需要新能力而存储不支持时明确拒绝，不静默全字段或无缓存。纯旧路径继续工作。客户端按server_instance_id复用能力并检查向量完成帧确认；若旧计算服务忽略新选项，拒绝成功。普通SQL不能因会话开启缓存而被错误要求向量完成帧确认。

## 8. 指标与可观察性

计划新增`data_read_version`及`data_cache`对象：requested/effective/reason、scope（epoch等，不含token）、lookup_rows/hit_rows/miss_rows/bypass_rows/admitted_rows/admitted_payload_bytes、evictions、lookup_ms/copy_ms/admit_ms、scan_admission、accounted_bytes/entry_count快照。统计直接由当前请求累加，不能用全局计数差在并发下归因。

定义lookup_rows=hit_rows+miss_rows；bypass不进入分母。命中率无lookup时为null。`vectors_fetched`继续表示真实网络返回行数；增加vectors_served_from_cache、vectors_used_for_rerank，不能把命中记成网络读取。fused预过滤lookup的hit_rows可包含最终被WHERE拒绝的候选，不能把这些行计为实际服务命中；`cache_payload_bytes_avoided=vectors_served_from_cache×dimension×4`仅为向量裸字节推算，不是实际节省的网络流量。

保留rpc_calls、rpc_payload_bytes、RPC等待口径；新增metadata_requested_fields、metadata_returned_rows、candidate_read_protocol、read_subbatches和分帧/拆分计数。实验比较实际Protobuf总量，不能拿推算的avoided值减旧总量伪造实际结果。物理扫描行数仍为null，未测量的服务端分量不虚构。

EXPLAIN只取得租约/索引和配置，缓存不得lookup/touch/insert/clear，不改变LRU；预测命中率标为unknown。ANALYZE只执行一次。输出实际requested/effective/cache bypass；设备、质量和精确回退的既有字段完整保留。

## 9. 验收矩阵

| 编号 | 必须证明的行为 |
| --- | --- |
| C01 | off/default旧路径结果、RPC和能力行为兼容；普通SQL/CRUD不被新确认误拒绝 |
| C02 | 冷miss→暖hit→部分hit：结果一致、向量RPC实际减少、顺序正确；id>2^53、负零/有限float32逐字节保持 |
| C03 | 同scope不同谓词仍远程过滤；未命中/不存在ID不能被缓存当作普通不匹配 |
| C04 | 写入revision、其他表写入、DROP同名重建、两库同ID隔离，旧租约跨写入仍返回旧答案 |
| C05 | 存储重启/还原相同revision时epoch改变；计算重启空缓存；双节点冷热不同但同快照答案一致 |
| C06 | 同revision不同artifact仍可命中向量，但索引head/质量摘要/CORRUPT检查不可绕过 |
| C07 | LRU顺序、容量/条数上限、单项过大bypass、并发冷miss、clear与在途填充、超额工作区bypass，无泄漏 |
| C08 | 缺字段、额外字段、错ID/顺序/长度/epoch/revision、NaN、截断scan/重复COMPLETE失败，不激活坏缓存 |
| C09 | 命中路径取消/TTL/总deadline生效；无无限等待；终态前检查；EndRead best effort明确 |
| C10 | id-only、别名、distance、SELECT *、WHERE不投影列、空结果、同距离排序结果完全一致 |
| C11 | fused全miss/全hit/部分hit，过滤与向量交集覆盖；小批/大字段拆分和单行超限；四扫描槽满时Fetch仍可用 |
| C12 | exact扫描默认不污染；scan-admission on可预热；ANN filter-first/search-first以及精确回退均正确 |
| C13 | EXPLAIN无缓存触碰/数据RPC、ANALYZE一次、并发每请求统计不串；旧服务不忽略显式新选项 |
| C14 | 真实PTY操作缓存、两个计算进程、Landlock拒读目录/工件、CPU及真实GPU同一输入对照 |
| C15 | full/projected×cache off/on×separate/fused独立消融，资源/退化/零命中结果完整归档 |

### 实验方法

先在最终业务源码和迁移后环境重测off/full基线，再以相同数据、索引工件、nprobe/候选参数、设备及查询序列配对测试。冻结参数，不用新评估集调ANN。exact逐条匹配独立NumPy真值；ANN优化开关前后ID/距离/实际回退一致，并报告既有Recall，不能只看平均召回掩盖答案变化。

基础矩阵1万/10万×128、独立/相关、过滤比例0.1%/1%/10%/50%/100%、K10/100；每场景200条固定查询种子20261019，各配置3次，配置次序固定随机化并记录。先并发1正确性矩阵：exact、CPU ANN两路径；search-first加2×2×2开关组合，其他路径2×2；fallback固定error或exact各有明确场景。质量不足/不支持的原ANN参数保留实际状态，不临时调参。

成本子集使用10%/K10、100%/K100和一个选择性强的0.1%/K10负载，各规模/分布、并发1/4/8、相同200查询/3重复。每次清空缓存（不清索引），分开测：第一遍cold、相同查询序列第二遍warm、不同种子20261020的independent、受控20条热点占80%的hotset；后两者不得声称一定零/高命中，报告实测。另用8MiB小容量驱逐压力与64MiB建议容量做消融；预算作为独立配置，不只挑完全容纳数据的案例。

GPU只选原有支持且冻结质量配置可应用的search-first负载，至少一个10万宽过滤K100和一个已支持选择性负载，无法满足质量证据则显式跳过该负载并记录原因；真实GPU功能检查不能全部跳过。复用既有留出集是回归不是新泛化证据。

以同机Landlock为主；另用测试专用转发代理给每个读取RPC转发前及首个响应返回前各增加固定单向1ms/5ms；stream后续帧不重复加固定传播延迟，不模拟带宽/拥塞。记录实际RTT及代理开销。不修改系统tc或服务生产延迟，不把模拟延迟称跨机实测。代理路径与直连数据逐条一致。

输出P50/P95、吞吐、Recall、结果不足、RPC/字节、cache hit/bypass/eviction、accounted_bytes/工作区峰值、各进程RSS/CPU和适用GPU显存。每5ms采样可能漏峰，冷指应用缓存已清理，OS页缓存未清。完整基线/随机顺序/源码SHA/锁定环境/每查询原始记录必须保留。性能无固定提升门槛：C02中可控全hit应消除对应Fetch RPC，C10大category未投影应减少实际Protobuf；cold/独立查询退化必须报告，不能因此隐去。

## 10. 实施边界与风险

原始向量缓存会与CPU/GPU索引重复内存；64MiB只是建议起点，验收必须报告全部驻留资源。每行LRU的Python开销和锁复制可能抵消小批收益；不先引入分片、复杂策略或跨请求等待。按需字段无法消除存储的WHERE读取或现有全量元数据物化。fused选择性读取可能增加SQL执行成本，保留原路径并精确归因。

本设计沿用可信网络、单存储进程，无新增鉴权或离线服务承诺。业务实现前审阅本提案及计划；任何扩大到缓存过滤结果、自动切路径、共享内存或后台预热的改动都需更新范围与验收。
