# VecStor 项目开发指南

本文件适用于整个项目，供团队成员与开发代理共同遵循。记录项目实施计划、目录结构、技术栈和开发约束；后续开发应同步维护，避免文档与实际实现脱节。

最后更新：2026-11-10。

## 1. 项目背景与当前状态

- 项目名称：面向混合查询的存算分离向量数据引擎研究。
- 项目类型：大学生创新训练项目；团队名称：VecStor。
- 依据：`README.md` 与 `项目申报书2.pdf`。
- 申报实施周期：2026-01-01 至 2026-12-31。
- 预期成果：一套可运行的混合查询存算分离向量数据引擎原型，以及包含架构设计、优化方案和性能测试的结题报告。
- 已建立 Conda `vecstor` 环境；P1 本地基线、P2 gRPC 服务和 P2.5 交互式 SQL、运行时选库、持久 CRUD 已实现。P2.5 通过真实终端、P1 对照、重启及 Landlock 验收；P3 CPU ANN、双路径、候选扩展和计划展示已实现，通过 427 项回归、1 万行真实终端/Landlock 及 360 次规模实验。GPU ANN search-first 已接入，P4权威索引持久发布、双节点加载/重启恢复及有界数据缓存/按需传输子阶段已完成，自适应策略与增量维护仍待实现。
- 以下排期按 2026-10-10 起剩余约 12 周制定。若存在其他位置的代码或学校提前验收，应先核实再更新排期。

## 2. 目标与范围

围绕“条件过滤与向量 Top-K 联合查询”构建原型，研究执行顺序、缓存和批量传输在存算分离场景下对性能的影响。

### 本期交付

1. 独立存储服务与计算服务，通过网络接口通信。
2. 单表混合查询：字段投影、数值范围、类别相等、AND/OR 条件组合、向量距离排序及 LIMIT。
3. 精确查询基线，以及先过滤、先检索两种执行路径。
4. 根据过滤比例、候选量、传输成本与缓存状态选择执行策略，并展示执行计划。
5. 有容量限制的缓存、批量读取与按需字段获取。
6. 索引持久化、重启加载、批量更新、版本切换，以及两个计算节点的版本一致性验证。
7. 可复现的正确性测试、性能对照与消融实验、部署说明及结题材料。

复杂 JOIN、通用事务、自动分片、分布式共识、流式实时更新和超内存检索不作为初版承诺。涉及申报范围的实质调整需与导师核对。

## 3. 架构与技术栈

### 架构职责

```text
客户端：SQL + 查询向量
           |
计算服务：解析校验 -> 执行计划 -> 条件过滤 / 向量检索 -> 结果融合
           |                  本地有界缓存、可重建索引副本
           |
           +------ 网络批量访问 ------+
                                    |
存储服务：元数据、向量、持久化索引、版本清单
```

- 存储服务持有权威数据；计算服务的缓存和索引副本可删除并重新加载。
- 计算服务不得通过共享目录直接读取存储文件来替代网络访问。
- 初期可同机隔离进程或容器部署；实验需区分同机、跨机和模拟网络环境。
- 存储侧可支持简单过滤下推，但须明确其计算职责并统计资源开销。
- 初版允许计算侧加载完整索引，必须报告内存占用，不宣称支持超内存检索。

### 技术栈与验证状态

| 技术 | 用途 | 状态与约束 |
| --- | --- | --- |
| Python / Conda | 服务编排、查询调度、实验脚本 | Python 3.12.15；复用本机 Conda，独立环境 `vecstor` |
| DuckDB / SQLGlot | SQL 执行、语法树解析 | DuckDB 1.5.6、SQLGlot 30.21.0；持久 CRUD、受控单表 SQL、向量查询及 MVCC 读租约已实现 |
| Faiss CPU/GPU | 向量检索、索引保存与加载 | `faiss-gpu` 1.12.0 同时支持 CPU/GPU；组件级 Flat、IVFFlat 与持久化验证通过；P3 业务接入 CPU IVFFlat 及显式GPU search-first副本；CPU训练/float64重排；P4已实现CPU权威工件、RPC发布/加载与GPU同源恢复 |
| CUDA / NumPy | GPU 运行库与精确计算 | CUDA 12.4 运行库、NumPy 1.26.4；P1 使用 CPU NumPy float64 穷举距离，GPU仅ANN检索，精确计算/重排保留CPU |
| gRPC / Protobuf | 查询、目录、类型化 SQL 结果与读租约 | grpcio/grpcio-tools 1.83.1、protobuf 7.35.1；兼容 P2 v1 并新增 P2.5 v2；P4已接入类型化工件发布/分发与能力确认 |
| prompt_toolkit / Rich | 多行终端、历史/补全与表格 | 3.0.53 / 15.0.0，已通过 Conda 安装并锁定 |
| Arrow Flight | 批量列式数据传输 | 可选，只有测出传输瓶颈后再评估 |
| pytest | 正确性、接口与集成测试 | 9.1.1；云端部署及导入转换修复后948项完整真实GPU回归通过（零跳过），覆盖P1/P2/P2.5/P3/质量/GPU/P4索引与缓存，包含真实 PTY、gRPC、版本索引、并发租约、重启和隔离；命令 `python -m pytest -q` |
| 容器与部署脚本 | 隔离部署、复现实验 | 随原型补充，不先引入复杂集群平台 |

安装状态与业务集成状态必须分开记录。通过 Conda 管理依赖，不混用 uv/pip 覆盖 Conda 管理的 Faiss、NumPy 和 CUDA 库。`environment.yml` 固定直接依赖，`environments/vecstor-linux-64.explicit.txt` 固定全部 Conda 包的构建和 MD5。保留 CPU 后端，GPU 计算使用本机已有设备。

## 4. 查询语义与数据约定

已固定：id 为非负唯一 int64，category 为非空且不含 NUL 的 Unicode 字符串，timestamp 为 UTC Unix 毫秒 int64，value 为有限 float64，vector 为固定维度的有限 float32；不支持 NULL。默认生成 128 维，距离为平方 L2，使用 float64 累加，按 `(距离, id)` 升序排序。完整规范见 [数据与查询规范](docs/design/data-and-query-spec.md)。

本地执行器已支持以下查询形态；向量由调用参数传入，SQLGlot 解析后由自定义执行器调度，不代表 DuckDB 原生支持该向量算子：

```sql
SELECT id, category
FROM items
WHERE category = '论文' AND value >= 10
ORDER BY vector_distance(vector, :query_vector)
LIMIT 10;
```

P2.5 交互 SQL 入口使用内联数组，例如 `ORDER BY vector_distance(vector, [0.1,0.2]) LIMIT 10`，支持距离别名投影。普通表支持基础类型、NULL、单表 CRUD 与聚合，向量表仍保持五字段主键/非空/有限值契约，表名可自定义。新入口 SELECT * 返回所有实际列，包括 vector；P1/P2 旧接口投影规则保持兼容。完整语法和界面命令见 [终端使用说明](docs/sql-shell.md)。

开发顺序与语义要求：

1. 先实现条件过滤后完整计算距离的精确基线，用作标准答案。
2. 先过滤路径：获取符合条件的 ID，批量获取向量，再计算近邻。
3. 先检索路径：扩大近邻候选集后过滤；不足 K 时继续扩展，不能直接过滤全局 Top-K 后就当作正确结果返回。
4. 精确模式必要时回退到完整过滤与精确计算；近似模式必须显式标明并测量召回率，精确重排候选并不等于全局精确检索。
5. 合法记录少于 K 时返回全部合法记录；空结果、同距离并列、无效维度、非法 K 和不支持的 SQL 都需定义行为并测试。
6. 禁止使用正则或字符串拼接作为完整 SQL 解析与参数绑定方案；使用成熟解析能力和明确的语法白名单。

## 5. 实施计划

P1、P2 与 P2.5 技术交付已完成。旧 P2 只读快照模式保留，P2.5 新增托管数据库、运行时选库与事务写入。学校结题要求等外部事项仍待核实；P3 CPU 技术验收已完成，P4—P7 尚未完成。证据见 [P1](docs/validation/p1-local-baseline.md)、[P2](docs/validation/p2-grpc.md)、[P2.5](docs/validation/p25-sql-shell.md) 和 [P3](docs/validation/p3-faiss.md) 验证记录，不得等同于全部项目验收。

| 阶段 | 时间 | 工作与交付 | 验收条件 |
| --- | --- | --- | --- |
| P1 | 10/10—10/16 | 盘点已有工作、核对结题要求；固定数据格式、查询子集和技术版本；跑通 SQL、检索及索引读写 | 技术方案、组件示例、精确答案生成脚本可复现 |
| P2 | 10/17—10/30 | 存储/计算服务、数据导入、批量读取、精确混合查询 | 端到端查询正确；计算服务无法直接访问存储目录仍可运行 |
| P2.5 | 10/12 技术验收完成 | 交互式 SQL 终端、会话选库、托管数据导入、持久 CRUD 与读写一致性 | 319 项回归、真实终端、重启、P1 对照与 Landlock 读写验收通过 |
| P3 | 10/13 CPU 技术验收完成（原排期 10/31—11/13） | CPU IVFFlat、两种执行路径、候选扩展、EXPLAIN/实际计划 | 427 项回归、1万行 PTY/Landlock、1万/10万×128维的360次实验通过；默认 ANN 召回率需按负载校准；显式质量配置验收见下文 |
| P4 | 11/14—11/27 | 缓存、批量传输、策略选择、索引版本发布与批量更新 | 优化可独立开关；重启恢复、更新一致性测试通过 |
| P5 | 11/28—12/11 | 性能对照、消融实验、两个计算节点版本切换 | 原始数据、图表、资源统计、局限性分析齐全 |
| P6 | 12/12—12/25 | 修复问题、部署复现、报告与答辩演示 | 干净环境复现成功，材料形成可提交版本 |
| P7 | 12/26—12/31 | 缓冲、导师反馈和提交 | 冻结代码与最终成果，完成结题 |

P2.5 提前完成了 P4 的部分写入一致性基础，但不代表 ANN 索引更新或优化已实现。设计见 [交互式 SQL 终端](docs/design/interactive-sql-shell.md)，执行记录见 [实施计划](docs/superpowers/plans/2026-10-12-interactive-sql-shell.md)。P3 已复用数据库 revision 和读租约。

P3 CPU 实现见 [使用说明](docs/p3-search.md)、[Faiss 执行设计](docs/design/p3-faiss-execution.md) 和 [六任务实施记录](docs/superpowers/plans/2026-10-13-p3-faiss.md)。保留默认精确模式，显式开启 CPU IVF 的 filter-first/search-first；按快照版本复用索引，候选不足可精确回退，并提供 EXPLAIN/ANALYZE 与实际执行统计。数据库级 revision 保持不变，其他表写入也保守失效。该P3验收时GPU尚未接入；当时未实现权威索引持久化/分发；当前P4索引子阶段已完成，增量维护和自适应策略仍待推进。360 次实验包含 120 次默认精确、168 次真正 ANN、72 次精确回退；默认参数未达到普遍 Recall@10≥0.95，见 [实测召回与资源记录](docs/validation/p3-faiss.md)。

ANN 质量优化四阶段已完成：显式快照绑定配置、质量预算下限、查询内选择器复用及候选批量绑定/租约计划缓存。60 组各 500 个独立留出查询，最低平均 Recall@10=0.9702，零精确回退；最终 720 次隔离 RPC 对照及 96 组成本消融通过。默认 exact 与未选择配置时的 ANN 参数保持不变；不是任意数据或逐查询召回保证。见 [最终验收与成本](docs/validation/ann-recall-optimization.md) 和 [实施计划](docs/superpowers/plans/2026-10-13-ann-recall-optimization.md)。

GPU ANN 已按计划接入：SQL SETTINGS逐请求选择CPU/GPU、显式GPU副本、独立worker/预算/设备回退，首期仅GPU search-first。真实GPU校准60组：36通过、21不支持、3留出完整性失败；失败组不导出质量配置，不承诺所有负载纯GPU达标。CPU训练、精确基线和float64重排保持不变。实现与限制见 [GPU说明](docs/gpu-search.md)、[验收记录](docs/validation/gpu-ann.md) 和 [实施计划](docs/superpowers/plans/2026-10-15-gpu-ann.md)。

GPU G0—G2已实施：修复候选完整覆盖与GPU响应校验，热会话复用查询能力并拆分计时，提供可选候选过滤/向量合并RPC（默认关闭、SQL可覆盖）。原生batch1/4/8探测完成；native占比低且GPU排队短，本轮不部署运行时批调度、批内取消或二进制IPC。自动策略/GPU覆盖扩展仍留给G3—G5；持久版本已由P4索引子阶段接入。见 [改进验收](docs/validation/gpu-improvements.md)、[问题分析](docs/design/gpu-improvement-analysis.md) 和 [执行计划](docs/superpowers/plans/2026-10-15-gpu-improvements.md)。

P4索引子阶段T1—T9已于2026-10-18完成：[设计](docs/design/p4-index-publication.md)、[实施计划](docs/superpowers/plans/2026-10-16-p4-index-publication.md)、[使用说明](docs/p4-indexes.md)、[验收记录](docs/validation/p4-indexes.md)。计算侧显式构建，存储校验/落盘/目录原子发布，读租约绑定权威工件，双计算节点RPC加载；支持批量写入后完整重建、零训练重启、GPU重克隆、v3工件质量绑定及引用保护GC。最终不设置PYTHONPATH的真实GPU完整回归687通过，另补4项进程验收通过；1万/10万规模、12组生命周期成本和CPU/GPU各500条冻结留出验证通过。审查补齐目录同步重试、DROP表回收与上传清理重试。该索引子阶段保留数据库级revision；数据缓存进度见下文，自适应策略、表级版本与增量维护仍待推进，不代表全部P4完成。

P4数据缓存与传输优化子阶段T1—T8已于2026-10-19按代表性实验范围完成：[设计](docs/design/p4-data-cache-and-transfer.md)、[计划](docs/superpowers/plans/2026-10-19-p4-data-cache-and-transfer.md)、[使用说明](docs/p4-cache.md)、[验收](docs/validation/p4-cache.md)。实现epoch/revision隔离的有界原始向量LRU、miss批读、按需元数据及选择性fused、独立启动/会话/SQL开关和用量/清理接口；默认关闭，保留每请求租约与存储过滤。审查修复后完整真实GPU且无PYTHONPATH回归838通过；代表性38场景/630次配置/33744计时查询通过，CPU/GPU源码SHA与回归一致。传输减少不保证延迟下降，CPU部分串行/容量组有退化；原验收保留的每查询统计遍历已于2026-10-23改为常数时间快照，见后续记录。完整200查询矩阵未运行，全部复现入口保留；自适应策略、表级版本、增量维护及过滤结果缓存仍后置，不代表全部P4完成。

2026-10-23查询统计优化完成：[计划](docs/superpowers/plans/2026-10-23-cache-stats-snapshot.md)、[验收](docs/validation/cache-stats-snapshot.md)。新增锁内常数时间process快照，查询不再扫描缓存，显式分库管理统计仍按需遍历；无PYTHONPATH真实GPU全套841通过。针对既有CPU串行/8及64MiB容量退化完成前后72配置/13440查询、6720对结果与计数一致；64MiB暖态开启缓存P50由63.79降至41.31ms，仍保留关闭缓存控制与波动/退化。新增benchmarks/p4/cache_stats.py独立微基准，不将微基准收益外推全部负载。

P4自适应查询策略下一子阶段已形成[详细设计](docs/design/p4-adaptive-query-strategy.md)和[八任务实施计划](docs/superpowers/plans/2026-10-23-p4-adaptive-query-strategy.md)，2026-11-03起实施。T1/T2契约、策略解析、scope快照及有界观测65项定向测试通过，初始真实GPU全套基线841通过；T3候选资格及质量绑定通过31项测试，T4纯成本/风险选择器及相关模块47项通过；T5内部执行接入60项相关回归通过；T6公开入口及能力确认已接入，真实GPU完整922项通过；T7实验工具及T8小规模生命周期已接入；2026-11-04审查修复后完整真实GPU回归943项通过（无PYTHONPATH、零跳过），正式校准与最终性能验收仍待完成。首期显式CPU path=auto，质量资格优先，结合完整谓词基数、候选相关性、RPC/字节及scope缓存证据作成本决策；不自动改变设备或ANN预算，不新增在线统计扫描。规划包含风险分层、冷启动退化、双节点/重启、完整策略质量及分级性能验收；设计阈值并非已测收益。

2026-11-10自适应性能修复状态：旧云端135配置/43470计时查询的质量通过，18个性能单元均失败。风险/成本分离、偏差反馈与并发键、拟合求解和规划配置缓存修复已分阶段提交；冻结实现`1a8cff9`本机无PYTHONPATH完整回归970通过（零失败/错误/跳过）。规划开销仍未达标，新增预测模板缓存因无稳定收益撤回。云端新独立留出任务`adaptive-v2-frozen-20261110`运行中，未完成最终性能验收；旧失败证据保留，详见[验收记录](docs/validation/p4-adaptive.md)。

关键检查点：10 月底必须打通完整查询链路；若未达到，暂停扩展功能。11 月底冻结主要功能，后续优先实验与结题。若学校提前验收，前移相应阶段。

### 团队协作

- 周煜翔：总体架构、SQL 解析与执行调度、模块集成。
- 蒙全精：存储服务、向量索引、缓存、通信与版本管理。
- 文鑫：数据集、正确性测试、基准实验、文档与报告。
- 分工为依据申报书细化的建议，可随团队安排调整。测试与文档从首阶段参与。
- 每周集成演示，每两周向导师汇报可运行成果、实验发现和风险。

## 6. 目录结构

### 当前项目文件

```text
.
├── .gitignore            # 本地环境、缓存、数据及凭据排除规则
├── AGENTS.md             # 实施计划、技术栈、目录与开发约束
├── CHANGELOG.md          # 项目改动日志
├── README.md             # 项目简介，后续补充部署与使用入口
├── pyproject.toml        # 包元数据、vecstor 命令入口和 pytest 配置
├── environment.yml       # Conda 环境与直接依赖版本
├── environments/
│   └── vecstor-linux-64.explicit.txt # Linux x86_64 精确包锁定
├── scripts/
│   ├── check_environment.py # SQL、Faiss CPU/GPU、gRPC 环境检查
│   ├── vecstor.py        # generate/query/demo/remote-query 命令行
│   ├── serve.py          # 存储/计算服务进程入口
│   ├── check_p2.py       # 独立进程、P1 对照及 Landlock 隔离验收
│   ├── check_shell.py    # 真实终端、持久 CRUD、重启和隔离验收
│   ├── check_gpu_ann.py  # 独立子进程GPU组件能力探测
│   ├── check_gpu.py      # 真实GPU/PTY/双进程存储隔离和生命周期验收
│   ├── check_p3.py       # CPU ANN 双路径、真实终端与隔离验收
│   ├── check_p4_adaptive.py # 自适应双节点、PTY及重启验收
│   ├── check_p4_cache.py # 缓存/投影真实终端、双节点/重启与隔离验收
│   ├── check_p4_indexes.py # 双计算节点持久工件、重启与隔离生命周期验收
│   └── generate_proto.py # 生成协议 Python 模块
├── proto/vecstor/rpc/services.proto # P2 v1 协议源文件
├── proto/vecstor/rpc/sql.proto # v2 SQL/租约、P3检索及P4工件发布/缓存投影读取协议
├── src/vecstor/
│   ├── cache/            # 有界版本向量LRU、独立字节所有权及批内工作区（已通过代表性验收）
│   ├── __init__.py
│   ├── __main__.py       # console-script 与 shell/serve/旧命令路由
│   ├── data.py           # 数据与向量校验、只读数组副本
│   ├── datasets.py       # 合成数据生成、NPZ 快照读写
│   ├── evaluation.py     # 独立 NumPy 真值和 12 查询验收负载
│   ├── cli.py            # 命令行参数和产物输出
│   ├── isolation.py      # 计算进程 Landlock 权限与拒绝读取探针
│   ├── storage/          # service.py兼容P2；catalog/database/sql_service管理库/事务/租约，index_catalog/index_artifacts/index_service管理索引目录、工件与发布RPC
│   ├── compute/          # P2 RemoteEngine、v2路由/执行、lease_io/index_manager与index_publication/index_recovery显式发布加载及预热；planner_types提供待接入的自适应契约
│   ├── index/            # CPU工件格式/摘要校验、GPU worker/副本、CPU IVF、快照键、有界registry与构建进程
│   ├── rpc/              # v1/v2 协议、能力缓存会话客户端、timing接入计时、类型化结果、生命周期和验收
│   ├── shell/            # app/buffer/commands/index_commands/cache_commands/render/completion：SQL终端与索引/缓存控制
│   └── query/
│       ├── __init__.py
│       ├── parser.py     # SQL AST 白名单、参数化过滤计划
│       ├── statements.py # 普通 SQL/CRUD/库命令/内联向量/EXPLAIN 校验
│       ├── search_options.py # 请求级精确/近似路径、质量配置和候选预算
│       ├── execution_settings.py # SQLGlot SETTINGS扩展与逐请求选项合并
│       ├── quality_policy.py # 快照/SQL 范围匹配、证据校验和质量预算
│       └── engine.py     # 本地 DuckDB 过滤、块式精确 Top-K
├── benchmarks/p3/
│   ├── run.py            # 1万/10万行、两分布的原始 P3 矩阵
│   ├── costs.py          # 训练参数探索、RPC 并发与绑定/缓存成本消融
│   ├── calibrate.py      # 独立校准/留出、带簇负载、冻结配置与恢复运行
│   └── validate_quality.py # 冻结参数 RPC 对照及实际快照绑定
├── benchmarks/gpu/       # calibrate/run质量校准与RPC；sessions热会话/合并RPC对照；batch_probe原生批探测
├── benchmarks/p4/        # 工件v3质量、索引生命周期、缓存传输配对/资源消融及统计快照微基准
├── configs/quality/       # 质量配置示例，绑定实际快照而非通用默认参数
├── tests/
│   ├── support/          # 测试专用透明延迟代理，不进入生产服务
│   ├── unit/             # 数据、SQL、持久事务、协议、终端输入与 PTY 测试
│   └── integration/      # CLI、RPC、真实终端、并发/超时、重启与文件隔离测试
├── docs/
│   ├── user-guide.md     # 面向用户的安装启动、SQL、导入、ANN、运维与排错说明书
│   ├── environment.md    # 环境使用、复现及验证记录
│   ├── p2-services.md    # P2 启停、查询、隔离验收与限制
│   ├── sql-shell.md      # P2.5 安装入口、界面、SQL 范围与使用方法
│   ├── gpu-search.md     # GPU SQL、设备/资源、质量和故障说明
│   ├── p3-search.md      # ANN 命令、资源/版本语义与实验复现
│   ├── p4-adaptive.md    # CPU auto、策略校准、诊断与限制（验收中）
│   ├── p4-cache.md       # 缓存/投影启动、会话控制、统计及限制（代表性验收完成）
│   ├── p4-indexes.md     # 持久工件发布/加载、双节点、重启预热与回收
│   ├── design/data-and-query-spec.md # P1 数据与查询契约
│   ├── design/p2-grpc.md  # P2 协议、快照与隔离设计
│   ├── design/interactive-sql-shell.md # P2.5 终端与可写数据库设计
│   ├── design/p3-faiss-execution.md # P3 索引、两路径与计划展示设计
│   ├── design/p4-data-cache-and-transfer.md # 有界向量缓存/字段传输设计与验收边界
│   ├── design/p4-adaptive-query-strategy.md # CPU自适应路径、统计/成本/质量约束设计（待实施）
│   ├── design/p4-index-publication.md # P4索引工件/发布/双节点/恢复设计与实施边界（索引子阶段完成）
│   ├── design/gpu-improvement-analysis.md # GPU不足、补充探测与改进优先级
│   ├── design/gpu-ann-execution.md # GPU ANN 与 SQL 设备选择设计
│   ├── design/ann-recall-optimization.md # 低召回原因、校准证据和成本评估
│   ├── validation/p1-local-baseline.md # P1 验收结果与局限
│   ├── validation/p2-grpc.md # P2 对照、隔离与审查修复证据
│   ├── validation/p25-sql-shell.md # P2.5 终端、持久化、P1 对照和隔离证据
│   ├── validation/ann-recall-optimization.md # 正式质量、成本、资源及隔离证据
│   ├── validation/gpu-ann-capabilities.md # GPU原生能力边界
│   ├── validation/gpu-ann.md # GPU质量/隔离/成本/限制
│   ├── validation/gpu-improvements.md # G0—G2边界/热会话/合并RPC/原生批验证
│   ├── validation/p4-cache.md # 缓存一致性/传输/审查修复与代表性实验记录
│   ├── validation/p4-indexes.md # 双节点/重启/故障矩阵、成本及工件质量证据
│   ├── validation/p3-faiss.md # P3 CPU 回归、隔离、召回实验与已知限制
│   ├── superpowers/plans/2026-10-11-local-baseline.md # 执行计划与记录
│   ├── superpowers/plans/2026-10-12-p2-grpc.md # P2 执行记录
│   ├── superpowers/plans/2026-10-12-interactive-sql-shell.md # P2.5 执行计划与完成记录
│   ├── superpowers/plans/2026-10-13-p3-faiss.md # P3 六任务实施计划
│   ├── superpowers/plans/2026-10-15-gpu-improvements.md # GPU G0—G2实施记录与G3—G5待办
│   ├── superpowers/plans/2026-10-23-p4-adaptive-query-strategy.md # 自适应策略八任务计划（待实施）
│   ├── superpowers/plans/2026-10-19-p4-data-cache-and-transfer.md # 缓存与传输八任务实施记录（代表性范围完成）
│   ├── superpowers/plans/2026-10-16-p4-index-publication.md # P4索引子阶段九任务计划与验收矩阵（已完成）
│   ├── superpowers/plans/2026-10-15-gpu-ann.md # GPU 六阶段实施记录
│   ├── superpowers/plans/2026-10-13-ann-recall-optimization.md # 质量校准、配置与优化实施记录
│   └── research/
│       └── github-reference-survey.md # 相关仓库、复用入口与接入建议
├── data/                 # 已生成的本地示例，Git 忽略
├── results/              # 已生成的验收产物，Git 忽略
└── 项目申报书2.pdf        # 原始申报材料
```

隐藏的工具管理目录不列入业务结构，不因本规划而修改。

### 规划目录（尚未创建，按实际开发需要逐步建立）

```text
src/vecstor/
└── common/               # 数据类型、协议定义、配置与统计
```

不要为满足目录树而创建空模块。新增、移动、删除目录或改变职责时，在同一次改动中更新本节；已实现目录移入“当前项目文件”，未实现内容保留为规划。

index/与storage/已支持权威索引持久发布与完整重建，cache/及读取链路已支持有界向量缓存和按需传输；每查询统计已改为常数时间快照，后续推进自适应策略和更广泛并发/消融实验。参考资料入口：[相关 GitHub 仓库与复用建议](docs/research/github-reference-survey.md)。该调研为技术评估，不表示已集成所有上游组件。

## 7. 开发约束

### 范围与工程实践

- 优先最小完整查询链路，再实现优化；复用成熟 SQL 与向量检索组件。
- 模块接口明确输入、输出、错误、版本和资源边界；禁止跨层直接访问内部文件或数据结构。
- 配置与代码分离；不硬编码机器绝对路径、服务地址或凭据。
- 不提交密钥、个人联系方式、未经许可的数据集或大型生成文件；不复制申报书中的个人联系信息到公开文档。
- 保留原始申报书，修订说明记录在文档中，不覆盖源材料。
- 环境检查：`conda run --no-capture-output -n vecstor python scripts/check_environment.py`；依赖检查：`conda run -n vecstor python -m pip check`；全套业务测试：`conda run -n vecstor python -m pytest -q`。P2 测试需要 localhost 监听，沙箱受限时在沙箱外运行，不得将环境检查当作业务测试通过。
- 本地验收：`python scripts/vecstor.py demo --output-dir results/p1-demo`；输出目录必须不存在。生成快照不覆盖现有文件。所有命令从项目根目录、在 vecstor 环境内执行。
- Dataset 拥有输入副本并设置只读标志；可信本地调用者不得主动解除数组写保护。不要把此约束当作恶意输入隔离或并发快照协议。
- P2 服务使用 `scripts/serve.py storage/compute`，远程查询使用 `scripts/vecstor.py remote-query`；完整参数见 [P2 服务说明](docs/p2-services.md)。P2 验收使用 `python scripts/check_p2.py --output-dir results/p2-check`，输出目录不得存在。
- 修改 `proto/vecstor/rpc/services.proto` 后运行 `python scripts/generate_proto.py` 并同步生成文件；不要手改生成的 pb2 模块。
- P2.5 使用 `vecstor` 交互入口，`vecstor serve storage --data-root ...` 管理持久库；旧 `--dataset` 模式保留。新机器恢复 Conda 后，以 `python -m pip install --no-deps --no-build-isolation -e .` 只注册本项目入口。
- 托管导入保持单事务和幂等记录，以最多 1024 行参数化批插入；向量文本只能作为绑定参数转换，禁止拼接数据到 SQL。每批前后检查取消，失败整事务回滚。
- SQL 分句器只识别词法边界，不代替 SQLGlot 校验；不要以 split(';') 处理字符串/注释中的分号。终端补全刷新替换当前库缓存，不在按键时发 RPC。
- 修改任一 proto 后运行生成脚本，v2 不改变旧字段含义；Cell 保持 BIGINT/NULL/布尔/向量类型，客户端必须验证帧顺序及行宽。

### 一致性与故障行为

- 单个存储进程持有权威数据。P2 保持不可变快照；P2.5 通过持久 DuckDB 事务与 MVCC 读租约保持每条向量查询的元数据/向量同版本。后续索引也必须匹配该版本。
- 缓存键必须包含必要的数据/索引版本；设置容量和淘汰策略。
- 新版本完整落盘并验证后才发布；保留旧版本直至使用它的查询结束。
- 网络调用设置超时；读请求重试有界；写请求重试必须有幂等语义。
- 服务不可达、索引损坏、版本不匹配应返回明确错误或执行有记录的恢复流程，不能静默返回不完整结果。
- 将批量更新的可见性延迟写入文档，不将其描述为实时强一致更新。
- P2 的 `--dataset` 仍只在启动时读取 NPZ。P2.5 的 `--data-root` 支持单语句事务写入和持久 revision，读租约最长 60 秒、每库至多 16 个；P3 派生内存索引严格匹配数据库 revision，P4已实现持久版本发布与批量写入后的完整重建，增量维护仍未实现。
- 每条请求携带 database_id，禁止全局 current_database。失败选库保留原客户端选择，计算侧不得直接读 DuckDB 文件。
- 写操作数据、revision、成功结果在同事务提交；失败回滚后持久记录失败摘要。同一 operation_id 不得绑定不同请求。响应丢失须查询状态，UNKNOWN 不能当作回滚或成功；导入重试必须先查幂等记录，不依赖源文件仍存在。
- 每库专用 SQL 队列；超时/取消须中断正确活动连接并在分批读取时检查期限，不能只取消已运行 Future。避免中断串行队列中后续的其他请求。
- P2 每条查询传播总 deadline，应用重试为 0；向量每批至多 1 MiB、1024 行，消息上限 4 MiB。版本/顺序/大小/流完整性不匹配必须失败，不返回部分成功结果。
- 存储端 8 个工作线程、最多 4 条活跃扫描，超额扫描非阻塞返回 RESOURCE_EXHAUSTED，保留向量读取容量，禁止改成扫描流占满共享线程池的实现。
- `rpc_payload_bytes` 仅为 Protobuf 请求/响应序列化字节数，不是实际网络流量。默认仅本机可信网络，无 TLS/鉴权，不宣称生产部署完备。

### P3 索引与执行约束

- 默认 exact/filter-first；ANN 必须显式开启，候选重排不等于全局精确。请求选项不可变并按会话隔离；v1 仍为精确查询。
- 索引按 database_id/table_id/revision/schema_version/dimension/metric/构建指纹匹配。查询持引用；旧版本不能用于新读。只显式构建，无查询时隐藏训练。
- CPU 构建使用新解释器和匿名管道；管道只由单次 communicate 的专用 I/O 线程操作，主线程检查取消/期限并终止、回收子进程，避免大输入分段重试阻塞。完整校验后、持锁再次检查取消才发布。
- 默认索引驻留估算 256 MiB/8 条目、1 个构建任务；预估构建工作集加驻留量不超过预算两倍。先回收无引用旧版本，仍不满足则明确拒绝；此预算不等于 native RSS 硬限。
- StreamVectorSnapshot 与 FilterCandidates 共用 4 个存储扫描名额，向量读取保留容量。先检索无回退时不得调用全量 ScanMetadata；FilterCandidates 只能处理指定 ID，未知 ID 不能作为普通过滤失败丢弃。
- 质量配置为计算侧显式只读文件，至多 1 MiB/128 条；在 Landlock 前校验但禁止从 storage data-root 读取。必须匹配实际快照、AST metadata SQL、K/路径和查询范围；显式预算不足拒绝，旧服务忽略质量请求不能当作成功。
- EXPLAIN 只获取租约/索引元信息；ANALYZE 只执行一次。计划统计必须反映实际模式、回退、候选轮次及 Protobuf 字节；未知物理扫描行数为 null，不在线虚构 Recall。
- CPU 验收 `python scripts/check_p3.py --output-dir results/p3-check`；规模实验 `python benchmarks/p3/run.py --output-dir results/p3-matrix`。输出目录须不存在；完整命令及当前限制见 [P3 说明](docs/p3-search.md)。

### 测试与性能实验

- 验证混合查询过滤正确性、精确结果、候选不足、空结果、非法输入、重启加载、更新及缓存失效。
- 近似查询的标准答案应在满足过滤条件的数据内精确计算；合法结果少于 K 时按实际真值数量评估，空真值单独统计。
- 建议数据规模：1 万条用于调试、10 万条用于正式实验，资源允许再扩至 100 万条。
- 条件通过率覆盖 0.1%、1%、10%、50%、100%；Top-K 覆盖 10、100；并发覆盖 1、4、8，按资源调整。
- 同时包含属性与向量分布独立、相关两类负载；区分冷缓存和热缓存。
- 对比精确基线、固定先过滤、固定先检索、自适应策略；分别关闭缓存、批量传输和自适应策略做消融。
- 报告 P50/P95 延迟、吞吐、Recall@K、结果不足比例、峰值内存、网络字节、RPC 次数、缓存命中率及索引维护耗时。
- 每组实验固定配置和随机种子，至少重复三次；记录硬件、依赖版本、代码版本及数据来源。参数校准与最终评估使用不同查询集合。
- 性能比较保持资源与召回率可比。Recall@10 ≥ 0.95 是待基线校准的建议目标，不是已达到的结果或申报硬指标。
- 不承诺预设性能提升倍数，不隐藏退化场景；原始结果与分析结论一并保留。

### 环境依赖相关问题

- 本地已有可用的显卡，可通过 `nvidia-smi` 查看，若检测不到则应该是沙箱权限等限制，此时允许在沙箱外检测和使用显卡。
- 2026-10-18迁移后已验证本机 RTX 4060 Laptop GPU（8188 MiB）、驱动615.78.08可运行锁定环境（旧系统驱动580.178.04）。GPU 和 localhost gRPC 检查可能需要沙箱外执行；`--cpu-only` 明确跳过 GPU，不能据此声称 GPU 验证成功。
- 新系统CachyOS/Miniforge的78个Conda包与锁定文件一致，环境/真实GPU检查和Conda健康检查通过；初检项目入口未注册，原样业务回归551通过/81失败。临时指定源码PYTHONPATH后632项完整真实GPU回归通过，无跳过；同日已按授权补注册本项目，清除PYTHONPATH后632项完整真实GPU回归通过（180.97秒，无跳过）。检查及修复记录见[环境说明](docs/environment.md)，未安装额外依赖，也不代表P4验收完成。
- 环境操作与复现说明见 [docs/environment.md](docs/environment.md)。已有 `vecstor` 环境时先检查再更新，不重复创建，不修改 `base` 或系统驱动。
- P2 的 `--isolate-storage-dir` 在导入计算库、启动线程前应用 Landlock，失败必须停止，不降级；默认普通启动模式不改变文件权限。正式隔离证据必须同时包含目录/文件读取拒绝和远程查询成功。
- P2.5 隔离整个 data-root，探针可用 imports 内 NPZ 或 databases 内 DuckDB 文件；空目录需先准备托管源或数据库。正式验收使用 `python scripts/check_shell.py --output-dir results/shell-check --rows 10000`，目标目录不能已存在。

### 参考项目与复用

- 在实现某项功能前，可以先从参考项目里寻找可供学习参照或者二次复用的部分，避免重复造轮子。

### git注意事项

- 执行 git 操作前需要先征得用户的同意。
- 开发新功能时，需要创建新的git分支，在分支上进行开发，确认功能正确后才能合并到 main 。


## 8. 文档维护与变更记录

- 项目发生实质改动时，在同一次工作中更新 `CHANGELOG.md`，记录实际完成的内容、影响、验证方式与未解决限制。
- 架构、技术栈、目录、阶段计划和开发约束发生变化时同步更新本文件及“最后更新”日期。
- 对外使用、安装和部署方式变化时更新 `README.md`；详细接口与实验方法放入 `docs/`。
- 计划事项保留在本文件，不在日志中写成已完成事项；只有执行过的测试才能记录为通过。
- 不修改历史日志以掩盖问题，后续更正应新增条目。发布时将未发布内容归档到实际版本与日期，不虚构版本号。

## 9. 待核实事项与最终成果

- 核实实际结题日期、校级材料要求、现有设备和团队已完成但未纳入此目录的工作。
- 申报书首页及预算合计为 3000 元，预算大写处为“一千元”；采购前核对正式批复金额与可支出类别。
- 关于现有数据库不足和技术空白的判断需通过最新文献核实，不直接将申报书表述当作已证明结论。
- 最终交付：源码与固定依赖、部署说明、查询示例、基准脚本与数据说明、原始结果和图表、结题报告、答辩 PPT 与演示备份。

候选过滤默认使用绑定 BIGINT[] 文本参数，每条活跃租约至多缓存一份谓词计划（原 SQL 与生成 SQL 各最多 64 KiB），结束租约释放。存储选项 `--candidate-binding legacy` 和 `--no-candidate-plan-cache` 用于独立消融；不是 P4 数据缓存。

### GPU 首期约束

- 默认CPU exact；SETTINGS仅向量SELECT/EXPLAIN可用，SQL覆盖会话不改变会话。GPU仅approx/search-first、gpu_id=0，单次L/nprobe≤2048。显式GPU默认device_fallback=error，不能静默转CPU或截断预算。
- CPU/GPU副本分别管理，GPU句柄绑定完整IndexKey及worker generation；显式CPU训练/克隆，无查询时隐藏构建。默认GPU逻辑512MiB/8条目、scratch128MiB、设备余量256MiB、1活动/8等待，均非显存硬限。
- 单owner新解释器worker拥有GPU资源。活动取消/超时回收worker，失效其全部副本和等待请求；CPU服务继续可用，必须显式重建。损坏数据/取消/deadline不被回退吞掉。
- v1质量配置只用于CPU；GPU v2还绑定float32、Faiss版本、机型及独立证据。校准/评估失败组不发布，不用CPU回退充当GPU质量。
- GPU隔离允许CUDA所需procfs读及既有伪文件写/截断、CPU拓扑和指定字符设备；存储目录/文件仍须在计算进程和GPU worker内实际验证读拒绝。不是恶意同用户进程通用沙箱。
- 真实GPU验收 `python scripts/check_gpu.py --output-dir results/gpu-check`；GPU强制测试 `VECSTOR_REQUIRE_GPU=1 python -m pytest -q`。无GPU环境专属测试明确skip，不能当作GPU成功。实验见 `benchmarks/gpu/`。


### G0—G2新增约束

- 完整候选覆盖须有同快照唯一ID验证；nprobe=nlist本身不是完整性证明。损坏GPU ID/帧/统计值在转换前拒绝，不得借设备回退掩盖。
- 只读查询能力按会话/服务实例缓存；完成帧仍确认设备/质量/显式数据路径。构建/清理实时确认；普通SQL/CRUD不要求向量执行计划确认。GPU复用遇队列满等准入拒绝不得重置健康worker。
- FilterCandidateVectors保持同租约/版本、输入ID存在性及顺序验证，共用4扫描名额，最多1024行/1MiB向量/4MiB消息。仅特定响应超大错误可拆分或单行退回两步读取，并记录mixed路径；其他错误直接传播。
- 合并读取默认关闭，计算端显式--fused-candidates且存储能力确认后启用；SQL candidate_data_path=default|separate|fused，fused仅approx/search-first。优化不得改变候选预算、质量配置或设备策略。
- 性能对照固定参数/查询、复用每工作线程会话，控制面RPC和存储RPC分开。未测CUDA事件时kernel/H2D/D2H为null；原生批摊销不能代替端到端结果，退化必须报告。

2026-11-05云端部署补充：容器GPU物理节点可能为/dev/nvidia6等非零编号，隔离仅枚举可见的编号NVIDIA字符设备并拒绝普通文件/软链接，CUDA逻辑设备仍为0。托管导入元数据采用JSON字符串参数并显式转为类型化数组，避免DuckDB对Python列表逐标量反复探测缺失pandas；保留1024行批次、事务/幂等/取消和15秒默认期限。云端完整真实GPU、无PYTHONPATH回归948通过（342.91秒，零失败/错误/跳过），GPU/PTY/文件隔离通过；成本模型需按新源码与云端环境重新校准，性能验收仍在进行。
