# VecStor 项目开发指南

本文件适用于整个项目，供团队成员与开发代理共同遵循。记录项目实施计划、目录结构、技术栈和开发约束；后续开发应同步维护，避免文档与实际实现脱节。

最后更新：2026-10-14。

## 1. 项目背景与当前状态

- 项目名称：面向混合查询的存算分离向量数据引擎研究。
- 项目类型：大学生创新训练项目；团队名称：VecStor。
- 依据：`README.md` 与 `项目申报书2.pdf`。
- 申报实施周期：2026-01-01 至 2026-12-31。
- 预期成果：一套可运行的混合查询存算分离向量数据引擎原型，以及包含架构设计、优化方案和性能测试的结题报告。
- 已建立 Conda `vecstor` 环境；P1 本地基线、P2 gRPC 服务和 P2.5 交互式 SQL、运行时选库、持久 CRUD 已实现。P2.5 通过真实终端、P1 对照、重启及 Landlock 验收；P3 CPU ANN、双路径、候选扩展和计划展示已实现，通过 427 项回归、1 万行真实终端/Landlock 及 360 次规模实验。GPU 业务路径、索引在线维护和缓存优化仍待实现。
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
| Faiss CPU/GPU | 向量检索、索引保存与加载 | `faiss-gpu` 1.12.0 同时支持 CPU/GPU；组件级 Flat、IVFFlat 与持久化验证通过；P3 业务接入 CPU IVFFlat 内存索引，GPU/业务索引持久化未实现 |
| CUDA / NumPy | GPU 运行库与精确计算 | CUDA 12.4 运行库、NumPy 1.26.4；P1 使用 CPU NumPy float64 穷举距离，GPU 业务路径待实现 |
| gRPC / Protobuf | 查询、目录、类型化 SQL 结果与读租约 | grpcio/grpcio-tools 1.83.1、protobuf 7.35.1；兼容 P2 v1 并新增 P2.5 v2；索引分发待实现 |
| prompt_toolkit / Rich | 多行终端、历史/补全与表格 | 3.0.53 / 15.0.0，已通过 Conda 安装并锁定 |
| Arrow Flight | 批量列式数据传输 | 可选，只有测出传输瓶颈后再评估 |
| pytest | 正确性、接口与集成测试 | 9.1.1；473 项 P1/P2/P2.5/P3/质量优化测试通过，包含真实 PTY、gRPC、版本索引、并发租约、重启和隔离；命令 `python -m pytest -q` |
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

P3 CPU 实现见 [使用说明](docs/p3-search.md)、[Faiss 执行设计](docs/design/p3-faiss-execution.md) 和 [六任务实施记录](docs/superpowers/plans/2026-10-13-p3-faiss.md)。保留默认精确模式，显式开启 CPU IVF 的 filter-first/search-first；按快照版本复用索引，候选不足可精确回退，并提供 EXPLAIN/ANALYZE 与实际执行统计。数据库级 revision 保持不变，其他表写入也保守失效。GPU、权威索引持久化/分发、批量维护和自适应策略仍未实现。360 次实验包含 120 次默认精确、168 次真正 ANN、72 次精确回退；默认参数未达到普遍 Recall@10≥0.95，见 [实测召回与资源记录](docs/validation/p3-faiss.md)。

ANN 质量优化四阶段已完成：显式快照绑定配置、质量预算下限、查询内选择器复用及候选批量绑定/租约计划缓存。60 组各 500 个独立留出查询，最低平均 Recall@10=0.9702，零精确回退；最终 720 次隔离 RPC 对照及 96 组成本消融通过。默认 exact 与未选择配置时的 ANN 参数保持不变；不是任意数据或逐查询召回保证。见 [最终验收与成本](docs/validation/ann-recall-optimization.md) 和 [实施计划](docs/superpowers/plans/2026-10-13-ann-recall-optimization.md)。

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
│   ├── check_p3.py       # CPU ANN 双路径、真实终端与隔离验收
│   └── generate_proto.py # 生成协议 Python 模块
├── proto/vecstor/rpc/services.proto # P2 v1 协议源文件
├── proto/vecstor/rpc/sql.proto # v2 目录/SQL/读租约与 P3 快照导出/索引/计划协议
├── src/vecstor/
│   ├── __init__.py
│   ├── __main__.py       # console-script 与 shell/serve/旧命令路由
│   ├── data.py           # 数据与向量校验、只读数组副本
│   ├── datasets.py       # 合成数据生成、NPZ 快照读写
│   ├── evaluation.py     # 独立 NumPy 真值和 12 查询验收负载
│   ├── cli.py            # 命令行参数和产物输出
│   ├── isolation.py      # 计算进程 Landlock 权限与拒绝读取探针
│   ├── storage/          # service.py 兼容 P2；catalog/database/sql_service 管理持久库、事务、租约
│   ├── compute/          # P2 RemoteEngine、v2 路由、lease_io/index_manager/vector_execution
│   ├── index/            # CPU IVF 后端、快照键、带引用的有界 registry、可取消构建进程
│   ├── rpc/              # v1/v2 协议、会话客户端、类型化结果、生命周期和验收
│   ├── shell/            # app/buffer/commands/render/completion：交互界面及分句
│   └── query/
│       ├── __init__.py
│       ├── parser.py     # SQL AST 白名单、参数化过滤计划
│       ├── statements.py # 普通 SQL/CRUD/库命令/内联向量/EXPLAIN 校验
│       ├── search_options.py # 请求级精确/近似路径、质量配置和候选预算
│       ├── quality_policy.py # 快照/SQL 范围匹配、证据校验和质量预算
│       └── engine.py     # 本地 DuckDB 过滤、块式精确 Top-K
├── benchmarks/p3/
│   ├── run.py            # 1万/10万行、两分布的原始 P3 矩阵
│   ├── costs.py          # 训练参数探索、RPC 并发与绑定/缓存成本消融
│   ├── calibrate.py      # 独立校准/留出、带簇负载、冻结配置与恢复运行
│   └── validate_quality.py # 冻结参数 RPC 对照及实际快照绑定
├── configs/quality/       # 质量配置示例，绑定实际快照而非通用默认参数
├── tests/
│   ├── unit/             # 数据、SQL、持久事务、协议、终端输入与 PTY 测试
│   └── integration/      # CLI、RPC、真实终端、并发/超时、重启与文件隔离测试
├── docs/
│   ├── environment.md    # 环境使用、复现及验证记录
│   ├── p2-services.md    # P2 启停、查询、隔离验收与限制
│   ├── sql-shell.md      # P2.5 安装入口、界面、SQL 范围与使用方法
│   ├── p3-search.md      # ANN 命令、资源/版本语义与实验复现
│   ├── design/data-and-query-spec.md # P1 数据与查询契约
│   ├── design/p2-grpc.md  # P2 协议、快照与隔离设计
│   ├── design/interactive-sql-shell.md # P2.5 终端与可写数据库设计
│   ├── design/p3-faiss-execution.md # P3 索引、两路径与计划展示设计
│   ├── design/ann-recall-optimization.md # 低召回原因、校准证据和成本评估
│   ├── validation/p1-local-baseline.md # P1 验收结果与局限
│   ├── validation/p2-grpc.md # P2 对照、隔离与审查修复证据
│   ├── validation/p25-sql-shell.md # P2.5 终端、持久化、P1 对照和隔离证据
│   ├── validation/ann-recall-optimization.md # 正式质量、成本、资源及隔离证据
│   ├── validation/p3-faiss.md # P3 CPU 回归、隔离、召回实验与已知限制
│   ├── superpowers/plans/2026-10-11-local-baseline.md # 执行计划与记录
│   ├── superpowers/plans/2026-10-12-p2-grpc.md # P2 执行记录
│   ├── superpowers/plans/2026-10-12-interactive-sql-shell.md # P2.5 执行计划与完成记录
│   ├── superpowers/plans/2026-10-13-p3-faiss.md # P3 六任务实施计划
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
├── cache/                # 有界缓存与失效管理
└── common/               # 数据类型、协议定义、配置与统计
```

不要为满足目录树而创建空模块。新增、移动、删除目录或改变职责时，在同一次改动中更新本节；已实现目录移入“当前项目文件”，未实现内容保留为规划。

后续扩展 index/ 的持久化与批量维护，在 storage 中增加权威索引发布/分发，扩展 benchmarks/ 的并发与消融实验。参考资料入口：[相关 GitHub 仓库与复用建议](docs/research/github-reference-survey.md)。该调研为技术评估，不表示已集成所有上游组件。

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
- P2 的 `--dataset` 仍只在启动时读取 NPZ。P2.5 的 `--data-root` 支持单语句事务写入和持久 revision，读租约最长 60 秒、每库至多 16 个；P3 派生内存索引严格匹配数据库 revision，持久版本发布与批量维护待 P4。
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
- 已验证本机 RTX 4060 Laptop GPU（8188 MiB）、驱动 580.178.04 可运行当前环境。GPU 和 localhost gRPC 检查可能需要沙箱外执行；`--cpu-only` 明确跳过 GPU，不能据此声称 GPU 验证成功。
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
