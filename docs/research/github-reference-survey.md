# VecStor 相关 GitHub 仓库与复用建议

调研日期：2026-10-10。面向当前大创项目的存算分离、SQL 条件与向量 Top-K 联合查询、GPU 计算和 RPC 通信，筛选 10 个核心参考仓库及 2 个配套组件。

建议继续以 DuckDB、Faiss 和 gRPC 组合建设轻量原型，直接复用成熟库；重点借鉴 pgvector、DuckDB VSS 和 VBASE 的过滤与执行机制，参考 Milvus 和 Lance 的存储边界与版本管理。VecFlow 是 GPU 过滤查询的重要研究对照，但不宜立即替换 Faiss 主线。

本文核对了仓库说明、部分源码、目录树和许可证信息；尚未安装或运行这些系统。接入成本是针对本项目的工程判断，性能与本机兼容性仍需实测。链接主要指向可变主分支，正式引入时应固定发布版本或 commit，并记录对应许可证与依赖。

## 选择依据

这里的“混合查询”主要指 SQL 属性条件与向量近邻联合查询。部分仓库的 hybrid search 指稀疏与稠密检索融合，不应仅凭同名术语认为与本项目相同。

| 仓库 | 对本项目的主要价值 | 建议使用方式 | 接入成本 | 仓库级许可证 |
| --- | --- | --- | --- | --- |
| [Milvus](https://github.com/milvus-io/milvus) | 存算分离、节点与数据段管理 | 架构参考，后期外部对照 | 高 | Apache-2.0 |
| [pgvector](https://github.com/pgvector/pgvector) | SQL 过滤、迭代扫描 | 策略参考，SQL 对照系统 | 中；源码移植高 | PostgreSQL 风格许可，见原文 |
| [DuckDB VSS](https://github.com/duckdb/duckdb-vss) | DuckDB 向量计划改写与预过滤 | 源码参考，可选本地基线 | 中；扩展开发高 | MIT |
| [MSVBASE](https://github.com/microsoft/MSVBASE) | 关系算子与近似检索融合 | 研究方法和实验参考 | 高 | MIT |
| [Lance](https://github.com/lance-format/lance) | 向量随机访问、清单和版本 | 存储设计参考，可选存储后端 | 中 | Apache-2.0 |
| [DiskANN](https://github.com/microsoft/DiskANN) | 检索与数据提供接口解耦 | 索引接口参考，后期扩展 | 高 | MIT |
| [Faiss](https://github.com/facebookresearch/faiss) | CPU/GPU 检索与索引读写 | 直接依赖 | CPU 低，GPU 中 | MIT |
| [cuVS](https://github.com/NVIDIA/cuvs) | GPU 向量检索算法 | 可选 GPU 后端或对照 | 中至高 | Apache-2.0 |
| [VecFlow](https://github.com/Supercomputing-System-AI-Lab/VecFlow) | GPU 上的标签过滤检索 | 策略参考，兼容性验证后再对照 | 高 | Apache-2.0 |
| [VectorDBBench](https://github.com/zilliztech/VectorDBBench) | 数据集、过滤负载、系统适配 | 复用评测方法与适配框架 | 中 | MIT |

许可证列表示仓库级声明，不覆盖所有子模块、第三方文件或数据集。pgvector 的 GitHub API 自动识别为 NOASSERTION，但仓库存在明确的 [LICENSE](https://github.com/pgvector/pgvector/blob/master/LICENSE)，不能误写为无许可证。

## 存算分离与存储参考

### Milvus

Milvus 的 README 明确描述存算分离、查询节点扩展、数据段副本及 CPU/GPU 向量检索。它与本项目的整体方向接近，但 Go/C++ 分布式系统的集成负担明显高于轻量原型。[仓库说明](https://github.com/milvus-io/milvus#why-milvus)

- **阅读入口**：[`internal/querynodev2`](https://github.com/milvus-io/milvus/tree/master/internal/querynodev2)、[`internal`](https://github.com/milvus-io/milvus/tree/master/internal)、[`configs`](https://github.com/milvus-io/milvus/tree/master/configs)。已核对查询节点目录和集群工作节点文件存在。
- **可借鉴**：查询节点与持久化数据分离；按数据段加载与缓存；可重建计算副本。VecStor 可先简化为一个存储服务、一个计算服务及带版本的索引快照。
- **复用建议**：后期部署成外部对照系统；不要为复用少量调度逻辑而将整套服务框架搬入项目。
- **边界**：Milvus Lite 的进程内体验不能代替分布式部署实验；标量过滤接口也不能直接等同于通用 SQL 执行器。

### Lance

Lance 提供面向多模态数据的格式、随机访问、向量索引和版本能力，并给出 DuckDB/PyArrow 集成示例。旧的 `lancedb/lance` 地址现在应关注 `lance-format/lance`。[仓库说明](https://github.com/lance-format/lance)

- **阅读入口**：[`rust/lance-table/src/format/manifest.rs`](https://github.com/lance-format/lance/blob/main/rust/lance-table/src/format/manifest.rs)、[`rust/lance-table/src/io/manifest.rs`](https://github.com/lance-format/lance/blob/main/rust/lance-table/src/io/manifest.rs)、[`python/src/scanner.rs`](https://github.com/lance-format/lance/blob/main/python/src/scanner.rs)。这些路径已通过目录树核对，未逐行审计。
- **可借鉴**：清单描述数据版本；数据扫描与底层文件分离；按候选行批量读取向量。
- **复用建议**：若自行管理向量文件和版本显著拖慢 P2，可评估把 Lance 封装在存储服务内部。初期只借鉴清单与扫描接口即可。
- **边界**：存储格式本身不是独立 RPC 服务。若计算侧直接打开共享文件，仍不满足本项目的存算隔离要求。启用其内置检索还会改变 Faiss 后端的实验边界。

### DiskANN

当前主分支介绍的是 Rust 方向的 DiskANN3，通过 `DataProvider` 接口连接不同存储实现，支持更新和过滤相关接口。旧 C++ 实现保留在 `cpp_main`，README 明确称其不再积极开发维护。[当前仓库与分支说明](https://github.com/microsoft/DiskANN)

- **可借鉴**：将向量和邻接表访问抽象为数据提供接口，让索引算法不绑定某一种存储。
- **复用建议**：作为未来磁盘索引后端候选；当前仅用于设计 `VectorStore` 与 `VectorIndex` 边界。
- **边界**：磁盘检索不自动等于远程存算分离；把图搜索中每个随机访问变成一次 RPC 可能引入严重延迟。Rust 接口或旧 C++ Python 绑定与当前代码的兼容性不能混用判断。
- **许可证**：主分支 [LICENSE.txt](https://github.com/microsoft/DiskANN/blob/main/LICENSE.txt) 为 MIT；旧分支和第三方部分应分别核对。

## SQL 与过滤执行参考

### pgvector

pgvector 将向量检索集成到 PostgreSQL，支持精确检索、HNSW、IVFFlat。其文档直接讨论后过滤造成结果不足的问题，并提供迭代扫描，在结果不足时继续扫描，直至满足条件或达到资源上限。[过滤与迭代扫描说明](https://github.com/pgvector/pgvector#filtering)

- **源码入口**：[`src/hnswscan.c`](https://github.com/pgvector/pgvector/blob/master/src/hnswscan.c)、[`src/ivfscan.c`](https://github.com/pgvector/pgvector/blob/master/src/ivfscan.c)。已打开 HNSW 扫描源码并核对 IVF 路径。
- **可借鉴**：把检索设计为可继续扩展的过程，记录候选预算、扫描轮次和停止原因，而不是一次固定 Top-K 调用。
- **复用建议**：在自己的查询调度器实现相同问题的处理逻辑；作为 SQL 混合查询的外部基线。C 扩展依赖 PostgreSQL 内部 API，不能直接粘贴到 Python 服务。
- **边界**：严格距离排序不代表全局精确召回；必须继续区分近似检索与精确基线。

### DuckDB VSS

这是基于 USearch 的实验性 DuckDB HNSW 扩展。当前主分支支持把部分 WHERE 条件下推为过滤位图，再执行近似检索；README 说明构建位图有逐查询 O(n) 扫描成本，并非所有谓词均可下推。[主分支说明](https://github.com/duckdb/duckdb-vss)

- **源码入口**：[`src/hnsw/hnsw_index_scan.cpp`](https://github.com/duckdb/duckdb-vss/blob/main/src/hnsw/hnsw_index_scan.cpp)、[`src/hnsw/hnsw_optimize_scan.cpp`](https://github.com/duckdb/duckdb-vss/blob/main/src/hnsw/hnsw_optimize_scan.cpp)、[`test/sql/hnsw/hnsw_prefilter.test`](https://github.com/duckdb/duckdb-vss/blob/main/test/sql/hnsw/hnsw_prefilter.test)。
- **已核对的机制**：扫描文件中 `BuildFilterBitmap` 构建合法行集合；初始化时将该集合传入索引扫描，之后按行 ID 获取投影字段。
- **可借鉴**：WHERE → 合法 ID/位图 → 向量算子 → 按需回表的执行路径，以及 EXPLAIN 可观察性。
- **复用建议**：优先阅读源码与测试；可作为同机 DuckDB 基线，但不替代 Faiss GPU 后端。
- **边界**：主分支能力不等于已发布扩展包能力；它仍是进程内扩展，不能替代远程存储协议。C++ 扩展改造应安排为后续工作，避免首阶段进入数据库内核开发。

### MSVBASE

VBASE 的官方代码仓库名为 `microsoft/MSVBASE`，将高维索引接入 PostgreSQL，研究近似检索与关系算子的融合。它提供 SQL 样例、补丁及评测材料，适合本项目调研查询执行机制。[官方仓库](https://github.com/microsoft/MSVBASE)

- **阅读入口**：[`src`](https://github.com/microsoft/MSVBASE/tree/main/src)、[`patch`](https://github.com/microsoft/MSVBASE/tree/main/patch)、[`evaluation`](https://github.com/microsoft/MSVBASE/tree/main/evaluation)。目录树包含数据准备、不同选择率查询和 ground truth 文件。
- **可借鉴**：检索迭代器与关系执行器的交互，以及不同条件通过率下的实验设计。
- **复用建议**：借鉴查询类型、评测流程和执行思想；重新生成适合 VecStor 数据与距离定义的真值，不直接拿原输出当自己的标准答案。
- **边界**：需要 PostgreSQL 补丁和子模块；研究代码的构建、依赖年代与运行条件必须单独验证。不能把“宽松单调性”简化成不带条件的精确 Top-K 保证。
- **相关材料**：[VBASE-artifacts](https://github.com/Catoverflow/VBASE-artifacts) 已归档，可用于论文复现参考；它与官方主仓库不能混为一个维护状态，其许可需独立核对。

## GPU 计算参考

### Faiss

Faiss 是当前方案最适合直接复用的检索库，提供 CPU/GPU 索引、训练、查询和持久化能力。[官方仓库](https://github.com/facebookresearch/faiss)

- **入口**：[`tutorial/python/4-GPU.py`](https://github.com/facebookresearch/faiss/blob/main/tutorial/python/4-GPU.py)、[`tutorial/python/2-IVFFlat.py`](https://github.com/facebookresearch/faiss/blob/main/tutorial/python/2-IVFFlat.py)、[`faiss/index_io.h`](https://github.com/facebookresearch/faiss/blob/main/faiss/index_io.h)、[`faiss/impl/IDSelector.h`](https://github.com/facebookresearch/faiss/blob/main/faiss/impl/IDSelector.h)。
- **复用建议**：封装 CPU/GPU 可切换后端，先做 Flat 精确基线，再做 IVF 近似路径；复用索引读写接口，不重新实现距离计算和索引文件格式。
- **边界**：`IDSelector` 的存在不表示所有 GPU 索引都支持相同过滤参数；逐组合验证。GPU 索引序列化前应转回 CPU；设备拷贝和临时显存都计入实验成本。[GPU 文档](https://github.com/facebookresearch/faiss/wiki/Faiss-on-the-GPU)

### cuVS

cuVS 提供 GPU 向量搜索与聚类算法及多语言接口；旧地址 `rapidsai/cuvs` 当前重定向到 `NVIDIA/cuvs`。[仓库与示例入口](https://github.com/NVIDIA/cuvs)

- **可复用**：通过其 API 接入 GPU 检索，比较不同 GPU 算法；从仓库 README 的 examples 入口开始，不先移植 CUDA 内核。
- **建议顺序**：Faiss GPU 跑通后再评估，独立环境锁定 CUDA 与库版本。
- **边界**：它不提供 VecStor 的 SQL 调度、远程数据版本协议或存储服务；双 GPU 后端会增加环境与测试矩阵。本机只有约 8 GB 显存，需先评估数据、索引与工作空间的总占用。

### VecFlow

VecFlow 面向 GPU 上带标签过滤的近似检索，根据标签对应数据量选择不同索引路径，并提供多标签 AND 查询。它比通用 GPU 检索库更接近本项目“过滤条件影响执行策略”的研究问题。[官方仓库](https://github.com/Supercomputing-System-AI-Lab/VecFlow)

- **入口**：[`vecflow/README.md`](https://github.com/Supercomputing-System-AI-Lab/VecFlow/blob/main/vecflow/README.md) 列出 Python/C++ 用法、绑定文件和算法所在的 `cpp/src/neighbors/vecflow/`、`filtered_bfs/` 路径。
- **可借鉴**：用条件对应的数据量驱动路径选择；分开测量常见与稀有标签；在相同召回率下评估吞吐与延迟。
- **本机限制**：文档列出的预编译目标为 `sm_80`、`sm_90`、`sm_90a`，没有承诺覆盖本机 RTX 4060。需要检查包是否有可用兼容代码，必要时针对本机重新编译和验证。
- **依赖限制**：文档指出其修改后的 `libcuvs.so` 与上游 cuVS 同名，不应安装到同一环境。
- **复用建议**：先借鉴策略，再做独立小规模可行性验证。标签过滤不覆盖任意 SQL 范围与 OR 表达式；面向其他硬件的论文加速比不能套用到本机，也不引入面向 Grace-Hopper 的 Chamfer 分支作为初版要求。

## 评测与 RPC 配套组件

### VectorDBBench

该框架提供插入、向量搜索和带过滤查询的评测流程，并给出添加数据库客户端的说明。仓库由 Zilliz 赞助，适合复用工作负载与适配框架；项目结论仍应来自统一配置的本地复现实验。[仓库与扩展指南](https://github.com/zilliztech/VectorDBBench)

- **入口**：[`vectordb_bench/backend`](https://github.com/zilliztech/VectorDBBench/tree/main/vectordb_bench/backend)，尤其是 README 指出的客户端实现与注册流程。
- **复用建议**：先用自有小型基准验证 VecStor，再为其编写适配器，对接 pgvector 或 Milvus 的同语义查询。
- **需要补充**：存算网络字节、RPC 次数、CPU/GPU 拷贝、缓存和版本更新指标。通用框架不能自动给出这些内部开销。
- **边界**：避免一次下载全部大数据集；过滤真值、距离度量和资源预算必须统一，代码许可不替代数据集许可。

### gRPC 与 SQLGlot

| 组件 | 入口与许可证 | 可复用部分 | VecStor 仍需实现 |
| --- | --- | --- | --- |
| [grpc/grpc](https://github.com/grpc/grpc) | [Python Route Guide 示例](https://github.com/grpc/grpc/tree/master/examples/python/route_guide)，Apache-2.0 | 服务定义、客户端/服务端框架、流式调用示例 | 数据版本、批量格式、超时重试、块大小、统计；RPC 不自动实现 GPU 显存零拷贝 |
| [tobymao/sqlglot](https://github.com/tobymao/sqlglot) | 仓库解析与 AST 示例，MIT | SQL 语法树、方言解析与表达式遍历 | 语法白名单、类型与参数校验、向量算子识别、代价规则；解析器不是执行引擎 |

建议 RPC 接口围绕清单获取、元数据扫描、向量批量读取、索引分块下载和查询执行定义，不照搬大系统全部 proto。

## 复用实施顺序

| 当前阶段 | 建议行动 | 可检查的产出 |
| --- | --- | --- |
| P1 | 直接复用 Faiss 与 SQLGlot；以 pgvector/VSS 查询为语义参考 | 统一距离定义、过滤真值和 CPU/GPU 小样例 |
| P2 | 参考 gRPC 示例实现协议；借鉴 Lance 清单与 Milvus 节点边界 | 存储目录隔离后的端到端查询；每个请求绑定数据版本 |
| P3 | 借鉴 pgvector 候选扩展和 VSS 位图；参考 VBASE 选择率负载 | 先过滤/先检索路径、预算和停止原因、结果不足测试 |
| P4 | 独立控制缓存、批量传输、设备选择；评估是否值得接入 VecFlow/cuVS | 分项计时和相同召回率的路径对比 |
| P5 | 参考 VectorDBBench 适配方法；选 pgvector 为首个外部对照 | 相同数据、条件、距离和资源下的可重复结果 |

Milvus 是可选第二个外部对照，Lance 是可选存储后端，DiskANN 是后续磁盘索引方向。不要同时引入所有候选仓库。上述建议不表示项目已切换技术栈或增加交付承诺。

## 对项目研究定位的影响

存算分离、SQL 与向量融合、过滤候选扩展及 GPU 标签检索均已有相关实现，不能单独声称为本项目首创。更适合大创的研究问题是：

> 在受限内存和显存、不同网络条件下，如何根据过滤比例、缓存状态与批量大小，选择混合查询执行路径并控制数据传输，使端到端延迟和召回率达到更好的平衡？

该问题是建议的研究切入点，不是已证明的新颖性结论。实验应先比较固定路径，再逐个加入缓存、批量传输和设备选择，区分软件优化收益与更换硬件带来的收益。

## 维护状态与复用记录

本次 GitHub API 元数据核对中，Milvus、pgvector、DuckDB VSS、Lance、Faiss、cuVS、VectorDBBench、gRPC 和 SQLGlot 均未归档，返回的仓库推送日期在 2026-10-06 至 2026-10-10 之间。推送日期仅表明仓库活动，不等于稳定性、发布质量或所需功能已发布。MSVBASE 和 VecFlow 按研究实现评估，不据此假定持续维护承诺；DiskANN 的主分支与旧 C++ 分支须分开判断。

实际引入时建立组件记录：上游地址、固定版本/commit、许可文件、引用或修改的文件、接入模块、验证命令与结果。优先通过依赖调用；如复制或修改源码，保留原有版权与许可信息，检查对应 NOTICE 和第三方条款。研究报告分别说明使用的开源组件、借鉴的方法及自行实现的部分。

本次没有复制第三方业务源码、下载数据集或运行上游性能测试；没有核实的兼容性不作为安装成功或性能保证。
