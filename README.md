# 面向混合查询的存算分离向量数据引擎研究

设计和优化面向混合查询的存算分离向量数据引擎，重点解决如何在存储和计算解耦的架构下高效处理结构化与非结构化数据的联合查询。通过存算分离架构，研究将分别优化数据存储和计算过程，提升向量数据的检索效率，并在同一查询中同时处理SQL查询和向量检索任务。

## 用户使用说明书

首次使用请阅读 [VecStor 用户使用说明书](docs/user-guide.md)：从环境准备、三个终端启动，到建库、SQL 增删改查、数据导入、ANN 与质量配置、执行计划及常见问题，提供可连续执行的示例。

## 开发环境

使用 Conda 独立环境 `vecstor`。当前机器已完成安装，无需再次创建：

```bash
conda activate vecstor
python scripts/check_environment.py
```

在新的 Linux x86_64 机器上，可通过精确锁定文件复现：

```bash
conda create -n vecstor --file environments/vecstor-linux-64.explicit.txt
```

依赖声明见 `environment.yml`。环境包含 Python、DuckDB、SQLGlot、Faiss CPU/GPU、gRPC/Protobuf 和 pytest。默认检查需要可用的 NVIDIA GPU 和本地 TCP 监听权限；沙箱受限时在沙箱外执行。仅验证 CPU 时可显式使用 `--cpu-only`。

环境检查已通过，P1 本地基线、P2 gRPC 存算分离和 P2.5 交互式 SQL/持久 CRUD 已实现。P3 已接入 CPU Faiss IVFFlat、两条近似路径、候选扩展和执行计划，默认仍为 NumPy float64 精确查询。GPU 已新增显式 ANN 先检索路径，CPU 训练与 float64 重排保留；持久索引分发与自适应策略留待后续阶段。详细版本见 [环境说明](docs/environment.md)，项目计划见 [AGENTS.md](AGENTS.md)。

## 交互式 SQL 终端

本机已安装 `vecstor` 命令。新环境需从项目根目录注册入口（不重新解析依赖）：

```bash
conda activate vecstor
python -m pip install --no-deps --no-build-isolation -e .
```

分别在三个终端运行：

```bash
vecstor serve storage --data-root data/catalog
vecstor serve compute --storage 127.0.0.1:50051
vecstor --compute 127.0.0.1:50052
```

进入后用 `.open demo --create` 建库/选库，直接输入 SQL。回车续行，分号后回车执行；支持表管理、INSERT/UPDATE/DELETE、普通 SELECT、聚合和内联向量查询。可用 `.help`、`.databases`、`.tables`、`.schema`、`.datasets`、`.import`。数据持久保存于存储服务，终端不打开数据库文件。

完整示例、SQL 范围与隔离运行方法见 [终端使用说明](docs/sql-shell.md)。旧脚本和 P1/P2 入口保留兼容。

## CPU 近似查询（P3）

在已选库、已导入向量表（至少 1024 行）的终端中显式建索引并开启近似模式：

```text
.index build items
.search mode approx
.search path search-first
.plan on
```

执行与精确模式相同的向量 SQL；用 `.search path filter-first` 切换先过滤路径，`.search reset` 恢复精确默认值。写入后旧索引失效，查询默认精确回退，再次 `.index build items` 恢复 ANN。`.index list` 查看版本；EXPLAIN/EXPLAIN ANALYZE 展示逻辑/实际计划。近似输出明确标识，精确重排候选不等于全局精确召回。

质量配置可由计算服务 `--quality-profiles FILE` 显式加载，再用 `.search profiles` 和 `.search profile ID|off` 选择；配置匹配实际数据/索引版本及查询条件，离线质量不等于单条保证。

完整命令、资源约束和实验入口见 [P3 使用说明](docs/p3-search.md)，验证结果见 [P3 验证记录](docs/validation/p3-faiss.md)。

正式独立质量校准可运行 `python benchmarks/p3/calibrate.py --output-dir results/recall-calibration`，默认每组 200 个校准查询和 500 个独立评估查询，包含带簇数据；详细恢复规则见 [实验说明](benchmarks/p3/README.md)。

## 本地混合查询

以下命令在项目根目录和 `vecstor` 环境中执行。无需 pip 安装本项目，命令行入口会加载 `src/`。

生成 1000 条三维示例数据：

```bash
python scripts/vecstor.py generate --output data/example.npz --rows 1000 --dimension 3
```

执行先过滤、再精确 Top-K 的查询：

```bash
python scripts/vecstor.py query --dataset data/example.npz \
  --sql "SELECT id, category, value FROM items WHERE value < 100 ORDER BY vector_distance(vector, :query_vector) LIMIT 5" \
  --vector '[0,0,0]'
```

结果包括投影字段、对应平方 L2 距离、命中总数和执行路径。同距离按 id 升序；空结果和不足 K 不补齐。不支持的 SQL、非法向量和损坏快照会报错，CLI 退出码为 2。完整字段与 SQL 子集见 [数据与查询规范](docs/design/data-and-query-spec.md)。

## 测试与验收

```bash
python -m pytest -q
python scripts/vecstor.py demo --output-dir results/p1-independent --distribution independent
python scripts/vecstor.py demo --output-dir results/p1-correlated --distribution correlated
```

每组默认生成 10000 条、128 维数据，运行 12 条查询，并与独立 NumPy 真值比对。产物为 `dataset.npz`、`queries.json`、`ground_truth.json`、`actual.json` 和 `report.json`。输出文件和目录必须不存在，以防误覆盖；重复运行请更换路径。`data/` 和 `results/` 为 Git 忽略的本地产物。

这些是正确性验收，不是性能基准；P1 本地测试不需要 GPU 或网络监听，完整测试套件包含需 localhost 监听的 P2 测试。P1 证据见 [P1 验证记录](docs/validation/p1-local-baseline.md)。

## P2 存算分离

分别启动两个进程，存储服务持有数据，计算服务只配置存储地址：

```bash
# 终端一：复用前面生成的三维示例
python scripts/serve.py storage --dataset data/example.npz --listen 127.0.0.1:50051
# 终端二
python scripts/serve.py compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50052
# 终端三
python scripts/vecstor.py remote-query --compute 127.0.0.1:50052 \
  --sql "SELECT id FROM items WHERE value < 100 ORDER BY vector_distance(vector, :query_vector) LIMIT 5" \
  --vector '[0,0,0]'
```

通过 Linux Landlock 验证计算进程无法读取存储目录，并与 P1 结果对照：

```bash
python scripts/check_p2.py --output-dir results/p2-check --distribution independent
```

验收会自动创建独立服务、施加文件隔离、验证 16 条查询、停止存储后确认查询失败，并回收进程。输出目录必须不存在；沙箱限制网络监听时应在沙箱外运行。详细配置、错误行为与限制见 [P2 服务说明](docs/p2-services.md)。

ANN 质量优化验收：60 组独立留出评估最低平均 Recall@10 为 0.9702，默认仍为精确查询。显式配置与实测延迟、资源、存算隔离证据见 [质量优化验收](docs/validation/ann-recall-optimization.md)；该统计不保证任意数据或每条查询达到同样召回。

## GPU ANN 与逐条 SQL 设备选择

先在已选库的终端显式准备副本：`.index build items --device gpu`。随后可在三维向量查询中指定：

```sql
SELECT id FROM items WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10
SETTINGS device='gpu', mode='approx', path='search-first';
```

默认仍为CPU精确查询。GPU仅支持先检索、每轮候选/nprobe最多2048；默认不静默回退CPU。写入/计算重启/worker失效后需显式重建。使用方式、质量与资源边界见 [GPU说明](docs/gpu-search.md)；[GPU验收](docs/validation/gpu-ann.md) 保留不支持及完整性失败组，不宣称所有负载都达标或加速。

G0—G2已补强GPU候选完整性和响应校验，减少热会话能力查询，并新增可选 `--fused-candidates` 合并读取及SQL `candidate_data_path` 覆盖。默认关闭合并读取；运行时合批尚未上线。使用方法见 [GPU说明](docs/gpu-search.md)，性能与局限见 [改进验收](docs/validation/gpu-improvements.md)。

## P4 持久索引（验收进行中）

新增显式`--index-source published`，通过`.index publish`向存储发布权威CPU工件，两个计算节点分别`.index load`并按读租约绑定同一工件。支持GPU重克隆、显式启动预加载和受引用保护的回收；默认local/精确查询保持兼容。操作流程和边界见[P4索引使用说明](docs/p4-indexes.md)。T9正式验收仍在进行，不代表全部P4完成。
