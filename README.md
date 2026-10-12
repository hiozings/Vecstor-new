# 面向混合查询的存算分离向量数据引擎研究

设计和优化面向混合查询的存算分离向量数据引擎，重点解决如何在存储和计算解耦的架构下高效处理结构化与非结构化数据的联合查询。通过存算分离架构，研究将分别优化数据存储和计算过程，提升向量数据的检索效率，并在同一查询中同时处理SQL查询和向量检索任务。

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

环境检查已通过，P1 本地精确混合查询基线已实现。当前业务路径使用 DuckDB 条件过滤和 NumPy 精确距离计算；存算分离服务及 Faiss ANN/GPU 业务调度在后续阶段实现。详细版本见 [环境说明](docs/environment.md)，项目计划见 [AGENTS.md](AGENTS.md)。

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

这些是正确性验收，不是性能基准；普通业务测试不需要 GPU 或网络监听权限。最新证据见 [P1 验证记录](docs/validation/p1-local-baseline.md)。
