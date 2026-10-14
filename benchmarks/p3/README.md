# P3 CPU 实验入口

从项目根目录的 Conda vecstor 环境运行，输出目录必须不存在：

```bash
python benchmarks/p3/run.py --output-dir results/p3-matrix --code-version <当前代码版本>
```

默认矩阵：10000/100000 行、128 维、independent/correlated 两类数据、0.1%/1%/10%/50%/100% 通过率、K=10/100、exact/filter-first/search-first、3 次重复，共 360 条查询。每个数据场景启动独立存储与计算进程，清空注册表后显式构建一次索引；查询采用热索引、串行执行。构建与导入时间单独记录，没有宣称模拟冷数据缓存或并发负载。

每个 selectivity/K 使用固定查询向量，三个路径与全部重复复用同一查询与独立 P1 LocalEngine 真值。Recall 分母为 min(K, matched_count)，空真值单独标记为 null；actual_mode、exact_fallback 和完整 execution_plan 保留，精确回退不计为真正 ANN。校验条件、唯一 ID、数量、排序与 float64 距离；不强制预设 Recall 或加速比。

产物包含 config.json、environment.json、cases.jsonl、summary.json、report.json，以及每个数据场景的服务日志、scenario.json 和原始 NPZ/持久数据库。source_sha256 标识运行时 Python 源码，--code-version 记录人工提供的版本。案例保留查询向量、真值 ID、返回 ID、客户端耗时、RPC 字节/次数和各阶段耗时。summary.json 按场景/查询/路径汇总 P50/P95、串行查询吞吐、平均 Recall 和独立的真正 ANN Recall；三次重复只描述波动，不构成稳定尾延迟保证。scenario.json 记录构建统计和 Linux /proc VmHWM 峰值 RSS；逻辑索引字节不代表 native 内存硬上限。

快速验证：

```bash
python benchmarks/p3/run.py --output-dir results/p3-small --sizes 1024 --dimension 2 --distributions independent --repeats 1
python scripts/check_p3.py --output-dir results/p3-acceptance
```

正式结果须保留完整默认矩阵；小规模运行仅验证脚本。只使用 CPU；本机可信 localhost，无 TLS/鉴权。网络受限沙箱需在允许 localhost 监听的环境执行。
