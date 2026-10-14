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

## 独立质量校准

```bash
python benchmarks/p3/calibrate.py --output-dir results/recall-calibration
```

默认 1 万/10 万×128 维、独立/相关/带簇三类合成数据、五档过滤比例、两条路径，每组 200 个校准查询和 500 个独立评估查询。只用校准集选择平均 Recall≥0.97 且结果完整的最小初始 nprobe；参数先冻结落盘，再评估平均 Recall≥0.95。评估集与校准集重叠会被拒绝；评估不能反向改变参数。没有达到目标时返回非零退出码并保留失败报告，不自动重新调参。

这是严格 ANN 离线质量测量，包含生产候选扩展规则；空真值与不足 K 的真值使用已计算的精确 oracle，明确标为 exact，不能计入 ANN 均值。计时只含检索/重排，不含预先生成真值，不能用该计时代表 RPC 延迟或精确查询耗时。

产物含 dataset.npz、独立查询数组、每组冻结配置/校准网格汇总、逐查询 heldout.jsonl、候选 L 消融、构建统计、源码和脚本摘要及查询级 bootstrap 区间。`report.json` 的内存为离线校准进程峰值，不是计算服务预算。原始公开 embedding 在确认来源与许可后再加入；当前只生成可复现合成数据。

中断后以相同参数加 `--resume` 恢复，已完成组不会重算，冻结配置不会重选；不同参数或重复证据会被拒绝。恢复后报告的 elapsed_seconds 是本次恢复调用耗时，并非完整实验总时间。

## 冻结参数的真实 RPC 验证

```bash
python benchmarks/p3/validate_quality.py --calibration-dir results/recall-calibration \
  --output-dir results/recall-rpc-validation
```

为每个数据场景启动独立服务、导入对应 NPZ、显式构建同配置索引。每组默认取 4 个留出查询、重复三次，逐项核对返回 ID 与离线冻结结果及 float64 距离；这是 RPC 一致性证据，不能用四条查询替代 500 条独立质量验收。生成实际 database/table/revision/build 指纹绑定的 `quality-profiles.json`；原 nprobe=8 对照单独保留，精确回退不冒称 ANN 质量。
