# P4 自适应查询策略使用说明

状态：实现与工具验证中，正式质量、性能和最终回归结果见[验收记录](validation/p4-adaptive.md)。本功能默认不启用；默认查询仍为CPU exact/filter-first。

## 请求与服务

- `path='auto'`显式启用选择；CPU approx要求published索引、已加载当前工件及计算侧策略文件。
- `mode='exact',path='auto'`保持exact；`fallback='error'`不允许成本模型主动选择exact。
- GPU继续使用显式`device='gpu',path='search-first'`；GPU auto拒绝。
- 质量profile按路径、工件、SQL/K及向量范围独立校验。指定单一profile会限制候选路径；成本模型不能降低预算。
- 配置无效拒绝启动；成本环境或输入不匹配时保守选择合法参考路径；没有合法ANN质量证据时按exact/error处理。

计算启动参数：

```text
--index-source published
--adaptive-policy <prepare生成的policy.json>
--adaptive-network-class loopback
--adaptive-mode apply|shadow
--quality-profiles <对应数据组的quality-profiles.json>
--data-cache-mib 64
--metadata-projection projected
```

`apply`执行建议计划，`shadow`只执行参考计划并报告建议，不做双重查询。两个计算节点可以选择不同路径，但必须匹配各自请求的同一权威快照。策略文件启动加载后冻结，替换文件需重启计算服务；更新数据/工件后不能沿用旧质量映射。

终端可用`.search path auto`；SQL SETTINGS覆盖当前请求、不修改会话。以下是SQL结构模板，数组须替换成真实维度的数值：

```sql
SELECT id, vector_distance(vector, [/* 实际向量 */]) AS distance
FROM "items" WHERE value < 10000 ORDER BY distance LIMIT 10
SETTINGS mode='approx', path='auto', fallback='error',
         data_cache='on', metadata_projection='projected', candidate_data_path='separate';
```

成本证据绑定实际有效选项与投影；默认设置和等效显式设置共享指纹。切换网络类别、并发档位、缓存配置或代码/依赖后，可能没有匹配成本模型，此时EXPLAIN明确显示原因。部署指纹读取DuckDB包元数据，不在计算进程初始化DuckDB；策略加载保持隔离前纯标准库读取。

性能修复版导出器保留被拒绝拟合的原始风险统计，并以`cost_usable=false`标记其成本不可用（旧配置缺省为true，仍受误差门槛校验）。这些条目不参与延迟比较，不计为成本覆盖成功；同工件、预算和上下文的失败率证据不会再因成本拟合未通过而消失。真实预算失败观测及预测候选不足仍使风险转为未知。新字段不被旧解析器接受时应拒绝启动，不可静默忽略。

## 校准与验证

在项目根、vecstor环境执行，不设置PYTHONPATH。各输出目录必须新建；仅明确`--resume`允许恢复同一清单的未完成作业。

```bash
# 先估算真实耗时；只作pilot，不作为正式验收。
python benchmarks/p4/adaptive.py pilot --output-dir results/adaptive-pilot --case-ids 0 5

# 独立组件质量、成本拟合及冻结策略；默认9组代表性CPU负载。
python benchmarks/p4/adaptive.py prepare --output-dir results/adaptive-calibration

# 冻结后评估：固定FF/SF、auto、exact探针和1%阈值基线。
python benchmarks/p4/adaptive.py evaluate --calibration-dir results/adaptive-calibration \
  --output-dir results/adaptive-evaluation

# 恢复：参数/源码/策略与原始证据校验必须全部一致。
python benchmarks/p4/adaptive.py evaluate --calibration-dir results/adaptive-calibration \
  --output-dir results/adaptive-evaluation --resume

# 预先声明的成本输入消融，保留真实候选参数和读路径。
python benchmarks/p4/adaptive.py ablate --calibration-dir results/adaptive-calibration \
  --output-dir results/adaptive-ablate-cache --case-ids 1 5 --ablation cache-input
```

其他消融为`transfer`、`candidate-correlation`、`observations`。只关闭显式成本项，不声称分离掉不可辨识的截距成本。文件parameters中的四个布尔开关默认全开；消融文件有独立policy_id/hash，完整模型的拟合误差不能被当作消融模型的误差保证。

`evaluate`支持`--samples 20`、`--concurrency 1|4|8`、`--delay-ms 0|1|5`和`--patterns cold warm independent hotset`。代理延迟是测试注入量，不是直接测得的网络RTT。未校准的并发/网络类别应明确退化，不能把原本机串行模型改名冒充该环境证据。

ANN成本拟合使用128个不同向量（96拟合、32验证），组件质量校准与留出各500，最终完整策略另用500个不重叠向量。主计时取最终集合前200条，每种配置三次重复。exact默认只用5个不同探针（3拟合/2验证，三次重复）；该小样本桶单列，不报告稳定尾延迟。需更广证据时使用`prepare --exact-fit-queries 128`及`evaluate --full-exact`，高通过率exact可能耗时很长。

`smoke`只证明工具链，短样本不得生成ANN质量profile；其auto只能走合法exact退化，报告明确标为`smoke_exact_only`。

计时尝试在RPC前写入`attempts.jsonl`，失败和中断证据保留；失败或中断的trial不能通过`--resume`重跑成成功，修复后须使用新输出目录。已完整完成且哈希一致的trial可以复用。逐查询延迟不含日志写入，吞吐包含持久日志开销，二者口径不同。

性能发布判定要求每个冷热状态至少三次重复、每次200条、模型覆盖率至少95%；两固定路径实测差距达到20%时，还要求选择较便宜路径的比例至少90%。同时检查P50/P95 regret与规划/反馈开销。20条扩展探测仅作诊断，不能通过正式性能门槛；correctness层的短样本性能标为`NOT_RUN`。字节模型与延迟模型都只用拟合集训练，验证集不参与预处理拟合。

## 计划和限制

`execution_plan.adaptive`显示建议/执行方案、资格排除、M及来源、候选轮次、缓存scope与命中预测、RPC/字节估计、成本区间、策略身份和实测规划/反馈时间。EXPLAIN不扫描或更新观测，ANALYZE执行一次。每条查询仍访问存储取得租约并过滤；缓存不会提供离线成功。

历史记录仅保存有界标量与摘要（256条、TTL300秒），不保存过滤结果或候选ID。缓存重复证据绑定查询、路径、有效预算、工件及丢失/清理序号。规划不遍历全缓存；显式管理统计仍可能遍历。小型开发探针的耗时不能替代正式128维负载的规划开销验收。

完整算法及研究边界见[设计](design/p4-adaptive-query-strategy.md)、[八任务计划](superpowers/plans/2026-10-23-p4-adaptive-query-strategy.md)。
