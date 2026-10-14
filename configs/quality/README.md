# 快照绑定的质量配置

`recall95-example.json` 是正式实验中一个实际快照的配置示例。只有其 database_id/table_id/revision/schema/dimension/metric/build fingerprint、过滤 SQL、K 和路径都匹配时才适用；重新导入同一 NPZ 也会生成不同数据库/表身份，不能直接套用。

通过 `benchmarks/p3/calibrate.py` 独立校准，再用 `benchmarks/p3/validate_quality.py` 对照真实 RPC，生成环境对应的 `quality-profiles.json`。校准数据和大规模结果留在 results/，这里不保存原始向量或查询数据。离线平均质量不是单条查询保证。

服务加载及终端选择接口将在后续质量配置接入任务提供；当前文件用于定义与审核格式。
