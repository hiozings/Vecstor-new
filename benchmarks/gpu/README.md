# GPU 实验复现

从项目根目录的Conda vecstor环境运行；输出目录必须不存在，GPU/localhost需真实设备权限。

```bash
python scripts/check_gpu_ann.py --output-dir results/gpu-capabilities
python scripts/check_gpu.py --output-dir results/gpu-check --rows 10000
python benchmarks/gpu/calibrate.py --output-dir results/gpu-calibration
python benchmarks/gpu/run.py --calibration-dir results/gpu-calibration --output-dir results/gpu-rpc
VECSTOR_REQUIRE_GPU=1 python -m pytest -q
```

校准为1万/10万×128维、三分布、五通过率、K10/100，共60组，每组200校准/500独立留出。选择参数只看校准集；冻结文件先落盘，留出失败保留。合法数<K、不满足质量/完整性或候选上限的组单列，绝不让CPU回退提高GPU均值。无支持配置的组为unsupported，留出失败为failed；出现failed退出2，原始数据/报告仍可分析。

RPC工具仅对passed组执行成本对照和配置导出，同时把未入选组完整写入excluded_groups，不要求原矩阵总体passed=true来隐藏失败。正式样本数满足200/500时导出v2实际数据库快照绑定配置，重启隔离计算服务加载该文件、重建匹配GPU索引并验证。小规模调试样本只用显式参数，不导出伪造质量证据。

RPC每组3个留出向量×3次重复，CPU/GPU×并发1/4/8。GPU返回ID须与冻结结果一致、所有路径过滤/数量/float64距离校验；CPU同参数完整留出质量见校准cpu_summary。P95只有9个样本，属于描述性短实验。查询P50/P95不含客户端建连/选库，吞吐包括每请求客户端开销；每组先预热，克隆另记build.json。

每5ms采样compute/storage/GPU worker RSS和设备整体VRAM，CPU时间分别记录。VRAM包含桌面及其他程序，不是单查询显存，采样可能漏峰；设备指标通过本机NVML共享库获取，未添加Python依赖。raw cases.jsonl保留计划、RPC/字节、GPU host/IPC队列统计。单请求批量化、GPU训练与分片检索不属于本实验。

产物包括源代码摘要、配置、冻结参数、逐查询真值/结果、分组报告、服务日志、持久数据库和实际可加载quality-profiles.json。数据库重导入会换身份，不能直接复用旧配置。正式结果与限制见 [GPU验收](../../docs/validation/gpu-ann.md)。

## G1/G2 热会话与原生合批对照

```bash
python benchmarks/gpu/sessions.py --calibration-dir results/gpu-calibration --output-dir results/gpu-sessions-g1 --variants uncached cached
python benchmarks/gpu/batch_probe.py --calibration-dir results/gpu-calibration --output-dir results/gpu-native-batch
python benchmarks/gpu/sessions.py --calibration-dir results/gpu-calibration --output-dir results/gpu-sessions-g2 --variants cached fused
```

sessions每个并发工作线程持有自己的连接，先选库/预热，再计查询耗时。默认三类冻结负载（1万/10万、10%/100%、K10/100），并发1/4/8，CPU/GPU、两个变体，共36组；每组500个不同留出查询重复三次，合计54000请求。每次重复重排查询顺序，并交替CPU/GPU执行顺序。变体执行顺序固定，温度/时序漂移仍可能影响比较，不宣称统计上的普遍加速。

该对照复用冻结ANN参数，CPU/GPU使用相同显式SETTINGS；不在计时请求中加载质量profile，也不重新调参。GPU结果与冻结候选答案比对，所有查询核验过滤、数量、float64距离及召回。已有留出集合用于传输/会话回归，不是新的独立质量校准。P50置信区间按查询跨三次重复聚类bootstrap（500次）；它不包含硬件或时间漂移的不确定性。CPU/GPU克隆、冷启动和旧run.py的短新连接实验另列。

`uncached`关闭查询能力缓存；`cached`启用缓存；`fused`同时开启候选过滤+向量合并RPC。记录客户端能力RPC、存储RPC、接入/native/GPU队列/IPC计时、吞吐、P50/P95以及compute/storage/worker RSS/CPU和设备整体VRAM。指标定义及5ms采样限制同上。

batch_probe只测实际Faiss GPU host调用，批大小1/4/8；核验超过2^53的int64标签和逐查询/批查询候选集合与顺序。它没有RPC、排队等待或生产批调度，不可用摊销native时间声称端到端加速。G1测得native占比低、GPU排队短，本轮保留实验而未部署运行时合批、批内取消或二进制IPC。

正式结果与延期依据见 [G0—G2验收](../../docs/validation/gpu-improvements.md)。
