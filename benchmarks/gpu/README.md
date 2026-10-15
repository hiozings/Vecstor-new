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
