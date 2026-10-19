# P4 索引实验入口

所有命令在项目根目录、Conda vecstor 环境执行，输出目录必须不存在。CPU/GPU性能测量不要并行运行。

## 已发布工件的质量验证

目标计算服务须使用published模式，且已经通过`.index load`加载待验证CPU/GPU工件。参数来自原校准目录，本工具不重新选参数、不训练。

```bash
python benchmarks/p4/validate_artifact_quality.py \
  --calibration-dir results/recall-calibration-20261014/10000-independent \
  --compute 127.0.0.1:50052 --database demo --table items \
  --device cpu --selectivity 1 --k 10 --path search-first \
  --output-dir results/p4-artifact-quality-cpu
```

GPU使用对应GPU校准目录并指定`--device gpu`，只支持search-first。实际数据ID/向量/value指纹、构建配置、Faiss版本和GPU机型须与所选冻结负载一致。至少200校准、500完整独立留出；验证实际后端、工件及revision不变、过滤/完整性和float64距离。校准平均Recall≥0.97、实际工件留出平均Recall≥0.95、零精确回退/零不完整时才导出v3质量配置。

产物为`cases.jsonl`、`report.json`和通过后才存在的`quality-profiles.json`。复用既有留出集属于工件一致性回归，不是新独立泛化证明。失败保留报告，不重调冻结参数，不仅复制旧summary/修改身份字段。

## 双节点生命周期与成本

```bash
python scripts/check_p4_indexes.py --output-dir results/p4-indexes-10k --rows 10000 --dimension 128
python scripts/check_p4_indexes.py --output-dir results/p4-indexes-100k --rows 100000 --dimension 128
python scripts/check_p4_indexes.py --output-dir results/p4-indexes-gpu --rows 10000 --dimension 128 --require-gpu
python benchmarks/p4/index_lifecycle.py --output-dir results/p4-lifecycle-costs
```

生命周期脚本启动存储、bootstrap与两个独立计算进程，实际PTY发布、工件文件拒读、10档过滤/K正确性、批量更新、重启预热、全进程恢复和回收；GPU按显式选项验证。失败仍保存report并清理进程。确定性进程崩溃及协议故障由`tests/integration/test_p4_index_crashes.py`等专项回归验证，最终验收矩阵合并真实脚本与pytest证据，不把脚本未执行的故障项记为通过。

成本工具默认1万/10万×128、独立/相关分布、每组3次重复、并发1/4/8。每次重复使用新的进程与数据根，管理阶段单独计时。查询成本使用热工件、全nprobe、一个固定向量，各并发客户端复用连接，每客户端4个计时请求；样本数有限，P95仅为描述性统计，不代表普遍ANN性能。恢复与重建的范围不同，原始阶段耗时保留，不承诺提速。

每5ms采样各进程RSS/CPU和索引文件占用；GPU成本运行可加`--require-gpu`，此时整卡显存包含其他程序且采样可能漏峰。build/verifier峰值来自其子进程自报，独立记录；不同进程峰值不能相加当同步峰值。storage_verify_ms为验证子进程main阶段，storage_install_ms包含落盘/fsync/rename，finalize_rpc_ms还包含RPC、启动和目录提交；未独立测量的storage_catalog_commit_ms为null。所有传输字节为Protobuf/工件逻辑字节，非网卡流量。

verifier峰值使用exec后`/proc/self/status`的VmHWM，同时保留原始ru_maxrss计数。实测表明小子进程的ru_maxrss可带入父进程exec前峰值；[getrusage手册](https://man7.org/linux/man-pages/man2/getrusage.2.html)也说明资源计数跨exec保留。原build-worker字段保留历史含义，不能把它与verifier的exec后VmHWM直接比较；内核RSS计数和采样都不是原生分配器硬限。

整体设计与验收要求见[设计](../../docs/design/p4-index-publication.md)。

2026-10-18正式结果及最终回归/故障矩阵见[P4验收记录](../../docs/validation/p4-indexes.md)。原始成本版本与后续审查修复版本分别记录，不把测量前的耗时当作最终修复后重测。

## 数据缓存与字段传输（代表性验收完成）

```bash
python benchmarks/p4/data_reads.py --output-dir results/p4-data-smoke --tier smoke
python benchmarks/p4/data_reads.py --output-dir results/p4-data-representative --tier representative
# 完整tier较耗时；每场景200查询、三次重复。
python benchmarks/p4/data_reads.py --output-dir results/p4-data-correctness --tier correctness
python benchmarks/p4/data_reads.py --output-dir results/p4-data-costs --tier costs
python benchmarks/p4/data_reads.py --output-dir results/p4-data-gpu --tier gpu \
  --gpu-calibration-dir results/gpu-calibration-20261015
```

用户已选择本轮只执行representative：38个场景、630次配置运行、33,744条计时查询。主CPU读成本为1万/10万×独立/相关×并发1/4/8，每阶段20查询、3重复、8种cache/projection/separate-fused组合；另有exact/filter-first的小样本正确性探针、选择性负载、8/64MiB容量、0/1/5ms单向延迟和两个GPU冻结支持负载。exact/filter-first探针每阶段仅1查询，不能作延迟统计推论。容量组每阶段200查询；各场景精确参数记录于config.json，不声称全量覆盖。

为串行运行不同后端，可使用`--case-ids`选择已输出manifest的编号；选择写入配置，部分运行不会标成full coverage。当前representative的0—35为CPU，36—37为GPU。GPU组会先按已发布工件重新执行对应500条留出验证并导出v3配置，然后带实际质量配置测试优化；缺校准目录明确失败，不用随意参数替代。

每个变体新建计算进程、加载同一工件、预热各客户端一次后清缓存，再按cold/warm/independent/hotset序列测量；cold仅指应用缓存已清，索引、会话与OS页缓存已热。warm复用cold查询，independent换seed，hotset80%取前20条cold查询，状态延续前序阶段，实测命中率不预设。配置顺序固定seed随机化；答案参考进程在计时前退出，避免额外驻留索引/显存影响比较。正确性校验在吞吐计时之外；每查询保留原始计划/答案、独立NumPy真值及冻结基线签名。少于20样本不输出P95，其余分位数仍为描述性统计。

延迟由测试专用loopback代理在读取RPC转发前/首响应前各加固定时延，后续stream帧不重复延迟，不模拟带宽/拥塞；同时保留0ms代理对照和BeginRead实测往返（含存储执行）。代理与实验驱动共进程，资源字段明确包含驱动开销，不冒充纯代理CPU。网络字节仍为Protobuf序列化字节。

资源每5ms采样；compute为新进程，storage在同数据组中复用，storage CPU字段是累计进程生命周期值，不能直接解释为单变体CPU开销。缓存记账峰值为查询完成时采样，工作区峰值来自服务计数；不是物理RSS硬限。未执行的完整tier在报告标NOT_RUN。

最终838项完整回归及代表性630次配置/33744查询通过，运行时源码9362353；完整收益、退化、资源口径和未跑项见[缓存验收记录](../../docs/validation/p4-cache.md)。原始报告位于results/p4-cache-data-cpu-20261019-final和gpu对应目录；汇总脚本/表格在results/p4-cache-final-20261019。
