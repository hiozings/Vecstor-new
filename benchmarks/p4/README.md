# P4 索引实验入口

所有命令在项目根目录、Conda vecstor 环境执行，输出目录必须不存在。CPU/GPU性能测量不要并行运行。

## 已发布工件的质量验证

目标计算服务须使用published模式，且已经通过`.index load`加载待验证CPU/GPU工件。参数来自原校准目录，本工具不重新选参数、不训练。

```bash
python benchmarks/p4/validate_artifact_quality.py \
  --calibration-dir results/recall-calibration-20261014 \
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
