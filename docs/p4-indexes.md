# P4 持久索引与版本发布

状态：功能已接入，正式规模与完整故障验收仍在T9进行。设计见[索引发布设计](design/p4-index-publication.md)，实施记录见[计划](superpowers/plans/2026-10-16-p4-index-publication.md)。数据缓存、自适应策略和增量维护不属于本索引子阶段。

## 两个计算节点

在Conda vecstor环境中分别启动：

```bash
vecstor serve storage --data-root data/catalog --listen 127.0.0.1:50051
vecstor serve compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50052 --index-source published --timeout 60
vecstor serve compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50053 --index-source published --timeout 60
vecstor --compute 127.0.0.1:50052 --timeout 60
```

先按[用户指南](user-guide.md)创建数据库并导入至少1024行的向量表。以下假设库为demo、表为items：

```text
.open demo
.index publish items --operation-id publish-initial --timeout 60
.index publication-status publish-initial
.index load items --device cpu --timeout 60
.index inspect items
.index published items
```

再连接50053，选同一库，执行`.index load items --device cpu`和`.index inspect items`。在没有并发写入/发布时，两边的descriptor.artifact_id、generation应一致且cpu_ready为true，然后显式启用`.search mode approx`和所需路径。默认查询仍为CPU exact。

publish只构建并提交存储侧权威工件，不自动加载各计算节点。load只通过RPC下载、校验和激活，绝不训练。publish可指定`--nlist`、`--seed`和`--expected-generation`；load的`--artifact-id`只是预期当前工件身份校验，不是历史数据查询入口。

原有默认`--index-source local`与`.index build`保持兼容。双节点权威工件一致性要求两节点都选择published；该模式下`.index build`提示使用publish/load。本地存在同key但未绑定工件的对象时，load会明确拒绝，先确认无运行中引用再`.index clear`。

## 写入和失败处理

数据库级revision在数据写入后立即推进，包括其他表写入。新版本未发布/未加载时，CPU ANN按现有fallback选项精确回退或明确失败；不会使用旧版本索引返回新数据。批量更新采用已有单条多行SQL/托管导入事务，然后用新的operation-id完整重建发布。

已有读租约继续使用自己的数据与工件，最长60秒；新读固定新的发布head。同revision换构建配置也要按工件身份重新加载。发布不递增数据revision；publication generation单独计数。

超时或响应丢失时，先用`.index publication-status ID`确认状态；UNKNOWN不是成功或回滚证明。相同ID同意图重放返回原结果，不重复训练或发布；不同参数不可复用ID。发布成功后某节点加载失败不撤销已经提交的工件。每次发布/加载最多60秒，不能靠放大客户端timeout突破租约限制。

## 重启与GPU恢复

计算重启后默认内存索引为空，可显式load。也可在存储目录外创建预加载JSON：

```json
{"format_version":1,"targets":[{"database":"demo","table":"items","device":"cpu"}]}
```

将文件作为`--index-preload configs/preload.json`传给published计算服务。它在报告ready之前串行加载并复查head；配置错误、缺工件、预算不足或head变化均使启动失败。文件最多1MiB/128个目标，GPU目标隐含先加载CPU来源，无需为同一表重复列CPU/GPU。每目标最多60秒、整个预加载最多300秒。

GPU恢复使用`.index load items --device gpu`；从相同CPU工件重新克隆，worker故障不导致权威CPU工件丢失。GPU仍仅approx/search-first、gpu_id=0、单次L/nprobe≤2048，显式设备回退规则不变。published模式的质量配置须为v3并绑定实际工件摘要；见[工件质量验证工具](../benchmarks/p4/README.md)。

存储重启会核对所有目录引用和工件文件，已提交发布保留，未提交发布标为ABORTED，未完成回收继续固定删除集合。旧读token失效。损坏工件标为CORRUPT并拒绝ANN；精确查询仍可用，不自动改用旧revision或偷偷重建。

## 容量与回收

`.index clear [TABLE]`只清理计算内存。存储回收单独执行：

```text
.index prune --keep-last 2
.index prune --keep-last 2 --execute --operation-id prune-20261018
```

默认是只读预览。keep-last按每个仍存在的table_id最近激活的不同工件计数，额外保护当前数据head、有效读租约、下载引用和正在发布的工件；被保护的数量可以超过keep-last。已DROP的表在租约/下载等引用释放后可回收全部工件，同名重建使用新的table_id。回收保留key/digest墓碑和幂等结果，相同key不能复活为不同字节。deleted_bytes是工件payload加manifest的逻辑字节，不是文件系统实际释放块数。

存储参数：`--index-artifact-max-mib 256`（v1格式上限，可调低）、`--index-storage-mib 2048`、`--index-disk-margin-mib 256`、`--index-verifier-memory-mib 512`。原有CPU驻留预算256MiB/8条目、GPU预算等仍独立生效；工作集/RSS估算不是硬限。预算不足明确拒绝，不强删被引用对象。

使用`--isolate-storage-dir DATA_ROOT`时，计算进程和GPU worker还会实际探测已存在的工件payload/manifest读取被拒绝。没有共享目录读取替代网络分发。部署仍是单存储权威、可信网络，无TLS/鉴权，不代表存储高可用或生产完备。
