# GPU ANN 使用说明

GPU 版本加速 `approx/search-first` 的 Faiss IVFFlat 检索。索引训练、SQL 过滤、RPC、候选融合和 float64 精确重排仍在 CPU；不支持 GPU 精确查询、GPU filter-first、GPU训练、FP16或多GPU。

## 显式准备与逐语句选择

按 [用户说明书](user-guide.md) 启动服务、选库并导入至少1024行的向量表，然后运行：

```text
.index build items --device gpu --gpu-id 0
.index list
```

构建先复用或生成同快照的CPU索引，再克隆到GPU。普通查询不自动训练或克隆。下面假定表为三维：

```sql
SELECT id, category, vector_distance(vector, [0,0,0]) AS distance
FROM items WHERE value < 1000
ORDER BY distance LIMIT 10
SETTINGS device = 'gpu', mode = 'approx', path = 'search-first';
```

`SETTINGS` 是VecStor查询扩展。将device改为cpu可对照；不带SETTINGS时沿用原会话设置，默认CPU精确查询。只指定device不会自动改变mode/path，GPU示例须明确approx/search-first。设置优先级：本语句 > 当前会话 > 默认值，本条设置不会污染下一条。

还可指定gpu_id（当前仅0）、device_fallback、nprobe、candidates、max_candidates、max_rounds、fallback、quality_profile_id。值必须是字面量，重复键/未知键/表达式拒绝；普通SQL与CRUD不接受SETTINGS。SQL仍以分号提交，EXPLAIN/ANALYZE支持同一设置。

## 能力边界和回退

本机Faiss 1.12.0实测单次检索候选数L与nprobe上限为2048；L是候选数量，不仅是SQL LIMIT。低通过率需要更大候选集时GPU可能无法完成。参数不会被静默截断。GPU选择器实测被忽略，故明确不提供GPU先过滤。

默认 `device_fallback='error'`：指定GPU但设备/副本不可用、路径不支持、GPU候选上限不足时明确失败。允许转CPU需要显式写：

```sql
SELECT id FROM items WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10
SETTINGS device='gpu', mode='approx', path='search-first', device_fallback='cpu';
```

设备回退与原 `fallback='exact'|'error'` 分开。实际结果可能是GPU ANN、CPU ANN或CPU精确，必须看actual_device、actual_mode及回退原因；取消、超时和损坏数据不能被当成成功回退。CPU/GPU浮点候选可能不同，最终返回距离仍按CPU float64重排，GPU近似不等于全局精确。

`.plan on` 查看详细计划，`.mode json` 保留完整字段。gpu_host_ms为worker同步host调用耗时，gpu_ipc_queue_ms包含父子通信/排队，gpu_kernel_ms及独立H2D/D2H未测量时为null，不把整体时间称为kernel时间。EXPLAIN只读已有元信息，不初始化GPU或克隆索引。

## 版本与生命周期

写入使数据库revision变化，CPU/GPU旧副本都不能用于新读；已开始的旧读持有其快照与引用。重新 `.index build items --device gpu` 准备新版本。

```text
.index clear items --device gpu
.index clear items --device cpu
```

不指定设备时清理无活动引用的CPU/GPU副本，busy表示仍有读引用。副本在内存/显存中，计算服务重启后需显式重建。GPU任务超时/活动取消会终止该设备worker并失效其全部GPU副本，其他排队请求也明确失败；CPU服务仍可用，后续显式重建重新启动worker。

CPU构建成功但GPU克隆失败时保留CPU索引并报告错误，不将GPU状态标成ready。

## 资源与隔离

计算服务参数：

```bash
vecstor serve compute --storage 127.0.0.1:50051 \
  --gpu-index-memory-mib 512 --gpu-index-max-entries 8 \
  --gpu-scratch-mib 128 --gpu-memory-margin-mib 256 --gpu-max-pending 8
```

GPU默认单任务运行、最多8个等待任务，单次索引IPC负载至多512 MiB；索引预算为估算字节，不是进程或整块设备的硬内存上限。还需计入context、scratch、克隆临时数据以及其他程序占用。达到限制会拒绝或清理无引用副本，不强制驱逐活跃查询。

`--isolate-storage-dir data/catalog` 可验证计算进程和GPU worker均不能读取存储目录/文件，查询仍通过RPC完成。CUDA驱动需要读procfs/CPU拓扑、写既有procfs伪文件为后代辅助线程命名，以及访问特定NVIDIA字符设备。GPU隔离策略为此增加相应权限，不允许在procfs新建/删除文件；它是可信本地进程的存储文件访问隔离，不是针对恶意同用户进程的通用安全沙箱。CUDA磁盘缓存关闭，质量配置仍必须位于存储根之外。

## 质量配置

CPU v1配置不自动升级成GPU证据。GPU v2配置增加execution.device/precision/faiss_version/gpu_model，必须匹配实际索引身份、查询范围和GPU环境。`.search profiles` 查看后，SQL中可用 `quality_profile_id='实际ID'` 选择。

GPU独立校准入口：

```bash
python benchmarks/gpu/calibrate.py --output-dir results/my-gpu-calibration
```

输出目录必须不存在。每组200校准/500留出，冻结参数后评估；不支持组与评估失败组完整保留，没有CPU回退混入GPU均值。校准文件不能直接代替实际数据库身份绑定的服务配置。质量结果与能力限制见 [GPU验收](validation/gpu-ann.md)。

GPU构建和定向清理同样需要服务声明支持设备控制并确认响应设备；旧服务不能忽略device后把CPU构建报告成GPU成功，也不能因忽略设备参数误清CPU索引。

完整校准后可用 `python benchmarks/gpu/run.py --calibration-dir results/my-gpu-calibration --output-dir results/my-gpu-rpc` 做真实RPC校验并导出 `quality-profiles.json`。该工具针对校准生成的合成负载和新建验收库；不支持把这些配置直接用于任意已有用户库。自有数据需要独立校准并绑定自己的实际快照。流程、产物及短实验限制见 [GPU实验入口](../benchmarks/gpu/README.md)。

## 热会话与候选合并读取（G1/G2）

终端会话与复用的 `SessionClient` 会缓存只读查询能力，热查询不再逐次获取能力列表。构建/清理仍进行新鲜能力确认；每条向量查询仍校验完成帧的设备、质量配置和显式数据路径。普通 SQL/CRUD 不受会话向量选项影响。服务身份变化、错误或不支持响应会失效缓存。实验可用 `SessionClient(..., cache_query_capabilities=False)` 关闭缓存作对照。

先检索可以将候选过滤和合法向量读取合并为一个存储 RPC。默认关闭；存储服务必须支持新增协议，再在计算服务启动参数中加：

```bash
vecstor serve compute --storage 127.0.0.1:50051 --fused-candidates
```

该开关启用后，CPU/GPU ANN 先检索默认采用合并读取。可逐语句指定路径：

```sql
SELECT id FROM items
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10
SETTINGS device='gpu', mode='approx', path='search-first',
         candidate_data_path='fused';
```

`candidate_data_path='separate'` 使用原过滤后取向量路径；`default` 跟随服务设置。未开启服务开关时显式 `fused` 会拒绝，旧存储服务也不能静默忽略新请求。先过滤/精确查询不能显式选择 fused。该优化没有改变 ANN 参数、读租约、revision 校验或重排语义。

合并结果仍受每批1024个ID、1 MiB向量和4 MiB消息上限约束。超大元数据响应会拆分；单行仍过大时改用原两步读取，计划显示 `mixed` 并记录拆分/回退次数。这里仅改变数据传输方式，不表示设备回退或更换数据版本。计划中的 requested_candidate_data_path、candidate_data_path 和 counts 展示实际路径。

客户端计划 `client` 区分能力控制RPC与 ExecuteSQL；服务 `server_admission_ms` 包括接入调度和反序列化，不能当作纯线程排队。`gpu_queue_ms` 和 `gpu_ipc_ms` 分开报告等待及父子进程开销；后者为roundtrip扣除native调用的估计，仍不是独立CUDA事件计时。

完整覆盖全部候选空间时，严格GPU查询可直接返回全部合法记录（包括0条及不足K），计划显示 candidate_space_complete 和 candidate_space_exhausted；单有 nprobe=nlist 不能证明完整覆盖。队列满时明确拒绝，不因复用检查重置健康GPU副本。

当前合并读取对不同负载可能提速或退化，应按自己的负载对照后启用。原生batch 1/4/8实验已完成，但本轮未上线运行时合批；主动取消仍可能回收整个GPU worker。证据、测量口径与后续范围见 [G0—G2验收](validation/gpu-improvements.md)。
