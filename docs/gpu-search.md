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

GPU默认单任务运行、最多8个等待任务；索引预算为估算字节，不是进程或整块设备的硬内存上限。还需计入context、scratch、克隆临时数据以及其他程序占用。达到限制会拒绝或清理无引用副本，不强制驱逐活跃查询。

`--isolate-storage-dir data/catalog` 可验证计算进程和GPU worker均不能读取存储目录/文件，查询仍通过RPC完成。CUDA驱动需要读procfs/CPU拓扑、写既有procfs伪文件为后代辅助线程命名，以及访问特定NVIDIA字符设备。GPU隔离策略为此增加相应权限，不允许在procfs新建/删除文件；它是可信本地进程的存储文件访问隔离，不是针对恶意同用户进程的通用安全沙箱。CUDA磁盘缓存关闭，质量配置仍必须位于存储根之外。

## 质量配置

CPU v1配置不自动升级成GPU证据。GPU v2配置增加execution.device/precision/faiss_version/gpu_model，必须匹配实际索引身份、查询范围和GPU环境。`.search profiles` 查看后，SQL中可用 `quality_profile_id='实际ID'` 选择。

GPU独立校准入口：

```bash
python benchmarks/gpu/calibrate.py --output-dir results/my-gpu-calibration
```

输出目录必须不存在。每组200校准/500留出，冻结参数后评估；不支持组与评估失败组完整保留，没有CPU回退混入GPU均值。校准文件不能直接代替实际数据库身份绑定的服务配置。质量结果与能力限制见 [GPU验收](validation/gpu-ann.md)。
