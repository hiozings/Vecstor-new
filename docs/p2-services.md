# P2 服务运行与隔离验收

本阶段提供独立 gRPC 存储服务和计算服务，保留 P1 本地执行器作为基线。所有命令从项目根目录、在 `conda activate vecstor` 后执行，不需要新装依赖。

## 启动与查询

先生成本地数据。目标文件必须不存在：

```bash
python scripts/vecstor.py generate --output data/p2/snapshot.npz --rows 10000 --dimension 3
```

终端一运行存储服务；仅此服务读取快照文件：

```bash
python scripts/serve.py storage --dataset data/p2/snapshot.npz --listen 127.0.0.1:50051
```

终端二运行计算服务；计算业务接口只需要存储地址：

```bash
python scripts/serve.py compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50052
```

终端三提交查询：

```bash
python scripts/vecstor.py remote-query --compute 127.0.0.1:50052 \
  --sql "SELECT id, category, value FROM items WHERE value < 100 ORDER BY vector_distance(vector, :query_vector) LIMIT 5" \
  --vector '[0,0,0]' --timeout 15
```

结果保留 P1 的 rows、distances、matched_count、metric、mode，并增加 snapshot_version 和 RPC 统计。`rpc_payload_bytes` 是计算侧与存储侧 Protobuf 请求/响应的序列化大小之和，不是网卡或 TCP 实际字节数。

进程启动成功后输出 `ready` JSON，包含角色、PID 和地址；`--listen 127.0.0.1:0` 可让系统分配端口。Ctrl-C 或 SIGTERM 会关闭服务。默认计算总超时 10 秒，可使用 `--timeout` 调整；外部请求更短的 deadline 优先生效。批量默认 256 行，`--batch-rows` 范围为 1—1024，并自动按向量维度缩小到 1 MiB 负载预算。

## 内核文件隔离

普通启动模式不修改当前用户的文件权限，但计算实现没有本地数据读取回退。若要让计算进程在操作系统层面无法读取存储目录，使用 Linux 验收模式：

```bash
python scripts/serve.py compute --storage 127.0.0.1:50051 \
  --listen 127.0.0.1:50052 --isolate-storage-dir data/p2
```

限制在导入计算依赖、启动线程之前应用。进程只允许读取解释器、项目源码/脚本及必要系统目录；不允许读取快照目录。它会亲自尝试目录列出与快照读取，两者都必须得到 PermissionError，并输出 `filesystem_isolation` 证据，之后才启动服务。该参数只用于施加限制和探测，不用于加载查询数据。

需要 Linux Landlock ABI ≥ 3。路径不能与允许读取的运行时目录重叠；存储目录应独立于 `src/`、`scripts/`、Conda 等目录。隔离不可用或探针失败时直接退出，不自动降级。该模式不等同于完整容器或多租户安全隔离。

## 自动化验收

```bash
python -m pytest -q
python scripts/check_p2.py --output-dir results/p2-independent --distribution independent
python scripts/check_p2.py --output-dir results/p2-correlated --distribution correlated
```

P2 测试会监听 localhost 并创建子进程；工具沙箱不允许监听时需在沙箱外执行。验收默认每组 10000 条、128 维，采用不连续 ID 并植入重复向量，运行 12 条 P1 负载及空结果、少于 K、并列、投影四条查询。逐项比较投影字段、命中数和距离，随后停止存储进程，确认计算查询明确失败。所有子进程最终回收。

输出目录不得已存在。产物包括：

- `private-storage/snapshot.npz`：仅存储进程可读取的测试快照。
- `queries.json`、`p1-results.json`、`p2-results.json`：输入及逐查询对照。
- `storage.log`、`compute.log`：进程启动与实际隔离探针证据。
- `report.json`：汇总、PID、拒绝访问结果、存储停止后状态和清理结果。

这些是同机 TCP 正确性与文件隔离验证，不是跨机部署或吞吐基准。最新记录见 [P2 验证记录](validation/p2-grpc.md)。

## 协议与运行限制

协议源文件位于 `proto/vecstor/rpc/services.proto`，更新后运行：

```bash
python scripts/generate_proto.py
```

生成的 Python 模块应随源码保存，避免使用者运行服务时额外生成。每次修改协议后须运行测试；固定依赖版本见环境锁定文件。

存储服务加载单个固定快照，版本为文件 SHA-256。修改源文件不改变运行中的快照，需重启加载；在线更新与版本退役留待 P4。元数据过滤由存储侧执行，Top-K 距离由计算侧执行，数据访问均走 RPC。

单消息上限 4 MiB，向量批次上限 1 MiB，超大输入/响应会明确失败。存储侧 8 个工作线程、最多 4 条活跃扫描流，超额扫描返回 RESOURCE_EXHAUSTED，避免流式背压占满线程并阻塞向量读取。计算侧 4 个工作线程；应用层重试次数为 0。元数据仍在存储内存中完整过滤并取出，本阶段不宣称超内存查询或最优按列传输。

默认未启用 TLS/鉴权，仅用于可信本地或受控实验网络。没有接入 ANN/GPU 业务路径；环境中的 Faiss/GPU 能力与 P2 精确查询实现分开记录。
