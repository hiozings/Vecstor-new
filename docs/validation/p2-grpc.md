# P2 gRPC 存算分离验收记录

日期：2026-10-12。分支：`p2-grpc-storage-compute`。设计见 [P2 协议与架构](../design/p2-grpc.md)，运行方法见 [服务说明](../p2-services.md)。

## 结果

全套 `conda run -n vecstor python -m pytest -q`：**159 passed**，退出码 0。保留原 P1 的 133 项测试，新增 26 项 P2 测试。

在最终修复后的代码上，分别运行：

```bash
conda run --no-capture-output -n vecstor python scripts/check_p2.py \
  --output-dir results/p2-2026-10-12-independent-final \
  --rows 10000 --dimension 128 --distribution independent
conda run --no-capture-output -n vecstor python scripts/check_p2.py \
  --output-dir results/p2-2026-10-12-correlated-final \
  --rows 10000 --dimension 128 --distribution correlated
```

| 验收项 | 独立分布 | 相关分布 |
| --- | --- | --- |
| 数据规模 | 10000 × 128 | 10000 × 128 |
| 查询与 P1 一致 | 16/16 | 16/16 |
| 计算进程目录列出 | PermissionError | PermissionError |
| 计算进程快照读取 | PermissionError | PermissionError |
| 隔离机制 | Landlock ABI 4 | Landlock ABI 4 |
| 停止存储后查询 | UNAVAILABLE | UNAVAILABLE |
| 测试后两个进程 | 均已回收 | 均已回收 |

合计 32 条查询，比较全部投影字段、命中数及距离；距离采用 rtol=0、atol=0 的严格比对。每组包含 P1 的 12 条过滤负载，另加空结果、少于 K、并列与全字段投影。不连续 ID 和重复向量由验收脚本植入，防止行号/ID 混用与并列排序错误被普通随机数据掩盖。

## 隔离证据与网络依赖

隔离不是更换工作目录或省略文件参数：计算进程在导入数值/RPC 库和创建线程之前安装 Landlock 规则，存储目录不在允许列表中。随后该进程主动列出目录和读取真实快照，均捕获 PermissionError，才启动 Query 服务。

两组最终输出中的 `compute.log` 保存 `filesystem_isolation` 及 ready 事件；`report.json` 保存探针结果和不同存储/计算 PID。`queries.json`、`p1-results.json`、`p2-results.json` 保存逐条输入与结果。文件均位于上述被 Git 忽略的 results 目录，可使用新的输出目录复现。

停止存储后，原计算服务的下一条查询返回 UNAVAILABLE，没有使用本地文件或全量数据缓存回退。服务结束后验收脚本确认两个子进程均不再运行。

## 错误与并发覆盖

- 固定快照版本、完整元数据流的 total/offset、同序向量 ID、字节长度、维度、有限值检查。
- 错误版本、非法 SQL、未知或重复 ID、过大批次明确拒绝。
- 损坏/缺失/乱序响应触发 DATA_LOSS，不返回部分结果。
- 下游不可达、计算非法向量、CLI 远程调用、总 deadline 传播。
- 四条并发查询的状态独立，int64 字段不经浮点 JSON 中转，结果与 P1 一致。

独立审查发现：较大的 ScanMetadata 流受背压阻塞时，四条流可能占满四个存储线程，FetchVectors 无线程可用，从而超时。已用 300 行、每行 100000 字符 category、单行批次复现失败；修复为 8 个工作线程并以非阻塞信号量将扫描限制为最多 4 条。四条大查询现在均成功；额外扫描立即得到 RESOURCE_EXHAUSTED，已占满扫描名额时向量请求仍成功。两项回归测试由失败转为通过，随后全套 159 项通过。

另核对 grpcio-tools 重新生成的两个 Python 协议模块与已保存源码逐字节一致，`git diff --check` 无格式错误。未新增依赖、修改 Conda 环境或更改 P1 排序语义。

## 边界

这是同机独立进程、真实 TCP 与内核文件权限的验收，没有开展跨物理机器、吞吐或 GPU 性能实验。存储侧只读单快照，在线更新、索引分发、ANN/GPU 调度、缓存优化与多计算节点版本同步仍待后续阶段。

默认普通服务模式未施加 Landlock；明确使用隔离参数或验收脚本时才有上述内核权限保证。Landlock 在本实验中证明文件访问被拒绝，不等同于完整容器隔离。TLS、身份认证与不可信网络防护不属于当前本地原型范围。

已按用户授权创建功能分支并保留原有 AGENTS 修改；本次未提交、推送或合并到 main。
