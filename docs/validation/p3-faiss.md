# P3 CPU Faiss 验证记录

日期：2026-10-13。开发分支：`p3-faiss-execution`，基于 P2.5；未合并或推送。依赖沿用 Conda `vecstor`，未新增包。P3 使用 CPU IVFFlat，GPU 业务路径未验证。

## 最终检查

- `conda run --no-capture-output -n vecstor python -m pytest -q`：**427 passed in 35.14s**。
- 最终 1 万条独立真实 PTY/Landlock 验收通过；360 次完整实验矩阵通过，进程均回收。
- `conda run -n vecstor python -m pip check`：No broken requirements found；未重装依赖。
- 从 proto 重新生成后，所有 pb2/pb2_grpc 模块哈希不变；文档链接和 `git diff --check` 通过。
- 最终实验前后源码 SHA256 一致，性能数据对应已验证的工作区源码。

## 验证范围

- 原 P1/P2/P2.5 精确查询、SQL/CRUD、持久化、类型协议和终端保持回归。
- 真正的 IVF prefilter 与 search-first 候选过滤；局部 nprobe/IDSelector、不连续大 int64、跨轮去重、候选扩展和严格预算错误。
- 默认精确回退、M<K、空集合、巨大有限值、float64 距离及 `(distance,id)` 排序。
- 同一读租约扫描/取向量/导出/过滤一致；并发旧读、新写、新索引，旧查询固定旧索引；同名表重建、其他表写入、过期 token 均不能误用缓存。
- 构建输入和索引实际 ID 验证、截断/错序/错误版本响应拒绝、预算回收与引用保护、取消/超时后清理及禁止发布。
- EXPLAIN 没有扫描/取向量，ANALYZE 只执行一次；会话选项独立、普通 SQL 不受影响。

## 真实终端与隔离

```bash
conda run --no-capture-output -n vecstor python scripts/check_p3.py \
  --output-dir results/p3-check-final-20261013 --rows 10000
```

该次通过。使用 10000 条二维定制数据、超过 2^53 的 ID，保证最近的前 100 条不满足过滤。真实 PTY 完成导入、建索引、双路径、EXPLAIN/ANALYZE、UPDATE 后失效/重建；RPC 结果与 P1 相同，先检索至少两轮且无全量 ScanMetadata。两次计算进程启动均有 Landlock ABI 4 的目录及文件读取拒绝证据；构建子进程继承隔离仍正常。计算服务重启后索引为空并精确回退；停存储后失败，全部子进程回收。

该定制用例使用 nlist=1，验证确定性扩展与协议正确性；ANN 召回测量使用下述多桶独立实验，不能将该用例的精确结果当作一般召回保证。

## 实验方法

```bash
conda run --no-capture-output -n vecstor python benchmarks/p3/run.py \
  --output-dir results/p3-matrix-final-20261013 \
  --code-version p3-faiss-execution-working-tree
```

矩阵：10000/100000 行、128 维，属性与向量独立/相关分布，过滤通过率 0.1%/1%/10%/50%/100%，K=10/100，精确/先过滤 ANN/先检索 ANN，每组重复 3 次，共 360 次。每个查询固定向量和 P1 LocalEngine 真值；Recall 分母为 min(K,M)。各行独立验证过滤、去重、数量、排序和 float64 原始距离，实际 exact 结果逐项等于 P1。

参数为 seed=20261013、默认 nlist（100/256）、初始 nprobe=8、默认候选扩展/预算及 fallback=exact。独立构建一次后串行热索引查询，无数据缓存；检索服务使用一个 OpenMP/BLAS 线程，构建 worker 使用两个线程。构建、导入时间单独统计，样本只有三次重复，不适合作为稳定尾延迟或生产吞吐指标。

`config.json` 保存参数及源码 SHA256，`environment.json` 保存硬件/依赖，`cases.jsonl` 保存每次查询向量、真值/结果 ID、实际计划和 Recall，`summary.json` 保存按配置分组的 P50/P95、串行吞吐和真正 ANN 召回，`scenario.json`/`report.json` 保存构建和进程峰值 RSS。原始文件位于 Git 忽略的 results/，复现命令不会覆盖已有目录。

## 最终矩阵结果

`report.json`：passed=true，360 次查询完成，120 次默认精确，168 次实际 ANN，72 次 ANN 请求转为精确；没有不完整成功结果，全部进程退出。矩阵总耗时 361.9 秒（含数据准备、服务生命周期、构建与查询）。代码 SHA256：`d2f06306e49beaf9189bc4ba0a76fe25144bfe0ef398fad33963e178d7b9c05c`。

下表平均值仅纳入实际 ANN，按该 K 下实际发生 ANN 的过滤档位及重复平均；各行进入 ANN 的档位不同，因此不能将均值直接作为路径优劣排名。回退单独列出。

| 行数 | 分布 | 请求路径 | 真正 ANN / 请求次数 | 平均 Recall@10 | 平均 Recall@100 | 精确回退次数 |
| --- | --- | --- | --- | --- | --- | --- |
| 10000 | independent | filter-first | 18 / 30 | 0.233 | 0.277 | 12 |
| 10000 | independent | search-first | 21 / 30 | 0.450 | 0.323 | 9 |
| 10000 | correlated | filter-first | 18 / 30 | 0.267 | 0.270 | 12 |
| 10000 | correlated | search-first | 21 / 30 | 0.500 | 0.337 | 9 |
| 100000 | independent | filter-first | 24 / 30 | 0.100 | 0.150 | 6 |
| 100000 | independent | search-first | 21 / 30 | 0.375 | 0.157 | 9 |
| 100000 | correlated | filter-first | 24 / 30 | 0.200 | 0.128 | 6 |
| 100000 | correlated | search-first | 21 / 30 | 0.350 | 0.153 | 9 |

真正 ANN 的单次 Recall@K 范围为 **0.0—1.0**；默认参数没有达到普遍 Recall@10≥0.95 的建议目标。原始记录保留了低召回结果；未提高参数后挑选结果，也未用精确回退抬高表中的均值。后续需在独立校准查询上调节 nprobe/候选预算，再在独立评估集合衡量质量与成本。

| 行数 | 分布 | 导入 s | 建索引 s | 索引估算 MiB | 计算峰值 MiB | 构建 worker 峰值 MiB | 存储峰值 MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10000 | independent | 3.24 | 0.44 | 5.01 | 238.26 | 208.50 | 224.77 |
| 10000 | correlated | 3.30 | 0.42 | 5.01 | 237.80 | 207.77 | 211.76 |
| 100000 | independent | 32.07 | 2.70 | 49.72 | 605.57 | 453.39 | 964.40 |
| 100000 | correlated | 31.87 | 2.72 | 49.72 | 606.45 | 453.43 | 964.53 |

进程峰值为 Linux VmHWM；worker 的 ru_maxrss 采样覆盖输出序列化，排除最后一次统计字段重打包。三者属于不同进程，不能把峰值直接相加解释为同时占用。索引字节预算明显小于进程实际 RSS，支持“预算是估算”的限制说明。宿主机未隔离为专用性能测试环境，延迟/吞吐仅作初步对照；全部分组 P50/P95 与 RPC 字节/次数以原始产物为准。

## 发现与修复

1. 初次 1 万行建索引在期限内无法完成：Python 3.12 下对 `communicate(input,timeout)` 超时后使用 `communicate(None,...)` 重试，使尚未发送完的 stdin 不再写入。改为一个 I/O 线程持续执行单次 communicate，主线程检查取消/期限并终止、回收 worker；大输入、传输中取消与期限测试通过。失败产物：`results/p3-check-20261013`、`results/p3-matrix-20261013`。
2. 修复构建后重跑矩阵，1 万行两分布通过，10 万×128 导入因原逐行浮点参数绑定触及 180 秒期限。改为 1024 行参数化批插入，向量绑定字符串转换为固定 FLOAT 数组，未增加依赖或修改 read lease 上限。跨批、大整数、极值/次正规 float32、负零按位比较与取消整事务回滚测试通过。失败产物：`results/p3-matrix-fixed-20261013`（180 条查询，不作为完整验收）。
3. 审查修复了 A→B→A 配置重复训练、构建前未回收无引用旧 revision、小表跳过非法参数验证、候选过滤缺少共享名额、worker 索引真实 ID 覆盖校验和峰值 RSS 采样范围问题。
4. 最终独立只读审查未发现有证据的重大功能、一致性或资源问题；审查结论不代替测试执行。

## 当前限制

- 默认 ANN 参数不是 Recall@10≥0.95 的保证；候选充足就停止可能遗漏真值。精确重排只保证所召回集合的距离和排序；需要完整精确结果时使用默认 exact。
- ANN 质量必须排除 exact 回退后单独报告。低选择率、合法集合很小或候选预算不足会触发精确计算，这不是 ANN 质量改善。
- 单机 localhost 串行实验；尚未进行跨机、网络延迟注入、并发 4/8、参数校准与独立评估集、P4/P5 自适应和消融实验。三次重复也不足以支持强性能结论。
- 内存预算为索引序列化大小与构建工作集估算，并非进程 RSS 硬限制。全表索引驻留；元数据和 SQL 结果仍可能物化，不支持超内存检索。
- 本阶段内存索引不在存储侧持久化/分发，计算重启后需重建；更新采用数据库 revision 保守失效，未实现增量维护。GPU、数据缓存、自适应优化及多计算节点版本发布待后续阶段。
- RPC 字节统计为存储接口 Protobuf 序列化负载，不含 TCP/TLS 开销；物理扫描行数未测量，不使用候选数冒充它。服务仍仅面向本机可信网络，无 TLS/鉴权。
