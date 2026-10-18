# VecStor 用户使用说明书

适用版本：2026-10-15 的 P3、ANN 召回率优化与GPU先检索版本。

VecStor 支持在同一条 SQL 中筛选结构化字段，并按向量距离返回最相近的记录。它也支持普通表的查询、聚合和基础增删改。默认采用精确向量查询；需要加速时，可以显式建立 CPU Faiss 索引并启用近似查询。

本说明按首次使用顺序组织。所有操作均从项目根目录开始；标为 `bash` 的代码在系统终端运行，标为 `sql` 的代码在 VecStor 交互界面执行，点命令也在 VecStor 界面执行。

## 1. 使用前了解三个组件

| 组件 | 作用 | 默认地址 |
| --- | --- | --- |
| 存储服务 | 保存数据库和导入源，执行持久写入与过滤 | `127.0.0.1:50051` |
| 计算服务 | 处理查询、计算向量距离、管理 ANN 索引 | `127.0.0.1:50052` |
| 交互终端 | 接收 SQL、选择数据库、展示结果 | 连接计算服务 |

终端与计算服务通过网络访问数据，不直接打开数据库文件。关闭交互终端不会停止服务，也不会删除已提交的数据。

默认业务检索使用 CPU；GPU ANN 需显式构建GPU副本并通过SQL选择，精确模式和float64重排仍在CPU。当前服务适用于可信本机或受控网络实验，尚无 TLS 和用户鉴权，不应直接暴露到公网。

## 2. 准备环境

先检查本机是否已有环境：

```bash
conda env list
```

如果已有 `vecstor`，直接激活：

```bash
conda activate vecstor
```

仅在新 Linux x86_64 机器、且尚无该环境时创建：

```bash
conda create -n vecstor --file environments/vecstor-linux-64.explicit.txt
conda activate vecstor
```

首次使用当前项目 checkout 时注册命令入口：

```bash
python -m pip install --no-deps --no-build-isolation -e .
vecstor --help
python scripts/check_environment.py --cpu-only
```

最后一条检查 CPU 组件和本地 gRPC，明确跳过 GPU。不要再用 pip 安装 `faiss-cpu` 覆盖已有 Faiss，也不要混用其他管理器更新 NumPy/CUDA。完整环境说明见 [environment.md](environment.md)。

## 3. 启动服务和交互终端

打开三个系统终端，每个终端均进入项目根目录并执行 `conda activate vecstor`。

**终端一：启动存储服务。**

```bash
vecstor serve storage --data-root data/catalog --listen 127.0.0.1:50051
```

`data/catalog` 为持久数据根目录，首次启动会创建所需目录。看到包含 `"event": "ready"` 的输出后，保持该终端运行。

**终端二：启动计算服务。**

```bash
vecstor serve compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50052
```

同样等待 ready 输出。

**终端三：打开交互界面。**

```bash
vecstor --compute 127.0.0.1:50052
```

进入后可输入 `.help`。若端口已占用，先确认是否已有对应服务；另选端口时，下一层的连接地址必须同步修改。

## 4. 第一次查询：创建一个三维向量表

以下示例使用新数据库 `guide_demo`。若已执行过且表仍存在，请换一个数据库名，不要重复建表或插入相同主键。

先在交互界面建库并选择：

```text
.open guide_demo --create
```

然后逐条执行：

```sql
CREATE TABLE items (
    id BIGINT PRIMARY KEY,
    category VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL,
    value DOUBLE NOT NULL,
    vector FLOAT[3] NOT NULL
);

INSERT INTO items VALUES
    (1, '论文', 0, 12, [0.1, 0.2, 0.3]),
    (2, '书籍', 0, 5, [0.4, 0.5, 0.6]),
    (3, '论文', 0, 30, [0.2, 0.1, 0.1]);

SELECT id, category, value FROM items ORDER BY id;

SELECT id, category, vector_distance(vector, [0,0,0]) AS distance
FROM items
WHERE category = '论文' AND value < 100
ORDER BY distance
LIMIT 10;
```

最后一条查询在满足条件的“论文”记录中寻找最近邻，应先返回 id=3，再返回 id=1。平方距离约为 0.06 和 0.14，显示值可能有 float32 表示误差。只有两条合法记录，因此即使 LIMIT 为 10，也只返回两条。

`vector_distance` 第一参数必须是列名 `vector`，第二参数才是查询数组。终端中直接写 `[0,0,0]`，无需 `:query_vector`；后者是旧脚本接口的参数形式。

### 输入与结果显示

- 按回车换行；在语句末尾写 `;`，再按回车提交。
- 字符串或注释中的分号不会提前结束语句。
- 可粘贴多条 SQL；各语句独立提交，遇错停止，剩余文本保留。它们不是一个整体事务。
- 点命令独占一行，不加分号，例如 `.tables`。
- Tab 补全，方向键浏览当前会话历史；历史不跨进程持久保存。
- Ctrl-C 清空编辑内容或取消执行；空输入时 Ctrl-D、或 `.quit` 退出。
- 表格每 100 行询问是否继续，q 停止显示；不会修改查询的 LIMIT。查看长字段或向量可用 `.mode json`。

## 5. 选择数据库与查看表

```text
.databases
.open guide_demo
.tables
.schema items
```

`.open 名称` 只选择已有库；`.open 名称 --create` 可创建并选择。选库失败时仍保留原选择。不同交互终端分别维护自己的当前库。

也可以使用 SQL：

```sql
SHOW DATABASES;
USE guide_demo;
SHOW TABLES;
DESCRIBE items;
```

`CREATE DATABASE 名称;` 不会自动切库，仍需 `USE`。库名、表名和列名以 ASCII 字母开头，后续使用字母、数字、下划线；字符串内容可以使用中文。

## 6. 修改数据和普通 SQL

继续使用上面的 `items`：

```sql
UPDATE items
SET value = value + 1, vector = [0.2, 0.3, 0.4]
WHERE id = 1;

SELECT id, value, vector FROM items WHERE id = 1;

DELETE FROM items WHERE id = 2;

SELECT category, COUNT(*) AS total, AVG(value) AS average_value
FROM items GROUP BY category ORDER BY category;
```

每条写 SQL 单独提交，成功后持久保存；校验失败则整条回滚。UPDATE/DELETE 没有 WHERE 时会影响全表。SQL 终端不支持手动 BEGIN/COMMIT，也没有交互式撤销命令。

不需要向量时可以创建普通表：

```sql
CREATE TABLE notes (id BIGINT, title VARCHAR, score DOUBLE);
INSERT INTO notes VALUES (1, '实验记录', 8.5), (2, '待评分', NULL);
SELECT id, title FROM notes WHERE score >= 8 ORDER BY id;
```

普通表支持 BOOLEAN、INTEGER、BIGINT、DOUBLE、VARCHAR 和 NULL，支持单表投影、条件、排序、LIMIT/OFFSET，以及 COUNT/SUM/AVG/MIN/MAX、GROUP BY/HAVING。当前不支持 JOIN、CTE、窗口、复杂子查询、INSERT SELECT、ALTER TABLE、任意外部文件函数或通用事务。

### 向量表的数据要求

| 字段 | 约束 |
| --- | --- |
| `id` | 非负、唯一的 int64 主键 |
| `category` | 非空字符串，不含 NUL 字符 |
| `timestamp` | UTC Unix 毫秒，int64；示例 0 表示纪元起点 |
| `value` | 有限 float64，不允许 NaN/Infinity |
| `vector` | 固定维度的有限 float32 数组，例如 `FLOAT[3]` |

向量表采用上述五字段契约，不允许 NULL。查询数组维度必须与表一致。距离为**平方 L2**，精确计算使用 float64 累加；距离相同时按 id 升序返回。向量 SQL 必须包含距离排序和 LIMIT。引擎接收已生成的向量，不负责将文本或图片转换成 embedding。

## 7. 导入较大数据集

当前支持受管理的 NPZ 导入，不支持在交互界面上传任意本地文件，也没有 CSV/Parquet 导入入口。

在**存储服务所在机器**的系统终端生成示例数据；存储服务启动后 `imports` 目录已经存在：

```bash
conda activate vecstor
vecstor generate --output data/catalog/imports/guide_vectors.npz --rows 10000 --dimension 3
```

输出文件必须不存在；再次执行请换文件名。然后在交互界面：

```text
.open guide_ann --create
.datasets
.import guide_vectors.npz items
.tables
```

```sql
SELECT COUNT(*) AS total FROM items;
SELECT id, category, value FROM items
WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10;
```

导入名称是 `.datasets` 列出的存储侧来源，不是终端机器上的路径。目标表必须不存在；导入在单事务中完成，不覆盖已有表。自有 NPZ 须遵守 [数据规范](design/data-and-query-spec.md)，推荐通过项目的 `Dataset` 校验和 `save_dataset` 写出，不要把任意 `.npz` 当作可导入文件。

## 8. 使用 ANN 近似查询

接着使用第 7 节的 `guide_ann.items`。小于 1024 行的表不训练 IVF，所以第 4 节的三行表不适合演示 ANN。

```text
.index build items
.index list
.search mode approx
.search path filter-first
.plan on
```

执行与精确模式相同的 SQL：

```sql
SELECT id, category, vector_distance(vector, [0,0,0]) AS distance
FROM items WHERE value < 1000
ORDER BY distance LIMIT 10;
```

### 选择执行路径

| 设置 | 行为 | 使用建议 |
| --- | --- | --- |
| `.search path filter-first` | 先过滤出合法 ID，再在这些 ID 中检索 | 过滤后只剩少量数据时优先尝试 |
| `.search path search-first` | 先近邻检索，再过滤候选，不足时扩展 | 过滤较宽松时尝试，并比较实测耗时 |

当前需要手动选择，尚无自动策略优化器。近似查询可能遗漏真正最近邻；返回满 K 条不表示召回率合格。候选重排使用精确距离，也不能弥补未检索到的记录。

### 常用参数

```text
.search
.search nprobe 32
.search candidates 64
.search max-candidates 4096
.search max-rounds 6
.search fallback exact
```

`nprobe` 控制初始搜索桶数，不得超过 `.index list` 对应索引的 nlist；上例 32 适用于本节默认构建的 1 万行索引。增大它通常提高覆盖率并增加计算量。`candidates` 控制初始候选量，`max-candidates` 和 `max-rounds` 限制扩展；只增加候选量未必解决漏桶问题。

索引缺失/过期或候选不足时，默认回退精确查询，并在结果中标注实际模式和原因。需要测量纯 ANN 行为时使用 `.search fallback error`，此时预算不足会明确失败，不返回不完整成功结果。小合法集合默认也可能走精确捷径。

恢复日常精确模式：

```text
.search reset
.plan off
```

这些设置只影响当前终端的向量查询，不影响普通 SQL 和其他终端。

## 9. 使用经过校准的质量配置

质量配置将某个数据快照及查询范围对应的 nprobe、候选预算下限固定下来。它需要先离线校准，再绑定实际数据库和索引身份；不是“任何数据都保证 95% 召回”的开关。

如果管理员已经提供了适用于当前数据库的配置文件，停止原计算服务，再以该文件启动；路径示意如下，应替换为实际文件：

```bash
vecstor serve compute --storage 127.0.0.1:50051 \
  --listen 127.0.0.1:50052 --quality-profiles /path/to/quality-profiles.json
```

重新连接交互终端、选择对应库，并按配置要求重建索引后：

```text
.search reset
.search mode approx
.search path filter-first
.search profiles
```

从列表选择适用的 ID，执行 `.search profile 实际配置ID`；路径必须匹配配置，可以是 filter-first 或 search-first。然后执行该配置覆盖的过滤条件、LIMIT 和向量输入范围内的查询。

显式手动预算低于配置下限会报错；配置缺失、版本或查询范围不匹配则按 fallback 设置回退或失败。关闭配置用 `.search profile off`；恢复全部默认值用 `.search reset`。

仓库的 `configs/quality/recall95-example.json` 绑定特定验收数据库，不能直接用于本说明新建的 `guide_ann`。当前校准工具主要覆盖内置合成负载，自有数据需要设计对应的校准/独立留出查询并验证后再导出配置。流程见 [实验入口](../benchmarks/p3/README.md)。已测 60 组最低平均 Recall@10=0.9702，范围与成本见 [验收报告](validation/ann-recall-optimization.md)。

## 10. 查看执行计划和耗时

```text
.timer on
.plan on
```

```sql
EXPLAIN SELECT id FROM items WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10;

EXPLAIN ANALYZE SELECT id FROM items WHERE value < 1000
ORDER BY vector_distance(vector, [0,0,0]) LIMIT 10;
```

EXPLAIN 查看计划和索引状态，不执行完整向量查询；EXPLAIN ANALYZE 实际执行一次。目前只支持向量 SELECT 的计划展示。

优先查看 `actual_mode/path`、`fallback_reason`、各轮候选数与 nprobe、RPC 次数及耗时。`quality_profile` 中的召回数字是离线证据，不是当前查询的召回率。`rpc_payload_bytes` 是 Protobuf 序列化字节，不是实际网络流量；未知统计为 null。

## 11. 写入后重建索引、停止和恢复

ANN 索引位于计算进程内存中。写入改变数据库 revision 后，后续查询不会使用旧版本索引；修改同库其他表也可能使它失效。普通精确查询仍可执行。需要继续 ANN 时：

```text
.index build items
.index list
```

已有质量配置也可能因 revision 改变而失效，重建索引不会自动更新配置，需要重新验证和绑定。已经开始的向量查询持有原版本读租约，元数据与向量保持同一快照。

结束使用时，在交互界面 `.quit`，再对计算服务和存储服务依次按 Ctrl-C。重启时使用第 3 节相同的 `--data-root`，重新 `.open` 选择库；已提交数据仍在；默认local模式的计算索引需要重建，published模式可通过RPC重新加载已发布工件，无需训练。不要在服务运行期间手工编辑 DuckDB 文件。备份可在停止服务后复制整个数据根目录；项目尚未提供在线备份命令。

写入响应丢失或超时时，留意界面的 operation_id，执行 `.status 实际操作ID`：COMMITTED 表示已提交，FAILED 表示失败，UNKNOWN 表示暂时无法确认。不要把 UNKNOWN 当作回滚后盲目重复 INSERT。

## 12. 常见问题

| 现象 | 检查与处理 |
| --- | --- |
| `vecstor: command not found` | 激活 Conda 环境，在项目根目录重新注册 editable 入口 |
| 连接失败 / UNAVAILABLE | 确认两个服务均 ready，端口和地址一致，服务仍在运行 |
| 按回车没有执行 | SQL 需要句末分号；检查引号和括号是否闭合 |
| `Expected an unqualified column` | 将 `vector_distance([0,0,0], ...)` 改为 `vector_distance(vector, [0,0,0])` |
| 查询向量维度不匹配 | 用 `.schema items` 确认维度，查询数组长度必须一致 |
| 导入源不可见 | 将符合格式的 NPZ 放在存储机器 data-root 的 imports 下，用 `.datasets` 检查 |
| 导入目标表已存在 | 换表名或换库；导入不会覆盖旧表 |
| 开启 ANN 后仍显示 exact | 看 fallback_reason，确认表规模、合法集合大小、索引是否构建及版本是否匹配 |
| FAILED_PRECONDITION | 检查索引或质量配置是否缺失、过期、范围不匹配；重建索引或选择适用配置 |
| INVALID_ARGUMENT / 质量预算冲突 | 检查 SQL、维度与参数；先 `.search reset` 清除手动低预算，再重新选模式、路径和配置 |
| RESOURCE_EXHAUSTED | 检查是候选预算、索引预算还是扫描并发限制；按实际原因调整，必要时恢复精确模式或降低并发 |
| DEADLINE_EXCEEDED | 缩小数据/查询范围，查看服务负载；写入须确认操作状态，不直接重发 |
| 重启后没有索引 | 默认local索引需重新 `.index build 表名`；published模式使用 `.index load 表名` 或启动预加载，详见[P4索引说明](p4-indexes.md) |

## 13. 管理员：验证存算隔离

Linux 上，先按前文完成建库或准备 NPZ，让数据根中至少有一个可探测文件。停止普通计算服务，再启动：

```bash
vecstor serve compute --storage 127.0.0.1:50051 \
  --listen 127.0.0.1:50052 --isolate-storage-dir data/catalog
```

计算进程将通过 Landlock 禁止读取该数据根，并验证目录和文件访问被拒绝；之后仍通过 RPC 正常查询。隔离初始化失败会停止服务。若同时加载质量配置，该文件必须位于数据根之外。

索引默认逻辑预算为 256 MiB、8 个条目，可用 `--index-memory-mib` 和 `--index-max-entries` 调整。逻辑预算不是进程 RSS 硬上限，初版也不支持超内存检索。更详细的资源、服务及查询约束见 [SQL 终端](sql-shell.md) 和 [P3 使用说明](p3-search.md)。

## 14. 在 SQL 中选择 GPU

本节使用前面至少1024行的 `guide_ann.items`（三维）。先 `.open guide_ann`，再显式构建：

```text
.index build items --device gpu
.index list
```

```sql
SELECT id, vector_distance(vector, [0,0,0]) AS distance
FROM items WHERE value < 1000
ORDER BY distance LIMIT 10
SETTINGS device='gpu', mode='approx', path='search-first';
```

SETTINGS 仅覆盖当前语句，改为device='cpu'可切换CPU。GPU不能用于exact或filter-first。默认GPU不可用时明确失败；需允许转CPU才增加 `device_fallback='cpu'`，然后核对结果的实际设备/模式。候选上限2048可能使低通过率查询失败；CPU v1质量配置不能当作GPU证据。完整参数、索引重建、显存和隔离限制见 [GPU使用说明](gpu-search.md)。

GPU改进版自动复用终端会话的查询能力信息；普通SQL和CRUD仍按原语义执行。可选的候选合并读取需启动计算服务时加 `--fused-candidates`，再用向量SQL的 `SETTINGS candidate_data_path='fused'|'separate'` 对照。默认关闭，部分负载会退化；具体配置和计划字段见 [热会话与候选合并读取](gpu-search.md#热会话与候选合并读取g1g2)。

## P4 权威工件与双节点恢复

需要重启后零训练加载或两个计算节点使用相同索引时，计算服务显式设置 `--index-source published`。数据导入后由一个节点 `.index publish 表名 --operation-id 唯一ID`，各节点分别 `.index load 表名`，用 `.index inspect 表名` 核对相同artifact_id与generation。发布成功不等于各节点已加载；写入后须重新完整构建发布，新版本缺索引时明确精确回退或失败。

GPU通过 `.index load 表名 --device gpu` 从同一CPU工件克隆。存储回收使用 `.index prune --keep-last 2` 预览，再加 `--execute --operation-id 唯一ID` 执行；`.index clear`仍只清内存。操作状态、启动预加载、预算、故障与质量配置v3的完整说明见[P4索引说明](p4-indexes.md)，验证范围见[验收记录](validation/p4-indexes.md)。
