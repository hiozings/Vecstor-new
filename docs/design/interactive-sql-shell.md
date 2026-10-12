# 交互式 SQL 终端与可写数据库设计

日期：2026-10-12。状态：已获用户批准并于 2026-10-12 完成 P2.5 技术验收。对应用户要求：终端交互、多行分号结束、界面选择数据/数据库、基础增删查改与向量 SQL、参考 DuckDB CLI。

建议作为 P2.5，安排在 P2 与 P3 之间。它包含终端、协议和存储模型三部分；将原定 P4 的部分写入一致性基础提前，但不提前实现 ANN 索引更新或自动优化。

## 改造前实现的限制

- `src/vecstor/cli.py` 仅接受单次命令参数，没有交互会话。
- `src/vecstor/query/parser.py` 仅接受固定 items 表、带向量 ORDER BY 的 SELECT，不支持普通 SELECT 或写入。
- `src/vecstor/storage/service.py` 启动时读取一个 NPZ，向量放在只读 NumPy 数组中，DuckDB 元数据表在内存中。
- `proto/vecstor/rpc/services.proto` 的 Row 固定为四个元数据字段，无法表达 COUNT、任意投影、NULL 或多种表结构。
- P2 的 SHA-256 固定版本适合只读快照，写入后不能只替换数组并沿用原版本，否则多次 RPC 可能读取不同时刻的数据。

因此不能只增加 input 循环或把任意 SQL 直接交给现有连接。

## 方案比较

| 方案 | 优点 | 代价或限制 |
| --- | --- | --- |
| 继续修改 NPZ 并逐次重写 | 初期改动小 | 数据持久化、原子写入与并发版本需自行实现，通用 SQL 很难衔接 |
| 存储端使用持久化 DuckDB，计算端保留向量执行，推荐 | 复用 SQL、事务、约束和恢复；兼顾现有存算分离 | 需要数据库目录、通用结果协议和跨 RPC 读快照 |
| 直接让 DuckDB CLI 打开数据库文件 | 可以立即获得完整 SQL 终端 | 绕开计算/gRPC 层，不能验证原架构，也无法直接使用自定义远程向量路径 |

选择第二种。普通 SQL 与写入在存储侧由 DuckDB 执行，计算侧负责入口校验、路由和向量计算。这扩展了现有“过滤下推”的职责，但不宣称所有关系算子都已在独立计算节点执行。

## 用户交互

安装项目入口后，用户激活 Conda 环境，使用 `vecstor` 命令，无需反复输入 Python 命令。拟提供：

```text
vecstor serve storage --data-root ./data/catalog
vecstor serve compute --storage 127.0.0.1:50051
vecstor --compute 127.0.0.1:50052
```

前三行分别用于不同终端；客户端不在后台静默启动或关闭服务。旧的脚本和非交互命令保留，便于实验复现。

进入界面后显示连接、当前数据库和帮助入口：

```text
VecStor SQL Shell
Connected to 127.0.0.1:50052
No database selected. Use .databases or .open <name>.

vecstor> .open demo --create
Database demo selected.
demo> SELECT id, category
   -> FROM items
   -> WHERE value >= 10
   -> ORDER BY vector_distance(vector, [0.1, 0.2, 0.3])
   -> LIMIT 5;
```

上述命令与语法已实现，实际操作见 `docs/sql-shell.md`。

### 输入和分句规则

1. 回车默认增加一行。输入分号后按回车提交已结束的语句，未结束部分保留为续行；采用 DuckDB 常见操作习惯，不在用户刚键入分号时立即执行。
2. 只有普通词法状态中的分号才能结束 SQL。字符串、带双引号的标识符、`--` 注释、可嵌套 `/* */` 注释及 DuckDB dollar-quoted 字符串中的分号均视为内容。处理转义引号和跨行状态。
3. 粘贴多条 SQL 时顺序执行；中途出错就停止，显示已完成数量，保留余下语句待用户重新提交。默认每条语句独立提交，不暗示整批原子性。
4. 纯注释和空语句不发送 RPC。未闭合引号/注释继续续行，设置缓冲区大小上限并给出可恢复错误。
5. UI 分句器只识别语句边界，不承担 SQL 解析。完整语句仍交给 SQLGlot AST 和服务端校验，禁止以 split(';') 或正则替代 SQL 解析。
6. 空闲 Ctrl-C 清空输入；执行中 Ctrl-C 取消读取请求。写入取消/超时不能直接宣称回滚，应查询操作状态。空缓冲 Ctrl-D 退出；有未提交内容时提示先清空，避免误丢输入。

### 界面命令

| 命令 | 行为 |
| --- | --- |
| `.help` | 显示 SQL 范围、命令及示例 |
| `.databases` | 列出存储端托管数据库 |
| `.open <name>` | 选择已存在数据库；不存在时报错，不悄悄建库 |
| `.open <name> --create` | 显式创建并选择数据库 |
| `.datasets` | 列出存储端 imports 目录中的可导入 NPZ 数据源 |
| `.import <source_id> <table>` | 把托管 NPZ 导入当前数据库的新表，保留原文件 |
| `.tables` / `.schema [table]` | 显示表和结构 |
| `.status <operation_id>` | 查询写操作是否提交、失败或状态未知 |
| `.mode table\|json` / `.timer on\|off` | 输出格式与耗时开关 |
| `.clear` / `.quit` | 清空缓冲、退出客户端 |

点命令不需要分号，只在独立命令行且不位于 SQL 字符串/注释内部时识别；`.clear` 可清除待输入 SQL。数据库名、表名和数据源名使用明确参数语法，显示具体错误。

提供语法高亮、上下键历史、表/列补全、查询耗时、影响行数和表格输出。大结果分批显示，可停止继续读取；终端展示截断不改变 SQL LIMIT 或服务端结果语义。历史先保存在会话内，持久历史作为可选设置。

## 数据库选择与导入

存储服务改为启动时指定 `--data-root`，管理多个持久化数据库；启动时不必选定业务数据。

```text
data/catalog/
├── databases/            # 托管 DuckDB 文件
└── imports/              # 可从界面选择的 NPZ 数据源
```

客户端只使用逻辑名称和服务端返回的 database_id，不直接打开服务端文件。`.open demo` 使该终端的后续请求路由到 demo，存储端按请求选择对应数据库。禁止把“切库”实现成一个覆盖所有客户端的全局 current_database。

每条请求显式携带 database_id；执行中的查询在开始时固定目标库，其他会话切库不会改变它。选择失败保留原选择；未选库时允许帮助、列库和建库，访问表的语句提示先选库。

首版导入支持已有 NPZ，通过 `.datasets` 选择存储端托管的数据源。源文件经过现有格式/维度/有限值检查后，在一个事务中导入新表；失败不留下半张表，目标已存在则拒绝覆盖。用户本机任意路径不自动当作服务端路径；客户端上传和 CSV/Parquet 导入列为后续扩展，避免共享文件假设。

文件路径解析、符号链接和数据库名必须限制在托管目录内；数据库文件由单个存储进程读写，计算服务仍在隔离测试中被禁止直接访问整个 data-root。

## SQL 首版范围

“支持各类 SQL”落实为明确可验收的常用集合，不等同于立即兼容全部 DuckDB SQL。第一版扩展普通关系 SQL，但向量联合查询仍保持单表边界。

| 类别 | 第一版支持 |
| --- | --- |
| 数据库 | `CREATE DATABASE name;`、`USE name;`、`SHOW DATABASES;`，由 VecStor 路由处理 |
| 表管理 | `CREATE TABLE`、`DROP TABLE`、`SHOW TABLES`、`DESCRIBE` |
| 插入 | `INSERT INTO ... VALUES (...)`，支持多行和明确列清单 |
| 更新 | `UPDATE ... SET ... WHERE ...`，允许数值算术表达式与列赋值 |
| 删除 | `DELETE FROM ... WHERE ...`，无 WHERE 为整表删除，按 SQL 原义执行 |
| 普通查询 | 单表投影、别名、比较/AND/OR、NULL 判断、排序、LIMIT/OFFSET、COUNT/SUM/AVG/MIN/MAX 和 GROUP BY/HAVING |
| 向量写入 | INSERT/UPDATE 中的数组字面量，维度和分量校验 |
| 向量查询 | `ORDER BY vector_distance(vector, [常量向量]) LIMIT K`，与元数据条件组合；距离别名投影及 id 并列排序 |

普通表先支持 BOOLEAN、INTEGER、BIGINT、DOUBLE、VARCHAR 等基础类型及 NULL；向量表保留 P1 五字段契约，表名可自定义，id 为非负唯一主键，固定维度 `vector FLOAT[D]` 且无 NULL。不在这一步支持任意嵌套类型、任意向量列布局或更换向量表的关键约束。

普通 SELECT * 返回数据库实际列；为避免终端输出过宽，可在展示层缩略向量。旧 Query RPC 的 SELECT * 元数据语义保持不变，新 ExecuteSQL 语义明确区分。

暂不包含：JOIN、CTE、复杂子查询、窗口函数、INSERT SELECT、ALTER TABLE、存储过程、跨库查询、用户显式 BEGIN/COMMIT/ROLLBACK。单语句事务必须实现；多语句事务后续单独设计会话生命周期。INSTALL/LOAD、任意 PRAGMA、ATTACH 外部路径、COPY 文件和外部读取函数也不开放给 SQL 入口。

### 示例工作流

```sql
CREATE DATABASE demo;
USE demo;

CREATE TABLE items (
    id BIGINT PRIMARY KEY,
    category VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL,
    value DOUBLE NOT NULL,
    vector FLOAT[3] NOT NULL
);

INSERT INTO items VALUES
    (1, '论文', 0, 12.5, [0.1, 0.2, 0.3]),
    (2, '书籍', 0, 7.0, [0.4, 0.5, 0.6]);

SELECT id, category, value FROM items WHERE value > 5 ORDER BY id;
UPDATE items SET value = value + 1 WHERE id = 1;
UPDATE items SET vector = [0.2, 0.3, 0.4] WHERE id = 1;

SELECT id, category,
       vector_distance(vector, [0.1, 0.2, 0.3]) AS distance
FROM items
WHERE value >= 5
ORDER BY distance
LIMIT 5;

DELETE FROM items WHERE id = 2;
SELECT COUNT(*) AS total FROM items;
```

## 执行与一致性

```text
交互终端
  输入缓冲、历史、分句、渲染、当前 database_id
        ↓ gRPC
计算服务
  SQL 分类与校验、关系 SQL 路由、向量执行
        ↓ gRPC
存储服务
  数据库目录、持久化 DuckDB、写事务、查询读租约
```

通用 SQL 分派器识别数据库命令、DDL、DML、普通 SELECT、向量 SELECT；只允许列出的 AST 结构和函数。存储端再次校验，执行受控 SQL/绑定参数，不把接受一类顶层语句误当作允许它嵌套任意文件函数。

### 持久化写入

存储进程为每个数据库管理 SQL 执行队列和专用连接，串行调度该库的 SQL 操作，避免无保护地跨线程共享连接。不同查询读租约持有各自事务连接，由该库执行队列操作。

每次写入在同一 DuckDB 事务中：校验 → 执行 → 验证向量表约束 → 更新持久 revision → 记录 operation_id/请求摘要/结果 → 提交。失败全部回滚，revision 不变。元数据和向量在同一权威表内更新，不保留一份容易失效的可写 NumPy 镜像。

同一 operation_id 重试只返回已记录结果，若携带不同 SQL 则拒绝。终端超时或取消写入后通过 `.status` 查询；无法确认时显示“结果未知”，不自动生成新 ID 重发。提交日志和 SQL 变更必须处于同一事务，服务重启后仍能确认已提交结果。

### 跨 RPC 的读快照

向量查询通过 BeginRead 获得 read_token，并在存储端保持一个只读事务连接，固定 database_id、table_id、revision、维度和 schema_version。ScanMetadata/FetchVectors 使用同一 read_token，在 DuckDB MVCC 快照中读取，直到 EndRead。

表或数据库切换、其他会话写入不能改变进行中的查询。写入成功后的下一条查询获得新快照，能看到新增、修改或删除；读取过程中不能只逐 RPC 检查“最新 revision”而混用数据。

读租约有 deadline、上限和到期清理；RPC 取消/异常时释放。服务重启使旧 token 失效，明确报错。未来索引/缓存键包含 `(database_id, table_id, revision, schema_version)`；本次仍走精确查询，避免为尚不存在的 ANN 实现更新机制。

## 协议演进

保留 P2 的兼容接口与独立回归测试，新增 v2 服务/消息，不复用旧字段编号改变含义。

| 接口 | 主要字段/结果 |
| --- | --- |
| ListDatabases / OpenDatabase / CreateDatabase | 逻辑名称、是否创建、稳定 database_id |
| ListDatasets / ImportDataset | source_id、database_id、table、operation_id |
| ExecuteSQL | database_id、SQL、operation_id；返回结果流或写入完成结果 |
| GetOperationStatus | database_id、operation_id、请求摘要；提交/失败/未知 |
| BeginRead / EndRead | database_id、table_id、read_token、revision、schema_version、过期时间 |
| ScanMetadata / FetchVectors v2 | read_token、批次位置/ID；继承完整性检查 |

ExecuteSQL 的结果流首帧为列名/类型，后续为行批次，末帧为完成状态、影响行数、耗时与可选执行计划。Cell 使用明确的 null/bool/int64/double/string/向量数组类型，不用 Protobuf Struct 将 BIGINT 转成 double；首版不支持的数据库类型明确拒绝。

整个语句保持总 deadline，继承 P2 批次/消息上限及扫描名额限制。读取中断释放结果游标和读租约，写结果丢失遵循 operation_id 规则。

## 技术与参考

已新增 `prompt_toolkit` 处理多行编辑、快捷键、历史与补全，`rich` 处理表格与终端宽度；已通过 Conda 安装 prompt-toolkit 3.0.53、Rich 15.0.0 并更新锁定文件，原有数值依赖不变。

`pyproject.toml` 增加项目元数据与 `vecstor` console-script。仅安装本项目入口，不允许安装过程重新解析或覆盖已锁定的 Faiss/NumPy/CUDA 依赖。构建工具先检查再按需补齐。

参考 DuckDB 的交互习惯，不复制或嵌入其 CLI 进程；保留自有 RPC 路由。

- [DuckDB CLI](https://duckdb.org/docs/stable/clients/cli/overview)：分号提交、多行输入和结果显示。
- [DuckDB 点命令](https://duckdb.org/docs/stable/clients/cli/dot_commands)：open、tables、schema、mode、timer 的习惯。
- [DuckDB Python API](https://duckdb.org/docs/stable/clients/python/dbapi)：持久化连接与事务。
- [DuckDB 并发](https://duckdb.org/docs/stable/connect/concurrency)：由一个存储进程拥有数据库读写。

## 验收原则

采用真实终端 PTY 测试，而不只模拟 input。覆盖中文、多行粘贴、引号/注释内分号、续行、Ctrl-C/D、补全与宽表输出。功能测试覆盖数据库选择隔离、持久 CRUD、错误回滚、写入结果未知后的状态查询、并发读写快照一致性。

最终演示必须从交互终端完成：选库/建库 → 导入/建表 → INSERT → 普通 SELECT → UPDATE 向量与属性 → 向量查询 → DELETE → 重启存储 → 复查持久结果。计算进程始终无法读取 data-root；原 P1/P2 回归继续通过。

## 实施补充

- 创建数据库按逻辑名称幂等复用已有库，不清空文件；当前库只有 .open/USE 成功后改变。
- 失败操作的摘要也持久保存；导入的操作摘要绑定源名与目标表，文件内容 SHA-256 另记为来源证据，成功后重试不重新打开源文件。
- 普通 SQL 结果在存储端分块读取后物化，再以类型化批次传输；不是超内存执行。
- 完成情况与审查修复见 `docs/validation/p25-sql-shell.md`，后续 ANN/缓存/索引维护不在本阶段完成范围。
