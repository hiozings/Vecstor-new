# 交互式 SQL 终端

P2.5 已提供终端内选库、导入、普通 SQL、持久增删改和精确向量查询。数据库文件只由存储服务打开，终端与计算服务通过 gRPC 访问。

## 启动

本机 `vecstor` 命令已安装到既有 Conda 环境。新机器按环境文件安装依赖后，在项目根目录注册本项目入口：

```bash
conda activate vecstor
python -m pip install --no-deps --no-build-isolation -e .
```

这里只注册项目，不解析或覆盖 Conda 管理的依赖。三个终端分别运行：

```bash
# 终端一：管理目录模式，启动时无需指定数据库
vecstor serve storage --data-root data/catalog --listen 127.0.0.1:50051

# 终端二
vecstor serve compute --storage 127.0.0.1:50051 --listen 127.0.0.1:50052

# 终端三：进入交互界面
vecstor --compute 127.0.0.1:50052
```

终端一创建 `databases/` 和 `imports/`。客户端显示目标服务地址；第一次请求时确认连接，服务不可用会显示错误而非静默本地执行。退出终端不会关闭服务，服务可通过 Ctrl-C 停止。

## 输入方式

回车换行，输入分号后按回车提交 SQL。引号、注释或 dollar-quoted 字符串里的分号不会结束语句。可一次粘贴多条 SQL，按顺序逐条提交；遇错停止，余下文本保留在输入框。纯注释不执行。

```text
vecstor> .open demo --create
demo> SELECT id, category
   -> FROM items
   -> WHERE value >= 10;
```

上下键浏览会话历史，Tab 补全关键词、表名和列名。成功切库或修改结构后刷新补全，输入时不发网络请求。Ctrl-C 在编辑时清空，在执行时取消读取；有未提交文本时 Ctrl-D 不退出，空输入 Ctrl-D 或 `.quit` 退出。

普通表格按终端宽度显示，每 100 行询问是否继续；选择 q 关闭结果流，明确标为未完整显示，不改变 SQL 的 LIMIT。`.mode json` 可查看数组和长字段的完整值。历史仅驻留当前会话。

## 数据库与点命令

| 命令 | 用途 |
| --- | --- |
| `.help` | 当前 SQL 范围和快捷键 |
| `.databases` | 列出托管数据库 |
| `.open demo` | 选择现有库，失败时保留原选择 |
| `.open demo --create` | 不存在则创建，再选择 |
| `.datasets` | 列出 imports 中可导入的 NPZ |
| `.import sample.npz imported` | 导入当前库的新表 imported，不覆盖已有表 |
| `.tables` / `.schema [table]` | 列表或查看表结构 |
| `.status ID` | 查询写操作的 COMMITTED、FAILED 或 UNKNOWN 状态 |
| `.mode table` / `.mode json` | 设置结果格式 |
| `.timer on` / `.timer off` | 设置耗时显示 |
| `.clear` / `.quit` | 清空待提交输入或退出 |

点命令独占一行，不需要分号。也支持 `CREATE DATABASE demo;`、`USE demo;`、`SHOW DATABASES;`、`SHOW TABLES;` 和 `DESCRIBE items;`。VecStor 的建库命令按逻辑名称幂等复用已有数据库，不清空文件；CREATE DATABASE 本身不改变当前选择，USE 才选择。

选择存储在终端客户端，每条请求显式携带 database_id；一个终端切库不会影响另一个。库名、表名和列名使用 ASCII 字母开头、字母/数字/下划线组成的名称，大小写归一化，内部保留名称不可使用。字符串数据支持中文。

## 完整 SQL 示例

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
    (1, '论文;示例', 0, 12, [0.1, 0.2, 0.3]),
    (2, '书籍', 0, 5, [0.4, 0.5, 0.6]);
SELECT id, category, value FROM items WHERE value >= 5 ORDER BY id;
UPDATE items SET value = value + 1, vector = [0.2, 0.3, 0.4] WHERE id = 1;
SELECT id, category, vector_distance(vector, [0.1, 0.2, 0.3]) AS distance
FROM items WHERE value >= 5 ORDER BY distance LIMIT 5;
DELETE FROM items WHERE id = 2;
SELECT COUNT(*) AS total FROM items;
```

普通表支持 BOOLEAN、INTEGER、BIGINT、DOUBLE、VARCHAR 及 NULL；支持单表查询、别名、数值运算、条件组合、排序、LIMIT/OFFSET、COUNT/SUM/AVG/MIN/MAX、GROUP BY/HAVING。向量表保持示例中的五字段契约和主键/非空约束，维度固定，元数据和向量必须满足 P1 数值约束。

向量查询使用内联数组，平方 L2、float64 累加、距离相同按 id 升序。`SELECT *` 在新 SQL 入口返回所有实际列，包括 vector；旧 P2 Query RPC 保持原先只投影元数据的兼容语义。

暂不支持 JOIN、CTE、复杂子查询、窗口、INSERT SELECT、ALTER TABLE、用户显式事务、任意函数或外部文件 SQL。计算入口和存储入口都校验 AST；不会将任意输入直接执行。整个 SQL 缓冲区上限 65536 字符，AST 上限 4096 节点，向量维度上限 65536；不支持的结果类型或超出 int64 的整数结果会明确拒绝。

## 导入数据

在存储机器准备数据源，例如：

```bash
vecstor generate --output data/catalog/imports/sample.npz --rows 10000 --dimension 3
```

随后在 SQL 终端执行 `.datasets` 和 `.import sample.npz imported`。导入验证 NPZ 后在一个事务中建表和插入数据，不覆盖原文件。客户端本地路径不会被当成服务器路径，当前未提供文件上传或 CSV/Parquet 导入。

## 写入、快照与取消

- 每条写 SQL 是一个事务；元数据与向量在同一 DuckDB 表内修改，校验失败回滚，revision 不变。
- 同一 operation_id 绑定同一请求摘要；成功和失败状态都持久保存。提交响应丢失时可查询状态，重复提交同一请求不会重复写入，换 SQL 复用 ID 会被拒绝。
- 导入操作记录源文件摘要，但幂等键绑定源名称和目标表。已提交操作重试直接返回原结果，即使源文件已移走，不会再读取或重复导入。
- 超时/中断可能发生在提交之后，界面会查询状态；无法确认时显示 UNKNOWN，不自动重发。UNKNOWN 既不代表成功，也不代表回滚；可稍后再次 `.status`。
- 向量查询在存储端持有 MVCC 读租约，同一次扫描/向量读取固定快照。并发 UPDATE/DELETE 后旧查询仍读旧版本，下一查询读新版本。
- 每库 SQL 在专用队列中执行；超时会取消排队任务或中断正在执行的连接，并在分批读取时检查期限。读租约最多每库 16 个、最长 60 秒，后台清理到期事务，重启使旧 token 失效。

普通结果目前在存储端分块读取后物化，再分批通过 RPC 发送，尚不承诺超内存结果；向量表写入后的完整校验优先保证原型正确性，尚未优化写入吞吐。仍保留 4 MiB 消息上限和共享 4 条结果/元数据流上限，防止背压饿死向量请求。

## 存算隔离与验收

完成数据准备后，可让计算进程无法访问整个数据根目录：

```bash
vecstor serve compute --storage 127.0.0.1:50051 \
  --listen 127.0.0.1:50052 --isolate-storage-dir data/catalog
```

隔离探针要求根目录中已存在托管数据库或 NPZ 数据源。限制在计算库导入和线程启动前施加；计算进程亲自确认目录列出和文件读取被拒绝。普通启动模式不会自动改变文件权限。默认仅可信本地网络，尚无 TLS/鉴权。

```bash
python -m pytest -q
python scripts/check_shell.py --output-dir results/my-shell-check --rows 10000
```

第二条命令通过真实 PTY 驱动终端，验证双库、CRUD、导入、向量与 P1 对照、重启、写状态以及 Landlock；回收所有服务和终端进程。输出目录必须不存在，报告和终端记录在该目录下。这是正确性验收，不是性能基准。详见 [P2.5 验收记录](validation/p25-sql-shell.md)。
