# P1 本地精确混合查询规范

日期：2026-10-11。依据：用户已同意按约定与顺序实施；本文件细化数据类型、错误行为与验收条件。实现范围为本地参考执行器，P2 才引入独立存储服务与网络访问。

## 数据模型

单表名为 `items`，以下字段全部必需，不支持 NULL 或自动填充。

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| id | int64 | 非负、唯一，不要求连续或按行号排序 |
| category | Unicode 字符串 | 非空、不含 U+0000、大小写敏感；不自动裁剪空白 |
| timestamp | int64 | UTC Unix 毫秒，可为负值表示纪元以前 |
| value | float64 | 有限数值 |
| vector | float32[N,D] | D 为数据集固定的正整数；默认生成 128 维；各分量有限 |

Python API 接受数值序列并显式验证，拒绝字符串数值、布尔值、复数和有损整数转换；向量允许有限数值转为 float32，但转换溢出必须报错。查询向量采用同样转换，形状必须为 `(D,)`。空数据集合法，向量仍必须保留 `(0,D)` 形状。

数据集持有输入的独立副本，并将数组标记为只读；本地可信 API 的调用者不得主动重新启用写标志。category 拒绝 U+0000，避免 NumPy Unicode 存储丢弃尾部 NUL 而改变过滤语义。

数据文件为 `.npz`，包含 `ids/categories/timestamps/values/vectors` 和标量 JSON `manifest`，禁止 pickle。manifest 保存 `format_version=1`、行数、维度、`metric=squared_l2` 和生成参数；加载时重新验证实际数组与清单一致。此格式是 P1 本地实验快照，不是 P2 远程版本发布协议。

## 查询子集

```sql
SELECT id, category
FROM items
WHERE (category = '论文' OR category = '书籍') AND value >= 10
ORDER BY vector_distance(vector, :query_vector) ASC
LIMIT 10;
```

- 必须且只能有一条 SELECT，固定无别名的 `items` 表。
- 投影为 `id/category/timestamp/value` 的非重复列子集；`SELECT *` 展开为四个元数据字段。初版不返回 vector，也不支持投影表达式或别名。
- WHERE 可省略；支持括号、AND/OR；叶子为“列 比较运算符 字面量”，不支持交换操作数或列间比较。
- 数值列支持 `= != <> < <= > >=`；category 仅支持 `= != <>` 与字符串字面量。id/timestamp 的字面量必须为有符号 int64 范围内的整数；value 接受有限数值。
- ORDER BY 必须为 `vector_distance(vector, :query_vector)`，仅升序；可额外写 `id ASC`。无论是否显式写出，距离相同始终按 id 升序。
- LIMIT 必须是 1 至 int64 最大值之间的整数字面量；实际返回数为 `min(K, 合法记录数)`。
- 不支持 JOIN、子查询、CTE、UNION、聚合、DISTINCT、OFFSET、GROUP BY、HAVING、窗口、写入语句、外部文件函数、任意函数及其他占位参数。
- 表名、字段名和函数名不区分大小写；不支持表前缀和别名；字符串内容区分大小写。
- SQLGlot 按 DuckDB 方言生成 AST，采用结构白名单校验，再构建仅包含元数据读取的 SQL；字面量绑定为参数，不把原始 SQL 直接交给 DuckDB。

## 精确结果

向量首先转为 float32，计算时转为 float64，以直接差值平方和计算 squared L2。完整计算所有满足条件的记录；按 `(distance, id)` 升序排序。距离的精确含义是全量穷举和规定的浮点计算，不承诺实数算术或 CPU/GPU 浮点位级一致。

返回 JSON 对象：`rows` 保留投影字段，`distances` 与 rows 一一对应，`matched_count` 表示过滤命中总数，`metric=squared_l2`、`mode=exact`，并给出执行路径与数据维度。距离不占用用户字段名。

引擎使用分块 NumPy 全量距离计算与有界 Top-K 合并；基准脚本使用独立 NumPy 全矩阵距离、独立布尔过滤和 lexsort 生成真值，不调用引擎的解析、过滤或排序函数。手工数据测试同时验证距离、ID 和投影，防止两个实现共享错误。

Faiss CPU/GPU 在环境检查中已验证；本阶段不把有限候选的 ANN 输出当真值，不为每次过滤重建 Faiss 索引。将本地精确执行器保留为 P3 的独立对照，不提前实现 GPU 调度。

## 数据与实验

使用固定 seed 和 NumPy Generator；默认 10000 行、128 维。支持 `independent`（属性秩随机排列）和 `correlated`（属性秩随向量首分量变化）两种合成分布。value 为 0 到 N-1 的无重复秩，使用 `value < floor(N*f)` 精确控制命中数；category 为秩分桶，timestamp 为固定起点加秩秒数。

基础负载覆盖通过率 0.1%、1%、10%、50%、100% 与 K=10/100，另有 AND/OR 组合；查询向量由另一随机流生成。小 N 时报告实际命中数，不声称名义通过率精确成立。生成时不覆盖已有输出文件或目录。

## 验证与限制

验收包括手工精确结果、空表/空结果、少于 K、边界并列、非连续 ID、重复 ID、维度错误、NaN/Inf、非法 SQL、注入字面量、可重复生成、快照损坏与 CLI 错误退出。全套 pytest 与两个分布的 10000 行演示必须通过。

当前只用于可信本地开发；不提供资源隔离、多用户 SQL 服务或恶意超大文件防护。限制 SQL 长度与 AST 节点数以避免明显失控，仍不应直接暴露到公网。元数据和向量放在本进程中，不宣称已实现存算分离。

参考现有调研中的 [pgvector 过滤语义](https://github.com/pgvector/pgvector#filtering) 与 [DuckDB VSS](https://github.com/duckdb/duckdb-vss) 的过滤后检索路径；直接使用 SQLGlot/DuckDB 公共 API，不复制上游源码。参考关系见 `docs/research/github-reference-survey.md`。
