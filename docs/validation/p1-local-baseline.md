# P1 本地精确查询验收记录

验证日期：2026-10-12。实现及首次测试于 2026-10-11 开始，完成输入边界修复后复验。规范见 [数据与查询规范](../design/data-and-query-spec.md)。

## 已实现范围

固定类型的数据模型与 NPZ 快照、独立/相关属性分布生成、SQL AST 白名单、DuckDB 参数化元数据过滤、分块精确平方 L2 Top-K、命令行生成/查询/验收，以及独立 NumPy 真值脚本。

向量使用 float32 存储和 float64 累加；同距离按 id 升序。真值脚本使用自行构建的布尔过滤掩码和全矩阵距离排序，不调用引擎的解析、过滤和块式 Top-K；手工数据测试验证具体距离、ID 和投影字段。另有故意把引擎改为空结果的测试，确认验收能够检测错误。

## 实际执行

| 检查 | 命令或输入 | 结果 |
| --- | --- | --- |
| 全套业务测试 | `conda run -n vecstor python -m pytest -q` | 133 passed，退出码 0 |
| 独立分布 | `conda run --no-capture-output -n vecstor python scripts/vecstor.py demo --output-dir results/p1-2026-10-12-independent --rows 10000 --dimension 128 --distribution independent` | 12/12 查询通过，退出码 0 |
| 相关分布 | `conda run --no-capture-output -n vecstor python scripts/vecstor.py demo --output-dir results/p1-2026-10-12-correlated --rows 10000 --dimension 128 --distribution correlated` | 12/12 查询通过，退出码 0 |
| 示例查询 | `data/p1-example.npz`，三维零向量，`value < 100`，K=5 | 命中 100 行；结果 ID 为 233、141、593、482、728 |

两组演示均使用 seed=20261011。每组覆盖通过率 0.1%、1%、10%、50%、100%，各配 K=10/100，再加两条 AND/OR 查询。报告、查询输入、真值、实际结果和数据快照均在各自输出目录。生成目录被 Git 忽略；可按命令使用新的输出目录复现，不覆盖旧结果。

版本：Python 3.12.15、DuckDB 1.5.6、SQLGlot 30.21.0、NumPy 1.26.4、pytest 9.1.1。未增加依赖或修改 Conda 环境。

## 边界与审查修复

测试覆盖：空表、空结果、少于 K、跨块并列、非连续 ID、字段投影、逻辑优先级、重复 ID、非法类型、NaN/Inf、维度错误、快照读写/清单损坏、现有文件拒绝覆盖、非法 SQL、多语句、未知函数、参数绑定和 CLI 错误退出。

独立只读审查发现的三个重要问题均已用回归测试重现并修复：

1. 有限长双精度 value 转 float64 后可能溢出：增加转换后的有限性检查。
2. 极端 SQL 指数字面量可能触发 Decimal 上下文溢出：改用不依赖上下文的符号处理，并统一捕获 Decimal 异常。
3. Unicode category 的尾部 NUL 可能被 NumPy 丢弃：明确拒绝 U+0000，避免静默改变过滤语义。

只读数组的限制已明确：本地 API 调用者不得主动使用 `setflags` 解除保护；当前不提供不可绕过的数组隔离。

## 阶段界限

- 这是 P1 技术基线验收，不是学校结题验收；实际结题日期、材料要求等仍需团队核实。
- P2 远程存储、版本发布、gRPC 业务协议尚未实现，本地文件读取是参考执行器行为。
- Faiss CPU/GPU 的环境能力在前次环境任务验证；本地业务基线使用 NumPy，不宣称实现 ANN/GPU 业务调度。
- 本次未开展性能基准、吞吐测试或跨机部署；正确性结果不能作为性能提升证据。
- 未构建可发布 Python 包；通过 `scripts/vecstor.py` 加载源码。当前目录没有可用 Git 元数据，未创建提交或 PR。
