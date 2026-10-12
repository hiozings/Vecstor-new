# VecStor 开发环境

本项目使用本机已有 Conda 管理独立的 `vecstor` 环境，避免修改 `base`。GPU 版 Faiss 包同时提供 CPU 索引和 GPU 索引，不应在同一环境额外安装覆盖它的 `faiss-cpu` pip 包。

## 使用环境

```bash
conda activate vecstor
python scripts/check_environment.py
```

不激活环境也可以运行：

```bash
conda run --no-capture-output -n vecstor python scripts/check_environment.py
```

检查脚本必须从项目根目录运行。默认要求 GPU 可访问；只有明确不验证 GPU 时才使用 `--cpu-only`，此时结果会标注跳过 GPU，不能据此声称 GPU 验证通过。

## 环境验证范围

- DuckDB 建表、插入、条件过滤与参数绑定。
- SQLGlot 的 DuckDB 方言、条件语法树和拟定向量表达式解析；这不等同于业务查询执行器已实现。
- NumPy 精确距离作为真值，验证 Faiss CPU Flat 和 IVFFlat。IVF 检查扫描全部列表，用于正确性验证，不代表近似检索性能。
- GPU Flat 和 IVFFlat 与真值比较；CPU 索引保存加载，以及 GPU 转 CPU 保存后重新加载到 GPU。
- 使用 `grpcio-tools` 实际编译临时 proto，启动仅绑定 `127.0.0.1` 的服务，通过 TCP 完成 Protobuf 消息往返。

临时数据、索引和生成的协议代码在结束后清理。检查脚本使用固定随机种子与小规模输入，不构成性能基准或项目业务测试。

## GPU 与隔离环境

已确认本机硬件为 NVIDIA GeForce RTX 4060 Laptop GPU，显存 8188 MiB，驱动 580.178.04。`nvidia-smi` 显示的 CUDA 13.0 是驱动支持能力，不代表项目必须使用 CUDA 13。

沙箱可能限制 NVIDIA 设备或本地网络监听。此时应在获得授权的沙箱外运行上述验证，不能将沙箱内设备访问失败直接归因于驱动损坏。环境安装不修改系统驱动，不安装完整 CUDA 编译工具链；使用预编译 Faiss GPU 所需的 CUDA 运行库。

## 依赖维护

`environment.yml` 固定直接依赖版本；`environments/vecstor-linux-64.explicit.txt` 包含全部 Conda 包的 URL、构建和 MD5，限定 Linux x86_64。GPU 可用性仍取决于宿主机设备和驱动。

新机器或新环境名称下优先使用精确锁定文件：

```bash
conda create -n vecstor --file environments/vecstor-linux-64.explicit.txt
```

也可使用 `conda env create -f environment.yml` 重新解析相同直接依赖；其传递依赖可能变化，不是逐包精确复现。上述新环境重建命令已提供，但本次没有为了复现测试额外创建第二套环境。

变更依赖时使用 Conda 解析，验证通过后重新导出锁定记录，并更新 `AGENTS.md` 与 `CHANGELOG.md`：

```bash
conda list -n vecstor --explicit --md5 > environments/vecstor-linux-64.explicit.txt
```

不要用 uv 或 pip 在同一环境中覆盖 Conda 管理的 NumPy、Faiss 或 CUDA 库。

## 2026-10-10 验证记录

安装前检查：Conda 25.11.1 已存在，只有 `base` 环境；项目依赖在该环境中均未安装。新建 `vecstor`，保留 `base` 和系统驱动，复用 Conda 缓存。下载中的连接中断由重试恢复，安装命令最终退出码为 0。

| 依赖 | 实际版本 |
| --- | --- |
| Python | 3.12.15 |
| DuckDB | 1.5.6 |
| SQLGlot | 30.21.0 |
| Faiss GPU 包，含 CPU 功能 | 1.12.0，CUDA 12.4 构建 |
| CUDA 版本约束 | 12.4 |
| NumPy | 1.26.4 |
| grpcio / grpcio-tools | 1.83.1 |
| protobuf | 7.35.1 |
| pytest | 9.1.1 |

实际执行结果：

- `conda run --no-capture-output -n vecstor python scripts/check_environment.py`：退出码 0；SQL、Faiss CPU、Faiss GPU、索引读写恢复和 gRPC 检查均为 PASS。
- `conda run -n vecstor python -m pip check`：退出码 0，报告 `No broken requirements found.`；此项检查 Python 包元数据，不替代 GPU 功能验证。
- `conda run -n vecstor python -m pytest --version`：退出码 0，输出 pytest 9.1.1；仅验证测试工具入口，没有业务测试用例。

不将上述结果解释为性能结论、跨机部署验证或 P1 全部完成。
