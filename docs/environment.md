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

2026-10-18 迁移后确认本机硬件仍为 NVIDIA GeForce RTX 4060 Laptop GPU，显存 8188 MiB，驱动为 615.78.08（旧系统为 580.178.04）。项目继续使用锁定的 CUDA 12.4 运行库；`nvidia-smi` 的 CUDA 版本表示驱动支持能力，不是项目运行库版本。

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

不要用 uv 或 pip 在同一环境中覆盖 Conda 管理的 NumPy、Faiss 或 CUDA 库。仅本项目使用 `python -m pip install --no-deps --no-build-isolation -e .` 注册 console-script，项目本身不在 Conda 显式包清单中，复现后需单独注册入口。

## P2.5 终端依赖更新

2026-10-12 在先检查已有环境后，以 `--freeze-installed` 方式新增 prompt-toolkit 3.0.53 与 Rich 15.0.0；预解析确认没有替换原有包。原已安装的 setuptools 84.0.0、wheel 0.48.0 纳入直接依赖声明，以复现项目入口构建。

`environment.yml` 与 Linux 精确包锁定文件已更新。依赖一致性检查通过；`vecstor --help` 可运行。项目版本为 `0.2.5.dev0`，入口链接当前源码，未发布到包仓库。

## 2026-10-10 验证记录

以下为原系统历史记录。新系统结果见下方迁移检查，不能将历史入口安装状态视为新环境已安装。

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

## 2026-10-18 系统迁移检查

系统为 CachyOS，Linux 7.2.9-2-cachyos x86_64、glibc 2.44；Conda 26.7.2 来自 Miniforge，`vecstor` 环境存在。78 个 Conda 包的版本、构建及包 MD5 记录与精确锁定文件一致。`conda doctor -n vecstor --json` 的实际文本报告确认无缺失/改动包文件、环境一致且支持文件锁；本次没有使用 `--fix`。Python 和全部直接依赖版本与 `environment.yml` 一致，`pip check` 无依赖冲突。

在沙箱外实际执行环境检查，SQL、Faiss CPU/GPU Flat/IVFFlat、索引保存/加载/GPU恢复、临时Protobuf编译和localhost TCP gRPC全部通过。Faiss、NumPy、DuckDB、gRPC、SSL扩展的动态链接无缺失库。Landlock ABI为10；GPU在沙箱内不可访问、沙箱外正常，不判为驱动故障。Conda环境和项目目录在宿主机可写。

**迁移遗漏尚未修复：本项目没有注册进新环境。** `pip show vecstor` 找不到包，`conda run -n vecstor vecstor --help` 找不到命令，`python -m vecstor` 无法导入。原样完整回归为551通过、81失败（93.35秒），失败涉及构建/GPU/验证子进程无法导入`vecstor`。

仅对测试进程临时设置 `PYTHONPATH` 指向当前项目的 `src`，并设置 `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 VECSTOR_REQUIRE_GPU=1` 后，当前工作区完整回归 **632通过，175.54秒，无跳过**。这说明源码路径可解析时依赖与业务测试正常，不代表console-script已经修复，也不代表未完成的P4阶段已经验收。

另执行1万行×128维P1演示，12个正确性用例通过；在临时目录重新生成协议，四个生成模块与当前源码逐字节一致，未改写仓库生成文件。

从项目根目录执行以下命令即可补做项目注册（**本次检查未执行安装**）：

```bash
conda run --no-capture-output -n vecstor python -m pip install --no-deps --no-build-isolation -e .
conda run --no-capture-output -n vecstor vecstor --help
```

注册完成后应在不额外指定`PYTHONPATH`的情况下复跑完整业务测试。本次没有安装/升级依赖、修改驱动或业务代码。完整报告、原样失败日志、源码路径复测日志和文件摘要保存在 `results/environment-migration-20261018-f1l31ij8/`（Git忽略），不属于性能实验或新环境独立重建验收。

### 同日补注册与无临时路径复验

按用户授权执行 `python -m pip install --no-deps --no-build-isolation -e .`，仅注册本项目 `vecstor 0.2.5.dev0`，没有安装额外依赖。清除PYTHONPATH后，`vecstor --help`、模块导入和pip check通过；强制真实GPU的完整回归 **632通过，180.97秒，无跳过**。原迁移遗漏已修复；保留上面的初检失败记录，不将条件复测替换为无条件成功。证据为同一results目录的registration-suite.log和registration-recheck.json。

## 2026-11-04 CompShare容器部署（验收中）

云端使用既有Conda建立独立`vecstor`环境，不更新base或驱动。起始源码为功能分支`ce5ae23`的已提交归档，不包含本机无关修改或数据；后续非零GPU设备修复单独记录源码哈希。云端实际硬件为EPYC 7B13、20个逻辑CPU可用、90GiB内存、RTX4090约48GiB显存、驱动610.57.04，Linux 6.12.101、Landlock ABI6，100GB容器文件系统。

公网下载CUDA大包较慢，保留原任务日志并停止该部署任务，改为上传本机缓存的78个安装包。上传前和云端解包后均按原锁文件逐包核对MD5；使用`file://...#md5`的临时explicit清单离线创建环境，再以`pip install --no-deps --no-build-isolation -e .`注册项目。下载URL不同不改变包内容；安装后按包文件名、版本构建和MD5对照原锁文件，78包一致，pip check通过。临时离线清单不替换仓库的原始锁文件。

首次GPU组件检查通过，但真实GPU隔离验收失败：容器暴露`/dev/nvidia6`，既有规则硬编码`/dev/nvidia0`，导致隔离后的GPU worker报`gpu_unavailable`。修复仅枚举可见的编号NVIDIA字符设备，保留原控制设备，拒绝普通文件、软链接及无关名称；不放开整个`/dev`写权限，CUDA逻辑设备仍为0。三项新回归先失败，再与既有隔离测试合计4项通过；云端1万行真实GPU、PTY、计算/worker存储拒读及恢复验收已通过。完整无PYTHONPATH回归正在运行，尚未据此宣布云端业务验收完成。

证据分别保留在云端`results/cloud-deployment/`（初次环境与隔离失败）、`results/cloud-validation-gpu-node/`（修复后环境/完整回归）和`results/cloud-gpu-isolation-fixed/`（真实GPU隔离）。所有长任务通过CompShare durable job运行，可在SSH断开后继续；先查任务状态再恢复，避免重复提交。具体实例ID和操作清单存于本机Git忽略的`results/`云端部署记录中。

迁移后成本模型必须重新校准，不能把本机模型的硬件/网络指纹改名冒充云端证据。为修复运行时代码而结束的本机旧版评估，保留75个完整计时配置/24150条查询及前4组完整auto留出；未完成部分明确中断，不将其拼接到新源码云端矩阵。

### 2026-11-05回归失败与独立复现

实例重启中断首次完整回归后，在新目录重跑。真实GPU与隔离再次通过；完整回归结果为940通过、1失败、5错误，耗时1366.43秒。6项失败均发生在默认15秒导入期限：`test_catalog.py::test_import_batches_preserve_large_ids_and_float32_values`，以及`test_index_catalog.py`的同操作重放、其他表写入后提交（两个参数）、同名表重建、结束租约释放发布槽位的fixture。

没有修改源码或超时限制，单独重跑上述两个测试文件，32项全部通过（25.63秒），对应导入约1.8秒。检查时没有遗留业务进程，也未观察到CPU cgroup限流；该证据不足以确定首次失败是资源波动还是测试间影响。原失败日志与JUnit保留在`results/cloud-validation-20261105/`，独立复现为`import-diagnostic.xml`。随后在`results/cloud-validation-retry-20261105/`按相同参数完整复跑，零失败、零错误、零跳过才允许继续pilot、校准和代表性主矩阵。当前尚不能宣称云端完整回归通过。
