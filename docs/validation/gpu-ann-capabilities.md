# GPU ANN 组件能力验证

2026-10-15，RTX 4060 Laptop 8188 MiB、驱动580.178.04，复用Conda vecstor/Faiss1.12.0/CUDA12.4，无依赖安装。

`scripts/check_environment.py` SQL、CPU/GPU Faiss、gRPC检查通过。`scripts/check_gpu_ann.py --output-dir results/gpu-capabilities-20261015` 在独立子进程运行：

| 探测 | 实测 |
| --- | --- |
| CPU→GPU IVFFlat克隆、转回CPU | 通过 |
| float32检索、ID大于2^53 | 通过，ID无精度损失 |
| 请求级nprobe切换再恢复 | 通过 |
| 候选2048/2049 | 2048通过，2049拒绝 |
| nprobe2048/2049 | 2048通过，2049拒绝 |
| IDSelectorBatch | 参数被接受但过滤结果不正确（选择器被忽略）；不启用GPU filter-first |

每项独立进程退出后CUDA context回收，探测后设备空闲7711 MiB；此数字是设备整体快照，不是单请求峰值。GPU调用以同步host时间测量，不声称kernel-only。业务层必须在native调用前限制L/nprobe≤2048，使用单owner资源，保留CPU float64重排。本记录为组件验证，尚非GPU业务交付。

CPU-only运行明确标记skipped/gpu_verified=false。原始子进程日志保留在上述results目录。
