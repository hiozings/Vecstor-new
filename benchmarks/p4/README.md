# P4 索引实验入口

所有命令在项目根目录、Conda vecstor 环境执行，输出目录必须不存在。CPU/GPU性能测量不要并行运行。

## 已发布工件的质量验证

目标计算服务须使用published模式，且已经通过RPC加载待验证CPU/GPU工件；终端管理与预加载入口由T8接入。参数来自原校准目录，本工具不重新选参数、不训练。

```bash
python benchmarks/p4/validate_artifact_quality.py \
  --calibration-dir results/recall-calibration-20261014 \
  --compute 127.0.0.1:50052 --database demo --table items \
  --device cpu --selectivity 1 --k 10 --path search-first \
  --output-dir results/p4-artifact-quality-cpu
```

GPU使用对应GPU校准目录并指定`--device gpu`，只支持search-first。实际数据ID/向量/value指纹、构建配置、Faiss版本和GPU机型须与所选冻结负载一致。至少200校准、500完整独立留出；验证实际后端、工件及revision不变、过滤/完整性和float64距离。校准平均Recall≥0.97、实际工件留出平均Recall≥0.95、零精确回退/零不完整时才导出v3质量配置。

产物为`cases.jsonl`、`report.json`和通过后才存在的`quality-profiles.json`。复用既有留出集属于工件一致性回归，不是新独立泛化证明。失败保留报告，不重调冻结参数，不仅复制旧summary/修改身份字段。

生命周期、双计算节点和重启成本工具在T9交付；整体设计与验收要求见[设计](../../docs/design/p4-index-publication.md)。
