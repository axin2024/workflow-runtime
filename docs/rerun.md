# 部分重跑与运行报告

源运行结束为 `failed` 或 `canceled`，且所有节点已结算后，可选一个或多个失败节点重跑。命令中的 `42` 是源运行号，`inspect` 是失败节点键。多个节点用逗号分隔。

```sh
workflow run-rerun-preview demo 42 inspect
workflow run-rerun demo 42 repair-inspect-001 inspect
workflow run-export demo 42 > source-run.json
workflow run-export demo 43 > derived-run.json
```

将 `43` 换成确认返回的 `run_id`。同一源运行重复提交原请求键和原选集会返回同一个 `run_id`。同键改选集会报冲突。预览和报告以工作空间为范围。

选中节点及其传递后继按原 DAG 版本重新判断依赖。其他节点复用原状态和产物。工作流逻辑时刻和输入沿用源运行，新运行有独立的节点尝试和租约。预览列出 DAG 版本、定义摘要、输入摘要，以及每个节点的初始状态、执行方式和可用的来源产物。

业务操作键跨重跑保持稳定，便于幂等处理器复用下游去重标识。受影响节点已尝试过非幂等外部动作时，系统会拒绝重跑并返回节点键。仍有外部结果待核对时，先通过运行控制结算。 [运行控制](run-control.md)

`workflow run` 与 `workflow run-export` 提供以下字段。

| 字段 | 含义 |
| --- | --- |
| `rerun.plan`、`recent_reruns` | 子运行的来源与预览快照，以及父运行最近登记的 64 次子运行。 |
| `output_artifact` | 实际产生输出的运行号、节点键、尝试代数及 SHA-256。多次派生仍可定位最初的产出。 |
| `dependency_wait_ms` | 从登记运行到全部前置条件判定完成所用的毫秒数。根节点为 0，仍在等待或沿用旧状态的节点省略该字段。 |
| `attempts[].queue_wait_ms` | 从本次尝试进入可领取队列到 Worker 领取的时间。 |
| `attempts[].execution_ms` | 从领取到该次尝试结算的时间，单位为毫秒。 |
| `attempts[].log_excerpt`、`log_sha256` | Worker 提交的日志片段及摘要。 |

导出格式为 `workflow-runtime/run-report-v1`，含生成时间、操作者及完整运行详情。报告包含原始输入、输出和日志片段，按业务数据的访问规则保存和共享。
