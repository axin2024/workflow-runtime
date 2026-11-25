# 运行控制

下列命令由运维人员在具备数据库访问权限的终端执行。`WORKFLOW_ACTOR` 写入操作记录。先从 `trigger` 回执或运行详情取得所属工作空间和运行号。

## 取消运行

```sh
workflow run-cancel --grace-seconds 15 demo 1 dispatch-incident-42 "上游数据待更正"
workflow run demo 1
```

取消立即封闭待运行节点，并通知持有有效租约的 Worker 停止。已完成的节点及产物仍保留。Worker 确认后结束尝试。Worker 失联时，调度器在原租约或取消宽限期中先到的时间之后标记 `cancel_timeout`。`workflow execution-sweep` 可手动执行一轮过期检查。

同一运行只接受一个取消请求。重复使用相同请求键、原因和宽限时间，会返回原记录。详情包含取消人、原因、申请时间、截止时间及完成时间。外部动作可能仍需查证，此时记录等待人工核对，已有的取消决定继续生效。

## 核对外部动作

当某个外部节点显示 `action_required`，先记录运行详情中的尝试号和 `operation_key`，再用业务单据、第三方回执或对方账本查证结果。保存核对文件，例如 `confirmed.json`。

```json
{
  "request_key": "carrier-receipt-9020",
  "epoch": 1,
  "evidence": "承运系统回执 9020，状态已收件",
  "state": "succeeded",
  "outputs": {"receipt": "9020"}
}
```

```sh
workflow run-resolve demo 1 ship confirmed.json
workflow run demo 1
```

成功结果遵循原版本的输出与分支契约。确认失败使用 `"state": "failed"`，省略输出和分支。相同核对请求可重送。运行详情并列保留原尝试、操作号、核对人和证据。一般运行按该结果推进未取消的依赖。已取消的运行完成对账后保持取消，所有已关闭的下游节点仍然关闭。
