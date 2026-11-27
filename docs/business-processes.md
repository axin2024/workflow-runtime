# 持久化业务流程

业务流程和 DAG 共用工作空间、数据库、管理站与调度服务，各自有独立的定义和实例。作者可在「流程定义」查看步骤图、业务出口和诊断，办理人在「人工待办」处理任务，运营员从「流程实例」追查收件箱、子流程、补偿和迁移。

## 采购流程演练

先按[自托管快速开始](quickstart.md)配置 Go、MySQL 和 `WORKFLOW_MYSQL_DSN`。以下命令用独立的演练工作空间，要求执行用户有数据库运维权限。可用发行包中的 `workflow` 替换 `go run ./cmd/workflow`。

~~~sh
go run ./cmd/workflow migrate
go run ./cmd/workflow workspace-create procurement
go run ./cmd/workflow process-check procurement examples/purchasing-process.json
go run ./cmd/workflow process-publish procurement purchasing 0 examples/purchasing-process.json
go run ./cmd/workflow console-grant procurement purchase-operator operator
go run ./cmd/workflow console-grant procurement finance-user viewer
go run ./cmd/workflow console-grant procurement buyer-user viewer
go run ./cmd/workflow process-duty-grant procurement finance-user finance
go run ./cmd/workflow process-duty-grant procurement buyer-user purchasing
go run ./cmd/workflow process-connector-grant procurement purchasing warehouse-adapter
go run ./cmd/workflow process-start procurement purchasing 1 PO-7 examples/purchasing-inputs.json
~~~

在不同终端启动三个进程。管理站登录凭据和连接器令牌由对应授权命令一次返回，请通过本机环境或密钥管理设施传给对应进程。

~~~sh
go run ./cmd/workflow console --listen 127.0.0.1:8081
go run ./cmd/workflow worker-api --listen 127.0.0.1:8080
go run ./cmd/workflow scheduler
~~~

1. 用财务账号从「人工待办」认领并同意冻结额度，用采购账号办理订货批准。应先让连接器回报冻结额度成功，采购待办才会出现。
2. 可在审批尚未完成时用仓库令牌投递收货信号。等待点建立后用原事件号读取消费回执。
3. 若需演练补偿，请另起一单，等订货成功后、收货完成前取消。取消由有操作权限的账号发起。应先看到撤订被领取并确认，再看到解冻可领取。
4. 运营员从实例页导出 JSON，或运行 `workflow process-show procurement RUN_ID`。换用 `process-start` 返回的实际实例编号。较早的审计事件可用详情返回的分页游标继续读。

## 信号接口

业务连接器令牌限定到工作空间和流程。事件号、幂等键、关联键和事件内容在重送时沿用原值。用实际 `RUN_ID` 和本地环境中的 `WORKFLOW_SIGNAL_TOKEN` 发送。

~~~sh
curl -i -X POST "http://127.0.0.1:8080/process-runs/${RUN_ID}/signals" \
  -H "Authorization: Bearer ${WORKFLOW_SIGNAL_TOKEN}" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: receiving-E3' \
  --data '{"event_id":"E3","correlation_key":"PO-7","kind":"goods_received","payload":{"qty":3}}'
~~~

等待匹配会返回 202，`Location` 给出 `GET /process-runs/{id}/signals/{event_id}` 地址。用同一授权读回 `receipt_id`、`outcome`、`consumed_by` 和时间字段。提前信号最多保留七天。重送相同请求取得原回执，改动同号事件的内容得到 409，字段错误得到 422，其他工作空间或流程的对象得到 404。

审批表单携带当前任务修订，认领、委派或截止状态变化后要刷新表单。服务端保留获胜和迟到的输入，实例详情可查看原请求键、任务编号、信号号或截止时刻。

## 操作连接器与补偿

每个操作签发独立令牌。例如 `workflow process-connector-grant procurement purchasing freeze-adapter freeze-credit`。为采购样例的 `place-order`、`withdraw-order`、`release-credit` 分别授予对应连接器。

| 接口 | 请求与返回 |
| --- | --- |
| `POST /process-actions/claim` | 请求 `{"lease_seconds":60}`。200 返回动作编号、操作键、租约代数和业务输入，204 表示该操作暂无可领动作。 |
| `POST /process-actions/{id}/receipt` | 通过 `Idempotency-Key` 标识回报请求。请求包含 `lease_epoch`、`outcome`、证据对象 `evidence`，成功时附 `external_receipt`。 |

连接器应把领取响应的 `operation_key` 作为下游业务幂等键使用。回报状态支持 `succeeded`、`failed`、`unknown`。下游超时或失去租约后，先用原键查证是否已执行。补偿动作可读取 `original_receipt`。撤订结果未知时，依赖它的解冻保持等待。责任人可在实例页登记已执行、确认未执行或终止，并保存依据。选择确认未执行后，重试仍沿用原操作键。

采购样例需要接入操作连接器。隔离演练可以用上述领取和回报接口提交合成回执，例如用 `{"lease_epoch":1,"outcome":"succeeded","external_receipt":"sandbox-freeze-1","evidence":{"fixture":"purchase-demo"}}` 回报冻结成功。租约代数和动作编号必须与领取响应一致。生产连接器应保存外部引用并实现按键查单。服务端可用 `process-connector-revoke` 撤销相同流程和操作范围的凭据。

## 子流程与迁移

子流程组声明明确的已发布版本和成功门槛，例如三条并发子流程需要两条成功。详情中可以打开各子实例并读到汇合结果。取消父实例时，仍在运行的子实例收到取消请求，已经结束的子实例保留原执行记录。

新定义发布后，在「实例迁移」选择旧版、目标版本、实例编号和等待点映射。CLI 接收同样的 JSON 清单。示例中已发布第 2 版，且两个等待点继续使用同一审批角色。

~~~json
{
  "process_key": "purchasing",
  "source_revision": 1,
  "target_revision": 2,
  "run_ids": [42, 43, 44],
  "mapping": {"credit-review": "credit-check"}
}
~~~

~~~sh
workflow process-migration-preview procurement purchasing migration.json
workflow process-migration-apply procurement MIGRATION_ID
~~~

预演冻结实例、等待点与当时的状态版本，执行前逐条复核。通过的实例沿用绝对截止时间，生成新待办并保留旧版事件。发生变化或没有兼容映射的实例继续原来的流程。业务步骤已产生的操作键、回执和补偿关系均保留。

## 停收、排空和恢复

用 `workflow process-intake procurement paused` 停收新业务实例。已登记请求的重送及其后续信号、任务和操作回报仍可处理。用 `workflow process-drain procurement` 导出运行数量、未决等待、任务及补偿编号，列表可通过游标分页。制定恢复点前，在独立实例演练备份与迁移，核对工作空间归属及旧版 DAG 记录。

恢复后，从文件和数据库读回相同的实例键、信号回执、未决动作与时间线，再使用操作键和外部系统核对已生效结果。确认后执行 `workflow process-intake procurement open` 恢复接单。对备份、凭据、HTTPS 及公开部署的配置见[恢复与升级](recovery.md)与[Worker API](worker-api.md)。
