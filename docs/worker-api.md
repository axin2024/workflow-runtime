# Worker HTTP 接口

`workflow worker-api` 在回环地址提供接口。异机 Worker 通过同机反向代理的 HTTPS 地址连接，例如 `https://workflow.example.org`。反向代理转发 `Authorization`，并保持接口路径。`GET /healthz` 返回 `204`，表示 HTTP 服务已启动。

每个写请求都使用 `Content-Type: application/json` 和 `Authorization: Bearer <token>`。身份由令牌查出，授权项为工作空间内的队列和任务类型组合。数据库仅保存令牌摘要。每个 Worker 身份可以配置 1 到 64 个活跃租约，内置 Worker 进程每次执行一个节点。

## 领取任务

`POST /v1/worker/claim`。请求体声明该进程实现的任务类型和所需租约时间，租约为 1 到 120 秒。

```json
{"tasks":["aggregate","inspect"],"lease_seconds":15}
```

领取后返回 `task` 对象，包含 `run_id`、`node`、`epoch`、`operation_key`、`task`、`queue`、`effect`、`inputs`、`claimed_at`、`lease_until` 和 `deadline`。输入来自本次运行固定的版本及其指定的上游输出。领取后应立即开始执行。暂时无任务时返回 `{"task":null}`。

## 续期和完成

`POST /v1/worker/renew` 接收 `lease` 和 `lease_seconds`。`lease` 对象必须原样携带领取结果里的 `run_id`、`node`、`epoch` 和 `operation_key`。续期成功返回 `renewed_at` 和 `lease_until`，租约截止时间受节点执行期限限制。收到 `lease_lost` 后应停止当前处理。

`POST /v1/worker/complete` 接收同样的 `lease` 以及以下结果字段。

| 结果 | 字段 |
| --- | --- |
| 成功 | `success: true`，`outputs` 为符合节点输出契约的 JSON 对象。声明了分支的节点还要提交 `branch`。 |
| 失败 | `success: false`，必填 `failure_code`，可选 `failure_message`。代码与 DAG 中的重试列表匹配时，按定义的间隔安排下一次尝试。 |

成功接收后返回尝试标识与生效状态。当前代数的同一结果可以按原样重送，响应带 `replayed: true`。同一尝试改报其他结果返回 `completion_conflict`，已被接管的旧代数返回 `lease_lost`。二者均为 HTTP `409`。无效凭据返回 `401`，请求或输出契约错误返回 `400`，服务端异常返回 `500`。

## 失联处置

同一运行节点的 `operation_key` 在不同尝试中保持稳定。幂等处理器可将它用作下游系统的去重键。兼容 Worker 领取时会处理过期租约。可安全接管的任务在仍有尝试额度时开启新代数。已执行的外部动作失联、超时或明确报告结果未知时进入 `action_required`，等待人工核对。
