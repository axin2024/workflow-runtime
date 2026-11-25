# 外部触发 HTTP 接口

外部应用可以用工作流专属凭据登记运行、重送原请求，并按运行号或请求键查询当前状态。`workflow worker-api` 提供 `/v1/triggers`。本机调用使用回环地址，跨机器时通过反向代理以 HTTPS 发布 `/v1/triggers` 及其详情路径。

## 签发凭据

先发布工作流，再由运维使用 `WORKFLOW_MYSQL_DSN` 执行以下命令。凭据对应工作空间、工作流与服务身份三项。把签发返回的 `token` 存到调用端的密钥存储。

```sh
workflow trigger-grant demo dispatch-report report-importer
workflow worker-api --listen 127.0.0.1:8080
```

调用端在 `Authorization: Bearer <token>` 中提交凭据。凭据在数据库中以摘要存储，并用于限定触发目标和回执归属。用相同三项重新签发会轮换令牌，同一身份可继续读取自己此前的回执。撤销使用 `workflow trigger-revoke demo dispatch-report report-importer`。已登记的运行仍通过 [运行控制](run-control.md) 管理。

## 登记或重送运行

`POST /v1/triggers` 接收 `application/json`。`key` 是调用方为一次业务请求生成的稳定标识，最多 128 字节。`inputs` 是符合工作流输入契约的 JSON 对象。`revision` 和 `logical_at` 可省略，分别按登记时的活动版本和数据库当前时刻解析。显式传入时使用正整数版本和 RFC 3339 时间。

```json
{
  "key": "workflow/weekly:2026-W44",
  "revision": 1,
  "logical_at": "2026-11-01T09:00:00+08:00",
  "inputs": {"rows": 12, "source": "synthetic-harbor-shifts"}
}
```

将示例保存为 `event.json`，从调用端传入凭据和 API 地址：

```sh
printf 'Authorization: Bearer %s\n' "$WORKFLOW_TRIGGER_TOKEN" |
  curl --include --header @- --header 'Content-Type: application/json' \
    --data-binary @event.json "$WORKFLOW_TRIGGER_ORIGIN/v1/triggers"
```

首次写入完成后返回 HTTP `202` 和 `Location: /v1/triggers/<运行号>`。响应 `run` 包含编号、工作流、请求键、固定版本、逻辑时刻和状态。`queued` 表示运行已登记并等待执行。请求超时后用相同键、输入、版本和逻辑时刻重送，服务端返回 HTTP `200`、`replayed: true` 和原运行编号。输入字段顺序不影响请求摘要。

## 回执查询与错误

`GET /v1/triggers/<运行号>` 用编号查询。`GET /v1/triggers?key=<编码后的请求键>` 用原请求键查询。回执随工作流推进更新状态，进入终态后包含 `finished_at`。

```sh
printf 'Authorization: Bearer %s\n' "$WORKFLOW_TRIGGER_TOKEN" |
  curl --get --header @- --data-urlencode 'key=workflow/weekly:2026-W44' \
    "$WORKFLOW_TRIGGER_ORIGIN/v1/triggers"
```

| 状态 | 响应含义 |
| --- | --- |
| `400 invalid_request` | 检查 `field` 和 `message`，例如 `inputs.rows` 的类型与已发布定义不符。 |
| `401 unauthorized` | 检查服务凭据的来源以及签发或撤销状态。 |
| `404 not_found` | 指定的版本或调用方可读的回执未找到。 |
| `409 trigger_conflict` | 键已被该工作流登记。属于同一身份的冲突返回原运行号供核对。 |
| `415 unsupported_media_type` | 使用 `application/json` 提交请求。 |
| `500 internal_error` | 用相同请求键查询结果或重送，运维端可在服务日志中查异常。 |
