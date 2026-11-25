# 分布式任务编排与执行平台

Workflow Runtime 是一个基于 Go 和 MySQL 的自托管工作流编排平台。工作流用 DAG 描述任务、依赖、输入输出和失败处理。每次运行绑定发布时确定的图版本和输入。

当前版本提供定义校验、MySQL 版本发布、手工和外部触发、定时调度，以及按工作空间查询历史图和节点状态。按时刻选择补跑和 Worker 执行仍在开发中。参见[产品需求](docs/requirements/01-产品总览-prd.md)和[验收场景](docs/requirements/04-验收与运营-prd.md)。

## 校验工作流

```sh
go run ./cmd/workflow validate examples/diamond.json examples/diamond-inputs.json
go test ./...
```

节点声明任务类型、队列、超时、重试条件和副作用策略。输入绑定引用工作流输入或成功依赖的节点输出。示例包含两路并行任务、条件汇合和失败清理。节点、依赖、分支及重试条件的声明顺序不影响定义摘要。

## 发布和登记运行

需要 Go 1.26 和一个独立的 MySQL 8.4 数据库。用 `WORKFLOW_MYSQL_DSN` 传入具备该库权限的连接串，例如 `workflow:password@tcp(127.0.0.1:3306)/workflow`。`WORKFLOW_ACTOR` 指定审计记录中的操作者，默认 `local-operator`。下列命令面向可直接访问数据库的运维人员。

```sh
go run ./cmd/workflow migrate
go run ./cmd/workflow workspace-create demo
go run ./cmd/workflow publish demo 0 examples/diamond.json
go run ./cmd/workflow trigger demo dispatch-report demo-001 examples/diamond-inputs.json
go run ./cmd/workflow revision demo dispatch-report 1
go run ./cmd/workflow run demo 1
```

首次发布使用基线 `0`，后续发布填写当前活动版本。发布冲突会返回当前活动版本。`run` 命令的编号来自 `trigger` 返回的 `run.id`。登记运行时只把无前置依赖的节点标为就绪，其余节点等待依赖。

触发键在同一工作空间的同一工作流内唯一。调用方超时后可用原键和原请求重送，响应会返回已登记的运行。输入对象的字段顺序和空白不影响请求摘要。同一键改用其他输入、触发类型、显式版本或逻辑时刻时，命令返回冲突，并保存原请求和新请求的摘要。`--revision` 可明确选择历史版本，`--logical-at` 接受 RFC 3339 时间并按 UTC 保存。

## 定时调度

在已发布工作流的工作空间中保存计划，再启动调度器。正常运行时，调度器使用 MySQL 的 UTC 时钟。维护时可在测试库用 `--once --at` 指定扫描时刻。

```sh
go run ./cmd/workflow schedule-put demo dispatch-report 0 examples/dispatch-schedule.json
go run ./cmd/workflow scheduler --once --at 2026-11-01T03:00:00+08:00
go run ./cmd/workflow schedule-get demo dispatch-report hourly-dispatch
go run ./cmd/workflow scheduler --interval 5s
```

计划用五字段 Cron 和 IANA 时区定义，`start_after` 是首次匹配的排除边界。春季拨快时，未出现的本地时刻会跳过。秋季回拨时，同一壁钟时间按两个 UTC 时刻分别登记。计划的运行、首批节点和游标在同一事务提交，多台调度器可连接同一数据库。

- `catch_up` 按计划顺序补齐错过的时刻。达到 `max_active_runs` 时保留游标，等待运行释放名额。
- `skip` 跳过宽限期之前的时刻，审计记录保留跳过的时间窗。`grace_seconds` 应覆盖预期的扫描周期。
- 计划默认在登记运行时使用活动版本，也可设置 `revision` 固定版本。活动版本的输入契约变化时，游标和错误信息会保留。更新输入或固定到兼容版本即可继续。
- 首次保存配置传代数 `0`，后续保存传 `schedule-get` 返回的 `generation`。设置 `enabled` 为 `false` 暂停，恢复时继续原游标。修改 Cron、时区或 `start_after` 时重新计算游标。

## MySQL 集成测试

Windows 可用 `./scripts/test-mysql.ps1 -MySQLBin C:/path/to/mysql/bin` 启动独立的回环测试库。测试覆盖并发去重、v2 发布后的 v1 运行回读、跨工作空间读隔离、冲突审计、两台调度器并发扫描、暂停恢复以及编译后 CLI 的完整调用。也可准备专用测试库，库名以 `workflow_test_` 开头，将其连接串设为 `WORKFLOW_TEST_MYSQL_DSN` 后执行 `go test -count=1 -v ./...`。
