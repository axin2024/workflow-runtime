# 分布式任务编排与执行平台

Workflow Runtime 是一个基于 Go 和 MySQL 的自托管工作流编排平台。工作流用 DAG 描述任务、依赖、输入输出和失败处理。每次运行绑定发布时确定的图版本和输入。

当前版本提供定义校验、MySQL 版本发布、手工和外部触发、定时调度、历史补跑、Worker 执行、运行取消、外部动作核对、失败节点部分重跑和运行报告。参见[产品需求](docs/requirements/01-产品总览-prd.md)和[验收场景](docs/requirements/04-验收与运营-prd.md)。

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

## 浏览器管理站

初始化工作空间后，可签发浏览器登录凭据，在管理站发布 DAG、配置定时计划、选择历史补跑时刻、调整执行配额、诊断运行和导出报告。安装和角色说明见 [管理站指南](docs/console.md)。

```sh
go run ./cmd/workflow console-grant demo demo-operator operator
go run ./cmd/workflow console --listen 127.0.0.1:8081
```

在浏览器打开 `http://127.0.0.1:8081`，输入签发命令返回的 `token`。

## 执行示例工作流

运维端注册 Worker，从响应中安全提取一次性显示的 `token`，注入 Worker 进程的 `WORKFLOW_WORKER_TOKEN` 环境变量。以下命令在上述 `demo` 工作空间注册示例处理器，并在运维端启动 Worker API。

```sh
go run ./cmd/workflow worker-register demo dispatch-a 1 reports:extract reports:aggregate reports:inspect reports:report reports:cleanup
go run ./cmd/workflow worker-api --listen 127.0.0.1:8080
```

在另一个终端注入 Worker 令牌后启动执行进程。该进程只需要 API 地址与令牌。

```sh
go run ./cmd/workflow worker --endpoint http://127.0.0.1:8080
```

`--once` 只尝试领取一次，暂时无任务时输出 `null`。常驻 Worker 在空闲时轮询，持有任务时定期续期。可同时为不同队列、任务类型配置独立 Worker 凭据。示例处理器读取合成数据，最终生成 `12 dispatch records (passed)`。用 `workflow run demo <运行编号>` 查看节点输出、依赖判定和各次尝试。异机接入及自定义处理器参见 [Worker HTTP 接口](docs/worker-api.md)。

可为每个工作空间和队列设置有效租约上限，也可将多个工作空间的队列绑定到同一资源池。配置、用量查看和压测命令见 [执行配额](docs/execution-capacity.md)。

## 定时调度

在已发布工作流的工作空间中保存计划，再启动调度器。正常运行时，调度器使用 MySQL 的 UTC 时钟。维护时可在测试库用 `--once --at` 指定扫描时刻。

```sh
go run ./cmd/workflow schedule-put demo dispatch-report 0 examples/dispatch-schedule.json
go run ./cmd/workflow scheduler --once --at 2026-11-01T03:00:00+08:00
go run ./cmd/workflow schedule-get demo dispatch-report hourly-dispatch
go run ./cmd/workflow scheduler --interval 5s
```

计划用五字段 Cron 和 IANA 时区定义，`start_after` 是首次匹配的排除边界。春季拨快时，未出现的本地时刻会跳过。秋季回拨时，同一壁钟时间按两个 UTC 时刻分别登记。计划的运行、首批节点和游标在同一事务提交，多台调度器可连接同一数据库。

常驻调度器还清理已过期的 Worker 租约和等待超时的取消请求。维护时可运行 `workflow execution-sweep` 单独处理一次。人工取消、核对和证据字段参见 [运行控制](docs/run-control.md)。

- `catch_up` 按计划顺序补齐错过的时刻。达到 `max_active_runs` 时保留游标，等待运行释放名额。
- `skip` 跳过宽限期之前的时刻，审计记录保留跳过的时间窗。`grace_seconds` 应覆盖预期的扫描周期。
- 计划默认在登记运行时使用活动版本，也可设置 `revision` 固定版本。活动版本的输入契约变化时，游标和错误信息会保留。更新输入或固定到兼容版本即可继续。
- 首次保存配置传代数 `0`，后续保存传 `schedule-get` 返回的 `generation`。设置 `enabled` 为 `false` 暂停，恢复时继续原游标。修改 Cron、时区或 `start_after` 时重新计算游标。

## 按时刻选择补跑

在测试库保存示例计划并暂停调度器。下面的时间窗预览 7 个候选时刻，使用明确指定的 DAG 版本 `1`。确认时把示例批次号 `1` 换成预览结果中的 `id`，并从 `available` 项中选择序号。

```sh
go run ./cmd/workflow backfill-preview demo dispatch-report hourly-dispatch 1 2026-11-01T01:00:00+08:00 2026-11-01T07:00:00+08:00
go run ./cmd/workflow backfill-confirm demo 1 1,3,6
go run ./cmd/workflow backfill-get demo 1
go run ./cmd/workflow backfill-resume demo 1
```

预览使用包含起止时刻的时间窗，最多 31 天、128 个候选时刻。默认使用计划输入，也可在命令末尾给出历史版本兼容的输入文件。批次保存 UTC 时刻、业务时区与本地偏移、DAG 版本和输入摘要，每项列出触发键、受影响节点和业务操作号。`already_registered` 项会显示已有运行和实际版本。

确认会冻结选集并保留待处理时刻。旧批次继续使用预览时保存的版本和输入。每条运行与其补跑回执一起落库。达到计划并发上限时，剩余项显示 `pending`，释放名额后运行 `backfill-resume` 即可继续。按原选集重新确认也可继续，序号顺序不影响确认结果。如果普通调度在确认前已登记某个时刻，批次会以 `reused` 回执记录已有运行和实际版本。批次显示 `completed` 时，选中的运行都已登记，可按运行号跟踪各自状态。

## 部分重跑与运行报告

对已结束的失败运行，可选择失败节点并预览受影响的后继。确认后生成子运行，沿用原 DAG 版本及输入。可分别导出父子运行，查看产物来源、依赖等待和执行尝试。命令和字段说明见 [部分重跑](docs/rerun.md)。

## MySQL 集成测试

Windows 可用 `./scripts/test-mysql.ps1 -MySQLBin C:/path/to/mysql/bin` 启动独立的回环测试库。测试覆盖并发去重、历史版本、跨工作空间隔离、冲突审计、调度与补跑、租约失效接管、并行分支汇合、失败清理、取消和外部任务核对。脚本还用两个 Worker 身份经 HTTP 执行示例并回读 MySQL。也可准备专用测试库，库名以 `workflow_test_` 开头，将其连接串设为 `WORKFLOW_TEST_MYSQL_DSN` 后执行 `go test -count=1 -v ./...`。
