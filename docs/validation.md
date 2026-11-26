# 验收复现

用例使用合成工作流、账号和输入。Go 集成测试在通过 `WORKFLOW_TEST_MYSQL_DSN` 指定的独立 MySQL 8.4 库中执行。库名须以 `workflow_test_` 开头。Windows 脚本另行启动 Worker API、管理站和调度器，并把备份恢复到第二个 MySQL 实例。 [需求验收矩阵](requirements/04-验收与运营-prd.md)

| 功能编号 | 验收入口 |
| --- | --- |
| F01–F03 | 工作流编辑、文件导出与发布历史，图与契约校验测试，旧版输入和图摘要测试 |
| F04–F07 | 管理站手工触发、签名 HTTP 触发、时区切换、并行调度和补跑测试 |
| F08–F10 | 条件分支和汇合测试，核对跳过原因、依赖状态和运行详情 |
| F11–F14 | Worker API、授权队列、进程间租约接管、租约代数拒绝和容量约束测试 |
| F15–F17 | 退避、外部动作隔离与确认、取消并保留旧结果测试 |
| F18–F21 | 管理站运行详情、等待与领取时间、部分重跑预览、父子报告和过滤导出 |

| 编号 | 核对结果 | 执行入口 |
| --- | --- | --- |
| AC-01 | 环和非法引用有具体字段错误。发布后旧实例仍能导出原图版本。 | `TestDefinitionRejectsCyclesAndInvalidContracts`、`TestMySQLVersionAndTriggerIdentity` |
| AC-02 | 两个调度器从停机窗口追回三个时刻，每个时刻只有一条运行。 | `TestMySQLConcurrentSchedulersCatchUpAndRespectLimits`、Windows 集成脚本 |
| AC-03 | 菱形分支并发完成后只有一次汇合领取，失败时执行对应清理节点。 | `TestMySQLWorkersFenceExpiredLeaseAndAdvanceDiamond` |
| AC-04 | Worker B 在新代数完成原任务，Worker A 的旧结果被拒。 | `TestMySQLWorkersFenceExpiredLeaseAndAdvanceDiamond`、双 Worker 演示 |
| AC-05 | 已结束节点保留结果，仍在运行的节点被取消或超时回收，后继保持未执行。 | `TestMySQLCancelPreservesResultsAndSettlesUnresponsiveWorker` |
| AC-06 | 父子分别导出，子运行只重做必要节点，保留实际产物的来源运行和摘要。 | `TestMySQLWorkersFenceExpiredLeaseAndAdvanceDiamond`、Windows 集成脚本 |
| AC-07 | 更新节点输入类型时，仍在执行的旧运行按原类型完成，下一次触发按新版契约完成。 | `TestMySQLInFlightRunExecutesItsOriginalInputContract` |
| AC-08 | 七个时刻预览后只选三个，冻结时区和历史版本，重试仍返回同一批运行。报告可核对三个触发键、旧版号和 Worker 实际产物摘要。 | `TestMySQLSelectiveBackfillFreezesThreeOfSevenAndResumes`、`TestMySQLBackfillReportShowsFrozenVersionAndActualArtifacts`、Windows 集成脚本 |
| AC-09 | 分支未选中时被跳过，接受「已结束」的汇合继续，要求「成功」的汇合记录阻塞原因。 | `TestMySQLConditionalJoinExplainsSkippedBranches` |
| AC-10 | 两个工作空间计入各自上限及共享槽位，采样与历史峰值均受约束。 | `TestMySQLSharedPoolEnforcesTenantAndQueueCapacity`、`TestMySQLCapacityLoad` |
| AC-11 | 去重能力未知的外部动作保持待人工核对，原操作号和处置证据留在报告。 | `TestMySQLWorkersBackOffAndQuarantineExternalExpiry`、`TestMySQLCanceledExternalActionPreservesUncertaintyUntilResolved`、Windows 集成脚本 |
| AC-12 | 发布新版后仍可发起旧版部分重跑，核查旧版号、输入摘要和上游产物引用。 | `TestMySQLWorkersFenceExpiredLeaseAndAdvanceDiamond` |

Windows HTTP 验收还覆盖不同角色与工作空间、外部触发重送和凭据轮换，以及日志到期、批量清理和审计导出。`TestMySQLTerminalCallbackDeliveryAndLeaseRecovery` 通过真实回环 HTTP 验签，接收方在接收时查询已提交的运行结果，依次返回 503 与 204。测试核对退避、事件编号、权限隔离、租约接管及最终确认状态。测试输出会打印样例运行号和摘要，运行号由独立测试库分配。 [集成脚本](../scripts/test-mysql.ps1)

## 本地复现

~~~powershell
pwsh -File ./scripts/test-mysql.ps1 -MySQLBin 'C:/mysql/bin' -CapacityLoad
~~~

测试前一阶段的数据升级路径时，把历史 schema 2 CLI 提供给 `-PreviousCLI`。可以从历史提交 `5595b8b` 导出源码，在独立目录执行 `go build -o workflow-schema2.exe ./cmd/workflow`。脚本核查迁移前的历史调度实例，并在迁移后的数据库上执行测试、备份与独立恢复。 [恢复与升级](recovery.md)

回调队列回填可用历史提交 `ff0a372` 构建的 schema 10 CLI 作为 `-PreviousCLI`。脚本通过旧版 HTTP 接口登记一个已取消运行和一个尚未完成运行，升级后核查两者的回调状态。

用 `v1.0.0` 构建上一版 CLI 也可以传入 `-PreviousCLI`。脚本会先用旧版登记已定时触发的运行，再由新版本读回原记录，验证同一计划继续使用原有的去重身份。

GitHub Actions 的 MySQL 作业验证 Go 测试与 CLI 构建。手动触发时可以启用容量测试。备份恢复、两个 Worker 进程和管理站 HTTP 检查使用上述 Windows 脚本复现。

## 容量实测

2026-11-25 对运行代码提交 `1e13ca5` 执行验收。主机为 Windows amd64，使用 Intel Core Ultra 7 155H、22 个逻辑处理器、Go 1.26.0 和 MySQL 8.4.13。两个合成工作空间各有队列和租约上限，10 个逻辑 Worker 共用 20 个执行槽。

| 阶段 | 完成节点 | 耗时 | 领取 p95 | 完成 p95 |
| --- | ---: | ---: | ---: | ---: |
| 预热 | 250 | 3.233 秒 | 154.426 毫秒 | 114.373 毫秒 |
| 稳定负载 | 4,750 | 89.553 秒 | 406.451 毫秒 | 294.136 毫秒 |

共完成 1,000 条工作流和 5,000 个节点。数据库记录的活跃租约峰值为 20，结束后为零，过期尝试 1 次，40 次旧代数提交全部拒绝。稳定阶段未出现异常，Go 已分配堆约 1.6 MiB，本次 MySQL 进程工作集峰值 28.4 MiB。此测试在同一主机以 Go 工作协程模拟 10 个 Worker。实际跨进程 HTTP 接管另用集成脚本验证。

AC-07 对应运行 `1068`、`1069`，分别使用修订 1 的整数输入与修订 2 的字符串输入，均完成。AC-09 对应运行 `1070`，第三方可在本机复现并按自己的运行编号检查日志。该用例中跳过的分支记录 `branch_not_selected`，两个汇合点分别以 `satisfied/upstream_skipped` 和 `unmet/upstream_skipped` 记录判定。

同次测试从 schema 2 升级到 schema 10。接管演示在运行 `1088` 复现 Worker B 接管和旧代数拒绝。将 3,594,247 字节的逻辑备份导入独立 MySQL 实例后，28 张表校验和与 10 项 CLI 查询结果一致。原触发与部分重跑请求重送返回原编号，另一个新请求正常登记。备份导出用时 346 毫秒，恢复导入用时 2.975 秒。

## Schema 11 回归

开启回调队列后，从 schema 2 升级的负载测试再次完成 1,000 条工作流与 5,000 个节点。稳定阶段处理 4,750 个节点，用时 112.922 秒，吞吐 42.1 节点/秒，领取和完成的 p95 分别为 609.882 毫秒和 430.612 毫秒。活跃租约峰值仍为 20，40 次旧代数提交全部被拒。独立 MySQL 实例的恢复结果与源库的 29 张表校验和及 10 项 CLI 读回一致。

另一次测试用 schema 10 CLI 创建两条外部运行。升级后，已取消的运行 `1` 获得一条尚未尝试投递的回调，未结束的运行 `2` 未进入队列。运行 `3` 的接收方依次返回 503 和 204，第二次投递沿用原事件编号。运行 `73` 的过期投递租约成功接管，旧租约无法写回结果。

## v1.1.0 复验

2026-11-26 在同一台 Windows amd64 主机上使用 Go 1.26.0 和独立 MySQL 8.4.13 复验 12 条编号用例。运行列表下载复用了页面筛选，报告记录 UTC 时间、生成账号、工作空间与分页游标。另一个工作空间查询得到空列表，访问具体补跑报告返回 404。Worker 按历史版本实际完成了第 1、3、6 项。报告中三条触发键各不相同，产物 SHA-256 与原始输出一致，重复提交未生成新运行。

使用确切的 `v1.0.0` 构建上一版 CLI，原定时触发记录在新版本中仍被识别为已有历史运行。两个独立 Worker 进程演示失联接管，新代数完成任务，旧代数提交被拒。独立恢复核对了 29 张表的校验和及 10 项 CLI 查询，恢复前后的触发与部分重跑回执保持原编号。逻辑备份为 229,602 字节，导出用时 205 毫秒，导入用时 2.195 秒。

独立容量用例完成 1,000 条五节点工作流。预热完成 250 个节点，用时 2.953 秒。稳定阶段 4,750 个节点用时 77.116 秒，吞吐量为每秒 61.6 个节点，领取及完成延迟的 p95 分别为 321.513 毫秒及 227.142 毫秒。数据库记录的租约峰值为 20，结束后为零，40 次旧代数提交全部被拒。
