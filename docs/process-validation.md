# 业务流程 2.0 验收

测试使用独立 MySQL 8.4 实例和合成采购数据。外部操作结果来自测试连接器或合成回执。表中的编号对应 2.0 需求验收场景，测试名可直接搜索。DAG、Worker、回调、补跑和恢复的回归场景见[基础验收](validation.md)。

| 编号 | 读回结果 | 测试入口 |
| --- | --- | --- |
| A1–A2 | 图和表展示两个审批的各自超时出口。引用未注册补偿时返回 422 和 `unbound_compensation`。 | `scripts/test-process-console.cjs` |
| A3–A4 | 等待建立前返回 202 和查询地址。等待建立后沿用原回执号并只消费一次。同号改量得到 409，校验错误得到 422，其他空间得到 404。 | `TestMySQLProcessDurableEarlySignalAndTimeout`、`TestMySQLProcessConnectorSignalContract` |
| A5–A6 | 两人争抢只产生一个领取者。委派到期后提交旧修订 3 被拒，最后由原办理人以修订 5 确认。 | `TestMySQLProcessApprovalClaimAndDelegation` |
| A7–A8 | 三子两成只产生一次汇合。父实例取消后两个未结束的子实例收到取消，另一个已完成结果仍可查询。 | `TestMySQLProcessChildrenVersionQuorumAndCancellation` |
| A9–A10 | 撤订成功之后才能领取解冻。未知结果暂停后继并保留责任人，核对后用同一补偿键继续。两个补偿均留下确认回执。 | `TestMySQLProcessCompensationReceiptAndUnknownOutcome` |
| A11–A12 | 十条待批中只有所选三条迁移。同一清单重送不产生额外变更。缺映射实例保持原版本并消费原信号。 | `TestMySQLProcessMigrationSelectedWaitsAndOlderSignals` |
| A13 | 审批先到、定时先到、两者并行各执行 100 次。重新打开存储并重启调度后，300 次均只有一条生效出口，原审批和定时器请求号仍可查询。 | `TestMySQLProcessApprovalTimerRace`、`scripts/test-process-console.cjs` |
| A14 | 10 万休眠实例，以每秒 100 条投递信号，处理延迟 P95 低于 2 秒。重启后重送 100 条，重复推进 0 次。 | `TestMySQLProcessHundredThousandWaits` |

## 容量

在 Windows amd64、22 个逻辑 CPU、Go 1.26.0、MySQL 8.4.13 上以回环 HTTP 投递，在信号入口至回执之间计时。建立 100,000 条休眠实例耗时 5.029 秒。以 200 条请求预热 1.990 秒，随后按每秒 100.00 条投递 3,000 条请求，总耗时 30.006 秒。成功 3,000 条，拒绝、超时和其他错误各 0 条，P95 为 50.623 毫秒。发送器并发上限为 24，实测请求发出时的峰值并发为 14。重启后重送 100 条，96,800 条仍等待，已完成实例、信号回执和持久转换各有 3,200 条，重复推进 0 次。该结果对应本地实例、合成请求与指定并发。 [逐请求记录](results/process-v2/signal-load.csv)

## 浏览器、升级与恢复

Chrome 158 验证流程设计器、审批表单、倒计时、作用域和审计导出。三个竞态实例 `102630`、`102631`、`102632` 的工作台回执与 CLI 逐项相同，获胜出口依次为同意、超时、同意。样例使用可控调度时钟推进到各自的截止时刻，审计同时记录实际的数据库写入时间。 [原输入、回执和时间线](results/process-v2/approval-timer-race.json)

从 v1.1.0 公开二进制创建的 schema 11 升级到 schema 18，原有补跑记录仍可查询。停收后，未结束的实例 `102633`、等待 `131405`、任务 `318` 均可从排空清单定位，归属冲突为 0。独立实例恢复后比对 48 张表与 15 条 CLI 查询，结果一致。导出文件 40,538,838 字节，SHA-256 为 `86d8bdad004f312af3b26afb00c58aba4b87034d302470550eab24842462143c`，本次导出和导入分别耗时 1.517 秒与 9.864 秒。

## 复现

需要 MySQL 8.4、Node.js、Playwright 及其 Chromium，或提供本机 Chrome。准备 v1.1.0 Windows 发行包中的 `workflow.exe` 用于旧数据库升级。测试脚本随机创建本地数据库和账户，使用两套隔离数据目录执行恢复比对。

~~~powershell
pwsh -File ./scripts/test-mysql.ps1 -MySQLBin 'C:/mysql/bin' -PreviousCLI 'C:/workflow-v1.1.0/workflow.exe' -CapacityLoad -ProcessBrowserModule 'C:/browser-test/node_modules/playwright' -Chrome 'C:/Program Files/Google/Chrome/Application/chrome.exe' -ProcessEvidenceDirectory '.local/process-evidence'
~~~

可用 `npm install --prefix C:/browser-test playwright` 准备模块。改用随 Playwright 安装的 Chromium 时，运行 `npx playwright install chromium` 并省略 `-Chrome`。省略 `-CapacityLoad` 可跳过 10 万实例测试。只有指定 `-PreviousCLI` 和 `-ProcessBrowserModule` 时才会运行旧版升级和浏览器场景。
