# MySQL 备份、恢复与升级

MySQL 保存工作流版本、输入、运行、租约、尝试、审计、回执和凭据摘要。数据库备份需要加密保存，并按生产数据限制访问。部署配置、反向代理证书、Worker 凭据及外部系统的数据和回执由各自的存储负责备份。

## 导出

使用 MySQL 8.4 客户端。在备份期间暂停数据库迁移及其他表结构改动。计划切换时先停止旧环境接收触发和调度，再等现有 Worker 完成或按运行控制流程处理。在线备份仍可使用一致性快照，快照完成后的提交需要另行恢复。

使用 `mysql_config_editor` 为源和目标配置各自的登录路径，密码通过客户端提示输入。以下 `workflow` 和 `workflow_recovery` 分别是源库和目标库。

```sh
mysql_config_editor set --login-path=workflow-backup --host=source.example.org --port=3306 --user=workflow_backup --password
mysqldump --login-path=workflow-backup --single-transaction --quick --hex-blob --no-tablespaces --set-gtid-purged=OFF --skip-add-drop-table --skip-add-locks --result-file=workflow-backup.sql workflow
```

检查退出码，记录 SQL 文件的 SHA-256、字节数、源服务器版本、当前发行版和导出时间。Linux 可用 `sha256sum workflow-backup.sql`，PowerShell 可用 `Get-FileHash -Algorithm SHA256 -LiteralPath workflow-backup.sql`。导出文件中的数据包括工作流输入和任务日志片段，应与生产备份使用相同的保留策略。

管理站清理后生成的备份包含清理记录和日志摘要。此前生成的备份仍可包含旧日志正文，由备份保存期限控制其访问和删除。

数据库备份也包含终态回调队列、事件编号和确认状态。切换恢复库前先停掉旧投递进程。恢复点之后接收方已经确认的事件可能在新环境重试，接收方沿用原事件编号去重。 [终态回调](trigger-api.md#终态回调)

## 恢复到独立实例

1. 准备与源环境同版的 MySQL 8.4 实例。由管理员创建空库 `workflow_recovery`，字符集使用 `utf8mb4`，排序规则使用 `utf8mb4_0900_bin`。目标账户应能在该库创建表、索引和约束，以及写入数据。恢复演练期间保持 Worker API、调度器、外部触发和真实副作用接入停止。
2. 对比备份 SHA-256。连接目标后核对主机、库名及 `SHOW TABLES` 的空结果。恢复中断时，保留失败信息，再准备一个新的空库重试。

```sh
mysql_config_editor set --login-path=workflow-restore --host=target.example.org --port=3306 --user=workflow_restore --password
mysql --login-path=workflow-restore --database=workflow_recovery --execute="SELECT @@hostname, DATABASE(); SHOW TABLES;"
mysql --login-path=workflow-restore --binary-mode --database=workflow_recovery < workflow-backup.sql
```

PowerShell 中用原始文件作为客户端标准输入：

```powershell
$backupFile = (Resolve-Path -LiteralPath 'workflow-backup.sql').Path
$import = Start-Process -FilePath mysql.exe -ArgumentList @('--login-path=workflow-restore', '--binary-mode', '--database=workflow_recovery') `
    -RedirectStandardInput $backupFile -NoNewWindow -Wait -PassThru
if ($import.ExitCode -ne 0) { throw "MySQL restore failed with exit code $($import.ExitCode)" }
```

3. 用备份对应的 `workflow` 发行版，把 `WORKFLOW_MYSQL_DSN` 指向恢复库，执行 `workflow migrate` 检查库版本。按已记录的工作空间和对象编号核对版本摘要、调度游标、补跑明细、运行状态、尝试与节点输出、人工核对证据、父子运行和产物引用。示例命令中的编号来自备份前记录。

```sh
workflow revision demo dispatch-report 1
workflow schedule-get demo dispatch-report hourly-dispatch
workflow backfill-get demo 1
workflow run demo 1
workflow run-export demo 1
```

4. 正式切换前，确认旧环境的调度器、Worker 和触发已断开。对备份时存在的活动租约，先核对旧执行者和外部接收方的实际状态。按 [运行控制](run-control.md) 处理结果未知的外部动作。快照后的外部动作可能已发生，需用原业务操作号和对端回执核对。确认完后依次开放管理站、Worker API 和调度器，最后恢复入口流量。恢复后的数据库仍包含快照时的身份和会话记录。因凭据泄漏执行恢复时，应先轮换相应密钥与凭据。

## 升级

以待安装版本的发行说明为准，核对覆盖的升级路径。停止写入，制作备份并在独立实例上试恢复。先在恢复库上使用新版本执行 `workflow migrate`，核对上节的业务记录后，再对目标数据库执行迁移。迁移失败时保留日志并关闭写入，回到由原始备份恢复的数据库，再用备份对应的版本启动服务。

Windows [集成脚本](../scripts/test-mysql.ps1) 在两个临时 MySQL 8.4 实例之间导出和恢复样例数据，核对全库表校验和、CLI 查询和历史请求重送。传入 `-PreviousCLI` 可从 schema 2 验证历史调度基线，或从 schema 10 验证迁移时只登记已经结束的外部运行回调。 [复现说明](validation.md#本地复现)
