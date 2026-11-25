# 自托管快速开始

从空库运行一条四节点工作流，使用两个 Worker 演练租约接管，并在管理站回读两次尝试和最终结果。初次安装准备依赖后，数据库、管理站和 Worker 均可通过回环地址离线运行。

| 依赖 | 版本与用途 |
| --- | --- |
| Go | 1.26.0 或更高，用于从源码构建 |
| MySQL | 8.4，使用 InnoDB 与 `utf8mb4_0900_bin` |
| 浏览器 | 打开本机管理站并查看工作流和报告 |
| PowerShell | 7.4 或更高，用于可选的 Windows 集成脚本 |

应用由同一个 `workflow` 可执行文件提供调度器、Worker API、Worker 和管理站。应用状态存入 MySQL 的数据目录，服务日志输出到进程标准错误。自托管时由服务管理器接管日志和进程。使用专用的演示 MySQL 库与数据库账号。

## 初始化和启动

1. 在 MySQL 中创建 `workflow_demo` 空库和仅能操作该库的账号。示例假定数据库服务监听 `127.0.0.1:3306`。为账号生成独立密码。

```sql
CREATE DATABASE workflow_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'workflow_demo'@'127.0.0.1' IDENTIFIED BY '<本机演示账号密码>';
GRANT ALL PRIVILEGES ON workflow_demo.* TO 'workflow_demo'@'127.0.0.1';
```

2. 在仓库根目录运行 `go build -o bin/workflow ./cmd/workflow`。Windows 用 `go build -o bin/workflow.exe ./cmd/workflow`。以下 `workflow` 代表生成的二进制，可加入 PATH 或改用完整路径。为运行管理命令与服务的终端设置 `WORKFLOW_MYSQL_DSN`，例如 `workflow_demo:密码@tcp(127.0.0.1:3306)/workflow_demo`。

```sh
workflow demo-init
workflow console-grant workflow-demo demo-admin operator
```

`demo-init` 先显示主机、库名和工作空间，再建表、导入固定的 DAG 和首笔运行。记录输出中的 `run.id`。再次运行会返回同一个编号和 `replayed: true`。管理站凭据由 `console-grant` 单独签发，重新签发会更新凭据并结束旧会话。

3. 用两个终端分别启动接口和管理站。日常使用的调度器可以在第三个终端启动。自托管升级和数据备份见 [恢复和升级](recovery.md)。

```sh
workflow worker-api --listen 127.0.0.1:8080
workflow console --listen 127.0.0.1:8081
workflow scheduler --interval 5s
```

打开 `http://127.0.0.1:8081`，用 `demo-admin` 的 `token` 登录。在「运行记录」打开上一步的运行编号。

## 完成一次接管

在管理终端注册两个身份，分别保留各自的 `token`。Worker 终端只需 `WORKFLOW_WORKER_TOKEN`。PowerShell 可用 `$env:WORKFLOW_WORKER_TOKEN = Read-Host -MaskInput 'Worker token'`，Bash 可用 `read -r -s WORKFLOW_WORKER_TOKEN` 后执行 `export WORKFLOW_WORKER_TOKEN`。

```sh
workflow worker-register workflow-demo worker-a 1 failover:aggregate
workflow worker-register workflow-demo worker-b 1 failover:extract failover:aggregate failover:inspect failover:report
```

1. 在 Worker B 终端执行 `workflow worker --once`。确认 `node` 为 `extract`。
2. 在 Worker A 终端执行 `workflow demo-worker --lease-seconds 15`。看到 `phase: paused`、`node: aggregate` 和 `epoch: 1` 后，让这个终端保持等待。这个样例进程暂停发送心跳和完成请求，模拟 Worker 失联。
3. 在 Worker B 终端执行 `workflow worker --once`。等待租约期间可完成 `inspect`。继续执行，直到返回 `aggregate`、`epoch: 2` 和 `state: succeeded`。没有可领取节点时命令返回 `null`。两次 `aggregate` 的 `operation_key` 应相同。
4. 返回 Worker A 终端按 Enter，确认旧结果输出 `outcome: lease_lost`。在 Worker B 终端完成剩余节点，直到得到 `report` 的 `epoch: 1` 和 `state: succeeded`。再执行一次，确认返回 `null`。
5. 在管理站刷新该运行，或执行 `workflow run-export workflow-demo <运行编号>`。核对汇总节点的两次尝试分别为 Worker A 已过期和 Worker B 成功。末节点应只有一次成功尝试，结果为 `12 dispatch records (passed)`。

需要再演练一次时，保留已注册的初始身份，在前一次运行结束后执行 `workflow trigger --revision 1 workflow-demo lease-failover second-dispatch examples/diamond-inputs.json`，将后续步骤指向新编号。演示数据、文件摘要与输入变化说明见 [样例清单](../examples/README.md)。
