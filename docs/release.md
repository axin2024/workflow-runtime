# 构建与下载发行包

GitHub 是缺陷和功能反馈入口，码云提供同一源码版本。每个发行标签对应相同的 Git 提交。发行附件包含 Windows amd64 和 Linux amd64 两个 ZIP 压缩包，以及 `SHA256SUMS`。包内的 `BUILD.json` 记录 Git 提交、工具链版本、目标系统和二进制摘要。

## 安装

下载对应系统的包，按 `SHA256SUMS` 核查文件后解压。Windows 可直接运行 `workflow.exe --help`。Linux 解压后执行 `chmod +x workflow`，再运行 `./workflow --help`。MySQL 8.4 独立部署，编排服务通过 `WORKFLOW_MYSQL_DSN` 连接。 [自托管快速开始](quickstart.md)

运行样例时，管理站、Worker API、调度器和两个 Worker 可以共用一台主机。最小建议为 2 个逻辑 CPU、2 GiB 可用内存和 1 GiB 可用磁盘，另按工作流数据与备份留出空间。上限和延迟以实际业务及部署条件为准。 [测试环境与复现](validation.md)

## 从指定提交生成包

环境需要 Git、Go 1.26.0 和 PowerShell 7。打包脚本从指定 Git 提交导出源码，再编译两个目标，把文档、样例、许可证与构建清单放进包内。可以使用标签或完整提交号。

~~~powershell
pwsh -File ./scripts/package-release.ps1 -Version v2.0.0 -Revision v2.0.0
~~~

默认输出至 `.local/release-v2.0.0/`。GitHub 发行页提供 ZIP 和 `SHA256SUMS`，码云可从同一标签取得源码。 [变更记录](../CHANGELOG.md) · [第三方声明](../THIRD_PARTY_NOTICES.md)
