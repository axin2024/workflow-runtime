# 第三方声明

Workflow Runtime 的源码、文档和合成示例按 [Apache License 2.0](LICENSE) 发布。第三方组件遵循各自条款。仅按仓库中的依赖版本构建即可运行。文档和样例未包含第三方模型、媒体或数据集。 [样例清单](examples/README.md)

| 组件 | 固定版本 | 许可正文 |
| --- | --- | --- |
| Go MySQL Driver | v1.10.2 | [MPL-2.0](licenses/go-sql-driver-mysql.txt) |
| golang-migrate | v4.20.1 | [MIT](licenses/golang-migrate.txt) |
| robfig/cron | v3.0.1 | [MIT](licenses/robfig-cron.txt) |
| filippo.io/edwards25519 | v1.2.0 | [BSD-3-Clause](licenses/edwards25519.txt) |
| Go `x/sys` | v0.49.0 | [BSD-3-Clause](licenses/go-x-sys.txt) · [PATENTS](licenses/go-x-sys-patents.txt) |
| Go 标准库和运行时 | Go 1.26.0 | [Go LICENSE](licenses/go-runtime.txt) · [PATENTS](licenses/go-runtime-patents.txt) |

发布的可执行文件使用 Go 标准库和固定的构建依赖。MySQL 8.4 由自托管者单独安装。Go MySQL Driver 的上游对应版本源码可从其发布标签获取。 [Go MySQL Driver v1.10.2 源码](https://github.com/go-sql-driver/mysql/tree/v1.10.2)
