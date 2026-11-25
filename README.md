# 分布式任务编排与执行平台

Workflow Runtime is a self-hosted workflow orchestrator for Go and MySQL. It uses versioned DAG definitions and renewable worker leases to track how each task completes.

项目按[产品需求](docs/requirements/01-产品总览-prd.md)和[验收场景](docs/requirements/04-验收与运营-prd.md)开发。当前可以用本地命令校验声明式工作流和输入文件，查看拓扑顺序以及用于版本核对的 SHA-256 摘要。

```sh
go run ./cmd/workflow validate examples/diamond.json examples/diamond-inputs.json
go test ./...
```

节点声明任务类型、队列、超时、重试条件和副作用策略。输入绑定引用工作流输入或成功依赖的节点输出。示例包含两路并行任务、条件汇合和失败清理。节点、依赖、分支及重试条件的声明顺序不影响定义摘要。
