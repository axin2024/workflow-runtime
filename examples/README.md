# 样例数据

样例由本项目贡献者编写与维护，内容来自项目内构造的合成数据。表中版本均为 `example-v1`，可按仓库的 Apache-2.0 许可使用和再分发。SHA-256 按 UTF-8、LF 文件字节计算。 [许可证](../LICENSE)

| 文件 | 用途 | 文件 SHA-256 |
| --- | --- | --- |
| [`diamond.json`](diamond.json) | 菱形依赖、条件汇合和失败清理 | `719b813777b2b1c9a7674402108a356b76552410cecdb3e21c925e6f0bcec92d` |
| [`diamond-inputs.json`](diamond-inputs.json) | 12 条合成调度记录的生成参数 | `b85943d0ace92d6e1ce891612d466128b704f20c3403479618732f5f23afac23` |
| [`dispatch-schedule.json`](dispatch-schedule.json) | `Asia/Shanghai` 时区的整点补跑计划 | `20c6b5e0126e6c6ea83e9e1968522a0dca35cb64bd60caf45d396a7003e24549` |
| [`lease-failover.json`](lease-failover.json) | 首次体验的四节点图，汇总节点允许一次租约接管 | `d7de4ee778b16166b59ab8eff4942aec573f5bd075e04e242624b84bd4d91989` |

`workflow demo-init` 内置接管工作流和 12 条记录的参数，首笔业务时刻固定为 `2026-11-01T01:00:00Z`。租约依照当前 MySQL UTC 时钟推进。成功时末节点的 `outputs.summary` 为 `12 dispatch records (passed)`。将 `rows` 改为另一个正整数会改变记录数量和摘要。设为 `0` 会选中 `manual` 分支并跳过汇总报告。改变 `source` 会改变输入和提取结果的摘要。报告中的运行号、尝试时间和操作号取自实际环境。 [自托管快速开始](../docs/quickstart.md)
