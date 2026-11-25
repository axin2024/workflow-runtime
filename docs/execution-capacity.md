# 执行配额

活跃槽以 MySQL UTC 时间和有效租约计算。一次领取受 Worker 身份、工作空间、所在队列和关联共享池约束。已完成、被取消或超过租约截止时间的尝试释放槽。数据库在同一个事务内校验配额并建立租约，Worker 续约使用相同的容量锁。运行详情的每次尝试保留领取时使用的共享池。

## 配置与观察

下列命令由具备数据库访问权限的运维人员执行。假设已建立 `wholesale` 和 `retail` 两个工作空间，示例配置两队列共享 20 个槽，每个工作空间当下至多使用 12 个槽。`wholesale` 的报表队列还有自己的 10 槽上限。

```sh
workflow pool-put batch 0 20
workflow workspace-quota-put wholesale 0 12
workflow workspace-quota-put retail 0 12
workflow queue-quota-put wholesale reports 0 10 batch
workflow queue-quota-put retail exports 0 unlimited batch

workflow pool-get batch
workflow workspace-quota-get wholesale
workflow queue-quota-get wholesale reports
```

响应的 `active` 是当前活跃租约数，`max_active` 是配置上限。首次配置使用代数 `0`，后续使用查询返回的 `generation`。工作空间和单个队列可设 `unlimited`，仍受其余适用上限约束。设为 `0` 可以暂停发放新租约。缩容前先等待占用降至目标值。队列迁移到其他池或解除绑定前，先等待该队列现有租约清空。解除绑定时省略 `POOL` 参数。

## 复现并发测试

```powershell
./scripts/test-mysql.ps1 -MySQLBin C:/path/to/mysql/bin -CapacityLoad
```

常规集成测试验证两工作空间竞争同一资源池、满队列后的候选回退、续约、占用释放以及代数冲突。`-CapacityLoad` 运行 1,000 个五节点工作流。50 个用于预热，950 个用于正式测量。10 个 Worker 身份通过 30 条并发执行协程竞争 20 个槽。测试读取 MySQL 的尝试时间线，重算活跃租约峰值，并检查丢失租约的旧代数在接管前后均被拒绝。

## 本机实测

Windows x64（构建 26200）、Intel Core Ultra 7 155H（Go 可见 22 个逻辑 CPU）、Go 1.26.0、MySQL 8.4.13。执行器直接调用与 Worker HTTP 接口共用的数据库事务。表中的 p95 统计成功领取与成功提交的耗时，吞吐按整段阶段计时，含竞争等待。

| 阶段 | 完成节点 | 历时 | 节点/秒 | 领取 p95 | 提交 p95 | 空手领取次数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 预热 | 250 | 2.679 秒 | 93.3 | 121.695 毫秒 | 90.859 毫秒 | 585 |
| 正式测量 | 4,750 | 89.114 秒 | 53.3 | 625.890 毫秒 | 440.179 毫秒 | 5,682 |

1,000 个运行均成功，活跃租约峰值为 20，结束时活跃租约为 0。测试引入一个过期尝试，它在新代数恢复执行。接管前后共发起 40 次旧代数完成请求，全部被拒。两阶段意外错误均为 0。正式测量结束时测试程序 Go 堆占用 1.6 MiB，测试脚本记录的 MySQL 进程峰值工作集为 28.4 MiB。
