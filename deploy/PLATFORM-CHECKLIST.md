# ForgeLink 平台验收清单（§34.5 三目标平台）

> 每个平台（Windows x64 / Linux x64 / Linux ARM64）部署后逐项勾选。
> Linux ARM64 功能契约与 x64 一致；峰值性能允许不同（§34.5），性能
> 数值验收仅在 x64 基线硬件上执行（§34.2，见 `docs/benchmark.md`）。

| # | 检查项 | 命令/操作 | 通过标准 | 结果 |
|---|--------|-----------|----------|------|
| 1 | Collector 启动/停止 | `collector config/collector.example.yaml` 启动后 Ctrl+C（Linux 可 `kill -TERM`） | 启动日志无 error、有序停机退出码 0 | ☐ |
| 2 | 动态 Driver 加载 | 启动日志检查 `Native Plugin 加载成功` 与 `abi=1.0` | cdylib 经 `forgelink_driver_entry_v1` 加载、ABI 版本校验通过 | ☐ |
| 3 | Modbus TCP 协议模拟器读取 | 任一 Modbus TCP 模拟器（或 `modbus-mock`）按示例配置接入 | REST `/api/v1/devices` 可见设备且健康快照无 `last_error` | ☐ |
| 4 | MQTT 发布 | 本机 mosquitto 接入；订阅 `forgelink/v1/telemetry/#` | 收到 Telemetry Batch（schema `forgelink.telemetry.v1.*`）且 QoS 1 结算 | ☐ |
| 5 | REST 健康检查 | `curl http://127.0.0.1:18080/api/v1/health` | HTTP 200 且 `has_anomalies=false`（组件正常时） | ☐ |
| 6 | 断网缓存恢复 | 断开 broker ≥60s 后恢复 | WAL 在途积压后自动补传；消费端按 message_id 去重后 0 丢失 | ☐ |
| 7 | 异常退出 WAL 恢复 | 运行中强杀进程（Windows: `taskkill /F`；Linux: `kill -9`）后重启同配置 | 重启后补传 `replayed=true` 批次；已 fsync 数据不丢失 | ☐ |

## 备注

- 第 6/7 项的自动化等价物为仓库测试：`cargo test -p collector --test resilience`
  与 `cargo test -p local-buffer --test wal_crash --all-features`（真实进程
  强杀恢复）；本清单为其在目标平台的部署形态冒烟复验。
- 记录验收环境：CPU 型号、核心数、内存、磁盘、OS 版本、构建 commit
  （`forgelink-bench` 报告头部含编译期注入的 commit/rustc 信息可参考）。
