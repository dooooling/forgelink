# ForgeLink

ForgeLink 是面向工业设备的 Rust IoT 采集与边缘平台。

## 当前状态

当前主线已完成采集链路的基础能力，但还不是可直接部署的端到端产品。

已完成并纳入测试的模块：

- `observation-model`：共享设备、原始结果、质量和时间模型
- `driver-sdk` / `driver-loader`：Driver 契约、ABI v1、Native Plugin 加载与生命周期管理
- `diagnostics`：结构化日志、`RUST_LOG`、text/JSON 格式和脱敏
- `profile-engine` / `domain-model`：Profile 校验与转换、领域路径和 Observation 映射
- `poll-engine`：周期调度、超时、指数退避、取消和阻塞调用隔离
- `drivers/modbus`：Modbus TCP/RTU、地址解析、批量读取、超时/断线重连和 Native Plugin C ABI
- `device-manager`：设备实例注册、Driver/Profile 绑定校验、读取项生成与全链路数据映射
- `data-pipeline`：Telemetry Batch 组包、按设备聚合、背压/取消/有界排空
- `mqtt-client`：QoS 1 北向发布（rumqttc）、Topic 命名空间与 Status Envelope、断线重发与指数退避重连、LWT、TLS/mTLS、优雅停机排空
- `local-buffer`：Local Buffer/WAL（SQLite Embedded DB）——以完整 ObservationBatch 为持久化单位、本地序号按序补传、message_id 幂等、PUBACK 后删除、容量背压/拒绝、崩溃恢复
- `modbus-mock`：测试共用 Mock Modbus TCP server（非生产）

仍在建设中的能力：

- `collector`、`edge-server`、`manager` 尚未完成运行时组装
- REST API、Control Engine 仍为占位
- 尚未完成三平台真实部署、性能基准和长时间稳定性验收

架构依据见：

- [Rust 工业 IoT 采集平台架构设计方案](./Rust工业IoT采集平台架构设计方案.md)
- [开发规范](./开发规范.md)

## 目录

```text
crates/       公共核心库
drivers/      协议驱动
profiles/     设备 Profile
apps/         Collector、Edge Server、Manager
```

## 日志

进程入口统一使用 `diagnostics::init_logging`。默认级别为 `INFO`、文本格式；
可通过环境变量覆盖：

```text
RUST_LOG=debug
FORGELINK_LOG_FORMAT=json
```

优雅退出前应调用 `diagnostics::shutdown_logging()`，确保非阻塞日志队列刷写完成。

## Modbus Driver

Modbus Driver 是 Native Plugin，支持 TCP/RTU 和 FC01~FC04 批量读取。地址由 Driver
私有解析，常用形式如下：

```text
1!40001          # 从站 1，保持寄存器
coil:00001       # 默认从站的线圈
2!input:30001    # 从站 2，输入寄存器
```

TCP 配置示例：

```json
{
  "mode": "tcp",
  "host": "192.168.1.10",
  "port": 502,
  "timeout_ms": 3000,
  "unit_id": 1
}
```

RTU 配置示例：

```json
{
  "mode": "rtu",
  "serial": {
    "port": "COM3",
    "baud_rate": 9600,
    "data_bits": 8,
    "stop_bits": 1,
    "parity": "none"
  },
  "timeout_ms": 1000,
  "unit_id": 1
}
```

驱动测试会按需构建并加载本地 `cdylib` 产物；真实设备冒烟测试默认 `ignored`，需要
本机 `127.0.0.1:502` 提供 Modbus 服务后再显式运行。

## 验证

```bash
cargo check --workspace --all-targets --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --no-deps --all-features
```

Modbus Driver 的专项验证：

```bash
cargo test -p driver-modbus --all-features
```

## 分支开发

禁止直接在 `main` 分支开发、提交或推送。所有变更必须在独立分支完成，通过 Pull Request 合并到 `main`。

## 许可证

本项目采用 Apache License 2.0，详见 [LICENSE](./LICENSE)。
