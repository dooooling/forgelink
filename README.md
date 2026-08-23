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
- `drivers/modbus`：Modbus TCP/RTU、地址解析、批量读取、写功能 FC05/06/15/16（响应回显校验、精确相邻批量合并）、超时/断线重连和 Native Plugin C ABI
- `drivers/s7comm`：Siemens S7comm Driver V0.2（§34.6 路线图，读+写）——ISO-on-TCP（TPKT/COTP）+ Read/Write Var；地址文法 `db10.dbw0` / `db1.dbx0.3` / `mw20` / `m0.1`（I 区只读）；同区跳洞合并（位项精确相邻）、写侧精确相邻不覆盖未请求地址、分块受协商 PDU 预算约束；配套 `crates/s7comm-mock`
- `s7comm-mock`：测试共用 Mock S7 PLC server（握手/协商/读写应答与失步注入，非生产）
- `device-manager`：设备实例注册、Driver/Profile 绑定校验、读取项生成与全链路数据映射；ControlExecutor 适配层（DriverSession 共享会话读写同锁互斥、保守 Indeterminate 映射）
- `control-engine`：Control Engine 基础（§81-§90）——统一提交/取消/查询入口、幂等键去重、每设备有界队列与优先级、超时/取消 Indeterminate 结算、审计日志与 FileJournal
- `data-pipeline`：Telemetry Batch 组包、按设备聚合、背压/取消/有界排空
- `mqtt-client`：QoS 1 北向发布（rumqttc）、Topic 命名空间与 Status Envelope、断线重发与指数退避重连、LWT、TLS/mTLS、优雅停机排空
- `local-buffer`：Local Buffer/WAL（SQLite Embedded DB）——以完整 ObservationBatch 为持久化单位、本地序号按序补传、message_id 幂等、PUBACK 后删除、容量背压/拒绝、崩溃恢复
- `modbus-mock`：测试共用 Mock Modbus TCP server（非生产）
- `rest-api`：REST v1 管理接口（§31.5/§31.6/§104）——只读：设备/资源/属性查询、健康检查、`forgelink.error.v1` 错误模型、有界并发与优雅停机；控制（control feature 门控）：`POST /api/v1/devices/{id}/controls`（202 + request_id 异步受理）、`GET /api/v1/devices/{id}/control-requests/{request_id}`（三态查询，查询键与幂等键对齐）、Bearer 认证（§90.2）；指标：`GET /api/v1/metrics`（§34.2.1）；已接入 `collector` 运行时
- `metrics`：指标门面（§34.2.1）——零依赖原子 Counter/Gauge/固定桶直方图，poll/pipeline/WAL/MQTT/control 五组件埋点，collector 共享单一注册表并经 REST 暴露

仍在建设中的能力：

- `edge-server`、`manager` 尚未开始运行时组装
- 性能验收的**执行**待目标硬件：基准工具已交付（`apps/bench` 的
  `forgelink-bench`，操作手册见 [docs/benchmark.md](./docs/benchmark.md)），
  正式验收（x64 数值、72h soak）与 Linux ARM64 板载七项检查按手册执行；
  Windows x64 / Linux x64 部署包由 CI 构建上传，ARM64 经
  [scripts/build-linux-arm64.sh](./scripts/build-linux-arm64.sh) 交叉构建

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

## REST v1 只读管理接口

Collector 提供 REST v1 只读管理接口（§31.5/§31.6/§104）。**默认禁用**，需显式配置：

```yaml
# collector.yaml（片段）
rest:
  listen: "127.0.0.1:8080"   # 缺省禁用；端口 0 = 操作系统分配（开发/测试）
  max_concurrency: 64        # 有界并发（缺省 64），超限请求排队，超时返回 503
```

端点（响应体均含 `schema` 字段标识契约版本）：

| 端点 | 响应 schema |
| --- | --- |
| `GET /api/v1/devices` | `forgelink.devices.v1` |
| `GET /api/v1/devices/{id}` | `forgelink.device.v1` |
| `GET /api/v1/devices/{id}/resources` | `forgelink.resources.v1` |
| `GET /api/v1/devices/{id}/properties` | `forgelink.properties.v1` |
| `GET /api/v1/health` | `forgelink.health.v1` |

错误响应统一为 `forgelink.error.v1`，含 `code`/`message`/`request_id`；404 区分
`DEVICE_NOT_FOUND`/`RESOURCE_NOT_FOUND`，405 为 `METHOD_NOT_ALLOWED`，非法请求
400，并发超限 503，内部错误 500（401/403/409/422 由控制端点使用）。

安全边界（§90.1）：

- 默认只允许 loopback 地址；监听非 loopback 必须显式配置。
- 响应不包含 Driver 地址、连接配置、凭据（密码/证书/私钥）与缓冲路径。
- 只读接口不暴露控制路由（`controls`/`control-requests` 一律 404/405）。
- 停机时 REST 最先关闭，拒绝新连接后再排空采集链路。

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
