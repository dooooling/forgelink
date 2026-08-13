# AGENTS.md

## 仓库状态

- 当前已完成核心规范模型：`crates/observation-model`（§96 共享模型）、`crates/driver-sdk`（Driver 契约、ABI v1 Tag/Envelope 契约 §17）、`crates/profile-engine`（Device Profile 模型 §37）；其余 crate 与 `drivers/modbus`、`apps/` 仍为占位。仓库已推送至 GitHub（origin，默认分支 `main`）；GitHub Actions CI 见 `.github/workflows/ci.yml`。
- 唯一架构依据是 `Rust工业IoT采集平台架构设计方案.md`，提出架构或编写代码前必须阅读相关章节。
- 日常编码、测试和变更要求遵循 `开发规范.md`。
- 文档是渐进式设计：冲突时以后文及“更新后”“最终”章节为准；仍无法判断时先询问，不自行创造第三种方案。
- 未经用户明确要求，不初始化代码、CI 或部署文件。

## 固定架构

- 五层边界固定：`Transport` -> `Protocol Driver` -> `Device Profile` -> `Domain Model` -> `Observation`。
- Driver 表示协议，不表示设备型号。仅当报文、握手、寻址或认证机制本质不同才拆 Driver；型号、地址、缩放、单位、枚举和能力差异属于 Profile。兼容型号可共用 `Profile Family`。
- Core 不得按 `driver_id` 分支，只能通过 Driver Manager 分发。
- Driver 地址是私有不透明数据；Core 只传递 `DriverReadItem { id, address, expected_type }`（语义 Property -> Driver 地址的映射由 Profile 完成）。
- 批量读取、地址合并和会话串行化属于 Driver，不属于 Core。
- Driver 返回原始结果，`Observation` 只能由 Profile + Domain 映射后生成；每个 `Observation` 必须包含 Quality 和 Timestamp。
- Property Write 与 Command 必须分开，并统一经过 Control Engine，完成认证、授权、参数与前置条件校验、风险策略、设备级队列、超时/取消/去重、`request_id` 关联和审计。
- 软件前置条件不能替代安全 PLC、急停、门锁等硬件安全机制。
- Runtime Role（collector/edge/manager）与 domain/driver/profile 正交。Collector 通过 Cargo feature 禁用控制，并设置运行时 `read_only`；只读版本不得暴露控制入口。
- Driver 支持 Static、Native Plugin、Process Plugin。Native Plugin 仅使用稳定 C ABI：`cdylib`、`forgelink_driver_entry_v1()`、ABI 版本校验和 `libloading`；禁止跨 FFI 暴露 Rust trait 或 `async fn`。Vendor SDK 优先使用 Process Plugin 隔离。
- 目标平台：Windows x64、Linux x64、Linux ARM64。
- MVP：Modbus、Poll Engine、MQTT 输出、REST API；S7、CIP、FOCAS 等按文档阶段推进。

## 实施约定

- 规范模型和命名以文档定义为准：`Device`、`Resource`、`Property`、`Observation`、`Value`、`Quality`、`CommandRequest/Result`、`RawReadResult`、`DriverReadItem`、`DriverApiV1` 等不得擅自改名或改变字段。
- 新增设备前先判断：已有协议则新增 Profile；新协议才新增 Driver；随后映射 Domain Model。
- 顶层规划为 `crates/`、`drivers/`、`profiles/`、`apps/`；具体 workspace 组成按文档后部最终方案执行。
- 新增或修改架构文档使用中文，并检查术语、章节编号、交叉引用和代码示例一致性。
- 代码应有充分且合理的注释；公共 API 必须使用 Rustdoc，详细要求见 `开发规范.md`。
- 网络 I/O、采集调度和消息管道应合理使用 Rust 异步机制，并采用有界并发、背压和阻塞任务隔离；不得无限制创建异步任务。
- 工具链：Rust 1.95+，edition 2024。构建验证命令：`cargo check --workspace`（全部 17 个成员）。CI 覆盖 `cargo fmt --all --check`、`cargo test --workspace`、`cargo doc --workspace --no-deps`；目前无 lint 配置，本地验证加 `cargo clippy --workspace --all-targets -- -D warnings`。
- Collector 的 `collector`/`control` feature 目前为空占位标记；接入控制链路（control-engine、driver-write）后，必须验证 `cargo check -p collector --no-default-features --features collector` 的构建产物不包含控制代码。
- Git 操作必须遵循 `开发规范.md`：禁止直接在 `main` 分支开发、提交或推送，所有变更通过独立分支和 Pull Request 合并。
- 保持任务范围最小，不重新讨论文档中已确定的决策。
