# AGENTS.md

## 仓库状态

- 当前已完成：核心规范模型（`crates/observation-model`，§96 共享模型）、Driver 契约与 ABI v1 Tag/Envelope 契约（`crates/driver-sdk`，§17）、日志基础设施（`crates/diagnostics`，§6）、Native Plugin 加载器（`crates/driver-loader`，§19/§20）、Profile Engine（`crates/profile-engine`：Device Profile 模型 §37 + 运行逻辑——完整校验 validate、JSON 加载 loader、注册表 registry、读解码/写编码 convert §37.1）、Domain Model 最小映射（`crates/domain-model`：标准路径前缀表 standard、`validate_domain_path`/`build_observation` mapper，observation_id 为长度前缀无歧义编码并嵌入 collector_session_id）、Poll Engine（`crates/poll-engine`：周期调度/超时/退避重试/取消，PR #9 已合并）、Modbus Driver MVP（`drivers/modbus`：TCP/RTU、地址解析 `1!40001`/`coil:00001`/`input:30001`、批量合并读 FC01~FC04、Native Plugin C ABI v1、超时/断线重连；测试含 mock Modbus TCP server 的 ABI 全链路与 poll-engine 最小集成）、设备管理（`crates/device-manager`：设备实例注册、Driver/Profile 绑定校验、读取项生成与分组、RawReadResult→Profile→Domain→Observation 全链路映射，含 Modbus Mock 全链路测试；测试共用 `crates/modbus-mock`）、数据管道（`crates/data-pipeline`：Telemetry Batch Envelope §31.2——`site_id`/`device_id`/独立批次序号 `sequence`/`message_id` 长度前缀无歧义编码并嵌入 `collector_session_id`、按设备聚合禁止混批、满批+定时刷新输出、保留 Observation 原 `sequence` 不重编号、有界背压/取消/有界排空 `shutdown`、输出关闭终止并异步排空结算、`validate` 拒绝非法与溢出配置；测试 14 项含背压/停机/输出关闭/溢出等，PR #13 已合并）、MQTT 输出（`crates/mqtt-client`：§31/§34/§90.1——rumqttc 0.24 基于 QoS 1 发布、Topic 命名空间 §31.1、PUBACK 结算与断线重发 §31.3/§31.4、指数退避重连 §34.3、Status Envelope `forgelink.status.v1`（在线 `publish_online` retained + 断线按设备全集重建重发周期并优先于普通请求（防饿死）、二次断线已确认设备重新入队、显式离线 `publish_offline` 注销设备、停机前为全部已跟踪设备逐条转发 retained 离线（通道满时循环转发直至期限）、LWT `sent_at_ns=0` 以到达时间为准、断线重排与 rumqttc `EventLoop::clean` 重发顺序严格一致（遗留未重发 -> 在途 pkid 槽位 -> 本会话通道，二次断线 PUBACK 不错位关联）、pkid 碰撞 `Outgoing::AwaitAck` 停放处理（碰撞消息不提前分配标识、旧消息确认后恢复，重发写事件与碰撞恢复写事件区分——碰撞未决断线重连后首个同标识写是重发、不得提前解除停放，第二个未决碰撞立即以 `CollisionOverwritten` 失败结算被覆盖的旧碰撞条目并切换碰撞标识、停放条目按配对标识独立解除，WAL 不提前删除；停机排空三阶段与主循环统一处理碰撞状态）、CONNECT UTF-8 字段与长度校验、TLS/mTLS §90.1、优雅停机 DISCONNECT 不触发 LWT + 排空结算；测试 61 项含 mock broker 的断线重发/多设备在线状态重发/超容量重发推进/重发防饿死/二次断线完整周期/普通消息二次断线确认归属/pkid 碰撞停放与断线恢复/第二个未决碰撞失败结算旧条目/停机排空碰撞结算/离线注销/停机离线/通道满停机离线/背压/停机排空等）。`drivers/modbus` 测试加载真实 cdylib 产物，产物缺失时自动 `cargo build -p driver-modbus`（嵌套 build 安全）。尚未完成的主要能力包括 REST API、Local Buffer/WAL、Control Engine 和三个运行程序的完整组装。仓库已推送至 GitHub（origin，默认分支 `main`）；GitHub Actions CI 见 `.github/workflows/ci.yml`。
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
- MVP：Modbus、Poll Engine、MQTT 输出（已交付）、REST API（待交付）；S7、CIP、FOCAS 等按文档阶段推进。

## 实施约定

- 规范模型和命名以文档定义为准：`Device`、`Resource`、`Property`、`Observation`、`Value`、`Quality`、`CommandRequest/Result`、`RawReadResult`、`DriverReadItem`、`DriverApiV1` 等不得擅自改名或改变字段。
- 新增设备前先判断：已有协议则新增 Profile；新协议才新增 Driver；随后映射 Domain Model。
- 顶层规划为 `crates/`、`drivers/`、`profiles/`、`apps/`；具体 workspace 组成按文档后部最终方案执行。
- 新增或修改架构文档使用中文，并检查术语、章节编号、交叉引用和代码示例一致性。
- 代码应有充分且合理的注释；公共 API 必须使用 Rustdoc，详细要求见 `开发规范.md`。
- 网络 I/O、采集调度和消息管道应合理使用 Rust 异步机制，并采用有界并发、背压和阻塞任务隔离；不得无限制创建异步任务。
- 工具链：Rust 1.95+，edition 2024。构建验证命令：`cargo check --workspace`（全部 18 个成员）。当前 CI 实际覆盖 `cargo fmt --all --check`、`cargo check --workspace`、`cargo test --workspace`、`cargo doc --workspace --no-deps`；带 `--all-targets --all-features` 的完整检查和 Clippy 由开发规范要求在本地或 CI 增强任务中执行。
- Collector 的 `collector`/`control` feature 目前为空占位标记；接入控制链路（control-engine、driver-write）后，必须验证 `cargo check -p collector --no-default-features --features collector` 的构建产物不包含控制代码。
- Git 操作必须遵循 `开发规范.md`：禁止直接在 `main` 分支开发、提交或推送，所有变更通过独立分支和 Pull Request 合并。
- 保持任务范围最小，不重新讨论文档中已确定的决策。
