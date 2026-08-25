# ForgeLink Runtime Architecture V2 实施方案

> 状态：Draft for Implementation — Consistency Review Revision  
> 目标版本：ForgeLink Runtime V2  
> 适用仓库：`dooooling/forgelink`  
> 文档目的：用于直接指导架构重构、任务拆分、PR 实施和验收  
> 原则：**保留已稳定的数据模型与北向链路，重构 Driver Runtime / Device Session / Plugin Runtime**  
> 决策状态：本文区分 **Normative / Target / Transitional / Proposed**；未明确标记为 `Proposed` 的目标态要求，在对应迁移 Phase 完成后生效。

---

## 1. 背景

ForgeLink 当前已经具备较完整的采集、控制、缓存、MQTT、REST、Profile、Domain、Metrics 等基础能力，但 Driver Runtime 的实现已经与最初架构目标产生明显漂移。

当前主要问题包括：

1. Collector 顶层配置只能装配一个 Driver，但 Device 模型允许多个 `driver_id`。
2. `driver.json` 未成为唯一事实来源，Collector 会重复构造 Manifest。
3. Native Driver 由 Collector 直接 `dlopen`，Driver 崩溃会进入 Collector 故障域。
4. Device Session 依赖 `Arc<Mutex<DriverSession>>` 完成 Poll / Control 串行化，Mutex 实际承担了调度器职责。
5. Poll timeout 只能停止等待，不能终止已经运行的阻塞 Driver 调用。
6. Driver 连接状态在 Core 与 Driver 内部存在重复状态源。
7. ABI v1 使用一个大函数表表达所有能力，Capability / feature flag / function pointer 多套机制重复表达同一件事。
8. `get_last_error_json` 为句柄级“最后错误”，不适合未来并发调用。
9. Driver 二进制身份没有和 Manifest 强绑定。
10. Driver Mock 大量承担“协议正确性证明”职责，存在 Driver 与 Mock 同源同错的问题。
11. S7comm 已经暴露出协议模型错误被 Mock 测试掩盖的现实风险。

因此，本次调整不定义为“修 Driver Loader”或“修 S7”，而定义为：

> **ForgeLink Runtime Architecture V2**

### 1.1 文档规范级别

为避免把迁移期行为、目标态要求和待决策事项混为一谈，本文使用以下术语：

| 标记 | 含义 |
|---|---|
| `Normative` | 已确认的实现约束；实现不得自行偏离 |
| `Target` | Runtime V2 目标态约束；在指定 Phase cutover 后成为 Normative |
| `Transitional` | 仅迁移期允许；必须有明确删除点 |
| `Proposed` | 尚需架构/路线图决策，不得在代码中默认为已批准 |

当前有两个明确的 `Proposed` 决策门：

1. **D1 — 生产环境是否强制所有 ForgeLink 自研 Rust Driver 也必须经 Driver Host。** 本文推荐“是”；无论 D1 是否批准，`BlockingUninterruptible` / 闭源 Vendor SDK 均必须进独立 Host。
2. **D2 — Runtime V2 期间是否冻结 FINS / FOCAS / ADS 等新协议路线图。** 本文推荐冻结新增功能，只允许严重 bug、安全和协议正确性修复；最终以路线图决策为准。

`Edge Core` 不是新的 Runtime Role。本文中的 `Edge Core` 指 Collector / Edge 可复用的内部核心层；Runtime Role 仍沿用既有 `Collector / Edge / Manager`。Runtime V2 第一实施对象是 **Collector Role**。

### 1.2 关键术语

| 术语 | 本文唯一含义 |
|---|---|
| Driver | 协议/厂商能力实现；可以是 ForgeLink Rust 协议实现，也可以包装 Vendor SDK |
| Native Driver | 通过 C ABI 动态加载的 Driver binary；不等同于“所有 Rust Driver” |
| Driver Package | `driver.json` + 当前平台 artifact + 发布元数据 |
| Driver Host | 承载一个或多个 Driver Session 的独立 OS 进程 |
| Host Group | 某 Driver 的 Host 拓扑管理对象，可实现 `shared/per_driver/per_device` |
| DeviceActor | Core/driver-runtime 中每设备唯一的逻辑执行仲裁入口 |
| HostSessionWorker | Driver Host 内某设备 Session 的执行单元；不是第二个 Core DeviceActor |
| Session | 一台逻辑设备与 Driver 的连接/协议上下文 |
| Control queue | control-engine 已有的业务/安全队列；不由 DeviceActor 重做 |
| Runtime circuit breaker | driver-runtime 的连接恢复节流；不同于 control-engine 的 Indeterminate safety cooldown |

---

## 2. 重构目标

Runtime V2 必须解决以下问题：

- 一个 Collector 同时运行多个协议 Driver。
- Manifest 成为 Driver 包的唯一元数据事实来源。
- `Target`：Phase 8 cutover 后 Collector 默认路径不再直接加载 Native Driver 动态库。
- Poll 与 Control 不直接持有或调用 Driver。
- 每台设备拥有显式的 Session Runtime。
- Poll 支持 deadline、过期丢弃、coalesce。
- Control 支持优先级与 deadline。
- Driver hang 不允许永久锁死 Core 调度链路；隔离粒度必须按 `ExecutionModel` 明确到 per-driver 或 per-device。
- Driver crash 不允许导致 Collector 崩溃；同一 Host 内受影响连接的故障范围必须在隔离策略中显式声明。
- Driver Host 可被 Supervisor 自动重启。
- Driver 二进制身份必须和 Manifest 交叉校验。
- Startup 阶段完成 Driver / Profile / Address / Capability 预检。
- ABI v1 可以继续运行，避免四个现有 Driver 一次性重写。
- ABI v2 采用 Base Descriptor + `query_interface()` 模式。
- Mock 不再作为协议正确性的唯一 Oracle。

---

## 3. 非目标

Runtime V2 第一阶段不做以下事情：

- 不重写 `observation-model`。
- 不重写 `profile-engine`。
- 不重写 `domain-model`。
- 不重写 `control-engine` 的授权、幂等、Journal 语义。
- 不改变 MQTT Topic / Telemetry Envelope。
- 不改变 REST v1 的现有对外契约。
- 不改变 WAL 的持久化语义。
- 不强制所有 Driver 立即迁移到 ABI v2。
- Driver Host 的进程隔离首先解决 crash/hang 可用性故障；MVP 不把同用户权限下的 Host 宣称为“恶意插件安全沙箱”。第三方不可信 Driver 的 seccomp/AppContainer/低权限账户等属于后续安全加固。
- `Proposed(D2)`：Runtime V2 初期暂停增加新的工业协议 Driver；在路线图决策完成前不得把该建议当作已批准的产品计划变更。

---

## 4. 保留与重构边界

### 4.1 基本保留

以下模块原则上只做适配，不做架构重写：

```text
observation-model
profile-engine
domain-model
control-engine
data-pipeline
local-buffer
mqtt-client
rest-api
metrics
diagnostics
```

保留核心数据链路：

```text
Driver
  ↓
Raw Protocol Result
  ↓
Profile
  ↓
Domain
  ↓
Observation
```

### 4.2 重点重构

```text
driver-sdk
driver-loader
device-manager
poll-engine
collector runtime assembly
```

### 4.3 新增模块

建议新增：

```text
crates/
  driver-contract/
  driver-abi/
  driver-package/
  driver-host-protocol/
  driver-runtime/
  native-driver-loader/

apps/
  driver-host/
```

### 4.4 `device-manager` 的目标去向

`device-manager` **不删除**。它当前同时承担设备注册、Driver/Profile 绑定、读取项生成、Raw Result → Observation 映射、Session ownership 和 ControlExecutor 适配。Runtime V2 只迁走“会话执行与调度”职责：

```text
device-manager 保留
  ├── DeviceRegistry / DeviceInstance
  ├── Driver ↔ Profile binding
  ├── ReadItem / ReadGroup 生成
  ├── RawReadResult → Profile → Domain → Observation
  └── Sequence / mapping 编排

driver-runtime 接管
  ├── DeviceActor / DeviceHandle
  ├── Session state machine
  ├── Host routing
  ├── request queue / priority / deadline
  ├── recovery / circuit breaker
  └── session ownership
```

迁移期允许 `device-manager::DriverSession` 作为 Adapter 存在；目标态 `device-manager` 不再拥有 `Arc<Mutex<DriverSession>>`，`DeviceControlExecutor` 改为调用 `DeviceHandle`，最终可迁到 `driver-runtime` 的 ControlExecutor Adapter。

---

## 5. Runtime V2 总体架构

`Edge Core` 在本文中是**内部核心层**，不是第四种 Runtime Role；Runtime Role 仍为 `Collector / Edge / Manager`。本实施方案首先改造 Collector Role，未来 Edge Role 可复用同一套 Driver Runtime。

```text
                     Northbound
          MQTT / REST / OPC UA / Database
                         │
                         ▼
              ┌────────────────────────┐
              │       Edge Core        │
              │                        │
              │ Device Registry        │
              │ Profile / Domain       │
              │ Control / Pipeline     │
              │ WAL / MQTT             │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │    Driver Runtime      │
              │                        │
              │ DeviceActor            │
              │ Session Supervisor     │
              │ Priority / Deadline    │
              │ Backpressure / Circuit │
              └────────────┬───────────┘
                           │
                      Async IPC
                           │
              ┌────────────▼───────────┐
              │ Host Supervisor        │
              └────────────┬───────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌────────────────┐ ┌────────────────┐ ┌────────────────┐
│ S7 Driver Host │ │ MC Driver Host │ │ ENIP DriverHost│
│ Session Workers│ │ Session Workers│ │ Session Workers│
│ Native ABI     │ │ Native ABI     │ │ Native ABI     │
└───────┬────────┘ └───────┬────────┘ └───────┬────────┘
        ▼                  ▼                  ▼
     Siemens           Mitsubishi            Logix
```

### 5.1 目标态硬约束与生效点

以下约束是 Runtime V2 的 `Target`。其中“Collector 不直接 `dlopen()`”在 **Phase 8 Core cutover 完成后**强制生效；Phase 2～7 仅允许 `legacy-native-runtime` 迁移路径直接加载 ABI v1。

> Poll / Control 不直接调用 Driver；DeviceActor 是每台设备唯一的请求调度入口。

> `BlockingUninterruptible` / 高风险 Native / Vendor SDK 不得进入 Core 故障域，且必须使用可杀死的 Driver Host。

> **Async 是调度与并发模型，不是崩溃隔离模型。**

> 可控网络协议优先实现为纯 Rust async session；不可中断 FFI 不能依赖 Future cancellation 获得强制终止保证。

> Driver Host 的隔离粒度必须显式声明。`per_driver` 会让同一 Host 内的多个连接共享进程故障域；需要“单连接 crash/hang 不影响其他连接”的 Driver 必须使用 `per_device`，或证明故障可在 session 内完成恢复而无需 kill 整个 Host。

### 5.2 Proposed D1 — 自研 Rust Driver 的生产隔离政策

本文推荐：Phase 8 之后，**所有生产 Driver 默认经 Driver Host**，即使协议实现是 ForgeLink 自研 Rust。这样 Core 与协议实现不共享进程故障域。

这是比原架构“只对不稳定/闭源 SDK 提供 Process Plugin”更严格的政策收紧，必须在 Phase 0 RFC 中显式批准。D1 未批准前：

- Runtime V2 仍必须完整实现 Host 路径；
- `BlockingUninterruptible` / Vendor SDK 仍强制进 Host；
- 自研 `AsyncCancelable` Driver 的 in-process 生产支持不得被默认承诺，只能保留为 Transitional 能力直到决策完成。

## 6. 新的模块职责

### 6.1 `driver-contract`

职责：

- Core、Driver Runtime、Driver Host 共用的 Rust **Driver 语义契约**。
- 不包含 `libloading`、Native ABI、具体协议逻辑和 Tokio 调度类型。
- 不复制 `observation-model` 已经稳定的 Raw/Quality 数据类型。

`driver-contract` 自己拥有：

```rust
DriverReadItem
DriverWriteItem
DriverCommand
ProtocolCapabilities
AddressMetadata
DriverBrowseNode
SubscriptionRequest / SubscriptionId
HistoryRequest
RawWriteResult / RawCommandResult / RawEvent / RawHistoryPage
DriverErrorCategory
DriverCallError
```

注意：`driver-contract` 定义的是 **Driver 调用 payload / result**，不定义 DeviceActor 的调度信封。
`DeviceReadRequest` / `DeviceWriteRequest` / `DeviceExecuteRequest` / `DeviceAdminRequest`
属于 `driver-runtime`，它们只是在上述契约类型外包一层运行时元数据、取消令牌和 reply channel。
尤其不得把 `control-engine` 的领域请求类型（例如 `CommandRequest`）直接放进
`driver-runtime`，以免形成 `driver-runtime -> control-engine` 的反向依赖。

现有这些类型继续由 `observation-model` 定义，`driver-contract` 只做 re-export / 使用：

```rust
RawReadResult
RawValue
RawFieldValue
DriverErrorInfo
DataType
```

这样满足“Runtime V2 不重写 observation-model”，也避免同名 Raw 类型出现两份事实来源。

Runtime 调用级错误使用一个包装类型，而不是强行修改现有 Observation 边界：

```rust
pub struct DriverCallError {
    pub info: observation_model::DriverErrorInfo,
    pub category: DriverErrorCategory,
}
```

`RawReadResult.error` 等现有逐项错误仍保持 `DriverErrorInfo` 语义；Host/Session 级失败使用 `DriverCallError`，到 Profile/Domain 映射前再落回既有 Quality 规则。

目标依赖：

```text
observation-model
       ↑
driver-contract
       ↑
device-manager / driver-runtime / driver-host adapters
```

### 6.2 `driver-abi`

这是 `driver-sdk::abi` 的目标归属，专门保存跨动态库边界的 C ABI 类型：

```text
FfiStr / FfiSlice / FfiOwnedBuffer
DriverHandle
DriverApiV1
DriverDescriptorV2
ABI tag / envelope
entry symbol constants
```

规则：

- 所有跨 ABI 类型必须 `#[repr(C)]` 或显式 `ptr + len`。
- `driver-contract` 不依赖 `driver-abi`。
- `native-driver-loader` 和 Native Driver 可以依赖 `driver-abi`。
- ABI v1/v2 可在同一 crate 内按模块版本化，但不得把 loader 行为混进 ABI 定义。

### 6.3 `driver-package`

职责：

- 扫描 Driver 目录。
- 读取并校验 `driver.json`。
- 选择当前平台 artifact。
- 校验 Manifest schema / hash。
- 生成 `DriverPackageDescriptor`。

不得：

- `dlopen()`。
- 创建 Driver Handle。
- 调用协议代码。

### 6.4 `driver-host-protocol`

职责：定义 Core ↔ Driver Host 的**版本化本地 IPC 契约**。

MVP 固定为：

```text
transport: Unix Domain Socket / Windows Named Pipe
framing:   u32 little-endian length + UTF-8 JSON payload
schema:    forgelink.driver-host.v1
max frame: 8 MiB（超限在分配大 buffer 前拒绝）
```

V1 不在 JSON / CBOR / MessagePack 之间运行时任选。若未来改二进制编码，必须通过新的协议版本或显式 negotiation 引入。

### 6.5 `native-driver-loader`

职责：

```text
dlopen / LoadLibrary
entry symbol resolution
ABI v1/v2 validation
FFI buffer ownership / bounds check
ptr/len/cap sanity + maximum response size before copy
Native session creation
ABI ↔ driver-contract adapter
```

只允许 `apps/driver-host` 依赖。Collector / Core 禁止直接依赖。

### 6.6 `driver-runtime`

职责：

```text
DriverRegistry
HostSupervisor / HostClient
SessionSupervisor
DeviceActor / DeviceHandle
CircuitBreaker
RequestQueue
ControlExecutor Adapter
```

这是 Runtime V2 的调度和故障恢复核心。

### 6.7 `device-manager`

保留设备与语义映射职责，不再承担底层 Session 调度：

```text
DeviceRegistry / DeviceInstance
Driver/Profile binding
ReadItem / ReadGroup
Raw Result → Profile → Domain → Observation
```

迁移完成后不得再创建 Native Driver Handle，也不得持有用于 Poll/Control 串行化的 Driver Mutex。

### 6.8 `driver-sdk` 兼容外壳

现有四个 Driver 已依赖 `driver-sdk`，因此不能在第一步直接删除。

`Transitional` 策略：

```text
driver-sdk
  ├── re-export driver-contract
  ├── re-export driver-abi v1
  └── transitional DriverManifest alias / legacy module
```

新代码直接依赖 `driver-contract` / `driver-abi` / `driver-package`；现有代码若仍引用 `driver_sdk::DriverManifest`，在 Phase 1 通过 alias/compat module 保持编译。

当前 `driver-sdk::Driver` async Rust trait 也标记为 `Transitional`：它可以继续服务现有 Rust 内部测试/适配，但**不是新的跨插件契约**。目标态 Core 不保存 `Box<dyn Driver>`；Core 只面向 `DeviceHandle/HostClient`，Native 插件只面向 `driver-abi`。如 Host 内部需要 Rust trait，应定义为 Host 私有适配层，不跨动态库 ABI 暴露。

最后一个 ABI v1/legacy manifest/legacy Driver trait 使用点迁移完成后删除兼容外壳。

## 7. Driver Package 设计

一个 Driver Package 可以包含多个平台 artifact；因此 Manifest 不再使用“一个 `binary` + 多个 `platforms`”这种会产生矛盾的表达。

目录示例：

```text
drivers/
  s7comm/
    driver.json
    libdriver_s7comm.so
    driver_s7comm.dll
```

Manifest v2 示例：

```json
{
  "schema_version": "2.0",
  "id": "s7comm",
  "name": "Siemens S7comm",
  "version": "0.3.0",

  "abi": {
    "major": 1,
    "minor": 0
  },

  "artifacts": {
    "linux-x86_64": {
      "path": "libdriver_s7comm.so",
      "sha256": "..."
    },
    "windows-x86_64": {
      "path": "driver_s7comm.dll",
      "sha256": "..."
    },
    "linux-aarch64": {
      "path": "libdriver_s7comm.so",
      "sha256": "..."
    }
  },

  "runtime": {
    "kind": "native",
    "execution_model": "blocking_bounded",
    "minimum_isolation": "per_driver",
    "default_isolation": "per_driver"
  },

  "min_core_version": "0.3.0"
}
```

高风险 Vendor SDK 示例：

```json
{
  "runtime": {
    "kind": "native",
    "execution_model": "blocking_uninterruptible",
    "minimum_isolation": "per_device",
    "default_isolation": "per_device"
  }
}
```

规则：

- `artifacts` 的 key 就是支持平台集合，不再单独维护 `platforms` 数组。
- `sha256` 建议成为发布包必填字段；开发态可由显式 dev policy 放宽。Hash 只提供 artifact 与 Manifest 的完整性绑定，不等同于“发布者身份可信”；第三方包签名/证书链属于后续供应链安全能力。
- artifact path `canonicalize` 后必须仍位于 package root 内，拒绝 `..` / symlink 逃逸到包外 artifact。
- 生产目录不得是其他非服务用户可写目录；Hash 必须在真正 load 前再次校验，避免“扫描后文件被替换”的 TOCTOU 窗口。
- `minimum_isolation` 是安全下限；部署配置只允许选择**相同或更严格**的隔离级别。
- `execution_model` 是 Driver 能力/风险声明。现有 ABI v1 Driver 即使由 Rust 编写，只要 ABI 调用仍是同步阻塞函数，就不能仅凭“内部用了 Tokio”声明为 `async_cancelable`；Host 启动后还要与 Binary Descriptor（ABI v2）交叉校验。
- `driver.json` 是 Package 元数据唯一事实来源；Collector 不重复声明 id/version/ABI/artifact。

### 7.1 当前 Manifest v1 → v2 Cutover

当前仓库四个 `driver.json` 仍是 v1 形态：没有 `schema_version`，使用 `platforms`，且不包含 artifact/hash。Package Runtime 不能假设这些文件已经具备 v2 语义。

迁移规则：

1. Phase 1 的 Manifest PR 同时升级四个现有 `driver.json` 与 package scripts。
2. `driver-package` 可以提供**离线 migration parser/tool**读取 v1，帮助生成 v2；生产 Runtime V2 discovery 不长期静默接受缺失 `schema_version` 的 v1 Manifest。
3. 旧 Collector 的 `driver.plugin + manifest` 兼容属于 §41.1 的 Collector Config 迁移，与 Package Manifest v1/v2 是两件不同事情。
4. 发布打包时必须校验当前平台 artifact 实际存在并计算 hash；Runtime 只验证当前平台 artifact，不要求一个平台包携带其他平台二进制。

因此 source manifest 可以声明多平台 artifact 元数据，但**每个平台发布包只需包含当前平台 artifact**；Package Scanner 选择当前平台条目后再做 path/hash 校验。

## 8. Collector 配置 V2

删除当前：

```yaml
driver:
  plugin: ...
  manifest:
    id: ...
    version: ...
    abi: ...
```

改成：

```yaml
site_id: factory-a

drivers:
  directories:
    - "./drivers"
  # 可选：只能把隔离策略调得更严格，不能低于 Manifest.minimum_isolation
  isolation_overrides: {}

runtime:
  device_actor:
    ingress_capacity: 64
    admin_queue_capacity: 16
  circuit_breaker:
    failure_threshold: 3
    initial_cooldown_ms: 5000
    max_cooldown_ms: 60000
    backoff_factor: 2.0
  host_supervisor:
    request_grace_ms: 1000
    restart_initial_backoff_ms: 1000
    restart_max_backoff_ms: 30000
    crash_window_ms: 300000
    max_restarts_in_window: 5

devices:
  - id: plc-01
    driver: s7comm
    profile: siemens-s7-demo
    connection:
      host: 192.168.1.10
      port: 102
      rack: 0
      slot: 1

  - id: plc-02
    driver: mitsubishi-mc
    profile: mitsubishi-q-demo
    connection:
      host: 192.168.1.20
      port: 6006
```

Collector 不再知道：

```text
DLL / SO 文件名
entry symbol
ABI version
Driver version
platform list
```

这些全部来自 Driver Package Manifest。

Collector 允许的部署级 override 只包含**运行策略**（例如把某个 Driver 从 `per_driver` 提升到 `per_device`），不得覆盖 Driver 的 `id/version/abi/artifact` 身份元数据。

MVP 配置校验：`ingress_capacity/admin_queue_capacity > 0`；`failure_threshold >= 1`；`0 < initial_cooldown_ms <= max_cooldown_ms`；`backoff_factor >= 1.0`；Host restart backoff/窗口/次数均必须为正。Control queue capacity、风险 priority、Indeterminate cooldown 继续使用 control-engine 既有配置，不在 `runtime.device_actor` 重复配置。

---

## 9. 启动流程 V2

启动流程必须严格按以下顺序：

```text
Load Collector Config
        ↓
Discover Driver Packages
        ↓
Validate Manifest
        ↓
Select platform binaries
        ↓
Start Driver Hosts
        ↓
Query binary descriptor
        ↓
Cross-check manifest / binary identity
        ↓
Build DriverRegistry
        ↓
Load Profiles
        ↓
Bind Devices
        ↓
Validate connection config
        ↓
Validate all profile addresses
        ↓
Validate capabilities
        ↓
Create DeviceActors
        ↓
Start Poll Scheduler
        ↓
Start Northbound
```

任何 Preflight 失败：

```text
fail-fast
```

不得等到第一次 Poll 才发现配置错误。

---

## 10. Driver Registry

核心结构建议：

```rust
pub struct DriverRegistry {
    drivers: BTreeMap<String, RegisteredDriver>,
}

pub struct RegisteredDriver {
    pub package: DriverPackageDescriptor,
    pub descriptor: DriverDescriptor,
    /// 管理该 Driver 的 Host 拓扑；内部可实现 shared/per_driver/per_device。
    pub host_group: DriverHostGroupHandle,
}
```

必须支持：

```text
modbus-tcp
s7comm
mitsubishi-mc
ethernet-ip
```

同时存在。

`RegisteredDriver` 不持有“唯一 Host”，因为 `per_device` 模式下一个 Driver 会对应多个 Host 实例；所有 spawn/restart/session routing 通过 `DriverHostGroupHandle` / `HostSupervisor` 完成。

---

## 11. Binary Identity

目标态（ABI v2）要求 Driver Binary 自己声明身份：

```rust
DriverDescriptorV2 {
    driver_id,
    driver_version,
    build_id,
    abi_major,
    abi_minor,
    execution_model,
}
```

Loader / Host 必须检查：

```text
Manifest.id              == Binary.driver_id
Manifest.version         == Binary.driver_version
Manifest.abi             == Binary.abi
Manifest.execution_model == Binary.execution_model
Manifest artifact hash   == actual binary hash
```

不一致立即拒绝：

```text
DRIVER_IDENTITY_MISMATCH
```

### 11.1 ABI v1 的过渡限制

ABI v1 当前入口函数表**没有 driver_id/version/build_id**，Host-side adapter 自己填写这些字段不能证明“二进制自声明身份”。因此 ABI v1 迁移期只能做较弱校验：

```text
package path is fixed by manifest
artifact SHA-256 matches manifest
entry symbol exists
ABI major/minor matches expected v1
```

这只能证明“加载的是 Manifest 指定 artifact”，不能证明 Binary 内部身份。真正的 Manifest ↔ Binary 身份交叉验证从 ABI v2 起成为完整 Normative 要求。

## 12. DeviceActor

每台设备一个 **异步 Actor**。

```text
Device plc-01
    ↓
DeviceActor(plc-01)
```

唯一入口：

```text
Poll
Control
Admin
Internal Recovery
```

全部进入同一 bounded mailbox。

Core 侧不再使用：

```rust
Arc<Mutex<Box<dyn DriverSession>>>
```

而使用：

```rust
pub struct DeviceHandle {
    tx: tokio::sync::mpsc::Sender<DeviceRequest>,
}
```

每台设备由一个 Tokio task 独占其逻辑 Session 状态：

```rust
tokio::spawn(run_device_actor(...));
```

Actor **不能在等待一次 Driver IPC 时停止接收 mailbox**，否则长 Poll 期间新到的 Control 无法进入优先级队列。建议事件循环使用 `tokio::select!` 同时处理：

```text
ingress request
in-flight Host call completion
recovery/circuit timer
shutdown
```

同一设备仍只启动一个协议 in-flight 请求；新请求只进入 Actor 的内部调度状态。

### 12.1 DeviceActor 调度信封

`DeviceRequest` 是 `driver-runtime` 的**进程内调度信封**，不是 Driver 契约本身，
也不是 `control-engine` 的领域请求。命名统一使用 `Device*Request`，避免与现有
`CommandRequest` / `PropertyWriteRequest` 等业务类型混淆。

```rust
pub enum DeviceRequest {
    Read(DeviceReadRequest),
    Write(DeviceWriteRequest),
    Execute(DeviceExecuteRequest),

    Connect(DeviceAdminRequest),
    Disconnect(DeviceAdminRequest),
    Reset(DeviceAdminRequest),

    Shutdown,
}

pub struct DeviceReadRequest {
    pub meta: RequestMeta,
    pub items: Vec<driver_contract::DriverReadItem>,
    pub reply: tokio::sync::oneshot::Sender<
        Result<Vec<observation_model::RawReadResult>, driver_contract::DriverCallError>
    >,
}

pub struct DeviceWriteRequest {
    pub meta: RequestMeta,
    pub items: Vec<driver_contract::DriverWriteItem>,
    pub reply: tokio::sync::oneshot::Sender<
        Result<Vec<driver_contract::RawWriteResult>, driver_contract::DriverCallError>
    >,
}

pub struct DeviceExecuteRequest {
    pub meta: RequestMeta,
    pub command: driver_contract::DriverCommand,
    pub reply: tokio::sync::oneshot::Sender<
        Result<driver_contract::RawCommandResult, driver_contract::DriverCallError>
    >,
}

pub struct DeviceAdminRequest {
    pub meta: RequestMeta,
    pub reply: tokio::sync::oneshot::Sender<
        Result<(), driver_contract::DriverCallError>
    >,
}
```

其中映射关系固定为：

```text
PollScheduler
  PollTarget.items: Vec<DriverReadItem>
        ↓ wrap
  DeviceReadRequest
        ↓ DeviceActor / Host
  Vec<RawReadResult>

ControlEngine
  业务 PropertyWriteRequest / CommandRequest
        ↓ validate + Profile mapping
  DriverWriteItem / DriverCommand
        ↓ DeviceControlExecutor Adapter
  DeviceWriteRequest / DeviceExecuteRequest
        ↓ DeviceActor / Host
  RawWriteResult / RawCommandResult
```

因此 `driver-runtime` 只依赖 `driver-contract` / `observation-model` 等底层契约，
**不得依赖 `control-engine`**。`device-manager::DeviceControlExecutor`（迁移后的适配层）
负责把 control-engine 已经完成校验和 Profile 映射的结果包装成 DeviceActor 信封。

### 12.2 通用运行时元数据

```rust
pub struct RequestMeta {
    /// Core / Northbound 关联 ID；跨模块日志与控制幂等语义使用该值。
    pub correlation_id: String,
    pub origin: RequestOrigin,
    pub priority: RequestPriority,
    /// 仅在 Core 进程内比较，不直接序列化到 Host。
    pub deadline: tokio::time::Instant,
    /// 进程内取消信号。对于已经进入不可中断 FFI 的调用，仅表示停止等待/
    /// 请求 Host 尽力取消；强制回收仍依赖 Driver Host 进程边界。
    pub cancellation: tokio_util::sync::CancellationToken,
}
```

`reply` channel 只存在于 Core 进程内，不进入 IPC。DeviceActor 调用 `HostClient` 时，
按 §24 将 `correlation_id` 映射为诊断字段，并分配独立的 `ipc_request_id: u64`；
`deadline` 按 §16.3 转成 `remaining_budget_ns`，不得把 `Instant` 直接跨进程序列化。

核心原则：

```text
一个 DeviceActor
    ↓
一个设备状态机
    ↓
同一时刻一个协议事务（默认）
```

这样设备串行性来自 **Actor 独占所有权**，而不是共享 Mutex。

Driver / Session 不应被多个 Tokio task 共享可变引用。

## 13. 请求来源

```rust
pub enum RequestOrigin {
    Poll,
    Control,
    Admin,
    Internal,
}
```

---

## 14. DeviceActor 调度模型

DeviceActor 同时承载优先级、bounded mailbox、deadline 和 Poll coalescing。它是**设备级调度器**，不是简单的“异步 Mutex 包装”。

### 14.1 优先级与饥饿保护

推荐：

```text
P0 Admin/Emergency
P1 Control
P2 Poll
P3 Browse/Background
```

不得简单永久 `Control > Poll`，否则会造成 Poll starvation。

建议增加 aging：

```text
effective_priority = base_priority + wait_time_boost
```

或每执行 N 个高优先级请求后允许一个仍在 deadline 内的低优先级请求。

### 14.2 Async Runtime 原则

Collector / Core 主进程应尽量保持纯异步调度：

```rust
tokio::spawn(...)
tokio::select!
tokio::sync::mpsc
tokio::sync::oneshot
tokio::time::timeout_at
tokio_util::sync::CancellationToken
```

正常 Driver 调用链路不再以：

```rust
spawn_blocking(|| native_driver.read())
```

作为主执行模型。Core 等待的是 **Driver Host IPC Future**；真正危险的 Driver 执行位于独立 Host 故障域。

必须明确：

```text
Rust async
    ≠ 线程终止
    ≠ native FFI 取消
    ≠ segfault 隔离
```

Runtime V2 依赖：

```text
Tokio async        → 高并发 / 低线程占用 / 可组合调度
DeviceActor        → 独占 Session / 设备级顺序与背压
Process isolation  → crash / uninterruptible hang containment
Supervisor         → 自动恢复
```

### 14.3 Bounded Mailbox

DeviceActor 必须使用有界队列，例如：

```rust
tokio::sync::mpsc::channel(64)
```

容量是配置项，但必须有上下限校验；禁止 unbounded channel。

队列满时：

#### 14.3.1 Poll

允许：

```text
coalesce
replace stale pending poll
```

不得为了“保住每个 tick”无限等待 mailbox。

#### 14.3.2 Control

DeviceActor **不再建立第二套长期 Control queue**。control-engine 现有每设备队列、风险优先级、幂等、deadline 和 Indeterminate cooldown 继续作为 Control 的唯一业务调度来源；它的每设备 worker 一次只向 DeviceActor 提交一个已获准执行的 Control。

DeviceActor 只负责把这个“当前已获准 Control”与 Poll/Admin 做设备级仲裁。如果 DeviceActor/Host 在 `AcceptedForExecution` 前不可用，ControlExecutor 返回“确定未下发”的内部错误，并继续经过 control-engine 现有稳定码白名单；不得新增一套 `CONTROL_QUEUE_FULL` 北向契约。

### 14.4 Poll backlog 语义

Poll 表达“当前需要一次有效采集”，而不是“每个历史 tick 都必须执行”。同一个 `(device_id, poll_group)` 只保留最新有效 pending Poll。

```text
T0 T1 T2 T3 T4
设备繁忙
→ 不形成 5 个历史请求
→ pending_poll = latest effective poll
```

详细 stale / coalesce 规则见 §18。

## 15. Device Session 状态机

取消 Core 与 Driver 双状态。唯一逻辑状态由 DeviceActor / SessionSupervisor 维护：

```text
Created
   ↓
Disconnected
   ↓
Connecting
   ↓
Online
   ↓
Recovering
   ├──→ Online
   └──→ Faulted
   ↓
Stopping
   ↓
Stopped
```

```rust
pub enum SessionState {
    Created,
    Disconnected,
    Connecting,
    Online,
    Recovering,
    Faulted,
    Stopping,
    Stopped,
}
```

Driver 只维护内部 socket / SDK handle；Driver 不允许隐藏一个无限自动重连状态机。

`Faulted` 表示自动恢复已停止（例如运行期发现不可恢复配置/契约错误或 Host 重启超过阈值），只有 Admin reset、配置 reload 或进程重启才能重新进入恢复流程。

Circuit Breaker 的 `Closed/Open/HalfOpen` 是与 `SessionState` **正交的恢复节流状态**，不是 `SessionState` 的额外枚举值。实现中应分别保存，避免出现 `RecoveringOpen` 之类隐式组合字符串。

### 15.1 Session State 与 Observation Quality

Runtime 状态不能让采集链路“静默消失”。目标态必须保持现有 `Raw Result → Profile → Domain → Observation` 的 Quality 语义：

| Runtime 情况 | 是否做物理 I/O | 有效 Poll 的结果 |
|---|---:|---|
| `Online` | 是 | 正常 RawReadResult；逐项错误按现有规则映射 |
| `Recovering` / `Disconnected` | 否或仅 recovery I/O | 返回批级 `HostUnavailable/NotConnected`，映射 `value=None + Bad/NotConnected` |
| Circuit `Open` | 不做正常读取 | 对**有效 Poll**快速返回不可用失败，映射 `Bad/NotConnected` |
| 实际读超时 | 已尝试 | `value=None + Bad/Timeout` |
| 协议错误 | 已尝试 | `value=None + Bad/ProtocolError` |
| Last Good fallback（显式启用时） | 不代表新读取成功 | `Some(last_good) + Uncertain/Stale` |

历史 tick 因 stale/coalesce 被丢弃时**不生成补偿式假 Observation**，只增加 `poll_stale_dropped_total` / `poll_coalesced_total`。

如果未来允许某个配置错误设备以 `disabled_due_to_config` 启动，则默认不创建 Poll 任务；MVP 仍按 §29 fail-fast。

### 15.2 Runtime 状态与 Control

控制动作在进入 Host 执行前若设备处于不可用状态，应作为“确定未下发”处理；一旦 Host 已确认 `AcceptedForExecution`，随后发生 timeout/crash 时必须按 §23.3 进入 `Indeterminate`，不得自动重放高风险动作。

## 16. Timeout / Deadline 模型

必须区分 **等待超时、协议 I/O 超时和进程级硬回收**。

所有请求使用绝对 Deadline：

```rust
pub deadline: tokio::time::Instant
```

Core 进程内的绝对 `tokio::time::Instant` 从请求进入系统时创建，并贯穿到 HostClient。**该 Instant 不跨进程序列化。**

```text
REST / Poll
    ↓
Control Engine / Poll Scheduler
    ↓
DeviceActor
    ↓
HostClient --计算 remaining_budget_ns--> IPC
    ↓
Driver Host --用自己的 monotonic clock 建本地 deadline
```

禁止每层重新获得一个完整的 `timeout = 5s`，避免：

```text
队列等待 4s
+
Driver 又执行 5s
=
总延迟 9s
```

### 16.1 Queue Deadline

请求在 DeviceActor 队列中已经过期：

```text
do not execute
```

结果：

```text
DeadlineExceeded
```

Poll 可映射为：

```text
DroppedStale
```

---

### 16.2 Driver I/O Timeout

协议 Driver 自己必须设置：

```text
connect timeout
socket read timeout
socket write timeout
SDK timeout（如果厂商支持）
```

纯 Rust async Driver 优先使用 Tokio I/O：

```rust
tokio::net::TcpStream
tokio::io::AsyncReadExt
tokio::io::AsyncWriteExt
```

并通过 `tokio::select!` / `timeout_at` 实现真正可取消的等待。

---

### 16.3 跨进程 Deadline 与 Host Execution Deadline

`tokio::time::Instant` / OS monotonic 值不作为可移植 IPC 契约。HostRequest 传递的是 Core 在**写入 IPC 前**计算出的剩余预算：

```rust
remaining_budget_ns: u64
```

Host 收到后使用自己的 monotonic clock：

```rust
host_deadline = Instant::now() + remaining_budget
```

Core 自己仍按原绝对 deadline 停止等待，因此端到端上限由 Core 保证；Host 本地预算最多比 Core 多一个本地 IPC 传输/调度延迟，Hard Kill 仍以 Core deadline + grace 为最终边界。

可选 `sent_at_unix_ns` 只用于诊断，不用于 timeout enforcement，避免系统时钟跳变改变安全语义。

超过 Core deadline：

```text
HostClient stops waiting
        ↓
request/session marked recovering
        ↓
small grace period
        ↓
session-local reset if possible
        ↓
only if required by isolation policy: restart / kill host
```

---

### 16.4 可取消与不可取消调用

必须区分 Driver 执行模型：

```rust
pub enum ExecutionModel {
    AsyncCancelable,
    BlockingBounded,
    BlockingUninterruptible,
}
```

#### 16.4.1 AsyncCancelable

适用于：

```text
纯 Rust TCP/UDP 协议
Tokio AsyncRead / AsyncWrite
```

可以通过取消 Future 停止等待和 I/O。

#### 16.4.2 BlockingBounded

适用于：

```text
同步 SDK
但 SDK 自身支持可靠 timeout
```

使用专用 worker thread，不使用随机 `spawn_blocking` 线程。

#### 16.4.3 BlockingUninterruptible

适用于：

```text
可能永久阻塞的 Vendor DLL / SDK
不可取消 FFI
线程亲和 / COM / TLS 状态
```

必须依赖：

```text
独立 Driver Host
+
hard process kill
```

CancellationToken 对已经进入不可中断 FFI 的调用不能提供强制终止保证。

---

### 16.5 Hard Hang Recovery

进程是回收不可中断线程/死锁/无限循环的最终边界，但 **hard-kill 的粒度必须服从 §22 隔离策略**。

示例策略：

```text
request deadline        = 3s
host grace              = 1s
hard recovery threshold = 4s
```

处理顺序：

```text
Core deadline reached
      ↓
mark request timed out / session Recovering
      ↓
AsyncCancelable
  → cancel I/O future + close session socket
  → 不因单连接 timeout kill per-driver Host

BlockingBounded
  → 等 SDK 自身 timeout + grace
  → 超过声明上界视为 execution-model contract violation

BlockingUninterruptible
  → 必须位于 per_device（或等价独立故障域）
  → grace 后 kill 对应 Host process
```

只有当**整个 Host heartbeat/event-loop 已失去响应**时，Supervisor 才可以重启 `per_driver` Host；这会影响该 Host 内全部连接，必须产生 `driver_host_restart` 诊断事件。

不得允许一个不可返回调用永久占用 Core 资源，也不得为了回收单个可取消连接而无条件杀死包含其他设备的 Host。

## 17. Poll Scheduler V2

Poll Scheduler 只负责：

```text
什么时候产生一个有效 Poll intent
该 poll group 读哪些 items
```

不再：

```text
spawn_blocking
lock driver mutex
manage driver timeout
run driver reconnect/backoff retry loop
```

新链路：

```text
PollScheduler
      ↓
PollRequest / PollIntent
      ↓
DeviceActor
      ↓
Driver Host
```

现有 Poll Engine 的“驱动可重试错误 → 自己指数退避并重复调用”在 V2 移除。连接恢复与 backoff 归 §15/§20 的 SessionSupervisor + CircuitBreaker；这样不会同时存在 Poll retry loop、Driver hidden reconnect、Runtime circuit breaker 三套重连状态机。

---

## 18. Poll Stale / Coalesce

假设：

```text
100ms Poll
```

设备阻塞 700ms。

不允许队列堆积：

```text
P1 P2 P3 P4 P5 P6 P7
```

应该只保留：

```text
latest effective poll
```

策略：

```text
同 device + same poll group
如果已有 pending poll
→ replace/coalesce
```

`PollGroupId` 在 Startup Preflight/DeviceInstance 构建时为每个稳定 ReadGroup 分配，是 opaque `u64/String` 标识；运行期不得通过“当前 items 的 JSON/hash”临时推导。Profile reload 后若 ReadGroup 结构变化，应生成新 group generation，旧 generation 的 pending poll 直接 stale-drop。

增加指标：

```text
poll_coalesced_total
poll_stale_dropped_total
```

---

## 19. Control 与 Poll 调度

Runtime V2 不替换 control-engine 的业务安全职责。

```text
REST
  ↓
ControlEngine
  ├── auth / validation / idempotency / journal
  ├── per-device control queue / risk priority
  ├── control deadline / cancel
  └── indeterminate safety cooldown
          ↓ one admitted operation at a time per device
ControlExecutor Adapter
          ↓
DeviceActor
  ├── arbitrate admitted Control vs Poll/Admin
  └── single device in-flight
          ↓
Driver Host
```

因此不存在两套 Control FIFO/priority queue。DeviceActor 的 priority 主要用于**不同来源之间**的设备事务仲裁。

### 19.1 Control Execution Context

当前 ControlExecutor 内部契约需要增加一个**不改变 REST v1**的执行上下文，把原 control-engine 已计算出的执行边界传下去：

```rust
pub struct ControlExecutionContext {
    pub correlation_id: String,
    pub deadline: std::time::Instant,
    pub cancellation: CancellationToken,
}
```

如果具体 crate 希望使用 `tokio::time::Instant`，只能在同一进程内由该 `std::time::Instant` 转换；到 Host IPC 仍按 §16.3 转为 `remaining_budget_ns`。

`ControlExecutionContext` 只属于 control-engine ↔ `DeviceControlExecutor` 这一适配边界；
它**不会**成为 `driver-runtime` 的依赖。适配器按如下规则构造 §12 的 `RequestMeta`：

```text
ControlExecutionContext.correlation_id → RequestMeta.correlation_id
ControlExecutionContext.deadline       → RequestMeta.deadline（同进程转换）
ControlExecutionContext.cancellation   → RequestMeta.cancellation
origin                                 → Control
priority                               → 已获准 Control 的设备级仲裁优先级
```

已经经过 Profile 映射的 payload 则分别为：

```text
Vec<DriverWriteItem> → DeviceWriteRequest.items
DriverCommand        → DeviceExecuteRequest.command
```

运行中取消/超时：

- ControlEngine 仍按现有规则结算 `Indeterminate`；
- DeviceActor/Host **尽力取消** `AsyncCancelable` I/O；
- “Future 被取消”不代表物理写入确定未发生；
- 不得因为 Runtime V2 引入真正的 cancellation 就把现有安全语义降级成 `Cancelled/Failed`。

如果当前正在执行 Poll，Control 只能在当前协议事务完成/取消收敛后成为下一笔事务；不允许通过另一个 Mutex 路径并发进入同一 Session。

## 20. Circuit Breaker

设备持续离线时启用：

```text
Closed
  ↓ N failures
Open
  ↓ cooldown
HalfOpen
  ↓ success
Closed
```

MVP 默认值（对应 §8 配置，可显式覆盖但必须通过校验）：

```text
failure_threshold = 3
initial_cooldown = 5s
max_cooldown = 60s
backoff_factor = 2.0
```

Open 状态：

- 不执行正常设备读取 I/O，但有效 Poll 请求应快速返回 `NotConnected/HostUnavailable`，由现有映射链生成 `Bad/NotConnected`；不得为已经 stale/coalesced 的历史 tick 补发 Observation。
- Control 在 Host `AcceptedForExecution` 前若因设备连接 CircuitOpen 而拒绝，使用 `DriverCallError { category=ConnectionFailed, info.code="connection_failed" }`，并结算确定性 `Failed`；不得标为 `Indeterminate`。
- Supervisor 按 cooldown 尝试 HalfOpen recovery；不要让每个 Poll 自己触发 reconnect 风暴。

### 20.1 错误分类与状态转换

`retryable: bool` 不能独自承担 Runtime 状态机。V2 使用 `DriverErrorCategory`：

```text
ConnectionFailed / ConnectionLost / Timeout
  → failure counter / Recovering / Circuit Breaker

ProtocolViolation
  → 默认丢当前 session 并 Recovering；达到阈值可 Open

Config / InvalidAddress / Unsupported
  → 不做 reconnect storm；Startup 应已拦截，运行期出现则 Faulted + diagnostics

DeviceRejected
  → 设备明确负确认，不自动判定连接失效；按请求/逐项错误处理
```

ABI v1 Adapter 在没有 `category` 时根据现有稳定 `code + retryable` 做兼容映射，并把未知码保守归为 `ProtocolViolation`；Host crash/exit 由 Runtime 自己产生 `HostUnavailable/DriverCrashed`，不依赖插件上报。

**Runtime circuit breaker cooldown** 只控制连接恢复/正常 Poll 尝试；**control-engine Indeterminate cooldown** 继续控制“可能已下发的物理动作后能否继续控制”。两者不得共用同一个状态字段或配置项。

---

## 21. Driver Host

新增：

```text
apps/driver-host
```

启动参数示例：

```text
forgelink-driver-host
  --package ./drivers/s7comm
  --ipc <endpoint>
```

职责：

```text
load package
load binary
validate ABI
create sessions
execute requests
heartbeat
shutdown cleanly
```

生产 Host 采用 **load-once / process-lifetime ownership**：Driver binary 加载后不在运行期主动 `dlclose/FreeLibrary` 再热卸载。版本升级或配置 reload 通过受控 Host restart + Session recreate 完成；MVP **不承诺零停机热迁移**。进程退出是 Native 全局状态和遗留线程的最终清理边界。

Driver Host 本身建议使用 Tokio Runtime：

```text
Driver Host Tokio Runtime
        │
        ├── IPC reader task
        ├── IPC writer task
        ├── heartbeat task
        └── session workers
```

### 21.1 Pure Rust Async Driver

对于 ForgeLink 自研且网络协议可控的 Driver：

```text
Modbus TCP
S7comm
Mitsubishi MC TCP
EtherNet/IP explicit messaging
```

目标实现使用：

```rust
tokio::net::TcpStream
AsyncReadExt
AsyncWriteExt
```

但必须注意 ABI 边界：**Rust `Future` / `tokio::Handle` 不得跨动态库 C ABI 暴露。** 现有 ABI v1 的 `read/write/execute` 是同步函数，因此从 Host 视角仍属于 blocking execution；不能因为 Driver 源码是 Rust 就宣称真正 async-cancelable。

真正的 async session 在 ABI v2 通过 C-safe 的 `ASYNC_EXECUTION_V1` completion/cancel interface 实现，Host 再包装成 Rust Future。迁移完成后的形态：

```text
DriverHost
   │
   ├── HostSessionWorker PLC-1 ── async socket
   ├── HostSessionWorker PLC-2 ── async socket
   └── HostSessionWorker PLC-3 ── async socket
```

这样大量在线设备主要消耗 Future 和 socket，而不是为每个在途设备调用占用 OS thread。

### 21.2 Blocking / Vendor SDK Driver

Vendor SDK 不强制“伪 async 化”。

采用：

```text
Async IPC
   ↓
Session proxy
   ↓
Dedicated Worker Thread
   ↓
Blocking SDK
```

每个需要线程亲和的 Session 绑定固定 worker thread，Native handle 只在该 worker 上创建、调用和销毁。不要为了让类型进入 Tokio 线程池而给未知 Vendor handle 随意增加 `unsafe impl Send/Sync`。

不要依赖每次调用随机进入 Tokio blocking pool，因为部分工业 SDK 可能依赖：

```text
thread affinity
thread-local state
COM apartment
hidden global state
```

### 21.3 Session Single-flight

即使 Host IPC 支持多路复用，同一设备默认保持：

```text
single in-flight protocol transaction
```

而不同设备之间并行：

```text
Device A ─┐
Device B ─┼─ concurrent
Device C ─┘
```

这正是 Rust async 的主要收益之一。


---

## 22. Driver Host 隔离模式

支持三种隔离级别：

```text
shared < per_driver < per_device
```

这里的“更严格”指**更小的故障影响范围**。

### 22.1 `shared`

仅测试 / 开发。多个不同 Driver 共用一个 Host，任一崩溃都可能影响全部 Driver，不建议生产使用。

### 22.2 `per_driver`

适用于 `AsyncCancelable`、ForgeLink 可控且稳定的协议实现；也是本文对 D1 的推荐默认值：

```text
s7comm-host
modbus-host
mc-host
ethernet-ip-host
```

一个 Host 内可以承载多个设备 async session。必须明确故障范围：**Host 进程 crash/restart 会暂时影响该 Driver 的全部设备连接**。因此，`per_driver` 不能被描述成“任意单连接崩溃都完全不影响其他连接”。

对于 async session 的普通 timeout，应优先做 session-local close/reconnect，**不得因为一个连接超时就 kill 整个 per-driver Host**。

### 22.3 `per_device`

以下情况必须使用：

```text
BlockingUninterruptible
不可取消 Vendor DLL / SDK
COM / 强线程亲和
历史稳定性差的闭源 SDK
业务明确要求单设备进程故障不影响其他连接
```

示例：

```text
fanuc-01-host
fanuc-02-host
```

此模式允许对一个卡死 session 执行 hard process kill，而不会中断其他设备连接。

### 22.4 Runtime Policy

Manifest 使用 §7 的统一 `runtime` 结构，不再同时出现 `runtime_policy` 与另一个 `runtime` 对象：

```json
{
  "runtime": {
    "kind": "native",
    "execution_model": "async_cancelable",
    "minimum_isolation": "per_driver",
    "default_isolation": "per_driver"
  }
}
```

Vendor SDK：

```json
{
  "runtime": {
    "kind": "native",
    "execution_model": "blocking_uninterruptible",
    "minimum_isolation": "per_device",
    "default_isolation": "per_device"
  }
}
```

部署配置允许把隔离调得更严格，禁止低于 `minimum_isolation`。Host 还必须校验 Binary Descriptor 声明的 `ExecutionModel` 与 Manifest 一致。

## 23. Host Supervisor

核心职责：

```text
spawn
secure endpoint setup
health / heartbeat
detect exit
restart
rebind sessions
```

```rust
pub enum HostState {
    Starting,
    Ready,
    Degraded,
    Restarting,
    Failed,
    Stopped,
}
```

Driver Host 异常退出：

```text
associated DeviceActors → Recovering
        ↓
settle all in-flight IPC requests
        ↓
restart host
        ↓
reload driver
        ↓
recreate sessions
        ↓
reconnect
        ↓
Online
```

Collector 不退出。

### 23.1 Restart Backoff / Crash Loop

Supervisor 必须有界重启，禁止 Host crash 后无间隔 fork/spawn 风暴。MVP 默认值对应 §8 `runtime.host_supervisor`：

```text
initial_restart_backoff = 1s
max_restart_backoff     = 30s
crash_window            = 5min
max_restarts_in_window  = 5
```

超过 crash-window 阈值：

```text
HostState::Failed
associated SessionState::Faulted
```

停止自动重启，等待 Admin reset/配置 reload。每次 restart/backoff/final-failed 都必须有 metric + structured log。

### 23.2 Host crash 的采集结算

所有未完成 Read 立即以内部 `HostUnavailable` 结束；`device-manager` 继续通过既有失败映射生成 `Bad/NotConnected` Observation。不得让等待者一直等到原请求业务 timeout 才知道进程已经退出。

### 23.3 Host crash 的控制结算与确定性

写入/命令必须区分 Host 是否已经接管执行：

```text
Request 未收到 AcceptedForExecution
→ 可以证明尚未交给 Driver Session
→ WriteOutcome/ExecuteOutcome::Failed
→ DriverCallError.category = HostUnavailable
→ DriverErrorInfo.code = "driver_call_failed"（复用现有 REST 白名单）

Request 已收到 AcceptedForExecution
但未收到最终 Response 就 crash / hard-kill
→ 设备动作可能已经下发
→ WriteOutcome/ExecuteOutcome::Indeterminate
→ category = HostUnavailable
→ info.code = "driver_call_failed"
→ 进入现有 control cooldown
→ High/Critical 禁止自动重放
```

这与当前 control-engine “只有能证明未下发才允许 Failed，否则保守 Indeterminate” 的语义保持一致。

`AcceptedForExecution` 只是**保守确定性边界**，不表示设备一定已经执行成功。

## 24. Async IPC、安全与消息模型

Core ↔ Driver Host IPC 必须异步，且仅暴露本机 endpoint：

```text
Unix    → tokio::net::UnixStream
Windows → Tokio-compatible Named Pipe
```

MVP **禁止监听 TCP 端口**。IPC 认证/ACL 用于防止同机误连接和非授权本地进程调用，但它不替代第三方恶意 Driver 的 OS 权限沙箱。

### 24.1 Framing 与版本

V1 固定：

```text
u32 little-endian frame length
+
UTF-8 JSON payload
```

要求：

- schema 固定为 `forgelink.driver-host.v1`；
- frame length 在分配 payload 前校验；
- MVP `MAX_FRAME_BYTES = 8 MiB`；大 payload 未来使用 chunking/streaming，而不是无限增大 frame；
- malformed JSON、未知 schema、未知消息类型均 fail-closed；
- 同一个 Host 连接支持 request multiplexing。

### 24.2 两类 Request ID

不要把 Core 的业务关联 ID 和 IPC 自增 ID 混成同一个字段：

```text
correlation_id: String
  Core / Northbound / 日志 / 控制幂等关联

ipc_request_id: u64
  单 HostClient connection 内部请求-响应配对
```

`ipc_request_id` 仅在连接内唯一；重连后可以重新从 1 开始。Host 日志同时记录 `correlation_id` 与 `ipc_request_id`。

每次 Host 启动生成新的 `host_instance_id`。`session_id` 只在 `(host_instance_id, connection)` 范围内有效；Host restart 后旧 `session_id` 全部失效，SessionSupervisor 必须重新 `CreateSession`，禁止把旧 session id 直接复用到新进程。

### 24.3 跨进程 Deadline

HostRequest 不发送 Core 的 `Instant`：

```rust
struct HostRequest {
    ipc_request_id: u64,
    correlation_id: String,
    session_id: u64,
    remaining_budget_ns: u64,
    body: HostRequestBody,
}
```

语义见 §16.3。

### 24.4 执行确认与响应

Host 协议必须有显式中间确认，以支持 §23.3 的控制确定性：

```rust
enum HostFrame {
    Request(HostRequest),
    AcceptedForExecution {
        ipc_request_id: u64,
    },
    Response {
        ipc_request_id: u64,
        result: Result<HostResponseBody, DriverCallError>,
    },
    Heartbeat {
        host_instance_id: String,
    },
}
```

`AcceptedForExecution` 的发送点固定：

- **同步 ABI 调用**：Host 完成本地参数/Session 校验后，在进入可能触发设备 I/O 的 FFI 调用**之前紧邻发送**；从此点发生 crash 视为可能已执行。
- **`ASYNC_EXECUTION_V1`**：`submit_*` 若同步返回错误，不发送 Accepted；只有 `submit_*` 成功取得 operation_id 后发送 Accepted。

对于 Write / Execute，从 Accepted 起发生 crash/hard-kill 一律按 `Indeterminate` 保守处理。Accepted 不表示设备已成功，只表示 Runtime 已失去“确定未下发”的证明。

`HostRequestBody` 至少包括：

```text
Hello / DriverDescriptor
CreateSession / DestroySession
Connect / Disconnect
Read / Write / Execute
ValidateAddress / Capabilities
Shutdown
```

ABI v2 禁止句柄级 `last_error`；错误随每次 Response 返回。

### 24.5 IPC 安全边界

Driver Host IPC 与 REST loopback 一样属于本机攻击面，必须显式限制：

#### 24.5.1 Unix

```text
runtime directory: 0700
socket file:       0600
owner:             Collector service user
```

优先校验 peer credentials（平台支持时）；Host 退出后清理 stale socket。

#### 24.5.2 Windows

Named Pipe 创建时使用显式 ACL，只允许 Collector/DriverHost 对应服务 SID 或运行用户访问；禁止使用 Everyone / Anonymous 可写 ACL。

#### 24.5.3 双平台共同要求

- Supervisor 为每次 Host 启动生成至少 256-bit 随机 `host_auth_token`。MVP 通过**仅对子进程继承的环境变量**传递（禁止放在命令行参数）；Driver Host 启动读取后立即从自身环境中移除，并且永不记录原值。`Hello` 必须完成 token 校验后才处理业务帧。
- token、连接配置、设备凭据不得写入日志。
- endpoint 名包含随机 host instance id，防止复用陈旧 endpoint。
- Core 对异常 peer / protocol version mismatch 立即断开并重启 Host；不得降级到不认证模式。

### 24.6 Multiplexing 与 single-flight

HostClient 维护的 pending table 不能只放一个 `oneshot::Sender<HostFrame>`，因为同一请求会先收到 `AcceptedForExecution`，再收到最终 `Response`。建议：

```rust
struct PendingCall {
    correlation_id: String,
    accepted: bool,
    response_tx: oneshot::Sender<Result<HostResponseBody, HostCallError>>,
}

HashMap<IpcRequestId, PendingCall>
```

reader task 收到 `AcceptedForExecution` 只把 `accepted=true`；收到最终 Response 才 remove + 完成 oneshot。连接断开时遍历 pending table，并把 `accepted` 状态带入 `HostCallError`，供 §23.3 判定 Failed vs Indeterminate。

不同设备可以并发共享一个 IPC 连接；同一设备默认仍由 DeviceActor / HostSessionWorker 保持 single-flight。IPC multiplexing 不等于允许一个协议 Session 并发事务。

## 25. ABI v1 迁移策略

第一阶段保留现有 Driver。

结构：

```text
Core
 ↓
IPC V2
 ↓
driver-host
 ↓
ABI v1 Adapter
 ↓
现有 Native Driver
```

现有：

```text
Modbus
S7
MC
EtherNet/IP
```

无需立即重写。

这一步完成后即可获得：

```text
process isolation
multi-driver
host restart
session runtime
```

而不需要同步改四个 Driver。

### 25.1 ABI v1 Host Adapter 必须先做的安全加固

把 v1 移进 Host 不能原样复制现有 unsafe 假设。Adapter 必须在 Phase 7 同时完成：

1. **header-first struct-size 校验**：entry 返回指针后，先读取最小 ABI header/前缀；在验证 `struct_size` 之前不得形成一个可能大于插件实际对象的完整 Rust reference。
2. **bounded copy**：只复制 Host 已知且插件声明存在的函数表字节；缺少必需 v1 函数直接拒绝加载。
3. **FFI buffer sanity**：校验 `ptr/len/capacity` 关系、null 规则、最大响应长度，完成复制后只调用插件自己的 `free_buffer` 一次。
4. **create failure contract**：输出 handle 先清零；只有 create 成功才进入 destroy 生命周期。
5. **thread affinity**：需要 dedicated worker 的 Native handle 在同一线程 create/use/destroy，不通过 `unsafe impl Send` 绕过约束。
6. **no runtime unload**：Driver binary 与 Host 进程同生命周期，避免插件遗留线程在 `dlclose` 后回调已卸载代码。

这些是 ABI v1 在进入新的故障隔离架构前的最低安全门槛，不等待 ABI v2 才修。

---

## 26. ABI v2

在 S7 Protocol Reset 与 Runtime fault-isolation 稳定后实施。ABI 类型定义位于 `driver-abi`；`native-driver-loader` 只负责加载和校验。

Base Descriptor：

```rust
#[repr(C)]
pub struct DriverDescriptorHeader {
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
}

#[repr(C)]
pub struct DriverDescriptorV2 {
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,

    pub driver_id: FfiStr,
    pub driver_version: FfiStr,
    pub build_id: FfiStr,
    /// driver-abi 稳定 u32 ExecutionModel code。
    pub execution_model: u32,

    pub create_session: extern "C" fn(...),
    pub destroy_session: extern "C" fn(...),

    pub query_interface: extern "C" fn(...),
}
```

Loader 读取 Descriptor 时不得在知道 `struct_size` 前直接构造一个可能越过插件实际内存的 `&DriverDescriptorV2`。必须先读取固定 `DriverDescriptorHeader`，校验 `struct_size >= size_of::<DriverDescriptorV2>()`，再 bounded-copy 已知 base descriptor。Base Descriptor 在 ABI major 2 内保持固定；新增能力走 `query_interface()`，不再依赖无限 append-only 大结构。

Descriptor / interface vtable 指针以及 `driver_id/driver_version/build_id` 的 borrowed `FfiStr` 必须在 Driver binary 整个加载生命周期内保持有效且不可变；Host 不释放这些静态描述符内存。动态响应数据仍走显式 owned-buffer / callback ownership 契约。

对于真正 async Driver，ABI v2 增加 C-safe completion interface，而不是跨 ABI 返回 Rust Future：

```text
ASYNC_EXECUTION_V1
  submit_read(..., completion_cb, user_data, out_operation_id)
  submit_write(...)
  submit_execute(...)
  cancel_operation(operation_id)
```

`ASYNC_EXECUTION_V1` 生命周期固定：

- `submit_*` 返回成功后，completion callback **恰好一次**；
- callback 可在 Driver 自有线程/async runtime 调用，Host 必须把它快速转发到 channel，不在 callback 内做阻塞业务；
- `cancel_operation` 是取消请求，不是“物理动作确定未发生”的证明；最终仍由 completion 或 Host crash settlement 决定结果；
- `destroy_session` 返回前必须保证该 Session 不会再产生 callback；无法保证时 Host 不做动态 unload，而是进程级回收；
- Host shutdown 必须先停止接收新 submit，再 drain/cancel outstanding operations 到 deadline。

只有成功提供该接口并满足上述 cancellation/lifecycle 契约的 Driver 才能声明 `ExecutionModel::AsyncCancelable`。同步 `READ_V1/WRITE_V1` Driver 仍按 `BlockingBounded` 或 `BlockingUninterruptible` 运行。

`create_session` 输出句柄的契约固定为：**仅返回成功状态时 out_handle 才有效**。Host 必须先把输出位置置为 null/zero；create 失败时绝不调用 destroy 一个未确认有效的句柄。

---

## 27. Interface-based Capability

能力不再通过“大函数表 + Unsupported Stub”表达。

接口：

```text
READ_V1
WRITE_V1
COMMAND_V1
BROWSE_V1
SUBSCRIPTION_V1
HISTORY_V1
ASYNC_EXECUTION_V1
```

调用：

```text
query_interface(READ_V1)
```

结果：

```text
non-null → supported
null     → unsupported
```

每个返回的 interface vtable 都必须以固定 header 开头：

```text
struct_size
interface_major
interface_minor
```

Host 使用与 Descriptor 相同的 header-first / bounded-copy 规则，避免对较小旧结构形成越界引用；`query_interface()` 返回的 vtable 指针在 Driver 加载生命周期内保持稳定。

---

## 28. Error Model V2

保持现有 `observation-model::DriverErrorInfo` 的公开 Rust 语义不变：

```rust
pub struct DriverErrorInfo {
    pub code: String,
    pub message: String,
    pub protocol_code: Option<i64>,
    pub retryable: bool,
}
```

Runtime V2 新增调用级分类：

```rust
pub struct DriverCallError {
    pub info: DriverErrorInfo,
    pub category: DriverErrorCategory,
}

pub enum DriverErrorCategory {
    Config,
    InvalidAddress,
    Unsupported,

    ConnectionFailed,
    ConnectionLost,
    Timeout,

    ProtocolViolation,
    DeviceRejected,

    DriverPanic,
    DriverCrashed,
    HostUnavailable,

    Cancelled,
    DeadlineExceeded,
}
```

这样不需要把 Runtime 私有 category 强塞进 `RawReadResult.error` 或 Observation schema。

跨 C ABI 时 `DriverErrorCategory` 映射为 `driver-abi` 定义的稳定 `u32` category code，禁止把 Rust enum 默认布局直接暴露给插件。ABI v2 每次调用/完成事件直接携带 `DriverCallError` 等价 envelope，删除句柄级：

```text
get_last_error_json
```

### 28.1 Error Category 的北向边界

`DriverErrorCategory` 是 Runtime 内部分类，**不得直接成为 REST v1 / MQTT 的新公开枚举**。现有北向契约保持不变：

```text
DriverCallError.info.code
    ↓
control-engine 现有稳定码白名单
    ↓
known code 或 driver_error
```

未知 Host/Driver code 默认仍归一为 `driver_error`，原始 Driver message 不北向透传。MVP 的 `HostUnavailable` **不新增 REST code**：调用级失败复用现有白名单 `driver_call_failed`，连接 CircuitOpen 复用 `connection_failed`；真正的精细原因保留在内部 `category`/metrics/log。若未来希望新增 `HOST_UNAVAILABLE` 北向码，应作为单独兼容性变更评审。

采集 Quality 映射使用内部 category/code 推导 `Timeout / NotConnected / ProtocolError`，但 Observation 既有 schema 不变。

### 28.2 ABI v1 Adapter 的 category 映射

ABI v1 没有 category 字段。Adapter 使用稳定 code 做兼容映射：

```text
*timeout*                         → Timeout
connection_failed                → ConnectionFailed
connection_lost / not_connected  → ConnectionLost
invalid_address                  → InvalidAddress
unsupported                      → Unsupported
config_error                     → Config
其他插件调用错误                 → ProtocolViolation
```

Host process exit / panic / hard-kill 不从插件 code 推断，由 Runtime 直接生成 `HostUnavailable / DriverCrashed`。该兼容表需要单元测试固化，直到 ABI v1 删除。

## 29. Startup Preflight

每个 Device 启动前：

```text
1. driver exists
2. profile exists
3. profile.driver_id matches
4. driver connection config valid
5. every profile address validates
6. profile capability requirements satisfied
7. write properties require Write API
8. commands require Command API
9. subscription profiles require Subscription API
```

MVP 规则：任一上述配置/契约 Preflight 失败，**Collector startup fail-fast**。

```text
no partial start
no silent device disable
```

未来若引入 `device.disabled_due_to_config`，必须作为新的显式配置模式和兼容性决策；不属于 Runtime V2 MVP。

---

## 30. Observability V2

新增指标：

```text
driver_host_up
driver_host_restarts_total
driver_host_crashes_total

device_session_state
device_reconnect_total
device_circuit_breaker_open_total

device_request_queue_depth
device_request_queue_wait_ms
device_request_duration_ms
device_request_timeout_total

poll_stale_dropped_total
poll_coalesced_total

control_queue_wait_ms

driver_request_hard_kill_total
driver_execution_model
driver_ipc_inflight
driver_ipc_roundtrip_ms
```

日志统一字段：

```text
component
driver_id
device_id
host_id
correlation_id
ipc_request_id
session_state
error_code
duration_ms
```

---

## 31. Driver 测试等级

以后统一定义：

| Level | 说明 |
|---|---|
| L1 | Codec / golden bytes |
| L2 | ForgeLink Mock fault injection |
| L3 | Independent implementation |
| L4 | Official simulator |
| L5 | Real hardware |

Release Metadata：

```yaml
compatibility:
  s7-1200:
    simulator: passed
    simulator_name: PLCSIM Advanced
    hardware: pending

  s7-1500:
    simulator: passed
    hardware: pending
```

---

## 32. Mock 职责

Mock 主要负责：

```text
timeout
disconnect
wrong request id
malformed frame
partial frame
access denied
protocol error
late response
reconnect
```

Mock 不作为“协议标准”。

正常协议 golden 应优先来自：

```text
PCAP
官方 simulator
独立实现
真实设备
```

---

## 33. S7 专项 Protocol Reset

PR #29 证明当前 S7 实现与 Mock/真实端点之间确实存在协议布局差异，因此支持“需要 Protocol Reset”的方向；但 PR #29 的 DWORD 双语义兼容、Mock 已知差异和 ignored real-device test **不等同于 §45 完整验收**。

在 Runtime V2 实施期间，S7 只做协议正确性与迁移所需修改，不继续扩展新功能。

必须先完成：

```text
AnyTransportSize
≠
DataTransportSize
```

拆分。

建议：

```rust
#[repr(u8)]
enum AnyTransportSize {
    Bit   = 0x01,
    Byte  = 0x02,
    Char  = 0x03,
    Word  = 0x04,
    Int   = 0x05,
    Dword = 0x06,
    Dint  = 0x07,
    Real  = 0x08,
}

#[repr(u8)]
enum DataTransportSize {
    Bit           = 0x03,
    ByteWordDword = 0x04,
    Int           = 0x05,
    Dint          = 0x06,
    Real          = 0x07,
    OctetString   = 0x09,
}
```

然后依次重建：

```text
Setup request
Setup response
Read Var
Write Var
length semantics
padding
batch grouping
mock
golden
```

最低 canonical 规则：

```text
Setup Communication parameter (8 bytes total)
0      Function = 0xF0
1      Reserved
2..3   Max AMQ Calling
4..5   Max AMQ Called
6..7   PDU Length

Write/Read data transport namespace
BIT                  → 0x03, length in bits
BYTE/WORD/DWORD raw  → 0x04, length in bits
```

默认 parser 必须按 DataTransportSize 严格计算 payload length；非标准模拟器兼容行为只能放到显式 quirk mode，不能通过“单项时吞掉全部 remaining bytes”进入默认路径。

---

## 34. S7 Batch 策略

第一阶段优先正确性。

分组：

```text
(area, db, S7Type)
```

禁止 BYTE / WORD / DWORD 混组。

后续优化可采用：

```text
统一 byte span read
```

例如：

```text
DB1.DBW0
DB1.DBD4
DB1.DBB9
```

统一读：

```text
DB1 bytes 0..10
```

然后本地按 offset 解码。

---

## 35. Workspace 目标结构

```text
crates/
  observation-model/
  profile-engine/
  domain-model/
  device-manager/          # 保留 registry / binding / mapping

  driver-contract/         # Rust semantic contract
  driver-abi/              # C ABI v1/v2 types
  driver-sdk/              # Transitional compatibility facade
  driver-package/
  driver-host-protocol/
  driver-runtime/
  native-driver-loader/

  control-engine/
  poll-engine/
  data-pipeline/
  local-buffer/
  mqtt-client/
  rest-api/
  metrics/
  diagnostics/

  modbus-mock/             # test infrastructure
  s7comm-mock/
  mc-mock/
  etherip-mock/

apps/
  collector/
  driver-host/
  bench/

drivers/
  modbus/
  s7comm/
  ethernet-ip/
  mitsubishi-mc/
```

Mock crates 属于测试基础设施，不是生产依赖。`driver-sdk` 在最后一个 ABI v1 Driver 完成迁移后再删除，避免一次性破坏现有四个 Driver。

## 36. 依赖方向约束

目标态依赖：

```text
collector
  ├── device-manager
  └── driver-runtime
         ↓
  driver-host-protocol
         ↓ IPC boundary
apps/driver-host
  ├── driver-host-protocol
  ├── driver-contract
  ├── driver-abi
  └── native-driver-loader
```

规则：

- Collector 禁止依赖 `native-driver-loader`、`libloading`、`driver_abi::DriverApiV1/V2`。
- `driver-contract` 禁止依赖 `driver-abi` / Tokio / libloading。
- `device-manager` 目标态禁止创建 Native Handle；只通过 `DeviceHandle` 发起设备操作。
- `poll-engine` / `control-engine` 禁止依赖 Native Driver 类型。
- 现有 Driver 可以在迁移期经 `driver-sdk` re-export 使用 ABI v1；新 Driver/新 Runtime 代码不得新增这种依赖。

## 37. 实施阶段

### 37.1 Phase 0 — RFC / Decision Gates

新增：

```text
docs/architecture/runtime-v2.md
```

必须在该 RFC 中明确：

- D1：是否批准“所有生产 Driver 默认经 Host”的政策收紧；
- D2：是否批准 Runtime V2 期间冻结新协议路线图；
- `Edge Core` 与 `Collector / Edge / Manager` 的术语关系；
- 本文的 Target / Transitional 生效点。

在 D2 未批准前，不得把路线图冻结写进 AGENTS.md 成为既成事实。

### 37.2 Phase 1 — Contract / ABI Split + Driver Package

新增：

```text
driver-contract
driver-abi
driver-package
```

并把 `driver-sdk` 改为兼容 re-export 外壳。

实现：

- Manifest v2 schema / scanner / platform artifact selector；
- duplicate id / hash / invalid manifest 检查；
- 把 Rust semantic types 与 C ABI types 拆开；
- 保证四个现有 Driver 不需要一次性重写。

验收：四个 Driver 均可继续编译；Package scanner 能发现并校验四个包。

### 37.3 Phase 2 — Multi Driver Registry

Collector 配置增加 `drivers.directories`，新增 `DriverRegistry`。`Transitional`：仍允许 legacy direct ABI v1 runtime。

验收：同一 Collector 同时注册至少两个不同 Driver；旧 `driver:` 配置可转换并打印 deprecated。

### 37.4 Phase 3 — Startup Preflight

实现 connection / address / capability / profile-driver binding 预检。MVP 任一配置错误 fail-fast。

### 37.5 Phase 4 — DeviceActor + device-manager Adapter

新增 `driver-runtime::DeviceActor / SessionSupervisor / RequestQueue`。

- `device-manager` 保留 registry/binding/mapping；
- Session ownership 迁入 driver-runtime；
- 第一版 Actor 内可继续使用 ABI v1 `DriverSession` Adapter。

验收：Poll / Control 不再直接持有 Driver Mutex；所有设备操作经过 DeviceActor；Quality 失败映射保持现有语义。

### 37.6 Phase 5 — Poll / Control Runtime Cutover

Poll Engine 删除 driver `spawn_blocking`、Mutex ownership 和 driver timeout ownership；增加 coalesce / stale drop。

ControlExecutor Adapter 改走 `DeviceHandle`，并增加 §19.1 的内部 `ControlExecutionContext` 传递 correlation/deadline/cancel；既有 `Failed / Indeterminate / cooldown / whitelist` 语义不得回退。ControlEngine 自己的 control queue 不迁入 DeviceActor。

### 37.7 Phase 6 — Driver Host Protocol

新增 `driver-host-protocol`，用 Fake Host 跑通：

```text
Hello/Auth
Descriptor
CreateSession
AcceptedForExecution
Read / Write / Execute
Shutdown
```

验收包括 UDS/Named Pipe 权限、auth token、8 MiB frame 上限、malformed/unknown schema、request multiplexing 与 `correlation_id` / `ipc_request_id` 配对。

### 37.8 Phase 7 — Driver Host + ABI v1 Adapter

新增 `apps/driver-host`，把现有 loader 演进为 `native-driver-loader`。

至少跑通 ABI v1 Modbus create/read/write，并完成 §25.1 的 struct-size/buffer/create/thread-affinity/no-unload 加固。ABI v1 同步函数使用 bounded dedicated worker；真正 `AsyncCancelable` adapter 不在 ABI v1 阶段伪造，留到 Phase 11 的 `ASYNC_EXECUTION_V1`。

### 37.9 Phase 8 — Core 切换到 IPC

Collector 默认路径：

```text
DriverRegistry → HostSupervisor → HostClient
```

完成后：

- 默认 production runtime-v2 feature set 的 Collector 依赖树中不存在 `libloading` / `native-driver-loader`；
- `legacy-native-runtime` 仅作为默认关闭的 Transitional feature 保留一个兼容版本；
- 若 D1 已批准，生产配置禁止绕过 Host。

### 37.10 Phase 9 — Crash / Hang Isolation

增加测试 Driver：

```text
panic
segfault
abort
sleep forever
exit()
```

验收：Collector/MQTT/WAL/REST 与其他 Host 不退出；`BlockingUninterruptible` 在 `per_device` 下可 hard-kill；单个 bounded session timeout 不得无条件 kill 整个 per-driver Host；在途控制按 §23.3 正确结算。真正 async session 的并发/cancellation 验收在 Phase 11/12 完成。

### 37.11 Phase 10 — S7 Protocol Reset

在 ABI v1 Host Adapter 上先完成 §33 / §34 / §45：transport-size 分层、Setup/Read/Write、batch、canonical mock、independent golden。这样协议正确性不依赖 ABI v2 重写。

### 37.12 Phase 11 — ABI v2

实现 `DriverDescriptorV2 / query_interface / binary identity / per-call error`，同时保留 AbiV1Adapter / AbiV2Adapter。

验收：Host 同时加载 v1/v2；Manifest ↔ Binary 身份不一致拒绝；Unsupported interface 返回 null。

### 37.13 Phase 12 — Driver ABI v2 迁移

顺序：

```text
1. Modbus
2. S7
3. Mitsubishi MC
4. EtherNet/IP
```

S7 的协议 Reset 已在 Phase 10 完成；Phase 12 只做 ABI/runtime 迁移，不再次混入大规模协议重写。至少一个 ForgeLink 自研网络 Driver 在本 Phase 迁移到 `ASYNC_EXECUTION_V1`，用于验证真正的 async-cancelable session 模型。

## 38. PR 拆分

建议严格拆成：

| PR | 内容 |
|---|---|
| PR-1 | Runtime V2 RFC / D1-D2 decisions / terminology |
| PR-2 | `driver-contract` + `driver-abi` split + `driver-sdk` facade |
| PR-3 | `driver-package` + Manifest v2 schema + four manifests/package scripts migration |
| PR-4 | Multi Driver Registry |
| PR-5 | Startup Preflight |
| PR-6 | DeviceActor / SessionSupervisor + device-manager adapter |
| PR-7 | Poll → DeviceActor + Quality regression |
| PR-8 | Control → DeviceActor + Indeterminate regression |
| PR-9 | Driver Host Protocol + IPC security |
| PR-10 | `forgelink-driver-host` + ABI v1 Host Adapter |
| PR-11 | Core IPC cutover |
| PR-12 | Host crash / hang recovery + in-flight settlement |
| PR-13 | S7 Protocol Reset |
| PR-14 | ABI v2 Base Descriptor / query_interface / ASYNC_EXECUTION_V1 |
| PR-15 | Modbus ABI v2 |
| PR-16 | S7 ABI v2 integration |
| PR-17 | MC migration |
| PR-18 | EtherNet/IP migration |

禁止压缩成一个巨大 PR；尤其禁止把 S7 Protocol Reset 与 ABI v2 Base Descriptor 放进同一 PR。

## 39. 每个 PR 的最低门禁

所有 PR：

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

涉及 Runtime 的 PR 必须额外：

```text
architecture tests
fault tests
shutdown tests
```

---

## 40. 架构测试建议

增加测试确保：

```text
default production collector cannot depend on native-driver-loader
collector runtime-v2 path cannot import driver_abi::{DriverApiV1, DriverDescriptorV2}
driver-contract cannot depend on driver-abi
device-manager cannot import NativeDriver after Phase 4
poll-engine cannot import NativeDriver
control-engine cannot import NativeDriver
```

可通过：

- cargo metadata 检查脚本；
- workspace dependency lint；
- CI grep / deny；
- 后续引入 `cargo-deny` 自定义检查。

---

## 41. 兼容策略

### 41.1 Collector Config

旧：

```yaml
driver:
```

保留一个过渡版本。

启动时转换：

```text
legacy single driver
→ synthetic driver package registration
```

日志：

```text
COLLECTOR_CONFIG_DRIVER_DEPRECATED
```

下一 major 删除。

---

### 41.2 ABI

继续支持：

```text
ABI 1.x
```

但只允许在 Driver Host 中。

新增：

```text
ABI 2.x
```

逐 Driver 迁移。

---

### 41.3 Profile / Domain

目标：

```text
0 breaking changes
```

---

### 41.4 MQTT / REST / Observation

目标：

```text
0 breaking changes
```

### 41.5 Driver Manifest

当前无 `schema_version` 的 Manifest v1 仅作为迁移输入；Package Runtime 的正式目标格式是 Manifest v2。四个现有 Driver 的 v2 Manifest 与打包脚本在 Phase 1 一次性迁移，避免长期维护 v1/v2 两套生产 package 语义。

---

## 42. 回滚策略

每个阶段必须可单独回滚。

在 Phase 8 Core 完成 IPC 切换之前允许：

```text
legacy direct native runtime
```

Phase 8 之后允许保留一个版本的 Transitional feature：

```text
legacy-native-runtime
```

该 feature **默认关闭、正式生产包不启用**；架构测试的“Collector 不依赖 native-driver-loader/libloading”指默认生产 feature set。若 CI 使用 `--all-features`，必须单独标识 legacy 路径，不把它误判为目标态依赖回归。

一个兼容版本后删除该 feature；删除后再把“任何 feature set 都不存在 direct loader”提升为最终硬门禁。

---

## 43. 风险列表

### 43.1 R1：IPC 增加延迟

本地 UDS / Named Pipe 预期比设备网络 I/O 小，但不能把这一点当成无条件事实。

验收必须增加 IPC micro-benchmark 和真实 Poll e2e 对比，记录 p50/p95/p99 与 CPU 开销。只有数据表明 IPC 成为显著瓶颈后才考虑 CBOR/共享内存等优化；在此之前优先正确性、隔离和可调试性。

### 43.2 R2：进程数增加

`Proposed(D1)` 推荐自研可控 Driver 使用 `per_driver` 控制进程数；`BlockingUninterruptible` 必须 `per_device`。部署若要求严格单连接进程故障隔离，可把特定 Driver 提升到 `per_device`。

### 43.3 R3：Runtime V2 迁移过大

通过 ABI v1 Host Adapter 避免同时重写 Driver。

### 43.4 R4：DeviceActor 复杂度提升

复杂度本来已经存在，只是当前隐藏在：

```text
Mutex
spawn_blocking
Poll timeout
Control queue
Driver reconnect
```

V2 将隐式复杂度显式化，但必须避免把 control-engine 已有业务队列复制到 DeviceActor。

### 43.5 R5：把进程隔离误当安全沙箱

Driver Host 能隔离 crash/hang，但如果 Host 与 Collector 使用同一 OS 用户，它不能防止恶意 Driver 主动读取该用户可访问的文件/内存外资源。MVP 必须如实标记为 availability isolation；不可信第三方 Driver 需要后续最小权限账户、seccomp/AppContainer/签名等额外控制。

### 43.6 R6：`per_driver` 的同协议 Blast Radius

`per_driver` 降低进程数，但 Host crash 会让该 Driver 的全部 Session 一起 Recovering。对 `BlockingUninterruptible` 或“任何单连接事故都不能影响其他连接”的场景，必须提升到 `per_device`。不得在 SLA/文档中把 `per_driver` 描述成单连接完全隔离。

---

## 44. Runtime V2 最终验收标准

以下全部满足才允许标记 Runtime V2 Complete：

- [ ] Collector 可同时运行 Modbus + S7 + MC + EtherNet/IP。
- [ ] Collector 配置不再重复声明 Driver Manifest。
- [ ] `driver.json` 是唯一 Driver package 元数据来源。
- [ ] 默认生产 Collector runtime-v2 路径不直接 `dlopen()`；legacy Transitional feature 按 §42 到期删除。
- [ ] 默认生产 Collector feature set 不依赖 `native-driver-loader`；Transitional legacy feature 在约定版本后删除。
- [ ] 每台设备只有一个 DeviceActor 调度入口。
- [ ] Poll 不直接调用 Driver。
- [ ] Control 不直接调用 Driver；control-engine 仍是唯一 Control 业务队列，DeviceActor 不复制第二套 Control FIFO/priority。
- [ ] 不再以 `Arc<Mutex<DriverSession>>` 作为上层调度模型。
- [ ] Poll 支持 stale drop。
- [ ] Poll 支持 coalesce。
- [ ] ControlEngine 保留现有风险优先级；DeviceActor 仅负责 Control/Poll/Admin 的 origin arbitration。
- [ ] Poll/Control/Admin 的有效请求都有 deadline；Control 原 deadline/cancel 通过 §19.1 传到 DeviceActor。
- [ ] Core `Instant` 不跨进程序列化；Host 只接收 `remaining_budget_ns`。
- [ ] `correlation_id: String` 与 `ipc_request_id: u64` 语义分离。
- [ ] Host IPC 使用 UDS/Named Pipe 本地 endpoint，具备 ACL/权限与启动 token 认证。
- [ ] IPC frame 有明确 schema/version/max-size，并在大内存分配前拒绝超限。
- [ ] Core Driver 调用链路使用 async IPC，不以 `spawn_blocking + Mutex` 作为正常运行模型。
- [ ] DeviceActor ingress 与内部 pending 容器均有界；不能只限制 mpsc 而留下 unbounded 内部队列。
- [ ] Poll backlog 可 coalesce，不无限积压历史 tick。
- [ ] Driver 明确声明 `ExecutionModel`，且 ABI v1 同步函数不能误标为 `AsyncCancelable`。
- [ ] 至少一个 Pure Rust network Driver 通过 C-safe `ASYNC_EXECUTION_V1` 支持真正 async-cancelable session；不得跨 ABI 暴露 Rust Future。
- [ ] Blocking Vendor SDK 使用 dedicated worker thread 或更强隔离。
- [ ] `BlockingUninterruptible` 位于 `per_device`/等价独立故障域，可通过 Host hard-kill 回收而不影响其他连接。
- [ ] Device Session 有显式状态机，并定义 Recovering/CircuitOpen 对 Observation Quality 的映射。
- [ ] Driver hang 不会永久锁死 Core；hard-kill 不会因单设备普通 timeout 误伤不相关 Host。
- [ ] Driver crash 不会导致 Collector crash。
- [ ] Driver Host crash 可恢复；Read 与 Write/Execute 的在途请求按 §23.2/§23.3 正确结算。
- [ ] Host restart/backoff/crash-loop cutoff 有指标，超过阈值进入 Failed 而非无限重启。
- [ ] ABI v2 Driver 完成 Manifest ↔ Binary Descriptor 身份交叉验证；ABI v1 迁移期至少完成 artifact hash + ABI/entry 校验。
- [ ] Startup 验证所有 Profile 地址。
- [ ] Startup 验证 capabilities。
- [ ] ABI v1 可以通过 Host 运行，完成 §25.1 FFI 安全加固，并明确其 binary identity 校验弱于 ABI v2。
- [ ] ABI v2 使用 `query_interface()`。
- [ ] ABI v2 不使用 handle-global last-error。
- [ ] Mock 不再是协议正确性的唯一 Oracle。
- [ ] Driver Release 有 compatibility matrix。
- [ ] S7 完成独立协议验证。
- [ ] 至少一类 Driver 完成真实硬件验证。

---

## 45. S7 单独验收

S7 不允许只凭 ForgeLink Mock 标记完成。

最低要求：

- [ ] `AnyTransportSize` / `DataTransportSize` 已拆分。
- [ ] Setup request 使用正确结构。
- [ ] Setup response 使用正确结构。
- [ ] Read response length 严格按 Data Transport 语义。
- [ ] Write request data transport 正确。
- [ ] BYTE / WORD / DWORD Batch 不错误混组。
- [ ] Mock 默认生成标准布局。
- [ ] malformed compatibility 作为单独 quirk test。
- [ ] golden packet 来自独立来源。
- [ ] Snap7 differential test 通过。
- [ ] PLCSIM Advanced 在其实际支持的 CPU/服务范围内验证通过；不得据此外推所有 S7-300/400/legacy 行为。
- [ ] PCAP 固化并记录来源、CPU family/firmware 或独立实现版本。
- [ ] 真实 Siemens PLC 验证后再标 `hardware-validated`；localhost endpoint 只有在明确记录其后端确为物理设备时才算 hardware validation。
- [ ] PR #29 的兼容 fallback 不作为 Protocol Reset 完成证明；默认 parser/mock 必须回到 canonical model。

---

## 46. 实际开发顺序

与 §37 / §38 唯一一致的开发顺序：

```text
Runtime V2 RFC / D1-D2
      ↓
Contract + ABI split
      ↓
Driver Package
      ↓
Multi Driver Registry
      ↓
Startup Preflight
      ↓
DeviceActor + device-manager adapter
      ↓
Poll / Control cutover
      ↓
Driver Host Protocol + IPC security
      ↓
Driver Host + ABI v1 Adapter
      ↓
Core IPC cutover
      ↓
Crash / Hang Recovery
      ↓
S7 Protocol Reset
      ↓
ABI v2
      ↓
Driver ABI v2 migration
```

关键顺序约束：

1. **S7 Protocol Reset 在 ABI v2 之前。** 先把协议模型做对，再做 ABI 迁移，避免两个风险面混在同一 PR。
2. **Core `dlopen` 禁令在 Phase 8 cutover 后生效。** 此前 direct ABI v1 只是 Transitional。
3. **D1 / D2 必须在 Phase 0 形成明确记录。** 不允许实现过程中悄悄把 Proposed 变成 Normative。

## 47. Proposed D2 — Runtime V2 开发冻结

以下是**建议的路线图调整，不是已批准 Normative 规则**。

建议 Runtime V2 核心 cutover 前暂停新增：

```text
Omron FINS
FANUC FOCAS
Beckhoff ADS
更多 PLC Driver
ABI v1 新能力
```

允许：

```text
严重 bug fix
安全修复
协议正确性修复
测试基础设施
Runtime V2
```

批准方式：Phase 0 RFC / 产品路线图明确记录 D2。D2 未批准前，不得自行修改 AGENTS.md 把上述协议从既有路线图中删除；可以在具体 PR 中基于风险控制暂缓扩展，但不能宣称全局冻结已经生效。

## 48. Definition of Done

任一 Runtime V2 功能只有同时满足：

```text
设计文档更新
代码
单元测试
集成测试
故障测试
指标
日志
迁移说明
边界/状态/错误语义回归说明
```

才视为完成。

禁止仅以：

```text
cargo test green
```

作为架构完成标准。

---

## 49. 九条最终架构原则

Runtime V2 完成后，ForgeLink 必须满足：

> **Core 不理解工业协议。**

> **Driver 不理解业务领域。**

> **Profile 不管理连接。**

> **Poll / Control 不直接调用 Driver。**

> **高风险 Native / Vendor Driver 不进入 Core 故障域；若 D1 批准，则所有生产 Driver 均经 Host。**

> **Mock 不定义协议真相。**

> **Async 是调度模型，不是崩溃隔离模型。**

> **可控网络协议优先使用纯 Rust async session。**

> **不可中断 Native / Vendor SDK 必须位于 `per_device` 或等价可独立回收的 Driver Host 故障域中。**

---

## 50. 第一批可立即创建的任务

建议立即建立以下 Issues / Tasks：

### 50.1 Runtime

- [ ] 明确 `device-manager` 保留职责与 Session ownership 迁移边界

- [ ] 创建 `docs/architecture/runtime-v2.md`
- [ ] 新建 `driver-contract` / `driver-abi` 并把 `driver-sdk` 改为兼容 facade
- [ ] 新建 `driver-package`
- [ ] 定义 Manifest v2 schema
- [ ] 升级四个现有 driver.json + package scripts 到 Manifest v2
- [ ] Collector 支持 `drivers.directories`
- [ ] 新建 `DriverRegistry`
- [ ] 加入 multi-driver e2e
- [ ] 加入 startup address preflight
- [ ] 新建 `driver-runtime`
- [ ] 实现 DeviceActor
- [ ] Poll Engine 改走 DeviceActor
- [ ] Control Engine 改走 DeviceActor（保留原 Control queue）
- [ ] ControlExecutor 增加内部 execution context（correlation/deadline/cancel）
- [ ] DeviceActor mailbox 改为 bounded `mpsc`
- [ ] 为 Core 请求增加进程内 absolute deadline，并在 IPC 边界转换为 remaining budget
- [ ] Poll 增加 latest/coalesce 语义
- [ ] 定义 `ExecutionModel`

### 50.2 Driver Host

- [ ] 定义 Host Protocol（JSON v1 / 8 MiB frame cap / schema version）
- [ ] 定义 `correlation_id` / `ipc_request_id` / `host_instance_id` / session scope 与 `AcceptedForExecution`
- [ ] 实现 Unix Socket transport
- [ ] 实现 Windows Named Pipe transport
- [ ] 实现 UDS mode / Named Pipe ACL / per-host auth token
- [ ] 新建 `forgelink-driver-host`
- [ ] 移动 Native loader 到 Host
- [ ] 实现 HostSupervisor
- [ ] 实现 host crash restart + bounded backoff/crash-loop cutoff
- [ ] 实现 Read 与 Control 在途请求 crash settlement + no-auto-replay rules
- [ ] 实现 Driver Runtime 有界 shutdown / hard-kill fallback
- [ ] 实现 hard execution deadline
- [ ] HostClient 使用 async multiplexing
- [ ] Blocking SDK 支持 dedicated worker thread
- [ ] `BlockingUninterruptible` hard-kill fault test

### 50.3 ABI

- [ ] ABI v1 Host Adapter + header-first/bounded-copy/buffer sanity hardening
- [ ] DriverDescriptorV2
- [ ] `query_interface`
- [ ] `ASYNC_EXECUTION_V1` callback/cancel ABI + callback lifetime contract
- [ ] Host 将 async completion 包装成 Future
- [ ] 至少一个 Pure Rust Driver 使用 Tokio async I/O 完成迁移
- [ ] per-call error
- [ ] binary identity

### 50.4 S7

- [ ] 拆分 Any/Data Transport Size
- [ ] 修 Setup request
- [ ] 修 Write data transport
- [ ] 删除默认 DWORD malformed fallback
- [ ] 修 batch grouping
- [ ] 重建标准 Mock
- [ ] Snap7 differential
- [ ] PLCSIM Advanced
- [ ] PCAP golden
- [ ] hardware matrix

---

## 51. 建议的里程碑

### 51.1 Milestone A — Core Runtime Stabilized

```text
Contract / ABI split
Driver Package
Multi Driver
Preflight
DeviceActor
Poll / Control cutover
```

目标：Core 调度模型收敛，`device-manager` 职责边界稳定。

### 51.2 Milestone B — Fault Isolation

```text
Driver Host Protocol + IPC security
Driver Host
Host Supervisor
ABI v1 Adapter
Core IPC cutover
Crash / Hang Recovery
```

目标：危险 Driver 不再拖垮 Collector；不可中断调用可在正确隔离粒度下进程级回收；控制确定性不因 Host crash 被破坏。

### 51.3 Milestone C — Protocol Correctness

```text
S7 Protocol Reset
canonical mock
golden / differential / simulator / PCAP
```

目标：先把协议模型做对，Driver 正确性不依赖自研 Mock。

### 51.4 Milestone D — Driver Contract V2

```text
ABI v2
query_interface
per-call error
binary identity
```

目标：Driver 扩展机制长期可维护。

### 51.5 Milestone E — Driver Migration / Validation

```text
Modbus / S7 / MC / ENIP ABI v2 migration
compatibility matrix
hardware validation where available
```

目标：完成 Transitional ABI v1 / driver-sdk facade 的退出条件。

## 52. 最终目标

Runtime V2 完成后，ForgeLink 应从当前：

```text
Collector
  + Native plugins
  + Mutex sessions
  + blocking timeout wrappers
```

演进为（这里的 `Edge Core` 是 Collector/Edge 复用的内部核心层，不是新的 Runtime Role）：

```text
Collector Role
  + Edge Core libraries
  + Tokio async scheduling
  + explicit Device Runtime / DeviceActor
  + supervised killable Driver Hosts
  + async IPC multiplexing
  + versioned Driver Contract
  + protocol-independent Profile/Domain
```

这不是为了“做得更复杂”，而是把工业 Edge Runtime 本来就存在的：

```text
故障域
会话状态
调度
超时
优先级
协议隔离
插件生命周期
兼容性
```

从隐式行为变成显式架构。

只有完成这一层收敛后，ForgeLink 才适合继续扩展更多工业协议、CNC Vendor SDK、OPC UA、Edge Server 与 Manager。
