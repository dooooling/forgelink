# Rust 工业 IoT 采集平台架构设计方案

> **文档状态：v1 架构规范草案（已收敛）**  
> 本文中的 `Normative` 章节是实现契约；示例仅用于说明。Driver 只负责协议与原始结果，Device Profile 负责型号映射，Domain Model 负责业务语义，最终由 Core 生成 Observation。  
> 时间统一使用 UTC Unix Epoch 纳秒；所有北向写入与动作命令统一经过 Control Engine。

## 1. 目标

设计一个基于 Rust 的工业 IoT / Edge 采集平台，面向 PLC、CNC、机器人、仪表等工业设备。

核心目标：

- 支持 Windows x64、Linux x64、Linux ARM64。
- Driver 插件化，后续新增驱动不需要修改主程序。
- PLC、CNC 等设备底层协议和数据结构可以完全不同，但上层统一输出。
- 支持 Modbus、Siemens S7、EtherNet/IP、Omron FINS、Mitsubishi MC/SLMP、FANUC FOCAS 等。
- 支持 MQTT、OPC UA、REST、数据库等北向输出。
- 支持批量采集、重连、Quality、Timestamp、缓存、诊断。
- 对不稳定或闭源厂商 SDK 支持独立进程隔离。

---

# 2. 总体架构

```text
                     Northbound
          MQTT / OPC UA / REST / Database
                         |
                         v
+------------------------------------------------+
|             Unified Data Model                 |
|                                                |
| Device / Resource / Property / Observation     |
| Command / Quality / Timestamp                  |
+------------------------------------------------+
                         |
                         v
+------------------------------------------------+
|                  Edge Core                     |
|                                                |
| Device Manager                                 |
| Driver Manager                                 |
| Tag / Resource Engine                          |
| Poll Scheduler                                 |
| Cache                                          |
| Data Pipeline                                  |
| Diagnostics                                    |
+------------------------------------------------+
                         |
                         v
+------------------------------------------------+
|                  Driver SDK                    |
|             Stable C ABI / Plugin API          |
+------------------------------------------------+
      |              |              |
      v              v              v
 Siemens S7       FANUC FOCAS      Mitsubishi
 Driver           Driver           Driver
      |              |              |
      v              v              v
 PLC              CNC             PLC/CNC
```

核心原则：

> 不统一底层协议，而统一 Driver 之上的数据模型。

---

# 3. 为什么不能把所有设备都抽象成 PLC Tag

Siemens PLC 可能通过：

```text
DB1.DBD0
DB1.DBX4.0
M10.0
```

读取，而 FANUC CNC 可能通过 FOCAS API：

```text
cnc_rdposition()
cnc_statinfo()
cnc_alarm()
cnc_rdmacro()
pmc_rdpmcrng()
```

获取数据。两者底层完全不同：

```text
Siemens S7                      FANUC CNC

DB1.DBD0                        cnc_rdposition()
DB1.DBX4.0                      cnc_statinfo()
M10.0                           cnc_alarm()
     |                               |
     v                               v
 S7 Driver                       FANUC Driver
     |                               |
     +---------------+---------------+
                     |
                     v
             Raw Protocol Results
                     |
                     v
              Device Profile
                     |
                     v
               Domain Model
                     |
                     v
                Observation
```

因此最终规范是：

- Driver 内部保留协议和厂商 API 语义。
- Driver **不生成 Observation**，只返回协议层原始读取结果 `RawReadResult`。
- Device Profile 负责地址映射、缩放、单位、枚举和型号差异。
- Domain Model 负责把型号语义映射成标准 PLC/CNC/Robot/Drive 等领域路径。
- Core 在 Profile + Domain 映射完成后生成 `Observation`。

> **Normative：** 后续任何 Polling、Subscription、Event、History 数据都必须遵循 `Driver -> Raw Result -> Profile -> Domain -> Observation`，不得让 Driver 绕过 Profile/Domain 直接输出 Observation。

---

# 4. 最终规范核心模型（Normative）

本章定义实现时唯一有效的核心模型。后续示例如果与本章冲突，以本章为准。

## 4.1 基础标识与时间

```rust
pub type DeviceId = String;
pub type ResourcePath = String;
pub type PropertyPath = String;
pub type TimestampNs = i64;
```

`TimestampNs` 的语义固定为：

```text
UTC Unix Epoch nanoseconds
```

即从 `1970-01-01T00:00:00Z` 起的纳秒数。

## 4.2 Device

```rust
pub struct Device {
    pub id: DeviceId,
    pub name: String,

    pub domain: DomainKind,
    pub driver_id: String,
    pub profile_id: String,

    pub connection: DeviceConnection,
    pub enabled: bool,
    pub labels: std::collections::BTreeMap<String, String>,
}
```

```rust
pub enum DomainKind {
    Plc,
    Cnc,
    Robot,
    Drive,
    Servo,
    Meter,
    Sensor,
    Instrument,
    Machine,
    PowerDevice,
    BuildingDevice,
    Custom(String),
}
```

`DeviceConnection` 保存 Driver 所需的连接配置，但 Core 不解释协议私有字段：

```rust
pub struct DeviceConnection {
    pub config: serde_json::Value,
}
```

## 4.3 数据流边界

统一数据流固定为：

```text
Physical Device
      |
      v
Transport
      |
      v
Protocol Driver
      |
      v
RawReadResult / RawEvent
      |
      v
Device Profile
      |
      v
Semantic Property
      |
      v
Domain Model
      |
      v
Observation
```

控制流固定为：

```text
Northbound Control Request
      |
      v
Control Engine
      |
      +--> Property Write --> Profile Mapping --> Driver.write()
      |
      +--> Command Execute -> Profile Mapping --> Driver.execute()
```

北向接口不得直接调用 Driver 的写入或命令方法。

---

# 5. Resource（Normative）

Resource 表示设备内部的逻辑对象树，而不是协议地址本身。

```rust
pub struct Resource {
    pub path: ResourcePath,
    pub kind: String,
    pub display_name: String,

    pub properties: Vec<Property>,
    pub commands: Vec<CommandDescriptor>,
    pub children: Vec<ResourcePath>,

    pub metadata: std::collections::BTreeMap<String, String>,
}
```

示例：

```text
/device/siemens01/memory/db1
/device/fanuc01/axis/x
/device/fanuc01/spindle/1
/device/robot01/joint/j1
```

`Resource.path` 是平台语义路径；Driver 私有地址保存在 Profile 的属性映射中。

---

# 6. Property、DataType 与 Value（Normative）

## 6.1 Property

```rust
pub struct Property {
    pub path: PropertyPath,
    pub display_name: String,
    pub value_type: DataType,
    pub unit: Option<String>,
    pub readable: bool,
    pub writable: bool,
    pub metadata: std::collections::BTreeMap<String, String>,
}
```

## 6.2 DataType

```rust
pub enum DataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    String,
    Bytes,
    Array(Box<DataType>),
    Struct(Vec<FieldSchema>),
}

pub struct FieldSchema {
    pub name: String,
    pub data_type: DataType,
}
```

## 6.3 Value 与 FieldValue

```rust
pub enum Value {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Struct(Vec<FieldValue>),
}

pub struct FieldValue {
    pub name: String,
    pub value: Value,
}
```

`Value` 是 Profile/Domain 归一化后的平台值。协议原始值使用下一章的 `RawValue`。

---

# 7. Driver 原始结果与 Observation（Normative）

## 7.1 RawValue

Driver 完成协议解码后返回 `RawValue`，但不负责单位、缩放、业务路径和领域语义。

```rust
pub enum RawValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<RawValue>),
    Struct(Vec<RawFieldValue>),
}

pub struct RawFieldValue {
    pub name: String,
    pub value: RawValue,
}
```

## 7.2 RawReadResult

```rust
pub struct RawReadResult {
    pub item_id: u64,
    pub value: Option<RawValue>,

    pub source_timestamp_ns: Option<TimestampNs>,
    pub received_timestamp_ns: TimestampNs,

    pub protocol_quality_code: Option<i64>,
    pub error: Option<DriverErrorInfo>,
}

pub struct DriverErrorInfo {
    pub code: String,
    pub message: String,
    pub protocol_code: Option<i64>,
    pub retryable: bool,
}
```

`received_timestamp_ns` 由 Core/Driver Runtime 在收到设备结果时生成；`source_timestamp_ns` 只有设备或协议明确提供可信设备时间时才填写。

## 7.3 Observation

`Observation` 只能在 Profile + Domain 映射之后生成：

```rust
pub struct Observation {
    pub observation_id: String,
    pub device_id: DeviceId,
    pub path: PropertyPath,

    pub value: Option<Value>,
    pub quality: Quality,

    pub source_timestamp_ns: Option<TimestampNs>,
    pub ingest_timestamp_ns: TimestampNs,

    pub sequence: u64,
    pub metadata: std::collections::BTreeMap<String, String>,
}
```

示例：

```text
Driver Raw:
  item_id = 100
  value = U64(5000)

Profile:
  scale = 0.01
  unit = Hz

Domain:
  drive.output.frequency

Observation:
  value = 50.0
  quality = Good
  path = drive.output.frequency
```

Driver 不得直接构造上述 Observation。

---

# 8. 时间语义（Normative）

平台统一使用：

```text
UTC Unix Epoch nanoseconds
```

并明确区分两种时间：

```text
source_timestamp_ns
= 设备/协议提供的数据产生时间，可为空
```

```text
ingest_timestamp_ns
= Edge/Core 接受并归一化该条数据的时间，必填
```

规则：

1. 设备没有可靠时间时，`source_timestamp_ns = None`，不得伪造设备时间。
2. `ingest_timestamp_ns` 必须由平台时钟生成。
3. 北向消息可额外携带 `sent_at_ns`，但它不是 Observation 的数据时间。
4. Store-and-Forward 补传时必须保留原始 `source_timestamp_ns` 与 `ingest_timestamp_ns`，不能在补传时重写。
5. NTP/PTP 时钟异常应进入诊断指标，不应静默修改历史时间。

---

# 9. Quality 与错误语义（Normative）

Quality 分为“级别”和“原因”，协议原始质量码单独保留。

```rust
pub enum QualityLevel {
    Good,
    Uncertain,
    Bad,
}

pub enum QualityReason {
    None,
    Stale,
    Timeout,
    NotConnected,
    InvalidAddress,
    DeviceError,
    ProtocolError,
    ConfigurationError,
    Unsupported,
}

pub struct Quality {
    pub level: QualityLevel,
    pub reason: QualityReason,
    pub protocol_code: Option<i64>,
    pub message: Option<String>,
}
```

值与 Quality 的组合规则：

| 情况 | `value` | Quality |
|---|---|---|
| 正常读取 | `Some(value)` | `Good` |
| 设备返回可用但可疑值 | `Some(value)` | `Uncertain` |
| Timeout / NotConnected / 协议错误 | `None` | `Bad` |
| 使用缓存的 Last Good Value | `Some(last_good)` | `Uncertain + Stale` |

关键规则：

- `Bad/Timeout` 的新读取结果不得伪装成 Last Good Value。
- Last Good Value 属于 Cache 层；只有调用方明确允许 stale fallback 时才返回，并标记 `Uncertain/Stale`。
- Driver 的 `protocol_quality_code`、设备错误码和错误详情在映射后应尽量保留到 `Quality.protocol_code` 或诊断元数据中。

---

# 10. Driver 地址与 Profile 映射边界（Normative）

Core 的业务配置不应该直接依赖协议地址。

上层采集计划使用语义 Property：

```rust
pub struct PropertyReadRequest {
    pub id: u64,
    pub path: PropertyPath,
}
```

Device Profile 负责映射：

```text
Property Path
    |
    v
ProfileProperty.driver_address
    |
    v
DriverReadItem
```

例如：

```text
drive.output.frequency
    -> Profile
    -> 1!40001
    -> Modbus Driver
```

或者：

```text
cnc.axis.x.absolute_position
    -> Profile
    -> axis.absolute[1]
    -> FANUC Driver
```

Driver 私有地址仍由 Driver 自己解析和验证：

```rust
pub struct DriverReadItem {
    pub id: u64,
    pub address: String,
    pub expected_type: Option<DataType>,
}
```

因此：

```text
Core / Domain 不理解协议地址
Profile 保存“语义 -> Driver 地址”的映射
Driver 理解并执行该地址
```

这样未来 Profile 可以变化而无需修改 Core。

---

# 11. Siemens S7 地址模型

例如：

```text
DB10.DBD20
```

Driver 内部解析：

```rust
pub enum S7Address {
    Db {
        db: u16,
        offset: u32,
        data_type: S7Type,
    },

    Marker {
        offset: u32,
    },

    Input {
        offset: u32,
    },

    Output {
        offset: u32,
    },
}
```

然后转换为 S7 ReadVar 请求。

---

# 12. FANUC 地址模型

FANUC 不需要强行模拟 PLC 地址。

可以定义自己的逻辑 DSL：

```text
axis.absolute[1]
axis.machine[1]

spindle.speed[1]
spindle.load[1]

program.name

alarm.current

macro[100]

pmc.R100
```

Driver 内部：

```rust
pub enum FanucAddress {
    AxisAbsolute {
        axis: u16,
    },

    AxisMachine {
        axis: u16,
    },

    SpindleSpeed {
        spindle: u16,
    },

    SpindleLoad {
        spindle: u16,
    },

    Macro {
        number: u32,
    },

    Pmc {
        area: PmcArea,
        offset: u32,
    },

    ProgramName,

    Status,

    Alarm,
}
```

例如：

```text
axis.absolute[1]
```

转换成：

```rust
FanucAddress::AxisAbsolute {
    axis: 1
}
```

然后内部调用：

```text
FOCAS cnc_rdposition(...)
```

Core 完全不知道 FOCAS。

---

# 13. Capabilities 分层（Normative）

Capabilities 必须按职责拆分，禁止把协议能力、型号能力和领域能力混进同一个 `DriverCapabilities`。

## 13.1 ProtocolCapabilities - 属于 Driver

```rust
pub struct ProtocolCapabilities {
    pub read: bool,
    pub write: bool,
    pub batch_read: bool,
    pub batch_write: bool,
    pub browse: bool,
    pub polling: bool,
    pub subscription: bool,
    pub events: bool,
    pub history: bool,
}
```

这里只描述协议实现能做什么，例如 S7 是否支持写、OPC UA 是否支持 Subscription。

## 13.2 ProfileCapabilities - 属于 Device Profile

```rust
pub struct AcquisitionConstraints {
    // None = inherit Driver capability; Some(false) = explicitly disabled by this profile;
    // Some(true) = profile requires the capability and it is effective only if Driver also supports it.
    pub polling: Option<bool>,
    pub subscription: Option<bool>,
    pub events: Option<bool>,
    pub history: Option<bool>,
}

pub struct ProfileCapabilities {
    pub supported_properties: Vec<PropertyPath>,
    pub supported_commands: Vec<String>,
    pub acquisition: AcquisitionConstraints,
    pub limits: std::collections::BTreeMap<String, Value>,
}
```

这里描述具体型号能力，例如：

```text
FANUC 0i 支持哪些 FOCAS 功能
最大轴数 / 主轴数
某型号是否允许 Program Select
某变频器频率设定范围
```

## 13.3 DomainCapabilities - 属于 Domain Model

Domain 不再用 Driver bitflag 表示 `CNC_AXIS`、`CNC_TOOL` 等能力，而通过标准 Resource / Property / Command 的存在性表达，例如：

```text
cnc.axis.*
cnc.tool.*
robot.joint.*
drive.frequency.set
```

因此旧的 `PLC_MEMORY / CNC_AXIS / CNC_PROGRAM / CNC_ALARM / CNC_TOOL` Driver bitflag **废止**。

---

# 14. Command

不能把所有设备控制功能都做成 writable Tag。

建议增加 Command。

```rust
pub struct Command {
    pub id: String,
    pub name: String,
    pub parameters: Vec<CommandParameter>,
}
```

PLC：

```text
write memory
reset bit
```

CNC：

```text
program.start
program.stop
alarm.reset
```

机器人：

```text
start
stop
home
```

---

# 15. Driver Rust API（Normative）

内部 Rust Driver 契约以“原始协议结果”为边界：

```rust
pub struct AddressMetadata {
    pub canonical_address: String,
    pub raw_type: Option<DataType>,
    pub readable: bool,
    pub writable: bool,
}

pub struct DriverReadItem {
    pub id: u64,
    pub address: String,
    pub expected_type: Option<DataType>,
}

pub struct DriverWriteItem {
    pub id: u64,
    pub address: String,
    pub value: RawValue,
}

pub struct RawWriteResult {
    pub item_id: u64,
    pub success: bool,
    pub protocol_code: Option<i64>,
    pub error: Option<DriverErrorInfo>,
}

pub struct DriverCommand {
    pub command_id: String,
    pub payload: serde_json::Value,
}

pub struct RawCommandResult {
    pub success: bool,
    pub protocol_code: Option<i64>,
    pub payload: Option<serde_json::Value>,
    pub error: Option<DriverErrorInfo>,
}

pub struct DriverBrowseNode {
    pub id: String,
    pub display_name: String,
    pub address: Option<String>,
    pub has_children: bool,
    pub metadata: serde_json::Value,
}

pub type SubscriptionId = u64;

pub struct SubscriptionRequest {
    // Data-change subscriptions.
    pub items: Vec<DriverReadItem>,

    // Event subscriptions such as alarm/state-change. Empty means no vendor event class requested.
    pub event_types: Vec<String>,
    pub protocol_filter: Option<serde_json::Value>,

    pub publishing_interval_ms: Option<u64>,
}

pub enum RawEventKind {
    DataChange,
    Alarm,
    StateChange,
    Diagnostic,
    Custom(String),
}

pub struct RawEvent {
    pub subscription_id: Option<SubscriptionId>,
    pub event_id: Option<String>,
    pub kind: RawEventKind,
    pub items: Vec<RawReadResult>,
    pub payload: Option<serde_json::Value>,
    pub source_timestamp_ns: Option<i64>,
    pub sequence: Option<u64>,
    pub protocol_code: Option<i64>,
}

pub struct HistoryRequest {
    pub items: Vec<DriverReadItem>,
    pub start_time_ns: i64,
    pub end_time_ns: i64,
    pub limit: Option<u32>,
    pub continuation: Option<String>,
}

pub struct RawHistoryPage {
    pub items: Vec<RawReadResult>,
    pub continuation: Option<String>,
}
```

Driver trait：

v1 内部动态分发明确使用 `async-trait`，Core 保存 `Box<dyn Driver + Send>`，避免依赖 native `async fn trait` 的对象安全细节。

```rust
#[async_trait::async_trait]
pub trait Driver: Send {
    async fn connect(&mut self, config: &DeviceConnection) -> Result<(), DriverErrorInfo>;
    async fn disconnect(&mut self) -> Result<(), DriverErrorInfo>;

    fn protocol_capabilities(&self) -> ProtocolCapabilities;
    fn validate_address(&self, address: &str) -> Result<AddressMetadata, DriverErrorInfo>;

    async fn read(&mut self, items: &[DriverReadItem]) -> Result<Vec<RawReadResult>, DriverErrorInfo>;
    async fn write(&mut self, items: &[DriverWriteItem]) -> Result<Vec<RawWriteResult>, DriverErrorInfo>;
    async fn execute(&mut self, command: &DriverCommand) -> Result<RawCommandResult, DriverErrorInfo>;
    async fn browse(&mut self, path: Option<&str>) -> Result<Vec<DriverBrowseNode>, DriverErrorInfo>;

    async fn subscribe(
        &mut self,
        request: &SubscriptionRequest,
        sink: tokio::sync::mpsc::Sender<RawEvent>,
    ) -> Result<SubscriptionId, DriverErrorInfo>;

    async fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> Result<(), DriverErrorInfo>;

    async fn query_history(&mut self, request: &HistoryRequest) -> Result<RawHistoryPage, DriverErrorInfo>;
}
```

实现规则：

- `ProtocolCapabilities.subscription == true` 时，`subscribe/unsubscribe` 必须可用。
- `ProtocolCapabilities.events == true` 时，事件同样通过 `subscribe()` 建立订阅；`SubscriptionRequest.event_types/protocol_filter` 描述事件过滤，事件通过 `RawEvent` sink 推送，`RawEvent.kind` 区分 DataChange/Alarm/StateChange 等。
- `ProtocolCapabilities.history == true` 时，`query_history` 必须可用。
- `ProtocolCapabilities.browse == true` 时，`browse` 必须可用。
- capability 为 false 时，对应方法必须返回标准 `Unsupported` 错误，不能 panic。
- Native Plugin 的 C ABI callback 由 `driver-loader` 适配成 `tokio::mpsc::Sender<RawEvent>`，Driver trait 本身不暴露 C callback。

Profile Engine 负责把语义 Property/Command 映射成 Driver 请求；Driver 不知道 `cnc.spindle.1.speed` 这类领域路径。

Polling、Subscription、Event 和 History 均先产生 `RawReadResult` / `RawEvent` / `RawHistoryPage`，再经过 Profile + Domain 生成 Observation。

---

# 16. Driver 插件 ABI 总则（Normative）

动态插件采用：

```text
Rust Core
   |
   v
Stable C ABI v1
   |
   +-- Rust Driver
   +-- C Driver
   +-- C++ Driver
```

Rust 插件：

```toml
[lib]
crate-type = ["cdylib"]
```

唯一入口：

```rust
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1()
    -> *const DriverApiV1
{
    &DRIVER_API_V1
}
```

ABI 边界禁止直接暴露：

```text
Rust String / Vec
Rust enum 默认布局
Box<dyn Trait>
Future
Result<T, E>
Rust panic unwind
```

所有跨 ABI 类型必须 `#[repr(C)]` 或由明确的 `ptr + len` 字节/字符串结构组成。

---

# 17. Driver ABI v1 详细契约（Normative）

## 17.1 字符串与数组

字符串统一为 **UTF-8**，不要求 `NUL` 结尾：

```rust
#[repr(C)]
pub struct FfiStr {
    pub ptr: *const u8,
    pub len: usize,
}
```

规则：

- `len > 0` 时 `ptr` 必须非空。
- `len == 0` 时允许 `ptr = null`。
- `FfiStr` 默认是 borrowed，仅在当前函数调用期间有效。
- 数组统一使用 `ptr + len`，禁止 sentinel 结束方式。

```rust
#[repr(C)]
pub struct FfiSlice<T> {
    pub ptr: *const T,
    pub len: usize,
}
```

## 17.2 ABI 请求元素

ABI v1 不直接暴露 Rust `DataType` / `RawValue` 枚举布局，而使用稳定整数 tag + bytes：

```rust
#[repr(C)]
pub struct FfiReadItem {
    pub id: u64,
    pub address: FfiStr,
    pub expected_type: u32,
}

#[repr(C)]
pub struct FfiWriteItem {
    pub id: u64,
    pub address: FfiStr,
    pub value_type: u32,
    pub value_bytes: FfiStr,
}
```

`expected_type/value_type` 的数值映射由 ABI v1 固定；`value_bytes` 的具体标量编码由对应 type 规定，复杂结果统一通过带 schema 的 UTF-8 JSON envelope 返回。

### 类型 Tag 表（ABI v1 固定）

| Tag | 数值 | 说明 |
|---|---|---|
| `TAG_UNKNOWN` | 0 | 未指定（线格式哨兵值，不是合法的 `TypeTag`） |
| Bool | 1 | 1 字节 |
| I8 | 2 | 1 字节 |
| I16 | 3 | 2 字节 |
| I32 | 4 | 4 字节 |
| I64 | 5 | 8 字节 |
| U8 | 6 | 1 字节 |
| U16 | 7 | 2 字节 |
| U32 | 8 | 4 字节 |
| U64 | 9 | 8 字节 |
| F32 | 10 | IEEE-754 binary32，4 字节 |
| F64 | 11 | IEEE-754 binary64，8 字节 |
| String | 12 | UTF-8 |
| Bytes | 13 | 原样字节 |
| Array | 14 | 复杂类型，仅允许作为 `expected_type` 提示 |
| Struct | 15 | 复杂类型，仅允许作为 `expected_type` 提示 |

映射规则：

- `expected_type = 0`（`TAG_UNKNOWN`）表示未指定类型，是合法输入；
  Core/Plugin 统一通过 `Option<DataType> ↔ u32` 转换：`None → 0`、`0 → None`。
- `Array`/`Struct` 只能作为 `expected_type` 提示（元素/字段 schema 由 Profile
  提供），缺少 schema 时无法还原为完整 `DataType`。
- 增删改 Tag 属于 ABI 破坏性变更，必须升级 ABI major（§18）。

### value_bytes 标量编码（ABI v1 固定）

- 整数：定宽**小端序**（little-endian），字节数由 Tag 决定且必须精确匹配。
- Bool：恰好 1 字节，`0x00 = false`、`0x01 = true`。
- F32/F64：IEEE-754 小端编码（4 / 8 字节）。
- String：UTF-8 字节，长度 = `FfiStr.len`，不要求 NUL 结尾；Bytes：原样字节。
- `Array`/`Struct` **不允许**写入 `value_bytes`：复杂结果统一通过带
  `schema_version` 的 UTF-8 JSON envelope 返回（§17.9），
  ABI v1 不提供复杂值写入通道。

## 17.3 内存所有权

跨 DLL/SO 禁止 Core 直接释放 Plugin allocator 分配的内存，反之亦然。

统一规则：

```text
谁分配，谁释放。
```

Plugin 返回的 owned buffer 必须通过 Plugin 自己的 `free_buffer` 释放：

```rust
#[repr(C)]
pub struct FfiOwnedBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub capacity: usize,
}
```

```rust
pub free_buffer:
    extern "C" fn(buffer: FfiOwnedBuffer);
```

请求参数默认由 Core 持有，Plugin 只能在函数调用有效期内借用；如需异步保存必须自行复制。

## 17.4 ABI 版本与结构扩展

```rust
#[repr(C)]
pub struct DriverApiV1 {
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub feature_flags: u64,

    // function pointers ...
}
```

兼容规则：

```text
abi_major 必须完全一致。
plugin.abi_minor <= core 支持的 minor。
同一 major 内只能在 struct 尾部追加字段。
Core 必须通过 struct_size 判断字段是否存在。
新增可选能力必须由 feature_flags 或 capability 声明保护。
删除、重排、改变字段含义 => ABI major + 1。
```

## 17.5 Handle 与线程安全

```rust
#[repr(C)]
pub struct DriverHandle {
    pub ptr: *mut std::ffi::c_void,
}
```

默认模型：

```text
一个 DriverHandle 非重入、非并发安全。
Core 默认串行调用同一 Handle。
```

只有 Plugin 显式声明 `THREAD_SAFE_HANDLE` 后，Core 才允许对同一 Handle 并发调用。

## 17.6 错误详情

函数返回稳定的 `i32` 状态码：

```text
0  = OK
>0 = ForgeLink 标准错误
<0 = Driver/Protocol 错误类别
```

详细错误通过：

```rust
pub get_last_error_json:
    extern "C" fn(
        handle: DriverHandle,
        out: *mut FfiOwnedBuffer,
    ) -> i32;
```

返回内容为 UTF-8 JSON，例如：

```json
{
  "code": "MODBUS_EXCEPTION",
  "message": "illegal data address",
  "protocol_code": 2,
  "retryable": false
}
```

该 buffer 必须由 `free_buffer` 释放。

## 17.7 Panic / Exception 边界

任何 Rust panic、C++ exception 都不得穿过 C ABI。

Rust Plugin SDK 的每个导出函数必须使用统一 wrapper：

```text
catch_unwind
  -> convert to DRIVER_PANIC error
  -> save last_error
  -> return non-zero status
```

Native Plugin 推荐 `panic = "unwind"` 并在 ABI 边界 catch；不能安全捕获的厂商 SDK 应优先部署到 Process Plugin / driver-host 中。

## 17.8 Callback 生命周期

Subscription/Event callback 的规范：

- callback 函数指针和 `user_data` 由 Core 提供。
- 从 `subscribe()` 成功返回到对应 `unsubscribe()` 开始生效。
- `unsubscribe()` 返回后 Plugin **不得再次调用** callback。
- Plugin 若跨线程调用 callback，必须在 capability 中声明。
- callback 中传入的数据只在 callback 调用期间有效，Core 如需长期保存必须复制。

## 17.9 ABI v1 API 最小函数表

`ProtocolCapabilities` 中声明为支持的能力，ABI 函数表必须存在对应入口。

```rust
pub type FfiEventCallback = extern "C" fn(
    user_data: *mut std::ffi::c_void,
    event_json: FfiStr,
);

#[repr(C)]
pub struct DriverApiV1 {
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub feature_flags: u64,

    pub create: extern "C" fn(FfiStr, *mut DriverHandle) -> i32,
    pub destroy: extern "C" fn(DriverHandle) -> i32,
    pub connect: extern "C" fn(DriverHandle) -> i32,
    pub disconnect: extern "C" fn(DriverHandle) -> i32,

    pub get_capabilities_json: extern "C" fn(DriverHandle, *mut FfiOwnedBuffer) -> i32,
    pub validate_address: extern "C" fn(DriverHandle, FfiStr, *mut FfiOwnedBuffer) -> i32,

    pub read: extern "C" fn(DriverHandle, *const FfiReadItem, usize, *mut FfiOwnedBuffer) -> i32,
    pub write: extern "C" fn(DriverHandle, *const FfiWriteItem, usize, *mut FfiOwnedBuffer) -> i32,
    pub execute: extern "C" fn(DriverHandle, FfiStr, *mut FfiOwnedBuffer) -> i32,
    pub browse: extern "C" fn(DriverHandle, FfiStr, *mut FfiOwnedBuffer) -> i32,

    pub subscribe: extern "C" fn(
        DriverHandle,
        FfiStr,
        FfiEventCallback,
        *mut std::ffi::c_void,
        *mut u64,
    ) -> i32,
    pub unsubscribe: extern "C" fn(DriverHandle, u64) -> i32,
    pub query_history: extern "C" fn(DriverHandle, FfiStr, *mut FfiOwnedBuffer) -> i32,

    pub get_last_error_json: extern "C" fn(DriverHandle, *mut FfiOwnedBuffer) -> i32,
    pub free_buffer: extern "C" fn(FfiOwnedBuffer),
}
```

`FfiOwnedBuffer` 中 read/write/execute/browse/history 的 payload，以及 subscription/event callback 的 `event_json`，都必须由 ABI minor 固定。v1 初始实现统一使用稳定、带 `schema_version` 的 UTF-8 JSON envelope；后续若引入二进制编码必须通过 feature flag/新 minor 协商，不得静默改变。

Envelope 契约（ABI 1.0 固定）：

- 所有请求/结果/事件 Envelope 必须携带 `schema_version` 字段，v1 固定为 `"1.0"`，
  与 ABI minor 同步演进。
- Core/Plugin 反序列化时必须校验 `schema_version`，与自身支持版本不一致的
  Envelope 必须直接拒绝，不得降级解析。
- 同一 ABI minor 内 Envelope 结构不变，新增可选字段属于 minor 演进；
  破坏性变更必须升级 ABI major（§17.4、§18）。
- 例外：`get_last_error_json`（§17.6）保持固定形状，不携带 `schema_version`。

---

---

# 18. ABI Version 与兼容策略（Normative）

版本定义：

```text
ABI = major.minor
```

首版：

```text
1.0
```

加载规则：

```text
Core 1.4
  可以加载 Plugin 1.0 ~ 1.4
  不能加载 Plugin 2.x
```

同时必须检查：

```text
struct_size
feature_flags
required function pointers
plugin manifest declared ABI
```

Driver Manifest 示例：

```json
{
  "id": "modbus-tcp",
  "version": "0.1.0",
  "abi": {
    "major": 1,
    "minor": 0
  }
}
```

同一 ABI major 内遵循 append-only 原则；任何内存布局破坏性修改都必须升级 major。

---

# 19. 动态加载

推荐使用：

```text
libloading
```

主程序：

```text
启动
 |
 v
扫描 ./drivers/
 |
 +-- modbus.dll
 +-- s7.dll
 +-- fanuc.dll
 |
 v
LoadLibrary / dlopen
 |
 v
forgelink_driver_entry_v1()
 |
 v
注册 Driver
```

新增驱动时：

```text
drivers/
 |
 +-- driver_modbus.dll
 +-- driver_s7.dll
 +-- driver_hnc.dll
```

主程序不修改、不重新编译。

---

# 20. Driver Manifest

每个驱动建议包含：

```text
drivers/
└── modbus/
    ├── driver_modbus.dll
    └── driver.json
```

Manifest 必须显式声明 ABI major/minor 和平台：

```json
{
  "id": "modbus-tcp",
  "name": "Modbus TCP",
  "version": "0.1.0",
  "entry": "forgelink_driver_entry_v1",
  "abi": {
    "major": 1,
    "minor": 0
  },
  "platforms": [
    "windows-x86_64",
    "linux-x86_64",
    "linux-aarch64"
  ]
}
```

Loader 必须同时验证：

```text
manifest ABI
entry symbol
DriverApiV1.abi_major/minor
DriverApiV1.struct_size
required feature flags
```

Manifest 声明与实际入口不一致时拒绝加载。

---

# 21. UI Schema

Driver 自己提供配置 Schema。

Modbus：

```json
{
  "ip": {
    "type": "string",
    "title": "IP Address"
  },
  "port": {
    "type": "integer",
    "default": 502
  },
  "station": {
    "type": "integer",
    "default": 1
  }
}
```

Siemens：

```text
IP
Rack
Slot
PLC Type
```

FANUC：

```text
IP
FOCAS Port
Timeout
```

这样 Web UI 可以动态生成，不需要为每个新 Driver 修改前端。

---

# 22. Poll Scheduler

Core 负责周期调度：

```text
Device
 |
 +-- Group 100 ms
 |     +-- Tag 1
 |     +-- Tag 2
 |
 +-- Group 1 s
 |     +-- Tag 3
 |     +-- Tag 4
 |
 +-- Group 10 s
       +-- Tag 5
```

调度到期：

```text
Poll Engine
    |
    v
driver.read_group(items)
```

Driver 自己决定怎么执行。

---

# 23. PLC Batch Read

例如 Siemens：

```text
DB1.DBW0
DB1.DBW2
DB1.DBD4
DB1.DBW8
```

Driver 不应该发送 4 次请求，而应该：

```text
Address Sort
     |
     v
Merge
     |
     v
DB1 byte 0 ~ 10
     |
     v
1 / few S7 ReadVar
```

读取之后 Driver 按 `item_id` 拆分成多个 `RawReadResult`：

```text
S7 Response
    |
    v
RawReadResult[item 1]
RawReadResult[item 2]
RawReadResult[item 3]
RawReadResult[item 4]
    |
    v
Profile + Domain
    |
    v
Observations
```

批量优化属于 Driver；语义归一化不属于 Driver。

---

# 24. FANUC Batch Read

FANUC 的批量逻辑完全不同。

例如配置：

```text
axis.absolute[1]
axis.machine[1]
axis.relative[1]
```

一个 FOCAS API 可能一次返回多个 position 类型，因此：

```text
3 logical read items
        |
        v
Fanuc Read Optimizer
        |
        v
one cnc_rdposition()
        |
        v
split by item_id
        |
        v
3 RawReadResults
        |
        v
Profile + CNC Domain
        |
        v
3 Observations
```

因此批量优化必须属于 Driver，而 Observation 的构造必须留在 Driver 之外。

---

# 25. 异步模型与 ABI Callback

内部 Rust API 可以是 async，但动态库 ABI 不能直接暴露 Rust `Future`。

推荐：

```text
Core Tokio Runtime
      |
      v
Driver Runtime / Worker
      |
      v
C ABI synchronous function or callback API
```

阻塞型 SDK 可以使用专门 Worker 或 `spawn_blocking`；高风险 Vendor SDK 优先进入 Process Plugin。

Subscription/Event 使用 callback 时必须遵守第 17.8 节生命周期规则：

```text
subscribe()
   |
   +--> callback(RawEvent / RawReadResult)
   |
unsubscribe()
   |
   +--> return 后禁止再 callback
```

callback 仍然输出原始协议结果，不能直接输出 Observation。

---

# 26. 三种 Driver 模式

建议平台最终支持三种 Driver。

## 26.1 Static Driver

适合：

```text
内部系统模块
MQTT
```

---

## 26.2 Native Plugin

```text
DLL / SO
```

适合：

```text
Modbus
S7
EtherNet/IP
FINS
Mitsubishi MC
BACnet
IEC104
```

---

## 26.3 Process Plugin

```text
Edge Core
   |
   v
IPC
   |
   v
driver-host
   |
   v
Vendor SDK
```

适合：

```text
FANUC FOCAS
CNC vendor SDK
闭源 DLL
不稳定第三方库
```

优势：

```text
Driver crash
    |
    v
driver-host crash
    |
    v
Core remains alive
```

---

# 27. FANUC 推荐架构

```text
edge-core
    |
    v
driver-host-fanuc
    |
    v
Rust FFI
    |
    v
FANUC FOCAS SDK
    |
    v
FANUC CNC
```

这样：

- Core 不依赖 FOCAS SDK。
- SDK 崩溃不影响主服务。
- 可以单独处理 Windows/Linux SDK 差异。

---

# 28. 推荐 Rust Workspace

```text
iot-edge/
│
├── crates/
│   ├── edge-core/
│   ├── driver-sdk/
│   ├── driver-loader/
│   ├── device-manager/
│   ├── tag-engine/
│   ├── resource-model/
│   ├── poll-engine/
│   ├── data-pipeline/
│   └── northbound/
│
├── drivers/
│   ├── modbus/
│   ├── siemens-s7/
│   ├── ethernet-ip/
│   ├── omron-fins/
│   ├── mitsubishi-mc/
│   └── fanuc-focas/
│
├── apps/
│   ├── edge-server/
│   └── driver-host/
│
└── web/
```

---

# 29. 第一阶段 Driver Roadmap

建议 P0：

```text
Modbus TCP
Modbus RTU

Siemens S7comm

EtherNet/IP / CIP

OPC UA Client

MQTT
```

P1：

```text
Mitsubishi MC / SLMP
Omron FINS
FANUC FOCAS
```

P2：

```text
S7comm Plus

GSK
KND
HNC
SYNTEC

BACnet
IEC104
DNP3
IEC61850
```

---

# 30. 国产设备 Roadmap

PLC：

```text
汇川 Inovance
信捷 XINJE
和利时 HollySys
禾川 HCFA
英威腾 INVT
台达 Delta
```

CNC：

```text
华中数控 HNC
广州数控 GSK
凯恩帝 KND
新代 SYNTEC
北京精雕
维宏
```

---

# 31. Northbound 可执行契约（Normative）

北向协议处理的是已经归一化的 Observation / Control Request，绝不能暴露 Driver 私有 ABI。

## 31.1 MQTT Topic Namespace v1

推荐固定：

```text
forgelink/v1/telemetry/{site_id}/{device_id}
forgelink/v1/status/{site_id}/{device_id}
forgelink/v1/control/request/{site_id}/{device_id}
forgelink/v1/control/result/{site_id}/{device_id}
```

MVP 默认约定：

| Topic | QoS | Retain |
|---|---:|---:|
| telemetry | 1 | false |
| status | 1 | true |
| control/request | 1 | false |
| control/result | 1 | false |

`status` 使用 retained message + MQTT LWT 表示在线状态；Telemetry 不 retain，避免新订阅者误把旧值当作实时值。

### 31.1.1 Status Envelope（更新后）

在线 / 离线状态载荷统一为 Status Envelope，`schema` 为 `forgelink.status.v1`：

```json
{
  "schema": "forgelink.status.v1",
  "site_id": "plant-a",
  "device_id": "cnc-01",
  "status": "online",
  "sent_at_ns": 1780000000000000000
}
```

字段语义：

- `status`：`online`（客户端 `publish_online` 发布）或 `offline`（客户端显式发布 / LWT 代发）。
- `sent_at_ns`：客户端发布（在线、显式离线、重连重发、停机）时刻的真实时间戳——重发时必须重新生成时间戳，不得复用旧载荷；**LWT 固定为 `0`**——Will 载荷在 CONNECT 时固化、由 broker 在断连时按原样发布，客户端无法预知真实发布时间，消费者必须以消息到达时间作为离线发生时间。

在线状态覆盖规则（更新后）：

- 客户端按 `(site_id, device_id)` 逐设备记录已发布的在线状态；异常断线重连后（broker 已发布 retained 离线 LWT）逐设备重新发布在线状态，避免设备恢复采集后仍显示离线。
- **每次断线重建完整重发周期**：断线时清空待重发队列，以在线设备全集重新填充——上一轮周期中已确认推送（已从队列弹出）的设备在二次断线时必须重新入队，否则其 LWT 已发布、重连后永久离线。未确认的在线状态由 rumqttc 在重连后原样重发，与重建周期的重发布幂等（同一 retained 主题，最终值一致）。
- **重发优先于普通请求**：worker 每轮先执行重发（再接收新请求），保证断线重连后持续业务流量占满 pending 时重发不被饿死（普通请求可等待，重发受断线窗口限制）。
- 重发在 pending 队列有空位时即执行（同一连接内完成，不依赖下一次重连）；**重发进度保存在待重发队列中，从队首推进**：设备数超过 `publish_capacity` 时每轮只推进部分设备，下一轮从断点继续——不得每次从头遍历，否则尾部设备永久遗漏。
- `publish_offline`（设备下线 / 删除）：发布 retained 离线状态并立即停止该设备的在线跟踪（清除待重发队列与已入队在线重发条目），重连时不再重新标记在线。

离线闭环（更新后）：

- 单个 MQTT 客户端只能配置一个 LWT（MQTT 3.1.1 限制），覆盖一个设备（通常为主设备）：任意断连（含进程崩溃）由 broker 代为发布其离线状态。
- **优雅停机**：客户端在发送 DISCONNECT 前为**所有**已跟踪设备显式发布 retained 离线状态（DISCONNECT 不触发 LWT，不主动发布则设备将长期显示在线）。离线请求**逐条转发送达**：rumqttc 通道容量有限（= `publish_capacity`），设备数超过容量时单次转发装不下，客户端循环"转发 + 泵事件循环腾出通道空间"直到全部入队或停机期限届满（与 DISCONNECT 冲刷共享 `GRACE_PERIOD` 预算）；期限内未能送达的剩余离线请求按 Closed 结算并告警，不得静默丢弃。
- **异常断线且无法恢复**（网络分区、进程崩溃、重连上限耗尽）：除 Will 设备外，broker 无法感知客户端已死——**broker 侧会话过期只清理会话状态，不会删除 retained 在线消息**，其余设备可能长期显示在线。客户端在断连期间无法发布任何消息，该场景必须由消费端兜底：**不得单独以 retained 在线消息判断设备在线**，应以最后刷新时间 + 合理超时（含客户端重连预算与余量）推断离线，并据此告警或降级。
- **续租规则（更新后）**：最后刷新时间以该设备**任意消息**（Telemetry 或 Status）的最后到达时间为准——Telemetry 即续租，消费端无需依赖周期性的状态心跳。正常业务下采集流量持续续租，设备不会因"仅在连接时发布一次在线状态"而被误判离线；仅当设备无采集数据（如已停用但未显式下线）时可能超时误判，此时由 `publish_offline` 显式注销避免。周期状态心跳仅作为无业务流量场景的可选演进（需同步调整 §31.1.1 的 LWT 与租约语义），当前版本不引入。

## 31.2 Telemetry Batch Envelope

```json
{
  "schema": "forgelink.telemetry.v1",
  "message_id": "01J...",
  "site_id": "plant-a",
  "device_id": "cnc-01",
  "sequence": 12345,
  "sent_at_ns": 1780000000000000000,
  "replayed": false,
  "observations": []
}
```

字段语义（更新后）：

- `sequence` 是**独立批次序号（Batch Sequence）**，与 Observation 的
  `sequence` 正交：同一 `device_id` 在单一 Collector session 内单调递增
  （从 0 开始），表示"该设备的第几个批"，不取首条或末条 Observation 的
  sequence。Observation 原有的 `sequence` 在批内原样保留，data-pipeline
  不得重新编号。
- `message_id` 由 **data-pipeline 在组包时生成**，采用**长度前缀无歧义
  编码**（与 `observation_id` 同风格，§47）：每段先写十进制长度再写内容，
  `sequence` 为末段按剩余内容整体解析；格式：
  `{session_len}:{collector_session_id}{device_len}:{device_id}{sequence}`。
  `collector_session_id`/`device_id` 允许包含 `-`、`:` 等字符而不会碰撞
  （如 `session="a-b"、device="c"` 与 `session="a"、device="b-c"` 生成
  不同的 `message_id`），嵌入会话 ID 保证 Collector 重启后消息级去重键
  不冲突（§31.3），并天然带设备维度。
- data-pipeline 默认配置：`max_batch_size = 1000`（对齐 §34.2 单批验收
  目标）、`flush_interval = 1s`；两者均可配置。
- 禁止跨设备混批：一个 Batch 只属于一个 `device_id`，不要求全局有序，
  跨设备可并行输出。

每个 Observation 至少携带：

```text
observation_id
device_id
path
value / null
quality.level
quality.reason
source_timestamp_ns / null
ingest_timestamp_ns
sequence
```

## 31.3 Delivery / Ordering / Deduplication

MVP 采用：

```text
at-least-once delivery
```

因此允许重复，不承诺 exactly-once。

去重键：

```text
message_id        - 消息级去重
observation_id    - 数据点级去重
request_id        - 控制请求级幂等/关联
```

`observation_id` 生成时必须嵌入 `collector_session_id`（Collector 启动时
生成），保证 Collector 重启、sequence 重新递增后相同
`device_id + path + sequence` 不会产生重复 ID，消费者不会误判为重复而丢弃。

顺序只保证：

```text
同一 device_id + 单一 Collector session 内按 sequence 单调递增。
```

跨断线重连、Store-and-Forward、Broker 集群不承诺全局严格有序，消费者应按 timestamp + sequence 处理。

## 31.4 Store-and-Forward

补传必须：

- 保留原 Observation 的时间和 `observation_id`。
- `replayed = true`。
- 仍使用 QoS 1。
- 网络恢复后按本地持久化顺序补传。
- Broker ACK 后才能删除对应 WAL 记录。
- 客户端不得因协议层面的**包标识碰撞**（broker 乱序确认导致 pkid 回绕撞上未确认消息，rumqttc 发出 `Outgoing::AwaitAck`）提前结算：碰撞消息在旧同标识消息确认前未写出，`acked()` 必须以真实确认（其消息实际被 broker 收到）为返回 `Ok` 的依据，碰撞恢复后照常结算——防止未送达消息被误判成功、WAL 记录被提前删除。
- 碰撞未决时断线重连，rumqttc 重发保留原包标识：重连后的首个同标识**写事件是重发**（碰撞尚未解决、不得解除碰撞消息的停放），旧同标识消息确认后的同标识写事件才是**碰撞恢复写**（解除停放并关联标识）。客户端必须区分二者，否则碰撞消息可能被后续写事件抢占标识、PUBACK 关联错位、WAL 记录被提前删除；停机排空各阶段与主循环统一按此规则处理。
- rumqttc 的碰撞槽是单个，第二个未决碰撞会覆盖槽位（旧碰撞消息永久丢失，其恢复写永远不会出现）。客户端必须**立即失败结算**被覆盖的碰撞请求（以专用错误 `CollisionOverwritten` 失败——区别于"客户端已关闭"的 `Closed`，客户端本身仍正常运行，调用方不得因此停止客户端；WAL 记录保留可重试补传——连接保持健康时不得让请求永久等待），并把碰撞标识切换到 rumqttc 实际保存的新碰撞；仍可恢复的碰撞消息按配对标识正常恢复确认。

WAL 持久化单位为**完整 Batch**（与 MQTT 发布单位一致）：每条 WAL 记录对应
一个 `message_id` 的完整 Batch，PUBACK 后整条删除；消息级去重键
（`message_id`，§31.3）与 WAL 记录一一对应，不做单条 Observation 粒度的
持久化与补传。

## 31.5 REST v1

最小资源路径：

```text
GET  /api/v1/devices
GET  /api/v1/devices/{device_id}
GET  /api/v1/devices/{device_id}/resources
GET  /api/v1/devices/{device_id}/properties

POST /api/v1/devices/{device_id}/controls
GET  /api/v1/devices/{device_id}/control-requests/{request_id}
```

控制提交统一返回：

```http
202 Accepted
```

```json
{
  "schema": "forgelink.control.accepted.v1",
  "request_id": "cmd-...",
  "status": "accepted"
}
```

`GET /api/v1/devices/{device_id}/control-requests/{request_id}` 返回请求当前状态（§77 异步控制三态）。查询键与幂等键（§80.1）对齐——request_id 的唯一性作用域是设备；不同设备复用同一 request_id 是两个独立请求，互不影响：

```json
{
  "schema": "forgelink.control.status.v1",
  "request_id": "cmd-...",
  "state": "unknown | running | settled",
  "result": {
    "request_id": "cmd-...",
    "namespace": "plant-a",
    "device_id": "dev-1",
    "status": "succeeded | failed | timeout | cancelled | indeterminate | rejected",
    "started_at_ns": 0,
    "completed_at_ns": 0,
    "result": {},
    "error": { "code": "...", "message": "...", "details": {} }
  }
}
```

- `state = unknown`：服务端当前无该 request_id 的可答记录（可能是从未提交，也可能因进程重启丢失内存台账——持久化 Journal 中或有记录；**unknown 不构成重试或任何后续动作的依据**，客户端应沿用原 request_id 或人工确认）；
- `state = running`：已受理尚未结算（`result` 缺省）；
- `state = settled`：终态，`result` 为 §80.1 `ControlResult` 完整载荷。

Property Write 与 Command Execute 都使用 `/controls`，通过 `kind` 区分，且都进入 Control Engine。

控制端点必须经过 §90.2 认证（Bearer Token）；未认证请求一律 `401`。

`forgelink.control.request.v1` 不携带 `namespace`/`device_id`/`requested_at_ns`/`timeout_ms`：`device_id` 取自路径，`requested_at_ns` 由服务端生成，`timeout_ms` 与 `namespace` 由服务端配置提供（`namespace` 进入幂等键 §80.1，须在 Collector 配置中显式声明）。

## 31.6 REST Error Model

```json
{
  "schema": "forgelink.error.v1",
  "code": "DEVICE_NOT_CONNECTED",
  "message": "device is offline",
  "request_id": "req-...",
  "details": {}
}
```

建议状态码：

```text
400 malformed request
401 unauthenticated
403 unauthorized
404 resource not found
409 state conflict / idempotency conflict
422 semantic validation failed
429 rate limited
503 device/driver temporarily unavailable
```

所有消息与 REST payload 都必须显式带 schema/version，禁止依赖隐式字段解释。

---

# 32. Northbound 消息示例

## 32.1 Telemetry

```json
{
  "schema": "forgelink.telemetry.v1",
  "message_id": "msg-10001",
  "site_id": "plant-a",
  "device_id": "fanuc01",
  "sequence": 8821,
  "sent_at_ns": 1780000005000000000,
  "replayed": false,
  "observations": [
    {
      "observation_id": "obs-8821-1",
      "path": "cnc.axis.x.absolute_position",
      "value": 123.456,
      "quality": {
        "level": "good",
        "reason": "none"
      },
      "source_timestamp_ns": null,
      "ingest_timestamp_ns": 1780000004999000000,
      "sequence": 8821
    }
  ]
}
```

## 32.2 Property Write

```json
{
  "schema": "forgelink.control.request.v1",
  "request_id": "cmd-20001",
  "kind": "property_write",
  "items": [
    {
      "path": "drive.output.frequency",
      "value": 50.0
    }
  ]
}
```

## 32.3 Command Execute

```json
{
  "schema": "forgelink.control.request.v1",
  "request_id": "cmd-20002",
  "kind": "command_execute",
  "command": "cnc.program.start",
  "parameters": {}
}
```

两类控制请求都必须经过同一个 Control Engine。

---

# 33. 关键设计原则

## 原则 1

不要统一协议。

统一：

```text
Observation
Property
Resource
Command
```

---

## 原则 2

Core 永远不要出现：

```rust
match driver {
    "modbus" => ...
    "s7" => ...
    "fanuc" => ...
}
```

正确方式：

```rust
let driver =
    driver_manager.create(driver_id)?;

driver.connect().await?;

let data =
    driver.read(items).await?;
```

---

## 原则 3

协议地址属于 Driver。

Core 不理解：

```text
DB1.DBD0
D100
40001
axis.absolute[1]
```

---

## 原则 4

批量优化属于 Driver。

因为：

```text
Modbus batching
S7 batching
FOCAS API batching
```

逻辑完全不同。

---

## 原则 5

统一的是结果，不是读取方法。

最终统一：

```text
Device
Resource
Property
Observation
Command

Value
Quality
Timestamp
```

---

# 34. MVP 范围与验收标准（Normative）

MVP 功能范围：

```text
edge-core
collector
driver-sdk
driver-loader
profile-engine
domain-model
device-manager
poll-engine
control-engine
local-buffer

driver-modbus
MQTT v1
REST v1
```

以上是 MVP 的目标范围，不表示当前仓库已经全部交付。当前实现进度见 §34.7；
未完成能力仍以本章验收标准为准，不得在部署文档中按已实现功能使用。

## 34.1 功能验收

必须完成：

```text
Device / Resource / Property
RawReadResult -> Profile -> Domain -> Observation
Read / Property Write / Command framework
Polling / Batching
Timeout / Reconnect
Quality / Timestamp
MQTT QoS1
REST Control 202 + request_id
WAL Store-and-Forward
Driver dynamic loading
```

## 34.2 初始性能目标

以下指标必须绑定测试环境和工作负载。v1 MVP 使用以下 Reference Benchmark Profile；每次报告必须记录 CPU 型号、核心数、内存、磁盘、OS、Rust 版本和构建 commit。

### Reference Benchmark Profile

x86_64 主性能基线：

```text
CPU: 4 cores, sustained >= 2.5 GHz
RAM: 8 GiB
Disk: SSD/NVMe
NIC: 1 GbE
OS: Linux x86_64
Build: cargo build --release
```

Windows x64 用同等级硬件做功能/稳定性复验。ARM64：

```text
CPU: 4 cores, sustained >= 2.0 GHz
RAM: 4 GiB
Disk: eMMC/SSD
NIC: 1 GbE
Build: cargo build --release
```

标准 Modbus TCP workload：

```text
simulated devices: 100
properties/device: 100
total points: 10,000
poll interval: 500 ms
simulated response latency: 5 ms +/- 1 ms
address layout: contiguous/batchable
northbound: local MQTT broker, QoS 1
payload: standard Observation batch envelope
```

理论速率：

```text
10,000 / 0.5 s = 20,000 observations/s
```

独立 100 ms 调度测试：

```text
devices: 10
properties/device: 100
poll interval: 100 ms
duration: 30 min
```

故障测试：

```text
network disconnect: 30 min
device timeout injection: 1%
broker unavailable: 30 min
forced process restart during WAL write
```

报告至少记录 throughput、CPU、RSS、p50/p95/p99 调度延迟、设备请求延迟、MQTT publish 延迟、WAL backlog、补传数、重复数和已持久化数据丢失数。

在该基线下，以下为 v1 MVP 初始验收目标：

| 指标 | MVP 目标 |
|---|---:|
| 单节点配置点数 | >= 10,000 points |
| 持续归一化吞吐 | >= 20,000 observations/s |
| 网络型协议最小调度周期 | 100 ms |
| 单 Collector Modbus TCP 设备数 | >= 100（典型轻量点表） |
| 单批 Observation 数 | >= 1,000 |
| 连续稳定运行 | >= 72 h soak test |
| 100 ms 调度测试 | p99 调度延迟 <= 25 ms，30 min 无调度器崩溃 |
| 30 min Broker 断网恢复 | 已落盘 Observation 0 丢失；允许 at-least-once 重复 |
| WAL 强制重启恢复 | 已 fsync 记录 0 丢失 |
| 72 h soak test 内存增长 | 稳态 RSS 漂移 <= 10%（排除有界缓存配置变化） |

性能验收必须同时记录 CPU、RSS、P95/P99 采集延迟，不能只看吞吐。

### 34.2.1 指标埋点要求（Normative）

§34.2 报告项必须有对应运行时指标支撑。组件在代码路径上暴露计数器/直方图，由统一 metrics 门面聚合；指标是验收与现场运维的共同基础设施：

```text
poll-engine          poll_batches_total / poll_errors_total{reason} /
                     schedule_delay_ns（直方图：调度触发 vs 计划时刻偏差）
data-pipeline        batches_flushed_total / observations_total /
                     flush_backpressure_wait_ns
local-buffer         wal_inflight（gauge）/ wal_disk_bytes（gauge）/
                     wal_replayed_total / wal_ack_dropped_total /
                     wal_persist_ns（直方图）
mqtt-client          mqtt_inflight（gauge）/ mqtt_published_total /
                     mqtt_redelivered_total / mqtt_failed_total /
                     mqtt_publish_ns（直方图）
control-engine       control_queue_depth{device}（gauge，有界采样）/
                     control_settled_total{status} / control_cooldown_entered_total /
                     control_journal_settle_failed_total
diagnostics          日志丢弃计数（非阻塞队列满）
```

约定：

- 门面必须零依赖（不引入第三方 metrics 库）、默认实现为进程内原子计数器；
- 埋点在热路径上只做一次原子操作，禁止加锁与堆分配；
- 直方图为固定桶边界（ns 级），快照读取为无锁聚合；
- 未注册的指标读取返回 0，不得 panic；
- REST 只读暴露 `GET /api/v1/metrics`（`forgelink.metrics.v1`），属管理接口非控制面——只读构建同样可用；响应不含文件路径、地址、凭据。


## 34.3 Timeout / Reconnect

必须满足：

```text
连接、请求 timeout 可配置。
断线后自动重连。
Poll Engine 默认使用指数退避：1s -> 2s -> 4s ... 上限 30s。
Driver 内部的连接尝试属于协议会话职责，间隔和次数由具体 Driver 配置决定；
Modbus MVP 使用 `reconnect_delay_ms` 固定间隔和 `reconnect_max_attempts` 次数，
不替代 Poll Engine 的批次级退避。
成功重连后退避重置。
单设备故障不得阻塞其他设备。
```

## 34.4 缓存与断电恢复

MVP 至少支持：

```text
Memory Queue + Disk WAL
磁盘上限可配置
保留时间可配置
背压策略可配置
```

验收测试：

1. Broker 断网 30 分钟，Collector 继续采集并落 WAL。
2. Broker 恢复后自动补传。
3. `kill -9` / 非正常重启后 WAL 可恢复且文件不损坏。
4. 已持久化但未 ACK 的记录允许重复发送，但不得静默丢失。
5. 重复数据通过 `message_id / observation_id` 可去重。

交付语义明确为：

```text
at-least-once
```

## 34.5 三目标平台验收

必须在以下平台分别构建并运行：

```text
Windows x64
Linux x64
Linux ARM64
```

每个平台至少验证：

```text
Collector 启动/停止
动态 Driver 加载
Modbus TCP 真实或协议模拟器读取
MQTT 发布
REST 健康检查
断网缓存恢复
异常退出后的 WAL 恢复
```

Linux ARM64 的峰值性能可以与 x64 不同，但功能契约必须一致。

## 34.6 MVP Driver Roadmap

```text
MVP: Modbus TCP / RTU
V0.2: Siemens S7comm
V0.3: EtherNet/IP + Mitsubishi MC + Omron FINS
V0.4: FANUC FOCAS Process Plugin
```

每增加一个 Driver，都必须通过同一套 Driver Contract、Quality、Timeout、Reconnect、Batch 与 ABI/Process isolation 测试。

## 34.7 当前仓库实施状态（非架构决策）

截至控制链路交付合并，以下能力已经在 workspace 中实现并有自动化测试：

```text
observation-model     共享规范模型
driver-sdk            Driver 契约与 ABI v1 Tag/Envelope
diagnostics           结构化日志、级别/格式切换与脱敏
driver-loader         Native Plugin 加载、ABI 校验与句柄生命周期
profile-engine        Profile 校验、加载、注册与读写转换
domain-model          Domain 路径校验与 Observation 映射
poll-engine           周期调度、超时、指数退避、取消与阻塞隔离
driver-modbus         Modbus TCP/RTU Driver：读取 MVP；写功能 FC05/06/15/16（帧编解码、
                      响应回显校验、精确相邻批量合并、多寄存器字序镜像读侧）
control-engine        Control Engine 基础（§81-§90：统一入口、幂等键去重、每设备有界队列、
                      审计日志与 FileJournal）
device-manager        设备实例注册、Driver/Profile 绑定校验、读取项生成与分组、全链路数据映射；
                      ControlExecutor 适配层（DriverSession 共享会话抽象，读写同锁互斥；
                      保守 Indeterminate 映射——仅可证明未上线才 Failed）
data-pipeline         Telemetry Batch 聚合输出（有界队列、按设备分批、背压/取消/有界排空）
mqtt-client           MQTT 北向客户端（QoS 1 发布、自动重连退避、LWT、TLS/mTLS、有界队列与背压）
local-buffer          Local Buffer/WAL（SQLite WAL 崩溃恢复、两级缓冲、幂等补传与 ack 删除）
rest-api              REST v1 管理接口——只读：设备/资源/属性/健康、错误模型、有界并发
                      （§31.5/§31.6）；控制：POST controls 202 异步受理 + GET control-requests
                      三态查询、Bearer 认证（§90.2）、错误映射 400/404/409/422/503；
                      指标：GET /api/v1/metrics（§34.2.1，管理接口非控制面）
metrics               指标门面（§34.2.1）：零依赖原子 Counter/Gauge/固定桶 Histogram、
                      有界注册表与溢出降级、无锁快照；五组件埋点接入
                      （poll/pipeline/WAL/MQTT/control），collector 共享单一注册表
collector             Collector 运行时组装（§93/§100：只读采集链路——轮询→映射→组包→WAL→MQTT
                      QoS1 发布；有序停机有限排空，REST 服务异常退出触发停机；control feature：
                      control 配置段装配 Control Engine，停机第 0.5 步结算在途控制；
                      metrics 注册表全组件共享并经 REST 暴露）
modbus-mock           测试共用 Mock Modbus TCP server（非生产）
```

以下能力仍未完成端到端交付：

```text
edge-server / manager 的完整运行时组装、三平台部署、性能基准和长时间稳定性验收。
```

本节只记录实现进度，不改变前述 Normative 契约和 MVP 验收标准。

---

# 35. 最终目标

最终平台可以形成：

```text
                     ForgeLink Industrial Edge

+-----------------------------------------------------+
| Web UI / REST / MQTT / OPC UA                      |
+-----------------------------------------------------+
| Observation Pipeline / Control Engine              |
+-----------------------------------------------------+
| Domain Model                                       |
+-----------------------------------------------------+
| Device Profile                                     |
+-----------------------------------------------------+
| Driver Manager / Stable Driver ABI                 |
+-----------------------------------------------------+
| Protocol Drivers                                   |
| Modbus / S7 / CIP / FINS / MC / FOCAS / ...       |
+-----------------------------------------------------+
| Transport / Process Driver Host                    |
+-----------------------------------------------------+
| PLC / CNC / Robot / Drive / Meter / Sensor         |
+-----------------------------------------------------+
```

职责固定为：

```text
Driver
= 协议通信、编解码、批量优化、原始结果

Device Profile
= 品牌型号、地址映射、缩放、单位、型号能力

Domain Model
= PLC/CNC/Robot/Drive 等标准业务语义

Core
= Observation、缓存、调度、控制、安全、北向
```

新增 Driver 或 Profile 时不修改 Core 的协议分支代码。

---

# 36. Device Profile：协议驱动与具体设备型号解耦

仅有 Driver 还不够。

工业现场经常出现：

```text
同一个协议
    ↓
很多不同设备
```

例如 Modbus RTU 可以被以下设备使用：

```text
PLC
变频器
伺服
电表
温控器
称重仪
传感器
注塑机
专机
```

如果每个设备都写一个新的 Driver：

```text
1000 种设备
=
1000 个 Driver
```

平台会很难维护。

因此建议在 Protocol Driver 之上增加：

```text
Device Profile
```

完整层次：

```text
Device
  |
  v
Device Profile
  |
  v
Protocol Driver
  |
  v
Transport
```

例如汇川变频器：

```text
Inovance MD500 Profile
        |
        v
Modbus RTU Driver
        |
        v
Serial Transport
```

台达电表：

```text
Delta Power Meter Profile
        |
        v
Modbus TCP Driver
        |
        v
TCP Transport
```

Siemens S7-1500：

```text
Siemens S7-1500 Profile
        |
        v
S7 Driver
        |
        v
TCP Transport
```

FANUC 0i：

```text
FANUC 0i Profile
        |
        v
FOCAS Driver
        |
        v
Vendor SDK / Ethernet
```

因此平台最终更合理的关系是：

```text
1000 Device Profiles
        |
        v
几十个 Protocol Drivers
        |
        v
少量 Transport 实现
```

---

# 37. Device Profile 数据结构建议（Normative）

Device Profile 负责描述品牌/系列/型号如何映射到 Driver 和 Domain。

```rust
pub struct DeviceProfile {
    pub id: String,
    pub vendor: String,
    pub family: String,
    pub models: Vec<String>,
    pub domain: DomainKind,
    pub driver_id: String,

    pub properties: Vec<ProfileProperty>,
    pub commands: Vec<ProfileCommand>,
    pub capabilities: ProfileCapabilities,
}
```

Property 映射：

```rust
pub enum WriteRounding {
    Exact,
    Nearest,
    Floor,
    Ceil,
    Truncate,
}

pub struct ProfileProperty {
    pub path: PropertyPath,
    pub driver_address: String,
    pub raw_type: DataType,
    pub value_type: DataType,
    pub unit: Option<String>,

    // Read: semantic = raw * scale + offset
    pub scale: f64,
    pub offset: f64,
    pub write_rounding: WriteRounding,

    pub readable: bool,
    pub writable: bool,
    pub default_interval_ms: Option<u64>,

    // Semantic range, not raw register range.
    pub min: Option<Value>,
    pub max: Option<Value>,
}
```

Command 映射：

```rust
pub struct ProfileCommand {
    pub id: String,
    pub driver_command_id: String,
    pub parameters: Vec<CommandParameterDescriptor>,
    pub risk_level: CommandRiskLevel,
    pub preconditions: Vec<CommandPrecondition>,
}
```

例如汇川变频器：

```text
path: drive.output.frequency
driver_address: 1!40001
raw_type: U16
value_type: F64
scale: 0.01
offset: 0
write_rounding: Nearest
unit: Hz
min: 0.0
max: 50.0
```

底层 `5000` 经过 Profile 后得到 `50.00 Hz`，再由 Drive Domain 暴露标准语义路径。

## 37.1 读取与写入转换规则（Normative）

读取：

```text
semantic_value = raw_value * scale + offset
```

转换后必须按 `value_type` 做 checked conversion，禁止静默溢出。

Property Write 逆变换：

```text
raw_candidate = (semantic_value - offset) / scale
```

固定处理顺序：

```text
1. 检查 writable
2. 校验 semantic value_type
3. 校验 semantic min/max
4. 检查 scale != 0 且数值 finite
5. 执行逆变换
6. 按 write_rounding 处理整数 raw_type
7. 检查 raw_type 可表示范围
8. checked conversion
9. 生成 DriverWriteItem
10. 进入 Control Queue / Driver
```

- `Exact`：不能无损表示为目标 `raw_type` 时拒绝。
- `Nearest/Floor/Ceil/Truncate`：只有 Profile 显式声明时允许。
- overflow、underflow、NaN、Infinity、`scale == 0` 必须拒绝，禁止截断。
- 枚举映射必须定义可逆 mapping；无法唯一反向映射时拒绝写入。
- `min/max` 作用于语义值；raw 范围由 `raw_type` checked conversion 再保证。
- Profile Engine 必须提供 `encode_write` / `decode_read` 成对测试。

---

# 38. Device Profile 不应该写死在主程序

Profile 应该和 Driver 一样动态加载。

例如：

```text
profiles/
├── inovance/
│   ├── md500.json
│   ├── md520.json
│   └── md810.json
│
├── delta/
│   ├── vfd-e.json
│   └── power-meter.json
│
├── fanuc/
│   └── 0i.json
│
└── siemens/
    ├── s7-1200.json
    └── s7-1500.json
```

新增一个设备型号时：

```text
增加 Profile JSON
```

而不是：

```text
重新修改 Edge Core
```

因此平台可以做到：

```text
新增设备型号
    |
    +-- 如果协议已支持
    |       |
    |       v
    |   只新增 Profile
    |
    +-- 如果协议未支持
            |
            v
        新增 Driver
```

这会显著降低后续设备扩展成本。

---

# 39. Transport 层

建议将网络和协议进一步拆分。

Transport 只负责通信通道。

```rust
pub trait Transport {
    async fn open(&mut self) -> Result<()>;

    async fn close(&mut self) -> Result<()>;

    async fn send(
        &mut self,
        data: &[u8],
    ) -> Result<()>;

    async fn receive(
        &mut self,
        buffer: &mut [u8],
    ) -> Result<usize>;
}
```

常见 Transport：

```text
TCP
UDP
Serial
CAN
USB
Vendor SDK
IPC
```

完整架构：

```text
+----------------------------------+
| Unified Data Model               |
+----------------------------------+
              |
              v
+----------------------------------+
| Domain Model                     |
+----------------------------------+
              |
              v
+----------------------------------+
| Device Profile                   |
+----------------------------------+
              |
              v
+----------------------------------+
| Protocol Driver                  |
+----------------------------------+
              |
              v
+----------------------------------+
| Transport                        |
| TCP / UDP / Serial / SDK / CAN   |
+----------------------------------+
```

---

# 40. Transport 与 Driver 的关系

例如 Modbus TCP：

```text
Modbus Driver
      |
      v
TCP Transport
```

Modbus RTU：

```text
Modbus Driver
      |
      v
Serial Transport
```

Omron FINS：

```text
FINS Driver
   |
   +-- TCP Transport
   |
   +-- UDP Transport
```

Mitsubishi MC：

```text
MC Driver
   |
   +-- TCP
   |
   +-- UDP
```

FOCAS：

```text
FANUC Driver
     |
     v
FOCAS SDK Transport
```

这样协议解析和连接管理不会互相污染。

---

# 41. Domain Model：在统一数据之上增加工业领域语义

`Resource / Property / Observation` 是平台的通用基础模型。

但是商业级工业平台还应该进一步定义标准领域模型：

```text
PLC Model
CNC Model
Robot Model
Drive Model
Meter Model
Sensor Model
Machine Model
```

这样上层应用不需要理解厂商差异。

---

# 42. PLC Domain Model

PLC 可以统一为：

```text
PLC
├── Memory
├── Program
├── CPU
├── IO
└── Diagnostics
```

例如：

```text
plc.cpu.run_state
plc.cpu.cycle_time

plc.io.input.x0
plc.io.output.y0

plc.memory.temperature
```

底层可能来自：

```text
Siemens S7
Mitsubishi MC
Omron FINS
Allen-Bradley CIP
Modbus
汇川
信捷
```

---

# 43. CNC Domain Model

建议统一为：

```text
CNC
├── Controller
├── Axis[]
├── Spindle[]
├── Program
├── Tool[]
├── Alarm[]
├── Offset
├── Macro
└── PLC / PMC
```

标准路径可以设计为：

```text
cnc.controller.status

cnc.axis.x.absolute_position
cnc.axis.x.machine_position
cnc.axis.x.load

cnc.spindle.1.speed
cnc.spindle.1.load

cnc.program.current.name
cnc.program.current.number
cnc.program.block

cnc.tool.current.number

cnc.alarm.active

cnc.macro.100
```

FANUC、Siemens SINUMERIK、Mitsubishi CNC、HNC、GSK、KND、SYNTEC 都尽量映射到这一套标准模型。

---

# 44. Robot Domain Model

机器人建议统一：

```text
Robot
├── Controller
├── Joint[]
├── TCP
├── Program
├── IO
├── Alarm
└── Safety
```

例如：

```text
robot.controller.mode

robot.joint.j1.position
robot.joint.j2.position
robot.joint.j3.position

robot.tcp.x
robot.tcp.y
robot.tcp.z
robot.tcp.rx
robot.tcp.ry
robot.tcp.rz

robot.program.current

robot.alarm.active
```

底层可以对应：

```text
FANUC Robot
ABB
KUKA
Yaskawa
Estun
EFORT
GSK
```

---

# 45. Drive / Servo Domain Model

变频器、伺服、驱动器建议统一：

```text
Drive
├── Motor
├── Output
├── Command
├── Status
└── Alarm
```

标准属性：

```text
drive.motor.speed
drive.motor.current
drive.motor.torque
drive.motor.temperature

drive.output.frequency
drive.output.voltage
drive.output.current

drive.status.running

drive.alarm.code
```

底层可能只是：

```text
Modbus RTU
Modbus TCP
CANopen
EtherCAT
厂商私有协议
```

---

# 46. Meter / Sensor Domain Model

电表：

```text
meter.voltage.a
meter.voltage.b
meter.voltage.c

meter.current.a
meter.current.b
meter.current.c

meter.power.active
meter.power.reactive
meter.energy.total
```

传感器：

```text
sensor.temperature
sensor.pressure
sensor.flow
sensor.vibration
sensor.humidity
```

这样无论设备原始地址是什么，上层都是语义化数据。

---

# 47. 设备数据映射流程

完整数据路径建议为：

```text
Physical Device
      |
      v
Transport
      |
      v
Protocol Driver
      |
      v
Raw Value
      |
      v
Device Profile
      |
      v
Semantic Property
      |
      v
Domain Model
      |
      v
Observation
      |
      v
MQTT / OPC UA / REST / DB
```

例如汇川变频器：

```text
40001 = 5000
      |
      v
Modbus Driver
      |
      v
Profile scale = 0.01
      |
      v
drive.output.frequency
      |
      v
50.00 Hz
```

---

# 48. 不同数据获取模式与 ProtocolCapabilities

不能假设所有设备都是轮询型。

采集模式属于 **ProtocolCapabilities**：

```text
polling
subscription
events
history
```

它们描述协议/Driver 是否支持某种数据获取机制，不描述设备属于 PLC、CNC 还是 Robot。

Profile 通过 `ProfileCapabilities.acquisition: AcquisitionConstraints` 限制具体型号的获取方式。例如协议支持 Subscription，但某型号只允许 Polling，则 `subscription = Some(false)`。`None` 表示继承 Driver；`Some(true)` 表示 Profile 要求该能力，但只有 Driver 同时支持时才生效。

最终有效能力：

```text
Effective Capability
= Driver ProtocolCapabilities
  ∩ ProfileCapabilities / constraints
```

Domain Model 只负责标准语义，不参与协议能力 bitflag。

---

# 49. Polling 模式

适合：

```text
Modbus
S7
FINS
Mitsubishi MC
多数 PLC
多数仪表
```

流程：

```text
Poll Scheduler
      |
      v
Driver.read()
      |
      v
RawReadResult[]
      |
      v
Profile Engine
      |
      v
Domain Model
      |
      v
Observation[]
```

Poll Scheduler 不解析协议数据。

---

# 50. Subscription 模式

适合：

```text
OPC UA Subscription
MQTT
部分厂商实时接口
```

流程：

```text
Device
   |
   v
Driver callback
   |
   v
RawEvent / RawReadResult
   |
   v
Profile Engine
   |
   v
Domain Model
   |
   v
Observation
```

Core 不主动周期调用 read，但 Subscription 也不得绕过 Profile/Domain。

---

# 51. Event 模式

适合：

```text
Alarm
Program Changed
Machine State Changed
Robot Event
```

例如：

```text
FANUC alarm
robot safety state
machine program changed
```

这些数据可以采用：

```text
Event + Polling Hybrid
```

而不是全部高频轮询。

---

# 52. History 模式

部分设备或服务器能够直接提供历史数据。

例如：

```text
OPC UA Historical Access
Historian
部分 CNC 生产记录
```

Driver 可以声明：

```text
HISTORY
```

Core 则提供统一 Historical Query API。

---

# 53. Driver 与 Device Profile 的职责边界

Driver 负责：

```text
连接
协议握手
编解码
地址解析
请求发送
响应解析
批量优化
重连
协议错误
```

Device Profile 负责：

```text
厂商
型号
地址映射
缩放
单位
枚举
语义名称
默认采样周期
设备特定参数
```

Domain Model 负责：

```text
不同厂商设备之间的标准语义
```

例如：

```text
FANUC spindle speed
HNC spindle speed
Mitsubishi CNC spindle speed
```

最终都映射：

```text
cnc.spindle.1.speed
```

---

# 54. Driver、Profile、Domain Model 示例

## Siemens S7-1500

```text
TCP
 |
 v
S7 Driver
 |
 v
S7-1500 Device Profile
 |
 v
PLC Domain Model
 |
 v
plc.memory.production_count
```

## FANUC 0i

```text
FOCAS SDK
 |
 v
FANUC Driver
 |
 v
FANUC 0i Device Profile
 |
 v
CNC Domain Model
 |
 v
cnc.axis.x.absolute_position
```

## 华中数控 HNC

```text
HNC SDK / Protocol
 |
 v
HNC Driver
 |
 v
HNC-848 Profile
 |
 v
CNC Domain Model
 |
 v
cnc.spindle.1.speed
```

## 汇川 MD500

```text
Serial
 |
 v
Modbus Driver
 |
 v
Inovance MD500 Profile
 |
 v
Drive Domain Model
 |
 v
drive.output.frequency
```

---

# 55. 平台最终分层

推荐最终架构固定为：

```text
+--------------------------------------------------+
|                 Northbound                       |
| MQTT / OPC UA / REST / DB / Sparkplug B         |
+--------------------------------------------------+
                       |
                       v
+--------------------------------------------------+
|             Unified Observation Layer            |
| Value / Quality / Timestamp / Metadata           |
+--------------------------------------------------+
                       |
                       v
+--------------------------------------------------+
|                  Domain Model                    |
| PLC / CNC / Robot / Drive / Meter / Sensor       |
+--------------------------------------------------+
                       |
                       v
+--------------------------------------------------+
|                 Device Profile                   |
| Vendor / Model / Mapping / Scale / Unit          |
+--------------------------------------------------+
                       |
                       v
+--------------------------------------------------+
|                Protocol Driver                   |
| S7 / Modbus / CIP / FINS / MC / FOCAS / ...     |
+--------------------------------------------------+
                       |
                       v
+--------------------------------------------------+
|                   Transport                      |
| TCP / UDP / Serial / CAN / SDK / IPC             |
+--------------------------------------------------+
                       |
                       v
+--------------------------------------------------+
|              Physical Industrial Device          |
+--------------------------------------------------+
```

---

# 56. 支持设备类别范围

该架构不限定 Siemens PLC 和 FANUC CNC。

目标覆盖：

```text
PLC
CNC
Robot
Motion Controller
Servo
VFD
Meter
Sensor
Instrument
Injection Machine
Press Machine
Welding Machine
Laser Machine
SMT
Packaging Machine
Power Equipment
Building Equipment
Special Machine
```

厂商可以包括：

```text
Siemens
Rockwell / Allen-Bradley
Mitsubishi
Omron
Schneider
Beckhoff
Keyence

Inovance
XINJE
HollySys
HCFA
INVT
Delta

FANUC
SINUMERIK
HNC
GSK
KND
SYNTEC
Mazak
HEIDENHAIN

ABB Robot
KUKA
Yaskawa
Estun
EFORT
```

---

# 57. 新增设备的决策流程

以后新增设备时，先判断：

```text
是否已有协议 Driver？
```

如果有：

```text
已有 Driver
   |
   v
新增 Device Profile
   |
   v
映射 Domain Model
```

例如：

```text
新 Modbus 电表
```

通常不需要开发新 Driver。

如果没有协议 Driver：

```text
新增 Protocol Driver
        |
        v
新增 Device Profile
        |
        v
映射 Domain Model
```

这样整个系统能够持续扩展，而 Core 保持稳定。

---

# 58. 更新后的核心设计原则

平台最终应坚持以下边界：

```text
Transport
负责“怎么传”
```

```text
Protocol Driver
负责“怎么说协议”
```

```text
Device Profile
负责“这个具体型号的数据在哪里、如何解释”
```

```text
Domain Model
负责“这种工业设备在业务上是什么”
```

```text
Observation
负责“统一向上输出什么”
```

最终核心思想可以总结为：

> 不把所有设备强行变成 PLC。

> 不把所有协议强行变成 Tag。

> 不把所有设备型号都做成独立 Driver。

> 使用 Transport + Protocol Driver + Device Profile + Domain Model + Observation 五层模型，支撑 PLC、CNC、Robot、Drive、Meter、Sensor 以及其他工业设备的长期扩展。

---

# 59. Driver、Profile、Domain 的划分原则

平台需要同时描述三个不同维度：

```text
设备类别           通信协议             具体设备型号
Domain             Driver               Device Profile
```

这三个维度不能混在一起。

推荐固定为：

```text
Domain
  |
  v
Device Profile
  |
  v
Protocol Driver
  |
  v
Transport
```

其中：

```text
Domain
负责“这是什么类型的工业设备”
```

```text
Driver
负责“使用什么通信协议”
```

```text
Device Profile
负责“具体是哪一个品牌、系列、型号，以及如何解释它的数据”
```

---

# 60. Driver 应按协议划分，而不是按设备型号划分

Driver 的职责是实现协议本身，例如：

```text
Modbus TCP
Modbus RTU

Siemens S7comm
Siemens S7comm+

EtherNet/IP / CIP

Omron FINS

Mitsubishi MC / SLMP

FANUC FOCAS

Beckhoff ADS

OPC UA

BACnet

IEC104
```

Driver 负责：

```text
连接
握手
报文编码
报文解码
地址解析
请求发送
响应解析
批量优化
超时
重连
协议错误处理
```

因此只要多个设备使用同一种协议，就应该尽量复用同一个 Driver。

错误方式：

```text
driver_inovance_md500
driver_inovance_md520
driver_inovance_md810
```

如果这些设备底层都使用 Modbus RTU，就不应该复制三套协议实现。

正确方式：

```text
                    Modbus RTU Driver
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
     MD500 Profile    MD520 Profile    MD810 Profile
```

---

# 61. Device Profile 应按品牌 + 系列/型号划分

Device Profile 负责具体设备差异。

例如：

```text
Siemens S7-1200
Siemens S7-1500

Mitsubishi FX5U
Mitsubishi iQ-R

Omron NX1P2
Omron CJ2M

Inovance MD500
Inovance MD520

FANUC 0i-F Plus
FANUC 31i-B

HNC 818
HNC 848

GSK 980
KND K2000
```

Profile 中可以保存：

```text
厂商
设备类别
产品系列
具体型号

使用哪个 Driver

默认连接参数

地址映射
寄存器映射
单位
缩放系数

状态枚举
报警解释

支持哪些功能
默认采样周期
型号兼容差异
```

---

# 62. Domain 应按设备类别划分

Domain 不关心厂商和协议。

典型 Domain：

```text
PLC
CNC
Robot
Drive
Servo
Meter
Sensor
Instrument
Machine
PowerDevice
BuildingDevice
```

例如：

```text
Domain = CNC
Driver = fanuc-focas
Profile = fanuc-0i-f-plus
```

或者：

```text
Domain = Drive
Driver = modbus-rtu
Profile = inovance-md500
```

或者：

```text
Domain = PLC
Driver = mitsubishi-mc-3e
Profile = mitsubishi-fx5u
```

---

# 63. 推荐三级标识

每个设备实例建议至少保存：

```text
domain
driver_id
profile_id
```

例如：

```json
{
  "name": "CNC-01",
  "domain": "cnc",
  "driver_id": "fanuc-focas",
  "profile_id": "fanuc-0i-f-plus"
}
```

汇川变频器：

```json
{
  "name": "VFD-01",
  "domain": "drive",
  "driver_id": "modbus-rtu",
  "profile_id": "inovance-md500"
}
```

三菱 PLC：

```json
{
  "name": "PLC-01",
  "domain": "plc",
  "driver_id": "mitsubishi-mc-3e",
  "profile_id": "mitsubishi-fx5u"
}
```

---

# 64. Siemens 示例

Siemens 不应该简单按 PLC 型号做 Driver。

推荐：

```text
                     Siemens

          +-------------+-------------+
          |                           |
          v                           v
     S7comm Driver              S7comm+ Driver
          |                           |
     +----+-----+                +----+-----+
     |          |                |          |
     v          v                v          v
 S7-300      S7-1200         S7-1200     S7-1500
 Profile      Profile          Profile      Profile
```

这里：

```text
S7comm
```

和：

```text
S7comm+
```

如果协议机制差异足够大，可以做两个不同 Driver。

但是：

```text
S7-300
S7-400
S7-1200
S7-1500
```

本身更适合作为 Profile 差异；只有协议实现层面的差异才进入 ProtocolCapabilities，而不是简单“一型号一 Driver”。

例如：

```text
Domain:
PLC

Driver:
siemens-s7

Profile:
siemens-s7-1500
```

---

# 65. FANUC CNC 示例

推荐：

```text
                 FANUC FOCAS Driver
                        |
       +----------------+----------------+
       |                |                |
       v                v                v
  FANUC 0i         FANUC 30i        FANUC 31i
  Profile           Profile          Profile
```

Driver 负责统一封装：

```text
cnc_rdposition
cnc_statinfo
cnc_alarm
cnc_rdmacro
pmc_rdpmcrng
...
```

Profile 负责：

```text
型号支持的 API
轴数量
主轴数量
PMC 区域
默认端口
型号差异
功能开关
兼容性参数
```

例如：

```text
Domain:
CNC

Driver:
fanuc-focas

Profile:
fanuc-0i-f-plus
```

---

# 66. 汇川变频器示例

如果：

```text
MD500
MD520
MD810
```

都走 Modbus RTU，则：

```text
                Modbus RTU Driver
                      |
       +--------------+--------------+
       |              |              |
       v              v              v
   MD500 Profile   MD520 Profile   MD810 Profile
```

Profile 中定义：

```text
40001 -> drive.output.frequency
40002 -> drive.motor.current
40003 -> drive.output.voltage
```

以及：

```text
scale
unit
enum
alarm code
```

而 Modbus Driver 只负责：

```text
FC03
FC06
FC16
CRC
Slave ID
Serial Timeout
Batch Read
```

---

# 67. Mitsubishi 示例

Mitsubishi 可能同时存在多个协议族，因此需要先按协议拆 Driver：

```text
Mitsubishi
   |
   +-- MC 1E Driver
   |
   +-- MC 3E / SLMP Driver
   |
   +-- MC 4E Driver
   |
   +-- FX Programming Driver
```

然后再挂 Profile：

```text
MC 3E Driver
   |
   +-- FX5U Profile
   |
   +-- iQ-F Profile
   |
   +-- Q Series Profile
```

所以：

```text
品牌
```

不是 Driver 的唯一划分依据。

真正决定是否要拆 Driver 的，是：

```text
协议报文
连接方式
握手机制
寻址方式
认证方式
```

是否发生了本质变化。

---

# 68. 新增 Driver 还是新增 Profile 的判断规则

建议使用以下规则：

| 场景 | 新增 Driver | 新增 Profile |
|---|---:|---:|
| 新型号，协议完全相同 | 否 | 是 |
| 新品牌，但使用标准 Modbus | 否 | 是 |
| 只是寄存器地址不同 | 否 | 是 |
| 只是缩放、单位不同 | 否 | 是 |
| 只是报警码、状态枚举不同 | 否 | 是 |
| 同协议，仅型号能力不同 | 通常否 | 是 |
| 新品牌使用全新私有协议 | 是 | 是 |
| 同品牌但底层协议完全不同 | 是 | 是 |
| 报文结构发生本质变化 | 是 | 是 |
| 握手 / 认证机制完全不同 | 是 | 是 |
| 传输层变化但协议相同 | 通常复用协议实现 | 视情况 |
| 厂商 SDK 与普通网络协议完全不同 | 是 | 是 |

核心判断：

> 数据位置和语义变化，通常属于 Profile。

> 通信机制和报文语义变化，通常属于 Driver。

---

# 69. 不同品牌共用一个 Driver 的场景

这是 Device Profile 架构最重要的价值之一。

例如 Modbus RTU Driver 可以同时服务：

```text
汇川变频器
台达变频器
施耐德电表
国产温控器
称重仪
流量计
传感器
```

对应：

```text
                    Modbus RTU Driver
                           |
        +------------------+------------------+
        |                  |                  |
        v                  v                  v
 Inovance MD500      Delta VFD-E       Power Meter X
    Profile             Profile            Profile
```

协议实现只有一份。

---

# 70. Profile 可以覆盖型号族，而不一定每个型号一个文件

也不需要机械地做到：

```text
每个型号 = 一个 Profile
```

如果多个型号数据模型完全兼容，可以定义：

```text
Profile Family
```

例如：

```text
inovance-md500-series
```

支持：

```text
MD500
MD500E
MD520
```

或者：

```text
fanuc-0i-family
```

然后在 Profile 内声明：

```json
{
  "models": [
    "0i-F",
    "0i-F Plus"
  ]
}
```

只有在：

```text
寄存器
功能
地址空间
API
能力
```

明显不同时才拆成独立 Profile。

---

# 71. 推荐的仓库目录

建议进一步调整为：

```text
iot-edge/
│
├── crates/
│   ├── edge-core/
│   ├── driver-sdk/
│   ├── driver-loader/
│   ├── profile-engine/
│   ├── domain-model/
│   ├── device-manager/
│   ├── poll-engine/
│   └── data-pipeline/
│
├── drivers/
│   ├── modbus/
│   ├── siemens-s7/
│   ├── siemens-s7plus/
│   ├── ethernet-ip/
│   ├── omron-fins/
│   ├── mitsubishi-mc/
│   └── fanuc-focas/
│
├── profiles/
│   ├── siemens/
│   ├── mitsubishi/
│   ├── omron/
│   ├── inovance/
│   ├── fanuc/
│   ├── hnc/
│   ├── gsk/
│   └── knd/
│
└── domains/
    ├── plc/
    ├── cnc/
    ├── robot/
    ├── drive/
    ├── meter/
    └── sensor/
```

---

# 72. 最终关系模型

最终建议明确成：

```text
Physical Device
      |
      v
Device Instance
      |
      +----------------------+
      |                      |
      v                      v
   Domain                 Profile
                              |
                              v
                           Driver
                              |
                              v
                          Transport
```

数据向上：

```text
Transport
    |
    v
Protocol Driver
    |
    v
Device Profile
    |
    v
Domain Model
    |
    v
Observation
```

例如：

```text
FANUC 0i-F Plus
      |
      v
Profile: fanuc-0i-f-plus
      |
      v
Driver: fanuc-focas
      |
      v
Domain: cnc
      |
      v
cnc.spindle.1.speed
```

或者：

```text
Inovance MD500
      |
      v
Profile: inovance-md500
      |
      v
Driver: modbus-rtu
      |
      v
Domain: drive
      |
      v
drive.output.frequency
```

---

# 73. 最终划分原则总结

推荐固定以下三条规则：

> **Driver 按通信协议划分。**

> **Device Profile 按品牌 + 系列/型号划分。**

> **Domain Model 按设备类别划分。**

三者解决的问题完全不同：

```text
Domain
回答：
“它是什么设备？”
```

```text
Driver
回答：
“怎么和它通信？”
```

```text
Profile
回答：
“这个具体型号的数据在哪里、怎么解释？”
```

这样才能在后续支持几百个协议、几千个设备型号时，仍然保持 Core 和 Driver 架构稳定。

---

# 74. 反向控制：Telemetry 与 Control 双向架构

平台明确区分：

```text
上行：Telemetry / Observation
设备 -> 平台

下行：Control
平台 -> 设备
```

Control 统一包含两种操作：`PropertyWrite` 和 `CommandExecute`。操作本身由第 80.1 节正式定义的 `ControlOperation` 表示，并由顶层 `ControlRequest` 信封承载。

整体模型：

```text
                Unified Device Model
                        |
        +---------------+---------------+
        |                               |
        v                               v
 Observation / Telemetry             Control
        |                               |
        v                               v
 MQTT / OPC UA / DB              Control Engine
                                        |
                             +----------+----------+
                             |                     |
                             v                     v
                      Property Write          Command Execute
                             |                     |
                             +----------+----------+
                                        |
                                        v
                                  Profile Mapping
                                        |
                                        v
                                    Driver
```

> **Normative：** 所有北向写入，包括 writable Property，都必须经过 Control Engine，不允许 REST/MQTT/OPC UA 直接调用 `Driver.write()`。

---

# 75. Property Write 与 Command 必须分开，但共享安全链路

## 75.1 Property Write

Property Write 表示改变一个属性值，例如：

```text
PLC M10.0 = true
Drive frequency = 50 Hz
CNC macro[100] = 20
```

平台请求模型：

```rust
pub struct PropertyWriteRequest {
    pub items: Vec<PropertyWriteItem>,
}

pub struct PropertyWriteItem {
    pub path: PropertyPath,
    pub value: Value,
}
```

Profile Engine 将语义路径映射成 `DriverWriteItem { address, raw_value }`。

## 75.2 Command

Command 表示执行动作，例如：

```text
cnc.program.start
robot.home
drive.reset
```

Command 使用 `CommandRequest` / `CommandResult`。

## 75.3 共同安全链路

两者必须经过完全相同的基础链路：

```text
Authentication
Authorization
Validation
Policy / Preconditions
Queue / Serialization
Audit
Correlation / request_id
Driver
```

差异仅在 Profile Mapping 和 Driver 最终调用：

```text
PropertyWrite -> Driver.write()
Command       -> Driver.execute()
```

不得存在“Property Write 是简单写寄存器，所以绕过 Control Engine”的例外。

---

# 76. Command 一等数据模型（Normative）

`CommandRequest` / `CommandResult` 只是 Control 的领域 payload；`request_id`、设备、状态、时间统一由顶层 `ControlRequest` / `ControlResult` 承载。

```rust
pub struct CommandParameter {
    pub name: String,
    pub value: Value,
}

pub struct CommandRequest {
    pub command: String,
    pub parameters: Vec<CommandParameter>,
}

pub struct CommandResult {
    pub device_code: Option<i64>,
    pub message: Option<String>,
    pub payload: Option<serde_json::Value>,
}
```

这样 Property Write 和 Command 不再各自维护另一套 request/status/timestamp 生命周期。

---

# 77. 异步控制状态

很多工业控制不是瞬时完成的。

例如：

```text
CNC program.start
        |
        v
Accepted
        |
        v
设备开始执行
        |
        v
Running
        |
        v
Succeeded
```

因此不能把所有控制 API 设计成：

```text
HTTP Request
    |
    v
等待设备动作全部完成
```

更合理的是：

```text
提交命令
    |
    v
返回 request_id + Accepted
    |
    v
后台执行
    |
    v
Command Event / Result
```

例如：

```json
{
  "request_id": "cmd-8fa231",
  "status": "accepted"
}
```

后续结果：

```json
{
  "request_id": "cmd-8fa231",
  "status": "succeeded"
}
```

---

# 78. Command Descriptor（Normative）

标准业务 Command Descriptor 属于 **Domain + Device Profile**，不是 Driver 的领域能力声明。

```rust
pub struct CommandParameterDescriptor {
    pub name: String,
    pub data_type: DataType,
    pub required: bool,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

pub enum CommandRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

pub struct CommandDescriptor {
    pub id: String,
    pub parameters: Vec<CommandParameterDescriptor>,
    pub risk_level: CommandRiskLevel,
}
```

例如 Profile 可以声明：

```json
[
  {
    "id": "cnc.program.start",
    "parameters": [],
    "risk_level": "high"
  },
  {
    "id": "cnc.program.select",
    "parameters": [
      {
        "name": "program",
        "type": "string",
        "required": true
      }
    ],
    "risk_level": "medium"
  }
]
```

Driver 只需要声明协议层 `execute = true/false`，以及实现 Profile 映射后的 `DriverCommand`。

---

# 79. Command、Profile、Domain 的职责

反向控制仍然遵守现有三层原则。

```text
Driver
负责：
“这个命令怎么通过协议真正执行”
```

```text
Device Profile
负责：
“这个具体型号支持哪些命令，以及参数限制”
```

```text
Domain Model
负责：
“这个业务动作的标准名称是什么”
```

例如 FANUC：

```text
Vendor API
    |
    v
FANUC Driver
    |
    v
FANUC 0i Profile
    |
    v
cnc.program.start
```

HNC 也可以映射：

```text
HNC Vendor API
    |
    v
HNC Driver
    |
    v
HNC 848 Profile
    |
    v
cnc.program.start
```

上层应用只使用：

```text
cnc.program.start
```

不需要理解厂商 API。

---

# 80. Domain 标准控制命令

## CNC

建议标准命令：

```text
cnc.program.start
cnc.program.stop
cnc.program.select

cnc.alarm.reset

cnc.override.feed.set
cnc.override.spindle.set
```

## Robot

```text
robot.program.start
robot.program.stop

robot.reset
robot.home

robot.speed_override.set
```

## Drive

```text
drive.start
drive.stop
drive.reset

drive.frequency.set
```

## PLC

PLC 更多使用 Property Write：

```text
plc.property.write
```

但也可以定义高层业务 Command，例如：

```text
plc.machine.start
plc.machine.stop
plc.machine.reset
```

这些高层 Command 可以由 Profile 映射为底层 bit / register 写操作。

---

# 80.1 统一 ControlRequest / ControlResult（Normative）

Property Write 与 Command Execute 使用同一个控制信封和结果模型。`PropertyWriteRequest` 和 `CommandRequest` 使用第 75/76 节的 payload 定义。

```rust
pub struct ControlError {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

pub struct PropertyWriteItemResult {
    pub path: PropertyPath,
    pub success: bool,
    pub protocol_code: Option<i64>,
    pub error: Option<ControlError>,
}

pub enum ControlOperation {
    PropertyWrite(PropertyWriteRequest),
    CommandExecute(CommandRequest),
}

pub struct ControlRequest {
    pub request_id: String,
    pub namespace: String,
    pub device_id: DeviceId,
    pub requested_at_ns: TimestampNs,
    pub timeout_ms: u64,
    pub operation: ControlOperation,
}

pub enum ControlStatus {
    Accepted,
    Running,
    Succeeded,
    Failed,
    Rejected,
    Timeout,
    Cancelled,
    Indeterminate,
}

pub enum ControlPayloadResult {
    PropertyWrite(Vec<PropertyWriteItemResult>),
    Command(CommandResult),
}

pub struct ControlResult {
    pub request_id: String,
    pub namespace: String,
    pub device_id: DeviceId,
    pub status: ControlStatus,
    pub started_at_ns: Option<TimestampNs>,
    pub completed_at_ns: Option<TimestampNs>,
    pub result: Option<ControlPayloadResult>,
    pub error: Option<ControlError>,
}
```

幂等 key：

```text
(namespace, device_id, request_id)
```

- 下发 Driver 前先把 canonical payload hash 和状态持久化到 Control Journal。
- 同 key + 同 payload：返回已有状态/结果，不重复执行。
- 同 key + 不同 payload：返回 Conflict。
- MVP 幂等记录至少保留 24 小时，可配置延长；重启后恢复 Journal。
- 已发到设备但结果不确定且无法安全查询时标记 `Indeterminate`。
- `High/Critical` 的 `Indeterminate` 控制禁止自动重放。
- 只有 Profile 明确声明 replay-safe/idempotent 时才允许自动重试控制动作。

---

# 81. Control Engine（Normative）

北向控制请求不能直接进入 Driver。

不允许：

```text
MQTT / REST / OPC UA
        |
        v
driver.write() / driver.execute()
```

统一链路：

```text
REST / MQTT / OPC UA / Web UI
               |
               v
           Control API
               |
               v
        Authentication
               |
               v
         Authorization
               |
               v
           Validation
               |
               v
      Safety / Policy Check
               |
               v
          Control Queue
               |
               v
        Profile Mapping
               |
               v
            Driver
               |
               v
            Device
```

`ControlRequest` 可以是 Property Write 或 Command Execute；两者没有安全绕行路径。

---

# 82. Control Engine 职责

至少包括：

```text
Authentication
Authorization
Parameter / Type Validation
Profile Range Validation
Precondition Check
Policy / Risk Level
Per-device Queue
Priority
Timeout
Cancellation
Deduplication / Idempotency
Audit Log
Result Correlation
```

Property Write 和 Command Execute 必须统一具备上述能力。

对于设备协议本身只能串行通信的情况，Control Queue 与 Read Scheduler 最终还要进入 Driver Session Scheduler，避免读写并发破坏协议状态。

---

# 83. 权限模型

建议最少支持：

```text
viewer
只能读取

operator
允许普通操作

engineer
允许参数修改

administrator
允许设备和系统配置
```

命令可以声明要求的权限：

```text
cnc.program.start
-> operator

cnc.parameter.write
-> engineer
```

---

# 84. 参数范围校验

Device Profile 可以直接声明控制参数限制。

例如：

```text
drive.frequency.set
```

Profile：

```text
min = 0
max = 50
unit = Hz
```

如果请求：

```text
500 Hz
```

Control Engine 在进入 Driver 前直接拒绝。

这样 Driver 不承担全部业务验证责任。

---

# 85. Command Preconditions

部分命令需要满足设备状态条件。

例如：

```text
cnc.program.start
```

可能要求：

```text
machine_mode == AUTO
alarm == false
door_state == closed
```

可以定义：

```rust
pub struct CommandPrecondition {
    pub property: String,
    pub operator: Operator,
    pub value: Value,
}
```

但必须明确：

> 软件平台中的 Preconditions 只能作为辅助保护，不能替代设备安全 PLC、安全继电器、急停回路、门锁和其他硬件安全机制。

---

# 86. Control Policy 与风险级别

建议 Command 支持风险分级：

```rust
pub enum CommandRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}
```

例如：

```text
修改普通设定值
-> Low / Medium

CNC Cycle Start
-> High

Robot Motion
-> High / Critical

安全相关动作
-> 必须由设备本身安全系统负责
```

不同风险等级可以配置：

```text
角色要求
二次确认
来源限制
本地操作要求
超时
审批流程
```

---

# 87. Command Queue

每台设备建议维护独立命令队列。

```text
Device A
   |
   +-- Command Queue

Device B
   |
   +-- Command Queue
```

避免以下命令无序并发：

```text
start
stop
reset
program.select
parameter.write
```

队列应支持：

```text
priority
timeout
cancel
deduplicate
serialization
```

---

# 88. Read Worker 与 Command Worker 分离

推荐 Driver Runtime 内部把采集与控制分开：

```text
                  Driver Runtime

          +-------------+-------------+
          |                           |
          v                           v
      Read Worker               Command Worker
          |                           |
          v                           v
      Telemetry                   Control
```

这样：

```text
100 ms 高频轮询
```

不会长期阻塞：

```text
stop
reset
```

等控制请求。

对于 Serial、Vendor SDK 等不能真正并发的设备，Driver 内部再通过统一 Session Scheduler 做安全串行化。

---

# 89. request_id / correlation_id

所有控制命令必须有唯一 ID。

例如：

```text
request_id = cmd-8fa231
```

整个链路保持：

```text
Northbound
   |
   v
Control Engine
   |
   v
Driver
   |
   v
Device
```

最终审计和结果都使用同一个 request_id。

---

# 90. Audit Log

每一个反向控制必须记录：

```text
谁执行
何时执行
来源地址
目标设备
命令名称
命令参数
request_id
执行结果
设备错误码
耗时
```

例如：

```json
{
  "user": "operator01",
  "device": "fanuc01",
  "command": "cnc.program.start",
  "request_id": "cmd-123",
  "result": "succeeded"
}
```

控制审计是工业平台必须从早期架构就保留的数据。

---

# 90.1 控制面与北向传输安全基线（Normative）

只要启用了 Control 能力，以下要求为强制要求。

## 网络监听默认值

- Collector 默认不开放远程控制监听端口。
- Edge REST/Control API 默认只监听 `127.0.0.1` / `::1`。
- 绑定 `0.0.0.0`、`::` 或非 loopback 地址必须显式配置。
- 远程 Control API 未配置 TLS 凭据时禁止启动。

## TLS / mTLS

- REST Control API 只允许 HTTPS，最低 TLS 1.2，推荐 TLS 1.3。
- MQTT 生产部署必须使用 TLS；Managed Collector 推荐并默认使用 mTLS。
- Manager 与 Collector/Edge 的配置、证书、Driver/Profile 下发必须使用 TLS；受管模式使用 mTLS 设备身份。
- 生产环境禁止 `insecure_skip_verify=true` 一类跳过证书验证配置。
- 南向协议原生支持安全模式时应优先启用；不支持 TLS 的传统 PLC/CNC 协议必须依赖工控网络分区、ACL/VLAN/防火墙等边界。

## 证书与密钥

- 私钥、密码、Token 禁止进入 Device Profile、普通明文配置、日志和诊断 dump。
- Core 使用 `SecretProvider` 抽象读取秘密。
- Windows MVP 优先使用 DPAPI/Windows Credential；Linux MVP 优先使用 systemd credentials 或服务账户专用 `0600` secret 文件。
- mTLS 私钥应允许后续扩展 TPM/HSM。
- 证书支持轮换和短暂 overlap。
- 认证失败、证书过期、密钥读取失败必须 fail closed，不能回退明文控制。

默认：

```text
collector build: control disabled
edge build: control endpoint disabled until TLS + auth configured
```

远程 Property Write / Command Execute 必须经过：

```text
TLS -> Authentication -> Authorization -> Validation
    -> Policy -> Durable Control Journal -> Queue -> Driver
```

# 90.2 REST 控制面认证（Normative）

启用 Control 能力时，REST 控制 API（§31.5 `POST /api/v1/devices/{device_id}/controls`、`GET /api/v1/devices/{device_id}/control-requests/{request_id}`）必须启用认证。MVP 采用静态 Bearer Token 方案；mTLS 客户端证书认证留给 Edge/Manager 阶段扩展。

## 凭据形态

- 客户端每次请求携带 `Authorization: Bearer <token>` 头。
- Token 必须为高熵随机串（建议 ≥ 32 字节的 base64/hex 编码）；禁止低熵口令。
- 凭据文件（JSON，schema 显式版本化）：

```json
{
  "schema": "forgelink.control.credentials.v1",
  "credentials": [
    { "token": "<高熵随机串>", "subject": "alice", "role": "operator" },
    { "token": "<高熵随机串>", "subject": "bob", "role": "viewer" }
  ]
}
```

- 主配置（YAML）只允许配置凭据文件**路径**，禁止内联 Token（§90.1：Token 禁止进入普通明文配置）。
- 凭据文件权限：Linux/Unix 必须为 `0600`（启动时校验，过宽拒绝启动）；Windows 使用服务账户 ACL 保护（MVP 不做 ACL 强校验）。
- `subject`/`role` 语义见 §83 角色模型；REST 层只负责**认证**（Token → subject/role 上下文），**授权**由 Control Engine 的 Authorizer 执行（§81、§83）。

## 认证行为

- Token 比较必须使用常量时间比较，防止时序侧信道枚举。
- 缺失头、格式非法、未知 Token → `401`（`forgelink.error.v1`，code `UNAUTHENTICATED`）。
- 已认证但角色不足 → 由 Control Engine 授权拒绝映射为 `403`（`INSUFFICIENT_ROLE`）。
- 凭据文件缺失、权限校验失败、解析失败、schema 不符 → **启动失败**（fail-closed，§90.1），不得降级为无认证运行。
- 同一 Token 重复出现、`subject` 为空、非法角色值 → 启动失败（fail-closed）。
- Token、`Authorization` 头与凭据文件内容禁止进入日志与诊断（§6 脱敏兜底，不记录敏感字段优先）。

## 认证范围

- **控制路由必须认证**：无有效凭据一律 `401`，loopback 监听亦不豁免（fail-closed）。
- 只读管理接口（§31.5 GET 端点）MVP 维持现状（不强制认证，依赖 loopback 默认与 §90.1 网络边界）；后续版本再统一收紧。
- 远程（非 loopback）监听必须同时满足 §90.1 的 TLS 要求；TLS 终止前的明文控制请求不得放行。MVP 实现强制：控制链路启用时 REST 只允许绑定 loopback——远程访问由带 TLS 的反向代理转发到 loopback，原生 TLS listener 就绪前不开放非 loopback 直连。

---

# 91. 设备侧轻量部署需求

平台除了完整 Edge Server，还需要支持：

> 只在设备旁边部署一个轻量采集程序。

这种场景常见于：

```text
机床边缘盒子
PLC 网关
ARM64 工控机
国产 ARM 工控板
Docker 容器
OEM 内置采集服务
```

设备侧通常只需要：

```text
连接设备
采集数据
简单缓存
上传数据
自动重连
健康检查
```

不需要完整平台功能。

---

# 92. Runtime Role

建议增加一个与 Device Profile 完全独立的概念：

```text
Runtime Role
```

推荐至少三种：

```text
collector
edge
manager
```

其中：

```text
Device Profile
=
连接的是什么设备
```

```text
Runtime Role
=
当前程序部署在哪里、承担什么职责
```

两者不能混淆。

---

# 93. Collector Agent

设备侧轻量运行形态：

```text
PLC / CNC / Robot / Instrument
              |
              v
+--------------------------------+
|        Rust Collector          |
|                                |
| Driver Loader                  |
| Device Profile                 |
| Domain Mapping                 |
| Poll Scheduler                 |
| Observation                    |
| Local Buffer                   |
| MQTT / HTTP Publisher          |
| Diagnostics                    |
+--------------------------------+
              |
              v
       Central / Edge Platform
```

Collector 可以不包含：

```text
Web UI
完整用户系统
复杂报表
Control Engine
完整数据库服务
集中设备管理
复杂北向协议
```

---

# 94. Edge Server

完整边缘节点可以包含：

```text
Driver
Profile
Domain Model

Telemetry
Control Engine

Web UI
REST API
OPC UA Server
MQTT

Local Database
Rule Engine
Diagnostics
```

适用于：

```text
工厂边缘服务器
产线服务器
工业 PC
本地数据中心
```

---

# 95. Central Manager

中心管理角色主要负责：

```text
设备配置
Profile 管理
Driver 包管理
Collector 管理
配置下发
远程状态监控
版本管理
日志
审计
统一策略
```

它不一定直接连接每台 PLC / CNC。

推荐：

```text
                    Manager
                       |
             Config / Deployment
                       |
          +------------+------------+
          |                         |
          v                         v
      Collector                  Edge Server
          |                         |
          v                         v
        Device                    Devices
```

---

# 96. 三种标准发行形态

建议未来产品明确形成：

| Runtime Role | 主要用途 | 主要能力 |
|---|---|---|
| Collector | 设备侧轻量采集 | Driver、Profile、Polling、Buffer、Upload |
| Edge | 工厂本地边缘服务 | 采集、控制、UI、缓存、处理、北向 |
| Manager | 中心管理 | 配置、部署、监控、版本和策略管理 |

三种形态共享：

```text
driver-sdk
profile-engine
domain-model
observation-model
```

但运行组件不同。

---

# 97. Rust Workspace 对 Runtime Role 的支持

推荐：

```text
iot-platform/
│
├── crates/
│   ├── driver-sdk/
│   ├── driver-loader/
│   ├── profile-engine/
│   ├── domain-model/
│   ├── poll-engine/
│   ├── control-engine/
│   ├── local-buffer/
│   ├── mqtt-client/
│   └── diagnostics/
│
└── apps/
    ├── collector/
    ├── edge-server/
    └── manager/
```

这样不是维护三套代码，而是：

```text
共享 crate
+
不同 app 组合
```

---

# 98. Collector 只读构建

对于只负责采集的现场设备，建议从编译层就能禁用控制。

例如 Cargo Features：

```toml
[features]
default = ["collector"]

collector = [
    "driver-read",
    "mqtt",
    "local-cache"
]

control = [
    "driver-write",
    "control-engine"
]

web-ui = []
opcua-server = []
```

构建设备侧纯采集版本：

```bash
cargo build --release \
  --no-default-features \
  --features collector
```

这样生成的程序中甚至可以完全不包含控制链路。

优点：

```text
体积更小
攻击面更小
依赖更少
部署更简单
```

---

# 99. Collector 按设备裁剪 Driver

Collector 不需要携带所有驱动。

例如 FANUC 专用采集盒：

```text
collector/
├── collector
├── driver-fanuc.so
└── fanuc-0i-profile.json
```

Siemens 专用采集盒：

```text
collector/
├── collector
├── driver-s7.so
└── s7-1500-profile.json
```

不需要同时部署：

```text
FINS
Mitsubishi
BACnet
IEC104
...
```

---

# 100. Profile 与采集配置下发

中心平台可以生成一个设备部署包：

```text
device.yaml
profile.json
driver.so
```

例如：

```yaml
device:
  id: cnc-01
  driver: fanuc-focas
  profile: fanuc-0i-f-plus

connection:
  host: 192.168.10.20
  port: 8193

northbound:
  type: mqtt
  broker: mqtt://10.0.0.10:1883

groups:
  - interval: 100ms
    properties:
      - cnc.axis.x.absolute_position
      - cnc.axis.y.absolute_position

  - interval: 1s
    properties:
      - cnc.spindle.1.speed
      - cnc.program.current.name
```

Collector 启动：

```text
Load Config
    |
    v
Load Driver
    |
    v
Load Profile
    |
    v
Connect Device
    |
    v
Polling
    |
    v
Observation
    |
    v
Upload
```

---

# 101. Standalone 与 Managed 两种 Collector 模式

Collector 建议同时支持：

## Standalone

本地配置：

```text
YAML / JSON
```

适合：

```text
离线工厂
OEM 设备
单机部署
调试
```

## Managed

配置来自 Manager：

```text
Manager
   |
   v
Config Deployment
   |
   v
Collector
```

Collector 核心运行时不需要因为两种模式而分叉。

---

# 102. 离线缓存与 Store-and-Forward

设备侧 Collector 必须考虑中心网络中断。

典型情况：

```text
PLC / CNC 正常
       |
       v
Collector 正常
       |
       X
MQTT / Central 网络断开
```

Collector 不能停止采集。

建议：

```text
Device
   |
   v
Collector
   |
   +------> Publisher
   |
   +------> Local Buffer
                 |
                 v
         Store-and-Forward
                 |
          网络恢复后补传
```

---

# 103. Local Buffer

推荐至少两级：

```text
Memory Queue
     |
     v
Disk WAL / Embedded DB
```

可使用：

```text
Append-only WAL
SQLite
其他嵌入式 KV / Log
```

配置：

```yaml
buffer:
  memory_records: 10000
  disk_max: 2GB
  retention: 72h
```

需要定义：

```text
最大磁盘容量
最大保留时间
过期策略
上传顺序
断点恢复
重复数据策略
```

---

# 104. Collector Watchdog 与自恢复

设备侧重点不是复杂功能，而是长期稳定。

建议：

```text
systemd / Windows Service
          |
          v
       collector
          |
          +-- Driver watchdog
          +-- Connection watchdog
          +-- Publisher reconnect
          +-- Buffer health
          +-- Memory / CPU limits
          +-- Health endpoint
```

设备断线：

```text
Exponential Backoff Reconnect
```

MQTT 断线：

```text
Local Buffer
```

Collector 进程异常：

```text
Service Restart
```

Driver 子进程异常：

```text
Restart Driver Host
```

---

# 105. Collector 与 Process Driver

对于 Vendor SDK，设备侧仍然建议支持：

```text
Collector
    |
    v
driver-host
    |
    v
Vendor SDK
```

例如：

```text
Collector
    |
    v
fanuc-driver-host
    |
    v
FOCAS SDK
```

这样厂商 SDK 崩溃不会直接带崩采集主进程。

---

# 106. Collector 的安全边界

如果设备侧节点只需要采集，推荐：

```text
完全不部署 Control Engine
```

并且：

```text
Driver ABI
仍可支持 write / command
```

但 Collector 构建不暴露控制入口。

可以进一步配置：

```text
read_only = true
```

形成双重约束：

```text
编译能力限制
+
运行时策略限制
```

---

# 107. Runtime Role 与 Driver / Profile 的关系

这几个概念是正交的：

```text
Runtime Role
Collector / Edge / Manager
```

描述：

```text
软件部署职责
```

```text
Domain
PLC / CNC / Robot / Drive
```

描述：

```text
设备业务类型
```

```text
Driver
S7 / Modbus / FOCAS / FINS
```

描述：

```text
设备通信协议
```

```text
Profile
Siemens S7-1500 / FANUC 0i / MD500
```

描述：

```text
具体品牌型号
```

例如：

```text
Runtime Role:
collector

Domain:
cnc

Driver:
fanuc-focas

Profile:
fanuc-0i-f-plus
```

完全合理。

---

# 108. 最终完整平台架构

最终建议平台形成以下总体架构：

```text
                     Northbound
          MQTT / REST / OPC UA / Web UI
                         |
          +--------------+--------------+
          |                             |
          v                             v
      Telemetry                      Control
          |                             |
          v                             v
     Observation                  Control Engine
          |                    Auth / Policy / Queue
          |                             |
          +--------------+--------------+
                         |
                         v
                  Domain Model
                         |
                         v
                  Device Profile
                         |
                         v
                 Protocol Driver
                         |
                         v
                    Transport
                         |
                         v
                      Device
```

运行形态：

```text
                      Shared Rust Core
                            |
          +-----------------+-----------------+
          |                 |                 |
          v                 v                 v
      Collector            Edge            Manager
       轻量采集           完整边缘           中心管理
```

---

# 109. 最终扩展原则

设备扩展：

```text
新设备型号
   |
   +-- 协议已存在
   |       |
   |       v
   |   新增 Profile
   |
   +-- 新协议
           |
           v
       新增 Driver
```

部署扩展：

```text
只采集
-> Collector

本地采集 + 控制 + UI
-> Edge

集中配置 + 部署 + 监控
-> Manager
```

数据方向：

```text
Device -> Platform
-> Observation

Platform -> Device
-> Command
```

最终平台不只是工业数据采集器，而是可以同时演进为：

```text
Industrial Connectivity Platform
Industrial Edge Platform
Industrial Device Management Platform
Industrial Control Gateway
```

同时仍然保持 Driver、Profile、Domain 和 Runtime Role 之间的边界清晰。

---

# 110. 规范优先级与实现检查表

为避免历史草案再次产生歧义，代码实现和 Code Review 必须按以下顺序判断：

```text
1. Normative Core Model
2. Driver Rust API / Driver ABI v1
3. Profile / Domain boundaries
4. Control Engine contract
5. Northbound MQTT / REST v1
6. MVP Acceptance Criteria
7. 其他说明性示例
```

实现前必须检查：

- [x] Driver `read()` 返回 `RawReadResult`，而不是 Observation。
- [ ] Subscription/Event callback 返回 Raw Result/Event，仍经过 Profile/Domain。
- [x] `Device` 同时包含 `domain / driver_id / profile_id`。
- [x] `Resource / Property / DataType / Value / FieldValue` 均有正式定义。
- [x] Timestamp 使用 UTC Unix Epoch ns，并区分 source / ingest。
- [x] Bad/Timeout 新结果 `value = None`；缓存值只能以 `Uncertain/Stale` 返回。
- [x] Driver ABI 定义字符串编码、ptr+len、内存释放、panic、thread、callback、error 与版本兼容。
- [x] Protocol/Profile/Domain Capabilities 不混层。
- [x] Property Write 和 Command Execute 都必须经过 Control Engine。
- [ ] MQTT 明确 topic、QoS、retain、schema、去重和重传语义。
- [x] REST Control 使用 `202 + request_id` 异步模型。
- [ ] MVP 有性能、重连、缓存、断电恢复和三平台验收测试。

其中 `[x]` 表示当前仓库已有实现与测试，`[ ]` 表示仍需后续功能或验收；
清单不替代 §34 的完整验收标准。

如果未来修改上述任何契约，应直接修改 Normative 章节并同步测试，不再通过在文末追加相互冲突的新草案来覆盖旧定义。
