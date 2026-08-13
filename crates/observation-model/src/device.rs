//! `Device`、`DomainKind` 与 `DeviceConnection`（§4.2）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 设备业务类型（§4.2）。
///
/// 与 Runtime Role（collector/edge/manager）正交，只描述"这是什么类型的工业设备"。
///
/// JSON 编码统一 snake_case（如 `drive`、`power_device`），与文档示例一致。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// 尚未纳入标准枚举的自定义设备类别。
    Custom(String),
}

/// Driver 所需的连接配置（§4.2）。
///
/// Core 只透传该值，不解释协议私有字段；字段语义由对应 Driver 定义。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceConnection {
    /// 不透明连接配置（JSON），由 Driver 自行解析与校验。
    pub config: serde_json::Value,
}

/// 设备实例（§4.2 Normative）。
///
/// `domain` / `driver_id` / `profile_id` 三级标识（§63）分别描述
/// 设备类别、通信协议、具体品牌型号。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Device {
    pub id: crate::DeviceId,
    pub name: String,

    pub domain: DomainKind,
    /// 使用哪个协议 Driver（如 `modbus-rtu`）。
    pub driver_id: String,
    /// 使用哪个 Device Profile（如 `inovance-md500`）。
    pub profile_id: String,

    pub connection: DeviceConnection,
    /// 设备是否参与采集。
    pub enabled: bool,
    /// 可扩展标签，用于分组、过滤等上层用途。
    pub labels: BTreeMap<String, String>,
}
