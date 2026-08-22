//! 设备目录抽象（§81：设备存在性与 Profile 解析）。
//!
//! Control Engine 需要判断「设备是否存在且已启用」（§4.2 `Device.enabled`），
//! 并取得该设备的 `DeviceProfile` 用于校验与映射（§37、§79）。本 crate 通过
//! [`DeviceCatalog`] trait 解耦设备来源；上层（Device Manager）负责实现。
//!
//! Core / 本引擎只消费语义路径与 Profile 提供的 `driver_address`，不解析
//! Driver 地址（§10）。

use std::collections::HashMap;

use observation_model::DeviceId;
use profile_engine::DeviceProfile;

/// 设备控制视图（设备存在性 + 启用状态 + 绑定 Profile）。
#[derive(Debug, Clone)]
pub struct DeviceInfo {
    /// 设备是否参与采集/控制（§4.2 `Device.enabled`）。
    pub enabled: bool,
    /// 绑定的 Device Profile（§37）。
    pub profile: std::sync::Arc<DeviceProfile>,
}

impl DeviceInfo {
    pub fn new(enabled: bool, profile: std::sync::Arc<DeviceProfile>) -> Self {
        Self { enabled, profile }
    }
}

/// 设备目录抽象（§81）。
///
/// 接口必须可替换；生产实现基于 Device Manager 的设备注册表，
/// 本 crate 提供内存版 [`MemoryDeviceCatalog`] 供测试与初期使用。
pub trait DeviceCatalog: Send + Sync {
    fn device(&self, device_id: &DeviceId) -> Option<DeviceInfo>;

    /// 全部已登记设备 ID（§34.2.1 per-device 指标维度的取值域：装配期
    /// 静态清单，数量有界且可枚举）。默认空——未实现的目录退化为只有
    /// 全局聚合维度（不破坏外部实现者）。
    fn device_ids(&self) -> Vec<DeviceId> {
        Vec::new()
    }
}

/// 内存版设备目录（测试与初期使用）。
#[derive(Debug, Default)]
pub struct MemoryDeviceCatalog {
    devices: HashMap<DeviceId, DeviceInfo>,
}

impl MemoryDeviceCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记设备（同 ID 覆盖）。
    pub fn insert(&mut self, device_id: DeviceId, info: DeviceInfo) {
        self.devices.insert(device_id, info);
    }

    /// 便捷登记：按 Profile 创建已启用的设备视图。
    pub fn insert_profile(&mut self, device_id: DeviceId, profile: std::sync::Arc<DeviceProfile>) {
        self.insert(device_id, DeviceInfo::new(true, profile));
    }

    /// 便捷登记：已禁用的设备视图（控制应被拒绝，§4.2）。
    pub fn insert_disabled(&mut self, device_id: DeviceId, profile: std::sync::Arc<DeviceProfile>) {
        self.insert(device_id, DeviceInfo::new(false, profile));
    }
}

impl DeviceCatalog for MemoryDeviceCatalog {
    fn device(&self, device_id: &DeviceId) -> Option<DeviceInfo> {
        self.devices.get(device_id).cloned()
    }

    fn device_ids(&self) -> Vec<DeviceId> {
        self.devices.keys().cloned().collect()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use observation_model::Value;

    use super::*;

    #[test]
    fn memory_catalog_returns_unknown_as_none() {
        let catalog = MemoryDeviceCatalog::new();
        assert!(catalog.device(&"dev-1".to_owned()).is_none());
    }

    #[test]
    fn memory_catalog_returns_inserted_device() {
        let profile = profile_for_test();
        let mut catalog = MemoryDeviceCatalog::new();
        catalog.insert_profile("dev-1".to_owned(), profile.clone());
        let info = catalog.device(&"dev-1".to_owned()).unwrap();
        assert!(info.enabled);
        assert_eq!(info.profile.id, "test-profile");
    }

    #[test]
    fn disabled_device_view() {
        let profile = profile_for_test();
        let mut catalog = MemoryDeviceCatalog::new();
        catalog.insert_disabled("dev-1".to_owned(), profile);
        let info = catalog.device(&"dev-1".to_owned()).unwrap();
        assert!(!info.enabled);
    }

    /// 供测试使用的简易 Profile（一读一写属性 + 一条命令）。
    pub(crate) fn profile_for_test() -> std::sync::Arc<DeviceProfile> {
        use observation_model::{
            CommandParameterDescriptor, CommandPrecondition, CommandRiskLevel, DataType,
            DomainKind, Operator, PropertyPath,
        };
        use profile_engine::{
            AcquisitionConstraints, ProfileCapabilities, ProfileCommand, ProfileProperty,
            WriteRounding,
        };

        let profile = DeviceProfile {
            id: "test-profile".to_owned(),
            vendor: "test".to_owned(),
            family: "test-family".to_owned(),
            models: vec!["test-1".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "test-driver".to_owned(),
            properties: vec![
                ProfileProperty {
                    path: PropertyPath::from("drive.output.frequency".to_owned()),
                    driver_address: "1!40001".to_owned(),
                    raw_type: DataType::U16,
                    value_type: DataType::F64,
                    unit: Some("Hz".to_owned()),
                    scale: 0.01,
                    offset: 0.0,
                    write_rounding: WriteRounding::Exact,
                    readable: true,
                    writable: true,
                    default_interval_ms: Some(1000),
                    min: Some(Value::F64(0.0)),
                    max: Some(Value::F64(400.0)),
                },
                ProfileProperty {
                    path: PropertyPath::from("drive.mode".to_owned()),
                    driver_address: "1!40002".to_owned(),
                    raw_type: DataType::U16,
                    value_type: DataType::String,
                    unit: None,
                    scale: 0.0,
                    offset: 0.0,
                    write_rounding: WriteRounding::Exact,
                    readable: true,
                    writable: false,
                    default_interval_ms: None,
                    min: None,
                    max: None,
                },
            ],
            commands: vec![ProfileCommand {
                id: "drive.reset".to_owned(),
                driver_command_id: "reset".to_owned(),
                parameters: vec![CommandParameterDescriptor {
                    name: "ack".to_owned(),
                    data_type: DataType::Bool,
                    required: true,
                    min: None,
                    max: None,
                }],
                risk_level: CommandRiskLevel::Medium,
                preconditions: vec![CommandPrecondition {
                    property: "drive.mode".to_owned(),
                    operator: Operator::Eq,
                    value: Value::String("auto".to_owned()),
                }],
            }],
            capabilities: ProfileCapabilities {
                supported_properties: vec![
                    PropertyPath::from("drive.output.frequency".to_owned()),
                    PropertyPath::from("drive.mode".to_owned()),
                ],
                supported_commands: vec!["drive.reset".to_owned()],
                acquisition: AcquisitionConstraints::default(),
                limits: Default::default(),
            },
        };
        std::sync::Arc::new(profile)
    }
}
