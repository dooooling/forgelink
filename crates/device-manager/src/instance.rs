//! 设备实例与设备管理器（§4.2 Device、§63 三级标识、§72 关系模型）。
//!
//! # 注册流程（§100 Collector 配置下发）
//!
//! 1. 校验 `profile_id` 已注册（§38）；
//! 2. 校验 Profile 声明的 `driver_id` 与设备一致（§72：Profile → Driver）；
//! 3. 校验 Profile 声明的 `domain` 与设备一致（§72：Device Instance ↔ Domain）；
//! 4. 通过 [`DriverFactory`] 创建驱动实例（Core 不按 `driver_id` 分支，§33）；
//! 5. 生成读取项并按采集间隔分组（§22）。
//!
//! 注册后设备实例保持"配置 + 运行时绑定"的只读视图，供上层组装
//! Poll Engine 调度与全链路映射。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use observation_model::{Device, DeviceId};
use poll_engine::{PollDriver, PollTarget};
use profile_engine::{DeviceProfile, ProfileRegistry};
use tracing::warn;

use crate::bind::DriverFactory;
use crate::error::DeviceManagerError;
use crate::read_items::{ReadGroup, ReadItem, generate_read_items, group_read_items};

/// 已绑定的设备实例（§72 Device Instance）。
///
/// - `profile`：绑定的 Device Profile（§37）；
/// - `driver`：绑定的驱动实例（Poll Driver 契约，§22），多采集组共享；
/// - `groups`：按采集间隔分组的读取项（§22 Group）。
///
/// `driver` 为 `Arc<Mutex<Box<dyn PollDriver>>>`：同一设备的所有
/// `PollTarget` 共享一个实例，由 Poll Engine 串行化调用（§17.5 调用约定）。
pub struct DeviceInstance {
    /// 设备配置（§4.2）。
    pub device: Device,
    /// 绑定的 Device Profile。
    pub profile: Arc<DeviceProfile>,
    /// 绑定的驱动实例（供 PollScheduler 共享）。
    pub driver: Arc<Mutex<Box<dyn PollDriver>>>,
    /// 全部读取项（声明序，`item_id` 即数组索引）。
    pub(crate) read_items: Vec<ReadItem>,
    /// 按采集间隔分组的读取项。
    pub groups: Vec<ReadGroup>,
}

impl DeviceInstance {
    /// 按 `item_id` 查询读取项（对应 `RawReadResult.item_id`）。
    pub fn item(&self, item_id: u64) -> Option<&ReadItem> {
        self.read_items.get(item_id as usize)
    }

    /// 生成本设备的所有 Poll 目标（§22 Group → PollTarget）。
    ///
    /// 返回的每个目标与 `driver` 一一配对即可交给
    /// [`PollScheduler`](poll_engine::PollScheduler) 调度。
    pub fn poll_targets(&self) -> Vec<PollTarget> {
        self.groups
            .iter()
            .map(|group| PollTarget {
                device_id: self.device.id.clone(),
                interval_ms: group.interval_ms,
                items: group.driver_items.clone(),
            })
            .collect()
    }
}

impl std::fmt::Debug for DeviceInstance {
    /// 仅输出配置视图；驱动实例（`dyn PollDriver`）不实现 Debug，跳过。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceInstance")
            .field("device", &self.device)
            .field("profile", &self.profile)
            .field("groups", &self.groups)
            .finish_non_exhaustive()
    }
}

/// 设备实例管理器：注册、绑定与查询（§2 Edge Core · Device Manager）。
pub struct DeviceManager {
    profiles: ProfileRegistry,
    drivers: Box<dyn DriverFactory>,
    /// 属性未声明 `default_interval_ms` 时的默认采集间隔（毫秒）。
    default_interval_ms: u64,
    devices: HashMap<DeviceId, DeviceInstance>,
}

impl DeviceManager {
    /// 新建管理器。
    ///
    /// - `profiles`：Profile 注册表（§38）；
    /// - `drivers`：Driver 工厂（Native Plugin / 测试替身）；
    /// - `default_interval_ms`：默认采集间隔，必须大于 0。
    pub fn new(
        profiles: ProfileRegistry,
        drivers: Box<dyn DriverFactory>,
        default_interval_ms: u64,
    ) -> Self {
        assert!(default_interval_ms > 0, "默认采集间隔必须大于 0");
        Self {
            profiles,
            drivers,
            default_interval_ms,
            devices: HashMap::new(),
        }
    }

    /// 注册设备实例（§100：Load Profile → Load Driver → 生成读取项）。
    ///
    /// 失败时设备不进入注册表，返回具体绑定错误。
    pub fn register_device(&mut self, device: Device) -> Result<(), DeviceManagerError> {
        if self.devices.contains_key(&device.id) {
            return Err(DeviceManagerError::DuplicateDevice {
                device_id: device.id,
            });
        }
        let profile = self.profiles.get(&device.profile_id).ok_or_else(|| {
            DeviceManagerError::ProfileNotFound {
                device_id: device.id.clone(),
                profile_id: device.profile_id.clone(),
            }
        })?;
        if profile.driver_id != device.driver_id {
            return Err(DeviceManagerError::DriverMismatch {
                device_id: device.id.clone(),
                device_driver_id: device.driver_id.clone(),
                profile_driver_id: profile.driver_id.clone(),
            });
        }
        if profile.domain != device.domain {
            return Err(DeviceManagerError::DomainMismatch {
                device_id: device.id.clone(),
                device_domain: format!("{:?}", device.domain),
                profile_domain: format!("{:?}", profile.domain),
            });
        }

        let driver = self
            .drivers
            .create_driver(&device.driver_id, &device.connection.config)
            .map_err(|error| DeviceManagerError::DriverBindFailed {
                device_id: device.id.clone(),
                driver_id: device.driver_id.clone(),
                reason: format!("{error}"),
            })?;

        let read_items = generate_read_items(profile, self.default_interval_ms);
        if read_items.is_empty() {
            warn!(
                component = "device-manager",
                device_id = %device.id,
                profile_id = %profile.id,
                error_code = "device_no_readable_property",
                "设备无任何可读属性（无轮询组）"
            );
        }
        let groups = group_read_items(read_items.clone());

        let instance = DeviceInstance {
            device,
            profile: Arc::clone(profile),
            driver: Arc::new(Mutex::new(driver)),
            read_items,
            groups,
        };
        self.devices.insert(instance.device.id.clone(), instance);
        Ok(())
    }

    /// 查询设备实例。
    pub fn get(&self, device_id: &str) -> Option<&DeviceInstance> {
        self.devices.get(device_id)
    }

    /// 移除设备实例（返回被移除的实例）。
    pub fn remove(&mut self, device_id: &str) -> Option<DeviceInstance> {
        self.devices.remove(device_id)
    }

    /// 已注册设备数。
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }

    /// 全部设备 ID（无序）。
    pub fn device_ids(&self) -> impl Iterator<Item = &DeviceId> {
        self.devices.keys()
    }
}

#[cfg(test)]
mod tests {
    use observation_model::{DataType, DomainKind, Value};
    use profile_engine::{DeviceProfile, ProfileProperty, WriteRounding};

    use super::*;
    use crate::bind::BindError;

    /// 无副作用测试工厂：按 driver_id 返回同名驱动（不连接）。
    struct StubFactory;

    impl DriverFactory for StubFactory {
        fn create_driver(
            &self,
            driver_id: &str,
            _config: &serde_json::Value,
        ) -> Result<Box<dyn PollDriver>, BindError> {
            match driver_id {
                "modbus-tcp" => Ok(Box::new(StubDriver)),
                other => Err(BindError::UnknownDriver {
                    driver_id: other.to_owned(),
                }),
            }
        }
    }

    /// 最小 PollDriver 实现（pipeline 测试直接调用，不依赖网络）。
    #[derive(Debug)]
    struct StubDriver;

    impl PollDriver for StubDriver {
        fn read_batch(
            &mut self,
            _items: &[driver_sdk::DriverReadItem],
        ) -> Result<Vec<observation_model::RawReadResult>, driver_sdk::DriverErrorInfo> {
            Ok(vec![])
        }
    }

    fn profile() -> DeviceProfile {
        DeviceProfile {
            id: "inovance-md500".to_owned(),
            vendor: "Inovance".to_owned(),
            family: "MD500".to_owned(),
            models: vec!["MD500".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "modbus-tcp".to_owned(),
            properties: vec![ProfileProperty {
                path: "drive.output.frequency".to_owned(),
                driver_address: "1!40001".to_owned(),
                raw_type: DataType::U16,
                value_type: DataType::F64,
                unit: Some("Hz".to_owned()),
                scale: 0.01,
                offset: 0.0,
                write_rounding: WriteRounding::Nearest,
                readable: true,
                writable: true,
                default_interval_ms: Some(100),
                min: Some(Value::F64(0.0)),
                max: Some(Value::F64(50.0)),
            }],
            commands: vec![],
            capabilities: profile_engine::ProfileCapabilities {
                supported_properties: vec![],
                supported_commands: vec![],
                acquisition: Default::default(),
                limits: Default::default(),
            },
        }
    }

    fn manager() -> DeviceManager {
        let mut registry = ProfileRegistry::new();
        registry.register(profile()).unwrap();
        DeviceManager::new(registry, Box::new(StubFactory), 1000)
    }

    fn device() -> Device {
        Device {
            id: "vfd-01".to_owned(),
            name: "VFD-01".to_owned(),
            domain: DomainKind::Drive,
            driver_id: "modbus-tcp".to_owned(),
            profile_id: "inovance-md500".to_owned(),
            connection: observation_model::DeviceConnection {
                config: serde_json::json!({ "mode": "tcp" }),
            },
            enabled: true,
            labels: Default::default(),
        }
    }

    #[test]
    fn registers_device_with_groups_and_poll_targets() {
        let mut mgr = manager();
        mgr.register_device(device()).unwrap();
        let instance = mgr.get("vfd-01").expect("设备已注册");
        assert_eq!(instance.device.name, "VFD-01");
        assert_eq!(instance.profile.id, "inovance-md500");
        assert_eq!(instance.groups.len(), 1);
        assert_eq!(instance.groups[0].interval_ms, 100);

        let targets = instance.poll_targets();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].device_id, "vfd-01");
        assert_eq!(targets[0].items[0].address, "1!40001");

        let item = instance.item(0).expect("item_id 0 存在");
        assert_eq!(item.path, "drive.output.frequency");
        assert!(instance.item(99).is_none());
    }

    #[test]
    fn rejects_duplicate_device() {
        let mut mgr = manager();
        mgr.register_device(device()).unwrap();
        let err = mgr.register_device(device()).unwrap_err();
        assert_eq!(
            err,
            DeviceManagerError::DuplicateDevice {
                device_id: "vfd-01".to_owned()
            }
        );
    }

    #[test]
    fn rejects_profile_driver_mismatch() {
        let mut mgr = manager();
        let mut d = device();
        d.driver_id = "s7comm".to_owned();
        let err = mgr.register_device(d).unwrap_err();
        assert!(matches!(err, DeviceManagerError::DriverMismatch { .. }));
    }

    #[test]
    fn rejects_domain_mismatch() {
        let mut mgr = manager();
        let mut d = device();
        d.domain = DomainKind::Plc;
        let err = mgr.register_device(d).unwrap_err();
        assert!(matches!(err, DeviceManagerError::DomainMismatch { .. }));
    }

    #[test]
    fn rejects_unknown_profile() {
        let mut mgr = manager();
        let mut d = device();
        d.profile_id = "no-such-profile".to_owned();
        let err = mgr.register_device(d).unwrap_err();
        assert!(matches!(err, DeviceManagerError::ProfileNotFound { .. }));
    }

    #[test]
    fn remove_and_len() {
        let mut mgr = manager();
        assert!(mgr.is_empty());
        mgr.register_device(device()).unwrap();
        assert_eq!(mgr.len(), 1);
        assert!(mgr.remove("vfd-01").is_some());
        assert!(mgr.is_empty());
    }
}
