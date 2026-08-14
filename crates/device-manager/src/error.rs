//! 设备实例管理错误类型。

use std::fmt;

/// 设备实例管理错误（§63、§72 绑定校验）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceManagerError {
    /// `device_id` 重复注册。
    DuplicateDevice { device_id: String },
    /// `profile_id` 未在 Profile 注册表中找到。
    ProfileNotFound {
        device_id: String,
        profile_id: String,
    },
    /// 设备 `driver_id` 与 Profile 声明的 `driver_id` 不一致（§72：Profile → Driver）。
    DriverMismatch {
        device_id: String,
        device_driver_id: String,
        profile_driver_id: String,
    },
    /// 设备 `domain` 与 Profile 声明的 `domain` 不一致（§72：Device Instance ↔ Domain）。
    DomainMismatch {
        device_id: String,
        device_domain: String,
        profile_domain: String,
    },
    /// Driver 工厂无法创建/绑定驱动实例。
    DriverBindFailed {
        device_id: String,
        driver_id: String,
        reason: String,
    },
}

impl fmt::Display for DeviceManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateDevice { device_id } => {
                write!(f, "设备 `{device_id}` 已注册")
            }
            Self::ProfileNotFound {
                device_id,
                profile_id,
            } => write!(f, "设备 `{device_id}` 的 profile `{profile_id}` 不存在"),
            Self::DriverMismatch {
                device_id,
                device_driver_id,
                profile_driver_id,
            } => write!(
                f,
                "设备 `{device_id}` driver_id=`{device_driver_id}` 与 profile 声明 `{profile_driver_id}` 不一致"
            ),
            Self::DomainMismatch {
                device_id,
                device_domain,
                profile_domain,
            } => write!(
                f,
                "设备 `{device_id}` domain=`{device_domain}` 与 profile 声明 `{profile_domain}` 不一致"
            ),
            Self::DriverBindFailed {
                device_id,
                driver_id,
                reason,
            } => write!(
                f,
                "设备 `{device_id}` 绑定 driver `{driver_id}` 失败: {reason}"
            ),
        }
    }
}

impl std::error::Error for DeviceManagerError {}
