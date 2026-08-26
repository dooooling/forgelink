//! Driver Manifest v1（§20，legacy）。
//!
//! Runtime V2 Phase 3 将由 `driver-package` 的 Manifest v2 取代；
//! 在此之前四个现有 Driver 与 driver-loader 继续经由 `driver_sdk::manifest`
//! / `driver_sdk::DriverManifest` 使用本定义（方案 §6.8 Transitional）。

use serde::{Deserialize, Serialize};

use crate::abi::{ABI_MAJOR, ABI_MINOR};

/// 目标平台标识常量（§20、§29）。
///
/// 值格式与 Manifest `platforms` 字段一致。
pub mod platform {
    /// Windows x64。
    pub const WINDOWS_X86_64: &str = "windows-x86_64";
    /// Linux x64。
    pub const LINUX_X86_64: &str = "linux-x86_64";
    /// Linux ARM64。
    pub const LINUX_AARCH64: &str = "linux-aarch64";
}

/// Manifest 声明的 ABI 版本（§20）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiVersion {
    pub major: u16,
    pub minor: u16,
}

impl Default for AbiVersion {
    /// 默认值与 SDK 当前 ABI 一致（§18：首版 1.0）。
    fn default() -> Self {
        Self {
            major: ABI_MAJOR,
            minor: ABI_MINOR,
        }
    }
}

/// Driver Manifest（§20）。
///
/// Loader 必须同时验证（§20）：manifest ABI、entry symbol、
/// `DriverApiV1.abi_major/minor`、`struct_size`、required feature flags；
/// Manifest 声明与实际入口不一致时拒绝加载。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DriverManifest {
    /// 协议驱动 ID（如 `modbus-tcp`）。
    pub id: String,
    pub name: String,
    pub version: String,
    /// 入口符号名（§16、§18）。
    #[serde(default = "default_entry")]
    pub entry: String,
    pub abi: AbiVersion,
    /// 目标平台列表（值见 `platform` 模块常量）。
    pub platforms: Vec<String>,
}

fn default_entry() -> String {
    crate::abi::ENTRY_SYMBOL.to_owned()
}
