//! 驱动错误分类与 `DriverErrorInfo` 映射。
//!
//! 分类规则（决定 `retryable`）：
//!
//! - 连接失败/断线/超时/设备瞬态：可重试（`retryable = true`）；
//! - 配置、地址、解码、异常码 0x01~0x03、未实现功能：不可重试。
//!
//! `protocol_code` 保留 Modbus 异常码或 0。

use observation_model::DriverErrorInfo;

/// 驱动统一错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModbusError {
    /// 稳定错误码（`DriverErrorInfo.code`）。
    pub code: &'static str,
    pub message: String,
    /// Modbus 异常码（仅 `modbus_exception` 类错误）。
    pub protocol_code: Option<i64>,
    pub retryable: bool,
}

impl ModbusError {
    fn new(code: &'static str, message: String, retryable: bool) -> Self {
        Self {
            code,
            message,
            protocol_code: None,
            retryable,
        }
    }

    /// 连接建立失败（拒绝、无路由等）。
    pub fn connection_failed(message: String) -> Self {
        Self::new("connection_failed", message, true)
    }

    /// 已建立连接在请求过程中断开。
    pub fn connection_lost(message: String) -> Self {
        Self::new("connection_lost", message, true)
    }

    /// 请求超时（设备无响应）。
    pub fn timeout() -> Self {
        Self::new("timeout", "请求超时：设备未在期限内响应".to_owned(), true)
    }

    /// Modbus 异常响应（`protocol_code` 为异常码）。
    pub fn modbus_exception(code: u8, name: &str) -> Self {
        Self {
            code: "modbus_exception",
            message: format!("Modbus 异常 {code:#04x}: {name}"),
            protocol_code: Some(code as i64),
            retryable: !matches!(code, 0x01..=0x03),
        }
    }

    /// 响应帧无效（unit/功能码不匹配、CRC 失败、截断）。
    pub fn invalid_response(message: String) -> Self {
        Self::new("invalid_response", message, false)
    }

    /// 地址解析失败。
    pub fn invalid_address(message: String) -> Self {
        Self::new("invalid_address", message, false)
    }

    /// 配置失败。
    pub fn config_error(message: String) -> Self {
        Self::new("config_error", message, false)
    }

    /// 单项解码失败。
    pub fn decode_error(message: String) -> Self {
        Self::new("decode_error", message, false)
    }

    /// 本 MVP 未实现的功能。
    pub fn not_implemented(feature: &str) -> Self {
        Self::new(
            "not_implemented",
            format!("{feature} 未实现（本阶段仅支持读取）"),
            false,
        )
    }

    /// 映射为 `DriverErrorInfo`（§7.2）。
    pub fn into_info(self) -> DriverErrorInfo {
        DriverErrorInfo {
            code: self.code.to_owned(),
            message: self.message,
            protocol_code: self.protocol_code,
            retryable: self.retryable,
        }
    }
}

impl std::fmt::Display for ModbusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ModbusError {}
