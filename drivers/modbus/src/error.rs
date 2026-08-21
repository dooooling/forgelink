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
    /// 人类可读的错误详情（如连接目标、帧内容、异常说明）。
    pub message: String,
    /// Modbus 异常码（仅 `modbus_exception` 类错误）。
    pub protocol_code: Option<i64>,
    /// 是否可重试（连接/超时/设备瞬态为 true；配置、地址、解码、非法参数为 false）。
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

    /// 期望类型 Tag 非法（未知 Tag 或 Array/Struct 复杂类型缺 schema，§17.2）。
    pub fn invalid_type(message: String) -> Self {
        Self::new("invalid_type", message, false)
    }

    /// Driver 内部 panic 被 C ABI 边界捕获（§17.7 DRIVER_PANIC）。
    pub fn driver_panic(message: String) -> Self {
        Self::new("DRIVER_PANIC", message, false)
    }

    /// 标准 `Unsupported` 错误（capability 为 `false` 的方法必须返回，§15）。
    ///
    /// code 固定为 `"unsupported"`，调用方据此稳定识别“能力未声明”，
    /// 与 `QualityReason::Unsupported` 及能力声明语义一致；不得使用
    /// 非标准码（如 `not_implemented`）替代。
    pub fn unsupported(feature: &str) -> Self {
        Self::new(
            "unsupported",
            format!("{feature} 未声明（capability=false，本阶段支持读取与写入）"),
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

    /// 是否属于传输级错误：连接断开 / 建连失败 / 超时 / 响应失步。
    ///
    /// 这类错误表示会话已不可用（或设备不可达），批量读取必须整体失败
    /// 返回（PollDriver 约定：连接错误与超时返回整体失败，由上层退避/重连，
    /// §22、§34.3）；不得转成单项错误伪装成成功批次。其余错误（从站异常、
    /// 解码失败等）属于协议/业务级，会话仍可用，可逐项标记。
    pub fn is_transport_level(&self) -> bool {
        matches!(
            self.code,
            "connection_lost" | "connection_failed" | "timeout" | "invalid_response"
        )
    }
}

impl std::fmt::Display for ModbusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ModbusError {}
