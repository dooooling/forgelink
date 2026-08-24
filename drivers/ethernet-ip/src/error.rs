//! 驱动错误分类与 `DriverErrorInfo` 映射。
//!
//! 分类规则（决定 `retryable` 与会话处置）：
//!
//! - 连接失败/断线/超时：可重试，传输级——整体失败丢会话；
//! - 响应失步（封装头结构坏、session handle / sender context 回显不符、
//!   item 计数与偏移越界）：不可重试但传输级——迟到帧必然错位，丢弃
//!   会话重连（与 modbus/S7 同一论证）；
//! - CIP 协议级（子服务 general status 非 0）：不可重试且**非**传输级
//!   ——会话仍可用，逐项标记后继续。

use observation_model::DriverErrorInfo;

/// 驱动统一错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtherIpError {
    /// 稳定错误码（`DriverErrorInfo.code`）。
    pub code: &'static str,
    /// 人类可读的错误详情。
    pub message: String,
    /// 协议码（封装层 status 或 CIP general status）。
    pub protocol_code: Option<i64>,
    /// 是否可重试。
    pub retryable: bool,
}

impl EtherIpError {
    fn new(code: &'static str, message: String, retryable: bool) -> Self {
        Self {
            code,
            message,
            protocol_code: None,
            retryable,
        }
    }

    /// 连接建立失败（TCP 拒绝、RegisterSession 失败）。
    pub fn connection_failed(message: String) -> Self {
        Self::new("connection_failed", message, true)
    }

    /// 已建立连接在请求过程中断开。
    pub fn connection_lost(message: String) -> Self {
        Self::new("connection_lost", message, true)
    }

    /// 请求超时（PLC 无响应）。
    pub fn timeout() -> Self {
        Self::new("timeout", "请求超时：PLC 未在期限内响应".to_owned(), true)
    }

    /// 响应结构无效（封装头坏、session handle/sender context 回显不符、
    /// item 计数/偏移越界、载荷长度不符）。会话已失步，必须丢弃重连。
    pub fn invalid_response(message: String) -> Self {
        Self::new("invalid_response", message, false)
    }

    /// 应答 command 与请求错位（镜像 S7 unexpected_function_code）。失步丢会话。
    pub fn unexpected_command_code(expected: u16, got: u16) -> Self {
        Self::new(
            "unexpected_command_code",
            format!("响应命令码错位：期望 {expected:#06x}，收到 {got:#06x}"),
            false,
        )
    }

    /// 封装层整体否定（status != 0；`protocol_code = status`）。
    pub fn enip_error_response(status: u32) -> Self {
        Self {
            code: "enip_error_response",
            message: format!("EtherNet/IP 封装层否定响应（status {status:#010x}）"),
            protocol_code: Some(i64::from(status)),
            retryable: false,
        }
    }

    /// 其余逐项失败（子服务 general status；会话保留）。
    pub fn cip_item_error(status: u8) -> Self {
        Self {
            code: "cip_item_error",
            message: format!("CIP 服务失败（general status {status:#04x}）"),
            protocol_code: Some(i64::from(status)),
            retryable: false,
        }
    }

    /// 逐项访问被拒（CIP status 0x0F privilege violation；写保护标签等）。
    pub fn access_denied(status: u8) -> Self {
        Self {
            code: "access_denied",
            message: format!("CIP 访问被拒（general status {status:#04x}）"),
            protocol_code: Some(i64::from(status)),
            retryable: false,
        }
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

    /// 期望类型非法（复杂类型拒绝、值域越界等）。
    pub fn invalid_type(message: String) -> Self {
        Self::new("invalid_type", message, false)
    }

    /// Driver 内部 panic 被 C ABI 边界捕获（§17.7 DRIVER_PANIC）。
    pub fn driver_panic(message: String) -> Self {
        Self::new("DRIVER_PANIC", message, false)
    }

    /// 标准 `Unsupported` 错误（capability 为 `false` 的方法必须返回，§15）。
    pub fn unsupported(feature: &str) -> Self {
        Self::new(
            "unsupported",
            format!("{feature} 未声明（capability=false，本驱动支持读取与写入）"),
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

    /// 是否属于传输级错误：建连失败 / 断线 / 超时 / 失步。
    ///
    /// 这类错误表示会话已不可用，批量读写必须整体失败返回并由上层退避/
    /// 重连（§22、§34.3）；其余（CIP 协议级、地址/解码失败）属于协议或
    /// 业务级，会话仍可用，可逐项标记。
    pub fn is_transport_level(&self) -> bool {
        matches!(
            self.code,
            "connection_lost"
                | "connection_failed"
                | "timeout"
                | "invalid_response"
                | "unexpected_command_code"
                | "enip_error_response"
        )
    }
}

impl std::fmt::Display for EtherIpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EtherIpError {}
