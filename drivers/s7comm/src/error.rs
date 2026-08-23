//! 驱动错误分类与 `DriverErrorInfo` 映射。
//!
//! 分类规则（决定 `retryable`）：
//!
//! - 连接失败/断线/超时：可重试（`retryable = true`）；
//! - 响应失步（pdu_ref 错位、item count 不符、TPKT/COTP 结构坏）：不可
//!   重试，但属传输级——会话已失步必须丢弃重连（超时后迟到的响应帧会
//!   与后续请求错位，与 modbus 同一论证）；
//! - S7 协议错误（Ack_Data 整体否定、逐项 return code 非 0xFF）：不可
//!   重试且非传输级——会话仍可用，逐项标记。
//!
//! `protocol_code` 保留 S7 item 返回码或 Ack_Data error_class/code。

use observation_model::DriverErrorInfo;

/// 驱动统一错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S7Error {
    /// 稳定错误码（`DriverErrorInfo.code`）。
    pub code: &'static str,
    /// 人类可读的错误详情（如连接目标、帧内容、协议说明）。
    pub message: String,
    /// S7 协议码（item 返回码，或 Ack_Data 的 class<<8|code）。
    pub protocol_code: Option<i64>,
    /// 是否可重试（连接/超时为 true；其余 false）。
    pub retryable: bool,
}

impl S7Error {
    fn new(code: &'static str, message: String, retryable: bool) -> Self {
        Self {
            code,
            message,
            protocol_code: None,
            retryable,
        }
    }

    /// 连接建立失败（拒绝、无路由、COTP/Setup 握手失败）。
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

    /// 响应结构无效（TPKT/COTP/PDU 结构坏、pdu_ref 不匹配、item count
    /// 不符、数据区长度不符）。会话已失步，必须整体失败并重连。
    pub fn invalid_response(message: String) -> Self {
        Self::new("invalid_response", message, false)
    }

    /// 响应 function 与请求错位（Read 得到 Write 应答等）。失步丢会话。
    pub fn unexpected_function_code(expected: u8, got: u8) -> Self {
        Self::new(
            "unexpected_function_code",
            format!("响应功能码错位：期望 {expected:#04x}，收到 {got:#04x}"),
            false,
        )
    }

    /// Ack_Data 整体否定（error_class != 0；`protocol_code = class<<8|code`）。
    pub fn s7_error_response(error_class: u8, error_code: u8) -> Self {
        Self {
            code: "s7_error_response",
            message: format!(
                "S7 整体否定响应：error class {error_class:#04x}, code {error_code:#04x}"
            ),
            protocol_code: Some((i64::from(error_class) << 8) | i64::from(error_code)),
            retryable: false,
        }
    }

    /// 逐项访问被拒（item return 0x07；会话保留）。
    pub fn access_denied(return_code: u8) -> Self {
        Self {
            code: "access_denied",
            message: format!("S7 访问被拒（item return {return_code:#04x}）"),
            protocol_code: Some(i64::from(return_code)),
            retryable: false,
        }
    }

    /// 其余逐项失败（item return 0x05 地址越界 / 0x0A 对象不存在等；
    /// 会话保留）。
    pub fn s7_item_error(return_code: u8) -> Self {
        Self {
            code: "s7_item_error",
            message: format!("S7 项失败（item return {return_code:#04x}）"),
            protocol_code: Some(i64::from(return_code)),
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

    /// 期望类型非法（语法宽度不兼容、复杂类型缺 schema 等）。
    pub fn invalid_type(message: String) -> Self {
        Self::new("invalid_type", message, false)
    }

    /// Driver 内部 panic 被 C ABI 边界捕获（§17.7 DRIVER_PANIC）。
    pub fn driver_panic(message: String) -> Self {
        Self::new("DRIVER_PANIC", message, false)
    }

    /// 标准 `Unsupported` 错误（capability 为 `false` 的方法必须返回，
    /// §15）。code 固定为 `"unsupported"`，不得用非标准码替代。
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

    /// 是否属于传输级错误：建连失败 / 断线 / 超时 / 响应失步。
    ///
    /// 这类错误表示会话已不可用，批量读写必须整体失败返回并由上层退避/
    /// 重连（§22、§34.3）；其余（S7 协议错误、地址/解码失败）属于协议或
    /// 业务级，会话仍可用，可逐项标记。
    pub fn is_transport_level(&self) -> bool {
        matches!(
            self.code,
            "connection_lost"
                | "connection_failed"
                | "timeout"
                | "invalid_response"
                | "unexpected_function_code"
        )
    }
}

impl std::fmt::Display for S7Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for S7Error {}
