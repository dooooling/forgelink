//! 驱动错误分类与 `DriverErrorInfo` 映射。
//!
//! 分类规则（决定 `retryable` 与会话处置）：
//!
//! - 连接失败/断线/超时：可重试，传输级——整体失败丢会话；
//! - 响应失步（副头不符、路由区回声不符、声明数据长与期望长不符）：
//!   不可重试但传输级——MC 3E 应答无事务号，结构自洽校验失败即视为
//!   帧错位，丢弃会话重连（「放弃路径必丢会话」纪律，见 session 模块）；
//! - MC 结束代码非 0（应答副头内）：不可重试且**非**传输级——会话仍
//!   可用。**粒度差异**：MC 批量应答只有一个结束代码，无逐项粒度——
//!   非 0 即整计划失败，映射到该计划全部 item 后继续后续计划。

use observation_model::DriverErrorInfo;

/// 驱动统一错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McError {
    /// 稳定错误码（`DriverErrorInfo.code`）。
    pub code: &'static str,
    /// 人类可读的错误详情。
    pub message: String,
    /// 协议码（MC 结束代码）。
    pub protocol_code: Option<i64>,
    /// 是否可重试。
    pub retryable: bool,
}

impl McError {
    fn new(code: &'static str, message: String, retryable: bool) -> Self {
        Self {
            code,
            message,
            protocol_code: None,
            retryable,
        }
    }

    /// 连接建立失败。
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

    /// 响应结构无效（副头不符、路由区回声不符、数据长不符）。
    /// 会话已失步，必须丢弃重连。
    pub fn invalid_response(message: String) -> Self {
        Self::new("invalid_response", message, false)
    }

    /// 应答副头不是 0x00D0（失步丢会话）。
    pub fn unexpected_subheader(got: u16) -> Self {
        Self::new(
            "unexpected_subheader",
            format!("应答副头错位：期望 0x00D0，收到 {got:#06x}"),
            false,
        )
    }

    /// MC 结束代码非 0（协议级；会话保留；protocol_code = 原始码）。
    pub fn mc_error_response(end_code: u16) -> Self {
        Self {
            code: "mc_error_response",
            message: format!("MC 结束代码 {end_code:#06x}"),
            protocol_code: Some(i64::from(end_code)),
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

    /// 期望类型非法。
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
    /// 重连（§22、§34.3）；其余（结束代码、地址/解码失败）属于协议或
    /// 业务级，会话仍可用。
    pub fn is_transport_level(&self) -> bool {
        matches!(
            self.code,
            "connection_lost"
                | "connection_failed"
                | "timeout"
                | "invalid_response"
                | "unexpected_subheader"
        )
    }
}

impl std::fmt::Display for McError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for McError {}
