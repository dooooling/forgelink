//! 调用级错误模型 V2（Runtime V2 方案 §28 Normative）。
//!
//! 保持现有 `observation_model::DriverErrorInfo` 的公开语义不变；本模块新增
//! Runtime 内部使用的调用级分类包装 [`DriverCallError`]：
//!
//! - `RawReadResult.error` 等现有逐项错误仍保持 `DriverErrorInfo` 语义；
//! - Host/Session 级失败使用 `DriverCallError`，到 Profile/Domain 映射前
//!   再落回既有 Quality 规则（方案 §6.1）。
//!
//! # 北向边界（§28.1）
//!
//! `DriverErrorCategory` 是 Runtime 内部分类，**不得直接成为 REST v1 / MQTT
//! 的新公开枚举**：`DriverCallError.info.code` 经 control-engine 现有稳定码
//! 白名单映射（未知码归一 `driver_error`）；MVP 的 `HostUnavailable` 不新增
//! REST code——调用级失败复用 `driver_call_failed`，连接 CircuitOpen 复用
//! `connection_failed`。

use serde::{Deserialize, Serialize};

/// Driver 调用失败的 Runtime 内部分类（§28）。
///
/// 跨 C ABI 时由 `driver-abi` 层映射为稳定 `u32` category code；
/// 禁止把 Rust enum 默认布局直接暴露给插件（§28）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriverErrorCategory {
    /// 配置非法（连接参数等）。
    Config,
    /// 地址无法被 Driver 解析或校验失败。
    InvalidAddress,
    /// 能力未声明（capability 为 false 时的标准拒绝，§15）。
    Unsupported,

    /// 连接建立失败。
    ConnectionFailed,
    /// 已建立连接意外中断。
    ConnectionLost,
    /// 调用超时。
    Timeout,

    /// 协议帧/序列违反协议规范。
    ProtocolViolation,
    /// 设备显式拒绝（协议异常码等）。
    DeviceRejected,

    /// Driver 实现内部 panic（进程内可观测）。
    DriverPanic,
    /// Driver 进程崩溃（Host 场景）。
    DriverCrashed,
    /// Driver Host 不可达/不可用。
    HostUnavailable,

    /// 取消（未开始或可证明未生效）。
    Cancelled,
    /// 截止时间已过（含队列等待期过期）。
    DeadlineExceeded,
}

/// Host/Session 级调用错误包装（§28）。
///
/// `info` 携带稳定码与消息（北向白名单映射的输入），`category` 供 Runtime
/// 做故障恢复决策与指标分类（内部使用，不北向透传）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverCallError {
    pub info: crate::DriverErrorInfo,
    pub category: DriverErrorCategory,
}

impl DriverCallError {
    /// 以稳定码 + 分类构造（message 由调用方补足）。
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        category: DriverErrorCategory,
    ) -> Self {
        Self {
            info: crate::DriverErrorInfo {
                code: code.into(),
                message: message.into(),
                protocol_code: None,
                retryable: false,
            },
            category,
        }
    }
}

impl std::fmt::Display for DriverCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (category={:?}): {}",
            self.info.code, self.category, self.info.message
        )
    }
}

impl std::error::Error for DriverCallError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_error_round_trips_through_json() {
        let err = DriverCallError {
            info: crate::DriverErrorInfo {
                code: "timeout".to_owned(),
                message: "读取超时".to_owned(),
                protocol_code: None,
                retryable: true,
            },
            category: DriverErrorCategory::Timeout,
        };
        let json = serde_json::to_string(&err).expect("序列化失败");
        let back: DriverCallError = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(err, back);
    }

    #[test]
    fn category_is_internal_not_northbound() {
        // §28.1：分类是 Runtime 内部概念，serde 表示仅供内部日志/metrics 使用；
        // 稳定码经 info.code 进入既有白名单，此处固化 snake_case 形状防漂移。
        assert_eq!(
            serde_json::to_string(&DriverErrorCategory::HostUnavailable).expect("序列化失败"),
            r#""host_unavailable""#
        );
    }
}
