//! `Quality` 与错误语义（§9 Normative）。

use serde::{Deserialize, Serialize};

/// 质量级别（§9）。
///
/// | 级别 | 含义 |
/// |---|---|
/// | `Good` | 正常读取 |
/// | `Uncertain` | 设备返回可用但可疑的值（含 Last Good Value 回退） |
/// | `Bad` | Timeout / NotConnected / 协议错误等 |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityLevel {
    Good,
    Uncertain,
    Bad,
}

/// 质量原因（§9）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityReason {
    None,
    /// 使用缓存的 Last Good Value（由 Cache 层标记，§9）。
    Stale,
    Timeout,
    NotConnected,
    InvalidAddress,
    DeviceError,
    ProtocolError,
    ConfigurationError,
    Unsupported,
}

/// 观测质量（§9 Normative）。
///
/// 关键规则：
/// - `Bad/Timeout` 的新读取结果不得伪装成 Last Good Value。
/// - Last Good Value 属于 Cache 层；只有调用方明确允许 stale fallback 时才返回，
///   并标记为 `Uncertain + Stale`。
/// - Driver 的 `protocol_quality_code`、设备错误码和错误详情在映射后应尽量
///   保留到 `protocol_code` 或诊断元数据中。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quality {
    pub level: QualityLevel,
    pub reason: QualityReason,
    /// 协议原始质量码 / 设备错误码。
    pub protocol_code: Option<i64>,
    /// 人类可读的补充说明。
    pub message: Option<String>,
}
