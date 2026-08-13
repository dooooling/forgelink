//! Driver 原始结果边界类型（§7 Normative）。
//!
//! 本模块类型在 Driver（driver-sdk）与 Profile（profile-engine）之间共享：
//! Driver 返回原始结果，Profile + Domain 映射为 `Observation` 后才能进入上层。

use serde::{Deserialize, Serialize};

/// 协议原始值（§7.1）。
///
/// Driver 完成协议解码后返回本类型，但不负责单位、缩放、业务路径和领域语义。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<RawValue>),
    Struct(Vec<RawFieldValue>),
}

/// 原始结构值的一个命名字段（§7.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawFieldValue {
    pub name: String,
    pub value: RawValue,
}

/// Driver 错误信息（§7.2）。
///
/// 错误码 `code` 为字符串类别标识；`retryable` 供上层决定是否重试。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverErrorInfo {
    pub code: String,
    pub message: String,
    pub protocol_code: Option<i64>,
    /// 是否为可重试错误。
    pub retryable: bool,
}

/// Driver 单次读取的原始结果（§7.2 Normative）。
///
/// - `item_id` 对应请求批次中 `DriverReadItem.id`。
/// - `received_timestamp_ns` 由 Core/Driver Runtime 在收到设备结果时生成。
/// - `source_timestamp_ns` 只有设备或协议明确提供可信设备时间时才填写（§8）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawReadResult {
    pub item_id: u64,
    pub value: Option<RawValue>,

    pub source_timestamp_ns: Option<crate::TimestampNs>,
    pub received_timestamp_ns: crate::TimestampNs,

    /// 协议原始质量码，映射时尽量保留到 `Quality.protocol_code`（§9）。
    pub protocol_quality_code: Option<i64>,
    pub error: Option<DriverErrorInfo>,
}
