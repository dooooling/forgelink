//! `Observation`（§7.3 Normative）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 唯一观测（§7.3）。
///
/// # 生成边界
///
/// `Observation` **只能**由 Profile + Domain 映射后生成；
/// Driver 只返回 `RawReadResult`（`crate::raw`），不得直接构造本类型。
///
/// # 时间语义（§8）
///
/// - `source_timestamp_ns`：设备/协议提供的数据产生时间，可为空，不得伪造设备时间。
/// - `ingest_timestamp_ns`：Edge/Core 接受并归一化该条数据的时间，必填。
///
/// # 序列化
///
/// `observation_id` 是数据点级去重键（§31.3）；`sequence` 在同一
/// device_id + 单一 Collector session 内单调递增。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    /// 观测 ID，数据点级去重键（§31.3）。
    pub observation_id: String,
    pub device_id: crate::DeviceId,
    pub path: crate::PropertyPath,

    /// 值为 `None` 时表示本次读取无有效值（如 Timeout），此时
    /// `quality` 必须为 `Bad`（§9）。
    pub value: Option<crate::Value>,
    pub quality: crate::Quality,

    pub source_timestamp_ns: Option<crate::TimestampNs>,
    pub ingest_timestamp_ns: crate::TimestampNs,

    pub sequence: u64,
    pub metadata: BTreeMap<String, String>,
}
