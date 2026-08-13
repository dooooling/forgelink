//! Driver 结果与订阅/历史类型（§15 Normative）。

use observation_model::{DriverErrorInfo, RawReadResult, TimestampNs};
use serde::{Deserialize, Serialize};

use crate::DriverReadItem;

/// 地址校验结果（§15）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressMetadata {
    /// 规范化后的地址（供调试与去重）。
    pub canonical_address: String,
    /// 协议实际数据类型。
    pub raw_type: Option<observation_model::DataType>,
    pub readable: bool,
    pub writable: bool,
}

/// 单次写入的原始结果（§15）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawWriteResult {
    pub item_id: u64,
    pub success: bool,
    pub protocol_code: Option<i64>,
    pub error: Option<DriverErrorInfo>,
}

/// 命令执行的原始结果（§15）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawCommandResult {
    pub success: bool,
    pub protocol_code: Option<i64>,
    pub payload: Option<serde_json::Value>,
    pub error: Option<DriverErrorInfo>,
}

/// 浏览节点（§15）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DriverBrowseNode {
    pub id: String,
    pub display_name: String,
    /// 可读取的协议地址；容器节点可为 `None`。
    pub address: Option<String>,
    pub has_children: bool,
    pub metadata: serde_json::Value,
}

/// 订阅 ID（§15）。
pub type SubscriptionId = u64;

/// 订阅请求（§15 Normative）。
///
/// `ProtocolCapabilities.subscription == true` 时 `subscribe/unsubscribe` 必须可用；
/// `events == true` 时事件同样通过订阅建立，`event_types` / `protocol_filter` 描述事件过滤。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    /// 数据变化订阅项。
    pub items: Vec<DriverReadItem>,
    /// 事件订阅类别（如 alarm、state-change）；空表示不请求厂商事件类别。
    pub event_types: Vec<String>,
    pub protocol_filter: Option<serde_json::Value>,
    /// 发布间隔；`None` 由 Driver 默认决定。
    pub publishing_interval_ms: Option<u64>,
}

/// 原始事件类别（§15）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawEventKind {
    DataChange,
    Alarm,
    StateChange,
    Diagnostic,
    Custom(String),
}

/// 原始事件（§15 Normative）。
///
/// # Callback 生命周期（§17.8）
///
/// 事件经 Driver 内部订阅链路推送；callback 中的数据只在调用期间有效，
/// Core 如需长期保存必须复制。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawEvent {
    pub subscription_id: Option<SubscriptionId>,
    pub event_id: Option<String>,
    pub kind: RawEventKind,
    pub items: Vec<RawReadResult>,
    pub payload: Option<serde_json::Value>,
    pub source_timestamp_ns: Option<TimestampNs>,
    pub sequence: Option<u64>,
    pub protocol_code: Option<i64>,
}

/// 历史查询请求（§15）。
///
/// `ProtocolCapabilities.history == true` 时 `query_history` 必须可用。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryRequest {
    pub items: Vec<DriverReadItem>,
    /// 查询窗口起点（UTC Unix Epoch 纳秒）。
    pub start_time_ns: i64,
    /// 查询窗口终点（UTC Unix Epoch 纳秒）。
    pub end_time_ns: i64,
    pub limit: Option<u32>,
    /// 分页续传令牌；由上一页 `RawHistoryPage.continuation` 返回。
    pub continuation: Option<String>,
}

/// 历史查询原始结果页（§15）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawHistoryPage {
    pub items: Vec<RawReadResult>,
    /// 下一页续传令牌；`None` 表示没有更多数据。
    pub continuation: Option<String>,
}
