//! 统一控制信封与结果模型（§80.1 Normative）。

use serde::{Deserialize, Serialize};

use crate::command::{CommandRequest, CommandResult};
use crate::{DeviceId, PropertyPath, TimestampNs};

/// 控制错误（§80.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlError {
    /// 稳定错误码（如 `DEVICE_NOT_CONNECTED`）。
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

/// 单个属性写入项的逐项结果（§80.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyWriteItemResult {
    pub path: PropertyPath,
    pub success: bool,
    pub protocol_code: Option<i64>,
    pub error: Option<ControlError>,
}

/// 控制操作（§80.1）。
///
/// Property Write 与 Command Execute 使用同一个控制信封，
/// 通过本枚举区分最终 Driver 调用（`write()` / `execute()`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    PropertyWrite(crate::PropertyWriteRequest),
    CommandExecute(CommandRequest),
}

/// 统一控制请求（§80.1 Normative）。
///
/// 幂等键：`(namespace, device_id, request_id)`（§80.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlRequest {
    pub request_id: String,
    /// 命名空间（如 `plant-a`），参与幂等键。
    pub namespace: String,
    pub device_id: DeviceId,
    pub requested_at_ns: TimestampNs,
    pub timeout_ms: u64,
    pub operation: ControlOperation,
}

/// 控制执行状态（§80.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlStatus {
    Accepted,
    Running,
    Succeeded,
    Failed,
    Rejected,
    Timeout,
    Cancelled,
    /// 结果不确定（如设备状态无法确认），需要人工核查。
    Indeterminate,
}

/// 控制结果 payload（§80.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPayloadResult {
    PropertyWrite(Vec<PropertyWriteItemResult>),
    Command(CommandResult),
}

/// 统一控制结果（§80.1 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlResult {
    pub request_id: String,
    pub namespace: String,
    pub device_id: DeviceId,
    pub status: ControlStatus,
    pub started_at_ns: Option<TimestampNs>,
    pub completed_at_ns: Option<TimestampNs>,
    pub result: Option<ControlPayloadResult>,
    pub error: Option<ControlError>,
}
