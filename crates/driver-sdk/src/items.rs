//! Driver 请求项类型（§10、§15 Normative）。

use observation_model::{DataType, RawValue};
use serde::{Deserialize, Serialize};

/// Driver 读取请求项（§10、§15 Normative）。
///
/// # 地址边界
///
/// `address` 是 Driver 私有不透明地址（如 `1!40001`、`axis.absolute[1]`），
/// 由 Profile 映射产生；Core / Domain 不理解其含义，只有 Driver 可以解析和验证。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverReadItem {
    /// 请求批次内唯一 ID，结果通过 `RawReadResult.item_id` 关联。
    pub id: u64,
    pub address: String,
    /// 期望的协议数据类型；为 `None` 时由 Driver 根据协议自行确定。
    pub expected_type: Option<DataType>,
}

/// Driver 写入请求项（§15 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DriverWriteItem {
    pub id: u64,
    pub address: String,
    /// 协议原始值（由 Profile 完成语义值到原始值的逆变换）。
    pub value: RawValue,
}

/// Driver 命令请求（§15 Normative）。
///
/// Profile 把标准业务命令（如 `cnc.program.start`）映射为本类型；
/// Driver 不知道领域路径。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DriverCommand {
    /// Driver/协议层的命令 ID。
    pub command_id: String,
    /// 协议命令参数（JSON），结构由 Driver 定义。
    pub payload: serde_json::Value,
}
