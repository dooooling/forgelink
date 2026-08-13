//! Command 领域模型（§76、§78、§85 Normative）。
//!
//! `CommandRequest` / `CommandResult` 只是 Control 的领域 payload；
//! `request_id`、设备、状态、时间统一由顶层 `ControlRequest` / `ControlResult`
//! （`crate::control`）承载（§76）。

use serde::{Deserialize, Serialize};

/// 命令参数（§76）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandParameter {
    pub name: String,
    pub value: crate::Value,
}

/// 命令执行请求 payload（§76 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandRequest {
    /// 标准业务命令 ID（如 `cnc.program.start`）。
    pub command: String,
    pub parameters: Vec<CommandParameter>,
}

/// 命令执行结果 payload（§76 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandResult {
    /// 设备返回的错误码（若有）。
    pub device_code: Option<i64>,
    pub message: Option<String>,
    /// 设备返回的结构化载荷。
    pub payload: Option<serde_json::Value>,
}

/// 命令参数描述（§78 Normative）。
///
/// `min` / `max` 作用于语义值，类型与 `data_type` 一致。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandParameterDescriptor {
    pub name: String,
    pub data_type: crate::DataType,
    pub required: bool,
    pub min: Option<crate::Value>,
    pub max: Option<crate::Value>,
}

/// 命令风险级别（§78、§86）。
///
/// 用于配置角色要求、二次确认、来源限制、审批流程等策略；
/// 安全相关动作必须由设备本身安全系统负责，软件分级不能替代（§85）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// 标准业务命令描述（§78 Normative）。
///
/// 属于 Domain + Device Profile，不是 Driver 的领域能力声明。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandDescriptor {
    /// 标准业务命令 ID（如 `cnc.program.start`）。
    pub id: String,
    pub parameters: Vec<CommandParameterDescriptor>,
    pub risk_level: CommandRiskLevel,
}

/// 前置条件比较运算符（§85）。
///
/// 文档 §85 引用 `Operator` 但未给出定义；此处按比较语义补充最小集合
/// （等值/不等/大小比较），如需扩展请先更新文档。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// 命令前置条件（§85 Normative）。
///
/// 例如 `machine_mode == AUTO`、`alarm == false`。
///
/// # 安全边界
///
/// 软件中的前置条件只能作为辅助保护，**不能替代**设备安全 PLC、
/// 安全继电器、急停回路、门锁和其他硬件安全机制。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandPrecondition {
    /// 被比较的属性路径。
    pub property: String,
    pub operator: Operator,
    pub value: crate::Value,
}
