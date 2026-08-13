//! Profile 数据模型（§13.2、§37 Normative）。

use std::collections::BTreeMap;

use observation_model::{
    CommandParameterDescriptor, CommandPrecondition, CommandRiskLevel, DataType, DomainKind,
    PropertyPath, Value,
};
use serde::{Deserialize, Serialize};

/// 写入取整策略（§37 Normative）。
///
/// - `Exact`：不能无损表示为目标 `raw_type` 时拒绝。
/// - `Nearest/Floor/Ceil/Truncate`：只有 Profile 显式声明时允许。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteRounding {
    Exact,
    Nearest,
    Floor,
    Ceil,
    Truncate,
}

/// 属性映射（§37 Normative）。
///
/// # 转换规则（§37.1）
///
/// 读取：`semantic_value = raw_value * scale + offset`；
/// 写入逆变换：`raw_candidate = (semantic_value - offset) / scale`。
/// 转换后必须按 `value_type` 做 checked conversion，禁止静默溢出。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileProperty {
    pub path: PropertyPath,
    /// Driver 私有地址（如 `1!40001`），由 Driver 解析（§10）。
    pub driver_address: String,
    /// 协议原始数据类型。
    pub raw_type: DataType,
    /// 语义数据类型。
    pub value_type: DataType,
    pub unit: Option<String>,

    /// 读取缩放系数：`semantic = raw * scale + offset`。
    pub scale: f64,
    /// 读取偏移。
    pub offset: f64,
    pub write_rounding: WriteRounding,

    pub readable: bool,
    pub writable: bool,
    /// 推荐采样周期；`None` 继承 Driver/域默认。
    pub default_interval_ms: Option<u64>,

    /// 语义值范围（非原始寄存器范围）；写入校验依据（§37.1 步骤 3）。
    pub min: Option<Value>,
    pub max: Option<Value>,
}

/// 命令映射（§37 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileCommand {
    /// 标准业务命令 ID（如 `cnc.program.start`）。
    pub id: String,
    /// Driver/协议层命令 ID（映射到 `DriverCommand.command_id`）。
    pub driver_command_id: String,
    pub parameters: Vec<CommandParameterDescriptor>,
    pub risk_level: CommandRiskLevel,
    pub preconditions: Vec<CommandPrecondition>,
}

/// 采集方式约束（§13.2 Normative）。
///
/// - `None` = 继承 Driver capability；
/// - `Some(false)` = Profile 显式禁用；
/// - `Some(true)` = Profile 要求该能力，且仅当 Driver 也支持时才生效。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AcquisitionConstraints {
    pub polling: Option<bool>,
    pub subscription: Option<bool>,
    pub events: Option<bool>,
    pub history: Option<bool>,
}

/// 型号层能力（§13.2 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileCapabilities {
    /// 该型号支持的语义属性。
    pub supported_properties: Vec<PropertyPath>,
    /// 该型号支持的标准业务命令。
    pub supported_commands: Vec<String>,
    pub acquisition: AcquisitionConstraints,
    /// 型号特定限制（如最大轴数、频率设定范围）。
    pub limits: BTreeMap<String, Value>,
}

/// Device Profile（§37 Normative）。
///
/// 描述"具体是哪一个品牌、系列、型号，以及如何解释它的数据"；
/// 动态加载于 `profiles/` 目录（§38）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceProfile {
    /// Profile ID（如 `inovance-md500`）。
    pub id: String,
    pub vendor: String,
    pub family: String,
    /// 覆盖的型号列表；兼容型号可共用 Profile Family（§70）。
    pub models: Vec<String>,
    pub domain: DomainKind,
    /// 使用的协议 Driver ID。
    pub driver_id: String,

    pub properties: Vec<ProfileProperty>,
    pub commands: Vec<ProfileCommand>,
    pub capabilities: ProfileCapabilities,
}

impl DeviceProfile {
    /// 按语义路径查询属性映射。
    pub fn property(&self, path: &str) -> Option<&ProfileProperty> {
        self.properties.iter().find(|p| p.path == path)
    }
}
