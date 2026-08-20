//! 控制请求校验与 Profile 映射（§74、§75、§84、§85 Normative）。
//!
//! 所有校验（设备存在/启用 → 属性/命令存在 → 可写 → 类型 → 范围 → 前置条件）
//! 在进入队列与 Driver 前完成（§84：Driver 不承担全部业务验证责任）。
//!
//! # 边界（§10、§74、§75.3）
//!
//! - 属性写入：`profile_engine::convert::encode_write` 完成类型/范围/逆变换
//!   校验并产生原始值；`DriverWriteItem.address` 直接取自
//!   `ProfileProperty.driver_address`——本引擎不解析 Driver 地址；
//! - 命令：`ProfileCommand.driver_command_id` 映射为 `DriverCommand.command_id`，
//!   参数按 descriptor 校验完整性、类型与范围；
//! - 前置条件（§85）由引擎在入队前调用 [`PreconditionChecker`](crate::PreconditionChecker)。

use driver_sdk::{DriverCommand, DriverWriteItem};
#[cfg(test)]
use observation_model::{CommandParameter, PropertyWriteItem};
use observation_model::{
    CommandPrecondition, CommandRequest, CommandRiskLevel, ControlError, DataType, PropertyPath,
    PropertyWriteRequest, Value,
};
use profile_engine::DeviceProfile;
use profile_engine::{ConversionError, ProfileProperty, encode_write};

/// 校验通过后、交给队列与执行器的已映射操作。
#[derive(Debug, Clone, PartialEq)]
pub enum ValidatedOperation {
    /// 属性批量写入：逐项 `DriverWriteItem` + 对应的语义路径（顺序一致）。
    Write {
        items: Vec<DriverWriteItem>,
        paths: Vec<PropertyPath>,
    },
    /// 命令执行：映射后的 `DriverCommand` + 风险等级（用于策略）
    /// + 前置条件（§85，由引擎在入队前检查）。
    Execute {
        command: DriverCommand,
        risk_level: CommandRiskLevel,
        preconditions: Vec<CommandPrecondition>,
    },
}

impl ValidatedOperation {
    /// 风险等级（命令）/ 策略操作种类。
    pub fn risk_level(&self) -> Option<CommandRiskLevel> {
        match self {
            ValidatedOperation::Write { .. } => None,
            ValidatedOperation::Execute { risk_level, .. } => Some(*risk_level),
        }
    }

    /// 策略操作种类（§86：属性写入 / 按风险等级的命令）。
    pub fn kind(&self) -> crate::policy::OperationKind {
        match self {
            ValidatedOperation::Write { .. } => crate::policy::OperationKind::PropertyWrite,
            ValidatedOperation::Execute { risk_level, .. } => {
                crate::policy::OperationKind::Command(*risk_level)
            }
        }
    }
}

/// 校验错误（§84 范围校验；全部在 Driver 前拒绝）。
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// 属性不存在于 Profile（§75.1）。
    PropertyNotFound { path: PropertyPath },
    /// 属性存在但 `writable == false`（§75.1）。
    PropertyNotWritable { path: PropertyPath },
    /// 语义值类型与 `value_type` 不匹配。
    ValueTypeMismatch {
        path: PropertyPath,
        expected: DataType,
    },
    /// 语义值超出 `min`/`max`（§84）。
    ValueOutOfRange {
        path: PropertyPath,
        value: f64,
        min: Option<f64>,
        max: Option<f64>,
    },
    /// 无法无损表示为协议原始值（Exact/溢出/精度损失，§37.1）。
    ValueNotRepresentable { path: PropertyPath, reason: String },
    /// 命令不存在于 Profile（§78）。
    CommandNotFound { command: String },
    /// 必填参数缺失（§78 `required`）。
    MissingParameter { command: String, parameter: String },
    /// 请求包含 Profile 未声明的参数（§78 参数列表是唯一合法集合）。
    UnknownParameter { command: String, parameter: String },
    /// 参数类型与 descriptor `data_type` 不匹配。
    ParameterTypeMismatch {
        command: String,
        parameter: String,
        expected: DataType,
    },
    /// 参数超出 descriptor `min`/`max`（§84）。
    ParameterOutOfRange {
        command: String,
        parameter: String,
        value: f64,
        min: Option<f64>,
        max: Option<f64>,
    },
    /// Profile 缩放配置非法（加载时已拦截，防御性路径）。
    ProfileConfiguration { path: PropertyPath, reason: String },
}

impl ValidationError {
    /// 稳定错误码（供 `ControlError.code` 与审计）。
    pub fn code(&self) -> &'static str {
        match self {
            ValidationError::PropertyNotFound { .. } => "PROPERTY_NOT_FOUND",
            ValidationError::PropertyNotWritable { .. } => "PROPERTY_NOT_WRITABLE",
            ValidationError::ValueTypeMismatch { .. } => "VALUE_TYPE_MISMATCH",
            ValidationError::ValueOutOfRange { .. } => "VALUE_OUT_OF_RANGE",
            ValidationError::ValueNotRepresentable { .. } => "VALUE_NOT_REPRESENTABLE",
            ValidationError::CommandNotFound { .. } => "COMMAND_NOT_FOUND",
            ValidationError::MissingParameter { .. } => "MISSING_PARAMETER",
            ValidationError::UnknownParameter { .. } => "UNKNOWN_PARAMETER",
            ValidationError::ParameterTypeMismatch { .. } => "PARAMETER_TYPE_MISMATCH",
            ValidationError::ParameterOutOfRange { .. } => "PARAMETER_OUT_OF_RANGE",
            ValidationError::ProfileConfiguration { .. } => "PROFILE_CONFIGURATION",
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::PropertyNotFound { path } => write!(f, "属性 {path} 不存在"),
            ValidationError::PropertyNotWritable { path } => write!(f, "属性 {path} 不可写"),
            ValidationError::ValueTypeMismatch { path, expected } => {
                write!(f, "属性 {path} 的值类型与 {expected:?} 不匹配")
            }
            ValidationError::ValueOutOfRange {
                path,
                value,
                min,
                max,
            } => {
                write!(
                    f,
                    "属性 {path} 的值 {value} 超出范围 min={min:?} max={max:?}"
                )
            }
            ValidationError::ValueNotRepresentable { path, reason } => {
                write!(f, "属性 {path} 的值无法无损表示: {reason}")
            }
            ValidationError::CommandNotFound { command } => write!(f, "命令 {command} 不存在"),
            ValidationError::MissingParameter { command, parameter } => {
                write!(f, "命令 {command} 缺少必填参数 {parameter}")
            }
            ValidationError::UnknownParameter { command, parameter } => {
                write!(f, "命令 {command} 包含未声明参数 {parameter}")
            }
            ValidationError::ParameterTypeMismatch {
                command,
                parameter,
                expected,
            } => {
                write!(
                    f,
                    "命令 {command} 参数 {parameter} 类型与 {expected:?} 不匹配"
                )
            }
            ValidationError::ParameterOutOfRange {
                command,
                parameter,
                value,
                min,
                max,
            } => {
                write!(
                    f,
                    "命令 {command} 参数 {parameter} 的值 {value} 超出范围 min={min:?} max={max:?}"
                )
            }
            ValidationError::ProfileConfiguration { path, reason } => {
                write!(f, "属性 {path} 的 Profile 配置非法: {reason}")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// 属性写入校验 + 映射（§75.1 → §37.1 逆变换）。
///
/// 逐项：路径存在 → `writable` → 类型/范围/逆变换（`encode_write`）→
/// `DriverWriteItem`。任一失败整批拒绝（不部分执行）。
pub fn validate_property_write(
    profile: &DeviceProfile,
    request: &PropertyWriteRequest,
) -> Result<ValidatedOperation, ValidationError> {
    let mut items = Vec::with_capacity(request.items.len());
    let mut paths = Vec::with_capacity(request.items.len());
    for (index, item) in request.items.iter().enumerate() {
        let property =
            profile
                .property(&item.path)
                .ok_or_else(|| ValidationError::PropertyNotFound {
                    path: item.path.clone(),
                })?;
        if !property.writable {
            return Err(ValidationError::PropertyNotWritable {
                path: item.path.clone(),
            });
        }
        let raw = encode_write(property, &item.value).map_err(|e| map_conversion(property, e))?;
        items.push(DriverWriteItem {
            id: index as u64,
            address: property.driver_address.clone(),
            value: raw,
        });
        paths.push(item.path.clone());
    }
    Ok(ValidatedOperation::Write { items, paths })
}

/// 命令校验 + 映射（§76、§78、§84）。
///
/// 命令存在 → 参数完整性（必填存在、无未声明参数）→ 参数类型 → 参数范围。
/// 前置条件检查由引擎在入队前完成（依赖策略挂载的检查器，§85）。
pub fn validate_command(
    profile: &DeviceProfile,
    request: &CommandRequest,
) -> Result<ValidatedOperation, ValidationError> {
    let descriptor = profile
        .commands
        .iter()
        .find(|c| c.id == request.command)
        .ok_or_else(|| ValidationError::CommandNotFound {
            command: request.command.clone(),
        })?;

    // 完整性：必填参数必须出现。
    for param in &descriptor.parameters {
        if param.required && !request.parameters.iter().any(|p| p.name == param.name) {
            return Err(ValidationError::MissingParameter {
                command: request.command.clone(),
                parameter: param.name.clone(),
            });
        }
    }
    // 完整性：不允许未声明参数。
    for param in &request.parameters {
        let Some(desc) = descriptor.parameters.iter().find(|d| d.name == param.name) else {
            return Err(ValidationError::UnknownParameter {
                command: request.command.clone(),
                parameter: param.name.clone(),
            });
        };
        if !value_matches_type(&param.value, &desc.data_type) {
            return Err(ValidationError::ParameterTypeMismatch {
                command: request.command.clone(),
                parameter: param.name.clone(),
                expected: desc.data_type.clone(),
            });
        }
        if !param_within_range(&param.value, desc.min.as_ref(), desc.max.as_ref()) {
            let (value, min, max) = range_as_f64(&param.value, &desc.min, &desc.max);
            return Err(ValidationError::ParameterOutOfRange {
                command: request.command.clone(),
                parameter: param.name.clone(),
                value,
                min,
                max,
            });
        }
    }

    let payload = serde_json::to_value(
        request
            .parameters
            .iter()
            .map(|p| (p.name.clone(), value_to_json(&p.value)))
            .collect::<serde_json::Map<_, _>>(),
    )
    .expect("参数映射为 JSON 不应失败");

    Ok(ValidatedOperation::Execute {
        command: DriverCommand {
            command_id: descriptor.driver_command_id.clone(),
            payload,
        },
        risk_level: descriptor.risk_level,
        preconditions: descriptor.preconditions.clone(),
    })
}

/// `Value` → `serde_json::Value`（命令参数透传给 Driver）。
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::I8(v) => serde_json::Value::Number((*v).into()),
        Value::I16(v) => serde_json::Value::Number((*v).into()),
        Value::I32(v) => serde_json::Value::Number((*v).into()),
        Value::I64(v) => serde_json::Value::Number((*v).into()),
        Value::U8(v) => serde_json::Value::Number((*v).into()),
        Value::U16(v) => serde_json::Value::Number((*v).into()),
        Value::U32(v) => serde_json::Value::Number((*v).into()),
        Value::U64(v) => serde_json::Value::Number((*v).into()),
        Value::F32(v) => serde_json::Number::from_f64(*v as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::F64(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(v) => serde_json::Value::String(v.clone()),
        Value::Bytes(v) => serde_json::Value::Array(
            v.iter()
                .map(|b| serde_json::Value::Number((*b).into()))
                .collect(),
        ),
        Value::Array(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Struct(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|f| (f.name.clone(), value_to_json(&f.value)))
                .collect(),
        ),
    }
}

/// 语义值类型是否与 descriptor `data_type` 兼容（数值族互通，与
/// `profile_engine` 写入语义一致）。
fn value_matches_type(value: &Value, data_type: &DataType) -> bool {
    match (data_type, value) {
        (DataType::Bool, Value::Bool(_)) => true,
        (DataType::String, Value::String(_)) => true,
        (DataType::Bytes, Value::Bytes(_)) => true,
        (DataType::Array(_), Value::Array(_)) => true,
        (DataType::Struct(_), Value::Struct(_)) => true,
        (t, v) if is_numeric_type(t) && is_numeric_value(v) => true,
        _ => false,
    }
}

fn is_numeric_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::F32
            | DataType::F64
    )
}

fn is_numeric_value(v: &Value) -> bool {
    matches!(
        v,
        Value::I8(_)
            | Value::I16(_)
            | Value::I32(_)
            | Value::I64(_)
            | Value::U8(_)
            | Value::U16(_)
            | Value::U32(_)
            | Value::U64(_)
            | Value::F32(_)
            | Value::F64(_)
    )
}

/// 参数范围校验（§84）；整数走 `i128` 精确比较（防 64 位整数经 f64 失真），
/// 其余数值走 `f64`。非数值参数无范围概念（Profile 校验阶段已保证 min/max
/// 与 `data_type` 同族，见 profile-engine validate）。
fn param_within_range(value: &Value, min: Option<&Value>, max: Option<&Value>) -> bool {
    if let (Some(v), Some(bound)) = (value_to_i128(value), min.and_then(value_to_i128)) {
        if v < bound {
            return false;
        }
    } else if let (Some(v), Some(bound)) = (value_to_f64(value), min.and_then(value_to_f64)) {
        if v < bound {
            return false;
        }
    }
    if let (Some(v), Some(bound)) = (value_to_i128(value), max.and_then(value_to_i128)) {
        if v > bound {
            return false;
        }
    } else if let (Some(v), Some(bound)) = (value_to_f64(value), max.and_then(value_to_f64)) {
        if v > bound {
            return false;
        }
    }
    true
}

fn value_to_i128(v: &Value) -> Option<i128> {
    match v {
        Value::I8(x) => Some(i128::from(*x)),
        Value::I16(x) => Some(i128::from(*x)),
        Value::I32(x) => Some(i128::from(*x)),
        Value::I64(x) => Some(i128::from(*x)),
        Value::U8(x) => Some(i128::from(*x)),
        Value::U16(x) => Some(i128::from(*x)),
        Value::U32(x) => Some(i128::from(*x)),
        Value::U64(x) => Some(i128::from(*x)),
        _ => None,
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::I8(x) => Some(*x as f64),
        Value::I16(x) => Some(*x as f64),
        Value::I32(x) => Some(*x as f64),
        Value::I64(x) => Some(*x as f64),
        Value::U8(x) => Some(*x as f64),
        Value::U16(x) => Some(*x as f64),
        Value::U32(x) => Some(*x as f64),
        Value::U64(x) => Some(*x as f64),
        Value::F32(x) => Some(*x as f64),
        Value::F64(x) => Some(*x),
        _ => None,
    }
}

fn range_as_f64(
    value: &Value,
    min: &Option<Value>,
    max: &Option<Value>,
) -> (f64, Option<f64>, Option<f64>) {
    (
        value_to_f64(value).unwrap_or(f64::NAN),
        min.as_ref().and_then(value_to_f64),
        max.as_ref().and_then(value_to_f64),
    )
}

/// `encode_write` 错误 → 稳定校验错误（§37.1 步骤 1~8 的全部失败面）。
fn map_conversion(property: &ProfileProperty, error: ConversionError) -> ValidationError {
    let path = property.path.clone();
    match error {
        ConversionError::NotWritable => ValidationError::PropertyNotWritable { path },
        ConversionError::SemanticTypeMismatch { expected } => {
            ValidationError::ValueTypeMismatch { path, expected }
        }
        ConversionError::MinMaxViolation { value, min, max } => ValidationError::ValueOutOfRange {
            path,
            value,
            min,
            max,
        },
        ConversionError::ScaleInvalid { scale, offset } => ValidationError::ProfileConfiguration {
            path,
            reason: format!("scale={scale} offset={offset} 必须为有限数值且 scale 非 0"),
        },
        ConversionError::NotFinite(v) => ValidationError::ValueNotRepresentable {
            path,
            reason: format!("数值 {v} 非有限"),
        },
        ConversionError::ExactRequired { candidate } => ValidationError::ValueNotRepresentable {
            path,
            reason: format!("Exact 要求无损表示，但候选值 {candidate} 无法精确表示"),
        },
        ConversionError::Overflow {
            candidate,
            raw_type,
        } => ValidationError::ValueNotRepresentable {
            path,
            reason: format!("候选值 {candidate} 超出 {raw_type:?} 可表示范围"),
        },
        ConversionError::PrecisionLoss { value } => ValidationError::ValueNotRepresentable {
            path,
            reason: format!("整数 {value} 无法无损转换为 f64（禁止静默精度损失）"),
        },
    }
}

/// Driver 错误码白名单（§90.1 信息隔离，与 Collector 运维视图同一集合）：
/// `DriverErrorInfo.code` 由 Native Plugin 任意返回，可能携带路径/地址等敏感
/// 细节，不得直接进入控制结果与审计。只有固定集合的稳定码可以透传，
/// 无法识别的统一映射为 `driver_error`（原始码只进脱敏日志）。
const DRIVER_CODE_WHITELIST: &[&str] = &[
    "driver_load_failed",
    "driver_entry_not_found",
    "driver_manifest_entry_invalid",
    "driver_entry_null",
    "driver_struct_too_small",
    "driver_abi_incompatible",
    "driver_manifest_abi_mismatch",
    "driver_missing_function",
    "driver_create_failed",
    "driver_call_failed",
    "driver_empty_response",
    "driver_invalid_response",
    "driver_invalid_handle",
    "driver_encoding_error",
    "driver_request_timeout",
    "driver_read_panicked",
    "connection_failed",
    "connection_lost",
    "timeout",
    "modbus_exception",
    "invalid_response",
    "invalid_address",
    "config_error",
    "decode_error",
];

/// 白名单归一（§90.1）：命中原样透传，否则 `driver_error`。
fn whitelist_driver_code(code: &str) -> &'static str {
    for known in DRIVER_CODE_WHITELIST {
        if *known == code {
            return known;
        }
    }
    "driver_error"
}

/// `DriverErrorInfo` → 稳定 `ControlError`（§80.1、§90.1）。
pub fn map_driver_error(info: &driver_sdk::DriverErrorInfo) -> ControlError {
    ControlError {
        code: whitelist_driver_code(&info.code).to_owned(),
        message: info.message.clone(),
        details: info
            .protocol_code
            .map(|code| serde_json::json!({ "protocol_code": code })),
    }
}

/// 命令参数类型辅助（供测试引用）：`CommandParameter` 值。
#[cfg(test)]
pub fn param(name: &str, value: Value) -> CommandParameter {
    CommandParameter {
        name: name.to_owned(),
        value,
    }
}

/// 属性写入项辅助（供测试引用）。
#[cfg(test)]
pub fn write_item(path: &str, value: Value) -> PropertyWriteItem {
    PropertyWriteItem {
        path: path.to_owned(),
        value,
    }
}

#[cfg(test)]
mod tests {
    use driver_sdk::RawValue;
    use observation_model::CommandPrecondition;

    use super::*;
    use crate::catalog::tests::profile_for_test;

    #[test]
    fn property_write_maps_address_and_inverse_value() {
        let profile = profile_for_test();
        let op = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.output.frequency", Value::F64(50.0))],
            },
        )
        .expect("50Hz 写入应通过校验");
        let ValidatedOperation::Write { items, paths } = op else {
            panic!("应为 Write");
        };
        assert_eq!(items.len(), 1);
        // 地址直接来自 Profile，引擎不解析（§10）。
        assert_eq!(items[0].address, "1!40001");
        assert_eq!(items[0].value, RawValue::U64(5000));
        assert_eq!(paths[0], "drive.output.frequency");
    }

    #[test]
    fn property_write_rejects_unknown_property() {
        let profile = profile_for_test();
        let err = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.nonexistent", Value::F64(1.0))],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "PROPERTY_NOT_FOUND");
    }

    #[test]
    fn property_write_rejects_readonly_property() {
        let profile = profile_for_test();
        let err = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.mode", Value::String("auto".to_owned()))],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "PROPERTY_NOT_WRITABLE");
    }

    #[test]
    fn property_write_rejects_type_mismatch() {
        let profile = profile_for_test();
        let err = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.output.frequency", Value::Bool(true))],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "VALUE_TYPE_MISMATCH");
    }

    #[test]
    fn property_write_rejects_out_of_range() {
        let profile = profile_for_test();
        let err = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.output.frequency", Value::F64(500.0))],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "VALUE_OUT_OF_RANGE");
    }

    #[test]
    fn property_write_rejects_non_representable() {
        // scale=0.01、raw=U16、Exact：50.015Hz → 5001.5 非整数，拒绝。
        let profile = profile_for_test();
        let err = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.output.frequency", Value::F64(50.015))],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "VALUE_NOT_REPRESENTABLE");
    }

    #[test]
    fn property_write_batch_rejects_any_failure() {
        let profile = profile_for_test();
        let err = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![
                    write_item("drive.output.frequency", Value::F64(50.0)),
                    write_item("drive.mode", Value::String("auto".to_owned())),
                ],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "PROPERTY_NOT_WRITABLE");
    }

    #[test]
    fn command_maps_to_driver_command() {
        let profile = profile_for_test();
        let op = validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![param("ack", Value::Bool(true))],
            },
        )
        .expect("drive.reset 应通过校验");
        let ValidatedOperation::Execute {
            command,
            risk_level,
            ..
        } = op
        else {
            panic!("应为 Execute");
        };
        assert_eq!(command.command_id, "reset");
        assert_eq!(command.payload, serde_json::json!({ "ack": true }));
        assert_eq!(risk_level, CommandRiskLevel::Medium);
    }

    #[test]
    fn command_rejects_unknown_command() {
        let profile = profile_for_test();
        let err = validate_command(
            &profile,
            &CommandRequest {
                command: "cnc.program.start".to_owned(),
                parameters: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "COMMAND_NOT_FOUND");
    }

    #[test]
    fn command_rejects_missing_required_parameter() {
        let profile = profile_for_test();
        let err = validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "MISSING_PARAMETER");
        assert!(
            matches!(err, ValidationError::MissingParameter { parameter, .. } if parameter == "ack")
        );
    }

    #[test]
    fn command_rejects_unknown_parameter() {
        let profile = profile_for_test();
        let err = validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![
                    param("ack", Value::Bool(true)),
                    param("extra", Value::I32(1)),
                ],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "UNKNOWN_PARAMETER");
    }

    #[test]
    fn command_rejects_parameter_type_mismatch() {
        let profile = profile_for_test();
        let err = validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![param("ack", Value::F64(1.0))],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "PARAMETER_TYPE_MISMATCH");
    }

    #[test]
    fn command_rejects_parameter_out_of_range() {
        // 构造带范围的命令：在 profile 副本上追加参数范围。
        let profile = profile_for_test();
        let mut profile = (*profile).clone();
        let command = profile
            .commands
            .iter_mut()
            .find(|c| c.id == "drive.reset")
            .unwrap();
        command.parameters[0].min = Some(Value::I64(0));
        command.parameters[0].max = Some(Value::I64(10));
        command
            .parameters
            .push(observation_model::CommandParameterDescriptor {
                name: "level".to_owned(),
                data_type: DataType::U16,
                required: true,
                min: Some(Value::U16(1)),
                max: Some(Value::U16(3)),
            });

        let err = validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![
                    param("ack", Value::Bool(true)),
                    param("level", Value::U16(5)),
                ],
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), "PARAMETER_OUT_OF_RANGE");

        // 边界内通过。
        validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![
                    param("ack", Value::Bool(true)),
                    param("level", Value::U16(3)),
                ],
            },
        )
        .expect("level=3 应在范围内");
    }

    #[test]
    fn numeric_family_params_accepted() {
        let profile = profile_for_test();
        let mut profile = (*profile).clone();
        let command = profile
            .commands
            .iter_mut()
            .find(|c| c.id == "drive.reset")
            .unwrap();
        command
            .parameters
            .push(observation_model::CommandParameterDescriptor {
                name: "level".to_owned(),
                data_type: DataType::U16,
                required: false,
                min: None,
                max: None,
            });
        // U16 descriptor 接受任意数值变体（与写入语义一致）。
        for value in [Value::U16(1), Value::I32(1), Value::F64(1.5)] {
            validate_command(
                &profile,
                &CommandRequest {
                    command: "drive.reset".to_owned(),
                    parameters: vec![
                        param("ack", Value::Bool(true)),
                        param("level", value.clone()),
                    ],
                },
            )
            .unwrap_or_else(|e| panic!("数值变体 {value:?} 应被接受: {e}"));
        }
    }

    #[test]
    fn map_driver_error_whitelists_stable_codes() {
        let known = map_driver_error(&driver_sdk::DriverErrorInfo {
            code: "timeout".to_owned(),
            message: "slave 无响应".to_owned(),
            protocol_code: Some(0x80),
            retryable: true,
        });
        assert_eq!(known.code, "timeout");
        assert_eq!(
            known.details,
            Some(serde_json::json!({ "protocol_code": 0x80 }))
        );

        let unknown = map_driver_error(&driver_sdk::DriverErrorInfo {
            code: "C:\\factory\\secret_path 里的怪码".to_owned(),
            message: "detail".to_owned(),
            protocol_code: None,
            retryable: false,
        });
        assert_eq!(unknown.code, "driver_error");
    }

    #[test]
    fn risk_level_surfaces_from_command() {
        let profile = profile_for_test();
        let op = validate_command(
            &profile,
            &CommandRequest {
                command: "drive.reset".to_owned(),
                parameters: vec![param("ack", Value::Bool(true))],
            },
        )
        .unwrap();
        assert_eq!(op.risk_level(), Some(CommandRiskLevel::Medium));
        let write_op = validate_property_write(
            &profile,
            &PropertyWriteRequest {
                items: vec![write_item("drive.output.frequency", Value::F64(50.0))],
            },
        )
        .unwrap();
        assert_eq!(write_op.risk_level(), None);
    }

    #[test]
    fn precondition_type_referenced() {
        // §85 引用完整性：ProfileCommand.preconditions 类型可用。
        let _p: Option<CommandPrecondition> = None;
    }
}
