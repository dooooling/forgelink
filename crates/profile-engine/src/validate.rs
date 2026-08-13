//! Device Profile 完整校验（§37、§38 Normative）。
//!
//! 校验规则（与转换逻辑 §37.1 保持一致的约束）：
//!
//! - 必填字段非空：`id`、`vendor`、`family`、`models`、`driver_id`；
//! - 属性路径为点分标准语义路径，各段非空（§41~§46）；
//! - 属性路径不得重复；
//! - `scale`/`offset` 必须有限；`scale != 0`（scale=0 的 Profile 无法做逆变换）；
//! - `raw_type`/`value_type` 必须属于同一值族（数值/数值、Bool/Bool、
//!   String/String、Bytes/Bytes）；`Array/Struct` 暂不支持标量转换；
//! - String/Bytes/Bool 属性禁止缩放（`scale == 0.0` 且 `offset == 0.0`），
//!   且 `write_rounding` 必须为 `Exact`（无取整语义）；
//! - `min`/`max` 必须与 `value_type` 同族（数值属性必须为数值），且 `min <= max`；
//! - `default_interval_ms` 若存在必须大于 0；
//! - 命令 `id`/`driver_command_id` 非空且 `id` 唯一；参数名非空唯一；
//!   前置条件属性路径非空。

use std::fmt;

use observation_model::{CommandParameterDescriptor, DataType, Value};

use crate::models::{DeviceProfile, ProfileCommand, ProfileProperty, WriteRounding};

/// 校验失败详情。
///
/// `field` 为具体字段路径（如 `properties[0].scale`），便于定位错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    /// 出错 Profile 的 ID。
    pub profile_id: String,
    /// 出错字段路径。
    pub field: String,
    /// 失败原因。
    pub reason: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "profile `{}` 字段 `{}` 校验失败: {}",
            self.profile_id, self.field, self.reason
        )
    }
}

impl std::error::Error for ValidationError {}

/// 校验一个完整 Profile（§37 字段约束）。
pub fn validate_profile(profile: &DeviceProfile) -> Result<(), ValidationError> {
    fail_if_empty(&profile.id, "id")?;
    fail_if_empty(&profile.vendor, "vendor")?;
    fail_if_empty(&profile.family, "family")?;
    if profile.models.is_empty() {
        return Err(err(profile, "models", "models 列表不能为空"));
    }
    for (i, model) in profile.models.iter().enumerate() {
        fail_if_empty(model, &format!("models[{i}]"))?;
    }
    fail_if_empty(&profile.driver_id, "driver_id")?;
    if profile.properties.is_empty() {
        return Err(err(profile, "properties", "properties 列表不能为空"));
    }

    let mut seen_paths: Vec<&str> = Vec::new();
    for (i, property) in profile.properties.iter().enumerate() {
        validate_property(profile, property, &format!("properties[{i}]"))?;
        let path = property.path.as_str();
        if seen_paths.contains(&path) {
            return Err(err(
                profile,
                &format!("properties[{i}].path"),
                &format!("路径 `{path}` 重复"),
            ));
        }
        seen_paths.push(path);
    }

    let mut seen_commands: Vec<&str> = Vec::new();
    for (i, command) in profile.commands.iter().enumerate() {
        validate_command(profile, command, &format!("commands[{i}]"))?;
        if seen_commands.contains(&command.id.as_str()) {
            return Err(err(
                profile,
                &format!("commands[{i}].id"),
                &format!("命令 `{}` 重复", command.id),
            ));
        }
        seen_commands.push(&command.id);
    }

    Ok(())
}

/// 校验单个属性映射。
pub fn validate_property(
    profile: &DeviceProfile,
    property: &ProfileProperty,
    field: &str,
) -> Result<(), ValidationError> {
    let path = property.path.as_str();
    if !valid_path(path) {
        return Err(err(
            profile,
            &format!("{field}.path"),
            &format!("路径 `{path}` 不是合法的点分语义路径"),
        ));
    }
    fail_if_empty(&property.driver_address, &format!("{field}.driver_address"))?;

    if !property.scale.is_finite() || !property.offset.is_finite() {
        return Err(err(
            profile,
            &format!("{field}.scale"),
            "scale/offset 必须为有限数值（不允许 NaN/±Infinity）",
        ));
    }

    validate_value_pair(profile, &property.raw_type, &property.value_type, field)?;

    // String/Bytes/Bool 不支持缩放（scale/offset 必须为 0），写路径无取整语义；
    // 数值属性则要求 scale 非 0（否则无法做写入逆变换）。
    let no_scale_family = matches!(
        property.value_type,
        DataType::String | DataType::Bytes | DataType::Bool
    );
    if no_scale_family {
        if property.scale != 0.0 || property.offset != 0.0 {
            return Err(err(
                profile,
                &format!("{field}.scale"),
                "String/Bytes/Bool 属性禁止缩放（scale/offset 必须为 0）",
            ));
        }
        if property.write_rounding != WriteRounding::Exact {
            return Err(err(
                profile,
                &format!("{field}.write_rounding"),
                "String/Bytes/Bool 属性必须使用 Exact（无取整语义）",
            ));
        }
    } else if property.scale == 0.0 {
        return Err(err(
            profile,
            &format!("{field}.scale"),
            "数值属性 scale 不能为 0（无法进行写入逆变换）",
        ));
    }

    validate_min_max(profile, property, field)?;

    if let Some(interval) = property.default_interval_ms
        && interval == 0
    {
        return Err(err(
            profile,
            &format!("{field}.default_interval_ms"),
            "default_interval_ms 必须大于 0",
        ));
    }

    Ok(())
}

/// 校验 `raw_type`/`value_type` 属于同一值族。
fn validate_value_pair(
    profile: &DeviceProfile,
    raw_type: &DataType,
    value_type: &DataType,
    field: &str,
) -> Result<(), ValidationError> {
    let same_family = match (raw_type, value_type) {
        (DataType::Bool, DataType::Bool) => true,
        (DataType::String, DataType::String) => true,
        (DataType::Bytes, DataType::Bytes) => true,
        (a, b) if is_numeric(a) && is_numeric(b) => true,
        _ => false,
    };
    if !same_family {
        return Err(err(
            profile,
            &format!("{field}.raw_type"),
            &format!(
                "raw_type {raw_type:?} 与 value_type {value_type:?} 不属于同一值族 \
                 （仅支持数值/数值、Bool/Bool、String/String、Bytes/Bytes）"
            ),
        ));
    }
    Ok(())
}

/// 校验 `min`/`max` 与 `value_type` 同族、数值界有限，且 `min <= max`。
fn validate_min_max(
    profile: &DeviceProfile,
    property: &ProfileProperty,
    field: &str,
) -> Result<(), ValidationError> {
    let value_type = &property.value_type;
    let check = |bound: &Value, name: &str| -> Result<(), ValidationError> {
        let compatible = match (value_type, bound) {
            (DataType::Bool, Value::Bool(_)) => true,
            (DataType::String, Value::String(_)) => true,
            (DataType::Bytes, Value::Bytes(_)) => true,
            (a, b) if is_numeric(a) && is_numeric_value(b) => true,
            _ => false,
        };
        if !compatible {
            return Err(err(
                profile,
                &format!("{field}.{name}"),
                &format!("{name} 与 value_type {value_type:?} 类型不匹配"),
            ));
        }
        if is_numeric_value(bound) && value_to_f64(bound).is_none_or(|f| !f.is_finite()) {
            return Err(err(
                profile,
                &format!("{field}.{name}"),
                &format!("{name} 必须为有限数值（不允许 NaN/±Infinity）"),
            ));
        }
        Ok(())
    };
    if let Some(min) = &property.min {
        check(min, "min")?;
    }
    if let Some(max) = &property.max {
        check(max, "max")?;
    }
    if let (Some(min), Some(max)) = (&property.min, &property.max)
        && min_greater_than_max(min, max) == Some(true)
    {
        return Err(err(
            profile,
            &format!("{field}.min"),
            "min 必须小于等于 max",
        ));
    }
    Ok(())
}

/// 校验单个命令映射。
fn validate_command(
    profile: &DeviceProfile,
    command: &ProfileCommand,
    field: &str,
) -> Result<(), ValidationError> {
    fail_if_empty(&command.id, &format!("{field}.id"))?;
    fail_if_empty(
        &command.driver_command_id,
        &format!("{field}.driver_command_id"),
    )?;

    let mut seen_names: Vec<&str> = Vec::new();
    for (i, parameter) in command.parameters.iter().enumerate() {
        let param_field = format!("{field}.parameters[{i}]");
        let name = parameter.name.as_str();
        if name.is_empty() {
            return Err(err(
                profile,
                &format!("{param_field}.name"),
                "参数名不能为空",
            ));
        }
        if seen_names.contains(&name) {
            return Err(err(
                profile,
                &format!("{param_field}.name"),
                &format!("参数名 `{name}` 重复"),
            ));
        }
        seen_names.push(name);
        validate_parameter_bounds(profile, parameter, &param_field)?;
    }

    for (i, precondition) in command.preconditions.iter().enumerate() {
        if precondition.property.is_empty() {
            return Err(err(
                profile,
                &format!("{field}.preconditions[{i}].property"),
                "前置条件属性路径不能为空",
            ));
        }
    }

    Ok(())
}

/// 校验命令参数 `min`/`max` 与 `data_type` 匹配且 `min <= max`（P2）。
///
/// - 标量/数值 `data_type`：`min`/`max` 必须属于同一值族；
/// - `Array/Struct` 无比较语义，不允许携带 `min`/`max`。
fn validate_parameter_bounds(
    profile: &DeviceProfile,
    parameter: &CommandParameterDescriptor,
    field: &str,
) -> Result<(), ValidationError> {
    let data_type = &parameter.data_type;

    if matches!(data_type, DataType::Array(_) | DataType::Struct(_)) {
        if parameter.min.is_some() || parameter.max.is_some() {
            return Err(err(
                profile,
                &format!("{field}.min"),
                &format!("data_type {data_type:?} 无比较语义，不允许携带 min/max"),
            ));
        }
        return Ok(());
    }

    let check = |bound: &Value, name: &str| -> Result<(), ValidationError> {
        let compatible = match (data_type, bound) {
            (DataType::Bool, Value::Bool(_)) => true,
            (DataType::String, Value::String(_)) => true,
            (DataType::Bytes, Value::Bytes(_)) => true,
            (a, b) if is_numeric(a) && is_numeric_value(b) => true,
            _ => false,
        };
        if !compatible {
            return Err(err(
                profile,
                &format!("{field}.{name}"),
                &format!("{name} 与 data_type {data_type:?} 类型不匹配"),
            ));
        }
        if is_numeric_value(bound) && value_to_f64(bound).is_none_or(|f| !f.is_finite()) {
            return Err(err(
                profile,
                &format!("{field}.{name}"),
                &format!("{name} 必须为有限数值（不允许 NaN/±Infinity）"),
            ));
        }
        Ok(())
    };
    if let Some(min) = &parameter.min {
        check(min, "min")?;
    }
    if let Some(max) = &parameter.max {
        check(max, "max")?;
    }
    if let (Some(min), Some(max)) = (&parameter.min, &parameter.max)
        && min_greater_than_max(min, max) == Some(true)
    {
        return Err(err(
            profile,
            &format!("{field}.min"),
            "min 必须小于等于 max",
        ));
    }
    Ok(())
}

/// 属性路径是否为合法的点分语义路径（如 `drive.output.frequency`）。
fn valid_path(path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    path.split('.').all(|segment| !segment.is_empty())
}

fn fail_if_empty(value: &str, field: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        Err(ValidationError {
            profile_id: "?".to_owned(),
            field: field.to_owned(),
            reason: "不能为空".to_owned(),
        })
    } else {
        Ok(())
    }
}

fn err(profile: &DeviceProfile, field: &str, reason: &str) -> ValidationError {
    ValidationError {
        profile_id: profile.id.clone(),
        field: field.to_owned(),
        reason: reason.to_owned(),
    }
}

fn is_numeric(t: &DataType) -> bool {
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

/// `min > max` 的精确判定（P2）。
///
/// - 整数对整数：`i128` 精确比较（`min=U64(2^53+1)`、`max=U64(2^53)`
///   经 f64 会误判为相等）；
/// - 整数 vs 浮点界：按 `floor`/`ceil` 整数化后精确比较（`x > f ⟺
///   x > floor(f)`、`f > x ⟺ ceil(f) > x`）；
/// - 浮点对浮点：f64 比较（NaN 由有限性校验先行拦截）。
fn min_greater_than_max(min: &Value, max: &Value) -> Option<bool> {
    match (integer_value_to_i128(min), integer_value_to_i128(max)) {
        (Some(a), Some(b)) => Some(a > b),
        (Some(a), None) => value_to_f64(max).map(|b| i128_gt_f64(a, b)),
        (None, Some(b)) => value_to_f64(min).map(|a| (a.ceil() as i128) > b),
        (None, None) => {
            let a = value_to_f64(min)?;
            let b = value_to_f64(max)?;
            Some(
                a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
                    == std::cmp::Ordering::Greater,
            )
        }
    }
}

/// 整数语义值 `a` 与浮点界 `b` 的精确比较：`a > b`。
///
/// 整数 `a` 大于小数界 ⟺ `a > floor(b)`；`b` 本身为整数时直接比较。
/// `|b|` 超出 `i128` 范围时饱和转换仍给出正确结果（`a ≤ 1.8e19`）。
fn i128_gt_f64(a: i128, b: f64) -> bool {
    if b.is_nan() {
        return false;
    }
    let integerized = if b.fract() == 0.0 { b } else { b.floor() };
    a > (integerized as i128)
}

/// 整数 `Value` → `i128`（精确）；浮点/其他返回 `None`。
fn integer_value_to_i128(v: &Value) -> Option<i128> {
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

#[cfg(test)]
mod tests {
    use observation_model::{
        CommandParameterDescriptor, CommandPrecondition, CommandRiskLevel, DomainKind, Operator,
    };

    use super::*;
    use crate::models::{AcquisitionConstraints, ProfileCapabilities};

    fn sample() -> DeviceProfile {
        DeviceProfile {
            id: "inovance-md500".to_owned(),
            vendor: "Inovance".to_owned(),
            family: "MD500".to_owned(),
            models: vec!["MD500".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "modbus-rtu".to_owned(),
            properties: vec![sample_property()],
            commands: vec![],
            capabilities: ProfileCapabilities {
                supported_properties: vec!["drive.output.frequency".to_owned()],
                supported_commands: vec![],
                acquisition: AcquisitionConstraints::default(),
                limits: Default::default(),
            },
        }
    }

    fn sample_property() -> ProfileProperty {
        ProfileProperty {
            path: "drive.output.frequency".to_owned(),
            driver_address: "1!40001".to_owned(),
            raw_type: DataType::U16,
            value_type: DataType::F64,
            unit: Some("Hz".to_owned()),
            scale: 0.01,
            offset: 0.0,
            write_rounding: WriteRounding::Nearest,
            readable: true,
            writable: true,
            default_interval_ms: Some(1000),
            min: Some(Value::F64(0.0)),
            max: Some(Value::F64(400.0)),
        }
    }

    #[test]
    fn valid_profile_passes() {
        validate_profile(&sample()).expect("合法 Profile 应通过校验");
    }

    #[test]
    fn empty_required_fields_rejected() {
        let mut profile = sample();
        profile.id.clear();
        assert_eq!(
            validate_profile(&profile)
                .expect_err("空 id 应被拒绝")
                .field,
            "id"
        );

        let mut profile = sample();
        profile.vendor.clear();
        assert_eq!(
            validate_profile(&profile)
                .expect_err("空 vendor 应被拒绝")
                .field,
            "vendor"
        );

        let mut profile = sample();
        profile.family.clear();
        assert_eq!(
            validate_profile(&profile)
                .expect_err("空 family 应被拒绝")
                .field,
            "family"
        );

        let mut profile = sample();
        profile.driver_id.clear();
        assert_eq!(
            validate_profile(&profile)
                .expect_err("空 driver_id 应被拒绝")
                .field,
            "driver_id"
        );
    }

    #[test]
    fn empty_models_rejected() {
        let mut profile = sample();
        profile.models.clear();
        let e = validate_profile(&profile).expect_err("空 models 应被拒绝");
        assert_eq!(e.field, "models");
    }

    #[test]
    fn invalid_path_rejected() {
        let mut profile = sample();
        profile.properties[0].path = "drive..frequency".to_owned();
        let e = validate_profile(&profile).expect_err("空路径段应被拒绝");
        assert_eq!(e.field, "properties[0].path");

        profile = sample();
        profile.properties[0].path = ".drive.frequency".to_owned();
        validate_profile(&profile).expect_err("前导点应被拒绝");

        profile = sample();
        profile.properties[0].path = "drive.frequency.".to_owned();
        validate_profile(&profile).expect_err("尾随点应被拒绝");
    }

    #[test]
    fn duplicate_paths_rejected() {
        let mut profile = sample();
        let dup = sample_property();
        profile.properties.push(dup);
        let e = validate_profile(&profile).expect_err("重复路径应被拒绝");
        assert_eq!(e.field, "properties[1].path");
    }

    #[test]
    fn non_finite_scale_rejected() {
        for scale in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut profile = sample();
            profile.properties[0].scale = scale;
            let e = validate_profile(&profile).expect_err("非有限 scale 应被拒绝");
            assert_eq!(e.field, "properties[0].scale");
        }
    }

    #[test]
    fn zero_scale_rejected() {
        let mut profile = sample();
        profile.properties[0].scale = 0.0;
        let e = validate_profile(&profile).expect_err("scale=0 应被拒绝");
        assert_eq!(e.field, "properties[0].scale");
    }

    #[test]
    fn type_family_mismatch_rejected() {
        let mut profile = sample();
        profile.properties[0].value_type = DataType::String;
        let e = validate_profile(&profile).expect_err("类型族不匹配应被拒绝");
        assert_eq!(e.field, "properties[0].raw_type");
    }

    #[test]
    fn string_property_with_scale_rejected() {
        let mut profile = sample();
        profile.properties[0].raw_type = DataType::String;
        profile.properties[0].value_type = DataType::String;
        profile.properties[0].scale = 1.0;
        let e = validate_profile(&profile).expect_err("String 属性缩放应被拒绝");
        assert_eq!(e.field, "properties[0].scale");
    }

    #[test]
    fn string_property_with_rounding_rejected() {
        let mut profile = sample();
        profile.properties[0].raw_type = DataType::String;
        profile.properties[0].value_type = DataType::String;
        profile.properties[0].scale = 0.0;
        profile.properties[0].offset = 0.0;
        profile.properties[0].write_rounding = WriteRounding::Nearest;
        let e = validate_profile(&profile).expect_err("String 属性取整应被拒绝");
        assert_eq!(e.field, "properties[0].write_rounding");
    }

    #[test]
    fn string_property_with_zero_scale_passes() {
        let mut profile = sample();
        profile.properties[0].raw_type = DataType::String;
        profile.properties[0].value_type = DataType::String;
        profile.properties[0].scale = 0.0;
        profile.properties[0].offset = 0.0;
        profile.properties[0].write_rounding = WriteRounding::Exact;
        profile.properties[0].min = None;
        profile.properties[0].max = None;
        validate_profile(&profile).expect("String 属性（scale=0）应通过校验");
    }

    #[test]
    fn min_max_type_mismatch_rejected() {
        let mut profile = sample();
        profile.properties[0].min = Some(Value::String("low".to_owned()));
        let e = validate_profile(&profile).expect_err("min 类型不匹配应被拒绝");
        assert_eq!(e.field, "properties[0].min");
    }

    #[test]
    fn min_greater_than_max_rejected() {
        let mut profile = sample();
        profile.properties[0].min = Some(Value::F64(500.0));
        profile.properties[0].max = Some(Value::F64(400.0));
        let e = validate_profile(&profile).expect_err("min > max 应被拒绝");
        assert_eq!(e.field, "properties[0].min");
    }

    #[test]
    fn zero_interval_rejected() {
        let mut profile = sample();
        profile.properties[0].default_interval_ms = Some(0);
        let e = validate_profile(&profile).expect_err("interval=0 应被拒绝");
        assert_eq!(e.field, "properties[0].default_interval_ms");
    }

    #[test]
    fn duplicate_command_rejected() {
        let mut profile = sample();
        profile.commands.push(ProfileCommand {
            id: "drive.command.start".to_owned(),
            driver_command_id: "0x01".to_owned(),
            parameters: vec![CommandParameterDescriptor {
                name: "speed".to_owned(),
                data_type: DataType::F64,
                required: false,
                min: Some(Value::F64(0.0)),
                max: Some(Value::F64(400.0)),
            }],
            risk_level: CommandRiskLevel::Medium,
            preconditions: vec![CommandPrecondition {
                property: "drive.status.ready".to_owned(),
                operator: Operator::Eq,
                value: Value::Bool(true),
            }],
        });
        profile.commands.push(ProfileCommand {
            id: "drive.command.start".to_owned(),
            driver_command_id: "0x02".to_owned(),
            parameters: vec![],
            risk_level: CommandRiskLevel::Medium,
            preconditions: vec![],
        });
        let e = validate_profile(&profile).expect_err("重复命令应被拒绝");
        assert_eq!(e.field, "commands[1].id");
    }

    #[test]
    fn duplicate_parameter_rejected() {
        let mut profile = sample();
        profile.commands.push(ProfileCommand {
            id: "drive.command.start".to_owned(),
            driver_command_id: "0x01".to_owned(),
            parameters: vec![
                CommandParameterDescriptor {
                    name: "speed".to_owned(),
                    data_type: DataType::F64,
                    required: false,
                    min: None,
                    max: None,
                },
                CommandParameterDescriptor {
                    name: "speed".to_owned(),
                    data_type: DataType::F64,
                    required: false,
                    min: None,
                    max: None,
                },
            ],
            risk_level: CommandRiskLevel::Medium,
            preconditions: vec![],
        });
        let e = validate_profile(&profile).expect_err("重复参数应被拒绝");
        assert_eq!(e.field, "commands[0].parameters[1].name");
    }

    #[test]
    fn empty_precondition_property_rejected() {
        let mut profile = sample();
        profile.commands.push(ProfileCommand {
            id: "drive.command.start".to_owned(),
            driver_command_id: "0x01".to_owned(),
            parameters: vec![],
            risk_level: CommandRiskLevel::Medium,
            preconditions: vec![CommandPrecondition {
                property: "".to_owned(),
                operator: Operator::Eq,
                value: Value::Bool(true),
            }],
        });
        let e = validate_profile(&profile).expect_err("空前置条件应被拒绝");
        assert_eq!(e.field, "commands[0].preconditions[0].property");
    }

    // ---- 命令参数 min/max 校验（P2） ----

    fn sample_command() -> ProfileCommand {
        ProfileCommand {
            id: "drive.command.start".to_owned(),
            driver_command_id: "0x01".to_owned(),
            parameters: vec![CommandParameterDescriptor {
                name: "speed".to_owned(),
                data_type: DataType::U16,
                required: false,
                min: Some(Value::U16(0)),
                max: Some(Value::U16(400)),
            }],
            risk_level: CommandRiskLevel::Medium,
            preconditions: vec![],
        }
    }

    #[test]
    fn valid_parameter_bounds_pass() {
        let mut profile = sample();
        profile.commands.push(sample_command());
        validate_profile(&profile).expect("U16 参数 + U16 min/max 应通过校验");
    }

    #[test]
    fn parameter_min_max_type_mismatch_rejected() {
        let mut profile = sample();
        let mut command = sample_command();
        command.parameters[0].min = Some(Value::String("low".to_owned()));
        profile.commands.push(command);
        let e = validate_profile(&profile).expect_err("data_type=U16 + String min 应被拒绝");
        assert_eq!(e.field, "commands[0].parameters[0].min");
    }

    #[test]
    fn parameter_min_greater_than_max_rejected() {
        let mut profile = sample();
        let mut command = sample_command();
        command.parameters[0].min = Some(Value::U16(500));
        command.parameters[0].max = Some(Value::U16(400));
        profile.commands.push(command);
        let e = validate_profile(&profile).expect_err("min > max 应被拒绝");
        assert_eq!(e.field, "commands[0].parameters[0].min");
    }

    #[test]
    fn array_parameter_with_bounds_rejected() {
        let mut profile = sample();
        let mut command = sample_command();
        command.parameters[0].data_type = DataType::Array(Box::new(DataType::U16));
        command.parameters[0].min = Some(Value::U16(0));
        profile.commands.push(command);
        let e = validate_profile(&profile).expect_err("Array 参数不允许 min/max");
        assert_eq!(e.field, "commands[0].parameters[0].min");
    }

    #[test]
    fn string_parameter_with_numeric_bound_rejected() {
        let mut profile = sample();
        let mut command = sample_command();
        command.parameters[0].data_type = DataType::String;
        command.parameters[0].min = Some(Value::F64(0.0));
        profile.commands.push(command);
        let e = validate_profile(&profile).expect_err("String 参数 + 数值 min 应被拒绝");
        assert_eq!(e.field, "commands[0].parameters[0].min");
    }

    #[test]
    fn valid_path_check() {
        assert!(valid_path("drive.output.frequency"));
        assert!(!valid_path(""));
        assert!(!valid_path("drive."));
        assert!(!valid_path(".drive"));
        assert!(!valid_path("a..b"));
    }

    // ---- P2：min/max 精确比较（64 位整数界不得经 f64 舍入） ----

    fn int_property() -> ProfileProperty {
        let mut p = sample_property();
        p.raw_type = DataType::U64;
        p.value_type = DataType::U64;
        p.scale = 1.0;
        p.offset = 0.0;
        p.min = None;
        p.max = None;
        p
    }

    #[test]
    fn property_int_min_greater_than_int_max_rejected() {
        // U64(2^53+1) 与 U64(2^53) 经 f64 会误判为相等，必须精确拒绝。
        let mut profile = sample();
        let mut p = int_property();
        p.min = Some(Value::U64(9_007_199_254_740_993));
        p.max = Some(Value::U64(9_007_199_254_740_992));
        profile.properties[0] = p;
        let e = validate_profile(&profile).expect_err("min > max（64 位整数精确比较）应被拒绝");
        assert_eq!(e.field, "properties[0].min");
    }

    #[test]
    fn property_int_bounds_precise_ordering_passes() {
        let mut profile = sample();
        let mut p = int_property();
        p.min = Some(Value::U64(9_007_199_254_740_992));
        p.max = Some(Value::U64(9_007_199_254_740_993));
        profile.properties[0] = p;
        validate_profile(&profile).expect("min=2^53 < max=2^53+1 应通过校验");
    }

    #[test]
    fn property_float_bound_compared_precisely_against_int() {
        // F64(2^53) min 与 U64(2^53+1) max：min < max 应通过；
        // F64(2^53) min 与 U64(2^53) max 相等也应通过；
        // F64(2^53) min 与 U64(2^53-1) max 应被拒绝。
        let mut profile = sample();
        let mut p = int_property();
        p.min = Some(Value::F64(9_007_199_254_740_992.0));
        p.max = Some(Value::U64(9_007_199_254_740_993));
        profile.properties[0] = p;
        validate_profile(&profile).expect("float min=2^53 < int max=2^53+1 应通过");

        let mut profile = sample();
        let mut p = int_property();
        p.min = Some(Value::F64(9_007_199_254_740_992.0));
        p.max = Some(Value::U64(9_007_199_254_740_992));
        profile.properties[0] = p;
        validate_profile(&profile).expect("float min=2^53 == int max=2^53 应通过");

        let mut profile = sample();
        let mut p = int_property();
        p.min = Some(Value::F64(9_007_199_254_740_992.0));
        p.max = Some(Value::U64(9_007_199_254_740_991));
        profile.properties[0] = p;
        let e = validate_profile(&profile).expect_err("float min=2^53 > int max=2^53-1 应被拒绝");
        assert_eq!(e.field, "properties[0].min");
    }

    #[test]
    fn non_finite_numeric_bound_rejected() {
        for bound in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut profile = sample();
            profile.properties[0].min = Some(Value::F64(bound));
            let e = validate_profile(&profile).expect_err("非有限 min 应被拒绝");
            assert_eq!(e.field, "properties[0].min");

            profile = sample();
            profile.properties[0].max = Some(Value::F64(bound));
            let e = validate_profile(&profile).expect_err("非有限 max 应被拒绝");
            assert_eq!(e.field, "properties[0].max");
        }
    }

    #[test]
    fn parameter_int_bounds_compared_precisely() {
        let mut profile = sample();
        let mut command = sample_command();
        command.parameters[0].data_type = DataType::U64;
        command.parameters[0].min = Some(Value::U64(9_007_199_254_740_993));
        command.parameters[0].max = Some(Value::U64(9_007_199_254_740_992));
        profile.commands.push(command);
        let e =
            validate_profile(&profile).expect_err("参数 min > max（64 位整数精确比较）应被拒绝");
        assert_eq!(e.field, "commands[0].parameters[0].min");
    }

    #[test]
    fn parameter_non_finite_bound_rejected() {
        let mut profile = sample();
        let mut command = sample_command();
        command.parameters[0].max = Some(Value::F64(f64::INFINITY));
        profile.commands.push(command);
        let e = validate_profile(&profile).expect_err("参数非有限 max 应被拒绝");
        assert_eq!(e.field, "commands[0].parameters[0].max");
    }
}
