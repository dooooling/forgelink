//! 读取解码与写入编码转换（§37.1 Normative）。
//!
//! # 64 位整数精度（P1）
//!
//! `f64` 只能精确表示 `≤ 2^53` 的整数；为遵守"禁止静默溢出/精度损失"：
//!
//! - 恒等变换（`scale == 1.0` 且 `offset == 0.0`）且目标为整数类型时，
//!   读取/写入均走精确整数路径（`i128` + `try_from`），完全不经过 f64；
//! - 其余缩放路径中，`I64/U64` 必须**无损**进入 f64 中间值
//!   （`i64_lossless_to_f64`/`u64_lossless_to_f64` 往返检查），否则读取
//!   映射为 `Bad`、写入返回 `ConversionError::PrecisionLoss`。
//!
//! # 读取（`decode_read`）
//!
//! `semantic_value = raw_value * scale + offset`，随后按 `value_type` 做
//! checked conversion，禁止静默溢出；整数目标类型先取整再检查范围。
//! 原始错误 / 缺失值 / 类型不匹配一律映射为 `Bad`，不生成伪造的 Good 值（§9）。
//!
//! # 写入（`encode_write`）
//!
//! 固定步骤（§37.1）：
//!
//! 1. 属性必须 `writable`；
//! 2. 语义值类型必须与 `value_type` 匹配；
//! 3. 校验语义值在 `min`/`max` 范围内（范围属于语义值，非原始寄存器范围）；
//! 4. `scale` 必须非 0 且有限、`offset` 有限；
//! 5. 逆变换 `raw_candidate = (semantic_value - offset) / scale`；
//! 6. 按 `write_rounding` 处理整数 `raw_type`：`Exact` 必须无损表示，
//!    否则拒绝；`Nearest/Floor/Ceil/Truncate` 仅 Profile 显式声明时允许；
//! 7. `raw_candidate` 必须在 `raw_type` 可表示范围内；
//! 8. checked conversion 生成 `RawValue`（溢出即拒绝，不静默截断）。
//!
//! 本函数只生成 `RawValue`；`DriverWriteItem` 的组装与认证/授权/前置条件
//! 校验属于 Control Engine（§14、§15）。

use std::fmt;

use observation_model::{
    DataType, Quality, QualityLevel, QualityReason, RawReadResult, RawValue, Value,
};

use crate::models::{ProfileProperty, WriteRounding};

/// 读取解码结果（§7.3）。
///
/// `value` 为 `None` 时表示本次无有效语义值（错误 / 缺失 / 转换失败），
/// 由 `quality` 说明原因；上层不得用 Last Good Value 伪装（§9）。
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedRead {
    pub value: Option<Value>,
    pub quality: Quality,
}

/// 写入逆变换错误（§37.1 步骤 1~8）。
#[derive(Debug, Clone, PartialEq)]
pub enum ConversionError {
    /// 属性不可写（步骤 1）。
    NotWritable,
    /// 语义值类型与 `value_type` 不匹配（步骤 2）。
    SemanticTypeMismatch { expected: DataType },
    /// 语义值超出 `min`/`max`（步骤 3）。
    MinMaxViolation {
        value: f64,
        min: Option<f64>,
        max: Option<f64>,
    },
    /// `scale` 为 0 或非有限（步骤 4）。
    ScaleInvalid { scale: f64, offset: f64 },
    /// 语义值或逆变换结果非有限（NaN/±Infinity）。
    NotFinite(f64),
    /// `Exact` 要求无损表示，但候选值无法精确表示为目标 `raw_type`（步骤 6）。
    ExactRequired { candidate: f64 },
    /// 候选值超出 `raw_type` 可表示范围（步骤 7）。
    Overflow { candidate: f64, raw_type: DataType },
    /// 64 位整数无法无损转换为 f64（超出 f64 精确表示范围）——拒绝而非
    /// 静默丢失精度（如 `U64(9_007_199_254_740_993)` → f64 会变成 ...992）。
    /// 出现在：缩放路径无法用 f64 精确计算中间值，或整数语义写入浮点
    /// 寄存器（非 Exact 策略同样拒绝）。
    PrecisionLoss { value: i128 },
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConversionError::NotWritable => write!(f, "属性不可写"),
            ConversionError::SemanticTypeMismatch { expected } => {
                write!(f, "语义值类型与 value_type {expected:?} 不匹配")
            }
            ConversionError::MinMaxViolation { value, min, max } => {
                write!(f, "语义值 {value} 超出范围 min={:?} max={:?}", min, max)
            }
            ConversionError::ScaleInvalid { scale, offset } => {
                write!(
                    f,
                    "scale={scale} offset={offset} 必须为有限数值且 scale 非 0"
                )
            }
            ConversionError::NotFinite(v) => write!(f, "数值 {v} 非有限（NaN/±Infinity）"),
            ConversionError::ExactRequired { candidate } => write!(
                f,
                "Exact 取整要求无损表示，但候选值 {candidate} 无法精确表示为目标 raw_type"
            ),
            ConversionError::Overflow {
                candidate,
                raw_type,
            } => {
                write!(f, "候选值 {candidate} 超出 {raw_type:?} 可表示范围")
            }
            ConversionError::PrecisionLoss { value } => {
                write!(f, "整数 {value} 无法无损转换为 f64（禁止静默精度损失）")
            }
        }
    }
}

impl std::error::Error for ConversionError {}

/// 读取解码：`RawReadResult` → 语义 `Value` + `Quality`（§37.1、§7.3）。
///
/// - 携带错误 → `Bad`；协议原始码尽量保留到 `Quality.protocol_code`（§9）；
/// - 数值按 `semantic = raw * scale + offset` 变换后做 checked conversion；
/// - 任何失败均不伪造值，返回 `Bad`。
pub fn decode_read(property: &ProfileProperty, result: &RawReadResult) -> DecodedRead {
    if let Some(error) = &result.error {
        return DecodedRead {
            value: None,
            quality: Quality {
                level: QualityLevel::Bad,
                reason: QualityReason::ProtocolError,
                protocol_code: error.protocol_code.or(result.protocol_quality_code),
                message: Some(error.message.clone()),
            },
        };
    }

    let raw = match &result.value {
        Some(raw) => raw,
        None => return bad_result(result, QualityReason::ProtocolError, "设备无返回值"),
    };

    match decode_value(property, raw) {
        Ok(value) => DecodedRead {
            value: Some(value),
            quality: Quality {
                level: QualityLevel::Good,
                reason: QualityReason::None,
                protocol_code: result.protocol_quality_code,
                message: None,
            },
        },
        Err(message) => bad_result(result, QualityReason::ConfigurationError, &message),
    }
}

fn bad_result(result: &RawReadResult, reason: QualityReason, message: &str) -> DecodedRead {
    DecodedRead {
        value: None,
        quality: Quality {
            level: QualityLevel::Bad,
            reason,
            protocol_code: result.protocol_quality_code,
            message: Some(message.to_owned()),
        },
    }
}

/// 将原始值解码为语义值（§37.1 读取方向）。
fn decode_value(property: &ProfileProperty, raw: &RawValue) -> Result<Value, String> {
    match (&property.raw_type, &property.value_type) {
        (DataType::Bool, DataType::Bool) => match raw {
            RawValue::Bool(b) => Ok(Value::Bool(*b)),
            _ => Err("原始值不是 Bool".to_owned()),
        },
        (DataType::String, DataType::String) => match raw {
            RawValue::String(s) => Ok(Value::String(s.clone())),
            _ => Err("原始值不是 String".to_owned()),
        },
        (DataType::Bytes, DataType::Bytes) => match raw {
            RawValue::Bytes(b) => Ok(Value::Bytes(b.clone())),
            _ => Err("原始值不是 Bytes".to_owned()),
        },
        (raw_type, value_type) if is_numeric(raw_type) && is_numeric(value_type) => {
            // 恒等变换（scale=1、offset=0）且目标为整数类型：全程整数运算，
            // 避免 64 位整数经 f64 丢失精度（如 U64(9_007_199_254_740_993)）。
            if property.scale == 1.0 && property.offset == 0.0 && is_integer_type(value_type) {
                let exact = match (raw_type, raw) {
                    (
                        DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64,
                        RawValue::I64(v),
                    ) => Some(exact_int_semantic(value_type, i128::from(*v))),
                    (
                        DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64,
                        RawValue::U64(v),
                    ) => Some(exact_int_semantic(value_type, i128::from(*v))),
                    _ => None,
                };
                if let Some(result) = exact {
                    return result
                        .ok_or_else(|| format!("原始值 {:?} 无法无损转换为 {value_type:?}", raw));
                }
            }

            let raw_num = numeric_raw_to_f64(raw_type, raw)?;
            let semantic = raw_num * property.scale + property.offset;
            checked_to_semantic(value_type, semantic)
                .ok_or_else(|| format!("语义值 {semantic} 无法无损转换为 {value_type:?}"))
        }
        _ => Err("raw_type/value_type 不支持标量转换（校验阶段应已拦截）".to_owned()),
    }
}

/// 原始数值 → f64（缩放路径中间值）。
///
/// 64 位整数必须无损进入 f64（超出精确表示范围即报错，禁止静默精度损失）；
/// 家族不匹配同样报错。
fn numeric_raw_to_f64(raw_type: &DataType, raw: &RawValue) -> Result<f64, String> {
    match (raw_type, raw) {
        // 整数 raw_type 严格要求家族一致；浮点 raw_type 接受任意数值族。
        (DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64, RawValue::I64(v)) => {
            i64_lossless_to_f64(*v)
                .ok_or_else(|| format!("原始值 {v} 无法无损转换为 f64（禁止静默精度损失）"))
        }
        (DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64, RawValue::U64(v)) => {
            u64_lossless_to_f64(*v)
                .ok_or_else(|| format!("原始值 {v} 无法无损转换为 f64（禁止静默精度损失）"))
        }
        (DataType::F32 | DataType::F64, RawValue::F64(v)) => Ok(*v),
        (DataType::F32 | DataType::F64, RawValue::I64(v)) => i64_lossless_to_f64(*v)
            .ok_or_else(|| format!("原始值 {v} 无法无损转换为 f64（禁止静默精度损失）")),
        (DataType::F32 | DataType::F64, RawValue::U64(v)) => u64_lossless_to_f64(*v)
            .ok_or_else(|| format!("原始值 {v} 无法无损转换为 f64（禁止静默精度损失）")),
        _ => Err(format!(
            "原始值 {raw:?} 与 raw_type {raw_type:?} 家族不匹配"
        )),
    }
}

/// 精确整数语义转换：整数原始值 → 目标整数 `value_type`（范围检查）。
fn exact_int_semantic(value_type: &DataType, v: i128) -> Option<Value> {
    let result = match value_type {
        DataType::I8 => Value::I8(i8::try_from(v).ok()?),
        DataType::I16 => Value::I16(i16::try_from(v).ok()?),
        DataType::I32 => Value::I32(i32::try_from(v).ok()?),
        DataType::I64 => Value::I64(i64::try_from(v).ok()?),
        DataType::U8 => Value::U8(u8::try_from(v).ok()?),
        DataType::U16 => Value::U16(u16::try_from(v).ok()?),
        DataType::U32 => Value::U32(u32::try_from(v).ok()?),
        DataType::U64 => Value::U64(u64::try_from(v).ok()?),
        _ => return None,
    };
    Some(result)
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

fn is_integer_type(t: &DataType) -> bool {
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
    )
}

/// checked conversion：语义浮点 → 目标 `value_type`，溢出即 `None`。
///
/// 整数目标先四舍五入再检查范围（读取方向无取整策略语义，取 Nearest）；
/// `F32` 目标检查上溢为 Infinity。
fn checked_to_semantic(value_type: &DataType, semantic: f64) -> Option<Value> {
    if !semantic.is_finite() {
        return None;
    }
    let rounded = semantic.round();
    let result = match value_type {
        DataType::I8 => Value::I8(i8::try_from(f64_to_i64(rounded)?).ok()?),
        DataType::I16 => Value::I16(i16::try_from(f64_to_i64(rounded)?).ok()?),
        DataType::I32 => Value::I32(i32::try_from(f64_to_i64(rounded)?).ok()?),
        DataType::I64 => Value::I64(f64_to_i64(rounded)?),
        DataType::U8 => Value::U8(u8::try_from(f64_to_u64(rounded)?).ok()?),
        DataType::U16 => Value::U16(u16::try_from(f64_to_u64(rounded)?).ok()?),
        DataType::U32 => Value::U32(u32::try_from(f64_to_u64(rounded)?).ok()?),
        DataType::U64 => Value::U64(f64_to_u64(rounded)?),
        DataType::F32 => {
            let narrowed = semantic as f32;
            if narrowed.is_infinite() {
                return None;
            }
            Value::F32(narrowed)
        }
        DataType::F64 => Value::F64(semantic),
        _ => return None,
    };
    Some(result)
}

/// f64 → i64 checked conversion（std 不提供 `TryFrom<f64>`，手工实现）。
///
/// 边界说明：`i64::MAX as f64` 会舍入为 `2^63`，因此上界必须用 `2^63`
/// 本身判断（`>= 2^63` 拒绝）；传入值必须是已取整的整数语义值。
fn f64_to_i64(v: f64) -> Option<i64> {
    if !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&v) {
        None
    } else {
        Some(v as i64)
    }
}

/// f64 → u64 checked conversion。
///
/// 上界同理：`u64::MAX` 无法精确表示为 f64，以 `2^64` 作为拒绝阈值。
fn f64_to_u64(v: f64) -> Option<u64> {
    if !(0.0..18_446_744_073_709_551_616.0).contains(&v) {
        None
    } else {
        Some(v as u64)
    }
}

/// i64 → f64 无损转换；超出 f64 精确表示范围返回 `None`。
///
/// 例如 `i64::MAX` 转 f64 会舍入为 `2^63`，往返检查即可发现（P1）。
fn i64_lossless_to_f64(v: i64) -> Option<f64> {
    let f = v as f64;
    f64_to_i64(f).filter(|&back| back == v).map(|_| f)
}

/// u64 → f64 无损转换；超出 f64 精确表示范围返回 `None`。
fn u64_lossless_to_f64(v: u64) -> Option<f64> {
    let f = v as f64;
    f64_to_u64(f).filter(|&back| back == v).map(|_| f)
}

/// 写入逆变换：语义 `Value` → 协议原始 `RawValue`（§37.1 写入方向）。
///
/// 失败一律返回 `Err`，绝不静默截断或取近似。
pub fn encode_write(
    property: &ProfileProperty,
    semantic: &Value,
) -> Result<RawValue, ConversionError> {
    // 步骤 1：可写性。
    if !property.writable {
        return Err(ConversionError::NotWritable);
    }

    // Bool/String/Bytes：无缩放直通（校验阶段已保证 scale/offset 为 0）。
    match (&property.value_type, semantic) {
        (DataType::Bool, Value::Bool(b)) => return Ok(RawValue::Bool(*b)),
        (DataType::String, Value::String(s)) => return Ok(RawValue::String(s.clone())),
        (DataType::Bytes, Value::Bytes(b)) => return Ok(RawValue::Bytes(b.clone())),
        (value_type, _)
            if matches!(
                value_type,
                DataType::Bool | DataType::String | DataType::Bytes
            ) =>
        {
            return Err(ConversionError::SemanticTypeMismatch {
                expected: value_type.clone(),
            });
        }
        _ => {}
    }

    // 恒等变换（scale=1、offset=0）且语义为 64 位整数：全程整数运算，
    // 避免经 f64 丢失精度（如 U64(9_007_199_254_740_993) 写入被改成 ...992）。
    if property.scale == 1.0 && property.offset == 0.0 {
        match semantic {
            Value::I64(v) => return encode_exact_int(property, i128::from(*v)),
            Value::U64(v) => return encode_exact_int(property, i128::from(*v)),
            _ => {}
        }
    }

    // 步骤 2：语义值类型与 value_type 匹配（数值族内跨整数/浮点接受，
    // 覆盖 I8..U64/F32/F64 全部数值变体，如 value_type=U16 时传入 Value::U16）。
    // 64 位整数必须无损进入 f64；缩放路径无法精确计算时拒绝（P1）。
    let Some(v) = (match semantic {
        Value::I64(x) => i64_lossless_to_f64(*x),
        Value::U64(x) => u64_lossless_to_f64(*x),
        other => numeric_value_to_f64(other),
    }) else {
        let value = match semantic {
            Value::I64(x) => i128::from(*x),
            Value::U64(x) => i128::from(*x),
            _ => {
                return Err(ConversionError::SemanticTypeMismatch {
                    expected: property.value_type.clone(),
                });
            }
        };
        return Err(ConversionError::PrecisionLoss { value });
    };
    if !v.is_finite() {
        return Err(ConversionError::NotFinite(v));
    }

    // 步骤 3：min/max 范围（语义值范围）。
    let min = property.min.as_ref().and_then(numeric_value_to_f64);
    let max = property.max.as_ref().and_then(numeric_value_to_f64);
    if let Some(min) = min
        && v < min
    {
        return Err(ConversionError::MinMaxViolation {
            value: v,
            min: Some(min),
            max,
        });
    }
    if let Some(max) = max
        && v > max
    {
        return Err(ConversionError::MinMaxViolation {
            value: v,
            min,
            max: Some(max),
        });
    }

    // 步骤 4：scale/offset 必须有限且 scale 非 0（防御：校验阶段已拦截）。
    if property.scale == 0.0 || !property.scale.is_finite() || !property.offset.is_finite() {
        return Err(ConversionError::ScaleInvalid {
            scale: property.scale,
            offset: property.offset,
        });
    }

    // 步骤 5：逆变换。
    let candidate = (v - property.offset) / property.scale;
    if !candidate.is_finite() {
        return Err(ConversionError::NotFinite(candidate));
    }

    match property.raw_type {
        // 步骤 6~8：整数 raw_type 取整 + 范围检查 + checked conversion。
        DataType::I8 => checked_signed(property, candidate, -128.0, 127.0),
        DataType::I16 => checked_signed(property, candidate, -32768.0, 32767.0),
        DataType::I32 => checked_signed(property, candidate, i32::MIN as f64, i32::MAX as f64),
        DataType::I64 => checked_signed(property, candidate, i64::MIN as f64, i64::MAX as f64),
        DataType::U8 => checked_unsigned(property, candidate, 255.0),
        DataType::U16 => checked_unsigned(property, candidate, 65535.0),
        DataType::U32 => checked_unsigned(property, candidate, u32::MAX as f64),
        DataType::U64 => checked_unsigned(property, candidate, u64::MAX as f64),
        DataType::F32 => checked_float(property, candidate),
        DataType::F64 => Ok(RawValue::F64(candidate)),
        _ => Err(ConversionError::SemanticTypeMismatch {
            expected: property.value_type.clone(),
        }),
    }
}

/// 有符号整数 raw_type：取整 + 范围 + checked conversion。
fn checked_signed(
    property: &ProfileProperty,
    candidate: f64,
    min: f64,
    max: f64,
) -> Result<RawValue, ConversionError> {
    let rounded = apply_rounding(property, candidate)?;
    // 先做预检以区分"取整后溢出"与"raw_type 不可表示"（范围界按 f64 表述，
    // 最终以 f64_to_i64 的 checked conversion 为准，I64 极值有精度差异）。
    if !(min..=max).contains(&rounded) {
        return Err(ConversionError::Overflow {
            candidate: rounded,
            raw_type: property.raw_type.clone(),
        });
    }
    let value = f64_to_i64(rounded).ok_or_else(|| ConversionError::Overflow {
        candidate: rounded,
        raw_type: property.raw_type.clone(),
    })?;
    Ok(RawValue::I64(value))
}

/// 无符号整数 raw_type：取整 + 范围 + checked conversion。
fn checked_unsigned(
    property: &ProfileProperty,
    candidate: f64,
    max: f64,
) -> Result<RawValue, ConversionError> {
    let rounded = apply_rounding(property, candidate)?;
    if !(0.0..=max).contains(&rounded) {
        return Err(ConversionError::Overflow {
            candidate: rounded,
            raw_type: property.raw_type.clone(),
        });
    }
    let value = f64_to_u64(rounded).ok_or_else(|| ConversionError::Overflow {
        candidate: rounded,
        raw_type: property.raw_type.clone(),
    })?;
    Ok(RawValue::U64(value))
}

/// 浮点 raw_type：`Exact` 要求 f32 无损；任何取整策略都不得上溢为 Infinity。
fn checked_float(property: &ProfileProperty, candidate: f64) -> Result<RawValue, ConversionError> {
    if property.write_rounding == WriteRounding::Exact {
        let narrowed = candidate as f32;
        if (narrowed as f64) != candidate {
            return Err(ConversionError::ExactRequired { candidate });
        }
    }
    let narrowed = candidate as f32;
    if narrowed.is_infinite() {
        return Err(ConversionError::Overflow {
            candidate,
            raw_type: property.raw_type.clone(),
        });
    }
    Ok(RawValue::F64(narrowed as f64))
}

/// 按 `write_rounding` 处理整数候选值（§37.1 步骤 6）。
fn apply_rounding(property: &ProfileProperty, candidate: f64) -> Result<f64, ConversionError> {
    let rounded = match property.write_rounding {
        WriteRounding::Exact => {
            if candidate.fract() != 0.0 {
                return Err(ConversionError::ExactRequired { candidate });
            }
            candidate
        }
        WriteRounding::Nearest => candidate.round(),
        WriteRounding::Floor => candidate.floor(),
        WriteRounding::Ceil => candidate.ceil(),
        WriteRounding::Truncate => candidate.trunc(),
    };
    Ok(rounded)
}

/// 恒等变换（scale=1、offset=0）下，64 位整数语义 → 原始值的精确整数路径。
///
/// 全程整数运算：min/max 整数界按 `i128` 精确比较；取整策略对整数输入恒等
/// （Exact/Nearest/Floor/Ceil/Truncate 均为原值），无需 apply_rounding；
/// 整数 raw_type 直接 checked conversion，不经过 f64（P1）。
fn encode_exact_int(property: &ProfileProperty, v: i128) -> Result<RawValue, ConversionError> {
    check_exact_bounds(property, v)?;

    let raw = match property.raw_type {
        DataType::I8 => i8::try_from(v).map(i64::from).map(RawValue::I64),
        DataType::I16 => i16::try_from(v).map(i64::from).map(RawValue::I64),
        DataType::I32 => i32::try_from(v).map(i64::from).map(RawValue::I64),
        DataType::I64 => i64::try_from(v).map(RawValue::I64),
        DataType::U8 => u8::try_from(v).map(u64::from).map(RawValue::U64),
        DataType::U16 => u16::try_from(v).map(u64::from).map(RawValue::U64),
        DataType::U32 => u32::try_from(v).map(u64::from).map(RawValue::U64),
        DataType::U64 => u64::try_from(v).map(RawValue::U64),
        DataType::F32 => {
            // F32 寄存器按自身精度要求无损承载整数语义：Exact 下 I64(2^24+1)
            // 不得写成 2^24（f32 缩窄精度校验，P1）。f32 无损 ⟹ f64 无损，
            // 此处只需针对 f32 检查。`narrowed as i128` 精确无饱和：
            // |v| ≤ 2^64-1，f32 动态范围（≈3.4e38）足以承载，不会溢出。
            let narrowed = (v as f64) as f32;
            if (narrowed as i128) != v {
                return Err(match property.write_rounding {
                    WriteRounding::Exact => ConversionError::ExactRequired {
                        candidate: narrowed as f64,
                    },
                    _ => ConversionError::PrecisionLoss { value: v },
                });
            }
            return Ok(RawValue::F64(narrowed as f64));
        }
        DataType::F64 => {
            // 64 位整数语义必须无损进入 f64，否则拒绝——取整策略只允许作用于
            // 整数 raw_type 的候选量化（§37.1 步骤 6），不得静默改写整数语义
            // 本身（如 U64(2^53+1) → 2^53）。
            // `f as i128` 饱和转换在此安全：|f| 不超过 |v| ≤ 1.7e19 << 2^127。
            let f = v as f64;
            if (f as i128) != v {
                return Err(match property.write_rounding {
                    WriteRounding::Exact => ConversionError::ExactRequired { candidate: f },
                    _ => ConversionError::PrecisionLoss { value: v },
                });
            }
            return Ok(RawValue::F64(f));
        }
        _ => {
            return Err(ConversionError::SemanticTypeMismatch {
                expected: property.value_type.clone(),
            });
        }
    };
    raw.map_err(|_| ConversionError::Overflow {
        candidate: v as f64,
        raw_type: property.raw_type.clone(),
    })
}

/// 整数语义的 min/max 精确比较：整数界按 `i128` 精确比较；
/// 浮点界按 `ceil`/`floor` 整数化后精确比较（P1：避免 `U64(2^53+1)`
/// 经 f64 变成 `2^53` 后与 `F64(2^53)` 界误判为相等）。
fn check_exact_bounds(property: &ProfileProperty, v: i128) -> Result<(), ConversionError> {
    let min = property.min.as_ref();
    let max = property.max.as_ref();
    let violation = |min: Option<f64>, max: Option<f64>| ConversionError::MinMaxViolation {
        value: v as f64,
        min,
        max,
    };
    if let Some(min_value) = min {
        let below = match integer_value_to_i128(min_value) {
            Some(bound) => v < bound,
            None => i128_lt_f64(v, numeric_value_to_f64(min_value).unwrap_or(f64::NAN)),
        };
        if below {
            return Err(violation(
                numeric_value_to_f64(min_value),
                max.and_then(numeric_value_to_f64),
            ));
        }
    }
    if let Some(max_value) = max {
        let above = match integer_value_to_i128(max_value) {
            Some(bound) => v > bound,
            None => i128_gt_f64(v, numeric_value_to_f64(max_value).unwrap_or(f64::NAN)),
        };
        if above {
            return Err(violation(
                min.and_then(numeric_value_to_f64),
                numeric_value_to_f64(max_value),
            ));
        }
    }
    Ok(())
}

/// 整数语义值 `v` 与浮点界 `bound` 的精确比较：`v > bound`。
///
/// 整数 `v` 大于小数界 ⟺ `v > floor(bound)`；`bound` 本身为整数时直接比较。
/// `|bound|` 超出 `i128` 范围时饱和转换仍给出正确结果（`v ≤ 1.8e19`）。
/// NaN 界视为无约束（校验阶段应已拒绝非有限界）。
fn i128_gt_f64(v: i128, bound: f64) -> bool {
    if bound.is_nan() {
        return false;
    }
    let integerized = if bound.fract() == 0.0 {
        bound
    } else {
        bound.floor()
    };
    v > (integerized as i128)
}

/// 整数语义值 `v` 与浮点界 `bound` 的精确比较：`v < bound`（按 `ceil` 整数化）。
fn i128_lt_f64(v: i128, bound: f64) -> bool {
    if bound.is_nan() {
        return false;
    }
    let integerized = if bound.fract() == 0.0 {
        bound
    } else {
        bound.ceil()
    };
    v < (integerized as i128)
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

/// 数值 `Value` → `f64`（覆盖全部数值变体）；非数值返回 `None`。
fn numeric_value_to_f64(v: &Value) -> Option<f64> {
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
    use observation_model::{DriverErrorInfo, QualityLevel};

    use super::*;

    /// 默认写入属性：U16 raw、F64 语义、scale 0.01、范围 0..400。
    fn write_property() -> ProfileProperty {
        ProfileProperty {
            path: "drive.output.frequency".to_owned(),
            driver_address: "1!40001".to_owned(),
            raw_type: DataType::U16,
            value_type: DataType::F64,
            unit: Some("Hz".to_owned()),
            scale: 0.01,
            offset: 0.0,
            write_rounding: WriteRounding::Exact,
            readable: true,
            writable: true,
            default_interval_ms: None,
            min: Some(Value::F64(0.0)),
            max: Some(Value::F64(400.0)),
        }
    }

    fn result(value: Option<RawValue>) -> RawReadResult {
        RawReadResult {
            item_id: 1,
            value,
            source_timestamp_ns: Some(1_700_000_000_000_000_000),
            received_timestamp_ns: 1_700_000_000_000_000_001,
            protocol_quality_code: None,
            error: None,
        }
    }

    // ---- decode_read：正常路径 ----

    #[test]
    fn decode_scaled_value() {
        let property = write_property();
        let decoded = decode_read(&property, &result(Some(RawValue::U64(5000))));
        assert_eq!(decoded.value, Some(Value::F64(50.0)));
        assert_eq!(decoded.quality.level, QualityLevel::Good);
        assert_eq!(decoded.quality.reason, QualityReason::None);
    }

    #[test]
    fn decode_with_offset_and_negative_scale() {
        let mut property = write_property();
        property.raw_type = DataType::I16;
        property.scale = -0.1;
        property.offset = 10.0;
        let decoded = decode_read(&property, &result(Some(RawValue::I64(-100))));
        assert_eq!(decoded.value, Some(Value::F64(20.0)));
    }

    #[test]
    fn decode_integer_value_type_rounds() {
        let mut property = write_property();
        property.value_type = DataType::U16;
        let decoded = decode_read(&property, &result(Some(RawValue::U64(5001))));
        assert_eq!(decoded.value, Some(Value::U16(50)));
    }

    #[test]
    fn decode_float_raw_accepts_numeric_family() {
        let mut property = write_property();
        property.raw_type = DataType::F32;
        let decoded = decode_read(&property, &result(Some(RawValue::F64(5000.0))));
        assert_eq!(decoded.value, Some(Value::F64(50.0)));
    }

    #[test]
    fn decode_bool_and_string_passthrough() {
        let mut property = write_property();
        property.raw_type = DataType::Bool;
        property.value_type = DataType::Bool;
        let decoded = decode_read(&property, &result(Some(RawValue::Bool(true))));
        assert_eq!(decoded.value, Some(Value::Bool(true)));

        let mut property = write_property();
        property.raw_type = DataType::String;
        property.value_type = DataType::String;
        let decoded = decode_read(
            &property,
            &result(Some(RawValue::String("MD500".to_owned()))),
        );
        assert_eq!(decoded.value, Some(Value::String("MD500".to_owned())));
    }

    // ---- decode_read：错误与边界路径 ----

    #[test]
    fn decode_driver_error_maps_to_bad() {
        let property = write_property();
        let mut result = result(Some(RawValue::U64(5000)));
        result.error = Some(DriverErrorInfo {
            code: "timeout".to_owned(),
            message: "slave 无响应".to_owned(),
            protocol_code: Some(0x80),
            retryable: true,
        });
        let decoded = decode_read(&property, &result);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
        assert_eq!(decoded.quality.reason, QualityReason::ProtocolError);
        assert_eq!(decoded.quality.protocol_code, Some(0x80));
        assert_eq!(decoded.quality.message.as_deref(), Some("slave 无响应"));
    }

    #[test]
    fn decode_missing_value_is_bad() {
        let property = write_property();
        let decoded = decode_read(&property, &result(None));
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
    }

    #[test]
    fn decode_type_mismatch_is_bad() {
        let property = write_property();
        let decoded = decode_read(&property, &result(Some(RawValue::String("x".to_owned()))));
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
        assert_eq!(decoded.quality.reason, QualityReason::ConfigurationError);
    }

    #[test]
    fn decode_integer_overflow_is_bad() {
        let mut property = write_property();
        property.value_type = DataType::U8;
        let decoded = decode_read(&property, &result(Some(RawValue::U64(30_000))));
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
    }

    #[test]
    fn decode_f32_overflow_is_bad() {
        let mut property = write_property();
        property.value_type = DataType::F32;
        property.scale = f64::MAX / 2.0;
        property.offset = 0.0;
        let decoded = decode_read(&property, &result(Some(RawValue::U64(1))));
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn decode_protocol_code_carried_on_good() {
        let property = write_property();
        let mut result = result(Some(RawValue::U64(5000)));
        result.protocol_quality_code = Some(42);
        let decoded = decode_read(&property, &result);
        assert_eq!(decoded.quality.level, QualityLevel::Good);
        assert_eq!(decoded.quality.protocol_code, Some(42));
    }

    // ---- decode：64 位整数精度（P1） ----

    #[test]
    fn decode_identity_u64_full_precision() {
        // 恒等变换（scale=1、offset=0）下 U64 必须精确传递，
        // 不得经 f64 变成 9_007_199_254_740_992。
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::U64;
        property.scale = 1.0;
        property.offset = 0.0;
        let decoded = decode_read(
            &property,
            &result(Some(RawValue::U64(9_007_199_254_740_993))),
        );
        assert_eq!(decoded.value, Some(Value::U64(9_007_199_254_740_993)));
        assert_eq!(decoded.quality.level, QualityLevel::Good);
    }

    #[test]
    fn decode_identity_i64_full_precision() {
        let mut property = write_property();
        property.raw_type = DataType::I64;
        property.value_type = DataType::I64;
        property.scale = 1.0;
        property.offset = 0.0;
        let decoded = decode_read(
            &property,
            &result(Some(RawValue::I64(-9_223_372_036_854_775_807))),
        );
        assert_eq!(decoded.value, Some(Value::I64(-9_223_372_036_854_775_807)));
    }

    #[test]
    fn decode_identity_narrowing_rejected() {
        let mut property = write_property();
        property.raw_type = DataType::U16;
        property.value_type = DataType::U8;
        property.scale = 1.0;
        property.offset = 0.0;
        let decoded = decode_read(&property, &result(Some(RawValue::U64(300))));
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
    }

    #[test]
    fn decode_identity_u64_to_f64_lossy_is_bad() {
        // 恒等变换 + 浮点语义目标：U64 无法无损进入 f64 时拒绝，
        // 不允许静默变成最近值。
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::F64;
        property.scale = 1.0;
        property.offset = 0.0;
        let decoded = decode_read(
            &property,
            &result(Some(RawValue::U64(9_007_199_254_740_993))),
        );
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
        assert_eq!(decoded.quality.reason, QualityReason::ConfigurationError);
    }

    #[test]
    fn decode_scaled_lossy_u64_is_bad() {
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::U64;
        property.scale = 2.0;
        property.offset = 0.0;
        let decoded = decode_read(
            &property,
            &result(Some(RawValue::U64(9_007_199_254_740_993))),
        );
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);
    }

    // ---- encode_write：正常路径 ----

    #[test]
    fn encode_inverse_scale() {
        let property = write_property();
        let raw = encode_write(&property, &Value::F64(50.0)).expect("50Hz 应可无损编码");
        assert_eq!(raw, RawValue::U64(5000));
    }

    #[test]
    fn encode_offset_and_scale() {
        let mut property = write_property();
        property.scale = 0.1;
        property.offset = 4.0;
        let raw = encode_write(&property, &Value::F64(54.0)).expect("逆变换 (54-4)/0.1=500");
        assert_eq!(raw, RawValue::U64(500));
    }

    #[test]
    fn encode_integer_value_type_input() {
        let property = write_property();
        let raw = encode_write(&property, &Value::I64(50)).expect("I64 语义值应被接受");
        assert_eq!(raw, RawValue::U64(5000));
    }

    #[test]
    fn encode_matching_integer_variant_accepted() {
        // value_type=U16 时应接受 Value::U16（P1：覆盖全部数值变体）。
        let mut property = write_property();
        property.value_type = DataType::U16;
        let raw = encode_write(&property, &Value::U16(50)).expect("Value::U16 应被接受");
        assert_eq!(raw, RawValue::U64(5000));
    }

    #[test]
    fn encode_all_numeric_variants_accepted() {
        let mut property = write_property();
        property.value_type = DataType::F64;
        for value in [
            Value::I8(1),
            Value::I16(1),
            Value::I32(1),
            Value::I64(1),
            Value::U8(1),
            Value::U16(1),
            Value::U32(1),
            Value::U64(1),
            Value::F32(1.0),
            Value::F64(1.0),
        ] {
            encode_write(&property, &value)
                .unwrap_or_else(|e| panic!("数值变体 {value:?} 应被接受: {e}"));
        }
    }

    #[test]
    fn encode_f32_semantic_variant_accepted() {
        let property = write_property();
        let raw = encode_write(&property, &Value::F32(50.0)).expect("F32 语义值应被接受");
        assert_eq!(raw, RawValue::U64(5000));
    }

    // ---- encode：64 位整数精度（P1） ----

    #[test]
    fn encode_identity_u64_full_precision() {
        // 恒等变换（scale=1、offset=0）下 U64 必须精确写出，
        // 不得经 f64 变成 9_007_199_254_740_992。
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::U64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        let raw =
            encode_write(&property, &Value::U64(9_007_199_254_740_993)).expect("恒等变换应无损");
        assert_eq!(raw, RawValue::U64(9_007_199_254_740_993));
    }

    #[test]
    fn encode_identity_i64_full_precision() {
        let mut property = write_property();
        property.raw_type = DataType::I64;
        property.value_type = DataType::I64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        let raw = encode_write(&property, &Value::I64(-9_223_372_036_854_775_807))
            .expect("恒等变换应无损");
        assert_eq!(raw, RawValue::I64(-9_223_372_036_854_775_807));
    }

    #[test]
    fn encode_identity_narrowing_overflow_rejected() {
        let mut property = write_property();
        property.raw_type = DataType::U8;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        let e = encode_write(&property, &Value::U64(300)).expect_err("300 超出 U8 应拒绝");
        assert!(matches!(e, ConversionError::Overflow { .. }));
    }

    #[test]
    fn encode_identity_bounds_exact_integer() {
        // 整数 min/max 界必须精确比较：f64 界会把 9_007_199_254_740_993
        // 与 max=9_007_199_254_740_992 误判为相等。
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::U64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = Some(Value::U64(0));
        property.max = Some(Value::U64(9_007_199_254_740_992));
        let e = encode_write(&property, &Value::U64(9_007_199_254_740_993))
            .expect_err("超出整数 max 应被拒绝");
        assert!(matches!(e, ConversionError::MinMaxViolation { .. }));
    }

    #[test]
    fn encode_scaled_lossy_u64_rejected() {
        // 缩放路径无法用 f64 精确计算中间值时拒绝（PrecisionLoss）。
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::U64;
        property.scale = 2.0;
        property.min = None;
        property.max = None;
        let e = encode_write(&property, &Value::U64(9_007_199_254_740_993))
            .expect_err("缩放路径无损要求应拒绝");
        assert!(matches!(
            e,
            ConversionError::PrecisionLoss {
                value: 9_007_199_254_740_993
            }
        ));
    }

    #[test]
    fn encode_identity_f32_raw_exact_narrowing_check() {
        // P1：F32 寄存器 + Exact 必须做 f32 缩窄精度校验——
        // I64(2^24+1) 不得静默写成 2^24（f64 无损但 f32 有损）。
        let mut property = write_property();
        property.raw_type = DataType::F32;
        property.value_type = DataType::I64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        let e = encode_write(&property, &Value::I64(16_777_217))
            .expect_err("F32 无法无损承载 2^24+1，Exact 应拒绝");
        assert!(matches!(e, ConversionError::ExactRequired { .. }));

        let raw = encode_write(&property, &Value::I64(16_777_216)).expect("f32 可表示值应成功");
        assert_eq!(raw, RawValue::F64(16_777_216.0));

        encode_write(&property, &Value::I64(-16_777_216)).expect("负 f32 可表示值应成功");
        let e =
            encode_write(&property, &Value::I64(-16_777_217)).expect_err("负 2^24+1 同样应被拒绝");
        assert!(matches!(e, ConversionError::ExactRequired { .. }));
    }

    #[test]
    fn encode_identity_f32_raw_non_exact_narrowing_rejected() {
        // 非 Exact 策略同样不得把整数语义缩窄进 f32（如 2^24+1 → 2^24）。
        let mut property = write_property();
        property.raw_type = DataType::F32;
        property.value_type = DataType::I64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        property.write_rounding = WriteRounding::Nearest;
        let e = encode_write(&property, &Value::I64(16_777_217))
            .expect_err("Nearest 不得静默缩窄整数语义");
        assert!(matches!(
            e,
            ConversionError::PrecisionLoss { value: 16_777_217 }
        ));
    }

    #[test]
    fn encode_identity_f64_raw_holds_beyond_f32_precision() {
        // F64 寄存器对 2^24+1 无损（仅 F32 需要缩窄校验）。
        let mut property = write_property();
        property.raw_type = DataType::F64;
        property.value_type = DataType::I64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        let raw = encode_write(&property, &Value::I64(16_777_217)).expect("f64 应无损承载");
        assert_eq!(raw, RawValue::F64(16_777_217.0));
    }

    #[test]
    fn encode_identity_float_raw_exact_round_trip() {
        let mut property = write_property();
        property.raw_type = DataType::F64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = None;
        let e = encode_write(&property, &Value::U64(9_007_199_254_740_993))
            .expect_err("F64 寄存器无法无损承载该整数，Exact 应拒绝");
        assert!(matches!(e, ConversionError::ExactRequired { .. }));

        let raw =
            encode_write(&property, &Value::U64(9_007_199_254_740_992)).expect("可表示值应成功");
        assert_eq!(raw, RawValue::F64(9_007_199_254_740_992.0));
    }

    #[test]
    fn encode_identity_float_raw_non_exact_precision_loss() {
        // P1：raw_type=F64 且策略非 Exact 时，U64(2^53+1) 不得静默写成 2^53。
        for rounding in [
            WriteRounding::Nearest,
            WriteRounding::Floor,
            WriteRounding::Ceil,
            WriteRounding::Truncate,
        ] {
            let mut property = write_property();
            property.raw_type = DataType::F64;
            property.scale = 1.0;
            property.offset = 0.0;
            property.min = None;
            property.max = None;
            property.write_rounding = rounding;
            let e = encode_write(&property, &Value::U64(9_007_199_254_740_993))
                .expect_err("非 Exact 策略同样不得静默损失整数精度");
            assert!(
                matches!(
                    e,
                    ConversionError::PrecisionLoss {
                        value: 9_007_199_254_740_993
                    }
                ),
                "rounding={rounding:?} 应返回 PrecisionLoss"
            );
        }
    }

    #[test]
    fn encode_identity_float_bound_exact_compare() {
        // P1：浮点界比较不得因 f64 舍入把 U64(2^53+1) 与 F64(2^53) 判为相等。
        let mut property = write_property();
        property.raw_type = DataType::U64;
        property.value_type = DataType::U64;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = Some(Value::U64(0));
        property.max = Some(Value::F64(9_007_199_254_740_992.0));
        let e = encode_write(&property, &Value::U64(9_007_199_254_740_993))
            .expect_err("超出浮点 max=2^53 应被精确拒绝");
        assert!(matches!(e, ConversionError::MinMaxViolation { .. }));

        // 有小数部分的浮点界：50.5 Hz 语义 → 51 越界、50 通过。
        let mut property = write_property();
        property.raw_type = DataType::U16;
        property.value_type = DataType::U16;
        property.scale = 1.0;
        property.offset = 0.0;
        property.min = None;
        property.max = Some(Value::F64(50.5));
        encode_write(&property, &Value::U16(50)).expect("50 ≤ 50.5 应通过");
        let e = encode_write(&property, &Value::U16(51)).expect_err("51 > 50.5 应拒绝");
        assert!(matches!(e, ConversionError::MinMaxViolation { .. }));
    }

    #[test]
    fn encode_bool_string_passthrough() {
        let mut property = write_property();
        property.raw_type = DataType::Bool;
        property.value_type = DataType::Bool;
        property.scale = 0.0;
        property.offset = 0.0;
        let raw = encode_write(&property, &Value::Bool(true)).expect("Bool 直通");
        assert_eq!(raw, RawValue::Bool(true));

        let mut property = write_property();
        property.raw_type = DataType::String;
        property.value_type = DataType::String;
        property.scale = 0.0;
        property.offset = 0.0;
        let raw = encode_write(&property, &Value::String("start".to_owned())).expect("String 直通");
        assert_eq!(raw, RawValue::String("start".to_owned()));
    }

    // ---- encode_write：取整策略 ----

    #[test]
    fn encode_exact_rejects_non_integral_candidate() {
        let property = write_property();
        let e = encode_write(&property, &Value::F64(50.015))
            .expect_err("Exact 应拒绝非整数候选 (5001.5)");
        assert!(matches!(e, ConversionError::ExactRequired { .. }));
    }

    #[test]
    fn encode_nearest_rounds() {
        let mut property = write_property();
        property.write_rounding = WriteRounding::Nearest;
        let raw = encode_write(&property, &Value::F64(50.015)).expect("Nearest 应取整");
        assert_eq!(raw, RawValue::U64(5002));

        let raw = encode_write(&property, &Value::F64(50.014)).expect("Nearest 应向下");
        assert_eq!(raw, RawValue::U64(5001));
    }

    #[test]
    fn encode_floor_ceil_truncate() {
        let base = |rounding: WriteRounding, v: f64| -> u64 {
            let mut property = write_property();
            property.write_rounding = rounding;
            let raw = encode_write(&property, &Value::F64(v)).expect("应编码成功");
            let RawValue::U64(x) = raw else {
                panic!("应为 U64");
            };
            x
        };
        assert_eq!(base(WriteRounding::Floor, 50.019), 5001);
        assert_eq!(base(WriteRounding::Ceil, 50.011), 5002);
        assert_eq!(base(WriteRounding::Truncate, 50.019), 5001);
        // 负语义值在允许范围内时，Truncate 向零截断（不同于 Floor）。
        let mut property = write_property();
        property.min = Some(Value::F64(-1.0));
        property.write_rounding = WriteRounding::Truncate;
        let raw = encode_write(&property, &Value::F64(-0.009)).expect("范围内负值应编码成功");
        assert_eq!(raw, RawValue::U64(0));
        // Floor 向负无穷取整得到 -1，超出 U16 范围 → 拒绝。
        property.write_rounding = WriteRounding::Floor;
        let e = encode_write(&property, &Value::F64(-0.009)).expect_err("Floor 负值应溢出拒绝");
        assert!(matches!(e, ConversionError::Overflow { .. }));
    }

    // ---- encode_write：错误路径（用户指定场景） ----

    #[test]
    fn encode_zero_scale_rejected() {
        let mut property = write_property();
        property.scale = 0.0;
        let e = encode_write(&property, &Value::F64(50.0)).expect_err("scale=0 应拒绝");
        assert!(matches!(e, ConversionError::ScaleInvalid { scale, .. } if scale == 0.0));
    }

    #[test]
    fn encode_nan_scale_rejected() {
        let mut property = write_property();
        property.scale = f64::NAN;
        let e = encode_write(&property, &Value::F64(50.0)).expect_err("NaN scale 应拒绝");
        assert!(matches!(e, ConversionError::ScaleInvalid { .. }));
    }

    #[test]
    fn encode_infinity_scale_rejected() {
        for scale in [f64::INFINITY, f64::NEG_INFINITY] {
            let mut property = write_property();
            property.scale = scale;
            let e = encode_write(&property, &Value::F64(50.0)).expect_err("Infinity scale 应拒绝");
            assert!(matches!(e, ConversionError::ScaleInvalid { .. }));
        }
    }

    #[test]
    fn encode_non_finite_offset_rejected() {
        let mut property = write_property();
        property.offset = f64::NAN;
        let e = encode_write(&property, &Value::F64(50.0)).expect_err("NaN offset 应拒绝");
        assert!(matches!(e, ConversionError::ScaleInvalid { .. }));
    }

    #[test]
    fn encode_type_mismatch_rejected() {
        let property = write_property();
        let e = encode_write(&property, &Value::String("50".to_owned()))
            .expect_err("String 语义值应拒绝");
        assert!(matches!(e, ConversionError::SemanticTypeMismatch { .. }));
    }

    #[test]
    fn encode_nan_semantic_rejected() {
        let property = write_property();
        let e = encode_write(&property, &Value::F64(f64::NAN)).expect_err("NaN 语义值应拒绝");
        assert!(matches!(e, ConversionError::NotFinite(_)));
    }

    #[test]
    fn encode_out_of_range_rejected() {
        let property = write_property();
        let e = encode_write(&property, &Value::F64(400.01)).expect_err("超出 max 应拒绝");
        assert!(matches!(e, ConversionError::MinMaxViolation { .. }));

        let e = encode_write(&property, &Value::F64(-0.01)).expect_err("低于 min 应拒绝");
        assert!(matches!(e, ConversionError::MinMaxViolation { .. }));
    }

    #[test]
    fn encode_integer_overflow_rejected() {
        let mut property = write_property();
        property.raw_type = DataType::U8;
        property.min = None;
        property.max = None;
        let e =
            encode_write(&property, &Value::F64(300.0)).expect_err("300Hz→30000 超出 U8 应拒绝");
        assert!(matches!(e, ConversionError::Overflow { .. }));
    }

    #[test]
    fn encode_negative_to_unsigned_rejected() {
        let mut property = write_property();
        property.min = None;
        let e = encode_write(&property, &Value::F64(-1.0)).expect_err("负数语义→U16 应拒绝");
        assert!(matches!(e, ConversionError::Overflow { .. }));
    }

    #[test]
    fn encode_signed_raw_type_accepts_negative() {
        let mut property = write_property();
        property.raw_type = DataType::I16;
        property.scale = 1.0;
        property.min = None;
        property.max = None;
        let raw = encode_write(&property, &Value::F64(-123.0)).expect("I16 应接受负数");
        assert_eq!(raw, RawValue::I64(-123));
    }

    #[test]
    fn encode_not_writable_rejected() {
        let mut property = write_property();
        property.writable = false;
        let e = encode_write(&property, &Value::F64(50.0)).expect_err("只读属性应拒绝");
        assert_eq!(e, ConversionError::NotWritable);
    }

    #[test]
    fn encode_f32_exact_rejects_lossy() {
        let mut property = write_property();
        property.raw_type = DataType::F32;
        property.scale = 1.0;
        property.min = None;
        property.max = None;
        let e = encode_write(&property, &Value::F64(0.1))
            .expect_err("0.1 在 f32 中有舍入误差，Exact 应拒绝");
        assert!(matches!(e, ConversionError::ExactRequired { .. }));
    }

    #[test]
    fn encode_f32_exact_accepts_representable() {
        let mut property = write_property();
        property.raw_type = DataType::F32;
        property.scale = 1.0;
        property.min = None;
        property.max = None;
        let raw = encode_write(&property, &Value::F64(0.5)).expect("0.5 可无损表示为 f32");
        assert_eq!(raw, RawValue::F64(0.5));
    }
}
