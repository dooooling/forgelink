//! 写值编码：`RawValue` → MC 批量写数据区载荷。
//!
//! # 编码规则（写入侧，权威）
//!
//! 与读取侧同表：expected_type（来自 Profile value_type）决定点数；
//! Loader 通路恒发宽 Tag——按值最小无损收窄定点数：
//!
//! | 值 | 点数 | 规则 |
//! |---|---|---|
//! | I64 ∈ [i16, u16 域] / U64 ≤ u16::MAX | 1 | LE 2 字节 |
//! | I64 ∈ [i32, u32 域] / U64 ≤ u32::MAX | 2 | LE 4 字节 |
//! | 其余整数 | 4 | LE 8 字节 |
//! | F64 可无损缩窄 f32 | 2 | f32 位型 LE |
//! | F64 其余（含 NaN/Inf） | 4 | f64 位型 LE |
//!
//! 位软元件仅接受 Bool → 1 字节 0/1。越界不存在（宽 Tag 值域全覆盖），
//! 类型不兼容一律 invalid_type。全小端。

use observation_model::{DataType, RawValue};

use crate::address::DeviceKind;
use crate::error::McError;

/// 编码后的写载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedWrite {
    /// 访问点数。
    pub points: u16,
    /// 数据字节（字 LE 或位串单字节）。
    pub data: Vec<u8>,
}

/// 编码一个写值。
///
/// `expected` 保留为 ABI 层签名一致性占位（MC 写宽度由值本身决定最小
/// 无损点数，与读取侧 expected_type 定宽度的方向相反）。
///
/// # Errors
///
/// Tag 与软元件类别不兼容返回 `invalid_type`。
pub fn encode_write(
    kind: DeviceKind,
    expected: Option<DataType>,
    value: &RawValue,
) -> Result<EncodedWrite, McError> {
    let _ = expected;
    if kind.is_bit() {
        return encode_bit(value);
    }
    let type_name = |v: &RawValue| match v {
        RawValue::Bool(_) => "Bool",
        RawValue::I64(_) => "I64",
        RawValue::U64(_) => "U64",
        RawValue::F64(_) => "F64",
        RawValue::String(_) => "String",
        RawValue::Bytes(_) => "Bytes",
        RawValue::Array(_) => "Array",
        RawValue::Struct(_) => "Struct",
    };
    // 字软元件：以值本身决定最小无损宽度（宽 Tag 收窄）。
    match (kind, value) {
        (DeviceKind::D, RawValue::I64(v)) => narrow_signed_i(*v),
        (DeviceKind::D, RawValue::U64(v)) => narrow_unsigned_u(*v),
        (DeviceKind::D, RawValue::F64(v)) => Ok(narrow_float(*v)),
        // W/R/ZR/SD 走同一数值编码路径。
        (_, RawValue::I64(v)) if matches!(kind, DeviceKind::W | DeviceKind::R | DeviceKind::Zr) => {
            narrow_signed_i(*v)
        }
        (_, RawValue::U64(v)) if matches!(kind, DeviceKind::W | DeviceKind::R | DeviceKind::Zr) => {
            narrow_unsigned_u(*v)
        }
        (_, RawValue::F64(v)) if matches!(kind, DeviceKind::W | DeviceKind::R | DeviceKind::Zr) => {
            Ok(narrow_float(*v))
        }
        (_, v) => Err(McError::invalid_type(format!(
            "字软元件 {kind:?} 不接受值类型 {}（映射表见模块文档）",
            type_name(v)
        ))),
    }
}

fn encode_bit(value: &RawValue) -> Result<EncodedWrite, McError> {
    let type_name = |v: &RawValue| match v {
        RawValue::Bool(_) => "Bool",
        RawValue::I64(_) => "I64",
        RawValue::U64(_) => "U64",
        RawValue::F64(_) => "F64",
        RawValue::String(_) => "String",
        RawValue::Bytes(_) => "Bytes",
        RawValue::Array(_) => "Array",
        RawValue::Struct(_) => "Struct",
    };
    match value {
        RawValue::Bool(b) => Ok(EncodedWrite {
            points: 1,
            data: vec![u8::from(*b)],
        }),
        other => Err(McError::invalid_type(format!(
            "位软元件不接受值类型 {}（仅 Bool）",
            type_name(other)
        ))),
    }
}

/// 有符号整数按最小无损宽度收窄。
#[allow(clippy::cast_possible_truncation)]
fn narrow_signed_i(v: i64) -> Result<EncodedWrite, McError> {
    let (points, bytes): (u16, Vec<u8>) = if i32::try_from(v).is_ok() && i16::try_from(v).is_ok() {
        (1, v.to_le_bytes()[..2].to_vec())
    } else if i32::try_from(v).is_ok() {
        (2, v.to_le_bytes()[..4].to_vec())
    } else {
        (4, v.to_le_bytes().to_vec())
    };
    Ok(EncodedWrite {
        points,
        data: bytes,
    })
}

/// 无符号整数按最小无损宽度收窄。
fn narrow_unsigned_u(v: u64) -> Result<EncodedWrite, McError> {
    let (points, bytes): (u16, Vec<u8>) = if v <= u64::from(u16::MAX) {
        (1, v.to_le_bytes()[..2].to_vec())
    } else if v <= u64::from(u32::MAX) {
        (2, v.to_le_bytes()[..4].to_vec())
    } else {
        (4, v.to_le_bytes().to_vec())
    };
    Ok(EncodedWrite {
        points,
        data: bytes,
    })
}

/// 浮点按可否无损缩窄决定 2 点（f32）或 4 点（f64）；NaN/Inf 恒 4 点。
#[allow(clippy::cast_possible_truncation)]
fn narrow_float(v: f64) -> EncodedWrite {
    let narrowed = v as f32;
    if !narrowed.is_nan() && f64::from(narrowed) == v {
        EncodedWrite {
            points: 2,
            data: narrowed.to_le_bytes().to_vec(),
        }
    } else {
        EncodedWrite {
            points: 4,
            data: v.to_le_bytes().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_accepts_only_bool() {
        assert_eq!(
            encode_write(DeviceKind::Y, Some(DataType::Bool), &RawValue::Bool(true)).unwrap(),
            EncodedWrite {
                points: 1,
                data: vec![1]
            }
        );
        assert!(encode_write(DeviceKind::Y, Some(DataType::Bool), &RawValue::I64(1)).is_err());
    }

    #[test]
    fn integers_narrow_minimally() {
        // 5000 → 1 点。
        assert_eq!(
            encode_write(DeviceKind::D, None, &RawValue::I64(5000)).unwrap(),
            EncodedWrite {
                points: 1,
                data: vec![0x88, 0x13]
            }
        );
        // -2 → 1 点补码。
        assert_eq!(
            encode_write(DeviceKind::D, None, &RawValue::I64(-2)).unwrap(),
            EncodedWrite {
                points: 1,
                data: vec![0xFE, 0xFF]
            }
        );
        // 70000 超 u16 → 2 点。
        assert_eq!(
            encode_write(DeviceKind::D, None, &RawValue::I64(70_000)).unwrap(),
            EncodedWrite {
                points: 2,
                data: vec![0x70, 0x11, 0x01, 0x00]
            }
        );
        // U64 极大 → 4 点。
        let big = encode_write(DeviceKind::D, None, &RawValue::U64(u64::MAX)).unwrap();
        assert_eq!(big.points, 4);
        assert_eq!(big.data, vec![0xFF; 8]);
    }

    #[test]
    fn floats_narrow_losslessly_or_stay_wide() {
        // 1.5 可无损缩窄 → 2 点 f32。
        assert_eq!(
            encode_write(DeviceKind::D, None, &RawValue::F64(1.5)).unwrap(),
            EncodedWrite {
                points: 2,
                data: vec![0, 0, 0xC0, 0x3F]
            }
        );
        // 0.1 不可缩窄 → 4 点 f64。
        assert_eq!(
            encode_write(DeviceKind::D, None, &RawValue::F64(0.1))
                .unwrap()
                .points,
            4
        );
        // NaN 恒 4 点。
        assert_eq!(
            encode_write(DeviceKind::D, None, &RawValue::F64(f64::NAN))
                .unwrap()
                .points,
            4
        );
    }

    #[test]
    fn string_rejected_on_word_device() {
        assert!(encode_write(DeviceKind::D, None, &RawValue::String("x".into())).is_err());
    }
}
