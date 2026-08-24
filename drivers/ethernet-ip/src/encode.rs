//! 写值编码：`RawValue` → Write Tag 载荷（按发现的类型码收窄）。
//!
//! # 编码规则（写入侧，权威）
//!
//! 目标类型码来自**懒式类型发现缓存**（首写先 Read Tag 探明设备声明
//! 的类型——Logix 写要求类型码精确匹配，按值猜测现场随机失败）：
//!
//! | 发现的类型码 | 允许的 Tag 与规则 |
//! |---|---|
//! | C1 BOOL | 仅 `Bool` → 1 字节 0/1 |
//! | C2/C3/C4/C5 | `I64` 按宽度收窄（越界 invalid_type 禁止静默截断）；`U64` 非负且在域内可转有符号写入？**否**——符号域不匹配直接 invalid_type（设备声明有符号类型就写补码语义）|
//! | C6/C7/C8/C9 | `U64` 按宽度收窄；`I64` 负数 invalid_type |
//! | CA REAL | `F64` 无损缩窄 f32（NaN/Inf 原样位型）；整型 Tag invalid_type |
//! | CB LREAL | `F64` 直写 |
//!
//! String/Bytes/Array/Struct 一律 invalid_type。全部小端。

use observation_model::RawValue;

use crate::cip;
use crate::error::EtherIpError;

/// 按发现的类型码编码一个写值载荷。
///
/// # Errors
///
/// 类型码未知/复杂、Tag 与类型码类别不符、整数值越界、浮点无法无损
/// 缩窄时返回 `invalid_type`。
pub fn encode_write(type_code: u16, value: &RawValue) -> Result<Vec<u8>, EtherIpError> {
    match (type_code, value) {
        (cip::TYPE_BOOL, RawValue::Bool(b)) => Ok(vec![u8::from(*b)]),
        (
            code @ (cip::TYPE_SINT | cip::TYPE_INT | cip::TYPE_DINT | cip::TYPE_LINT),
            RawValue::I64(v),
        ) => {
            let width = cip::type_width(code).expect("已知类型码");
            narrow_signed(*v, width)
        }
        (
            code @ (cip::TYPE_USINT | cip::TYPE_UINT | cip::TYPE_UDINT | cip::TYPE_ULINT),
            RawValue::U64(v),
        ) => {
            let width = cip::type_width(code).expect("已知类型码");
            narrow_unsigned(*v, width)
        }
        (cip::TYPE_REAL, RawValue::F64(v)) => {
            let narrowed = *v as f32;
            if !narrowed.is_nan() && f64::from(narrowed) != *v {
                return Err(EtherIpError::invalid_type(format!(
                    "F64 值 {v} 无法无损缩窄为 REAL(f32)"
                )));
            }
            Ok(narrowed.to_le_bytes().to_vec())
        }
        (cip::TYPE_LREAL, RawValue::F64(v)) => Ok(v.to_le_bytes().to_vec()),
        (type_code, value) => {
            let name = match value {
                RawValue::Bool(_) => "Bool",
                RawValue::I64(_) => "I64",
                RawValue::U64(_) => "U64",
                RawValue::F64(_) => "F64",
                RawValue::String(_) => "String",
                RawValue::Bytes(_) => "Bytes",
                RawValue::Array(_) => "Array",
                RawValue::Struct(_) => "Struct",
            };
            Err(EtherIpError::invalid_type(format!(
                "设备类型码 {type_code:#06x} 不接受值类型 {name}（映射表见模块文档）"
            )))
        }
    }
}

/// 有符号值按目标宽度收窄并输出小端字节。
fn narrow_signed(v: i64, width: usize) -> Result<Vec<u8>, EtherIpError> {
    let fits = match width {
        1 => i8::try_from(v).is_ok(),
        2 => i16::try_from(v).is_ok(),
        4 => i32::try_from(v).is_ok(),
        _ => true,
    };
    if !fits {
        return Err(EtherIpError::invalid_type(format!(
            "值 {v} 超出 {width} 字节有符号目标域（禁止静默截断）"
        )));
    }
    Ok(v.to_le_bytes()[..width].to_vec())
}

/// 无符号值按目标宽度收窄为小端字节（越界显式失败，禁止静默截断）。
fn narrow_unsigned(v: u64, width: usize) -> Result<Vec<u8>, EtherIpError> {
    let fits = match width {
        1 => u8::try_from(v).is_ok(),
        2 => u16::try_from(v).is_ok(),
        4 => u32::try_from(v).is_ok(),
        _ => true,
    };
    if !fits {
        return Err(EtherIpError::invalid_type(format!(
            "值 {v} 超出 {width} 字节无符号目标域（禁止静默截断）"
        )));
    }
    Ok(v.to_le_bytes()[..width].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_and_int_families() {
        assert_eq!(
            encode_write(cip::TYPE_BOOL, &RawValue::Bool(true)).unwrap(),
            vec![1]
        );
        // DINT 收窄 4 字节小端。
        assert_eq!(
            encode_write(cip::TYPE_DINT, &RawValue::I64(-2)).unwrap(),
            vec![0xFE, 0xFF, 0xFF, 0xFF]
        );
        // UINT 值域检查。
        assert!(encode_write(cip::TYPE_UINT, &RawValue::U64(70_000)).is_err());
        assert_eq!(
            encode_write(cip::TYPE_UINT, &RawValue::U64(777)).unwrap(),
            vec![9, 3]
        );
    }

    #[test]
    fn tag_category_mismatches_rejected() {
        // 有符号设备类型配 U64 Tag：符号域不匹配拒绝。
        assert!(
            encode_write(cip::TYPE_DINT, &RawValue::U64(5)).is_err(),
            "DINT 不接受 U64"
        );
        assert!(
            encode_write(cip::TYPE_UINT, &RawValue::I64(-1)).is_err(),
            "UINT 不接受 I64 负数"
        );
        // REAL 配整型。
        assert!(encode_write(cip::TYPE_REAL, &RawValue::I64(1)).is_err());
        // 复杂值。
        assert!(encode_write(cip::TYPE_DINT, &RawValue::String("x".into())).is_err());
    }

    #[test]
    fn real_requires_lossless_narrowing() {
        assert_eq!(
            encode_write(cip::TYPE_REAL, &RawValue::F64(1.5)).unwrap(),
            vec![0, 0, 0xC0, 0x3F]
        );
        assert!(encode_write(cip::TYPE_REAL, &RawValue::F64(0.1)).is_err());
        // LREAL 直写 8 字节。
        assert_eq!(
            encode_write(cip::TYPE_LREAL, &RawValue::F64(0.5))
                .unwrap()
                .len(),
            8
        );
    }
}
