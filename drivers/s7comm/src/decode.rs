//! 读值解码：S7 响应载荷 → `RawValue`。
//!
//! # 类型映射表（读取侧，权威）
//!
//! 宽度由**地址语法**固定；`expected_type`（来自 Profile 的 value_type，
//! 经 `DriverReadItem.expected_type` 传入）只决定解释方式。64 位载体
//! Tag（I64/U64/F64）按语法宽度降级解释再提升回 64 位输出：
//!
//! | 语法 | 字节宽 | None 默认 | 兼容 expected_type → RawValue |
//! |---|---|---|---|
//! | 位 | 1 | `Bool` | Bool → Bool |
//! | 字节 | 1 | `U64`(u8) | U8/U64 → U64；I8/I64 → I64(i8) |
//! | 字 | 2 | `U64`(u16) | U16/U64 → U64；I16/I64 → I64(i16) |
//! | 双字 | 4 | `U64`(u32) | U32/U64 → U64；I32/I64 → I64(i32)；F32/F64 → F64(Real) |
//!
//! 不兼容组合（如位地址 + F64、字节地址 + Bool/String/复杂类型）一律
//! 单项错误：宽度不符为 `invalid_response`（结构完整性），解释不兼容为
//! `invalid_type`。

use observation_model::DataType;

use crate::address::S7Type;
use crate::error::S7Error;

/// 解码一条读响应载荷。
///
/// # Errors
///
/// 载荷长度与语法宽度不符返回 `invalid_response`（协议完整性）；期望
/// 类型不兼容返回 `invalid_type`。
pub fn decode_read(
    ty: S7Type,
    expected: Option<DataType>,
    payload: &[u8],
) -> Result<RawValueOut, S7Error> {
    if payload.len() != ty.width_bytes() as usize {
        return Err(S7Error::invalid_response(format!(
            "载荷长度 {} 与语法宽度 {} 不符",
            payload.len(),
            ty.width_bytes()
        )));
    }
    match ty {
        S7Type::Bit => match expected {
            None | Some(DataType::Bool) => Ok(RawValueOut::Bool(payload[0] & 1 != 0)),
            Some(other) => Err(incompatible(S7Type::Bit, other)),
        },
        S7Type::Byte => {
            let raw = payload[0];
            match expected {
                None | Some(DataType::U8) | Some(DataType::U64) => {
                    Ok(RawValueOut::Unsigned(u64::from(raw)))
                }
                Some(DataType::I8) | Some(DataType::I64) => {
                    Ok(RawValueOut::Signed(i64::from(raw as i8)))
                }
                Some(other) => Err(incompatible(ty, other)),
            }
        }
        S7Type::Word => {
            let raw = u16::from_be_bytes([payload[0], payload[1]]);
            match expected {
                None | Some(DataType::U16) | Some(DataType::U64) => {
                    Ok(RawValueOut::Unsigned(u64::from(raw)))
                }
                Some(DataType::I16) | Some(DataType::I64) => {
                    Ok(RawValueOut::Signed(i64::from(raw as i16)))
                }
                Some(other) => Err(incompatible(ty, other)),
            }
        }
        S7Type::Dword => {
            let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            match expected {
                None | Some(DataType::U32) | Some(DataType::U64) => {
                    Ok(RawValueOut::Unsigned(u64::from(raw)))
                }
                Some(DataType::I32) | Some(DataType::I64) => {
                    Ok(RawValueOut::Signed(i64::from(raw as i32)))
                }
                // Real(f32)：按 IEEE 位型解释并提升 f64（§7.3 RawValue 无 F32）。
                Some(DataType::F32) | Some(DataType::F64) => {
                    Ok(RawValueOut::Real(f32::from_bits(raw)))
                }
                Some(other) => Err(incompatible(ty, other)),
            }
        }
    }
}

/// 解码输出（会话层再装配为 [`observation_model::RawReadResult`]）。
#[derive(Debug, Clone, PartialEq)]
pub enum RawValueOut {
    /// 布尔（位）。
    Bool(bool),
    /// 有符号整数（i8/i16/i32 解释后提升）。
    Signed(i64),
    /// 无符号整数（u8/u16/u32 解释后提升）。
    Unsigned(u64),
    /// 实数（Real(f32) 位型）。
    Real(f32),
}

fn incompatible(ty: S7Type, expected: DataType) -> S7Error {
    S7Error::invalid_type(format!(
        "语法宽度 {ty:?} 与期望类型 {expected:?} 不兼容（映射表见模块文档）"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_decodes_bool_only() {
        assert_eq!(
            decode_read(S7Type::Bit, Some(DataType::Bool), &[1]).unwrap(),
            RawValueOut::Bool(true)
        );
        assert_eq!(
            decode_read(S7Type::Bit, None, &[0]).unwrap(),
            RawValueOut::Bool(false)
        );
        assert!(decode_read(S7Type::Bit, Some(DataType::F64), &[1]).is_err());
    }

    #[test]
    fn byte_sign_interpretation_follows_expected() {
        // 0xFF：无符号 255 / 有符号 -1。
        assert_eq!(
            decode_read(S7Type::Byte, Some(DataType::U8), &[0xFF]).unwrap(),
            RawValueOut::Unsigned(255)
        );
        assert_eq!(
            decode_read(S7Type::Byte, Some(DataType::I8), &[0xFF]).unwrap(),
            RawValueOut::Signed(-1)
        );
        assert_eq!(
            decode_read(S7Type::Byte, None, &[0x80]).unwrap(),
            RawValueOut::Unsigned(128),
            "None 默认无符号"
        );
    }

    #[test]
    fn word_dword_endianness_and_promotion() {
        assert_eq!(
            decode_read(S7Type::Word, Some(DataType::I16), &[0xFF, 0xFE]).unwrap(),
            RawValueOut::Signed(-2),
            "大端补码"
        );
        assert_eq!(
            decode_read(
                S7Type::Dword,
                Some(DataType::U64),
                &[0xDE, 0xAD, 0xBE, 0xEF]
            )
            .unwrap(),
            RawValueOut::Unsigned(0xDEAD_BEEF)
        );
        // Real：1.5f32 = 0x3FC00000。
        assert_eq!(
            decode_read(
                S7Type::Dword,
                Some(DataType::F64),
                &[0x3F, 0xC0, 0x00, 0x00]
            )
            .unwrap(),
            RawValueOut::Real(1.5)
        );
    }

    #[test]
    fn width_mismatch_is_protocol_error_and_complex_types_rejected() {
        assert!(decode_read(S7Type::Word, Some(DataType::U16), &[0x01]).is_err());
        assert!(decode_read(S7Type::Byte, Some(DataType::String), &[0]).is_err());
        assert!(
            decode_read(
                S7Type::Dword,
                Some(DataType::Array(Box::new(DataType::U8))),
                &[0; 4]
            )
            .is_err()
        );
        assert!(decode_read(S7Type::Dword, Some(DataType::Struct(vec![])), &[0; 4]).is_err());
    }
}
