//! 响应数据解码：寄存器/位字节 → `RawValue`（按 `expected_type`）。
//!
//! Modbus 寄存器数据默认 **大端序**（高字节在前）。数值类型占寄存器数：
//!
//! ```text
//! U8 / I8               1 寄存器（取低字节）
//! U16 / I16             1 寄存器
//! U32 / I32 / F32       2 寄存器
//! U64 / I64 / F64       4 寄存器
//! ```
//!
//! 多寄存器（32/64 位）的字序由配置 `word_order` 决定（见 [`WordOrder`]）：
//! 默认 `Abcd`（高字在前），`Cdab`（低字在前）用于 CD/AB 约定设备。
//! coil / discrete（位操作）按协议 LSB 优先逐位展开：地址号小者对应低字节低位。
//! `String` / `Bytes` / `Array` / `Struct` 不参与解码（无长度语义），返回解码错误。

use observation_model::{DataType, RawValue};

use crate::address::RegisterKind;
use crate::config::WordOrder;

/// 解码错误（单项失败，不影响同批次其他 item）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    /// 解码失败原因（如类型不兼容、数据不足、寄存器段出现 Bool）。
    pub message: String,
}

/// 期望类型的寄存器宽度（1/2/4）；位类型（coil/discrete）恒为 1 位。
pub fn register_width(data_type: &DataType) -> Option<u16> {
    match data_type {
        DataType::U8 | DataType::I8 | DataType::U16 | DataType::I16 => Some(1),
        DataType::U32 | DataType::I32 | DataType::F32 => Some(2),
        DataType::U64 | DataType::I64 | DataType::F64 => Some(4),
        DataType::Bool => Some(1),
        DataType::String | DataType::Bytes | DataType::Array(_) | DataType::Struct(_) => None,
    }
}

/// 从位数据（LSB 优先）解码布尔值。
///
/// `bit_offset` 为 0-based 位偏移（相对响应数据起始）。
pub fn decode_bit(bytes: &[u8], bit_offset: usize) -> Result<bool, DecodeError> {
    let byte = bytes.get(bit_offset / 8).ok_or_else(|| DecodeError {
        message: format!("位偏移 {bit_offset} 超出响应数据长度 {}", bytes.len()),
    })?;
    Ok((byte >> (bit_offset % 8)) & 1 != 0)
}

/// 从寄存器数据（大端，每寄存器 2 字节）解码一个标量值。
///
/// `reg_offset` 为 0-based 寄存器偏移（相对响应数据起始）。
/// `word_order` 决定多寄存器数值的字序（见 [`WordOrder`]）。
pub fn decode_register_value(
    kind: RegisterKind,
    bytes: &[u8],
    reg_offset: u16,
    data_type: Option<&DataType>,
    word_order: WordOrder,
) -> Result<RawValue, DecodeError> {
    let data_type = match data_type {
        Some(dt) => dt.clone(),
        // 未指定类型时按段默认：位段 Bool，寄存器段 U16。
        None => {
            return match kind {
                RegisterKind::Coil | RegisterKind::DiscreteInput => {
                    decode_bit(bytes, reg_offset as usize).map(RawValue::Bool)
                }
                RegisterKind::HoldingRegister | RegisterKind::InputRegister => {
                    decode_u16(bytes, reg_offset).map(|v| RawValue::U64(v as u64))
                }
            };
        }
    };

    if kind == RegisterKind::Coil || kind == RegisterKind::DiscreteInput {
        if data_type != DataType::Bool {
            return Err(DecodeError {
                message: format!("位段只支持 Bool 类型，收到 {data_type:?}"),
            });
        }
        return decode_bit(bytes, reg_offset as usize).map(RawValue::Bool);
    }

    // 寄存器段不支持 Bool：布尔语义只存在于位段（coil/discrete）。
    // 这里必须返回明确错误而不能 panic（unreachable 会被 ABI 边界误报为
    // DRIVER_PANIC，掩盖配置错误的真实原因）。
    if data_type == DataType::Bool {
        return Err(DecodeError {
            message: format!("寄存器段（{kind:?}）不支持 Bool 类型，请改用位段地址"),
        });
    }

    let words = register_width(&data_type).ok_or_else(|| DecodeError {
        message: format!("类型 {data_type:?} 不参与 Modbus 寄存器解码"),
    })?;
    let byte_start = reg_offset as usize * 2;
    let byte_end = byte_start + words as usize * 2;
    if bytes.len() < byte_end {
        return Err(DecodeError {
            message: format!(
                "值需要 {words} 个寄存器（字节 {byte_start}..{byte_end}），响应只有 {} 字节",
                bytes.len()
            ),
        });
    }
    let slice = &bytes[byte_start..byte_end];
    let value = match data_type {
        DataType::U8 => RawValue::U64(slice[1] as u64),
        DataType::I8 => RawValue::I64(slice[1] as i8 as i64),
        DataType::U16 => decode_u16(bytes, reg_offset).map(|v| RawValue::U64(v as u64))?,
        DataType::I16 => RawValue::I64(i16::from_be_bytes([slice[0], slice[1]]) as i64),
        DataType::U32 => {
            let w = reorder_words4(slice, word_order);
            RawValue::U64(u32::from_be_bytes(w) as u64)
        }
        DataType::I32 => {
            let w = reorder_words4(slice, word_order);
            RawValue::I64(i32::from_be_bytes(w) as i64)
        }
        DataType::F32 => {
            let w = reorder_words4(slice, word_order);
            RawValue::F64(f32::from_be_bytes(w) as f64)
        }
        DataType::U64 => {
            let w = reorder_words8(slice, word_order);
            RawValue::U64(u64::from_be_bytes(w))
        }
        DataType::I64 => {
            let w = reorder_words8(slice, word_order);
            RawValue::I64(i64::from_be_bytes(w))
        }
        DataType::F64 => {
            let w = reorder_words8(slice, word_order);
            RawValue::F64(f64::from_be_bytes(w))
        }
        DataType::Bool => unreachable!("位段分支已处理"),
        DataType::String | DataType::Bytes | DataType::Array(_) | DataType::Struct(_) => {
            return Err(DecodeError {
                message: format!("类型 {data_type:?} 不参与 Modbus 寄存器解码"),
            });
        }
    };
    Ok(value)
}

/// 按字序重排 2 寄存器（4 字节）：`Cdab` 交换高低字。
fn reorder_words4(slice: &[u8], word_order: WordOrder) -> [u8; 4] {
    match word_order {
        WordOrder::Abcd => [slice[0], slice[1], slice[2], slice[3]],
        WordOrder::Cdab => [slice[2], slice[3], slice[0], slice[1]],
    }
}

/// 按字序重排 4 寄存器（8 字节）：`Cdab` 反转字顺序。
fn reorder_words8(slice: &[u8], word_order: WordOrder) -> [u8; 8] {
    match word_order {
        WordOrder::Abcd => [
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ],
        WordOrder::Cdab => [
            slice[6], slice[7], slice[4], slice[5], slice[2], slice[3], slice[0], slice[1],
        ],
    }
}

fn decode_u16(bytes: &[u8], reg_offset: u16) -> Result<u16, DecodeError> {
    let byte_start = reg_offset as usize * 2;
    let byte_end = byte_start + 2;
    if bytes.len() < byte_end {
        return Err(DecodeError {
            message: format!(
                "值需要 1 个寄存器（字节 {byte_start}..{byte_end}），响应只有 {} 字节",
                bytes.len()
            ),
        });
    }
    Ok(u16::from_be_bytes([
        bytes[byte_start],
        bytes[byte_start + 1],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use observation_model::RawValue;

    #[test]
    fn decodes_u16_big_endian() {
        let bytes = [0x13, 0x88, 0x00, 0x0B];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::U16),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::U64(5000));
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            1,
            Some(&DataType::U16),
            WordOrder::Cdab,
        )
        .unwrap();
        assert_eq!(value, RawValue::U64(11));
    }

    #[test]
    fn decodes_i16() {
        let bytes = [0xFF, 0xF6];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::I16),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::I64(-10));
    }

    #[test]
    fn decodes_u32_two_registers() {
        let bytes = [0x00, 0x01, 0x02, 0x03];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::U32),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::U64(0x00010203));
    }

    #[test]
    fn decodes_f32() {
        // 1.5f32 = 0x3FC00000。
        let bytes = [0x3F, 0xC0, 0x00, 0x00];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::F32),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::F64(1.5));
    }

    #[test]
    fn decodes_f32_cdab_low_word_first() {
        // CD/AB 设备（如 40003=0x3EFA、40004=0x42C6，值为 99.123）。
        let bytes = [0x3E, 0xFA, 0x42, 0xC6];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::F32),
            WordOrder::Cdab,
        )
        .unwrap();
        let RawValue::F64(value) = value else {
            panic!("Cdab 解码期望 F64");
        };
        assert!((value - 99.123).abs() < 1e-5, "期望 ≈99.123，得到 {value}");
        // 同一数据按 Abcd 解码得到 0.4888（字序错误的典型症状）。
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::F32),
            WordOrder::Abcd,
        )
        .unwrap();
        let RawValue::F64(backwards) = value else {
            panic!("Abcd 解码期望 F64，得到 {value:?}");
        };
        assert!((backwards - 0.4888).abs() < 1e-4);
    }

    #[test]
    fn decodes_f64_four_registers() {
        // 2.5f64 = 0x4004000000000000。
        let bytes = [0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::F64),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::F64(2.5));
    }

    #[test]
    fn decodes_f64_cdab_words_reversed() {
        // 2.5f64 = 0x4004000000000000，CD/AB 时寄存器为 [0x0000, 0x0000, 0x0000, 0x4004]。
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x04];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::F64),
            WordOrder::Cdab,
        )
        .unwrap();
        assert_eq!(value, RawValue::F64(2.5));
    }

    #[test]
    fn decodes_i64() {
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::I64),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::I64(-2));
    }

    #[test]
    fn decodes_u8_low_byte() {
        let bytes = [0x12, 0x34];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            Some(&DataType::U8),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::U64(0x34));
    }

    #[test]
    fn decodes_bits_lsb_first() {
        // 位 0..=7：字节低位的位序（LSB 优先，地址号小者低位）。
        let bytes = [0b1000_0001, 0b0000_0001];
        let value = decode_register_value(
            RegisterKind::Coil,
            &bytes,
            0,
            Some(&DataType::Bool),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::Bool(true));
        let value = decode_register_value(
            RegisterKind::Coil,
            &bytes,
            1,
            Some(&DataType::Bool),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::Bool(false));
        let value = decode_register_value(
            RegisterKind::Coil,
            &bytes,
            8,
            Some(&DataType::Bool),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::Bool(true));
    }

    #[test]
    fn defaults_when_type_unspecified() {
        let bytes = [0x13, 0x88];
        let value = decode_register_value(
            RegisterKind::HoldingRegister,
            &bytes,
            0,
            None,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(value, RawValue::U64(5000));
        let bytes = [0b1010_0000];
        let value =
            decode_register_value(RegisterKind::Coil, &bytes, 0, None, WordOrder::Abcd).unwrap();
        assert_eq!(value, RawValue::Bool(false));
    }

    #[test]
    fn bit_segment_rejects_register_types() {
        let err = decode_register_value(
            RegisterKind::Coil,
            &[0x00],
            0,
            Some(&DataType::U16),
            WordOrder::Abcd,
        )
        .unwrap_err();
        assert!(err.message.contains("只支持 Bool"));
    }

    #[test]
    fn rejects_out_of_range_offset() {
        assert!(
            decode_register_value(
                RegisterKind::HoldingRegister,
                &[0x00, 0x01],
                5,
                None,
                WordOrder::Abcd
            )
            .is_err()
        );
        assert!(
            decode_register_value(RegisterKind::Coil, &[0x00], 8, None, WordOrder::Abcd).is_err()
        );
    }

    #[test]
    fn rejects_unsupported_types() {
        assert!(
            decode_register_value(
                RegisterKind::HoldingRegister,
                &[0x00, 0x00],
                0,
                Some(&DataType::String),
                WordOrder::Abcd,
            )
            .is_err()
        );
        assert!(
            decode_register_value(
                RegisterKind::HoldingRegister,
                &[0x00, 0x00],
                0,
                Some(&DataType::Bytes),
                WordOrder::Abcd,
            )
            .is_err()
        );
    }
}
