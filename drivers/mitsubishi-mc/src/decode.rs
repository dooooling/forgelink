//! 读值解码：MC 应答载荷 → `RawValue`。
//!
//! # 类型×点数换算表（读取侧，权威）
//!
//! 宽度与点数由 **expected_type 决定**（镜像 modbus 期望类型定寄存器数
//! 的做法）；字软元件数据全小端：
//!
//! | 软元件类别 | expected_type | 点数 | RawValue |
//! |---|---|---|---|
//! | 位（X/Y/M/B/S/SM） | Bool / None | 1 | Bool |
//! | 位 | 非 Bool | — | invalid_type |
//! | 字（D/W/R/ZR/SD） | U8/U16/I8/I16/None | 1 | U64/I64（U8/I8 按符号提升） |
//! | 字 | U32/I32/F32 | 2 | U64/I64/F64 |
//! | 字 | U64/I64/F64 | 4 | U64/I64/F64 |
//! | 字 | Bool/String/Bytes/Array/Struct | — | invalid_type |
//!
//! None 在位软元件按 Bool、在字软元件按 U16 解释。F32 按 f32 位型提升
//! f64 输出（§7.3 RawValue 无 F32）。

use observation_model::{DataType, RawValue};

use crate::address::DeviceKind;
use crate::error::McError;

/// 解码一条读取项载荷（从计划数据区按 offset 切出的字节）。
///
/// # Errors
///
/// 载荷长度与类型点数不符返回 `invalid_response`；expected_type 与软
/// 元件类别不兼容返回 `invalid_type`。
/// 解码一条读取项载荷（从计划数据区按 offset 切出的字节）。
///
/// # Errors
///
/// 载荷长度与类型点数不符返回 `invalid_response`；expected_type 与软
/// 元件类别不兼容返回 `invalid_type`。
pub fn decode_read(
    kind: DeviceKind,
    expected: Option<DataType>,
    payload: &[u8],
) -> Result<RawValue, McError> {
    decode_read_ref(kind, expected.as_ref(), payload)
}

/// 借用版解码（内部复用）。
fn decode_read_ref(
    kind: DeviceKind,
    expected: Option<&DataType>,
    payload: &[u8],
) -> Result<RawValue, McError> {
    if kind.is_bit() {
        return decode_bit(expected, payload);
    }
    let (points, signed, float) = word_layout(expected)?;
    if payload.len() != points * 2 {
        return Err(McError::invalid_response(format!(
            "载荷长度 {} 与类型点数 {points} 不符",
            payload.len()
        )));
    }
    // 小端拷贝到定长缓冲（高位清零防残留）。
    let mut buf = [0u8; 8];
    buf[..payload.len()].copy_from_slice(payload);
    match (points, signed, float) {
        (1, false, false) => Ok(RawValue::U64(u64::from(u16::from_le_bytes([
            buf[0], buf[1],
        ])))),
        (1, true, false) => Ok(RawValue::I64(i64::from(i16::from_le_bytes([
            buf[0], buf[1],
        ])))),
        (2, false, false) => Ok(RawValue::U64(u64::from(u32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ])))),
        (2, true, false) => Ok(RawValue::I64(i64::from(i32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ])))),
        (2, false, true) => Ok(RawValue::F64(f64::from(f32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ])))),
        (4, false, false) => Ok(RawValue::U64(u64::from_le_bytes(buf))),
        (4, true, false) => Ok(RawValue::I64(i64::from_le_bytes(buf))),
        (4, false, true) => Ok(RawValue::F64(f64::from_le_bytes(buf))),
        _ => Err(McError::invalid_type(format!(
            "期望类型 {expected:?} 无对应解码布局"
        ))),
    }
}

/// 该类型的访问点数与（有符号, 浮点）解释标志；None 默认 1 点无符号。
///
/// # Errors
///
/// 类型不支持返回 `invalid_type`。
pub fn word_layout(expected: Option<&DataType>) -> Result<(usize, bool, bool), McError> {
    let layout = match expected {
        None => (1, false, false),
        Some(DataType::U8 | DataType::U16) => (1, false, false),
        Some(DataType::I8 | DataType::I16) => (1, true, false),
        Some(DataType::U32) => (2, false, false),
        Some(DataType::I32) => (2, true, false),
        Some(DataType::F32) => (2, false, true),
        Some(DataType::U64) => (4, false, false),
        Some(DataType::I64) => (4, true, false),
        Some(DataType::F64) => (4, false, true),
        Some(other) => {
            return Err(McError::invalid_type(format!(
                "字软元件不接受期望类型 {other:?}（映射表见模块文档）"
            )));
        }
    };
    Ok(layout)
}

/// 位软元件解码：单字节取 LSB。
fn decode_bit(expected: Option<&DataType>, payload: &[u8]) -> Result<RawValue, McError> {
    match expected {
        None | Some(DataType::Bool) => {}
        Some(other) => {
            return Err(McError::invalid_type(format!(
                "位软元件不接受期望类型 {other:?}（仅 Bool）"
            )));
        }
    }
    if payload.is_empty() {
        return Err(McError::invalid_response("位载荷为空".to_owned()));
    }
    Ok(RawValue::Bool(payload[0] & 1 != 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_decodes_bool_only() {
        assert_eq!(
            decode_read(DeviceKind::M, None, &[1]).unwrap(),
            RawValue::Bool(true)
        );
        assert_eq!(
            decode_read(DeviceKind::Y, Some(DataType::Bool), &[0]).unwrap(),
            RawValue::Bool(false)
        );
        assert!(decode_read(DeviceKind::M, Some(DataType::U16), &[1]).is_err());
        assert!(decode_read(DeviceKind::X, Some(DataType::Bool), &[]).is_err());
    }

    #[test]
    fn word_layouts_and_endianness() {
        // D 寄存器 LE [0x34,0x12] = 0x1234 = 4660。
        assert_eq!(
            decode_read(DeviceKind::D, None, &[0x34, 0x12]).unwrap(),
            RawValue::U64(4660)
        );
        // I16 补码 -2。
        assert_eq!(
            decode_read(DeviceKind::D, Some(DataType::I16), &[0xFE, 0xFF]).unwrap(),
            RawValue::I64(-2)
        );
        // DINT 32 位。
        assert_eq!(
            decode_read(
                DeviceKind::D,
                Some(DataType::I32),
                &[0xFE, 0xFF, 0xFF, 0xFF]
            )
            .unwrap(),
            RawValue::I64(-2)
        );
        // F32 提升：1.5f32 LE = [0,0,0xC0,0x3F]。
        assert_eq!(
            decode_read(DeviceKind::W, Some(DataType::F32), &[0, 0, 0xC0, 0x3F]).unwrap(),
            RawValue::F64(1.5)
        );
        // F64 直读 4 字 = 8 字节。
        let v = 0.5f64.to_le_bytes();
        assert_eq!(
            decode_read(DeviceKind::Zr, Some(DataType::F64), &v).unwrap(),
            RawValue::F64(0.5)
        );
    }

    #[test]
    fn width_mismatch_rejected() {
        assert!(decode_read(DeviceKind::D, Some(DataType::U32), &[1, 0]).is_err());
        assert!(decode_read(DeviceKind::D, Some(DataType::String), &[1, 0]).is_err());
    }
}
