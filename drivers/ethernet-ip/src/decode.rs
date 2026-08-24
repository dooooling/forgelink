//! 读值解码：CIP 应答载荷 → `RawValue`。
//!
//! # 类型映射表（读取侧，权威）
//!
//! 与 S7 的本质差异：**宽度与基础编码由设备应答的类型码承载**（协议
//! 自带类型信息），expected_type 只决定解释方式与兼容性把关：
//!
//! | 类型码 | 宽 | None 默认 | 兼容 expected_type → RawValue |
//! |---|---|---|---|
//! | C1 BOOL | 1 | `Bool` | 仅 Bool → Bool |
//! | C2 SINT | 1 | Signed(i8) | I8/I64 → I64；U8/U64 → U64 |
//! | C6 USINT | 1 | Unsigned(u8) | U8/U64 → U64；I8/I64 → I64 |
//! | C3 INT / C7 UINT | 2 | 同上模式（i16/u16） | 同宽符号互解释提升 |
//! | C4 DINT / C8 UDINT | 4 | 同上（i32/u32） | 同上 |
//! | C5 LINT / C9 ULINT | 8 | i64/u64 | 直接输出；ULINT > i64::MAX 配 I64 → invalid_type |
//! | CA REAL | 4 | f32→f64 提升 | 仅 F32/F64 → F64 |
//! | CB LREAL | 8 | f64 | 仅 F64 → F64 |
//!
//! 关键规则：**同宽异符号允许互解释**（expected 决定解释，镜像 S7 精神）；
//! **跨宽/跨类别（整型↔浮点）一律 invalid_type**——设备已明确断言类型，
//! 重解释只会掩盖 Profile 配置错误。复杂类型码（A0–A3 结构体/数组占位、
//! 未知码）逐项拒绝——Fragmented Read 缺失下大数组/结构体整读在此显式
//! 失败而非静默截断。全部小端。

use observation_model::{DataType, RawValue};

use crate::cip;
use crate::error::EtherIpError;

/// 解码一条读应答。
///
/// # Errors
///
/// 载荷长度与类型宽不符返回 `invalid_response`；类型码未知/复杂或与
/// expected_type 不兼容返回 `invalid_type`。
pub fn decode_read(
    type_code: u16,
    expected: Option<DataType>,
    payload: &[u8],
) -> Result<RawValue, EtherIpError> {
    let Some(width) = cip::type_width(type_code) else {
        return Err(EtherIpError::invalid_type(format!(
            "CIP 类型码 {type_code:#06x} 为复杂或未知类型（V0.3 面向标量点位）"
        )));
    };
    if payload.len() != width {
        return Err(EtherIpError::invalid_response(format!(
            "载荷长度 {} 与类型宽 {width} 不符",
            payload.len()
        )));
    }
    // 小端字节序拷贝到定长缓冲（未覆盖的高位字节必须清零——短类型
    // 的 from_le_bytes 会读到残留）。
    let mut buf = [0u8; 8];
    buf[..width].copy_from_slice(payload);

    match type_code {
        cip::TYPE_BOOL => match expected {
            None | Some(DataType::Bool) => Ok(RawValue::Bool(buf[0] & 1 != 0)),
            Some(other) => Err(incompatible(type_code, other)),
        },
        cip::TYPE_SINT | cip::TYPE_USINT => signed_or_unsigned(type_code, expected, width, &buf),
        cip::TYPE_INT | cip::TYPE_UINT => signed_or_unsigned(type_code, expected, width, &buf),
        cip::TYPE_DINT | cip::TYPE_UDINT => signed_or_unsigned(type_code, expected, width, &buf),
        cip::TYPE_LINT | cip::TYPE_ULINT => {
            let raw = u64::from_le_bytes(buf);
            let is_i64_domain = matches!(expected.as_ref(), None | Some(DataType::I64));
            let is_u64_expected = matches!(expected.as_ref(), Some(DataType::U64) | None);
            if !is_i64_domain && !is_u64_expected {
                return Err(incompatible(type_code, expected.unwrap_or(DataType::Bool)));
            }
            if type_code == cip::TYPE_LINT {
                Ok(RawValue::I64(raw as i64))
            } else if is_i64_domain && raw > i64::MAX as u64 {
                // ULINT 超出 I64 正域且期望 I64：显式失败不回绕。
                Err(EtherIpError::invalid_type(format!(
                    "ULINT 值 {raw} 超出 I64 正域且期望为 I64"
                )))
            } else {
                Ok(RawValue::U64(raw))
            }
        }
        cip::TYPE_REAL => match expected {
            Some(DataType::F32) | Some(DataType::F64) | None => {
                Ok(RawValue::F64(f64::from(f32::from_le_bytes([
                    buf[0], buf[1], buf[2], buf[3],
                ]))))
            }
            Some(other) => Err(incompatible(type_code, other)),
        },
        cip::TYPE_LREAL => match expected {
            Some(DataType::F64) | None => Ok(RawValue::F64(f64::from_le_bytes(buf))),
            Some(other) => Err(incompatible(type_code, other)),
        },
        _ => Err(EtherIpError::invalid_type(format!(
            "CIP 类型码 {type_code:#06x} 未支持"
        ))),
    }
}

/// 同宽整型族的有/无符号互解释（SINT↔USINT、INT↔UINT、DINT↔UDINT）。
fn signed_or_unsigned(
    type_code: u16,
    expected: Option<DataType>,
    width: usize,
    buf: &[u8; 8],
) -> Result<RawValue, EtherIpError> {
    // 设备声明的符号方向决定 None 默认；expected 的同宽 Tag 决定重解释
    // ——**期望 Tag 的宽度必须等于设备宽度**（INT 配 U32 期望是 Profile
    // 配置错误，fail-loud 不静默降级）。
    let declared_signed = matches!(type_code, cip::TYPE_SINT | cip::TYPE_INT | cip::TYPE_DINT);
    let want_signed = match expected {
        None => declared_signed,
        // I64/U64 为 64 位载体 Tag（§17.2）：按设备宽度降级解释再提升
        // 输出——与任何整型设备宽度都兼容。
        Some(DataType::I64) => true,
        Some(DataType::U64) => false,
        // 精确宽度 Tag：宽度必须与设备声明一致（INT 配 U32 是 Profile
        // 配置错误，fail-loud）。
        Some(dt @ (DataType::I32 | DataType::I16 | DataType::I8)) => {
            if int_type_width(&dt) != Some(width) {
                return Err(incompatible(type_code, dt));
            }
            true
        }
        Some(dt @ (DataType::U32 | DataType::U16 | DataType::U8)) => {
            if int_type_width(&dt) != Some(width) {
                return Err(incompatible(type_code, dt));
            }
            false
        }
        Some(other) => return Err(incompatible(type_code, other)),
    };
    match (want_signed, width) {
        (true, 1) => Ok(RawValue::I64(i64::from(buf[0] as i8))),
        (true, 2) => Ok(RawValue::I64(i64::from(i16::from_le_bytes([
            buf[0], buf[1],
        ])))),
        (true, 4) => Ok(RawValue::I64(i64::from(i32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ])))),
        (true, 8) => Ok(RawValue::I64(i64::from_le_bytes(*buf))),
        (false, 1) => Ok(RawValue::U64(u64::from(buf[0]))),
        (false, 2) => Ok(RawValue::U64(u64::from(u16::from_le_bytes([
            buf[0], buf[1],
        ])))),
        (false, 4) => Ok(RawValue::U64(u64::from(u32::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
        ])))),
        (false, 8) => Ok(RawValue::U64(u64::from_le_bytes(*buf))),
        _ => Err(incompatible(type_code, DataType::U8)),
    }
}

/// ULINT 超出 i64 域且期望 I64 时显式失败（不静默回绕）。
fn incompatible(type_code: u16, expected: DataType) -> EtherIpError {
    EtherIpError::invalid_type(format!(
        "CIP 类型码 {type_code:#06x} 与期望类型 {expected:?} 不兼容（映射表见模块文档）"
    ))
}

/// 整型 DataType 的字节宽度（非整型返回 None）。
fn int_type_width(dt: &DataType) -> Option<usize> {
    match dt {
        DataType::I8 | DataType::U8 => Some(1),
        DataType::I16 | DataType::U16 => Some(2),
        DataType::I32 | DataType::U32 => Some(4),
        DataType::I64 | DataType::U64 => Some(8),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_accepts_only_bool() {
        assert_eq!(
            decode_read(cip::TYPE_BOOL, Some(DataType::Bool), &[1]).unwrap(),
            RawValue::Bool(true)
        );
        assert_eq!(
            decode_read(cip::TYPE_BOOL, None, &[0]).unwrap(),
            RawValue::Bool(false)
        );
        assert!(decode_read(cip::TYPE_BOOL, Some(DataType::F64), &[1]).is_err());
    }

    #[test]
    fn same_width_sign_reinterpretation_allowed() {
        // UINT(LE [0xFF,0xFE]) = 0xFEFF：无符号 65279 / 有符号 -257。
        assert_eq!(
            decode_read(cip::TYPE_UINT, Some(DataType::U64), &[0xFF, 0xFE]).unwrap(),
            RawValue::U64(65_279)
        );
        assert_eq!(
            decode_read(cip::TYPE_UINT, Some(DataType::I16), &[0xFF, 0xFE]).unwrap(),
            RawValue::I64(-257)
        );
        assert_eq!(
            decode_read(cip::TYPE_INT, None, &[0xFF, 0xFE]).unwrap(),
            RawValue::I64(-257),
            "None 按设备声明符号"
        );
        assert_eq!(
            decode_read(cip::TYPE_USINT, Some(DataType::U64), &[0x80]).unwrap(),
            RawValue::U64(128)
        );
    }

    #[test]
    fn cross_width_or_category_rejected() {
        // INT 应答配 U32 期望（跨宽）：Profile 配置错误必须暴露。
        assert!(decode_read(cip::TYPE_INT, Some(DataType::U32), &[1, 0]).is_err());
        // REAL 应答配整型期望（跨类别）。
        assert!(decode_read(cip::TYPE_REAL, Some(DataType::I32), &[0; 4]).is_err());
        // DINT 应答配浮点期望。
        assert!(decode_read(cip::TYPE_DINT, Some(DataType::F64), &[0; 4]).is_err());
    }

    #[test]
    fn real_promotes_to_f64_little_endian() {
        // 1.5f32 LE = [0,0,0xC0,0x3F]。
        assert_eq!(
            decode_read(cip::TYPE_REAL, Some(DataType::F64), &[0, 0, 0xC0, 0x3F]).unwrap(),
            RawValue::F64(1.5)
        );
    }

    #[test]
    fn complex_and_unknown_types_rejected() {
        assert!(decode_read(0xA0, None, &[]).is_err(), "结构体占位拒绝");
        assert!(decode_read(0xC1 + 0x100, None, &[1]).is_err(), "未知码拒绝");
        // 载荷宽度不符 = 结构完整性错误。
        assert!(decode_read(cip::TYPE_DINT, None, &[1, 2]).is_err());
    }

    #[test]
    fn ulint_overflow_guarded() {
        // ULINT 最大值配 I64 期望：超出正域必须显式失败而非回绕。
        let max_le = u64::MAX.to_le_bytes();
        let err = decode_read(cip::TYPE_ULINT, Some(DataType::I64), &max_le).unwrap_err();
        assert!(err.message.contains("超出"), "{err:?}");
        // ULINT 小值按 U64 正常输出。
        assert_eq!(
            decode_read(cip::TYPE_ULINT, None, &5u64.to_le_bytes()).unwrap(),
            RawValue::U64(5)
        );
    }
}
