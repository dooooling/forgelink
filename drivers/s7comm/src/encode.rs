//! 写值编码：`RawValue` → S7 Write Var 数据项载荷。
//!
//! # 类型映射表（写入侧，权威）
//!
//! 目标宽度由**地址语法后缀**固定；`RawValue` 的 Tag 决定值域解释：
//!
//! | 目标语法 | 允许的 Tag 与规则 | transport size / length |
//! |---|---|---|
//! | 位（dbx / m0.1 等） | 仅 `Bool` → 1 字节 0/1 | TS_BIT(0x01) / 1（位） |
//! | 字节（dbb / mb…） | `I64` ∈ [-128,127]、`U64` ∈ [0,255]，大端 1B | TS_BYTE(0x03) / 1 |
//! | 字（dbw / mw…） | `I64` ∈ [-32768,32767]、`U64` ∈ [0,65535]，大端 2B | TS_WORD(0x04) / 1（字） |
//! | 双字（dbd / md…） | `I64` ∈ [i32 范围]、`U64` ∈ [u32 范围]，大端 4B；`F64` 须可无损缩窄 f32（NaN/Inf 按 IEEE 位型原样编码） | TS_DWORD(0x06) / 1（双字） |
//!
//! 越界/类型不兼容一律单项 `invalid_type`，**禁止静默截断**；S7 Classic
//! 无 64 位实数，`F64` 写双字地址按 Real(f32) 语义缩窄。

use observation_model::RawValue;

use crate::address::S7Type;
use crate::error::S7Error;
use crate::pdu::{TS_BIT, TS_BYTE, TS_DWORD, TS_WORD};

/// 编码后的写数据项。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedWrite {
    /// transport size（BIT/BYTE/WORD/DWORD）。
    pub transport_size: u8,
    /// Any length（元素数：BIT=1 位、BYTE=1 字节、WORD=1 字、DWORD=1 双字）。
    pub length: u16,
    /// 大端载荷（不含对齐填充）。
    pub payload: Vec<u8>,
}

/// 编码一个写值。
///
/// # Errors
///
/// Tag 与目标语法不兼容、整数值越界、浮点无法无损缩窄时返回
/// `invalid_type`。
pub fn encode_write(ty: S7Type, value: &RawValue) -> Result<EncodedWrite, S7Error> {
    match (ty, value) {
        (S7Type::Bit, RawValue::Bool(b)) => Ok(EncodedWrite {
            transport_size: TS_BIT,
            length: 1,
            payload: vec![u8::from(*b)],
        }),
        (S7Type::Byte, RawValue::I64(v)) => {
            let Ok(v) = i8::try_from(*v) else {
                return Err(out_of_range("字节", *v));
            };
            Ok(byte_write(v.to_be_bytes()))
        }
        (S7Type::Byte, RawValue::U64(v)) => {
            let Ok(v) = u8::try_from(*v) else {
                return Err(out_of_range("字节", v));
            };
            Ok(byte_write([v]))
        }
        (S7Type::Word, RawValue::I64(v)) => {
            let Ok(v) = i16::try_from(*v) else {
                return Err(out_of_range("字", *v));
            };
            Ok(word_write(v.to_be_bytes()))
        }
        (S7Type::Word, RawValue::U64(v)) => {
            let Ok(v) = u16::try_from(*v) else {
                return Err(out_of_range("字", v));
            };
            Ok(word_write(v.to_be_bytes()))
        }
        (S7Type::Dword, RawValue::I64(v)) => {
            let Ok(v) = i32::try_from(*v) else {
                return Err(out_of_range("双字", *v));
            };
            Ok(dword_write(v.to_be_bytes()))
        }
        (S7Type::Dword, RawValue::U64(v)) => {
            let Ok(v) = u32::try_from(*v) else {
                return Err(out_of_range("双字", v));
            };
            Ok(dword_write(v.to_be_bytes()))
        }
        // F64 写双字地址：按 Real(f32) 语义无损缩窄（NaN/Inf 原样位型）。
        (S7Type::Dword, RawValue::F64(v)) => {
            let narrowed = *v as f32;
            if !narrowed.is_nan() && f64::from(narrowed) != *v {
                return Err(S7Error::invalid_type(format!(
                    "F64 值 {v} 无法无损缩窄为 f32（S7 Classic 无 LReal）"
                )));
            }
            Ok(dword_write(narrowed.to_be_bytes()))
        }
        (ty, value) => {
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
            Err(S7Error::invalid_type(format!(
                "目标语法 {ty:?} 不接受值类型 {name}（映射表见模块文档）"
            )))
        }
    }
}

fn out_of_range(width: &str, v: impl std::fmt::Display) -> S7Error {
    S7Error::invalid_type(format!("值 {v} 超出{width}目标可表示范围（禁止静默截断）"))
}

fn byte_write(bytes: [u8; 1]) -> EncodedWrite {
    EncodedWrite {
        transport_size: TS_BYTE,
        length: 1,
        payload: bytes.to_vec(),
    }
}

fn word_write(bytes: [u8; 2]) -> EncodedWrite {
    EncodedWrite {
        transport_size: TS_WORD,
        length: 1,
        payload: bytes.to_vec(),
    }
}

fn dword_write(bytes: [u8; 4]) -> EncodedWrite {
    EncodedWrite {
        transport_size: TS_DWORD,
        length: 1,
        payload: bytes.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_accepts_only_bool() {
        let e = encode_write(S7Type::Bit, &RawValue::Bool(true)).unwrap();
        assert_eq!(
            (e.transport_size, e.length, e.payload.as_slice()),
            (TS_BIT, 1, &[1][..])
        );
        assert!(encode_write(S7Type::Bit, &RawValue::I64(1)).is_err());
    }

    #[test]
    fn integer_ranges_enforced_by_tag_sign() {
        // 字目标：i16 边界。
        assert!(encode_write(S7Type::Word, &RawValue::I64(-32768)).is_ok());
        assert!(encode_write(S7Type::Word, &RawValue::I64(32767)).is_ok());
        assert!(encode_write(S7Type::Word, &RawValue::I64(32768)).is_err());
        // u16 Tag：65535 合法、-1 非法。
        assert_eq!(
            encode_write(S7Type::Word, &RawValue::U64(65535))
                .unwrap()
                .payload,
            vec![0xFF, 0xFF]
        );
        assert!(encode_write(S7Type::Word, &RawValue::U64(u64::MAX)).is_err());
        // 字节目标：-128 合法（补码）。
        assert_eq!(
            encode_write(S7Type::Byte, &RawValue::I64(-128))
                .unwrap()
                .payload,
            vec![0x80]
        );
    }

    #[test]
    fn f64_requires_lossless_f32_narrowing() {
        assert!(encode_write(S7Type::Dword, &RawValue::F64(1.5)).is_ok());
        assert_eq!(
            encode_write(S7Type::Dword, &RawValue::F64(0.1)).err(),
            Some(S7Error::invalid_type(
                "F64 值 0.1 无法无损缩窄为 f32（S7 Classic 无 LReal）".to_owned()
            ))
        );
        // NaN 原样编码（IEEE 位型）。
        let nan = encode_write(S7Type::Dword, &RawValue::F64(f64::NAN)).unwrap();
        assert_eq!(nan.payload.len(), 4);
    }
}
