//! ABI v1 类型 Tag 表与 `value_bytes` 编码（§17.2 Normative）。
//!
//! `FfiReadItem.expected_type` / `FfiWriteItem.value_type` 使用本模块的
//! 稳定 u32 Tag；`FfiWriteItem.value_bytes` 使用本模块规定的编码。
//!
//! # 编码规则（ABI v1 固定）
//!
//! - 整数：定宽 **小端序**（little-endian），宽度由 Tag 决定，字节数必须精确匹配。
//! - `Bool`：恰好 1 字节，`0x00 = false`、`0x01 = true`。
//! - `F32`：IEEE-754 binary32 小端（4 字节）；`F64`：IEEE-754 binary64 小端（8 字节）。
//! - `String`：UTF-8 字节，长度 = `FfiStr.len`，不要求 NUL 结尾。
//! - `Bytes`：原样字节。
//! - `Array`/`Struct`（复杂值）：**不允许**进入 `value_bytes`（§17.2 只定义
//!   标量编码，复杂结果统一走带 schema 的 JSON envelope；ABI v1 没有复杂
//!   写入通道，对复杂值写入返回 `ComplexTypeNotEncodable`）。复杂 Tag 仅允许
//!   作为读取时的 `expected_type` 提示。
//!
//! # 未指定类型
//!
//! `TAG_UNKNOWN = 0` 表示"未指定"，仅存在于线格式（`expected_type`）；
//! `TypeTag` 枚举本身没有 Unknown 变体。进程内转换必须使用
//! [`data_type_to_tag`] / [`tag_to_data_type`]（`Option<DataType> ↔ u32`），
//! 不要直接对 0 调用 `TypeTag::try_from`。

use std::fmt;

use observation_model::{DataType, RawValue};

/// 未指定 / 未知类型 Tag（线格式哨兵值，不是合法的 `TypeTag`）。
pub const TAG_UNKNOWN: u32 = 0;

/// ABI v1 类型 Tag（§17.2 数值映射，固定不可变更）。
///
/// 变更（增删改）属于 ABI 破坏性修改 => ABI major + 1（§17.4、§18）。
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeTag {
    Bool = 1,
    I8 = 2,
    I16 = 3,
    I32 = 4,
    I64 = 5,
    U8 = 6,
    U16 = 7,
    U32 = 8,
    U64 = 9,
    F32 = 10,
    F64 = 11,
    String = 12,
    Bytes = 13,
    /// 复杂类型（元素类型需由 Profile/校验提供）：仅用于 `expected_type`。
    Array = 14,
    /// 复杂类型（字段 schema 需由 Profile/校验提供）：仅用于 `expected_type`。
    Struct = 15,
}

impl TypeTag {
    /// 由进程内 `DataType` 映射到 ABI Tag（`Array`/`Struct` 映射为对应复杂 Tag）。
    pub fn from_data_type(data_type: DataType) -> Self {
        use DataType as D;
        match data_type {
            D::Bool => Self::Bool,
            D::I8 => Self::I8,
            D::I16 => Self::I16,
            D::I32 => Self::I32,
            D::I64 => Self::I64,
            D::U8 => Self::U8,
            D::U16 => Self::U16,
            D::U32 => Self::U32,
            D::U64 => Self::U64,
            D::F32 => Self::F32,
            D::F64 => Self::F64,
            D::String => Self::String,
            D::Bytes => Self::Bytes,
            D::Array(_) => Self::Array,
            D::Struct(_) => Self::Struct,
        }
    }

    /// 反向映射；`Array`/`Struct` 缺少元素/字段 schema 信息，返回 `None`。
    ///
    /// 需要严格语义（区分"未指定"与"复杂类型"）时使用 [`tag_to_data_type`]。
    fn to_data_type(self) -> Option<DataType> {
        Some(match self {
            Self::Bool => DataType::Bool,
            Self::I8 => DataType::I8,
            Self::I16 => DataType::I16,
            Self::I32 => DataType::I32,
            Self::I64 => DataType::I64,
            Self::U8 => DataType::U8,
            Self::U16 => DataType::U16,
            Self::U32 => DataType::U32,
            Self::U64 => DataType::U64,
            Self::F32 => DataType::F32,
            Self::F64 => DataType::F64,
            Self::String => DataType::String,
            Self::Bytes => DataType::Bytes,
            Self::Array | Self::Struct => return None,
        })
    }
}

/// `Option<DataType> ↔ u32` 转换：`None` 映射为 `TAG_UNKNOWN = 0`（§17.2）。
///
/// 读取请求未指定期望类型时使用本函数，而不是 `TypeTag::try_from(0)`。
pub fn data_type_to_tag(data_type: Option<DataType>) -> u32 {
    match data_type {
        Some(dt) => TypeTag::from_data_type(dt) as u32,
        None => TAG_UNKNOWN,
    }
}

/// `u32 → Option<DataType>` 转换（§17.2）。
///
/// - `TAG_UNKNOWN (0)` => `Ok(None)`：未指定类型，是合法输入；
/// - 标量/字符串 Tag => `Ok(Some(DataType))`；
/// - `Array`/`Struct` => `Err(ComplexTypeNeedsSchema)`：缺少元素/字段 schema，
///   无法还原为完整 `DataType`；
/// - 表外数值 => `Err(UnknownTag)`。
pub fn tag_to_data_type(tag: u32) -> Result<Option<DataType>, TagError> {
    match tag {
        TAG_UNKNOWN => Ok(None),
        _ => {
            let tag = TypeTag::try_from(tag)?;
            tag.to_data_type()
                .map(Some)
                .ok_or(TagError::ComplexTypeNeedsSchema(tag as u32))
        }
    }
}

impl TryFrom<u32> for TypeTag {
    type Error = TagError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Bool),
            2 => Ok(Self::I8),
            3 => Ok(Self::I16),
            4 => Ok(Self::I32),
            5 => Ok(Self::I64),
            6 => Ok(Self::U8),
            7 => Ok(Self::U16),
            8 => Ok(Self::U32),
            9 => Ok(Self::U64),
            10 => Ok(Self::F32),
            11 => Ok(Self::F64),
            12 => Ok(Self::String),
            13 => Ok(Self::Bytes),
            14 => Ok(Self::Array),
            15 => Ok(Self::Struct),
            _ => Err(TagError::UnknownTag(value)),
        }
    }
}

/// Tag 编解码错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagError {
    /// Tag 不在 ABI v1 表中（`TAG_UNKNOWN = 0` 不是合法 `TypeTag`）。
    UnknownTag(u32),
    /// `Array`/`Struct` Tag 缺少元素/字段 schema，无法还原为完整 `DataType`。
    ComplexTypeNeedsSchema(u32),
    /// 值变体与 Tag 类型不匹配。
    TypeMismatch { tag: u32 },
    /// `Array`/`Struct` 不允许写入 `value_bytes`（§17.2 标量编码；
    /// 复杂结果走 JSON envelope，ABI v1 无复杂写入通道）。
    ComplexTypeNotEncodable,
    /// 字节长度与 Tag 标量宽度不符。
    InvalidLength {
        tag: u32,
        expected: usize,
        actual: usize,
    },
    /// Bool 字节必须为 0 或 1。
    InvalidBool(u8),
    /// 数值超出目标 Tag 可表示范围。
    OutOfRange { tag: u32 },
    /// String 字节不是合法 UTF-8。
    InvalidUtf8,
}

impl fmt::Display for TagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTag(tag) => write!(f, "未知 ABI 类型 Tag: {tag}"),
            Self::ComplexTypeNeedsSchema(tag) => {
                write!(
                    f,
                    "Tag {tag} 为复杂类型，缺少元素/字段 schema，无法还原为 DataType"
                )
            }
            Self::TypeMismatch { tag } => write!(f, "值变体与 Tag {tag} 不匹配"),
            Self::ComplexTypeNotEncodable => write!(
                f,
                "复杂类型(Array/Struct)不允许写入 value_bytes（§17.2 标量编码，ABI v1 无复杂写入通道）"
            ),
            Self::InvalidLength {
                tag,
                expected,
                actual,
            } => {
                write!(f, "Tag {tag} 需要 {expected} 字节，实际 {actual} 字节")
            }
            Self::InvalidBool(byte) => write!(f, "Bool 字节必须为 0/1，实际 0x{byte:02x}"),
            Self::OutOfRange { tag } => write!(f, "数值超出 Tag {tag} 的范围"),
            Self::InvalidUtf8 => write!(f, "String 编码不是合法 UTF-8"),
        }
    }
}

impl std::error::Error for TagError {}

/// 校验字节长度精确等于标量宽度。
fn exact_len(tag: TypeTag, expected: usize, bytes: &[u8]) -> Result<(), TagError> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(TagError::InvalidLength {
            tag: tag as u32,
            expected,
            actual: bytes.len(),
        })
    }
}

/// 把协议原始值编码为 `value_bytes`（§17.2 标量编码，小端）。
///
/// `tag` 决定目标编码宽度；值变体必须与 Tag 匹配，超出范围返回
/// `TagError::OutOfRange`。
pub fn encode_value_bytes(tag: u32, value: &RawValue) -> Result<Vec<u8>, TagError> {
    let tag = TypeTag::try_from(tag)?;
    match (tag, value) {
        (TypeTag::Bool, RawValue::Bool(b)) => Ok(vec![*b as u8]),
        (TypeTag::Bool, _) => Err(TagError::TypeMismatch { tag: tag as u32 }),
        (TypeTag::I8, RawValue::I64(v)) => {
            let v = i8::try_from(*v).map_err(|_| TagError::OutOfRange { tag: tag as u32 })?;
            Ok(v.to_le_bytes().to_vec())
        }
        (TypeTag::I16, RawValue::I64(v)) => {
            let v = i16::try_from(*v).map_err(|_| TagError::OutOfRange { tag: tag as u32 })?;
            Ok(v.to_le_bytes().to_vec())
        }
        (TypeTag::I32, RawValue::I64(v)) => {
            let v = i32::try_from(*v).map_err(|_| TagError::OutOfRange { tag: tag as u32 })?;
            Ok(v.to_le_bytes().to_vec())
        }
        (TypeTag::I64, RawValue::I64(v)) => Ok(v.to_le_bytes().to_vec()),
        (TypeTag::U8, RawValue::U64(v)) => {
            let v = u8::try_from(*v).map_err(|_| TagError::OutOfRange { tag: tag as u32 })?;
            Ok(v.to_le_bytes().to_vec())
        }
        (TypeTag::U16, RawValue::U64(v)) => {
            let v = u16::try_from(*v).map_err(|_| TagError::OutOfRange { tag: tag as u32 })?;
            Ok(v.to_le_bytes().to_vec())
        }
        (TypeTag::U32, RawValue::U64(v)) => {
            let v = u32::try_from(*v).map_err(|_| TagError::OutOfRange { tag: tag as u32 })?;
            Ok(v.to_le_bytes().to_vec())
        }
        (TypeTag::U64, RawValue::U64(v)) => Ok(v.to_le_bytes().to_vec()),
        (TypeTag::F32, RawValue::F64(v)) => {
            // std 无 `TryFrom<f64> for f32`；溢出语义：有限值转换后变为 ±inf 视为越界。
            let v32 = *v as f32;
            if v.is_finite() && v32.is_infinite() {
                return Err(TagError::OutOfRange { tag: tag as u32 });
            }
            Ok(v32.to_le_bytes().to_vec())
        }
        (TypeTag::F64, RawValue::F64(v)) => Ok(v.to_le_bytes().to_vec()),
        (TypeTag::String, RawValue::String(s)) => Ok(s.as_bytes().to_vec()),
        (TypeTag::Bytes, RawValue::Bytes(b)) => Ok(b.clone()),
        // §17.2：value_bytes 只定义标量编码；复杂值必须走 JSON envelope，
        // ABI v1 没有复杂写入通道，直接拒绝。
        (TypeTag::Array | TypeTag::Struct, _) => Err(TagError::ComplexTypeNotEncodable),
        _ => Err(TagError::TypeMismatch { tag: tag as u32 }),
    }
}

/// 按 Tag 把 `value_bytes` 解码为协议原始值（§17.2，小端）。
///
/// `Array`/`Struct` Tag 与编码规则不兼容（无标量编码），返回
/// `TagError::ComplexTypeNotEncodable`。
pub fn decode_value_bytes(tag: u32, bytes: &[u8]) -> Result<RawValue, TagError> {
    let tag = TypeTag::try_from(tag)?;
    match tag {
        TypeTag::Bool => {
            exact_len(tag, 1, bytes)?;
            match bytes[0] {
                0 => Ok(RawValue::Bool(false)),
                1 => Ok(RawValue::Bool(true)),
                b => Err(TagError::InvalidBool(b)),
            }
        }
        TypeTag::I8 => {
            exact_len(tag, 1, bytes)?;
            Ok(RawValue::I64(bytes[0] as i8 as i64))
        }
        TypeTag::I16 => {
            exact_len(tag, 2, bytes)?;
            Ok(RawValue::I64(
                i16::from_le_bytes([bytes[0], bytes[1]]) as i64
            ))
        }
        TypeTag::I32 => {
            exact_len(tag, 4, bytes)?;
            Ok(RawValue::I64(
                i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64,
            ))
        }
        TypeTag::I64 => {
            exact_len(tag, 8, bytes)?;
            Ok(RawValue::I64(i64::from_le_bytes(
                bytes.try_into().expect("长度已校验为 8 字节"),
            )))
        }
        TypeTag::U8 => {
            exact_len(tag, 1, bytes)?;
            Ok(RawValue::U64(bytes[0] as u64))
        }
        TypeTag::U16 => {
            exact_len(tag, 2, bytes)?;
            Ok(RawValue::U64(
                u16::from_le_bytes([bytes[0], bytes[1]]) as u64
            ))
        }
        TypeTag::U32 => {
            exact_len(tag, 4, bytes)?;
            Ok(RawValue::U64(
                u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64,
            ))
        }
        TypeTag::U64 => {
            exact_len(tag, 8, bytes)?;
            Ok(RawValue::U64(u64::from_le_bytes(
                bytes.try_into().expect("长度已校验为 8 字节"),
            )))
        }
        TypeTag::F32 => {
            exact_len(tag, 4, bytes)?;
            let v = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            Ok(RawValue::F64(v as f64))
        }
        TypeTag::F64 => {
            exact_len(tag, 8, bytes)?;
            Ok(RawValue::F64(f64::from_le_bytes(
                bytes.try_into().expect("长度已校验为 8 字节"),
            )))
        }
        TypeTag::String => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(RawValue::String(s.to_owned())),
            Err(_) => Err(TagError::InvalidUtf8),
        },
        TypeTag::Bytes => Ok(RawValue::Bytes(bytes.to_vec())),
        // §17.2：value_bytes 无复杂值编码，Array/Struct Tag 拒绝解码。
        TypeTag::Array | TypeTag::Struct => Err(TagError::ComplexTypeNotEncodable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_mapping_round_trip() {
        assert_eq!(TypeTag::from_data_type(DataType::U16), TypeTag::U16);
        assert_eq!(TypeTag::from_data_type(DataType::F64), TypeTag::F64);
        assert_eq!(
            TypeTag::from_data_type(DataType::Array(Box::new(DataType::F64))),
            TypeTag::Array
        );
        assert_eq!(tag_to_data_type(7), Ok(Some(DataType::U16)));
        assert_eq!(TypeTag::try_from(7), Ok(TypeTag::U16));
        // 0 是线格式哨兵值，不是合法 TypeTag。
        assert_eq!(TypeTag::try_from(0), Err(TagError::UnknownTag(0)));
        assert_eq!(TypeTag::try_from(99), Err(TagError::UnknownTag(99)));
    }

    #[test]
    fn option_data_type_conversion_matches_unknown_semantics() {
        // None ↔ TAG_UNKNOWN(0)：无类型读取是合法输入（§17.2）。
        assert_eq!(data_type_to_tag(None), TAG_UNKNOWN);
        assert_eq!(data_type_to_tag(Some(DataType::U16)), 7);
        assert_eq!(tag_to_data_type(TAG_UNKNOWN), Ok(None));
        // Array/Struct 缺少 schema，无法还原为 DataType。
        assert_eq!(
            tag_to_data_type(TypeTag::Array as u32),
            Err(TagError::ComplexTypeNeedsSchema(TypeTag::Array as u32))
        );
        assert_eq!(tag_to_data_type(99), Err(TagError::UnknownTag(99)));
    }

    #[test]
    fn encode_u16_little_endian() {
        let bytes = encode_value_bytes(7, &RawValue::U64(5000)).expect("编码失败");
        assert_eq!(bytes, vec![0x88, 0x13]);
    }

    #[test]
    fn encode_i8_negative() {
        let bytes = encode_value_bytes(2, &RawValue::I64(-8)).expect("编码失败");
        assert_eq!(bytes, vec![0xF8]);
    }

    #[test]
    fn encode_f64_little_endian() {
        let bytes = encode_value_bytes(11, &RawValue::F64(50.0)).expect("编码失败");
        assert_eq!(bytes.len(), 8);
        assert_eq!(
            bytes,
            50.0f64.to_le_bytes().to_vec(),
            "F64 必须为 IEEE-754 binary64 小端"
        );
    }

    #[test]
    fn encode_string_and_bytes() {
        assert_eq!(
            encode_value_bytes(12, &RawValue::String("温度".to_owned())).expect("编码失败"),
            "温度".as_bytes().to_vec()
        );
        assert_eq!(
            encode_value_bytes(13, &RawValue::Bytes(vec![0x01, 0x02])).expect("编码失败"),
            vec![0x01, 0x02]
        );
    }

    #[test]
    fn decode_scalars_round_trip() {
        for (tag, value) in [
            (1, RawValue::Bool(true)),
            (2, RawValue::I64(-8)),
            (3, RawValue::I64(-16)),
            (4, RawValue::I64(-32)),
            (5, RawValue::I64(-64)),
            (6, RawValue::U64(8)),
            (7, RawValue::U64(5000)),
            (8, RawValue::U64(32)),
            (9, RawValue::U64(64)),
            (10, RawValue::F64(1.5)),
            (11, RawValue::F64(2.5)),
            (12, RawValue::String("abc".to_owned())),
            (13, RawValue::Bytes(vec![0xAA, 0xBB])),
        ] {
            let bytes = encode_value_bytes(tag, &value).expect("编码失败");
            let back = decode_value_bytes(tag, &bytes).expect("解码失败");
            assert_eq!(back, value, "Tag {tag} 编解码往返");
        }
    }

    #[test]
    fn decode_rejects_wrong_length() {
        let err = decode_value_bytes(7, &[0x01]).expect_err("U16 只需 2 字节");
        assert_eq!(
            err,
            TagError::InvalidLength {
                tag: 7,
                expected: 2,
                actual: 1
            }
        );
        let err = decode_value_bytes(11, &[]).expect_err("F64 需要 8 字节");
        assert_eq!(
            err,
            TagError::InvalidLength {
                tag: 11,
                expected: 8,
                actual: 0
            }
        );
    }

    #[test]
    fn decode_bool_validates_byte() {
        assert_eq!(
            decode_value_bytes(1, &[0x00]).expect("解码失败"),
            RawValue::Bool(false)
        );
        assert_eq!(
            decode_value_bytes(1, &[0x01]).expect("解码失败"),
            RawValue::Bool(true)
        );
        assert_eq!(
            decode_value_bytes(1, &[0x02]),
            Err(TagError::InvalidBool(0x02))
        );
    }

    #[test]
    fn encode_rejects_out_of_range() {
        let err = encode_value_bytes(6, &RawValue::U64(300)).expect_err("U8 最大 255");
        assert_eq!(err, TagError::OutOfRange { tag: 6 });
        let err = encode_value_bytes(2, &RawValue::I64(128)).expect_err("I8 最大 127");
        assert_eq!(err, TagError::OutOfRange { tag: 2 });
    }

    #[test]
    fn encode_rejects_type_mismatch() {
        let err = encode_value_bytes(7, &RawValue::I64(5)).expect_err("U16 Tag 需要 U64 值");
        assert_eq!(err, TagError::TypeMismatch { tag: 7 });
    }

    #[test]
    fn complex_values_rejected_in_value_bytes() {
        // §17.2：value_bytes 只定义标量编码，ABI v1 无复杂写入通道。
        let err =
            encode_value_bytes(14, &RawValue::Array(vec![RawValue::U64(1)])).expect_err("复杂写入");
        assert_eq!(err, TagError::ComplexTypeNotEncodable);
        let err = encode_value_bytes(15, &RawValue::Struct(vec![])).expect_err("复杂写入");
        assert_eq!(err, TagError::ComplexTypeNotEncodable);
        let err = decode_value_bytes(14, b"{\"array\":[]}").expect_err("复杂解码");
        assert_eq!(err, TagError::ComplexTypeNotEncodable);
        let err = decode_value_bytes(15, &[]).expect_err("复杂解码");
        assert_eq!(err, TagError::ComplexTypeNotEncodable);
    }

    #[test]
    fn decode_rejects_invalid_utf8() {
        let err = decode_value_bytes(12, &[0xFF, 0xFE]).expect_err("非法 UTF-8");
        assert_eq!(err, TagError::InvalidUtf8);
    }
}
