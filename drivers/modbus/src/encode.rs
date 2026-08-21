//! 写入值编码：`RawValue` → 线圈位 / 寄存器大端字节（读侧 [`crate::decode`] 的逆变换）。
//!
//! # 寄存器编码（镜像读侧约定）
//!
//! 寄存器内字节序恒为大端（高字节在前）；多寄存器（32/64 位）的字序由配置
//! `word_order` 决定，与读侧共用同一字置换（字交换为对合变换，逆变换即自身）：
//!
//! ```text
//! Abcd（默认）：值的高字写第一个寄存器
//! Cdab：       值的低字写第一个寄存器
//! ```
//!
//! # 宽度决定规则（写入没有 expected_type，宽度来自 ABI Tag + 值本身）
//!
//! 写请求项携带 ABI v1 值类型 Tag（§17.2）与按 Tag 编码的标量值：
//!
//! - **窄 Tag 精确遵守**：`U8/I8/U16/I16` → 1 寄存器、`U32/I32/F32` → 2 寄存器。
//!   调用方显式给出协议宽度时不得擅自收窄（如 U32 写小值也必须清零高字）；
//! - **载体 Tag 按值收窄**：进程内 `RawValue` 只有 64 位变体，经 Loader 通路
//!   所有整数都以 `U64/I64` Tag 到达、浮点以 `F64` 到达——Tag 无法区分
//!   "属性真是 64 位"与"仅作载体"。此时按值的**最小无损宽度**写入，
//!   避免把单寄存器设定值放大成多寄存器写而污染相邻寄存器：
//!   整数取 u16/i16 → 1、u32/i32 → 2、否则 4 个寄存器；
//!   浮点可无损缩窄为 f32 时按 F32（2 寄存器）写，否则按 F64（4 寄存器）。
//!   （Profile 的 F32 属性写入值本就是 f32 无损提升的 f64，恰好落在 F32 分支。）
//!
//! 位段（coil）只接受 Bool；寄存器段不接受 Bool/String/Bytes（与读侧解码
//! 规则一一对应），返回单项错误。

use driver_sdk::abi::tag::TypeTag;
use observation_model::RawValue;

use crate::address::RegisterKind;
use crate::config::WordOrder;
use crate::decode;

/// 单项写入编码错误（不影响同批次其他 item）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError {
    /// 编码失败原因（类型不兼容等）。
    pub message: String,
}

impl EncodeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// 一个写项的编码结果。
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedWrite {
    /// 单个线圈位（FC05 的值或 FC15 位流中的一个位）。
    Coil(bool),
    /// 寄存器数据：大端字节、已按 `word_order` 排字，长度 = 寄存器数 × 2。
    Registers(Vec<u8>),
}

impl EncodedWrite {
    /// 占用的单元数：线圈恒 1 位；寄存器为 1/2/4 个寄存器。
    pub fn units(&self) -> u16 {
        match self {
            Self::Coil(_) => 1,
            Self::Registers(data) => (data.len() / 2) as u16,
        }
    }
}

/// 线圈 ON/OFF 的协议线值（Modbus 规定 ON = 0xFF00，其余非法）。
pub fn coil_payload(on: bool) -> [u8; 2] {
    if on { [0xFF, 0x00] } else { [0x00, 0x00] }
}

/// FC15 位流打包：LSB 优先（地址号小者对应低字节低位，镜像读侧 `decode_bit`）。
pub fn pack_bits(bits: &[bool]) -> Vec<u8> {
    let mut bytes = vec![0u8; bits.len().div_ceil(8)];
    for (index, bit) in bits.iter().enumerate() {
        if *bit {
            bytes[index / 8] |= 1 << (index % 8);
        }
    }
    bytes
}

/// 把一个写项的值编码为线数据。
///
/// `value_type` 为 ABI v1 Tag（§17.2），决定显式协议宽度（窄 Tag）或触发
/// 载体收窄（U64/I64/F64）；`word_order` 只作用于多寄存器整数/浮点。
///
/// # Errors
///
/// 位段出现非 Bool、寄存器段出现 Bool/String/Bytes、Tag 非法时返回
/// 单项 [`EncodeError`]。
pub fn encode_write_value(
    kind: RegisterKind,
    value_type: u32,
    value: &RawValue,
    word_order: WordOrder,
) -> Result<EncodedWrite, EncodeError> {
    match kind {
        RegisterKind::Coil => match value {
            RawValue::Bool(bit) => Ok(EncodedWrite::Coil(*bit)),
            other => Err(EncodeError::new(format!(
                "位段只支持 Bool 类型，收到 {other:?}"
            ))),
        },
        RegisterKind::HoldingRegister => {
            let data = encode_register_data(value_type, value, word_order)?;
            Ok(EncodedWrite::Registers(data))
        }
        // 不可写段由规划阶段整体拒绝（invalid_address），此处防御性兜底。
        RegisterKind::DiscreteInput | RegisterKind::InputRegister => {
            Err(EncodeError::new(format!("数据段 {kind:?} 只读，不可写入")))
        }
    }
}

/// 编码保持寄存器写数据（大端 + 字序置换）。
fn encode_register_data(
    value_type: u32,
    value: &RawValue,
    word_order: WordOrder,
) -> Result<Vec<u8>, EncodeError> {
    let tag = TypeTag::try_from(value_type)
        .map_err(|e| EncodeError::new(format!("写入值类型 Tag 非法：{e}")))?;

    let bytes: Vec<u8> = match (tag, value) {
        // ---- 整数：窄 Tag 精确遵守；载体 Tag（U64/I64）按值最小宽度收窄。
        (TypeTag::U64, RawValue::U64(v)) => unsigned_be(*v, minimal_unsigned_words(*v)),
        (TypeTag::I64, RawValue::I64(v)) => signed_be(*v, minimal_signed_words(*v)),
        (TypeTag::U8, RawValue::U64(v)) => {
            // 镜像读侧：U8 取寄存器低字节，高字节补 0。
            vec![0x00, *v as u8]
        }
        (TypeTag::I8, RawValue::I64(v)) => vec![0x00, *v as i8 as u8],
        (TypeTag::U16, RawValue::U64(v)) => (*v as u16).to_be_bytes().to_vec(),
        (TypeTag::I16, RawValue::I64(v)) => (*v as i16).to_be_bytes().to_vec(),
        (TypeTag::U32, RawValue::U64(v)) => (*v as u32).to_be_bytes().to_vec(),
        (TypeTag::I32, RawValue::I64(v)) => (*v as i32).to_be_bytes().to_vec(),
        // ---- 浮点：F32 Tag 显式 2 寄存器；F64 载体可无损缩窄时按 F32。
        (TypeTag::F32, RawValue::F64(v)) => (*v as f32).to_be_bytes().to_vec(),
        (TypeTag::F64, RawValue::F64(v)) => {
            if f32_lossless(*v) {
                (*v as f32).to_be_bytes().to_vec()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        // Bool/String/Bytes 不参与寄存器写入（与读侧解码规则一致）。
        (TypeTag::Bool, _) => {
            return Err(EncodeError::new("寄存器段不支持 Bool 类型，请改用位段地址"));
        }
        // 值变体与 Tag 不匹配（正常通路 decode_value_bytes 已保证一致，防御性兜底）。
        (tag, _) => {
            return Err(EncodeError::new(format!(
                "值变体与写入类型 Tag {tag:?} 不匹配，无法编码为寄存器数据"
            )));
        }
    };

    // 单寄存器（2 字节）无需字序；多寄存器按 word_order 排字后返回。
    Ok(match bytes.len() {
        2 => bytes,
        4 => decode::reorder_words4(&bytes, word_order).to_vec(),
        8 => decode::reorder_words8(&bytes, word_order).to_vec(),
        len => {
            return Err(EncodeError::new(format!(
                "寄存器编码长度异常（{len} 字节）"
            )));
        }
    })
}

/// 无符号整数的自然大端字节（按最小宽度收窄后的字节数）。
fn unsigned_be(v: u64, words: u16) -> Vec<u8> {
    match words {
        1 => (v as u16).to_be_bytes().to_vec(),
        2 => (v as u32).to_be_bytes().to_vec(),
        _ => v.to_be_bytes().to_vec(),
    }
}

/// 有符号整数的自然大端字节（按最小宽度收窄后的字节数）。
fn signed_be(v: i64, words: u16) -> Vec<u8> {
    match words {
        1 => (v as i16).to_be_bytes().to_vec(),
        2 => (v as i32).to_be_bytes().to_vec(),
        _ => v.to_be_bytes().to_vec(),
    }
}

/// 载体 Tag 下无符号值的最小寄存器宽度。
fn minimal_unsigned_words(v: u64) -> u16 {
    if v <= u16::MAX as u64 {
        1
    } else if v <= u32::MAX as u64 {
        2
    } else {
        4
    }
}

/// 载体 Tag 下有符号值的最小寄存器宽度。
fn minimal_signed_words(v: i64) -> u16 {
    if (i16::MIN as i64..=i16::MAX as i64).contains(&v) {
        1
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
        2
    } else {
        4
    }
}

/// f64 是否可无损缩窄为 f32（NaN 不可比较，恒按 F64 全宽写）。
fn f32_lossless(v: f64) -> bool {
    !v.is_nan() && {
        let narrowed = v as f32;
        // 无穷大双向均可精确表示（f32::INFINITY as f64 == INFINITY）。
        narrowed as f64 == v
    }
}

#[cfg(test)]
mod tests {
    use observation_model::DataType;

    use super::*;

    #[test]
    fn coil_payload_on_off() {
        assert_eq!(coil_payload(true), [0xFF, 0x00]);
        assert_eq!(coil_payload(false), [0x00, 0x00]);
    }

    #[test]
    fn packs_bits_lsb_first() {
        // 镜像 decode_bit：地址号小者对应低字节低位。
        assert_eq!(pack_bits(&[true, false, true]), vec![0b101]);
        assert_eq!(pack_bits(&[false]), vec![0x00]);
        assert_eq!(
            pack_bits(&[true; 9]),
            vec![0xFF, 0x01],
            "第 9 位落入第二字节最低位"
        );
    }

    #[test]
    fn encodes_u16_single_register() {
        // 载体 U64 收窄：5000 -> 1 寄存器大端。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::U64 as u32,
            &RawValue::U64(5000),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded.units(), 1);
        assert_eq!(encoded, EncodedWrite::Registers(vec![0x13, 0x88]));
    }

    #[test]
    fn narrows_u64_by_value_range() {
        let cases: [(u64, u16, Vec<u8>); 3] = [
            (70_000, 2, vec![0x00, 0x01, 0x11, 0x70]), // 需要 2 寄存器
            (5_000, 1, vec![0x13, 0x88]),              // 1 寄存器足够
            (
                u64::from(u32::MAX) + 1,
                4,
                vec![0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
        ];
        for (value, units, data) in cases {
            let encoded = encode_write_value(
                RegisterKind::HoldingRegister,
                TypeTag::U64 as u32,
                &RawValue::U64(value),
                WordOrder::Abcd,
            )
            .unwrap();
            assert_eq!(encoded.units(), units, "value {value}");
            assert_eq!(encoded, EncodedWrite::Registers(data), "value {value}");
        }
    }

    #[test]
    fn narrows_i64_by_value_range_with_sign() {
        // -10 落入 i16 -> 1 寄存器补码。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::I64 as u32,
            &RawValue::I64(-10),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded, EncodedWrite::Registers(vec![0xFF, 0xF6]));
        // -100_000 超出 i16 -> 2 寄存器（i32 补码 0xFFFE7960）。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::I64 as u32,
            &RawValue::I64(-100_000),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded.units(), 2);
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0xFF, 0xFE, 0x79, 0x60])
        );
        // i64::MIN 需要 4 寄存器。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::I64 as u32,
            &RawValue::I64(i64::MIN),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded.units(), 4);
    }

    #[test]
    fn narrow_tags_are_honored_exactly() {
        // 显式 U32 Tag 即使值很小也必须写满 2 寄存器（清零高字是写入语义的一部分）。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::U32 as u32,
            &RawValue::U64(5),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded.units(), 2);
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0x00, 0x00, 0x00, 0x05])
        );
        // I16 Tag 负值 1 寄存器。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::I16 as u32,
            &RawValue::I64(-2),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded, EncodedWrite::Registers(vec![0xFF, 0xFE]));
        // U8/I8 镜像读侧：值在低字节，高字节补 0。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::U8 as u32,
            &RawValue::U64(0x34),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded, EncodedWrite::Registers(vec![0x00, 0x34]));
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::I8 as u32,
            &RawValue::I64(-8),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded, EncodedWrite::Registers(vec![0x00, 0xF8]));
    }

    #[test]
    fn encodes_u32_word_orders() {
        let value = RawValue::U64(0x0001_0203);
        // Abcd：高字在前（镜像读侧 reorder_words4(Abcd) 恒等）。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::U64 as u32,
            &value,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0x00, 0x01, 0x02, 0x03])
        );
        // Cdab：低字在前（读侧 Cdab 解码的精确逆）。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::U64 as u32,
            &value,
            WordOrder::Cdab,
        )
        .unwrap();
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0x02, 0x03, 0x00, 0x01])
        );
    }

    #[test]
    fn encodes_f64_four_registers_cdab_reversed() {
        // 2.6 无法无损缩窄为 f32，走 F64 全宽：0x4004CCCCCCCCCCCD；
        // Cdab 反转字顺序后高字 0x4004 落在第 4 寄存器（镜像读侧测试）。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::F64 as u32,
            &RawValue::F64(2.6),
            WordOrder::Cdab,
        )
        .unwrap();
        assert_eq!(encoded.units(), 4);
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0xCC, 0xCD, 0xCC, 0xCC, 0xCC, 0xCC, 0x40, 0x04])
        );
    }

    #[test]
    fn narrows_f64_to_f32_when_lossless() {
        // Profile 的 F32 属性写入值为 f32 无损提升：50.0 -> F32 两寄存器。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::F64 as u32,
            &RawValue::F64(50.0),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0x42, 0x48, 0x00, 0x00])
        );
        // 0.1 无法无损缩窄 -> F64 四寄存器全宽。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::F64 as u32,
            &RawValue::F64(0.1),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded.units(), 4);
        assert_eq!(
            encoded,
            EncodedWrite::Registers(0.1f64.to_be_bytes().to_vec())
        );
        // NaN 不可比较，恒按 F64 全宽。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::F64 as u32,
            &RawValue::F64(f64::NAN),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(encoded.units(), 4);
        // 显式 F32 Tag 恒 2 寄存器。
        let encoded = encode_write_value(
            RegisterKind::HoldingRegister,
            TypeTag::F32 as u32,
            &RawValue::F64(1.5),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(
            encoded,
            EncodedWrite::Registers(vec![0x3F, 0xC0, 0x00, 0x00])
        );
    }

    #[test]
    fn coil_accepts_only_bool() {
        let ok = encode_write_value(
            RegisterKind::Coil,
            TypeTag::Bool as u32,
            &RawValue::Bool(true),
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(ok, EncodedWrite::Coil(true));
        let err = encode_write_value(
            RegisterKind::Coil,
            TypeTag::U64 as u32,
            &RawValue::U64(1),
            WordOrder::Abcd,
        )
        .unwrap_err();
        assert!(err.message.contains("只支持 Bool"));
    }

    #[test]
    fn register_segment_rejects_bool_string_bytes() {
        for (tag, value) in [
            (TypeTag::Bool as u32, RawValue::Bool(true)),
            (TypeTag::String as u32, RawValue::String("x".to_owned())),
            (TypeTag::Bytes as u32, RawValue::Bytes(vec![1])),
        ] {
            let err =
                encode_write_value(RegisterKind::HoldingRegister, tag, &value, WordOrder::Abcd)
                    .unwrap_err();
            assert!(!err.message.is_empty(), "tag {tag}");
        }
    }

    #[test]
    fn rejects_unknown_tag() {
        let err = encode_write_value(
            RegisterKind::HoldingRegister,
            99,
            &RawValue::U64(1),
            WordOrder::Abcd,
        )
        .unwrap_err();
        assert!(err.message.contains("Tag 非法"));
    }

    #[test]
    fn read_write_round_trip_mirrors_decode() {
        // 写侧编码必须能被读侧按同类型/字序还原（镜像对称性）。
        for (tag, value, words) in [
            (TypeTag::U64 as u32, RawValue::U64(5000), DataType::U16),
            (TypeTag::I64 as u32, RawValue::I64(-10), DataType::I16),
            (TypeTag::U64 as u32, RawValue::U64(70_000), DataType::U32),
            (TypeTag::I64 as u32, RawValue::I64(-100_000), DataType::I32),
            (TypeTag::F64 as u32, RawValue::F64(50.0), DataType::F32),
            (TypeTag::F64 as u32, RawValue::F64(2.6), DataType::F64),
            (TypeTag::U64 as u32, RawValue::U64(u64::MAX), DataType::U64),
        ] {
            for order in [WordOrder::Abcd, WordOrder::Cdab] {
                let encoded =
                    encode_write_value(RegisterKind::HoldingRegister, tag, &value, order).unwrap();
                let EncodedWrite::Registers(data) = encoded else {
                    panic!("寄存器段期望 Registers");
                };
                let decoded = crate::decode::decode_register_value(
                    RegisterKind::HoldingRegister,
                    &data,
                    0,
                    Some(&words),
                    order,
                )
                .unwrap_or_else(|e| panic!("{words:?}/{order:?} 解码失败：{e:?}"));
                assert_eq!(decoded, value, "{words:?}/{order:?} 读写往返");
            }
        }
    }
}
