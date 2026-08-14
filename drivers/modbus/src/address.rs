//! Modbus 地址解析与校验（Driver 私有不透明数据，§10）。
//!
//! # 地址格式（本 Driver 定义）
//!
//! 支持以下等价形式，全部为 **1-based 传统 Modbus 地址号**：
//!
//! ```text
//! 40001              仅数据地址（unit 取连接配置默认值）
//! coil:00001         显式数据段 + 地址号（coil/discrete/input/holding）
//! 1!40001            unit + 数据地址
//! 2!holding:40001    unit + 显式数据段 + 地址号
//! ```
//!
//! 数据段与数字地址号的映射（Modicon 传统约定）：
//!
//! ```text
//! 0xxxx（1..=9999）      -> coil（FC01 Read Coils）
//! 1xxxx（10001..=19999） -> discrete input（FC02 Read Discrete Inputs）
//! 3xxxx（30001..=39999） -> input register（FC04 Read Input Registers）
//! 4xxxx（40001..=49999） -> holding register（FC03 Read Holding Registers）
//! ```
//!
//! 地址号换算协议偏移：`offset = address - segment_base`（如 `40001 -> offset 0`）。
//! 显式段名形式不受 5 位数字段限制，允许协议 16 位偏移全范围
//! （如 `holding:65537`），仅受数据段上限约束。
//!
//! 规范化形式（`validate_address` / 去重）：`{unit}!{kind}:{address}`，
//! 如 `1!holding:40001`、`3!coil:1`。

use std::fmt;

use serde::{Deserialize, Serialize};

/// Modbus 数据段（决定功能码与读写属性）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisterKind {
    /// 线圈：FC01 读 / FC05、FC15 写。
    Coil,
    /// 离散输入：FC02 只读。
    DiscreteInput,
    /// 输入寄存器：FC04 只读。
    InputRegister,
    /// 保持寄存器：FC03 读 / FC06、FC16 写。
    HoldingRegister,
}

impl RegisterKind {
    /// 读功能码（FC01/FC02/FC03/FC04）。
    pub fn function_code(self) -> u8 {
        match self {
            Self::Coil => 0x01,
            Self::DiscreteInput => 0x02,
            Self::HoldingRegister => 0x03,
            Self::InputRegister => 0x04,
        }
    }

    /// 传统数字段基数（`40001 -> offset 0` 即基数 40001）。
    fn segment_base(self) -> u32 {
        match self {
            Self::Coil => 1,
            Self::DiscreteInput => 10_001,
            Self::InputRegister => 30_001,
            Self::HoldingRegister => 40_001,
        }
    }

    /// 显式段名地址号上限（协议 16 位偏移全范围）。
    fn explicit_max(self) -> u32 {
        self.segment_base() + 65_535
    }

    /// 数字地址号是否属于本段（传统 5 位数字段范围）。
    fn matches_numeric(self, address: u32) -> bool {
        let base = self.segment_base();
        address >= base && address < base + 9_999
    }

    /// 是否可写（coil / holding 可写，discrete / input 只读）。
    pub fn writable(self) -> bool {
        matches!(self, Self::Coil | Self::HoldingRegister)
    }

    /// 段名（`coil` / `discrete` / `input` / `holding`，地址字符串用）。
    pub fn name(self) -> &'static str {
        match self {
            Self::Coil => "coil",
            Self::DiscreteInput => "discrete",
            Self::InputRegister => "input",
            Self::HoldingRegister => "holding",
        }
    }
}

/// 解析后的 Modbus 地址。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModbusAddress {
    /// 从站地址（1..=247，Modbus 协议范围）。
    pub unit_id: u8,
    /// 数据段。
    pub kind: RegisterKind,
    /// 1-based 传统地址号（coil: 1..=65536；holding: 40001..=105536）。
    pub address: u32,
}

impl ModbusAddress {
    /// 协议偏移（0-based）：`40001 -> 0`。
    pub fn offset(self) -> u16 {
        (self.address - self.kind.segment_base()) as u16
    }

    /// 规范化形式 `{unit}!{kind}:{address}`（去重与调试用）。
    pub fn canonical(self) -> String {
        format!("{}!{}:{}", self.unit_id, self.kind.name(), self.address)
    }
}

impl fmt::Display for ModbusAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical())
    }
}

/// 地址解析错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressError {
    /// 地址号超出协议范围（16 位偏移）。
    OutOfRange(String),
    /// 数字地址不在任何已知数据段（2xxxx/5xxxx 等）。
    UnknownSegment(String),
    /// 语法无法解析。
    InvalidSyntax(String),
    /// 从站地址越界（Modbus 只允许 1..=247）。
    InvalidUnit(u8),
}

impl fmt::Display for AddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange(a) => write!(f, "地址 {a} 超出协议偏移范围"),
            Self::UnknownSegment(a) => write!(
                f,
                "地址 {a} 不在任何已知数据段（coil/discrete/input/holding）"
            ),
            Self::InvalidSyntax(a) => write!(f, "地址 {a:?} 语法无法解析"),
            Self::InvalidUnit(u) => write!(f, "从站地址 {u} 越界（Modbus 只允许 1..=247）"),
        }
    }
}

/// 解析地址字符串。
///
/// 支持 `40001`、`coil:00001`、`1!40001`、`2!holding:40001` 四种形式；
/// 未显式给出 unit 时使用 `default_unit_id`。
pub fn parse_address(input: &str, default_unit_id: u8) -> Result<ModbusAddress, AddressError> {
    if default_unit_id == 0 || default_unit_id > 247 {
        return Err(AddressError::InvalidUnit(default_unit_id));
    }
    let (unit, rest) = match input.split_once('!') {
        Some((u, rest)) => {
            let unit: u8 = u
                .trim()
                .parse()
                .map_err(|_| AddressError::InvalidSyntax(input.to_owned()))?;
            (unit, rest)
        }
        None => (default_unit_id, input),
    };
    if unit == 0 || unit > 247 {
        return Err(AddressError::InvalidUnit(unit));
    }
    let (kind, number) = match rest.split_once(':') {
        Some((name, number)) => {
            let kind = match name.trim() {
                "coil" => RegisterKind::Coil,
                "discrete" => RegisterKind::DiscreteInput,
                "input" => RegisterKind::InputRegister,
                "holding" => RegisterKind::HoldingRegister,
                other => return Err(AddressError::InvalidSyntax(format!("未知数据段 {other:?}"))),
            };
            let number: u32 = number
                .trim()
                .parse()
                .map_err(|_| AddressError::InvalidSyntax(input.to_owned()))?;
            (kind, number)
        }
        None => {
            // 纯数字：按传统数字段推断数据段。
            let number: u32 = rest
                .trim()
                .parse()
                .map_err(|_| AddressError::InvalidSyntax(input.to_owned()))?;
            let kind = infer_kind_from_numeric(number)?;
            (kind, number)
        }
    };
    validate(kind, number).map(|address| ModbusAddress {
        unit_id: unit,
        ..address
    })
}

/// 由传统数字地址号推断数据段（0/1/3/4 前缀段）。
fn infer_kind_from_numeric(number: u32) -> Result<RegisterKind, AddressError> {
    for kind in [
        RegisterKind::Coil,
        RegisterKind::DiscreteInput,
        RegisterKind::InputRegister,
        RegisterKind::HoldingRegister,
    ] {
        if kind.matches_numeric(number) {
            return Ok(kind);
        }
    }
    Err(AddressError::UnknownSegment(number.to_string()))
}

/// 校验地址号范围并构造 `ModbusAddress`（不含 unit）。
fn validate(kind: RegisterKind, number: u32) -> Result<ModbusAddress, AddressError> {
    // 数字段形式已由 matches_numeric 限定（传统 5 位数字段）；
    // 显式段名形式允许协议 16 位偏移全范围。
    if number == 0 || number > kind.explicit_max() {
        return Err(AddressError::OutOfRange(number.to_string()));
    }
    Ok(ModbusAddress {
        unit_id: 1,
        kind,
        address: number,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_numeric_with_unit() {
        let addr = parse_address("1!40001", 1).unwrap();
        assert_eq!(addr.unit_id, 1);
        assert_eq!(addr.kind, RegisterKind::HoldingRegister);
        assert_eq!(addr.address, 40_001);
        assert_eq!(addr.offset(), 0);
        assert_eq!(addr.canonical(), "1!holding:40001");
    }

    #[test]
    fn parses_numeric_without_unit_uses_default() {
        let addr = parse_address("30001", 5).unwrap();
        assert_eq!(addr.unit_id, 5);
        assert_eq!(addr.kind, RegisterKind::InputRegister);
        assert_eq!(addr.address, 30_001);
        assert_eq!(addr.offset(), 0);
    }

    #[test]
    fn parses_kind_with_prefix_zeroes() {
        let addr = parse_address("coil:00001", 1).unwrap();
        assert_eq!(addr.kind, RegisterKind::Coil);
        assert_eq!(addr.address, 1);
        assert_eq!(addr.offset(), 0);
    }

    #[test]
    fn parses_kind_and_unit_combined() {
        let addr = parse_address("2!holding:40002", 1).unwrap();
        assert_eq!(addr.unit_id, 2);
        assert_eq!(addr.kind, RegisterKind::HoldingRegister);
        assert_eq!(addr.offset(), 1);
        assert_eq!(addr.canonical(), "2!holding:40002");
    }

    #[test]
    fn parses_numeric_segments() {
        assert_eq!(parse_address("1", 1).unwrap().kind, RegisterKind::Coil);
        assert_eq!(parse_address("9999", 1).unwrap().kind, RegisterKind::Coil);
        assert_eq!(
            parse_address("10001", 1).unwrap().kind,
            RegisterKind::DiscreteInput
        );
        assert_eq!(
            parse_address("19999", 1).unwrap().kind,
            RegisterKind::DiscreteInput
        );
        assert_eq!(
            parse_address("30001", 1).unwrap().kind,
            RegisterKind::InputRegister
        );
        assert_eq!(
            parse_address("40001", 1).unwrap().kind,
            RegisterKind::HoldingRegister
        );
        assert_eq!(
            parse_address("49999", 1).unwrap().kind,
            RegisterKind::HoldingRegister
        );
    }

    #[test]
    fn rejects_unknown_numeric_segment() {
        assert_eq!(
            parse_address("20001", 1),
            Err(AddressError::UnknownSegment("20001".to_owned()))
        );
        assert_eq!(
            parse_address("50000", 1),
            Err(AddressError::UnknownSegment("50000".to_owned()))
        );
    }

    #[test]
    fn rejects_zero_and_out_of_range() {
        assert!(parse_address("0", 1).is_err());
        assert!(parse_address("coil:0", 1).is_err());
        assert!(parse_address("holding:0", 1).is_err());
        // 显式段名允许协议偏移全范围：65536 是合法地址号（offset 65535）。
        assert!(parse_address("coil:65536", 1).is_ok());
        assert_eq!(parse_address("coil:65536", 1).unwrap().offset(), 65_535);
        assert!(parse_address("coil:65537", 1).is_err());
    }

    #[test]
    fn rejects_invalid_syntax() {
        assert!(parse_address("", 1).is_err());
        assert!(parse_address("abc", 1).is_err());
        assert!(parse_address("1!abc", 1).is_err());
        assert!(parse_address("coil:x", 1).is_err());
        assert!(parse_address("1!x:40001", 1).is_err());
        assert!(parse_address("1!holding", 1).is_err());
    }

    #[test]
    fn rejects_invalid_unit() {
        assert_eq!(
            parse_address("0!40001", 1),
            Err(AddressError::InvalidUnit(0))
        );
        assert_eq!(
            parse_address("248!40001", 1),
            Err(AddressError::InvalidUnit(248))
        );
        assert_eq!(
            parse_address("x!40001", 1),
            Err(AddressError::InvalidSyntax("x!40001".to_owned()))
        );
    }

    #[test]
    fn writable_matches_segment() {
        assert!(RegisterKind::Coil.writable());
        assert!(RegisterKind::HoldingRegister.writable());
        assert!(!RegisterKind::DiscreteInput.writable());
        assert!(!RegisterKind::InputRegister.writable());
    }

    #[test]
    fn function_codes() {
        assert_eq!(RegisterKind::Coil.function_code(), 0x01);
        assert_eq!(RegisterKind::DiscreteInput.function_code(), 0x02);
        assert_eq!(RegisterKind::HoldingRegister.function_code(), 0x03);
        assert_eq!(RegisterKind::InputRegister.function_code(), 0x04);
    }
}
