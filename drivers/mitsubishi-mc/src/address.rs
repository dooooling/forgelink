//! 软元件地址解析。
//!
//! # 文法
//!
//! ```text
//! addr := {软元件前缀}{十进制编号}
//! ```
//!
//! 例：`D200`、`M100`、`X20`、`W0`、`ZR100`、`SM400`。前缀大小写不敏感；
//! canonical = 大写前缀 + trim 后文本（编号十进制无歧义，无需变换）。
//!
//! # 编号基数决策（权威）
//!
//! **一律按十进制解析**。MELSEC 中 X/Y/B/W 按 HEX 书写、D/M 按十进制是
//! GX Works 的显示约定而非协议要求——混合基数是现场第一陷阱。本驱动
//! 把换算挡在驱动内：Profile 层永远只有十进制一种心智模型。错误信息
//! 显式提示「编号按十进制解析」。
//!
//! # V0.3 支持子集
//!
//! 位软元件：X（只读）/ Y / M / B / S / SM（只读）；
//! 字软元件：D / W / R / ZR / SD（只读）。
//!
//! T/C 推迟 V0.4（触点/线圈/当前值三套子代码混位/字语义需 Profile 配合
/// 区分）；`.` 位偏移语法（如 `D200.3`）属随机访问语义，显式拒绝。
use crate::error::McError;

/// 软元件种类与访问单位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeviceKind {
    /// 输入继电器 X（位，只读；编号协议层按 HEX 编码——本驱动入口恒十进制）。
    X,
    /// 输出继电器 Y（位）。
    Y,
    /// 内部继电器 M（位）。
    M,
    /// 锁存继电器 B（位）。
    B,
    /// 步进继电器 S（位）。
    S,
    /// 特殊继电器 SM（位，只读）。
    Sm,
    /// 数据寄存器 D（字）。
    D,
    /// 链接寄存器 W（字）。
    W,
    /// 文件寄存器 R（字）。
    R,
    /// 扩展文件寄存器 ZR（字）。
    Zr,
    /// 特殊寄存器 SD（字，只读）。
    Sd,
}

impl DeviceKind {
    /// 3E 帧软元件代码（出处：《MELSEC SLMP 参考手册》SH-081948ENG，
    /// Phase 0 核对表；golden 单测与 mc-mock 交叉固化）。
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::X => 0x9C,
            Self::Y => 0x9D,
            Self::M => 0x90,
            Self::B => 0xA0,
            Self::S => 0x98,
            Self::Sm => 0x91,
            Self::D => 0xA8,
            Self::W => 0xB4,
            Self::R => 0xAF,
            Self::Zr => 0xB0,
            Self::Sd => 0xA9,
        }
    }

    /// 访问单位：位软元件按点、字软元件按字（16 位）。
    #[must_use]
    pub const fn is_bit(self) -> bool {
        matches!(
            self,
            Self::X | Self::Y | Self::M | Self::B | Self::S | Self::Sm
        )
    }

    /// 是否可写（X/SM 为过程输入与系统区，只读）。
    #[must_use]
    pub const fn writable(self) -> bool {
        !matches!(self, Self::X | Self::Sm)
    }

    /// 从前缀字母解析种类（大小写不敏感；多字母仅 SM/SD/ZR 合法）。
    fn parse_prefix(prefix: &str) -> Option<Self> {
        Some(match prefix.to_ascii_uppercase().as_str() {
            "X" => Self::X,
            "Y" => Self::Y,
            "M" => Self::M,
            "B" => Self::B,
            "S" => Self::S,
            "D" => Self::D,
            "W" => Self::W,
            "R" => Self::R,
            "SM" => Self::Sm,
            "SD" => Self::Sd,
            "ZR" => Self::Zr,
            _ => return None,
        })
    }
}

/// 解析后的软元件地址。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceAddress {
    /// 软元件种类。
    pub kind: DeviceKind,
    /// 编号（十进制语义；协议编码由帧构造层负责）。
    pub number: u32,
}

/// 软元件号上限（3 字节 LE 编码域，u24）。
const MAX_NUMBER: u32 = 0x00FF_FFFF;

/// 解析软元件地址。
///
/// # Errors
///
/// 语法或范围非法时返回 [`McError::invalid_address`]。
pub fn parse(input: &str) -> Result<DeviceAddress, McError> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(McError::invalid_address("地址为空".to_owned()));
    }
    // 前缀 = 开头的字母序列；编号 = 其后的数字序列。
    let split = raw.find(|c: char| c.is_ascii_digit()).ok_or_else(|| {
        McError::invalid_address(format!("'{raw}' 缺少十进制编号（编号按十进制解析）"))
    })?;
    let (prefix, digits) = raw.split_at(split);
    // 位偏移语法（D200.3）属随机访问语义，V0.3 拒绝并引导。
    if digits.contains('.') {
        return Err(McError::invalid_address(format!(
            "'{raw}' 含位偏移语法 '.'（随机访问不在支持范围；请改用独立点位地址）"
        )));
    }
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(McError::invalid_address(format!(
            "'{raw}' 编号非法（仅允许十进制数字）"
        )));
    }
    let kind = DeviceKind::parse_prefix(prefix).ok_or_else(|| {
        McError::invalid_address(format!(
            "未知软元件前缀 '{prefix}'（支持 X/Y/M/B/S/SM/D/W/R/ZR/SD；T/C 推迟后续版本）"
        ))
    })?;
    let number: u32 = digits.parse().map_err(|_| {
        McError::invalid_address(format!("'{raw}' 编号溢出（u24 上限 {MAX_NUMBER}）"))
    })?;
    if number > MAX_NUMBER {
        return Err(McError::invalid_address(format!(
            "'{raw}' 编号超 u24 上限 {MAX_NUMBER}"
        )));
    }
    Ok(DeviceAddress { kind, number })
}

impl DeviceAddress {
    /// 规范化文本形式（大写前缀 + 十进制编号）。
    #[must_use]
    pub fn canonical(&self) -> String {
        format!("{}{}", self.kind.canonical_prefix(), self.number)
    }
}

impl DeviceKind {
    /// canonical 前缀文本。
    #[must_use]
    pub const fn canonical_prefix(self) -> &'static str {
        match self {
            Self::X => "X",
            Self::Y => "Y",
            Self::M => "M",
            Self::B => "B",
            Self::S => "S",
            Self::Sm => "SM",
            Self::D => "D",
            Self::W => "W",
            Self::R => "R",
            Self::Zr => "ZR",
            Self::Sd => "SD",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_devices_case_insensitive_decimal() {
        let a = parse("D200").unwrap();
        assert_eq!(a.kind, DeviceKind::D);
        assert_eq!(a.number, 200);
        assert_eq!(a.canonical(), "D200");

        // 大小写不敏感 + trim。
        assert_eq!(parse(" d200 ").unwrap(), a);

        // HEX 陷阱：X20 是**十进制** 20（驱动负责转协议 HEX 编码格式）。
        let x = parse("X20").unwrap();
        assert_eq!(x.number, 20);

        for (input, kind) in [
            ("M100", DeviceKind::M),
            ("Y0", DeviceKind::Y),
            ("w10", DeviceKind::W),
            ("ZR100", DeviceKind::Zr),
            ("sm400", DeviceKind::Sm),
        ] {
            assert_eq!(parse(input).unwrap().kind, kind, "{input}");
        }
    }

    #[test]
    fn rejects_invalid_addresses() {
        for bad in [
            "",
            "   ",
            "D",            // 缺编号
            "DX200",        // 未知多字母前缀
            "T0",           // T/C 推迟
            "C10",          // 同上
            "D200.3",       // 位偏移语法拒绝
            "D-5",          // 负号
            "D20x",         // 编号含非数字
            "D99999999999", // 溢出
            "200",          // 无前缀
        ] {
            let err = parse(bad).unwrap_err();
            assert_eq!(err.code, "invalid_address", "'{bad}' 应被拒绝");
        }
    }

    #[test]
    fn writability_follows_device() {
        assert!(!parse("X20").unwrap().kind.writable(), "X 只读");
        assert!(!parse("SM400").unwrap().kind.writable());
        assert!(parse("Y0").unwrap().kind.writable());
        assert!(parse("D200").unwrap().kind.writable());
        assert!(parse("ZR100").unwrap().kind.writable());
    }
}
