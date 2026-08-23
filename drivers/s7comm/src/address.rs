//! 地址解析（§11 Siemens S7 地址模型）。
//!
//! # 文法（大小写不敏感，前后空白忽略）
//!
//! ```text
//! DB 区:   db<db>.dbx<byte>.<bit>   位（1 bit）
//!          db<db>.dbb<byte>         字节（1 B）
//!          db<db>.dbw<byte>         字（2 B）
//!          db<db>.dbd<byte>         双字（4 B）
//! M 区:    m<byte>.<bit> | mb<n> | mw<n> | md<n>
//! I 区:    i<byte>.<bit> | ib<n> | iw<n> | id<n>
//! Q 区:    q<byte>.<bit> | qb<n> | qw<n> | qd<n>
//! ```
//!
//! 宽度由语法后缀固定；值的语义解释（有符号/无符号/实数）由读取请求
//! 的 `expected_type` 决定（见 `decode` 模块映射表）。裸 `m20`（无宽度
//! 后缀也无位号）被拒绝——S7 无 Modicon 数字段推断惯例，消除宽度歧义。
//!
//! 范围：db ∈ 1..=65535（0 保留）；字节偏移 ≤ 0x001F_FFFF（Any 指针
//! 3 字节地址域，低 3 位为位号）；bit ∈ 0..=7。

/// 存储区（§11：Db / Marker / Input / Output）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Area {
    /// Data Block。
    Db,
    /// Marker（M 区）。
    Marker,
    /// 过程映像输入（I 区，只读）。
    Input,
    /// 过程映像输出（Q 区）。
    Output,
}

impl Area {
    /// S7 Any 指针的 area 代码。
    pub fn code(self) -> u8 {
        match self {
            Self::Db => crate::pdu::AREA_DB,
            Self::Marker => crate::pdu::AREA_MARKER,
            Self::Input => crate::pdu::AREA_INPUT,
            Self::Output => crate::pdu::AREA_OUTPUT,
        }
    }
}

/// 数据宽度类型（§11 `S7Type`）：由地址语法后缀承载，与值语义解释
/// （expected_type）正交。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum S7Type {
    /// 位（1 bit）。
    Bit,
    /// 字节（1 B）。
    Byte,
    /// 字（2 B）。
    Word,
    /// 双字（4 B）。
    Dword,
}

impl S7Type {
    /// 该宽度的字节数（位以 1 字节承载）。
    pub fn width_bytes(self) -> u32 {
        match self {
            Self::Bit | Self::Byte => 1,
            Self::Word => 2,
            Self::Dword => 4,
        }
    }

    /// 语法后缀小写形式（canonical 输出用）。
    fn suffix(self) -> &'static str {
        match self {
            Self::Bit => "dbx",
            Self::Byte => "dbb",
            Self::Word => "dbw",
            Self::Dword => "dbd",
        }
    }
}

/// 解析后的 S7 地址（§11 `S7Address` 的统一扁平形式）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S7Address {
    /// 存储区。
    pub area: Area,
    /// DB 号（非 DB 区恒 0）。
    pub db: u16,
    /// 字节偏移。
    pub byte: u32,
    /// 位号（仅 `S7Type::Bit` 有效，0..=7）。
    pub bit: u8,
    /// 宽度类型（语法后缀承载）。
    pub ty: S7Type,
}

/// 地址错误（全部映射为稳定码 `invalid_address`，不可重试）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressError {
    /// 语法无法识别（如缺少宽度后缀、非法字符）。
    InvalidSyntax(String),
    /// 数值越界（db=0 或 >65535、bit>7、偏移超 3 字节地址域）。
    OutOfRange(String),
}

impl std::fmt::Display for AddressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSyntax(msg) | Self::OutOfRange(msg) => write!(f, "{msg}"),
        }
    }
}

/// Any 指针地址域可表示的最大字节偏移（3 字节大端，低 3 位为位号）。
const MAX_BYTE_OFFSET: u32 = 0x001F_FFFF;

/// 解析 S7 地址字符串（§11）。
///
/// # Errors
///
/// 语法或范围非法时返回 [`AddressError`]（调用方映射为 `invalid_address`）。
pub fn parse(input: &str) -> Result<S7Address, AddressError> {
    let s = input.trim().to_ascii_lowercase();
    if let Some(rest) = s.strip_prefix("db") {
        return parse_db(rest);
    }
    if let Some(rest) = strip_area(&s) {
        return parse_area(rest);
    }
    Err(AddressError::InvalidSyntax(format!(
        "无法识别的 S7 地址: {input}"
    )))
}

fn strip_area(s: &str) -> Option<(Area, &str)> {
    let (c, rest) = s.split_at(1);
    let area = match c {
        "m" => Area::Marker,
        "i" => Area::Input,
        "q" => Area::Output,
        _ => return None,
    };
    Some((area, rest))
}

/// DB 形式：`<db>.dbx<b>.<bit>` / `.dbb|.dbw|.dbd<b>`。
fn parse_db(rest: &str) -> Result<S7Address, AddressError> {
    let (db_str, tail) = rest
        .split_once('.')
        .ok_or_else(|| AddressError::InvalidSyntax(format!("DB 地址缺少 '.': db{rest}")))?;
    let db = parse_u16(db_str).map_err(AddressError::OutOfRange)?;
    if db == 0 {
        return Err(AddressError::OutOfRange("DB 号不得为 0".to_owned()));
    }
    let (suffix, offset_str) = tail.split_at(
        tail.find(|c: char| c.is_ascii_digit())
            .ok_or_else(|| AddressError::InvalidSyntax(format!("缺少偏移: {tail}")))?,
    );
    let (ty, byte, bit) = match suffix {
        "dbx" => {
            let (off, b) = split_bit(offset_str).ok_or_else(|| {
                AddressError::InvalidSyntax(format!("dbx 需要位号: {offset_str}"))
            })?;
            (S7Type::Bit, off, b)
        }
        "dbb" => (S7Type::Byte, parse_u32(offset_str)?, 0),
        "dbw" => (S7Type::Word, parse_u32(offset_str)?, 0),
        "dbd" => (S7Type::Dword, parse_u32(offset_str)?, 0),
        other => {
            return Err(AddressError::InvalidSyntax(format!(
                "未知 DB 后缀 '{other}'（支持 dbx/dbb/dbw/dbd）"
            )));
        }
    };
    validate_offset(byte)?;
    if ty == S7Type::Bit {
        validate_bit(bit)?;
    }
    Ok(S7Address {
        area: Area::Db,
        db,
        byte,
        bit,
        ty,
    })
}

/// M/I/Q 形式：`<byte>.<bit>` 或 `<b|w|d><n>`。
fn parse_area((area, rest): (Area, &str)) -> Result<S7Address, AddressError> {
    let addr = if let Some((byte, bit)) = split_bit(rest) {
        validate_bit(bit)?;
        S7Address {
            area,
            db: 0,
            byte,
            bit,
            ty: S7Type::Bit,
        }
    } else {
        let (suffix, digits) = rest.split_at(
            rest.find(|c: char| c.is_ascii_digit())
                .ok_or_else(|| AddressError::InvalidSyntax(format!("缺少偏移: {rest}")))?,
        );
        let ty = match suffix {
            "b" => S7Type::Byte,
            "w" => S7Type::Word,
            "d" => S7Type::Dword,
            other => {
                return Err(AddressError::InvalidSyntax(format!(
                    "未知宽度后缀 '{other}'（裸数字地址必须带位号，如 m0.1）"
                )));
            }
        };
        S7Address {
            area,
            db: 0,
            byte: parse_u32(digits)?,
            bit: 0,
            ty,
        }
    };
    validate_offset(addr.byte)?;
    Ok(addr)
}

/// 拆分 `<digits>.<bit>`；无 '.' 时返回 None（交由宽度后缀分支处理）。
fn split_bit(s: &str) -> Option<(u32, u8)> {
    let (off, bit) = s.split_once('.')?;
    Some((parse_u32(off).ok()?, parse_u8(bit).ok()?))
}

fn parse_u16(s: &str) -> Result<u16, String> {
    s.parse::<u16>().map_err(|_| format!("非法数值 '{s}'"))
}

fn parse_u32(s: &str) -> Result<u32, AddressError> {
    s.parse::<u32>()
        .map_err(|_| AddressError::OutOfRange(format!("非法数值 '{s}'")))
}

fn parse_u8(s: &str) -> Result<u8, String> {
    s.parse::<u8>().map_err(|_| format!("非法数值 '{s}'"))
}

fn validate_bit(bit: u8) -> Result<(), AddressError> {
    if bit > 7 {
        return Err(AddressError::OutOfRange(format!(
            "位号 {bit} 越界（0..=7）"
        )));
    }
    Ok(())
}

fn validate_offset(byte: u32) -> Result<(), AddressError> {
    if byte > MAX_BYTE_OFFSET {
        return Err(AddressError::OutOfRange(format!(
            "字节偏移 {byte} 超 Any 指针 3 字节地址域上限（{MAX_BYTE_OFFSET}）"
        )));
    }
    Ok(())
}

impl S7Address {
    /// 规范化文本形式（小写；validate_address 返回与去重用）。
    pub fn canonical(&self) -> String {
        match self.area {
            Area::Db => match self.ty {
                S7Type::Bit => format!("db{}.dbx{}.{}", self.db, self.byte, self.bit),
                _ => format!("db{}.{}{}", self.db, self.ty.suffix(), self.byte),
            },
            Area::Marker => canonical_area("m", self),
            Area::Input => canonical_area("i", self),
            Area::Output => canonical_area("q", self),
        }
    }

    /// I 区为过程映像输入，只读（真机会拒绝写 I）。
    pub fn writable(&self) -> bool {
        self.area != Area::Input
    }

    /// 该地址覆盖的字节数（位以 1 字节承载）。
    pub fn width_bytes(&self) -> u32 {
        self.ty.width_bytes()
    }
}

fn canonical_area(prefix: &str, a: &S7Address) -> String {
    match a.ty {
        S7Type::Bit => format!("{prefix}{}.{}", a.byte, a.bit),
        _ => format!(
            "{prefix}{}{}",
            match a.ty {
                S7Type::Byte => "b",
                S7Type::Word => "w",
                _ => "d",
            },
            a.byte
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_db_forms_case_insensitive() {
        let a = parse("DB10.DBD20").unwrap();
        assert_eq!(
            a,
            S7Address {
                area: Area::Db,
                db: 10,
                byte: 20,
                bit: 0,
                ty: S7Type::Dword
            }
        );
        assert_eq!(a.canonical(), "db10.dbd20");
        // 大小写混合与空格。
        assert_eq!(parse(" Db10.DBD20 ").unwrap(), a);
        assert_eq!(parse("db1.dbx0.3").unwrap().bit, 3);
        assert_eq!(parse("db1.dbx0.3").unwrap().ty, S7Type::Bit);
        assert_eq!(parse("db65535.dbb0").unwrap().db, 65535);
    }

    #[test]
    fn parses_marker_input_output_forms() {
        assert_eq!(parse("mw20").unwrap().canonical(), "mw20");
        assert_eq!(parse("MD4").unwrap().ty, S7Type::Dword);
        assert_eq!(parse("ib10").unwrap().area, Area::Input);
        assert_eq!(parse("qw2").unwrap().area, Area::Output);
        let bit = parse("m0.1").unwrap();
        assert_eq!((bit.ty, bit.byte, bit.bit), (S7Type::Bit, 0, 1));
        assert_eq!(bit.canonical(), "m0.1");
    }

    #[test]
    fn rejects_invalid_addresses() {
        for bad in [
            "m20",          // 裸数字无宽度后缀也无位号
            "db0.dbw0",     // DB 号 0 保留
            "db65536.dbw0", // DB 号超 u16
            "db1.dbx0.8",   // 位号 > 7
            "db1.dbq0",     // 未知后缀
            "db1.dbw",      // 缺偏移
            "xyz",          // 非法前缀
            "mw99999999",   // 偏移溢出
        ] {
            assert!(parse(bad).is_err(), "'{bad}' 应被拒绝");
        }
    }

    #[test]
    fn writability_follows_area() {
        assert!(parse("db1.dbw0").unwrap().writable());
        assert!(parse("mw0").unwrap().writable());
        assert!(parse("qw0").unwrap().writable());
        assert!(!parse("iw0").unwrap().writable(), "过程映像输入只读");
    }
}
