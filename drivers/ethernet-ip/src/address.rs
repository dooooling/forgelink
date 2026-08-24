//! 标签路径文法解析（CIP 符号寻址）。
//!
//! # 文法
//!
//! ```text
//! path := seg ( '.' seg | '[' idx (',' idx){0,2} ']' )*
//! seg  := [A-Za-z_][A-Za-z0-9_]*        // 单段 ≤ 40 字符
//! idx  := 十进制 0..=4294967295
//! ```
//!
//! 总长 ≤ 240 字节（0x91 符号段长度域为 u8，整条路径入单段须留余量）；
//! 仅 ASCII 可见字符、不含空白。
//!
//! # 大小写敏感（与 S7 地址相反！）
//!
//! CIP 标签查找区分大小写：`Motor` 与 `motor` 是两个不同标签。
//! canonical 形式 = trim 前后空白后**原样保留**——绝不改变大小写。
//!
//! # 与数字寄存器驱动的本质差异
//!
//! 宽度与基础编码不由语法承载，而由设备应答携带的 CIP 类型码承载
//! （见 `decode` 模块映射表）；本模块只做离线语法校验。

/// 解析后的标签路径：canonical 原样保留 + 维度数（信息性）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagPath {
    /// 规范化路径文本（trim 后原样保留大小写）。
    pub raw: String,
    /// 数组维度数（0 = 非数组下标）。
    pub dims: usize,
}

/// 地址错误（全部映射为稳定码 `invalid_address`，不可重试）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressError {
    /// 语法无法识别。
    InvalidSyntax(String),
    /// 数值/长度越界。
    OutOfRange(String),
}

impl std::fmt::Display for AddressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSyntax(msg) | Self::OutOfRange(msg) => write!(f, "{msg}"),
        }
    }
}

const MAX_TOTAL_LEN: usize = 240;
const MAX_SEG_LEN: usize = 40;
const MAX_DIMS: usize = 3;
const MAX_INDICES_PER_DIM: usize = 3;

/// 解析并校验标签路径。
///
/// # Errors
///
/// 语法或长度非法时返回 [`AddressError`]（调用方映射为 `invalid_address`）。
pub fn parse(input: &str) -> Result<TagPath, AddressError> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(AddressError::InvalidSyntax("地址为空".to_owned()));
    }
    if raw.len() > MAX_TOTAL_LEN {
        return Err(AddressError::OutOfRange(format!(
            "地址总长 {} 超上限 {MAX_TOTAL_LEN}",
            raw.len()
        )));
    }
    if !raw.is_ascii() || raw.chars().any(char::is_whitespace) {
        return Err(AddressError::InvalidSyntax(
            "地址仅允许 ASCII 可见字符且不得含空白".to_owned(),
        ));
    }

    // 累加器状态机：段名缓冲 / 下标缓冲，遇分隔符统一收口校验。
    let mut dims = 0usize;
    let mut seg = String::new();
    let mut index = String::new();
    let mut in_bracket = false;
    let mut indices_in_dim = 0usize;

    macro_rules! close_segment {
        () => {
            finish_seg(&seg)?;
            seg.clear();
        };
    }

    for c in raw.chars() {
        match c {
            '.' if !in_bracket => {
                close_segment!();
            }
            '[' if !in_bracket => {
                close_segment!();
                in_bracket = true;
                indices_in_dim = 0;
                dims += 1;
                if dims > MAX_DIMS {
                    return Err(AddressError::OutOfRange(format!(
                        "数组维度 {dims} 超上限 {MAX_DIMS}"
                    )));
                }
            }
            ']' if in_bracket => {
                parse_index(&index)?;
                index.clear();
                in_bracket = false;
            }
            ',' if in_bracket => {
                parse_index(&index)?;
                index.clear();
                indices_in_dim += 1;
                if indices_in_dim >= MAX_INDICES_PER_DIM {
                    return Err(AddressError::OutOfRange(format!(
                        "单维度下标数超上限 {MAX_INDICES_PER_DIM}"
                    )));
                }
            }
            other => {
                if in_bracket {
                    index.push(other);
                } else {
                    seg.push(other);
                }
            }
        }
    }
    if in_bracket {
        return Err(AddressError::InvalidSyntax(format!("未闭合的 '[': {raw}")));
    }
    // 收尾段：末尾若紧跟下标已在上面的 ']' 分支闭合；裸段在此校验。
    if !seg.is_empty() || raw.ends_with('.') {
        finish_seg(&seg)?;
    }
    Ok(TagPath {
        raw: raw.to_owned(),
        dims,
    })
}

fn finish_seg(seg: &str) -> Result<(), AddressError> {
    if seg.is_empty() {
        return Err(AddressError::InvalidSyntax("空段名".to_owned()));
    }
    if seg.len() > MAX_SEG_LEN {
        return Err(AddressError::OutOfRange(format!(
            "段长 {} 超上限 {MAX_SEG_LEN}: {seg}",
            seg.len()
        )));
    }
    let first = seg.as_bytes()[0] as char;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(AddressError::InvalidSyntax(format!(
            "段首字符非法 '{first}'（须字母或下划线）"
        )));
    }
    if !seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(AddressError::InvalidSyntax(format!(
            "段含非法字符: {seg}（仅字母/数字/下划线）"
        )));
    }
    Ok(())
}

fn parse_index(text: &str) -> Result<u32, AddressError> {
    text.parse::<u32>()
        .map_err(|_| AddressError::OutOfRange(format!("数组下标 '{text}' 非法（十进制 u32）")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_paths_preserving_case() {
        for (input, dims) in [
            ("Tag", 0),
            ("Motor.Starter[2].Speed", 1),
            ("Matrix[2,3]", 1),
            ("Cube[1,2,3]", 1),
            ("  Line1.Value  ", 0),
        ] {
            let p = parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(p.dims, dims);
        }
        // 大小写敏感：canonical 原样保留，绝不转小写。
        assert_eq!(parse("MixedCase_Tag[10]").unwrap().raw, "MixedCase_Tag[10]");
        assert_ne!(
            parse("motor").unwrap().raw,
            parse("MOTOR").unwrap().raw,
            "大小写是不同标签"
        );
    }

    #[test]
    fn rejects_invalid_paths() {
        for bad in [
            "",
            "   ",
            "[0]",
            "a..b",
            "a.",
            ".a",
            "a[]",
            "a[",
            "a[-1]",
            "a[99999999999]",
            "a[1,2,3,4]",
            "a b",
            "中文标签",
            "9Start",
            "a[1][2]",
            "a[[1]]",
            "a[1,]b",
        ] {
            assert!(parse(bad).is_err(), "'{bad}' 应被拒绝");
        }
    }

    #[test]
    fn total_length_capped() {
        let long_tag = "a".repeat(241);
        assert!(parse(&long_tag).is_err());
        let ok_tag = format!("{}[1]", "a".repeat(200));
        assert!(parse(&ok_tag).is_ok());
    }
}
