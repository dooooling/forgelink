//! 敏感信息脱敏（`开发规范.md` §6）。

use std::sync::OnceLock;

use regex::Regex;

/// 掩盖文本中的常见敏感模式，供错误链等日志场景使用。
///
/// 覆盖：
/// - 键值凭据：`password=...`、`passwd=...`、`pwd=...`、`token=...`、
///   `access_token=...`、`refresh_token=...`、`id_token=...`、
///   `secret=...`、`client_secret=...`、`api_key=...`、`private_key=...`
///   （键名大小写不敏感，允许两端引号与空白）；
/// - URL 内嵌凭据：`scheme://user:pass@host`；
/// - HTTP 认证头：`Basic <base64>`、`Bearer <token>`；
/// - PEM 私钥块：`-----BEGIN * PRIVATE KEY----- ... -----END ...`。
///
/// # 注意
///
/// 脱敏是兜底措施，不能替代"不记录敏感字段"的纪律：日志字段（如
/// `component`、`device_id`）本身不得携带凭据；本函数只用于可能包含
/// 凭据的自由文本（如错误链）。漏报新凭据格式需经评审补充规则。
pub fn redact(text: &str) -> String {
    let out = credentials().replace_all(text, "$1$2***");
    let out = url_credentials().replace_all(&out, "://***@");
    let out = auth_scheme().replace_all(&out, "$1 ***");
    pem_block().replace_all(&out, "[REDACTED]").into_owned()
}

/// 键值凭据：`(password|token|...)\s*[:=]\s*<值>`，值到空白或常见分隔符。
fn credentials() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?i)(['"]?\b(?:password|passwd|pwd|secret|client[_-]?secret|api[_-]?key|private[_-]?key|(?:access|refresh|id)[_-]?token|token)\b['"]?)(\s*[:=]\s*)(['"]?[^\s,;'"}&]+)"#,
        )
        .expect("静态正则应合法")
    })
}

/// URL 内嵌凭据：`scheme://user[:pass]@host`。
fn url_credentials() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?i)://[^@/\s]+@"#).expect("静态正则应合法"))
}

/// HTTP 认证头方案：`Basic <base64>`、`Bearer <token>`（含 JWT）。
fn auth_scheme() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\b(Basic|Bearer)\s+[A-Za-z0-9._~+/=-]+").expect("静态正则应合法")
    })
}

/// PEM 私钥块。
fn pem_block() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----")
            .expect("静态正则应合法")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_value_credentials_masked() {
        assert_eq!(redact("password=abc123"), "password=***");
        assert_eq!(redact("token = abc"), "token = ***");
        assert_eq!(redact("api_key=abc"), "api_key=***");
        assert_eq!(redact("PASSWORD=abc"), "PASSWORD=***");
        assert_eq!(redact("\"password\": \"xyz\""), "\"password\": ***\"");
        assert_eq!(
            redact("?user=admin&passwd=secret&host=h"),
            "?user=admin&passwd=***&host=h"
        );
        assert_eq!(
            redact("失败: client_secret=deadbeef, 请重试"),
            "失败: client_secret=***, 请重试"
        );
    }

    #[test]
    fn url_credentials_masked() {
        assert_eq!(
            redact("mysql://user:secret@host:3306/db"),
            "mysql://***@host:3306/db"
        );
        assert_eq!(
            redact("https://u:p@example.com/x"),
            "https://***@example.com/x"
        );
        // 无凭据 URL 不受影响。
        assert_eq!(redact("https://example.com/x"), "https://example.com/x");
    }

    #[test]
    fn basic_auth_masked() {
        assert_eq!(
            redact("Authorization: Basic dXNlcjpwYXNz"),
            "Authorization: Basic ***"
        );
    }

    #[test]
    fn bearer_token_masked() {
        assert_eq!(
            redact("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abcdefgh"),
            "Authorization: Bearer ***"
        );
        assert_eq!(
            redact("Authorization: bearer abcdef"),
            "Authorization: bearer ***"
        );
    }

    #[test]
    fn underscored_token_keys_masked() {
        assert_eq!(
            redact("access_token=abc&refresh_token=def"),
            "access_token=***&refresh_token=***"
        );
        assert_eq!(redact("?id_token=xyz"), "?id_token=***");
    }

    #[test]
    fn pem_block_masked() {
        let pem = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----";
        assert_eq!(redact(pem), "[REDACTED]");
    }

    #[test]
    fn plain_text_untouched() {
        let text = "Profile 加载失败: bad.json";
        assert_eq!(redact(text), text);
    }
}
