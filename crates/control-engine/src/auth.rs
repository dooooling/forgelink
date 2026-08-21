//! 静态 Token 认证（§90.2 Normative）。
//!
//! [`StaticTokenAuthorizer`] 从 §90.2 规定的凭据文件加载 `token → (subject,
//! role)` 映射，提供：
//!
//! - **认证**（[`StaticTokenAuthorizer::authenticate`]）：Bearer Token →
//!   身份上下文；Token 比较为常量时间（线性扫描 + 逐字节异或累积），防止
//!   时序侧信道枚举；
//! - **授权**（实现 [`Authorizer`]）：按 subject 查表判定角色是否满足
//!   （§83），与 [`MemoryAuthorizer`] 同语义。
//!
//! # Fail-closed（§90.2）
//!
//! 凭据文件缺失、权限过宽（Unix 非 `0600`）、解析失败、schema 不符、
//! Token 重复、`subject` 为空一律加载失败——调用方必须以启动失败响应，
//! 不得降级为无认证运行。
//!
//! # 敏感边界（§90.2、§6）
//!
//! Token 明文只在内存中持有；本模块的任何错误信息都**不包含** Token 内容
//! 或文件内容原文（只含路径与原因类别），避免凭据经日志泄漏。

use std::collections::HashSet;
use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::role::{AuthorizationError, Authorizer, Role};

/// 凭据文件 schema 标识（§90.2 显式版本化）。
pub const CREDENTIALS_SCHEMA: &str = "forgelink.control.credentials.v1";

/// 凭据加载失败（§90.2 fail-closed）。
///
/// 错误信息只含文件路径与原因类别，不含 Token/文件内容。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialsError {
    /// 文件读取失败（缺失/无权限打开等）。
    Io(String),
    /// Unix 权限过宽（非 `0600`）；携带实际八进制模式。
    #[cfg(unix)]
    PermissionsTooOpen(u32),
    /// JSON 解析失败。
    Parse(String),
    /// `schema` 字段缺失或不匹配。
    SchemaMismatch(String),
    /// 同一 Token 出现多次。
    DuplicateToken,
    /// `subject` 为空。
    EmptySubject,
}

impl fmt::Display for CredentialsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(path) => write!(f, "凭据文件读取失败: {path}"),
            #[cfg(unix)]
            Self::PermissionsTooOpen(mode) => {
                write!(f, "凭据文件权限过宽: {mode:o}（要求 0600，§90.2）")
            }
            Self::Parse(reason) => write!(f, "凭据文件解析失败: {reason}"),
            Self::SchemaMismatch(actual) => {
                write!(
                    f,
                    "凭据文件 schema 不符: {actual}（要求 {CREDENTIALS_SCHEMA}）"
                )
            }
            Self::DuplicateToken => write!(f, "凭据文件存在重复 Token"),
            Self::EmptySubject => write!(f, "凭据条目 subject 为空"),
        }
    }
}

impl std::error::Error for CredentialsError {}

/// 单条凭据（内部表示；Token 明文仅存内存）。
#[derive(Debug, Clone)]
struct CredentialEntry {
    token: String,
    subject: String,
    role: Role,
}

/// 静态 Bearer Token 认证器（§90.2 MVP 方案）。
///
/// 从凭据文件一次性加载（装配期同步 I/O，启动路径调用）；运行期只读，
/// 无锁。条目数量为静态配置规模（通常 ≤ 数十），线性扫描即可支撑常量
/// 时间比较——不得改用哈希表查找（哈希耗时随输入变化，破坏常量时间性质）。
#[derive(Debug, Clone)]
pub struct StaticTokenAuthorizer {
    entries: Vec<CredentialEntry>,
    /// subject → role 二级索引（authorize 用；subject 来自本结构 authenticate
    /// 的返回值，必在表中）。
    roles: std::collections::HashMap<String, Role>,
}

/// 凭据文件的 serde 中间表示（先验 schema 再取条目）。
#[derive(Deserialize)]
struct CredentialsFile {
    schema: String,
    credentials: Vec<CredentialEntryRaw>,
}

#[derive(Deserialize)]
struct CredentialEntryRaw {
    token: String,
    subject: String,
    role: Role,
}

impl StaticTokenAuthorizer {
    /// 从凭据文件加载（§90.2 格式与校验规则）。
    ///
    /// # Errors
    ///
    /// 任一 fail-closed 条件命中即返回 [`CredentialsError`]：文件缺失/
    /// 不可读、Unix 权限非 `0600`、JSON 解析失败、schema 不符、Token 重复、
    /// `subject` 为空。
    pub fn from_file(path: &Path) -> Result<Self, CredentialsError> {
        // 权限校验必须在读取内容之前：权限过宽时连内容都不应信任。
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(path).map_err(|e| CredentialsError::Io(e.to_string()))?;
            let mode = meta.permissions().mode() & 0o777;
            if mode != 0o600 {
                return Err(CredentialsError::PermissionsTooOpen(mode));
            }
        }
        let content =
            std::fs::read_to_string(path).map_err(|e| CredentialsError::Io(e.to_string()))?;
        Self::parse(&content)
    }

    /// 从字符串解析（[`Self::from_file`] 的内容处理部分；测试与非常规
    /// 来源复用）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::from_file`]（除文件 I/O 与权限项）。
    pub fn parse(content: &str) -> Result<Self, CredentialsError> {
        let file: CredentialsFile =
            serde_json::from_str(content).map_err(|e| CredentialsError::Parse(e.to_string()))?;
        if file.schema != CREDENTIALS_SCHEMA {
            return Err(CredentialsError::SchemaMismatch(file.schema));
        }
        let mut entries = Vec::with_capacity(file.credentials.len());
        let mut seen: HashSet<String> = HashSet::new();
        let mut roles = std::collections::HashMap::new();
        for raw in file.credentials {
            if raw.token.is_empty() {
                return Err(CredentialsError::DuplicateToken);
            }
            if raw.subject.is_empty() {
                return Err(CredentialsError::EmptySubject);
            }
            if !seen.insert(raw.token.clone()) {
                return Err(CredentialsError::DuplicateToken);
            }
            roles.insert(raw.subject.clone(), raw.role);
            entries.push(CredentialEntry {
                token: raw.token,
                subject: raw.subject,
                role: raw.role,
            });
        }
        Ok(Self { entries, roles })
    }

    /// 认证：Bearer Token → `(subject, role)`；未知 Token 返回 `None`。
    ///
    /// 常量时间比较（逐字节异或累积后统一判定），扫描耗时与条目数线性
    /// 相关、与匹配位置无关。长度不同的输入立即返回不等——长度本身不是
    /// 秘密（高熵随机串长度公开无害），这是标准取舍（§90.2 注明）。
    pub fn authenticate(&self, token: &str) -> Option<(&str, Role)> {
        let token_bytes = token.as_bytes();
        let mut matched: Option<usize> = None;
        for (index, entry) in self.entries.iter().enumerate() {
            if constant_time_eq(token_bytes, entry.token.as_bytes()) {
                matched = Some(index);
            }
        }
        // 不提前返回：即使已命中也完成全表扫描，保持耗时与内容无关。
        matched.map(|index| {
            let entry = &self.entries[index];
            (entry.subject.as_str(), entry.role)
        })
    }

    /// 查询 subject 角色（未登记为 `None`）。
    pub fn role_of(&self, subject: &str) -> Option<Role> {
        self.roles.get(subject).copied()
    }
}

/// 常量时间字节串比较：累积 XOR 差异，最后统一判定。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl Authorizer for StaticTokenAuthorizer {
    fn authorize(
        &self,
        subject: &str,
        required: Role,
        _device_id: &observation_model::DeviceId,
    ) -> Result<(), AuthorizationError> {
        // subject 只应来自 authenticate() 的返回值；查不到按角色不足拒绝
        // （fail-closed，不区分"未登记"与"角色不够"，避免信息泄露）。
        let granted = self.role_of(subject).unwrap_or(Role::Viewer);
        if crate::role::role_ordering(granted, required) {
            Ok(())
        } else {
            Err(AuthorizationError {
                code: "INSUFFICIENT_ROLE",
                message: format!("用户 {subject} 角色 {granted:?} 不满足操作所需角色 {required:?}"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_JSON: &str = r#"{
        "schema": "forgelink.control.credentials.v1",
        "credentials": [
            { "token": "token-alice-0123456789abcdef", "subject": "alice", "role": "operator" },
            { "token": "token-bob-0123456789abcdef00", "subject": "bob", "role": "viewer" }
        ]
    }"#;

    #[test]
    fn parse_valid_file_and_authenticate() {
        let auth = StaticTokenAuthorizer::parse(VALID_JSON).expect("合法凭据应可加载");
        let (subject, role) = auth
            .authenticate("token-alice-0123456789abcdef")
            .expect("已知 Token 应认证通过");
        assert_eq!(subject, "alice");
        assert_eq!(role, Role::Operator);
        assert!(auth.authenticate("token-bob-0123456789abcdef00").is_some());
    }

    #[test]
    fn unknown_token_rejected() {
        let auth = StaticTokenAuthorizer::parse(VALID_JSON).unwrap();
        assert!(auth.authenticate("token-unknown").is_none());
        assert!(auth.authenticate("").is_none());
        // 前缀匹配不算命中（常量时间比较是全等比较）。
        assert!(auth.authenticate("token-alice").is_none());
    }

    #[test]
    fn duplicate_token_fails_closed() {
        let json = r#"{
            "schema": "forgelink.control.credentials.v1",
            "credentials": [
                { "token": "dup", "subject": "a", "role": "viewer" },
                { "token": "dup", "subject": "b", "role": "operator" }
            ]
        }"#;
        assert_eq!(
            StaticTokenAuthorizer::parse(json).unwrap_err(),
            CredentialsError::DuplicateToken
        );
    }

    #[test]
    fn empty_subject_fails_closed() {
        let json = r#"{
            "schema": "forgelink.control.credentials.v1",
            "credentials": [
                { "token": "t", "subject": "", "role": "operator" }
            ]
        }"#;
        assert_eq!(
            StaticTokenAuthorizer::parse(json).unwrap_err(),
            CredentialsError::EmptySubject
        );
    }

    #[test]
    fn schema_mismatch_fails_closed() {
        let json = r#"{
            "schema": "forgelink.control.credentials.v2",
            "credentials": []
        }"#;
        assert_eq!(
            StaticTokenAuthorizer::parse(json).unwrap_err(),
            CredentialsError::SchemaMismatch("forgelink.control.credentials.v2".to_owned())
        );
    }

    #[test]
    fn invalid_role_string_fails_closed() {
        let json = r#"{
            "schema": "forgelink.control.credentials.v1",
            "credentials": [
                { "token": "t", "subject": "a", "role": "superuser" }
            ]
        }"#;
        assert!(matches!(
            StaticTokenAuthorizer::parse(json),
            Err(CredentialsError::Parse(_))
        ));
    }

    #[test]
    fn missing_file_fails_closed() {
        let err = StaticTokenAuthorizer::from_file(Path::new("/nonexistent/credentials.json"))
            .unwrap_err();
        assert!(matches!(err, CredentialsError::Io(_)));
    }

    #[cfg(unix)]
    #[test]
    fn permissions_too_open_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("fl-auth-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("creds.json");
        std::fs::write(&path, VALID_JSON).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = StaticTokenAuthorizer::from_file(&path).unwrap_err();
        assert_eq!(err, CredentialsError::PermissionsTooOpen(0o644));

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(StaticTokenAuthorizer::from_file(&path).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn authorize_follows_role_ordering() {
        let auth = StaticTokenAuthorizer::parse(VALID_JSON).unwrap();
        // alice=operator：满足 Operator 及以下，不满足 Administrator。
        assert!(
            auth.authorize("alice", Role::Operator, &"dev-1".to_owned())
                .is_ok()
        );
        assert!(
            auth.authorize("alice", Role::Administrator, &"dev-1".to_owned())
                .is_err()
        );
        // bob=viewer：不能控制。
        assert!(
            auth.authorize("bob", Role::Operator, &"dev-1".to_owned())
                .is_err()
        );
        // 未登记 subject 默认 Viewer（只读，与 MemoryAuthorizer 同语义）：
        // Viewer 级可过，控制级必拒。
        assert!(
            auth.authorize("nobody", Role::Viewer, &"dev-1".to_owned())
                .is_ok()
        );
        assert!(
            auth.authorize("nobody", Role::Operator, &"dev-1".to_owned())
                .is_err()
        );
    }

    #[test]
    fn error_display_never_contains_token() {
        let json = r#"{
            "schema": "bad",
            "credentials": [ { "token": "secret-token-value", "subject": "a", "role": "viewer" } ]
        }"#;
        let err = StaticTokenAuthorizer::parse(json).unwrap_err().to_string();
        assert!(
            !err.contains("secret-token-value"),
            "错误信息不得包含 Token"
        );
    }
}
