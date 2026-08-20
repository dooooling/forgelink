//! 权限角色模型与授权器（§83 Normative）。
//!
//! 角色至少支持 `viewer` / `operator` / `engineer` / `administrator`，
//! 从低到高分别为：只读、普通操作、参数修改、设备与系统配置。
//!
//! 授权器通过 [`Authorizer`] trait 抽象，本 crate 提供内存版
//! [`MemoryAuthorizer`]；上层可用任意后端（LDAP / DB / 配置表）替换。

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use observation_model::DeviceId;
use serde::{Deserialize, Serialize};

/// 权限角色（§83）。
///
/// 声明顺序即角色等级：`Viewer < Operator < Engineer < Administrator`。
/// 拥有高等级角色的调用者自动具备低等级角色的全部权限。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Viewer,
    Operator,
    Engineer,
    Administrator,
}

/// 角色等级比较：`required` 是否可被 `granted` 满足（同级或更高）。
pub fn role_ordering(granted: Role, required: Role) -> bool {
    granted >= required
}

/// 授权失败（§83）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationError {
    /// 稳定错误码：`UNKNOWN_SUBJECT` / `INSUFFICIENT_ROLE`。
    pub code: &'static str,
    pub message: String,
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for AuthorizationError {}

/// 授权器抽象（§83，接口必须可替换）。
///
/// 实现负责回答「`subject` 是否被允许以 `required` 角色操作 `device_id`」。
/// 设备级拒绝（如黑名单）也在此处表达。
pub trait Authorizer: Send + Sync {
    fn authorize(
        &self,
        subject: &str,
        required: Role,
        device_id: &DeviceId,
    ) -> Result<(), AuthorizationError>;
}

/// 内存版授权器（§83 初始实现，接口可替换）。
///
/// - 已知用户按显式角色判定；未登记用户默认 `Viewer`（只读，不能控制）；
/// - 角色不满足（或未登记）一律返回 `INSUFFICIENT_ROLE`；
/// - 设备级拒绝可通过 `deny` 列表声明。
#[derive(Debug, Default)]
pub struct MemoryAuthorizer {
    roles: RwLock<HashMap<String, Role>>,
    denied: RwLock<Vec<String>>,
}

impl MemoryAuthorizer {
    /// 新建授权器（初始为空，所有用户默认 `Viewer`）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置（或覆盖）用户角色。
    pub fn set_role(&self, subject: &str, role: Role) {
        self.roles
            .write()
            .expect("MemoryAuthorizer roles 锁被毒化")
            .insert(subject.to_owned(), role);
    }

    /// 移除用户角色（恢复默认 `Viewer`）。
    pub fn remove_role(&self, subject: &str) {
        self.roles
            .write()
            .expect("MemoryAuthorizer roles 锁被毒化")
            .remove(subject);
    }

    /// 声明某用户对某设备完全拒绝（黑名单，优先级最高）。
    pub fn deny(&self, subject: &str, device_id: &str) {
        self.denied
            .write()
            .expect("MemoryAuthorizer denied 锁被毒化")
            .push(format!("{subject}@{device_id}"));
    }

    /// 查询用户角色（未登记为 `None`）。
    pub fn role_of(&self, subject: &str) -> Option<Role> {
        self.roles
            .read()
            .expect("MemoryAuthorizer roles 锁被毒化")
            .get(subject)
            .copied()
    }
}

impl Authorizer for MemoryAuthorizer {
    fn authorize(
        &self,
        subject: &str,
        required: Role,
        device_id: &DeviceId,
    ) -> Result<(), AuthorizationError> {
        if self
            .denied
            .read()
            .expect("MemoryAuthorizer denied 锁被毒化")
            .iter()
            .any(|entry| entry == &format!("{subject}@{device_id}"))
        {
            return Err(AuthorizationError {
                code: "INSUFFICIENT_ROLE",
                message: format!("用户 {subject} 被禁止操作设备 {device_id}"),
            });
        }
        // 未登记用户默认 Viewer（只读），控制操作必然不足。
        let granted = self.role_of(subject).unwrap_or(Role::Viewer);
        if role_ordering(granted, required) {
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

    #[test]
    fn role_ordering_is_inclusive() {
        assert!(role_ordering(Role::Administrator, Role::Administrator));
        assert!(role_ordering(Role::Administrator, Role::Viewer));
        assert!(role_ordering(Role::Engineer, Role::Operator));
        assert!(!role_ordering(Role::Operator, Role::Engineer));
        assert!(!role_ordering(Role::Viewer, Role::Operator));
    }

    #[test]
    fn unknown_subject_is_viewer_and_cannot_control() {
        let auth = MemoryAuthorizer::new();
        let err = auth
            .authorize("nobody", Role::Operator, &"dev-1".to_owned())
            .unwrap_err();
        assert_eq!(err.code, "INSUFFICIENT_ROLE");
    }

    #[test]
    fn set_role_grants_and_remove_revokes() {
        let auth = MemoryAuthorizer::new();
        auth.set_role("alice", Role::Engineer);
        assert!(
            auth.authorize("alice", Role::Operator, &"dev-1".to_owned())
                .is_ok()
        );
        assert!(
            auth.authorize("alice", Role::Engineer, &"dev-1".to_owned())
                .is_ok()
        );
        assert!(
            auth.authorize("alice", Role::Administrator, &"dev-1".to_owned())
                .is_err()
        );
        auth.remove_role("alice");
        assert!(
            auth.authorize("alice", Role::Operator, &"dev-1".to_owned())
                .is_err()
        );
    }

    #[test]
    fn deny_list_takes_precedence() {
        let auth = MemoryAuthorizer::new();
        auth.set_role("bob", Role::Administrator);
        auth.deny("bob", "dev-1");
        assert!(
            auth.authorize("bob", Role::Administrator, &"dev-1".to_owned())
                .is_err()
        );
        // 仅拒绝特定设备，其他设备不受影响。
        assert!(
            auth.authorize("bob", Role::Administrator, &"dev-2".to_owned())
                .is_ok()
        );
    }

    #[test]
    fn role_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&Role::Administrator).unwrap(),
            r#""administrator""#
        );
        let back: Role = serde_json::from_str(r#""engineer""#).unwrap();
        assert_eq!(back, Role::Engineer);
    }
}
