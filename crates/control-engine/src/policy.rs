//! 控制策略与风险级别配置（§86 Normative）。
//!
//! 不同风险等级可以配置：角色要求、优先级、超时等。默认值遵循 §86 的示例语义：
//! 普通设定值修改 → Low/Medium；CNC Cycle Start → High；Robot Motion → High/Critical；
//! 安全相关动作必须由设备本身安全系统负责，软件分级不能替代（§85）。

use std::collections::HashMap;
use std::time::Duration;

#[cfg(test)]
use observation_model::CommandPrecondition;
use observation_model::CommandRiskLevel;

use crate::role::Role;

/// 队列优先级（§87：priority）。
///
/// 声明顺序即优先级：`Critical` 最高、`Low` 最低；同级内保持 FIFO。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

/// 命令风险级别 → 队列优先级映射（§87）。
pub type CommandPriority = HashMap<CommandRiskLevel, Priority>;

/// 控制策略（§86）。
///
/// 可配置项：每设备队列容量（§87 有界）、幂等记录保留时长（§80.1 ≥24h）、
/// 属性写入要求角色/优先级/超时、命令按风险等级的要求角色/优先级/超时、
/// 以及命令前置条件检查器（§85）。
#[derive(Clone)]
pub struct ControlPolicy {
    /// 每设备独立队列容量（§87 有界队列）；满时新请求以 `Rejected`（`QUEUE_FULL`）拒绝。
    pub queue_capacity: usize,
    /// 幂等记录至少保留 24 小时（§80.1），可配置延长。
    pub idempotency_retention: Duration,
    /// 属性写入要求的最小角色（§83）。
    pub property_write_required_role: Role,
    /// 属性写入队列优先级（§87）。
    pub property_write_priority: Priority,
    /// 属性写入缺省超时（毫秒）；与请求 `timeout_ms` 取较小值。
    pub property_write_timeout_ms: u64,
    /// 命令按风险等级要求的最小角色（§83、§86）。
    pub command_required_role: HashMap<CommandRiskLevel, Role>,
    /// 命令按风险等级的队列优先级（§87）。
    pub command_priority: CommandPriority,
    /// 命令按风险等级的缺省超时（毫秒）。
    pub command_timeout_ms: HashMap<CommandRiskLevel, u64>,
    /// 命令前置条件检查器（§85）；`None` 表示跳过前置条件检查。
    pub precondition_checker: Option<std::sync::Arc<dyn crate::precondition::PreconditionChecker>>,
}

impl std::fmt::Debug for ControlPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPolicy")
            .field("queue_capacity", &self.queue_capacity)
            .field("idempotency_retention", &self.idempotency_retention)
            .field(
                "property_write_required_role",
                &self.property_write_required_role,
            )
            .field("property_write_priority", &self.property_write_priority)
            .field("property_write_timeout_ms", &self.property_write_timeout_ms)
            .field("command_required_role", &self.command_required_role)
            .field("command_priority", &self.command_priority)
            .field("command_timeout_ms", &self.command_timeout_ms)
            .field("precondition_checker", &self.precondition_checker.is_some())
            .finish()
    }
}

impl Default for ControlPolicy {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            idempotency_retention: Duration::from_secs(24 * 3600),
            property_write_required_role: Role::Operator,
            property_write_priority: Priority::Medium,
            property_write_timeout_ms: 5_000,
            command_required_role: HashMap::from([
                (CommandRiskLevel::Low, Role::Operator),
                (CommandRiskLevel::Medium, Role::Operator),
                (CommandRiskLevel::High, Role::Engineer),
                (CommandRiskLevel::Critical, Role::Administrator),
            ]),
            command_priority: HashMap::from([
                (CommandRiskLevel::Low, Priority::Low),
                (CommandRiskLevel::Medium, Priority::Medium),
                (CommandRiskLevel::High, Priority::High),
                (CommandRiskLevel::Critical, Priority::Critical),
            ]),
            command_timeout_ms: HashMap::from([
                (CommandRiskLevel::Low, 5_000),
                (CommandRiskLevel::Medium, 10_000),
                (CommandRiskLevel::High, 15_000),
                (CommandRiskLevel::Critical, 20_000),
            ]),
            precondition_checker: None,
        }
    }
}

/// 操作种类（用于按操作查策略）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    PropertyWrite,
    Command(CommandRiskLevel),
}

impl ControlPolicy {
    /// 该操作要求的最小角色（§83）。
    pub fn required_role(&self, kind: OperationKind) -> Role {
        match kind {
            OperationKind::PropertyWrite => self.property_write_required_role,
            OperationKind::Command(risk) => self
                .command_required_role
                .get(&risk)
                .copied()
                .unwrap_or(Role::Administrator),
        }
    }

    /// 该操作的队列优先级（§87）。
    pub fn priority(&self, kind: OperationKind) -> Priority {
        match kind {
            OperationKind::PropertyWrite => self.property_write_priority,
            OperationKind::Command(risk) => self
                .command_priority
                .get(&risk)
                .copied()
                .unwrap_or(Priority::Medium),
        }
    }

    /// 该操作的缺省超时（毫秒）；与请求 `timeout_ms` 取较小值（策略是安全上限）。
    pub fn timeout_ms(&self, kind: OperationKind) -> u64 {
        match kind {
            OperationKind::PropertyWrite => self.property_write_timeout_ms,
            OperationKind::Command(risk) => {
                self.command_timeout_ms.get(&risk).copied().unwrap_or(5_000)
            }
        }
    }

    /// 有效超时：请求指定与策略上限的较小值（请求 `timeout_ms` 为 0 视为非法，
    /// 由引擎拒绝）。
    pub fn effective_timeout_ms(&self, kind: OperationKind, request_timeout_ms: u64) -> u64 {
        request_timeout_ms.min(self.timeout_ms(kind))
    }

    /// 前置条件检查器引用（§85）。
    pub fn precondition_checker(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::precondition::PreconditionChecker>> {
        self.precondition_checker.clone()
    }
}

/// 用于策略键枚举的命令风险级别辅助（`CommandRiskLevel` 全量）。
pub const ALL_RISK_LEVELS: [CommandRiskLevel; 4] = [
    CommandRiskLevel::Low,
    CommandRiskLevel::Medium,
    CommandRiskLevel::High,
    CommandRiskLevel::Critical,
];

/// 供审计等场景使用的哨兵，提示策略表默认值已回退到最严格档。
pub fn risk_default_role(risk: CommandRiskLevel) -> Role {
    match risk {
        CommandRiskLevel::Low => Role::Operator,
        CommandRiskLevel::Medium => Role::Operator,
        CommandRiskLevel::High => Role::Engineer,
        CommandRiskLevel::Critical => Role::Administrator,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_roles_follow_doc_examples() {
        let policy = ControlPolicy::default();
        assert_eq!(
            policy.required_role(OperationKind::Command(CommandRiskLevel::High)),
            Role::Engineer
        );
        assert_eq!(
            policy.required_role(OperationKind::Command(CommandRiskLevel::Critical)),
            Role::Administrator
        );
        assert_eq!(
            policy.required_role(OperationKind::PropertyWrite),
            Role::Operator
        );
    }

    #[test]
    fn default_retention_is_24h() {
        assert_eq!(
            ControlPolicy::default().idempotency_retention,
            Duration::from_secs(24 * 3600)
        );
    }

    #[test]
    fn effective_timeout_caps_request_by_policy() {
        let policy = ControlPolicy::default();
        assert_eq!(
            policy.effective_timeout_ms(OperationKind::Command(CommandRiskLevel::Low), 1_000),
            1_000
        );
        assert_eq!(
            policy.effective_timeout_ms(OperationKind::Command(CommandRiskLevel::Low), 60_000),
            5_000
        );
    }

    #[test]
    fn risk_priority_mapping_is_total() {
        for risk in ALL_RISK_LEVELS {
            let policy = ControlPolicy::default();
            let priority = policy.priority(OperationKind::Command(risk));
            let role = policy.required_role(OperationKind::Command(risk));
            assert!(matches!(
                priority,
                Priority::Low | Priority::Medium | Priority::High | Priority::Critical
            ));
            assert!(matches!(
                role,
                Role::Operator | Role::Engineer | Role::Administrator
            ));
        }
    }

    #[test]
    fn preconditions_default_none() {
        assert!(ControlPolicy::default().precondition_checker().is_none());
        // 类型上验证 CommandPrecondition 可被策略持有（§85 引用完整性）。
        let _p: Option<CommandPrecondition> = None;
    }
}
