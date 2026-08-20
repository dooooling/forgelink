//! 命令前置条件（§85 Normative）。
//!
//! 部分命令需要满足设备状态条件（如 `machine_mode == AUTO`、`alarm == false`）。
//! 前置条件检查在 Driver 前完成；失败时以 `Rejected`（`PRECONDITION_FAILED`）
//! 拒绝，不进入队列与 Driver。
//!
//! # 安全边界（§85）
//!
//! 软件中的前置条件只能作为辅助保护，**不能替代**设备安全 PLC、安全继电器、
//! 急停回路、门锁和其他硬件安全机制。

use std::fmt;

use observation_model::{CommandPrecondition, DeviceId};

/// 前置条件检查失败（§85）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreconditionError {
    pub message: String,
}

impl fmt::Display for PreconditionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "前置条件不满足：{}", self.message)
    }
}

impl std::error::Error for PreconditionError {}

/// 命令前置条件检查器（§85）。
///
/// 由上层实现（读取当前设备状态后判定）；本 crate 只定义接口并在
/// [`ControlPolicy`](crate::ControlPolicy) 中挂载。`None` 时跳过检查。
pub trait PreconditionChecker: Send + Sync {
    fn check(
        &self,
        device_id: &DeviceId,
        preconditions: &[CommandPrecondition],
    ) -> Result<(), PreconditionError>;
}

/// 通过一切前置条件的检查器（不执行任何限制）。
///
/// 仅在明确"无状态依赖"的测试/演示场景使用；生产环境应提供真实检查器。
pub struct PermissivePreconditionChecker;

impl PreconditionChecker for PermissivePreconditionChecker {
    fn check(
        &self,
        _device_id: &DeviceId,
        _preconditions: &[CommandPrecondition],
    ) -> Result<(), PreconditionError> {
        Ok(())
    }
}

/// 按前置条件文本匹配拒绝的检查器（测试替身）。
///
/// `fail_if` 是子串：某前置条件出现该子串即判定失败（用于测试
/// `PRECONDITION_FAILED` 在 Driver 前被拒绝的路径）。
pub struct PatternPreconditionChecker {
    pub fail_if: Vec<String>,
}

impl PreconditionChecker for PatternPreconditionChecker {
    fn check(
        &self,
        _device_id: &DeviceId,
        preconditions: &[CommandPrecondition],
    ) -> Result<(), PreconditionError> {
        for condition in preconditions {
            let text = format!(
                "{}.{:?}.{:?}",
                condition.property, condition.operator, condition.value
            );
            if self.fail_if.iter().any(|pattern| text.contains(pattern)) {
                return Err(PreconditionError {
                    message: format!("前置条件 {text} 不满足"),
                });
            }
        }
        Ok(())
    }
}
