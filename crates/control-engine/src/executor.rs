//! 控制执行器（§88 Read Worker 与 Command Worker 分离的 Control 侧）。
//!
//! [`ControlExecutor`] 是 Control Engine 调用 Driver 的唯一抽象：引擎把
//! Profile 映射后的 `DriverWriteItem` / `DriverCommand` 交给执行器，执行器
//! 负责真正落到 Driver（并承担协议层串行化）。
//!
//! # 结果分层
//!
//! 执行器处于 Driver 边界，返回**原始结果**（`RawWriteResult` /
//! `RawCommandResult`）；引擎负责把原始结果映射为语义
//! `ControlPayloadResult`（`PropertyWriteItemResult` / `CommandResult`，
//! §80.1），并附加语义路径（引擎校验阶段已记录）。
//!
//! # 会话串行化（§82、§88）
//!
//! 引擎保证**同设备控制请求串行执行**（§87 每设备队列）；执行器实现应基于
//! Device Instance 共享的 `Arc<Mutex<Box<dyn PollDriver>>>`（§72 设备实例
//! 边界）调用写/执行，与轮询读取共用同一 Driver 会话互斥，避免读写并发
//! 破坏协议状态（§82 最后一段）。驱动适配层由上层（Device Manager）实现，
//! 本 crate 只定义契约。
//!
//! # 超时 / 取消 / Indeterminate（§80.1）
//!
//! 引擎以三路 select（截止时间 / 取消令牌 / 执行器结果）包裹执行器调用。
//! 执行器一旦开始，超时/取消即视为"结果不确定"（驱动可能已下发控制但未
//! 返回）——引擎以 `Indeterminate` 结算，禁止上层盲目重放；只有排队期间
//! 已过期/已取消（执行器从未被轮询）才结算为 `Timeout`/`Cancelled`。
//! 执行器若自身确认请求**已下发但结果无法确认**（如设备无应答且无法安全
//! 查询），应返回 `Indeterminate`。

use async_trait::async_trait;
use driver_sdk::{
    DriverCommand, DriverErrorInfo, DriverWriteItem, RawCommandResult, RawWriteResult,
};
use observation_model::DeviceId;

/// 批量写入执行结果（Driver 边界，原始结果）。
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOutcome {
    /// 成功：逐项原始结果（`item_id` 对应入参 `item.id`）。
    Succeeded(Vec<RawWriteResult>),
    /// 明确失败。
    Failed(DriverErrorInfo),
    /// 已下发但结果不确定（§80.1）。
    Indeterminate(DriverErrorInfo),
}

/// 命令执行结果（Driver 边界，原始结果）。
#[derive(Debug, Clone, PartialEq)]
pub enum ExecuteOutcome {
    Succeeded(RawCommandResult),
    Failed(DriverErrorInfo),
    Indeterminate(DriverErrorInfo),
}

/// 控制执行器抽象（§88 Command Worker）。
#[async_trait]
pub trait ControlExecutor: Send + Sync {
    /// 批量写入（§75.1 → Profile 映射后的 `DriverWriteItem`）。
    async fn write(&self, device_id: &DeviceId, items: &[DriverWriteItem]) -> WriteOutcome;

    /// 命令执行（§76 → Profile 映射后的 `DriverCommand`）。
    async fn execute(&self, device_id: &DeviceId, command: &DriverCommand) -> ExecuteOutcome;
}
