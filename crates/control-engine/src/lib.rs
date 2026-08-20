//! Control Engine（§81-§90 Normative）。
//!
//! 北向控制请求（REST / MQTT / OPC UA / Web UI）不能直接进入 Driver，
//! 必须统一经过本引擎：认证 → 授权 → 校验 → 策略/前置条件 → 每设备队列 →
//! Profile 映射 → Driver（§81）。
//!
//! 本 crate 直接复用 `observation-model` 的 `ControlRequest` / `ControlResult`
//! （§80.1），不新增同义模型；`PropertyWriteRequest` / `CommandRequest` 只是
//! 顶层信封的领域 payload（§76）。
//!
//! # 核心接口
//!
//! - [`ControlEngine`]：统一入口（提交 / 取消 / 查询 / 停机）；
//! - [`ControlExecutor`]：Driver 执行抽象（写 / 命令执行）；
//! - [`ControlJournal`]：幂等 Journal（§80.1 幂等键 `(namespace, device_id, request_id)`）；
//! - [`Authorizer`]：可替换的权限授权器（§83 viewer/operator/engineer/administrator）；
//! - [`ControlPolicy`]：策略与风险级别配置（§86）。
//!
//! # 生命周期（§77、§80.1）
//!
//! ```text
//! Accepted → Running → Succeeded / Failed / Timeout / Cancelled / Indeterminate
//! ```
//!
//! 校验/授权/入队失败在 Driver 前以 `Rejected` 结算（§84）。
//!
//! # 与 Driver 的边界（§74、§75.3、§81）
//!
//! - 属性写入：Profile 逆变换（`profile_engine::convert::encode_write`）产生
//!   `DriverWriteItem`，`address` 来自 Profile（`ProfileProperty.driver_address`），
//!   Core / 本引擎不解析 Driver 地址（§10）。
//! - 命令：Profile 把标准业务命令映射为 `DriverCommand`（`ProfileCommand.driver_command_id`）。
//! - 本引擎通过 [`ControlExecutor`] 调用 Driver；实际驱动适配层（Device Instance
//!   的 `Arc<Mutex<Box<dyn PollDriver>>>` 会话串行化，§88）由上层实现，
//!   引擎保证同设备控制请求串行执行（§87）。

mod audit;
mod catalog;
mod engine;
#[cfg(test)]
mod engine_tests;
mod executor;
mod journal;
mod policy;
mod precondition;
mod queue;
mod role;
mod validate;

pub use audit::{
    AuditEvent, AuditOperation, AuditParameter, AuditSink, MemoryAuditSink, NoopAuditSink,
};
pub use catalog::{DeviceCatalog, DeviceInfo, MemoryDeviceCatalog};
pub use engine::{
    CancelError, ControlEngine, ControlEngineConfig, ControlReceipt, SubmitContext, SubmitError,
};
pub use executor::{ControlExecutor, ExecuteOutcome, WriteOutcome};
pub use journal::{
    FileJournal, IdempotencyKey, InMemoryJournal, JournalDecision, JournalEntry, JournalError,
    payload_hash,
};
pub use policy::{
    ALL_RISK_LEVELS, CommandPriority, ControlPolicy, OperationKind, Priority, risk_default_role,
};
pub use precondition::{
    PatternPreconditionChecker, PermissivePreconditionChecker, PreconditionChecker,
    PreconditionError,
};
pub use role::{AuthorizationError, Authorizer, MemoryAuthorizer, Role, role_ordering};
pub use validate::{
    ValidatedOperation, ValidationError, map_driver_error, validate_command,
    validate_property_write,
};
