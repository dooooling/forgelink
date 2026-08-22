//! 设备 Driver 会话：读 + 写 + 命令的统一会话契约与互斥（§72、§82）。
//!
//! # 会话串行化（§82 最后一段）
//!
//! "对于设备协议本身只能串行通信的情况，Control Queue 与 Read Scheduler
//! 最终还要进入 Driver Session Scheduler，避免读写并发破坏协议状态。"
//!
//! 本模块把该约束落到 Core 侧：
//!
//! - 每设备一个 [`DriverSession`] 实例（一条底层连接/一份协议状态），
//!   由 [`SharedSession`]（`Arc<Mutex<..>>`）持有；
//! - Poll Engine 的读取经 [`SessionPollHandle`] 进入**同一把**会话锁，
//!   Control Engine 的写入/命令经 `DeviceInstance::session` 进入同一把锁；
//! - 读写因此互斥且共用同一条连接，不存在"两条连接各自串行"的伪互斥。
//!
//! # 与 poll-engine 的关系
//!
//! [`poll_engine::PollDriver`] 只声明读取（§22 批量读取），写入/命令能力在
//! driver-loader 的 ABI v1 `write` / `execute` 入口（§15）。本模块提供完整
//! 会话视图 [`DriverSession`]，并用 [`SessionPollHandle`] 把它降维为 Poll
//! Engine 需要的只读视图，避免为控制链路另开驱动实例。

use std::sync::{Arc, Mutex, MutexGuard};

use driver_loader::{LoaderError, NativeDriver};
use driver_sdk::{
    DriverCommand, DriverErrorInfo, DriverReadItem, DriverWriteItem, RawCommandResult,
    RawReadResult, RawWriteResult,
};
use poll_engine::PollDriver;

/// 共享设备会话句柄：每设备一个，读/写/命令全部经它串行化（§82）。
///
/// 锁为 `std::sync::Mutex`：被保护的驱动调用是同步阻塞 FFI（§17.5 句柄非
/// 并发安全），持锁方在专用阻塞线程上执行（poll-engine `spawn_blocking` /
/// 控制执行器 `spawn_blocking`），异步任务本身不持锁等待。
pub type SharedSession = Arc<Mutex<Box<dyn DriverSession>>>;

/// 设备 Driver 会话契约：同一底层连接上的批量读取 + 批量写入 + 命令执行。
///
/// # 调用约定（§17.5）
///
/// 实现默认非并发安全；调用方必须通过同一把互斥锁（[`SharedSession`]）
/// 串行化调用。方法均为同步阻塞：超时由实现内部约束（如 Modbus
/// `timeout_ms`），调用方负责阻塞隔离与整体超时。
///
/// # 结果边界（§15）
///
/// 返回原始结果（`RawReadResult` / `RawWriteResult` / `RawCommandResult`），
/// 保留逐项错误与质量信息；语义归一化属于 Profile + Domain（§37.1），
/// 控制语义结算属于 Control Engine（§80.1）。
pub trait DriverSession: Send {
    /// 批量读取（§15 `read`）。整体失败（连接/超时）返回 `Err`，
    /// 单项失败以 `RawReadResult.error` 表达。
    fn read_batch(
        &mut self,
        items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo>;

    /// 批量写入（§15 `write`）。整体失败（传输级）返回 `Err`，
    /// 协议级失败（如从站异常拒绝）以逐项 `success = false` 表达。
    fn write_batch(
        &mut self,
        items: &[DriverWriteItem],
    ) -> Result<Vec<RawWriteResult>, DriverErrorInfo>;

    /// 命令执行（§15 `execute`）。`Ok` 表示调用完成且结果确定
    /// （`success = false` 为设备明确拒绝）；`Err` 为调用级失败。
    fn execute_command(
        &mut self,
        command: &DriverCommand,
    ) -> Result<RawCommandResult, DriverErrorInfo>;
}

/// 基于 Native Plugin（C ABI v1）的完整会话实现（§19、§20）。
///
/// 直接包装 [`NativeDriver`]，把其 `read` / `write` / `execute` 三个同步
/// 入口统一暴露为 [`DriverSession`]——与 poll-engine 的 `NativeDriverAdapter`
/// （仅读取）相比，本类型是控制链路可写的完整视图。
pub struct NativeSessionDriver {
    driver: NativeDriver,
}

impl NativeSessionDriver {
    /// 包装一个已创建句柄的 Native Driver（`NativeDriver::create`）。
    pub fn new(driver: NativeDriver) -> Self {
        Self { driver }
    }
}

impl DriverSession for NativeSessionDriver {
    fn read_batch(
        &mut self,
        items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo> {
        self.driver.read(items).map_err(map_loader_error)
    }

    fn write_batch(
        &mut self,
        items: &[DriverWriteItem],
    ) -> Result<Vec<RawWriteResult>, DriverErrorInfo> {
        self.driver.write(items).map_err(map_loader_error)
    }

    fn execute_command(
        &mut self,
        command: &DriverCommand,
    ) -> Result<RawCommandResult, DriverErrorInfo> {
        self.driver.execute(command).map_err(map_loader_error)
    }
}

/// Loader 错误 → `DriverErrorInfo`。
///
/// 映射规则与 poll-engine `NativeDriverAdapter` 保持一致（读写两条路径的
/// 错误语义必须相同，上层才能按同一套错误码处理）：
///
/// - `CallFailed` 且 Plugin 提供了 `get_last_error_json` 详情：原样保留
///   Driver 的错误码、协议码与可重试标记，不覆盖原始语义（§17.6）；
/// - 无详情的调用失败：稳定错误码 `driver_call_failed`，连接类错误保守
///   标记可重试；
/// - 加载/ABI/契约/配置类错误为永久错误，重试无意义。
fn map_loader_error(error: LoaderError) -> DriverErrorInfo {
    match error {
        LoaderError::CallFailed {
            detail: Some(info), ..
        } => info,
        LoaderError::CallFailed { .. } => DriverErrorInfo {
            code: error.code().to_owned(),
            message: error.to_string(),
            protocol_code: None,
            retryable: true,
        },
        other => DriverErrorInfo {
            code: other.code().to_owned(),
            message: other.to_string(),
            protocol_code: None,
            retryable: false,
        },
    }
}

/// Poll Engine 兼容句柄：共享会话的只读视图。
///
/// `read_batch` 经**同一把**会话锁进入驱动——与控制执行器的写入/命令互斥
/// （§82 最后一段）。锁获取采用 poison 恢复（与 poll-engine 读取路径一致）：
/// 驱动 panic 不传播为锁中毒，避免后续读写永久死锁；panic 后的会话状态由
/// Driver 自身管理（Modbus 传输层在下一个请求自动重连，§34.3）。
pub(crate) struct SessionPollHandle {
    session: SharedSession,
}

impl SessionPollHandle {
    /// 包装共享会话（与控制执行器持有同一个 `Arc`）。
    pub(crate) fn new(session: SharedSession) -> Self {
        Self { session }
    }
}

impl PollDriver for SessionPollHandle {
    fn read_batch(
        &mut self,
        items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo> {
        lock_session(&self.session).read_batch(items)
    }
}

/// 获取会话锁（poison 恢复）。
///
/// 与 poll-engine 的读取路径同一策略：`Mutex` 中毒只说明某次驱动调用
/// panic 过，锁本身仍可用；恢复后继续串行化，不放大故障。
pub(crate) fn lock_session(
    session: &Mutex<Box<dyn DriverSession>>,
) -> MutexGuard<'_, Box<dyn DriverSession>> {
    session
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
