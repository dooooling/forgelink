//! 控制执行器适配层（§88 Command Worker 的 Driver 边界实现）。
//!
//! 把 [`control_engine::ControlExecutor`] 契约落到 Device Manager 管理的
//! 真实 Driver 会话上：
//!
//! - **会话路由**：按 `device_id` 查设备注册表，取该设备共享的
//!   [`SharedSession`]；未知设备返回稳定错误码 `device_not_found` 的
//!   `Failed`（不 panic，§80.1 结果模型）。
//! - **会话互斥（§82 最后一段）**：写入/命令与 Poll Engine 的读取共用
//!   同一把会话锁——阻塞 FFI 调用在 `spawn_blocking` 中持锁执行，读写
//!   在协议层严格串行，避免读写并发破坏协议状态。
//! - **超时边界**：引擎以三路 select（截止时间/取消/结果）包裹执行器
//!   （§88）；本层自身不做超时，阻塞时长由 Driver 内部超时约束（如
//!   Modbus `timeout_ms`）。引擎超时/取消放弃等待 Future 后，已在飞行的
//!   阻塞任务继续持锁直到驱动自然返回——同设备下一条控制请求仍会在会话
//!   锁上等待上一动作收尾，不会与未确认的物理动作并发（§82 物理动作
//!   停止契约：取消不保证底层动作已停止，串行化是最后一道防线）。
//! - **结果映射**：Driver 原始结果原样透传（`Succeeded` 携带逐项
//!   success 标志，部分失败由引擎结算 `PARTIAL_WRITE_FAILURE`）；整体
//!   失败按"请求是否可能已下发"映射 `Failed` / `Indeterminate`
//!   （规则见 [`classify_failure`]）。

use std::sync::Arc;

use async_trait::async_trait;
use control_engine::{ControlExecutor, ExecuteOutcome, WriteOutcome};
use driver_sdk::{DriverCommand, DriverErrorInfo, DriverWriteItem};
use observation_model::DeviceId;
use tracing::warn;

use crate::instance::DeviceManager;
use crate::session::{SharedSession, lock_session};

/// 执行器稳定错误码：设备未注册（§6 结构化日志 `error_code` 约定）。
pub const DEVICE_NOT_FOUND_CODE: &str = "device_not_found";

/// 执行器稳定错误码：阻塞任务在飞行中 panic（结果不确定）。
const BLOCKING_PANIC_CODE: &str = "executor_blocking_panicked";

/// 执行器稳定错误码：运行时停机导致调用未发起（确定未下发）。
const RUNTIME_SHUTDOWN_CODE: &str = "executor_runtime_shutdown";

/// 控制执行器：[`ControlExecutor`] 的 Device Manager 适配实现。
///
/// 持有设备注册表的共享句柄；设备经 [`DeviceManager::register_device`]
/// 注册后即可被本执行器路由。克隆成本低（内部仅 `Arc`）。
#[derive(Clone)]
pub struct DeviceControlExecutor {
    devices: Arc<DeviceManager>,
}

impl DeviceControlExecutor {
    /// 基于设备注册表构建执行器。
    pub fn new(devices: Arc<DeviceManager>) -> Self {
        Self { devices }
    }

    /// 按 `device_id` 路由到该设备的共享会话；未注册返回 `None`。
    fn session_of(&self, device_id: &str) -> Option<SharedSession> {
        self.devices.get(device_id).map(|i| i.session.clone())
    }
}

/// 整体失败的可确定性分类（§80.1 `Indeterminate` 判定）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Certainty {
    /// 请求确定未下发（或结果是确定的拒绝）：可安全结算 `Failed`。
    Deterministic,
    /// 请求可能已下发、结果无法确认：必须结算 `Indeterminate`。
    Uncertain,
}

/// 已确认"请求未发往设备"的驱动错误码集合（映射 `Failed`）。
///
/// # 判定原则（§80.1）
///
/// "已发到设备但结果不确定且无法安全查询时标记 `Indeterminate`"；
/// `High/Critical` 的 `Indeterminate` 控制禁止自动重放。反向推理：只有
/// 能**证明请求从未上线**的错误才允许 `Failed`（上层可安全重试）——
/// 误把已生效的写入报成 `Failed` 会诱导上层重放控制动作（危险方向）；
/// 无法证明时一律保守映射 `Indeterminate`（代价仅是设备冷却期与人工
/// 确认，方向安全）。因此未知错误码落入 `Uncertain`。
///
/// 集合来源（driver-modbus `error.rs` 错误分类 + driver-loader 稳定错误码）：
///
/// | 错误码 | 依据 |
/// |---|---|
/// | `connection_failed` | 建连失败，连接未建立，请求不可能上线 |
/// | `unsupported` | 能力未声明，ABI 入口即拒绝，无协议交互（§15） |
/// | `config_error` | 配置非法，请求构造期失败 |
/// | `invalid_address` | 地址解析/规划失败（如只读段、越界），未上线 |
/// | `invalid_type` | 值编码失败（§17.2 Tag 校验），未上线 |
/// | `driver_encoding_error` | Core 侧请求编码失败（Loader），ABI 调用未发生 |
/// | `modbus_exception` | 设备明确负确认（从站异常响应）——结果是确定的 |
///   "拒绝"。正常路径下 Modbus 异常以逐项 `success=false` 返回，此处
///   防御插件将其作为整体错误上报的情形 |
const NOT_SENT_CODES: &[&str] = &[
    "connection_failed",
    "unsupported",
    "config_error",
    "invalid_address",
    "invalid_type",
    "driver_encoding_error",
    "modbus_exception",
];

/// 分类驱动整体失败（§80.1：`Failed` 与 `Indeterminate` 的分界）。
///
/// - 命中 [`NOT_SENT_CODES`] → `Deterministic`（请求未上线或结果确定）；
/// - 其余（`timeout`、`connection_lost`、`invalid_response`、`DRIVER_PANIC`、
///   插件自定义码等）→ `Uncertain`：请求可能已在飞行中，设备侧状态未知，
///   必须以 `Indeterminate` 结算并禁止盲目重放（§80.1）。
fn classify_failure(info: &DriverErrorInfo) -> Certainty {
    if NOT_SENT_CODES.contains(&info.code.as_str()) {
        Certainty::Deterministic
    } else {
        Certainty::Uncertain
    }
}

/// 未知设备的 `Failed` 结果（写入与命令共用，错误码稳定）。
fn device_not_found(device_id: &str) -> DriverErrorInfo {
    DriverErrorInfo {
        code: DEVICE_NOT_FOUND_CODE.to_owned(),
        message: format!("设备 `{device_id}` 未注册，无法执行控制动作"),
        protocol_code: None,
        retryable: false,
    }
}

#[async_trait]
impl ControlExecutor for DeviceControlExecutor {
    /// 批量写入（§75.1 → Profile 映射后的 `DriverWriteItem`）。
    ///
    /// # 结果语义
    ///
    /// - `Succeeded(Vec<RawWriteResult>)`：驱动调用完成且逐项结果确定。
    ///   逐项 `success = false`（如从站异常拒绝）是**确定的负确认**，
    ///   原样透传，部分失败的顶层结算由引擎负责（§80.1）；
    /// - `Failed`：整体失败且可证明请求未上线（见模块文档映射规则）；
    /// - `Indeterminate`：整体失败且请求可能已下发（§80.1）。
    async fn write(&self, device_id: &DeviceId, items: &[DriverWriteItem]) -> WriteOutcome {
        let Some(session) = self.session_of(device_id) else {
            warn!(
                component = "device-manager",
                device_id = %device_id,
                error_code = DEVICE_NOT_FOUND_CODE,
                "控制写入目标设备未注册"
            );
            return WriteOutcome::Failed(device_not_found(device_id));
        };
        let items = items.to_vec();
        // 阻塞 FFI 调用隔离到阻塞线程池（开发规范 §5）；会话锁在调用期间
        // 持有，与 Poll Engine 读取互斥（§82）。引擎超时放弃等待后本任务
        // 脱离（JoinHandle 丢弃）继续执行至驱动自然返回，会话锁保证后续
        // 控制请求串行等待（§82 物理动作停止契约）。
        let outcome =
            tokio::task::spawn_blocking(move || lock_session(&session).write_batch(&items)).await;
        match outcome {
            // 驱动返回逐项原始结果（含逐项失败）：结果是确定的，透传。
            Ok(Ok(results)) => WriteOutcome::Succeeded(results),
            // 驱动整体失败：按"是否可能已下发"分类（§80.1）。
            Ok(Err(info)) => match classify_failure(&info) {
                Certainty::Deterministic => WriteOutcome::Failed(info),
                Certainty::Uncertain => {
                    warn!(
                        component = "device-manager",
                        device_id = %device_id,
                        error_code = %info.code,
                        "控制写入整体失败且结果不确定，结算 Indeterminate"
                    );
                    WriteOutcome::Indeterminate(info)
                }
            },
            // 运行时停机导致阻塞任务未执行：调用未发起，确定未下发。
            Err(error) if error.is_cancelled() => WriteOutcome::Failed(DriverErrorInfo {
                code: RUNTIME_SHUTDOWN_CODE.to_owned(),
                message: format!("设备 `{device_id}` 写入调用未发起：阻塞线程池已关闭"),
                protocol_code: None,
                retryable: false,
            }),
            // 阻塞任务 panic：调用已在飞行中，是否下发无法确认（§80.1）。
            Err(error) => {
                warn!(
                    component = "device-manager",
                    device_id = %device_id,
                    error_code = BLOCKING_PANIC_CODE,
                    "控制写入阻塞任务异常终止，结算 Indeterminate"
                );
                WriteOutcome::Indeterminate(DriverErrorInfo {
                    code: BLOCKING_PANIC_CODE.to_owned(),
                    message: format!("设备 `{device_id}` 写入阻塞任务异常终止：{error}"),
                    protocol_code: None,
                    retryable: false,
                })
            }
        }
    }

    /// 命令执行（§76 → Profile 映射后的 `DriverCommand`）。
    ///
    /// # 结果语义
    ///
    /// - `Succeeded(RawCommandResult)`：调用完成且结果确定（`success = false`
    ///   为设备明确拒绝，由引擎映射为语义失败）；
    /// - `Failed` / `Indeterminate`：与 [`ControlExecutor::write`] 同一规则。
    async fn execute(&self, device_id: &DeviceId, command: &DriverCommand) -> ExecuteOutcome {
        let Some(session) = self.session_of(device_id) else {
            warn!(
                component = "device-manager",
                device_id = %device_id,
                error_code = DEVICE_NOT_FOUND_CODE,
                "控制命令目标设备未注册"
            );
            return ExecuteOutcome::Failed(device_not_found(device_id));
        };
        let command = command.clone();
        // 会话互斥与阻塞隔离依据同 [`ControlExecutor::write`]（§82）。
        let outcome =
            tokio::task::spawn_blocking(move || lock_session(&session).execute_command(&command))
                .await;
        match outcome {
            // 调用完成且结果确定（含设备明确拒绝 success=false）。
            Ok(Ok(result)) => ExecuteOutcome::Succeeded(result),
            // 调用级失败：按"是否可能已下发"分类（§80.1）。
            Ok(Err(info)) => match classify_failure(&info) {
                Certainty::Deterministic => ExecuteOutcome::Failed(info),
                Certainty::Uncertain => {
                    warn!(
                        component = "device-manager",
                        device_id = %device_id,
                        error_code = %info.code,
                        "控制命令整体失败且结果不确定，结算 Indeterminate"
                    );
                    ExecuteOutcome::Indeterminate(info)
                }
            },
            // 运行时停机导致阻塞任务未执行：确定未下发。
            Err(error) if error.is_cancelled() => ExecuteOutcome::Failed(DriverErrorInfo {
                code: RUNTIME_SHUTDOWN_CODE.to_owned(),
                message: format!("设备 `{device_id}` 命令调用未发起：阻塞线程池已关闭"),
                protocol_code: None,
                retryable: false,
            }),
            // 阻塞任务 panic：结果无法确认（§80.1）。
            Err(error) => {
                warn!(
                    component = "device-manager",
                    device_id = %device_id,
                    error_code = BLOCKING_PANIC_CODE,
                    "控制命令阻塞任务异常终止，结算 Indeterminate"
                );
                ExecuteOutcome::Indeterminate(DriverErrorInfo {
                    code: BLOCKING_PANIC_CODE.to_owned(),
                    message: format!("设备 `{device_id}` 命令阻塞任务异常终止：{error}"),
                    protocol_code: None,
                    retryable: false,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use driver_sdk::{RawCommandResult, RawWriteResult};

    use super::*;

    /// 已确认未上线的错误码必须分类为确定性失败（可安全结算 Failed）。
    #[test]
    fn classifies_not_sent_codes_as_deterministic() {
        for code in NOT_SENT_CODES {
            let info = DriverErrorInfo {
                code: (*code).to_owned(),
                message: "测试".to_owned(),
                protocol_code: None,
                retryable: false,
            };
            assert_eq!(
                classify_failure(&info),
                Certainty::Deterministic,
                "`{code}` 应为确定性失败"
            );
        }
    }

    /// 传输级不确定错误与插件自定义码必须保守分类为不确定（§80.1）。
    #[test]
    fn classifies_uncertain_codes_conservatively() {
        for code in [
            "timeout",
            "connection_lost",
            "invalid_response",
            "decode_error",
            "DRIVER_PANIC",
            "driver_call_failed",
            "vendor_custom_error",
        ] {
            let info = DriverErrorInfo {
                code: code.to_owned(),
                message: "测试".to_owned(),
                protocol_code: None,
                retryable: true,
            };
            assert_eq!(
                classify_failure(&info),
                Certainty::Uncertain,
                "`{code}` 应保守映射为不确定"
            );
        }
    }

    /// 空设备表路由：未注册设备返回 None（不 panic）。
    #[test]
    fn session_of_unknown_device_is_none() {
        let manager = Arc::new(
            DeviceManager::new(
                profile_engine::ProfileRegistry::new(),
                Box::new(crate::bind::NativeDriverFactory::new()),
                1000,
            )
            .expect("默认间隔合法"),
        );
        let executor = DeviceControlExecutor::new(manager);
        assert!(executor.session_of("ghost").is_none());
    }

    /// `Arc<Mutex<..>>` 类型别名与锁获取的编译期约定（poison 恢复不 panic）。
    #[test]
    fn lock_session_recovers_from_poison() {
        let session: SharedSession = Arc::new(Mutex::new(Box::new(PoisonSession)));
        // 制造中毒：持锁期间 panic。
        let clone = Arc::clone(&session);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = lock_session(&clone);
            panic!("制造锁中毒");
        }));
        // poison 恢复：后续获取不 panic、不死锁。
        let _guard = lock_session(&session);
    }

    /// 触发锁中毒用的空会话。
    struct PoisonSession;

    impl crate::session::DriverSession for PoisonSession {
        fn read_batch(
            &mut self,
            _items: &[driver_sdk::DriverReadItem],
        ) -> Result<Vec<driver_sdk::RawReadResult>, DriverErrorInfo> {
            Ok(vec![])
        }

        fn write_batch(
            &mut self,
            _items: &[DriverWriteItem],
        ) -> Result<Vec<RawWriteResult>, DriverErrorInfo> {
            Ok(vec![])
        }

        fn execute_command(
            &mut self,
            _command: &DriverCommand,
        ) -> Result<RawCommandResult, DriverErrorInfo> {
            Ok(RawCommandResult {
                success: true,
                protocol_code: None,
                payload: None,
                error: None,
            })
        }
    }
}
