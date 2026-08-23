//! 设备轮询任务：周期调度、阻塞隔离、超时、指数退避重试与取消（§22、§34.3）。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diagnostics::redact;
use driver_sdk::{DriverErrorInfo, DriverReadItem};
use observation_model::RawReadResult;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::PollConfig;
use crate::driver::PollDriver;
use crate::events::{PollBatch, PollEvent};
use crate::metrics::PollMetrics;

/// 请求超时错误码（§17.6 标准错误类别：`driver_*` 前缀）。
pub(crate) const ERROR_TIMEOUT_CODE: &str = "driver_request_timeout";

/// 驱动阻塞调用 panic 的错误码。
const ERROR_PANIC_CODE: &str = "driver_read_panicked";

/// 单周期轮询目标（§22 Group：同一采集周期的属性集合）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollTarget {
    /// 设备 ID。
    pub device_id: String,
    /// 采集周期（毫秒），必须大于 0（`PollScheduler::spawn` 校验）。
    pub interval_ms: u64,
    /// 批量读取项；`DriverReadItem.id` 由调用方分配，结果经 `RawReadResult.item_id` 关联。
    pub items: Vec<DriverReadItem>,
}

impl PollTarget {
    /// 校验目标配置（`PollScheduler::spawn` 启动前调用）。
    pub fn validate(&self) -> Result<(), crate::config::PollConfigError> {
        if self.interval_ms == 0 {
            return Err(crate::config::PollConfigError::InvalidInterval);
        }
        Ok(())
    }
}

/// 单设备轮询任务。
///
/// - 每设备一个任务，设备间互不阻塞（§34.3）；
/// - 同步驱动调用在 `spawn_blocking` 上执行，隔离阻塞、不占运行时线程；
///   同一时刻最多一个阻塞调用在途（单飞行），超时不会堆积线程池任务；
/// - 单次请求受 `PollConfig::request_timeout` 约束，超时丢弃本次结果；
/// - 可重试错误按指数退避重试（§34.3），永久错误回到周期节律；
///   成功批次原样下发（`RawReadResult` 的错误与质量信息保留，语义归一化属于
///   Profile + Domain）；
/// - 事件通道关闭或取消令牌触发时任务退出（发送事件同样响应取消，停机不被
///   有界通道阻塞）；
/// - 退出前等待最后在途的阻塞调用收尾（受 `shutdown_drain_timeout` 上限约束，
///   超时记录告警并报告未完成回收，不无限阻塞停机）；
/// - 本函数为公开 API：入口处再次校验配置（`interval_ms` 为 0 等非法值仅告警并
///   返回，不触发 Tokio panic）。
///
/// `metrics`：本任务的指标句柄集合（§34.2.1）；未注入时全部 no-op。
pub async fn poll_loop(
    target: PollTarget,
    driver: Arc<Mutex<Box<dyn PollDriver>>>,
    config: PollConfig,
    cancel: CancellationToken,
    events: mpsc::Sender<PollEvent>,
    metrics: PollMetrics,
) {
    // 防御性校验：公开入口不应能被非法配置触发 panic 或空转（§34）。
    if let Err(error) = target.validate().and_then(|_| config.validate()) {
        warn!(
            component = "poll-engine",
            device_id = %target.device_id,
            interval_ms = target.interval_ms,
            error_code = "poll_config_invalid",
            error = %error,
            "轮询配置非法，任务不启动"
        );
        return;
    }

    info!(
        component = "poll-engine",
        device_id = %target.device_id,
        interval_ms = target.interval_ms,
        batch_size = target.items.len(),
        "设备轮询任务启动"
    );

    let mut call = BatchCall::default();
    run_loop(
        &target, &driver, &config, &cancel, &events, &mut call, &metrics,
    )
    .await;

    // 有序停机：取消令牌已触发（或通道关闭），等待最后在途的阻塞调用完成
    // （受 `shutdown_drain_timeout` 上限约束，超时记录告警并按未完成回收）。
    call.drain(&target, &config).await;
}

/// 轮询主循环（退出路径统一收敛，由 [`poll_loop`] 负责停机收尾）。
async fn run_loop(
    target: &PollTarget,
    driver: &Arc<Mutex<Box<dyn PollDriver>>>,
    config: &PollConfig,
    cancel: &CancellationToken,
    events: &mpsc::Sender<PollEvent>,
    call: &mut BatchCall,
    metrics: &PollMetrics,
) {
    // 错过的 tick 不补发（§22/§34 有界负载：读取耗时或退避期间不产生补偿式突发）。
    let mut ticker = tokio::time::interval(Duration::from_millis(target.interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut backoff = config.backoff();
    // 首个 tick 立即触发（interval 语义），计划时刻即任务启动时刻，无
    // 有意义的调度偏差——**只跳过观测，不跳过采集**（评审 P1 回归修复：
    // 此前 `continue` 跳过了整个首轮 call.run()，低频周期设备重启后会
    // 白等一个完整 interval 才首次采集）。
    let mut first_tick = true;

    loop {
        // 正常节奏：等待周期 tick。调度偏差观测（§34.2.1）：`tick()` 返回
        // 值是该次 tick 的**计划时刻**（scheduled），实际唤醒时刻与其差值
        // 即调度延迟（评审 P1：此前用"本轮开始前预写的 planned"计算，
        // 测的是上一轮处理耗时/剩余 slack，不是唤醒偏差，p99 数据不可信）。
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(component = "poll-engine", device_id = %target.device_id, "设备轮询任务已取消");
                break;
            }
            scheduled = ticker.tick() => {
                if !first_tick {
                    // 调度延迟 = 实际唤醒 − 计划时刻（唤醒晚于计划为正偏差）。
                    let late =
                        tokio::time::Instant::now().saturating_duration_since(scheduled);
                    if let Some(hist) = metrics.schedule_delay_ns.as_ref() {
                        hist.observe_ns(late.as_nanos() as u64);
                    }
                }
                first_tick = false;
            }
        }

        match call.run(target, driver, config, cancel, metrics).await {
            Ok(outcome) => {
                backoff.reset();
                if !send_batch(target, events, cancel, outcome, metrics).await {
                    break;
                }
            }
            Err(TaskError::Cancelled) => {
                info!(component = "poll-engine", device_id = %target.device_id, "设备轮询任务已取消");
                break;
            }
            Err(TaskError::Driver(error)) => {
                metrics.observe_error(&error.retryable, &error.code);
                let retryable = error.retryable;
                if !send_failure(target, events, cancel, &error).await {
                    break;
                }
                // 仅可重试错误进入指数退避重试（§34.3）；永久错误（配置/ABI/契约）
                // 回到周期节律，避免高频请求与重复告警。
                if !retryable {
                    continue;
                }
                loop {
                    let wait = backoff.next();
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            info!(component = "poll-engine", device_id = %target.device_id, "设备轮询任务已取消");
                            return;
                        }
                        _ = tokio::time::sleep(wait) => {}
                    }
                    match call.run(target, driver, config, cancel, metrics).await {
                        Ok(outcome) => {
                            backoff.reset();
                            if !send_batch(target, events, cancel, outcome, metrics).await {
                                return;
                            }
                            break;
                        }
                        Err(TaskError::Cancelled) => {
                            info!(component = "poll-engine", device_id = %target.device_id, "设备轮询任务已取消");
                            return;
                        }
                        Err(TaskError::Driver(error)) => {
                            metrics.observe_error(&error.retryable, &error.code);
                            if !send_failure(target, events, cancel, &error).await {
                                return;
                            }
                            if !error.retryable {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// 轮询任务内部错误：驱动错误与取消。
enum TaskError {
    /// 驱动/超时/panic 错误。
    Driver(DriverErrorInfo),
    /// 取消令牌触发（任务应退出）。
    Cancelled,
}

/// 发送成功批次事件；取消或通道关闭时返回 `false`（任务应退出）。
async fn send_batch(
    target: &PollTarget,
    events: &mpsc::Sender<PollEvent>,
    cancel: &CancellationToken,
    outcome: BatchOutcome,
    metrics: &PollMetrics,
) -> bool {
    if let Some(counter) = metrics.batches_total.as_ref() {
        counter.inc();
    }
    let elapsed_ms = (outcome.elapsed_ns / 1_000_000) as u64;
    debug!(
        component = "poll-engine",
        device_id = %target.device_id,
        batch_size = outcome.results.len(),
        elapsed_ms,
        "设备批量读取成功"
    );
    let event = PollEvent::Batch(PollBatch {
        device_id: target.device_id.clone(),
        interval_ms: target.interval_ms,
        results: outcome.results,
        elapsed_ms,
    });
    tokio::select! {
        _ = cancel.cancelled() => false,
        result = events.send(event) => {
            if result.is_err() {
                warn!(component = "poll-engine", device_id = %target.device_id, "事件通道已关闭，轮询任务退出");
                false
            } else {
                true
            }
        }
    }
}

/// 发送失败事件并记录结构化日志（消息经 `diagnostics::redact` 脱敏）；
/// 取消或通道关闭时返回 `false`（任务应退出）。
async fn send_failure(
    target: &PollTarget,
    events: &mpsc::Sender<PollEvent>,
    cancel: &CancellationToken,
    error: &DriverErrorInfo,
) -> bool {
    warn!(
        component = "poll-engine",
        device_id = %target.device_id,
        interval_ms = target.interval_ms,
        error_code = %error.code,
        retryable = error.retryable,
        error = %redact(&error.message),
        "设备批量读取失败，按指数退避重试"
    );
    let event = PollEvent::Failed {
        device_id: target.device_id.clone(),
        interval_ms: target.interval_ms,
        items: target.items.clone(),
        error: error.clone(),
    };
    tokio::select! {
        _ = cancel.cancelled() => false,
        result = events.send(event) => {
            if result.is_err() {
                warn!(component = "poll-engine", device_id = %target.device_id, "事件通道已关闭，轮询任务退出");
                false
            } else {
                true
            }
        }
    }
}

/// 批次执行结果（成功时同时携带原始结果与耗时）。
struct BatchOutcome {
    results: Vec<RawReadResult>,
    elapsed_ns: u128,
}

/// 单飞行的批量读取调用：同一时刻最多一个 `spawn_blocking` 任务在途。
///
/// 超时只放弃等待，不终止已在运行的阻塞任务；其 `JoinHandle` 保留在 `slot` 中，
/// 下一次 [`BatchCall::run`] 先收尾该调用（结果已作废，仅回收线程），再发起新调用，
/// 避免超时后持续堆积阻塞任务（P1）。
#[derive(Default)]
struct BatchCall {
    slot: Option<JoinHandle<Result<Vec<RawReadResult>, DriverErrorInfo>>>,
}

impl BatchCall {
    /// 停机收尾：在 `PollConfig::shutdown_drain_timeout` 上限内等待最后在途的
    /// 阻塞调用完成（有序停机，取消令牌已触发后不再发起新调用）。
    ///
    /// 超时表示驱动永久阻塞（阻塞线程无法安全终止）：记录告警后返回，不再无限
    /// 等待；驱动句柄由阻塞调用闭包内的 `Arc` 引用持有，直到线程自然结束才释放，
    /// 不会提前 `destroy` 正在使用的句柄。
    async fn drain(&mut self, target: &PollTarget, config: &PollConfig) {
        if let Some(handle) = self.slot.take()
            && tokio::time::timeout(config.shutdown_drain_timeout, handle)
                .await
                .is_err()
        {
            warn!(
                component = "poll-engine",
                device_id = %target.device_id,
                error_code = "poll_drain_timeout",
                shutdown_drain_timeout_ms = config.shutdown_drain_timeout.as_millis(),
                "停机收尾超时：在途驱动调用未能在期限内完成，线程将在自然结束后回收"
            );
        }
    }

    /// 执行一次批量读取：`spawn_blocking` 隔离 + `request_timeout` 超时。
    ///
    /// 每次调用收尾时观测设备请求往返延迟（§34.2 必录「设备请求延迟」）：
    /// 成功、驱动错误、panic 与超时（按超时上限截断）均计入同一分布。
    async fn run(
        &mut self,
        target: &PollTarget,
        driver: &Arc<Mutex<Box<dyn PollDriver>>>,
        config: &PollConfig,
        cancel: &CancellationToken,
        metrics: &PollMetrics,
    ) -> Result<BatchOutcome, TaskError> {
        // 1. 收尾上一次（可能超时的）在途调用，期间响应取消；结果已作废。
        if let Some(handle) = &mut self.slot {
            if !handle.is_finished() {
                tokio::select! {
                    _ = cancel.cancelled() => return Err(TaskError::Cancelled),
                    _ = handle => {}
                }
            }
            self.slot = None;
        }

        // 2. 发起新调用。
        let started = Instant::now();
        let lock = Arc::clone(driver);
        let items = target.items.clone();
        let item_count = items.len();
        let mut handle = tokio::task::spawn_blocking(move || {
            let mut guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.read_batch(&items)
        });

        // 等待结果收尾（成功/错误/panic/超时四出口统一在此之后观测一次
        // 完整往返耗时，避免遗漏分支）。
        let result = tokio::time::timeout(config.request_timeout, &mut handle).await;
        metrics.observe_request_latency(started.elapsed());
        match result {
            // 驱动返回整体错误。
            Ok(Ok(Err(error))) => Err(TaskError::Driver(error)),
            // 驱动调用 panic（跨 FFI 已收口，此为防御）。
            Ok(Err(join_error)) => Err(TaskError::Driver(DriverErrorInfo {
                code: ERROR_PANIC_CODE.to_owned(),
                message: join_error.to_string(),
                protocol_code: None,
                retryable: true,
            })),
            // 成功批次。
            Ok(Ok(Ok(results))) => Ok(BatchOutcome {
                results,
                elapsed_ns: started.elapsed().as_nanos(),
            }),
            // 超时：结果丢弃，阻塞线程自然结束；保留 JoinHandle 由下一次 run 收尾。
            Err(_elapsed) => {
                self.slot = Some(handle);
                Err(TaskError::Driver(DriverErrorInfo {
                    code: ERROR_TIMEOUT_CODE.to_owned(),
                    message: format!(
                        "批量读取超过 {}ms 超时上限（{item_count} items）",
                        config.request_timeout.as_millis()
                    ),
                    protocol_code: None,
                    retryable: true,
                }))
            }
        }
    }
}
