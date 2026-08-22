//! 轮询调度器（§22 Poll Scheduler）：统一管理设备轮询任务与停机。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::{PollConfig, PollConfigError};
use crate::driver::PollDriver;
use crate::events::PollEvent;
use crate::metrics::PollMetrics;
use crate::poll::{PollTarget, poll_loop};

/// 轮询调度器。
///
/// 持有根取消令牌与全部设备任务句柄；[`PollScheduler::shutdown`] 统一取消并等待所有
/// 任务退出。同一驱动实例被多个目标共享时，由调用方用
/// `Arc<Mutex<Box<dyn PollDriver>>>` 包装以串行化访问。
///
/// 指标埋点（§34.2.1）：[`PollScheduler::spawn_with_metrics`] 注入
/// `Arc<MetricsRegistry>` 后，本调度器启动的全部轮询任务共享同一组句柄；
/// 未注入（[`PollScheduler::spawn`]）时不埋点——热路径仅一次 `Option`
/// 判断，零额外开销。
#[derive(Default)]
pub struct PollScheduler {
    cancel: Option<CancellationToken>,
    tasks: Vec<JoinHandle<()>>,
    metrics: PollMetrics,
}

impl PollScheduler {
    /// 创建一个空调度器（不埋点）。
    pub fn new() -> Self {
        Self {
            cancel: None,
            tasks: Vec::new(),
            metrics: PollMetrics::NOOP,
        }
    }

    /// 创建一个带指标埋点的调度器：全部后续 [`Self::spawn`] 的任务共用
    /// 同一组指标句柄（注册幂等；重复注册返回同一底层单元格）。
    pub fn with_metrics(registry: Arc<metrics::MetricsRegistry>) -> Self {
        Self {
            cancel: None,
            tasks: Vec::new(),
            metrics: PollMetrics::new(Some(&registry)),
        }
    }

    /// 启动一个设备轮询任务（每设备每周期一组，对应 §22 的 Group）。
    ///
    /// 启动前校验配置（`interval_ms`、超时与退避参数必须为正），校验失败返回
    /// [`PollConfigError`] 且不创建任务。等价于不埋点的
    /// [`Self::spawn_with_metrics`]。
    pub fn spawn(
        &mut self,
        target: PollTarget,
        driver: Arc<Mutex<Box<dyn PollDriver>>>,
        config: PollConfig,
        events: mpsc::Sender<PollEvent>,
    ) -> Result<(), PollConfigError> {
        self.spawn_with_metrics(target, driver, config, events)
    }

    /// 启动一个设备轮询任务并携带本调度器的指标句柄（§34.2.1）。
    pub fn spawn_with_metrics(
        &mut self,
        target: PollTarget,
        driver: Arc<Mutex<Box<dyn PollDriver>>>,
        config: PollConfig,
        events: mpsc::Sender<PollEvent>,
    ) -> Result<(), PollConfigError> {
        target.validate()?;
        config.validate()?;
        let root = self.cancel.get_or_insert_with(CancellationToken::new);
        let cancel = root.child_token();
        let handle = tokio::spawn(poll_loop(
            target,
            driver,
            config,
            cancel,
            events,
            self.metrics.clone(),
        ));
        self.tasks.push(handle);
        Ok(())
    }

    /// 当前设备任务数。
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// 取消全部任务并等待它们结束。
    pub async fn shutdown(&mut self) {
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
            for task in self.tasks.drain(..) {
                if let Err(error) = task.await {
                    warn!(
                        component = "poll-engine",
                        error_code = "poll_task_panicked",
                        error = %error,
                        "轮询任务异常退出"
                    );
                }
            }
            self.cancel = None;
        }
    }

    /// 取消全部任务并等待它们结束；所有任务共享**统一截止时间** `grace`
    /// （评审 P1：按任务分别等待会使总耗时随任务数线性放大——任务数 ×
    /// grace，失败路径清理仍可能长时间阻塞）。截止时间前等待全部任务
    /// 自然结束，超时则一次性 `abort` 剩余全部任务，总等待 ≈ grace。
    ///
    /// 实现上直接持有原始 `JoinHandle`（评审 P1：`JoinSet` 包装任务被
    /// `abort_all` 取消时只会丢弃内部句柄，而丢弃句柄**不会**取消 Tokio
    /// 任务——底层轮询任务会脱离管理继续运行并遗留后台线程，尤其在
    /// 阻塞 Native Driver 调用或 `shutdown_drain_timeout` 时）。超时必须
    /// 显式对每个原始句柄调用 `abort`。
    pub async fn shutdown_with_timeout(&mut self, grace: Duration) {
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
            let deadline = tokio::time::Instant::now() + grace;
            let mut tasks = self.tasks.drain(..).collect::<Vec<_>>();
            let mut timed_out = false;
            for task in &mut tasks {
                if timed_out {
                    task.abort();
                    let _ = (&mut *task).await;
                    continue;
                }
                match tokio::time::timeout_at(deadline, &mut *task).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        warn!(
                            component = "poll-engine",
                            error_code = "poll_task_panicked",
                            error = %error,
                            "轮询任务异常退出"
                        );
                    }
                    Err(_) => {
                        timed_out = true;
                        warn!(
                            component = "poll-engine",
                            error_code = "poll_task_shutdown_timeout",
                            "轮询任务停机等待超时，强制取消全部剩余任务"
                        );
                        task.abort();
                        let _ = (&mut *task).await;
                    }
                }
            }
            self.cancel = None;
        }
    }
}
