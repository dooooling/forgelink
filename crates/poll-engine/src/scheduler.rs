//! 轮询调度器（§22 Poll Scheduler）：统一管理设备轮询任务与停机。

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::{PollConfig, PollConfigError};
use crate::driver::PollDriver;
use crate::events::PollEvent;
use crate::poll::{PollTarget, poll_loop};

/// 轮询调度器。
///
/// 持有根取消令牌与全部设备任务句柄；[`PollScheduler::shutdown`] 统一取消并等待所有
/// 任务退出。同一驱动实例被多个目标共享时，由调用方用
/// `Arc<Mutex<Box<dyn PollDriver>>>` 包装以串行化访问。
#[derive(Default)]
pub struct PollScheduler {
    cancel: Option<CancellationToken>,
    tasks: Vec<JoinHandle<()>>,
}

impl PollScheduler {
    /// 创建一个空调度器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 启动一个设备轮询任务（每设备每周期一组，对应 §22 的 Group）。
    ///
    /// 启动前校验配置（`interval_ms`、超时与退避参数必须为正），校验失败返回
    /// [`PollConfigError`] 且不创建任务。
    pub fn spawn(
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
        let handle = tokio::spawn(poll_loop(target, driver, config, cancel, events));
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
}
