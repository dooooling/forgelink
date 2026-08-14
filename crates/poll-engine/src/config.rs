//! 轮询引擎配置与失败退避策略（§34.3）。

use std::fmt;
use std::time::Duration;

/// 轮询配置校验错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollConfigError {
    /// 采集周期为 0（Tokio interval 会 panic）。
    InvalidInterval,
    /// 请求超时为零（每次都立即超时）。
    InvalidTimeout,
    /// 退避基数或上限为零（高频空转）。
    InvalidBackoff,
    /// 退避倍率为 0（退避恒为 0）。
    InvalidBackoffFactor,
    /// 停机收尾超时为零（立即放弃收尾）。
    InvalidDrainTimeout,
}

impl fmt::Display for PollConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInterval => write!(f, "interval_ms 必须大于 0"),
            Self::InvalidTimeout => write!(f, "request_timeout 必须大于 0"),
            Self::InvalidBackoff => write!(f, "退避基数与上限必须大于 0"),
            Self::InvalidBackoffFactor => write!(f, "backoff_factor 必须大于 0"),
            Self::InvalidDrainTimeout => write!(f, "shutdown_drain_timeout 必须大于 0"),
        }
    }
}

impl std::error::Error for PollConfigError {}

/// 轮询引擎配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollConfig {
    /// 单次批量读取的超时上限（含驱动阻塞调用）。
    pub request_timeout: Duration,
    /// 失败重试指数退避基数（§34.3 默认 1s）。
    pub backoff_base_ms: u64,
    /// 退避上限（§34.3 默认 30s）。
    pub backoff_max_ms: u64,
    /// 退避倍率。
    pub backoff_factor: u32,
    /// 停机收尾上限：等待最后在途阻塞调用完成的期限（默认 10s）。
    ///
    /// 超时后任务不再等待（阻塞线程无法安全终止），记录告警并按未完成回收；
    /// 驱动句柄由阻塞调用闭包内的 `Arc` 引用持有，线程自然结束后才释放。
    pub shutdown_drain_timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            backoff_base_ms: 1_000,
            backoff_max_ms: 30_000,
            backoff_factor: 2,
            shutdown_drain_timeout: Duration::from_secs(10),
        }
    }
}

impl PollConfig {
    /// 校验配置合法性（`PollScheduler::spawn` 启动前调用）。
    pub fn validate(&self) -> Result<(), PollConfigError> {
        if self.request_timeout.is_zero() {
            return Err(PollConfigError::InvalidTimeout);
        }
        if self.backoff_base_ms == 0 || self.backoff_max_ms == 0 {
            return Err(PollConfigError::InvalidBackoff);
        }
        if self.backoff_factor == 0 {
            return Err(PollConfigError::InvalidBackoffFactor);
        }
        if self.shutdown_drain_timeout.is_zero() {
            return Err(PollConfigError::InvalidDrainTimeout);
        }
        Ok(())
    }

    /// 构造指数退避迭代器（从 `backoff_base_ms` 开始，每次失败乘以 `backoff_factor`，
    /// 上限 `backoff_max_ms`）。
    pub(crate) fn backoff(&self) -> Backoff {
        Backoff::new(
            self.backoff_base_ms,
            self.backoff_max_ms,
            self.backoff_factor,
        )
    }
}

/// 指数退避（§34.3：`1s → 2s → 4s … 上限 30s`；成功后退避重置）。
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    base_ms: u64,
    max_ms: u64,
    factor: u32,
    attempt: u32,
}

impl Backoff {
    pub(crate) fn new(base_ms: u64, max_ms: u64, factor: u32) -> Self {
        Self {
            base_ms,
            max_ms,
            factor,
            attempt: 0,
        }
    }

    /// 下一次等待时长；每次调用递增尝试次数。
    pub(crate) fn next(&mut self) -> Duration {
        let millis = self
            .base_ms
            .saturating_mul(u64::from(self.factor.saturating_pow(self.attempt)))
            .min(self.max_ms);
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_millis(millis)
    }

    /// 成功后重置退避（§34.3）。
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }
}
