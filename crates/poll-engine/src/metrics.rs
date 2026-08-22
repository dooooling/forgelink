//! 指标埋点（§34.2.1）：静态指标名常量与可选句柄集合。
//!
//! 约定：
//! - 指标名必须来自本模块的静态常量（禁止运行时拼接维度值，§34.2.1）；
//! - 未注入 registry 时全部句柄为 no-op（热路径仅一次 `Option::as_ref`
//!   判断，句柄本身不触碰任何原子量）；
//! - 错误类别按固定枚举编码进常量名（可重试 / 永久 / 超时）。

use std::sync::Arc;

use metrics::{Counter, Histogram, MetricsRegistry};

/// 静态指标名清单（§34.2.1 规范表）。
pub(crate) mod metric_names {
    /// 成功完成的轮询批次数。
    pub const POLL_BATCHES_TOTAL: &str = "poll_batches_total";
    /// 可重试错误批次（进入指数退避重试，§34.3）。
    pub const POLL_ERRORS_RETRYABLE_TOTAL: &str = "poll_errors_retryable_total";
    /// 永久错误批次（配置/ABI/契约错误，回到周期节律）。
    pub const POLL_ERRORS_PERMANENT_TOTAL: &str = "poll_errors_permanent_total";
    /// 请求超时批次（§34.3 超时类别）。
    pub const POLL_ERRORS_TIMEOUT_TOTAL: &str = "poll_errors_timeout_total";
    /// 调度触发与计划时刻偏差（ns 直方图；tokio 运行时拥塞/阻塞可见性）。
    pub const SCHEDULE_DELAY_NS_HIST: &str = "schedule_delay_ns_hist";
}

/// 本组件的指标句柄集合（装配期注册一次；`None` = 不埋点）。
///
/// `poll_loop` 为公开 API，句柄集合随其签名公开；字段保持 crate 内部，
/// 由 [`PollMetrics::new`] 装配。
#[derive(Clone, Default)]
pub struct PollMetrics {
    pub batches_total: Option<Counter>,
    pub errors_retryable: Option<Counter>,
    pub errors_permanent: Option<Counter>,
    pub errors_timeout: Option<Counter>,
    pub schedule_delay_ns: Option<Histogram>,
}

impl PollMetrics {
    /// 注册全部句柄；未提供 registry 时返回全 no-op。
    pub fn new(registry: Option<&Arc<MetricsRegistry>>) -> Self {
        let Some(registry) = registry else {
            return Self::NOOP;
        };
        Self {
            batches_total: Some(registry.counter(metric_names::POLL_BATCHES_TOTAL)),
            errors_retryable: Some(registry.counter(metric_names::POLL_ERRORS_RETRYABLE_TOTAL)),
            errors_permanent: Some(registry.counter(metric_names::POLL_ERRORS_PERMANENT_TOTAL)),
            errors_timeout: Some(registry.counter(metric_names::POLL_ERRORS_TIMEOUT_TOTAL)),
            schedule_delay_ns: Some(registry.histogram(metric_names::SCHEDULE_DELAY_NS_HIST)),
        }
    }

    /// 记录一次批次失败：按错误类别选择常量名计数器（§34.2.1 固定枚举
    /// 维度编码进指标名）。超时类别以驱动标准错误码判定，其余按
    /// `retryable` 二分。
    pub(crate) fn observe_error(&self, retryable: &bool, error_code: &str) {
        let counter = if error_code == crate::poll::ERROR_TIMEOUT_CODE {
            self.errors_timeout.as_ref()
        } else if *retryable {
            self.errors_retryable.as_ref()
        } else {
            self.errors_permanent.as_ref()
        };
        if let Some(counter) = counter {
            counter.inc();
        }
    }

    /// 全 no-op 句柄（未注入 registry 时使用，零额外开销）。
    pub const NOOP: Self = Self {
        batches_total: None,
        errors_retryable: None,
        errors_permanent: None,
        errors_timeout: None,
        schedule_delay_ns: None,
    };
}
