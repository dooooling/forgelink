//! 指标埋点（§34.2.1）：静态指标名常量与可选句柄集合。
//!
//! 句柄在装配期注册一次并放入 [`EngineContext`](crate::engine::EngineContext)
//! 共享；`Counter` / `Gauge` 为内部 `Arc` 原子量的轻量克隆，未注入
//! registry 时全 no-op。队列深度 gauge 的增减发生在多设备 worker 与提交
//! 线程之间（并发），镜像计数用原子量维护。

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use metrics::{Counter, Gauge, MetricsRegistry};
use observation_model::ControlStatus;

/// 静态指标名清单（§34.2.1 规范表；固定终态维度编码进常量名）。
pub(crate) mod metric_names {
    /// 控制队列深度（排队 + 执行中；入队 +1、结算 -1）。
    pub const CONTROL_QUEUE_DEPTH_GAUGE: &str = "control_queue_depth_gauge";
    /// 结算为 Succeeded（§80.1 终态）。
    pub const CONTROL_SETTLED_SUCCEEDED_TOTAL: &str = "control_settled_succeeded_total";
    /// 结算为 Failed（§80.1 终态）。
    pub const CONTROL_SETTLED_FAILED_TOTAL: &str = "control_settled_failed_total";
    /// 结算为 Timeout（§80.1 终态）。
    pub const CONTROL_SETTLED_TIMEOUT_TOTAL: &str = "control_settled_timeout_total";
    /// 结算为 Cancelled（§80.1 终态）。
    pub const CONTROL_SETTLED_CANCELLED_TOTAL: &str = "control_settled_cancelled_total";
    /// 结算为 Indeterminate（§80.1 终态）。
    pub const CONTROL_SETTLED_INDETERMINATE_TOTAL: &str = "control_settled_indeterminate_total";
    /// 结算为 Rejected（Driver 前拒绝：校验/授权/前置条件/队列满/冷却等）。
    pub const CONTROL_SETTLED_REJECTED_TOTAL: &str = "control_settled_rejected_total";
    /// 不确定结果冷却期建立次数（五审 P1 冷却语义）。
    pub const CONTROL_COOLDOWN_ENTERED_TOTAL: &str = "control_cooldown_entered_total";
    /// 幂等结算落盘失败次数（降级 Indeterminate 的路径）。
    pub const CONTROL_JOURNAL_SETTLE_FAILED_TOTAL: &str = "control_journal_settle_failed_total";
}

/// 本组件的指标句柄集合（装配期注册一次；`None` = 不埋点）。
#[derive(Clone, Default)]
pub struct ControlMetrics {
    /// 队列深度 gauge（全引擎聚合，不做 per-device 维度——§34.2.1
    /// 禁止运行时拼接任意维度值）。
    queue_depth: Option<Gauge>,
    /// 队列深度的全局镜像（入队 +1 / 结算 -1 跨线程配对）。
    queue_depth_value: Arc<AtomicI64>,
    /// 六个终态计数器。
    settled_succeeded: Option<Counter>,
    settled_failed: Option<Counter>,
    settled_timeout: Option<Counter>,
    settled_cancelled: Option<Counter>,
    settled_indeterminate: Option<Counter>,
    settled_rejected: Option<Counter>,
    /// 冷却期建立计数器。
    cooldown_entered: Option<Counter>,
    /// Journal 结算失败计数器。
    journal_settle_failed: Option<Counter>,
}

impl ControlMetrics {
    /// 注册全部句柄；未提供 registry 时返回全 no-op。
    ///
    /// `ControlMetrics` 未实现手动 no-op 构造之外的捷径：no-op 装配统一
    /// 走本函数的 `None` 分支，避免误把空句柄当已注册。
    pub fn new(registry: Option<&Arc<MetricsRegistry>>) -> Self {
        let Some(registry) = registry else {
            return Self::default();
        };
        Self {
            queue_depth: Some(registry.gauge(metric_names::CONTROL_QUEUE_DEPTH_GAUGE)),
            queue_depth_value: Arc::new(AtomicI64::new(0)),
            settled_succeeded: Some(
                registry.counter(metric_names::CONTROL_SETTLED_SUCCEEDED_TOTAL),
            ),
            settled_failed: Some(registry.counter(metric_names::CONTROL_SETTLED_FAILED_TOTAL)),
            settled_timeout: Some(registry.counter(metric_names::CONTROL_SETTLED_TIMEOUT_TOTAL)),
            settled_cancelled: Some(
                registry.counter(metric_names::CONTROL_SETTLED_CANCELLED_TOTAL),
            ),
            settled_indeterminate: Some(
                registry.counter(metric_names::CONTROL_SETTLED_INDETERMINATE_TOTAL),
            ),
            settled_rejected: Some(registry.counter(metric_names::CONTROL_SETTLED_REJECTED_TOTAL)),
            cooldown_entered: Some(registry.counter(metric_names::CONTROL_COOLDOWN_ENTERED_TOTAL)),
            journal_settle_failed: Some(
                registry.counter(metric_names::CONTROL_JOURNAL_SETTLE_FAILED_TOTAL),
            ),
        }
    }

    /// 入队成功：队列深度 +1。
    ///
    /// 由 [`crate::queue`] 的入队在持有队列锁的临界区外调用；
    /// 原子镜像保证跨线程配对不丢失。
    pub(crate) fn observe_enqueued(&self) {
        if let Some(gauge) = self.queue_depth.as_ref() {
            let value = self.queue_depth_value.fetch_add(1, Ordering::Relaxed) + 1;
            gauge.set(value.max(0));
        }
    }

    /// 结算完成（任何终态）：队列深度 -1（饱和于 0）。
    pub(crate) fn observe_settled_exit(&self) {
        if let Some(gauge) = self.queue_depth.as_ref() {
            let current = self.queue_depth_value.load(Ordering::Relaxed);
            let next = current.saturating_sub(1);
            self.queue_depth_value.store(next, Ordering::Relaxed);
            gauge.set(next);
        }
    }

    /// 按终态记录一次结算（§80.1 结果模型；`Rejected` 含 Driver 前
    /// 各类拒绝路径）。
    pub(crate) fn observe_settled(&self, status: ControlStatus) {
        let counter = match status {
            ControlStatus::Succeeded => self.settled_succeeded.as_ref(),
            ControlStatus::Failed => self.settled_failed.as_ref(),
            ControlStatus::Timeout => self.settled_timeout.as_ref(),
            ControlStatus::Cancelled => self.settled_cancelled.as_ref(),
            ControlStatus::Indeterminate => self.settled_indeterminate.as_ref(),
            ControlStatus::Rejected => self.settled_rejected.as_ref(),
            // Accepted / Running 是中间态，不是结算终态：忽略。
            ControlStatus::Accepted | ControlStatus::Running => None,
        };
        if let Some(counter) = counter {
            counter.inc();
        }
    }

    /// 记录一次不确定结果冷却期建立。
    pub(crate) fn observe_cooldown_entered(&self) {
        if let Some(counter) = self.cooldown_entered.as_ref() {
            counter.inc();
        }
    }

    /// 记录一次幂等结算落盘失败（结果降级 Indeterminate 的路径）。
    pub(crate) fn observe_journal_settle_failed(&self) {
        if let Some(counter) = self.journal_settle_failed.as_ref() {
            counter.inc();
        }
    }
}
