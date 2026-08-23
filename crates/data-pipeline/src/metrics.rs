//! 指标埋点（§34.2.1）：静态指标名常量与可选句柄集合。
//!
//! 约定：未注入 registry 时全部句柄为 no-op；观测点在事件粒度上判空
//! （每批一次），不在热路径反复判断。

use metrics::{Counter, Histogram, MetricsRegistry};
use std::sync::Arc;

/// 静态指标名清单（§34.2.1 规范表）。
pub(crate) mod metric_names {
    /// 已输出（含停机排空）的批次总数。
    pub const PIPELINE_BATCHES_FLUSHED_TOTAL: &str = "pipeline_batches_flushed_total";
    /// 已输出的 Observation 总数（批量 `add(n)`）。
    pub const PIPELINE_OBSERVATIONS_TOTAL: &str = "pipeline_observations_total";
    /// 输出通道背压等待时长（ns 直方图）：从批次进入 outbox 到取得
    /// 发送许可的等待时间。
    pub const PIPELINE_FLUSH_BACKPRESSURE_WAIT_NS_HIST: &str =
        "pipeline_flush_backpressure_wait_ns_hist";
}

/// 本组件的指标句柄集合（装配期注册一次；`None` = 不埋点）。
#[derive(Clone, Default)]
pub struct PipelineMetrics {
    /// 批次输出计数器（每批一次 `inc`）。
    pub batches_flushed_total: Option<Counter>,
    /// 观测计数器（每批一次 `add(n)`，n = 批内观测数）。
    pub observations_total: Option<Counter>,
    /// 背压等待直方图（每次成功发送观测一次）。
    pub flush_backpressure_wait_ns: Option<Histogram>,
}

impl PipelineMetrics {
    /// 注册全部句柄；未提供 registry 时返回全 no-op。
    pub fn new(registry: Option<&Arc<MetricsRegistry>>) -> Self {
        let Some(registry) = registry else {
            return Self::default();
        };
        Self {
            batches_flushed_total: Some(
                registry.counter(metric_names::PIPELINE_BATCHES_FLUSHED_TOTAL),
            ),
            observations_total: Some(registry.counter(metric_names::PIPELINE_OBSERVATIONS_TOTAL)),
            flush_backpressure_wait_ns: Some(
                registry.histogram(metric_names::PIPELINE_FLUSH_BACKPRESSURE_WAIT_NS_HIST),
            ),
        }
    }

    /// 记录一批成功输出：批次 +1、观测 +n、背压等待时长观测。
    ///
    /// 单一入口保证三个指标的观测点一致（同一批发送事件）。
    pub(crate) fn observe_batch_emitted(&self, observations: usize, waited_ns: u128) {
        if let Some(counter) = self.batches_flushed_total.as_ref() {
            counter.inc();
        }
        if let Some(counter) = self.observations_total.as_ref() {
            counter.add(observations as u64);
        }
        if let Some(hist) = self.flush_backpressure_wait_ns.as_ref() {
            hist.observe_ns(waited_ns as u64);
        }
    }
}
