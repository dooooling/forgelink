//! 指标埋点（§34.2.1）：静态指标名常量与可选句柄集合。
//!
//! 句柄在装配期注册一次并随配置传入 worker 专用阻塞线程——
//! `Counter` / `Gauge` / `Histogram` 均为内部 `Arc` 原子量的轻量克隆，
//! `Send + Sync`，可安全跨线程使用；未注入 registry 时全 no-op。

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use metrics::{Counter, Gauge, Histogram, MetricsRegistry};

/// 静态指标名清单（§34.2.1 规范表）。
pub(crate) mod metric_names {
    /// 在途（已持久化、等待 PUBACK 确认）记录数：push 成功 +1、ACK 删除 -1。
    pub const WAL_INFLIGHT_GAUGE: &str = "wal_inflight_gauge";
    /// 补传计数：`next` 取出补传记录（`replayed = true`）时 +1。
    pub const WAL_REPLAYED_TOTAL: &str = "wal_replayed_total";
    /// 落盘耗时（ns 直方图）：单条记录 INSERT 的耗时。
    pub const WAL_PERSIST_NS_HIST: &str = "wal_persist_ns_hist";
}

/// 本组件的指标句柄集合（装配期注册一次；`None` = 不埋点）。
///
/// 字段 crate 内部：仅 worker 模块在事件粒度上判空使用。
///
/// gauge 的增减配对：worker 线程内单线程访问，本地镜像计数
/// （`AtomicI64`）保存当前在途值——gauge 句柄只暴露 `set(i64)`，
/// 镜像值保证 push +1 / ack -1 语义正确且饱和于 0。
#[derive(Clone, Default)]
pub struct WalMetrics {
    /// 在途记录数 gauge。
    inflight: Option<Gauge>,
    /// 在途记录数的线程内镜像（worker 单线程访问）。
    inflight_value: Arc<AtomicI64>,
    /// 补传计数器。
    replayed: Option<Counter>,
    /// 落盘耗时直方图。
    persist_ns: Option<Histogram>,
}

impl WalMetrics {
    /// 注册全部句柄；未提供 registry 时返回全 no-op。
    pub fn new(registry: Option<&Arc<MetricsRegistry>>) -> Self {
        let Some(registry) = registry else {
            return Self::default();
        };
        Self {
            inflight: Some(registry.gauge(metric_names::WAL_INFLIGHT_GAUGE)),
            inflight_value: Arc::new(AtomicI64::new(0)),
            replayed: Some(registry.counter(metric_names::WAL_REPLAYED_TOTAL)),
            persist_ns: Some(registry.histogram(metric_names::WAL_PERSIST_NS_HIST)),
        }
    }

    /// 记录一条记录成功落盘：in-flight +1 并观测落盘耗时。
    ///
    /// push 直连与背压 flush 共用；恢复加载的历史记录不计（由
    /// [`Self::observe_restored`] 在启动时统一入账）。
    pub(crate) fn observe_persisted(&self, elapsed_ns: u64) {
        if let Some(gauge) = self.inflight.as_ref() {
            let value = self.inflight_value.fetch_add(1, Ordering::Relaxed) + 1;
            gauge.set(value);
        }
        if let Some(hist) = self.persist_ns.as_ref() {
            hist.observe_ns(elapsed_ns);
        }
    }

    /// 启动时把恢复的未确认记录数计入 in-flight（worker 启动后调用一次）：
    /// 这些记录是上一会话 push 的、仍在等待确认，ACK 时会 -1——不入账会
    /// 让镜像计数漂移为负。
    pub(crate) fn observe_restored(&self, count: i64) {
        if let Some(gauge) = self.inflight.as_ref() {
            self.inflight_value.store(count.max(0), Ordering::Relaxed);
            gauge.set(count.max(0));
        }
    }

    /// 记录 n 条记录因保留时间到期被清理（§103）：与 ACK 同为"离开未确认
    /// 集合"，必须同步扣减，否则 gauge 漂移。
    pub(crate) fn observe_expired(&self, n: usize) {
        if let Some(gauge) = self.inflight.as_ref()
            && n > 0
        {
            let current = self.inflight_value.load(Ordering::Relaxed);
            let next = current.saturating_sub(n as i64);
            self.inflight_value.store(next, Ordering::Relaxed);
            gauge.set(next);
        }
    }

    /// 记录一条记录 ACK 删除：in-flight -1（饱和于 0；重复 ACK 幂等）。
    pub(crate) fn observe_acked(&self) {
        if let Some(gauge) = self.inflight.as_ref() {
            let current = self.inflight_value.load(Ordering::Relaxed);
            let next = current.saturating_sub(1);
            self.inflight_value.store(next, Ordering::Relaxed);
            gauge.set(next);
        }
    }

    /// 记录一次补传取出（恢复积压或重发标记，§31.4）。
    pub(crate) fn observe_replayed(&self) {
        if let Some(counter) = self.replayed.as_ref() {
            counter.inc();
        }
    }
}
