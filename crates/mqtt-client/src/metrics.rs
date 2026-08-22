//! 指标埋点（§34.2.1）：静态指标名常量与可选句柄集合。
//!
//! 句柄在装配期注册一次并移入后台 worker 任务；`Counter` / `Gauge`
//! 均为内部 `Arc` 原子量的轻量克隆（`Send + Sync`），未注入 registry
//! 时全 no-op。

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use metrics::{Counter, Gauge, MetricsRegistry};

/// 静态指标名清单（§34.2.1 规范表）。
pub(crate) mod metric_names {
    /// 在途（已入队等待 PUBACK）发布数：请求进入 worker 队列 +1、
    /// PUBACK 确认 / 失败结算 -1。
    pub const MQTT_INFLIGHT_GAUGE: &str = "mqtt_inflight_gauge";
    /// 已确认送达的发布数（收到 broker PUBACK）。
    pub const MQTT_PUBLISHED_TOTAL: &str = "mqtt_published_total";
    /// 断线重发计数：断线时存在未确认在途消息即 +1（重连后由 rumqttc
    /// 重发，§31.3）。
    pub const MQTT_REDELIVERED_TOTAL: &str = "mqtt_redelivered_total";
    /// 失败结算计数：转发失败 / 碰撞覆盖 / 停机与重连上限结算为
    /// `Closed` / `Disconnected` / `CollisionOverwritten` 的发布数。
    pub const MQTT_FAILED_TOTAL: &str = "mqtt_failed_total";
}

/// 本组件的指标句柄集合（装配期注册一次；`None` = 不埋点）。
///
/// 字段 crate 内部：仅 client 模块在事件粒度上判空使用。gauge 用本地
/// 镜像计数（worker 单线程访问）维护 push +1 / settle -1 配对，饱和于 0。
#[derive(Clone)]
pub struct MqttMetrics {
    /// 在途发布数 gauge。
    inflight: Option<Gauge>,
    /// 在途发布数的镜像值（worker 单线程访问）。
    inflight_value: Arc<AtomicI64>,
    /// 已确认发布计数器。
    published: Option<Counter>,
    /// 断线重发计数器。
    redelivered: Option<Counter>,
    /// 失败结算计数器。
    failed: Option<Counter>,
}

impl Default for MqttMetrics {
    fn default() -> Self {
        Self {
            inflight: None,
            inflight_value: Arc::new(AtomicI64::new(0)),
            published: None,
            redelivered: None,
            failed: None,
        }
    }
}

impl MqttMetrics {
    /// 全 no-op 句柄（未注入 registry 时使用，零额外开销）。
    pub fn noop() -> Self {
        Self::default()
    }

    /// 注册全部句柄；未提供 registry 时返回全 no-op。
    pub fn new(registry: Option<&Arc<MetricsRegistry>>) -> Self {
        let Some(registry) = registry else {
            return Self::default();
        };
        Self {
            inflight: Some(registry.gauge(metric_names::MQTT_INFLIGHT_GAUGE)),
            inflight_value: Arc::new(AtomicI64::new(0)),
            published: Some(registry.counter(metric_names::MQTT_PUBLISHED_TOTAL)),
            redelivered: Some(registry.counter(metric_names::MQTT_REDELIVERED_TOTAL)),
            failed: Some(registry.counter(metric_names::MQTT_FAILED_TOTAL)),
        }
    }

    fn bump_inflight(&self, delta: i64) {
        if let Some(gauge) = self.inflight.as_ref() {
            let value = self.inflight_value.fetch_add(delta, Ordering::Relaxed) + delta;
            gauge.set(value.max(0));
        }
    }

    /// 记录一次失败结算（任何以错误收场的发布）。
    pub(crate) fn observe_failed(&self) {
        if let Some(counter) = self.failed.as_ref() {
            counter.inc();
        }
        self.bump_inflight(-1);
    }

    /// 记录一次成功确认（PUBACK 到达，§31.4）。
    pub(crate) fn observe_published(&self) {
        if let Some(counter) = self.published.as_ref() {
            counter.inc();
        }
        self.bump_inflight(-1);
    }

    /// 记录一次成功入队（请求进入 worker 待发队列）：in-flight +1。
    ///
    /// 仅对用户可见的发布计（带 ack_tx 的请求）；停机离线状态等内部
    /// 生成、无等待者的请求不计——它们的结算同样不计（见下）。
    pub(crate) fn observe_accepted(&self) {
        self.bump_inflight(1);
    }

    /// 记录一次断线重发窗口开启（断线且存在未确认在途消息，§31.3）。
    pub(crate) fn observe_disconnect_with_unacked(&self) {
        if let Some(counter) = self.redelivered.as_ref() {
            counter.inc();
        }
    }
}
