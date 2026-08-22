//! 指标埋点（§34.2.1）：静态指标名常量与可选句柄集合。
//!
//! 句柄在装配期注册一次并放入 [`EngineContext`](crate::engine::EngineContext)
//! 共享；`Counter` / `Gauge` 为内部 `Arc` 原子量的轻量克隆，未注入
//! registry 时全 no-op。队列深度 gauge 的增减发生在多设备 worker 与提交
//! 线程之间（并发）——直接用 [`Gauge`] 的原子 `add`/`sub_saturating`
//! 操作单一底层值（评审 P1：镜像变量 + `set` 会因两次原子操作交错产生
//! 后写覆盖前写/丢失递减，已废弃）。
//!
//! # per-device 维度（§34.2.1 有界采样）
//!
//! `queue_depth_device` 按设备拆分队列深度：名称格式
//! `control_queue_depth_device_{device_id}`。§34.2.1 禁止运行期拼接
//! **任意**维度值，但本维度的取值来自**静态配置的设备清单**（Device
//! Catalog 装配期固定、数量有界且可枚举），不是任意外部输入——引擎
//! 构造时枚举目录清单**预注册全部设备 gauge**（评审 P1：首见注册会在
//! 控制热路径引入锁/堆分配/字符串拼接，违背"埋点只做一次原子操作"，
//! 且 registry freeze 后运行期注册的指标不进快照）。运行期观测路径：
//! 一次 HashMap 只读查找 + 原子 add/sub。

use std::collections::HashMap;
use std::sync::Arc;

use metrics::{Counter, Gauge, MetricsRegistry};
use observation_model::ControlStatus;

/// 静态指标名清单（§34.2.1 规范表；固定终态维度编码进常量名）。
pub(crate) mod metric_names {
    /// 控制队列深度（排队 + 执行中；入队 +1、结算 -1）。
    pub const CONTROL_QUEUE_DEPTH_GAUGE: &str = "control_queue_depth_gauge";
    /// per-device 队列深度前缀：完整名 =
    /// `{PREFIX}{device_id}`（维度值来自静态设备清单，见模块文档）。
    pub const CONTROL_QUEUE_DEPTH_DEVICE_PREFIX: &str = "control_queue_depth_device_";
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
    /// 队列深度 gauge（全引擎聚合；并发增减经 Gauge 原生原子
    /// add/sub_saturating，无镜像变量）。
    queue_depth: Option<Gauge>,
    /// per-device 队列深度 gauge（**启动期按静态设备清单预注册**，评审
    /// P1：首见注册会在控制热路径引入锁+堆分配+字符串拼接，违背
    /// §34.2.1"埋点只做一次原子操作"；且与 registry freeze 的一次性语义
    /// 冲突——运行期注册的指标不进快照）。
    ///
    /// `None` = 未注入 registry 或未提供设备清单（不建 map）。设备清单
    /// 来自 Device Catalog（装配期固定、数量有界且可枚举），MAX_METRICS
    /// 兜底超限（返回 no-op 句柄，只影响该维度观测）。
    queue_depth_device: Option<Arc<HashMap<String, Gauge>>>,
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
    /// `device_ids`：静态设备清单（§34.2.1 per-device 维度的取值域），
    /// 装配期从 Device Catalog 枚举传入——启动时一次性预注册全部设备的
    /// 队列深度 gauge，运行期热路径零锁零分配。未传（空切片等价）则
    /// 只保留全局聚合维度。
    pub fn new(registry: Option<&Arc<MetricsRegistry>>, device_ids: &[String]) -> Self {
        let Some(registry) = registry else {
            return Self::default();
        };
        let queue_depth_device = if device_ids.is_empty() {
            None
        } else {
            let mut map = HashMap::with_capacity(device_ids.len());
            for device_id in device_ids {
                let name = format!(
                    "{}{device_id}",
                    metric_names::CONTROL_QUEUE_DEPTH_DEVICE_PREFIX
                );
                map.insert(device_id.clone(), registry.gauge(&name));
            }
            Some(Arc::new(map))
        };
        Self {
            queue_depth: Some(registry.gauge(metric_names::CONTROL_QUEUE_DEPTH_GAUGE)),
            queue_depth_device,
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

    /// 取指定设备预注册的队列深度 gauge（启动期已建表，运行期只读查找：
    /// 一次 HashMap 命中 + 句柄克隆，零锁零分配；未在清单中的设备返回
    /// `None`——理论上不可达，DeviceQueue 由同一清单构造）。
    fn device_gauge(&self, device_id: &str) -> Option<Gauge> {
        self.queue_depth_device.as_ref()?.get(device_id).cloned()
    }

    /// 入队成功：全局与 per-device 队列深度 +1。
    ///
    /// 由 [`crate::queue`] 的入队在持有队列锁的临界区外调用；Gauge 原生
    /// 原子加保证跨线程配对不丢失（单一底层值，评审 P1）。
    pub(crate) fn observe_enqueued(&self, device_id: &str) {
        if let Some(gauge) = self.queue_depth.as_ref() {
            gauge.add(1);
        }
        if let Some(gauge) = self.device_gauge(device_id) {
            gauge.add(1);
        }
    }

    /// 结算完成（任何终态）：全局与 per-device 队列深度 -1（饱和于 0）。
    pub(crate) fn observe_settled_exit(&self, device_id: &str) {
        if let Some(gauge) = self.queue_depth.as_ref() {
            gauge.sub_saturating(1);
        }
        if let Some(gauge) = self.device_gauge(device_id) {
            gauge.sub_saturating(1);
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
