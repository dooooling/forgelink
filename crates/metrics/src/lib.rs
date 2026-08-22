//! ForgeLink 指标门面（§34.2.1 Normative）。
//!
//! 零依赖的进程内指标层：组件在热路径上通过 [`Counter`] / [`Gauge`] /
//! [`Histogram`] 句柄做单次原子操作，[`MetricsRegistry`] 负责注册、聚合与
//! 无锁快照读取（REST `GET /api/v1/metrics` 的数据源）。
//!
//! # 设计约定（§34.2.1）
//!
//! - **热路径零开销**：埋点只做一次 `fetch_add`/`store`/桶定位，禁止加锁
//!   与堆分配；句柄是 `Arc` 内部字段的轻量克隆；
//! - **有界注册表**：指标名总数有界（[`MAX_METRICS`]），防字符串拼接类
//!   指标名爆炸吃掉内存；满时新注册返回既有 no-op 句柄并记一次告警计数；
//! - **直方图固定桶**：ns 级对数桶边界编译期确定，无堆分配、无锁聚合
//!   （读侧把各桶计数值累加进快照）；
//! - **未注册读取返回 0**：快照只含已注册指标，不 panic；
//! - **标签语义**：本 MVP 用"指标名内嵌维度"（如
//!   `control_settled_total{status=succeeded}` 编码为独立名称）而非通用
//!   标签模型——保持零依赖与快照结构简单；名称仍须来自静态常量，
//!   禁止运行时拼接任意维度值（有界性前提）。
//!
//! # 示例
//!
//! ```
//! use metrics::MetricsRegistry;
//!
//! let registry = MetricsRegistry::new();
//! let batches = registry.counter("poll_batches_total");
//! batches.inc();                    // 热路径：一次原子加
//! let depth = registry.gauge("wal_inflight");
//! depth.set(3);
//! let snap = registry.snapshot();   // 无锁读
//! assert_eq!(snap.get("poll_batches_total"), Some(&metrics::MetricValue::Count(1)));
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// 注册表容量上限（§34.2.1 有界性）：指标名必须来自静态清单，
/// 当前全部组件合计远低于此值；触顶说明出现了非法的动态命名。
pub const MAX_METRICS: usize = 1_024;

/// 指标快照值（§34.2.1：计数器为 u64，gauge 允许负向差值语义，直方图
/// 输出分桶计数 + 总和）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricValue {
    /// 累加计数（`*_total`）。
    Count(u64),
    /// 即时量（`*_gauge`；如队列深度、在途数——非负语义由使用方保证）。
    Gauge(i64),
    /// 直方图：固定桶边界（ns）与各桶计数、总和。
    Histogram {
        /// 桶上界（ns），升序；最后一个元素为 [`u64::MAX`]（+Inf 桶）。
        bounds: &'static [u64],
        /// 与 bounds 等长的各桶累计计数。
        counts: Vec<u64>,
        /// 全部观测值的总和（ns；供均值计算）。
        sum: u64,
        /// 观测次数。
        count: u64,
    },
}

/// 计数器句柄（单调递增，饱和于 u64::MAX 不回绕）。
///
/// 克隆廉价（内部 `Arc`）；热路径调用 [`Counter::inc`] 只有一次原子加。
#[derive(Clone)]
pub struct Counter {
    cell: Arc<AtomicU64>,
}

impl Counter {
    /// 加 1。热路径约定：每次事件恰好一次调用。
    pub fn inc(&self) {
        self.cell.fetch_add(1, Ordering::Relaxed);
    }

    /// 加 n（批量事件；n=0 时仍是原子操作，调用方自行短路更优）。
    pub fn add(&self, n: u64) {
        self.cell.fetch_add(n, Ordering::Relaxed);
    }

    fn zeroed() -> Self {
        Self {
            cell: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// 即时量句柄（设置即覆盖；读侧取最新值）。
#[derive(Clone)]
pub struct Gauge {
    cell: Arc<AtomicU64>,
}

impl Gauge {
    /// 设置当前值（i64 经位模式存储，负值合法——如待办差值语义）。
    pub fn set(&self, v: i64) {
        self.cell.store(v as u64, Ordering::Relaxed);
    }

    fn zeroed() -> Self {
        Self {
            cell: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// 直方图观测句柄：固定对数桶（ns），记录延迟分布。
///
/// 桶边界按 §34.2 验收关注点选取：100 ms 调度周期 p99 ≤ 25 ms 是核心
/// 断言，故 1 ms~64 ms 区间细分；上下两端粗粒度兜底。
#[derive(Clone)]
pub struct Histogram {
    counts: Arc<[AtomicU64]>,
    sum: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
}

/// 直方图桶上界（纳秒，升序；末尾隐含 +Inf 桶）。
///
/// 对数近似序列：50us、100us、250us、500us、1ms、2.5ms、5ms、10ms、25ms、
/// 50ms、100ms、250ms、500ms、1s、2.5s、10s、60s、300s、+Inf。
pub(crate) const BUCKET_BOUNDS: &[u64] = &[
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    10_000_000_000,
    60_000_000_000,
    300_000_000_000,
];

impl Histogram {
    /// 观测一个 ns 值。热路径：一次线性桶定位（19 桶，分支可预测）+
    /// 两次原子加，无锁无堆分配。
    pub fn observe_ns(&self, ns: u64) {
        // 从后往前找第一个 >= 值的上界；全部小于则落入 +Inf 桶（末位）。
        let mut idx = BUCKET_BOUNDS.len();
        for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
            if ns <= *bound {
                idx = i;
                break;
            }
        }
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(ns.saturating_add(1), Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn zeroed() -> Self {
        Self {
            counts: Arc::from(
                (0..=BUCKET_BOUNDS.len())
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            sum: Arc::new(AtomicU64::new(0)),
            count: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// 注册表单元格（Clone = 句柄共享同一底层原子量，非数据拷贝）。
#[derive(Clone)]
enum MetricCell {
    Count(Counter),
    Gauge(Gauge),
    Hist(Histogram),
}

impl MetricCell {
    fn to_value(&self) -> MetricValue {
        match self {
            Self::Count(c) => MetricValue::Count(c.cell.load(Ordering::Relaxed)),
            Self::Gauge(g) => MetricValue::Gauge(g.cell.load(Ordering::Relaxed) as i64),
            Self::Hist(h) => MetricValue::Histogram {
                bounds: BUCKET_BOUNDS,
                counts: h.counts.iter().map(|c| c.load(Ordering::Relaxed)).collect(),
                sum: h.sum.load(Ordering::Relaxed),
                count: h.count.load(Ordering::Relaxed),
            },
        }
    }
}

/// 指标注册表（进程内单实例，组件共享）。
///
/// 注册在装配期完成（同步、低频）；运行期只有热路径原子操作与快照读取，
/// 内部 `Mutex` 仅保护注册表 map 本身（注册互斥），不被热路径触碰。
pub struct MetricsRegistry {
    inner: Mutex<RegistryInner>,
}

struct RegistryInner {
    metrics: BTreeMap<String, MetricCell>,
    /// 注册溢出计数（运维自检；触顶后新注册一律降级 no-op）。
    overflow_count: AtomicU64,
}

impl MetricsRegistry {
    /// 新建空注册表。
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RegistryInner {
                metrics: BTreeMap::new(),
                overflow_count: AtomicU64::new(0),
            }),
        }
    }

    /// 注册（或获取既有）计数器。名称必须来自静态常量清单（§34.2.1：
    /// 禁止运行时拼接维度值）。重复注册返回**同一**底层单元格的句柄
    /// （组件重启装配幂等）。
    ///
    /// # Panics
    ///
    /// 仅当内部锁中毒（其他线程持锁 panic）时 panic——注册发生在装配期，
    /// 该失败等价于进程无法启动。
    #[must_use = "注册结果应被组件持有用于热路径"]
    pub fn counter(&self, name: &str) -> Counter {
        match self.register(
            name,
            || MetricCell::Count(Counter::zeroed()),
            |cell| match cell {
                MetricCell::Count(c) => Some(MetricCell::Count(c.clone())),
                // 类型不符（同名不同种类）：返回 None → 调用方降级 no-op，
                // 不 panic、不覆盖既有指标。
                _ => None,
            },
        ) {
            Some(MetricCell::Count(c)) => c,
            _ => Counter::zeroed(),
        }
    }

    /// 注册（或获取既有）即时量。语义同 [`Self::counter`]。
    #[must_use = "注册结果应被组件持有用于热路径"]
    pub fn gauge(&self, name: &str) -> Gauge {
        match self.register(
            name,
            || MetricCell::Gauge(Gauge::zeroed()),
            |cell| match cell {
                MetricCell::Gauge(g) => Some(MetricCell::Gauge(g.clone())),
                _ => None,
            },
        ) {
            Some(MetricCell::Gauge(g)) => g,
            _ => Gauge::zeroed(),
        }
    }

    /// 注册（或获取既有）直方图。语义同 [`Self::counter`]。
    #[must_use = "注册结果应被组件持有用于热路径"]
    pub fn histogram(&self, name: &str) -> Histogram {
        match self.register(
            name,
            || MetricCell::Hist(Histogram::zeroed()),
            |cell| match cell {
                MetricCell::Hist(h) => Some(MetricCell::Hist(h.clone())),
                _ => None,
            },
        ) {
            Some(MetricCell::Hist(h)) => h,
            _ => Histogram::zeroed(),
        }
    }

    /// 已注册指标数量（测试与运维自检用）。
    pub fn len(&self) -> usize {
        self.inner.lock().expect("metrics 锁被毒化").metrics.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 无锁语义的快照读取：仅短暂持有注册表锁拷贝元数据，随后对各单元格
    /// 做原子加载（热路径从不持该锁，读侧不会阻塞写侧）。
    pub fn snapshot(&self) -> BTreeMap<String, MetricValue> {
        let cells: Vec<(String, MetricCell)> = {
            let inner = self.inner.lock().expect("metrics 锁被毒化");
            inner.metrics.clone().into_iter().collect()
        };
        cells
            .into_iter()
            .map(|(name, cell)| (name, cell.to_value()))
            .collect()
    }

    /// 统一注册入口：存在则校验类型一致并克隆句柄；不存在且未溢出则以
    /// 零值单元格插入并返回其句柄——同名重复注册拿到同一底层数据。
    /// 返回 `None` 表示溢出降级（调用方换 no-op 句柄）。
    fn register(
        &self,
        name: &str,
        make_zero: impl FnOnce() -> MetricCell,
        clone_handle: impl FnOnce(&MetricCell) -> Option<MetricCell>,
    ) -> Option<MetricCell> {
        let mut inner = self.inner.lock().expect("metrics 锁被毒化");
        if let Some(cell) = inner.metrics.get(name) {
            return clone_handle(cell);
        }
        if inner.metrics.len() >= MAX_METRICS {
            // 只计数不刷日志（避免日志风暴；运维自检可读 overflow 计数）。
            inner.overflow_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let handle = make_zero();
        inner.metrics.insert(name.to_owned(), handle.clone());
        Some(handle)
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetricsRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsRegistry")
            .field("registered", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_increments_and_snapshot_reflects() {
        let r = MetricsRegistry::new();
        let c = r.counter("poll_batches_total");
        c.inc();
        c.add(4);
        assert_eq!(
            r.snapshot().get("poll_batches_total"),
            Some(&MetricValue::Count(5))
        );
    }

    #[test]
    fn duplicate_registration_shares_cell() {
        let r = MetricsRegistry::new();
        let a = r.counter("events_total");
        let b = r.counter("events_total");
        a.inc();
        b.add(2);
        assert_eq!(r.len(), 1, "同名注册不得新增槽位");
        assert_eq!(
            r.snapshot().get("events_total"),
            Some(&MetricValue::Count(3))
        );
    }

    #[test]
    fn gauge_stores_latest_value() {
        let r = MetricsRegistry::new();
        let g = r.gauge("wal_inflight_gauge");
        g.set(7);
        assert_eq!(
            r.snapshot().get("wal_inflight_gauge"),
            Some(&MetricValue::Gauge(7))
        );
        g.set(0);
        assert_eq!(
            r.snapshot().get("wal_inflight_gauge"),
            Some(&MetricValue::Gauge(0))
        );
    }

    #[test]
    fn histogram_buckets_and_sum() {
        let r = MetricsRegistry::new();
        let h = r.histogram("schedule_delay_ns_hist");
        h.observe_ns(30_000); // → 第 1 桶（≤50us）
        h.observe_ns(20_000_000); // → 25ms 桶
        h.observe_ns(400_000_000_000); // → +Inf 桶
        let snap = r.snapshot();
        let Some(MetricValue::Histogram {
            counts, sum, count, ..
        }) = snap.get("schedule_delay_ns_hist")
        else {
            panic!("应为直方图值");
        };
        assert_eq!(*count, 3);
        assert_eq!(counts[0], 1); // 50us 桶
        assert_eq!(counts[8], 1); // 25ms 桶（bounds[8]=25_000_000）
        assert_eq!(counts[BUCKET_BOUNDS.len()], 1); // +Inf 桶
        assert_eq!(*sum, 30_001 + 20_000_001 + 400_000_000_001);
    }

    #[test]
    fn unregistered_read_is_absent_not_panic() {
        let r = MetricsRegistry::new();
        assert!(!r.snapshot().contains_key("nonexistent_total"));
    }

    #[test]
    fn registry_bounded_and_degrades_to_noop() {
        let r = MetricsRegistry::new();
        for i in 0..MAX_METRICS {
            let c = r.counter(&format!("fill_{i}_total"));
            c.inc();
        }
        assert_eq!(r.len(), MAX_METRICS);
        // 超限注册得到 no-op 句柄：inc 不生效、快照不含该名。
        let overflow = r.counter("overflow_total");
        overflow.inc();
        assert_eq!(r.len(), MAX_METRICS);
        assert!(!r.snapshot().contains_key("overflow_total"));
    }

    #[test]
    fn concurrent_hot_path_updates_are_lossless_enough() {
        use std::sync::Arc;
        let r = Arc::new(MetricsRegistry::new());
        let c = r.counter("concurrent_total");
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = c.clone();
                std::thread::spawn(move || {
                    for _ in 0..10_000 {
                        c.inc();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("线程不应 panic");
        }
        assert_eq!(
            r.snapshot().get("concurrent_total"),
            Some(&MetricValue::Count(80_000))
        );
    }
}
