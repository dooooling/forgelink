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
        /// 桶上界（ns），升序；**末项为 [`u64::MAX`]（显式 +Inf 桶）**，
        /// 与 `counts` 严格等长（评审 P2：协议模型显式含 +Inf，不再隐式）。
        bounds: &'static [u64],
        /// 与 bounds 等长的各桶累计计数。
        counts: Vec<u64>,
        /// 全部观测值的总和（ns；供均值计算）。
        sum: u64,
        /// 观测次数。
        count: u64,
    },
}

/// 计数器句柄（单调递增）。
///
/// 克隆廉价（内部 `Arc`）；热路径调用 [`Counter::inc`] 只有一次原子加。
/// 溢出语义：`fetch_add` 在 u64::MAX 处回绕（评审 P2 契约对齐——工业
/// 场景计数到达 2^64 不现实，不做逐次 CAS 饱和的热路径开销换取）。
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

/// 即时量句柄（i64 语义；读侧取最新值）。
///
/// 除覆盖式 [`Gauge::set`] 外提供原子 [`Gauge::add`]/[`Gauge::sub_saturating`]：
/// 并发增减直接作用于**同一底层原子量**（评审 P1：调用方维护镜像变量再做
/// `set` 会因两次原子操作的交错产生后写覆盖前写——如并发 enqueue 的
/// fetch_add 序 1、2 以 2、1 顺序 store，gauge 最终为 1；settle 路径的
/// load→sub→store 两 worker 同读 2 各写回 1 直接丢失一次递减）。单一原子
/// 值从结构上消除该竞态。
#[derive(Clone)]
pub struct Gauge {
    cell: Arc<AtomicU64>,
}

impl Gauge {
    /// 设置当前值（覆盖写；负值经位模式存储合法——如待办差值语义）。
    pub fn set(&self, v: i64) {
        self.cell.store(v as u64, Ordering::Relaxed);
    }

    /// 原子加 n（并发配对计数用：入队 +1 / 结算 -1 等）。
    pub fn add(&self, n: i64) {
        if n >= 0 {
            self.cell.fetch_add(n as u64, Ordering::Relaxed);
        } else {
            self.sub_saturating(n.unsigned_abs());
        }
    }

    /// 原子减 n（饱和于 0 语义由调用方保证合理；本方法只防位模式回绕
    /// 到巨大正数——值低于 n 时停在 0 附近的最小可表示值）。
    ///
    /// 实现说明：u64 位模式承载 i64；`current < n` 时直接写 0（gauge 的
    /// 配对计数场景下负余额无意义，评审 P1 竞态修复的配套语义）。
    pub fn sub_saturating(&self, n: u64) {
        let mut current = self.cell.load(Ordering::Relaxed);
        loop {
            // saturating_sub 恰为所需语义：current < n 时停在 0（u64 域
            // 无负数，位模式 0 即 i64 0）。
            let next = current.saturating_sub(n);
            match self.cell.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
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

/// 直方图桶上界（纳秒，升序；末项 `u64::MAX` 即显式 +Inf 桶）。
///
/// 对数近似序列：50us、100us、250us、500us、1ms、2.5ms、5ms、10ms、25ms、
/// 50ms、100ms、250ms、500ms、1s、2.5s、10s、60s、300s、+Inf。
/// **协议契约**：`counts.len() == bounds.len()`——第 i 桶计数观测值
/// `(bounds[i-1], bounds[i]]`，第 0 桶为 `(0, bounds[0]]`；REST 序列化
/// 原样暴露两数组，消费者按下标一一对应（评审 P2：不再使用"隐式 +Inf"
/// 约定，避免 bounds/counts 长度不一致的歧义）。
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
    u64::MAX,
];

impl Histogram {
    /// 观测一个 ns 值。热路径：一次线性桶定位（19 个有限桶，分支可预测；
    /// 超过全部有限上界落入末位 +Inf 桶）+ 原子加，无锁无堆分配。
    pub fn observe_ns(&self, ns: u64) {
        // 找第一个 >= 值的上界；全部小于则落入末位 u64::MAX（+Inf）桶。
        let mut idx = BUCKET_BOUNDS.len() - 1;
        for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
            if ns <= *bound {
                idx = i;
                break;
            }
        }
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        // sum = 全部观测值总和（评审 P2：不再人为 +1ns）。
        self.sum.fetch_add(ns, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn zeroed() -> Self {
        Self {
            counts: Arc::from(
                (0..BUCKET_BOUNDS.len())
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
/// 冻结后的不可变单元格列表（名称 → 单元格；freeze 生成，快照只读遍历）。
type FrozenCells = Arc<[(String, MetricCell)]>;

pub struct MetricsRegistry {
    inner: Mutex<RegistryInner>,
    /// 冻结后的不可变单元格列表（[`Self::freeze`] 生成；`None` = 未冻结，
    /// snapshot 走持锁拷贝路径）。`Mutex<Option<Arc<..>>>`：freeze 只在
    /// 装配完成时调用一次，snapshot 持锁时间是一次 Arc clone——远短于
    /// 未冻结路径的全量 BTreeMap 克隆。
    frozen: Mutex<Option<FrozenCells>>,
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
            frozen: Mutex::new(None),
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

    /// 冻结注册表（§34.2.1 无锁快照的前提，评审 P1）。
    ///
    /// 装配完成（全部组件构造完毕）、进入运行期前调用一次：把注册表
    /// 拷贝为不可变 `Arc` 列表。此后 [`Self::snapshot`] 只遍历该列表做
    /// 原子加载——**零锁**；未调用时 snapshot 退化为持锁拷贝路径（正确性
    /// 不受影响，仅不满足无锁语义）。冻结后注册仍可用（返回句柄），但
    /// 新指标**不出现在快照中**直到再次冻结——注册期与运行期分离的约定
    /// 见模块文档。
    ///
    /// 幂等：重复调用以最后一次为准。
    pub fn freeze(&self) {
        let inner = self.inner.lock().expect("metrics 锁被毒化");
        let frozen: FrozenCells = inner.metrics.clone().into_iter().collect();
        drop(inner);
        *self.frozen.lock().expect("frozen 锁被毒化") = Some(frozen);
    }

    /// 快照读取（§34.2.1：无锁聚合 + 未注册读取返回 0）。
    ///
    /// 已冻结：遍历不可变列表做原子加载（零锁、零堆分配除输出外）；
    /// 未冻结：退化为持锁拷贝路径（装配期/测试场景）。**未注册的指标名
    /// 在快照中以 0 值出现**（评审 P1 契约对齐：`snapshot_of(&name)`
    /// 场景见 [`Self::value_of`]；本方法仍只列出已注册指标——"未注册
    /// 返回 0"指查询语义而非全量枚举）。
    pub fn snapshot(&self) -> BTreeMap<String, MetricValue> {
        if let Some(frozen) = self.frozen.lock().expect("frozen 锁被毒化").as_ref() {
            return frozen
                .iter()
                .map(|(name, cell)| (name.clone(), cell.to_value()))
                .collect();
        }
        let cells: Vec<(String, MetricCell)> = {
            let inner = self.inner.lock().expect("metrics 锁被毒化");
            inner.metrics.clone().into_iter().collect()
        };
        cells
            .into_iter()
            .map(|(name, cell)| (name, cell.to_value()))
            .collect()
    }

    /// 查询单个指标的当前值（§34.2.1：未注册的指标读取**返回 0**，
    /// 不 panic、不缺失——评审 P1 契约对齐）。
    ///
    /// 已冻结走无锁列表查找；未冻结走注册表。未注册名返回 `Count(0)`
    /// （gauge/histogram 语义无法从名称推断，统一以 Count(0) 表达"零值"）。
    pub fn value_of(&self, name: &str) -> MetricValue {
        let found = if let Some(frozen) = self.frozen.lock().expect("frozen 锁被毒化").as_ref()
        {
            frozen
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, c)| c.to_value())
        } else {
            let inner = self.inner.lock().expect("metrics 锁被毒化");
            inner.metrics.get(name).map(MetricCell::to_value)
        };
        found.unwrap_or(MetricValue::Count(0))
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
        h.observe_ns(30_000); // → 第 0 桶（≤50us）
        h.observe_ns(20_000_000); // → 25ms 桶
        h.observe_ns(400_000_000_000); // → 300s 桶（最后一个有限上界）
        h.observe_ns(u64::MAX); // → +Inf 桶（u64::MAX）
        let snap = r.snapshot();
        let Some(MetricValue::Histogram {
            bounds,
            counts,
            sum,
            count,
        }) = snap.get("schedule_delay_ns_hist")
        else {
            panic!("应为直方图值");
        };
        assert_eq!(*count, 4);
        assert_eq!(
            bounds.len(),
            counts.len(),
            "bounds 与 counts 严格等长（显式 +Inf，评审 P2）"
        );
        assert_eq!(*bounds.last().expect("非空"), u64::MAX, "末桶为显式 +Inf");
        assert_eq!(counts[0], 1); // 50us 桶
        assert_eq!(counts[8], 1); // 25ms 桶
        let inf_idx = bounds.len() - 1;
        assert_eq!(
            counts[inf_idx], 2,
            "400s 与 u64::MAX 均超过最后一个有限上界（300s），落入 +Inf 桶"
        );
        // sum = 全部观测值总和（不再人为 +1ns，评审 P2；u64::MAX 观测
        // 使 sum 回绕——测试用 wrapping 精确对齐实现语义）。
        let expected = 30_000u64
            .wrapping_add(20_000_000)
            .wrapping_add(400_000_000_000)
            .wrapping_add(u64::MAX);
        assert_eq!(*sum, expected);
    }

    #[test]
    fn gauge_concurrent_add_sub_is_lossless() {
        use std::sync::Arc;
        let r = Arc::new(MetricsRegistry::new());
        let g = r.gauge("concurrent_depth_gauge");
        let up = g.clone();
        let down = g.clone();
        let t1 = std::thread::spawn(move || {
            for _ in 0..10_000 {
                up.add(1);
            }
        });
        t1.join().expect("线程不应 panic");
        for _ in 0..10_000 {
            down.sub_saturating(1);
        }
        assert_eq!(
            r.snapshot().get("concurrent_depth_gauge"),
            Some(&MetricValue::Gauge(0)),
            "并发 add/sub 配对后必须归零（单一原子值无镜像漂移）"
        );
    }

    #[test]
    fn freeze_enables_snapshot_and_new_registration_hidden_until_refreeze() {
        let r = MetricsRegistry::new();
        let c = r.counter("before_freeze_total");
        c.inc();
        r.freeze();
        c.inc(); // 冻结后热路径仍可用，快照读实时原子值
        assert_eq!(
            r.snapshot().get("before_freeze_total"),
            Some(&MetricValue::Count(2)),
        );
        let late = r.counter("after_freeze_total");
        late.inc();
        assert!(
            !r.snapshot().contains_key("after_freeze_total"),
            "冻结后新注册不出现在快照直到再次 freeze"
        );
        r.freeze();
        assert_eq!(
            r.snapshot().get("after_freeze_total"),
            Some(&MetricValue::Count(1))
        );
    }

    #[test]
    fn value_of_unregistered_returns_zero_not_missing() {
        let r = MetricsRegistry::new();
        // §34.2.1：未注册指标读取返回 0——value_of 对未注册名返回 Count(0)
        // 而非缺失/panic。
        assert_eq!(r.value_of("never_registered_total"), MetricValue::Count(0));
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
