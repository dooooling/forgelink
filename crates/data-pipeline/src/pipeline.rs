//! data-pipeline 核心：有界队列、按设备聚合、批量输出（§31.2）。

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use observation_model::Observation;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::batch::ObservationBatch;
use crate::config::{BUFFER_BATCH_MULTIPLIER, PipelineConfig};
use crate::metrics::PipelineMetrics;

/// 管道错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    /// 配置非法（`PipelineConfig::validate` 拒绝）。
    InvalidConfig { reason: String },
    /// 管道已关闭（已停机、输出接收端已关闭或所有 ingest 发送端已释放）。
    Closed,
    /// 后台任务异常终止（panic），统计结果不可信。
    TaskFailed { reason: String },
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { reason } => write!(f, "data-pipeline 配置非法: {reason}"),
            Self::Closed => write!(f, "data-pipeline 已关闭"),
            Self::TaskFailed { reason } => {
                write!(f, "data-pipeline 后台任务异常终止: {reason}")
            }
        }
    }
}

impl std::error::Error for PipelineError {}

/// 管道运行统计（停机排空结果）。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DrainStats {
    /// 已输出（含停机排空）的批次总数。
    pub batches_emitted: usize,
    /// 已输出的 Observation 总数。
    pub observations_emitted: usize,
    /// 因输出通道关闭 / 排空超时而丢弃的批次数。
    pub batches_dropped: usize,
    /// 被丢弃的 Observation 总数。
    pub observations_dropped: usize,
}

/// 单设备待输出批次（达到 `max_batch_size` 或刷新周期时转为
/// [`ObservationBatch`] 进入 outbox）。
struct PendingBatch {
    sequence: u64,
    observations: Vec<Observation>,
}

/// outbox 中在途批次的指标上下文：入队时刻（背压等待起点，§34.2.1）。
struct QueuedBatch {
    batch: ObservationBatch,
    queued_at: Instant,
}

/// data-pipeline（§31.2）。
///
/// # 用法
///
/// ```ignore
/// let (out_tx, out_rx) = tokio::sync::mpsc::channel(64);
/// let pipeline = Pipeline::spawn(
///     PipelineConfig::new("plant-a", "session-001"),
///     out_tx,
/// )?;
/// pipeline.ingest(observation).await?;
/// // 消费者从 out_rx 接收 ObservationBatch；
/// let stats = pipeline.shutdown().await?; // 优雅停机（有界排空）
/// ```
#[derive(Debug)]
pub struct Pipeline {
    ingest_tx: mpsc::Sender<Observation>,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<DrainStats>,
}

impl Pipeline {
    /// 启动管道。
    ///
    /// - `output`：输出有界通道（容量由调用方决定），消费者收取稳定的
    ///   [`ObservationBatch`]；输出接收端永久关闭时管道立即终止：
    ///   `ingest()` 返回 [`PipelineError::Closed`]，剩余数据结算为丢弃
    ///   统计，不静默丢失。
    /// - 配置非法时返回 [`PipelineError::InvalidConfig`]，不启动任务。
    /// - 不埋点（§34.2.1 句柄全 no-op）；需要指标用
    ///   [`Self::spawn_with_metrics`]。
    pub fn spawn(
        config: PipelineConfig,
        output: mpsc::Sender<ObservationBatch>,
    ) -> Result<Self, PipelineError> {
        Self::spawn_inner(config, output, PipelineMetrics::new(None))
    }

    /// 启动管道并注入指标注册表（§34.2.1）：批次输出、观测计数与
    /// 背压等待时长经 `registry` 暴露（注册幂等）。
    pub fn spawn_with_metrics(
        config: PipelineConfig,
        output: mpsc::Sender<ObservationBatch>,
        registry: Arc<metrics::MetricsRegistry>,
    ) -> Result<Self, PipelineError> {
        Self::spawn_inner(config, output, PipelineMetrics::new(Some(&registry)))
    }

    fn spawn_inner(
        config: PipelineConfig,
        output: mpsc::Sender<ObservationBatch>,
        metrics: PipelineMetrics,
    ) -> Result<Self, PipelineError> {
        if let Err(reason) = config.validate() {
            return Err(PipelineError::InvalidConfig { reason });
        }
        let (ingest_tx, ingest_rx) = mpsc::channel(config.input_capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run(config, ingest_rx, output, shutdown_rx, metrics));
        Ok(Self {
            ingest_tx,
            shutdown_tx,
            task,
        })
    }

    /// 送入一条已归一化的 `Observation`。
    ///
    /// 有界背压：输入队列满时等待；管道已关闭时返回
    /// [`PipelineError::Closed`]。
    pub async fn ingest(&self, observation: Observation) -> Result<(), PipelineError> {
        self.ingest_tx
            .send(observation)
            .await
            .map_err(|_| PipelineError::Closed)
    }

    /// 优雅停机：停止接收新输入，在 `drain_timeout` 时限内排空剩余
    /// partial 批次（有界排空），随后任务结束并返回统计。
    ///
    /// 在途输出发送可被停机信号中断（批次保留并进入有界排空），
    /// 停机不会因输出队列满而永久卡住；排空超时未输出的数据计入
    /// `DrainStats::observations_dropped`。任务异常终止时返回
    /// [`PipelineError::TaskFailed`]，不伪装成零统计。
    ///
    /// 调用方应确保不再持有 `ingest` 发送端；停机开始后送入的观测不保证
    /// 送达。
    pub async fn shutdown(self) -> Result<DrainStats, PipelineError> {
        // 先发停机信号、等待任务排空完成后退出：任务通过 watch 分支
        // 统一退出，不会走"通道关闭即取消"的竞争路径。
        let _ = self.shutdown_tx.send(true);
        self.task.await.map_err(|error| PipelineError::TaskFailed {
            reason: error.to_string(),
        })
    }
}

/// 管道主循环。
///
/// 状态：
///
/// - `pending`：各设备正在聚合的 partial 批次（满批时转为 Batch 进入
///   `outbox`）；
/// - `outbox`：已完成、等待输出的 [`ObservationBatch`]（发送与停机信号
///   在同一 `select!` 中竞争，输出队列满时停机仍可中断在途发送）；
/// - `buffered`：`pending + outbox` 缓存的 Observation 总数，达到统一上限
///   （[`BUFFER_BATCH_MULTIPLIER`]）时暂停消费输入（背压），内存保持有界。
async fn run(
    config: PipelineConfig,
    mut ingest_rx: mpsc::Receiver<Observation>,
    output_tx: mpsc::Sender<ObservationBatch>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: PipelineMetrics,
) -> DrainStats {
    let mut pending: HashMap<String, PendingBatch> = HashMap::new();
    let mut device_sequence: HashMap<String, u64> = HashMap::new();
    // outbox 携带入队时刻（背压等待起点，§34.2.1）。
    let mut outbox: VecDeque<QueuedBatch> = VecDeque::new();
    let mut buffered = 0usize;
    let mut stats = DrainStats::default();
    // P2：`PipelineConfig::validate()` 已拒绝乘法溢出的配置，
    // 此处不会溢出（不 panic、不回绕）。
    let buffer_limit = config
        .max_batch_size
        .checked_mul(BUFFER_BATCH_MULTIPLIER)
        .expect("validate() 已拒绝溢出的 max_batch_size");

    // 定时刷新（§31.2）：首个 tick 对齐到 flush_interval，首批数据完整
    // 等待一个刷新周期后才输出（P2：interval() 首 tick 立即触发）。
    let flush = tokio::time::interval_at(
        tokio::time::Instant::now() + config.flush_interval,
        config.flush_interval,
    );
    let mut flush = flush;

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() {
                    // 停机发送端已释放（Pipeline 被 drop）：取消路径，
                    // 立即停止，不排空（P1：不得空转空耗）。
                    break;
                }
                if *shutdown_rx.borrow() {
                    // 优雅停机：收齐输入队列中已送入的观测 → 剩余 partial
                    // 全部转为 Batch → 在 drain_timeout 时限内有界输出。
                    while let Ok(observation) = ingest_rx.try_recv() {
                        aggregate_observation(&mut pending, &mut device_sequence, observation);
                    }
                    flush_pending_to_outbox(&config, &mut pending, &mut device_sequence, &mut outbox);
                    let deadline = Instant::now() + config.drain_timeout;
                    drain_outbox(&mut outbox, &output_tx, deadline, &mut stats, &metrics).await;
                    break;
                }
            }
            _ = flush.tick() => {
                flush_pending_to_outbox(&config, &mut pending, &mut device_sequence, &mut outbox);
            }
            maybe = ingest_rx.recv(), if buffered < buffer_limit => {
                match maybe {
                    Some(observation) => {
                        buffered += 1;
                        // 按设备聚合；新设备批次序号取该设备的下一序号（独立批次序号）。
                        let device_id = observation.device_id.clone();
                        let full = {
                            let entry = pending
                                .entry(device_id.clone())
                                .or_insert_with(|| PendingBatch {
                                    sequence: *device_sequence.entry(device_id.clone()).or_insert(0),
                                    observations: Vec::new(),
                                });
                            entry.observations.push(observation);
                            entry.observations.len() >= config.max_batch_size
                        };
                        if full {
                            // 满批立即输出（§31.2）：转入 outbox，由发送分支输出。
                            let entry = pending.remove(&device_id).expect("满批必须存在");
                            outbox.push_back(QueuedBatch {
                                batch: ObservationBatch::new(
                                    &config.site_id,
                                    &config.collector_session_id,
                                    device_id.clone(),
                                    entry.sequence,
                                    entry.observations,
                                ),
                                queued_at: Instant::now(),
                            });
                            if let Some(seq) = device_sequence.get_mut(&device_id) {
                                *seq += 1;
                            }
                        }
                    }
                    // 所有 ingest 发送端已释放（取消路径）：立即停止，不排空。
                    None => break,
                }
            }
            result = output_tx.reserve(), if !outbox.is_empty() => {
                match result {
                    Ok(permit) => {
                        let queued = outbox.pop_front().expect("guard 保证 outbox 非空");
                        let count = queued.batch.observations.len();
                        permit.send(queued.batch);
                        buffered -= count;
                        stats.batches_emitted += 1;
                        stats.observations_emitted += count;
                        metrics.observe_batch_emitted(count, queued.queued_at.elapsed().as_nanos());
                    }
                    Err(_) => {
                        // P1：reserve 失败即输出接收端永久消失 → 终止管道。
                        terminate_on_output_closed(
                            &config,
                            &mut ingest_rx,
                            &mut pending,
                            &mut device_sequence,
                            &mut outbox,
                            &mut stats,
                        )
                        .await;
                        break;
                    }
                }
            }
            _ = output_tx.closed() => {
                // P1：输出接收端已释放（无需等待发送失败）→ 终止管道，
                // 不遗留空转的后台任务。
                terminate_on_output_closed(
                    &config,
                    &mut ingest_rx,
                    &mut pending,
                    &mut device_sequence,
                    &mut outbox,
                    &mut stats,
                )
                .await;
                break;
            }
        }
    }
    stats
}

/// 输出接收端永久关闭：终止管道（P1）。
///
/// 先 `close()` 输入 Receiver 阻断新发送（此后 `ingest()` 返回
/// [`PipelineError::Closed`]），再**异步排空**：`close()` 后必须持续
/// `recv()` 直到 `None`，才能等待所有已取得 Permit 的在途发送完成并收齐
/// 消息（tokio 明确规定；`try_recv()` 遇暂时为空便退出会漏计并发发送）。
/// 随后剩余 partial 全部转为 Batch，连同 outbox 中未发出的批次一并计入
/// 丢弃统计（结算，不静默丢失）。任务返回后 `ingest_rx` 随之释放。
async fn terminate_on_output_closed(
    config: &PipelineConfig,
    ingest_rx: &mut mpsc::Receiver<Observation>,
    pending: &mut HashMap<String, PendingBatch>,
    device_sequence: &mut HashMap<String, u64>,
    outbox: &mut VecDeque<QueuedBatch>,
    stats: &mut DrainStats,
) {
    ingest_rx.close();
    while let Some(observation) = ingest_rx.recv().await {
        aggregate_observation(pending, device_sequence, observation);
    }
    flush_pending_to_outbox(config, pending, device_sequence, outbox);
    let dropped_batches = outbox.len();
    let dropped_observations: usize = outbox.iter().map(|q| q.batch.observations.len()).sum();
    stats.batches_dropped += dropped_batches;
    stats.observations_dropped += dropped_observations;
    warn!(
        component = "data-pipeline",
        error_code = "pipeline_output_closed",
        "输出接收端已关闭，管道终止；{dropped_batches} 个批次（{dropped_observations} 条观测）结算为丢弃"
    );
    outbox.clear();
}

/// 聚合一条观测到对应设备的待输出批次（新设备取下一批次序号）。
fn aggregate_observation(
    pending: &mut HashMap<String, PendingBatch>,
    device_sequence: &mut HashMap<String, u64>,
    observation: Observation,
) {
    let device_id = observation.device_id.clone();
    let entry = pending.entry(device_id).or_insert_with(|| PendingBatch {
        sequence: *device_sequence
            .entry(observation.device_id.clone())
            .or_insert(0),
        observations: Vec::new(),
    });
    entry.observations.push(observation);
}

/// 定时刷新：把全部非空 partial 批次转为 Batch 进入 outbox（由发送分支
/// 逐个输出，输出通道满时自然背压）。
///
/// 按 `max_batch_size` 分块：停机或刷新瞬间聚合可能持有超过批次上限的
/// 观测，必须拆分保证批次大小上限契约（§31.2），每块独立递增批次序号。
fn flush_pending_to_outbox(
    config: &PipelineConfig,
    pending: &mut HashMap<String, PendingBatch>,
    device_sequence: &mut HashMap<String, u64>,
    outbox: &mut VecDeque<QueuedBatch>,
) {
    let device_ids: Vec<String> = pending.keys().cloned().collect();
    for device_id in device_ids {
        let mut entry = pending.remove(&device_id).expect("待输出批次必须存在");
        let mut sequence = entry.sequence;
        while !entry.observations.is_empty() {
            let take = entry.observations.len().min(config.max_batch_size);
            let observations = entry.observations.drain(..take).collect();
            outbox.push_back(QueuedBatch {
                batch: ObservationBatch::new(
                    &config.site_id,
                    &config.collector_session_id,
                    device_id.clone(),
                    sequence,
                    observations,
                ),
                queued_at: Instant::now(),
            });
            sequence += 1;
        }
        if let Some(seq) = device_sequence.get_mut(&device_id) {
            *seq = sequence;
        }
    }
}

/// 有界排空：在 `deadline` 时限内输出 outbox 中的批次。
///
/// 每批 ≤ `max_batch_size`（聚合时已保证，§31.2 批次上限契约）；
/// 超时或输出通道关闭时，未输出的数据计入统计（有界排空）。
async fn drain_outbox(
    outbox: &mut VecDeque<QueuedBatch>,
    output_tx: &mpsc::Sender<ObservationBatch>,
    deadline: Instant,
    stats: &mut DrainStats,
    metrics: &PipelineMetrics,
) {
    while let Some(queued) = outbox.pop_front() {
        let observation_count = queued.batch.observations.len();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            drop_outbox_rest(
                queued,
                outbox,
                stats,
                "pipeline_drain_timeout",
                "停机排空超时",
            );
            break;
        }
        // reserve 与截止时间竞争：超时分支中批次仍在本地（reserve 未成功
        // 不会发出数据），可准确计入丢弃（有界排空不丢账）。
        tokio::select! {
            result = output_tx.reserve() => {
                match result {
                    Ok(permit) => {
                        permit.send(queued.batch);
                        stats.batches_emitted += 1;
                        stats.observations_emitted += observation_count;
                        metrics.observe_batch_emitted(
                            observation_count,
                            queued.queued_at.elapsed().as_nanos(),
                        );
                    }
                    Err(_) => {
                        drop_outbox_rest(
                            queued,
                            outbox,
                            stats,
                            "pipeline_output_closed",
                            "输出通道已关闭",
                        );
                        break;
                    }
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                drop_outbox_rest(
                    queued,
                    outbox,
                    stats,
                    "pipeline_drain_timeout",
                    "停机排空超时",
                );
                break;
            }
        }
    }
}

/// 丢弃当前批次与 outbox 中剩余批次并计入统计。
fn drop_outbox_rest(
    current: QueuedBatch,
    outbox: &mut VecDeque<QueuedBatch>,
    stats: &mut DrainStats,
    error_code: &str,
    reason: &str,
) {
    stats.batches_dropped += 1 + outbox.len();
    stats.observations_dropped += current.batch.observations.len()
        + outbox
            .iter()
            .map(|q| q.batch.observations.len())
            .sum::<usize>();
    warn!(
        component = "data-pipeline",
        device_id = %current.batch.device_id,
        error_code = error_code,
        "{reason}，{count} 个批次被丢弃",
        count = 1 + outbox.len(),
    );
    for queued in outbox.drain(..) {
        warn!(
            component = "data-pipeline",
            device_id = %queued.batch.device_id,
            error_code = error_code,
            "{reason}，批次被丢弃"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use observation_model::{Quality, QualityLevel, QualityReason, Value};

    use super::*;

    fn make_obs(device_id: &str, path: &str, sequence: u64) -> Observation {
        Observation {
            observation_id: format!("obs-{device_id}-{sequence}"),
            device_id: device_id.to_owned(),
            path: path.to_owned(),
            value: Some(Value::U64(sequence)),
            quality: Quality {
                level: QualityLevel::Good,
                reason: QualityReason::None,
                protocol_code: None,
                message: None,
            },
            source_timestamp_ns: None,
            ingest_timestamp_ns: 1_700_000_000_000_000_000_i64 + sequence as i64,
            sequence,
            metadata: Default::default(),
        }
    }

    fn cfg(
        site: &str,
        session: &str,
        max_batch_size: usize,
        flush_interval: Duration,
    ) -> PipelineConfig {
        PipelineConfig {
            collector_session_id: session.to_owned(),
            site_id: site.to_owned(),
            max_batch_size,
            flush_interval,
            input_capacity: 8,
            drain_timeout: Duration::from_millis(500),
        }
    }

    #[test]
    fn rejects_invalid_config() {
        let (out_tx, _out_rx) = mpsc::channel(1);
        let mut bad = cfg("plant-a", "s1", 1000, Duration::from_secs(1));
        bad.max_batch_size = 0;
        assert!(matches!(
            Pipeline::spawn(bad, out_tx.clone()).unwrap_err(),
            PipelineError::InvalidConfig { .. }
        ));
        let mut bad = cfg("plant-a", "s1", 1000, Duration::from_secs(1));
        bad.collector_session_id.clear();
        assert!(matches!(
            Pipeline::spawn(bad, out_tx.clone()).unwrap_err(),
            PipelineError::InvalidConfig { .. }
        ));
        let mut bad = cfg("plant-a", "s1", 1000, Duration::from_secs(1));
        bad.site_id.clear();
        assert!(matches!(
            Pipeline::spawn(bad, out_tx.clone()).unwrap_err(),
            PipelineError::InvalidConfig { .. }
        ));
        let mut bad = cfg("plant-a", "s1", 1000, Duration::from_secs(1));
        bad.input_capacity = 0;
        assert!(matches!(
            Pipeline::spawn(bad, out_tx.clone()).unwrap_err(),
            PipelineError::InvalidConfig { .. }
        ));
        let mut bad = cfg("plant-a", "s1", 1000, Duration::from_secs(1));
        bad.drain_timeout = Duration::ZERO;
        assert!(matches!(
            Pipeline::spawn(bad, out_tx.clone()).unwrap_err(),
            PipelineError::InvalidConfig { .. }
        ));
    }

    #[tokio::test]
    async fn emits_full_batch_at_capacity() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline =
            Pipeline::spawn(cfg("plant-a", "s1", 3, Duration::from_secs(60)), out_tx).unwrap();
        for sequence in 0..3 {
            pipeline
                .ingest(make_obs("dev-a", "drive.a", sequence))
                .await
                .unwrap();
        }
        // 满批立即输出（§31.2），不等待刷新周期。
        let batch = out_rx.recv().await.expect("满批应立即输出");
        assert_eq!(batch.schema, "forgelink.telemetry.v1");
        assert_eq!(batch.site_id, "plant-a");
        assert_eq!(batch.device_id, "dev-a");
        assert_eq!(batch.sequence, 0);
        // 长度前缀无歧义编码：2:s1 5:dev-a 0
        assert_eq!(batch.message_id, "2:s15:dev-a0");
        assert!(!batch.replayed);
        assert_eq!(batch.observations.len(), 3);
        assert!(batch.sent_at_ns > 0);

        // 第二批：批次序号单调递增（独立批次序号，§31.2 更新后）。
        for sequence in 3..6 {
            pipeline
                .ingest(make_obs("dev-a", "drive.a", sequence))
                .await
                .unwrap();
        }
        let batch2 = out_rx.recv().await.expect("第二个满批应立即输出");
        assert_eq!(batch2.sequence, 1);
        assert_eq!(batch2.message_id, "2:s15:dev-a1");

        let stats = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.batches_emitted, 2);
        assert_eq!(stats.observations_emitted, 6);
    }

    #[tokio::test]
    async fn message_id_length_prefixed_no_collision() {
        // P2：仅用 `-` 拼接会碰撞（`session="a-b"、device="c"` 与
        // `session="a"、device="b-c"` 生成相同 message_id）；长度前缀编码
        // 必须不同，且可无歧义解析。
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let mut config = cfg("plant-a", "a-b", 1, Duration::from_secs(60));
        config.input_capacity = 4;
        let pipeline = Pipeline::spawn(config, out_tx).unwrap();
        pipeline.ingest(make_obs("c", "drive.a", 0)).await.unwrap();
        let batch_a = out_rx.recv().await.expect("满批输出");
        assert_eq!(batch_a.message_id, "3:a-b1:c0");

        let (out_tx2, mut out_rx2) = mpsc::channel(4);
        let mut config2 = cfg("plant-a", "a", 1, Duration::from_secs(60));
        config2.input_capacity = 4;
        let pipeline2 = Pipeline::spawn(config2, out_tx2).unwrap();
        pipeline2
            .ingest(make_obs("b-c", "drive.a", 0))
            .await
            .unwrap();
        let batch_b = out_rx2.recv().await.expect("满批输出");
        assert_eq!(batch_b.message_id, "1:a3:b-c0");

        assert_ne!(batch_a.message_id, batch_b.message_id);
        let _ = pipeline.shutdown().await.unwrap();
        let _ = pipeline2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn flushes_partial_batch_on_interval() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline = Pipeline::spawn(
            cfg("plant-a", "s1", 1000, Duration::from_millis(50)),
            out_tx,
        )
        .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 10))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 11))
            .await
            .unwrap();
        // 未满批：刷新周期结束后输出（§31.2）。
        let batch = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .expect("定时刷新应输出 partial 批次")
            .expect("输出批次");
        assert_eq!(batch.observations.len(), 2);
        assert_eq!(batch.sequence, 0);
        let stats = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.batches_emitted, 1);
    }

    #[tokio::test]
    async fn first_flush_waits_full_interval() {
        // P2：interval() 首 tick 立即触发，不得在未满一个刷新周期时输出。
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline = Pipeline::spawn(
            cfg("plant-a", "s1", 1000, Duration::from_millis(200)),
            out_tx,
        )
        .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 1))
            .await
            .unwrap();
        let started = Instant::now();
        let batch = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .expect("刷新后应输出")
            .expect("输出批次");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(150),
            "首批必须等待完整刷新周期（实际 {elapsed:?}）"
        );
        assert_eq!(batch.observations.len(), 1);
        let _ = pipeline.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn preserves_arrival_order_and_original_sequence() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline = Pipeline::spawn(
            cfg("plant-a", "s1", 1000, Duration::from_millis(50)),
            out_tx,
        )
        .unwrap();
        // 乱序到达（sequence 本身非递增）：必须保留到达顺序与原始 sequence，
        // 不重新编号（§31.2 更新后）。
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 5))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.b", 7))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.c", 6))
            .await
            .unwrap();
        let batch = out_rx.recv().await.expect("定时刷新输出");
        let paths: Vec<&str> = batch.observations.iter().map(|o| o.path.as_str()).collect();
        let sequences: Vec<u64> = batch.observations.iter().map(|o| o.sequence).collect();
        assert_eq!(paths, ["drive.a", "drive.b", "drive.c"]);
        assert_eq!(sequences, [5, 7, 6]);
        let _ = pipeline.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn isolates_devices_with_independent_sequence() {
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let pipeline =
            Pipeline::spawn(cfg("plant-a", "s1", 2, Duration::from_secs(60)), out_tx).unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 1))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-b", "drive.x", 1))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 2))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-b", "drive.x", 2))
            .await
            .unwrap();

        // 禁止跨设备混批：每批只含单一设备（§31.2 更新后）。
        let b1 = out_rx.recv().await.expect("dev-a 满批");
        let b2 = out_rx.recv().await.expect("dev-b 满批");
        assert_ne!(b1.device_id, b2.device_id);
        for batch in [&b1, &b2] {
            assert_eq!(batch.sequence, 0);
            assert!(
                batch
                    .observations
                    .iter()
                    .all(|o| o.device_id == batch.device_id),
                "批次不得混入其他设备"
            );
        }

        // 两设备各自独立编号：第二批均为 sequence = 1。
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 3))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-b", "drive.x", 3))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 4))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-b", "drive.x", 4))
            .await
            .unwrap();
        let b3 = out_rx.recv().await.expect("dev-a 第二批");
        let b4 = out_rx.recv().await.expect("dev-b 第二批");
        assert_eq!(b3.sequence, 1);
        assert_eq!(b4.sequence, 1);
        let _ = pipeline.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_when_queues_full() {
        let mut config = cfg("plant-a", "s1", 2, Duration::from_secs(60));
        config.input_capacity = 2;
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let pipeline = Pipeline::spawn(config, out_tx).unwrap();

        // 输出通道容量 1 且无人消费 → 任务阻塞在输出 reserve，
        // 内部缓存（pending + outbox）达到统一上限后暂停消费输入 →
        // 持续送入直到输入队列满（有界背压，§22）。
        let mut sent = 0u64;
        let mut blocked = false;
        for sequence in 1..=100 {
            let ingest = pipeline.ingest(make_obs("dev-a", "drive.a", sequence));
            match tokio::time::timeout(Duration::from_millis(100), ingest).await {
                Ok(Ok(())) => sent = sequence,
                Ok(Err(_)) => panic!("管道不应关闭"),
                Err(_) => {
                    blocked = true;
                    break;
                }
            }
        }
        assert!(blocked, "输入队列满时 ingest 必须背压等待");
        assert!(sent >= 2, "至少应成功送入两条");

        // 消费输出解除背压；任务恢复后持续产出批次。
        loop {
            match tokio::time::timeout(Duration::from_millis(100), out_rx.recv()).await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("输出通道不应在停机前关闭"),
                Err(_) => break,
            }
        }
        // 后台持续消费，保证停机排空时内部缓存（最多
        // BUFFER_BATCH_MULTIPLIER × max_batch_size 条观测）都能在
        // drain_timeout 内送达（无消费者时 drain 会在时限内阻塞并丢弃）。
        let consumer = tokio::spawn(async move { while out_rx.recv().await.is_some() {} });

        // 停机（排空剩余 partial）：成功送入的观测必须全部输出，不得丢弃。
        let stats = pipeline.shutdown().await.unwrap();
        consumer.await.unwrap();
        assert_eq!(
            stats.observations_emitted + stats.observations_dropped,
            sent as usize,
            "成功送入的观测必须全部输出或计入统计"
        );
        assert_eq!(stats.batches_dropped, 0);
        assert!(stats.batches_emitted > 0);
    }

    #[tokio::test]
    async fn shutdown_interrupts_blocked_send() {
        // P1：输出队列满（无消费者）时，在途发送必须可被停机信号中断，
        // drain_timeout 保证停机有界，不得永久卡住。
        let mut config = cfg("plant-a", "s1", 2, Duration::from_secs(60));
        config.drain_timeout = Duration::from_millis(200);
        let (out_tx, _out_rx) = mpsc::channel(1);
        let pipeline = Pipeline::spawn(config, out_tx).unwrap();
        for sequence in 1..=4 {
            pipeline
                .ingest(make_obs("dev-a", "drive.a", sequence))
                .await
                .unwrap();
        }
        // 输出容量 1 且无人消费：首批发入输出通道缓冲（容量 1），
        // 第二批次在 reserve 上阻塞 → 停机信号中断在途发送。
        let started = Instant::now();
        let stats = pipeline.shutdown().await.expect("停机必须在时限内返回");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "停机不得因输出队列满而卡住（实际 {elapsed:?}）"
        );
        // 首批已成功进入输出通道（计入已输出）；第二批在途发送被中断、
        // 排空超时后计入丢弃（不静默丢失）。
        assert_eq!(stats.batches_emitted, 1);
        assert_eq!(stats.observations_emitted, 2);
        assert_eq!(stats.batches_dropped, 1);
        assert_eq!(stats.observations_dropped, 2);
    }

    #[tokio::test]
    async fn terminates_and_closes_ingest_when_output_closed() {
        // P1：输出接收端永久关闭 → 管道立即终止：ingest() 返回 Closed
        // （不永久阻塞等待），剩余数据结算为丢弃统计（不静默丢失）。
        let mut config = cfg("plant-a", "s1", 2, Duration::from_secs(60));
        config.input_capacity = 4;
        let (out_tx, mut out_rx) = mpsc::channel(2);
        let pipeline = Pipeline::spawn(config, out_tx).unwrap();
        for sequence in 1..=3 {
            pipeline
                .ingest(make_obs("dev-a", "drive.a", sequence))
                .await
                .unwrap();
        }
        // obs1/2 → 满批输出；obs3 → partial（已送入，尚未成批）。
        let first = out_rx.recv().await.expect("首批应已输出");
        assert_eq!(first.observations.len(), 2);
        drop(out_rx); // 输出接收端永久关闭。

        // 管道终止：后续 ingest 必须返回 Closed，不得永久等待。
        // 终止前成功送入的观测计入"剩余数据"统一结算。
        let extra_sent = tokio::time::timeout(Duration::from_secs(2), async {
            let mut sent = 0usize;
            loop {
                if pipeline
                    .ingest(make_obs("dev-a", "drive.a", 99))
                    .await
                    .is_err()
                {
                    return sent;
                }
                sent += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("输出关闭后 ingest 必须返回 Closed，不得永久等待");

        // 结算：首批（obs1/2）已输出；obs3 与终止前送入的观测全部计入
        // 丢弃统计，不得静默丢失。
        let stats = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.batches_emitted, 1);
        assert_eq!(stats.observations_emitted, 2);
        assert_eq!(
            stats.observations_emitted + stats.observations_dropped,
            3 + extra_sent,
            "已送入观测必须全部输出或计入丢弃统计"
        );
        assert!(
            stats.batches_dropped >= 1 && stats.observations_dropped >= 1,
            "obs3 至少计入一个丢弃批次"
        );
    }

    #[tokio::test]
    async fn shutdown_drains_partial_batch() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline =
            Pipeline::spawn(cfg("plant-a", "s1", 1000, Duration::from_secs(60)), out_tx).unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 1))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 2))
            .await
            .unwrap();

        // 优雅停机：有界排空输出 partial 批次（§31.2）。
        let stats = pipeline.shutdown().await.unwrap();
        let batch = out_rx.recv().await.expect("停机排空应输出剩余批次");
        assert_eq!(batch.observations.len(), 2);
        assert_eq!(stats.batches_emitted, 1);
        assert_eq!(stats.observations_emitted, 2);
        assert_eq!(stats.batches_dropped, 0);
    }

    #[tokio::test]
    async fn shutdown_chunks_over_capacity_batches() {
        // 停机时仍持有超过 max_batch_size 的观测：
        // 排空必须按 max_batch_size 分块输出，批次大小上限契约（§31.2）。
        let mut config = cfg("plant-a", "s1", 2, Duration::from_secs(60));
        config.input_capacity = 16;
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let pipeline = Pipeline::spawn(config, out_tx).unwrap();
        for sequence in 0..5 {
            pipeline
                .ingest(make_obs("dev-a", "drive.a", sequence))
                .await
                .unwrap();
        }
        let stats = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.batches_emitted, 3, "5 条观测按 max=2 分为 3 批");
        assert_eq!(stats.observations_emitted, 5);
        let mut sizes = Vec::new();
        while let Ok(batch) = out_rx.try_recv() {
            sizes.push(batch.observations.len());
        }
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn rejects_overflowing_max_batch_size() {
        // P2：max_batch_size × BUFFER_BATCH_MULTIPLIER 溢出 → validate()
        // 拒绝该配置；不得饱和到 usize::MAX（等同取消背压，
        // 与"内部缓存有界"冲突）。
        let (out_tx, _out_rx) = mpsc::channel(4);
        let mut config = cfg("plant-a", "s1", usize::MAX, Duration::from_secs(60));
        assert!(matches!(
            Pipeline::spawn(config.clone(), out_tx.clone()).unwrap_err(),
            PipelineError::InvalidConfig { .. }
        ));

        // 恰好不溢出的边界值：正常启动、正常停机分块输出。
        config.max_batch_size = usize::MAX / BUFFER_BATCH_MULTIPLIER;
        let pipeline = Pipeline::spawn(config, out_tx).unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 1))
            .await
            .unwrap();
        let stats = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.batches_emitted, 1);
        assert_eq!(stats.observations_emitted, 1);
    }

    #[tokio::test]
    async fn drop_cancels_without_drain() {
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline = Pipeline::spawn(
            cfg("plant-a", "s1", 1000, Duration::from_millis(50)),
            out_tx,
        )
        .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 1))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-a", "drive.a", 2))
            .await
            .unwrap();

        // 取消：丢弃句柄即停止，不排空（无输出）。
        drop(pipeline);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            out_rx.try_recv().is_err(),
            "取消后不得排空输出（刷新周期内不应出现批次）"
        );
    }

    // ---- 指标埋点（§34.2.1） ---------------------------------------------------

    /// 注入 registry 后：满批输出计入 `pipeline_batches_flushed_total` 与
    /// `pipeline_observations_total`（add(n) 批量加）；背压直方图有观测。
    #[tokio::test]
    async fn metrics_count_flushed_batches_and_observations() {
        let registry = Arc::new(metrics::MetricsRegistry::new());
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let pipeline = Pipeline::spawn_with_metrics(
            cfg("plant-a", "s1", 3, Duration::from_secs(60)),
            out_tx,
            registry.clone(),
        )
        .unwrap();
        for sequence in 0..3 {
            pipeline
                .ingest(make_obs("dev-m", "drive.a", sequence))
                .await
                .unwrap();
        }
        // 满批立即输出（§31.2），不等待刷新周期。
        let batch = out_rx.recv().await.expect("满批应立即输出");
        assert_eq!(batch.observations.len(), 3);
        // 停机排空剩余 partial：再产出一批。
        for sequence in 3..5 {
            pipeline
                .ingest(make_obs("dev-m", "drive.a", sequence))
                .await
                .unwrap();
        }
        let stats = pipeline.shutdown().await.unwrap();
        assert_eq!(stats.batches_emitted, 2);
        assert_eq!(stats.observations_emitted, 5);

        use crate::metrics::metric_names;
        use metrics::MetricValue;
        let snap = registry.snapshot();
        assert_eq!(
            snap.get(metric_names::PIPELINE_BATCHES_FLUSHED_TOTAL),
            Some(&MetricValue::Count(2)),
            "批次计数必须与输出批次一致"
        );
        assert_eq!(
            snap.get(metric_names::PIPELINE_OBSERVATIONS_TOTAL),
            Some(&MetricValue::Count(5)),
            "观测计数必须 add(批量) 累计"
        );
        let Some(MetricValue::Histogram { count, .. }) =
            snap.get(metric_names::PIPELINE_FLUSH_BACKPRESSURE_WAIT_NS_HIST)
        else {
            panic!("背压直方图应已注册");
        };
        assert_eq!(*count, 2, "每次成功发送观测一次背压等待");
    }

    /// 背压场景：输出通道满时批次滞留 outbox，解除后等待时长进入直方图
    ///（> 0 的观测证明等待窗口被真实记录，而非恒 0 占位）。
    #[tokio::test]
    async fn metrics_record_backpressure_wait() {
        let registry = Arc::new(metrics::MetricsRegistry::new());
        let mut config = cfg("plant-a", "s1", 1, Duration::from_secs(60));
        config.input_capacity = 8;
        // 输出通道容量 1 且暂不消费：首批占位，第二批在 reserve 上背压。
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let pipeline = Pipeline::spawn_with_metrics(config, out_tx, registry.clone()).unwrap();
        pipeline
            .ingest(make_obs("dev-bp", "drive.a", 0))
            .await
            .unwrap();
        pipeline
            .ingest(make_obs("dev-bp", "drive.a", 1))
            .await
            .unwrap();

        // 第二批必然经历背压等待；消费后管道继续推进至停机排空。
        let stats = {
            let _first = out_rx.recv().await.expect("首批");
            pipeline.shutdown().await.unwrap()
        };
        assert_eq!(stats.batches_emitted, 2);

        use crate::metrics::metric_names;
        use metrics::MetricValue;
        let snap = registry.snapshot();
        assert_eq!(
            snap.get(metric_names::PIPELINE_BATCHES_FLUSHED_TOTAL),
            Some(&MetricValue::Count(2))
        );
        let Some(MetricValue::Histogram { sum, count, .. }) =
            snap.get(metric_names::PIPELINE_FLUSH_BACKPRESSURE_WAIT_NS_HIST)
        else {
            panic!("背压直方图应已注册");
        };
        assert_eq!(*count, 2);
        assert!(*sum > 0, "背压等待必须产生非零时长观测");
    }
}
