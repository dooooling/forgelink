//! Collector 工作任务：事件泵（pump）、发送循环（forward）与心跳日志。
//!
//! # pump：PollEvent → DeviceManager 映射 → Pipeline
//!
//! 按设备回指 `DeviceInstance`，`map_results`/`map_failure`（§37.1 解码 +
//! §7.3 领域映射）产出 `Observation` 逐条 `ingest`；`SequenceAllocator`
//! 保证同设备序列单调（跨批/跨组不重号）。
//!
//! # forward：Pipeline 输出 → Local Buffer → MQTT
//!
//! 单任务串联两级缓冲与发布：输出端批次先落盘（WAL，§103），再从缓冲
//! 队头取出发布（QoS 1）。**PUBACK 是唯一删除路径**（§31.3）：确认后
//! `ack` 删除；`Closed`/`Disconnected`/`CollisionOverwritten` 一律
//! `requeue` 保留补传。断线期间采集持续落盘，网络恢复后按本地序号
//! 顺序补传（补传批次 `replayed=true`）。
//!
//! 输出端与缓冲交替服务（`select`）：管道空闲时照常发送缓冲存量；
//! WAL 为空时按 `idle_poll` 唤醒并同时监听输出端/停机信号。
//!
//! 在途发布被停机/排空中断后进入**只收不发**：输出端批次继续落盘，
//! 不再从缓冲取记录发布——被中断记录保持 in-flight、WAL 不删除，
//! 重启后按序补传，后续记录不得先于它确认（评审 P1）。只收不发
//! 期间落盘限时（评审 P1）：无 PUBACK 时 Backpressure 容量不足会
//! 让 push 无限等待，即死锁。落盘等待超时的批次由发送循环**单条
//! 持有**（`pending_retry`）：不再接收新批次（后续批次留在上游
//! 管道，有界背压），落盘顺序不被破坏，也不仅存内存队列（进程
//! 强杀即丢失）；容量恢复后先落盘再续收。发送循环退出时少量未
//! 落盘批次进入**收尾队列**（有界），由停机流程在 MQTT 结算后
//! 限时重试落盘，成功则随下次启动补传，失败明确结算（不静默
//! 丢弃）；MQTT 结算失败不短路收尾（评审 P1）。
//!
//! # heartbeat：周期健康状态日志（§104 Watchdog 可观测性）
//!
//! 停机由 `watch::Receiver<bool>` 通知：forward 收到后进入**有限排空**
//! 模式（输出端收完 + WAL 能发的发完，期限内退出，保留失败记录）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use device_manager::{DeviceManager, SequenceAllocator};
use poll_engine::PollEvent;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

use crate::error::CollectorError;
use crate::health::{BufferHealth, CollectorHealth, DeviceHealth, MqttHealth};

/// 发送失败后的固定小退避（避免 requeue→next 立即重试的空转；
/// 断线重连由 mqtt-client 内部指数退避负责，§34.3）。
const PUBLISH_FAIL_BACKOFF: Duration = Duration::from_millis(200);

/// 单次 push 的容量等待上限（评审 P1：Backpressure 容量不足时 push
/// 等待 ACK 释放；发送循环暂停/断线期间没有 ACK，无限等待即死锁——
/// push 等待期间 select 被占用，连 WAL 发布都停摆。超时批次进收尾
/// 队列，发送循环继续，不阻塞）。
const PUSH_CAPACITY_WAIT: Duration = Duration::from_millis(500);

/// 共享健康状态（原子计数 + 短锁小字段，供 `health()` 快照）。
#[derive(Default)]
pub(crate) struct HealthState {
    pub(super) started_at_ns: AtomicI64,
    mqtt_acked: AtomicU64,
    mqtt_failed: AtomicU64,
    last_acked_at_ns: AtomicI64,
    last_failed_at_ns: AtomicI64,
    last_error: Mutex<Option<String>>,
    replayed: AtomicU64,
    inflight: AtomicUsize,
    device_last_batch: Mutex<BTreeMap<String, i64>>,
    device_error: Mutex<BTreeMap<String, Option<String>>>,
}

impl HealthState {
    pub(super) fn record_device_batch(&self, device_id: &str, at_ns: i64) {
        if let Ok(mut m) = self.device_last_batch.lock() {
            m.insert(device_id.to_owned(), at_ns);
        }
    }

    pub(super) fn record_device_error(&self, device_id: &str, err: Option<String>) {
        if let Ok(mut m) = self.device_error.lock() {
            m.insert(device_id.to_owned(), err);
        }
    }

    fn record_acked(&self, at_ns: i64) {
        self.mqtt_acked.fetch_add(1, Ordering::Relaxed);
        self.last_acked_at_ns.store(at_ns, Ordering::Relaxed);
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_failed(&self, at_ns: i64, err: &str) {
        self.mqtt_failed.fetch_add(1, Ordering::Relaxed);
        self.last_failed_at_ns.store(at_ns, Ordering::Relaxed);
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        if let Ok(mut m) = self.last_error.lock() {
            *m = Some(err.to_owned());
        }
    }

    fn record_inflight(&self) {
        self.inflight.fetch_add(1, Ordering::Relaxed);
    }

    fn record_replayed(&self) {
        self.replayed.fetch_add(1, Ordering::Relaxed);
    }

    /// 收集健康快照（设备元数据由调用方传入：id/enabled/读取项数/组数）。
    pub(crate) fn snapshot(&self, devices: &[(String, bool, usize, usize)]) -> CollectorHealth {
        let last_batch = self
            .device_last_batch
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let errors = self.device_error.lock().unwrap_or_else(|e| e.into_inner());
        CollectorHealth {
            started_at_ns: self.started_at_ns.load(Ordering::Relaxed),
            devices: devices
                .iter()
                .map(|(id, enabled, items, groups)| DeviceHealth {
                    device_id: id.clone(),
                    enabled: *enabled,
                    read_items: *items,
                    groups: *groups,
                    last_batch_at_ns: last_batch.get(id).copied(),
                    last_error: errors.get(id).cloned().flatten(),
                })
                .collect(),
            mqtt: MqttHealth {
                last_acked_at_ns: {
                    let v = self.last_acked_at_ns.load(Ordering::Relaxed);
                    (v != 0).then_some(v)
                },
                last_failed_at_ns: {
                    let v = self.last_failed_at_ns.load(Ordering::Relaxed);
                    (v != 0).then_some(v)
                },
                last_error: self
                    .last_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone(),
                publishes_acked: self.mqtt_acked.load(Ordering::Relaxed),
                publishes_failed: self.mqtt_failed.load(Ordering::Relaxed),
            },
            buffer: BufferHealth {
                db_path: String::new(), // 由 runtime.health() 覆盖
                inflight: self.inflight.load(Ordering::Relaxed),
                replayed_batches: self.replayed.load(Ordering::Relaxed),
            },
            ..Default::default()
        }
    }
}

/// 事件泵：PollEvent → DeviceManager 映射（Profile+Domain）→ 管道 ingest。
pub(super) async fn run_pump(
    mut events_rx: mpsc::Receiver<PollEvent>,
    manager: Arc<DeviceManager>,
    pipeline: Arc<data_pipeline::Pipeline>,
    session_id: String,
    health: Arc<HealthState>,
) {
    let mut sequences = SequenceAllocator::new();
    // 事件通道由 PollScheduler 持有：其 `shutdown()` 取消并等待全部
    // 轮询任务结束后 sender 才会 drop（通道关闭）。pump 持续 recv 至
    // 通道关闭，停机瞬间尚未完全取消的轮询任务发送的最后一个事件
    // 不会丢失（评审 P2：try_recv 只取当前快照，收到停机信号即退出
    // 可能丢弃该事件）。
    while let Some(event) = events_rx.recv().await {
        pump_event(
            &manager,
            &pipeline,
            &session_id,
            &health,
            &mut sequences,
            &event,
        )
        .await;
    }
    debug!(component = "collector", "pump 退出");
}

/// 处理单个轮询事件：DeviceManager 映射（Profile+Domain）→ 管道 ingest。
async fn pump_event(
    manager: &DeviceManager,
    pipeline: &data_pipeline::Pipeline,
    session_id: &str,
    health: &HealthState,
    sequences: &mut SequenceAllocator,
    event: &PollEvent,
) {
    let ctx = device_manager::MapContext {
        collector_session_id: session_id.to_owned(),
        ingest_timestamp_ns: crate::now_ns(),
    };
    match event {
        PollEvent::Batch(batch) => {
            let Some(instance) = manager.get(&batch.device_id) else {
                warn!(
                    component = "collector",
                    device_id = %batch.device_id,
                    "收到未知设备的轮询批次（设备已移除？）"
                );
                return;
            };
            health.record_device_batch(&batch.device_id, ctx.ingest_timestamp_ns);
            match device_manager::map_results(instance, &batch.results, &ctx, sequences) {
                Ok(observations) => {
                    health.record_device_error(&batch.device_id, None);
                    ingest_all(pipeline, observations).await;
                }
                Err(e) => {
                    warn!(
                        component = "collector",
                        device_id = %batch.device_id,
                        error = %e,
                        "轮询批次映射失败"
                    );
                    health.record_device_error(&batch.device_id, Some(format!("映射失败: {e}")));
                }
            }
        }
        PollEvent::Failed {
            device_id,
            items,
            error,
            ..
        } => {
            let Some(instance) = manager.get(device_id) else {
                return;
            };
            health.record_device_error(device_id, Some(error.message.clone()));
            match device_manager::map_failure(instance, items, error, &ctx, sequences) {
                Ok(observations) => ingest_all(pipeline, observations).await,
                Err(e) => {
                    warn!(
                        component = "collector",
                        device_id,
                        error = %e,
                        "故障映射失败"
                    );
                }
            }
        }
    }
}

/// 逐条 ingest；管道关闭（停机）时退出，由停机编排接管。
async fn ingest_all(
    pipeline: &data_pipeline::Pipeline,
    observations: Vec<observation_model::Observation>,
) {
    for obs in observations {
        if let Err(e) = pipeline.ingest(obs).await {
            match e {
                data_pipeline::PipelineError::Closed => {
                    debug!(component = "collector", "管道已关闭，pump 停止 ingest");
                    return;
                }
                other => {
                    warn!(component = "collector", error = %other, "Observation ingest 失败");
                }
            }
        }
    }
}

/// 落盘失败时允许的瞬时重试次数（SQLite/磁盘瞬时抖动；评审 P1：
/// 永久错误不得无限重试——否则占住唯一发送循环，无法发送旧记录
/// 释放容量，停机也只能超时）。
const PUSH_TRANSIENT_RETRIES: usize = 3;

/// 批次落盘结果：决定发送循环的后续动作。
enum PushResult {
    /// 已落盘。
    Stored,
    /// 永久失败（容量拒绝 / 非法批次 / 损坏 / 已停机）：批次已进
    /// 收尾队列，发送循环应退出（评审 P1：不无限重试占住发送循环）。
    Fatal(CollectorError),
    /// Backpressure 容量等待超时（评审 P1：暂停/断线期间无 ACK 释放
    /// 容量，等待无望）：批次**返回**发送循环持有（单条 `pending_retry`，
    /// 不进入内存收尾队列）——阻塞后续批次保序，批次留在上游管道；
    /// 容量恢复后先落盘再续收。
    Overflow(data_pipeline::ObservationBatch),
}

/// 批次落盘：瞬时错误（`Db`，SQLite/磁盘抖动）退避重试有限次；
/// 单次 push 限时 [`PUSH_CAPACITY_WAIT`]（容量等待不得无限阻塞发送
/// 循环，评审 P1）。`push` 失败时批次已被 worker 消费，此处持克隆
/// 重试。
///
/// 失败批次一律进入收尾队列（评审 P1：批次既未进入 WAL、也无法由
/// 停机流程恢复，不得静默丢弃——队列有界，满时明确结算），由停机
/// 流程在 MQTT 结算后限时重试落盘，成功则随下次启动按序补传。
async fn push_batch(
    buffer: &local_buffer::LocalBuffer,
    batch: data_pipeline::ObservationBatch,
    lost_tx: &mpsc::Sender<data_pipeline::ObservationBatch>,
) -> PushResult {
    let pending = batch;
    let mut transient_left = PUSH_TRANSIENT_RETRIES;
    loop {
        match tokio::time::timeout(PUSH_CAPACITY_WAIT, buffer.push(pending.clone())).await {
            Ok(Ok(())) => return PushResult::Stored,
            Ok(Err(e)) => {
                if is_transient_push_error(&e) && transient_left > 0 {
                    transient_left -= 1;
                    warn!(
                        component = "collector",
                        error = %e,
                        remaining = transient_left,
                        "批次落盘瞬时失败，退避重试（不丢弃数据）"
                    );
                    tokio::time::sleep(PUBLISH_FAIL_BACKOFF).await;
                    continue;
                }
                // 永久失败：批次进收尾队列（保留供停机收尾重试），
                // 发送循环退出（评审 P1）。
                let err = CollectorError::Buffer(e);
                settle_lost(lost_tx, pending.clone(), &err);
                return PushResult::Fatal(err);
            }
            Err(_) => {
                // 容量等待超时（Backpressure）：等待无望（无 ACK 释放）。
                // 批次返回发送循环持有（评审 P1：不得仅存内存收尾队列
                // ——单条 pending_retry 阻塞后续批次，批次留在上游
                // 管道，落盘顺序不被破坏；容量恢复后先落盘再续收）。
                let cause =
                    CollectorError::Task("批次落盘等待容量超时（发布暂停/断线）".to_owned());
                warn!(
                    component = "collector",
                    error = %cause,
                    "批次落盘等待容量超时，由发送循环持有待重试（不接收新批次，保持落盘顺序）"
                );
                return PushResult::Overflow(pending);
            }
        }
    }
}

/// 落盘失败批次入收尾队列（评审 P1：批次既未进入 WAL、也无法由
/// 停机流程恢复，不得静默丢弃）。队列有界（`LOST_QUEUE_CAPACITY`），
/// 满时明确结算——告警说明丢弃原因。
fn settle_lost(
    lost_tx: &mpsc::Sender<data_pipeline::ObservationBatch>,
    batch: data_pipeline::ObservationBatch,
    cause: &CollectorError,
) {
    if let Err(send_err) = lost_tx.try_send(batch) {
        error!(
            component = "collector",
            cause = %cause,
            send_error = %send_err,
            "落盘失败批次收尾队列已满，明确结算丢弃（未进入 WAL）"
        );
    }
}

/// push 错误是否可重试：仅 SQLite 操作失败（瞬时磁盘/IO 抖动）可
/// 重试。容量拒绝（`Reject` 策略下磁盘满）由发送循环重试无法自行
/// 释放——发送并确认旧记录才能释放空间，形成死锁；非法批次 / 损坏 /
/// 已停机均不会自行恢复，一律视为永久错误（评审 P1）。
fn is_transient_push_error(e: &local_buffer::LocalBufferError) -> bool {
    matches!(e, local_buffer::LocalBufferError::Db { .. })
}

/// 发送循环：输出端批次落盘 + 缓冲队头发布（PUBACK 后 ack 删除；
/// 失败 requeue 保留）。停机信号后进入有限排空模式。
///
/// 永久性落盘错误（容量拒绝 / 非法批次 / 损坏 / 已停机）以
/// [`CollectorError`] 返回，由停机编排上报（评审 P1：不无限重试
/// 占住发送循环）。
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_forward(
    mut out_rx: mpsc::Receiver<data_pipeline::ObservationBatch>,
    buffer: Arc<local_buffer::LocalBuffer>,
    mqtt: Arc<mqtt_client::MqttClient>,
    mut stopping_rx: watch::Receiver<bool>,
    health: Arc<HealthState>,
    idle_poll: Duration,
    drain_timeout: Duration,
    lost_tx: mpsc::Sender<data_pipeline::ObservationBatch>,
) -> Result<(), CollectorError> {
    let mut draining = false;
    let mut out_closed = false;
    // 排空期限从进入排空模式时起算（而非任务启动时）：正常等待阶段
    // 不消耗排空窗口（评审：停机排空须给足完整期限）。
    let mut drain_deadline: Option<tokio::time::Instant> = None;
    // 在途发布被停机/排空中断后置位：不再从缓冲取数发布（被中断记录
    // 保持 in-flight、WAL 不删除，重启后按序补传；后续记录不得先于它
    // 确认，否则补传顺序倒置，评审 P1），但输出端批次仍继续落盘。
    let mut publish_suspended = false;
    // 容量等待中的批次（评审 P1）：落盘失败（等待超时）的批次由
    // 发送循环**单条**持有——不进入内存收尾队列（进程强杀即丢失），
    // 且阻塞后续批次（不接收新批次，批次留在上游管道），容量恢复后
    // 先落盘再续收，WAL 落盘顺序不被破坏。仅在发送循环退出时结算
    // 给收尾队列（量小、停机流程限时重试）。
    let mut pending_retry: Option<data_pipeline::ObservationBatch> = None;
    loop {
        if !draining && *stopping_rx.borrow() {
            draining = true;
            drain_deadline = Some(tokio::time::Instant::now() + drain_timeout);
            info!(
                component = "collector",
                timeout_ms = drain_timeout.as_millis(),
                "发送循环进入有限排空模式"
            );
        }
        if draining && drain_deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            warn!(
                component = "collector",
                timeout_ms = drain_timeout.as_millis(),
                "发送循环排空超时，未确认记录保留待下次启动补传"
            );
            break;
        }

        // 容量等待状态（评审 P1）：只重试落盘 + 发布（ACK 删除释放
        // 容量），**不接收新批次**——后续批次留在上游管道（有界
        // 背压），WAL 落盘顺序不被破坏。容量恢复后先落盘再续收。
        if pending_retry.is_some() {
            match tokio::time::timeout(
                PUSH_CAPACITY_WAIT,
                buffer.push(pending_retry.clone().expect("容量等待分支必有待重试批次")),
            )
            .await
            {
                Ok(Ok(())) => {
                    info!(component = "collector", "容量恢复，重试批次落盘成功");
                    pending_retry = None;
                }
                Ok(Err(e)) => {
                    // 永久错误（Reject 容量拒绝 / 非法批次 / 损坏 /
                    // 已停机）：无法自行恢复，进收尾队列并退出。
                    let err = CollectorError::Buffer(e);
                    settle_lost(&lost_tx, pending_retry.take().expect("有值"), &err);
                    return Err(err);
                }
                Err(_) => {
                    warn!(
                        component = "collector",
                        "批次仍无容量可落盘，继续发布以释放空间（不接收新批次）"
                    );
                }
            }
            if pending_retry.is_some() && !publish_suspended {
                // 发布队头（ACK 删除释放容量，评审 P1：输出端已关闭
                // 的排空场景同样必须发布——否则磁盘被旧记录占满时只
                // 重试落盘不释放容量，最终排空超时）。暂停时不得取数
                // （在途记录保持 in-flight，重启后按序补传）。
                match buffer.next().await {
                    Ok(Some(stored)) => {
                        let outcome = publish_stored(
                            &mqtt,
                            &buffer,
                            stored,
                            &health,
                            &mut stopping_rx,
                            drain_deadline,
                        )
                        .await;
                        if matches!(outcome, PublishOutcome::StopTaking) {
                            publish_suspended = true;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(component = "collector", error = %e, "缓冲读取失败");
                    }
                }
            }
            continue;
        }

        if publish_suspended {
            // 只收不发：输出端批次继续落盘，直至输出端关闭（管道已
            // 排空）。缓冲存量不取——留给下次启动按本地序号补传。
            // 落盘限时（评审 P1：暂停时无 PUBACK 释放容量，Backpressure
            // 满时 push 无限等待即死锁）：超时/失败批次进收尾队列，
            // 发送循环继续收尾，不阻塞。
            match out_rx.recv().await {
                Some(batch) => match push_batch(&buffer, batch, &lost_tx).await {
                    PushResult::Stored => {}
                    PushResult::Fatal(e) => {
                        warn!(
                            component = "collector",
                            error = %e,
                            "暂停期间批次永久落盘失败，进入收尾队列，发送循环退出"
                        );
                        return Err(e);
                    }
                    PushResult::Overflow(b) => pending_retry = Some(b),
                },
                None => break,
            }
            continue;
        }

        if out_closed {
            // 输出端已关闭（管道已停机）：out_rx 恒就绪返回 None，不再
            // poll，避免与缓冲 next 分支空转竞争。
            match buffer.next().await {
                Ok(Some(stored)) => {
                    let outcome = publish_stored(
                        &mqtt,
                        &buffer,
                        stored,
                        &health,
                        &mut stopping_rx,
                        drain_deadline,
                    )
                    .await;
                    if matches!(outcome, PublishOutcome::StopTaking) {
                        break;
                    }
                }
                Ok(None) => {
                    if draining {
                        debug!(component = "collector", "发送循环排空完成");
                        break;
                    }
                    // 异常路径：未进入排空但输出端先关闭，等待停机信号。
                    tokio::time::sleep(idle_poll).await;
                }
                Err(e) => {
                    warn!(component = "collector", error = %e, "缓冲读取失败");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            continue;
        }

        // 无偏置 select：输出端与缓冲公平轮询——`biased` 会在输出端
        // 持续就绪时饿死缓冲发布分支（WAL 只进不出，评审 P1）。
        tokio::select! {
            _ = stopping_rx.changed() => {}
            batch = out_rx.recv() => {
                if let Some(batch) = batch {
                    match push_batch(&buffer, batch, &lost_tx).await {
                        PushResult::Stored => {}
                        PushResult::Fatal(e) => return Err(e),
                        // 容量等待超时：批次由发送循环持有（阻塞后续
                        // 批次保序），容量恢复后先落盘再续收（评审 P1）。
                        PushResult::Overflow(b) => pending_retry = Some(b),
                    }
                } else {
                    out_closed = true;
                }
            }
            r = buffer.next() => {
                match r {
                    Ok(Some(stored)) => {
                        let outcome = publish_stored(
                            &mqtt,
                            &buffer,
                            stored,
                            &health,
                            &mut stopping_rx,
                            drain_deadline,
                        )
                        .await;
                        if matches!(outcome, PublishOutcome::StopTaking) {
                            publish_suspended = true;
                        }
                    }
                    Ok(None) => {
                        if draining && out_closed {
                            debug!(component = "collector", "发送循环排空完成");
                            break;
                        }
                        if draining {
                            // 输出端未关闭：等待管道剩余批次（无偏置
                            // select 下此处仅避免空转）。
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        // 空闲轮询：WAL 为空，等待新数据 / 输出端 / 停机。
                        tokio::select! {
                            _ = tokio::time::sleep(idle_poll) => {}
                            _ = stopping_rx.changed() => {}
                            b = out_rx.recv() => {
                                if let Some(batch) = b {
                                    match push_batch(&buffer, batch, &lost_tx).await {
                                        PushResult::Stored => {}
                                        PushResult::Fatal(e) => return Err(e),
                                        PushResult::Overflow(b) => pending_retry = Some(b),
                                    }
                                } else {
                                    out_closed = true;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(component = "collector", error = %e, "缓冲读取失败");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        }
    }
    // 退出前：未落盘的待重试批次进收尾队列（评审 P1：不得静默丢弃；
    // 停机流程在 MQTT 结算后限时重试落盘，成功则随下次启动按序补传）。
    if let Some(batch) = pending_retry {
        settle_lost(
            &lost_tx,
            batch,
            &CollectorError::Task("发送循环退出时待重试批次未落盘".to_owned()),
        );
    }
    debug!(component = "collector", "forward 退出");
    Ok(())
}

/// 批次发布结果：决定发送循环的后续动作。
enum PublishOutcome {
    /// 已确认删除或失败 requeue：继续发送循环。
    Continue,
    /// 停机/排空中断在途发布等待：停止从缓冲取数（被中断记录保持
    /// in-flight、WAL 不删除，重启后按序补传；后续记录不得先于它
    /// 确认，评审 P1）。输出端批次仍继续落盘（不丢管道排空数据）。
    StopTaking,
}

/// 发布缓冲队头记录：登记在途计数与补传计数，失败统一 requeue 保留。
async fn publish_stored(
    mqtt: &mqtt_client::MqttClient,
    buffer: &local_buffer::LocalBuffer,
    stored: local_buffer::StoredBatch,
    health: &HealthState,
    stopping_rx: &mut watch::Receiver<bool>,
    drain_deadline: Option<tokio::time::Instant>,
) -> PublishOutcome {
    health.record_inflight();
    if stored.batch.replayed {
        health.record_replayed();
    }
    match forward_batch(mqtt, buffer, stored, health, stopping_rx, drain_deadline).await {
        Ok(()) => PublishOutcome::Continue,
        // 停机/排空中断：记录保持 in-flight（WAL 不删除），停止取数。
        Err(CollectorError::Task(msg)) => {
            warn!(
                component = "collector",
                error = %msg,
                "在途发布被停机/排空中断，发送循环停止取数（记录保留，重启后按序补传）"
            );
            PublishOutcome::StopTaking
        }
        Err(e) => {
            warn!(
                component = "collector",
                error = %e,
                "批次发布失败（在途记录由 MQTT 停机结算或保留补传）"
            );
            PublishOutcome::Continue
        }
    }
}

/// 发布一个批次：publish_batch → PUBACK 确认 → ack 删除；
/// 失败（Closed/Disconnected/CollisionOverwritten 等）→ requeue 保留。
///
/// PUBACK 等待与停机信号赛跑：停机可能发生在等待途中（此时任务还
/// 没回到循环顶部的排空期限检查），挂起的发布不得无限阻塞排空。
/// 停机/排空中断时**不 requeue**：记录保持 in-flight，WAL 不删除，
/// 由 mqtt-client 停机结算（Closed）兜底，重启后按序补传——避免对
/// 已挂起的发布立即重发（重复投递且可能被 ACK 删除）。中断以
/// `CollectorError::Task` 返回，`run_forward` 收到后停止从缓冲取数
/// （评审 P1：后续记录不得先于被中断记录确认）。
async fn forward_batch(
    mqtt: &mqtt_client::MqttClient,
    buffer: &local_buffer::LocalBuffer,
    stored: local_buffer::StoredBatch,
    health: &HealthState,
    stopping_rx: &mut watch::Receiver<bool>,
    drain_deadline: Option<tokio::time::Instant>,
) -> Result<(), CollectorError> {
    let at_ns = crate::now_ns();
    match mqtt.publish_batch(&stored.batch).await {
        Ok(receipt) => {
            // 等待 PUBACK：与停机信号赛跑；排空模式下额外受期限约束。
            let ack_result = match drain_deadline {
                Some(deadline) => tokio::select! {
                    biased;
                    _ = stopping_rx.changed() => {
                        health.record_failed(at_ns, "停机中断在途发布等待");
                        return Err(CollectorError::Task(
                            "停机中断在途发布等待，记录保留待下次启动补传".to_owned(),
                        ));
                    }
                    r = tokio::time::timeout_at(deadline, receipt.acked()) => match r {
                        Err(_) => {
                            health.record_failed(at_ns, "排空期限内未收到 PUBACK");
                            return Err(CollectorError::Task(
                                "排空期限内未收到 PUBACK，记录保留待下次启动补传".to_owned(),
                            ));
                        }
                        Ok(r) => r,
                    },
                },
                None => tokio::select! {
                    biased;
                    _ = stopping_rx.changed() => {
                        health.record_failed(at_ns, "停机中断在途发布等待");
                        return Err(CollectorError::Task(
                            "停机中断在途发布等待，记录保留待下次启动补传".to_owned(),
                        ));
                    }
                    r = receipt.acked() => r,
                },
            };
            match ack_result {
                Ok(()) => {
                    buffer.ack(stored.local_seq).await?;
                    health.record_acked(at_ns);
                    debug!(
                        component = "collector",
                        local_seq = stored.local_seq,
                        message_id = %stored.batch.message_id,
                        "批次已确认并删除 WAL 记录"
                    );
                    Ok(())
                }
                Err(e) => {
                    // Closed（停机）/ Disconnected / CollisionOverwritten：
                    // 不得删除，放回队头按序补传（§31.3）。
                    buffer.requeue(stored.local_seq).await?;
                    health.record_failed(at_ns, &e.to_string());
                    tokio::time::sleep(PUBLISH_FAIL_BACKOFF).await;
                    Err(CollectorError::Mqtt(e))
                }
            }
        }
        Err(e) => {
            buffer.requeue(stored.local_seq).await?;
            health.record_failed(at_ns, &e.to_string());
            tokio::time::sleep(PUBLISH_FAIL_BACKOFF).await;
            Err(CollectorError::Mqtt(e))
        }
    }
}

/// 心跳：周期输出健康状态（§104：长期稳定性的可观测性基线）。
pub(super) async fn run_heartbeat(
    mut stopping_rx: watch::Receiver<bool>,
    health: Arc<HealthState>,
    interval: Duration,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = stopping_rx.changed() => break,
        }
        if *stopping_rx.borrow() {
            break;
        }
        let h = health.snapshot(&[]);
        info!(
            component = "collector",
            mqtt_acked = h.mqtt.publishes_acked,
            mqtt_failed = h.mqtt.publishes_failed,
            inflight = h.buffer.inflight,
            replayed = h.buffer.replayed_batches,
            "Collector 心跳"
        );
    }
}
