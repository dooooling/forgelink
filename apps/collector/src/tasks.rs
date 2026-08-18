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
use tracing::{debug, info, warn};

use crate::error::CollectorError;
use crate::health::{BufferHealth, CollectorHealth, DeviceHealth, MqttHealth};

/// 发送失败后的固定小退避（避免 requeue→next 立即重试的空转；
/// 断线重连由 mqtt-client 内部指数退避负责，§34.3）。
const PUBLISH_FAIL_BACKOFF: Duration = Duration::from_millis(200);

/// 共享健康状态（原子计数 + 短锁小字段，供 `health()` 快照）。
#[derive(Default)]
pub(super) struct HealthState {
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
    pub(super) fn snapshot(&self, devices: &[(String, bool, usize, usize)]) -> CollectorHealth {
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
    mut stopping_rx: watch::Receiver<bool>,
    health: Arc<HealthState>,
) {
    let mut sequences = SequenceAllocator::new();
    loop {
        let event = tokio::select! {
            biased;
            _ = stopping_rx.changed() => break,
            event = events_rx.recv() => match event {
                Some(e) => e,
                None => break,
            },
        };
        let ctx = device_manager::MapContext {
            collector_session_id: session_id.clone(),
            ingest_timestamp_ns: crate::now_ns(),
        };
        match &event {
            PollEvent::Batch(batch) => {
                let Some(instance) = manager.get(&batch.device_id) else {
                    warn!(
                        component = "collector",
                        device_id = %batch.device_id,
                        "收到未知设备的轮询批次（设备已移除？）"
                    );
                    continue;
                };
                health.record_device_batch(&batch.device_id, ctx.ingest_timestamp_ns);
                match device_manager::map_results(instance, &batch.results, &ctx, &mut sequences) {
                    Ok(observations) => {
                        health.record_device_error(&batch.device_id, None);
                        ingest_all(&pipeline, observations).await;
                    }
                    Err(e) => {
                        warn!(
                            component = "collector",
                            device_id = %batch.device_id,
                            error = %e,
                            "轮询批次映射失败"
                        );
                        health
                            .record_device_error(&batch.device_id, Some(format!("映射失败: {e}")));
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
                    continue;
                };
                health.record_device_error(device_id, Some(error.message.clone()));
                match device_manager::map_failure(instance, items, error, &ctx, &mut sequences) {
                    Ok(observations) => ingest_all(&pipeline, observations).await,
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
    debug!(component = "collector", "pump 退出");
}

/// 逐条 ingest；管道关闭（停机）时退出，由停机编排接管。
async fn ingest_all(
    pipeline: &Arc<data_pipeline::Pipeline>,
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

/// 发送循环：输出端批次落盘 + 缓冲队头发布（PUBACK 后 ack 删除；
/// 失败 requeue 保留）。停机信号后进入有限排空模式。
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_forward(
    mut out_rx: mpsc::Receiver<data_pipeline::ObservationBatch>,
    buffer: Arc<local_buffer::LocalBuffer>,
    mqtt: Arc<mqtt_client::MqttClient>,
    mut stopping_rx: watch::Receiver<bool>,
    health: Arc<HealthState>,
    idle_poll: Duration,
    drain_timeout: Duration,
) {
    let mut draining = false;
    let mut out_closed = false;
    // 排空期限从进入排空模式时起算（而非任务启动时）：正常等待阶段
    // 不消耗排空窗口（评审：停机排空须给足完整期限）。
    let mut drain_deadline: Option<tokio::time::Instant> = None;
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

        if out_closed {
            // 输出端已关闭（管道已停机）：不再 poll out_rx。否则 biased
            // select 恒选中立即就绪的 None 分支，饿死缓冲 next 分支，
            // 排空批次永不发布。
            match buffer.next().await {
                Ok(Some(stored)) => {
                    publish_stored(
                        &mqtt,
                        &buffer,
                        stored,
                        &health,
                        &mut stopping_rx,
                        drain_deadline,
                    )
                    .await;
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

        tokio::select! {
            biased;
            _ = stopping_rx.changed() => {}
            batch = out_rx.recv() => {
                if let Some(batch) = batch {
                    if let Err(e) = buffer.push(batch).await {
                        warn!(
                            component = "collector",
                            error = %e,
                            "批次落盘失败（按容量策略处理；记录不静默丢弃）"
                        );
                    }
                } else {
                    out_closed = true;
                }
            }
            r = buffer.next() => {
                match r {
                    Ok(Some(stored)) => {
                        publish_stored(&mqtt, &buffer, stored, &health, &mut stopping_rx, drain_deadline).await;
                    }
                    Ok(None) => {
                        if draining && out_closed {
                            debug!(component = "collector", "发送循环排空完成");
                            break;
                        }
                        if draining {
                            // 输出端未关闭：等待管道剩余批次（select 已
                            // 偏置 out_rx，此处仅避免空转）。
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                        // 空闲轮询：WAL 为空，等待新数据 / 输出端 / 停机。
                        tokio::select! {
                            _ = tokio::time::sleep(idle_poll) => {}
                            _ = stopping_rx.changed() => {}
                            b = out_rx.recv() => {
                                if let Some(batch) = b {
                                    if let Err(e) = buffer.push(batch).await {
                                        warn!(component = "collector", error = %e, "批次落盘失败");
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
    debug!(component = "collector", "forward 退出");
}

/// 发布缓冲队头记录：登记在途计数与补传计数，失败统一 requeue 保留。
async fn publish_stored(
    mqtt: &mqtt_client::MqttClient,
    buffer: &local_buffer::LocalBuffer,
    stored: local_buffer::StoredBatch,
    health: &HealthState,
    stopping_rx: &mut watch::Receiver<bool>,
    drain_deadline: Option<tokio::time::Instant>,
) {
    health.record_inflight();
    if stored.batch.replayed {
        health.record_replayed();
    }
    if let Err(e) = forward_batch(mqtt, buffer, stored, health, stopping_rx, drain_deadline).await {
        warn!(
            component = "collector",
            error = %e,
            "批次发布失败（在途记录由 MQTT 停机结算或保留补传）"
        );
    }
}

/// 发布一个批次：publish_batch → PUBACK 确认 → ack 删除；
/// 失败（Closed/Disconnected/CollisionOverwritten 等）→ requeue 保留。
///
/// PUBACK 等待与停机信号赛跑：停机可能发生在等待途中（此时任务还
/// 没回到循环顶部的排空期限检查），挂起的发布不得无限阻塞排空。
/// 停机/排空中断时**不 requeue**：记录保持 in-flight，WAL 不删除，
/// 由 mqtt-client 停机结算（Closed）兜底，重启后按序补传——避免对
/// 已挂起的发布立即重发（重复投递且可能被 ACK 删除）。
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
