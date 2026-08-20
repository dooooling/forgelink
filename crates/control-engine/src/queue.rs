//! 每设备独立控制队列（§87 Normative）。
//!
//! - **有界队列**：容量来自 [`ControlPolicy::queue_capacity`]，满时拒绝
//!   （`QUEUE_FULL`），不静默丢弃；
//! - **同设备串行执行**：每台设备一个 worker，一次只执行一条请求
//!   （§87 避免 `start/stop/reset/program.select/parameter.write` 无序并发）；
//! - **优先级**：按 [`Priority`](crate::Priority) 取最高优先级，同级 FIFO；
//! - **超时**：入队即计算截止时间（请求与策略超时取较小值）；排到但已过期
//!   的请求直接以 `Timeout` 结算，执行中也以同一截止时间限时；
//! - **取消**：`CancellationToken`；排队中与执行中的请求均可取消（执行中
//!   由引擎 select 取消分支中止执行器 future；已下发但结果不确定的情况由
//!   执行器显式返回 `Indeterminate`，§80.1）。

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use observation_model::{ControlResult, DeviceId};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::audit::{AuditOperation, AuditParameter};
use crate::engine::{EngineContext, SharedResult};
use crate::journal::IdempotencyKey;
use crate::policy::Priority;
use crate::validate::ValidatedOperation;

/// 入队结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EnqueueError {
    /// 队列已满（有界，§87）。
    Full { capacity: usize },
    /// 引擎已停机。
    Closed,
}

/// 取消结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CancelOutcome {
    /// 已找到并标记取消；worker 稍后以 `Cancelled` 结算。
    Marked,
    /// 未找到（已结算 / 未知请求）。
    NotFound,
    /// 引擎已停机（队列已关闭）。
    Closed,
}

/// 排队中的控制请求（提交时已完成全部校验与映射）。
pub(crate) struct QueuedEntry {
    /// 幂等键（§80.1）。
    pub key: IdempotencyKey,
    /// 已映射的操作（写 / 命令）。
    pub operation: ValidatedOperation,
    /// 提交者与来源（§90 审计）。
    pub subject: String,
    pub source: String,
    /// 截止时间（提交时 = now + 有效超时）。
    pub deadline: std::time::Instant,
    pub cancel: CancellationToken,
    /// 结果回传（§77 异步控制；幂等 Duplicate 的等待者共享）。
    pub reply: Arc<SharedResult>,
    /// 预生成的审计元数据（§90）。
    pub audit_meta: AuditMeta,
}

impl QueuedEntry {
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// 审计元数据（提交时预生成，结算时使用；§90）。
pub(crate) struct AuditMeta {
    pub operation: AuditOperation,
    /// 命令 ID 或属性路径列表（§90）。
    pub target: String,
    /// 参数摘要（脱敏，§90）。
    pub parameters: Vec<AuditParameter>,
    pub risk_level: Option<observation_model::CommandRiskLevel>,
    /// 入队时间（计算耗时，§90）。
    pub queued_at_ns: i64,
}

struct QueueInner {
    /// 优先级 → FIFO 队列（`BTreeMap` 升序迭代 + `rev()` = 优先级从高到低）。
    entries: BTreeMap<Priority, VecDeque<QueuedEntry>>,
    /// 有界容量（§87）。
    capacity: usize,
    /// 停机标记：停机后不再接收，排空后 worker 退出。
    closed: bool,
    /// 已入队请求数（含等待与正在执行）。
    len: usize,
    /// 正在执行的请求取消令牌（§87 cancel：运行中的请求也须可取消）。
    running_cancel: Option<CancellationToken>,
}

/// 每设备独立队列 + 串行 worker（§87）。
pub(crate) struct DeviceQueue {
    device_id: DeviceId,
    inner: Mutex<QueueInner>,
    notify: Notify,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DeviceQueue {
    pub fn new(device_id: DeviceId, capacity: usize) -> Self {
        Self {
            device_id,
            inner: Mutex::new(QueueInner {
                entries: BTreeMap::new(),
                capacity,
                closed: false,
                len: 0,
                running_cancel: None,
            }),
            notify: Notify::new(),
            worker: Mutex::new(None),
        }
    }

    /// 入队（有界）。成功时确保 worker 已启动（惰性，每设备一个）。
    pub fn enqueue(
        self: &Arc<Self>,
        entry: QueuedEntry,
        ctx: &Arc<EngineContext>,
    ) -> Result<(), EnqueueError> {
        {
            let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
            if inner.closed {
                return Err(EnqueueError::Closed);
            }
            if inner.len >= inner.capacity {
                return Err(EnqueueError::Full {
                    capacity: inner.capacity,
                });
            }
            let priority = ctx.policy.priority(entry.operation.kind());
            inner.entries.entry(priority).or_default().push_back(entry);
            inner.len += 1;
        }
        self.ensure_worker(ctx);
        self.notify.notify_one();
        Ok(())
    }

    /// 取消：标记排队中或执行中的请求（worker 统一结算 `Cancelled`）。
    pub fn cancel(&self, key: &IdempotencyKey) -> CancelOutcome {
        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
        if inner.closed {
            return CancelOutcome::Closed;
        }
        for deque in inner.entries.values_mut() {
            for entry in deque.iter_mut() {
                if entry.key.namespace == key.namespace && entry.key.request_id == key.request_id {
                    entry.cancel.cancel();
                    return CancelOutcome::Marked;
                }
            }
        }
        // 运行中：取消令牌生效于 run_entry 的三路 select（§87）。
        if let Some(token) = &inner.running_cancel {
            token.cancel();
            return CancelOutcome::Marked;
        }
        CancelOutcome::NotFound
    }

    /// 唤醒 worker（取消/入队后调用；worker 会重新扫描队列）。
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// 停机：不再接收；worker 把剩余请求以 `Cancelled` 结算后退出。
    pub fn shutdown(&self) {
        {
            let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
            inner.closed = true;
        }
        self.notify.notify_one();
    }

    /// 等待 worker 退出（引擎停机流程用；join 后句柄被取走）。
    pub async fn join(&self, grace: std::time::Duration) {
        let handle = self
            .worker
            .lock()
            .expect("DeviceQueue worker 锁被毒化")
            .take();
        if let Some(handle) = handle {
            let abort = handle.abort_handle();
            match tokio::time::timeout(grace, handle).await {
                Ok(_) => {}
                Err(_) => {
                    abort.abort();
                    warn!(
                        component = "control-engine",
                        device_id = %self.device_id,
                        error_code = "queue_worker_join_timeout",
                        "控制队列 worker 停机超时，已强制中止"
                    );
                }
            }
        }
    }

    /// 惰性启动 worker（每设备一个，串行执行，§87）。
    fn ensure_worker(self: &Arc<Self>, ctx: &Arc<EngineContext>) {
        let mut worker = self.worker.lock().expect("DeviceQueue worker 锁被毒化");
        if worker.is_none() {
            let this = self.clone();
            let ctx = ctx.clone();
            *worker = Some(tokio::spawn(async move {
                this.worker_loop(&ctx).await;
            }));
        }
    }

    async fn worker_loop(self: &Arc<Self>, ctx: &Arc<EngineContext>) {
        loop {
            // 取最高优先级、FIFO 顺序的请求；已取消/已过期的就地弹出并结算。
            let picked: Option<Picked> = {
                let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                if inner.closed && inner.len == 0 {
                    return;
                }
                let mut picked = None;
                for deque in inner.entries.values_mut().rev() {
                    if deque.is_empty() {
                        continue;
                    }
                    let front = deque.front().expect("非空");
                    let kind = if front.is_cancelled() {
                        PickedKind::Cancelled
                    } else if Instant::now() >= front.deadline {
                        PickedKind::Expired
                    } else {
                        PickedKind::Ready
                    };
                    let entry = deque.pop_front().expect("front 非空");
                    inner.len -= 1;
                    picked = Some(Picked { entry, kind });
                    break;
                }
                picked
            };

            let Some(picked) = picked else {
                // 空队列：等待唤醒（停机时 notify 会唤醒）。
                self.notify.notified().await;
                continue;
            };

            match picked.kind {
                PickedKind::Cancelled => {
                    settle_entry(ctx, picked.entry, RunResult::Cancelled);
                }
                PickedKind::Expired => {
                    settle_entry(ctx, picked.entry, RunResult::Timeout);
                }
                PickedKind::Ready => {
                    {
                        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                        inner.running_cancel = Some(picked.entry.cancel.clone());
                    }
                    let result = run_entry(ctx, &picked.entry).await;
                    {
                        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                        inner.running_cancel = None;
                    }
                    settle_entry(ctx, picked.entry, result);
                }
            }
        }
    }
}

/// 取出的条目类别。
struct Picked {
    entry: QueuedEntry,
    kind: PickedKind,
}

enum PickedKind {
    Ready,
    Cancelled,
    Expired,
}

/// 执行结果（引擎结算用）。
enum RunResult {
    Done(ControlResult),
    Timeout,
    Cancelled,
}

/// 执行 + 超时 + 取消三路竞争（§77、§80.1）。
async fn run_entry(ctx: &Arc<EngineContext>, entry: &QueuedEntry) -> RunResult {
    let remaining = entry.deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return RunResult::Timeout;
    }
    let cancel = entry.cancel.clone();
    tokio::select! {
        _ = tokio::time::sleep(remaining) => RunResult::Timeout,
        _ = cancel.cancelled() => RunResult::Cancelled,
        outcome = run_operation(ctx, &entry.key.device_id, &entry.operation) => RunResult::Done(outcome),
    }
}

/// 调用执行器并映射为 `ControlResult`（§80.1 结果模型）。
async fn run_operation(
    ctx: &Arc<EngineContext>,
    device_id: &DeviceId,
    operation: &ValidatedOperation,
) -> ControlResult {
    let base = ControlResult {
        request_id: String::new(),
        namespace: String::new(),
        device_id: device_id.clone(),
        status: observation_model::ControlStatus::Running,
        started_at_ns: Some(now_ns()),
        completed_at_ns: None,
        result: None,
        error: None,
    };
    match operation {
        ValidatedOperation::Write { items, paths } => {
            let outcome = ctx.executor.write(device_id, items).await;
            match outcome {
                crate::executor::WriteOutcome::Succeeded(raw_results) => {
                    let item_results = paths
                        .iter()
                        .enumerate()
                        .map(|(index, path)| {
                            let item_id = items[index].id;
                            let raw = raw_results.iter().find(|r| r.item_id == item_id);
                            match raw {
                                Some(raw) => observation_model::PropertyWriteItemResult {
                                    path: path.clone(),
                                    success: raw.success,
                                    protocol_code: raw.protocol_code,
                                    error: raw
                                        .error
                                        .as_ref()
                                        .map(crate::validate::map_driver_error),
                                },
                                None => observation_model::PropertyWriteItemResult {
                                    path: path.clone(),
                                    success: false,
                                    protocol_code: None,
                                    error: Some(observation_model::ControlError {
                                        code: "DRIVER_NO_ITEM_RESULT".to_owned(),
                                        message: format!("写入项 {item_id} 无执行结果"),
                                        details: None,
                                    }),
                                },
                            }
                        })
                        .collect();
                    ControlResult {
                        status: observation_model::ControlStatus::Succeeded,
                        result: Some(observation_model::ControlPayloadResult::PropertyWrite(
                            item_results,
                        )),
                        ..base
                    }
                }
                crate::executor::WriteOutcome::Failed(info) => ControlResult {
                    status: observation_model::ControlStatus::Failed,
                    error: Some(crate::validate::map_driver_error(&info)),
                    ..base
                },
                crate::executor::WriteOutcome::Indeterminate(info) => ControlResult {
                    status: observation_model::ControlStatus::Indeterminate,
                    error: Some(crate::validate::map_driver_error(&info)),
                    ..base
                },
            }
        }
        ValidatedOperation::Execute { command, .. } => {
            let outcome = ctx.executor.execute(device_id, command).await;
            match outcome {
                crate::executor::ExecuteOutcome::Succeeded(raw) => {
                    if raw.success {
                        ControlResult {
                            status: observation_model::ControlStatus::Succeeded,
                            result: Some(observation_model::ControlPayloadResult::Command(
                                observation_model::CommandResult {
                                    device_code: raw.protocol_code,
                                    message: raw.error.as_ref().map(|e| e.message.clone()),
                                    payload: raw.payload,
                                },
                            )),
                            ..base
                        }
                    } else {
                        ControlResult {
                            status: observation_model::ControlStatus::Failed,
                            error: Some(
                                raw.error
                                    .as_ref()
                                    .map(crate::validate::map_driver_error)
                                    .unwrap_or_else(|| observation_model::ControlError {
                                        code: "driver_error".to_owned(),
                                        message: "设备拒绝命令".to_owned(),
                                        details: None,
                                    }),
                            ),
                            ..base
                        }
                    }
                }
                crate::executor::ExecuteOutcome::Failed(info) => ControlResult {
                    status: observation_model::ControlStatus::Failed,
                    error: Some(crate::validate::map_driver_error(&info)),
                    ..base
                },
                crate::executor::ExecuteOutcome::Indeterminate(info) => ControlResult {
                    status: observation_model::ControlStatus::Indeterminate,
                    error: Some(crate::validate::map_driver_error(&info)),
                    ..base
                },
            }
        }
    }
}

/// 结算：补全时间戳 → 幂等 Journal → 审计 → 回传结果（§80.1、§89、§90）。
fn settle_entry(ctx: &Arc<EngineContext>, entry: QueuedEntry, run: RunResult) {
    let completed_at_ns = now_ns();
    let mut result = match run {
        RunResult::Done(result) => result,
        RunResult::Timeout => ControlResult {
            request_id: entry.key.request_id.clone(),
            namespace: entry.key.namespace.clone(),
            device_id: entry.key.device_id.clone(),
            status: observation_model::ControlStatus::Timeout,
            started_at_ns: None,
            completed_at_ns: Some(completed_at_ns),
            result: None,
            error: Some(observation_model::ControlError {
                code: "TIMEOUT".to_owned(),
                message: "控制请求未在期限内完成".to_owned(),
                details: None,
            }),
        },
        RunResult::Cancelled => ControlResult {
            request_id: entry.key.request_id.clone(),
            namespace: entry.key.namespace.clone(),
            device_id: entry.key.device_id.clone(),
            status: observation_model::ControlStatus::Cancelled,
            started_at_ns: None,
            completed_at_ns: Some(completed_at_ns),
            result: None,
            error: Some(observation_model::ControlError {
                code: "CANCELLED".to_owned(),
                message: "控制请求已被取消".to_owned(),
                details: None,
            }),
        },
    };
    result.completed_at_ns = Some(completed_at_ns);
    // 回填信封标识（§89：Done 路径由执行器构造，须统一回填 request_id/namespace）。
    result.request_id = entry.key.request_id.clone();
    result.namespace = entry.key.namespace.clone();

    // 幂等结算（§80.1：下发前已持久化，完成后更新状态与结果）。
    if let Err(e) = ctx.journal.settle(&entry.key, &result) {
        warn!(
            component = "control-engine",
            request_id = %entry.key.request_id,
            error_code = "journal_settle_failed",
            "幂等结算落盘失败: {e}"
        );
    }
    // 从活跃登记移除（幂等 Duplicate 的等待者此后从 Journal 取已结算结果）。
    ctx.active
        .lock()
        .expect("active 锁被毒化")
        .remove(&entry.key);

    // 审计（§90：每个反向控制必须记录；含拒绝/超时/取消）。
    let duration_ms = completed_at_ns
        .saturating_sub(entry.audit_meta.queued_at_ns)
        .max(0) as u64
        / 1_000_000;
    ctx.audit.record(crate::audit::build_event(
        &entry.subject,
        &entry.source,
        &entry.key.namespace,
        &entry.key.device_id,
        &entry.key.request_id,
        entry.audit_meta.operation,
        &entry.audit_meta.target,
        &entry.audit_meta.parameters,
        entry.audit_meta.risk_level,
        result.status,
        result.error.as_ref().map(|e| e.code.clone()),
        result.result.as_ref().and_then(|r| match r {
            observation_model::ControlPayloadResult::PropertyWrite(items) => {
                items.iter().find_map(|i| i.protocol_code)
            }
            observation_model::ControlPayloadResult::Command(c) => c.device_code,
        }),
        duration_ms,
        completed_at_ns,
    ));

    // 回传结果（§77 异步控制：提交后等待/轮询结果）。
    entry.reply.set(result);
}

/// 当前时间（UTC Unix Epoch 纳秒）。
pub(crate) fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
