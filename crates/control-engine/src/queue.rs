//! 每设备独立控制队列（§87 Normative）。
//!
//! - **有界队列**：容量来自 [`ControlPolicy::queue_capacity`]，满时拒绝
//!   （`QUEUE_FULL`），不静默丢弃；
//! - **同设备串行执行**：每台设备一个 worker，一次只执行一条请求
//!   （§87 避免 `start/stop/reset/program.select/parameter.write` 无序并发）；
//! - **优先级**：按 [`Priority`](crate::Priority) 取最高优先级，同级 FIFO；
//! - **超时**：入队即计算截止时间（请求与策略超时取较小值）；排到但已过期
//!   的请求直接以 `Timeout` 结算；执行中（执行器已开始）超时视为结果不确定，
//!   以 `Indeterminate` 结算（驱动可能已下发，§80.1）；
//! - **取消**：`CancellationToken`；排队中取消以 `Cancelled` 结算，执行中取消
//!   以 `Indeterminate` 结算（结果不确定，§80.1）；取消按幂等键匹配，不误伤
//!   其他请求。

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
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
#[derive(Clone)]
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
#[derive(Clone)]
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
    /// 正在执行的请求条目（§87 cancel：运行中的请求也须可取消；
    /// 保存完整条目以便强制中止时结算，且取消按幂等键匹配防止误取消）。
    running: Option<QueuedEntry>,
}

/// 每设备独立队列 + 串行 worker（§87）。
pub(crate) struct DeviceQueue {
    device_id: DeviceId,
    inner: Mutex<QueueInner>,
    /// 引擎级停机标志（P1-A：停机在排空后仍可能被 `get_or_create_queue` 重建
    /// 队列——该标志由 worker/enqueue 检查，保证停机后新建队列也不会接收请求）。
    closed_flag: Arc<AtomicBool>,
    notify: Notify,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DeviceQueue {
    pub fn new(device_id: DeviceId, capacity: usize, closed_flag: Arc<AtomicBool>) -> Self {
        Self {
            device_id,
            inner: Mutex::new(QueueInner {
                entries: BTreeMap::new(),
                capacity,
                closed: false,
                len: 0,
                running: None,
            }),
            closed_flag,
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
            // P1-A：引擎已停机则拒绝——停机排空后重建的队列也必须拒绝，
            // 否则请求在停机后仍被接收且不被任何 worker join。
            if inner.closed || self.closed_flag.load(Ordering::SeqCst) {
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

    /// 查找请求的操作种类（四审 P1：取消前授权用）。
    ///
    /// 扫描排队与运行中条目；未找到返回 `None`。
    pub fn peek_kind(&self, key: &IdempotencyKey) -> Option<crate::policy::OperationKind> {
        let inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
        for deque in inner.entries.values() {
            for entry in deque.iter() {
                if entry.key.namespace == key.namespace && entry.key.request_id == key.request_id {
                    return Some(entry.operation.kind());
                }
            }
        }
        inner
            .running
            .as_ref()
            .filter(|e| e.key.namespace == key.namespace && e.key.request_id == key.request_id)
            .map(|e| e.operation.kind())
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
        // 运行中：按幂等键匹配才取消（§87 取消不得误伤其他请求）。
        if let Some(running) = &inner.running {
            if running.key.namespace == key.namespace && running.key.request_id == key.request_id {
                running.cancel.cancel();
                return CancelOutcome::Marked;
            }
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
    ///
    /// `grace` 内未退出则强制中止：运行中条目以 `Indeterminate` 结算、
    /// 排队条目以 `Cancelled` 结算——收据等待者不会永久挂起，Journal 也不
    /// 残留 `Running`（§93 停机语义）。
    pub async fn join(&self, grace: std::time::Duration, ctx: &Arc<EngineContext>) {
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
                        "控制队列 worker 停机超时，已强制中止并结算遗留请求"
                    );
                    self.settle_abandoned(ctx).await;
                }
            }
        }
    }

    /// 强制中止后结算遗留请求（P1-4）：运行中条目结果未知 → `Indeterminate`
    /// （与 P1-7 一致：执行器可能在飞行中，不得宣称未执行）；排队条目从未
    /// 执行 → `Cancelled`。结算在释放队列锁之后进行（`settle_entry` 会访问
    /// Journal/审计/active，不得持有队列锁）。
    ///
    /// 三审 P1：`running` 保留到 worker 结算完成才清除——若强制中止恰好发生在
    /// worker 的异步 Journal 结算期间，`settle_abandoned` 仍能找到该条目并结算
    /// （收据不永久挂起、Journal 不残留 Running）。已结算的条目（`reply` 已
    /// 写入）跳过，避免重复结算。
    ///
    /// 已知竞态（可接受）：中止若落在 worker 的 `settle_record` 阻塞任务执行
    /// 期间，被丢弃的任务仍可能写入真实结果，与本函数的 QUEUE_WORKER_ABORTED
    /// 形成"最后写者胜出"——Journal 终态不确定，但收据方向安全（至多
    /// Indeterminate，绝不向调用方宣称成功；§80.1 本就禁止对 Indeterminate
    /// 盲目重放）。
    async fn settle_abandoned(&self, ctx: &Arc<EngineContext>) {
        let running: Option<QueuedEntry>;
        let mut queued: Vec<QueuedEntry> = Vec::new();
        let mut taken: usize = 0;
        {
            let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
            inner.closed = true;
            running = inner.running.take();
            for deque in inner.entries.values_mut() {
                while let Some(entry) = deque.pop_front() {
                    taken += 1;
                    queued.push(entry);
                }
            }
            inner.len = inner.len.saturating_sub(taken);
        }
        if let Some(entry) = running.filter(|e| !e.reply.is_set()) {
            settle_entry(
                ctx,
                entry,
                RunResult::Indeterminate(
                    "QUEUE_WORKER_ABORTED",
                    "控制队列强制中止，执行结果未知（驱动可能已下发）",
                ),
            )
            .await;
        }
        for entry in queued {
            settle_entry(ctx, entry, RunResult::Cancelled).await;
        }
    }

    /// 惰性启动 worker（每设备一个，串行执行，§87）。
    ///
    /// 四审 P2：worker 任务 panic 后句柄仍在但任务已死——`is_finished()`
    /// 检测到已结束的 worker 时重新拉起，避免队列永久失活（排队请求无人
    /// 处理、收据永久挂起）。
    fn ensure_worker(self: &Arc<Self>, ctx: &Arc<EngineContext>) {
        let mut worker = self.worker.lock().expect("DeviceQueue worker 锁被毒化");
        if worker.as_ref().is_none_or(|h| h.is_finished()) {
            let this = self.clone();
            let ctx = ctx.clone();
            *worker = Some(tokio::spawn(async move {
                this.worker_loop(&ctx).await;
            }));
        }
    }

    async fn worker_loop(self: &Arc<Self>, ctx: &Arc<EngineContext>) {
        // 四审 P2：前一个 worker panic 遗留的"运行中"条目在此结算——
        // 执行结果未知（Indeterminate），收据与 Journal 不残留。
        let orphan = {
            let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
            inner.running.take()
        };
        if let Some(entry) = orphan.filter(|e| !e.reply.is_set()) {
            warn!(
                component = "control-engine",
                device_id = %self.device_id,
                request_id = %entry.key.request_id,
                error_code = "queue_worker_panic",
                "检测到已终止 worker 的遗留请求，以 Indeterminate 结算"
            );
            settle_entry(
                ctx,
                entry,
                RunResult::Indeterminate(
                    "QUEUE_WORKER_ABORTED",
                    "控制队列 worker 异常终止，执行结果未知",
                ),
            )
            .await;
        }
        loop {
            // 取最高优先级、FIFO 顺序的请求；已取消/已过期的就地弹出并结算。
            let picked: Option<Picked> = {
                let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                // P1-A：引擎已停机时关闭本队列——排空后 worker 退出，即使队列
                // 在停机排空后被重建（enqueue 已拒绝，不会再有新条目），
                // worker 也不会残留运行。
                if self.closed_flag.load(Ordering::SeqCst) {
                    inner.closed = true;
                }
                if inner.closed && inner.len == 0 {
                    return;
                }
                let mut picked = None;
                // 四审 P2：优先级老化——低优先级请求排队超过阈值后按停留
                // 时长逐级提升有效优先级（至 Critical），防止严格优先级调度
                // 长期饿死低优先级队列（过期/取消条目直接取走结算，不参与
                // 比较）。同级内仍保持 FIFO；同有效优先级时基础优先级高者
                // 先行（降序遍历 + 严格大于实现）。
                let aging_ms = ctx.policy.priority_aging_ms;
                let now_ns = now_ns();
                let mut best: Option<(i64, Priority)> = None;
                for (prio, deque) in inner.entries.iter().rev() {
                    let Some(front) = deque.front() else {
                        continue;
                    };
                    let ready = !front.is_cancelled() && Instant::now() < front.deadline;
                    let base = *prio as i64;
                    let boost = if aging_ms > 0 && ready {
                        let waited_ms =
                            now_ns.saturating_sub(front.audit_meta.queued_at_ns) / 1_000_000;
                        (waited_ms / aging_ms as i64).clamp(0, 3 - base)
                    } else {
                        0
                    };
                    let eff = base + boost;
                    if best.is_none_or(|(best_eff, _)| eff > best_eff) {
                        best = Some((eff, *prio));
                    }
                }
                if let Some((_, prio)) = best {
                    let deque = inner.entries.get_mut(&prio).expect("存在");
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
                    settle_entry(ctx, picked.entry, RunResult::Cancelled).await;
                }
                PickedKind::Expired => {
                    settle_entry(ctx, picked.entry, RunResult::Timeout).await;
                }
                PickedKind::Ready => {
                    let entry = picked.entry;
                    {
                        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                        inner.running = Some(entry.clone());
                    }
                    let result = run_entry(ctx, &entry).await;
                    // 三审 P1：结算完成前不清除 running——否则强制中止恰好在
                    // 异步 Journal 结算期间发生时，settle_abandoned 找不到该
                    // 条目，收据会永久挂起且 Journal 残留 Running。
                    settle_entry(ctx, entry, result).await;
                    {
                        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                        inner.running = None;
                    }
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
    /// 执行器在飞行中被超时/取消/中止打断，结果未知（§80.1：不得宣称未执行）。
    Indeterminate(&'static str, &'static str),
}

/// 执行 + 超时 + 取消三路竞争（§77、§80.1）。
///
/// P1-7：执行器一旦开始（`started` 标记），超时/取消不得再宣称
/// `Timeout`/`Cancelled`——驱动可能已下发控制但结果未返回，此时以
/// `Indeterminate` 结算（禁止上层盲目重试）；只有排队期间已过期/已取消
/// （执行器从未被轮询）才保持 `Timeout`/`Cancelled`。
async fn run_entry(ctx: &Arc<EngineContext>, entry: &QueuedEntry) -> RunResult {
    let remaining = entry.deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return RunResult::Timeout;
    }
    let cancel = entry.cancel.clone();
    let started = Arc::new(AtomicBool::new(false));
    let flag = started.clone();
    let operation = entry.operation.clone();
    let device_id = entry.key.device_id.clone();
    let executor_fut = async move {
        flag.store(true, Ordering::SeqCst);
        run_operation(ctx, &device_id, &operation).await
    };
    tokio::pin!(executor_fut);
    tokio::select! {
        _ = tokio::time::sleep(remaining) => {
            if started.load(Ordering::SeqCst) {
                RunResult::Indeterminate("TIMEOUT", "控制请求超时且结果不确定（驱动可能已下发）")
            } else {
                RunResult::Timeout
            }
        }
        _ = cancel.cancelled() => {
            if started.load(Ordering::SeqCst) {
                RunResult::Indeterminate("CANCELLED", "控制请求已取消且结果不确定（驱动可能已下发）")
            } else {
                RunResult::Cancelled
            }
        }
        outcome = &mut executor_fut => RunResult::Done(outcome),
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
                        .collect::<Vec<_>>();
                    // P2-12：批量写入部分失败不得宣称顶层成功（北向状态须自洽）。
                    let any_failed = item_results.iter().any(|r| !r.success);
                    let status = if any_failed {
                        observation_model::ControlStatus::Failed
                    } else {
                        observation_model::ControlStatus::Succeeded
                    };
                    let error = any_failed.then(|| observation_model::ControlError {
                        code: "PARTIAL_WRITE_FAILURE".to_owned(),
                        message: "批量写入存在失败项（详见逐项结果）".to_owned(),
                        details: None,
                    });
                    ControlResult {
                        status,
                        error,
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
        ValidatedOperation::Execute {
            command,
            preconditions,
            risk_level: _,
        } => {
            // 四审 P1（TOCTOU）：入队前的前置条件检查通过不代表执行时仍满足
            // ——设备状态可能在排队期间变化。Driver 调用前必须复查；此时
            // 尚未下发，失败为确定性失败（`Failed`，非 Indeterminate）。
            // 检查器缺失同样 fail-closed（与提交期语义一致）；单次检查受
            // 策略超时约束，卡死的检查器不得占用设备 worker。
            if !preconditions.is_empty() {
                let failure = match ctx.policy.precondition_checker() {
                    None => Some(observation_model::ControlError {
                        code: "PRECONDITION_UNCONFIGURED".to_owned(),
                        message: "命令声明了前置条件但引擎未配置前置条件检查器".to_owned(),
                        details: None,
                    }),
                    Some(checker) => {
                        let check = checker.check(device_id, preconditions);
                        match tokio::time::timeout(
                            std::time::Duration::from_millis(ctx.policy.precondition_timeout_ms),
                            check,
                        )
                        .await
                        {
                            Err(_) => Some(observation_model::ControlError {
                                code: "PRECONDITION_TIMEOUT".to_owned(),
                                message: format!(
                                    "前置条件检查超时（{} ms）",
                                    ctx.policy.precondition_timeout_ms
                                ),
                                details: None,
                            }),
                            Ok(Err(e)) => Some(observation_model::ControlError {
                                code: "PRECONDITION_FAILED".to_owned(),
                                message: e.message,
                                details: None,
                            }),
                            Ok(Ok(())) => None,
                        }
                    }
                };
                if let Some(error) = failure {
                    return ControlResult {
                        status: observation_model::ControlStatus::Failed,
                        error: Some(error),
                        ..base
                    };
                }
            }
            let outcome = ctx.executor.execute(device_id, command).await;
            match outcome {
                crate::executor::ExecuteOutcome::Succeeded(raw) => {
                    if raw.success {
                        ControlResult {
                            status: observation_model::ControlStatus::Succeeded,
                            result: Some(observation_model::ControlPayloadResult::Command(
                                observation_model::CommandResult {
                                    device_code: raw.protocol_code,
                                    // P1-8：不得把 Driver 原始错误文本透传给北向
                                    // （可能含路径/地址等敏感细节，§90.1）。
                                    message: None,
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
async fn settle_entry(ctx: &Arc<EngineContext>, entry: QueuedEntry, run: RunResult) {
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
        RunResult::Indeterminate(code, message) => ControlResult {
            request_id: entry.key.request_id.clone(),
            namespace: entry.key.namespace.clone(),
            device_id: entry.key.device_id.clone(),
            status: observation_model::ControlStatus::Indeterminate,
            started_at_ns: Some(completed_at_ns),
            completed_at_ns: Some(completed_at_ns),
            result: None,
            error: Some(observation_model::ControlError {
                code: code.to_owned(),
                message: message.to_owned(),
                details: None,
            }),
        },
    };
    result.completed_at_ns = Some(completed_at_ns);
    // 回填信封标识（§89：Done 路径由执行器构造，须统一回填 request_id/namespace）。
    result.request_id = entry.key.request_id.clone();
    result.namespace = entry.key.namespace.clone();

    // 幂等结算（§80.1：下发前已持久化，完成后更新状态与结果）。
    // P2-H：磁盘 I/O 在阻塞线程池执行，不占用 Tokio worker。
    if let Err(e) =
        crate::journal::settle_record(&ctx.journal, &ctx.journal_io_gate, &entry.key, &result).await
    {
        warn!(
            component = "control-engine",
            request_id = %entry.key.request_id,
            error_code = "journal_settle_failed",
            "幂等结算落盘失败: {e}"
        );
        // P1-2：结算失败不得向调用方宣称成功——当前进程与重启恢复
        // （Indeterminate）必须一致。降级为 Indeterminate（原始错误只进日志，
        // 不进入北向结果）。
        result = ControlResult {
            status: observation_model::ControlStatus::Indeterminate,
            error: Some(observation_model::ControlError {
                code: "JOURNAL_SETTLE_FAILED".to_owned(),
                message: "幂等结算持久化失败，结果不确定".to_owned(),
                details: None,
            }),
            ..result
        };
    }
    // 从活跃登记移除（幂等 Duplicate 的等待者此后从 Journal 取已结算结果）。
    ctx.active
        .lock()
        .expect("active 锁被毒化")
        .remove(&entry.key);

    // 审计（§90：每个反向控制必须记录；含拒绝/超时/取消）。
    // 四审 P2：有界超时，慢审计不阻塞设备 worker。
    let duration_ms = completed_at_ns
        .saturating_sub(entry.audit_meta.queued_at_ns)
        .max(0) as u64
        / 1_000_000;
    crate::audit::record_bounded(
        &ctx.audit,
        ctx.policy.audit_timeout_ms,
        crate::audit::build_event(
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
        ),
    )
    .await;

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
