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
use std::time::{Duration, Instant};

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
    /// 已脱离 `entries`、正在等待/处于结算中的条目（五审回归 P1）。
    ///
    /// 共享可见不变量：任何条目在 `reply.is_set()` 之前必须始终可从共享
    /// 状态（`entries`/`running`/`draining`）到达——worker 只能在
    /// `settle_entry` 完成后才把它从共享结构移除。否则强制中止落在结算
    /// await 点时，条目随 worker 本地状态一起丢失，收据永久挂起且 Journal
    /// 残留 Running（`settle_abandoned` 无法接管）。
    draining: VecDeque<QueuedEntry>,
    /// 不确定结果冷却期截止时刻（五审 P1）：底层物理动作可能仍在进行，
    /// 冷却期内 worker 不启动新动作。
    cooldown_until: Option<Instant>,
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
    /// 引擎上下文（五审 S3：监督任务重启 worker / 冷却期判定所需）。
    ctx: Arc<EngineContext>,
}

impl DeviceQueue {
    pub fn new(
        device_id: DeviceId,
        capacity: usize,
        closed_flag: Arc<AtomicBool>,
        ctx: Arc<EngineContext>,
    ) -> Self {
        Self {
            device_id,
            inner: Mutex::new(QueueInner {
                entries: BTreeMap::new(),
                capacity,
                closed: false,
                len: 0,
                running: None,
                draining: VecDeque::new(),
                cooldown_until: None,
            }),
            closed_flag,
            notify: Notify::new(),
            worker: Mutex::new(None),
            ctx,
        }
    }

    /// 是否处于不确定结果冷却期（五审 P1：提交侧拒绝新动作）。
    pub fn in_cooldown(&self) -> bool {
        let inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
        inner.cooldown_until.is_some_and(|t| t > Instant::now())
    }

    /// 结算一个已脱离 `entries` 的条目，期间保持共享可见（五审回归 P1）。
    ///
    /// 条目先登记进共享 `inner.draining`，`settle_entry` 完成后才从共享
    /// 结构移除——强制中止无论落在哪个 await 点，[`Self::settle_abandoned`]
    /// 都能从 `draining` 接管收据未写的条目（收据不永久挂起、Journal 不
    /// 残留 Running）。移除时顺带清理 `draining` 队头所有已写收据的条目
    /// （含此前结算完未及移除的残留克隆）。
    async fn settle_tracked(
        self: &Arc<Self>,
        ctx: &Arc<EngineContext>,
        entry: QueuedEntry,
        run: RunResult,
    ) {
        {
            let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
            inner.draining.push_back(entry.clone());
        }
        settle_entry(ctx, entry, run, None).await;
        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
        while inner
            .draining
            .front()
            .is_some_and(|front| front.reply.is_set())
        {
            inner.draining.pop_front();
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
        // §34.2.1：入队成功，队列深度 +1（结算时 -1 配对）。
        ctx.metrics.observe_enqueued();
        self.ensure_worker();
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
    /// 残留 `Running`（§93 停机语义）。worker 在停机窗口内 panic
    /// （`Ok(Err(JoinError))`）同样触发结算——此时 supervisor 因
    /// `closed_flag` 置位不再重启，遗留条目仅此路径可接管（五审回归 P1）。
    ///
    /// 五审 S5：强制中止后的结算阶段有独立总预算（约 grace 的一半，钳制在
    /// 50ms~500ms）——极端磁盘卡死时，超预算的条目跳过 Journal 落盘、仅
    /// 回填收据（Journal 停留 Running，重启恢复为 Indeterminate，语义
    /// 一致），保证停机总时长有界。
    pub async fn join(&self, grace: std::time::Duration, ctx: &Arc<EngineContext>) {
        let handle = self
            .worker
            .lock()
            .expect("DeviceQueue worker 锁被毒化")
            .take();
        if let Some(handle) = handle {
            let abort = handle.abort_handle();
            // 结算预算（五审 S5）：grace 的一半，钳制在 50ms~500ms——超预算
            // 条目跳过 Journal 落盘仅回填收据，停机总时长严格有界。
            let settle_budget = (grace / 2)
                .max(std::time::Duration::from_millis(50))
                .min(std::time::Duration::from_millis(500));
            match tokio::time::timeout(grace, handle).await {
                // 正常退出：worker 已自行排空（closed 且无 running/draining），
                // 无遗留条目。
                Ok(Ok(())) => {}
                // 五审回归 P1：worker 在停机窗口内 panic（`JoinError`）——
                // supervisor 因 `closed_flag` 已置位不再重启，遗留的
                // running/draining 条目只有此处能接管；当作正常退出吞掉会
                // 导致收据永久挂起、Journal 残留 Running。
                Ok(Err(_)) => {
                    warn!(
                        component = "control-engine",
                        device_id = %self.device_id,
                        error_code = "queue_worker_panic",
                        "控制队列 worker 在停机窗口内异常终止，结算遗留请求"
                    );
                    self.settle_abandoned(ctx, settle_budget).await;
                }
                Err(_) => {
                    abort.abort();
                    warn!(
                        component = "control-engine",
                        device_id = %self.device_id,
                        error_code = "queue_worker_join_timeout",
                        "控制队列 worker 停机超时，已强制中止并结算遗留请求"
                    );
                    self.settle_abandoned(ctx, settle_budget).await;
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
    /// 五审 S5：`budget` 为结算阶段总预算——每条目的 Journal 落盘受剩余预算
    /// 约束；预算耗尽后跳过落盘仅回填收据（Journal 停留 Running，重启恢复
    /// Indeterminate），保证停机总时长严格有界。
    ///
    /// 已知竞态（可接受）：中止若落在 worker 的 `settle_record` 阻塞任务执行
    /// 期间，被丢弃的任务仍可能写入真实结果，与本函数的 QUEUE_WORKER_ABORTED
    /// 形成"最后写者胜出"——Journal 终态不确定，但收据方向安全（至多
    /// Indeterminate，绝不向调用方宣称成功；§80.1 本就禁止对 Indeterminate
    /// 盲目重放）。
    ///
    /// 五审回归 P1：接管范围含共享 `draining`——排空/拾取取消路径的在途条目
    /// 登记其中，强制中止落在 worker 结算 await 点时由此结算（收据不永久
    /// 挂起、Journal 不残留 Running）。`draining` 中已写收据的条目（worker
    /// 结算完未及移除的克隆）跳过，避免重复结算。
    async fn settle_abandoned(&self, ctx: &Arc<EngineContext>, budget: std::time::Duration) {
        let running: Option<QueuedEntry>;
        let mut queued: Vec<QueuedEntry> = Vec::new();
        let mut draining: Vec<QueuedEntry> = Vec::new();
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
            while let Some(entry) = inner.draining.pop_front() {
                draining.push(entry);
            }
            inner.len = inner.len.saturating_sub(taken);
        }
        let deadline = Instant::now() + budget;
        if let Some(entry) = running.filter(|e| !e.reply.is_set()) {
            settle_entry(
                ctx,
                entry,
                RunResult::Indeterminate(
                    "QUEUE_WORKER_ABORTED",
                    "控制队列强制中止，执行结果未知（驱动可能已下发）",
                ),
                Some(deadline),
            )
            .await;
        }
        for entry in queued
            .into_iter()
            .chain(draining)
            .filter(|e| !e.reply.is_set())
        {
            settle_entry(ctx, entry, RunResult::Cancelled, Some(deadline)).await;
        }
    }

    /// 惰性启动 worker（每设备一个，串行执行，§87）。
    ///
    /// 四审 P2：worker 任务 panic 后句柄仍在但任务已死——`is_finished()`
    /// 检测到已结束的 worker 时重新拉起，避免队列永久失活（排队请求无人
    /// 处理、收据永久挂起）。
    ///
    /// 五审 S3：每次拉起 worker 时同时派生监督任务（500ms 轮询）——worker
    /// 异常终止（panic）时自动重启，不依赖下一次入队/取消等外部触发；
    /// 引擎停机后监督结束。
    fn ensure_worker(self: &Arc<Self>) {
        let mut worker = self.worker.lock().expect("DeviceQueue worker 锁被毒化");
        if worker.as_ref().is_none_or(|h| h.is_finished()) {
            let this = self.clone();
            *worker = Some(tokio::spawn(async move {
                this.worker_loop().await;
            }));
            drop(worker);
            // 五审 S3：监督任务轮询 worker 存活状态（500ms 周期）——panic
            // 后自动重启，不依赖下一次入队等外部触发。JoinHandle 不可克隆，
            // 用 is_finished 轮询实现；正常退出（停机排空）后随 closed_flag
            // 结束监督。
            let supervisor = self.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if supervisor.closed_flag.load(Ordering::SeqCst) {
                        return;
                    }
                    let dead = supervisor
                        .worker
                        .lock()
                        .expect("DeviceQueue worker 锁被毒化")
                        .as_ref()
                        .is_some_and(|h| h.is_finished());
                    if dead {
                        warn!(
                            component = "control-engine",
                            device_id = %supervisor.device_id,
                            error_code = "queue_worker_panic_respawn",
                            "控制队列 worker 异常终止，已自动重启"
                        );
                        supervisor.ensure_worker();
                        return;
                    }
                }
            });
        }
    }

    async fn worker_loop(self: &Arc<Self>) {
        let ctx = self.ctx.clone();
        // 四审 P2：前一个 worker panic 遗留的"运行中"条目在此结算——
        // 执行结果未知（Indeterminate），收据与 Journal 不残留。
        // 五审回归 P1：遗留的 `draining` 条目（排空/拾取取消路径在途）一并
        // 收养（从未下发 → Cancelled）；两者均经 `settle_tracked` 结算，
        // 本 worker 自身被中止时仍保持共享可见。
        let (orphan, orphan_draining) = {
            let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
            (inner.running.take(), std::mem::take(&mut inner.draining))
        };
        if let Some(entry) = orphan.filter(|e| !e.reply.is_set()) {
            warn!(
                component = "control-engine",
                device_id = %self.device_id,
                request_id = %entry.key.request_id,
                error_code = "queue_worker_panic",
                "检测到已终止 worker 的遗留请求，以 Indeterminate 结算"
            );
            self.settle_tracked(
                &ctx,
                entry,
                RunResult::Indeterminate(
                    "QUEUE_WORKER_ABORTED",
                    "控制队列 worker 异常终止，执行结果未知",
                ),
            )
            .await;
        }
        for entry in orphan_draining {
            if entry.reply.is_set() {
                continue;
            }
            self.settle_tracked(&ctx, entry, RunResult::Cancelled).await;
        }
        loop {
            // ---- 阶段 A：停机排空（独立锁作用域，await 不持有队列锁）----
            // 五审回归 P1：排队条目移入共享 `draining`（而非 worker 本地
            // 容器）后逐条结算队头——强制中止无论落在哪个结算 await 点，
            // `settle_abandoned` 都能从 `draining` 接管，条目不随 worker
            // 本地状态丢失。
            let drain_front: Option<QueuedEntry> = {
                let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                // P1-A：引擎已停机时关闭本队列——排空后 worker 退出，即使队列
                // 在停机排空后被重建（enqueue 已拒绝，不会再有新条目），
                // worker 也不会残留运行。
                if self.closed_flag.load(Ordering::SeqCst) {
                    inner.closed = true;
                }
                if !inner.closed {
                    None
                } else {
                    // 五审 P1：停机后不得启动新动作——排队条目移入 `draining`
                    // 就地以 `Cancelled` 结算；仅允许已在执行的条目自然完成
                    // （受 join grace 约束），完成后下一轮循环退出。
                    if inner.len > 0 {
                        let mut moved: Vec<QueuedEntry> = Vec::with_capacity(inner.len);
                        for deque in inner.entries.values_mut() {
                            while let Some(entry) = deque.pop_front() {
                                moved.push(entry);
                            }
                        }
                        for entry in moved {
                            inner.draining.push_back(entry);
                        }
                        inner.len = 0;
                    }
                    if inner.draining.is_empty() {
                        if inner.running.is_none() {
                            return;
                        }
                        None
                    } else {
                        // 队头克隆结算（settle_tracked 完成后才真正移除）。
                        inner.draining.front().cloned()
                    }
                }
            };
            if let Some(entry) = drain_front {
                self.settle_tracked(&ctx, entry, RunResult::Cancelled).await;
                continue;
            }

            // ---- 阶段 B：冷却判定 + 挑选（独立锁作用域）----
            // 取最高优先级、FIFO 顺序的请求；已取消/已过期的就地弹出并结算。
            let mut cooldown_remaining = Duration::ZERO;
            let picked: Option<Picked> = {
                let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                // 五审 P1：不确定结果冷却期——底层物理动作可能仍在进行，
                // 冷却期内不启动新动作（已有条目暂缓，等冷却结束再调度）。
                if let Some(until) = inner.cooldown_until {
                    let remaining = until.saturating_duration_since(Instant::now());
                    if !remaining.is_zero() {
                        cooldown_remaining = remaining;
                    }
                }
                let mut picked = None;
                if cooldown_remaining.is_zero() {
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
                }
                picked
            };

            let Some(picked) = picked else {
                // 空队列（或冷却期内）：等待唤醒或冷却截止。
                if cooldown_remaining.is_zero() {
                    self.notify.notified().await;
                } else {
                    tokio::select! {
                        _ = self.notify.notified() => {}
                        _ = tokio::time::sleep(cooldown_remaining) => {}
                    }
                }
                continue;
            };

            match picked.kind {
                PickedKind::Cancelled => {
                    // 五审回归 P1：结算期间登记共享 `draining`——强制中止落在
                    // 结算 await 点时 settle_abandoned 可接管（与排空路径同一
                    // 共享可见不变量）。
                    self.settle_tracked(&ctx, picked.entry, RunResult::Cancelled)
                        .await;
                }
                PickedKind::Expired => {
                    self.settle_tracked(&ctx, picked.entry, RunResult::Timeout)
                        .await;
                }
                PickedKind::Ready => {
                    let entry = picked.entry;
                    {
                        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                        inner.running = Some(entry.clone());
                    }
                    let result = run_entry(&ctx, &entry).await;
                    // 五审 P1：执行器已开始但结果不确定（超时/取消/中止打断），
                    // 或执行器自报 Indeterminate——底层物理动作可能仍在进行，
                    // 进入设备冷却期，冷却结束前不启动新动作。
                    let indeterminate = matches!(&result, RunResult::Indeterminate(..))
                        || matches!(&result, RunResult::Done(r)
                            if r.status == observation_model::ControlStatus::Indeterminate);
                    if indeterminate && ctx.policy.indeterminate_cooldown_ms > 0 {
                        let mut inner = self.inner.lock().expect("DeviceQueue 锁被毒化");
                        inner.cooldown_until = Some(
                            Instant::now()
                                + Duration::from_millis(ctx.policy.indeterminate_cooldown_ms),
                        );
                        // §34.2.1：冷却期建立计数（不确定结果后拒绝新动作的窗口）。
                        ctx.metrics.observe_cooldown_entered();
                    }
                    // 三审 P1：结算完成前不清除 running——否则强制中止恰好在
                    // 异步 Journal 结算期间发生时，settle_abandoned 找不到该
                    // 条目，收据会永久挂起且 Journal 残留 Running。
                    settle_entry(&ctx, entry, result, None).await;
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
///
/// `deadline`（五审 S5 + 五审回归）：可选结算总预算截止时刻，Journal 与
/// 审计共享同一预算——`None` 正常落盘/审计；`Some(dl)` 时两者各自受剩余
/// 预算约束，预算耗尽跳过该步骤（Journal 停留 Running 由重启恢复为
/// Indeterminate；审计丢弃并显式留痕），收据照常回填，停机总时长严格
/// 有界。仅停机结算路径传入。
async fn settle_entry(
    ctx: &Arc<EngineContext>,
    entry: QueuedEntry,
    run: RunResult,
    deadline: Option<Instant>,
) {
    let completed_at_ns = now_ns();
    // §34.2.1：本条目离开队列（结算完成），深度 -1；终态计数按状态归类。
    ctx.metrics.observe_settled_exit();
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
    // 五审 S5：停机预算耗尽（剩余 0）或单次落盘超时时跳过 Journal 更新——
    // 收据照常回填，Journal 停留 Running 由重启恢复兜底。
    let journal_timeout = deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
    let skip_journal = journal_timeout.is_some_and(|d| d.is_zero());
    let settle_result = {
        let settle =
            crate::journal::settle_record(&ctx.journal, &ctx.journal_io_gate, &entry.key, &result);
        if skip_journal {
            warn!(
                component = "control-engine",
                request_id = %entry.key.request_id,
                error_code = "journal_settle_skipped",
                "停机结算预算耗尽，跳过幂等落盘（重启恢复为 Indeterminate）"
            );
            Ok(())
        } else if let Some(timeout) = journal_timeout {
            match tokio::time::timeout(timeout, settle).await {
                Err(_) => {
                    warn!(
                        component = "control-engine",
                        request_id = %entry.key.request_id,
                        error_code = "journal_settle_timeout",
                        "幂等结算落盘超时，跳过（重启恢复为 Indeterminate）"
                    );
                    Ok(())
                }
                Ok(r) => r,
            }
        } else {
            settle.await
        }
    };
    if let Err(e) = settle_result {
        warn!(
            component = "control-engine",
            request_id = %entry.key.request_id,
            error_code = "journal_settle_failed",
            "幂等结算落盘失败: {e}"
        );
        ctx.metrics.observe_journal_settle_failed();
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
    // 五审回归：审计与 Journal 共享停机结算总预算——预算耗尽跳过本条审计
    // 并显式留痕（§90"必审计"与"有界停机"冲突时，停机路径选择丢弃）；
    // 正常路径传 None，维持单条 audit_timeout_ms 上界语义。
    let duration_ms = completed_at_ns
        .saturating_sub(entry.audit_meta.queued_at_ns)
        .max(0) as u64
        / 1_000_000;
    let audit_budget_exhausted = deadline
        .map(|dl| dl.saturating_duration_since(Instant::now()).is_zero())
        .unwrap_or(false);
    if audit_budget_exhausted {
        warn!(
            component = "control-engine",
            request_id = %entry.key.request_id,
            error_code = "audit_skipped_shutdown_budget",
            "停机结算预算耗尽，跳过审计写入"
        );
    } else {
        crate::audit::record_bounded(
            &ctx.audit,
            ctx.policy.audit_timeout_ms,
            deadline,
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
    }

    // 回传结果（§77 异步控制：提交后等待/轮询结果）。
    // §34.2.1：按最终终态（含 Journal 失败降级后的 Indeterminate）计数。
    ctx.metrics.observe_settled(result.status);
    entry.reply.set(result);
}

/// 当前时间（UTC Unix Epoch 纳秒）。
pub(crate) fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
