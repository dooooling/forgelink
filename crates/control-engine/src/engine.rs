//! 控制引擎入口（§81 Normative）。
//!
//! 统一链路：认证/授权 → 校验 → 策略/前置条件 → 每设备队列 → Profile 映射 →
//! Driver（§81）。本模块实现 [`ControlEngine`] 提交/取消/查询/停机，
//! 以及统一请求生命周期（§77、§80.1）：
//!
//! ```text
//! Accepted → Running → Succeeded / Failed / Timeout / Cancelled / Indeterminate
//! ```
//!
//! 校验/授权/前置条件/队列满等失败在 Driver 前以 `Rejected` 结算（§84）；
//! 幂等命中（§80.1）直接返回既有结果，不重复执行。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use observation_model::{
    ControlError, ControlOperation, ControlRequest, ControlResult, ControlStatus, DeviceId,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::audit::{AuditOperation, AuditTarget, summarize_parameters};
use crate::catalog::DeviceCatalog;
use crate::executor::ControlExecutor;
use crate::journal::{ControlJournal, IdempotencyKey, JournalDecision, payload_hash};
use crate::policy::ControlPolicy;
use crate::queue::{AuditMeta, CancelOutcome, DeviceQueue, EnqueueError, QueuedEntry, now_ns};
use crate::role::Authorizer;
use crate::validate::{ValidatedOperation, validate_command, validate_property_write};

/// 引擎内部上下文（提交后的执行路径共享）。
pub(crate) struct EngineContext {
    pub executor: Arc<dyn ControlExecutor>,
    pub journal: Arc<dyn ControlJournal>,
    pub audit: Arc<dyn crate::audit::AuditSink>,
    pub policy: Arc<ControlPolicy>,
    /// 活跃（未结算）请求的共享结果：幂等 Duplicate 时等待首个请求的最终结果。
    pub active: Mutex<HashMap<IdempotencyKey, Arc<SharedResult>>>,
}

/// 引擎构造配置（§81 依赖装配）。
pub struct ControlEngineConfig {
    /// 设备目录（存在性 + 启用状态 + Profile，§81）。
    pub catalog: Arc<dyn DeviceCatalog>,
    /// 授权器（§83，可替换）。
    pub authorizer: Arc<dyn Authorizer>,
    /// 幂等 Journal（§80.1）。
    pub journal: Arc<dyn ControlJournal>,
    /// 控制执行器（§88）。
    pub executor: Arc<dyn ControlExecutor>,
    /// 审计输出（§90）。
    pub audit: Arc<dyn crate::audit::AuditSink>,
    /// 策略（§86）。
    pub policy: Arc<ControlPolicy>,
}

/// 提交上下文（§90：谁、来自哪里）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitContext {
    /// 调用者/用户。
    pub subject: String,
    /// 来源地址（如 `rest:127.0.0.1:53211`）。
    pub source: String,
}

/// 提交错误（信封/幂等冲突/停机等无法产生 `ControlResult` 的情形）。
#[derive(Debug)]
pub enum SubmitError {
    /// 同 key + 不同 payload（§80.1）。
    Conflict {
        existing: crate::journal::JournalEntry,
    },
    /// 信封非法：`request_id` 为空或 `timeout_ms == 0`。
    InvalidRequest { code: &'static str, message: String },
    /// 引擎已停机。
    EngineClosed,
}

impl std::fmt::Display for SubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubmitError::Conflict { .. } => write!(f, "幂等冲突：同 key 不同 payload"),
            SubmitError::InvalidRequest { code, message } => write!(f, "{code}: {message}"),
            SubmitError::EngineClosed => write!(f, "控制引擎已停机"),
        }
    }
}

impl std::error::Error for SubmitError {}

/// 取消错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelError {
    /// 未找到（已结算或未知请求）。
    NotFound,
    /// 引擎已停机。
    EngineClosed,
}

impl std::fmt::Display for CancelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CancelError::NotFound => write!(f, "请求不存在（已结算或未知）"),
            CancelError::EngineClosed => write!(f, "控制引擎已停机"),
        }
    }
}

impl std::error::Error for CancelError {}

/// 控制请求收据（§77 异步控制）。
///
/// 幂等命中或即时拒绝时立即就绪（[`ControlReceipt::is_ready`]）；
/// 否则等待 worker 执行完成（[`ControlReceipt::wait`]）。
pub struct ControlReceipt {
    ready: Option<ControlResult>,
    pending: Option<SharedResultWaiter>,
}

struct SharedResultWaiter {
    shared: Arc<SharedResult>,
    _cancel: CancellationToken,
}

impl ControlReceipt {
    fn ready(result: ControlResult) -> Self {
        Self {
            ready: Some(result),
            pending: None,
        }
    }

    fn pending(shared: Arc<SharedResult>) -> Self {
        Self {
            ready: None,
            pending: Some(SharedResultWaiter {
                shared,
                _cancel: CancellationToken::new(),
            }),
        }
    }

    /// 是否已就绪（幂等命中 / 即时拒绝）。
    pub fn is_ready(&self) -> bool {
        self.ready.is_some()
    }

    /// 等待最终结果（§77：提交后异步获取结果）。
    pub async fn wait(self) -> ControlResult {
        if let Some(result) = self.ready {
            return result;
        }
        self.pending
            .expect("pending 与 ready 互斥")
            .shared
            .wait()
            .await
    }
}

/// 可共享的最终结果（首个请求的 worker 写入；幂等 Duplicate 的等待者共享）。
#[derive(Debug)]
pub(crate) struct SharedResult {
    state: std::sync::Mutex<Option<ControlResult>>,
    notify: Notify,
}

impl SharedResult {
    pub fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(None),
            notify: Notify::new(),
        }
    }

    /// 写入最终结果并唤醒所有等待者（在幂等结算之后调用）。
    pub fn set(&self, result: ControlResult) {
        *self.state.lock().expect("SharedResult 锁被毒化") = Some(result);
        self.notify.notify_waiters();
    }

    /// 等待最终结果。
    pub async fn wait(&self) -> ControlResult {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.state.lock().expect("SharedResult 锁被毒化").clone() {
                return result;
            }
            notified.await;
        }
    }
}

struct Inner {
    catalog: Arc<dyn DeviceCatalog>,
    authorizer: Arc<dyn Authorizer>,
    context: Arc<EngineContext>,
    queues: Mutex<HashMap<DeviceId, Arc<DeviceQueue>>>,
    closed: AtomicBool,
}

/// 控制引擎（§81 统一入口）。
///
/// 线程安全：`Arc` 共享，可跨任务/线程并发提交。
#[derive(Clone)]
pub struct ControlEngine {
    inner: Arc<Inner>,
}

impl ControlEngine {
    /// 装配引擎（§81）。
    pub fn new(config: ControlEngineConfig) -> Self {
        let context = Arc::new(EngineContext {
            executor: config.executor,
            journal: config.journal,
            audit: config.audit,
            policy: config.policy,
            active: Mutex::new(HashMap::new()),
        });
        Self {
            inner: Arc::new(Inner {
                catalog: config.catalog,
                authorizer: config.authorizer,
                context,
                queues: Mutex::new(HashMap::new()),
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// 提交控制请求（§81 统一链路入口）。
    ///
    /// 流程：信封校验 → 幂等登记（§80.1）→ 设备存在/启用 → 校验与映射
    /// （§75/§84）→ 前置条件（§85）→ 授权（§83）→ 入队（§87）。
    ///
    /// 返回收据：幂等命中/即时拒绝已就绪；否则等待 worker 执行结果。
    pub async fn submit(
        &self,
        request: ControlRequest,
        ctx: &SubmitContext,
    ) -> Result<ControlReceipt, SubmitError> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SubmitError::EngineClosed);
        }

        // 1. 信封校验。
        if request.request_id.is_empty() {
            return Err(SubmitError::InvalidRequest {
                code: "EMPTY_REQUEST_ID",
                message: "request_id 不能为空".to_owned(),
            });
        }
        if request.timeout_ms == 0 {
            return Err(SubmitError::InvalidRequest {
                code: "INVALID_TIMEOUT",
                message: "timeout_ms 必须大于 0".to_owned(),
            });
        }

        // 2. 幂等登记（§80.1：下发前先持久化）。
        let key = IdempotencyKey {
            namespace: request.namespace.clone(),
            device_id: request.device_id.clone(),
            request_id: request.request_id.clone(),
        };
        let hash = payload_hash(&request.operation);
        let now = now_ns();
        let expires = now.saturating_add(
            self.inner
                .context
                .policy
                .idempotency_retention
                .as_nanos()
                .min(i64::MAX as u128) as i64,
        );
        match self
            .inner
            .context
            .journal
            .try_insert(&key, hash.clone(), now, expires)
        {
            JournalDecision::Duplicate(entry) => {
                // 同 key + 同 payload：返回已有结果/状态，不重复执行（§80.1）。
                debug!(
                    component = "control-engine",
                    request_id = %entry.key.request_id,
                    status = ?entry.status,
                    "幂等命中，返回既有结果"
                );
                if let Some(result) = entry.result {
                    return Ok(ControlReceipt::ready(result));
                }
                // 首个请求仍在执行：共享其最终结果。
                if let Some(shared) = self
                    .inner
                    .context
                    .active
                    .lock()
                    .expect("active 锁被毒化")
                    .get(&key)
                    .cloned()
                {
                    return Ok(ControlReceipt::pending(shared));
                }
                // 防御分支（理论上不可达：未结算则 active 必在）：结果不确定。
                return Ok(ControlReceipt::ready(interrupted_result(
                    &request,
                    "EXECUTION_INTERRUPTED",
                    "执行状态未知（进程可能已重启）",
                )));
            }
            JournalDecision::Conflict { existing } => {
                return Err(SubmitError::Conflict { existing });
            }
            JournalDecision::Inserted => {}
        }

        // 3. 设备存在且已启用（§4.2）。
        let Some(device_info) = self.inner.catalog.device(&request.device_id) else {
            return Ok(self.reject(
                &request,
                ctx,
                &key,
                "DEVICE_NOT_FOUND",
                format!("设备 {} 不存在", request.device_id),
            ));
        };
        if !device_info.enabled {
            return Ok(self.reject(
                &request,
                ctx,
                &key,
                "DEVICE_DISABLED",
                format!("设备 {} 已禁用", request.device_id),
            ));
        }

        // 4. 校验与映射（§75/§76/§84；全部在 Driver 前完成）。
        let validated = match &request.operation {
            ControlOperation::PropertyWrite(payload) => {
                match validate_property_write(&device_info.profile, payload) {
                    Ok(op) => op,
                    Err(e) => {
                        return Ok(self.reject(&request, ctx, &key, e.code(), e.to_string()));
                    }
                }
            }
            ControlOperation::CommandExecute(payload) => {
                let op = match validate_command(&device_info.profile, payload) {
                    Ok(op) => op,
                    Err(e) => {
                        return Ok(self.reject(&request, ctx, &key, e.code(), e.to_string()));
                    }
                };
                // 前置条件（§85）：入队前检查，失败在 Driver 前拒绝。
                if let Some(checker) = self.inner.context.policy.precondition_checker() {
                    let ValidatedOperation::Execute { preconditions, .. } = &op else {
                        unreachable!("CommandExecute 校验结果必为 Execute");
                    };
                    if let Err(e) = checker.check(&request.device_id, preconditions) {
                        return Ok(self.reject(
                            &request,
                            ctx,
                            &key,
                            "PRECONDITION_FAILED",
                            e.message,
                        ));
                    }
                }
                op
            }
        };

        // 5. 授权（§83）——入队前。
        let required = self.inner.context.policy.required_role(validated.kind());
        if let Err(e) = self
            .inner
            .authorizer
            .authorize(&ctx.subject, required, &request.device_id)
        {
            return Ok(self.reject(&request, ctx, &key, e.code, e.message));
        }

        // 6. 有效超时 + 截止时间。
        let effective_ms = self
            .inner
            .context
            .policy
            .effective_timeout_ms(validated.kind(), request.timeout_ms);
        let deadline = std::time::Instant::now() + Duration::from_millis(effective_ms);

        // 7. 审计元数据（提交时预生成，§90）。
        let audit_meta = audit_meta_for(&request, validated.risk_level());

        // 8. 入队（§87 每设备有界队列；同设备串行）。
        let queue = self.get_or_create_queue(&request.device_id);
        let shared = Arc::new(SharedResult::new());
        let entry = QueuedEntry {
            key,
            operation: validated,
            subject: ctx.subject.clone(),
            source: ctx.source.clone(),
            deadline,
            cancel: CancellationToken::new(),
            reply: shared.clone(),
            audit_meta,
        };
        match queue.enqueue(entry, &self.inner.context) {
            Ok(()) => {
                // 入队成功后才登记活跃结果（供幂等 Duplicate 等待）。
                self.inner
                    .context
                    .active
                    .lock()
                    .expect("active 锁被毒化")
                    .insert(
                        IdempotencyKey {
                            namespace: request.namespace.clone(),
                            device_id: request.device_id.clone(),
                            request_id: request.request_id.clone(),
                        },
                        shared.clone(),
                    );
                Ok(ControlReceipt::pending(shared))
            }
            Err(EnqueueError::Full { capacity }) => Ok(self.reject(
                &request,
                ctx,
                &IdempotencyKey {
                    namespace: request.namespace.clone(),
                    device_id: request.device_id.clone(),
                    request_id: request.request_id.clone(),
                },
                "QUEUE_FULL",
                format!("设备控制队列已满（容量 {capacity}）"),
            )),
            Err(EnqueueError::Closed) => Err(SubmitError::EngineClosed),
        }
    }

    /// 取消请求（§87：cancel）。
    ///
    /// 排队中的请求被标记取消，worker 以 `Cancelled` 结算；
    /// 执行中的请求通过 `CancellationToken` 中止执行器调用。
    pub async fn cancel(&self, key: &IdempotencyKey) -> Result<(), CancelError> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(CancelError::EngineClosed);
        }
        let queue = {
            let queues = self.inner.queues.lock().expect("queues 锁被毒化");
            queues.get(&key.device_id).cloned()
        };
        let Some(queue) = queue else {
            return Err(CancelError::NotFound);
        };
        match queue.cancel(key) {
            CancelOutcome::Marked => {
                self.notify_queue(key);
                Ok(())
            }
            CancelOutcome::NotFound => Err(CancelError::NotFound),
            CancelOutcome::Closed => Err(CancelError::EngineClosed),
        }
    }

    fn notify_queue(&self, key: &IdempotencyKey) {
        if let Some(queue) = self
            .inner
            .queues
            .lock()
            .expect("queues 锁被毒化")
            .get(&key.device_id)
        {
            queue.wake();
        }
    }

    /// 查询既有结果（§77：异步控制轮询；§80.1 幂等查询）。
    ///
    /// 只返回已结算的结果；未完成或未知请求返回 `None`。
    pub fn status(&self, key: &IdempotencyKey) -> Option<ControlResult> {
        let now = now_ns();
        let _ = self.inner.context.journal.purge_expired(now);
        self.inner.context.journal.get(key).and_then(|e| e.result)
    }

    /// 有序停机（§93 停机语义）。
    ///
    /// 停止接收新请求（`submit` 返回 `EngineClosed`）；每设备队列的 worker
    /// 以 `Cancelled` 结算剩余请求后退出；`grace` 内未退出则强制中止。
    pub async fn shutdown(&self, grace: Duration) {
        self.inner.closed.store(true, Ordering::SeqCst);
        let queues: Vec<Arc<DeviceQueue>> = {
            let mut queues = self.inner.queues.lock().expect("queues 锁被毒化");
            queues.drain().map(|(_, q)| q).collect()
        };
        for queue in &queues {
            queue.shutdown();
        }
        for queue in &queues {
            queue.join(grace).await;
        }
    }

    fn get_or_create_queue(&self, device_id: &DeviceId) -> Arc<DeviceQueue> {
        let mut queues = self.inner.queues.lock().expect("queues 锁被毒化");
        queues
            .entry(device_id.clone())
            .or_insert_with(|| {
                Arc::new(DeviceQueue::new(
                    device_id.clone(),
                    self.inner.context.policy.queue_capacity,
                ))
            })
            .clone()
    }

    /// 校验/授权/前置条件/队列满等 Driver 前的拒绝（§84、§85、§86）。
    ///
    /// 以 `Rejected` 结算：幂等 Journal 记录 + 审计（§90），并立即返回。
    fn reject(
        &self,
        request: &ControlRequest,
        ctx: &SubmitContext,
        key: &IdempotencyKey,
        code: &str,
        message: String,
    ) -> ControlReceipt {
        let result = ControlResult {
            request_id: request.request_id.clone(),
            namespace: request.namespace.clone(),
            device_id: request.device_id.clone(),
            status: ControlStatus::Rejected,
            started_at_ns: None,
            completed_at_ns: Some(now_ns()),
            result: None,
            error: Some(ControlError {
                code: code.to_owned(),
                message,
                details: None,
            }),
        };
        // 幂等结算（§80.1：拒绝同样是终态）。
        let _ = self.inner.context.journal.settle(key, &result);
        // 审计（§90：每个反向控制必须记录）。
        let audit_meta = audit_meta_for(request, None);
        self.inner.context.audit.record(crate::audit::build_event(
            &ctx.subject,
            &ctx.source,
            &request.namespace,
            &request.device_id,
            &request.request_id,
            audit_meta.operation,
            &audit_meta.target,
            &audit_meta.parameters,
            None,
            result.status,
            result.error.as_ref().map(|e| e.code.clone()),
            None,
            0,
            now_ns(),
        ));
        ControlReceipt::ready(result)
    }
}

/// 由请求操作预生成审计元数据（§90）。
fn audit_meta_for(
    request: &ControlRequest,
    risk_level: Option<observation_model::CommandRiskLevel>,
) -> AuditMeta {
    let (operation, target_text, parameters) = match &request.operation {
        ControlOperation::PropertyWrite(payload) => {
            let (target, params) =
                summarize_parameters(&AuditTarget::PropertyWrite(&payload.items));
            (AuditOperation::PropertyWrite, target, params)
        }
        ControlOperation::CommandExecute(payload) => {
            let (target, params) = summarize_parameters(&AuditTarget::Command {
                command: &payload.command,
                parameters: &payload.parameters,
            });
            (AuditOperation::CommandExecute, target, params)
        }
    };
    AuditMeta {
        operation,
        target: target_text,
        parameters,
        risk_level,
        queued_at_ns: now_ns(),
    }
}

/// 进程中断后的防御性结果（§80.1 Indeterminate）。
fn interrupted_result(request: &ControlRequest, code: &str, message: &str) -> ControlResult {
    ControlResult {
        request_id: request.request_id.clone(),
        namespace: request.namespace.clone(),
        device_id: request.device_id.clone(),
        status: ControlStatus::Indeterminate,
        started_at_ns: None,
        completed_at_ns: Some(now_ns()),
        result: None,
        error: Some(ControlError {
            code: code.to_owned(),
            message: message.to_owned(),
            details: None,
        }),
    }
}
