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
use tokio::sync::watch;
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
    /// Journal 磁盘 I/O 阻塞任务的有界并发闸门（三审 P2）。
    pub journal_io_gate: Arc<tokio::sync::Semaphore>,
    pub audit: Arc<dyn crate::audit::AuditSink>,
    pub policy: Arc<ControlPolicy>,
    /// 活跃（未结算）请求的共享结果：幂等 Duplicate 时等待首个请求的最终结果。
    pub active: Mutex<HashMap<IdempotencyKey, Arc<SharedResult>>>,
    /// 指标句柄（§34.2.1；未注入时全 no-op）。
    pub metrics: crate::metrics::ControlMetrics,
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
    /// 指标注册表（§34.2.1，可选）：`None` 时引擎不埋点。
    ///
    /// 用 [`ControlEngineConfig::with_metrics`]( Self::with_metrics) 附加。
    pub metrics: Option<Arc<metrics::MetricsRegistry>>,
}

impl ControlEngineConfig {
    /// 附加指标注册表（§34.2.1）：队列深度、六终态计数、冷却期建立与
    /// Journal 结算失败经 `registry` 暴露（注册幂等）。未调用时引擎不埋点。
    #[must_use]
    pub fn with_metrics(mut self, registry: Arc<metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(registry);
        self
    }
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
    /// 调用者角色不足（四审 P1：取消是控制面操作，须与被取消操作的
    /// 风险等级要求角色一致，防止低权限方绕行取消高权限动作）。
    Unauthorized { code: String, message: String },
}

impl std::fmt::Display for CancelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CancelError::NotFound => write!(f, "请求不存在（已结算或未知）"),
            CancelError::EngineClosed => write!(f, "控制引擎已停机"),
            CancelError::Unauthorized { code, message } => write!(f, "{code}: {message}"),
        }
    }
}

impl std::error::Error for CancelError {}

/// 控制请求状态查询结果（§77：Accepted/Running 是合法中间态；四审 P1：
/// "未知请求"与"正在执行"必须可区分——两者都返回 `None` 会把执行中误报
/// 为不存在，调用方无法安全轮询）。
#[derive(Debug, Clone)]
pub enum StatusQuery {
    /// 无该请求的任何记录（未知 `request_id`，或已过保留期被清理）。
    Unknown,
    /// 已接受、尚未结算（排队或执行中）。
    Running,
    /// 已有终态结果。
    Settled(Box<ControlResult>),
}

/// 状态查询错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusError {
    /// 调用者角色不足（四审 P1：控制面查询同样需要授权，防止未授权方
    /// 借状态接口探测设备/命令信息）。
    Unauthorized { code: String, message: String },
}

impl std::fmt::Display for StatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatusError::Unauthorized { code, message } => write!(f, "{code}: {message}"),
        }
    }
}

impl std::error::Error for StatusError {}

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
///
/// 基于 `watch` 通道：`wait` 先 `borrow_and_update` 再 `changed().await`，
/// 该模式对"set 发生在检查与注册之间"的丢失唤醒是安全的（watch 的版本号
/// 保证值变更可被观察到）——三审 P1：收据等待不得因竞态永久挂起。
///
/// 注意：`watch::Sender::send` 在**没有任何接收者**时会返回错误并丢弃本次
/// 写入（值不落库、版本不递增）。`SharedResult` 自身持有一个永不消费的
/// 接收者 `_keep_alive`，保证 `send` 始终成功——否则"结算完成后才调用
/// `wait()`"的晚到等待者将永远看不到结果（三审回归修复）。
#[derive(Debug)]
pub(crate) struct SharedResult {
    value: watch::Sender<Option<ControlResult>>,
    _keep_alive: watch::Receiver<Option<ControlResult>>,
}

impl SharedResult {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self {
            value: tx,
            _keep_alive: rx,
        }
    }

    /// 写入最终结果并唤醒所有等待者（在幂等结算之后调用）。
    pub fn set(&self, result: ControlResult) {
        let _ = self.value.send(Some(result));
    }

    /// 是否已写入最终结果（P1-6：入队后检查，避免快速完成时残留 stale active 条目）。
    pub fn is_set(&self) -> bool {
        self.value.borrow().is_some()
    }

    /// 等待最终结果。
    pub async fn wait(&self) -> ControlResult {
        let mut rx = self.value.subscribe();
        loop {
            if let Some(result) = rx.borrow_and_update().clone() {
                return result;
            }
            if rx.changed().await.is_err() {
                // 发送端被丢弃：不可达（SharedResult 自身持有 Sender）。
                if let Some(result) = rx.borrow_and_update().clone() {
                    return result;
                }
                unreachable!("SharedResult 发送端被丢弃");
            }
        }
    }
}

struct Inner {
    catalog: Arc<dyn DeviceCatalog>,
    authorizer: Arc<dyn Authorizer>,
    context: Arc<EngineContext>,
    queues: Mutex<HashMap<DeviceId, Arc<DeviceQueue>>>,
    closed: Arc<AtomicBool>,
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
    ///
    /// # Panics
    ///
    /// 策略配置非法（如 `idempotency_retention < 24h`）时 panic——配置错误属于
    /// 装配期编程错误，须 fail-fast（§80.1 保留期下限，防止重复控制/结果错配）。
    pub fn new(config: ControlEngineConfig) -> Self {
        if let Err(e) = config.policy.validate() {
            panic!("控制策略非法：{e}");
        }
        // §34.2.1 per-device 维度：启动期按目录静态清单预注册全部设备的
        // 队列深度 gauge（评审 P1：运行期首见注册会在控制热路径引入锁与
        // 堆分配，且冻结快照不含运行期注册的指标）。
        let device_ids = config.catalog.device_ids();
        let control_metrics =
            crate::metrics::ControlMetrics::new(config.metrics.as_ref(), &device_ids);
        let context = Arc::new(EngineContext {
            executor: config.executor,
            journal: config.journal,
            journal_io_gate: Arc::new(tokio::sync::Semaphore::new(
                crate::journal::JOURNAL_IO_CONCURRENCY,
            )),
            audit: config.audit,
            policy: config.policy,
            active: Mutex::new(HashMap::new()),
            metrics: control_metrics,
        });
        Self {
            inner: Arc::new(Inner {
                catalog: config.catalog,
                authorizer: config.authorizer,
                context,
                queues: Mutex::new(HashMap::new()),
                closed: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    /// 提交控制请求（§81 统一链路入口）。
    ///
    /// 流程：信封校验 → 新鲜度 → 授权（§83，五审 P1：先于幂等登记——未授权
    /// 请求不得占用 request_id 或借 Duplicate 探测既有状态）→ 设备存在/启用
    /// → 幂等登记（§80.1）→ 校验与映射（§75/§84）→ 前置条件（§85）→
    /// 入队（§87）。
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
        // 2. 新鲜度（四审 P2 + 五审 S1）：`requested_at_ns` 距今超过策略上限
        // 的旧请求拒绝执行（长时间滞留/重放）；远超时钟偏差的未来时间戳同样
        // 拒绝——否则可绕过过期判定使新鲜度校验失效。
        {
            let now = now_ns();
            let max_age_ms = self.inner.context.policy.max_request_age_ms as i64;
            let age_ms = now.saturating_sub(request.requested_at_ns) / 1_000_000;
            if age_ms > max_age_ms {
                return Err(SubmitError::InvalidRequest {
                    code: "REQUEST_TOO_OLD",
                    message: format!("请求年龄 {age_ms} ms 超过上限 {max_age_ms} ms，拒绝执行"),
                });
            }
            let ahead_ms = request.requested_at_ns.saturating_sub(now) / 1_000_000;
            if ahead_ms > max_age_ms {
                return Err(SubmitError::InvalidRequest {
                    code: "REQUEST_TOO_NEW",
                    message: format!(
                        "请求时间戳超前 {ahead_ms} ms，超过允许时钟偏差 {max_age_ms} ms"
                    ),
                });
            }
        }

        // 3. 授权（五审 P1：先于幂等登记——未授权请求不写 Journal、不登记
        // active，无法占用 request_id 阻塞合法用户，也无法借 Duplicate 探测
        // 既有请求状态）。命令风险等级来自静态 Profile；设备未知时按最严格
        // Critical 要求授权（防设备存在性探测），设备检查在授权之后进行。
        let required = match &request.operation {
            ControlOperation::PropertyWrite(_) => self
                .inner
                .context
                .policy
                .required_role(crate::policy::OperationKind::PropertyWrite),
            ControlOperation::CommandExecute(payload) => {
                let risk = self.inner.catalog.device(&request.device_id).map_or(
                    observation_model::CommandRiskLevel::Critical,
                    |info| {
                        info.profile
                            .commands
                            .iter()
                            .find(|c| c.id == payload.command)
                            .map(|c| c.risk_level)
                            .unwrap_or(observation_model::CommandRiskLevel::Critical)
                    },
                );
                self.inner
                    .context
                    .policy
                    .required_role(crate::policy::OperationKind::Command(risk))
            }
        };
        if let Err(e) = self
            .inner
            .authorizer
            .authorize(&ctx.subject, required, &request.device_id)
        {
            return Ok(self
                .reject_unregistered(&request, ctx, e.code, e.message)
                .await);
        }

        // 4. 设备存在且已启用（§4.2）。此时尚未持久化/登记，拒绝不留 Journal
        // 记录（审计仍记录，§90）。
        let Some(device_info) = self.inner.catalog.device(&request.device_id) else {
            return Ok(self
                .reject_unregistered(
                    &request,
                    ctx,
                    "DEVICE_NOT_FOUND",
                    format!("设备 {} 不存在", request.device_id),
                )
                .await);
        };
        if !device_info.enabled {
            return Ok(self
                .reject_unregistered(
                    &request,
                    ctx,
                    "DEVICE_DISABLED",
                    format!("设备 {} 已禁用", request.device_id),
                )
                .await);
        }

        // 5. 幂等登记（§80.1：下发前先持久化）。
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

        // 三审 P1：active 在 Journal 插入前登记（`or_insert_with` 不覆盖已有
        // 条目）。并发重复提交无论以何种顺序交错，都拿到同一 Arc（首个登记
        // 请求创建的），从根上消除"Journal 已提交但 active 未登记"的窗口——
        // Duplicate 请求不会再错误返回 EXECUTION_INTERRUPTED。
        // 需要区分"本次新建"与"命中已有活跃执行"：若 Journal 仍为 Running
        // 但本 key 无活跃执行者（首个执行已结束但结算落盘失败，active 已移除），
        // 本次新建的 shared 永远不会被写入，需立即返回 Indeterminate 而非挂起。
        let (shared, active_fresh) = {
            let mut active = self.inner.context.active.lock().expect("active 锁被毒化");
            match active.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(o) => (o.get().clone(), false),
                std::collections::hash_map::Entry::Vacant(v) => {
                    let s = Arc::new(SharedResult::new());
                    v.insert(s.clone());
                    (s, true)
                }
            }
        };

        // P2-H：幂等登记的磁盘 I/O 在阻塞线程池执行，不占用 Tokio worker。
        match crate::journal::insert_record(
            &self.inner.context.journal,
            &self.inner.context.journal_io_gate,
            &key,
            hash.clone(),
            now,
            expires,
        )
        .await
        {
            Err(e) => {
                // P1-1：幂等记录持久化失败不得继续下发（进程崩溃后缺少记录，
                // 重试可能重复执行控制动作）。以 Rejected 结算并立即返回。
                tracing::warn!(
                    component = "control-engine",
                    request_id = %key.request_id,
                    error_code = "journal_insert_failed",
                    "幂等记录持久化失败: {e}"
                );
                if active_fresh {
                    // 本请求创建了 active 条目：由 reject_with_shared 结算
                    // shared 并移除条目，并发等待者获得一致的拒绝结果、不挂起。
                    return Ok(self
                        .reject_with_shared(
                            &request,
                            ctx,
                            &key,
                            &shared,
                            "JOURNAL_UNAVAILABLE",
                            "幂等记录持久化失败，控制请求被拒绝".to_owned(),
                        )
                        .await);
                }
                // 四审 P1：非 fresh——active 条目属于其他在途提交（可能正成为
                // 执行者），不得写其 shared / 移除其登记。本请求既未持久化也
                // 未下发，审计后直接以拒绝收据返回。
                return Ok(self
                    .reject_unregistered(
                        &request,
                        ctx,
                        "JOURNAL_UNAVAILABLE",
                        "幂等记录持久化失败，控制请求被拒绝".to_owned(),
                    )
                    .await);
            }
            Ok(JournalDecision::Duplicate(entry)) => {
                // 同 key + 同 payload：返回已有结果/状态，不重复执行（§80.1）。
                debug!(
                    component = "control-engine",
                    request_id = %entry.key.request_id,
                    status = ?entry.status,
                    "幂等命中，返回既有结果"
                );
                if let Some(result) = entry.result {
                    // 已结算：仅当 active 条目是本请求新建时才移除（无执行者
                    // 会清理）；非 fresh 说明条目属于其他在途提交，由其自身
                    // 终态路径清理，不得触碰（四审 P1）。
                    if active_fresh {
                        self.inner
                            .context
                            .active
                            .lock()
                            .expect("active 锁被毒化")
                            .remove(&key);
                    }
                    return Ok(ControlReceipt::ready(result));
                }
                if entry.status == ControlStatus::Running {
                    if !active_fresh {
                        // 执行中：共享首请求的最终结果（命中已有活跃执行）。
                        return Ok(ControlReceipt::pending(shared));
                    }
                    // 首个执行已结束但结算落盘失败（Journal 仍 Running，active
                    // 已被首个执行移除），本次新建的 shared 无执行者会写入：
                    // 立即返回 Indeterminate 并清理刚登记的条目。
                    // 必须先写入 shared 再移除——并发重复提交可能在本请求登记
                    // 之后、移除之前拿到同一 Arc 并走 pending 分支，不写入会
                    // 永久挂起（三审回归修复）。
                    let result = interrupted_result(
                        &request,
                        "EXECUTION_INTERRUPTED",
                        "执行状态未知（进程可能已重启或结算失败）",
                    );
                    shared.set(result.clone());
                    self.inner
                        .context
                        .active
                        .lock()
                        .expect("active 锁被毒化")
                        .remove(&key);
                    return Ok(ControlReceipt::ready(result));
                }
                // 防御分支（理论上不可达：插入必为 Running 且未结算）：结果不确定。
                // 同上：先写 shared 再移除，防止并发等待者挂起；非 fresh 不触碰
                // 他人在途条目。
                let result = interrupted_result(
                    &request,
                    "EXECUTION_INTERRUPTED",
                    "执行状态未知（进程可能已重启）",
                );
                if active_fresh {
                    shared.set(result.clone());
                    self.inner
                        .context
                        .active
                        .lock()
                        .expect("active 锁被毒化")
                        .remove(&key);
                }
                return Ok(ControlReceipt::ready(result));
            }
            Ok(JournalDecision::Conflict { existing }) => {
                // 四审 P1：本请求未成为执行者。若 active 条目是本请求新建的，
                // 必须立即移除——包括 existing 为 Running 的情形（如重启恢复的
                // 未结算记录）：否则 stale 条目会让后续同 key 提交误判"命中
                // 活跃执行"而永久挂起。非 fresh 时条目属于其他在途提交，由其
                // 自身终态路径清理，不得触碰。
                if active_fresh {
                    self.inner
                        .context
                        .active
                        .lock()
                        .expect("active 锁被毒化")
                        .remove(&key);
                }
                return Err(SubmitError::Conflict { existing });
            }
            Ok(JournalDecision::Inserted) => {
                if !active_fresh {
                    // 五审 P1：领导权与 active 条目所有权分离——本请求的幂等
                    // 记录已持久化，但活跃条目属于其他并发提交（其登记早于
                    // 本请求，其 Journal 写入可能已失败并将结算该条目为
                    // Rejected）。若本请求继续执行，会出现"设备动作进行中、
                    // 客户端却收到拒绝/不确定"。安全做法：不执行；将记录落盘
                    // 为 Rejected 并直接返回就绪收据（不触碰他人 shared/active，
                    // 对方终态路径会写入 shared）。
                    return Ok(self
                        .reject_orphan_record(
                            &request,
                            ctx,
                            &key,
                            "IDEMPOTENCY_RACE",
                            "并发提交竞争导致本请求未能执行，请使用新的 request_id 重试".to_owned(),
                        )
                        .await);
                }
                // 本请求成为执行者（且拥有 active 条目）。
            }
        }

        // 6. 校验与映射（§75/§76/§84；全部在 Driver 前完成）。
        let validated = match &request.operation {
            ControlOperation::PropertyWrite(payload) => {
                match validate_property_write(&device_info.profile, payload) {
                    Ok(op) => op,
                    Err(e) => {
                        return Ok(self
                            .reject_with_shared(
                                &request,
                                ctx,
                                &key,
                                &shared,
                                e.code(),
                                e.to_string(),
                            )
                            .await);
                    }
                }
            }
            ControlOperation::CommandExecute(payload) => {
                let op = match validate_command(&device_info.profile, payload) {
                    Ok(op) => op,
                    Err(e) => {
                        return Ok(self
                            .reject_with_shared(
                                &request,
                                ctx,
                                &key,
                                &shared,
                                e.code(),
                                e.to_string(),
                            )
                            .await);
                    }
                };
                // 前置条件（§85）：入队前检查，失败在 Driver 前拒绝。
                // 四审 P1：Profile 声明了前置条件但未配置检查器时必须
                // fail-closed（静默跳过会让安全前置条件形同虚设）；
                // 检查器卡死不得阻塞提交——单次检查受策略超时约束。
                let ValidatedOperation::Execute { preconditions, .. } = &op else {
                    unreachable!("CommandExecute 校验结果必为 Execute");
                };
                if !preconditions.is_empty() {
                    let Some(checker) = self.inner.context.policy.precondition_checker() else {
                        return Ok(self
                            .reject_with_shared(
                                &request,
                                ctx,
                                &key,
                                &shared,
                                "PRECONDITION_UNCONFIGURED",
                                "命令声明了前置条件但引擎未配置前置条件检查器".to_owned(),
                            )
                            .await);
                    };
                    let check = checker.check(&request.device_id, preconditions);
                    match tokio::time::timeout(
                        Duration::from_millis(self.inner.context.policy.precondition_timeout_ms),
                        check,
                    )
                    .await
                    {
                        Err(_) => {
                            return Ok(self
                                .reject_with_shared(
                                    &request,
                                    ctx,
                                    &key,
                                    &shared,
                                    "PRECONDITION_TIMEOUT",
                                    format!(
                                        "前置条件检查超时（{} ms）",
                                        self.inner.context.policy.precondition_timeout_ms
                                    ),
                                )
                                .await);
                        }
                        Ok(Err(e)) => {
                            return Ok(self
                                .reject_with_shared(
                                    &request,
                                    ctx,
                                    &key,
                                    &shared,
                                    "PRECONDITION_FAILED",
                                    e.message,
                                )
                                .await);
                        }
                        Ok(Ok(())) => {}
                    }
                }
                op
            }
        };

        // 6. 有效超时 + 截止时间。
        let effective_ms = self
            .inner
            .context
            .policy
            .effective_timeout_ms(validated.kind(), request.timeout_ms);
        let deadline = std::time::Instant::now() + Duration::from_millis(effective_ms);

        // 7. 审计元数据（提交时预生成，§90）。
        let audit_meta = audit_meta_for(&request, validated.risk_level());

        // 8. 入队（§87 每设备有界队列；同设备串行）。P1-A：`get_or_create_queue`
        //    传入引擎级停机标志——停机排空后重建的队列同样拒绝入队。
        let queue = self.get_or_create_queue(&request.device_id);
        // 五审 P1：不确定结果冷却期——底层物理动作可能仍在进行，冷却期内
        // 不接受该设备的新动作（已持久化记录以 Rejected 结算，可安全重试）。
        if queue.in_cooldown() {
            return Ok(self
                .reject_with_shared(
                    &request,
                    ctx,
                    &key,
                    &shared,
                    "DEVICE_COOLDOWN",
                    format!(
                        "设备存在未确定结果的控制动作，冷却期（{} ms）内拒绝新动作",
                        self.inner.context.policy.indeterminate_cooldown_ms
                    ),
                )
                .await);
        }
        let entry = QueuedEntry {
            key: key.clone(),
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
                // 入队成功但 worker 可能已极快完成（在本次 active 登记之后、
                // 此检查之前）：shared 已写入则移除此条 stale active 条目。
                if shared.is_set() {
                    self.inner
                        .context
                        .active
                        .lock()
                        .expect("active 锁被毒化")
                        .remove(&key);
                }
                Ok(ControlReceipt::pending(shared))
            }
            Err(EnqueueError::Full { capacity }) => {
                // 队列满：拒绝并结算（reject_with_shared 会结算 shared 并
                // 移除 active——幂等等待者获得一致的拒绝结果）。
                Ok(self
                    .reject_with_shared(
                        &request,
                        ctx,
                        &key,
                        &shared,
                        "QUEUE_FULL",
                        format!("设备控制队列已满（容量 {capacity}）"),
                    )
                    .await)
            }
            Err(EnqueueError::Closed) => {
                // 入队失败且已登记：结算为 Indeterminate（结果未知），
                // 避免 Journal 残留 Running（重启恢复同语义，§80.1）；
                // shared 一并写入——并发幂等等待者不会永久挂起。
                let result =
                    interrupted_result(&request, "QUEUE_CLOSED", "引擎停机中，请求未能入队执行");
                // P2-H：结算落盘在阻塞线程池执行；失败只记日志（结果本就是
                // Indeterminate，重启恢复语义一致，无需再次降级）。
                if let Err(e) = crate::journal::settle_record(
                    &self.inner.context.journal,
                    &self.inner.context.journal_io_gate,
                    &key,
                    &result,
                )
                .await
                {
                    tracing::warn!(
                        component = "control-engine",
                        request_id = %key.request_id,
                        error_code = "journal_settle_failed",
                        "停机拒绝结算落盘失败: {e}"
                    );
                    // §34.2.1：Journal 结算落盘失败统一计数（含停机拒绝
                    // 路径——评审 P2：此前仅队列正常结算路径计数）。
                    self.inner.context.metrics.observe_journal_settle_failed();
                }
                self.inner
                    .context
                    .active
                    .lock()
                    .expect("active 锁被毒化")
                    .remove(&key);
                // §34.2.1：停机拒绝以 Indeterminate 终态结算（未入队成功，
                // 无深度配对；Journal 结算失败已另行计数）。
                self.inner
                    .context
                    .metrics
                    .observe_settled(observation_model::ControlStatus::Indeterminate);
                shared.set(result);
                Err(SubmitError::EngineClosed)
            }
        }
    }

    /// 取消请求（§87：cancel）。
    ///
    /// 取消请求（§87：排队中 → `Cancelled`；执行中 → 中止执行器调用，
    /// 以 `Indeterminate` 结算——驱动可能已下发）。
    ///
    /// 四审 P1：取消是控制面操作，必须携带授权上下文——调用者角色须达到
    /// 被取消操作的风险等级要求角色（与提交该操作一致），防止低权限方
    /// 绕行取消高权限动作。调用者必然已知 `request_id`（自己提交或合法
    /// 获悉），故"未找到"不额外做存在性隐藏。
    pub async fn cancel(
        &self,
        key: &IdempotencyKey,
        ctx: &SubmitContext,
    ) -> Result<(), CancelError> {
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
        // 授权（四审 P1）：按被取消操作的操作种类取要求角色。
        let Some(kind) = queue.peek_kind(key) else {
            return Err(CancelError::NotFound);
        };
        let required = self.inner.context.policy.required_role(kind);
        if let Err(e) = self
            .inner
            .authorizer
            .authorize(&ctx.subject, required, &key.device_id)
        {
            return Err(CancelError::Unauthorized {
                code: e.code.to_owned(),
                message: e.message,
            });
        }
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

    /// 查询请求状态（§77：异步控制轮询；§80.1 幂等查询）。
    ///
    /// 四审 P1：返回三态——`Unknown`（无记录）/`Running`（已接受未结算，
    /// 含排队与执行中）/`Settled`（终态结果），"未知"与"正在执行"不再
    /// 混淆。Journal 无记录但 active 注册表在途的提交同样报告 `Running`
    /// （覆盖"已登记、幂等记录尚未落盘"的窗口）。
    ///
    /// 四审 P1：控制面查询需要授权——调用者角色须达到
    /// [`ControlPolicy::control_status_required_role`]。
    ///
    /// 三审 P2：过期清理与查询在阻塞线程池执行（受 Journal 并发闸门约束），
    /// 异步调用方不会在 Tokio worker 上做同步磁盘 I/O。
    pub async fn status(
        &self,
        key: &IdempotencyKey,
        ctx: &SubmitContext,
    ) -> Result<StatusQuery, StatusError> {
        let required = self.inner.context.policy.control_status_required_role;
        if let Err(e) = self
            .inner
            .authorizer
            .authorize(&ctx.subject, required, &key.device_id)
        {
            return Err(StatusError::Unauthorized {
                code: e.code.to_owned(),
                message: e.message,
            });
        }
        let now = now_ns();
        let entry = crate::journal::purge_and_get(
            &self.inner.context.journal,
            &self.inner.context.journal_io_gate,
            key,
            now,
        )
        .await;
        match entry {
            Some(entry) => match entry.result {
                Some(result) => Ok(StatusQuery::Settled(Box::new(result))),
                None => Ok(StatusQuery::Running),
            },
            None => {
                let in_flight = self
                    .inner
                    .context
                    .active
                    .lock()
                    .expect("active 锁被毒化")
                    .contains_key(key);
                Ok(if in_flight {
                    StatusQuery::Running
                } else {
                    StatusQuery::Unknown
                })
            }
        }
    }

    /// 有序停机（§93 停机语义）。
    ///
    /// 停止接收新请求（`submit` 返回 `EngineClosed`）；每设备队列的 worker
    /// 以 `Cancelled` 结算剩余请求后退出；`grace` 内未退出则强制中止（遗留
    /// 条目以 Indeterminate/Cancelled 结算，收据不永久挂起）。P1-5：全部设备
    /// 队列并发 join——总停机时间 ≈ grace，而不是设备数 × grace。
    pub async fn shutdown(&self, grace: Duration) {
        self.inner.closed.store(true, Ordering::SeqCst);
        let queues: Vec<Arc<DeviceQueue>> = {
            let mut queues = self.inner.queues.lock().expect("queues 锁被毒化");
            queues.drain().map(|(_, q)| q).collect()
        };
        for queue in &queues {
            queue.shutdown();
        }
        let mut set = tokio::task::JoinSet::new();
        for queue in queues {
            let ctx = self.inner.context.clone();
            set.spawn(async move {
                queue.join(grace, &ctx).await;
            });
        }
        while set.join_next().await.is_some() {}
    }

    fn get_or_create_queue(&self, device_id: &DeviceId) -> Arc<DeviceQueue> {
        let mut queues = self.inner.queues.lock().expect("queues 锁被毒化");
        queues
            .entry(device_id.clone())
            .or_insert_with(|| {
                Arc::new(DeviceQueue::new(
                    device_id.clone(),
                    self.inner.context.policy.queue_capacity,
                    self.inner.closed.clone(),
                    self.inner.context.clone(),
                ))
            })
            .clone()
    }

    /// 拒绝收据（不触碰 shared/active/Journal）。
    ///
    /// 用于"本请求未创建 active 条目且未持久化"的路径（四审 P1：Journal
    /// 插入失败且非 fresh）——条目属于其他在途提交，不得写其 shared 或
    /// 移除其登记；仅审计后返回就绪收据。
    async fn reject_unregistered(
        &self,
        request: &ControlRequest,
        ctx: &SubmitContext,
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
        // 审计（§90：每个反向控制必须记录）。四审 P2：有界超时，慢审计不阻塞。
        let audit_meta = audit_meta_for(request, None);
        crate::audit::record_bounded(
            &self.inner.context.audit,
            self.inner.context.policy.audit_timeout_ms,
            None,
            crate::audit::build_event(
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
            ),
        )
        .await;
        // §34.2.1：Driver 前拒绝计入 Rejected 终态（未入队，无深度配对）。
        self.inner
            .context
            .metrics
            .observe_settled(ControlStatus::Rejected);
        ControlReceipt::ready(result)
    }

    /// 结算"已持久化但本请求不拥有 active 条目"的幂等记录（五审 P1）。
    ///
    /// 用于 `Inserted && !fresh`：Journal 记录由本请求写入，但执行权不归本
    /// 请求（并发竞争）。落盘 `Rejected` + 审计后返回就绪收据；不触碰他人
    /// shared/active（对方终态路径负责写入 shared）。
    async fn reject_orphan_record(
        &self,
        request: &ControlRequest,
        ctx: &SubmitContext,
        key: &IdempotencyKey,
        code: &str,
        message: String,
    ) -> ControlReceipt {
        let mut result = ControlResult {
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
        // 记录已存在（本请求插入），结算安全；失败降级 Indeterminate（与
        // reject_with_shared 一致，重启恢复语义一致）。
        if let Err(e) = crate::journal::settle_record(
            &self.inner.context.journal,
            &self.inner.context.journal_io_gate,
            key,
            &result,
        )
        .await
        {
            tracing::warn!(
                component = "control-engine",
                request_id = %key.request_id,
                error_code = "journal_settle_failed",
                "孤儿记录结算落盘失败: {e}"
            );
            // §34.2.1：Journal 结算落盘失败统一计数（评审 P2）。
            self.inner.context.metrics.observe_journal_settle_failed();
            result = ControlResult {
                status: ControlStatus::Indeterminate,
                error: Some(ControlError {
                    code: "JOURNAL_SETTLE_FAILED".to_owned(),
                    message: "幂等结算持久化失败，结果不确定".to_owned(),
                    details: None,
                }),
                ..result
            };
        }
        // 审计（§90）。四审 P2：有界超时。
        let audit_meta = audit_meta_for(request, None);
        crate::audit::record_bounded(
            &self.inner.context.audit,
            self.inner.context.policy.audit_timeout_ms,
            None,
            crate::audit::build_event(
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
            ),
        )
        .await;
        // §34.2.1：孤儿记录以 Rejected（或降级 Indeterminate）终态结算。
        self.inner.context.metrics.observe_settled(result.status);
        ControlReceipt::ready(result)
    }

    /// 校验/授权/前置条件/队列满等 Driver 前的拒绝（§84、§85、§86）。
    ///
    /// 以 `Rejected` 结算：幂等 Journal 记录 + 审计（§90），并立即返回。
    /// `shared` 必为已登记 active 的条目（三审 P1：active 在 Journal 插入前
    /// 登记，拒绝路径结算 shared 并移除条目，并发等待者获得一致结果）。
    /// `shared` 必为已登记 active 的条目（三审 P1：active 在 Journal 插入前
    /// 登记，拒绝路径结算 shared 并移除条目，并发等待者获得一致结果）。
    async fn reject_with_shared(
        &self,
        request: &ControlRequest,
        ctx: &SubmitContext,
        key: &IdempotencyKey,
        shared: &Arc<SharedResult>,
        code: &str,
        message: String,
    ) -> ControlReceipt {
        let mut result = ControlResult {
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
        // 幂等结算（§80.1：拒绝同样是终态）。P2-H：落盘在阻塞线程池执行。
        if let Err(e) = crate::journal::settle_record(
            &self.inner.context.journal,
            &self.inner.context.journal_io_gate,
            key,
            &result,
        )
        .await
        {
            tracing::warn!(
                component = "control-engine",
                request_id = %key.request_id,
                error_code = "journal_settle_failed",
                "拒绝结算落盘失败: {e}"
            );
            // §34.2.1：Journal 结算落盘失败统一计数（评审 P2）。
            self.inner.context.metrics.observe_journal_settle_failed();
            // P2-G：结算失败不得宣称已以 Rejected 终态落盘——当前进程与重启
            // 恢复（Indeterminate）必须一致。降级为 Indeterminate（原始错误只
            // 进日志，不进入北向结果）。
            result = ControlResult {
                status: ControlStatus::Indeterminate,
                error: Some(ControlError {
                    code: "JOURNAL_SETTLE_FAILED".to_owned(),
                    message: "幂等结算持久化失败，结果不确定".to_owned(),
                    details: None,
                }),
                ..result
            };
        }
        // 审计（§90：每个反向控制必须记录）。四审 P2：有界超时，慢审计不阻塞。
        let audit_meta = audit_meta_for(request, None);
        crate::audit::record_bounded(
            &self.inner.context.audit,
            self.inner.context.policy.audit_timeout_ms,
            None,
            crate::audit::build_event(
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
            ),
        )
        .await;
        // 结算共享结果并移除 active（P1-6：拒绝同样是终态，幂等等待者获得一致
        // 结果，不残留 stale 条目；shared 必为已登记 active 的条目）。
        self.inner.context.metrics.observe_settled(result.status);
        self.inner
            .context
            .active
            .lock()
            .expect("active 锁被毒化")
            .remove(key);
        shared.set(result.clone());
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
