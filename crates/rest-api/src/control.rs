//! 控制端点（§31.5 控制链路、§90.2 Bearer 认证）——`control` feature 门控。
//!
//! # 契约（§31.5/§32.2/§32.3）
//!
//! ```text
//! POST /api/v1/devices/{device_id}/controls    提交控制请求 → 202 受理
//! GET  /api/v1/control-requests/{request_id}   查询状态（三态）
//! ```
//!
//! - 提交成功返回 **202** + `forgelink.control.accepted.v1`
//!   （`{"schema","request_id","status":"accepted"}`）；控制是异步的，
//!   客户端轮询状态端点获取最终结果（§77）。
//! - 状态查询返回 `forgelink.control.status.v1`，三态字段名为 **`state`**
//!   （§31.5 Normative：`"state": "unknown | running | settled"`）；仅
//!   `settled` 携带完整 `result`（§80.1 `ControlResult` 序列化）。
//! - 请求体（§32.2/§32.3）：`schema` 必须为
//!   [`CONTROL_REQUEST_SCHEMA`]，按 `kind` 分发：`property_write`
//!   （`items[{path,value}]`）或 `command_execute`（`command` +
//!   `parameters`）。`namespace`/`device_id`/`requested_at_ns`/
//!   `timeout_ms` 由服务端提供（device_id 取路径，namespace 与超时来自
//!   [`ControlAdapter`] 配置）。
//!
//! # 错误映射（§31.6）
//!
//! | 来源 | HTTP | 信封 code |
//! |---|---|---|
//! | 缺失/非法/未知 Bearer Token（§90.2） | 401 | `UNAUTHENTICATED` |
//! | 状态查询角色不足（§83/§86） | 403 | `INSUFFICIENT_ROLE` |
//! | 就绪拒绝收据 `INSUFFICIENT_ROLE`（§83） | 403 | `INSUFFICIENT_ROLE` |
//! | 就绪拒绝收据 `DEVICE_NOT_FOUND` | 404 | `DEVICE_NOT_FOUND` |
//! | `DEVICE_DISABLED` / `IDEMPOTENCY_RACE` / 幂等冲突 | 409 | 引擎稳定码 |
//! | 同 request_id 不同完整幂等键（REST 台账准入，§80.1） | 409 | `IDEMPOTENCY_CONFLICT` |
//! | `SubmitError::InvalidRequest` 信封/时效类 | 400 | 引擎稳定码 |
//! | `InvalidRequest` 语义类（`PARAMETER_*`/`EMPTY_WRITE`/`PRECONDITION_*`）与其余校验类拒绝收据 | 422 | 引擎稳定码 |
//! | `QUEUE_FULL` / `JOURNAL_UNAVAILABLE` / `DEVICE_COOLDOWN` / 引擎停机 | 503 | 引擎稳定码或 `SERVICE_UNAVAILABLE` |
//! | 台账被未结算请求占满（REST 台账准入） | 503 | `LEDGER_FULL` |
//!
//! 控制链路的信封 `code` 透传引擎稳定错误码（§80.1 `ControlError.code`），
//! HTTP 状态按上表映射；message 使用引擎文案（引擎侧已保证不含敏感细节，
//! §90.1）。
//!
//! # 适配层
//!
//! 镜像 [`crate::state::ApiState`] 模式：rest-api 不直接依赖采集运行时，
//! 调用方实现 [`ControlAdapter`]（提交/查询），或直接使用内置的
//! [`EngineControlAdapter`] 包装真实 [`control_engine::ControlEngine`]。
//!
//! 类型词汇尽量复用引擎真实类型（[`ControlRequest`]/[`ControlResult`]/
//! [`SubmitError`]）。唯二例外是状态查询的
//! [`ControlStatusQuery`]/[`StatusQueryError`]——与引擎 `StatusQuery`/
//! `StatusError` 同构的镜像类型：这两个引擎类型当前未从 control-engine
//! 根导出（私有模块），rest-api 无法命名；引擎侧导出后可原样替换。
//!
//! # request_id → 幂等键台账
//!
//! 引擎状态查询需要完整幂等键 `(namespace, device_id, request_id)`
//! （§80.1），而 REST 只有 request_id：[`EngineControlAdapter`] 在提交前
//! 把映射登记进有界台账（`RequestLedger`，容量
//! `REQUEST_LEDGER_CAPACITY`），并在请求结算时补记最终结果。状态查询
//! 直接由台账回答三态：
//!
//! - 无条目 → `unknown`（未知 request_id、settled 条目已被淘汰或**进程
//!   重启后台账丢失**）；
//! - 有条目无结果 → `running`（已受理未结算）；
//! - 有结果 → `settled`（终态，含完整 `ControlResult`）。
//!
//! **`unknown` 不构成"可安全重试"的依据**（§80.1：不得盲目重放未确定
//! 动作）：对已提交过的请求，旧物理动作可能仍在执行——换用新 request_id
//! 重试会绕过引擎幂等键导致**重复执行**。客户端应沿用原 request_id 查询，
//! 或经人工确认后再决定后续动作。
//!
//! # 台账淘汰与提交准入（评审 P1）
//!
//! - **running 永不淘汰**：淘汰扫描只移除 settled 条目（FIFO 取最早插入
//!   的 settled）。running 条目的物理动作可能仍在执行，淘汰会让其状态
//!   永久不可查。
//! - **容量被 running 占满 → 拒绝新请求**：submit 在引擎提交前做台账
//!   准入；无 settled 可淘汰时返回 503 `LEDGER_FULL`（消息说明存在大量
//!   未结算请求），避免受理后状态不可查。
//! - **同 request_id 不同完整幂等键 → 409**：request_id 已登记但
//!   `(namespace, device_id)` 不同——放行会绕过引擎幂等键导致同一逻辑
//!   请求重复执行（§80.1），直接以 `IDEMPOTENCY_CONFLICT` 拒绝。
//!
//! 结算结果的获取方式：受理路径由适配器派生的后台等待者持有收据等待
//! 终态（每个在途请求恰好一个等待者，随结算终止——任务数与在途控制请求
//! 同阶，有界；收据由引擎保证不永久挂起）。引擎 Err 路径（信封非法/
//! 冲突/停机）回滚 running 占位，不留永久不可结算条目。
//!
//! # 安全边界（§90.2）
//!
//! - 所有控制端点要求 `Authorization: Bearer <token>`；缺失/格式非法/
//!   未知 Token 一律 401 `UNAUTHENTICATED`（fail-closed，不区分原因以
//!   避免凭据探测信息泄露）。
//! - 状态查询另有角色门槛（[`ControlGateway::status_required_role`]，
//!   §86 `control_status_required_role` 的 REST 侧镜像）：提交链路的授权
//!   在引擎内完成（§83），查询链路在 REST 层完成。
//! - Token 明文**永不**进入日志与错误信息（认证失败只记固定文案）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use control_engine::{
    ControlEngine, IdempotencyKey, Role, StaticTokenAuthorizer, SubmitContext, SubmitError,
    role_ordering,
};
use observation_model::{
    CommandParameter, CommandRequest, ControlError, ControlOperation, ControlRequest,
    ControlResult, ControlStatus, FieldValue, PropertyWriteItem, PropertyWriteRequest, Value,
};
use serde::Serialize;
use tracing::{debug, info, warn};

use crate::error::{ApiError, ErrorCode, RequestId};
use crate::server::{ApiErrorResponse, ApiResult, acquire, path_rejection_error};

/// 控制请求体 schema（§32.2 显式版本化；不符 → 400）。
pub const CONTROL_REQUEST_SCHEMA: &str = "forgelink.control.request.v1";

/// 受理响应 schema（§31.5：202 异步受理）。
pub const CONTROL_ACCEPTED_SCHEMA: &str = "forgelink.control.accepted.v1";

/// 状态查询响应 schema（§31.5 三态 unknown|running|settled）。
pub const CONTROL_STATUS_SCHEMA: &str = "forgelink.control.status.v1";

/// 控制提交端点路径（§31.5；axum 路由参数语法）。
pub const CONTROLS_PATH: &str = "/api/v1/devices/{device_id}/controls";

/// 状态查询端点路径（§31.5）。
pub const CONTROL_REQUESTS_PATH: &str = "/api/v1/control-requests/{request_id}";

/// 审计/日志中的来源标识（§90：谁、来自哪里）。REST 不解析对端地址
/// （需要 `into_make_service_with_connect_info` 装配变更），来源统一为
/// 固定标识；调用方如需更细粒度来源可自行实现 [`ControlAdapter`]。
const SOURCE_REST: &str = "rest";

/// 受理响应的固定状态文本。
const ACCEPTED_STATUS: &str = "accepted";

/// request_id 台账容量（有界：满时淘汰最早插入，内存有界）。
const REQUEST_LEDGER_CAPACITY: usize = 10_000;

/// 状态查询三态（§77/§80.1）。
///
/// 引擎 `StatusQuery` 的同构镜像（该类型当前未从 control-engine 根导出，
/// rest-api 无法命名；变体一一对应，引擎导出后可原样替换）。
#[derive(Debug, Clone)]
pub enum ControlStatusQuery {
    /// 无该请求的任何记录（未知 request_id、已淘汰或进程重启后台账丢失）。
    Unknown,
    /// 已受理、尚未结算（排队或执行中）。
    Running,
    /// 已有终态结果。
    Settled(Box<ControlResult>),
}

/// 状态查询失败（§83 授权不足）。
///
/// 引擎 `StatusError::Unauthorized` 的同构镜像。REST 层已先行做角色门槛
/// （[`ControlGateway::status_required_role`]），内置适配器不会返回本错误；
/// 自定义适配器若自带授权（如包装引擎 status 查询）可通过它上报 403。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusQueryError {
    /// 稳定错误码（如 `INSUFFICIENT_ROLE`）。
    pub code: String,
    pub message: String,
}

/// 提交结果（§77/§80.1）。
#[derive(Debug)]
pub enum Submission {
    /// 已受理、异步执行中（202；客户端轮询状态端点取结果）。
    Accepted,
    /// 提交即终态：即时拒绝（§84 Driver 前失败）或幂等命中已结算
    /// （§80.1，结果以首次提交为准）。
    Ready(Box<ControlResult>),
}

/// 控制提交错误：引擎 [`SubmitError`] + REST 台账准入层的自身拒绝
/// （评审 P1：running 永不淘汰带来的两个新拒绝路径）。
#[derive(Debug)]
pub enum ControlSubmitError {
    /// 引擎提交错误（信封非法/幂等冲突/停机，§80.1）。Box 削减枚举
    /// 尺寸（`SubmitError::Conflict` 内嵌 `JournalEntry`，远大于其余
    /// 单元变体——热路径上频繁按值传递）。
    Engine(Box<SubmitError>),
    /// 同 request_id 已登记但完整幂等键 `(namespace, device_id)` 不同：
    /// 放行会绕过引擎幂等键导致同一逻辑请求重复执行（§80.1）→ 409
    /// `IDEMPOTENCY_CONFLICT`。
    IdempotencyConflict,
    /// 台账被未结算（running）请求占满且无 settled 可淘汰：拒绝新请求
    /// → 503 `LEDGER_FULL`（running 永不淘汰，见模块文档"台账淘汰与
    /// 提交准入"）。
    LedgerFull,
}

/// 控制端点装配（§81/§90.2）：适配器 + Bearer 认证器 + 查询角色门槛。
///
/// 生产装配时 `authorizer` 应与引擎侧授权器同源（同一凭据文件加载出的
/// [`StaticTokenAuthorizer`]），保证 REST 认证出的 subject/角色与引擎
/// 授权一致。
#[derive(Clone)]
pub struct ControlGateway {
    /// 控制请求适配器（提交/查询）。
    pub adapter: Arc<dyn ControlAdapter>,
    /// Bearer Token 认证器（常量时间比较，§90.2）。
    pub authorizer: Arc<StaticTokenAuthorizer>,
    /// 状态查询所需最低角色（§86 `control_status_required_role` 的 REST
    /// 侧镜像，默认 Operator）。提交链路的授权在引擎内完成（§83）。
    pub status_required_role: Role,
}

impl ControlGateway {
    /// 创建网关（状态查询默认要求 Operator 角色，§86 默认策略）。
    pub fn new(adapter: Arc<dyn ControlAdapter>, authorizer: Arc<StaticTokenAuthorizer>) -> Self {
        Self {
            adapter,
            authorizer,
            status_required_role: Role::Operator,
        }
    }
}

/// 控制适配层（rest-api 与控制运行时的边界，镜像 [`crate::state::ApiState`] 模式）。
///
/// 实现必须线程安全（`Send + Sync`，被并发请求共享）；异步方法内部不得
/// 在 Tokio worker 上做阻塞 I/O（control-engine 的 Journal 磁盘操作已在
/// 内部转移到阻塞线程池）。
#[async_trait]
pub trait ControlAdapter: Send + Sync {
    /// 控制命名空间（参与幂等键 §80.1；来自适配层配置）。
    fn namespace(&self) -> &str;

    /// 默认控制超时（毫秒；来自适配层配置。§32.2 中 timeout_ms 由服务端提供）。
    fn default_timeout_ms(&self) -> u64;

    /// 提交控制请求（§81 统一链路入口：认证后的 subject 与来源进入
    /// 授权与审计，§83/§90）。
    ///
    /// 返回 [`Submission`]：即时拒绝与幂等命中为就绪终态
    /// （[`Submission::Ready`]，提交即结算）；否则已入队待执行
    /// （[`Submission::Accepted`]，客户端轮询状态）。
    ///
    /// # Errors
    ///
    /// 信封非法/幂等冲突/引擎停机（[`ControlSubmitError::Engine`]）、
    /// REST 台账准入拒绝（同 request_id 不同完整幂等键 →
    /// [`ControlSubmitError::IdempotencyConflict`]；台账被未结算请求
    /// 占满 → [`ControlSubmitError::LedgerFull`]，见模块文档"台账淘汰
    /// 与提交准入"）时返回错误。
    async fn submit(
        &self,
        request: ControlRequest,
        subject: String,
        source: String,
    ) -> Result<Submission, ControlSubmitError>;

    /// 按 request_id 查询请求状态（§77 三态轮询）。
    ///
    /// 未知 request_id 返回 [`ControlStatusQuery::Unknown`]——不向调用方
    /// 泄露存在性信息。
    ///
    /// # Errors
    ///
    /// 适配器自带授权时角色不足返回 [`StatusQueryError`]（§83：控制面
    /// 查询同样需要授权）。
    async fn status(&self, request_id: &str) -> Result<ControlStatusQuery, StatusQueryError>;
}

/// 内置适配器：包装真实 [`ControlEngine`]（§81）。
///
/// 职责：
/// - 把 REST 侧的 `(subject, source)` 组装为引擎的 [`SubmitContext`]；
/// - 维护 request_id → 幂等键/结果的有界台账（见模块文档），使状态查询
///   能从 request_id 回答三态。
///
/// 引擎本身由调用方装配（catalog/journal/executor/policy 等，§81），
/// 本类型不引入对采集运行时的依赖。
pub struct EngineControlAdapter {
    engine: ControlEngine,
    namespace: String,
    default_timeout_ms: u64,
    ledger: Arc<RequestLedger>,
}

impl EngineControlAdapter {
    /// 创建适配器。
    pub fn new(
        engine: ControlEngine,
        namespace: impl Into<String>,
        default_timeout_ms: u64,
    ) -> Self {
        Self {
            engine,
            namespace: namespace.into(),
            default_timeout_ms,
            ledger: Arc::new(RequestLedger::new()),
        }
    }

    /// 测试专用：指定台账容量（生产固定 [`REQUEST_LEDGER_CAPACITY`]；
    /// 评审 P1-A 场景需要小容量才能在测试中触发 LEDGER_FULL）。
    #[cfg(test)]
    fn with_ledger_capacity(
        engine: ControlEngine,
        namespace: impl Into<String>,
        default_timeout_ms: u64,
        capacity: usize,
    ) -> Self {
        Self {
            engine,
            namespace: namespace.into(),
            default_timeout_ms,
            ledger: Arc::new(RequestLedger::with_capacity(capacity)),
        }
    }
}

#[async_trait]
impl ControlAdapter for EngineControlAdapter {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn default_timeout_ms(&self) -> u64 {
        self.default_timeout_ms
    }

    async fn submit(
        &self,
        request: ControlRequest,
        subject: String,
        source: String,
    ) -> Result<Submission, ControlSubmitError> {
        // 幂等键在 request 被 move 进引擎前构造（§80.1 三元组）。
        let key = IdempotencyKey {
            namespace: request.namespace.clone(),
            device_id: request.device_id.clone(),
            request_id: request.request_id.clone(),
        };
        // 台账准入先于引擎提交（评审 P1，模块文档"台账淘汰与提交准入"）：
        // - 同 request_id 不同完整幂等键 → 409（放行会绕过引擎幂等键，
        //   同一逻辑请求重复执行）；
        // - 容量被 running 占满 → 503（避免受理后状态不可查）；
        // - 否则占位 running 条目（淘汰只针对 settled，见 `admit`）。
        match self.ledger.admit(&key) {
            Ok(_) => {}
            Err(AdmissionError::Conflict) => {
                return Err(ControlSubmitError::IdempotencyConflict);
            }
            Err(AdmissionError::Full) => return Err(ControlSubmitError::LedgerFull),
        }
        let receipt = match self
            .engine
            .submit(request, &SubmitContext { subject, source })
            .await
        {
            Ok(receipt) => receipt,
            Err(e) => {
                // 引擎未受理（信封非法/冲突/停机）：回滚 running 占位，
                // 不留永久不可结算条目（状态查询回到 unknown）。
                self.ledger.rollback(&key.request_id);
                return Err(ControlSubmitError::Engine(Box::new(e)));
            }
        };
        if receipt.is_ready() {
            // 就绪收据（提交即终态）：即时拒绝或幂等命中已结算。
            let result = receipt.wait().await;
            self.ledger.record(key, Some(result.clone()));
            Ok(Submission::Ready(Box::new(result)))
        } else {
            self.ledger.record(key.clone(), None);
            // 已受理：派生后台等待者持有收据等待终态并补记台账（模块文档：
            // 任务数与在途请求同阶，随结算终止；收据保证不永久挂起）。
            let ledger = Arc::clone(&self.ledger);
            tokio::spawn(async move {
                let result = receipt.wait().await;
                ledger.record(key, Some(result));
            });
            Ok(Submission::Accepted)
        }
    }

    async fn status(&self, request_id: &str) -> Result<ControlStatusQuery, StatusQueryError> {
        // 角色门槛已在 REST 层完成（ControlGateway::status_required_role）；
        // 台账直接回答三态（模块文档"request_id → 幂等键台账"）。
        Ok(self.ledger.query(request_id))
    }
}

/// 台账准入结果（引擎提交前的占位，评审 P1）。
#[derive(Debug, PartialEq, Eq)]
enum Admission {
    /// 新条目已登记为 running 占位；引擎 Err 路径须 [`RequestLedger::rollback`]。
    Admitted,
    /// 同 request_id 已有登记且完整幂等键一致（幂等重试路径），无需新槽位。
    Existing,
}

/// 台账准入失败（REST 层拒绝，先于引擎提交）。
#[derive(Debug, PartialEq, Eq)]
enum AdmissionError {
    /// 同 request_id 已登记但 `(namespace, device_id)` 不同：放行会绕过
    /// 引擎幂等键导致同一逻辑请求重复执行（§80.1）→ 409。
    Conflict,
    /// 容量被未结算（running）请求占满且无 settled 可淘汰 → 503。
    Full,
}

/// request_id → 幂等键/结果的有界台账。
///
/// 淘汰规则（评审 P1，模块文档"台账淘汰与提交准入"）：**running 永不
/// 淘汰**——淘汰扫描只移除最早插入的 settled 条目；容量被 running 占满时
/// 新请求被拒绝（[`RequestLedger::admit`] 返回 [`AdmissionError::Full`]），
/// 而不是淘汰 running 让其状态永久不可查。
///
/// 标准互斥锁短临界区（纯内存操作、无 await），锁中毒时取回内部数据
/// 继续工作（生产路径禁 panic）。
#[derive(Debug)]
struct RequestLedger {
    capacity: usize,
    inner: Mutex<LedgerInner>,
}

#[derive(Debug, Default)]
struct LedgerInner {
    entries: HashMap<String, LedgerEntry>,
    /// 插入顺序（FIFO 淘汰依据；淘汰时跳过 running 条目）。
    order: VecDeque<String>,
}

#[derive(Debug)]
struct LedgerEntry {
    /// 完整幂等键（§80.1 三元组）：准入时校验同 request_id 的
    /// `(namespace, device_id)` 一致性（跨设备复用 → 409）；状态查询
    /// 直接由缓存结果回答——control-engine 导出 `StatusQuery` 后，running
    /// 态可回退引擎 Journal 查询（`request_id` → key → `engine.status`）。
    key: IdempotencyKey,
    /// 结算结果（`None` = 已受理未结算，即 running）。
    result: Option<ControlResult>,
}

impl RequestLedger {
    fn new() -> Self {
        Self::with_capacity(REQUEST_LEDGER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            inner: Mutex::new(LedgerInner::default()),
        }
    }

    /// 提交准入（先于引擎提交调用）：同 request_id 校验完整幂等键一致性；
    /// 新 request_id 占位 running 条目，满员时只淘汰 settled 条目，无
    /// settled 可淘汰（容量被 running 占满）则拒绝。
    fn admit(&self, key: &IdempotencyKey) -> Result<Admission, AdmissionError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match inner.entries.get(&key.request_id) {
            Some(entry) => {
                if entry.key.namespace != key.namespace || entry.key.device_id != key.device_id {
                    return Err(AdmissionError::Conflict);
                }
                Ok(Admission::Existing)
            }
            None => {
                while inner.order.len() >= self.capacity {
                    if !Self::evict_oldest_settled(&mut inner) {
                        // 全是 running：宁可拒绝新请求也不淘汰在途条目。
                        return Err(AdmissionError::Full);
                    }
                }
                let request_id = key.request_id.clone();
                inner.entries.insert(
                    request_id.clone(),
                    LedgerEntry {
                        key: key.clone(),
                        result: None,
                    },
                );
                inner.order.push_back(request_id);
                Ok(Admission::Admitted)
            }
        }
    }

    /// 引擎 Err 路径回滚 running 占位：仅移除仍为 running 的条目（已被
    /// 并发路径结算的终态保留），状态查询回到 unknown。
    fn rollback(&self, request_id: &str) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner
            .entries
            .get(request_id)
            .is_some_and(|entry| entry.result.is_none())
        {
            inner.entries.remove(request_id);
            if let Some(position) = inner.order.iter().position(|id| id == request_id) {
                inner.order.remove(position);
            }
        }
    }

    /// 淘汰最早插入的 **settled** 条目（跳过 running——物理动作可能仍在
    /// 执行，见模块文档）。无可淘汰对象时返回 `false`。
    fn evict_oldest_settled(inner: &mut LedgerInner) -> bool {
        for index in 0..inner.order.len() {
            let Some(entry) = inner.entries.get(&inner.order[index]) else {
                continue; // 防御：order 与 entries 失配时跳过
            };
            if entry.result.is_some() {
                let request_id = inner.order.remove(index).expect("index 取自当前长度范围内");
                inner.entries.remove(&request_id);
                return true;
            }
        }
        false
    }

    /// 登记条目。已存在时仅补结果（同 request_id 幂等重试/结算回填不刷新
    /// 插入序）；不存在时插入（防御路径：正常流经 [`Self::admit`] 占位）
    /// 并在满员时只淘汰 settled 条目，无 settled 可淘汰则放弃登记。
    fn record(&self, key: IdempotencyKey, result: Option<ControlResult>) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match inner.entries.get_mut(&key.request_id) {
            Some(entry) => {
                if result.is_some() {
                    entry.result = result;
                }
            }
            None => {
                while inner.order.len() >= self.capacity {
                    if !Self::evict_oldest_settled(&mut inner) {
                        return;
                    }
                }
                let request_id = key.request_id.clone();
                inner
                    .entries
                    .insert(request_id.clone(), LedgerEntry { key, result });
                inner.order.push_back(request_id);
            }
        }
    }

    /// 三态查询（模块文档"request_id → 幂等键台账"）。
    fn query(&self, request_id: &str) -> ControlStatusQuery {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match inner.entries.get(request_id) {
            None => ControlStatusQuery::Unknown,
            Some(entry) => match &entry.result {
                Some(result) => ControlStatusQuery::Settled(Box::new(result.clone())),
                None => ControlStatusQuery::Running,
            },
        }
    }
}

/// 受理响应（§31.5：202 + `forgelink.control.accepted.v1`）。
#[derive(Debug, Serialize)]
pub struct ControlAcceptedResponse {
    pub schema: &'static str,
    pub request_id: String,
    pub status: &'static str,
}

/// 状态查询响应（§31.5 三态；仅 `settled` 携带完整 `result`）。
///
/// 三态字段名为 **`state`**（§31.5 Normative：
/// `"state": "unknown | running | settled"`）。注意区分：受理信封的
/// `"status":"accepted"` 与内层 `ControlResult.status`（执行结果状态）
/// 是另外的字段，与文档一致，不受本字段影响。
#[derive(Debug, Serialize)]
pub struct ControlStatusResponse {
    pub schema: &'static str,
    pub request_id: String,
    /// `unknown` | `running` | `settled`（§31.5 Normative 字段名 `state`）。
    pub state: &'static str,
    /// §80.1 `ControlResult` 完整序列化；unknown/running 缺省该字段。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ControlResult>,
}

impl ControlStatusResponse {
    fn unknown(request_id: String) -> Self {
        Self {
            schema: CONTROL_STATUS_SCHEMA,
            request_id,
            state: "unknown",
            result: None,
        }
    }

    fn running(request_id: String) -> Self {
        Self {
            schema: CONTROL_STATUS_SCHEMA,
            request_id,
            state: "running",
            result: None,
        }
    }

    fn settled(request_id: String, result: ControlResult) -> Self {
        Self {
            schema: CONTROL_STATUS_SCHEMA,
            request_id,
            state: "settled",
            result: Some(result),
        }
    }
}

/// 控制路由共享状态。
#[derive(Clone)]
struct ControlState {
    gateway: ControlGateway,
    concurrency: Arc<tokio::sync::Semaphore>,
}

/// 组装控制路由（feature 门控挂载；只读构建不调用本函数，路由不存在）。
///
/// 返回已应用 state 的路由器，由 [`crate::server`] 合并进总路由并统一
/// 应用 fallback 与 `request_id` 层。
pub(crate) fn control_router(
    gateway: ControlGateway,
    concurrency: Arc<tokio::sync::Semaphore>,
) -> Router {
    Router::new()
        .route(CONTROLS_PATH, post(submit_control))
        .route(CONTROL_REQUESTS_PATH, get(control_status))
        .with_state(ControlState {
            gateway,
            concurrency,
        })
}

/// 从 `Authorization: Bearer <token>` 提取并认证（§90.2）。
///
/// 失败统一 401 `UNAUTHENTICATED`；message 为固定文案，**不回显 Token**
/// （§90.2 敏感边界）。认证通过返回 `(subject, role)`——subject 进入
/// 引擎授权与审计，role 用于查询角色门槛与日志。
fn authenticate(
    headers: &HeaderMap,
    authorizer: &StaticTokenAuthorizer,
) -> Result<(String, Role), ApiError> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::unauthenticated("缺少 Authorization 凭据"))?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            ApiError::unauthenticated("Authorization 凭据格式非法（要求 Bearer Token）")
        })?;
    authorizer
        .authenticate(token)
        .map(|(subject, role)| (subject.to_owned(), role))
        .ok_or_else(|| ApiError::unauthenticated("未知凭据"))
}

/// 认证后的控制用户（§90.2）：`FromRequestParts` 提取器。
///
/// 认证发生在**任何请求体字节被读取之前**（评审 P2-C：body 消费型
/// extractor——`Bytes`/`Json`——会先缓冲完整请求体才进 handler，未认证
/// 客户端可借大 body 消耗内存；parts 阶段完成认证后，未认证请求在
/// body 缓冲前即被 401 拒绝）。并发信号量同样在认证之后才获取
/// （handler 内 [`acquire`]）。
struct AuthenticatedUser {
    subject: String,
    role: Role,
}

impl FromRequestParts<ControlState> for AuthenticatedUser {
    type Rejection = ApiErrorResponse;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &ControlState,
    ) -> Result<Self, Self::Rejection> {
        // `request_id` 层在路由前注入 extension（crate::server）；缺失时
        // 兜底占位（生产路径禁 panic）。
        let id = parts
            .extensions
            .get::<RequestId>()
            .cloned()
            .unwrap_or_else(|| RequestId("req-unknown".to_owned()));
        authenticate(&parts.headers, &state.gateway.authorizer)
            .map(|(subject, role)| Self { subject, role })
            .map_err(|e| {
                warn!(component = "rest-api", request_id = %id, "控制请求认证失败");
                ApiErrorResponse(id, e)
            })
    }
}

/// request_id 路径参数合法性：非空、不含 `/` 与控制字符（URL 编码错误
/// 已在 `PathRejection` 层拦截为 400）。非法字符不进入台账查询。
fn validate_request_id(request_id: &str) -> Result<(), &'static str> {
    if request_id.is_empty() {
        return Err("不能为空");
    }
    if request_id.contains('/') {
        return Err("包含路径分隔符");
    }
    if request_id.chars().any(char::is_control) {
        return Err("包含控制字符");
    }
    Ok(())
}

/// 解析后的提交内容（§32.2/§32.3 → §80.1 操作）。
struct ParsedSubmission {
    request_id: String,
    operation: ControlOperation,
}

/// 解析控制请求体：`schema` 显式校验（不符 → 400），按 `kind` 分发到
/// 属性写入/命令执行。结构问题（缺字段/类型错/null 值）→ 400 malformed；
/// 语义问题（未知属性/超范围/参数不符等）交由引擎校验后以 422 返回
/// （§84 校验在 Driver 前完成，单一事实来源在引擎）。
fn parse_submission(body: &[u8]) -> Result<ParsedSubmission, ApiError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request(format!("请求体不是合法 JSON: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("请求体必须是 JSON 对象"))?;

    let schema = object
        .get("schema")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::bad_request("缺少 schema 字段"))?;
    if schema != CONTROL_REQUEST_SCHEMA {
        return Err(ApiError::bad_request(format!(
            "schema 字段不符: 要求 {CONTROL_REQUEST_SCHEMA}"
        )));
    }

    let request_id = object
        .get("request_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::bad_request("缺少 request_id 字段"))?
        .to_owned();

    let kind = object
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::bad_request("缺少 kind 字段"))?;

    let operation = match kind {
        "property_write" => {
            let items = object
                .get("items")
                .and_then(|v| v.as_array())
                .ok_or_else(|| ApiError::bad_request("property_write 缺少 items 数组"))?;
            let mut parsed = Vec::with_capacity(items.len());
            for item in items {
                let path = item
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ApiError::bad_request("写入项缺少 path 字段"))?;
                let raw_value = item
                    .get("value")
                    .ok_or_else(|| ApiError::bad_request("写入项缺少 value 字段"))?;
                let value = json_to_value(raw_value)
                    .map_err(|reason| ApiError::bad_request(format!("写入项值非法: {reason}")))?;
                parsed.push(PropertyWriteItem {
                    path: path.to_owned(),
                    value,
                });
            }
            ControlOperation::PropertyWrite(PropertyWriteRequest { items: parsed })
        }
        "command_execute" => {
            let command = object
                .get("command")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ApiError::bad_request("command_execute 缺少 command 字段"))?
                .to_owned();
            let parameters = match object.get("parameters") {
                None | Some(serde_json::Value::Null) => Vec::new(),
                Some(raw) => {
                    let Some(map) = raw.as_object() else {
                        return Err(ApiError::bad_request("parameters 必须是对象"));
                    };
                    let mut parsed = Vec::with_capacity(map.len());
                    for (name, raw_value) in map {
                        let value = json_to_value(raw_value).map_err(|reason| {
                            ApiError::bad_request(format!("参数 {name} 值非法: {reason}"))
                        })?;
                        parsed.push(CommandParameter {
                            name: name.clone(),
                            value,
                        });
                    }
                    parsed
                }
            };
            ControlOperation::CommandExecute(CommandRequest {
                command,
                parameters,
            })
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "未知 kind: {other}（支持 property_write / command_execute）"
            )));
        }
    };

    Ok(ParsedSubmission {
        request_id,
        operation,
    })
}

/// JSON 值 → §6.2 `Value`（null 不是合法控制值；数值按整数优先映射，
/// 与 Profile 编码的数值族互通语义一致）。
fn json_to_value(raw: &serde_json::Value) -> Result<Value, String> {
    match raw {
        serde_json::Value::Null => Err("null 不是合法的控制值".to_owned()),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Value::I64)
            .or_else(|| n.as_u64().map(Value::U64))
            .or_else(|| n.as_f64().map(Value::F64))
            .ok_or_else(|| format!("数值 {n} 无法表示")),
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        serde_json::Value::Array(items) => {
            let mut parsed = Vec::with_capacity(items.len());
            for item in items {
                parsed.push(json_to_value(item)?);
            }
            Ok(Value::Array(parsed))
        }
        serde_json::Value::Object(fields) => {
            let mut parsed = Vec::with_capacity(fields.len());
            for (name, raw_value) in fields {
                parsed.push(FieldValue {
                    name: name.clone(),
                    value: json_to_value(raw_value)?,
                });
            }
            Ok(Value::Struct(parsed))
        }
    }
}

/// 控制提交（§31.5 POST /api/v1/devices/{device_id}/controls）。
///
/// 流程：路径提取 → 认证（[`AuthenticatedUser`] 提取器，§90.2——先于
/// 请求体缓冲、并发门控与解析，未认证请求不消耗服务资源）→ 有界并发 →
/// 请求体解析（§32.2/§32.3）→ 构造 §80.1 信封（namespace/timeout 来自
/// 适配层，requested_at_ns=now）→ 引擎提交（含台账准入，见模块文档）→
/// 收据/错误映射（见模块文档映射表）。
async fn submit_control(
    path: Result<Path<String>, PathRejection>,
    State(state): State<ControlState>,
    Extension(id): Extension<RequestId>,
    user: AuthenticatedUser,
    body: axum::body::Bytes,
) -> ApiResult<(StatusCode, Json<ControlAcceptedResponse>)> {
    let Path(device_id) = path.map_err(|rejection| path_rejection_error(&rejection, &id))?;
    let _permit = acquire(&state.concurrency, &id).await?;
    debug!(
        component = "rest-api",
        request_id = %id,
        subject = %user.subject,
        role = ?user.role,
        device_id = %device_id,
        "控制请求已认证"
    );

    let parsed = parse_submission(&body).map_err(|e| ApiErrorResponse(id.clone(), e))?;
    let request = ControlRequest {
        request_id: parsed.request_id.clone(),
        namespace: state.gateway.adapter.namespace().to_owned(),
        device_id: device_id.clone(),
        requested_at_ns: crate::now_ns(),
        timeout_ms: state.gateway.adapter.default_timeout_ms(),
        operation: parsed.operation,
    };

    match state
        .gateway
        .adapter
        .submit(request, user.subject, SOURCE_REST.to_owned())
        .await
    {
        Ok(submission) => {
            // 就绪终态（提交即结算）：即时拒绝映射为对应 HTTP 错误；其余
            // 终态（幂等命中已结算的 Succeeded/Failed/Timeout/Cancelled/
            // Indeterminate）保持受理语义——结果以首次提交为准，客户端
            // 轮询状态端点获取（§77/§80.1）。
            let echoed_request_id = match submission {
                Submission::Ready(result) => {
                    if result.status == ControlStatus::Rejected {
                        let err = rejected_receipt_error(&result);
                        warn!(
                            component = "rest-api",
                            request_id = %id,
                            code = err.code_text(),
                            "控制请求被即时拒绝"
                        );
                        return Err(ApiErrorResponse(id, err));
                    }
                    result.request_id
                }
                Submission::Accepted => parsed.request_id,
            };
            info!(
                component = "rest-api",
                request_id = %id,
                control_request_id = %echoed_request_id,
                device_id = %device_id,
                "控制请求已受理"
            );
            Ok((
                StatusCode::ACCEPTED,
                Json(ControlAcceptedResponse {
                    schema: CONTROL_ACCEPTED_SCHEMA,
                    request_id: echoed_request_id,
                    status: ACCEPTED_STATUS,
                }),
            ))
        }
        Err(e) => Err(ApiErrorResponse(id, map_submit_error(&e))),
    }
}

/// 控制状态查询（§31.5 GET /api/v1/control-requests/{request_id}）。
async fn control_status(
    path: Result<Path<String>, PathRejection>,
    State(state): State<ControlState>,
    Extension(id): Extension<RequestId>,
    user: AuthenticatedUser,
) -> ApiResult<Json<ControlStatusResponse>> {
    let Path(request_id) = path.map_err(|rejection| path_rejection_error(&rejection, &id))?;
    if let Err(reason) = validate_request_id(&request_id) {
        return Err(ApiErrorResponse(
            id,
            ApiError::bad_request(format!("非法 request_id: {reason}")),
        ));
    }
    // 查询角色门槛（§83/§86）：先于台账查询，低权限方无法借状态接口
    // 探测设备/命令信息（含不存在性）。
    if !role_ordering(user.role, state.gateway.status_required_role) {
        warn!(
            component = "rest-api",
            request_id = %id,
            subject = %user.subject,
            "状态查询角色不足"
        );
        return Err(ApiErrorResponse(
            id,
            ApiError::insufficient_role(format!("用户 {} 角色不足以查询控制状态", user.subject)),
        ));
    }
    let _permit = acquire(&state.concurrency, &id).await?;

    match state.gateway.adapter.status(&request_id).await {
        Ok(query) => Ok(Json(match query {
            ControlStatusQuery::Unknown => ControlStatusResponse::unknown(request_id),
            ControlStatusQuery::Running => ControlStatusResponse::running(request_id),
            ControlStatusQuery::Settled(result) => {
                ControlStatusResponse::settled(request_id, *result)
            }
        })),
        Err(e) => Err(ApiErrorResponse(
            id,
            ApiError::control(ErrorCode::InsufficientRole, e.code, e.message),
        )),
    }
}

/// `SubmitError::InvalidRequest` 的 code 是否属于语义校验类（422）：
/// 参数级校验（`PARAMETER_*`）、空写入（`EMPTY_WRITE`）与前置条件
/// （`PRECONDITION_*`）是"格式正确但违反业务约束"；其余（空 request_id、
/// 非法超时、新鲜度 REQUEST_TOO_OLD/TOO_NEW）属于请求格式/时效问题（400）。
fn is_semantic_validation_code(code: &str) -> bool {
    code.starts_with("PARAMETER_") || code == "EMPTY_WRITE" || code.starts_with("PRECONDITION_")
}

/// `ControlSubmitError` → §31.6 错误（信封 code 透传引擎稳定码或台账
/// 准入稳定码）。
fn map_submit_error(err: &ControlSubmitError) -> ApiError {
    match err {
        ControlSubmitError::Engine(engine) => map_engine_submit_error(engine),
        ControlSubmitError::IdempotencyConflict => ApiError::control(
            ErrorCode::StateConflict,
            "IDEMPOTENCY_CONFLICT",
            "同 request_id 已绑定不同的 namespace/device_id（§80.1 幂等键不一致）：\
             复用将绕过引擎幂等保护导致重复执行，请更换 request_id",
        ),
        ControlSubmitError::LedgerFull => ApiError::control(
            ErrorCode::ServiceUnavailable,
            "LEDGER_FULL",
            "控制台账中存在大量未结算请求，暂不接受新请求；\
             请稍后重试或先查询既有请求状态",
        ),
    }
}

/// 引擎 `SubmitError` → §31.6 错误（信封 code 透传引擎稳定码）。
fn map_engine_submit_error(err: &SubmitError) -> ApiError {
    match err {
        SubmitError::InvalidRequest { code, message } => {
            if is_semantic_validation_code(code) {
                ApiError::control(ErrorCode::ValidationFailed, *code, message.clone())
            } else {
                ApiError::control(ErrorCode::BadRequest, *code, message.clone())
            }
        }
        SubmitError::Conflict { .. } => ApiError::control(
            ErrorCode::StateConflict,
            "IDEMPOTENCY_CONFLICT",
            "同 request_id 已存在不同 payload 的控制记录（§80.1），请更换 request_id 重试",
        ),
        // 引擎停机与未来新增变体统一按服务端暂不可用处理（503）：当前
        // 版本授权失败以 Rejected 收据返回（INSUFFICIENT_ROLE → 403，见
        // `rejected_receipt_error`）；引擎新增变体（如 Unauthorized）落地
        // 时应在此补充显式映射。
        _ => ApiError::unavailable("控制引擎暂不可用"),
    }
}

/// 就绪拒绝收据（§84 Driver 前拒绝）→ §31.6 错误。
///
/// HTTP 状态按模块文档映射表；信封 code 透传引擎稳定码，message 使用
/// 引擎文案（引擎侧已保证不含敏感细节，§90.1）。
fn rejected_receipt_error(result: &ControlResult) -> ApiError {
    let (engine_code, message) = result
        .error
        .as_ref()
        .map(|error: &ControlError| (error.code.clone(), error.message.clone()))
        .unwrap_or_else(|| ("REJECTED".to_owned(), "控制请求被拒绝".to_owned()));
    let mut err = match engine_code.as_str() {
        "DEVICE_NOT_FOUND" => ApiError::control(ErrorCode::DeviceNotFound, engine_code, message),
        "DEVICE_DISABLED" | "IDEMPOTENCY_RACE" => {
            ApiError::control(ErrorCode::StateConflict, engine_code, message)
        }
        "INSUFFICIENT_ROLE" => ApiError::control(ErrorCode::InsufficientRole, engine_code, message),
        // 服务端瞬时不可用类：持久化失败/冷却期/队列满——重试可能成功，
        // 不得误报为客户端语义错误（422）。
        "QUEUE_FULL" | "JOURNAL_UNAVAILABLE" | "DEVICE_COOLDOWN" => {
            ApiError::control(ErrorCode::ServiceUnavailable, engine_code, message)
        }
        "REQUEST_TOO_OLD" | "REQUEST_TOO_NEW" => {
            ApiError::control(ErrorCode::BadRequest, engine_code, message)
        }
        // 其余为语义校验类（PROPERTY_NOT_FOUND、VALUE_OUT_OF_RANGE、
        // PARAMETER_*、EMPTY_WRITE、PRECONDITION_* 等，§84）→ 422。
        _ => ApiError::control(ErrorCode::ValidationFailed, engine_code, message),
    };
    err.details = match result.error.as_ref().and_then(|e| e.details.clone()) {
        Some(detail) => serde_json::json!({ "device_id": result.device_id, "cause": detail }),
        None => serde_json::json!({ "device_id": result.device_id }),
    };
    err
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use control_engine::{
        ControlEngine, ControlEngineConfig, ControlExecutor, ControlPolicy, ExecuteOutcome,
        InMemoryJournal, JournalEntry, MemoryAuditSink, MemoryDeviceCatalog, WriteOutcome,
    };
    use driver_sdk::{DriverCommand, DriverWriteItem, RawCommandResult, RawWriteResult};
    use observation_model::{
        CommandParameterDescriptor, CommandRiskLevel, DataType, DeviceId, DomainKind,
    };
    use profile_engine::{
        AcquisitionConstraints, DeviceProfile, ProfileCapabilities, ProfileCommand,
        ProfileProperty, WriteRounding,
    };
    use serde_json::Value as JsonValue;
    use tower::ServiceExt;

    use super::*;
    use crate::state::{ApiState, StateError};

    // ---- 测试替身与装配 -----------------------------------------------------

    /// 测试不读取只读快照（控制路由不依赖 [`ApiState`]）。
    struct EmptyState;

    impl ApiState for EmptyState {
        fn snapshot(&self) -> Result<crate::models::ApiSnapshot, StateError> {
            Err(StateError::Internal("测试不读取快照".to_owned()))
        }
    }

    /// 可控执行器：默认阻塞直到 `release()`（运行中/队列满状态可控），
    /// 放行后所有调用立即成功。
    struct GateExecutor {
        calls: AtomicUsize,
        release: tokio::sync::watch::Sender<bool>,
        _keep: tokio::sync::watch::Receiver<bool>,
    }

    impl GateExecutor {
        fn new() -> Arc<Self> {
            let (tx, rx) = tokio::sync::watch::channel(false);
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                release: tx,
                _keep: rx,
            })
        }

        fn release(&self) {
            let _ = self.release.send(true);
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        async fn gate(&self) {
            let mut rx = self.release.subscribe();
            while !*rx.borrow_and_update() {
                if rx.changed().await.is_err() {
                    return;
                }
            }
        }
    }

    #[async_trait]
    impl ControlExecutor for GateExecutor {
        async fn write(&self, _device_id: &DeviceId, items: &[DriverWriteItem]) -> WriteOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.gate().await;
            WriteOutcome::Succeeded(
                items
                    .iter()
                    .map(|item| RawWriteResult {
                        item_id: item.id,
                        success: true,
                        protocol_code: Some(0),
                        error: None,
                    })
                    .collect(),
            )
        }

        async fn execute(&self, _device_id: &DeviceId, _command: &DriverCommand) -> ExecuteOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.gate().await;
            ExecuteOutcome::Succeeded(RawCommandResult {
                success: true,
                protocol_code: Some(0),
                payload: None,
                error: None,
            })
        }
    }

    const NS: &str = "plant-a";
    const DEV: &str = "vfd-01";
    const DEV_DISABLED: &str = "vfd-off";

    /// 测试凭据（§90.2 格式；`parse` 不做文件权限校验，跨平台可用）：
    /// alice=operator（可控制），bob=viewer（不可控制）。
    const CREDENTIALS_JSON: &str = r#"{
        "schema": "forgelink.control.credentials.v1",
        "credentials": [
            { "token": "token-alice-operator-0123456789abcdef", "subject": "alice", "role": "operator" },
            { "token": "token-bob-viewer-0123456789abcdef00", "subject": "bob", "role": "viewer" }
        ]
    }"#;
    const TOKEN_OPERATOR: &str = "token-alice-operator-0123456789abcdef";
    const TOKEN_VIEWER: &str = "token-bob-viewer-0123456789abcdef00";

    /// 最小测试 Profile：一可写频率属性（0~400Hz，scale 0.01，Exact）、
    /// 一只读属性、一条需必填 Bool 参数的命令（无前置条件——免配检查器，
    /// 引擎对声明了前置条件但未配置检查器的命令 fail-closed）。
    fn test_profile() -> Arc<DeviceProfile> {
        let profile = DeviceProfile {
            id: "test-profile".to_owned(),
            vendor: "test".to_owned(),
            family: "test-family".to_owned(),
            models: vec!["test-1".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "test-driver".to_owned(),
            properties: vec![
                ProfileProperty {
                    path: "drive.output.frequency".to_owned(),
                    driver_address: "1!40001".to_owned(),
                    raw_type: DataType::U16,
                    value_type: DataType::F64,
                    unit: Some("Hz".to_owned()),
                    scale: 0.01,
                    offset: 0.0,
                    write_rounding: WriteRounding::Exact,
                    readable: true,
                    writable: true,
                    default_interval_ms: Some(1000),
                    min: Some(Value::F64(0.0)),
                    max: Some(Value::F64(400.0)),
                },
                ProfileProperty {
                    path: "drive.mode".to_owned(),
                    driver_address: "1!40002".to_owned(),
                    raw_type: DataType::U16,
                    value_type: DataType::String,
                    unit: None,
                    scale: 0.0,
                    offset: 0.0,
                    write_rounding: WriteRounding::Exact,
                    readable: true,
                    writable: false,
                    default_interval_ms: None,
                    min: None,
                    max: None,
                },
            ],
            commands: vec![ProfileCommand {
                id: "drive.reset".to_owned(),
                driver_command_id: "reset".to_owned(),
                parameters: vec![CommandParameterDescriptor {
                    name: "ack".to_owned(),
                    data_type: DataType::Bool,
                    required: true,
                    min: None,
                    max: None,
                }],
                risk_level: CommandRiskLevel::Medium,
                preconditions: vec![],
            }],
            capabilities: ProfileCapabilities {
                supported_properties: vec![
                    "drive.output.frequency".to_owned(),
                    "drive.mode".to_owned(),
                ],
                supported_commands: vec!["drive.reset".to_owned()],
                acquisition: AcquisitionConstraints::default(),
                limits: Default::default(),
            },
        };
        Arc::new(profile)
    }

    /// 装配真实引擎 + 适配器 + 控制路由的完整应用（§81 in_memory 装配，
    /// 镜像 control-engine engine_tests 的组装方式；profile 由本 crate
    /// 自行构造——control-engine 的 profile_for_test 是其测试模块私有项）。
    fn control_app(
        executor: Arc<GateExecutor>,
        tweak_policy: impl FnOnce(&mut ControlPolicy),
    ) -> axum::Router {
        control_app_with_ledger(executor, tweak_policy, REQUEST_LEDGER_CAPACITY)
    }

    /// 同 [`control_app`]，但可指定 request_id 台账容量（评审 P1-A 场景
    /// 需要小容量才能在测试中触发 LEDGER_FULL）。
    fn control_app_with_ledger(
        executor: Arc<GateExecutor>,
        tweak_policy: impl FnOnce(&mut ControlPolicy),
        ledger_capacity: usize,
    ) -> axum::Router {
        let gateway = control_gateway_with(executor, tweak_policy, ledger_capacity);
        crate::server::router_with_control(
            Arc::new(EmptyState),
            gateway,
            Arc::new(tokio::sync::Semaphore::new(64)),
        )
    }

    /// 装配真实引擎 + 适配器 + Bearer 认证器的控制网关（供需要
    /// [`ControlGateway`] 本体的测试使用，如真实 TCP 服务器的 P2-C 场景）。
    fn control_gateway_with(
        executor: Arc<GateExecutor>,
        tweak_policy: impl FnOnce(&mut ControlPolicy),
        ledger_capacity: usize,
    ) -> ControlGateway {
        let profile = test_profile();
        let mut catalog = MemoryDeviceCatalog::new();
        catalog.insert_profile(DEV.to_owned(), profile.clone());
        catalog.insert_disabled(DEV_DISABLED.to_owned(), profile);
        let authorizer =
            Arc::new(StaticTokenAuthorizer::parse(CREDENTIALS_JSON).expect("凭据合法"));
        // 冷却期由专项机制测试覆盖：关闭以避免干扰连续提交。
        let mut policy = ControlPolicy {
            indeterminate_cooldown_ms: 0,
            ..ControlPolicy::default()
        };
        tweak_policy(&mut policy);
        let engine = ControlEngine::new(ControlEngineConfig {
            catalog: Arc::new(catalog),
            authorizer: authorizer.clone(),
            journal: Arc::new(InMemoryJournal::new()),
            executor,
            audit: Arc::new(MemoryAuditSink::new()),
            policy: Arc::new(policy),
        });
        ControlGateway::new(
            Arc::new(EngineControlAdapter::with_ledger_capacity(
                engine,
                NS,
                5_000,
                ledger_capacity,
            )),
            authorizer,
        )
    }

    // ---- HTTP 辅助 ----------------------------------------------------------

    async fn send(
        router: axum::Router,
        method: Method,
        path: &str,
        authorization: Option<&str>,
        body: Option<&str>,
    ) -> (StatusCode, JsonValue) {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(value) = authorization {
            builder = builder.header(axum::http::header::AUTHORIZATION, value);
        }
        let request = builder
            .body(Body::from(body.unwrap_or_default().to_owned()))
            .expect("请求合法");
        let response = router.oneshot(request).await.expect("路由可用");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("读取响应");
        (status, serde_json::from_slice(&bytes).expect("响应为 JSON"))
    }

    fn bearer(token: &str) -> String {
        format!("Bearer {token}")
    }

    async fn post_control(
        router: axum::Router,
        token: Option<&str>,
        device: &str,
        body: &str,
    ) -> (StatusCode, JsonValue) {
        send(
            router,
            Method::POST,
            &format!("/api/v1/devices/{device}/controls"),
            token.map(bearer).as_deref(),
            Some(body),
        )
        .await
    }

    async fn get_status(
        router: axum::Router,
        token: Option<&str>,
        request_id: &str,
    ) -> (StatusCode, JsonValue) {
        send(
            router,
            Method::GET,
            &format!("/api/v1/control-requests/{request_id}"),
            token.map(bearer).as_deref(),
            None,
        )
        .await
    }

    fn write_body(request_id: &str, value: f64) -> String {
        format!(
            r#"{{"schema":"{CONTROL_REQUEST_SCHEMA}","request_id":"{request_id}","kind":"property_write","items":[{{"path":"drive.output.frequency","value":{value}}}]}}"#
        )
    }

    fn command_body(request_id: &str, parameters: &str) -> String {
        format!(
            r#"{{"schema":"{CONTROL_REQUEST_SCHEMA}","request_id":"{request_id}","kind":"command_execute","command":"drive.reset","parameters":{parameters}}}"#
        )
    }

    async fn wait_for_calls(executor: &GateExecutor, n: usize) {
        for _ in 0..500 {
            if executor.call_count() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("等待执行器调用超时");
    }

    /// 轮询状态端点直至 settled（真实引擎异步结算，毫秒级）。
    ///
    /// 三态字段名为 `state`（§31.5 Normative：`"state": "unknown |
    /// running | settled"`；信封内层 `result.status` 才是 ControlResult
    /// 的执行状态字段）。
    async fn wait_until_settled(router: &axum::Router, request_id: &str) -> JsonValue {
        for _ in 0..300 {
            let (status, body) = get_status(router.clone(), Some(TOKEN_OPERATOR), request_id).await;
            assert_eq!(status, StatusCode::OK);
            if body["state"] == "settled" {
                return body;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("等待控制请求结算超时");
    }

    // ---- 认证（§90.2）-------------------------------------------------------

    #[tokio::test]
    async fn unauthenticated_missing_header_returns_401() {
        let app = control_app(GateExecutor::new(), |_| {});
        let (status, body) = post_control(app, None, DEV, &write_body("w-401", 50.0)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["schema"], "forgelink.error.v1");
        assert_eq!(body["code"], "UNAUTHENTICATED");
        assert!(
            body["request_id"]
                .as_str()
                .expect("含 request_id")
                .starts_with("req-")
        );
    }

    #[tokio::test]
    async fn unauthenticated_bad_scheme_returns_401() {
        let app = control_app(GateExecutor::new(), |_| {});
        let (status, body) = send(
            app,
            Method::POST,
            "/api/v1/devices/vfd-01/controls",
            Some("Basic dXNlcjpwYXNz"),
            Some(&write_body("w-401b", 50.0)),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "UNAUTHENTICATED");
    }

    #[tokio::test]
    async fn unauthenticated_unknown_token_returns_401_and_never_echoes_token() {
        let app = control_app(GateExecutor::new(), |_| {});
        let secret = "token-mallory-not-in-file";
        let (status, body) =
            post_control(app, Some(secret), DEV, &write_body("w-401c", 50.0)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "UNAUTHENTICATED");
        let text = body.to_string();
        assert!(!text.contains(secret), "错误信息不得回显 Token 内容");
    }

    // ---- 认证先于请求体缓冲与业务逻辑（评审 P2-C）---------------------------

    /// 计数适配器：统计 submit 调用次数（断言未认证请求不进入业务逻辑）。
    struct CountingAdapter {
        submits: AtomicUsize,
    }

    #[async_trait]
    impl ControlAdapter for CountingAdapter {
        fn namespace(&self) -> &str {
            NS
        }

        fn default_timeout_ms(&self) -> u64 {
            5_000
        }

        async fn submit(
            &self,
            _request: ControlRequest,
            _subject: String,
            _source: String,
        ) -> Result<Submission, ControlSubmitError> {
            self.submits.fetch_add(1, Ordering::SeqCst);
            Ok(Submission::Accepted)
        }

        async fn status(&self, _request_id: &str) -> Result<ControlStatusQuery, StatusQueryError> {
            Ok(ControlStatusQuery::Unknown)
        }
    }

    fn counting_app(adapter: Arc<CountingAdapter>) -> axum::Router {
        let authorizer =
            Arc::new(StaticTokenAuthorizer::parse(CREDENTIALS_JSON).expect("凭据合法"));
        let gateway = ControlGateway::new(adapter, authorizer);
        crate::server::router_with_control(
            Arc::new(EmptyState),
            gateway,
            Arc::new(tokio::sync::Semaphore::new(64)),
        )
    }

    #[tokio::test]
    async fn unauthenticated_bad_token_never_enters_business_logic() {
        // 评审 P2-C：认证先于业务逻辑——超大/畸形 body + 坏 Token 一律
        // 401，适配层零调用（计数器断言）。
        let adapter = Arc::new(CountingAdapter {
            submits: AtomicUsize::new(0),
        });
        let app = counting_app(adapter.clone());

        // 超大 body + 坏 Token。
        let huge = "x".repeat(256 * 1024);
        let (status, body) =
            post_control(app.clone(), Some("token-mallory-not-in-file"), DEV, &huge).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "UNAUTHENTICATED");

        // 畸形 JSON + 坏 Token：认证先于解析（401 而非 400）。
        let (status, body) =
            post_control(app, Some("token-mallory-not-in-file"), DEV, "{not json").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "UNAUTHENTICATED");

        assert_eq!(
            adapter.submits.load(Ordering::SeqCst),
            0,
            "未认证请求不得进入适配层"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unauthenticated_request_rejected_before_body_fully_buffered() {
        // 评审 P2-C：认证必须发生在请求体缓冲之前。客户端声明大
        // Content-Length 却只发送少量字节即停顿——坏 Token 必须立即得到
        // 401，而不是等服务端收满 body 才处理（旧实现中 `Bytes` extractor
        // 先缓冲完整请求体才进 handler，未认证客户端可借此消耗内存/挂起
        // 请求；本测试在旧实现下会挂起直至超时失败）。
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let executor = GateExecutor::new();
        executor.release();
        let gateway = control_gateway_with(executor, |_| {}, REQUEST_LEDGER_CAPACITY);
        let server = crate::server::RestApiServer::spawn_with_control(
            Arc::new(EmptyState),
            gateway,
            crate::server::RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: 8,
            },
        )
        .await
        .expect("绑定成功");

        let mut stream = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("连接 REST 服务器");
        // 声明 1 MiB body，只发送头部与一小块即停顿。
        let request = format!(
            "POST /api/v1/devices/{DEV}/controls HTTP/1.1\r\n\
             Host: test\r\n\
             Authorization: Bearer token-mallory-not-in-file\r\n\
             Content-Length: 1048576\r\n\
             Connection: close\r\n\
             \r\n\
             {{\"partial\":"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("写入半截请求");

        let mut buf = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        // 外层 Err = 超时（旧实现下会走到这里：服务端等满 body 不响应）；
        // 内层 Err = 读取失败。
        let bytes = read
            .expect("应在 body 缓冲完成前返回响应（而非挂起等待完整 body）")
            .expect("读取响应应成功");
        assert!(bytes > 0, "应收到 401 响应");
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("401"), "应返回 401: {text}");
        assert!(
            text.contains("UNAUTHENTICATED"),
            "错误码应为 UNAUTHENTICATED: {text}"
        );

        server.shutdown().await;
    }

    // ---- 提交成功路径 -------------------------------------------------------

    #[tokio::test]
    async fn authenticated_submit_accepted_202() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let (status, body) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("w-ok", 50.0),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["schema"], "forgelink.control.accepted.v1");
        assert_eq!(body["request_id"], "w-ok");
        assert_eq!(body["status"], "accepted");

        // 受理后可查询到终态（放行的执行器立即成功）。
        let settled = wait_until_settled(&app, "w-ok").await;
        assert_eq!(settled["result"]["status"], "succeeded");
    }

    #[tokio::test]
    async fn command_execute_submit_accepted_202() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let (status, body) = post_control(
            app,
            Some(TOKEN_OPERATOR),
            DEV,
            &command_body("c-ok", r#"{"ack":true}"#),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["schema"], "forgelink.control.accepted.v1");
        assert_eq!(body["request_id"], "c-ok");
        assert_eq!(body["status"], "accepted");
    }

    // ---- 请求体解析（§32.2/§32.3，malformed → 400）-------------------------

    #[tokio::test]
    async fn schema_mismatch_returns_400() {
        let app = control_app(GateExecutor::new(), |_| {});
        let raw = write_body("w-schema", 50.0)
            .replace(CONTROL_REQUEST_SCHEMA, "forgelink.control.request.v2");
        let (status, body) = post_control(app, Some(TOKEN_OPERATOR), DEV, &raw).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "BAD_REQUEST");
    }

    #[tokio::test]
    async fn unknown_kind_returns_400() {
        let app = control_app(GateExecutor::new(), |_| {});
        let raw = format!(
            r#"{{"schema":"{CONTROL_REQUEST_SCHEMA}","request_id":"k-1","kind":"reboot","items":[]}}"#
        );
        let (status, body) = post_control(app, Some(TOKEN_OPERATOR), DEV, &raw).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let message = body["message"].as_str().expect("message 为字符串");
        assert!(message.contains("kind"), "应指出 kind 问题: {message}");
    }

    #[tokio::test]
    async fn malformed_json_returns_400() {
        let app = control_app(GateExecutor::new(), |_| {});
        let (status, body) = post_control(app, Some(TOKEN_OPERATOR), DEV, "{not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "BAD_REQUEST");
    }

    #[tokio::test]
    async fn null_write_value_rejected_as_malformed() {
        let app = control_app(GateExecutor::new(), |_| {});
        let raw = format!(
            r#"{{"schema":"{CONTROL_REQUEST_SCHEMA}","request_id":"w-null","kind":"property_write","items":[{{"path":"drive.output.frequency","value":null}}]}}"#
        );
        let (status, _) = post_control(app, Some(TOKEN_OPERATOR), DEV, &raw).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    // ---- 语义校验失败（§84，经引擎拒绝收据 → 422）---------------------------

    #[tokio::test]
    async fn property_write_out_of_range_maps_to_422() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let (status, body) = post_control(
            app,
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("w-range", 500.0),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], "VALUE_OUT_OF_RANGE", "信封透传引擎稳定码");
        assert_eq!(body["details"]["device_id"], DEV);
    }

    #[tokio::test]
    async fn command_missing_required_parameter_maps_to_422() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let (status, body) = post_control(
            app,
            Some(TOKEN_OPERATOR),
            DEV,
            &command_body("c-missing", "{}"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], "MISSING_PARAMETER");
    }

    // ---- 就绪拒绝收据映射 ---------------------------------------------------

    #[tokio::test]
    async fn device_not_found_maps_to_404() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let (status, body) = post_control(
            app,
            Some(TOKEN_OPERATOR),
            "nope",
            &write_body("w-404", 50.0),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "DEVICE_NOT_FOUND");
    }

    #[tokio::test]
    async fn disabled_device_maps_to_409() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let (status, body) = post_control(
            app,
            Some(TOKEN_OPERATOR),
            DEV_DISABLED,
            &write_body("w-409", 50.0),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "DEVICE_DISABLED");
    }

    #[tokio::test]
    async fn insufficient_role_maps_to_403() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        // bob=viewer：属性写入要求 Operator（§83/§86 默认策略）→ 拒绝收据。
        let (status, body) =
            post_control(app, Some(TOKEN_VIEWER), DEV, &write_body("w-403", 50.0)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["code"], "INSUFFICIENT_ROLE");
    }

    #[tokio::test]
    async fn queue_full_maps_to_503() {
        let executor = GateExecutor::new(); // 阻塞首个请求，占住 worker
        let app = control_app(executor.clone(), |policy| policy.queue_capacity = 1);

        // A 占 worker（运行中，不计入队列容量）。
        let (first, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("q-1", 10.0),
        )
        .await;
        assert_eq!(first, StatusCode::ACCEPTED);
        wait_for_calls(&executor, 1).await;
        // B 入队占满容量 1。
        let (second, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("q-2", 20.0),
        )
        .await;
        assert_eq!(second, StatusCode::ACCEPTED);
        // C：队列满 → 即时拒绝 QUEUE_FULL → 503。
        let (third, body) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("q-3", 30.0),
        )
        .await;
        assert_eq!(third, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "QUEUE_FULL");

        executor.release();
    }

    #[tokio::test]
    async fn idempotency_conflict_maps_to_409() {
        let executor = GateExecutor::new(); // 阻塞首条，保持 Journal 记录 Running
        let app = control_app(executor.clone(), |_| {});

        let (first, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("w-c", 10.0),
        )
        .await;
        assert_eq!(first, StatusCode::ACCEPTED);
        // 同 request_id + 不同 payload → §80.1 Conflict → 409。
        let (second, body) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("w-c", 20.0),
        )
        .await;
        assert_eq!(second, StatusCode::CONFLICT);
        assert_eq!(body["code"], "IDEMPOTENCY_CONFLICT");

        executor.release();
    }

    #[tokio::test]
    async fn empty_request_id_maps_to_400_with_engine_code() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        let raw = format!(
            r#"{{"schema":"{CONTROL_REQUEST_SCHEMA}","request_id":"","kind":"property_write","items":[{{"path":"drive.output.frequency","value":50.0}}]}}"#
        );
        let (status, body) = post_control(app, Some(TOKEN_OPERATOR), DEV, &raw).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "EMPTY_REQUEST_ID");
    }

    // ---- 幂等命中已结算仍受理 ----------------------------------------------

    #[tokio::test]
    async fn idempotent_resubmit_after_settle_still_accepted_202() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});

        let (first, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("w-idem", 50.0),
        )
        .await;
        assert_eq!(first, StatusCode::ACCEPTED);
        let settled = wait_until_settled(&app, "w-idem").await;
        assert_eq!(settled["result"]["status"], "succeeded");

        // 同 key 同 payload 重放：幂等命中已结算 → 仍 202 受理信封
        // （结果以首次提交为准，客户端轮询 status 获取）。
        let (second, body) =
            post_control(app, Some(TOKEN_OPERATOR), DEV, &write_body("w-idem", 50.0)).await;
        assert_eq!(second, StatusCode::ACCEPTED);
        assert_eq!(body["schema"], "forgelink.control.accepted.v1");
        assert_eq!(body["request_id"], "w-idem");
        assert_eq!(body["status"], "accepted");
    }

    // ---- 台账准入的 HTTP 映射（评审 P1-A）-----------------------------------

    #[tokio::test]
    async fn ledger_full_rejects_new_submissions_and_keeps_running_queryable() {
        let executor = GateExecutor::new(); // 阻塞：首个请求保持 running
        let app = control_app_with_ledger(executor.clone(), |_| {}, 1);

        // A 受理并保持 running（占满容量 1 的台账）。
        let (first, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("lg-1", 10.0),
        )
        .await;
        assert_eq!(first, StatusCode::ACCEPTED);
        wait_for_calls(&executor, 1).await;
        let (_, body) = get_status(app.clone(), Some(TOKEN_OPERATOR), "lg-1").await;
        assert_eq!(body["state"], "running");

        // B：台账被未结算请求占满 → 503 LEDGER_FULL，消息说明存在大量
        // 未结算请求（而非淘汰 running 导致其状态不可查）。
        let (second, body) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("lg-2", 20.0),
        )
        .await;
        assert_eq!(second, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "LEDGER_FULL");
        let message = body["message"].as_str().expect("message 为字符串");
        assert!(
            message.contains("未结算"),
            "消息应说明存在大量未结算请求: {message}"
        );

        // 原请求状态仍可查（running 未被淘汰）。
        let (_, body) = get_status(app.clone(), Some(TOKEN_OPERATOR), "lg-1").await;
        assert_eq!(body["state"], "running");

        // A 结算后：settled 可被淘汰，新请求恢复受理；lg-1 变为 unknown
        // （有界语义内的 settled 淘汰）。
        executor.release();
        wait_until_settled(&app, "lg-1").await;
        let (third, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("lg-3", 30.0),
        )
        .await;
        assert_eq!(third, StatusCode::ACCEPTED);
        let (_, body) = get_status(app, Some(TOKEN_OPERATOR), "lg-1").await;
        assert_eq!(body["state"], "unknown");
    }

    #[tokio::test]
    async fn cross_device_same_request_id_maps_to_409_idempotency_conflict() {
        let executor = GateExecutor::new(); // 阻塞首条保持 running
        let app = control_app(executor.clone(), |_| {});

        let (first, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("xd-1", 10.0),
        )
        .await;
        assert_eq!(first, StatusCode::ACCEPTED);
        wait_for_calls(&executor, 1).await;

        // 同 request_id 提交到不同设备：完整幂等键不同——放行会绕过引擎
        // 幂等键导致同一逻辑请求重复执行（§80.1），REST 层直接 409。
        let (second, body) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV_DISABLED,
            &write_body("xd-1", 10.0),
        )
        .await;
        assert_eq!(second, StatusCode::CONFLICT);
        assert_eq!(body["code"], "IDEMPOTENCY_CONFLICT");

        // 原请求状态不受影响。
        let (_, body) = get_status(app.clone(), Some(TOKEN_OPERATOR), "xd-1").await;
        assert_eq!(body["state"], "running");

        executor.release();
    }

    // ---- 状态查询三态（§31.5/§77）-------------------------------------------

    #[tokio::test]
    async fn status_query_three_states() {
        let executor = GateExecutor::new(); // 阻塞以制造 running 窗口
        let app = control_app(executor.clone(), |_| {});

        // unknown：无该请求的任何记录，且无 result 字段。三态字段名为
        // `state`（§31.5 Normative）。
        let (status, body) = get_status(app.clone(), Some(TOKEN_OPERATOR), "no-such").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.control.status.v1");
        assert_eq!(body["request_id"], "no-such");
        assert_eq!(body["state"], "unknown");
        assert!(body.get("result").is_none(), "unknown 不得携带 result 字段");

        // running：已受理未结算（排队/执行中），无 result 字段。
        let (accepted, _) = post_control(
            app.clone(),
            Some(TOKEN_OPERATOR),
            DEV,
            &write_body("st-run", 10.0),
        )
        .await;
        assert_eq!(accepted, StatusCode::ACCEPTED);
        wait_for_calls(&executor, 1).await;
        let (_, body) = get_status(app.clone(), Some(TOKEN_OPERATOR), "st-run").await;
        assert_eq!(body["state"], "running");
        assert!(body.get("result").is_none(), "running 不得携带 result 字段");

        // settled：完整 ControlResult 序列化。
        executor.release();
        let body = wait_until_settled(&app, "st-run").await;
        assert_eq!(body["state"], "settled");
        assert_eq!(body["result"]["request_id"], "st-run");
        assert_eq!(body["result"]["namespace"], NS);
        assert_eq!(body["result"]["device_id"], DEV);
        assert_eq!(body["result"]["status"], "succeeded");
    }

    #[tokio::test]
    async fn status_query_insufficient_role_maps_to_403() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        // 查询角色门槛（§86 control_status_required_role 的 REST 侧镜像，
        // 默认 Operator）：viewer 一律 403，先于台账查询——低权限方无法
        // 借状态接口探测请求存在性。
        let (status, body) = get_status(app, Some(TOKEN_VIEWER), "w-st").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["code"], "INSUFFICIENT_ROLE");
    }

    #[tokio::test]
    async fn request_id_invalid_chars_rejected_400() {
        let executor = GateExecutor::new();
        executor.release();
        let app = control_app(executor, |_| {});
        // %00 解码为控制字符、%2F 解码为路径分隔符：一律 400（PathRejection
        // 模式的显式字符集补充；%FF 等 URL 编码错误由 PathRejection 拦截）。
        for request_id in ["ab%00cd", "a%2Fb"] {
            let (status, body) = get_status(app.clone(), Some(TOKEN_OPERATOR), request_id).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{request_id}");
            assert_eq!(body["code"], "BAD_REQUEST", "{request_id}");
        }
    }

    // ---- 只读装配语义（feature 启用也不改变 spawn/router 行为）--------------

    #[tokio::test]
    async fn plain_router_keeps_control_routes_hidden_even_with_feature_enabled() {
        // `router()`/`spawn()` 保持纯只读装配：即使 `control` feature 已启用，
        // 未走 `spawn_with_control` 时控制路由不存在（404）。
        let app = crate::server::router(
            Arc::new(EmptyState),
            Arc::new(tokio::sync::Semaphore::new(8)),
        );
        let (status, body) =
            post_control(app, Some(TOKEN_OPERATOR), DEV, &write_body("w-hidden", 1.0)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["schema"], "forgelink.error.v1");
    }

    // ---- 映射函数与台账单元测试 ---------------------------------------------

    fn rejected_result(code: &str) -> ControlResult {
        ControlResult {
            request_id: "r".to_owned(),
            namespace: NS.to_owned(),
            device_id: DEV.to_owned(),
            status: ControlStatus::Rejected,
            started_at_ns: None,
            completed_at_ns: Some(1),
            result: None,
            error: Some(ControlError {
                code: code.to_owned(),
                message: "m".to_owned(),
                details: None,
            }),
        }
    }

    #[test]
    fn invalid_request_codes_classified_400_vs_422() {
        // 语义校验类（PARAMETER_*/EMPTY_WRITE/PRECONDITION_*）→ 422。
        for code in [
            "PARAMETER_TYPE_MISMATCH",
            "PARAMETER_OUT_OF_RANGE",
            "PARAMETER_NOT_FINITE",
            "EMPTY_WRITE",
            "PRECONDITION_FAILED",
            "PRECONDITION_TIMEOUT",
        ] {
            let err = map_submit_error(&ControlSubmitError::Engine(Box::new(
                SubmitError::InvalidRequest {
                    code,
                    message: "x".to_owned(),
                },
            )));
            assert_eq!(
                err.code.status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{code} 应为 422"
            );
            assert_eq!(err.code_text(), code);
        }
        // 信封/时效类 → 400。
        for code in [
            "EMPTY_REQUEST_ID",
            "INVALID_TIMEOUT",
            "REQUEST_TOO_OLD",
            "REQUEST_TOO_NEW",
        ] {
            let err = map_submit_error(&ControlSubmitError::Engine(Box::new(
                SubmitError::InvalidRequest {
                    code,
                    message: "x".to_owned(),
                },
            )));
            assert_eq!(
                err.code.status(),
                StatusCode::BAD_REQUEST,
                "{code} 应为 400"
            );
        }
    }

    #[test]
    fn submit_error_conflict_and_closed_mapping() {
        let conflict = map_submit_error(&ControlSubmitError::Engine(Box::new(
            SubmitError::Conflict {
                existing: JournalEntry {
                    key: IdempotencyKey {
                        namespace: NS.to_owned(),
                        device_id: DEV.to_owned(),
                        request_id: "r".to_owned(),
                    },
                    payload_hash: "h".to_owned(),
                    status: ControlStatus::Running,
                    created_at_ns: 0,
                    expires_at_ns: 1,
                    result: None,
                },
            },
        )));
        assert_eq!(conflict.code.status(), StatusCode::CONFLICT);

        // 引擎停机 → 503（兜底分支覆盖当前与未来新增变体）。
        let closed = map_submit_error(&ControlSubmitError::Engine(Box::new(
            SubmitError::EngineClosed,
        )));
        assert_eq!(closed.code.status(), StatusCode::SERVICE_UNAVAILABLE);

        // REST 台账层拒绝（评审 P1-A）：跨完整幂等键冲突 → 409；
        // 台账被未结算请求占满 → 503 LEDGER_FULL。
        let cross_key = map_submit_error(&ControlSubmitError::IdempotencyConflict);
        assert_eq!(cross_key.code.status(), StatusCode::CONFLICT);
        assert_eq!(cross_key.code_text(), "IDEMPOTENCY_CONFLICT");
        let full = map_submit_error(&ControlSubmitError::LedgerFull);
        assert_eq!(full.code.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(full.code_text(), "LEDGER_FULL");
        assert!(
            full.message.contains("未结算"),
            "LEDGER_FULL 消息应说明存在大量未结算请求: {}",
            full.message
        );
    }

    #[test]
    fn rejected_receipt_codes_map_to_http_statuses() {
        let cases = [
            ("DEVICE_NOT_FOUND", StatusCode::NOT_FOUND),
            ("DEVICE_DISABLED", StatusCode::CONFLICT),
            ("IDEMPOTENCY_RACE", StatusCode::CONFLICT),
            ("INSUFFICIENT_ROLE", StatusCode::FORBIDDEN),
            ("QUEUE_FULL", StatusCode::SERVICE_UNAVAILABLE),
            ("JOURNAL_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE),
            ("DEVICE_COOLDOWN", StatusCode::SERVICE_UNAVAILABLE),
            ("REQUEST_TOO_OLD", StatusCode::BAD_REQUEST),
            ("REQUEST_TOO_NEW", StatusCode::BAD_REQUEST),
            ("VALUE_OUT_OF_RANGE", StatusCode::UNPROCESSABLE_ENTITY),
            ("PROPERTY_NOT_WRITABLE", StatusCode::UNPROCESSABLE_ENTITY),
            ("PRECONDITION_FAILED", StatusCode::UNPROCESSABLE_ENTITY),
        ];
        for (code, expected) in cases {
            let err = rejected_receipt_error(&rejected_result(code));
            assert_eq!(err.code.status(), expected, "{code}");
            assert_eq!(err.code_text(), code, "信封应透传引擎稳定码 {code}");
        }
    }

    #[test]
    fn request_ledger_is_bounded_fifo_and_tracks_settlement() {
        let ledger = RequestLedger::with_capacity(3);
        let key = |rid: &str| ledger_key(NS, DEV, rid);

        for rid in ["r1", "r2", "r3"] {
            ledger.admit(&key(rid)).expect("准入");
        }
        assert!(matches!(ledger.query("r1"), ControlStatusQuery::Running));
        assert!(matches!(
            ledger.query("no-such"),
            ControlStatusQuery::Unknown
        ));

        // 全部结算后才有可淘汰对象（running 永不淘汰）。
        for rid in ["r1", "r2", "r3"] {
            ledger.record(key(rid), Some(rejected_result("QUEUE_FULL")));
        }

        // 满员插入 → 只淘汰最早插入的 **settled** 条目 r1。
        ledger.admit(&key("r4")).expect("有 settled 可淘汰");
        assert!(
            matches!(ledger.query("r1"), ControlStatusQuery::Unknown),
            "最早插入的 settled 条目应被淘汰"
        );
        assert!(matches!(ledger.query("r2"), ControlStatusQuery::Settled(_)));
        assert!(matches!(ledger.query("r3"), ControlStatusQuery::Settled(_)));
        assert!(matches!(ledger.query("r4"), ControlStatusQuery::Running));

        // 结算回填（条目已存在）→ settled，且不刷新插入序：
        // 再插入 r5 → 淘汰插入序次早的 r2。
        ledger.record(key("r4"), Some(rejected_result("QUEUE_FULL")));
        ledger.admit(&key("r5")).expect("有 settled 可淘汰");
        assert!(matches!(ledger.query("r2"), ControlStatusQuery::Unknown));
        assert!(matches!(ledger.query("r3"), ControlStatusQuery::Settled(_)));
        assert!(matches!(ledger.query("r4"), ControlStatusQuery::Settled(_)));
        assert!(matches!(ledger.query("r5"), ControlStatusQuery::Running));

        // 被淘汰的 request_id 再次登记（携带结果）→ 重新可见（有界语义内）。
        ledger.record(key("r1"), Some(rejected_result("DEVICE_NOT_FOUND")));
        let ControlStatusQuery::Settled(result) = ledger.query("r1") else {
            panic!("r1 应已重新登记并结算");
        };
        assert_eq!(result.error.expect("含错误").code, "DEVICE_NOT_FOUND");
    }

    // ---- 台账准入与淘汰（评审 P1-A：running 永不淘汰）-----------------------

    fn ledger_key(namespace: &str, device_id: &str, request_id: &str) -> IdempotencyKey {
        IdempotencyKey {
            namespace: namespace.to_owned(),
            device_id: device_id.to_owned(),
            request_id: request_id.to_owned(),
        }
    }

    #[test]
    fn request_ledger_running_never_evicted_and_full_capacity_rejects() {
        let ledger = RequestLedger::with_capacity(2);
        ledger.admit(&ledger_key(NS, DEV, "r1")).expect("r1 准入");
        ledger.admit(&ledger_key(NS, DEV, "r2")).expect("r2 准入");

        // 容量被 running 占满：拒绝新请求（LEDGER_FULL 语义），原请求仍可查。
        assert!(
            matches!(
                ledger.admit(&ledger_key(NS, DEV, "r3")),
                Err(AdmissionError::Full)
            ),
            "running 满容量必须拒绝新请求"
        );
        assert!(matches!(ledger.query("r1"), ControlStatusQuery::Running));
        assert!(matches!(ledger.query("r2"), ControlStatusQuery::Running));

        // r1 结算后：淘汰最早插入的 settled（r1），r3 得以准入；running 的
        // r2 存活（淘汰扫描跳过 running）。
        ledger.record(
            ledger_key(NS, DEV, "r1"),
            Some(rejected_result("QUEUE_FULL")),
        );
        ledger
            .admit(&ledger_key(NS, DEV, "r3"))
            .expect("结算后有空间");
        assert!(
            matches!(ledger.query("r1"), ControlStatusQuery::Unknown),
            "settled 条目应被淘汰"
        );
        assert!(
            matches!(ledger.query("r2"), ControlStatusQuery::Running),
            "running 条目永不淘汰"
        );
        assert!(matches!(ledger.query("r3"), ControlStatusQuery::Running));
    }

    #[test]
    fn request_ledger_admit_conflicts_on_same_request_id_different_full_key() {
        let ledger = RequestLedger::with_capacity(8);
        ledger.admit(&ledger_key(NS, DEV, "r1")).expect("首次准入");

        // 同 request_id、不同 device_id → 冲突（放行会绕过引擎幂等键，
        // 同一逻辑请求重复执行，§80.1）。
        assert!(matches!(
            ledger.admit(&ledger_key(NS, "other-device", "r1")),
            Err(AdmissionError::Conflict)
        ));
        // 不同 namespace 同理。
        assert!(matches!(
            ledger.admit(&ledger_key("other-ns", DEV, "r1")),
            Err(AdmissionError::Conflict)
        ));
        // 完整幂等键一致 → 幂等重试，非冲突。
        assert!(matches!(
            ledger.admit(&ledger_key(NS, DEV, "r1")),
            Ok(Admission::Existing)
        ));
    }

    #[test]
    fn request_ledger_rollback_removes_only_running_placeholder() {
        let ledger = RequestLedger::with_capacity(4);
        // 引擎 Err 路径回滚占位：不留永久 running（状态查询回到 unknown）。
        ledger.admit(&ledger_key(NS, DEV, "r1")).expect("准入");
        ledger.rollback("r1");
        assert!(matches!(ledger.query("r1"), ControlStatusQuery::Unknown));

        // 已结算条目不受回滚影响（终态保留）。
        ledger.admit(&ledger_key(NS, DEV, "r2")).expect("准入");
        ledger.record(
            ledger_key(NS, DEV, "r2"),
            Some(rejected_result("DEVICE_NOT_FOUND")),
        );
        ledger.rollback("r2");
        assert!(
            matches!(ledger.query("r2"), ControlStatusQuery::Settled(_)),
            "回滚不得删除已结算条目"
        );

        // 回滚后的 request_id 可重新准入。
        assert!(matches!(
            ledger.admit(&ledger_key(NS, DEV, "r1")),
            Ok(Admission::Admitted)
        ));
    }
}
