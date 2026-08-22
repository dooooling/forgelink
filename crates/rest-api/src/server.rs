//! REST v1 HTTP 服务器（axum）。
//!
//! # 结构
//!
//! - `request_id` 中间件：请求进入时生成 `req-{纳秒}-{序号}`，注入
//!   `tracing` span 与错误响应（§31.6）；
//! - 有界并发：每请求先获取 [`RestConfig::max_concurrency`] 信号量
//!   （排队而非拒绝，超时后 503）；
//! - handler 只调用 [`ApiState::snapshot`]（同步短锁），不持有任何
//!   运行时锁跨 `await`；
//! - 优雅停机：停止后拒绝新连接，在途请求限时排空（独立任务，不
//!   阻塞采集/WAL/MQTT）；
//! - 未匹配路径与不支持的方法统一返回 §31.6 错误载荷（控制路由仅在
//!   `control` feature 且 [`RestApiServer::spawn_with_control`] 装配时
//!   存在；[`RestApiServer::spawn`] 与只读构建下控制路由不可达——404/405）。

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore, watch};
use tracing::{Instrument, debug, error, info, warn};

use crate::error::{ApiError, ErrorCode, RequestId, to_response};
use crate::models::{
    ApiSnapshot, DeviceResponse, DevicesResponse, HealthResponse, HealthStatus, MetricView,
    MetricsResponse, PropertiesResponse, ResourcesResponse,
};
use crate::state::{ApiState, map_state_error};
use crate::{HEALTH_PATH, METRICS_PATH};

/// 统一 handler 返回类型：错误携带 `request_id`（§31.6）。
pub(crate) type ApiResult<T> = Result<T, ApiErrorResponse>;

/// 带 `request_id` 的 API 错误（newtype 以满足 `IntoResponse` 孤儿规则）。
pub struct ApiErrorResponse(pub RequestId, pub ApiError);

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        let Self(id, err) = self;
        to_response(&err, &id.0).into_response()
    }
}

/// 并发请求排队等待信号量的上限（超时 → 503）。
const CONCURRENCY_WAIT: Duration = Duration::from_secs(10);

/// 优雅停机时等待在途请求排空的上限。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// 服务器配置。
#[derive(Debug, Clone)]
pub struct RestConfig {
    /// 监听地址（`host:port`）。默认只监听 loopback（§90.1）；非
    /// loopback 绑定必须显式配置（本字段本身即显式配置）。
    pub listen: SocketAddr,
    /// 最大并发请求数（有界并发，默认 64）。
    pub max_concurrency: usize,
    /// §34.2.1 指标注册表（管理接口 `GET /api/v1/metrics` 的数据源）。
    /// 缺省 `None`：端点返回 503（未装配可区分于路径错误）。与 control
    /// feature 正交——只读装配同样可注入。
    pub metrics: Option<std::sync::Arc<metrics::MetricsRegistry>>,
}

impl Default for RestConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".parse().expect("静态地址合法"),
            max_concurrency: 64,
            metrics: None,
        }
    }
}

/// 服务器句柄：持有停止信号与任务，提供可共享的有界停机。
pub struct RestApiServer {
    stop: watch::Sender<bool>,
    /// serve 任务句柄。`Mutex<Option<_>>` 使停机成为**可共享调用**
    /// （`shutdown(&self)`）：任意持有方都能发送停止信号并接管排空
    /// 等待，多次调用幂等（首个完成等待后置 `None`）。评审 P1：外部
    /// 监督方持有 `Arc` 副本时，运行时停机仍必须无条件关闭 REST——
    /// 关闭责任属于运行时，不得转交给外部副本。
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 服务存活标记：`serve` 任务退出（正常停机或异常）后置 false，
    /// 供调用方（Collector 运行时）感知异常退出（评审 P2）。
    alive: Arc<AtomicBool>,
    /// 异常退出通知接收端：serve 任务因错误退出时置 true（发送端由
    /// serve 任务持有，正常停机不触发）。调用方订阅后据此错误上报
    /// 或触发停机，不得静默继续运行（评审 P2）。
    exit_rx: watch::Receiver<bool>,
    /// 异常退出通知发送端（测试钩子共用；正常路径由 serve 任务持有
    /// 克隆，此处保留一份便于测试模拟异常退出；非 test-utils 构建仅
    /// 持有不读）。
    #[cfg_attr(not(feature = "test-utils"), allow(dead_code))]
    exit_tx: watch::Sender<bool>,
    /// 实际监听地址（配置 `:0` 随机端口时用于查询）。
    pub addr: SocketAddr,
}

impl RestApiServer {
    /// 绑定并启动服务器（独立任务；`state` 为只读快照提供者）。
    ///
    /// 只读接口：**不挂载控制路由**（即使启用 `control` feature）——需要
    /// 控制端点时使用 [`Self::spawn_with_control`]（feature 门控）。这保证
    /// 只读构建与只读装配语义一致：控制路由不存在（404/405）。
    ///
    /// # Errors
    ///
    /// 监听地址绑定失败（占用/权限等）或并发配置非法时返回错误，调用方
    /// 应显式失败启动（不静默降级）。
    pub async fn spawn(
        state: Arc<dyn ApiState>,
        mut config: RestConfig,
    ) -> Result<Self, std::io::Error> {
        Self::validate_config(&config)?;
        let listener = TcpListener::bind(config.listen).await?;
        let concurrency = Arc::new(Semaphore::new(config.max_concurrency));
        let metrics = config.metrics.take();
        let app = router_with_options(state, metrics, concurrency);
        Self::serve(listener, app, config).await
    }

    /// 绑定并启动带控制端点的服务器（§31.5 控制链路；`control` feature）。
    ///
    /// 在只读路由之上合并控制路由（`POST /api/v1/devices/{device_id}/controls`
    /// 与 `GET /api/v1/devices/{device_id}/control-requests/{request_id}`），
    /// 共用同一并发门控；所有控制端点要求 Bearer 认证（§90.2）。listen 地址
    /// 必须为 **loopback**（评审二轮 P2，[`Self::validate_config`] 之外的额外
    /// 校验）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::spawn`]；另：listen 非 loopback（§90.2 MVP 控制面仅允许
    /// loopback 直连，远程访问须经 TLS 反向代理转发）时返回
    /// `InvalidInput` 配置错误——collector 配置层已先行校验，此处对直接
    /// 构造的 [`RestConfig`] 兜底拒绝（纵深防御）。
    #[cfg(feature = "control")]
    pub async fn spawn_with_control(
        state: Arc<dyn ApiState>,
        control: crate::control::ControlGateway,
        mut config: RestConfig,
    ) -> Result<Self, std::io::Error> {
        Self::validate_config(&config)?;
        Self::validate_control_listen(config.listen)?;
        let listener = TcpListener::bind(config.listen).await?;
        let concurrency = Arc::new(Semaphore::new(config.max_concurrency));
        // 控制路由与 metrics 正交：控制装配同样可携带指标注册表。
        let app = router_with_control(state, control, config.metrics.take(), concurrency);
        Self::serve(listener, app, config).await
    }

    /// 并发配置校验（评审 P2：0 与超上限都拒绝而非静默修正/崩溃）。
    fn validate_config(config: &RestConfig) -> Result<(), std::io::Error> {
        // 评审 P2：`Semaphore::new` 对超过 `MAX_PERMITS` 的 permits 会
        // panic。配置层（collector config）已先行校验，此处兜底拒绝
        // 直接构造的 `RestConfig`，返回错误而非崩溃。
        if config.max_concurrency > Semaphore::MAX_PERMITS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "rest.max_concurrency 超出 Tokio Semaphore 并发上限 {}",
                    Semaphore::MAX_PERMITS
                ),
            ));
        }
        // 评审 P2：0 是非法配置（无意义并发），直接拒绝而非静默改 1——
        // 静默修正会让实际并发数与配置不一致，掩盖配置错误。
        if config.max_concurrency == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "rest.max_concurrency 必须大于 0",
            ));
        }
        Ok(())
    }

    /// 控制装配的 listen 地址校验（评审二轮 P2，§90.2）：必须 loopback
    /// （IPv4 `127.0.0.0/8`、IPv6 `::1`）。远程访问须经 TLS 反向代理转发，
    /// 而 MVP 无原生 TLS——非 loopback 监听会让控制端点绕过该约束直接
    /// 对外暴露。collector 配置层（启用 control 时）已先行校验，此处对
    /// 直接构造的 [`RestConfig`] 兜底拒绝；只读 [`Self::spawn`] 不受此限
    /// （§90.1 允许显式配置非 loopback 的只读接口）。
    #[cfg(feature = "control")]
    fn validate_control_listen(addr: SocketAddr) -> Result<(), std::io::Error> {
        if addr.ip().is_loopback() {
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "控制端点仅允许 loopback 监听（当前 {addr}）：§90.2 MVP 控制面\
                 仅允许 loopback 直连（IPv4 127.0.0.0/8、IPv6 ::1），远程访问\
                 须经 TLS 反向代理转发"
            ),
        ))
    }

    /// 启动 serve 任务（绑定已完成；只读与控制装配共用此逻辑）。
    async fn serve(
        listener: TcpListener,
        app: Router,
        config: RestConfig,
    ) -> Result<Self, std::io::Error> {
        let addr = listener.local_addr()?;
        let (stop_tx, stop_rx) = watch::channel(false);
        // 异常退出通知通道（评审 P2）：serve 任务错误退出时置 true，
        // 正常停机（stop 信号触发的优雅退出）不触发。
        let (exit_tx, exit_rx) = watch::channel(false);
        let exit_tx_task = exit_tx.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let alive_task = Arc::clone(&alive);

        let join = tokio::spawn(async move {
            let shutdown = async move {
                let mut rx = stop_rx;
                // 服务器任务与停机信号同生命周期：信号通道关闭即停止。
                let _ = rx.changed().await;
            };
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await;
            // 任务退出（无论异常还是停机）：标记服务不可用。停机路径由
            // `shutdown()` 消费句柄，正常路径由 `is_alive()` 感知（评审 P2）。
            alive_task.store(false, Ordering::SeqCst);
            if let Err(e) = result {
                error!(component = "rest-api", error = %e, "REST 服务器异常退出");
                // 异常退出必须通知调用方（评审 P2）：API 已不可用，调用方
                // 据此错误上报或触发停机，不得继续静默运行。
                let _ = exit_tx_task.send(true);
            }
        });
        info!(
            component = "rest-api",
            addr = %addr,
            max_concurrency = config.max_concurrency,
            "REST v1 接口已启动（loopback 默认绑定）"
        );
        Ok(Self {
            stop: stop_tx,
            join: Mutex::new(Some(join)),
            alive,
            exit_rx,
            exit_tx,
            addr,
        })
    }

    /// 服务是否仍在运行（`serve` 任务正常退出或异常退出后为 `false`）。
    ///
    /// 调用方（Collector 运行时）据此感知 REST 已不可用（评审 P2：
    /// 异常退出不能只记日志，地址必须失效）。
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// 订阅异常退出通知：`serve` 任务因错误退出后该通道值为 `true`
    /// （正常停机不触发）。调用方（Collector 运行时）据此错误上报
    /// 或触发停机（评审 P2：API 已不可用但采集进程继续运行属于
    /// 静默故障，必须显式反应）。
    pub fn exit_notified(&self) -> watch::Receiver<bool> {
        self.exit_rx.clone()
    }

    /// 测试钩子：模拟 `serve` 任务异常退出——中止任务、标记服务不可用
    /// 并置位异常退出通知。正常路径由 serve 任务在 listener 错误退出时
    /// 自行完成这三件事（评审 P2）；真实 listener 错误难以在测试中
    /// 复现，此钩子供集成测试验证调用方（Collector 运行时）的监督反应
    /// （评审 P1：REST 异常退出从启动后即被监视）。
    #[cfg(feature = "test-utils")]
    pub async fn force_abnormal_exit(&self) {
        self.alive.store(false, Ordering::SeqCst);
        if let Some(handle) = self.join.lock().await.as_ref() {
            handle.abort();
        }
        let _ = self.exit_tx.send(true);
    }

    /// 优雅停机：拒绝新连接 → 在途请求限时排空 → 任务结束。
    ///
    /// **可共享调用**（`&self`，评审 P1）：停止信号与排空等待不依赖
    /// 唯一所有权——`Arc` 副本被外部（监督方/测试）持有时，任何一方
    /// 调用都会无条件发送停止信号并等待排空，不得把关闭责任转交给
    /// 外部副本。多次调用幂等：任务已结束（`join` 置 `None`）后重复
    /// 调用立即返回。
    ///
    /// 有界（`SHUTDOWN_GRACE` 2s）：超时强制取消，绝不阻塞采集链路。
    pub async fn shutdown(&self) {
        let _ = self.stop.send(true);
        let mut guard = self.join.lock().await;
        if let Some(handle) = guard.as_mut()
            && tokio::time::timeout(SHUTDOWN_GRACE, &mut *handle)
                .await
                .is_err()
        {
            warn!(component = "rest-api", "REST 停机排空超时，强制取消");
            // 强制 abort 不经过 serve 任务内部的 alive=false 置位
            // （评审 P2），此处显式清除存活标记，调用方不得继续误报。
            self.alive.store(false, Ordering::SeqCst);
            handle.abort();
            let _ = handle.await;
        }
        // 释放句柄：任务已结束（优雅退出或 abort 后等待完成），后续
        // 重复调用不再重复等待/排空。
        guard.take();
        info!(component = "rest-api", "REST v1 接口已停止");
    }
}

/// 组装只读路由（§31.5 最小资源路径 + §104 健康检查）。
///
/// 未匹配路径（404）与不支持的 method（405）都返回统一 §31.6 错误
/// 载荷（含 request_id）；`/controls` 等控制路由不存在于本只读契约
/// （启用 `control` feature 时也仅 [`Self::spawn_with_control`] 挂载，
/// 本函数保持纯只读语义，供既有调用方与测试复用）。
pub(crate) fn router(state: Arc<dyn ApiState>, concurrency: Arc<Semaphore>) -> Router {
    router_with_options(state, None, concurrency)
}

/// 统一路由组装入口：只读路由 + 可选 metrics 注册表（§34.2.1）。
///
/// 未匹配路径（404）与不支持的 method（405）都返回统一 §31.6 错误
/// 载荷（含 request_id）；`/controls` 等控制路由不存在于本只读契约
/// （启用 `control` feature 时也仅 [`Self::spawn_with_control`] 挂载，
/// 本函数保持纯只读语义，供既有调用方与测试复用）。
pub(crate) fn router_with_options(
    state: Arc<dyn ApiState>,
    metrics: Option<std::sync::Arc<metrics::MetricsRegistry>>,
    concurrency: Arc<Semaphore>,
) -> Router {
    base_router(state, concurrency, metrics)
        .fallback(fallback_not_found)
        .method_not_allowed_fallback(fallback_method_not_allowed)
        // request_id 层必须在全部路由合并之后应用：axum 的 `.layer` 只
        // 包裹已注册的路由，后合并进来的控制路由也要经过同一层（§31.6
        // 贯穿日志与错误响应依赖该层注入的 `RequestId` extension）。
        .layer(middleware::from_fn(request_id_layer))
}

/// 组装只读 + 控制路由（§31.5 控制链路；`control` feature 门控）。
///
/// 控制路由与只读路由共用同一并发门控（全局有界，§90.1）；fallback 与
/// `request_id` 层在合并后统一应用。`metrics` 与控制正交（§34.2.1）。
#[cfg(feature = "control")]
pub(crate) fn router_with_control(
    state: Arc<dyn ApiState>,
    control: crate::control::ControlGateway,
    metrics: Option<std::sync::Arc<metrics::MetricsRegistry>>,
    concurrency: Arc<Semaphore>,
) -> Router {
    base_router(state, concurrency.clone(), metrics)
        .merge(crate::control::control_router(control, concurrency))
        .fallback(fallback_not_found)
        .method_not_allowed_fallback(fallback_method_not_allowed)
        .layer(middleware::from_fn(request_id_layer))
}

/// 只读路由集合（不含 fallback 与中间件；由 [`router_with_options`] /
/// [`router_with_control`] 统一收尾）。
///
/// `metrics` 为 §34.2.1 指标注册表（管理接口，非控制面）：`None` 时
/// `/api/v1/metrics` 返回 503（未装配可区分于路径错误）。
fn base_router(
    state: Arc<dyn ApiState>,
    concurrency: Arc<Semaphore>,
    metrics: Option<std::sync::Arc<metrics::MetricsRegistry>>,
) -> Router {
    Router::new()
        .route("/api/v1/devices", get(devices))
        .route("/api/v1/devices/{device_id}", get(device))
        .route("/api/v1/devices/{device_id}/resources", get(resources))
        .route("/api/v1/devices/{device_id}/properties", get(properties))
        .route(HEALTH_PATH, get(health))
        .route(METRICS_PATH, get(metrics_snapshot))
        .with_state(AppState {
            state,
            concurrency,
            metrics,
        })
}

/// 未匹配路径（404，§31.6 统一错误载荷）。
async fn fallback_not_found(axum::Extension(id): axum::Extension<RequestId>) -> Response {
    ApiErrorResponse(
        id,
        ApiError {
            code: ErrorCode::ResourceNotFound,
            code_override: None,
            message: "接口路径不存在".to_owned(),
            details: serde_json::Value::Object(Default::default()),
        },
    )
    .into_response()
}

/// 路径存在但 method 不支持的响应（405，§31.6 统一错误载荷）。
async fn fallback_method_not_allowed(axum::Extension(id): axum::Extension<RequestId>) -> Response {
    ApiErrorResponse(
        id,
        ApiError {
            code: ErrorCode::MethodNotAllowed,
            code_override: None,
            message: "接口不支持该 HTTP 方法".to_owned(),
            details: serde_json::Value::Object(Default::default()),
        },
    )
    .into_response()
}

/// 路由共享状态。
#[derive(Clone)]
struct AppState {
    state: Arc<dyn ApiState>,
    concurrency: Arc<Semaphore>,
    /// §34.2.1 指标注册表（管理接口；None = 未装配，端点 503）。
    metrics: Option<std::sync::Arc<metrics::MetricsRegistry>>,
}

/// 请求进入时生成 `request_id` 并注入日志 span（§31.6 贯穿日志）。
async fn request_id_layer(req: Request<axum::body::Body>, next: Next) -> Response {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = RequestId(format!(
        "req-{}-{}",
        crate::now_ns(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let span = tracing::info_span!(
        "rest_request",
        component = "rest-api",
        request_id = %id,
        method = %req.method(),
        path = %req.uri().path()
    );
    let mut req = req;
    req.extensions_mut().insert(id);
    async move { next.run(req).await }.instrument(span).await
}

/// 每个 handler 前的有界并发门控：获取信号量，超时 → 503。
///
/// 只读与控制路由共用同一 `Semaphore`（全局有界并发，§90.1）。
pub(crate) async fn acquire(
    concurrency: &Arc<Semaphore>,
    id: &RequestId,
) -> Result<tokio::sync::OwnedSemaphorePermit, ApiErrorResponse> {
    match tokio::time::timeout(CONCURRENCY_WAIT, concurrency.clone().acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(_)) => Err(ApiErrorResponse(
            id.clone(),
            ApiError::unavailable("并发门控信号量已关闭"),
        )),
        Err(_) => Err(ApiErrorResponse(
            id.clone(),
            ApiError::unavailable("并发请求排队超时（服务繁忙）"),
        )),
    }
}

/// 取快照并映射适配错误（503/500）。
fn snapshot_or_error(
    state: &Arc<dyn ApiState>,
    id: &RequestId,
) -> Result<ApiSnapshot, ApiErrorResponse> {
    state.snapshot().map_err(|e| {
        // 评审 P2：`StateError` 原文可能含路径、连接信息等敏感内容，
        // 不得直接写入日志；只记录映射后的稳定错误码与固定安全文案
        // （`map_state_error` 输出固定 message）。
        error!(
            component = "rest-api",
            request_id = %id,
            error = %map_state_error(&e),
            "REST 快照失败"
        );
        ApiErrorResponse(id.clone(), map_state_error(&e))
    })
}

/// 设备 ID 路径参数校验：与 Collector 配置校验（§31.1 主题规则）共用
/// 同一实现——`mqtt-client` 的 `telemetry_topic` 按**完整 MQTT Topic**
/// （`forgelink/v1/telemetry/{site_id}/{device_id}`）校验长度与字符集
/// （评审 P2：REST 此前只限设备 ID 自身 ≤65535 字节，而配置校验完整
/// 主题长度，接近上限的设备 ID 通过 REST 却无法启动配置，二者必须
/// 同集合）。`site_id` 取当前快照，与配置启动校验同源。
///
/// 允许集合与配置一致（空格、反斜杠等字符由 URL 百分号编码后匹配），
/// 拒绝：空段、`/`、通配符（`#`/`+`）、控制字符与完整主题超长。
fn validate_device_id(
    device_id: &str,
    site_id: &str,
    id: &RequestId,
) -> Result<(), ApiErrorResponse> {
    let reason = match mqtt_client::telemetry_topic(site_id, device_id) {
        Ok(_) => return Ok(()),
        // 只回显原因，不回显完整主题（§90.1 响应不泄漏不必要细节）。
        Err(mqtt_client::MqttClientError::InvalidTopic { reason, .. }) => reason,
        Err(other) => other.to_string(),
    };
    Err(ApiErrorResponse(
        id.clone(),
        ApiError::bad_request(format!("非法设备标识 {device_id:?}: {reason}")),
    ))
}

/// `Path<String>` 提取失败统一映射为 §31.6 400 错误载荷（评审 P2）：
/// 非法 URL 编码（如 `%FF` 解码为非法 UTF-8）会在 handler 之前被
/// axum 拒绝，此前返回默认纯文本 400，不含 `forgelink.error.v1` 与
/// `request_id`。这里把拒绝交给 handler 处理，固定安全文案、原始
/// 拒绝原因只进日志。（只读与控制路由共用同一模式。）
pub(crate) fn path_rejection_error(rejection: &PathRejection, id: &RequestId) -> ApiErrorResponse {
    debug!(
        component = "rest-api",
        request_id = %id,
        rejection = %rejection,
        "设备标识路径参数解码失败"
    );
    ApiErrorResponse(
        id.clone(),
        ApiError::bad_request("设备标识路径参数非法（URL 编码错误）"),
    )
}

async fn devices(
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<DevicesResponse>> {
    let _permit = acquire(&state.concurrency, &id).await?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    Ok(Json(DevicesResponse {
        schema: DevicesResponse::SCHEMA,
        devices: snapshot.devices,
    }))
}

async fn device(
    path: Result<Path<String>, PathRejection>,
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<DeviceResponse>> {
    let Path(device_id) = path.map_err(|rej| path_rejection_error(&rej, &id))?;
    let _permit = acquire(&state.concurrency, &id).await?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    validate_device_id(&device_id, &snapshot.site_id, &id)?;
    match snapshot.devices.iter().find(|d| d.device_id == device_id) {
        Some(d) => Ok(Json(DeviceResponse {
            schema: DeviceResponse::SCHEMA,
            device: d.clone(),
        })),
        None => Err(ApiErrorResponse(id, ApiError::device_not_found(&device_id))),
    }
}

async fn resources(
    path: Result<Path<String>, PathRejection>,
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<ResourcesResponse>> {
    let Path(device_id) = path.map_err(|rej| path_rejection_error(&rej, &id))?;
    let _permit = acquire(&state.concurrency, &id).await?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    validate_device_id(&device_id, &snapshot.site_id, &id)?;
    match snapshot.devices.iter().find(|d| d.device_id == device_id) {
        Some(d) => Ok(Json(ResourcesResponse {
            schema: ResourcesResponse::SCHEMA,
            resources: d.resources.clone(),
        })),
        None => Err(ApiErrorResponse(id, ApiError::device_not_found(&device_id))),
    }
}

async fn properties(
    path: Result<Path<String>, PathRejection>,
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<PropertiesResponse>> {
    let Path(device_id) = path.map_err(|rej| path_rejection_error(&rej, &id))?;
    let _permit = acquire(&state.concurrency, &id).await?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    validate_device_id(&device_id, &snapshot.site_id, &id)?;
    match snapshot.devices.iter().find(|d| d.device_id == device_id) {
        Some(d) => Ok(Json(PropertiesResponse {
            schema: PropertiesResponse::SCHEMA,
            properties: d.properties.clone(),
        })),
        None => Err(ApiErrorResponse(id, ApiError::device_not_found(&device_id))),
    }
}

async fn health(
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<HealthResponse>> {
    let _permit = acquire(&state.concurrency, &id).await?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    let status = if snapshot.has_anomalies() {
        HealthStatus::Degraded
    } else {
        HealthStatus::Ok
    };
    Ok(Json(HealthResponse {
        schema: HealthResponse::SCHEMA,
        status,
        site_id: snapshot.site_id,
        session_id: snapshot.session_id,
        started_at_ns: snapshot.started_at_ns,
        devices: snapshot.devices,
        mqtt: snapshot.mqtt,
        buffer: snapshot.buffer,
    }))
}

/// 指标快照（§34.2.1；管理接口，非控制面）。
///
/// 未装配注册表时返回 503（运维可区分"未装配"与"路径错误"）；空注册表
/// 返回 200 + 空 `metrics` 对象。快照读取无锁语义，不阻塞组件热路径。
async fn metrics_snapshot(
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> Response {
    let Some(registry) = &state.metrics else {
        return ApiErrorResponse(id, ApiError::unavailable("指标未装配")).into_response();
    };
    let _permit = match acquire(&state.concurrency, &id).await {
        Ok(permit) => permit,
        Err(response) => return response.into_response(),
    };
    let metrics = registry
        .snapshot()
        .into_iter()
        .map(|(name, value)| (name, MetricView::from(value)))
        .collect();
    Json(MetricsResponse {
        schema: MetricsResponse::SCHEMA,
        captured_at_ns: crate::now_ns(),
        metrics,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;
    use crate::models::{BufferView, DeviceView, MqttView};
    use crate::state::StateError;

    fn snapshot() -> ApiSnapshot {
        ApiSnapshot {
            site_id: "plant-a".to_owned(),
            session_id: "sess-1".to_owned(),
            started_at_ns: 1,
            devices: vec![DeviceView {
                device_id: "vfd-01".to_owned(),
                name: "VFD-01".to_owned(),
                domain: "drive".to_owned(),
                driver_id: "modbus-tcp".to_owned(),
                profile_id: "inovance-md500".to_owned(),
                enabled: true,
                labels: Default::default(),
                read_items: 3,
                groups: vec![],
                properties: vec![],
                resources: vec![],
                last_batch_at_ns: Some(2),
                last_error: None,
            }],
            mqtt: MqttView {
                last_acked_at_ns: Some(3),
                last_failed_at_ns: None,
                last_error: None,
                publishes_acked: 5,
                publishes_failed: 0,
            },
            buffer: BufferView {
                inflight: 0,
                replayed_batches: 1,
            },
        }
    }

    struct StaticState(ApiSnapshot);

    impl ApiState for StaticState {
        fn snapshot(&self) -> Result<ApiSnapshot, StateError> {
            Ok(self.0.clone())
        }
    }

    struct UnavailableState;

    impl ApiState for UnavailableState {
        fn snapshot(&self) -> Result<ApiSnapshot, StateError> {
            Err(StateError::Unavailable("停机收尾中".to_owned()))
        }
    }

    struct FailingState;

    impl ApiState for FailingState {
        fn snapshot(&self) -> Result<ApiSnapshot, StateError> {
            Err(StateError::Internal("快照构造失败".to_owned()))
        }
    }

    fn app(state: Arc<dyn ApiState>) -> Router {
        router(state, Arc::new(Semaphore::new(64)))
    }

    /// 带 metrics 注册表的测试装配（§34.2.1 metrics 端点）。
    fn app_with_metrics(
        state: Arc<dyn ApiState>,
        registry: std::sync::Arc<metrics::MetricsRegistry>,
    ) -> Router {
        router_with_options(state, Some(registry), Arc::new(Semaphore::new(64)))
            .fallback(fallback_not_found)
            .method_not_allowed_fallback(fallback_method_not_allowed)
            .layer(middleware::from_fn(request_id_layer))
    }

    async fn get(router: Router, path: &str) -> (StatusCode, Value) {
        let res = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(path)
                    .body(Body::empty())
                    .expect("请求合法"),
            )
            .await
            .expect("路由可用");
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .expect("读取响应");
        (status, serde_json::from_slice(&bytes).expect("响应为 JSON"))
    }

    // ---- §34.2.1 GET /api/v1/metrics ------------------------------------

    #[tokio::test]
    async fn metrics_endpoint_reflects_registry_snapshot() {
        let registry = std::sync::Arc::new(metrics::MetricsRegistry::new());
        let counter = registry.counter("poll_batches_total");
        counter.inc();
        counter.add(2);
        registry.gauge("wal_inflight_gauge").set(7);
        let hist = registry.histogram("schedule_delay_ns_hist");
        hist.observe_ns(30_000);

        let app = app_with_metrics(Arc::new(StaticState(snapshot())), registry);
        let (status, body) = get(app, "/api/v1/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.metrics.v1");
        assert!(
            body["captured_at_ns"].is_u64(),
            "captured_at_ns 应为 ns 时间戳"
        );
        assert_eq!(body["metrics"]["poll_batches_total"]["kind"], "count");
        assert_eq!(body["metrics"]["poll_batches_total"]["value"], 3);
        assert_eq!(body["metrics"]["wal_inflight_gauge"]["kind"], "gauge");
        assert_eq!(body["metrics"]["wal_inflight_gauge"]["value"], 7);
        assert_eq!(
            body["metrics"]["schedule_delay_ns_hist"]["kind"],
            "histogram"
        );
        assert_eq!(body["metrics"]["schedule_delay_ns_hist"]["count"], 1);
        assert!(
            body["metrics"]["schedule_delay_ns_hist"]["bounds"].is_array(),
            "直方图应输出固定桶边界"
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_empty_registry_returns_empty_object() {
        let app = app_with_metrics(
            Arc::new(StaticState(snapshot())),
            std::sync::Arc::new(metrics::MetricsRegistry::new()),
        );
        let (status, body) = get(app, "/api/v1/metrics").await;
        assert_eq!(status, StatusCode::OK, "空注册表是合法状态（200 非 404）");
        assert_eq!(body["schema"], "forgelink.metrics.v1");
        assert_eq!(
            body["metrics"].as_object().map(|m| m.len()),
            Some(0),
            "空注册表应序列化为空对象"
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_absent_without_registry_is_503_envelope() {
        // 未注入 registry 的服务器：管理端点按 §31.6 信封返回 503
        // （SERVICE_UNAVAILABLE）而非 404/panic——运维可据此区分"未装配"
        // 与"路径错误"。
        let app = app(Arc::new(StaticState(snapshot())));
        let (status, body) = get(app, "/api/v1/metrics").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["schema"], "forgelink.error.v1");
    }

    #[tokio::test]
    async fn devices_list_and_single() {
        let app = app(Arc::new(StaticState(snapshot())));
        let (status, body) = get(app.clone(), "/api/v1/devices").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.devices.v1");
        assert_eq!(body["devices"][0]["device_id"], "vfd-01");
        assert_eq!(body["devices"][0]["driver_id"], "modbus-tcp");
        assert_eq!(body["devices"][0]["profile_id"], "inovance-md500");

        let (status, body) = get(app, "/api/v1/devices/vfd-01").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.device.v1");
        assert_eq!(body["device"]["enabled"], true);
        assert_eq!(body["device"]["last_batch_at_ns"], 2);
    }

    #[tokio::test]
    async fn unknown_device_returns_404_with_request_id() {
        let app = app(Arc::new(StaticState(snapshot())));
        let (status, body) = get(app, "/api/v1/devices/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["schema"], "forgelink.error.v1");
        assert_eq!(body["code"], "DEVICE_NOT_FOUND");
        assert!(body["request_id"].as_str().unwrap().starts_with("req-"));
    }

    #[tokio::test]
    async fn resources_and_properties_endpoints() {
        let mut s = snapshot();
        s.devices[0].resources =
            crate::resource::derive_resources(["drive.output.frequency", "drive.output.current"]);
        s.devices[0].properties = vec![crate::models::PropertyView {
            path: "drive.output.frequency".to_owned(),
            display_name: "drive.output.frequency".to_owned(),
            value_type: "f64".to_owned(),
            unit: Some("Hz".to_owned()),
            readable: true,
            writable: true,
            min: None,
            max: None,
            interval_ms: Some(50),
        }];
        let app = app(Arc::new(StaticState(s)));

        let (status, body) = get(app.clone(), "/api/v1/devices/vfd-01/resources").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.resources.v1");
        assert_eq!(body["resources"][0]["path"], "drive");
        let output_properties = body["resources"]
            .as_array()
            .expect("resources 为数组")
            .iter()
            .find(|r| r["path"] == "drive.output")
            .expect("drive.output 资源存在");
        assert_eq!(
            output_properties["properties"],
            serde_json::json!(["drive.output.current", "drive.output.frequency"]),
            "drive.output 下挂两个属性（字典序）"
        );

        let (status, body) = get(app.clone(), "/api/v1/devices/vfd-01/properties").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.properties.v1");
        assert_eq!(body["properties"][0]["unit"], "Hz");
        assert_eq!(body["properties"][0]["value_type"], "f64");

        let (status, body) = get(app, "/api/v1/devices/nope/resources").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "DEVICE_NOT_FOUND");
    }

    #[tokio::test]
    async fn health_endpoint_fields() {
        let app = app(Arc::new(StaticState(snapshot())));
        let (status, body) = get(app, "/api/v1/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema"], "forgelink.health.v1");
        assert_eq!(body["status"], "ok");
        assert_eq!(body["site_id"], "plant-a");
        assert_eq!(body["mqtt"]["publishes_acked"], 5);
        assert_eq!(body["buffer"]["replayed_batches"], 1);
    }

    #[tokio::test]
    async fn health_degraded_when_device_error() {
        let mut s = snapshot();
        s.devices[0].last_error = Some("读超时".to_owned());
        let app = app(Arc::new(StaticState(s)));
        let (_, body) = get(app, "/api/v1/health").await;
        assert_eq!(body["status"], "degraded");
    }

    #[tokio::test]
    async fn health_degraded_when_mqtt_error() {
        let mut s = snapshot();
        s.mqtt.last_error = Some("连接重置".to_owned());
        let app = app(Arc::new(StaticState(s)));
        let (_, body) = get(app, "/api/v1/health").await;
        assert_eq!(body["status"], "degraded", "MQTT last_error 应降级");
    }

    #[tokio::test]
    async fn health_degraded_when_publish_failed() {
        let mut s = snapshot();
        s.mqtt.publishes_failed = 3;
        let app = app(Arc::new(StaticState(s)));
        let (_, body) = get(app, "/api/v1/health").await;
        assert_eq!(body["status"], "degraded", "累计发布失败应降级");
    }

    #[tokio::test]
    async fn health_degraded_when_wal_inflight_stuck() {
        let mut s = snapshot();
        // 在途记录滞留且北向最近失败：WAL 侧异常（评审 P2）。
        s.buffer.inflight = 2;
        s.mqtt.last_failed_at_ns = Some(4);
        let app = app(Arc::new(StaticState(s)));
        let (_, body) = get(app, "/api/v1/health").await;
        assert_eq!(body["status"], "degraded", "WAL 在途滞留应降级");
    }

    #[tokio::test]
    async fn health_ok_with_inflight_but_no_mqtt_failure() {
        let mut s = snapshot();
        // 在途记录属正常 ACK 窗口：无北向失败时不得误判降级。
        s.buffer.inflight = 2;
        let app = app(Arc::new(StaticState(s)));
        let (_, body) = get(app, "/api/v1/health").await;
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn unavailable_maps_to_503() {
        let app = app(Arc::new(UnavailableState));
        let (status, body) = get(app, "/api/v1/health").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "SERVICE_UNAVAILABLE");
    }

    #[tokio::test]
    async fn internal_error_maps_to_500() {
        let app = app(Arc::new(FailingState));
        let (status, body) = get(app, "/api/v1/devices").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["code"], "INTERNAL_ERROR");
    }

    #[tokio::test]
    async fn control_routes_not_exposed() {
        let app = app(Arc::new(StaticState(snapshot())));
        // POST /controls 与 GET devices/{id}/control-requests 都不存在；
        // 未知方法返回 405。
        // 本测试在两种构建下都必须通过：
        // - 默认（只读）构建：`control` feature 未启用，控制代码不编译，
        //   路由天然不存在（§固定架构：只读版本不得暴露控制入口）；
        // - `--all-features` 构建：`router()`/`spawn()` 保持纯只读装配，
        //   控制路由仅经 `spawn_with_control` 挂载。
        for (method, path, expect_status) in [
            (
                Method::POST,
                "/api/v1/devices/vfd-01/controls",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::GET,
                "/api/v1/devices/vfd-01/control-requests/cmd-1",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::POST,
                "/api/v1/devices/vfd-01/properties",
                StatusCode::METHOD_NOT_ALLOWED,
            ),
        ] {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(&method)
                        .uri(path)
                        .body(Body::empty())
                        .expect("请求合法"),
                )
                .await
                .expect("路由可用");
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .expect("读取响应");
            assert_eq!(status, expect_status, "{method} {path}");
            let body: Value = serde_json::from_slice(&bytes).expect("响应为 JSON");
            assert_eq!(body["schema"], "forgelink.error.v1", "{method} {path}");
        }
    }

    #[tokio::test]
    async fn sensitive_fields_never_exposed() {
        let mut s = snapshot();
        s.devices[0]
            .labels
            .insert("note".to_owned(), "hello".to_owned());
        let app = app(Arc::new(StaticState(s)));
        let (_, body) = get(app, "/api/v1/devices").await;
        let text = serde_json::to_string(&body).expect("序列化");
        for banned in [
            "connection",
            "driver_address",
            "password",
            "username",
            "ca_pem",
            "private_key",
            "db_path",
            "1!40001",
        ] {
            assert!(!text.contains(banned), "响应不得泄漏敏感字段 {banned:?}");
        }
        assert!(text.contains("labels"));
    }

    #[tokio::test]
    async fn bad_device_id_rejected_with_400() {
        // %2F 解码为 `/`：破坏主题层级，配置层同样拒绝（§31.1）。
        let router = app(Arc::new(StaticState(snapshot())));
        let (status, body) = get(router, "/api/v1/devices/a%2Fb").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "BAD_REQUEST");

        // 通配符与空标识同样非法（与配置校验集合一致，评审 P2）。
        let router = app(Arc::new(StaticState(snapshot())));
        let (status, _) = get(router, "/api/v1/devices/ab%2Bcd").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "通配符 + 解码后拒绝");
        let router = app(Arc::new(StaticState(snapshot())));
        let (status, _) = get(router, "/api/v1/devices/%00").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "控制字符拒绝");
    }

    #[tokio::test]
    async fn device_id_with_space_or_backslash_is_queryable() {
        // 评审 P2：配置校验允许空格/反斜杠（§31.1 主题规则仅拒绝
        // 空、`/`、通配符、控制字符与超长），REST 不得比配置更严——
        // 设备出现在列表中就必须可通过详情接口访问。
        let mut s = snapshot();
        s.devices[0].device_id = "cnc 01".to_owned();
        let router = app(Arc::new(StaticState(s)));
        let (status, body) = get(router, "/api/v1/devices/cnc%2001").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["device"]["device_id"], "cnc 01");

        let mut s = snapshot();
        s.devices[0].device_id = r"cnc\01".to_owned();
        let router = app(Arc::new(StaticState(s)));
        let (status, body) = get(router, "/api/v1/devices/cnc%5C01").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["device"]["device_id"], r"cnc\01");
    }

    #[tokio::test]
    async fn state_error_message_never_leaks_internal_detail() {
        // 评审 P2：StateError 原始文本（可能含路径/连接信息）只进日志，
        // 外部响应为固定安全文案。
        let router = app(Arc::new(UnavailableState));
        let (_, body) = get(router, "/api/v1/health").await;
        assert_eq!(body["message"], "运行时暂不可用");
        assert!(
            !body["message"].as_str().unwrap().contains("停机收尾中"),
            "内部细节不得出现在响应 message"
        );

        let router = app(Arc::new(FailingState));
        let (_, body) = get(router, "/api/v1/devices").await;
        assert_eq!(body["message"], "内部错误");
        assert!(
            !body["message"].as_str().unwrap().contains("快照构造失败"),
            "内部细节不得出现在响应 message"
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_does_not_fire_abnormal_exit_notification() {
        let server = RestApiServer::spawn(
            Arc::new(StaticState(snapshot())),
            RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: 4,
                metrics: None,
            },
        )
        .await
        .expect("绑定成功");
        let mut exit = server.exit_notified();
        server.shutdown().await;
        // 正常停机走 stop 信号，serve 任务优雅返回 Ok：不触发通知。
        assert!(!*exit.borrow_and_update(), "正常停机不得触发异常退出通知");
        assert!(exit.has_changed().is_err() || !*exit.borrow(), "通道无新值");
    }

    #[tokio::test]
    async fn is_alive_true_while_serving_and_false_after_task_exit() {
        let server = RestApiServer::spawn(
            Arc::new(StaticState(snapshot())),
            RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: 4,
                metrics: None,
            },
        )
        .await
        .expect("绑定成功");
        assert!(server.is_alive(), "serve 任务运行中 alive 必须为 true");

        // 触发停机信号并等待任务退出（异常退出走同一置位路径，
        // 评审 P2：`serve` 任务结束即标记不可用）。
        let alive = server.alive.clone();
        let _ = server.stop.send(true);
        drop(server.stop);
        let _ = server.join.lock().await.take().expect("任务句柄存在").await;
        assert!(
            !alive.load(Ordering::SeqCst),
            "serve 任务退出后 alive 必须为 false"
        );
    }

    #[tokio::test]
    async fn invalid_url_encoding_returns_unified_error() {
        // 评审 P2：`%FF` 等非法 UTF-8 编码在 handler 之前被 axum 拒绝
        // （PathRejection），必须统一映射为 §31.6 400 错误载荷（含
        // request_id），而不是 axum 默认的纯文本 400。
        let router = app(Arc::new(StaticState(snapshot())));
        for path in [
            "/api/v1/devices/%FF",
            "/api/v1/devices/a%FFb",
            "/api/v1/devices/%FF/resources",
            "/api/v1/devices/%FF/properties",
        ] {
            let (status, body) = get(router.clone(), path).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(body["schema"], "forgelink.error.v1", "{path}");
            assert_eq!(body["code"], "BAD_REQUEST", "{path}");
            assert!(
                body["request_id"].as_str().unwrap().starts_with("req-"),
                "{path} 必须携带 request_id"
            );
        }
    }

    #[tokio::test]
    async fn device_id_length_validated_on_full_topic() {
        // 评审 P2：配置校验的是完整 MQTT 主题长度（§31.1，
        // `forgelink/v1/telemetry/{site_id}/{device_id}` ≤ 65535 字节），
        // REST 不得只校验设备 ID 自身——接近上限的 ID 通过 REST 却
        // 无法启动配置，二者必须同集合（共用 `telemetry_topic`）。
        let router = app(Arc::new(StaticState(snapshot())));
        // site_id "plant-a"（7 字节）+ 前缀 "forgelink/v1/telemetry/"
        // （23 字节）+ '/'（1 字节）：设备 ID 上限 = 65535 - 31 = 65504。
        let at_limit = "x".repeat(65504);
        let (status, body) = get(router.clone(), &format!("/api/v1/devices/{at_limit}")).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "65504 字节设备 ID 完整主题恰好 65535，应通过校验（仅 404）"
        );
        assert_eq!(body["code"], "DEVICE_NOT_FOUND");

        let over_limit = "x".repeat(65505);
        let (status, body) = get(router, &format!("/api/v1/devices/{over_limit}")).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "65505 字节设备 ID 完整主题超长，配置层同样拒绝"
        );
        assert_eq!(body["code"], "BAD_REQUEST");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn forced_abort_clears_alive_flag() {
        // 评审 P2：排空超时强制 abort 时，serve 任务内部的 alive=false
        // 置位不会执行，abort 分支必须显式清除存活标记。
        use tokio::io::AsyncWriteExt;

        let server = RestApiServer::spawn(
            Arc::new(StaticState(snapshot())),
            RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: 4,
                metrics: None,
            },
        )
        .await
        .expect("绑定成功");
        // 半截 HTTP 请求让连接停在"在途请求"状态：优雅停机等待它
        // 完成，2s 排空超时后触发强制 abort 分支。
        let mut stream = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("连接成功");
        stream
            .write_all(b"GET /api/v1/devices HTTP/1.1\r\nHost: test\r\n")
            .await
            .expect("写入半截请求");
        // 等待 serve 任务接受连接并进入"在途请求"读取状态（否则停机
        // 信号先于连接注册到达，优雅停机立即完成，测不到 abort 分支）。
        tokio::time::sleep(Duration::from_millis(200)).await;

        let alive = server.alive.clone();
        let started = std::time::Instant::now();
        server.shutdown().await;
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "应等待排空超时（约 2s），未走到 abort 分支"
        );
        assert!(
            !alive.load(Ordering::SeqCst),
            "强制 abort 后 alive 必须显式清除为 false"
        );
    }

    #[tokio::test]
    async fn spawn_rejects_zero_concurrency() {
        // 评审 P2：0 是非法配置，`spawn` 必须拒绝而非静默改为 1——
        // 静默修正会让实际并发数与配置不一致，掩盖配置错误。
        let result = RestApiServer::spawn(
            Arc::new(StaticState(snapshot())),
            RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: 0,
                metrics: None,
            },
        )
        .await;
        let err = match result {
            Ok(_) => panic!("0 并发必须拒绝"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn spawn_rejects_above_max_permits() {
        // 评审 P2：超过 `Semaphore::MAX_PERMITS` 时 `Semaphore::new`
        // 会 panic，`spawn` 必须返回错误而非崩溃。
        let result = RestApiServer::spawn(
            Arc::new(StaticState(snapshot())),
            RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: Semaphore::MAX_PERMITS + 1,
                metrics: None,
            },
        )
        .await;
        let err = match result {
            Ok(_) => panic!("超限必须拒绝"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_listen_validation_accepts_only_loopback() {
        // 评审二轮 P2（§90.2）：控制装配仅允许 loopback——IPv4
        // 127.0.0.0/8 与 IPv6 ::1；其余（含通配与私网）一律拒绝。
        for addr in ["127.0.0.1:8080", "127.9.9.9:8080", "[::1]:8080"] {
            RestApiServer::validate_control_listen(addr.parse().expect("静态地址合法"))
                .unwrap_or_else(|e| panic!("{addr} 应通过: {e}"));
        }
        for addr in [
            "0.0.0.0:8080",
            "192.168.1.10:8080",
            "[::]:8080",
            "[fd00::1]:8080",
        ] {
            let err = RestApiServer::validate_control_listen(addr.parse().expect("静态地址合法"))
                .expect_err("非 loopback 必须拒绝");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{addr}");
            assert!(
                err.to_string().contains("loopback"),
                "错误应说明仅允许 loopback: {addr}"
            );
        }
    }
}
