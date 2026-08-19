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
//! - 未匹配路径与不支持的方法统一返回 §31.6 错误载荷（控制路由
//!   [`crate::models`] 只读契约之外不可达）。

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tracing::{Instrument, error, info, warn};

use crate::HEALTH_PATH;
use crate::error::{ApiError, ErrorCode, RequestId, to_response};
use crate::models::{
    ApiSnapshot, DeviceResponse, DevicesResponse, HealthResponse, HealthStatus, PropertiesResponse,
    ResourcesResponse,
};
use crate::state::{ApiState, map_state_error};

/// 统一 handler 返回类型：错误携带 `request_id`（§31.6）。
type ApiResult<T> = Result<T, ApiErrorResponse>;

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
}

impl Default for RestConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".parse().expect("静态地址合法"),
            max_concurrency: 64,
        }
    }
}

/// 服务器句柄：持有停止信号与任务，提供有界停机。
pub struct RestApiServer {
    stop: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
    /// 服务存活标记：`serve` 任务退出（正常停机或异常）后置 false，
    /// 供调用方（Collector 运行时）感知异常退出（评审 P2）。
    alive: Arc<AtomicBool>,
    /// 实际监听地址（配置 `:0` 随机端口时用于查询）。
    pub addr: SocketAddr,
}

impl RestApiServer {
    /// 绑定并启动服务器（独立任务；`state` 为只读快照提供者）。
    ///
    /// # Errors
    ///
    /// 监听地址绑定失败（占用/权限等）时返回错误，调用方应显式失败
    /// 启动（不静默降级）。
    pub async fn spawn(
        state: Arc<dyn ApiState>,
        config: RestConfig,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(config.listen).await?;
        let addr = listener.local_addr()?;
        let (stop_tx, stop_rx) = watch::channel(false);
        let concurrency = Arc::new(Semaphore::new(config.max_concurrency.max(1)));
        let app = router(state, concurrency);
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
            }
        });
        info!(
            component = "rest-api",
            addr = %addr,
            max_concurrency = config.max_concurrency,
            "REST v1 只读接口已启动（loopback 默认绑定）"
        );
        Ok(Self {
            stop: stop_tx,
            join,
            alive,
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

    /// 优雅停机：拒绝新连接 → 在途请求限时排空 → 任务结束。
    ///
    /// 有界（`SHUTDOWN_GRACE` 2s）：超时强制取消，绝不阻塞采集链路。
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        drop(self.stop);
        if tokio::time::timeout(SHUTDOWN_GRACE, &mut self.join)
            .await
            .is_err()
        {
            warn!(component = "rest-api", "REST 停机排空超时，强制取消");
            self.join.abort();
            let _ = (&mut self.join).await;
        }
        info!(component = "rest-api", "REST v1 接口已停止");
    }
}

/// 组装只读路由（§31.5 最小资源路径 + §104 健康检查）。
///
/// 未匹配路径（404）与不支持的 method（405）都返回统一 §31.6 错误
/// 载荷（含 request_id）；`/controls` 等控制路由不存在于本只读契约。
fn router(state: Arc<dyn ApiState>, concurrency: Arc<Semaphore>) -> Router {
    Router::new()
        .route("/api/v1/devices", get(devices))
        .route("/api/v1/devices/{device_id}", get(device))
        .route("/api/v1/devices/{device_id}/resources", get(resources))
        .route("/api/v1/devices/{device_id}/properties", get(properties))
        .route(HEALTH_PATH, get(health))
        .with_state(AppState { state, concurrency })
        .fallback(fallback_not_found)
        .method_not_allowed_fallback(fallback_method_not_allowed)
        .layer(middleware::from_fn(request_id_layer))
}

/// 未匹配路径（404，§31.6 统一错误载荷）。
async fn fallback_not_found(axum::Extension(id): axum::Extension<RequestId>) -> Response {
    ApiErrorResponse(
        id,
        ApiError {
            code: ErrorCode::ResourceNotFound,
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
async fn acquire(
    state: &AppState,
    id: &RequestId,
) -> Result<tokio::sync::OwnedSemaphorePermit, ApiErrorResponse> {
    match tokio::time::timeout(CONCURRENCY_WAIT, state.concurrency.clone().acquire_owned()).await {
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
        error!(component = "rest-api", request_id = %id, error = %e, "REST 快照失败");
        ApiErrorResponse(id.clone(), map_state_error(&e))
    })
}

/// 设备 ID 路径参数校验（§31.1 主题层级规则：非法字符直接 400）。
fn validate_device_id(device_id: &str, id: &RequestId) -> Result<(), ApiErrorResponse> {
    if device_id.is_empty()
        || device_id.contains('/')
        || device_id.contains('\\')
        || device_id.contains(' ')
    {
        return Err(ApiErrorResponse(
            id.clone(),
            ApiError::bad_request(format!("非法设备标识 {device_id:?}")),
        ));
    }
    Ok(())
}

async fn devices(
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<DevicesResponse>> {
    let _permit = acquire(&state, &id).await?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    Ok(Json(DevicesResponse {
        schema: DevicesResponse::SCHEMA,
        devices: snapshot.devices,
    }))
}

async fn device(
    Path(device_id): Path<String>,
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<DeviceResponse>> {
    let _permit = acquire(&state, &id).await?;
    validate_device_id(&device_id, &id)?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    match snapshot.devices.iter().find(|d| d.device_id == device_id) {
        Some(d) => Ok(Json(DeviceResponse {
            schema: DeviceResponse::SCHEMA,
            device: d.clone(),
        })),
        None => Err(ApiErrorResponse(id, ApiError::device_not_found(&device_id))),
    }
}

async fn resources(
    Path(device_id): Path<String>,
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<ResourcesResponse>> {
    let _permit = acquire(&state, &id).await?;
    validate_device_id(&device_id, &id)?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
    match snapshot.devices.iter().find(|d| d.device_id == device_id) {
        Some(d) => Ok(Json(ResourcesResponse {
            schema: ResourcesResponse::SCHEMA,
            resources: d.resources.clone(),
        })),
        None => Err(ApiErrorResponse(id, ApiError::device_not_found(&device_id))),
    }
}

async fn properties(
    Path(device_id): Path<String>,
    State(state): State<AppState>,
    axum::Extension(id): axum::Extension<RequestId>,
) -> ApiResult<Json<PropertiesResponse>> {
    let _permit = acquire(&state, &id).await?;
    validate_device_id(&device_id, &id)?;
    let snapshot = snapshot_or_error(&state.state, &id)?;
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
    let _permit = acquire(&state, &id).await?;
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
        // POST /controls 与 GET /control-requests 都不存在（§31.5 控制
        // 路由属于 Control Engine，本分支禁止暴露）；未知方法返回 405。
        for (method, path, expect_status) in [
            (
                Method::POST,
                "/api/v1/devices/vfd-01/controls",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::GET,
                "/api/v1/control-requests/cmd-1",
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
        let app = app(Arc::new(StaticState(snapshot())));
        let (status, body) = get(app, "/api/v1/devices/a%2Fb").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "BAD_REQUEST");
    }

    #[tokio::test]
    async fn is_alive_true_while_serving_and_false_after_task_exit() {
        let server = RestApiServer::spawn(
            Arc::new(StaticState(snapshot())),
            RestConfig {
                listen: "127.0.0.1:0".parse().expect("静态地址合法"),
                max_concurrency: 4,
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
        let _ = server.join.await;
        assert!(
            !alive.load(Ordering::SeqCst),
            "serve 任务退出后 alive 必须为 false"
        );
    }
}
