//! ForgeLink REST v1 管理接口（§31.5/§31.6、§90.1/§90.2 安全基线）。
//!
//! # 契约
//!
//! 只读管理与健康检查始终可用；控制链路（Property Write / Command
//! Execute，经 Control Engine §81 统一入口）由 **`control` feature 门控**：
//! 默认不启用——只读构建不编译任何控制代码，控制路由不存在（§固定架构：
//! 只读版本不得暴露控制入口）。启用后需经
//! [`RestApiServer::spawn_with_control`] 装配（[`RestApiServer::spawn`]
//! 保持纯只读语义）。
//!
//! 端点（`/api/v1` 前缀）：
//!
//! ```text
//! GET  /api/v1/devices                        设备列表
//! GET  /api/v1/devices/{device_id}            单设备详情
//! GET  /api/v1/devices/{device_id}/resources  设备资源树（由属性路径派生）
//! GET  /api/v1/devices/{device_id}/properties 设备属性清单
//! GET  /api/v1/health                         健康检查（§104 Health endpoint）
//! --- 以下仅 control feature ---
//! POST /api/v1/devices/{device_id}/controls   控制提交（202 异步受理，§31.5）
//! GET  /api/v1/control-requests/{request_id}  控制状态查询（三态，§31.5）
//! ```
//!
//! 所有响应显式携带 `schema` 版本字段（§31.6：禁止依赖隐式字段解释）：
//! `forgelink.devices.v1` / `forgelink.device.v1` / `forgelink.resources.v1`
//! / `forgelink.properties.v1` / `forgelink.health.v1` / `forgelink.error.v1`
//! / `forgelink.control.accepted.v1` / `forgelink.control.status.v1`。
//!
//! # 错误模型（§31.6）
//!
//! ```json
//! {
//!   "schema": "forgelink.error.v1",
//!   "code": "DEVICE_NOT_FOUND",
//!   "message": "设备 vfd-09 不存在",
//!   "request_id": "req-...",
//!   "details": {}
//! }
//! ```
//!
//! 状态码：400 请求格式错误 / 401 未认证（§90.2）/ 403 角色不足（§83）/
//! 404 资源不存在 / 405 方法不支持 / 409 状态冲突 / 422 语义校验失败
//! （§84）/ 500 内部错误 / 503 运行时不可用。控制链路的信封 `code`
//! 透传引擎稳定错误码（如 `VALUE_OUT_OF_RANGE`），HTTP 状态按 §31.6
//! 映射。`request_id` 在请求进入时生成（`req-{纳秒}-{序号}`），贯穿
//! 错误响应与 `tracing` 日志字段。
//!
//! # 安全边界（§90.1/§90.2）
//!
//! - 服务器**默认不启动**（`listen = None`）；启用必须显式配置，默认
//!   绑定 `127.0.0.1` / `::1`（loopback）。
//! - 控制端点要求 `Authorization: Bearer <token>`（§90.2
//!   [`control_engine::StaticTokenAuthorizer`] 常量时间比较）；缺失/
//!   非法/未知 Token 一律 401，Token 明文不进日志与错误信息。
//! - 响应**禁止**返回：Driver 连接配置与地址、MQTT 用户名/密码、TLS
//!   证书与私钥、内部句柄/线程/数据库细节（如 WAL 文件路径）。
//! - 并发有界（`RestConfig::max_concurrency`，默认 64，`Semaphore`；
//!   只读与控制路由共用同一门控）。
//! - 停机有界：停止后拒绝新连接，在途请求限时排空，不阻塞采集链路。
//!
//! # 适配层
//!
//! 本 crate 不直接依赖采集运行时：调用方实现 [`ApiState`]（只读快照，
//! 同步、短锁，禁止跨 `await` 持有锁）与 [`control::ControlAdapter`]
//! （控制提交/查询；可直接使用内置的
//! [`control::EngineControlAdapter`] 包装真实 Control Engine），用
//! [`RestApiServer::spawn`] / [`RestApiServer::spawn_with_control`]
//! 启动。

#[cfg(feature = "control")]
pub mod control;
pub mod error;
pub mod models;
pub mod resource;
pub mod server;
pub mod state;

#[cfg(feature = "control")]
pub use control::{
    CONTROL_ACCEPTED_SCHEMA, CONTROL_REQUEST_SCHEMA, CONTROL_REQUESTS_PATH, CONTROL_STATUS_SCHEMA,
    CONTROLS_PATH, ControlAcceptedResponse, ControlAdapter, ControlGateway, ControlStatusQuery,
    ControlStatusResponse, EngineControlAdapter, StatusQueryError, Submission,
};
pub use error::{ApiError, ErrorCode, ErrorResponse, RequestId};
pub use models::{
    ApiSnapshot, BufferView, DeviceView, GroupView, HealthResponse, HealthStatus, MqttView,
    PropertyView, ResourceView,
};
pub use server::{RestApiServer, RestConfig};
pub use state::{ApiState, StateError};

/// 本分支 REST 契约的 API 前缀（版本化，§31.5）。
pub const API_PREFIX: &str = "/api/v1";

/// 健康检查端点的完整路径（§104 Health endpoint）。
pub const HEALTH_PATH: &str = "/api/v1/health";

/// 当前时间纳秒（i64，UNIX 时间；request_id 前缀与时间戳语义）。
pub fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default()
}
