//! ForgeLink REST v1 只读管理接口（§31.5/§31.6、§90.1 安全基线）。
//!
//! # 契约
//!
//! 本分支（`feat/rest-api-readonly`）只提供只读管理与健康检查，**不实现
//! 控制链路**（Property Write / Command Execute 属于 Control Engine，
//! 后续分支接入；本 crate 不暴露 `/controls` 路由）。
//!
//! 端点（`/api/v1` 前缀）：
//!
//! ```text
//! GET /api/v1/devices                      设备列表
//! GET /api/v1/devices/{device_id}          单设备详情
//! GET /api/v1/devices/{device_id}/resources 设备资源树（由属性路径派生）
//! GET /api/v1/devices/{device_id}/properties 设备属性清单
//! GET /api/v1/health                       健康检查（§104 Health endpoint）
//! ```
//!
//! 所有响应显式携带 `schema` 版本字段（§31.6：禁止依赖隐式字段解释）：
//! `forgelink.devices.v1` / `forgelink.device.v1` / `forgelink.resources.v1`
//! / `forgelink.properties.v1` / `forgelink.health.v1` / `forgelink.error.v1`。
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
//! 状态码：400 请求格式错误 / 404 资源不存在 / 409 状态冲突（本分支
//! 无写操作，模型保留供控制阶段使用）/ 500 内部错误 / 503 运行时不可用。
//! `request_id` 在请求进入时生成（`req-{纳秒}-{序号}`），贯穿错误响应
//! 与 `tracing` 日志字段。
//!
//! # 安全边界（§90.1）
//!
//! - 服务器**默认不启动**（`listen = None`）；启用必须显式配置，默认
//!   绑定 `127.0.0.1` / `::1`（loopback）。
//! - 响应**禁止**返回：Driver 连接配置与地址、MQTT 用户名/密码、TLS
//!   证书与私钥、内部句柄/线程/数据库细节（如 WAL 文件路径）。
//! - 并发有界（`RestConfig::max_concurrency`，默认 64，`Semaphore`）。
//! - 停机有界：停止后拒绝新连接，在途请求限时排空，不阻塞采集链路。
//!
//! # 适配层
//!
//! 本 crate 不依赖任何运行时：调用方实现 [`ApiState`]（只读快照，
//! 同步、短锁，禁止跨 `await` 持有锁），用 [`RestApiServer::spawn`]
//! 启动。

pub mod error;
pub mod models;
pub mod resource;
pub mod server;
pub mod state;

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
