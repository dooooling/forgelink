//! 只读适配接口（§31.5 运行时接入：REST 不直接依赖采集组件）。
//!
//! 调用方（Collector 运行时等）实现 [`ApiState`]：每次请求**同步**取齐
//! 一份 [`ApiSnapshot`]（短锁/原子计数，毫秒级），禁止在 `await` 期间
//! 持有任何运行时锁——`snapshot` 为同步方法，服务器侧只在其返回后
//! 才继续异步 I/O。API 停止不影响采集、WAL 与 MQTT（服务器是独立任务）。

use crate::models::ApiSnapshot;

/// 适配层返回错误：`Unavailable` → 503，`Internal` → 500。
#[derive(Debug, Clone)]
pub enum StateError {
    /// 运行时暂不可用（如停机收尾阶段）。
    Unavailable(String),
    /// 快照构造失败（内部错误）。
    Internal(String),
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(msg) => write!(f, "运行时不可用: {msg}"),
            Self::Internal(msg) => write!(f, "内部错误: {msg}"),
        }
    }
}

impl std::error::Error for StateError {}

/// 只读快照提供者（`Send + Sync`：可被服务器任务与请求并发调用）。
pub trait ApiState: Send + Sync + 'static {
    /// 取一份完整快照（同步；禁止阻塞与跨 `await` 持锁）。
    fn snapshot(&self) -> Result<ApiSnapshot, StateError>;
}

/// 把适配错误映射为 API 错误（503/500）。
pub(crate) fn map_state_error(err: &StateError) -> crate::ApiError {
    match err {
        StateError::Unavailable(msg) => crate::ApiError::unavailable(msg.clone()),
        StateError::Internal(msg) => crate::ApiError::internal(msg.clone()),
    }
}
