//! Local Buffer 错误模型。

use std::fmt;

/// 容量类型（§103）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityKind {
    /// 同一待落盘记录的**等待者数量**达到上限（评审 P3：不再表示
    /// mem + inflight 达到 `memory_records`——内存窗口不是容量，
    /// 磁盘是第二级容量；本类型仅用于背压等待队列的等待者上限）。
    Memory,
    /// 磁盘未确认记录字节达到 `disk_max_bytes`。
    Disk,
}

/// Local Buffer 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalBufferError {
    /// 配置非法（`LocalBufferConfig::validate` 拒绝，或 `open` 前置
    /// 校验失败——父目录不存在、路径不可写等均**明确报错**，不静默
    /// 回退默认值）。
    InvalidConfig { field: &'static str, reason: String },
    /// Batch 非法（`site_id` / `device_id` 为空或包含 `/`，无法派生
    /// §31.1 主题；或序列化失败）。
    InvalidBatch { reason: String },
    /// 容量不足且 [`CapacityPolicy::Reject`](crate::CapacityPolicy::Reject)
    /// ——push 被显式拒绝，未入队（禁止静默覆盖未确认数据）。
    ///
    /// 单位契约（评审 P3）：`limit` / `current` / `requested` 的单位
    /// 随 `kind`——`Memory` 为**记录条数**（背压等待队列长度），
    /// `Disk` 为**估算成本字节数**。
    CapacityExceeded {
        kind: CapacityKind,
        /// 配置上限（单位随 `kind`）。
        limit: u64,
        /// 当前占用（单位随 `kind`）。
        current: u64,
        /// 本次请求的新增量（单位随 `kind`；`Memory` 时为 1，即新增
        /// 一个等待者）。
        requested: u64,
    },
    /// 数据库损坏或 schema 非法（非 SQLite 文件、表缺失、`user_version`
    /// 超出已知版本）。
    Corrupt { reason: String },
    /// SQLite 操作失败（磁盘错误等）。
    Db { reason: String },
    /// 缓冲已停机（`shutdown` 后所有命令返回此错误）。
    Closed,
    /// 后台任务异常终止。
    WorkerFailed { reason: String },
}

impl fmt::Display for LocalBufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(f, "local-buffer 配置非法（{field}）: {reason}")
            }
            Self::InvalidBatch { reason } => write!(f, "local-buffer Batch 非法: {reason}"),
            Self::CapacityExceeded {
                kind,
                limit,
                current,
                requested,
            } => {
                let (name, unit) = match kind {
                    CapacityKind::Memory => ("内存等待队列", "条记录"),
                    CapacityKind::Disk => ("磁盘", "字节"),
                };
                write!(
                    f,
                    "local-buffer 容量不足（{name}: {current}/{limit} {unit}，本次请求 {requested} {unit}）"
                )
            }
            Self::Corrupt { reason } => write!(f, "local-buffer 数据库损坏或非法: {reason}"),
            Self::Db { reason } => write!(f, "local-buffer SQLite 操作失败: {reason}"),
            Self::Closed => write!(f, "local-buffer 已停机"),
            Self::WorkerFailed { reason } => {
                write!(f, "local-buffer 后台任务异常终止: {reason}")
            }
        }
    }
}

impl std::error::Error for LocalBufferError {}

impl From<rusqlite::Error> for LocalBufferError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Db {
            reason: e.to_string(),
        }
    }
}
