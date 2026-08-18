//! Local Buffer 配置（§103）。
//!
//! 两级缓冲：
//!
//! ```text
//! Memory Queue（`memory_records` 条，发送窗口——非容量）
//!      ↓ 入队即落盘（窗口满时仅落盘，不阻塞）
//! SQLite（`disk_max_bytes` 字节，唯一硬上限）
//! ```
//!
//! 容量不足时按 [`CapacityPolicy`] 显式背压（等待空间释放后继续）或
//! 拒绝（返回 [`LocalBufferError::CapacityExceeded`]）——禁止静默覆盖
//! 未确认数据。

use std::{path::PathBuf, time::Duration};

use crate::error::LocalBufferError;

/// 容量不足时的处理策略（§103）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityPolicy {
    /// 背压：push 等待，直到已有记录被 ACK 删除（或停机）腾出空间。
    /// 与 mqtt-client 的有界通道背压一致（§34.2），沿调用链向上传导。
    Backpressure,
    /// 拒绝：立即返回 [`LocalBufferError::CapacityExceeded`]，调用方
    /// 自行决定重试或丢弃（不允许静默覆盖，错误必须显式）。
    Reject,
}

/// Local Buffer 配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBufferConfig {
    /// SQLite 数据库文件路径（Embedded DB，§103）。父目录不存在时
    /// `open` 报错，不自动创建（显式配置，避免静默落在错误位置）。
    pub db_path: PathBuf,
    /// 内存队列（等待发送）+ 在途（已取出未 ACK）记录数**持有上限**
    ///（评审 P1-1：内存窗口，不是容量）——窗口满时 push 仍直接落盘
    ///（磁盘是第二级容量），记录由分页加载在空间释放后按本地序号
    /// 进入内存；上限同时约束背压等待队列长度。
    pub memory_records: usize,
    /// 磁盘上未确认记录的总字节上限（按**估算成本**计：payload +
    /// topic + message_id + 每条固定开销 512 字节，覆盖 SQLite B-tree
    /// 页 / UNIQUE 索引 / 页头；WAL 与主库同源不重复计，评审 P3）。
    /// 达到上限后 push 按 [`CapacityPolicy`] 处理；单条记录成本超过
    /// 上限时任何策略都立即报错（背压等待也永远无法获得容量）。
    pub disk_max_bytes: u64,
    /// 未确认记录的保留时间：到期未 ACK 的记录在后续 push 时
    /// 显式清理（告警日志，计入"过期丢弃"），不阻塞新数据。
    pub retention: Duration,
    /// 容量不足时的策略。
    pub capacity_policy: CapacityPolicy,
}

impl LocalBufferConfig {
    /// 校验配置合法性（§103 需要定义磁盘上限 / 保留时间 / 内存容量 /
    /// 背压策略；非法配置必须**明确报错**，不静默取默认值）。
    pub fn validate(&self) -> Result<(), LocalBufferError> {
        if self.db_path.as_os_str().is_empty() {
            return Err(LocalBufferError::InvalidConfig {
                field: "db_path",
                reason: "数据库文件路径不能为空".into(),
            });
        }
        if self.memory_records == 0 {
            return Err(LocalBufferError::InvalidConfig {
                field: "memory_records",
                reason: "内存队列容量必须大于 0".into(),
            });
        }
        if self.disk_max_bytes == 0 {
            return Err(LocalBufferError::InvalidConfig {
                field: "disk_max_bytes",
                reason: "磁盘容量上限必须大于 0".into(),
            });
        }
        if self.retention.is_zero() {
            return Err(LocalBufferError::InvalidConfig {
                field: "retention",
                reason: "保留时间必须大于 0".into(),
            });
        }
        if self.retention.as_nanos() > i64::MAX as u128 {
            // 评审 P2-2：as_nanos 转 i64 会截断，超大配置导致减法
            // 溢出 / 错误清理（i64::MAX 纳秒 ≈ 292 年）。
            return Err(LocalBufferError::InvalidConfig {
                field: "retention",
                reason: "保留时间超过 i64 纳秒范围（≈292 年）".into(),
            });
        }
        Ok(())
    }
}
