//! Local Buffer / WAL（§103）：以**完整 ObservationBatch** 为持久化
//! 单位的本地持久化缓冲（Embedded DB：SQLite）。
//!
//! # 两级缓冲
//!
//! ```text
//! Memory Queue（`memory_records` 条，发送窗口）→ 发送（next）
//!      ↓ 入队即落盘（内存窗口满时记录仅落盘，不阻塞）
//! SQLite（`disk_max_bytes` 字节，WAL 日志模式 + synchronous=FULL）
//! ```
//!
//! - 记录保存本地递增序号（`local_seq`），恢复后按原顺序补传（§31.4
//!   按本地持久化顺序）。
//! - `message_id` 唯一（§31.3 消息级去重键）：重复写入幂等，不覆盖
//!   原记录；背压等待中（尚未落盘）的重复请求共享最终落盘结果，
//!   不得提前返回持久化成功。
//! - **唯一删除路径**是 [`LocalBuffer::ack`]（broker PUBACK 后调用）；
//!   `Closed` / `Disconnected` / `CollisionOverwritten` 均不得删除——
//!   发送失败用 [`LocalBuffer::requeue`] 放回队头重试。
//! - 补传（`sent_count > 0`，或属于本次会话恢复的积压——会话内
//!   恢复水位标记，不修改数据库）置 `replayed = true`，原
//!   `message_id`、Observation ID 与时间保留。
//! - 恢复按内存窗口分页（内存持有 `mem + inflight` ≤ `memory_records`，
//!   随 `next` 消耗 / ACK 释放后按本地序号顺序补充），磁盘记录数
//!   不受内存容量限制也不会一次性全量载入（OOM 防护；在途积压
//!   期间不补页）。
//! - **磁盘是第二级容量**（评审 P1-1）：`memory_records` 只限制内存
//!   持有量，**不是容量**——内存窗口满时 push 仍直接落盘成功（Broker
//!   断网期间持续写入），记录由分页加载在空间释放后按本地序号进入
//!   内存。磁盘估算成本（`payload + topic + message_id + 固定开销`）
//!   是唯一硬上限，超限按 [`CapacityPolicy`] 显式背压或报错，
//!   **禁止静默覆盖未确认数据**；单条记录成本超过磁盘上限时任何
//!   策略都立即拒绝（背压等待也永远无法获得容量）；背压等待队列
//!   有界（上限 = `memory_records`），同一待落盘记录的等待者数量
//!   有界（上限 = 1024），超限均显式报错。
//! - 背压等待期间 worker 持续处理（清理过期 / 分页加载 / 入队），
//!   不阻塞等待新命令（评审 P2-1）——恢复的过期数据跨多个分页窗口
//!   时逐轮清理即可释放容量；磁盘满且无命令时按发送队列**最近的
//!   过期时刻**超时唤醒（`poll_recv` + `park_timeout` 手动驱动），
//!   到期自动清理释放容量，等待中的请求不会被永久阻塞。
//! - `next` 采用**交付确认握手**（评审 P1-1）：结果放入回复通道后
//!   worker 登记确认，调用方提取后确认；调用方在提取前取消（回复
//!   通道或确认通道关闭）时记录归还发送队列——不滞留 in-flight
//!   等待重启恢复。
//! - 分页加载失败（`load_error` 未清除）时，新写入的记录仅落盘
//!   不入内存（评审 P2-3）——`next` 返回错误而非越过无法加载的
//!   旧记录，不破坏 `local_seq` 补传顺序。
//! - Topic 校验与 mqtt-client 一致（§31.1）：控制字符（含 NUL）与
//!   超过 65535 字节的主题入队前拒绝（MQTT 必然拒绝的记录不得成为
//!   队头）。
//! - 保留时间到期仅清理**发送队列**中滞留的记录（显式丢弃并告警，
//!   不阻塞新数据）；**在途记录不清理**——等待 ACK / requeue（§31.4
//!   唯一删除路径是 ack）。清理不假设队头时间戳单调（评审 P2-5：
//!   SystemTime 回拨后后部记录可能更早过期，按全部记录过滤后按
//!   本地序号精确移除）；清理失败时按固定短退避重试（评审 P2-1：
//!   已过期记录会让超时计算为 0 → 空转烧 CPU，改用短退避不空转）。
//! - 背压等待的超时按发送队列最近的过期时刻计算（评审 P2-1）：
//!   差值钳制到非负（`saturating_sub` 只防溢出不钳负值，过期时刻
//!   早于当前——时钟回拨 / 清理完成后刚过期——时必须显式
//!   `max(0)` 钳为 0，立即唤醒重试），负数转 `u64` 会变成巨大时长
//!   导致超长休眠。
//! - 磁盘操作全部在专用阻塞线程完成（§103），通过有界异步通道调用，
//!   不阻塞 Tokio；支持有界优雅停机（[`LocalBuffer::shutdown`]）与
//!   异常重启恢复（SQLite WAL 崩溃安全，未 ACK 记录 0 丢失——集成
//!   测试以真实子进程强杀（`Child::kill()`，SIGKILL /
//!   TerminateProcess，见 `src/bin/wal-crash-helper.rs`）验证）。
//! - 有界优雅停机与异常退出统一语义（评审 P1-1）：worker 在任何
//!   退出路径前置位停机标志——停机后 / 句柄全部释放后，命令一律
//!   [`LocalBufferError::Closed`]；tokio 允许已取得 permit 的发送者在
//!   `Receiver::close()` 后完成发送，此类竞态窗口内被丢弃的命令
//!   回复通道关闭时按停机标志映射为 `Closed`，绝不误报 `WorkerFailed`。
//! - schema v1 完整校验（评审 P2-2/P2-3/P2-4）：表缺失、缺列、**列数
//!   不符**（额外列会让所有 INSERT 失败）、列类型、NOT NULL / 主键 /
//!   默认值、**恰好作用于 message_id 单列**的唯一索引（存在任意 UNIQUE
//!   约束不得通过）、**本表 DDL 含 AUTOINCREMENT**（全库存在
//!   sqlite_sequence 不代表本表使用，其他表使用也能伪造）——不符
//!   一律 [`LocalBufferError::Corrupt`]；`user_version = 0` 但表已存在
//!   同样 Corrupt（不跳过校验在旧表上建表）。

pub mod config;
pub mod metrics;

mod error;
mod worker;

pub use config::{CapacityPolicy, LocalBufferConfig};
pub use data_pipeline::ObservationBatch;
pub use error::{CapacityKind, LocalBufferError};
pub use metrics::WalMetrics;

use std::{
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
    time::UNIX_EPOCH,
};

use tokio::sync::{mpsc, oneshot};

use worker::Cmd;

/// 从 Local Buffer 取出的、待发送（或补传）的 Batch。
#[derive(Debug)]
pub struct StoredBatch {
    /// 本地递增序号（持久化 / 补传顺序，§31.4）。ACK 与 requeue
    /// 均以此标识记录。
    pub local_seq: i64,
    /// §31.1 Telemetry 主题（`forgelink/v1/telemetry/{site_id}/{device_id}`，
    /// 入队时派生并校验）。
    pub topic: String,
    /// Batch 深拷贝：首次取出保持原样；补传（曾取出或重启恢复）时
    /// `replayed = true`，`message_id` / Observation ID / 时间不变。
    pub batch: ObservationBatch,
}

/// Local Buffer 句柄（与专用阻塞 Worker 通过有界通道通信）。
///
/// 未调用 [`LocalBuffer::shutdown`] 就释放句柄时，worker 线程随通道
/// 关闭退出，已持久化的未确认记录保留（等价异常退出），重新
/// `open` 同一数据库后按原顺序恢复。
#[derive(Debug)]
pub struct LocalBuffer {
    tx: mpsc::Sender<Cmd>,
    #[allow(dead_code)] // 持有句柄保证 worker 线程存活；退出由其自行结束。
    handle: JoinHandle<()>,
    /// worker 停机 / 退出标志（worker 在退出前置位，评审 P1-1）：
    /// 竞态窗口内已入队但未处理的命令（tokio 允许已取得 permit 的
    /// 发送者在 `Receiver::close()` 后完成发送）回复通道最终关闭，
    /// 据此映射为 [`LocalBufferError::Closed`] 而非 `WorkerFailed`。
    closed: Arc<AtomicBool>,
}

/// 当前 UNIX 纳秒时间。系统时钟异常（早于 1970）时回退 0 纳秒，
/// 不 panic（评审 P2-2）。
pub(crate) fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

impl LocalBuffer {
    /// 打开（或恢复）Local Buffer：校验配置、启动专用 Worker、在
    /// Worker 线程内打开 SQLite（WAL + FULL）并**分页加载**未确认记录
    ///（先载前 `memory_records` 条，随 `next` 消耗 / ACK 释放按本地
    /// 序号顺序补充；恢复积压经会话内恢复水位标记为补传）。损坏的
    /// 数据库或非法 schema **明确报错**（[`LocalBufferError::Corrupt`]）。
    ///
    /// # Errors
    ///
    /// 配置非法（[`LocalBufferError::InvalidConfig`]，含数据库路径
    /// 打不开）、数据库损坏 / schema 非法（[`LocalBufferError::Corrupt`]）、
    /// 线程启动失败（[`LocalBufferError::WorkerFailed`]）。
    pub async fn open(config: LocalBufferConfig) -> Result<Self, LocalBufferError> {
        Self::open_inner(config, metrics::WalMetrics::new(None)).await
    }

    /// 打开（或恢复）Local Buffer 并注入指标注册表（§34.2.1）：在途
    /// gauge、补传计数与落盘耗时直方图经 `registry` 暴露。语义与
    /// [`Self::open`] 完全一致。
    pub async fn open_with_metrics(
        config: LocalBufferConfig,
        registry: Arc<::metrics::MetricsRegistry>,
    ) -> Result<Self, LocalBufferError> {
        Self::open_inner(config, metrics::WalMetrics::new(Some(&registry))).await
    }

    async fn open_inner(
        config: LocalBufferConfig,
        wal_metrics: metrics::WalMetrics,
    ) -> Result<Self, LocalBufferError> {
        let (tx, handle, ready, closed) = worker::spawn(config, wal_metrics)?;
        ready.await.map_err(|_| LocalBufferError::WorkerFailed {
            reason: "worker 线程在就绪前退出".into(),
        })??;
        Ok(Self { tx, handle, closed })
    }

    /// 持久化一个 Batch（以完整 Batch 为单位，§31.4）。
    ///
    /// 幂等：同 `message_id` 已存在（在途 / 等待 / 磁盘）时直接成功，
    /// **不覆盖**原记录（覆盖会破坏其本地序号与补传顺序）；背压
    /// 等待中（尚未落盘）的重复请求与首个请求共享最终落盘结果。
    ///
    /// 内存窗口（`memory_records`）满不影响 push：记录直接落盘
    ///（磁盘是第二级容量，评审 P1-1），由分页加载在空间释放后
    /// 进入发送队列。磁盘估算成本超限时按 [`CapacityPolicy`]：
    /// `Reject` 立即返回 [`LocalBufferError::CapacityExceeded`]；
    /// `Backpressure` 等待直到空间释放（ACK 删除）或停机——但背压
    /// 等待队列有界（上限 = `memory_records`），超限时同样显式报错。
    ///
    /// # Errors
    ///
    /// [`LocalBufferError::InvalidBatch`]（`site_id` / `device_id` 非法
    /// 或序列化失败）、[`LocalBufferError::CapacityExceeded`]、
    /// [`LocalBufferError::Closed`]（已停机）、
    /// [`LocalBufferError::Db`]（磁盘错误）。
    pub async fn push(&self, batch: ObservationBatch) -> Result<(), LocalBufferError> {
        self.command(|reply| Cmd::Push { batch, reply }).await
    }

    /// 取最早未发送记录（队头 = 本地序号最小 = §31.4 补传顺序）。
    /// 取出后该记录进入"在途"状态：只有 [`LocalBuffer::ack`] 或
    /// [`LocalBuffer::requeue`] 能结束它；重启后仍在磁盘上（未 ACK
    /// 数据不丢失）。
    ///
    /// 返回 `None` 表示队列为空（轮询语义；配合重连退避轮询）。
    /// 本 future 在结果返回前被取消时，记录归还发送队列（评审
    /// P1-1/P1-2）——无论结果是否已放入回复通道，都不会滞留
    /// in-flight 等待重启恢复。
    ///
    /// # Errors
    ///
    /// 已停机（[`LocalBufferError::Closed`]）、磁盘错误
    /// （[`LocalBufferError::Db`]）。
    pub async fn next(&self) -> Result<Option<StoredBatch>, LocalBufferError> {
        // 交付确认（评审 P1-1）：worker 把结果放入回复通道后登记
        // 确认；本函数提取结果后 send 确认。若 future 在提取前被
        // 取消（回复通道关闭或确认通道关闭），worker 归还记录。
        let (deliver_tx, deliver_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Next {
                reply: reply_tx,
                delivered: deliver_rx,
            })
            .await
            .map_err(|_| LocalBufferError::Closed)?;
        let result = reply_rx.await.map_err(|_| self.map_reply_err())?;
        // 提取完成：确认交付（同步执行，紧接提取，不会被调度打断）。
        let _ = deliver_tx.send(());
        result
    }

    /// 确认已送达（broker PUBACK 后）：删除对应记录——**唯一**删除
    /// 路径（§31.4）。幂等：记录不存在（已删 / 已过期清理）时成功。
    ///
    /// # Errors
    ///
    /// 已停机（[`LocalBufferError::Closed`]）、磁盘错误
    /// （[`LocalBufferError::Db`]）。
    pub async fn ack(&self, local_seq: i64) -> Result<(), LocalBufferError> {
        self.command(|reply| Cmd::Ack { local_seq, reply }).await
    }

    /// 发送失败（未 ACK——`Closed` / `Disconnected` /
    /// `CollisionOverwritten` 等，WAL 记录**不得删除**）：把在途记录
    /// 放回队头，下次 [`LocalBuffer::next`] 优先重试（补传时
    /// `replayed = true`）。
    ///
    /// # Errors
    ///
    /// 已停机（[`LocalBufferError::Closed`]）。
    pub async fn requeue(&self, local_seq: i64) -> Result<(), LocalBufferError> {
        self.command(|reply| Cmd::Requeue { local_seq, reply })
            .await
    }

    /// 有界优雅停机：显式拒绝等待入队的背压请求（回复
    /// [`LocalBufferError::Closed`]），已入队 / 在途的未确认记录保留
    /// 在 SQLite（重启后恢复），关闭数据库。worker 线程在收到本命令
    /// 后立即退出（线程句柄随 `LocalBuffer` 释放，无泄漏）；停机后
    /// 所有命令返回 [`LocalBufferError::Closed`]。
    ///
    /// 停机返回成功后不再接受任何命令（评审 P1-1）：worker 原子关闭
    /// 接收端（此后入队的 `send` 立即失败）并显式拒绝停机时仍在
    /// 通道中的命令；tokio 允许**已取得 permit** 的发送者在 `close()`
    /// 后完成发送，此类命令最终随接收端销毁被丢弃——调用方的回复
    /// 通道关闭时按 worker 的 `closed` 标志映射为 `Closed`，不会出现
    /// 命令入队后无人处理、最终返回 `WorkerFailed` 的情况。
    ///
    /// # Errors
    ///
    /// 已停机或 worker 已退出（[`LocalBufferError::Closed`]）。
    pub async fn shutdown(&self) -> Result<(), LocalBufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Shutdown { reply: reply_tx })
            .await
            .map_err(|_| LocalBufferError::Closed)?;
        reply_rx.await.map_err(|_| LocalBufferError::Closed)?
    }

    /// 发送命令并等待回复（有界：通道有界 + oneshot 有界）。
    /// 回复通道关闭 = 命令未被处理（worker 已退出）：若 worker 已
    /// 置位 `closed`（停机 / 句柄释放），映射为
    /// [`LocalBufferError::Closed`]；否则视为异常终止（`WorkerFailed`）。
    async fn command<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, LocalBufferError>>) -> Cmd,
    ) -> Result<T, LocalBufferError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make(reply_tx))
            .await
            .map_err(|_| LocalBufferError::Closed)?;
        reply_rx.await.map_err(|_| self.map_reply_err())?
    }

    /// 把"回复通道在 worker 退出时关闭"映射为合适的错误（评审 P1-1）：
    /// 停机 / 句柄释放后一律 [`LocalBufferError::Closed`]（含 tokio
    /// 已取得 permit 的发送者在 `close()` 后完成发送的竞态窗口——
    /// 消息随接收端销毁，回复通道关闭时 `closed` 已置位）；未置位
    /// 才视为 worker 异常终止（`WorkerFailed`）。
    fn map_reply_err(&self) -> LocalBufferError {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            LocalBufferError::Closed
        } else {
            LocalBufferError::WorkerFailed {
                reason: "worker 线程已退出（异常终止）".into(),
            }
        }
    }
}
