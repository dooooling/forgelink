//! Poll Engine：周期轮询调度引擎（§22 Poll Scheduler、§34.3 Timeout/Reconnect）。
//!
//! # 职责
//!
//! - 按设备/采集周期调度批量读取（§22：Core 按周期分组成 Group，到期调用 `driver.read_group`）；
//! - 每设备一个异步任务，设备之间互不阻塞（§34.3：单设备故障不得阻塞其他设备）；
//! - 同步驱动调用通过 `spawn_blocking` 隔离，单次请求有超时上限；
//! - 失败按指数退避重试（§34.3 默认 `1s → 2s → 4s … 上限 30s`），成功后退避重置；
//! - 通过取消令牌（[`tokio_util::sync::CancellationToken`]）统一停机；
//! - `RawReadResult` 的错误与质量信息原样保留，供下游 Profile + Domain 映射。
//!
//! # 分层
//!
//! 上层（Device Manager）将设备按采集周期拆分为多个 [`PollTarget`]（对应 §22 的
//! Group），每个目标通过 [`PollScheduler::spawn`] 启动一个轮询任务；多个 Group 共享
//! 同一驱动实例时，用 `Arc<Mutex<Box<dyn PollDriver>>>` 串行化驱动访问。
//!
//! 下游（Profile + Domain 映射）通过有界 `mpsc` 通道订阅 [`PollEvent`]。

pub mod config;
pub mod driver;
pub mod events;
pub mod metrics;
pub mod poll;
pub mod scheduler;

pub use config::{PollConfig, PollConfigError};
pub use driver::{NativeDriverAdapter, PollDriver};
pub use events::{PollBatch, PollEvent};
pub use poll::{PollTarget, poll_loop};
pub use scheduler::PollScheduler;
