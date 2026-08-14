//! data-pipeline（§31.2 Telemetry Batch Envelope）。
//!
//! # 职责边界
//!
//! - 输入只能是已经归一化的 [`Observation`](observation_model::Observation)
//!   （由 Profile + Domain 映射生成，见 `domain-model`/`device-manager`），
//!   本 crate **不依赖** `poll-engine`、`device-manager`、Driver 或 Profile。
//! - 按 `device_id` 分批，**禁止跨设备混批**；一个 [`ObservationBatch`]
//!   只属于一个 `device_id`，跨设备可并行输出。
//! - 保留 `Observation` 原有 `sequence`，**不重新编号**；`sequence` 字段
//!   （[`ObservationBatch::sequence`]）是独立批次序号（§31.2 更新后），
//!   与 Observation `sequence` 正交。
//! - 输出稳定的 Telemetry Batch（§31.2），`message_id` 在组包时生成，
//!   采用**长度前缀无歧义编码**（§47 同风格）：
//!   `{session_len}:{collector_session_id}{device_len}:{device_id}{sequence}`，
//!   并嵌入 [`PipelineConfig::collector_session_id`]（§31.3 消息级去重键）。
//!
//! # 背压 / 取消 / 有界排空
//!
//! - 输入与输出均为有界异步通道，全链路背压（§22 有界并发/背压）；
//!   内部缓存（partial 批次 + 在途 Batch）设有统一容量上限，多设备场景
//!   也不会无限累积。
//! - 输出接收端永久关闭时管道**立即终止**：`ingest()` 返回
//!   [`PipelineError::Closed`]，剩余数据结算计入丢弃统计（不静默丢失）。
//! - 丢弃管道句柄即**取消**：任务立即停止接收，不排空剩余数据。
//! - 调用 [`Pipeline::shutdown`] 即**优雅停机**：在
//!   [`PipelineConfig::drain_timeout`] 时限内排空剩余 partial 批次后结束；
//!   输出队列满时可被停机信号中断（有界排空，不永久卡住）。

mod batch;
mod config;
mod pipeline;

pub use batch::{ObservationBatch, TELEMETRY_SCHEMA};
pub use config::PipelineConfig;
pub use pipeline::{DrainStats, Pipeline, PipelineError};
