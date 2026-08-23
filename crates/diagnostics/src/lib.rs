//! 结构化日志基础设施（统一初始化入口，`开发规范.md` §6）。
//!
//! 所有进程入口（`apps/*`）在启动时**最先**调用 [`init_logging`]，随后
//! 业务 crate 直接依赖 `tracing` 输出事件，不自行初始化 subscriber。
//!
//! # 行为约定
//!
//! - **级别**：由环境变量 `RUST_LOG` 控制（标准 `tracing` 过滤语法）；
//!   未设置时使用 [`LoggingConfig::default_level`]（默认 `info`，
//!   `trace` 默认关闭）。非法过滤表达式返回
//!   [`LoggingError::InvalidFilter`]，进程应输出明确错误并退出。
//! - **格式**：由 [`LoggingConfig::format`] 指定（text/json），可被环境
//!   变量 `FORGELINK_LOG_FORMAT`（`text`/`json`）覆盖；非法取值返回
//!   [`LoggingError::InvalidFormat`]。
//! - **幂等**：重复调用返回 [`InitOutcome::AlreadyInitialized`]，不 panic，
//!   配置以首次调用为准；初始化失败后可重试。若全局 subscriber 已由
//!   其他代码安装（如第三方库自行初始化日志），返回
//!   [`LoggingError::SubscriberAlreadySet`]——进程应终止或改用外部配置，
//!   不得静默接受。
//! - **非阻塞**：事件写入有界通道（8192 行），由专用线程刷到 stdout；
//!   通道满时丢弃新行——日志丢帧优先于阻塞采集路径（§5 异步与并发）。
//!   丢弃行数经 [`dropped_log_count`] 自诊断暴露（§34.2.1 log pipeline
//!   健康度）。进程入口在优雅退出前必须调用 [`shutdown_logging`]
//!   关闭发送端并等待刷写线程排空，否则退出时未刷出的日志会丢失。
//! - **脱敏**：错误链可能包含凭据（带密码的连接串等）时，记录前必须经
//!   [`redact`] 掩盖；脱敏不能替代"不记录敏感字段"的纪律。
//!
//! 日志级别与字段规范见 `开发规范.md` §6。

mod init;
mod redact;
mod writer;

pub use init::{
    InitOutcome, LogFormat, LoggingConfig, LoggingError, init_logging, shutdown_logging,
};
pub use redact::redact;
pub use tracing;

pub use writer::dropped_log_count;
