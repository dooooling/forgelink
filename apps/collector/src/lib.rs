//! Collector 应用库（§93 Collector Agent）。
//!
//! `collector` feature（默认）：只读采集链路——配置加载 → Driver/Profile
//! → Device Manager → Poll Engine → Data Pipeline → Local Buffer/WAL →
//! MQTT 输出（§100 启动顺序）。`control` feature 保持为空占位（§98：
//! 接入控制链路后验证 `--no-default-features --features collector`
//! 构建产物不含控制代码）。

#![cfg(feature = "collector")]

pub mod config;
pub mod error;
pub mod health;
mod runtime;
mod tasks;

pub use runtime::CollectorRuntime;

// 组件配置类型 re-export：Collector 配置文件是这些组件的唯一入口，
// 外部（REST API 等）只经 Collector 配置访问，不直接依赖组件 crate。
pub use data_pipeline::PipelineConfig;
pub use local_buffer::{CapacityPolicy, LocalBufferConfig};
pub use mqtt_client::{MqttClientConfig, TlsMode};
pub use observation_model::DomainKind;
pub use poll_engine::PollConfig;

/// 单条 MQTT 发布的最小报文尺寸下限（§31.1 主题 + Envelope 头）。
pub const MIN_PACKET_SIZE: usize = 1024;

/// 当前时间纳秒（i64，单调可靠源；与观察模型时间戳约定一致）。
pub fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default()
}
