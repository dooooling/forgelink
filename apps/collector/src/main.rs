//! collector：设备侧轻量采集程序（占位）。
//!
//! Runtime Role = collector（§92、§93）：只采集、缓存、上传。
//! 通过 Cargo feature 禁用控制链路（§98），运行时设置 `read_only`（§106）。

use diagnostics::{LoggingConfig, init_logging, redact};
use tracing::info;

fn main() {
    // 日志基础设施最先初始化（§6）。初始化失败说明日志配置不合法，
    // 此时只能向标准错误输出脱敏后的错误并退出。
    if let Err(e) = init_logging(LoggingConfig::default()) {
        eprintln!("日志初始化失败: {}", redact(&e.to_string()));
        std::process::exit(1);
    }

    info!(
        component = "collector",
        role = "collector",
        version = env!("CARGO_PKG_VERSION"),
        "collector 启动"
    );

    // TODO: 组装 edge-core 组件，加载配置并启动采集

    info!(component = "collector", "collector 退出");
    // 优雅退出：关闭发送端并等待刷写线程排空，避免日志丢失（§6）。
    diagnostics::shutdown_logging();
}
