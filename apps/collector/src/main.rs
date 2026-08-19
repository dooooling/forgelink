//! collector：设备侧轻量采集程序（§92/§93 Collector Agent）。
//!
//! Runtime Role = collector：只采集、缓存、上传；通过 Cargo feature
//! 禁用控制链路（§98）。构建为不含控制代码的只读版本时，入口仅打印
//! 说明并退出。
//!
//! 命令行参数：`collector [CONFIG_PATH]`（缺省 `collector.yaml`，
//! §101 Standalone YAML）。

use std::process::ExitCode;

// 仅在启用采集功能时引入依赖 crate 的符号：`--no-default-features`
// 构建为只读占位，tokio/diagnostics/tracing 等依赖随 feature 一起
// 被禁用，未门控的 import 会导致编译失败（评审 P2）。
#[cfg(feature = "collector")]
use std::path::Path;

#[cfg(feature = "collector")]
use collector::CollectorRuntime;
#[cfg(feature = "collector")]
use collector::config::CollectorConfig;
#[cfg(feature = "collector")]
use diagnostics::{LoggingConfig, init_logging, redact, shutdown_logging};
#[cfg(feature = "collector")]
use tokio::sync::watch;
#[cfg(feature = "collector")]
use tracing::{error, info};

fn main() -> ExitCode {
    #[cfg(feature = "collector")]
    {
        let path = std::env::args()
            .nth(1)
            .unwrap_or_else(|| "collector.yaml".to_owned());

        // 日志基础设施最先初始化（§6）。初始化失败说明日志配置不合法，
        // 此时只能向标准错误输出脱敏后的错误并退出。
        if let Err(e) = init_logging(LoggingConfig::default()) {
            eprintln!("日志初始化失败: {}", redact(&e.to_string()));
            return ExitCode::FAILURE;
        }

        let config = match CollectorConfig::load_path(Path::new(&path)) {
            Ok(c) => c,
            Err(e) => {
                error!(component = "collector", path = %path, error = %e, "配置加载失败");
                shutdown_logging();
                return ExitCode::FAILURE;
            }
        };

        let rt = tokio::runtime::Runtime::new();
        match rt {
            Ok(rt) => rt.block_on(async {
                // 停机信号通道与 SIGTERM 监听必须创建在 Tokio 运行时内：
                // tokio::signal 与 tokio::spawn 依赖运行时上下文，在
                // Runtime::new() 之前调用会 panic（评审 P1）。
                let (sig_tx, sig_rx) = watch::channel(false);

                // 系统信号：Ctrl+C（SIGINT）。Windows 不支持 SIGTERM；
                // UNIX 下补充 SIGTERM（docker stop 默认信号）。
                #[cfg(unix)]
                {
                    let tx = sig_tx.clone();
                    let mut sigterm = match tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::terminate(),
                    ) {
                        Ok(s) => s,
                        Err(e) => {
                            error!(component = "collector", error = %e, "SIGTERM 监听初始化失败");
                            shutdown_logging();
                            return ExitCode::FAILURE;
                        }
                    };
                    tokio::spawn(async move {
                        sigterm.recv().await;
                        info!(component = "collector", "收到 SIGTERM");
                        tx.send(true).ok();
                    });
                }

                // 启动运行时（配置 → Driver/Profile → 设备 → 管道 → 缓冲
                // → MQTT，§100 启动顺序）。启动失败不做部分资源清理。
                let runtime = match CollectorRuntime::start(config).await {
                    Ok(r) => r,
                    Err(e) => {
                        error!(component = "collector", error = %e, "Collector 启动失败");
                        return ExitCode::FAILURE;
                    }
                };

                // 等待任一停机信号：SIGINT（ctrl_c）或 SIGTERM 任务置位
                // 的 watch。此前仅等待 ctrl_c，SIGTERM 置位 watch 后 main
                // 仍阻塞在 ctrl_c，不会触发停机（评审 P1）。
                let mut signal_wait = sig_rx.clone();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        info!(component = "collector", "收到 SIGINT（Ctrl+C）");
                    }
                    r = signal_wait.changed() => {
                        if r.is_err() {
                            info!(component = "collector", "停机信号通道关闭，按停机处理");
                        }
                    }
                }
                sig_tx.send(true).ok();
                let code = match runtime.run_until_shutdown(sig_rx).await {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => {
                        error!(component = "collector", error = %e, "停机失败");
                        ExitCode::FAILURE
                    }
                };
                shutdown_logging();
                code
            }),
            Err(e) => {
                error!(component = "collector", error = %e, "Tokio 运行时创建失败");
                shutdown_logging();
                ExitCode::FAILURE
            }
        }
    }

    #[cfg(not(feature = "collector"))]
    {
        eprintln!(
            "collector 构建未启用采集功能（缺省 features 或 --no-default-features 构建为只读占位）"
        );
        ExitCode::FAILURE
    }
}
