//! collector：设备侧轻量采集程序（§92/§93 Collector Agent）。
//!
//! Runtime Role = collector：只采集、缓存、上传；通过 Cargo feature
//! 禁用控制链路（§98）。构建为不含控制代码的只读版本时，入口仅打印
//! 说明并退出。
//!
//! 命令行参数：`collector [CONFIG_PATH]`（缺省 `collector.yaml`，
//! §101 Standalone YAML）。

use std::path::Path;
use std::process::ExitCode;

use collector::CollectorRuntime;
use collector::config::CollectorConfig;
use diagnostics::{LoggingConfig, init_logging, redact, shutdown_logging};
use tokio::sync::watch;
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

        let (sig_tx, sig_rx) = watch::channel(false);

        // 系统信号：Ctrl+C（SIGINT）。Windows 不支持 SIGTERM；UNIX 下
        // 补充 SIGTERM（docker stop 默认信号）。
        #[cfg(unix)]
        {
            let tx = sig_tx.clone();
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
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

        let rt = tokio::runtime::Runtime::new();
        match rt {
            Ok(rt) => rt.block_on(async {
                // 启动运行时（配置 → Driver/Profile → 设备 → 管道 → 缓冲
                // → MQTT，§100 启动顺序）。启动失败不做部分资源清理。
                let runtime = match CollectorRuntime::start(config).await {
                    Ok(r) => r,
                    Err(e) => {
                        error!(component = "collector", error = %e, "Collector 启动失败");
                        return ExitCode::FAILURE;
                    }
                };

                // 等待任一停机信号（SIGINT / SIGTERM 任务置位 watch），
                // 随后交给运行时有序停机。
                tokio::signal::ctrl_c().await.ok();
                info!(component = "collector", "收到 SIGINT（Ctrl+C）");
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
