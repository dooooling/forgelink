//! 统一日志初始化：配置解析、subscriber 组装与全局安装（幂等）。

use std::fmt;
use std::sync::OnceLock;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt as ts_fmt;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::registry::Registry;

use crate::writer::NonBlockingWriter;

/// 日志输出格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// 人类可读文本（默认）。
    Text,
    /// 结构化 JSON，每行一个事件。
    Json,
}

/// 日志初始化配置。
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// 默认级别：`RUST_LOG` 未设置时生效。默认 `info`；`trace` 仅用于
    /// 低层诊断，默认关闭。
    pub default_level: LevelFilter,
    /// 输出格式；可被环境变量 `FORGELINK_LOG_FORMAT`（`text`/`json`）覆盖。
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            default_level: LevelFilter::INFO,
            format: LogFormat::Text,
        }
    }
}

/// 日志初始化结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitOutcome {
    /// 本次调用完成初始化。
    Initialized,
    /// 日志已由先前调用初始化（配置以首次调用为准）。
    AlreadyInitialized,
}

/// 日志初始化失败。
#[derive(Debug)]
pub enum LoggingError {
    /// `RUST_LOG` 过滤表达式非法。
    InvalidFilter {
        expression: String,
        source: tracing_subscriber::filter::ParseError,
    },
    /// `FORGELINK_LOG_FORMAT` 取值非法（仅允许 `text`/`json`）。
    InvalidFormat { value: String },
    /// 非阻塞 writer 线程启动失败。
    Writer(std::io::Error),
    /// 全局默认 subscriber 已被其他代码安装。
    SubscriberAlreadySet,
}

impl fmt::Display for LoggingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoggingError::InvalidFilter { expression, source } => {
                write!(f, "RUST_LOG 过滤表达式 `{expression}` 非法: {source}")
            }
            LoggingError::InvalidFormat { value } => write!(
                f,
                "FORGELINK_LOG_FORMAT 取值 `{value}` 非法（仅允许 text/json）"
            ),
            LoggingError::Writer(e) => write!(f, "日志 writer 线程启动失败: {e}"),
            LoggingError::SubscriberAlreadySet => {
                write!(f, "全局日志 subscriber 已被其他代码安装")
            }
        }
    }
}

impl std::error::Error for LoggingError {}

/// 初始化全局结构化日志（幂等，见 crate 文档）。
///
/// # Errors
///
/// `RUST_LOG` 非法、`FORGELINK_LOG_FORMAT` 非法、writer 线程启动失败
/// 时返回 [`LoggingError`]；失败后允许重试（本次调用不会安装任何状态）。
pub fn init_logging(config: LoggingConfig) -> Result<InitOutcome, LoggingError> {
    static STATE: OnceLock<InitOutcome> = OnceLock::new();
    if STATE.get().is_some() {
        return Ok(InitOutcome::AlreadyInitialized);
    }
    let outcome = match install(config) {
        Ok(()) => InitOutcome::Initialized,
        // 并发竞态：另一线程已先完成全局安装，按幂等返回。
        Err(LoggingError::SubscriberAlreadySet) => InitOutcome::AlreadyInitialized,
        Err(e) => return Err(e),
    };
    let _ = STATE.set(outcome);
    Ok(outcome)
}

/// 解析级别过滤：优先 `RUST_LOG`，未设置时回退默认级别。
fn filter_from_env_or(default: LevelFilter) -> Result<EnvFilter, LoggingError> {
    match std::env::var("RUST_LOG") {
        Ok(expr) if !expr.trim().is_empty() => {
            EnvFilter::try_new(expr.clone()).map_err(|source| LoggingError::InvalidFilter {
                expression: expr,
                source,
            })
        }
        _ => Ok(EnvFilter::new(default.to_string())),
    }
}

/// 解析输出格式：环境变量 `FORGELINK_LOG_FORMAT` 优先，非法取值报错。
fn format_from_env_or(requested: LogFormat) -> Result<LogFormat, LoggingError> {
    match std::env::var("FORGELINK_LOG_FORMAT") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "text" => Ok(LogFormat::Text),
            "json" => Ok(LogFormat::Json),
            _ => Err(LoggingError::InvalidFormat { value }),
        },
        Err(_) => Ok(requested),
    }
}

/// 组装并安装全局 subscriber（Registry + EnvFilter 过滤 + text/json fmt
/// Layer）；事件先入有界通道，由专用线程刷 stdout。
fn install(config: LoggingConfig) -> Result<(), LoggingError> {
    let filter = filter_from_env_or(config.default_level)?;
    let format = format_from_env_or(config.format)?;
    let writer = NonBlockingWriter::spawn().map_err(LoggingError::Writer)?;
    let subscriber = build_subscriber(filter, format, writer);
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| LoggingError::SubscriberAlreadySet)
}

/// 构建 subscriber（供全局安装与测试捕获共用）。
///
/// 过滤使用 [`EnvFilter`]（`RUST_LOG` 语义）；fmt Layer 关闭 ANSI 颜色，
/// 保证输出（含测试捕获）为纯文本/纯 JSON。
pub(crate) fn build_subscriber<W>(
    filter: EnvFilter,
    format: LogFormat,
    writer: W,
) -> Box<dyn tracing::Subscriber + Send + Sync>
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let layer: Box<dyn tracing_subscriber::layer::Layer<Registry> + Send + Sync> = match format {
        LogFormat::Text => Box::new(
            ts_fmt::layer()
                .with_writer(writer)
                .with_ansi(false)
                .with_filter(filter),
        ),
        LogFormat::Json => Box::new(
            ts_fmt::layer()
                .json()
                .with_writer(writer)
                .with_ansi(false)
                .with_filter(filter),
        ),
    };
    Box::new(Registry::default().with(layer))
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::writer::NonBlockingWriter;

    /// 测试捕获 writer：事件写入内存缓冲，断言用。
    #[derive(Clone)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("测试锁").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// 修改环境变量的测试串行执行，避免并行测试间的环境竞争。
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn capture() -> (CaptureWriter, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        (CaptureWriter(buf.clone()), buf)
    }

    fn capture_text(
        filter: EnvFilter,
    ) -> (
        Box<dyn tracing::Subscriber + Send + Sync>,
        Arc<Mutex<Vec<u8>>>,
    ) {
        let (writer, buf) = capture();
        (build_subscriber(filter, LogFormat::Text, writer), buf)
    }

    fn capture_json(
        filter: EnvFilter,
    ) -> (
        Box<dyn tracing::Subscriber + Send + Sync>,
        Arc<Mutex<Vec<u8>>>,
    ) {
        let (writer, buf) = capture();
        (build_subscriber(filter, LogFormat::Json, writer), buf)
    }

    #[test]
    fn default_level_filters_debug() {
        // 默认级别过滤：未设置 RUST_LOG 时按 default_level（info），
        // debug 事件不产生输出。
        let (subscriber, buf) = capture_text(EnvFilter::new("info"));
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("不应出现");
            tracing::info!("应出现");
        });
        let out = String::from_utf8(buf.lock().expect("测试锁").clone()).unwrap();
        assert!(out.contains("应出现"));
        assert!(!out.contains("不应出现"));
    }

    #[test]
    fn rust_log_env_overrides_default_level() {
        // RUST_LOG=debug 覆盖默认 info。
        let _guard = ENV_MUTEX.lock().expect("测试锁");
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        let filter = filter_from_env_or(LevelFilter::INFO).expect("合法表达式应解析");
        unsafe { std::env::remove_var("RUST_LOG") };
        drop(_guard);

        let (subscriber, buf) = capture_text(filter);
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("debug 应出现");
        });
        let out = String::from_utf8(buf.lock().expect("测试锁").clone()).unwrap();
        assert!(out.contains("debug 应出现"));
    }

    #[test]
    fn invalid_filter_expression_rejected() {
        let _guard = ENV_MUTEX.lock().expect("测试锁");
        unsafe { std::env::set_var("RUST_LOG", "trace=banana") };
        let e = filter_from_env_or(LevelFilter::INFO).expect_err("非法表达式应报错");
        unsafe { std::env::remove_var("RUST_LOG") };
        drop(_guard);
        assert!(matches!(e, LoggingError::InvalidFilter { .. }));
    }

    #[test]
    fn invalid_format_env_rejected() {
        let _guard = ENV_MUTEX.lock().expect("测试锁");
        unsafe { std::env::set_var("FORGELINK_LOG_FORMAT", "yaml") };
        let e = format_from_env_or(LogFormat::Text).expect_err("非法格式应报错");
        unsafe { std::env::remove_var("FORGELINK_LOG_FORMAT") };
        drop(_guard);
        assert!(matches!(e, LoggingError::InvalidFormat { value } if value == "yaml"));
    }

    #[test]
    fn format_env_switches_to_json() {
        let _guard = ENV_MUTEX.lock().expect("测试锁");
        unsafe { std::env::set_var("FORGELINK_LOG_FORMAT", "json") };
        let format = format_from_env_or(LogFormat::Text).expect("json 应合法");
        unsafe { std::env::remove_var("FORGELINK_LOG_FORMAT") };
        drop(_guard);
        assert_eq!(format, LogFormat::Json);
    }

    #[test]
    fn text_format_output() {
        let (subscriber, buf) = capture_text(EnvFilter::new("info"));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(component = "diagnostics-test", "text 格式事件");
        });
        let out = String::from_utf8(buf.lock().expect("测试锁").clone()).unwrap();
        assert!(out.contains("text 格式事件"), "输出: {out}");
        assert!(
            out.contains("component=\"diagnostics-test\""),
            "输出: {out}"
        );
    }

    #[test]
    fn json_format_output() {
        let (subscriber, buf) = capture_json(EnvFilter::new("info"));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(component = "diagnostics-test", "json 格式事件");
        });
        let out = String::from_utf8(buf.lock().expect("测试锁").clone()).unwrap();
        let line = out.lines().next().expect("应有输出行");
        let value: serde_json::Value = serde_json::from_str(line).expect("应为合法 JSON");
        assert_eq!(value["level"], "INFO");
        assert_eq!(value["fields"]["component"], "diagnostics-test");
        assert_eq!(value["fields"]["message"], "json 格式事件");
    }

    #[test]
    fn repeated_init_is_idempotent() {
        // 保证环境干净，避免与其他环境测试相互影响。
        let _guard = ENV_MUTEX.lock().expect("测试锁");
        unsafe { std::env::remove_var("RUST_LOG") };
        unsafe { std::env::remove_var("FORGELINK_LOG_FORMAT") };
        let first = init_logging(LoggingConfig::default()).expect("首次初始化应成功");
        assert_eq!(first, InitOutcome::Initialized);
        let second = init_logging(LoggingConfig::default()).expect("重复初始化应成功返回");
        assert_eq!(second, InitOutcome::AlreadyInitialized);
        // 不同配置同样以首次为准。
        let third = init_logging(LoggingConfig {
            default_level: LevelFilter::DEBUG,
            format: LogFormat::Json,
        })
        .expect("重复初始化应成功返回");
        assert_eq!(third, InitOutcome::AlreadyInitialized);
    }

    #[test]
    fn non_blocking_writer_enqueues_line() {
        let (tx, rx) = mpsc::sync_channel::<Box<[u8]>>(1);
        let mut writer = NonBlockingWriter::from_channel(Arc::new(tx));
        writer.write_all(b"hello\n").expect("写入不应失败");
        let line = rx.recv().expect("应收到事件行");
        assert_eq!(&*line, b"hello\n");
    }

    #[test]
    fn non_blocking_writer_drops_when_full_without_blocking() {
        let (tx, rx) = mpsc::sync_channel::<Box<[u8]>>(1);
        let mut writer = NonBlockingWriter::from_channel(Arc::new(tx));
        writer.write_all(b"first\n").expect("写入不应失败");
        writer
            .write_all(b"second\n")
            .expect("通道满时写入不应失败/阻塞");
        // 通道已满：second 被丢弃，first 仍可取出。
        let line = rx.recv().expect("应收到首行");
        assert_eq!(&*line, b"first\n");
    }
}
