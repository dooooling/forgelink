//! 外部已安装 subscriber 的场景（独立测试进程）。
//!
//! 其他代码（如第三方库）先安装全局 subscriber 时，`init_logging` 必须
//! 返回 [`LoggingError::SubscriberAlreadySet`] 而非静默返回
//! `AlreadyInitialized`——否则日志格式与过滤配置可能不符合 ForgeLink 规范。
//!
//! 本场景依赖"进程内全局默认只能安装一次"，因此放在独立测试进程
//! （integration test）中，与单测的 `repeated_init_is_idempotent`
//! （真实安装 ForgeLink subscriber）互不干扰。

use diagnostics::{LoggingConfig, LoggingError, init_logging};

#[test]
fn external_subscriber_rejected() {
    // 模拟其他模块提前安装全局 subscriber。
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::INFO)
        .with_ansi(false)
        .try_init()
        .expect("测试进程内首次安装应成功");

    let e = init_logging(LoggingConfig::default()).expect_err("外部已装 subscriber 应报错");
    assert!(
        matches!(e, LoggingError::SubscriberAlreadySet),
        "应为 SubscriberAlreadySet，实际: {e:?}"
    );

    // 失败后允许重试，语义一致。
    let e2 = init_logging(LoggingConfig::default()).expect_err("应保持同一错误");
    assert!(matches!(e2, LoggingError::SubscriberAlreadySet));
}
