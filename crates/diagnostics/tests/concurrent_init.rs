//! 并发初始化场景（独立测试进程）。
//!
//! 多个线程同时首次调用 `init_logging` 时：恰好一个线程完成初始化
//! （`Initialized`），其余线程幂等返回 `AlreadyInitialized`，不得出现
//! `SubscriberAlreadySet` 误报（P2）。
//!
//! 本场景依赖"进程内全局默认只能安装一次"，因此放在独立测试进程
//! （integration test）中，与单测的 `repeated_init_is_idempotent`
//! （真实安装 ForgeLink subscriber）互不干扰。

use std::thread;

use diagnostics::{InitOutcome, LoggingConfig, LoggingError, init_logging, shutdown_logging};

#[test]
fn concurrent_init_allows_single_initialization() {
    let mut handles = Vec::new();
    for _ in 0..8 {
        handles.push(thread::spawn(|| init_logging(LoggingConfig::default())));
    }

    let mut initialized = 0;
    let mut already = 0;
    for handle in handles {
        match handle.join().expect("初始化线程不应 panic") {
            Ok(InitOutcome::Initialized) => initialized += 1,
            Ok(InitOutcome::AlreadyInitialized) => already += 1,
            Err(LoggingError::SubscriberAlreadySet) => {
                panic!("并发首次调用不得误报 SubscriberAlreadySet")
            }
            Err(e) => panic!("并发初始化不应报错: {e}"),
        }
    }
    assert_eq!(initialized, 1, "恰好一个线程完成初始化");
    assert_eq!(already, 7, "其余线程幂等返回");

    // 清理真实刷写线程，避免测试进程退出时残留。
    shutdown_logging();
}
