//! Poll Engine 集成测试：调度周期、超时、退避重试、取消、设备隔离与错误/质量保留。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use driver_sdk::{DriverErrorInfo, DriverReadItem};
use observation_model::{RawReadResult, RawValue, TimestampNs};
use poll_engine::{PollConfig, PollConfigError, PollDriver, PollEvent, PollScheduler, PollTarget};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// 行为可编程的 mock 驱动。
struct MockDriver {
    /// 每次调用前的阻塞时长（模拟慢设备）。
    delay: Duration,
    /// 剩余整体失败次数；降为 0 后成功。
    fail_remaining: AtomicUsize,
    /// 整体失败时返回的错误。
    fail_error: DriverErrorInfo,
    /// 成功时返回的结果。
    results: Vec<RawReadResult>,
    /// 在途调用数（单飞行断言）。
    in_flight: Arc<AtomicUsize>,
    /// 历史最大并发调用数。
    max_concurrent: Arc<AtomicUsize>,
}

impl PollDriver for MockDriver {
    fn read_batch(
        &mut self,
        _items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent.fetch_max(current, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if self.fail_remaining.load(Ordering::SeqCst) > 0 {
            self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(self.fail_error.clone());
        }
        Ok(self.results.clone())
    }
}

/// 构造 mock 驱动（可指定失败错误是否可重试）。
///
/// 返回 `(驱动, 当前在途计数, 历史最大并发计数)`。
#[allow(clippy::type_complexity)]
fn mock_driver_with_counters(
    delay: Duration,
    fail_times: usize,
    fail_retryable: bool,
    results: Vec<RawReadResult>,
) -> (
    Arc<Mutex<Box<dyn PollDriver>>>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let fail_error = DriverErrorInfo {
        code: "driver_call_failed".to_owned(),
        message: "模拟设备不可达".to_owned(),
        protocol_code: None,
        retryable: fail_retryable,
    };
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let driver: Arc<Mutex<Box<dyn PollDriver>>> = Arc::new(Mutex::new(Box::new(MockDriver {
        delay,
        fail_remaining: AtomicUsize::new(fail_times),
        fail_error,
        results,
        in_flight: in_flight.clone(),
        max_concurrent: max_concurrent.clone(),
    })));
    (driver, in_flight, max_concurrent)
}

fn mock_driver(
    delay: Duration,
    fail_times: usize,
    results: Vec<RawReadResult>,
) -> Arc<Mutex<Box<dyn PollDriver>>> {
    mock_driver_with_counters(delay, fail_times, true, results).0
}

fn items(count: u64) -> Vec<DriverReadItem> {
    (0..count)
        .map(|id| DriverReadItem {
            id,
            address: format!("1!{:04}", 1000 + id),
            expected_type: None,
        })
        .collect()
}

fn ok_result(item_id: u64) -> RawReadResult {
    let now: TimestampNs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    RawReadResult {
        item_id,
        value: Some(RawValue::I64(42)),
        source_timestamp_ns: Some(now),
        received_timestamp_ns: now,
        protocol_quality_code: Some(0xC0),
        error: None,
    }
}

/// 带到达时间戳（wall-clock 毫秒，`u128` 可复制）的事件。
#[derive(Debug, Clone)]
struct TimedEvent {
    at_ms: u128,
    event: PollEvent,
}

/// 启动收集任务：把通道内事件（含到达时间）累积到共享 Vec。
fn collector(mut rx: mpsc::Receiver<PollEvent>) -> Arc<Mutex<Vec<TimedEvent>>> {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&collected);
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            sink.lock().unwrap().push(TimedEvent { at_ms, event });
        }
    });
    collected
}

fn wait_events(
    collected: &Arc<Mutex<Vec<TimedEvent>>>,
    count: usize,
    timeout: Duration,
) -> Vec<TimedEvent> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = collected.lock().unwrap().clone();
        if snapshot.len() >= count {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "等待 {} 个事件超时（当前 {}）",
            count,
            snapshot.len()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn failed_events(events: &[TimedEvent]) -> Vec<&TimedEvent> {
    events
        .iter()
        .filter(|t| matches!(t.event, PollEvent::Failed { .. }))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn polls_at_configured_interval() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-1".to_owned(),
        interval_ms: 50,
        items: items(3),
    };
    let config = PollConfig {
        request_timeout: Duration::from_secs(5),
        backoff_base_ms: 1_000,
        backoff_max_ms: 30_000,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };

    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            target,
            mock_driver(
                Duration::ZERO,
                0,
                vec![ok_result(0), ok_result(1), ok_result(2)],
            ),
            config,
            tx,
        )
        .unwrap();

    let events = wait_events(&collected, 3, Duration::from_secs(5));
    scheduler.shutdown().await;

    let batches: Vec<_> = events
        .iter()
        .filter_map(|t| match &t.event {
            PollEvent::Batch(b) => Some(b),
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 3);
    for batch in &batches {
        assert_eq!(batch.device_id, "dev-1");
        assert_eq!(batch.interval_ms, 50);
        assert_eq!(batch.results.len(), 3);
    }
    // 周期 ~50ms：相邻批次到达时间差 >= 30ms 且不超过 200ms。
    let arrivals: Vec<u128> = events
        .iter()
        .filter(|t| matches!(t.event, PollEvent::Batch(_)))
        .map(|t| t.at_ms)
        .collect();
    for pair in arrivals.windows(2) {
        let gap = pair[1] - pair[0];
        assert!((30..=200).contains(&gap), "批次间隔异常：{gap}ms");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn preserves_result_error_and_quality() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-2".to_owned(),
        interval_ms: 50,
        items: items(2),
    };

    let mut ok = ok_result(0);
    ok.protocol_quality_code = Some(0x58);
    let mut failed = ok_result(1);
    failed.value = None;
    failed.error = Some(DriverErrorInfo {
        code: "driver_item_timeout".to_owned(),
        message: "单项读取超时".to_owned(),
        protocol_code: Some(3),
        retryable: true,
    });

    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            target,
            mock_driver(Duration::ZERO, 0, vec![ok, failed.clone()]),
            PollConfig::default(),
            tx,
        )
        .unwrap();

    let events = wait_events(&collected, 1, Duration::from_secs(5));
    scheduler.shutdown().await;

    let batch = match &events[0].event {
        PollEvent::Batch(b) => b.clone(),
        other => panic!("预期 Batch，得到 {other:?}"),
    };
    // 质量码与单项错误必须原样保留（语义归一化属于 Profile + Domain）。
    assert_eq!(batch.results[0].protocol_quality_code, Some(0x58));
    assert_eq!(batch.results[0].error, None);
    assert_eq!(batch.results[1], failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn times_out_slow_device_and_keeps_running() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-slow".to_owned(),
        interval_ms: 50,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_millis(50),
        backoff_base_ms: 10,
        backoff_max_ms: 100,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };

    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            target,
            mock_driver(Duration::from_millis(300), 0, vec![ok_result(0)]),
            config,
            tx,
        )
        .unwrap();

    let events = wait_events(&collected, 1, Duration::from_secs(5));
    scheduler.shutdown().await;

    let failed = match &events[0].event {
        PollEvent::Failed {
            device_id,
            interval_ms,
            error,
            ..
        } => {
            assert_eq!(device_id, "dev-slow");
            assert_eq!(*interval_ms, 50);
            error.clone()
        }
        other => panic!("预期 Failed，得到 {other:?}"),
    };
    assert_eq!(failed.code, "driver_request_timeout");
    assert!(failed.retryable);
}

#[tokio::test(flavor = "multi_thread")]
async fn backs_off_with_increasing_delay_then_resets() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-flaky".to_owned(),
        interval_ms: 50,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_secs(1),
        backoff_base_ms: 10,
        backoff_max_ms: 100,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };

    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            target,
            mock_driver(Duration::ZERO, 3, vec![ok_result(0)]),
            config,
            tx,
        )
        .unwrap();

    // 失败 3 次（10ms → 20ms → 40ms 退避），随后成功。
    let events = wait_events(&collected, 4, Duration::from_secs(5));
    scheduler.shutdown().await;

    let fails = failed_events(&events);
    assert_eq!(fails.len(), 3, "应连续失败 3 次");
    assert!(
        matches!(events.last().map(|t| &t.event), Some(PollEvent::Batch(_))),
        "第 4 次应成功"
    );
    // 退避间隔递增：10ms → 20ms → 40ms（宽松断言：后一段不短于前一段）。
    let gaps: Vec<u128> = fails.windows(2).map(|w| w[1].at_ms - w[0].at_ms).collect();
    assert!(gaps.len() == 2);
    assert!(gaps[1] >= gaps[0], "退避应递增，实际 {gaps:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_stops_all_tasks() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-cancel".to_owned(),
        interval_ms: 10,
        items: items(1),
    };

    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            target,
            mock_driver(Duration::ZERO, 0, vec![ok_result(0)]),
            PollConfig::default(),
            tx,
        )
        .unwrap();
    let _ = wait_events(&collected, 1, Duration::from_secs(5));

    let before = collected.lock().unwrap().len();
    scheduler.shutdown().await;
    // 停机后任务不再产生事件。
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(collected.lock().unwrap().len(), before);
    assert_eq!(scheduler.task_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_device_does_not_block_others() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);

    let mut scheduler = PollScheduler::new();
    let config = PollConfig {
        request_timeout: Duration::from_millis(50),
        backoff_base_ms: 10,
        backoff_max_ms: 50,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };
    scheduler
        .spawn(
            PollTarget {
                device_id: "slow".to_owned(),
                interval_ms: 20,
                items: items(1),
            },
            mock_driver(Duration::from_millis(300), 0, vec![ok_result(0)]),
            config.clone(),
            tx.clone(),
        )
        .unwrap();
    scheduler
        .spawn(
            PollTarget {
                device_id: "fast".to_owned(),
                interval_ms: 20,
                items: items(1),
            },
            mock_driver(Duration::ZERO, 0, vec![ok_result(0)]),
            config,
            tx,
        )
        .unwrap();

    // 慢设备持续超时期间，快设备仍按周期出 Batch。
    let events = wait_events(&collected, 6, Duration::from_secs(5));
    scheduler.shutdown().await;

    let fast_batches = events
        .iter()
        .filter(|t| matches!(t.event, PollEvent::Batch(ref b) if b.device_id == "fast"))
        .count();
    assert!(
        fast_batches >= 3,
        "快设备应持续产出批次，实际 {fast_batches}"
    );
    assert!(
        events.iter().any(
            |t| matches!(t.event, PollEvent::Failed { ref device_id, .. } if device_id == "slow")
        ),
        "慢设备应产生超时失败事件"
    );
}

/// 直接调用 poll_loop + 手动取消令牌的取消测试。
#[tokio::test(flavor = "multi_thread")]
async fn external_cancel_token_stops_loop() {
    let (tx, _rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let target = PollTarget {
        device_id: "dev-token".to_owned(),
        interval_ms: 10,
        items: items(1),
    };
    let driver = mock_driver(Duration::ZERO, 0, vec![ok_result(0)]);

    let handle = tokio::spawn(poll_engine::poll_loop(
        target,
        driver,
        PollConfig::default(),
        cancel.clone(),
        tx,
    ));
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
    result.expect("任务应正常结束").expect("任务不应 panic");
}

/// 不可重试错误不进入退避快速循环，回到周期节律（间隔 ~= interval 而非 backoff）。
#[tokio::test(flavor = "multi_thread")]
async fn non_retryable_error_returns_to_cycle() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-perm".to_owned(),
        interval_ms: 50,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_secs(1),
        backoff_base_ms: 10,
        backoff_max_ms: 100,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };

    let mut scheduler = PollScheduler::new();
    let (driver, _, _) =
        mock_driver_with_counters(Duration::ZERO, usize::MAX, false, vec![ok_result(0)]);
    scheduler.spawn(target, driver, config, tx).unwrap();

    let events = wait_events(&collected, 2, Duration::from_secs(5));
    scheduler.shutdown().await;

    let fails = failed_events(&events);
    assert_eq!(fails.len(), 2);
    for timed in &fails {
        let error = match &timed.event {
            PollEvent::Failed { error, .. } => error,
            _ => unreachable!(),
        };
        assert!(!error.retryable);
    }
    // 若进入退避循环间隔应 ~10ms；回到周期节律则为 ~50ms。
    // 阈值 30ms：既要区分两者，又容忍负载下的调度抖动（曾实测 37ms）。
    let gap = fails[1].at_ms - fails[0].at_ms;
    assert!(
        gap >= 30,
        "永久错误应回到周期节律而非退避重试，实际间隔 {gap}ms"
    );
}

/// 有界通道满时（接收端存活），取消令牌必须让任务退出，停机不被阻塞。
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_unblocks_full_channel() {
    let (tx, rx) = mpsc::channel(1);
    // 保留接收端（不消费）：通道满后 send 会阻塞。
    let rx_guard = rx;
    std::hint::black_box(&rx_guard);

    let target = PollTarget {
        device_id: "dev-full".to_owned(),
        interval_ms: 10,
        items: items(1),
    };
    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            target,
            mock_driver(Duration::ZERO, 0, vec![ok_result(0)]),
            PollConfig::default(),
            tx,
        )
        .unwrap();

    // 等通道被首个事件占满，随后取消并停机。
    std::thread::sleep(Duration::from_millis(100));
    let result = tokio::time::timeout(Duration::from_secs(2), scheduler.shutdown()).await;
    result.expect("停机不应被满通道阻塞");
}

/// 超时后同一时刻至多一个阻塞调用在途（单飞行），不堆积 spawn_blocking 任务。
#[tokio::test(flavor = "multi_thread")]
async fn single_in_flight_call() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-single".to_owned(),
        interval_ms: 20,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_millis(50),
        backoff_base_ms: 5,
        backoff_max_ms: 20,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };

    let mut scheduler = PollScheduler::new();
    let (driver, _, max_concurrent) =
        mock_driver_with_counters(Duration::from_millis(300), 0, true, vec![ok_result(0)]);
    scheduler.spawn(target, driver, config, tx).unwrap();

    // 持续运行，让慢驱动反复超时并触发重试。
    let _ = wait_events(&collected, 3, Duration::from_secs(5));
    scheduler.shutdown().await;

    assert_eq!(
        max_concurrent.load(Ordering::SeqCst),
        1,
        "阻塞调用必须单飞行，不得堆积"
    );
}

/// 非法轮询配置在 spawn 时被拒绝，不创建任务。
#[tokio::test(flavor = "multi_thread")]
async fn rejects_invalid_config() {
    let target = |interval_ms: u64| PollTarget {
        device_id: "dev-cfg".to_owned(),
        interval_ms,
        items: items(1),
    };
    let driver = mock_driver(Duration::ZERO, 0, vec![ok_result(0)]);
    let (tx, _rx) = mpsc::channel(16);

    let mut scheduler = PollScheduler::new();
    let ok_config = PollConfig::default();

    let err = scheduler
        .spawn(
            target(0),
            Arc::clone(&driver),
            ok_config.clone(),
            tx.clone(),
        )
        .unwrap_err();
    assert_eq!(err, PollConfigError::InvalidInterval);

    let mut bad_factor = ok_config.clone();
    bad_factor.backoff_factor = 0;
    let err = scheduler
        .spawn(target(50), Arc::clone(&driver), bad_factor, tx.clone())
        .unwrap_err();
    assert_eq!(err, PollConfigError::InvalidBackoffFactor);

    let mut bad_timeout = ok_config.clone();
    bad_timeout.request_timeout = Duration::ZERO;
    let err = scheduler
        .spawn(target(50), Arc::clone(&driver), bad_timeout, tx.clone())
        .unwrap_err();
    assert_eq!(err, PollConfigError::InvalidTimeout);

    let mut bad_backoff = ok_config.clone();
    bad_backoff.backoff_base_ms = 0;
    let err = scheduler
        .spawn(target(50), Arc::clone(&driver), bad_backoff, tx.clone())
        .unwrap_err();
    assert_eq!(err, PollConfigError::InvalidBackoff);

    assert_eq!(scheduler.task_count(), 0, "校验失败不得创建任务");
    scheduler.shutdown().await;
}

/// 有序停机：shutdown 返回前，超时遗留的在途阻塞调用必须已收尾（驱动不再存活）。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_inflight_worker() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-drain".to_owned(),
        interval_ms: 10,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_millis(50),
        backoff_base_ms: 5,
        backoff_max_ms: 20,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_secs(2),
    };

    let mut scheduler = PollScheduler::new();
    let (driver, in_flight, _) =
        mock_driver_with_counters(Duration::from_millis(300), 0, true, vec![ok_result(0)]);
    scheduler.spawn(target, driver, config, tx).unwrap();

    // 等到第一次超时失败产生（此时 300ms 的阻塞调用仍在途）。
    let _ = wait_events(&collected, 1, Duration::from_secs(5));
    assert_eq!(in_flight.load(Ordering::SeqCst), 1, "慢驱动应仍在阻塞");

    // shutdown 必须等待在途调用收尾后再返回。
    let result = tokio::time::timeout(Duration::from_secs(5), scheduler.shutdown()).await;
    result.expect("停机应等待在途阻塞调用收尾（300ms 内完成）");
    assert_eq!(
        in_flight.load(Ordering::SeqCst),
        0,
        "shutdown 返回后驱动调用不得存活"
    );
}

/// 直接调用公开的 poll_loop 时，非法配置不得触发 Tokio panic。
#[tokio::test(flavor = "multi_thread")]
async fn poll_loop_rejects_invalid_config_without_panic() {
    let (tx, _rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let target = PollTarget {
        device_id: "dev-badcfg".to_owned(),
        interval_ms: 0,
        items: items(1),
    };
    let driver = mock_driver(Duration::ZERO, 0, vec![ok_result(0)]);

    let handle = tokio::spawn(poll_engine::poll_loop(
        target,
        driver,
        PollConfig::default(),
        cancel.clone(),
        tx,
    ));
    let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
    result.expect("任务应正常返回").expect("任务不应 panic");
}

/// 停机收尾有上限：驱动永久阻塞时 shutdown 在 `shutdown_drain_timeout` 内返回，
/// 不无限等待；句柄由阻塞线程的 `Arc` 引用持有，线程自然结束后释放。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_bounded_when_driver_blocks_forever() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-hang".to_owned(),
        interval_ms: 10,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_millis(50),
        backoff_base_ms: 5,
        backoff_max_ms: 20,
        backoff_factor: 2,
        shutdown_drain_timeout: Duration::from_millis(200),
    };

    let mut scheduler = PollScheduler::new();
    // 驱动阻塞 1s（远超收尾上限 200ms，视为永久阻塞）：每次调用都会超时并在途遗留。
    let (driver, _, _) =
        mock_driver_with_counters(Duration::from_secs(1), 0, true, vec![ok_result(0)]);
    scheduler.spawn(target, driver, config, tx).unwrap();

    // 等到第一次超时失败（在途 worker 遗留）。
    let _ = wait_events(&collected, 1, Duration::from_secs(5));

    // shutdown 必须在收尾上限附近返回，不得被永久阻塞的驱动拖住。
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), scheduler.shutdown()).await;
    result.expect("停机不得被永久阻塞的驱动无限拖住");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "停机应受收尾上限约束，实际耗时 {elapsed:?}"
    );
}

/// `shutdown_drain_timeout` 为零的配置在 spawn 时被拒绝。
#[tokio::test(flavor = "multi_thread")]
async fn rejects_zero_drain_timeout() {
    let target = PollTarget {
        device_id: "dev-drain0".to_owned(),
        interval_ms: 50,
        items: items(1),
    };
    let driver = mock_driver(Duration::ZERO, 0, vec![ok_result(0)]);
    let (tx, _rx) = mpsc::channel(16);

    let config = PollConfig {
        shutdown_drain_timeout: Duration::ZERO,
        ..PollConfig::default()
    };

    let mut scheduler = PollScheduler::new();
    let err = scheduler.spawn(target, driver, config, tx).unwrap_err();
    assert_eq!(err, PollConfigError::InvalidDrainTimeout);
    assert_eq!(scheduler.task_count(), 0);
    scheduler.shutdown().await;
}

/// `shutdown_with_timeout` 有界：驱动永久阻塞且收尾上限内无法自然结束时，
/// 等待超时后强制 abort 轮询任务并返回，不得无限等待（评审 P1：REST
/// 绑定失败清理等失败路径必须能按时返回）。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_timeout_aborts_stuck_tasks() {
    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);
    let target = PollTarget {
        device_id: "dev-stuck".to_owned(),
        interval_ms: 10,
        items: items(1),
    };
    let config = PollConfig {
        request_timeout: Duration::from_millis(50),
        backoff_base_ms: 5,
        backoff_max_ms: 20,
        backoff_factor: 2,
        // 收尾上限很长：专门证明 shutdown_with_timeout 自身的 grace 生效。
        shutdown_drain_timeout: Duration::from_secs(60),
    };

    let mut scheduler = PollScheduler::new();
    // 驱动阻塞 2s（远超 grace 100ms，视为永久阻塞）。
    let (driver, _, _) =
        mock_driver_with_counters(Duration::from_secs(2), 0, true, vec![ok_result(0)]);
    scheduler.spawn(target, driver, config, tx).unwrap();

    // 等到第一次超时失败（在途 worker 遗留，任务阻塞在收尾阶段）。
    let _ = wait_events(&collected, 1, Duration::from_secs(5));

    // grace 100ms：等待超时 → 强制 abort，不得被 30s 阻塞的驱动拖住。
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.shutdown_with_timeout(Duration::from_millis(100)),
    )
    .await;
    result.expect("shutdown_with_timeout 必须按时返回");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "超时后应强制 abort 并返回，实际耗时 {elapsed:?}"
    );
}
