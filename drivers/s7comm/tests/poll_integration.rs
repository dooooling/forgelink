//! Poll Engine × S7comm Driver 最小集成测试（§34.6 质量门槛：经
//! poll-engine 周期采集真实 Native Plugin 驱动）。
//!
//! 链路：mock S7 PLC → `driver-s7comm` cdylib（Native Plugin）→
//! `driver-loader` `NativeDriver` → `NativeDriverAdapter` → `PollScheduler`
//! 周期轮询，断言 `PollEvent::Batch` 携带正确的值、质量与时间戳。

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use observation_model::{DataType, RawValue};
use poll_engine::{PollConfig, PollDriver, PollEvent, PollScheduler, PollTarget};
use s7comm_mock::{MockBehavior, MockServer};
use tokio::sync::mpsc;

use common::load_plugin;

/// 事件快照。
#[derive(Debug, Clone)]
struct TimedEvent {
    event: PollEvent,
}

/// 收集通道事件（与 poll-engine 自身测试相同的模式）。
fn collector(mut rx: mpsc::Receiver<PollEvent>) -> Arc<Mutex<Vec<TimedEvent>>> {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&collected);
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            sink.lock().unwrap().push(TimedEvent { event });
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
            "等待 {count} 个事件超时（当前 {}）",
            snapshot.len()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// 经真实 S7 Native Plugin + poll-engine 周期采集，值/质量/时间戳完整传递。
#[tokio::test(flavor = "multi_thread")]
async fn polls_s7_driver_via_native_plugin() {
    // DB7.DBW0 = 50（U16）；M10.2 = true。
    let behavior = MockBehavior::new()
        .with_db_bytes(7, 0, &[0x00, 0x32])
        .with_bit(s7comm_mock::AREA_MARKER, 0, 10, 2, true);
    let server = MockServer::start(behavior);
    let mut driver = NativeDriver::create(load_plugin(), &s7comm_mock::tcp_config(&server, 1000))
        .expect("create 失败");
    driver.connect().expect("connect 失败");

    let adapter: Box<dyn PollDriver> = Box::new(poll_engine::NativeDriverAdapter::new(driver));
    let shared: Arc<Mutex<Box<dyn PollDriver>>> = Arc::new(Mutex::new(adapter));

    let (tx, rx) = mpsc::channel(64);
    let collected = collector(rx);

    let mut scheduler = PollScheduler::new();
    scheduler
        .spawn(
            PollTarget {
                device_id: "s7-dev".to_owned(),
                interval_ms: 50,
                items: vec![
                    DriverReadItem {
                        id: 1,
                        address: "db7.dbw0".to_owned(),
                        expected_type: Some(DataType::U16),
                    },
                    DriverReadItem {
                        id: 2,
                        address: "m10.2".to_owned(),
                        expected_type: Some(DataType::Bool),
                    },
                ],
            },
            shared,
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
    assert_eq!(batch.device_id, "s7-dev");
    assert_eq!(batch.interval_ms, 50);
    // 值与质量原样传递（语义归一化属于 Profile + Domain，驱动不感知）。
    assert_eq!(batch.results.len(), 2);
    assert_eq!(batch.results[0].item_id, 1);
    assert_eq!(batch.results[0].value, Some(RawValue::U64(50)));
    assert_eq!(batch.results[0].protocol_quality_code, Some(0));
    assert!(batch.results[0].error.is_none());
    assert!(batch.results[0].received_timestamp_ns > 0);
    assert_eq!(batch.results[1].value, Some(RawValue::Bool(true)));
    // 周期轮询已真实发起 S7 请求（DB 与 M 分属不同 PDU）。
    assert!(server.request_count() >= 2, "轮询必须实际发出协议请求");
}
