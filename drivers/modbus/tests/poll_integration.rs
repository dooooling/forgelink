//! Poll Engine × Modbus Driver 最小集成测试（§33 验收：经 poll-engine 周期
//! 采集真实 Native Plugin 驱动）。
//!
//! 链路：mock Modbus TCP server → `driver-modbus` cdylib（Native Plugin）→
//! `driver-loader` `NativeDriver` → `NativeDriverAdapter` → `PollScheduler`
//! 周期轮询，断言 `PollEvent::Batch` 携带正确的值、质量与错误语义。

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use modbus_mock::{MockBehavior, MockServer};
use observation_model::{DataType, RawValue};
use poll_engine::{PollConfig, PollDriver, PollEvent, PollScheduler, PollTarget};
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

/// 经真实 Modbus Native Plugin + poll-engine 周期采集，值/质量/时间戳完整传递。
#[tokio::test(flavor = "multi_thread")]
async fn polls_modbus_driver_via_native_plugin() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[0x1388]) // 40001 = 5000
        .with_coil_range(1, 0, &[true, false]);
    let server = MockServer::start(behavior);
    let mut driver = NativeDriver::create(load_plugin(), &modbus_mock::tcp_config(&server, 1000))
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
                device_id: "modbus-dev".to_owned(),
                interval_ms: 50,
                items: vec![
                    DriverReadItem {
                        id: 1,
                        address: "1!40001".to_owned(),
                        expected_type: Some(DataType::U16),
                    },
                    DriverReadItem {
                        id: 2,
                        address: "1!coil:1".to_owned(),
                        expected_type: Some(DataType::Bool),
                    },
                    DriverReadItem {
                        id: 3,
                        address: "1!coil:2".to_owned(),
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
    assert_eq!(batch.device_id, "modbus-dev");
    assert_eq!(batch.interval_ms, 50);
    // 值与质量原样传递（语义归一化属于 Profile + Domain，驱动不感知）。
    assert_eq!(batch.results.len(), 3);
    assert_eq!(batch.results[0].item_id, 1);
    assert_eq!(batch.results[0].value, Some(RawValue::U64(0x1388)));
    assert_eq!(batch.results[0].protocol_quality_code, Some(0));
    assert!(batch.results[0].error.is_none());
    assert!(batch.results[0].received_timestamp_ns > 0);
    assert_eq!(batch.results[1].value, Some(RawValue::Bool(true)));
    assert_eq!(batch.results[2].value, Some(RawValue::Bool(false)));
    // 周期轮询已真实发出协议请求（合并正确性由 driver_abi 单测覆盖）。
    assert!(server.request_count() >= 1, "轮询必须实际发起 Modbus 请求");
}
