//! Poll Engine × EtherNet/IP Driver 集成测试（mock PLC → cdylib →
//! NativeDriverAdapter → PollScheduler 全链路）。

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use etherip_mock::{MockBehavior, MockServer};
use observation_model::{DataType, RawValue};
use poll_engine::{PollConfig, PollDriver, PollEvent, PollScheduler, PollTarget};
use tokio::sync::mpsc;

use common::load_plugin;

/// 事件快照。
#[derive(Debug, Clone)]
struct TimedEvent {
    event: PollEvent,
}

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
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = collected.lock().unwrap().clone();
        if snapshot.len() >= count {
            return snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待 {count} 个事件超时（当前 {}）",
            snapshot.len()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn polls_enip_driver_via_native_plugin() {
    // DINT 计数 + REAL 温度两标签。
    let behavior = MockBehavior::new()
        .with_dint("Line1.Count", 1234)
        .with_real("Temp.PV", 36.5);
    let server = MockServer::start(behavior);
    let mut driver = NativeDriver::create(load_plugin(), &etherip_mock::tcp_config(&server, 1000))
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
                device_id: "enip-dev".to_owned(),
                interval_ms: 50,
                items: vec![
                    DriverReadItem {
                        id: 1,
                        address: "Line1.Count".to_owned(),
                        expected_type: Some(DataType::I32),
                    },
                    DriverReadItem {
                        id: 2,
                        address: "Temp.PV".to_owned(),
                        expected_type: Some(DataType::F32),
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
    assert_eq!(batch.device_id, "enip-dev");
    assert_eq!(batch.results.len(), 2);
    assert_eq!(batch.results[0].value, Some(RawValue::I64(1234)));
    assert_eq!(batch.results[1].value, Some(RawValue::F64(36.5)));
    assert_eq!(batch.results[0].protocol_quality_code, Some(0));
    assert!(batch.results[0].received_timestamp_ns > 0);
    // 两标签打包进一条 Multi（周期轮询真实发出协议请求）。
    assert!(server.request_count() >= 1, "轮询必须实际发起协议请求");
}
