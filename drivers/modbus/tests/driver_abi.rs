//! Modbus Driver C ABI 全链路测试（§33 验收：Native Plugin 可被 driver-loader 加载）。
//!
//! 加载 `target/debug/driver_modbus.{dll,so}`（本 crate 的 cdylib 产物，
//! `cargo test` 构建 lib 时产出），经 driver-loader 的 `NativePlugin` 加载后：
//! 创建句柄、连接 mock Modbus TCP server、校验地址、批量读取（含批量合并、
//! 每 item 错误/类型/质量保留）、异常响应、断线重连与超时。

mod common;
mod mock_server;

use std::time::Duration;

use driver_loader::NativeDriver;
use driver_sdk::{AddressMetadata, DriverReadItem};
use mock_server::{MockBehavior, MockServer};
use observation_model::{DataType, RawValue};

use common::load_plugin;

fn create(config: &str) -> NativeDriver {
    NativeDriver::create(load_plugin(), config).expect("create 失败")
}

fn item(id: u64, address: &str, expected_type: Option<DataType>) -> DriverReadItem {
    DriverReadItem {
        id,
        address: address.to_owned(),
        expected_type,
    }
}

#[test]
fn create_connect_validate_round_trip() {
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&mock_server::tcp_config(&server, 1000));

    driver.connect().expect("connect 失败");
    assert!(driver.is_connected());

    let meta = driver.validate_address("1!40001").expect("地址校验失败");
    assert_eq!(
        meta,
        AddressMetadata {
            canonical_address: "1!holding:40001".to_owned(),
            raw_type: None,
            readable: true,
            writable: true,
        }
    );
    let meta = driver
        .validate_address("input:30001")
        .expect("地址校验失败");
    assert_eq!(meta.canonical_address, "1!input:30001");
    assert!(!meta.writable);
    let meta = driver.validate_address("3!coil:1").expect("地址校验失败");
    assert_eq!(meta.canonical_address, "3!coil:1");
    assert!(meta.writable);

    driver.disconnect().expect("disconnect 失败");
    assert!(!driver.is_connected());
}

#[test]
fn validate_address_rejects_invalid() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&mock_server::tcp_config(&server, 1000));
    let err = driver
        .validate_address("20001")
        .expect_err("未知段必须被拒绝");
    assert_eq!(err.code(), "driver_call_failed");
}

#[test]
fn reads_multiple_addresses_with_types_and_quality() {
    let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x1388, 0x0001]); // 40001, 40002
    let server = MockServer::start(behavior);
    let mut driver = create(&mock_server::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "1!40001", Some(DataType::U16)),
            item(2, "1!40002", Some(DataType::I16)),
            item(3, "1!40003", Some(DataType::U32)),
        ])
        .expect("read 失败");

    // 三个地址连续 -> 一次合并请求（1 个事务），区间覆盖到 U32 的第二个寄存器。
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].item_id, 1);
    assert_eq!(results[0].value, Some(RawValue::U64(0x1388)));
    assert_eq!(results[0].protocol_quality_code, Some(0));
    assert!(results[0].error.is_none());
    assert_eq!(results[1].value, Some(RawValue::I64(0x0001)));
    assert_eq!(results[2].value, Some(RawValue::U64(0)));
}

#[test]
fn reads_coils_and_input_registers() {
    let behavior = MockBehavior::new()
        .with_coil_range(1, 0, &[true, false, true])
        .with_input_range(1, 0, &[0x002A]);
    let server = MockServer::start(behavior);
    let mut driver = create(&mock_server::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "1!coil:1", Some(DataType::Bool)),
            item(2, "1!coil:2", Some(DataType::Bool)),
            item(3, "1!coil:3", Some(DataType::Bool)),
            item(4, "1!input:30001", Some(DataType::U16)),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].value, Some(RawValue::Bool(true)));
    assert_eq!(results[1].value, Some(RawValue::Bool(false)));
    assert_eq!(results[2].value, Some(RawValue::Bool(true)));
    assert_eq!(results[3].value, Some(RawValue::U64(42)));
}

#[test]
fn merge_does_not_cross_unit_or_requests() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[0x11, 0x22, 0x33])
        .with_holding_range(2, 1, &[0xAA, 0xBB]);
    let server = MockServer::start(behavior);
    let mut driver = create(&mock_server::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // unit 1 的 40001..40003 与 unit 2 的 40002..40003：分属两个请求。
    let results = driver
        .read(&[
            item(1, "1!40001", None),
            item(2, "1!40002", None),
            item(3, "2!40002", None),
            item(4, "2!40003", None),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].value, Some(RawValue::U64(0x11)));
    assert_eq!(results[1].value, Some(RawValue::U64(0x22)));
    assert_eq!(results[2].value, Some(RawValue::U64(0xAA)));
    assert_eq!(results[3].value, Some(RawValue::U64(0xBB)));
    // unit 1 与 unit 2 各一次请求，无跨单元合并。
    assert_eq!(server.request_count(), 2);
}

#[test]
fn preserves_per_item_errors_on_plan_failure() {
    // 40002 处异常（illegal data address）→ 覆盖该计划的 item 得到错误，
    // 独立计划（coil）正常。
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[0x01])
        .with_coil_range(1, 0, &[true]);
    let server = MockServer::start(behavior);
    server
        .behavior()
        .lock()
        .unwrap()
        .exception_at
        .insert((1, mock_server::Kind::HoldingRegister, 1), 0x02);
    let mut driver = create(&mock_server::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "1!40001", None),
            item(2, "1!40002", None),
            item(3, "1!coil:1", Some(DataType::Bool)),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 3);
    // 40001 与 40002 在同一计划，异常响应使整个计划失败（协议层无部分成功）。
    assert!(results[0].value.is_none());
    let error = results[0].error.as_ref().expect("40001 必须有错误");
    assert_eq!(error.code, "modbus_exception");
    assert_eq!(error.protocol_code, Some(0x02));
    assert!(!error.retryable);
    // 40002 异常：protocol_code = 0x02，retryable = false（配置/寻址类）。
    assert!(results[1].value.is_none());
    let error = results[1].error.as_ref().expect("40002 必须有错误");
    assert_eq!(error.code, "modbus_exception");
    assert_eq!(error.protocol_code, Some(0x02));
    assert!(!error.retryable);
    assert_eq!(results[1].protocol_quality_code, Some(0x02));
    // coil 独立计划不受影响。
    assert_eq!(results[2].value, Some(RawValue::Bool(true)));
}

#[test]
fn timeout_returns_retryable_error() {
    let behavior = MockBehavior::new().with_response_delay(Duration::from_millis(500));
    let server = MockServer::start(behavior);
    let mut driver = create(&mock_server::tcp_config(&server, 100));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[item(1, "1!40001", None)])
        .expect("超时应作为单项/整体错误返回");

    let error = results[0].error.as_ref().expect("超时必须有错误");
    assert_eq!(error.code, "timeout");
    assert!(error.retryable);
    assert!(results[0].value.is_none());
}

#[test]
fn connection_drop_then_reconnect() {
    let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x2A]);
    let server = MockServer::start(behavior);
    let mut driver = create(&mock_server::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 第一次读取成功。
    let results = driver.read(&[item(1, "1!40001", None)]).expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(0x2A)));

    // 服务端断开连接，下一次读取自动重连成功。
    server.behavior().lock().unwrap().drop_connection = true;
    let _ = driver.read(&[item(1, "1!40001", None)]);
    server.behavior().lock().unwrap().drop_connection = false;
    // 等待断开的连接被 mock 线程关闭。
    std::thread::sleep(Duration::from_millis(50));

    let results = driver
        .read(&[item(1, "1!40001", None)])
        .expect("重连后 read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(0x2A)));
}

#[test]
fn create_rejects_invalid_config() {
    let err = NativeDriver::create(load_plugin(), "{}").expect_err("缺少 mode 的配置必须被拒绝");
    assert_eq!(err.code(), "driver_create_failed");
    // detail 来自 get_last_error_json（ErrorEnvelope）。
    match err {
        driver_loader::LoaderError::CreateFailed { detail: Some(info) } => {
            assert_eq!(info.code, "config_error");
            assert!(!info.retryable);
        }
        _ => panic!("create 失败应携带错误详情"),
    }
}
