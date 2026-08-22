//! Modbus Driver C ABI 全链路测试（§33 验收：Native Plugin 可被 driver-loader 加载）。
//!
//! 加载 `target/debug/driver_modbus.{dll,so}`（本 crate 的 cdylib 产物，
//! `cargo test` 构建 lib 时产出），经 driver-loader 的 `NativePlugin` 加载后：
//! 创建句柄、连接 mock Modbus TCP server、校验地址、批量读取（含批量合并、
//! 每 item 错误/类型/质量保留）、异常响应、断线重连与超时。

mod common;

use std::time::Duration;

use driver_loader::NativeDriver;
use driver_sdk::{AddressMetadata, DriverReadItem};
use modbus_mock::{MockBehavior, MockServer};
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

/// 连接配置：`mode=tcp` + mock server 地址。
fn tcp_config(server: &MockServer, timeout_ms: u64) -> String {
    modbus_mock::tcp_config(server, timeout_ms)
}

/// 从整体调用失败中取出驱动错误详情（Loader 经 `get_last_error_json` 回传）。
fn error_info(err: driver_loader::LoaderError) -> driver_sdk::DriverErrorInfo {
    match err {
        driver_loader::LoaderError::CallFailed {
            detail: Some(info), ..
        } => info,
        other => panic!("期望 CallFailed 且携带错误详情，得到 {other:?}"),
    }
}

#[test]
fn create_connect_validate_round_trip() {
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 1000));

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
    let mut driver = create(&tcp_config(&server, 1000));
    let err = driver
        .validate_address("20001")
        .expect_err("未知段必须被拒绝");
    assert_eq!(err.code(), "driver_call_failed");
}

#[test]
fn reads_multiple_addresses_with_types_and_quality() {
    let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x1388, 0x0001]); // 40001, 40002
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 1000));
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
    let mut driver = create(&tcp_config(&server, 1000));
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
    let mut driver = create(&tcp_config(&server, 1000));
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
        .insert((1, modbus_mock::Kind::HoldingRegister, 1), 0x02);
    let mut driver = create(&tcp_config(&server, 1000));
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
    let mut driver = create(&tcp_config(&server, 100));
    driver.connect().expect("connect 失败");

    // 超时属于传输级失败：必须整体失败返回（PollDriver 约定 §22），
    // 不能转成单项错误伪装成成功批次（否则上层不触发退避/重连）。
    let err = driver
        .read(&[item(1, "1!40001", None)])
        .expect_err("超时必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "timeout");
    assert!(info.retryable);
}

#[test]
fn timeout_then_recovery_reconnects() {
    // 第一次请求超时（响应延迟 > 超时）；超时后会话必须被丢弃，
    // 恢复延迟后再次读取应在新连接上成功，而不是读到迟到帧（事务号错位）。
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[0x2A])
        .with_response_delay(Duration::from_millis(500));
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 100));
    driver.connect().expect("connect 失败");

    let err = driver
        .read(&[item(1, "1!40001", None)])
        .expect_err("超时必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "timeout");
    assert!(info.retryable);

    // 服务端恢复正常延迟，下一次读取必须成功（重连后新事务号）。
    server.behavior().lock().unwrap().response_delay = None;
    let results = driver.read(&[item(1, "1!40001", None)]).expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(0x2A)));
    assert_eq!(results[0].protocol_quality_code, Some(0));
    assert!(results[0].error.is_none());
    // 重连意味着建立新连接并发出新请求。
    assert!(server.request_count() >= 2);
}

#[test]
fn rejects_explicit_address_below_segment_base() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    // 地址号低于段基数会让 offset 下溢（曾 panic / 读错地址）。
    for bad in [
        "holding:1",
        "input:1",
        "discrete:1",
        "2!holding:40000",
        "2!holding:105537",
    ] {
        let err = driver
            .validate_address(bad)
            .expect_err(&format!("{bad} 必须被拒绝"));
        assert_eq!(err.code(), "driver_call_failed");
    }
    // 段基数本身合法。
    assert!(driver.validate_address("holding:40001").is_ok());
    assert!(driver.validate_address("coil:1").is_ok());
}

#[test]
fn rejects_complex_type_tag() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");
    // Array/Struct 复杂 Tag 缺少 schema，不得当作未指定类型。
    let err = driver
        .read(&[item(
            1,
            "1!40001",
            Some(observation_model::DataType::Array(Box::new(
                observation_model::DataType::U16,
            ))),
        )])
        .expect_err("复杂 Tag 必须被拒绝");
    assert_eq!(err.code(), "driver_call_failed");
}

#[test]
fn rejects_bool_on_register_segment() {
    let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x2A]);
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 寄存器段 expected_type=Bool 必须返回明确解码错误（decode_error），
    // 不得 panic 被 ABI 边界误报为 DRIVER_PANIC。
    let results = driver
        .read(&[item(1, "1!40001", Some(DataType::Bool))])
        .expect("解码失败应作为单项错误返回（非传输级）");
    assert!(results[0].value.is_none());
    let error = results[0].error.as_ref().expect("必须有错误");
    assert_eq!(error.code, "decode_error");
    assert!(!error.retryable);
}

#[test]
fn rejects_response_body_length_mismatch() {
    // 服务端 Byte Count 声明与实际长度不符（坏实现）：响应必须整体失败
    // 并丢弃会话，不得把截断/超长数据当成功，剩余字节不得污染下一次事务。
    let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x2A]);
    let server = MockServer::start(behavior);
    server.behavior().lock().unwrap().declare_wrong_byte_count = true;
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let err = driver
        .read(&[item(1, "1!40001", None)])
        .expect_err("响应长度不符必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "invalid_response");

    // 会话已丢弃：恢复正常后下一次读取在新连接上成功。
    server.behavior().lock().unwrap().declare_wrong_byte_count = false;
    let results = driver.read(&[item(1, "1!40001", None)]).expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(0x2A)));
}

#[test]
fn rejects_malformed_exception_response() {
    // 异常响应必须恰好 2 字节（fc|0x80 + 异常码）：缺异常码或多余字节的
    // 畸形帧不得被映射为可重试的 Modbus 异常，必须按响应失步整体失败。
    for malformed in [
        modbus_mock::MalformedException::MissingCode,
        modbus_mock::MalformedException::ExtraByte,
    ] {
        let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x2A]);
        let server = MockServer::start(behavior);
        server.behavior().lock().unwrap().malformed_exception = Some(malformed);
        server
            .behavior()
            .lock()
            .unwrap()
            .exception_at
            .insert((1, modbus_mock::Kind::HoldingRegister, 0), 0x02);
        let mut driver = create(&tcp_config(&server, 1000));
        driver.connect().expect("connect 失败");

        let err = driver
            .read(&[item(1, "1!40001", None)])
            .expect_err("畸形异常响应必须整体失败");
        let info = error_info(err);
        assert_eq!(info.code, "invalid_response", "{malformed:?}");
        assert!(!info.retryable);

        // 会话已丢弃：恢复正常后下一次读取在新连接上成功。
        server.behavior().lock().unwrap().malformed_exception = None;
        server.behavior().lock().unwrap().exception_at.clear();
        let results = driver.read(&[item(1, "1!40001", None)]).expect("read 失败");
        assert_eq!(results[0].value, Some(RawValue::U64(0x2A)));
    }
}

#[test]
fn unsupported_capabilities_return_standard_code() {
    // capability=false 的方法必须返回标准 Unsupported 错误（code =
    // "unsupported"，§15），调用方据此稳定识别"不支持"，不得 panic。
    // （read/write 已声明，见 writes_* 与 reads_* 测试。）
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));

    let err = driver
        .execute(&driver_sdk::DriverCommand {
            command_id: "cmd".to_owned(),
            payload: serde_json::json!({}),
        })
        .expect_err("execute 未声明必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "unsupported");

    let err = driver
        .browse(Some(""))
        .expect_err("browse 未声明必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "unsupported");

    let err = driver
        .query_history(&driver_sdk::HistoryRequest {
            items: vec![],
            start_time_ns: 0,
            end_time_ns: 0,
            limit: None,
            continuation: None,
        })
        .expect_err("history 未声明必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "unsupported");
}

// ---------------------------------------------------------------- 写链路

fn write_item(id: u64, address: &str, value: RawValue) -> driver_sdk::DriverWriteItem {
    driver_sdk::DriverWriteItem {
        id,
        address: address.to_owned(),
        value,
    }
}

#[test]
fn capabilities_declare_read_and_write() {
    use driver_sdk::abi::envelope::CapabilitiesEnvelope;
    use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiStr};

    // Loader 未暴露能力声明便捷方法：直接加载 cdylib 调用 ABI 入口校验。
    // 先经 load_plugin 触发产物增量构建（避免加载过期 DLL）。
    let _ = load_plugin();
    let lib =
        unsafe { libloading::Library::new(common::plugin_file()) }.expect("加载 cdylib 产物失败");
    let entry = unsafe {
        lib.get::<unsafe extern "C" fn() -> *const DriverApiV1>(
            driver_sdk::abi::ENTRY_SYMBOL.as_bytes(),
        )
    }
    .expect("入口符号缺失");
    let api = unsafe { &*entry() };

    // get_capabilities_json 要求有效句柄：先 create（配置指向 mock server）。
    let server = MockServer::start(MockBehavior::new());
    let config = tcp_config(&server, 1000);
    let mut handle = DriverHandle {
        ptr: std::ptr::null_mut(),
    };
    let status = unsafe {
        (api.create)(
            FfiStr {
                ptr: config.as_ptr(),
                len: config.len(),
            },
            &mut handle,
        )
    };
    assert_eq!(status, 0);

    let mut out = FfiOwnedBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
        capacity: 0,
    };
    let status = unsafe { (api.get_capabilities_json)(handle, &mut out) };
    assert_eq!(status, 0);
    assert!(!out.ptr.is_null());
    let bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) };
    let envelope: CapabilitiesEnvelope = serde_json::from_slice(bytes).expect("能力声明非法");
    unsafe { (api.free_buffer)(out) };
    assert!(envelope.capabilities.read);
    assert!(envelope.capabilities.write, "写能力必须声明");
    assert!(envelope.capabilities.batch_read);
    assert!(envelope.capabilities.batch_write, "批量写能力必须声明");
    assert!(!envelope.capabilities.subscription);
    assert!(!envelope.capabilities.history);

    unsafe { (api.destroy)(handle) };
}

#[test]
fn writes_single_coil_via_fc05_and_reads_back() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[write_item(1, "1!coil:1", RawValue::Bool(true))])
        .expect("写线圈失败");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].item_id, 1);
    assert!(results[0].success);
    assert_eq!(results[0].protocol_code, Some(0));
    assert!(results[0].error.is_none());

    // 单布尔值必须用 FC05（非 FC15），数量 1。
    assert_eq!(
        server.write_records(),
        vec![modbus_mock::WriteRecord {
            unit: 1,
            function: 0x05,
            start_offset: 0,
            quantity: 1,
        }]
    );
    assert_eq!(server.value(1, modbus_mock::Kind::Coil, 0), Some(1));

    // 读回验证写入生效。
    let results = driver
        .read(&[item(1, "1!coil:1", Some(DataType::Bool))])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::Bool(true)));

    // 写 false 同样走 FC05。
    let results = driver
        .write(&[write_item(2, "1!coil:1", RawValue::Bool(false))])
        .expect("写线圈失败");
    assert!(results[0].success);
    assert_eq!(server.value(1, modbus_mock::Kind::Coil, 0), Some(0));
}

#[test]
fn writes_single_register_via_fc06_and_reads_back() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "1!40001", RawValue::U64(5000)),
            write_item(2, "2!40001", RawValue::I64(-10)),
        ])
        .expect("写寄存器失败");
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.success));

    // 载体 U64/I64 值收窄为单寄存器：两个 unit 各一次 FC06。
    let records = server.write_records();
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .all(|r| r.function == 0x06 && r.quantity == 1)
    );

    let results = driver
        .read(&[
            item(1, "1!40001", Some(DataType::U16)),
            item(2, "2!40001", Some(DataType::I16)),
        ])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(5000)));
    assert_eq!(results[1].value, Some(RawValue::I64(-10)));
}

#[test]
fn writes_u32_value_as_fc16_and_reads_back() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 70000 需要 2 寄存器：单 item 也必须用 FC16（FC06 只写 1 个寄存器）。
    let results = driver
        .write(&[write_item(1, "1!40010", RawValue::U64(70_000))])
        .expect("写寄存器失败");
    assert!(results[0].success);
    assert_eq!(
        server.write_records(),
        vec![modbus_mock::WriteRecord {
            unit: 1,
            function: 0x10,
            start_offset: 9,
            quantity: 2,
        }]
    );
    assert_eq!(
        server.value(1, modbus_mock::Kind::HoldingRegister, 9),
        Some(0x0001)
    );
    assert_eq!(
        server.value(1, modbus_mock::Kind::HoldingRegister, 10),
        Some(0x1170)
    );

    let results = driver
        .read(&[item(1, "1!40010", Some(DataType::U32))])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(70_000)));
}

#[test]
fn writes_adjacent_coils_merged_into_single_fc15() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "1!coil:5", RawValue::Bool(true)),
            write_item(2, "1!coil:6", RawValue::Bool(false)),
            write_item(3, "1!coil:7", RawValue::Bool(true)),
        ])
        .expect("批量写线圈失败");
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.success));

    // 地址连续的 3 个线圈合并为一次 FC15（数量 3），而非 3 次 FC05。
    assert_eq!(
        server.write_records(),
        vec![modbus_mock::WriteRecord {
            unit: 1,
            function: 0x0F,
            start_offset: 4,
            quantity: 3,
        }]
    );

    let results = driver
        .read(&[
            item(1, "1!coil:5", Some(DataType::Bool)),
            item(2, "1!coil:6", Some(DataType::Bool)),
            item(3, "1!coil:7", Some(DataType::Bool)),
        ])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::Bool(true)));
    assert_eq!(results[1].value, Some(RawValue::Bool(false)));
    assert_eq!(results[2].value, Some(RawValue::Bool(true)));
}

#[test]
fn writes_adjacent_registers_merged_into_single_fc16() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "1!40001", RawValue::U64(5000)),
            write_item(2, "1!40002", RawValue::I64(-10)),
            write_item(3, "1!40003", RawValue::U64(1)),
        ])
        .expect("批量写寄存器失败");
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.success));

    assert_eq!(
        server.write_records(),
        vec![modbus_mock::WriteRecord {
            unit: 1,
            function: 0x10,
            start_offset: 0,
            quantity: 3,
        }]
    );

    let results = driver
        .read(&[
            item(1, "1!40001", Some(DataType::U16)),
            item(2, "1!40002", Some(DataType::I16)),
            item(3, "1!40003", Some(DataType::U16)),
        ])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::U64(5000)));
    assert_eq!(results[1].value, Some(RawValue::I64(-10)));
    assert_eq!(results[2].value, Some(RawValue::U64(1)));
}

#[test]
fn writes_non_consecutive_addresses_split_into_multiple_requests() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 40001 与 40003 不连续：必须拆两次请求，不得把 40002 一并覆盖。
    let results = driver
        .write(&[
            write_item(1, "1!40001", RawValue::U64(0x0001)),
            write_item(2, "1!40003", RawValue::U64(0x0003)),
        ])
        .expect("批量写失败");
    assert!(results.iter().all(|r| r.success));

    let records = server.write_records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].start_offset, 0);
    assert_eq!(records[1].start_offset, 2);
    // 中间地址 40002 保持未写（mock 默认 0，且未被任何请求覆盖）。
    assert_eq!(server.value(1, modbus_mock::Kind::HoldingRegister, 1), None);
}

#[test]
fn writes_reject_unwritable_segments_whole_call() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 离散输入/输入寄存器只读：写请求必须显式失败（不得静默成功）。
    for address in ["1!input:30001", "1!discrete:10001"] {
        let err = driver
            .write(&[write_item(1, address, RawValue::U64(1))])
            .expect_err("只读段写入必须失败");
        let info = error_info(err);
        assert_eq!(info.code, "invalid_address", "{address}");
        assert!(!info.retryable);
        assert!(info.message.contains("只读"), "{address}");
    }
    assert!(server.write_records().is_empty(), "不得发出任何写请求");
}

#[test]
fn writes_report_per_item_exception_and_keep_other_plans() {
    // 40002 处从站异常（illegal data address）：覆盖该计划的 item 全部失败，
    // 独立计划（coil）不受影响。
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    server
        .behavior()
        .lock()
        .unwrap()
        .exception_at
        .insert((1, modbus_mock::Kind::HoldingRegister, 1), 0x02);
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "1!40001", RawValue::U64(1)),
            write_item(2, "1!40002", RawValue::U64(2)),
            write_item(3, "1!coil:1", RawValue::Bool(true)),
        ])
        .expect("协议级异常必须逐项返回（非传输级）");
    assert_eq!(results.len(), 3);
    // 40001 与 40002 同计划：异常响应使整个计划失败（协议层无部分成功）。
    for result in &results[..2] {
        assert!(!result.success);
        let error = result.error.as_ref().expect("必须有错误");
        assert_eq!(error.code, "modbus_exception");
        assert_eq!(error.protocol_code, Some(0x02));
        assert!(!error.retryable);
        assert_eq!(result.protocol_code, Some(0x02));
    }
    // coil 独立计划不受影响。
    assert!(results[2].success);
    assert_eq!(results[2].protocol_code, Some(0));
}

#[test]
fn writes_reject_bool_on_holding_register_per_item() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // Bool 写保持寄存器：单项 invalid_type（镜像读侧 Bool-on-register 规则），
    // 同批次其他 item 照常执行。
    let results = driver
        .write(&[
            write_item(1, "1!40001", RawValue::Bool(true)),
            write_item(2, "1!coil:1", RawValue::Bool(false)),
        ])
        .expect("类型错误必须作为单项错误返回");
    assert_eq!(results.len(), 2);
    assert!(!results[0].success);
    let error = results[0].error.as_ref().expect("必须有错误");
    assert_eq!(error.code, "invalid_type");
    assert!(!error.retryable);
    assert!(results[1].success);
    assert!(server.write_records().len() == 1, "仅 coil 项下发");
}

#[test]
fn writes_timeout_is_transport_level_failure() {
    let behavior = MockBehavior::new().with_response_delay(Duration::from_millis(500));
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 100));
    driver.connect().expect("connect 失败");

    // 写超时与读一致：传输级失败必须整体返回（上层据此退避/重连），
    // 不得转成单项错误伪装成成功批次。
    let err = driver
        .write(&[write_item(1, "1!coil:1", RawValue::Bool(true))])
        .expect_err("写超时必须整体失败");
    let info = error_info(err);
    assert_eq!(info.code, "timeout");
    assert!(info.retryable);
    // 超时时写入状态未知：mock 不得报告写入生效。
    assert!(
        server.write_records().is_empty() || server.value(1, modbus_mock::Kind::Coil, 0).is_none()
    );
}

#[test]
fn writes_f32_cdab_word_order_round_trip() {
    // word_order=Cdab：F32 写入时低字在前（镜像读侧 Cdab 合并约定）。
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&format!(
        r#"{{"mode":"tcp","host":"{}","port":{},"timeout_ms":1000,"word_order":"cdab"}}"#,
        server.addr.ip(),
        server.addr.port()
    ));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[write_item(1, "1!40001", RawValue::F64(50.0))])
        .expect("写 F32 失败");
    assert!(results[0].success);

    // 50.0f32 = 0x42480000；Cdab 低字在前：40001=0x0000，40002=0x4248。
    assert_eq!(
        server.value(1, modbus_mock::Kind::HoldingRegister, 0),
        Some(0x0000)
    );
    assert_eq!(
        server.value(1, modbus_mock::Kind::HoldingRegister, 1),
        Some(0x4248)
    );

    // 按同字序读回（F32 属性写入值本就是 f32 无损提升，往返精确相等）。
    let results = driver
        .read(&[item(1, "1!40001", Some(DataType::F32))])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::F64(50.0)));
}

#[test]
fn writes_reconnect_after_connection_drop() {
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    server.behavior().lock().unwrap().drop_connection = true;
    let err = driver
        .write(&[write_item(1, "1!coil:1", RawValue::Bool(true))])
        .expect_err("断线时写入必须失败");
    let info = error_info(err);
    assert_eq!(info.code, "connection_lost");
    server.behavior().lock().unwrap().drop_connection = false;
    std::thread::sleep(Duration::from_millis(50));

    // 下一次写入在新连接上成功。
    let results = driver
        .write(&[write_item(2, "1!coil:1", RawValue::Bool(true))])
        .expect("重连后写入失败");
    assert!(results[0].success);
    assert_eq!(server.value(1, modbus_mock::Kind::Coil, 0), Some(1));
}

#[test]
fn connection_drop_then_reconnect() {
    let behavior = MockBehavior::new().with_holding_range(1, 0, &[0x2A]);
    let server = MockServer::start(behavior);
    let mut driver = create(&tcp_config(&server, 1000));
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
