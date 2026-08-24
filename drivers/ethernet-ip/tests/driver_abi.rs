//! EtherNet/IP Driver C ABI 全链路测试（§34.6 质量门槛：经 driver-loader
//! 加载真实 cdylib，连 etherip-mock 覆盖地址校验、Multi 打包、逐项错误、
//! 超时重连、失步检测与读+写回环）。

mod common;

use std::time::Duration;

use driver_loader::NativeDriver;
use driver_sdk::{AddressMetadata, DriverReadItem};
use etherip_mock::{MockBehavior, MockServer};
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

fn write_item(id: u64, address: &str, value: RawValue) -> driver_sdk::DriverWriteItem {
    driver_sdk::DriverWriteItem {
        id,
        address: address.to_owned(),
        value,
    }
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

// ---------------------------------------------------------------- 基础链路

#[test]
fn create_connect_validate_round_trip() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));

    driver.connect().expect("connect 失败");
    assert!(driver.is_connected());
    assert_eq!(
        server.registered_sessions(),
        1,
        "一次连接必须恰好一次 RegisterSession"
    );

    let meta = driver.validate_address("Line1.Speed").expect("校验失败");
    assert_eq!(
        meta,
        AddressMetadata {
            canonical_address: "Line1.Speed".to_owned(),
            raw_type: None,
            readable: true,
            writable: true,
        }
    );
    // 大小写敏感：canonical 原样保留。
    let meta = driver
        .validate_address("  MixedCase_Tag[10] ")
        .expect("校验失败");
    assert_eq!(meta.canonical_address, "MixedCase_Tag[10]");

    driver.disconnect().expect("disconnect 失败");
    assert!(!driver.is_connected());
}

#[test]
fn validate_address_rejects_invalid() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    for bad in ["", "a..b", "a[]", "[0]", "a b"] {
        let err = driver.validate_address(bad).expect_err("必须被拒绝");
        assert!(
            error_info(err).code.starts_with("invalid"),
            "'{bad}' 应映射 invalid_*"
        );
    }
}

#[test]
fn create_rejects_invalid_config() {
    let server = MockServer::start(MockBehavior::new());
    let result = NativeDriver::create(
        load_plugin(),
        &format!(
            r#"{{"host":"{}","port":{},"max_services_per_multi":0}}"#,
            server.addr.ip(),
            server.addr.port()
        ),
    );
    let err = match result {
        Err(driver_loader::LoaderError::CallFailed {
            detail: Some(info), ..
        })
        | Err(driver_loader::LoaderError::CreateFailed {
            detail: Some(info), ..
        }) => info,
        other => panic!("期望 create 失败，得到 {other:?}"),
    };
    assert_eq!(err.code, "config_error");
}

// ---------------------------------------------------------------- 读链路

#[test]
fn reads_packed_into_single_multi() {
    let behavior = MockBehavior::new()
        .with_dint("Line1.Speed", 1500)
        .with_bool("Motor.Run", true);
    let server = MockServer::start(behavior);
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "Line1.Speed", Some(DataType::I32)),
            item(2, "Motor.Run", Some(DataType::Bool)),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 2);
    assert_eq!(server.request_count(), 1, "两标签必须打包进同一 Multi");
    assert_eq!(server.read_records().len(), 2);
    // 结果按 item_id 排序回填。
    assert_eq!(results[0].item_id, 1);
    assert_eq!(results[0].value, Some(RawValue::I64(1500)));
    assert_eq!(results[1].value, Some(RawValue::Bool(true)));
    assert_eq!(results[0].protocol_quality_code, Some(0));
}

#[test]
fn reads_split_by_byte_budget() {
    // 20 个长名标签（每子请求 ~30B，总计 > 600B）：
    // 收紧 max_bytes_per_multi=256 → 必拆多次请求；放宽 → 单请求。
    let seed_tags = |server: &MockServer| {
        let behavior = server.behavior();
        let mut guard = behavior.lock().unwrap();
        for i in 0..20usize {
            guard.tags.insert(
                format!("LongTagName{i}"),
                etherip_mock::TagValue {
                    type_code: etherip_mock::TYPE_DINT,
                    data: (i as i32 + 1).to_le_bytes().to_vec(),
                },
            );
        }
    };
    let items: Vec<DriverReadItem> = (0..20)
        .map(|i| item(10 + i, &format!("LongTagName{i}"), Some(DataType::I32)))
        .collect();

    let server = MockServer::start(MockBehavior::new());
    seed_tags(&server);
    let config = format!(
        r#"{{"host":"{}","port":{},"timeout_ms":1000,"max_bytes_per_multi":256}}"#,
        server.addr.ip(),
        server.addr.port()
    );
    let mut driver = create(&config);
    driver.connect().expect("connect 失败");
    let results = driver.read(&items).expect("read 失败");
    assert_eq!(results.len(), 20);
    assert!(
        results.iter().all(|r| r.error.is_none()),
        "拆分后每项仍须成功"
    );
    assert!(server.request_count() > 1, "预算收紧必须拆分");

    // 放宽预算：同规模单请求。
    let server = MockServer::start(MockBehavior::new());
    seed_tags(&server);
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");
    let _ = driver.read(&items).expect("read 失败");
    assert_eq!(server.request_count(), 1, "默认预算下应单请求合并");
}

#[test]
fn preserves_per_item_errors_in_same_multi() {
    // 缺失标签（status 0x14）与正常标签共存同一 Multi，逐项独立标记。
    let behavior = MockBehavior::new().with_dint("Exists", 42);
    let server = MockServer::start(behavior);
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "Exists", Some(DataType::I32)),
            item(2, "Missing.Tag", Some(DataType::I32)),
        ])
        .expect("read 不应整体失败");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Some(RawValue::I64(42)), "正常项成功");
    assert!(results[1].error.is_some(), "缺失项失败");
    assert_eq!(results[1].error.as_ref().unwrap().code, "cip_item_error");
}

#[test]
fn timeout_returns_retryable_error_and_recovers() {
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&etherip_mock::tcp_config(&server, 100));
    driver.connect().expect("connect 应在延迟注入前成功");

    server.behavior().lock().unwrap().response_delay = Some(Duration::from_millis(600));
    let err = error_info(
        driver
            .read(&[item(1, "T", Some(DataType::I32))])
            .expect_err("响应延迟超期必须超时"),
    );
    assert_eq!(err.code, "timeout");
    assert!(err.retryable);

    server.behavior().lock().unwrap().response_delay = None;
    let _ = driver
        .read(&[item(1, "T", Some(DataType::I32))])
        .expect("恢复后应读取成功");
}

#[test]
fn connection_drop_then_re_register() {
    let behavior = MockBehavior::new().with_dint("T", 7);
    let server = MockServer::start(behavior);
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    server.behavior().lock().unwrap().drop_connection = true;
    let err = error_info(
        driver
            .read(&[item(1, "T", Some(DataType::I32))])
            .expect_err("断线必须失败"),
    );
    assert_eq!(err.code, "connection_lost");

    server.behavior().lock().unwrap().drop_connection = false;
    let results = driver
        .read(&[item(1, "T", Some(DataType::I32))])
        .expect("重连后应成功");
    assert_eq!(results[0].value, Some(RawValue::I64(7)));
    assert!(
        server.registered_sessions() >= 2,
        "断线重连必须重新 RegisterSession"
    );
}

#[test]
fn desync_injections_fail_whole_call_then_recover() {
    type Knob = fn(&mut MockBehavior);
    let cases: [(&str, Knob); 3] = [
        ("wrong_session_handle", |b: &mut MockBehavior| {
            b.wrong_session_handle = true
        }),
        ("wrong_sender_context", |b: &mut MockBehavior| {
            b.wrong_sender_context = true
        }),
        ("declare_wrong_item_count", |b: &mut MockBehavior| {
            b.declare_wrong_item_count = true;
        }),
    ];
    for (name, setup) in cases {
        let behavior = MockBehavior::new().with_dint("T", 9);
        let server = MockServer::start(behavior);
        let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
        driver.connect().expect("connect 失败");
        setup(&mut server.behavior().lock().unwrap());

        let err = error_info(
            driver
                .read(&[item(1, "T", Some(DataType::I32))])
                .expect_err("{name} 必须整体失败"),
        );
        assert!(
            matches!(
                err.code.as_str(),
                "invalid_response" | "unexpected_command_code" | "enip_error_response"
            ),
            "{name} 应映射失步类错误，得到 {err:?}"
        );

        // 解除注入：会话丢弃后自动重连恢复。
        {
            let behavior = server.behavior();
            let mut guard = behavior.lock().unwrap();
            guard.wrong_session_handle = false;
            guard.wrong_sender_context = false;
            guard.declare_wrong_item_count = false;
        }
        let results = driver
            .read(&[item(1, "T", Some(DataType::I32))])
            .unwrap_or_else(|_| panic!("{name} 解除后应恢复"));
        assert_eq!(results[0].value, Some(RawValue::I64(9)));
    }
}

// ---------------------------------------------------------------- 写链路

#[test]
fn writes_bool_int_real_and_reads_back_with_discovery() {
    let server = MockServer::start(
        MockBehavior::new()
            .with_bool("Motor.Run", true)
            .with_dint("Set.Point", -500)
            .with_real("Temp.PV", 36.5),
    );
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "Motor.Run", RawValue::Bool(false)),
            write_item(2, "Set.Point", RawValue::I64(-1234)),
            write_item(3, "Temp.PV", RawValue::F64(37.25)),
        ])
        .expect("写入失败");
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.success), "{results:?}");

    // 首写触发类型发现：captured_reads 含三个标签的 Read 记录。
    let discovered: Vec<String> = server
        .read_records()
        .iter()
        .map(|r| r.tag.clone())
        .collect();
    assert!(
        discovered.contains(&"Motor.Run".to_owned()),
        "首写必须先发现类型"
    );
    assert!(discovered.contains(&"Set.Point".to_owned()));
    assert!(discovered.contains(&"Temp.PV".to_owned()));

    // 读回验证映像生效。
    let results = driver
        .read(&[
            item(1, "Motor.Run", Some(DataType::Bool)),
            item(2, "Set.Point", Some(DataType::I32)),
            item(3, "Temp.PV", Some(DataType::F32)),
        ])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::Bool(false)));
    assert_eq!(results[1].value, Some(RawValue::I64(-1234)));
    assert_eq!(results[2].value, Some(RawValue::F64(37.25)));
}

#[test]
fn writes_packed_into_single_multi() {
    let server = MockServer::start(MockBehavior::new().with_uint("A", 0).with_uint("B", 0));
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "A", RawValue::U64(111)),
            write_item(2, "B", RawValue::U64(222)),
        ])
        .expect("写入失败");
    assert!(results.iter().all(|r| r.success));
    // 类型发现 1 次 Multi + 写入 1 次 Multi = 至少 2 次请求；
    // 关键断言：两条写合并进同一条 Write Multi（而非各一包）。
    assert_eq!(server.request_count(), 2, "发现+写入各一包");
    assert_eq!(server.write_records().len(), 2);
    assert_eq!(server.tag_value("A").map(|t| t.data), Some(vec![111, 0]));
    assert_eq!(server.tag_value("B").map(|t| t.data), Some(vec![222, 0]));
}

#[test]
fn writes_out_of_range_and_denied_per_item() {
    let server = MockServer::start(
        MockBehavior::new()
            .with_uint("Counter", 0)
            .with_uint("Locked", 5),
    );
    server
        .behavior()
        .lock()
        .unwrap()
        .deny_writes_at
        .insert("Locked".to_owned());
    let mut driver = create(&etherip_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "Counter", RawValue::U64(70_000)),
            write_item(2, "Locked", RawValue::U64(1)),
        ])
        .expect("部分失败不应整体报错");
    assert_eq!(results.len(), 2);
    assert!(
        results[0]
            .error
            .as_ref()
            .is_some_and(|e| e.code == "invalid_type"),
        "UINT 值域越界须 invalid_type：{results:?}"
    );
    assert!(
        results[1]
            .error
            .as_ref()
            .is_some_and(|e| e.code == "access_denied"),
        "写保护标签须 access_denied"
    );
    assert_eq!(
        server.tag_value("Locked").map(|t| t.data),
        Some(vec![5, 0]),
        "拒绝不得改写"
    );
}

// ---------------------------------------------------------------- 能力声明

#[test]
fn capabilities_declare_read_and_write() {
    use driver_sdk::abi::envelope::CapabilitiesEnvelope;
    use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiStr};

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

    let server = MockServer::start(MockBehavior::new());
    let config = etherip_mock::tcp_config(&server, 1000);
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
    assert!(envelope.capabilities.write, "V0.3 写能力必须声明");
    assert!(envelope.capabilities.batch_read);
    assert!(envelope.capabilities.batch_write);
    assert!(!envelope.capabilities.subscription);

    unsafe { (api.destroy)(handle) };
}
