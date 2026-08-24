//! Mitsubishi MC Driver C ABI 全链路测试（§34.6 质量门槛：经 driver-loader
//! 加载真实 cdylib，连 mc-mock 覆盖地址校验、批量合并、结束代码错误、
//! 超时重连、失步检测与读+写回环）。

mod common;

use std::time::Duration;

use driver_loader::NativeDriver;
use driver_sdk::{AddressMetadata, DriverReadItem};
use mc_mock::{McBehavior, McServer};
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
    let server = McServer::start(McBehavior::new());
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));

    driver.connect().expect("connect 失败");
    assert!(driver.is_connected());

    let meta = driver.validate_address("D200").expect("校验失败");
    assert_eq!(
        meta,
        AddressMetadata {
            canonical_address: "D200".to_owned(),
            raw_type: None,
            readable: true,
            writable: true,
        }
    );
    // HEX 陷阱：X20 是十进制 20，canonical 保持十进制文本。
    let meta = driver.validate_address(" x20 ").expect("校验失败");
    assert_eq!(meta.canonical_address, "X20");
    assert!(!meta.writable, "X 为过程输入只读");

    driver.disconnect().expect("disconnect 失败");
    assert!(!driver.is_connected());
}

#[test]
fn validate_address_rejects_invalid() {
    let server = McServer::start(McBehavior::new());
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    for bad in ["", "T0", "C10", "D200.3", "DX200", "D-5"] {
        let err = driver.validate_address(bad).expect_err("必须被拒绝");
        assert!(
            error_info(err).code.starts_with("invalid"),
            "'{bad}' 应映射 invalid_*"
        );
    }
}

// ---------------------------------------------------------------- 读链路

#[test]
fn reads_merged_into_single_frame() {
    // 连续 D 寄存器 → 单次字批量读（captured_reads 恰 1 条覆盖全区间）。
    let behavior = McBehavior::new()
        .with_d(200, 1000)
        .with_d(201, 2000)
        .with_d(202, 3000);
    let server = McServer::start(behavior);
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "D200", Some(DataType::U16)),
            item(2, "D201", Some(DataType::I16)),
            item(3, "D202", Some(DataType::U16)),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 3);
    assert_eq!(
        server.read_records(),
        vec![mc_mock::ReadRecord {
            code: 0xA8,
            number: 200,
            points: 3,
        }],
        "连续 D 寄存器必须合并为一帧"
    );
    assert_eq!(results[0].value, Some(RawValue::U64(1000)));
    assert_eq!(results[1].value, Some(RawValue::I64(2000)));
    assert_eq!(results[2].value, Some(RawValue::U64(3000)));
    assert_eq!(results[0].protocol_quality_code, Some(0));
}

#[test]
fn reads_across_devices_split_and_bit_word_paths() {
    let behavior = McBehavior::new().with_m(100, true);
    let server = McServer::start(behavior);
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "M100", Some(DataType::Bool)),
            item(2, "D500", Some(DataType::U16)),
        ])
        .expect("read 失败");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Some(RawValue::Bool(true)));
    assert_eq!(results[1].value, Some(RawValue::U64(0)), "未写地址读出恒 0");
    assert_eq!(server.request_count(), 2, "位/字软元件不得合并");
}

#[test]
fn end_code_error_marks_whole_plan_and_session_survives() {
    let server = McServer::start(McBehavior::new());
    server.behavior().lock().unwrap().force_end_code = Some(0xC059); // 软元件代码错
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // MC 结束代码无逐项粒度：非 0 即整计划 item 逐项标记（协议级，
    // 会话保留）——不整体 CallFailed。
    let results = driver
        .read(&[item(1, "D200", Some(DataType::U16))])
        .expect("协议级失败不应整体报错");
    assert_eq!(results.len(), 1);
    assert!(results[0].value.is_none());
    let e = results[0].error.as_ref().expect("必须携带逐项错误");
    assert_eq!(e.code, "mc_error_response");
    assert_eq!(e.protocol_code, Some(0xC059));
    assert_eq!(results[0].protocol_quality_code, Some(0xC059));

    // 解除注入：会话保留（协议级），直接恢复读取。
    server.behavior().lock().unwrap().force_end_code = None;
    let results = driver
        .read(&[item(1, "D200", Some(DataType::U16))])
        .expect("解除后应恢复");
    assert_eq!(results[0].value, Some(RawValue::U64(0)));
}

#[test]
fn timeout_returns_retryable_error_and_recovers() {
    let behavior = McBehavior::new();
    let server = McServer::start(behavior);
    let mut driver = create(&mc_mock::tcp_config(&server, 100));
    driver.connect().expect("connect 应在延迟注入前成功");

    server.behavior().lock().unwrap().response_delay = Some(Duration::from_millis(600));
    let err = error_info(
        driver
            .read(&[item(1, "D200", Some(DataType::U16))])
            .expect_err("响应延迟超期必须超时"),
    );
    assert_eq!(err.code, "timeout");
    assert!(err.retryable);

    server.behavior().lock().unwrap().response_delay = None;
    let _ = driver
        .read(&[item(1, "D200", Some(DataType::U16))])
        .expect("恢复后应读取成功");
}

#[test]
fn connection_drop_then_recover() {
    let behavior = McBehavior::new();
    let server = McServer::start(behavior);
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    server.behavior().lock().unwrap().drop_connection = true;
    let err = error_info(
        driver
            .read(&[item(1, "D200", Some(DataType::U16))])
            .expect_err("断线必须失败"),
    );
    assert_eq!(err.code, "connection_lost");

    server.behavior().lock().unwrap().drop_connection = false;
    let _ = driver
        .read(&[item(1, "D200", Some(DataType::U16))])
        .expect("重连后应成功");
}

#[test]
fn desync_injections_fail_whole_call_then_recover() {
    type Knob = fn(&mut McBehavior);
    let cases: [(&str, Knob); 3] = [
        ("wrong_subheader", |b: &mut McBehavior| {
            b.wrong_subheader = true
        }),
        ("wrong_routing_echo", |b: &mut McBehavior| {
            b.wrong_routing_echo = true
        }),
        ("declare_wrong_data_length", |b: &mut McBehavior| {
            b.declare_wrong_data_length = true;
        }),
    ];
    for (name, setup) in cases {
        let behavior = McBehavior::new();
        let server = McServer::start(behavior);
        let mut driver = create(&mc_mock::tcp_config(&server, 1000));
        driver.connect().expect("connect 失败");
        setup(&mut server.behavior().lock().unwrap());

        let err = error_info(
            driver
                .read(&[item(1, "D200", Some(DataType::U16))])
                .expect_err("{name} 必须整体失败"),
        );
        assert!(
            matches!(
                err.code.as_str(),
                "invalid_response" | "unexpected_subheader"
            ),
            "{name} 应映射失步类错误，得到 {err:?}"
        );

        // 解除注入：会话丢弃后自动重连恢复。
        {
            let guard = server.behavior();
            let mut g = guard.lock().unwrap();
            g.wrong_subheader = false;
            g.wrong_routing_echo = false;
            g.declare_wrong_data_length = false;
        }
        let results = driver
            .read(&[item(1, "D200", Some(DataType::U16))])
            .unwrap_or_else(|_| panic!("{name} 解除后应恢复"));
        assert_eq!(results[0].value, Some(RawValue::U64(0)));
    }
}

// ---------------------------------------------------------------- 写链路

#[test]
fn writes_bool_and_words_read_back() {
    let server = McServer::start(McBehavior::new());
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 写点数由值的最小无损宽度决定（写入无 expected_type，镜像 modbus）：
    // -1234 → 1 点（i16 域内）、1.5f32 可缩窄 → 2 点。
    let results = driver
        .write(&[
            write_item(1, "Y40", RawValue::Bool(true)),
            write_item(2, "D100", RawValue::I64(-1234)),
            write_item(3, "ZR10", RawValue::F64(1.5)),
        ])
        .expect("写入失败");
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.success), "{results:?}");

    // 读回验证——读取点数与写入点数同宽对称。
    let results = driver
        .read(&[
            item(1, "Y40", Some(DataType::Bool)),
            item(2, "D100", Some(DataType::I16)),
            item(3, "ZR10", Some(DataType::F32)),
        ])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::Bool(true)));
    assert_eq!(results[1].value, Some(RawValue::I64(-1234)));
    assert_eq!(results[2].value, Some(RawValue::F64(1.5)));

    // 写捕获：Y 位串 1 点、D 字 1 点、ZR 浮点 2 点。
    let records = server.write_records();
    assert!(records.iter().any(|r| r.code == 0x9D && r.points == 1));
    assert!(records.iter().any(|r| r.code == 0xA8 && r.points == 1));
    assert!(records.iter().any(|r| r.code == 0xB0 && r.points == 2));
}

#[test]
fn writes_adjacent_merged_hole_split() {
    let server = McServer::start(McBehavior::new());
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 精确相邻 D0+D1 → 一帧。
    let results = driver
        .write(&[
            write_item(1, "D0", RawValue::U64(10)),
            write_item(2, "D1", RawValue::U64(20)),
        ])
        .expect("写入失败");
    assert!(results.iter().all(|r| r.success));
    assert_eq!(server.write_records()[0].points, 2, "精确相邻合并为一帧");
    assert_eq!(server.cell(0xA8, 0), 10);
    assert_eq!(server.cell(0xA8, 1), 20);

    // 空洞 D0 与 D2 → 两帧，D1 不被波及。
    let results = driver
        .write(&[
            write_item(3, "D0", RawValue::U64(99)),
            write_item(4, "D2", RawValue::U64(77)),
        ])
        .expect("写入失败");
    assert!(results.iter().all(|r| r.success));
    assert_eq!(server.write_records().len(), 3, "空洞拆分为两帧");
    assert_eq!(server.cell(0xA8, 1), 20, "洞内值不得被改写");
    assert_eq!(server.cell(0xA8, 2), 77);
}

#[test]
fn writes_to_readonly_rejected_per_item() {
    let server = McServer::start(McBehavior::new());
    let mut driver = create(&mc_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[write_item(1, "X20", RawValue::Bool(true))])
        .expect("只读拒绝在规划期预填，不应整体报错");
    assert_eq!(results.len(), 1);
    assert!(!results[0].success);
    assert_eq!(
        results[0].error.as_ref().map(|e| e.code.as_str()),
        Some("invalid_address")
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

    let server = McServer::start(McBehavior::new());
    let config = mc_mock::tcp_config(&server, 1000);
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
