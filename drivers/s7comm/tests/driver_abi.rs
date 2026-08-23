//! S7comm Driver C ABI 全链路测试（§34.6 质量门槛：经 driver-loader 加载
//! 真实 cdylib，连 s7comm-mock 覆盖地址校验、批量合并、逐项错误、超时
//! 重连、失步检测与读+写回环）。

mod common;

use std::time::Duration;

use driver_loader::NativeDriver;
use driver_sdk::{AddressMetadata, DriverReadItem};
use observation_model::{DataType, RawValue};
use s7comm_mock::{MockBehavior, MockServer};

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
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));

    driver.connect().expect("connect 失败");
    assert!(driver.is_connected());
    // mock 必须记录远端 TSAP 的 rack/slot（默认 0/0）。
    assert_eq!(
        server.last_called_tsap(),
        Some((0, 0)),
        "TSAP rack/slot 编码"
    );

    let meta = driver.validate_address("DB10.DBD20").expect("校验失败");
    assert_eq!(
        meta,
        AddressMetadata {
            canonical_address: "db10.dbd20".to_owned(),
            raw_type: None,
            readable: true,
            writable: true,
        }
    );
    // I 区只读。
    let meta = driver.validate_address("iw0").expect("校验失败");
    assert!(!meta.writable);
    assert!(driver.validate_address("mw0").unwrap().writable);

    driver.disconnect().expect("disconnect 失败");
    assert!(!driver.is_connected());
}

#[test]
fn validate_address_rejects_invalid() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    for bad in ["m20", "db0.dbw0", "db1.dbx0.8", "db1.dbq0"] {
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
            r#"{{"host":"{}","port":{},"slot":99}}"#,
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
    assert_eq!(err.code, "config_error", "slot 越界必须 config_error");
}

// ---------------------------------------------------------------- 读链路

#[test]
fn reads_merged_into_single_read_var() {
    // 连续 DBW 地址 → 单次 Read Var（captured_reads 恰 1 条覆盖全区间）。
    let behavior = MockBehavior::new().with_db_bytes(1, 0, &[0x00, 0x64, 0x00, 0x03, 0xFF, 0xFE]);
    let server = MockServer::start(behavior);
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "db1.dbw0", Some(DataType::U16)),
            item(2, "db1.dbw2", Some(DataType::I16)),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 2);
    assert_eq!(
        server.read_records(),
        vec![s7comm_mock::ReadRecord {
            area: s7comm_mock::AREA_DB,
            db: 1,
            start_byte: 0,
            len_bytes: 4,
        }],
        "相邻字必须合并为单条 Any 指针"
    );
    assert_eq!(server.request_count(), 1);
    assert_eq!(results[0].value, Some(RawValue::U64(100)));
    assert_eq!(results[1].value, Some(RawValue::I64(3)));
    assert_eq!(results[0].protocol_quality_code, Some(0));
}

#[test]
fn reads_across_areas_split_into_multiple_pdus() {
    let behavior = MockBehavior::new()
        .with_db_bytes(1, 0, &[0x11])
        .with_mw(20, 0x2244);
    let server = MockServer::start(behavior);
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "db1.dbb0", Some(DataType::U8)),
            item(2, "mw20", Some(DataType::U16)),
        ])
        .expect("read 失败");

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].value,
        Some(RawValue::U64(0x11)),
        "字节按无符号解释"
    );
    assert_eq!(results[1].value, Some(RawValue::U64(0x2244)));
    assert_eq!(server.request_count(), 2, "跨区不得合并");
}

#[test]
fn real_dword_interprets_as_f32_real() {
    // DBD4 = 1.5f32 位型；expected F64 → Real 提升为 F64。
    let behavior = MockBehavior::new().with_db_bytes(1, 4, &[0x3F, 0xC0, 0x00, 0x00]);
    let server = MockServer::start(behavior);
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[item(1, "db1.dbd4", Some(DataType::F64))])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::F64(1.5)));

    // 同一载荷按 I32 解释 → 有符号提升。
    let results = driver
        .read(&[item(1, "db1.dbd4", Some(DataType::I32))])
        .expect("read 失败");
    assert_eq!(
        results[0].value,
        Some(RawValue::I64(i64::from(0x3FC0_0000u32)))
    );
}

#[test]
fn preserves_per_item_errors_on_plan_failure() {
    // 注入 M 区起始项的拒绝返回码：不同区各自成 PDU，逐项返回码独立。
    let behavior = MockBehavior::new()
        .with_db_bytes(1, 0, &[1])
        .with_mw(20, 0x2244);
    let server = MockServer::start(behavior);
    server
        .behavior()
        .lock()
        .unwrap()
        .access_denied_at
        .insert((s7comm_mock::AREA_MARKER, 0, 20), ());
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .read(&[
            item(1, "db1.dbb0", Some(DataType::U8)),
            item(2, "mw20", Some(DataType::U16)),
        ])
        .expect("read 不应整体失败");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Some(RawValue::U64(1)), "未注入项成功");
    assert!(results[1].error.is_some(), "注入项失败");
    assert_eq!(results[1].error.as_ref().unwrap().code, "access_denied");
    assert_eq!(results[1].protocol_quality_code, Some(i64::from(0x07)));
}

#[test]
fn timeout_returns_retryable_error_and_recovers() {
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&s7comm_mock::tcp_config(&server, 100));
    driver.connect().expect("connect 应在延迟注入前成功");

    // 注入响应延迟（> 请求超时）：读取超时。
    server.behavior().lock().unwrap().response_delay = Some(Duration::from_millis(600));
    let err = error_info(
        driver
            .read(&[item(1, "db1.dbw0", Some(DataType::U16))])
            .expect_err("响应延迟超期必须超时"),
    );
    assert_eq!(err.code, "timeout");
    assert!(err.retryable);

    // 解除延迟后恢复（会话已因超时丢弃，自动重连重走完整握手）。
    server.behavior().lock().unwrap().response_delay = None;
    let results = driver
        .read(&[item(1, "db1.dbw0", Some(DataType::U16))])
        .expect("恢复后应读取成功");
    assert!(results.iter().all(|r| r.error.is_none()));
}

#[test]
fn connection_drop_then_reconnect() {
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 断线注入：连接被直接关闭 → connection_lost。
    server.behavior().lock().unwrap().drop_connection = true;
    let err = error_info(
        driver
            .read(&[item(1, "db1.dbw0", Some(DataType::U16))])
            .expect_err("断线必须失败"),
    );
    assert_eq!(err.code, "connection_lost");

    // 解除断线 → 自动重连并读取成功。
    server.behavior().lock().unwrap().drop_connection = false;
    let results = driver
        .read(&[item(1, "db1.dbw0", Some(DataType::U16))])
        .expect("重连后应成功");
    assert!(results.iter().all(|r| r.error.is_none()));
}

#[test]
fn desync_injections_fail_whole_call_then_recover() {
    // 三种失步注入都必须整体失败（invalid_response / unexpected_*），
    // 会话丢弃后解除注入即可恢复。
    // 三种失步注入的启用钩子（同构函数指针数组）。
    type Knob = fn(&mut MockBehavior);
    let cases: [(&str, Knob); 3] = [
        ("wrong_pdu_ref", |b: &mut MockBehavior| {
            b.wrong_pdu_ref = true
        }),
        ("wrong_item_count", |b: &mut MockBehavior| {
            b.declare_wrong_item_count = true
        }),
        ("bad_tpkt_version", |b: &mut MockBehavior| {
            b.bad_tpkt_version = true;
        }),
    ];
    for (name, setup) in cases {
        let behavior = MockBehavior::new().with_db_bytes(1, 0, &[7]);
        let server = MockServer::start(behavior);
        let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
        // 先连接（握手不受注入影响），再启用失步注入。
        driver.connect().expect("connect 失败");
        setup(&mut server.behavior().lock().unwrap());

        let err = error_info(
            driver
                .read(&[item(1, "db1.dbb0", Some(DataType::U8))])
                .expect_err("{name} 必须整体失败"),
        );
        assert_eq!(
            err.code, "invalid_response",
            "{name} 应映射 invalid_response"
        );

        // 解除注入：会话丢弃后自动重连恢复。
        server.behavior().lock().unwrap().bad_tpkt_version = false;
        server.behavior().lock().unwrap().wrong_pdu_ref = false;
        server.behavior().lock().unwrap().declare_wrong_item_count = false;
        let results = driver
            .read(&[item(1, "db1.dbb0", Some(DataType::U8))])
            .unwrap_or_else(|_| panic!("{name} 解除后应恢复"));
        assert_eq!(results[0].value, Some(RawValue::U64(7)));
    }
}

#[test]
fn pdu_negotiation_shrinks_chunks() {
    // offered=80 → 预算收紧，15 个连续字必须拆分多次请求；
    // 默认 480 时同区间单请求完成。
    let words: Vec<u8> = (0..30u16).map(|i| (i % 251) as u8).collect();
    let behavior = MockBehavior::new().with_db_bytes(1, 0, &words);
    let server = MockServer::start(behavior);
    server.behavior().lock().unwrap().offered_pdu_size = 80;
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let items: Vec<DriverReadItem> = (0..15)
        .map(|i| {
            item(
                100 + i as u64,
                &format!("db1.dbw{}", i * 2),
                Some(DataType::U16),
            )
        })
        .collect();
    let results = driver.read(&items).expect("read 失败");
    assert_eq!(results.len(), 15);
    assert!(
        results.iter().all(|r| r.error.is_none()),
        "拆分后每项仍须成功：{:?}",
        results.iter().find_map(|r| r.error.as_ref())
    );
    let shrunk_requests = server.request_count();
    assert!(shrunk_requests > 1, "预算收紧必须拆分（{shrunk_requests}）");

    // 放宽协商上限：同区间单请求。
    let behavior = MockBehavior::new().with_db_bytes(1, 0, &words);
    let server = MockServer::start(behavior);
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");
    let items: Vec<DriverReadItem> = (0..15)
        .map(|i| {
            item(
                100 + i as u64,
                &format!("db1.dbw{}", i * 2),
                Some(DataType::U16),
            )
        })
        .collect();
    let _ = driver.read(&items).expect("read 失败");
    assert_eq!(server.request_count(), 1, "默认预算下应单请求合并");
}

// ---------------------------------------------------------------- 写链路

#[test]
fn writes_bit_word_dword_and_reads_back() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    let results = driver
        .write(&[
            write_item(1, "db5.dbx0.3", RawValue::Bool(true)),
            write_item(2, "db5.dbw10", RawValue::U64(5000)),
            write_item(3, "md8", RawValue::I64(-7040)),
        ])
        .expect("写入失败");
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.success));

    // 读回验证映像生效。
    let results = driver
        .read(&[
            item(1, "db5.dbx0.3", Some(DataType::Bool)),
            item(2, "db5.dbw10", Some(DataType::U16)),
            item(3, "md8", Some(DataType::I32)),
        ])
        .expect("read 失败");
    assert_eq!(results[0].value, Some(RawValue::Bool(true)));
    assert_eq!(results[1].value, Some(RawValue::U64(5000)));
    assert_eq!(results[2].value, Some(RawValue::I64(-7040)));
}

#[test]
fn writes_adjacent_words_merged_single_pdu() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 精确相邻的两个字 → 合并为一条 WORD×2 写请求。
    let results = driver
        .write(&[
            write_item(1, "db9.dbw0", RawValue::U64(0x0102)),
            write_item(2, "db9.dbw2", RawValue::U64(0x0304)),
        ])
        .expect("写入失败");
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.success));

    let records = server.write_records();
    assert_eq!(
        records.len(),
        1,
        "精确相邻的同类写项必须合并为单条 Any 指针"
    );
    assert_eq!(records[0].start_byte, 0);
    assert_eq!(records[0].data, vec![0x01, 0x02, 0x03, 0x04]);

    // 非连续（中间留空洞）：拆分为两条独立写，中间字节保持原值 0。
    let results = driver
        .write(&[
            write_item(3, "db9.dbw10", RawValue::U64(0xAAAA)),
            write_item(4, "db9.dbw14", RawValue::U64(0xBBBB)),
        ])
        .expect("写入失败");
    assert!(results.iter().all(|r| r.success));
    assert_eq!(server.write_records().len(), 3, "空洞拆分为两条独立请求");
    // 洞内字节从未被写入：稀疏映像保持未设置（读取语义为 0）。
    assert_eq!(
        server.byte(s7comm_mock::AREA_DB, 9, 12),
        None,
        "洞内不得被覆盖"
    );
}

#[test]
fn writes_out_of_range_and_readonly_rejected_per_item() {
    let server = MockServer::start(MockBehavior::new());
    let mut driver = create(&s7comm_mock::tcp_config(&server, 1000));
    driver.connect().expect("connect 失败");

    // 字目标越界（65535 > i16 上限按 Tag 符号判定——U64 载体 40000 合法、
    // I64 载体 -1 非法）。
    let results = driver
        .write(&[
            write_item(1, "db1.dbw0", RawValue::U64(40_000)),
            write_item(2, "db1.dbw0", RawValue::I64(-1)),
            write_item(3, "iw0", RawValue::U64(5)),
        ])
        .expect("部分失败不应整体报错");
    assert_eq!(results.len(), 3);
    assert!(results[0].success, "U64 载体 40000 在 u16 域内合法");
    assert!(
        results.iter().any(|r| !r.success && r.error.is_some()),
        "越界/只读项必须失败"
    );
}

// ---------------------------------------------------------------- 能力声明

#[test]
fn capabilities_declare_read_and_write() {
    use driver_sdk::abi::envelope::CapabilitiesEnvelope;
    use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiStr};

    // Loader 未暴露能力声明便捷方法：直接加载 cdylib 调用 ABI 入口校验。
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
    let config = s7comm_mock::tcp_config(&server, 1000);
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
    assert!(envelope.capabilities.write, "V0.2 写能力必须声明");
    assert!(envelope.capabilities.batch_read);
    assert!(envelope.capabilities.batch_write);
    assert!(!envelope.capabilities.subscription);
    assert!(!envelope.capabilities.history);

    unsafe { (api.destroy)(handle) };
}
