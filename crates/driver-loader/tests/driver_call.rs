//! NativeDriver 同步调用适配测试（§17.9 最小函数表、§15）。
//!
//! 依赖 `examples/test_driver_plugin` 的 cdylib 产物，环境要求同
//! `tests/load_plugin.rs`。

use std::path::PathBuf;
use std::sync::Arc;

use driver_loader::{LoaderError, NativeDriver, NativePlugin};
use driver_sdk::abi::ENTRY_SYMBOL;
use driver_sdk::{
    AddressMetadata, DriverCommand, DriverReadItem, DriverWriteItem, HistoryRequest,
    ProtocolCapabilities, RawValue,
};
use observation_model::DataType;

fn plugin_file() -> PathBuf {
    let name = if cfg!(windows) {
        "test_driver_plugin.dll"
    } else {
        "libtest_driver_plugin.so"
    };
    let dir = if let Some(dir) = std::env::var_os("FORGELINK_TEST_PLUGIN_DIR") {
        PathBuf::from(dir)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/examples")
    };
    dir.join(name)
}

fn load_plugin() -> Arc<NativePlugin> {
    let manifest = driver_sdk::DriverManifest {
        id: "test-plugin".to_owned(),
        name: "Test Plugin".to_owned(),
        version: "0.1.0".to_owned(),
        entry: ENTRY_SYMBOL.to_owned(),
        abi: driver_sdk::manifest::AbiVersion { major: 1, minor: 0 },
        platforms: vec![],
    };
    Arc::new(
        NativePlugin::load(&plugin_file(), manifest)
            .expect("加载失败——先执行 cargo build --example test_driver_plugin"),
    )
}

fn create(config: &str) -> NativeDriver {
    NativeDriver::create(load_plugin(), config).expect("create 失败")
}

#[test]
fn create_and_destroy_round_trip() {
    let driver = create("{}");
    drop(driver);
}

#[test]
fn connect_and_disconnect() {
    let mut driver = create("{}");
    driver.connect().expect("connect 失败");
    assert!(driver.is_connected());
    driver.disconnect().expect("disconnect 失败");
    assert!(!driver.is_connected());
}

#[test]
fn protocol_capabilities_parsed_and_cached() {
    let mut driver = create("{}");
    let caps = driver.protocol_capabilities().expect("能力解析失败");
    assert_eq!(*caps, ProtocolCapabilities::default());
    assert!(caps.read && caps.polling);
    // 二次调用命中缓存。
    let caps_again = {
        // 显式作用域结束第一次借用，允许再次 &mut 借用。
        let driver = &mut driver;
        driver.protocol_capabilities().expect("能力解析失败")
    };
    assert!(caps_again.read && caps_again.polling);
}

#[test]
fn validate_address_round_trip() {
    let mut driver = create("{}");
    let meta = driver.validate_address("1!40001").expect("地址校验失败");
    assert_eq!(
        meta,
        AddressMetadata {
            canonical_address: "1!40001".to_owned(),
            raw_type: None,
            readable: true,
            writable: false,
        }
    );
}

#[test]
fn read_returns_results_by_item_id() {
    let mut driver = create("{}");
    let results = driver
        .read(&[
            DriverReadItem {
                id: 1,
                address: "1!40001".to_owned(),
                expected_type: Some(DataType::U16),
            },
            DriverReadItem {
                id: 2,
                address: "1!40002".to_owned(),
                expected_type: None,
            },
        ])
        .expect("read 失败");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].item_id, 1);
    assert_eq!(results[0].value, Some(RawValue::U64(1001)));
    assert_eq!(results[1].item_id, 2);
}

#[test]
fn write_returns_results() {
    let mut driver = create("{}");
    let results = driver
        .write(&[DriverWriteItem {
            id: 7,
            address: "1!40001".to_owned(),
            value: RawValue::U64(42),
        }])
        .expect("write 失败");
    assert_eq!(results.len(), 1);
    assert!(results[0].success);
    assert_eq!(results[0].item_id, 7);
}

#[test]
fn execute_returns_result() {
    let mut driver = create("{}");
    let result = driver
        .execute(&DriverCommand {
            command_id: "reset".to_owned(),
            payload: serde_json::json!({}),
        })
        .expect("execute 失败");
    assert!(result.success);
}

#[test]
fn browse_returns_nodes() {
    let mut driver = create("{}");
    let nodes = driver.browse(None).expect("browse 失败");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, "root");
    let nodes = driver.browse(Some("sub")).expect("browse 失败");
    assert_eq!(nodes.len(), 1);
}

#[test]
fn query_history_returns_page() {
    let mut driver = create("{}");
    let request = HistoryRequest {
        items: vec![DriverReadItem {
            id: 1,
            address: "1!40001".to_owned(),
            expected_type: None,
        }],
        start_time_ns: 0,
        end_time_ns: i64::MAX,
        limit: Some(10),
        continuation: None,
    };
    let page = driver.query_history(&request).expect("query_history 失败");
    assert!(page.items.is_empty());
    assert_eq!(page.continuation, None);
}

#[test]
fn create_failure_reports_detail() {
    let err = NativeDriver::create(load_plugin(), "{\"fail_create\": true}")
        .expect_err("fail_create 配置必须使 create 失败");
    assert!(matches!(
        err,
        LoaderError::CreateFailed {
            detail: Some(ref detail)
        } if detail.code == "CONFIG_INVALID" && !detail.retryable
    ));
    assert_eq!(err.code(), "driver_create_failed");
}

#[test]
fn call_failure_reports_status_and_detail() {
    let mut driver = create("{\"fail_connect\": true}");
    let err = driver
        .connect()
        .expect_err("fail_connect 配置必须使 connect 失败");
    assert!(matches!(
        err,
        LoaderError::CallFailed {
            function: "connect",
            status: -1,
            detail: Some(ref detail),
        } if detail.code == "CONNECT_REFUSED" && detail.retryable
    ));
    assert_eq!(err.code(), "driver_call_failed");
}

#[test]
fn create_failure_with_null_handle() {
    // create 失败且句柄为空：Loader 不得把空句柄传给
    // get_last_error_json / destroy（§17.5 句柄值未定义）。
    let err = NativeDriver::create(load_plugin(), "{\"fail_create_null\": true}")
        .expect_err("fail_create_null 配置必须使 create 失败");
    assert!(matches!(err, LoaderError::CreateFailed { detail: None }));
    assert_eq!(err.code(), "driver_create_failed");
}

#[test]
fn driver_drop_keeps_plugin_alive_until_destroy() {
    // Drop 顺序安全：先 destroy 句柄、后释放插件（库卸载）——
    // 若顺序错误（库先卸载）destroy 会访问已卸载函数表导致崩溃。
    let plugin = load_plugin();
    let driver = NativeDriver::create(plugin.clone(), "{}").expect("create 失败");
    drop(driver);
    drop(plugin);
}
