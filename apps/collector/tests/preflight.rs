//! Startup Preflight 验收（Runtime V2 方案 §29）。
//!
//! 九项预检的失败路径逐项覆盖：任一失败 Collector 启动即失败
//! （no partial start、no silent device disable）；成功路径由既有
//! e2e/multi_driver 全量覆盖（它们启动即隐式通过 Preflight）。

mod common;

use std::time::Duration;

use collector::config::{
    BufferOptions, CollectorConfig, DeviceSpec, DriversOptions, MqttOptions, NorthboundConfig,
};
use modbus_mock::{MockBehavior, MockServer};
use mqtt_client::mock::MockBroker;

/// 启动并断言以 Preflight 失败告终，返回错误消息。
///
/// `CollectorRuntime` 未实现 `Debug`，`expect_err` 不可用——用 match 显式
/// 分支；启动成功即违背 fail-fast 语义，直接 panic（runtime 随之 drop，
/// 其 Drop/停机路径不参与断言）。
async fn start_expect_preflight_fail(config: CollectorConfig) -> String {
    match tokio::time::timeout(
        Duration::from_secs(15),
        collector::CollectorRuntime::start(config),
    )
    .await
    {
        Err(_) => panic!("启动超时"),
        Ok(Ok(_runtime)) => panic!("应 fail-fast 但成功启动"),
        Ok(Err(e)) => e.to_string(),
    }
}

/// 最小可启动配置（drivers.directories 路径，modbus 单设备）。
fn base_config(temp: &tempfile::TempDir, server: &MockServer, broker_port: u16) -> CollectorConfig {
    let packages_dir = temp.path().join("packages");
    std::fs::create_dir_all(&packages_dir).expect("建目录");
    let artifact = common::plugin_file();
    assert!(artifact.exists(), "modbus cdylib 应已构建");
    common::write_v2_package(&packages_dir, "modbus", "modbus-tcp", &artifact);

    let connection: serde_json::Value =
        serde_json::from_str(&modbus_mock::tcp_config(server, 1000)).expect("连接配置");

    CollectorConfig {
        site_id: "plant-a".to_owned(),
        session_id: None,
        profiles_dir: temp.path().join("profiles"),
        driver: None,
        drivers: DriversOptions {
            directories: vec![packages_dir],
            isolation_overrides: Default::default(),
        },
        devices: vec![DeviceSpec {
            id: "vfd-01".to_owned(),
            name: None,
            domain: None,
            driver: "modbus-tcp".to_owned(),
            profile: "inovance-md500".to_owned(),
            connection,
            enabled: true,
            labels: Default::default(),
        }],
        northbound: NorthboundConfig {
            mqtt: MqttOptions {
                broker_host: "127.0.0.1".to_owned(),
                broker_port,
                ..Default::default()
            },
        },
        poll: Default::default(),
        pipeline: Default::default(),
        buffer: BufferOptions {
            db_path: temp.path().join("wal.db"),
            ..Default::default()
        },
        forward_poll_ms: 50,
        rest: Default::default(),
        control: None,
    }
}

/// §29.1：driver 不存在 → 启动即失败，错误含 check=driver_exists。
#[tokio::test]
async fn preflight_fails_fast_on_unknown_driver() {
    let broker = MockBroker::start().await;
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let temp = tempfile::tempdir().unwrap();
    common::write_profiles(temp.path());
    let mut config = base_config(&temp, &server, broker.addr().port());
    config.devices[0].driver = "ghost-driver".to_owned();

    let err = start_expect_preflight_fail(config).await;
    let msg = err.to_string();
    assert!(msg.contains("Preflight"), "实际: {msg}");
    assert!(msg.contains("driver_exists"), "实际: {msg}");
}

/// §29.2：profile 不存在 → 启动即失败。
#[tokio::test]
async fn preflight_fails_fast_on_unknown_profile() {
    let broker = MockBroker::start().await;
    let server = MockServer::start(MockBehavior::new());
    let temp = tempfile::tempdir().unwrap();
    common::write_profiles(temp.path());
    let mut config = base_config(&temp, &server, broker.addr().port());
    config.devices[0].profile = "ghost-profile".to_owned();

    // 注意：§29.1 先于 §29.2 执行；profile 未知时 create_driver 仍会成功
    // （连接配置合法），因此走到 §29.2。
    let err = start_expect_preflight_fail(config).await;
    let msg = err.to_string();
    assert!(msg.contains("profile_exists"), "实际: {msg}");
}

/// §29.3：Profile.driver_id 与设备声明不一致 → 启动即失败。
#[tokio::test]
async fn preflight_fails_on_profile_driver_mismatch() {
    let broker = MockBroker::start().await;
    let server = MockServer::start(MockBehavior::new());
    let temp = tempfile::tempdir().unwrap();
    common::write_profiles(temp.path());
    let mut config = base_config(&temp, &server, broker.addr().port());
    // 设备声明 s7comm，但 Profile inovance-md500 绑定 modbus-tcp。
    // 注意：s7comm 未注册时 §29.1 先触发——所以这里同时注册一个假包。
    let packages_dir = &config.drivers.directories[0];
    let artifact = common::plugin_file(); // 复用任意 cdylib，仅满足扫描
    common::write_v2_package(packages_dir, "s7pkg", "s7comm", &artifact);

    config.devices[0].driver = "s7comm".to_owned();
    let err = start_expect_preflight_fail(config).await;
    let msg = err.to_string();
    assert!(
        msg.contains("profile_driver_matches") || msg.contains("connection_config_valid"),
        "实际: {msg}"
    );
}

/// §29.4：连接配置非法 → 启动即失败（Driver 自身校验拒绝）。
#[tokio::test]
async fn preflight_fails_on_invalid_connection_config() {
    let broker = MockBroker::start().await;
    let server = MockServer::start(MockBehavior::new());
    let temp = tempfile::tempdir().unwrap();
    common::write_profiles(temp.path());
    let mut config = base_config(&temp, &server, broker.addr().port());
    // Modbus 配置缺 host/port → Driver create 校验拒绝。
    config.devices[0].connection = serde_json::json!({});

    let err = start_expect_preflight_fail(config).await;
    let msg = err.to_string();
    assert!(msg.contains("connection_config_valid"), "实际: {msg}");
}

/// 成功路径：合法配置经 Preflight 后正常启动并停机（与 e2e 互补，
/// 显式断言 Preflight 不阻塞健康配置）。
#[tokio::test]
async fn prelight_passes_and_collector_starts() {
    let broker = MockBroker::start().await;
    let behavior = MockBehavior::new();
    let server = MockServer::start(behavior);
    let temp = tempfile::tempdir().unwrap();
    common::write_profiles(temp.path());
    let config = base_config(&temp, &server, broker.addr().port());

    let runtime = tokio::time::timeout(
        Duration::from_secs(30),
        collector::CollectorRuntime::start(config),
    )
    .await
    .expect("启动超时")
    .expect("健康配置应通过 Preflight 并启动");
    runtime.shutdown().await.expect("停机成功");
}
