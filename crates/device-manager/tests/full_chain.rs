//! Modbus Mock 全链路测试（§33 验收、§47 数据映射流程）。
//!
//! 链路：
//!
//! ```text
//! Mock Modbus TCP server
//!   → driver-modbus cdylib（Native Plugin，C ABI v1）
//!   → device-manager：DeviceManager 注册绑定（§100）
//!   → PollScheduler 周期采集（§22）
//!   → pipeline：RawReadResult → Profile（decode_read）→ Domain → Observation（§47）
//! ```
//!
//! 覆盖：
//!
//! - 设备实例注册（domain/driver_id/profile_id 三级标识，§63）；
//! - 读取项生成与分组（按 `default_interval_ms`，§22）；
//! - 全链路值转换（5000 × 0.01 = 50.0 Hz，§47 示例）；
//! - `Observation` 语义字段（quality、sequence、observation_id 会话嵌入，§7.3、§31.3）；
//! - 绑定错误路径（未知 Profile / Driver 不一致）。

mod common;

use std::time::{Duration, Instant};

use device_manager::{
    DeviceManager, DeviceManagerError, MapContext, NativeDriverFactory, SequenceAllocator,
    map_results,
};
use modbus_mock::{MockBehavior, MockServer};
use observation_model::{Device, DeviceConnection, DomainKind, QualityLevel, Value};
use poll_engine::{PollConfig, PollEvent, PollScheduler};
use profile_engine::{DeviceProfile, ProfileRegistry};
use tokio::sync::mpsc;

use common::load_plugin;

/// 文档 §37 示例风格的 Profile JSON（Inovance MD500 变频器最小子集）。
fn profile_json() -> &'static str {
    r#"{
        "id": "inovance-md500",
        "vendor": "Inovance",
        "family": "MD500",
        "models": ["MD500"],
        "domain": "drive",
        "driver_id": "modbus-tcp",
        "properties": [
            {
                "path": "drive.output.frequency",
                "driver_address": "1!40001",
                "raw_type": "u16",
                "value_type": "f64",
                "unit": "Hz",
                "scale": 0.01,
                "offset": 0.0,
                "write_rounding": "nearest",
                "readable": true,
                "writable": true,
                "default_interval_ms": 50,
                "min": null,
                "max": null
            },
            {
                "path": "drive.output.current",
                "driver_address": "1!40002",
                "raw_type": "u16",
                "value_type": "f64",
                "unit": "A",
                "scale": 0.01,
                "offset": 0.0,
                "write_rounding": "nearest",
                "readable": true,
                "writable": false,
                "default_interval_ms": 50,
                "min": null,
                "max": null
            },
            {
                "path": "drive.run.status",
                "driver_address": "1!coil:1",
                "raw_type": "bool",
                "value_type": "bool",
                "unit": null,
                "scale": 0.0,
                "offset": 0.0,
                "write_rounding": "exact",
                "readable": true,
                "writable": false,
                "default_interval_ms": 100,
                "min": null,
                "max": null
            }
        ],
        "commands": [],
        "capabilities": {
            "supported_properties": [
                "drive.output.frequency",
                "drive.output.current",
                "drive.run.status"
            ],
            "supported_commands": [],
            "acquisition": {},
            "limits": {}
        }
    }"#
}

fn register_profile(registry: &mut ProfileRegistry) {
    let profile: DeviceProfile =
        serde_json::from_str(profile_json()).expect("示例 Profile 应可反序列化");
    registry.register(profile).expect("示例 Profile 应通过校验");
}

/// 构造设备实例（连接配置指向 Mock server）。
fn device(mock: &MockServer) -> Device {
    let config: serde_json::Value =
        serde_json::from_str(&modbus_mock::tcp_config(mock, 1000)).expect("连接配置 JSON");
    Device {
        id: "vfd-01".to_owned(),
        name: "VFD-01".to_owned(),
        domain: DomainKind::Drive,
        driver_id: "modbus-tcp".to_owned(),
        profile_id: "inovance-md500".to_owned(),
        connection: DeviceConnection { config },
        enabled: true,
        labels: Default::default(),
    }
}

/// 构建已注册 vfd-01 的 DeviceManager（Native Plugin 工厂）。
fn manager_with_device(mock: &MockServer) -> DeviceManager {
    let mut registry = ProfileRegistry::new();
    register_profile(&mut registry);
    let mut factory = NativeDriverFactory::new();
    factory.add_plugin(load_plugin()).expect("插件注册成功");
    let mut manager = DeviceManager::new(registry, Box::new(factory), 1000).expect("默认间隔合法");
    manager
        .register_device(device(mock))
        .expect("设备注册应成功");
    manager
}

#[test]
fn registers_device_and_generates_read_groups() {
    let server = MockServer::start(MockBehavior::new());
    let manager = manager_with_device(&server);
    let instance = manager.get("vfd-01").expect("设备已注册");

    // 读取项分组：50ms（frequency + current）与 100ms（status）两组（§22）。
    assert_eq!(instance.groups.len(), 2);
    assert_eq!(instance.groups[0].interval_ms, 50);
    assert_eq!(instance.groups[0].read_items.len(), 2);
    assert_eq!(instance.groups[1].interval_ms, 100);
    assert_eq!(instance.groups[1].read_items.len(), 1);

    // 读取项 → DriverReadItem：地址透传、类型取自 Profile raw_type（§10）。
    let targets = instance.poll_targets();
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].items[0].address, "1!40001");
    assert_eq!(targets[0].items[1].address, "1!40002");

    // item_id 稳定映射回属性。
    assert_eq!(
        instance.item(0).expect("存在").path,
        "drive.output.frequency"
    );
    assert_eq!(instance.item(2).expect("存在").path, "drive.run.status");
    assert!(instance.item(3).is_none());
}

/// 绑定错误路径：Profile 与设备不一致必须拒绝（§72）。
#[test]
fn rejects_binding_mismatches() {
    let server = MockServer::start(MockBehavior::new());
    let mut registry = ProfileRegistry::new();
    register_profile(&mut registry);
    let mut factory = NativeDriverFactory::new();
    factory.add_plugin(load_plugin()).expect("插件注册成功");
    // 同名插件重复注册必须拒绝，且不覆盖已有绑定。
    assert!(factory.add_plugin(load_plugin()).is_err());
    let mut manager = DeviceManager::new(registry, Box::new(factory), 1000).expect("默认间隔合法");

    // 未知 Profile。
    let mut d = device(&server);
    d.profile_id = "no-such".to_owned();
    assert!(matches!(
        manager.register_device(d),
        Err(DeviceManagerError::ProfileNotFound { .. })
    ));

    // Driver 与 Profile 声明不一致（§72：Profile → Driver）。
    let mut d = device(&server);
    d.driver_id = "s7comm".to_owned();
    assert!(matches!(
        manager.register_device(d),
        Err(DeviceManagerError::DriverMismatch { .. })
    ));

    // Domain 与 Profile 声明不一致（§72：Device Instance ↔ Domain）。
    let mut d = device(&server);
    d.domain = DomainKind::Plc;
    assert!(matches!(
        manager.register_device(d),
        Err(DeviceManagerError::DomainMismatch { .. })
    ));

    // 以上失败均不占用注册表。
    assert!(manager.is_empty());
}

/// 全链路：Mock 值 40001=5000、40002=2000、coil:1=true，
/// 经 PollScheduler 周期采集后映射为 50.0 Hz / 20.0 A / true 的 Observation。
#[tokio::test(flavor = "multi_thread")]
async fn full_chain_mock_modbus_to_observations() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[5000, 2000]) // 40001, 40002
        .with_coil_range(1, 0, &[true]);
    let server = MockServer::start(behavior);
    let manager = manager_with_device(&server);
    let instance = manager.get("vfd-01").expect("设备已注册");

    // PollScheduler 调度全部组（§22：每设备一驱动实例、多组共享）。
    let (tx, mut rx) = mpsc::channel(64);
    let mut scheduler = PollScheduler::new();
    for target in instance.poll_targets() {
        scheduler
            .spawn(
                target,
                instance.driver.clone(),
                PollConfig::default(),
                tx.clone(),
            )
            .expect("调度配置合法");
    }

    // 按 interval_ms 收齐两个不同采集组（50ms 与 100ms）各至少一轮：
    // 避免"连续收到同一组两次"造成的假通过。
    // 批次按到达顺序保存（即 sequence 分配顺序，保证后续单调性断言有效）。
    let mut batches: Vec<poll_engine::PollBatch> = Vec::new();
    let mut seen_intervals: std::collections::HashSet<u64> = Default::default();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen_intervals.len() < 2 {
        assert!(Instant::now() < deadline, "等待两个采集组超时");
        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("事件通道应保持打开")
            .expect("事件通道已关闭");
        match event {
            PollEvent::Batch(batch) => {
                if seen_intervals.insert(batch.interval_ms) {
                    batches.push(batch);
                }
            }
            PollEvent::Failed { error, .. } => panic!("采集不应失败: {error:?}"),
        }
    }
    scheduler.shutdown().await;
    assert_eq!(batches.len(), 2, "两个不同采集组都必须产出");

    // 合并两个组的原始结果，全链路映射为 Observation。
    // sequence 由同一分配器按设备统一分配：跨组、跨批次单调递增（§31.3）。
    let ctx = MapContext {
        collector_session_id: "test-session-001".to_owned(),
        ingest_timestamp_ns: 1_700_000_000_000_000_000,
    };
    let mut sequences = SequenceAllocator::new();
    let mut observations = Vec::new();
    for batch in &batches {
        let mapped = map_results(instance, &batch.results, &ctx, &mut sequences)
            .expect("Profile 路径与 Domain 一致");
        observations.extend(mapped);
    }

    // 三个属性都必须出现（空集合循环断言是假通过）。
    let by_path = |path: &str| {
        observations
            .iter()
            .filter(|o| o.path == path)
            .collect::<Vec<_>>()
    };

    // 40001 = 5000，scale 0.01 → 50.0 Hz（§47 示例）。
    let freq = by_path("drive.output.frequency");
    assert!(!freq.is_empty(), "frequency 必须被采集");
    for obs in &freq {
        assert_eq!(obs.value, Some(Value::F64(50.0)));
        assert_eq!(obs.quality.level, QualityLevel::Good);
        assert_eq!(obs.device_id, "vfd-01");
        // observation_id 嵌入 collector_session_id（§31.3 去重）。
        assert!(obs.observation_id.contains("test-session-001"));
        assert!(obs.observation_id.contains("vfd-01"));
    }

    // 40002 = 2000，scale 0.01 → 20.0 A。
    let current = by_path("drive.output.current");
    assert!(!current.is_empty(), "current 必须被采集");
    for obs in &current {
        assert_eq!(obs.value, Some(Value::F64(20.0)));
        assert_eq!(obs.quality.level, QualityLevel::Good);
    }

    // coil:1 = true。
    let status = by_path("drive.run.status");
    assert!(!status.is_empty(), "status 必须被采集");
    for obs in &status {
        assert_eq!(obs.value, Some(Value::Bool(true)));
        assert_eq!(obs.quality.level, QualityLevel::Good);
    }

    // sequence 跨组唯一且单调递增（同设备同会话，§31.3）。
    let seqs: Vec<u64> = observations.iter().map(|o| o.sequence).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(seqs.len(), sorted.len(), "sequence 必须唯一：{seqs:?}");
    assert_eq!(sorted, seqs, "sequence 必须单调递增：{seqs:?}");

    // 所有 Observation 具备时间戳与序列（§8）。
    assert!(observations.iter().all(|o| o.ingest_timestamp_ns > 0));
}
