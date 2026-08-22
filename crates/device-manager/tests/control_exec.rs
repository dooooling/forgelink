//! 控制执行器适配层测试（§82、§88）。
//!
//! 链路：
//!
//! ```text
//! ControlExecutor::write / execute（device-manager 适配层）
//!   → 共享 Driver 会话锁（与 Poll Engine 读取互斥，§82 最后一段）
//!   → driver-modbus cdylib（Native Plugin，C ABI v1）
//!   → Mock Modbus TCP server
//! ```
//!
//! 覆盖：
//!
//! - 写入经适配层到达 mock server 且寄存器值正确（FC06 单点 + FC16 批量合并）；
//! - 写与读共用同一把会话锁：并发读写下不产生协议交错（mock 延迟断言串行性）；
//! - 未知设备 → `Failed` 且错误码稳定；
//! - Driver 整体失败按"请求是否可能已下发"映射 `Failed` / `Indeterminate`
//!   （§80.1，脚本化会话单测）；
//! - 命令执行结果映射与 Modbus `execute` 未声明能力 → `Failed`。

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use control_engine::{ControlExecutor, ExecuteOutcome, WriteOutcome};
use device_manager::{
    BindError, DeviceControlExecutor, DeviceManager, DriverFactory, DriverSession,
};
use driver_sdk::{
    DriverCommand, DriverErrorInfo, DriverReadItem, DriverWriteItem, RawCommandResult,
    RawReadResult, RawWriteResult,
};
use modbus_mock::{Kind, MockBehavior, MockServer};
use observation_model::{Device, DeviceConnection, DomainKind, RawValue};

use common::load_plugin;

/// 文档 §37 示例风格的 Profile JSON（可写频率设定 + 只读电流，最小子集）。
fn modbus_profile_json() -> &'static str {
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
            }
        ],
        "commands": [],
        "capabilities": {
            "supported_properties": ["drive.output.frequency", "drive.output.current"],
            "supported_commands": [],
            "acquisition": {},
            "limits": {}
        }
    }"#
}

/// 脚本化驱动的 Profile（driver_id 指向测试工厂，不依赖网络）。
fn scripted_profile_json() -> &'static str {
    r#"{
        "id": "scripted-profile",
        "vendor": "Test",
        "family": "Scripted",
        "models": ["S1"],
        "domain": "drive",
        "driver_id": "scripted",
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
                "default_interval_ms": 1000,
                "min": null,
                "max": null
            }
        ],
        "commands": [],
        "capabilities": {
            "supported_properties": ["drive.output.frequency"],
            "supported_commands": [],
            "acquisition": {},
            "limits": {}
        }
    }"#
}

fn register_profile(registry: &mut profile_engine::ProfileRegistry, json: &str) {
    let profile: profile_engine::DeviceProfile =
        serde_json::from_str(json).expect("示例 Profile 应可反序列化");
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

/// 构建已注册 vfd-01 的 DeviceManager（Native Plugin 工厂，真实 cdylib 路径）。
fn manager_with_device(mock: &MockServer) -> DeviceManager {
    let mut registry = profile_engine::ProfileRegistry::new();
    register_profile(&mut registry, modbus_profile_json());
    let mut factory = device_manager::NativeDriverFactory::new();
    factory.add_plugin(load_plugin()).expect("插件注册成功");
    let mut manager = DeviceManager::new(registry, Box::new(factory), 1000).expect("默认间隔合法");
    manager
        .register_device(device(mock))
        .expect("设备注册应成功");
    manager
}

// ------------------------------------------------------- 写入到达 mock server

/// FC06 单点写入：经适配层到达 mock server，寄存器值正确、功能码正确。
#[tokio::test(flavor = "multi_thread")]
async fn writes_single_register_via_fc06_and_reaches_mock() {
    let server = MockServer::start(MockBehavior::new());
    let manager = Arc::new(manager_with_device(&server));
    let executor = DeviceControlExecutor::new(Arc::clone(&manager));

    let outcome = executor
        .write(
            &"vfd-01".to_owned(),
            &[DriverWriteItem {
                id: 7,
                address: "1!40001".to_owned(),
                value: RawValue::U64(5000),
            }],
        )
        .await;

    // 结果：逐项原始结果透传，item_id 与入参一致。
    let WriteOutcome::Succeeded(results) = outcome else {
        panic!("FC06 单点写入应成功：{outcome:?}");
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].item_id, 7);
    assert!(
        results[0].success,
        "逐项结果应为成功：{:?}",
        results[0].error
    );

    // 到达 mock server：寄存器值生效（40001 -> 协议偏移 0），功能码 FC06。
    assert_eq!(server.value(1, Kind::HoldingRegister, 0), Some(5000));
    let records = server.write_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].function, 0x06);
    assert_eq!(records[0].start_offset, 0);
    assert_eq!(records[0].quantity, 1);

    // 同一会话回读可见新值（写入落在与读取相同的连接上）。
    let instance = manager.get("vfd-01").expect("设备已注册");
    let driver = instance.driver.clone();
    let after = tokio::task::spawn_blocking(move || {
        let mut guard = driver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.read_batch(&[DriverReadItem {
            id: 0,
            address: "1!40001".to_owned(),
            expected_type: None,
        }])
    })
    .await
    .expect("读取任务不应 panic")
    .expect("回读应成功");
    assert_eq!(after[0].value, Some(RawValue::U64(5000)));
}

/// FC16 批量写入：精确相邻的两个写项合并为一帧 FC16，寄存器值逐项正确。
#[tokio::test(flavor = "multi_thread")]
async fn merges_adjacent_items_into_fc16_batch_write() {
    let server = MockServer::start(MockBehavior::new());
    let manager = Arc::new(manager_with_device(&server));
    let executor = DeviceControlExecutor::new(Arc::clone(&manager));

    // 40101/40102 -> 协议偏移 100/101（精确相邻，驱动合并为 FC16 quantity=2）。
    let outcome = executor
        .write(
            &"vfd-01".to_owned(),
            &[
                DriverWriteItem {
                    id: 1,
                    address: "1!40101".to_owned(),
                    value: RawValue::U64(0x1111),
                },
                DriverWriteItem {
                    id: 2,
                    address: "1!40102".to_owned(),
                    value: RawValue::U64(0x2222),
                },
            ],
        )
        .await;

    let WriteOutcome::Succeeded(results) = outcome else {
        panic!("FC16 批量写入应成功：{outcome:?}");
    };
    assert_eq!(results.len(), 2, "逐项结果必须齐全");
    assert!(
        results.iter().all(|r| r.success),
        "全部逐项结果应为成功：{results:?}"
    );
    let item_ids: Vec<u64> = results.iter().map(|r| r.item_id).collect();
    assert_eq!(item_ids, vec![1, 2]);

    // 合并为一帧 FC16（quantity=2，起始偏移 100），两个寄存器值均生效。
    assert_eq!(server.value(1, Kind::HoldingRegister, 100), Some(0x1111));
    assert_eq!(server.value(1, Kind::HoldingRegister, 101), Some(0x2222));
    let records = server.write_records();
    assert_eq!(records.len(), 1, "相邻地址必须合并为单帧写入");
    assert_eq!(records[0].function, 0x10);
    assert_eq!(records[0].start_offset, 100);
    assert_eq!(records[0].quantity, 2);
}

// ------------------------------------------------------- 会话互斥（§82）

/// 写等待在途读：读取持有共享会话期间发起的写入必须等其结束后才开始，
/// 不产生协议交错（mock 延迟放大占用窗口，按耗时断言串行性）。
#[tokio::test(flavor = "multi_thread")]
async fn write_waits_for_in_flight_read_on_shared_session() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[0])
        .with_response_delay(Duration::from_millis(150));
    let server = MockServer::start(behavior);
    let manager = Arc::new(manager_with_device(&server));
    let executor = DeviceControlExecutor::new(Arc::clone(&manager));
    let instance = manager.get("vfd-01").expect("设备已注册");

    // 在途读取：经 Poll 兼容句柄持锁（与 poll-engine 相同的调用方式），
    // mock 延迟使该读取占用会话约 150ms。
    let driver = instance.driver.clone();
    let read = tokio::task::spawn_blocking(move || {
        let mut guard = driver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.read_batch(&[DriverReadItem {
            id: 0,
            address: "1!40001".to_owned(),
            expected_type: None,
        }])
    });

    // 稍候确保读取已先进入会话，再发起写入。
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = Instant::now();
    let outcome = executor
        .write(
            &"vfd-01".to_owned(),
            &[DriverWriteItem {
                id: 1,
                address: "1!40001".to_owned(),
                value: RawValue::U64(5000),
            }],
        )
        .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(outcome, WriteOutcome::Succeeded(_)),
        "写入应成功：{outcome:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(100),
        "写入必须等待在途读取释放共享会话锁（实际耗时 {elapsed:?}）"
    );
    read.await.expect("读取任务不应 panic").expect("读取应成功");

    // 帧完整性：恰好 2 个请求（在途读 + 写），无重连/重发帧，写入已生效。
    assert_eq!(server.request_count(), 2);
    assert_eq!(server.value(1, Kind::HoldingRegister, 0), Some(5000));
}

/// 读等待在途写：写入持有共享会话期间发起的读取必须等其结束，且在同一
/// 连接上看到写入后的新值（证明读写共用同一会话而非两条独立连接）。
#[tokio::test(flavor = "multi_thread")]
async fn read_waits_for_in_flight_write_on_shared_session() {
    let behavior = MockBehavior::new().with_response_delay(Duration::from_millis(150));
    let server = MockServer::start(behavior);
    let manager = Arc::new(manager_with_device(&server));
    let executor = DeviceControlExecutor::new(Arc::clone(&manager));
    let instance = manager.get("vfd-01").expect("设备已注册");

    // 先发起写入（持有会话约 150ms）。
    let write_task = tokio::spawn({
        let executor = executor.clone();
        async move {
            executor
                .write(
                    &"vfd-01".to_owned(),
                    &[DriverWriteItem {
                        id: 1,
                        address: "1!40001".to_owned(),
                        value: RawValue::U64(5000),
                    }],
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 随后发起读取：必须等待写入释放会话锁。
    let started = Instant::now();
    let driver = instance.driver.clone();
    let read = tokio::task::spawn_blocking(move || {
        let mut guard = driver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.read_batch(&[DriverReadItem {
            id: 0,
            address: "1!40001".to_owned(),
            expected_type: None,
        }])
    })
    .await
    .expect("读取任务不应 panic")
    .expect("读取应成功");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(100),
        "读取必须等待在途写入释放共享会话锁（实际耗时 {elapsed:?}）"
    );
    // 同一会话可见性：读取看到的是写入后的新值。
    assert_eq!(read[0].value, Some(RawValue::U64(5000)));
    assert!(
        matches!(
            write_task.await.expect("写入任务不应 panic"),
            WriteOutcome::Succeeded(_)
        ),
        "写入应成功"
    );
    assert_eq!(server.request_count(), 2);
}

// ------------------------------------------------------- 未知设备

/// 未知设备：`Failed` 且错误码稳定（不 panic、不触碰任何驱动）。
#[tokio::test(flavor = "multi_thread")]
async fn unknown_device_fails_with_stable_error_code() {
    let server = MockServer::start(MockBehavior::new());
    let manager = Arc::new(manager_with_device(&server));
    let executor = DeviceControlExecutor::new(Arc::clone(&manager));

    let write_outcome = executor
        .write(
            &"no-such-device".to_owned(),
            &[DriverWriteItem {
                id: 1,
                address: "1!40001".to_owned(),
                value: RawValue::U64(1),
            }],
        )
        .await;
    let execute_outcome = executor
        .execute(
            &"no-such-device".to_owned(),
            &DriverCommand {
                command_id: "c.start".to_owned(),
                payload: serde_json::json!({}),
            },
        )
        .await;

    let WriteOutcome::Failed(info) = write_outcome else {
        panic!("未知设备的写入必须 Failed：{write_outcome:?}");
    };
    assert_eq!(info.code, "device_not_found", "错误码必须稳定");
    assert!(!info.retryable, "未知设备不可重试");

    let ExecuteOutcome::Failed(cmd_info) = execute_outcome else {
        panic!("未知设备的命令必须 Failed：{execute_outcome:?}");
    };
    assert_eq!(cmd_info.code, "device_not_found", "错误码必须稳定");

    // 稳定性：重复调用错误码一致。
    let again = executor.write(&"no-such-device".to_owned(), &[]).await;
    let WriteOutcome::Failed(again_info) = again else {
        panic!("未知设备的空写入同样必须 Failed：{again:?}");
    };
    assert_eq!(again_info.code, "device_not_found");
    assert_eq!(server.request_count(), 0, "不得触碰任何驱动会话");
}

// ------------------------------------------------------- 错误映射规则（脚本化会话）

/// 脚本化会话行为（测试用例按需注入结果/错误）。
#[derive(Default)]
struct Script {
    /// 下一次 `write_batch` 的返回（None 时视为未预期调用）。
    write: Option<Result<Vec<RawWriteResult>, DriverErrorInfo>>,
    /// 下一次 `execute_command` 的返回。
    execute: Option<Result<RawCommandResult, DriverErrorInfo>>,
}

impl Script {
    fn err(code: &str) -> Result<Vec<RawWriteResult>, DriverErrorInfo> {
        Err(DriverErrorInfo {
            code: code.to_owned(),
            message: format!("脚本化错误 {code}"),
            protocol_code: None,
            retryable: false,
        })
    }

    fn cmd_err(code: &str) -> Result<RawCommandResult, DriverErrorInfo> {
        Err(DriverErrorInfo {
            code: code.to_owned(),
            message: format!("脚本化命令错误 {code}"),
            protocol_code: None,
            retryable: false,
        })
    }
}

/// 脚本化 Driver 会话：把预设脚本原样返回给适配层（错误映射规则测试）。
struct ScriptedSession {
    script: Arc<Mutex<Script>>,
}

impl DriverSession for ScriptedSession {
    fn read_batch(
        &mut self,
        _items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo> {
        Ok(vec![])
    }

    fn write_batch(
        &mut self,
        items: &[DriverWriteItem],
    ) -> Result<Vec<RawWriteResult>, DriverErrorInfo> {
        self.script
            .lock()
            .expect("脚本锁")
            .write
            .take()
            .unwrap_or_else(|| {
                Ok(items
                    .iter()
                    .map(|item| RawWriteResult {
                        item_id: item.id,
                        success: true,
                        protocol_code: Some(0),
                        error: None,
                    })
                    .collect())
            })
    }

    fn execute_command(
        &mut self,
        _command: &DriverCommand,
    ) -> Result<RawCommandResult, DriverErrorInfo> {
        self.script
            .lock()
            .expect("脚本锁")
            .execute
            .take()
            .unwrap_or(Ok(RawCommandResult {
                success: true,
                protocol_code: Some(0),
                payload: None,
                error: None,
            }))
    }
}

/// 脚本化工厂：`driver_id == "scripted"` 时创建共享同一脚本的会话。
struct ScriptedFactory {
    script: Arc<Mutex<Script>>,
}

impl DriverFactory for ScriptedFactory {
    fn create_driver(
        &self,
        driver_id: &str,
        _config: &serde_json::Value,
    ) -> Result<Box<dyn DriverSession>, BindError> {
        match driver_id {
            "scripted" => Ok(Box::new(ScriptedSession {
                script: Arc::clone(&self.script),
            })),
            other => Err(BindError::UnknownDriver {
                driver_id: other.to_owned(),
            }),
        }
    }
}

/// 构建注册了 scripted 设备的管理器与执行器。
fn scripted_manager(script: Arc<Mutex<Script>>) -> Arc<DeviceManager> {
    let mut registry = profile_engine::ProfileRegistry::new();
    register_profile(&mut registry, scripted_profile_json());
    let mut manager = DeviceManager::new(registry, Box::new(ScriptedFactory { script }), 1000)
        .expect("默认间隔合法");
    manager
        .register_device(Device {
            id: "dev-01".to_owned(),
            name: "DEV-01".to_owned(),
            domain: DomainKind::Drive,
            driver_id: "scripted".to_owned(),
            profile_id: "scripted-profile".to_owned(),
            connection: DeviceConnection {
                config: serde_json::json!({}),
            },
            enabled: true,
            labels: Default::default(),
        })
        .expect("脚本化设备注册应成功");
    Arc::new(manager)
}

fn write_item() -> DriverWriteItem {
    DriverWriteItem {
        id: 1,
        address: "1!40001".to_owned(),
        value: RawValue::U64(1),
    }
}

/// 确定的负确认（逐项 success=false）不是整体失败：结果是确定的，
/// 以 `Succeeded` 携带逐项失败透传，由引擎结算部分失败语义。
#[tokio::test(flavor = "multi_thread")]
async fn definitive_negative_ack_maps_succeeded_with_failed_item() {
    let script = Arc::new(Mutex::new(Script {
        write: Some(Ok(vec![RawWriteResult {
            item_id: 1,
            success: false,
            protocol_code: Some(0x02),
            error: Some(DriverErrorInfo {
                code: "modbus_exception".to_owned(),
                message: "Modbus 异常 0x02: 非法数据地址".to_owned(),
                protocol_code: Some(0x02),
                retryable: false,
            }),
        }])),
        execute: None,
    }));
    let executor = DeviceControlExecutor::new(scripted_manager(Arc::clone(&script)));

    let outcome = executor.write(&"dev-01".to_owned(), &[write_item()]).await;
    let WriteOutcome::Succeeded(results) = outcome else {
        panic!("确定的负确认必须以 Succeeded 携带逐项失败透传：{outcome:?}");
    };
    assert_eq!(results.len(), 1);
    assert!(!results[0].success, "逐项失败标志必须保留");
    assert_eq!(results[0].protocol_code, Some(0x02));
}

/// 整体失败按"请求是否可能已下发"分类：
///
/// - `connection_failed`（建连失败，未上线）→ `Failed`；
/// - `timeout`（可能已下发但无应答，§80.1）→ `Indeterminate`；
/// - 插件自定义未知错误码（无法证明未下发）→ 保守 `Indeterminate`。
#[tokio::test(flavor = "multi_thread")]
async fn transport_errors_map_by_certainty() {
    for (code, expect_indeterminate) in [
        ("connection_failed", false),
        ("timeout", true),
        ("vendor_custom_error", true),
    ] {
        let script = Arc::new(Mutex::new(Script {
            write: Some(Script::err(code)),
            execute: None,
        }));
        let executor = DeviceControlExecutor::new(scripted_manager(Arc::clone(&script)));

        let outcome = executor.write(&"dev-01".to_owned(), &[write_item()]).await;
        if expect_indeterminate {
            let WriteOutcome::Indeterminate(info) = outcome else {
                panic!("`{code}` 必须映射 Indeterminate：{outcome:?}");
            };
            assert_eq!(info.code, code, "原始错误码必须保留");
        } else {
            let WriteOutcome::Failed(info) = outcome else {
                panic!("`{code}` 必须映射 Failed：{outcome:?}");
            };
            assert_eq!(info.code, code, "原始错误码必须保留");
        }
    }
}

/// 命令执行结果映射：成功 → `Succeeded`；能力未声明（确定未下发）→
/// `Failed`；超时（可能已下发）→ `Indeterminate`。
#[tokio::test(flavor = "multi_thread")]
async fn command_results_map_to_execute_outcome() {
    let command = DriverCommand {
        command_id: "c.start".to_owned(),
        payload: serde_json::json!({}),
    };

    // 成功。
    let executor = DeviceControlExecutor::new(scripted_manager(Default::default()));
    let outcome = executor.execute(&"dev-01".to_owned(), &command).await;
    let ExecuteOutcome::Succeeded(raw) = outcome else {
        panic!("命令成功必须映射 Succeeded：{outcome:?}");
    };
    assert!(raw.success);

    // 能力未声明：ABI 入口即拒绝，确定未下发 → Failed。
    let script = Arc::new(Mutex::new(Script {
        write: None,
        execute: Some(Script::cmd_err("unsupported")),
    }));
    let executor = DeviceControlExecutor::new(scripted_manager(script));
    let outcome = executor.execute(&"dev-01".to_owned(), &command).await;
    let ExecuteOutcome::Failed(info) = outcome else {
        panic!("unsupported 必须映射 Failed：{outcome:?}");
    };
    assert_eq!(info.code, "unsupported");

    // 超时：可能已下发 → Indeterminate（§80.1）。
    let script = Arc::new(Mutex::new(Script {
        write: None,
        execute: Some(Script::cmd_err("timeout")),
    }));
    let executor = DeviceControlExecutor::new(scripted_manager(script));
    let outcome = executor.execute(&"dev-01".to_owned(), &command).await;
    let ExecuteOutcome::Indeterminate(info) = outcome else {
        panic!("timeout 必须映射 Indeterminate：{outcome:?}");
    };
    assert_eq!(info.code, "timeout");
}

// ------------------------------------------------------- 真实驱动路径

/// 真实 cdylib 路径：Modbus 驱动未声明 `execute` 能力，整体失败为
/// `unsupported`（确定未下发）→ `Failed`。
#[tokio::test(flavor = "multi_thread")]
async fn modbus_execute_unsupported_maps_failed() {
    let server = MockServer::start(MockBehavior::new());
    let manager = Arc::new(manager_with_device(&server));
    let executor = DeviceControlExecutor::new(Arc::clone(&manager));

    let outcome = executor
        .execute(
            &"vfd-01".to_owned(),
            &DriverCommand {
                command_id: "c.start".to_owned(),
                payload: serde_json::json!({}),
            },
        )
        .await;

    let ExecuteOutcome::Failed(info) = outcome else {
        panic!("Modbus execute 未声明能力必须 Failed：{outcome:?}");
    };
    assert_eq!(info.code, "unsupported");
}
