//! Collector 集成测试公用：cdylib 加载、Profile 装配、运行时 Harness。

use std::path::{Path, PathBuf};

use collector::config::CollectorConfig;
use collector::config::{
    AbiSpec, BufferOptions, DeviceSpec, DriverSpec, ManifestSpec, MqttOptions, NorthboundConfig,
};
use modbus_mock::{MockBehavior, MockServer};

/// cdylib 产物文件名（Windows: `.dll`，Linux: `.so`）。
pub fn plugin_file() -> PathBuf {
    let name = if cfg!(windows) {
        "driver_modbus.dll"
    } else {
        "libdriver_modbus.so"
    };
    let dir = if let Some(dir) = std::env::var_os("FORGELINK_TEST_PLUGIN_DIR") {
        PathBuf::from(dir)
    } else if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        PathBuf::from(dir).join("debug")
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug")
    };
    dir.join(name)
}

/// 确保 cdylib 已构建（`cargo test` 不产出 cdylib 时自动 `cargo build -p driver-modbus`）。
/// 进程内统一执行一次（OnceLock）；cargo build 为增量，测试阶段主 cargo
/// 已释放编译锁，嵌套 build 安全（与 `drivers/modbus` 测试同模式）。
fn ensure_plugin_built() {
    static BUILD_GUARD: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    BUILD_GUARD.get_or_init(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "-p", "driver-modbus"])
            .status()
            .expect("无法执行 cargo build");
        assert!(
            status.success(),
            "cargo build -p driver-modbus 失败：cdylib 产物缺失或构建出错"
        );
    });
}

/// 示例 Profile（与 `crates/device-manager` 集成测试同源）。
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

/// 将示例 Profile 写入目录（§38：启动时从目录加载）。
pub fn write_profiles(dir: &Path) {
    std::fs::create_dir_all(dir.join("profiles")).expect("创建 profiles 目录");
    std::fs::write(dir.join("profiles/inovance-md500.json"), profile_json())
        .expect("写入 Profile 文件");
}

/// 测试 Harness：Mock Modbus + 临时目录 + 完整 Collector 配置。
///
/// `broker_port`：MockBroker 监听端口（broker 须由测试在 tokio runtime
/// 内先行启动——其任务依赖调用方 runtime 存活）。
pub struct Harness {
    /// Mock Modbus server（持有以保持监听存活）。
    #[allow(dead_code)]
    pub server: MockServer,
    /// 临时目录（持有以保持配置目录与 WAL 文件存活）。
    #[allow(dead_code)]
    pub temp: tempfile::TempDir,
    pub config: CollectorConfig,
}

impl Harness {
    /// 装配配置（不启动 Collector 运行时）。同步构造；Broker 由测试
    /// `MockBroker::start().await` 启动后传入端口。
    pub fn new(behavior: MockBehavior, broker_port: u16) -> Self {
        ensure_plugin_built();
        let server = MockServer::start(behavior);
        let temp = tempfile::tempdir().expect("临时目录");
        write_profiles(temp.path());

        let connection: serde_json::Value =
            serde_json::from_str(&modbus_mock::tcp_config(&server, 1000)).expect("连接配置 JSON");
        let config = CollectorConfig {
            site_id: "plant-a".to_owned(),
            session_id: None,
            profiles_dir: temp.path().join("profiles"),
            driver: DriverSpec {
                plugin: plugin_file(),
                manifest: ManifestSpec {
                    id: "modbus-tcp".to_owned(),
                    name: "Modbus TCP".to_owned(),
                    version: "0.1.0".to_owned(),
                    abi: AbiSpec { major: 1, minor: 0 },
                },
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
                db_path: temp.path().join("collector-wal.db"),
                ..Default::default()
            },
            forward_poll_ms: 50,
            rest: Default::default(),
        };
        Self {
            server,
            temp,
            config,
        }
    }
}

/// 解析 Telemetry Batch 载荷。
#[allow(dead_code)] // e2e/resilience/rest 各测试按需使用
pub fn parse_batch(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).expect("Telemetry Batch 应可解析")
}

/// 等待（默认 5s 内）条件满足。
pub async fn wait_until<F>(cond: F)
where
    F: FnMut() -> bool,
{
    mqtt_client::mock::wait_until(cond).await
}
