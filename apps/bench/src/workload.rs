//! workload 生成：Profile JSON 批量生成 + `CollectorConfig` 程序化构造
//! + YAML 落盘（§34.2 标准 workload：设备 × 点数 × 周期参数化）。
//!
//! 地址布局（§34.2 "address layout: contiguous/batchable"）：每设备一份
//! Profile——Modbus 地址 `{unit}!40001..` 的 unit 编码在地址字符串内
//! （driver 无连接级 unit 覆盖），故 N 台设备需要 N 个 Profile；每设备
//! 的 100 个属性为连续保持寄存器，驱动按相邻批量合并为每周期 1 次 FC03。
//! unit 为 u8，设备数上限 250（§34.2 workload 100 台，余量充足）。

use std::io;
use std::path::Path;

use collector::config::{
    AbiSpec, BufferOptions, CollectorConfig, DeviceSpec, DriverSpec, ManifestSpec, MqttOptions,
    NorthboundConfig, PipelineOptions, RestOptions,
};
use serde_json::json;

pub const SITE_ID: &str = "bench-site";
/// 设备数硬上限（unit 号为 u8，0 保留给广播语义）。
pub const MAX_DEVICES: usize = 250;

/// workload 形状参数。
#[derive(Debug, Clone)]
pub struct WorkloadPlan {
    pub devices: usize,
    pub props_per_device: usize,
    pub interval_ms: u64,
    /// pipeline 冲刷间隔（毫秒）：调大可让单设备多轮采集聚合进同一批
    /// （「单批 ≥1000 Observation」验收的可观测前提）。
    pub flush_interval_ms: u64,
}

/// 生成结果：配置文件路径。
#[derive(Debug)]
pub struct WorkloadPaths {
    pub config_path: std::path::PathBuf,
}

/// 校验形状参数（unit 上限与点表规模）。
pub fn validate_plan(plan: &WorkloadPlan) -> Result<(), String> {
    if plan.devices == 0 || plan.devices > MAX_DEVICES {
        return Err(format!("设备数必须在 1..={}（unit 号为 u8）", MAX_DEVICES));
    }
    if plan.props_per_device == 0 || plan.props_per_device > 10_000 {
        return Err("每设备属性数必须在 1..=10000".to_owned());
    }
    if plan.interval_ms == 0 {
        return Err("采集周期必须大于 0ms".to_owned());
    }
    Ok(())
}

/// 生成环境：workload 内容之外的装配地址与路径（Modbus 模拟器、北向
/// broker、REST 端口、cdylib 绝对路径）。
pub struct GenerateEnv<'a> {
    pub dir: &'a Path,
    pub modbus_addr: std::net::SocketAddr,
    pub mqtt_host: &'a str,
    pub mqtt_port: u16,
    pub rest_port: u16,
    pub plugin_path: &'a Path,
}

/// 生成全部 workload 文件并返回配置路径。
pub fn generate(env: &GenerateEnv<'_>, plan: &WorkloadPlan) -> io::Result<WorkloadPaths> {
    let profiles_dir = env.dir.join("profiles");
    std::fs::create_dir_all(&profiles_dir)?;
    for i in 0..plan.devices {
        let unit = (i + 1) as u8;
        let profile = profile_json(unit, plan.props_per_device, plan.interval_ms);
        std::fs::write(
            profiles_dir.join(format!("bench-{unit:03}.json")),
            serde_json::to_vec(&profile).expect("Profile JSON 序列化"),
        )?;
    }

    let connection: serde_json::Value = serde_json::from_str(&modbus_mock::tcp_config_at(
        &env.modbus_addr.ip().to_string(),
        env.modbus_addr.port(),
        1000,
    ))
    .expect("连接配置 JSON");

    let devices: Vec<DeviceSpec> = (0..plan.devices)
        .map(|i| {
            let unit = (i + 1) as u8;
            DeviceSpec {
                id: device_id(unit),
                name: None,
                domain: None,
                driver: "modbus-tcp".to_owned(),
                profile: format!("bench-{unit:03}"),
                connection: connection.clone(),
                enabled: true,
                labels: Default::default(),
            }
        })
        .collect();

    let db_path = env.dir.join("collector-wal.db");
    let config = CollectorConfig {
        site_id: SITE_ID.to_owned(),
        session_id: None,
        profiles_dir,
        driver: Some(DriverSpec {
            plugin: env.plugin_path.to_path_buf(),
            manifest: ManifestSpec {
                id: "modbus-tcp".to_owned(),
                name: "Modbus TCP".to_owned(),
                version: "0.1.0".to_owned(),
                abi: AbiSpec { major: 1, minor: 0 },
            },
        }),
        drivers: Default::default(),
        devices,
        northbound: NorthboundConfig {
            mqtt: MqttOptions {
                broker_host: env.mqtt_host.to_owned(),
                broker_port: env.mqtt_port,
                ..Default::default()
            },
        },
        poll: Default::default(),
        pipeline: PipelineOptions {
            max_batch_size: 1000,
            flush_interval_ms: plan.flush_interval_ms,
            ..Default::default()
        },
        buffer: BufferOptions {
            db_path: db_path.clone(),
            ..Default::default()
        },
        forward_poll_ms: 200,
        rest: RestOptions {
            listen: Some(format!("127.0.0.1:{}", env.rest_port)),
            max_concurrency: 16,
        },
        control: None,
    };

    let config_path = env.dir.join("collector.yaml");
    let yaml = serde_yaml::to_string(&config)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    std::fs::write(&config_path, yaml)?;
    Ok(WorkloadPaths { config_path })
}

/// 设备 ID（合法 MQTT topic 段：小写字母数字连字符）。
pub fn device_id(unit: u8) -> String {
    format!("bench-dev-{unit:03}")
}

/// 单设备 Profile：`props` 个连续保持寄存器（40001 起），周期写入每个
/// 属性的 `default_interval_ms`（§100：采集周期来自 Profile 属性）。
///
/// 属性路径必须满足 Domain 前缀约定（`plc.` 前缀对应 `domain: "plc"`，
/// 否则映射层整批拒绝——Domain Model §42 的标准路径校验）。
fn profile_json(unit: u8, props: usize, interval_ms: u64) -> serde_json::Value {
    let properties: Vec<serde_json::Value> = (0..props)
        .map(|j| {
            json!({
                "path": format!("plc.bench.point{j}"),
                "driver_address": format!("{unit}!{}", 40001 + j as u64),
                "raw_type": "u16",
                "value_type": "f64",
                "unit": null,
                "scale": 1.0,
                "offset": 0.0,
                "write_rounding": "nearest",
                "readable": true,
                "writable": false,
                "default_interval_ms": interval_ms,
                "min": null,
                "max": null,
            })
        })
        .collect();
    let supported: Vec<String> = (0..props).map(|j| format!("plc.bench.point{j}")).collect();
    json!({
        "id": format!("bench-{:03}", unit),
        "vendor": "bench",
        "family": "load",
        "models": ["simulated"],
        "domain": "plc",
        "driver_id": "modbus-tcp",
        "properties": properties,
        "commands": [],
        "capabilities": {
            "supported_properties": supported,
            "supported_commands": [],
            "acquisition": {},
            "limits": {},
        },
    })
}
