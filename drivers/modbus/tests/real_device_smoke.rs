//! 真实设备冒烟测试：连接本机 127.0.0.1:502 的 Modbus TCP 服务，按用户提供的
//! 点位表（connection + points，YAML）逐点读取并打印结果。
//!
//! 仅本地手动运行（需要 502 端口有真实 Modbus 服务）：
//!
//! ```text
//! cargo test -p driver-modbus --test real_device_smoke -- --ignored --nocapture
//! ```

mod common;

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use observation_model::DataType;

use common::load_plugin;

/// 连接段（用户 YAML 的 `connection`）：
///
/// ```yaml
/// connection:
///   protocol: modbus-tcp
///   host: 127.0.0.1
///   port: 502
///   unit_id: 1
/// ```
///
/// `word_order: cdab`：本机服务 32 位值低字在前（40003=0x3EFA、40004=0x42C6
/// 拼出 99.123）；Modbus 协议未规定多寄存器字序，此为设备能力差异，
/// 由连接配置表达。
const CONFIG: &str = r#"{
    "mode": "tcp",
    "host": "127.0.0.1",
    "port": 502,
    "unit_id": 1,
    "word_order": "cdab"
}"#;

/// 点位表（用户 YAML 的 `points`），已映射为驱动地址（含默认 unit 1）。
const POINTS: &[(&str, &str, DataType)] = &[
    ("Coil1", "1!coil:1", DataType::Bool),
    ("Coil2", "1!coil:2", DataType::Bool),
    ("Discrete1", "1!discrete:10001", DataType::Bool),
    ("InputReg1", "1!input:30001", DataType::U16),
    ("Holding1", "1!40001", DataType::U16),
    ("Holding2", "1!40002", DataType::U16),
    ("HoldingFloat", "1!40003", DataType::F32),
];

/// 读取全部点位并打印 name -> 值/质量/错误。
#[test]
#[ignore = "需要本机 127.0.0.1:502 有真实 Modbus 服务"]
fn reads_all_points_from_local_modbus() {
    let mut driver = NativeDriver::create(load_plugin(), CONFIG).expect("create 失败");
    driver.connect().expect("connect 127.0.0.1:502 失败");

    let items: Vec<DriverReadItem> = POINTS
        .iter()
        .enumerate()
        .map(|(id, (_, address, data_type))| DriverReadItem {
            id: id as u64 + 1,
            address: (*address).to_owned(),
            expected_type: Some(data_type.clone()),
        })
        .collect();

    let results = driver.read(&items).expect("read 失败");
    assert_eq!(results.len(), POINTS.len(), "必须为每个点位返回结果");

    let mut all_good = true;
    for (point, result) in POINTS.iter().zip(results.iter()) {
        let (name, address, data_type) = point;
        match (&result.value, &result.error) {
            (Some(value), None) => {
                println!("{name:<12} {address:<20} {data_type:?} = {value:?}");
            }
            _ => {
                all_good = false;
                let err = result
                    .error
                    .as_ref()
                    .map(|e| format!("{} (code={}, retryable={})", e.message, e.code, e.retryable))
                    .unwrap_or_else(|| "无错误但无值".to_owned());
                println!("{name:<12} {address:<20} ERROR: {err}");
            }
        }
    }
    assert!(all_good, "存在读取失败的点位，详见上方输出");
    driver.disconnect().expect("disconnect 失败");
}
