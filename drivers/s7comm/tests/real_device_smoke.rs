//! 真实 S7 PLC 冒烟测试（默认 ignored，需要本机 127.0.0.1:102 有真实
//! S7 服务；rack/slot 按目标型号调整）。
//!
//! 运行：
//! ```text
//! cargo test -p driver-s7comm --test real_device_smoke -- --ignored --nocapture
//! ```

mod common;

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use observation_model::DataType;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 102;
/// S7-300/400 典型 slot=2；S7-1200/1500 典型 0。
const RACK: u8 = 0;
const SLOT: u8 = 2;

#[test]
#[ignore = "需要本机 127.0.0.1:102 有真实 S7 服务"]
fn real_plc_read_smoke() {
    let config = format!(
        r#"{{"host":"{HOST}","port":{PORT},"rack":{RACK},"slot":{SLOT},"timeout_ms":2000}}"#
    );
    let mut driver = NativeDriver::create(common::load_plugin(), &config).expect("create 失败");
    driver.connect().expect("connect 失败");

    // 已知点表：按现场实际 DB 调整（此处以 DB1.DBW0 / M10.1 为例）。
    let results = driver
        .read(&[
            DriverReadItem {
                id: 1,
                address: "db1.dbw0".to_owned(),
                expected_type: Some(DataType::U16),
            },
            DriverReadItem {
                id: 2,
                address: "m10.1".to_owned(),
                expected_type: Some(DataType::Bool),
            },
        ])
        .expect("read 失败");
    assert_eq!(results.len(), 2);
    for r in &results {
        assert!(r.error.is_none(), "读取失败：{:?}", r.error);
        assert_eq!(r.protocol_quality_code, Some(0));
        assert!(r.received_timestamp_ns > 0);
    }
    println!("DB1.DBW0 = {:?}", results[0].value);
    println!("M10.1 = {:?}", results[1].value);
}
