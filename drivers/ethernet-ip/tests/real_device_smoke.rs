//! 真实 EtherNet/IP PLC 冒烟测试（默认 ignored，需要本机 127.0.0.1:44818
//! 有真实 Logix PLC 或模拟器；标签名按现场程序调整）。
//!
//! 运行：
//! ```text
//! cargo test -p driver-ether-ip --test real_device_smoke -- --ignored --nocapture
//! ```

mod common;

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use observation_model::DataType;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 44_818;

#[test]
#[ignore = "需要本机 127.0.0.1:44818 有真实 EtherNet/IP PLC"]
fn real_plc_read_smoke() {
    let config = format!(r#"{{"host":"{HOST}","port":{PORT},"timeout_ms":2000}}"#);
    let mut driver = NativeDriver::create(common::load_plugin(), &config).expect("create 失败");
    driver.connect().expect("connect 失败");

    // 已知标签：按现场 PLC 程序调整（此处以 DINT 计数与 REAL 温度为例）。
    let results = driver
        .read(&[
            DriverReadItem {
                id: 1,
                address: "Line1.Count".to_owned(),
                expected_type: Some(DataType::I32),
            },
            DriverReadItem {
                id: 2,
                address: "Temp.PV".to_owned(),
                expected_type: Some(DataType::F32),
            },
        ])
        .expect("read 失败");
    assert_eq!(results.len(), 2);
    for r in &results {
        // 标签不存在（现场未建）按逐项失败暴露——冒烟脚本按现场点表核对。
        assert!(r.error.is_none(), "读取失败：{:?}", r.error);
        assert_eq!(r.protocol_quality_code, Some(0));
        assert!(r.received_timestamp_ns > 0);
    }
    println!("Line1.Count = {:?}", results[0].value);
    println!("Temp.PV = {:?}", results[1].value);
}
