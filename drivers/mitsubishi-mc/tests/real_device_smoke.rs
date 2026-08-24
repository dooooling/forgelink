//! 真实三菱 PLC 冒烟测试（默认 ignored，需要本机 127.0.0.1:6006 有真实
//! MELSEC PLC 或模拟器；路由区参数与点表按现场调整）。
//!
//! 运行：
//! ```text
//! cargo test -p driver-mitsubishi-mc --test real_device_smoke -- --ignored --nocapture
//! ```
//!
//! 现场调整指引：FX5U/Q CPU 直连典型 module_io=0x03FF、PC 号 0；
//! 经 CC-Link/Ethernet 模块中转时按模块实际 I/O 与站号配置。

mod common;

use driver_loader::NativeDriver;
use driver_sdk::DriverReadItem;
use observation_model::DataType;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 6_006;

#[test]
#[ignore = "需要本机 127.0.0.1:6006 有真实 MELSEC PLC"]
fn real_plc_read_smoke() {
    let config = format!(r#"{{"host":"{HOST}","port":{PORT},"timeout_ms":2000}}"#);
    let mut driver = NativeDriver::create(common::load_plugin(), &config).expect("create 失败");
    driver.connect().expect("connect 失败");

    // 已知点表：按现场程序调整（此处以 D 寄存器字与 M 位为例）。
    let results = driver
        .read(&[
            DriverReadItem {
                id: 1,
                address: "D500".to_owned(),
                expected_type: Some(DataType::U16),
            },
            DriverReadItem {
                id: 2,
                address: "M10".to_owned(),
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
    println!("D500 = {:?}", results[0].value);
    println!("M10 = {:?}", results[1].value);
}
