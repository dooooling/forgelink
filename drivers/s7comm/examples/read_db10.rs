//! S7 真机读写示例：连接 127.0.0.1:102 的 PLC，对指定地址执行
//! 「写测试值 → 读回 → 打印」或纯读取。
//!
//! 运行（先构建驱动 cdylib）：
//! ```text
//! cargo build -p driver-s7comm
//! cargo run -p driver-s7comm --example read_db10 -- <子命令> [参数...]
//! ```
//!
//! 子命令：
//! ```text
//! read   <address> <type>            读一次（type: bit|byte|word|dword）
//! watch  <address> <type> [间隔秒]   周期读取打印（默认 2s）
//! write  <address> <type> <value>    写入后立即读回验证
//! ```

use std::sync::Arc;
use std::time::Duration;

use driver_loader::{NativeDriver, NativePlugin};
use driver_sdk::abi::ENTRY_SYMBOL;
use driver_sdk::{DriverManifest, DriverReadItem, DriverWriteItem};
use observation_model::{DataType, RawValue};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first().cloned() else {
        print_usage_and_exit();
    };
    // 统一为 (地址, 类型, 可选写入值)：read/watch 时 value 为 None。
    let (address, ty, value) = match cmd.as_str() {
        "read" => (
            args.get(1).expect("缺地址").clone(),
            args.get(2).map(String::as_str).unwrap_or("word").to_owned(),
            None,
        ),
        "watch" => {
            let address = args.get(1).expect("缺地址").clone();
            let ty = args.get(2).map(String::as_str).unwrap_or("word").to_owned();
            let interval: u64 = args
                .get(3)
                .map_or(Ok(2), |s| s.parse())
                .expect("间隔须为数字");
            watch_loop(&address, &ty, interval);
            return;
        }
        "write" => (
            args.get(1).expect("缺地址").clone(),
            args.get(2).expect("缺类型").clone(),
            Some(
                args.get(3)
                    .expect("缺写入值")
                    .parse::<f64>()
                    .expect("值须为数字"),
            ),
        ),
        _ => print_usage_and_exit(),
    };
    let value_ref = value.as_ref();
    let (s7_address, expected) = build_address(&address, &ty);

    let mut driver = connect();
    if let Some(v) = value_ref {
        // 写 → 读回验证。
        let raw = encode_value(&ty, *v);
        let result = driver
            .write(&[DriverWriteItem {
                id: 1,
                address: s7_address.clone(),
                value: raw,
            }])
            .unwrap_or_else(|e| panic!("write 失败：{e}"));
        println!(
            "[{}] write {address} = {v} → {}",
            timestamp(),
            if result[0].success {
                "成功"
            } else {
                "失败"
            },
        );
    }

    let results = driver
        .read(&[DriverReadItem {
            id: 1,
            address: s7_address.clone(),
            expected_type: Some(expected),
        }])
        .unwrap_or_else(|e| panic!("read 失败：{e}"));
    match results.first().and_then(|r| r.value.clone()) {
        Some(v) => println!("[{}] read {address} = {:?}", timestamp(), v),
        None => panic!(
            "read 返回错误：{:?}",
            results.first().and_then(|r| r.error.clone())
        ),
    }
}

fn watch_loop(address: &str, ty: &str, interval_secs: u64) {
    let (s7_address, expected) = build_address(address, ty);
    let mut driver = connect();
    println!("已连接 —— 每 {interval_secs}s 读取 {s7_address}，Ctrl+C 退出");
    loop {
        match driver.read(&[DriverReadItem {
            id: 1,
            address: s7_address.clone(),
            expected_type: Some(expected.clone()),
        }]) {
            Ok(results) => match results.first().and_then(|r| r.value.clone()) {
                Some(v) => println!("[{}] {s7_address} = {v:?}", timestamp()),
                None => println!(
                    "[{}] 错误：{:?}",
                    timestamp(),
                    results.first().and_then(|r| r.error.clone())
                ),
            },
            Err(e) => println!("[{}] 调用失败：{e}", timestamp()),
        }
        std::thread::sleep(Duration::from_secs(interval_secs.max(1)));
    }
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "用法：read_db10 <read|watch|write> ...\n  read  <addr> <bit|byte|word|dword>\n  watch <addr> <type> [间隔秒]\n  write <addr> <type> <数值>\n示例：read_db10 write db10.dbw0 word 1234"
    );
    std::process::exit(2);
}

/// 地址 + 类型 → S7 地址文本与期望 DataType。
fn build_address(addr: &str, ty: &str) -> (String, DataType) {
    let a = addr.trim().to_ascii_lowercase();
    let expected = match ty {
        "bit" => DataType::Bool,
        "byte" => DataType::U8,
        "dword" => DataType::U32,
        _ => DataType::U16,
    };
    // bit 类型时若用户给的是裸字地址（如 m0），补 .0 位偏移提示由驱动拒绝——
    // 位读必须显式带位号（m0.0 / db10.dbx0.0）。
    (a, expected)
}

/// 按类型构造写入值（宽 Tag，驱动按目标宽度收窄）。
fn encode_value(ty: &str, v: f64) -> RawValue {
    match ty {
        "bit" => RawValue::Bool(v != 0.0),
        "dword" => RawValue::U64(v as u64),
        // word/byte 走 I64 宽 Tag。
        _ => RawValue::I64(v as i64),
    }
}

fn connect() -> NativeDriver {
    let plugin_path = if cfg!(windows) {
        "target/debug/driver_s7comm.dll"
    } else {
        "target/debug/libdriver_s7comm.so"
    };
    let manifest = DriverManifest {
        id: "s7comm".to_owned(),
        name: "Siemens S7comm".to_owned(),
        version: "0.1.0".to_owned(),
        entry: ENTRY_SYMBOL.to_owned(),
        abi: driver_sdk::manifest::AbiVersion { major: 1, minor: 0 },
        platforms: vec![],
    };
    let plugin = Arc::new(
        NativePlugin::load(std::path::Path::new(plugin_path), manifest).unwrap_or_else(|e| {
            panic!("加载 {plugin_path} 失败（先执行 cargo build -p driver-s7comm）：{e}")
        }),
    );
    let mut driver = NativeDriver::create(
        plugin,
        r#"{"host":"127.0.0.1","port":102,"rack":0,"slot":2,"timeout_ms":2000}"#,
    )
    .expect("create 失败");
    driver.connect().expect("connect 失败");
    driver
}

/// UNIX 毫秒时间戳（打印用）。
fn timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
