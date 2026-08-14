//! 共享测试辅助：定位并加载本 crate 的 cdylib 插件（driver_abi / poll_integration 共用）。

use std::path::PathBuf;
use std::sync::Arc;

use driver_loader::NativePlugin;
use driver_sdk::DriverManifest;
use driver_sdk::abi::ENTRY_SYMBOL;

/// cdylib 产物文件名（Windows: `.dll`；Linux: `.so`）。
pub fn plugin_file() -> PathBuf {
    let name = if cfg!(windows) {
        "driver_modbus.dll"
    } else {
        "libdriver_modbus.so"
    };
    let dir = if let Some(dir) = std::env::var_os("FORGELINK_TEST_PLUGIN_DIR") {
        PathBuf::from(dir)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug")
    };
    dir.join(name)
}

/// 确保 cdylib 产物存在：`cargo test` 不产出 cdylib，产物缺失时按需
/// `cargo build -p driver-modbus`（测试阶段主 cargo 已释放编译锁，嵌套
/// build 安全；产物一次构建后复用）。
fn ensure_plugin_built() {
    let path = plugin_file();
    if path.exists() {
        return;
    }
    let status = std::process::Command::new("cargo")
        .args(["build", "-p", "driver-modbus"])
        .status()
        .expect("无法启动 cargo build");
    assert!(
        status.success(),
        "cargo build -p driver-modbus 失败（cdylib 产物缺失）"
    );
}

/// 加载驱动插件（若产物缺失先构建）。
pub fn load_plugin() -> Arc<NativePlugin> {
    ensure_plugin_built();
    let manifest = DriverManifest {
        id: "modbus-tcp".to_owned(),
        name: "Modbus TCP".to_owned(),
        version: "0.1.0".to_owned(),
        entry: ENTRY_SYMBOL.to_owned(),
        abi: driver_sdk::manifest::AbiVersion { major: 1, minor: 0 },
        platforms: vec![],
    };
    Arc::new(
        NativePlugin::load(&plugin_file(), manifest)
            .expect("cdylib 产物缺失，请先构建 driver-modbus"),
    )
}
