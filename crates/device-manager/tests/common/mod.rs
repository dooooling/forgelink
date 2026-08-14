//! 测试公用：构建并加载 `driver-modbus` cdylib（Native Plugin）。

use std::path::PathBuf;
use std::sync::Arc;

use driver_loader::NativePlugin;
use driver_sdk::DriverManifest;
use driver_sdk::abi::ENTRY_SYMBOL;

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
///
/// 使用 `OnceLock` 保证同一测试进程内只触发一次构建；产物缺失时构建，
/// 已是最新时 cargo 直接返回，嵌套构建安全。
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

/// 加载 Modbus Native Plugin（Manifest 与 `driver-modbus` 保持一致）。
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
