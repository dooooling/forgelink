//! 共享测试辅助：定位并加载本 crate 的 cdylib 插件（driver_abi /
//! poll_integration / real_device_smoke 共用）。

use std::path::PathBuf;
use std::sync::Arc;

use driver_loader::NativePlugin;
use driver_sdk::DriverManifest;
use driver_sdk::abi::ENTRY_SYMBOL;

/// cdylib 产物文件名（Windows: `.dll`；Linux: `.so`）。
pub fn plugin_file() -> PathBuf {
    let name = if cfg!(windows) {
        "driver_mitsubishi_mc.dll"
    } else {
        "libdriver_mitsubishi_mc.so"
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

/// 确保 cdylib 产物存在且为最新（OnceLock 保证同进程只启动一次 Cargo）。
fn ensure_plugin_built() {
    static BUILD_GUARD: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    BUILD_GUARD.get_or_init(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "-p", "driver-mitsubishi-mc"])
            .status()
            .expect("无法启动 cargo build");
        assert!(
            status.success(),
            "cargo build -p driver-mitsubishi-mc 失败（cdylib 产物缺失或过期）"
        );
    });
}

/// 加载驱动插件（若产物缺失先构建）。
pub fn load_plugin() -> Arc<NativePlugin> {
    ensure_plugin_built();
    let manifest = DriverManifest {
        id: "mitsubishi-mc".to_owned(),
        name: "Mitsubishi MC".to_owned(),
        version: "0.1.0".to_owned(),
        entry: ENTRY_SYMBOL.to_owned(),
        abi: driver_sdk::manifest::AbiVersion { major: 1, minor: 0 },
        platforms: vec![],
    };
    Arc::new(
        NativePlugin::load(&plugin_file(), manifest)
            .expect("cdylib 产物缺失，请先构建 driver-mitsubishi-mc"),
    )
}
