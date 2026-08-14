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
    } else if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        // 测试由 cargo 启动时会继承 CARGO_TARGET_DIR，产物与源码构建同目录。
        PathBuf::from(dir).join("debug")
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug")
    };
    dir.join(name)
}

/// 确保 cdylib 产物存在且为最新：`cargo test` 不产出 cdylib，故每个测试
/// 进程统一执行一次增量 `cargo build -p driver-modbus`。
///
/// 不自行判断依赖新鲜度（修改时间只覆盖 `src/*.rs`，Cargo.toml、driver-sdk、
/// observation-model 或构建参数变化时会加载旧产物）；cargo build 本身是
/// 增量的，up-to-date 时秒级返回（测试阶段主 cargo 已释放编译锁，嵌套
/// build 安全）。
///
/// 使用 `OnceLock` 保证同一测试进程内多个测试（如 14 个并行 ABI 测试）只
/// 启动一次 Cargo，避免文件锁竞争与重复编译。
fn ensure_plugin_built() {
    static BUILD_GUARD: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    BUILD_GUARD.get_or_init(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "-p", "driver-modbus"])
            .status()
            .expect("无法启动 cargo build");
        assert!(
            status.success(),
            "cargo build -p driver-modbus 失败（cdylib 产物缺失或过期）"
        );
    });
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
