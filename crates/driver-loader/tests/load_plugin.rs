//! NativePlugin 加载与校验测试（§19、§20）。
//!
//! 通过 libloading 加载 `examples/test_driver_plugin` 的 cdylib 产物
//! （`target/debug/examples/`），覆盖 Windows（.dll）与 Linux（.so）双平台。
//!
//! 若测试产物缺失，先执行 `cargo build --example test_driver_plugin`；
//! 使用非默认 target 目录时设置环境变量 `FORGELINK_TEST_PLUGIN_DIR`
//! 指向 example 产物目录。

use std::path::PathBuf;

use driver_loader::{LoaderError, NativePlugin};
use driver_sdk::abi::{ABI_MAJOR, ABI_MINOR, ENTRY_SYMBOL};
use driver_sdk::manifest::{AbiVersion, DriverManifest, platform};

/// example 产物文件名（Windows: `.dll`；Linux: `.so`）。
fn plugin_file() -> PathBuf {
    let name = if cfg!(windows) {
        "test_driver_plugin.dll"
    } else {
        "libtest_driver_plugin.so"
    };
    plugin_dir().join(name)
}

fn plugin_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("FORGELINK_TEST_PLUGIN_DIR") {
        return PathBuf::from(dir);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/examples")
}

fn manifest(entry: &str, major: u16, minor: u16) -> DriverManifest {
    DriverManifest {
        id: "test-plugin".to_owned(),
        name: "Test Plugin".to_owned(),
        version: "0.1.0".to_owned(),
        entry: entry.to_owned(),
        abi: AbiVersion { major, minor },
        platforms: vec![
            if cfg!(windows) {
                platform::WINDOWS_X86_64
            } else {
                platform::LINUX_X86_64
            }
            .to_owned(),
        ],
    }
}

fn load(entry: &str) -> Result<NativePlugin, LoaderError> {
    let path = plugin_file();
    let manifest = manifest(entry, ABI_MAJOR, ABI_MINOR);
    NativePlugin::load(&path, manifest)
}

#[test]
fn loads_valid_plugin() {
    let plugin =
        load(ENTRY_SYMBOL).expect("加载失败——先执行 cargo build --example test_driver_plugin");
    assert_eq!(plugin.manifest().id, "test-plugin");
    assert_eq!(plugin.manifest().abi.major, ABI_MAJOR);
    assert!(plugin.path().is_file());
}

#[test]
fn rejects_missing_entry_symbol() {
    let err = load("no_such_symbol").expect_err("不存在的入口符号必须被拒绝");
    assert!(matches!(err, LoaderError::EntryNotFound { .. }));
    assert_eq!(err.code(), "driver_entry_not_found");
}

#[test]
fn rejects_null_entry() {
    let err = load("forgelink_driver_entry_v1_null").expect_err("空指针入口必须被拒绝");
    assert!(matches!(err, LoaderError::NullEntry { .. }));
    assert_eq!(err.code(), "driver_entry_null");
}

#[test]
fn rejects_abi_major_mismatch() {
    let err = load("forgelink_driver_entry_v1_bad_abi").expect_err("abi_major 不一致必须被拒绝");
    assert!(matches!(err, LoaderError::AbiIncompatible { major: 2, .. }));
    assert_eq!(err.code(), "driver_abi_incompatible");
}

#[test]
fn rejects_abi_minor_beyond_supported() {
    let err = load("forgelink_driver_entry_v1_bad_abi_minor")
        .expect_err("abi_minor 超出支持范围必须被拒绝");
    assert!(matches!(
        err,
        LoaderError::AbiIncompatible { minor: 99, .. }
    ));
    assert_eq!(err.code(), "driver_abi_incompatible");
}

#[test]
fn rejects_small_struct_size() {
    let err =
        load("forgelink_driver_entry_v1_small_struct").expect_err("struct_size 不足必须被拒绝");
    assert!(matches!(err, LoaderError::StructTooSmall { .. }));
    assert_eq!(err.code(), "driver_struct_too_small");
}

#[test]
fn rejects_missing_required_function() {
    let err =
        load("forgelink_driver_entry_v1_missing_function").expect_err("必需函数指针缺失必须被拒绝");
    assert!(matches!(
        err,
        LoaderError::MissingFunction {
            name: "free_buffer",
            ..
        }
    ));
    assert_eq!(err.code(), "driver_missing_function");
}

#[test]
fn rejects_manifest_abi_mismatch() {
    // 正常入口 + Manifest 声明 ABI 2.0：声明与实际入口不一致必须拒绝（§20）。
    let path = plugin_file();
    let manifest = manifest(ENTRY_SYMBOL, 2, 0);
    let err =
        NativePlugin::load(&path, manifest).expect_err("Manifest ABI 与实际入口不一致必须被拒绝");
    assert!(matches!(err, LoaderError::ManifestAbiMismatch { .. }));
    assert_eq!(err.code(), "driver_manifest_abi_mismatch");
}

#[test]
fn rejects_manifest_entry_with_nul() {
    // Manifest 从 JSON 读取，entry 可能包含 \u0000（§20 可恢复配置错误），
    // 不得 panic，必须返回明确的 LoaderError。
    let err =
        load("forgelink_driver_entry_v1\u{0}trailing").expect_err("含 NUL 的入口名称必须被拒绝");
    assert!(matches!(err, LoaderError::InvalidEntryName { .. }));
    assert_eq!(err.code(), "driver_manifest_entry_invalid");
}

#[test]
fn rejects_missing_file() {
    let path = plugin_dir().join("no_such_plugin.dll");
    let manifest = manifest(ENTRY_SYMBOL, ABI_MAJOR, ABI_MINOR);
    let err = NativePlugin::load(&path, manifest).expect_err("不存在的库必须被拒绝");
    assert!(matches!(err, LoaderError::LoadFailed { .. }));
    assert_eq!(err.code(), "driver_load_failed");
}
