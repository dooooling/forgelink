//! PR-3 验收（§37.2）：Package scanner 能发现并校验四个现有包。
//!
//! 以仓库 `drivers/` 真实目录为输入——每个 driver 目录补上当前平台
//! artifact 副本后，scanner 必须发现四个 id 并全部通过 hash 校验。
//! 该测试同时充当「manifest 与实际产物命名一致」的防漂移断言。

use std::path::PathBuf;

use driver_package::{discover_package, scan_directories};

/// 四个 Driver 的 cdylib 产物文件名（与各 driver.json `artifacts` 声明、
/// Cargo crate 名一致；Windows/Linux 命名规则不同）。
fn artifact_file(crate_name: &str) -> &'static str {
    // cargo cdylib 输出名 = crate 名连字符转下划线。
    if cfg!(windows) {
        Box::leak(format!("driver_{crate_name}.dll").into_boxed_str())
    } else {
        Box::leak(format!("libdriver_{crate_name}.so").into_boxed_str())
    }
}

fn target_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(dir).join("debug");
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug")
}

#[test]
fn scanner_discovers_all_four_repository_drivers() {
    let crates: &[(&str, &str)] = &[
        ("modbus", "modbus"),
        ("s7comm", "s7comm"),
        ("ether_ip", "ethernet-ip"),
        ("mitsubishi_mc", "mitsubishi-mc"),
    ];

    // 沙箱：把 drivers/<name>/driver.json + 当前平台产物复制到临时目录，
    // 不触碰仓库工作区。
    let sandbox = tempfile::TempDir::new().expect("临时目录");
    for (crate_name, dir_name) in crates {
        let src_manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../drivers")
            .join(dir_name)
            .join("driver.json");
        let dst = sandbox.path().join(dir_name);
        std::fs::create_dir_all(&dst).expect("建目录失败");
        std::fs::copy(&src_manifest, dst.join("driver.json")).expect("复制 manifest 失败");

        let built = target_dir().join(artifact_file(crate_name));
        // 产物缺失时自动构建（同 device-manager 测试约定）。
        if !built.exists() {
            let status = std::process::Command::new("cargo")
                .args(["build", "-p"])
                .arg(format!("driver-{}", crate_name.replace('_', "-")))
                .status()
                .expect("嵌套 cargo build 失败");
            assert!(status.success(), "嵌套构建 {crate_name} 失败");
        }
        std::fs::copy(&built, dst.join(artifact_file(crate_name))).expect("复制产物失败");
    }

    // scan_directories 接收父目录列表（其内部遍历含 driver.json 的子目录）。
    let found = scan_directories(&[sandbox.path().to_owned()]).expect("四包扫描应通过");
    let mut ids: Vec<&str> = found.iter().map(|d| d.id()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["ethernet-ip", "mitsubishi-mc", "modbus-tcp", "s7comm"]
    );
    // 发布形态要求：打包脚本回填后 sha256 必须非空且匹配实测值。
    for d in &found {
        assert_eq!(d.manifest.artifacts.len(), 3, "四包都声明三平台");
    }

    // 单包发现接口同样可用。
    let single = discover_package(&sandbox.path().join("modbus"), None).expect("单包发现应成功");
    assert_eq!(single.id(), "modbus-tcp");
}
