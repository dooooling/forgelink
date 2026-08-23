//! 基准工具端到端测试：以**真实 CLI 子进程**跑缩配 smoke 场景全链路
//! （构建 collector/cdylib → 生成 workload → 启动 → 采样 → 报告）。
//!
//! 这是 CI 的防退化锚点（§34.7 bench-smoke job 直接复用）：验证的是
//! 工具本身的编排契约与报告判定，不承载性能结论（debug 构建 + 缩配
//! workload，性能项在 smoke 模式下本就 SKIP）。

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// cdylib 产物文件名（与 collector 测试 helper 同约定）。
fn plugin_name() -> &'static str {
    if cfg!(windows) {
        "driver_modbus.dll"
    } else {
        "libdriver_modbus.so"
    }
}

fn binary_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_owned()
    }
}

/// 确保测试依赖的二进制已构建（collector 与 cdylib；bench 自身由
/// `cargo test` 先行编译）。进程内一次；嵌套 build 为增量，主 cargo
/// 已释放编译锁（与 collector 测试 Harness 同模式）。返回 collector
/// 二进制路径。
fn ensure_built() -> PathBuf {
    static GUARD: OnceLock<()> = OnceLock::new();
    GUARD.get_or_init(|| {
        let status = Command::new("cargo")
            .args(["build", "-p", "collector", "-p", "driver-modbus"])
            .status()
            .expect("无法执行 cargo build");
        assert!(status.success(), "测试二进制构建失败");
        let target_debug = target_debug_dir();
        let collector = target_debug.join(binary_name("collector"));
        let plugin = target_debug.join(plugin_name());
        assert!(collector.exists(), "collector 二进制缺失: {collector:?}");
        assert!(plugin.exists(), "cdylib 产物缺失: {plugin:?}");
    });
    target_debug_dir().join(binary_name("collector"))
}

fn target_debug_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug")
}

#[test]
fn smoke_end_to_end_produces_passing_report() {
    let collector_bin = ensure_built();
    let bench_bin = target_debug_dir().join(binary_name("forgelink-bench"));

    let temp = tempfile::tempdir().expect("临时目录");
    let output_dir = temp.path().join("out");
    let work_dir = temp.path().join("work");

    // REST 固定端口：避开默认 18080（防与本机其他实例冲突），选冷门口。
    let rest_port = 18123;

    let status = Command::new(&bench_bin)
        .args([
            "smoke",
            "--collector-bin",
            collector_bin.to_str().expect("路径 UTF-8"),
            "--rest-port",
            &rest_port.to_string(),
            "--duration-secs",
            "8",
            "--output-dir",
            output_dir.to_str().expect("路径 UTF-8"),
            "--work-dir",
            work_dir.to_str().expect("路径 UTF-8"),
            "--sample-interval-secs",
            "1",
        ])
        .status()
        .expect("forgelink-bench 启动失败");
    assert!(status.success(), "smoke 场景必须整体 PASS（exit 0）");

    // 报告存在且核心项通过。
    let report_path = output_dir.join("smoke").join("bench-report.json");
    let raw =
        std::fs::read(&report_path).unwrap_or_else(|e| panic!("报告缺失 {:?}: {e}", report_path));
    let report: serde_json::Value = serde_json::from_slice(&raw).expect("报告 JSON 解析");
    assert_eq!(report["schema"], "forgelink.bench.report.v1");
    assert_eq!(report["scenario"], "smoke");
    assert_eq!(report["broker_mode"], "mock");

    let criteria = report["criteria"].as_array().expect("criteria 数组");
    let no_fail = criteria.iter().all(|c| c["verdict"] != "fail");
    assert!(no_fail, "不允许 FAIL 项：{criteria:?}");
    let delivery = criteria
        .iter()
        .find(|c| c["name"] == "delivery_no_loss")
        .expect("交付无丢失判定必须存在");
    assert_eq!(delivery["verdict"], "pass");

    // 工作目录已清理（未传 --keep）。
    assert!(!work_dir.exists(), "默认应清理工作目录");
}
