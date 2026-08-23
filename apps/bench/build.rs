//! 构建期注入环境信息（§34.2：每次报告必须记录构建 commit 与 Rust 版本；
//! 目标硬件上未必安装 rustc/git，故在编译期固化而非运行期探测）。

use std::process::Command;

fn main() {
    // git commit（短哈希）；非 git 环境（如源码包）留空，报告标注。
    let commit = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default();
    println!("cargo:rustc-env=BENCH_GIT_COMMIT={commit}");

    // 编译器版本（RUSTC 由 cargo 注入，指向实际执行编译的 rustc）。
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default();
    println!("cargo:rustc-env=BENCH_RUSTC_VERSION={version}");

    println!("cargo:rerun-if-changed=build.rs");
}
