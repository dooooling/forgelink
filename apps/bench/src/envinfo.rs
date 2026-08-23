//! 环境信息收集（§34.2：报告必须记录 CPU 型号、核心数、内存、磁盘、
//! OS、Rust 版本和构建 commit）。
//!
//! 策略：编译期注入 rustc/git（build.rs，目标硬件未必装工具链）；运行期
//! Linux 读 `/proc` 零依赖；Windows 尽力而为 `reg query`（失败留空——
//! §34.2 明文 Windows x64 仅功能/稳定性复验，基线数据只在 Linux 有效）。
//! 磁盘型号无零依赖获取手段，固定为报告模板中的人工填写字段。

use std::process::Command;

use serde::Serialize;

/// 报告头部的环境块。
#[derive(Debug, Serialize)]
pub struct EnvironmentInfo {
    pub os: String,
    pub cpu_model: String,
    pub cpu_cores: String,
    pub mem_total: String,
    /// 磁盘型号/类型（SSD/NVMe/eMMC）——人工填写字段（§34.2 要求记录，
    /// 无跨平台零依赖读取手段）。
    pub disk: String,
    pub rustc_version: String,
    pub git_commit: String,
    /// 性能指标（RSS/CPU）是否在本平台采集：仅 Linux 为 true，
    /// Windows 报告显式标注"复验平台不采集"防误读。
    pub perf_metrics_collected: bool,
}

impl EnvironmentInfo {
    /// 收集当前运行环境。任何单项失败留空字符串，不阻塞基准执行。
    pub fn collect() -> Self {
        let (cpu_model, mem_total, kernel) = if cfg!(target_os = "linux") {
            (
                read_linux_cpu_model(),
                read_linux_mem_total(),
                read_proc_version(),
            )
        } else if cfg!(windows) {
            (read_windows_cpu_model(), String::new(), String::new())
        } else {
            (String::new(), String::new(), String::new())
        };
        let os_name = std::env::consts::OS.to_owned();
        let os = if kernel.is_empty() {
            os_name
        } else {
            format!("{os_name} ({kernel})")
        };
        let cores = std::thread::available_parallelism()
            .map(|n| n.get().to_string())
            .unwrap_or_default();
        Self {
            os,
            cpu_model,
            cpu_cores: cores,
            mem_total,
            disk: "（人工填写：SSD/NVMe/eMMC 型号）".to_owned(),
            rustc_version: env!("BENCH_RUSTC_VERSION").to_owned(),
            git_commit: env!("BENCH_GIT_COMMIT").to_owned(),
            perf_metrics_collected: cfg!(target_os = "linux"),
        }
    }
}

/// `/proc/cpuinfo` 第一个 `model name`（x86）；ARM 无该字段时回退
/// `Hardware` 或 `implementer` 行，仍失败留空。
fn read_linux_cpu_model() -> String {
    let Ok(text) = std::fs::read_to_string("/proc/cpuinfo") else {
        return String::new();
    };
    for key in ["model name", "Hardware", "implementer"] {
        for line in text.lines() {
            if let Some(v) = line.strip_prefix(key)
                && let Some(v) = v.trim().strip_prefix(':')
            {
                return v.trim().to_owned();
            }
        }
    }
    String::new()
}

/// `/proc/meminfo` 的 `MemTotal`（kB → GiB 一位小数）。
fn read_linux_mem_total() -> String {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return String::new();
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:")
            && let Some(kb) = rest.trim().strip_suffix(" kB")
            && let Ok(kb) = kb.trim().parse::<f64>()
        {
            return format!("{:.1} GiB", kb / 1024.0 / 1024.0);
        }
    }
    String::new()
}

fn read_proc_version() -> String {
    std::fs::read_to_string("/proc/version")
        .map(|s| s.trim().to_owned())
        .unwrap_or_default()
}

/// Windows 尽力而为：注册表查 CPU 型号（失败留空，不影响执行）。
fn read_windows_cpu_model() -> String {
    Command::new("reg")
        .args([
            "query",
            r"HKLM\HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            "/v",
            "ProcessorNameString",
        ])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .find(|l| l.contains("ProcessorNameString"))
                .and_then(|l| l.split_once("REG_SZ"))
                .map(|(_, v)| v.trim().to_owned())
        })
        .unwrap_or_default()
}
