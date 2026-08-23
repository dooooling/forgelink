//! `forgelink-bench`：ForgeLink 性能基准工具（§34.2 验收三件套之一）。
//!
//! 编排器进程模型：生成 workload → 子进程拉起 release 版 collector →
//! 经 REST 采指标、经 MQTT 订阅流式记账、定时采 /proc → 出验收报告。
//! 正式验收在目标硬件人工执行（§34.2 Reference Benchmark Profile），
//! CI 仅运行 smoke 场景防退化。

mod accounting;
mod broker;
mod cli;
mod envinfo;
mod orchestrator;
mod report;
mod sampler;
mod scenario;
mod workload;

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser as _;
use cli::{BrokerKind, Resolved, Scenario};
use report::Verdict;

fn main() -> ExitCode {
    let cli = cli::Cli::parse();
    let rt = tokio::runtime::Runtime::new().expect("Tokio 运行时创建失败");
    rt.block_on(run(cli.scenario))
}

async fn run(scenario: Scenario) -> ExitCode {
    // run_id：work_dir 自动命名的唯一性来源（纳秒时间戳）。
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    match dispatch(scenario, run_id).await {
        Ok((report_path, all_passed)) => {
            if all_passed {
                println!("PASS — 报告：{}", report_path.display());
                ExitCode::SUCCESS
            } else {
                println!("FAIL — 报告：{}", report_path.display());
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("bench 执行失败：{e}");
            ExitCode::FAILURE
        }
    }
}

/// 收尾：写报告 → 汇总判定 → 按需清理工作目录。
fn finish(
    resolved: &Resolved,
    scenario_name: &str,
    report: report::BenchReport,
) -> Result<(std::path::PathBuf, bool), String> {
    let path = report
        .write_outputs(&resolved.output_dir.join(scenario_name))
        .map_err(|e| format!("报告写出失败: {e}"))?;
    let all_passed = report.criteria.iter().all(|c| c.verdict != Verdict::Fail);
    if !resolved.keep {
        // 工作目录清理失败不掩盖基准结论（报告已落盘输出目录）。
        let _ = std::fs::remove_dir_all(&resolved.work_dir);
    }
    Ok((path, all_passed))
}

async fn dispatch(scenario: Scenario, run_id: u128) -> Result<(std::path::PathBuf, bool), String> {
    match scenario {
        Scenario::Smoke {
            common,
            duration_secs,
        } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report =
                scenario::load::run(&ctx, &scenario::load::smoke_params(duration_secs), "smoke")
                    .await?;
            finish(&resolved, "smoke", report)
        }
        Scenario::Throughput {
            common,
            devices,
            props_per_device,
            interval_ms,
            duration_secs,
        } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::load::run(
                &ctx,
                &scenario::load::throughput_params(
                    devices,
                    props_per_device,
                    interval_ms,
                    duration_secs,
                ),
                "formal",
            )
            .await?;
            finish(&resolved, "throughput", report)
        }
        Scenario::Schedule {
            common,
            duration_secs,
        } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::load::run(
                &ctx,
                &scenario::load::schedule_params(duration_secs),
                "formal",
            )
            .await?;
            finish(&resolved, "schedule", report)
        }
        Scenario::FaultNet { common, fault_secs } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::faults::fault_net(&ctx, fault_secs).await?;
            finish(&resolved, "fault-net", report)
        }
        Scenario::FaultTimeout { common, fault_secs } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::faults::fault_timeout(&ctx, fault_secs).await?;
            finish(&resolved, "fault-timeout", report)
        }
        Scenario::FaultBroker { common, fault_secs } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::faults::fault_broker(&ctx, fault_secs).await?;
            finish(&resolved, "fault-broker", report)
        }
        Scenario::CrashWal { common, run_secs } => {
            let resolved = common.resolve(run_id)?;
            // crash-wal 依赖同目录重启，工作目录必须显式保留到场景结束。
            let resolved = Resolved {
                keep: true,
                ..resolved
            };
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::crash::run(&ctx, run_secs).await?;
            finish(&resolved, "crash-wal", report)
        }
        Scenario::Soak {
            common,
            duration_secs,
        } => {
            let resolved = common.resolve(run_id)?;
            let ctx = scenario::Ctx::new(resolved.clone()).await;
            let report = scenario::soak::run(&ctx, duration_secs).await?;
            finish(&resolved, "soak", report)
        }
    }
}

/// 报告标注用（供 cli 的 BrokerKind 与 broker 模块共享语义）。
const _: Option<BrokerKind> = None;
