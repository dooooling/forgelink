//! 故障场景（§34.2）：Modbus 断连窗口、设备超时 1% 注入、broker 停机
//! 窗口。统一时间线：基线期 → 故障窗口 → 恢复观察期 → 静默判定。
//!
//! 判定核心：**恢复后丢失候选为 0**（§34.2「30min Broker 断网恢复：
//! 已落盘 Observation 0 丢失；允许 at-least-once 重复」）。故障窗口内
//! 的重复投递如实上报。

use std::time::Duration;

use crate::accounting::Accounting;
use crate::orchestrator::CollectorProc;
use crate::report::{self, BenchReport, criterion};
use crate::sampler::Sampler;
use crate::scenario::{Ctx, QUIESCE_TIMEOUT, stop_semantics_note};
use crate::workload;

/// 故障场景共享参数：标准吞吐 workload 缩时版。
fn fault_plan() -> workload::WorkloadPlan {
    workload::WorkloadPlan {
        devices: 20,
        props_per_device: 100,
        interval_ms: 200,
        flush_interval_ms: 1000,
    }
}

/// 统一时间线执行器：`apply_fault(true)` 进入故障、`apply_fault(false)`
/// 解除。基线 30s / 故障 `fault_secs` / 恢复 90s。
#[allow(clippy::too_many_arguments)]
async fn run_fault_timeline(
    ctx: &Ctx,
    name: &'static str,
    fault_secs: u64,
    apply_fault: impl Fn(bool),
) -> Result<(BenchReport, crate::accounting::AccountingSnapshot), String> {
    let resolved = &ctx.resolved;
    let plan = fault_plan();
    let device_ids: Vec<String> = (0..plan.devices)
        .map(|i| workload::device_id((i + 1) as u8))
        .collect();

    let feed = ctx
        .northbound
        .start_feed(workload::SITE_ID, &device_ids)
        .await?;
    let accounting = Accounting::new();
    let _task = accounting.clone().spawn(feed);

    let (mqtt_host, mqtt_port) = ctx.northbound.connection();
    let paths = workload::generate(
        &workload::GenerateEnv {
            dir: &resolved.work_dir,
            modbus_addr: ctx.mock_server.addr,
            mqtt_host: &mqtt_host,
            mqtt_port,
            rest_port: resolved.rest_port,
            plugin_path: &resolved.plugin_path,
        },
        &plan,
    )
    .map_err(|e| format!("workload 生成失败: {e}"))?;

    let mut proc = CollectorProc::spawn(
        &resolved.collector_bin,
        &paths.config_path,
        resolved.rest_port,
    )
    .await
    .map_err(|e| format!("collector 启动失败: {e}"))?;
    proc.wait_healthy(Duration::from_secs(60)).await?;

    let mut sampler = Sampler::start(
        resolved.rest_port,
        proc.pid,
        Duration::from_secs(resolved.sample_interval_secs),
        ctx.output_dir(name).join("samples.jsonl"),
    );

    // 基线期。
    tokio::time::sleep(Duration::from_secs(30)).await;
    // 故障窗口。
    apply_fault(true);
    tokio::time::sleep(Duration::from_secs(fault_secs)).await;
    apply_fault(false);
    // 恢复观察期：补传 + 重连结算。
    tokio::time::sleep(Duration::from_secs(90)).await;

    proc.quiesce(QUIESCE_TIMEOUT).await?;
    sampler.stop().await;
    proc.stop().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let acc = accounting.snapshot();
    let samples = sampler.samples();
    let summary = report::summarize(&samples, Duration::from_secs(30));
    let criteria = vec![
        criterion(
            "recovery_no_loss",
            "故障恢复后丢失候选 = 0",
            format!("丢失候选 {}，重复 {}", acc.loss_candidates, acc.duplicates),
            acc.loss_candidates == 0,
        ),
        criterion(
            "delivery_progressed",
            "恢复后交付持续推进（收到批次 > 0）",
            format!("收到批次 {}", acc.received_batches),
            acc.received_batches > 0,
        ),
        report::skip_criterion("steady_throughput", "≥ 20,000 observations/s"),
        report::skip_criterion("configured_points", "≥ 10,000 points"),
    ];
    let notes = vec![
        stop_semantics_note(),
        format!(
            "故障时间线：基线 30s → 故障 {fault_secs}s → 恢复观察 90s；workload {}×{}@{}ms",
            plan.devices, plan.props_per_device, plan.interval_ms
        ),
    ];
    Ok((
        BenchReport {
            schema: "forgelink.bench.report.v1",
            scenario: name.to_owned(),
            mode: "formal",
            broker_mode: resolved.broker.as_str().to_owned(),
            env: crate::envinfo::EnvironmentInfo::collect(),
            summary,
            accounting: acc,
            criteria,
            notes,
        },
        accounting.snapshot(),
    ))
}

/// Modbus 断连窗口：drop_connection 开关。
pub async fn fault_net(ctx: &Ctx, fault_secs: u64) -> Result<BenchReport, String> {
    let behavior = ctx.mock_server.behavior();
    let (report, _) = run_fault_timeline(ctx, "fault-net", fault_secs, |on| {
        behavior.lock().expect("行为锁").drop_connection = on;
    })
    .await?;
    Ok(report)
}

/// 设备超时 1%：timeout_rate (1,100) 窗口（命中静默直至客户端超时）。
pub async fn fault_timeout(ctx: &Ctx, fault_secs: u64) -> Result<BenchReport, String> {
    let behavior = ctx.mock_server.behavior();
    let (report, _) = run_fault_timeline(ctx, "fault-timeout", fault_secs, |on| {
        behavior.lock().expect("行为锁").timeout_rate = on.then_some((1, 100));
    })
    .await?;
    Ok(report)
}

/// broker 停机窗口：仅 mock 模式（受理闸门关闭，端口仍监听）。
pub async fn fault_broker(ctx: &Ctx, fault_secs: u64) -> Result<BenchReport, String> {
    let broker = match &ctx.northbound {
        crate::broker::Northbound::Mock(b) => b,
        crate::broker::Northbound::Real { .. } => {
            return Err(
                "fault-broker 仅支持 --broker mock；真实 broker 的停机窗口由操作手册的\
                 人工步骤承担（外部进程无法由本工具安全操控）"
                    .to_owned(),
            );
        }
    };
    let (report, _) = run_fault_timeline(ctx, "fault-broker", fault_secs, |on| {
        broker.set_accepts_enabled(!on);
    })
    .await?;
    Ok(report)
}
