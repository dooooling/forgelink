//! 长稳 soak（§34.2：72h 连续运行，稳态 RSS 漂移 ≤10%）。
//!
//! 与负载场景同流程；差异：采样间隔放宽（≥30s，控制 72h 样本量与
//! 检查点体积）、预热基线取 1h、判定聚焦 RSS 漂移与交付无丢失。
//! 每样本逐条落盘 `samples.jsonl`——中断后可用已落盘数据续算。

use std::time::Duration;

use crate::accounting::Accounting;
use crate::orchestrator::CollectorProc;
use crate::report::{self, BenchReport, criterion};
use crate::sampler::Sampler;
use crate::scenario::{Ctx, QUIESCE_TIMEOUT, stop_semantics_note};
use crate::workload;

/// 预热基线（§34.2 排除有界缓存配置变化——缓存上限在启动期固定，
/// 1h 足以到达稳态）。
const WARMUP: Duration = Duration::from_secs(3600);

pub async fn run(ctx: &Ctx, duration_secs: u64) -> Result<BenchReport, String> {
    let resolved = &ctx.resolved;
    let plan = workload::WorkloadPlan {
        devices: 100,
        props_per_device: 100,
        interval_ms: 500,
        flush_interval_ms: 5000,
    };
    workload::validate_plan(&plan)?;
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

    // 采样间隔 ≥30s（72h × 30s ≈ 8.6k 点）。
    let interval = resolved.sample_interval_secs.max(30);
    let mut sampler = Sampler::start(
        resolved.rest_port,
        proc.pid,
        Duration::from_secs(interval),
        ctx.output_dir("soak").join("samples.jsonl"),
    );

    tokio::time::sleep(Duration::from_secs(duration_secs)).await;

    proc.quiesce(QUIESCE_TIMEOUT).await?;
    sampler.stop().await;
    proc.stop().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let acc = accounting.snapshot();
    let samples = sampler.samples();
    let summary = report::summarize(&samples, WARMUP);
    let criteria = vec![
        // RSS 漂移 ≤10%（soak 的核心验收项；Linux 才有数据源）。
        match summary.rss_drift_pct {
            Some(drift) if cfg!(target_os = "linux") => criterion(
                "rss_drift",
                "72h 稳态 RSS 漂移 ≤ 10%",
                format!("{drift:.1}%"),
                drift <= 10.0,
            ),
            Some(_) => report::skip_criterion(
                "rss_drift",
                "72h 稳态 RSS 漂移 ≤ 10%（非 Linux 复验平台不采集）",
            ),
            None => criterion(
                "rss_drift",
                "72h 稳态 RSS 漂移 ≤ 10%",
                "样本不足".to_owned(),
                false,
            ),
        },
        match summary.steady_throughput_obs_per_s {
            Some(t) => criterion(
                "steady_throughput",
                "≥ 20,000 observations/s",
                format!("{t:.0} obs/s"),
                t >= 20_000.0,
            ),
            None => criterion(
                "steady_throughput",
                "≥ 20,000 observations/s",
                "采样不足".to_owned(),
                false,
            ),
        },
        match summary.schedule_delay.as_ref().and_then(|d| d.p99_ns) {
            Some(p99_ns) => criterion(
                "schedule_p99_delay",
                "p99 ≤ 25 ms",
                format!("p99 ∈ {} ms", bound_ms(p99_ns)),
                p99_ns <= 25_000_000,
            ),
            None => criterion(
                "schedule_p99_delay",
                "p99 ≤ 25 ms",
                "无观测".to_owned(),
                false,
            ),
        },
        criterion(
            "delivery_no_loss",
            "丢失候选 = 0",
            format!("丢失候选 {}，重复 {}", acc.loss_candidates, acc.duplicates),
            acc.loss_candidates == 0,
        ),
    ];
    let notes = vec![
        stop_semantics_note(),
        format!(
            "soak 时长 {duration_secs}s，采样间隔 {interval}s，预热基线 1h；\
             samples.jsonl 支持中断后续算"
        ),
    ];
    Ok(BenchReport {
        schema: "forgelink.bench.report.v1",
        scenario: "soak".to_owned(),
        mode: "formal",
        broker_mode: resolved.broker.as_str().to_owned(),
        env: crate::envinfo::EnvironmentInfo::collect(),
        summary,
        accounting: acc,
        criteria,
        notes,
    })
}

fn bound_ms(bound_ns: u64) -> String {
    if bound_ns == u64::MAX {
        ">300000".to_owned()
    } else {
        format!("{:.3}", bound_ns as f64 / 1e6)
    }
}
