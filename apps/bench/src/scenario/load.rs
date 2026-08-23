//! 负载型场景：smoke（CI 冒烟）/ throughput（标准吞吐 workload）/
//! schedule（独立调度测试）。三者共享同一编排流程，仅形状参数与
//! 判定集合不同。

use std::time::Duration;

use crate::accounting::Accounting;
use crate::cli::BrokerKind;
use crate::orchestrator::CollectorProc;
use crate::report::{self, BenchReport, Summary, Verdict, criterion, skip_criterion};
use crate::sampler::Sampler;
use crate::scenario::{Ctx, QUIESCE_TIMEOUT, stop_semantics_note};
use crate::workload::{self, WorkloadPlan};

/// 负载场景参数。
pub struct LoadParams {
    pub name: &'static str,
    pub plan: WorkloadPlan,
    pub duration_secs: u64,
    /// 预热期（吞吐与 RSS 基线剔除；§34.2 未定值，取 60s 或时长的 10%）。
    pub warmup: Duration,
}

/// smoke：缩配 2 设备 × 4 点 @50ms 短跑。
pub fn smoke_params(duration_secs: u64) -> LoadParams {
    LoadParams {
        name: "smoke",
        plan: WorkloadPlan {
            devices: 2,
            props_per_device: 4,
            interval_ms: 50,
            flush_interval_ms: 500,
        },
        duration_secs,
        warmup: Duration::from_secs(0),
    }
}

/// 标准吞吐 workload：默认 100×100@500ms + 5ms±1ms 模拟响应延迟。
pub fn throughput_params(
    devices: usize,
    props: usize,
    interval_ms: u64,
    duration_secs: u64,
) -> LoadParams {
    LoadParams {
        name: "throughput",
        plan: WorkloadPlan {
            devices,
            props_per_device: props,
            interval_ms,
            // 冲刷间隔大于采集周期：单设备多轮聚合进同一批，
            // 使「单批 ≥1000 Observation」可观测。
            flush_interval_ms: interval_ms.max(1000) * 10,
        },
        duration_secs,
        warmup: Duration::from_secs(60),
    }
}

/// 独立调度测试：10×100@100ms（§34.2）。
pub fn schedule_params(duration_secs: u64) -> LoadParams {
    LoadParams {
        name: "schedule",
        plan: WorkloadPlan {
            devices: 10,
            props_per_device: 100,
            interval_ms: 100,
            flush_interval_ms: 1000,
        },
        duration_secs,
        warmup: Duration::from_secs(60),
    }
}

/// 统一负载流程：订阅 → 记账 → 生成 → 启动 → 采样 → 静默 → 报告。
pub async fn run(ctx: &Ctx, p: &LoadParams, mode: &'static str) -> Result<BenchReport, String> {
    let resolved = &ctx.resolved;
    workload::validate_plan(&p.plan)?;
    let device_ids: Vec<String> = (0..p.plan.devices)
        .map(|i| workload::device_id((i + 1) as u8))
        .collect();

    // 订阅先于 collector 启动（不漏计首批）。
    let feed = ctx
        .northbound
        .start_feed(workload::SITE_ID, &device_ids)
        .await?;
    let accounting = Accounting::new();
    // JoinHandle 落盘即 detach（tokio 任务独立运行），记账随通道关闭自然结束。
    let _accounting_task = accounting.clone().spawn(feed);

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
        &p.plan,
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
        ctx.output_dir(p.name).join("samples.jsonl"),
    );

    tokio::time::sleep(Duration::from_secs(p.duration_secs)).await;

    proc.quiesce(QUIESCE_TIMEOUT).await?;
    sampler.stop().await;
    let exit_code = proc.stop().await?;
    // 尾包结算宽限：collector 停止后允许订阅通道排空在途消息。
    tokio::time::sleep(Duration::from_secs(2)).await;
    let acc = accounting.snapshot();
    let samples = sampler.samples();

    let summary = report::summarize(&samples, p.warmup);
    let criteria = load_criteria(mode, &p.plan, &summary, &acc);
    let notes = vec![
        stop_semantics_note(),
        format!(
            "workload：{} 设备 × {} 点 @ {}ms（连续保持寄存器，每周期合并为 1 次 FC03）；运行 {}s",
            p.plan.devices, p.plan.props_per_device, p.plan.interval_ms, p.duration_secs
        ),
        match exit_code {
            Some(code) => format!("collector 退出码 {code}"),
            None => "collector 被信号终止（有序停机超时升级）".to_owned(),
        },
    ];

    Ok(BenchReport {
        schema: "forgelink.bench.report.v1",
        scenario: p.name.to_owned(),
        mode,
        broker_mode: broker_mode_str(resolved.broker),
        env: crate::envinfo::EnvironmentInfo::collect(),
        summary,
        accounting: acc,
        criteria,
        notes,
    })
}

fn broker_mode_str(broker: BrokerKind) -> String {
    broker.as_str().to_owned()
}

/// 负载场景判定集合：按场景裁剪 §34.2 验收表。
fn load_criteria(
    mode: &'static str,
    plan: &WorkloadPlan,
    summary: &Summary,
    acc: &crate::accounting::AccountingSnapshot,
) -> Vec<report::Criterion> {
    let formal = mode == "formal";
    let points = (plan.devices * plan.props_per_device) as u64;
    let mut out = Vec::new();

    // 配置点数与设备数：仅 throughput 场景对应验收表条目。
    if plan.devices >= 100 && points >= 10_000 {
        out.push(if formal {
            criterion(
                "configured_points",
                "≥ 10,000 points",
                format!("{points}"),
                points >= 10_000,
            )
        } else {
            skip_criterion("configured_points", "≥ 10,000 points")
        });
    }
    // 稳态吞吐 ≥20k obs/s：throughput formal 才判定。
    if formal && plan.interval_ms <= 500 && plan.props_per_device >= 100 && plan.devices >= 100 {
        out.push(match summary.steady_throughput_obs_per_s {
            Some(t) => criterion(
                "steady_throughput",
                "≥ 20,000 observations/s",
                format!("{t:.0} obs/s"),
                t >= 20_000.0,
            ),
            None => fail_criterion("steady_throughput", "≥ 20,000 observations/s", "采样不足"),
        });
    } else {
        out.push(skip_criterion(
            "steady_throughput",
            "≥ 20,000 observations/s",
        ));
    }

    // 单批 ≥1000 Observation：flush 间隔足以聚合时才判定（smoke 的
    // flush=500ms、4 点/批不构成条件）。
    if formal && plan.flush_interval_ms > plan.interval_ms * 10 {
        out.push(criterion(
            "max_batch_observations",
            "单批 ≥ 1,000 Observation",
            format!("实测单批最大 {}", acc.max_batch_observations),
            acc.max_batch_observations >= 1_000,
        ));
    } else {
        out.push(skip_criterion(
            "max_batch_observations",
            "单批 ≥ 1,000 Observation",
        ));
    }

    // p99 调度延迟 ≤25ms（§34.2；100ms 周期可达性的量化口径）。
    out.push(if formal {
        match summary.schedule_delay.as_ref().and_then(|d| d.p99_ns) {
            Some(p99_ns) => criterion(
                "schedule_p99_delay",
                "p99 ≤ 25 ms",
                format!("p99 ∈ {} ms", format_bound(p99_ns)),
                p99_ns <= 25_000_000,
            ),
            None => fail_criterion("schedule_p99_delay", "p99 ≤ 25 ms", "无调度延迟观测"),
        }
    } else {
        skip_criterion("schedule_p99_delay", "p99 ≤ 25 ms")
    });

    // 功能性底线（所有模式都判）：交付语义 at-least-once 下不允许丢失
    // 候选（重复如实上报不计 FAIL）；且链路必须真实有数据流动——空转
    // 的 0 丢失不构成通过。
    out.push(criterion(
        "delivery_no_loss",
        "丢失候选 = 0（at-least-once 允许重复）",
        format!("丢失候选 {}，重复 {}", acc.loss_candidates, acc.duplicates),
        acc.loss_candidates == 0,
    ));
    out.push(criterion(
        "delivery_active",
        "收到批次 > 0（数据链路活跃）",
        format!(
            "收到批次 {}（observations {}）",
            acc.received_batches, acc.received_observations
        ),
        acc.received_batches > 0,
    ));
    out
}

/// 桶上界的展示形式（ns → ms 字符串）。
fn format_bound(bound_ns: u64) -> String {
    if bound_ns == u64::MAX {
        ">300000".to_owned()
    } else {
        format!("{:.3}", bound_ns as f64 / 1e6)
    }
}

/// FAIL 项便捷构造。
fn fail_criterion(name: &str, requirement: &str, actual: &str) -> report::Criterion {
    report::Criterion {
        name: name.to_owned(),
        requirement: requirement.to_owned(),
        actual: actual.to_owned(),
        verdict: Verdict::Fail,
    }
}
