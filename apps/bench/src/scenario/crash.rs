//! WAL 强杀恢复场景（§34.2「forced process restart during WAL write」/
//! §34.4 验收 3-5）：运行中 SIGKILL → 同 work-dir 重启 → 补传结算。
//!
//! # 记账口径（与负载场景不同！）
//!
//! 重启后 per-device `sequence` 归零、`message_id` 内嵌新 session 后缀，
//! 水位线记账会把"补传旧批次 + 新批次序号重叠"误判为重复——本场景改用
//! **message_id 集合记账**（workload 小，集合有界）。
//!
//! 丢失判定：强杀前 broker 已收到的批次（唯一 id 数）与当时 WAL 在途数
//! （REST 采样）**不相交**（已收到 = 已 PUBACK = 已从 WAL 删除），二者
//! 之和是"collector 曾产出且必须最终可达 broker"的下界；重启补传后唯一
//! id 总数 ≥ 该下界 ⟺ 0 丢失。

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;

use crate::orchestrator::CollectorProc;
use crate::report::{self, BenchReport, criterion};
use crate::sampler::Sampler;
use crate::scenario::{Ctx, stop_semantics_note};
use crate::workload;

/// message_id 级账本。
#[derive(Debug, Default, Clone)]
struct MsgIdLedger {
    seen: HashSet<String>,
    replayed_batches: u64,
    parse_errors: u64,
}

impl MsgIdLedger {
    fn record(&mut self, payload: &[u8]) {
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
            self.parse_errors += 1;
            return;
        };
        let id = v
            .get("message_id")
            .and_then(|m| m.as_str())
            .unwrap_or_default();
        if id.is_empty() {
            self.parse_errors += 1;
            return;
        }
        if self.seen.insert(id.to_owned())
            && v.get("replayed").and_then(|r| r.as_bool()).unwrap_or(false)
        {
            self.replayed_batches += 1;
        }
    }

    fn unique(&self) -> u64 {
        self.seen.len() as u64
    }
}

#[derive(Debug, Serialize)]
struct CrashAccounting {
    pub unique_messages: u64,
    pub replayed_batches: u64,
    pub parse_errors: u64,
}

fn spawn_ledger_task(
    feed: crate::broker::Feed,
) -> (Arc<Mutex<MsgIdLedger>>, tokio::task::JoinHandle<()>) {
    let ledger = Arc::new(Mutex::new(MsgIdLedger::default()));
    let handle = ledger.clone();
    let task = tokio::spawn(async move {
        let mut feed = feed;
        while let Some(msg) = feed.recv().await {
            handle.lock().expect("记账锁").record(&msg.payload);
        }
    });
    (ledger, task)
}

/// 执行 crash-wal 场景。
pub async fn run(ctx: &Ctx, run_secs: u64) -> Result<BenchReport, String> {
    let resolved = &ctx.resolved;
    let plan = workload::WorkloadPlan {
        devices: 10,
        props_per_device: 100,
        interval_ms: 100,
        flush_interval_ms: 1000,
    };
    let device_ids: Vec<String> = (0..plan.devices)
        .map(|i| workload::device_id((i + 1) as u8))
        .collect();
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

    // ── 阶段一：正常运行后强杀 ──
    let feed1 = ctx
        .northbound
        .start_feed(workload::SITE_ID, &device_ids)
        .await?;
    let (ledger1, _task1) = spawn_ledger_task(feed1);
    let mut proc = CollectorProc::spawn(
        &resolved.collector_bin,
        &paths.config_path,
        resolved.rest_port,
    )
    .await
    .map_err(|e| format!("collector 启动失败: {e}"))?;
    proc.wait_healthy(Duration::from_secs(60)).await?;
    let mut sampler1 = Sampler::start(
        resolved.rest_port,
        proc.pid,
        Duration::from_secs(resolved.sample_interval_secs),
        ctx.output_dir("crash-wal").join("samples.jsonl"),
    );
    tokio::time::sleep(Duration::from_secs(run_secs)).await;

    // 强杀前快照：WAL 在途数（丢失下界的组成部分）。
    let wal_inflight_at_kill = match proc.inflight().await {
        Some((wal, _)) => wal.max(0) as u64,
        None => return Err("强杀前无法读取 WAL 在途指标".to_owned()),
    };
    sampler1.stop().await;
    proc.kill().await.map_err(|e| format!("强杀失败: {e}"))?;
    // 尾包宽限：阶段一订阅通道排空。
    tokio::time::sleep(Duration::from_secs(2)).await;
    let phase1 = ledger1.lock().expect("记账锁").clone();

    // ── 阶段二：同 work-dir 重启（session 自动更新）→ 补传 ──
    let feed2 = ctx
        .northbound
        .start_feed(workload::SITE_ID, &device_ids)
        .await?;
    let (ledger2, _task2) = spawn_ledger_task(feed2);
    let mut proc2 = CollectorProc::spawn(
        &resolved.collector_bin,
        &paths.config_path,
        resolved.rest_port,
    )
    .await
    .map_err(|e| format!("collector 重启失败: {e}"))?;
    let restart_ok = proc2.wait_healthy(Duration::from_secs(60)).await.is_ok();
    let mut quiesce_ok = false;
    if restart_ok {
        quiesce_ok = proc2
            .quiesce(crate::scenario::QUIESCE_TIMEOUT)
            .await
            .is_ok();
    }
    let mut sampler2 = Sampler::start(
        resolved.rest_port,
        proc2.pid,
        Duration::from_secs(resolved.sample_interval_secs),
        ctx.output_dir("crash-wal").join("samples-recovery.jsonl"),
    );
    tokio::time::sleep(Duration::from_secs(15)).await;
    sampler2.stop().await;
    proc2.stop().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let phase2 = ledger2.lock().expect("记账锁").clone();

    // ── 判定 ──
    let union_unique = {
        let s1 = ledger1.lock().expect("记账锁");
        let s2 = ledger2.lock().expect("记账锁");
        let mut all = s1.seen.clone();
        all.extend(s2.seen.iter().cloned());
        all.len() as u64
    };
    let expected_min = phase1.unique() + wal_inflight_at_kill;
    let acc = CrashAccounting {
        unique_messages: union_unique,
        replayed_batches: phase2.replayed_batches,
        parse_errors: phase1.parse_errors + phase2.parse_errors,
    };
    let samples2 = sampler2.samples();
    let summary = report::summarize(&samples2, Duration::ZERO);
    let criteria = vec![
        criterion(
            "restart_after_sigkill",
            "SIGKILL 后同 work-dir 重启并达到健康",
            if restart_ok {
                "重启成功"
            } else {
                "重启失败/超时"
            }
            .to_owned(),
            restart_ok,
        ),
        criterion(
            "wal_replay_happened",
            "重启后发生 WAL 补传（replayed 批次 > 0）",
            format!("replayed 批次 {}", acc.replayed_batches),
            acc.replayed_batches > 0,
        ),
        criterion(
            "crash_no_loss",
            &format!(
                "唯一消息总数 ≥ 强杀前已交付 {} + WAL 在途 {}",
                phase1.unique(),
                wal_inflight_at_kill
            ),
            format!("唯一消息总数 {union_unique}"),
            union_unique >= expected_min,
        ),
        criterion(
            "post_recovery_quiesce",
            "重启补传后在途归零",
            if quiesce_ok {
                "已静默"
            } else {
                "未能在期限内静默"
            }
            .to_owned(),
            quiesce_ok,
        ),
    ];
    let notes = vec![
        stop_semantics_note(),
        format!(
            "时间线：运行 {run_secs}s → SIGKILL → 立即重启同 work-dir；\
             强杀前 WAL 在途 {wal_inflight_at_kill} 条"
        ),
        "记账口径：message_id 唯一集合（跨会话 sequence 归零不可用水位线，见模块注释）".to_owned(),
    ];
    Ok(BenchReport {
        schema: "forgelink.bench.report.v1",
        scenario: "crash-wal".to_owned(),
        mode: "formal",
        broker_mode: resolved.broker.as_str().to_owned(),
        env: crate::envinfo::EnvironmentInfo::collect(),
        summary,
        accounting: crate::accounting::AccountingSnapshot {
            received_batches: union_unique,
            received_observations: 0,
            duplicates: phase1
                .seen
                .iter()
                .filter(|id| phase2.seen.contains(*id))
                .count() as u64,
            loss_candidates: expected_min.saturating_sub(union_unique),
            replayed_batches: acc.replayed_batches,
            max_batch_observations: 0,
            parse_errors: acc.parse_errors,
            invalid_sequences: 0,
        },
        criteria,
        notes,
    })
}
