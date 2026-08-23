//! 报告：§34.2 验收表逐项判定 + JSON/Markdown 双输出。
//!
//! 判定精度约定：直方图分位数用「累计占比法」取**桶上界**——桶恰在
//! 验收阈值（如 p99 ≤ 25ms 的 25ms 桶）时 PASS 判定精确；FAIL 时只能
//! 断言落在区间 (前桶, 本桶]，措辞如实标注，不做插值伪装精度。

use std::path::Path;
use std::time::Duration;

use serde::Serialize;

use crate::accounting::AccountingSnapshot;
use crate::envinfo::EnvironmentInfo;
use crate::sampler::{HistSnapshot, Sample};

/// 单项判定。
#[derive(Debug, Serialize)]
pub struct Criterion {
    pub name: String,
    pub requirement: String,
    pub actual: String,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Fail,
    /// 冒烟模式或不适用的场景：性能项不判定（报告显式标注 SKIP）。
    Skip,
}

/// 分位数三元组（桶上界毫秒，保留 3 位小数）。
#[derive(Debug, Serialize)]
pub struct Percentiles {
    pub p50_ms: String,
    pub p95_ms: String,
    pub p99_ms: String,
    /// 数值形式（桶上界，ns；+Inf 尾桶为 u64::MAX）——验收判定的数据源，
    /// 与字符串展示同源同值。
    #[serde(skip)]
    pub p99_ns: Option<u64>,
}

impl Percentiles {
    /// 从全程累计直方图（最后一次采样即进程启动以来全量）提取。
    pub fn from_hist(hist: &HistSnapshot) -> Option<Self> {
        if hist.count == 0 {
            return None;
        }
        let fmt = |bound: Option<u64>| match bound {
            Some(b) if b == u64::MAX => ">300s".to_owned(),
            Some(b) => format!("{:.3}", b as f64 / 1e6),
            None => "n/a".to_owned(),
        };
        let p99 = hist.percentile_upper_bound(0.99);
        Some(Self {
            p50_ms: fmt(hist.percentile_upper_bound(0.50)),
            p95_ms: fmt(hist.percentile_upper_bound(0.95)),
            p99_ms: fmt(p99),
            p99_ns: p99,
        })
    }
}

/// 汇总统计（§34.2 报告必录项）。
#[derive(Debug, Serialize)]
pub struct Summary {
    /// 稳态窗口吞吐（observations/s，剔除预热期）。
    pub steady_throughput_obs_per_s: Option<f64>,
    pub schedule_delay: Option<Percentiles>,
    pub request_latency: Option<Percentiles>,
    pub mqtt_publish: Option<Percentiles>,
    pub wal_persist: Option<Percentiles>,
    /// 平均 CPU 占用（%；单核为 100% 基准；仅 Linux）。
    pub cpu_percent_avg: Option<f64>,
    pub rss_start_mib: Option<f64>,
    pub rss_max_mib: Option<f64>,
    /// 稳态 RSS 漂移（预热结束基线 → 运行期最大值；§34.2 ≤10%）。
    pub rss_drift_pct: Option<f64>,
    pub wal_backlog_max: i64,
}

/// 完整报告。
#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub schema: &'static str,
    pub scenario: String,
    /// formal = 正式验收口径；smoke = 冒烟（性能项 SKIP）。
    pub mode: &'static str,
    pub broker_mode: String,
    pub env: EnvironmentInfo,
    pub summary: Summary,
    pub accounting: AccountingSnapshot,
    pub criteria: Vec<Criterion>,
    /// 场景特有备注（平台收尾方式、故障窗口参数等）。
    pub notes: Vec<String>,
}

impl BenchReport {
    /// 写出 JSON + Markdown 到 `dir`（目录自动创建），返回 JSON 路径。
    pub fn write_outputs(&self, dir: &Path) -> std::io::Result<std::path::PathBuf> {
        std::fs::create_dir_all(dir)?;
        let json_path = dir.join("bench-report.json");
        std::fs::write(
            &json_path,
            serde_json::to_vec_pretty(self).expect("报告序列化"),
        )?;
        std::fs::write(dir.join("bench-report.md"), self.render_markdown())?;
        Ok(json_path)
    }

    fn render_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("# ForgeLink 基准报告 — {}\n\n", self.scenario));
        out.push_str(&format!(
            "- mode: `{}`  broker: `{}`\n",
            self.mode, self.broker_mode
        ));
        out.push_str(&format!("- OS: {}\n", self.env.os));
        out.push_str(&format!(
            "- CPU: {} × {}\n",
            self.env.cpu_model, self.env.cpu_cores
        ));
        out.push_str(&format!("- MEM: {}\n", self.env.mem_total));
        out.push_str(&format!("- DISK: {}（人工填写）\n", self.env.disk));
        out.push_str(&format!("- rustc: {}\n", self.env.rustc_version));
        out.push_str(&format!("- commit: {}\n", self.env.git_commit));
        if !self.env.perf_metrics_collected {
            out.push_str("- **性能指标（RSS/CPU）本平台不采集（复验平台，§34.2）**\n");
        }
        out.push_str("\n## 汇总\n\n");
        if let Some(t) = self.summary.steady_throughput_obs_per_s {
            out.push_str(&format!("- 稳态吞吐：{t:.0} obs/s\n"));
        }
        for (label, p) in [
            ("调度延迟", &self.summary.schedule_delay),
            ("设备请求延迟", &self.summary.request_latency),
            ("MQTT 发布", &self.summary.mqtt_publish),
            ("WAL 落盘", &self.summary.wal_persist),
        ] {
            if let Some(p) = p {
                out.push_str(&format!(
                    "- {label} p50/p95/p99：{} / {} / {} ms\n",
                    p.p50_ms, p.p95_ms, p.p99_ms
                ));
            }
        }
        if let Some(cpu) = self.summary.cpu_percent_avg {
            out.push_str(&format!("- 平均 CPU：{cpu:.1}%\n"));
        }
        if let (Some(start), Some(max), Some(drift)) = (
            self.summary.rss_start_mib,
            self.summary.rss_max_mib,
            self.summary.rss_drift_pct,
        ) {
            out.push_str(&format!(
                "- RSS：start {start:.1} MiB / max {max:.1} MiB / 漂移 {drift:.1}%\n"
            ));
        }
        out.push_str(&format!(
            "- WAL backlog 峰值：{} 条\n",
            self.summary.wal_backlog_max
        ));
        out.push_str("\n## 记账\n\n");
        let a = &self.accounting;
        out.push_str(&format!(
            "- 收到批次 {}（observations {}，单批最大 {}）\n",
            a.received_batches, a.received_observations, a.max_batch_observations
        ));
        out.push_str(&format!(
            "- 重复 {}，丢失候选 {}，补传批次 {}，解析错误 {}，非法序号 {}\n",
            a.duplicates,
            a.loss_candidates,
            a.replayed_batches,
            a.parse_errors,
            a.invalid_sequences
        ));
        if !self.notes.is_empty() {
            out.push_str("\n## 备注\n\n");
            for n in &self.notes {
                out.push_str(&format!("- {n}\n"));
            }
        }
        out.push_str("\n## 验收判定\n\n");
        out.push_str("| 项目 | 要求 | 实测 | 判定 |\n|---|---|---|---|\n");
        for c in &self.criteria {
            let verdict = match c.verdict {
                Verdict::Pass => "PASS",
                Verdict::Fail => "**FAIL**",
                Verdict::Skip => "SKIP",
            };
            out.push_str(&format!(
                "| {} | {} | {} | {verdict} |\n",
                c.name, c.requirement, c.actual
            ));
        }
        out
    }
}

/// 从样本序列计算汇总统计。
///
/// `warmup: Duration` 之前的样本不计入吞吐与 RSS 基线（预热剔除）。
pub fn summarize(samples: &[Sample], warmup: Duration) -> Summary {
    let last = samples.last();
    let steady_start = samples
        .iter()
        .position(|s| Duration::from_millis(s.at_ms.saturating_sub(samples[0].at_ms)) >= warmup);
    let throughput = match (steady_start, last) {
        (Some(i), Some(last)) if samples.len() > i + 1 => {
            let a = &samples[i];
            let dt_ms = last.at_ms.saturating_sub(a.at_ms);
            (dt_ms >= 1_000).then(|| {
                (last.obs_total.saturating_sub(a.obs_total)) as f64 / (dt_ms as f64 / 1_000.0)
            })
        }
        _ => None,
    };
    let rss_series: Vec<u64> = samples.iter().filter_map(|s| s.rss_bytes).collect();
    let (rss_start, rss_max, drift) = match (steady_start, rss_series.len()) {
        (Some(i), n) if n > i => {
            let baseline = rss_series[i] as f64;
            let max = rss_series[i..].iter().copied().max().unwrap_or(0) as f64;
            (
                Some(rss_series[0] as f64 / (1024.0 * 1024.0)),
                Some(max / (1024.0 * 1024.0)),
                ((max - baseline) / baseline * 100.0).max(0.0),
            )
        }
        _ => (None, None, f64::NAN),
    };
    let drift = if drift.is_nan() { None } else { Some(drift) };
    let cpu = cpu_percent_avg(samples);
    Summary {
        steady_throughput_obs_per_s: throughput,
        schedule_delay: last.and_then(|s| Percentiles::from_hist(&s.schedule_delay_hist)),
        request_latency: last.and_then(|s| Percentiles::from_hist(&s.request_latency_hist)),
        mqtt_publish: last.and_then(|s| Percentiles::from_hist(&s.publish_ns_hist)),
        wal_persist: last.and_then(|s| Percentiles::from_hist(&s.persist_ns_hist)),
        cpu_percent_avg: cpu,
        rss_start_mib: rss_start,
        rss_max_mib: rss_max,
        rss_drift_pct: drift,
        wal_backlog_max: samples.iter().map(|s| s.wal_inflight).max().unwrap_or(0),
    }
}

/// 平均 CPU 占用（滴差差分；CLK_TCK=100 假设，单核 100% 基准）。
fn cpu_percent_avg(samples: &[Sample]) -> Option<f64> {
    let mut sum = 0.0f64;
    let mut n = 0u64;
    for pair in samples.windows(2) {
        let (Some(a), Some(b)) = (pair[0].cpu_ticks, pair[1].cpu_ticks) else {
            continue;
        };
        let dt_ms = pair[1].at_ms.saturating_sub(pair[0].at_ms);
        if dt_ms == 0 {
            continue;
        }
        let ticks = b.saturating_sub(a) as f64;
        // 100 ticks/s；dt 内可用 ticks = dt_ms/1000*100。
        sum += ticks / (dt_ms as f64 / 1_000.0 * 100.0) * 100.0;
        n += 1;
    }
    (n > 0).then(|| sum / n as f64)
}

/// 便捷判定构造。
pub fn criterion(name: &str, requirement: &str, actual: String, pass: bool) -> Criterion {
    Criterion {
        name: name.to_owned(),
        requirement: requirement.to_owned(),
        actual,
        verdict: if pass { Verdict::Pass } else { Verdict::Fail },
    }
}

/// 冒烟模式跳过项。
pub fn skip_criterion(name: &str, requirement: &str) -> Criterion {
    Criterion {
        name: name.to_owned(),
        requirement: requirement.to_owned(),
        actual: "smoke 模式不判定".to_owned(),
        verdict: Verdict::Skip,
    }
}
