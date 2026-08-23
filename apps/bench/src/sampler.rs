//! 周期采样：REST `/api/v1/metrics` 快照差值 + Linux `/proc` 进程资源。
//!
//! - 指标经手写 HTTP/1.0 GET（零额外依赖；管理接口本机回环）；
//! - RSS/CPU 仅 Linux 采集（§34.2：x64 主基线即 Linux；Windows 为复验
//!   平台，字段留 `None` 并在报告中标注）；
//! - 样本序列全量驻留内存（30min@2s ≈ 900 点、72h@30s ≈ 8.6k 点，均为
//!   小结构），同时**逐样本追加落盘** `samples.jsonl`——长跑中途崩溃
//!   不丢已测数据（soak 断点续测的依据）。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 直方图快照（桶边界与计数拷贝）。
#[derive(Debug, Clone, Default, Serialize)]
pub struct HistSnapshot {
    pub bounds: Vec<u64>,
    pub counts: Vec<u64>,
    pub sum: u64,
    pub count: u64,
}

impl HistSnapshot {
    /// 累计占比法分位数：返回首个累计计数 ≥ ceil(p×count) 的**桶上界**。
    /// 桶恰在验收阈值上界时判定精确；FAIL 时只能给出区间（报告措辞
    /// 如实标注"∈ (前桶, 本桶]"，不伪造插值精度）。count=0 返回 None。
    pub fn percentile_upper_bound(&self, p: f64) -> Option<u64> {
        if self.count == 0 || self.bounds.is_empty() {
            return None;
        }
        let target = ((self.count as f64) * p).ceil() as u64;
        let mut cum = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            cum += c;
            if cum >= target && i < self.bounds.len() {
                return Some(self.bounds[i]);
            }
        }
        self.bounds.last().copied()
    }
}

/// 单次采样帧。
#[derive(Debug, Clone, Default, Serialize)]
pub struct Sample {
    /// UNIX 毫秒。
    pub at_ms: u64,
    pub obs_total: u64,
    pub batches_flushed_total: u64,
    pub poll_batches_total: u64,
    pub wal_inflight: i64,
    pub mqtt_inflight: i64,
    pub mqtt_published_total: u64,
    pub mqtt_redelivered_total: u64,
    pub mqtt_failed_total: u64,
    pub wal_replayed_total: u64,
    pub wal_ack_dropped_total: u64,
    pub poll_errors_timeout_total: u64,
    pub schedule_delay_hist: HistSnapshot,
    pub request_latency_hist: HistSnapshot,
    pub publish_ns_hist: HistSnapshot,
    pub persist_ns_hist: HistSnapshot,
    /// 进程 RSS 字节（仅 Linux）。
    pub rss_bytes: Option<u64>,
    /// 进程 CPU 累计时钟滴答（仅 Linux；CLK_TCK=100 假设）。
    pub cpu_ticks: Option<u64>,
}

/// 从 `/api/v1/metrics` 响应体抽取一帧样本。
pub fn parse_sample(body: &serde_json::Value) -> Sample {
    let empty = serde_json::Map::new();
    let metrics = body
        .as_object()
        .and_then(|o| o.get("metrics"))
        .and_then(|m| m.as_object())
        .unwrap_or(&empty);
    let counter = |name: &str| {
        metrics
            .get(name)
            .and_then(|v| v.get("value"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    };
    let gauge = |name: &str| -> i64 {
        metrics
            .get(name)
            .and_then(|v| v.get("value"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    };
    let hist = |name: &str| -> HistSnapshot {
        let Some(h) = metrics.get(name) else {
            return HistSnapshot::default();
        };
        HistSnapshot {
            bounds: h
                .get("bounds")
                .and_then(|b| b.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
                .unwrap_or_default(),
            counts: h
                .get("counts")
                .and_then(|c| c.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
                .unwrap_or_default(),
            sum: h.get("sum").and_then(|s| s.as_u64()).unwrap_or(0),
            count: h.get("count").and_then(|s| s.as_u64()).unwrap_or(0),
        }
    };
    Sample {
        at_ms: now_ms(),
        obs_total: counter("pipeline_observations_total"),
        batches_flushed_total: counter("pipeline_batches_flushed_total"),
        poll_batches_total: counter("poll_batches_total"),
        wal_inflight: gauge("wal_inflight_gauge"),
        mqtt_inflight: gauge("mqtt_inflight_gauge"),
        mqtt_published_total: counter("mqtt_published_total"),
        mqtt_redelivered_total: counter("mqtt_redelivered_total"),
        mqtt_failed_total: counter("mqtt_failed_total"),
        wal_replayed_total: counter("wal_replayed_total"),
        wal_ack_dropped_total: counter("wal_ack_dropped_total"),
        poll_errors_timeout_total: counter("poll_errors_timeout_total"),
        schedule_delay_hist: hist("schedule_delay_ns_hist"),
        request_latency_hist: hist("poll_request_ns_hist"),
        publish_ns_hist: hist("mqtt_publish_ns_hist"),
        persist_ns_hist: hist("wal_persist_ns_hist"),
        rss_bytes: None,
        cpu_ticks: None,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 健康探测：`/api/v1/health` 返回可解析 JSON 即视为就绪（§31.5）。
pub(crate) async fn health_ok(port: u16) -> Result<bool, std::io::Error> {
    let body = http_get_json(port, "/api/v1/health").await?;
    Ok(body.get("schema").is_some())
}

/// 手写 HTTP/1.0 GET：连接、请求、读完整响应、剥离头部取 JSON 体
/// （管理接口仅本机回环访问，无需完整 HTTP 客户端依赖）。
pub(crate) async fn http_get_json(port: u16, path: &str) -> std::io::Result<serde_json::Value> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    // 剥离 HTTP 头后定位 JSON 体边界（对 chunked/多余空白均鲁棒）。
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or_default();
    let start = body
        .find('{')
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "响应无 JSON 体"))?;
    let end = body
        .rfind('}')
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "响应 JSON 不完整"))?;
    serde_json::from_str(&body[start..=end])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Linux：读取子进程 RSS 与 CPU 滴答。stat 的 comm 字段可能含空格，
/// 以最后一个 ')' 切分后字段序号整体左移 3（从 state 起算）。
#[cfg(target_os = "linux")]
fn proc_sample(pid: u32) -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, rest) = stat.rsplit_once(')')?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // fields[0] = state（第 3 字段）；utime=第 14 → 下标 11，stime=第 15 → 12。
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kb = status.lines().find_map(|l| {
        l.strip_prefix("VmRSS:")
            .and_then(|r| r.trim().strip_suffix(" kB"))
            .and_then(|n| n.trim().parse::<u64>().ok())
    })?;
    Some((rss_kb * 1024, utime + stime))
}

#[cfg(not(target_os = "linux"))]
fn proc_sample(_pid: u32) -> Option<(u64, u64)> {
    None
}

/// 运行中的采样器句柄。
pub struct Sampler {
    samples: Arc<Mutex<Vec<Sample>>>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Sampler {
    /// 启动周期采样（collector 就绪后调用；REST 不可达的轮次跳过不
    /// 中断——故障场景中 broker/设备断连不影响 REST）。
    ///
    /// `checkpoint_path`：samples.jsonl 逐样本追加落盘。
    pub fn start(rest_port: u16, pid: u32, interval: Duration, checkpoint_path: PathBuf) -> Self {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let samples_for_task = samples.clone();
        let task = tokio::spawn(async move {
            // 追加写检查点文件（场景重启时截断重建由调用方负责）。
            let mut ckpt = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&checkpoint_path)
                .ok();
            while !*stop_rx.borrow() {
                if let Ok(body) = http_get_json(rest_port, "/api/v1/metrics").await {
                    let mut sample = parse_sample(&body);
                    if let Some((rss, ticks)) = proc_sample(pid) {
                        sample.rss_bytes = Some(rss);
                        sample.cpu_ticks = Some(ticks);
                    }
                    if let Ok(line) = serde_json::to_string(&sample) {
                        use std::io::Write as _;
                        if let Some(f) = ckpt.as_mut() {
                            let _ = writeln!(f, "{line}");
                        }
                    }
                    samples_for_task.lock().expect("采样锁").push(sample);
                }
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
        Self {
            samples,
            stop_tx,
            task: Some(task),
        }
    }

    /// 已采集的样本序列快照。
    pub fn samples(&self) -> Vec<Sample> {
        self.samples.lock().expect("采样锁").clone()
    }

    /// 停止并等待任务退出（保留样本可读）。
    pub async fn stop(&mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
