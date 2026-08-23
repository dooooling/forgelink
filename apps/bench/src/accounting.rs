//! 流式记账订阅端：丢失/重复/补传的 O(设备数) 内存判定。
//!
//! # 设计约束（模块契约）
//!
//! - 每设备维护**连续水位线** `watermark`（已按序收到的最大批次序号）
//!   与**待填间隙集** `pending_gaps`（乱序/重发尚未补齐的序号）：
//!   - `seq > watermark`：`(watermark, seq)` 开区间计入间隙，推进水位线；
//!   - `seq ∈ pending_gaps`：从间隙移除（乱序补齐或 QoS1 重发），计一次
//!     有效接收；
//!   - `seq ≤ watermark` 且不在间隙中：QoS1 重复投递，计重复；
//!   - 场景结束时仍未填充的间隙 = **丢失候选**。
//! - 跨会话判重（crash-wal 重启后 per-device `sequence` 归零、
//!   `message_id` 内嵌新 session 后缀）：由 crash-wal 场景在重启点切换
//!   新账本实例实现——本模块不维护全量 message_id 集合（72h soak 的
//!   批次量级下会无限膨胀），内存上界 = 设备数 × 间隙数。
//! - 记账任务必须独占并及时排空 Feed（无界通道，消费滞后即内存膨胀）。

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::broker::Feed;

/// 单设备台账。
#[derive(Debug, Default)]
struct DeviceLedger {
    /// 已按序收到的最大批次序号（i64 以容纳 -1 初始值；u64 序号经
    /// try_into 转换，超界视为协议违规计数）。
    watermark: i64,
    pending_gaps: BTreeSet<u64>,
    received_batches: u64,
    received_observations: u64,
    duplicates: u64,
    invalid: u64,
}

impl DeviceLedger {
    fn record(&mut self, seq: u64, obs_count: u64) {
        if let Ok(seq) = i64::try_from(seq) {
            if seq <= self.watermark {
                // 水位线之下的序号：间隙补齐算有效接收，否则为重复。
                if self.pending_gaps.remove(&(seq as u64)) {
                    self.received_batches += 1;
                    self.received_observations += obs_count;
                    return;
                }
                self.duplicates += 1;
                return;
            }
            // 推进水位线，跳过的序号进入待填间隙。
            for missing in (self.watermark + 1)..seq {
                self.pending_gaps.insert(missing as u64);
            }
            self.watermark = seq;
        } else {
            self.invalid += 1;
            return;
        }
        self.received_batches += 1;
        self.received_observations += obs_count;
    }

    /// 未填充的丢失候选数。
    fn loss_candidates(&self) -> u64 {
        self.pending_gaps.len() as u64
    }
}

/// 记账汇总快照（报告与静默判定用）。
#[derive(Debug, Clone, Default, Serialize)]
pub struct AccountingSnapshot {
    pub received_batches: u64,
    pub received_observations: u64,
    pub duplicates: u64,
    /// 场景结束时仍未补齐的序号总数（丢失候选）。
    pub loss_candidates: u64,
    /// `replayed=true` 的批次数（WAL 补传标记）。
    pub replayed_batches: u64,
    /// 单批最大 Observation 数（「单批 ≥1000」验收的实测来源）。
    pub max_batch_observations: u64,
    pub parse_errors: u64,
    pub invalid_sequences: u64,
}

/// 共享记账状态句柄。
#[derive(Clone)]
pub struct Accounting {
    state: Arc<Mutex<AccountingInner>>,
}

struct AccountingInner {
    devices: HashMap<String, DeviceLedger>,
    replayed_batches: u64,
    max_batch_observations: u64,
    parse_errors: u64,
}

impl Accounting {
    /// 空账本（场景启动时创建）。
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(AccountingInner {
                devices: HashMap::new(),
                replayed_batches: 0,
                max_batch_observations: 0,
                parse_errors: 0,
            })),
        }
    }

    /// 启动后台消费任务：独占排空 Feed 直到通道关闭。返回后快照可随时
    /// 通过 [`Self::snapshot`] 读取。
    pub fn spawn(self, mut feed: Feed) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(msg) = feed.recv().await {
                self.record_payload(&msg.payload);
            }
        })
    }

    /// 解析并记录一条 Telemetry Batch 载荷（解析失败计错误不 panic——
    /// 基准不得因个别坏报文中断）。
    pub fn record_payload(&self, payload: &[u8]) {
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
            self.state.lock().expect("记账锁").parse_errors += 1;
            return;
        };
        let device = v
            .get("device_id")
            .and_then(|d| d.as_str())
            .unwrap_or_default()
            .to_owned();
        let seq = v.get("sequence").and_then(|s| s.as_u64());
        let obs_count = v
            .get("observations")
            .and_then(|o| o.as_array())
            .map(|a| a.len() as u64)
            .unwrap_or(0);
        let replayed = v.get("replayed").and_then(|r| r.as_bool()).unwrap_or(false);
        let mut inner = self.state.lock().expect("记账锁");
        match (device.is_empty(), seq) {
            (true, _) | (_, None) => {
                inner.parse_errors += 1;
            }
            (false, Some(seq)) => {
                inner
                    .devices
                    .entry(device)
                    .or_default()
                    .record(seq, obs_count);
                if replayed {
                    inner.replayed_batches += 1;
                }
                inner.max_batch_observations = inner.max_batch_observations.max(obs_count);
            }
        }
    }

    /// 当前快照。
    pub fn snapshot(&self) -> AccountingSnapshot {
        let inner = self.state.lock().expect("记账锁");
        AccountingSnapshot {
            received_batches: inner.devices.values().map(|l| l.received_batches).sum(),
            received_observations: inner
                .devices
                .values()
                .map(|l| l.received_observations)
                .sum(),
            duplicates: inner.devices.values().map(|l| l.duplicates).sum(),
            loss_candidates: inner.devices.values().map(|l| l.loss_candidates()).sum(),
            replayed_batches: inner.replayed_batches,
            max_batch_observations: inner.max_batch_observations,
            parse_errors: inner.parse_errors,
            invalid_sequences: inner.devices.values().map(|l| l.invalid).sum(),
        }
    }
}
