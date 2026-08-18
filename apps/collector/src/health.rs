//! Collector 健康状态（§104 Watchdog：采集/缓冲/发布三面健康，结构化
//! 日志 + 可查询状态；REST 阶段在此之上暴露 HTTP 端点）。

use std::fmt;

/// Collector 运行状态快照。
#[derive(Debug, Clone, Default)]
pub struct CollectorHealth {
    pub site_id: String,
    pub session_id: String,
    /// 启动时刻（纳秒，UNIX 时间）。
    pub started_at_ns: i64,
    pub devices: Vec<DeviceHealth>,
    pub mqtt: MqttHealth,
    pub buffer: BufferHealth,
}

/// 单台采集设备健康。
#[derive(Debug, Clone, Default)]
pub struct DeviceHealth {
    pub device_id: String,
    pub enabled: bool,
    /// 读取项总数。
    pub read_items: usize,
    /// 轮询组数（按间隔分组的批量读取）。
    pub groups: usize,
    /// 最近一次成功批次到达时刻（纳秒）。
    pub last_batch_at_ns: Option<i64>,
    /// 最近一次失败详情（驱动错误，§9）。
    pub last_error: Option<String>,
}

/// 北向发布健康（由发送循环观测维护）。
#[derive(Debug, Clone, Default)]
pub struct MqttHealth {
    /// 最近一次 PUBACK 确认时刻（纳秒）。
    pub last_acked_at_ns: Option<i64>,
    /// 最近一次发布失败时刻（纳秒）。
    pub last_failed_at_ns: Option<i64>,
    pub last_error: Option<String>,
    /// 累计 PUBACK 确认（WAL 已删除）的批次。
    pub publishes_acked: u64,
    /// 累计发布失败（已 requeue 保留）的批次。
    pub publishes_failed: u64,
}

/// 本地缓冲健康。
#[derive(Debug, Clone, Default)]
pub struct BufferHealth {
    pub db_path: String,
    /// 当前在途（已取出未确认）批次近似数。
    pub inflight: usize,
    /// 累计补传（replayed=true）批次。
    pub replayed_batches: u64,
}

impl CollectorHealth {
    /// 是否有任何设备处于异常（最近批失败）。
    pub fn has_device_errors(&self) -> bool {
        self.devices.iter().any(|d| d.last_error.is_some())
    }
}

impl fmt::Display for CollectorHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "site={} session={} devices={} (errors={}) mqtt_acked={} mqtt_failed={} inflight={} replayed={}",
            self.site_id,
            self.session_id,
            self.devices.len(),
            self.devices
                .iter()
                .filter(|d| d.last_error.is_some())
                .count(),
            self.mqtt.publishes_acked,
            self.mqtt.publishes_failed,
            self.buffer.inflight,
            self.buffer.replayed_batches,
        )
    }
}
