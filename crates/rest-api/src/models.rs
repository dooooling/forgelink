//! REST v1 只读视图模型（§31.5/§31.6）。
//!
//! 全部响应模型显式携带 `schema` 版本字段；字段名统一 `snake_case`。
//! 本模块只包含**可安全公开**的字段：禁止连接配置、凭据、证书内容、
//! Driver 私有地址与内部实现细节（§90.1 安全基线）。

use std::collections::BTreeMap;

use serde::Serialize;

/// 设备列表响应（`GET /api/v1/devices`）。
#[derive(Debug, Clone, Serialize)]
pub struct DevicesResponse {
    pub schema: &'static str,
    pub devices: Vec<DeviceView>,
}

impl DevicesResponse {
    pub const SCHEMA: &'static str = "forgelink.devices.v1";
}

/// 单设备响应（`GET /api/v1/devices/{device_id}`）。
#[derive(Debug, Clone, Serialize)]
pub struct DeviceResponse {
    pub schema: &'static str,
    pub device: DeviceView,
}

impl DeviceResponse {
    pub const SCHEMA: &'static str = "forgelink.device.v1";
}

/// 设备资源树响应（`GET /api/v1/devices/{device_id}/resources`）。
#[derive(Debug, Clone, Serialize)]
pub struct ResourcesResponse {
    pub schema: &'static str,
    /// 顶层资源（含嵌套 `children`）。
    pub resources: Vec<ResourceView>,
}

impl ResourcesResponse {
    pub const SCHEMA: &'static str = "forgelink.resources.v1";
}

/// 设备属性清单响应（`GET /api/v1/devices/{device_id}/properties`）。
#[derive(Debug, Clone, Serialize)]
pub struct PropertiesResponse {
    pub schema: &'static str,
    pub properties: Vec<PropertyView>,
}

impl PropertiesResponse {
    pub const SCHEMA: &'static str = "forgelink.properties.v1";
}

/// 健康检查响应（`GET /api/v1/health`，§104 Health endpoint）。
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub schema: &'static str,
    /// 汇总状态：`ok`（全部正常）/ `degraded`（设备采集、北向发布或
    /// WAL 在途存在异常）。
    pub status: HealthStatus,
    pub site_id: String,
    pub session_id: String,
    pub started_at_ns: i64,
    pub devices: Vec<DeviceView>,
    pub mqtt: MqttView,
    pub buffer: BufferView,
}

impl HealthResponse {
    pub const SCHEMA: &'static str = "forgelink.health.v1";
}

/// 健康汇总状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Ok,
    Degraded,
}

/// 单设备只读视图（§4.2 Device + 采集健康，§104）。
///
/// 安全边界（§90.1）：**不含** `connection`（Driver 连接配置）与
/// `labels` 以外的自由文本；属性只返回语义视图，不含 Driver 地址。
#[derive(Debug, Clone, Serialize)]
pub struct DeviceView {
    pub device_id: String,
    /// 显示名称（缺省与 device_id 相同）。
    pub name: String,
    /// 业务类别（§107：`drive`/`plc`/`cnc`…）。
    pub domain: String,
    pub driver_id: String,
    pub profile_id: String,
    pub enabled: bool,
    pub labels: BTreeMap<String, String>,
    /// 读取项总数（§22 Tag 数）。
    pub read_items: usize,
    /// 按采集间隔分组的读取项（§22 Group）。
    pub groups: Vec<GroupView>,
    /// 属性语义清单（不含 Driver 地址，§10 私有不透明数据）。
    pub properties: Vec<PropertyView>,
    /// 由属性路径派生的资源树（§5 Resource 最小视图）。
    pub resources: Vec<ResourceView>,
    /// 最近一次成功批次到达时刻（纳秒）；从未成功时为 `null`。
    pub last_batch_at_ns: Option<i64>,
    /// 最近一次失败的**稳定错误码**（如 `connection_lost`/
    /// `timeout`/`map_failed`）；无异常时为 `null`。§90.1：驱动原始
    /// 错误文本可能含地址等内部细节，只进脱敏日志，不回传本字段。
    pub last_error: Option<String>,
}

/// 采集分组视图（§22 Group）。
#[derive(Debug, Clone, Serialize)]
pub struct GroupView {
    /// 采集间隔（毫秒）。
    pub interval_ms: u64,
    /// 本组读取项数。
    pub read_items: usize,
    /// 本组属性语义路径（§6.1；不含 Driver 地址）。
    pub paths: Vec<String>,
}

/// 属性语义视图（§37 ProfileProperty 的可公开子集）。
#[derive(Debug, Clone, Serialize)]
pub struct PropertyView {
    pub path: String,
    pub display_name: String,
    /// 语义数据类型（`f64`/`bool`/…）。
    pub value_type: String,
    pub unit: Option<String>,
    pub readable: bool,
    pub writable: bool,
    /// 语义值范围（写入校验依据，§37.1）。
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
    /// 推荐采集间隔（毫秒）；仅可写属性（无采集）为 `null`。
    pub interval_ms: Option<u64>,
}

/// 资源节点视图（§5 Resource 最小实现：由属性路径派生）。
///
/// 路径分隔符为 `.`（与语义属性路径一致，如 `drive.output`）；`kind`
/// 取路径首段（对应 Domain 标准前缀，§41~§47）。
#[derive(Debug, Clone, Serialize)]
pub struct ResourceView {
    /// 资源路径（不含设备 ID，如 `drive.output`）。
    pub path: String,
    /// 资源类型标识（`drive`/`cnc`/…，来自路径首段）。
    pub kind: String,
    pub display_name: String,
    /// 直接挂在本资源下的属性路径。
    pub properties: Vec<String>,
    /// 子资源路径。
    pub children: Vec<String>,
}

/// 北向发布健康（§31.3 确认语义；与 `CollectorHealth.mqtt` 同构）。
#[derive(Debug, Clone, Serialize)]
pub struct MqttView {
    /// 最近一次 PUBACK 确认时刻（纳秒）；从未确认时为 `null`。
    pub last_acked_at_ns: Option<i64>,
    /// 最近一次发布失败时刻（纳秒）；从未失败时为 `null`。
    pub last_failed_at_ns: Option<i64>,
    /// 最近一次发布失败的**稳定错误码**（如 `disconnected`/
    /// `publish_failed`）。§90.1：`MqttClientError` 原文可能含 broker
    /// 地址/主题等细节，只进脱敏日志，不回传本字段。
    pub last_error: Option<String>,
    /// 累计 PUBACK 确认（WAL 已删除）的批次。
    pub publishes_acked: u64,
    /// 累计发布失败（已 requeue 保留）的批次。
    pub publishes_failed: u64,
}

/// 本地缓冲/WAL 健康（§103；不暴露文件路径等内部细节，§90.1）。
#[derive(Debug, Clone, Serialize)]
pub struct BufferView {
    /// 当前在途（已取出未确认）批次近似数。
    pub inflight: usize,
    /// 累计补传（replayed=true）批次。
    pub replayed_batches: u64,
}

/// 运行时只读快照（适配层单次取齐，禁止跨 `await` 持锁）。
#[derive(Debug, Clone)]
pub struct ApiSnapshot {
    pub site_id: String,
    pub session_id: String,
    pub started_at_ns: i64,
    pub devices: Vec<DeviceView>,
    pub mqtt: MqttView,
    pub buffer: BufferView,
}

impl ApiSnapshot {
    /// 是否存在需要关注的状态（health 汇总降级判定，评审 P2）：
    ///
    /// - 任一设备最近采集失败；
    /// - 北向发布异常（`last_error` 存在，或累计失败 > 0）；
    /// - WAL 在途异常（在途记录滞留——存在未确认在途且北向最近失败）。
    pub fn has_anomalies(&self) -> bool {
        self.devices.iter().any(|d| d.last_error.is_some())
            || self.mqtt.last_error.is_some()
            || self.mqtt.publishes_failed > 0
            || (self.buffer.inflight > 0 && self.mqtt.last_failed_at_ns.is_some())
    }
}

/// 单个指标快照值（§34.2.1；`kind` 判别三种语义）。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetricView {
    /// 累加计数（`*_total`）。
    Count { value: u64 },
    /// 即时量（`*_gauge`）。
    Gauge { value: i64 },
    /// 直方图：固定桶边界（ns）与各桶计数、总和、次数。
    Histogram {
        bounds: Vec<u64>,
        counts: Vec<u64>,
        sum: u64,
        count: u64,
    },
}

/// 指标快照响应（`GET /api/v1/metrics`，§34.2.1）。
///
/// 管理接口非控制面：只读构建同样可用；值来自进程内注册表快照，
/// 不含文件路径/地址/凭据（§90.1 安全基线）。
#[derive(Debug, Clone, Serialize)]
pub struct MetricsResponse {
    pub schema: &'static str,
    /// 快照时刻（UTC Unix Epoch ns）。
    pub captured_at_ns: i64,
    /// 已注册指标的当前值（名称 → 值；空注册表序列化为 `{}`）。
    pub metrics: BTreeMap<String, MetricView>,
}

impl MetricsResponse {
    pub const SCHEMA: &'static str = "forgelink.metrics.v1";
}

impl From<metrics::MetricValue> for MetricView {
    fn from(value: metrics::MetricValue) -> Self {
        match value {
            metrics::MetricValue::Count(v) => Self::Count { value: v },
            metrics::MetricValue::Gauge(v) => Self::Gauge { value: v },
            metrics::MetricValue::Histogram {
                bounds,
                counts,
                sum,
                count,
            } => Self::Histogram {
                bounds: bounds.to_vec(),
                counts,
                sum,
                count,
            },
        }
    }
}
