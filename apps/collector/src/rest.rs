//! Collector REST v1 只读适配层（§31.5/§31.6/§104）。
//!
//! `CollectorApiState` 实现 [`rest_api::ApiState`]：请求时同步取齐
//! 一份快照（静态设备元数据 + 动态健康，短锁/原子，
//! 不跨 `await` 持锁），REST 服务器由此独立于采集链路运行——API 停止
//! 不影响 Poll/WAL/MQTT。
//!
//! 安全边界（§90.1）：视图**不含** Driver 连接配置（`connection`）、
//! 属性 Driver 地址（§10 私有不透明数据）、MQTT/TLS 凭据与 WAL 文件
//! 路径；`CollectorApiState` 只从注册后的静态元数据与健康状态构造，
//! 不引用任何配置原文。

use std::collections::BTreeMap;
use std::sync::Arc;

use rest_api::models::{ApiSnapshot, BufferView, DeviceView, GroupView, MqttView, PropertyView};
use rest_api::{ApiState, StateError};

use crate::health::CollectorHealth;
use crate::tasks::HealthState;

/// 静态设备元数据（注册后不变；REST 视图与健康快照的静态部分）。
#[derive(Debug, Clone)]
pub struct DeviceMeta {
    pub device_id: String,
    pub name: String,
    pub domain: String,
    pub driver_id: String,
    pub profile_id: String,
    pub enabled: bool,
    pub labels: BTreeMap<String, String>,
    /// 分组信息（间隔 + 组内属性路径，路径序稳定）。
    pub groups: Vec<GroupMeta>,
    /// 全部属性语义视图（去重，按声明序）。
    pub properties: Vec<PropertyView>,
    /// 由属性路径派生的资源树（§5 最小实现）。
    pub resources: Vec<rest_api::models::ResourceView>,
}

/// 采集分组静态信息（§22 Group 的可公开子集）。
#[derive(Debug, Clone)]
pub struct GroupMeta {
    pub interval_ms: u64,
    pub paths: Vec<String>,
}

/// 健康快照需要的设备概要（id/enabled/读取项数/组数）。
type DeviceSummary = (String, bool, usize, usize);

/// Collector 只读快照提供者：静态元数据 + 动态健康。
pub(crate) struct CollectorApiState {
    health: Arc<HealthState>,
    site_id: String,
    session_id: String,
    devices: Vec<DeviceMeta>,
    summary: Vec<DeviceSummary>,
}

impl CollectorApiState {
    /// 构造适配层（`health` 与运行时的共享健康状态同源）。
    pub(crate) fn new(
        health: Arc<HealthState>,
        site_id: String,
        session_id: String,
        devices: Vec<DeviceMeta>,
    ) -> Self {
        let summary = devices
            .iter()
            .map(|d| {
                (
                    d.device_id.clone(),
                    d.enabled,
                    d.properties.len(),
                    d.groups.len(),
                )
            })
            .collect();
        Self {
            health,
            site_id,
            session_id,
            devices,
            summary,
        }
    }
}

impl ApiState for CollectorApiState {
    fn snapshot(&self) -> Result<ApiSnapshot, StateError> {
        let h: CollectorHealth = self.health.snapshot(&self.summary);
        let mut views = Vec::with_capacity(self.devices.len());
        for meta in &self.devices {
            let health = h.devices.iter().find(|d| d.device_id == meta.device_id);
            views.push(DeviceView {
                device_id: meta.device_id.clone(),
                name: meta.name.clone(),
                domain: meta.domain.clone(),
                driver_id: meta.driver_id.clone(),
                profile_id: meta.profile_id.clone(),
                enabled: meta.enabled,
                labels: meta.labels.clone(),
                read_items: meta.properties.len(),
                groups: meta
                    .groups
                    .iter()
                    .map(|g| GroupView {
                        interval_ms: g.interval_ms,
                        read_items: g.paths.len(),
                        paths: g.paths.clone(),
                    })
                    .collect(),
                properties: meta.properties.clone(),
                resources: meta.resources.clone(),
                last_batch_at_ns: health.and_then(|d| d.last_batch_at_ns),
                last_error: health.and_then(|d| d.last_error.clone()),
            });
        }
        Ok(ApiSnapshot {
            site_id: self.site_id.clone(),
            session_id: self.session_id.clone(),
            started_at_ns: h.started_at_ns,
            devices: views,
            mqtt: MqttView {
                last_acked_at_ns: h.mqtt.last_acked_at_ns,
                last_failed_at_ns: h.mqtt.last_failed_at_ns,
                last_error: h.mqtt.last_error,
                publishes_acked: h.mqtt.publishes_acked,
                publishes_failed: h.mqtt.publishes_failed,
            },
            buffer: BufferView {
                inflight: h.buffer.inflight,
                replayed_batches: h.buffer.replayed_batches,
            },
        })
    }
}

/// 从已注册的 DeviceManager 提取静态元数据（注册后调用一次）。
///
/// 只读取 Profile/读取项/分组的**语义**视图：属性路径、类型、单位、
/// 读写标志、范围与间隔；**不**包含 `driver_address`（Driver 私有
/// 数据）与连接配置。
pub fn extract_device_meta(manager: &device_manager::DeviceManager) -> Vec<DeviceMeta> {
    let mut out = Vec::new();
    for device_id in manager.device_ids() {
        let Some(instance) = manager.get(device_id) else {
            continue;
        };
        let mut properties: Vec<PropertyView> = Vec::new();
        let mut groups: Vec<GroupMeta> = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for group in &instance.groups {
            let paths: Vec<String> = group
                .read_items
                .iter()
                .map(|item| item.path.clone())
                .collect();
            groups.push(GroupMeta {
                interval_ms: group.interval_ms,
                paths: paths.clone(),
            });
            for item in &group.read_items {
                if !seen.insert(item.path.clone()) {
                    continue;
                }
                let p = &item.property;
                properties.push(PropertyView {
                    path: p.path.clone(),
                    display_name: p.path.clone(),
                    value_type: data_type_str(&p.value_type),
                    unit: p.unit.clone(),
                    readable: p.readable,
                    writable: p.writable,
                    min: p.min.as_ref().and_then(value_to_json),
                    max: p.max.as_ref().and_then(value_to_json),
                    interval_ms: item.interval_ms,
                });
            }
        }
        let paths: Vec<String> = properties.iter().map(|p| p.path.clone()).collect();
        let resources = rest_api::resource::derive_resources(&paths);
        out.push(DeviceMeta {
            device_id: instance.device.id.clone(),
            name: instance.device.name.clone(),
            domain: domain_str(&instance.device.domain),
            driver_id: instance.device.driver_id.clone(),
            profile_id: instance.device.profile_id.clone(),
            enabled: instance.device.enabled,
            labels: instance.device.labels.clone(),
            groups,
            properties,
            resources,
        });
    }
    out.sort_by(|a, b| a.device_id.cmp(&b.device_id));
    out
}

/// `DomainKind` → 稳定字符串（与 serde 序列化名一致，如 `drive`）。
fn domain_str(domain: &crate::DomainKind) -> String {
    serde_json::to_value(domain)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_owned()))
        .unwrap_or_else(|| format!("{domain:?}"))
}

/// `DataType` → 稳定字符串（与 serde 序列化名一致，如 `f64`/`bool`）。
fn data_type_str(dt: &crate::DataType) -> String {
    serde_json::to_value(dt)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_owned()))
        .unwrap_or_else(|| format!("{dt:?}"))
}

/// `observation_model::Value` → JSON 值（无法转换时返回 `None`）。
fn value_to_json(v: &crate::Value) -> Option<serde_json::Value> {
    serde_json::to_value(v).ok()
}
