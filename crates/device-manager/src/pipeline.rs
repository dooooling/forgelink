//! 全链路映射：`RawReadResult → Profile → Domain → Observation`（§47、§7.3）。
//!
//! # 边界（§53）
//!
//! - Driver 只返回原始结果（含质量/时间戳），不产生 `Observation`；
//! - Profile 负责缩放/单位/类型映射（`profile_engine::decode_read`，§37.1）；
//! - Domain 负责语义路径校验与 `Observation` 组装
//!   （`domain_model::build_observation`，§7.3）。
//!
//! 本模块是三层边界的编排点，且只做编排，不解释协议私有字段。
//!
//! # 失败语义（§9）
//!
//! - 单条 `RawReadResult` 携带错误 → `Bad/ProtocolError`（decode_read 已处理）；
//! - 整批失败（`PollEvent::Failed`）→ 批内每个读取项产出 `Bad`，
//!   原因按 Driver 错误码映射（超时/连接/协议），原始 `protocol_code` 保留；
//! - 未知 `item_id`（驱动返回未请求项）→ 跳过并告警，不伪造值；
//! - `sequence` 由 [`SequenceAllocator`] 按设备统一分配：多采集组、多批次
//!   共享同一序列源，保证同设备同会话内单调递增（§31.3）。

use domain_model::{DomainError, build_observation};
use driver_sdk::{DriverErrorInfo, DriverReadItem};
use observation_model::{
    Observation, Quality, QualityLevel, QualityReason, RawReadResult, TimestampNs,
};
use profile_engine::{DecodedRead, decode_read};
use tracing::warn;

use crate::instance::DeviceInstance;
use crate::sequence::SequenceAllocator;

/// 单批映射上下文（`collector_session_id` 与时间戳由上层维护）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapContext {
    /// Collector 会话 ID（§31.3 去重；`build_observation` 要求非空）。
    pub collector_session_id: String,
    /// 采集端收到数据的时刻（§8 `ingest_timestamp_ns`）。
    pub ingest_timestamp_ns: TimestampNs,
}

/// 成功批次映射：`RawReadResult` 列表 → `Observation` 列表（§47）。
///
/// - 每条结果按 `item_id` 回指 Profile 属性并解码；
/// - 未请求的 `item_id` 跳过并告警；
/// - `sequence` 由 `sequences` 按设备连续分配（批内递增、跨批单调，§31.3）；
/// - 返回顺序与 `results` 一致。
///
/// # Errors
///
/// Profile 语义路径与设备 Domain 不匹配时（Profile 配置错误），
/// 返回 [`DomainError`] 且不产生任何 Observation。
pub fn map_results(
    instance: &DeviceInstance,
    results: &[RawReadResult],
    ctx: &MapContext,
    sequences: &mut SequenceAllocator,
) -> Result<Vec<Observation>, DomainError> {
    let sequence_start = sequences.allocate(&instance.device.id, results.len());
    let mut observations = Vec::with_capacity(results.len());
    for (index, result) in results.iter().enumerate() {
        let Some(item) = instance.item(result.item_id) else {
            warn!(
                component = "device-manager",
                device_id = %instance.device.id,
                item_id = result.item_id,
                error_code = "pipeline_unknown_item_id",
                "驱动返回了未请求的读取项，已跳过"
            );
            continue;
        };
        let decoded = decode_read(&item.property, result);
        let observation = build_observation(
            instance.device.id.clone(),
            &instance.profile.domain,
            item.path.clone(),
            decoded,
            result.source_timestamp_ns,
            ctx.ingest_timestamp_ns,
            sequence_start + index as u64,
            &ctx.collector_session_id,
        )?;
        observations.push(observation);
    }
    Ok(observations)
}

/// 整批失败映射：`PollEvent::Failed` → 批内每个读取项产出 `Bad`（§9）。
///
/// `sequence` 由 `sequences` 按设备连续分配（与成功批次共享同一序列源，
/// 保证同设备内单调，§31.3）；返回顺序与 `items` 一致；
/// `protocol_code` 保留 Driver 原始错误码。
pub fn map_failure(
    instance: &DeviceInstance,
    items: &[DriverReadItem],
    error: &DriverErrorInfo,
    ctx: &MapContext,
    sequences: &mut SequenceAllocator,
) -> Result<Vec<Observation>, DomainError> {
    let sequence_start = sequences.allocate(&instance.device.id, items.len());
    let mut observations = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let Some(read_item) = instance.item(item.id) else {
            warn!(
                component = "device-manager",
                device_id = %instance.device.id,
                item_id = item.id,
                error_code = "pipeline_unknown_item_id",
                "失败批含未请求的读取项，已跳过"
            );
            continue;
        };
        let decoded = DecodedRead {
            value: None,
            quality: Quality {
                level: QualityLevel::Bad,
                reason: reason_for_driver_error(error),
                protocol_code: error.protocol_code,
                message: Some(error.message.clone()),
            },
        };
        let observation = build_observation(
            instance.device.id.clone(),
            &instance.profile.domain,
            read_item.path.clone(),
            decoded,
            None,
            ctx.ingest_timestamp_ns,
            sequence_start + index as u64,
            &ctx.collector_session_id,
        )?;
        observations.push(observation);
    }
    Ok(observations)
}

/// 按 Driver 错误码推导 Quality 原因（§9）。
///
/// 匹配规则基于稳定错误码前缀：
///
/// - 含 `timeout` → [`QualityReason::Timeout`]；
/// - 含 `connection` / `not_connected` → [`QualityReason::NotConnected`]；
/// - 其余 → [`QualityReason::ProtocolError`]。
pub fn reason_for_driver_error(error: &DriverErrorInfo) -> QualityReason {
    let code = error.code.to_ascii_lowercase();
    if code.contains("timeout") {
        QualityReason::Timeout
    } else if code.contains("connection") || code.contains("not_connected") {
        QualityReason::NotConnected
    } else {
        QualityReason::ProtocolError
    }
}

#[cfg(test)]
mod tests {
    use observation_model::{DataType, Device, DeviceConnection, DomainKind, Value};
    use profile_engine::{DeviceProfile, ProfileProperty, WriteRounding};

    use super::*;

    fn profile() -> DeviceProfile {
        DeviceProfile {
            id: "test-drive".to_owned(),
            vendor: "Test".to_owned(),
            family: "D".to_owned(),
            models: vec!["D1".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "modbus-tcp".to_owned(),
            properties: vec![
                ProfileProperty {
                    path: "drive.output.frequency".to_owned(),
                    driver_address: "1!40001".to_owned(),
                    raw_type: DataType::U16,
                    value_type: DataType::F64,
                    unit: Some("Hz".to_owned()),
                    scale: 0.01,
                    offset: 0.0,
                    write_rounding: WriteRounding::Nearest,
                    readable: true,
                    writable: true,
                    default_interval_ms: Some(100),
                    min: None,
                    max: None,
                },
                ProfileProperty {
                    path: "drive.other.path".to_owned(), // 与 domain 前缀不一致（故意）
                    driver_address: "1!40002".to_owned(),
                    raw_type: DataType::U16,
                    value_type: DataType::F64,
                    unit: None,
                    scale: 1.0,
                    offset: 0.0,
                    write_rounding: WriteRounding::Nearest,
                    readable: true,
                    writable: true,
                    default_interval_ms: None,
                    min: None,
                    max: None,
                },
            ],
            commands: vec![],
            capabilities: profile_engine::ProfileCapabilities {
                supported_properties: vec![],
                supported_commands: vec![],
                acquisition: Default::default(),
                limits: Default::default(),
            },
        }
    }

    fn device() -> Device {
        Device {
            id: "vfd-01".to_owned(),
            name: "VFD-01".to_owned(),
            domain: DomainKind::Drive,
            driver_id: "modbus-tcp".to_owned(),
            profile_id: "test-drive".to_owned(),
            connection: DeviceConnection {
                config: serde_json::json!({}),
            },
            enabled: true,
            labels: Default::default(),
        }
    }

    /// 由 Profile 直接构造实例（绕过 Driver 工厂，纯 pipeline 测试）。
    fn instance() -> DeviceInstance {
        let profile = std::sync::Arc::new(profile());
        let read_items = crate::read_items::generate_read_items(&profile, 1000);
        let groups = crate::read_items::group_read_items(read_items.clone()).expect("间隔合法");
        DeviceInstance {
            device: device(),
            profile,
            driver: std::sync::Arc::new(std::sync::Mutex::new(Box::new(NoopDriver))),
            read_items,
            groups,
        }
    }

    struct NoopDriver;

    impl poll_engine::PollDriver for NoopDriver {
        fn read_batch(
            &mut self,
            _items: &[DriverReadItem],
        ) -> Result<Vec<RawReadResult>, DriverErrorInfo> {
            Ok(vec![])
        }
    }

    fn ctx() -> MapContext {
        MapContext {
            collector_session_id: "sess-1".to_owned(),
            ingest_timestamp_ns: 1_700_000_000_000_000_000,
        }
    }

    fn allocator() -> SequenceAllocator {
        SequenceAllocator::new()
    }

    fn raw_result(item_id: u64) -> RawReadResult {
        RawReadResult {
            item_id,
            value: Some(observation_model::RawValue::U64(5000)),
            source_timestamp_ns: None,
            received_timestamp_ns: 0,
            protocol_quality_code: None,
            error: None,
        }
    }

    #[test]
    fn maps_good_results_with_scaling() {
        let results = vec![raw_result(0)];
        let observations = map_results(&instance(), &results, &ctx(), &mut allocator()).unwrap();
        assert_eq!(observations.len(), 1);
        let obs = &observations[0];
        // 5000 * 0.01 = 50.0 Hz（§47 示例）。
        assert_eq!(obs.value, Some(Value::F64(50.0)));
        assert_eq!(obs.quality.level, QualityLevel::Good);
        assert_eq!(obs.path, "drive.output.frequency");
        assert_eq!(obs.device_id, "vfd-01");
        assert_eq!(obs.sequence, 0);
        assert!(obs.observation_id.contains("vfd-01"));
        assert!(obs.observation_id.contains("sess-1"));
    }

    #[test]
    fn preserves_per_item_driver_error_as_bad() {
        let mut result = raw_result(0);
        result.error = Some(DriverErrorInfo {
            code: "slave_exception".to_owned(),
            message: "从站异常".to_owned(),
            protocol_code: Some(2),
            retryable: false,
        });
        let observations = map_results(&instance(), &[result], &ctx(), &mut allocator()).unwrap();
        assert_eq!(observations[0].value, None);
        assert_eq!(observations[0].quality.level, QualityLevel::Bad);
        assert_eq!(observations[0].quality.reason, QualityReason::ProtocolError);
        assert_eq!(observations[0].quality.protocol_code, Some(2));
    }

    #[test]
    fn skips_unknown_item_ids() {
        let observations =
            map_results(&instance(), &[raw_result(99)], &ctx(), &mut allocator()).unwrap();
        assert!(observations.is_empty());
    }

    #[test]
    fn rejects_path_with_wrong_domain_prefix() {
        // 第二条属性 path=drive.other.path 通过校验；构造 domain=Plc 的 profile
        // 使第一条属性前缀不匹配 → DomainError。
        let mut inst = instance();
        let plc_profile = {
            let mut p = (*inst.profile).clone();
            p.domain = DomainKind::Plc;
            std::sync::Arc::new(p)
        };
        inst.profile = plc_profile;
        let err = map_results(&inst, &[raw_result(0)], &ctx(), &mut allocator()).unwrap_err();
        assert!(matches!(err, DomainError::PathPrefix { .. }));
    }

    #[test]
    fn maps_failed_batch_to_bad_observations() {
        let error = DriverErrorInfo {
            code: "driver_request_timeout".to_owned(),
            message: "读取超时".to_owned(),
            protocol_code: None,
            retryable: true,
        };
        let items = vec![
            DriverReadItem {
                id: 0,
                address: "1!40001".to_owned(),
                expected_type: Some(DataType::U16),
            },
            DriverReadItem {
                id: 99,
                address: "1!40002".to_owned(),
                expected_type: Some(DataType::U16),
            },
        ];
        let observations =
            map_failure(&instance(), &items, &error, &ctx(), &mut allocator()).unwrap();
        assert_eq!(observations.len(), 1); // item 99 未请求，跳过
        let obs = &observations[0];
        assert_eq!(obs.value, None);
        assert_eq!(obs.quality.level, QualityLevel::Bad);
        assert_eq!(obs.quality.reason, QualityReason::Timeout);
        assert_eq!(obs.sequence, 0);
    }

    #[test]
    fn reason_mapping_rules() {
        let e = |code: &str| DriverErrorInfo {
            code: code.to_owned(),
            message: String::new(),
            protocol_code: None,
            retryable: true,
        };
        assert_eq!(
            reason_for_driver_error(&e("request_timeout")),
            QualityReason::Timeout
        );
        assert_eq!(
            reason_for_driver_error(&e("driver_connection_lost")),
            QualityReason::NotConnected
        );
        assert_eq!(
            reason_for_driver_error(&e("slave_exception")),
            QualityReason::ProtocolError
        );
    }

    #[test]
    fn allocator_keeps_sequence_monotonic_across_batches() {
        // 多批次共享同一分配器：sequence 跨批单调递增（§31.3）。
        let mut seq = allocator();
        let first = map_results(&instance(), &[raw_result(0)], &ctx(), &mut seq).unwrap();
        assert_eq!(first[0].sequence, 0);
        let second = map_results(
            &instance(),
            &[raw_result(0), raw_result(1)],
            &ctx(),
            &mut seq,
        )
        .unwrap();
        assert_eq!(second[0].sequence, 1);
        assert_eq!(second[1].sequence, 2);
        // 失败批次共享同一序列源，不产生重复。
        let error = DriverErrorInfo {
            code: "timeout".to_owned(),
            message: String::new(),
            protocol_code: None,
            retryable: true,
        };
        let failed = map_failure(
            &instance(),
            &[DriverReadItem {
                id: 0,
                address: "1!40001".to_owned(),
                expected_type: Some(DataType::U16),
            }],
            &error,
            &ctx(),
            &mut seq,
        )
        .unwrap();
        assert_eq!(failed[0].sequence, 3);

        let all: Vec<u64> = first
            .iter()
            .chain(&second)
            .chain(&failed)
            .map(|o| o.sequence)
            .collect();
        let mut sorted = all.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(all, sorted, "同一设备 sequence 必须唯一且单调递增");
    }

    #[test]
    fn different_devices_have_independent_sequences() {
        let mut seq = allocator();
        let mut other = instance();
        other.device.id = "vfd-02".to_owned();
        let a = map_results(&instance(), &[raw_result(0)], &ctx(), &mut seq).unwrap();
        let b = map_results(&other, &[raw_result(0)], &ctx(), &mut seq).unwrap();
        assert_eq!(a[0].sequence, 0);
        assert_eq!(b[0].sequence, 0);
    }
}
