//! 领域映射：路径校验与 `Observation` 组装（§7.3、§41~§46）。
//!
//! `build_observation` 完成链路最后一步：
//!
//! ```text
//! RawReadResult → Profile（decode_read）→ Domain（本模块）→ Observation
//! ```
//!
//! - 先校验语义路径首段与设备 `DomainKind` 的标准前缀一致（如 Drive 域
//!   必须是 `drive.*`），拒绝跨域路径；
//! - 随后组装 `Observation`：`observation_id` 采用长度前缀无歧义编码
//!   `{len(device_id)}:{device_id}:{len(path)}:{path}:{sequence}:{session}`。
//!   `sequence` 只在单一 Collector 会话内单调递增（§31.3），故 ID 必须
//!   嵌入 `collector_session_id`，否则 Collector 重启后相同设备/路径/序号
//!   会生成相同 ID，消费者会误判为重复而丢弃（P1）。

use std::collections::BTreeMap;
use std::fmt;

use observation_model::{DeviceId, DomainKind, Observation, PropertyPath, TimestampNs};
use profile_engine::DecodedRead;

use crate::standard::{first_segment, standard_prefix};

/// 领域路径校验失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// 语义路径不属于设备领域。
    PathPrefix {
        /// 设备领域。
        domain: String,
        /// 语义路径。
        path: String,
        /// 期望的前缀（如 `drive.`）。
        expected_prefix: String,
    },
    /// `collector_session_id` 为空（P2）。
    EmptyCollectorSession,
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomainError::PathPrefix {
                domain,
                path,
                expected_prefix,
            } => write!(
                f,
                "路径 `{path}` 不属于 `{domain}` 域：应以前缀 `{expected_prefix}` 开头"
            ),
            DomainError::EmptyCollectorSession => {
                write!(f, "collector_session_id 不能为空")
            }
        }
    }
}

impl std::error::Error for DomainError {}

/// 校验语义路径首段与设备领域匹配（§41~§46）。
pub fn validate_domain_path(domain: &DomainKind, path: &PropertyPath) -> Result<(), DomainError> {
    let prefix = standard_prefix(domain);
    let expected = format!("{prefix}.");
    if first_segment(path) == prefix {
        Ok(())
    } else {
        Err(DomainError::PathPrefix {
            domain: format!("{domain:?}"),
            path: path.clone(),
            expected_prefix: expected,
        })
    }
}

/// 组装领域 `Observation`（链路最后一步，§7.3）。
///
/// `decoded` 来自 `profile_engine::decode_read`；`source_timestamp_ns` 透传
/// 设备/协议时间（无则 `None`，禁止伪造设备时间，§8）；
/// `ingest_timestamp_ns` 由调用方（Poll Engine）填写；
/// `collector_session_id` 由 Collector 启动时生成（如启动时刻标识），
/// 保证 ID 跨会话唯一。
// 参数即 `Observation` 的组成字段，无冗余可折叠，故允许该 lint。
#[allow(clippy::too_many_arguments)]
pub fn build_observation(
    device_id: DeviceId,
    domain: &DomainKind,
    path: PropertyPath,
    decoded: DecodedRead,
    source_timestamp_ns: Option<TimestampNs>,
    ingest_timestamp_ns: TimestampNs,
    sequence: u64,
    collector_session_id: &str,
) -> Result<Observation, DomainError> {
    if collector_session_id.is_empty() {
        return Err(DomainError::EmptyCollectorSession);
    }
    validate_domain_path(domain, &path)?;
    Ok(Observation {
        // 长度前缀无歧义编码（P2）：`DeviceId`/`PropertyPath` 均为字符串，
        // 允许包含 `:`，仅用 `:` 拼接可能碰撞（如 `device_id="d:drive.x"`、
        // `path="drive.y"` 与 `device_id="d"`、`path="drive.x:drive.y"` 会
        // 生成相同 ID）。解析时先读十进制长度，再取对应字节数，天然无歧义；
        // `collector_session_id` 为末段，按剩余内容整体解析。
        // `collector_session_id` 的唯一性由其生成方（Collector 启动时）保证，
        // 本函数只校验非空。
        observation_id: format!(
            "{}:{device_id}:{}:{path}:{sequence}:{collector_session_id}",
            device_id.len(),
            path.len(),
        ),
        device_id,
        path,
        value: decoded.value,
        quality: decoded.quality,
        source_timestamp_ns,
        ingest_timestamp_ns,
        sequence,
        metadata: BTreeMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use observation_model::{QualityLevel, RawReadResult, RawValue, Value};
    use profile_engine::{DeviceProfile, decode_read};

    use super::*;

    fn sample_profile() -> DeviceProfile {
        serde_json::from_str(
            r#"{
                "id": "inovance-md500",
                "vendor": "Inovance",
                "family": "MD500",
                "models": ["MD500"],
                "domain": "drive",
                "driver_id": "modbus-rtu",
                "properties": [{
                    "path": "drive.output.frequency",
                    "driver_address": "1!40001",
                    "raw_type": "u16",
                    "value_type": "f64",
                    "unit": "Hz",
                    "scale": 0.01,
                    "offset": 0.0,
                    "write_rounding": "exact",
                    "readable": true,
                    "writable": true,
                    "default_interval_ms": 1000,
                    "min": null,
                    "max": null
                }],
                "commands": [],
                "capabilities": {
                    "supported_properties": ["drive.output.frequency"],
                    "supported_commands": [],
                    "acquisition": {},
                    "limits": {}
                }
            }"#,
        )
        .expect("示例 Profile 应可反序列化")
    }

    /// 全链路测试（§7.3 示例）：Raw U64(5000) × scale 0.01 → 50.00 Hz。
    #[test]
    fn full_chain_raw_to_observation() {
        let profile = sample_profile();
        let property = profile
            .property("drive.output.frequency")
            .expect("Profile 应包含该属性");

        let result = RawReadResult {
            item_id: 1,
            value: Some(RawValue::U64(5000)),
            source_timestamp_ns: Some(1_700_000_000_000_000_000),
            received_timestamp_ns: 1_700_000_000_000_000_001,
            protocol_quality_code: None,
            error: None,
        };

        let decoded = decode_read(property, &result);
        assert_eq!(decoded.value, Some(Value::F64(50.0)));
        assert_eq!(decoded.quality.level, QualityLevel::Good);

        let observation = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            property.path.clone(),
            decoded,
            result.source_timestamp_ns,
            1_700_000_000_000_000_002,
            7,
            "sess-1700000000000000000",
        )
        .expect("链路组装应成功");

        assert_eq!(observation.device_id, "VFD-01");
        assert_eq!(observation.path, "drive.output.frequency");
        assert_eq!(observation.value, Some(Value::F64(50.0)));
        assert_eq!(observation.quality.level, QualityLevel::Good);
        assert_eq!(observation.sequence, 7);
        assert_eq!(
            observation.observation_id,
            "6:VFD-01:22:drive.output.frequency:7:sess-1700000000000000000"
        );
        assert_eq!(
            observation.source_timestamp_ns,
            Some(1_700_000_000_000_000_000)
        );
    }

    #[test]
    fn full_chain_error_keeps_bad_quality() {
        let profile = sample_profile();
        let property = profile.property("drive.output.frequency").unwrap();
        let result = RawReadResult {
            item_id: 1,
            value: None,
            source_timestamp_ns: None,
            received_timestamp_ns: 1,
            protocol_quality_code: None,
            error: Some(observation_model::DriverErrorInfo {
                code: "timeout".to_owned(),
                message: "slave 无响应".to_owned(),
                protocol_code: None,
                retryable: true,
            }),
        };
        let decoded = decode_read(property, &result);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.quality.level, QualityLevel::Bad);

        let observation = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            property.path.clone(),
            decoded,
            result.source_timestamp_ns,
            1,
            8,
            "sess-1",
        )
        .expect("错误链路也应组装成功（Bad 观测）");
        assert_eq!(observation.value, None);
        assert_eq!(observation.quality.level, QualityLevel::Bad);
    }

    #[test]
    fn cross_domain_path_rejected() {
        let profile = sample_profile();
        let e = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            "plc.cpu.cycle_time".to_owned(),
            DecodedRead {
                value: None,
                quality: observation_model::Quality {
                    level: QualityLevel::Bad,
                    reason: observation_model::QualityReason::ProtocolError,
                    protocol_code: None,
                    message: None,
                },
            },
            None,
            1,
            1,
            "sess-1",
        )
        .expect_err("跨域路径应拒绝");
        assert!(matches!(
            e,
            DomainError::PathPrefix {
                expected_prefix,
                ..
            } if expected_prefix == "drive."
        ));
    }

    #[test]
    fn custom_domain_uses_custom_prefix() {
        let domain = DomainKind::Custom("vendor-xyz".to_owned());
        assert!(validate_domain_path(&domain, &"custom.status.ok".to_owned()).is_ok());
        assert!(validate_domain_path(&domain, &"drive.output.frequency".to_owned()).is_err());
    }

    #[test]
    fn observation_id_unique_across_collector_sessions() {
        // P1：Collector 重启后 sequence 重新从 0 递增，相同设备/路径/序号
        // 必须因 collector_session_id 不同而生成不同 observation_id，
        // 否则消费者会误判为重复数据而丢弃。
        let profile = sample_profile();
        let property = profile.property("drive.output.frequency").unwrap();
        let decoded = decode_read(
            property,
            &RawReadResult {
                item_id: 1,
                value: Some(RawValue::U64(5000)),
                source_timestamp_ns: None,
                received_timestamp_ns: 1,
                protocol_quality_code: None,
                error: None,
            },
        );
        let session_a = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            property.path.clone(),
            decoded.clone(),
            None,
            1,
            1,
            "sess-a",
        )
        .expect("会话 A 应成功");
        let session_b = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            property.path.clone(),
            decoded.clone(),
            None,
            1,
            1,
            "sess-b",
        )
        .expect("会话 B 应成功");
        assert_eq!(
            session_a.observation_id,
            "6:VFD-01:22:drive.output.frequency:1:sess-a"
        );
        assert_eq!(
            session_b.observation_id,
            "6:VFD-01:22:drive.output.frequency:1:sess-b"
        );
        assert_ne!(session_a.observation_id, session_b.observation_id);

        // 同会话同序号 → 同一 ID（点级去重键仍成立）。
        let session_a_dup = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            property.path.clone(),
            decoded,
            None,
            1,
            1,
            "sess-a",
        )
        .expect("同会话重复组装应成功");
        assert_eq!(session_a.observation_id, session_a_dup.observation_id);
    }

    #[test]
    fn observation_id_unambiguous_with_colons_in_components() {
        // P2：device_id/path 允许包含 `:`，长度前缀编码保证无碰撞——
        // device_id="d:drive.x" + path="drive.y" 与
        // device_id="d" + path="drive.x:drive.y" 必须生成不同 ID。
        let profile = sample_profile();
        let property = profile.property("drive.output.frequency").unwrap();
        let decoded = decode_read(
            property,
            &RawReadResult {
                item_id: 1,
                value: Some(RawValue::U64(5000)),
                source_timestamp_ns: None,
                received_timestamp_ns: 1,
                protocol_quality_code: None,
                error: None,
            },
        );
        let a = build_observation(
            "d:drive.x".to_owned(),
            &profile.domain,
            "drive.y".to_owned(),
            decoded.clone(),
            None,
            1,
            1,
            "sess-a",
        )
        .expect("含冒号 device_id 应成功");
        let b = build_observation(
            "d".to_owned(),
            &profile.domain,
            "drive.x:drive.y".to_owned(),
            decoded.clone(),
            None,
            1,
            1,
            "sess-a",
        )
        .expect("含冒号 path 应成功");
        assert_eq!(a.observation_id, "9:d:drive.x:7:drive.y:1:sess-a");
        assert_eq!(b.observation_id, "1:d:15:drive.x:drive.y:1:sess-a");
        assert_ne!(a.observation_id, b.observation_id);

        // 会话自身含 `:` 也无歧义（末段整体解析）。
        let c = build_observation(
            "d".to_owned(),
            &profile.domain,
            "drive.y".to_owned(),
            decoded,
            None,
            1,
            1,
            "sess:a:b",
        )
        .expect("含冒号会话 ID 应成功");
        assert_eq!(c.observation_id, "1:d:7:drive.y:1:sess:a:b");
    }

    #[test]
    fn empty_collector_session_rejected() {
        let profile = sample_profile();
        let property = profile.property("drive.output.frequency").unwrap();
        let decoded = decode_read(
            property,
            &RawReadResult {
                item_id: 1,
                value: Some(RawValue::U64(5000)),
                source_timestamp_ns: None,
                received_timestamp_ns: 1,
                protocol_quality_code: None,
                error: None,
            },
        );
        let e = build_observation(
            "VFD-01".to_owned(),
            &profile.domain,
            property.path.clone(),
            decoded,
            None,
            1,
            1,
            "",
        )
        .expect_err("空 collector_session_id 应被拒绝");
        assert_eq!(e, DomainError::EmptyCollectorSession);
    }
}
