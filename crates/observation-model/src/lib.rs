//! observation-model：平台共享规范模型（§96）。
//!
//! 承载《架构设计方案》§4~§10 与 §74~§80.1 中所有 Normative 数据模型：
//! `Device` / `Resource` / `Property` / `Observation` / `Value` / `Quality` /
//! `DataType` / `Command*` / `Control*`，以及 Driver 原始结果边界类型
//! `RawValue` / `RawReadResult` / `DriverErrorInfo`（§7）。
//!
//! 本 crate 只声明类型，不含任何运行逻辑；driver-sdk、profile-engine、
//! domain-model 等 crate 均依赖本 crate，禁止反向依赖。

pub mod command;
pub mod control;
pub mod data_type;
pub mod device;
pub mod observation;
pub mod property;
pub mod quality;
pub mod raw;
pub mod resource;
pub mod types;
pub mod value;

pub use command::{
    CommandDescriptor, CommandParameter, CommandParameterDescriptor, CommandPrecondition,
    CommandRequest, CommandResult, CommandRiskLevel, Operator,
};
pub use control::{
    ControlError, ControlOperation, ControlPayloadResult, ControlRequest, ControlResult,
    ControlStatus, PropertyWriteItemResult,
};
pub use data_type::{DataType, FieldSchema};
pub use device::{Device, DeviceConnection, DomainKind};
pub use observation::Observation;
pub use property::{Property, PropertyReadRequest, PropertyWriteItem, PropertyWriteRequest};
pub use quality::{Quality, QualityLevel, QualityReason};
pub use raw::{DriverErrorInfo, RawFieldValue, RawReadResult, RawValue};
pub use resource::Resource;
pub use types::{DeviceId, PropertyPath, ResourcePath, TimestampNs};
pub use value::{FieldValue, Value};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn value_serde_round_trip_all_variants() {
        let values = vec![
            Value::Bool(true),
            Value::I8(-8),
            Value::I16(-16),
            Value::I32(-32),
            Value::I64(-64),
            Value::U8(8),
            Value::U16(16),
            Value::U32(32),
            Value::U64(64),
            Value::F32(1.5),
            Value::F64(2.5),
            Value::String("温度".to_owned()),
            Value::Bytes(vec![0x01, 0x02]),
            Value::Array(vec![Value::I32(1), Value::I32(2)]),
            Value::Struct(vec![FieldValue {
                name: "x".to_owned(),
                value: Value::F64(3.0),
            }]),
        ];
        for v in values {
            let json = serde_json::to_string(&v).expect("序列化失败");
            let back: Value = serde_json::from_str(&json).expect("反序列化失败");
            assert_eq!(v, back);
        }
    }

    #[test]
    fn value_serde_rejects_unknown_variant() {
        let err = serde_json::from_str::<Value>(r#"{"I128": 1}"#);
        assert!(err.is_err(), "未知变体应反序列化失败");
    }

    #[test]
    fn observation_round_trip_with_quality() {
        let obs = Observation {
            observation_id: "obs-1".to_owned(),
            device_id: "fanuc01".to_owned(),
            path: "cnc.axis.x.absolute_position".to_owned(),
            value: Some(Value::F64(123.456)),
            quality: Quality {
                level: QualityLevel::Good,
                reason: QualityReason::None,
                protocol_code: None,
                message: None,
            },
            source_timestamp_ns: None,
            ingest_timestamp_ns: 1_780_000_000_000_000_000,
            sequence: 42,
            metadata: BTreeMap::new(),
        };
        let json = serde_json::to_string(&obs).expect("序列化失败");
        let back: Observation = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(obs, back);
    }

    #[test]
    fn domain_kind_serde_matches_doc_examples() {
        // §4.2 规范示例：snake_case。
        assert_eq!(
            serde_json::to_string(&DomainKind::Drive).expect("序列化失败"),
            r#""drive""#
        );
        assert_eq!(
            serde_json::to_string(&DomainKind::PowerDevice).expect("序列化失败"),
            r#""power_device""#
        );
        let back: DomainKind = serde_json::from_str(r#""building_device""#).expect("反序列化失败");
        assert_eq!(back, DomainKind::BuildingDevice);
    }

    #[test]
    fn quality_level_serde_matches_doc_examples() {
        assert_eq!(
            serde_json::to_string(&QualityLevel::Good).expect("序列化失败"),
            r#""good""#
        );
        let back: QualityLevel = serde_json::from_str(r#""uncertain""#).expect("反序列化失败");
        assert_eq!(back, QualityLevel::Uncertain);
    }

    #[test]
    fn risk_level_serde_matches_doc_examples() {
        assert_eq!(
            serde_json::to_string(&CommandRiskLevel::High).expect("序列化失败"),
            r#""high""#
        );
        let back: CommandRiskLevel = serde_json::from_str(r#""critical""#).expect("反序列化失败");
        assert_eq!(back, CommandRiskLevel::Critical);
    }

    #[test]
    fn control_status_serde_matches_doc_examples() {
        assert_eq!(
            serde_json::to_string(&ControlStatus::Accepted).expect("序列化失败"),
            r#""accepted""#
        );
        let back: ControlStatus = serde_json::from_str(r#""succeeded""#).expect("反序列化失败");
        assert_eq!(back, ControlStatus::Succeeded);
    }

    #[test]
    fn data_type_and_value_serde_use_snake_case() {
        assert_eq!(
            serde_json::to_string(&DataType::Array(Box::new(DataType::U16))).expect("序列化失败"),
            r#"{"array":"u16"}"#
        );
        assert_eq!(
            serde_json::to_string(&Value::I32(5)).expect("序列化失败"),
            r#"{"i32":5}"#
        );
        let back: Value = serde_json::from_str(r#"{"f64":1.5}"#).expect("反序列化失败");
        assert_eq!(back, Value::F64(1.5));
    }

    #[test]
    fn quality_rejects_unknown_level() {
        let err = serde_json::from_str::<QualityLevel>(r#""Great""#);
        assert!(err.is_err(), "未知质量级别应反序列化失败");
    }

    #[test]
    fn raw_read_result_round_trip() {
        let raw = RawReadResult {
            item_id: 100,
            value: Some(RawValue::U64(5000)),
            source_timestamp_ns: None,
            received_timestamp_ns: 1_780_000_000_000_000_000,
            protocol_quality_code: None,
            error: Some(DriverErrorInfo {
                code: "MODBUS_EXCEPTION".to_owned(),
                message: "illegal data address".to_owned(),
                protocol_code: Some(2),
                retryable: false,
            }),
        };
        let json = serde_json::to_string(&raw).expect("序列化失败");
        let back: RawReadResult = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(raw, back);
    }

    #[test]
    fn domain_kind_custom_round_trip() {
        let kind = DomainKind::Custom("注塑机".to_owned());
        let json = serde_json::to_string(&kind).expect("序列化失败");
        let back: DomainKind = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(kind, back);
    }

    #[test]
    fn control_request_round_trip() {
        let req = ControlRequest {
            request_id: "cmd-8fa231".to_owned(),
            namespace: "plant-a".to_owned(),
            device_id: "fanuc01".to_owned(),
            requested_at_ns: 1_780_000_000_000_000_000,
            timeout_ms: 5_000,
            operation: ControlOperation::CommandExecute(CommandRequest {
                command: "cnc.program.start".to_owned(),
                parameters: vec![],
            }),
        };
        let json = serde_json::to_string(&req).expect("序列化失败");
        let back: ControlRequest = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(req, back);
    }
}
