//! ABI v1 JSON Envelope（§17.2、§17.9 Normative）。
//!
//! v1 中所有 ABI payload（`read`/`write`/`execute`/`browse`/`history` 结果、
//! `execute`/`subscribe`/`query_history` 请求、event callback 的 `event_json`）
//! 统一使用带 `schema_version` 的 UTF-8 JSON envelope（§17.9）。
//!
//! Envelope 字段与进程内 Rust 类型（`observation_model` / `driver-sdk`）
//! 完全一致，Loader 可直接反序列化，无需二次映射。
//!
//! # 兼容规则
//!
//! - `schema_version` 与 ABI minor 同步演进（§17.9）：`SchemaVersion` 自定义
//!   反序列化强制校验，版本不一致的 JSON 直接拒绝，Loader 不会误处理
//!   不兼容数据。
//! - 同一 ABI minor 内 envelope 结构不变；新增可选字段属于 minor 演进，
//!   破坏性变更 => ABI major + 1（§17.4、§18）。
//! - 例外：`get_last_error_json` 保持 §17.6 固定形状（`DriverErrorInfo`），
//!   不携带 `schema_version`。

use observation_model::{DriverErrorInfo, RawReadResult};
use serde::{Deserialize, Deserializer, Serialize};

use crate::items::DriverCommand;
use crate::results::{
    AddressMetadata, DriverBrowseNode, RawCommandResult, RawEvent, RawHistoryPage, RawWriteResult,
    SubscriptionRequest,
};
use crate::{HistoryRequest, ProtocolCapabilities};

/// 本 SDK 实现的 ABI 版本对应的 Envelope schema 版本（ABI 1.0）。
pub const SCHEMA_VERSION: &str = "1.0";

/// Envelope schema 版本（§17.9）。
///
/// 反序列化时**强制校验**：值必须等于 [`SCHEMA_VERSION`]，否则整个
/// envelope 反序列化失败，防止 Loader 误处理不兼容数据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaVersion(String);

impl SchemaVersion {
    /// 当前 SDK 支持的 schema 版本。
    pub fn current() -> Self {
        Self(SCHEMA_VERSION.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let version = String::deserialize(deserializer)?;
        if version == SCHEMA_VERSION {
            Ok(Self(version))
        } else {
            Err(serde::de::Error::custom(format!(
                "不支持的 schema_version {version:?}，当前仅支持 {SCHEMA_VERSION:?}"
            )))
        }
    }
}

/// `read` 结果 Envelope（§17.2）。
///
/// `received_timestamp_ns` 由 Plugin 尽力填写（设备接收时间，可为 0），
/// Core 收到后会覆写为自身生成的时间戳（§7.2）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadEnvelope {
    pub schema_version: SchemaVersion,
    pub results: Vec<RawReadResult>,
}

impl ReadEnvelope {
    pub fn new(results: Vec<RawReadResult>) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            results,
        }
    }
}

/// `write` 结果 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteEnvelope {
    pub schema_version: SchemaVersion,
    pub results: Vec<RawWriteResult>,
}

impl WriteEnvelope {
    pub fn new(results: Vec<RawWriteResult>) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            results,
        }
    }
}

/// `execute` 请求 Envelope（`command_json`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteRequestEnvelope {
    pub schema_version: SchemaVersion,
    pub command: DriverCommand,
}

impl ExecuteRequestEnvelope {
    pub fn new(command: DriverCommand) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            command,
        }
    }
}

/// `execute` 结果 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteEnvelope {
    pub schema_version: SchemaVersion,
    pub result: RawCommandResult,
}

impl ExecuteEnvelope {
    pub fn new(result: RawCommandResult) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            result,
        }
    }
}

/// `browse` 结果 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrowseEnvelope {
    pub schema_version: SchemaVersion,
    pub nodes: Vec<DriverBrowseNode>,
}

impl BrowseEnvelope {
    pub fn new(nodes: Vec<DriverBrowseNode>) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            nodes,
        }
    }
}

/// `subscribe` 请求 Envelope（`request_json`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeEnvelope {
    pub schema_version: SchemaVersion,
    pub request: SubscriptionRequest,
}

impl SubscribeEnvelope {
    pub fn new(request: SubscriptionRequest) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            request,
        }
    }
}

/// `query_history` 请求 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryRequestEnvelope {
    pub schema_version: SchemaVersion,
    pub request: HistoryRequest,
}

impl HistoryRequestEnvelope {
    pub fn new(request: HistoryRequest) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            request,
        }
    }
}

/// `query_history` 结果 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryEnvelope {
    pub schema_version: SchemaVersion,
    pub page: RawHistoryPage,
}

impl HistoryEnvelope {
    pub fn new(page: RawHistoryPage) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            page,
        }
    }
}

/// event callback 的 `event_json` Envelope（§17.8）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: SchemaVersion,
    pub event: RawEvent,
}

impl EventEnvelope {
    pub fn new(event: RawEvent) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            event,
        }
    }
}

/// `get_capabilities_json` 结果 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilitiesEnvelope {
    pub schema_version: SchemaVersion,
    pub capabilities: ProtocolCapabilities,
}

impl CapabilitiesEnvelope {
    pub fn new(capabilities: ProtocolCapabilities) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            capabilities,
        }
    }
}

/// `validate_address` 结果 Envelope。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressEnvelope {
    pub schema_version: SchemaVersion,
    pub address: AddressMetadata,
}

impl AddressEnvelope {
    pub fn new(address: AddressMetadata) -> Self {
        Self {
            schema_version: SchemaVersion::current(),
            address,
        }
    }
}

/// `get_last_error_json` 内容（§17.6 固定形状，无 `schema_version`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub code: String,
    pub message: String,
    pub protocol_code: Option<i64>,
    pub retryable: bool,
}

impl From<&DriverErrorInfo> for ErrorEnvelope {
    fn from(error: &DriverErrorInfo) -> Self {
        Self {
            code: error.code.clone(),
            message: error.message.clone(),
            protocol_code: error.protocol_code,
            retryable: error.retryable,
        }
    }
}

#[cfg(test)]
mod tests {
    use observation_model::{RawValue, TimestampNs};

    use super::*;
    use crate::results::RawEventKind;

    #[test]
    fn read_envelope_exact_json() {
        let env = ReadEnvelope::new(vec![RawReadResult {
            item_id: 1,
            value: Some(RawValue::F64(50.0)),
            source_timestamp_ns: None,
            received_timestamp_ns: 0,
            protocol_quality_code: None,
            error: None,
        }]);
        assert_eq!(
            serde_json::to_string(&env).expect("序列化失败"),
            r#"{"schema_version":"1.0","results":[{"item_id":1,"value":{"f64":50.0},"source_timestamp_ns":null,"received_timestamp_ns":0,"protocol_quality_code":null,"error":null}]}"#
        );
    }

    #[test]
    fn read_envelope_round_trip() {
        let env = ReadEnvelope::new(vec![RawReadResult {
            item_id: 2,
            value: Some(RawValue::U64(5000)),
            source_timestamp_ns: Some(1_780_000_000_000_000_000),
            received_timestamp_ns: 0,
            protocol_quality_code: Some(0),
            error: None,
        }]);
        let json = serde_json::to_string(&env).expect("序列化失败");
        let back: ReadEnvelope = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(env, back);
    }

    #[test]
    fn loader_rejects_unsupported_schema_version() {
        // Loader 规则（§17.9）：schema_version 不符必须反序列化失败。
        let err = serde_json::from_str::<ReadEnvelope>(r#"{"schema_version":"0.5","results":[]}"#);
        assert!(err.is_err(), "不支持的 schema_version 必须被拒绝");

        let ok = serde_json::from_str::<ReadEnvelope>(r#"{"schema_version":"1.0","results":[]}"#);
        assert!(ok.is_ok(), "支持的 schema_version 必须通过");
    }

    #[test]
    fn event_envelope_uses_snake_case_kind() {
        let env = EventEnvelope::new(RawEvent {
            subscription_id: Some(7),
            event_id: None,
            kind: RawEventKind::DataChange,
            items: vec![],
            payload: None,
            source_timestamp_ns: None,
            sequence: Some(3),
            protocol_code: None,
        });
        let value = serde_json::to_value(&env).expect("序列化失败");
        assert_eq!(value["schema_version"], SCHEMA_VERSION);
        assert_eq!(value["event"]["kind"], "data_change");
        assert_eq!(value["event"]["subscription_id"], 7);
        let back: EventEnvelope = serde_json::from_value(value).expect("反序列化失败");
        assert_eq!(back, env);
    }

    #[test]
    fn capabilities_envelope_round_trip() {
        let env = CapabilitiesEnvelope::new(ProtocolCapabilities::default());
        let json = serde_json::to_string(&env).expect("序列化失败");
        let back: CapabilitiesEnvelope = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(env, back);
        assert!(back.capabilities.read && back.capabilities.polling);
    }

    #[test]
    fn error_envelope_matches_doc_example() {
        // §17.6 文档示例形状。
        let error = DriverErrorInfo {
            code: "MODBUS_EXCEPTION".to_owned(),
            message: "illegal data address".to_owned(),
            protocol_code: Some(2),
            retryable: false,
        };
        let env = ErrorEnvelope::from(&error);
        assert_eq!(
            serde_json::to_string(&env).expect("序列化失败"),
            r#"{"code":"MODBUS_EXCEPTION","message":"illegal data address","protocol_code":2,"retryable":false}"#
        );
    }

    #[test]
    fn execute_and_write_envelope_round_trip() {
        let exec = ExecuteEnvelope::new(RawCommandResult {
            success: true,
            protocol_code: None,
            payload: Some(serde_json::json!({"program": "P1234"})),
            error: None,
        });
        let back: ExecuteEnvelope =
            serde_json::from_str(&serde_json::to_string(&exec).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(exec, back);

        let write = WriteEnvelope::new(vec![RawWriteResult {
            item_id: 0,
            success: true,
            protocol_code: None,
            error: None,
        }]);
        let back: WriteEnvelope =
            serde_json::from_str(&serde_json::to_string(&write).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(write, back);
    }

    #[test]
    fn history_browse_address_round_trip() {
        let hist = HistoryEnvelope::new(RawHistoryPage {
            items: vec![RawReadResult {
                item_id: 1,
                value: Some(RawValue::F64(1.0)),
                source_timestamp_ns: Some(1_780_000_000_000_000_000),
                received_timestamp_ns: 0,
                protocol_quality_code: None,
                error: None,
            }],
            continuation: None,
        });
        let back: HistoryEnvelope =
            serde_json::from_str(&serde_json::to_string(&hist).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(hist, back);

        let browse = BrowseEnvelope::new(vec![DriverBrowseNode {
            id: "axis-1".to_owned(),
            display_name: "X 轴".to_owned(),
            address: Some("1!40001".to_owned()),
            has_children: false,
            metadata: serde_json::json!({}),
        }]);
        let back: BrowseEnvelope =
            serde_json::from_str(&serde_json::to_string(&browse).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(browse, back);

        let addr = AddressEnvelope::new(AddressMetadata {
            canonical_address: "1!40001".to_owned(),
            raw_type: Some(observation_model::DataType::U16),
            readable: true,
            writable: true,
        });
        let value = serde_json::to_value(&addr).expect("序列化失败");
        assert_eq!(value["address"]["raw_type"], "u16");
        let back: AddressEnvelope = serde_json::from_value(value).expect("反序列化失败");
        assert_eq!(addr, back);
    }

    #[test]
    fn request_envelopes_round_trip() {
        let subscribe = SubscribeEnvelope::new(SubscriptionRequest {
            items: vec![],
            event_types: vec![],
            protocol_filter: None,
            publishing_interval_ms: Some(1000),
        });
        let back: SubscribeEnvelope =
            serde_json::from_str(&serde_json::to_string(&subscribe).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(subscribe, back);

        let history = HistoryRequestEnvelope::new(HistoryRequest {
            items: vec![],
            start_time_ns: 0,
            end_time_ns: TimestampNs::MAX,
            limit: Some(100),
            continuation: None,
        });
        let back: HistoryRequestEnvelope =
            serde_json::from_str(&serde_json::to_string(&history).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(history, back);

        let exec = ExecuteRequestEnvelope::new(DriverCommand {
            command_id: "reset".to_owned(),
            payload: serde_json::json!({}),
        });
        let back: ExecuteRequestEnvelope =
            serde_json::from_str(&serde_json::to_string(&exec).expect("序列化失败"))
                .expect("反序列化失败");
        assert_eq!(exec, back);
    }
}
