//! profile-engine：Device Profile 引擎（§37、§38 Normative）。
//!
//! Profile 负责：地址映射、缩放、单位、枚举、语义名称、默认采样周期、
//! 型号能力等"具体品牌型号"层语义；不实现任何协议编解码。
//!
//! 本 crate 提供：
//!
//! - 数据模型（`models`）：`DeviceProfile` / `ProfileProperty` 等；
//! - 完整校验（`validate`）：字段、路径、缩放、类型族、范围（§37）；
//! - 动态加载（`loader`）：`profiles/` 目录 JSON 递归加载（§38）；
//! - 注册表（`registry`）：按 `profile_id` 索引的只读查询；
//! - 转换（`convert`）：读取解码 `RawReadResult → Value + Quality`（§37.1），
//!   写入逆变换 `Value → RawValue`（缩放、取整、范围与溢出检查）。

mod convert;
mod loader;
mod models;
mod registry;
mod validate;

pub use convert::{ConversionError, DecodedRead, decode_read, encode_write};
pub use loader::{LoaderError, load_profiles_dir, load_single};
pub use models::{
    AcquisitionConstraints, DeviceProfile, ProfileCapabilities, ProfileCommand, ProfileProperty,
    WriteRounding,
};
pub use registry::{ProfileRegistry, RegistryError};
pub use validate::{ValidationError, validate_profile, validate_property};

#[cfg(test)]
mod tests {
    use observation_model::{CommandRiskLevel, DataType, DomainKind, Value};

    use super::*;

    fn sample_profile() -> DeviceProfile {
        DeviceProfile {
            id: "inovance-md500".to_owned(),
            vendor: "Inovance".to_owned(),
            family: "MD500".to_owned(),
            models: vec!["MD500".to_owned(), "MD500E".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "modbus-rtu".to_owned(),
            properties: vec![ProfileProperty {
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
                default_interval_ms: Some(1000),
                min: Some(Value::F64(0.0)),
                max: Some(Value::F64(50.0)),
            }],
            commands: vec![ProfileCommand {
                id: "drive.reset".to_owned(),
                driver_command_id: "reset".to_owned(),
                parameters: vec![],
                risk_level: CommandRiskLevel::Medium,
                preconditions: vec![],
            }],
            capabilities: ProfileCapabilities {
                supported_properties: vec!["drive.output.frequency".to_owned()],
                supported_commands: vec!["drive.reset".to_owned()],
                acquisition: Default::default(),
                limits: Default::default(),
            },
        }
    }

    #[test]
    fn device_profile_serde_round_trip() {
        let profile = sample_profile();
        let json = serde_json::to_string(&profile).expect("序列化失败");
        let back: DeviceProfile = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(profile, back);
    }

    #[test]
    fn device_profile_serde_rejects_missing_field() {
        // 错误路径：缺少必填字段 driver_id 时反序列化必须失败。
        let json = r#"{"id":"x","vendor":"v","family":"f","models":[],"domain":"Plc"}"#;
        let err = serde_json::from_str::<DeviceProfile>(json);
        assert!(err.is_err(), "缺少必填字段应反序列化失败");
    }

    #[test]
    fn profile_json_accepts_doc_snake_case_enums() {
        // §37 规范示例 JSON：domain=drive、risk_level=high 必须能反序列化。
        let json = r#"{
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
                "write_rounding": "nearest",
                "readable": true,
                "writable": true,
                "default_interval_ms": 1000,
                "min": { "f64": 0.0 },
                "max": { "f64": 50.0 }
            }],
            "commands": [{
                "id": "drive.reset",
                "driver_command_id": "reset",
                "parameters": [],
                "risk_level": "high",
                "preconditions": []
            }],
            "capabilities": {
                "supported_properties": ["drive.output.frequency"],
                "supported_commands": ["drive.reset"],
                "acquisition": {},
                "limits": {}
            }
        }"#;
        let profile: DeviceProfile =
            serde_json::from_str(json).expect("文档示例 JSON 必须可反序列化");
        assert_eq!(profile.domain, DomainKind::Drive);
        assert_eq!(profile.commands[0].risk_level, CommandRiskLevel::High);
        assert_eq!(profile.properties[0].write_rounding, WriteRounding::Nearest);
    }

    #[test]
    fn write_rounding_serde_uses_snake_case() {
        assert_eq!(
            serde_json::to_string(&WriteRounding::Nearest).expect("序列化失败"),
            r#""nearest""#
        );
        let back: WriteRounding = serde_json::from_str(r#""ceil""#).expect("反序列化失败");
        assert_eq!(back, WriteRounding::Ceil);
    }

    #[test]
    fn profile_property_scale_semantics_documented() {
        // 验证转换规则常量可组合（§37.1）：5000 * 0.01 + 0.0 = 50.0 Hz。
        let p = &sample_profile().properties[0];
        let semantic = 5000.0 * p.scale + p.offset;
        assert_eq!(semantic, 50.0);
    }
}
