//! driver-sdk：Driver 插件开发 SDK（§15、§16、§17 Normative）。
//!
//! 提供两层契约：
//!
//! 1. **内部 Rust Driver 契约**（`driver`）：以"原始协议结果"为边界，
//!    Core 只传递 `DriverReadItem`，Driver 返回 `RawReadResult` / `RawEvent`
//!    等原始结果，**不生成 `Observation`**（§7.3）。
//! 2. **稳定 C ABI v1**（`abi`）：唯一入口 `forgelink_driver_entry_v1()`、
//!    `DriverApiV1`、`FfiStr` / `FfiSlice` / `FfiReadItem` / `FfiWriteItem` /
//!    `FfiOwnedBuffer`，以及 ABI 版本与结构扩展规则（§17.4、§18）。
//!
//! 禁止跨 FFI 暴露 Rust trait 或 `async fn`；Native Plugin 仅暴露 `cdylib` 入口。

pub mod abi;
pub mod capabilities;
pub mod driver;
pub mod items;
pub mod manifest;
pub mod results;

pub use capabilities::ProtocolCapabilities;
pub use driver::Driver;
pub use items::{DriverCommand, DriverReadItem, DriverWriteItem};
pub use manifest::DriverManifest;
pub use results::{
    AddressMetadata, DriverBrowseNode, HistoryRequest, RawCommandResult, RawEvent, RawEventKind,
    RawHistoryPage, RawWriteResult, SubscriptionId, SubscriptionRequest,
};

// 原始结果边界类型由 observation-model 定义（§7），在此转发以方便 Driver 使用。
pub use observation_model::{DriverErrorInfo, RawFieldValue, RawReadResult, RawValue};

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use super::*;

    #[test]
    fn abi_version_constants() {
        assert_eq!(abi::ABI_MAJOR, 1);
        assert_eq!(abi::ABI_MINOR, 0);
        assert_eq!(abi::ENTRY_SYMBOL, "forgelink_driver_entry_v1");
    }

    #[test]
    fn ffi_types_are_repr_c() {
        // 布局断言：跨 ABI 类型必须保持 #[repr(C)]（§16）。
        assert_eq!(size_of::<abi::FfiStr>(), size_of::<usize>() * 2);
        assert_eq!(align_of::<abi::FfiStr>(), align_of::<usize>());
        assert!(size_of::<abi::DriverApiV1>() > 0);
        assert_eq!(abi::FfiStr::empty().len, 0);
    }

    #[test]
    fn manifest_defaults_entry_symbol() {
        // entry 字段缺失时（§20 JSON 可省略）默认取 SDK 入口符号。
        let json = r#"{
            "id": "modbus-tcp",
            "name": "Modbus TCP",
            "version": "0.1.0",
            "abi": { "major": 1, "minor": 0 },
            "platforms": ["windows-x86_64"]
        }"#;
        let back: DriverManifest = serde_json::from_str(json).expect("反序列化失败");
        assert_eq!(back.entry, abi::ENTRY_SYMBOL);
        assert_eq!(back.abi.major, abi::ABI_MAJOR);
    }

    #[test]
    fn capabilities_default_is_read_polling() {
        let caps = ProtocolCapabilities::default();
        assert!(caps.read && caps.polling);
        assert!(!caps.write && !caps.subscription);
    }

    #[test]
    fn driver_read_item_round_trip() {
        let item = DriverReadItem {
            id: 1,
            address: "1!40001".to_owned(),
            expected_type: Some(observation_model::DataType::U16),
        };
        let json = serde_json::to_string(&item).expect("序列化失败");
        let back: DriverReadItem = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(item, back);
    }
}
