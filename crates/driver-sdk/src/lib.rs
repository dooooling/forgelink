//! driver-sdk：Driver 插件开发 SDK（§15、§16、§17 Normative）。
//!
//! # Runtime V2 Transitional 兼容外壳（方案 §6.8）
//!
//! 类型本体已拆分至新 crate，本 crate 仅做 re-export 以保持既有
//! `driver_sdk::*` 导入路径编译不变：
//!
//! - Rust 语义契约（items/results/capabilities/`Driver` trait + 调用级错误模型）
//!   → [`driver_contract`]；
//! - 稳定 C ABI v1 类型（FfiStr/FfiSlice/FfiOwnedBuffer、DriverHandle、
//!   DriverApiV1、tag/envelope）→ [`driver_abi`]；
//! - Manifest v1 与 `platform` 常量为 legacy 模块，保留至 Phase 3 由
//!   `driver-package` Manifest v2 取代。
//!
//! 当前 `driver_sdk::Driver` async Rust trait 为 `Transitional`：可继续服务
//! 现有 Rust 内部测试/适配，但**不是新的跨插件契约**。目标态 Core 不保存
//! `Box<dyn Driver>`；Core 只面向 DeviceHandle/HostClient，Native 插件只面向
//! `driver-abi`（§6.8）。最后一个 ABI v1 / legacy manifest / legacy Driver
//! trait 使用点迁移完成后删除本兼容外壳。

pub mod abi {
    //! 稳定 C ABI v1（转发 [`driver_abi`]，§16~§18）。
    pub use driver_abi::*;
}

pub mod capabilities {
    //! 协议层能力声明（§13.1；转发 [`driver_contract`]）。
    pub use driver_contract::capabilities::*;
}

pub mod driver {
    //! Driver Rust 契约（§15；转发 [`driver_contract`]）。
    pub use driver_contract::driver::*;
}

pub mod items {
    //! Driver 请求项类型（§10、§15；转发 [`driver_contract`]）。
    pub use driver_contract::items::*;
}

pub mod manifest {
    //! Driver Manifest v1（§20，legacy——Phase 3 由 `driver-package`
    //! Manifest v2 取代）。
    pub use crate::manifest_v1::*;
}

pub mod results {
    //! Driver 结果与订阅/历史类型（§15；转发 [`driver_contract`]）。
    pub use driver_contract::results::*;
}

pub mod error {
    //! 调用级错误模型 V2（§28；转发 [`driver_contract`]）。
    pub use driver_contract::error::*;
}

pub use driver_abi::{
    DriverApiV1, DriverHandle, FfiEventCallback, FfiOwnedBuffer, FfiReadItem, FfiSlice, FfiStr,
    FfiWriteItem,
};
pub use driver_contract::{
    AddressMetadata, Driver, DriverBrowseNode, DriverCallError, DriverCommand, DriverErrorCategory,
    DriverReadItem, DriverWriteItem, HistoryRequest, ProtocolCapabilities, RawCommandResult,
    RawEvent, RawEventKind, RawHistoryPage, RawWriteResult, SubscriptionId, SubscriptionRequest,
};

// Manifest v1（legacy，§6.8）——保持既有 `driver_sdk::DriverManifest` 路径。
pub use crate::manifest_v1::{AbiVersion, DriverManifest, platform};

// ABI 版本与入口符号常量（§17.4、§18）。
pub use driver_abi::{ABI_MAJOR, ABI_MINOR, ENTRY_SYMBOL};

// 原始结果边界类型由 observation-model 定义（§7），在此转发以方便 Driver 使用。
pub use observation_model::{DataType, DriverErrorInfo, RawFieldValue, RawReadResult, RawValue};

// Manifest v1 本体暂留于此（legacy 模块，§6.8）；见上方 manifest 模块文档。
mod manifest_v1;

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use super::*;

    #[test]
    fn abi_version_constants() {
        assert_eq!(abi::ABI_MAJOR, 1);
        assert_eq!(abi::ABI_MINOR, 0);
        assert_eq!(abi::ENTRY_SYMBOL, "forgelink_driver_entry_v1");
        // re-export 与本体一致。
        assert_eq!(ABI_MAJOR, abi::ABI_MAJOR);
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
        assert_eq!(back.entry, ENTRY_SYMBOL);
        assert_eq!(back.abi.major, ABI_MAJOR);
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
            expected_type: Some(DataType::U16),
        };
        let json = serde_json::to_string(&item).expect("序列化失败");
        let back: DriverReadItem = serde_json::from_str(&json).expect("反序列化失败");
        assert_eq!(item, back);
    }

    #[test]
    fn contract_and_reexports_are_same_types() {
        // 外壳 re-export 的类型与新 crate 本体必须是同一类型（而非复制定义）：
        // 同一实例可双向赋值即证明。
        let caps: driver_contract::ProtocolCapabilities = ProtocolCapabilities::default();
        let _: ProtocolCapabilities = caps;
        let item = driver_contract::DriverReadItem {
            id: 1,
            address: "a".to_owned(),
            expected_type: None,
        };
        let _: DriverReadItem = item;
        let _: driver_abi::FfiStr = FfiStr::empty();
    }
}
