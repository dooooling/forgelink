//! driver-contract：Core、Driver Runtime 与 Driver Host 共用的 Rust Driver 语义契约
//! （Runtime V2 方案 §6.1 Normative）。
//!
//! 本 crate 拥有 Driver 调用 payload / result 类型与 Rust `Driver` trait；
//! 不包含 `libloading`、Native C ABI（见 `driver-abi`）、具体协议逻辑和
//! Tokio 调度类型。Raw/Quality 数据类型仍以 `observation-model` 为唯一事实来源，
//! 此处仅 re-export，不复制定义。
//!
//! # 依赖方向（§6.1）
//!
//! ```text
//! observation-model
//!        ↑
//! driver-contract
//!        ↑
//! device-manager / driver-runtime / driver-host adapters
//! ```
//!
//! 本 crate **不依赖** `driver-abi`；跨动态库边界的 C ABI 类型属于
//! `driver-abi` crate（Runtime V2 方案 §6.2）。

pub mod capabilities;
pub mod driver;
pub mod error;
pub mod items;
pub mod results;

pub use capabilities::ProtocolCapabilities;
pub use driver::Driver;
pub use error::{DriverCallError, DriverErrorCategory};
pub use items::{DriverCommand, DriverReadItem, DriverWriteItem};
pub use results::{
    AddressMetadata, DriverBrowseNode, HistoryRequest, RawCommandResult, RawEvent, RawEventKind,
    RawHistoryPage, RawWriteResult, SubscriptionId, SubscriptionRequest,
};

// 原始结果边界类型由 observation-model 定义（§7），在此转发以方便使用方。
// Runtime V2 不重写 observation-model，避免同名 Raw 类型出现两份事实来源（§6.1）。
pub use observation_model::{DataType, DriverErrorInfo, RawFieldValue, RawReadResult, RawValue};
