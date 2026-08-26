//! driver-abi：跨动态库边界的稳定 C ABI 类型（Runtime V2 方案 §6.2 Normative）。
//!
//! 本 crate 是原 `driver-sdk::abi` 的目标归属，专门保存 C ABI 类型：
//! `FfiStr` / `FfiSlice` / `FfiOwnedBuffer`、`DriverHandle`、`DriverApiV1`、
//! ABI tag / envelope 与 entry symbol 常量。
//!
//! # 规则（§6.2）
//!
//! - 所有跨 ABI 类型必须 `#[repr(C)]` 或显式 `ptr + len`；
//! - `driver-contract` 不依赖本 crate（§36 依赖方向约束）；envelope JSON 中
//!   内嵌的语义类型（`DriverCommand` 等）引用自 `driver-contract`，
//!   不在此复制第二份定义；
//! - `native-driver-loader` 和 Native Driver 可以依赖本 crate；
//! - ABI v1/v2 在同一 crate 内按模块版本化，但不得把 loader 行为混进 ABI 定义。

pub mod envelope;
pub mod tag;
pub mod v1;

pub use v1::{
    ABI_MAJOR, ABI_MINOR, DriverApiV1, DriverHandle, ENTRY_SYMBOL, FfiEventCallback,
    FfiOwnedBuffer, FfiReadItem, FfiSlice, FfiStr, FfiWriteItem,
};
