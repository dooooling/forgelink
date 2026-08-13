//! driver-sdk：Driver 插件开发 SDK（占位）。
//!
//! 定义稳定 C ABI v1（§16、§17 Normative）：唯一入口 `forgelink_driver_entry_v1()`、
//! `DriverApiV1`、`FfiStr`/`FfiSlice`/`FfiReadItem`/`FfiWriteItem`/`FfiOwnedBuffer`，
//! 以及 ABI 版本与结构扩展规则（`struct_size`/`abi_major`/`abi_minor`，§17.4）。
//!
//! Driver 内部 Rust 接口（§15）：Core 只传递 `DriverReadItem { id, address, expected_type }`
//! （§10），Driver 返回 `RawReadResult`/`RawEvent` 等原始结果（§7.2），
//! 不生成 `Observation`（§7.3，Observation 只能由 Profile + Domain 映射产生）。
//! 禁止跨 FFI 暴露 Rust trait 或 `async fn`。
