//! driver-modbus：Modbus TCP / RTU 协议驱动（占位）。
//!
//! MVP 首个 Driver（§34）。实现协议编解码、地址解析、批量合并与会话串行化；
//! 按协议划分而非设备型号划分（§60），型号差异交由 Profile 承担。
//!
//! 插件形态：Native Plugin，`cdylib`，导出稳定 C ABI 入口
//! （见 driver-sdk 定义，禁止跨 FFI 暴露 Rust trait 或 `async fn`）。
