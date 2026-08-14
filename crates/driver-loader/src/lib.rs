//! driver-loader：Native Plugin 加载器（§19、§20）。
//!
//! 负责把驱动动态库（cdylib）加载进进程并适配为可调用的内部接口：
//!
//! - [`NativePlugin`]：加载动态库、解析 `forgelink_driver_entry_v1()` 入口、
//!   校验 ABI 版本 / `struct_size` / Manifest 一致性 / 必需函数指针
//!   （§17.4、§18、§20），并管理动态库生命周期——只要插件存活，
//!   其函数表就不会失效。
//! - [`NativeDriver`]：在已加载插件上 `create` 句柄，提供同步调用适配
//!   （§17.9 最小函数表）：状态码 → 稳定错误、Plugin 分配的 owned buffer
//!   由 RAII 通过 `free_buffer` 释放（§17.3 谁分配谁释放）、Drop 时
//!   自动 `destroy` 句柄。`subscribe`/`unsubscribe` 的回调适配
//!   （callback → 事件通道）属于后续事件适配任务，本 crate 不提供。
//! - [`LoaderError`]：稳定、机器可读的错误码，用于结构化日志
//!   `error_code` 字段（开发规范 §6）。
//!
//! # 安全模型
//!
//! - **panic 边界（§17.7）**：Rust panic / C++ exception 不得穿过 C ABI。
//!   边界收口在 Plugin 侧（SDK 要求 Plugin 在入口与每个 ABI 函数内
//!   `catch_unwind` 并转标准错误码）；本 crate 无法可靠地在 Core 侧捕获
//!   跨 FFI 的 panic，因此只做防御性入参校验与快速失败。
//! - **库卸载**：`NativePlugin` 持有 `libloading::Library`，函数表指针
//!   指向库内静态数据；`NativeDriver` 持有 `Arc<NativePlugin>` 保证
//!   `destroy` 完成前库不会卸载（见 [`NativeDriver`] 结构注释）。
//! - **调用串行**：句柄默认非并发安全（§17.5），`NativeDriver` 的全部
//!   方法接收 `&mut self`，由借用规则强制同一实例的调用串行化。
//!
//! # 目标平台（§6）
//!
//! Windows x64、Linux x64、Linux ARM64；动态库加载测试覆盖
//! Windows（`.dll`）与 Linux（`.so`）两种平台形态。

pub mod driver;
pub mod error;
pub mod plugin;

pub use driver::NativeDriver;
pub use error::LoaderError;
pub use plugin::NativePlugin;
