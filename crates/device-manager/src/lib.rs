//! device-manager：设备实例管理（§2 Edge Core · Device Manager）。
//!
//! 管理设备实例（§63 三级标识：`domain` / `driver_id` / `profile_id`），
//! 完成 Driver/Profile 绑定（§72 关系模型）、读取项生成（§22、§100）
//! 与 `RawReadResult → Profile → Domain → Observation` 全链路映射（§47）。
//!
//! # 模块
//!
//! - [`DeviceManager`] / [`DeviceInstance`]：设备注册、绑定校验与查询；
//! - [`DriverFactory`]（`bind`）：Driver 创建抽象与 Native Plugin 默认实现
//!   （Core 不得按 `driver_id` 分支，§33 原则 2）；
//! - [`ReadItem`] / [`ReadGroup`]（`read_items`）：读取项生成与分组；
//! - [`map_results`] / [`map_failure`]（`pipeline`）：全链路映射，
//!   `Observation` 只能由 Profile + Domain 生成（§7.3）。
//!
//! # 与其他层的边界
//!
//! 本 crate 不执行网络 I/O 与周期调度：调度由 Poll Engine（§22）完成，
//! 本 crate 提供 [`DeviceInstance::poll_targets`] 供其组装 Poll 目标，
//! 并提供 `pipeline` 把 `PollEvent` 映射为 `Observation` 列表。

mod bind;
mod error;
mod instance;
mod pipeline;
mod read_items;
mod sequence;

pub use bind::{BindError, DriverFactory, NativeDriverFactory};
pub use error::DeviceManagerError;
pub use instance::{DeviceInstance, DeviceManager};
pub use pipeline::{MapContext, map_failure, map_results, reason_for_driver_error};
pub use read_items::{ReadGroup, ReadItem, ReadItemsError, generate_read_items, group_read_items};
pub use sequence::SequenceAllocator;
