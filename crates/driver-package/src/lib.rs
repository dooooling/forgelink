//! driver-package：Driver Package 扫描、Manifest v2 校验与平台 artifact 选择
//! （Runtime V2 方案 §6.3、§7 Normative）。
//!
//! `driver.json` 是 Package 元数据**唯一事实来源**（§7）；本 crate 负责：
//!
//! - Manifest v2 schema 解析与静态校验（[`manifest`]）；
//! - 目录扫描 / duplicate id / 当前平台 artifact 选择 / path 逃逸与 hash
//!   校验（[`scanner`]）；
//! - v1 → v2 离线迁移 parser（[`migration`]，非生产 discovery 兼容通道）。
//!
//! # 职责禁令（§6.3）
//!
//! 本 crate **不得** `dlopen()`、创建 Driver Handle 或调用协议代码——
//! 加载属于 `native-driver-loader`（Host 侧，Phase 7 起）。

pub mod manifest;
pub mod migration;
pub mod scanner;

pub use manifest::{
    AbiSpec, ArtifactSpec, DriverManifestV2, ExecutionModel, Isolation, ManifestError, PackageKind,
    RuntimeSpec, SCHEMA_VERSION_V2,
};
pub use migration::{MigrationError, migrate_v1_json};
pub use scanner::{
    DriverPackageDescriptor, ScanError, discover_package, scan_directories, sha256_file,
};
