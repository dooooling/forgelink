//! Manifest v1 → v2 离线迁移 parser（Runtime V2 方案 §7.1 规则 2）。
//!
//! 读取旧形态 `driver.json`（无 `schema_version`、`platforms` 数组、
//! 不含 artifact/hash），生成 v2 结构骨架。用途：
//!
//! - **离线 migration tool / 生成辅助**——帮助维护者把存量 manifest 升级为 v2；
//! - **不是**生产 discovery 的兼容通道：Runtime V2 discovery 不长期静默接受
//!   缺失 `schema_version` 的 v1 Manifest（§7.1）。
//!
//! v1 没有 artifact/hash/runtime 信息，迁移产物必须由人工或打包脚本补齐：
//! `artifacts`（含各平台 path/sha256）与 `runtime` 块。

use serde::Deserialize;

use crate::manifest::{
    AbiSpec, ArtifactSpec, DriverManifestV2, ExecutionModel, Isolation, PackageKind, RuntimeSpec,
    SCHEMA_VERSION_V2,
};

/// v1 形态字段（与 driver-sdk legacy `DriverManifest` 同形状，此处独立定义
/// 避免 driver-package 依赖 driver-sdk 兼容外壳）。
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestV1 {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub entry: Option<String>,
    pub abi: AbiSpec,
    /// v1 的平台列表；v2 中被 `artifacts` key 取代。
    pub platforms: Vec<String>,
}

/// 迁移错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationError {
    /// 输入实际是 v2（带 schema_version）——无需迁移。
    AlreadyV2,
    /// v1 字段无法映射。
    Invalid { reason: String },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyV2 => write!(f, "输入已是 Manifest v2，无需迁移"),
            Self::Invalid { reason } => write!(f, "v1 manifest 非法: {reason}"),
        }
    }
}

impl std::error::Error for MigrationError {}

/// 把 v1 JSON 文本转换为 v2 骨架。
///
/// 生成的 v2 保留 v1 的 id/name/version/abi；每个 v1 平台生成一个
/// **无 hash** 的 artifact 占位（path 由调用方按实际产物文件名填写，
/// 打包脚本随后计算 sha256 回填）；`runtime` 采用保守默认
/// （`native` + `blocking_bounded` + `per_driver`）——现有 ABI v1 Driver
/// 全部是同步阻塞函数表（§7：不得凭"内部用了 Tokio"声明 async_cancelable），
/// 高风险 Vendor SDK 场景由维护者显式上调到 `blocking_uninterruptible` +
/// `per_device`。
pub fn migrate_v1_json(v1_json: &str) -> Result<DriverManifestV2, MigrationError> {
    // 先探测：带 schema_version 的一律拒绝（防误降级覆盖）。
    if let Ok(earlier) = serde_json::from_str::<serde_json::Value>(v1_json)
        && earlier.get("schema_version").is_some()
    {
        return Err(MigrationError::AlreadyV2);
    }
    let v1: ManifestV1 = serde_json::from_str(v1_json).map_err(|e| MigrationError::Invalid {
        reason: e.to_string(),
    })?;
    if v1.platforms.is_empty() {
        return Err(MigrationError::Invalid {
            reason: "v1 platforms 为空，无法生成 artifacts".to_owned(),
        });
    }

    let mut artifacts = std::collections::BTreeMap::new();
    for platform in &v1.platforms {
        artifacts.insert(
            platform.clone(),
            ArtifactSpec {
                // 占位路径：与仓库打包脚本产物命名一致（driver_{id}.dll /
                // libdriver_{id}.so），由打包流程校准并回填 sha256。
                path: format!("TODO-artifact-path-for-{platform}"),
                sha256: None,
            },
        );
    }

    Ok(DriverManifestV2 {
        schema_version: SCHEMA_VERSION_V2.to_owned(),
        id: v1.id,
        name: v1.name,
        version: v1.version,
        abi: AbiSpec {
            major: v1.abi.major,
            minor: v1.abi.minor,
        },
        artifacts,
        runtime: RuntimeSpec {
            kind: PackageKind::Native,
            execution_model: ExecutionModel::BlockingBounded,
            minimum_isolation: Isolation::PerDriver,
            default_isolation: Isolation::PerDriver,
        },
        min_core_version: None,
    })
}
