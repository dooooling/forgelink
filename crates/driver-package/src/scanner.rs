//! Driver Package 扫描与发现（Runtime V2 方案 §6.3、§7 Normative）。
//!
//! 职责边界（§6.3）：扫描 Driver 目录、读取并校验 `driver.json`、选择当前
//! 平台 artifact、校验 Manifest schema / hash、生成 `DriverPackageDescriptor`。
//!
//! **不得**：`dlopen()`、创建 Driver Handle、调用协议代码——加载属于
//! `native-driver-loader`（Phase 7 起 Host 侧）。
//!
//! # TOCTOU（§7）
//!
//! Hash 必须在真正 load 前再次校验。本 crate 在扫描时计算并缓存 hash，
//! [`DriverPackageDescriptor::artifact_path`] 的消费方（loader/Host）在
//! 打开文件前必须重验——该二次校验点在 Phase 7 Host Adapter 落地。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub use crate::manifest::{
    AbiSpec, ArtifactSpec, DriverManifestV2, ExecutionModel, Isolation, ManifestError, PackageKind,
    RuntimeSpec, SCHEMA_VERSION_V2,
};

/// 扫描错误。
#[derive(Debug)]
pub enum ScanError {
    /// 目录不存在或不可读。
    Io { path: PathBuf, reason: String },
    /// 某个 package 的 manifest 非法。
    Manifest {
        path: PathBuf,
        source: ManifestError,
    },
    /// 同一扫描集合内出现重复 driver id（§6.3 duplicate id 检查）。
    DuplicateId { id: String, second_path: PathBuf },
    /// 当前平台 artifact 缺失或非法。
    Artifact {
        path: PathBuf,
        platform: String,
        reason: String,
    },
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, reason } => write!(f, "目录不可读 {}: {reason}", path.display()),
            Self::Manifest { path, source } => {
                write!(f, "manifest 非法 {}: {source}", path.display())
            }
            Self::DuplicateId { id, second_path } => {
                write!(
                    f,
                    "重复 driver id {id:?}（再次出现于 {}）",
                    second_path.display()
                )
            }
            Self::Artifact {
                path,
                platform,
                reason,
            } => write!(
                f,
                "平台 artifact 非法（{platform}）{}: {reason}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ScanError {}

/// 校验通过的单个 Driver Package 描述（§6.3 `DriverPackageDescriptor`）。
#[derive(Debug, Clone)]
pub struct DriverPackageDescriptor {
    /// Manifest 全量内容（id/version/abi/runtime 等以 manifest 为唯一事实来源，§7）。
    pub manifest: DriverManifestV2,
    /// driver.json 所在目录（package root）。
    pub root: PathBuf,
    /// 当前平台 artifact 绝对路径（canonicalize 后，已验证位于 root 内）。
    pub artifact_path: PathBuf,
    /// 扫描时实测的当前平台 artifact SHA-256（小写 hex）。
    ///
    /// 仅证明"扫描时刻"的内容；load 前须重验（TOCTOU，§7）。
    pub artifact_sha256: String,
}

impl DriverPackageDescriptor {
    /// driver id 快捷访问。
    pub fn id(&self) -> &str {
        &self.manifest.id
    }

    /// 版本快捷访问。
    pub fn version(&self) -> &str {
        &self.manifest.version
    }
}

/// 计算文件 SHA-256（小写十六进制）。
pub fn sha256_file(path: &Path) -> Result<String, ScanError> {
    let bytes = std::fs::read(path).map_err(|e| ScanError::Io {
        path: path.to_owned(),
        reason: e.to_string(),
    })?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("{digest:x}"))
}

/// 单个 package root 的发现与校验：读取 `driver.json` → 解析 v2 →
/// 选择当前平台 artifact → canonicalize 路径逃逸检查 → hash 校验。
///
/// `platform` 传 `None` 时使用 [`DriverManifestV2::current_platform`]；
/// 测试可显式传入目标平台。
///
/// 开发态放宽（§7 dev policy）：artifact 尚未写入 manifest `sha256`
/// （`None`）时跳过 hash **比对**，但仍计算并记录实测值——发布打包
/// （`scripts/package.*`）必须补齐 sha256 字段后才是完整发布包。
/// 已声明 sha256 时严格比对，不匹配即拒绝。
pub fn discover_package(
    root: &Path,
    platform: Option<&str>,
) -> Result<DriverPackageDescriptor, ScanError> {
    let platform: &str = match platform {
        Some(p) => p,
        None => DriverManifestV2::current_platform(),
    };
    let manifest_path = root.join("driver.json");
    let text = std::fs::read_to_string(&manifest_path).map_err(|e| ScanError::Io {
        path: manifest_path.clone(),
        reason: e.to_string(),
    })?;
    let manifest = DriverManifestV2::parse(&text).map_err(|source| ScanError::Manifest {
        path: manifest_path.clone(),
        source,
    })?;

    let artifact = manifest
        .artifact_for(platform)
        .ok_or_else(|| ScanError::Artifact {
            path: root.to_owned(),
            platform: platform.to_owned(),
            reason: "manifest 未声明当前平台 artifact".to_owned(),
        })?;

    let artifact_path = root.join(&artifact.path);
    // §7：canonicalize 后必须仍位于 package root 内，拒绝 `..` / symlink 逃逸。
    let canonical = artifact_path
        .canonicalize()
        .map_err(|e| ScanError::Artifact {
            path: root.to_owned(),
            platform: platform.to_owned(),
            reason: format!("artifact 不存在或不可达（{:?}）: {e}", artifact.path),
        })?;
    let canonical_root = root.canonicalize().map_err(|e| ScanError::Io {
        path: root.to_owned(),
        reason: format!("package root 不可达: {e}"),
    })?;
    if !canonical.starts_with(&canonical_root) {
        return Err(ScanError::Artifact {
            path: root.to_owned(),
            platform: platform.to_owned(),
            reason: format!(
                "artifact 逃逸出 package root（{:?} -> {}）",
                artifact.path,
                canonical.display()
            ),
        });
    }

    let actual_hash = sha256_file(&canonical)?;
    match &artifact.sha256 {
        Some(expected) if expected != &actual_hash => {
            return Err(ScanError::Artifact {
                path: root.to_owned(),
                platform: platform.to_owned(),
                reason: format!("hash 不匹配：manifest={expected} actual={actual_hash}"),
            });
        }
        _ => {}
    }

    Ok(DriverPackageDescriptor {
        manifest,
        root: root.to_owned(),
        artifact_path: canonical,
        artifact_sha256: actual_hash,
    })
}

/// 扫描一组候选目录（如 Collector 配置的 `drivers.directories`，方案 §37.3），
/// 返回全部校验通过的 package；同集合内 driver id 重复即失败（§6.3）。
///
/// MVP 规则（§29 fail-fast 精神）：任一 package 非法即整体报错，
/// 不静默跳过损坏包。
pub fn scan_directories(dirs: &[PathBuf]) -> Result<Vec<DriverPackageDescriptor>, ScanError> {
    let mut by_id: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut out = Vec::new();
    for dir in dirs {
        let entries = std::fs::read_dir(dir).map_err(|e| ScanError::Io {
            path: dir.clone(),
            reason: e.to_string(),
        })?;
        // 固定顺序遍历，保证 duplicate id 报告确定性。
        let mut roots: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.join("driver.json").is_file())
            .collect();
        roots.sort();
        for root in roots {
            let descriptor = discover_package(&root, None)?;
            let id = descriptor.id().to_owned();
            if by_id.contains_key(&id) {
                return Err(ScanError::DuplicateId {
                    id,
                    second_path: root,
                });
            }
            by_id.insert(id, root.clone());
            out.push(descriptor);
        }
    }
    Ok(out)
}
