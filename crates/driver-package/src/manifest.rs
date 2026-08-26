//! Driver Manifest v2 schema 与校验（Runtime V2 方案 §7、§20 Normative）。
//!
//! `driver.json` 是 Package 元数据的**唯一事实来源**（§7）：Collector 不得
//! 重复声明 id/version/ABI/artifact。一个 Package 可包含多个平台 artifact，
//! 因此 v2 不再使用 v1 的"单一 binary + `platforms` 数组"矛盾表达——
//! `artifacts` 的 key 就是支持平台集合（§7）。
//!
//! # 校验责任边界
//!
//! 本模块只做 **schema/语义静态校验**；当前平台 artifact 的存在性、hash
//! 与路径逃逸检查由 [`crate::scanner`] 在发现时执行（§7.1：Runtime 只验证
//! 当前平台 artifact，不要求一个平台包携带其他平台二进制）。

use serde::{Deserialize, Serialize};

/// Driver Package 支持的 `runtime.kind` 值（§7 示例仅定义 `native`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageKind {
    /// Native C ABI 动态库（`cdylib`）。
    Native,
}

/// Driver 执行模型声明（§16.4 Normative）。
///
/// 这是 Driver 的**能力/风险声明**，不是运行时事实：现有 ABI v1 Driver 即使
/// 由 Rust 编写，只要 ABI 调用仍是同步阻塞函数，就不能声明为
/// `async_cancelable`（§7）；Host 启动后还须与 Binary Descriptor（ABI v2）
/// 交叉校验（§11、§22.4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionModel {
    /// 纯 Rust async I/O；可通过取消 Future 停止等待与 I/O（§16.4.1）。
    AsyncCancelable,
    /// 同步 SDK 但自身支持可靠 timeout；使用专用 worker thread（§16.4.2）。
    BlockingBounded,
    /// 可能永久阻塞的 Vendor DLL / 不可取消 FFI / 线程亲和；
    /// 必须依赖独立 Driver Host + hard process kill（§16.4.3）。
    BlockingUninterruptible,
}

/// Host 隔离级别（§22 Normative）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    /// 所有该 Driver 的 Session 共享一个 Host 进程。
    Shared,
    /// 每个该 Driver 的实例一个 Host 进程（同协议连接共享故障域，§43.6）。
    PerDriver,
    /// 每个设备连接独立 Host 进程（最强隔离，§22.3）。
    PerDevice,
}

/// Manifest v2（§7 示例形状）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DriverManifestV2 {
    /// 固定 `"2.0"`。缺失即按 v1 处理（生产 discovery 不长期静默接受
    /// 缺失 `schema_version` 的 Manifest，§7.1）。
    pub schema_version: String,
    /// 协议驱动 ID（如 `modbus-tcp`）；同目录集合内必须唯一（§6.3 duplicate id 检查）。
    pub id: String,
    pub name: String,
    pub version: String,

    /// 目标 ABI 版本（§18/§20）。
    pub abi: AbiSpec,

    /// 平台 → artifact 映射；key 即支持平台集合（§7）。
    pub artifacts: std::collections::BTreeMap<String, ArtifactSpec>,

    /// 运行时要求块（§7）。
    pub runtime: RuntimeSpec,

    /// 兼容的最小 Core 版本（§7 示例字段；MVP 仅记录，不做 semver 解析）。
    #[serde(default)]
    pub min_core_version: Option<String>,
}

/// ABI 版本声明（§20 形状沿用 v1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiSpec {
    pub major: u16,
    pub minor: u16,
}

/// 单平台 artifact 声明（§7）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSpec {
    /// 相对 package root 的路径；canonicalize 后必须仍在 root 内（§7 路径逃逸禁令）。
    pub path: String,
    /// artifact SHA-256（小写十六进制）。发布必填；开发态可由显式 dev policy
    /// 放宽（§7）。Hash 只提供完整性绑定，不等同发布者身份可信（§7）。
    #[serde(default)]
    pub sha256: Option<String>,
}

/// 运行时要求（§7）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSpec {
    pub kind: PackageKind,
    pub execution_model: ExecutionModel,
    /// 安全下限：部署配置只允许选择**相同或更严格**的隔离级别（§7）。
    pub minimum_isolation: Isolation,
    /// 默认部署隔离级别；不得低于 `minimum_isolation`（§7 / §22.4）。
    pub default_isolation: Isolation,
}

/// Manifest v2 静态校验错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    /// `schema_version` 不是 `"2.0"`。
    UnsupportedSchema(String),
    /// 字段缺失或取值非法。
    InvalidField { field: &'static str, reason: String },
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchema(v) => write!(f, "不支持的 manifest schema_version: {v:?}"),
            Self::InvalidField { field, reason } => {
                write!(f, "manifest 字段非法 {field}: {reason}")
            }
        }
    }
}

impl std::error::Error for ManifestError {}

/// Manifest v2 固定 schema 版本值。
pub const SCHEMA_VERSION_V2: &str = "2.0";

impl DriverManifestV2 {
    /// 从 JSON 文本解析并完成静态校验。
    ///
    /// 校验项（§6.3 / §7）：
    /// - `schema_version == "2.0"`；
    /// - id 非空且不含路径分隔符（id 参与目录布局）；
    /// - `artifacts` 非空；
    /// - 每个 artifact `path` 为相对路径且不含父目录分量（`..`）；
    ///   绝对路径与 symlink 逃逸由 scanner 以 canonicalize 兜底拒绝；
    /// - `sha256` 若存在必须是 64 位小写十六进制；
    /// - `default_isolation >= minimum_isolation`（不得低于安全下限）。
    pub fn parse(json: &str) -> Result<Self, ManifestError> {
        let manifest: Self =
            serde_json::from_str(json).map_err(|e| ManifestError::InvalidField {
                field: "(json)",
                reason: e.to_string(),
            })?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// 对已反序列化的结构补齐静态校验（migration parser 复用）。
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.schema_version != SCHEMA_VERSION_V2 {
            return Err(ManifestError::UnsupportedSchema(
                self.schema_version.clone(),
            ));
        }
        if self.id.is_empty() || self.id.contains('/') || self.id.contains('\\') {
            return Err(ManifestError::InvalidField {
                field: "id",
                reason: format!("id 不得为空或含路径分隔符: {:?}", self.id),
            });
        }
        if self.artifacts.is_empty() {
            return Err(ManifestError::InvalidField {
                field: "artifacts",
                reason: "至少声明一个平台 artifact".to_owned(),
            });
        }
        for (platform, artifact) in &self.artifacts {
            let path = std::path::Path::new(&artifact.path);
            // 绝对路径判定须跨平台：Windows 上 "/x" 无盘符不算 is_absolute，
            // 但 has_root() 为 true 且在 canonicalize 后会落到当前盘根——
            // 同样视为逃逸（§7）。
            if path.is_absolute()
                || path.has_root()
                || artifact.path.split(['/', '\\']).any(|seg| seg == "..")
            {
                return Err(ManifestError::InvalidField {
                    field: "artifacts.path",
                    reason: format!(
                        "artifact 路径必须位于 package root 内: {platform} -> {:?}",
                        artifact.path
                    ),
                });
            }
            if let Some(hash) = &artifact.sha256
                && (hash.len() != 64
                    || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
            {
                return Err(ManifestError::InvalidField {
                    field: "sha256",
                    reason: format!("必须是 64 位小写十六进制: {platform}"),
                });
            }
        }
        if self.runtime.default_isolation < self.runtime.minimum_isolation {
            return Err(ManifestError::InvalidField {
                field: "runtime.default_isolation",
                reason: format!(
                    "默认隔离 {:?} 不得低于安全下限 {:?}",
                    self.runtime.default_isolation, self.runtime.minimum_isolation
                ),
            });
        }
        Ok(())
    }

    /// 取指定平台的 artifact 声明。
    pub fn artifact_for(&self, platform: &str) -> Option<&ArtifactSpec> {
        self.artifacts.get(platform)
    }

    /// 当前构建目标对应的平台 key（值域与打包脚本一致，§20）。
    pub fn current_platform() -> &'static str {
        if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
            "windows-x86_64"
        } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            "linux-x86_64"
        } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
            "linux-aarch64"
        } else {
            // 未支持平台的处理交给 scanner 显式报错，不在编译期 panic。
            "unknown"
        }
    }
}
