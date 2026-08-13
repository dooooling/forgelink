//! Device Profile 动态加载（§38 Normative）。
//!
//! `profiles/` 目录按 厂商/系列 子目录组织，本模块递归扫描其中所有
//! `*.json` 文件，逐个反序列化为 `DeviceProfile` 并通过 `validate_profile`
//! 完整校验（§37）。任一文件失败即返回带文件路径的 `LoaderError`，
//! 保证注册进 Registry 的 Profile 全部有效。

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use crate::models::DeviceProfile;
use crate::validate::{ValidationError, validate_profile};

/// Profile 加载错误。
#[derive(Debug)]
pub enum LoaderError {
    /// 文件系统错误（含目录不存在）。
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// JSON 解析错误。
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    /// 加载后校验失败。
    Validation {
        path: PathBuf,
        source: ValidationError,
    },
    /// 注册时发现 `profile_id` 重复。
    Duplicate { path: PathBuf, profile_id: String },
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoaderError::Io { path, source } => {
                write!(f, "读取 `{}` 失败: {source}", path.display())
            }
            LoaderError::Json { path, source } => {
                write!(f, "解析 `{}` 失败: {source}", path.display())
            }
            LoaderError::Validation { path, source } => {
                write!(f, "`{}` 校验失败: {source}", path.display())
            }
            LoaderError::Duplicate { path, profile_id } => {
                write!(
                    f,
                    "`{}` 的 profile_id `{profile_id}` 已注册",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for LoaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LoaderError::Io { source, .. } => Some(source),
            LoaderError::Json { source, .. } => Some(source),
            LoaderError::Validation { source, .. } => Some(source),
            LoaderError::Duplicate { .. } => None,
        }
    }
}

/// 从目录递归加载全部 `*.json` Profile（§38）。
pub fn load_profiles_dir(dir: &Path) -> Result<Vec<DeviceProfile>, LoaderError> {
    let mut profiles = Vec::new();
    collect_dir(dir, &mut profiles)?;
    Ok(profiles)
}

fn collect_dir(dir: &Path, out: &mut Vec<DeviceProfile>) -> Result<(), LoaderError> {
    let entries = fs::read_dir(dir).map_err(|source| LoaderError::Io {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| LoaderError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_dir(&path, out)?;
        } else if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            out.push(load_single(&path)?);
        }
    }
    Ok(())
}

/// 加载并校验单个 Profile 文件。
pub fn load_single(path: &Path) -> Result<DeviceProfile, LoaderError> {
    let text = fs::read_to_string(path).map_err(|source| LoaderError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let profile: DeviceProfile =
        serde_json::from_str(&text).map_err(|source| LoaderError::Json {
            path: path.to_path_buf(),
            source,
        })?;
    validate_profile(&profile).map_err(|source| LoaderError::Validation {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(profile)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use observation_model::DomainKind;

    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "profile-engine-loader-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("创建临时目录失败");
        dir
    }

    fn sample_json() -> String {
        r#"{
            "id": "inovance-md500",
            "vendor": "Inovance",
            "family": "MD500",
            "models": ["MD500", "MD500E"],
            "domain": "drive",
            "driver_id": "modbus-rtu",
            "properties": [
                {
                    "path": "drive.output.frequency",
                    "driver_address": "1!40001",
                    "raw_type": "u16",
                    "value_type": "f64",
                    "unit": "Hz",
                    "scale": 0.01,
                    "offset": 0.0,
                    "write_rounding": "nearest",
                    "readable": true,
                    "writable": true,
                    "default_interval_ms": 1000,
                    "min": {"f64": 0.0},
                    "max": {"f64": 400.0}
                }
            ],
            "commands": [],
            "capabilities": {
                "supported_properties": ["drive.output.frequency"],
                "supported_commands": [],
                "acquisition": {},
                "limits": {}
            }
        }"#
        .to_owned()
    }

    #[test]
    fn load_nested_directory() {
        let root = temp_dir("nested");
        let vendor_dir = root.join("inovance").join("md500");
        fs::create_dir_all(&vendor_dir).expect("创建厂商子目录失败");
        fs::write(vendor_dir.join("profile.json"), sample_json()).expect("写入 JSON 失败");

        let profiles = load_profiles_dir(&root).expect("应成功加载");
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].id, "inovance-md500");
        assert_eq!(profiles[0].domain, DomainKind::Drive);
        assert_eq!(profiles[0].properties[0].path, "drive.output.frequency");
    }

    #[test]
    fn ignores_non_json_files() {
        let root = temp_dir("ignore");
        fs::write(root.join("notes.txt"), "not json").expect("写入失败");
        let profiles = load_profiles_dir(&root).expect("应忽略非 JSON 文件");
        assert!(profiles.is_empty());
    }

    #[test]
    fn missing_directory_is_io_error() {
        let missing = std::env::temp_dir().join("profile-engine-no-such-dir-42");
        let e = load_profiles_dir(&missing).expect_err("目录不存在应报错");
        assert!(matches!(e, LoaderError::Io { .. }));
    }

    #[test]
    fn invalid_json_reported_with_path() {
        let root = temp_dir("badjson");
        fs::write(root.join("bad.json"), "{ not json").expect("写入失败");
        let e = load_profiles_dir(&root).expect_err("非法 JSON 应报错");
        match e {
            LoaderError::Json { path, .. } => {
                assert!(path.to_string_lossy().ends_with("bad.json"));
            }
            other => panic!("应为 Json 错误: {other}"),
        }
    }

    #[test]
    fn invalid_profile_rejected() {
        let root = temp_dir("scale0");
        fs::write(
            root.join("bad.json"),
            sample_json().replace("\"scale\": 0.01", "\"scale\": 0.0"),
        )
        .expect("写入失败");
        let e = load_profiles_dir(&root).expect_err("scale=0 应被拒绝");
        match e {
            LoaderError::Validation { path, source } => {
                assert!(path.to_string_lossy().ends_with("bad.json"));
                assert_eq!(source.field, "properties[0].scale");
            }
            other => panic!("应为校验错误: {other}"),
        }
    }

    #[test]
    fn single_file_load() {
        let root = temp_dir("single");
        let path = root.join("md500.json");
        fs::write(&path, sample_json()).expect("写入失败");
        let profile = load_single(&path).expect("应成功加载");
        assert_eq!(profile.id, "inovance-md500");
    }
}
