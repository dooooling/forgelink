//! Profile 注册表（§38 Normative）。
//!
//! 运行期通过 `profile_id` 查询 `DeviceProfile`。注册时先做完整校验（§37），
//! 重复 `profile_id` 一律拒绝；只读角色（Collector）与 Manager 共用本注册表，
//! 控制链路由 Control Engine 按 Profile 能力另行分派。

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use crate::loader::{LoaderError, load_profiles_dir};
use crate::models::DeviceProfile;
use crate::validate::validate_profile;

/// 注册错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// `profile_id` 已存在。
    DuplicateId { profile_id: String },
    /// 校验失败（字段、路径、缩放等，§37）。
    Invalid { profile_id: String, reason: String },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::DuplicateId { profile_id } => {
                write!(f, "profile_id `{profile_id}` 已注册")
            }
            RegistryError::Invalid { profile_id, reason } => {
                write!(f, "profile `{profile_id}` 校验失败: {reason}")
            }
        }
    }
}

impl std::error::Error for RegistryError {}

/// 按 `profile_id` 索引的只读 Profile 注册表。
#[derive(Debug, Default)]
pub struct ProfileRegistry {
    profiles: HashMap<String, Arc<DeviceProfile>>,
}

impl ProfileRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册单个 Profile；先校验，再查重（§37、§38）。
    ///
    /// 注册结果通过结构化日志记录（`component`、`profile_id`、`error_code`）。
    pub fn register(&mut self, profile: DeviceProfile) -> Result<(), RegistryError> {
        if let Err(e) = validate_profile(&profile) {
            let err = RegistryError::Invalid {
                profile_id: profile.id.clone(),
                reason: e.to_string(),
            };
            tracing::warn!(
                component = "profile-engine",
                profile_id = %profile.id,
                error_code = "profile_invalid",
                error = %err,
                "Profile 注册失败：校验未通过"
            );
            return Err(err);
        }
        if self.profiles.contains_key(&profile.id) {
            let err = RegistryError::DuplicateId {
                profile_id: profile.id.clone(),
            };
            tracing::warn!(
                component = "profile-engine",
                profile_id = %profile.id,
                error_code = "profile_duplicate",
                error = %err,
                "Profile 注册失败：ID 已存在"
            );
            return Err(err);
        }
        tracing::info!(
            component = "profile-engine",
            profile_id = %profile.id,
            "Profile 注册成功"
        );
        self.profiles.insert(profile.id.clone(), Arc::new(profile));
        Ok(())
    }

    /// 从 `profiles/` 目录加载并注册全部 Profile（§38）。
    ///
    /// 原子性保证：任一文件失败则整体失败，并且本次已写入的注册项
    /// 全部回滚，注册表保持加载前状态；已注册的同名 `profile_id`
    /// 不会被覆盖。
    pub fn load_dir(&mut self, dir: &Path) -> Result<usize, LoaderError> {
        let profiles = load_profiles_dir(dir)?;
        let count = profiles.len();
        let mut inserted: Vec<String> = Vec::with_capacity(count);
        for profile in profiles {
            let id = profile.id.clone();
            match self.register(profile) {
                Ok(()) => inserted.push(id),
                Err(e) => {
                    // 回滚：移除本次已注册的 Profile。
                    for id in &inserted {
                        self.profiles.remove(id);
                    }
                    return Err(match e {
                        RegistryError::DuplicateId { profile_id } => LoaderError::Duplicate {
                            path: dir.to_path_buf(),
                            profile_id,
                        },
                        RegistryError::Invalid { .. } => {
                            unreachable!("load_profiles_dir 已完成完整校验")
                        }
                    });
                }
            }
        }
        Ok(count)
    }

    /// 按 ID 查询 Profile。
    pub fn get(&self, profile_id: &str) -> Option<&Arc<DeviceProfile>> {
        self.profiles.get(profile_id)
    }

    /// 全部已注册 Profile（顺序不保证）。
    pub fn list(&self) -> impl Iterator<Item = &Arc<DeviceProfile>> {
        self.profiles.values()
    }

    /// 已注册数量。
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use observation_model::DomainKind;

    use super::*;
    use crate::models::{
        AcquisitionConstraints, ProfileCapabilities, ProfileProperty, WriteRounding,
    };
    use crate::test_util::init_global_subscriber;

    fn sample(id: &str) -> DeviceProfile {
        DeviceProfile {
            id: id.to_owned(),
            vendor: "Inovance".to_owned(),
            family: "MD500".to_owned(),
            models: vec!["MD500".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "modbus-rtu".to_owned(),
            properties: vec![ProfileProperty {
                path: "drive.output.frequency".to_owned(),
                driver_address: "1!40001".to_owned(),
                raw_type: observation_model::DataType::U16,
                value_type: observation_model::DataType::F64,
                unit: Some("Hz".to_owned()),
                scale: 0.01,
                offset: 0.0,
                write_rounding: WriteRounding::Nearest,
                readable: true,
                writable: true,
                default_interval_ms: None,
                min: None,
                max: None,
            }],
            commands: vec![],
            capabilities: ProfileCapabilities {
                supported_properties: vec![],
                supported_commands: vec![],
                acquisition: AcquisitionConstraints::default(),
                limits: Default::default(),
            },
        }
    }

    #[test]
    fn register_and_get() {
        init_global_subscriber();
        let mut registry = ProfileRegistry::new();
        registry
            .register(sample("inovance-md500"))
            .expect("注册成功");
        let profile = registry.get("inovance-md500").expect("应可查询");
        assert_eq!(profile.vendor, "Inovance");
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.list().count(), 1);
    }

    #[test]
    fn duplicate_id_rejected() {
        init_global_subscriber();
        let mut registry = ProfileRegistry::new();
        registry
            .register(sample("inovance-md500"))
            .expect("首次注册成功");
        let e = registry
            .register(sample("inovance-md500"))
            .expect_err("重复 ID 应拒绝");
        assert_eq!(
            e,
            RegistryError::DuplicateId {
                profile_id: "inovance-md500".to_owned()
            }
        );
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn invalid_profile_rejected() {
        init_global_subscriber();
        let mut registry = ProfileRegistry::new();
        let mut profile = sample("bad-profile");
        profile.properties[0].scale = 0.0;
        let e = registry.register(profile).expect_err("非法 Profile 应拒绝");
        assert!(matches!(e, RegistryError::Invalid { .. }));
        assert!(registry.is_empty());
    }

    #[test]
    fn missing_profile_returns_none() {
        init_global_subscriber();
        let registry = ProfileRegistry::new();
        assert!(registry.get("none").is_none());
    }

    #[test]
    fn load_dir_rolls_back_on_duplicate() {
        init_global_subscriber();
        // P2：目录加载期间发生 ID 冲突时，本次已注册项必须全部回滚。
        let dir = std::env::temp_dir().join(format!(
            "profile-engine-reg-rollback-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("创建目录失败");
        std::fs::write(
            dir.join("a.json"),
            sample_json("other-drive", "drive.output.frequency"),
        )
        .expect("写入失败");
        std::fs::write(
            dir.join("b.json"),
            sample_json("inovance-md500", "drive.output.voltage"),
        )
        .expect("写入失败");

        let mut registry = ProfileRegistry::new();
        registry
            .register(sample("inovance-md500"))
            .expect("预注册成功");
        let before = registry.len();

        let e = registry.load_dir(&dir).expect_err("冲突应导致整体失败");
        assert!(matches!(e, LoaderError::Duplicate { .. }));

        // 回滚验证：a.json 中成功的注册项被移除，注册表回到加载前状态。
        assert!(
            registry.get("other-drive").is_none(),
            "失败后应回滚本次注册"
        );
        assert!(registry.get("inovance-md500").is_some(), "原有注册不受影响");
        assert_eq!(registry.len(), before);
    }

    #[test]
    fn load_dir_registers_all() {
        init_global_subscriber();
        let dir = std::env::temp_dir().join(format!("profile-engine-reg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("创建目录失败");
        std::fs::write(
            dir.join("md500.json"),
            sample_json("inovance-md500", "drive.output.frequency"),
        )
        .expect("写入失败");

        let mut registry = ProfileRegistry::new();
        let count = registry.load_dir(&dir).expect("加载注册成功");
        assert_eq!(count, 1);
        assert!(registry.get("inovance-md500").is_some());
    }

    fn sample_json(id: &str, path: &str) -> String {
        format!(
            r#"{{
                "id": "{id}",
                "vendor": "Inovance",
                "family": "MD500",
                "models": ["MD500"],
                "domain": "drive",
                "driver_id": "modbus-rtu",
                "properties": [{{
                    "path": "{path}",
                    "driver_address": "1!40001",
                    "raw_type": "u16",
                    "value_type": "f64",
                    "unit": "Hz",
                    "scale": 0.01,
                    "offset": 0.0,
                    "write_rounding": "exact",
                    "readable": true,
                    "writable": true,
                    "default_interval_ms": null,
                    "min": null,
                    "max": null
                }}],
                "commands": [],
                "capabilities": {{
                    "supported_properties": [],
                    "supported_commands": [],
                    "acquisition": {{}},
                    "limits": {{}}
                }}
            }}"#
        )
    }
}
