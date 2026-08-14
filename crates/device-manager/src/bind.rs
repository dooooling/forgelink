//! Driver 绑定：Driver 工厂抽象与 Native Plugin 默认实现（§100 Load Driver）。

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use driver_loader::{LoaderError, NativeDriver, NativePlugin};
use poll_engine::{NativeDriverAdapter, PollDriver};

/// Driver 绑定失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    /// `driver_id` 没有可用的 Driver 实现。
    UnknownDriver { driver_id: String },
    /// Driver 实例创建失败（配置非法等）。
    CreateFailed { driver_id: String, error: String },
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDriver { driver_id } => write!(f, "driver `{driver_id}` 不可用"),
            Self::CreateFailed { driver_id, error } => {
                write!(f, "driver `{driver_id}` 创建失败: {error}")
            }
        }
    }
}

impl std::error::Error for BindError {}

/// Driver 工厂：按 `driver_id` 创建已绑定配置的 [`PollDriver`] 实例。
///
/// # 约定（§33 原则 2）
///
/// Core 不得按 `driver_id` 分支；设备实例通过本抽象分发 Driver 创建，
/// 具体实现（Native Plugin / Static / Process Plugin）由上层注入。
pub trait DriverFactory: Send + Sync {
    /// 创建并绑定一个 Driver 实例。
    ///
    /// `config` 是 `DeviceConnection.config`（§4.2），Core 只透传 JSON，
    /// 不解释协议私有字段；合法性由 Driver 自己校验。
    ///
    /// 本方法返回的驱动可能处于未连接状态：传输级错误由 Poll Engine
    /// 按 [`PollDriver`] 约定整体失败并退避重试（§34.3），Driver 负责
    /// 在读取前建立连接。
    fn create_driver(
        &self,
        driver_id: &str,
        config: &serde_json::Value,
    ) -> Result<Box<dyn PollDriver>, BindError>;
}

/// 基于 Native Plugin（C ABI v1）的默认 Driver 工厂（§19、§20）。
///
/// 上层负责按 `driver_id` 预加载 [`NativePlugin`]（Manifest 解析见 §20），
/// 本工厂只做"插件 → 实例"的创建与适配。
#[derive(Debug, Default)]
pub struct NativeDriverFactory {
    plugins: HashMap<String, Arc<NativePlugin>>,
}

impl NativeDriverFactory {
    /// 空工厂。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个已加载的 Native Plugin（按 Manifest `id` 索引）。
    pub fn add_plugin(&mut self, driver_id: String, plugin: Arc<NativePlugin>) {
        self.plugins.insert(driver_id, plugin);
    }
}

impl DriverFactory for NativeDriverFactory {
    fn create_driver(
        &self,
        driver_id: &str,
        config: &serde_json::Value,
    ) -> Result<Box<dyn PollDriver>, BindError> {
        let plugin = self
            .plugins
            .get(driver_id)
            .ok_or_else(|| BindError::UnknownDriver {
                driver_id: driver_id.to_owned(),
            })?;
        let config_str =
            serde_json::to_string(config).map_err(|source| BindError::CreateFailed {
                driver_id: driver_id.to_owned(),
                error: format!("连接配置 JSON 序列化失败: {source}"),
            })?;
        let driver = NativeDriver::create(Arc::clone(plugin), &config_str).map_err(|error| {
            BindError::CreateFailed {
                driver_id: driver_id.to_owned(),
                error: loader_error_detail(&error),
            }
        })?;
        Ok(Box::new(NativeDriverAdapter::new(driver)))
    }
}

/// 生成 Driver 创建失败的结构化错误描述（保留 Loader 的错误码）。
fn loader_error_detail(error: &LoaderError) -> String {
    match error {
        LoaderError::CallFailed { detail, .. } => {
            if let Some(info) = detail {
                format!("{}: {}", error.code(), info.message)
            } else {
                error.to_string()
            }
        }
        _ => format!("{}: {error}", error.code()),
    }
}
