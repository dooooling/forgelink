//! Collector 错误类型（§93/§104：启动失败必须显式、可诊断）。

use std::fmt;

/// 配置错误（§100：非法配置明确报错，不静默取默认值）。
#[derive(Debug, Clone)]
pub enum ConfigError {
    /// 配置文件读取失败。
    Read { path: String, reason: String },
    /// 配置解析失败（语法/类型/未知字段）。
    Parse { path: String, reason: String },
    /// 字段级校验失败。
    Invalid { field: &'static str, reason: String },
}

impl ConfigError {
    pub(crate) fn invalid(field: &'static str, reason: impl Into<String>) -> Self {
        Self::Invalid {
            field,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, reason } => write!(f, "读取配置 {path} 失败: {reason}"),
            Self::Parse { path, reason } => write!(f, "解析配置 {path} 失败: {reason}"),
            Self::Invalid { field, reason } => write!(f, "配置项 {field} 非法: {reason}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Collector 运行时错误（组装/启动/运行阶段）。
#[derive(Debug)]
pub enum CollectorError {
    /// 配置校验或加载失败。
    Config(ConfigError),
    /// Device Profile 加载失败（目录不存在 / 文件非法）。
    Profiles(String),
    /// Native Plugin 加载失败（§19/§20，含 ABI 校验）。
    Driver(Box<dyn std::error::Error + Send + Sync>),
    /// 设备注册失败（Profile 绑定 / Driver 创建 / 读取项生成）。
    Device(device_manager::DeviceManagerError),
    /// 轮询任务启动失败。
    Poll(poll_engine::PollConfigError),
    /// 数据管道启动失败。
    Pipeline(data_pipeline::PipelineError),
    /// Local Buffer 打开失败（§103）。
    Buffer(local_buffer::LocalBufferError),
    /// MQTT 客户端启动失败（§31）。
    Mqtt(mqtt_client::MqttClientError),
    /// REST v1 只读接口启动失败（§31.5：监听绑定失败等）。
    Rest(String),
    /// 控制链路装配/停机失败（§81/§90：凭据缺失、Journal 打开失败、
    /// 策略非法等——fail-closed，启动即失败不静默降级）。
    Control(String),
    /// 运行期任务异常终止。
    Task(String),
    /// 停机超时（有限排空期限内未完成）。
    ShutdownTimeout { stage: &'static str },
    /// 输入输出（读取配置/证书等）。
    Io {
        context: &'static str,
        reason: String,
    },
}

impl From<ConfigError> for CollectorError {
    fn from(e: ConfigError) -> Self {
        Self::Config(e)
    }
}
impl From<device_manager::DeviceManagerError> for CollectorError {
    fn from(e: device_manager::DeviceManagerError) -> Self {
        Self::Device(e)
    }
}
impl From<poll_engine::PollConfigError> for CollectorError {
    fn from(e: poll_engine::PollConfigError) -> Self {
        Self::Poll(e)
    }
}
impl From<data_pipeline::PipelineError> for CollectorError {
    fn from(e: data_pipeline::PipelineError) -> Self {
        Self::Pipeline(e)
    }
}
impl From<local_buffer::LocalBufferError> for CollectorError {
    fn from(e: local_buffer::LocalBufferError) -> Self {
        Self::Buffer(e)
    }
}
impl From<mqtt_client::MqttClientError> for CollectorError {
    fn from(e: mqtt_client::MqttClientError) -> Self {
        Self::Mqtt(e)
    }
}

impl fmt::Display for CollectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(e) => write!(f, "配置错误: {e}"),
            Self::Profiles(e) => write!(f, "Profile 加载失败: {e}"),
            Self::Driver(e) => write!(f, "Driver 加载失败: {e}"),
            Self::Device(e) => write!(f, "设备注册失败: {e}"),
            Self::Poll(e) => write!(f, "轮询启动失败: {e}"),
            Self::Pipeline(e) => write!(f, "数据管道失败: {e}"),
            Self::Buffer(e) => write!(f, "Local Buffer 失败: {e}"),
            Self::Mqtt(e) => write!(f, "MQTT 客户端失败: {e}"),
            Self::Rest(e) => write!(f, "REST 接口启动失败: {e}"),
            Self::Control(e) => write!(f, "控制链路错误: {e}"),
            Self::Task(e) => write!(f, "运行时任务异常: {e}"),
            Self::ShutdownTimeout { stage } => write!(f, "停机超时（{stage} 未在期限内完成）"),
            Self::Io { context, reason } => write!(f, "{context} 失败: {reason}"),
        }
    }
}

impl std::error::Error for CollectorError {}
