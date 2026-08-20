//! Collector 配置模型（§100 device.yaml 风格 + §103 buffer + §31 北向契约）。
//!
//! 配置来源为 Standalone 本地文件（§101）：YAML 或 JSON（serde_yaml
//! 兼容两者）。全部字段带默认值（除 `site_id` 与 `devices` 等必填项），
//! `deny_unknown_fields` 拒绝拼写错误的键。
//!
//! 运行时校验在 [`CollectorConfig::validate`] 完成；组件配置的构造
//! 全部经由本文件，最终落到各 crate 的配置类型
//! （`MqttClientConfig` / `PipelineConfig` / `LocalBufferConfig` /
//! `PollConfig`）时复用其自身 `validate`，不静默取默认值。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{CollectorError, ConfigError};

/// Collector 顶层配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorConfig {
    /// 站点标识（§31.2 Telemetry Batch 契约：`site_id` 必填非空）。
    pub site_id: String,
    /// 采集会话标识（可用作部署标识）；无论配置与否，生效会话
    /// （[`CollectorConfig::effective_session_id`]）都追加启动时刻纳秒
    /// 后缀，保证跨重启唯一（`build_observation`/`PipelineConfig` 均
    /// 拒绝空会话；评审 P1：固定会话会令 message_id 跨重启复用）。
    #[serde(default)]
    pub session_id: Option<String>,
    /// Device Profile 目录（§38：Profile 不写死在主程序，启动加载）。
    #[serde(default = "default_profiles_dir")]
    pub profiles_dir: PathBuf,
    /// 协议 Driver（Native Plugin，§19/§20：cdylib + Manifest）。
    #[serde(default)]
    pub driver: DriverSpec,
    /// 采集设备清单（§100）。至少一台；domain 缺省时由 Profile 决定。
    #[serde(default)]
    pub devices: Vec<DeviceSpec>,
    /// 北向输出（§31）。
    #[serde(default)]
    pub northbound: NorthboundConfig,
    /// 轮询参数（§22/§34.3），默认与 `PollConfig::default` 一致。
    #[serde(default)]
    pub poll: PollOptions,
    /// 数据管道（§31.2），默认与 `PipelineConfig::new` 一致。
    #[serde(default)]
    pub pipeline: PipelineOptions,
    /// Local Buffer/WAL（§103）。
    #[serde(default)]
    pub buffer: BufferOptions,
    /// 发送循环空闲轮询间隔（WAL 为空时的唤醒间隔，毫秒）。
    #[serde(default = "default_forward_poll_ms")]
    pub forward_poll_ms: u64,
    /// REST v1 只读管理接口（§31.5/§90.1）：默认禁用；启用必须显式
    /// 配置 `listen`（默认只监听 loopback，非 loopback 绑定需显式写明）。
    #[serde(default)]
    pub rest: RestOptions,
}

fn default_profiles_dir() -> PathBuf {
    PathBuf::from("profiles")
}
fn default_forward_poll_ms() -> u64 {
    500
}

/// Driver（Native Plugin）规格（§19/§20）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverSpec {
    /// cdylib 文件路径（Windows `.dll` / Linux `.so`）。
    pub plugin: PathBuf,
    /// 插件 Manifest（与 `driver-modbus` 等 Driver 声明的 id/abi 一致）。
    #[serde(default)]
    pub manifest: ManifestSpec,
}

impl Default for DriverSpec {
    fn default() -> Self {
        Self {
            plugin: PathBuf::new(),
            manifest: ManifestSpec::default(),
        }
    }
}

/// 插件 Manifest 声明（§20）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSpec {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_manifest_version")]
    pub version: String,
    /// ABI 版本（§17.4/§18）；缺省 1.0。
    #[serde(default)]
    pub abi: AbiSpec,
}

fn default_manifest_version() -> String {
    "0.1.0".into()
}

impl Default for ManifestSpec {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            version: default_manifest_version(),
            abi: AbiSpec::default(),
        }
    }
}

/// ABI 版本（§18）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiSpec {
    #[serde(default)]
    pub major: u16,
    #[serde(default)]
    pub minor: u16,
}

impl Default for AbiSpec {
    fn default() -> Self {
        Self { major: 1, minor: 0 }
    }
}

/// 采集设备（§100 device.yaml 风格；`domain` 缺省时由 Profile 决定）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceSpec {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    /// 业务类别；`None` 时取所绑 Profile 的 `domain`。
    #[serde(default)]
    pub domain: Option<crate::DomainKind>,
    /// 使用哪个协议 Driver（如 `modbus-tcp`）。
    pub driver: String,
    /// 使用哪个 Device Profile（如 `modbus-holding`）。
    pub profile: String,
    /// Driver 连接配置（不透明 JSON，由 Driver 自行解析；§4.2）。
    pub connection: serde_json::Value,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

fn default_true() -> bool {
    true
}

/// 北向输出配置（§31）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NorthboundConfig {
    #[serde(default)]
    pub mqtt: MqttOptions,
}

/// MQTT 输出配置（§31.1/§34.3/§90.1）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttOptions {
    pub broker_host: String,
    #[serde(default = "default_broker_port")]
    pub broker_port: u16,
    /// 客户端标识；空时运行时生成 `collector-{site_id}`。
    #[serde(default)]
    pub client_id: String,
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,
    /// 最大报文大小（字节），默认 16 MiB（§31.1 主题/载荷上限）。
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: usize,
    /// 断线重连指数退避（§34.3），默认 1s→30s。
    #[serde(default = "default_reconnect_min_secs")]
    pub reconnect_min_secs: u64,
    #[serde(default = "default_reconnect_max_secs")]
    pub reconnect_max_secs: u64,
    /// 重连次数上限；`None` 无限重试。
    #[serde(default)]
    pub max_reconnect_retries: Option<u32>,
    /// 发布队列容量（有界背压，§34.2），默认 256。
    #[serde(default = "default_publish_capacity")]
    pub publish_capacity: usize,
    /// 最大在途 QoS1 数量，默认 100。
    #[serde(default = "default_max_inflight")]
    pub max_inflight: u16,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// TLS/mTLS（§90.1）；缺省明文。
    #[serde(default)]
    pub tls: TlsOptions,
    /// LWT 离线信号覆盖的设备（§31.1 契约，通常主设备）；
    /// `None` 不启用 LWT。
    #[serde(default)]
    pub will_device_id: Option<String>,
}

fn default_broker_port() -> u16 {
    1883
}
fn default_keep_alive_secs() -> u64 {
    30
}
fn default_max_packet_size() -> usize {
    16 * 1024 * 1024
}
fn default_reconnect_min_secs() -> u64 {
    1
}
fn default_reconnect_max_secs() -> u64 {
    30
}
fn default_publish_capacity() -> usize {
    256
}
fn default_max_inflight() -> u16 {
    100
}

impl Default for MqttOptions {
    fn default() -> Self {
        Self {
            broker_host: String::new(),
            broker_port: default_broker_port(),
            client_id: String::new(),
            keep_alive_secs: default_keep_alive_secs(),
            max_packet_size: default_max_packet_size(),
            reconnect_min_secs: default_reconnect_min_secs(),
            reconnect_max_secs: default_reconnect_max_secs(),
            max_reconnect_retries: None,
            publish_capacity: default_publish_capacity(),
            max_inflight: default_max_inflight(),
            username: None,
            password: None,
            tls: TlsOptions::None,
            will_device_id: None,
        }
    }
}

/// TLS 模式（§90.1）。证书文件在运行时读取并校验。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TlsOptions {
    /// 明文 TCP（缺省）。
    #[default]
    None,
    /// 单向 TLS：校验 Broker 证书。
    ServerAuth {
        /// PEM 编码的 CA 证书文件。
        ca_pem_path: PathBuf,
    },
    /// 双向 TLS：额外出示客户端证书。
    Mutual {
        ca_pem_path: PathBuf,
        client_cert_pem_path: PathBuf,
        client_key_pem_path: PathBuf,
    },
}

/// 轮询参数（§22/§34.3），默认与 `PollConfig::default` 一致。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollOptions {
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: f64,
    #[serde(default = "default_backoff_base_ms")]
    pub backoff_base_ms: u64,
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: u64,
    #[serde(default = "default_backoff_factor")]
    pub backoff_factor: u32,
    #[serde(default = "default_shutdown_drain_secs")]
    pub shutdown_drain_secs: u64,
    /// 属性缺省采集周期（毫秒）；DeviceManager 构造参数（§37）。
    #[serde(default = "default_interval_ms")]
    pub default_interval_ms: u64,
}

fn default_request_timeout_secs() -> f64 {
    5.0
}
fn default_backoff_base_ms() -> u64 {
    1_000
}
fn default_backoff_max_ms() -> u64 {
    30_000
}
fn default_backoff_factor() -> u32 {
    2
}
fn default_shutdown_drain_secs() -> u64 {
    10
}
fn default_interval_ms() -> u64 {
    1_000
}

impl Default for PollOptions {
    fn default() -> Self {
        Self {
            request_timeout_secs: default_request_timeout_secs(),
            backoff_base_ms: default_backoff_base_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            backoff_factor: default_backoff_factor(),
            shutdown_drain_secs: default_shutdown_drain_secs(),
            default_interval_ms: default_interval_ms(),
        }
    }
}

/// 数据管道参数（§31.2），默认与 `PipelineConfig::new` 一致。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineOptions {
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_input_capacity")]
    pub input_capacity: usize,
    #[serde(default = "default_pipeline_drain_secs")]
    pub drain_timeout_secs: u64,
}

fn default_max_batch_size() -> usize {
    1_000
}
fn default_flush_interval_ms() -> u64 {
    1_000
}
fn default_input_capacity() -> usize {
    4_096
}
fn default_pipeline_drain_secs() -> u64 {
    5
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            max_batch_size: default_max_batch_size(),
            flush_interval_ms: default_flush_interval_ms(),
            input_capacity: default_input_capacity(),
            drain_timeout_secs: default_pipeline_drain_secs(),
        }
    }
}

/// Local Buffer/WAL 参数（§103）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferOptions {
    /// SQLite 数据库文件路径（必填；父目录须已存在）。
    pub db_path: PathBuf,
    /// 内存窗口（持有上限，非容量；§103 建议 10000）。
    #[serde(default = "default_memory_records")]
    pub memory_records: usize,
    /// 磁盘容量上限（字节），默认 2 GiB。
    #[serde(default = "default_disk_max_bytes")]
    pub disk_max_bytes: u64,
    /// 未确认记录保留时间（秒），默认 72h。
    #[serde(default = "default_retention_secs")]
    pub retention_secs: u64,
    /// 容量不足策略，默认背压（§103）。
    #[serde(default)]
    pub capacity_policy: BufferCapacityPolicy,
    /// 停机时发送循环的有限排空上限（秒），默认 5。
    #[serde(default = "default_buffer_drain_secs")]
    pub drain_timeout_secs: u64,
}

fn default_memory_records() -> usize {
    10_000
}
fn default_disk_max_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}
fn default_retention_secs() -> u64 {
    72 * 3600
}
fn default_buffer_drain_secs() -> u64 {
    5
}

impl Default for BufferOptions {
    fn default() -> Self {
        Self {
            db_path: PathBuf::new(),
            memory_records: default_memory_records(),
            disk_max_bytes: default_disk_max_bytes(),
            retention_secs: default_retention_secs(),
            capacity_policy: BufferCapacityPolicy::default(),
            drain_timeout_secs: default_buffer_drain_secs(),
        }
    }
}

/// 缓冲容量不足策略（§103，字符串枚举便于配置）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferCapacityPolicy {
    /// 背压：push 等待空间释放（默认）。
    #[default]
    Backpressure,
    /// 拒绝：立即返回容量错误。
    Reject,
}

/// REST v1 只读管理接口配置（§31.5/§90.1 安全基线）。
///
/// 默认**禁用**：`listen = None` 时不启动服务器。启用必须显式配置
/// 监听地址；`0.0.0.0`/`::`/非 loopback 地址属于显式配置（默认值
/// 只允许 loopback，§90.1）。端口 `0` 表示由操作系统分配（开发/测试）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestOptions {
    /// 监听地址（`host:port`）；`None` 禁用（缺省）。
    pub listen: Option<String>,
    /// 最大并发请求数（有界并发，§5），默认 64。
    #[serde(default = "default_rest_concurrency")]
    pub max_concurrency: usize,
}

impl Default for RestOptions {
    fn default() -> Self {
        Self {
            listen: None,
            max_concurrency: default_rest_concurrency(),
        }
    }
}

fn default_rest_concurrency() -> usize {
    64
}

impl RestOptions {
    fn validate(&self) -> Result<(), CollectorError> {
        if let Some(listen) = &self.listen {
            let addr = listen.parse::<std::net::SocketAddr>().map_err(|e| {
                ConfigError::invalid("rest.listen", format!("监听地址 {listen:?} 无法解析: {e}"))
            })?;
            // 端口 0 = 操作系统分配（开发/测试用，实际地址经
            // `CollectorRuntime::rest_addr()` 获取）。
            let _ = addr;
        }
        if self.max_concurrency == 0 {
            return Err(ConfigError::invalid("rest.max_concurrency", "必须大于 0").into());
        }
        // 评审 P2：超过 Tokio `Semaphore::MAX_PERMITS` 时 `Semaphore::new`
        // 会 panic，配置层必须限制上限并返回配置错误（不静默截断）。
        if self.max_concurrency > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ConfigError::invalid(
                "rest.max_concurrency",
                format!(
                    "超出 Tokio Semaphore 并发上限 {}",
                    tokio::sync::Semaphore::MAX_PERMITS
                ),
            )
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_max_concurrency_upper_bound_rejected() {
        // 评审 P2：Semaphore::new 对超限 permits 会 panic，配置必须
        // 在启动前返回错误而非崩溃。
        let ok = RestOptions {
            listen: None,
            max_concurrency: tokio::sync::Semaphore::MAX_PERMITS,
        };
        assert!(ok.validate().is_ok(), "等于上限应通过");
        let over = RestOptions {
            listen: None,
            max_concurrency: tokio::sync::Semaphore::MAX_PERMITS + 1,
        };
        let err = over.validate().expect_err("超限必须拒绝");
        assert!(
            err.to_string().contains("rest.max_concurrency"),
            "错误应指向字段: {err}"
        );
    }
}

impl CollectorConfig {
    /// 从文件加载配置（YAML 或 JSON，按扩展名/内容自动识别）。
    pub fn load_path(path: &Path) -> Result<Self, CollectorError> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            CollectorError::Config(ConfigError::Read {
                path: path.display().to_string(),
                reason: e.to_string(),
            })
        })?;
        let config: Self = serde_yaml::from_str(&text).map_err(|e| {
            CollectorError::Config(ConfigError::Parse {
                path: path.display().to_string(),
                reason: e.to_string(),
            })
        })?;
        Ok(config)
    }

    /// 校验配置合法性；组件级约束与各 crate `validate` 一致，缺省
    /// 值在此补齐后统一生效（不静默取默认值）。
    pub fn validate(&self) -> Result<(), CollectorError> {
        if self.site_id.is_empty() {
            return Err(ConfigError::invalid("site_id", "站点标识不能为空").into());
        }
        // §31.1 主题层级校验：site_id/device_id 为空或含 `/` 会破坏固定
        // Topic 层级（订阅/ACL 路由错误）。此前仅非空校验，非法标识到
        // 运行时发布才被拒绝（评审 P2），此处启动即拦截。
        mqtt_client::telemetry_topic(&self.site_id, "device").map_err(|e| {
            ConfigError::invalid("site_id", format!("站点标识无法构成合法主题: {e}"))
        })?;
        if self.session_id.as_ref().is_some_and(|s| s.is_empty()) {
            return Err(ConfigError::invalid("session_id", "会话标识不能为空").into());
        }
        if self.devices.is_empty() {
            return Err(ConfigError::invalid("devices", "至少需要一台采集设备").into());
        }
        let mut seen = std::collections::HashSet::new();
        for d in &self.devices {
            if d.id.is_empty() {
                return Err(ConfigError::invalid("devices[].id", "设备标识不能为空").into());
            }
            mqtt_client::telemetry_topic(&self.site_id, &d.id).map_err(|e| {
                ConfigError::invalid(
                    "devices[].id",
                    format!("设备 {} 无法构成合法主题: {e}", d.id),
                )
            })?;
            if d.driver.is_empty() {
                return Err(ConfigError::invalid(
                    "devices[].driver",
                    format!("设备 {} 未指定 Driver", d.id),
                )
                .into());
            }
            if d.profile.is_empty() {
                return Err(ConfigError::invalid(
                    "devices[].profile",
                    format!("设备 {} 未指定 Profile", d.id),
                )
                .into());
            }
            if !seen.insert(d.id.clone()) {
                return Err(
                    ConfigError::invalid("devices[].id", format!("设备 {} 重复", d.id)).into(),
                );
            }
        }
        // LWT 状态主题（§31.1）与在线/离线状态同一校验，非法标识启动
        // 即拒绝；且 LWT 必须指向设备清单中存在且**已启用**的设备——
        // 指向不存在或已禁用的设备会在异常断线时发布虚假设备的离线
        // 状态（评审 P2）。
        if let Some(will) = &self.northbound.mqtt.will_device_id {
            mqtt_client::status_topic(&self.site_id, will).map_err(|e| {
                ConfigError::invalid(
                    "northbound.mqtt.will_device_id",
                    format!("设备 {will} 无法构成合法状态主题: {e}"),
                )
            })?;
            match self.devices.iter().find(|d| &d.id == will) {
                Some(d) if d.enabled => {}
                Some(_) => {
                    return Err(ConfigError::invalid(
                        "northbound.mqtt.will_device_id",
                        format!("设备 {will} 已禁用，LWT 不得指向禁用设备"),
                    )
                    .into());
                }
                None => {
                    return Err(ConfigError::invalid(
                        "northbound.mqtt.will_device_id",
                        format!("设备 {will} 不在设备清单中"),
                    )
                    .into());
                }
            }
        }
        if self.driver.plugin.as_os_str().is_empty() {
            return Err(ConfigError::invalid("driver.plugin", "Driver 插件路径不能为空").into());
        }
        if self.driver.manifest.id.is_empty() {
            return Err(ConfigError::invalid("driver.manifest.id", "Driver 标识不能为空").into());
        }
        self.northbound.mqtt.validate()?;
        self.poll.validate()?;
        self.pipeline.validate()?;
        self.buffer.validate()?;
        self.rest.validate()?;
        Ok(())
    }

    /// 生效的采集会话标识：无论配置与否都追加启动时刻纳秒后缀，
    /// 保证每次进程启动的会话不同——`message_id`/`observation_id`
    /// 嵌入会话，跨重启不重复，下游可区分两次运行的观测
    /// （评审 P1：固定会话会令 message_id 跨重启复用）。
    pub fn effective_session_id(&self) -> String {
        match &self.session_id {
            Some(s) => format!("{s}-{}", crate::now_ns()),
            None => format!("{}-{}", self.site_id, crate::now_ns()),
        }
    }
}

impl MqttOptions {
    /// 组件级校验（§31.1/§34.3/§90.1），与 `MqttClientConfig::validate` 对齐。
    pub fn validate(&self) -> Result<(), CollectorError> {
        if self.broker_host.is_empty() {
            return Err(
                ConfigError::invalid("northbound.mqtt.broker_host", "Broker 主机不能为空").into(),
            );
        }
        if self.broker_port == 0 {
            return Err(ConfigError::invalid(
                "northbound.mqtt.broker_port",
                "Broker 端口必须大于 0",
            )
            .into());
        }
        if self.keep_alive_secs == 0 {
            return Err(ConfigError::invalid(
                "northbound.mqtt.keep_alive_secs",
                "keep_alive 必须大于 0",
            )
            .into());
        }
        if self.max_packet_size < crate::MIN_PACKET_SIZE {
            return Err(ConfigError::invalid(
                "northbound.mqtt.max_packet_size",
                format!("最大报文大小不能小于 {}", crate::MIN_PACKET_SIZE),
            )
            .into());
        }
        if self.publish_capacity == 0 || self.max_inflight == 0 {
            return Err(ConfigError::invalid(
                "northbound.mqtt",
                "publish_capacity 与 max_inflight 必须大于 0",
            )
            .into());
        }
        if self.will_device_id.as_ref().is_some_and(|d| d.is_empty()) {
            return Err(
                ConfigError::invalid("northbound.mqtt.will_device_id", "设备标识不能为空").into(),
            );
        }
        match &self.tls {
            TlsOptions::None => {}
            TlsOptions::ServerAuth { ca_pem_path } => {
                if ca_pem_path.as_os_str().is_empty() {
                    return Err(ConfigError::invalid(
                        "northbound.mqtt.tls.ca_pem_path",
                        "CA 证书路径不能为空",
                    )
                    .into());
                }
            }
            TlsOptions::Mutual {
                ca_pem_path,
                client_cert_pem_path,
                client_key_pem_path,
            } => {
                if ca_pem_path.as_os_str().is_empty()
                    || client_cert_pem_path.as_os_str().is_empty()
                    || client_key_pem_path.as_os_str().is_empty()
                {
                    return Err(ConfigError::invalid(
                        "northbound.mqtt.tls",
                        "mTLS 需要 ca/cert/key 三个证书文件路径",
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    /// 转换为 `MqttClientConfig`（复用其自身校验，不静默取默认值）。
    pub fn to_client_config(
        &self,
        site_id: &str,
    ) -> Result<crate::MqttClientConfig, CollectorError> {
        let client_id = if self.client_id.is_empty() {
            format!("collector-{site_id}")
        } else {
            self.client_id.clone()
        };
        let mut cfg = crate::MqttClientConfig::new(&client_id, &self.broker_host, self.broker_port);
        cfg.keep_alive = Duration::from_secs(self.keep_alive_secs);
        cfg.max_packet_size = self.max_packet_size;
        cfg.reconnect_min_delay = Duration::from_secs(self.reconnect_min_secs);
        cfg.reconnect_max_delay = Duration::from_secs(self.reconnect_max_secs);
        cfg.max_reconnect_retries = self.max_reconnect_retries;
        cfg.publish_capacity = self.publish_capacity;
        cfg.max_inflight = self.max_inflight;
        cfg.username = self.username.clone();
        cfg.password = self.password.clone();
        cfg.tls = self.tls.to_tls_mode()?;
        // LWT 离线信号（§31.1）：will_device_id 指定的设备以 retained
        // 离线 Envelope 作为 Will（与 publish_online 在线状态同构，
        // sent_at_ns=0 以到达时间为准）；主题/载荷合法性由
        // `WillConfig::offline_status` 校验，启动即失败。此前配置的
        // will_device_id 从未生效（评审 P2）。
        if let Some(device_id) = &self.will_device_id {
            cfg.will = Some(mqtt_client::WillConfig::offline_status(site_id, device_id)?);
        }
        cfg.validate().map_err(|reason| {
            CollectorError::Config(ConfigError::invalid("northbound.mqtt", reason))
        })?;
        Ok(cfg)
    }
}

impl TlsOptions {
    /// 读取证书文件并构造 `TlsMode`（§90.1）。
    pub fn to_tls_mode(&self) -> Result<crate::TlsMode, CollectorError> {
        Ok(match self {
            TlsOptions::None => crate::TlsMode::None,
            TlsOptions::ServerAuth { ca_pem_path } => crate::TlsMode::ServerAuth {
                ca_pem: read_file("ca_pem_path", ca_pem_path)?,
            },
            TlsOptions::Mutual {
                ca_pem_path,
                client_cert_pem_path,
                client_key_pem_path,
            } => crate::TlsMode::MutualTls {
                ca_pem: read_file("ca_pem_path", ca_pem_path)?,
                client_cert_pem: read_file("client_cert_pem_path", client_cert_pem_path)?,
                client_key_pem: read_file("client_key_pem_path", client_key_pem_path)?,
            },
        })
    }
}

fn read_file(field: &'static str, path: &Path) -> Result<Vec<u8>, CollectorError> {
    std::fs::read(path).map_err(|e| {
        ConfigError::invalid(field, format!("读取证书文件 {} 失败: {e}", path.display())).into()
    })
}

impl PollOptions {
    fn validate(&self) -> Result<(), CollectorError> {
        if self.request_timeout_secs <= 0.0 || !self.request_timeout_secs.is_finite() {
            return Err(ConfigError::invalid("poll.request_timeout_secs", "必须为正数").into());
        }
        if self.backoff_base_ms == 0 || self.backoff_max_ms == 0 {
            return Err(ConfigError::invalid("poll", "退避基数与上限必须大于 0").into());
        }
        if self.backoff_factor == 0 {
            return Err(ConfigError::invalid("poll.backoff_factor", "必须大于 0").into());
        }
        if self.shutdown_drain_secs == 0 {
            return Err(ConfigError::invalid("poll.shutdown_drain_secs", "必须大于 0").into());
        }
        if self.default_interval_ms == 0 {
            return Err(ConfigError::invalid("poll.default_interval_ms", "必须大于 0").into());
        }
        Ok(())
    }

    /// 转换为 `PollConfig`。
    pub fn to_poll_config(&self) -> crate::PollConfig {
        crate::PollConfig {
            request_timeout: Duration::from_secs_f64(self.request_timeout_secs),
            backoff_base_ms: self.backoff_base_ms,
            backoff_max_ms: self.backoff_max_ms,
            backoff_factor: self.backoff_factor,
            shutdown_drain_timeout: Duration::from_secs(self.shutdown_drain_secs),
        }
    }
}

impl PipelineOptions {
    fn validate(&self) -> Result<(), CollectorError> {
        if self.max_batch_size == 0
            || self.flush_interval_ms == 0
            || self.input_capacity == 0
            || self.drain_timeout_secs == 0
        {
            return Err(ConfigError::invalid(
                "pipeline",
                "max_batch_size/flush_interval_ms/input_capacity/drain_timeout_secs 必须大于 0",
            )
            .into());
        }
        Ok(())
    }

    /// 转换为 `PipelineConfig`（复用其自身校验）。
    pub fn to_pipeline_config(
        &self,
        site_id: &str,
        session_id: &str,
    ) -> Result<crate::PipelineConfig, CollectorError> {
        let mut cfg = crate::PipelineConfig::new(site_id, session_id);
        cfg.max_batch_size = self.max_batch_size;
        cfg.flush_interval = Duration::from_millis(self.flush_interval_ms);
        cfg.input_capacity = self.input_capacity;
        cfg.drain_timeout = Duration::from_secs(self.drain_timeout_secs);
        cfg.validate()
            .map_err(|reason| CollectorError::Config(ConfigError::invalid("pipeline", reason)))?;
        Ok(cfg)
    }
}

impl BufferOptions {
    fn validate(&self) -> Result<(), CollectorError> {
        if self.db_path.as_os_str().is_empty() {
            return Err(ConfigError::invalid("buffer.db_path", "数据库路径不能为空").into());
        }
        if self.memory_records == 0 || self.disk_max_bytes == 0 || self.retention_secs == 0 {
            return Err(ConfigError::invalid(
                "buffer",
                "memory_records/disk_max_bytes/retention_secs 必须大于 0",
            )
            .into());
        }
        if self.retention_secs as u128 * 1_000_000_000 > i64::MAX as u128 {
            return Err(ConfigError::invalid(
                "buffer.retention_secs",
                "保留时间超过 i64 纳秒范围（≈292 年）",
            )
            .into());
        }
        if self.drain_timeout_secs == 0 {
            return Err(ConfigError::invalid("buffer.drain_timeout_secs", "必须大于 0").into());
        }
        Ok(())
    }

    /// 转换为 `LocalBufferConfig`（复用其自身校验）。
    pub fn to_buffer_config(&self) -> Result<crate::LocalBufferConfig, CollectorError> {
        let cfg = crate::LocalBufferConfig {
            db_path: self.db_path.clone(),
            memory_records: self.memory_records,
            disk_max_bytes: self.disk_max_bytes,
            retention: Duration::from_secs(self.retention_secs),
            capacity_policy: match self.capacity_policy {
                BufferCapacityPolicy::Backpressure => crate::CapacityPolicy::Backpressure,
                BufferCapacityPolicy::Reject => crate::CapacityPolicy::Reject,
            },
        };
        cfg.validate()?;
        Ok(cfg)
    }
}
