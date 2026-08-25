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
    /// 控制链路配置段（§81/§90；`Some` = 用户显式提供了 `control:` 段，
    /// 即启用控制链路——与 `rest.listen` 启用 REST 同一模式）。
    ///
    /// 段与构建 feature 的一致性由 `validate` 强制：
    /// - `control` feature 构建：段出现即装配控制链路（凭据/Journal 加载
    ///   失败 fail-closed）；段缺省则保持只读采集（启动时告警提示，避免
    ///   运维误以为控制已启用）；
    /// - 只读构建：段**不得出现**——防止用户误以为控制已启用（fail-fast）。
    ///
    /// 另要求 `rest.listen` 存在且为 **loopback** 地址（§90.2：远程必须
    /// TLS 而 MVP 无原生 TLS——MVP 控制面仅允许 loopback 直连，远程访问
    /// 须经 TLS 反向代理转发；非 loopback 监听 fail-fast 启动失败）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ControlOptions>,
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

/// 控制链路配置（§81/§90；仅 `control` feature 构建下生效）。
///
/// `namespace` 与 `credentials_file` 必填；策略覆盖项给合理默认（与
/// `control_engine::ControlPolicy::default` 对齐），只列运维最常调整的
/// 子集——角色门槛/优先级/幂等保留期等安全默认值不开放配置，避免误配
/// 拉低 §86 基线。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlOptions {
    /// 控制命名空间（§80.1 幂等键三元组之一；必填非空）。不同部署环境
    /// 应使用不同 namespace，避免 Journal 记录跨环境串扰。
    pub namespace: String,
    /// §90.2 静态凭据文件路径（JSON，schema
    /// `forgelink.control.credentials.v1`；Unix 要求 0600 权限）。
    /// 加载失败即启动失败（fail-closed，§90.2）。
    pub credentials_file: PathBuf,
    /// 幂等 Journal 文件路径（JSONL，§80.1/§103）。缺省取
    /// `<buffer.db_path 父目录>/control-journal.jsonl`——与 WAL 同一数据
    /// 目录，崩溃恢复记录与采集数据同生命周期。父目录必须已存在
    /// （与 `buffer.db_path` 约定一致）。
    #[serde(default)]
    pub journal_path: Option<PathBuf>,
    /// REST 提交控制请求的默认超时（毫秒）；引擎按策略上限取较小值，
    /// 默认 5000。
    #[serde(default = "default_control_timeout_ms")]
    pub timeout_ms: u64,
    /// 每设备控制队列容量（§87 有界队列），默认 64。
    #[serde(default = "default_control_queue_capacity")]
    pub queue_capacity: usize,
    /// 单条审计事件写入超时（毫秒），默认 1000（慢审计不得阻塞控制
    /// worker，§90）。
    #[serde(default = "default_control_audit_timeout_ms")]
    pub audit_timeout_ms: u64,
    /// 控制引擎停机宽限（毫秒）：在途请求的结算期限，超时强制中止并按
    /// Indeterminate/Cancelled 结算（收据不永久挂起），默认 5000。
    #[serde(default = "default_control_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
}

fn default_control_timeout_ms() -> u64 {
    5_000
}
fn default_control_queue_capacity() -> usize {
    64
}
fn default_control_audit_timeout_ms() -> u64 {
    1_000
}
fn default_control_shutdown_grace_ms() -> u64 {
    5_000
}

impl ControlOptions {
    // 只读构建下本方法不被调用（段存在即在 validate 中被拒绝），
    // 字段级校验仅在 control 构建生效。
    #[cfg_attr(not(feature = "control"), allow(dead_code))]
    fn validate(&self) -> Result<(), CollectorError> {
        if self.namespace.is_empty() {
            return Err(ConfigError::invalid("control.namespace", "控制命名空间不能为空").into());
        }
        if self.namespace.chars().any(char::is_control) {
            return Err(
                ConfigError::invalid("control.namespace", "控制命名空间不能含控制字符").into(),
            );
        }
        if self.credentials_file.as_os_str().is_empty() {
            return Err(
                ConfigError::invalid("control.credentials_file", "凭据文件路径不能为空").into(),
            );
        }
        // 零值会让对应机制立即失效/永久阻塞（与 ControlPolicy::validate
        // 同一 fail-fast 原则），启动前拒绝而非静默修正。
        if self.timeout_ms == 0 {
            return Err(ConfigError::invalid("control.timeout_ms", "必须大于 0").into());
        }
        if self.queue_capacity == 0 {
            return Err(ConfigError::invalid("control.queue_capacity", "必须大于 0").into());
        }
        if self.audit_timeout_ms == 0 {
            return Err(ConfigError::invalid("control.audit_timeout_ms", "必须大于 0").into());
        }
        if self.shutdown_grace_ms == 0 {
            return Err(ConfigError::invalid("control.shutdown_grace_ms", "必须大于 0").into());
        }
        if let Some(path) = &self.journal_path
            && path.as_os_str().is_empty()
        {
            return Err(
                ConfigError::invalid("control.journal_path", "Journal 路径不能为空").into(),
            );
        }
        Ok(())
    }
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
        // 控制链路（§98/§81）：配置段与构建 feature 必须一致——
        // - control 构建：段出现即启用（字段校验 + 必须启用 REST 且监听
        //   loopback：控制端点经 REST v1 暴露，无 REST 的控制没有提交
        //   入口；远程（非 loopback）必须 TLS 而 MVP 无原生 TLS，§90.2）；
        //   段缺省保持只读采集（运行时启动时告警提示）；
        // - 只读构建：段不得出现（fail-fast，防止用户误以为控制已启用）。
        #[cfg(feature = "control")]
        if let Some(control) = &self.control {
            control.validate()?;
            let listen = self.rest.listen.as_deref().ok_or_else(|| {
                ConfigError::invalid(
                    "rest.listen",
                    "控制链路经 REST v1 暴露，启用 control 时必须配置 rest.listen",
                )
            })?;
            // §90.2：远程（非 loopback）必须 TLS，MVP 无原生 TLS——控制面
            // 仅允许 loopback 直连（IPv4 127.0.0.0/8、IPv6 ::1），fail-fast
            // 启动失败；远程访问须经 TLS 反向代理转发。`listen` 为 IP:port
            // （`SocketAddr` 解析，主机名形式如 `localhost:8080` 在
            // `RestOptions::validate` 已被拒绝）。
            let addr = listen.parse::<std::net::SocketAddr>().map_err(|e| {
                ConfigError::invalid("rest.listen", format!("监听地址 {listen:?} 无法解析: {e}"))
            })?;
            if !addr.ip().is_loopback() {
                return Err(ConfigError::invalid(
                    "rest.listen",
                    format!(
                        "启用 control 时仅允许 loopback 监听（当前 {addr}）：\
                         MVP 控制面仅允许 loopback 直连，远程访问须经 TLS 反向代理转发"
                    ),
                )
                .into());
            }
        }
        #[cfg(not(feature = "control"))]
        if self.control.is_some() {
            return Err(ConfigError::invalid(
                "control",
                "已配置 control 段，但当前为只读构建（未启用 control feature）；\
                 请移除该段或改用 control 构建",
            )
            .into());
        }
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

    /// 最小合法配置（§100 校验全通过；测试按需覆盖单字段）。
    fn minimal_config() -> CollectorConfig {
        CollectorConfig {
            site_id: "plant-a".to_owned(),
            session_id: None,
            profiles_dir: PathBuf::from("profiles"),
            driver: DriverSpec {
                plugin: PathBuf::from("driver.dll"),
                manifest: ManifestSpec {
                    id: "modbus-tcp".to_owned(),
                    ..Default::default()
                },
            },
            devices: vec![DeviceSpec {
                id: "vfd-01".to_owned(),
                name: None,
                domain: None,
                driver: "modbus-tcp".to_owned(),
                profile: "inovance-md500".to_owned(),
                connection: serde_json::json!({ "host": "127.0.0.1", "port": 1502 }),
                enabled: true,
                labels: Default::default(),
            }],
            northbound: NorthboundConfig {
                mqtt: MqttOptions {
                    broker_host: "127.0.0.1".to_owned(),
                    ..Default::default()
                },
            },
            poll: Default::default(),
            pipeline: Default::default(),
            buffer: BufferOptions {
                db_path: PathBuf::from("data/collector-wal.db"),
                ..Default::default()
            },
            forward_poll_ms: 500,
            rest: RestOptions {
                listen: Some("127.0.0.1:18080".to_owned()),
                max_concurrency: 8,
            },
            control: None,
        }
    }

    /// 合法 control 段（凭据/Journal 路径仅作占位，路径存在性由运行时
    /// 装配期校验，配置层只查非空）。
    #[cfg(feature = "control")]
    fn valid_control() -> ControlOptions {
        ControlOptions {
            namespace: "plant-a".to_owned(),
            credentials_file: PathBuf::from("control-credentials.json"),
            journal_path: None,
            timeout_ms: 5_000,
            queue_capacity: 16,
            audit_timeout_ms: 1_000,
            shutdown_grace_ms: 5_000,
        }
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_build_accepts_valid_control_section() {
        let mut config = minimal_config();
        config.control = Some(valid_control());
        config.validate().expect("合法 control 配置应通过");
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_build_without_section_stays_readonly() {
        // 段是控制的启用开关（与 rest.listen 同模式）：缺省保持只读采集，
        // 运行时启动时告警提示（不误导也不阻塞既有只读部署）。
        let config = minimal_config();
        config.validate().expect("缺 control 段应通过（只读运行）");
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_build_requires_rest_listen() {
        // 控制端点经 REST v1 暴露（§31.5）：无 REST 的控制构建没有提交入口。
        let mut config = minimal_config();
        config.rest.listen = None;
        config.control = Some(valid_control());
        let err = config.validate().expect_err("未启用 REST 必须拒绝");
        assert!(
            err.to_string().contains("rest.listen"),
            "错误应指向 rest.listen: {err}"
        );
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_requires_loopback_listen_rejects_non_loopback() {
        // §90.2：远程（非 loopback）必须 TLS，MVP 无原生 TLS——控制面仅
        // 允许 loopback 直连（fail-fast 启动失败），远程访问须经 TLS 反向
        // 代理转发。
        let mut config = minimal_config();
        config.rest.listen = Some("0.0.0.0:8080".to_owned());
        config.control = Some(valid_control());
        let err = config.validate().expect_err("通配地址监听必须拒绝");
        let text = err.to_string();
        assert!(
            text.contains("loopback"),
            "错误应说明仅允许 loopback: {text}"
        );
        assert!(
            text.contains("TLS 反向代理"),
            "错误应说明远程访问方案: {text}"
        );

        // 私网地址同样非 loopback，拒绝。
        let mut config = minimal_config();
        config.rest.listen = Some("192.168.1.10:8080".to_owned());
        config.control = Some(valid_control());
        assert!(config.validate().is_err(), "私网地址同样必须拒绝");
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_allows_loopback_listen_ipv4_and_ipv6() {
        // IPv4 127.0.0.0/8 与 IPv6 ::1 均为 loopback，允许启用控制链路。
        for listen in ["127.0.0.1:8080", "[::1]:8080", "127.9.9.9:8080"] {
            let mut config = minimal_config();
            config.rest.listen = Some(listen.to_owned());
            config.control = Some(valid_control());
            config
                .validate()
                .unwrap_or_else(|e| panic!("{listen} 应通过: {e}"));
        }
    }

    #[cfg(feature = "control")]
    #[test]
    fn control_options_rejects_invalid_fields() {
        let cases: Vec<(&str, ControlOptions)> = vec![
            (
                "control.namespace",
                ControlOptions {
                    namespace: String::new(),
                    ..valid_control()
                },
            ),
            (
                "control.timeout_ms",
                ControlOptions {
                    timeout_ms: 0,
                    ..valid_control()
                },
            ),
            (
                "control.queue_capacity",
                ControlOptions {
                    queue_capacity: 0,
                    ..valid_control()
                },
            ),
            (
                "control.audit_timeout_ms",
                ControlOptions {
                    audit_timeout_ms: 0,
                    ..valid_control()
                },
            ),
            (
                "control.shutdown_grace_ms",
                ControlOptions {
                    shutdown_grace_ms: 0,
                    ..valid_control()
                },
            ),
            (
                "control.credentials_file",
                ControlOptions {
                    credentials_file: PathBuf::new(),
                    ..valid_control()
                },
            ),
        ];
        for (field, options) in cases {
            let err = options.validate().expect_err("非法字段必须拒绝");
            assert!(err.to_string().contains(field), "错误应指向 {field}: {err}");
        }
    }

    #[cfg(not(feature = "control"))]
    #[test]
    fn readonly_build_rejects_control_section() {
        // 只读构建出现 control 段必须报错（fail-fast，防止用户误以为
        // 控制已启用）。
        let mut config = minimal_config();
        config.control = Some(ControlOptions {
            namespace: "plant-a".to_owned(),
            credentials_file: PathBuf::from("control-credentials.json"),
            journal_path: None,
            timeout_ms: 5_000,
            queue_capacity: 16,
            audit_timeout_ms: 1_000,
            shutdown_grace_ms: 5_000,
        });
        let err = config.validate().expect_err("只读构建必须拒绝 control 段");
        assert!(
            err.to_string().contains("只读"),
            "错误应说明当前为只读构建: {err}"
        );
    }

    #[cfg(not(feature = "control"))]
    #[test]
    fn readonly_build_ok_without_control_section() {
        minimal_config()
            .validate()
            .expect("无 control 段的只读配置应通过");
    }
}
