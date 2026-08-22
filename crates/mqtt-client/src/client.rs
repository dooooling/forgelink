//! mqtt-client 核心：MQTT QoS 1 发布客户端（§31 / §34.3 / §90.1）。

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ring::signature;
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, LastWill, MqttOptions, Outgoing, QoS,
    TlsConfiguration, Transport,
};
use rustls::client::ClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{RootCertStore, SignatureAlgorithm, SignatureScheme};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use crate::config::{MqttClientConfig, TlsMode};
use crate::metrics::MqttMetrics;

/// 优雅断开（发送并冲刷 DISCONNECT 报文）时限。
const GRACE_PERIOD: Duration = Duration::from_secs(2);
/// QoS 1 PUBLISH 报文的固定开销上限（§31.1 / §31.2 载荷尺寸校验用）：
/// 固定头 5 字节（含 4 字节剩余长度）+ 主题长度字段 2 字节 + PacketId 2 字节。
const PUBLISH_OVERHEAD_MAX: usize = 5 + 2 + 2;

/// mqtt-client 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttClientError {
    /// 配置非法（`MqttClientConfig::validate` 拒绝，或 TLS 材料解析失败）。
    InvalidConfig { field: &'static str, reason: String },
    /// 主题非法（为空、包含通配符或非法字符）。
    InvalidTopic { topic: String, reason: String },
    /// 载荷序列化失败。
    InvalidPayload { reason: String },
    /// 载荷超过 `max_packet_size`（MQTT 报文上限，入队前拒绝，
    /// 避免超限错误在事件循环中触发、被误判为连接故障）。
    PayloadTooLarge { size: usize, max: usize },
    /// 客户端已关闭（停机后调用，或所有句柄已释放）。
    Closed,
    /// 发布请求未能进入发送队列（rumqttc 内部队列错误）。
    PublishFailed { reason: String },
    /// 包标识碰撞槽被第二次未决碰撞覆盖：本消息已被 rumqttc 丢弃，
    /// 不可能再写出或确认（§31.4）。
    ///
    /// 客户端本身仍正常运行，仅这一条发布失败——调用方应保留对应
    /// WAL 记录并重试补传，不得因此停止整个 MQTT 客户端。
    CollisionOverwritten,
    /// 连续重连失败达到上限，事件循环已退出；此后发布返回 `Closed`。
    Disconnected { reason: String },
    /// 后台任务异常终止（panic）。
    TaskFailed { reason: String },
}

impl fmt::Display for MqttClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(f, "mqtt-client 配置非法（{field}）: {reason}")
            }
            Self::InvalidTopic { topic, reason } => {
                write!(f, "mqtt-client 主题非法（{topic}）: {reason}")
            }
            Self::InvalidPayload { reason } => write!(f, "mqtt-client 载荷非法: {reason}"),
            Self::PayloadTooLarge { size, max } => {
                write!(f, "mqtt-client 载荷 {size} 字节超过报文上限 {max} 字节")
            }
            Self::Closed => write!(f, "mqtt-client 已关闭"),
            Self::PublishFailed { reason } => {
                write!(f, "mqtt-client 发布请求失败: {reason}")
            }
            Self::CollisionOverwritten => {
                write!(f, "mqtt-client 发布请求被包标识碰撞覆盖丢弃（消息未送达）")
            }
            Self::Disconnected { reason } => {
                write!(f, "mqtt-client 重连失败已达到上限: {reason}")
            }
            Self::TaskFailed { reason } => {
                write!(f, "mqtt-client 后台任务异常终止: {reason}")
            }
        }
    }
}

impl MqttClientError {
    /// 稳定错误码（kebab-case，如 `disconnected`）：不携带连接地址、
    /// 主题、原因文本等内部细节，可用于外部响应与健康状态（§90.1
    /// 信息隔离——原始原因仅进日志，经 `diagnostics::redact` 脱敏）。
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig { .. } => "invalid_config",
            Self::InvalidTopic { .. } => "invalid_topic",
            Self::InvalidPayload { .. } => "invalid_payload",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::Closed => "closed",
            Self::PublishFailed { .. } => "publish_failed",
            Self::CollisionOverwritten => "collision_overwritten",
            Self::Disconnected { .. } => "disconnected",
            Self::TaskFailed { .. } => "task_failed",
        }
    }
}

impl std::error::Error for MqttClientError {}

/// 一条待发布的请求（带完成通知，保证发布方能感知入队失败、PUBACK
/// 或停机取消）。
struct PublishRequest {
    topic: String,
    payload: Vec<u8>,
    /// 是否 retained（§31.1：仅 `status` 主题使用）。
    retain: bool,
    /// 是否为在线状态发布（`publish_online`）：worker 按设备记录在线
    /// 状态（`status_ids`），异常断线重连后逐设备重新发布（§31.1 配合
    /// retained LWT；payload 在重发时重新生成，时间戳不失真）。
    online_status: bool,
    /// 在线状态所属设备（`publish_online` / `publish_offline` 时提供；
    /// 普通发布为 `None`）。
    status_ids: Option<(String, String)>,
    /// 是否为设备下线请求（`publish_offline`）：入队（校验通过）时
    /// 立即从在线跟踪中移除该设备——含尚未转发的在线重发条目与待重发
    /// 队列——重连时不得再标记在线（§31.1）。
    deregister: bool,
    /// 请求被 worker 接受（校验通过进入 pending）的时刻：PUBACK resolve
    /// 时据此观测发布往返耗时（§34.2.1 `mqtt_publish_ns_hist`；断线重发
    /// 后确认的耗时同样覆盖——同一 resolve 路径）。
    accepted_at: Instant,
    ack_tx: oneshot::Sender<Result<(), MqttClientError>>,
}

/// 已转发给 rumqttc、等待 PUBACK 确认的发布（§31.4：WAL 记录在
/// broker 确认后才能删除）。
///
/// `pkid` 在收到 `Outgoing::Publish` 事件时填充；`PubAck(pkid)` 到达
/// 后按包标识关联并结算对应请求。
struct AwaitingAck {
    pkid: Option<u16>,
    /// 包标识碰撞时被 rumqttc 停放的消息（`Outgoing::AwaitAck`），
    /// 记录其碰撞标识（§31.4）。
    ///
    /// rumqttc 在包标识回绕撞到未确认槽位时把新消息停放在碰撞槽，待
    /// 旧同标识消息确认后才写出（`state.rs` `outgoing_publish` /
    /// `check_collision`）——停放期间该消息**不在**待发 / 通道队列中，
    /// `assign_pkid` 不得提前分配，否则后续写事件会抢占其位置、PUBACK
    /// 关联错位（§31.4）。解除停放的时机是旧消息确认后紧随其后的
    /// `Outgoing::Publish` 写事件（`unpark_on_publish`）——rumqttc
    /// 处理旧消息 PUBACK 时先入队该写事件、后入队 `Incoming::PubAck`。
    ///
    /// 记录碰撞标识用于**配对解除**：旧碰撞尚未恢复时 rumqttc 的单碰撞
    /// 槽可能被第二次碰撞覆盖（旧碰撞消息永久丢失），此时会同时存在
    /// 多个停放条目——按配对标识解除，绝不触碰其它停放条目（否则会把
    /// 未送达消息误判为成功、WAL 被提前删除，§31.4）。`None` 表示未
    /// 停放。
    parked_pkid: Option<u16>,
    /// 断线前已存在（上一轮 `reset_pkids` 时已在队列中）的标记。
    ///
    /// rumqttc 断线时（`EventLoop::clean`）重发顺序为：上一轮遗留的
    /// 未重发请求（位于待发队列最前）-> 本次会话在途请求（pkid 槽位
    /// 顺序）-> 本次会话新转发的通道请求（最末）。后两类请求的
    /// `pkid = None` 无法从包标识区分，`reset_pkids` 必须借助该标记
    /// 保持重排后与 rumqttc 一致（否则二次断线后 PUBACK 会关联到
    /// 错误的请求，§31.4）。
    leftover: bool,
    /// 请求被 worker 接受的时刻（自 [`PublishRequest`] 转入）：PUBACK
    /// resolve 时观测发布往返耗时（§34.2.1 `mqtt_publish_ns_hist`）。
    accepted_at: Instant,
    ack_tx: oneshot::Sender<Result<(), MqttClientError>>,
}

/// 一次 [`MqttClient::publish`] 的确认句柄：收到 broker 的 PUBACK 后
/// 由 [`PublishReceipt::acked`] 返回 `Ok`（§31.4 中 Local Buffer 以此
/// 为删除 WAL 记录的依据）。
///
/// 未等待（直接丢弃）时与"入队即返回"的 fire-and-forget 等价；
/// 但此时无法感知消息是否确认，需要不丢数据的场景必须等待。
#[derive(Debug)]
pub struct PublishReceipt {
    ack_rx: oneshot::Receiver<Result<(), MqttClientError>>,
}

impl PublishReceipt {
    /// 等待本次发布的 PUBACK 确认。
    ///
    /// - `Ok`：broker 已确认（PUBACK），可安全删除对应 WAL 记录（§31.4）。
    /// - `Err(Disconnected)`：重连失败达到上限，任务退出，未确认。
    /// - `Err(Closed)`：停机取消或客户端已关闭。
    /// - `Err(CollisionOverwritten)`：包标识碰撞槽被第二次碰撞覆盖，
    ///   本条消息未送达（客户端仍正常运行，WAL 记录保留、可重试补传）。
    ///
    /// # Errors
    ///
    /// 见上；确认通道已关闭（任务退出前丢弃）时返回 [`MqttClientError::Closed`]。
    pub async fn acked(self) -> Result<(), MqttClientError> {
        self.ack_rx.await.map_err(|_| MqttClientError::Closed)?
    }
}

/// MQTT 北向客户端（§31）。
///
/// 基于 rumqttc（MQTT 3.1.1）：QoS 1 发布、PUBACK 自动处理与断线重发
/// （§31.3 at-least-once）、自动重连（指数退避，§34.3）、LWT（§31.1）、
/// TLS / mTLS（§90.1）。
///
/// # 语义
///
/// - `publish` 返回 [`PublishReceipt`]：`receipt.acked()` 在收到 broker
///   PUBACK 后返回 `Ok`——即"消息已送达 broker"（§31.4 中 Local Buffer
///   以此删除 WAL 记录）；断线时未确认的 QoS 1 消息在重连后自动重发
///   （rumqttc 0.24 重发时不置 DUP 位，消费者按 `message_id` 去重，§31.3）。
///   请求进入有界队列时 `publish` 即返回（队列上限 `publish_capacity`，
///   满时阻塞——背压沿调用链向上传导，§34.2）。连接中断期间请求仍在
///   有界队列中等待，不会取消在途连接尝试（`connect_timeout` 与退避/
///   重试上限确定生效，§34.3）。
/// - 重连退避默认 1s 起、翻倍、上限 30s，成功连接后重置（§34.3）；
///   `max_reconnect_retries = None` 时无限重试（默认）。
/// - 停机：发送 DISCONNECT 优雅断开（broker 不发布 LWT），等待任务
///   退出；停机期间未确认的发布以 `Closed` 结算（未收到 PUBACK 的
///   消息不得删除 WAL，§31.4）。
/// - 可靠性为内存级（进程退出即丢失）；持久化补传由 Local Buffer / WAL
///   （§31.4）承担。注意：`max_reconnect_retries` 达到上限任务退出时，
///   未确认的发布以 `Disconnected` 结算；已确认（PUBACK）的消息
///   broker 已收到，不会丢失。
///
/// # 用法
///
/// ```ignore
/// let mut config = MqttClientConfig::new("cnc-01", "broker.example.com", 8883);
/// config.tls = TlsMode::ServerAuth { ca_pem };
/// let client = MqttClient::spawn(config)?;
/// let receipt = client
///     .publish("forgelink/v1/telemetry/plant-a/cnc-01", payload)
///     .await?;
/// receipt.acked().await?; // broker 确认后返回（§31.4 删除 WAL 的依据）
/// client.shutdown().await?;
/// ```
#[derive(Debug)]
pub struct MqttClient {
    request_tx: mpsc::Sender<PublishRequest>,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
    // 报文上限：入队前同步校验载荷大小（§34.2 前置校验）。
    max_packet_size: usize,
}

impl MqttClient {
    /// 校验配置并启动客户端；后台任务立即开始连接（断线自动重连）。
    ///
    /// TLS 材料（CA 证书、客户端证书与私钥）在启动时解析并验证——
    /// 损坏的证书、无效私钥或证书与私钥不匹配都会在此返回错误，不会
    /// 进入"spawn 成功、连接无限重试"的状态（§90.1）。
    ///
    /// # Errors
    ///
    /// 配置非法（含 TLS 材料解析失败）时返回
    /// [`MqttClientError::InvalidConfig`]。
    pub fn spawn(config: MqttClientConfig) -> Result<Self, MqttClientError> {
        Self::spawn_inner(config, MqttMetrics::new(None))
    }

    /// 校验配置并启动客户端，注入指标注册表（§34.2.1）：在途 gauge、
    /// 确认 / 重发 / 失败计数经 `registry` 暴露。语义与 [`Self::spawn`]
    /// 完全一致。
    ///
    /// # Errors
    ///
    /// 同 [`Self::spawn`]。
    pub fn spawn_with_metrics(
        config: MqttClientConfig,
        registry: std::sync::Arc<metrics::MetricsRegistry>,
    ) -> Result<Self, MqttClientError> {
        Self::spawn_inner(config, MqttMetrics::new(Some(&registry)))
    }

    fn spawn_inner(
        config: MqttClientConfig,
        mqtt_metrics: MqttMetrics,
    ) -> Result<Self, MqttClientError> {
        config
            .validate()
            .map_err(|reason| MqttClientError::InvalidConfig {
                field: "MqttClientConfig",
                reason,
            })?;

        let mut options = MqttOptions::new(
            config.client_id.clone(),
            config.broker_host.clone(),
            config.broker_port,
        );
        options.set_keep_alive(config.keep_alive);
        options.set_max_packet_size(config.max_packet_size, config.max_packet_size);
        // 在途窗口（pkid 回绕边界，§31.3）：broker 乱序确认时回绕会触发
        // 包标识碰撞（`Outgoing::AwaitAck`），worker 按碰撞契约处理。
        options.set_inflight(config.max_inflight);
        if let Some(username) = &config.username {
            options.set_credentials(
                username.clone(),
                config.password.clone().unwrap_or_default(),
            );
        }
        if let Some(will) = &config.will {
            options.set_last_will(LastWill::new(
                will.topic.clone(),
                will.payload.clone(),
                QoS::AtLeastOnce,
                will.retain,
            ));
        }
        options.set_transport(build_tls_config(&config.tls)?);

        let (client, mut eventloop) = AsyncClient::new(options, config.publish_capacity);
        // 连接超时（rumqttc 以秒为单位）。
        eventloop
            .network_options
            .set_connection_timeout(config.connect_timeout.as_secs());

        let (request_tx, request_rx) = mpsc::channel(config.publish_capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // 先取出报文上限（config 随后整体移入 worker）。
        let max_packet_size = config.max_packet_size;

        let task = tokio::spawn(run_worker(
            config,
            client,
            eventloop,
            request_rx,
            shutdown_rx,
            mqtt_metrics,
        ));
        Ok(Self {
            request_tx,
            shutdown_tx,
            task,
            max_packet_size,
        })
    }

    /// 以 QoS 1、不 retain 发布一条消息（§31.1 Telemetry 约定）。
    ///
    /// # 返回语义
    ///
    /// 返回 [`PublishReceipt`]：`acked()` 在收到 broker PUBACK 后返回
    /// `Ok`（§31.4：Local Buffer 以"确认"为删除 WAL 记录的依据）。
    /// 请求进入有界队列时 `publish` 即返回（队列满时阻塞，背压传导，
    /// §34.2）；断线时未确认的消息在重连后自动重发（§31.3），期间
    /// `acked()` 保持等待。停机或被重连上限终止时，未确认的发布以
    /// `Closed` / `Disconnected` 结算——调用方必须保留 WAL 记录。
    ///
    /// # Errors
    ///
    /// - 主题非法（空、通配符、控制字符或超长）：[`MqttClientError::InvalidTopic`]。
    /// - 载荷超过报文上限（`max_packet_size`）：
    ///   [`MqttClientError::PayloadTooLarge`]（入队前拒绝，不会在事件
    ///   循环中被误判为连接故障）。
    /// - 客户端已关闭：[`MqttClientError::Closed`]。
    /// - 重连失败达到上限后任务已退出：[`MqttClientError::Closed`]
    ///   （此前的未确认发布经 `acked()` 返回 `Disconnected`）。
    pub async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<PublishReceipt, MqttClientError> {
        self.publish_inner(topic, payload.into(), false, false, None, false)
            .await
    }

    /// 以 QoS 1、retain 发布一条消息（§31.1 `status` 在线状态约定：
    /// 设备上线时发布 retained 在线状态，配合 retained LWT 离线状态
    /// 表示设备在线 / 离线）。
    ///
    /// 返回语义与 [`MqttClient::publish`] 相同。
    ///
    /// # Errors
    ///
    /// 同 [`MqttClient::publish`]。
    pub async fn publish_retained(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<PublishReceipt, MqttClientError> {
        self.publish_inner(topic, payload.into(), true, false, None, false)
            .await
    }

    async fn publish_inner(
        &self,
        topic: &str,
        payload: Vec<u8>,
        retain: bool,
        online_status: bool,
        status_ids: Option<(String, String)>,
        deregister: bool,
    ) -> Result<PublishReceipt, MqttClientError> {
        // 入队前同步校验：非法主题 / 超限载荷不进入队列，直接拒绝
        //（§34.2 前置校验；rumqttc 对非法主题与队列满都返回
        // `ClientError::Request`，无法区分，见 `accept_request` 说明）。
        validate_publish_topic(topic)?;
        validate_payload_size(topic, &payload, self.max_packet_size)?;
        let (ack_tx, ack_rx) = oneshot::channel();
        let request = PublishRequest {
            topic: topic.to_owned(),
            payload,
            retain,
            online_status,
            status_ids,
            deregister,
            // 起始时刻取构造点（入队前）：PUBACK resolve 时观测完整
            // 往返（§34.2.1 `mqtt_publish_ns_hist`）。
            accepted_at: Instant::now(),
            ack_tx,
        };
        self.request_tx
            .send(request)
            .await
            .map_err(|_| MqttClientError::Closed)?;
        Ok(PublishReceipt { ack_rx })
    }

    /// 发布一个 Telemetry Batch（§31.1 / §31.2）。
    ///
    /// 主题为 `forgelink/v1/telemetry/{site_id}/{device_id}`，载荷为
    /// 批次 JSON 序列化结果；QoS 1、不 retain。批次由 data-pipeline
    /// 组包（§31.2）。
    ///
    /// # Errors
    ///
    /// `site_id` / `device_id` 为空或包含 `/` 时返回
    /// [`MqttClientError::InvalidTopic`]；序列化失败返回
    /// [`MqttClientError::InvalidPayload`]；其余同
    /// [`MqttClient::publish`]。
    pub async fn publish_batch(
        &self,
        batch: &crate::ObservationBatch,
    ) -> Result<PublishReceipt, MqttClientError> {
        let topic = telemetry_topic(&batch.site_id, &batch.device_id)?;
        let payload = serde_json::to_vec(batch).map_err(|e| MqttClientError::InvalidPayload {
            reason: e.to_string(),
        })?;
        self.publish(&topic, payload).await
    }

    /// 发布 §31.1 设备在线状态（retained，配合 retained LWT 离线状态）。
    ///
    /// 主题为 `forgelink/v1/status/{site_id}/{device_id}`，QoS 1、retain；
    /// 载荷为 Status Envelope（§32：所有消息必须显式携带 schema/version），
    /// 与 [`WillConfig::offline_status`](crate::WillConfig::offline_status)
    /// 生成的离线状态使用同一 Envelope。Collector 启动完成并成功连接后
    /// 调用，通知订阅方设备在线（§31.1）。
    ///
    /// # Errors
    ///
    /// 同 [`MqttClient::publish_retained`]。
    pub async fn publish_online(
        &self,
        site_id: &str,
        device_id: &str,
    ) -> Result<PublishReceipt, MqttClientError> {
        let topic = status_topic(site_id, device_id)?;
        let payload = status_envelope(site_id, device_id, "online");
        self.publish_inner(
            &topic,
            payload,
            true,
            true,
            Some((site_id.to_owned(), device_id.to_owned())),
            false,
        )
        .await
    }

    /// 发布 retained 离线状态并将设备从在线跟踪中移除（§31.1）。
    ///
    /// 主题与载荷同 [`MqttClient::publish_online`]（`status = offline`，
    /// `sent_at_ns` 为发布时刻）。设备下线（删除 / 停用）时调用：
    /// worker 立即停止该设备的在线跟踪——重连时不再重新标记在线，已
    /// 入队的在线重发与待重发条目一并清除——避免删除的设备在重连后
    /// 重新"上线"。对未跟踪设备调用同样发布离线状态（幂等）。
    ///
    /// 注意：异常断线（进程崩溃 / 网络不可达）时无法主动发布离线，
    /// 该场景由 LWT（单设备）与消费端在线时间判断兜底（§31.1 契约）。
    ///
    /// # Errors
    ///
    /// 同 [`MqttClient::publish`]。
    pub async fn publish_offline(
        &self,
        site_id: &str,
        device_id: &str,
    ) -> Result<PublishReceipt, MqttClientError> {
        let topic = status_topic(site_id, device_id)?;
        let payload = status_envelope(site_id, device_id, "offline");
        self.publish_inner(
            &topic,
            payload,
            true,
            false,
            Some((site_id.to_owned(), device_id.to_owned())),
            true,
        )
        .await
    }

    /// 优雅停机：发送 DISCONNECT（broker 不发布 LWT），等待后台任务
    /// 退出并结算未入队请求。
    ///
    /// 停机后调用 [`MqttClient::publish`] 返回 `Closed`。
    ///
    /// # Errors
    ///
    /// 后台任务异常终止时返回 [`MqttClientError::TaskFailed`]。
    pub async fn shutdown(self) -> Result<(), MqttClientError> {
        let _ = self.shutdown_tx.send(true);
        match self.task.await {
            Ok(()) => {
                info!(component = "mqtt-client", "MQTT 客户端已停机");
                Ok(())
            }
            Err(e) => Err(MqttClientError::TaskFailed {
                reason: e.to_string(),
            }),
        }
    }
}

/// 生成 Telemetry 主题（§31.1）：`forgelink/v1/telemetry/{site_id}/{device_id}`。
///
/// # Errors
///
/// `site_id` / `device_id` 为空或包含 `/`（会改变固定 Topic 层级，
/// 导致订阅与 ACL 路由错误）时返回 [`MqttClientError::InvalidTopic`]。
pub fn telemetry_topic(site_id: &str, device_id: &str) -> Result<String, MqttClientError> {
    let topic = format!("forgelink/v1/telemetry/{site_id}/{device_id}");
    validate_path_segment("site_id", site_id, &topic)?;
    validate_path_segment("device_id", device_id, &topic)?;
    validate_publish_topic(&topic)?;
    Ok(topic)
}

/// 生成 Status 主题（§31.1）：`forgelink/v1/status/{site_id}/{device_id}`
/// （QoS 1、retain，配合 retained LWT 表示在线状态）。
///
/// # Errors
///
/// 同 [`telemetry_topic`]。
pub fn status_topic(site_id: &str, device_id: &str) -> Result<String, MqttClientError> {
    let topic = format!("forgelink/v1/status/{site_id}/{device_id}");
    validate_path_segment("site_id", site_id, &topic)?;
    validate_path_segment("device_id", device_id, &topic)?;
    validate_publish_topic(&topic)?;
    Ok(topic)
}

/// Status 消息 Schema（§32：所有消息必须显式携带 schema/version）。
pub const STATUS_SCHEMA: &str = "forgelink.status.v1";

/// 构建 §31.1 在线 / 离线状态 Envelope（§32 显式 schema/version 要求）：
///
/// ```json
/// {"schema":"forgelink.status.v1","site_id":"plant-a","device_id":"cnc-01",
///  "status":"online","sent_at_ns":1780000000000000000}
/// ```
///
/// 在线状态（`publish_online`）与离线状态（LWT，
/// [`WillConfig::offline_status`](crate::WillConfig::offline_status)）
/// 使用同一 Envelope，消费者无需区分消息来源。
pub(crate) fn status_envelope(site_id: &str, device_id: &str, status: &str) -> Vec<u8> {
    // 时钟异常（早于 UNIX_EPOCH）时退化为 0，不阻塞状态发布。
    let sent_at_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    serde_json::json!({
        "schema": STATUS_SCHEMA,
        "site_id": site_id,
        "device_id": device_id,
        "status": status,
        "sent_at_ns": sent_at_ns,
    })
    .to_string()
    .into_bytes()
}

/// 构建 §31.1 离线状态 LWT 载荷（供
/// [`WillConfig::offline_status`](crate::WillConfig::offline_status) 使用）。
///
/// 与在线状态同一 Envelope，但 `sent_at_ns` 固定为 `0`：MQTT 3.1.1 的
/// Will 载荷在 CONNECT 时由客户端指定、由 broker 在断连时按原样发布，
/// 客户端无法预知真实发布时间——配置创建时刻的时间戳会严重失真
///（配置可能生效数月后才断连）。`sent_at_ns = 0` 表示"未知"，消费者
/// 必须以消息到达时间（broker 侧）作为离线发生时间（§31.1 契约）。
pub(crate) fn lwt_offline_envelope(site_id: &str, device_id: &str) -> Vec<u8> {
    serde_json::json!({
        "schema": STATUS_SCHEMA,
        "site_id": site_id,
        "device_id": device_id,
        "status": "offline",
        "sent_at_ns": 0,
    })
    .to_string()
    .into_bytes()
}

/// 校验主题路径段（`site_id` / `device_id`）：不能为空、不能包含 `/`
///（避免改变固定 Topic 层级，§31.1）。
fn validate_path_segment(field: &str, value: &str, topic: &str) -> Result<(), MqttClientError> {
    if value.is_empty() {
        return Err(MqttClientError::InvalidTopic {
            topic: topic.to_owned(),
            reason: format!("{field} 不能为空"),
        });
    }
    if value.contains('/') {
        return Err(MqttClientError::InvalidTopic {
            topic: topic.to_owned(),
            reason: format!("{field} 不能包含 '/'（主题路径分隔符，§31.1）"),
        });
    }
    Ok(())
}

/// 校验发布主题合法性（MQTT 3.1.1 规范 §4.7.3）。
///
/// 发布主题禁止通配符（`#` / `+`）、空主题、超长主题（> 65535 字节）
/// 与控制字符；在调用 rumqttc 之前本地校验，避免把"非法主题"与
/// "内部队列已满"（rumqttc 均返回 `ClientError::Request`）混为一谈。
/// LWT 主题同样适用（MQTT 3.1.1 §3.1.3.2：Will Topic 禁止通配符），
/// 由 [`MqttClientConfig::validate`](crate::MqttClientConfig::validate) 调用。
pub(crate) fn validate_publish_topic(topic: &str) -> Result<(), MqttClientError> {
    let reason = if topic.is_empty() {
        "发布主题不能为空".to_owned()
    } else if topic.len() > 65535 {
        "发布主题超过 65535 字节上限".to_owned()
    } else if topic.contains(['#', '+']) {
        "发布主题不允许通配符（# / +）".to_owned()
    } else if topic
        .chars()
        .any(|c| (c as u32) <= 0x1F || (c as u32) == 0x7F)
    {
        "发布主题不允许包含控制字符".to_owned()
    } else {
        return Ok(());
    };
    Err(MqttClientError::InvalidTopic {
        topic: topic.to_owned(),
        reason,
    })
}

/// 校验载荷不超过报文上限（`max_packet_size`），在入队前拒绝。
///
/// QoS 1 PUBLISH 报文 = 固定头（最多 5 字节）+ 主题长度（2 字节）+
/// 主题 + PacketId（2 字节）+ 载荷；按最坏情况计算上界，与 rumqttc
/// 事件循环中的 `OutgoingPacketTooLarge` 判定一致。在本地提前拒绝，
/// 避免超限错误在事件循环触发后被误判为连接故障而触发重连（消息
/// 已应答、静默丢失）。
fn validate_payload_size(
    topic: &str,
    payload: &[u8],
    max_packet_size: usize,
) -> Result<(), MqttClientError> {
    let size = PUBLISH_OVERHEAD_MAX + topic.len() + payload.len();
    if size > max_packet_size {
        return Err(MqttClientError::PayloadTooLarge {
            size,
            max: max_packet_size,
        });
    }
    Ok(())
}

/// 把 `TlsMode` 构建为 rumqttc 传输配置（§90.1），并在启动时解析验证
/// TLS 材料：损坏的证书、无效私钥、证书与私钥不匹配都在 `spawn` 阶段
/// 返回 [`MqttClientError::InvalidConfig`]，避免"spawn 成功、连接无限
/// 重试"的永久配置错误。
fn build_tls_config(tls: &TlsMode) -> Result<Transport, MqttClientError> {
    match tls {
        TlsMode::None => Ok(Transport::tcp()),
        TlsMode::ServerAuth { ca_pem } => {
            let roots = load_root_certs(ca_pem)?;
            let config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            Ok(Transport::Tls(TlsConfiguration::Rustls(Arc::new(config))))
        }
        TlsMode::MutualTls {
            ca_pem,
            client_cert_pem,
            client_key_pem,
        } => {
            let roots = load_root_certs(ca_pem)?;
            let certs = parse_cert_chain(client_cert_pem, "tls.client_cert_pem")?;
            let key = parse_private_key(client_key_pem)?;
            verify_key_matches_cert(&certs[0], client_key_pem)?;
            let config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|e| MqttClientError::InvalidConfig {
                    field: "tls.client_cert_pem / client_key_pem",
                    reason: e.to_string(),
                })?;
            Ok(Transport::Tls(TlsConfiguration::Rustls(Arc::new(config))))
        }
    }
}

/// 解析 CA PEM 并加载为受信根证书（rustls 校验服务器身份，§90.1）。
fn load_root_certs(ca_pem: &[u8]) -> Result<RootCertStore, MqttClientError> {
    let mut roots = RootCertStore::empty();
    for cert in parse_cert_chain(ca_pem, "tls.ca_pem")? {
        roots
            .add(cert)
            .map_err(|e| MqttClientError::InvalidConfig {
                field: "tls.ca_pem",
                reason: e.to_string(),
            })?;
    }
    Ok(roots)
}

/// 解析 PEM 证书链（至少一张证书）。
fn parse_cert_chain(
    pem: &[u8],
    field: &'static str,
) -> Result<Vec<CertificateDer<'static>>, MqttClientError> {
    let mut reader = std::io::BufReader::new(pem);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    let certs = certs.map_err(|e| MqttClientError::InvalidConfig {
        field,
        reason: format!("PEM 证书解析失败: {e}"),
    })?;
    if certs.is_empty() {
        return Err(MqttClientError::InvalidConfig {
            field,
            reason: "没有找到任何证书".to_owned(),
        });
    }
    Ok(certs)
}

/// 解析 PEM 私钥（PKCS#8 / PKCS#1 / SEC1 均可）。
fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, MqttClientError> {
    let mut reader = std::io::BufReader::new(pem);
    match rustls_pemfile::private_key(&mut reader) {
        Ok(Some(key)) => Ok(key),
        Ok(None) => Err(MqttClientError::InvalidConfig {
            field: "tls.client_key_pem",
            reason: "没有找到任何私钥".to_owned(),
        }),
        Err(e) => Err(MqttClientError::InvalidConfig {
            field: "tls.client_key_pem",
            reason: format!("私钥解析失败: {e}"),
        }),
    }
}

/// 验证私钥与证书匹配（§90.1）：用私钥对固定消息签名，再用证书的
/// 公钥（SPKI）验签；任何算法（RSA / ECDSA / Ed25519）均适用。
/// 不匹配的密钥对会在 TLS 握手时被服务端拒绝，必须启动时拦截。
/// 私钥在此独立解析一份副本用于自测，不消耗传给 rustls 的那份。
fn verify_key_matches_cert(
    cert: &CertificateDer<'static>,
    client_key_pem: &[u8],
) -> Result<(), MqttClientError> {
    let key = parse_private_key(client_key_pem)?;
    let provider = rustls::crypto::ring::default_provider();
    let signing_key = provider.key_provider.load_private_key(key).map_err(|e| {
        MqttClientError::InvalidConfig {
            field: "tls.client_key_pem",
            reason: format!("私钥无效: {e}"),
        }
    })?;
    // 证书公钥（SPKI），用于验签。crate 为 rustls-webpki，[lib] 名 webpki。
    let spki = webpki::EndEntityCert::try_from(cert)
        .map_err(|e| MqttClientError::InvalidConfig {
            field: "tls.client_cert_pem",
            reason: format!("证书无效: {e}"),
        })?
        .subject_public_key_info()
        .as_ref()
        .to_owned();
    // ring 验签所需的公钥格式与 SPKI 不同：ECDSA 为裸非压缩点、
    // RSA 为 PKCS#1 RSAPublicKey（SEQUENCE{n, e}）、Ed25519 为 32
    // 字节裸公钥——恰好都是 SPKI 中 BIT STRING 的载荷，统一提取。
    let public_key = extract_spki_key(&spki).ok_or_else(|| MqttClientError::InvalidConfig {
        field: "tls.client_cert_pem",
        reason: "证书无效: 无法从 SPKI 提取公钥".to_owned(),
    })?;

    const MESSAGE: &[u8] = b"forgelink tls key match check";
    // 按密钥类型选择签名方案（ECDSA 先试 P-256，不支持时回落 P-384）。
    let scheme = match signing_key.algorithm() {
        SignatureAlgorithm::RSA => SignatureScheme::RSA_PKCS1_SHA256,
        SignatureAlgorithm::ECDSA => SignatureScheme::ECDSA_NISTP256_SHA256,
        SignatureAlgorithm::ED25519 => SignatureScheme::ED25519,
        other => {
            return Err(MqttClientError::InvalidConfig {
                field: "tls.client_key_pem",
                reason: format!("不支持的密钥类型: {other:?}"),
            });
        }
    };
    let signer = signing_key
        .choose_scheme(&[scheme, SignatureScheme::ECDSA_NISTP384_SHA384])
        .ok_or_else(|| MqttClientError::InvalidConfig {
            field: "tls.client_key_pem",
            reason: "私钥无效: 不支持的签名方案".to_owned(),
        })?;
    let signature = signer
        .sign(MESSAGE)
        .map_err(|e| MqttClientError::InvalidConfig {
            field: "tls.client_key_pem",
            reason: format!("私钥无效: {e}"),
        })?;

    // 按签名方案映射到 ring 验签算法（与签名方案一致，含哈希）。
    let verification_algorithm: &dyn signature::VerificationAlgorithm = match signer.scheme() {
        SignatureScheme::RSA_PKCS1_SHA256 => &signature::RSA_PKCS1_2048_8192_SHA256,
        SignatureScheme::RSA_PKCS1_SHA384 => &signature::RSA_PKCS1_2048_8192_SHA384,
        SignatureScheme::RSA_PKCS1_SHA512 => &signature::RSA_PKCS1_2048_8192_SHA512,
        SignatureScheme::ECDSA_NISTP256_SHA256 => &signature::ECDSA_P256_SHA256_ASN1,
        SignatureScheme::ECDSA_NISTP384_SHA384 => &signature::ECDSA_P384_SHA384_ASN1,
        SignatureScheme::ED25519 => &signature::ED25519,
        other => {
            return Err(MqttClientError::InvalidConfig {
                field: "tls.client_key_pem",
                reason: format!("不支持的签名方案: {other:?}"),
            });
        }
    };
    signature::UnparsedPublicKey::new(verification_algorithm, &public_key)
        .verify(MESSAGE, &signature)
        .map_err(|_| MqttClientError::InvalidConfig {
            field: "tls.client_key_pem",
            reason: "私钥与客户端证书不匹配".to_owned(),
        })
}

/// 从 SPKI（SEQUENCE { SEQUENCE { OID… }, BIT STRING { key } }）提取
/// BIT STRING 载荷（首个字节为未用位计数，其后为公钥主体）。
///
/// rustls-webpki 的 `subject_public_key_info()` 返回完整 SPKI；ring 的
/// `UnparsedPublicKey::verify` 需要的是裸公钥格式（ECDSA 非压缩点 /
/// RSA PKCS#1 / Ed25519 裸公钥），即 SPKI 中 BIT STRING 的载荷。
/// 仅做最小 DER 遍历：跳过算法 SEQUENCE 后取 BIT STRING 内容。
/// 长度字段支持 DER 短/长两种格式（`read_der_len` 返回长度字段之后的
/// 字节，避免固定偏移——RSA 2048 等大 SPKI 使用多字节长度字段）。
fn extract_spki_key(spki: &[u8]) -> Option<Vec<u8>> {
    // 外层 SPKI SEQUENCE。
    let (len, rest) = read_der_len(spki.get(1..)?)?;
    let content = rest.get(..len)?;
    // 算法 SEQUENCE（OID 等），整体跳过。
    let (alg_len, rest) = read_der_len(content.get(1..)?)?;
    let after_alg = rest.get(alg_len..)?;
    // BIT STRING：载荷 = 未用位计数 + 公钥。
    let (bit_len, rest) = read_der_len(after_alg.get(1..)?)?;
    Some(rest.get(..bit_len)?.get(1..)?.to_vec())
}

/// 读 DER 长度（短/长格式，最多 2 字节），返回 (长度, 长度字段之后)。
fn read_der_len(input: &[u8]) -> Option<(usize, &[u8])> {
    let first = *input.first()?;
    if first < 0x80 {
        return Some((first as usize, &input[1..]));
    }
    let num_bytes = (first & 0x7F) as usize;
    if num_bytes == 0 || num_bytes > 2 || input.len() < 1 + num_bytes {
        return None;
    }
    let mut len = 0usize;
    for b in &input[1..1 + num_bytes] {
        len = len.checked_mul(256)? + *b as usize;
    }
    Some((len, &input[1 + num_bytes..]))
}

/// 重连退避延迟：`min × 2^(失败次数-1)`，上限 `max`（§34.3）。
fn backoff_delay(min: Duration, max: Duration, failures: u32) -> Duration {
    // failures 从 1 起；指数以 u32 计算（Duration::saturating_mul 的因子
    // 为 u32），超过 31 次失败按 2^31 的倍数饱和到 `max`。
    let exponent = (failures.saturating_sub(1)).min(31);
    let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
    min.saturating_mul(multiplier).min(max)
}

/// 后台任务：驱动 rumqttc 事件循环（连接 / PUBACK / 保活 / 重连退避），
/// 串行处理发布请求（FIFO，天然保持单连接内顺序，§31.3）。
///
/// # 结构（避免在途 connect 被取消）
///
/// 请求不会在 `select!` 中与 `eventloop.poll()` 竞争：未连接时仅用
/// `try_recv` 非阻塞接收（存入有界 `pending` 队列），已连接时 `recv`
/// 分支也只在 `pending` 有空位时启用。这样任何请求处理都不取消在途
/// connect，`connect_timeout` 与退避/重试上限（§34.3）才能确定生效。
/// `pending` 转发使用 `try_publish`（非阻塞），转发失败（rumqttc
/// 通道满）时保留在 `pending` 下轮重试。
///
/// # 确认语义（§31.4）
///
/// 请求经 `accept_request` 校验（主题 + 载荷上限）后进入 `pending`，
/// `publish` 即返回 [`PublishReceipt`]；`forward_pending` 把请求送入
/// rumqttc 后转入 `awaiting_ack`。收到 `Outgoing::Publish(pkid)` 时
/// 为最老的未关联条目填充包标识；收到 `Incoming::PubAck(pkid)` 时按
/// 包标识结算对应条目（`acked()` 返回 `Ok`）。断线重连时 rumqttc
/// 以新包标识重发未确认消息，worker 按 `clean()` 的重发顺序重排
/// `awaiting_ack` 并清空包标识，保证重发后的 PUBACK 仍关联到原请求。
/// 重连失败达到上限或停机时，`pending` / `awaiting_ack` 与通道内请求
/// 全部结算为失败（`Disconnected` / `Closed`）——未确认的消息不会
/// 被误报为成功，调用方可保留 WAL 记录（§31.4）。
async fn run_worker(
    config: MqttClientConfig,
    client: AsyncClient,
    mut eventloop: EventLoop,
    mut request_rx: mpsc::Receiver<PublishRequest>,
    mut shutdown_rx: watch::Receiver<bool>,
    mqtt_metrics: MqttMetrics,
) {
    // 当前是否处于已连接状态（决定停机时是否发送 DISCONNECT，以及
    // select 的 recv 分支与 pending 转发是否启用）。
    let mut connected = false;
    // 连续连接失败次数（成功连接后重置）。
    let mut connect_failures: u32 = 0;
    // 退避等待截止时刻（`Some` 期间不驱动事件循环）。
    let mut backoff_until: Option<Instant> = None;
    // 已接受但尚未转发给 rumqttc 的请求（有界：不超过 publish_capacity）。
    let mut pending: VecDeque<PublishRequest> = VecDeque::new();
    // 已转发、等待 PUBACK 的请求（FIFO；`pkid` 关联见模块文档）。
    let mut awaiting_ack: VecDeque<AwaitingAck> = VecDeque::new();
    // 最近一次收到 PUBACK 的包标识（断线重排用，对齐 rumqttc 内部
    // `last_puback`：任何 PUBACK 都会更新它）。
    let mut last_puback: u16 = 0;
    // 包标识碰撞状态（§31.4）：`Outgoing::AwaitAck` 记录的碰撞标识，
    // 旧同标识消息确认后的写事件上清除（`unpark_on_publish`）。跨断线
    // 存活（rumqttc `clean()` 不清除碰撞槽），重连后由
    // `collision_reset_pending` 区分"重发写事件"与"碰撞恢复写事件"。
    let mut collision_pkid: Option<u16> = None;
    // 旧同标识消息已确认（`Incoming::PubAck(pkid)`）：rumqttc 处理
    // 确认时把碰撞消息以同一 pkid 恢复写出，其 `Outgoing::Publish`
    // 事件随后入队——该事件才是"碰撞恢复写"，解除停放发生在它上面。
    let mut collision_recovered = false;
    // 碰撞未决期间断线（`reset_pkids` 后置位）：rumqttc 重发保留原
    // pkid，重连后的首个同标识写事件是旧消息的**重发**而非碰撞恢复
    // 写——不得提前解除停放（否则碰撞消息可能被后续写事件抢占标识，
    // PUBACK 关联错位、WAL 提前删除未确认记录，§31.4）。由
    // `on_publish_event` 在首个同标识重发事件上消费。
    let mut collision_reset_pending = false;
    // 已成功转发的在线状态设备集合（site_id, device_id，有序）：异常
    // 断线后 broker 已发布 retained 离线 LWT，重连成功后逐设备重新发布
    // 在线状态（§31.1）。一个 MQTT 客户端可承载多个设备，必须按设备
    // 记录；LWT 只有一个，只能覆盖一个设备（见 §31.1 契约）。
    let mut last_online: BTreeSet<(String, String)> = BTreeSet::new();
    // 待重发在线状态的设备队列：断线时从 `last_online` 快照填充（仅当
    // 队列为空，避免打断未完成的重发周期），重试时从队首推进——设备
    // 数超过容量时不会每次从头遍历（否则尾部设备永久遗漏，§31.1）。
    let mut pending_online_republish: VecDeque<(String, String)> = VecDeque::new();
    // 连接中断标记：重连后重新发布在线状态（pending 有空位即重试，
    // 不依赖下一次 ConnAck，§31.1）。
    let mut needs_online_republish = false;
    // 转发给 rumqttc 的最大未完成量（pending 队列上限，§31.2 有界背压）。
    let capacity = config.publish_capacity;

    loop {
        // 退避期间不驱动事件循环，但持续响应停机信号。
        if let Some(until) = backoff_until.take() {
            let wait = until.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = shutdown_rx.changed() => break,
            }
        }

        // 在线状态重发优先于普通请求（§31.1）：先于接收阶段执行，保证
        // 断线重连后即使持续业务流量占满 pending，重发也能获得空位
        //（普通请求可等待，重发受断线窗口限制，不得被饿死）。重发
        // 进度保存在 `pending_online_republish`（队首弹出，不重复、
        // 不遗漏）；pending 满时下轮继续，同一连接内即可恢复，不依赖
        // 下一次 ConnAck（否则当前连接恢复后设备仍显示离线）。
        if needs_online_republish && connected {
            step_online_republish(
                &mut pending_online_republish,
                &mut pending,
                capacity,
                &mut needs_online_republish,
            );
        }

        // 接收阶段：已连接时把通道内请求转入 pending（有界 staging，
        // 校验 + 等待转发）。未连接时不转移——请求留在有界通道内，
        // 通道即背压点：发送方在通道满时阻塞（§31.2 / §34.2，总未完成
        // 量不超过 publish_capacity）。用 try_recv 不阻塞、不取消在途
        // connect；已连接且有空位时由下方 select 的 recv 分支接收。
        if connected {
            while pending.len() < capacity {
                match request_rx.try_recv() {
                    Ok(req) => {
                        accept_request(
                            &mut pending,
                            req,
                            config.max_packet_size,
                            &mut last_online,
                            &mut pending_online_republish,
                            &mqtt_metrics,
                        );
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break, // 无调用方
                }
            }
        }

        // 转发阶段：已连接时把 pending 送入 rumqttc 待发队列（非阻塞）。
        if connected {
            forward_pending(
                &client,
                &mut pending,
                &mut awaiting_ack,
                &mut last_online,
                None,
                &mqtt_metrics,
            );
        }

        tokio::select! {
            req = request_rx.recv(), if connected && pending.len() < capacity => {
                match req {
                    Some(req) => {
                        accept_request(
                            &mut pending,
                            req,
                            config.max_packet_size,
                            &mut last_online,
                            &mut pending_online_republish,
                            &mqtt_metrics,
                        );
                    }
                    None => break, // 所有句柄已释放，无调用方
                }
            }
            ev = eventloop.poll() => {
                match ev {
                    Ok(ev) => {
                        // 事件循环存活即视为连接可用；成功（重）连接后
                        // 重置退避与失败计数（§34.3）。
                        connected = true;
                        connect_failures = 0;
                        match ev {
                            Event::Incoming(Incoming::ConnAck(_)) => {
                                info!(
                                    component = "mqtt-client",
                                    broker_host = %config.broker_host,
                                    broker_port = config.broker_port,
                                    "MQTT broker 连接已建立"
                                );
                            }
                            // 报文已写出（含重连后的重发与碰撞解除后的
                            // 写出）：区分"碰撞恢复写"与"重发写"（§31.4，
                            // rumqttc 处理旧消息 PUBACK 时**先**入队
                            // `Outgoing::Publish`、**后**入队
                            // `Incoming::PubAck`，解除停放必须发生在
                            // 写事件上，否则碰撞消息会被下一个写事件
                            // 抢占标识、永久无法确认）。
                            Event::Outgoing(Outgoing::Publish(pkid)) => {
                                on_publish_event(
                                    &mut awaiting_ack,
                                    &mut collision_pkid,
                                    &mut collision_recovered,
                                    &mut collision_reset_pending,
                                    pkid,
                                );
                            }
                            // 包标识碰撞（§31.4）：rumqttc 把本应使用该
                            // 标识的消息停放在碰撞槽，旧同标识消息确认后
                            // 才写出。停放条目不得提前分配标识。
                            Event::Outgoing(Outgoing::AwaitAck(pkid)) => {
                                on_await_ack_event(
                                    &mut awaiting_ack,
                                        &mut collision_pkid,
                                        &mut collision_recovered,
                                    pkid,
                                        &mqtt_metrics,
                                );
                            }
                            // broker 确认：按包标识结算对应请求（§31.4）。
                            // 碰撞解除（`unpark_on_publish`）发生在紧随其
                            // 后的写事件上，此处只结算——rumqttc 的事件
                            // 顺序保证该 PUBACK 属于旧同标识消息。
                            Event::Incoming(Incoming::PubAck(puback)) => {
                                last_puback = puback.pkid;
                                on_puback_event(
                                    &mut awaiting_ack,
                                        &mut collision_pkid,
                                        &mut collision_recovered,
                                    puback.pkid,
                                        &mqtt_metrics,
                                );
                            }
                            _ => trace!(component = "mqtt-client", "MQTT 事件: {ev:?}"),
                        }
                    }
                    Err(e) => {
                        connected = false;
                        connect_failures += 1;
                        // 断线重发计数（§31.3 / §34.2.1）：存在未确认在途
                        // 消息即计一次重发窗口（重连后由 rumqttc 重发）。
                        if !awaiting_ack.is_empty() {
                            mqtt_metrics.observe_disconnect_with_unacked();
                        }
                        // 断线：未确认消息在重连后由 rumqttc 重发（新包
                        // 标识）；按 clean() 的重发顺序重排并清空包标识，
                        // 使重发后的 PUBACK 仍关联到原请求。同时标记：
                        // 重连后需重新发布在线状态（broker 每次断连都会
                        // 为 Will 设备发布离线 LWT，§31.1）。
                        reset_pkids(&mut awaiting_ack, last_puback);
                        // 碰撞未决时断线（§31.4）：rumqttc 的碰撞槽不被
                        // `clean()` 清除，重连后旧同标识消息以原 pkid 重发
                        // ——其写事件不是碰撞恢复写，不得解除停放（否则
                        // 碰撞消息可能被后续写事件抢占标识、PUBACK 关联
                        // 错位、WAL 提前删除未确认记录）。由
                        // `on_publish_event` 在首个同标识重发事件上消费。
                        if collision_pkid.is_some() {
                            collision_recovered = false;
                            collision_reset_pending = true;
                        }
                        rebuild_online_republish(
                            &mut pending_online_republish,
                            &last_online,
                        );
                        needs_online_republish = true;
                        let delay = backoff_delay(
                            config.reconnect_min_delay,
                            config.reconnect_max_delay,
                            connect_failures,
                        );
                        warn!(
                            component = "mqtt-client",
                            error_code = "mqtt_connection_lost",
                            reconnect_delay_ms = delay.as_millis(),
                            "MQTT 连接中断: {e}; {connect_failures} 次连续失败，{}ms 后重试",
                            delay.as_millis()
                        );
                        if let Some(max) = config.max_reconnect_retries
                            && connect_failures >= max
                        {
                            error!(
                                component = "mqtt-client",
                                error_code = "mqtt_reconnect_exhausted",
                                max_reconnect_retries = max,
                                "MQTT 重连失败达到上限，客户端退出: {e}"
                            );
                            fail_all_queued(
                                &mut request_rx,
                                &mut pending,
                                &mut awaiting_ack,
                                MqttClientError::Disconnected {
                                    reason: e.to_string(),
                                },
                                &mqtt_metrics,
                            );
                            return;
                        }
                        backoff_until = Some(Instant::now() + delay);
                    }
                }
            }
            _ = shutdown_rx.changed() => break,
        }
    }

    // 优雅断开：仅当连接存在时发送 DISCONNECT，并冲刷到网络
    //（broker 收到 DISCONNECT 才不发布 LWT，§31.1）。
    if connected {
        // 阶段零：停机前为所有已跟踪设备主动发布 retained 离线状态
        //（§31.1 契约）：DISCONNECT 不触发 LWT，仅 LWT 只能覆盖一个
        // 设备；不显式发布离线，其余设备将长期显示在线。离线请求优先
        // 于 pending 中的用户请求（后者在停机时按 Closed 结算，不转发，
        // §31.4）。
        let mut offline_count = 0;
        for (site_id, device_id) in last_online.iter().rev() {
            if let Some(request) = make_offline_status_request(site_id, device_id) {
                pending.push_front(request);
                offline_count += 1;
            }
        }
        // 期限在阶段零之前确定：停机离线发布与优雅断开共享同一
        // GRACE_PERIOD 预算（§31.1 / §34.5）。
        let deadline = tokio::time::Instant::now() + GRACE_PERIOD;
        // 阶段零循环：离线请求必须全部送入 rumqttc 待发队列——设备数
        // 超过通道容量（= publish_capacity）时单次转发装不下，剩余的
        // 留在 pending 队首；若此时直接排入 DISCONNECT，剩余离线请求
        // 将按 Closed 结算、永不发送（设备长期显示在线）。因此每次
        // 转发后泵事件循环腾出通道空间，再继续转发，直到全部入队或
        // 期限届满。离线请求最后入队、位于队首且彼此连续（`online_status
        // = false`），队首 `take_while` 计数即剩余量。
        let mut offline_remaining = offline_count;
        while offline_remaining > 0 && tokio::time::Instant::now() < deadline {
            forward_pending(
                &client,
                &mut pending,
                &mut awaiting_ack,
                &mut last_online,
                Some(offline_remaining),
                &mqtt_metrics,
            );
            offline_remaining = pending
                .iter()
                .take_while(|r| r.status_ids.is_some() && !r.online_status)
                .count();
            if offline_remaining == 0 {
                break;
            }
            // 泵事件循环：写出已入队的离线请求并腾出通道空间；期间仍
            // 结算 PUBACK（已确认消息不得在停机时误报 Closed，§31.4）。
            tokio::select! {
                ev = eventloop.poll() => {
                    match ev {
                        Ok(Event::Incoming(Incoming::PubAck(puback))) => {
                            on_puback_event(
                                &mut awaiting_ack,
                                    &mut collision_pkid,
                                    &mut collision_recovered,
                                puback.pkid,
                                    &mqtt_metrics,
                            );
                        }
                        Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                            on_publish_event(
                                &mut awaiting_ack,
                                &mut collision_pkid,
                                &mut collision_recovered,
                                &mut collision_reset_pending,
                                pkid,
                            );
                        }
                        Ok(Event::Outgoing(Outgoing::AwaitAck(pkid))) => {
                            on_await_ack_event(
                                &mut awaiting_ack,
                                    &mut collision_pkid,
                                    &mut collision_recovered,
                                pkid,
                                    &mqtt_metrics,
                            );
                        }
                        Ok(_) => {}
                        Err(_) => break, // 连接已断：剩余离线请求按 Closed 结算
                    }
                }
                // 期限保护：与阶段一/二共享预算，网络写阻塞时不得无限等待。
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }
        if offline_count > 0 {
            if offline_remaining == 0 {
                info!(
                    component = "mqtt-client",
                    offline_count,
                    "MQTT 停机：{offline_count} 台设备的 retained 离线状态已全部入队"
                );
            } else {
                warn!(
                    component = "mqtt-client",
                    error_code = "mqtt_shutdown_offline_unflushed",
                    offline_count,
                    offline_remaining,
                    "MQTT 停机：{offline_remaining}/{offline_count} 台设备的离线状态未能在期限内送达，将按 Closed 结算"
                );
            }
        }
        let mut disconnect_written = false;
        // 阶段一：写出 DISCONNECT。同时泵事件循环与等待 DISCONNECT
        // 入队：rumqttc 待发队列满时 `disconnect()` 会等待队列腾空，
        // 必须持续 poll 才能排空队列，否则 DISCONNECT 永远无法写出、
        // 超时后直接断开并触发 LWT。`disconnect()` 只能调用一次：
        // 重复调用会让 rumqttc 写出多份 DISCONNECT 报文。
        let mut disconnect_pending = true;
        while !disconnect_written && tokio::time::Instant::now() < deadline {
            tokio::select! {
                _ = client.disconnect(), if disconnect_pending => {
                    // DISCONNECT 请求已入队；继续泵事件循环直到写出。
                    disconnect_pending = false;
                }
                ev = eventloop.poll() => {
                    match ev {
                        Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                            on_publish_event(
                                &mut awaiting_ack,
                                &mut collision_pkid,
                                &mut collision_recovered,
                                &mut collision_reset_pending,
                                pkid,
                            );
                        }
                        Ok(Event::Outgoing(Outgoing::AwaitAck(pkid))) => {
                            on_await_ack_event(
                                &mut awaiting_ack,
                                    &mut collision_pkid,
                                    &mut collision_recovered,
                                pkid,
                                    &mqtt_metrics,
                            );
                        }
                        Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                            disconnect_written = true;
                        }
                        // 停机排空期间仍结算已确认的发布（§31.4：已
                        // PUBACK 的消息不得结算为 Closed，否则 WAL
                        // 会重复补传）。
                        Ok(Event::Incoming(Incoming::PubAck(puback))) => {
                            on_puback_event(
                                &mut awaiting_ack,
                                    &mut collision_pkid,
                                    &mut collision_recovered,
                                puback.pkid,
                                    &mqtt_metrics,
                            );
                        }
                        Ok(_) => {}
                        Err(_) => break, // 连接已断：无需 DISCONNECT
                    }
                }
                // 期限保护：网络写阻塞时 `poll()` 可能等待一个完整
                // connect_timeout，必须由 sleep 分支兜底，否则声明的
                // 停机期限失效。
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }
        // 阶段二：DISCONNECT 写出后，broker 仍可能回传在途 PUBACK
        //（先收 PUBLISH、后收 DISCONNECT 时确认晚于断开）；持续泵事件
        // 循环直到全部未确认发布结算或期限届满（§31.4 同上）。
        if disconnect_written {
            while !awaiting_ack.is_empty() && tokio::time::Instant::now() < deadline {
                tokio::select! {
                    ev = eventloop.poll() => {
                        match ev {
                            Ok(Event::Incoming(Incoming::PubAck(puback))) => {
                                on_puback_event(
                                    &mut awaiting_ack,
                                        &mut collision_pkid,
                                        &mut collision_recovered,
                                    puback.pkid,
                                        &mqtt_metrics,
                                );
                            }
                            Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                                on_publish_event(
                                    &mut awaiting_ack,
                                    &mut collision_pkid,
                                    &mut collision_recovered,
                                    &mut collision_reset_pending,
                                    pkid,
                                );
                            }
                            Ok(Event::Outgoing(Outgoing::AwaitAck(pkid))) => {
                                on_await_ack_event(
                                    &mut awaiting_ack,
                                        &mut collision_pkid,
                                        &mut collision_recovered,
                                    pkid,
                                        &mqtt_metrics,
                                );
                            }
                            Ok(_) => {}
                            // 连接已关闭：在途确认已全部到达（broker 先
                            // 回 PUBACK 再关闭，FIFO 保证顺序）。
                            Err(_) => break,
                        }
                    }
                    // 期限保护：broker 沉默或确认迟到时不得无限等待。
                    _ = tokio::time::sleep_until(deadline) => break,
                }
            }
        }
        if disconnect_written {
            debug!(component = "mqtt-client", "MQTT 优雅断开完成");
        } else {
            warn!(
                component = "mqtt-client",
                error_code = "mqtt_graceful_disconnect_timeout",
                "优雅断开超时（{GRACE_PERIOD:?}）：DISCONNECT 未写出，broker 可能发布 LWT"
            );
        }
    }
    // 结算未处理的发布请求（停机取消；未确认消息不得误报成功，§31.4）。
    fail_all_queued(
        &mut request_rx,
        &mut pending,
        &mut awaiting_ack,
        MqttClientError::Closed,
        &mqtt_metrics,
    );
}

/// 校验并接受一个发布请求：主题与载荷合法则入队 `pending`（等待转发
/// 与 PUBACK），非法则直接应答 [`MqttClientError::InvalidTopic`] /
/// [`MqttClientError::PayloadTooLarge`]。
///
/// 本地先校验主题：rumqttc 对非法主题与内部队列已满都返回
/// `ClientError::Request`，无法区分，故在调用前校验；载荷上限同理
///（超限错误若发生在事件循环，会被误判为连接故障并触发重连）。
fn accept_request(
    pending: &mut VecDeque<PublishRequest>,
    request: PublishRequest,
    max_packet_size: usize,
    last_online: &mut BTreeSet<(String, String)>,
    pending_online_republish: &mut VecDeque<(String, String)>,
    mqtt_metrics: &MqttMetrics,
) {
    let result = validate_publish_topic(&request.topic)
        .and_then(|()| validate_payload_size(&request.topic, &request.payload, max_packet_size));
    match result {
        Ok(()) => {
            // `publish_offline`（设备下线）：校验通过即从在线跟踪移除
            //——含尚未转发的在线重发条目与待重发队列——重连时不得再
            // 标记在线（§31.1）。校验失败则不做任何清理（设备保持在线
            // 跟踪，离线请求被拒绝）。
            if request.deregister
                && let Some(ids) = &request.status_ids
            {
                last_online.remove(ids);
                pending.retain(|r| !(r.online_status && r.status_ids.as_ref() == Some(ids)));
                pending_online_republish.retain(|d| d != ids);
            }
            mqtt_metrics.observe_accepted();
            pending.push_back(request);
        }
        Err(e) => {
            // 入队前拒绝（非法主题 / 超限载荷）：计入失败结算。
            mqtt_metrics.observe_failed();
            let _ = request.ack_tx.send(Err(e));
        }
    }
}

/// 已连接时把 `pending` 转发到 rumqttc 待发队列（QoS 1，§31.1），
/// 成功后转入 `awaiting_ack`（等待 PUBACK）。`limit` 限制最多转发条数
///（停机阶段零只转发离线状态，不转发其后的用户请求——后者在停机时
/// 按 Closed 结算，§31.4；主循环传 `None` 不限量）。
///
/// 使用非阻塞 `try_publish`：rumqttc 通道满时保留在 `pending` 等待
/// 事件循环消费（下轮重试），不会阻塞 worker 驱动连接。请求在
/// `pending` 中保持 FIFO，转发顺序即入队顺序（§31.3）。在线状态
/// 发布转发成功后按设备记录到 `last_online`，异常断线重连后据此重新
/// 发布（§31.1 配合 retained LWT；未转发成功的状态不记录，重连后由
/// `pending` 本身转发，不会重复发布）。
fn forward_pending(
    client: &AsyncClient,
    pending: &mut VecDeque<PublishRequest>,
    awaiting_ack: &mut VecDeque<AwaitingAck>,
    last_online: &mut BTreeSet<(String, String)>,
    limit: Option<usize>,
    mqtt_metrics: &MqttMetrics,
) {
    let mut forwarded = 0;
    while let Some(req) = pending.front() {
        if let Some(limit) = limit
            && forwarded >= limit
        {
            break;
        }
        let topic = req.topic.clone();
        let payload = req.payload.clone();
        let retain = req.retain;
        let online_status = req.online_status;
        match client.try_publish(&topic, QoS::AtLeastOnce, retain, payload) {
            Ok(()) => {
                let req = pending.pop_front().expect("front 与 pop 之间无人修改");
                if online_status && let Some(ids) = &req.status_ids {
                    last_online.insert(ids.clone());
                }
                awaiting_ack.push_back(AwaitingAck {
                    pkid: None,
                    // 本轮会话新转发：断线时位于 rumqttc 通道（重发顺序最末）。
                    leftover: false,
                    parked_pkid: None,
                    accepted_at: req.accepted_at,
                    ack_tx: req.ack_tx,
                });
                forwarded += 1;
            }
            Err(rumqttc::ClientError::TryRequest(_)) => break, // 通道满，下轮重试
            Err(e) => {
                // 转发失败（异常路径）：以明确错误结算该请求（调用方
                // 不得删除 WAL，§31.4），并继续处理后续请求。
                let failed = pending.pop_front().expect("front 与 pop 之间无人修改");
                mqtt_metrics.observe_failed();
                let _ = failed.ack_tx.send(Err(MqttClientError::PublishFailed {
                    reason: e.to_string(),
                }));
                forwarded += 1;
                warn!(
                    component = "mqtt-client",
                    error_code = "mqtt_publish_forward_failed",
                    topic = %topic,
                    "MQTT 请求转发失败: {e}"
                );
            }
        }
    }
}

/// 为指定设备生成在线状态重发请求（断线重连后逐设备重发，§31.1）：
/// payload 重新生成 Status Envelope（`sent_at_ns` 取重发时刻，不复用
/// 旧载荷——旧时间戳会失真，§31.1 契约）。设备 ID 在 `publish_online`
/// 时已校验；构造失败（如后续放宽校验）返回 `None`，跳过该设备。
fn make_online_status_request(site_id: &str, device_id: &str) -> Option<PublishRequest> {
    let topic = status_topic(site_id, device_id).ok()?;
    let payload = status_envelope(site_id, device_id, "online");
    let (ack_tx, _ack_rx) = oneshot::channel();
    Some(PublishRequest {
        topic,
        payload,
        retain: true,
        online_status: true,
        status_ids: Some((site_id.to_owned(), device_id.to_owned())),
        deregister: false,
        accepted_at: Instant::now(),
        ack_tx,
    })
}

/// 为指定设备生成 retained 离线状态请求（停机前主动发布，§31.1 契约：
/// DISCONNECT 不触发 LWT，必须显式发布离线，否则设备将长期显示在线）。
/// `online_status = false`：离线发布不进入在线跟踪（`forward_pending`
/// 不会将其写入 `last_online`）。
fn make_offline_status_request(site_id: &str, device_id: &str) -> Option<PublishRequest> {
    let topic = status_topic(site_id, device_id).ok()?;
    let payload = status_envelope(site_id, device_id, "offline");
    let (ack_tx, _ack_rx) = oneshot::channel();
    Some(PublishRequest {
        topic,
        payload,
        retain: true,
        online_status: false,
        status_ids: Some((site_id.to_owned(), device_id.to_owned())),
        deregister: false,
        accepted_at: Instant::now(),
        ack_tx,
    })
}

/// 断线时重建完整在线状态重发周期（§31.1）：清空进度队列后用
/// `last_online` 全集填充。每次断线都重建——上一轮重发周期中已确认
/// 推送（已从队列弹出）的设备在二次断线时不会出现在
/// `pending_online_republish` 里，只清空不重建会让这些设备永久离线
///（其 LWT 已发布，重连后必须重新发布在线状态）。
///
/// 不跳过仍有在途条目的设备：未确认的在线状态由 rumqttc 在重连后
/// 原样重发，重建周期再发布一份新时间戳的在线状态——两者幂等
///（同一 retained 主题，最终值一致），保证每次断线后都是完整周期。
fn rebuild_online_republish(
    pending_online_republish: &mut VecDeque<(String, String)>,
    last_online: &BTreeSet<(String, String)>,
) {
    pending_online_republish.clear();
    pending_online_republish.extend(last_online.iter().cloned());
}

/// 单轮推进在线状态重发：从 `pending_online_republish` 队首取出设备，
/// 生成重发请求加入 `pending`（不超过 `capacity` 空位）。队列耗尽时
/// 清除 `needs_online_republish` 标记。从队首推进保证进度不丢失：
/// 设备数超过容量时每轮只推进部分设备，下一轮从断点继续（每次从头
/// 遍历会让尾部设备永久遗漏，§31.1）。
fn step_online_republish(
    pending_online_republish: &mut VecDeque<(String, String)>,
    pending: &mut VecDeque<PublishRequest>,
    capacity: usize,
    needs_online_republish: &mut bool,
) {
    while pending.len() < capacity {
        let Some((site_id, device_id)) = pending_online_republish.pop_front() else {
            *needs_online_republish = false;
            break;
        };
        if let Some(request) = make_online_status_request(&site_id, &device_id) {
            pending.push_back(request);
        }
    }
}

/// `Outgoing::Publish(pkid)`：为最老的未关联条目填充包标识（重发事件
/// 会命中已有关联的条目，此时不做任何事；包标识在断线重连后会复用，
/// 已由 `reset_pkids` 清空）。碰撞停放的条目（`parked_pkid` 非空）跳过：
/// 其在 rumqttc 碰撞槽中，尚未进入写序列——旧同标识消息确认时 rumqttc
/// 先入队 `Outgoing::Publish`（本事件）、后入队 `Incoming::PubAck`，
/// `on_publish_event` 已在调用本函数前解除停放，故碰撞消息以原标识
/// 关联到紧随其后的写事件（§31.4：提前分配会让后续写事件抢占其位置，
/// PUBACK 关联错位）。
fn assign_pkid(awaiting_ack: &mut VecDeque<AwaitingAck>, pkid: u16) {
    if let Some(entry) = awaiting_ack
        .iter_mut()
        .find(|e| e.pkid.is_none() && e.parked_pkid.is_none())
    {
        entry.pkid = Some(pkid);
    }
}

/// `Outgoing::AwaitAck(pkid)`：包标识碰撞（rumqttc `state.rs`
/// `outgoing_publish`——pkid 回绕撞到未确认槽位时，把本应使用该标识的
/// 新消息停放在碰撞槽并发出本事件，等待旧同标识消息确认后才写出）。
/// 把最老的未关联、未停放条目标记为停放，配对记录碰撞标识（该消息
/// 就是碰撞槽中的消息：rumqttc 的写顺序 = 转发顺序）。
///
/// `collision_pkid` 总是切换到本次碰撞标识：rumqttc 的碰撞槽是单个，
/// 第二次碰撞会**覆盖**槽位（旧碰撞消息永久丢失，`on_await_ack_event`
/// 已先把旧停放条目失败结算，此处只剩当前碰撞槽的消息）。异常时
/// （无可用条目）仅告警，不做猜测性标记。
fn park_collided(
    awaiting_ack: &mut VecDeque<AwaitingAck>,
    collision_pkid: &mut Option<u16>,
    pkid: u16,
) {
    if let Some(entry) = awaiting_ack
        .iter_mut()
        .find(|e| e.pkid.is_none() && e.parked_pkid.is_none())
    {
        entry.parked_pkid = Some(pkid);
        *collision_pkid = Some(pkid);
    } else {
        warn!(
            component = "mqtt-client",
            error_code = "mqtt_collision_park_missing",
            pkid,
            "收到 AwaitAck 但无可停放的未关联请求（协议异常路径）"
        );
    }
}

/// `Outgoing::Publish(pkid)`：填充最老未关联条目的包标识，并区分
/// "碰撞恢复写"与"重发写"（§31.4）。
///
/// rumqttc 处理旧同标识消息的确认时（`state.rs` `handle_incoming_puback`
/// -> `check_collision`），事件入队顺序是**先** `Outgoing::Publish(pkid)`
///（碰撞消息以同一 pkid 写出）、**后** `Incoming::PubAck(pkid)`（旧
/// 消息确认，`state.rs` `handle_incoming_packet` 最后入队）。因此解除
/// 停放必须发生在写事件上：本事件到达时碰撞已解决，停放条目可恢复
/// 参与 `assign_pkid`，紧随其后的 PUBACK 结算旧消息、碰撞消息等待
/// broker 的后续确认。
///
/// 但断线重连后的**重发写事件**也会以同一 pkid 出现（rumqttc 重发保留
/// 原 pkid，`clean()` 不清除碰撞槽），此时碰撞尚未解决、解除停放会让
/// 碰撞消息被后续写事件抢占标识（PUBACK 关联错位、WAL 提前删除未确认
/// 记录）。区分依据：
///
/// - 碰撞恢复写只可能由**本连接内**的旧消息 PUBACK 触发（入队时序
///   先写后确认，故用 `collision_recovered` 记录"已确认、恢复写将至"）；
/// - 重发写只可能发生在**碰撞未决期间断线重连**之后（`reset_pkids` 时
///   置 `collision_reset_pending`，在首个同标识写事件上消费）。
///
/// 解除停放按**配对标识**进行（`parked_pkid`）：rumqttc 单碰撞槽被
/// 第二次碰撞覆盖时存在多个停放条目，按写事件 pkid 匹配配对条目——
/// 绝不触碰其它停放条目（其消息已被碰撞槽覆盖丢失，保持停放直至失败
/// 结算，防止未发送消息被误判成功、WAL 提前删除，§31.4）。
fn on_publish_event(
    awaiting_ack: &mut VecDeque<AwaitingAck>,
    collision_pkid: &mut Option<u16>,
    collision_recovered: &mut bool,
    collision_reset_pending: &mut bool,
    pkid: u16,
) {
    if let Some(entry) = awaiting_ack
        .iter_mut()
        .find(|e| e.parked_pkid == Some(pkid))
    {
        if *collision_recovered || !*collision_reset_pending {
            // 碰撞恢复写（同连接内，或旧消息已确认后）：按配对解除
            // 停放，碰撞消息恢复参与标识分配。
            entry.parked_pkid = None;
            *collision_pkid = None;
            *collision_recovered = false;
        } else {
            // 重发写（碰撞未决期间断线重连后的首个同标识事件）：
            // 碰撞尚未解决，停放条目保持停放；消费本次重发标记。
            *collision_reset_pending = false;
        }
    }
    assign_pkid(awaiting_ack, pkid);
}

/// `Incoming::PubAck(pkid)`：按包标识结算第一个匹配的请求，并记录
/// 碰撞恢复状态（§31.4）。
///
/// 若 `pkid` 是进行中碰撞的标识，本确认属于旧同标识消息——rumqttc
/// 处理该确认时会把碰撞消息以同一 pkid 恢复写出（其 `Outgoing::Publish`
/// 事件随后入队）。置 `collision_recovered` 后，下一个同标识写事件即
/// 被 `on_publish_event` 识别为碰撞恢复写并解除停放。
fn on_puback_event(
    awaiting_ack: &mut VecDeque<AwaitingAck>,
    collision_pkid: &mut Option<u16>,
    collision_recovered: &mut bool,
    pkid: u16,
    mqtt_metrics: &MqttMetrics,
) {
    if *collision_pkid == Some(pkid) {
        *collision_recovered = true;
    }
    resolve_puback(awaiting_ack, pkid, mqtt_metrics);
}

/// `Outgoing::AwaitAck(pkid)`：包标识碰撞开始（§31.4）。停放入队最老的
/// 未关联条目并记录碰撞标识；丢弃残留的"恢复写将至"标记（新碰撞开始）。
///
/// 已有未决碰撞时（`collision_pkid` 非空）说明 rumqttc 的**单碰撞槽已
/// 被第二次碰撞覆盖**：旧碰撞消息已不可能再写出或确认（其恢复写永远
/// 不会出现）——立即以 [`MqttClientError::CollisionOverwritten`] 失败
/// 结算旧停放条目（`acked()` 返回 `Err`，WAL 记录不得删除、可重试补传；
/// 否则连接保持健康时该请求会永久等待，WAL 无法重试。客户端本身仍
/// 正常运行，此错误仅针对被覆盖的单条发布），并把碰撞标识切换到
/// rumqttc 实际保存的新碰撞。
fn on_await_ack_event(
    awaiting_ack: &mut VecDeque<AwaitingAck>,
    collision_pkid: &mut Option<u16>,
    collision_recovered: &mut bool,
    pkid: u16,
    mqtt_metrics: &MqttMetrics,
) {
    *collision_recovered = false;
    if let Some(previous) = *collision_pkid {
        if let Some(index) = awaiting_ack
            .iter()
            .position(|e| e.parked_pkid == Some(previous))
        {
            let entry = awaiting_ack.remove(index).expect("position 与 remove 一致");
            mqtt_metrics.observe_failed();
            let _ = entry
                .ack_tx
                .send(Err(MqttClientError::CollisionOverwritten));
            warn!(
                component = "mqtt-client",
                error_code = "mqtt_collision_slot_overwrite",
                pkid,
                previous,
                "rumqttc 碰撞槽已被第二次碰撞覆盖：旧碰撞消息已不可能写出或确认，按 CollisionOverwritten 失败结算（WAL 保留可重试）"
            );
        } else {
            warn!(
                component = "mqtt-client",
                error_code = "mqtt_collision_park_missing",
                pkid,
                previous,
                "第二个未决碰撞但找不到配对旧碰撞条目（协议异常路径）"
            );
        }
    }
    park_collided(awaiting_ack, collision_pkid, pkid);
}

/// `Incoming::PubAck(pkid)`：按包标识结算第一个匹配的请求（PUBACK
/// 允许乱序到达，按标识关联而不是队首）。未匹配（协议异常）时仅记录
/// 日志：该请求保持未确认，由退出路径结算，不会误报成功。
fn resolve_puback(awaiting_ack: &mut VecDeque<AwaitingAck>, pkid: u16, mqtt_metrics: &MqttMetrics) {
    if let Some(index) = awaiting_ack.iter().position(|e| e.pkid == Some(pkid)) {
        let entry = awaiting_ack.remove(index).expect("position 与 remove 一致");
        // §34.2.1：发布往返耗时（请求接受 → PUBACK resolve；断线重发后
        // 确认的耗时同样覆盖——同一 resolve 路径）。
        mqtt_metrics.observe_publish_latency(entry.accepted_at.elapsed());
        mqtt_metrics.observe_published();
        let _ = entry.ack_tx.send(Ok(()));
        trace!(
            component = "mqtt-client",
            pkid, "MQTT PUBACK 已确认（§31.4 可删除对应 WAL 记录）"
        );
    } else {
        warn!(
            component = "mqtt-client",
            error_code = "mqtt_unsolicited_puback",
            pkid,
            "收到无法关联的 PUBACK（可能为重连重排或 pkid 复用的边界情况）"
        );
    }
}

/// 断线时重排 `awaiting_ack`，使重连后的重发 PUBACK 仍关联到原请求。
///
/// rumqttc 断线时（`EventLoop::clean`）的重发顺序是：
///
/// 1. 上一轮遗留的未重发请求（还在 `EventLoop::pending` 中，排最前）；
/// 2. 本轮会话在途请求（`MqttState::clean` 按 pkid 槽位顺序：`pkid >
///    last_puback` 升序在前、`<= last_puback` 升序在后）；
/// 3. 本轮会话新转发、仍在通道中的请求（排最后）。
///
/// 重连后按该顺序以新包标识重发（重发优先于新请求）。此处按同样顺序
/// 重排并清空包标识；第 1 类请求以 `leftover` 标记识别（`pkid = None`
/// 的两类请求无法从包标识区分），第 3 类请求由 `forward_pending` 置
/// `leftover = false`。`assign_pkid` 按队首优先重关联后，PUBACK 即
/// 回到正确的请求上（§31.4：不得把确认关联到别的请求，否则 WAL 可能
/// 提前删除未确认记录）。
fn reset_pkids(awaiting_ack: &mut VecDeque<AwaitingAck>, last_puback: u16) {
    let mut entries: Vec<AwaitingAck> = awaiting_ack.drain(..).collect();
    entries.sort_by_key(|e| match e.pkid {
        // 第一组：上一轮遗留、本轮尚未重发的请求（排最前）。
        None if e.leftover => (0, (false, 0)),
        // 第二组：本轮在途请求，pkid > last_puback（升序）在前、
        // pkid <= last_puback（升序）在后，与 `MqttState::clean` 的
        // 槽位迭代顺序一致。
        Some(pkid) => (1, (pkid <= last_puback, pkid)),
        // 第三组：本轮新转发、仍在通道中的请求（排最后）。
        None => (2, (false, 0)),
    });
    for entry in &mut entries {
        entry.pkid = None;
        // 全部标记为遗留：下一次断线时它们位于 rumqttc 待发队列最前
        //（尚未重发者），排序时归入第一组。
        entry.leftover = true;
    }
    *awaiting_ack = entries.into();
}

/// 把通道中、`pending` 与 `awaiting_ack` 中未结算的请求全部以 `err`
/// 结算（任务退出前调用）：任何未确认消息都以明确错误告知调用方
///（§31.4：不得误报成功、不得静默丢弃）。
fn fail_all_queued(
    request_rx: &mut mpsc::Receiver<PublishRequest>,
    pending: &mut VecDeque<PublishRequest>,
    awaiting_ack: &mut VecDeque<AwaitingAck>,
    err: MqttClientError,
    mqtt_metrics: &MqttMetrics,
) {
    while let Ok(req) = request_rx.try_recv() {
        mqtt_metrics.observe_failed();
        let _ = req.ack_tx.send(Err(err.clone()));
    }
    while let Some(req) = pending.pop_front() {
        mqtt_metrics.observe_failed();
        let _ = req.ack_tx.send(Err(err.clone()));
    }
    while let Some(entry) = awaiting_ack.pop_front() {
        mqtt_metrics.observe_failed();
        let _ = entry.ack_tx.send(Err(err.clone()));
    }
}

#[cfg(test)]
mod tests {
    use data_pipeline::ObservationBatch;

    use super::*;
    use crate::config::{DEFAULT_RECONNECT_MAX_DELAY, DEFAULT_RECONNECT_MIN_DELAY, WillConfig};
    use crate::mock::{CapturedWill, MockBroker, RawClient, wait_until};

    /// 指向 mock broker 的测试配置：快速退避、短连接超时。
    fn test_config(broker: &MockBroker, client_id: &str) -> MqttClientConfig {
        let mut config = MqttClientConfig::new(client_id, "127.0.0.1", broker.addr().port());
        config.reconnect_min_delay = Duration::from_millis(50);
        config.reconnect_max_delay = Duration::from_millis(200);
        config.connect_timeout = Duration::from_secs(1);
        config
    }

    /// 一个最小 Batch（§31.2 信封字段齐全）。
    fn make_batch(site_id: &str, device_id: &str, sequence: u64) -> ObservationBatch {
        ObservationBatch {
            schema: data_pipeline::TELEMETRY_SCHEMA.to_owned(),
            message_id: format!("2:s15:{device_id}{sequence}"),
            site_id: site_id.to_owned(),
            device_id: device_id.to_owned(),
            sequence,
            sent_at_ns: 1_700_000_000_000_000_000,
            replayed: false,
            observations: Vec::new(),
        }
    }

    #[tokio::test]
    async fn publishes_qos1_batch_to_telemetry_topic() {
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn(test_config(&broker, "client-a")).unwrap();
        let batch = make_batch("plant-a", "cnc-01", 3);
        client
            .publish_batch(&batch)
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();

        wait_until(|| broker.publishes().len() == 1).await;
        let captured = &broker.publishes()[0];
        assert_eq!(captured.topic, "forgelink/v1/telemetry/plant-a/cnc-01");
        assert_eq!(captured.qos, 1);
        assert!(!captured.retain, "Telemetry 不 retain（§31.1）");
        assert!(!captured.dup);
        let payload: serde_json::Value = serde_json::from_slice(&captured.payload).unwrap();
        assert_eq!(payload["schema"], "forgelink.telemetry.v1");
        assert_eq!(payload["message_id"], batch.message_id);
        assert_eq!(payload["site_id"], "plant-a");
        assert_eq!(payload["device_id"], "cnc-01");
        assert_eq!(payload["sequence"], 3);
        assert!(!payload["replayed"].as_bool().unwrap());

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn publish_after_broker_drop_reconnects_and_delivers() {
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn(test_config(&broker, "client-b")).unwrap();

        let first = client.publish("t/1", b"first").await.unwrap();
        first.acked().await.unwrap();
        wait_until(|| broker.publishes().len() == 1).await;

        // 模拟网络故障：broker 中断全部连接（任务被中止，连接任务无法
        // 运行结束逻辑，abnormal_disconnects 不会增加）。
        broker.drop_all_connections();
        // 客户端自动重连（§34.3）。
        wait_until(|| broker.connections() == 2).await;

        // 断线后发布：自动重连后送达并确认（at-least-once，§31.3；
        // PUBACK 确认才能删除 WAL，§31.4）。
        let second = client.publish("t/2", b"second").await.unwrap();
        second.acked().await.unwrap();
        wait_until(|| broker.publishes().len() == 2).await;
        assert_eq!(broker.connections(), 2, "应建立第二条连接");
        assert_eq!(broker.publishes()[1].payload, b"second");

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn unacked_publish_redelivered_after_reconnect() {
        let broker = MockBroker::start().await;
        broker.drop_connection_after_publish();
        let client = MqttClient::spawn(test_config(&broker, "client-c")).unwrap();

        // 首个 PUBLISH 未收到 PUBACK 时连接被断开：重连后 rumqttc
        // 自动重发（at-least-once，§31.3）。注意 rumqttc 0.24 重发时
        // 不置 DUP 位，消费侧按 message_id 去重（§31.3）。
        let receipt = client.publish("t/redeliver", b"payload-x").await.unwrap();
        wait_until(|| broker.publishes().len() == 2).await;

        let first = &broker.publishes()[0];
        let second = &broker.publishes()[1];
        assert_eq!(first.topic, second.topic);
        assert_eq!(first.payload, second.payload);
        assert_eq!(broker.connections(), 2, "重发必须发生在重连后的新连接");

        // 重发后的 PUBACK 必须关联到原请求：确认只在重发送达后到达
        //（§31.4：WAL 记录直到 broker 确认才可删除）。
        receipt.acked().await.unwrap();

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn connect_registers_will() {
        let broker = MockBroker::start().await;
        let mut config = test_config(&broker, "client-d");
        config.will = Some(WillConfig {
            topic: "forgelink/v1/status/plant-a/cnc-01".to_owned(),
            payload: b"offline".to_vec(),
            retain: true,
        });
        let client = MqttClient::spawn(config).unwrap();

        wait_until(|| broker.connections() == 1).await;
        wait_until(|| !broker.wills().is_empty()).await;
        let will = &broker.wills()[0];
        assert_eq!(will.topic, "forgelink/v1/status/plant-a/cnc-01");
        assert_eq!(will.payload, b"offline");
        assert_eq!(will.qos, 1, "LWT QoS 固定为 1（§31.1）");
        assert!(will.retain, "status 使用 retained（§31.1）");

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn lwt_published_on_abnormal_disconnect() {
        let broker = MockBroker::start().await;
        let mut sub = broker.subscribe("forgelink/v1/status/plant-a/cnc-01").await;

        // 原生 MQTT 连接携带 Will 后直接掉线（未发送 DISCONNECT）：
        // broker 应代为发布 LWT（§31.1）。
        let raw = RawClient::connect_with_will(
            broker.addr(),
            Some(&CapturedWill {
                topic: "forgelink/v1/status/plant-a/cnc-01".to_owned(),
                payload: b"offline".to_vec(),
                qos: 1,
                retain: true,
            }),
        )
        .await;
        raw.drop();

        let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("LWT 未在期限内发布")
            .expect("订阅通道关闭");
        assert_eq!(msg.topic, "forgelink/v1/status/plant-a/cnc-01");
        assert_eq!(msg.payload, b"offline");
        assert!(msg.retain);
        assert_eq!(broker.abnormal_disconnects(), 1);
        broker.stop().await;
    }

    #[tokio::test]
    async fn graceful_shutdown_does_not_trigger_will() {
        let broker = MockBroker::start().await;
        let mut sub = broker.subscribe("forgelink/v1/status/plant-a/cnc-01").await;
        let mut config = test_config(&broker, "client-e");
        config.will = Some(WillConfig {
            topic: "forgelink/v1/status/plant-a/cnc-01".to_owned(),
            payload: b"offline".to_vec(),
            retain: true,
        });
        let client = MqttClient::spawn(config).unwrap();

        wait_until(|| broker.connections() == 1).await;
        client.shutdown().await.unwrap();

        assert_eq!(
            broker.abnormal_disconnects(),
            0,
            "优雅停机必须发送 DISCONNECT，不得触发 LWT"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), sub.recv())
                .await
                .is_err(),
            "优雅停机不得发布 LWT"
        );
        broker.stop().await;
    }

    /// 循环发布直到失败，避免"发布恰好在任务退出前入队"的竞态。
    /// 重连失败达到上限后任务退出：在途确认全部以 `Disconnected` 结算，
    /// 此后的发布返回 `Closed`（≤128 次发布、10s 内，均在有界队列容量内）。
    async fn wait_publish_failure(client: &MqttClient) -> Result<(), MqttClientError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "等待发布失败超时（任务未退出？）"
            );
            let result =
                tokio::time::timeout(Duration::from_secs(3), client.publish("t/fail", b"x"))
                    .await
                    .expect("发布在期限内未返回");
            match result {
                Ok(receipt) => {
                    // 发布成功入队：等待确认失败（任务退出时结算）。
                    match receipt.acked().await {
                        Ok(()) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    #[tokio::test]
    async fn bounded_retries_fail_publishes() {
        // broker 不可达：静态低端口 11880（低于系统临时端口范围，
        // 并行测试中 `bind("127.0.0.1:0")` 不会分配到这里；目标机上
        // 无服务监听 → 连接快速失败，退避/重试上限可确定触发）。
        let mut config = MqttClientConfig::new("client-f", "127.0.0.1", 11880);
        config.reconnect_min_delay = Duration::from_millis(50);
        config.reconnect_max_delay = Duration::from_millis(100);
        config.connect_timeout = Duration::from_secs(1);
        config.max_reconnect_retries = Some(2);
        let client = MqttClient::spawn(config).unwrap();

        // 连续 2 次连接失败后任务退出：发布最终必然失败。
        let result = wait_publish_failure(&client).await;
        assert!(
            matches!(
                result,
                Err(MqttClientError::Disconnected { .. }) | Err(MqttClientError::Closed)
            ),
            "期望 Disconnected/Closed，实际 {result:?}"
        );
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn backpressure_blocks_and_shutdown_unblocks() {
        // broker 不可达（同 bounded_retries_fail_publishes 的静态端口
        // 说明）+ 队列容量 1：第二个发布必须被有界队列阻塞，
        // 停机必须能打断阻塞（不遗留悬挂任务）。
        let mut config = MqttClientConfig::new("client-g", "127.0.0.1", 11880);
        config.reconnect_min_delay = Duration::from_millis(50);
        config.reconnect_max_delay = Duration::from_millis(100);
        config.connect_timeout = Duration::from_secs(1);
        config.publish_capacity = 1;
        let client = MqttClient::spawn(config).unwrap();

        // 第一个发布入队成功（等待确认失败/停机结算）。
        let first = client.publish("t/1", b"a").await.unwrap();
        // 第二个发布被内部有界队列阻塞。
        let blocked =
            tokio::time::timeout(Duration::from_millis(300), client.publish("t/2", b"b")).await;
        assert!(blocked.is_err(), "有界队列必须阻塞第二个发布");

        // 停机必须在阻塞期间完成（取消在途发布并结算未确认请求）。
        let shutdown = tokio::time::timeout(Duration::from_secs(2), client.shutdown()).await;
        assert!(shutdown.is_ok(), "停机被阻塞的发布卡死");
        // 停机结算：未确认的发布必须以明确错误返回（§31.4，不得误报成功）。
        let first_result = tokio::time::timeout(Duration::from_secs(2), first.acked()).await;
        assert!(
            matches!(
                first_result,
                Ok(Err(MqttClientError::Closed)) | Ok(Err(MqttClientError::Disconnected { .. }))
            ),
            "停机后未确认发布必须失败结算，实际 {first_result:?}"
        );
    }

    #[tokio::test]
    async fn publish_rejects_invalid_topic() {
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn(test_config(&broker, "client-h")).unwrap();

        let err = client.publish("a/+/b", b"x").await.unwrap_err();
        assert!(
            matches!(err, MqttClientError::InvalidTopic { .. }),
            "实际 {err:?}"
        );

        let err = client.publish("", b"x").await.unwrap_err();
        assert!(
            matches!(err, MqttClientError::InvalidTopic { .. }),
            "实际 {err:?}"
        );

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn telemetry_topic_rejects_empty_ids() {
        assert!(telemetry_topic("", "dev").is_err());
        assert!(telemetry_topic("site", "").is_err());
        assert_eq!(
            telemetry_topic("plant-a", "cnc-01").unwrap(),
            "forgelink/v1/telemetry/plant-a/cnc-01"
        );
    }

    #[test]
    fn topic_path_segments_reject_slash() {
        // site_id / device_id 含 '/' 会改变固定 Topic 层级，导致订阅与
        // ACL 路由错误（§31.1），必须拒绝。
        for ids in [("site/a", "dev"), ("site", "dev/a"), ("a/b", "c/d")] {
            let err = telemetry_topic(ids.0, ids.1).unwrap_err();
            assert!(
                matches!(err, MqttClientError::InvalidTopic { .. }),
                "实际 {err:?}"
            );
        }
        // status 主题同样受约束。
        assert!(status_topic("site/a", "dev").is_err());
        assert!(status_topic("site", "dev").is_ok());
    }

    #[test]
    fn publish_rejects_oversized_payload() {
        // 载荷超过 max_packet_size 必须在入队前拒绝（PayloadTooLarge），
        // 不能等事件循环误判为连接故障（§31.2 / §34.3）。
        let topic = "t/big";
        let max = 1024usize;
        let ok_size = max - PUBLISH_OVERHEAD_MAX - topic.len();
        assert!(validate_payload_size(topic, &vec![0u8; ok_size], max).is_ok());
        let err = validate_payload_size(topic, &vec![0u8; ok_size + 1], max).unwrap_err();
        assert!(
            matches!(err, MqttClientError::PayloadTooLarge { .. }),
            "实际 {err:?}"
        );
    }

    #[tokio::test]
    async fn publish_rejects_oversized_payload_before_queueing() {
        let broker = MockBroker::start().await;
        let mut config = test_config(&broker, "client-i");
        config.max_packet_size = 1024;
        let client = MqttClient::spawn(config).unwrap();

        let err = client.publish("t/big", vec![0u8; 4096]).await.unwrap_err();
        assert!(
            matches!(err, MqttClientError::PayloadTooLarge { .. }),
            "实际 {err:?}"
        );
        assert_eq!(broker.publishes().len(), 0, "超限载荷不得发出");

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn retained_publish_sets_retain_flag() {
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn(test_config(&broker, "client-j")).unwrap();

        // §31.1：status 在线状态使用 retained。
        client
            .publish_retained("forgelink/v1/status/plant-a/cnc-01", b"online")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        wait_until(|| broker.publishes().len() == 1).await;
        assert!(broker.publishes()[0].retain, "status 必须 retain（§31.1）");

        // Telemetry 不 retain。
        client
            .publish("t/plain", b"x")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        wait_until(|| broker.publishes().len() == 2).await;
        assert!(
            !broker.publishes()[1].retain,
            "Telemetry 不 retain（§31.1）"
        );

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn publish_online_uses_retained_status_topic() {
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn(test_config(&broker, "client-k")).unwrap();

        client
            .publish_online("plant-a", "cnc-01")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        wait_until(|| broker.publishes().len() == 1).await;
        let captured = &broker.publishes()[0];
        assert_eq!(captured.topic, "forgelink/v1/status/plant-a/cnc-01");
        assert!(captured.retain, "在线状态必须 retain（§31.1）");
        // §32：所有消息必须显式携带 schema/version（Status Envelope）。
        let payload: serde_json::Value = serde_json::from_slice(&captured.payload).unwrap();
        assert_eq!(payload["schema"], STATUS_SCHEMA);
        assert_eq!(payload["site_id"], "plant-a");
        assert_eq!(payload["device_id"], "cnc-01");
        assert_eq!(payload["status"], "online");
        assert!(
            payload["sent_at_ns"].as_u64().unwrap() > 0,
            "在线状态 Envelope 的 sent_at_ns 必须为发布时刻（§31.1）"
        );

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn online_status_republished_after_reconnect() {
        let broker = MockBroker::start().await;
        // 在线状态发布后连接被断开（首个 PUBLISH 不回复 PUBACK）：
        // broker 已发布 retained 离线 LWT，设备重新在线后必须重新发布
        // 在线状态（§31.1），不能持续显示离线。
        broker.drop_connection_after_publish();
        let client = MqttClient::spawn(test_config(&broker, "client-l")).unwrap();

        // 首次连接不回复 PUBACK：确认只能来自断线重发，`acked()` 返回
        // 时 broker 已记录原发 + 重发两条。
        client
            .publish_online("plant-a", "cnc-01")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();

        // status 主题共 3 条：原发 + 断线重发 + 重连成功后重发布。
        wait_until(|| broker.publishes().len() == 3).await;
        assert_eq!(broker.connections(), 2, "重发布必须发生在重连后的新连接");
        let republished = &broker.publishes()[2];
        assert_eq!(republished.topic, "forgelink/v1/status/plant-a/cnc-01");
        assert!(republished.retain, "重发布的在线状态必须 retain（§31.1）");
        let payload: serde_json::Value = serde_json::from_slice(&republished.payload).unwrap();
        assert_eq!(payload["status"], "online");
        assert_eq!(payload["schema"], STATUS_SCHEMA);

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn online_statuses_republished_per_device_after_reconnect() {
        let broker = MockBroker::start().await;
        // 两个设备的在线状态都发布并确认后，连接在第二个 PUBLISH 处被
        // 断开（不回复 PUBACK）：重连后两个设备都必须重新发布在线状态
        //（§31.1 逐设备记录，不能只恢复最近一个）。
        broker.drop_connection_after_publishes(2);
        let client = MqttClient::spawn(test_config(&broker, "client-l2")).unwrap();

        client
            .publish_online("plant-a", "cnc-01")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        client
            .publish_online("plant-a", "cnc-02")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();

        // 重连成功后逐设备重发布：cnc-01 = 原发 + 重发布（2 条，断线
        // 前已确认）；cnc-02 = 原发 + 断线重发 + 重发布（3 条）。
        wait_until(|| broker.publishes().len() == 5).await;
        assert_eq!(broker.connections(), 2);
        let publishes = broker.publishes();
        let status_publishes: Vec<_> = publishes
            .iter()
            .filter(|p| p.topic.starts_with("forgelink/v1/status/"))
            .collect();
        for (device, expected) in [("cnc-01", 2), ("cnc-02", 3)] {
            let per_device: Vec<_> = status_publishes
                .iter()
                .filter(|p| p.topic.ends_with(device))
                .collect();
            assert_eq!(per_device.len(), expected, "设备 {device} 在线状态条数");
            let last = per_device.last().expect("非空");
            let payload: serde_json::Value = serde_json::from_slice(&last.payload).unwrap();
            assert_eq!(payload["status"], "online");
            assert_eq!(payload["device_id"], device);
            assert!(
                payload["sent_at_ns"].as_u64().unwrap() > 0,
                "重发必须重新生成时间戳（不复用旧载荷，§31.1）"
            );
        }

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn online_status_republish_with_more_devices_than_capacity() {
        // 设备数（3）超过 publish_capacity（2）：重发必须从断点继续，
        // 否则第 3 台设备永久遗漏（§31.1）。
        let broker = MockBroker::start().await;
        broker.drop_connection_after_publishes(3);
        let mut config = test_config(&broker, "client-l3");
        config.publish_capacity = 2;
        let client = MqttClient::spawn(config).unwrap();

        for i in 0..3 {
            client
                .publish_online("plant-a", &format!("cnc-{i:02}"))
                .await
                .unwrap()
                .acked()
                .await
                .unwrap();
        }
        // 3 台设备全部确认后连接被断开（第 3 台未收到 PUBACK，重连后
        // 由 rumqttc 重发）；重连后逐台重发在线状态（断点推进，不遗漏）。
        wait_until(|| broker.publishes().len() == 7).await;
        assert_eq!(broker.connections(), 2);
        let publishes = broker.publishes();
        // cnc-00 / cnc-01：原发 + 重发布；cnc-02：原发 + 断线重发 + 重发布。
        for (i, expected) in [(0, 2), (1, 2), (2, 3)] {
            let device = format!("cnc-{i:02}");
            let per_device: Vec<_> = publishes
                .iter()
                .filter(|p| p.topic.ends_with(device.as_str()))
                .collect();
            assert_eq!(
                per_device.len(),
                expected,
                "设备 {device} 必须重发在线状态（原发 + 重发）"
            );
            let last = per_device.last().expect("非空");
            let payload: serde_json::Value = serde_json::from_slice(&last.payload).unwrap();
            assert_eq!(payload["status"], "online");
            assert!(payload["sent_at_ns"].as_u64().unwrap() > 0);
        }

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn publish_offline_deregisters_device_from_republish() {
        let broker = MockBroker::start().await;
        // 第二个 PUBLISH（离线状态）不回复 PUBACK 直接断开：离线原发
        // 未确认，重连后重发；设备已从在线跟踪移除，不得再重发在线。
        broker.drop_connection_after_publishes(2);
        let client = MqttClient::spawn(test_config(&broker, "client-n")).unwrap();

        client
            .publish_online("plant-a", "cnc-01")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        // 设备下线：发布 retained 离线状态并从在线跟踪移除。
        client
            .publish_offline("plant-a", "cnc-01")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        // 原发在线（1）+ 原发离线（2）+ 断线重发离线（3）。
        wait_until(|| broker.publishes().len() == 3).await;
        assert_eq!(broker.connections(), 2);
        let publishes = broker.publishes();
        for captured in &publishes[1..] {
            let payload: serde_json::Value = serde_json::from_slice(&captured.payload).unwrap();
            assert_eq!(payload["status"], "offline");
            assert!(
                payload["sent_at_ns"].as_u64().unwrap() > 0,
                "显式发布离线使用真实时间戳（仅 LWT 为 0，§31.1）"
            );
        }

        // 重连后不得再重发该设备的在线状态（已从跟踪移除）。
        let republished = tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                if broker.publishes().len() > 3 {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            republished.is_err(),
            "设备已下线，重连后不得重发在线状态（实际 4+ 条发布）"
        );

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn graceful_shutdown_publishes_offline_for_all_tracked_devices() {
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn(test_config(&broker, "client-o")).unwrap();

        client
            .publish_online("plant-a", "cnc-01")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        client
            .publish_online("plant-a", "cnc-02")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();

        // 停机：DISCONNECT 不触发 LWT，必须为每台已跟踪设备主动发布
        // retained 离线状态（§31.1 契约），否则设备将长期显示在线。
        client.shutdown().await.unwrap();
        let publishes = broker.publishes();
        let offline: Vec<_> = publishes
            .iter()
            .filter(|p| {
                serde_json::from_slice::<serde_json::Value>(&p.payload)
                    .map(|v| v["status"] == "offline")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(offline.len(), 2, "每台已跟踪设备必须发布离线状态");
        for p in &offline {
            assert!(p.retain, "离线状态必须 retain（§31.1）");
        }
        assert_eq!(
            broker.abnormal_disconnects(),
            0,
            "优雅停机必须发送 DISCONNECT（LWT 不触发）"
        );

        broker.stop().await;
    }

    #[tokio::test]
    async fn shutdown_delivers_all_offline_statuses_even_when_channel_full() {
        // rumqttc 通道容量 = publish_capacity（`AsyncClient::new` 的 cap
        // 参数）：3 台设备 > 容量 2 时，停机阶段零单次转发装不下全部
        // 离线请求，剩余的会留在 pending 并被后续 DISCONNECT 按 Closed
        // 结算、永不发送（§31.1）。阶段零必须循环转发 + 泵事件循环，
        // 直到全部离线状态入队或期限届满。
        let broker = MockBroker::start().await;
        let mut config = test_config(&broker, "client-q");
        config.publish_capacity = 2;
        let client = MqttClient::spawn(config).unwrap();

        for i in 0..3 {
            client
                .publish_online("plant-a", &format!("cnc-{i:02}"))
                .await
                .unwrap()
                .acked()
                .await
                .unwrap();
        }

        client.shutdown().await.unwrap();
        let publishes = broker.publishes();
        let offline: Vec<_> = publishes
            .iter()
            .filter(|p| {
                serde_json::from_slice::<serde_json::Value>(&p.payload)
                    .map(|v| v["status"] == "offline")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            offline.len(),
            3,
            "通道容量不足时停机离线状态必须逐条转发送达（不得 Closed 丢弃）"
        );
        for p in &offline {
            assert!(p.retain, "离线状态必须 retain（§31.1）");
        }
        assert_eq!(
            broker.abnormal_disconnects(),
            0,
            "优雅停机必须发送 DISCONNECT（LWT 不触发）"
        );
        broker.stop().await;
    }

    #[tokio::test]
    async fn republish_not_starved_by_continuous_telemetry() {
        // 断线重连后持续业务流量不得饿死在线重发（§31.1）：重发先于
        // 普通请求执行（重发受断线窗口限制、普通请求可等待），否则
        // pending 每轮被遥测占满，重发永远得不到空位。
        let broker = MockBroker::start().await;
        broker.drop_connection_after_publishes(3);
        let mut config = test_config(&broker, "client-f");
        config.publish_capacity = 2;
        let client = Arc::new(MqttClient::spawn(config).unwrap());

        for i in 0..3 {
            client
                .publish_online("plant-a", &format!("cnc-{i:02}"))
                .await
                .unwrap()
                .acked()
                .await
                .unwrap();
        }
        // 第 3 台设备（cnc-02）确认后连接被断开（未收到 PUBACK）。

        // 持续遥测流量：有界背压下把通道与 pending 占满（断线退避期间
        // 通道满，发送方阻塞，不会空转）。
        let flood = {
            let client = client.clone();
            tokio::spawn(async move {
                loop {
                    if client
                        .publish("forgelink/v1/telemetry/flood/dev", b"flood")
                        .await
                        .is_err()
                    {
                        break; // 停机后通道关闭，流量结束
                    }
                }
            })
        };
        // 重连后：cnc-02 断线重发（+1），随后全部设备重发在线状态。
        wait_until(|| {
            let publishes = broker.publishes();
            let count = |device: &str| {
                publishes
                    .iter()
                    .filter(|p| p.topic.ends_with(device))
                    .count()
            };
            count("cnc-00") >= 2 && count("cnc-01") >= 2 && count("cnc-02") >= 3
        })
        .await;
        // 等待 flood 任务真正结束（`abort` 只发取消信号，任务可能尚未
        // 释放其 `Arc` 克隆），再恢复独占所有权。
        flood.abort();
        let _ = flood.await;

        let client = Arc::try_unwrap(client).expect("flood 任务已结束，仅剩测试持有引用");
        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn republish_restarts_full_cycle_after_second_disconnect() {
        // 重发周期未完成时二次断线：上一轮周期中已确认推送（已从队列
        // 弹出）的设备必须重新进入重发周期（§31.1）——其 LWT 已发布，
        // 不重建则重连后永久离线。
        let broker = MockBroker::start().await;
        // 连接 1：第 3 个 PUBLISH（cnc-02 在线原发）后断开，cnc-02 未确认。
        broker.drop_connection_after_publishes(3);
        // 连接 2：第 3 个 PUBLISH 后断开——此时 cnc-00 重发已确认、
        // cnc-01 重发未确认（rumqttc 重连后重发）。
        broker.drop_connection_number(2, 3);
        let mut config = test_config(&broker, "client-r");
        config.publish_capacity = 2;
        let client = MqttClient::spawn(config).unwrap();

        for i in 0..3 {
            client
                .publish_online("plant-a", &format!("cnc-{i:02}"))
                .await
                .unwrap()
                .acked()
                .await
                .unwrap();
        }
        // 连接 2 的发布顺序：cnc-02 断线重发（1）、cnc-00 重发（2）、
        // cnc-01 重发（3，断开）。连接 2 上已确认的发布（cnc-02 重发、
        // cnc-00 重发）在断开时是否已收到 PUBACK 取决于 socket 时序
        //（断线瞬间在途确认可能丢失），因此不断言精确总数，只断言：
        // 重建周期后每台设备都被再次发布（cnc-00 已确认仍重新入队，
        // 其余设备由 rumqttc 重发 + 重建周期覆盖）。
        wait_until(|| {
            let publishes = broker.publishes();
            let count = |device: &str| {
                publishes
                    .iter()
                    .filter(|p| p.topic.ends_with(device))
                    .count()
            };
            (0..3).all(|i| count(&format!("cnc-{i:02}")) >= 3)
        })
        .await;
        assert_eq!(broker.connections(), 3);
        assert_eq!(broker.abnormal_disconnects(), 2);
        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn normal_publish_acks_attributed_after_double_disconnect() {
        // 普通（非状态）消息在二次断线后的确认归属（§31.4）：断线时
        // `reset_pkids` 必须与 rumqttc `EventLoop::clean` 的重发顺序一致
        //（遗留未重发 -> 在途 -> 本会话通道），否则重发 PUBACK 会结算到
        // 错误的请求（WAL 可能提前删除未确认记录）。精确的顺序语义由
        // `reset_pkids_orders_leftover_then_inflight_then_channel` 单测
        // 覆盖——断线瞬间在途确认可能丢失，集成测试无法断言解析顺序，
        // 这里验证：两次断线后所有消息都按原请求正确确认、无挂起。
        let broker = MockBroker::start().await;
        // 连接 1：第 3 个 PUBLISH（t/2）后断开；t/3、t/4 仍在通道中。
        broker.drop_connection_after_publishes(3);
        // 连接 2：重发 + 新转发共第 3 个 PUBLISH 后断开（重发进行中二次断线）。
        broker.drop_connection_number(2, 3);
        let mut config = test_config(&broker, "client-n");
        config.publish_capacity = 2;
        let client = MqttClient::spawn(config).unwrap();

        let mut receipts = Vec::new();
        for i in 0..5 {
            receipts.push(client.publish(&format!("t/{i}"), &[i as u8]).await.unwrap());
        }

        // 连接 3 重建待发队列：t/3、t/4（遗留）在前，t/0..t/2（在途）
        // 随后，全部重发并确认——每张收据必须成功结算。
        for (i, receipt) in receipts.into_iter().enumerate() {
            let result = tokio::time::timeout(Duration::from_secs(5), receipt.acked())
                .await
                .expect("重发确认不得挂起");
            assert!(
                result.is_ok(),
                "t/{i} 两次断线重发后必须确认成功，实际 {result:?}"
            );
        }
        assert_eq!(broker.connections(), 3);
        assert_eq!(broker.abnormal_disconnects(), 2);
        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn collision_parked_message_restored_after_disconnect() {
        // 断线重连 + 碰撞恢复路径的集成回归（§31.4）：在途窗口 4，
        // 发布 6 条（容量 2 受背压逐条入队）。确认延迟 + 背压下 pkid
        // 序列为 1,2,3,4,1,2——第 5 条回绕撞到未确认槽位触发碰撞停放
        //（`Outgoing::AwaitAck`），断线重连后旧同标识消息重发并确认，
        // 碰撞消息以同一 pkid 写出并确认——所有收据必须成功结算，且
        // 碰撞消息（t/5）的确认不得早于旧同标识消息（t/0）（归属正确，
        // WAL 不会提前删除未确认记录）。
        // 注：本测试的"碰撞未决断线"时序受背压与 rumqttc 单碰撞槽
        //（第二次回绕会覆盖碰撞槽）限制，碰撞是否真实发生取决于运行
        // 时序；精确的停放 / 重发 / 解除机制由
        // `collision_pending_resend_must_not_unpark_before_ack` 单测
        // 覆盖（重发写事件不解、恢复写事件才解，§31.4）。
        let broker = MockBroker::start().await;
        // 在途窗口 4：pkid 序列 1,2,3,4,5,1——第 6 条回绕撞到 m1 的槽位
        // 触发碰撞；ACK 延迟保证碰撞发生时旧消息均未确认（槽位未腾空）。
        broker.set_puback_delay(Duration::from_millis(300));
        // 连接 1 收到第 5 个 PUBLISH（m1..m5，pkid 1-5）后断开；m6 已因
        // 碰撞被停放（未写出，broker 收不到）。
        broker.drop_connection_after_publishes(5);
        let mut config = test_config(&broker, "client-c");
        config.publish_capacity = 2;
        config.max_inflight = 4;
        let client = MqttClient::spawn(config).unwrap();

        let mut receipts = Vec::new();
        for i in 0..6 {
            receipts.push(client.publish(&format!("t/{i}"), &[i as u8]).await.unwrap());
        }

        // 连接 2：重发 m1..m5（pkid 保留）→ m1 确认后碰撞解决，m6 以
        // pkid 1 写出并确认。记录每张收据的解析序号（worker 单线程按
        // 事件顺序结算，序号确定）。
        let resolve_order = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = receipts
            .into_iter()
            .enumerate()
            .map(|(i, receipt)| {
                let seq = resolve_order.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(Duration::from_secs(5), receipt.acked())
                        .await
                        .expect("确认不得挂起");
                    (
                        i,
                        seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                        result,
                    )
                })
            })
            .collect();
        let mut order = [usize::MAX; 6];
        for handle in handles {
            let (i, n, result) = handle.await.unwrap();
            assert!(
                result.is_ok(),
                "t/{i} 碰撞恢复后必须确认成功，实际 {result:?}"
            );
            order[i] = n;
        }
        assert!(
            order[5] > order[0],
            "碰撞消息（t/5）的确认不得早于旧同标识消息（t/0）"
        );
        assert_eq!(broker.connections(), 2);
        assert_eq!(broker.abnormal_disconnects(), 1);
        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn collision_parked_message_confirmed_within_same_connection() {
        // 同连接内包标识回绕碰撞（§31.4）：在途窗口 4，pkid 1-4 发出后
        // 第 5 条回绕撞到未确认的 pkid 1 → rumqttc 停放碰撞消息
        //（`Outgoing::AwaitAck`）。旧消息确认时事件入队顺序为
        // `Outgoing::Publish(1)`（碰撞消息写出）**在前**、
        // `Incoming::PubAck(1)`（旧消息确认）**在后**——worker 必须在
        // 写事件上解除停放并关联标识，否则碰撞消息会被下一个写事件抢占
        // 标识、永久无法确认（回归：解除停放曾在 PubAck 事件上执行，
        // 与真实顺序相反，见 §31.4）。本测试不依赖断线：同一连接内
        // 完成停放 → 恢复 → 确认全程。
        let broker = MockBroker::start().await;
        // 挂起第 1 个 PUBLISH 的 PUBACK：槽位 1 保持占用，2-4 正常确认
        // 后第 5 条回绕撞上槽位 1 触发碰撞。
        let release = broker.hold_puback(1);
        let mut config = test_config(&broker, "client-col");
        config.publish_capacity = 6;
        config.max_inflight = 4;
        let client = MqttClient::spawn(config).unwrap();

        let mut receipts = Vec::new();
        for i in 0..5 {
            receipts.push(client.publish(&format!("t/{i}"), &[i as u8]).await.unwrap());
        }

        // 碰撞消息（t/4）被停放、未写出：broker 只能收到前 4 条。
        wait_until(|| broker.publishes().len() == 4).await;
        // 短暂等待，保证 AwaitAck 已被 worker 处理（停放完成）。
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 放行第 1 条确认：rumqttc 结算槽位 1，碰撞消息以 pkid 1 写出
        //（`Outgoing::Publish(1)` 先入队、解除停放并关联），随后
        // `Incoming::PubAck(1)` 结算旧消息；broker 再收到 t/4 并确认。
        release.store(true, std::sync::atomic::Ordering::Release);
        let resolve_order = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = receipts
            .into_iter()
            .enumerate()
            .map(|(i, receipt)| {
                let seq = resolve_order.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(Duration::from_secs(5), receipt.acked())
                        .await
                        .expect("确认不得挂起");
                    (
                        i,
                        seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                        result,
                    )
                })
            })
            .collect();
        let mut order = [usize::MAX; 5];
        for handle in handles {
            let (i, n, result) = handle.await.unwrap();
            assert!(
                result.is_ok(),
                "t/{i} 碰撞恢复后必须确认成功，实际 {result:?}"
            );
            order[i] = n;
        }
        assert!(
            order[4] > order[0],
            "碰撞消息（t/4）的确认不得早于旧同标识消息（t/0）"
        );
        assert_eq!(
            broker.publishes().len(),
            5,
            "碰撞消息必须最终写出并被 broker 收到"
        );
        assert_eq!(
            broker.connections(),
            1,
            "同连接内完成碰撞恢复，不得触发重连"
        );
        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[test]
    fn online_republish_progress_resumes_where_left_off() {
        // 设备数（5）超过容量（2）时，重发必须从上次断点继续（队首
        // 推进）：每次从头遍历会让尾部设备永久遗漏（§31.1）。
        let mut needs = true;
        let mut queue: VecDeque<(String, String)> = (0..5)
            .map(|i| (format!("site-{i}"), format!("dev-{i}")))
            .collect();
        let mut pending: VecDeque<PublishRequest> = VecDeque::new();
        let mut seen: Vec<String> = Vec::new();

        // 每轮只推进 2 台（容量上限），标记保持；中间模拟转发清空
        // pending（与 worker 循环一致）。
        step_online_republish(&mut queue, &mut pending, 2, &mut needs);
        assert!(needs);
        assert_eq!(pending.len(), 2);
        seen.extend(pending.drain(..).map(|r| r.topic));
        step_online_republish(&mut queue, &mut pending, 2, &mut needs);
        assert!(needs);
        assert_eq!(pending.len(), 2);
        seen.extend(pending.drain(..).map(|r| r.topic));
        // 最后一轮推进剩余 1 台并清空标记。
        step_online_republish(&mut queue, &mut pending, 8, &mut needs);
        assert!(!needs);
        assert_eq!(pending.len(), 1);
        seen.extend(pending.drain(..).map(|r| r.topic));
        assert!(queue.is_empty(), "待重发队列必须清空");

        // 无重复、无遗漏：每台设备恰好入队一次。
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 5, "重发请求不得重复");

        // 入队的是重新生成的在线状态（online + 新时间戳），retain 且带
        // 设备标识。
        let mut queue: VecDeque<(String, String)> = (0..5)
            .map(|i| (format!("site-{i}"), format!("dev-{i}")))
            .collect();
        let mut pending: VecDeque<PublishRequest> = VecDeque::new();
        step_online_republish(&mut queue, &mut pending, 8, &mut needs);
        for (i, req) in pending.iter().enumerate() {
            let ids = req.status_ids.as_ref().expect("在线状态必须带设备标识");
            assert_eq!(ids.1, format!("dev-{i}"));
            assert_eq!(req.topic, format!("forgelink/v1/status/site-{i}/dev-{i}"));
            assert!(req.retain, "重发在线状态必须 retain（§31.1）");
            let payload: serde_json::Value = serde_json::from_slice(&req.payload).unwrap();
            assert_eq!(payload["status"], "online");
            assert!(payload["sent_at_ns"].as_u64().unwrap() > 0);
        }
    }

    #[test]
    fn rebuild_online_republish_restarts_full_cycle_each_disconnect() {
        // 断线重建完整重发周期（§31.1）：清空后用 `last_online` 全集
        // 填充。二次断线时，上一轮周期中已确认推送（已从队列弹出）的
        // 设备必须重新入队——否则其 LWT 已发布，重连后永久离线。
        let mut queue: VecDeque<(String, String)> = VecDeque::new();
        let last_online: BTreeSet<(String, String)> = [
            ("site-a".into(), "dev-a".into()),
            ("site-a".into(), "dev-b".into()),
            ("site-a".into(), "dev-c".into()),
        ]
        .into_iter()
        .collect();

        // 首次断线：周期从空队列启动，填充全集。
        rebuild_online_republish(&mut queue, &last_online);
        let snapshot = |q: &VecDeque<(String, String)>| {
            let mut got: Vec<(String, String)> = q.iter().cloned().collect();
            got.sort_unstable();
            got
        };
        let full: Vec<(String, String)> = vec![
            ("site-a".into(), "dev-a".into()),
            ("site-a".into(), "dev-b".into()),
            ("site-a".into(), "dev-c".into()),
        ];
        assert_eq!(snapshot(&queue), full);

        // 模拟上一轮推进了 dev-a、dev-b（已确认，弹出队列），dev-c
        // 仍在队列中；二次断线重建必须重新得到全集（dev-a/dev-b 不得
        // 被遗漏在重发周期外）。
        queue.pop_front();
        queue.pop_front();
        assert_eq!(queue.len(), 1, "前置：dev-c 仍在队列中");
        rebuild_online_republish(&mut queue, &last_online);
        let mut again: Vec<(String, String)> = queue.iter().cloned().collect();
        again.sort_unstable();
        assert_eq!(
            again, full,
            "每次断线必须重建完整重发周期（已确认设备重新入队）"
        );
    }

    #[test]
    fn reset_pkids_orders_leftover_then_inflight_then_channel() {
        // 二次断线（重发周期进行中）时的重排顺序（§31.4）：rumqttc
        // `EventLoop::clean` 的重发顺序为 [上一轮遗留未重发] + [本轮在途
        // (pkid 槽位)] + [本轮新转发通道]，`reset_pkids` 必须与之完全
        // 一致，否则重发 PUBACK 会关联到错误的请求（WAL 提前删除未确认
        // 记录）。`ack_tx` 作为条目身份：向它发送后只有对应接收器的
        // `try_recv` 返回 `Ok`（已消费的返回 `Closed`，未发送的返回
        // `Empty`）。
        let mut receivers = Vec::new();
        let mut entry = |pkid: Option<u16>, leftover: bool| -> AwaitingAck {
            let (ack_tx, ack_rx) = oneshot::channel();
            receivers.push(ack_rx);
            AwaitingAck {
                pkid,
                leftover,
                parked_pkid: None,
                ack_tx,
                accepted_at: Instant::now(),
            }
        };
        // 模拟二次断线瞬间的队列（插入顺序刻意打乱）：
        let mut awaiting: VecDeque<AwaitingAck> = VecDeque::new();
        awaiting.push_back(entry(Some(5), true)); // 0: 本轮在途（pkid > last_puback）
        awaiting.push_back(entry(None, true)); //    1: 上一轮遗留、尚未重发
        awaiting.push_back(entry(Some(2), true)); // 2: 本轮在途（pkid <= last_puback）
        awaiting.push_back(entry(None, false)); //   3: 本轮新转发（仍在通道）
        awaiting.push_back(entry(None, true)); //    4: 上一轮遗留、尚未重发
        awaiting.push_back(entry(Some(4), true)); // 5: 本轮在途（pkid > last_puback）

        reset_pkids(&mut awaiting, 3);

        // 期望顺序：遗留未重发（1、4）-> 在途 pkid > 3 升序（5、0）->
        // 在途 pkid <= 3（2）-> 本轮通道（3）；且包标识全部清空、全部
        // 转为遗留（下次断线时排序规则不变）。
        let expected = [1usize, 4, 5, 0, 2, 3];
        assert_eq!(awaiting.len(), expected.len());
        for (pos, tag) in expected.into_iter().enumerate() {
            let resolved: Vec<usize> = receivers
                .iter_mut()
                .enumerate()
                .filter_map(|(i, rx)| rx.try_recv().is_ok().then_some(i))
                .collect();
            assert!(resolved.is_empty(), "前置：第 {pos} 位结算前无已就绪收据");
            let entry = awaiting.pop_front().unwrap();
            assert_eq!(entry.pkid, None, "第 {pos} 位（原 {tag} 号）包标识必须清空");
            assert!(entry.leftover, "第 {pos} 位（原 {tag} 号）必须转为遗留标记");
            let _ = entry.ack_tx.send(Ok(()));
            let resolved: Vec<usize> = receivers
                .iter_mut()
                .enumerate()
                .filter_map(|(i, rx)| rx.try_recv().is_ok().then_some(i))
                .collect();
            assert_eq!(
                resolved,
                [tag],
                "第 {pos} 位必须是原 {tag} 号条目（重排后顺序须与 rumqttc 重发顺序一致）"
            );
        }
    }

    #[test]
    fn collision_parked_entry_skips_foreign_pkids_then_gets_own_write() {
        // 包标识碰撞（§31.4）：rumqttc 把碰撞消息停放在碰撞槽（`AwaitAck`
        // 事件），旧同标识消息确认后才以同一 pkid 写出。停放条目不得被
        // 后续写事件抢占标识（否则 PUBACK 关联错位、WAL 提前删除未确认
        // 记录）。
        let entry = |pkid: Option<u16>, parked_pkid: Option<u16>| AwaitingAck {
            pkid,
            leftover: true,
            parked_pkid,
            ack_tx: oneshot::channel().0,
            accepted_at: Instant::now(),
        };
        let mut awaiting: VecDeque<AwaitingAck> = VecDeque::new();
        awaiting.push_back(entry(Some(1), None)); // A：已写出（pkid 1，未确认）
        awaiting.push_back(entry(None, None)); //    E：碰撞消息（将被停放）
        awaiting.push_back(entry(None, None)); //    F：通道中新转发
        awaiting.push_back(entry(None, None)); //    G：通道中新转发

        let mut collision_pkid = None;
        let mut collision_recovered = false;
        let mut collision_reset_pending = false;
        // 碰撞：rumqttc 试图给 E 分配 pkid 1（回绕）失败 → AwaitAck(1)。
        on_await_ack_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(collision_pkid, Some(1));
        assert_eq!(
            awaiting[1].parked_pkid,
            Some(1),
            "E 必须被停放并配对 pkid 1"
        );

        // 后续写事件（F、G 的 pkid）不得抢占停放条目。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            2,
        ); // F 的写
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            3,
        ); // G 的写
        assert_eq!(awaiting[1].pkid, None, "停放条目不得被提前分配标识");
        assert_eq!(awaiting[2].pkid, Some(2));
        assert_eq!(awaiting[3].pkid, Some(3));

        // 旧同标识消息确认（rumqttc `handle_incoming_puback` 处理时，
        // 事件入队顺序为：先 `Outgoing::Publish(1)`——碰撞消息写出；
        // 后 `Incoming::PubAck(1)`——旧消息确认）。解除停放必须发生在
        // 写事件上：Publish(1) 到达时解除停放并关联 E，PubAck(1) 只
        // 结算旧消息 A。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            1,
        );
        assert_eq!(collision_pkid, None);
        assert_eq!(awaiting[1].parked_pkid, None, "写事件到达后 E 必须解除停放");
        assert_eq!(awaiting[1].pkid, Some(1));

        // PUBACK 归属：pkid 1 的确认先给 A（旧消息），再给 E（碰撞消息，
        // broker 对碰撞消息的后续写出再发一次同标识 PUBACK）。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(awaiting.len(), 3, "A 必须被结算");
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(awaiting.len(), 2, "E 必须被结算");
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            2,
            &MqttMetrics::noop(),
        );
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            3,
            &MqttMetrics::noop(),
        );
        assert!(awaiting.is_empty(), "全部请求必须结算");
    }

    #[test]
    fn collision_pending_resend_must_not_unpark_before_ack() {
        // 碰撞未决时断线重连（§31.4）：rumqttc 重发保留原 pkid，重连后
        // 首个同标识写事件是旧消息的**重发**——碰撞尚未解决（旧消息未
        // 确认、碰撞槽未清），解除停放会让碰撞消息被后续写事件抢占标识、
        // PUBACK 关联错位（回归：`unpark_on_publish` 曾把重发写事件误判
        // 为碰撞恢复写而提前解除停放）。事件序（worker 视角）：
        //   Publish(1)[重发 A] -> ... -> PubAck(1)[A 确认] ->
        //   Publish(1)[碰撞恢复写 E] -> PubAck(1)[E 确认]
        let entry = |pkid: Option<u16>, parked_pkid: Option<u16>| AwaitingAck {
            pkid,
            leftover: false,
            parked_pkid,
            ack_tx: oneshot::channel().0,
            accepted_at: Instant::now(),
        };
        let mut awaiting: VecDeque<AwaitingAck> = VecDeque::new();
        awaiting.push_back(entry(Some(1), None)); // A：在途（pkid 1，未确认）
        awaiting.push_back(entry(None, None)); //    E：碰撞消息（将被停放）
        awaiting.push_back(entry(None, None)); //    F：通道中新转发

        let mut collision_pkid = None;
        let mut collision_recovered = false;
        let mut collision_reset_pending = false;
        on_await_ack_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(
            awaiting[1].parked_pkid,
            Some(1),
            "E 必须被停放并配对 pkid 1"
        );

        // 断线：碰撞未决期间断线 → 置重发标记（主循环 Err 分支逻辑）。
        reset_pkids(&mut awaiting, 0);
        if collision_pkid.is_some() {
            collision_recovered = false;
            collision_reset_pending = true;
        }
        assert!(collision_reset_pending);

        // 重连后重发 A：Publish(1) 是重发写事件——不得解除停放。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            1,
        );
        assert_eq!(collision_pkid, Some(1), "碰撞未解决，碰撞标识必须保留");
        assert_eq!(awaiting[1].parked_pkid, Some(1), "重发写事件不得解除停放");
        assert!(
            !collision_reset_pending,
            "重发标记必须在首个同标识事件上消费"
        );
        assert_eq!(awaiting[0].pkid, Some(1), "重发写事件必须关联到 A");
        assert_eq!(awaiting[1].pkid, None, "停放条目不得被重发写事件抢占");

        // 重发期间其他新写事件（重连后的新发布）也不得抢占停放条目。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            2,
        );
        assert_eq!(awaiting[2].pkid, Some(2), "新写事件关联到 F");
        assert_eq!(awaiting[1].pkid, None, "停放条目仍不得被抢占");

        // A 确认：碰撞进入恢复流程（rumqttc 将随后恢复写出 E）。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert!(collision_recovered, "旧消息确认后必须记录恢复状态");
        assert_eq!(awaiting.len(), 2, "A 必须被结算");
        assert_eq!(awaiting[0].parked_pkid, Some(1), "E 仍在停放（恢复写未到）");

        // 碰撞恢复写：解除停放并以同一 pkid 关联 E。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            1,
        );
        assert_eq!(collision_pkid, None, "恢复写到达后碰撞结束");
        assert_eq!(awaiting[0].parked_pkid, None, "恢复写必须解除停放");
        assert_eq!(awaiting[0].pkid, Some(1), "E 必须以同一 pkid 关联");

        // E 确认：结算。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(awaiting.len(), 1, "E 必须被结算");
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            2,
            &MqttMetrics::noop(),
        );
        assert!(awaiting.is_empty(), "全部请求必须结算");
    }

    #[test]
    fn collision_parked_state_survives_reset_pkids() {
        // 碰撞未决时断线（§31.4）：rumqttc 的碰撞槽不被 `clean()` 清除，
        // 停放状态必须跨断线保留——`reset_pkids` 重排后停放条目仍不得
        // 被重发 / 新写事件抢占标识；旧同标识消息确认后解除停放并关联
        // 到紧随其后的写事件。
        let entry = |pkid: Option<u16>, parked_pkid: Option<u16>| AwaitingAck {
            pkid,
            leftover: false,
            parked_pkid,
            ack_tx: oneshot::channel().0,
            accepted_at: Instant::now(),
        };
        let mut awaiting: VecDeque<AwaitingAck> = VecDeque::new();
        awaiting.push_back(entry(Some(1), None)); // A：在途（pkid 1，未确认）
        awaiting.push_back(entry(None, None)); //    E：碰撞消息（将被停放）
        awaiting.push_back(entry(None, None)); //    F：通道中新转发

        let mut collision_pkid = None;
        let mut collision_recovered = false;
        let mut collision_reset_pending = false;
        on_await_ack_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );

        reset_pkids(&mut awaiting, 0);
        if collision_pkid.is_some() {
            collision_recovered = false;
            collision_reset_pending = true;
        }
        let parked_index = awaiting
            .iter()
            .position(|e| e.parked_pkid.is_some())
            .expect("停放条目");
        assert_eq!(awaiting[parked_index].pkid, None, "停放条目包标识被清空");
        // 重连后的写事件（重发 / 新转发）不得抢占停放条目。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            1,
        );
        assert_eq!(awaiting[0].pkid, Some(1), "重发写事件关联到 A");
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            4,
        );
        assert_eq!(awaiting[2].pkid, Some(4));
        assert_eq!(
            awaiting[parked_index].pkid, None,
            "重排后停放条目仍不得被抢占标识"
        );

        // 重连后旧消息（pkid 1）确认：rumqttc 先入队 Outgoing::Publish(1)
        //（碰撞消息写出）——在此写事件上解除停放并关联；Incoming::PubAck(1)
        // 紧随其后，只结算旧消息。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(awaiting.len(), 2, "A 必须被结算");
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            1,
        );
        assert_eq!(collision_pkid, None);
        assert_eq!(
            awaiting[parked_index - 1].parked_pkid,
            None,
            "写事件到达后必须解除停放"
        );
        assert_eq!(
            awaiting[parked_index - 1].pkid,
            Some(1),
            "碰撞消息必须以同一 pkid 关联"
        );
    }

    #[test]
    fn second_pending_collision_fails_overwritten_entry_and_switches() {
        // 第二个未决碰撞（§31.4）：旧碰撞尚未恢复时 pkid 再次回绕，
        // rumqttc 的单碰撞槽被**覆盖**——旧碰撞消息永久丢失（其恢复写
        // 永远不会出现）。客户端必须**立即失败结算**旧停放条目（否则
        // 连接保持健康时 `acked()` 永久等待、WAL 无法重试），并把
        // `collision_pkid` 切换到 rumqttc 实际保存的新碰撞；新碰撞消息
        // 配对停放，恢复写按配对解除。
        let entry = |pkid: Option<u16>, parked_pkid: Option<u16>| AwaitingAck {
            pkid,
            leftover: false,
            parked_pkid,
            ack_tx: oneshot::channel().0,
            accepted_at: Instant::now(),
        };
        let mut awaiting: VecDeque<AwaitingAck> = VecDeque::new();
        awaiting.push_back(entry(Some(1), None)); // A：在途（pkid 1，未确认）
        awaiting.push_back(entry(Some(2), None)); // B：在途（pkid 2，未确认）
        // E：第一个碰撞消息（pkid 1）——将被第二个碰撞覆盖并失败结算。
        let (e_ack_tx, mut e_ack_rx) = oneshot::channel();
        awaiting.push_back(AwaitingAck {
            pkid: None,
            leftover: false,
            parked_pkid: None,
            ack_tx: e_ack_tx,
            accepted_at: Instant::now(),
        });
        awaiting.push_back(entry(None, None)); //    F：第二个碰撞消息（pkid 2）
        awaiting.push_back(entry(None, None)); //    G：通道中新转发

        let mut collision_pkid = None;
        let mut collision_recovered = false;
        let mut collision_reset_pending = false;

        // 第一个碰撞：E 配对 pkid 1。
        on_await_ack_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert_eq!(collision_pkid, Some(1), "首个碰撞记录标识");
        assert_eq!(awaiting[2].parked_pkid, Some(1), "E 配对 pkid 1");

        // 第二个碰撞（rumqttc 覆盖碰撞槽）：E 必须**立即失败结算**
        //（消息已不可能写出或确认），碰撞标识切换到新碰撞 pkid 2，
        // F 配对停放。
        on_await_ack_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            2,
            &MqttMetrics::noop(),
        );
        assert_eq!(
            match e_ack_rx.try_recv() {
                Ok(Err(MqttClientError::CollisionOverwritten)) => "overwritten",
                other => panic!("E 必须立即收到 CollisionOverwritten 失败结算，实际 {other:?}"),
            },
            "overwritten",
            "E 的 ack 通道必须已结算"
        );
        assert_eq!(collision_pkid, Some(2), "碰撞标识切换到新碰撞");
        assert_eq!(awaiting.len(), 4, "E 必须被移除");
        assert!(
            !awaiting.iter().any(|e| e.parked_pkid == Some(1)),
            "旧配对条目不得残留"
        );

        // 普通写事件不得抢占停放条目。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            3,
        );
        // 队列：[A(1), B(2), F(park2), G]
        assert_eq!(awaiting[3].pkid, Some(3), "G 正常关联");
        assert_eq!(awaiting[2].pkid, None, "F 不得被抢占");

        // A 确认：碰撞标识已是新碰撞（2），不进入其恢复流程，仅结算 A。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            1,
            &MqttMetrics::noop(),
        );
        assert!(!collision_recovered, "A 的确认不匹配当前碰撞标识");
        assert_eq!(awaiting.len(), 3, "A 必须被结算");
        // 队列：[B(2), F(park2), G(3)]

        // B 确认（槽位 2 的原持有者）：匹配当前碰撞标识——rumqttc 将
        // 恢复写出 F，记录"恢复写将至"标记。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            2,
            &MqttMetrics::noop(),
        );
        assert!(collision_recovered, "B 确认后进入恢复流程");
        assert_eq!(awaiting.len(), 2, "B 必须被结算");

        // F 的恢复写：解除配对 pkid 2 的 F 并以同一标识关联。
        on_publish_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            &mut collision_reset_pending,
            2,
        );
        assert_eq!(collision_pkid, None, "碰撞在恢复写后结束");
        assert_eq!(awaiting[0].parked_pkid, None, "F 必须被解除");
        assert_eq!(awaiting[0].pkid, Some(2), "F 以同一 pkid 关联");

        // PubAck(2)（broker 对 F 的确认）：结算 F。
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            2,
            &MqttMetrics::noop(),
        );
        assert_eq!(awaiting.len(), 1, "F 必须被结算");
        on_puback_event(
            &mut awaiting,
            &mut collision_pkid,
            &mut collision_recovered,
            3,
            &MqttMetrics::noop(),
        );
        assert!(awaiting.is_empty(), "全部请求必须结算");
    }

    #[tokio::test]
    async fn shutdown_settles_puback_received_during_drain() {
        let broker = MockBroker::start().await;
        // 延迟 PUBACK：发布已送达 broker，但确认恰在停机排空期间到达。
        broker.set_puback_delay(Duration::from_millis(150));
        let client = MqttClient::spawn(test_config(&broker, "client-m")).unwrap();

        let receipt = client.publish("t/drain", b"x").await.unwrap();
        wait_until(|| broker.publishes().len() == 1).await;

        client.shutdown().await.unwrap();

        // 排空期间收到的 PUBACK 必须按成功结算（§31.4：已确认的消息
        // 不得结算为 Closed，否则 WAL 会重复补传）。
        let drain_result = receipt.acked().await;
        assert!(
            drain_result.is_ok(),
            "停机排空期间已确认的发布必须成功结算，实际 {drain_result:?}"
        );

        broker.stop().await;
    }

    #[tokio::test]
    async fn shutdown_settles_parked_collision_message() {
        // 停机排空与包标识碰撞（§31.4）：碰撞停放的消息在停机各阶段
        //（离线排空 / DISCONNECT 写出 / 在途结算）必须与主循环同样
        // 处理——碰撞恢复写解除停放并关联标识、PUBACK 按真实归属结算，
        // 否则停放条目永远无法关联 pkid、被错误结算为 Closed（WAL
        // 重复补传已送达消息）。
        let broker = MockBroker::start().await;
        // 挂起第 1 个 PUBACK：槽位 1 保持占用，pkid 1-4 发出后第 5 条
        // 回绕撞上槽位 1 → 碰撞停放（不写出，broker 只收到前 4 条）。
        // 注：只发 5 条——rumqttc 的单碰撞槽会被第二次碰撞覆盖（pkid
        // 再次回绕时），碰撞消息将永久丢失，无法验证停机结算路径。
        let release = broker.hold_puback(1);
        let mut config = test_config(&broker, "client-shutcol");
        config.publish_capacity = 8;
        config.max_inflight = 4;
        let client = MqttClient::spawn(config).unwrap();

        let mut receipts = Vec::new();
        for i in 0..5 {
            receipts.push(client.publish(&format!("t/{i}"), &[i as u8]).await.unwrap());
        }
        wait_until(|| broker.publishes().len() == 4).await;
        // 短暂等待，保证 AwaitAck 已被 worker 处理（停放完成）。
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 停机；排空开始后放行挂起的确认——碰撞恢复写与其余 PUBACK
        // 都在停机阶段处理。
        let shutdown_handle = tokio::spawn(async move { client.shutdown().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(50)).await;
        release.store(true, std::sync::atomic::Ordering::Release);
        shutdown_handle.await.unwrap();

        let mut results = Vec::new();
        for receipt in receipts {
            let result = tokio::time::timeout(Duration::from_secs(5), receipt.acked())
                .await
                .expect("确认不得挂起");
            results.push(result);
        }
        for (i, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "t/{i} 停机排空期间必须按真实归属结算，实际 {result:?}"
            );
        }
        broker.stop().await;
    }

    #[test]
    fn extract_spki_key_supports_long_form_lengths() {
        // 合成 SPKI：SEQUENCE { SEQUENCE { OID… }, BIT STRING { 0 + key } }。
        // 公钥取 256 字节（>127），强制外层 SEQUENCE 与 BIT STRING 使用
        // DER 长格式长度字段（RSA 2048 等大 SPKI 的真实形态）；旧实现
        // 固定跳过 2 字节会解析错误。
        let alg = vec![0x30, 0x03, 0x01, 0x01, 0x07]; // SEQUENCE { INTEGER 7 }
        let key: Vec<u8> = (0..=255u8).collect();
        let mut bit_string = vec![0x03];
        push_der_len(&mut bit_string, 1 + key.len());
        bit_string.push(0); // 未用位计数
        bit_string.extend(&key);
        let mut alg_seq = vec![0x30];
        push_der_len(&mut alg_seq, alg.len());
        alg_seq.extend(&alg);
        let mut spki = vec![0x30];
        push_der_len(&mut spki, alg_seq.len() + bit_string.len());
        spki.extend(&alg_seq);
        spki.extend(&bit_string);

        assert_eq!(extract_spki_key(&spki), Some(key.clone()));
        // 损坏输入必须返回 None（不 panic）。
        assert_eq!(extract_spki_key(b""), None);
        assert_eq!(extract_spki_key(&[0x30, 0x81]), None);
    }

    /// 构造 DER 长度字段（<128 用短格式，否则长格式）。
    fn push_der_len(out: &mut Vec<u8>, len: usize) {
        if len < 128 {
            out.push(len as u8);
        } else {
            let mut bytes = vec![];
            let mut rest = len;
            while rest > 0 {
                bytes.push((rest & 0xFF) as u8);
                rest >>= 8;
            }
            bytes.reverse();
            out.push(0x80 | bytes.len() as u8);
            out.extend(bytes);
        }
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let min = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        assert_eq!(backoff_delay(min, max, 1), Duration::from_secs(1));
        assert_eq!(backoff_delay(min, max, 2), Duration::from_secs(2));
        assert_eq!(backoff_delay(min, max, 3), Duration::from_secs(4));
        assert_eq!(backoff_delay(min, max, 5), Duration::from_secs(16));
        assert_eq!(
            backoff_delay(min, max, 6),
            Duration::from_secs(30),
            "上限 30s"
        );
        assert_eq!(backoff_delay(min, max, 100), Duration::from_secs(30));
        assert_eq!(
            backoff_delay(DEFAULT_RECONNECT_MIN_DELAY, DEFAULT_RECONNECT_MAX_DELAY, 1),
            Duration::from_secs(1)
        );
    }

    /// 生成测试证书：CA、服务端证书（SAN localhost + 127.0.0.1，serverAuth）、
    /// 客户端证书（SAN localhost，clientAuth），均由同一 CA 签发。
    #[allow(clippy::type_complexity)]
    fn test_certs() -> (
        Vec<u8>, // ca pem
        Vec<u8>, // ca der
        Vec<u8>, // server cert der
        Vec<u8>, // server key der (pkcs8)
        Vec<u8>, // client cert pem
        Vec<u8>, // client key pem
    ) {
        use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair};

        let mut ca_params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "forgelink-test-ca");
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();

        // 测试客户端固定以 127.0.0.1 连接 broker，服务端证书 SAN 必须覆盖
        //（rustls 校验服务器身份，§90.1）。
        let mut server_params =
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()]).unwrap();
        server_params
            .distinguished_name
            .push(DnType::CommonName, "forgelink-server");
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

        let mut client_params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        client_params
            .distinguished_name
            .push(DnType::CommonName, "forgelink-client");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate().unwrap();
        let client = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();

        (
            ca.pem().into_bytes(),
            ca.der().to_vec(),
            server.der().to_vec(),
            server_key.serialize_der(),
            client.pem().into_bytes(),
            client_key.serialize_pem().into_bytes(),
        )
    }

    #[tokio::test]
    async fn tls_server_auth_handshake() {
        let (ca_pem, _, server_cert_der, server_key_der, _, _) = test_certs();
        let broker = MockBroker::start_tls(server_cert_der, server_key_der, None).await;
        let mut config = test_config(&broker, "client-tls-a");
        config.tls = TlsMode::ServerAuth { ca_pem };
        let client = MqttClient::spawn(config).unwrap();

        client
            .publish("t/tls", b"secure")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        wait_until(|| broker.publishes().len() == 1).await;
        assert_eq!(broker.publishes()[0].payload, b"secure");

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[tokio::test]
    async fn tls_mutual_auth_handshake() {
        let (ca_pem, ca_der, server_cert_der, server_key_der, client_cert_pem, client_key_pem) =
            test_certs();
        let broker = MockBroker::start_tls(server_cert_der, server_key_der, Some(ca_der)).await;
        let mut config = test_config(&broker, "client-tls-b");
        config.tls = TlsMode::MutualTls {
            ca_pem,
            client_cert_pem,
            client_key_pem,
        };
        let client = MqttClient::spawn(config).unwrap();

        client
            .publish("t/mtls", b"mtls")
            .await
            .unwrap()
            .acked()
            .await
            .unwrap();
        wait_until(|| broker.publishes().len() == 1).await;
        assert_eq!(broker.publishes()[0].payload, b"mtls");

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[test]
    fn spawn_rejects_corrupt_tls_materials() {
        // 损坏证书 / 无效私钥必须在 spawn 阶段返回 InvalidConfig
        //（§90.1），不能"spawn 成功、连接无限重试"。
        let (ca_pem, _, _, _, client_cert_pem, client_key_pem) = test_certs();

        let mut config = MqttClientConfig::new("tls-bad-a", "127.0.0.1", 1883);
        config.tls = TlsMode::ServerAuth {
            ca_pem: b"-----BEGIN CERTIFICATE-----\nnot-a-cert\n-----END CERTIFICATE-----".to_vec(),
        };
        let err = MqttClient::spawn(config).unwrap_err();
        assert!(
            matches!(err, MqttClientError::InvalidConfig { .. }),
            "损坏 CA 必须拒绝: {err:?}"
        );

        let mut config = MqttClientConfig::new("tls-bad-b", "127.0.0.1", 1883);
        config.tls = TlsMode::MutualTls {
            ca_pem,
            client_cert_pem: b"garbage".to_vec(),
            client_key_pem,
        };
        let err = MqttClient::spawn(config).unwrap_err();
        assert!(
            matches!(err, MqttClientError::InvalidConfig { .. }),
            "损坏客户端证书必须拒绝: {err:?}"
        );

        let mut config = MqttClientConfig::new("tls-bad-c", "127.0.0.1", 1883);
        config.tls = TlsMode::MutualTls {
            ca_pem: b"ca".to_vec(),
            client_cert_pem,
            client_key_pem: b"not-a-key".to_vec(),
        };
        let err = MqttClient::spawn(config).unwrap_err();
        assert!(
            matches!(err, MqttClientError::InvalidConfig { .. }),
            "无效私钥必须拒绝: {err:?}"
        );
    }

    #[test]
    fn spawn_rejects_key_cert_mismatch() {
        // 证书与私钥不匹配（拿别的密钥对签名）必须在 spawn 阶段拦截：
        // 握手阶段被服务端拒绝会造成永久性重连（§90.1）。
        let (ca_pem, _, _, _, client_cert_pem, _) = test_certs();
        // 独立生成一把与客户端证书无关的密钥（PEM）。
        let other_key = rcgen::KeyPair::generate()
            .unwrap()
            .serialize_pem()
            .into_bytes();
        let mut config = MqttClientConfig::new("tls-bad-d", "127.0.0.1", 1883);
        config.tls = TlsMode::MutualTls {
            ca_pem,
            client_cert_pem,
            client_key_pem: other_key,
        };
        let err = MqttClient::spawn(config).unwrap_err();
        assert!(
            matches!(err, MqttClientError::InvalidConfig { .. }),
            "证书与私钥不匹配必须拒绝: {err:?}"
        );
    }

    #[tokio::test]
    async fn tls_rejects_unknown_ca() {
        // 客户端信任的 CA 与 broker 证书签发 CA 不同：TLS 握手必须失败
        //（fail closed，§90.1），不能回退明文。
        let (_ca_pem, _, server_cert_der, server_key_der, _, _) = test_certs();
        let broker = MockBroker::start_tls(server_cert_der, server_key_der, None).await;
        let mut config = test_config(&broker, "client-tls-c");
        let other_ca = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        config.tls = TlsMode::ServerAuth {
            ca_pem: other_ca.cert.pem().into_bytes(),
        };
        config.reconnect_min_delay = Duration::from_millis(50);
        config.reconnect_max_delay = Duration::from_millis(100);
        config.max_reconnect_retries = Some(2);
        let client = MqttClient::spawn(config).unwrap();

        // 未知 CA 无法完成握手（§90.1 fail closed）：重连失败达到上限后
        // 任务退出，发布最终必然失败。
        let result = wait_publish_failure(&client).await;
        assert!(
            matches!(
                result,
                Err(MqttClientError::Disconnected { .. }) | Err(MqttClientError::Closed)
            ),
            "未知 CA 必须握手失败，实际 {result:?}"
        );
        assert_eq!(broker.publishes().len(), 0, "TLS 失败不得产生任何发布");

        client.shutdown().await.unwrap();
        broker.stop().await;
    }

    #[test]
    fn publish_batch_maps_batch_fields() {
        // 纯函数断言：telemetry_topic 与批字段映射（无需 broker）。
        let batch = make_batch("plant-a", "cnc-01", 7);
        let topic = telemetry_topic(&batch.site_id, &batch.device_id).unwrap();
        assert_eq!(topic, "forgelink/v1/telemetry/plant-a/cnc-01");
    }

    // ---- 指标埋点（§34.2.1） ---------------------------------------------------

    use crate::metrics::metric_names;
    use metrics::MetricValue;

    /// 注入 registry 后：PUBACK 确认计入 `mqtt_published_total`，在途
    /// gauge 增减配对（入队 +1、确认 -1）。
    #[tokio::test]
    async fn metrics_count_published_and_inflight_pairing() {
        let registry = std::sync::Arc::new(metrics::MetricsRegistry::new());
        let broker = MockBroker::start().await;
        let client = MqttClient::spawn_with_metrics(
            test_config(&broker, "client-metrics-a"),
            registry.clone(),
        )
        .unwrap();

        let snap0 = registry.snapshot();
        // 装配期即注册全部指标名（句柄恒存在）：未发布时在途计数为 0。
        assert_eq!(
            snap0.get(metric_names::MQTT_INFLIGHT_GAUGE),
            Some(&MetricValue::Gauge(0)),
            "未发布前在途计数必须为 0"
        );

        let first = client.publish("t/m1", b"one").await.unwrap();
        // 请求已进入 worker 队列（in-flight 曾 +1）；确认可能极快完成，
        // 不在中间态上断言，只验证最终配对。
        first.acked().await.unwrap();
        let second = client.publish("t/m2", b"two").await.unwrap();
        second.acked().await.unwrap();
        wait_until(|| broker.publishes().len() == 2).await;

        client.shutdown().await.unwrap();
        let snap = registry.snapshot();
        assert_eq!(
            snap.get(metric_names::MQTT_PUBLISHED_TOTAL),
            Some(&MetricValue::Count(2)),
            "两次 PUBACK 确认必须计数"
        );
        assert_eq!(
            snap.get(metric_names::MQTT_INFLIGHT_GAUGE),
            Some(&MetricValue::Gauge(0)),
            "全部确认后在途计数必须归零（增减配对）"
        );
        assert_eq!(
            snap.get(metric_names::MQTT_FAILED_TOTAL),
            Some(&MetricValue::Count(0))
        );
        broker.stop().await;
    }

    /// 断线重发：存在未确认在途消息的断线计入 `mqtt_redelivered_total`，
    /// 重发后确认仍按成功结算。
    #[tokio::test]
    async fn metrics_count_redelivery_after_disconnect() {
        let registry = std::sync::Arc::new(metrics::MetricsRegistry::new());
        let broker = MockBroker::start().await;
        broker.drop_connection_after_publish();
        let client = MqttClient::spawn_with_metrics(
            test_config(&broker, "client-metrics-b"),
            registry.clone(),
        )
        .unwrap();

        let receipt = client.publish("t/redeliver-m", b"payload").await.unwrap();
        wait_until(|| broker.publishes().len() == 2).await;
        receipt.acked().await.unwrap();

        client.shutdown().await.unwrap();
        let snap = registry.snapshot();
        assert_eq!(
            snap.get(metric_names::MQTT_REDELIVERED_TOTAL),
            Some(&MetricValue::Count(1)),
            "带未确认在途消息的断线必须计一次重发"
        );
        assert_eq!(
            snap.get(metric_names::MQTT_PUBLISHED_TOTAL),
            Some(&MetricValue::Count(1)),
            "重发送达后的确认只结算一次"
        );
        assert_eq!(
            snap.get(metric_names::MQTT_FAILED_TOTAL),
            Some(&MetricValue::Count(0)),
            "成功路径不得产生失败计数"
        );
        broker.stop().await;
    }

    /// 重连上限耗尽：任务退出时全部未确认请求以失败结算
    /// `mqtt_failed_total`；此后 in-flight 归零。
    #[tokio::test]
    async fn metrics_count_failures_on_reconnect_exhausted() {
        let registry = std::sync::Arc::new(metrics::MetricsRegistry::new());
        // broker 不可达（同 bounded_retries_fail_publishes 的静态端口说明）。
        let mut config = MqttClientConfig::new("client-metrics-c", "127.0.0.1", 11880);
        config.reconnect_min_delay = Duration::from_millis(50);
        config.reconnect_max_delay = Duration::from_millis(100);
        config.connect_timeout = Duration::from_secs(1);
        config.max_reconnect_retries = Some(1);
        let client = MqttClient::spawn_with_metrics(config, registry.clone()).unwrap();

        let result = wait_publish_failure(&client).await;
        assert!(result.is_err(), "前置：发布应最终失败");
        client.shutdown().await.unwrap();

        let snap = registry.snapshot();
        let failed = match snap.get(metric_names::MQTT_FAILED_TOTAL) {
            Some(MetricValue::Count(n)) => *n,
            other => panic!("失败计数应已注册，实际 {other:?}"),
        };
        assert!(failed >= 1, "任务退出时的失败结算必须计数，实际 {failed}");
        assert_eq!(
            snap.get(metric_names::MQTT_INFLIGHT_GAUGE),
            Some(&MetricValue::Gauge(0)),
            "全部失败结算后在途计数必须归零"
        );
    }
}
