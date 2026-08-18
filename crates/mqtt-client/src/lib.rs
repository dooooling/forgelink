//! mqtt-client：MQTT 北向客户端（§31 / §34 / §90.1）。
//!
//! 基于 rumqttc（MQTT 3.1.1）实现北向发布：QoS 1 发布、Topic（§31.1）、
//! PUBACK 自动处理与断线重发（§31.3 at-least-once）、自动重连（指数退避，
//! §34.3）、LWT（§31.1）、TLS / mTLS（§90.1）。
//!
//! # 契约要点
//!
//! - 主题命名空间（§31.1）：
//!   `forgelink/v1/telemetry/{site_id}/{device_id}`（QoS 1，不 retain）；
//!   `forgelink/v1/status/{site_id}/{device_id}`（QoS 1、retain，配合
//!   retained LWT 表示在线状态）。在线 / 离线载荷为 Status Envelope
//!   （[`STATUS_SCHEMA`] = `forgelink.status.v1`，契约见 §31.1）：
//!   在线由 [`MqttClient::publish_online`] 发布、离线由
//!   [`MqttClient::publish_offline`] 发布（均为真实时间戳）；LWT 代发的
//!   离线 `sent_at_ns` 固定为 0（以消费者到达时间为准）。
//! - 离线闭环（§31.1）：一个 MQTT 客户端只有一个 LWT，仅覆盖一个设备
//!   （[`WillConfig::offline_status`]）；异常断线重连后按设备重新发布
//!   在线状态（逐设备记录 + 待重发队列断点推进，单个重发队列不遗漏；
//!   QoS 1 未确认重发与断线重建周期会产生重复，消费端需按
//!   `message_id` 幂等处理）；
//!   优雅停机在 DISCONNECT 前为所有已跟踪设备显式发布 retained 离线
//!   （DISCONNECT 不触发 LWT）；异常断线无法恢复时其余设备的离线由
//!   消费端以最后刷新时间 + 超时兜底（broker 会话过期不会删除 retained
//!   在线消息）；最后刷新时间以该设备任意消息（Telemetry / Status）的
//!   最后到达时间为准，正常采集流量即持续续租。[`MqttClient::publish_offline`] 同时将设备移出在线跟踪
//!   （设备删除 / 停用），重连时不再标记在线。
//! - 发布语义（§31.3 / §31.4）：至少一次送达，允许重复；消费者按
//!   `message_id` 去重。断线时未收到 PUBACK 的 QoS 1 消息在重连后
//!   自动重发（rumqttc 0.24 重发时不置 DUP 位）。broker 乱序确认使
//!   包标识回绕撞上未确认消息时，rumqttc 发出 `Outgoing::AwaitAck`
//!   碰撞事件：客户端把碰撞消息停放（不提前分配包标识）。旧同标识
//!   消息确认时 rumqttc 的事件顺序是**先** `Outgoing::Publish`（碰撞
//!   消息写出，客户端在此事件上解除停放并关联标识）、**后**
//!   `Incoming::PubAck`（旧消息确认）——`acked()` 只对真实确认结算，
//!   WAL 不会提前删除未送达记录（§31.4）。rumqttc 单碰撞槽被第二个
//!   未决碰撞覆盖时，被覆盖的碰撞消息已不可能写出或确认：客户端立即
//!   以 [`CollisionOverwritten`](crate::MqttClientError::CollisionOverwritten)
//!   失败结算该单条发布（WAL 保留、可重试补传；客户端本身仍正常运行），
//!   并把碰撞标识切换到新碰撞。每次发布返回
//!   [`PublishReceipt`]，`acked()` 在收到 PUBACK 后返回 `Ok`——
//!   删除 WAL 记录的唯一依据（§31.4）。
//! - 重连（§34.3）：默认指数退避 1s -> 2s -> ... 上限 30s，成功连接后
//!   重置；`max_reconnect_retries = None` 时无限重试。
//! - 安全（§90.1）：生产必须使用 TLS；TLS 材料在 `spawn` 时解析验证
//!   （损坏证书 / 无效私钥 / 密钥不匹配均拒绝启动）；`TlsMode::MutualTls`
//!   用于受管模式；不支持跳过证书校验，未知 CA 握手失败（fail closed）。
//! - 可靠性为内存级：进程退出后未确认消息丢失，持久化补传由
//!   Local Buffer / WAL 承担（§31.4）。
//!
//! # 布局
//!
//! - [`MqttClientConfig`] / [`TlsMode`] / [`WillConfig`]：配置与校验。
//! - [`MqttClient`]：QoS 1 发布客户端（`spawn` / `publish` /
//!   `publish_retained` / `publish_batch` / `publish_online` /
//!   `publish_offline` / `shutdown`）。
//! - [`PublishReceipt`]：PUBACK 确认句柄（§31.4 删除 WAL 的依据）。
//! - [`telemetry_topic`] / [`status_topic`]：§31.1 主题生成。
//!
//! # 用法
//!
//! ```ignore
//! use mqtt_client::{MqttClient, MqttClientConfig, TlsMode};
//!
//! let mut config = MqttClientConfig::new("cnc-01", "broker.example.com", 8883);
//! config.tls = TlsMode::ServerAuth { ca_pem: ca_pem_bytes };
//! let client = MqttClient::spawn(config)?;
//! let receipt = client
//!     .publish("forgelink/v1/telemetry/plant-a/cnc-01", payload)
//!     .await?;
//! receipt.acked().await?; // broker 确认后返回
//! client.shutdown().await?;
//! ```

mod client;
mod config;
// Mock MQTT Broker：本 crate 测试与上层（collector 端到端测试）共用。
// 默认仅测试编译；开启 `test-utils` feature 后对外公开（§34 验收）。
#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

pub use client::{
    MqttClient, MqttClientError, PublishReceipt, STATUS_SCHEMA, status_topic, telemetry_topic,
};
pub use config::{MqttClientConfig, TlsMode, WillConfig};
pub use data_pipeline::ObservationBatch;
