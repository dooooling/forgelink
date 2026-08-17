//! mqtt-client 配置（§31.1 / §34.3 / §90.1）。

use std::fmt;
use std::time::Duration;

/// 默认心跳保活周期（30s；`Duration::ZERO` 表示禁用保活）。
pub const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(30);
/// MQTT 3.1.1 keep-alive 为 u16 秒，最大值 65535s（超出会被 rumqttc 截断）。
pub const KEEP_ALIVE_MAX_SECS: u64 = u16::MAX as u64;
/// 默认连接超时（rumqttc 以秒为单位计算连接超时，最小 1s）。
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// 默认最大报文大小（16 MiB）。
///
/// rumqttc 默认仅 10 KiB，无法容纳满批（1000 条观测）的 JSON 载荷
/// （§31.2 / §34.2），必须显式调大。
pub const DEFAULT_MAX_PACKET_SIZE: usize = 16 * 1024 * 1024;
/// 默认重连退避起始延迟（1s，§34.3 指数退避）。
pub const DEFAULT_RECONNECT_MIN_DELAY: Duration = Duration::from_secs(1);
/// 默认重连退避上限（30s，§34.3）。
pub const DEFAULT_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
/// 默认内部有界发布队列容量（背压边界，见 [`MqttClientConfig::publish_capacity`]）。
pub const DEFAULT_PUBLISH_CAPACITY: usize = 256;
/// 默认最大在途发布数（rumqttc 默认值；见 [`MqttClientConfig::max_inflight`]）。
pub const DEFAULT_MAX_INFLIGHT: u16 = 100;
/// MQTT 3.1.1 CONNECT 报文字段长度上限：`client_id` / `username` /
/// `password` / Will payload 均为 16 位长度前缀编码，超过 65535 字节
/// 会被截断并生成损坏的 CONNECT 报文，必须在 spawn 前拒绝。
pub const CONNECT_FIELD_MAX: usize = u16::MAX as usize;

/// MQTT 传输安全模式（§90.1）。
///
/// 生产部署必须使用 TLS；`None`（明文）仅限开发环境与受控内网。
/// 私钥（`client_key_pem`）为敏感材料，`Debug` 输出已脱敏
/// （安全规范：私钥不得进入日志）。
#[derive(Clone, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// 明文 TCP（仅限测试 / 内网，生产必须使用 TLS，§90.1）。
    #[default]
    None,
    /// TLS 单向认证：仅校验 broker 证书（CA PEM）。
    ServerAuth {
        /// 用于校验 broker 证书的 CA（PEM 编码）。
        ca_pem: Vec<u8>,
    },
    /// mTLS（§90.1 受管模式推荐）：校验 broker 证书并出示客户端证书。
    MutualTls {
        /// 用于校验 broker 证书的 CA（PEM 编码）。
        ca_pem: Vec<u8>,
        /// 客户端证书（PEM 编码，用于服务端校验客户端身份）。
        client_cert_pem: Vec<u8>,
        /// 客户端私钥（PEM 编码，PKCS#8 / PKCS#1 / SEC1 均可）。
        client_key_pem: Vec<u8>,
    },
}

impl fmt::Debug for TlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::ServerAuth { ca_pem } => f
                .debug_struct("ServerAuth")
                .field("ca_pem", &format_args!("{} bytes", ca_pem.len()))
                .finish(),
            Self::MutualTls {
                ca_pem,
                client_cert_pem,
                client_key_pem: _,
            } => f
                .debug_struct("MutualTls")
                .field("ca_pem", &format_args!("{} bytes", ca_pem.len()))
                .field(
                    "client_cert_pem",
                    &format_args!("{} bytes", client_cert_pem.len()),
                )
                .field("client_key_pem", &"[REDACTED]")
                .finish(),
        }
    }
}

/// MQTT Last Will（LWT）配置（§31.1）。
///
/// 客户端非正常断开（未发送 DISCONNECT 即断线）时由 broker 代为发布，
/// QoS 固定为 1。`status` 主题建议 `retain = true`，与 retained 在线
/// 状态消息配合表示设备在线 / 离线（§31.1）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WillConfig {
    /// LWT 发布主题（§31.1 建议：
    /// `forgelink/v1/status/{site_id}/{device_id}`）。
    pub topic: String,
    /// LWT 载荷。
    pub payload: Vec<u8>,
    /// 是否 retained（§31.1：`status` 使用 retained）。
    pub retain: bool,
}

impl WillConfig {
    /// 构建 §31.1 离线状态 LWT：主题为
    /// `forgelink/v1/status/{site_id}/{device_id}`（retained），载荷为
    /// 与 [`MqttClient::publish_online`](crate::MqttClient::publish_online)
    /// 在线状态同构的 Status Envelope（§32：所有消息必须显式携带
    /// schema/version）——在线与离线使用统一类型化 Envelope，消费者
    /// 无需区分消息来源。
    ///
    /// 注意：MQTT 3.1.1 的 Will 载荷在 CONNECT 时固化、由 broker 在
    /// 断连时原样发布，客户端无法预知发布时间，因此 Envelope 的
    /// `sent_at_ns` 固定为 `0`（未知，以消费者到达时间为准，§31.1）。
    ///
    /// 一个 MQTT 客户端只有一个 LWT（§31.1 契约）：多设备场景下只能
    /// 为一个设备提供离线状态，调用方须显式指定（通常为主设备）。
    ///
    /// # Errors
    ///
    /// `site_id` / `device_id` 为空或包含 `/` 时返回
    /// [`MqttClientError::InvalidTopic`](crate::MqttClientError::InvalidTopic)。
    pub fn offline_status(
        site_id: &str,
        device_id: &str,
    ) -> Result<Self, crate::client::MqttClientError> {
        let topic = crate::client::status_topic(site_id, device_id)?;
        let payload = crate::client::lwt_offline_envelope(site_id, device_id);
        Ok(Self {
            topic,
            payload,
            retain: true,
        })
    }
}

/// mqtt-client 配置（§31 / §34.3 / §90.1）。
///
/// 全部字段公开，由调用方（Collector / Edge 组装层）负责从配置文件或
/// 下发参数填充；`MqttClient::spawn` 先校验（`validate`）后创建。
/// `password` 为敏感材料，`Debug` 输出已脱敏（安全规范）。
#[derive(Clone, PartialEq, Eq)]
pub struct MqttClientConfig {
    /// MQTT 客户端标识（连接 broker 的身份，必须非空且不可为空字符串）。
    pub client_id: String,
    /// broker 主机名或 IP；TLS 模式下必须与 broker 证书 SAN 匹配
    /// （rustls 以此校验服务器身份，§90.1）。
    pub broker_host: String,
    /// broker 端口（明文默认 1883；TLS 部署常见 8883，由调用方显式指定）。
    pub broker_port: u16,
    /// 心跳保活周期（`Duration::ZERO` 禁用；非零时必须 >= 1s 且
    /// <= 65535s——MQTT 3.1.1 keep-alive 是 u16 秒，超出会被截断）。
    pub keep_alive: Duration,
    /// TCP / TLS 连接超时（以秒为单位取整，最小 1s）。
    pub connect_timeout: Duration,
    /// 最大报文大小（收发双向，须容纳满批 JSON 载荷）。
    pub max_packet_size: usize,
    /// 重连指数退避起始延迟（§34.3）。
    pub reconnect_min_delay: Duration,
    /// 重连指数退避上限（§34.3）。
    pub reconnect_max_delay: Duration,
    /// 连续连接失败后放弃重连的次数；`None` 表示无限重试（默认）。
    pub max_reconnect_retries: Option<u32>,
    /// 内部有界发布队列容量（背压边界；发布在队列满时阻塞，停机可取消）。
    pub publish_capacity: usize,
    /// 最大在途（未确认）发布数（§31.3，映射 rumqttc `set_inflight`）。
    ///
    /// rumqttc 以此值作为包标识回绕窗口：pkid 递增到该值后从 1 重新
    /// 分配，broker 乱序确认时回绕会命中未确认槽位并触发包标识碰撞
    ///（`Outgoing::AwaitAck`，由客户端停放待写消息、确认后恢复，见
    /// `crate` 契约）。必须大于 0（rumqttc 拒绝 0）。
    pub max_inflight: u16,
    /// 传输安全模式（§90.1；生产必须 TLS）。
    pub tls: TlsMode,
    /// MQTT 用户名（可选）。
    pub username: Option<String>,
    /// MQTT 密码（可选；禁止记录到日志，`validate` 要求 username 必填时
    /// 才允许 password）。
    pub password: Option<String>,
    /// Last Will 配置（可选，§31.1）。
    pub will: Option<WillConfig>,
}

impl fmt::Debug for MqttClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttClientConfig")
            .field("client_id", &self.client_id)
            .field("broker_host", &self.broker_host)
            .field("broker_port", &self.broker_port)
            .field("keep_alive", &self.keep_alive)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_packet_size", &self.max_packet_size)
            .field("reconnect_min_delay", &self.reconnect_min_delay)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("max_reconnect_retries", &self.max_reconnect_retries)
            .field("publish_capacity", &self.publish_capacity)
            .field("max_inflight", &self.max_inflight)
            .field("tls", &self.tls)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("will", &self.will)
            .finish()
    }
}

impl MqttClientConfig {
    /// 使用默认值构建配置；默认 `tls = None`（明文）。
    ///
    /// # 参数
    ///
    /// - `client_id`：MQTT 客户端标识，不可为空。
    /// - `broker_host`：broker 主机名或 IP，不可为空。
    /// - `broker_port`：broker 端口，不可为 0。
    pub fn new(
        client_id: impl Into<String>,
        broker_host: impl Into<String>,
        broker_port: u16,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            broker_host: broker_host.into(),
            broker_port,
            keep_alive: DEFAULT_KEEP_ALIVE,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            reconnect_min_delay: DEFAULT_RECONNECT_MIN_DELAY,
            reconnect_max_delay: DEFAULT_RECONNECT_MAX_DELAY,
            max_reconnect_retries: None,
            publish_capacity: DEFAULT_PUBLISH_CAPACITY,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            tls: TlsMode::default(),
            username: None,
            password: None,
            will: None,
        }
    }

    /// 校验配置合法性；非法时返回带字段路径的错误原因。
    ///
    /// 非法配置由 [`MqttClient::spawn`](crate::MqttClient::spawn) 拒绝，
    /// 不 panic（公共 API 不得因配置断言崩溃，开发规范 §6）。
    pub fn validate(&self) -> Result<(), String> {
        if self.client_id.is_empty() {
            return Err("client_id 不能为空（MQTT 规范要求非空客户端标识）".to_owned());
        }
        // MQTT 3.1.1 UTF-8 字段禁止 U+0000（MQTT-3.1.3-2 / MQTT-3.1.3-8）：
        // broker 会拒绝连接，导致客户端永久重试；必须在 spawn 前拦截。
        if self.client_id.contains('\0') {
            return Err("client_id 不允许包含 NUL（U+0000，MQTT UTF-8 字段限制）".to_owned());
        }
        if self.broker_host.is_empty() {
            return Err("broker_host 不能为空".to_owned());
        }
        if self.broker_port == 0 {
            return Err("broker_port 必须大于 0".to_owned());
        }
        // rumqttc 断言：非零保活周期必须 >= 1s，校验而非崩溃（开发规范 §3）。
        if !self.keep_alive.is_zero() && self.keep_alive < Duration::from_secs(1) {
            return Err("keep_alive 必须为 0（禁用）或 >= 1s（rumqttc 限制）".to_owned());
        }
        // MQTT 3.1.1 keep-alive 为 u16 秒；超出 65535s 会被 rumqttc 截断
        //（`as u16`），客户端保活定时器与 wire 值不一致会导致 broker
        // 反复判定失联断开，必须拒绝。
        if self.keep_alive.as_secs() > KEEP_ALIVE_MAX_SECS {
            return Err(format!(
                "keep_alive 超过 MQTT 3.1.1 上限 {KEEP_ALIVE_MAX_SECS}s"
            ));
        }
        // rumqttc 连接超时以秒为单位取整，小于 1s 会被取整为 0（立即失败）。
        if self.connect_timeout < Duration::from_secs(1) {
            return Err("connect_timeout 必须 >= 1s（rumqttc 以秒为单位计算）".to_owned());
        }
        if self.max_packet_size == 0 {
            return Err("max_packet_size 必须大于 0".to_owned());
        }
        if self.reconnect_min_delay.is_zero() {
            return Err("reconnect_min_delay 必须大于 0".to_owned());
        }
        if self.reconnect_max_delay < self.reconnect_min_delay {
            return Err("reconnect_max_delay 必须 >= reconnect_min_delay".to_owned());
        }
        if self.publish_capacity == 0 {
            return Err("publish_capacity 必须大于 0".to_owned());
        }
        // rumqttc `set_inflight` 断言 in-flight != 0，校验而非崩溃（开发规范 §3）。
        if self.max_inflight == 0 {
            return Err("max_inflight 必须大于 0（rumqttc 限制）".to_owned());
        }
        if self.password.is_some() && self.username.is_none() {
            return Err("password 设置时必须同时设置 username（MQTT 规范要求）".to_owned());
        }
        // CONNECT 字符串字段（client_id / username / password / Will
        // payload）均为 16 位长度前缀编码；超限会被 rumqttc 截断生成
        // 损坏报文，且此类永久配置错误表现为连接失败并持续重试，必须
        // 在 spawn 前拒绝。
        if self.client_id.len() > CONNECT_FIELD_MAX {
            return Err(format!(
                "client_id 超过 MQTT 3.1.1 字段上限 {CONNECT_FIELD_MAX} 字节"
            ));
        }
        if let Some(username) = &self.username
            && username.len() > CONNECT_FIELD_MAX
        {
            return Err(format!(
                "username 超过 MQTT 3.1.1 字段上限 {CONNECT_FIELD_MAX} 字节"
            ));
        }
        if let Some(username) = &self.username
            && username.contains('\0')
        {
            // MQTT 3.1.1 UTF-8 字段禁止 U+0000（MQTT-3.1.3-2）。
            return Err("username 不允许包含 NUL（U+0000，MQTT UTF-8 字段限制）".to_owned());
        }
        if let Some(password) = &self.password
            && password.len() > CONNECT_FIELD_MAX
        {
            return Err(format!(
                "password 超过 MQTT 3.1.1 字段上限 {CONNECT_FIELD_MAX} 字节"
            ));
        }
        if let Some(will) = &self.will {
            // Will Topic 与发布主题同规则（MQTT 3.1.1 §3.1.3.2 禁止
            // 通配符、控制字符，上限 65535 字节）；此类永久配置错误
            // 会表现为连接失败并持续重试，必须启动前拦截。
            if let Err(e) = crate::client::validate_publish_topic(&will.topic) {
                return Err(format!("will.topic 非法: {e}"));
            }
            if will.payload.len() > CONNECT_FIELD_MAX {
                return Err(format!(
                    "will.payload 超过 MQTT 3.1.1 字段上限 {CONNECT_FIELD_MAX} 字节"
                ));
            }
        }
        match &self.tls {
            TlsMode::None => {}
            TlsMode::ServerAuth { ca_pem } => {
                if ca_pem.is_empty() {
                    return Err("tls.ca_pem 不能为空".to_owned());
                }
            }
            TlsMode::MutualTls {
                ca_pem,
                client_cert_pem,
                client_key_pem,
            } => {
                if ca_pem.is_empty() {
                    return Err("tls.ca_pem 不能为空".to_owned());
                }
                if client_cert_pem.is_empty() {
                    return Err("tls.client_cert_pem 不能为空".to_owned());
                }
                if client_key_pem.is_empty() {
                    return Err("tls.client_key_pem 不能为空".to_owned());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> MqttClientConfig {
        MqttClientConfig::new("client-a", "broker.example.com", 1883)
    }

    #[test]
    fn defaults_are_valid() {
        assert!(base().validate().is_ok());
    }

    #[test]
    fn rejects_empty_client_id() {
        let mut c = base();
        c.client_id.clear();
        assert!(c.validate().unwrap_err().contains("client_id"));
    }

    #[test]
    fn rejects_empty_host() {
        let mut c = base();
        c.broker_host.clear();
        assert!(c.validate().unwrap_err().contains("broker_host"));
    }

    #[test]
    fn rejects_zero_port() {
        let mut c = base();
        c.broker_port = 0;
        assert!(c.validate().unwrap_err().contains("broker_port"));
    }

    #[test]
    fn rejects_subsecond_keep_alive() {
        let mut c = base();
        c.keep_alive = Duration::from_millis(500);
        assert!(c.validate().unwrap_err().contains("keep_alive"));
        c.keep_alive = Duration::ZERO;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_keep_alive_beyond_mqtt_u16_range() {
        // MQTT 3.1.1 keep-alive 是 u16 秒，超出会被 rumqttc 截断：
        // 客户端定时器与 wire 值不一致会让 broker 反复断开连接。
        let mut c = base();
        c.keep_alive = Duration::from_secs(KEEP_ALIVE_MAX_SECS);
        assert!(c.validate().is_ok());
        c.keep_alive = Duration::from_secs(KEEP_ALIVE_MAX_SECS + 1);
        assert!(c.validate().unwrap_err().contains("keep_alive"));
    }

    #[test]
    fn rejects_subsecond_connect_timeout() {
        let mut c = base();
        c.connect_timeout = Duration::from_millis(500);
        assert!(c.validate().unwrap_err().contains("connect_timeout"));
    }

    #[test]
    fn rejects_zero_publish_capacity() {
        let mut c = base();
        c.publish_capacity = 0;
        assert!(c.validate().unwrap_err().contains("publish_capacity"));
    }

    #[test]
    fn rejects_zero_max_inflight() {
        let mut c = base();
        c.max_inflight = 0;
        assert!(c.validate().unwrap_err().contains("max_inflight"));
    }

    #[test]
    fn rejects_reconnect_max_below_min() {
        let mut c = base();
        c.reconnect_min_delay = Duration::from_secs(10);
        c.reconnect_max_delay = Duration::from_secs(1);
        assert!(c.validate().unwrap_err().contains("reconnect_max_delay"));
    }

    #[test]
    fn rejects_password_without_username() {
        let mut c = base();
        c.password = Some("secret".to_owned());
        assert!(c.validate().unwrap_err().contains("username"));
        c.username = Some("u".to_owned());
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_empty_will_topic() {
        let mut c = base();
        c.will = Some(WillConfig {
            topic: String::new(),
            payload: b"offline".to_vec(),
            retain: true,
        });
        assert!(c.validate().unwrap_err().contains("will.topic"));
    }

    #[test]
    fn rejects_wildcard_or_invalid_will_topic() {
        // Will Topic 与发布主题同规则（MQTT 3.1.1 §3.1.3.2）：通配符、
        // 控制字符等永久配置错误会在连接阶段表现为失败并无限重试，
        // 必须在启动前拦截。
        for topic in ["a/#", "a/+/b", "a\x00b", "a\nb"] {
            let mut c = base();
            c.will = Some(WillConfig {
                topic: topic.to_owned(),
                payload: b"offline".to_vec(),
                retain: true,
            });
            assert!(c.validate().unwrap_err().contains("will.topic"), "{topic}");
        }
        let mut c = base();
        c.will = Some(WillConfig {
            topic: "forgelink/v1/status/plant-a/cnc-01".to_owned(),
            payload: b"offline".to_vec(),
            retain: true,
        });
        assert!(c.validate().is_ok(), "合法 Will Topic 必须通过");
    }

    #[test]
    fn rejects_connect_field_length_overflow() {
        // CONNECT 字符串字段为 16 位长度前缀编码：超限会被截断生成
        // 损坏报文，必须在 spawn 前拒绝。
        let mut c = base();
        c.client_id = "x".repeat(CONNECT_FIELD_MAX + 1);
        assert!(c.validate().unwrap_err().contains("client_id"));

        let mut c = base();
        c.username = Some("u".repeat(CONNECT_FIELD_MAX + 1));
        assert!(c.validate().unwrap_err().contains("username"));

        let mut c = base();
        c.password = Some("p".repeat(CONNECT_FIELD_MAX + 1));
        assert!(c.validate().unwrap_err().contains("password"));

        let mut c = base();
        c.will = Some(WillConfig {
            topic: "forgelink/v1/status/plant-a/cnc-01".to_owned(),
            payload: vec![0u8; CONNECT_FIELD_MAX + 1],
            retain: true,
        });
        assert!(c.validate().unwrap_err().contains("will.payload"));

        // 恰好等于上限必须通过（边界值合法）。
        let mut c = base();
        c.client_id = "x".repeat(CONNECT_FIELD_MAX);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_nul_in_utf8_connect_fields() {
        // MQTT 3.1.1 UTF-8 字段禁止 U+0000（MQTT-3.1.3-2）：broker 会
        // 拒绝连接并导致永久重试，必须在 spawn 前拦截。
        let mut c = base();
        c.client_id = "a\x00b".to_owned();
        assert!(c.validate().unwrap_err().contains("client_id"));

        let mut c = base();
        c.username = Some("u\x00ser".to_owned());
        assert!(c.validate().unwrap_err().contains("username"));

        // will.topic 由 validate_publish_topic 统一拦截（控制字符）。
        let mut c = base();
        c.will = Some(WillConfig {
            topic: "forgelink/v1/status/plant-a/cnc\x0001".to_owned(),
            payload: b"offline".to_vec(),
            retain: true,
        });
        assert!(c.validate().unwrap_err().contains("will.topic"));
    }

    #[test]
    fn will_offline_status_builds_status_envelope() {
        // §31.1/§32：离线 LWT 与在线状态使用同一 Status Envelope；
        // sent_at_ns 固定为 0（配置创建时间会失真，§31.1 契约）。
        let will = WillConfig::offline_status("plant-a", "cnc-01").unwrap();
        assert_eq!(will.topic, "forgelink/v1/status/plant-a/cnc-01");
        assert!(will.retain, "状态 LWT 必须 retain（§31.1）");
        let payload: serde_json::Value = serde_json::from_slice(&will.payload).unwrap();
        assert_eq!(payload["schema"], crate::client::STATUS_SCHEMA);
        assert_eq!(payload["status"], "offline");
        assert_eq!(payload["site_id"], "plant-a");
        assert_eq!(payload["device_id"], "cnc-01");
        assert_eq!(
            payload["sent_at_ns"].as_u64(),
            Some(0),
            "LWT 发布时间不可预知，sent_at_ns 必须为 0（§31.1）"
        );

        // 非法 ID 必须拒绝。
        assert!(WillConfig::offline_status("plant/a", "cnc-01").is_err());
        assert!(WillConfig::offline_status("", "cnc-01").is_err());
    }

    #[test]
    fn rejects_empty_tls_materials() {
        let mut c = base();
        c.tls = TlsMode::ServerAuth { ca_pem: Vec::new() };
        assert!(c.validate().unwrap_err().contains("ca_pem"));

        c.tls = TlsMode::MutualTls {
            ca_pem: Vec::new(),
            client_cert_pem: b"cert".to_vec(),
            client_key_pem: b"key".to_vec(),
        };
        assert!(c.validate().unwrap_err().contains("ca_pem"));

        c.tls = TlsMode::MutualTls {
            ca_pem: b"ca".to_vec(),
            client_cert_pem: Vec::new(),
            client_key_pem: b"key".to_vec(),
        };
        assert!(c.validate().unwrap_err().contains("client_cert_pem"));

        c.tls = TlsMode::MutualTls {
            ca_pem: b"ca".to_vec(),
            client_cert_pem: b"cert".to_vec(),
            client_key_pem: Vec::new(),
        };
        assert!(c.validate().unwrap_err().contains("client_key_pem"));
    }

    #[test]
    fn debug_redacts_password_and_private_key() {
        // 安全规范：密码与私钥不得出现在 Debug / 日志输出中。
        let mut c = base();
        c.password = Some("s3cr3t-password".to_owned());
        c.tls = TlsMode::MutualTls {
            ca_pem: b"ca-pem".to_vec(),
            client_cert_pem: b"cert-pem".to_vec(),
            client_key_pem: b"PRIVATE-KEY-MATERIAL".to_vec(),
        };
        let debug = format!("{c:?}");
        assert!(debug.contains("client-a"), "Debug 应保留非敏感字段");
        assert!(
            !debug.contains("s3cr3t-password"),
            "password 必须脱敏: {debug}"
        );
        assert!(
            !debug.contains("PRIVATE-KEY-MATERIAL"),
            "client_key_pem 必须脱敏: {debug}"
        );
        assert!(debug.contains("[REDACTED]"));
    }
}
