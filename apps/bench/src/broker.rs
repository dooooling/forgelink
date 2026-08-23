//! 北向消息源：双模式统一（§34.2 "northbound: local MQTT broker"）。
//!
//! - `Mock`：进程内 MockBroker。collector 直连其监听端口；bench 以
//!   订阅者身份逐设备注册精确主题（Mock 仅支持精确匹配），合并进同一
//!   无界通道。
//! - `Real`：真实 MQTT broker（mosquitto 等）。bench 以 rumqttc 客户端
//!   SUBSCRIBE 通配主题 `forgelink/v1/telemetry/{site}/+`。
//!
//! 两种模式产出同构的 `Feed`（payload 字节流），下游记账代码完全一致
//! ——这是双 broker 决策的落点。

use tokio::sync::mpsc;

use mqtt_client::{mock::MockBroker, telemetry_topic};

/// 北向 broker 形态。
pub enum Northbound {
    Mock(MockBroker),
    Real { host: String, port: u16 },
}

/// 单条北向 Telemetry 批次（仅保留记账所需字段）。
pub struct RawMessage {
    pub payload: Vec<u8>,
}

/// 合并后的消息接收端。
pub struct Feed {
    rx: mpsc::UnboundedReceiver<RawMessage>,
}

impl Feed {
    /// 取下一条消息；通道关闭（所有转发任务退出）返回 `None`。
    pub async fn recv(&mut self) -> Option<RawMessage> {
        self.rx.recv().await
    }
}

impl Northbound {
    /// 启动订阅并为全部设备建立消息转发。必须在 collector 启动**之前**
    /// 调用（订阅先于发布，避免漏计首批）。
    pub async fn start_feed(&self, site_id: &str, device_ids: &[String]) -> Result<Feed, String> {
        let (tx, rx) = mpsc::unbounded_channel();
        match self {
            Self::Mock(broker) => {
                for device in device_ids {
                    // §31.1 主题生成返回 Result（设备 ID 合法性校验）；
                    // workload 生成的 ID 恒合法，失败即装配错误。
                    let topic = telemetry_topic(site_id, device)
                        .map_err(|e| format!("遥测主题构造失败: {e}"))?;
                    let mut sub = broker.subscribe(&topic).await;
                    let tx = tx.clone();
                    // 每设备一个转发任务：MockBroker 的订阅通道是精确主题
                    // 匹配，无法通配；任务随通道关闭退出。消费侧必须及时
                    // 排空（无界通道，滞后即内存膨胀）。
                    tokio::spawn(async move {
                        while let Some(msg) = sub.recv().await {
                            if tx
                                .send(RawMessage {
                                    payload: msg.payload,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                }
            }
            Self::Real { host, port } => {
                let opts =
                    rumqttc::MqttOptions::new("forgelink-bench-accounting", host.clone(), *port);
                let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 256);
                // 通配订阅一次覆盖全部设备（真实 broker 支持 + 通配）。
                client
                    .subscribe(
                        format!("forgelink/v1/telemetry/{site_id}/+"),
                        rumqttc::QoS::AtLeastOnce,
                    )
                    .await
                    .map_err(|e| format!("SUBSCRIBE 失败: {e}"))?;
                tokio::spawn(async move {
                    loop {
                        match eventloop.poll().await {
                            Ok(notif) => {
                                if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) =
                                    notif
                                    && tx
                                        .send(RawMessage {
                                            payload: publish.payload.to_vec(),
                                        })
                                        .is_err()
                                {
                                    break;
                                }
                            }
                            // 网络抖动由 rumqttc 内部重连处理；持续失败在
                            // 场景层以健康/静默判定暴露，此处仅记录后重试。
                            Err(e) => {
                                eprintln!("bench 订阅端 eventloop 错误（将重试）: {e}");
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                });
            }
        }
        Ok(Feed { rx })
    }

    /// mock 模式下的 broker 监听地址（生成 collector.yaml 用）；real
    /// 模式返回配置的 host/port。
    pub fn connection(&self) -> (String, u16) {
        match self {
            Self::Mock(b) => (b.addr().ip().to_string(), b.addr().port()),
            Self::Real { host, port } => (host.clone(), *port),
        }
    }
}
