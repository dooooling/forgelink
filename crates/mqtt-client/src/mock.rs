//! 测试专用 Mock MQTT 3.1.1 Broker（仅 `cfg(test)` 编译）。
//!
//! 支持：CONNECT/CONNACK、PUBLISH/PUBACK、SUBSCRIBE/SUBACK、PINGREQ/PINGRESP、
//! DISCONNECT；异常断开（未收到 DISCONNECT 报文）时按 CONNECT 中的 Will
//! 发布 LWT（§31.1）；可选的 TLS 服务端（含 mTLS 客户端证书校验）。
//!
//! 只实现测试所需的最小报文子集，不为生产使用设计。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// 被捕获的一条 PUBLISH 报文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedPublish {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    /// 重传标记（MQTT DUP 位，断线重发时置位）。
    pub dup: bool,
}

/// 从 CONNECT 报文中捕获的 Will（LWT）配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedWill {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

/// Mock broker 全局可观察状态。
#[derive(Default)]
struct MockState {
    /// 累计接受过的连接数（含重连）。
    connections: usize,
    /// 未收到 DISCONNECT 报文即断开的连接数。
    abnormal_disconnects: usize,
    /// 收到的全部 PUBLISH（按连接顺序）。
    publishes: Vec<CapturedPublish>,
    /// 每次 CONNECT 中携带的 Will（若有）。
    wills: Vec<CapturedWill>,
    /// 测试订阅者（精确主题匹配）。
    subscribers: Vec<Subscriber>,
    /// 当前存活的连接任务句柄（`drop_all_connections` 使用）。
    connection_tasks: Vec<JoinHandle<()>>,
    /// 测试钩子：收到第 N 个 PUBLISH 的连接不回复 PUBACK 直接断开
    /// （`None` 关闭；`Some(n)` 时每收到一个 PUBLISH 递减，归零即断开，
    /// 用于断言断线重发 / 重连后多设备在线状态重发布）。
    drop_connection_after_publish: Option<usize>,
    /// 测试钩子：指定连接（从 1 起计数）收到第 M 个 PUBLISH 后断开
    /// （用于断言重发周期未完成时二次断线会重建完整重发周期）。
    drop_connection_sequence: Option<(usize, usize)>,
    /// 测试钩子：每个 PUBLISH 延迟指定时长后再回复 PUBACK（用于断言
    /// 停机排空期间仍结算已确认的发布）。
    puback_delay: Duration,
    /// 测试钩子：挂起第 N 个（全局 PUBLISH 计数，从 1 起）QoS 1
    /// PUBLISH 的 PUBACK，直到测试将返回的 `AtomicBool` 置为 `true`
    ///（用于构造包标识回绕碰撞：早期槽位保持占用、后续确认先回，
    /// 客户端下一条发布回绕撞上未确认槽位触发 `Outgoing::AwaitAck`）。
    /// 挂起不阻塞连接读循环（后续报文照常接收、确认）。
    puback_hold_n: Option<(usize, Arc<std::sync::atomic::AtomicBool>)>,
}

struct Subscriber {
    topic: String,
    tx: mpsc::UnboundedSender<CapturedPublish>,
}

/// 测试用 Mock MQTT broker。
pub(crate) struct MockBroker {
    addr: std::net::SocketAddr,
    state: Arc<Mutex<MockState>>,
    shutdown_tx: watch::Sender<bool>,
    server_task: JoinHandle<()>,
}

impl MockBroker {
    /// 启动明文 TCP broker，监听 127.0.0.1 的随机端口。
    pub(crate) async fn start() -> Self {
        Self::start_inner(None).await
    }

    /// 启动 TLS broker（`client_ca_der` 为 `Some` 时启用 mTLS，
    /// 要求客户端出示由该 CA 签发的证书）。
    pub(crate) async fn start_tls(
        server_cert_der: Vec<u8>,
        server_key_der: Vec<u8>,
        client_ca_der: Option<Vec<u8>>,
    ) -> Self {
        let builder = rustls::ServerConfig::builder();
        let config = match client_ca_der {
            Some(ca) => {
                let mut roots = rustls::RootCertStore::empty();
                roots
                    .add(rustls::pki_types::CertificateDer::from(ca))
                    .expect("无效的 CA 证书");
                let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .expect("无法构建客户端证书校验器");
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        };
        let cert = rustls::pki_types::CertificateDer::from(server_cert_der);
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(server_key_der),
        );
        let config = config
            .with_single_cert(vec![cert], key)
            .expect("无效的服务端证书或密钥");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        Self::start_inner(Some(acceptor)).await
    }

    async fn start_inner(acceptor: Option<tokio_rustls::TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("无法绑定测试端口");
        let addr = listener.local_addr().expect("无法获取测试监听地址");
        let state = Arc::new(Mutex::new(MockState::default()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server_task = tokio::spawn(Self::run_server(
            listener,
            state.clone(),
            shutdown_rx,
            acceptor,
        ));
        Self {
            addr,
            state,
            shutdown_tx,
            server_task,
        }
    }

    /// broker 监听地址。
    pub(crate) fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// 累计连接次数。
    pub(crate) fn connections(&self) -> usize {
        self.state.lock().expect("state 锁中毒").connections
    }

    /// 捕获到的全部 PUBLISH。
    pub(crate) fn publishes(&self) -> Vec<CapturedPublish> {
        self.state.lock().expect("state 锁中毒").publishes.clone()
    }

    /// 捕获到的全部 Will（按连接顺序）。
    pub(crate) fn wills(&self) -> Vec<CapturedWill> {
        self.state.lock().expect("state 锁中毒").wills.clone()
    }

    /// 未收到 DISCONNECT 报文即断开的连接数。
    pub(crate) fn abnormal_disconnects(&self) -> usize {
        self.state
            .lock()
            .expect("state 锁中毒")
            .abnormal_disconnects
    }

    /// 测试钩子：下一个收到 PUBLISH 的连接不回复 PUBACK 直接断开。
    pub(crate) fn drop_connection_after_publish(&self) {
        self.drop_connection_after_publishes(1);
    }

    /// 测试钩子：收到第 N 个 PUBLISH 后不回复 PUBACK 直接断开（先记录
    /// 报文；重连后 rumqttc 会重发相同报文）。
    pub(crate) fn drop_connection_after_publishes(&self, n: usize) {
        self.state
            .lock()
            .expect("state 锁中毒")
            .drop_connection_after_publish = Some(n);
    }

    /// 测试钩子：指定连接（从 1 起计数）收到第 M 个 PUBLISH 后断开
    /// （先记录报文，不回复 PUBACK）。与 [`Self::drop_connection_after_publishes`]
    /// 按全局计数不同，本钩子按连接定位：可用于断言"上一轮重发周期中
    /// 已确认的设备在二次断线后会被重新加入重发"。
    pub(crate) fn drop_connection_number(&self, conn_index: usize, publish_count: usize) {
        self.state
            .lock()
            .expect("state 锁中毒")
            .drop_connection_sequence = Some((conn_index, publish_count));
    }

    /// 测试钩子：延迟指定时长后再回复 PUBACK（默认立即回复）。
    pub(crate) fn set_puback_delay(&self, delay: Duration) {
        self.state.lock().expect("state 锁中毒").puback_delay = delay;
    }

    /// 测试钩子：挂起第 N 个 QoS 1 PUBLISH 的 PUBACK，直到返回的
    /// `AtomicBool` 被置为 `true`（用于构造 pkid 回绕碰撞场景）。
    pub(crate) fn hold_puback(&self, nth: usize) -> Arc<std::sync::atomic::AtomicBool> {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.state.lock().expect("state 锁中毒").puback_hold_n = Some((nth, flag.clone()));
        flag
    }

    /// 注册精确主题订阅，返回消息接收端（含 LWT 发布）。
    pub(crate) async fn subscribe(&self, topic: &str) -> mpsc::UnboundedReceiver<CapturedPublish> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("state 锁中毒")
            .subscribers
            .push(Subscriber {
                topic: topic.to_owned(),
                tx,
            });
        rx
    }

    /// 中断全部客户端连接（模拟网络故障）。注意：任务被 `abort` 中止后
    /// 无法运行连接结束逻辑，因此 `abnormal_disconnects` 不会增加、LWT
    /// 也不会发布——与真实的 TCP 断连不同，这是测试钩子的已知语义。
    pub(crate) fn drop_all_connections(&self) {
        let mut state = self.state.lock().expect("state 锁中毒");
        for task in state.connection_tasks.drain(..) {
            task.abort();
        }
    }

    /// 停止 broker 并等待服务端任务退出（测试收尾）。
    pub(crate) async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.server_task.await;
    }

    async fn run_server(
        listener: TcpListener,
        state: Arc<Mutex<MockState>>,
        mut shutdown_rx: watch::Receiver<bool>,
        acceptor: Option<tokio_rustls::TlsAcceptor>,
    ) {
        loop {
            let accept = tokio::select! {
                _ = shutdown_rx.changed() => break,
                r = listener.accept() => r,
            };
            let (stream, _peer) = match accept {
                Ok(v) => v,
                // 瞬时错误（如连接被对端立即关闭）不终止服务端。
                Err(_) => continue,
            };
            let state = state.clone();
            let state_for_conn = state.clone();
            let acceptor = acceptor.clone();
            // 连接序号在任务启动前确定（从 1 起）：`drop_connection_number`
            // 钩子按此序号定位连接，任务内直接使用，无竞态。
            let conn_index = {
                let mut state = state.lock().expect("state 锁中毒");
                state.connections += 1;
                state.connections
            };
            let handle = tokio::spawn(async move {
                // 统一为 trait 对象：明文 TCP 与 TLS 流都满足读写约束。
                let stream: Box<dyn MqttStream> = match acceptor {
                    Some(a) => match a.accept(stream).await {
                        Ok(s) => Box::new(s),
                        Err(_) => return, // TLS 握手失败（如未知 CA）直接丢弃
                    },
                    None => Box::new(stream),
                };
                handle_connection(stream, state_for_conn, conn_index).await;
            });
            state
                .lock()
                .expect("state 锁中毒")
                .connection_tasks
                .push(handle);
        }
    }
}

/// 解析后的最小 MQTT 3.1.1 报文（测试所需子集）。
enum Packet {
    Connect(ConnectInfo),
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
        dup: bool,
        packet_id: u16,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    PingReq,
    Disconnect,
    /// 测试未涉及的报文类型（PUBACK/PUBREC 等），忽略继续处理。
    Ignore,
}

#[derive(Default)]
struct ConnectInfo {
    client_id: String,
    will: Option<CapturedWill>,
}

/// 测试连接流统一抽象：明文 TCP 与 TLS 流均满足（均 `Send`）。
trait MqttStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> MqttStream for T {}

/// 处理单条客户端连接：应答 CONNACK / PUBACK / SUBACK / PINGRESP，
/// 记录 PUBLISH 与 Will；未收到 DISCONNECT 报文即断开时发布 LWT。
async fn handle_connection(
    mut stream: Box<dyn MqttStream>,
    state: Arc<Mutex<MockState>>,
    conn_index: usize,
) {
    let mut clean_disconnect = false;
    let mut will: Option<CapturedWill> = None;
    // 本连接收到的 PUBLISH 计数（`drop_connection_number` 钩子使用）。
    let mut own_publishes = 0;
    // 挂起的 PUBACK（第 N 个 PUBLISH 的确认，放行标志置位后补发）：
    // 不阻塞读循环，后续报文照常接收、确认。
    let mut held_ack: Option<(Arc<std::sync::atomic::AtomicBool>, [u8; 4])> = None;

    loop {
        // 放行标志已置位：补发挂起的 PUBACK。
        if let Some((flag, ack)) = &held_ack
            && flag.load(std::sync::atomic::Ordering::Acquire)
        {
            let _ = stream.write_all(ack).await;
            held_ack = None;
        }

        // 有挂起 PUBACK 时以短轮询读报文：放行标志可能在任意时刻置位
        //（挂起期间客户端停止发送，纯 `read_packet` 会阻塞在 socket 上
        // 导致补发检查永不执行）。
        let packet = if held_ack.is_some() {
            tokio::select! {
                r = read_packet(&mut stream) => r,
                _ = tokio::time::sleep(Duration::from_millis(10)) => continue,
            }
        } else {
            read_packet(&mut stream).await
        };
        let packet = match packet {
            Ok(Some(p)) => p,
            Ok(None) => break, // EOF（对端关闭）
            Err(_) => break,   // 报文损坏或 I/O 错误
        };

        match packet {
            Packet::Connect(conn) => {
                if let Some(w) = conn.will.clone() {
                    will = Some(w.clone());
                    state.lock().expect("state 锁中毒").wills.push(w);
                }
                // CONNACK：session present = 0，return code = 0（接受）。
                let _ = stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await;
            }
            Packet::Publish {
                topic,
                payload,
                qos,
                retain,
                dup,
                packet_id,
            } => {
                let captured = CapturedPublish {
                    topic: topic.clone(),
                    payload: payload.clone(),
                    qos,
                    retain,
                    dup,
                };
                {
                    let mut state = state.lock().expect("state 锁中毒");
                    state.publishes.push(captured.clone());
                    deliver(&state, captured);
                    // 测试钩子：第 N 个 PUBLISH 不回复 PUBACK 直接断开
                    //（先记录报文；重连后 rumqttc 会重发相同报文）。
                    if let Some(remaining) = &mut state.drop_connection_after_publish {
                        *remaining -= 1;
                        if *remaining == 0 {
                            state.drop_connection_after_publish = None;
                            break;
                        }
                    }
                    // 测试钩子：指定连接的 PUBLISH 计数（先记录报文，
                    // 不回复 PUBACK）。
                    own_publishes += 1;
                    if let Some((idx, count)) = state.drop_connection_sequence
                        && idx == conn_index
                        && own_publishes >= count
                    {
                        state.drop_connection_sequence = None;
                        break;
                    }
                }
                if qos > 0 {
                    // 测试钩子：延迟 PUBACK（断言停机排空期间仍结算
                    // 已确认的发布）。
                    let delay = {
                        let state = state.lock().expect("state 锁中毒");
                        state.puback_delay
                    };
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    // 测试钩子：挂起第 N 个 PUBLISH 的 PUBACK（先记录
                    // 报文；放行前客户端该槽位保持占用，用于构造包标识
                    // 回绕碰撞）。挂起不阻塞读循环：ACK 字节暂存，放行
                    // 标志置位后由下轮循环补发。
                    let held = {
                        let mut state = state.lock().expect("state 锁中毒");
                        match state.puback_hold_n.take() {
                            // 全局序号 = 已记录报文数（当前报文已 push）。
                            Some((n, flag)) if n == state.publishes.len() => Some(flag),
                            other => {
                                state.puback_hold_n = other;
                                None
                            }
                        }
                    };
                    let ack = [0x40, 0x02, (packet_id >> 8) as u8, packet_id as u8];
                    if let Some(flag) = held {
                        held_ack = Some((flag, ack));
                    } else {
                        let _ = stream.write_all(&ack).await;
                    }
                }
            }
            Packet::Subscribe { packet_id, filters } => {
                let mut suback = vec![0x90, 0x00, (packet_id >> 8) as u8, packet_id as u8];
                for filter in &filters {
                    let (tx, _rx) = mpsc::unbounded_channel();
                    {
                        let mut state = state.lock().expect("state 锁中毒");
                        state.subscribers.push(Subscriber {
                            topic: filter.clone(),
                            tx,
                        });
                    }
                    suback.push(0x00); // granted QoS 0
                }
                suback[1] = (suback.len() - 2) as u8;
                let _ = stream.write_all(&suback).await;
            }
            Packet::PingReq => {
                let _ = stream.write_all(&[0xD0, 0x00]).await;
            }
            Packet::Disconnect => {
                clean_disconnect = true;
                break;
            }
            Packet::Ignore => {}
        }
    }

    if !clean_disconnect {
        state.lock().expect("state 锁中毒").abnormal_disconnects += 1;
        if let Some(will) = will {
            // broker 代客户端发布 LWT（§31.1）。
            let captured = CapturedPublish {
                topic: will.topic,
                payload: will.payload,
                qos: will.qos,
                retain: will.retain,
                dup: false,
            };
            let state = state.lock().expect("state 锁中毒");
            deliver(&state, captured);
        }
    }
}

/// 按精确主题把消息分发给已注册的测试订阅者。
fn deliver(state: &MockState, msg: CapturedPublish) {
    for sub in &state.subscribers {
        if sub.topic == msg.topic {
            let _ = sub.tx.send(msg.clone());
        }
    }
}

/// 读取一个完整 MQTT 报文（固定头 + 变长剩余长度 + 报文体）。
///
/// 返回 `Ok(None)` 表示对端已优雅关闭（EOF）。
async fn read_packet<S>(stream: &mut S) -> std::io::Result<Option<Packet>>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 1];
    let n = stream.read(&mut header).await?;
    if n == 0 {
        return Ok(None);
    }
    let first = header[0];
    let ptype = first >> 4;
    let mut remaining = 0usize;
    let mut multiplier = 1usize;
    for _ in 0..4 {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        remaining += (byte[0] & 0x7F) as usize * multiplier;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).await?;

    let mut cursor = Cursor::new(&body);
    Ok(Some(match ptype {
        1 => Packet::Connect(parse_connect(&mut cursor)),
        3 => {
            let topic = cursor.read_string()?;
            let qos = (first >> 1) & 0x03;
            let packet_id = if qos > 0 { cursor.read_u16()? } else { 0 };
            Packet::Publish {
                topic,
                payload: cursor.remaining(),
                qos,
                retain: first & 0x01 != 0,
                dup: first & 0x08 != 0,
                packet_id,
            }
        }
        8 => {
            let packet_id = cursor.read_u16()?;
            let mut filters = Vec::new();
            while cursor.has_remaining() {
                filters.push(cursor.read_string()?);
                cursor.skip(1)?; // requested QoS 字节
            }
            Packet::Subscribe { packet_id, filters }
        }
        12 => Packet::PingReq,
        14 => Packet::Disconnect,
        _ => {
            // 其他报文（PUBACK/PUBREC/PUBREL/PUBCOMP/UNSUBSCRIBE 等）测试用不到，
            // 忽略并继续处理后续报文。
            return Ok(Some(Packet::Ignore));
        }
    }))
}

/// CONNECT 报文体解析：可变头（协议名/级别/标志/保活）+ 载荷
/// （client_id、可选 Will、可选用户名/密码）。
fn parse_connect(cursor: &mut Cursor<'_>) -> ConnectInfo {
    let mut info = ConnectInfo::default();
    // 协议名与级别（MQTT 3.1.1："MQTT" + 4）。
    let _ = cursor.read_string();
    let _ = cursor.read_u8();
    let flags = match cursor.read_u8() {
        Ok(f) => f,
        Err(_) => return info,
    };
    let _ = cursor.read_u16();
    info.client_id = cursor.read_string().unwrap_or_default();
    if flags & 0x04 != 0 {
        let topic = cursor.read_string().unwrap_or_default();
        let qos = (flags >> 3) & 0x03;
        let payload = cursor.read_bytes().unwrap_or_default();
        info.will = Some(CapturedWill {
            topic,
            payload,
            qos,
            retain: flags & 0x20 != 0,
        });
    }
    info
}

/// 简易游标：按 MQTT 编码从报文体读取字段。
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn has_remaining(&self) -> bool {
        self.pos < self.data.len()
    }

    fn remaining(&mut self) -> Vec<u8> {
        let rest = self.data[self.pos..].to_vec();
        self.pos = self.data.len();
        rest
    }

    fn read_u8(&mut self) -> std::io::Result<u8> {
        if self.pos + 1 > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "报文体不足",
            ));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16(&mut self) -> std::io::Result<u16> {
        let hi = self.read_u8()?;
        let lo = self.read_u8()?;
        Ok(((hi as u16) << 8) | lo as u16)
    }

    /// 读取 MQTT 长度前缀字符串（2 字节长度 + UTF-8 内容）。
    fn read_string(&mut self) -> std::io::Result<String> {
        let len = self.read_u16()? as usize;
        if self.pos + len > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "字符串长度越界",
            ));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(s)
    }

    /// 读取 MQTT 长度前缀字节串（2 字节长度 + 内容）。
    fn read_bytes(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_u16()? as usize;
        if self.pos + len > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "字节串长度越界",
            ));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    fn skip(&mut self, n: usize) -> std::io::Result<()> {
        if self.pos + n > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "跳过越界",
            ));
        }
        self.pos += n;
        Ok(())
    }
}

/// 发送原始 MQTT 客户端报文（测试中直接构造 CONNECT 等）。
pub(crate) struct RawClient {
    stream: TcpStream,
}

impl RawClient {
    /// 建立 TCP 连接并发送 CONNECT（可选 Will），返回已连接客户端。
    pub(crate) async fn connect_with_will(
        addr: std::net::SocketAddr,
        will: Option<&CapturedWill>,
    ) -> Self {
        let mut stream = TcpStream::connect(addr).await.expect("TCP 连接失败");
        let mut packet = Vec::new();
        // 固定头：CONNECT
        packet.push(0x10);
        let mut flags = 0u8;
        if let Some(w) = will {
            flags |= 0x04 | (w.qos << 3) | if w.retain { 0x20 } else { 0 };
        }
        // 可变头：协议名 "MQTT"、级别 4、标志、保活 60s
        let var = [0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, flags, 0x00, 0x3C];
        let mut payload = Vec::new();
        // client_id（"raw-01" 共 6 字节）
        payload.extend_from_slice(&[0x00, 0x06]);
        payload.extend_from_slice(b"raw-01");
        if let Some(w) = will {
            let topic = w.topic.as_bytes();
            payload.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            payload.extend_from_slice(topic);
            payload.extend_from_slice(&(w.payload.len() as u16).to_be_bytes());
            payload.extend_from_slice(&w.payload);
        }
        let body = [&var[..], &payload[..]].concat();
        packet.push(body.len() as u8);
        packet.extend_from_slice(&body);
        stream.write_all(&packet).await.expect("CONNECT 发送失败");
        // 等待 CONNACK（0x20 0x02 0x00 0x00）
        let mut ack = [0u8; 4];
        stream.read_exact(&mut ack).await.expect("CONNACK 读取失败");
        assert_eq!(ack, [0x20, 0x02, 0x00, 0x00], "CONNACK 异常");
        Self { stream }
    }

    /// 直接关闭连接（不发送 DISCONNECT），模拟客户端异常掉线。
    pub(crate) fn drop(mut self) {
        std::mem::drop(self.stream.shutdown());
    }
}

/// 测试辅助：在期限内轮询断言。
pub(crate) async fn wait_until<F>(mut cond: F)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if cond() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wait_until 超时：条件未满足");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
