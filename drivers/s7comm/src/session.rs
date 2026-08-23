//! TCP 会话：ISO-on-TCP 连接、S7 Setup 握手、请求-响应收发。
//!
//! # 生命周期与并发约束
//!
//! - 懒建连：create 只构造会话；`connect` 执行完整三步握手
//!   （TCP → COTP CR/CC → Setup 协商），断线后重连同样**必须重新
//!   Setup**（协商结果属于连接级状态，不能复用旧连接的 negotiated_pdu）；
//! - 句柄由 Loader 保证单线程串行调用（非重入，§17.5）；pdu-ref 由会话
//!   单调自增回绕并按引用匹配响应——错位即失步；
//! - 超时经 socket 读超时实现。超时/断线后迟到的响应帧必然与后续请求
//!   错位，因此一律丢弃会话整体失败（与 modbus 同一论证，§34.3）。

use std::io::Write as _;
use std::net::TcpStream;
use std::time::Duration;

use crate::cotp;
use crate::error::S7Error;
use crate::pdu::{self, PROPOSED_PDU_SIZE, parse_setup_ack};

/// 握手协商出的 pdu 下限：至少容得下单条 DWORD 读的完整往返，
/// 低于此值视为 PLC 异常——快速失败并在错误信息中提示调参，
/// 不做自动降级重试风暴。
const MIN_USABLE_PDU: u16 = 64;

/// S7 TCP 会话。
pub struct TcpSession {
    host: String,
    port: u16,
    rack: u8,
    slot: u8,
    timeout: Duration,
    stream: Option<TcpStream>,
    pdu_ref: u16,
    /// Setup 协商结果（连接建立后有效）。
    negotiated_pdu: u16,
}

impl TcpSession {
    /// 构造会话（不建立连接）。
    #[must_use]
    pub fn new(host: String, port: u16, rack: u8, slot: u8, timeout_ms: u64) -> Self {
        Self {
            host,
            port,
            rack,
            slot,
            timeout: Duration::from_millis(timeout_ms),
            stream: None,
            pdu_ref: 0,
            negotiated_pdu: PROPOSED_PDU_SIZE,
        }
    }

    /// 协商后的 PDU 上限（仅在连接建立后有意义的分块预算输入；
    /// 调用方在握手之后读取）。
    #[must_use]
    pub const fn negotiated_pdu(&self) -> u16 {
        self.negotiated_pdu
    }

    /// 连接是否存活。
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// 断开（置断开态；下次请求自动重连并重新 Setup）。
    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    /// 完整三步握手：TCP 建连 → COTP CR/CC → Setup 协商。
    ///
    /// # Errors
    ///
    /// TCP/COTP 失败为 `connection_failed`；Setup 应答异常为
    /// `invalid_response`；协商值过小为 `connection_failed`。
    pub fn connect(&mut self) -> Result<(), S7Error> {
        let stream = self.tcp_connect()?;
        self.stream = Some(stream);
        if let Err(e) = self.handshake() {
            // 握手失败不留半开连接（下次请求重走完整握手）。
            self.disconnect();
            return Err(e);
        }
        Ok(())
    }

    fn tcp_connect(&mut self) -> Result<TcpStream, S7Error> {
        let targets = std::net::ToSocketAddrs::to_socket_addrs(&(self.host.as_str(), self.port))
            .map_err(|e| {
                S7Error::connection_failed(format!("地址解析失败 {}:{}: {e}", self.host, self.port))
            })?
            .collect::<Vec<_>>();
        let mut last = None;
        for target in targets {
            match TcpStream::connect_timeout(&target, self.timeout) {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    let _ = stream.set_read_timeout(Some(self.timeout));
                    let _ = stream.set_write_timeout(Some(self.timeout));
                    return Ok(stream);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(S7Error::connection_failed(format!(
            "TCP 建连失败 {}:{}: {}",
            self.host,
            self.port,
            last.map_or_else(|| "无可用地址".to_owned(), |e| e.to_string())
        )))
    }

    fn handshake(&mut self) -> Result<(), S7Error> {
        // 1. COTP CR/CC。
        let cr = cotp::connection_request(&[0x01, 0x00], &cotp::remote_tsap(self.rack, self.slot));
        self.send_frame(&cr)?;
        let frame = self.read_frame()?;
        if !matches!(
            cotp::parse_frame(&frame)?,
            cotp::CotpFrame::ConnectionConfirm
        ) {
            return Err(S7Error::invalid_response("COTP 握手应答不是 CC".to_owned()));
        }
        // 2. Setup Communication（协商取双方较小者，由本端完成取小）。
        let setup_ref = self.next_ref();
        let setup = pdu::build_setup(setup_ref, PROPOSED_PDU_SIZE);
        let ack = self.exchange(setup_ref, &setup)?;
        let offered = parse_setup_ack(&ack.param)?;
        if offered < MIN_USABLE_PDU {
            return Err(S7Error::connection_failed(format!(
                "PLC 协商 PDU {offered} 过小（< {MIN_USABLE_PDU}），无法承载单条读取"
            )));
        }
        self.negotiated_pdu = PROPOSED_PDU_SIZE.min(offered);
        Ok(())
    }

    /// 发送一条 Job PDU（ref 已由调用方经 [`Self::next_ref`] 编入）并返回
    /// 匹配 ref 的 Ack 分区。传输错误后主动置断开态。
    ///
    /// # Errors
    ///
    /// 断线/超时/失步均为传输级（[`S7Error::is_transport_level`]）。
    pub fn exchange(&mut self, expected_ref: u16, job_pdu: &[u8]) -> Result<AckOwned, S7Error> {
        self.send_frame(&cotp::data_tpdu(job_pdu))?;
        let frame = self.read_frame()?;
        let s7 = match cotp::parse_frame(&frame)? {
            cotp::CotpFrame::Data(payload) => payload.to_vec(),
            cotp::CotpFrame::ConnectionConfirm => {
                self.disconnect();
                return Err(S7Error::invalid_response(
                    "数据阶段收到 COTP CC（失步）".to_owned(),
                ));
            }
        };
        let result = pdu::parse_ack(&s7).and_then(|ack| {
            if ack.pdu_ref != expected_ref {
                Err(S7Error::invalid_response(format!(
                    "pdu_ref 不匹配：期望 {expected_ref:#06x}，收到 {:#06x}",
                    ack.pdu_ref
                )))
            } else {
                Ok(AckOwned {
                    param: ack.param.to_vec(),
                    data: ack.data.to_vec(),
                })
            }
        });
        if let Err(e) = &result
            && e.is_transport_level()
        {
            // 失步/结构坏：会话不可继续（迟到帧错位论证见模块注释）。
            self.disconnect();
        }
        result
    }

    /// pdu-ref 自增（回绕语义：65535 → 0；调用方编入 Job 头 [4..6]，
    /// [`Self::exchange`] 据此匹配响应）。
    #[must_use]
    pub fn next_ref(&mut self) -> u16 {
        self.pdu_ref = self.pdu_ref.wrapping_add(1);
        self.pdu_ref
    }

    fn send_frame(&mut self, tpdu: &[u8]) -> Result<(), S7Error> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(S7Error::connection_lost("会话未建立".to_owned()));
        };
        let len = tpdu.len() + 4;
        if len > 65_579 {
            return Err(S7Error::invalid_response("TPKT 帧超长".to_owned()));
        }
        let mut frame = Vec::with_capacity(len);
        frame.extend_from_slice(&[cotp::TPKT_VERSION, 0, (len >> 8) as u8, (len & 0xFF) as u8]);
        frame.extend_from_slice(tpdu);
        if let Err(e) = stream.write_all(&frame) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        Ok(())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, S7Error> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(S7Error::connection_lost("会话未建立".to_owned()));
        };
        use std::io::Read;
        let mut header = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut header) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        if header[0] != cotp::TPKT_VERSION || header[1] != 0 {
            self.disconnect();
            return Err(S7Error::invalid_response(format!(
                "TPKT 版本非法：{:#04x}",
                header[0]
            )));
        }
        let declared =
            usize::from(u16::from_be_bytes([header[2], header[3]])).max(cotp::TPKT_HEADER_LEN);
        let mut rest = vec![0u8; declared - cotp::TPKT_HEADER_LEN];
        if let Err(e) = stream.read_exact(&mut rest) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        rest.splice(..0, header);
        Ok(rest)
    }
}

/// 已拷贝出生命周期的 Ack 分区。
#[derive(Debug)]
pub struct AckOwned {
    /// 参数区。
    pub param: Vec<u8>,
    /// 数据区。
    pub data: Vec<u8>,
}

fn map_io_error(e: std::io::Error) -> S7Error {
    match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => S7Error::timeout(),
        _ => S7Error::connection_lost(e.to_string()),
    }
}
