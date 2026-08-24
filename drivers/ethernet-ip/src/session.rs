//! TCP 会话：EN/IP 连接、RegisterSession、请求-响应收发。
//!
//! # 生命周期与并发约束
//!
//! - 懒建连：create 只构造会话；`connect` = TCP 建连 + RegisterSession
//!   拿 session handle。handle 属连接级状态，断线重连必须重新 Register
//!   （镜像 S7 重 Setup 论证）；
//! - 句柄由 Loader 保证单线程串行调用（非重入，§17.5）；sender context
//!   由会话自增填充、应答逐字节回显校验——错位即失步；
//! - 超时经 socket 读超时实现。超时/断线后迟到的响应帧必然与后续请求
//!   错位，一律丢弃会话整体失败（与 modbus/S7 同一论证，§34.3）。

use std::net::TcpStream;
use std::time::Duration;

use crate::enip::{self, SenderContext};
use crate::error::EtherIpError;

/// EN/IP TCP 会话。
pub struct TcpSession {
    host: String,
    port: u16,
    timeout: Duration,
    stream: Option<TcpStream>,
    /// RegisterSession 分配的 handle（连接建立后有效）。
    session_handle: u32,
    /// sender context 自增计数（响应匹配 nonce）。
    context_counter: u64,
}

impl TcpSession {
    /// 构造会话（不建立连接）。
    #[must_use]
    pub fn new(host: String, port: u16, timeout_ms: u64) -> Self {
        Self {
            host,
            port,
            timeout: Duration::from_millis(timeout_ms),
            stream: None,
            session_handle: 0,
            context_counter: 0,
        }
    }

    /// 当前 session handle（连接建立后有效）。
    #[must_use]
    pub const fn session_handle(&self) -> u32 {
        self.session_handle
    }

    /// 连接是否存活。
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// 断开（置断开态；下次请求自动重连并重新 Register）。
    pub fn disconnect(&mut self) {
        self.stream = None;
        // handle 已失效：清零防误用。
        self.session_handle = 0;
    }

    /// 完整握手：TCP 建连 → RegisterSession。
    ///
    /// # Errors
    ///
    /// TCP 失败为 `connection_failed`；Register 否定/结构异常为
    /// `connection_failed` / `invalid_response`。
    pub fn connect(&mut self) -> Result<(), EtherIpError> {
        let stream = self.tcp_connect()?;
        self.stream = Some(stream);
        if let Err(e) = self.register_session() {
            self.disconnect();
            return Err(e);
        }
        Ok(())
    }

    fn tcp_connect(&mut self) -> Result<TcpStream, EtherIpError> {
        let targets = std::net::ToSocketAddrs::to_socket_addrs(&(self.host.as_str(), self.port))
            .map_err(|e| {
                EtherIpError::connection_failed(format!(
                    "地址解析失败 {}:{}: {e}",
                    self.host, self.port
                ))
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
        Err(EtherIpError::connection_failed(format!(
            "TCP 建连失败 {}:{}: {}",
            self.host,
            self.port,
            last.map_or_else(|| "无可用地址".to_owned(), |e| e.to_string())
        )))
    }

    fn register_session(&mut self) -> Result<(), EtherIpError> {
        let context = self.next_context();
        let request = enip::build_register_session(&context);
        self.send_frame(&request)?;
        let frame = self.read_frame()?;
        let (command, body_len, _session, status, ctx) = enip::parse_header(&frame)?;
        if command != enip::CMD_REGISTER_SESSION {
            return Err(EtherIpError::unexpected_command_code(
                enip::CMD_REGISTER_SESSION,
                command,
            ));
        }
        if status != 0 {
            return Err(EtherIpError::connection_failed(format!(
                "RegisterSession 被拒绝（status {status:#010x}）"
            )));
        }
        if ctx != context {
            return Err(EtherIpError::invalid_response(
                "RegisterSession 应答 sender context 回显不符".to_owned(),
            ));
        }
        let handle = enip::parse_register_session_reply(
            &frame[enip::HEADER_LEN..enip::HEADER_LEN + body_len],
        )?;
        if handle == 0 {
            return Err(EtherIpError::invalid_response(
                "RegisterSession 返回零 handle".to_owned(),
            ));
        }
        self.session_handle = handle;
        Ok(())
    }

    /// 发送一条 CIP 消息并返回匹配的应答 CIP 载荷（四重回显校验后剥离）。
    ///
    /// 传输错误后主动置断开态。
    ///
    /// # Errors
    ///
    /// 断线/超时/失步均为传输级（[`EtherIpError::is_transport_level`]）；
    /// 封装 status 非 0 为 `enip_error_response`（同属传输级）。
    pub fn exchange(&mut self, cip_request: &[u8]) -> Result<Vec<u8>, EtherIpError> {
        let expect_ctx = self.next_context();
        let mut frame = enip::build_header(
            enip::CMD_SEND_RR_DATA,
            enip::wrap_rr_data(cip_request).len(),
            self.session_handle,
            0,
            &expect_ctx,
        );
        frame.extend_from_slice(&enip::wrap_rr_data(cip_request));
        self.send_frame(&frame)?;
        let resp = self.read_frame()?;

        let (command, body_len, session, status, ctx) = enip::parse_header(&resp)?;
        if command != enip::CMD_SEND_RR_DATA {
            self.disconnect();
            return Err(EtherIpError::unexpected_command_code(
                enip::CMD_SEND_RR_DATA,
                command,
            ));
        }
        if session != self.session_handle {
            self.disconnect();
            return Err(EtherIpError::invalid_response(format!(
                "session handle 不符：期望 {:#010x}，收到 {session:#010x}",
                self.session_handle
            )));
        }
        if status != 0 {
            self.disconnect();
            return Err(EtherIpError::enip_error_response(status));
        }
        if ctx != expect_ctx {
            self.disconnect();
            return Err(EtherIpError::invalid_response(
                "sender context 回显不符（失步）".to_owned(),
            ));
        }
        let cip = enip::unwrap_rr_data(&resp[enip::HEADER_LEN..enip::HEADER_LEN + body_len])?;
        Ok(cip.to_vec())
    }

    fn next_context(&mut self) -> SenderContext {
        self.context_counter = self.context_counter.wrapping_add(1);
        self.context_counter.to_le_bytes()
    }

    fn send_frame(&mut self, frame: &[u8]) -> Result<(), EtherIpError> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(EtherIpError::connection_lost("会话未建立".to_owned()));
        };
        use std::io::Write;
        if let Err(e) = stream.write_all(frame) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        Ok(())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, EtherIpError> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(EtherIpError::connection_lost("会话未建立".to_owned()));
        };
        use std::io::Read;
        let mut header = [0u8; enip::HEADER_LEN];
        if let Err(e) = stream.read_exact(&mut header) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        // 先按已读头做基础校验，再读体。
        let (command, body_len, _session, _status, _ctx) = {
            // parse_header 需要完整 24B 头——此处已满足。
            match enip::parse_header(&header) {
                Ok(parts) => parts,
                Err(e) => {
                    self.disconnect();
                    return Err(e);
                }
            }
        };
        let _ = command;
        let mut body = vec![0u8; body_len];
        if let Err(e) = stream.read_exact(&mut body) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        let mut frame = header.to_vec();
        frame.extend_from_slice(&body);
        Ok(frame)
    }
}

fn map_io_error(e: std::io::Error) -> EtherIpError {
    match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => EtherIpError::timeout(),
        _ => EtherIpError::connection_lost(e.to_string()),
    }
}
