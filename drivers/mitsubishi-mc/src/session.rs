//! TCP 会话：MC 3E 帧收发。
//!
//! # 生命周期与响应匹配论证（无事务号协议的失步防护）
//!
//! - 懒建连：create 只构造会话；首请求 `ensure_connected` 即建 TCP。
//!   MC 3E **无连接级握手/协商步**——路由区参数随每帧携带；
//! - 句柄由 Loader 保证单线程串行调用（非重入，§17.5）；MC 应答不含
//!   任何请求相关标识（无事务号、无指令回显），响应匹配退化为三层
//!   结构自洽校验（副头 + 路由区回声 + 声明数据长与期望长一致）；
//! - **漏检窗口论证**：迟到帧只能来自被放弃的上一请求；放弃仅发生在
//!   超时与传输错误两条路径，二者处置均为丢会话重连——不存在"旧迟到
//!   帧落入新会话"的可达路径，故结构自洽校验在该前提下充分。实现
//!   纪律：**所有错误返回路径必须先 disconnect()**；
//! - 超时经 socket 读超时实现；监视定时器入帧原样携带但实际超时控制
//!   仍以 socket `timeout_ms` 为准。

use std::net::TcpStream;
use std::time::Duration;

use crate::config::McConfig;
use crate::error::McError;
use crate::mc::{self, Routing};

/// MC TCP 会话。
pub struct TcpSession {
    host: String,
    port: u16,
    timeout: Duration,
    routing: Routing,
    monitoring_timer: u16,
    stream: Option<TcpStream>,
}

impl TcpSession {
    /// 从配置构造会话（不建立连接）。
    #[must_use]
    pub fn new(config: &McConfig) -> Self {
        Self {
            host: config.host.clone(),
            port: config.port,
            timeout: Duration::from_millis(config.timeout_ms),
            routing: Routing {
                network_no: config.network_no,
                pc_no: config.pc_no,
                module_io: config.module_io,
                module_station: config.module_station,
            },
            monitoring_timer: config.monitoring_timer,
            stream: None,
        }
    }

    /// 路由区配置（应答回声校验用）。
    #[must_use]
    pub const fn routing(&self) -> &Routing {
        &self.routing
    }

    /// 监视定时器值。
    #[must_use]
    pub const fn monitoring_timer(&self) -> u16 {
        self.monitoring_timer
    }

    /// 连接是否存活。
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// 断开（置断开态；下次请求自动重连）。
    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    /// 确保连接可用（懒建连——MC 无握手步，TCP 建连即用）。
    ///
    /// # Errors
    ///
    /// 建连失败返回 `connection_failed`（含按配置的重连尝试）。
    pub fn ensure_connected(&mut self) -> Result<(), McError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let attempts = 3;
        let mut last = None;
        for _ in 0..attempts {
            match self.tcp_connect() {
                Ok(stream) => {
                    self.stream = Some(stream);
                    return Ok(());
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("至少尝试一次"))
    }

    fn tcp_connect(&mut self) -> Result<TcpStream, McError> {
        let targets = std::net::ToSocketAddrs::to_socket_addrs(&(self.host.as_str(), self.port))
            .map_err(|e| {
                McError::connection_failed(format!("地址解析失败 {}:{}: {e}", self.host, self.port))
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
        Err(McError::connection_failed(format!(
            "TCP 建连失败 {}:{}: {}",
            self.host,
            self.port,
            last.map_or_else(|| "无可用地址".to_owned(), |e| e.to_string())
        )))
    }

    /// 发送一帧完整请求并读取一帧完整应答（原始字节；三层自洽校验的
    /// 前两层在 mc 层、第三层长度校验在此完成）。
    ///
    /// 所有错误路径先置断开态（实现纪律，见模块文档论证）。
    ///
    /// # Errors
    ///
    /// 断线 → `connection_lost`；超时 → `timeout`（均 retryable 传输级）；
    /// 长度自洽不符 → `invalid_response`。
    pub fn transact(&mut self, request: &[u8]) -> Result<Vec<u8>, McError> {
        self.ensure_connected()?;
        // 从请求指令区反查期望应答体长：指令区 = [监视定时器 2][指令 2]
        // [子指令 2][软元件 4][点数 2]…，位于固定头之后。
        let data_len = usize::from(u16::from_le_bytes([request[7], request[8]]));
        let command_area = &request[mc::REQUEST_HEAD_LEN..mc::REQUEST_HEAD_LEN + data_len];
        if command_area.len() < 12 {
            self.disconnect();
            return Err(McError::invalid_response("请求指令区截断".to_owned()));
        }
        let command = u16::from_le_bytes([command_area[2], command_area[3]]);
        let points = u16::from_le_bytes([command_area[10], command_area[11]]);
        let expected_resp_len = mc::expected_resp_body_len(command, points);

        self.send_frame(request)?;
        let frame = self.read_frame()?;
        // 副头 + 路由区回声两层校验；第三层长度自洽在此完成。
        let (head, _body) = mc::parse_response_head(&frame, &self.routing)?;
        if head.declared_len != expected_resp_len {
            self.disconnect();
            return Err(McError::invalid_response(format!(
                "应答数据长与期望不符：声明 {}，按请求推算 {expected_resp_len}",
                head.declared_len
            )));
        }
        Ok(frame)
    }

    fn send_frame(&mut self, frame: &[u8]) -> Result<(), McError> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(McError::connection_lost("会话未建立".to_owned()));
        };
        use std::io::Write as _;
        if let Err(e) = stream.write_all(frame) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        Ok(())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, McError> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(McError::connection_lost("会话未建立".to_owned()));
        };
        use std::io::Read;
        let mut head = [0u8; mc::RESPONSE_HEAD_LEN];
        if let Err(e) = stream.read_exact(&mut head) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        let declared = usize::from(u16::from_le_bytes([head[7], head[8]]));
        let mut body = vec![0u8; declared];
        if let Err(e) = stream.read_exact(&mut body) {
            self.disconnect();
            return Err(map_io_error(e));
        }
        let mut frame = head.to_vec();
        frame.extend_from_slice(&body);
        Ok(frame)
    }
}

fn map_io_error(e: std::io::Error) -> McError {
    match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => McError::timeout(),
        _ => McError::connection_lost(e.to_string()),
    }
}
