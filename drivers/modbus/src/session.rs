//! 传输层会话：TCP（MBAP）与 RTU（串口）的同步收发。
//!
//! - 请求/响应按帧串行收发（驱动句柄由 Loader 串行调用，天然串行化）；
//! - 读写均受配置超时约束（TCP 用 `TcpStream::set_read_timeout`；
//!   RTU 用 serialport 的 read timeout）；
//! - 连接类错误返回 [`ModbusError::connection_lost`]，由上层按配置自动重连；
//!   超时返回 [`ModbusError::timeout`]（不判定连接断开）。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::config::ModbusConfig;
use crate::error::ModbusError;
use crate::frame::{self, FrameError, parse_tcp_response_header, rtu_response_total_len};
/// 传输层抽象。
pub trait Transport: Send {
    /// 建立连接（TCP 建流 / RTU 打开串口）。
    fn connect(&mut self) -> Result<(), ModbusError>;
    /// 是否处于已连接状态。
    fn is_connected(&self) -> bool;
    /// 关闭连接。
    fn disconnect(&mut self);
    /// 执行一次读事务：发送请求帧，读取并校验完整响应（含 CRC/unit/功能码）。
    fn read_transaction(
        &mut self,
        unit_id: u8,
        function: u8,
        start_offset: u16,
        quantity: u16,
    ) -> Result<Vec<u8>, ModbusError>;
}

/// 按连接配置创建传输。
pub fn create_transport(config: &ModbusConfig) -> Box<dyn Transport> {
    match config.mode {
        crate::config::TransportMode::Tcp => Box::new(TcpTransport::new(config)),
        crate::config::TransportMode::Rtu => Box::new(RtuTransport::new(config)),
    }
}

/// TCP 传输（MBAP 帧）。
pub struct TcpTransport {
    host: String,
    port: u16,
    timeout: Duration,
    stream: Option<TcpStream>,
    /// 事务号（0..=65535 回绕）。
    transaction_id: u16,
}

impl TcpTransport {
    pub fn new(config: &ModbusConfig) -> Self {
        Self {
            host: config.host.clone().unwrap_or_default(),
            port: config.port,
            timeout: config.request_timeout(),
            stream: None,
            transaction_id: 0,
        }
    }

    fn stream(&self) -> Result<&TcpStream, ModbusError> {
        self.stream
            .as_ref()
            .ok_or_else(|| ModbusError::connection_lost("TCP 连接未建立或已断开".to_owned()))
    }

    /// 读满 `len` 字节（超时由 socket 读超时保证）。
    fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, ModbusError> {
        let mut buf = vec![0u8; len];
        self.stream()?.read_exact(&mut buf).map_err(map_io_error)?;
        Ok(buf)
    }
}

impl Transport for TcpTransport {
    fn connect(&mut self) -> Result<(), ModbusError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let addr = format!("{}:{}", self.host, self.port);
        let stream = TcpStream::connect(&addr)
            .map_err(|e| ModbusError::connection_failed(format!("连接 {addr} 失败：{e}")))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(|e| ModbusError::connection_failed(format!("设置读超时失败：{e}")))?;
        self.stream = Some(stream);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    fn disconnect(&mut self) {
        self.stream = None;
    }

    fn read_transaction(
        &mut self,
        unit_id: u8,
        function: u8,
        start_offset: u16,
        quantity: u16,
    ) -> Result<Vec<u8>, ModbusError> {
        self.transaction_id = self.transaction_id.wrapping_add(1);
        let request = frame::build_tcp_read_request(
            self.transaction_id,
            unit_id,
            function,
            start_offset,
            quantity,
        );
        // 写失败视为连接断开（对端已关闭）。
        self.stream()?
            .write_all(&request)
            .map_err(|e| ModbusError::connection_lost(format!("发送请求失败：{e}")))?;
        let header = self.read_exact(7)?;
        let meta = parse_tcp_response_header(&header)
            .map_err(|e| ModbusError::invalid_response(frame_error_message(e)))?;
        if meta.transaction_id != self.transaction_id {
            return Err(ModbusError::invalid_response(format!(
                "事务号不匹配（期望 {}，收到 {}）",
                self.transaction_id, meta.transaction_id
            )));
        }
        if meta.unit_id != unit_id {
            return Err(ModbusError::invalid_response(format!(
                "从站号不匹配（期望 {unit_id}，收到 {}）",
                meta.unit_id
            )));
        }
        // 响应 body 首字节为功能码（异常响应时置高位）。
        let body = self.read_exact(meta.data_len)?;
        if body.is_empty() {
            return Err(ModbusError::invalid_response("响应体为空".to_owned()));
        }
        let raw_function = body[0];
        let response_function = raw_function & 0x7F;
        if response_function != function {
            return Err(ModbusError::invalid_response(format!(
                "功能码不匹配（期望 {function:#04x}，收到 {response_function:#04x}）"
            )));
        }
        if raw_function & 0x80 != 0 {
            let code = body.get(1).copied().unwrap_or(0);
            let (name, _) = frame::exception_code_name(code);
            return Err(ModbusError::modbus_exception(code, name));
        }
        // 正常响应第二字节为字节计数（Byte Count）。
        let expected = frame::expected_data_len(function, quantity);
        if body.len() < 2 || body[1] as usize != expected {
            return Err(ModbusError::invalid_response(format!(
                "字节计数不符（声明 {}，期望 {expected}）",
                body.get(1).copied().unwrap_or(0)
            )));
        }
        Ok(body[2..].to_vec())
    }
}

/// RTU 传输（串口）。
pub struct RtuTransport {
    serial: crate::config::SerialConfig,
    timeout: Duration,
    port: Option<Box<dyn serialport::SerialPort>>,
}

impl RtuTransport {
    pub fn new(config: &ModbusConfig) -> Self {
        Self {
            serial: config.serial.clone().expect("rtu 模式必有 serial"),
            timeout: config.request_timeout(),
            port: None,
        }
    }

    fn port<'a>(
        &'a mut self,
    ) -> Result<&'a mut (dyn serialport::SerialPort + 'static), ModbusError> {
        self.port
            .as_deref_mut()
            .ok_or_else(|| ModbusError::connection_lost("串口未打开或已关闭".to_owned()))
    }
}

impl Transport for RtuTransport {
    fn connect(&mut self) -> Result<(), ModbusError> {
        if self.port.is_some() {
            return Ok(());
        }
        let port = serialport::new(&self.serial.port, self.serial.baud_rate)
            .data_bits(match self.serial.data_bits {
                5 => serialport::DataBits::Five,
                6 => serialport::DataBits::Six,
                7 => serialport::DataBits::Seven,
                _ => serialport::DataBits::Eight,
            })
            .stop_bits(match self.serial.stop_bits {
                1 => serialport::StopBits::One,
                2 => serialport::StopBits::Two,
                _ => serialport::StopBits::One,
            })
            .parity(match self.serial.parity.as_str() {
                "even" => serialport::Parity::Even,
                "odd" => serialport::Parity::Odd,
                _ => serialport::Parity::None,
            })
            .timeout(self.timeout)
            .open()
            .map_err(|e| {
                ModbusError::connection_failed(format!("打开串口 {} 失败：{e}", self.serial.port))
            })?;
        self.port = Some(port);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.port.is_some()
    }

    fn disconnect(&mut self) {
        self.port = None;
    }

    fn read_transaction(
        &mut self,
        unit_id: u8,
        function: u8,
        start_offset: u16,
        quantity: u16,
    ) -> Result<Vec<u8>, ModbusError> {
        let request = frame::build_rtu_read_request(unit_id, function, start_offset, quantity);
        self.port()?
            .write_all(&request)
            .map_err(|e| ModbusError::connection_lost(format!("发送 RTU 帧失败：{e}")))?;
        // 读 unit + function 两个字节后按功能码计算剩余长度。
        let mut head = [0u8; 2];
        self.port()?.read_exact(&mut head).map_err(map_io_error)?;
        if head[0] != unit_id {
            return Err(ModbusError::invalid_response(format!(
                "RTU 从站号不匹配（期望 {unit_id}，收到 {}）",
                head[0]
            )));
        }
        let total = rtu_response_total_len(&head, function, quantity)
            .map_err(|e| ModbusError::invalid_response(frame_error_message(e)))?;
        let mut rest = vec![0u8; total - 2];
        self.port()?.read_exact(&mut rest).map_err(map_io_error)?;
        let mut frame = Vec::with_capacity(total);
        frame.extend_from_slice(&head);
        frame.extend_from_slice(&rest);
        if !crate::crc::verify(&frame) {
            return Err(ModbusError::invalid_response(
                "RTU 帧 CRC 校验失败".to_owned(),
            ));
        }
        let meta = frame::parse_rtu_response_meta(&frame)
            .map_err(|e| ModbusError::invalid_response(frame_error_message(e)))?;
        if meta.function != function {
            return Err(ModbusError::invalid_response(format!(
                "RTU 功能码不匹配（期望 {function:#04x}，收到 {:#04x}）",
                meta.function
            )));
        }
        if meta.is_exception {
            let code = frame[2];
            let (name, _) = frame::exception_code_name(code);
            return Err(ModbusError::modbus_exception(code, name));
        }
        Ok(frame[3..total - 2].to_vec())
    }
}

/// I/O 错误分类：超时 -> `timeout`；其余 -> 连接断开。
fn map_io_error(e: std::io::Error) -> ModbusError {
    match e.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => ModbusError::timeout(),
        _ => ModbusError::connection_lost(format!("I/O 失败：{e}")),
    }
}

fn frame_error_message(e: FrameError) -> String {
    match e {
        FrameError::Truncated(m) | FrameError::Invalid(m) => format!("响应帧无效：{m}"),
    }
}
