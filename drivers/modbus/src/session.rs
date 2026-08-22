//! 传输层会话：TCP（MBAP）与 RTU（串口）的同步收发。
//!
//! - 请求/响应按帧串行收发（驱动句柄由 Loader 串行调用，天然串行化）；
//! - 读写均受配置超时约束（TCP 用 `TcpStream::set_read_timeout`；
//!   RTU 用 serialport 的 read timeout）；
//! - 连接类错误返回 [`ModbusError::connection_lost`]；超时返回
//!   [`ModbusError::timeout`]。超时虽不能确定连接已断开（设备可能只是
//!   慢），但响应可能迟到导致事务号错位，`request_plan` 对全部传输级
//!   错误（含超时）都会主动丢弃会话，下一次请求在新连接上重新开始
//!   （§22 由上层按配置退避/重连）。

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::config::ModbusConfig;
use crate::error::ModbusError;
use crate::frame::{
    self, FrameError, is_write_function, parse_tcp_response_header, rtu_response_total_len,
};
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
    /// 执行一次写事务（FC05/06/15/16）：发送写请求帧，读取并校验回显响应。
    ///
    /// `payload` 为功能码后随载荷（不含地址）：FC05/06 为 2 字节值；
    /// FC15/16 为数量(2) + 字节计数(1) + 数据。成功返回表示从站已确认
    /// 回显（地址与值/数量逐字节一致）；Modbus 异常映射为
    /// [`ModbusError::modbus_exception`]。
    fn write_transaction(
        &mut self,
        unit_id: u8,
        function: u8,
        start_offset: u16,
        payload: &[u8],
    ) -> Result<(), ModbusError>;
}

/// 按连接配置创建传输（TCP 或 RTU）。
///
/// # Errors
///
/// RTU 模式缺少 `serial` 配置时返回 `config_error`（TCP 无需配置，不会失败）。
pub fn create_transport(config: &ModbusConfig) -> Result<Box<dyn Transport>, ModbusError> {
    match config.mode {
        crate::config::TransportMode::Tcp => Ok(Box::new(TcpTransport::new(config))),
        crate::config::TransportMode::Rtu => Ok(Box::new(RtuTransport::new(config)?)),
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
    /// 从连接配置构造（不建立连接，建连见 [`Transport::connect`]）。
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
        // 建连必须受 timeout_ms 约束：`TcpStream::connect` 会阻塞到操作系统
        // 超时（远超配置期限），必须解析地址后使用 connect_timeout。
        // 主机可解析出多个地址（如 localhost -> ::1、127.0.0.1），
        // 必须逐个尝试：仅连接第一个会在设备只监听 IPv4 时直接失败。
        let addrs = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| {
                ModbusError::connection_failed(format!(
                    "解析主机 {}:{} 失败：{e}",
                    self.host, self.port
                ))
            })?;
        let mut last_error = None;
        let mut connected = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, self.timeout) {
                Ok(stream) => {
                    connected = Some(stream);
                    break;
                }
                Err(e) => last_error = Some(e),
            }
        }
        let Some(stream) = connected else {
            return Err(ModbusError::connection_failed(format!(
                "连接 {}:{} 全部解析地址失败：{}",
                self.host,
                self.port,
                last_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "无解析地址".to_owned())
            )));
        };
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(|e| ModbusError::connection_failed(format!("设置读超时失败：{e}")))?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(|e| ModbusError::connection_failed(format!("设置写超时失败：{e}")))?;
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
            // 异常响应必须恰好 2 字节：功能码（置高位）+ 异常码。
            // 缺少异常码时置 0、多余字节被接受都会把畸形帧误判为
            // 可重试的 Modbus 异常，必须按响应失步（invalid_response）处理。
            if body.len() != 2 {
                return Err(ModbusError::invalid_response(format!(
                    "异常响应长度不符（{} 字节，期望 2）",
                    body.len()
                )));
            }
            let code = body[1];
            let (name, _) = frame::exception_code_name(code);
            return Err(ModbusError::modbus_exception(code, name));
        }
        // 正常响应第二字节为字节计数（Byte Count）；body 长度必须与期望
        // 完全一致（fc + byte count + expected 数据）。仅校验 Byte Count
        // 字段不够：错误的 MBAP length 会让截断/超长响应被当作成功数据，
        // 且多余字节会污染下一次事务。
        let expected = frame::expected_data_len(function, quantity);
        if body.len() != expected + 2 || body[1] as usize != expected {
            return Err(ModbusError::invalid_response(format!(
                "响应体长度不符（声明 {} 字节计数，body {} 字节，期望 {}）",
                body.get(1).copied().unwrap_or(0),
                body.len(),
                expected + 2
            )));
        }
        Ok(body[2..].to_vec())
    }

    fn write_transaction(
        &mut self,
        unit_id: u8,
        function: u8,
        start_offset: u16,
        payload: &[u8],
    ) -> Result<(), ModbusError> {
        self.transaction_id = self.transaction_id.wrapping_add(1);
        let request = frame::build_tcp_write_request(
            self.transaction_id,
            unit_id,
            function,
            start_offset,
            payload,
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
        let body = self.read_exact(meta.data_len)?;
        validate_write_body(function, start_offset, payload, &body)
    }
}

/// RTU 传输（串口）。
pub struct RtuTransport {
    serial: crate::config::SerialConfig,
    timeout: Duration,
    port: Option<Box<dyn SerialIo>>,
}

/// 串口字节流抽象：生产用 serialport，测试注入内存管道（RTU 全链路可测）。
pub(crate) trait SerialIo: Read + Write + Send {}

impl<T: Read + Write + Send> SerialIo for T {}

/// 适配 `serialport::SerialPort` 到 [`SerialIo`]（blanket impl 覆盖 SerialIo）。
struct SerialPortAdapter(Box<dyn serialport::SerialPort>);

impl Read for SerialPortAdapter {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for SerialPortAdapter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl RtuTransport {
    /// 从连接配置构造（不打开串口，打开见 [`Transport::connect`]）。
    ///
    /// # Errors
    ///
    /// `config.serial` 为 `None`（rtu 模式缺串口配置）时返回
    /// `config_error`，不 panic；调用方（`create_transport`/`validate`）
    /// 已保证模式与配置一致，此处校验用于防御公开 API 直接调用。
    pub fn new(config: &ModbusConfig) -> Result<Self, ModbusError> {
        let serial = config
            .serial
            .clone()
            .ok_or_else(|| ModbusError::config_error("rtu 模式缺少 serial 串口配置".to_owned()))?;
        Ok(Self {
            serial,
            timeout: config.request_timeout(),
            port: None,
        })
    }

    fn port<'a>(&'a mut self) -> Result<&'a mut (dyn SerialIo + 'static), ModbusError> {
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
        // 值已由 config.validate 严格校验（data_bits ∈ 5..=8、stop_bits ∈ 1..=2、
        // parity ∈ none/even/odd），默认分支为防御性兜底、实际不可达。
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
        self.port = Some(Box::new(SerialPortAdapter(port)));
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
        // 正常响应第二字节为字节计数（Byte Count），必须与期望一致。
        let expected = frame::expected_data_len(function, quantity);
        if frame[2] as usize != expected {
            return Err(ModbusError::invalid_response(format!(
                "RTU 字节计数不符（声明 {}，期望 {expected}）",
                frame[2]
            )));
        }
        Ok(frame[3..total - 2].to_vec())
    }

    fn write_transaction(
        &mut self,
        unit_id: u8,
        function: u8,
        start_offset: u16,
        payload: &[u8],
    ) -> Result<(), ModbusError> {
        let request = frame::build_rtu_write_request(unit_id, function, start_offset, payload);
        self.port()?
            .write_all(&request)
            .map_err(|e| ModbusError::connection_lost(format!("发送 RTU 帧失败：{e}")))?;
        // 读 unit + function 两个字节后按功能码计算剩余长度
        // （写响应无 Byte Count，正常恒为 4 字节回显 + CRC）。
        let mut head = [0u8; 2];
        self.port()?.read_exact(&mut head).map_err(map_io_error)?;
        if head[0] != unit_id {
            return Err(ModbusError::invalid_response(format!(
                "RTU 从站号不匹配（期望 {unit_id}，收到 {}）",
                head[0]
            )));
        }
        let total = rtu_response_total_len(&head, function, 0)
            .map_err(|e| ModbusError::invalid_response(frame_error_message(e)))?;
        let mut rest = vec![0u8; total - 2];
        self.port()?.read_exact(&mut rest).map_err(map_io_error)?;
        let mut frame_bytes = Vec::with_capacity(total);
        frame_bytes.extend_from_slice(&head);
        frame_bytes.extend_from_slice(&rest);
        if !crate::crc::verify(&frame_bytes) {
            return Err(ModbusError::invalid_response(
                "RTU 帧 CRC 校验失败".to_owned(),
            ));
        }
        let meta = frame::parse_rtu_response_meta(&frame_bytes)
            .map_err(|e| ModbusError::invalid_response(frame_error_message(e)))?;
        if meta.function != function {
            return Err(ModbusError::invalid_response(format!(
                "RTU 功能码不匹配（期望 {function:#04x}，收到 {:#04x}）",
                meta.function
            )));
        }
        if meta.is_exception {
            let code = frame_bytes[2];
            let (name, _) = frame::exception_code_name(code);
            return Err(ModbusError::modbus_exception(code, name));
        }
        // 正常写响应：unit + fc + 4 回显 + CRC；校验回显内容。
        validate_write_body(function, start_offset, payload, &frame_bytes[1..total - 2])
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

/// 校验写响应体（TCP body / RTU 去除 unit 与 CRC 后的载荷）。
///
/// 异常响应（功能码置高位）必须恰好 2 字节（fc|0x80 + 异常码），映射为
/// [`ModbusError::modbus_exception`]（与读路径同一规则）；正常响应必须为
/// 4 字节逐字节回显（地址 + 值/数量，无 Byte Count 字段），回显不符按
/// 响应失步（invalid_response）处理——把未确认的写入当成功会破坏
/// 控制链路的可信性。
fn validate_write_body(
    function: u8,
    start_offset: u16,
    payload: &[u8],
    body: &[u8],
) -> Result<(), ModbusError> {
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
        if body.len() != 2 {
            return Err(ModbusError::invalid_response(format!(
                "异常响应长度不符（{} 字节，期望 2）",
                body.len()
            )));
        }
        let code = body[1];
        let (name, _) = frame::exception_code_name(code);
        return Err(ModbusError::modbus_exception(code, name));
    }
    if !is_write_function(function) {
        return Err(ModbusError::invalid_response(
            "写事务收到非写功能码响应".to_owned(),
        ));
    }
    frame::validate_write_echo(&body[1..], start_offset, payload)
        .map_err(|e| ModbusError::invalid_response(frame_error_message(e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    /// 内存串口：`tx` 是本端写出的字节（模拟从站收到的请求），
    /// `rx` 是对端写入的字节（本端读到响应）。
    struct MemSerial {
        rx: Arc<Mutex<VecDeque<u8>>>,
        tx: Arc<Mutex<Vec<u8>>>,
    }

    /// 测试侧预置的响应缓冲 / 请求捕获缓冲句柄。
    type MemRx = Arc<Mutex<VecDeque<u8>>>;
    type MemTx = Arc<Mutex<Vec<u8>>>;

    impl MemSerial {
        fn new() -> (Self, MemRx, MemTx) {
            let rx = Arc::new(Mutex::new(VecDeque::new()));
            let tx = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    rx: Arc::clone(&rx),
                    tx: Arc::clone(&tx),
                },
                rx,
                tx,
            )
        }
    }

    impl Read for MemSerial {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let mut rx = self.rx.lock().unwrap();
            let n = rx.len().min(buf.len());
            for (i, b) in rx.drain(..n).enumerate() {
                buf[i] = b;
            }
            Ok(n)
        }
    }

    impl Write for MemSerial {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.tx.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn rtu_transport(serial: Box<dyn SerialIo>) -> RtuTransport {
        RtuTransport {
            serial: crate::config::SerialConfig {
                port: "MOCK".to_owned(),
                baud_rate: 9600,
                data_bits: 8,
                stop_bits: 1,
                parity: "none".to_owned(),
            },
            timeout: Duration::from_millis(1000),
            port: Some(serial),
        }
    }

    /// 组装带 CRC 的 RTU 响应帧。
    fn rtu_response(payload: &[u8]) -> Vec<u8> {
        let mut frame = payload.to_vec();
        let crc = crate::crc::crc16(&frame);
        frame.extend_from_slice(&crc.to_le_bytes());
        frame
    }

    #[test]
    fn rtu_transaction_full_round_trip() {
        let (mem, rx, tx) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        // 从站侧预置响应：unit=1, FC03, byte count=4, 2 寄存器数据。
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x03, 0x04, 0x12, 0x34, 0x56, 0x78]));

        let data = transport
            .read_transaction(1, frame::FC_READ_HOLDING_REGISTERS, 0, 2)
            .expect("RTU 读取失败");
        assert_eq!(data, vec![0x12, 0x34, 0x56, 0x78]);

        // 请求帧：unit+fc+offset+quantity+CRC = 8 字节，CRC 合法。
        let request = tx.lock().unwrap().clone();
        assert_eq!(request.len(), 8);
        assert_eq!(request[0], 0x01);
        assert_eq!(request[1], 0x03);
        assert_eq!(&request[2..4], &[0x00, 0x00]);
        assert_eq!(&request[4..6], &[0x00, 0x02]);
        assert!(crate::crc::verify(&request));
    }

    #[test]
    fn rtu_transaction_exception_maps_protocol_code() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        rx.lock().unwrap().extend(rtu_response(&[0x01, 0x83, 0x02]));

        let err = transport
            .read_transaction(1, frame::FC_READ_HOLDING_REGISTERS, 0, 2)
            .expect_err("异常响应必须失败");
        assert_eq!(err.code, "modbus_exception");
        assert_eq!(err.protocol_code, Some(0x02));
        assert!(!err.retryable);
    }

    #[test]
    fn rtu_transaction_rejects_bad_crc() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        // 响应载荷正确（FC03 读 2 寄存器：unit+fc+bc+4 数据）但 CRC 被篡改。
        let mut frame = rtu_response(&[0x01, 0x03, 0x04, 0x12, 0x34, 0x56, 0x78]);
        *frame.last_mut().unwrap() ^= 0xFF;
        rx.lock().unwrap().extend(frame);

        let err = transport
            .read_transaction(1, frame::FC_READ_HOLDING_REGISTERS, 0, 2)
            .expect_err("CRC 错误必须失败");
        assert_eq!(err.code, "invalid_response");
        assert!(!err.retryable);
    }

    #[test]
    fn rtu_transaction_rejects_wrong_unit() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x02, 0x03, 0x02, 0x11, 0x22]));

        let err = transport
            .read_transaction(1, frame::FC_READ_HOLDING_REGISTERS, 0, 1)
            .expect_err("从站号不匹配必须失败");
        assert_eq!(err.code, "invalid_response");
    }

    #[test]
    fn rtu_transaction_rejects_wrong_function() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        // 期望 FC03，从站回 FC04。
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x04, 0x02, 0x11, 0x22]));

        let err = transport
            .read_transaction(1, frame::FC_READ_HOLDING_REGISTERS, 0, 1)
            .expect_err("功能码不匹配必须失败");
        assert_eq!(err.code, "invalid_response");
    }

    #[test]
    fn rtu_transaction_rejects_bad_byte_count() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        // 声明 3 字节数据，期望（FC03 读 1 寄存器）为 2 字节；帧长仍完整。
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x03, 0x03, 0x11, 0x22]));

        let err = transport
            .read_transaction(1, frame::FC_READ_HOLDING_REGISTERS, 0, 1)
            .expect_err("字节计数不符必须失败");
        assert_eq!(err.code, "invalid_response");
    }

    // ------------------------------------------------------ RTU 写事务

    #[test]
    fn rtu_write_transaction_validates_echo() {
        let (mem, rx, tx) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        // 从站回显：unit=1, FC06, addr=0x0005, value=0x1388。
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x06, 0x00, 0x05, 0x13, 0x88]));

        transport
            .write_transaction(1, frame::FC_WRITE_SINGLE_REGISTER, 5, &[0x13, 0x88])
            .expect("RTU 写入失败");

        // 请求帧：unit+fc+offset+value+CRC = 8 字节，CRC 合法。
        let request = tx.lock().unwrap().clone();
        assert_eq!(request.len(), 8);
        assert_eq!(request[1], 0x06);
        assert_eq!(&request[2..4], &[0x00, 0x05]);
        assert_eq!(&request[4..6], &[0x13, 0x88]);
        assert!(crate::crc::verify(&request));
    }

    #[test]
    fn rtu_write_transaction_rejects_wrong_echo() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        // 回显值与请求不符（0x1388 vs 0x0000）。
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x06, 0x00, 0x05, 0x00, 0x00]));

        let err = transport
            .write_transaction(1, frame::FC_WRITE_SINGLE_REGISTER, 5, &[0x13, 0x88])
            .expect_err("回显不符必须失败");
        assert_eq!(err.code, "invalid_response");
        assert!(err.is_transport_level(), "响应失步必须丢弃会话");
    }

    #[test]
    fn rtu_write_transaction_rejects_wrong_echo_address() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x06, 0x00, 0x06, 0x13, 0x88]));

        let err = transport
            .write_transaction(1, frame::FC_WRITE_SINGLE_REGISTER, 5, &[0x13, 0x88])
            .expect_err("回显地址不符必须失败");
        assert_eq!(err.code, "invalid_response");
    }

    #[test]
    fn rtu_write_transaction_exception_maps_protocol_code() {
        let (mem, rx, _) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        rx.lock().unwrap().extend(rtu_response(&[0x01, 0x86, 0x02]));

        let err = transport
            .write_transaction(1, frame::FC_WRITE_SINGLE_REGISTER, 5, &[0x13, 0x88])
            .expect_err("异常响应必须失败");
        assert_eq!(err.code, "modbus_exception");
        assert_eq!(err.protocol_code, Some(0x02));
        assert!(!err.retryable);
        assert!(!err.is_transport_level(), "从站异常不丢弃会话");
    }

    #[test]
    fn rtu_write_multiple_registers_round_trip() {
        // FC16 写 2 寄存器：payload = qty(2)+bc(1)+4 数据；回显 qty。
        let (mem, rx, tx) = MemSerial::new();
        let mut transport = rtu_transport(Box::new(mem));
        rx.lock()
            .unwrap()
            .extend(rtu_response(&[0x01, 0x10, 0x00, 0x00, 0x00, 0x02]));

        let payload = [0x00, 0x02, 0x04, 0x12, 0x34, 0x56, 0x78];
        transport
            .write_transaction(1, frame::FC_WRITE_MULTIPLE_REGISTERS, 0, &payload)
            .expect("RTU FC16 写入失败");

        let request = tx.lock().unwrap().clone();
        // unit + fc + addr(2) + payload + CRC。
        assert_eq!(request.len(), 4 + payload.len() + 2);
        assert_eq!(request[1], 0x10);
        assert_eq!(&request[4..4 + payload.len()], &payload);
        assert!(crate::crc::verify(&request));
    }
}
