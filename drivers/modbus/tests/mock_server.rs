//! 同步 mock Modbus TCP server（集成测试共用）。
//!
//! 支持行为配置：寄存器表（按 unit/kind/offset 的值）、指定地址返回异常、
//! 响应延迟、连接立即断开。

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// 数据段（与驱动 `RegisterKind` 对应，避免依赖内部类型）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Coil,
    DiscreteInput,
    InputRegister,
    HoldingRegister,
}

/// 地址键：`(unit, kind, offset)`。
type AddrKey = (u8, Kind, u16);

/// Mock 服务器行为配置。
#[derive(Debug, Clone, Default)]
pub struct MockBehavior {
    /// 寄存器表：地址 -> 16 位值（线圈/离散用低 1 位）。
    pub values: HashMap<AddrKey, u16>,
    /// 指定地址返回异常码（覆盖正常响应）。
    pub exception_at: HashMap<AddrKey, u8>,
    /// 每个请求的响应延迟。
    pub response_delay: Option<Duration>,
    /// 是否在响应前立即断开连接（模拟断线）。
    pub drop_connection: bool,
    /// 统计：收到的请求数。
    pub request_count: Arc<AtomicU32>,
}

impl MockBehavior {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            exception_at: HashMap::new(),
            response_delay: None,
            drop_connection: false,
            request_count: Arc::new(AtomicU32::new(0)),
        }
    }

    /// 便捷：填充一段连续保持寄存器值。
    pub fn with_holding_range(mut self, unit: u8, start: u16, values: &[u16]) -> Self {
        for (i, v) in values.iter().enumerate() {
            self.values
                .insert((unit, Kind::HoldingRegister, start + i as u16), *v);
        }
        self
    }

    /// 便捷：填充一段连续输入寄存器值。
    pub fn with_input_range(mut self, unit: u8, start: u16, values: &[u16]) -> Self {
        for (i, v) in values.iter().enumerate() {
            self.values
                .insert((unit, Kind::InputRegister, start + i as u16), *v);
        }
        self
    }

    /// 便捷：填充一段连续线圈值。
    pub fn with_coil_range(mut self, unit: u8, start: u16, bits: &[bool]) -> Self {
        for (i, b) in bits.iter().enumerate() {
            self.values
                .insert((unit, Kind::Coil, start + i as u16), *b as u16);
        }
        self
    }

    /// 便捷：设置响应延迟。
    pub fn with_response_delay(mut self, delay: Duration) -> Self {
        self.response_delay = Some(delay);
        self
    }
}

/// 运行的 Mock 服务器。
pub struct MockServer {
    pub addr: std::net::SocketAddr,
    behavior: Arc<Mutex<MockBehavior>>,
    handle: Option<thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl MockServer {
    /// 启动服务器并返回地址。
    pub fn start(behavior: MockBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("绑定测试端口失败");
        let addr = listener.local_addr().expect("读取监听地址失败");
        let behavior = Arc::new(Mutex::new(behavior));
        let behavior_ref = Arc::clone(&behavior);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        listener.set_nonblocking(true).expect("设置非阻塞失败");
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let behavior = Arc::clone(&behavior_ref);
                        let stop = Arc::clone(&stop_thread);
                        // 每个连接一个线程（测试规模小，无并发压力）。
                        thread::spawn(move || {
                            handle_connection(stream, behavior, stop);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        if stop_thread.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            behavior,
            handle: Some(handle),
            stop,
        }
    }

    /// 共享行为引用（测试中修改配置用）。
    pub fn behavior(&self) -> Arc<Mutex<MockBehavior>> {
        Arc::clone(&self.behavior)
    }

    /// 收到的请求总数。
    pub fn request_count(&self) -> u32 {
        self.behavior
            .lock()
            .expect("行为锁")
            .request_count
            .load(Ordering::Relaxed)
    }

    /// 停止服务器（等待线程退出）。
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("服务器线程退出失败");
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 功能码 -> 数据段。
fn kind_of(function: u8) -> Option<Kind> {
    match function {
        0x01 => Some(Kind::Coil),
        0x02 => Some(Kind::DiscreteInput),
        0x03 => Some(Kind::HoldingRegister),
        0x04 => Some(Kind::InputRegister),
        _ => None,
    }
}

fn handle_connection(
    mut stream: TcpStream,
    behavior: Arc<Mutex<MockBehavior>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    // 监听 socket 非阻塞时，accept 出的连接也继承非阻塞标志，
    // 必须恢复阻塞模式才能用 read_exact 等待完整帧。
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // 读 MBAP 头（7 字节）。
        let mut header = [0u8; 7];
        if stream.read_exact(&mut header).is_err() {
            return;
        }
        let length = u16::from_be_bytes([header[4], header[5]]) as usize;
        // length 含 unit（已位于头第 7 字节），body = fc + 数据。
        let mut rest = vec![0u8; length - 1];
        if stream.read_exact(&mut rest).is_err() {
            return;
        }
        // rest = [function, addr_hi, addr_lo, qty_hi, qty_lo]（读请求 body 5 字节，
        // unit 已在 MBAP 头第 7 字节）。
        if rest.len() < 5 {
            return;
        }
        let unit = header[6];
        let function = rest[0];
        let start = u16::from_be_bytes([rest[1], rest[2]]);
        let quantity = u16::from_be_bytes([rest[3], rest[4]]);
        let transaction_id = u16::from_be_bytes([header[0], header[1]]);

        let mut guard = behavior.lock().expect("行为锁");
        guard.request_count.fetch_add(1, Ordering::Relaxed);

        // 断线模拟：直接关闭。
        if guard.drop_connection {
            drop(guard);
            return;
        }
        if let Some(delay) = guard.response_delay {
            drop(guard);
            thread::sleep(delay);
            guard = behavior.lock().expect("行为锁");
        }

        let kind = match kind_of(function) {
            Some(k) => k,
            None => return,
        };

        // 检查异常覆盖（区间起始地址）。
        let exception = (0..quantity).find_map(|offset| {
            guard
                .exception_at
                .get(&(unit, kind, start + offset))
                .copied()
        });

        // 读数据段：寄存器 2 字节大端；线圈/离散按位打包（LSB 优先）。
        let data: Vec<u8> = if let Some(code) = exception {
            // 异常响应：功能码置高位 + 异常码，连接保持。
            let mut response = vec![0u8; 9];
            response[0..2].copy_from_slice(&header[0..2]);
            response[2..4].copy_from_slice(&[0x00, 0x00]);
            response[4..6].copy_from_slice(&[0x00, 0x03]);
            response[6] = unit;
            response[7] = function | 0x80;
            response[8] = code;
            if stream.write_all(&response).is_err() {
                return;
            }
            continue;
        } else if matches!(kind, Kind::HoldingRegister | Kind::InputRegister) {
            let mut data = Vec::with_capacity(quantity as usize * 2);
            for offset in 0..quantity {
                let value = guard
                    .values
                    .get(&(unit, kind, start + offset))
                    .copied()
                    .unwrap_or(0);
                data.extend_from_slice(&value.to_be_bytes());
            }
            data
        } else {
            // 位操作：打包为字节（LSB 优先）。
            let byte_len = quantity.div_ceil(8) as usize;
            let mut bytes = vec![0u8; byte_len];
            for offset in 0..quantity {
                if guard
                    .values
                    .get(&(unit, kind, start + offset))
                    .copied()
                    .unwrap_or(0)
                    != 0
                {
                    bytes[offset as usize / 8] |= 1 << (offset % 8);
                }
            }
            bytes
        };
        drop(guard);

        // 组装响应：MBAP 头 + unit + fc + byte count + 数据。
        let mut response = Vec::with_capacity(10 + data.len());
        response.extend_from_slice(&transaction_id.to_be_bytes());
        response.extend_from_slice(&[0x00, 0x00]);
        // MBAP length = unit + fc + byte count + 数据。
        let body_len = (data.len() + 3) as u16;
        response.extend_from_slice(&body_len.to_be_bytes());
        response.push(unit);
        response.push(function);
        response.push(data.len() as u8);
        response.extend_from_slice(&data);
        if stream.write_all(&response).is_err() {
            return;
        }
    }
}

/// 便捷：配置 host/port 的连接 JSON。
pub fn tcp_config(server: &MockServer, timeout_ms: u64) -> String {
    serde_json::json!({
        "mode": "tcp",
        "host": server.addr.ip().to_string(),
        "port": server.addr.port(),
        "timeout_ms": timeout_ms,
        "reconnect": true,
        "reconnect_max_attempts": 2,
        "reconnect_delay_ms": 50,
    })
    .to_string()
}
