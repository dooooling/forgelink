//! 同步 Mock Modbus TCP server（测试共用，非生产代码）。
//!
//! 供 Modbus Driver 与上层全链路测试复用：支持行为配置——寄存器表
//! （按 unit/kind/offset 的值）、指定地址返回异常、畸形异常响应、
//! 响应延迟、错误字节计数、连接立即断开，以及请求计数统计。
//!
//! # 注意
//!
//! 本 crate 仅用于测试，不参与生产构建路径。

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

/// 畸形异常响应模式（模拟坏实现，驱动必须按响应失步拒绝）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MalformedException {
    /// 异常码缺失：body 仅 1 字节（fc|0x80）。
    MissingCode,
    /// 异常响应多余字节：body 3 字节（fc|0x80 + 异常码 + 额外 1 字节）。
    ExtraByte,
}

/// 捕获的写请求（测试断言合并/功能码选择行为用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecord {
    /// 从站号。
    pub unit: u8,
    /// 功能码（FC05/06/15/16）。
    pub function: u8,
    /// 协议偏移起点。
    pub start_offset: u16,
    /// 数量（位数或寄存器数；FC05/06 恒为 1）。
    pub quantity: u16,
}

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
    /// 概率超时注入 `(分子, 分母)`：请求按该比例命中"静默不响应"路径
    /// ——不回包也不关连接（客户端按自身超时判定设备无响应，随后可在
    /// 同一连接上继续后续请求），模拟设备偶发无响应。`(1, 100)` = 1%；
    /// 分子为 0 等价未启用；分母必须非 0。命中与否由连接内 xorshift-32
    /// 伪随机判定（固定种子派生自从站号，同一请求序列跨次运行可复现，
    /// 不引入外部 rand 依赖）。命中时该请求不计入写捕获。
    pub timeout_rate: Option<(u32, u32)>,
    /// 声明错误的字节计数：Byte Count 与实际数据长度不符（模拟坏实现）。
    pub declare_wrong_byte_count: bool,
    /// 畸形异常响应模式（配合 `exception_at` 使用）。
    pub malformed_exception: Option<MalformedException>,
    /// 统计：收到的请求数。
    pub request_count: Arc<AtomicU32>,
    /// 收到的写请求（FC05/06/15/16，含被异常拒绝的请求）。
    pub captured_writes: Arc<Mutex<Vec<WriteRecord>>>,
}

impl MockBehavior {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            exception_at: HashMap::new(),
            response_delay: None,
            drop_connection: false,
            timeout_rate: None,
            declare_wrong_byte_count: false,
            malformed_exception: None,
            request_count: Arc::new(AtomicU32::new(0)),
            captured_writes: Arc::new(Mutex::new(Vec::new())),
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

    /// 便捷：设置概率超时注入（见 [`MockBehavior::timeout_rate`]）。
    pub fn with_timeout_rate(mut self, numerator: u32, denominator: u32) -> Self {
        self.timeout_rate = Some((numerator, denominator));
        self
    }
}

/// 连接内 xorshift-32 伪随机（非零状态）：概率超时注入的确定性序列源。
struct XorShift32(u32);

impl XorShift32 {
    /// 从从站号派生固定非零种子：同一连接上的请求序列跨次运行可复现。
    fn new(unit: u8) -> Self {
        let seed = (u32::from(unit) ^ 0x9E37_79B9) | 1;
        Self(seed)
    }

    fn next(&mut self) -> u32 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.0 = x;
        x
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

    /// 收到的写请求快照（FC05/06/15/16，按到达顺序）。
    pub fn write_records(&self) -> Vec<WriteRecord> {
        self.behavior
            .lock()
            .expect("行为锁")
            .captured_writes
            .lock()
            .expect("写请求锁")
            .clone()
    }

    /// 读取寄存器表当前值（写入生效断言用）。
    pub fn value(&self, unit: u8, kind: Kind, offset: u16) -> Option<u16> {
        self.behavior
            .lock()
            .expect("行为锁")
            .values
            .get(&(unit, kind, offset))
            .copied()
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

/// 功能码 -> 数据段（读 FC01~FC04 与写 FC05/06/15/16）。
fn kind_of(function: u8) -> Option<Kind> {
    match function {
        0x01 | 0x05 | 0x0F => Some(Kind::Coil),
        0x02 => Some(Kind::DiscreteInput),
        0x03 | 0x06 | 0x10 => Some(Kind::HoldingRegister),
        0x04 => Some(Kind::InputRegister),
        _ => None,
    }
}

/// 是否为写功能码（FC05/06/15/16）。
fn is_write_function(function: u8) -> bool {
    matches!(function, 0x05 | 0x06 | 0x0F | 0x10)
}

/// 组装异常响应帧（MBAP 头 + fc|0x80 + 异常码；畸形模式改变 body 字节数）。
fn exception_response(
    header: &[u8; 7],
    unit: u8,
    function: u8,
    code: u8,
    malformed: Option<MalformedException>,
) -> Vec<u8> {
    let mut body = vec![function | 0x80, code];
    match malformed {
        Some(MalformedException::MissingCode) => {
            body.pop();
        }
        Some(MalformedException::ExtraByte) => body.push(0x00),
        None => {}
    }
    let mut response = vec![0u8; 7 + body.len()];
    response[0..2].copy_from_slice(&header[0..2]);
    response[2..4].copy_from_slice(&[0x00, 0x00]);
    // MBAP length = unit + body 字节数。
    response[4..6].copy_from_slice(&((1 + body.len()) as u16).to_be_bytes());
    response[6] = unit;
    response[7..].copy_from_slice(&body);
    response
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
    // 概率超时注入的确定性随机序列（种子派生自首个请求的从站号）。
    let mut rng: Option<XorShift32> = None;
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

        // 概率超时注入：命中即静默跳过响应且不关连接——客户端按自身
        // 超时判定设备无响应，随后可在同一连接上继续后续请求。随机源
        // 在连接内惰性初始化（种子派生自从站号，序列可复现）。
        if let Some((num, den)) = guard.timeout_rate
            && num > 0
            && den > 0
        {
            let roll = rng.get_or_insert_with(|| XorShift32::new(unit)).next();
            if roll % den < num {
                continue;
            }
        }

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

        // 写功能码的数量字段位置与读不同：FC05/06 无数量（载荷首二字为值，
        // 数量恒 1）；FC15/16 的数量位于 rest[3..5]。
        let write_quantity = if is_write_function(function) {
            match function {
                0x05 | 0x06 => 1,
                _ => quantity,
            }
        } else {
            quantity
        };

        // 写请求捕获（供测试断言合并与功能码选择；含被异常拒绝的请求）。
        if is_write_function(function) {
            guard
                .captured_writes
                .lock()
                .expect("写请求锁")
                .push(WriteRecord {
                    unit,
                    function,
                    start_offset: start,
                    quantity: write_quantity,
                });
        }

        // 检查异常覆盖（区间起始地址）。
        let exception = (0..write_quantity).find_map(|offset| {
            guard
                .exception_at
                .get(&(unit, kind, start + offset))
                .copied()
        });

        // 读数据段：寄存器 2 字节大端；线圈/离散按位打包（LSB 优先）。
        let data: Vec<u8> = if let Some(code) = exception {
            // 异常响应：功能码置高位 + 异常码，连接保持。
            // 畸形模式按配置改变 body 字节数（缺异常码 / 多余字节）。
            let malformed = guard.malformed_exception;
            let response = exception_response(&header, unit, function, code, malformed);
            if stream.write_all(&response).is_err() {
                return;
            }
            continue;
        } else if is_write_function(function) {
            // 写请求：校验载荷合法性（违规回异常 0x03 illegal data value），
            // 应用到寄存器 bank 后按协议回显（addr + 值/数量）。
            let echo_tail: u16 = match function {
                0x05 => match quantity {
                    // quantity 此处即 FC05 的值字段（0xFF00/0x0000 之外非法）。
                    0xFF00 => 0xFF00,
                    0x0000 => 0x0000,
                    _ => {
                        let response = exception_response(
                            &header,
                            unit,
                            function,
                            0x03,
                            guard.malformed_exception,
                        );
                        if stream.write_all(&response).is_err() {
                            return;
                        }
                        continue;
                    }
                },
                0x06 => quantity,
                0x0F | 0x10 => {
                    let max = if function == 0x0F { 1_968u16 } else { 123 };
                    let byte_count = *rest.get(5).unwrap_or(&0) as usize;
                    let expected = if function == 0x0F {
                        write_quantity.div_ceil(8) as usize
                    } else {
                        write_quantity as usize * 2
                    };
                    if write_quantity == 0
                        || write_quantity > max
                        || byte_count != expected
                        || rest.len() < 6 + byte_count
                    {
                        let response = exception_response(
                            &header,
                            unit,
                            function,
                            0x03,
                            guard.malformed_exception,
                        );
                        if stream.write_all(&response).is_err() {
                            return;
                        }
                        continue;
                    }
                    write_quantity
                }
                _ => unreachable!("is_write_function 已限定"),
            };
            // 应用写入：线圈存 0/1，寄存器存 16 位原值。
            match function {
                0x05 => {
                    guard
                        .values
                        .insert((unit, kind, start), (echo_tail == 0xFF00) as u16);
                }
                0x06 => {
                    guard.values.insert((unit, kind, start), echo_tail);
                }
                0x0F => {
                    let bits = &rest[6..6 + write_quantity.div_ceil(8) as usize];
                    for offset in 0..write_quantity {
                        let bit = (bits[offset as usize / 8] >> (offset % 8)) & 1;
                        guard
                            .values
                            .insert((unit, kind, start + offset), u16::from(bit));
                    }
                }
                0x10 => {
                    let data = &rest[6..6 + write_quantity as usize * 2];
                    for offset in 0..write_quantity {
                        let base = offset as usize * 2;
                        let value = u16::from_be_bytes([data[base], data[base + 1]]);
                        guard.values.insert((unit, kind, start + offset), value);
                    }
                }
                _ => unreachable!("is_write_function 已限定"),
            }
            // 写响应：MBAP + unit + fc + addr(2) + 值/数量(2)，无 Byte Count。
            let mut response = Vec::with_capacity(12);
            response.extend_from_slice(&transaction_id.to_be_bytes());
            response.extend_from_slice(&[0x00, 0x00]);
            // MBAP length = unit(1) + fc(1) + addr(2) + 值/数量(2)。
            response.extend_from_slice(&6u16.to_be_bytes());
            response.push(unit);
            response.push(function);
            response.extend_from_slice(&start.to_be_bytes());
            response.extend_from_slice(&echo_tail.to_be_bytes());
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
        // 坏实现标志：Byte Count 声明多 1 字节、body 随之多 1 字节，
        // 与驱动期望（expected + 2）不符，必须被拒绝。
        let wrong_byte_count = guard.declare_wrong_byte_count;
        drop(guard);

        // 组装响应：MBAP 头 + unit + fc + byte count + 数据。
        let mut response = Vec::with_capacity(10 + data.len());
        response.extend_from_slice(&transaction_id.to_be_bytes());
        response.extend_from_slice(&[0x00, 0x00]);
        // MBAP length = unit + fc + byte count + 数据。
        let declared_len = if wrong_byte_count {
            data.len() + 1
        } else {
            data.len()
        };
        let body_len = (declared_len + 3) as u16;
        response.extend_from_slice(&body_len.to_be_bytes());
        response.push(unit);
        response.push(function);
        response.push(declared_len as u8);
        response.extend_from_slice(&data);
        if wrong_byte_count {
            response.push(0x00);
        }
        if stream.write_all(&response).is_err() {
            return;
        }
    }
}

/// 便捷：配置 host/port 的连接 JSON（Modbus Driver `mode=tcp`）。
pub fn tcp_config(server: &MockServer, timeout_ms: u64) -> String {
    tcp_config_at(
        &server.addr.ip().to_string(),
        server.addr.port(),
        timeout_ms,
    )
}

/// 便捷：按显式地址配置连接 JSON（[`tcp_config`] 的直传变体，供无法
/// 持有 [`MockServer`] 句柄的调用方使用——如 bench 的 workload 生成）。
pub fn tcp_config_at(host: &str, port: u16, timeout_ms: u64) -> String {
    serde_json::json!({
        "mode": "tcp",
        "host": host,
        "port": port,
        "timeout_ms": timeout_ms,
        "reconnect": true,
        "reconnect_max_attempts": 2,
        "reconnect_delay_ms": 50,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 发送一帧请求并读取完整响应（MBAP 头 + body）。
    fn exchange(addr: std::net::SocketAddr, unit: u8, body: &[u8], txn: u16) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).expect("连接 mock 失败");
        let mut frame = Vec::with_capacity(7 + body.len());
        frame.extend_from_slice(&txn.to_be_bytes());
        frame.extend_from_slice(&[0x00, 0x00]);
        // MBAP length 含 unit 字节。
        frame.extend_from_slice(&((body.len() + 1) as u16).to_be_bytes());
        frame.extend_from_slice(&[unit]);
        frame.extend_from_slice(body);
        stream.write_all(&frame).expect("发送请求失败");
        let mut header = [0u8; 7];
        stream.read_exact(&mut header).expect("读响应头失败");
        let length = u16::from_be_bytes([header[4], header[5]]) as usize;
        let mut response = header.to_vec();
        let mut rest = vec![0u8; length - 1];
        stream.read_exact(&mut rest).expect("读响应体失败");
        response.extend_from_slice(&rest);
        response
    }

    #[test]
    fn fc05_applies_coil_and_echos() {
        let server = MockServer::start(MockBehavior::new());
        // 写线圈 offset 3 = ON。
        let response = exchange(server.addr, 1, &[0x05, 0x00, 0x03, 0xFF, 0x00], 7);
        assert_eq!(&response[0..2], &[0x00, 0x07]); // 事务号回显
        assert_eq!(response[6], 1);
        assert_eq!(response[7], 0x05);
        assert_eq!(&response[8..12], &[0x00, 0x03, 0xFF, 0x00]); // addr + value 回显
        assert_eq!(server.value(1, Kind::Coil, 3), Some(1));

        // OFF。
        let _ = exchange(server.addr, 1, &[0x05, 0x00, 0x03, 0x00, 0x00], 8);
        assert_eq!(server.value(1, Kind::Coil, 3), Some(0));
    }

    #[test]
    fn fc05_rejects_non_standard_value() {
        // 协议规定 FC05 值只能 0xFF00/0x0000，其余必须异常（illegal data value）。
        let server = MockServer::start(MockBehavior::new());
        let response = exchange(server.addr, 1, &[0x05, 0x00, 0x03, 0x00, 0x01], 1);
        assert_eq!(response[7], 0x85);
        assert_eq!(response[8], 0x03);
        assert_eq!(server.value(1, Kind::Coil, 3), None);
    }

    #[test]
    fn fc06_applies_register_and_echos() {
        let server = MockServer::start(MockBehavior::new());
        let response = exchange(server.addr, 2, &[0x06, 0x00, 0x09, 0x13, 0x88], 3);
        assert_eq!(response[7], 0x06);
        assert_eq!(&response[8..12], &[0x00, 0x09, 0x13, 0x88]);
        assert_eq!(server.value(2, Kind::HoldingRegister, 9), Some(5000));
    }

    #[test]
    fn fc15_packs_bits_lsb_and_echos() {
        let server = MockServer::start(MockBehavior::new());
        // 从 offset 10 写 3 位 [true, false, true]：byte count=1，位流 0b101。
        let response = exchange(
            server.addr,
            1,
            &[0x0F, 0x00, 0x0A, 0x00, 0x03, 0x01, 0b101],
            4,
        );
        assert_eq!(response[7], 0x0F);
        assert_eq!(&response[8..12], &[0x00, 0x0A, 0x00, 0x03]); // addr + qty 回显
        assert_eq!(server.value(1, Kind::Coil, 10), Some(1));
        assert_eq!(server.value(1, Kind::Coil, 11), Some(0));
        assert_eq!(server.value(1, Kind::Coil, 12), Some(1));
    }

    #[test]
    fn fc16_applies_registers_and_echos() {
        let server = MockServer::start(MockBehavior::new());
        // 从 offset 2 写 2 寄存器 [0x1234, 0x5678]。
        let response = exchange(
            server.addr,
            1,
            &[0x10, 0x00, 0x02, 0x00, 0x02, 0x04, 0x12, 0x34, 0x56, 0x78],
            5,
        );
        assert_eq!(response[7], 0x10);
        assert_eq!(&response[8..12], &[0x00, 0x02, 0x00, 0x02]);
        assert_eq!(server.value(1, Kind::HoldingRegister, 2), Some(0x1234));
        assert_eq!(server.value(1, Kind::HoldingRegister, 3), Some(0x5678));
    }

    #[test]
    fn fc15_fc16_reject_bad_quantity_or_byte_count() {
        let server = MockServer::start(MockBehavior::new());
        // FC16 数量超上限（124 > 123）。
        let response = exchange(
            server.addr,
            1,
            &[0x10, 0x00, 0x00, 0x00, 0x7C, 0xF8, 0x00],
            1,
        );
        assert_eq!(response[7], 0x90);
        assert_eq!(response[8], 0x03);
        // FC16 字节计数与数量不符。
        let response = exchange(
            server.addr,
            1,
            &[0x10, 0x00, 0x00, 0x00, 0x02, 0x03, 0x11, 0x22, 0x33],
            2,
        );
        assert_eq!(response[7], 0x90);
        assert_eq!(response[8], 0x03);
        // FC15 字节计数与位数不符。
        let response = exchange(
            server.addr,
            1,
            &[0x0F, 0x00, 0x00, 0x00, 0x09, 0x01, 0xFF],
            3,
        );
        assert_eq!(response[7], 0x8F);
        assert_eq!(response[8], 0x03);
        // FC15 数量为 0。
        let response = exchange(server.addr, 1, &[0x0F, 0x00, 0x00, 0x00, 0x00, 0x00], 4);
        assert_eq!(response[7], 0x8F);
        assert_eq!(response[8], 0x03);
    }

    #[test]
    fn write_exception_injection_and_malformed_modes() {
        let behavior = MockBehavior::new();
        let server = MockServer::start(behavior);
        server
            .behavior()
            .lock()
            .unwrap()
            .exception_at
            .insert((1, Kind::HoldingRegister, 5), 0x02);

        // 注入异常覆盖写响应。
        let response = exchange(server.addr, 1, &[0x06, 0x00, 0x05, 0x00, 0x01], 9);
        assert_eq!(response[7], 0x86);
        assert_eq!(response[8], 0x02);
        assert_eq!(
            server.value(1, Kind::HoldingRegister, 5),
            None,
            "异常时不得应用写入"
        );

        // 畸形模式同样作用于写异常响应（多余字节）。
        server.behavior().lock().unwrap().malformed_exception = Some(MalformedException::ExtraByte);
        let response = exchange(server.addr, 1, &[0x06, 0x00, 0x05, 0x00, 0x01], 10);
        assert_eq!(response.len(), 7 + 3, "ExtraByte 模式 body 多 1 字节");
        assert_eq!(response[7], 0x86);
        assert_eq!(response[8], 0x02);
        assert_eq!(response[9], 0x00);
    }

    #[test]
    fn captured_writes_record_requests() {
        let server = MockServer::start(MockBehavior::new());
        let _ = exchange(server.addr, 1, &[0x05, 0x00, 0x00, 0xFF, 0x00], 1);
        let _ = exchange(server.addr, 1, &[0x06, 0x00, 0x02, 0x00, 0x2A], 2);
        let _ = exchange(
            server.addr,
            1,
            &[0x10, 0x00, 0x04, 0x00, 0x02, 0x04, 0xAA, 0xBB, 0xCC, 0xDD],
            3,
        );
        assert_eq!(
            server.write_records(),
            vec![
                WriteRecord {
                    unit: 1,
                    function: 0x05,
                    start_offset: 0,
                    quantity: 1
                },
                WriteRecord {
                    unit: 1,
                    function: 0x06,
                    start_offset: 2,
                    quantity: 1
                },
                WriteRecord {
                    unit: 1,
                    function: 0x10,
                    start_offset: 4,
                    quantity: 2
                },
            ]
        );
    }

    /// 发送一帧 FC03 读请求（不等待响应）。
    fn send_read(stream: &mut TcpStream, txn: u16, unit: u8) {
        let frame = [
            (txn >> 8) as u8,
            txn as u8,
            0x00,
            0x00,
            0x00,
            0x06,
            unit,
            0x03,
            0x00,
            0x00,
            0x00,
            0x01,
        ];
        stream.write_all(&frame).expect("发送请求失败");
    }

    /// `(1, 1)` 全命中：请求一律静默且**不关连接**——错误类别必须是读
    /// 超时而非 EOF，后续请求在同一连接上继续被（同样）静默处理。
    #[test]
    fn timeout_rate_full_hit_stays_silent_and_connected() {
        let server = MockServer::start(MockBehavior::new().with_timeout_rate(1, 1));
        let mut stream = TcpStream::connect(server.addr).expect("连接 mock 失败");
        stream
            .set_read_timeout(Some(Duration::from_millis(300)))
            .expect("设置读超时失败");

        for txn in 1..=2u16 {
            send_read(&mut stream, txn, 7);
            let mut header = [0u8; 7];
            let err = stream.read_exact(&mut header).unwrap_err();
            assert!(
                matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ),
                "第 {txn} 个请求应为读超时（静默）而非断连: {err:?}"
            );
        }
        assert_eq!(server.request_count(), 2, "请求计数不受静默影响");
    }

    /// `(1, 2)` 部分命中：命中与否的序列由连接内 xorshift-32 决定。本
    /// 测试以本地同构实现复算期望序列并逐请求比对——固化"同一从站号
    /// 同一请求序列跨次运行可复现"的契约，防止算法被无意改动。
    #[test]
    fn timeout_rate_partial_hits_are_reproducible() {
        let server = MockServer::start(MockBehavior::new().with_timeout_rate(1, 2));
        let unit = 9u8;
        let mut stream = TcpStream::connect(server.addr).expect("连接 mock 失败");
        stream
            .set_read_timeout(Some(Duration::from_millis(300)))
            .expect("设置读超时失败");

        // 本地复算期望命中序列（与 XorShift32::new(unit) 同种子同迭代）。
        let mut x = (u32::from(unit) ^ 0x9E37_79B9) | 1;
        let mut hits = Vec::new();
        for _ in 0..8 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            // 判定谓词与实现一致：roll % den < num，此处 (1, 2) 即偶数命中。
            hits.push(x % 2 == 0);
        }

        for (i, txn) in (1..=8u16).enumerate() {
            send_read(&mut stream, txn, unit);
            let mut header = [0u8; 7];
            let result = stream.read_exact(&mut header);
            if hits[i] {
                let err = result.unwrap_err();
                assert!(
                    matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ),
                    "请求 {} 应命中静默: {err:?}",
                    i + 1
                );
            } else {
                result.expect("未命中的请求应立即得到响应");
                let length = u16::from_be_bytes([header[4], header[5]]) as usize;
                let mut rest = vec![0u8; length - 1];
                stream.read_exact(&mut rest).expect("读响应体失败");
                assert_eq!(rest[0], 0x03, "应为 FC03 正常响应");
            }
        }
    }
}
