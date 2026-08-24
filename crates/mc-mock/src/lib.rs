//! 同步 Mock 三菱 MC PLC Server（测试共用，非生产代码）。
//!
//! MC 3E 二进制帧：副头 0x0050 请求 / 0x00D0 应答（u16 LE），路由区
//! 五字段回声，批量读(0x0401/0x0402)/批量写(0x1401/0x1402) 应答；内存
//! 软元件表 `HashMap<(软元件代码, 编号), Cell>`，未写地址读出恒 0
//! （PLC 语义）。行为开关与 s7comm-mock/etherip-mock 逐条对齐。
//!
//! # 协议常量出处
//!
//! 对照《MELSEC SLMP 参考手册》(SH-081948ENG) QnA 兼容 3E 帧（Phase 0
//! 核对，与驱动侧常量经 golden 单测交叉固化）：
//!
//! - 帧结构：`[副头 u16 LE][网络号 u8][PC 号 u8][模块 I/O u16 LE]
//!   [站号 u8][数据长 u16 LE]` + 指令区；
//! - 指令区：`[监视定时器 u16 LE][指令 u16 LE][子指令 u16 LE]
//!   [软元件代码 u8][软元件号 3B LE][点数 u16 LE]` +（写）数据；
//! - 应答指令区：`[结束代码 u16 LE]` +（读）数据；
//! - 字数据小端；位串每字节 8 点 LSB 在前。

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ── 协议常量（见模块文档出处） ──

/// 副头：请求。
pub const SUBHEADER_REQUEST: u16 = 0x0050;
/// 副头：应答。
pub const SUBHEADER_RESPONSE: u16 = 0x00D0;
/// 指令：字批量读。
pub const CMD_READ_WORD: u16 = 0x0401;
/// 指令：位批量读。
pub const CMD_READ_BIT: u16 = 0x0402;
/// 指令：字批量写。
pub const CMD_WRITE_WORD: u16 = 0x1401;
/// 指令：位批量写。
pub const CMD_WRITE_BIT: u16 = 0x1402;

/// 内存单元：字存 u16、位存 bool 的统一载体（data 为 0/1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    /// 字值（位单元取低 1 位）。
    pub word: u16,
}

/// 捕获的一条读请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRecord {
    /// 软元件代码。
    pub code: u8,
    /// 起始编号。
    pub number: u32,
    /// 访问点数。
    pub points: u16,
}

/// 捕获的一条写请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecord {
    /// 软元件代码。
    pub code: u8,
    /// 起始编号。
    pub number: u32,
    /// 写入点数。
    pub points: u16,
    /// 写入数据字节（字 LE 或位串）。
    pub data: Vec<u8>,
}

/// Mock 服务器行为配置。
#[derive(Debug)]
pub struct McBehavior {
    /// 内存软元件表：(软元件代码, 编号) -> 单元。缺省读出恒 0。
    pub cells: HashMap<(u8, u32), Cell>,
    /// 每个请求的响应延迟。
    pub response_delay: Option<Duration>,
    /// 是否在响应前立即断开连接。
    pub drop_connection: bool,
    /// 概率超时注入 `(分子, 分母)`：命中静默不响应且不关连接
    /// （连接内 xorshift-32 固定种子可复现）。
    pub timeout_rate: Option<(u32, u32)>,
    /// 应答副头错乱为请求副头（测失步检测）。
    pub wrong_subheader: bool,
    /// 应答路由区回声错乱（各字段 +1，测失步检测）。
    pub wrong_routing_echo: bool,
    /// 应答声明数据长 +2（测长度自洽校验）。
    pub declare_wrong_data_length: bool,
    /// 强制所有批量请求返回该结束代码（错误映射注入）。
    pub force_end_code: Option<u16>,
    /// 写保护软元件集合：(代码, 编号)。命中返回 C050? ——用稳定注入码
    /// 0xC050 之外的自定义语义：直接复用通用拒绝码 0xC050 不在手册表内，
    /// 改用手册「访问被拒」类结束代码由驱动映射 mc_error_response 即可。
    pub deny_writes_at: HashSet<(u8, u32)>,
    /// 写保护命中时返回的结束代码（默认 C051 参数错——保守选择手册内
    /// 稳定值；测试按需覆盖）。
    pub deny_end_code: u16,
    /// 统计：收到的批量请求数。
    pub request_count: Arc<AtomicU32>,
    /// 收到的读请求（按到达顺序）。
    pub captured_reads: Arc<Mutex<Vec<ReadRecord>>>,
    /// 收到的写请求。
    pub captured_writes: Arc<Mutex<Vec<WriteRecord>>>,
}

impl Default for McBehavior {
    fn default() -> Self {
        Self::new()
    }
}

impl McBehavior {
    pub fn new() -> Self {
        Self {
            cells: HashMap::new(),
            response_delay: None,
            drop_connection: false,
            timeout_rate: None,
            wrong_subheader: false,
            wrong_routing_echo: false,
            declare_wrong_data_length: false,
            force_end_code: None,
            deny_writes_at: HashSet::new(),
            deny_end_code: 0xC051,
            request_count: Arc::new(AtomicU32::new(0)),
            captured_reads: Arc::new(Mutex::new(Vec::new())),
            captured_writes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 便捷：设置一个 D 寄存器字值。
    pub fn with_d(mut self, number: u32, value: u16) -> Self {
        self.cells.insert((0xA8, number), Cell { word: value });
        self
    }

    /// 便捷：置一个 M 位。
    pub fn with_m(mut self, number: u32, on: bool) -> Self {
        self.cells.insert(
            (0x90, number),
            Cell {
                word: u16::from(on),
            },
        );
        self
    }

    /// 读取内存单元当前字值（缺省 0——PLC 语义）。
    #[must_use]
    pub fn cell(&self, code: u8, number: u32) -> u16 {
        self.cells.get(&(code, number)).map_or(0, |c| c.word)
    }
}

/// 连接内 xorshift-32：概率超时注入的确定性序列源。
struct XorShift32(u32);

impl XorShift32 {
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
pub struct McServer {
    pub addr: std::net::SocketAddr,
    behavior: Arc<Mutex<McBehavior>>,
    handle: Option<thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl McServer {
    /// 启动服务器并返回地址。
    pub fn start(behavior: McBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("绑定测试端口失败");
        let addr = listener.local_addr().expect("读取监听地址失败");
        let behavior = Arc::new(Mutex::new(behavior));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        listener.set_nonblocking(true).expect("设置非阻塞失败");
        let behavior_ref = Arc::clone(&behavior);
        let stop_thread = Arc::clone(&stop);
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
    pub fn behavior(&self) -> Arc<Mutex<McBehavior>> {
        Arc::clone(&self.behavior)
    }

    /// 收到的批量请求总数。
    pub fn request_count(&self) -> u32 {
        self.behavior
            .lock()
            .expect("行为锁")
            .request_count
            .load(Ordering::Relaxed)
    }

    /// 收到的读请求快照。
    pub fn read_records(&self) -> Vec<ReadRecord> {
        self.behavior
            .lock()
            .expect("行为锁")
            .captured_reads
            .lock()
            .expect("读捕获锁")
            .clone()
    }

    /// 收到的写请求快照。
    pub fn write_records(&self) -> Vec<WriteRecord> {
        self.behavior
            .lock()
            .expect("行为锁")
            .captured_writes
            .lock()
            .expect("写捕获锁")
            .clone()
    }

    /// 读取内存单元当前字值。
    #[must_use]
    pub fn cell(&self, code: u8, number: u32) -> u16 {
        self.behavior.lock().expect("行为锁").cell(code, number)
    }

    /// 停止服务器（等待线程退出）。
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("服务器线程退出失败");
        }
    }
}

impl Drop for McServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// 连接级状态。
struct ConnState {
    rng: XorShift32,
    routing: [u8; 5],
}

fn handle_connection(
    mut stream: TcpStream,
    behavior: Arc<Mutex<McBehavior>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    // 监听 socket 非阻塞时 accept 出的连接继承非阻塞标志——恢复阻塞模式
    // 才能用 read_exact 等待完整帧（既有 mock 同坑同修）。
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);
    let mut state = ConnState {
        rng: XorShift32(0x9E37_79B9 | 1),
        routing: [0; 5],
    };
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // 副头(2) + 网络号(1) + PC 号(1) + 模块IO(2) + 站号(1) + 数据长(2)。
        let mut head = [0u8; 9];
        if stream.read_exact(&mut head).is_err() {
            return;
        }
        let data_len = usize::from(u16::from_le_bytes([head[7], head[8]]));
        let mut body = vec![0u8; data_len];
        if stream.read_exact(&mut body).is_err() {
            return;
        }

        behavior
            .lock()
            .expect("行为锁")
            .request_count
            .fetch_add(1, Ordering::Relaxed);

        // 行为开关：延迟 / 断连 / 概率超时（命中静默不关连接）。
        if let Some(d) = behavior.lock().expect("行为锁").response_delay {
            thread::sleep(d);
        }
        if behavior.lock().expect("行为锁").drop_connection {
            return;
        }
        if let Some((num, den)) = behavior.lock().expect("行为锁").timeout_rate
            && num > 0
            && den > 0
            && state.rng.next() % den < num
        {
            continue;
        }

        state.routing.copy_from_slice(&head[2..7]);
        let reply_body = match handle_command(&behavior, &body) {
            Some(r) => r,
            None => return, // 指令区结构坏：严格不宽容
        };
        let (wrong_sub, wrong_echo, wrong_len) = {
            let g = behavior.lock().expect("行为锁");
            (
                g.wrong_subheader,
                g.wrong_routing_echo,
                g.declare_wrong_data_length,
            )
        };
        send_response(
            &mut stream,
            &state.routing,
            &reply_body,
            wrong_sub,
            wrong_echo,
            wrong_len,
        );
    }
}

/// 处理指令区，返回应答体（结束代码 + 数据）。
fn handle_command(behavior: &Arc<Mutex<McBehavior>>, body: &[u8]) -> Option<Vec<u8>> {
    // [监视定时器 2][指令 2][子指令 2][软元件代码 1][编号 3][点数 2]…
    if body.len() < 12 {
        return None;
    }
    let command = u16::from_le_bytes([body[2], body[3]]);
    let _sub = u16::from_le_bytes([body[4], body[5]]);
    let code = body[6];
    let number = u32::from_le_bytes([body[7], body[8], body[9], 0]);
    let points = u16::from_le_bytes([body[10], body[11]]);

    let mut guard = behavior.lock().expect("行为锁");
    if let Some(forced) = guard.force_end_code {
        return Some(vec![forced.to_le_bytes()[0], forced.to_le_bytes()[1]]);
    }
    match command {
        CMD_READ_WORD | CMD_READ_BIT => {
            guard
                .captured_reads
                .lock()
                .expect("读捕获锁")
                .push(ReadRecord {
                    code,
                    number,
                    points,
                });
            let end = guard.force_end_code.unwrap_or(0).to_le_bytes();
            let mut out = vec![end[0], end[1]];
            if command == CMD_READ_WORD {
                for i in 0..points {
                    let v = guard.cell(code, u32::from(i) + number);
                    out.extend_from_slice(&v.to_le_bytes());
                }
            } else {
                // 位串：每字节 8 点 LSB 在前。
                let total = usize::from(points);
                for byte in 0..total.div_ceil(8) {
                    let mut b = 0u8;
                    for bit in 0..8 {
                        let idx = byte * 8 + bit;
                        if idx >= total {
                            break;
                        }
                        if guard.cell(code, number + idx as u32) & 1 != 0 {
                            b |= 1 << bit;
                        }
                    }
                    out.push(b);
                }
            }
            Some(out)
        }
        CMD_WRITE_WORD | CMD_WRITE_BIT => {
            let payload = &body[12..];
            let denied = guard.deny_writes_at.contains(&(code, number));
            guard
                .captured_writes
                .lock()
                .expect("写捕获锁")
                .push(WriteRecord {
                    code,
                    number,
                    points,
                    data: payload.to_vec(),
                });
            if denied {
                let ec = guard.deny_end_code.to_le_bytes();
                return Some(vec![ec[0], ec[1]]);
            }
            if command == CMD_WRITE_WORD {
                for i in 0..points {
                    let at = i as usize * 2;
                    if at + 2 > payload.len() {
                        return None;
                    }
                    let v = u16::from_le_bytes([payload[at], payload[at + 1]]);
                    guard
                        .cells
                        .insert((code, number + u32::from(i)), Cell { word: v });
                }
            } else {
                let total = usize::from(points);
                for (byte_i, b) in payload.iter().enumerate() {
                    for bit in 0..8 {
                        let idx = byte_i * 8 + bit;
                        if idx >= total {
                            break;
                        }
                        guard.cells.insert(
                            (code, number + idx as u32),
                            Cell {
                                word: u16::from(b & (1 << bit) != 0),
                            },
                        );
                    }
                }
            }
            Some(vec![0x00, 0x00]) // 结束代码 0 成功
        }
        _ => None, // 未知指令：严格不宽容
    }
}

/// 组装并发送一帧 3E 应答。
fn send_response(
    stream: &mut TcpStream,
    routing: &[u8; 5],
    reply_body: &[u8],
    wrong_subheader: bool,
    wrong_routing_echo: bool,
    wrong_data_length: bool,
) {
    use std::io::Write as _;
    let sub = if wrong_subheader {
        SUBHEADER_REQUEST
    } else {
        SUBHEADER_RESPONSE
    };
    let declared = reply_body.len() as u16 + if wrong_data_length { 2 } else { 0 };
    let mut frame = Vec::with_capacity(9 + reply_body.len());
    frame.extend_from_slice(&sub.to_le_bytes());
    for (i, b) in routing.iter().enumerate() {
        let v = if wrong_routing_echo {
            b.wrapping_add(1)
        } else {
            *b
        };
        frame.push(v);
        let _ = i;
    }
    frame.extend_from_slice(&declared.to_le_bytes());
    frame.extend_from_slice(reply_body);
    let _ = stream.write_all(&frame);
}

/// 配置 host/port 与路由区的连接 JSON（driver-mitsubishi-mc `mode=tcp`）。
pub fn tcp_config_full(
    host: &str,
    port: u16,
    timeout_ms: u64,
    network_no: u8,
    pc_no: u8,
    module_io: u16,
    module_station: u8,
) -> String {
    serde_json::json!({
        "mode": "tcp",
        "host": host,
        "port": port,
        "timeout_ms": timeout_ms,
        "reconnect": true,
        "reconnect_max_attempts": 2,
        "reconnect_delay_ms": 50,
        "network_no": network_no,
        "pc_no": pc_no,
        "module_io": module_io,
        "module_station": module_station,
    })
    .to_string()
}

/// 便捷：从运行中的服务器取地址配置连接 JSON（默认路由区）。
pub fn tcp_config(server: &McServer, timeout_ms: u64) -> String {
    tcp_config_full(
        &server.addr.ip().to_string(),
        server.addr.port(),
        timeout_ms,
        0,
        0,
        0x03FF,
        0,
    )
}
