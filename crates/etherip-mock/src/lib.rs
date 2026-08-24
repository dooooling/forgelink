//! 同步 Mock EtherNet/IP PLC Server（测试共用，非生产代码）。
//!
//! 封装层（EN/IP，24B 小端头）RegisterSession/SendRRData/UnregisterSession
//! 与 CIP Message Router 的 Read Tag(0x4C)/Write Tag(0x4D)/Multi-Service
//! (0x0A) 应答；内存标签表为 `HashMap<String, {type_code, bytes}>`，
//! **大小写敏感**精确匹配。行为开关与 s7comm-mock 逐条对齐。
//!
//! # 协议常量出处
//!
//! 全部常量对照 Wireshark `enip`/`cip` dissector 文档与 ODVA CIP Vol.1
//! Appendix C（Phase 0 核对步骤，与驱动侧常量经 golden 单测交叉固化）：
//!
//! - 封装头 24 字节小端：`[command u16][length u16][session u32]
//!   [status u32][sender-context 8B][options u32]`；
//! - 命令：RegisterSession=0x0065（体 `[version=1 u16][option=0 u16]`，
//!   应答在头内携带分配的 session handle）、UnregisterSession=0x0066、
//!   SendRRData=0x00F0；
//! - SendRRData 体：`[interface-handle u32=0][timeout u16][item-count
//!   u16=2][地址项 type=0x0000 len=0][数据项 type=0x00B1 len=N + CIP]`；
//! - CIP 类型码：C1 BOOL/C2 SINT/C3 INT/C4 DINT/C5 LINT/C6 USINT/
//!   C7 UINT/C8 UDINT/C9 ULINT/CA REAL(f32)/CB LREAL(f64)，载荷小端；
//! - 服务：Read Tag=0x4C（数据 `[元素数 u16]`）、Write Tag=0x4D（数据
//!   `[类型 u16][元素数 u16][载荷]`）、Multi-Service=0x0A（路径 =
//!   Connection Manager `20 06 24 01`；数据 `[子服务数][偏移表 u16×N]
//!   [子请求拼接]`，偏移相对子服务数域起点）；应答 service = 请求|0x80。

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ── 协议常量（见模块文档出处） ──

/// 命令：RegisterSession。
pub const CMD_REGISTER_SESSION: u16 = 0x00_65;
/// 命令：UnregisterSession。
pub const CMD_UNREGISTER_SESSION: u16 = 0x00_66;
/// 命令：SendRRData。
pub const CMD_SEND_RR_DATA: u16 = 0x00_F0;
/// CIP 服务：Read Tag。
pub const SVC_READ_TAG: u8 = 0x4C;
/// CIP 服务：Write Tag。
pub const SVC_WRITE_TAG: u8 = 0x4D;
/// CIP 服务：Multi-Service。
pub const SVC_MULTI: u8 = 0x0A;
/// CIP 类型码：BOOL（1 字节 0/1）。
pub const TYPE_BOOL: u16 = 0xC1;
/// CIP 类型码：SINT（i8）。
pub const TYPE_SINT: u16 = 0xC2;
/// CIP 类型码：INT（i16）。
pub const TYPE_INT: u16 = 0xC3;
/// CIP 类型码：DINT（i32）。
pub const TYPE_DINT: u16 = 0xC4;
/// CIP 类型码：LINT（i64）。
pub const TYPE_LINT: u16 = 0xC5;
/// CIP 类型码：USINT（u8）。
pub const TYPE_USINT: u16 = 0xC6;
/// CIP 类型码：UINT（u16）。
pub const TYPE_UINT: u16 = 0xC7;
/// CIP 类型码：UDINT（u32）。
pub const TYPE_UDINT: u16 = 0xC8;
/// CIP 类型码：ULINT（u64）。
pub const TYPE_ULINT: u16 = 0xC9;
/// CIP 类型码：REAL（f32）。
pub const TYPE_REAL: u16 = 0xCA;
/// CIP 类型码：LREAL（f64）。
pub const TYPE_LREAL: u16 = 0xCB;
/// 子服务返回码：成功。
pub const STATUS_SUCCESS: u8 = 0x00;
/// 子服务返回码：访问被拒（privilege violation；注入用稳定值）。
pub const STATUS_ACCESS_DENIED: u8 = 0x0F;
/// 子服务返回码：标签不存在（mock 缺省缺失语义）。
pub const STATUS_TAG_NOT_FOUND: u8 = 0x14;

const ENIP_HEADER_LEN: usize = 24;

/// 内存标签值：CIP 类型码 + 载荷字节（LE）。
#[derive(Debug, Clone)]
pub struct TagValue {
    /// CIP 类型码。
    pub type_code: u16,
    /// 载荷（小端）。
    pub data: Vec<u8>,
}

/// 捕获的一条写请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecord {
    /// 标签路径（原样大小写）。
    pub tag: String,
    /// 写入使用的 CIP 类型码。
    pub type_code: u16,
    /// 写入载荷。
    pub data: Vec<u8>,
}

/// 捕获的一条读请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRecord {
    /// 标签路径（原样大小写）。
    pub tag: String,
}

/// Mock 服务器行为配置。
#[derive(Debug)]
pub struct MockBehavior {
    /// 内存标签表：标签路径 -> 值（大小写敏感精确匹配）。
    pub tags: HashMap<String, TagValue>,
    /// 每个 SendRRData 的响应延迟。
    pub response_delay: Option<Duration>,
    /// 是否在响应前立即断开连接。
    pub drop_connection: bool,
    /// 概率超时注入 `(分子, 分母)`：命中静默不响应且不关连接
    /// （连接内 xorshift-32 固定种子派生自 session handle，可复现）。
    pub timeout_rate: Option<(u32, u32)>,
    /// RegisterSession 直接否定（status 0x69）——测 connection_failed。
    pub register_reject: bool,
    /// 写保护标签集合：命中的 Write Tag 子服务返回 0x0F。
    pub deny_writes_at: HashSet<String>,
    /// 应答 session handle +1（测失步检测）。
    pub wrong_session_handle: bool,
    /// 应答 sender context 回显错乱（+1，测失步检测）。
    pub wrong_sender_context: bool,
    /// Multi 应答子服务计数错乱（测结构校验）。
    pub declare_wrong_item_count: bool,
    /// 封装头长度域损坏（+10，测分帧校验）。
    pub bad_frame_length: bool,
    /// 统计：收到的 SendRRData 请求数。
    pub request_count: Arc<AtomicU32>,
    /// 累计注册会话数（重连必须重新 Register 的断言依据）。
    pub registered_sessions: Arc<AtomicU32>,
    /// 收到的读请求（按到达顺序）。
    pub captured_reads: Arc<Mutex<Vec<ReadRecord>>>,
    /// 收到的写请求。
    pub captured_writes: Arc<Mutex<Vec<WriteRecord>>>,
}

impl Default for MockBehavior {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBehavior {
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
            response_delay: None,
            drop_connection: false,
            timeout_rate: None,
            register_reject: false,
            deny_writes_at: HashSet::new(),
            wrong_session_handle: false,
            wrong_sender_context: false,
            declare_wrong_item_count: false,
            bad_frame_length: false,
            request_count: Arc::new(AtomicU32::new(0)),
            registered_sessions: Arc::new(AtomicU32::new(0)),
            captured_reads: Arc::new(Mutex::new(Vec::new())),
            captured_writes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 便捷：写入一个 DINT 标签。
    pub fn with_dint(mut self, tag: &str, value: i32) -> Self {
        self.tags.insert(
            tag.to_owned(),
            TagValue {
                type_code: TYPE_DINT,
                data: value.to_le_bytes().to_vec(),
            },
        );
        self
    }

    /// 便捷：写入一个 REAL 标签。
    pub fn with_real(mut self, tag: &str, value: f32) -> Self {
        self.tags.insert(
            tag.to_owned(),
            TagValue {
                type_code: TYPE_REAL,
                data: value.to_le_bytes().to_vec(),
            },
        );
        self
    }

    /// 便捷：写入一个 BOOL 标签。
    pub fn with_bool(mut self, tag: &str, on: bool) -> Self {
        self.tags.insert(
            tag.to_owned(),
            TagValue {
                type_code: TYPE_BOOL,
                data: vec![u8::from(on)],
            },
        );
        self
    }

    /// 便捷：写入一个 UINT 标签。
    pub fn with_uint(mut self, tag: &str, value: u16) -> Self {
        self.tags.insert(
            tag.to_owned(),
            TagValue {
                type_code: TYPE_UINT,
                data: value.to_le_bytes().to_vec(),
            },
        );
        self
    }
}

/// 连接内 xorshift-32：概率超时注入的确定性序列源（种子派生自
/// session handle，同一连接跨次运行可复现）。
struct XorShift32(u32);

impl XorShift32 {
    fn new(seed: u32) -> Self {
        Self(seed | 1)
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
    pub fn behavior(&self) -> Arc<Mutex<MockBehavior>> {
        Arc::clone(&self.behavior)
    }

    /// 收到的 SendRRData 请求总数。
    pub fn request_count(&self) -> u32 {
        self.behavior
            .lock()
            .expect("行为锁")
            .request_count
            .load(Ordering::Relaxed)
    }

    /// 累计注册会话数（断线重连须重新 Register 的断言依据）。
    pub fn registered_sessions(&self) -> u32 {
        self.behavior
            .lock()
            .expect("行为锁")
            .registered_sessions
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

    /// 读取内存标签当前值。
    #[must_use]
    pub fn tag_value(&self, tag: &str) -> Option<TagValue> {
        self.behavior.lock().expect("行为锁").tags.get(tag).cloned()
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

/// 连接级状态：已分配的 session handle（未注册时为 None）。
struct ConnState {
    session: Option<u32>,
    rng: XorShift32,
}

fn handle_connection(
    mut stream: TcpStream,
    behavior: Arc<Mutex<MockBehavior>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    // 监听 socket 非阻塞时 accept 出的连接继承非阻塞标志——恢复阻塞模式
    // 才能用 read_exact 等待完整帧（modbus/s7comm-mock 同坑同修）。
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);
    let mut state = ConnState {
        session: None,
        rng: XorShift32::new(1),
    };
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let mut header = [0u8; ENIP_HEADER_LEN];
        if stream.read_exact(&mut header).is_err() {
            return;
        }
        let command = u16::from_le_bytes([header[0], header[1]]);
        let body_len = usize::from(u16::from_le_bytes([header[2], header[3]]));
        let session_in = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let context: [u8; 8] = header[12..20].try_into().expect("context 切片");
        let mut body = vec![0u8; body_len];
        if stream.read_exact(&mut body).is_err() {
            return;
        }

        match command {
            CMD_REGISTER_SESSION => {
                let reject = behavior.lock().expect("行为锁").register_reject;
                let (status, session_out) = if reject {
                    (0x69, 0)
                } else {
                    let session = state.session.get_or_insert_with(|| {
                        behavior
                            .lock()
                            .expect("行为锁")
                            .registered_sessions
                            .fetch_add(1, Ordering::Relaxed)
                            + 1
                    });
                    state.rng = XorShift32::new(*session);
                    (0, *session)
                };
                // 应答体：[version=1 u16][option=0 u16][session handle u32]
                //（与驱动 parse_register_session_reply 契约一致）。
                let mut body = vec![0x01, 0x00, 0x00, 0x00];
                body.extend_from_slice(&session_out.to_le_bytes());
                send_enip(
                    &mut stream,
                    &behavior,
                    command,
                    session_out,
                    status,
                    echo_ctx(&behavior, context),
                    &body,
                );
            }
            CMD_UNREGISTER_SESSION => {
                state.session = None;
                send_enip(
                    &mut stream,
                    &behavior,
                    command,
                    session_in,
                    0,
                    echo_ctx(&behavior, context),
                    &[],
                );
            }
            CMD_SEND_RR_DATA => {
                let Some(session) = state.session else {
                    return; // 未注册即发数据：严格不宽容
                };
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
                let Some(cip) = extract_cip(&body) else {
                    return; // RRData 结构坏：严格不宽容
                };
                let cip_reply = match handle_cip(&behavior, &cip) {
                    Some(r) => r,
                    None => return,
                };
                let reply_body = build_rr_data(&cip_reply);
                let (session_out, ctx_out) = {
                    let guard = behavior.lock().expect("行为锁");
                    let s = if guard.wrong_session_handle {
                        session.wrapping_add(1)
                    } else {
                        session
                    };
                    let c = if guard.wrong_sender_context {
                        bumped(context)
                    } else {
                        context
                    };
                    (s, c)
                };
                send_enip(
                    &mut stream,
                    &behavior,
                    CMD_SEND_RR_DATA,
                    session_out,
                    0,
                    ctx_out,
                    &reply_body,
                );
            }
            _ => return, // 其余命令：严格不宽容
        }
    }
}

/// 发送一帧封装层应答（bad_frame_length 开关注入长度域损坏）。
fn send_enip(
    stream: &mut TcpStream,
    behavior: &Arc<Mutex<MockBehavior>>,
    command: u16,
    session: u32,
    status: u32,
    context: [u8; 8],
    body: &[u8],
) {
    let bad_len = behavior.lock().expect("行为锁").bad_frame_length;
    let declared = body.len() as u16 + if bad_len { 10 } else { 0 };
    let mut frame = Vec::with_capacity(ENIP_HEADER_LEN + body.len());
    frame.extend_from_slice(&command.to_le_bytes());
    frame.extend_from_slice(&declared.to_le_bytes());
    frame.extend_from_slice(&session.to_le_bytes());
    frame.extend_from_slice(&status.to_le_bytes());
    frame.extend_from_slice(&context);
    frame.extend_from_slice(&[0; 4]); // options
    frame.extend_from_slice(body);
    let _ = stream.write_all(&frame);
}

/// sender context 回显（wrong_sender_context 开关注入 +1 错乱——由调用方
/// 在需要处直接构造，此处仅透传；保留函数统一出口便于调试断点）。
fn echo_ctx(_behavior: &Arc<Mutex<MockBehavior>>, context: [u8; 8]) -> [u8; 8] {
    context
}

/// context +1（失步注入）。
fn bumped(mut context: [u8; 8]) -> [u8; 8] {
    for b in context.iter_mut() {
        let (nv, overflow) = b.overflowing_add(1);
        *b = nv;
        if !overflow {
            break;
        }
    }
    context
}

/// 从 SendRRData 体剥出 CIP 载荷：`[interface 4B][timeout 2B]
/// [item-count 2B][地址项 type=0000 len=0][数据项 type=B1 len=N + N 字节]`。
fn extract_cip(body: &[u8]) -> Option<Vec<u8>> {
    if body.len() < 12 {
        return None;
    }
    let item_count = u16::from_le_bytes([body[6], body[7]]);
    if item_count != 2 {
        return None;
    }
    let addr_type = u16::from_le_bytes([body[8], body[9]]);
    let addr_len = usize::from(u16::from_le_bytes([body[10], body[11]]));
    if addr_type != 0x0000 || addr_len != 0 {
        return None;
    }
    let data_off = 12;
    if body.len() < data_off + 4 {
        return None;
    }
    let data_type = u16::from_le_bytes([body[data_off], body[data_off + 1]]);
    let data_len = usize::from(u16::from_le_bytes([body[data_off + 2], body[data_off + 3]]));
    if data_type != 0x00_B1 || body.len() < data_off + 4 + data_len {
        return None;
    }
    Some(body[data_off + 4..data_off + 4 + data_len].to_vec())
}

/// 把 CIP 应答包进 SendRRData 体。
fn build_rr_data(cip: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + cip.len());
    out.extend_from_slice(&0u32.to_le_bytes()); // interface handle
    out.extend_from_slice(&0u16.to_le_bytes()); // timeout
    out.extend_from_slice(&2u16.to_le_bytes()); // item count
    out.extend_from_slice(&0x0000u16.to_le_bytes()); // 地址项 type
    out.extend_from_slice(&0u16.to_le_bytes()); // 地址项 len
    out.extend_from_slice(&0x00B1u16.to_le_bytes()); // 数据项 type
    out.extend_from_slice(&(cip.len() as u16).to_le_bytes());
    out.extend_from_slice(cip);
    out
}

/// 处理一条 CIP 消息，返回应答（首字节 service|0x80）。
///
/// Multi-Service 解包为多个子请求逐个处理再按偏移表拼回应答；
/// 结构性不符返回 `None`（调用方断连——严格不宽容）。
fn handle_cip(behavior: &Arc<Mutex<MockBehavior>>, cip: &[u8]) -> Option<Vec<u8>> {
    if cip.len() < 2 {
        return None;
    }
    let service = cip[0];
    let path_words = usize::from(cip[1]);
    let data_start = 2 + path_words * 2;
    if cip.len() < data_start {
        return None;
    }
    match service {
        SVC_MULTI => handle_multi(behavior, &cip[data_start..]),
        SVC_READ_TAG => {
            let tag = extract_tag_path(&cip[2..data_start])?;
            read_one_tag(behavior, &tag)
        }
        SVC_WRITE_TAG => {
            let tag = extract_tag_path(&cip[2..data_start])?;
            let denied = behavior
                .lock()
                .expect("行为锁")
                .deny_writes_at
                .contains(&tag);
            let status = if denied {
                STATUS_ACCESS_DENIED
            } else {
                STATUS_SUCCESS
            };
            if !denied {
                write_one_tag(behavior, &tag, &cip[data_start..])?;
            }
            Some(vec![SVC_WRITE_TAG | 0x80, 0x00, status])
        }
        _ => None,
    }
}

/// 从 EPATH 字节提取标签名（0x91 段：[0x91][len][ascii…][pad]）。
fn extract_tag_path(path: &[u8]) -> Option<String> {
    if path.first() != Some(&0x91) {
        return None;
    }
    let len = usize::from(*path.get(1)?);
    Some(String::from_utf8_lossy(&path[2..2 + len]).into_owned())
}

/// 处理单条 Read Tag：查表回 `[service|0x80][status][type u16][data]`
/// 或 `[service|0x80][status 非 0]`。
fn read_one_tag(behavior: &Arc<Mutex<MockBehavior>>, tag: &str) -> Option<Vec<u8>> {
    behavior
        .lock()
        .expect("行为锁")
        .captured_reads
        .lock()
        .expect("读捕获锁")
        .push(ReadRecord {
            tag: tag.to_owned(),
        });
    let guard = behavior.lock().expect("行为锁");
    match guard.tags.get(tag) {
        Some(value) => {
            let mut reply = vec![SVC_READ_TAG | 0x80, 0x00, STATUS_SUCCESS, 0x00];
            reply.extend_from_slice(&value.type_code.to_le_bytes());
            reply.extend_from_slice(&value.data);
            Some(reply)
        }
        None => Some(vec![SVC_READ_TAG | 0x80, 0x00, STATUS_TAG_NOT_FOUND]),
    }
}

/// 处理单条 Write Tag：应用写并捕获；拒绝注入返回 0x0F 子状态。
///
/// 返回 None 仅在请求结构非法时（调用方断连）。
fn write_one_tag(
    behavior: &Arc<Mutex<MockBehavior>>,
    tag: &str,
    request_data: &[u8],
) -> Option<()> {
    // 请求数据：[type u16][elements u16][载荷]。
    if request_data.len() < 4 {
        return None;
    }
    let type_code = u16::from_le_bytes([request_data[0], request_data[1]]);
    let payload = &request_data[4..];
    behavior
        .lock()
        .expect("行为锁")
        .captured_writes
        .lock()
        .expect("写捕获锁")
        .push(WriteRecord {
            tag: tag.to_owned(),
            type_code,
            data: payload.to_vec(),
        });
    let mut guard = behavior.lock().expect("行为锁");
    if guard.deny_writes_at.contains(tag) {
        return Some(()); // 拒绝由调用方以 status 0x0F 应答
    }
    guard.tags.insert(
        tag.to_owned(),
        TagValue {
            type_code,
            data: payload.to_vec(),
        },
    );
    Some(())
}

/// 处理 Multi-Service 数据区：`[子服务数][偏移表 u16×N][子请求拼接]`，
/// 偏移相对子服务数域起点。逐子请求处理，按同构偏移表拼回应答。
///
/// 拒绝注入的 Write Tag 子服务以子状态 0x0F 应答（不应用写入）。
fn handle_multi(behavior: &Arc<Mutex<MockBehavior>>, data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 2 {
        return None;
    }
    let count = usize::from(u16::from_le_bytes([data[0], data[1]]));
    if count == 0 || data.len() < 2 + count * 2 {
        return None;
    }
    let mut offsets = Vec::with_capacity(count);
    for i in 0..count {
        let off = usize::from(u16::from_le_bytes([data[2 + i * 2], data[3 + i * 2]]));
        offsets.push(off);
    }

    struct SubReply {
        bytes: Vec<u8>,
    }

    let mut sub_replies: Vec<SubReply> = Vec::with_capacity(count);
    for (i, &off) in offsets.iter().enumerate() {
        if off >= data.len() || data.len() < off + 2 {
            return None;
        }
        // 子请求边界：偏移表是权威——本子请求的可用区间到下一偏移
        // （或数据区末尾）为止；Write 载荷据此截断，防止吞并后续子请求。
        let sub_end = offsets.get(i + 1).map_or(data.len(), |&next| next);
        let service = data[off];
        let path_words = usize::from(data[off + 1]);
        let sub_data_start = off + 2 + path_words * 2;
        if sub_end < sub_data_start {
            return None;
        }
        match service {
            SVC_READ_TAG => {
                let tag = extract_tag_path(&data[off + 2..sub_data_start])?;
                let bytes = read_one_tag(behavior, &tag)?;
                sub_replies.push(SubReply { bytes });
            }
            SVC_WRITE_TAG => {
                let tag = extract_tag_path(&data[off + 2..sub_data_start])?;
                let denied = behavior
                    .lock()
                    .expect("行为锁")
                    .deny_writes_at
                    .contains(&tag);
                write_one_tag(behavior, &tag, &data[sub_data_start..sub_end])?;
                let status = if denied {
                    STATUS_ACCESS_DENIED
                } else {
                    STATUS_SUCCESS
                };
                sub_replies.push(SubReply {
                    bytes: vec![SVC_WRITE_TAG | 0x80, 0x00, status],
                });
            }
            other => {
                // 未知子服务：status 0x08 否定子应答。
                sub_replies.push(SubReply {
                    bytes: vec![other | 0x80, 0x00, 0x08],
                });
            }
        }
    }

    // 拼装应答数据区：[count][偏移表][子应答拼接]，偏移指向各子应答。
    let wrong_count = behavior.lock().expect("行为锁").declare_wrong_item_count;
    let declared_count = if wrong_count {
        count.wrapping_add(1) as u16
    } else {
        count as u16
    };
    let mut body = Vec::new();
    body.extend_from_slice(&declared_count.to_le_bytes());
    let mut running_offset = 2 + count * 2;
    let mut replies = Vec::with_capacity(sub_replies.len());
    for sub in &sub_replies {
        body.extend_from_slice(&(running_offset as u16).to_le_bytes());
        running_offset += sub.bytes.len();
        replies.push(sub.bytes.clone());
    }
    for reply in replies {
        body.extend_from_slice(&reply);
    }
    let mut out = vec![SVC_MULTI | 0x80, 0x00, STATUS_SUCCESS, 0x00];
    out.extend_from_slice(&body);
    Some(out)
}

/// 配置 host/port 的连接 JSON（driver-ether-ip `mode=tcp`）。
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

/// 便捷：从运行中的服务器取地址配置连接 JSON。
pub fn tcp_config(server: &MockServer, timeout_ms: u64) -> String {
    tcp_config_at(
        &server.addr.ip().to_string(),
        server.addr.port(),
        timeout_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用原始客户端：手工组帧与 mock 往返（固化协议常量契约）。
    struct RawClient {
        stream: TcpStream,
        context: [u8; 8],
        session: u32,
    }

    impl RawClient {
        fn connect(addr: std::net::SocketAddr, rack: u8, slot: u8) -> Self {
            let mut stream = TcpStream::connect(addr).expect("连接 mock 失败");
            // RegisterSession：头 24B + 体 [01 00 00 00]；context = rack<<8|slot。
            let context = [rack, slot, 0, 0, 0, 0, 0, 1];
            let session = send_and_read_session(&mut stream, &context);
            assert!(session > 0, "应分配非零 session handle");
            Self {
                stream,
                context,
                session,
            }
        }

        fn rr_data(&mut self, cip: &[u8]) -> (u32, [u8; 8], Vec<u8>) {
            let body = build_rr_data(cip);
            let mut frame = Vec::with_capacity(ENIP_HEADER_LEN + body.len());
            frame.extend_from_slice(&CMD_SEND_RR_DATA.to_le_bytes());
            frame.extend_from_slice(&(body.len() as u16).to_le_bytes());
            frame.extend_from_slice(&self.session.to_le_bytes());
            frame.extend_from_slice(&[0; 4]); // status
            frame.extend_from_slice(&self.context);
            frame.extend_from_slice(&[0; 4]);
            frame.extend_from_slice(&body);
            self.stream.write_all(&frame).expect("发送失败");
            let resp = read_frame(&mut self.stream);
            let cmd = u16::from_le_bytes([resp[0], resp[1]]);
            assert_eq!(cmd, CMD_SEND_RR_DATA);
            // read_frame 已按声明长度读全帧；体 = 头之后的 len 字节。
            (
                u32::from_le_bytes([resp[4], resp[5], resp[6], resp[7]]),
                resp[12..20].try_into().expect("ctx"),
                extract_cip(&resp[ENIP_HEADER_LEN..]).unwrap_or_default(),
            )
        }
    }

    /// 发送一帧并读取应答，返回 session handle（RegisterSession 用）。
    /// 必须同时消费应答体（4 字节），否则残留字节污染后续帧解析。
    fn send_and_read_session(stream: &mut TcpStream, context: &[u8; 8]) -> u32 {
        let body = [0x01, 0x00, 0x00, 0x00];
        let mut frame = Vec::with_capacity(ENIP_HEADER_LEN + body.len());
        frame.extend_from_slice(&CMD_REGISTER_SESSION.to_le_bytes());
        frame.extend_from_slice(&(body.len() as u16).to_le_bytes());
        frame.extend_from_slice(&0u32.to_le_bytes());
        frame.extend_from_slice(&0u32.to_le_bytes());
        frame.extend_from_slice(context);
        frame.extend_from_slice(&[0; 4]);
        frame.extend_from_slice(&body);
        stream.write_all(&frame).expect("发送失败");
        let mut header = [0u8; ENIP_HEADER_LEN];
        stream.read_exact(&mut header).expect("读应答失败");
        let mut reply_body = vec![0u8; usize::from(u16::from_le_bytes([header[2], header[3]]))];
        stream.read_exact(&mut reply_body).expect("读应答体失败");
        u32::from_le_bytes([header[4], header[5], header[6], header[7]])
    }

    /// 读一帧完整封装应答。
    fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0u8; ENIP_HEADER_LEN];
        stream.read_exact(&mut header).expect("读封装头失败");
        let len = usize::from(u16::from_le_bytes([header[2], header[3]]));
        let mut rest = vec![0u8; len];
        stream.read_exact(&mut rest).expect("读体失败");
        let mut frame = header.to_vec();
        frame.extend_from_slice(&rest);
        frame
    }

    /// 构造一条 Read Tag 子请求：`[service][path-size=words][0x91][len][tag…pad][elements u16]`。
    fn read_tag_request(tag: &str) -> Vec<u8> {
        let tag_len = tag.len();
        let seg_len = 2 + tag_len + usize::from(tag_len % 2 == 1); // 0x91+len+chars+pad
        let words = seg_len / 2;
        let mut req = vec![SVC_READ_TAG, words as u8, 0x91, tag_len as u8];
        req.extend_from_slice(tag.as_bytes());
        if tag_len % 2 == 1 {
            req.push(0);
        }
        req.extend_from_slice(&1u16.to_le_bytes()); // elements
        req
    }

    /// 构造一条 Write Tag 子请求。
    fn write_tag_request(tag: &str, type_code: u16, payload: &[u8]) -> Vec<u8> {
        let tag_len = tag.len();
        let seg_len = 2 + tag_len + usize::from(tag_len % 2 == 1);
        let words = seg_len / 2;
        let mut req = vec![SVC_WRITE_TAG, words as u8, 0x91, tag_len as u8];
        req.extend_from_slice(tag.as_bytes());
        if tag_len % 2 == 1 {
            req.push(0);
        }
        req.extend_from_slice(&type_code.to_le_bytes());
        req.extend_from_slice(&1u16.to_le_bytes());
        req.extend_from_slice(payload);
        req
    }

    /// 把多条子请求打包为 Multi-Service 数据区。
    fn pack_multi(sub_requests: &[Vec<u8>]) -> Vec<u8> {
        let count = sub_requests.len() as u16;
        let mut data = vec![0u8; 2 + sub_requests.len() * 2]; // 占位
        let mut running = data.len() as u16;
        let mut offsets = Vec::with_capacity(sub_requests.len());
        for sub in sub_requests {
            offsets.push(running);
            running += sub.len() as u16;
        }
        data[0..2].copy_from_slice(&count.to_le_bytes());
        for (i, off) in offsets.iter().enumerate() {
            let at = 2 + i * 2;
            data[at..at + 2].copy_from_slice(&off.to_le_bytes());
        }
        for sub in sub_requests {
            data.extend_from_slice(sub);
        }
        // Multi CIP 消息：service + path size(2 words) + CM path + data。
        let mut cip = vec![SVC_MULTI, 2, 0x20, 0x06, 0x24, 0x01];
        cip.extend_from_slice(&data);
        cip
    }

    #[test]
    fn register_assigns_session_handle() {
        let server = MockServer::start(MockBehavior::new());
        let client = RawClient::connect(server.addr, 0, 2);
        assert_eq!(server.registered_sessions(), 1, "一次注册必须计数一次");
        let _ = client;
    }

    #[test]
    fn read_write_round_trip_via_multi() {
        let behavior = MockBehavior::new()
            .with_dint("Line1.Speed", 1500)
            .with_bool("Motor.Run", true)
            .with_uint("Counter.Total", 777);
        let server = MockServer::start(behavior);
        let mut client = RawClient::connect(server.addr, 0, 0);

        let multi = pack_multi(&[
            read_tag_request("Line1.Speed"),
            read_tag_request("Motor.Run"),
        ]);
        let (_, _, cip_reply) = client.rr_data(&multi);
        assert_eq!(cip_reply[0], SVC_MULTI | 0x80);
        assert_eq!(cip_reply[2], STATUS_SUCCESS, "Multi 整体成功");
        // 子服务数与偏移表在 reply data 内（跳过 service/保留/status×2）。
        let data_start = 4;
        let count = u16::from_le_bytes([cip_reply[data_start], cip_reply[data_start + 1]]);
        assert_eq!(count, 2);

        // 读到的 DINT 载荷应为 1500 小端（子应答内偏移由 mock 决定，
        // 简化为断言整段含该字节序列）。
        let hex: Vec<u8> = cip_reply.clone();
        assert!(hex.windows(4).any(|w| w == [220, 5, 0, 0]), "1500 LE");

        assert_eq!(server.request_count(), 1, "两条读合并进一条 Multi");
        assert_eq!(server.read_records().len(), 2);
    }

    #[test]
    fn write_updates_table_and_captures() {
        let server = MockServer::start(MockBehavior::new().with_uint("T", 0));
        let mut client = RawClient::connect(server.addr, 0, 0);

        let multi = pack_multi(&[
            write_tag_request("T", TYPE_UINT, &777u16.to_le_bytes()),
            write_tag_request("X", TYPE_BOOL, &[1]),
        ]);
        let (_, _, cip_reply) = client.rr_data(&multi);
        assert_eq!(cip_reply[0], SVC_MULTI | 0x80);

        assert_eq!(server.tag_value("T").map(|t| t.data), Some(vec![9, 3]));
        assert!(server.tag_value("X").is_some(), "未知标签写入即创建");
        assert_eq!(server.write_records().len(), 2);
    }

    #[test]
    fn deny_writes_returns_0x0f_per_item() {
        let behavior = MockBehavior::new()
            .with_uint("Locked", 1)
            .with_uint("Free", 2);
        let server = MockServer::start(behavior);
        server
            .behavior()
            .lock()
            .unwrap()
            .deny_writes_at
            .insert("Locked".to_owned());
        let mut client = RawClient::connect(server.addr, 0, 0);

        let multi = pack_multi(&[
            write_tag_request("Free", TYPE_UINT, &9u16.to_le_bytes()),
            write_tag_request("Locked", TYPE_UINT, &9u16.to_le_bytes()),
        ]);
        let (_, _, cip_reply) = client.rr_data(&multi);

        // Free 成功（status 0），Locked 返回 0x0F——两个子应答都在包内。
        let joined = &cip_reply;
        assert!(
            joined
                .windows(3)
                .any(|w| w == [SVC_WRITE_TAG | 0x80, 0, STATUS_SUCCESS]),
            "Free 应成功"
        );
        assert!(
            joined
                .windows(3)
                .any(|w| w == [SVC_WRITE_TAG | 0x80, 0, STATUS_ACCESS_DENIED]),
            "Locked 应被拒绝"
        );
        // Locked 值不得被改写。
        assert_eq!(server.tag_value("Locked").map(|t| t.data), Some(vec![1, 0]));
    }

    #[test]
    fn wrong_sender_context_injection() {
        let server = MockServer::start(MockBehavior::new());
        server.behavior().lock().unwrap().wrong_sender_context = true;
        let mut client = RawClient::connect(server.addr, 0, 0);
        let (_, ctx, _) = client.rr_data(&pack_multi(&[read_tag_request("A")]));
        assert_ne!(ctx, client.context, "注入后 context 必须错乱");
    }
}
