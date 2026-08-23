//! 同步 Mock S7comm Server（测试共用，非生产代码）。
//!
//! ISO-on-TCP 最小子集：TPKT 分帧 + COTP CR/CC 握手 + DT 数据传输 +
//! S7 Setup Communication 协商与 Read/Write Var 应答，内存映像为稀疏
//! 字节表（缺省读 0）。行为配置与 `crates/modbus-mock` 逐条对齐：
//! 响应延迟、连接断开、概率超时（连接内可复现伪随机）、逐项拒绝注入、
//! 失步注入（pdu_ref/item_count/TPKT 版本）与请求捕获统计。
//!
//! # 协议常量出处
//!
//! 全部常量对照 Wireshark `cotp`/`s7comm` dissector 文档（tpkt.html、
//! cotp.html、s7comm.html 及其子页），golden 单测固化本文件与驱动侧的
//! 一致性：
//!
//! - TPKT：version=3、保留位 0、长度 BE u16（含头 4 字节）；
//! - COTP：CR=0xE0、CC=0xD0（Connection **Confirm**，注意不是 CR）、
//!   DT=0xF0；TSAP 参数码 calling=0xC1 / called=0xC2，远端 TSAP 两字节
//!   `[0x03, (rack<<5)|slot]`；
//! - S7：ROSCTR Job=0x01（请求头 10 字节）、Ack_Data=0x03（响应头 12
//!   字节含 error_class/error_code）；Setup Communication function=0xF0
//!   （参数区 6 字节：F0 00 + 双方 max pdu 各 BE u16）；Read Var=0x04 /
//!   Write Var=0x05；Any 指针 spec=0x12、长度=0x0A、syntax id=0x10，
//!   地址域 3 字节大端（低 3 位为位号，高位为字节号）；transport size
//!   BIT=0x01 / BYTE=0x03 / WORD=0x04 / DWORD=0x06（length 分别按位数/
//!   字节数/字数/双字数计）；item 返回码 0xFF 成功。
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

// ── 协议常量（见模块文档出处） ──

/// TPKT 协议版本。
pub const TPKT_VERSION: u8 = 3;
/// COTP Connection Request。
pub const COTP_CR: u8 = 0xE0;
/// COTP Connection Confirm。
pub const COTP_CC: u8 = 0xD0;
/// COTP Data Transfer。
pub const COTP_DT: u8 = 0xF0;
/// S7 协议魔数（每条 S7 PDU 首字节）。
pub const S7_PROTOCOL_ID: u8 = 0x32;
/// S7 ROSCTR：Job（请求）。
pub const ROSCTR_JOB: u8 = 0x01;
/// S7 ROSCTR：Ack_Data（带数据确认响应）。
pub const ROSCTR_ACK_DATA: u8 = 0x03;
/// S7 function：Setup Communication。
pub const FUNCTION_SETUP: u8 = 0xF0;
/// S7 function：Read Var。
pub const FUNCTION_READ: u8 = 0x04;
/// S7 function：Write Var。
pub const FUNCTION_WRITE: u8 = 0x05;
/// transport size：BIT（Any length 单位 = 位）。
pub const TS_BIT: u8 = 0x01;
/// transport size：BYTE（单位 = 字节）。
pub const TS_BYTE: u8 = 0x03;
/// transport size：WORD（单位 = 字）。
pub const TS_WORD: u8 = 0x04;
/// transport size：DWORD（单位 = 双字）。
pub const TS_DWORD: u8 = 0x06;
/// item 返回码：成功。
pub const RC_SUCCESS: u8 = 0xFF;
/// item 返回码：访问被拒（注入用稳定值；驱动映射为 `access_denied`）。
pub const RC_ACCESS_DENIED: u8 = 0x07;

/// 存储区代码：过程映像输入（只读区）。
pub const AREA_INPUT: u8 = 0x81;
/// 存储区代码：过程映像输出。
pub const AREA_OUTPUT: u8 = 0x82;
/// 存储区代码：Marker（M 区）。
pub const AREA_MARKER: u8 = 0x83;
/// 存储区代码：Data Block。
pub const AREA_DB: u8 = 0x84;

/// 内存映像地址键：`(area, db, 字节偏移)`。
type AddrKey = (u8, u16, u32);

/// 捕获的一条写请求项（合并后单条记录，供合并/区间断言）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecord {
    /// 存储区代码。
    pub area: u8,
    /// DB 号（非 DB 区为 0）。
    pub db: u16,
    /// 起始字节偏移。
    pub start_byte: u32,
    /// 写入字节数（位写恒 1）。
    pub len_bytes: u32,
    /// 写入载荷（按字节展开）。
    pub data: Vec<u8>,
}

/// 捕获的一条读请求项（合并后单条记录）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRecord {
    pub area: u8,
    pub db: u16,
    pub start_byte: u32,
    /// 读取字节数（位读恒 1）。
    pub len_bytes: u32,
}

/// Mock 服务器行为配置。
#[derive(Debug)]
pub struct MockBehavior {
    /// 内存映像：地址 -> 字节（缺省读 0）。
    pub values: HashMap<AddrKey, u8>,
    /// Setup Communication 应答提供的 max pdu（协商取较小者）。
    pub offered_pdu_size: u16,
    /// 每个请求的响应延迟。
    pub response_delay: Option<Duration>,
    /// 是否在响应前立即断开连接（模拟断线）。
    pub drop_connection: bool,
    /// 概率超时注入 `(分子, 分母)`：命中静默不响应且不关连接
    /// （连接内 xorshift-32 固定种子派生自 rack/slot，序列可复现）。
    pub timeout_rate: Option<(u32, u32)>,
    /// 指定起始地址的读/写项注入拒绝返回码（模拟逐项失败）。
    pub access_denied_at: HashMap<AddrKey, ()>,
    /// 响应 pdu_ref +1（测失步检测：驱动必须判 invalid_response 丢会话）。
    pub wrong_pdu_ref: bool,
    /// 响应 item count 与请求不符（测结构校验）。
    pub declare_wrong_item_count: bool,
    /// 响应 TPKT version 置 4（测分帧校验）。
    pub bad_tpkt_version: bool,
    /// 统计：收到的 S7 请求数（Setup 不计）。
    pub request_count: Arc<AtomicU32>,
    /// 收到的写项（合并后单条记录，含被拒绝注入的项）。
    pub captured_writes: Arc<Mutex<Vec<WriteRecord>>>,
    /// 收到的读项。
    pub captured_reads: Arc<Mutex<Vec<ReadRecord>>>,
}

impl MockBehavior {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            offered_pdu_size: 480,
            response_delay: None,
            drop_connection: false,
            timeout_rate: None,
            access_denied_at: HashMap::new(),
            wrong_pdu_ref: false,
            declare_wrong_item_count: false,
            bad_tpkt_version: false,
            request_count: Arc::new(AtomicU32::new(0)),
            captured_writes: Arc::new(Mutex::new(Vec::new())),
            captured_reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 便捷：设置响应延迟。
    pub fn with_response_delay(mut self, delay: Duration) -> Self {
        self.response_delay = Some(delay);
        self
    }

    /// 便捷：填充一段连续 DB 字节。
    pub fn with_db_bytes(mut self, db: u16, start: u32, bytes: &[u8]) -> Self {
        for (i, b) in bytes.iter().enumerate() {
            self.values.insert((AREA_DB, db, start + i as u32), *b);
        }
        self
    }

    /// 便捷：设置一个 M 字（大端两字节写入映像）。
    pub fn with_mw(mut self, offset: u32, value: u16) -> Self {
        self.values
            .insert((AREA_MARKER, 0, offset), (value >> 8) as u8);
        self.values
            .insert((AREA_MARKER, 0, offset + 1), value as u8);
        self
    }

    /// 便捷：设置一个 M 双字（大端四字节写入映像）。
    pub fn with_md(mut self, offset: u32, value: u32) -> Self {
        for i in 0..4u32 {
            let shift = 8 * (3 - i);
            self.values
                .insert((AREA_MARKER, 0, offset + i), (value >> shift) as u8);
        }
        self
    }

    /// 便捷：置一个位（任意存储区）。
    pub fn with_bit(mut self, area: u8, db: u16, byte: u32, bit: u8, on: bool) -> Self {
        let key = (area, db, byte);
        let b = self.values.get(&key).copied().unwrap_or(0);
        let b = if on { b | (1 << bit) } else { b & !(1 << bit) };
        self.values.insert(key, b);
        self
    }
}

impl Default for MockBehavior {
    fn default() -> Self {
        Self::new()
    }
}

/// 连接内 xorshift-32（非零状态）：概率超时注入的确定性序列源，
/// 种子派生自握手的 rack/slot（同一连接跨次运行可复现）。
struct XorShift32(u32);

impl XorShift32 {
    fn new(rack: u8, slot: u8) -> Self {
        let seed = (u32::from(rack) << 5 | u32::from(slot)) ^ 0x9E37_79B9;
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
    last_tsap: Arc<Mutex<Option<(u8, u8)>>>,
    handle: Option<thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl MockServer {
    /// 启动服务器并返回地址。
    pub fn start(behavior: MockBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("绑定测试端口失败");
        let addr = listener.local_addr().expect("读取监听地址失败");
        let behavior = Arc::new(Mutex::new(behavior));
        let last_tsap = Arc::new(Mutex::new(None));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        listener.set_nonblocking(true).expect("设置非阻塞失败");
        let behavior_ref = Arc::clone(&behavior);
        let tsap_ref = Arc::clone(&last_tsap);
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let behavior = Arc::clone(&behavior_ref);
                        let tsap = Arc::clone(&tsap_ref);
                        let stop = Arc::clone(&stop_thread);
                        // 每个连接一个线程（测试规模小，无并发压力）。
                        thread::spawn(move || {
                            handle_connection(stream, behavior, tsap, stop);
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
            last_tsap,
            handle: Some(handle),
            stop,
        }
    }

    /// 共享行为引用（测试中修改配置用）。
    pub fn behavior(&self) -> Arc<Mutex<MockBehavior>> {
        Arc::clone(&self.behavior)
    }

    /// 最近一次握手记录到的远端 TSAP `(rack, slot)`（连接断言用）。
    pub fn last_called_tsap(&self) -> Option<(u8, u8)> {
        *self.last_tsap.lock().expect("tsap 锁")
    }

    /// 收到的 S7 请求总数（Setup 不计）。
    pub fn request_count(&self) -> u32 {
        self.behavior
            .lock()
            .expect("行为锁")
            .request_count
            .load(Ordering::Relaxed)
    }

    /// 收到的写项快照（按到达顺序）。
    pub fn write_records(&self) -> Vec<WriteRecord> {
        self.behavior
            .lock()
            .expect("行为锁")
            .captured_writes
            .lock()
            .expect("写捕获锁")
            .clone()
    }

    /// 收到的读项快照（按到达顺序）。
    pub fn read_records(&self) -> Vec<ReadRecord> {
        self.behavior
            .lock()
            .expect("行为锁")
            .captured_reads
            .lock()
            .expect("读捕获锁")
            .clone()
    }

    /// 读取内存映像当前字节（写生效断言用）。
    pub fn byte(&self, area: u8, db: u16, offset: u32) -> Option<u8> {
        self.behavior
            .lock()
            .expect("行为锁")
            .values
            .get(&(area, db, offset))
            .copied()
    }

    /// 读取内存映像当前位。
    pub fn bit(&self, area: u8, db: u16, byte: u32, bit: u8) -> Option<bool> {
        self.byte(area, db, byte).map(|b| b & (1 << bit) != 0)
    }

    /// 读取内存映像当前字（大端组装）。
    pub fn word(&self, area: u8, db: u16, offset: u32) -> Option<u16> {
        let hi = self.byte(area, db, offset)?;
        let lo = self.byte(area, db, offset + 1)?;
        Some((u16::from(hi) << 8) | u16::from(lo))
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

// ── 连接处理 ──

fn handle_connection(
    mut stream: TcpStream,
    behavior: Arc<Mutex<MockBehavior>>,
    last_tsap: Arc<Mutex<Option<(u8, u8)>>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    // 监听 socket 非阻塞时，accept 出的连接继承非阻塞标志，必须恢复
    // 阻塞模式才能用 read_exact 等待完整帧（与 modbus-mock 同坑同修）。
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);
    let mut rng: Option<XorShift32> = None;
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // 读一帧 TPKT + 内嵌 COTP/S7 载荷。
        let mut header = [0u8; 4];
        if stream.read_exact(&mut header).is_err() {
            return;
        }
        let total = u16::from_be_bytes([header[2], header[3]]) as usize;
        if !(4..=65_579).contains(&total) {
            return; // 非法长度：直接丢弃连接（严格不宽容）
        }
        let mut rest = vec![0u8; total - 4];
        if stream.read_exact(&mut rest).is_err() {
            return;
        }
        // COTP 头：LI(1) + 类型(1)；DT 的 LI=2。
        if rest.len() < 2 {
            return;
        }
        let li = rest[0] as usize;
        let cotp_type = rest[1];
        match cotp_type {
            COTP_CR => {
                // 解析 called TSAP（参数码 0xC2）。ISO 8073：LI 不含自身，
                // 计数从类型字节起——CR 固定头 = type(1)+dstref(2)+srcref(2)
                // +class(1) = 7 字节，变参区为 rest[7..1+li]。
                let mut called: Option<(u8, u8)> = None;
                if rest.len() >= 8 {
                    let end = (li as usize + 1).min(rest.len());
                    let var = &rest[7..end];
                    let mut i = 0usize;
                    while i + 2 <= var.len() {
                        let code = var[i];
                        let plen = var[i + 1] as usize;
                        if code == 0xC2 && plen == 2 && i + 4 <= var.len() && var[i + 2] == 0x03 {
                            called = Some((var[i + 3] >> 5, var[i + 3] & 0x1F));
                        }
                        i += 2 + plen;
                    }
                }
                *last_tsap.lock().expect("tsap 锁") = called;
                let (_, slot) = called.unwrap_or((0, 0));
                rng = Some(XorShift32::new(called.map(|(r, _)| r).unwrap_or(0), slot));
                // CC：LI=0x0E（type1+引用4+class1+C1/C2 参数各 4 字节），
                // 回显客户端引用与两个 TSAP（内容不影响驱动判定）。
                let cc: [u8; 15] = [
                    0x0E, COTP_CC, 0x00, 0x01, 0x00, 0x0F, 0x00, 0xC1, 0x02, 0x01, 0x00, 0xC2,
                    0x02, 0x03, slot,
                ];
                let frame_len = (cc.len() + 4) as u16;
                let mut out = vec![TPKT_VERSION, 0, (frame_len >> 8) as u8, frame_len as u8];
                out.extend_from_slice(&cc);
                if stream.write_all(&out).is_err() {
                    return;
                }
            }
            COTP_DT => {
                let pdu = &rest[(li + 1)..];
                if pdu.len() < 10 {
                    return;
                }
                // 行为开关：延迟 / 断连 / 概率超时（命中静默不关连接）。
                {
                    let guard = behavior.lock().expect("行为锁");
                    if let Some(d) = guard.response_delay {
                        drop(guard);
                        thread::sleep(d);
                    } else {
                        drop(guard);
                    }
                    let guard = behavior.lock().expect("行为锁");
                    if guard.drop_connection {
                        return;
                    }
                    if let Some((num, den)) = guard.timeout_rate {
                        let roll = rng.get_or_insert_with(|| XorShift32::new(0, 0)).next();
                        if num > 0 && den > 0 && roll % den < num {
                            continue;
                        }
                    }
                }
                let response = match handle_s7(&behavior, pdu) {
                    Some(resp) => resp,
                    None => return, // 非 Job 帧：严格不宽容
                };
                // 失步注入：响应 pdu_ref +1（Ack_Data 头内 ref 位于
                // 裸 S7 PDU 的 [4..6]）。驱动必须判 invalid_response。
                let response = {
                    let guard = behavior.lock().expect("行为锁");
                    if guard.wrong_pdu_ref && response.len() > 6 {
                        let bumped = u16::from_be_bytes([response[4], response[5]]).wrapping_add(1);
                        let mut r = response;
                        r[4] = (bumped >> 8) as u8;
                        r[5] = bumped as u8;
                        r
                    } else {
                        response
                    }
                };
                // TPKT + COTP DT 包裹响应。
                let frame_len = (response.len() + 4 + 3) as u16;
                let bad_version = behavior.lock().expect("行为锁").bad_tpkt_version;
                let mut out = vec![
                    if bad_version { 4 } else { TPKT_VERSION },
                    0,
                    (frame_len >> 8) as u8,
                    frame_len as u8,
                    0x02,
                    COTP_DT,
                    0x80,
                ];
                out.extend_from_slice(&response);
                if stream.write_all(&out).is_err() {
                    return;
                }
            }
            _ => return, // 其余 COTP 类型：严格不宽容
        }
    }
}

/// 处理一条 S7 Job PDU，返回 Ack_Data PDU 内容（无 TPKT/COTP 包裹）；
/// 非 Job 帧返回 `None`。
///
/// 帧结构（Wireshark s7comm dissector）：`[0x32][rosctr][red-id 2]
/// [pdu-ref 2][参数长 2][数据长 2]`——Job 头共 10 字节；Ack_Data 在此
/// 之上追加 error_class/error_code 共 12 字节。
fn handle_s7(behavior: &Arc<Mutex<MockBehavior>>, pdu: &[u8]) -> Option<Vec<u8>> {
    if pdu.len() < 10 || pdu[0] != S7_PROTOCOL_ID {
        return None;
    }
    if pdu[1] != ROSCTR_JOB {
        return None;
    }
    let pdu_ref = u16::from_be_bytes([pdu[4], pdu[5]]);
    let param_len = u16::from_be_bytes([pdu[6], pdu[7]]) as usize;
    if pdu.len() < 10 + param_len {
        return None;
    }
    let param = &pdu[10..10 + param_len];
    if param.is_empty() {
        return None;
    }
    let mut guard = behavior.lock().expect("行为锁");
    match param[0] {
        FUNCTION_SETUP => {
            // 应答提供自身 max pdu（协商由客户端取较小者完成）。
            let offered = guard.offered_pdu_size.to_be_bytes();
            let param_area = [FUNCTION_SETUP, 0x00, offered[0], offered[1]];
            Some(build_ack_data(pdu_ref, &param_area, &[]))
        }
        FUNCTION_READ => {
            guard.request_count.fetch_add(1, Ordering::Relaxed);
            let count = param[1] as usize;
            let mut data = Vec::new();
            // Any 指针每项 12 字节（spec+len+10 字节体）。
            for i in 0..count {
                let base = 2 + i * 12;
                if base + 12 > param.len() {
                    return None;
                }
                let item = AnyItem::parse(&param[base..base + 12])?;
                let denied =
                    guard
                        .access_denied_at
                        .contains_key(&(item.area, item.db, item.start_byte));
                let rc = if denied { RC_ACCESS_DENIED } else { RC_SUCCESS };
                guard
                    .captured_reads
                    .lock()
                    .expect("读捕获锁")
                    .push(ReadRecord {
                        area: item.area,
                        db: item.db,
                        start_byte: item.start_byte,
                        len_bytes: item.width_bytes(),
                    });
                let payload = read_image(&guard.values, &item);
                data.push(rc);
                data.push(item.transport_size);
                data.extend_from_slice(&(item.length as u16).to_be_bytes());
                data.extend_from_slice(&payload);
                if payload.len() % 2 == 1 {
                    data.push(0x00); // 偶对齐填充
                }
            }
            let mut param_area = vec![FUNCTION_READ, param[1], 0x00, 0x00];
            if guard.declare_wrong_item_count {
                param_area[1] = param_area[1].wrapping_add(1);
            }
            Some(build_ack_data(pdu_ref, &param_area, &data))
        }
        FUNCTION_WRITE => {
            guard.request_count.fetch_add(1, Ordering::Relaxed);
            let count = param[1] as usize;
            // 数据区起点：头(10) + 参数区之后。Any 指针每项 12 字节。
            let data_off = 10 + param_len;
            let data_len = u16::from_be_bytes([pdu[8], pdu[9]]) as usize;
            if pdu.len() < data_off + data_len {
                return None;
            }
            let data_in = &pdu[data_off..data_off + data_len];
            let mut cursor = 0usize;
            let mut results = Vec::with_capacity(count);
            for i in 0..count {
                let base = 2 + i * 12;
                if base + 12 > param.len() {
                    return None;
                }
                let item = AnyItem::parse(&param[base..base + 12])?;
                // 数据项：[return 占位][ts][length u16][载荷(+pad)]。
                if cursor + 4 > data_in.len() {
                    return None;
                }
                let ts = data_in[cursor + 1];
                let declared = u16::from_be_bytes([data_in[cursor + 2], data_in[cursor + 3]]);
                cursor += 4;
                let payload_bytes = payload_size(ts, declared as u32, &item);
                if cursor + payload_bytes > data_in.len() {
                    return None;
                }
                let payload = &data_in[cursor..cursor + payload_bytes];
                cursor += payload_bytes;
                if payload_bytes % 2 == 1 && cursor < data_in.len() {
                    cursor += 1; // 偶对齐填充
                }
                let denied =
                    guard
                        .access_denied_at
                        .contains_key(&(item.area, item.db, item.start_byte));
                let rc = if denied { RC_ACCESS_DENIED } else { RC_SUCCESS };
                guard
                    .captured_writes
                    .lock()
                    .expect("写捕获锁")
                    .push(WriteRecord {
                        area: item.area,
                        db: item.db,
                        start_byte: item.start_byte,
                        len_bytes: payload_bytes as u32,
                        data: payload.to_vec(),
                    });
                if rc == RC_SUCCESS {
                    apply_write(&mut guard.values, &item, payload);
                }
                results.push(rc);
            }
            let param_area = vec![FUNCTION_WRITE, param[1]];
            Some(build_ack_data(pdu_ref, &param_area, &results))
        }
        _ => None,
    }
}

/// 组装 Ack_Data：`[0x32][rosctr][red-id 2][pdu-ref 2][参数长 2][数据长 2]
/// [error_class][error_code]` 头 12 字节 + 参数区 + 数据区。
fn build_ack_data(pdu_ref: u16, param: &[u8], data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + param.len() + data.len());
    out.push(S7_PROTOCOL_ID);
    out.push(ROSCTR_ACK_DATA);
    out.extend_from_slice(&[0x00, 0x00]); // redundant identification
    out.extend_from_slice(&pdu_ref.to_be_bytes());
    out.extend_from_slice(&(param.len() as u16).to_be_bytes());
    out.extend_from_slice(&(data.len() as u16).to_be_bytes());
    out.push(0x00); // error class = 0
    out.push(0x00); // error code = 0
    out.extend_from_slice(param);
    out.extend_from_slice(data);
    out
}

/// 解析后的 S7 Any 指针项。
struct AnyItem {
    transport_size: u8,
    /// 元素数（BIT=位、BYTE=字节、WORD=字、DWORD=双字）。
    length: u32,
    area: u8,
    db: u16,
    /// 字节偏移。
    start_byte: u32,
    /// 位号（仅 BIT 有效）。
    bit: u8,
}

impl AnyItem {
    /// 解析 12 字节 Any 指针（spec 0x12 + 后续长度 0x0A + 10 字节体）。
    fn parse(b: &[u8]) -> Option<Self> {
        if b.len() < 12 || b[0] != 0x12 || b[1] != 0x0A || b[2] != 0x10 {
            return None;
        }
        let transport_size = b[3];
        let length = u16::from_be_bytes([b[4], b[5]]) as u32;
        let db = u16::from_be_bytes([b[6], b[7]]);
        let area = b[8];
        let addr = (u32::from(b[9]) << 16) | (u32::from(b[10]) << 8) | u32::from(b[11]);
        Some(Self {
            transport_size,
            length,
            area,
            db,
            start_byte: addr >> 3,
            bit: (addr & 0x07) as u8,
        })
    }

    /// 该项覆盖的字节数（位读/写恒 1 字节承载）。
    fn width_bytes(&self) -> u32 {
        match self.transport_size {
            TS_BIT => 1,
            TS_BYTE => self.length.max(1),
            TS_WORD => self.length.max(1) * 2,
            TS_DWORD => self.length.max(1) * 4,
            _ => 0,
        }
    }
}

/// 数据项载荷实际字节数（length 单位随 transport size 变化）。
fn payload_size(ts: u8, length: u32, _item: &AnyItem) -> usize {
    match ts {
        TS_BIT => 1,
        TS_BYTE => length as usize,
        TS_WORD => length as usize * 2,
        TS_DWORD => length as usize * 4,
        _ => length as usize,
    }
}

/// 从映像读取该项载荷（缺省字节补 0）。
fn read_image(values: &HashMap<AddrKey, u8>, item: &AnyItem) -> Vec<u8> {
    match item.transport_size {
        TS_BIT => {
            let b = values
                .get(&(item.area, item.db, item.start_byte))
                .copied()
                .unwrap_or(0);
            vec![if b & (1 << item.bit) != 0 { 1 } else { 0 }]
        }
        TS_BYTE => (0..item.length)
            .map(|i| {
                values
                    .get(&(item.area, item.db, item.start_byte + i))
                    .copied()
                    .unwrap_or(0)
            })
            .collect(),
        TS_WORD => (0..item.length)
            .flat_map(|w| {
                let off = item.start_byte + w * 2;
                [
                    values.get(&(item.area, item.db, off)).copied().unwrap_or(0),
                    values
                        .get(&(item.area, item.db, off + 1))
                        .copied()
                        .unwrap_or(0),
                ]
            })
            .collect(),
        TS_DWORD => (0..item.length)
            .flat_map(|d| {
                let off = item.start_byte + d * 4;
                (0..4).map(move |i| {
                    values
                        .get(&(item.area, item.db, off + i))
                        .copied()
                        .unwrap_or(0)
                })
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// 把写载荷应用到映像。
fn apply_write(values: &mut HashMap<AddrKey, u8>, item: &AnyItem, payload: &[u8]) {
    match item.transport_size {
        TS_BIT => {
            let key = (item.area, item.db, item.start_byte);
            let b = values.get(&key).copied().unwrap_or(0);
            let bit_val = payload.first().copied().unwrap_or(0) & 1;
            let b = if bit_val != 0 {
                b | (1 << item.bit)
            } else {
                b & !(1 << item.bit)
            };
            values.insert(key, b);
        }
        TS_BYTE => {
            for (i, b) in payload.iter().enumerate() {
                values.insert((item.area, item.db, item.start_byte + i as u32), *b);
            }
        }
        TS_WORD => {
            for (w, chunk) in payload.chunks(2).enumerate() {
                for (i, b) in chunk.iter().enumerate() {
                    values.insert(
                        (
                            item.area,
                            item.db,
                            item.start_byte + w as u32 * 2 + i as u32,
                        ),
                        *b,
                    );
                }
            }
        }
        TS_DWORD => {
            for (d, chunk) in payload.chunks(4).enumerate() {
                for (i, b) in chunk.iter().enumerate() {
                    values.insert(
                        (
                            item.area,
                            item.db,
                            item.start_byte + d as u32 * 4 + i as u32,
                        ),
                        *b,
                    );
                }
            }
        }
        _ => {}
    }
}

// ── 便捷函数 ──

/// 配置 host/port/rack/slot 的连接 JSON（S7 Driver `mode=tcp`）。
pub fn tcp_config_at(host: &str, port: u16, rack: u8, slot: u8, timeout_ms: u64) -> String {
    serde_json::json!({
        "mode": "tcp",
        "host": host,
        "port": port,
        "rack": rack,
        "slot": slot,
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
        0,
        0,
        timeout_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用原始客户端：手工组帧与 mock 往返（固化协议常量契约）。
    struct RawClient {
        stream: TcpStream,
    }

    impl RawClient {
        fn connect(addr: std::net::SocketAddr, rack: u8, slot: u8) -> Self {
            let mut stream = TcpStream::connect(addr).expect("连接 mock 失败");
            // CR：LI=0x0E（type1+引用4+class1+C1/C2 参数各 4 字节）。
            // calling TSAP = 0x0100；called TSAP = [0x03, (rack<<5)|slot]。
            let cotp = [
                0x0E,
                COTP_CR,
                0x00,
                0x00,
                0x00,
                0x0F,
                0x00,
                0xC1,
                0x02,
                0x01,
                0x00,
                0xC2,
                0x02,
                0x03,
                (rack << 5) | slot,
            ];
            send_tpkt(&mut stream, &cotp);
            let cc = read_tpkt(&mut stream);
            assert_eq!(cc[1], COTP_CC, "应答应为 CC");
            Self { stream }
        }

        fn s7_job(&mut self, pdu_ref: u16, param: &[u8]) -> Vec<u8> {
            // S7 Job 头 10 字节：0x32 + ROSCTR + red-id(2) + pdu-ref(2)
            // + 参数长(2) + 数据长(2)；Setup 无数据区（数据长=0）。
            let dt = [
                0x02,
                COTP_DT,
                0x80,
                S7_PROTOCOL_ID,
                ROSCTR_JOB,
                0x00,
                0x00,
                (pdu_ref >> 8) as u8,
                pdu_ref as u8,
                (param.len() >> 8) as u8,
                param.len() as u8,
                0x00,
                0x00,
            ];
            let mut frame = dt.to_vec();
            frame.extend_from_slice(param);
            send_tpkt(&mut self.stream, &frame);
            read_tpkt(&mut self.stream)
        }
    }

    fn send_tpkt(stream: &mut TcpStream, payload: &[u8]) {
        let len = (payload.len() + 4) as u16;
        let mut frame = vec![TPKT_VERSION, 0, (len >> 8) as u8, len as u8];
        frame.extend_from_slice(payload);
        stream.write_all(&frame).expect("发送失败");
    }

    /// 读一帧并返回载荷：DT 帧剥到裸 S7 PDU，CC 帧原样返回（握手断言
    /// 需检查 COTP 类型）。
    fn read_tpkt(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).expect("读 TPKT 失败");
        assert_eq!(header[0], TPKT_VERSION, "TPKT 版本必须为 3");
        let len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let mut rest = vec![0u8; len - 4];
        stream.read_exact(&mut rest).expect("读 TPKT 体失败");
        if rest[1] == COTP_DT {
            // 剥离 COTP DT 头（LI+type+EOT 共 3 字节）。
            rest.split_off(3)
        } else {
            rest
        }
    }

    /// 构造一条 DB 读 Any 指针参数区（含 function/count 头）。
    fn read_param(items: &[[u8; 12]]) -> Vec<u8> {
        let mut p = vec![FUNCTION_READ, items.len() as u8];
        for it in items {
            p.extend_from_slice(it);
        }
        p
    }

    fn any_item(ts: u8, length: u16, db: u16, area: u8, byte: u32, bit: u8) -> [u8; 12] {
        let addr = byte << 3 | u32::from(bit);
        [
            0x12,
            0x0A,
            0x10,
            ts,
            (length >> 8) as u8,
            length as u8,
            (db >> 8) as u8,
            db as u8,
            area,
            (addr >> 16) as u8,
            (addr >> 8) as u8,
            addr as u8,
        ]
    }

    #[test]
    fn handshake_records_tsap_and_negotiates_pdu() {
        let server = MockServer::start(MockBehavior::new());
        let mut c = RawClient::connect(server.addr, 0, 2);
        assert_eq!(server.last_called_tsap(), Some((0, 2)), "必须记录远端 TSAP");

        // Setup：提议 500，mock 提供 480 → 应答字段为 480（协商由客户端取小）。
        let resp = c.s7_job(1, &[FUNCTION_SETUP, 0x00, 0x01, 0xF4, 0x01, 0xF4]);
        assert_eq!(resp[0], S7_PROTOCOL_ID);
        assert_eq!(resp[1], ROSCTR_ACK_DATA);
        assert_eq!(u16::from_be_bytes([resp[4], resp[5]]), 1, "pdu_ref 回显");
        // Ack_Data 头 12 字节（含 error_class/error_code），参数区从 12 起。
        let param_len = u16::from_be_bytes([resp[6], resp[7]]) as usize;
        assert_eq!(&resp[12..12 + param_len][..2], &[FUNCTION_SETUP, 0x00]);
        let neg = u16::from_be_bytes([resp[14], resp[15]]);
        assert_eq!(neg, 480, "应答必须携带自身 offered pdu");
        assert_eq!(server.request_count(), 0, "Setup 不计入请求数");
    }

    #[test]
    fn read_var_returns_image_and_pads_to_even() {
        let behavior = MockBehavior::new()
            .with_db_bytes(10, 20, &[0x12, 0x34, 0x56])
            .with_bit(AREA_DB, 10, 30, 3, true);
        let server = MockServer::start(behavior);
        let mut c = RawClient::connect(server.addr, 0, 0);

        // 读 DBW20（1 字）+ 位 DBX30.3（独立项）：字载荷 2B 无 pad、
        // 位载荷 1B 补 pad 至偶数。
        let resp = c.s7_job(
            7,
            &read_param(&[
                any_item(TS_WORD, 1, 10, AREA_DB, 20, 0),
                any_item(TS_BIT, 1, 10, AREA_DB, 30, 3),
            ]),
        );
        assert_eq!(server.request_count(), 1);
        assert_eq!(server.read_records().len(), 2, "两项各自捕获");
        assert_eq!(
            server.read_records()[0],
            ReadRecord {
                area: AREA_DB,
                db: 10,
                start_byte: 20,
                len_bytes: 2
            }
        );
        let data_len = u16::from_be_bytes([resp[8], resp[9]]) as usize;
        let data_start = 12 + u16::from_be_bytes([resp[6], resp[7]]) as usize;
        let data = &resp[data_start..data_start + data_len];
        // 项 1（DBW20）：FF 04 0001 1234（头 4B + 载荷 2B，无 pad）。
        assert_eq!(&data[0..6], &[RC_SUCCESS, TS_WORD, 0x00, 0x01, 0x12, 0x34]);
        // 项 2（位）：FF 01 0001 + 载荷 01 + pad 00（奇数载荷补偶对齐）。
        assert_eq!(&data[6..10], &[RC_SUCCESS, TS_BIT, 0x00, 0x01]);
        assert_eq!(data[10], 0x01);
        assert_eq!(data[11], 0x00, "奇数载荷补偶对齐填充");
        assert_eq!(data_len, 12);
    }

    #[test]
    fn write_var_applies_image_and_echoes_refs() {
        let server = MockServer::start(MockBehavior::new());
        let mut c = RawClient::connect(server.addr, 0, 0);

        // 写 MD4 = 0x11223344（DWORD 1 个）。
        let mut p = vec![FUNCTION_WRITE, 1u8];
        p.extend_from_slice(&any_item(TS_DWORD, 1, 5, AREA_DB, 4, 0));
        let mut data = vec![0x00, TS_DWORD, 0x00, 0x01, 0x11, 0x22, 0x33, 0x44];
        data.push(0x00); // 4B 载荷本身偶数——此处不加 pad；改用奇数场景单独验证
        data.pop();
        let dt = [
            0x02,
            COTP_DT,
            0x80,
            S7_PROTOCOL_ID,
            ROSCTR_JOB,
            0x00,
            0x00,
            0x00,
            0x64,
            ((p.len()) >> 8) as u8,
            p.len() as u8,
            (data.len() >> 8) as u8,
            data.len() as u8,
        ];
        let mut frame = dt.to_vec();
        frame.extend_from_slice(&p);
        frame.extend_from_slice(&data);
        send_tpkt(&mut c.stream, &frame);
        let resp = read_tpkt(&mut c.stream);

        assert_eq!(u16::from_be_bytes([resp[4], resp[5]]), 0x64, "pdu_ref 回显");
        let param_len = u16::from_be_bytes([resp[6], resp[7]]) as usize;
        let data_off = 12 + param_len;
        assert_eq!(resp[data_off], RC_SUCCESS, "写结果逐项返回码");
        let recs = server.write_records();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].start_byte, 4);
        assert_eq!(recs[0].data, vec![0x11, 0x22, 0x33, 0x44]);
        assert_eq!(server.word(AREA_DB, 5, 4), Some(0x1122));
        assert_eq!(server.word(AREA_DB, 5, 6), Some(0x3344));
    }

    #[test]
    fn access_denied_injection_marks_single_item() {
        let behavior = MockBehavior::new().with_db_bytes(1, 0, &[1, 2, 3, 4]);
        let server = MockServer::start(behavior);
        server
            .behavior()
            .lock()
            .unwrap()
            .access_denied_at
            .insert((AREA_DB, 1, 2), ());
        let mut c = RawClient::connect(server.addr, 0, 0);

        let resp = c.s7_job(
            9,
            &read_param(&[
                any_item(TS_BYTE, 2, 1, AREA_DB, 0, 0),
                any_item(TS_BYTE, 1, 1, AREA_DB, 2, 0),
            ]),
        );
        let data_len = u16::from_be_bytes([resp[8], resp[9]]) as usize;
        let data_off = 12 + u16::from_be_bytes([resp[6], resp[7]]) as usize;
        let data = &resp[data_off..data_off + data_len];
        // 项 1（BYTE×2）：FF 03 0002 0102（6 字节）；项 2 从偏移 6 起。
        assert_eq!(&data[0..6], &[RC_SUCCESS, TS_BYTE, 0x00, 0x02, 0x01, 0x02]);
        assert_eq!(data[6], RC_ACCESS_DENIED, "注入项拒绝");
        assert_eq!(data[7], TS_BYTE);
    }

    #[test]
    fn wrong_pdu_ref_and_item_count_knobs_apply() {
        let mut behavior = MockBehavior::new();
        behavior.values.insert((AREA_DB, 1, 0), 9);
        behavior.wrong_pdu_ref = true;
        behavior.declare_wrong_item_count = true;
        let server = MockServer::start(behavior);
        let mut c = RawClient::connect(server.addr, 0, 0);

        let resp = c.s7_job(
            0x1234,
            &read_param(&[any_item(TS_BYTE, 1, 1, AREA_DB, 0, 0)]),
        );
        assert_eq!(
            u16::from_be_bytes([resp[4], resp[5]]),
            0x1235,
            "wrong_pdu_ref 必须使 ref 错位"
        );
        // 参数区从 12 起：count 是参数区第 2 字节。
        assert_eq!(resp[13], 2, "declare_wrong_item_count 使计数错位");
    }
}
