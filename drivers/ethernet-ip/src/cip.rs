//! CIP 层编解码：Message Router 服务与 Multi-Service 打包。
//!
//! # 帧结构（出处：Wireshark `cip` dissector 文档 + ODVA CIP Vol.1 App C）
//!
//! - Message Router 请求：`[service u8][路径长 u8（以字计）][EPATH]
//!   [服务数据…]`；应答首字节 = service|0x80；
//! - 符号段（ANSI extended symbol，段类型 0x91）：`[0x91][ASCII 长度
//!   u8][字节…][奇数补 0 pad]`——整条标签路径（含 `[i]`/`.field`）入
//!   单段（与 libplctag/pycomm3 主流实现一致，真机兼容面最大）；
//!   0x28/0x29 数字下标段仅留常量备查不使用；
//! - Read Tag (0x4C)：数据 `[元素数 u16]`；应答 `[service|0x80]
//!   [status][type u16][载荷]` 或 status≠0 时无类型/载荷；
//! - Write Tag (0x4D)：数据 `[type u16][元素数 u16][载荷]`；应答
//!   `[service|0x80][status]`；
//! - Multi-Service (0x0A)：路径 = Connection Manager `20 06 24 01`；
//!   数据 `[子服务数 u16][偏移表 u16×N][子请求拼接]`（偏移相对子服务数
//!   域起点）；应答同构，偏移指向各子应答；
//! - CIP 元素类型码（Phase 0 核对）：C1 BOOL / C2 SINT / C3 INT /
//!   C4 DINT / C5 LINT / C6 USINT / C7 UINT / C8 UDINT / C9 ULINT /
//!   CA REAL(f32) / CB LREAL(f64)，多字节载荷小端。
//!
//! # 与 S7 的预算差异
//!
//! EN/IP 无 Setup 式 PDU 协商——Multi 分块上限是**静态配置值**
//! （`max_services_per_multi`/`max_bytes_per_multi`），规划不依赖连接。

use crate::error::EtherIpError;

/// CIP 服务：Read Tag。
pub const SVC_READ_TAG: u8 = 0x4C;
/// CIP 服务：Write Tag。
pub const SVC_WRITE_TAG: u8 = 0x4D;
/// CIP 服务：Multi-Service。
pub const SVC_MULTI: u8 = 0x0A;

/// EPATH 段类型：ANSI extended symbol。
pub const SEG_ANSI_SYMBOL: u8 = 0x91;

/// Multi-Service 的 Connection Manager 类路径（class 0x06, instance 1）。
pub const CM_PATH: [u8; 4] = [0x20, 0x06, 0x24, 0x01];

/// 子服务返回码：成功。
pub const STATUS_SUCCESS: u8 = 0x00;
/// 子服务返回码：连接相关失败（资源不可用等）。
pub const STATUS_CONN_FAILED: u8 = 0x01;
/// 子服务返回码：privilege violation（写保护等）。
pub const STATUS_ACCESS_DENIED: u8 = 0x0F;
/// 子服务返回码：路径目标未知（标签不存在）。
pub const STATUS_PATH_NOT_FOUND: u8 = 0x14;

// CIP 元素类型码（见模块文档出处表）。
/// 类型码：BOOL。
pub const TYPE_BOOL: u16 = 0xC1;
/// 类型码：SINT（i8）。
pub const TYPE_SINT: u16 = 0xC2;
/// 类型码：INT（i16）。
pub const TYPE_INT: u16 = 0xC3;
/// 类型码：DINT（i32）。
pub const TYPE_DINT: u16 = 0xC4;
/// 类型码：LINT（i64）。
pub const TYPE_LINT: u16 = 0xC5;
/// 类型码：USINT（u8）。
pub const TYPE_USINT: u16 = 0xC6;
/// 类型码：UINT（u16）。
pub const TYPE_UINT: u16 = 0xC7;
/// 类型码：UDINT（u32）。
pub const TYPE_UDINT: u16 = 0xC8;
/// 类型码：ULINT（u64）。
pub const TYPE_ULINT: u16 = 0xC9;
/// 类型码：REAL（f32）。
pub const TYPE_REAL: u16 = 0xCA;
/// 类型码：LREAL（f64）。
pub const TYPE_LREAL: u16 = 0xCB;

/// CIP 标量类型的字节宽度（数组元素访问元素计数恒 1）。
#[must_use]
pub fn type_width(type_code: u16) -> Option<usize> {
    Some(match type_code {
        TYPE_BOOL | TYPE_SINT | TYPE_USINT => 1,
        TYPE_INT | TYPE_UINT => 2,
        TYPE_DINT | TYPE_REAL => 4,
        TYPE_LINT | TYPE_ULINT | TYPE_LREAL => 8,
        _ => return None,
    })
}

/// 构造 ANSI 符号段（整条路径入单段，奇数补 pad）。
#[must_use]
pub fn build_symbol_segment(path: &str) -> Vec<u8> {
    let bytes = path.as_bytes();
    let mut seg = Vec::with_capacity(2 + bytes.len() + usize::from(bytes.len() % 2 == 1));
    seg.push(SEG_ANSI_SYMBOL);
    seg.push(bytes.len() as u8); // 调用方已校验总长 ≤ 240
    seg.extend_from_slice(bytes);
    if bytes.len() % 2 == 1 {
        seg.push(0);
    }
    seg
}

/// 构造 Read Tag 请求（单元素）。
#[must_use]
pub fn build_read_tag(path: &str) -> Vec<u8> {
    let seg = build_symbol_segment(path);
    let words = (seg.len() / 2) as u8;
    let mut out = Vec::with_capacity(2 + seg.len() + 2);
    out.push(SVC_READ_TAG);
    out.push(words);
    out.extend_from_slice(&seg);
    out.extend_from_slice(&1u16.to_le_bytes()); // elements = 1
    out
}

/// 构造 Write Tag 请求（单元素，类型码精确匹配——Logix 写要求）。
#[must_use]
pub fn build_write_tag(path: &str, type_code: u16, payload: &[u8]) -> Vec<u8> {
    let seg = build_symbol_segment(path);
    let words = (seg.len() / 2) as u8;
    let mut out = Vec::with_capacity(2 + seg.len() + 4 + payload.len());
    out.push(SVC_WRITE_TAG);
    out.push(words);
    out.extend_from_slice(&seg);
    out.extend_from_slice(&type_code.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes()); // elements = 1
    out.extend_from_slice(payload);
    out
}

/// 解析后的 Read Tag 应答。
#[derive(Debug)]
pub struct ReadReply<'a> {
    /// general status（非 [`STATUS_SUCCESS`] 时 type/payload 无意义）。
    pub status: u8,
    /// 设备声明的 CIP 类型码（status 成功时存在）。
    pub type_code: Option<u16>,
    /// 载荷（小端；status 成功时存在）。
    pub payload: Option<&'a [u8]>,
}

/// 解析 Read Tag 应答体（不含 service/status 头两字节之外的部分——
/// 输入从 status 字节起：`[status][reserved][type u16][payload]`）。
///
/// # Errors
///
/// status 成功但结构截断时返回 `invalid_response`。
pub fn parse_read_reply(body: &[u8]) -> Result<ReadReply<'_>, EtherIpError> {
    let Some(&status) = body.first() else {
        return Err(EtherIpError::invalid_response(
            "Read 应答缺 status".to_owned(),
        ));
    };
    if status != STATUS_SUCCESS {
        return Ok(ReadReply {
            status,
            type_code: None,
            payload: None,
        });
    }
    if body.len() < 4 {
        return Err(EtherIpError::invalid_response(format!(
            "Read 应答截断：{}",
            body.len()
        )));
    }
    let type_code = u16::from_le_bytes([body[2], body[3]]);
    Ok(ReadReply {
        status,
        type_code: Some(type_code),
        payload: Some(&body[4..]),
    })
}

/// 构造 Multi-Service 请求：子请求打包进偏移表结构。
#[must_use]
pub fn build_multi(sub_requests: &[Vec<u8>]) -> Vec<u8> {
    let count = sub_requests.len();
    // 数据区 = count(2) + 偏移表(count×2) + 各子请求。
    let mut data = vec![0u8; 2 + count * 2];
    data[0..2].copy_from_slice(&(count as u16).to_le_bytes());
    let mut running = (2 + count * 2) as u16;
    for (i, sub) in sub_requests.iter().enumerate() {
        let at = 2 + i * 2;
        data[at..at + 2].copy_from_slice(&running.to_le_bytes());
        running += sub.len() as u16;
    }
    for sub in sub_requests {
        data.extend_from_slice(sub);
    }
    // CIP 消息头：service + 路径字数（CM_PATH = 2 字）+ CM_PATH + 数据区。
    let mut out = Vec::with_capacity(2 + CM_PATH.len() + data.len());
    out.push(SVC_MULTI);
    out.push((CM_PATH.len() / 2) as u8);
    out.extend_from_slice(&CM_PATH);
    out.extend_from_slice(&data);
    out
}

/// 单个子应答（Multi 解包产物）。
#[derive(Debug)]
pub struct SubReply<'a> {
    /// 完整子应答字节（service|0x80 开头）。
    pub bytes: &'a [u8],
}

/// 解析 Multi-Service 应答数据区，按偏移表切出各子应答。
///
/// # Errors
///
/// 计数不符、偏移越界、未恰好闭合时返回 `invalid_response`
/// ——结构性失步必须整体失败丢会话（与 S7 数据区闭合同一论证）。
pub fn parse_multi_reply(
    data: &[u8],
    expected_count: usize,
) -> Result<Vec<SubReply<'_>>, EtherIpError> {
    if data.len() < 2 {
        return Err(EtherIpError::invalid_response("Multi 应答截断".to_owned()));
    }
    let declared = usize::from(u16::from_le_bytes([data[0], data[1]]));
    if declared != expected_count {
        return Err(EtherIpError::invalid_response(format!(
            "Multi 子应答计数不符：期望 {expected_count}，收到 {declared}"
        )));
    }
    if data.len() < 2 + expected_count * 2 {
        return Err(EtherIpError::invalid_response(
            "Multi 偏移表截断".to_owned(),
        ));
    }
    let mut offsets = Vec::with_capacity(expected_count);
    for i in 0..expected_count {
        offsets.push(usize::from(u16::from_le_bytes([
            data[2 + i * 2],
            data[3 + i * 2],
        ])));
    }
    let mut replies = Vec::with_capacity(expected_count);
    for (i, &off) in offsets.iter().enumerate() {
        if off >= data.len() {
            return Err(EtherIpError::invalid_response(format!(
                "Multi 子应答偏移越界：{off} ≥ {}",
                data.len()
            )));
        }
        let end = offsets.get(i + 1).copied().unwrap_or(data.len());
        if end > data.len() || end < off {
            return Err(EtherIpError::invalid_response(format!(
                "Multi 子应答边界非法：[{off}, {end})"
            )));
        }
        replies.push(SubReply {
            bytes: &data[off..end],
        });
    }
    Ok(replies)
}

/// Multi 包内一条子请求的封装开销（偏移表项 2 字节；子请求本身另计）。
#[must_use]
pub fn sub_request_overhead() -> usize {
    2
}
