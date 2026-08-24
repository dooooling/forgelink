//! EtherNet/IP 封装层编解码（纯函数，无 I/O）。
//!
//! # 帧结构（出处：Wireshark `enip` dissector 文档）
//!
//! 封装头 24 字节**小端**（与 modbus/S7 大端相反！）：
//!
//! ```text
//! [command u16][length u16][session handle u32][status u32]
//! [sender context 8B][options u32]
//! ```
//!
//! - RegisterSession = 0x0065：体 `[version=1 u16][option=0 u16]`，
//!   应答在头内携带分配的 session handle；
//! - UnregisterSession = 0x0066；
//! - SendRRData = 0x00F0：体 `[interface handle u32=0][timeout u16]
//!   [item count u16=2][地址项 type=0x0000 len=0]
//!   [数据项 type=0x00B1 len=N + CIP 消息]`。
//!
//! sender context 由本端填充、应答必须逐字节回显——驱动用作响应匹配
//! nonce（封装层无 S7 式 pdu-ref；context 是等价的迟到帧防护）。

use crate::error::EtherIpError;

/// 命令：RegisterSession。
pub const CMD_REGISTER_SESSION: u16 = 0x00_65;
/// 命令：UnregisterSession。
pub const CMD_UNREGISTER_SESSION: u16 = 0x00_66;
/// 命令：SendRRData。
pub const CMD_SEND_RR_DATA: u16 = 0x00_F0;

/// 封装头长度。
pub const HEADER_LEN: usize = 24;

/// sender context 类型（8 字节 opaque；驱动以自增 nonce 填充）。
pub type SenderContext = [u8; 8];

/// 构造封装头（小端）。
#[must_use]
pub fn build_header(
    command: u16,
    body_len: usize,
    session: u32,
    status: u32,
    context: &SenderContext,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN);
    out.extend_from_slice(&command.to_le_bytes());
    out.extend_from_slice(&(body_len as u16).to_le_bytes());
    out.extend_from_slice(&session.to_le_bytes());
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(context);
    out.extend_from_slice(&[0; 4]); // options
    out
}

/// 构造完整 RegisterSession 请求帧。
#[must_use]
pub fn build_register_session(context: &SenderContext) -> Vec<u8> {
    let body = [0x01, 0x00, 0x00, 0x00]; // version=1 LE, option=0
    let mut frame = build_header(CMD_REGISTER_SESSION, body.len(), 0, 0, context);
    frame.extend_from_slice(&body);
    frame
}

/// 构造完整 UnregisterSession 请求帧（best-effort 发送，忽略应答）。
#[must_use]
pub fn build_unregister_session(session: u32, context: &SenderContext) -> Vec<u8> {
    build_header(CMD_UNREGISTER_SESSION, 0, session, 0, context)
}

/// 解析封装头。
///
/// 返回 `(command, body_len, session_handle, status, context)`。
///
/// # Errors
///
/// 头截断或 options 非 0 时返回 `invalid_response`。
pub fn parse_header(frame: &[u8]) -> Result<(u16, usize, u32, u32, SenderContext), EtherIpError> {
    if frame.len() < HEADER_LEN {
        return Err(EtherIpError::invalid_response(format!(
            "封装头截断：{} < {HEADER_LEN}",
            frame.len()
        )));
    }
    let options = u32::from_le_bytes([frame[20], frame[21], frame[22], frame[23]]);
    if options != 0 {
        return Err(EtherIpError::invalid_response(format!(
            "封装 options 非 0：{options:#010x}"
        )));
    }
    let mut context: SenderContext = [0; 8];
    context.copy_from_slice(&frame[12..20]);
    Ok((
        u16::from_le_bytes([frame[0], frame[1]]),
        usize::from(u16::from_le_bytes([frame[2], frame[3]])),
        u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]),
        u32::from_le_bytes([frame[8], frame[9], frame[10], frame[11]]),
        context,
    ))
}

/// 解析 RegisterSession 应答体，返回分配的 session handle。
///
/// # Errors
///
/// 体长不符或版本回显非法时返回 `invalid_response`；status != 0 由
/// 调用方在头解析后统一处理（[`parse_header`] 已取出 status）。
pub fn parse_register_session_reply(body: &[u8]) -> Result<u32, EtherIpError> {
    // 应答体：[version=1 u16][option=0 u16][session handle u32]。
    if body.len() < 8 {
        return Err(EtherIpError::invalid_response(format!(
            "RegisterSession 应答体截断：{}",
            body.len()
        )));
    }
    let version = u16::from_le_bytes([body[0], body[1]]);
    if version != 1 {
        return Err(EtherIpError::invalid_response(format!(
            "RegisterSession 版本回显非法：{version}"
        )));
    }
    Ok(u32::from_le_bytes([body[4], body[5], body[6], body[7]]))
}

/// 把 CIP 消息包进 SendRRData 请求体。
#[must_use]
pub fn wrap_rr_data(cip: &[u8]) -> Vec<u8> {
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

/// 从 SendRRData 体剥出 CIP 载荷。
///
/// # Errors
///
/// 体截断、item 数非 2、地址/数据项类型不符时返回 `invalid_response`。
pub fn unwrap_rr_data(body: &[u8]) -> Result<&[u8], EtherIpError> {
    if body.len() < 12 {
        return Err(EtherIpError::invalid_response(format!(
            "SendRRData 体截断：{}",
            body.len()
        )));
    }
    let item_count = u16::from_le_bytes([body[6], body[7]]);
    if item_count != 2 {
        return Err(EtherIpError::invalid_response(format!(
            "SendRRData item count 非 2：{item_count}"
        )));
    }
    let addr_type = u16::from_le_bytes([body[8], body[9]]);
    let addr_len = usize::from(u16::from_le_bytes([body[10], body[11]]));
    if addr_type != 0x0000 || addr_len != 0 {
        return Err(EtherIpError::invalid_response(format!(
            "地址项非法：type={addr_type:#06x} len={addr_len}"
        )));
    }
    let data_off = 12;
    if body.len() < data_off + 4 {
        return Err(EtherIpError::invalid_response("数据项头截断".to_owned()));
    }
    let data_type = u16::from_le_bytes([body[data_off], body[data_off + 1]]);
    let data_len = usize::from(u16::from_le_bytes([body[data_off + 2], body[data_off + 3]]));
    if data_type != 0x00_B1 || body.len() < data_off + 4 + data_len {
        return Err(EtherIpError::invalid_response(format!(
            "数据项非法：type={data_type:#06x} len={data_len} 总长 {}",
            body.len()
        )));
    }
    Ok(&body[data_off + 4..data_off + 4 + data_len])
}
