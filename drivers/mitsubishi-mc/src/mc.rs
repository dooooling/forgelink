//! MC 3E 二进制帧编解码（纯函数，无 I/O）。
//!
//! # 帧结构（出处：《MELSEC SLMP 参考手册》SH-081948ENG，QnA 兼容 3E 帧）
//!
//! ```text
//! 请求：[副头 0x0050 u16 LE][网络号 u8][PC 号 u8][模块 I/O u16 LE]
//!       [站号 u8][请求数据长 u16 LE] + 指令区
//! 指令区：[监视定时器 u16 LE][指令 u16 LE][子指令 u16 LE]
//!         [软元件代码 u8][软元件号 3B LE][点数 u16 LE] +（写）数据
//! 应答：[副头 0x00D0 u16 LE][路由区五字段回声][应答数据长 u16 LE]
//!       + [结束代码 u16 LE] +（读）数据
//! ```
//!
//! 软元件号为 **3 字节小端**（u24）——不是 u16。全帧无事务号/指令回显，
//! 响应匹配依赖结构自洽校验（副头 + 路由区回声 + 数据长），论证见
//! `session` 模块文档。
//!
//! # 结束代码表（常用项；未知码兜底通用文案）
//!
//! 出处同上手册「错误代码」章：C058 点数/编号超限、C059 软元件代码错、
//! C04D 请求数据长异常、C051 参数错、C050? 保守略——未知码携带原始码。

use crate::address::DeviceKind;
use crate::error::McError;

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
/// 子指令：二进制模式访问（3E 帧固定 0x0000——按点为单位不做字/字节单位混算）。
const SUBCOMMAND_BINARY: u16 = 0x0000;

/// 请求固定头长（副头 2 + 路由区 5 + 数据长 2）。
pub const REQUEST_HEAD_LEN: usize = 9;
/// 应答固定头长（副头 2 + 路由区 5 + 数据长 2）。
pub const RESPONSE_HEAD_LEN: usize = 9;

/// 软元件访问单位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// 位软元件（X/Y/M/B/S/SM）：按点访问，数据为位串。
    Bit,
    /// 字软元件（D/W/R/ZR/SD）：按 16 位字访问。
    Word,
}

impl DeviceKind {
    /// 访问单位与对应批量指令码。
    #[must_use]
    pub const fn unit(self) -> Unit {
        if self.is_bit() { Unit::Bit } else { Unit::Word }
    }
}

/// 路由区五字段（网络号/PC 号/模块 I/O/站号 + 占位对齐）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Routing {
    pub network_no: u8,
    pub pc_no: u8,
    pub module_io: u16,
    pub module_station: u8,
}

impl Routing {
    /// 编码为路由区 5 字节。
    fn encode(&self) -> [u8; 5] {
        let io = self.module_io.to_le_bytes();
        [
            self.network_no,
            self.pc_no,
            io[0],
            io[1],
            self.module_station,
        ]
    }

    /// 从应答帧头解码路由区（回声校验用）。
    pub(crate) fn decode(frame: &[u8]) -> Option<Self> {
        if frame.len() < 7 {
            return None;
        }
        Some(Self {
            network_no: frame[2],
            pc_no: frame[3],
            module_io: u16::from_le_bytes([frame[4], frame[5]]),
            module_station: frame[6],
        })
    }
}

/// 构造批量读请求帧（按软元件单位自动选 0x0401/0x0402）。
#[must_use]
pub fn build_read_request(
    kind: DeviceKind,
    start_number: u32,
    points: u16,
    monitoring_timer: u16,
    routing: &Routing,
) -> Vec<u8> {
    let command = match kind.unit() {
        Unit::Word => CMD_READ_WORD,
        Unit::Bit => CMD_READ_BIT,
    };
    // 指令区 = 监视定时器(2) + 指令(2) + 子指令(2) + 软元件(4) + 点数(2)。
    let body = build_command_body(command, kind, start_number, points, &[]);
    assemble_frame(SUBHEADER_REQUEST, routing, monitoring_timer, &body)
}

/// 构造批量写请求帧。
#[must_use]
pub fn build_write_request(
    kind: DeviceKind,
    start_number: u32,
    points: u16,
    data: &[u8],
    monitoring_timer: u16,
    routing: &Routing,
) -> Vec<u8> {
    let command = match kind.unit() {
        Unit::Word => CMD_WRITE_WORD,
        Unit::Bit => CMD_WRITE_BIT,
    };
    let body = build_command_body(command, kind, start_number, points, data);
    assemble_frame(SUBHEADER_REQUEST, routing, monitoring_timer, &body)
}

fn build_command_body(
    command: u16,
    kind: DeviceKind,
    start_number: u32,
    points: u16,
    data: &[u8],
) -> Vec<u8> {
    let mut body = Vec::with_capacity(12 + data.len());
    // 监视定时器占位于 assemble_frame 写入前的指令区首部：
    // 这里先放监视定时器占位（由 assemble_frame 覆盖）。
    body.extend_from_slice(&[0, 0]);
    body.extend_from_slice(&command.to_le_bytes());
    body.extend_from_slice(&SUBCOMMAND_BINARY.to_le_bytes());
    body.push(kind.code());
    // 软元件号 u24 小端（3 字节）。
    let n = start_number.to_le_bytes();
    body.extend_from_slice(&n[0..3]);
    body.extend_from_slice(&points.to_le_bytes());
    body.extend_from_slice(data);
    body
}

/// 组装完整请求帧（监视定时器写入指令区首部、数据长覆盖指令区总长）。
fn assemble_frame(
    subheader: u16,
    routing: &Routing,
    monitoring_timer: u16,
    body: &[u8],
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(REQUEST_HEAD_LEN + body.len());
    frame.extend_from_slice(&subheader.to_le_bytes());
    frame.extend_from_slice(&routing.encode());
    // 请求数据长 = 监视定时器(2) + 其余指令区。
    frame.extend_from_slice(&(body.len() as u16).to_le_bytes());
    frame.extend_from_slice(body);
    // 回填监视定时器到指令区首部。
    let at = REQUEST_HEAD_LEN;
    frame[at] = (monitoring_timer & 0xFF) as u8;
    frame[at + 1] = (monitoring_timer >> 8) as u8;
    frame
}

/// 解析后的应答头部信息。
#[derive(Debug)]
pub struct ResponseHead {
    /// 结束代码（0 = 成功）。
    pub end_code: u16,
    /// 声明的应答数据长（含结束代码自身 2 字节）。
    pub declared_len: usize,
}

/// 解析应答帧头并做三层自洽校验（副头 + 路由区回声 + 数据长存在性）。
///
/// 返回 `(头信息, 应答体切片)`。
///
/// # Errors
///
/// 副头不符 → `unexpected_subheader`；路由区回声不符或帧截断 →
/// `invalid_response`（均为失步丢会话）。
pub fn parse_response_head<'a>(
    frame: &'a [u8],
    expect_routing: &Routing,
) -> Result<(ResponseHead, &'a [u8]), McError> {
    if frame.len() < RESPONSE_HEAD_LEN {
        return Err(McError::invalid_response(format!(
            "应答帧截断：{} < {RESPONSE_HEAD_LEN}",
            frame.len()
        )));
    }
    let subheader = u16::from_le_bytes([frame[0], frame[1]]);
    if subheader != SUBHEADER_RESPONSE {
        return Err(McError::unexpected_subheader(subheader));
    }
    let got_routing =
        Routing::decode(frame).ok_or_else(|| McError::invalid_response("应答帧截断".to_owned()))?;
    if got_routing != *expect_routing {
        return Err(McError::invalid_response(format!(
            "应答路由区回声不符：期望 {expect_routing:?}，收到 {got_routing:?}"
        )));
    }
    let declared_len = usize::from(u16::from_le_bytes([frame[7], frame[8]]));
    if frame.len() < RESPONSE_HEAD_LEN + declared_len {
        return Err(McError::invalid_response(format!(
            "应答体不足：声明 {declared_len}，实际 {}",
            frame.len() - RESPONSE_HEAD_LEN
        )));
    }
    let body = &frame[RESPONSE_HEAD_LEN..RESPONSE_HEAD_LEN + declared_len];
    if body.len() < 2 {
        return Err(McError::invalid_response("应答缺结束代码".to_owned()));
    }
    Ok((
        ResponseHead {
            end_code: u16::from_le_bytes([body[0], body[1]]),
            declared_len,
        },
        body,
    ))
}

/// 按请求参数计算期望的应答体长度（结束代码 + 读数据）。
///
/// 写请求应答仅含结束代码（2 字节）；读字含 points×2 字节、读位含
/// ceil(points/8) 字节。
#[must_use]
pub fn expected_resp_body_len(command: u16, points: u16) -> usize {
    match command {
        CMD_READ_WORD => 2 + usize::from(points) * 2,
        CMD_READ_BIT => 2 + usize::from(points).div_ceil(8),
        _ => 2,
    }
}

/// 由软元件种类与点数换算出实际发出的指令码（读侧）。
///
/// # Errors
///
/// 位/字单位不匹配返回 `config_error`（不可达——规划层已保证）。
pub fn read_command_of(kind: DeviceKind) -> u16 {
    match kind.unit() {
        Unit::Word => CMD_READ_WORD,
        Unit::Bit => CMD_READ_BIT,
    }
}

/// 由软元件种类与点数换算出实际发出的指令码（写侧）。
#[must_use]
pub fn write_command_of(kind: DeviceKind) -> u16 {
    match kind.unit() {
        Unit::Word => CMD_WRITE_WORD,
        Unit::Bit => CMD_WRITE_BIT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_routing() -> Routing {
        Routing {
            network_no: 0,
            pc_no: 0,
            module_io: 0x03FF,
            module_station: 0,
        }
    }

    #[test]
    fn golden_read_word_request_bytes() {
        // D200 起 3 字读：精确 19 字节断言（Phase 0 手册帧布局固化）。
        let frame = build_read_request(DeviceKind::D, 200, 3, 2000, &test_routing());
        assert_eq!(frame.len(), REQUEST_HEAD_LEN + 12);
        assert_eq!(&frame[0..2], &[0x50, 0x00], "副头 0x0050 LE");
        assert_eq!(frame[4], 0xFF, "模块 I/O 低字节");
        assert_eq!(frame[5], 0x03, "模块 I/O 高字节");
        let data_len = u16::from_le_bytes([frame[7], frame[8]]);
        assert_eq!(data_len, 12);
        // 指令区：监视定时器 2000 LE + 0x0401 + 子指令 0 + D=0xA8 + u24(200) + 3。
        assert_eq!(
            &frame[9..],
            &[0xD0, 0x07, 0x01, 0x04, 0x00, 0x00, 0xA8, 200, 0, 0, 3, 0]
        );
    }

    #[test]
    fn golden_write_bit_request_bytes() {
        // Y40 起 10 位写，位串 0b10100101（LSB first）。
        let frame = build_write_request(
            DeviceKind::Y,
            40,
            10,
            &[0b1010_0101, 0b11],
            2000,
            &test_routing(),
        );
        // 指令区 12 + 数据 2 = 14。
        assert_eq!(frame.len(), REQUEST_HEAD_LEN + 14);
        let cmd = u16::from_le_bytes([frame[11], frame[12]]);
        assert_eq!(cmd, CMD_WRITE_BIT);
        assert_eq!(frame[15], DeviceKind::Y.code());
        let n = u32::from_le_bytes([frame[16], frame[17], frame[18], 0]);
        assert_eq!(n, 40);
        assert_eq!(&frame[frame.len() - 2..], &[0b1010_0101, 0b11]);
    }

    #[test]
    fn response_head_validates_echo_and_rejects_mismatch() {
        let request = build_read_request(DeviceKind::D, 200, 1, 2000, &test_routing());
        // 手工构造合法应答：副头 0x00D0 + 路由区回声 + 数据长 4 + 结束码 0 + 数据。
        let mut reply = vec![0xD0, 0x00];
        reply.extend_from_slice(&request[2..7]); // 路由区原样回声
        reply.extend_from_slice(&4u16.to_le_bytes());
        reply.extend_from_slice(&0u16.to_le_bytes()); // 结束代码成功
        reply.extend_from_slice(&0x1234u16.to_le_bytes()); // 读到的字

        let (head, body) = parse_response_head(&reply, &test_routing()).unwrap();
        assert_eq!(head.end_code, 0);
        assert_eq!(head.declared_len, 4);
        assert_eq!(body, &[0x00, 0x00, 0x34, 0x12]);

        // 路由区被篡改 → 失步拒绝。
        let mut bad = reply.clone();
        bad[4] ^= 0xFF;
        assert!(parse_response_head(&bad, &test_routing()).is_err());

        // 副头错乱 → unexpected_subheader。
        let mut bad = reply.clone();
        bad[0] = 0x50;
        let err = parse_response_head(&bad, &test_routing()).unwrap_err();
        assert_eq!(err.code, "unexpected_subheader");
    }

    #[test]
    fn expected_lengths_match_protocol() {
        assert_eq!(expected_resp_body_len(CMD_READ_WORD, 3), 8);
        assert_eq!(expected_resp_body_len(CMD_READ_BIT, 10), 4);
        assert_eq!(expected_resp_body_len(CMD_WRITE_WORD, 3), 2);
    }
}
