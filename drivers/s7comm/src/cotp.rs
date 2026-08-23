//! TPKT（RFC 1006）与 COTP TPDU 编解码（纯函数，无 I/O）。
//!
//! 字节布局（出处：Wireshark `tpkt`/`cotp` dissector 文档）：
//!
//! - TPKT：`[version=3][reserved=0][长度 BE u16]`，长度含头 4 字节；
//! - COTP CR：`[LI][type=0xE0][dst-ref 2][src-ref 2][class 1]
//!   [变参…]`；变参含 calling TSAP（0xC1）/ called TSAP（0xC2），远端
//!   TSAP 为 `[0x03, (rack<<5)|slot]`；
//! - COTP CC：同构，type=0xD0；
//! - COTP DT：`[LI=2][type=0xF0][TPDU-NR+EOT=0x80]`，载荷即 S7 PDU。
//!
//! LI 不计自身字节。

use crate::error::S7Error;

/// TPKT 协议版本。
pub const TPKT_VERSION: u8 = 3;
/// TPKT 头长度。
pub const TPKT_HEADER_LEN: usize = 4;
/// COTP Connection Request 类型码。
pub const COTP_CR: u8 = 0xE0;
/// COTP Connection Confirm 类型码。
pub const COTP_CC: u8 = 0xD0;
/// COTP Data Transfer 类型码。
pub const COTP_DT: u8 = 0xF0;

/// 单帧 TPKT 长度上限（协议上限 65_579；防御性拒绝异常长度）。
const MAX_TPKT_LEN: usize = 65_579;

/// 远端 TSAP 两字节编码：`[0x03, (rack<<5)|slot]`。
///
/// rack/slot 的取值范围已在 [`crate::config`] 校验（≤7/≤31），
/// 编码恒可落入单字节。
#[must_use]
pub fn remote_tsap(rack: u8, slot: u8) -> [u8; 2] {
    [0x03, (rack << 5) | slot]
}

/// 构造 COTP Connection Request TPDU（不含 TPKT 头）。
#[must_use]
pub fn connection_request(local_tsap: &[u8; 2], remote_tsap: &[u8; 2]) -> Vec<u8> {
    // 固定头：type(1)+dstref(2)+srcref(2)+class(1)=6；变参：两个 TSAP
    // 各 [code][len][2B]=4。LI = 6+8 = 14。
    vec![
        0x0E,
        COTP_CR,
        0x00,
        0x00, // dst-ref（CR 中置 0）
        0x00,
        0x0F, // src-ref（任意非零标识）
        0x00, // class 0 / options
        0xC1,
        0x02,
        local_tsap[0],
        local_tsap[1],
        0xC2,
        0x02,
        remote_tsap[0],
        remote_tsap[1],
    ]
}

/// 构造 COTP DT TPDU（不含 TPKT 头）：载荷为一条完整 S7 PDU。
#[must_use]
pub fn data_tpdu(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 3);
    out.push(0x02); // LI
    out.push(COTP_DT);
    out.push(0x80); // TPDU-NR 0 + EOT（末段）
    out.extend_from_slice(payload);
    out
}

/// 解析一帧完整 TPKT（头 + 体），返回内嵌 COTP 载荷视图：
/// - [`CotpFrame::ConnectionConfirm`]：握手确认（引用与 TSAP 不校验内容——
///   PLC 回显语义因型号而异，驱动只要求类型正确）；
/// - [`CotpFrame::Data`]：剥离 DT 头后的 S7 PDU。
///
/// # Errors
///
/// TPKT 版本/长度非法、COTP 结构坏时返回 `invalid_response`
/// （传输级：会话失步必须丢弃）。
pub fn parse_frame(frame: &[u8]) -> Result<CotpFrame<'_>, S7Error> {
    if frame.len() < TPKT_HEADER_LEN || frame[0] != TPKT_VERSION || frame[1] != 0 {
        return Err(S7Error::invalid_response(format!(
            "TPKT 头非法：version={:#04x} reserved={:#04x}",
            frame.first().copied().unwrap_or(0),
            frame.get(1).copied().unwrap_or(0)
        )));
    }
    let declared = usize::from(u16::from_be_bytes([frame[2], frame[3]]));
    if !(TPKT_HEADER_LEN..=MAX_TPKT_LEN).contains(&declared) || declared != frame.len() {
        return Err(S7Error::invalid_response(format!(
            "TPKT 长度不符：声明 {declared}，实际 {}",
            frame.len()
        )));
    }
    let body = &frame[TPKT_HEADER_LEN..];
    if body.len() < 2 {
        return Err(S7Error::invalid_response("COTP 头截断".to_owned()));
    }
    let li = body[0] as usize;
    match body[1] {
        COTP_CC => Ok(CotpFrame::ConnectionConfirm),
        COTP_DT => {
            // DT：LI 恒 2（type + TPDU-NR/EOT），EOT 位必须置位（单段传输）。
            if li < 2 || body.len() < li + 1 || body[2] & 0x80 == 0 {
                return Err(S7Error::invalid_response("COTP DT 结构非法".to_owned()));
            }
            Ok(CotpFrame::Data(&body[(li + 1)..]))
        }
        other => Err(S7Error::invalid_response(format!(
            "未预期的 COTP 类型 {other:#04x}"
        ))),
    }
}

/// 一帧 TPKT 的解析结果。
#[derive(Debug)]
pub enum CotpFrame<'a> {
    /// Connection Confirm（握手应答）。
    ConnectionConfirm,
    /// Data Transfer，载荷为 S7 PDU。
    Data(&'a [u8]),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_connection_request_with_tsaps() {
        let cr = connection_request(&[0x01, 0x00], &remote_tsap(0, 2));
        assert_eq!(cr[0] as usize, cr.len() - 1, "LI 等于除自身外的头长");
        assert_eq!(cr[1], COTP_CR);
        assert_eq!(&cr[cr.len() - 4..], &[0xC2, 0x02, 0x03, 0x02]);
        // rack<<5|slot：rack=1,slot=2 → 0x22。
        let cr2 = connection_request(&[0x01, 0x00], &remote_tsap(1, 2));
        assert_eq!(cr2[cr2.len() - 1], 0x22);
    }

    #[test]
    fn round_trips_data_tpdu_and_rejects_bad_frames() {
        let payload = [0x32, 0x01, 0xAA];
        let tpdu = data_tpdu(&payload);
        let mut frame = vec![TPKT_VERSION, 0, 0, 0];
        frame.extend_from_slice(&tpdu);
        let len = frame.len() as u16;
        frame[2] = (len >> 8) as u8;
        frame[3] = len as u8;

        match parse_frame(&frame).unwrap() {
            CotpFrame::Data(pdu) => assert_eq!(pdu, &payload),
            _ => panic!("应为 DT 帧"),
        }

        // 版本错误 / 截断 / 非 DT 类型均拒绝。
        assert!(parse_frame(&[4, 0, 0, 7, 2, COTP_DT, 0x80]).is_err());
        assert!(parse_frame(&[3, 0, 0, 5, 0x02]).is_err());
    }
}
