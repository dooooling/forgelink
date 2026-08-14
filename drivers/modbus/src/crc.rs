//! CRC16-Modbus（多项式 0xA001，低位先行）。
//!
//! RTU 帧校验：初始值 `0xFFFF`，按字节异或-右移 8 次，结果小端附加在帧尾。

/// 计算 CRC16-Modbus 校验值（初始 `0xFFFF`）。
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for byte in data {
        crc ^= *byte as u16;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xA001
            } else {
                crc >> 1
            };
        }
    }
    crc
}

/// 校验一帧的 CRC（帧末两字节为小端 CRC）。
pub fn verify(frame: &[u8]) -> bool {
    if frame.len() < 2 {
        return false;
    }
    let (payload, expected) = frame.split_at(frame.len() - 2);
    let expected = u16::from_le_bytes([expected[0], expected[1]]);
    crc16(payload) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 标准测试向量：请求 `01 03 00 00 00 01` 的 CRC 寄存器值为 `0x0A84`
    /// （帧尾附加顺序 `84 0A`，Modbus 规范 CRC = 0x840A）。
    #[test]
    fn crc_known_vector_request() {
        assert_eq!(crc16(&[0x01, 0x03, 0x00, 0x00, 0x00, 0x01]), 0x0A84);
    }

    /// 标准测试向量：响应 `01 03 02 00 0B` 的 CRC 寄存器值为 `0x83F9`
    /// （帧尾附加顺序 `F9 83`）。
    #[test]
    fn crc_known_vector_response() {
        assert_eq!(crc16(&[0x01, 0x03, 0x02, 0x00, 0x0B]), 0x83F9);
    }

    /// CRC 对单字节翻转敏感。
    #[test]
    fn crc_detects_bit_flip() {
        let frame = [0x01, 0x03, 0x02, 0x00, 0x0B, 0xF9, 0x83];
        assert!(verify(&frame));
        let mut flipped = frame;
        flipped[3] ^= 0x01;
        assert!(!verify(&flipped));
    }

    #[test]
    fn verify_requires_two_bytes() {
        assert!(!verify(&[]));
        assert!(!verify(&[0x01]));
    }
}
