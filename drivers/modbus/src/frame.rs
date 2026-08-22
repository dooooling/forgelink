//! Modbus 帧编解码（TCP MBAP 与 RTU）与功能码/异常码表。
//!
//! 全部为纯函数帧构造/解析（同步 I/O 在 `session` 层），帧级逻辑可用
//! 单元测试覆盖（RTU 无串口亦可验证，§33 验收）。

use crate::address::RegisterKind;

/// 读功能码（FC01/FC02/FC03/FC04）。
pub const FC_READ_COILS: u8 = 0x01;
/// 读离散输入功能码。
pub const FC_READ_DISCRETE_INPUTS: u8 = 0x02;
/// 读保持寄存器功能码。
pub const FC_READ_HOLDING_REGISTERS: u8 = 0x03;
/// 读输入寄存器功能码。
pub const FC_READ_INPUT_REGISTERS: u8 = 0x04;

/// 写单线圈功能码（FC05）：单布尔值写线圈，ON = 0xFF00、OFF = 0x0000。
pub const FC_WRITE_SINGLE_COIL: u8 = 0x05;
/// 写单保持寄存器功能码（FC06）：恰好 1 个寄存器。
pub const FC_WRITE_SINGLE_REGISTER: u8 = 0x06;
/// 写多线圈功能码（FC15）：连续位区间批量写。
pub const FC_WRITE_MULTIPLE_COILS: u8 = 0x0F;
/// 写多保持寄存器功能码（FC16）：连续寄存器区间批量写。
pub const FC_WRITE_MULTIPLE_REGISTERS: u8 = 0x10;

/// 协议帧上限：单次寄存器读 125 个寄存器、线圈/离散读 2000 位。
pub const MAX_REGISTERS_PER_REQUEST: u16 = 125;
/// 位读（FC01/FC02）单帧最大数量（2000 位）。
pub const MAX_BITS_PER_REQUEST: u16 = 2_000;

/// 写寄存器（FC16）单帧上限：123 个寄存器（PDU ≤ 253 字节约束，
/// 与读侧 125 不同——写请求额外携带数量与字节计数字段）。
pub const MAX_WRITE_REGISTERS_PER_REQUEST: u16 = 123;
/// 写线圈（FC15）单帧上限：1968 位（PDU 上限内 246 数据字节 × 8 位）。
pub const MAX_WRITE_BITS_PER_REQUEST: u16 = 1_968;

/// 是否为写功能码（FC05/FC06/FC15/FC16）。
pub fn is_write_function(function: u8) -> bool {
    matches!(
        function,
        FC_WRITE_SINGLE_COIL
            | FC_WRITE_SINGLE_REGISTER
            | FC_WRITE_MULTIPLE_COILS
            | FC_WRITE_MULTIPLE_REGISTERS
    )
}

/// 功能码对应的读请求类型。
pub fn read_function_code(kind: RegisterKind) -> u8 {
    kind.function_code()
}

/// 读/写响应的期望数据字节数（由功能码与数量决定）。
///
/// 写响应（FC05/06/15/16）统一回显 4 字节：地址 + 值/数量，
/// 无 Byte Count 字段（与读响应结构不同）。
pub fn expected_data_len(function: u8, quantity: u16) -> usize {
    match function {
        FC_READ_COILS | FC_READ_DISCRETE_INPUTS => quantity.div_ceil(8) as usize,
        FC_READ_HOLDING_REGISTERS | FC_READ_INPUT_REGISTERS => quantity as usize * 2,
        FC_WRITE_SINGLE_COIL
        | FC_WRITE_SINGLE_REGISTER
        | FC_WRITE_MULTIPLE_COILS
        | FC_WRITE_MULTIPLE_REGISTERS => 4,
        _ => 0,
    }
}

/// 读请求是否为位操作（coil/discrete，决定拆包上限与解码方式）。
pub fn is_bit_function(function: u8) -> bool {
    matches!(function, FC_READ_COILS | FC_READ_DISCRETE_INPUTS)
}

/// 构建 TCP（MBAP）读请求帧。
///
/// 结构：`[transaction_id(2) protocol_id(2) length(2) unit(1) function(1) addr(2) quantity(2)]`。
pub fn build_tcp_read_request(
    transaction_id: u16,
    unit_id: u8,
    function: u8,
    start_offset: u16,
    quantity: u16,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(12);
    frame.extend_from_slice(&transaction_id.to_be_bytes());
    frame.extend_from_slice(&0u16.to_be_bytes()); // protocol id = 0（Modbus）
    frame.extend_from_slice(&6u16.to_be_bytes()); // 后续字节数（unit+function+addr+quantity）
    frame.push(unit_id);
    frame.push(function);
    frame.extend_from_slice(&start_offset.to_be_bytes());
    frame.extend_from_slice(&quantity.to_be_bytes());
    frame
}

/// 构建 RTU 读请求帧（`build_tcp_read_request` 的载荷 + CRC）。
pub fn build_rtu_read_request(
    unit_id: u8,
    function: u8,
    start_offset: u16,
    quantity: u16,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8);
    frame.push(unit_id);
    frame.push(function);
    frame.extend_from_slice(&start_offset.to_be_bytes());
    frame.extend_from_slice(&quantity.to_be_bytes());
    let crc = crate::crc::crc16(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

/// 构建 TCP（MBAP）写请求帧。
///
/// 结构：`[transaction_id(2) protocol_id(2) length(2) unit(1) function(1)
/// addr(2) payload(..)]`。`payload` 为功能码后随载荷（不含地址）：
/// FC05/06 为 2 字节值；FC15/16 为数量(2) + 字节计数(1) + 数据。
pub fn build_tcp_write_request(
    transaction_id: u16,
    unit_id: u8,
    function: u8,
    start_offset: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(6 + 4 + payload.len());
    frame.extend_from_slice(&transaction_id.to_be_bytes());
    frame.extend_from_slice(&0u16.to_be_bytes()); // protocol id = 0（Modbus）
    // 后续字节数（unit + function + addr + payload）。
    frame.extend_from_slice(&((4 + payload.len()) as u16).to_be_bytes());
    frame.push(unit_id);
    frame.push(function);
    frame.extend_from_slice(&start_offset.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// 构建 RTU 写请求帧（`build_tcp_write_request` 的载荷 + CRC）。
pub fn build_rtu_write_request(
    unit_id: u8,
    function: u8,
    start_offset: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(3 + payload.len());
    frame.push(unit_id);
    frame.push(function);
    frame.extend_from_slice(&start_offset.to_be_bytes());
    frame.extend_from_slice(payload);
    let crc = crate::crc::crc16(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

/// 校验写响应回显（FC05/06/15/16 正常响应均为 4 字节数据）。
///
/// Modbus 规定写响应必须逐字节回显请求：`[addr_hi, addr_lo, tail0, tail1]`，
/// 其中 `tail` 为请求载荷前 2 字节（FC05/06 为写入值，FC15/16 为数量）。
/// 回显不符说明响应与请求错位（或设备实现有缺陷），按响应失步拒绝，
/// 避免把未确认的写入当作成功。
pub fn validate_write_echo(
    data: &[u8],
    start_offset: u16,
    payload: &[u8],
) -> Result<(), FrameError> {
    if data.len() != 4 {
        return Err(FrameError::Invalid("写响应数据不是 4 字节回显"));
    }
    if data[0..2] != start_offset.to_be_bytes() {
        return Err(FrameError::Invalid("写响应回显地址与请求不一致"));
    }
    if payload.len() < 2 || data[2..4] != payload[0..2] {
        return Err(FrameError::Invalid("写响应回显值/数量与请求不一致"));
    }
    Ok(())
}

/// 从 TCP MBAP 响应头（7 字节）解析 unit/function，并计算后续数据字节数。
///
/// # Errors
///
/// - 头长度不足 / protocol id 非 0 / length < 2：无效帧。
/// - 响应数据（body）首字节为功能码：高位为 1 表示异常响应（后续 2 字节异常码）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpResponseHeader {
    /// 事务号（须与请求一致）。
    pub transaction_id: u16,
    /// 从站号（须与请求一致）。
    pub unit_id: u8,
    /// 紧随头部之后的数据字节数（含 body 首字节功能码）。
    pub data_len: usize,
}

/// 解析 TCP 响应头（MBAP 头 7 字节：事务号/协议号/长度/从站号）。
///
/// `data_len = length - 1`（length 含 unit；unit 已位于头第 7 字节，
/// 剩余 body 为 function + 数据，由调用方读取）。
pub fn parse_tcp_response_header(header: &[u8]) -> Result<TcpResponseHeader, FrameError> {
    if header.len() < 7 {
        return Err(FrameError::Truncated("MBAP 头不足 7 字节"));
    }
    let protocol = u16::from_be_bytes([header[2], header[3]]);
    if protocol != 0 {
        return Err(FrameError::Invalid("protocol id 非 0"));
    }
    let length = u16::from_be_bytes([header[4], header[5]]) as usize;
    if length < 2 {
        return Err(FrameError::Invalid("MBAP length 小于 2"));
    }
    Ok(TcpResponseHeader {
        transaction_id: u16::from_be_bytes([header[0], header[1]]),
        unit_id: header[6],
        data_len: length - 1,
    })
}

/// RTU 响应（unit + function + data + CRC）解析结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtuResponseMeta {
    /// 从站号。
    pub unit_id: u8,
    /// 功能码（异常响应时已去除高位）。
    pub function: u8,
    /// 是否异常响应（功能码高位为 1）。
    pub is_exception: bool,
    /// 数据字节数（异常响应恒为 1：异常码；正常响应为帧长 - 5）。
    pub data_len: usize,
}

/// 解析 RTU 响应帧头（unit + function），返回后续期望字节数（含 CRC）。
pub fn rtu_response_total_len(
    frame: &[u8],
    request_function: u8,
    request_quantity: u16,
) -> Result<usize, FrameError> {
    if frame.len() < 2 {
        return Err(FrameError::Truncated("RTU 响应不足 2 字节"));
    }
    let function = frame[1];
    let is_exception = function & 0x80 != 0;
    let function = function & 0x7F;
    if function != request_function {
        return Err(FrameError::Invalid("响应功能码与请求不一致"));
    }
    // 正常读响应：unit + fc + byte count + data + CRC = 5 + data_len 字节；
    // 正常写响应无 byte count 字段（数据即 4 字节回显）；
    // 异常响应：unit + fc(置高位) + 异常码 + CRC = 5 字节（无 byte count）。
    let data_len = if is_exception {
        1
    } else if is_write_function(function) {
        expected_data_len(function, request_quantity)
    } else {
        1 + expected_data_len(function, request_quantity)
    };
    Ok(2 + data_len + 2)
}

/// 从 RTU 响应解析元信息。
pub fn parse_rtu_response_meta(frame: &[u8]) -> Result<RtuResponseMeta, FrameError> {
    if frame.len() < 4 {
        return Err(FrameError::Truncated("RTU 响应不足 4 字节"));
    }
    let unit_id = frame[0];
    let raw_function = frame[1];
    let data_len = if raw_function & 0x80 != 0 {
        1
    } else {
        // 数据字节数 = 帧长 - unit - fc - byte count - CRC。
        frame.len() - 5
    };
    Ok(RtuResponseMeta {
        unit_id,
        function: raw_function & 0x7F,
        is_exception: raw_function & 0x80 != 0,
        data_len,
    })
}

/// 帧解析错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// 数据不足。
    Truncated(&'static str),
    /// 协议字段非法。
    Invalid(&'static str),
}

/// Modbus 异常码表（异常响应的 data 字节）。
///
/// 分类规则：
/// - `0x01~0x03`：非法功能/地址/数值——配置或寻址错误，重试无意义（`retryable = false`）；
/// - `0x04~0x0B`：设备/网关瞬态（忙碌、故障、网关超时）——`retryable = true`；
/// - 未知码保守标记可重试。
pub fn exception_code_name(code: u8) -> (&'static str, bool) {
    match code {
        0x01 => ("illegal function", false),
        0x02 => ("illegal data address", false),
        0x03 => ("illegal data value", false),
        0x04 => ("slave device failure", true),
        0x05 => ("acknowledge", true),
        0x06 => ("slave device busy", true),
        0x08 => ("memory parity error", true),
        0x0A => ("gateway path unavailable", true),
        0x0B => ("gateway target device failed to respond", true),
        _ => ("unknown modbus exception", true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_read_request_structure() {
        // 1!40001 -> holding offset 0，读 2 个寄存器。
        let frame = build_tcp_read_request(7, 1, FC_READ_HOLDING_REGISTERS, 0, 2);
        assert_eq!(frame.len(), 12);
        assert_eq!(&frame[0..2], &[0x00, 0x07]); // transaction
        assert_eq!(&frame[2..4], &[0x00, 0x00]); // protocol
        assert_eq!(&frame[4..6], &[0x00, 0x06]); // length
        assert_eq!(frame[6], 1); // unit
        assert_eq!(frame[7], 0x03); // FC03
        assert_eq!(&frame[8..10], &[0x00, 0x00]); // offset
        assert_eq!(&frame[10..12], &[0x00, 0x02]); // quantity
    }

    #[test]
    fn rtu_read_request_crc_round_trip() {
        let frame = build_rtu_read_request(1, FC_READ_HOLDING_REGISTERS, 0, 2);
        assert_eq!(frame.len(), 8);
        assert!(crate::crc::verify(&frame));
    }

    #[test]
    fn parse_tcp_response_header_ok() {
        // 正常响应：unit=1, 数据 4 字节（length=5 含 unit，body 为 function+字节数+数据）。
        let header = [0x00, 0x07, 0x00, 0x00, 0x00, 0x05, 0x01];
        let meta = parse_tcp_response_header(&header).unwrap();
        assert_eq!(meta.transaction_id, 7);
        assert_eq!(meta.unit_id, 1);
        assert_eq!(meta.data_len, 4);
    }

    #[test]
    fn parse_tcp_response_header_exception() {
        let header = [0x00, 0x08, 0x00, 0x00, 0x00, 0x03, 0x01];
        let meta = parse_tcp_response_header(&header).unwrap();
        assert_eq!(meta.data_len, 2);
    }

    #[test]
    fn parse_tcp_response_header_rejects_bad_protocol() {
        let header = [0x00, 0x07, 0x00, 0x01, 0x00, 0x05, 0x01];
        assert_eq!(
            parse_tcp_response_header(&header),
            Err(FrameError::Invalid("protocol id 非 0"))
        );
    }

    #[test]
    fn expected_data_len_matches_function() {
        assert_eq!(expected_data_len(FC_READ_HOLDING_REGISTERS, 2), 4);
        assert_eq!(expected_data_len(FC_READ_INPUT_REGISTERS, 125), 250);
        assert_eq!(expected_data_len(FC_READ_COILS, 8), 1);
        assert_eq!(expected_data_len(FC_READ_DISCRETE_INPUTS, 2000), 250);
        assert_eq!(expected_data_len(FC_READ_COILS, 1), 1);
    }

    #[test]
    fn rtu_total_len_matches() {
        // FC03 读 2 寄存器：unit+fc+byte count+4 数据+CRC = 9 字节。
        assert_eq!(
            rtu_response_total_len(&[0x01, 0x03], FC_READ_HOLDING_REGISTERS, 2).unwrap(),
            9
        );
        assert_eq!(
            rtu_response_total_len(&[0x01, 0x83], FC_READ_HOLDING_REGISTERS, 2).unwrap(),
            5
        );
        // 3 位线圈 -> 1 字节数据 + byte count + 2 CRC。
        assert_eq!(
            rtu_response_total_len(&[0x01, 0x01], FC_READ_COILS, 3).unwrap(),
            6
        );
        // 125 寄存器 -> 1 + 250 字节。
        assert_eq!(
            rtu_response_total_len(&[0x01, 0x03], FC_READ_HOLDING_REGISTERS, 125).unwrap(),
            2 + 1 + 250 + 2
        );
    }

    #[test]
    fn exception_names_and_retryable() {
        assert_eq!(exception_code_name(0x02).0, "illegal data address");
        assert!(!exception_code_name(0x02).1);
        assert!(exception_code_name(0x06).1);
        assert!(exception_code_name(0x7F).1);
    }

    // ------------------------------------------------------------ 写帧

    #[test]
    fn tcp_write_single_coil_request_structure() {
        // 1!coil:1 -> offset 0，ON = 0xFF00。
        let frame = build_tcp_write_request(9, 1, FC_WRITE_SINGLE_COIL, 0, &[0xFF, 0x00]);
        assert_eq!(frame.len(), 12);
        assert_eq!(&frame[0..2], &[0x00, 0x09]); // transaction
        assert_eq!(&frame[2..4], &[0x00, 0x00]); // protocol
        assert_eq!(&frame[4..6], &[0x00, 0x06]); // length = unit+fc+addr+value
        assert_eq!(frame[6], 1); // unit
        assert_eq!(frame[7], 0x05); // FC05
        assert_eq!(&frame[8..10], &[0x00, 0x00]); // offset
        assert_eq!(&frame[10..12], &[0xFF, 0x00]); // ON
    }

    #[test]
    fn tcp_write_multiple_registers_length_covers_payload() {
        // FC16 写 2 个寄存器：payload = qty(2) + byte count(1) + 4 数据 = 7 字节。
        let payload = [0x00, 0x02, 0x04, 0x12, 0x34, 0x56, 0x78];
        let frame = build_tcp_write_request(1, 2, FC_WRITE_MULTIPLE_REGISTERS, 5, &payload);
        assert_eq!(frame.len(), 6 + 4 + 7);
        // length = unit(1) + fc(1) + addr(2) + payload(7) = 11。
        assert_eq!(&frame[4..6], &[0x00, 0x0B]);
        assert_eq!(frame[6], 2);
        assert_eq!(frame[7], 0x10);
        assert_eq!(&frame[8..10], &[0x00, 0x05]);
        assert_eq!(&frame[10..], &payload);
    }

    #[test]
    fn rtu_write_request_crc_round_trip() {
        let frame = build_rtu_write_request(1, FC_WRITE_SINGLE_REGISTER, 3, &[0x13, 0x88]);
        assert_eq!(frame.len(), 8);
        assert!(crate::crc::verify(&frame));
        assert_eq!(frame[1], 0x06);
        assert_eq!(&frame[2..4], &[0x00, 0x03]);
        assert_eq!(&frame[4..6], &[0x13, 0x88]);
    }

    #[test]
    fn write_functions_are_write_and_expected_echo_len() {
        for fc in [
            FC_WRITE_SINGLE_COIL,
            FC_WRITE_SINGLE_REGISTER,
            FC_WRITE_MULTIPLE_COILS,
            FC_WRITE_MULTIPLE_REGISTERS,
        ] {
            assert!(is_write_function(fc), "fc {fc:#04x}");
            // 写响应统一回显 4 字节，数量参数不参与。
            assert_eq!(expected_data_len(fc, 0), 4);
            assert_eq!(expected_data_len(fc, 123), 4);
        }
        assert!(!is_write_function(FC_READ_HOLDING_REGISTERS));
        assert_eq!(expected_data_len(0x63, 1), 0);
    }

    #[test]
    fn rtu_write_response_total_len_matches() {
        // 正常写响应：unit + fc + 4 回显 + CRC = 8 字节。
        assert_eq!(
            rtu_response_total_len(&[0x01, 0x06], FC_WRITE_SINGLE_REGISTER, 0).unwrap(),
            8
        );
        // 异常响应恒 5 字节。
        assert_eq!(
            rtu_response_total_len(&[0x01, 0x86], FC_WRITE_SINGLE_REGISTER, 0).unwrap(),
            5
        );
    }

    #[test]
    fn write_echo_validation() {
        let payload = [0xFF, 0x00];
        assert_eq!(
            validate_write_echo(&[0x00, 0x01, 0xFF, 0x00], 1, &payload),
            Ok(())
        );
        // 地址回显不符。
        assert!(validate_write_echo(&[0x00, 0x02, 0xFF, 0x00], 1, &payload).is_err());
        // 值回显不符。
        assert!(validate_write_echo(&[0x00, 0x01, 0x00, 0x00], 1, &payload).is_err());
        // 长度不符。
        assert!(validate_write_echo(&[0x00, 0x01, 0xFF], 1, &payload).is_err());
        // FC15/16 回显数量。
        let payload = [0x00, 0x03, 0x01, 0b101];
        assert_eq!(
            validate_write_echo(&[0x00, 0x00, 0x00, 0x03], 0, &payload),
            Ok(())
        );
    }
}
