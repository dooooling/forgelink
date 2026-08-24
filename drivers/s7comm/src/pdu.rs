//! S7 通信层 PDU 编解码（纯函数，无 I/O）。
//!
//! 帧结构（出处：Wireshark `s7comm` dissector 文档）：
//!
//! - Job（请求）：`[0x32][rosctr=0x01][red-id 2][pdu-ref 2]
//!   [参数长 2][数据长 2]`，头共 10 字节；
//! - Ack_Data（响应）：`[0x32][rosctr=0x03][red-id 2][pdu-ref 2]
//!   [参数长 2][数据长 2][error-class][error-code]`，头共 12 字节；
//! - Setup Communication：function 0xF0，参数区 `[F0][00][max-pdu 请求
//!   BE u16][max-pdu 应答 BE u16]`，无数据区；
//! - Read Var (0x04)：参数区 `[04][项数][每项 Any 指针 12B]`；响应参数
//!   区 `[04][项数][00 00]`，数据区每项 `[return][transport-size]
//!   [length BE u16][载荷(奇数补偶对齐)]`；
//! - Write Var (0x05)：参数区同读；请求数据区每项 `[00 占位][ts]
//!   [length][载荷(+pad)]`；响应参数区 `[05][项数]`，数据区每项 1 字节
//!   return code。
//!
//! S7 Any 指针：`[spec=0x12][len=0x0A][syntax=0x10][transport-size]
//! [length BE u16（元素数：BIT 计位、BYTE 计字节、WORD 计字、DWORD 计
//! 双字）][db BE u16][area][地址域 3B 大端 = byte_offset<<3 | bit]`。

use crate::address::{Area, S7Address, S7Type};
use crate::error::S7Error;

/// S7 协议魔数。
pub const PROTOCOL_ID: u8 = 0x32;
/// ROSCTR：Job（请求）。
pub const ROSCTR_JOB: u8 = 0x01;
/// ROSCTR：Ack_Data（带数据确认响应）。
pub const ROSCTR_ACK_DATA: u8 = 0x03;
/// function：Setup Communication。
pub const FUNCTION_SETUP: u8 = 0xF0;
/// function：Read Var。
pub const FUNCTION_READ: u8 = 0x04;
/// function：Write Var。
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

/// 存储区代码：过程映像输入。
pub const AREA_INPUT: u8 = 0x81;
/// 存储区代码：过程映像输出。
pub const AREA_OUTPUT: u8 = 0x82;
/// 存储区代码：Marker。
pub const AREA_MARKER: u8 = 0x83;
/// 存储区代码：Data Block。
pub const AREA_DB: u8 = 0x84;

/// Job 头长度（含协议魔数）。
const JOB_HEADER_LEN: usize = 10;
/// Ack_Data 头长度（多 error_class/error_code 两字节）。
const ACK_HEADER_LEN: usize = 12;
/// 单个 Any 指针的线格式长度。
const ANY_ITEM_LEN: usize = 12;

/// Setup Communication 提议值（snap7/西门子工具常用 500；实际取协商小者）。
pub const PROPOSED_PDU_SIZE: u16 = 500;

/// 一条 S7 Any 指针寻址项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnyItem {
    /// transport size（[`TS_BIT`] 等）。
    pub transport_size: u8,
    /// 元素数（BIT 计位、BYTE 计字节、WORD 计字、DWORD 计双字）。
    pub length: u16,
    /// area 代码。
    pub area_code: u8,
    /// DB 号（非 DB 区为 0）。
    pub db: u16,
    /// 字节偏移。
    pub byte_offset: u32,
    /// 位号。
    pub bit: u8,
}

impl AnyItem {
    /// 由解析后的地址构造单元素读取/写入项。
    ///
    /// 宽度映射：Bit→TS_BIT(length=1 位)、Byte→TS_BYTE(1)、Word→
    /// TS_WORD(1 字)、Dword→TS_DWORD(1 双字)。
    #[must_use]
    pub fn from_address(addr: &S7Address) -> Self {
        let (transport_size, length) = match addr.ty {
            S7Type::Bit => (TS_BIT, 1u16),
            S7Type::Byte => (TS_BYTE, 1),
            S7Type::Word => (TS_WORD, 1),
            S7Type::Dword => (TS_DWORD, 1),
        };
        Self {
            transport_size,
            length,
            area_code: addr.area.code(),
            db: addr.db,
            byte_offset: addr.byte,
            bit: addr.bit,
        }
    }

    /// 编码为线格式 12 字节。
    #[must_use]
    pub fn encode(&self) -> [u8; ANY_ITEM_LEN] {
        let addr = self.byte_offset << 3 | u32::from(self.bit);
        [
            0x12,
            0x0A,
            0x10,
            self.transport_size,
            (self.length >> 8) as u8,
            self.length as u8,
            (self.db >> 8) as u8,
            self.db as u8,
            self.area_code,
            (addr >> 16) as u8,
            (addr >> 8) as u8,
            addr as u8,
        ]
    }

    /// 该项覆盖的字节数（位以 1 字节承载）。
    #[must_use]
    pub fn width_bytes(&self) -> usize {
        match self.transport_size {
            TS_BIT => 1,
            TS_BYTE => usize::from(self.length),
            TS_WORD => usize::from(self.length) * 2,
            TS_DWORD => usize::from(self.length) * 4,
            _ => 0,
        }
    }
}

/// 组装一条完整 Job PDU（S7 层，不含 TPKT/COTP 包裹）。
fn build_job(pdu_ref: u16, param: &[u8], data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(JOB_HEADER_LEN + param.len() + data.len());
    out.push(PROTOCOL_ID);
    out.push(ROSCTR_JOB);
    out.extend_from_slice(&[0x00, 0x00]); // redundant identification
    out.extend_from_slice(&pdu_ref.to_be_bytes());
    out.extend_from_slice(&(param.len() as u16).to_be_bytes());
    out.extend_from_slice(&(data.len() as u16).to_be_bytes());
    out.extend_from_slice(param);
    out.extend_from_slice(data);
    out
}

/// 构造 Setup Communication 请求。
#[must_use]
pub fn build_setup(pdu_ref: u16, proposed_pdu: u16) -> Vec<u8> {
    let param = [
        FUNCTION_SETUP,
        0x00,
        (proposed_pdu >> 8) as u8,
        proposed_pdu as u8,
        (proposed_pdu >> 8) as u8,
        proposed_pdu as u8,
    ];
    build_job(pdu_ref, &param, &[])
}

/// 解析 Setup 应答，返回 PLC 提供的 max pdu。
///
/// # Errors
///
/// 功能码不符或参数区截断返回 `unexpected_function_code` /
/// `invalid_response`。
pub fn parse_setup_ack(param: &[u8]) -> Result<u16, S7Error> {
    // 真实 PLC 应答参数区布局（Wireshark packet-s7comm.c
    // s7comm_decode_pdu_setup_communication，见 docs/s7comm-protocol-reference.md）：
    // [F0][reserved][max-AMQ-calling u16][max-AMQ-called u16][PDU length u16]
    // ——共 8 字节，PDU 长度位于 [6..8]。部分精简固件只回 4 字节
    // [F0][00][PDU u16]（无 AMQ 字段），此时 PDU 位于 [2..4]。
    // 两种布局以参数区长度区分。
    if param.len() >= 8 && param[0] == FUNCTION_SETUP {
        return Ok(u16::from_be_bytes([param[6], param[7]]));
    }
    if param.len() >= 4 && param[0] == FUNCTION_SETUP {
        return Ok(u16::from_be_bytes([param[2], param[3]]));
    }
    Err(S7Error::unexpected_function_code(
        FUNCTION_SETUP,
        param.first().copied().unwrap_or(0),
    ))
}

/// 构造 Read Var 请求。
#[must_use]
pub fn build_read(pdu_ref: u16, items: &[AnyItem]) -> Vec<u8> {
    let mut param = Vec::with_capacity(2 + items.len() * ANY_ITEM_LEN);
    param.push(FUNCTION_READ);
    param.push(items.len() as u8);
    for item in items {
        param.extend_from_slice(&item.encode());
    }
    build_job(pdu_ref, &param, &[])
}

/// 构造 Write Var 请求（数据区逐项 `[00 占位][ts][length][载荷+pad]`，
/// 载荷奇数字节补偶对齐填充）。
#[must_use]
pub fn build_write(pdu_ref: u16, items: &[(AnyItem, Vec<u8>)]) -> Vec<u8> {
    let mut param = Vec::with_capacity(2 + items.len() * ANY_ITEM_LEN);
    param.push(FUNCTION_WRITE);
    param.push(items.len() as u8);
    for (item, _) in items {
        param.extend_from_slice(&item.encode());
    }
    let mut data = Vec::new();
    for (item, payload) in items {
        data.push(0x00); // return code 占位
        data.push(item.transport_size);
        data.extend_from_slice(&(item.length).to_be_bytes());
        data.extend_from_slice(payload);
        if payload.len() % 2 == 1 {
            data.push(0x00); // 偶对齐填充
        }
    }
    build_job(pdu_ref, &param, &data)
}

/// 解析后的 Ack 头与分区视图。
#[derive(Debug)]
pub struct AckParts<'a> {
    /// pdu-ref 回显（驱动据此匹配请求）。
    pub pdu_ref: u16,
    /// 参数区。
    pub param: &'a [u8],
    /// 数据区。
    pub data: &'a [u8],
}

/// 解析 Ack_Data 头并校验整体结果。
///
/// # Errors
///
/// 协议魔数/ROSCTR 不符、头截断、error_class != 0（整体否定）时分别
/// 返回 `invalid_response` / `s7_error_response`。
pub fn parse_ack(pdu: &[u8]) -> Result<AckParts<'_>, S7Error> {
    if pdu.len() < ACK_HEADER_LEN || pdu[0] != PROTOCOL_ID {
        return Err(S7Error::invalid_response(format!(
            "Ack 头非法或截断：len={} b0={:#04x}",
            pdu.len(),
            pdu.first().copied().unwrap_or(0)
        )));
    }
    if pdu[1] != ROSCTR_ACK_DATA {
        return Err(S7Error::unexpected_function_code(ROSCTR_ACK_DATA, pdu[1]));
    }
    // 头布局：[0x32][rosctr][red-id 2][pdu-ref 2][参数长 2][数据长 2]
    // [error-class][error-code]。
    let pdu_ref = u16::from_be_bytes([pdu[4], pdu[5]]);
    let param_len = u16::from_be_bytes([pdu[6], pdu[7]]) as usize;
    let data_len = u16::from_be_bytes([pdu[8], pdu[9]]) as usize;
    // TPKT 分帧已保证整帧精确：分区声明之和必须与实际长度恰好一致
    // （多字节/少字节都是失步，整体失败丢会话）。
    let declared_total = ACK_HEADER_LEN + param_len + data_len;
    if pdu.len() < declared_total {
        return Err(S7Error::invalid_response(format!(
            "Ack 截断：声明参数 {param_len} + 数据 {data_len}，总长 {}",
            pdu.len()
        )));
    }
    if pdu.len() > declared_total {
        return Err(S7Error::invalid_response(format!(
            "Ack 帧有多余尾字节：声明参数 {param_len} + 数据 {data_len}，总长 {}",
            pdu.len()
        )));
    }
    if pdu[10] != 0 || pdu[11] != 0 {
        return Err(S7Error::s7_error_response(pdu[10], pdu[11]));
    }
    Ok(AckParts {
        pdu_ref,
        param: &pdu[ACK_HEADER_LEN..ACK_HEADER_LEN + param_len],
        data: &pdu[ACK_HEADER_LEN + param_len..ACK_HEADER_LEN + param_len + data_len],
    })
}

/// 读响应单项：返回码、回显 transport size 与载荷切片。
#[derive(Debug)]
pub struct ReadItemResult<'a> {
    /// item 返回码（非 [`RC_SUCCESS`] 时载荷无意义）。
    pub return_code: u8,
    /// PLC 回显的 transport size（驱动按语法宽度校验一致性）。
    pub transport_size: u8,
    /// 载荷（已剥对齐填充）。
    pub payload: &'a [u8],
}

/// 解析 Read Var 响应。
///
/// # Errors
///
/// 功能码不符、item 数不符、数据区未恰好闭合时返回 `invalid_response`
/// （失步丢会话）；单项 return code 非 0xFF 不在此处理——由调用方逐项
/// 映射（会话保留）。
pub fn parse_read_response<'a>(
    param: &[u8],
    data: &'a [u8],
    expected_items: usize,
) -> Result<Vec<ReadItemResult<'a>>, S7Error> {
    check_read_header(param, expected_items)?;
    let mut results = Vec::with_capacity(expected_items);
    let mut cursor = 0usize;
    for _ in 0..expected_items {
        if cursor + 4 > data.len() {
            return Err(S7Error::invalid_response("读响应数据区截断".to_owned()));
        }
        let return_code = data[cursor];
        let transport_size = data[cursor + 1];
        let declared = u16::from_be_bytes([data[cursor + 2], data[cursor + 3]]);
        // length 单位随 transport size 变化（Wireshark packet-s7comm.c
        // s7comm_decode_response_read_data 权威：BIT 与 WORD/INT 类按
        // 位计（向上取整到字节），BYTE/DWORD/DINT/REAL 等按字节计）。
        // 真机实测佐证：读 DBW 应答 ts=0x04 length=0x0010（16 位=2 字节）。
        let payload_len = match transport_size {
            TS_BIT => usize::from(declared).div_ceil(8),
            TS_WORD | 0x05 /* INT */ => usize::from(declared).div_ceil(8),
            _ => usize::from(declared),
        };
        cursor += 4;
        if cursor + payload_len > data.len() {
            return Err(S7Error::invalid_response("读响应载荷越界".to_owned()));
        }
        results.push(ReadItemResult {
            return_code,
            transport_size,
            payload: &data[cursor..cursor + payload_len],
        });
        cursor += payload_len;
        // 偶对齐填充只存在于项与项之间（末项 pad 可省略——PLC 实现不一，
        // 以"恰好闭合"为准：pad 字节计入则消费之，不计则自然闭合）。
        if cursor < data.len() && payload_len % 2 == 1 {
            cursor += 1;
        }
    }
    if cursor != data.len() {
        return Err(S7Error::invalid_response(format!(
            "读响应数据区未恰好闭合：消费 {cursor}，实际 {}",
            data.len()
        )));
    }
    Ok(results)
}

fn check_read_header(param: &[u8], expected_items: usize) -> Result<(), S7Error> {
    // 真实应答参数区只有 2 字节：[0x04][item count]（Wireshark
    // packet-s7comm.c ACK_DATA 分支——item count 紧跟功能码，无
    // reserved 字节）。原 `< 4` 条件是真机调试发现的缺陷：会把合法
    // 2 字节参数区误判为功能码错位（报错打印的首字节恰为 0x04）。
    if param.len() < 2 || param[0] != FUNCTION_READ {
        return Err(S7Error::unexpected_function_code(
            FUNCTION_READ,
            param.first().copied().unwrap_or(0),
        ));
    }
    if usize::from(param[1]) != expected_items {
        return Err(S7Error::invalid_response(format!(
            "读响应 item count 不符：期望 {expected_items}，收到 {}",
            param[1]
        )));
    }
    Ok(())
}

/// 解析 Write Var 响应：逐项 return code（数据区每项 1 字节）。
///
/// # Errors
///
/// 功能码不符、item 数不符、数据区长度不符时返回 `invalid_response`。
pub fn parse_write_response(
    param: &[u8],
    data: &[u8],
    expected_items: usize,
) -> Result<Vec<u8>, S7Error> {
    if param.len() < 2 || param[0] != FUNCTION_WRITE {
        return Err(S7Error::unexpected_function_code(
            FUNCTION_WRITE,
            param.first().copied().unwrap_or(0),
        ));
    }
    if usize::from(param[1]) != expected_items {
        return Err(S7Error::invalid_response(format!(
            "写响应 item count 不符：期望 {expected_items}，收到 {}",
            param[1]
        )));
    }
    if data.len() != expected_items {
        return Err(S7Error::invalid_response(format!(
            "写响应数据区长度不符：期望 {expected_items} 字节，收到 {}",
            data.len()
        )));
    }
    Ok(data.to_vec())
}

/// 读合并的数据区字节预算：单条 Read Var PDU 内所有项的**载荷+响应头**
/// 总和不得超过该值（§23 合并受协商 PDU 上限约束）。
///
/// 预算 = 协商 pdu − Job 头(10) − 响应参数区(4) − 每项响应头(4)
/// − 安全余量 16（不同固件的头部差异缓冲）。负值钳为 0。
#[must_use]
pub fn read_data_budget(negotiated_pdu: u16, item_count: usize) -> usize {
    negotiated_pdu.saturating_sub((JOB_HEADER_LEN + 4 + 4 * item_count + 16) as u16) as usize
}

/// 写合并的数据区字节预算：请求数据区（含每项 4 字节头与 pad）不得超过
/// 该值。预算 = 协商 pdu − Job 头(10) − 参数区(2 + 12×项数) − 安全余量 16。
#[must_use]
pub fn write_data_budget(negotiated_pdu: u16, item_count: usize) -> usize {
    negotiated_pdu.saturating_sub((JOB_HEADER_LEN + 2 + ANY_ITEM_LEN * item_count + 16) as u16)
        as usize
}

/// `Area → area 代码` 的集中映射（供 batch 分组键使用）。
#[must_use]
pub fn area_code_of(area: Area) -> u8 {
    match area {
        Area::Db => AREA_DB,
        Area::Marker => AREA_MARKER,
        Area::Input => AREA_INPUT,
        Area::Output => AREA_OUTPUT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db_addr(db: u16, byte: u32) -> S7Address {
        S7Address {
            area: Area::Db,
            db,
            byte,
            bit: 0,
            ty: S7Type::Word,
        }
    }

    #[test]
    fn any_item_encodes_wire_format() {
        let item = AnyItem::from_address(&db_addr(10, 20));
        assert_eq!(item.transport_size, TS_WORD);
        assert_eq!(item.length, 1, "单元素读取");
        let bytes = item.encode();
        // 地址域 = byte<<3：20<<3 = 0xA0（3 字节大端低字节）。
        assert_eq!(
            bytes,
            [
                0x12, 0x0A, 0x10, TS_WORD, 0x00, 0x01, 0x00, 0x0A, AREA_DB, 0x00, 0x00, 0xA0
            ]
        );
    }

    #[test]
    fn setup_round_trip_negotiates_offered() {
        let req = build_setup(0x0001, PROPOSED_PDU_SIZE);
        assert_eq!(&req[..4], &[PROTOCOL_ID, ROSCTR_JOB, 0x00, 0x00]);
        assert_eq!(u16::from_be_bytes([req[6], req[7]]), 6, "Setup 参数区长");
        assert_eq!(u16::from_be_bytes([req[8], req[9]]), 0, "Setup 无数据区");
        assert_eq!(&req[10..], &[FUNCTION_SETUP, 0x00, 0x01, 0xF4, 0x01, 0xF4]);

        // 应答：真实布局 8 字节参数区（Wireshark 权威）：
        // [F0][00][AMQ-calling][AMQ-called][PDU u16=480]。
        let ack = vec![
            PROTOCOL_ID,
            ROSCTR_ACK_DATA,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
            0x08,
            0x00,
            0x00,
            0x00,
            0x00,
            FUNCTION_SETUP,
            0x00,
            0x00,
            0x0A, // AMQ calling = 10
            0x00,
            0x05, // AMQ called = 5
            0x01,
            0xE0, // PDU length = 480
        ];
        let ack_parts = parse_ack(&ack).unwrap();
        assert_eq!(ack_parts.pdu_ref, 1);
        assert_eq!(parse_setup_ack(ack_parts.param).unwrap(), 480);

        // 精简固件短布局：4 字节参数区 [F0][00][PDU u16]。
        let short = vec![
            PROTOCOL_ID,
            ROSCTR_ACK_DATA,
            0x00,
            0x00,
            0x00,
            0x02,
            0x00,
            0x04,
            0x00,
            0x00,
            0x00,
            0x00,
            FUNCTION_SETUP,
            0x00,
            0x01,
            0xE0,
        ];
        let parts = parse_ack(&short).unwrap();
        assert_eq!(parse_setup_ack(parts.param).unwrap(), 480);
    }

    #[test]
    fn read_response_parses_items_and_requires_exact_closure() {
        // 手工构造完整 Ack_Data 帧：头 12B + 参数区 2B（真实布局
        // [0x04][item count]）+ 数据区。WORD 的 length 域按位计
        // （真机实测：读 DBW 应答 ts=0x04 length=0x0010=16 位=2 字节）。
        let mut pdu = vec![PROTOCOL_ID, ROSCTR_ACK_DATA, 0, 0, 0, 7, 0, 2, 0, 12, 0, 0];
        pdu.extend_from_slice(&[FUNCTION_READ, 2]);
        pdu.extend_from_slice(&[RC_SUCCESS, TS_WORD, 0x00, 0x10, 0x12, 0x34]);
        pdu.extend_from_slice(&[RC_SUCCESS, TS_BYTE, 0x00, 0x01, 0x56, 0x00]);
        let parts = parse_ack(&pdu).unwrap();
        let items = parse_read_response(parts.param, parts.data, 2).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].payload, &[0x12, 0x34]);
        assert_eq!(items[1].payload, &[0x56]);
        assert_eq!(items[1].return_code, RC_SUCCESS);

        // 多余尾字节 → 分区声明不闭合 → parse_ack 整体拒绝。
        let mut bad = pdu.clone();
        bad.push(0xAA);
        assert!(parse_ack(&bad).is_err());

        // item count 不符 → 整体失败。
        let parts = parse_ack(&pdu).unwrap();
        assert!(parse_read_response(parts.param, parts.data, 3).is_err());
    }

    #[test]
    fn write_response_maps_per_item_codes() {
        let mut pdu = vec![PROTOCOL_ID, ROSCTR_ACK_DATA, 0, 0, 0, 9, 0, 2, 0, 2, 0, 0];
        pdu.extend_from_slice(&[FUNCTION_WRITE, 2]);
        pdu.extend_from_slice(&[RC_SUCCESS, 0x07]);
        let parts = parse_ack(&pdu).unwrap();
        let codes = parse_write_response(parts.param, parts.data, 2).unwrap();
        assert_eq!(codes, vec![RC_SUCCESS, 0x07]);
    }

    #[test]
    fn budgets_leave_headroom_for_overhead() {
        let b1 = read_data_budget(240, 1);
        let b5 = read_data_budget(240, 5);
        assert!(b1 > b5, "项数越多预算越小");
        assert_eq!(b1, 240 - 10 - 4 - 4 - 16);
        let w = write_data_budget(240, 2);
        assert_eq!(w, 240 - 10 - 2 - 24 - 16);
    }
}
