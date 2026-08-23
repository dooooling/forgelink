//! 批量读写规划（§23：排序 → 同区字节区间合并 → 少量 PDU）。
//!
//! # 合并规则
//!
//! - 分组键 `(area, db, 语法)`：跨区/跨 DB 不合并；**位项独立分组**
//!   （BIT 与字节类 transport size 的 length 单位不同，混并徒增错位
//!   风险）；写侧不同语法宽度也不合并；
//! - 组内按起始地址升序；读侧允许跳过中间空洞（数据区 span 覆盖到末项
//!   终点，响应中未请求的字节忽略）；**写侧必须精确相邻才合并**（不覆盖
//!   未请求的地址），空洞/重叠一律拆分，升序排列保证确定的后写覆盖顺序；
//! - 分块受双重上限：`max_items_per_pdu`（配置）与协商 PDU 预算
//!   （[`crate::pdu::read_data_budget`] / [`write_data_budget`]）；
//!   单个 item 不跨块（保 item 完整，与 modbus 同策略）。
//!
//! 与 modbus 的顺序差异：modbus 规划不依赖连接；S7 的分块预算取决于
//! Setup 协商出的 `negotiated_pdu`，因此**先握手后规划**。

use std::collections::BTreeMap;

use driver_sdk::DriverReadItem;
use observation_model::DataType;

use crate::address::{S7Type, parse};
use crate::error::S7Error;
use crate::pdu::{area_code_of, read_data_budget, write_data_budget};

/// 读计划中的一个原始请求项。
#[derive(Debug, Clone)]
pub struct PlannedItem {
    /// 上层关联 ID。
    pub item_id: u64,
    /// 期望类型（解码解释用；None 按语法默认无符号）。
    pub expected_type: Option<DataType>,
    /// 在计划数据区内的字节偏移。
    pub offset_in_data: usize,
    /// 语法宽度。
    pub ty: S7Type,
    /// 位号（仅 `S7Type::Bit` 有效；flush 阶段由数据区内偏移换算）。
    pub bit: u8,
}

/// 一条 Read Var PDU 的执行计划。
#[derive(Debug)]
pub struct ReadPlan {
    /// area 代码。
    pub area_code: u8,
    /// DB 号（非 DB 区为 0）。
    pub db: u16,
    /// 数据区起始字节偏移。
    pub start_byte: u32,
    /// 数据区字节数（span，含中间未请求的洞）。
    pub data_len: usize,
    /// 计划内各项。
    pub items: Vec<PlannedItem>,
}

impl ReadPlan {
    /// 构造该计划的 Any 指针项（单条 PDU 单项：合并已折算进数据区
    /// 跨度；位计划恒单点——BIT 区间语义不允许跨洞，见 [`plan_read_batch`]）。
    #[must_use]
    pub fn any_item(&self) -> crate::pdu::AnyItem {
        let first = &self.items[0];
        if matches!(first.ty, S7Type::Bit) {
            // 位计划：数据区恒 1 字节，位号取自该项。
            return crate::pdu::AnyItem {
                transport_size: crate::pdu::TS_BIT,
                length: 1,
                area_code: self.area_code,
                db: self.db,
                byte_offset: self.start_byte,
                bit: first.bit,
            };
        }
        let (transport_size, unit) = match first.ty {
            S7Type::Byte => (crate::pdu::TS_BYTE, 1usize),
            S7Type::Word => (crate::pdu::TS_WORD, 2),
            _ => (crate::pdu::TS_DWORD, 4),
        };
        crate::pdu::AnyItem {
            transport_size,
            length: (self.data_len / unit) as u16,
            area_code: self.area_code,
            db: self.db,
            byte_offset: self.start_byte,
            bit: 0,
        }
    }
}

/// 读批量规划：解析 → 分组 → 排序 → 按预算分块。
///
/// # Errors
///
/// 任一地址解析失败返回 `invalid_address`（整体失败——上层配置错误应
/// 显式暴露，不得静默丢弃部分设备点）。
pub fn plan_read_batch(
    items: &[DriverReadItem],
    negotiated_pdu: u16,
    max_items_per_pdu: usize,
) -> Result<Vec<ReadPlan>, S7Error> {
    // 解析全部地址并按分组键聚合；组内按位寻址键（byte<<3|bit）升序。
    let mut groups: BTreeMap<(u8, u16, bool), Vec<(u32, PlannedItem)>> = BTreeMap::new();
    for item in items {
        let addr = parse(&item.address)
            .map_err(|e| S7Error::invalid_address(format!("{}: {e}", item.address)))?;
        let key = (
            area_code_of(addr.area),
            addr.db,
            matches!(addr.ty, S7Type::Bit),
        );
        let sort_key = addr.byte * 8 + u32::from(addr.bit);
        groups.entry(key).or_default().push((
            sort_key,
            PlannedItem {
                item_id: item.id,
                expected_type: item.expected_type.clone(),
                offset_in_data: 0,
                ty: addr.ty,
                bit: addr.bit,
            },
        ));
    }

    let mut plans = Vec::new();
    for ((area_code, db, is_bit), mut entries) in groups {
        entries.sort_by_key(|(key, _)| *key);
        // 位项按**精确相邻**（key 差 1）切游程：BIT 区间语义是连续位，
        // 跨洞合并会读到错误位号——与字节类的"允许跳洞"不同。
        let streams: Vec<Vec<(u32, PlannedItem)>> = if is_bit {
            let mut runs: Vec<Vec<(u32, PlannedItem)>> = Vec::new();
            for entry in entries {
                match runs.last_mut() {
                    Some(run) if run.last().is_some_and(|(k, _)| *k + 1 == entry.0) => {
                        run.push(entry);
                    }
                    _ => runs.push(vec![entry]),
                }
            }
            runs
        } else {
            vec![entries]
        };
        for mut stream in streams {
            let mut chunk: Vec<(u32, PlannedItem)> = Vec::new();
            while !stream.is_empty() {
                let entry = stream.remove(0);
                if !chunk.is_empty() {
                    let first_key = chunk[0].0;
                    let next_end = entry.0 / 8 + entry.1.ty.width_bytes();
                    let span = (next_end - first_key / 8) as usize;
                    let count = chunk.len() + 1;
                    if span + 4 * count > read_data_budget(negotiated_pdu, count)
                        || count > max_items_per_pdu
                    {
                        flush_read(&mut chunk, &mut plans, area_code, db);
                    }
                }
                chunk.push(entry);
            }
            flush_read(&mut chunk, &mut plans, area_code, db);
        }
    }
    Ok(plans)
}

/// 计划数据区终点字节（末项起始 + 宽度，含洞）。
fn data_end(chunk: &[(u32, PlannedItem)]) -> u32 {
    chunk
        .iter()
        .map(|(key, item)| key / 8 + item.ty.width_bytes())
        .max()
        .unwrap_or(0)
}

fn flush_read(
    chunk: &mut Vec<(u32, PlannedItem)>,
    plans: &mut Vec<ReadPlan>,
    area_code: u8,
    db: u16,
) {
    if chunk.is_empty() {
        return;
    }
    let first_key = chunk[0].0;
    let start_byte = first_key / 8;
    let data_len = (data_end(chunk) - start_byte) as usize;
    for (key, item) in chunk.iter_mut() {
        item.offset_in_data = ((*key - first_key) / 8) as usize;
    }
    plans.push(ReadPlan {
        area_code,
        db,
        start_byte,
        data_len,
        items: chunk.drain(..).map(|(_, item)| item).collect(),
    });
}

/// 批量写入请求项（ABI 层转换产物）。
#[derive(Debug)]
pub struct WriteRequest {
    /// 上层关联 ID。
    pub id: u64,
    /// 地址字符串。
    pub address: String,
    /// 写入值（已由 ABI Tag 解码）。
    pub value: observation_model::RawValue,
}

/// 一个 Write Var PDU 的执行计划：同语法、精确相邻的值合并为单条
/// Any 指针（payload 为各值编码按序拼接）。
#[derive(Debug)]
pub struct WritePlan {
    /// area 代码。
    pub area_code: u8,
    /// DB 号（非 DB 区为 0）。
    pub db: u16,
    /// 语法宽度（决定 transport size 与 length 单位）。
    pub ty: S7Type,
    /// 起始字节偏移。
    pub start_byte: u32,
    /// 拼接后的载荷（不含对齐填充）。
    pub payload: Vec<u8>,
    /// 参与合并的上层 item_id（按写入顺序）。
    pub item_ids: Vec<u64>,
}

impl WritePlan {
    /// 构造该计划的 Any 指针项（length 按 transport size 单位计数）。
    #[must_use]
    pub fn any_item(&self) -> crate::pdu::AnyItem {
        let (transport_size, units) = match self.ty {
            S7Type::Bit => (crate::pdu::TS_BIT, 1u16),
            S7Type::Byte => (crate::pdu::TS_BYTE, self.payload.len() as u16),
            S7Type::Word => (crate::pdu::TS_WORD, (self.payload.len() / 2) as u16),
            S7Type::Dword => (crate::pdu::TS_DWORD, (self.payload.len() / 4) as u16),
        };
        crate::pdu::AnyItem {
            transport_size,
            length: units,
            area_code: self.area_code,
            db: self.db,
            byte_offset: self.start_byte,
            bit: 0,
        }
    }
}

/// 写批量规划：编码 → 同语法精确相邻合并 → 按预算分块。
///
/// 返回 `(计划列表, 编码/只读拒绝项的预填失败结果)`：这类单项失败在
/// 规划期剔除并预填结果（镜像 modbus——不发出必然失败的请求）。
///
/// # Errors
///
/// 地址解析失败返回 `invalid_address`（整体失败）。I 区为过程映像输入
/// （现场驱动），写入显式拒绝。
/// 写规划中间聚合条目：`(起始字节, item_id, 值载荷)`。
type EncodedEntry = (u32, u64, Vec<u8>);
/// 编码失败预填结果：`(item_id, 错误)`。
type FailedWrite = (u64, S7Error);

pub fn plan_write_batch(
    items: &[WriteRequest],
    negotiated_pdu: u16,
    max_items_per_pdu: usize,
) -> Result<(Vec<WritePlan>, Vec<FailedWrite>), S7Error> {
    let mut groups: BTreeMap<(u8, u16, S7Type), Vec<EncodedEntry>> = BTreeMap::new();
    let mut failed: Vec<FailedWrite> = Vec::new();
    for req in items {
        let addr = parse(&req.address)
            .map_err(|e| S7Error::invalid_address(format!("{}: {e}", req.address)))?;
        if !addr.writable() {
            failed.push((
                req.id,
                S7Error::invalid_address(format!(
                    "{} 为过程映像输入（I 区只读），禁止写入",
                    req.address
                )),
            ));
            continue;
        }
        match crate::encode::encode_write(addr.ty, &req.value) {
            Ok(enc) => {
                groups
                    .entry((area_code_of(addr.area), addr.db, addr.ty))
                    .or_default()
                    .push((addr.byte, req.id, enc.payload));
            }
            Err(e) => failed.push((req.id, e)),
        }
    }

    let mut plans = Vec::new();
    for ((area_code, db, ty), mut entries) in groups {
        entries.sort_by_key(|(start, ..)| *start);
        // 精确相邻游程切分：任何空洞/重叠断开（升序保证后写覆盖确定）。
        let mut runs: Vec<Vec<(u32, u64, Vec<u8>)>> = Vec::new();
        for entry in entries {
            match runs.last_mut() {
                Some(run) => {
                    let (last_start, _, last_payload) = run.last().expect("游程非空");
                    if *last_start + last_payload.len() as u32 == entry.0 {
                        run.push(entry);
                        continue;
                    }
                    runs.push(vec![entry]);
                }
                None => runs.push(vec![entry]),
            }
        }
        // 游程按预算聚合为计划（预算以"值个数"计，pad 不计入载荷）。
        for run in runs {
            let mut current: Vec<(u32, u64, Vec<u8>)> = Vec::new();
            for piece in run {
                let count = current.len() + 1;
                let bytes: usize =
                    current.iter().map(|(_, _, p)| p.len()).sum::<usize>() + piece.2.len();
                if !current.is_empty()
                    && (count > max_items_per_pdu
                        || bytes + 4 * count > write_data_budget(negotiated_pdu, count))
                {
                    flush_write(&mut current, &mut plans, area_code, db, ty);
                }
                current.push(piece);
            }
            flush_write(&mut current, &mut plans, area_code, db, ty);
        }
    }
    Ok((plans, failed))
}

fn flush_write(
    chunk: &mut Vec<(u32, u64, Vec<u8>)>,
    plans: &mut Vec<WritePlan>,
    area_code: u8,
    db: u16,
    ty: S7Type,
) {
    if chunk.is_empty() {
        return;
    }
    let start_byte = chunk[0].0;
    let mut payload = Vec::new();
    let mut item_ids = Vec::with_capacity(chunk.len());
    for (_, id, piece) in chunk.drain(..) {
        payload.extend_from_slice(&piece);
        item_ids.push(id);
    }
    plans.push(WritePlan {
        area_code,
        db,
        ty,
        start_byte,
        payload,
        item_ids,
    });
}
