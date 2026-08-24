//! 批量读写规划（§23 精神：排序 → 同软元件连续区间合并 → 少量帧）。
//!
//! # 合并规则
//!
//! - 分组键 `(软元件种类, 单位)`：跨软元件不合并；
//! - 组内按起始编号升序；读侧允许跳洞拼接（缺口 ≤ `max_merge_gap_points`
//!   时并入同一游程——吸收多字宽伪空洞与稀疏点表小间隙）；写侧必须
//!   **精确相邻**才合并（不覆盖未请求地址），重叠拆分按升序执行获得
//!   确定的后写覆盖（镜像 modbus/S7 写规则）；
//! - 分块上限：单次访问点数（配置静态值——MC 无 PDU 协商步），item
//!   整体不跨块。
//!
//! 与 ether-ip 的差异：MC 是数字寄存器寻址，「区间」概念存在——与
//! modbus 同构而非符号标签的子服务打包。

use driver_sdk::DriverReadItem;
use observation_model::DataType;

use crate::address::{DeviceKind, parse};
use crate::error::McError;

/// 读计划中的一个原始请求项。
#[derive(Debug, Clone)]
pub struct PlannedItem {
    /// 上层关联 ID。
    pub item_id: u64,
    /// 期望类型（解码解释用；None 位=Bool、字=U16）。
    pub expected: Option<DataType>,
    /// 在计划数据区内的点偏移。
    pub offset_in_points: usize,
}
/// 一条批量读请求的执行计划。
#[derive(Debug)]
pub struct ReadPlan {
    /// 软元件种类。
    pub kind: DeviceKind,
    /// 起始编号。
    pub start_number: u32,
    /// 访问点数（含中间未请求的点）。
    pub points: u16,
    /// 计划内各项（按起点升序）。
    pub items: Vec<(PlannedItem, u32)>,
}

/// 读批量规划：解析 → 分组 → 排序 → 游程合并（允许小跳洞）→ 上限切块。
///
/// # Errors
/// 读规划解析后的中间条目：`(软元件, 起始编号, 访问点数, (item_id, 期望类型))`。
type ReadEntry = (DeviceKind, u32, usize, (u64, Option<DataType>));
/// 写规划编码后的中间条目：`(起始编号, 点数, item_id, 载荷)`。
type WriteEntry = (u32, usize, u64, Vec<u8>);
/// 编码失败预填结果。
pub type FailedWrite = (u64, McError);

///
/// 任一地址解析失败返回 `invalid_address`（整体失败）。
pub fn plan_read_batch(
    items: &[DriverReadItem],
    max_word_points: u16,
    max_bit_points: u16,
    max_merge_gap: u32,
) -> Result<Vec<ReadPlan>, McError> {
    // 每项解析为 (kind, start_number, points, item)。
    let mut entries: Vec<ReadEntry> = Vec::with_capacity(items.len());
    for item in items {
        let addr = parse(&item.address)
            .map_err(|e| McError::invalid_address(format!("{}: {e}", item.address)))?;
        let width = crate::decode::word_layout(item.expected_type.as_ref())
            .map_or(1, |(points, _, _)| points);
        let points = if addr.kind.is_bit() { 1 } else { width };
        entries.push((
            addr.kind,
            addr.number,
            points,
            (item.id, item.expected_type.clone()),
        ));
    }

    // 按 (kind 单位区分) 分组保确定性；位/字同种软元件单位一致故按 kind 即可。
    let mut groups: std::collections::BTreeMap<DeviceKind, Vec<_>> = Default::default();
    for e in entries {
        groups.entry(e.0).or_default().push(e);
    }

    let mut plans = Vec::new();
    for (kind, group) in groups {
        let unit_limit = if kind.is_bit() {
            usize::from(max_bit_points)
        } else {
            usize::from(max_word_points)
        };
        let mut run = group;
        run.sort_by_key(|(_, start, ..)| *start);

        let mut chunk: Vec<ReadEntry> = Vec::new();
        for entry in run {
            if !chunk.is_empty() {
                let first_start = chunk[0].1;
                let end = chunk_end(&chunk);
                let (next_start, next_width) = (entry.1, entry.2);
                // 跳洞合并条件：下一项起点在当前终点 + gap 之内。
                let gap_ok = next_start <= end + max_merge_gap;
                let span = usize::try_from(next_start + next_width as u32 - first_start)
                    .unwrap_or(usize::MAX);
                if !gap_ok || span > unit_limit || chunk.len() + 1 > MAX_ITEMS_PER_PLAN {
                    flush_read(&mut chunk, &mut plans);
                }
            }
            chunk.push(entry);
        }
        flush_read(&mut chunk, &mut plans);
    }
    Ok(plans)
}

/// 单计划内的最大 item 数（防御性上限，避免极端点表生成巨型计划结构）。
const MAX_ITEMS_PER_PLAN: usize = 960;

/// 计划数据区终点编号（末项编号 + 宽度，含洞）。
fn chunk_end(chunk: &[ReadEntry]) -> u32 {
    chunk
        .iter()
        .map(|(_, start, width, _)| start + *width as u32)
        .max()
        .unwrap_or(0)
}

fn flush_read(chunk: &mut Vec<ReadEntry>, plans: &mut Vec<ReadPlan>) {
    if chunk.is_empty() {
        return;
    }
    let kind = chunk[0].0;
    let start_number = chunk[0].1;
    let end = chunk_end(chunk);
    let points = end - start_number;
    let items = chunk
        .drain(..)
        .map(|(_, start, _width, (id, expected))| {
            (
                PlannedItem {
                    item_id: id,
                    expected,
                    offset_in_points: (start - start_number) as usize,
                },
                start,
            )
        })
        .collect();
    plans.push(ReadPlan {
        kind,
        start_number,
        points: points as u16,
        items,
    });
}

/// 批量写入请求项（ABI 层转换产物）。
#[derive(Debug)]
pub struct WriteRequest {
    /// 上层关联 ID。
    pub id: u64,
    /// 软元件地址。
    pub address: String,
    /// 写入值（已由 ABI Tag 解码）。
    pub value: observation_model::RawValue,
}

/// 一个批量写请求的执行计划：同软元件精确相邻的值合并为一次访问
/// （payload 为各值编码按序拼接；位串 LSB-first 打包）。
#[derive(Debug)]
pub struct WritePlan {
    /// 软元件种类。
    pub kind: DeviceKind,
    /// 起始编号。
    pub start_number: u32,
    /// 写入点数。
    pub points: u16,
    /// 数据字节（字 LE 或位串）。
    pub data: Vec<u8>,
    /// 参与合并的上层 item_id（按写入顺序）。
    pub item_ids: Vec<u64>,
}

/// 写批量规划：编码 → 同软元件精确相邻合并 → 上限分块。
///
/// 返回 `(计划列表, 编码失败项的预填失败结果)`：编码失败（类型不兼容）
/// 在规划期剔除并预填 `invalid_type`，不发出必然失败的请求。
///
/// # Errors
///
/// 地址解析失败返回 `invalid_address`（整体失败）。只读软元件（X/SM）
/// 在此显式拒绝。
pub fn plan_write_batch(
    items: &[WriteRequest],
    max_word_points: u16,
    max_bit_points: u16,
) -> Result<(Vec<WritePlan>, Vec<FailedWrite>), McError> {
    // 编码并分组：(kind, number, width_in_points, id, data)。
    let mut groups: std::collections::BTreeMap<DeviceKind, Vec<WriteEntry>> = Default::default();
    let mut failed: Vec<(u64, McError)> = Vec::new();
    for req in items {
        let addr = parse(&req.address)
            .map_err(|e| McError::invalid_address(format!("{}: {e}", req.address)))?;
        if !addr.kind.writable() {
            failed.push((
                req.id,
                McError::invalid_address(format!(
                    "{} 为过程输入/系统区（只读），禁止写入",
                    req.address
                )),
            ));
            continue;
        }
        match crate::encode::encode_write(addr.kind, None, &req.value) {
            Ok(enc) => groups.entry(addr.kind).or_default().push((
                addr.number,
                enc.points as usize,
                req.id,
                enc.data,
            )),
            Err(e) => failed.push((req.id, e)),
        }
    }

    let mut plans = Vec::new();
    for (kind, entries) in groups {
        let unit_limit = if kind.is_bit() {
            usize::from(max_bit_points)
        } else {
            usize::from(max_word_points)
        };
        let mut sorted = entries;
        sorted.sort_by_key(|(start, ..)| *start);
        // 精确相邻游程切分：任何空洞/重叠断开（升序保证后写覆盖确定）。
        let mut runs: Vec<Vec<WriteEntry>> = Vec::new();
        for entry in sorted {
            match runs.last_mut() {
                Some(run) => {
                    let (last_start, last_width, _, _) = *run.last().expect("游程非空");
                    if last_start + last_width as u32 == entry.0 {
                        run.push(entry);
                        continue;
                    }
                    runs.push(vec![entry]);
                }
                None => runs.push(vec![entry]),
            }
        }
        for mut run in runs {
            let mut current: Vec<WriteEntry> = Vec::new();
            for piece in run.drain(..) {
                let first_start = current.first().map_or(piece.0, |(s, ..)| *s);
                let span = piece.0 + piece.1 as u32 - first_start;
                if !current.is_empty()
                    && (usize::try_from(span).unwrap_or(usize::MAX) > unit_limit
                        || current.len() + 1 > MAX_ITEMS_PER_PLAN)
                {
                    flush_write(&mut current, &mut plans, kind);
                }
                current.push(piece);
            }
            flush_write(&mut current, &mut plans, kind);
        }
    }
    Ok((plans, failed))
}

fn flush_write(chunk: &mut Vec<WriteEntry>, plans: &mut Vec<WritePlan>, kind: DeviceKind) {
    if chunk.is_empty() {
        return;
    }
    let start_number = chunk[0].0;
    let mut data = Vec::new();
    let mut item_ids = Vec::with_capacity(chunk.len());
    if kind.is_bit() {
        // 位串打包：每字节 8 点 LSB 在前。精确相邻项按序重排为紧凑位串
        //（encode 对单点项产出 1 字节 0/1，此处收集为位向量再压缩）。
        let total: usize = chunk.iter().map(|(_, w, ..)| *w).sum();
        let mut bits = vec![false; total];
        let mut at = 0usize;
        for (_, width, _, piece_data) in chunk.iter() {
            for i in 0..*width {
                bits[at] = piece_data
                    .get(i / 8)
                    .is_some_and(|b| (b >> (i % 8)) & 1 != 0);
                at += 1;
            }
        }
        for byte_chunk in bits.chunks(8) {
            let mut b = 0u8;
            for (i, bit) in byte_chunk.iter().enumerate() {
                if *bit {
                    b |= 1 << i;
                }
            }
            data.push(b);
        }
        item_ids.extend(chunk.drain(..).map(|(_, _, id, _)| id));
        plans.push(WritePlan {
            kind,
            start_number,
            points: total as u16,
            data,
            item_ids,
        });
        return;
    }
    // 字路径：各值编码按序拼接。
    let mut points = 0usize;
    for (_, width, id, piece) in chunk.drain(..) {
        data.extend_from_slice(&piece);
        points += width;
        item_ids.push(id);
    }
    plans.push(WritePlan {
        kind,
        start_number,
        points: points as u16,
        data,
        item_ids,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use observation_model::RawValue;

    fn read_item(id: u64, address: &str) -> DriverReadItem {
        DriverReadItem {
            id,
            address: address.to_owned(),
            expected_type: None,
        }
    }

    #[test]
    fn merges_adjacent_reads_into_single_plan() {
        let items = vec![
            read_item(1, "D200"),
            read_item(2, "D201"),
            read_item(3, "D202"),
        ];
        let plans = plan_read_batch(&items, 960, 720, 8).unwrap();
        assert_eq!(plans.len(), 1, "连续 D200..202 必须合并为一帧");
        assert_eq!((plans[0].start_number, plans[0].points), (200, 3));
        assert_eq!(plans[0].items.len(), 3);
        assert_eq!(plans[0].items[0].0.offset_in_points, 0);
        assert_eq!(plans[0].items[2].0.offset_in_points, 2);
    }

    #[test]
    fn small_gap_merged_large_gap_split() {
        // D100 与 D104：缺口 3 ≤ gap 8 → 合并（span 覆盖到 105）。
        let near = vec![read_item(1, "D100"), read_item(2, "D104")];
        let plans = plan_read_batch(&near, 960, 720, 8).unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].points, 5);

        // D100 与 D120：缺口 19 > gap → 拆两帧。
        let far = vec![read_item(1, "D100"), read_item(2, "D120")];
        let plans = plan_read_batch(&far, 960, 720, 8).unwrap();
        assert_eq!(plans.len(), 2, "大空洞必须拆分");
    }

    #[test]
    fn cross_kind_never_merged() {
        let items = vec![read_item(1, "D200"), read_item(2, "M200")];
        let plans = plan_read_batch(&items, 960, 720, 8).unwrap();
        assert_eq!(plans.len(), 2, "位/字软元件不得合并");
    }

    #[test]
    fn writes_exact_adjacent_only_and_splits_on_hole() {
        let make = |id: u64, addr: &str, v: i64| WriteRequest {
            id,
            address: addr.to_owned(),
            value: RawValue::I64(v),
        };
        // 精确相邻 D0+D1 → 一帧。
        let adjacent = vec![make(1, "D0", 10), make(2, "D1", 20)];
        let (plans, failed) = plan_write_batch(&adjacent, 960, 720).unwrap();
        assert!(failed.is_empty());
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].points, 2);
        assert_eq!(plans[0].data, vec![10, 0, 20, 0]);

        // 空洞 D0 与 D2 → 两帧，互不波及。
        let holed = vec![make(1, "D0", 10), make(2, "D2", 30)];
        let (plans, failed) = plan_write_batch(&holed, 960, 720).unwrap();
        assert!(failed.is_empty());
        assert_eq!(plans.len(), 2, "空洞必须拆分");
        assert_eq!(plans[0].points, 1);
        assert_eq!(plans[1].points, 1);

        // 只读软元件 X → 规划期剔除。
        let ro = vec![make(1, "X20", 1)];
        let (_, failed) = plan_write_batch(&ro, 960, 720).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].1.code, "invalid_address");
    }
}
