//! 批量读取规划：按从站分组、按数据段合并连续地址、按协议上限拆分。
//!
//! 原则（§22、§39 原则 4：批量优化属于 Driver）：
//!
//! - **不跨设备/从站混淆**：不同 `unit_id` 的 item 划分为独立请求计划；
//! - **连续地址合并**：同段内地址连续（差 1）的 item 合并为一个请求区间，
//!   无 item 的中间地址一并读取（一次往返覆盖连续区间）；
//! - **协议上限拆分**：寄存器读每帧 ≤ 125 寄存器、位读每帧 ≤ 2000 位。

use std::collections::BTreeMap;

use driver_sdk::DriverReadItem;

use crate::address::{ModbusAddress, RegisterKind};
use crate::frame::{MAX_BITS_PER_REQUEST, MAX_REGISTERS_PER_REQUEST};

/// 分组条目：item_id + 解析地址 + 期望类型。
type GroupEntry = (u64, ModbusAddress, Option<observation_model::DataType>);

/// 一个读请求计划（一次 Modbus 读帧）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlan {
    pub unit_id: u8,
    pub kind: RegisterKind,
    /// 协议偏移起点（0-based，`40001 -> 0`）。
    pub start_offset: u16,
    /// 本帧读取的数量（寄存器数或位数）。
    pub quantity: u16,
    /// 本帧覆盖的 item（按地址号升序）。
    pub items: Vec<PlannedItem>,
}

/// 计划内单个 item 的定位信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedItem {
    pub item_id: u64,
    /// 相对本帧数据起始的偏移（位偏移或寄存器偏移）。
    pub offset_in_frame: u16,
    pub expected_type: Option<observation_model::DataType>,
    /// 本 item 期望类型占用的单元数（寄存器数或位数，解码用）。
    pub width: u16,
}

/// 解析 + 规划一批 item。
///
/// # Errors
///
/// 任一 item 地址非法即整体失败（地址错误属于配置类错误，重试无意义，
/// 由调用方在 `get_last_error_json` 中上报）。
pub fn plan_batch(
    items: &[DriverReadItem],
    default_unit_id: u8,
) -> Result<Vec<ReadPlan>, crate::error::ModbusError> {
    // 1. 解析地址，按 (unit, kind) 分组。
    let mut groups: BTreeMap<(u8, RegisterKind), Vec<GroupEntry>> = BTreeMap::new();
    for item in items {
        let address = crate::address::parse_address(&item.address, default_unit_id)
            .map_err(|e| crate::error::ModbusError::invalid_address(e.to_string()))?;
        groups
            .entry((address.unit_id, address.kind))
            .or_default()
            .push((item.id, address, item.expected_type.clone()));
    }

    // 2. 每 (unit, kind) 组内按地址排序并合并连续区间，按协议上限拆分。
    let mut plans = Vec::new();
    for ((unit_id, kind), mut group) in groups {
        group.sort_by_key(|(_, addr, _)| addr.address);
        let limit = match kind {
            RegisterKind::Coil | RegisterKind::DiscreteInput => MAX_BITS_PER_REQUEST,
            RegisterKind::HoldingRegister | RegisterKind::InputRegister => {
                MAX_REGISTERS_PER_REQUEST
            }
        };
        let mut run: Vec<GroupEntry> = Vec::new();
        for entry in group {
            let flush = run
                .last()
                .map(|(_, prev, _)| entry.1.address != prev.address + 1)
                .unwrap_or(false);
            if flush {
                emit_plan(&mut plans, unit_id, kind, limit, &run);
                run.clear();
            }
            run.push(entry);
        }
        emit_plan(&mut plans, unit_id, kind, limit, &run);
    }
    Ok(plans)
}

/// 把一组连续地址条目输出为（可能拆分的）请求计划。
///
/// 拆分按 item 粒度累计宽度（`item_width`），保证：
///
/// - 区间终点覆盖最后 item 的完整宽度（如 U32 占 2 寄存器）；
/// - 单个 item 不会被协议上限切断（跨 chunk 的 item 整体移入下一块）。
fn emit_plan(
    plans: &mut Vec<ReadPlan>,
    unit_id: u8,
    kind: RegisterKind,
    limit: u16,
    run: &[GroupEntry],
) {
    let mut idx = 0;
    while idx < run.len() {
        let chunk_start = run[idx].1.offset();
        let mut items = Vec::new();
        let mut used: u32 = 0;
        while idx < run.len() {
            let (item_id, addr, expected_type) = &run[idx];
            let width = item_width(kind, expected_type.as_ref()) as u32;
            if used + width > limit as u32 {
                break;
            }
            items.push(PlannedItem {
                item_id: *item_id,
                offset_in_frame: addr.offset() - chunk_start,
                expected_type: expected_type.clone(),
                width: width as u16,
            });
            used += width;
            idx += 1;
        }
        let last = items.last().expect("chunk 至少包含一个 item");
        let quantity = last.offset_in_frame + last.width;
        plans.push(ReadPlan {
            unit_id,
            kind,
            start_offset: chunk_start,
            quantity,
            items,
        });
    }
}

/// item 期望类型占用的单元数（寄存器或位）。
///
/// 类型未指定时按段默认（位段 1 位、寄存器段 1 寄存器）；不支持的类型按
/// 1 单元参与规划，解码阶段再报单项错误。
fn item_width(kind: RegisterKind, expected_type: Option<&observation_model::DataType>) -> u16 {
    match kind {
        RegisterKind::Coil | RegisterKind::DiscreteInput => 1,
        RegisterKind::HoldingRegister | RegisterKind::InputRegister => match expected_type {
            Some(dt) => crate::decode::register_width(dt).unwrap_or(1),
            None => 1,
        },
    }
}

#[cfg(test)]
mod tests {
    use observation_model::DataType;

    use super::*;

    fn item(id: u64, address: &str, expected_type: Option<DataType>) -> DriverReadItem {
        DriverReadItem {
            id,
            address: address.to_owned(),
            expected_type,
        }
    }

    #[test]
    fn merges_consecutive_addresses() {
        let plans = plan_batch(
            &[
                item(1, "1!40001", Some(DataType::U16)),
                item(2, "1!40002", Some(DataType::U16)),
                item(3, "1!40003", Some(DataType::U16)),
            ],
            1,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].start_offset, 0);
        assert_eq!(plans[0].quantity, 3);
        assert_eq!(plans[0].items.len(), 3);
        assert_eq!(plans[0].items[0].offset_in_frame, 0);
        assert_eq!(plans[0].items[2].offset_in_frame, 2);
    }

    #[test]
    fn plan_end_covers_last_item_width() {
        // 末尾 U32 占 2 寄存器：区间必须覆盖到其第二个寄存器。
        let plans = plan_batch(
            &[
                item(1, "1!40001", Some(DataType::U16)),
                item(2, "1!40002", Some(DataType::U16)),
                item(3, "1!40003", Some(DataType::U32)),
            ],
            1,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].start_offset, 0);
        assert_eq!(plans[0].quantity, 4);
        assert_eq!(plans[0].items[2].offset_in_frame, 2);
        assert_eq!(plans[0].items[2].width, 2);
    }

    #[test]
    fn splits_run_keeping_items_whole() {
        // 125 边界处的 U32（占 2 寄存器）整体移入下一块，不跨 chunk 截断。
        let mut items = Vec::new();
        for i in 0..124u32 {
            items.push(item(i as u64 + 1, &format!("holding:{}", 40001 + i), None));
        }
        items.push(item(125, "holding:40125", Some(DataType::U32)));
        let plans = plan_batch(&items, 1).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].quantity, 124);
        assert_eq!(plans[1].start_offset, 124);
        assert_eq!(plans[1].quantity, 2);
        assert_eq!(plans[1].items.len(), 1);
    }

    #[test]
    fn splits_non_consecutive_runs() {
        let plans = plan_batch(&[item(1, "40001", None), item(2, "40003", None)], 1).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].start_offset, 0);
        assert_eq!(plans[0].quantity, 1);
        assert_eq!(plans[1].start_offset, 2);
        assert_eq!(plans[1].quantity, 1);
    }

    #[test]
    fn groups_by_unit_id_no_cross_device_merge() {
        let plans = plan_batch(
            &[
                item(1, "1!40001", None),
                item(2, "1!40002", None),
                item(3, "2!40002", None),
                item(4, "2!40003", None),
            ],
            1,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].unit_id, 1);
        assert_eq!(plans[0].quantity, 2);
        assert_eq!(plans[1].unit_id, 2);
        assert_eq!(plans[1].quantity, 2);
    }

    #[test]
    fn groups_by_kind_within_unit() {
        let plans = plan_batch(
            &[
                item(1, "1!40001", None),
                item(2, "1!30001", None),
                item(3, "1!coil:1", None),
            ],
            1,
        )
        .unwrap();
        // 组序为 (unit, kind) 字典序（RegisterKind 派生 Ord 按声明顺序：
        // Coil < DiscreteInput < InputRegister < HoldingRegister）。
        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].kind, RegisterKind::Coil);
        assert_eq!(plans[0].items.len(), 1);
        assert_eq!(plans[1].kind, RegisterKind::InputRegister);
        assert_eq!(plans[1].items.len(), 1);
        assert_eq!(plans[2].kind, RegisterKind::HoldingRegister);
        assert_eq!(plans[2].items.len(), 1);
    }

    #[test]
    fn splits_register_run_at_protocol_limit() {
        let mut items = Vec::new();
        for i in 0..260u32 {
            items.push(item(i as u64 + 1, &format!("holding:{}", 40001 + i), None));
        }
        let plans = plan_batch(&items, 1).unwrap();
        // 260 个连续寄存器 → 125 + 125 + 10。
        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].quantity, 125);
        assert_eq!(plans[1].quantity, 125);
        assert_eq!(plans[2].quantity, 10);
        assert_eq!(plans[0].items.len(), 125);
        assert_eq!(plans[2].items.len(), 10);
    }

    #[test]
    fn splits_bit_run_at_protocol_limit() {
        let mut items = Vec::new();
        for i in 0..2_050u32 {
            items.push(item(i as u64 + 1, &format!("coil:{}", 1 + i), None));
        }
        let plans = plan_batch(&items, 1).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].quantity, 2_000);
        assert_eq!(plans[1].quantity, 50);
    }

    #[test]
    fn rejects_invalid_address_whole_batch() {
        let err = plan_batch(&[item(1, "40001", None), item(2, "holding:x", None)], 1).unwrap_err();
        assert_eq!(err.code, "invalid_address");
        assert!(!err.retryable);
    }

    #[test]
    fn default_unit_id_applies_to_bare_addresses() {
        let plans = plan_batch(&[item(1, "40001", None)], 7).unwrap();
        assert_eq!(plans[0].unit_id, 7);
    }
}
