//! 批量读取/写入规划：按从站分组、按数据段合并连续地址、按协议上限拆分。
//!
//! 原则（§22、§39 原则 4：批量优化属于 Driver）：
//!
//! - **不跨设备/从站混淆**：不同 `unit_id` 的 item 划分为独立请求计划；
//! - **连续地址合并**：同段内地址连续的 item 合并为一个请求区间
//!   （读侧允许跳过无 item 的中间地址；写侧必须精确相邻——写入会改写
//!   设备状态，禁止把未请求的中间地址一并覆盖）；
//! - **协议上限拆分**：寄存器读每帧 ≤ 125 寄存器、位读每帧 ≤ 2000 位；
//!   写侧为 FC16 ≤ 123 寄存器、FC15 ≤ 1968 位；
//! - **写侧确定性**：地址重叠（前项宽度越过后续项起点）的写项拆分为
//!   独立请求并按地址升序执行，保证确定的"后写覆盖"顺序。

use std::collections::BTreeMap;

use driver_sdk::DriverReadItem;
use observation_model::RawValue;

use crate::address::{ModbusAddress, RegisterKind};
use crate::encode::{EncodedWrite, coil_payload, encode_write_value, pack_bits};
use crate::error::ModbusError;
use crate::frame::{
    FC_WRITE_MULTIPLE_COILS, FC_WRITE_MULTIPLE_REGISTERS, FC_WRITE_SINGLE_COIL,
    FC_WRITE_SINGLE_REGISTER, MAX_BITS_PER_REQUEST, MAX_REGISTERS_PER_REQUEST,
    MAX_WRITE_BITS_PER_REQUEST, MAX_WRITE_REGISTERS_PER_REQUEST,
};

/// 分组条目：item_id + 解析地址 + 期望类型。
type GroupEntry = (u64, ModbusAddress, Option<observation_model::DataType>);

/// 一个读请求计划（一次 Modbus 读帧）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlan {
    /// 目标从站号（地址未显式指定时取连接配置默认值）。
    pub unit_id: u8,
    /// 数据段（决定功能码 FC01~FC04）。
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
    /// 请求项 ID（`DriverReadItem.id`，结果按此回传）。
    pub item_id: u64,
    /// 相对本帧数据起始的偏移（位偏移或寄存器偏移）。
    pub offset_in_frame: u16,
    /// 期望类型（None 表示未指定，解码按段默认：位段 Bool、寄存器段 U16）。
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
        // 多寄存器 item 占多个单元：偏移 + 宽度必须落在协议 16 位偏移范围内
        // （如 holding:105536 读 F64 会越过 65535）。
        let width = item_width(address.kind, item.expected_type.as_ref()) as u32;
        let end = address.offset() as u32 + width;
        if end > u16::MAX as u32 + 1 {
            return Err(crate::error::ModbusError::invalid_address(format!(
                "地址 {} 连同类型宽度 {width} 超出协议偏移上限 65535",
                address.canonical()
            )));
        }
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

// ---------------------------------------------------------------- 写入规划

/// 写入请求项（FFI 层按 ABI Tag 解码后的内部表示）。
///
/// 与 [`driver_sdk::DriverWriteItem`] 的差异：写入宽度语义由 `value_type`
/// Tag 承载（§17.2），进程内 `RawValue` 只有 64 位标量变体，无法区分
/// U16/U32/U64 目标宽度，故内部传递必须保留 Tag。
#[derive(Debug, Clone, PartialEq)]
pub struct WriteRequest {
    /// 请求批次内唯一 ID，结果通过 `RawWriteResult.item_id` 关联。
    pub id: u64,
    pub address: String,
    /// ABI v1 值类型 Tag（§17.2）；窄 Tag 精确决定协议宽度。
    pub value_type: u32,
    /// 已按 Tag 解码的标量值。
    pub value: RawValue,
}

/// 一个写请求计划（一次 Modbus 写帧）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePlan {
    /// 目标从站号。
    pub unit_id: u8,
    /// 数据段（只可能是 Coil 或 HoldingRegister）。
    pub kind: RegisterKind,
    /// 写功能码（FC05/FC06/FC15/FC16）。
    pub function: u8,
    /// 协议偏移起点（0-based）。
    pub start_offset: u16,
    /// 本帧写入数量（位数或寄存器数）。
    pub quantity: u16,
    /// 功能码后随载荷（不含地址）：FC05/06 为 2 字节值；
    /// FC15/16 为数量(2) + 字节计数(1) + 数据。
    pub payload: Vec<u8>,
    /// 本帧覆盖的 item id（按地址号升序；结果按此回传）。
    pub item_ids: Vec<u64>,
}

type WriteGroupEntry = (u64, ModbusAddress, EncodedWrite);

/// 解析 + 规划一批写 item。
///
/// # Errors
///
/// - 任一 item 地址非法或指向**不可写区域**（discrete/input）：整体失败
///   （`invalid_address`——与读侧一致，寻址类配置错误重试无意义）；
/// - 地址连同类型宽度越过协议 16 位偏移上限：整体失败。
///
/// 值编码失败（Bool 写寄存器等类型错误）**不**整体失败：返回单项错误结果，
/// 其余 item 照常规划执行（镜像读侧"解码失败逐项标记"的语义）。
///
/// 返回 `(请求计划, 预填的单项错误结果)`。
pub fn plan_write_batch(
    items: &[WriteRequest],
    default_unit_id: u8,
    word_order: crate::config::WordOrder,
) -> Result<(Vec<WritePlan>, Vec<driver_sdk::RawWriteResult>), ModbusError> {
    let mut groups: BTreeMap<(u8, RegisterKind), Vec<WriteGroupEntry>> = BTreeMap::new();
    let mut encode_errors = Vec::new();
    for item in items {
        let address = crate::address::parse_address(&item.address, default_unit_id)
            .map_err(|e| ModbusError::invalid_address(e.to_string()))?;
        // 不可写区域必须显式拒绝（discrete/input 只读），不得静默成功。
        if !address.kind.writable() {
            return Err(ModbusError::invalid_address(format!(
                "{}（{}）为只读数据段，不可写入",
                address.canonical(),
                address.kind.name()
            )));
        }
        let encoded =
            match encode_write_value(address.kind, item.value_type, &item.value, word_order) {
                Ok(encoded) => encoded,
                Err(e) => {
                    // 类型错误属于单项问题：逐项报错并从批次中剔除。
                    encode_errors.push(driver_sdk::RawWriteResult {
                        item_id: item.id,
                        success: false,
                        protocol_code: None,
                        error: Some(ModbusError::invalid_type(e.message).into_info()),
                    });
                    continue;
                }
            };
        // 偏移 + 宽度必须落在协议 16 位偏移范围内（如 holding:105536 写 U32）。
        let end = address.offset() as u32 + encoded.units() as u32;
        if end > u16::MAX as u32 + 1 {
            return Err(ModbusError::invalid_address(format!(
                "地址 {} 连同写入宽度 {} 超出协议偏移上限 65535",
                address.canonical(),
                encoded.units()
            )));
        }
        groups
            .entry((address.unit_id, address.kind))
            .or_default()
            .push((item.id, address, encoded));
    }

    let mut plans = Vec::new();
    for ((unit_id, kind), mut group) in groups {
        group.sort_by_key(|(_, addr, _)| addr.address);
        let limit = match kind {
            RegisterKind::Coil => MAX_WRITE_BITS_PER_REQUEST,
            RegisterKind::HoldingRegister => MAX_WRITE_REGISTERS_PER_REQUEST,
            // 上面已拒绝不可写段，防御性兜底。
            RegisterKind::DiscreteInput | RegisterKind::InputRegister => continue,
        };
        // 精确相邻才合并：下一项起点 == 前一项起点 + 前项宽度。
        // 重叠（前项多寄存器越过后续项起点）同样拆分，拆分后的请求按地址
        // 升序串行执行，保证确定的覆盖顺序。
        let mut run: Vec<WriteGroupEntry> = Vec::new();
        for entry in group {
            let flush = run
                .last()
                .map(|(_, prev, prev_encoded)| {
                    entry.1.offset() != prev.offset() + prev_encoded.units()
                })
                .unwrap_or(false);
            if flush {
                emit_write_plan(&mut plans, unit_id, kind, limit, &run);
                run.clear();
            }
            run.push(entry);
        }
        emit_write_plan(&mut plans, unit_id, kind, limit, &run);
    }
    Ok((plans, encode_errors))
}

/// 把一组精确相邻的写条目输出为（可能拆分的）写计划。
///
/// 拆分按 item 粒度累计宽度，保证单个 item 不被协议上限切断；
/// 单 item 且单单元的计划降级为 FC05/FC06 单写（协议惯例：单值用单写）。
fn emit_write_plan(
    plans: &mut Vec<WritePlan>,
    unit_id: u8,
    kind: RegisterKind,
    limit: u16,
    run: &[WriteGroupEntry],
) {
    let mut idx = 0;
    while idx < run.len() {
        let chunk_start = run[idx].1.offset();
        let mut chunk: Vec<WriteGroupEntry> = Vec::new();
        let mut used: u32 = 0;
        while idx < run.len() {
            let units = run[idx].2.units() as u32;
            if used + units > limit as u32 {
                break;
            }
            used += units;
            chunk.push(run[idx].clone());
            idx += 1;
        }
        debug_assert!(!chunk.is_empty(), "chunk 至少包含一个 item");
        let quantity = used as u16;
        let (function, payload) = build_write_payload(kind, &chunk, quantity);
        plans.push(WritePlan {
            unit_id,
            kind,
            function,
            start_offset: chunk_start,
            quantity,
            payload,
            item_ids: chunk.iter().map(|(id, _, _)| *id).collect(),
        });
    }
}

/// 组装功能码与载荷：单 item 单单元降级 FC05/FC06，其余 FC15/FC16。
fn build_write_payload(
    kind: RegisterKind,
    chunk: &[WriteGroupEntry],
    quantity: u16,
) -> (u8, Vec<u8>) {
    match kind {
        RegisterKind::Coil => {
            let bits: Vec<bool> = chunk
                .iter()
                .map(|(_, _, encoded)| match encoded {
                    EncodedWrite::Coil(bit) => *bit,
                    EncodedWrite::Registers(_) => unreachable!("线圈组只含位编码"),
                })
                .collect();
            if chunk.len() == 1 {
                (FC_WRITE_SINGLE_COIL, coil_payload(bits[0]).to_vec())
            } else {
                let data = pack_bits(&bits);
                let mut payload = Vec::with_capacity(3 + data.len());
                payload.extend_from_slice(&quantity.to_be_bytes());
                payload.push(data.len() as u8); // byte count
                payload.extend_from_slice(&data);
                (FC_WRITE_MULTIPLE_COILS, payload)
            }
        }
        RegisterKind::HoldingRegister => {
            let single = chunk.len() == 1 && chunk[0].2.units() == 1;
            if single {
                let EncodedWrite::Registers(data) = &chunk[0].2 else {
                    unreachable!("保持寄存器组只含寄存器编码");
                };
                (FC_WRITE_SINGLE_REGISTER, data.clone())
            } else {
                let mut data = Vec::with_capacity(quantity as usize * 2);
                for (_, _, encoded) in chunk {
                    let EncodedWrite::Registers(bytes) = encoded else {
                        unreachable!("保持寄存器组只含寄存器编码");
                    };
                    data.extend_from_slice(bytes);
                }
                let mut payload = Vec::with_capacity(3 + data.len());
                payload.extend_from_slice(&quantity.to_be_bytes());
                payload.push(data.len() as u8); // byte count
                payload.extend_from_slice(&data);
                (FC_WRITE_MULTIPLE_REGISTERS, payload)
            }
        }
        // plan_write_batch 已拒绝不可写段，此处不可达。
        RegisterKind::DiscreteInput | RegisterKind::InputRegister => unreachable!("只读段不可写"),
    }
}

#[cfg(test)]
mod tests {
    use observation_model::DataType;

    use super::*;
    use crate::config::WordOrder;

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
    fn rejects_address_with_width_beyond_protocol_offset() {
        // 最大偏移 65535（holding:105536）+ U32（2 寄存器）越过 16 位偏移上限。
        let err = plan_batch(&[item(1, "holding:105536", Some(DataType::U32))], 1).unwrap_err();
        assert_eq!(err.code, "invalid_address");
        // 单个 U16（1 寄存器）在边界上合法。
        let plans = plan_batch(&[item(1, "holding:105536", Some(DataType::U16))], 1).unwrap();
        assert_eq!(plans[0].start_offset, 65_535);
        assert_eq!(plans[0].quantity, 1);
    }

    #[test]
    fn default_unit_id_applies_to_bare_addresses() {
        let plans = plan_batch(&[item(1, "40001", None)], 7).unwrap();
        assert_eq!(plans[0].unit_id, 7);
    }

    // ------------------------------------------------------------ 写入规划

    fn write_item(id: u64, address: &str, value: RawValue) -> WriteRequest {
        WriteRequest {
            id,
            address: address.to_owned(),
            value_type: match value {
                RawValue::Bool(_) => 1, // TypeTag::Bool
                RawValue::I64(_) => 5,  // TypeTag::I64（载体 Tag，按值收窄）
                RawValue::U64(_) => 9,  // TypeTag::U64（载体 Tag，按值收窄）
                RawValue::F64(_) => 11, // TypeTag::F64
                _ => panic!("测试只使用标量值"),
            },
            value,
        }
    }

    #[test]
    fn write_plan_single_coil_uses_fc05() {
        let (plans, errors) = plan_write_batch(
            &[write_item(1, "1!coil:1", RawValue::Bool(true))],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert!(errors.is_empty());
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].function, 0x05);
        assert_eq!(plans[0].start_offset, 0);
        assert_eq!(plans[0].quantity, 1);
        assert_eq!(plans[0].payload, vec![0xFF, 0x00]);
        assert_eq!(plans[0].item_ids, vec![1]);
    }

    #[test]
    fn write_plan_single_register_uses_fc06() {
        let (plans, _) = plan_write_batch(
            &[write_item(1, "1!40001", RawValue::U64(5000))],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].function, 0x06);
        assert_eq!(plans[0].quantity, 1);
        assert_eq!(plans[0].payload, vec![0x13, 0x88]);
    }

    #[test]
    fn write_plan_multi_register_item_uses_fc16() {
        // 载体 U64 值 70000 收窄为 2 寄存器：单 item 也必须用 FC16（FC06 只写 1 个）。
        let (plans, _) = plan_write_batch(
            &[write_item(1, "1!40001", RawValue::U64(70_000))],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].function, 0x10);
        assert_eq!(plans[0].quantity, 2);
        assert_eq!(
            plans[0].payload,
            vec![0x00, 0x02, 0x04, 0x00, 0x01, 0x11, 0x70]
        );
    }

    #[test]
    fn write_plan_merges_consecutive_coils_into_fc15() {
        let (plans, _) = plan_write_batch(
            &[
                write_item(1, "1!coil:1", RawValue::Bool(true)),
                write_item(2, "1!coil:2", RawValue::Bool(false)),
                write_item(3, "1!coil:3", RawValue::Bool(true)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].function, 0x0F);
        assert_eq!(plans[0].quantity, 3);
        // qty=3, byte count=1, 位流 LSB 优先 = 0b101。
        assert_eq!(plans[0].payload, vec![0x00, 0x03, 0x01, 0b101]);
        assert_eq!(plans[0].item_ids, vec![1, 2, 3]);
    }

    #[test]
    fn write_plan_merges_consecutive_registers_into_fc16() {
        let (plans, _) = plan_write_batch(
            &[
                write_item(1, "1!40001", RawValue::U64(5000)),
                write_item(2, "1!40002", RawValue::I64(-10)),
                write_item(3, "1!40003", RawValue::U64(1)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].function, 0x10);
        assert_eq!(plans[0].quantity, 3);
        assert_eq!(
            plans[0].payload,
            vec![0x00, 0x03, 0x06, 0x13, 0x88, 0xFF, 0xF6, 0x00, 0x01]
        );
    }

    #[test]
    fn write_plan_splits_non_consecutive() {
        let (plans, _) = plan_write_batch(
            &[
                write_item(1, "1!40001", RawValue::U64(1)),
                write_item(2, "1!40003", RawValue::U64(2)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].start_offset, 0);
        assert_eq!(plans[0].quantity, 1);
        assert_eq!(plans[1].start_offset, 2);
        // 中间空隙不得并入写区间（写入会覆盖未请求的地址）。
        assert_eq!(plans[1].quantity, 1);
    }

    #[test]
    fn write_plan_splits_overlapping_items_by_address_order() {
        // 40001 写 U32（占 40001..40002）与 40002 单寄存器重叠：
        // 拆成两个请求并按地址升序执行，后写（40002）确定覆盖。
        let (plans, _) = plan_write_batch(
            &[
                write_item(1, "1!40001", RawValue::U64(70_000)),
                write_item(2, "1!40002", RawValue::U64(0xAAAA)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].start_offset, 0);
        assert_eq!(plans[0].quantity, 2);
        assert_eq!(plans[1].start_offset, 1);
        assert_eq!(plans[1].quantity, 1);
        assert_eq!(plans[0].item_ids, vec![1]);
        assert_eq!(plans[1].item_ids, vec![2]);
    }

    #[test]
    fn write_plan_splits_register_run_at_123_limit() {
        let mut items = Vec::new();
        for i in 0..130u32 {
            items.push(write_item(
                u64::from(i) + 1,
                &format!("holding:{}", 40001 + i),
                RawValue::U64(u64::from(i) + 1),
            ));
        }
        let (plans, _) = plan_write_batch(&items, 1, WordOrder::Abcd).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].quantity, 123);
        assert_eq!(plans[1].start_offset, 123);
        assert_eq!(plans[1].quantity, 7);
    }

    #[test]
    fn write_plan_splits_bit_run_at_1968_limit() {
        let mut items = Vec::new();
        for i in 0..2000u32 {
            items.push(write_item(
                u64::from(i) + 1,
                &format!("coil:{}", 1 + i),
                RawValue::Bool(true),
            ));
        }
        let (plans, _) = plan_write_batch(&items, 1, WordOrder::Abcd).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].quantity, 1_968);
        assert_eq!(plans[0].payload[2] as usize, 246); // 1968 / 8
        assert_eq!(plans[1].start_offset, 1_968);
        assert_eq!(plans[1].quantity, 32);
    }

    #[test]
    fn write_plan_keeps_wide_item_whole_at_limit() {
        // 第 123 个位置放 U32（2 寄存器）会越过 123 上限：整体移入下一块。
        let mut items = Vec::new();
        for i in 0..122u32 {
            items.push(write_item(
                u64::from(i) + 1,
                &format!("holding:{}", 40001 + i),
                RawValue::U64(1),
            ));
        }
        items.push(write_item(123, "holding:40123", RawValue::U64(70_000)));
        let (plans, _) = plan_write_batch(&items, 1, WordOrder::Abcd).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].quantity, 122);
        assert_eq!(plans[1].start_offset, 122);
        assert_eq!(plans[1].quantity, 2);
        assert_eq!(plans[1].item_ids, vec![123]);
    }

    #[test]
    fn write_plan_no_merge_across_unit() {
        let (plans, _) = plan_write_batch(
            &[
                write_item(1, "1!40001", RawValue::U64(1)),
                write_item(2, "2!40002", RawValue::U64(2)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].unit_id, 1);
        assert_eq!(plans[1].unit_id, 2);
    }

    #[test]
    fn write_plan_rejects_unwritable_segments() {
        for address in ["1!input:30001", "1!discrete:10001", "30001"] {
            let err = plan_write_batch(
                &[write_item(1, address, RawValue::U64(1))],
                1,
                WordOrder::Abcd,
            )
            .unwrap_err();
            assert_eq!(err.code, "invalid_address", "{address}");
            assert!(!err.retryable);
            assert!(err.message.contains("只读"), "{address}");
        }
    }

    #[test]
    fn write_plan_rejects_invalid_address_whole_batch() {
        let err = plan_write_batch(
            &[
                write_item(1, "1!40001", RawValue::U64(1)),
                write_item(2, "holding:x", RawValue::U64(2)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap_err();
        assert_eq!(err.code, "invalid_address");
    }

    #[test]
    fn write_plan_rejects_width_beyond_protocol_offset() {
        let err = plan_write_batch(
            &[write_item(1, "holding:105536", RawValue::U64(70_000))],
            1,
            WordOrder::Abcd,
        )
        .unwrap_err();
        assert_eq!(err.code, "invalid_address");
    }

    #[test]
    fn write_encode_error_yields_per_item_error() {
        // Bool 写保持寄存器：单项 invalid_type，其余 item 照常规划。
        let (plans, errors) = plan_write_batch(
            &[
                write_item(1, "1!40001", RawValue::Bool(true)),
                write_item(2, "1!40002", RawValue::U64(5)),
            ],
            1,
            WordOrder::Abcd,
        )
        .unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].item_id, 1);
        assert!(!errors[0].success);
        assert_eq!(errors[0].error.as_ref().unwrap().code, "invalid_type");
        assert_eq!(errors[0].protocol_code, None);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].item_ids, vec![2]);
        assert_eq!(plans[0].function, 0x06);
    }
}
