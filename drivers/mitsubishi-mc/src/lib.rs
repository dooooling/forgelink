//! driver-mitsubishi-mc：三菱 MC 协议（3E 帧）驱动（§34.6 V0.3 第二交付）。
//!
//! # 职责边界
//!
//! - **协议表示**：批量读（0x0401 字/0x0402 位）与批量写（0x1401/0x1402），
//!   3E 帧承载（副头+路由区五字段+指令区）；软元件寻址 `D200`/`M100`/
//!   `X20`/`ZR100` 等——编号一律十进制解析，X/Y/W 的 HEX 书写陷阱挡在
//!   驱动内（见 `address` 模块）；T/C 与随机访问推迟 V0.4；
//! - **批量合并**：同软元件连续区间合并——读侧允许小跳洞拼接
//!   （`max_merge_gap_points` 配置化）、写侧精确相邻不覆盖未请求地址
//!   （`batch` 模块）；分块受单次点数上限约束（静态配置，MC 无协商步）；
//! - **原始结果边界**：只返回 `RawReadResult` / `RawWriteResult`（§7.3）；
//!   访问点数由 expected_type 决定（映射表见 `decode`/`encode` 模块文档）；
//! - **粒度差异声明**：MC 批量应答只有一个结束代码，无逐项粒度——非 0
//!   即整计划失败，映射到计划全部 item 后继续后续计划；单项失败仅来自
//!   规划期剔除与解码期错误。
//!
//! 型号差异（缩放、单位、枚举）属于 Profile，本 Driver 不感知。
//!
//! # 插件形态
//!
//! Native Plugin（§26.2）：`cdylib`，唯一入口 `forgelink_driver_entry_v1()`，
//! 返回稳定 C ABI v1 函数表（driver-sdk §17.9）。同步 ABI 下超时由传输层
//! socket 读超时实现，阻塞任务隔离由上层（poll-engine `spawn_blocking`）
//! 负责。
//!
//! # 超时 / 断线重连
//!
//! 断开后下一次请求按配置重试建连（懒建连语义——MC 无握手步）；
//! 响应匹配的三层结构自洽校验与失步防护论证见 `session` 模块文档。
//!
//! # 错误语义（§17.6）
//!
//! 已知限制：control-engine 错误码白名单未含本驱动稳定码
//! （mc_error_response 等），北向控制链路归一为 driver_error——与 V0.2
//! S7 / V0.3 EtherNet/IP 现状一致。

pub mod address;
pub mod batch;
pub mod config;
pub mod decode;
pub mod encode;
pub mod error;
pub mod mc;
pub mod session;

use std::ffi::c_void;
use std::mem::size_of;

use driver_sdk::{ProtocolCapabilities, RawWriteResult};
use observation_model::{DriverErrorInfo, RawReadResult, TimestampNs};

use driver_sdk::abi::envelope::{
    AddressEnvelope, CapabilitiesEnvelope, ErrorEnvelope, ReadEnvelope, WriteEnvelope,
};
use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiReadItem, FfiStr};

use crate::batch::{plan_read_batch, plan_write_batch};
use crate::config::McConfig;
use crate::error::McError;
use crate::session::TcpSession;

// ---------------------------------------------------------------- 驱动实现

/// 单计划执行的应答体（owned）。
struct PlanResponse {
    end_code: u16,
    data: Vec<u8>,
}

/// 驱动句柄状态。
pub struct MitsubishiMcDriver {
    config: McConfig,
    session: Option<TcpSession>,
    last_error: Option<DriverErrorInfo>,
}

impl MitsubishiMcDriver {
    /// 从配置 JSON 创建（不建立连接；首请求懒建连）。
    fn create(config_json: &str) -> Result<Self, McError> {
        let config = config::parse_config(config_json)?;
        Ok(Self {
            session: Some(TcpSession::new(&config)),
            config,
            last_error: None,
        })
    }

    fn ensure_connected(&mut self) -> Result<(), McError> {
        self.session
            .as_mut()
            .expect("会话已创建")
            .ensure_connected()
    }

    fn disconnect(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.disconnect();
        }
    }

    /// 批量读取：规划 → 逐计划请求 → 按 offset 解包组装。
    fn read_batch(
        &mut self,
        items: &[driver_sdk::DriverReadItem],
    ) -> Result<Vec<RawReadResult>, McError> {
        self.ensure_connected()?;
        let plans = plan_read_batch(
            items,
            self.config.max_word_points_per_access,
            self.config.max_bit_points_per_access,
            self.config.max_merge_gap_points,
        )?;
        let mut results = Vec::with_capacity(items.len());
        for plan in &plans {
            // 传输级失败（断线/超时/失步）：整体失败返回（PollDriver 约定
            // §22），不得转成单项错误伪装成成功批次。
            let response = self.execute_read_plan(plan)?;
            if response.end_code != 0 {
                // 结束代码非 0：协议级失败——整计划 item 全部标记后继续。
                let e = McError::mc_error_response(response.end_code);
                for (planned, _) in &plan.items {
                    results.push(read_error_result(planned.item_id, &e, now_ns()));
                }
                continue;
            }
            let is_bit = plan.kind.is_bit();
            for (planned, _start) in &plan.items {
                results.push(assemble_read(planned, is_bit, &response.data));
            }
        }
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    fn execute_read_plan(&mut self, plan: &batch::ReadPlan) -> Result<PlanResponse, McError> {
        let session = self.session.as_mut().expect("会话已创建");
        let request = mc::build_read_request(
            plan.kind,
            plan.start_number,
            plan.points,
            session.monitoring_timer(),
            session.routing(),
        );
        let frame = session.transact(&request)?;
        let (head, body) = mc::parse_response_head(&frame, session.routing())?;
        Ok(PlanResponse {
            end_code: head.end_code,
            data: body[2..].to_vec(), // 剥结束代码
        })
    }

    /// 批量写入：编码规划 → 逐计划请求 → 组装每项结果。
    ///
    /// 编码失败 / 只读软元件在规划期剔除并预填单项结果。
    fn write_batch(
        &mut self,
        items: &[batch::WriteRequest],
    ) -> Result<Vec<RawWriteResult>, McError> {
        self.ensure_connected()?;
        let (plans, mut failed) = plan_write_batch(
            items,
            self.config.max_word_points_per_access,
            self.config.max_bit_points_per_access,
        )?;
        let mut results: Vec<RawWriteResult> = Vec::with_capacity(items.len());
        for (id, e) in failed.drain(..) {
            results.push(write_error_result(id, &e));
        }
        for plan in &plans {
            // 与读路径一致：传输级整体失败；结束代码整计划标记。
            let response = self.execute_write_plan(plan)?;
            if response.end_code != 0 {
                let e = McError::mc_error_response(response.end_code);
                for item_id in &plan.item_ids {
                    results.push(write_error_result(*item_id, &e));
                }
                continue;
            }
            for item_id in &plan.item_ids {
                results.push(RawWriteResult {
                    item_id: *item_id,
                    success: true,
                    protocol_code: Some(0),
                    error: None,
                });
            }
        }
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    fn execute_write_plan(&mut self, plan: &batch::WritePlan) -> Result<PlanResponse, McError> {
        let session = self.session.as_mut().expect("会话已创建");
        let request = mc::build_write_request(
            plan.kind,
            plan.start_number,
            plan.points,
            &plan.data,
            session.monitoring_timer(),
            session.routing(),
        );
        let frame = session.transact(&request)?;
        let (head, body) = mc::parse_response_head(&frame, session.routing())?;
        Ok(PlanResponse {
            end_code: head.end_code,
            data: body[2..].to_vec(),
        })
    }

    /// 校验并规范化地址（§15 `validate_address`）。
    fn validate_address(&mut self, address: &str) -> Result<driver_sdk::AddressMetadata, McError> {
        let parsed = address::parse(address)
            .map_err(|e| McError::invalid_address(format!("{address}: {e}")))?;
        Ok(driver_sdk::AddressMetadata {
            canonical_address: parsed.canonical(),
            raw_type: None,
            readable: true,
            writable: parsed.kind.writable(),
        })
    }
}

/// 组装单项读取结果：从计划数据区按 offset 切片解码。
///
/// 位计划：`data` 为位串（LSB-first），按位偏移取 1 字节（含目标位）；
/// 字计划：按点偏移 ×2 字节切片，宽度由 expected_type 决定。
fn assemble_read(planned: &batch::PlannedItem, is_bit: bool, data: &[u8]) -> RawReadResult {
    let now = now_ns();
    if is_bit {
        // 位串：目标位所在字节 = 位偏移 / 8；decode 只看 LSB。
        let bit_offset = planned.offset_in_points;
        let Some(byte) = data.get(bit_offset / 8).copied() else {
            return read_error_result(
                planned.item_id,
                &McError::invalid_response("位数据区越界".to_owned()),
                now,
            );
        };
        return match decode::decode_read(
            crate::address::DeviceKind::M,
            planned.expected.clone(),
            &[byte],
        ) {
            Ok(value) => RawReadResult {
                item_id: planned.item_id,
                value: Some(value),
                source_timestamp_ns: None,
                received_timestamp_ns: now,
                protocol_quality_code: Some(0),
                error: None,
            },
            Err(e) => read_error_result(planned.item_id, &e, now),
        };
    }
    let width = decode::word_layout(planned.expected.as_ref()).map_or(1, |(points, _, _)| points);
    let off = planned.offset_in_points * 2;
    let Some(slice) = data.get(off..off + width * 2) else {
        return read_error_result(
            planned.item_id,
            &McError::invalid_response(format!(
                "字数据区越界：偏移 {off} 宽 {}，总长 {}",
                width * 2,
                data.len()
            )),
            now,
        );
    };
    match decode::decode_read(
        crate::address::DeviceKind::D,
        planned.expected.clone(),
        slice,
    ) {
        Ok(value) => RawReadResult {
            item_id: planned.item_id,
            value: Some(value),
            source_timestamp_ns: None,
            received_timestamp_ns: now,
            protocol_quality_code: Some(0),
            error: None,
        },
        Err(e) => read_error_result(planned.item_id, &e, now),
    }
}

/// 组装单项读取失败结果。
#[allow(dead_code)]
fn read_error_result(item_id: u64, error: &McError, now: TimestampNs) -> RawReadResult {
    RawReadResult {
        item_id,
        value: None,
        source_timestamp_ns: None,
        received_timestamp_ns: now,
        protocol_quality_code: error.protocol_code,
        error: Some(error.clone().into_info()),
    }
}

fn write_error_result(item_id: u64, error: &McError) -> RawWriteResult {
    RawWriteResult {
        item_id,
        success: false,
        protocol_code: error.protocol_code,
        error: Some(error.clone().into_info()),
    }
}

/// 当前系统时间（纳秒，Unix epoch）。
#[must_use]
pub fn now_ns() -> TimestampNs {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as TimestampNs
}

// ---------------------------------------------------------------- C ABI

/// 能力声明：读 + 写 + 批量 + 轮询（订阅/事件/历史/浏览/命令未实现）。
const CAPABILITIES: ProtocolCapabilities = ProtocolCapabilities {
    read: true,
    write: true,
    batch_read: true,
    batch_write: true,
    browse: false,
    polling: true,
    subscription: false,
    events: false,
    history: false,
};

static API: DriverApiV1 = DriverApiV1 {
    struct_size: size_of::<DriverApiV1>() as u32,
    abi_major: driver_sdk::abi::ABI_MAJOR,
    abi_minor: driver_sdk::abi::ABI_MINOR,
    feature_flags: 0,
    create: api_create,
    destroy: api_destroy,
    connect: api_connect,
    disconnect: api_disconnect,
    get_capabilities_json: api_get_capabilities_json,
    validate_address: api_validate_address,
    read: api_read,
    write: api_write,
    execute: api_execute,
    browse: api_browse,
    subscribe: api_subscribe,
    unsubscribe: api_unsubscribe,
    query_history: api_query_history,
    get_last_error_json: api_get_last_error_json,
    free_buffer: api_free_buffer,
};

/// Native Plugin 唯一入口（§16、§17.9）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1() -> *const DriverApiV1 {
    &API
}

/// 句柄转换（§17.5：句柄由 create 产生，非空才进入函数）。
fn driver_mut<'a>(handle: DriverHandle) -> Option<&'a mut MitsubishiMcDriver> {
    if handle.ptr.is_null() {
        return None;
    }
    Some(unsafe { &mut *(handle.ptr as *mut MitsubishiMcDriver) })
}

/// §17.7 统一 panic 隔离：任何 Rust panic 都不得穿过 C ABI 边界。
///
/// `api_create` 阶段尚无句柄可写错误，单独处理（panic 直接返回失败）。
fn catch_abi_panic<T>(
    handle: DriverHandle,
    f: impl FnOnce() -> T + std::panic::UnwindSafe,
) -> Option<T> {
    match std::panic::catch_unwind(f) {
        Ok(value) => Some(value),
        Err(_) => {
            if let Some(driver) = driver_mut(handle) {
                driver.last_error = Some(
                    McError::driver_panic("内部 panic 已被 ABI 边界捕获（§17.7）".to_owned())
                        .into_info(),
                );
            }
            None
        }
    }
}

unsafe extern "C" fn api_create(config: FfiStr, out_handle: *mut DriverHandle) -> i32 {
    if out_handle.is_null() {
        return -1;
    }
    std::panic::catch_unwind(|| unsafe {
        let (status, driver, error) = match ffi_str_to_str(config) {
            Some(json) => match MitsubishiMcDriver::create(json) {
                Ok(driver) => (0, Some(driver), None),
                Err(e) => (-1, None, Some(e)),
            },
            None => (
                -1,
                None,
                Some(McError::config_error("config 非 UTF-8".to_owned())),
            ),
        };
        let mut driver = match driver {
            Some(driver) => driver,
            None => MitsubishiMcDriver {
                config: McConfig::default(),
                session: None,
                last_error: None,
            },
        };
        if let Some(error) = error {
            driver.last_error = Some(error.into_info());
        }
        *out_handle = DriverHandle {
            ptr: Box::into_raw(Box::new(driver)) as *mut c_void,
        };
        status
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_destroy(handle: DriverHandle) -> i32 {
    catch_abi_panic(handle, || {
        if !handle.ptr.is_null() {
            drop(unsafe { Box::from_raw(handle.ptr as *mut MitsubishiMcDriver) });
        }
        0
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_connect(handle: DriverHandle) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        match driver.ensure_connected() {
            Ok(()) => 0,
            Err(e) => {
                driver.last_error = Some(e.clone().into_info());
                -1
            }
        }
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_disconnect(handle: DriverHandle) -> i32 {
    catch_abi_panic(handle, || {
        if let Some(driver) = driver_mut(handle) {
            driver.disconnect();
        }
        0
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_get_capabilities_json(
    handle: DriverHandle,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let envelope = CapabilitiesEnvelope::new(CAPABILITIES);
        match serde_json::to_vec(&envelope) {
            Ok(bytes) => write_buffer(out, &bytes),
            Err(_) => {
                driver.last_error = Some(McError::unsupported("能力声明序列化").into_info());
                return -1;
            }
        }
        0
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_validate_address(
    handle: DriverHandle,
    address: FfiStr,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let address = match unsafe { ffi_str_to_str(address) } {
            Some(a) => a,
            None => {
                driver.last_error =
                    Some(McError::invalid_address("地址非 UTF-8".to_owned()).into_info());
                return -1;
            }
        };
        match driver.validate_address(address) {
            Ok(meta) => {
                let envelope = AddressEnvelope::new(meta);
                match serde_json::to_vec(&envelope) {
                    Ok(bytes) => write_buffer(out, &bytes),
                    Err(_) => return -1,
                }
                0
            }
            Err(e) => {
                driver.last_error = Some(e.clone().into_info());
                -1
            }
        }
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_read(
    handle: DriverHandle,
    items: *const FfiReadItem,
    len: usize,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        if items.is_null() && len > 0 {
            driver.last_error =
                Some(McError::invalid_address("items 指针为空但长度非 0".to_owned()).into_info());
            return -1;
        }
        let mut read_items: Vec<driver_sdk::DriverReadItem> = Vec::with_capacity(len);
        for i in 0..len {
            let item = unsafe { &*items.add(i) };
            // 复杂类型 Tag 与未知 Tag 必须整体失败（invalid_type，§17.2）。
            let expected_type = match driver_sdk::abi::tag::tag_to_data_type(item.expected_type) {
                Ok(t) => t,
                Err(e) => {
                    driver.last_error = Some(
                        McError::invalid_type(format!("item {} 期望类型 Tag 非法：{e}", item.id))
                            .into_info(),
                    );
                    return -1;
                }
            };
            read_items.push(driver_sdk::DriverReadItem {
                id: item.id,
                address: unsafe { ffi_str_to_str(item.address) }
                    .unwrap_or_default()
                    .to_owned(),
                expected_type,
            });
        }
        match driver.read_batch(&read_items) {
            Ok(results) => {
                let envelope = ReadEnvelope::new(results);
                match serde_json::to_vec(&envelope) {
                    Ok(bytes) => write_buffer(out, &bytes),
                    Err(_) => return -1,
                }
                0
            }
            Err(e) => {
                driver.last_error = Some(e.clone().into_info());
                -1
            }
        }
    })
    .unwrap_or(-1)
}

/// 批量写入（§15 `write`；`out` 为 `abi::envelope::WriteEnvelope`）。
unsafe extern "C" fn api_write(
    handle: DriverHandle,
    items: *const driver_sdk::abi::FfiWriteItem,
    len: usize,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        if items.is_null() && len > 0 {
            driver.last_error =
                Some(McError::invalid_address("items 指针为空但长度非 0".to_owned()).into_info());
            return -1;
        }
        let mut write_items: Vec<batch::WriteRequest> = Vec::with_capacity(len);
        for i in 0..len {
            let item = unsafe { &*items.add(i) };
            // value_bytes 借用调用期内存（§17.1）：len == 0 时 ptr 可为 null。
            let bytes = if item.value_bytes.len == 0 {
                &[][..]
            } else {
                unsafe { std::slice::from_raw_parts(item.value_bytes.ptr, item.value_bytes.len) }
            };
            let value = match driver_sdk::abi::tag::decode_value_bytes(item.value_type, bytes) {
                Ok(value) => value,
                Err(e) => {
                    driver.last_error = Some(
                        McError::invalid_type(format!("item {} 写入值 Tag 非法：{e}", item.id))
                            .into_info(),
                    );
                    return -1;
                }
            };
            write_items.push(batch::WriteRequest {
                id: item.id,
                address: unsafe { ffi_str_to_str(item.address) }
                    .unwrap_or_default()
                    .to_owned(),
                value,
            });
        }
        match driver.write_batch(&write_items) {
            Ok(results) => {
                let envelope = WriteEnvelope::new(results);
                match serde_json::to_vec(&envelope) {
                    Ok(bytes) => write_buffer(out, &bytes),
                    Err(_) => return -1,
                }
                0
            }
            Err(e) => {
                driver.last_error = Some(e.clone().into_info());
                -1
            }
        }
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_execute(
    handle: DriverHandle,
    _command_json: FfiStr,
    _out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let e = McError::unsupported("execute");
        driver.last_error = Some(e.into_info());
        -1
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_browse(
    handle: DriverHandle,
    _path: FfiStr,
    _out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let e = McError::unsupported("browse");
        driver.last_error = Some(e.into_info());
        -1
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_subscribe(
    handle: DriverHandle,
    _request_json: FfiStr,
    _callback: driver_sdk::abi::FfiEventCallback,
    _user_data: *mut c_void,
    _out_subscription_id: *mut u64,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let e = McError::unsupported("subscribe");
        driver.last_error = Some(e.into_info());
        -1
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_unsubscribe(handle: DriverHandle, _subscription_id: u64) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let e = McError::unsupported("unsubscribe");
        driver.last_error = Some(e.into_info());
        -1
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_query_history(
    handle: DriverHandle,
    _request_json: FfiStr,
    _out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let e = McError::unsupported("query_history");
        driver.last_error = Some(e.into_info());
        -1
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_get_last_error_json(
    handle: DriverHandle,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        match &driver.last_error {
            Some(error) => {
                let envelope = ErrorEnvelope::from(error);
                match serde_json::to_vec(&envelope) {
                    Ok(bytes) => write_buffer(out, &bytes),
                    Err(_) => return -1,
                }
            }
            None => unsafe {
                *out = FfiOwnedBuffer {
                    ptr: std::ptr::null_mut(),
                    len: 0,
                    capacity: 0,
                };
            },
        }
        0
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_free_buffer(buffer: FfiOwnedBuffer) {
    // §17.7：free 路径同样不允许 panic 穿过 ABI 边界。
    let _ = std::panic::catch_unwind(|| {
        if !buffer.ptr.is_null() {
            drop(unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.capacity) });
        }
    });
}

// ----------------------------------------------------------------- helpers

/// 把借用 FfiStr 读为 `&str`（§17.1：len == 0 时 ptr 可为 null）。
///
/// # Safety
///
/// 调用方必须保证 `ffi.ptr` 在返回值使用期间有效（调用期借用，§17.1）。
unsafe fn ffi_str_to_str<'a>(ffi: FfiStr) -> Option<&'a str> {
    if ffi.len == 0 {
        return Some("");
    }
    if ffi.ptr.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(ffi.ptr, ffi.len) };
    std::str::from_utf8(bytes).ok()
}

/// 把字节序列写入 Plugin 分配的 owned buffer（转移所有权）。
fn write_buffer(out: *mut FfiOwnedBuffer, bytes: &[u8]) {
    let mut value = bytes.to_vec();
    unsafe {
        *out = FfiOwnedBuffer {
            ptr: value.as_mut_ptr(),
            len: value.len(),
            capacity: value.capacity(),
        };
        std::mem::forget(value);
    }
}
