//! driver-modbus：Modbus TCP / RTU 协议驱动（MVP 首个 Driver，§34）。
//!
//! # 职责边界
//!
//! - **协议表示**：FC01/FC02/FC03/FC04 批量读取与 FC05/FC06/FC15/FC16 写入
//!   （§66 示例：FC03/FC06/FC16/CRC/Slave ID/串口超时/Batch Read 属于
//!   Driver）；
//! - **地址解析**：`1!40001`、`coil:00001`、`input:30001`（Driver 私有不透明
//!   数据，§10，语义见 `address` 模块）；coil → FC05/FC15，
//!   holding → FC06/FC16，discrete/input 只读（写请求显式拒绝）；
//! - **批量合并**：按从站分组、连续地址合并、协议上限拆分（`batch` 模块；
//!   读侧允许跳过中间地址，写侧必须精确相邻——不覆盖未请求的地址）；
//! - **原始结果边界**：只返回 `RawReadResult` / `RawWriteResult`，不生成
//!   `Observation`（§7.3）；保留每个 item 的错误、类型与质量信息。
//!
//! 型号差异（缩放、单位、枚举）属于 Profile，本 Driver 不感知。
//!
//! # 插件形态
//!
//! Native Plugin（§26.2）：`cdylib`，唯一入口 `forgelink_driver_entry_v1()`，
//! 返回稳定 C ABI v1 函数表（driver-sdk §17.9）。禁止跨 FFI 暴露 Rust
//! trait / `async fn`；同步 ABI 下超时由传输层 socket 读超时实现，
//! 阻塞任务隔离由上层（poll-engine `spawn_blocking`）负责。
//!
//! # 超时 / 断线重连
//!
//! - 请求超时：`timeout_ms`（socket 读超时），读写同样受约束，超时返回
//!   `retryable` 错误；
//! - 断线重连：请求中检测到连接断开后，下一次请求按
//!   `reconnect_max_attempts` × `reconnect_delay_ms` 自动重连（§34.3）。
//!
//! # 错误语义（§17.6）
//!
//! 非零状态码（`-1`）仅表示调用失败，详细错误经
//! `get_last_error_json` 返回 `ErrorEnvelope`（code/message/
//! protocol_code/retryable，`error` 模块）。

pub mod address;
pub mod batch;
pub mod config;
pub mod crc;
pub mod decode;
pub mod encode;
pub mod error;
pub mod frame;
pub mod session;

use std::ffi::c_void;
use std::mem::size_of;

use driver_sdk::ProtocolCapabilities;
use driver_sdk::RawWriteResult;
use driver_sdk::abi::envelope::{
    AddressEnvelope, CapabilitiesEnvelope, ErrorEnvelope, ReadEnvelope, WriteEnvelope,
};
use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiReadItem, FfiStr};
use observation_model::{DriverErrorInfo, RawReadResult, TimestampNs};

use crate::batch::{PlannedItem, WritePlan, plan_batch, plan_write_batch};
use crate::config::ModbusConfig;
use crate::error::ModbusError;

// ---------------------------------------------------------------- 驱动实现

/// 驱动句柄状态。
pub struct ModbusDriver {
    config: ModbusConfig,
    transport: Option<Box<dyn session::Transport>>,
    last_error: Option<DriverErrorInfo>,
}

impl ModbusDriver {
    /// 从配置 JSON 创建（不建立连接；连接在首次读取时懒建立）。
    fn create(config_json: &str) -> Result<Self, ModbusError> {
        let config = ModbusConfig::from_json(config_json)
            .map_err(|e| ModbusError::config_error(e.to_string()))?;
        let transport = session::create_transport(&config)?;
        Ok(Self {
            transport: Some(transport),
            config,
            last_error: None,
        })
    }

    /// 建立连接（含按配置的重连尝试）。
    fn connect(&mut self) -> Result<(), ModbusError> {
        let transport = self.transport.as_deref_mut().expect("传输已创建");
        if transport.is_connected() {
            return Ok(());
        }
        let attempts = if self.config.reconnect {
            self.config.reconnect_max_attempts.max(1)
        } else {
            1
        };
        let mut last_error = None;
        for attempt in 0..attempts {
            match transport.connect() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt + 1 < attempts {
                        std::thread::sleep(self.config.reconnect_delay());
                    }
                }
            }
        }
        Err(last_error.expect("至少尝试一次"))
    }

    fn disconnect(&mut self) {
        if let Some(transport) = self.transport.as_deref_mut() {
            transport.disconnect();
        }
    }

    /// 批量读取：规划 → 逐计划请求 → 组装每 item 结果。
    fn read_batch(
        &mut self,
        items: &[driver_sdk::DriverReadItem],
    ) -> Result<Vec<RawReadResult>, ModbusError> {
        let plans = plan_batch(items, self.config.unit_id)?;
        // 请求串行化：句柄非重入，Loader 保证单线程串行调用；此处逐计划
        // 顺序执行，不跨请求混淆（事务号/帧级校验在 session 层）。
        let mut results = Vec::with_capacity(items.len());
        for plan in &plans {
            self.ensure_connected_before_plan()?;
            let data = match self.request_plan(plan) {
                Ok(data) => data,
                // 传输级失败（连接断开/建连失败/超时/响应失步）：会话已不可用，
                // 必须整体失败返回（PollDriver 约定 §22），由上层退避/重连；
                // 不得转成单项错误伪装成成功批次。
                Err(e) if e.is_transport_level() => return Err(e),
                // 协议级错误（从站异常等）：会话仍可用，逐项标记后继续。
                Err(e) => {
                    for planned in &plan.items {
                        results.push(error_result(planned.item_id, &e));
                    }
                    continue;
                }
            };
            for planned in &plan.items {
                results.push(decode_item(
                    &plan.kind,
                    &data,
                    planned,
                    self.config.word_order,
                ));
            }
        }
        // 结果顺序与请求 item 顺序一致（供上层按 item_id 关联）。
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    /// 请求前确保连接可用（连接断开时自动重连）。
    fn ensure_connected_before_plan(&mut self) -> Result<(), ModbusError> {
        let transport = self.transport.as_deref_mut().expect("传输已创建");
        if transport.is_connected() {
            return Ok(());
        }
        self.connect()
    }

    /// 批量写入：规划 → 逐计划请求 → 组装每 item 结果。
    ///
    /// 部分失败语义与 [`ModbusDriver::read_batch`] 一致：
    ///
    /// - 传输级失败（断线/超时/响应失步）：整体失败返回，由上层退避/重连；
    /// - 协议级失败（从站异常）：同计划内全部 item 标记失败后继续后续计划；
    /// - 值编码失败（Bool 写寄存器等）：规划期剔除，逐项返回 `invalid_type`。
    fn write_batch(
        &mut self,
        items: &[batch::WriteRequest],
    ) -> Result<Vec<RawWriteResult>, ModbusError> {
        let (plans, mut results) =
            plan_write_batch(items, self.config.unit_id, self.config.word_order)?;
        // 请求串行化：与读路径一致，逐计划顺序执行；计划按 (unit, kind, 地址)
        // 升序生成，重叠地址的写项因此获得确定的覆盖顺序。
        for plan in &plans {
            self.ensure_connected_before_plan()?;
            match self.request_write_plan(plan) {
                Ok(()) => {
                    for item_id in &plan.item_ids {
                        results.push(RawWriteResult {
                            item_id: *item_id,
                            success: true,
                            // 写确认无附加协议质量码：成功置 0（与读路径一致）。
                            protocol_code: Some(0),
                            error: None,
                        });
                    }
                }
                // 传输级失败：会话已不可用，必须整体失败返回（PollDriver 约定
                // §22），不得转成单项错误伪装成成功批次。
                Err(e) if e.is_transport_level() => return Err(e),
                // 协议级错误（从站异常等）：会话仍可用，逐项标记后继续。
                Err(e) => {
                    for item_id in &plan.item_ids {
                        results.push(write_error_result(*item_id, &e));
                    }
                }
            }
        }
        // 结果顺序与请求 item 顺序一致（供上层按 item_id 关联）。
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    /// 执行一个计划的写帧（回显校验在 session 层）。
    fn request_write_plan(&mut self, plan: &WritePlan) -> Result<(), ModbusError> {
        let transport = self.transport.as_deref_mut().expect("传输已创建");
        let result = transport.write_transaction(
            plan.unit_id,
            plan.function,
            plan.start_offset,
            &plan.payload,
        );
        match result {
            Ok(()) => Ok(()),
            // 传输级错误：会话已失步/断开，标记断开供下次请求自动重连。
            Err(e) if e.is_transport_level() => {
                transport.disconnect();
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    /// 执行一个计划的读帧，返回纯数据字节（寄存器大端字节 / 位字节）。
    fn request_plan(&mut self, plan: &batch::ReadPlan) -> Result<Vec<u8>, ModbusError> {
        let transport = self.transport.as_deref_mut().expect("传输已创建");
        let result = transport.read_transaction(
            plan.unit_id,
            plan.kind.function_code(),
            plan.start_offset,
            plan.quantity,
        );
        match result {
            Ok(data) => Ok(data),
            // 传输级错误：会话已失步/断开，标记断开供下次请求自动重连，
            // 并原样整体返回（调用方按 `is_transport_level` 决定是否整体失败）。
            Err(e) if e.is_transport_level() => {
                transport.disconnect();
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    /// 校验并规范化地址（§15 `validate_address`）。
    fn validate_address(
        &mut self,
        address: &str,
    ) -> Result<driver_sdk::AddressMetadata, ModbusError> {
        let parsed = crate::address::parse_address(address, self.config.unit_id)
            .map_err(|e| ModbusError::invalid_address(e.to_string()))?;
        Ok(driver_sdk::AddressMetadata {
            canonical_address: parsed.canonical(),
            // Modbus 寄存器本身不携带类型，类型由 Profile 的 expected_type 决定。
            raw_type: None,
            readable: true,
            writable: parsed.kind.writable(),
        })
    }
}

/// 组装单项解码结果（保留错误与质量信息）。
fn decode_item(
    kind: &crate::address::RegisterKind,
    data: &[u8],
    planned: &PlannedItem,
    word_order: crate::config::WordOrder,
) -> RawReadResult {
    let now = now_ns();
    match decode::decode_register_value(
        *kind,
        data,
        planned.offset_in_frame,
        planned.expected_type.as_ref(),
        word_order,
    ) {
        Ok(value) => RawReadResult {
            item_id: planned.item_id,
            value: Some(value),
            source_timestamp_ns: None,
            received_timestamp_ns: now,
            // Modbus 无协议质量码：成功置 0（上层按 Good 处理）。
            protocol_quality_code: Some(0),
            error: None,
        },
        Err(e) => error_result_with_ts(planned.item_id, &ModbusError::decode_error(e.message), now),
    }
}

fn error_result(item_id: u64, error: &ModbusError) -> RawReadResult {
    error_result_with_ts(item_id, error, now_ns())
}

/// 组装单项写入失败结果。
fn write_error_result(item_id: u64, error: &ModbusError) -> RawWriteResult {
    RawWriteResult {
        item_id,
        success: false,
        protocol_code: error.protocol_code,
        error: Some(error.clone().into_info()),
    }
}

fn error_result_with_ts(item_id: u64, error: &ModbusError, now: TimestampNs) -> RawReadResult {
    RawReadResult {
        item_id,
        value: None,
        source_timestamp_ns: None,
        received_timestamp_ns: now,
        protocol_quality_code: error.protocol_code,
        error: Some(error.clone().into_info()),
    }
}

/// 当前系统时间（纳秒，Unix epoch）。
fn now_ns() -> TimestampNs {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as TimestampNs
}

// ---------------------------------------------------------------- C ABI

/// 能力声明：读 + 写 + 轮询（订阅/事件/历史本阶段未实现）。
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
fn driver_mut<'a>(handle: DriverHandle) -> Option<&'a mut ModbusDriver> {
    if handle.ptr.is_null() {
        return None;
    }
    Some(unsafe { &mut *(handle.ptr as *mut ModbusDriver) })
}

/// §17.7 统一 panic 隔离：任何 Rust panic 都不得穿过 C ABI 边界。
///
/// 每个 ABI 入口必须用本封装包裹主体逻辑；panic 被捕获后转换为
/// `DRIVER_PANIC` 错误（写入句柄 `last_error`，供 `get_last_error_json`
/// 查询）并返回 `None`，调用方回退为非零状态码。
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
                    ModbusError::driver_panic("内部 panic 已被 ABI 边界捕获（§17.7）".to_owned())
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
    // create 阶段无句柄可写 last_error：panic 直接返回非零（Loader 视为失败，
    // §17.5 句柄值未定义时可能为空）。
    std::panic::catch_unwind(|| unsafe {
        // create 失败也写入句柄（指向可查询 last_error 的状态），Loader 会读取
        // 详情并 destroy 清理（§17.5 句柄值未定义时可能为空）。
        let (status, driver, error) = match ffi_str_to_str(config) {
            Some(json) => match ModbusDriver::create(json) {
                Ok(driver) => (0, Some(driver), None),
                Err(e) => (-1, None, Some(e)),
            },
            None => (
                -1,
                None,
                Some(ModbusError::config_error("config 非 UTF-8".to_owned())),
            ),
        };
        let mut driver = match driver {
            Some(driver) => driver,
            None => ModbusDriver {
                config: ModbusConfig::default(),
                transport: None,
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
            unsafe {
                drop(Box::from_raw(handle.ptr as *mut ModbusDriver));
            }
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
        match driver.connect() {
            Ok(()) => 0,
            Err(e) => {
                let info = e.clone().into_info();
                driver.last_error = Some(info);
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
                driver.last_error = Some(ModbusError::unsupported("能力声明序列化").into_info());
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
                    Some(ModbusError::invalid_address("地址非 UTF-8".to_owned()).into_info());
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
                let info = e.clone().into_info();
                driver.last_error = Some(info);
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
            driver.last_error = Some(
                ModbusError::invalid_address("items 指针为空但长度非 0".to_owned()).into_info(),
            );
            return -1;
        }
        let mut read_items: Vec<driver_sdk::DriverReadItem> = Vec::with_capacity(len);
        for i in 0..len {
            let item = unsafe { &*items.add(i) };
            // 复杂类型 Tag（Array/Struct 缺 schema）与未知 Tag 必须整体失败
            // （invalid_type，§17.2）：不得静默降级为"未指定类型"（否则会按
            // U16 解码出错误数值）。
            let expected_type = match driver_sdk::abi::tag::tag_to_data_type(item.expected_type) {
                Ok(t) => t,
                Err(e) => {
                    driver.last_error = Some(
                        ModbusError::invalid_type(format!(
                            "item {} 期望类型 Tag 非法：{e}",
                            item.id
                        ))
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
                let info = e.clone().into_info();
                driver.last_error = Some(info);
                -1
            }
        }
    })
    .unwrap_or(-1)
}

/// 批量写入（§15 `write`；`out` 为 `abi::envelope::WriteEnvelope`）。
///
/// 每个 `FfiWriteItem` 的 `value_type` 为 ABI v1 Tag、`value_bytes` 为按
/// §17.2 标量编码的值：Tag 非法（未知/复杂类型/长度不符）必须整体失败
/// （invalid_type，§17.2），不得静默降级。
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
            driver.last_error = Some(
                ModbusError::invalid_address("items 指针为空但长度非 0".to_owned()).into_info(),
            );
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
                        ModbusError::invalid_type(format!("item {} 写入值 Tag 非法：{e}", item.id))
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
                value_type: item.value_type,
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
                let info = e.clone().into_info();
                driver.last_error = Some(info);
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
        let e = ModbusError::unsupported("execute");
        driver.last_error = Some(e.clone().into_info());
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
        let e = ModbusError::unsupported("browse");
        driver.last_error = Some(e.clone().into_info());
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
        let e = ModbusError::unsupported("subscribe");
        driver.last_error = Some(e.clone().into_info());
        -1
    })
    .unwrap_or(-1)
}

unsafe extern "C" fn api_unsubscribe(handle: DriverHandle, _subscription_id: u64) -> i32 {
    catch_abi_panic(handle, || {
        let Some(driver) = driver_mut(handle) else {
            return -1;
        };
        let e = ModbusError::unsupported("unsubscribe");
        driver.last_error = Some(e.clone().into_info());
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
        let e = ModbusError::unsupported("query_history");
        driver.last_error = Some(e.clone().into_info());
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
    // §17.7：free 路径同样不允许 panic 穿过 ABI 边界（这里没有句柄可写
    // 错误，panic 直接静默返回）。
    let _ = std::panic::catch_unwind(|| {
        if !buffer.ptr.is_null() {
            // 由 write_buffer 的 Vec 重建并释放（§17.3 谁分配谁释放）。
            unsafe {
                drop(Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.capacity));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// ABI 常量与入口可用性。
    #[test]
    fn entry_symbol_and_abi_constants() {
        let api = unsafe { &*forgelink_driver_entry_v1() };
        assert_eq!(api.abi_major, driver_sdk::abi::ABI_MAJOR);
        assert_eq!(api.abi_minor, driver_sdk::abi::ABI_MINOR);
        assert_eq!(
            api.struct_size as usize,
            size_of::<DriverApiV1>(),
            "struct_size 必须与定义一致（§17.4）"
        );
    }

    /// 无效配置 create 失败，错误详情可查询。
    #[test]
    fn create_rejects_invalid_config() {
        let mut handle = DriverHandle {
            ptr: std::ptr::null_mut(),
        };
        let status = unsafe { api_create(FfiStr::empty(), &mut handle) };
        assert_eq!(status, -1);
        assert!(!handle.ptr.is_null());
        let mut out = FfiOwnedBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let status = unsafe { api_get_last_error_json(handle, &mut out) };
        assert_eq!(status, 0);
        assert!(!out.ptr.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) };
        let json = String::from_utf8(bytes.to_vec()).unwrap();
        let envelope: ErrorEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(envelope.code, "config_error");
        assert!(!envelope.retryable);
        unsafe { api_free_buffer(out) };
        unsafe { api_destroy(handle) };
    }

    /// §17.7：ABI 入口内的 panic 必须被统一捕获为 `DRIVER_PANIC`，
    /// 不得穿过 C ABI 边界终止宿主进程。
    #[test]
    fn panic_is_caught_at_abi_boundary() {
        let mut handle = DriverHandle {
            ptr: std::ptr::null_mut(),
        };
        let status = unsafe { api_create(FfiStr::empty(), &mut handle) };
        assert_eq!(status, -1);
        assert!(!handle.ptr.is_null());

        // 注入 panic：wrapper 必须返回 None（入口回退为 -1）并记录
        // DRIVER_PANIC，供 get_last_error_json 查询。
        let caught = catch_abi_panic(handle, || panic!("注入的 panic"));
        assert_eq!(caught, None);

        let mut out = FfiOwnedBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let status = unsafe { api_get_last_error_json(handle, &mut out) };
        assert_eq!(status, 0);
        assert!(!out.ptr.is_null());
        let bytes = unsafe { std::slice::from_raw_parts(out.ptr, out.len) };
        let json = String::from_utf8(bytes.to_vec()).unwrap();
        let envelope: ErrorEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(envelope.code, "DRIVER_PANIC");
        assert!(!envelope.retryable);
        unsafe { api_free_buffer(out) };
        unsafe { api_destroy(handle) };
    }
}
