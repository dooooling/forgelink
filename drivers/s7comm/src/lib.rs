//! driver-s7comm：Siemens S7comm 协议驱动（§34.6 V0.2）。
//!
//! # 职责边界
//!
//! - **协议表示**：S7 Read Var（0x04）批量读取与 Write Var（0x05）写入，
//!   ISO-on-TCP（TPKT/COTP）承载；地址模型 `db10.dbw0` / `mw20` / `m0.1`
//!   等（§11，语义见 `address` 模块）；I 区只读；
//! - **批量合并**：按 `(area, db, 语法)` 分组、读侧允许跳洞、写侧精确
//!   相邻（`batch` 模块）；分块受协商 PDU 预算与配置项数上限约束；
//! - **原始结果边界**：只返回 `RawReadResult` / `RawWriteResult`，不生成
//!   `Observation`（§7.3）；宽度由地址语法承载、值解释由 expected_type
//!   决定（映射表见 `decode`/`encode` 模块文档）。
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
//! - 请求超时：`timeout_ms`（socket 读超时），超时返回 retryable 错误；
//! - 断线重连：断开后下一次请求按 `reconnect_max_attempts` ×
//!   `reconnect_delay_ms` 重走完整握手（COTP + Setup——协商结果属连接级
//!   状态不得复用，§34.3）；
//! - 与 modbus 的顺序差异：分块预算取决于协商 PDU，因此**先握手后规划**。
//!
//! # 错误语义（§17.6）
//!
//! 非零状态码（`-1`）仅表示调用失败，详细错误经 `get_last_error_json`
//! 返回 `ErrorEnvelope`。传输级失败（断线/超时/失步）整体返回由上层退避
//! 重连；协议级失败（逐项 return code 非 0xFF）会话保留、逐项标记。

pub mod address;
pub mod batch;
pub mod config;
pub mod cotp;
pub mod decode;
pub mod encode;
pub mod error;
pub mod pdu;
pub mod session;

use std::ffi::c_void;
use std::mem::size_of;
use std::time::Duration;

use driver_sdk::{ProtocolCapabilities, RawWriteResult};
use observation_model::{DriverErrorInfo, RawReadResult, RawValue, TimestampNs};

use driver_sdk::abi::envelope::{
    AddressEnvelope, CapabilitiesEnvelope, ErrorEnvelope, ReadEnvelope, WriteEnvelope,
};
use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiReadItem, FfiStr};

use crate::batch::{ReadPlan, plan_read_batch, plan_write_batch};
use crate::config::S7Config;
use crate::error::S7Error;
use crate::session::TcpSession;

// ---------------------------------------------------------------- 驱动实现

/// 单计划执行的响应项（owned 载荷，避免生命周期外泄）。
struct ItemResponse {
    return_code: u8,
    payload: Vec<u8>,
}

/// 驱动句柄状态。
pub struct S7commDriver {
    config: S7Config,
    session: Option<TcpSession>,
    last_error: Option<DriverErrorInfo>,
}

impl S7commDriver {
    /// 从配置 JSON 创建（不建立连接；连接在首次读写前懒建立）。
    fn create(config_json: &str) -> Result<Self, S7Error> {
        let config = config::parse_config(config_json)?;
        let session = TcpSession::new(
            config.host.clone(),
            config.port,
            config.rack,
            config.slot,
            config.timeout_ms,
        );
        Ok(Self {
            config,
            session: Some(session),
            last_error: None,
        })
    }

    /// 建立连接（含按配置的重连尝试；每次尝试都执行完整三步握手）。
    fn connect(&mut self) -> Result<(), S7Error> {
        let session = self.session.as_mut().expect("会话已创建");
        if session.is_connected() {
            return Ok(());
        }
        let attempts = if self.config.reconnect {
            self.config.reconnect_max_attempts.max(1)
        } else {
            1
        };
        let mut last_error = None;
        for attempt in 0..attempts {
            match session.connect() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt + 1 < attempts {
                        std::thread::sleep(Duration::from_millis(self.config.reconnect_delay_ms));
                    }
                }
            }
        }
        Err(last_error.expect("至少尝试一次"))
    }

    /// 确保连接可用并返回协商 PDU 上限（规划的预算输入）。
    fn ensure_connected(&mut self) -> Result<u16, S7Error> {
        if !self.session.as_ref().expect("会话已创建").is_connected() {
            self.connect()?;
        }
        Ok(self.session.as_ref().expect("会话已创建").negotiated_pdu())
    }

    fn disconnect(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.disconnect();
        }
    }

    /// 批量读取：连接（协商）→ 规划 → 逐计划请求 → 组装每项结果。
    ///
    /// 请求串行化：句柄非重入，Loader 保证单线程串行调用；逐计划顺序
    /// 执行；每条计划的合并区间编码为单条 Any 指针（一个 PDU 一项）。
    fn read_batch(
        &mut self,
        items: &[driver_sdk::DriverReadItem],
    ) -> Result<Vec<RawReadResult>, S7Error> {
        let negotiated = self.ensure_connected()?;
        let plans = plan_read_batch(items, negotiated, self.config.max_items_per_pdu)?;
        let mut results = Vec::with_capacity(items.len());
        for plan in &plans {
            // 传输级失败（断线/建连/超时/失步）：整体失败返回（PollDriver
            // 约定 §22），不得转成单项错误伪装成成功批次。
            let response = self.execute_read_plan(plan)?;
            for planned in &plan.items {
                results.push(assemble_item(planned, &response));
            }
        }
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    /// 执行一条读取计划（单 Any 指针），返回首个响应项（owned）。
    fn execute_read_plan(&mut self, plan: &ReadPlan) -> Result<ItemResponse, S7Error> {
        let session = self.session.as_mut().expect("会话已创建");
        let r = next_ref(session);
        let job = pdu::build_read(r, std::slice::from_ref(&plan.any_item()));
        let ack = session.exchange(r, &job)?;
        let mut parsed = pdu::parse_read_response(&ack.param, &ack.data, 1)?;
        let first = parsed.remove(0);
        Ok(ItemResponse {
            return_code: first.return_code,
            payload: first.payload.to_vec(),
        })
    }

    /// 批量写入：连接 → 编码规划 → 逐计划请求 → 组装每项结果。
    ///
    /// - 传输级失败：整体失败返回（PollDriver 约定 §22）；
    /// - 协议级失败（逐项 return code 非 0xFF）：同计划内全部 item 标记
    ///   失败后继续后续计划；
    /// - 编码失败 / 只读区拒绝：规划期剔除并预填单项结果。
    fn write_batch(
        &mut self,
        items: &[batch::WriteRequest],
    ) -> Result<Vec<RawWriteResult>, S7Error> {
        let negotiated = self.ensure_connected()?;
        let (plans, mut failed) =
            plan_write_batch(items, negotiated, self.config.max_items_per_pdu)?;
        let mut results: Vec<RawWriteResult> = Vec::with_capacity(items.len());
        for (id, e) in failed.drain(..) {
            results.push(write_error_result(id, &e));
        }
        for plan in &plans {
            let session = self.session.as_mut().expect("会话已创建");
            let r = next_ref(session);
            let job = pdu::build_write(r, &[(plan.any_item(), plan.payload.clone())]);
            let ack = session.exchange(r, &job)?;
            let codes = pdu::parse_write_response(&ack.param, &ack.data, 1)?;
            // 合并计划共享单条响应码：整段写成功或整段失败。
            if codes[0] == pdu::RC_SUCCESS {
                for item_id in &plan.item_ids {
                    results.push(RawWriteResult {
                        item_id: *item_id,
                        success: true,
                        protocol_code: Some(0),
                        error: None,
                    });
                }
            } else {
                let e = item_return_error(codes[0]);
                for item_id in &plan.item_ids {
                    results.push(write_error_result(*item_id, &e));
                }
            }
        }
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    /// 校验并规范化地址（§15 `validate_address`）。
    fn validate_address(&mut self, address: &str) -> Result<driver_sdk::AddressMetadata, S7Error> {
        let parsed = address::parse(address)
            .map_err(|e| S7Error::invalid_address(format!("{address}: {e}")))?;
        Ok(driver_sdk::AddressMetadata {
            canonical_address: parsed.canonical(),
            // S7 地址语法本身不携带值语义类型，解释由 Profile 的
            // expected_type 决定（映射表见 decode 模块文档）。
            raw_type: None,
            readable: true,
            writable: parsed.writable(),
        })
    }
}

/// 会话 pdu-ref 自增（封装可变借用边界）。
fn next_ref(session: &mut TcpSession) -> u16 {
    session.next_ref()
}

/// 组装单项读取结果（按 return code 分流成功/协议级失败）。
fn assemble_item(planned: &batch::PlannedItem, response: &ItemResponse) -> RawReadResult {
    let now = now_ns();
    if response.return_code != pdu::RC_SUCCESS {
        return error_result_with_ts(
            planned.item_id,
            &item_return_error(response.return_code),
            now,
        );
    }
    // 合并区间按项偏移切片（响应载荷是整段 span，非单点）。
    let start = planned.offset_in_data;
    let width = planned.ty.width_bytes() as usize;
    let Some(slice) = response.payload.get(start..start + width) else {
        return error_result_with_ts(
            planned.item_id,
            &S7Error::invalid_response(format!(
                "响应载荷越界：偏移 {start} 宽度 {width}，总长 {}",
                response.payload.len()
            )),
            now,
        );
    };
    match decode::decode_read(planned.ty, planned.expected_type.clone(), slice) {
        Ok(out) => RawReadResult {
            item_id: planned.item_id,
            value: Some(match out {
                decode::RawValueOut::Bool(b) => RawValue::Bool(b),
                decode::RawValueOut::Signed(v) => RawValue::I64(v),
                decode::RawValueOut::Unsigned(v) => RawValue::U64(v),
                decode::RawValueOut::Real(v) => RawValue::F64(f64::from(v)),
            }),
            source_timestamp_ns: None,
            received_timestamp_ns: now,
            protocol_quality_code: Some(0),
            error: None,
        },
        Err(e) => {
            // 解码期结构不符（载荷与语法宽度错位）在此已无法安全区分失步
            // 与数据异常——结构性校验已在 pdu 层完成，此处统一降为单项
            // decode_error 并保留原始信息。
            error_result_with_ts(planned.item_id, &S7Error::decode_error(e.message), now)
        }
    }
}

/// 逐项 return code → 错误分类。
fn item_return_error(return_code: u8) -> S7Error {
    if return_code == 0x07 {
        // 访问被拒（保护级别/只读区），稳定码 access_denied。
        S7Error::access_denied(return_code)
    } else {
        S7Error::s7_item_error(return_code)
    }
}

/// 组装单项读取失败结果。
fn error_result_with_ts(item_id: u64, error: &S7Error, now: TimestampNs) -> RawReadResult {
    RawReadResult {
        item_id,
        value: None,
        source_timestamp_ns: None,
        received_timestamp_ns: now,
        protocol_quality_code: error.protocol_code,
        error: Some(error.clone().into_info()),
    }
}

/// 组装单项写入失败结果。
fn write_error_result(item_id: u64, error: &S7Error) -> RawWriteResult {
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
fn driver_mut<'a>(handle: DriverHandle) -> Option<&'a mut S7commDriver> {
    if handle.ptr.is_null() {
        return None;
    }
    Some(unsafe { &mut *(handle.ptr as *mut S7commDriver) })
}

/// §17.7 统一 panic 隔离：任何 Rust panic 都不得穿过 C ABI 边界。
///
/// 每个 ABI 入口必须用本封装包裹主体逻辑；panic 被捕获后转换为
/// `DRIVER_PANIC` 错误（写入句柄 last_error，供 `get_last_error_json`
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
                    S7Error::driver_panic("内部 panic 已被 ABI 边界捕获（§17.7）".to_owned())
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
    // create 失败也写入句柄（指向可查询 last_error 的状态），Loader 会
    // 读取详情并 destroy 清理（§17.5）。
    std::panic::catch_unwind(|| unsafe {
        let (status, driver, error) = match ffi_str_to_str(config) {
            Some(json) => match S7commDriver::create(json) {
                Ok(driver) => (0, Some(driver), None),
                Err(e) => (-1, None, Some(e)),
            },
            None => (
                -1,
                None,
                Some(S7Error::config_error("config 非 UTF-8".to_owned())),
            ),
        };
        let mut driver = match driver {
            Some(driver) => driver,
            None => S7commDriver {
                config: S7Config::default(),
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
            drop(unsafe { Box::from_raw(handle.ptr as *mut S7commDriver) });
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
                driver.last_error = Some(S7Error::unsupported("能力声明序列化").into_info());
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
                    Some(S7Error::invalid_address("地址非 UTF-8".to_owned()).into_info());
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
                Some(S7Error::invalid_address("items 指针为空但长度非 0".to_owned()).into_info());
            return -1;
        }
        let mut read_items: Vec<driver_sdk::DriverReadItem> = Vec::with_capacity(len);
        for i in 0..len {
            let item = unsafe { &*items.add(i) };
            // 复杂类型 Tag（Array/Struct 缺 schema）与未知 Tag 必须整体
            // 失败（invalid_type，§17.2）：不得静默降级为"未指定类型"。
            let expected_type = match driver_sdk::abi::tag::tag_to_data_type(item.expected_type) {
                Ok(t) => t,
                Err(e) => {
                    driver.last_error = Some(
                        S7Error::invalid_type(format!("item {} 期望类型 Tag 非法：{e}", item.id))
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
///
/// 每个 `FfiWriteItem` 的 `value_type` 为 ABI v1 Tag、`value_bytes` 为按
/// §17.2 标量编码的值：Tag 非法（未知/复杂类型/长度不符）必须整体失败
/// （invalid_type），不得静默降级。
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
                Some(S7Error::invalid_address("items 指针为空但长度非 0".to_owned()).into_info());
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
                        S7Error::invalid_type(format!("item {} 写入值 Tag 非法：{e}", item.id))
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
        let e = S7Error::unsupported("execute");
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
        let e = S7Error::unsupported("browse");
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
        let e = S7Error::unsupported("subscribe");
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
        let e = S7Error::unsupported("unsubscribe");
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
        let e = S7Error::unsupported("query_history");
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
    // §17.7：free 路径同样不允许 panic 穿过 ABI 边界（这里没有句柄可写
    // 错误，panic 直接静默返回）。
    let _ = std::panic::catch_unwind(|| {
        if !buffer.ptr.is_null() {
            // 由 write_buffer 的 Vec 重建并释放（§17.3 谁分配谁释放）。
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
