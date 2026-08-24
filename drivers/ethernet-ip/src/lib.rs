//! driver-ether-ip：EtherNet/IP (CIP) 协议驱动（§34.6 V0.3）。
//!
//! # 职责边界
//!
//! - **协议表示**：CIP Read Tag (0x4C) / Write Tag (0x4D)，全部经
//!   Multi-Service (0x0A) 打包（单项也走 Multi——单一代码路径）；EN/IP
//!   封装层承载（24B 小端头 + RegisterSession 会话）；符号标签寻址
//!   （大小写敏感，见 `address` 模块）；
//! - **批量合并**：合并 = 同一 Multi 包内多个子服务（无地址区间拼接、
//!   无跳洞/精确相邻概念——各子服务独立寻址）；分块受静态配置双上限
//!   约束（`max_services_per_multi` / `max_bytes_per_multi`），规划不
//!   依赖连接（与 S7 先握手后规划相反）；
//! - **写侧懒式类型发现**：Logix 写要求 CIP 类型码精确匹配，首写先
//!   Read Tag 探明类型并缓存，再精确编码写入；
//! - **原始结果边界**：只返回 `RawReadResult` / `RawWriteResult`（§7.3）；
//!   宽度与基础编码由设备应答的类型码承载、解释由 expected_type 决定
//!   （映射表见 `decode`/`encode` 模块文档）。
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
//! - 请求超时：`timeout_ms`（socket 读超时），超时返回 retryable 错误；
//! - 断线重连：断开后下一次请求按 `reconnect_max_attempts` ×
//!   `reconnect_delay_ms` 重走完整握手（TCP + RegisterSession——session
//!   handle 属连接级状态不得复用，§34.3）。
//!
//! # 错误语义（§17.6）
//!
//! 非零状态码（`-1`）仅表示调用失败，详细错误经 `get_last_error_json`
//! 返回 `ErrorEnvelope`。传输级失败（断线/超时/失步/封装否定）整体返回
//! 由上层退避重连；协议级失败（子服务 general status 非 0）会话保留、
//! 逐项标记。已知限制：control-engine 错误码白名单未含本驱动新增稳定码
//! （cip_item_error 等），北向控制链路归一为 driver_error——与 V0.2 S7
//! 现状一致。

pub mod address;
pub mod batch;
pub mod cip;
pub mod config;
pub mod decode;
pub mod encode;
pub mod enip;
pub mod error;
pub mod session;

use std::collections::HashMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::time::Duration;

use driver_sdk::{ProtocolCapabilities, RawWriteResult};
use observation_model::{DriverErrorInfo, RawReadResult, TimestampNs};

use driver_sdk::abi::envelope::{
    AddressEnvelope, CapabilitiesEnvelope, ErrorEnvelope, ReadEnvelope, WriteEnvelope,
};
use driver_sdk::abi::{DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiReadItem, FfiStr};

use crate::batch::{plan_write_stage1, plan_write_stage2};
use crate::config::EnIpConfig;
use crate::error::EtherIpError;

// ---------------------------------------------------------------- 驱动实现

/// 驱动句柄状态。
pub struct EtherIpDriver {
    config: EnIpConfig,
    session: Option<session::TcpSession>,
    /// 写侧类型发现缓存（标签 → CIP 类型码）。类型码是设备侧静态属性
    /// 非连接状态——断线重连后保留有效。
    type_cache: HashMap<String, u16>,
    last_error: Option<DriverErrorInfo>,
}

impl EtherIpDriver {
    /// 从配置 JSON 创建（不建立连接；连接在首次读写前懒建立）。
    fn create(config_json: &str) -> Result<Self, EtherIpError> {
        let config = config::parse_config(config_json)?;
        let session = session::TcpSession::new(config.host.clone(), config.port, config.timeout_ms);
        Ok(Self {
            config,
            session: Some(session),
            type_cache: HashMap::new(),
            last_error: None,
        })
    }

    /// 建立连接（含按配置的重连尝试；每次尝试都重新 RegisterSession）。
    fn connect(&mut self) -> Result<(), EtherIpError> {
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

    fn ensure_connected(&mut self) -> Result<(), EtherIpError> {
        if !self.session.as_ref().expect("会话已创建").is_connected() {
            self.connect()?;
        }
        Ok(())
    }

    fn disconnect(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.disconnect();
        }
    }

    /// 批量读取：规划 → 逐 Multi 包请求 → 解包逐子应答 → 组装结果。
    ///
    /// 子应答与请求项的关联：规划按 canonical 升序产出子请求序列，
    /// 应答偏移表保序——zip 回填即恢复 item 对应；item_id/expected_type
    /// 经 [`ReadEntry`] 随子请求一起进入循环。
    fn read_batch(
        &mut self,
        items: &[driver_sdk::DriverReadItem],
    ) -> Result<Vec<RawReadResult>, EtherIpError> {
        self.ensure_connected()?;
        let entries = plan_read_entries(items)?;
        // 规划已按 tag 升序排定 entries 与子请求的一致顺序。
        let sub_requests: Vec<Vec<u8>> = entries
            .iter()
            .map(|e| cip::build_read_tag(&e.tag))
            .collect();
        let chunks = batch::chunk_multi(
            &sub_requests,
            self.config.max_services_per_multi,
            self.config.max_bytes_per_multi,
        );
        let mut results = Vec::with_capacity(items.len());
        let mut cursor = 0usize;
        for chunk in &chunks {
            let multi = cip::build_multi(chunk);
            let reply = self.exchange_checked(&multi)?;
            let sub_replies = cip::parse_multi_reply(&reply[4..], chunk.len())?;
            for (i, sub_reply) in sub_replies.iter().enumerate() {
                let entry = &entries[cursor + i];
                results.push(assemble_read_with(
                    entry.item_id,
                    entry.expected.clone(),
                    sub_reply.bytes,
                ));
            }
            cursor += chunk.len();
        }
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    /// 发送一条 CIP 请求并校验应答首部（service|0x80 + reserved +
    /// general status）。Multi 整体否定视为传输级失败（服务/会话异常）。
    fn exchange_checked(&mut self, cip_request: &[u8]) -> Result<Vec<u8>, EtherIpError> {
        let session = self.session.as_mut().expect("会话已创建");
        let reply = session.exchange(cip_request)?;
        if reply.len() < 4 {
            return Err(EtherIpError::invalid_response(format!(
                "CIP 应答截断：{}",
                reply.len()
            )));
        }
        let expect_service = u16::from(cip_request[0] | 0x80);
        if u16::from(reply[0]) != expect_service {
            return Err(EtherIpError::unexpected_command_code(
                expect_service,
                u16::from(reply[0]),
            ));
        }
        if reply[2] != cip::STATUS_SUCCESS {
            return Err(EtherIpError::cip_item_error(reply[2]));
        }
        Ok(reply)
    }

    /// 批量写入（两段式）：
    ///
    /// 1. 类型缓存分流 → 未命中项打包批量类型发现 Multi → 命中者登记
    ///    缓存、失败者预填逐项失败；
    /// 2. 全部已知类型的项编码打包 Write Tag 子服务 Multi → 逐子应答
    ///    回填结果。
    fn write_batch(
        &mut self,
        items: &[batch::WriteRequest],
    ) -> Result<Vec<RawWriteResult>, EtherIpError> {
        self.ensure_connected()?;
        let mut results: Vec<RawWriteResult> = Vec::with_capacity(items.len());
        // settled：已产出结果的 item_id（防两阶段间重复回填）。
        let mut settled: std::collections::HashSet<u64> = std::collections::HashSet::new();

        // ── 阶段 1：类型发现（对缓存未命中的标签先读类型）──
        let (discovery, _ready_stage1, mut failed) = plan_write_stage1(items, &self.type_cache)?;
        for (id, e) in failed.drain(..) {
            settled.insert(id);
            results.push(write_error_result(id, &e));
        }
        if !discovery.is_empty() {
            let sub_requests: Vec<Vec<u8>> = discovery
                .iter()
                .map(|d| cip::build_read_tag(&d.tag))
                .collect();
            let chunks = batch::chunk_multi(
                &sub_requests,
                self.config.max_services_per_multi,
                self.config.max_bytes_per_multi,
            );
            let mut cursor = 0usize;
            for chunk in &chunks {
                let multi = cip::build_multi(chunk);
                let reply = match self.exchange_checked(&multi) {
                    Ok(r) => r,
                    Err(e) => {
                        // 类型发现整体失败：这批写项不能安全编码，预填
                        // 失败并继续后续阶段（不中断可写项）。
                        for d in &discovery[cursor..cursor + chunk.len()] {
                            settled.insert(d.id);
                            results.push(write_error_result(
                                d.id,
                                &EtherIpError::decode_error(format!("类型发现失败：{}", e.message)),
                            ));
                        }
                        cursor += chunk.len();
                        continue;
                    }
                };
                let sub_replies = cip::parse_multi_reply(&reply[4..], chunk.len())?;
                for (i, sub_reply) in sub_replies.iter().enumerate() {
                    let d = &discovery[cursor + i];
                    let parsed_status = sub_reply.bytes.get(2).copied().unwrap_or(0xFF);
                    let type_code =
                        if parsed_status == cip::STATUS_SUCCESS && sub_reply.bytes.len() >= 6 {
                            Some(u16::from_le_bytes([sub_reply.bytes[4], sub_reply.bytes[5]]))
                        } else {
                            None
                        };
                    match type_code {
                        Some(tc) => {
                            self.type_cache.insert(d.tag.clone(), tc);
                        }
                        None => {
                            settled.insert(d.id);
                            results
                                .push(write_error_result(d.id, &item_status_error(parsed_status)));
                        }
                    }
                }
                cursor += chunk.len();
            }
        }

        // ── 阶段 2：重新分流（缓存已补全）→ 编码写入 ──
        let (_, ready, stage2_failed) = plan_write_stage1(items, &self.type_cache)?;
        for (id, e) in stage2_failed {
            if settled.insert(id) {
                results.push(write_error_result(id, &e));
            }
        }
        // ready 现在包含全部可写项（含阶段 1 刚发现类型的）。重建
        // id 关联：plan_write_stage2 按 ready 顺序产出子请求。
        let chunks = plan_write_stage2(
            &ready,
            self.config.max_services_per_multi,
            self.config.max_bytes_per_multi,
        );
        let mut cursor = 0usize;
        for chunk in &chunks {
            let multi = cip::build_multi(chunk);
            // 写入传输级失败：整体返回（已产出的逐项结果一并丢弃——
            // 上层按整体失败重试，WAL/控制层语义不受影响）。
            let reply = self.exchange_checked(&multi)?;
            let sub_replies = cip::parse_multi_reply(&reply[4..], chunk.len())?;
            for (i, sub_reply) in sub_replies.iter().enumerate() {
                let w = &ready[cursor + i];
                let status = sub_reply.bytes.get(2).copied().unwrap_or(0xFF);
                if settled.contains(&w.id) {
                    continue;
                }
                settled.insert(w.id);
                if status == cip::STATUS_SUCCESS {
                    results.push(RawWriteResult {
                        item_id: w.id,
                        success: true,
                        protocol_code: Some(0),
                        error: None,
                    });
                } else {
                    results.push(write_error_result(w.id, &item_status_error(status)));
                }
            }
            cursor += chunk.len();
        }

        // 未在任何阶段产出结果的项（理论上仅剩"重复地址多项"已被
        // settled 去重覆盖）不额外处理。
        results.sort_by_key(|r| r.item_id);
        Ok(results)
    }

    /// 校验并规范化地址（§15 `validate_address`；canonical 大小写保真）。
    fn validate_address(
        &mut self,
        address: &str,
    ) -> Result<driver_sdk::AddressMetadata, EtherIpError> {
        let parsed = address::parse(address)
            .map_err(|e| EtherIpError::invalid_address(format!("{address}: {e}")))?;
        Ok(driver_sdk::AddressMetadata {
            canonical_address: parsed.raw,
            // validate 为离线操作：CIP 标签类型须在线查询（浏览能力
            // V0.3 不做），解释完全交给 Profile 的 expected_type。
            raw_type: None,
            readable: true,
            writable: true,
        })
    }
}

/// 读取规划的条目（tag + 上层关联信息，保持规划顺序）。
struct ReadEntry {
    tag: String,
    item_id: u64,
    expected: Option<observation_model::DataType>,
}

/// 解析读取项为规划条目（canonical 升序——确定性子服务顺序）。
///
/// # Errors
///
/// 任一地址解析失败返回 `invalid_address`。
fn plan_read_entries(items: &[driver_sdk::DriverReadItem]) -> Result<Vec<ReadEntry>, EtherIpError> {
    let mut entries: Vec<ReadEntry> = Vec::with_capacity(items.len());
    for item in items {
        let path = address::parse(&item.address)
            .map_err(|e| EtherIpError::invalid_address(format!("{}: {e}", item.address)))?;
        entries.push(ReadEntry {
            tag: path.raw,
            item_id: item.id,
            expected: item.expected_type.clone(),
        });
    }
    entries.sort_by(|a, b| a.tag.cmp(&b.tag));
    Ok(entries)
}

/// 子服务 general status → 错误分类。
fn item_status_error(status: u8) -> EtherIpError {
    if status == cip::STATUS_ACCESS_DENIED {
        EtherIpError::access_denied(status)
    } else {
        EtherIpError::cip_item_error(status)
    }
}

/// 组装单条读取结果：从子应答解析并解码。
fn assemble_read_with(
    item_id: u64,
    expected: Option<observation_model::DataType>,
    sub_reply: &[u8],
) -> RawReadResult {
    let now = now_ns();
    let parsed = match cip::parse_read_reply(sub_reply.get(2..).unwrap_or(&[])) {
        Ok(p) => p,
        Err(e) => return read_error_result(item_id, &e, now),
    };
    if parsed.status != cip::STATUS_SUCCESS {
        return read_error_result(item_id, &item_status_error(parsed.status), now);
    }
    let type_code = parsed.type_code.unwrap_or(0);
    let payload = parsed.payload.unwrap_or(&[]);
    match decode::decode_read(type_code, expected, payload) {
        Ok(value) => RawReadResult {
            item_id,
            value: Some(value),
            source_timestamp_ns: None,
            received_timestamp_ns: now,
            protocol_quality_code: Some(0),
            error: None,
        },
        Err(e) => read_error_result(item_id, &e, now),
    }
}

fn read_error_result(item_id: u64, error: &EtherIpError, now: TimestampNs) -> RawReadResult {
    RawReadResult {
        item_id,
        value: None,
        source_timestamp_ns: None,
        received_timestamp_ns: now,
        protocol_quality_code: error.protocol_code,
        error: Some(error.clone().into_info()),
    }
}

fn write_error_result(item_id: u64, error: &EtherIpError) -> RawWriteResult {
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
fn driver_mut<'a>(handle: DriverHandle) -> Option<&'a mut EtherIpDriver> {
    if handle.ptr.is_null() {
        return None;
    }
    Some(unsafe { &mut *(handle.ptr as *mut EtherIpDriver) })
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
                    EtherIpError::driver_panic("内部 panic 已被 ABI 边界捕获（§17.7）".to_owned())
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
            Some(json) => match EtherIpDriver::create(json) {
                Ok(driver) => (0, Some(driver), None),
                Err(e) => (-1, None, Some(e)),
            },
            None => (
                -1,
                None,
                Some(EtherIpError::config_error("config 非 UTF-8".to_owned())),
            ),
        };
        let mut driver = match driver {
            Some(driver) => driver,
            None => EtherIpDriver {
                config: EnIpConfig::default(),
                session: None,
                type_cache: HashMap::new(),
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
            drop(unsafe { Box::from_raw(handle.ptr as *mut EtherIpDriver) });
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
                driver.last_error = Some(EtherIpError::unsupported("能力声明序列化").into_info());
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
                    Some(EtherIpError::invalid_address("地址非 UTF-8".to_owned()).into_info());
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
            driver.last_error = Some(
                EtherIpError::invalid_address("items 指针为空但长度非 0".to_owned()).into_info(),
            );
            return -1;
        }
        let mut read_items: Vec<driver_sdk::DriverReadItem> = Vec::with_capacity(len);
        for i in 0..len {
            let item = unsafe { &*items.add(i) };
            // 复杂类型 Tag 与未知 Tag 必须整体失败（invalid_type，§17.2）：
            // 不得静默降级为"未指定类型"。
            let expected_type = match driver_sdk::abi::tag::tag_to_data_type(item.expected_type) {
                Ok(t) => t,
                Err(e) => {
                    driver.last_error = Some(
                        EtherIpError::invalid_type(format!(
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
/// §17.2 标量编码的值：Tag 非法必须整体失败，不得静默降级。
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
                EtherIpError::invalid_address("items 指针为空但长度非 0".to_owned()).into_info(),
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
                        EtherIpError::invalid_type(format!(
                            "item {} 写入值 Tag 非法：{e}",
                            item.id
                        ))
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
        let e = EtherIpError::unsupported("execute");
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
        let e = EtherIpError::unsupported("browse");
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
        let e = EtherIpError::unsupported("subscribe");
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
        let e = EtherIpError::unsupported("unsubscribe");
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
        let e = EtherIpError::unsupported("query_history");
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
