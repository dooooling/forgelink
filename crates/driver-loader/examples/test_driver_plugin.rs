//! driver-loader 集成测试用 Native Plugin（cdylib）。
//!
//! 通过 libloading 加载 `target/debug/examples/test_driver_plugin.{dll,so}`
//! （Windows 下为 `test_driver_plugin.dll`，Linux 下为
//! `libtest_driver_plugin.so`）。
//!
//! 提供多个入口符号覆盖加载校验场景：
//!
//! - `forgelink_driver_entry_v1`：完整正常函数表；
//! - `forgelink_driver_entry_v1_null`：返回空指针；
//! - `forgelink_driver_entry_v1_bad_abi`：`abi_major = 2`；
//! - `forgelink_driver_entry_v1_bad_abi_minor`：`abi_minor = 99`；
//! - `forgelink_driver_entry_v1_small_struct`：`struct_size` 不足；
//! - `forgelink_driver_entry_v1_missing_function`：`free_buffer` 为 null。
//!
//! `create` 的 `config` 支持简单开关（测试约定，非 Envelope）：
//! 包含 `"fail_create"` 时 `create` 返回 -1；包含 `"fail_connect"` 时
//! `connect` 返回 -1；其余为正常模式。

use std::ffi::c_void;
use std::mem::{offset_of, size_of};

use driver_sdk::abi::envelope::{
    AddressEnvelope, BrowseEnvelope, CapabilitiesEnvelope, ErrorEnvelope, ExecuteEnvelope,
    HistoryEnvelope, ReadEnvelope, WriteEnvelope,
};
use driver_sdk::abi::{
    ABI_MAJOR, ABI_MINOR, DriverApiV1, DriverHandle, FfiEventCallback, FfiOwnedBuffer, FfiReadItem,
    FfiStr, FfiWriteItem,
};
use driver_sdk::{
    AddressMetadata, DriverBrowseNode, DriverErrorInfo, ProtocolCapabilities, RawCommandResult,
    RawHistoryPage, RawReadResult, RawValue, RawWriteResult,
};

/// 插件句柄状态。
struct PluginState {
    mode: Mode,
    last_error: Option<DriverErrorInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Normal,
    FailCreate,
    FailConnect,
}

const API: DriverApiV1 = DriverApiV1 {
    struct_size: size_of::<DriverApiV1>() as u32,
    abi_major: ABI_MAJOR,
    abi_minor: ABI_MINOR,
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

/// 与 `DriverApiV1` 字节布局一致的原始函数表（坏变体测试专用）。
///
/// 函数指针槽位以 `usize` 表示：Rust 函数指针不允许为 null，任何
/// 构造空 Rust 函数指针值的做法（union/transmute/zeroed）都是 UB，
/// 因此坏变体通过本结构存放空槽位（`0`），Loader 按 §17.9 以 `usize`
/// 读取校验，从不把该槽位当作 Rust 函数指针值使用。
#[repr(C)]
#[derive(Clone, Copy)]
struct RawApiTable {
    struct_size: u32,
    abi_major: u16,
    abi_minor: u16,
    feature_flags: u64,
    create: usize,
    destroy: usize,
    connect: usize,
    disconnect: usize,
    get_capabilities_json: usize,
    validate_address: usize,
    read: usize,
    write: usize,
    execute: usize,
    browse: usize,
    subscribe: usize,
    unsubscribe: usize,
    query_history: usize,
    get_last_error_json: usize,
    free_buffer: usize,
}

// 布局一致性编译期断言：`as *const RawApiTable as *const DriverApiV1`
// 依赖两结构字节布局完全相同（fn 指针大小/对齐 = usize）。
const _: () = {
    assert!(size_of::<RawApiTable>() == size_of::<DriverApiV1>());
    assert!(align_of::<RawApiTable>() == align_of::<DriverApiV1>());
    assert!(offset_of!(RawApiTable, free_buffer) == offset_of!(DriverApiV1, free_buffer));
};

/// 构造与正常函数表布局一致的原始表（函数地址以 `usize` 存放）。
fn raw_api_table() -> RawApiTable {
    RawApiTable {
        struct_size: size_of::<DriverApiV1>() as u32,
        abi_major: ABI_MAJOR,
        abi_minor: ABI_MINOR,
        feature_flags: 0,
        create: api_create as *const () as usize,
        destroy: api_destroy as *const () as usize,
        connect: api_connect as *const () as usize,
        disconnect: api_disconnect as *const () as usize,
        get_capabilities_json: api_get_capabilities_json as *const () as usize,
        validate_address: api_validate_address as *const () as usize,
        read: api_read as *const () as usize,
        write: api_write as *const () as usize,
        execute: api_execute as *const () as usize,
        browse: api_browse as *const () as usize,
        subscribe: api_subscribe as *const () as usize,
        unsubscribe: api_unsubscribe as *const () as usize,
        query_history: api_query_history as *const () as usize,
        get_last_error_json: api_get_last_error_json as *const () as usize,
        free_buffer: api_free_buffer as *const () as usize,
    }
}

/// 正常入口（§16 唯一入口符号）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1() -> *const DriverApiV1 {
    &API
}

/// 空指针入口（测试 NullEntry 校验）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1_null() -> *const DriverApiV1 {
    std::ptr::null()
}

/// `abi_major` 不兼容（测试 AbiIncompatible，§18）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1_bad_abi() -> *const DriverApiV1 {
    let mut table = raw_api_table();
    table.abi_major = 2;
    Box::leak(Box::new(table)) as *const RawApiTable as *const DriverApiV1
}

/// `abi_minor` 超出 Core 支持（测试 AbiIncompatible，§18）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1_bad_abi_minor() -> *const DriverApiV1 {
    let mut table = raw_api_table();
    table.abi_minor = 99;
    Box::leak(Box::new(table)) as *const RawApiTable as *const DriverApiV1
}

/// `struct_size` 不足（测试 StructTooSmall，§17.4）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1_small_struct() -> *const DriverApiV1 {
    let mut table = raw_api_table();
    table.struct_size = (size_of::<DriverApiV1>() / 2) as u32;
    Box::leak(Box::new(table)) as *const RawApiTable as *const DriverApiV1
}

/// `free_buffer` 槽位为 0（测试 MissingFunction，§17.9）。
#[unsafe(no_mangle)]
pub extern "C" fn forgelink_driver_entry_v1_missing_function() -> *const DriverApiV1 {
    let mut table = raw_api_table();
    // 空槽位存放在 `usize` 字段（合法值），Loader 以 `usize` 读取校验，
    // 从不构造 Rust 空函数指针值（那是 UB）。
    table.free_buffer = 0;
    Box::leak(Box::new(table)) as *const RawApiTable as *const DriverApiV1
}

// ---------------------------------------------------------------- create

unsafe extern "C" fn api_create(config: FfiStr, out_handle: *mut DriverHandle) -> i32 {
    let config = unsafe { ffi_str_to_str(config) }.unwrap_or("");
    let (status, mode, last_error, write_handle) = if config.contains("fail_create_null") {
        // 失败且不写入句柄（保持调用方初值 null）——验证 Loader 不把
        // 空句柄传给 get_last_error_json/destroy。
        (
            -1,
            Mode::FailCreate,
            Some(DriverErrorInfo {
                code: "CONFIG_INVALID".to_owned(),
                message: "test plugin: config requests fail_create_null".to_owned(),
                protocol_code: Some(-1),
                retryable: false,
            }),
            false,
        )
    } else if config.contains("fail_create") {
        // 失败但句柄有效——验证 Loader 读取详情并 destroy 清理。
        (
            -1,
            Mode::FailCreate,
            Some(DriverErrorInfo {
                code: "CONFIG_INVALID".to_owned(),
                message: "test plugin: config requests fail_create".to_owned(),
                protocol_code: Some(-1),
                retryable: false,
            }),
            true,
        )
    } else if config.contains("fail_connect") {
        (0, Mode::FailConnect, None, true)
    } else {
        (0, Mode::Normal, None, true)
    };
    if write_handle {
        let state = Box::new(PluginState { mode, last_error });
        unsafe {
            *out_handle = DriverHandle {
                ptr: Box::into_raw(state) as *mut c_void,
            };
        }
    }
    status
}

unsafe extern "C" fn api_destroy(handle: DriverHandle) -> i32 {
    if !handle.ptr.is_null() {
        unsafe {
            drop(Box::from_raw(handle.ptr as *mut PluginState));
        }
    }
    0
}

unsafe extern "C" fn api_connect(handle: DriverHandle) -> i32 {
    let state = unsafe { &mut *(handle.ptr as *mut PluginState) };
    if state.mode == Mode::FailConnect {
        state.last_error = Some(DriverErrorInfo {
            code: "CONNECT_REFUSED".to_owned(),
            message: "test plugin: connection refused".to_owned(),
            protocol_code: Some(0x01),
            retryable: true,
        });
        return -1;
    }
    0
}

unsafe extern "C" fn api_disconnect(_handle: DriverHandle) -> i32 {
    0
}

// ------------------------------------------------------------ capabilities

unsafe extern "C" fn api_get_capabilities_json(
    _handle: DriverHandle,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let envelope = CapabilitiesEnvelope::new(ProtocolCapabilities::default());
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// -------------------------------------------------------- validate_address

unsafe extern "C" fn api_validate_address(
    _handle: DriverHandle,
    address: FfiStr,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let address = unsafe { ffi_str_to_str(address) }.unwrap_or_default();
    let envelope = AddressEnvelope::new(AddressMetadata {
        canonical_address: address.to_owned(),
        raw_type: None,
        readable: true,
        writable: false,
    });
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// ------------------------------------------------------------------ read

unsafe extern "C" fn api_read(
    _handle: DriverHandle,
    items: *const FfiReadItem,
    len: usize,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let results: Vec<RawReadResult> = (0..len)
        .map(|i| {
            let item = unsafe { &*items.add(i) };
            RawReadResult {
                item_id: item.id,
                value: Some(RawValue::U64(1000 + item.id)),
                source_timestamp_ns: None,
                received_timestamp_ns: 0,
                protocol_quality_code: Some(0),
                error: None,
            }
        })
        .collect();
    let envelope = ReadEnvelope::new(results);
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// ----------------------------------------------------------------- write

unsafe extern "C" fn api_write(
    _handle: DriverHandle,
    items: *const FfiWriteItem,
    len: usize,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let results: Vec<RawWriteResult> = (0..len)
        .map(|i| {
            let item = unsafe { &*items.add(i) };
            RawWriteResult {
                item_id: item.id,
                success: true,
                protocol_code: Some(0),
                error: None,
            }
        })
        .collect();
    let envelope = WriteEnvelope::new(results);
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// --------------------------------------------------------------- execute

unsafe extern "C" fn api_execute(
    _handle: DriverHandle,
    _command_json: FfiStr,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let envelope = ExecuteEnvelope::new(RawCommandResult {
        success: true,
        protocol_code: Some(0),
        payload: None,
        error: None,
    });
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// ---------------------------------------------------------------- browse

unsafe extern "C" fn api_browse(
    _handle: DriverHandle,
    _path: FfiStr,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let envelope = BrowseEnvelope::new(vec![DriverBrowseNode {
        id: "root".to_owned(),
        display_name: "Root".to_owned(),
        address: None,
        has_children: false,
        metadata: serde_json::json!({}),
    }]);
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// ------------------------------------------------------------ subscribe

unsafe extern "C" fn api_subscribe(
    _handle: DriverHandle,
    _request_json: FfiStr,
    _callback: FfiEventCallback,
    _user_data: *mut c_void,
    out_subscription_id: *mut u64,
) -> i32 {
    if out_subscription_id.is_null() {
        return -1;
    }
    unsafe {
        *out_subscription_id = 1;
    }
    0
}

unsafe extern "C" fn api_unsubscribe(_handle: DriverHandle, _subscription_id: u64) -> i32 {
    0
}

// --------------------------------------------------------- query_history

unsafe extern "C" fn api_query_history(
    _handle: DriverHandle,
    _request_json: FfiStr,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let envelope = HistoryEnvelope::new(RawHistoryPage {
        items: vec![],
        continuation: None,
    });
    write_buffer(out, &serde_json::to_vec(&envelope).expect("序列化失败"));
    0
}

// -------------------------------------------------------------- last_error

unsafe extern "C" fn api_get_last_error_json(
    handle: DriverHandle,
    out: *mut FfiOwnedBuffer,
) -> i32 {
    let state = unsafe { &*(handle.ptr as *mut PluginState) };
    match &state.last_error {
        Some(error) => {
            write_buffer(
                out,
                &serde_json::to_vec(&ErrorEnvelope::from(error)).expect("序列化失败"),
            );
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
}

// ------------------------------------------------------------- free_buffer

unsafe extern "C" fn api_free_buffer(buffer: FfiOwnedBuffer) {
    if !buffer.ptr.is_null() {
        // 由 write_buffer 的 Vec 重建并释放（§17.3 谁分配谁释放）。
        unsafe {
            drop(Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.capacity));
        }
    }
}

// ----------------------------------------------------------------- helpers

/// 把借用 FfiStr 读为 `&str`（§17.1：len == 0 时 ptr 可为 null）。
///
/// # Safety
///
/// 调用方必须保证 `ffi.ptr` 在返回值使用期间有效（调用期借用，§17.1）；
/// 生命周期参数由调用方选择，不越过调用边界。
unsafe fn ffi_str_to_str<'a>(ffi: FfiStr) -> Option<&'a str> {
    if ffi.len == 0 {
        return Some("");
    }
    if ffi.ptr.is_null() {
        return None;
    }
    // Safety: 由调用方保证 ptr 指向 len 字节可读内存（§17.1）。
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
