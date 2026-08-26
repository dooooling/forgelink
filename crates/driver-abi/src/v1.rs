//! Driver C ABI v1 类型（§16、§17 Normative）。
//!
//! # 总则（§16）
//!
//! - 唯一入口：`forgelink_driver_entry_v1()`，返回 `*const DriverApiV1`。
//! - 禁止跨 ABI 暴露：Rust String / Vec、Rust enum 默认布局、`Box<dyn Trait>`、
//!   Future、`Result<T, E>`、Rust panic unwind。
//! - 所有跨 ABI 类型必须 `#[repr(C)]` 或由明确的 `ptr + len` 组成。
//!
//! # 固定契约（ABI v1）
//!
//! - 类型 Tag 表与 `value_bytes` 标量编码（小端）：`abi::tag`（§17.2）。
//! - 所有 JSON payload 的 Envelope schema：`abi::envelope`（§17.2、§17.9）。
//!
//! # 内存所有权（§17.3）
//!
//! 谁分配，谁释放：Plugin 返回的 owned buffer 必须通过 `free_buffer` 释放；
//! 请求参数默认由 Core 持有，Plugin 只能在调用期内借用。

use std::ffi::c_void;

/// ABI 主版本（§18）：`Core 1.x 可加载 Plugin 1.0 ~ 1.x，不能加载 2.x`。
pub const ABI_MAJOR: u16 = 1;

/// ABI 次版本（§18）：同一 major 内 append-only，只能在结构尾部追加字段。
pub const ABI_MINOR: u16 = 0;

/// Native Plugin 唯一入口符号名（§16、§20 Manifest `entry` 字段）。
pub const ENTRY_SYMBOL: &str = "forgelink_driver_entry_v1";

/// 字符串与数组（§17.1）。
///
/// 字符串统一为 UTF-8，不要求 NUL 结尾：
/// - `len > 0` 时 `ptr` 必须非空；
/// - `len == 0` 时允许 `ptr = null`；
/// - 默认是 borrowed，仅在当前函数调用期间有效。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FfiStr {
    pub ptr: *const u8,
    pub len: usize,
}

impl FfiStr {
    /// 构造空字符串（`len == 0, ptr = null`，§17.1）。
    pub const fn empty() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }
}

/// 数组视图（§17.1）：统一使用 `ptr + len`，禁止 sentinel 结束方式。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FfiSlice<T> {
    pub ptr: *const T,
    pub len: usize,
}

/// ABI 读取请求元素（§17.2）。
///
/// `expected_type` 为 ABI v1 固定的数值 Tag（`abi::tag::TypeTag`，§17.2）；
/// `Array`/`Struct` Tag 仅表示"期望复杂值"，元素/字段 schema 由 Profile 提供。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FfiReadItem {
    pub id: u64,
    pub address: FfiStr,
    pub expected_type: u32,
}

/// ABI 写入请求元素（§17.2）。
///
/// `value_type` 为 ABI v1 固定的数值 Tag（`abi::tag::TypeTag`）；
/// `value_bytes` 使用 `abi::tag` 规定的标量编码（定宽小端，长度必须精确匹配）；
/// `Array`/`Struct` 复杂值不允许写入（§17.2 只定义标量编码，ABI v1 无复杂写入通道）。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FfiWriteItem {
    pub id: u64,
    pub address: FfiStr,
    pub value_type: u32,
    pub value_bytes: FfiStr,
}

/// Plugin 返回的 owned buffer（§17.3）。
///
/// 由 Plugin 分配，必须通过 `DriverApiV1::free_buffer` 释放，Core 不得直接释放。
/// 独占所有权：不可复制、不可克隆（`Clone`/`Copy` 会导致重复释放）。
#[repr(C)]
#[derive(Debug)]
pub struct FfiOwnedBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

/// 插件句柄（§17.5）。
///
/// 默认模型：一个 Handle 非重入、非并发安全，Core 默认串行调用；
/// 只有 Plugin 显式声明 `THREAD_SAFE_HANDLE` 后才允许并发调用。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DriverHandle {
    pub ptr: *mut c_void,
}

/// 订阅/事件回调（§17.8）。
///
/// - callback 函数指针与 `user_data` 由 Core 提供。
/// - 从 `subscribe()` 成功返回到对应 `unsubscribe()` 开始生效；
///   `unsubscribe()` 返回后 Plugin 不得再次调用。
/// - `event_json` 只在回调调用期间有效，Core 如需长期保存必须复制。
pub type FfiEventCallback = extern "C" fn(user_data: *mut c_void, event_json: FfiStr);

/// Driver ABI v1 函数表（§17.9 Normative）。
///
/// # Safety
///
/// 所有函数指针均为 `unsafe extern "C" fn`：调用方（driver-loader）必须保证
/// - `DriverHandle` 由 `create` 产生且未被 `destroy` 释放，同一时刻没有并发调用；
/// - `FfiStr`/`FfiSlice` 的 `ptr` 指向有效、对齐且 `len` 匹配的可读内存
///   （`len == 0` 时 `ptr` 允许为 `null`）；
/// - `FfiReadItem`/`FfiWriteItem` 数组的 `ptr + len` 为一个完整有效数组；
/// - `out_handle`/`out`/`out_subscription_id` 指向有效可写内存；
/// - 函数成功返回后，`FfiOwnedBuffer` 只能被 `free_buffer` 释放一次；
/// - `callback` 与 `user_data` 的生命周期覆盖 `subscribe` 成功返回到
///   `unsubscribe` 返回的全程，Plugin 不得在卸载后回调。
///
/// # 兼容规则（§17.4、§18）
///
/// - `abi_major` 必须完全一致；`plugin.abi_minor <= core` 支持的 minor。
/// - 同一 major 内只能在结构尾部追加字段；Core 必须通过 `struct_size` 判断字段是否存在。
/// - 删除、重排、改变字段含义 => ABI major + 1。
///
/// # 错误语义（§17.6）
///
/// 函数返回稳定 `i32` 状态码：`0 = OK`，`>0 = ForgeLink 标准错误`，
/// `<0 = Driver/Protocol 错误类别`；详细错误通过 `get_last_error_json` 获取。
#[repr(C)]
pub struct DriverApiV1 {
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub feature_flags: u64,

    /// `config` 为 UTF-8 连接配置 JSON。
    pub create: unsafe extern "C" fn(config: FfiStr, out_handle: *mut DriverHandle) -> i32,
    pub destroy: unsafe extern "C" fn(handle: DriverHandle) -> i32,
    pub connect: unsafe extern "C" fn(handle: DriverHandle) -> i32,
    pub disconnect: unsafe extern "C" fn(handle: DriverHandle) -> i32,

    /// 返回 UTF-8 JSON 能力声明（`abi::envelope::CapabilitiesEnvelope`）。
    pub get_capabilities_json:
        unsafe extern "C" fn(handle: DriverHandle, out: *mut FfiOwnedBuffer) -> i32,
    pub validate_address: unsafe extern "C" fn(
        handle: DriverHandle,
        address: FfiStr,
        out: *mut FfiOwnedBuffer,
    ) -> i32,

    /// 读取；`out` 为 `abi::envelope::ReadEnvelope`（§17.2）。
    pub read: unsafe extern "C" fn(
        handle: DriverHandle,
        items: *const FfiReadItem,
        len: usize,
        out: *mut FfiOwnedBuffer,
    ) -> i32,
    pub write: unsafe extern "C" fn(
        handle: DriverHandle,
        items: *const FfiWriteItem,
        len: usize,
        out: *mut FfiOwnedBuffer,
    ) -> i32,
    /// 执行协议命令；`command_json` 为 `abi::envelope::ExecuteRequestEnvelope`。
    pub execute: unsafe extern "C" fn(
        handle: DriverHandle,
        command_json: FfiStr,
        out: *mut FfiOwnedBuffer,
    ) -> i32,
    pub browse:
        unsafe extern "C" fn(handle: DriverHandle, path: FfiStr, out: *mut FfiOwnedBuffer) -> i32,

    pub subscribe: unsafe extern "C" fn(
        handle: DriverHandle,
        request_json: FfiStr,
        callback: FfiEventCallback,
        user_data: *mut c_void,
        out_subscription_id: *mut u64,
    ) -> i32,
    pub unsubscribe: unsafe extern "C" fn(handle: DriverHandle, subscription_id: u64) -> i32,
    pub query_history: unsafe extern "C" fn(
        handle: DriverHandle,
        request_json: FfiStr,
        out: *mut FfiOwnedBuffer,
    ) -> i32,

    /// 详细错误 JSON（§17.6 固定形状 `abi::envelope::ErrorEnvelope`），
    /// 返回的 buffer 必须由 `free_buffer` 释放。
    pub get_last_error_json:
        unsafe extern "C" fn(handle: DriverHandle, out: *mut FfiOwnedBuffer) -> i32,
    /// 释放 Plugin 分配的 owned buffer（§17.3）。
    pub free_buffer: unsafe extern "C" fn(buffer: FfiOwnedBuffer),
}
