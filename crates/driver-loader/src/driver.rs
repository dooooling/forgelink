//! NativeDriver：对已加载 Plugin 的同步调用适配（§17.9 最小函数表、§15）。

use std::slice;
use std::sync::Arc;

use driver_sdk::abi::envelope::{
    AddressEnvelope, BrowseEnvelope, CapabilitiesEnvelope, ErrorEnvelope, ExecuteEnvelope,
    ExecuteRequestEnvelope, HistoryEnvelope, HistoryRequestEnvelope, ReadEnvelope, WriteEnvelope,
};
use driver_sdk::abi::tag::{TypeTag, data_type_to_tag, encode_value_bytes};
use driver_sdk::abi::{
    DriverApiV1, DriverHandle, FfiOwnedBuffer, FfiReadItem, FfiStr, FfiWriteItem,
};
use driver_sdk::{
    AddressMetadata, DriverBrowseNode, DriverCommand, DriverErrorInfo, DriverReadItem,
    DriverWriteItem, HistoryRequest, ProtocolCapabilities, RawCommandResult, RawHistoryPage,
    RawReadResult, RawValue, RawWriteResult,
};
use serde::Serialize;
use tracing::{debug, warn};

use crate::error::LoaderError;
use crate::plugin::NativePlugin;

/// 已创建句柄的同步驱动适配器。
///
/// # Safety 论证
///
/// - **库生命周期**：`handle` 必须声明在 `plugin` 之前——字段按声明顺序
///   Drop，先 Drop `handle`（普通 Copy 值，不访问函数表），随后
///   `Arc<NativePlugin>` 计数归零才可能卸载库；而本结构 Drop 时
///   显式调用 `destroy` 依赖函数指针，此时 `plugin` 必然仍存活。
/// - **句柄所有权**：`handle` 由 `create` 产生、只在本结构中存在，
///   `destroy` 恰好执行一次（Drop），不存在重复释放。
/// - **调用串行**：句柄默认非并发安全（§17.5）；所有方法接收 `&mut self`，
///   借用规则保证同一实例的调用串行化。
/// - **panic 边界（§17.7）**：跨 FFI 的 panic 由 Plugin 侧 catch_unwind
///   收口；本适配器只做入参校验与快速失败，不尝试捕获。
pub struct NativeDriver {
    handle: DriverHandle,
    plugin: Arc<NativePlugin>,
    capabilities: Option<ProtocolCapabilities>,
    connected: bool,
}

// Safety: `handle` 为不透明指针（`DriverHandle.ptr`）。句柄默认非重入、非并发
// 安全（§17.5），本结构所有方法均接收 `&mut self` 串行化调用；跨线程**转移**
// （Send）本身不引入并发调用。`plugin`（`Arc<NativePlugin>`）内部的
// `libloading::Library` 与 `&'static DriverApiV1` 均为 `Send + Sync`。
unsafe impl Send for NativeDriver {}

/// Plugin 分配的 owned buffer 的 RAII 包装（§17.3 谁分配谁释放）。
///
/// Drop 时通过 `free_buffer` 释放；不可复制，独占所有权。
struct OwnedFfiBuffer {
    buffer: FfiOwnedBuffer,
    free: unsafe extern "C" fn(FfiOwnedBuffer),
}

impl OwnedFfiBuffer {
    /// 包装 Plugin 返回的 buffer（`ptr` 已校验非空）。
    fn take(api: &DriverApiV1, buffer: FfiOwnedBuffer) -> Self {
        Self {
            buffer,
            free: api.free_buffer,
        }
    }

    /// 只读字节视图（`buffer` 存活期间有效）。
    fn as_bytes(&self) -> &[u8] {
        // Safety: buffer 由 Plugin 分配并声明为 `len` 字节可读（§17.3）；
        // 结构自身持有所有权，视图生命周期不越过结构。
        unsafe { slice::from_raw_parts(self.buffer.ptr as *const u8, self.buffer.len) }
    }

    /// 按 UTF-8 读取内容。
    fn as_str(&self, function: &'static str) -> Result<&str, LoaderError> {
        std::str::from_utf8(self.as_bytes()).map_err(|_| LoaderError::InvalidUtf8 { function })
    }
}

impl Drop for OwnedFfiBuffer {
    fn drop(&mut self) {
        if !self.buffer.ptr.is_null() {
            // Safety: buffer 由 Plugin 分配（§17.3），独占所有权，
            // RAII 保证每个 buffer 恰好释放一次。逐字段复制实现
            // 转移语义（FfiOwnedBuffer 无 Drop 逻辑，不会重复释放）。
            unsafe {
                (self.free)(FfiOwnedBuffer {
                    ptr: self.buffer.ptr,
                    len: self.buffer.len,
                    capacity: self.buffer.capacity,
                })
            };
        }
    }
}

/// 构造空 owned buffer（调用前初始化 out 参数）。
fn empty_buffer() -> FfiOwnedBuffer {
    FfiOwnedBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
        capacity: 0,
    }
}

/// 把借用字符串包装为调用期间有效的 `FfiStr`（§17.1）。
fn ffi_str(s: &str) -> FfiStr {
    FfiStr {
        ptr: s.as_ptr(),
        len: s.len(),
    }
}

/// 由 `RawValue` 推断 ABI 类型 Tag（复杂类型不允许写入，§17.2）。
fn tag_for_value(value: &RawValue) -> Result<TypeTag, LoaderError> {
    Ok(match value {
        RawValue::Bool(_) => TypeTag::Bool,
        RawValue::I64(_) => TypeTag::I64,
        RawValue::U64(_) => TypeTag::U64,
        RawValue::F64(_) => TypeTag::F64,
        RawValue::String(_) => TypeTag::String,
        RawValue::Bytes(_) => TypeTag::Bytes,
        RawValue::Array(_) | RawValue::Struct(_) => {
            return Err(LoaderError::Encoding(
                "复杂类型(Array/Struct)不允许写入 value_bytes（§17.2 只定义标量编码）".to_owned(),
            ));
        }
    })
}

/// 序列化请求 Envelope 为调用期间有效的 JSON 字符串。
fn encode_request<T: Serialize>(value: &T) -> Result<String, LoaderError> {
    serde_json::to_string(value).map_err(|source| LoaderError::Encoding(source.to_string()))
}

impl NativeDriver {
    /// 在已加载插件上创建驱动句柄（§17.9 `create`）。
    ///
    /// `config` 为 UTF-8 连接配置 JSON，内容由 Driver 定义。
    ///
    /// # Errors
    ///
    /// - [`LoaderError::CreateFailed`]：`create` 返回非零状态码
    ///   （`detail` 来自 `get_last_error_json`）；
    /// - [`LoaderError::InvalidHandle`]：`create` 成功但返回空句柄。
    pub fn create(plugin: Arc<NativePlugin>, config: &str) -> Result<Self, LoaderError> {
        let api = plugin.api();
        let mut handle = DriverHandle {
            ptr: std::ptr::null_mut(),
        };
        // Safety: config 为调用期间有效的 UTF-8 借用（§17.1）；
        // out_handle 指向栈上可写内存，且 create 只写一次。
        let status = unsafe { (api.create)(ffi_str(config), &mut handle) };
        if status != 0 {
            // create 失败时句柄值未定义（§17.5）：可能为空，也可能是
            // 有效句柄。仅对有效句柄读取错误详情并尽力销毁，避免
            // 把无效句柄传给 Plugin（空指针解引用）或泄漏资源。
            let detail = if !handle.ptr.is_null() {
                let detail = fetch_error(api, handle);
                // Safety: 句柄非空，destroy 只执行一次（此处未发生转移）。
                let destroy_status = unsafe { (api.destroy)(handle) };
                if destroy_status != 0 {
                    warn!(
                        component = "driver-loader",
                        driver_id = %plugin.manifest().id,
                        error_code = "driver_destroy_failed",
                        status = destroy_status,
                        "create 失败后清理句柄时 destroy 返回非零状态码"
                    );
                }
                detail
            } else {
                None
            };
            return Err(LoaderError::CreateFailed { detail });
        }
        if handle.ptr.is_null() {
            // 契约违规：create 成功但句柄为空。句柄无效无法交给 destroy 释放，
            // 只返回错误；Plugin 侧若分配了资源应由其自行在失败路径清理。
            return Err(LoaderError::InvalidHandle);
        }
        Ok(Self {
            handle,
            plugin,
            capabilities: None,
            connected: false,
        })
    }

    /// 协议层能力声明（§13.1），首次调用后缓存。
    pub fn protocol_capabilities(&mut self) -> Result<&ProtocolCapabilities, LoaderError> {
        if self.capabilities.is_none() {
            let mut out = empty_buffer();
            // Safety: handle 有效且无并发调用（&mut self）；out 指向栈上可写内存。
            let status = unsafe { (self.api().get_capabilities_json)(self.handle, &mut out) };
            if status != 0 {
                return Err(self.call_failed("get_capabilities_json", status));
            }
            let buffer = take_owned(self.api(), out, "get_capabilities_json")?;
            let json = buffer.as_str("get_capabilities_json")?;
            let envelope: CapabilitiesEnvelope =
                serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                    function: "get_capabilities_json",
                    source,
                })?;
            self.capabilities = Some(envelope.capabilities);
            debug!(
                component = "driver-loader",
                driver_id = %self.plugin.manifest().id,
                capabilities = ?self.capabilities,
                "协议能力已加载"
            );
        }
        Ok(self.capabilities.as_ref().expect("缓存已写入"))
    }

    /// 建立连接（§17.9 `connect`）。
    pub fn connect(&mut self) -> Result<(), LoaderError> {
        // Safety: handle 有效且无并发调用。
        let status = unsafe { (self.api().connect)(self.handle) };
        if status != 0 {
            return Err(self.call_failed("connect", status));
        }
        self.connected = true;
        Ok(())
    }

    /// 断开连接（§17.9 `disconnect`）。
    pub fn disconnect(&mut self) -> Result<(), LoaderError> {
        // Safety: handle 有效且无并发调用。
        let status = unsafe { (self.api().disconnect)(self.handle) };
        if status != 0 {
            return Err(self.call_failed("disconnect", status));
        }
        self.connected = false;
        Ok(())
    }

    /// 是否已连接（由 `connect`/`disconnect` 维护）。
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// 校验并规范化驱动地址（§15 `validate_address`）。
    ///
    /// 地址是 Driver 私有不透明数据，Core/Domain 不解析其含义。
    pub fn validate_address(&mut self, address: &str) -> Result<AddressMetadata, LoaderError> {
        let mut out = empty_buffer();
        // Safety: address 为调用期间有效的 UTF-8 借用。
        let status =
            unsafe { (self.api().validate_address)(self.handle, ffi_str(address), &mut out) };
        if status != 0 {
            return Err(self.call_failed("validate_address", status));
        }
        let buffer = take_owned(self.api(), out, "validate_address")?;
        let json = buffer.as_str("validate_address")?;
        let envelope: AddressEnvelope =
            serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                function: "validate_address",
                source,
            })?;
        Ok(envelope.address)
    }

    /// 批量读取（§15 `read`）；`expected_type` 为 `None` 时由驱动自行确定。
    pub fn read(&mut self, items: &[DriverReadItem]) -> Result<Vec<RawReadResult>, LoaderError> {
        let ffi_items: Vec<FfiReadItem> = items
            .iter()
            .map(|item| FfiReadItem {
                id: item.id,
                address: ffi_str(&item.address),
                expected_type: data_type_to_tag(item.expected_type.clone()),
            })
            .collect();
        let mut out = empty_buffer();
        // Safety: ffi_items 为完整有效数组（§17.1），调用期间存活；
        // len == 0 时 as_ptr 返回合法非空悬垂指针，不被访问。
        let status = unsafe {
            (self.api().read)(self.handle, ffi_items.as_ptr(), ffi_items.len(), &mut out)
        };
        if status != 0 {
            return Err(self.call_failed("read", status));
        }
        let buffer = take_owned(self.api(), out, "read")?;
        let json = buffer.as_str("read")?;
        let envelope: ReadEnvelope =
            serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                function: "read",
                source,
            })?;
        Ok(envelope.results)
    }

    /// 批量写入（§15 `write`）；`RawValue` 按 §17.2 标量编码为 `value_bytes`。
    ///
    /// `Array`/`Struct` 复杂值在 ABI v1 无写入通道，返回
    /// [`LoaderError::Encoding`]（§17.2）。
    pub fn write(&mut self, items: &[DriverWriteItem]) -> Result<Vec<RawWriteResult>, LoaderError> {
        let mut ffi_items: Vec<FfiWriteItem> = Vec::with_capacity(items.len());
        let mut value_bytes: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        for item in items {
            let tag = tag_for_value(&item.value)?;
            let bytes = encode_value_bytes(tag as u32, &item.value)
                .map_err(|e| LoaderError::Encoding(e.to_string()))?;
            ffi_items.push(FfiWriteItem {
                id: item.id,
                address: ffi_str(&item.address),
                value_type: tag as u32,
                value_bytes: FfiStr {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
                },
            });
            value_bytes.push(bytes);
        }
        let mut out = empty_buffer();
        // Safety: ffi_items 为完整有效数组；value_bytes 缓冲在调用期间存活
        // （bytes 的引用不越过本调用，见 FfiStr 借用规则 §17.1）。
        let status = unsafe {
            (self.api().write)(self.handle, ffi_items.as_ptr(), ffi_items.len(), &mut out)
        };
        if status != 0 {
            return Err(self.call_failed("write", status));
        }
        let buffer = take_owned(self.api(), out, "write")?;
        let json = buffer.as_str("write")?;
        let envelope: WriteEnvelope =
            serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                function: "write",
                source,
            })?;
        Ok(envelope.results)
    }

    /// 执行协议命令（§15 `execute`）。
    pub fn execute(&mut self, command: &DriverCommand) -> Result<RawCommandResult, LoaderError> {
        let request = encode_request(&ExecuteRequestEnvelope::new(command.clone()))?;
        let mut out = empty_buffer();
        // Safety: request 为调用期间有效的 UTF-8 借用。
        let status = unsafe { (self.api().execute)(self.handle, ffi_str(&request), &mut out) };
        if status != 0 {
            return Err(self.call_failed("execute", status));
        }
        let buffer = take_owned(self.api(), out, "execute")?;
        let json = buffer.as_str("execute")?;
        let envelope: ExecuteEnvelope =
            serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                function: "execute",
                source,
            })?;
        Ok(envelope.result)
    }

    /// 浏览协议节点（§15 `browse`）；`path` 为 `None` 时浏览根节点。
    pub fn browse(&mut self, path: Option<&str>) -> Result<Vec<DriverBrowseNode>, LoaderError> {
        let path_ffi = match path {
            Some(p) => ffi_str(p),
            None => FfiStr::empty(),
        };
        let mut out = empty_buffer();
        // Safety: path 为调用期间有效的 UTF-8 借用（None 时为空 FfiStr）。
        let status = unsafe { (self.api().browse)(self.handle, path_ffi, &mut out) };
        if status != 0 {
            return Err(self.call_failed("browse", status));
        }
        let buffer = take_owned(self.api(), out, "browse")?;
        let json = buffer.as_str("browse")?;
        let envelope: BrowseEnvelope =
            serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                function: "browse",
                source,
            })?;
        Ok(envelope.nodes)
    }

    /// 历史查询（§15 `query_history`）。
    pub fn query_history(
        &mut self,
        request: &HistoryRequest,
    ) -> Result<RawHistoryPage, LoaderError> {
        let request = encode_request(&HistoryRequestEnvelope::new(request.clone()))?;
        let mut out = empty_buffer();
        // Safety: request 为调用期间有效的 UTF-8 借用。
        let status =
            unsafe { (self.api().query_history)(self.handle, ffi_str(&request), &mut out) };
        if status != 0 {
            return Err(self.call_failed("query_history", status));
        }
        let buffer = take_owned(self.api(), out, "query_history")?;
        let json = buffer.as_str("query_history")?;
        let envelope: HistoryEnvelope =
            serde_json::from_str(json).map_err(|source| LoaderError::InvalidResponse {
                function: "query_history",
                source,
            })?;
        Ok(envelope.page)
    }

    /// 插件句柄对应的 ABI 函数表。
    fn api(&self) -> &'static DriverApiV1 {
        self.plugin.api()
    }

    /// 非零状态码 + `get_last_error_json` 详情（§17.6）。
    fn call_failed(&mut self, function: &'static str, status: i32) -> LoaderError {
        LoaderError::CallFailed {
            function,
            status,
            detail: fetch_error(self.api(), self.handle),
        }
    }
}

impl std::fmt::Debug for NativeDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeDriver")
            .field("handle", &self.handle)
            .field("plugin", &self.plugin)
            .field("connected", &self.connected)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

impl Drop for NativeDriver {
    fn drop(&mut self) {
        // Safety: handle 由 create 成功产生，未重复释放；&mut self 保证
        // 无并发调用。plugin（Arc）在本 Drop 执行期间必然存活，函数表有效；
        // 字段声明顺序（handle 在 plugin 之前）保证后续库卸载发生在
        // 本 Drop 完成之后。
        let status = unsafe { (self.plugin.api().destroy)(self.handle) };
        self.handle.ptr = std::ptr::null_mut();
        if status != 0 {
            warn!(
                component = "driver-loader",
                driver_id = %self.plugin.manifest().id,
                error_code = "driver_destroy_failed",
                status,
                "NativeDriver 销毁时 destroy 返回非零状态码"
            );
        }
    }
}

/// 取走 Plugin 写入的 owned buffer；`ptr` 为空视为空响应（§17.1）。
fn take_owned(
    api: &DriverApiV1,
    out: FfiOwnedBuffer,
    function: &'static str,
) -> Result<OwnedFfiBuffer, LoaderError> {
    if out.ptr.is_null() {
        return Err(LoaderError::EmptyResponse { function });
    }
    Ok(OwnedFfiBuffer::take(api, out))
}

/// 获取 `get_last_error_json`（§17.6 固定形状 `ErrorEnvelope`）。
///
/// 返回 `None` 表示无详细错误（调用失败、buffer 为空或解析失败）。
fn fetch_error(api: &'static DriverApiV1, handle: DriverHandle) -> Option<DriverErrorInfo> {
    let mut out = empty_buffer();
    // Safety: out 指向栈上可写内存；返回的 buffer 属于 Plugin。
    let status = unsafe { (api.get_last_error_json)(handle, &mut out) };
    if status != 0 || out.ptr.is_null() {
        return None;
    }
    let buffer = OwnedFfiBuffer::take(api, out);
    let json = buffer.as_str("get_last_error_json").ok()?;
    serde_json::from_str::<ErrorEnvelope>(json)
        .ok()
        .map(|envelope| DriverErrorInfo {
            code: envelope.code,
            message: envelope.message,
            protocol_code: envelope.protocol_code,
            retryable: envelope.retryable,
        })
}
