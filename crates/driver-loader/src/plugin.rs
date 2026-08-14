//! Native Plugin 加载与校验（§19、§20）。

use std::ffi::CString;
use std::mem::{offset_of, size_of};
use std::path::{Path, PathBuf};

use driver_sdk::abi::{ABI_MAJOR, ABI_MINOR, DriverApiV1};
use driver_sdk::manifest::{AbiVersion, DriverManifest};
use libloading::{Library, Symbol};
use tracing::{info, warn};

use crate::error::LoaderError;

/// ABI v1 函数表固定头部大小：`struct_size`/`abi_major`/`abi_minor`/
/// `feature_flags`（§17.9），其后才是函数指针表。
///
/// 头部字段都是标量（任何位组合均为合法值），可以安全地先于
/// 函数指针表读取；函数指针字段按 `struct_size` 边界逐个校验后
/// 才允许构造 Rust 函数指针值。
const HEADER_SIZE: usize = offset_of!(DriverApiV1, create);

/// 已加载并通过校验的 Native Plugin（§19、§20）。
///
/// # Safety 论证
///
/// - `api` 是 `&'static DriverApiV1`，指向动态库内的静态函数表；
///   其真实生命周期与 `lib`（[`Library`]）相同。
/// - 结构约束：`lib` 是插件字段的唯一持有者，卸载只会发生在
///   `NativePlugin` 被 Drop 时；对 `api` 的所有使用都只能发生在
///   插件存活期间（`api()` 返回的引用不越过插件调用边界），
///   因此指针不会失效。
/// - `&'static` 仅为绕过借用检查，公开 API 不对外泄漏该引用的
///   可写/复制能力：`api()` 为 `pub(crate)`，只能构造 [`crate::NativeDriver`]。
pub struct NativePlugin {
    api: &'static DriverApiV1,
    /// 动态库句柄：RAII 持有者，卸载只发生在插件 Drop 时；
    /// 字段不被读取（职责是生命周期），见结构 Safety 论证。
    #[allow(dead_code)]
    lib: Library,
    manifest: DriverManifest,
    path: PathBuf,
}

/// 校验 ABI v1 必需函数指针非空（§17.9 最小函数表）。
///
/// 通过 `usize` 读取函数指针字段（任何位组合都是合法指针值），
/// 避免在校验完成前构造可能非法的 Rust 函数指针值；同时用
/// `struct_size` 校验字段存在性（§17.4 尾部扩展规则：`struct_size`
/// 声明字段末尾边界）。
macro_rules! require_function_at {
    ($bytes:ident, $field:ident, $struct_size:ident, $path:ident) => {{
        let offset = offset_of!(DriverApiV1, $field);
        let field_end = offset + size_of::<usize>();
        if field_end > $struct_size {
            return Err(LoaderError::StructTooSmall {
                path: $path.to_owned(),
                size: $struct_size as u32,
                required: field_end,
            });
        }
        let value = unsafe { ($bytes.add(offset) as *const usize).read_unaligned() };
        if value == 0 {
            return Err(LoaderError::MissingFunction {
                path: $path.to_owned(),
                name: stringify!($field),
            });
        }
    }};
}

/// 记录加载失败的结构化日志（统一字段：`component`、`driver_id`、
/// `path`、`error_code`，开发规范 §6）。
fn warn_load_failure(manifest: &DriverManifest, path: &Path, error: &LoaderError) {
    warn!(
        component = "driver-loader",
        driver_id = %manifest.id,
        path = %path.display(),
        error_code = error.code(),
        error = %error,
        "Native Plugin 加载失败"
    );
}

impl NativePlugin {
    /// 加载并校验动态库（§19、§20）。
    ///
    /// 校验顺序（任一步失败即拒绝）：
    /// 1. 入口符号存在（`manifest.entry`，默认
    ///    [`driver_sdk::abi::ENTRY_SYMBOL`]）；
    /// 2. 入口返回非空函数表指针（§17.9）；
    /// 3. `struct_size` >= 必需函数表长度（§17.4）；
    /// 4. `abi_major` 一致且 `abi_minor <=` Core 支持（§18）；
    /// 5. Manifest 声明 ABI 与实际入口一致（§20）；
    /// 6. 必需函数指针全部非空（§17.9）。
    ///
    /// # Errors
    ///
    /// - [`LoaderError::LoadFailed`]：库无法加载；
    /// - [`LoaderError::EntryNotFound`]：入口符号缺失；
    /// - [`LoaderError::NullEntry`]：入口返回空指针；
    /// - [`LoaderError::StructTooSmall`] / [`LoaderError::AbiIncompatible`] /
    ///   [`LoaderError::ManifestAbiMismatch`] / [`LoaderError::MissingFunction`]：
    ///   校验失败。
    pub fn load(path: &Path, manifest: DriverManifest) -> Result<Self, LoaderError> {
        // Safety: libloading 要求动态库与进程 ABI（平台/架构/异常模型）兼容，
        // 调用方保证加载的是 ForgeLink Native Plugin 构建产物。
        let lib = unsafe { Library::new(path) }.map_err(|source| {
            let error = LoaderError::LoadFailed {
                path: path.to_owned(),
                source,
            };
            warn_load_failure(&manifest, path, &error);
            error
        })?;

        let symbol_name = CString::new(manifest.entry.as_str()).map_err(|_| {
            let error = LoaderError::InvalidEntryName {
                path: path.to_owned(),
                entry: manifest.entry.clone(),
            };
            warn_load_failure(&manifest, path, &error);
            error
        })?;
        // Safety: 入口签名由 ABI v1 契约固定（§16、§17.9），
        // 符号类型与 `forgelink_driver_entry_v1` 声明一致。
        let entry: Symbol<unsafe extern "C" fn() -> *const DriverApiV1> =
            unsafe { lib.get(symbol_name.as_bytes()) }.map_err(|_| {
                let error = LoaderError::EntryNotFound {
                    path: path.to_owned(),
                    symbol: manifest.entry.clone(),
                };
                warn_load_failure(&manifest, path, &error);
                error
            })?;

        // Safety: Plugin 侧必须 catch_unwind 保证不跨 C ABI panic（§17.7），
        // Core 无法可靠捕获；返回的指针生命周期与库相同。
        let api_ptr = unsafe { entry() };
        if api_ptr.is_null() {
            let error = LoaderError::NullEntry {
                path: path.to_owned(),
            };
            warn_load_failure(&manifest, path, &error);
            return Err(error);
        }
        // Safety: 指针指向库内静态函数表，库在 `lib` 存活期内有效；
        // 校验在原始指针上逐步进行，全部通过后才构造引用。
        let api = unsafe { validate_api(api_ptr, &manifest, path) }.inspect_err(|error| {
            warn_load_failure(&manifest, path, error);
        })?;

        info!(
            component = "driver-loader",
            driver_id = %manifest.id,
            version = %manifest.version,
            path = %path.display(),
            abi = %format!("{}.{}", api.abi_major, api.abi_minor),
            "Native Plugin 加载成功"
        );
        Ok(Self {
            api,
            lib,
            manifest,
            path: path.to_owned(),
        })
    }

    /// 插件声明的 Manifest（§20）。
    pub fn manifest(&self) -> &DriverManifest {
        &self.manifest
    }

    /// 动态库路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 校验后的 ABI 函数表（仅供 [`crate::NativeDriver`] 使用）。
    pub(crate) fn api(&self) -> &'static DriverApiV1 {
        self.api
    }
}

impl std::fmt::Debug for NativePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `DriverApiV1` 未实现 Debug（含裸函数指针，无调试价值），
        // 手动输出可读摘要。
        f.debug_struct("NativePlugin")
            .field("manifest", &self.manifest)
            .field("path", &self.path)
            .field(
                "abi",
                &format!("{}.{}", self.api.abi_major, self.api.abi_minor),
            )
            .finish()
    }
}

/// 从入口返回的原始指针逐步读取并校验 ABI 函数表
/// （§17.4、§17.9、§18、§20），全部通过后才构造 `&DriverApiV1`。
///
/// 读取顺序保证不构造"可能是非法值"的 Rust 类型：
/// 1. 固定头部（标量字段，任何位组合都是合法值）；
/// 2. ABI 版本与 Manifest 一致性；
/// 3. 必需函数指针逐个按 `struct_size` 边界读取为 `usize` 并检查非空。
///
/// # Safety
///
/// - `api_ptr` 必须非空且指向动态库内静态函数表，生命周期由调用方
///   （`Library` 持有者）保证；
/// - `struct_size` 声明的范围是信任边界：只读取 `api_ptr + offset <
///   struct_size` 范围内的字节（§17.4 尾部扩展规则）；
/// - 返回的 `&'static` 引用生命周期与 `api_ptr` 指向的数据一致。
unsafe fn validate_api(
    api_ptr: *const DriverApiV1,
    manifest: &DriverManifest,
    path: &Path,
) -> Result<&'static DriverApiV1, LoaderError> {
    let bytes = api_ptr as *const u8;

    // 1. 固定头部：标量字段任何位组合都是合法值，可安全读取。
    let struct_size = unsafe { (bytes as *const u32).read_unaligned() } as usize;
    if struct_size < HEADER_SIZE {
        return Err(LoaderError::StructTooSmall {
            path: path.to_owned(),
            size: struct_size as u32,
            required: HEADER_SIZE,
        });
    }
    let abi_major =
        unsafe { (bytes.add(offset_of!(DriverApiV1, abi_major)) as *const u16).read_unaligned() };
    let abi_minor =
        unsafe { (bytes.add(offset_of!(DriverApiV1, abi_minor)) as *const u16).read_unaligned() };

    // 2. ABI 版本（§18）。
    if abi_major != ABI_MAJOR || abi_minor > ABI_MINOR {
        return Err(LoaderError::AbiIncompatible {
            path: path.to_owned(),
            major: abi_major,
            minor: abi_minor,
        });
    }
    // 3. Manifest 声明与实际入口一致（§20）。
    if manifest.abi.major != abi_major || manifest.abi.minor != abi_minor {
        return Err(LoaderError::ManifestAbiMismatch {
            path: path.to_owned(),
            declared: manifest.abi,
            actual: AbiVersion {
                major: abi_major,
                minor: abi_minor,
            },
        });
    }

    // 4. 必需函数指针（§17.9 最小函数表）。
    require_function_at!(bytes, create, struct_size, path);
    require_function_at!(bytes, destroy, struct_size, path);
    require_function_at!(bytes, connect, struct_size, path);
    require_function_at!(bytes, disconnect, struct_size, path);
    require_function_at!(bytes, get_capabilities_json, struct_size, path);
    require_function_at!(bytes, validate_address, struct_size, path);
    require_function_at!(bytes, read, struct_size, path);
    require_function_at!(bytes, write, struct_size, path);
    require_function_at!(bytes, execute, struct_size, path);
    require_function_at!(bytes, browse, struct_size, path);
    require_function_at!(bytes, subscribe, struct_size, path);
    require_function_at!(bytes, unsubscribe, struct_size, path);
    require_function_at!(bytes, query_history, struct_size, path);
    require_function_at!(bytes, get_last_error_json, struct_size, path);
    require_function_at!(bytes, free_buffer, struct_size, path);

    // 5. 全部字段已校验为合法值（头部标量 + 非空函数指针），创建引用。
    Ok(unsafe { &*api_ptr })
}
