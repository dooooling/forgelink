//! Loader 错误类型与稳定错误码（§19、§20；结构化日志 `error_code`，开发规范 §6）。

use std::fmt;
use std::path::PathBuf;

use driver_sdk::DriverErrorInfo;
use driver_sdk::abi::{ABI_MAJOR, ABI_MINOR};
use driver_sdk::manifest::AbiVersion;

/// Driver 加载 / ABI 调用错误。
///
/// `code()` 返回稳定、机器可读的错误码，不得随实现细节变化；
/// 结构化日志使用 `error_code = e.code()` 字段（与 profile-engine 一致）。
#[derive(Debug)]
pub enum LoaderError {
    /// 动态库无法加载（文件不存在、格式错误、依赖缺失等）。
    LoadFailed {
        path: PathBuf,
        source: libloading::Error,
    },
    /// 入口符号不存在于动态库中（§20：Manifest `entry` 与实际符号不一致）。
    EntryNotFound { path: PathBuf, symbol: String },
    /// Manifest `entry` 不是合法 C 字符串（含 NUL，§20 可恢复配置错误）。
    InvalidEntryName { path: PathBuf, entry: String },
    /// 入口函数返回了空指针（契约违规，§17.9）。
    NullEntry { path: PathBuf },
    /// `struct_size` 小于必需函数表的最小长度（§17.4 尾部扩展规则）。
    StructTooSmall {
        path: PathBuf,
        size: u32,
        required: usize,
    },
    /// ABI 版本不兼容（§18）：`abi_major` 必须一致，且
    /// `plugin.abi_minor <= Core 支持的 minor`。
    AbiIncompatible {
        path: PathBuf,
        major: u16,
        minor: u16,
    },
    /// Manifest 声明的 ABI 与实际入口不符（§20 必须拒绝）。
    ManifestAbiMismatch {
        path: PathBuf,
        declared: AbiVersion,
        actual: AbiVersion,
    },
    /// ABI v1 必需函数指针为 `null`（§17.9 最小函数表）。
    MissingFunction { path: PathBuf, name: &'static str },
    /// `create` 返回非零状态码。
    CreateFailed { detail: Option<DriverErrorInfo> },
    /// ABI 调用返回非零状态码（§17.6：`0 = OK`、`>0 = 标准错误`、
    /// `<0 = Driver/协议错误`），`detail` 来自 `get_last_error_json`。
    CallFailed {
        function: &'static str,
        status: i32,
        detail: Option<DriverErrorInfo>,
    },
    /// 调用成功但返回空 buffer（契约违规，§17.3）。
    EmptyResponse { function: &'static str },
    /// Plugin 返回的 buffer 不是合法 UTF-8。
    InvalidUtf8 { function: &'static str },
    /// Plugin 返回的 JSON 无法解析（含 `schema_version` 不匹配，§17.9）。
    InvalidResponse {
        function: &'static str,
        source: serde_json::Error,
    },
    /// `create` 成功但返回空句柄。
    InvalidHandle,
    /// 参数编码错误（如复杂类型不允许写入 `value_bytes`，§17.2）。
    Encoding(String),
}

impl LoaderError {
    /// 稳定错误码（结构化日志 `error_code` 字段）。
    pub fn code(&self) -> &'static str {
        match self {
            Self::LoadFailed { .. } => "driver_load_failed",
            Self::EntryNotFound { .. } => "driver_entry_not_found",
            Self::InvalidEntryName { .. } => "driver_manifest_entry_invalid",
            Self::NullEntry { .. } => "driver_entry_null",
            Self::StructTooSmall { .. } => "driver_struct_too_small",
            Self::AbiIncompatible { .. } => "driver_abi_incompatible",
            Self::ManifestAbiMismatch { .. } => "driver_manifest_abi_mismatch",
            Self::MissingFunction { .. } => "driver_missing_function",
            Self::CreateFailed { .. } => "driver_create_failed",
            Self::CallFailed { .. } => "driver_call_failed",
            Self::EmptyResponse { .. } => "driver_empty_response",
            Self::InvalidUtf8 { .. } => "driver_invalid_utf8",
            Self::InvalidResponse { .. } => "driver_invalid_response",
            Self::InvalidHandle => "driver_invalid_handle",
            Self::Encoding(_) => "driver_encoding_error",
        }
    }
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LoadFailed { path, source } => {
                write!(f, "动态库加载失败 {}: {source}", path.display())
            }
            Self::EntryNotFound { path, symbol } => {
                write!(f, "{} 缺少入口符号 {symbol:?}", path.display())
            }
            Self::InvalidEntryName { path, entry } => {
                write!(
                    f,
                    "{} Manifest entry {entry:?} 含 NUL，不是合法 C 字符串",
                    path.display()
                )
            }
            Self::NullEntry { path } => write!(f, "{} 入口函数返回空指针", path.display()),
            Self::StructTooSmall {
                path,
                size,
                required,
            } => write!(
                f,
                "{} struct_size = {size} 小于必需函数表长度 {required}",
                path.display()
            ),
            Self::AbiIncompatible { path, major, minor } => write!(
                f,
                "{} ABI 版本 {major}.{minor} 不兼容（Core 支持 {ABI_MAJOR}.x，minor <= {ABI_MINOR}）",
                path.display()
            ),
            Self::ManifestAbiMismatch {
                path,
                declared,
                actual,
            } => write!(
                f,
                "{} Manifest 声明 ABI {}.{} 与实际入口 {}.{} 不一致",
                path.display(),
                declared.major,
                declared.minor,
                actual.major,
                actual.minor
            ),
            Self::MissingFunction { path, name } => {
                write!(f, "{} 缺少必需函数指针 {name}", path.display())
            }
            Self::CreateFailed { detail } => match detail {
                Some(d) => write!(f, "create 失败: {} (code={})", d.message, d.code),
                None => write!(f, "create 失败，无详细错误"),
            },
            Self::CallFailed {
                function,
                status,
                detail,
            } => match detail {
                Some(d) => write!(
                    f,
                    "{function} 调用失败 status={status}: {} (code={})",
                    d.message, d.code
                ),
                None => write!(f, "{function} 调用失败 status={status}"),
            },
            Self::EmptyResponse { function } => write!(f, "{function} 返回空 buffer"),
            Self::InvalidUtf8 { function } => write!(f, "{function} 返回非 UTF-8 数据"),
            Self::InvalidResponse { function, source } => {
                write!(f, "{function} 返回的 JSON 无法解析: {source}")
            }
            Self::InvalidHandle => write!(f, "create 成功但返回空句柄"),
            Self::Encoding(msg) => write!(f, "参数编码错误: {msg}"),
        }
    }
}

impl std::error::Error for LoaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LoadFailed { source, .. } => Some(source),
            Self::InvalidResponse { source, .. } => Some(source),
            _ => None,
        }
    }
}
