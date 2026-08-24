//! driver-mitsubishi-mc：三菱 MC 协议（3E 帧）驱动（§34.6 V0.3 第二交付）。
//!
//! # 模块
//!
//! - [`address`]：软元件文法（编号一律十进制解析——十六进制陷阱挡在驱动内）
//! - [`config`]：连接配置
//! - [`mc`]：3E 帧编解码（纯函数）
//! - [`error`]：错误分类
//! - `session` / `batch` / `encode` / `decode` 与 ABI 面：提交 3–6 交付

pub mod address;
pub mod batch;
pub mod config;
pub mod decode;
pub mod encode;
pub mod error;
pub mod mc;
