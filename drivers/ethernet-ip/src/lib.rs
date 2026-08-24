//! driver-ether-ip：EtherNet/IP (CIP) 协议驱动（§34.6 V0.3）。
//!
//! # 模块
//!
//! - [`address`]：标签路径文法（大小写敏感，canonical 原样保留）
//! - [`config`]：连接配置
//! - [`error`]：错误分类
//! - `enip` / `cip` / `session` / `batch` / `encode` / `decode`：提交 2–3 交付

pub mod address;
pub mod config;
pub mod error;
