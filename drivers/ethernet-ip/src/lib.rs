//! driver-ether-ip：EtherNet/IP (CIP) 协议驱动（§34.6 V0.3）。
//!
//! # 模块
//!
//! - [`address`]：标签路径文法（大小写敏感，canonical 原样保留）
//! - [`config`]：连接配置
//! - [`enip`]：封装层（24B 小端头、RegisterSession、SendRRData 包裹剥离）
//! - [`cip`]：CIP Message Router（Read/Write Tag、Multi-Service 打包解包、类型码表）
//! - [`encode`] / [`decode`]：类型映射表
//! - [`error`]：错误分类
//! - `session` / `batch` 与 ABI 面：提交 3 交付

pub mod address;
pub mod cip;
pub mod config;
pub mod decode;
pub mod encode;
pub mod enip;
pub mod error;
