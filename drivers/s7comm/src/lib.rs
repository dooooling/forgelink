//! driver-s7comm：Siemens S7comm Native Plugin（§34.6 V0.2）。
//!
//! ISO-on-TCP（TPKT/COTP）+ S7 Read/Write Var，批量合并与写规划见
//! `batch`，会话见 `session`，C ABI 面见 `ffi`（commit 3 交付）。
//!
//! # 模块
//!
//! - [`address`]：地址文法（§11）
//! - [`config`]：连接配置
//! - [`cotp`]：TPKT/COTP 编解码
//! - [`pdu`]：S7 PDU 编解码
//! - [`encode`] / [`decode`]：类型映射表
//! - [`error`]：错误分类

pub mod address;
pub mod config;
pub mod cotp;
pub mod decode;
pub mod encode;
pub mod error;
pub mod pdu;
