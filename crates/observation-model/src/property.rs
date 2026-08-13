//! `Property` 与采集/写入请求（§6.1、§10）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 可读取或写入的设备属性（§6.1 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Property {
    pub path: crate::PropertyPath,
    pub display_name: String,
    pub value_type: crate::DataType,
    pub unit: Option<String>,
    pub readable: bool,
    pub writable: bool,
    pub metadata: BTreeMap<String, String>,
}

/// 上层采集计划使用的语义 Property 引用（§10 Normative）。
///
/// Profile 负责把 `path` 映射为 `DriverReadItem`；Core 不理解协议地址。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PropertyReadRequest {
    /// 在本次请求批次内唯一的请求 ID。
    pub id: u64,
    pub path: crate::PropertyPath,
}

/// 属性写入请求（§75.1 Normative）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyWriteRequest {
    pub items: Vec<PropertyWriteItem>,
}

/// 单个属性写入项（§75.1）。
///
/// Profile Engine 将语义路径映射成 `DriverWriteItem { address, raw_value }`（§75.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyWriteItem {
    pub path: crate::PropertyPath,
    pub value: crate::Value,
}
