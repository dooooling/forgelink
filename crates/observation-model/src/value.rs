//! `Value` 与 `FieldValue`（§6.3）。

use serde::{Deserialize, Serialize};

/// Profile/Domain 归一化后的平台值（§6.3）。
///
/// 与协议原始值 `RawValue`（`crate::raw`）的区别：
/// 本类型经过 Profile 缩放、单位和领域语义映射，是上层与北向的唯一值类型。
///
/// # 序列化
///
/// 当前使用 serde 默认的外部标签编码（如 `{"i32": 5}`），保证可逆与无歧义；
/// 北向报文（MQTT/REST）中的"裸 JSON 值"编码由 data-pipeline 另行映射，不属于本类型职责。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Struct(Vec<FieldValue>),
}

/// 结构类型的一个命名字段值（§6.3）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldValue {
    /// 字段名。
    pub name: String,
    /// 字段值。
    pub value: Value,
}
