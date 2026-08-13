//! `DataType` 与 `FieldSchema`（§6.2）。

use serde::{Deserialize, Serialize};

/// 属性的数据类型声明。
///
/// `Array` 与 `Struct` 为递归类型：`Array` 表示元素类型相同的数组，
/// `Struct` 表示命名字段组成的结构（由 `FieldSchema` 描述）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    String,
    Bytes,
    /// 元素类型相同的数组。
    Array(Box<DataType>),
    /// 命名字段结构。
    Struct(Vec<FieldSchema>),
}

/// 结构类型字段的 schema 描述（§6.2）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldSchema {
    /// 字段名。
    pub name: String,
    /// 字段数据类型。
    pub data_type: DataType,
}
