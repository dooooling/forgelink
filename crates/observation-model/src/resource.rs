//! `Resource`（§5 Normative）。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 设备内部的逻辑对象树节点（§5）。
///
/// 注意：`path` 是平台语义路径（如 `/device/fanuc01/axis/x`），
/// 不是协议地址；Driver 私有地址保存在 Profile 的属性映射中。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    pub path: crate::ResourcePath,
    /// 资源类型标识（如 `axis`、`spindle`、`memory`），由 Domain/Profile 约定。
    pub kind: String,
    pub display_name: String,

    pub properties: Vec<crate::Property>,
    pub commands: Vec<crate::CommandDescriptor>,
    /// 子资源路径列表。
    pub children: Vec<crate::ResourcePath>,

    pub metadata: BTreeMap<String, String>,
}
