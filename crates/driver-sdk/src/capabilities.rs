//! `ProtocolCapabilities`（§13.1 Normative）。

use serde::{Deserialize, Serialize};

/// 协议层能力声明（§13.1 Normative）。
///
/// 只描述协议实现能做什么（如 S7 是否支持写、OPC UA 是否支持 Subscription）；
/// 型号能力属于 `ProfileCapabilities`，领域能力通过标准 Resource/Property/Command
/// 的存在性表达（§13.3），禁止把三者混进同一个结构。
///
/// capability 为 `false` 时，对应方法必须返回标准 `Unsupported` 错误，不能 panic（§15）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolCapabilities {
    pub read: bool,
    pub write: bool,
    pub batch_read: bool,
    pub batch_write: bool,
    pub browse: bool,
    pub polling: bool,
    pub subscription: bool,
    pub events: bool,
    pub history: bool,
}

impl Default for ProtocolCapabilities {
    /// 默认值：仅声明 `read` 与 `polling`（最简采集能力）。
    fn default() -> Self {
        Self {
            read: true,
            write: false,
            batch_read: false,
            batch_write: false,
            browse: false,
            polling: true,
            subscription: false,
            events: false,
            history: false,
        }
    }
}
