//! domain-model：工业领域标准语义模型（§41~§46）。
//!
//! Domain Model 负责把型号语义映射成标准 PLC / CNC / Robot / Drive /
//! Meter / Sensor 等领域路径，屏蔽厂商与协议差异；只负责标准语义，
//! 不参与协议能力 / bitflag / 编解码（§52）。
//!
//! 本阶段为最小实现：
//!
//! - `standard`：`DomainKind` → 标准路径前缀表（如 `drive.`）；
//! - `mapper`：领域路径校验与 `Observation` 组装，打通
//!   `RawReadResult → Profile → Domain → Observation` 链路（§7.3）。

mod mapper;
mod standard;

pub use mapper::{DomainError, build_observation, validate_domain_path};
pub use standard::standard_prefix;
