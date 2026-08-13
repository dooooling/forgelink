//! 基础标识与时间类型（§4.1）。

/// 设备唯一标识。
///
/// 例如 `siemens-plc-01`、`fanuc-cnc-01`。
pub type DeviceId = String;

/// 设备内部逻辑对象的语义路径（§5）。
///
/// 例如 `/device/fanuc01/axis/x`。协议私有地址保存在 Profile 映射中，
/// 本路径不包含任何协议地址语义。
pub type ResourcePath = String;

/// 属性的语义路径（§6.1）。
///
/// 例如 `cnc.axis.x.absolute_position`。
pub type PropertyPath = String;

/// UTC Unix Epoch 纳秒时间戳（§8）。
///
/// 从 `1970-01-01T00:00:00Z` 起的纳秒数。负数表示 1970 年之前，
/// 平台不应产生该情况，但类型不做限制。
pub type TimestampNs = i64;
