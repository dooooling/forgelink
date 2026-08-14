//! 连接配置解析与校验（`create` 的 `config` JSON）。
//!
//! 示例（TCP）：
//!
//! ```json
//! {
//!   "mode": "tcp",
//!   "host": "192.168.1.10",
//!   "port": 502,
//!   "timeout_ms": 3000,
//!   "unit_id": 1,
//!   "reconnect": true,
//!   "reconnect_max_attempts": 3,
//!   "reconnect_delay_ms": 1000
//! }
//! ```
//!
//! 示例（RTU）：
//!
//! ```json
//! {
//!   "mode": "rtu",
//!   "serial": { "port": "COM3", "baud_rate": 9600, "data_bits": 8, "stop_bits": 1, "parity": "none" },
//!   "timeout_ms": 1000,
//!   "unit_id": 1
//! }
//! ```
//!
//! 未知字段拒绝（`deny_unknown_fields`：配置错误尽早暴露，避免静默偏差）。

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// 连接模式。
/// 传输模式：TCP（MBAP，以太网）或 RTU（串口），决定必填配置字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportMode {
    /// TCP（MBAP 帧）：连接 `host:port`（默认 502）。
    Tcp,
    /// RTU（串口帧）：使用 `serial` 串口参数。
    Rtu,
}

/// 多寄存器（32/64 位）数值的字序。
///
/// Modbus 协议只规定寄存器内字节序（大端），不规定多寄存器间的字序，
/// 不同设备约定不同（AB/CD 与 CD/AB 均常见）：
///
/// ```text
/// Abcd（默认）：寄存器 r0 为高字，值 = (r0 << 16) | r1
/// Cdab：       寄存器 r0 为低字，值 = (r1 << 16) | r0
/// ```
///
/// 64 位类型（U64/I64/F64）同理按字反转，仅 2/4 寄存器类型受影响，
/// 单寄存器（U8/I8/U16/I16）与位类型无关。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WordOrder {
    /// 高字在前（第一个寄存器为高 16 位）。
    #[default]
    Abcd,
    /// 低字在前（第一个寄存器为低 16 位）。
    Cdab,
}

/// 串口参数（RTU）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerialConfig {
    /// 串口设备名（Windows: `COM3`；Linux: `/dev/ttyS0`）。
    pub port: String,
    /// 波特率（默认 9600）。
    #[serde(default = "default_baud_rate")]
    pub baud_rate: u32,
    /// 数据位：只允许 5/6/7/8（默认 8）。
    #[serde(default = "default_data_bits")]
    pub data_bits: u8,
    /// 停止位：只允许 1/2（默认 1）。
    #[serde(default = "default_stop_bits")]
    pub stop_bits: u8,
    /// 校验：只允许 `none`/`even`/`odd`（默认 none）。
    #[serde(default = "default_parity")]
    pub parity: String,
}

fn default_baud_rate() -> u32 {
    9_600
}
fn default_data_bits() -> u8 {
    8
}
fn default_stop_bits() -> u8 {
    1
}
fn default_parity() -> String {
    "none".to_owned()
}

/// Modbus 驱动连接配置。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModbusConfig {
    /// 传输模式：TCP（MBAP）或 RTU（串口），决定必填字段（host/port 或 serial）。
    pub mode: TransportMode,
    /// TCP 主机（mode = tcp 时必填）。
    #[serde(default)]
    pub host: Option<String>,
    /// TCP 端口（默认 502）。
    #[serde(default = "default_port")]
    pub port: u16,
    /// 串口参数（mode = rtu 时必填）。
    #[serde(default)]
    pub serial: Option<SerialConfig>,
    /// 请求超时（默认 3000ms）。
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// 地址未显式指定 unit 时的默认从站号（默认 1）。
    #[serde(default = "default_unit_id")]
    pub unit_id: u8,
    /// 多寄存器数值字序（默认 abcd，见 [`WordOrder`]）。
    #[serde(default)]
    pub word_order: WordOrder,
    /// 断线后是否自动重连（默认 true）。
    #[serde(default = "default_true")]
    pub reconnect: bool,
    /// 重连尝试次数上限（默认 3）。
    #[serde(default = "default_reconnect_attempts")]
    pub reconnect_max_attempts: u32,
    /// 每次重连尝试间隔（默认 1000ms）。
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
}

fn default_port() -> u16 {
    502
}
fn default_timeout_ms() -> u64 {
    3_000
}
fn default_unit_id() -> u8 {
    1
}
fn default_true() -> bool {
    true
}
fn default_reconnect_attempts() -> u32 {
    3
}
fn default_reconnect_delay_ms() -> u64 {
    1_000
}

impl Default for ModbusConfig {
    fn default() -> Self {
        Self {
            mode: TransportMode::Tcp,
            host: None,
            port: default_port(),
            serial: None,
            timeout_ms: default_timeout_ms(),
            unit_id: default_unit_id(),
            word_order: WordOrder::Abcd,
            reconnect: true,
            reconnect_max_attempts: default_reconnect_attempts(),
            reconnect_delay_ms: default_reconnect_delay_ms(),
        }
    }
}

impl ModbusConfig {
    /// 解析 `create(config)` 的 JSON 并校验。
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        if json.trim().is_empty() {
            return Err(ConfigError::invalid("config 为空"));
        }
        let config: ModbusConfig =
            serde_json::from_str(json).map_err(|e| ConfigError::invalid(&e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// 校验配置合法性。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.timeout_ms == 0 {
            return Err(ConfigError::invalid("timeout_ms 必须大于 0"));
        }
        if self.unit_id == 0 || self.unit_id > 247 {
            return Err(ConfigError::invalid(&format!(
                "unit_id 越界（{}，只允许 1..=247）",
                self.unit_id
            )));
        }
        match self.mode {
            TransportMode::Tcp => {
                if self.host.as_deref().unwrap_or("").trim().is_empty() {
                    return Err(ConfigError::invalid("tcp 模式必须提供 host"));
                }
            }
            TransportMode::Rtu => match &self.serial {
                Some(serial) => {
                    if serial.port.trim().is_empty() {
                        return Err(ConfigError::invalid("rtu 模式必须提供 serial.port"));
                    }
                    if serial.baud_rate == 0 {
                        return Err(ConfigError::invalid("baud_rate 必须大于 0"));
                    }
                    // 非法串口参数不得静默退化（曾把非法值替换为 8/1/none，
                    // 配置错误变成难以定位的通信异常）。
                    if !matches!(serial.data_bits, 5..=8) {
                        return Err(ConfigError::invalid(&format!(
                            "serial.data_bits 非法（{}，只允许 5/6/7/8）",
                            serial.data_bits
                        )));
                    }
                    if !matches!(serial.stop_bits, 1 | 2) {
                        return Err(ConfigError::invalid(&format!(
                            "serial.stop_bits 非法（{}，只允许 1/2）",
                            serial.stop_bits
                        )));
                    }
                    if !matches!(serial.parity.as_str(), "none" | "even" | "odd") {
                        return Err(ConfigError::invalid(&format!(
                            "serial.parity 非法（{}，只允许 none/even/odd）",
                            serial.parity
                        )));
                    }
                }
                None => {
                    return Err(ConfigError::invalid("rtu 模式必须提供 serial 配置"));
                }
            },
        }
        Ok(())
    }

    /// 请求超时。
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// 重连尝试间隔。
    pub fn reconnect_delay(&self) -> Duration {
        Duration::from_millis(self.reconnect_delay_ms)
    }
}

/// 配置错误（`retryable = false`：配置类错误重试无意义）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    /// 人类可读的错误详情（与校验规则一一对应，见 [`ModbusConfig::validate`]）。
    pub message: String,
}

impl ConfigError {
    /// 构造配置错误（message 为校验失败的规则说明）。
    pub fn invalid(message: &str) -> Self {
        Self {
            message: message.to_owned(),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "配置错误：{}", self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tcp_config() {
        let json = r#"{
            "mode": "tcp",
            "host": "192.168.1.10",
            "port": 1502,
            "timeout_ms": 1000,
            "unit_id": 2
        }"#;
        let config = ModbusConfig::from_json(json).unwrap();
        assert_eq!(config.mode, TransportMode::Tcp);
        assert_eq!(config.host.as_deref(), Some("192.168.1.10"));
        assert_eq!(config.port, 1502);
        assert_eq!(config.timeout_ms, 1000);
        assert_eq!(config.unit_id, 2);
        assert_eq!(config.word_order, WordOrder::Abcd);
        assert!(config.reconnect);
        assert_eq!(config.reconnect_max_attempts, 3);
    }

    #[test]
    fn applies_defaults() {
        let json = r#"{"mode": "tcp", "host": "127.0.0.1"}"#;
        let config = ModbusConfig::from_json(json).unwrap();
        assert_eq!(config.port, 502);
        assert_eq!(config.timeout_ms, 3000);
        assert_eq!(config.unit_id, 1);
        assert_eq!(config.reconnect_delay_ms, 1000);
    }

    #[test]
    fn parses_rtu_config() {
        let json = r#"{
            "mode": "rtu",
            "serial": { "port": "COM3", "baud_rate": 19200 },
            "timeout_ms": 500
        }"#;
        let config = ModbusConfig::from_json(json).unwrap();
        assert_eq!(config.mode, TransportMode::Rtu);
        let serial = config.serial.as_ref().unwrap();
        assert_eq!(serial.port, "COM3");
        assert_eq!(serial.baud_rate, 19200);
        assert_eq!(serial.data_bits, 8);
        assert_eq!(serial.stop_bits, 1);
        assert_eq!(serial.parity, "none");
    }

    #[test]
    fn rejects_empty_config() {
        assert!(ModbusConfig::from_json("").is_err());
        assert!(ModbusConfig::from_json("{}").is_err());
    }

    #[test]
    fn rejects_tcp_without_host() {
        assert!(ModbusConfig::from_json(r#"{"mode": "tcp"}"#).is_err());
    }

    #[test]
    fn rejects_rtu_without_serial() {
        assert!(ModbusConfig::from_json(r#"{"mode": "rtu"}"#).is_err());
    }

    #[test]
    fn rejects_zero_timeout_and_bad_unit() {
        let json = r#"{"mode": "tcp", "host": "x", "timeout_ms": 0}"#;
        assert!(ModbusConfig::from_json(json).is_err());
        let json = r#"{"mode": "tcp", "host": "x", "unit_id": 0}"#;
        assert!(ModbusConfig::from_json(json).is_err());
        let json = r#"{"mode": "tcp", "host": "x", "unit_id": 248}"#;
        assert!(ModbusConfig::from_json(json).is_err());
    }

    #[test]
    fn rejects_invalid_serial_params() {
        let base = r#""mode": "rtu", "serial": { "port": "COM3", "#;
        // data_bits 只允许 5/6/7/8。
        let json = format!("{{{base} \"data_bits\": 9 }}}}");
        assert!(
            ModbusConfig::from_json(&json).is_err(),
            "data_bits=9 必须拒绝"
        );
        // stop_bits 只允许 1/2。
        let json = format!("{{{base} \"stop_bits\": 3 }}}}");
        assert!(
            ModbusConfig::from_json(&json).is_err(),
            "stop_bits=3 必须拒绝"
        );
        // parity 只允许 none/even/odd。
        let json = format!("{{{base} \"parity\": \"mark\" }}}}");
        assert!(
            ModbusConfig::from_json(&json).is_err(),
            "parity=mark 必须拒绝"
        );
        // 合法参数通过。
        let json =
            format!("{{{base} \"data_bits\": 5, \"stop_bits\": 2, \"parity\": \"even\" }}}}");
        let parsed = ModbusConfig::from_json(&json);
        assert!(parsed.is_ok(), "合法串口参数必须通过：{json} -> {parsed:?}");
    }

    #[test]
    fn parses_word_order() {
        let json = r#"{"mode": "tcp", "host": "x", "word_order": "cdab"}"#;
        let config = ModbusConfig::from_json(json).unwrap();
        assert_eq!(config.word_order, WordOrder::Cdab);
        // 非法字序由 serde 拒绝。
        let json = r#"{"mode": "tcp", "host": "x", "word_order": "bogus"}"#;
        assert!(ModbusConfig::from_json(json).is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let json = r#"{"mode": "tcp", "host": "x", "unknown": 1}"#;
        assert!(ModbusConfig::from_json(json).is_err());
    }
}
