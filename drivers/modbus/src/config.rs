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
//! 未知字段忽略（向前兼容）。

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// 连接模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportMode {
    Tcp,
    Rtu,
}

/// 串口参数（RTU）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerialConfig {
    pub port: String,
    #[serde(default = "default_baud_rate")]
    pub baud_rate: u32,
    #[serde(default = "default_data_bits")]
    pub data_bits: u8,
    #[serde(default = "default_stop_bits")]
    pub stop_bits: u8,
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
    pub message: String,
}

impl ConfigError {
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
    fn rejects_unknown_fields() {
        let json = r#"{"mode": "tcp", "host": "x", "unknown": 1}"#;
        assert!(ModbusConfig::from_json(json).is_err());
    }
}
