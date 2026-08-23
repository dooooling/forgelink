//! 连接配置解析与校验（fail-fast，§100 配置校验精神）。
//!
//! JSON 形态（由 collector `devices[].connection` 传入）：
//!
//! ```json
//! {
//!   "mode": "tcp",
//!   "host": "192.168.0.1",
//!   "port": 102,
//!   "rack": 0,
//!   "slot": 2,
//!   "timeout_ms": 3000,
//!   "reconnect": true,
//!   "reconnect_max_attempts": 3,
//!   "reconnect_delay_ms": 1000,
//!   "max_items_per_pdu": 20
//! }
//! ```
//!
//! rack/slot 编码进远端 TSAP：S7-300/400 典型 `slot=2`；S7-1200/1500
//! 典型 `rack=0, slot=0`（部分固件接受 slot=1）。未知字段一律拒绝，
//! 防止配置拼写错误被静默忽略。

use serde::Deserialize;

use crate::error::S7Error;

/// S7 TCP 连接配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S7Config {
    /// 传输模式；当前仅支持 `"tcp"`（ISO-on-TCP，端口 102）。保留字段
    /// 便于未来扩展（如 S7 over UDP 的 PG 通信）。
    #[serde(default = "default_mode")]
    pub mode: String,
    /// PLC 地址（IP 或主机名，必填）。
    pub host: String,
    /// ISO-on-TCP 端口（RFC 1006 常用 102）。
    #[serde(default = "default_port")]
    pub port: u16,
    /// 机架号（TSAP 编码，0..=7）。
    #[serde(default)]
    pub rack: u8,
    /// 槽号（TSAP 编码，0..=31）。
    #[serde(default)]
    pub slot: u8,
    /// 单请求超时（毫秒，> 0）。
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// 断线后是否自动重连。
    #[serde(default = "default_true")]
    pub reconnect: bool,
    /// 重连尝试次数上限。
    #[serde(default = "default_reconnect_max_attempts")]
    pub reconnect_max_attempts: u32,
    /// 重连间隔（毫秒）。
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
    /// 单条 Read/Write Var PDU 的 item 数上限（1..=20 默认；协议允许更
    /// 多但保守值降低真机兼容风险——PLC 对单 PDU var 数各有内部限制）。
    #[serde(default = "default_max_items_per_pdu")]
    pub max_items_per_pdu: usize,
}

fn default_mode() -> String {
    "tcp".to_owned()
}

fn default_port() -> u16 {
    102
}

fn default_timeout_ms() -> u64 {
    3_000
}

fn default_true() -> bool {
    true
}

fn default_reconnect_max_attempts() -> u32 {
    3
}

fn default_reconnect_delay_ms() -> u64 {
    1_000
}

fn default_max_items_per_pdu() -> usize {
    20
}

/// 缺省重连语义镜像 modbus 驱动：`reconnect=true`、3 次 × 1000ms。
impl Default for S7Config {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            host: String::new(),
            port: default_port(),
            rack: 0,
            slot: 0,
            timeout_ms: default_timeout_ms(),
            reconnect: true,
            reconnect_max_attempts: default_reconnect_max_attempts(),
            reconnect_delay_ms: default_reconnect_delay_ms(),
            max_items_per_pdu: default_max_items_per_pdu(),
        }
    }
}

/// 从连接 JSON 解析并校验配置。
///
/// # Errors
///
/// JSON 解析失败或字段越界时返回 `config_error`（不可重试）。
pub fn parse_config(json: &str) -> Result<S7Config, S7Error> {
    let config: S7Config = serde_json::from_str(json)
        .map_err(|e| S7Error::config_error(format!("连接配置非法: {e}")))?;
    validate(&config)?;
    Ok(config)
}

fn validate(config: &S7Config) -> Result<(), S7Error> {
    if config.mode != "tcp" {
        return Err(S7Error::config_error(format!(
            "不支持的传输模式 '{}'（仅 tcp）",
            config.mode
        )));
    }
    if config.host.trim().is_empty() {
        return Err(S7Error::config_error("host 必填且不得为空白".to_owned()));
    }
    if config.rack > 7 {
        return Err(S7Error::config_error(format!(
            "rack {} 越界（0..=7，TSAP 编码约束）",
            config.rack
        )));
    }
    if config.slot > 31 {
        return Err(S7Error::config_error(format!(
            "slot {} 越界（0..=31，TSAP 编码约束）",
            config.slot
        )));
    }
    if config.timeout_ms == 0 {
        return Err(S7Error::config_error("timeout_ms 必须 > 0".to_owned()));
    }
    if config.max_items_per_pdu == 0 || config.max_items_per_pdu > 20 {
        return Err(S7Error::config_error(format!(
            "max_items_per_pdu {} 越界（1..=20）",
            config.max_items_per_pdu
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config_with_defaults_applied() {
        let c = parse_config(r#"{"host":"192.168.0.1","slot":2}"#).unwrap();
        assert_eq!(c.mode, "tcp");
        assert_eq!(c.port, 102);
        assert_eq!(c.rack, 0);
        assert_eq!(c.slot, 2);
        assert_eq!(c.timeout_ms, 3_000);
        assert!(c.reconnect);
        assert_eq!((c.reconnect_max_attempts, c.reconnect_delay_ms), (3, 1_000));
        assert_eq!(c.max_items_per_pdu, 20);
    }

    #[test]
    fn rejects_unknown_fields_and_out_of_range() {
        // 未知字段拒绝（拼写错误不得静默忽略）。
        assert!(parse_config(r#"{"host":"h","ports":102}"#).is_err());
        assert!(parse_config(r#"{"host":"h","mode":"rtu"}"#).is_err());
        assert!(parse_config(r#"{"host":"","port":102}"#).is_err());
        assert!(parse_config(r#"{"host":"h","rack":8}"#).is_err());
        assert!(parse_config(r#"{"host":"h","slot":32}"#).is_err());
        assert!(parse_config(r#"{"host":"h","timeout_ms":0}"#).is_err());
        assert!(parse_config(r#"{"host":"h","max_items_per_pdu":21}"#).is_err());
        assert!(parse_config(r#"{"host":"h","max_items_per_pdu":0}"#).is_err());
    }
}
