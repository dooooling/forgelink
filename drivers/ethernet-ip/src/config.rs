//! 连接配置解析与校验（fail-fast，deny_unknown_fields）。
//!
//! JSON 形态（由 collector `devices[].connection` 传入）：
//!
//! ```json
//! {
//!   "mode": "tcp",
//!   "host": "192.168.1.10",
//!   "port": 44818,
//!   "timeout_ms": 3000,
//!   "reconnect": true,
//!   "reconnect_max_attempts": 3,
//!   "reconnect_delay_ms": 1000,
//!   "max_services_per_multi": 20,
//!   "max_bytes_per_multi": 500
//! }
//! ```

use serde::Deserialize;

use crate::error::EtherIpError;

/// EtherNet/IP TCP 连接配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnIpConfig {
    /// 传输模式；当前仅支持 `"tcp"`（EN/IP 封装层）。UDP（ListIdentity
    /// 发现等）不在 V0.3 范围。
    #[serde(default = "default_mode")]
    pub mode: String,
    /// PLC 地址（IP 或主机名，必填）。
    pub host: String,
    /// EtherNet/IP TCP 端口。
    #[serde(default = "default_port")]
    pub port: u16,
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
    /// 单条 Multi-Service 包内的子服务数上限（1..=255——偏移表以 u16
    /// 计数；保守默认 20 降低精简固件兼容风险）。
    #[serde(default = "default_max_services_per_multi")]
    pub max_services_per_multi: usize,
    /// 单条 Multi-Service 包的 CIP 消息字节上限（256..=4000）。500 为
    /// 保守工程值（对应 EN2T 时代 508B 上限留余量），**非协商结果**
    /// ——EN/IP 无类 S7 Setup 的 PDU 协商步骤。
    #[serde(default = "default_max_bytes_per_multi")]
    pub max_bytes_per_multi: usize,
}

fn default_mode() -> String {
    "tcp".to_owned()
}

fn default_port() -> u16 {
    44_818
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

fn default_max_services_per_multi() -> usize {
    20
}

fn default_max_bytes_per_multi() -> usize {
    500
}

/// 缺省重连语义镜像 modbus/S7 驱动。
impl Default for EnIpConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            host: String::new(),
            port: default_port(),
            timeout_ms: default_timeout_ms(),
            reconnect: true,
            reconnect_max_attempts: default_reconnect_max_attempts(),
            reconnect_delay_ms: default_reconnect_delay_ms(),
            max_services_per_multi: default_max_services_per_multi(),
            max_bytes_per_multi: default_max_bytes_per_multi(),
        }
    }
}

/// 从连接 JSON 解析并校验配置。
///
/// # Errors
///
/// JSON 解析失败或字段越界时返回 `config_error`（不可重试）。
pub fn parse_config(json: &str) -> Result<EnIpConfig, EtherIpError> {
    let config: EnIpConfig = serde_json::from_str(json)
        .map_err(|e| EtherIpError::config_error(format!("连接配置非法: {e}")))?;
    validate(&config)?;
    Ok(config)
}

fn validate(config: &EnIpConfig) -> Result<(), EtherIpError> {
    if config.mode != "tcp" {
        return Err(EtherIpError::config_error(format!(
            "不支持的传输模式 '{}'（仅 tcp）",
            config.mode
        )));
    }
    if config.host.trim().is_empty() {
        return Err(EtherIpError::config_error(
            "host 必填且不得为空白".to_owned(),
        ));
    }
    if config.timeout_ms == 0 {
        return Err(EtherIpError::config_error("timeout_ms 必须 > 0".to_owned()));
    }
    if !(1..=255).contains(&config.max_services_per_multi) {
        return Err(EtherIpError::config_error(format!(
            "max_services_per_multi {} 越界（1..=255）",
            config.max_services_per_multi
        )));
    }
    if !(256..=4000).contains(&config.max_bytes_per_multi) {
        return Err(EtherIpError::config_error(format!(
            "max_bytes_per_multi {} 越界（256..=4000）",
            config.max_bytes_per_multi
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config_with_defaults_applied() {
        let c = parse_config(r#"{"host":"192.168.1.10"}"#).unwrap();
        assert_eq!(c.mode, "tcp");
        assert_eq!(c.port, 44_818);
        assert_eq!(c.timeout_ms, 3_000);
        assert_eq!((c.max_services_per_multi, c.max_bytes_per_multi), (20, 500));
    }

    #[test]
    fn rejects_unknown_fields_and_out_of_range() {
        assert!(parse_config(r#"{"host":"h","portz":1}"#).is_err());
        assert!(parse_config(r#"{"host":"h","mode":"udp"}"#).is_err());
        assert!(parse_config(r#"{"host":""}"#).is_err());
        assert!(parse_config(r#"{"host":"h","timeout_ms":0}"#).is_err());
        assert!(parse_config(r#"{"host":"h","max_services_per_multi":0}"#).is_err());
        assert!(parse_config(r#"{"host":"h","max_services_per_multi":256}"#).is_err());
        assert!(parse_config(r#"{"host":"h","max_bytes_per_multi":100}"#).is_err());
        assert!(parse_config(r#"{"host":"h","max_bytes_per_multi":8000}"#).is_err());
    }
}
