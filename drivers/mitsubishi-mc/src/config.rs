//! 连接配置解析与校验（fail-fast，deny_unknown_fields）。
//!
//! JSON 形态（由 collector `devices[].connection` 传入）：
//!
//! ```json
//! {
//!   "mode": "tcp",
//!   "host": "192.168.1.10",
//!   "port": 6006,
//!   "timeout_ms": 3000,
//!   "network_no": 0,
//!   "pc_no": 0,
//!   "module_io": 1023,
//!   "module_station": 0,
//!   "monitoring_timer": 2000
//! }
//! ```
//!
//! 路由区参数随 CPU 与接口模块而异：本机直连 Q/FX5U 典型默认值即可；
//! 经 CC-Link/Ethernet 模块中转时需按现场调整（见 real_device_smoke）。

use serde::Deserialize;

use crate::error::McError;

/// MC TCP 连接配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McConfig {
    /// 传输模式；当前仅支持 `"tcp"`（3E 帧 over TCP）。UDP/串口/4C 帧
    /// 不在 V0.3 范围。
    #[serde(default = "default_mode")]
    pub mode: String,
    /// PLC 地址（IP 或主机名，必填）。
    pub host: String,
    /// MELSEC 通信端口。
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
    /// 网络号（路由区；本机直连为 0）。
    #[serde(default)]
    pub network_no: u8,
    /// PC 号（路由区；本机直连为 0）。
    #[serde(default)]
    pub pc_no: u8,
    /// 请求目标模块 I/O（路由区 u16；CPU 直连典型 0x03FF=1023）。
    #[serde(default = "default_module_io")]
    pub module_io: u16,
    /// 请求目标模块站号（路由区；CPU 直连为 0）。
    #[serde(default)]
    pub module_station: u8,
    /// 监视定时器（入帧原样携带；实际超时控制仍由 socket timeout_ms 实现）。
    #[serde(default = "default_monitoring_timer")]
    pub monitoring_timer: u16,
}

fn default_mode() -> String {
    "tcp".to_owned()
}

fn default_port() -> u16 {
    6_006
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

fn default_module_io() -> u16 {
    0x03FF
}

fn default_monitoring_timer() -> u16 {
    2_000
}

/// 缺省重连语义镜像既有驱动。
impl Default for McConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            host: String::new(),
            port: default_port(),
            timeout_ms: default_timeout_ms(),
            reconnect: true,
            reconnect_max_attempts: default_reconnect_max_attempts(),
            reconnect_delay_ms: default_reconnect_delay_ms(),
            network_no: 0,
            pc_no: 0,
            module_io: default_module_io(),
            module_station: 0,
            monitoring_timer: default_monitoring_timer(),
        }
    }
}

/// 从连接 JSON 解析并校验配置。
///
/// # Errors
///
/// JSON 解析失败或字段越界时返回 `config_error`（不可重试）。
pub fn parse_config(json: &str) -> Result<McConfig, McError> {
    let config: McConfig = serde_json::from_str(json)
        .map_err(|e| McError::config_error(format!("连接配置非法: {e}")))?;
    validate(&config)?;
    Ok(config)
}

fn validate(config: &McConfig) -> Result<(), McError> {
    if config.mode != "tcp" {
        return Err(McError::config_error(format!(
            "不支持的传输模式 '{}'（仅 tcp；UDP/串口/4C 帧不在范围）",
            config.mode
        )));
    }
    if config.host.trim().is_empty() {
        return Err(McError::config_error("host 必填且不得为空白".to_owned()));
    }
    if config.timeout_ms == 0 {
        return Err(McError::config_error("timeout_ms 必须 > 0".to_owned()));
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
        assert_eq!(c.port, 6_006);
        assert_eq!(c.timeout_ms, 3_000);
        assert_eq!(c.module_io, 0x03FF);
        assert_eq!(c.monitoring_timer, 2_000);
        assert_eq!((c.network_no, c.pc_no, c.module_station), (0, 0, 0));
    }

    #[test]
    fn rejects_unknown_fields_and_invalid_values() {
        assert!(parse_config(r#"{"host":"h","portz":1}"#).is_err());
        assert!(parse_config(r#"{"host":"h","mode":"udp"}"#).is_err());
        assert!(parse_config(r#"{"host":""}"#).is_err());
        assert!(parse_config(r#"{"host":"h","timeout_ms":0}"#).is_err());
    }
}
