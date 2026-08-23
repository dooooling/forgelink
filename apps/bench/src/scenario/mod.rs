//! 场景模块：共享上下文（模拟设施常驻 + 公共参数）与各场景实现。

pub mod crash;
pub mod faults;
pub mod load;
pub mod soak;

use std::time::Duration;

use modbus_mock::{MockBehavior, MockServer};
use mqtt_client::mock::MockBroker;

use crate::broker::Northbound;
use crate::cli::{BrokerKind, Resolved};

/// 场景运行上下文。
///
/// Modbus 设备模拟器全场景常驻（collector 的全部设备连它）；北向 broker
/// 按 [`BrokerKind`] 持有——mock 模式的 [`MockBroker`] 在本进程内，real
/// 模式只记录地址（订阅端由记账侧建立）。
pub struct Ctx {
    pub resolved: Resolved,
    pub mock_server: MockServer,
    pub northbound: Northbound,
}

impl Ctx {
    /// 装配模拟设施。必须在生成 workload / 启动订阅**之前**调用
    /// （设备与 broker 地址由此确定）。
    pub async fn new(resolved: Resolved) -> Self {
        let mock_server = MockServer::start(MockBehavior::new());
        let northbound = match resolved.broker {
            BrokerKind::Mock => Northbound::Mock(MockBroker::start().await),
            BrokerKind::Real => Northbound::Real {
                host: resolved
                    .broker_url
                    .as_deref()
                    .and_then(parse_host)
                    .unwrap_or_else(|| "127.0.0.1".to_owned()),
                port: resolved
                    .broker_url
                    .as_deref()
                    .and_then(parse_port)
                    .unwrap_or(1883),
            },
        };
        Self {
            resolved,
            mock_server,
            northbound,
        }
    }

    /// 场景输出目录（`--output-dir/<scenario>`）。
    pub fn output_dir(&self, scenario: &str) -> std::path::PathBuf {
        self.resolved.output_dir.join(scenario)
    }
}

fn parse_host(url: &str) -> Option<String> {
    url.rsplit_once(':').map(|(h, _)| h.to_owned())
}

fn parse_port(url: &str) -> Option<u16> {
    url.rsplit_once(':').and_then(|(_, p)| p.parse().ok())
}

/// 平台收尾方式备注（报告如实标注停机语义差异）。
pub fn stop_semantics_note() -> String {
    if cfg!(windows) {
        "Windows 复验平台：静默排空后强杀收尾（无进程级 SIGTERM）；\
         丢失判定成立于静默前提（WAL/MQTT 在途归零后杀进程）。"
            .to_owned()
    } else {
        "UNIX：静默排空后 SIGTERM 有序停机（超时升级 SIGKILL）。".to_owned()
    }
}

/// 静默等待的通用上限：吞吐场景补传与结算可能需要时间。
pub const QUIESCE_TIMEOUT: Duration = Duration::from_secs(120);
