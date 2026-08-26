//! Driver Rust 契约（§15 Normative）。

use async_trait::async_trait;
use observation_model::{DeviceConnection, DriverErrorInfo};
use tokio::sync::mpsc;

use crate::ProtocolCapabilities;
use crate::RawReadResult;
use crate::items::{DriverCommand, DriverReadItem, DriverWriteItem};
use crate::results::{
    AddressMetadata, DriverBrowseNode, HistoryRequest, RawCommandResult, RawEvent, RawHistoryPage,
    RawWriteResult, SubscriptionId, SubscriptionRequest,
};

/// Driver 内部动态分发接口（§15 Normative）。
///
/// # 边界
///
/// - 以"原始协议结果"为边界：`read` 返回 `RawReadResult`，不生成 `Observation`（§7.3）。
/// - Driver 不知道 `cnc.spindle.1.speed` 这类领域路径（§15）。
/// - v1 明确使用 `async-trait` 保证对象安全，Core 保存 `Box<dyn Driver + Send>`。
/// - Native Plugin 的 C ABI callback 由 `driver-loader` 适配成
///   `tokio::mpsc::Sender<RawEvent>`，本 trait 不暴露 C callback（§15）。
///
/// # 实现规则（§15）
///
/// - capability 为 `false` 的方法必须返回标准 `Unsupported` 错误，不能 panic。
/// - `subscribe/unsubscribe` 仅在 `subscription == true` 时可用；
///   `query_history` 仅在 `history == true` 时可用；`browse` 仅在 `browse == true` 时可用。
#[async_trait]
pub trait Driver: Send {
    /// 建立设备连接；`config` 为 Driver 私有的不透明连接配置。
    async fn connect(&mut self, config: &DeviceConnection) -> Result<(), DriverErrorInfo>;

    /// 断开设备连接。
    async fn disconnect(&mut self) -> Result<(), DriverErrorInfo>;

    /// 声明协议层能力（§13.1）。
    fn protocol_capabilities(&self) -> ProtocolCapabilities;

    /// 校验并规范化 Driver 私有地址（§10）。
    fn validate_address(&self, address: &str) -> Result<AddressMetadata, DriverErrorInfo>;

    /// 批量读取；批量合并与会话串行化属于 Driver，不属于 Core（§23、§24）。
    async fn read(
        &mut self,
        items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo>;

    /// 批量写入。
    async fn write(
        &mut self,
        items: &[DriverWriteItem],
    ) -> Result<Vec<RawWriteResult>, DriverErrorInfo>;

    /// 执行协议命令（由 Profile 映射产生）。
    async fn execute(
        &mut self,
        command: &DriverCommand,
    ) -> Result<RawCommandResult, DriverErrorInfo>;

    /// 浏览设备资源。
    async fn browse(
        &mut self,
        path: Option<&str>,
    ) -> Result<Vec<DriverBrowseNode>, DriverErrorInfo>;

    /// 建立数据变化 / 事件订阅。
    async fn subscribe(
        &mut self,
        request: &SubscriptionRequest,
        sink: mpsc::Sender<RawEvent>,
    ) -> Result<SubscriptionId, DriverErrorInfo>;

    /// 取消订阅；返回后 Plugin/Driver 不得再向该订阅推送事件（§17.8）。
    async fn unsubscribe(&mut self, subscription_id: SubscriptionId)
    -> Result<(), DriverErrorInfo>;

    /// 查询协议历史数据。
    async fn query_history(
        &mut self,
        request: &HistoryRequest,
    ) -> Result<RawHistoryPage, DriverErrorInfo>;
}
