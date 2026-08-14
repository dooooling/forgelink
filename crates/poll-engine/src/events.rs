//! 轮询输出事件模型。

use driver_sdk::{DriverErrorInfo, DriverReadItem};
use observation_model::RawReadResult;

/// 一次成功批次的原始结果（§22 `driver.read_group` 返回值）。
///
/// `results` 按 `RawReadResult.item_id` 与读取项关联；单项失败保留在
/// `RawReadResult.error` 中，质量与时间戳信息原样保留，供下游 Profile + Domain 映射。
#[derive(Debug, Clone, PartialEq)]
pub struct PollBatch {
    /// 设备 ID。
    pub device_id: String,
    /// 该批次的采集周期。
    pub interval_ms: u64,
    /// 原始读取结果列表。
    pub results: Vec<RawReadResult>,
    /// 批次从发起读到结果返回的耗时（毫秒）。
    pub elapsed_ms: u64,
}

/// 轮询引擎输出事件。
#[derive(Debug, Clone, PartialEq)]
pub enum PollEvent {
    /// 一次成功的批量读取。
    Batch(PollBatch),
    /// 整批失败（连接/超时/驱动错误）。
    ///
    /// `interval_ms` 标识同一设备的哪个周期组（§22 Group）；`items` 为本次失败
    /// 批次的读取项，供下游将对应属性标记为 Bad Quality。仅 `error.retryable`
    /// 为 `true` 的错误会进入指数退避重试（§34.3），永久错误回到周期节律。
    Failed {
        device_id: String,
        interval_ms: u64,
        items: Vec<DriverReadItem>,
        error: DriverErrorInfo,
    },
}
