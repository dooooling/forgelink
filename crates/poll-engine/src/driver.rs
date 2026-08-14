//! 轮询引擎依赖的驱动访问接口，以及 driver-loader 适配（§22 批量读取）。

use driver_loader::{LoaderError, NativeDriver};
use driver_sdk::{DriverErrorInfo, DriverReadItem};
use observation_model::RawReadResult;

/// poll-engine 依赖的驱动读取接口。
///
/// 由 Driver Manager（基于 driver-loader）或测试 mock 实现。
///
/// # 约定
///
/// - 本接口是**同步阻塞**调用（驱动为同步 Plugin ABI）；poll-engine 负责通过
///   `spawn_blocking` 隔离执行并施加超时。
/// - 返回的 [`RawReadResult`] 必须保留每个 item 的错误与质量信息，poll-engine 不
///   做任何语义解释（语义归一化属于 Profile + Domain，§37.1）。
pub trait PollDriver: Send {
    /// 执行一次批量读取（§22 `driver.read_group(items)`）。
    ///
    /// 返回整体失败（连接错误、超时、加载错误）或单项结果列表；单项失败以
    /// `RawReadResult.error` 表达，不视为整体失败。
    fn read_batch(
        &mut self,
        items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo>;
}

/// driver-loader 适配器：将 [`NativeDriver`] 的同步批量读取暴露为 [`PollDriver`]。
#[derive(Debug)]
pub struct NativeDriverAdapter {
    driver: NativeDriver,
}

impl NativeDriverAdapter {
    /// 包装一个已创建的 Native Driver（`NativeDriver::create`）。
    pub fn new(driver: NativeDriver) -> Self {
        Self { driver }
    }
}

impl PollDriver for NativeDriverAdapter {
    fn read_batch(
        &mut self,
        items: &[DriverReadItem],
    ) -> Result<Vec<RawReadResult>, DriverErrorInfo> {
        self.driver.read(items).map_err(|error| match &error {
            // CallFailed 携带 Plugin 的 `get_last_error_json` 详情（§17.5）：原样保留
            // Driver 的错误码、协议码与可重试标记，不覆盖原始语义。
            LoaderError::CallFailed {
                detail: Some(info), ..
            } => DriverErrorInfo {
                code: info.code.clone(),
                message: info.message.clone(),
                protocol_code: info.protocol_code,
                retryable: info.retryable,
            },
            // 无详情的调用失败：稳定错误码，连接类错误保守标记可重试。
            LoaderError::CallFailed { .. } => DriverErrorInfo {
                code: error.code().to_owned(),
                message: error.to_string(),
                protocol_code: None,
                retryable: true,
            },
            // 加载/ABI/契约/配置类错误为永久错误，重试无意义。
            other => DriverErrorInfo {
                code: other.code().to_owned(),
                message: other.to_string(),
                protocol_code: None,
                retryable: false,
            },
        })
    }
}
