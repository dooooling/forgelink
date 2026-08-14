//! data-pipeline 配置（§31.2 更新后默认值）。

use std::time::Duration;

/// 单批 Observation 上限默认值（§31.2 更新后，对齐 §34.2 单批验收目标）。
pub const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
/// 定时刷新周期默认值（§31.2 更新后）。
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// 默认输入有界队列容量（背压边界）。
pub const DEFAULT_INPUT_CAPACITY: usize = 4096;
/// 默认停机有界排空时限。
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// data-pipeline 配置（§31.2）。
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Collector session ID（启动时生成），嵌入 `message_id`（§31.2 更新后）。
    pub collector_session_id: String,
    /// 站点标识（§31.1），透传到每个 [`ObservationBatch`](crate::ObservationBatch)。
    pub site_id: String,
    /// 单批 Observation 上限，达到即输出（默认 1000）。
    pub max_batch_size: usize,
    /// 定时刷新周期：周期结束时输出所有非空 partial 批次（默认 1s）。
    pub flush_interval: Duration,
    /// 输入有界队列容量（背压边界，默认 4096）。
    pub input_capacity: usize,
    /// 停机时排空剩余 partial 批次的时限（有界排空，默认 5s）。
    pub drain_timeout: Duration,
}

impl PipelineConfig {
    /// 使用 §31.2 更新后默认值构建配置。
    ///
    /// `site_id` 为站点标识（§31.1）；`collector_session_id` 由 Collector
    /// 启动时生成（§31.3），两者均不可为空。
    ///
    /// 输出通道容量由调用方创建时决定（`Pipeline::spawn` 只接收通道），
    /// 背压以输入队列容量和输出通道容量共同界定。
    pub fn new(site_id: impl Into<String>, collector_session_id: impl Into<String>) -> Self {
        Self {
            collector_session_id: collector_session_id.into(),
            site_id: site_id.into(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            input_capacity: DEFAULT_INPUT_CAPACITY,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        }
    }

    /// 校验配置合法性；返回错误原因字符串。
    ///
    /// 非法配置由 [`Pipeline::spawn`](crate::Pipeline::spawn) 拒绝，
    /// 不 panic（公共 API 不得因配置断言崩溃）。
    pub fn validate(&self) -> Result<(), String> {
        if self.site_id.is_empty() {
            return Err("site_id 不能为空".to_owned());
        }
        if self.collector_session_id.is_empty() {
            return Err("collector_session_id 不能为空".to_owned());
        }
        if self.max_batch_size == 0 {
            return Err("max_batch_size 必须大于 0".to_owned());
        }
        if self.flush_interval.is_zero() {
            return Err("flush_interval 必须大于 0".to_owned());
        }
        if self.input_capacity == 0 {
            return Err("input_capacity 必须大于 0".to_owned());
        }
        if self.drain_timeout.is_zero() {
            return Err("drain_timeout 必须大于 0".to_owned());
        }
        Ok(())
    }
}
