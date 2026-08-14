//! Telemetry Batch（§31.2）。

use std::time::{SystemTime, UNIX_EPOCH};

use observation_model::{DeviceId, Observation};
use serde::{Deserialize, Serialize};

/// Telemetry Batch Envelope 的 schema 标识（§31.2）。
pub const TELEMETRY_SCHEMA: &str = "forgelink.telemetry.v1";

/// Telemetry Batch（§31.2 Telemetry Batch Envelope）。
///
/// 字段语义（更新后）：
///
/// - `sequence` 是**独立批次序号（Batch Sequence）**，与 Observation
///   `sequence` 正交：同一 `device_id` 在单一 Collector session 内单调递增
///   （从 0 开始）；Observation 原有 `sequence` 在批内原样保留，不重新编号。
/// - `message_id` 由 data-pipeline 组包时生成，采用**长度前缀无歧义编码**
///   （与 `domain-model` 的 `observation_id` 同风格，§47）：
///   `{session_len}:{session}{device_len}:{device}{sequence}`，
///   段内允许任意字符（含 `:`、`-`），解析时先读十进制长度再取对应字节数，
///   不存在 `a-b/c/0` 与 `a/b-c/0` 这类拼接碰撞；嵌入 session 保证 Collector
///   重启后消息级去重键不冲突（§31.3）。
/// - `replayed` 恒为 `false`：补传（`replayed = true`）属于
///   Store-and-Forward（§31.4），由本地 Buffer 层标记，本类型不产生。
///
/// 实现 `Serialize`/`Deserialize`，可直接作为 MQTT 报文或 WAL 记录载体。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationBatch {
    /// Schema 标识，恒为 [`TELEMETRY_SCHEMA`]。
    pub schema: String,
    /// 消息级去重键（§31.3，长度前缀无歧义编码）。
    pub message_id: String,
    /// 站点标识（§31.1），来自 data-pipeline 配置，原样透传。
    pub site_id: String,
    /// 本批所属设备，禁止跨设备混批。
    pub device_id: String,
    /// 独立批次序号（从 0 开始，按设备单调递增，§31.2 更新后）。
    pub sequence: u64,
    /// 组包完成时间（UNIX 纳秒）。
    pub sent_at_ns: u64,
    /// 是否补传，data-pipeline 恒为 `false`（§31.4）。
    pub replayed: bool,
    /// 批内 Observation，按到达顺序排列，`sequence` 原样保留。
    pub observations: Vec<Observation>,
}

impl ObservationBatch {
    /// 由 data-pipeline 组包时构造（生成 `message_id` 与 `sent_at_ns`）。
    pub(crate) fn new(
        site_id: &str,
        collector_session_id: &str,
        device_id: DeviceId,
        sequence: u64,
        observations: Vec<Observation>,
    ) -> Self {
        // 长度前缀无歧义编码（P2）：`session`/`device_id` 允许包含 `-`/`:`，
        // 仅用分隔符拼接会碰撞（如 `session="a-b"、device="c"` 与
        // `session="a"、device="b-c"` 生成相同 message_id）。每段先写十进制
        // 长度再写内容，`sequence` 为末段按剩余内容整体解析（同 §47 风格）。
        let message_id = format!(
            "{}:{collector_session_id}{}:{device_id}{sequence}",
            collector_session_id.len(),
            device_id.len(),
        );
        let sent_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos() as u64);
        Self {
            schema: TELEMETRY_SCHEMA.to_owned(),
            message_id,
            site_id: site_id.to_owned(),
            device_id,
            sequence,
            sent_at_ns,
            replayed: false,
            observations,
        }
    }
}
