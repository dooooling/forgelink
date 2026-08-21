//! 控制审计事件模型（§90 Normative）。
//!
//! 每个反向控制必须记录：谁执行、何时执行、来源地址、目标设备、命令名称、
//! 命令参数摘要、`request_id`、执行结果、设备错误码、耗时（§90）。
//!
//! # 敏感信息脱敏（§90 数据边界）
//!
//! 审计**不记录**密码、Token、私钥和完整敏感载荷：字符串/字节序列只记录
//! 长度与哈希前缀；数组/结构体只记录元素个数；数值与布尔值保留原值
//! （设备运行参数不属于凭据）。实现见 [`summarize_value`]。
//!
//! # 可替换性
//!
//! 通过 [`AuditSink`] trait 抽象；本 crate 提供 [`NoopAuditSink`]（默认）与
//! [`MemoryAuditSink`]（测试/内存收集），上层可用数据库等后端替换。

use std::sync::Mutex;

use observation_model::{
    CommandParameter, CommandRiskLevel, ControlStatus, DeviceId, PropertyWriteItem, TimestampNs,
    Value,
};
use serde::{Deserialize, Serialize};

/// 审计操作类型（§90：命令名称/操作）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOperation {
    PropertyWrite,
    CommandExecute,
}

/// 审计参数摘要（脱敏）。
///
/// - `kind`：值类别（`bool` / `numeric` / `string` / `bytes` / `array` / `struct`）；
/// - `summary`：摘要内容；字符串/字节序列不包含原始内容。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditParameter {
    pub name: String,
    pub kind: &'static str,
    pub summary: String,
}

/// 审计事件（§90）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    /// 谁执行（§90）。
    pub user: String,
    /// 来源地址（如 `rest:127.0.0.1:53211`；§90 来源地址）。
    pub source: String,
    pub namespace: String,
    /// 目标设备（§90）。
    pub device_id: DeviceId,
    pub request_id: String,
    /// 操作类型（§90：命令名称/属性写入）。
    pub operation: AuditOperation,
    /// 命令 ID 或属性路径列表（以 `,` 连接）。
    pub target: String,
    /// 参数摘要（脱敏后，§90 命令参数）。
    pub parameters: Vec<AuditParameter>,
    /// 风险等级（§86、§90）。
    pub risk_level: Option<CommandRiskLevel>,
    /// 执行结果（§90）。
    pub status: ControlStatus,
    /// 稳定错误码 / 设备错误码（§90 设备错误码）。
    pub error_code: Option<String>,
    pub protocol_code: Option<i64>,
    /// 耗时（毫秒，§90 耗时）。
    pub duration_ms: u64,
    /// 审计时间戳。
    pub occurred_at_ns: TimestampNs,
}

/// 审计目标（按操作生成 target 与参数摘要）。
#[derive(Debug, Clone)]
pub enum AuditTarget<'a> {
    PropertyWrite(&'a [PropertyWriteItem]),
    Command {
        command: &'a str,
        parameters: &'a [CommandParameter],
    },
}

/// 审计输出（§90，接口可替换）。
///
/// `async`（四审 P2）：实现可能涉及磁盘/网络写入；引擎侧以有界超时调用
/// （[`crate::policy::ControlPolicy::audit_timeout_ms`]），超时放弃该条
/// 审计并记录错误日志——慢审计不得阻塞控制 worker。实现方应尽量非阻塞
/// （内部有界队列 + 后台落盘为推荐模式），且 `record` 被超时取消时不得
/// 留下半写状态。
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync {
    async fn record(&self, event: AuditEvent);
}

/// 带超时的审计写入（引擎统一入口，四审 P2）。
///
/// 超时放弃该条事件并记录 `audit_timeout` 错误日志：控制可用性优先于
/// 单条审计完整性（阻塞控制 worker 的代价更高）；丢失会显式留痕。
pub(crate) async fn record_bounded(
    sink: &std::sync::Arc<dyn AuditSink>,
    timeout_ms: u64,
    event: AuditEvent,
) {
    if tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        sink.record(event),
    )
    .await
    .is_err()
    {
        tracing::error!(
            component = "control-engine",
            error_code = "audit_timeout",
            "审计写入超时，本条审计事件丢失（控制继续）"
        );
    }
}

/// 丢弃审计事件的输出（默认值）。
#[derive(Debug, Default)]
pub struct NoopAuditSink;

#[async_trait::async_trait]
impl AuditSink for NoopAuditSink {
    async fn record(&self, _event: AuditEvent) {}
}

/// 内存收集审计输出（测试与进程内查看）。
#[derive(Debug, Default)]
pub struct MemoryAuditSink {
    events: Mutex<Vec<AuditEvent>>,
}

impl MemoryAuditSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// 已记录的全部事件。
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .expect("MemoryAuditSink 锁被毒化")
            .clone()
    }

    /// 清空并返回已记录事件。
    pub fn drain(&self) -> Vec<AuditEvent> {
        let mut events = self.events.lock().expect("MemoryAuditSink 锁被毒化");
        std::mem::take(&mut *events)
    }
}

#[async_trait::async_trait]
impl AuditSink for MemoryAuditSink {
    async fn record(&self, event: AuditEvent) {
        self.events
            .lock()
            .expect("MemoryAuditSink 锁被毒化")
            .push(event);
    }
}

/// 生成审计参数摘要（脱敏）。
pub fn summarize_value(value: &Value) -> AuditParameter {
    match value {
        Value::Bool(b) => AuditParameter {
            name: "value".to_owned(),
            kind: "bool",
            summary: b.to_string(),
        },
        Value::I8(v) => numeric("i8", v.to_string()),
        Value::I16(v) => numeric("i16", v.to_string()),
        Value::I32(v) => numeric("i32", v.to_string()),
        Value::I64(v) => numeric("i64", v.to_string()),
        Value::U8(v) => numeric("u8", v.to_string()),
        Value::U16(v) => numeric("u16", v.to_string()),
        Value::U32(v) => numeric("u32", v.to_string()),
        Value::U64(v) => numeric("u64", v.to_string()),
        Value::F32(v) => numeric("f32", v.to_string()),
        Value::F64(v) => numeric("f64", v.to_string()),
        Value::String(s) => redact_hashed("string", s.as_bytes()),
        Value::Bytes(b) => redact_hashed("bytes", b),
        Value::Array(items) => AuditParameter {
            name: "value".to_owned(),
            kind: "array",
            summary: format!("<array> 元素数={}", items.len()),
        },
        Value::Struct(fields) => AuditParameter {
            name: "value".to_owned(),
            kind: "struct",
            summary: format!(
                "<struct> 字段={} [{}]",
                fields.len(),
                fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        },
    }
}

fn numeric(kind: &'static str, summary: String) -> AuditParameter {
    AuditParameter {
        name: "value".to_owned(),
        kind,
        summary,
    }
}

/// 进程级随机盐（四审 P2）：低熵密码/PIN 的普通 SHA-256 前缀可被攻击者
/// 离线字典枚举；加盐后跨进程不可关联、不可预计算。盐来自 `RandomState`
/// 的随机种子（std 无 OS 随机 API，该构造每实例产生不可预测的哈希密钥）。
fn process_salt() -> &'static [u8; 32] {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::OnceLock;
    static SALT: OnceLock<[u8; 32]> = OnceLock::new();
    SALT.get_or_init(|| {
        let mut salt = [0u8; 32];
        for chunk in salt.chunks_mut(8) {
            let v = std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish();
            chunk.copy_from_slice(&v.to_le_bytes()[..chunk.len()]);
        }
        salt
    })
}

/// 哈希摘要：只暴露长度与加盐 SHA-256 前 12 位，绝不落完整内容。
///
/// 四审 P2：盐为进程启动时随机生成——同一秘密在进程内前缀稳定（可做
/// 关联分析），跨进程/重启不同，低熵凭据无法被预计算字典枚举。
fn redact_hashed(kind: &'static str, data: &[u8]) -> AuditParameter {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(process_salt());
    hasher.update(data);
    let digest = hasher.finalize();
    let prefix = format!("{digest:x}");
    AuditParameter {
        name: "value".to_owned(),
        kind,
        summary: format!(
            "<{kind}> len={} sha256={}",
            data.len(),
            &prefix[..prefix.len().min(12)]
        ),
    }
}

/// 生成操作审计参数列表（§90 命令参数，脱敏）。
pub fn summarize_parameters(target: &AuditTarget<'_>) -> (String, Vec<AuditParameter>) {
    match target {
        AuditTarget::PropertyWrite(items) => {
            let paths = items
                .iter()
                .map(|i| i.path.as_str())
                .collect::<Vec<_>>()
                .join(",");
            let params = items
                .iter()
                .map(|item| {
                    let mut p = summarize_value(&item.value);
                    p.name = item.path.clone();
                    p
                })
                .collect();
            (paths, params)
        }
        AuditTarget::Command {
            command,
            parameters,
        } => {
            let params = parameters
                .iter()
                .map(|param| {
                    let mut p = summarize_value(&param.value);
                    p.name = param.name.clone();
                    p
                })
                .collect();
            ((*command).to_owned(), params)
        }
    }
}

/// 供上层按请求组装完整审计事件（内部 helper）。
///
/// `target_text` 与 `parameters` 由 [`summarize_parameters`] 预生成
/// （提交时），此处只做组装。
pub(crate) fn build_event(
    user: &str,
    source: &str,
    namespace: &str,
    device_id: &DeviceId,
    request_id: &str,
    operation: AuditOperation,
    target_text: &str,
    parameters: &[AuditParameter],
    risk_level: Option<CommandRiskLevel>,
    status: ControlStatus,
    error_code: Option<String>,
    protocol_code: Option<i64>,
    duration_ms: u64,
    occurred_at_ns: TimestampNs,
) -> AuditEvent {
    AuditEvent {
        user: user.to_owned(),
        source: source.to_owned(),
        namespace: namespace.to_owned(),
        device_id: device_id.clone(),
        request_id: request_id.to_owned(),
        operation,
        target: target_text.to_owned(),
        parameters: parameters.to_vec(),
        risk_level,
        status,
        error_code,
        protocol_code,
        duration_ms,
        occurred_at_ns,
    }
}

/// 内部序列化辅助（仅测试引用）。
#[doc(hidden)]
pub fn _assert_types() {}

#[cfg(test)]
mod tests {
    use observation_model::CommandParameter;

    use super::*;

    #[test]
    fn string_value_is_redacted_not_recorded() {
        let secret = "s3cr3t-password";
        let p = summarize_value(&Value::String(secret.to_owned()));
        assert_eq!(p.kind, "string");
        assert!(!p.summary.contains(secret), "摘要不得包含原始字符串");
        assert!(p.summary.contains("len=15"));
        assert!(p.summary.contains("sha256="));
    }

    #[test]
    fn bytes_value_is_redacted() {
        let payload = b"private-key-bytes";
        let p = summarize_value(&Value::Bytes(payload.to_vec()));
        assert_eq!(p.kind, "bytes");
        assert!(!p.summary.contains("private-key"));
        assert!(p.summary.contains("len="));
    }

    #[test]
    fn numeric_values_are_recorded_verbatim() {
        assert_eq!(summarize_value(&Value::F64(50.0)).summary, "50");
        assert_eq!(summarize_value(&Value::Bool(true)).summary, "true");
        assert_eq!(summarize_value(&Value::U64(5000)).summary, "5000");
    }

    #[test]
    fn array_and_struct_only_structural_summary() {
        let arr = summarize_value(&Value::Array(vec![Value::I32(1), Value::I32(2)]));
        assert_eq!(arr.kind, "array");
        assert!(!arr.summary.contains("1"));

        let s = summarize_value(&Value::Struct(vec![]));
        assert_eq!(s.kind, "struct");
    }

    #[test]
    fn command_parameters_summarized_with_names() {
        let params = vec![
            CommandParameter {
                name: "frequency".to_owned(),
                value: Value::F64(50.0),
            },
            CommandParameter {
                name: "password".to_owned(),
                value: Value::String("hunter2".to_owned()),
            },
        ];
        let (target, summarized) = summarize_parameters(&AuditTarget::Command {
            command: "drive.set",
            parameters: &params,
        });
        assert_eq!(target, "drive.set");
        assert_eq!(summarized.len(), 2);
        assert_eq!(summarized[0].name, "frequency");
        assert_eq!(summarized[0].summary, "50");
        assert_eq!(summarized[1].name, "password");
        assert!(!summarized[1].summary.contains("hunter2"));
    }

    #[tokio::test]
    async fn memory_sink_collects_events() {
        let sink = MemoryAuditSink::new();
        let event = AuditEvent {
            user: "alice".to_owned(),
            source: "rest:1.2.3.4".to_owned(),
            namespace: "plant-a".to_owned(),
            device_id: "dev-1".to_owned(),
            request_id: "cmd-1".to_owned(),
            operation: AuditOperation::CommandExecute,
            target: "drive.reset".to_owned(),
            parameters: vec![],
            risk_level: Some(CommandRiskLevel::Medium),
            status: ControlStatus::Succeeded,
            error_code: None,
            protocol_code: None,
            duration_ms: 12,
            occurred_at_ns: 1_000,
        };
        sink.record(event.clone()).await;
        assert_eq!(sink.events(), vec![event]);
    }
}
