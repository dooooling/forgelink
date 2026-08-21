//! 幂等 Control Journal（§80.1 Normative）。
//!
//! 幂等键：`(namespace, device_id, request_id)`。
//!
//! # 语义（§80.1）
//!
//! - 下发 Driver 前先把 canonical payload hash 和状态持久化到 Journal；
//! - 同 key + 同 payload：返回已有状态/结果，不重复执行（[`JournalDecision::Duplicate`]）；
//! - 同 key + 不同 payload：返回 `Conflict`（[`JournalDecision::Conflict`]）；
//! - MVP 幂等记录至少保留 24 小时，可配置延长（`expires_at_ns` 过期后视为新请求）；
//! - 重启后恢复 Journal：未结算（`Accepted`/`Running`）的记录标记为 `Indeterminate`
//!   （执行状态丢失、结果不确定），禁止盲目自动重放；
//! - `High/Critical` 的 `Indeterminate` 控制禁止自动重放（本引擎对同一 key
//!   一律返回既有结果，不自动重放，§80.1）。

use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use observation_model::{ControlError, ControlResult, ControlStatus, DeviceId, TimestampNs};
use serde::{Deserialize, Serialize};

/// 幂等键（§80.1）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey {
    pub namespace: String,
    pub device_id: DeviceId,
    pub request_id: String,
}

/// 幂等记录（§80.1）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub key: IdempotencyKey,
    /// canonical payload hash（SHA-256）。
    pub payload_hash: String,
    /// 当前状态；未结算记录为 `Accepted`/`Running`。
    pub status: ControlStatus,
    pub created_at_ns: TimestampNs,
    /// 幂等保留截止；过期后同一 key 视为新请求（§80.1 ≥24h）。
    pub expires_at_ns: TimestampNs,
    /// 结算结果（`None` 表示尚未完成）。
    pub result: Option<ControlResult>,
}

/// 登记判定（§80.1）。
#[derive(Debug, Clone, PartialEq)]
pub enum JournalDecision {
    /// 新记录已持久化。
    Inserted,
    /// 同 key + 同 payload：返回既有记录，不重复执行。
    Duplicate(JournalEntry),
    /// 同 key + 不同 payload：返回 Conflict（携带既有记录）。
    Conflict { existing: JournalEntry },
}

/// Journal 错误。
#[derive(Debug)]
pub enum JournalError {
    Io(std::io::Error),
    /// 文件中的一行无法解析（行号从 1 开始）。
    Corrupt {
        line: usize,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Io(e) => write!(f, "Journal IO 错误: {e}"),
            JournalError::Corrupt { line } => write!(f, "Journal 第 {line} 行损坏"),
        }
    }
}

impl std::error::Error for JournalError {}

/// 幂等 Journal 抽象（§80.1，接口可替换）。
///
/// 实现必须是线程安全的；`try_insert` / `settle` 必须在返回前完成持久化
/// （本 crate 的 [`FileJournal`] 每次写入后 `sync_all`）。
pub trait ControlJournal: Send + Sync {
    /// 登记幂等记录（§80.1：下发 Driver 前先持久化）。
    ///
    /// 同 key 已有未过期记录时按 payload 判定 `Duplicate` / `Conflict`；
    /// 过期记录视为不存在（可覆盖）。
    ///
    /// **失败语义**：持久化失败必须返回 `Err`——引擎收到 `Err` 后不得继续
    /// 下发 Driver（否则进程崩溃后缺少幂等记录，重试可能重复执行控制动作）。
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        payload_hash: String,
        created_at_ns: TimestampNs,
        expires_at_ns: TimestampNs,
    ) -> Result<JournalDecision, JournalError>;

    /// 结算（覆盖状态与结果；幂等可重复调用）。
    fn settle(&self, key: &IdempotencyKey, result: &ControlResult) -> Result<(), JournalError>;

    /// 查询未过期记录（触发一次惰性过期清理）。
    fn get(&self, key: &IdempotencyKey) -> Option<JournalEntry>;

    /// 清理过期记录，返回清理条数（§80.1 保留期）。
    fn purge_expired(&self, now_ns: TimestampNs) -> usize;
}

/// 内存版 Journal（测试与进程内默认）。
///
/// 不跨进程持久化；跨重启恢复请使用 [`FileJournal`]。
#[derive(Debug, Default)]
pub struct InMemoryJournal {
    entries: Mutex<HashMap<IdempotencyKey, JournalEntry>>,
}

impl InMemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlJournal for InMemoryJournal {
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        payload_hash: String,
        created_at_ns: TimestampNs,
        expires_at_ns: TimestampNs,
    ) -> Result<JournalDecision, JournalError> {
        let mut entries = self.entries.lock().expect("InMemoryJournal 锁被毒化");
        if let Some(existing) = entries.get(key) {
            // 过期记录视为不存在。
            if existing.expires_at_ns >= created_at_ns {
                if existing.payload_hash == payload_hash {
                    return Ok(JournalDecision::Duplicate(existing.clone()));
                }
                return Ok(JournalDecision::Conflict {
                    existing: existing.clone(),
                });
            }
        }
        let entry = JournalEntry {
            key: key.clone(),
            payload_hash,
            status: ControlStatus::Running,
            created_at_ns,
            expires_at_ns,
            result: None,
        };
        entries.insert(key.clone(), entry.clone());
        Ok(JournalDecision::Inserted)
    }

    fn settle(&self, key: &IdempotencyKey, result: &ControlResult) -> Result<(), JournalError> {
        let mut entries = self.entries.lock().expect("InMemoryJournal 锁被毒化");
        let Some(entry) = entries.get_mut(key) else {
            return Ok(());
        };
        entry.status = result.status;
        entry.result = Some(result.clone());
        Ok(())
    }

    fn get(&self, key: &IdempotencyKey) -> Option<JournalEntry> {
        let entries = self.entries.lock().expect("InMemoryJournal 锁被毒化");
        entries.get(key).cloned()
    }

    fn purge_expired(&self, now_ns: TimestampNs) -> usize {
        let mut entries = self.entries.lock().expect("InMemoryJournal 锁被毒化");
        let before = entries.len();
        entries.retain(|_, e| e.expires_at_ns >= now_ns);
        before - entries.len()
    }
}

/// 磁盘 JSONL 幂等 Journal（§103 嵌入式存储精神的轻量实现）。
///
/// - 追加式日志：`insert` / `settle` 各写一行 JSON，写入后 `sync_all`
///   （低写入频率，保证崩溃后已确认状态不丢失）；
/// - 打开时顺序重放：`settle` 覆盖对应 `insert` 的状态与结果；
/// - 未结算（`Accepted`/`Running`）记录恢复为 `Indeterminate`
///   （执行进程已终止，结果未知，禁止盲目重放，§80.1）；
/// - 过期记录在加载与 `get`/`purge_expired` 时惰性清理，并在加载时压缩文件。
#[derive(Debug)]
pub struct FileJournal {
    path: PathBuf,
    inner: Mutex<FileJournalInner>,
}

#[derive(Debug)]
struct FileJournalInner {
    file: File,
    entries: HashMap<IdempotencyKey, JournalEntry>,
}

impl FileJournal {
    /// 打开（或创建）Journal 文件并重放已有记录。
    ///
    /// `now_ns` 用于判定过期；重放后若发现过期记录，会压缩重写文件。
    /// 损坏行不静默跳过（P1-F：丢弃 Insert 会重复下发控制动作），直接返回
    /// [`JournalError::Corrupt`]。
    pub fn open(path: &Path, now_ns: TimestampNs) -> Result<Self, JournalError> {
        let mut entries: HashMap<IdempotencyKey, JournalEntry> = HashMap::new();
        // 因过期被跳过的 Insert 的 key：其后续 Settle 同样跳过（正常生命周期，
        // 触发压缩即可），不得误判为孤立 Settle（三审回归修复）。
        let mut expired_keys: std::collections::HashSet<IdempotencyKey> =
            std::collections::HashSet::new();
        let mut need_compact = false;

        if path.exists() {
            let reader = BufReader::new(File::open(path)?);
            for (line_no, line) in reader.lines().enumerate() {
                let line_no = line_no + 1;
                let line = line.map_err(JournalError::Io)?;
                if line.trim().is_empty() {
                    continue;
                }
                // P1-F：损坏行必须 fail-closed——静默丢弃可能丢失已执行请求的
                // Insert 记录，重试会重复下发控制动作。返回 Corrupt{line}。
                let record: Record = serde_json::from_str(&line)
                    .map_err(|_| JournalError::Corrupt { line: line_no })?;
                match record {
                    Record::Insert {
                        key,
                        payload_hash,
                        status,
                        created_at_ns,
                        expires_at_ns,
                    } => {
                        if expires_at_ns < now_ns {
                            // 过期记录直接丢弃（触发压缩）。
                            expired_keys.insert(key);
                            need_compact = true;
                            continue;
                        }
                        entries.insert(
                            key.clone(),
                            JournalEntry {
                                key,
                                payload_hash,
                                status,
                                created_at_ns,
                                expires_at_ns,
                                result: None,
                            },
                        );
                    }
                    Record::Settle { key, result } => {
                        // 三审 P1：Settle 缺少对应 Insert 时不得静默跳过——若 Insert
                        // 因损坏丢失，静默跳过会让重启后同一请求再次执行，破坏
                        // 幂等安全（§80.1）。按 Corrupt fail-closed。
                        // 例外：Insert 因过期被跳过时，其 Settle 属正常生命周期，
                        // 一并跳过（触发压缩），不算孤立。
                        if !entries.contains_key(&key) {
                            if expired_keys.remove(&key) {
                                continue;
                            }
                            return Err(JournalError::Corrupt { line: line_no });
                        }
                        let entry = entries.get_mut(&key).expect("已确认存在");
                        entry.status = result.status;
                        entry.result = Some(result.clone());
                    }
                }
            }
        }

        // 未结算记录恢复为 Indeterminate（结果不确定，禁止盲目重放）并生成
        // 可见结果——`status()` 只返回 `entry.result`，P1-E：调用方必须能查询
        // 到"不确定"状态，而不是得到 None。
        for entry in entries.values_mut() {
            if entry.status == ControlStatus::Accepted || entry.status == ControlStatus::Running {
                entry.status = ControlStatus::Indeterminate;
                entry.result = Some(ControlResult {
                    request_id: entry.key.request_id.clone(),
                    namespace: entry.key.namespace.clone(),
                    device_id: entry.key.device_id.clone(),
                    status: ControlStatus::Indeterminate,
                    started_at_ns: None,
                    completed_at_ns: Some(now_ns),
                    result: None,
                    error: Some(ControlError {
                        code: "EXECUTION_INTERRUPTED".to_owned(),
                        message: "执行状态未知（进程可能已重启）".to_owned(),
                        details: None,
                    }),
                });
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let inner = FileJournalInner { file, entries };
        let journal = Self {
            path: path.to_owned(),
            inner: Mutex::new(inner),
        };

        if need_compact {
            journal.compact(now_ns)?;
        }
        Ok(journal)
    }

    /// 压缩：仅保留未过期记录并重写文件（临时文件 + 原子替换）。
    fn compact(&self, now_ns: TimestampNs) -> Result<(), JournalError> {
        let mut inner = self.inner.lock().expect("FileJournal 锁被毒化");
        let temp = self.path.with_extension("tmp");
        let mut out = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        for entry in inner.entries.values() {
            if entry.expires_at_ns < now_ns {
                continue;
            }
            let record = Record::Insert {
                key: entry.key.clone(),
                payload_hash: entry.payload_hash.clone(),
                status: entry.status,
                created_at_ns: entry.created_at_ns,
                expires_at_ns: entry.expires_at_ns,
            };
            let line = serde_json::to_string(&record)?;
            writeln!(out, "{line}")?;
            if let Some(result) = &entry.result {
                let settle = Record::Settle {
                    key: entry.key.clone(),
                    result: result.clone(),
                };
                let line = serde_json::to_string(&settle)?;
                writeln!(out, "{line}")?;
            }
        }
        out.flush()?;
        out.sync_all()?;
        drop(out);
        std::fs::rename(&temp, &self.path)?;
        let file = OpenOptions::new().append(true).open(&self.path)?;
        inner.file = file;
        Ok(())
    }

    /// 追加一行（调用方须已持有 `inner` 锁——避免 std Mutex 重入死锁）。
    fn write_record(inner: &mut FileJournalInner, record: &Record) -> Result<(), JournalError> {
        let line = serde_json::to_string(record)?;
        inner.file.write_all(line.as_bytes())?;
        inner.file.write_all(b"\n")?;
        inner.file.flush()?;
        inner.file.sync_all()?;
        Ok(())
    }
}

impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        JournalError::Io(e)
    }
}

impl From<serde_json::Error> for JournalError {
    fn from(e: serde_json::Error) -> Self {
        JournalError::Corrupt { line: 0 }.from_json(e)
    }
}

impl JournalError {
    fn from_json(self, _inner: serde_json::Error) -> Self {
        self
    }
}

impl ControlJournal for FileJournal {
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        payload_hash: String,
        created_at_ns: TimestampNs,
        expires_at_ns: TimestampNs,
    ) -> Result<JournalDecision, JournalError> {
        let mut inner = self.inner.lock().expect("FileJournal 锁被毒化");
        if let Some(existing) = inner.entries.get(key) {
            if existing.expires_at_ns >= created_at_ns {
                if existing.payload_hash == payload_hash {
                    return Ok(JournalDecision::Duplicate(existing.clone()));
                }
                return Ok(JournalDecision::Conflict {
                    existing: existing.clone(),
                });
            }
        }
        let entry = JournalEntry {
            key: key.clone(),
            payload_hash: payload_hash.clone(),
            status: ControlStatus::Running,
            created_at_ns,
            expires_at_ns,
            result: None,
        };
        let record = Record::Insert {
            key: key.clone(),
            payload_hash,
            status: ControlStatus::Running,
            created_at_ns,
            expires_at_ns,
        };
        // 先落盘再登记内存：失败必须向上传播（引擎据此拒绝下发，§80.1）。
        Self::write_record(&mut inner, &record)?;
        inner.entries.insert(key.clone(), entry.clone());
        Ok(JournalDecision::Inserted)
    }

    fn settle(&self, key: &IdempotencyKey, result: &ControlResult) -> Result<(), JournalError> {
        let mut inner = self.inner.lock().expect("FileJournal 锁被毒化");
        if !inner.entries.contains_key(key) {
            return Ok(());
        }
        // P1-D：先写盘、成功后改内存——写盘失败时内存仍保持 Running，
        // 与磁盘（Running）一致，避免"当前进程宣称成功、重启恢复 Indeterminate"
        // 的内外不一致。
        let record = Record::Settle {
            key: key.clone(),
            result: result.clone(),
        };
        Self::write_record(&mut inner, &record)?;
        let entry = inner.entries.get_mut(key).expect("已确认存在");
        entry.status = result.status;
        entry.result = Some(result.clone());
        Ok(())
    }

    fn get(&self, key: &IdempotencyKey) -> Option<JournalEntry> {
        let inner = self.inner.lock().expect("FileJournal 锁被毒化");
        inner.entries.get(key).cloned()
    }

    fn purge_expired(&self, now_ns: TimestampNs) -> usize {
        let mut inner = self.inner.lock().expect("FileJournal 锁被毒化");
        let before = inner.entries.len();
        inner.entries.retain(|_, e| e.expires_at_ns >= now_ns);
        let removed = before - inner.entries.len();
        drop(inner);
        if removed > 0 {
            let _ = self.compact(now_ns);
        }
        removed
    }
}

/// Journal 磁盘 I/O 阻塞任务的全局并发上限（三审 P2）：有界并发，防止大量
/// 请求同时堆积阻塞任务（项目要求的有界并发、背压优先）。
pub(crate) const JOURNAL_IO_CONCURRENCY: usize = 8;

/// 在阻塞线程池上执行幂等登记（P2-H：`write_all`/`flush`/`sync_all` 等磁盘
/// I/O 不占用 Tokio worker 线程；std Mutex 的等待也在阻塞线程上发生，不阻塞
/// 调度器）。`gate` 限制同时执行的 Journal 阻塞任务数（三审 P2）。
pub(crate) async fn insert_record(
    journal: &Arc<dyn ControlJournal>,
    gate: &Arc<tokio::sync::Semaphore>,
    key: &IdempotencyKey,
    payload_hash: String,
    created_at_ns: TimestampNs,
    expires_at_ns: TimestampNs,
) -> Result<JournalDecision, JournalError> {
    let journal = journal.clone();
    let gate = gate.clone();
    let key = key.clone();
    let _permit = gate
        .acquire_owned()
        .await
        .expect("Journal 并发闸门不应被关闭");
    tokio::task::spawn_blocking(move || {
        journal.try_insert(&key, payload_hash, created_at_ns, expires_at_ns)
    })
    .await
    .expect("Journal 阻塞任务不可取消")
}

/// 在阻塞线程池上执行幂等结算（P2-H，同上）。
pub(crate) async fn settle_record(
    journal: &Arc<dyn ControlJournal>,
    gate: &Arc<tokio::sync::Semaphore>,
    key: &IdempotencyKey,
    result: &ControlResult,
) -> Result<(), JournalError> {
    let journal = journal.clone();
    let gate = gate.clone();
    let key = key.clone();
    let result = result.clone();
    let _permit = gate
        .acquire_owned()
        .await
        .expect("Journal 并发闸门不应被关闭");
    tokio::task::spawn_blocking(move || journal.settle(&key, &result))
        .await
        .expect("Journal 阻塞任务不可取消")
}

/// 在阻塞线程池上执行过期清理 + 查询（三审 P2：`status()` 等异步调用点的
/// 同步磁盘 I/O 移出 Tokio worker，并受同一并发闸门约束）。
pub(crate) async fn purge_and_get(
    journal: &Arc<dyn ControlJournal>,
    gate: &Arc<tokio::sync::Semaphore>,
    key: &IdempotencyKey,
    now_ns: TimestampNs,
) -> Option<JournalEntry> {
    let journal = journal.clone();
    let gate = gate.clone();
    let key = key.clone();
    let _permit = gate
        .acquire_owned()
        .await
        .expect("Journal 并发闸门不应被关闭");
    tokio::task::spawn_blocking(move || {
        let _ = journal.purge_expired(now_ns);
        journal.get(&key)
    })
    .await
    .expect("Journal 阻塞任务不可取消")
}

/// 日志记录（JSONL 行的种类）。
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Record {
    Insert {
        key: IdempotencyKey,
        payload_hash: String,
        status: ControlStatus,
        created_at_ns: TimestampNs,
        expires_at_ns: TimestampNs,
    },
    Settle {
        key: IdempotencyKey,
        result: ControlResult,
    },
}

/// canonical payload 哈希（SHA-256 十六进制）。
///
/// 以 `serde_json` 序列化 `ControlOperation` 为准；同一构建内字段顺序确定，
/// 可作为同进程/同版本间的 canonical 表示（§80.1）。
pub fn payload_hash(operation: &observation_model::ControlOperation) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(operation).expect("ControlOperation 序列化不应失败");
    let digest = Sha256::digest(&bytes);
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use observation_model::{
        CommandRequest, ControlOperation, ControlResult, ControlStatus, PropertyWriteItem,
        PropertyWriteRequest, Value,
    };

    use super::*;

    fn key(id: &str) -> IdempotencyKey {
        IdempotencyKey {
            namespace: "plant-a".to_owned(),
            device_id: "fanuc01".to_owned(),
            request_id: id.to_owned(),
        }
    }

    fn result_for(id: &str, status: ControlStatus) -> ControlResult {
        ControlResult {
            request_id: id.to_owned(),
            namespace: "plant-a".to_owned(),
            device_id: "fanuc01".to_owned(),
            status,
            started_at_ns: Some(1_000),
            completed_at_ns: Some(2_000),
            result: None,
            error: None,
        }
    }

    #[test]
    fn in_memory_insert_duplicate_conflict() {
        let journal = InMemoryJournal::new();
        let k = key("cmd-1");
        let hash = "hash-a".to_owned();
        assert_eq!(
            journal.try_insert(&k, hash.clone(), 0, 100).unwrap(),
            JournalDecision::Inserted
        );
        assert_eq!(
            journal.try_insert(&k, hash.clone(), 0, 100).unwrap(),
            JournalDecision::Duplicate(journal.get(&k).unwrap())
        );
        assert_eq!(
            journal.try_insert(&k, "hash-b".to_owned(), 0, 100).unwrap(),
            JournalDecision::Conflict {
                existing: journal.get(&k).unwrap()
            }
        );
    }

    #[test]
    fn in_memory_expired_treated_as_new() {
        let journal = InMemoryJournal::new();
        let k = key("cmd-1");
        assert_eq!(
            journal.try_insert(&k, "hash-a".to_owned(), 0, 100).unwrap(),
            JournalDecision::Inserted
        );
        // 100 之后过期：此时 created_at_ns=200 > expires=100，视为新请求。
        assert_eq!(
            journal
                .try_insert(&k, "hash-b".to_owned(), 200, 300)
                .unwrap(),
            JournalDecision::Inserted
        );
    }

    #[test]
    fn in_memory_settle_and_get() {
        let journal = InMemoryJournal::new();
        let k = key("cmd-1");
        journal.try_insert(&k, "hash-a".to_owned(), 0, 100).unwrap();
        let result = result_for("cmd-1", ControlStatus::Succeeded);
        journal.settle(&k, &result).unwrap();
        let entry = journal.get(&k).unwrap();
        assert_eq!(entry.status, ControlStatus::Succeeded);
        assert_eq!(entry.result, Some(result));
    }

    #[test]
    fn file_journal_round_trip_and_recovery() {
        let dir =
            std::env::temp_dir().join(format!("forge-control-journal-{}-rt", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");

        // 第一次会话：插入 + 结算。
        {
            let journal = FileJournal::open(&path, 1_000).unwrap();
            let k = key("cmd-1");
            assert_eq!(
                journal
                    .try_insert(&k, "hash-a".to_owned(), 0, 10_000_000)
                    .unwrap(),
                JournalDecision::Inserted
            );
            journal
                .settle(&k, &result_for("cmd-1", ControlStatus::Succeeded))
                .unwrap();
            assert_eq!(
                journal.get(&k).unwrap().result.unwrap().status,
                ControlStatus::Succeeded
            );
        }

        // 第二次会话：恢复已结算记录。
        {
            let journal = FileJournal::open(&path, 2_000_000).unwrap();
            let k = key("cmd-1");
            let entry = journal.get(&k).unwrap();
            assert_eq!(entry.status, ControlStatus::Succeeded);
            assert_eq!(entry.result.unwrap().status, ControlStatus::Succeeded);
        }

        // 恢复后的引擎语义：同 key 同 payload → Duplicate（不重放）。
        {
            let journal = FileJournal::open(&path, 3_000_000).unwrap();
            let k = key("cmd-1");
            let decision = journal
                .try_insert(&k, "hash-a".to_owned(), 3_000_000, 4_000_000)
                .unwrap();
            match decision {
                JournalDecision::Duplicate(e) => assert_eq!(e.status, ControlStatus::Succeeded),
                other => panic!("应判定 Duplicate，实际 {other:?}"),
            }
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_journal_unsettled_recovers_as_indeterminate() {
        let dir = std::env::temp_dir().join(format!(
            "forge-control-journal-{}-unsettled",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");

        {
            let journal = FileJournal::open(&path, 1_000).unwrap();
            let k = key("cmd-1");
            journal
                .try_insert(&k, "hash-a".to_owned(), 0, 10_000_000)
                .unwrap();
            // 不结算，模拟进程在执行中被终止。
        }
        {
            let journal = FileJournal::open(&path, 2_000_000).unwrap();
            let entry = journal.get(&key("cmd-1")).unwrap();
            assert_eq!(entry.status, ControlStatus::Indeterminate);
            // P1-E：恢复时生成可见结果（`status()` 只返回 `entry.result`），
            // 调用方必须能查询到"不确定"，而不是 None。
            let result = entry.result.expect("恢复后应生成 Indeterminate 结果");
            assert_eq!(result.status, ControlStatus::Indeterminate);
            assert_eq!(result.error.unwrap().code, "EXECUTION_INTERRUPTED");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_journal_corrupt_line_fails_open() {
        let dir = std::env::temp_dir().join(format!(
            "forge-control-journal-{}-corrupt",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");

        {
            let journal = FileJournal::open(&path, 1_000).unwrap();
            let k = key("cmd-1");
            journal
                .try_insert(&k, "hash-a".to_owned(), 0, 10_000_000)
                .unwrap();
            journal
                .settle(&k, &result_for("cmd-1", ControlStatus::Succeeded))
                .unwrap();
        }
        // 追加一行损坏记录（截断的 JSON）——模拟磁盘/断电损坏。
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(file, "{{truncated").unwrap();
        }
        // P1-F：损坏行 fail-closed——静默跳过可能丢失已执行请求的 Insert，
        // 重试将重复下发控制动作。
        let err = FileJournal::open(&path, 2_000_000).unwrap_err();
        assert!(
            matches!(err, JournalError::Corrupt { line: 3 }),
            "损坏行应返回 Corrupt 错误，实际 {err:?}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_journal_expired_filtered_on_open() {
        let dir = std::env::temp_dir().join(format!(
            "forge-control-journal-{}-expired",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");

        {
            let journal = FileJournal::open(&path, 1_000).unwrap();
            let k = key("cmd-1");
            journal.try_insert(&k, "hash-a".to_owned(), 0, 500).unwrap(); // 早期过期
            let k2 = key("cmd-2");
            journal
                .try_insert(&k2, "hash-b".to_owned(), 0, 10_000_000)
                .unwrap(); // 长期
        }
        {
            // now=1_000_000：cmd-1 已过期，cmd-2 保留。
            let journal = FileJournal::open(&path, 1_000_000).unwrap();
            assert!(journal.get(&key("cmd-1")).is_none());
            assert!(journal.get(&key("cmd-2")).is_some());
            // purge 无残留。
            assert_eq!(journal.purge_expired(1_000_000), 0);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_journal_settle_after_expired_insert_not_corrupt() {
        // 三审回归：Insert 过期被跳过后，其 Settle 属正常生命周期（已结算
        // 请求自然老化），必须随压缩一并跳过，不得误判为孤立 Settle 而
        // Corrupt——否则重启后 Journal 永久无法打开。
        let dir = std::env::temp_dir().join(format!(
            "forge-control-journal-{}-expired-settle",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");

        {
            let journal = FileJournal::open(&path, 1_000).unwrap();
            let k = key("cmd-1");
            journal.try_insert(&k, "hash-a".to_owned(), 0, 500).unwrap(); // 早期过期
            journal
                .settle(&k, &result_for("cmd-1", ControlStatus::Succeeded))
                .unwrap();
        }
        {
            // now=1_000_000：Insert 已过期，Settle 不得触发 Corrupt。
            let journal = FileJournal::open(&path, 1_000_000)
                .expect("过期 Insert 的 Settle 不应判定为孤立记录");
            assert!(journal.get(&key("cmd-1")).is_none());
            // 压缩后文件不再含过期记录：重新打开仍成功。
            drop(journal);
            FileJournal::open(&path, 1_000_000).unwrap();
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn payload_hash_is_stable_and_distinguishes_payloads() {
        let a = ControlOperation::CommandExecute(CommandRequest {
            command: "drive.reset".to_owned(),
            parameters: vec![],
        });
        let b = ControlOperation::CommandExecute(CommandRequest {
            command: "drive.reset".to_owned(),
            parameters: vec![],
        });
        let c = ControlOperation::PropertyWrite(PropertyWriteRequest {
            items: vec![PropertyWriteItem {
                path: "drive.mode".to_owned(),
                value: Value::String("auto".to_owned()),
            }],
        });
        assert_eq!(payload_hash(&a), payload_hash(&b));
        assert_ne!(payload_hash(&a), payload_hash(&c));
    }
}
