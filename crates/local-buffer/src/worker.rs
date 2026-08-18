//! 专用阻塞 Worker：SQLite 访问与内存队列维护都在独立线程内完成，
//! 通过有界异步通道（§34.2 有界并发/背压）与调用方通信，**不阻塞
//! Tokio**（§103：磁盘操作放入专用阻塞 Worker）。

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use data_pipeline::ObservationBatch;
use rusqlite::{Connection, OptionalExtension};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::{
    StoredBatch,
    config::{CapacityPolicy, LocalBufferConfig},
    error::{CapacityKind, LocalBufferError},
};

/// 通道容量（有界，§34.2）。worker 消费很快（内存操作 + SQLite），
/// 该容量主要限制并发生产者堆积；容量背压由 push 命令内部的
/// 等待队列（[`WorkerState::pending_push`]）承担。
const CHANNEL_CAPACITY: usize = 1024;

/// 同一待落盘记录（背压等待中）允许的等待者上限（评审 P2-1）：
/// `PendingPush::replies` 有界，防止同 `message_id` 重试风暴导致
/// 内存无界增长；超限的新请求显式报 [`LocalBufferError::CapacityExceeded`]。
const MAX_WAITERS_PER_RECORD: usize = 1024;

/// 每条记录的磁盘成本中，SQLite 存储固定开销的保守估算（字节）：
/// B-tree 页、`message_id` UNIQUE 索引、行头与页填充等。容量统计用
/// `payload + topic + message_id + 本开销` 的估算值（§103 磁盘上限
/// 以估算成本计；WAL 文件与主库同源，不重复计入）。
const FIXED_OVERHEAD_PER_RECORD: usize = 512;

/// SQLite schema 版本（§103 Embedded DB；非法版本 = 损坏，明确报错）。
const SCHEMA_VERSION: i64 = 1;

/// 表结构：一条记录 = 一个完整 `ObservationBatch`（§31.4 WAL 持久化
/// 单位为完整 Batch，与 MQTT 发布单位一致）。
const CREATE_BATCHES: &str = r#"
CREATE TABLE IF NOT EXISTS batches (
    local_seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id     TEXT NOT NULL UNIQUE,
    topic          TEXT NOT NULL,
    payload        BLOB NOT NULL,
    created_at_ns  INTEGER NOT NULL,
    sent_count     INTEGER NOT NULL DEFAULT 0
)
"#;

/// 内存 / 磁盘同构的记录（恢复加载与运行期共用）。
#[derive(Debug, Clone)]
pub(crate) struct MemRecord {
    pub(crate) local_seq: i64,
    pub(crate) message_id: String,
    pub(crate) topic: String,
    /// 原始 Batch JSON（`replayed` 保持原值；发送时由 `next` 深拷贝
    /// 并按补传语义置 `replayed = true`，§31.4）。
    pub(crate) payload: Vec<u8>,
    pub(crate) created_at_ns: i64,
    pub(crate) sent_count: u64,
}

/// worker 通道命令（每种命令都带 oneshot 回复，调用方 `await` 等待
/// 结果——所有对 worker 的访问都是异步有界的）。
pub(crate) enum Cmd {
    Push {
        batch: ObservationBatch,
        reply: oneshot::Sender<Result<(), LocalBufferError>>,
    },
    Next {
        reply: oneshot::Sender<Result<Option<StoredBatch>, LocalBufferError>>,
        /// 交付确认（评审 P1-1）：调用方在提取结果后 `send(())`。
        /// worker 登记本通道，若在确认前发现通道已关闭（调用方
        /// future 在提取前被取消）则归还记录——`reply.send` 成功
        /// 不代表已交付，避免记录滞留 in-flight。
        delivered: oneshot::Receiver<()>,
    },
    Ack {
        local_seq: i64,
        reply: oneshot::Sender<Result<(), LocalBufferError>>,
    },
    Requeue {
        local_seq: i64,
        reply: oneshot::Sender<Result<(), LocalBufferError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), LocalBufferError>>,
    },
}

/// 背压策略下等待空间的 push 请求（§103：容量不足时显式背压，
/// 禁止静默覆盖）。
///
/// 同 `message_id` 的重复 push 会**共享最终落盘结果**（评审 P2-2）：
/// 记录尚未持久化时重复请求不得提前返回成功（崩溃/停机将丢失该
/// 记录），而是追加到 `replies`，由入队成功/失败时统一结算。
struct PendingPush {
    record: MemRecord,
    replies: Vec<oneshot::Sender<Result<(), LocalBufferError>>>,
}

/// push 内部结果。
enum PushOutcome {
    /// 请求已处理完毕（入队 / 幂等成功 / 错误已回复）。
    Handled,
    /// 容量不足且策略为背压：请求已进入等待队列，由后续 ACK 释放
    /// 空间后自动入队（reply 已随请求保存，勿再回复）。
    Backpressured,
}

/// worker 线程内状态（仅专用线程访问，无并发）。
struct WorkerState {
    mem: VecDeque<MemRecord>,
    inflight: HashMap<i64, MemRecord>,
    pending_push: VecDeque<PendingPush>,
    /// 已加载到内存的磁盘记录的本地序号水位（分页加载，P1-3）：
    /// 恢复 / 补充加载只取 `local_seq > last_loaded_seq` 的记录。
    last_loaded_seq: i64,
    /// 恢复水位（会话内补传标记，P2-2）：`open` 时磁盘上已存在的
    /// 最大本地序号。`local_seq <= 本水位` 的记录都是重启恢复的
    /// 积压，`next` 时一律 `replayed = true`——不修改数据库即可
    /// 标记补传（避免启动时对整表 UPDATE 产生大量 WAL 写入）。
    recover_watermark: i64,
    /// 最近一次分页补充加载（[`load_more`]）失败的错误（评审 P2-2）：
    /// 磁盘上仍有记录但查询失败时，`next` 在内存队列耗尽后返回该
    /// 错误而非 `None`——调用方不得误判 WAL 已清空。查询成功
    ///（窗口未满且执行了查询）后清除。
    load_error: Option<LocalBufferError>,
    /// 磁盘上未确认记录的估算字节总数（含 SQLite 固定开销，P2-4）。
    bytes: u64,
    /// 交付确认登记（评审 P1-1）：`next` 的结果已成功放入回复通道
    /// 但尚未收到调用方的交付确认。确认到达后移除；确认通道关闭
    ///（调用方在提取前取消）则归还记录，不滞留 in-flight。
    pending_confirm: Vec<PendingConfirm>,
}

/// `next` 交付确认条目（评审 P1-1）：调用方提取结果后经
/// `delivered` 通道确认；通道关闭且未确认 = 提取前取消 → 归还。
struct PendingConfirm {
    local_seq: i64,
    delivered: oneshot::Receiver<()>,
}

/// 单条记录的磁盘成本估算（P2-4）：`payload + topic + message_id +
/// FIXED_OVERHEAD_PER_RECORD`，用于容量上限统计。
fn record_cost(record: &MemRecord) -> u64 {
    (record.payload.len()
        + record.topic.len()
        + record.message_id.len()
        + FIXED_OVERHEAD_PER_RECORD) as u64
}

/// 派生出 §31.1 Telemetry 主题（与 mqtt-client `telemetry_topic` /
/// `validate_publish_topic` 同规则：`forgelink/v1/telemetry/{site_id}/{device_id}`，
/// 段内不得含 `/`，全主题不得含通配符、控制字符且 ≤ 65535 字节——
/// MQTT 必然拒绝的 Topic 不入队，避免成为无法发布的队头记录，
/// 评审 P1-2）。Local Buffer 存储该主题，`next` 时随 Batch 一并返回。
pub(crate) fn telemetry_topic(batch: &ObservationBatch) -> Result<String, LocalBufferError> {
    let topic = format!(
        "forgelink/v1/telemetry/{}/{}",
        batch.site_id, batch.device_id
    );
    for (field, segment) in [("site_id", &batch.site_id), ("device_id", &batch.device_id)] {
        if segment.is_empty()
            || segment.contains('/')
            || segment.contains('+')
            || segment.contains('#')
        {
            return Err(LocalBufferError::InvalidBatch {
                reason: format!("{field} 非法（不得为空或包含 /+\\#）: {segment:?}"),
            });
        }
        // 与 mqtt-client `validate_publish_topic` 一致（MQTT 3.1.1 §4.7.3）：
        // 控制字符（含 NUL，U+0000..U+001F 与 U+007F）会被 Broker 拒绝。
        if segment
            .chars()
            .any(|c| (c as u32) <= 0x1F || (c as u32) == 0x7F)
        {
            return Err(LocalBufferError::InvalidBatch {
                reason: format!("{field} 非法（包含控制字符，MQTT 必然拒绝）: {segment:?}"),
            });
        }
    }
    if topic.len() > 65535 {
        return Err(LocalBufferError::InvalidBatch {
            reason: format!("Topic 超过 65535 字节上限（MQTT 必然拒绝）: {topic:?}"),
        });
    }
    Ok(topic)
}

/// 打开数据库并校验 schema（§103：损坏或非法必须**明确报错**）。
/// 返回连接与恢复状态。
///
/// 恢复（P1-3 分页）：只加载前 `memory_records` 条到内存（内存窗口
/// 始终有界，防止 WAL 接近磁盘上限时启动 OOM），其余记录留在磁盘，
/// 由 [`load_more`] 随 `next` 消耗按本地序号顺序逐步补充。
///
/// 恢复的记录统一视为补传（P2-1/P2-2，§31.4）：从未发送过的记录
/// 也标记补传。用**会话内恢复水位**（磁盘上当前最大本地序号）实现，
/// 不修改数据库（避免启动时全表 UPDATE 的写入放大）。
fn open_db(
    config: &LocalBufferConfig,
) -> Result<(Connection, Vec<MemRecord>, i64, u64, i64), LocalBufferError> {
    let conn = Connection::open(&config.db_path).map_err(|e| {
        // 非 SQLite 文件（SQLITE_NOTADB）与打不开（路径/权限）分开描述。
        if matches!(e, rusqlite::Error::SqliteFailure(err, _) if err.code == rusqlite::ErrorCode::NotADatabase)
        {
            LocalBufferError::Corrupt {
                reason: format!("不是有效的 SQLite 数据库: {e}"),
            }
        } else {
            LocalBufferError::InvalidConfig {
                field: "db_path",
                reason: format!("无法打开数据库: {e}"),
            }
        }
    })?;

    // 崩溃恢复安全（§103）：WAL 日志模式 + 每次提交 fsync（§测试
    // 目标：强制重启恢复已持久化记录 0 丢失）。
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| LocalBufferError::Corrupt {
            reason: format!("设置 journal_mode=WAL 失败: {e}"),
        })?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.busy_timeout(Duration::from_secs(5))?;

    // schema 版本校验：0 = 首次创建；1 = 已知版本；其他 = 非法。
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    match version {
        0 => {
            // 评审 P2-2：user_version = 0 但 batches 表已存在（例如
            // user_version 被意外清零）→ 不得跳过校验在旧表上建表
            //（IF NOT EXISTS 会静默保留非法 schema）——一律 Corrupt。
            let has_table: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='batches')",
                [],
                |r| r.get(0),
            )?;
            if has_table {
                return Err(LocalBufferError::Corrupt {
                    reason: "user_version = 0 但 batches 表已存在（schema 版本被意外清零）".into(),
                });
            }
            conn.execute_batch(&format!(
                "BEGIN; {CREATE_BATCHES}; PRAGMA user_version = {SCHEMA_VERSION}; COMMIT;"
            ))?;
        }
        SCHEMA_VERSION => {
            // 完整 schema 校验（评审 P2-2/P2-3）：表缺失、缺列、列
            // 类型或约束（NOT NULL / 主键 / 默认值 / message_id 唯一
            // 索引 / AUTOINCREMENT）不符均为 Corrupt（约定），不混入
            // 普通 Db 错误——后者会触发无意义重试，前者意味着数据库
            // 本身不是本模块产物。
            let has_table: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='batches')",
                [],
                |r| r.get(0),
            )?;
            if !has_table {
                return Err(LocalBufferError::Corrupt {
                    reason: format!("schema 版本 {SCHEMA_VERSION} 但 batches 表缺失"),
                });
            }
            // PRAGMA table_info(batches)：cid, name, type, notnull,
            // dflt_value, pk。
            let mut stmt = conn.prepare("PRAGMA table_info(batches)")?;
            let mut actual: HashMap<String, (String, bool, Option<String>, bool)> = HashMap::new();
            for row in stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, bool>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            })? {
                let (name, ty, notnull, dflt, pk) = row?;
                actual.insert(name, (ty.to_ascii_uppercase(), notnull, dflt, pk != 0));
            }
            // 列：名称 + 类型 + NOT NULL + 默认值 + 主键。注意
            // local_seq 为 INTEGER PRIMARY KEY：隐含 NOT NULL，但
            // PRAGMA table_info 对主键列报告 notnull = 0。
            let expected_columns: [(&str, &str, bool, Option<&str>, bool); 6] = [
                ("local_seq", "INTEGER", false, None, true),
                ("message_id", "TEXT", true, None, false),
                ("topic", "TEXT", true, None, false),
                ("payload", "BLOB", true, None, false),
                ("created_at_ns", "INTEGER", true, None, false),
                ("sent_count", "INTEGER", true, Some("0"), false),
            ];
            // 评审 P2-4：列数必须恰好一致——额外列（如无默认值的
            // NOT NULL 列）会让所有 INSERT 失败，必须在启动时拒绝，
            // 不能只检查期望列存在。
            if actual.len() != expected_columns.len() {
                return Err(LocalBufferError::Corrupt {
                    reason: format!(
                        "batches 表列数不符（实际 {}，预期 {}）",
                        actual.len(),
                        expected_columns.len()
                    ),
                });
            }
            for (name, ty, notnull, dflt, is_pk) in expected_columns {
                match actual.get(name) {
                    Some((t, nn, d, pk)) => {
                        if t != ty {
                            return Err(LocalBufferError::Corrupt {
                                reason: format!("batches.{name} 列类型为 {t}（预期 {ty}）"),
                            });
                        }
                        if *nn != notnull {
                            return Err(LocalBufferError::Corrupt {
                                reason: format!("batches.{name} NOT NULL 约束不符"),
                            });
                        }
                        if d.as_deref() != dflt {
                            return Err(LocalBufferError::Corrupt {
                                reason: format!("batches.{name} 默认值不符"),
                            });
                        }
                        if *pk != is_pk {
                            return Err(LocalBufferError::Corrupt {
                                reason: format!("batches.{name} 主键约束不符"),
                            });
                        }
                    }
                    None => {
                        return Err(LocalBufferError::Corrupt {
                            reason: format!("batches 表缺列 {name}"),
                        });
                    }
                }
            }
            // message_id UNIQUE 唯一索引（origin = 'u'：由 UNIQUE 约束
            // 建立的自动索引；`unique` 是 SQL 关键字，需加引号）。
            // 评审 P2-2：必须确认唯一索引**恰好作用于 message_id**——
            // 仅检查"存在任意 UNIQUE 约束"会放过 UNIQUE(topic) 等
            // 无关索引（索引列取自 pragma_index_info，与索引列表
            // 同名关联）。
            let unique_ok: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 \
                 FROM pragma_index_list('batches') li \
                 JOIN pragma_index_info(li.name) ii ON ii.seqno >= 0 \
                 WHERE li.\"unique\" = 1 AND li.origin = 'u' \
                   AND ii.name = 'message_id' \
                   AND (SELECT COUNT(*) FROM pragma_index_info(li.name)) = 1)",
                [],
                |r| r.get(0),
            )?;
            if !unique_ok {
                return Err(LocalBufferError::Corrupt {
                    reason: "batches.message_id 唯一索引缺失（须恰好作用于 message_id 单列）"
                        .into(),
                });
            }
            // local_seq 为 INTEGER PRIMARY KEY AUTOINCREMENT：删除
            // 最大行后本地序号不复用（§31.2 会话内批次序号单调）。
            // 评审 P2-3：全库存在 sqlite_sequence 不代表本表使用了
            // AUTOINCREMENT（其他表也能创建该表）——直接检查本表的
            // DDL 是否含 AUTOINCREMENT 关键字（空表时 sqlite_sequence
            // 尚无本表行，不能查行）。
            let ddl: String = conn.query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'batches'",
                [],
                |r| r.get(0),
            )?;
            if !ddl.to_ascii_uppercase().contains("AUTOINCREMENT") {
                return Err(LocalBufferError::Corrupt {
                    reason: "batches.local_seq 未使用 AUTOINCREMENT".into(),
                });
            }
        }
        other => {
            return Err(LocalBufferError::Corrupt {
                reason: format!("未知 schema 版本 {other}（预期 {SCHEMA_VERSION}）"),
            });
        }
    }

    // 恢复水位 = 磁盘上当前最大本地序号（会话内补传标记，P2-2）：
    // 本次会话中 `local_seq <= 水位` 的记录都是重启恢复的积压，
    // `next` 时一律 `replayed = true`。不修改数据库。
    let recover_watermark: i64 =
        conn.query_row("SELECT COALESCE(MAX(local_seq), 0) FROM batches", [], |r| {
            r.get(0)
        })?;

    // 磁盘容量总量（估算成本，P2-4）：不逐条加载即统计。
    let bytes: i64 = conn.query_row(
        "SELECT COALESCE(SUM(length(payload) + length(topic) + length(message_id) + ?1), 0) \
         FROM batches",
        [FIXED_OVERHEAD_PER_RECORD as i64],
        |r| r.get(0),
    )?;

    // 恢复：分页加载前 `memory_records` 条（P1-3，内存窗口有界）；
    // 按本地递增序号升序 = §31.4 补传顺序。
    let mut stmt = conn.prepare(
        "SELECT local_seq, message_id, topic, payload, created_at_ns, sent_count \
         FROM batches ORDER BY local_seq LIMIT ?1",
    )?;
    let rows = stmt.query_map([config.memory_records as i64], |r| {
        Ok(MemRecord {
            local_seq: r.get(0)?,
            message_id: r.get(1)?,
            topic: r.get(2)?,
            payload: r.get(3)?,
            created_at_ns: r.get(4)?,
            sent_count: r.get::<_, i64>(5)? as u64,
        })
    })?;
    let mut records = Vec::new();
    for row in rows {
        records.push(row?);
    }
    drop(stmt);
    let last_loaded = records.last().map(|r| r.local_seq).unwrap_or(0);
    Ok((conn, records, last_loaded, bytes as u64, recover_watermark))
}

/// 分页补充加载（P1-3）：内存持有（`mem` + `inflight`）始终
/// ≤ `memory_records`。从磁盘按本地序号顺序取出**高于水位**的记录，
/// 随 `next` 消耗逐页补充——不会一次性把整库载入内存（OOM 防护）。
///
/// 窗口 = `mem + inflight`（评审 P1-1）：连续 `next` 而不 ACK 时，
/// 记录从 mem 转入 inflight，持有数不变，**不再补页**——避免在途
/// 积压期间把整个磁盘积压载入内存。仅 ACK 释放（持有数下降）后
/// 才继续补充。
///
/// 水位 = 已加载记录（mem 队列 + inflight）的最大本地序号：磁盘上
/// 的记录要么已入队（mem）、已取出（inflight），要么从未加载
///（`local_seq > 水位`）。用实际集合计算水位而非 `last_loaded_seq`
/// 水位，可避免把已取出（未 ACK，仍在磁盘）的记录重复加载回内存
/// 队列——那会破坏容量统计与"在途记录不得重复返回"（P1-3 修复）。
///
/// 返回 `Ok(true)` 表示查询已执行（窗口未满）；`Ok(false)` 表示窗口
/// 满、未执行查询（调用方据此决定是否清除 `load_error`，评审 P2-2）。
fn load_more(
    conn: &mut Connection,
    state: &mut WorkerState,
    config: &LocalBufferConfig,
) -> Result<bool, LocalBufferError> {
    let held = state.mem.len() + state.inflight.len();
    if held >= config.memory_records {
        return Ok(false);
    }
    let water_mark = state
        .mem
        .iter()
        .chain(state.inflight.values())
        .map(|r| r.local_seq)
        .max()
        .unwrap_or(state.last_loaded_seq);
    let need = (config.memory_records - held) as i64;
    let mut stmt = conn.prepare(
        "SELECT local_seq, message_id, topic, payload, created_at_ns, sent_count \
         FROM batches WHERE local_seq > ?1 ORDER BY local_seq LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![water_mark, need], |r| {
        Ok(MemRecord {
            local_seq: r.get(0)?,
            message_id: r.get(1)?,
            topic: r.get(2)?,
            payload: r.get(3)?,
            created_at_ns: r.get(4)?,
            sent_count: r.get::<_, i64>(5)? as u64,
        })
    })?;
    let mut loaded = Vec::new();
    for row in rows {
        loaded.push(row?);
    }
    drop(stmt);
    if let Some(last) = loaded.last() {
        state.last_loaded_seq = last.local_seq;
        state.mem.extend(loaded);
    }
    Ok(true)
}

/// `spawn` 的返回类型：命令发送端、线程句柄、就绪通知（open 结果）、
/// 停机状态标志（worker 退出前置位，`LocalBuffer` 用于把停机竞态
/// 窗口内的命令回复映射为 `Closed` 而非 `WorkerFailed`，评审 P1-1）。
pub(crate) type SpawnResult = (
    mpsc::Sender<Cmd>,
    JoinHandle<()>,
    oneshot::Receiver<Result<(), LocalBufferError>>,
    Arc<AtomicBool>,
);

/// 启动 worker 专用线程（§103：磁盘操作在专用阻塞 Worker，不阻塞
/// Tokio）。`open_db` 也在该线程内完成，通过 `ready` 通道返回结果
///（损坏 / 非法配置在 `LocalBuffer::open` 明确报错）。
pub(crate) fn spawn(config: LocalBufferConfig) -> Result<SpawnResult, LocalBufferError> {
    config.validate()?;
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (ready_tx, ready_rx) = oneshot::channel();
    let closed = Arc::new(AtomicBool::new(false));
    let closed_thread = Arc::clone(&closed);
    let handle = std::thread::Builder::new()
        .name("local-buffer-worker".into())
        .spawn(move || match open_db(&config) {
            Ok(opened) => {
                let _ = ready_tx.send(Ok(()));
                worker_loop(opened, config, rx, &closed_thread);
            }
            Err(e) => {
                let _ = ready_tx.send(Err(e));
            }
        })
        .map_err(|e| LocalBufferError::WorkerFailed {
            reason: format!("启动专用线程失败: {e}"),
        })?;
    Ok((tx, handle, ready_rx, closed))
}

/// 带超时的阻塞接收结果。
enum RecvOutcome {
    /// 超时（调用方重算下一次唤醒）。
    Timeout,
    /// 通道关闭（所有发送端已释放，等价异常退出）。
    Closed,
    /// 收到命令。
    Cmd(Cmd),
}

/// 带超时的阻塞接收（评审 P2-1：tokio mpsc 没有
/// `blocking_recv_timeout`）。用 `poll_recv` + `park_timeout` 在阻塞
/// 线程内手动驱动：唤醒后按剩余时间重新 park，直到消息到达或超时。
fn recv_timeout(rx: &mut mpsc::Receiver<Cmd>, timeout: Duration) -> RecvOutcome {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let deadline = Instant::now() + timeout;
    loop {
        match rx.poll_recv(&mut cx) {
            Poll::Ready(Some(cmd)) => return RecvOutcome::Cmd(cmd),
            Poll::Ready(None) => return RecvOutcome::Closed,
            Poll::Pending => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return RecvOutcome::Timeout; // 超时：调用方自行重算。
                }
                std::thread::park_timeout(remaining);
            }
        }
    }
}

/// 唤醒机制：unpark 被 park 的 worker 线程（与 [`recv_timeout`] 配对）。
struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// 交付确认检查（评审 P1-1）：调用方提取结果后 `send(())` 确认，
/// 已确认条目移除；确认通道关闭（调用方在提取前取消）则归还记录。
/// 返回是否有归还发生（progress 语义：归还后继续循环，让记录尽快
/// 回到发送队列）。
fn check_pending_confirm(conn: &mut Connection, state: &mut WorkerState) -> bool {
    let mut requeued = false;
    let mut i = 0;
    while i < state.pending_confirm.len() {
        match state.pending_confirm[i].delivered.try_recv() {
            Ok(()) => {
                state.pending_confirm.swap_remove(i); // 已确认交付。
            }
            Err(TryRecvError::Closed) => {
                let pc = state.pending_confirm.swap_remove(i);
                requeued = true;
                warn!(
                    component = "local-buffer",
                    error_code = "local_buffer_next_not_delivered",
                    local_seq = pc.local_seq,
                    "next 结果未交付（调用方在提取前取消），记录归还发送队列"
                );
                let _ = handle_requeue(conn, state, pc.local_seq);
            }
            Err(TryRecvError::Empty) => {
                i += 1; // 等待调用方提取结果。
            }
        }
    }
    requeued
}

/// worker 主循环（专用线程入口）。`closed` 在**任何退出路径**上于
/// 接收端销毁前置位（评审 P1-1）：调用方据此把停机竞态窗口内
/// 回复通道关闭的命令映射为 `Closed` 而非 `WorkerFailed`。
/// 启动数据以元组传入（open_db 结果），保持参数数量在 Clippy
/// 阈值内。
fn worker_loop(
    opened: (Connection, Vec<MemRecord>, i64, u64, i64),
    config: LocalBufferConfig,
    mut rx: mpsc::Receiver<Cmd>,
    closed: &AtomicBool,
) {
    let (conn, records, last_loaded_seq, bytes, recover_watermark) = opened;
    let mut state = WorkerState {
        mem: records.into(),
        inflight: HashMap::new(),
        pending_push: VecDeque::new(),
        last_loaded_seq,
        recover_watermark,
        load_error: None,
        bytes,
        pending_confirm: Vec::new(),
    };
    info!(
        component = "local-buffer",
        records = state.mem.len(),
        bytes,
        "Local Buffer 启动：已恢复未确认记录（分页加载，内存窗口 = memory_records）"
    );

    let mut conn = conn;
    loop {
        // 本轮处理是否有进展（清理 / 背压入队 / 交付归还）：有进展时
        // 继续循环处理（评审 P2-1）——恢复的过期数据超过多个分页
        // 窗口时，每轮清理一页又加载一页，若阻塞等待命令，已有背压
        // 请求会永久等待（继续清理即可释放容量）。无进展时才阻塞
        // 等命令。
        let mut progress = false;
        let mut cleanup_failed = false;

        // 交付确认（评审 P1-1）：调用方提取结果后 `send(())` 确认；
        // 确认通道关闭（提取前取消）则归还记录——`reply.send` 成功
        // 但未交付的 next 不滞留 in-flight。
        progress |= check_pending_confirm(&mut conn, &mut state);

        // 保留时间（§103）：队列中滞留超期的未确认记录显式丢弃
        //（告警；§31.4 在途记录不受影响，等待 ACK / requeue）。
        let cutoff = crate::now_ns() as i64 - config.retention.as_nanos() as i64;
        match cleanup_expired(&mut conn, &mut state, cutoff) {
            Ok(n) => progress |= n > 0,
            Err(e) => {
                // 记录失败状态：清理失败时用短退避重试（评审 P2-1），
                // 避免计算出的 0 超时空转烧 CPU。
                cleanup_failed = true;
                warn!(
                    component = "local-buffer",
                    error_code = "local_buffer_db_error",
                    "过期清理失败: {e}"
                );
            }
        }
        // 分页补充（P1-3）：`next` 消耗后按本地序号顺序从磁盘加载。
        // 查询失败时记录到 `load_error`（评审 P2-2）：`next` 在内存
        // 耗尽后返回该错误，调用方不得误判 WAL 已清空。
        match load_more(&mut conn, &mut state, &config) {
            Ok(true) => state.load_error = None,
            Ok(false) => {}
            Err(e) => {
                state.load_error = Some(e.clone());
                warn!(
                    component = "local-buffer",
                    error_code = "local_buffer_db_error",
                    "分页加载失败: {e}"
                );
            }
        }
        match flush_pending_push(&mut conn, &mut state, &config) {
            Ok(n) => progress |= n > 0,
            Err(e) => {
                // 磁盘错误：把错误分发给等待中的请求（显式失败，不静默
                // 丢弃），worker 继续运行以处理后续命令。
                warn!(
                    component = "local-buffer",
                    error_code = "local_buffer_db_error",
                    "背压等待队列入队失败: {e}"
                );
                for pending in state.pending_push.drain(..) {
                    for reply in pending.replies {
                        let _ = reply.send(Err(e.clone()));
                    }
                }
            }
        }
        if progress {
            continue; // 有进展：继续清理 / 入队（不阻塞等待命令）。
        }

        // 背压等待时按保留期限唤醒（评审 P2-1）：磁盘满且记录尚未
        // 过期时 worker 阻塞等待命令；若唯一生产者正等待 push()，
        // 没有 ack 命令到来、也没有清理动作，会永久停住。按发送
        // 队列中**最近的过期时刻**设置接收超时，到期自动醒来清理、
        // 释放容量后入队等待中的请求。无到期记录时用长兜底超时
        //（重算窗口，无实质影响）。
        let retention_ns = config.retention.as_nanos() as i64;
        let now = crate::now_ns() as i64;
        let timeout = if cleanup_failed {
            // 评审 P2-1：过期清理失败时记录仍在队列（已过期的记录
            // 会让超时计算为 0 → 空转烧 CPU；未过期的记录会让背压
            // 长期无法恢复）——用固定短退避重试，不空转、不休眠。
            Duration::from_millis(50)
        } else {
            state
                .mem
                .iter()
                .map(|r| r.created_at_ns.saturating_add(retention_ns))
                .min()
                .map(|t| {
                    // 评审 P2-1：`saturating_sub` 只防溢出（差值为负时
                    // 结果仍是负数），负数转 u64 会变成巨大时长导致
                    // 超长休眠——必须显式 `max(0)` 钳制为 0（立即唤醒
                    // 重试）。场景：记录在本轮清理完成后、计算超时前
                    // 刚好过期，或 SystemTime 回拨。
                    let diff = t.saturating_sub(now);
                    Duration::from_nanos(diff.max(0) as u64)
                })
                .unwrap_or(Duration::from_secs(3600))
        };

        match recv_timeout(&mut rx, timeout) {
            RecvOutcome::Timeout => continue, // 回到循环顶部（清理到期记录）。
            RecvOutcome::Closed => break,     // 句柄全部释放（等价异常退出）：
            // 未 ACK 记录已在 SQLite 中，重启后恢复。
            RecvOutcome::Cmd(cmd) => {
                // 处理命令前先做交付确认检查（评审 P1-1）：取消的 next
                // 先归还，随后的 next 才能取到记录（否则归还发生在
                // 本命令之后，新的 next 会误判队列为空）。
                check_pending_confirm(&mut conn, &mut state);
                match cmd {
                    Cmd::Push { batch, reply } => {
                        let outcome = push_inner(&mut conn, &mut state, &config, batch, reply);
                        if matches!(outcome, PushOutcome::Backpressured) {
                            // reply 已随请求保存，等待队列消化时回复。
                        }
                    }
                    Cmd::Next { reply, delivered } => {
                        let result = handle_next(&mut conn, &mut state);
                        let taken = result
                            .as_ref()
                            .ok()
                            .and_then(|o| o.as_ref().map(|s| s.local_seq));
                        if reply.send(result).is_err() {
                            // 调用方取消 / 超时（评审 P1-2）：回复通道已关闭
                            // ——记录已取出但调用方未取得 local_seq，归还
                            // 发送队列。
                            if let Some(local_seq) = taken {
                                warn!(
                                    component = "local-buffer",
                                    error_code = "local_buffer_next_cancelled",
                                    local_seq,
                                    "next 回复通道已关闭，记录归还发送队列"
                                );
                                let _ = handle_requeue(&mut conn, &mut state, local_seq);
                            }
                        } else if let Some(local_seq) = taken {
                            // 交付确认（评审 P1-1）：send 成功不等于已交付。
                            // 登记确认通道，调用方提取结果后 send；若在
                            // 提取前取消（通道关闭），主循环归还记录。
                            state.pending_confirm.push(PendingConfirm {
                                local_seq,
                                delivered,
                            });
                        }
                    }
                    Cmd::Ack { local_seq, reply } => {
                        let result = handle_ack(&mut conn, &mut state, local_seq);
                        if reply.send(result).is_err() {
                            warn!(
                                component = "local-buffer",
                                error_code = "local_buffer_reply_dropped",
                                "ack 回复通道已关闭"
                            );
                        }
                    }
                    Cmd::Requeue { local_seq, reply } => {
                        let result = handle_requeue(&mut conn, &mut state, local_seq);
                        if reply.send(result).is_err() {
                            warn!(
                                component = "local-buffer",
                                error_code = "local_buffer_reply_dropped",
                                "requeue 回复通道已关闭"
                            );
                        }
                    }
                    Cmd::Shutdown { reply } => {
                        // 有界优雅停机：先显式拒绝等待中的 push（未入队的
                        // 记录从未被接受，回复 Closed 由调用方决定重试），
                        // 再关闭数据库。已入队 / 在途的未确认记录保留在
                        // SQLite 中，重启后恢复。
                        for pending in state.pending_push.drain(..) {
                            for reply in pending.replies {
                                let _ = reply.send(Err(LocalBufferError::Closed));
                            }
                        }
                        // 停机完成后不得再接受命令（评审 P1-1）：`close()`
                        // 原子关闭接收端——此后任何 `send` 都立即失败
                        //（调用方收到 Closed）；已入队的命令逐一显式拒绝。
                        // 注意：tokio 允许**已取得 permit** 的发送者在
                        // close 后完成发送（`try_recv` 可能暂时 Empty），
                        // 这类消息最终随接收端销毁被丢弃——调用方等待
                        // 的回复通道关闭，由 `closed` 标志映射为 Closed
                        //（见 `LocalBuffer` 侧），而非 WorkerFailed。
                        rx.close();
                        while let Ok(cmd) = rx.try_recv() {
                            reject_cmd(cmd);
                        }
                        // 置位必须在回复 `Ok` 之前：停机返回成功后，任何
                        // 竞态窗口内丢失的命令一律看到 `closed` 标志。
                        closed.store(true, Ordering::Release);
                        if let Err((_conn, e)) = conn.close() {
                            warn!(
                                component = "local-buffer",
                                error_code = "local_buffer_close_failed",
                                "关闭 SQLite 连接时存在残留语句: {e}"
                            );
                        }
                        let _ = reply.send(Ok(()));
                        return;
                    }
                }
            }
        }
    }
    // 通道关闭（句柄全部释放，等价异常退出路径）：先置位 closed
    //（竞态窗口内命令的回复通道将关闭，映射为 Closed 而非
    // WorkerFailed），再关闭数据库连接，记录保留（已在 SQLite）。
    closed.store(true, Ordering::Release);
    let _ = conn.close();
}

/// 拒绝停机后仍留在通道中的命令（评审 P1-1）：一律回复
/// [`LocalBufferError::Closed`]（与停机约定一致）；重复 Shutdown 幂等。
fn reject_cmd(cmd: Cmd) {
    match cmd {
        Cmd::Push { reply, .. } | Cmd::Ack { reply, .. } | Cmd::Requeue { reply, .. } => {
            let _ = reply.send(Err(LocalBufferError::Closed));
        }
        Cmd::Next { reply, .. } => {
            let _ = reply.send(Err(LocalBufferError::Closed));
        }
        Cmd::Shutdown { reply } => {
            let _ = reply.send(Ok(()));
        }
    }
}

/// 处理背压等待队列：容量允许时逐个入队（磁盘 INSERT + 内存队列）。
/// 入队结果向该记录的全部等待请求（含重复 message_id 的共享请求，
/// P2-2）统一结算。返回入队条数（评审 P2-1：worker 循环据此判断
/// 是否继续处理）。
fn flush_pending_push(
    conn: &mut Connection,
    state: &mut WorkerState,
    config: &LocalBufferConfig,
) -> Result<usize, LocalBufferError> {
    let mut flushed = 0;
    while let Some(front) = state.pending_push.front() {
        let requested = record_cost(&front.record);
        if capacity_full(state, config, requested) {
            break; // 空间未释放，等待后续命令（如 ACK）腾出。
        }
        let pending = state.pending_push.pop_front().expect("front 已检查");
        match insert_record(conn, &pending.record) {
            Ok(()) => {
                // P1-1：取回 AUTOINCREMENT 分配的本地序号并写回记录，
                // 否则 local_seq = 0 无法被 next / ack 正确关联。
                let mut record = pending.record;
                record.local_seq = conn.last_insert_rowid();
                state.bytes = state.bytes.saturating_add(record_cost(&record));
                // 内存窗口有空位才进入发送队列（评审 P1-1，同 push）；
                // 分页失败（load_error）时同样不入内存（评审 P2-3），
                // 不得越过无法加载的旧记录破坏补传顺序。
                if state.load_error.is_none()
                    && state.mem.len() + state.inflight.len() < config.memory_records
                {
                    state.mem.push_back(record);
                }
                flushed += 1;
                for reply in pending.replies {
                    let _ = reply.send(Ok(()));
                }
            }
            Err(e) => {
                for reply in pending.replies {
                    let _ = reply.send(Err(e.clone()));
                }
            }
        }
    }
    Ok(flushed)
}

/// 容量检查：磁盘估算容量是**唯一硬上限**（评审 P1-1）。
///
/// 内存窗口（`memory_records`）不再是 push 的容量：内存满时记录仍
/// 直接落盘（磁盘 WAL 是第二级容量，Broker 断网期间可持续写入），
/// 由分页加载（[`load_more`]）在空间释放后按本地序号进入内存。
fn capacity_full(state: &WorkerState, config: &LocalBufferConfig, requested: u64) -> bool {
    state.bytes.saturating_add(requested) > config.disk_max_bytes
}

/// 过期清理（§103 保留时间）：仅清理**发送队列**（内存队列）中滞留
/// 超过保留时间的记录——显式丢弃并告警，不阻塞新数据（P1-2）。
///
/// **在途记录（已取出、等待 ACK / requeue）不清理**：§31.4 唯一删除
/// 路径是 `ack`（Broker PUBACK 后），保留时间不得删除未确认的已
/// 发送数据。内存队列按入队顺序（`local_seq` 递增 = `created_at_ns`
/// 单调）队头扫描，队头即最旧。
///
/// **事务提交成功后才更新内存状态**（评审 P2-3）：中途删除失败时
/// 内存队列与容量统计保持不变（记录仍可访问、容量不失真），错误
/// 向上传播由 worker 记录告警。
fn cleanup_expired(
    conn: &mut Connection,
    state: &mut WorkerState,
    cutoff_ns: i64,
) -> Result<usize, LocalBufferError> {
    // 评审 P2-5：不假设队头时间戳单调（SystemTime 回拨后，队列
    // 后部的记录可能更早过期）——`take_while` 会被未过期的队头
    // 阻断，过期记录无法及时释放容量；改为过滤全部记录。
    let expired: Vec<MemRecord> = state
        .mem
        .iter()
        .filter(|r| r.created_at_ns < cutoff_ns)
        .cloned()
        .collect();
    if expired.is_empty() {
        return Ok(0);
    }
    // 事务内删除磁盘（提交成功前不修改任何内存状态）。
    let tx = conn.transaction()?;
    for record in &expired {
        tx.execute(
            "DELETE FROM batches WHERE local_seq = ?1",
            [record.local_seq],
        )?;
    }
    tx.commit()?;
    // 提交成功后才更新内存状态：按本地序号精确移除（不假设队头
    // 连续弹出——过期记录可能散布在未过期记录之间）。
    let expired_ids: HashSet<i64> = expired.iter().map(|r| r.local_seq).collect();
    state.mem.retain(|r| !expired_ids.contains(&r.local_seq));
    let removed_bytes: u64 = expired.iter().map(record_cost).sum();
    state.bytes = state.bytes.saturating_sub(removed_bytes);
    warn!(
        component = "local-buffer",
        error_code = "local_buffer_expired_discard",
        deleted = expired.len(),
        "保留时间到期，发送队列中的记录被显式丢弃（§103 过期策略；在途记录不受影响）"
    );
    Ok(expired.len())
}

/// 推送一个 Batch（幂等：同 `message_id` 已存在时直接成功，不覆盖
/// 原记录——§31.3 消息级去重键；原记录可能已取出在途或等待发送，
/// 覆盖会破坏其本地序号与补传顺序）。
fn push_inner(
    conn: &mut Connection,
    state: &mut WorkerState,
    config: &LocalBufferConfig,
    batch: ObservationBatch,
    reply: oneshot::Sender<Result<(), LocalBufferError>>,
) -> PushOutcome {
    let topic = match telemetry_topic(&batch) {
        Ok(t) => t,
        Err(e) => {
            let _ = reply.send(Err(e));
            return PushOutcome::Handled;
        }
    };
    let payload = match serde_json::to_vec(&batch) {
        Ok(p) => p,
        Err(e) => {
            let _ = reply.send(Err(LocalBufferError::InvalidBatch {
                reason: format!("Batch 序列化失败: {e}"),
            }));
            return PushOutcome::Handled;
        }
    };
    let record = MemRecord {
        local_seq: 0, // INSERT 时由 AUTOINCREMENT 分配
        message_id: batch.message_id.clone(),
        topic,
        payload,
        created_at_ns: crate::now_ns() as i64,
        sent_count: 0,
    };

    // 幂等：同 message_id 已存在（在途 / 背压等待中 / 磁盘）→ 成功。
    // 在途与磁盘中的记录已持久化，可直接成功；
    // 背压等待中的记录**尚未落盘**（P2-2）：重复请求共享最终落盘
    // 结果（入队成功或失败统一结算），不得提前返回持久化成功——
    // 崩溃或停机将丢失该记录。
    if state
        .inflight
        .values()
        .any(|r| r.message_id == record.message_id)
    {
        let _ = reply.send(Ok(()));
        return PushOutcome::Handled;
    }
    if let Some(pending) = state
        .pending_push
        .iter_mut()
        .find(|p| p.record.message_id == record.message_id)
    {
        // 不立即回复：随首个请求一起等待最终落盘结果（flush 统一
        // 结算；停机时统一 Closed）。
        if pending.replies.len() >= MAX_WAITERS_PER_RECORD {
            // 同一待落盘记录的等待者数量有界（评审 P2-1）：防止同
            // message_id 重试风暴让 replies 无限增长。
            let _ = reply.send(Err(LocalBufferError::CapacityExceeded {
                kind: CapacityKind::Memory,
                limit: MAX_WAITERS_PER_RECORD as u64,
                current: pending.replies.len() as u64,
                requested: 1,
            }));
            return PushOutcome::Handled;
        }
        pending.replies.push(reply);
        return PushOutcome::Handled;
    }
    let exists: Option<i64> = match conn
        .query_row(
            "SELECT local_seq FROM batches WHERE message_id = ?1",
            [&record.message_id],
            |r| r.get(0),
        )
        .optional()
    {
        Ok(v) => v,
        Err(e) => {
            let _ = reply.send(Err(e.into()));
            return PushOutcome::Handled;
        }
    };
    if exists.is_some() {
        let _ = reply.send(Ok(()));
        return PushOutcome::Handled;
    }

    let requested = record_cost(&record);
    if requested > config.disk_max_bytes {
        // 单条记录成本已超过磁盘上限（评审 P1-1）：任何状态下
        // `bytes + requested > disk_max_bytes` 恒成立，背压等待将
        // 永久阻塞并拖住后续请求——任何策略都立即显式拒绝。
        let _ = reply.send(Err(LocalBufferError::CapacityExceeded {
            kind: CapacityKind::Disk,
            limit: config.disk_max_bytes,
            current: state.bytes,
            requested,
        }));
        return PushOutcome::Handled;
    }
    if capacity_full(state, config, requested) {
        let err = || LocalBufferError::CapacityExceeded {
            kind: CapacityKind::Disk,
            limit: config.disk_max_bytes,
            current: state.bytes,
            requested,
        };
        if state.pending_push.len() >= config.memory_records {
            // 背压等待队列有界（P2-3，与内存窗口同界）：防止满盘时
            // 请求无限累积导致内存无界增长——超出后即使策略为
            // Backpressure 也显式拒绝（不静默覆盖、不无限堆积）。
            let _ = reply.send(Err(err()));
            return PushOutcome::Handled;
        }
        match config.capacity_policy {
            CapacityPolicy::Reject => {
                let _ = reply.send(Err(err()));
                return PushOutcome::Handled;
            }
            CapacityPolicy::Backpressure => {
                // 显式背压：进入等待队列，由后续 ACK 释放空间后自动
                // 入队（§103：容量不足时显式背压，禁止静默覆盖）。
                state.pending_push.push_back(PendingPush {
                    record,
                    replies: vec![reply],
                });
                return PushOutcome::Backpressured;
            }
        }
    }

    // 入队 + 落盘（同一流程内完成，内存与磁盘一致；SQLite 事务保证
    // 崩溃后记录仍在——未 ACK 数据不丢失）。
    match insert_record(conn, &record) {
        Ok(()) => {
            // 取回本地递增序号（AUTOINCREMENT，§31.4 补传顺序依据）。
            let mut record = record;
            record.local_seq = conn.last_insert_rowid();
            state.bytes = state.bytes.saturating_add(record_cost(&record));
            // 内存窗口有空位才进入发送队列（评审 P1-1）：窗口满时
            // 记录仅落盘，由 load_more 在空间释放后按序加载——
            // 磁盘 WAL 是第二级容量，不受内存窗口限制。
            // 分页失败（load_error 未清除）时同样不入内存（评审 P2-3）：
            // 磁盘上仍有无法加载的旧记录，新记录先进内存会让 next
            // 越过旧记录，破坏 local_seq 补传顺序——此时 next 返回
            // load_error，待加载恢复后按序补入。
            if state.load_error.is_none()
                && state.mem.len() + state.inflight.len() < config.memory_records
            {
                state.mem.push_back(record);
            }
            let _ = reply.send(Ok(()));
            PushOutcome::Handled
        }
        Err(e) => {
            let _ = reply.send(Err(e));
            PushOutcome::Handled
        }
    }
}

fn insert_record(conn: &mut Connection, record: &MemRecord) -> Result<(), LocalBufferError> {
    conn.execute(
        "INSERT INTO batches (message_id, topic, payload, created_at_ns, sent_count) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            record.message_id,
            record.topic,
            record.payload,
            record.created_at_ns,
            record.sent_count as i64
        ],
    )?;
    Ok(())
}

/// 取最早未发送记录（内存队列头 = 本地序号最小 = §31.4 补传顺序）。
/// 返回的 Batch 为深拷贝：`sent_count > 0`（曾取出）或属于本次会话
/// 恢复的积压（`local_seq <= recover_watermark`，P2-2 会话内补传
/// 标记）时置 `replayed = true`，`message_id` / Observation ID / 时间
/// 均保留原值。
///
/// 失败时（反序列化 / 数据库更新出错）把记录**放回队首**（评审
/// P2-3）：不丢失内存队首，后续记录不得越过它发送；返回 Err 后由
/// 调用方决定停机或重试。
fn handle_next(
    conn: &mut Connection,
    state: &mut WorkerState,
) -> Result<Option<StoredBatch>, LocalBufferError> {
    let Some(record) = state.mem.pop_front() else {
        // 内存耗尽但磁盘分页加载失败：返回错误而非 `None`，避免
        // 调用方误判 WAL 已清空（评审 P2-2）。
        if let Some(e) = &state.load_error {
            return Err(e.clone());
        }
        return Ok(None);
    };
    let mut batch: ObservationBatch = match serde_json::from_slice(&record.payload) {
        Ok(b) => b,
        Err(e) => {
            let local_seq = record.local_seq;
            state.mem.push_front(record);
            return Err(LocalBufferError::Db {
                reason: format!("已持久化 Batch 反序列化失败（记录 local_seq={local_seq}）: {e}"),
            });
        }
    };
    if record.sent_count > 0 || record.local_seq <= state.recover_watermark {
        batch.replayed = true; // 补传（§31.4）：保留原 message_id/时间。
    }
    let sent_count = record.sent_count + 1;
    if let Err(e) = conn.execute(
        "UPDATE batches SET sent_count = ?1 WHERE local_seq = ?2",
        rusqlite::params![sent_count as i64, record.local_seq],
    ) {
        state.mem.push_front(record);
        return Err(e.into());
    }
    state.inflight.insert(
        record.local_seq,
        MemRecord {
            sent_count,
            ..record.clone()
        },
    );
    Ok(Some(StoredBatch {
        local_seq: record.local_seq,
        topic: record.topic,
        batch,
    }))
}

/// ACK（broker PUBACK 后调用）：删除对应记录——**唯一**删除路径
/// （§31.4：Broker ACK 后才能删除对应 WAL 记录；`Closed` /
/// `Disconnected` / `CollisionOverwritten` 均不得删除）。幂等：记录
/// 不存在（已删）时返回 `Ok`。
///
/// **先删磁盘，成功后再移除在途状态**（评审 P2-4）：SQLite 删除
/// 失败时在途状态保留，重试 ACK 仍能正确扣减容量（避免容量虚高）。
fn handle_ack(
    conn: &mut Connection,
    state: &mut WorkerState,
    local_seq: i64,
) -> Result<(), LocalBufferError> {
    let deleted = conn.execute("DELETE FROM batches WHERE local_seq = ?1", [local_seq])?;
    if deleted > 0
        && let Some(record) = state.inflight.remove(&local_seq)
    {
        // 容量统计用估算成本，ACK 释放也按同一口径扣减。
        state.bytes = state.bytes.saturating_sub(record_cost(&record));
    }
    Ok(())
}

/// 发送失败（未 ACK，连接中断等）时把在途记录放回发送队列
/// （保持补传顺序：失败者优先于未发送记录，多条失败记录之间按
/// 本地序号升序插入——连续 requeue 不逆转 §31.4 补传顺序，评审
/// P2-1）。记录已被 ACK 删除时 no-op。
fn handle_requeue(
    _conn: &mut Connection,
    state: &mut WorkerState,
    local_seq: i64,
) -> Result<(), LocalBufferError> {
    if let Some(record) = state.inflight.remove(&local_seq) {
        let pos = state
            .mem
            .iter()
            .position(|r| r.local_seq > record.local_seq);
        match pos {
            Some(i) => state.mem.insert(i, record),
            None => state.mem.push_back(record),
        }
    } else {
        warn!(
            component = "local-buffer",
            error_code = "local_buffer_requeue_missing",
            local_seq,
            "requeue 找不到在途记录（可能已被 ACK）"
        );
    }
    Ok(())
}
