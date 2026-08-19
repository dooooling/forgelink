//! Local Buffer 集成测试（§103）：写入 / 顺序读取 / ACK 删除 / 幂等、
//! 重启恢复、未 ACK 数据不丢失、容量与背压、损坏与非法配置、停机。

use std::{path::Path, sync::Arc, time::Duration};

use data_pipeline::ObservationBatch;
use local_buffer::{CapacityPolicy, LocalBuffer, LocalBufferConfig, LocalBufferError};

fn batch(message_id: &str, device: &str, seq: u64) -> ObservationBatch {
    ObservationBatch {
        schema: "forgelink.telemetry.v1".into(),
        message_id: message_id.into(),
        site_id: "plant-a".into(),
        device_id: device.into(),
        sequence: seq,
        sent_at_ns: 1_780_000_000_000_000_000 + seq,
        replayed: false,
        observations: Vec::new(),
    }
}

fn config(dir: &Path, mem: usize, disk: u64, policy: CapacityPolicy) -> LocalBufferConfig {
    LocalBufferConfig {
        db_path: dir.join("buffer.db"),
        memory_records: mem,
        disk_max_bytes: disk,
        retention: Duration::from_secs(3600),
        capacity_policy: policy,
    }
}

fn msg(s: &str) -> String {
    s.into()
}

/// 磁盘背压用的大 message_id：约 1KB。估算成本
/// `payload + topic + message_id + 固定开销(512)` 单条约 2.7KB
/// （下界 > 2.5KB），两条必然超过 4KB 磁盘上限（评审 P1-1 起
/// 背压按磁盘估算成本触发，内存窗口不再是容量）。
fn big_id(i: u64) -> String {
    format!("m-{i}-{}", "x".repeat(1000))
}

/// 1) 写入、顺序读取、ACK 删除、重复写入幂等。
#[tokio::test(flavor = "multi_thread")]
async fn push_next_ack_and_duplicate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");

    for (i, device) in ["cnc-01", "cnc-02", "cnc-03"].iter().enumerate() {
        buffer
            .push(batch(&msg(&format!("m-{i}")), device, i as u64))
            .await
            .expect("push 必须成功");
    }

    // 顺序读取：本地序号递增，message_id / topic 正确，首次取出非补传。
    let mut seqs = Vec::new();
    for i in 0..3 {
        let stored = buffer
            .next()
            .await
            .expect("next 必须成功")
            .expect("队列非空");
        seqs.push(stored.local_seq);
        assert_eq!(stored.batch.message_id, format!("m-{i}"));
        assert_eq!(
            stored.topic,
            format!("forgelink/v1/telemetry/plant-a/cnc-0{}", i + 1)
        );
        assert!(!stored.batch.replayed, "首次取出不得标记补传");
        assert_eq!(stored.batch.message_id, format!("m-{i}"));
    }
    assert_eq!(seqs, vec![1, 2, 3], "本地序号必须递增");
    assert!(buffer.next().await.expect("next").is_none(), "队列应已空");

    // 重复写入幂等：不覆盖原记录、不新增记录。
    buffer
        .push(batch("m-1", "cnc-02", 1))
        .await
        .expect("重复 push 必须成功");
    // 在途重复同样幂等。
    buffer
        .push(batch("m-2", "cnc-03", 2))
        .await
        .expect("在途重复 push 必须成功");

    // ACK 前两条：唯一删除路径。
    buffer.ack(seqs[0]).await.expect("ack 必须成功");
    buffer.ack(seqs[1]).await.expect("ack 必须成功");
    buffer.ack(seqs[1]).await.expect("重复 ack 必须幂等");

    // 在途（已取出未 ACK）的第三条不重复返回；重复写入未产生新记录。
    assert!(
        buffer.next().await.expect("next").is_none(),
        "在途记录不得重复返回"
    );

    // 未 ACK 的第三条保留在磁盘，重启后恢复（不丢失）。
    buffer.shutdown().await.expect("shutdown 必须成功");
    drop(buffer);
    tokio::time::sleep(Duration::from_millis(50)).await; // 等线程退出

    let buffer = LocalBuffer::open(config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");
    let stored = buffer
        .next()
        .await
        .expect("next")
        .expect("未确认记录必须保留");
    assert_eq!(stored.local_seq, seqs[2], "本地序号必须保持");
    assert!(stored.batch.replayed, "曾取出的记录重启后补传必须标记");
    buffer.ack(stored.local_seq).await.expect("ack");
    assert!(
        buffer.next().await.expect("next").is_none(),
        "不得出现重复记录"
    );
    buffer.shutdown().await.expect("shutdown");
}

/// 2) 进程重启后恢复：未 ACK 记录按原顺序补传，`replayed = true`，
/// message_id / Observation ID / 时间保留。
#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_unacked_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject);

    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    for i in 0..4 {
        buffer
            .push(batch(&msg(&format!("m-{i}")), "cnc-01", i))
            .await
            .expect("push");
    }
    // 取出两条未 ACK（第三条的 sent_count 仍为 0）。
    let first = buffer.next().await.expect("next").expect("有记录");
    let second = buffer.next().await.expect("next").expect("有记录");
    assert_eq!((first.local_seq, second.local_seq), (1, 2));
    buffer.shutdown().await.expect("shutdown");

    // 重新打开：4 条全部恢复（未 ACK 数据不丢失）。
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    let mut seen: Vec<(i64, String, bool, u64)> = Vec::new();
    for _ in 0..4 {
        let stored = buffer.next().await.expect("next").expect("全部恢复");
        seen.push((
            stored.local_seq,
            stored.batch.message_id.clone(),
            stored.batch.replayed,
            stored.batch.sent_at_ns,
        ));
        buffer.ack(stored.local_seq).await.expect("ack");
    }
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");

    // 顺序保持（1,2,3,4）；恢复的记录一律补传（P2-1）：曾取出
    // （1、2）与从未取出（3、4）重启后 `sent_count` 均提升为 1，
    // `replayed = true`。message_id / 时间保留。
    assert_eq!(seen[0].0, 1);
    assert_eq!(seen[0].1, "m-0");
    assert!(seen[0].2, "曾取出记录恢复后必须补传标记");
    assert_eq!(seen[1].0, 2);
    assert!(seen[1].2, "曾取出记录恢复后必须补传标记");
    assert_eq!(seen[2].0, 3);
    assert!(seen[2].2, "未取出记录恢复后首次发送同样必须补传标记");
    assert_eq!(seen[3].0, 4);
    assert!(seen[3].2, "未取出记录恢复后首次发送同样必须补传标记");
    for (i, (_, _, _, t)) in seen.iter().enumerate() {
        assert_eq!(*t, 1_780_000_000_000_000_000 + i as u64, "原时间必须保留");
    }
}

/// 3) 异常退出（不调用 shutdown 直接释放句柄）后未 ACK 数据不丢失。
#[tokio::test(flavor = "multi_thread")]
async fn crash_equivalent_drop_recovers_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject);

    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    for i in 0..3 {
        buffer
            .push(batch(&msg(&format!("m-{i}")), "cnc-01", i))
            .await
            .expect("push");
    }
    let stored = buffer.next().await.expect("next").expect("有记录");
    drop(buffer); // 模拟崩溃：不 shutdown，通道关闭 worker 退出
    tokio::time::sleep(Duration::from_millis(50)).await; // 等线程退出

    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    let mut recovered = 0;
    while let Some(s) = buffer.next().await.expect("next") {
        assert!(s.batch.replayed, "恢复记录一律补传标记（P2-1）");
        buffer.ack(s.local_seq).await.expect("ack");
        recovered += 1;
    }
    assert_eq!(recovered, 3, "全部记录必须恢复");
    assert_eq!(stored.batch.message_id, "m-0", "崩溃前取出的记录也必须恢复");
    buffer.shutdown().await.expect("shutdown");
}

/// 4) 发送失败（未 ACK）requeue 后放回队头：失败者优先重试。
#[tokio::test(flavor = "multi_thread")]
async fn requeue_retries_failed_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");

    for i in 0..3 {
        buffer
            .push(batch(&msg(&format!("m-{i}")), "cnc-01", i))
            .await
            .expect("push");
    }
    let s1 = buffer.next().await.expect("next").expect("s1");
    let s2 = buffer.next().await.expect("next").expect("s2");
    buffer.requeue(s2.local_seq).await.expect("requeue");
    buffer.ack(s1.local_seq).await.expect("ack s1");

    // 重试的 s2 必须优先于 m-2。
    let retry = buffer.next().await.expect("next").expect("重试记录");
    assert_eq!(retry.local_seq, s2.local_seq, "失败记录必须放回队头");
    assert!(retry.batch.replayed, "重试必须补传标记");
    let s3 = buffer.next().await.expect("next").expect("s3");
    assert_eq!(s3.local_seq, 3);
    buffer.ack(retry.local_seq).await.expect("ack");
    buffer.ack(s3.local_seq).await.expect("ack");
    buffer.shutdown().await.expect("shutdown");
}

/// 5) 磁盘是第二级容量（评审 P1-1）：内存窗口满不再拒绝——记录
/// 直接落盘、随分页加载按序进入内存；磁盘超限按策略显式拒绝
///（Reject），禁止静默覆盖。
#[tokio::test(flavor = "multi_thread")]
async fn capacity_reject_is_explicit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 2, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");

    // 内存窗口 2：第 3 条记录仍可写入（磁盘第二级容量，Broker 断网
    // 期间持续落盘），随 ACK 释放由分页加载补入内存。
    buffer.push(batch("m-0", "cnc-01", 0)).await.expect("push");
    buffer.push(batch("m-1", "cnc-01", 1)).await.expect("push");
    buffer
        .push(batch("m-2", "cnc-01", 2))
        .await
        .expect("内存窗口满不得拒绝（磁盘第二级容量）");

    // 顺序取出全部 3 条（第 3 条由 load_more 按本地序号补载）。
    for i in 0..3 {
        let stored = buffer.next().await.expect("next").expect("s{i + 1}");
        assert_eq!(stored.local_seq, i as i64 + 1, "顺序必须保持");
        assert_eq!(stored.batch.message_id, format!("m-{i}"));
        buffer.ack(stored.local_seq).await.expect("ack");
    }
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");

    // 磁盘上限（payload 很小：设置 1 字节上限必然超限）：Reject
    // 显式拒绝，禁止静默覆盖未确认数据。
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1, CapacityPolicy::Reject))
        .await
        .expect("open");
    let err = buffer
        .push(batch("m-3", "cnc-02", 3))
        .await
        .expect_err("磁盘容量满必须拒绝");
    assert!(
        matches!(
            &err,
            LocalBufferError::CapacityExceeded {
                kind: local_buffer::CapacityKind::Disk,
                ..
            }
        ),
        "必须为磁盘容量错误: {err:?}"
    );
    buffer.shutdown().await.expect("shutdown");
}

/// 6) 容量不足：Backpressure 等待空间释放后自动入队（磁盘背压）。
#[tokio::test(flavor = "multi_thread")]
async fn capacity_backpressure_waits_then_accepts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 4000, CapacityPolicy::Backpressure))
        .await
        .expect("open");

    // 单条大记录（估算成本 ≈2.7KB）可入队；第二条总成本超 4KB
    // 上限 → 显式背压等待（不静默覆盖、不拒绝）。
    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");

    // 第二个 push 必须显式等待（背压），不静默覆盖。
    let pending = buffer.push(batch(&big_id(1), "cnc-01", 1));
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        r = pending => panic!("背压中的 push 不得在无空间时完成: {r:?}"),
    }

    // ACK 释放空间后，等待中的 push 自动入队。
    let s1 = buffer.next().await.expect("next").expect("s1");
    buffer.ack(s1.local_seq).await.expect("ack");
    tokio::time::timeout(Duration::from_secs(2), async {
        let stored = buffer.next().await.expect("next").expect("背压请求已入队");
        assert_eq!(stored.batch.message_id, big_id(1));
        buffer.ack(stored.local_seq).await.expect("ack");
    })
    .await
    .expect("背压请求必须在空间释放后入队");
    buffer.shutdown().await.expect("shutdown");
}

/// 7) 保留时间到期：未确认记录显式丢弃，不阻塞新数据。
#[tokio::test(flavor = "multi_thread")]
async fn retention_expired_records_are_discarded() {
    let dir = tempfile::tempdir().expect("tempdir");
    // 余量要大于 worker 循环往返（CI 慢机器上可能数十毫秒）：到期
    // 时刻与清理轮次的间隔太小，新 push 的记录会在下一轮清理中被
    // 误删（m-1 入队后 if 清理轮次到达其到期时刻）。
    let cfg = LocalBufferConfig {
        retention: Duration::from_millis(300),
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    buffer.push(batch("m-0", "cnc-01", 0)).await.expect("push");

    // 超过保留时间后再 push：触发清理（到期未确认记录显式丢弃）。
    tokio::time::sleep(Duration::from_millis(500)).await;
    buffer
        .push(batch("m-1", "cnc-01", 1))
        .await
        .expect("push 不阻塞");

    let mut remaining = Vec::new();
    while let Some(s) = buffer.next().await.expect("next") {
        remaining.push(s.batch.message_id);
        buffer.ack(s.local_seq).await.expect("ack");
    }
    assert_eq!(remaining, vec!["m-1"], "过期记录必须被清理，新数据不受影响");

    // 磁盘同样已清理（重启后不复活）。
    buffer.shutdown().await.expect("shutdown");
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    assert!(
        buffer.next().await.expect("next").is_none(),
        "过期记录不得复活"
    );
    buffer.shutdown().await.expect("shutdown");
}

/// 8) 损坏数据库明确报错。
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_db_is_rejected_explicitly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("buffer.db");
    std::fs::write(&path, b"this is not a sqlite database at all").expect("写坏文件");
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    let err = LocalBuffer::open(cfg)
        .await
        .expect_err("损坏数据库必须报错");
    assert!(
        matches!(&err, LocalBufferError::Corrupt { .. }),
        "必须为 Corrupt: {err:?}"
    );
}

/// 9) 非法配置明确报错。
#[tokio::test(flavor = "multi_thread")]
async fn invalid_config_is_rejected_explicitly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = || config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject);

    for (field, cfg) in [
        (
            "memory_records",
            LocalBufferConfig {
                memory_records: 0,
                ..base()
            },
        ),
        (
            "disk_max_bytes",
            LocalBufferConfig {
                disk_max_bytes: 0,
                ..base()
            },
        ),
        (
            "retention",
            LocalBufferConfig {
                retention: Duration::ZERO,
                ..base()
            },
        ),
        (
            "retention",
            LocalBufferConfig {
                // 超过 i64 纳秒范围（≈292 年）：as_nanos 转 i64 截断
                //（评审 P2-2），拒绝。
                retention: Duration::from_secs(u64::MAX),
                ..base()
            },
        ),
        (
            "db_path",
            LocalBufferConfig {
                db_path: dir.path().join("no-such-dir").join("buffer.db"),
                ..base()
            },
        ),
    ] {
        let err = LocalBuffer::open(cfg).await.expect_err("非法配置必须报错");
        assert!(
            matches!(
                &err,
                LocalBufferError::InvalidConfig {
                    field: f,
                    ..
                } if *f == field
            ),
            "字段 {field} 必须报 InvalidConfig: {err:?}"
        );
    }
}

/// 10) 停机：等待中的背压请求显式拒绝，已入队 / 在途记录保留。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_rejects_backpressured_and_preserves_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path(), 100, 4000, CapacityPolicy::Backpressure);

    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");
    let _s1 = buffer.next().await.expect("next").expect("s1"); // 占满磁盘容量

    // 背压等待中的请求：停机时显式拒绝（Closed），不静默丢失。
    let pending = buffer.push(batch(&big_id(1), "cnc-01", 1));
    tokio::time::sleep(Duration::from_millis(100)).await;
    buffer.shutdown().await.expect("shutdown");
    let err = pending.await.expect_err("等待中的请求必须被拒绝");
    assert!(matches!(&err, LocalBufferError::Closed), "{err:?}");
    // 停机后所有命令返回 Closed。
    let err = buffer
        .push(batch("m-2", "cnc-01", 2))
        .await
        .expect_err("停机后必须 Closed");
    assert!(matches!(&err, LocalBufferError::Closed), "{err:?}");
    drop(buffer);

    // 已入队（含在途）记录保留，重启后恢复。
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    let mut recovered = 0;
    while let Some(s) = buffer.next().await.expect("next") {
        buffer.ack(s.local_seq).await.expect("ack");
        recovered += 1;
    }
    assert_eq!(recovered, 1, "已入队记录必须保留（未 ACK 不丢失）");
    buffer.shutdown().await.expect("shutdown");
}

/// 11) 并发写入：worker 串行化，本地序号单调，消息完整无覆盖。
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_push_assigns_monotonic_seq() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = std::sync::Arc::new(
        LocalBuffer::open(config(
            dir.path(),
            10000,
            1 << 30,
            CapacityPolicy::Backpressure,
        ))
        .await
        .expect("open"),
    );

    let mut producers = Vec::new();
    for p in 0..8u64 {
        let buffer = std::sync::Arc::clone(&buffer);
        producers.push(tokio::spawn(async move {
            for i in 0..20u64 {
                let id = format!("p{p}-{i}");
                buffer.push(batch(&id, "cnc-01", i)).await.expect("push");
            }
        }));
    }
    for p in producers {
        p.await.expect("producer");
    }

    let mut seqs = Vec::new();
    let mut ids = std::collections::HashSet::new();
    while let Some(s) = buffer.next().await.expect("next") {
        seqs.push(s.local_seq);
        ids.insert(s.batch.message_id);
        buffer.ack(s.local_seq).await.expect("ack");
    }
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "本地序号必须严格递增");
    assert_eq!(ids.len(), 160, "全部消息必须出现且无覆盖");
    buffer.shutdown().await.expect("shutdown");
}

/// 12) 分页恢复（P1-3）：磁盘记录多于内存窗口时只加载前
/// `memory_records` 条，随 `next` 消耗按本地序号顺序逐页补充——
/// 不一次性全量载入内存，全部记录仍完整恢复。
#[tokio::test(flavor = "multi_thread")]
async fn paginated_restart_recovers_all_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    // 写入阶段使用宽内存窗口（运行中容量按配置限制）。
    let write_cfg = config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject);
    let buffer = LocalBuffer::open(write_cfg.clone()).await.expect("open");
    for i in 0..5 {
        buffer
            .push(batch(&msg(&format!("m-{i}")), "cnc-01", i))
            .await
            .expect("push");
    }
    buffer.shutdown().await.expect("shutdown");

    // 重启时内存窗口只有 2：磁盘 5 条记录必须分页恢复，全部不丢失。
    let cfg = config(dir.path(), 2, 1 << 30, CapacityPolicy::Reject);
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    let mut seen = Vec::new();
    for _ in 0..5 {
        let stored = buffer.next().await.expect("next").expect("分页恢复全部");
        assert!(stored.batch.replayed, "恢复记录一律补传标记");
        seen.push((stored.local_seq, stored.batch.message_id.clone()));
        buffer.ack(stored.local_seq).await.expect("ack");
    }
    assert!(buffer.next().await.expect("next").is_none(), "不得重复");
    for (i, (seq, id)) in seen.iter().enumerate() {
        assert_eq!(*seq, i as i64 + 1, "本地序号必须保持递增");
        assert_eq!(*id, format!("m-{i}"), "顺序必须保持入队顺序");
    }
    buffer.shutdown().await.expect("shutdown");
}

/// 13) 背压重复共享（P2-2）：同 `message_id` 的重复 push 在记录
/// 尚未落盘（背压等待）时不得提前返回成功，必须与首个请求共享
/// 最终落盘结果（ACK 释放空间后一并成功）。
#[tokio::test(flavor = "multi_thread")]
async fn backpressure_duplicate_waits_and_shares_result() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 4000, CapacityPolicy::Backpressure))
        .await
        .expect("open");

    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");
    let s1 = buffer.next().await.expect("next").expect("s1"); // 占满磁盘容量

    // 首个请求与重复请求均进入背压等待（不得提前完成）。
    let mut first = Box::pin(buffer.push(batch(&big_id(1), "cnc-01", 1)));
    let mut dup = Box::pin(buffer.push(batch(&big_id(1), "cnc-01", 1)));
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        r = &mut first => panic!("未落盘前不得提前返回成功: {r:?}"),
        r = &mut dup => panic!("重复请求不得提前返回成功: {r:?}"),
    }

    // ACK 释放空间：两个请求共享同一落盘结果，一并成功。
    buffer.ack(s1.local_seq).await.expect("ack");
    let (r1, r2) = tokio::join!(first, dup);
    r1.expect("首个请求必须成功");
    r2.expect("重复请求必须共享成功结果");

    // 只产生一条记录（幂等不重复）；在途（已取出未 ACK）时的重复
    // push 直接成功且不产生新记录。
    let stored = buffer.next().await.expect("next").expect("仅一条");
    assert_eq!(stored.batch.message_id, big_id(1));
    buffer
        .push(batch(&big_id(1), "cnc-01", 1))
        .await
        .expect("在途重复直接成功");
    assert!(
        buffer.next().await.expect("next").is_none(),
        "在途记录不得重复返回，重复 push 不得产生新记录"
    );
    buffer.ack(stored.local_seq).await.expect("ack");
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");
}

/// 14) 背压等待队列有界（P2-3）：等待队列长度达到 `memory_records`
/// 后，即使策略为 Backpressure 也显式拒绝（不无限堆积内存）。
#[tokio::test(flavor = "multi_thread")]
async fn pending_push_queue_is_bounded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 2, 4000, CapacityPolicy::Backpressure))
        .await
        .expect("open");

    // 第一条占满磁盘容量（估算成本 ≈2.7KB）；等待队列上限 =
    // memory_records = 2。先让命令发出（poll 一次确认已进入背压
    // 等待，未提前完成）。
    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");
    let mut p1 = Box::pin(buffer.push(batch(&big_id(1), "cnc-01", 1)));
    let mut p2 = Box::pin(buffer.push(batch(&big_id(2), "cnc-01", 2)));
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        r = &mut p1 => panic!("p1 不得提前完成: {r:?}"),
        r = &mut p2 => panic!("p2 不得提前完成: {r:?}"),
    }

    // 等待队列已满（长度 = memory_records = 2）：显式拒绝，不得无限堆积。
    let err = buffer
        .push(batch(&big_id(3), "cnc-01", 3))
        .await
        .expect_err("等待队列满必须显式拒绝");
    assert!(
        matches!(
            &err,
            LocalBufferError::CapacityExceeded {
                kind: local_buffer::CapacityKind::Disk,
                ..
            }
        ),
        "必须为显式容量错误: {err:?}"
    );

    // 逐条取出并 ACK（3 条 = mem 1 条 + 等待队列 2 条），空间随 ACK
    // 逐步释放，等待中的请求随之全部入队。**入队顺序不确定**
    //（p1/p2 到达 worker 的顺序不定），先统一释放空间再 join。
    let mut ids = Vec::new();
    for _ in 0..3 {
        let stored = buffer.next().await.expect("next").expect("全部恢复");
        ids.push(stored.batch.message_id.clone());
        buffer.ack(stored.local_seq).await.expect("ack");
    }
    let (r1, r2) = tokio::join!(&mut p1, &mut p2);
    r1.expect("p1 必须成功");
    r2.expect("p2 必须成功");
    // 内存队列原有顺序保持；等待请求全部入队（两者先后顺序不定）。
    assert_eq!(
        &ids[..1],
        &["m-0-".to_string() + &"x".repeat(1000)],
        "内存队列顺序必须保持"
    );
    let mut tail = ids[1..].to_vec();
    tail.sort();
    let mut expected = vec![big_id(1), big_id(2)];
    expected.sort();
    assert_eq!(tail, expected, "等待请求必须全部入队");
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");
}

/// 15) 分页窗口包含在途（评审 P1-1）：连续 `next` 不 ACK 时，内存
/// 持有（mem + inflight）保持 ≤ `memory_records`，**不再补页**——
/// 在途积压不会把整个磁盘记录拉入内存。
#[tokio::test(flavor = "multi_thread")]
async fn pagination_window_includes_inflight() {
    let dir = tempfile::tempdir().expect("tempdir");
    let write_cfg = config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject);
    let buffer = LocalBuffer::open(write_cfg.clone()).await.expect("open");
    for i in 0..4 {
        buffer
            .push(batch(&msg(&format!("m-{i}")), "cnc-01", i))
            .await
            .expect("push");
    }
    buffer.shutdown().await.expect("shutdown");

    let cfg = config(dir.path(), 2, 1 << 30, CapacityPolicy::Reject);
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");
    // 恢复加载前 2 条；连续 next 两条不 ACK：持有 2（mem 0 + inflight 2），
    // 不得补页——第三条不得提前进入内存。
    let s1 = buffer.next().await.expect("next").expect("s1");
    let s2 = buffer.next().await.expect("next").expect("s2");
    assert_eq!((s1.local_seq, s2.local_seq), (1, 2));
    assert!(
        buffer.next().await.expect("next").is_none(),
        "窗口满（含在途）不得补页"
    );

    // ACK 一条释放窗口后继续按本地序号顺序补页。
    buffer.ack(s1.local_seq).await.expect("ack");
    let s3 = buffer.next().await.expect("next").expect("释放后补页");
    assert_eq!(s3.local_seq, 3, "补页必须按本地序号顺序");
    assert!(s3.batch.replayed, "恢复积压一律补传标记");
    buffer.ack(s2.local_seq).await.expect("ack");
    let s4 = buffer.next().await.expect("next").expect("s4");
    assert_eq!(s4.local_seq, 4);
    buffer.ack(s3.local_seq).await.expect("ack");
    buffer.ack(s4.local_seq).await.expect("ack");
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");
}

/// 16) 同一待落盘记录的等待者数量有界（评审 P2-1）：同 `message_id`
/// 重试风暴下等待者的 replies 不无限增长，超限的新请求显式报错，
/// 已等待的请求在空间释放后全部成功（共享同一落盘结果）。
#[tokio::test(flavor = "multi_thread")]
async fn waiters_per_record_is_bounded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = std::sync::Arc::new(
        LocalBuffer::open(config(dir.path(), 100, 4000, CapacityPolicy::Backpressure))
            .await
            .expect("open"),
    );
    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");
    let s1 = buffer.next().await.expect("next").expect("s1"); // 占满磁盘容量

    // 重试风暴：大量重复请求并发提交（共享同一最终落盘结果，不提前完成）。
    let mut waiters = Vec::new();
    for _ in 0..1500 {
        let b = std::sync::Arc::clone(&buffer);
        waiters.push(tokio::spawn(async move {
            b.push(batch(&big_id(1), "cnc-01", 1)).await
        }));
    }
    // 等全部重复命令被 worker 处理（replies 达到上限）。
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = buffer
        .push(batch(&big_id(1), "cnc-01", 1))
        .await
        .expect_err("等待者超限必须显式拒绝");
    assert!(
        matches!(
            &err,
            LocalBufferError::CapacityExceeded {
                kind: local_buffer::CapacityKind::Memory,
                ..
            }
        ),
        "必须为显式容量错误: {err:?}"
    );

    // ACK 释放空间：已等待的请求共享同一落盘结果一并成功；风暴
    // 中超出上限的请求被显式拒绝（有界语义，评审 P2-1）。
    buffer.ack(s1.local_seq).await.expect("ack");
    for w in waiters {
        let r = w.await.expect("等待者任务");
        match r {
            Ok(()) => {}
            Err(LocalBufferError::CapacityExceeded { .. }) => {}
            Err(e) => panic!("等待者出现意外错误: {e:?}"),
        }
    }
    buffer.shutdown().await.expect("shutdown");
}

/// 16.5) 取消的 push 不累积背压等待者（评审 P2）：调用方 `timeout`
/// 取消 push 后 reply 通道关闭，但等待者曾登记在 `pending_push`——
/// 同 `message_id` 限时重试（如发送循环的容量等待）若不断累积等待
/// 者，达到上限后会被误判永久错误触发停机。每次重试前必须清理
/// 已取消的等待者。
#[tokio::test(flavor = "multi_thread")]
async fn push_timeout_cancel_does_not_accumulate_waiters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = Arc::new(
        LocalBuffer::open(config(dir.path(), 100, 4000, CapacityPolicy::Backpressure))
            .await
            .expect("open"),
    );
    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");
    let s1 = buffer.next().await.expect("next").expect("s1"); // 占满磁盘容量

    // 模拟发送循环的限时重试（每次超时取消后同 message_id 再试）：
    // 次数远超等待者上限（1024），取消不得累积等待者。
    for _ in 0..1500 {
        let b = Arc::clone(&buffer);
        let h = tokio::spawn(async move { b.push(batch(&big_id(1), "cnc-01", 1)).await });
        // 给 worker 处理入队（登记等待者）的时间，然后取消（reply
        // 通道关闭）。
        tokio::time::sleep(Duration::from_millis(1)).await;
        h.abort();
    }

    // 最后一次请求应正常进入背压等待，而非误判等待者上限。
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        buffer.push(batch(&big_id(1), "cnc-01", 1)),
    )
    .await;
    if let Ok(r) = result {
        r.expect("等待中不应报错");
    }

    // ACK 释放空间：等待者自动落盘成功。
    buffer.ack(s1.local_seq).await.expect("ack");
    tokio::time::timeout(
        Duration::from_secs(2),
        buffer.push(batch(&big_id(1), "cnc-01", 1)),
    )
    .await
    .expect("释放容量后应落盘")
    .expect("落盘成功");
    buffer.shutdown().await.expect("shutdown");
}

/// 17) 单条记录超过磁盘上限：即使 `Backpressure` 策略也立即拒绝
///（评审 P1-1）——任何状态下 `bytes + requested > disk_max_bytes`
/// 恒成立，背压等待将永久阻塞并拖住后续请求。
#[tokio::test(flavor = "multi_thread")]
async fn oversized_record_rejected_even_with_backpressure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1024, CapacityPolicy::Backpressure))
        .await
        .expect("open");

    // 4KB message_id → payload + topic + message_id + 固定开销远超 1024。
    let big_id = "x".repeat(4096);
    let err = buffer
        .push(batch(&big_id, "cnc-01", 0))
        .await
        .expect_err("单条超限必须立即拒绝（不得进入背压等待）");
    assert!(
        matches!(
            &err,
            LocalBufferError::CapacityExceeded {
                kind: local_buffer::CapacityKind::Disk,
                ..
            }
        ),
        "必须为磁盘容量错误: {err:?}"
    );
    assert!(
        buffer.next().await.expect("next").is_none(),
        "被拒绝的记录不得入队"
    );

    // 后续正常请求不受阻塞。
    buffer
        .push(batch("m-0", "cnc-01", 0))
        .await
        .expect("正常请求不受影响");
    let stored = buffer.next().await.expect("next").expect("m-0");
    buffer.ack(stored.local_seq).await.expect("ack");
    buffer.shutdown().await.expect("shutdown");
}

/// 18) Topic 校验与 mqtt-client 一致（评审 P1-2）：控制字符（含 NUL）
/// 与超过 65535 字节的主题会在入队前被拒绝——不会持久化 MQTT
/// 必然拒绝的记录。
#[tokio::test(flavor = "multi_thread")]
async fn topic_with_control_char_or_oversized_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");

    // 控制字符（site_id 含 NUL）：MQTT 3.1.1 必拒。
    let mut bad = batch("m-0", "cnc-01", 0);
    bad.site_id = "plant\u{0000}-a".into();
    let err = buffer.push(bad).await.expect_err("控制字符必须拒绝");
    assert!(
        matches!(&err, LocalBufferError::InvalidBatch { .. }),
        "必须为 InvalidBatch: {err:?}"
    );

    // 超过 65535 字节：device_id 超长。
    let long = "d".repeat(70000);
    let err = buffer
        .push(batch("m-1", &long, 1))
        .await
        .expect_err("超长主题必须拒绝");
    assert!(
        matches!(&err, LocalBufferError::InvalidBatch { .. }),
        "必须为 InvalidBatch: {err:?}"
    );
    assert!(
        buffer.next().await.expect("next").is_none(),
        "被拒绝的记录不得入队"
    );
    buffer.shutdown().await.expect("shutdown");
}

/// 20) next 交付确认（评审 P1-1）：结果已放入回复通道后、调用方
/// 提取前取消 future → 记录归还发送队列（不滞留 in-flight）。
/// 手动 poll 一次（命令发出、worker 未处理完 → Pending），等 worker
/// 完成回复（send 成功、登记交付确认）后 drop future——确认通道
/// 关闭，worker 在主循环归还记录。
#[tokio::test(flavor = "multi_thread")]
async fn next_cancel_before_delivery_requeues_record() {
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: std::sync::Arc<Self>) {}
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");
    buffer.push(batch("m-0", "cnc-01", 0)).await.expect("push");

    let mut fut = Box::pin(buffer.next());
    let waker = Waker::from(std::sync::Arc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    assert!(
        matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
        "首次 poll 应为 Pending（命令已发出，worker 尚未处理完）"
    );
    // worker 已把结果放入回复通道（send 成功、登记交付确认）。
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(fut); // 提取前取消 → 确认通道关闭 → 归还。

    // 记录必须归还，可再次取出。
    let stored = buffer.next().await.expect("next").expect("归还后仍可取");
    assert_eq!(stored.local_seq, 1);
    assert_eq!(stored.batch.message_id, "m-0");
    buffer.ack(stored.local_seq).await.expect("ack");
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");
}

/// 21) 背压等待按保留期限唤醒（评审 P2-1）：磁盘满、记录到期且无
/// 任何命令时，worker 不永久阻塞——到期自动清理并释放容量，等待
/// 中的 push 自动入队（唯一生产者不会被永久停住）。
#[tokio::test(flavor = "multi_thread")]
async fn backpressure_wakes_on_retention_expiry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = LocalBufferConfig {
        // 稳定时序（评审方案）：retention = 1s，两次 push 间隔 400ms
        // ——m-1 入队时距 m-0 到期仍有约 500ms（含慢 CI 调度余量），
        // m-1 自身到期（入队后 1s）远晚于 m-0 清理，不会被误删。
        retention: Duration::from_secs(1),
        ..config(dir.path(), 100, 4000, CapacityPolicy::Backpressure)
    };
    let buffer = LocalBuffer::open(cfg.clone()).await.expect("open");

    // 大记录占满磁盘（≈2.7KB）；稳定时序（评审方案）：
    //   retention = 1s，两次 push 间隔 = 400ms。
    // - m-1 命令送达时（m-0 push 后 400ms）m-0 最早约 600ms 后才
    //   到期（created_at ≥ m-0 push 发起时刻，实际更晚），磁盘必然
    //   仍满 → m-1 必然进入背压等待；
    // - 阶段 1 验证 m-1 至少阻塞 100ms（阻塞结束时尚距 m-0 到期约
    //   500ms，pending 不可能因到期而完成）——pending 在 100ms 内
    //   完成只可能是直接成功（磁盘已被清理），显式失败而非静默；
    // - 阶段 2 起无任何命令，worker 只能靠保留期限（1s）超时唤醒
    //   清理 m-0 释放容量，pending 自动入队成功。
    buffer
        .push(batch(&big_id(0), "cnc-01", 0))
        .await
        .expect("push");
    tokio::time::sleep(Duration::from_millis(400)).await;
    // pin! + 分支内 &mut 借用：阶段 1 走 sleep 分支（超时观察）时不
    // 会 drop future，阶段 2 可继续 poll 同一 pending（tokio mpsc
    // send 支持取消后重 poll，不丢失已送达的命令/背压状态）。
    let mut pending = Box::pin(buffer.push(batch(&big_id(1), "cnc-01", 1)));

    // 阶段 1：必须真实进入背压——至少阻塞 100ms。biased + pending
    // 优先：pending 完成（直接成功）时必 panic，两分支同时就绪时
    // 也不随机选择；磁盘仍满时 pending 不可能完成，sleep 分支必然
    // 就绪，无任何时序歧义。
    tokio::select! {
        biased;
        r = &mut pending => panic!("m-1 必须进入背压等待，不得直接成功: {r:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }

    // 阶段 2：m-0 到期（1s）后 worker 超时醒来清理、释放磁盘容量，
    // 等待中的 push 自动入队成功（3s 上限含慢 CI 调度余量）。
    tokio::time::timeout(Duration::from_secs(3), &mut pending)
        .await
        .expect("到期后必须自动入队（不得永久阻塞）")
        .expect("push 必须成功");

    // m-0 已过期丢弃，只剩 m-1。
    let stored = buffer.next().await.expect("next").expect("m-1");
    assert_eq!(stored.batch.message_id, big_id(1));
    buffer.ack(stored.local_seq).await.expect("ack");
    assert!(
        buffer.next().await.expect("next").is_none(),
        "m-0 已过期丢弃，不得复活"
    );
    buffer.shutdown().await.expect("shutdown");
}

/// 22) schema 约束完整校验（评审 P2-2）：user_version=0 但表已存在、
/// 缺列、约束（NOT NULL / 主键 / 默认值 / 唯一索引 / AUTOINCREMENT）
/// 不符的数据库一律 Corrupt，不混入普通 Db 错误。
#[tokio::test(flavor = "multi_thread")]
async fn corrupt_schema_is_rejected_explicitly() {
    let dir = tempfile::tempdir().expect("tempdir");

    // 构造坏库：user_version = 0 但 batches 表已存在。
    let path = dir.path().join("zero-version.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE batches (local_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             message_id TEXT NOT NULL UNIQUE, topic TEXT NOT NULL, payload BLOB NOT NULL, \
             created_at_ns INTEGER NOT NULL, sent_count INTEGER NOT NULL DEFAULT 0)",
        )
        .expect("建表");
    }
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    assert!(
        matches!(
            LocalBuffer::open(cfg)
                .await
                .expect_err("user_version=0 且表已存在必须 Corrupt"),
            LocalBufferError::Corrupt { .. }
        ),
        "必须为 Corrupt"
    );

    // 构造坏库：schema v1 但缺 UNIQUE 索引（message_id 无唯一约束）。
    let path = dir.path().join("no-unique.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE batches (local_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             message_id TEXT NOT NULL, topic TEXT NOT NULL, payload BLOB NOT NULL, \
             created_at_ns INTEGER NOT NULL, sent_count INTEGER NOT NULL DEFAULT 0); \
             PRAGMA user_version = 1",
        )
        .expect("建表");
    }
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    assert!(
        matches!(
            LocalBuffer::open(cfg)
                .await
                .expect_err("缺 message_id 唯一索引必须 Corrupt"),
            LocalBufferError::Corrupt { .. }
        ),
        "必须为 Corrupt"
    );

    // 构造坏库：schema v1 但 sent_count 无 NOT NULL / 默认值。
    let path = dir.path().join("no-constraint.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE batches (local_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             message_id TEXT NOT NULL UNIQUE, topic TEXT NOT NULL, payload BLOB NOT NULL, \
             created_at_ns INTEGER NOT NULL, sent_count INTEGER); \
             PRAGMA user_version = 1",
        )
        .expect("建表");
    }
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    assert!(
        matches!(
            LocalBuffer::open(cfg)
                .await
                .expect_err("约束不符必须 Corrupt"),
            LocalBufferError::Corrupt { .. }
        ),
        "必须为 Corrupt"
    );

    // 构造坏库：schema v1 但唯一索引作用于 topic 而非 message_id
    //（评审 P2-2）：存在任意 UNIQUE 约束不得通过。
    let path = dir.path().join("unique-topic.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE batches (local_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             message_id TEXT NOT NULL, topic TEXT NOT NULL UNIQUE, payload BLOB NOT NULL, \
             created_at_ns INTEGER NOT NULL, sent_count INTEGER NOT NULL DEFAULT 0); \
             PRAGMA user_version = 1",
        )
        .expect("建表");
    }
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    assert!(
        matches!(
            LocalBuffer::open(cfg)
                .await
                .expect_err("UNIQUE(topic) 而非 message_id 必须 Corrupt"),
            LocalBufferError::Corrupt { .. }
        ),
        "必须为 Corrupt"
    );

    // 构造坏库：schema v1 但 batches 未用 AUTOINCREMENT，而其他表
    // 使用了（评审 P2-3）：全库存在 sqlite_sequence 不得通过。
    let path = dir.path().join("no-autoinc.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE batches (local_seq INTEGER PRIMARY KEY, \
             message_id TEXT NOT NULL UNIQUE, topic TEXT NOT NULL, payload BLOB NOT NULL, \
             created_at_ns INTEGER NOT NULL, sent_count INTEGER NOT NULL DEFAULT 0); \
             CREATE TABLE other (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT); \
             PRAGMA user_version = 1",
        )
        .expect("建表");
    }
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    assert!(
        matches!(
            LocalBuffer::open(cfg)
                .await
                .expect_err("batches 无 AUTOINCREMENT 必须 Corrupt"),
            LocalBufferError::Corrupt { .. }
        ),
        "必须为 Corrupt"
    );

    // 构造坏库：schema v1 但存在额外列（无默认值 NOT NULL，评审
    // P2-4）：后续所有 INSERT 都会失败，启动时必须拒绝。
    let path = dir.path().join("extra-column.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "CREATE TABLE batches (local_seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             message_id TEXT NOT NULL UNIQUE, topic TEXT NOT NULL, payload BLOB NOT NULL, \
             created_at_ns INTEGER NOT NULL, sent_count INTEGER NOT NULL DEFAULT 0, \
             extra TEXT NOT NULL); \
             PRAGMA user_version = 1",
        )
        .expect("建表");
    }
    let cfg = LocalBufferConfig {
        db_path: path,
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    assert!(
        matches!(
            LocalBuffer::open(cfg)
                .await
                .expect_err("额外列必须 Corrupt"),
            LocalBufferError::Corrupt { .. }
        ),
        "必须为 Corrupt"
    );
}

/// 19) 连续 requeue 多条不逆转补传顺序（评审 P2-1）：失败记录仍
/// 优先于未发送记录，但多条失败记录之间保持本地序号升序。
#[tokio::test(flavor = "multi_thread")]
async fn requeue_multiple_keeps_local_seq_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = LocalBuffer::open(config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject))
        .await
        .expect("open");
    for i in 0..3 {
        buffer
            .push(batch(&msg(&format!("m-{i}")), "cnc-01", i))
            .await
            .expect("push");
    }
    let s1 = buffer.next().await.expect("next").expect("s1"); // seq 1
    let s2 = buffer.next().await.expect("next").expect("s2"); // seq 2
    // 逆序 requeue：队列当前 [3]，插入后必须仍为 [1,2,3]。
    buffer.requeue(s2.local_seq).await.expect("requeue s2");
    buffer.requeue(s1.local_seq).await.expect("requeue s1");

    let mut seqs = Vec::new();
    for _ in 0..3 {
        let stored = buffer.next().await.expect("next").expect("记录");
        seqs.push(stored.local_seq);
        buffer.ack(stored.local_seq).await.expect("ack");
    }
    assert_eq!(seqs, vec![1, 2, 3], "requeue 不得逆转补传顺序");
    assert!(buffer.next().await.expect("next").is_none());
    buffer.shutdown().await.expect("shutdown");
}

/// 23) 停机期间并发命令（评审 P1-1）：`shutdown` 返回成功后不得再
/// 接受命令——竞态窗口内（接收端销毁前）已入队的命令必须被显式
/// 拒绝，任何命令都不得以 WorkerFailed 告终（命令入队后无人处理）。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_racing_commands_never_worker_failed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buffer = Arc::new(
        LocalBuffer::open(config(dir.path(), 4, 1 << 30, CapacityPolicy::Backpressure))
            .await
            .expect("open"),
    );
    // 大量并发 push 与 shutdown 竞争：部分在 shutdown 前到达（成功
    // 或被拒绝）、部分在 shutdown 后到达（Closed）。任务数超过通道
    // 容量（1024）——保证存在"已取得 permit"的发送者：tokio 允许
    // 它们在 `Receiver::close()` 后完成发送，其命令最终随接收端
    // 销毁被丢弃，回复通道关闭后必须映射为 Closed，不允许
    // WorkerFailed 或挂起。
    let mut handles = Vec::new();
    for i in 0..1500 {
        let b = Arc::clone(&buffer);
        handles.push(tokio::spawn(async move {
            match b.push(batch(&msg(&format!("race-{i}")), "cnc-01", i)).await {
                Ok(()) | Err(LocalBufferError::Closed) => {}
                Err(e) => panic!("停机期间命令必须成功或 Closed，实际 {e:?}"),
            }
        }));
    }
    // 与命令流并发停机（竞态窗口：worker 处理 shutdown 时仍有命令
    // 在途或已入队）。
    tokio::time::sleep(Duration::from_millis(2)).await;
    buffer.shutdown().await.expect("shutdown");
    for h in handles {
        h.await.expect("任务");
    }
    assert!(
        buffer.next().await.is_err(),
        "停机完成后 next 必须失败（Closed），不得返回记录"
    );
    assert!(
        buffer
            .push(batch(&msg("race-last"), "cnc-01", 999))
            .await
            .is_err(),
        "停机完成后 push 必须失败（Closed）"
    );
}
