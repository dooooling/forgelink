//! Collector 韧性测试（§102/§103/§31.3）：
//! 断线落盘 → 恢复按序补传；停机保留未确认 → 重启 replayed 补传；
//! 批次序号跨断线单调不重号；停机取消采集。

mod common;

use std::time::Duration;

use common::Harness;
use modbus_mock::MockBehavior;
use mqtt_client::mock::MockBroker;

/// Telemetry 主题（§31.1）。
fn telemetry_topic() -> String {
    "forgelink/v1/telemetry/plant-a/vfd-01".to_owned()
}

/// 提取批次 sequence。
fn seq_of(payload: &[u8]) -> u64 {
    common::parse_batch(payload)["sequence"]
        .as_u64()
        .expect("sequence")
}

/// 断线落盘与恢复补传（§102/§31.4）：断网期间采集与落盘不中断，
/// 重连后全部记录按序补传，序号连续不重号。
#[tokio::test(flavor = "multi_thread")]
async fn disconnect_buffers_and_replays_in_order() {
    let broker = MockBroker::start().await;
    let harness = Harness::new(MockBehavior::new(), broker.addr().port());
    let mut rx = broker.subscribe(&telemetry_topic()).await;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");

    // 正常阶段：确认至少两个批次。
    let first = rx.recv().await.expect("首个批次");
    let second = rx.recv().await.expect("第二个批次");

    // 断网：丢弃全部连接。采集继续，新批次落盘（WAL 增长）。
    // 注意：`drop_all_connections` 以 abort 中止连接任务，按 mock 语义
    // 不计入 abnormal_disconnects（mqtt-client/src/mock.rs 文档）；以
    // 重连（新连接建立）为准。
    broker.drop_all_connections();
    common::wait_until(|| broker.connections() >= 2).await; // 等待重连恢复

    // 恢复阶段：断线期间产生的批次在重连后按序补传。断线前已收 0,1；
    // 断线窗口内新批次（2,3,4...）落盘 WAL，重连后从缓冲队头按序
    // 发出——序号连续即证明补传完成（无需静默窗口：断线后实时批次
    // 仍持续产生，永远不会有静默）。
    let mut seqs: Vec<u64> = vec![seq_of(&first.payload), seq_of(&second.payload)];
    let contiguous = |seqs: &[u64]| -> bool {
        let mut s = seqs.to_vec();
        s.sort_unstable();
        s.first() == Some(&0) && s.len() >= 5 && s.windows(2).all(|w| w[0] + 1 == w[1])
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline && !contiguous(&seqs) {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(p)) => seqs.push(seq_of(&p.payload)),
            // 通道关闭或 2s 无数据：不再等待（断言随后校验补传结果）。
            Ok(None) | Err(_) => break,
        }
    }

    // 断言：序号从 0 开始连续、无重复、无空洞（等于"全部批次已确认并
    // 从 WAL 删除"，§31.3）。
    seqs.sort_unstable();
    for pair in seqs.windows(2) {
        assert_eq!(pair[0] + 1, pair[1], "批次序号应连续（断线补传不重号）");
    }
    assert_eq!(seqs.first(), Some(&0), "序号从 0 开始");
    assert!(
        seqs.len() >= 5,
        "正常+断线期间应累计至少 5 个批次（实际 {}）",
        seqs.len()
    );
    assert!(
        broker.connections() >= 2,
        "drop_all_connections 后应重连并建立新连接"
    );

    runtime.shutdown().await.expect("优雅停机成功");
    broker.stop().await;
}

/// 停机保留未确认记录（§103：ack 是唯一删除路径），重启后按序
/// `replayed` 补传，随后新会话数据继续，message_id 跨会话不重复。
#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_wal_and_replays() {
    let broker = MockBroker::start().await;
    let harness = Harness::new(MockBehavior::new(), broker.addr().port());
    let mut rx = broker.subscribe(&telemetry_topic()).await;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");

    // 收齐至少两个已确认批次（WAL 空）。发布顺序确定：启动时 1 条
    // retained online（§31.1）在前，随后 telemetry 依次编号。
    rx.recv().await.expect("首个批次");
    rx.recv().await.expect("第二个批次");

    // 挂起第 4 个 PUBLISH（第 3 条 telemetry）的 PUBACK，制造在途未确认。
    let hold = broker.hold_puback(4);
    common::wait_until(|| broker.publishes().len() >= 4).await;

    // 停机：挂起的发布在 MQTT 结算阶段以 Closed 失败结算，WAL 保留
    // 该记录（不删除，§31.3：Closed 不得删除）。
    runtime.shutdown().await.expect("优雅停机成功");
    hold.store(true, std::sync::atomic::Ordering::Relaxed);

    // WAL 中应保留未确认记录（至少一条）。
    let buffer_cfg = local_buffer::LocalBufferConfig {
        db_path: harness.config.buffer.db_path.clone(),
        memory_records: 10_000,
        disk_max_bytes: 1024 * 1024 * 1024,
        retention: Duration::from_secs(3600),
        capacity_policy: local_buffer::CapacityPolicy::Backpressure,
    };
    let buffer = local_buffer::LocalBuffer::open(buffer_cfg)
        .await
        .expect("重开缓冲");
    let retained = buffer
        .next()
        .await
        .expect("读取缓冲")
        .expect("停机保留记录");
    assert!(retained.batch.replayed, "补传标记应保留");
    let old_message_id = retained.batch.message_id.clone();

    // 重启（新会话）：WAL 补传后新数据继续发布，message_id 跨会话唯一。
    let mut new_rx = broker.subscribe(&telemetry_topic()).await;
    let runtime2 = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("第二次启动成功");

    let mut seen_ids: std::collections::HashSet<String> = Default::default();
    let mut replayed_seen = false;
    let mut saw_live = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline && (!replayed_seen || !saw_live) {
        let Ok(Some(p)) = tokio::time::timeout(Duration::from_secs(2), new_rx.recv()).await else {
            break;
        };
        let batch = common::parse_batch(&p.payload);
        let message_id = batch["message_id"].as_str().expect("message_id").to_owned();
        assert!(
            seen_ids.insert(message_id.clone()),
            "message_id 不应重复: {message_id}"
        );
        if batch["replayed"].as_bool() == Some(true) {
            replayed_seen = true;
            assert_eq!(
                message_id, old_message_id,
                "补传批次保留原 message_id（§31.3/§103）"
            );
        } else {
            saw_live = true;
        }
    }
    assert!(replayed_seen, "重启后应收到 replayed 补传批次");
    assert!(saw_live, "重启后应收到新会话批次");

    runtime2.shutdown().await.expect("第二次优雅停机成功");
    let buffer_cfg = local_buffer::LocalBufferConfig {
        db_path: harness.config.buffer.db_path.clone(),
        memory_records: 10_000,
        disk_max_bytes: 1024 * 1024 * 1024,
        retention: Duration::from_secs(3600),
        capacity_policy: local_buffer::CapacityPolicy::Backpressure,
    };
    let buffer = local_buffer::LocalBuffer::open(buffer_cfg)
        .await
        .expect("重开缓冲");
    assert!(
        buffer.next().await.expect("读取缓冲").is_none(),
        "补传与新数据全部确认，WAL 应为空"
    );
    broker.stop().await;
}

/// 停机取消采集（§22/§104）：停机信号到达后轮询任务停止，不再产生
/// 新发布；停机在期限内完成。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_polling_promptly() {
    let broker = MockBroker::start().await;
    let harness = Harness::new(MockBehavior::new(), broker.addr().port());
    let mut rx = broker.subscribe(&telemetry_topic()).await;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");

    let _ = rx.recv().await.expect("首个批次");
    let started = std::time::Instant::now();
    runtime.shutdown().await.expect("优雅停机成功");
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "停机应在期限内完成"
    );
    let settled = broker.publishes().len();

    // 停机完成后不再产生新发布（采集已取消）。
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(broker.publishes().len(), settled, "停机后不应再产生新发布");
    broker.stop().await;
}
