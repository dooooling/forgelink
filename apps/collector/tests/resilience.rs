//! Collector 韧性测试（§102/§103/§31.3）：
//! 断线落盘 → 恢复按序补传；停机保留未确认记录 → 重启 replayed 按序
//! 补传（含显式制造的第二条积压，不依赖调度时序竞态）；批次序号
//! 跨断线单调不重号；停机取消采集。

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
/// 第二条积压记录由测试显式 push 制造（停机落盘是否产生第二条
/// 取决于调度时序，不作为断言依据，评审 P1）。
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

    // 显式制造第二条恢复记录（评审 P1：停机时管道是否有第二条批次
    // 完成落盘取决于调度时序，测试不得依赖该竞态）——重开缓冲后
    // 手动 push 一条独立批次，与在途记录构成**确定**的两条积压。
    let manual = data_pipeline::ObservationBatch {
        schema: "forgelink.telemetry.v1".to_owned(),
        message_id: format!("{old_message_id}-manual"),
        site_id: "plant-a".to_owned(),
        device_id: "vfd-01".to_owned(),
        sequence: 900,
        sent_at_ns: 0,
        replayed: false,
        observations: Vec::new(),
    };
    buffer
        .push(manual.clone())
        .await
        .expect("手动落盘第二条记录");
    // 关闭重开缓冲（评审 P2）：释放 worker 线程与 SQLite 连接，避免
    // 与重启后的新缓冲并发访问同一 WAL 文件（锁竞争 / CI 时序不稳）。
    // 已落盘记录保留（§103 停机语义，未确认记录不删除）。
    buffer.shutdown().await.expect("关闭重开缓冲");

    // 重启（新会话）：WAL 积压（在途记录 + 显式制造的第二条，若停机
    // 时序另有落盘批次一并）按序补传（replayed），随后新数据继续
    // 发布，message_id 跨会话唯一。
    let mut new_rx = broker.subscribe(&telemetry_topic()).await;
    let runtime2 = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("第二次启动成功");

    let mut seen_ids: std::collections::HashSet<String> = Default::default();
    // 补传批次按接收顺序收集（WAL 队头顺序 = 本地序号顺序，§31.4）。
    let mut replayed: Vec<(u64, String)> = Vec::new();
    let mut saw_live = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline && (replayed.len() < 2 || !saw_live) {
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
            replayed.push((seq_of(&p.payload), message_id));
        } else {
            saw_live = true;
        }
    }
    // 至少两条恢复记录：在途记录 + 显式制造的第二条（评审 P1：不得
    // 假设停机落盘必然产生第二条，断言只依赖确定性数据）。
    assert!(
        replayed.len() >= 2,
        "应补传在途记录与手动制造的第二条（实际 {} 条）",
        replayed.len()
    );
    assert_eq!(
        replayed[0].1, old_message_id,
        "补传从最早在途记录开始，保留原 message_id（§31.3/§103）"
    );
    assert!(
        replayed.iter().any(|(_, id)| *id == manual.message_id),
        "手动制造的第二条记录应按序补传"
    );
    assert!(
        replayed.windows(2).all(|w| w[0].0 < w[1].0),
        "补传按 WAL 队头顺序（本地序号升序，实际 {:?}）",
        replayed.iter().map(|(s, _)| *s).collect::<Vec<_>>()
    );
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

/// 发送循环遇永久性落盘错误（容量拒绝）时执行完整有序停机并返回
/// 错误（评审 P1）：`run_until_shutdown` 已消费 forward 结果，停机
/// 阶段不得再次 join 同一 JoinHandle（Tokio 会 panic），且后台任务
/// 全部清理。
#[tokio::test(flavor = "multi_thread")]
async fn forward_fatal_error_runs_full_shutdown() {
    let broker = MockBroker::start().await;
    let mut harness = Harness::new(MockBehavior::new(), broker.addr().port());
    // 磁盘容量极小 + Reject 策略：单条记录即超限，push 立即永久拒绝。
    harness.config.buffer.capacity_policy = collector::config::BufferCapacityPolicy::Reject;
    harness.config.buffer.disk_max_bytes = 64;
    harness.config.pipeline.flush_interval_ms = 100;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");
    // 注意：sender 必须存活（下划线前缀绑定不立即 drop），否则
    // changed() 立即返回 Err 被当作停机信号。
    let (_sig_tx, sig_rx) = tokio::sync::watch::channel(false);

    let started = std::time::Instant::now();
    let result = runtime.run_until_shutdown(sig_rx).await;
    assert!(
        result.is_err(),
        "永久落盘错误应上报运行时（实际 {result:?}）"
    );
    assert!(
        result.err().unwrap().to_string().contains("发送循环"),
        "错误应包含发送循环原因"
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "完整有序停机应在期限内完成"
    );
    broker.stop().await;
}

/// 暂停发布 + 背压容量满不死锁（评审 P1）：停机中断在途发布后
/// forward 只收不发，落盘单次限时 500ms——容量不足的批次进收尾
/// 队列（不静默丢弃），发送循环继续收尾不阻塞，停机在期限内完成
/// （不依赖外层 join 超时兜底）。
#[tokio::test(flavor = "multi_thread")]
async fn suspended_publish_backpressure_does_not_deadlock() {
    let broker = MockBroker::start().await;
    let mut harness = Harness::new(MockBehavior::new(), broker.addr().port());
    harness.config.buffer.capacity_policy = collector::config::BufferCapacityPolicy::Backpressure;
    // 批次 ~2.5KB（max_batch_size=5）：容量 4KB 装得下首批（发布确认
    // 后删除），hold 在途一批后剩余空间装不下新批 → 背压等待触发。
    harness.config.buffer.disk_max_bytes = 4096;
    harness.config.pipeline.max_batch_size = 5;
    harness.config.pipeline.flush_interval_ms = 100;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");
    // 正常阶段：确认两个批次已发布（publish 0 = online retained，
    // 1/2 = telemetry；已确认即删除，磁盘空）。
    common::wait_until(|| broker.publishes().len() >= 3).await;
    let mut rx = broker.subscribe(&telemetry_topic()).await;
    rx.recv().await.expect("首个批次");
    rx.recv().await.expect("第二个批次");

    // 挂起第 3 个 PUBLISH 的 PUBACK：在途批次不确认 → WAL 不删除 →
    // 磁盘容量占满（4096B 约 1-2 批）；后续批次 push 背压等待，500ms
    // 后进收尾队列，forward 继续循环（不退出、不 panic、不阻塞）。
    let hold = broker.hold_puback(3);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 停机：在途发布被中断 → 只收不发；容量仍满 → 落盘限时进收尾
    // 队列；停机在期限内完成。
    let started = std::time::Instant::now();
    let result = runtime.shutdown().await;
    assert!(
        result.is_ok(),
        "停机应在期限内完成，不得依赖外层 join 超时（实际 {result:?}）"
    );
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "停机耗时异常（{:.1}s）",
        started.elapsed().as_secs_f32()
    );
    hold.store(true, std::sync::atomic::Ordering::Relaxed);
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
