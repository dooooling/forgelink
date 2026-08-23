//! Collector 端到端测试（§93/§100 验收）：
//! Mock Modbus → Poll Engine → Device Manager → Data Pipeline
//! → Local Buffer/WAL → Mock MQTT Broker 全链路。

mod common;

use std::time::Duration;

use common::Harness;
use modbus_mock::MockBehavior;
use mqtt_client::mock::MockBroker;

/// Telemetry 主题（§31.1）。
fn telemetry_topic() -> String {
    "forgelink/v1/telemetry/plant-a/vfd-01".to_owned()
}

/// 全链路端到端：Mock 寄存器值 → 按序组包 → MQTT 发布（PUBACK 后
/// 删除 WAL）→ 优雅停机排空。
#[tokio::test(flavor = "multi_thread")]
async fn e2e_modbus_to_mqtt_full_chain() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[5000, 2000])
        .with_coil_range(1, 0, &[true]);
    let broker = MockBroker::start().await;
    let harness = Harness::new(behavior, broker.addr().port());

    let mut rx = broker.subscribe(&telemetry_topic()).await;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");

    // 等待收齐**两个不同 sequence** 的批次（50ms 与 100ms 两个采集组各
    // 一轮）。§31.3/§31.4：QoS 1 at-least-once——连接瞬时断开时未确认
    // 批次由客户端自动重发（`replayed=true` 且保留原 message_id/sequence），
    // CI 慢环境偶发。因此不能假设"收到的前两条消息分别是 seq 0 和 1"：
    // 合法的重复消息会打乱顺序/重复出现。按 sequence 去重收集，直到
    // 收齐 {0, 1}；本会话新鲜性由 message_id 内嵌的 collector_session_id
    // 保证（跨会话 WAL 污染必然携带不同 session）。
    let session_of = |payload: &[u8]| -> String {
        let batch = common::parse_batch(payload);
        let message_id = batch["message_id"].as_str().expect("message_id");
        let colon = message_id.find(':').expect("长度前缀分隔符");
        let len: usize = message_id[..colon].parse().expect("长度数字");
        message_id[colon + 1..colon + 1 + len].to_owned()
    };
    let mut seen: std::collections::HashMap<u64, Vec<u8>> = std::collections::HashMap::new();
    // 首条消息同样入集合（它可能就是 seq 0）。
    {
        let first = rx.recv().await.expect("应收到首个批次");
        let batch = common::parse_batch(&first.payload);
        assert_eq!(batch["schema"], "forgelink.telemetry.v1");
        assert_eq!(batch["site_id"], "plant-a");
        assert_eq!(batch["device_id"], "vfd-01");
        let session = session_of(&first.payload);
        let seq = batch["sequence"].as_u64().expect("sequence");
        seen.insert(seq, first.payload);
        // 收集直到凑齐两个不同 sequence（重复消息跳过；30s 上限防挂起）。
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while seen.len() < 2 {
            let payload = match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(msg)) => msg.payload,
                _ => panic!("30s 内未收齐两个不同 sequence 的批次"),
            };
            let batch = common::parse_batch(&payload);
            assert_eq!(session_of(&payload), session, "所有消息必须同属当前会话");
            seen.insert(batch["sequence"].as_u64().expect("sequence"), payload);
        }
    }
    // 两个采集组（50ms/100ms 轮询）各产出批次：收到的两个不同 sequence
    // 必须是该会话的最初两批（0 与 1）——QoS 1 重发只会重复已有 seq，
    // 不会产生新 seq。
    let mut seqs: Vec<u64> = seen.keys().copied().collect();
    seqs.sort_unstable();
    assert_eq!(seqs, vec![0, 1], "应收齐该会话的前两个批次 sequence 0 与 1");
    for (expect_seq, payload) in &seen {
        let batch = common::parse_batch(payload);
        let message_id = batch["message_id"].as_str().expect("message_id");
        assert!(
            message_id.contains(&expect_seq.to_string()),
            "message_id 应内嵌 sequence {expect_seq}: {message_id}"
        );
    }

    // 值映射（§37.1 + §7.3）：40001=5000→50.0 Hz、40002=2000→20.0 A、
    // coil:1=true（按批次内容验证，避免两个批次顺序假设）。
    let mut saw_frequency = false;
    let mut saw_current = false;
    let mut saw_status = false;
    for p in seen.values() {
        let batch = common::parse_batch(p);
        for obs in batch["observations"].as_array().expect("observations 数组") {
            match obs["path"].as_str().expect("path") {
                "drive.output.frequency" => {
                    assert_eq!(obs["value"]["f64"], 50.0);
                    saw_frequency = true;
                }
                "drive.output.current" => {
                    assert_eq!(obs["value"]["f64"], 20.0);
                    saw_current = true;
                }
                "drive.run.status" => {
                    assert_eq!(obs["value"]["bool"], true);
                    saw_status = true;
                }
                other => panic!("未知 path: {other}"),
            }
            assert!(obs["quality"].is_object());
            assert!(obs["ingest_timestamp_ns"].is_number());
            assert!(obs["observation_id"].as_str().unwrap().contains("plant-a"));
        }
    }
    assert!(
        saw_frequency && saw_current && saw_status,
        "三个属性都应出现"
    );

    // 健康状态（§104）：设备注册、MQTT 已确认计数增长（PUBACK 异步）。
    common::wait_until(|| {
        let h = runtime.health();
        h.devices.len() == 1 && h.mqtt.publishes_acked >= 2
    })
    .await;

    // 优雅停机（§31.3：PUBACK 是唯一删除路径）。停机瞬间若有在途
    // 发布等待 PUBACK 会被中断并合法保留（重启补传）——慢环境更易
    // 命中该竞态，故残留记录必须带补传标记，按唯一删除路径 ack 清空。
    runtime.shutdown().await.expect("优雅停机成功");
    let config = local_buffer_config(&harness);
    let buffer = local_buffer::LocalBuffer::open(config)
        .await
        .expect("重开缓冲");
    let mut retained = 0;
    while let Some(stored) = buffer.next().await.expect("读取缓冲") {
        assert!(
            stored.batch.replayed,
            "停机保留的记录必须带补传标记（{}）",
            stored.batch.message_id
        );
        buffer.ack(stored.local_seq).await.expect("ack 清空残留");
        retained += 1;
    }
    assert!(
        retained <= 2,
        "残留应至多为停机瞬间的在途记录（实际 {retained} 条）"
    );
    buffer.shutdown().await.expect("关闭缓冲");

    // 在线状态（§31.1）：启动时每设备 retained online。
    let publishes = broker.publishes();
    let online = publishes
        .iter()
        .find(|p| p.topic == "forgelink/v1/status/plant-a/vfd-01" && p.retain);
    assert!(online.is_some(), "应有 retained 在线状态");
    broker.stop().await;
}

/// 停机排空：管道内未 flush 的 Observation 在停机时组包输出并发布
/// （§104 有限排空），不会丢失。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_pending_observations() {
    let mut harness = Harness::new(MockBehavior::new(), 0);
    harness.config.pipeline.flush_interval_ms = 3_600_000; // 长期不自动 flush
    harness.config.pipeline.max_batch_size = 100;
    let broker = MockBroker::start().await;
    harness.config.northbound.mqtt.broker_port = broker.addr().port();

    let mut rx = broker.subscribe(&telemetry_topic()).await;
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");

    // 等待管道收到至少一轮采集数据（不依赖 flush：直接停机触发排空）。
    common::wait_until(|| runtime.health().devices[0].last_batch_at_ns.is_some()).await;

    let started_at = std::time::Instant::now();
    runtime.shutdown().await.expect("优雅停机成功");
    assert!(
        started_at.elapsed() < Duration::from_secs(20),
        "停机不应长时间阻塞"
    );

    // 停机完成后管道排空的批次应已发布（消息不丢）。
    let mut received = Vec::new();
    while let Ok(b) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
        received.push(b.expect("通道不应关闭"));
    }
    assert!(!received.is_empty(), "停机排空应发布至少一个批次");
    for p in &received {
        let batch = common::parse_batch(&p.payload);
        assert_eq!(batch["site_id"], "plant-a");
        assert_eq!(batch["device_id"], "vfd-01");
    }
    // 批次序号按设备单调递增（§31.2）。
    let mut seqs: Vec<u64> = received
        .iter()
        .map(|p| {
            common::parse_batch(&p.payload)["sequence"]
                .as_u64()
                .unwrap()
        })
        .collect();
    seqs.sort_unstable();
    for pair in seqs.windows(2) {
        assert_eq!(pair[0] + 1, pair[1], "批次序号连续");
    }
    broker.stop().await;
}

fn local_buffer_config(harness: &Harness) -> local_buffer::LocalBufferConfig {
    local_buffer::LocalBufferConfig {
        db_path: harness.config.buffer.db_path.clone(),
        memory_records: 10_000,
        disk_max_bytes: 1024 * 1024 * 1024,
        retention: Duration::from_secs(3600),
        capacity_policy: local_buffer::CapacityPolicy::Backpressure,
    }
}
