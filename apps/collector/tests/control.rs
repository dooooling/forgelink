//! Collector 控制链路集成测试（§31.5/§81/§90/§98）：真实 HTTP、modbus-mock
//! 与 mock broker 全链路——Bearer 认证（401）、角色授权（403）、属性写入
//! 落地到 Mock 寄存器、凭据缺失 fail-closed 启动失败、在途控制请求停机
//! 不挂起且 Journal 无残留。
//!
//! 仅 `control` feature 构建编译运行（只读构建下本文件为空，§98）。
//!
//! 使用最简 HTTP/1.1 客户端（std `TcpStream`，与 tests/rest.rs 同模式，
//! 避免测试引入重型 HTTP 依赖）；请求带 `Connection: close`。

#![cfg(feature = "control")]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

// Harness 类型经 common::harness_control 构造，无需显式导入。
use modbus_mock::{Kind, MockBehavior};
use mqtt_client::mock::MockBroker;
use serde_json::Value;

/// §90.2 测试凭据（与 common::write_credentials 同源）：alice=operator
/// 可控制，bob=viewer 只读。
const TOKEN_OPERATOR: &str = "token-alice-operator-0123456789abcdef";
const TOKEN_VIEWER: &str = "token-bob-viewer-0123456789abcdef00";

/// 仅可写属性（Profile `drive.control.target_freq`：40100、scale 0.01、
/// 范围 [0, 50] Hz）。
const WRITE_PATH: &str = "drive.control.target_freq";
/// 40100 的协议偏移（40001 -> 0）。
const TARGET_OFFSET: u16 = 99;

/// 发送 HTTP/1.1 请求并返回 (状态码, JSON 响应体)。
fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: Option<&str>,
) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).expect("连接 REST 服务器");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("读超时");
    let body = body.unwrap_or("");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(token) = authorization {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    write!(stream, "{request}{body}").expect("写入请求");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("读取响应");
    let text = String::from_utf8(buf).expect("响应为 ASCII/UTF-8");
    let (head, resp_body) = text.split_once("\r\n\r\n").expect("响应含头部与体分隔");
    let status = head
        .lines()
        .next()
        .expect("状态行")
        .split_whitespace()
        .nth(1)
        .expect("状态码")
        .parse::<u16>()
        .expect("状态码数字");
    let parsed: Value = if resp_body.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(resp_body).expect("响应体为 JSON")
    };
    (status, parsed)
}

/// 构造属性写入请求体（§32.2）。
fn write_body(request_id: &str, value: f64) -> String {
    format!(
        r#"{{"schema":"forgelink.control.request.v1","request_id":"{request_id}","kind":"property_write","items":[{{"path":"{WRITE_PATH}","value":{value}}}]}}"#
    )
}

/// 轮询状态端点直至 settled（§77 三态；真实引擎异步结算，毫秒级）。
///
/// 状态查询路径带 device_id（查询键与幂等键 §80.1 对齐——request_id 的
/// 唯一性作用域是设备）。
async fn wait_until_settled(addr: SocketAddr, device_id: &str, request_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Instant::now() < deadline,
            "等待控制请求 {request_id} 结算超时"
        );
        let (status, body) = http(
            addr,
            "GET",
            &format!("/api/v1/devices/{device_id}/control-requests/{request_id}"),
            Some(TOKEN_OPERATOR),
            None,
        );
        assert_eq!(status, 200, "状态查询应成功: {body}");
        // 三态字段名为 `state`（§31.5 Normative）。
        match body["state"].as_str().expect("state 字段") {
            "settled" => return body,
            // 受理成功后台账必有条目：running 是唯一中间态。
            other => assert_eq!(other, "running", "意外状态: {body}"),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 全链路（§31.5/§81/§88）：operator Bearer 提交属性写入 → 202 受理 →
/// 轮询至 settled(succeeded) → modbus-mock 寄存器值已按 Profile 逆变换
/// 更新（25.5 Hz / scale 0.01 → raw 2550）。
#[tokio::test(flavor = "multi_thread")]
async fn control_write_end_to_end_updates_register() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[5000, 2000])
        .with_coil_range(1, 0, &[true]);
    let broker = MockBroker::start().await;
    let harness = common::harness_control(broker.addr().port(), behavior);
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");
    let addr = runtime.rest_addr().expect("REST 已启用");

    // 写入前寄存器为初值（未写入 2550）。
    assert_ne!(
        harness
            .server
            .value(1, Kind::HoldingRegister, TARGET_OFFSET),
        Some(2550),
        "写入前寄存器不应已是目标值"
    );

    let request_id = format!("ctl-e2e-{}", collector::now_ns());
    let (status, body) = http(
        addr,
        "POST",
        "/api/v1/devices/vfd-01/controls",
        Some(TOKEN_OPERATOR),
        Some(&write_body(&request_id, 25.5)),
    );
    assert_eq!(status, 202, "受理应返回 202: {body}");
    assert_eq!(body["schema"], "forgelink.control.accepted.v1");
    assert_eq!(body["request_id"], request_id);
    assert_eq!(body["status"], "accepted");

    // 轮询至终态：执行成功（Driver 写入完成）。
    let settled = wait_until_settled(addr, "vfd-01", &request_id).await;
    assert_eq!(settled["result"]["status"], "succeeded", "{settled}");
    assert_eq!(settled["result"]["request_id"], request_id);
    assert_eq!(settled["result"]["device_id"], "vfd-01");

    // 寄存器已变：Profile 逆变换 25.5 / 0.01 = 2550（§75.1）。
    assert_eq!(
        harness
            .server
            .value(1, Kind::HoldingRegister, TARGET_OFFSET),
        Some(2550),
        "控制写入应落地到 Mock 寄存器"
    );

    runtime.shutdown().await.expect("优雅停机");
    broker.stop().await;
}

/// 未知 Bearer Token 一律 401 UNAUTHENTICATED（fail-closed，§90.2），
/// 且错误信息不回显 Token 内容。
#[tokio::test(flavor = "multi_thread")]
async fn unknown_token_rejected_401() {
    let broker = MockBroker::start().await;
    let harness = common::harness_control(broker.addr().port(), MockBehavior::new());
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");
    let addr = runtime.rest_addr().expect("REST 已启用");

    let secret = "token-mallory-not-in-file";
    let (status, body) = http(
        addr,
        "POST",
        "/api/v1/devices/vfd-01/controls",
        Some(secret),
        Some(&write_body("ctl-401", 10.0)),
    );
    assert_eq!(status, 401, "未知 Token 必须 401: {body}");
    assert_eq!(body["schema"], "forgelink.error.v1");
    assert_eq!(body["code"], "UNAUTHENTICATED");
    let text = body.to_string();
    assert!(!text.contains(secret), "错误信息不得回显 Token 内容");

    // 未认证请求不得触发任何 Driver 写入。
    assert!(
        harness.server.write_records().is_empty(),
        "未认证请求不得下发写入"
    );

    runtime.shutdown().await.expect("优雅停机");
    broker.stop().await;
}

/// viewer 角色提交属性写入 → 引擎授权拒绝收据 INSUFFICIENT_ROLE → 403
/// （§83/§86 默认策略：属性写入要求 Operator），且设备未被写入。
#[tokio::test(flavor = "multi_thread")]
async fn viewer_role_write_rejected_403_and_register_unchanged() {
    let behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[5000, 2000])
        .with_coil_range(1, 0, &[true]);
    let broker = MockBroker::start().await;
    let harness = common::harness_control(broker.addr().port(), behavior);
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");
    let addr = runtime.rest_addr().expect("REST 已启用");

    let (status, body) = http(
        addr,
        "POST",
        "/api/v1/devices/vfd-01/controls",
        Some(TOKEN_VIEWER),
        Some(&write_body("ctl-403", 30.0)),
    );
    assert_eq!(status, 403, "viewer 必须被拒绝: {body}");
    assert_eq!(body["code"], "INSUFFICIENT_ROLE");

    // 拒绝发生在 Driver 前（§84）：无写请求到达 Mock，寄存器未变。
    assert!(
        harness.server.write_records().is_empty(),
        "被拒绝的请求不得下发写入"
    );

    runtime.shutdown().await.expect("优雅停机");
    broker.stop().await;
}

/// 凭据文件缺失 → 启动失败（fail-closed，§90.2）：不得静默降级为
/// 无认证控制。
#[tokio::test(flavor = "multi_thread")]
async fn missing_credentials_file_fails_startup_fail_closed() {
    let broker = MockBroker::start().await;
    let mut harness = common::harness_control(broker.addr().port(), MockBehavior::new());
    harness
        .config
        .control
        .as_mut()
        .expect("control 段已配置")
        .credentials_file = harness.temp.path().join("no-such-credentials.json");

    let err = match collector::CollectorRuntime::start(harness.config.clone()).await {
        Ok(_) => panic!("凭据缺失必须启动失败（fail-closed，§90.2）"),
        Err(e) => e,
    };
    assert!(
        matches!(err, collector::error::CollectorError::Control(_)),
        "应为控制链路装配错误: {err}"
    );
    assert!(
        err.to_string().contains("凭据"),
        "错误应说明凭据加载失败: {err}"
    );
    broker.stop().await;
}

/// 在途控制请求时停机（§93/§104）：不挂起、收据就绪（Journal 有 Settle
/// 记录）、Journal 无残留 Running。Mock 响应延迟 300ms 制造在途窗口
/// （写入请求已到达 Mock、响应未返回时停机）；延迟不能更长——它同样
/// 作用于轮询读，读与写共用会话锁（§82），过长的读会推迟写的完成时刻，
/// 可能越过请求自身的有效超时（策略上限 5s）使结算变为 Timeout/
/// Indeterminate，破坏本测试的自然完成前提。
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_with_inflight_control_settles_without_hanging() {
    let mut behavior = MockBehavior::new()
        .with_holding_range(1, 0, &[5000, 2000])
        .with_coil_range(1, 0, &[true]);
    behavior.response_delay = Some(Duration::from_millis(300));
    let broker = MockBroker::start().await;
    let harness = common::harness_control(broker.addr().port(), behavior);
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("Collector 启动成功");
    let addr = runtime.rest_addr().expect("REST 已启用");

    let request_id = format!("ctl-inflight-{}", collector::now_ns());
    let (status, body) = http(
        addr,
        "POST",
        "/api/v1/devices/vfd-01/controls",
        Some(TOKEN_OPERATOR),
        Some(&write_body(&request_id, 12.5)),
    );
    assert_eq!(status, 202, "受理应返回 202: {body}");

    // 等待写入请求已到达 Mock（在途窗口内），随后立即停机。
    common::wait_until(|| !harness.server.write_records().is_empty()).await;
    let started = Instant::now();
    runtime.shutdown().await.expect("优雅停机不应挂起");
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "停机应在引擎宽限与采集排空预算内完成（实际 {:?}）",
        started.elapsed()
    );

    // Journal 无残留：每条 Insert 都有对应 Settle（收据就绪落定），
    // 且本请求以 Succeeded 终结（1.5s < 宽限 5s，自然完成）。
    let journal_path = harness.temp.path().join("control-journal.jsonl");
    let text =
        std::fs::read_to_string(&journal_path).expect("缺省 Journal 应落在数据目录（WAL 同目录）");
    let mut inserts: Vec<String> = Vec::new();
    let mut settled: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(line).expect("Journal 行应为 JSON");
        if let Some(insert) = record.get("Insert") {
            inserts.push(
                insert["key"]["request_id"]
                    .as_str()
                    .expect("request_id")
                    .to_owned(),
            );
        }
        if let Some(settle) = record.get("Settle") {
            settled.push((
                settle["key"]["request_id"]
                    .as_str()
                    .expect("request_id")
                    .to_owned(),
                settle["result"]["status"]
                    .as_str()
                    .expect("status")
                    .to_owned(),
            ));
        }
    }
    for request_id in &inserts {
        assert!(
            settled.iter().any(|(id, _)| id == request_id),
            "Insert {request_id} 无对应 Settle（Journal 残留未结算记录）"
        );
    }
    let (_, status_text) = settled
        .iter()
        .find(|(id, _)| *id == request_id)
        .expect("本请求应有 Journal 记录");
    assert_eq!(
        status_text, "succeeded",
        "宽限内的在途写入应自然完成并以 Succeeded 结算"
    );

    // 写入最终落地（自然完成后生效）。
    assert_eq!(
        harness
            .server
            .value(1, Kind::HoldingRegister, TARGET_OFFSET),
        Some(1250),
        "12.5 Hz / scale 0.01 → raw 1250"
    );
    broker.stop().await;
}
