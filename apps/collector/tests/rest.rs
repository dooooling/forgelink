//! REST v1 只读接口集成测试（§31.5/§31.6/§104）：真实 HTTP 请求验证
//! Collector 运行时接入——设备查询、健康状态、错误模型、敏感字段、
//! 错误信息隔离（稳定错误码，§90.1）、并发不阻塞采集、优雅停机与
//! 控制路由未暴露。
//!
//! 使用最简 HTTP/1.1 客户端（std `TcpStream`），避免测试引入重型
//! HTTP 依赖；请求带 `Connection: close` 使服务端响应后关闭连接。

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use common::Harness;
use modbus_mock::MockBehavior;
use mqtt_client::mock::MockBroker;
use serde_json::Value;

/// 发送 GET 请求并返回 (状态码, JSON 响应体)。
fn http_get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).expect("连接 REST 服务器");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("读超时");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .expect("写入请求");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("读取响应");
    let text = String::from_utf8(buf).expect("响应为 ASCII/UTF-8");
    let (head, body) = text.split_once("\r\n\r\n").expect("响应含头部与体分隔");
    let status = head
        .lines()
        .next()
        .expect("状态行")
        .split_whitespace()
        .nth(1)
        .expect("状态码")
        .parse::<u16>()
        .expect("状态码数字");
    let body: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(body).expect("响应体为 JSON")
    };
    (status, body)
}

/// 带 REST 启用的 Harness（`listen: 127.0.0.1:0` 随机端口，§90.1 loopback）。
fn harness_rest(broker_port: u16) -> Harness {
    let mut harness = Harness::new(MockBehavior::new(), broker_port);
    harness.config.rest = collector::config::RestOptions {
        listen: Some("127.0.0.1:0".to_owned()),
        max_concurrency: 16,
    };
    harness
}

#[tokio::test(flavor = "multi_thread")]
async fn rest_readonly_endpoints_full_chain() {
    let broker = MockBroker::start().await;
    let harness = harness_rest(broker.addr().port());
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("运行时启动");
    let addr = runtime.rest_addr().expect("REST 已启用");

    // 1) 设备列表：schema + 设备字段（§31.5）。
    let (status, body) = http_get(addr, "/api/v1/devices");
    assert_eq!(status, 200, "设备列表应成功");
    assert_eq!(body["schema"], "forgelink.devices.v1");
    let device = &body["devices"][0];
    assert_eq!(device["device_id"], "vfd-01");
    assert_eq!(device["domain"], "drive");
    assert_eq!(device["driver_id"], "modbus-tcp");
    assert_eq!(device["profile_id"], "inovance-md500");
    assert_eq!(device["enabled"], true);
    assert!(device["read_items"].as_u64().unwrap() >= 3);
    let groups = device["groups"].as_array().expect("groups 数组");
    assert!(groups.iter().any(|g| g["interval_ms"] == 50), "50ms 组存在");
    assert!(
        groups.iter().any(|g| {
            g["paths"]
                .as_array()
                .expect("paths")
                .iter()
                .any(|p| p == "drive.run.status")
        }),
        "drive.run.status 在 100ms 组"
    );

    // 2) 单设备（§31.5）。
    let (status, body) = http_get(addr, "/api/v1/devices/vfd-01");
    assert_eq!(status, 200);
    assert_eq!(body["schema"], "forgelink.device.v1");
    assert_eq!(body["device"]["device_id"], "vfd-01");

    // 3) 资源与属性（§5 最小资源树 + §37 属性视图）。
    let (status, body) = http_get(addr, "/api/v1/devices/vfd-01/resources");
    assert_eq!(status, 200);
    assert_eq!(body["schema"], "forgelink.resources.v1");
    let resources = body["resources"].as_array().expect("resources 数组");
    assert!(
        resources.iter().any(|r| r["path"] == "drive"),
        "顶层资源 drive"
    );
    assert!(
        resources
            .iter()
            .any(|r| r["path"] == "drive.output" && r["kind"] == "drive"),
        "资源 drive.output（kind=drive）"
    );
    // 仅可写属性也必须进入资源树（评审 P2：只读采集过滤不得丢失属性）。
    assert!(
        resources
            .iter()
            .any(|r| r["path"] == "drive.control" && r["kind"] == "drive"),
        "仅可写属性 drive.control.target_freq 派生的资源 drive.control"
    );

    let (status, body) = http_get(addr, "/api/v1/devices/vfd-01/properties");
    assert_eq!(status, 200);
    assert_eq!(body["schema"], "forgelink.properties.v1");
    let properties = body["properties"].as_array().expect("properties 数组");
    assert!(properties.len() >= 4);
    assert!(
        properties
            .iter()
            .any(|p| p["path"] == "drive.output.frequency" && p["unit"] == "Hz")
    );
    // 仅可写属性：readable=false、writable=true、无采集间隔（null，
    // 评审 P2：属性清单不得因只读采集而缺失）。
    let write_only = properties
        .iter()
        .find(|p| p["path"] == "drive.control.target_freq")
        .expect("仅可写属性在属性清单中");
    assert_eq!(write_only["readable"], false);
    assert_eq!(write_only["writable"], true);
    assert!(write_only["interval_ms"].is_null(), "仅可写属性无采集间隔");
    assert_eq!(write_only["min"]["f64"], 0.0, "语义范围 min");
    // read_items 仍是可读属性数（§22 Tag 数），不含仅可写属性。
    assert_eq!(
        properties.iter().filter(|p| p["readable"] == true).count(),
        properties.len() - 1,
        "仅一个仅可写属性"
    );
    // 敏感字段不泄漏：Driver 地址、连接配置、凭据（§90.1）。
    let text = serde_json::to_string(&body).expect("序列化");
    for banned in [
        "driver_address",
        "1!40001",
        "connection",
        "password",
        "username",
        "ca_pem",
        "private_key",
        "db_path",
    ] {
        assert!(!text.contains(banned), "属性响应不得泄漏 {banned:?}");
    }

    // 4) 健康状态（§104）：等待设备首个批次到达后字段正确。
    common::wait_until(|| {
        let h = runtime.health();
        h.devices
            .first()
            .is_some_and(|d| d.last_batch_at_ns.is_some())
    })
    .await;
    let (status, body) = http_get(addr, "/api/v1/health");
    assert_eq!(status, 200);
    assert_eq!(body["schema"], "forgelink.health.v1");
    assert_eq!(body["status"], "ok");
    assert_eq!(body["site_id"], "plant-a");
    assert!(body["session_id"].as_str().unwrap().contains("plant-a"));
    assert!(body["started_at_ns"].as_i64().unwrap() > 0);
    assert_eq!(body["devices"][0]["device_id"], "vfd-01");
    assert!(
        body["devices"][0]["last_batch_at_ns"].as_i64().unwrap() > 0,
        "设备最近采集时间应已记录"
    );

    // 5) 错误模型（§31.6）：404 含 schema + request_id。
    let (status, body) = http_get(addr, "/api/v1/devices/nope");
    assert_eq!(status, 404);
    assert_eq!(body["schema"], "forgelink.error.v1");
    assert_eq!(body["code"], "DEVICE_NOT_FOUND");
    assert!(body["request_id"].as_str().unwrap().starts_with("req-"));

    // 6) 控制路由未暴露（§31.5：本分支只读，不实现控制链路）。
    for path in [
        "/api/v1/devices/vfd-01/controls",
        "/api/v1/control-requests/cmd-1",
    ] {
        let mut stream = TcpStream::connect(addr).expect("连接");
        write!(
            stream,
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
        )
        .expect("写入");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).expect("读取");
        let text = String::from_utf8(buf).expect("文本");
        let status = text
            .split_once("\r\n")
            .expect("状态行")
            .0
            .split_whitespace()
            .nth(1)
            .expect("状态码")
            .parse::<u16>()
            .expect("数字");
        assert_eq!(status, 404, "控制路由 {path} 必须 404");
        let body: Value = serde_json::from_str(text.split_once("\r\n\r\n").expect("分隔").1)
            .expect("错误载荷 JSON");
        assert_eq!(body["schema"], "forgelink.error.v1");
    }

    // 7) 并发请求不阻塞采集（有界并发 + 快照短锁）。
    let mut handles = Vec::new();
    for _ in 0..16 {
        handles.push(std::thread::spawn(move || {
            let (status, _) = http_get(addr, "/api/v1/devices");
            assert_eq!(status, 200);
        }));
    }
    for h in handles {
        h.join().expect("并发请求线程");
    }
    // 并发查询期间采集批次仍持续产出（至少再收到 1 批）。
    let before = runtime.health().mqtt.publishes_acked;
    common::wait_until(|| runtime.health().mqtt.publishes_acked > before).await;

    // 8) API 优雅停机：shutdown 后端口关闭、采集数据不丢（WAL 清空）。
    runtime.shutdown().await.expect("优雅停机");
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_err(),
        "停机后 REST 端口应关闭"
    );
}

/// §90.1 信息隔离（评审 P1）：设备失败与 MQTT 发布失败时，REST 响应
/// 的 `last_error` 只含稳定错误码，不得回传驱动/MQTT 原始错误文本
/// （可能含连接地址、文件路径等内部细节）。
#[tokio::test(flavor = "multi_thread")]
async fn health_errors_expose_only_stable_codes() {
    // 1) 设备采集失败：Mock Modbus 收到请求即断开连接，驱动原始错误
    //    文本包含目标地址（127.0.0.1:端口）。
    let broker = MockBroker::start().await;
    let mut behavior = MockBehavior::new();
    behavior.drop_connection = true;
    let mut harness = Harness::new(behavior, broker.addr().port());
    harness.config.rest = collector::config::RestOptions {
        listen: Some("127.0.0.1:0".to_owned()),
        max_concurrency: 16,
    };
    // 有限重连次数：broker 断连后客户端立即以 Disconnected 结算在途
    // 发布（§31.3：断线不得静默挂起），测试无需等待指数退避。
    harness.config.northbound.mqtt.max_reconnect_retries = Some(1);
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("运行时启动");
    let addr = runtime.rest_addr().expect("REST 已启用");

    // 设备失败记录（驱动断线重连耗尽后写入稳定错误码）。
    common::wait_until(|| {
        runtime
            .health()
            .devices
            .first()
            .is_some_and(|d| d.last_error.is_some())
    })
    .await;
    let (status, body) = http_get(addr, "/api/v1/health");
    assert_eq!(status, 200);
    assert_stable_code(
        body["devices"][0]["last_error"]
            .as_str()
            .expect("健康接口设备错误码"),
        "/api/v1/health 设备 last_error",
    );
    let (status, body) = http_get(addr, "/api/v1/devices");
    assert_eq!(status, 200);
    assert_stable_code(
        body["devices"][0]["last_error"]
            .as_str()
            .expect("设备列表错误码"),
        "/api/v1/devices 设备 last_error",
    );

    // 2) 北向发布失败：中断全部连接（重连上限为 1，客户端立即退出），
    //    在途发布以 Disconnected 结算失败并记录稳定错误码。
    common::wait_until(|| broker.connections() >= 1).await;
    broker.drop_all_connections();
    common::wait_until(|| runtime.health().mqtt.last_error.is_some()).await;
    let (status, body) = http_get(addr, "/api/v1/health");
    assert_eq!(status, 200);
    assert_stable_code(
        body["mqtt"]["last_error"]
            .as_str()
            .expect("健康接口 MQTT 错误码"),
        "/api/v1/health mqtt.last_error",
    );

    runtime.shutdown().await.expect("优雅停机");
    broker.stop().await;
}

/// 稳定错误码校验：小写字母/数字/下划线/连字符，不含地址、路径、
/// 端口等内部细节（§90.1：错误消息只进脱敏日志）。
fn assert_stable_code(code: &str, what: &str) {
    assert!(
        !code.is_empty()
            && code
                .chars()
                .all(|c| { c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' }),
        "{what} 应为稳定错误码，实际 {code:?}"
    );
    for banned in ["127.0.0.1", ":", "/", "\\"] {
        assert!(!code.contains(banned), "{what} 不得泄漏内部细节: {code:?}");
    }
}

/// 无 REST 配置时默认不监听（§90.1：REST 默认禁用）。
#[tokio::test(flavor = "multi_thread")]
async fn rest_disabled_by_default() {
    let broker = MockBroker::start().await;
    let harness = Harness::new(MockBehavior::new(), broker.addr().port());
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("运行时启动");
    assert!(runtime.rest_addr().is_none(), "默认不启用 REST");
    runtime.shutdown().await.expect("优雅停机");
}

/// REST 绑定失败：fail-fast 且已启动组件（轮询任务等）被回收
/// （评审 P2：不得遗留后台任务与阻塞 Driver 调用）。
#[tokio::test(flavor = "multi_thread")]
async fn rest_bind_failure_fails_start_and_cleans_up() {
    let broker = MockBroker::start().await;
    // 先占用一个端口，让 REST 绑定必然失败。
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").expect("占用端口");
    let port = blocker.local_addr().expect("获取端口").port();
    let mut harness = Harness::new(MockBehavior::new(), broker.addr().port());
    harness.config.rest.listen = Some(format!("127.0.0.1:{port}"));
    harness.config.rest.max_concurrency = 4;

    let err = match collector::CollectorRuntime::start(harness.config.clone()).await {
        Ok(_) => panic!("端口占用必须启动失败"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, collector::error::CollectorError::Rest(_)),
        "应为 REST 绑定错误: {err}"
    );
    // start() 返回即清理完成：轮询任务被取消并等待退出（scheduler
    // shutdown 为异步等待，start 内部完成）。这里再正常启动一次验证
    // 系统未被遗留任务污染（端口释放后）。
    drop(blocker);
    let runtime = collector::CollectorRuntime::start(harness.config.clone())
        .await
        .expect("端口释放后重新启动成功");
    runtime.shutdown().await.expect("优雅停机");
}
