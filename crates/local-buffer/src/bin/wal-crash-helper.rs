//! 崩溃恢复测试辅助进程（§103 kill -9 / 非正常重启验收）：
//!
//! 打开 `DB_PATH` 指定的 Local Buffer，写入 5 个 Batch（等待全部
//! `push` 落盘成功——push 成功 = SQLite WAL 已提交），输出 `READY`
//! 后**永久阻塞**（不调用 `shutdown`）。父测试进程随后强制终止本
//! 进程（Linux/macOS SIGKILL、Windows TerminateProcess，等价
//! kill -9），重新打开同一数据库验证 WAL 崩溃恢复。

use std::{env, io::Write, time::Duration};

use data_pipeline::ObservationBatch;
use local_buffer::{CapacityPolicy, LocalBuffer, LocalBufferConfig};

fn main() {
    let db_path = env::var("DB_PATH").expect("DB_PATH 环境变量必须设置");
    let config = LocalBufferConfig {
        db_path: db_path.into(),
        memory_records: 100,
        disk_max_bytes: 1 << 30,
        retention: Duration::from_secs(3600),
        capacity_policy: CapacityPolicy::Reject,
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async move {
        let buffer = LocalBuffer::open(config).await.expect("open");
        // 5 个 Batch；最后一个带 1 条 Observation（验证 Observation
        // ID / 时间跨崩溃保留）。用 JSON 反序列化构造（本辅助进程
        // 不需要引入 observation-model 依赖）。
        for i in 1..=5u64 {
            let json = if i == 5 {
                format!(
                    r#"{{"schema":"forgelink.telemetry.v1","message_id":"crash-m-{i}",
                    "site_id":"plant-a","device_id":"cnc-01","sequence":{i},
                    "sent_at_ns":{ts},"replayed":false,
                    "observations":[{{"observation_id":"obs-{i}","device_id":"cnc-01",
                    "path":"r1/cnc01.spindle_speed","value":{{"f64":{v}}},
                    "quality":{{"level":"good","reason":"none","protocol_code":null,"message":null}},
                    "source_timestamp_ns":{src},"ingest_timestamp_ns":{ts},"sequence":{i},
                    "metadata":{{"unit":"rpm"}}}}]}}"#,
                    ts = 1_780_000_000_000_000_000 + i,
                    src = 1_780_000_000_000_000_000 + i - 10,
                    v = 1200.0 + i as f64,
                )
            } else {
                format!(
                    r#"{{"schema":"forgelink.telemetry.v1","message_id":"crash-m-{i}",
                    "site_id":"plant-a","device_id":"cnc-01","sequence":{i},
                    "sent_at_ns":{ts},"replayed":false,"observations":[]}}"#,
                    ts = 1_780_000_000_000_000_000 + i,
                )
            };
            let batch: ObservationBatch = serde_json::from_str(&json).expect("batch JSON");
            buffer.push(batch).await.expect("push 必须成功");
        }
        // 通知父进程：全部写入已落盘。随后永久阻塞等待被强杀。
        println!("READY");
        std::io::stdout().flush().expect("flush");
        std::thread::park();
    });
}
