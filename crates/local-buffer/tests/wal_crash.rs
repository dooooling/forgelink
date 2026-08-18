//! 真实进程强杀 WAL 崩溃恢复集成测试（§103 kill -9 / 非正常重启
//! 验收）。本文件由 `test-utils` feature 门控（见 Cargo.toml 的
//! `[[test]] required-features`）：默认 `cargo test --workspace`
//! 不编译运行本测试（依赖辅助二进制 `wal-crash-helper`，评审
//! P1-1），`--all-features` 下才执行。

use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, ChildStdout, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use local_buffer::{CapacityPolicy, LocalBuffer, LocalBufferConfig};

fn config(dir: &Path, mem: usize, disk: u64, policy: CapacityPolicy) -> LocalBufferConfig {
    LocalBufferConfig {
        db_path: dir.join("buffer.db"),
        memory_records: mem,
        disk_max_bytes: disk,
        retention: Duration::from_secs(3600),
        capacity_policy: policy,
    }
}

/// 就绪等待结果：`Ok` = 已输出 READY；`Err(msg)` = EOF / 读取失败。
type ReadyResult = Result<(), String>;

/// 等待辅助进程输出 `READY`（其所有 push 已落盘）。
///
/// 读取在**独立线程**中阻塞执行（同步 `read_line` 可能在辅助进程
/// 卡住时无限阻塞，评审 P1-1 超时无法生效）；主线程通过通道接收
/// 结果并 `recv_timeout` 等待——EOF（辅助进程初始化失败 / 提前
/// 崩溃）或超过 30s 立即失败，不会永久挂起。超时后测试 panic，
/// `KillOnDrop` 守卫强杀辅助进程，管道随之关闭、读取线程退出，
/// 无线程泄漏。
fn wait_until_ready(stdout: ChildStdout) {
    let (tx, rx): (mpsc::Sender<ReadyResult>, mpsc::Receiver<ReadyResult>) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(Err("EOF（辅助进程提前退出）".into()));
                    return;
                }
                Ok(_) if line.trim() == "READY" => {
                    let _ = tx.send(Ok(()));
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(format!("读取失败: {e}")));
                    return;
                }
            }
        }
    });
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Ok(())) => {}
        Ok(Err(msg)) => panic!("辅助进程未就绪: {msg}（stderr 已透传）"),
        Err(_) => panic!("等待辅助进程 READY 超时（30s）：辅助进程卡住未输出"),
    }
}

/// 子进程 Drop 守卫（评审 P1-2）：测试正常路径显式 `kill + wait`
/// 回收；若读取 stdout、断言或测试线程 panic，辅助进程会永久
/// `park()` 遗留——守卫在 drop 时兜底强杀并回收（已回收则跳过，
/// 避免重复 `wait`）。
struct KillOnDrop(Child);

impl std::ops::Deref for KillOnDrop {
    type Target = Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for KillOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// 打开 Local Buffer；失败时短暂重试（Windows 上被强杀进程的文件
/// 句柄释放可能有延迟，每次失败会关闭失败连接的句柄）。
async fn open_with_retry(
    cfg: LocalBufferConfig,
    interval: Duration,
    attempts: usize,
) -> LocalBuffer {
    let mut last_err = None;
    for _ in 0..attempts {
        match LocalBuffer::open(cfg.clone()).await {
            Ok(buffer) => return buffer,
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(interval).await;
    }
    panic!("重试 {attempts} 次后仍无法打开数据库: {last_err:?}");
}

/// 真实进程强杀后的 WAL 崩溃恢复（§103 kill -9 / 非正常重启验收）：
/// 辅助进程（`src/bin/wal-crash-helper.rs`）写入 5 条并全部落盘后
/// 输出 READY 并阻塞；本进程用 `Child::kill()` 强制终止（Linux/
/// macOS SIGKILL、Windows TerminateProcess，等价 kill -9，不经过
/// 任何优雅退出路径），重新打开同一数据库验证：
/// - 全部记录恢复（WAL 崩溃安全，0 丢失）；
/// - `local_seq` 顺序不变（1..5）、`message_id` 不重复；
/// - `replayed == true`（会话内恢复水位标记补传）；
/// - 原始 `sent_at_ns` / Observation ID / 值保留（深拷贝恢复）；
/// - 全部 ACK（唯一删除路径）后重新打开，数据库为空。
#[tokio::test(flavor = "multi_thread")]
async fn wal_crash_kill_recovers_all_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("crash.db");

    let mut child = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_wal-crash-helper"))
            .env("DB_PATH", db_path.to_str().expect("路径"))
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("启动辅助进程"),
    );
    wait_until_ready(child.stdout.take().expect("stdout"));

    // 强制终止（等价 kill -9）：不调用 shutdown，直接杀进程。
    // 若此前任何步骤 panic，KillOnDrop 守卫会在 drop 时兜底回收。
    child.kill().expect("kill");
    child.wait().expect("wait");

    // 重新打开同一数据库（Windows 句柄释放可能延迟，带重试）。
    let cfg = LocalBufferConfig {
        db_path: db_path.clone(),
        ..config(dir.path(), 100, 1 << 30, CapacityPolicy::Reject)
    };
    let buffer = open_with_retry(cfg.clone(), Duration::from_millis(100), 20).await;

    // 全部恢复：顺序不变、不重复、补传标记、原始内容保留。
    let mut seqs = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for i in 1..=5u64 {
        let stored = buffer
            .next()
            .await
            .expect("next")
            .expect("记录必须全部恢复");
        seqs.push(stored.local_seq);
        assert!(
            ids.insert(stored.batch.message_id.clone()),
            "message_id 不得重复"
        );
        assert_eq!(stored.batch.message_id, format!("crash-m-{i}"));
        assert!(stored.batch.replayed, "崩溃恢复的记录必须标记补传");
        assert_eq!(
            stored.batch.sent_at_ns,
            1_780_000_000_000_000_000 + i,
            "原始时间必须保留"
        );
        assert_eq!(stored.batch.sequence, i, "原始 Batch 序号必须保留");
        if i == 5 {
            assert_eq!(stored.batch.observations.len(), 1);
            let obs = &stored.batch.observations[0];
            assert_eq!(obs.observation_id, "obs-5", "Observation ID 必须保留");
            assert_eq!(
                obs.source_timestamp_ns,
                Some(1_780_000_000_000_000_000_i64 + i as i64 - 10),
                "Observation 源时间必须保留"
            );
        }
    }
    assert_eq!(seqs, vec![1, 2, 3, 4, 5], "local_seq 顺序必须不变");
    assert!(buffer.next().await.expect("next").is_none(), "无多余记录");

    // 全部 ACK（唯一删除路径）后重新打开：数据库为空。
    for seq in seqs {
        buffer.ack(seq).await.expect("ack");
    }
    buffer.shutdown().await.expect("shutdown");
    let reopened = LocalBuffer::open(cfg).await.expect("重新打开");
    assert!(
        reopened.next().await.expect("next").is_none(),
        "ACK 后数据库必须为空"
    );
    reopened.shutdown().await.expect("shutdown");
}
