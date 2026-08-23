//! collector 子进程编排：拉起 → 健康等待 → 静默排空 → 收尾。
//!
//! # 停机语义（跨平台如实声明）
//!
//! - UNIX：静默排空（`wal_inflight==0 && mqtt_inflight==0`）后发
//!   SIGTERM，等待有序停机（§93 排空链路）；
//! - Windows：无进程级 SIGTERM 等价物，静默排空后直接 `kill()`
//!   （SIGKILL 语义）。**丢失判定在两种平台上都成立于静默前提**：
//!   排空后 WAL 与 MQTT 在途均为零，强杀不产生未确认记录；报告的
//!   平台字段显式标注 Windows 的收尾方式。
//!
//! 故障场景的"非静默强杀"仅 crash-wal 使用（那正是被测行为本身）。

use std::path::Path;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::sampler;

/// 运行中的 collector 子进程。
pub struct CollectorProc {
    child: Child,
    pub pid: u32,
    pub rest_port: u16,
}

impl CollectorProc {
    /// 拉起子进程（`collector [CONFIG_PATH]` 契约，§101 Standalone）。
    pub async fn spawn(bin: &Path, config_path: &Path, rest_port: u16) -> std::io::Result<Self> {
        let child = Command::new(bin).arg(config_path).spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| std::io::Error::other("子进程尚未取得 PID"))?;
        Ok(Self {
            child,
            pid,
            rest_port,
        })
    }

    /// 轮询 `/api/v1/health` 至 200（就绪）。子进程提前退出立即报错，
    /// 不空等超时。
    pub async fn wait_healthy(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "健康检查超时（{timeout:?}）：REST 未就绪 port={}",
                    self.rest_port
                ));
            }
            if matches!(sampler::health_ok(self.rest_port).await, Ok(true)) {
                return Ok(());
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("collector 提前退出：{status}"));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// 当前 WAL/MQTT 在途计数（静默判定与场景断言用）；REST 暂不可达
    /// 视为未知（None）。
    pub async fn inflight(&self) -> Option<(i64, i64)> {
        let body = sampler::http_get_json(self.rest_port, "/api/v1/metrics")
            .await
            .ok()?;
        let sample = sampler::parse_sample(&body);
        Some((sample.wal_inflight, sample.mqtt_inflight))
    }

    /// 静默排空：等待 WAL 与 MQTT 在途归零（全部批次已 PUBACK 结算）。
    /// 超时返回 Err——调用方决定放弃或延长（不得跳过静默做丢失判定）。
    pub async fn quiesce(&self, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.inflight().await {
                Some((wal, mqtt)) if wal == 0 && mqtt == 0 => return Ok(()),
                None if tokio::time::Instant::now() >= deadline => {
                    return Err("静默排空超时：REST 不可达".to_owned());
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("静默排空超时：在途未归零（采集仍活跃或补传未完成）".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// 有序停机：UNIX 发 SIGTERM 等待退出（超时升级 SIGKILL）；Windows
    /// 直接 kill。返回退出码（被信号终止时为 None/异常码）。
    pub async fn stop(mut self) -> Result<Option<i32>, String> {
        #[cfg(unix)]
        {
            // std 无 POSIX 信号 API；经 `kill`(1) 发送 TERM（procps 必有）。
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &self.pid.to_string()])
                .status();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while tokio::time::Instant::now() < deadline {
                if self.child.try_wait().map_err(|e| e.to_string())?.is_some() {
                    return Ok(self.child.wait().await.ok().and_then(|s| s.code()));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // 有序停机卡死：升级强杀（WAL 保证已 fsync 记录不丢）。
        }
        let _ = self.child.kill().await;
        let status = self.child.wait().await.map_err(|e| e.to_string())?;
        Ok(status.code())
    }

    /// 强杀（crash-wal 场景的被测动作 / 收尾兜底）。
    pub async fn kill(&mut self) -> Result<(), String> {
        self.child.kill().await.map_err(|e| e.to_string())?;
        self.child.wait().await.map_err(|e| e.to_string())?;
        Ok(())
    }
}
