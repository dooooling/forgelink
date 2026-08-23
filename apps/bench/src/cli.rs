//! 命令行定义（clap derive）：场景子命令 + 公共参数。
//!
//! 时长一律用秒（`--*-secs`）而非人类可读时长字符串——避免额外解析
//! 依赖，脚本化调用也更明确。

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// `forgelink-bench` 命令行入口。
#[derive(Debug, Parser)]
#[command(
    name = "forgelink-bench",
    version,
    about = "ForgeLink 性能基准工具（§34.2 验收三件套：workload 编排 + 指标采样 + 验收报告）"
)]
pub struct Cli {
    #[command(subcommand)]
    pub scenario: Scenario,
}

/// 场景矩阵（§34.2）：吞吐 / 调度 / 故障×3 / 强杀恢复 / 长稳。
#[derive(Debug, Subcommand)]
pub enum Scenario {
    /// CI 冒烟：缩配 workload 短跑全链路，验证工具与装配不退化。
    Smoke {
        #[command(flatten)]
        common: CommonArgs,
        /// 冒烟运行时长（秒）。
        #[arg(long, default_value_t = 15)]
        duration_secs: u64,
    },
    /// 标准吞吐 workload：默认 100 设备 × 100 点 @500ms（§34.2）。
    Throughput {
        #[command(flatten)]
        common: CommonArgs,
        #[arg(long, default_value_t = 100)]
        devices: usize,
        #[arg(long, default_value_t = 100)]
        props_per_device: usize,
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
        #[arg(long, default_value_t = 600)]
        duration_secs: u64,
    },
    /// 独立调度测试：10 设备 × 100 点 @100ms（§34.2；完整验收 1800s）。
    Schedule {
        #[command(flatten)]
        common: CommonArgs,
        #[arg(long, default_value_t = 1800)]
        duration_secs: u64,
    },
    /// 故障注入：Modbus 断连窗口（drop_connection 开/关）。
    FaultNet {
        #[command(flatten)]
        common: CommonArgs,
        /// 故障窗口时长（秒）；窗口前后各有基线与恢复观察期。
        #[arg(long, default_value_t = 60)]
        fault_secs: u64,
    },
    /// 故障注入：设备超时 1%（timeout_rate (1,100) 窗口）。
    FaultTimeout {
        #[command(flatten)]
        common: CommonArgs,
        #[arg(long, default_value_t = 60)]
        fault_secs: u64,
    },
    /// 故障注入：broker 停机窗口（仅 mock 模式——真实 broker 的停机由
    /// 操作手册的人工步骤承担）。
    FaultBroker {
        #[command(flatten)]
        common: CommonArgs,
        #[arg(long, default_value_t = 30)]
        fault_secs: u64,
    },
    /// WAL 强杀恢复：运行中 SIGKILL 后同 work-dir 重启，验证 0 丢失。
    CrashWal {
        #[command(flatten)]
        common: CommonArgs,
        /// 强杀前的采集时长（秒）。
        #[arg(long, default_value_t = 20)]
        run_secs: u64,
    },
    /// 长稳 soak：默认 72h（§34.2），采样间隔自动放宽并周期落盘检查点。
    Soak {
        #[command(flatten)]
        common: CommonArgs,
        #[arg(long, default_value_t = 259_200)]
        duration_secs: u64,
    },
}

/// 公共参数（所有场景共享）。
#[derive(Debug, Args)]
pub struct CommonArgs {
    /// 北向 broker 模式：mock = 进程内 MockBroker（CI/开发）；
    /// real = 真实 MQTT broker（正式验收），地址经 --broker-url 提供。
    #[arg(long, value_enum, default_value_t = BrokerKind::Mock)]
    pub broker: BrokerKind,
    /// real 模式 broker 地址 `host:port`（如 `127.0.0.1:1883`）。
    #[arg(long, requires_if("real", "broker"))]
    pub broker_url: Option<String>,
    /// collector 可执行文件路径（release 构建：
    /// `target/release/collector` / `collector.exe`）。
    #[arg(long)]
    pub collector_bin: PathBuf,
    /// Driver cdylib 插件路径（缺省取 collector 同目录的
    /// `driver_modbus.dll` / `libdriver_modbus.so`）。
    #[arg(long)]
    pub plugin_path: Option<PathBuf>,
    /// collector REST 固定监听端口（bench 经此拉取健康与指标；
    /// 必须非 0 且未被占用）。
    #[arg(long, default_value_t = 18_080)]
    pub rest_port: u16,
    /// 指标/资源采样间隔（秒）。
    #[arg(long, default_value_t = 2)]
    pub sample_interval_secs: u64,
    /// 报告输出目录（JSON + Markdown + 采样序列）。
    #[arg(long, default_value = "bench-report")]
    pub output_dir: PathBuf,
    /// workload 工作目录（Profile/配置/WAL 落盘处；缺省系统临时目录下
    /// 自动命名）。crash-wal 场景的重启复用同一目录。
    #[arg(long)]
    pub work_dir: Option<PathBuf>,
    /// 运行结束后保留工作目录（默认清理）。
    #[arg(long, default_value_t = false)]
    pub keep: bool,
}

/// 北向 broker 形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BrokerKind {
    Mock,
    Real,
}

impl BrokerKind {
    /// 报告标注用字符串（`broker_mode` 字段）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Real => "real",
        }
    }
}

/// 解析后的公共运行参数（含派生默认值）。
#[derive(Debug, Clone)]
pub struct Resolved {
    pub broker: BrokerKind,
    pub broker_url: Option<String>,
    pub collector_bin: PathBuf,
    pub plugin_path: PathBuf,
    pub rest_port: u16,
    pub sample_interval_secs: u64,
    pub output_dir: PathBuf,
    pub work_dir: PathBuf,
    pub keep: bool,
}

impl CommonArgs {
    /// 展开默认值（plugin 缺省路径、work_dir 自动命名）。
    ///
    /// `run_id` 用于 work_dir 自动命名的唯一性（纳秒时间戳）。
    pub fn resolve(&self, run_id: u128) -> Result<Resolved, String> {
        let plugin_path = match &self.plugin_path {
            Some(p) => p.clone(),
            None => {
                // 缺省：collector 同目录下的 cdylib（打包布局与
                // target/<profile>/ 布局均满足）。
                let name = if cfg!(windows) {
                    "driver_modbus.dll"
                } else {
                    "libdriver_modbus.so"
                };
                self.collector_bin
                    .parent()
                    .ok_or_else(|| "--collector-bin 缺少父目录".to_owned())?
                    .join(name)
            }
        };
        if !self.collector_bin.exists() {
            return Err(format!(
                "collector 二进制不存在：{}",
                self.collector_bin.display()
            ));
        }
        let work_dir = match &self.work_dir {
            Some(d) => d.clone(),
            None => std::env::temp_dir().join(format!("forgelink-bench-{run_id}")),
        };
        let broker = self.broker;
        let broker_url = match (broker, &self.broker_url) {
            (BrokerKind::Real, Some(url)) => Some(url.clone()),
            (BrokerKind::Real, None) => {
                return Err("real 模式必须提供 --broker-url host:port".to_owned());
            }
            (BrokerKind::Mock, _) => None,
        };
        Ok(Resolved {
            broker,
            broker_url,
            collector_bin: self.collector_bin.clone(),
            plugin_path,
            rest_port: self.rest_port,
            sample_interval_secs: self.sample_interval_secs.max(1),
            output_dir: self.output_dir.clone(),
            work_dir,
            keep: self.keep,
        })
    }
}
