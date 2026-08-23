# ForgeLink 性能基准操作手册（§34.2 验收三件套·手册）

> 工具：`forgelink-bench`（`apps/bench`）；本手册是正式验收的操作依据。
> 性能验收**绑定目标硬件**（§34.2 Reference Benchmark Profile）——CI 中的
> smoke 场景只防工具与链路退化，不构成验收结论。

## 1. 硬件与环境要求

| 项 | x64 主性能基线 | ARM64 | Windows x64 |
|---|---|---|---|
| CPU | 4 核持续 ≥2.5 GHz | 4 核持续 ≥2.0 GHz | 同 x64 基线等级 |
| 内存 | 8 GiB | 4 GiB | 8 GiB |
| 磁盘 | SSD/NVMe | eMMC/SSD | SSD |
| 网卡 | 1 GbE | 1 GbE | 1 GbE |
| 用途 | **正式性能验收** | 功能契约复验 | 功能/稳定性复验（不采性能指标） |

- 构建：`cargo build --release`（workspace 已配 thin LTO）。
- 正式验收北向 broker 用**真实 MQTT broker**（如 mosquitto）：
  `mosquitto -p 1883`（本机回环即可，§34.2 "local MQTT broker"）。
- 报告自动记录 CPU 型号/核心数/内存/OS/Rust 版本/构建 commit；**磁盘
  型号为人工填写字段**（报告模板留空位）。

## 2. 安装

解包发布产物（CI artifact 或 `scripts/package.*` 产出）：

```text
forgelink-{version}-{platform}/
├── collector(.exe)
├── drivers/modbus/{cdylib, driver.json}
├── config/collector.example.yaml
├── profiles/inovance-md500.json
└── PLATFORM-CHECKLIST.md
```

`forgelink-bench` 二进制在仓库 `target/release/` 下（不进部署包——它是
验收工具，不是被部署组件）。

## 3. 命令参考

```text
forgelink-bench <SCENARIO> [OPTIONS]
```

公共参数（所有场景）：

| 参数 | 说明 |
|---|---|
| `--broker mock\|real` | 北向 broker：mock = 进程内 MockBroker（开发/冒烟）；real = 真实 broker（正式验收必用） |
| `--broker-url host:port` | real 模式 broker 地址 |
| `--collector-bin PATH` | release 版 collector 二进制 |
| `--plugin-path PATH` | cdylib 路径（缺省取 collector 同目录） |
| `--rest-port N` | collector REST 固定端口（默认 18080，须空闲） |
| `--sample-interval-secs N` | 采样间隔（默认 2s；soak 自动放宽 ≥30s） |
| `--output-dir DIR` | 报告输出目录（默认 `bench-report/<场景>/`） |
| `--work-dir DIR` | workload 工作目录（缺省临时目录自动命名） |
| `--keep` | 保留工作目录供人工复查 |

场景矩阵（§34.2）：

| 场景 | workload | 默认时长 | 对应验收项 |
|---|---|---|---|
| `smoke` | 2×4@50ms | 15s | 工具/链路防退化（CI） |
| `throughput` | 100×100@500ms | 600s | ≥10k 点、≥20k obs/s、≥100 设备、单批≥1000 |
| `schedule` | 10×100@100ms | 1800s | 100ms 最小调度周期、p99 调度延迟 ≤25ms |
| `fault-net` | 20×100@200ms | 基线30s+故障60s+恢复90s | 断网缓存恢复 0 丢失 |
| `fault-timeout` | 同上（1% 超时注入） | 同上 | 设备超时下的交付语义 |
| `fault-broker` | 同上（broker 停机窗口） | 同上 | broker 断网 30min 恢复（mock 模式；real 模式由人工步骤承担） |
| `crash-wal` | 10×100@100ms | 运行20s+重启恢复 | WAL 强杀恢复 0 丢失 |
| `soak` | 100×100@500ms | 259200s（72h） | RSS 漂移 ≤10%、长稳 |

时长/规模参数均可覆盖（如 `throughput --duration-secs 1800`）。

## 4. 正式验收 step-by-step（x64 基线）

```bash
# 0) 前置：release 构建 + 本机 mosquitto
cargo build --release -p collector -p driver-modbus -p bench
mosquitto -p 1883 &

# 1) 标准吞吐 workload
./target/release/forgelink-bench throughput --broker real --broker-url 127.0.0.1:1883 \
  --collector-bin target/release/collector --duration-secs 1800

# 2) 独立调度测试（§34.2 要求 30min）
./target/release/forgelink-bench schedule --broker real --broker-url 127.0.0.1:1883 \
  --collector-bin target/release/collector

# 3) 故障场景（30min 断网为正式口径：--fault-secs 1800）
./target/release/forgelink-bench fault-net     --broker real --broker-url 127.0.0.1:1883 --collector-bin target/release/collector --fault-secs 1800
./target/release/forgelink-bench fault-timeout --broker real --broker-url 127.0.0.1:1883 --collector-bin target/release/collector
#    broker 停机窗口的自动化仅 mock 模式（--broker mock fault-broker）；
#    real 模式下按《PLATFORM-CHECKLIST.md》第 6 项人工执行：运行期间
#    停止/重启 mosquitto，经消费端去重核对 0 丢失。
#    （工具不操控外部进程——这是有意的安全边界。）

# 4) WAL 强杀恢复
./target/release/forgelink-bench crash-wal --broker real --broker-url 127.0.0.1:1883 \
  --collector-bin target/release/collector

# 5) 72h soak（后台执行，断点续测见下节）
nohup ./target/release/forgelink-bench soak --broker real --broker-url 127.0.0.1:1883 \
  --collector-bin target/release/collector > soak.log 2>&1 &
```

每步产出 `bench-report/<场景>/bench-report.{json,md}` + `samples.jsonl`。
全部场景判定列无 FAIL 即 §34.2 验收通过；连同环境信息归档。

> Windows 复验：仅跑 `PLATFORM-CHECKLIST.md` 七项 + smoke；报告会标注
> 「性能指标本平台不采集」（§34.2 Windows 仅功能/稳定性复验）。

## 5. 72h soak 断点续测

- 采样序列逐条落盘 `bench-report/soak/samples.jsonl`——soak 中途崩溃/
  中断后，已测数据不丢；重新执行 soak 即重跑（当前不自动续接，中断
  前的 samples.jsonl 保留作证据链）。
- 报告的 RSS 漂移以预热基线（运行 1h 后）为基准：`漂移 = (运行期最大
  RSS − 基线) / 基线`，≤10% 判 PASS（§34.2 排除有界缓存配置变化——
  缓存上限在启动期固定）。

## 6. 报告字段解读

- **稳态吞吐**：剔除预热期（默认 60s）后的 observations/s 均值。
- **p50/p95/p99**：直方图累计占比法取**桶上界**——PASS 判定精确（桶
  边界恰为 25ms 阈值）；FAIL 时显示为区间（如 `∈ (25, 50] ms`），不
  做插值伪装精度。
- **丢失候选**：按设备水位线+间隙集合判定（O(设备数) 内存）；重复为
  QoS1 at-least-once 的合法重投，如实上报不计 FAIL。
- **crash-wal 丢失判定**：`唯一消息总数 ≥ 强杀前已交付 + WAL 在途`
  （两集合不相交，见 `apps/bench/src/scenario/crash.rs` 模块注释）。
- **broker_mode**：`mock|real`——正式验收报告必须为 `real`。
- **perf_metrics_collected=false**：Windows 复验平台，RSS/CPU 字段为空
  属预期，非缺陷。

## 7. 已知边界

- 设备数上限 250（Modbus unit 号为 u8；§34.2 workload 100 台）。
- `fault-broker` 仅 mock 模式可自动化；real 模式按第 4 节人工窗口执行。
- 采样经 REST（管理接口，本机回环）；REST 不可达轮次跳过不中断。
