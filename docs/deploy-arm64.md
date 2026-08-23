# ForgeLink Linux ARM64 部署与验收（§34.5）

> 定位：Linux ARM64 暂不进 CI（用户决策）——交叉构建脚本 + 本文档支持
> 在真实板子上完成功能契约验收。性能数值验收仅在 x64 基线执行
> （§34.2：ARM64 功能契约必须一致，峰值性能允许不同）。

## 1. 交叉构建

```bash
cargo install cross          # 需要 Docker
./scripts/build-linux-arm64.sh
./scripts/package.sh --target-dir target/aarch64-unknown-linux-gnu/release \
  --dist-dir dist
```

无 Docker 时用板载原生构建备用路径（见 `scripts/build-linux-arm64.sh`
头注释），再在板子上直接打包。

## 2. 传输与部署

```bash
scp -r dist/forgelink-*-linux-aarch64/ board:/opt/forgelink/
ssh board 'cd /opt/forgelink && ./collector config/collector.example.yaml'
```

配置要点：

- `config/collector.example.yaml` 中 driver.plugin 路径改为
  `drivers/modbus/libdriver_modbus.so`（打包布局已按此放置）。
- 设备侧常见形态（§91）：机床边缘盒子 / ARM64 工控机 / Docker 容器。

### 可选 systemd 单元

```ini
# /etc/systemd/system/forgelink-collector.service
[Unit]
Description=ForgeLink Collector
After=network-online.target

[Service]
WorkingDirectory=/opt/forgelink
ExecStart=/opt/forgelink/collector /opt/forgelink/config/collector.example.yaml
Restart=on-failure
# 有序停机预算：§93 排空链路（采集→管道→WAL→MQTT DISCONNECT）
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

## 3. 功能契约验收（§34.5 七项）

逐项执行包内 `PLATFORM-CHECKLIST.md`，含 WAL 强杀恢复的板上操作：
运行中 `kill -9 <pid>` 后重启同配置，验证补传批次 `replayed=true` 且
0 丢失。自动化等价物（x64 CI 已覆盖）：`resilience` 与 `wal_crash`
测试套；板上为部署形态冒烟复验。

## 4. 性能契约差异声明

ARM64 上不执行 §34.2 数值验收（吞吐/延迟/RSS 目标绑定 x64 基线硬件）；
如需参考性数据可跑 `forgelink-bench throughput --broker mock ...`，
报告仅供观察，不作为验收依据。
