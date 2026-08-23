# ForgeLink 平台验收记录（§34.5）

> 每平台一份。七项检查的操作与通过标准见包内 `PLATFORM-CHECKLIST.md`；
> 性能数值验收记录见 `docs/benchmark.md` 第 4 节产出（仅 x64 基线）。

## 平台：Windows x64

- 验收日期：
- 硬件（CPU/核心/内存/磁盘）：
- OS 版本：
- 构建版本 / commit：（artifact 名 + 报告头部 commit 字段）
- 七项检查结果：1☐ 2☐ 3☐ 4☐ 5☐ 6☐ 7☐
- 偏差与备注：

## 平台：Linux x64

- 验收日期：
- 硬件（CPU/核心/内存/磁盘）：
- 内核版本：
- 构建版本 / commit：
- 七项检查结果：1☐ 2☐ 3☐ 4☐ 5☐ 6☐ 7☐
- 性能基准（§34.2）：throughput ☐ schedule ☐ fault-net ☐ fault-timeout ☐
  fault-broker(mock 自动化 + real 人工窗口) ☐ crash-wal ☐ soak(72h) ☐
- 基准报告归档位置：
- 偏差与备注：

## 平台：Linux ARM64

- 验收日期：
- 板卡型号 / 硬件：
- 内核版本：
- 构建方式：cross ☐ / 板载原生 ☐；构建版本 / commit：
- 七项检查结果：1☐ 2☐ 3☐ 4☐ 5☐ 6☐ 7☐
- 性能数值：不适用（§34.5 功能契约一致即可，见 docs/deploy-arm64.md §4）
- 偏差与备注：

---

全部平台通过后，MVP 三平台部署验收关账（§34.7 欠账清零项之一）。
