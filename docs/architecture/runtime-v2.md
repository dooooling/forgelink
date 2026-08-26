# ForgeLink Runtime V2 RFC

> 状态：**Active**
> 决议依据：`docs/FORGLINK_RUNTIME_V2_IMPLEMENTATION_PLAN.md`（下称「方案」）
> 本 RFC 是方案 §37.1（Phase 0）要求的落地文档，记录已批准决议、术语约定与
> Target / Transitional 生效点，并维护实施进度对照表。

## 1. 已批准决议记录

以下两项决议已于 **2026-08-25** 由仓库所有者正式批准，全文相应条目按此执行。

### D1 — 生产 Driver 隔离政策（APPROVED）

- Phase 8 cutover 之后，**所有生产 Driver 必须经 Driver Host**——即使协议实现
  是 ForgeLink 自研 Rust。Core 与任何协议实现不共享进程故障域。
- Pure Rust 自研 Driver 默认 `per_driver` 隔离。
- 高风险 Vendor SDK 可/应提升到 `per_device`；`BlockingUninterruptible`
  必须 `per_device`（方案 §22.3）。
- in-process 直接加载仅限开发与测试环境；生产配置不得绕过 Host。
- 生效时点：Phase 8 cutover 后强制；Host 路径的实现要求自 Phase 7 起有效。
- 全文见方案 §5.2。

### D2 — 协议扩展临时冻结（APPROVED）

- 自 2026-08-25 起**即时生效**，冻结范围：
  `Omron FINS`、`FANUC FOCAS`、`Beckhoff ADS`、其他新增协议 Driver、
  ABI v1 新能力。
- 不受限：现有协议 bug fix、安全修复、协议正确性修复、测试基础设施、
  Runtime V2 全部工作。
- 解除条件：**Milestone B（Fault Isolation）验收通过后解除**；解除时须在
  路线图文档中显式记录解除决议与日期。
- 全文见方案 §47；路线图同步标注见仓库根目录架构设计文档 §34.6 与 AGENTS.md。

后续任何规范级别（Normative/Target/Transitional）变更仍须显式记录，
不允许实现过程中悄悄变更。

## 2. 术语约定

### 2.1 Edge Core 与 Runtime Role 的关系

`Edge Core` **不是新的 Runtime Role**。它指 Collector / Edge 可复用的内部
核心层（driver-runtime、DeviceActor、Driver Registry 等模块的统称）。
Runtime Role 仍沿用既有三分类：

```text
Collector / Edge / Manager
```

Runtime V2 的第一实施对象是 **Collector Role**。

### 2.2 关键术语

以下术语在 Runtime V2 语境下的唯一含义以方案 §1.2 为准，此处摘录高频项：

| 术语 | 含义 |
|---|---|
| Driver | 协议/厂商能力实现；可以是 ForgeLink Rust 协议实现，也可以包装 Vendor SDK |
| Native Driver | 通过 C ABI 动态加载的 Driver binary；不等同于"所有 Rust Driver" |
| Driver Package | `driver.json` + 当前平台 artifact + 发布元数据 |
| Driver Host | 承载一个或多个 Driver Session 的独立 OS 进程 |
| Host Group | 某 Driver 的 Host 拓扑管理对象，可实现 `shared/per_driver/per_device` |
| DeviceActor | Core/driver-runtime 中每设备唯一的逻辑执行仲裁入口 |
| HostSessionWorker | Driver Host 内某设备 Session 的执行单元；不是第二个 Core DeviceActor |
| Session | 一台逻辑设备与 Driver 的连接/协议上下文 |
| Control queue | control-engine 已有的业务/安全队列；不由 DeviceActor 重做 |
| Runtime circuit breaker | driver-runtime 的连接恢复节流；不同于 control-engine 的 Indeterminate safety cooldown |

## 3. Target / Transitional 生效点

规范级别定义沿用方案 §1.1（Normative / Target / Transitional / Proposed）。
本 RFC 记录关键约束的生效时点：

### Target → Normative 生效点

| 约束 | 生效时点 | 方案依据 |
|---|---|---|
| Collector 默认路径不直接 `dlopen()` Native Driver | Phase 8 Core IPC cutover 完成后强制 | §5.1、§37.9 |
| 所有生产 Driver 必须经 Driver Host（D1） | Phase 8 cutover 后强制 | §5.2 |
| Core `dlopen` 禁令 | Phase 8 cutover 后生效 | §46 |

### Transitional 条目及删除点

| 条目 | 允许窗口 | 删除点 |
|---|---|---|
| legacy direct ABI v1 runtime | Phase 2~7 迁移期 | Phase 8 后仅作为默认关闭的 `legacy-native-runtime` feature 保留一个兼容版本，到期删除（§37.9、§42） |
| `driver:` 单驱动配置格式 | 过渡版本 | 启动时转换并打印 `COLLECTOR_CONFIG_DRIVER_DEPRECATED` 日志，下一 major 删除（§41.1） |
| driver-sdk 兼容 facade（re-export 外壳） | 四 Driver 迁移期 | Milestone E 完成兼容矩阵后退出（§51.5） |

Phase 2～7 仅允许 `legacy-native-runtime` 迁移路径直接加载 ABI v1；
当前无未决 `Proposed` 事项。

## 4. 实施进度对照表

顺序硬约束（方案 §46）：S7 Protocol Reset 在 ABI v2 之前；PR 拆分与门禁
按方案 §38 / §39 执行；每个 PR 的完成标准为方案 §48 DoD 九项。

| PR | 内容（方案 §38） | 对应 Phase | 状态 |
|---|---|---|---|
| PR-0 | clippy 存量清零（§39 门禁前置条件） | — | ✅ #32 已合并（2026-08-25） |
| PR-1 | Runtime V2 RFC / terminology（本文件） | 0 | ✅ #33 已合并（2026-08-25） |
| PR-2 | `driver-contract` + `driver-abi` split + `driver-sdk` facade | 1 | ✅ #34 已合并（2026-08-25） |
| PR-3 | `driver-package` + Manifest v2 + 四包迁移 | 1 | 🔄 下一步 |
| PR-4 | Multi Driver Registry | 2 | ⬜ 未开始 |
| PR-5 | Startup Preflight | 3 | ⬜ 未开始 |
| PR-6 | DeviceActor / SessionSupervisor + device-manager adapter | 4 | ⬜ 未开始 |
| PR-7 | Poll → DeviceActor + Quality regression | 5 | ⬜ 未开始 |
| PR-8 | Control → DeviceActor + Indeterminate regression | 5 | ⬜ 未开始 |
| PR-9 | Driver Host Protocol + IPC security | 6 | ⬜ 未开始 |
| PR-10 | `forgelink-driver-host` + ABI v1 Host Adapter | 7 | ⬜ 未开始 |
| PR-11 | Core IPC cutover | 8 | ⬜ 未开始 |
| PR-12 | Host crash / hang recovery + in-flight settlement | 9 | ⬜ 未开始 |
| PR-13 | S7 Protocol Reset | 10 | ⬜ 未开始 |
| PR-14 | ABI v2 Base Descriptor / query_interface / ASYNC_EXECUTION_V1 | 11 | ⬜ 未开始 |
| PR-15 | Modbus ABI v2 | 12 | ⬜ 未开始 |
| PR-16 | S7 ABI v2 integration | 12 | ⬜ 未开始 |
| PR-17 | MC migration | 12 | ⬜ 未开始 |
| PR-18 | EtherNet/IP migration | 12 | ⬜ 未开始 |

里程碑停点验收（方案 §51）：A = PR-8 后（Core Runtime Stabilized）、
B = PR-12 后（Fault Isolation，**D2 解冻触发条件**）、C = PR-13 后
（Protocol Correctness）、D = PR-14 后（Driver Contract V2）、
E = PR-18 后（Driver Migration / Validation）。

## 5. 外部依赖登记

| 依赖 | 影响范围 | 状态 |
|---|---|---|
| S7 真机 / PLCSIM Advanced / Snap7 环境 | Milestone C 验收（§45 十四项清单中的硬件项） | 长 lead-time 项，Phase 10 动工前须确认到位 |
