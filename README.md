# ForgeLink

ForgeLink 是面向工业设备的 Rust IoT 采集与边缘平台。

## 当前状态

当前完成 Rust workspace 项目骨架，核心模块和运行程序仍为占位实现。架构依据见：

- [Rust 工业 IoT 采集平台架构设计方案](./Rust工业IoT采集平台架构设计方案.md)
- [开发规范](./开发规范.md)

## 目录

```text
crates/       公共核心库
drivers/      协议驱动
profiles/     设备 Profile
apps/         Collector、Edge Server、Manager
```

## 验证

```bash
cargo check --workspace --all-targets --all-features
cargo test --workspace --all-features
cargo doc --workspace --no-deps --all-features
```

## 分支开发

禁止直接在 `main` 分支开发、提交或推送。所有变更必须在独立分支完成，通过 Pull Request 合并到 `main`。

## 许可证

本项目采用 Apache License 2.0，详见 [LICENSE](./LICENSE)。
