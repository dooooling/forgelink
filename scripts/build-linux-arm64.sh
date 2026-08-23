#!/usr/bin/env bash
# ForgeLink Linux ARM64 交叉构建（§34.5 三平台；暂不进 CI——用户决策，
# 由本脚本 + docs/deploy-arm64.md 支持在真实板子上验收）。
#
# 主路径：cross（容器内自带 aarch64 C 工具链，rusqlite bundled 需要 cc）。
#   cargo install cross && docker info   # 前置
#   ./scripts/build-linux-arm64.sh       # 产出 target/aarch64-unknown-linux-gnu/release/
#
# 备用路径（无 Docker 的板载原生构建，4G 内存板偏紧）：
#   在板子上安装 rustup 后直接
#     cargo build --release -p collector -p driver-modbus
#
# 构建完成后用 scripts/package.sh --target-dir <同上 release 目录> 打包。

set -euo pipefail

TARGET="aarch64-unknown-linux-gnu"

command -v cross >/dev/null || {
    echo "未找到 cross。安装：cargo install cross（需要 Docker）" >&2
    echo "或采用板载原生构建（见脚本头注释）。" >&2
    exit 1
}

cross build --release --target "$TARGET" -p collector -p driver-modbus

echo "构建完成：target/$TARGET/release/"
echo "打包：./scripts/package.sh --target-dir target/$TARGET/release"
