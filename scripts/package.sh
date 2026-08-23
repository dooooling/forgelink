#!/usr/bin/env bash
# ForgeLink 发布打包（Linux x64 / ARM64）。
#
# 产出目录布局（§19/§20 部署形态）：
#   dist/forgelink-{version}-{platform}/
#   ├── collector
#   ├── drivers/modbus/{libdriver_modbus.so, driver.json}
#   ├── config/collector.example.yaml
#   ├── profiles/inovance-md500.json
#   └── PLATFORM-CHECKLIST.md
#
# 用法：
#   ./scripts/package.sh                       # 本机 x64（target/release）
#   ./scripts/package.sh --target-dir target/aarch64-unknown-linux-gnu/release
#                                             # ARM64 交叉构建产物（见
#                                             # scripts/build-linux-arm64.sh）
set -euo pipefail

VERSION="0.1.0"
TARGET_DIR="target/release"
DIST_DIR="dist"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version) VERSION="$2"; shift 2 ;;
        --target-dir) TARGET_DIR="$2"; shift 2 ;;
        --dist-dir) DIST_DIR="$2"; shift 2 ;;
        *) echo "未知参数：$1" >&2; exit 1 ;;
    esac
done

case "$(uname -m)" in
    x86_64) PLATFORM="linux-x86_64" ;;
    aarch64) PLATFORM="linux-aarch64" ;;
    *) echo "不支持的主机架构：$(uname -m)" >&2; exit 1 ;;
esac

NAME="forgelink-${VERSION}-${PLATFORM}"
ROOT="${DIST_DIR}/${NAME}"

COLLECTOR_BIN="${TARGET_DIR}/collector"
PLUGIN_SO="${TARGET_DIR}/libdriver_modbus.so"
for f in "$COLLECTOR_BIN" "$PLUGIN_SO"; do
    if [[ ! -f "$f" ]]; then
        echo "构建产物缺失：$f（先执行 cargo build --release -p collector -p driver-modbus）" >&2
        exit 1
    fi
done

rm -rf "$ROOT"
mkdir -p "$ROOT/drivers/modbus" "$ROOT/config" "$ROOT/profiles"

cp "$COLLECTOR_BIN" "$ROOT/collector"
cp "$PLUGIN_SO" "$ROOT/drivers/modbus/libdriver_modbus.so"
cp drivers/modbus/driver.json "$ROOT/drivers/modbus/driver.json"
cp deploy/collector.example.yaml "$ROOT/config/collector.example.yaml"
cp deploy/profiles/inovance-md500.json "$ROOT/profiles/inovance-md500.json"
cp deploy/PLATFORM-CHECKLIST.md "$ROOT/PLATFORM-CHECKLIST.md"

echo "打包完成：$ROOT"
