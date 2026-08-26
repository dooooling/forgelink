#!/usr/bin/env bash
# ForgeLink 发布打包（Linux x64 / ARM64）。
#
# 产出目录布局（§19/§20 部署形态；Runtime V2 §7：driver.json 为 Package
# 元数据唯一事实来源，发布时回填当前平台 artifact sha256）：
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

# Manifest v2（§7.1 规则 4）：发布打包必须校验当前平台 artifact 实际存在，
# 并计算 SHA-256 回填到 manifest 的当前平台条目（其余平台条目保持 null，
# Runtime 只验证当前平台 artifact）。python3 仅用于 JSON 安全读写。
MANIFEST_SRC="drivers/modbus/driver.json"
SCHEMA=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['schema_version'])" "$MANIFEST_SRC")
if [[ "$SCHEMA" != "2.0" ]]; then
    echo "$MANIFEST_SRC 不是 Manifest v2（schema_version=$SCHEMA）" >&2; exit 1
fi
ARTIFACT_PATH=$(python3 -c "import json,sys; m=json.load(open(sys.argv[1])); print(m['artifacts']['$PLATFORM']['path'])" "$MANIFEST_SRC")
if [[ ! -f "$ROOT/drivers/modbus/$ARTIFACT_PATH" ]]; then
    echo "manifest 声明的 artifact 不存在：$ROOT/drivers/modbus/$ARTIFACT_PATH" >&2; exit 1
fi
HASH=$(sha256sum "$ROOT/drivers/modbus/$ARTIFACT_PATH" | cut -d' ' -f1)
python3 - "$MANIFEST_SRC" "$ROOT/drivers/modbus/driver.json" "$PLATFORM" "$HASH" <<'PYEOF'
import json, sys
src, dst, platform, sha = sys.argv[1:5]
m = json.load(open(src))
m["artifacts"][platform]["sha256"] = sha
json.dump(m, open(dst, "w"), indent=2)
PYEOF

cp deploy/collector.example.yaml "$ROOT/config/collector.example.yaml"
cp deploy/profiles/inovance-md500.json "$ROOT/profiles/inovance-md500.json"
cp deploy/PLATFORM-CHECKLIST.md "$ROOT/PLATFORM-CHECKLIST.md"

echo "打包完成：$ROOT（modbus-tcp artifact sha256=$HASH）"
