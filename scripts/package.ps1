# ForgeLink 发布打包（Windows x64）。
#
# 产出目录布局（§19/§20 部署形态；Runtime V2 §7：driver.json 为 Package
# 元数据唯一事实来源，发布时回填当前平台 artifact sha256）：
#   dist/forgelink-{version}-windows-x86_64/
#   ├── collector.exe
#   ├── drivers/modbus/{driver_modbus.dll, driver.json}
#   ├── config/collector.example.yaml
#   ├── profiles/inovance-md500.json
#   └── PLATFORM-CHECKLIST.md
#
# 用法：pwsh scripts/package.ps1 [-Version 0.1.0] [-TargetDir target/release] [-DistDir dist]

param(
    [string]$Version = "0.1.0",
    [string]$TargetDir = "target/release",
    [string]$DistDir = "dist"
)

$ErrorActionPreference = "Stop"

$platform = "windows-x86_64"
$name = "forgelink-$Version-$platform"
$root = Join-Path $DistDir $name

$collectorBin = Join-Path $TargetDir "collector.exe"
$pluginDll = Join-Path $TargetDir "driver_modbus.dll"
foreach ($f in @($collectorBin, $pluginDll)) {
    if (-not (Test-Path $f)) {
        throw "构建产物缺失：$f（先执行 cargo build --release -p collector -p driver-modbus）"
    }
}

if (Test-Path $root) { Remove-Item -Recurse -Force $root }
New-Item -ItemType Directory -Force -Path (Join-Path $root "drivers/modbus") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $root "config") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $root "profiles") | Out-Null

Copy-Item $collectorBin $root
Copy-Item $pluginDll (Join-Path $root "drivers/modbus/driver_modbus.dll")

# Manifest v2（§7.1 规则 4）：发布打包必须校验当前平台 artifact 实际存在，
# 并计算 SHA-256 回填到 manifest 的当前平台条目（其余平台条目保持 null，
# Runtime 只验证当前平台 artifact）。
$manifest = Get-Content "drivers/modbus/driver.json" -Raw | ConvertFrom-Json
if ($manifest.schema_version -ne "2.0") { throw "drivers/modbus/driver.json 不是 Manifest v2（schema_version=$($manifest.schema_version)）" }
$artifactSpec = $manifest.artifacts.$platform
if (-not $artifactSpec) { throw "manifest 未声明平台 $platform 的 artifact" }
$distArtifact = Join-Path $root "drivers/modbus/$($artifactSpec.path)"
if (-not (Test-Path $distArtifact)) { throw "manifest 声明的 artifact 不存在：$distArtifact" }
$hash = (Get-FileHash -Algorithm SHA256 $distArtifact).Hash.ToLower()
$artifactSpec.sha256 = $hash
$manifest | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $root "drivers/modbus/driver.json") -Encoding utf8

Copy-Item "deploy/collector.example.yaml" (Join-Path $root "config/collector.example.yaml")
Copy-Item "deploy/profiles/inovance-md500.json" (Join-Path $root "profiles/inovance-md500.json")
Copy-Item "deploy/PLATFORM-CHECKLIST.md" (Join-Path $root "PLATFORM-CHECKLIST.md")

Write-Host "打包完成：$root（modbus-tcp artifact sha256=$hash）"
