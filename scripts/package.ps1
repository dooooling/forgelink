# ForgeLink 发布打包（Windows x64）。
#
# 产出目录布局（§19/§20 部署形态）：
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
Copy-Item "drivers/modbus/driver.json" (Join-Path $root "drivers/modbus/driver.json")
Copy-Item "deploy/collector.example.yaml" (Join-Path $root "config/collector.example.yaml")
Copy-Item "deploy/profiles/inovance-md500.json" (Join-Path $root "profiles/inovance-md500.json")
Copy-Item "deploy/PLATFORM-CHECKLIST.md" (Join-Path $root "PLATFORM-CHECKLIST.md")

Write-Host "打包完成：$root"
