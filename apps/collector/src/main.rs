//! collector：设备侧轻量采集程序（占位）。
//!
//! Runtime Role = collector（§92、§93）：只采集、缓存、上传。
//! 通过 Cargo feature 禁用控制链路（§98），运行时设置 `read_only`（§106）。
fn main() {
    // TODO: 组装 edge-core 组件，加载配置并启动采集
}
