//! 测试共享工具（仅在测试构建中使用）。
//!
//! `tracing` 的 callsite interest 是**进程级全局缓存**：每个事件宏调用点
//! 只注册一次，注册时按“当前所有已注册 dispatcher”计算兴趣值并永久缓存。
//! 若某调用点在“未设置任何全局默认 subscriber”时被首次触发（并行测试中
//! 非常常见），tracing-core 会按当时线程的默认 dispatcher（无线程本地值时
//! 即 `NoSubscriber`）算出 `Interest::never()` 并缓存，此后该调用点的事件
//! 即使在线程本地 `with_default` 的 subscriber 下也会被永久丢弃。
//!
//! 因此，所有可能触发 `tracing` 事件的测试都必须在最开头调用
//! [`init_global_subscriber`]，保证任意调用点都先在一个真实的全局
//! subscriber 下完成注册，避免“谁先注册谁决定全进程行为”的竞态。

/// 安装测试进程级全局默认 subscriber（幂等，仅首次生效）。
///
/// 使用 `TRACE` 级别只负责让所有调用点正常注册（`Interest::sometimes()`），
/// 事件是否输出仍由各测试自身的线程本地 subscriber 决定。
pub(crate) fn init_global_subscriber() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_ansi(false)
        .try_init();
}
