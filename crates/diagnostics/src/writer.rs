//! 非阻塞 writer：事件先入有界通道，由专用线程刷到 stdout。
//!
//! 目的（`开发规范.md` §5）：日志写入不得阻塞异步采集路径。通道满时
//! 丢弃新行——日志丢帧优先于阻塞采集。

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use tracing_subscriber::fmt::MakeWriter;

/// 通道容量（行数）。满时丢弃新行。
const CHANNEL_LINES: usize = 8_192;

/// 非阻塞 stdout writer。
///
/// [`Write`] 只做有界投递（`try_send`），不执行任何阻塞 I/O；专用刷写
/// 线程从通道读取并写入 stdout。writer 句柄可 Clone（`SyncSender` 不可
/// Clone，内部经 `Arc` 共享）。
#[derive(Clone, Debug)]
pub(crate) struct NonBlockingWriter {
    inner: Arc<SyncSender<Box<[u8]>>>,
}

impl NonBlockingWriter {
    /// 启动专用刷写线程并返回 writer 句柄。
    ///
    /// # Errors
    ///
    /// 线程启动失败时返回 [`io::Error`]。
    pub(crate) fn spawn() -> Result<Self, io::Error> {
        let (tx, rx) = sync_channel::<Box<[u8]>>(CHANNEL_LINES);
        thread::Builder::new()
            .name("forgelink-log-writer".to_owned())
            .spawn(move || drain_to_stdout(rx))?;
        Ok(Self {
            inner: Arc::new(tx),
        })
    }

    /// 测试用：直接包装既有通道，不启动刷写线程。
    #[cfg(test)]
    pub(crate) fn from_channel(inner: Arc<SyncSender<Box<[u8]>>>) -> Self {
        Self { inner }
    }
}

/// 刷写线程主体：逐行写入 stdout（每个事件一行，fmt Layer 保证行完整）。
///
/// stdout 不可写（如管道断开）时退出线程，后续日志静默丢弃。
/// 注意：`stdout().lock()` 必须在单行写入后立即释放——锁本身是进程级
/// 全局锁，若在 `recv()` 阻塞期间一直持有，其他线程（含测试框架）
/// 的 stdout 输出会被永久卡死。
fn drain_to_stdout(rx: Receiver<Box<[u8]>>) {
    while let Ok(line) = rx.recv() {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        if out.write_all(&line).is_err() {
            break;
        }
    }
}

impl io::Write for NonBlockingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = self.inner.try_send(buf.to_vec().into_boxed_slice());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for NonBlockingWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
