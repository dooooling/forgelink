//! 非阻塞 writer：事件先入有界通道，由专用线程刷到 stdout。
//!
//! 目的（`开发规范.md` §5）：日志写入不得阻塞异步采集路径。通道满时
//! 丢弃新行——日志丢帧优先于阻塞采集。写入路径无锁：`try_send` 为
//! 无锁投递，关闭标志为原子读，不引入互斥锁。
//!
//! 生命周期：刷写线程与进程生命周期脱离，进程正常退出时通道中未刷出
//! 的事件会丢失。进程入口在退出前必须调用 [`shutdown`](Self::shutdown)
//! 置关闭标志，刷写线程排空已入队事件后退出（见 `init::shutdown_logging`）。

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing_subscriber::fmt::MakeWriter;

/// 通道容量（行数）。满时丢弃新行。
const CHANNEL_LINES: usize = 8_192;

/// 关闭后刷写线程的轮询间隔：关闭事件最多延迟该时长生效。
const SHUTDOWN_POLL: Duration = Duration::from_millis(50);

/// 非阻塞 stdout writer。
///
/// [`Write`] 只做有界投递（`try_send`），不执行任何阻塞 I/O；专用刷写
/// 线程从通道读取并写入 stdout。writer 句柄可 Clone（`SyncSender` 不可
/// Clone，内部经 `Arc` 共享）。
///
/// [`shutdown`](Self::shutdown) 置原子关闭标志：其后写入被丢弃，刷写
/// 线程在排空已入队事件后退出。发送端不显式关闭，由进程退出统一回收。
#[derive(Clone, Debug)]
pub(crate) struct NonBlockingWriter {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    tx: SyncSender<Box<[u8]>>,
    closed: AtomicBool,
}

impl NonBlockingWriter {
    /// 启动专用刷写线程，返回 writer 句柄与刷写线程句柄。
    ///
    /// # Errors
    ///
    /// 线程启动失败时返回 [`io::Error`]。
    pub(crate) fn spawn() -> Result<(Self, JoinHandle<()>), io::Error> {
        let (tx, rx) = sync_channel::<Box<[u8]>>(CHANNEL_LINES);
        let inner = Arc::new(Inner {
            tx,
            closed: AtomicBool::new(false),
        });
        let join = thread::Builder::new()
            .name("forgelink-log-writer".to_owned())
            .spawn({
                let inner = Arc::clone(&inner);
                move || drain_to_stdout(rx, inner)
            })?;
        Ok((Self { inner }, join))
    }

    /// 置关闭标志：后续写入全部丢弃；刷写线程排空已入队事件后退出。
    pub(crate) fn shutdown(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    /// 测试用：直接包装既有通道，不启动刷写线程。
    #[cfg(test)]
    pub(crate) fn from_channel(tx: SyncSender<Box<[u8]>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                tx,
                closed: AtomicBool::new(false),
            }),
        }
    }
}

/// 刷写线程主体：逐行写入 stdout（每个事件一行，fmt Layer 保证行完整）。
///
/// stdout 不可写（如管道断开）时退出线程，后续日志静默丢弃。
/// 注意：`stdout().lock()` 必须在单行写入后立即释放——锁本身是进程级
/// 全局锁，若在 `recv()` 阻塞期间一直持有，其他线程（含测试框架）
/// 的 stdout 输出会被永久卡死。
///
/// 关闭流程：`shutdown` 只置原子标志，不关闭通道（发送端由进程退出
/// 统一回收）；本线程以 [`SHUTDOWN_POLL`] 间隔轮询标志，置位后排空
/// 剩余事件并退出，保证通道中已入队日志在退出前刷出。
fn drain_to_stdout(rx: Receiver<Box<[u8]>>, inner: Arc<Inner>) {
    loop {
        match rx.recv_timeout(SHUTDOWN_POLL) {
            Ok(line) => {
                if !write_line(&line) {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) if inner.closed.load(Ordering::Acquire) => {
                while let Ok(line) = rx.try_recv() {
                    if !write_line(&line) {
                        return;
                    }
                }
                return;
            }
            Err(_) => return,
        }
    }
}

/// 写入单行 stdout；返回是否可继续写入。
fn write_line(line: &[u8]) -> bool {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    out.write_all(line).is_ok()
}

impl io::Write for NonBlockingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.inner.closed.load(Ordering::Relaxed) {
            let _ = self.inner.tx.try_send(buf.to_vec().into_boxed_slice());
        }
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