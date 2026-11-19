//! 线程间的小信箱：一条 `Mutex<VecDeque<T>>` + 一个 `eventfd` 唤醒位。
//!
//! ## 为什么不是 `std::sync::mpsc`
//!
//! 接收端必须能被 `epoll_wait` 唤醒 —— 前端 worker 的全部世界就是一个 epoll
//! 循环，提交线程的答案得和「socket 可读」用同一只眼睛看。mpsc 的 `Receiver`
//! 没有任何 fd 可挂，于是只能限时轮询（白付延迟）或「先查队列再睡」（查与睡
//! 之间插进一次 send 就永久睡死）。
//!
//! ## 关掉竞态的两条写序
//!
//! - 发：先入队，后 signal；
//! - 收：先清计数，再把队列取空。
//!
//! 两条合起来，任何一次 send 要么被本轮的取空看见，要么把计数留下、下一轮
//! epoll 照样醒来。计数只允许**一个读者**清：每个信箱只注册在一方的 epoll 里
//! （`worker → committer` 归提交线程清，`committer → worker` 归那条 worker
//! 清），所以不存在两个线程抢同一次唤醒。
//!
//! ## 成本
//!
//! 每个信箱 1 个 fd。`workers = N` 时前端并行总共多花 `3N` 个 fd
//! （N 张监听套接字 + N 个上行 eventfd + N 个下行 eventfd），与连接数无关。

use crate::error::{Error, Result};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// `eventfd(2)` 的计数位：写入 = 唤醒，读出 = 清零。
struct Ev(i32);

impl Ev {
    fn new() -> Result<Ev> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(Error::Io(format!(
                "eventfd: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(Ev(fd))
    }

    fn signal(&self) {
        let one: u64 = 1;
        // 计数器是 u64-1 上限，溢出要真的把唤醒位丢光才可能：每 fd 每秒
        // 2^64 次 send。所以这里忽略 write 的返回值是安全的。
        unsafe {
            libc::write(
                self.0,
                &one as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// 一次读空计数，避免留下永远可读的 fd（否则本轮 epoll 白醒）。
    fn clear(&self) {
        let mut buf = [0u8; 8];
        unsafe {
            libc::read(
                self.0,
                buf.as_mut_ptr() as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }
}

impl Drop for Ev {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// 单向信箱。克隆 `Arc<Mail<T>>` 即是多写端共享同一头队列。
pub struct Mail<T> {
    q: Mutex<VecDeque<T>>,
    ev: Ev,
}

impl<T> Mail<T> {
    pub fn new() -> Arc<Mail<T>> {
        Arc::new(Mail {
            q: Mutex::new(VecDeque::new()),
            ev: Ev::new().expect("eventfd"),
        })
    }

    /// 挂进 `epoll` 用的 fd（可读 = 有货或可能有货）。
    pub fn fd(&self) -> i32 {
        self.ev.0
    }

    pub fn send(&self, v: T) {
        {
            let mut g = self.q.lock().unwrap_or_else(|e| e.into_inner());
            g.push_back(v);
        }
        self.ev.signal();
    }

    /// 取空当前队列。先清计数再取货，见模块文档的写序。
    pub fn drain(&self) -> Vec<T> {
        self.ev.clear();
        let mut g = self.q.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *g).into_iter().collect()
    }

    /// 有货吗（不取）。提交线程用它决定要不要立刻再来一轮。
    pub fn is_empty(&self) -> bool {
        self.q.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
    }
}
