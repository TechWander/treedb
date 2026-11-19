//! 自研元数据引擎。
//!
//! 分层（自上而下）：
//!
//! | 模块 | 职责 | 依赖 |
//! |------|------|------|
//! | [`crate::net::server`] | 单线程 epoll 事件循环，1 fd/连接 | libc |
//! | [`core`] | 提交流水线：stage → WAL fsync → ack | [`apply`], [`wal`] |
//! | [`apply`] | `Op` → `(logs, reply)` 的 POSIX 语义翻译 | [`mem`], [`model`] |
//! | [`mem`] | 权威内存状态 + 幂等 `Log` + 快照编解码 | [`codec`] |
//! | [`wal`] | CRC 记录帧 + group commit + 崩溃重放 | [`codec`] |
//! | [`model`] / [`codec`] | 协议契约与字节原语 | 无 |
//!
//! 引擎本体只依赖 `libc` + `thiserror` + 标准库；`postgres` / `rusqlite`
//! 只出现在过渡档（[`crate::pool`] / [`crate::store`] / [`crate::persist`]）。

pub mod apply;
pub mod codec;
pub mod core;
pub mod mem;
pub mod model;
pub mod wal;

pub use codec::{Rd, W};
pub use core::{Durability, Engine, EngineStats, now_ms};
pub use mem::{Log, State};
pub use model::{Names, Op, Reply};
pub use wal::Wal;

/// 线协议单帧 payload 上限。目录 readdir 一次 10 万条也够；再大直接拒，
/// 避免一个坏连接把内存吃穿。
pub const MAX_FRAME: u32 = 8 * 1024 * 1024;
/// 帧头长度：payload 字节数（u32 小端，不含头）。
pub const FRAME_HDR: usize = 4;

/// 把 payload 包成一帧。
pub fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HDR + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// 把应答编成一帧。事件循环线程和工作池线程都要用它，所以放在引擎层而不是
/// 网络层：worker 在自己线程里就把字节活干完，交回来的只是成品帧。
pub fn reply_frame(reply: &Reply) -> Vec<u8> {
    let mut w = W::new();
    reply.encode(&mut w);
    encode_frame(&w.finish())
}

/// 连接接收缓冲：带 `start` 游标，避免每帧都 `drain(..)` 搬移整块内存。
#[derive(Default)]
pub struct RxBuf {
    data: Vec<u8>,
    start: usize,
}

impl RxBuf {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        RxBuf {
            data: Vec::with_capacity(cap),
            start: 0,
        }
    }

    pub fn pending(&self) -> usize {
        self.data.len() - self.start
    }

    pub fn extend(&mut self, more: &[u8]) {
        // 攒够一半以上就整体前移，防止只读不消费的连接无限膨胀。
        if self.start > 0 && (self.start >= 4096 || self.start * 2 >= self.data.len()) {
            self.data.copy_within(self.start.., 0);
            self.data.truncate(self.data.len() - self.start);
            self.start = 0;
        }
        self.data.extend_from_slice(more);
    }

    /// 取出一整帧 payload；不足一帧返回 `None`（下次读到再继续）。
    pub fn take_frame(&mut self) -> crate::error::Result<Option<Vec<u8>>> {
        if self.pending() < FRAME_HDR {
            return Ok(None);
        }
        let h = self.data[self.start..self.start + FRAME_HDR]
            .try_into()
            .expect("4-byte slice");
        let len = u32::from_le_bytes(h);
        if len > MAX_FRAME {
            return Err(crate::error::Error::Codec(format!(
                "frame {len} over limit {MAX_FRAME}"
            )));
        }
        if self.pending() < FRAME_HDR + len as usize {
            return Ok(None);
        }
        let s = self.start + FRAME_HDR;
        let out = self.data[s..s + len as usize].to_vec();
        self.start = s + len as usize;
        Ok(Some(out))
    }

    /// peek 帧头声明的长度；不足一个头返回 None。客户端用它判断“下一帧齐了没”。
    pub fn peek_len(&self) -> Option<u32> {
        if self.pending() < FRAME_HDR {
            return None;
        }
        let h = self.data[self.start..self.start + FRAME_HDR]
            .try_into()
            .expect("4-byte slice");
        Some(u32::from_le_bytes(h))
    }

    /// 直接对缓冲里的帧做迭代式解码（写侧零拷贝路径留给将来用）。
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.pending() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_splits_and_rejoins_across_reads() {
        let a = encode_frame(b"hello");
        let b = encode_frame(b"world!");
        let mut rx = RxBuf::new();
        // 逐字节喂：任何时刻都不该有多余帧被误判。
        for byte in a.iter().chain(b.iter()) {
            rx.extend(&[*byte]);
        }
        assert_eq!(rx.take_frame().unwrap().unwrap(), b"hello");
        assert_eq!(rx.take_frame().unwrap().unwrap(), b"world!");
        assert!(rx.take_frame().unwrap().is_none());
    }

    #[test]
    fn partial_frame_waits_for_more() {
        let f = encode_frame(&[7u8; 100]);
        let mut rx = RxBuf::new();
        rx.extend(&f[..10]);
        assert!(rx.take_frame().unwrap().is_none());
        rx.extend(&f[10..]);
        assert_eq!(rx.take_frame().unwrap().unwrap().len(), 100);
    }

    #[test]
    fn oversized_frame_header_is_rejected() {
        let mut rx = RxBuf::new();
        rx.extend(&(MAX_FRAME + 1).to_le_bytes());
        assert!(rx.take_frame().is_err());
    }

    #[test]
    fn buffer_compacts_instead_of_growing() {
        let mut rx = RxBuf::new();
        for _ in 0..50 {
            rx.extend(&encode_frame(b"0123456789"));
            rx.take_frame().unwrap().unwrap();
        }
        assert!(rx.pending() == 0);
    }
}
