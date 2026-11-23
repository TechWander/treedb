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
//! | [`sha256`] | 握手用的 SHA-256 / HMAC-SHA256（§4.4） | 无 |
//!
//! 引擎本体只依赖 `libc` + `thiserror` + 标准库；`postgres` / `rusqlite`
//! 只出现在过渡档（[`crate::pool`] / [`crate::store`] / [`crate::persist`]）。

pub mod apply;
pub mod archive;
pub mod codec;
pub mod core;
pub mod dump;
pub mod mem;
pub mod model;
pub mod sha256;
pub mod wal;

pub use archive::{pitr_restore, plan_pitr, Archive, PitrPlan};
pub use codec::{Rd, W};
pub use core::{
    dump_dir, migrate_snapshot, now_ms, probe_snapshot, read_fence, restore_dump, Durability,
    Engine, EngineStats, MigrationOutcome, SnapshotInfo, FENCE_FILE, LOCK_SWEEP_BATCH,
};
pub use dump::{Dump, DumpCounts, DUMP_MAGIC, DUMP_VERSION};
pub use mem::{Log, State, SNAP_LEGACY_VERSION, SNAP_MAGIC, SNAP_TRAIL, SNAP_TRAIL_LEGACY, SNAP_VERSION};
pub use model::{Names, Op, Reply, NONCE_LEN};
pub use wal::Wal;

/// 线协议单帧 payload 上限。目录 readdir 一次 10 万条也够；再大直接拒，
/// 避免一个坏连接把内存吃穿。
pub const MAX_FRAME: u32 = 8 * 1024 * 1024;

/// 线协议版本：`Op`/`Reply` 的 tag 空间与字段顺序就是它的字节布局。
///
/// 加 op、加 Reply 变体、改任何字段顺序都要 +1，并按 `model.rs` 里的全 tag
/// 往返用例补上对应语料。帧里没有版本位（那是 v1 定的形状），所以这一条
/// 只在第一次 `Stats` 上能查到对端跑的是哪一版；新版客户端解旧服务端的
/// `Stats` 会因多出一个字段而短读报错（大声失败），旧客户端解新服务端的
/// 则会丢掉尾部字节（静默少一个字段）—— 升级先升服务端、后升客户端。
///
/// 历史：v3 加通用表（`Table*` / `Row*`）与 `ReadDirPlus`；v4 加日志传送那两条
/// 控制面 op（`WalFetch` / `SnapFetch`）与对应应答（`WalBatch` / `SnapChunk`），
/// 并给 `EngineStats` 尾部追加副本水位那几个字段。tag 空间依旧只追加不改旧值，
/// 所以 v3 认识的字节在 v4 里一字未动；但 `Stats` 的尾部多长出一截 —— 上面那
/// 条「旧客户端静默少一个字段」的兼容缺口，v4 正正好踩在 `Stats` 这一个口上。
///
/// v5 加接入认证那两条握手 op（`AuthBegin` / `AuthFinish`）与两条应答
/// （`Challenge` / `AuthOk`），见 §4.4。同样是只追加不改旧值，而且**服务端没启用
/// 认证时一个握手 op 也不会发**，所以 v4 客户端对 v5 服务端逐字照旧 ——
/// 这一版的兼容缺口比 v4 那条窄：`Stats` 的字段布局没动。
///
/// v6 给 `WalBatch` / `SnapChunk` 的**尾部**各加一个 `epoch`（fencing token，
/// §6.6 第 3 条）。这一版是**不兼容**的：两个应答各自多一个 u64，v5 的副本解
/// v6 的主会短读报错（大声失败，符合预期），v6 的副本解 v5 的主同样短读。
/// 也就是说追加字段在这里换不来跨版本可用 —— 但换来了更重要的东西：一个
/// 令牌不对的副本，宁可连不上也不能接。所以升级时主与副本一起换，先换主、
/// 副本会经历一段 `primary answered Err` / 短读重试，直到自身也被换掉。
///
/// 那条「加 Reply 变体要 +1」已被收窄（§4.1）：**只追加、不改旧帧**时不用动
/// 版本号 —— 旧客户端不会发新 op，也不会收到新 reply。前四例是 `Link` /
/// `RowRange` / `Batch`+`Reply::Batch` / `TableAddColumn`，第五例是
/// `SnapFetch { len: 0 }` 的探针答复 `Reply::SnapPrefix`（0x0182）：旧副本从不发
/// `len == 0`（`--fetch-bytes 0` 启动时就拒），旧主对它回一个空 `SnapChunk`，
/// 新副本认出这个答复就退回整份重下。
///
/// v7（2026-11-23，§11 第 10 条 M4）删掉了四组专用控制面 op 的 14 个 Op tag
/// （0x0040–0x0062）与 9 个 Reply tag（0x0140–0x0161），`Stats` 的帧里也少了
/// 中间两个 u64（`keys` / `audit`）。删除既有帧不是「只追加」，按 §4.1 的判据
/// 必须抬版本：v6 客户端发 `KeyList` 之类会撞上 `unknown op tag`，读 v6 的
/// `Stats` 也会把后面的字段整体错位 —— 两边都在握手时就 fail-closed，好过
/// 静默解错。
pub const PROTOCOL_VERSION: u16 = 7;
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
