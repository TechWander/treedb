//! 预写日志（WAL）：已确认的写在返回给客户端之前先落盘。
//!
//! **记录帧格式**（全小端）：
//!
//! ```text
//! seq: u64 | crc: u32 | len: u32 | reserved: u32 | payload[len]
//! ```
//!
//! 头 20 字节。`crc` 覆盖 `payload`。选 20 而不是 16 是为了把 `reserved`
//! 明确写 0：将来加 flags/epoch 不必改帧长，老版本读到非 0 可拒绝。
//!
//! **崩溃语义**：`replay` 顺序扫，遇到下列任一情况即视为尾部并停止——
//! 短读（写到一半）、`len` 越上限、CRC 不符。因此掉电/`kill -9` 造成的
//! 半条记录只是「未确认的写」，自然被丢弃，已 fsync 的记录必然完整。
//!
//! 反之，**完好记录之间的 seq 跳号是硬错误**（不是尾部）：序号由写入端
//! 逐条递增分配，中间少一条只能意味着写入器把记录落丢了，属于必须当场
//! 炸掉的数据丢失，绝不能在恢复时当成“到这儿就没了”静默跳过。
//!
//! **group commit**：`append_batch` 一次 write + 一次 fsync 落多条记录，
//! 这是吞吐的来源；单条 `append` 走同一条路径（batch 长度 1）。
//!
//! **预分配**：文件长度不跟着提交长，而是按 4 MiB 起步、倍增地预先
//! `posix_fallocate` 出来，写入端自己记逻辑末尾 `end`。理由不是风格：机械盘 +
//! XFS 上「每次 commit 都把文件撑长 + fsync」要 6.9 ms，「预分配 +
//! fdatasync」只要 1.1 ms（`benches/` 里有实测），因为前者每次提交都得把 extent
//! 分配和 inode 大小**同步**写出去。长度不变后，数据落盘只需要 `fdatasync`。
//!
//! 预分配不能偷工：`posix_fallocate` 把空间填 0，所以扫描遇到全零头就是
//! 「到预分配空洞了」而不是「记录坏了」；但反过来，任何**改了文件长度**的动作
//! （open 截坏尾、reset、扩展）都必须紧跟一次 `sync_all`，否则掉电后文件比
//! 写进去的记录短，那些 ack 就真没了。

use crate::engine::codec::{Rd, W};
use crate::engine::mem::Log;
use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// 记录头长度：seq(8) + crc(4) + len(4) + reserved(4)。
pub const HDR: usize = 20;
/// 单条记录 payload 上限，防损坏头把恢复过程带进天坑。
pub const MAX_RECORD: u32 = 16 * 1024 * 1024;
/// WAL 预分配的起步粒度。
const PREALLOC_MIN: u64 = 4 << 20;
/// 预分配单次扩展的上限：倍增到 256 MiB 后按块递增，不一口气把数据目录
/// 的真实空间吃掉一大片（测试里一个目录只跑几条记录，4 MiB 就够）。
const PREALLOC_MAX: u64 = 256 << 20;

const CRC_POLY: u32 = 0xEDB8_8320;

fn crc_table() -> &'static [u32; 256] {
    use std::sync::OnceLock;
    static T: OnceLock<[u32; 256]> = OnceLock::new();
    T.get_or_init(|| {
        let mut t = [0u32; 256];
        for i in 0..256u32 {
            let mut c = i;
            for _ in 0..8 {
                c = if c & 1 != 0 { CRC_POLY ^ (c >> 1) } else { c >> 1 };
            }
            t[i as usize] = c;
        }
        t
    })
}

/// 标准 CRC32（IEEE，反射式）。引擎本体只依赖 libc + thiserror，
/// 所以这里自带查表实现，不引 crc crate。
pub fn crc32(buf: &[u8]) -> u32 {
    !crc32_update(0xFFFF_FFFF, buf)
}

/// CRC32 的**增量**形式：从 `0xFFFF_FFFF` 开局，逐段喂，最后取反。
///
/// 存在的理由：快照的 crc 要覆盖「正文 ‖ seq」两段相邻但不同类型的数据，
/// 而正文可能有几百 MB —— 为了拼一个连续 buffer 再算一遍，就是白白多一份
/// 全量拷贝的内存峰值。分两段算与一次算完逐位相同。
pub fn crc32_update(mut c: u32, buf: &[u8]) -> u32 {
    let t = crc_table();
    for &b in buf {
        c = t[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c
}

/// 把一条 `Log` 序列化成 payload 字节。
pub fn log_payload(log: &Log) -> Vec<u8> {
    let mut w = W::new();
    log.encode(&mut w);
    w.finish()
}

/// 日志传送（DESIGN.md §6.6）的一次只读交付结果。
///
/// 交付的是 payload **原字节**，不是解码后的 `Log`：副本把收到的记录逐字节落进
/// 自己的 WAL，两份日志因此可以按字节比对 —— 「副本重放的是同一条记录」这句
/// 话才有得验，而不是只能信一个「语义应该等价」的口头承诺。
pub struct WalDeliver {
    /// 要送出去的 payload，按 seq 升序且连续（`base_seq + i` 即 `records[i]`）。
    pub records: Vec<Vec<u8>>,
    /// `records[0]` 的 seq；没交付时等于请求的 `want_seq`。
    pub base_seq: u64,
    /// 交付末尾之后的偏移。调用方下次把它当 `hint_off` 回送就是顺序读，不必让
    /// 服务端从头扫 —— 这是个提示，丢了不致命，见 [`Wal::deliver`]。
    pub next_off: u64,
}

/// `deliver` 扫日志时的窗口大小：只读头就能跳记录，读一整窗比一条一次
/// `pread` 少两个数量级的系统调用。
const SCAN_WINDOW: usize = 256 * 1024;

/// 一条记录的头（[`HDR`] 那 20 字节）。
struct Header {
    seq: u64,
    crc: u32,
    len: u32,
}

/// [`Wal::deliver`] 与 [`Wal::locate`] 共用的只读滑动窗口。
///
/// 存在的理由只有一个数字：默认 64 MB 的 WAL 按 ~120 B/条算是 50 万条记录，
/// 逐条 `pread` 就是 100 万次系统调用（头一次、体一次）；按 256 KB 一窗读则是
/// 256 次。窗口不管记录会不会跨界 —— 跨界就从那一条的起点重读，多付几十
/// 字节，换来一个能用一句话说清的不变量：**窗口起点永远坐在记录边界上**。
#[derive(Default)]
struct Scan {
    /// 窗口首字节对应的文件偏移；`data` 为空 = 还没读过。
    off: u64,
    data: Vec<u8>,
}

pub struct Wal {
    path: PathBuf,
    file: File,
    /// 下一条要写的 seq。
    next_seq: u64,
    /// 已 fsync 到的最大 seq（ack 的前置条件）。
    durable_seq: u64,
    /// 逻辑末尾：下一条记录从这里开始写。文件可以比它长（预分配空洞），
    /// 但长度永远 `>= end`。
    end: u64,
    /// 已预分配到多少字节（= 文件长度）。
    allocated: u64,
    /// 这个文件系统是否支持 `posix_fallocate`。不支持时长度跟着写跑，每次
    /// commit 都会把文件撑长（慢 6 倍，但不会错）。
    prealloc_ok: bool,
}

impl Wal {
    /// 打开并扫描既有日志，返回 (wal, 最后一条有效 seq, 该 seq 之后的字节偏移)。
    ///
    /// 返回的偏移是**可安全追加点**：坏尾会被 `truncate` 掉。
    pub fn open(path: &Path) -> Result<(Wal, u64, u64)> {
        // 不用 O_APPEND：预分配后文件长度比逻辑末尾长，O_APPEND 会把记录
        // 写到空洞末尾，中间留出一大段零，重启后扫描在那段零上停下、后面的
        // ack 全丢。不用 O_APPEND 就意味着写入端自己记位置（`end`）。
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| Error::Io(format!("open wal {}: {e}", path.display())))?;
        let (last_seq, good_end) = scan(&mut file)?;
        let size = file.metadata()?.len();
        if size != good_end {
            // 截掉半条/坏尾，保证追加永远从一个合法记录边界开始。截完必须
            // 重新预分配：否则下一次写又把文件撑长，慢路径回来了。
            file.set_len(good_end)?;
        }
        let mut wal = Wal {
            path: path.to_path_buf(),
            file,
            next_seq: last_seq + 1,
            durable_seq: last_seq,
            end: good_end,
            // 上面那条 `set_len` 之后真实长度就是 good_end；没截过则相等。
            allocated: good_end,
            prealloc_ok: true,
        };
        // 只有真改了长度才付这一次 sync_all。干净重启（文件已经是预分配
        // 长度、末尾正好在记录边界）走不到这里，一个额外 fsync 都不花。
        wal.grow_to(wal.end + PREALLOC_MIN)?;
        Ok((wal, last_seq, good_end))
    }

    /// 把预分配水位抬到至少 `need`（4 MiB 起步、倍增，单次最多抬 256 MiB）。
    ///
    /// `posix_fallocate` 不支持/没空间时不致命：把 `prealloc_ok` 关掉，后续
    /// 写入自然把文件撑长。慢，但 `sync_data` 会把长度元数据一起刷，语义不变。
    fn grow_to(&mut self, need: u64) -> Result<()> {
        if !self.prealloc_ok || self.allocated >= need {
            return Ok(());
        }
        let mut target = self.allocated;
        while target < need {
            target += target.clamp(PREALLOC_MIN, PREALLOC_MAX);
        }
        if preallocate(&self.file, target).is_ok() {
            self.allocated = target;
            // 长度是元数据，必须**先于**写进那段空间的记录 durable，否则掉电
            // 后文件比记录短，那次 ack 就没了。
            self.file.sync_all()?;
        } else {
            self.prealloc_ok = false;
            self.allocated = self.end;
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn durable_seq(&self) -> u64 {
        self.durable_seq
    }

    /// 已追加到文件、但未必 fsync 过的最后一条 seq。
    ///
    /// 流水线（`multi` 路径）里提交线程在 `append_nodur` 之后拿它告诉
    /// flusher「fsync 到哪就算这批落了盘」，flusher 的完成通知把它带回
    /// 提交线程，由 [`Wal::advance_durable`] 推进水位。
    pub fn appended_seq(&self) -> u64 {
        self.next_seq - 1
    }

    /// dup 一个只用来 fsync 的独立 fd。
    ///
    /// `fsync`/`fdatasync` 不看文件偏移，所以与提交线程的 `seek + write`
    /// 并发安全；dup 共享同一个 file description，刷的就是同一批页。
    ///
    /// **`truncate_before` / `reset` 会把日志换成新 inode**：那之后必须重新
    /// 取一次本函数并让 flusher 换 fd（`multi::Flusher::retarget`），否则
    /// 后续 fsync 全刷到一个已被 rename 走的孤儿 inode 上 —— 新记录永不落盘，
    /// 而 ack 照发。
    pub fn try_clone_sync_fd(&self) -> Result<File> {
        self.file
            .try_clone()
            .map_err(|e| Error::Io(format!("dup wal fd: {e}")))
    }

    /// flusher 报完成之后由提交线程推进水位。
    ///
    /// 三重守卫都是必要的：只进不退（合并提交时晚到的 Done 可能带更小的
    /// seq）；不超过逻辑末尾（checkpoint 收口会把 `next_seq` 拨回水位，此时
    /// 一个旧 Done 带来的大 seq 在新一代日志里根本不存在）。
    pub fn advance_durable(&mut self, seq: u64) {
        if seq > self.durable_seq && seq < self.next_seq {
            self.durable_seq = seq;
        }
    }

    /// 把 seq 水位抬到 `seq`。
    ///
    /// checkpoint 会把 WAL 截成空文件，重启后 `scan` 只能查到 0，若不把
    /// 水位接回快照位置，新记录会从 1 重新发号，下次恢复时
    /// `replay(from_seq = snap_seq)` 会把这些记录当成「快照已覆盖」静默丢掉。
    pub fn fast_forward(&mut self, seq: u64) {
        if seq >= self.next_seq {
            self.next_seq = seq + 1;
            self.durable_seq = seq;
        }
    }

    /// 把一批 payload 拼成要落盘的连续字节，并把 `next_seq` 推过去。
    ///
    /// 不能用 `write_vectored` 循环：它返回的是**字节数**，把它当成消耗的
    /// iovec 个数推进游标会整段跳写，被跳过的那些记录从此再也没进过 WAL
    /// （已 ack 的写静默蒸发）。逐 iovec 补写还得处理“部分写落在某个 iovec
    /// 中间”，而 `write_all` 本来就把部分写循环包好了，代价只多一次 memcpy。
    fn frame(&mut self, payloads: &[Vec<u8>]) -> Result<(Vec<u8>, u64)> {
        let mut buf: Vec<u8> = Vec::with_capacity(payloads.iter().map(|p| p.len() + HDR).sum());
        let mut last = self.next_seq;
        for p in payloads {
            if p.len() as u32 > MAX_RECORD {
                return Err(Error::Codec(format!(
                    "record {} over wal limit {}",
                    p.len(),
                    MAX_RECORD
                )));
            }
            let seq = self.next_seq;
            self.next_seq += 1;
            buf.extend_from_slice(&seq.to_le_bytes());
            buf.extend_from_slice(&crc32(p).to_le_bytes());
            buf.extend_from_slice(&(p.len() as u32).to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.extend_from_slice(p);
            last = seq;
        }
        Ok((buf, last))
    }

    /// 追加一批记录并 durable 一次。返回最后一条的 seq。
    ///
    /// `payloads` 由调用方按提交顺序准备好（一次事务的多条 Log）。
    pub fn commit(&mut self, payloads: &[Vec<u8>]) -> Result<u64> {
        if payloads.is_empty() {
            return Ok(self.durable_seq);
        }
        let (buf, last) = self.frame(payloads)?;
        self.write_at(&buf)?;
        // `sync_data` = fdatasync：文件长度不变（预分配已经把它定下来了），
        // 所以不必再同步一次 inode 元数据。真按需增长时长度会变，
        // fdatasync 语义保证“影响后续读的长度元数据”仍会被刷下去。
        self.file.sync_data()?;
        self.durable_seq = last;
        Ok(last)
    }

    /// 只写不 fsync（`Durability::Never` 用）；后续任意一次 fsync 会把它
    /// 一起带下去。
    pub fn append_nodur(&mut self, payloads: &[Vec<u8>]) -> Result<u64> {
        if payloads.is_empty() {
            return Ok(self.durable_seq);
        }
        let (buf, last) = self.frame(payloads)?;
        self.write_at(&buf)?;
        Ok(last)
    }

    /// 从逻辑末尾写出去，然后把 `end` 推过去。
    ///
    /// 先 `seek` 再 `write`：不这么做的话 std 的文件游标会跟着上一次 `scan`
    /// 或前一次写跑位，记录就落在预分配空洞里去了。
    fn write_at(&mut self, buf: &[u8]) -> Result<()> {
        self.grow_to(self.end + buf.len() as u64)?;
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(buf)?;
        self.end += buf.len() as u64;
        Ok(())
    }

    /// 把已写出的记录刷到稳定介质。
    ///
    /// 没有新记录就直接返回：`durable_seq + 1 == next_seq` 意味着自上次 sync 以来
    /// 一个字节都没追加过，此时这次 `sync_data` 在语义上是空转。Never 档的事件
    /// 循环每 `sync_ms` 无条件调一次本函数，所以这个守卫让“纯读不碰盘”这个
    /// 说法在代码层成立。
    ///
    /// 但**别把它当优化**：实测（`benches/probe_srv.sh`，WAL 在 161 TB 机械 XFS
    /// 上，64 并发 readdir，`never` 档）加这道守卫前后是 50 622 → 49 856 ops/s，
    /// 完全在噪声里 —— 内核对着无脏页的文件做 `sync_data` 根本不往下发 IO。
    /// “空转 fsync 拖慢了读档”这个猜测已被证伪，见 DESIGN.md §7.10 末段。
    pub fn sync(&mut self) -> Result<()> {
        if self.durable_seq + 1 == self.next_seq {
            return Ok(());
        }
        self.file.sync_data()?;
        self.durable_seq = self.next_seq - 1;
        Ok(())
    }

    /// 重放 `(from_seq, ∞)` 的记录。`from_seq` 传快照覆盖到的 seq。
    ///
    /// 尾部破损（短读/CRC 不符/len 越界）只停止；完好记录之间跳号则直接报错，
    /// 语义与 [`scan`] 一致，两条路径不能对同一个文件给出不一致的结论。
    pub fn replay(path: &Path, from_seq: u64) -> Result<Vec<Log>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut out = Vec::new();
        let mut hdr = [0u8; HDR];
        let mut expect: Option<u64> = None;
        loop {
            match read_exact_or_eof(&mut file, &mut hdr)? {
                false => break,
                true => {}
            }
            let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
            let crc = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
            let len = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
            if len > MAX_RECORD {
                break;
            }
            // 零长 payload 不是记录，是预分配没写过的空洞（每条合法 Log 至少
            // 带一个 op tag）。不当这一步，整片零会被当成 crc 合上的记录，
            // 然后下面那个 seq 跳号硬错就会在健康日志上误报。
            if len == 0 {
                break;
            }
            let mut body = vec![0u8; len as usize];
            if !read_exact_or_eof(&mut file, &mut body)? {
                break;
            }
            if crc32(&body) != crc {
                break;
            }
            if let Some(e) = expect {
                if seq != e {
                    return Err(Error::Io(format!(
                        "wal record seq gap while replaying {}: expected {e}, found {seq}",
                        path.display()
                    )));
                }
            }
            expect = Some(seq + 1);
            if seq > from_seq {
                let mut r = Rd::new(&body);
                let log = Log::decode(&mut r)?;
                out.push(log);
            }
        }
        Ok(out)
    }

    /// 下一条要写的 seq。副本 apply 前用它验连续性：交付区间里跳号等于记录丢了。
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// 只读交付 `seq >= want_seq` 的记录，负载合计不超过 `max_bytes`。
    ///
    /// 三条口径都是刻意的：
    ///
    /// 1. **只给已 durable 的**。`Durability::Never` 下逻辑末尾比 `durable_seq`
    ///    超前，把没落盘的记录送给副本，等于让副本答出主随时可能丢的写 ——
    ///    副本的读必须永远落后或等于主的已确认集，绝不能领先。
    /// 2. `hint_off` 只在「那个偏移上确实坐着 `want_seq`，且它声明的记录完整落在
    ///    逻辑末尾之内」时才用；校验不过就退回从头扫。副本重启后没有提示，付一次
    ///    全扫，此后的每批都走顺序读。残留一种极端情形：伪提示恰好读出一个 seq
    ///    相符的假头，那第一次 CRC 校验会报错而不是静默错送 —— 副本收到错误会
    ///    把提示清空重试（回到 `WAL_HINT_NONE`），不会永久卡在同一个游标上。
    /// 3. 头读不全、CRC 不符一律**报错而不是当成尾部**。`open` 早就把坏尾截掉了，
    ///    所以 `seq <= durable_seq` 区间里出现坏记录只能是真损坏；在这里当尾部
    ///    咽下去，副本会永远停在同一个游标上不动，那比一次报错难查得多。
    ///
    /// 「缺的那段还在不在日志里」不在这个结构的答案里：那由调用方拿快照水位
    /// 判（见 [`crate::engine::core::Engine`] 的 `Op::WalFetch` 分支），日志本身只
    /// 回答「这段字节我给不给得出」。
    pub fn deliver(&self, want_seq: u64, hint_off: u64, max_bytes: u32) -> Result<WalDeliver> {
        let mut sc = Scan::default();
        // 已经给无可给：把调用方的游标原样退回，一个字节也不读。
        // 空闲副本每轮都走这一条，所以这条快路径值这个钱。
        if want_seq > self.durable_seq {
            return Ok(WalDeliver {
                records: Vec::new(),
                base_seq: want_seq,
                next_off: hint_off,
            });
        }
        let mut off = match self.peek(&mut sc, hint_off)? {
            Some(h)
                if h.seq == want_seq
                    && hint_off + HDR as u64 + h.len as u64 <= self.end =>
            {
                hint_off
            }
            _ => self.locate(&mut sc, want_seq)?,
        };
        let mut records: Vec<Vec<u8>> = Vec::new();
        let mut given = 0u32;
        let mut expect = want_seq;
        // 送达的上界是 `durable_seq`，不是「文件里已经写出去的字节」：`Never` 档的
        // 逻辑末尾超前已确认集，把那段送出去等于让副本答出主随时可能丢的写。
        while expect <= self.durable_seq {
            let h = match self.peek(&mut sc, off)? {
                Some(h) => h,
                None => break,
            };
            if h.seq != expect {
                return Err(Error::Io(format!(
                    "wal deliver: expected seq {expect} at offset {off}, found {} \
                     (a durable record is gone)",
                    h.seq
                )));
            }
            if off + HDR as u64 + h.len as u64 > self.end {
                return Err(Error::Io(format!(
                    "wal deliver: record {} at offset {off} claims len {}, past logical end {}",
                    h.seq, h.len, self.end
                )));
            }
            let body = self.chunk(&mut sc, off + HDR as u64, h.len as usize)?;
            if crc32(body) != h.crc {
                return Err(Error::Io(format!(
                    "wal deliver: crc mismatch on durable record {} at offset {off}",
                    h.seq
                )));
            }
            records.push(body.to_vec());
            given += h.len;
            off += HDR as u64 + h.len as u64;
            expect += 1;
            if given >= max_bytes {
                break;
            }
        }
        Ok(WalDeliver {
            records,
            base_seq: want_seq,
            next_off: off,
        })
    }

    /// 读 `off` 处的记录头。越界、或读到预分配空洞（`len == 0`）都返回 `None`，
    /// 语义与 [`scan`] 的「到这里就是尾」一致 —— 两条路径不能对同一个文件
    /// 给出不一致的结论，否则副本会停在主自己当成尾部的那个位置上。
    ///
    /// 先单独比 `off >= end` 再算 `off + HDR`：`hint_off` 可以是
    /// [`WAL_HINT_NONE`](crate::engine::model::WAL_HINT_NONE) = `u64::MAX`，
    /// 加法在那儿绕过一圈回来会落在一个看似合理的小偏移上。
    fn peek(&self, sc: &mut Scan, off: u64) -> Result<Option<Header>> {
        if off >= self.end || off + HDR as u64 > self.end {
            return Ok(None);
        }
        let raw = self.chunk(sc, off, HDR)?;
        let len = u32::from_le_bytes(raw[12..16].try_into().unwrap());
        if len == 0 || len > MAX_RECORD {
            return Ok(None);
        }
        Ok(Some(Header {
            seq: u64::from_le_bytes(raw[0..8].try_into().unwrap()),
            crc: u32::from_le_bytes(raw[8..12].try_into().unwrap()),
            len,
        }))
    }

    /// 从头扫到 `want_seq` 那条记录的偏移：只读头，payload 靠偏移跳过。
    ///
    /// 提示失效时的退路，不在稳态路径上。代价是 `WAL / SCAN_WINDOW` 次 `pread`
    /// 加一次内存里的线性扫头。
    fn locate(&self, sc: &mut Scan, want_seq: u64) -> Result<u64> {
        let mut off = 0u64;
        while let Some(h) = self.peek(sc, off)? {
            if h.seq == want_seq {
                return Ok(off);
            }
            if h.seq > want_seq {
                return Err(Error::Io(format!(
                    "wal locate: seq {want_seq} is not in the log; it starts at {}",
                    h.seq
                )));
            }
            off += HDR as u64 + h.len as u64;
        }
        Err(Error::Io(format!(
            "wal locate: seq {want_seq} past the last record (logical end {})",
            self.end
        )))
    }

    /// 保证 `[off, off + n)` 在窗口里，返回那段切片。不够就整窗重读。
    ///
    /// 生命周期只挂在 `sc` 上而不是 `&self`：返的切片来自窗口缓冲，而窗口缓冲归
    /// `Scan` —— 写成默认省略规则会绑到 `&self` 上，调用方马上拿不到。
    fn chunk<'a>(&self, sc: &'a mut Scan, off: u64, n: usize) -> Result<&'a [u8]> {
        if off + n as u64 > self.end {
            return Err(Error::Io(format!(
                "wal window read [{off}, {}) past logical end {}",
                off + n as u64,
                self.end
            )));
        }
        let hit = !sc.data.is_empty()
            && sc.off <= off
            && (off - sc.off) as usize + n <= sc.data.len();
        if !hit {
            let room = (self.end - off).min(n.max(SCAN_WINDOW) as u64) as usize;
            sc.data = self.read_at(off, room)?;
            sc.off = off;
            if sc.data.len() < n {
                return Err(Error::Io(format!(
                    "wal window short at {off}: wanted {n}, got {}",
                    sc.data.len()
                )));
            }
        }
        let b = (off - sc.off) as usize;
        Ok(&sc.data[b..b + n])
    }

    /// `pread`：不动写入端的文件游标，也不需要一个只读 `try_clone` 的 fd。
    /// 交付路径与提交路径因此可以在同一个 `Wal` 上各读各的、各写各的。
    fn read_at(&self, off: u64, len: usize) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; len];
        let mut got = 0usize;
        while got < len {
            match self.file.read_at(&mut buf[got..], off + got as u64) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(format!("read wal at {}: {e}", off + got as u64))),
            }
        }
        buf.truncate(got);
        Ok(buf)
    }

    /// checkpoint 之后清空日志：新文件从 `seq` 之后继续编号。
    pub fn reset(&mut self, seq: u64) -> Result<()> {
        let tmp = self.path.with_extension("wal.tmp");
        // 不用 `.truncate(true)`：本工具链的 std 拒绝 read+write+truncate
        // 的组合（"creating or truncating a file requires write or append
        // access"），而下面那句 `set_len(0)` 本来就完成了清空。
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&tmp)?;
        file.set_len(0)?;
        // 必须把旧一代的记录从物理上抹掉，不是只把游标拨回 0：预分配的空洞
        // 里还坐着上一代的完好记录，下次扫描读完新几条就会碰上它们，要么
        // seq 跳号报硬错，要么把老记录当成新写重放一遍。
        let pre = preallocate(&file, PREALLOC_MIN);
        file.sync_all()?;
        std::fs::rename(&tmp, &self.path)?;
        let dir = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        if let Ok(d) = File::open(&dir) {
            let _ = d.sync_all();
        }
        self.file = file;
        self.next_seq = seq + 1;
        self.durable_seq = seq;
        self.end = 0;
        self.allocated = if pre.is_ok() { PREALLOC_MIN } else { 0 };
        self.prealloc_ok = pre.is_ok();
        Ok(())
    }

    /// WAL 的**逻辑**长度（已写入的字节数）。文件真实长度可能更长
    /// （预分配空洞），对 checkpoint 判定和对外报表两者都只看这个值。
    pub fn logical_size(&self) -> Result<u64> {
        Ok(self.end)
    }

    /// WAL 在磁盘上占了多少字节（含预分配空洞）。与 [`logical_size`](Self::logical_size)
    /// 是两个数，容量规划要看后一个：预分配是倍增的，`du` 看到的可以比
    /// 逻辑长度大一截。取不到真实长度时退回 `allocated`（那个就是上一次
    /// 成功预分配到的位置）。
    pub fn disk_size(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .map(|m| m.len())
            .unwrap_or(self.allocated))
    }

    /// checkpoint 之后把 `(0, seq]` 的记录从日志里抹掉，**保留 `seq` 之后
    /// 的那些**，返回保留下来多少条。
    ///
    /// 为什么不直接 [`Wal::reset`]：同步 checkpoint 在单线程循环里跑，取水位
    /// 那一刻之后不可能再进新记录，所以「全清 + 水位接回 seq」恰好等价于
    /// 「抹掉 ≤ seq」。旁路快照（`Engine::checkpoint_start`）不同 —— 它在后台
    /// 线程里编，期间事件循环照常 commit，那些 > seq 的记录**已经 ack、只活在
    /// 日志里**（内存里也有，但重启之后只剩日志）。一起抹掉就是静默丢已确认
    /// 的写，所以这里逐帧把尾巴搬进新文件；seq 不从 0 重编 —— 重编会让恢复时
    /// 快照水位与日志首条对不上。
    ///
    /// 顺序与 [`Wal::reset`] 同一套纪律：新文件 → `sync_all` → rename → 刷父目录。
    /// 任一步崩溃只会留下「旧快照 + 更长的 WAL」或「新日志」两个自洽形状。
    /// 预分配空洞 / 短读 / CRC 不符一律当尾部（与 [`Wal::replay`] 同口径）：
    /// 那些字节要么从没写过，要么本身就是未确认的写。
    pub fn truncate_before(&mut self, seq: u64) -> Result<u64> {
        let tmp = self.path.with_extension("wal.tmp");
        let mut src = OpenOptions::new().read(true).open(&self.path)?;
        let mut out = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&tmp)?;
        out.set_len(0)?;
        let mut hdr = [0u8; HDR];
        let mut body: Vec<u8> = Vec::new();
        let mut end = 0u64;
        let mut kept = 0u64;
        let mut last = seq;
        let mut next = seq + 1;
        loop {
            if !read_exact_or_eof(&mut src, &mut hdr)? {
                break;
            }
            let s = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
            let crc = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
            let len = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
            if len == 0 || len > MAX_RECORD {
                break;
            }
            body.clear();
            body.resize(len as usize, 0);
            if !read_exact_or_eof(&mut src, &mut body)? {
                break;
            }
            if crc32(&body) != crc {
                break;
            }
            if s <= seq {
                continue;
            }
            if s != next {
                return Err(Error::Io(format!(
                    "wal record seq gap while compacting {}: expected {next}, found {s}",
                    self.path.display()
                )));
            }
            out.write_all(&s.to_le_bytes())?;
            out.write_all(&crc32(&body).to_le_bytes())?;
            out.write_all(&(body.len() as u32).to_le_bytes())?;
            out.write_all(&0u32.to_le_bytes())?;
            out.write_all(&body)?;
            end += (HDR + body.len()) as u64;
            next = s + 1;
            last = s;
            kept += 1;
        }
        let pre = preallocate(&out, PREALLOC_MIN.max(end));
        // 长度/内容先落定再 rename，与 reset 同一条理由：换名之后新文件就是
        // 唯一的日志，它的扇区与长度必须已经在盘上。
        out.sync_all()?;
        std::fs::rename(&tmp, &self.path)?;
        let dir = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        if let Ok(d) = File::open(&dir) {
            let _ = d.sync_all();
        }
        self.file = out;
        self.end = end;
        self.allocated = if pre.is_ok() { PREALLOC_MIN.max(end) } else { end };
        self.prealloc_ok = pre.is_ok();
        // 保留下来的那些记录刚被 `sync_all` 带下去了，所以水位至少是 `last`。
        self.next_seq = next;
        self.durable_seq = last.max(seq);
        Ok(kept)
    }
}

/// 把 `f` 的真实长度抬到 `target` 并保证那段空间可读为 0。
///
/// `posix_fallocate` 直接返回错误号（不是 -1 + errno），而且需要文件是以
/// 可写方式打开的。
fn preallocate(f: &File, target: u64) -> std::result::Result<(), i32> {
    let size = f.metadata().map(|m| m.len()).unwrap_or(0);
    if target <= size {
        return Ok(());
    }
    let rc = unsafe {
        libc::posix_fallocate(
            f.as_raw_fd(),
            0,
            target as libc::off_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(rc)
    }
}

fn read_exact_or_eof(f: &mut File, buf: &mut [u8]) -> Result<bool> {
    let mut got = 0usize;
    while got < buf.len() {
        match f.read(&mut buf[got..]) {
            Ok(0) => {
                if got == 0 {
                    return Ok(false);
                }
                // 读到一半对端 EOF：半条记录，当坏尾处理。
                return Ok(false);
            }
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::Io(e.to_string())),
        }
    }
    Ok(true)
}

/// 顺序扫到尾部，返回 (最后一条有效 seq,  good_end 偏移)。
fn scan(file: &mut File) -> Result<(u64, u64)> {
    let mut seq = 0u64;
    let mut off = 0u64;
    let mut hdr = [0u8; HDR];
    // 下一条完好记录应有的 seq。第一条把基准定下来（checkpoint 重置后
    // 文件从快照水位+1 开始，所以不能写死 0/1）。
    let mut expect: Option<u64> = None;
    loop {
        if !read_exact_or_eof(file, &mut hdr)? {
            break;
        }
        let s = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
        let crc = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
        let len = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
        if len > MAX_RECORD {
            break;
        }
        // 同 `replay`：零长头 = 预分配空洞，到这儿就是尾。
        if len == 0 {
            break;
        }
        let mut body = vec![0u8; len as usize];
        if !read_exact_or_eof(file, &mut body)? {
            break;
        }
        if crc32(&body) != crc {
            break;
        }
        if let Some(e) = expect {
            if s != e {
                return Err(Error::Io(format!(
                    "wal record seq gap at offset {off}: expected {e}, found {s} \
                     (a confirmed write never made it into the log)"
                )));
            }
        }
        expect = Some(s + 1);
        seq = s;
        off += HDR as u64 + len as u64;
    }
    Ok((seq, off))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mem::{Log, State};
    use crate::engine::model::ViewMeta;

    fn put_view(name: &[u8], id: u32) -> Log {
        Log::PutView {
            name: name.to_vec(),
            meta: ViewMeta {
                id,
                root: 1,
                next_ino: 2,
            },
        }
    }

    #[test]
    fn crc32_matches_known_vector() {
        // 标准测试向量："123456789" -> 0xCBF43926
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn replay_skips_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        {
            let mut w = Wal::open(&p).unwrap().0;
            w.commit(&[log_payload(&put_view(b"a", 1))]).unwrap();
            w.commit(&[log_payload(&put_view(b"b", 2))]).unwrap();
            // 手工制造半条第三条记录：写头不写体。
            //
            // 必须落在逻辑末尾，不是文件末尾：预分配后两者差着一大片零，
            // append 到文件末尾的垃圾根本扫不到，测不出想测的东西。
            use std::io::{Seek, Write};
            let end = w.logical_size().unwrap();
            let mut f = OpenOptions::new().write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start(end)).unwrap();
            f.write_all(&3u64.to_le_bytes()).unwrap();
            f.write_all(&0u32.to_le_bytes()).unwrap();
            f.write_all(&8u32.to_le_bytes()).unwrap();
            f.write_all(&0u32.to_le_bytes()).unwrap();
            f.write_all(b"1234").unwrap();
            f.flush().unwrap();
        }
        // 半条被丢弃，前两条完好；open 顺带把坏尾截掉。
        let (mut w, last, end) = Wal::open(&p).unwrap();
        assert_eq!(last, 2);
        assert_eq!(w.logical_size().unwrap(), end);
        let logs = Wal::replay(&p, 0).unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[1], put_view(b"b", 2));
        // 截完还能接着写：第三条真落盘且 seq 接得上，不留零长头也不跳号。
        w.commit(&[log_payload(&put_view(b"c", 3))]).unwrap();
        drop(w);
        let (w2, last2, _) = Wal::open(&p).unwrap();
        assert_eq!(last2, 3, "坏尾截掉后接着写必须能被完整重放");
        assert_eq!(Wal::replay(&p, 0).unwrap().len(), 3);
        drop(w2);
    }

    #[test]
    fn bit_flip_in_payload_stops_replay_at_that_record() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        {
            let mut w = Wal::open(&p).unwrap().0;
            w.commit(&[
                log_payload(&put_view(b"a", 1)),
                log_payload(&put_view(b"b", 2)),
            ])
            .unwrap();
        }
        let mut raw = std::fs::read(&p).unwrap();
        // 翻掉第一条记录 payload 的最后一个字节。
        let first_len = HDR + log_payload(&put_view(b"a", 1)).len();
        raw[first_len - 1] ^= 0xFF;
        std::fs::write(&p, &raw).unwrap();
        let logs = Wal::replay(&p, 0).unwrap();
        assert!(logs.is_empty(), "CRC 坏了的第一条之后全部作废");
    }

    #[test]
    fn replay_honors_snapshot_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        {
            let mut w = Wal::open(&p).unwrap().0;
            for i in 1..=5u32 {
                // 名字要互不同：同名的 PutView 在状态里只会留一行。
                w.commit(&[log_payload(&put_view(format!("v{i}").as_bytes(), i))])
                    .unwrap();
            }
        }
        let logs = Wal::replay(&p, 3).unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0], put_view(b"v4", 4));

        // 端到端：快照 + 尾日志 = 完整状态。
        let mut s = State::new();
        for l in Wal::replay(&p, 0).unwrap().into_iter().take(3) {
            s.apply(&l);
        }
        let mut w = W::new();
        s.save(&mut w);
        let bytes = w.finish();
        let mut back = State::load(&mut Rd::new(&bytes)).unwrap();
        for l in Wal::replay(&p, 3).unwrap() {
            back.apply(&l);
        }
        assert_eq!(back.views.len(), 5);
    }

    #[test]
    fn reset_keeps_sequence_numbering() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let mut w = Wal::open(&p).unwrap().0;
        w.commit(&[log_payload(&put_view(b"a", 1))]).unwrap();
        w.reset(7).unwrap();
        assert_eq!(w.logical_size().unwrap(), 0);
        let last = w.commit(&[log_payload(&put_view(b"b", 2))]).unwrap();
        assert_eq!(last, 8);
        drop(w);
        let logs = Wal::replay(&p, 7).unwrap();
        assert_eq!(logs, vec![put_view(b"b", 2)]);
        // 重开不能报跳号：预分配空洞里的上一代记录必须已经被抹掉。
        let (w2, last2, end2) = Wal::open(&p).unwrap();
        assert_eq!(last2, 8);
        assert_eq!(w2.logical_size().unwrap(), end2);
    }

    /// 旁路快照的收尾切割：`(0, seq]` 整段丢掉，`seq` 之后的尾巴原样搬走，
    /// seq 一个都不重编。
    ///
    /// 换成 `reset(seq)` 写这条测试就会红 —— 尾巴那两条已经 ack 的写会连着
    /// 前缀一起蒸发，这是「已确认的写不丢」直接破掉的地方。
    #[test]
    fn truncate_before_drops_the_covered_prefix_and_keeps_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let tail_bytes = (HDR + log_payload(&put_view(b"v4", 4)).len()
            + HDR
            + log_payload(&put_view(b"v5", 5)).len()) as u64;
        let mut w = Wal::open(&p).unwrap().0;
        for i in 1..=3u32 {
            w.commit(&[log_payload(&put_view(format!("v{i}").as_bytes(), i))])
                .unwrap();
        }
        w.commit(&[
            log_payload(&put_view(b"v4", 4)),
            log_payload(&put_view(b"v5", 5)),
        ])
        .unwrap();

        assert_eq!(w.truncate_before(3).unwrap(), 2, "水位之前那三条该被丢掉");
        assert_eq!(w.next_seq(), 6, "切割不能把已经用掉的 seq 还回去");
        assert_eq!(w.durable_seq(), 5, "搬过来的尾巴已经 sync，水位跟着它走");
        assert_eq!(w.logical_size().unwrap(), tail_bytes);
        assert_eq!(
            Wal::replay(&p, 0).unwrap(),
            vec![put_view(b"v4", 4), put_view(b"v5", 5)],
            "尾巴必须原样留在文件里，一条不少、号也不变"
        );

        // 切割之后接着写：seq 接得上，重开也不跳号。
        assert_eq!(w.commit(&[log_payload(&put_view(b"v6", 6))]).unwrap(), 6);
        drop(w);
        let (mut w2, last, end) = Wal::open(&p).unwrap();
        assert_eq!(last, 6);
        assert_eq!(w2.logical_size().unwrap(), end);
        assert_eq!(Wal::replay(&p, 0).unwrap().len(), 3);

        // 全部被盖住：文件清空，但水位不退回 1。
        assert_eq!(w2.truncate_before(6).unwrap(), 0);
        assert_eq!(w2.logical_size().unwrap(), 0);
        assert_eq!(w2.next_seq(), 7);
        assert_eq!(w2.durable_seq(), 6);
        assert_eq!(w2.commit(&[log_payload(&put_view(b"v7", 7))]).unwrap(), 7);
        drop(w2);
        assert_eq!(Wal::replay(&p, 0).unwrap(), vec![put_view(b"v7", 7)]);
    }

    /// 预分配空洞不能被当成记录：健康日志重开 N 次都得给出同样的结论。
    /// 零长头如果逃过检查，就会被当成 crc 合上的记录，接着撞上那个跳号硬错。
    #[test]
    fn prealloc_padding_reopens_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let mut w = Wal::open(&p).unwrap().0;
        for i in 1..=20u32 {
            w.commit(&[log_payload(&put_view(format!("v{i}").as_bytes(), i))])
                .unwrap();
        }
        let end = w.logical_size().unwrap();
        let size = std::fs::metadata(&p).unwrap().len();
        // 预分配可能不支持（退回按需增长），所以只在真预分配时断言长度差。
        if size > end {
            assert!(size >= PREALLOC_MIN, "文件长度应至少一个预分配粒度");
        }
        drop(w);
        for _ in 0..3 {
            let (rw, last, re) = Wal::open(&p).unwrap();
            assert_eq!(last, 20, "重开不得把预分配空洞当成记录");
            assert_eq!(re, end);
            assert_eq!(rw.logical_size().unwrap(), end);
            drop(rw);
        }
    }

    /// 一次提交里塞几千条记录：以前走的是 `write_vectored` 循环，把返回的
    /// 字节数当成 iovec 个数推进游标，于是大批次会整段跳写 —— 被跳过的那些
    /// 记录从来没进过磁盘，重启后就是一场静默的数据丢失。这条是其回归测试。
    #[test]
    fn huge_batch_writes_every_record() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        const N: usize = 5000;
        let payloads: Vec<Vec<u8>> = (1..=N)
            .map(|i| log_payload(&put_view(format!("v{i}").as_bytes(), i as u32)))
            .collect();
        {
            let mut w = Wal::open(&p).unwrap().0;
            let last = w.commit(&payloads).unwrap();
            assert_eq!(last as usize, N);
        }
        let logs = Wal::replay(&p, 0).unwrap();
        assert_eq!(logs.len(), N, "一次大 commit 把记录写丢了");
        assert_eq!(logs[0], put_view(b"v1", 1));
        assert_eq!(logs[N / 2], put_view(format!("v{}", N / 2 + 1).as_bytes(), (N / 2 + 1) as u32));
        assert_eq!(logs[N - 1], put_view(format!("v{N}").as_bytes(), N as u32));
        // seq 必须连续到 N，不能“条数对但号错位”。
        let (w, last, end) = Wal::open(&p).unwrap();
        assert_eq!(last as usize, N);
        assert_eq!(w.logical_size().unwrap(), end);
    }

    /// 完好记录之间跳号 = 已确认的写根本没落盘，必须报错而不是当成尾部。
    #[test]
    fn seq_gap_is_a_hard_error_not_a_tail() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        {
            let mut w = Wal::open(&p).unwrap().0;
            w.commit(&[
                log_payload(&put_view(b"a", 1)),
                log_payload(&put_view(b"b", 2)),
                log_payload(&put_view(b"c", 3)),
            ])
            .unwrap();
        }
        // 手工把中间那条的 seq 抬成 42（不动 payload，CRC 仍对）。
        let a_len = HDR + log_payload(&put_view(b"a", 1)).len();
        let mut raw = std::fs::read(&p).unwrap();
        raw[a_len..a_len + 8].copy_from_slice(&42u64.to_le_bytes());
        std::fs::write(&p, &raw).unwrap();

        let e = Wal::open(&p).err().expect("open 必须把跳号当错误");
        assert!(e.to_string().contains("gap"), "{e}");
        let e = Wal::replay(&p, 0).err().expect("replay 同样不能咽下跳号");
        assert!(e.to_string().contains("gap"), "{e}");
    }

    // ---- 日志传送：`deliver` 的四条口径各占一个用例 ----

    /// 稳态形状：上一轮返回的 `next_off` 当下一轮的 `hint_off` 回送，
    /// 就能一路顺序读到底，而且交出来的是 payload 原字节。
    #[test]
    fn deliver_streams_forward_with_the_returned_hint() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let payloads: Vec<Vec<u8>> = (1..=4u8)
            .map(|i| log_payload(&put_view(&[b'a' + i], i as u32)))
            .collect();
        let mut w = Wal::open(&p).unwrap().0;
        for pl in &payloads {
            w.commit(std::slice::from_ref(pl)).unwrap();
        }

        let d = w.deliver(1, u64::MAX, u32::MAX).unwrap();
        assert_eq!(d.records, payloads, "交付必须与落盘的字节一字不差");
        assert_eq!(d.base_seq, 1);
        assert_eq!(d.next_off, w.logical_size().unwrap(), "游标要正好停在尾");

        // 拿着提示接着要第 3、4 条：走的是顺序读，不回头扫。
        let d = w.deliver(3, d.next_off - (HDR as u64 * 2 + payloads[2].len() as u64 + payloads[3].len() as u64), u32::MAX).unwrap();
        assert_eq!(d.records, vec![payloads[2].clone(), payloads[3].clone()]);
        assert_eq!(d.base_seq, 3);

        // 已经读到尾：一个字节也不给，游标原样退回，不报错。
        let end = d.next_off;
        let d = w.deliver(5, end, u32::MAX).unwrap();
        assert!(d.records.is_empty());
        assert_eq!(d.base_seq, 5);
        assert_eq!(d.next_off, end, "空交付不能把调用方的游标弄丢");
    }

    /// 提示只是个提示：偏移上坐着的不是 `want_seq`（甚至根本不是个记录边界）
    /// 也要能给出正确答案，代价退回从头扫。副本重启、主做过 `reset` 都走这条。
    #[test]
    fn deliver_ignores_a_stale_or_absent_hint_and_still_lands_right() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let payloads: Vec<Vec<u8>> = (1..=3u8)
            .map(|i| log_payload(&put_view(&[b'a' + i], i as u32)))
            .collect();
        let mut w = Wal::open(&p).unwrap().0;
        w.commit(&payloads).unwrap();

        for hint in [u64::MAX, 0, 7, HDR as u64] {
            let d = w.deliver(3, hint, u32::MAX).unwrap();
            assert_eq!(d.records, vec![payloads[2].clone()], "hint {hint}");
            assert_eq!(d.base_seq, 3, "hint {hint}");
        }

        // 要一条从来没存在过的 seq：不是错，是「还没轮到」。
        let d = w.deliver(99, u64::MAX, u32::MAX).unwrap();
        assert!(d.records.is_empty());
    }

    /// `Never` 档的口径：已写出但未 fsync 的记录不能送。副本读到它们的瞬间，
    /// 主崩一下就会发现自己答了一条从未被确认的写。
    #[test]
    fn deliver_stops_at_the_durable_watermark_not_the_logical_end() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let a = log_payload(&put_view(b"a", 1));
        let b = log_payload(&put_view(b"b", 2));
        let c = log_payload(&put_view(b"c", 3));
        let mut w = Wal::open(&p).unwrap().0;
        w.commit(&[a.clone(), b.clone()]).unwrap(); // durable = 2
        w.append_nodur(&[c.clone()]).unwrap(); // 已写出，未落盘

        let d = w.deliver(1, u64::MAX, u32::MAX).unwrap();
        assert_eq!(d.records, vec![a, b], "未 durable 的那条不能送出去");
        // 要第 3 条：快路径直接拒，连文件都不碰。
        let d = w.deliver(3, u64::MAX, u32::MAX).unwrap();
        assert!(d.records.is_empty());

        // `sync` 之后同一条就可给了：送不送只由水位决定，不由谁读过决定。
        w.sync().unwrap();
        let d = w.deliver(3, u64::MAX, u32::MAX).unwrap();
        assert_eq!(d.records, vec![c]);
    }

    /// 窗口比一批记录短的时候必须还能逐条给对 —— 这是 `Scan` 那个滑动窗口
    /// 存在的全部理由，也是它最容易被改坏的地方（窗口起点错位一格，
    /// 后面每一条的 CRC 都对不上）。
    #[test]
    fn deliver_slides_the_window_across_many_records() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        // 记录名长度递增，让记录边界与窗口边界不周期性对齐。
        let payloads: Vec<Vec<u8>> = (1..=20_000usize)
            .map(|i| {
                let mut name = vec![b'x'; i % 97];
                name.extend_from_slice(i.to_string().as_bytes());
                log_payload(&put_view(&name, i as u32))
            })
            .collect();
        let mut w = Wal::open(&p).unwrap().0;
        w.commit(&payloads).unwrap();
        let bytes: usize = payloads.iter().map(|x| x.len()).sum();
        assert!(
            bytes > 4 * SCAN_WINDOW,
            "这一条要真的跨过多个窗口，否则测不到重点：{bytes}"
        );

        // 整批一次要完：扫到底、条数对、最后一条也对。
        let d = w.deliver(1, u64::MAX, u32::MAX).unwrap();
        assert_eq!(d.records.len(), payloads.len());
        assert_eq!(d.records, payloads, "滑窗把记录读串了");
        assert_eq!(d.next_off, w.logical_size().unwrap());

        // 再按中段随机要一次：不走从头扫的那条路也能落在正确边界上。
        let mid = 13_777usize;
        let mut off = 0u64;
        for pl in &payloads[..mid - 1] {
            off += HDR as u64 + pl.len() as u64;
        }
        let d = w.deliver(mid as u64, off, 1 << 20).unwrap();
        assert_eq!(d.base_seq, mid as u64);
        assert_eq!(d.records[0], payloads[mid - 1]);
        assert!(!d.records.is_empty());
    }

    /// `max_bytes` 是软上限：到线就停，但至少要给一条 —— 一条都不给的话，
    /// 副本的游标永远推不动，一个最小的请求值就能把它钉死在原地。
    #[test]
    fn deliver_honors_max_bytes_but_always_gives_at_least_one() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.wal");
        let payloads: Vec<Vec<u8>> = (1..=5u8)
            .map(|i| log_payload(&put_view(&[b'a' + i; 40], i as u32)))
            .collect();
        let mut w = Wal::open(&p).unwrap().0;
        w.commit(&payloads).unwrap();

        // 1 字节上限：仍旧交一条。
        let d = w.deliver(1, u64::MAX, 1).unwrap();
        assert_eq!(d.records.len(), 1, "上限再小也得往前走一格");

        // 逐轮把 `next_off` 回送，应当恰好吃完整批，一轮不多一轮不少。
        let (mut seq, mut hint, mut got) = (1u64, u64::MAX, 0usize);
        for _ in 0..10 {
            let d = w.deliver(seq, hint, payloads[0].len() as u32 * 2).unwrap();
            if d.records.is_empty() {
                break;
            }
            got += d.records.len();
            for rec in &d.records {
                assert_eq!(*rec, payloads[seq as usize - 1]);
                seq += 1;
            }
            hint = d.next_off;
        }
        assert_eq!(got, payloads.len(), "按 max_bytes 分轮不该丢也不该重");
        assert_eq!(seq, payloads.len() as u64 + 1);
    }
}
