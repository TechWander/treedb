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
    let t = crc_table();
    let mut c = 0xFFFF_FFFFu32;
    for &b in buf {
        c = t[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

/// 把一条 `Log` 序列化成 payload 字节。
pub fn log_payload(log: &Log) -> Vec<u8> {
    let mut w = W::new();
    log.encode(&mut w);
    w.finish()
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

    pub fn sync(&mut self) -> Result<()> {
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
}
