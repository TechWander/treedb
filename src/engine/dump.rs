//! 离线 dump / load：把一份数据目录导出成一个自描述文件，再从它重建目录。
//!
//! **为什么不是「拷 `state.snap`」**：快照是内部格式，读它的人必须跟着
//! `SNAP_VERSION` 走，跨代还要 `treedb-migrate`。dump 走的是**逻辑记录流**
//! （[`Log`]），只依赖日志标签（1..=27，历史上只追加）。容器与纯形状变更那五条
//! 不进 dump：`LOG_BATCH` 23 的效果是它包住的那些子日志，`LOG_ADD_COLUMN` 24 /
//! `LOG_CLEAR_TABLE` 25 / `LOG_DEL_RANGE` 26 / `LOG_RENAME_TABLE` 27 的效果则分别是
//! 追加后的整张形状、清空后的空表、删完后的剩余行、改名后的新名 —— 全都由
//! `dump` 从终态直接导出：同版本内可以跨机器、
//! 跨目录搬，也能被人用 `xxd` 一条条看懂。
//!
//! **契约**（与 `core::dump_dir` / `core::restore_dump` 一起成立）：
//!
//! - 两个动作都要求目录**独占**（持 `.lock`），服务在跑时直接拒。
//! - dump 读「快照 + 能重放到的全部 WAL」，与 [`Engine::open`](crate::engine::Engine::open)
//!   的恢复语义一致；所以它也会带上未 durable 的记录 —— 那是磁盘上真实存在的事实。
//! - load 只写 `state.snap`（水位 = dump 里的 seq），不写 WAL：目标目录里
//!   「快照之后没有任何写」是这句话的字面意思。
//! - load **不搬 `fence` 令牌**：那是部署态（谁是当前主），不是逻辑数据。源目录
//!   若是提升过的副本，导入后的目录按新主（fence 0）起步，需要 fencing 的拓扑
//!   由调用方按 §6.6 重新发号，而不是让一次 dump 悄悄把旧权威带过来。
//! - 两个 `next_*` 计数器不由任何 [`Log`] 表达，单独放在容器头上：漏掉它们会让
//!   下一张表复用已用过的 id。
//!
//! 导出是确定性的：同一份状态导出两次字节相同（BTreeMap 序 + 固定段落序），
//! 这条性质被 `dump → restore → dump` 的往返测试当成判据。

use std::path::Path;

use crate::engine::codec::{Rd, W};
use crate::engine::mem::{Log, State};
use crate::engine::model::ViewId;
use crate::engine::wal::crc32;
use crate::error::{Error, Result};

/// 容器魔数（8 字节）。第二个字符起是给人看的，末位是格式代。
pub const DUMP_MAGIC: &[u8] = b"TDBDUMP2";

/// 容器版本。日志标签或容器字段变了就 +1，读侧对未知版本 fail-closed。
///
/// v1 → v2（2026-11-23，§11 第 10 条 M4）：容器头少一个 `next_audit_seq`
/// 计数器，四张专用控制面表不再导出。旧 dump 拆不了这四张表，只能用旧
/// 二进制 `treedb-load` 进去再走 `control-migrate` —— 读侧不猜，直接拒。
pub const DUMP_VERSION: u32 = 2;

/// 一份完整的逻辑导出。
///
/// `logs` 是「把空状态重放成源状态」的**最小重建序列**（只含 Put* 与少数
/// 需要的删除语义，不含历史 Purge/Del）：它是终态的描述，不是写入历史。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dump {
    /// 源目录的 seq 水位，导入后写成快照尾巴。
    pub seq: u64,
    /// 下一个 view id。`Log` 里没有它的位置，见模块文档。
    pub next_view_id: ViewId,
    /// 下一个通用表 id。
    pub next_table_id: u32,
    pub logs: Vec<Log>,
}

/// [`Dump::counts`] 的返回值。字段名对齐 `treedb-server --stats` 与
/// `treedb-migrate` 的体检输出。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DumpCounts {
    pub views: usize,
    pub nodes: usize,
    pub dentries: usize,
    pub locks: usize,
    pub refs: usize,
    pub tables: usize,
    pub rows: usize,
}

impl Dump {
    /// 从一份状态生成导出（`seq` 是这份状态对应的 WAL 水位）。
    pub fn of_state(state: &State, seq: u64) -> Dump {
        Dump {
            seq,
            next_view_id: state.next_view_id,
            next_table_id: state.next_table_id,
            logs: state_to_logs(state),
        }
    }

    /// 把导出重建成状态。
    ///
    /// 两个 `next_*` 取「重放结果」与「容器头」的较大者：正常 dump 两者相等，
    /// 取 max 只为一件事 —— 手改过的 dump 不能让 id 回绕、把已存在的对象覆盖掉。
    pub fn state(&self) -> State {
        let mut s = State::new();
        for l in &self.logs {
            s.apply(l);
        }
        s.next_view_id = s.next_view_id.max(self.next_view_id);
        s.next_table_id = s.next_table_id.max(self.next_table_id);
        s
    }

    /// 编码成自描述容器：魔数、版本、两个计数器、条数、逐条记录、尾部 CRC32。
    ///
    /// CRC 覆盖到条数为止的全部字节（含魔数），尾部 4 字节是它自己 —— 与快照
    /// 尾巴、WAL 帧同一个口径：坏了就拒，不猜。
    pub fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        w.bytes(DUMP_MAGIC);
        w.u32(DUMP_VERSION);
        w.u64(self.seq);
        w.u32(self.next_view_id);
        w.u32(self.next_table_id);
        w.u64(self.logs.len() as u64);
        for l in &self.logs {
            let mut body = W::new();
            l.encode(&mut body);
            w.bytes(&body.0);
        }
        let mut out = w.finish();
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(raw: &[u8]) -> Result<Dump> {
        if raw.len() < 4 {
            return Err(Error::Codec("dump shorter than its crc tail".into()));
        }
        let (body, tail) = raw.split_at(raw.len() - 4);
        let want = u32::from_le_bytes(tail.try_into().expect("4-byte slice"));
        if crc32(body) != want {
            return Err(Error::Codec(format!(
                "dump crc mismatch: header/payload say {want:08x}, body is {:08x}",
                crc32(body)
            )));
        }
        let mut r = Rd::new(body);
        let magic = r.bytes()?;
        if magic != DUMP_MAGIC {
            return Err(Error::Codec(format!(
                "not a treedb dump: magic {:?}",
                String::from_utf8_lossy(&magic)
            )));
        }
        let version = r.u32()?;
        if version != DUMP_VERSION {
            return Err(Error::Codec(format!(
                "dump container version {version} is not {DUMP_VERSION}: this build cannot read it"
            )));
        }
        let seq = r.u64()?;
        let next_view_id = r.u32()?;
        let next_table_id = r.u32()?;
        let count = r.u64()?;
        // 每条记录至少 1 字节（tag）：条数超过剩余字节数一定是坏头，先拒掉，
        // 免得 `Vec::with_capacity` 按一个天文数字去要内存。
        if count > r.remaining() as u64 {
            return Err(Error::Codec(format!(
                "dump claims {count} record(s) in {} remaining byte(s)",
                r.remaining()
            )));
        }
        let mut logs = Vec::with_capacity(count as usize);
        for i in 0..count {
            let rec = r.bytes_ref()?;
            let log = Log::decode(&mut Rd::new(rec)).map_err(|e| {
                Error::Codec(format!("dump record {i} does not decode: {e}"))
            })?;
            logs.push(log);
        }
        if r.remaining() != 0 {
            return Err(Error::Codec(format!(
                "dump has {} trailing byte(s) after the last record",
                r.remaining()
            )));
        }
        Ok(Dump {
            seq,
            next_view_id,
            next_table_id,
            logs,
        })
    }

    /// 给人看的规模：views / nodes / dentries / locks / refs / tables / rows。
    pub fn counts(&self) -> DumpCounts {
        let s = self.state();
        DumpCounts {
            views: s.views.len(),
            nodes: s.nodes.len(),
            dentries: s.dentry_count(),
            locks: s.locks.len(),
            refs: s.refs.len(),
            tables: s.tables.len(),
            rows: s.row_count(),
        }
    }

    /// 两个工具（`treedb-dump` / `treedb-load`）共用的一行摘要。
    ///
    /// 放在库里而不是各写一份：两个命令的输出现场经常并排贴，格式一旦
    /// 分头演化，人眼对齐就会开始骗人。
    pub fn summary(&self) -> String {
        let c = self.counts();
        format!(
            "seq {} logs {} | views {} nodes {} dentries {} locks {} refs {} \
             tables {} rows {}",
            self.seq,
            self.logs.len(),
            c.views,
            c.nodes,
            c.dentries,
            c.locks,
            c.refs,
            c.tables,
            c.rows
        )
    }

    pub fn write_file(&self, path: &Path) -> Result<u64> {
        let buf = self.encode();
        let tmp = path.with_extension("dump.tmp");
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            use std::io::Write;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        if let Some(d) = path.parent() {
            if let Ok(d) = std::fs::File::open(d) {
                let _ = d.sync_all();
            }
        }
        Ok(buf.len() as u64)
    }

    pub fn read_file(path: &Path) -> Result<Dump> {
        let raw = std::fs::read(path)?;
        Dump::decode(&raw)
    }
}

/// 把终态展开成重建序列。
///
/// 顺序不是任选的，三条硬约束：
///
/// 1. **视图先于节点与目录项**：目录项只带 `view` id，重放时要在视图表里反查。
/// 2. **节点先于目录项**：`PutDentry` 的 apply 用 `nodes` 推出冗余的 `kind`；
///    反过来会让每条目录项落成 `DENT_KIND_UNKNOWN`。
/// 3. **表先于行**：行的二级索引由 apply 按表的 `def` 重建，def 不在就索引空。
///
/// 其余各段之间无依赖，按 BTreeMap 的键序输出 —— 导出因此是确定性的。
pub fn state_to_logs(s: &State) -> Vec<Log> {
    let mut out = Vec::new();
    for (name, meta) in &s.views {
        out.push(Log::PutView {
            name: name.clone(),
            meta: meta.clone(),
        });
    }
    for ((view, _ino), node) in &s.nodes {
        out.push(Log::PutNode {
            view: *view,
            node: node.clone(),
        });
    }
    for ((view, parent), inner) in &s.dentries {
        for (name, dent) in inner {
            out.push(Log::PutDentry {
                view: *view,
                parent: *parent,
                name: name.clone(),
                ino: dent.ino,
            });
        }
    }
    for ((view, ino, owner), rec) in &s.locks {
        out.push(Log::PutLock {
            view: *view,
            ino: *ino,
            owner: owner.clone(),
            rec: rec.clone(),
        });
    }
    for (data_ref, rec) in &s.refs {
        // 零票行在状态里本来就不存在（apply 会删）；这里再挡一道，免得
        // 手改过的状态导出一条「把账本清掉」的记录。
        if rec.count > 0 {
            out.push(Log::PutRef {
                data_ref: data_ref.clone(),
                rec: rec.clone(),
            });
        }
    }
    // `tables` 按名字排序，id 是另一维：id 小的先建，重放结果与源一致
    // （`next_table_id` 还会被容器头再校一次）。
    let mut tables: Vec<_> = s.tables.values().collect();
    tables.sort_by_key(|m| m.id);
    for m in tables {
        out.push(Log::PutTable {
            id: m.id,
            def: m.def.clone(),
        });
    }
    for (table, inner) in &s.rows {
        for row in inner.values() {
            out.push(Log::PutRow {
                table: *table,
                row: row.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::model::{
        ColumnDef, LockRec, Node, RefRec, RowRec, TableDef, ViewMeta, COL_UNIQUE,
    };
    use crate::engine::model::Kind;

    fn node(ino: u64, parent: u64, name: &[u8], kind: Kind) -> Node {
        Node {
            ino,
            parent,
            name: name.to_vec(),
            kind,
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 7,
            nlink: 1,
            atime: 1,
            mtime: 2,
            ctime: 3,
            data_ref: match kind {
                Kind::File => b"r1".to_vec(),
                _ => Vec::new(),
            },
            link: Vec::new(),
        }
    }

    /// 造一份「每一段都非空」的状态：少导出一段，这里就会红。
    fn rich_state() -> State {
        let mut s = State::new();
        s.apply(&Log::PutView {
            name: b"v1".to_vec(),
            meta: ViewMeta {
                id: 1,
                root: 1,
                next_ino: 3,
            },
        });
        s.apply(&Log::PutNode {
            view: 1,
            node: node(2, 1, b"f", Kind::File),
        });
        s.apply(&Log::PutNode {
            view: 1,
            node: node(1, 1, b"", Kind::Dir),
        });
        s.apply(&Log::PutDentry {
            view: 1,
            parent: 1,
            name: b"f".to_vec(),
            ino: 2,
        });
        s.apply(&Log::PutLock {
            view: 1,
            ino: 2,
            owner: b"o1".to_vec(),
            rec: LockRec {
                kind: 1,
                lease_expiry: 60_000,
            },
        });
        s.apply(&Log::PutRef {
            data_ref: b"r1".to_vec(),
            rec: RefRec { count: 1, bytes: 7 },
        });
        s.apply(&Log::PutTable {
            id: 1,
            def: TableDef {
                name: b"t1".to_vec(),
                columns: vec![
                    ColumnDef {
                        name: b"k".to_vec(),
                        flags: COL_UNIQUE,
                    },
                    ColumnDef {
                        name: b"v".to_vec(),
                        flags: 0,
                    },
                ],
            },
        });
        s.apply(&Log::PutRow {
            table: 1,
            row: RowRec {
                key: b"pk".to_vec(),
                cols: vec![b"pk".to_vec(), b"val".to_vec()],
            },
        });
        s
    }

    #[test]
    fn roundtrip_rebuilds_every_field_of_the_state() {
        let s = rich_state();
        let d = Dump::of_state(&s, 42);
        let back = d.state();
        assert_eq!(back, s, "dump 重放后的状态必须与源逐字段相等");
        assert_eq!(d.seq, 42);
        assert_eq!(back.next_view_id, s.next_view_id);
        assert_eq!(back.next_table_id, s.next_table_id);
    }

    /// 两个 `next_*` 计数器没有对应的 [`Log`]：只能靠容器头保住。
    #[test]
    fn container_keeps_next_counters_that_no_log_can_express() {
        let mut s = State::new();
        s.next_view_id = 9;
        s.next_table_id = 4;
        let d = Dump::of_state(&s, 7);
        assert!(d.logs.is_empty());
        let back = d.state();
        assert_eq!(back.next_view_id, 9);
        assert_eq!(back.next_table_id, 4);
    }

    #[test]
    fn encode_decode_is_byte_stable_and_rejects_corruption() {
        let d = Dump::of_state(&rich_state(), 42);
        let a = d.encode();
        let b = Dump::of_state(&rich_state(), 42).encode();
        assert_eq!(a, b, "同一份状态导出的字节必须相同");
        assert_eq!(Dump::decode(&a).unwrap(), d);

        let mut bad = a.clone();
        let n = bad.len();
        bad[n - 6] ^= 0xFF;
        let e = Dump::decode(&bad).expect_err("CRC 坏了必须拒");
        assert!(e.to_string().contains("crc"), "{e}");

        let e = Dump::decode(b"TDBDUMP2").expect_err("短到没有尾巴也要拒");
        assert!(e.to_string().contains("crc"), "{e}");
    }

    #[test]
    fn unknown_container_version_is_refused_by_name() {
        let d = Dump::of_state(&rich_state(), 1);
        let mut raw = d.encode();
        // 魔数(4B 长度前缀 + 8B) 之后的 4 字节是版本位。
        raw[12..16].copy_from_slice(&(DUMP_VERSION + 1).to_le_bytes());
        let crc = crc32(&raw[..raw.len() - 4]);
        let n = raw.len();
        raw[n - 4..].copy_from_slice(&crc.to_le_bytes());
        let e = Dump::decode(&raw).expect_err("未知版本必须拒");
        assert!(e.to_string().contains("version"), "{e}");
    }
}
