//! 引擎内存状态机：全部元数据的唯一权威布局。
//!
//! **设计立场（诚实边界）**：v0.2 采用「全内存状态 + WAL 重放 + 快照
//! checkpoint」——这让它的数据结构直接是索引形状（BTreeMap），op 延迟
//! 不受页缓存/索引维护拖累，恢复靠 WAL 前缀。代价是活元数据总量受内存
//! 约束（单节点约 200 B，10 亿 inode ≈ 200 GB）；页式 B+tree 落盘列为
//! 阶段 2 的目标是把万级视图（10^7 级 inode）这一档先吃透。
//!
//! 三张核心索引，形状与关系库版的三张表一致：
//! - `nodes: (view, ino) → Node`            ← `node` 表主键
//! - `dentries: (view, parent, name) → ino` ← `node(parent_id,name)` 唯一索引
//!   （key 的字典序 = 同目录内 name 的字节序，readdir 顺序天然稳定）
//! - `locks: (view, ino, owner) → LockRec`  ← `locks` 表
//!
//! `refs` 是跨视图共享的内容账本（对应那个独立的 refs schema）；
//! `keys`/`audit`/`defs`/`placement` 是控制面（对应那个 control schema）。

use crate::engine::codec::{Rd, W};
use crate::engine::model::{
    AuditRec, DataRef, DefRec, Ino, KeyRec, LockRec, Node, RefRec, ViewId, ViewMeta,
};
use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::ops::Bound;

/// 快照格式版本。布局变更必须 +1，`load` 拒绝不匹配的旧文件。
///
/// v1 → v2：`data_ref` 从 `u64` 改成不透明字节（格式定案，见
/// `model::DataRef`）。inode 行与 `refs` 账本行的字段宽度跟着变，旧快照不能被
/// 新代码重解 —— 不 +1 就会把旧文件的 8 字节整数当长度前缀读，CRC 保护不了
/// 已经存好的 payload 之外的东西，于是“静默解错”而不是“报错拒收”。
pub const SNAP_VERSION: u32 = 2;

/// 已提交（WAL 已 durable）的一条状态变更。重放 = 从头到尾 `apply`，
/// 因此每条都必须是幂等的整行写入，不记 delta。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Log {
    PutView {
        name: Vec<u8>,
        meta: ViewMeta,
    },
    /// 视图整体下线：按 view id 级联清 nodes/dentries/locks。
    PurgeView {
        name: Vec<u8>,
        view: ViewId,
    },
    PutNode {
        view: ViewId,
        node: Node,
    },
    DelNode {
        view: ViewId,
        ino: Ino,
    },
    PutDentry {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
        ino: Ino,
    },
    DelDentry {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
    },
    PutLock {
        view: ViewId,
        ino: Ino,
        owner: Vec<u8>,
        rec: LockRec,
    },
    DelLock {
        view: ViewId,
        ino: Ino,
        owner: Vec<u8>,
    },
    PutRef {
        data_ref: DataRef,
        rec: RefRec,
    },
    DelRef {
        data_ref: DataRef,
    },
    PutKey {
        key: KeyRec,
    },
    DelKey {
        id: Vec<u8>,
    },
    PutAudit {
        row: AuditRec,
    },
    /// 审计保留窗口裁剪：丢掉 seq <= 此值的行。
    TrimAudit {
        keep_from: u64,
    },
    PutDef {
        def: DefRec,
    },
    DelDef {
        name: Vec<u8>,
    },
    PutPlace {
        view: Vec<u8>,
        instance: Vec<u8>,
    },
    DelPlace {
        view: Vec<u8>,
    },
}

const LOG_PUT_VIEW: u8 = 1;
const LOG_PURGE_VIEW: u8 = 2;
const LOG_PUT_NODE: u8 = 3;
const LOG_DEL_NODE: u8 = 4;
const LOG_PUT_DENTRY: u8 = 5;
const LOG_DEL_DENTRY: u8 = 6;
const LOG_PUT_LOCK: u8 = 7;
const LOG_DEL_LOCK: u8 = 8;
const LOG_PUT_REF: u8 = 9;
const LOG_DEL_REF: u8 = 10;
const LOG_PUT_KEY: u8 = 11;
const LOG_DEL_KEY: u8 = 12;
const LOG_PUT_AUDIT: u8 = 13;
const LOG_TRIM_AUDIT: u8 = 14;
const LOG_PUT_DEF: u8 = 15;
const LOG_DEL_DEF: u8 = 16;
const LOG_PUT_PLACE: u8 = 17;
const LOG_DEL_PLACE: u8 = 18;

impl Log {
    pub fn encode(&self, w: &mut W) {
        match self {
            Log::PutView { name, meta } => {
                w.u8(LOG_PUT_VIEW);
                w.bytes(name);
                meta.encode(w);
            }
            Log::PurgeView { name, view } => {
                w.u8(LOG_PURGE_VIEW);
                w.bytes(name);
                w.u32(*view);
            }
            Log::PutNode { view, node } => {
                w.u8(LOG_PUT_NODE);
                w.u32(*view);
                node.encode(w);
            }
            Log::DelNode { view, ino } => {
                w.u8(LOG_DEL_NODE);
                w.u32(*view);
                w.u64(*ino);
            }
            Log::PutDentry {
                view,
                parent,
                name,
                ino,
            } => {
                w.u8(LOG_PUT_DENTRY);
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
                w.u64(*ino);
            }
            Log::DelDentry { view, parent, name } => {
                w.u8(LOG_DEL_DENTRY);
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
            }
            Log::PutLock {
                view,
                ino,
                owner,
                rec,
            } => {
                w.u8(LOG_PUT_LOCK);
                w.u32(*view);
                w.u64(*ino);
                w.bytes(owner);
                rec.encode(w);
            }
            Log::DelLock { view, ino, owner } => {
                w.u8(LOG_DEL_LOCK);
                w.u32(*view);
                w.u64(*ino);
                w.bytes(owner);
            }
            Log::PutRef { data_ref, rec } => {
                w.u8(LOG_PUT_REF);
                w.bytes(data_ref);
                rec.encode(w);
            }
            Log::DelRef { data_ref } => {
                w.u8(LOG_DEL_REF);
                w.bytes(data_ref);
            }
            Log::PutKey { key } => {
                w.u8(LOG_PUT_KEY);
                key.encode(w);
            }
            Log::DelKey { id } => {
                w.u8(LOG_DEL_KEY);
                w.bytes(id);
            }
            Log::PutAudit { row } => {
                w.u8(LOG_PUT_AUDIT);
                row.encode(w);
            }
            Log::TrimAudit { keep_from } => {
                w.u8(LOG_TRIM_AUDIT);
                w.u64(*keep_from);
            }
            Log::PutDef { def } => {
                w.u8(LOG_PUT_DEF);
                def.encode(w);
            }
            Log::DelDef { name } => {
                w.u8(LOG_DEL_DEF);
                w.bytes(name);
            }
            Log::PutPlace { view, instance } => {
                w.u8(LOG_PUT_PLACE);
                w.bytes(view);
                w.bytes(instance);
            }
            Log::DelPlace { view } => {
                w.u8(LOG_DEL_PLACE);
                w.bytes(view);
            }
        }
    }

    pub fn decode(r: &mut Rd) -> Result<Log> {
        let t = r.u8()?;
        let log = match t {
            LOG_PUT_VIEW => Log::PutView {
                name: r.bytes()?,
                meta: ViewMeta::decode(r)?,
            },
            LOG_PURGE_VIEW => Log::PurgeView {
                name: r.bytes()?,
                view: r.u32()?,
            },
            LOG_PUT_NODE => Log::PutNode {
                view: r.u32()?,
                node: Node::decode(r)?,
            },
            LOG_DEL_NODE => Log::DelNode {
                view: r.u32()?,
                ino: r.u64()?,
            },
            LOG_PUT_DENTRY => Log::PutDentry {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
                ino: r.u64()?,
            },
            LOG_DEL_DENTRY => Log::DelDentry {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
            },
            LOG_PUT_LOCK => Log::PutLock {
                view: r.u32()?,
                ino: r.u64()?,
                owner: r.bytes()?,
                rec: LockRec::decode(r)?,
            },
            LOG_DEL_LOCK => Log::DelLock {
                view: r.u32()?,
                ino: r.u64()?,
                owner: r.bytes()?,
            },
            LOG_PUT_REF => Log::PutRef {
                data_ref: r.bytes()?,
                rec: RefRec::decode(r)?,
            },
            LOG_DEL_REF => Log::DelRef { data_ref: r.bytes()? },
            LOG_PUT_KEY => Log::PutKey {
                key: KeyRec::decode(r)?,
            },
            LOG_DEL_KEY => Log::DelKey { id: r.bytes()? },
            LOG_PUT_AUDIT => Log::PutAudit {
                row: AuditRec::decode(r)?,
            },
            LOG_TRIM_AUDIT => Log::TrimAudit {
                keep_from: r.u64()?,
            },
            LOG_PUT_DEF => Log::PutDef {
                def: DefRec::decode(r)?,
            },
            LOG_DEL_DEF => Log::DelDef { name: r.bytes()? },
            LOG_PUT_PLACE => Log::PutPlace {
                view: r.bytes()?,
                instance: r.bytes()?,
            },
            LOG_DEL_PLACE => Log::DelPlace { view: r.bytes()? },
            x => return Err(Error::Codec(format!("unknown log tag {x}"))),
        };
        Ok(log)
    }
}

/// 引擎全量状态。单写者事件循环独占，无锁。
///
/// `PartialEq` 是给崩溃恢复测试用的：`snapshot + WAL 重放` 后的状态必须与
/// 崩溃前的内存状态逐字节相等，任何不等都是恢复路径的 bug。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct State {
    pub views: BTreeMap<Vec<u8>, ViewMeta>,
    /// 视图 id → 名字。目录树 op 只带 id，正向表反查要扫全表，
    /// 万级视图下每条 op 扫一次不可接受，故与 `views` 同批维护。
    pub view_by_id: BTreeMap<ViewId, Vec<u8>>,
    /// 下一个可分配的 view id。
    pub next_view_id: ViewId,
    pub nodes: BTreeMap<(ViewId, Ino), Node>,
    pub dentries: BTreeMap<(ViewId, Ino, Vec<u8>), Ino>,
    pub locks: BTreeMap<(ViewId, Ino, Vec<u8>), LockRec>,
    pub refs: BTreeMap<DataRef, RefRec>,
    pub keys: BTreeMap<Vec<u8>, KeyRec>,
    pub audit: BTreeMap<u64, AuditRec>,
    pub defs: BTreeMap<Vec<u8>, DefRec>,
    pub placement: BTreeMap<Vec<u8>, Vec<u8>>,
    /// 下一个可分配的 audit seq。
    pub next_audit_seq: u64,
}

impl State {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, log: &Log) {
        match log {
            Log::PutView { name, meta } => {
                self.view_by_id.insert(meta.id, name.clone());
                if meta.id >= self.next_view_id {
                    self.next_view_id = meta.id + 1;
                }
                self.views.insert(name.clone(), meta.clone());
            }
            Log::PurgeView { name, view } => {
                self.views.remove(name);
                self.purge_view(*view);
            }
            Log::PutNode { view, node } => {
                self.nodes.insert((*view, node.ino), node.clone());
            }
            Log::DelNode { view, ino } => {
                self.nodes.remove(&(*view, *ino));
            }
            Log::PutDentry {
                view,
                parent,
                name,
                ino,
            } => {
                self.dentries.insert((*view, *parent, name.clone()), *ino);
            }
            Log::DelDentry { view, parent, name } => {
                self.dentries.remove(&(*view, *parent, name.clone()));
            }
            Log::PutLock {
                view,
                ino,
                owner,
                rec,
            } => {
                self.locks.insert((*view, *ino, owner.clone()), rec.clone());
            }
            Log::DelLock { view, ino, owner } => {
                self.locks.remove(&(*view, *ino, owner.clone()));
            }
            Log::PutRef { data_ref, rec } => {
                if rec.count == 0 {
                    self.refs.remove(data_ref);
                } else {
                    self.refs.insert(data_ref.clone(), *rec);
                }
            }
            Log::DelRef { data_ref } => {
                self.refs.remove(data_ref);
            }
            Log::PutKey { key } => {
                self.keys.insert(key.id.clone(), key.clone());
                // 身份表按 id 单调，seq 由 audit 行自带。
            }
            Log::DelKey { id } => {
                self.keys.remove(id);
            }
            Log::PutAudit { row } => {
                if row.seq >= self.next_audit_seq {
                    self.next_audit_seq = row.seq + 1;
                }
                self.audit.insert(row.seq, row.clone());
            }
            Log::TrimAudit { keep_from } => {
                let dead: Vec<u64> = self
                    .audit
                    .range(..*keep_from)
                    .map(|(k, _)| *k)
                    .collect();
                for k in dead {
                    self.audit.remove(&k);
                }
            }
            Log::PutDef { def } => {
                self.defs.insert(def.name.clone(), def.clone());
            }
            Log::DelDef { name } => {
                self.defs.remove(name);
            }
            Log::PutPlace { view, instance } => {
                self.placement.insert(view.clone(), instance.clone());
            }
            Log::DelPlace { view } => {
                self.placement.remove(view);
            }
        }
    }

    fn purge_view(&mut self, view: ViewId) {
        let dead_nodes: Vec<(u32, u64)> = self
            .nodes
            .range((view, u64::MIN)..=(view, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for k in dead_nodes {
            self.nodes.remove(&k);
        }
        // 前缀扫的上界不能造「最大名字」（`vec![0xff; usize::MAX]` 会
        // capacity overflow 直接 panic），改用 Unbounded + take_while：
        // key 以 view 打头，遇到下一个 view 就停，复杂度与命中行数成正比。
        let dead_dent: Vec<(u32, u64, Vec<u8>)> = self
            .dentries
            .range((Bound::Included((view, u64::MIN, Vec::new())), Bound::Unbounded))
            .take_while(move |(k, _)| k.0 == view)
            .map(|(k, _)| k.clone())
            .collect();
        for k in dead_dent {
            self.dentries.remove(&k);
        }
        let dead_locks: Vec<(u32, u64, Vec<u8>)> = self
            .locks
            .range((Bound::Included((view, u64::MIN, Vec::new())), Bound::Unbounded))
            .take_while(move |(k, _)| k.0 == view)
            .map(|(k, _)| k.clone())
            .collect();
        for k in dead_locks {
            self.locks.remove(&k);
        }
        self.view_by_id.remove(&view);
    }

    /// `(view, parent, *)` 区间内、`after` 之后的至多 `limit` 条 dentry。
    /// key 的字典序即 name 的字节序：`(view, parent, name)` 元组比较
    /// 与 `(parent, name)` 目录内排序等价。`after` 为空 = 从头开始。
    pub fn read_dir(&self, view: ViewId, parent: Ino, after: &[u8], limit: usize) -> Vec<(Vec<u8>, Ino)> {
        let start = (view, parent, after.to_vec());
        // after 是「上一页最后一个名字」，用 Excluded 跳过它本身。
        let lo = if after.is_empty() {
            Bound::Included(start.clone())
        } else {
            Bound::Excluded(start)
        };
        self.dentries
            .range((lo, Bound::Unbounded))
            .take_while(move |k| k.0 .0 == view && k.0 .1 == parent)
            .take(limit)
            .map(|(k, ino)| (k.2.clone(), *ino))
            .collect()
    }

    /// 某目录是否为空（readdir 不含 `.`/`..`，故只看子 dentry）。
    pub fn dir_empty(&self, view: ViewId, parent: Ino) -> bool {
        self.dentries
            .range((Bound::Included((view, parent, Vec::new())), Bound::Unbounded))
            .take_while(|k| k.0 .0 == view && k.0 .1 == parent)
            .next()
            .is_none()
    }

    pub fn live_view_count(&self) -> usize {
        self.views.len()
    }

    // ---- 快照 ----

    pub fn save(&self, w: &mut W) {
        w.u32(SNAP_VERSION);
        w.u32(self.views.len() as u32);
        for (name, meta) in &self.views {
            w.bytes(name);
            meta.encode(w);
        }
        w.u32(self.next_view_id);
        w.u32(self.nodes.len() as u32);
        for ((view, _), node) in &self.nodes {
            w.u32(*view);
            node.encode(w);
        }
        w.u32(self.dentries.len() as u32);
        for ((view, parent, name), ino) in &self.dentries {
            w.u32(*view);
            w.u64(*parent);
            w.bytes(name);
            w.u64(*ino);
        }
        w.u32(self.locks.len() as u32);
        for ((view, ino, owner), rec) in &self.locks {
            w.u32(*view);
            w.u64(*ino);
            w.bytes(owner);
            rec.encode(w);
        }
        w.u32(self.refs.len() as u32);
        for (d, rec) in &self.refs {
            w.bytes(d);
            rec.encode(w);
        }
        w.u32(self.keys.len() as u32);
        for key in self.keys.values() {
            key.encode(w);
        }
        w.u32(self.audit.len() as u32);
        for row in self.audit.values() {
            row.encode(w);
        }
        w.u64(self.next_audit_seq);
        w.u32(self.defs.len() as u32);
        for def in self.defs.values() {
            def.encode(w);
        }
        w.u32(self.placement.len() as u32);
        for (view, inst) in &self.placement {
            w.bytes(view);
            w.bytes(inst);
        }
    }

    pub fn load(r: &mut Rd) -> Result<State> {
        let ver = r.u32()?;
        if ver != SNAP_VERSION {
            return Err(Error::Config(format!(
                "snapshot version {ver} unsupported (this build writes {SNAP_VERSION})"
            )));
        }
        let mut s = State::new();
        let n = r.u32()?;
        for _ in 0..n {
            let name = r.bytes()?;
            let meta = ViewMeta::decode(r)?;
            s.view_by_id.insert(meta.id, name.clone());
            s.views.insert(name, meta);
        }
        s.next_view_id = r.u32()?;
        let n = r.u32()?;
        for _ in 0..n {
            let view = r.u32()?;
            let node = Node::decode(r)?;
            let ino = node.ino;
            s.nodes.insert((view, ino), node);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let view = r.u32()?;
            let parent = r.u64()?;
            let name = r.bytes()?;
            let ino = r.u64()?;
            s.dentries.insert((view, parent, name), ino);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let view = r.u32()?;
            let ino = r.u64()?;
            let owner = r.bytes()?;
            let rec = LockRec::decode(r)?;
            s.locks.insert((view, ino, owner), rec);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let d = r.bytes()?;
            let rec = RefRec::decode(r)?;
            s.refs.insert(d, rec);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let key = KeyRec::decode(r)?;
            s.keys.insert(key.id.clone(), key);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let row = AuditRec::decode(r)?;
            s.audit.insert(row.seq, row);
        }
        s.next_audit_seq = r.u64()?;
        let n = r.u32()?;
        for _ in 0..n {
            let def = DefRec::decode(r)?;
            s.defs.insert(def.name.clone(), def);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let view = r.bytes()?;
            let inst = r.bytes()?;
            s.placement.insert(view, inst);
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            size: 0,
            nlink: 1,
            atime: 0,
            mtime: 0,
            ctime: 0,
            data_ref: Vec::new(),
            link: Vec::new(),
        }
    }

    #[test]
    fn log_codec_round_trip_all_variants() {
        let logs = vec![
            Log::PutView {
                name: b"alice".to_vec(),
                meta: ViewMeta {
                    id: 1,
                    root: 1,
                    next_ino: 2,
                },
            },
            Log::PurgeView {
                name: b"alice".to_vec(),
                view: 1,
            },
            Log::PutNode {
                view: 1,
                node: node(2, 1, b"a", Kind::File),
            },
            Log::DelNode { view: 1, ino: 2 },
            Log::PutDentry {
                view: 1,
                parent: 1,
                name: b"a".to_vec(),
                ino: 2,
            },
            Log::DelDentry {
                view: 1,
                parent: 1,
                name: b"a".to_vec(),
            },
            Log::PutLock {
                view: 1,
                ino: 2,
                owner: b"o".to_vec(),
                rec: LockRec {
                    kind: 1,
                    lease_expiry: -7,
                },
            },
            Log::DelLock {
                view: 1,
                ino: 2,
                owner: b"o".to_vec(),
            },
            Log::PutRef {
                data_ref: b"r5".to_vec(),
                rec: RefRec {
                    count: 2,
                    bytes: 100,
                },
            },
            Log::DelRef { data_ref: b"r5".to_vec() },
            Log::PutKey {
                key: KeyRec {
                    id: b"id".to_vec(),
                    label: b"l".to_vec(),
                    secret_hash: b"h".to_vec(),
                    created_at: 1,
                    last_used_at: 0,
                    revoked_at: 0,
                    view: Vec::new(),
                    ro: false,
                    owner: Vec::new(),
                    note: Vec::new(),
                    expires_at: 0,
                    created_by: Vec::new(),
                    secret_hint: Vec::new(),
                },
            },
            Log::DelKey { id: b"id".to_vec() },
            Log::PutAudit {
                row: AuditRec {
                    seq: 3,
                    ts: 1,
                    key_id: Vec::new(),
                    method: Vec::new(),
                    tool: Vec::new(),
                    ok: true,
                    detail: Vec::new(),
                },
            },
            Log::TrimAudit { keep_from: 3 },
            Log::PutDef {
                def: DefRec {
                    name: b"n".to_vec(),
                    kind: b"k".to_vec(),
                    payload: b"p".to_vec(),
                    updated_at: 1,
                },
            },
            Log::DelDef { name: b"n".to_vec() },
            Log::PutPlace {
                view: b"v".to_vec(),
                instance: b"i".to_vec(),
            },
            Log::DelPlace { view: b"v".to_vec() },
        ];
        for log in &logs {
            let mut w = W::new();
            log.encode(&mut w);
            let b = w.finish();
            let mut r = Rd::new(&b);
            let back = Log::decode(&mut r).unwrap();
            assert_eq!(*log, back);
            assert_eq!(r.remaining(), 0);
        }
    }

    #[test]
    fn apply_semantics_are_idempotent_upserts() {
        let mut s = State::new();
        let put = Log::PutNode {
            view: 1,
            node: node(2, 1, b"a", Kind::File),
        };
        s.apply(&put);
        s.apply(&put);
        assert_eq!(s.nodes.len(), 1);
        // count=0 的 ref 行自动消失（账本不留零行）。
        s.apply(&Log::PutRef {
            data_ref: b"r9".to_vec(),
            rec: RefRec {
                count: 0,
                bytes: 0,
            },
        });
        assert!(s.refs.is_empty());
        s.apply(&Log::PutRef {
            data_ref: b"r9".to_vec(),
            rec: RefRec {
                count: 1,
                bytes: 10,
            },
        });
        assert_eq!(s.refs.get(&b"r9".to_vec()).unwrap().count, 1);
    }

    #[test]
    fn purge_view_cascades_all_scoped_rows() {
        let mut s = State::new();
        s.apply(&Log::PutView {
            name: b"a".to_vec(),
            meta: ViewMeta {
                id: 1,
                root: 1,
                next_ino: 3,
            },
        });
        s.apply(&Log::PutNode {
            view: 1,
            node: node(1, 0, b"", Kind::Dir),
        });
        s.apply(&Log::PutNode {
            view: 1,
            node: node(2, 1, b"f", Kind::File),
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
            owner: b"o".to_vec(),
            rec: LockRec {
                kind: 1,
                lease_expiry: 0,
            },
        });
        // 另一个视图的行不能被牵连。
        s.apply(&Log::PutNode {
            view: 2,
            node: node(1, 0, b"", Kind::Dir),
        });
        s.apply(&Log::PurgeView {
            name: b"a".to_vec(),
            view: 1,
        });
        assert!(s.views.is_empty());
        assert_eq!(s.nodes.len(), 1);
        assert!(s.nodes.contains_key(&(2, 1)));
        assert!(s.dentries.is_empty());
        assert!(s.locks.is_empty());
    }

    #[test]
    fn trim_audit_drops_old_window() {
        let mut s = State::new();
        for i in 1..=5u64 {
            s.apply(&Log::PutAudit {
                row: AuditRec {
                    seq: i,
                    ts: i as i64,
                    key_id: Vec::new(),
                    method: Vec::new(),
                    tool: Vec::new(),
                    ok: true,
                    detail: Vec::new(),
                },
            });
        }
        assert_eq!(s.next_audit_seq, 6);
        s.apply(&Log::TrimAudit { keep_from: 4 });
        let keys: Vec<u64> = s.audit.keys().copied().collect();
        assert_eq!(keys, vec![4, 5]);
    }

    /// readdir 游标分页：按 name 字节序，`after` 本身不重复出现。
    #[test]
    fn read_dir_is_name_ordered_and_cursor_paginates() {
        let mut s = State::new();
        for (i, name) in [b"b".as_slice(), b"a", b"c", b"a.d"].iter().enumerate() {
            s.apply(&Log::PutDentry {
                view: 1,
                parent: 1,
                name: name.to_vec(),
                ino: 10 + i as u64,
            });
        }
        // 另一个目录/另一个视图的行不能漏进来。
        s.apply(&Log::PutDentry {
            view: 1,
            parent: 2,
            name: b"z".to_vec(),
            ino: 99,
        });
        s.apply(&Log::PutDentry {
            view: 2,
            parent: 1,
            name: b"z".to_vec(),
            ino: 98,
        });
        let all: Vec<Vec<u8>> = s
            .read_dir(1, 1, &[], 100)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        let exp: Vec<Vec<u8>> = vec![b"a".to_vec(), b"a.d".to_vec(), b"b".to_vec(), b"c".to_vec()];
        assert_eq!(all, exp);
        let p2 = s.read_dir(1, 1, b"a.d", 2);
        assert_eq!(
            p2.into_iter().map(|(n, _)| n).collect::<Vec<_>>(),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert!(s.read_dir(1, 1, b"c", 10).is_empty());
        assert!(s.dir_empty(1, 2) == false);
        assert!(s.dir_empty(1, 3));
    }

    /// 快照往返：node 的 (view, ino) 主键必须原样回来——早期版本用
    /// parent 低 32 位猜 view id，跨视图相同 ino 的行会互相覆盖。
    #[test]
    fn snapshot_round_trip_preserves_node_keys() {
        let mut s = State::new();
        for (view, ino) in [(1u32, 7u64), (2, 7), (2, 8)] {
            s.apply(&Log::PutNode {
                view,
                node: node(ino, 1, b"x", Kind::File),
            });
        }
        s.apply(&Log::PutView {
            name: b"a".to_vec(),
            meta: ViewMeta { id: 1, root: 1, next_ino: 9 },
        });
        let mut w = W::new();
        s.save(&mut w);
        let b = w.finish();
        let mut r = Rd::new(&b);
        let back = State::load(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(back.nodes.len(), 3);
        assert!(back.nodes.contains_key(&(1, 7)));
        assert!(back.nodes.contains_key(&(2, 7)));
        assert!(back.nodes.contains_key(&(2, 8)));
        assert_eq!(back.views, s.views);
    }
}
