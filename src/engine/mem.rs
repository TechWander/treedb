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
//! - `dentries: (view, parent) → { name → (ino, kind) }`
//!   ← `node(parent_id,name)` 唯一索引（内层 key 的字典序 = 同目录内 name 的
//!   字节序，readdir 顺序天然稳定；分两层是为了查找不用复制 name，见字段注释）
//! - `locks: (view, ino, owner) → LockRec`  ← `locks` 表
//!
//! `refs` 是跨视图共享的内容账本（对应那个独立的 refs schema）；
//! `keys`/`audit`/`defs`/`placement` 是历史遗留的控制面专用表；
//! `tables`/`rows`/`row_index` 是通用表：列名与语义由客户端定，引擎不解释值。

use crate::engine::codec::{Rd, W};
use crate::engine::model::{
    AuditRec, DataRef, DefRec, Ino, KeyRec, LockRec, Node, RefRec, RowRec, TableDef, ViewId,
    ViewMeta, MAX_TABLE_ITEMS,
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
///
/// v2 → v3：尾部多出通用表三段（`tables` / `next_table_id` / `rows`）。同样是
/// “不升版本就会把零字节当长度读”那一类，旧快照必须被拒而不是被解错。
///
/// 两代升级的**可迁移性不一样**，这决定了工具能做多少事：v1 → v2 改的是既有
/// 字段的宽度，没有「读旧写新」的稳妥路子；v2 → v3 只在尾部加了三段，前面逐
/// 字段一致，所以旧文件可以被当前代码完整解出再按新布局写出 ——
/// [`State::load_for_migration`] 与 `treedb-migrate`（§6.5）就建立在这个
/// 事实上，`legacy_snapshot_decodes_from_the_same_prefix` 钉住它。
pub const SNAP_VERSION: u32 = 3;

/// 上一个会被迁移工具接受的快照版本。**只此一个**：认识两代之前的布局等于把
/// 「格式变更要 +1 并给出门」这条规矩变成「格式变更可以攒着」。
pub const SNAP_LEGACY_VERSION: u32 = 2;

/// 快照文件尾巴的两种形状，靠 magic 自描述（不占版本位，因而不用迁移）：
///
/// ```text
/// 新版 16 字节：crc32(正文 ‖ seq) u32 | seq u64 | SNAP_MAGIC u32
/// 旧版  8 字节：seq u64                                  （v0.3.6 及更早）
/// ```
///
/// crc 把 **seq 也算进去**：seq 决定重放从哪里开始，它被写坏（偏大）就会
/// 静默跳过一批记录 —— 那正是「看起来启动成功、实际丢了写」那种事故。
///
/// 旧尾巴没有校验和 —— 这正是补上它的原因：正文里一个比特翻转只要不越界，
/// 解码器就会把它当成合法的 ino / nlink / size 收下，然后把这份错状态当真。
/// 用 magic 而不是「版本位 +1」来区分，是为了让已有的 v3 目录不必迁移就能
/// 再开一次门：正文布局一个字节没变，变的只是它后面跟了几个字节。
pub const SNAP_MAGIC: u32 = 0x3153_4254; // "TBS1"
/// 带 crc 的尾巴长度。
pub const SNAP_TRAIL: usize = 16;
/// 旧尾巴长度：只有 seq，没有校验和。
pub const SNAP_TRAIL_LEGACY: usize = 8;

/// 快照尾巴的 crc32：覆盖「正文 ‖ seq」，不插分隔字节。
fn snapshot_crc(body: &[u8], seq: u64) -> u32 {
    let c = crate::engine::wal::crc32_update(0xFFFF_FFFF, body);
    !crate::engine::wal::crc32_update(c, &seq.to_le_bytes())
}

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
    /// 建一张通用表。`id` 由翻译层从 `next_table_id` 分配（与 view id 同构），
    /// 进日志是为了重放时行与表能对上同一个 id。
    PutTable {
        id: u32,
        def: TableDef,
    },
    DelTable {
        id: u32,
        name: Vec<u8>,
    },
    /// 整行覆盖写（幂等，重放不需要知道前值）。二级索引由本模块从
    /// `rows` + def 就地维护，不进日志字节。
    PutRow {
        table: u32,
        row: RowRec,
    },
    DelRow {
        table: u32,
        key: Vec<u8>,
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
const LOG_PUT_TABLE: u8 = 19;
const LOG_DEL_TABLE: u8 = 20;
const LOG_PUT_ROW: u8 = 21;
const LOG_DEL_ROW: u8 = 22;

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
            Log::PutTable { id, def } => {
                w.u8(LOG_PUT_TABLE);
                w.u32(*id);
                def.encode(w);
            }
            Log::DelTable { id, name } => {
                w.u8(LOG_DEL_TABLE);
                w.u32(*id);
                w.bytes(name);
            }
            Log::PutRow { table, row } => {
                w.u8(LOG_PUT_ROW);
                w.u32(*table);
                row.encode(w);
            }
            Log::DelRow { table, key } => {
                w.u8(LOG_DEL_ROW);
                w.u32(*table);
                w.bytes(key);
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
            LOG_PUT_TABLE => Log::PutTable {
                id: r.u32()?,
                def: TableDef::decode(r)?,
            },
            LOG_DEL_TABLE => Log::DelTable {
                id: r.u32()?,
                name: r.bytes()?,
            },
            LOG_PUT_ROW => Log::PutRow {
                table: r.u32()?,
                row: RowRec::decode(r)?,
            },
            LOG_DEL_ROW => Log::DelRow {
                table: r.u32()?,
                key: r.bytes()?,
            },
            x => return Err(Error::Codec(format!("unknown log tag {x}"))),
        };
        Ok(log)
    }
}

/// 一张通用表在状态里的落地：引擎分配的 id + 客户端给的 def。
///
/// id 不放进 [`TableDef`]：def 是客户端写的东西（也是应答的一部分），
/// 给它加一个引擎才关心的字段等于把内部编号透到接口上。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    pub id: u32,
    pub def: TableDef,
}

/// `dentries` 内层 value。
///
/// `kind` 是从 `nodes` **冗余**过来的：readdir 一页 100 条，若每条回一次 inode 表，
/// 32k 目录规模下就是 100 次 BTreeMap 下钻、几乎全是 cache miss —— `treedb-micro`
/// 量到这一步占掉一页计算成本的九成（9.8 µs/页）。inode 的 kind 终身不变，冗余
/// 不会失真；它也不进 WAL 与快照字节（两处都能就地推出），故格式与版本都没动。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dent {
    pub ino: Ino,
    /// `Kind::code()`；`DENT_KIND_UNKNOWN` 表示写这条 dentry 时 inode 还没落地。
    pub kind: u8,
}

/// `Dent::kind` 的「不知道」哨兵。真实 kind 只有 0/1/2（见 `model::Kind::code`），
/// 故 `u8::MAX` 可安全占用；命中它说明日志顺序异常，读侧回表兜底。
pub const DENT_KIND_UNKNOWN: u8 = u8::MAX;

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
    /// 目录项两层表：外层按 `(view, parent)` 定位目录，内层按 name 字节序。
    ///
    /// 为什么不分层成一个 `(ViewId, Ino, Vec<u8>)` 扁平 key：`BTreeMap::get` 按
    /// key 的 `Ord` 逐字段比，含 `Vec<u8>` 的元组没法用 `&[u8]` 借位查（`Borrow`
    /// 只能整体换一次），于是每次 lookup 都要先 `name.clone()` 一个 `Vec`。分两层
    /// 后 `Vec<u8>: Borrow<[u8]>` 在内层这一维成立，查找与删除全程零分配。
    /// 遍历顺序不变（外层 `(view, parent)` 升序 + 内层 name 升序 = 原三元组字典序），
    /// 所以快照字节序列也与改造前逐字节一致。
    pub dentries: BTreeMap<(ViewId, Ino), BTreeMap<Vec<u8>, Dent>>,
    pub locks: BTreeMap<(ViewId, Ino, Vec<u8>), LockRec>,
    pub refs: BTreeMap<DataRef, RefRec>,
    pub keys: BTreeMap<Vec<u8>, KeyRec>,
    pub audit: BTreeMap<u64, AuditRec>,
    pub defs: BTreeMap<Vec<u8>, DefRec>,
    pub placement: BTreeMap<Vec<u8>, Vec<u8>>,
    /// 通用表（§11 T2）：名字 → id/def。与 `views`/`view_by_id` 同构，
    /// 因为同样的理由：行与索引只带 id，反查名字不能扫全表。
    pub tables: BTreeMap<Vec<u8>, TableMeta>,
    pub table_by_id: BTreeMap<u32, Vec<u8>>,
    /// 下一个可分配的 table id（从 0 起，与 view id 同一套路）。
    pub next_table_id: u32,
    /// 表 id → (主键 → 行)。分两层同样是为了拿 `&[u8]` 借位查不复制主键
    /// （与 [`State::dentries`] 同一个理由）。
    pub rows: BTreeMap<u32, BTreeMap<Vec<u8>, RowRec>>,
    /// 二级索引：`(表, 列, 值, 主键)` 一张扁平表，值占位。
    ///
    /// 为什么不分层：等值列举要的就是「(表,列,值) 前缀下按主键有序的一段」，
    /// 一层 `range` 直接命中；唯一性检查是同一段的非空判定，不需要另一份结构。
    /// 它**不进 WAL 也不进快照** —— 完全由 `rows` + def 推出，与 [`Dent::kind`]
    /// 同一个取舍（冗余可推字段不动磁盘格式）。
    pub row_index: BTreeMap<(u32, u32, Vec<u8>, Vec<u8>), ()>,
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
                let kind = self.node_kind(*view, *ino);
                self.dentries
                    .entry((*view, *parent))
                    .or_default()
                    .insert(name.clone(), Dent { ino: *ino, kind });
            }
            Log::DelDentry { view, parent, name } => {
                let empty = match self.dentries.get_mut(&(*view, *parent)) {
                    Some(m) => {
                        m.remove(name);
                        m.is_empty()
                    }
                    None => false,
                };
                // 空内层表必须收掉：否则 `dentries.len()` 混进越来越多零条目录项的
                // 目录，而它同时是 stats 与快照里的 dentry 计数依据。
                if empty {
                    self.dentries.remove(&(*view, *parent));
                }
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
            Log::PutTable { id, def } => {
                self.table_by_id.insert(*id, def.name.clone());
                if *id >= self.next_table_id {
                    self.next_table_id = id + 1;
                }
                self.tables.insert(
                    def.name.clone(),
                    TableMeta {
                        id: *id,
                        def: def.clone(),
                    },
                );
            }
            Log::DelTable { id, name } => {
                // 级联：行与索引都跟着走。`indexed_cols` 必须在表名映射还在
                // 的时候取（先摘后就拿不到 def，索引项会永久泄漏）。
                let cols = self.indexed_cols(*id);
                self.tables.remove(name);
                self.table_by_id.remove(id);
                if let Some(m) = self.rows.remove(id) {
                    for row in m.values() {
                        self.unindex_row(*id, row, &cols);
                    }
                }
            }
            Log::PutRow { table, row } => {
                let cols = self.indexed_cols(*table);
                let old = self
                    .rows
                    .get(table)
                    .and_then(|m| m.get(&row.key))
                    .cloned();
                if let Some(old) = old {
                    self.unindex_row(*table, &old, &cols);
                }
                self.rows
                    .entry(*table)
                    .or_default()
                    .insert(row.key.clone(), row.clone());
                self.index_row(*table, row, &cols);
            }
            Log::DelRow { table, key } => {
                let old = self
                    .rows
                    .get(table)
                    .and_then(|m| m.get(key))
                    .cloned();
                let Some(old) = old else { return };
                let cols = self.indexed_cols(*table);
                self.unindex_row(*table, &old, &cols);
                if let Some(m) = self.rows.get_mut(table) {
                    m.remove(key);
                }
                // 空内层表不留，与 `dentries` 同规矩（否则 `rows.len()` 会谎报表数）。
                if self.rows.get(table).map(|m| m.is_empty()).unwrap_or(false) {
                    self.rows.remove(table);
                }
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
        // dentries 外层 key 就是 `(view, parent)`，前缀扫直接命中，不用再把
        // 每条 key 克隆出来（旧写法在十万条目录项的视图上要拷十万个 Vec）。
        let dead_dirs: Vec<u64> = self
            .dentries
            .range((view, u64::MIN)..=(view, u64::MAX))
            .map(|(k, _)| k.1)
            .collect();
        for p in dead_dirs {
            self.dentries.remove(&(view, p));
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

    /// 某表需要维护二级索引的列序号（按 def 顺序）。表已被删则空。
    pub fn indexed_cols(&self, tid: u32) -> Vec<u32> {
        let Some(name) = self.table_by_id.get(&tid) else {
            return Vec::new();
        };
        let Some(t) = self.tables.get(name.as_slice()) else {
            return Vec::new();
        };
        t.def
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.indexed())
            .map(|(i, _)| i as u32)
            .collect()
    }

    fn index_row(&mut self, tid: u32, row: &RowRec, cols: &[u32]) {
        for &c in cols {
            // 列数比 def 少时该列不入索：写入侧本应在翻译层就拒掉 EINVAL，
            // 这里的处理只是不让重放 panic。
            if let Some(v) = row.cols.get(c as usize) {
                self.row_index
                    .insert((tid, c, v.clone(), row.key.clone()), ());
            }
        }
    }

    fn unindex_row(&mut self, tid: u32, row: &RowRec, cols: &[u32]) {
        for &c in cols {
            if let Some(v) = row.cols.get(c as usize) {
                self.row_index.remove(&(tid, c, v.clone(), row.key.clone()));
            }
        }
    }

    /// `(表, 列, 值)` 前缀下的全部主键，按主键序。唯一性检查就是它的非空判定。
    pub fn index_keys(&self, tid: u32, col: u32, value: &[u8]) -> Vec<Vec<u8>> {
        self.row_index
            .range((
                Bound::Included((tid, col, value.to_vec(), Vec::new())),
                Bound::Unbounded,
            ))
            .take_while(|(k, _)| k.0 == tid && k.1 == col && k.2 == value)
            .map(|(k, _)| k.3.clone())
            .collect()
    }

    /// 某表的主键升序页：`after` 之后的至多 `limit` 行，并说清到底没。
    ///
    /// `after` 是「上一页最后一行的主键」（不含本身），空 = 从头。与
    /// [`read_dir`](Self::read_dir) 同一个游标语义，也与主键不得为空那条校验
    /// 相配 —— 否则「从头」与「从空键起」两种意思会撞在一个写法上。
    pub fn row_page(&self, tid: u32, after: &[u8], limit: usize) -> (Vec<RowRec>, bool) {
        let Some(m) = self.rows.get(&tid) else {
            return (Vec::new(), true);
        };
        let lo = if after.is_empty() {
            Bound::Included(after)
        } else {
            Bound::Excluded(after)
        };
        let mut it = m.range::<[u8], _>((lo, Bound::Unbounded));
        let rows: Vec<RowRec> = it.by_ref().take(limit).map(|(_, r)| r.clone()).collect();
        let end = it.next().is_none();
        (rows, end)
    }

    /// 按索引列等值取一页。游标语义与 [`row_page`](Self::row_page) 一致（同一列、
    /// 同一个值内按主键继续）。
    pub fn index_page(
        &self,
        tid: u32,
        col: u32,
        value: &[u8],
        after: &[u8],
        limit: usize,
    ) -> (Vec<RowRec>, bool) {
        let lo = (tid, col, value.to_vec(), after.to_vec());
        let mut it = self
            .row_index
            .range((Bound::Excluded(lo), Bound::Unbounded))
            .take_while(move |(k, _)| k.0 == tid && k.1 == col && k.2 == *value)
            .filter_map(move |(k, _)| self.rows.get(&tid).and_then(|m| m.get(&k.3)).cloned());
        let rows: Vec<RowRec> = it.by_ref().take(limit).collect();
        let end = it.next().is_none();
        (rows, end)
    }

    /// 主键区间 `[lo, hi)` 的一页：`after` 之后的至多 `limit` 行，并说清到底没。
    ///
    /// `lo` / `hi` 空 = 该侧无界（主键不得为空，所以「空」只能是无界这一个意思）。
    /// `lo > hi` 是空区间而不是错误 —— 但 `BTreeMap::range` 在 `start > end` 时
    /// **panic**，所以得先在这里挡掉，不能让调用方替它记着这条。
    pub fn row_range(
        &self,
        tid: u32,
        lo: &[u8],
        hi: &[u8],
        after: &[u8],
        limit: usize,
    ) -> (Vec<RowRec>, bool) {
        let Some(m) = self.rows.get(&tid) else {
            return (Vec::new(), true);
        };
        if !lo.is_empty() && !hi.is_empty() && lo > hi {
            return (Vec::new(), true);
        }
        // 游标越过上界也落在这里：`after > hi` 时下界与上界反了，而 `after == hi`
        // 时两端都是 `Excluded` —— 这两种都在 `BTreeMap::range` 的 panic 条件里。
        if !after.is_empty() && !hi.is_empty() && after >= hi {
            return (Vec::new(), true);
        }
        // 下界取 max(lo, after)：有游标就从它之后接，但绝不能跑到 `lo` 前面去。
        let lower = if !after.is_empty() && (lo.is_empty() || after >= lo) {
            Bound::Excluded(after)
        } else if lo.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Included(lo)
        };
        let upper = if hi.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(hi)
        };
        let mut it = m.range::<[u8], _>((lower, upper));
        let rows: Vec<RowRec> = it.by_ref().take(limit).map(|(_, r)| r.clone()).collect();
        let end = it.next().is_none();
        (rows, end)
    }

    /// 索引列值区间 `[lo, hi)` 的一页；同一个值内按主键序。
    ///
    /// 游标是 `(after_val, after_key)` 一对：一个值可能跨页，只拿主键回不到
    /// 「这一页停在哪一段」。`None` = 从区间头开始。
    pub fn index_range(
        &self,
        tid: u32,
        col: u32,
        lo: &[u8],
        hi: &[u8],
        after: Option<(&[u8], &[u8])>,
        limit: usize,
    ) -> (Vec<RowRec>, bool) {
        // 起点 = max(lo, 游标)：给了游标就是「不含」，否则是「含 lo」。
        let (sv, sk, excl) = match after {
            Some((v, k)) if lo.is_empty() || (v, k) > (lo, &b""[..]) => (v, k, true),
            _ => (lo, &b""[..], false),
        };
        let lower = if excl {
            Bound::Excluded((tid, col, sv.to_vec(), sk.to_vec()))
        } else {
            Bound::Included((tid, col, sv.to_vec(), Vec::new()))
        };
        let hi_v = hi.to_vec();
        let mut it = self
            .row_index
            .range((lower, Bound::Unbounded))
            // 上界只能靠 `take_while`：键是 `(表, 列, 值, 主键)`，而
            // `(tid, col, hi, "")` 只是「值为 hi 的最小键」—— 同值的其它行排在它
            // 之后，用 `Excluded` 一个也挡不住。值为 `hi` 的那一段必须整段排除。
            .take_while(move |(k, _)| {
                k.0 == tid && k.1 == col && (hi_v.is_empty() || k.2 < hi_v)
            })
            .filter_map(|(k, _)| self.rows.get(&tid).and_then(|m| m.get(&k.3)).cloned());
        let rows: Vec<RowRec> = it.by_ref().take(limit).collect();
        let end = it.next().is_none();
        (rows, end)
    }

    /// `(view, parent, *)` 区间内、`after` 之后的至多 `limit` 条 dentry。
    /// 内层 key 的字节序即 name 的字节序。`after` 为空 = 从头开始。
    pub fn read_dir(&self, view: ViewId, parent: Ino, after: &[u8], limit: usize) -> Vec<(Vec<u8>, Ino)> {
        let Some(m) = self.dentries.get(&(view, parent)) else {
            return Vec::new();
        };
        // after 是「上一页最后一个名字」，用 Excluded 跳过它本身。
        let lo = if after.is_empty() {
            Bound::Included(after)
        } else {
            Bound::Excluded(after)
        };
        m.range::<[u8], _>((lo, Bound::Unbounded))
            .take(limit)
            .map(|(n, d)| (n.clone(), d.ino))
            .collect()
    }

    /// 某目录是否为空（readdir 不含 `.`/`..`，故只看子 dentry）。
    pub fn dir_empty(&self, view: ViewId, parent: Ino) -> bool {
        self.dentries
            .get(&(view, parent))
            .map_or(true, |m| m.is_empty())
    }

    /// 零分配 dentry 查找：内层 `BTreeMap<Vec<u8>, _>` 可以拿 `&[u8]` 借位查。
    #[inline]
    pub fn dentry(&self, view: ViewId, parent: Ino, name: &[u8]) -> Option<Dent> {
        self.dentries
            .get(&(view, parent))
            .and_then(|m| m.get(name))
            .copied()
    }

    /// 某 inode 的 kind 码，没有则 [`DENT_KIND_UNKNOWN`]。
    #[inline]
    fn node_kind(&self, view: ViewId, ino: Ino) -> u8 {
        self.nodes
            .get(&(view, ino))
            .map(|n| n.kind.code())
            .unwrap_or(DENT_KIND_UNKNOWN)
    }

    /// dentry 总条数。**O(目录数)**，只能用在诊断与 checkpoint 路径上，
    /// 不要放进事件循环的每轮。注意 `dentries.len()` 是**有子项的目录数**，
    /// 不是条数。
    pub fn dentry_count(&self) -> usize {
        self.dentries.values().map(|m| m.len()).sum()
    }

    pub fn live_view_count(&self) -> usize {
        self.views.len()
    }

    /// 通用表的总行数（`rows.len()` 是表数，不是行数）。
    pub fn row_count(&self) -> usize {
        self.rows.values().map(|m| m.len()).sum()
    }

    // ---- 快照 ----

    pub fn save(&self, w: &mut W) {
        self.save_versioned(w, SNAP_VERSION);
    }

    /// 写「正文 + 尾巴」，即一个完整的 `state.snap` 文件。
    ///
    /// 尾巴里的 seq 必须由调用方从 WAL 的 durable 水位取（[`Engine::checkpoint`]
    /// 用的是 `wal.durable_seq()`），本函数只负责把它连同正文的 crc 一起编进去：
    /// 自己发明一个水位，下次启动按它跳记录就是丢写。
    pub fn save_with_trailer(&self, w: &mut W, seq: u64) {
        self.save(w);
        let mut buf = std::mem::take(&mut w.0);
        let crc = snapshot_crc(&buf, seq);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
        w.0 = buf;
    }

    /// 把整份快照文件切成「正文 + 水位」，新版尾巴顺带验 crc32。
    ///
    /// 两代尾巴都认：8 字节的没有校验和，只能照收（它写的时候还没有 crc）；
    /// 16 字节的先验 crc 再交正文 —— 校验不过就到此为止，绝不让解码器去
    /// 解释一份已知是坏的字节。magic 被写坏的情形会退化成「旧尾巴」解释，
    /// 于是正文末尾多出 8 字节，由 `State::load` 的「正文必须刚好吃完」拒掉。
    pub fn snapshot_split(raw: &[u8]) -> Result<(&[u8], u64)> {
        if raw.len() < SNAP_TRAIL_LEGACY {
            return Err(Error::Codec(format!(
                "snapshot is {} bytes, shorter than its {SNAP_TRAIL_LEGACY}-byte seq trailer",
                raw.len()
            )));
        }
        let at = raw.len() - SNAP_TRAIL_LEGACY;
        let seq = u64::from_le_bytes(raw[at..].try_into().unwrap());
        if raw.len() >= SNAP_TRAIL {
            let at = raw.len() - SNAP_TRAIL;
            let crc = u32::from_le_bytes(raw[at..at + 4].try_into().unwrap());
            let magic = u32::from_le_bytes(raw[at + 12..at + 16].try_into().unwrap());
            if magic == SNAP_MAGIC {
                let body = &raw[..at];
                let seq = u64::from_le_bytes(raw[at + 4..at + 12].try_into().unwrap());
                let want = snapshot_crc(body, seq);
                if crc != want {
                    return Err(Error::Codec(format!(
                        "snapshot body/trailer fails its crc32: stored {crc:#010x}, computed \
                         {want:#010x} over {} body bytes at seq {seq}",
                        body.len()
                    )));
                }
                return Ok((body, seq));
            }
        }
        Ok((&raw[..at], seq))
    }

    /// 按上一代布局写快照：版本位写 [`SNAP_LEGACY_VERSION`]，尾部三段不写。
    ///
    /// 它存在的唯一理由是**让测试能造出真 v2 文件**（`tests/migrate.rs` 与下面的
    /// 往返用例）。产品路径永远走 [`State::save`]：旧文件是历史版本写出来的，
    /// 本仓不该假装替它写。所以这里有一道硬闸 —— 有表或有行直接拒，
    /// v2 布局装不下它们，静默丢掉等于抹掉用户的元数据。
    pub fn save_v2(&self, w: &mut W) -> Result<()> {
        if !self.tables.is_empty() || !self.rows.is_empty() {
            return Err(Error::Config(format!(
                "cannot write a version {SNAP_LEGACY_VERSION} snapshot while {} table(s) and {} row(s) exist: that layout has no place for them",
                self.tables.len(),
                self.row_count()
            )));
        }
        self.save_versioned(w, SNAP_LEGACY_VERSION);
        Ok(())
    }

    fn save_versioned(&self, w: &mut W, ver: u32) {
        w.u32(ver);
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
        w.u32(self.dentry_count() as u32);
        for ((view, parent), m) in &self.dentries {
            for (name, d) in m {
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
                w.u64(d.ino);
            }
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
        // ---- v3：通用表 ----
        if ver == SNAP_VERSION {
            w.u32(self.tables.len() as u32);
            for t in self.tables.values() {
                w.u32(t.id);
                t.def.encode(w);
            }
            w.u32(self.next_table_id);
            // 行数要先扫一遍才能写计数段：`rows` 是分层的，`len()` 是表数不是行数
            //（与上面 `dentry_count()` 同一个坑）。
            w.u32(self.row_count() as u32);
            for (tid, m) in &self.rows {
                for row in m.values() {
                    w.u32(*tid);
                    row.encode(w);
                }
            }
        }
        // `row_index` 不写：它由 `rows` + def 在载入时重建，写进磁盘格式就等于
        // 多一份可以互相矛盾的权威（与 `Dent::kind` 同一条理由）。
    }

    /// 载入快照。版本位不是 [`SNAP_VERSION`] 就拒 —— 包括上一代：常规启动路径
    /// 永远不开兼容口子，静默收下旧文件等于用错的布局解释已有的字节。
    /// 旧目录的出路是 `treedb-migrate`（§6.5），错误消息直接把它指出来。
    pub fn load(r: &mut Rd) -> Result<State> {
        Self::load_gate(r, false)
    }

    /// 迁移工具专用的载入：额外接受 [`SNAP_LEGACY_VERSION`]。
    ///
    /// 与 [`State::load`] 的唯一差别就是为了「动手之前先把输入看清楚」：只多认
    /// 一代。buffer 是**正文**（尾巴已由 [`State::snapshot_split`] 切掉），
    /// 正文必须被布局刚好吃完，多一个字节都说明这文件不是它自称的那个版本，
    /// 宁可不改也不猜。
    /// 解出来的 `State` 与解 v3 文件得到的完全同形（表与行两张空表、派生结构
    /// 为空），所以迁移只是换个布局写回去，中间不发生任何语义翻译。
    pub fn load_for_migration(r: &mut Rd) -> Result<State> {
        Self::load_gate(r, true)
    }

    /// 只读版本位，不碰正文。`treedb-migrate` 靠它决定要不要动手，
    /// 也靠它把「不认识的版本」和「认识但需要迁移的版本」分开报。
    pub fn snapshot_version(raw: &[u8]) -> Result<u32> {
        if raw.len() < 4 {
            return Err(Error::Codec(format!(
                "snapshot is {} bytes, shorter than its 4-byte version header",
                raw.len()
            )));
        }
        Ok(u32::from_le_bytes(raw[..4].try_into().unwrap()))
    }

    fn load_gate(r: &mut Rd, allow_legacy: bool) -> Result<State> {
        let ver = r.u32()?;
        let tail = if ver == SNAP_VERSION {
            true
        } else if allow_legacy && ver == SNAP_LEGACY_VERSION {
            false
        } else if ver == SNAP_LEGACY_VERSION {
            return Err(Error::Config(format!(
                "snapshot version {ver} is one generation behind (this build writes \
                 {SNAP_VERSION}): stop the server and run treedb-migrate --dir <data dir>, \
                 which rewrites the snapshot in place and leaves the WAL alone"
            )));
        } else {
            return Err(Error::Config(format!(
                "snapshot version {ver} unsupported (this build writes {SNAP_VERSION})"
            )));
        };
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
            // nodes 段先于本段载入，故 kind 就地可推；推不出来留哨兵，读侧兜底。
            let kind = s.node_kind(view, ino);
            s.dentries
                .entry((view, parent))
                .or_default()
                .insert(name, Dent { ino, kind });
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
        if tail {
            let n = r.u32()? as usize;
            if n > MAX_TABLE_ITEMS {
                return Err(Error::Config(format!(
                    "snapshot lists {n} tables, over {MAX_TABLE_ITEMS}"
                )));
            }
            // 计数上限只为拒敌意输入；BTreeMap 没有预留容量这回事，逐项插进去就行。
            for _ in 0..n {
                let id = r.u32()?;
                let def = TableDef::decode(r)?;
                s.table_by_id.insert(id, def.name.clone());
                if id >= s.next_table_id {
                    s.next_table_id = id + 1;
                }
                s.tables.insert(def.name.clone(), TableMeta { id, def });
            }
            // 取两个值的较大者：旧快照里 next_table_id 写得比实际 id 小也不能
            // 让重号发生（与 `Log::PutTable` 那条 `>=` 同一个守口）。
            s.next_table_id = s.next_table_id.max(r.u32()?);
            let n = r.u32()? as usize;
            if n > MAX_TABLE_ITEMS {
                return Err(Error::Config(format!(
                    "snapshot lists {n} rows, over {MAX_TABLE_ITEMS}"
                )));
            }
            for _ in 0..n {
                let tid = r.u32()?;
                let row = RowRec::decode(r)?;
                let cols = s.indexed_cols(tid);
                s.index_row(tid, &row, &cols);
                s.rows.entry(tid).or_default().insert(row.key.clone(), row);
            }
        }
        // 正文必须被布局**刚好**吃完。尾巴（crc + seq）由调用方先用
        // [`State::snapshot_split`] 切掉，所以这里剩下任何一个字节都说明
        // 这文件不是它自称的那个版本 —— 要么是更新的布局，要么是坏文件。
        // 常规启动路径同样走这条检查：以前只有迁移入口查，于是「正文后面
        // 挂了尾巴」的目录能被静默开起来。
        if r.remaining() != 0 {
            return Err(Error::Codec(format!(
                "snapshot body does not match its version {ver}: {} byte(s) trail the payload",
                r.remaining()
            )));
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::model::{ColumnDef, Kind, COL_INDEX, COL_UNIQUE};

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
            Log::PutTable {
                id: 4,
                def: TableDef {
                    name: b"kv".to_vec(),
                    columns: vec![
                        ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
                        ColumnDef { name: b"b".to_vec(), flags: 0 },
                    ],
                },
            },
            Log::DelTable { id: 4, name: b"kv".to_vec() },
            Log::PutRow {
                table: 4,
                row: RowRec { key: b"k1".to_vec(), cols: vec![b"v1".to_vec(), Vec::new()] },
            },
            Log::DelRow { table: 4, key: b"k1".to_vec() },
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

    /// 两层 dentry 表的四条契约：条数与目录数分开、借位零分配查找、
    /// kind 冗余不进快照字节、清空后的目录不留外层空表。
    #[test]
    fn dentries_are_two_level_and_kind_survives_snapshot() {
        let mut s = State::new();
        s.apply(&Log::PutView {
            name: b"v".to_vec(),
            meta: ViewMeta { id: 1, root: 1, next_ino: 9 },
        });
        for (ino, name, kind) in [(2u64, b"a".as_slice(), Kind::File), (3, b"d", Kind::Dir)] {
            s.apply(&Log::PutNode { view: 1, node: node(ino, 1, name, kind) });
            s.apply(&Log::PutDentry {
                view: 1,
                parent: 1,
                name: name.to_vec(),
                ino,
            });
        }
        // `.len()` 是目录数，`dentry_count()` 才是条数：两者混用会把 stats
        // 与快照计数写成 1。
        assert_eq!(s.dentries.len(), 1);
        assert_eq!(s.dentry_count(), 2);
        // 借位查找：调用方手里只有 &[u8]，查一次表不用复制名字。
        assert_eq!(
            s.dentry(1, 1, b"a"),
            Some(Dent { ino: 2, kind: Kind::File.code() })
        );
        assert_eq!(
            s.dentry(1, 1, b"d"),
            Some(Dent { ino: 3, kind: Kind::Dir.code() })
        );
        assert_eq!(s.dentry(1, 1, b"zz"), None);

        // kind 是推出来的冗余字段，不占快照一个字节：往返后必须从 nodes
        // 重新推出同样的值——否则就是快照格式被改了却没升 SNAPSHOT_VERSION。
        let mut w = W::new();
        s.save(&mut w);
        let b = w.finish();
        let mut r = Rd::new(&b);
        let back = State::load(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(back, s, "快照往返必须逐字段相等（含冗余的 kind）");

        for name in [b"a".as_slice(), b"d"] {
            s.apply(&Log::DelDentry { view: 1, parent: 1, name: name.to_vec() });
        }
        assert_eq!(s.dentry_count(), 0);
        assert!(s.dentries.is_empty(), "空内层表不能留在外层");
        assert!(s.dir_empty(1, 1));
    }

    fn page_keys(rows: &[RowRec]) -> Vec<Vec<u8>> {
        rows.iter().map(|r| r.key.clone()).collect()
    }

    /// 通用表的五条契约：id 正向表同批维护、二级索引随覆盖写更新、游标翻页
    /// 不重不漏、索引不占快照字节、删表级联不留残留。
    #[test]
    fn generic_tables_keep_rows_and_rebuild_indexes() {
        let mut s = State::new();
        s.apply(&Log::PutTable {
            id: 0,
            def: TableDef {
                name: b"kv".to_vec(),
                columns: vec![
                    ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
                    ColumnDef { name: b"b".to_vec(), flags: COL_INDEX },
                ],
            },
        });
        assert_eq!(
            s.table_by_id.get(&0).map(|n| n.as_slice()),
            Some(&b"kv"[..]),
            "行与索引只带 id，正向表必须同批建起来"
        );
        assert_eq!(s.next_table_id, 1, "id 分配也要从日志里推回来");
        assert_eq!(s.indexed_cols(0), vec![0, 1]);

        for (key, va, vb) in [
            (b"k1".as_slice(), b"v1".as_slice(), b"x".as_slice()),
            (b"k2", b"v2", b"x"),
            (b"k3", b"v3", b"y"),
        ] {
            s.apply(&Log::PutRow {
                table: 0,
                row: RowRec { key: key.to_vec(), cols: vec![va.to_vec(), vb.to_vec()] },
            });
        }
        // `rows.len()` 是表数，`row_count()` 才是行数 —— 与 dentries 同一个坑。
        assert_eq!(s.rows.len(), 1);
        assert_eq!(s.row_count(), 3);
        assert_eq!(s.index_keys(0, 0, b"v2"), vec![b"k2".to_vec()]);
        assert_eq!(
            s.index_keys(0, 1, b"x"),
            vec![b"k1".to_vec(), b"k2".to_vec()],
            "同值多行按主键序"
        );
        assert!(s.index_keys(0, 1, b"zz").is_empty());

        // 覆盖写：旧值必须从索引里摘掉，否则唯一列会同时挂着两个主键。
        s.apply(&Log::PutRow {
            table: 0,
            row: RowRec { key: b"k2".to_vec(), cols: vec![b"v9".to_vec(), b"x".to_vec()] },
        });
        assert!(s.index_keys(0, 0, b"v2").is_empty(), "旧值没摘干净");
        assert_eq!(s.index_keys(0, 0, b"v9"), vec![b"k2".to_vec()]);

        // 游标 = 上一页最后一行的主键，不含本身：翻完所有页既不重也不漏。
        let (p1, end1) = s.row_page(0, &[], 2);
        assert_eq!(page_keys(&p1), vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert!(!end1);
        let (p2, end2) = s.row_page(0, &p1.last().unwrap().key, 2);
        assert_eq!(page_keys(&p2), vec![b"k3".to_vec()]);
        assert!(end2);
        let (ix, iend) = s.index_page(0, 1, b"x", &[], 10);
        assert_eq!(page_keys(&ix), vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert!(iend);

        // 索引不进快照：往返后整个 State 逐字段相等，就等于它是被原样重推出来的。
        let mut w = W::new();
        s.save(&mut w);
        let bytes = w.finish();
        let mut r = Rd::new(&bytes);
        let back = State::load(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(back, s, "表/行/next_table_id 必须全回来，索引必须重推成同一个");

        // 删表级联：行、索引项、正向表一条不留。
        s.apply(&Log::DelTable { id: 0, name: b"kv".to_vec() });
        assert!(s.tables.is_empty() && s.table_by_id.is_empty() && s.rows.is_empty());
        assert!(s.row_index.is_empty(), "删表后残留索引项 = 永久泄漏");
        assert_eq!(s.indexed_cols(0), Vec::<u32>::new());
    }

    /// 一份只用了 v2 就能装下的状态，按两代布局各写一遍。
    fn legacy_sample() -> State {
        let mut s = State::new();
        s.apply(&Log::PutView {
            name: b"alice".to_vec(),
            meta: ViewMeta { id: 1, root: 1, next_ino: 3 },
        });
        s.apply(&Log::PutNode { view: 1, node: node(1, 1, b"", Kind::Dir) });
        s.apply(&Log::PutNode { view: 1, node: node(2, 1, b"a.txt", Kind::File) });
        s.apply(&Log::PutDentry { view: 1, parent: 1, name: b"a.txt".to_vec(), ino: 2 });
        s.apply(&Log::PutLock {
            view: 1,
            ino: 2,
            owner: b"o".to_vec(),
            rec: LockRec { kind: 1, lease_expiry: -7 },
        });
        s.apply(&Log::PutRef {
            data_ref: b"1:4096:dev0".to_vec(),
            rec: RefRec { count: 1, bytes: 4096 },
        });
        s.apply(&Log::PutKey {
            key: KeyRec {
                id: b"id".to_vec(),
                label: b"l".to_vec(),
                secret_hash: b"h".to_vec(),
                created_at: 1,
                last_used_at: 0,
                revoked_at: 0,
                view: b"alice".to_vec(),
                ro: false,
                owner: Vec::new(),
                note: Vec::new(),
                expires_at: 0,
                created_by: Vec::new(),
                secret_hint: Vec::new(),
            },
        });
        s.apply(&Log::PutAudit {
            row: AuditRec {
                seq: 7,
                ts: 1,
                key_id: b"id".to_vec(),
                method: Vec::new(),
                tool: Vec::new(),
                ok: true,
                detail: Vec::new(),
            },
        });
        s.apply(&Log::PutDef {
            def: DefRec { name: b"n".to_vec(), kind: b"k".to_vec(), payload: b"p".to_vec(), updated_at: 1 },
        });
        s.apply(&Log::PutPlace { view: b"alice".to_vec(), instance: b"i1".to_vec() });
        s
    }

    /// 「v2 与 v3 只差尾部三段」是迁移工具敢做读旧写新的全部依据。这条测试就
    /// 是那个依据的可执行版本：前缀字节逐字节相同，旧布局能被迁移入口完整解回，
    /// 而常规入口仍然拒收它。
    #[test]
    fn legacy_snapshot_decodes_from_the_same_prefix() {
        let s = legacy_sample();
        let mut w = W::new();
        s.save_v2(&mut w).unwrap();
        let mut raw = w.finish();
        assert_eq!(State::snapshot_version(&raw).unwrap(), SNAP_LEGACY_VERSION);
        raw.extend_from_slice(&4242u64.to_le_bytes());

        // 尾巴先切掉再解：crc/seq 不属于正文布局。
        let (body, seq) = State::snapshot_split(&raw).unwrap();
        assert_eq!(seq, 4242);
        let back = State::load_for_migration(&mut Rd::new(body)).unwrap();
        assert_eq!(back, s, "v2 的正文必须被同一套解码吃下来");
        assert!(
            back.tables.is_empty() && back.rows.is_empty() && back.row_index.is_empty(),
            "旧布局里没有表，解出来就该是两张空表而不是报错"
        );

        // 两代布局的前缀必须逐字节相同：不是「大致相同」，是同一个编码器
        // 少跑了一个 `if`。这句话一旦不成立，迁移工具的「读旧写新」就得改写。
        let mut w3 = W::new();
        s.save(&mut w3);
        let v3 = w3.finish();
        let body_end = raw.len() - 8;
        assert_eq!(
            &v3[4..body_end],
            &raw[4..body_end],
            "两代布局只差版本位与尾部那几段，正文必须逐字节相同"
        );
        assert!(v3.len() > body_end, "v3 不写表那三段就等于没升版本");
        // 当前布局走迁移入口也得能解（`--apply` 重跑第二次的幂等路径）。
        // 这里传的是正文（v3 是 `save` 的输出，尾巴由调用方拼）。
        assert_eq!(State::load_for_migration(&mut Rd::new(&v3)).unwrap(), s);

        // 常规启动路径不开口子，但拒的时候要指出出路，不能只说「不支持」。
        let err = State::load(&mut Rd::new(&raw)).unwrap_err().to_string();
        assert!(err.contains("treedb-migrate"), "旧目录的出路要写在错里：{err}");

        // 迁移入口按正文解：正文后面多一个字节就不是它自称的那个版本。
        let mut extra = raw.clone();
        extra.push(0);
        let (body, _) = State::snapshot_split(&extra).unwrap();
        assert!(State::load_for_migration(&mut Rd::new(body)).is_err());
        // 连 8 字节 seq 尾巴都放不下的文件，不是快照。
        assert!(State::snapshot_split(&raw[..4]).is_err());
        // 版本位越老越不认：两代之前没有迁移路径。
        let ancient = 1u32.to_le_bytes().to_vec();
        assert_eq!(State::snapshot_version(&ancient).unwrap(), 1);
        assert!(State::load_for_migration(&mut Rd::new(&ancient)).is_err());
    }

    /// 尾巴自描述：16 字节带 crc（覆盖面是「正文 ‖ seq」），8 字节照收。
    ///
    /// 两代共存是设计不是过渡期残留：旧目录不改一个字节就能开，下一次
    /// checkpoint 自己换成新尾巴。而 crc 覆盖 seq 是为了堵住一个很难查的
    /// 事故：水位被写大 → 启动时直接跳过一批 WAL 记录，状态“干净但少了”。
    #[test]
    fn snapshot_trailer_carries_crc_and_accepts_the_legacy_shape() {
        let s = legacy_sample();
        let mut w = W::new();
        s.save_with_trailer(&mut w, 77);
        let raw = w.finish();

        let (body, seq) = State::snapshot_split(&raw).unwrap();
        assert_eq!(seq, 77);
        assert_eq!(raw.len(), body.len() + SNAP_TRAIL);
        assert_eq!(State::load(&mut Rd::new(body)).unwrap(), s, "正文能被严格入口解回来");

        // 正文里翻一个比特：必须在切尾巴这一步就被拒，不能交给解码器。
        let mut flipped = raw.clone();
        flipped[body.len() / 2] ^= 0x40;
        let err = State::snapshot_split(&flipped).unwrap_err().to_string();
        assert!(err.contains("crc32"), "{err}");

        // 尾巴里的 seq 被写坏同样要拒：它在 crc 的覆盖面里。
        let mut bad_seq = raw.clone();
        let at = raw.len() - 12;
        bad_seq[at] ^= 0x01;
        assert!(
            State::snapshot_split(&bad_seq).is_err(),
            "seq 写坏会让重放跳记录，必须在开门时就拒"
        );

        // 旧尾巴（只有 seq）：照收，正文与水位都取对。
        let mut w2 = W::new();
        s.save(&mut w2);
        let mut legacy = w2.finish();
        assert_eq!(legacy.len(), body.len());
        legacy.extend_from_slice(&77u64.to_le_bytes());
        let (b2, seq2) = State::snapshot_split(&legacy).unwrap();
        assert_eq!((b2, seq2), (body, 77));
        assert_eq!(State::load(&mut Rd::new(b2)).unwrap(), s);
    }

    /// 旧布局装不下表与行，所以下行写必须硬拒。这个闸存在的原因是：一旦
    /// 有人拿它去做「导出成旧格式」，静默丢掉行就是掉用户的元数据。
    #[test]
    fn legacy_writer_refuses_to_drop_tables() {
        let mut s = State::new();
        s.apply(&Log::PutTable {
            id: 3,
            def: TableDef {
                name: b"kv".to_vec(),
                columns: vec![ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE }],
            },
        });
        let mut w = W::new();
        let err = s.save_v2(&mut w).unwrap_err().to_string();
        assert!(err.contains("no place for them"), "要说清楚为什么不能写：{err}");
        assert!(w.finish().is_empty(), "拒了就不该已经写出半截版本位");
    }
}
