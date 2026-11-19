//! 引擎数据模型与协议语义类型。
//!
//! 概念对齐「把元数据存进关系库」的那张表形状（`pg_store` 这类实现），但去掉 SQL：
//! - `node` 表 → [`Node`]（inode）+ `dentries`（`(view,parent,name) → ino`，
//!   内存里按 `(view,parent)` 分两层存，见 [`crate::engine::mem::State`]）；
//! - `locks` 表 → [`LockRec`]（`(view,ino,owner) → kind,lease`）；
//! - 内容引用账本 → [`RefRec`]（`data_ref → count,size`，跨视图共享）；
//! - 控制面「命名密钥 + 调用审计」两张表 → [`KeyRec`] / [`AuditRec`]。
//!
//! 命名与列语义与关系库那一版逐字段对得上，客户端接入时上层映射是翻译，
//! 不是重新建模 —— 这是这套形状存在的原因，不是对某个具体客户端的契约。

use crate::engine::codec::{Rd, W};
use crate::error::{Error, Result};

pub type ViewId = u32;
pub type Ino = u64;
/// 内容对象的引用：一段**不透明字节**，引擎不解释其含义。
///
/// 这里曾经是 `u64`，v2 定稿时改掉：客户端侧的 `data_ref` 是后端自己生成的
/// 字符串键（内容哈希 / 条带化多键），把它折成 `u64` 要么哈希（碰撞 = 两个文件的
/// 字节互相顶掉），要么在 provider 里养一张 `String ↔ u64` 映射表（表本身要持久化，
/// 崩溃后还原不出原键 = 已 ack 的文件读不到数据）。字节直接存进 inode 与 `refs`
/// 账本，两边表示一致，不需要任何映射层。空 = 尚未提交内容。
pub type DataRef = Vec<u8>;

/// inode 类型，编码 0/1/2，与关系库元数据表里 `node.is_dir` 的取值同序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
    Symlink,
}

impl Kind {
    pub fn code(self) -> u8 {
        match self {
            Kind::File => 0,
            Kind::Dir => 1,
            Kind::Symlink => 2,
        }
    }
    pub fn from_code(c: u8) -> Result<Self> {
        match c {
            0 => Ok(Kind::File),
            1 => Ok(Kind::Dir),
            2 => Ok(Kind::Symlink),
            x => Err(Error::Codec(format!("bad kind code {x}"))),
        }
    }
}

/// POSIX inode。`data_ref` 指向内容对象（空 = 未提交内容），字节本体
/// 不在元数据引擎里——字节走客户端自己的数据面，引擎只记账它存在（§5.1）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub ino: Ino,
    pub parent: Ino,
    pub name: Vec<u8>,
    pub kind: Kind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub nlink: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
    pub data_ref: DataRef,
    pub link: Vec<u8>,
}

impl Node {
    pub fn encode(&self, w: &mut W) {
        w.u64(self.ino);
        w.u64(self.parent);
        w.bytes(&self.name);
        w.u8(self.kind.code());
        w.u32(self.mode);
        w.u32(self.uid);
        w.u32(self.gid);
        w.u64(self.size);
        w.u32(self.nlink);
        w.i64(self.atime);
        w.i64(self.mtime);
        w.i64(self.ctime);
        w.bytes(&self.data_ref);
        w.bytes(&self.link);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(Node {
            ino: r.u64()?,
            parent: r.u64()?,
            name: r.bytes()?,
            kind: Kind::from_code(r.u8()?)?,
            mode: r.u32()?,
            uid: r.u32()?,
            gid: r.u32()?,
            size: r.u64()?,
            nlink: r.u32()?,
            atime: r.i64()?,
            mtime: r.i64()?,
            ctime: r.i64()?,
            data_ref: r.bytes()?,
            link: r.bytes()?,
        })
    }
}

/// 文件锁记录，形状同 `locks(ino, owner, kind, lease_expiry)`。
/// `kind`: 0 = fcntl read, 1 = fcntl write, 2 = flock（沿用同一套编码，不重排）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRec {
    pub kind: u8,
    pub lease_expiry: i64,
}

impl LockRec {
    pub fn encode(&self, w: &mut W) {
        w.u8(self.kind);
        w.i64(self.lease_expiry);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(LockRec {
            kind: r.u8()?,
            lease_expiry: r.i64()?,
        })
    }
}

/// refs 账本行：内容对象被引用的次数与字节合计。
///
/// `count` 是权威值（GC 判零只能看它）。`bytes` 维持不变量
/// `bytes == Σ 持有该对象的 inode.size`：提交内容时按 size 记账，
/// `SA_SIZE` 改大小会补差，释放/删除按离开时的 size 减。
/// 显式 [`Op::RefDown`] 是管理面的纠正入口，`bytes` 由调用方给。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefRec {
    pub count: u64,
    pub bytes: u64,
}

impl RefRec {
    pub fn encode(&self, w: &mut W) {
        w.u64(self.count);
        w.u64(self.bytes);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(RefRec {
            count: r.u64()?,
            bytes: r.u64()?,
        })
    }
}

/// 控制面 API 密钥行。列形状照「关系库版 api_keys 表」来，字段都是字节串；
/// 撤销/过期/只读这些规则由客户端解释（§11 第 7 条 T2 会把它换成通用表）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRec {
    pub id: Vec<u8>,
    pub label: Vec<u8>,
    pub secret_hash: Vec<u8>,
    pub created_at: i64,
    pub last_used_at: i64,
    pub revoked_at: i64,
    pub view: Vec<u8>,
    pub ro: bool,
    pub owner: Vec<u8>,
    pub note: Vec<u8>,
    pub expires_at: i64,
    pub created_by: Vec<u8>,
    pub secret_hint: Vec<u8>,
}

impl KeyRec {
    pub fn encode(&self, w: &mut W) {
        w.bytes(&self.id);
        w.bytes(&self.label);
        w.bytes(&self.secret_hash);
        w.i64(self.created_at);
        w.i64(self.last_used_at);
        w.i64(self.revoked_at);
        w.bytes(&self.view);
        w.bool(self.ro);
        w.bytes(&self.owner);
        w.bytes(&self.note);
        w.i64(self.expires_at);
        w.bytes(&self.created_by);
        w.bytes(&self.secret_hint);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(KeyRec {
            id: r.bytes()?,
            label: r.bytes()?,
            secret_hash: r.bytes()?,
            created_at: r.i64()?,
            last_used_at: r.i64()?,
            revoked_at: r.i64()?,
            view: r.bytes()?,
            ro: r.bool()?,
            owner: r.bytes()?,
            note: r.bytes()?,
            expires_at: r.i64()?,
            created_by: r.bytes()?,
            secret_hint: r.bytes()?,
        })
    }
    /// 0 = 不过期（对应关系库里这一列存空串的做法）。
    pub fn expired(&self, now_ms: i64) -> bool {
        self.expires_at != 0 && self.expires_at <= now_ms
    }
    pub fn revoked(&self) -> bool {
        self.revoked_at != 0
    }
}

/// 审计行：一次外部调用的记账，形状同关系库版的 mcp_calls 表。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRec {
    pub seq: u64,
    pub ts: i64,
    pub key_id: Vec<u8>,
    pub method: Vec<u8>,
    pub tool: Vec<u8>,
    pub ok: bool,
    pub detail: Vec<u8>,
}

impl AuditRec {
    pub fn encode(&self, w: &mut W) {
        w.u64(self.seq);
        w.i64(self.ts);
        w.bytes(&self.key_id);
        w.bytes(&self.method);
        w.bytes(&self.tool);
        w.bool(self.ok);
        w.bytes(&self.detail);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(AuditRec {
            seq: r.u64()?,
            ts: r.i64()?,
            key_id: r.bytes()?,
            method: r.bytes()?,
            tool: r.bytes()?,
            ok: r.bool()?,
            detail: r.bytes()?,
        })
    }
}

/// 视图定义行（MCP resource/prompt/tool 定义），对齐 `view_defs`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefRec {
    pub name: Vec<u8>,
    pub kind: Vec<u8>,
    pub payload: Vec<u8>,
    pub updated_at: i64,
}

impl DefRec {
    pub fn encode(&self, w: &mut W) {
        w.bytes(&self.name);
        w.bytes(&self.kind);
        w.bytes(&self.payload);
        w.i64(self.updated_at);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(DefRec {
            name: r.bytes()?,
            kind: r.bytes()?,
            payload: r.bytes()?,
            updated_at: r.i64()?,
        })
    }
}

/// 单个视图的目录树游标：视图名 → id/根 ino/下一个可用 ino。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewMeta {
    pub id: ViewId,
    pub root: Ino,
    pub next_ino: Ino,
}

impl ViewMeta {
    pub fn encode(&self, w: &mut W) {
        w.u32(self.id);
        w.u64(self.root);
        w.u64(self.next_ino);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(ViewMeta {
            id: r.u32()?,
            root: r.u64()?,
            next_ino: r.u64()?,
        })
    }
}

/// `Setattr` 的 valid 位，与 FUSE `SET_ATTR_MODE/UID/GID/SIZE/...` 对齐。
pub const SA_MODE: u32 = 1 << 0;
pub const SA_UID: u32 = 1 << 1;
pub const SA_GID: u32 = 1 << 2;
pub const SA_SIZE: u32 = 1 << 3;
pub const SA_ATIME: u32 = 1 << 4;
pub const SA_MTIME: u32 = 1 << 5;
/// 内部扩展位：data_ref 提交（内容对象写入完成后由 data-server 调用）。
pub const SA_DATA_REF: u32 = 1 << 6;

/// `Rename` 的 flags 位，与 Linux `RENAME_NOREPLACE` 对齐：目标已存在则失败
/// 而不是覆盖。服务端必须自己判：客户端先 `Lookup` 再 `Rename` 之间有窗口，
/// 落进来的同名目标会被静默覆盖掉。
pub const RENAME_NOREPLACE: u32 = 1 << 0;

/// 协议错误码：直接复用 POSIX errno 数值，客户端拿到后可原样回给 FUSE，
/// 不做二次翻译（把后端错误字符串映射成 errno 的做法会丢信息，这里直接回码）。
pub mod errno {
    pub const EPERM: u16 = 1;
    pub const ENOENT: u16 = 2;
    pub const EIO: u16 = 5;
    pub const EBADF: u16 = 9;
    pub const EACCES: u16 = 13;
    pub const EBUSY: u16 = 16;
    pub const EEXIST: u16 = 17;
    pub const EXDEV: u16 = 18;
    pub const ENODEV: u16 = 19;
    pub const ENOTDIR: u16 = 20;
    pub const EISDIR: u16 = 21;
    pub const EINVAL: u16 = 22;
    pub const ENOSPC: u16 = 28;
    pub const EROFS: u16 = 30;
    pub const ENAMETOOLONG: u16 = 36;
    pub const ENOTEMPTY: u16 = 39;
    pub const EAGAIN: u16 = 11;
    pub const EADV: u16 = 68; // 认证被拒（复用既有的「协议错误」码位，不另立）
    pub const ESTALE: u16 = 116;
}

/// 一次请求。字段直接用二进制编码进帧；语义与 POSIX/FUSE 的元数据 op 一一对应。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Ping,
    Stats,
    Checkpoint,
    Shutdown,

    // ---- 视图生命周期 ----
    CreateView {
        name: Vec<u8>,
    },
    DropView {
        name: Vec<u8>,
    },
    ListViews,

    // ---- 目录树 ----
    Lookup {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
    },
    Getattr {
        view: ViewId,
        ino: Ino,
    },
    Setattr {
        view: ViewId,
        ino: Ino,
        valid: u32,
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
        atime: i64,
        mtime: i64,
        data_ref: DataRef,
        ref_bytes: u64,
    },
    /// 创建普通文件（不存在才建；存在 → EEXIST，即 POSIX `O_CREAT|O_EXCL`）。
    Create {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Mkdir {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    Symlink {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
        target: Vec<u8>,
        uid: u32,
        gid: u32,
    },
    Readlink {
        view: ViewId,
        ino: Ino,
    },
    /// 删除一条目录项。应答是 [`Reply::NodeRef`]：内容对象若刚好失去最后一个
    /// 持有者，键原样带回（空串 = 还有人持有 / 本来就没内容），调用方据此删字节。
    Unlink {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
    },
    /// 同 [`Op::Unlink`]，只是限定目录。
    Rmdir {
        view: ViewId,
        parent: Ino,
        name: Vec<u8>,
    },
    /// 改名/移动，可覆盖目标。被覆盖目标若失去最后一个持有者，键走
    /// [`Reply::NodeRef`] 带回（没覆盖任何东西时回空串）。
    Rename {
        view: ViewId,
        old_parent: Ino,
        old_name: Vec<u8>,
        new_parent: Ino,
        new_name: Vec<u8>,
        /// [`RENAME_NOREPLACE`] 位图。
        flags: u32,
    },
    ReadDir {
        view: ViewId,
        parent: Ino,
        offset: u64,
        limit: u32,
    },

    // ---- 共享账本与锁 ----
    RefLookup {
        data_ref: DataRef,
    },
    RefUp {
        data_ref: DataRef,
        bytes: u64,
    },
    RefDown {
        data_ref: DataRef,
        bytes: u64,
    },
    LockQuery {
        view: ViewId,
        ino: Ino,
    },
    /// verb: 0 = 建立/续约, 1 = 释放该 owner 在此 inode 的全部锁。
    LockSet {
        view: ViewId,
        ino: Ino,
        owner: Vec<u8>,
        verb: u8,
        kind: u8,
        lease_expiry: i64,
    },

    // ---- 控制面 ----
    KeyCreate {
        key: KeyRec,
    },
    KeyGet {
        id: Vec<u8>,
    },
    KeyList,
    /// verb: 0 = revoke, 1 = rotate（换 hash+hint）, 2 = touch, 3 = set_ro。
    KeyUpdate {
        id: Vec<u8>,
        verb: u8,
        ts: i64,
        new_hash: Vec<u8>,
        new_hint: Vec<u8>,
        ro: bool,
    },
    /// verb: 0 = auth, 1 = live check。
    KeyCheck {
        id: Vec<u8>,
        verb: u8,
        hash: Vec<u8>,
        now_ms: i64,
    },
    AuditAppend {
        row: AuditRec,
    },
    AuditList {
        limit: u32,
    },
    DefPut {
        def: DefRec,
    },
    DefGet {
        name: Vec<u8>,
    },
    DefList {
        kind: Vec<u8>,
    },
    DefDel {
        name: Vec<u8>,
    },
    PlaceSet {
        view: Vec<u8>,
        instance: Vec<u8>,
    },
    PlaceGet {
        view: Vec<u8>,
    },
    PlaceList,
}

impl Op {
    pub fn tag(&self) -> u16 {
        match self {
            Op::Ping => 0x0001,
            Op::Stats => 0x0002,
            Op::Checkpoint => 0x0003,
            Op::Shutdown => 0x0004,
            Op::CreateView { .. } => 0x0010,
            Op::DropView { .. } => 0x0011,
            Op::ListViews => 0x0012,
            Op::Lookup { .. } => 0x0020,
            Op::Getattr { .. } => 0x0021,
            Op::Setattr { .. } => 0x0022,
            Op::Create { .. } => 0x0023,
            Op::Mkdir { .. } => 0x0024,
            Op::Symlink { .. } => 0x0025,
            Op::Readlink { .. } => 0x0026,
            Op::Unlink { .. } => 0x0027,
            Op::Rmdir { .. } => 0x0028,
            Op::Rename { .. } => 0x0029,
            Op::ReadDir { .. } => 0x002a,
            Op::RefLookup { .. } => 0x0030,
            Op::RefUp { .. } => 0x0031,
            Op::RefDown { .. } => 0x0032,
            Op::LockQuery { .. } => 0x0033,
            Op::LockSet { .. } => 0x0034,
            Op::KeyCreate { .. } => 0x0040,
            Op::KeyGet { .. } => 0x0041,
            Op::KeyList => 0x0042,
            Op::KeyUpdate { .. } => 0x0043,
            Op::KeyCheck { .. } => 0x0044,
            Op::AuditAppend { .. } => 0x0045,
            Op::AuditList { .. } => 0x0046,
            Op::DefPut { .. } => 0x0050,
            Op::DefGet { .. } => 0x0051,
            Op::DefList { .. } => 0x0052,
            Op::DefDel { .. } => 0x0053,
            Op::PlaceSet { .. } => 0x0060,
            Op::PlaceGet { .. } => 0x0061,
            Op::PlaceList => 0x0062,
        }
    }

    /// 该 op 是否**只改统计、不改状态**（`translate` 返回空 logs）。
    ///
    /// 这是读工作池的分派依据：只有这里的 `true` 才会被丢给 worker 线程并行
    /// 执行。判定必须与 `apply::translate` 的实际行为一致，否则一条被误判为
    /// 只读的写会在 worker 里产生 logs 却没人把它写进 WAL —— 那等于丢一次已
    /// 确认的写。所以这里是穷举 match（加 op 不改这里就编不过），并由
    /// `tests/read_scaling.rs::readonly_classification_matches_translate` 逐条
    /// 拿真实状态跑一遍 `translate`，断言判为只读的确实不产 logs。
    ///
    /// `Stats` 判 `false`：它要读 WAL 水位并动累计计数，属于控制面而不属于
    /// 可并行的数据面读；`Checkpoint`/`Shutdown` 同理。
    pub fn is_readonly(&self) -> bool {
        match self {
            Op::Ping
            | Op::ListViews
            | Op::Lookup { .. }
            | Op::Getattr { .. }
            | Op::Readlink { .. }
            | Op::ReadDir { .. }
            | Op::RefLookup { .. }
            | Op::LockQuery { .. }
            | Op::KeyGet { .. }
            | Op::KeyList
            | Op::KeyCheck { .. }
            | Op::AuditList { .. }
            | Op::DefGet { .. }
            | Op::DefList { .. }
            | Op::PlaceGet { .. }
            | Op::PlaceList => true,
            Op::Stats
            | Op::Checkpoint
            | Op::Shutdown
            | Op::CreateView { .. }
            | Op::DropView { .. }
            | Op::Setattr { .. }
            | Op::Create { .. }
            | Op::Mkdir { .. }
            | Op::Symlink { .. }
            | Op::Unlink { .. }
            | Op::Rmdir { .. }
            | Op::Rename { .. }
            | Op::RefUp { .. }
            | Op::RefDown { .. }
            | Op::LockSet { .. }
            | Op::KeyCreate { .. }
            | Op::KeyUpdate { .. }
            | Op::AuditAppend { .. }
            | Op::DefPut { .. }
            | Op::DefDel { .. }
            | Op::PlaceSet { .. } => false,
        }
    }

    pub fn encode(&self, w: &mut W) {
        w.u16(self.tag());
        match self {
            Op::Ping
            | Op::Stats
            | Op::Checkpoint
            | Op::Shutdown
            | Op::ListViews
            | Op::KeyList
            | Op::PlaceList => {}
            Op::AuditList { limit } => w.u32(*limit),
            Op::CreateView { name } => w.bytes(name),
            Op::DropView { name } => w.bytes(name),
            Op::Lookup {
                view,
                parent,
                name,
            } => {
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
            }
            Op::Getattr { view, ino } | Op::Readlink { view, ino } => {
                w.u32(*view);
                w.u64(*ino);
            }
            Op::Setattr {
                view,
                ino,
                valid,
                mode,
                uid,
                gid,
                size,
                atime,
                mtime,
                data_ref,
                ref_bytes,
            } => {
                w.u32(*view);
                w.u64(*ino);
                w.u32(*valid);
                w.u32(*mode);
                w.u32(*uid);
                w.u32(*gid);
                w.u64(*size);
                w.i64(*atime);
                w.i64(*mtime);
                w.bytes(data_ref);
                w.u64(*ref_bytes);
            }
            Op::Create {
                view,
                parent,
                name,
                mode,
                uid,
                gid,
            }
            | Op::Mkdir {
                view,
                parent,
                name,
                mode,
                uid,
                gid,
            } => {
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
                w.u32(*mode);
                w.u32(*uid);
                w.u32(*gid);
            }
            Op::Symlink {
                view,
                parent,
                name,
                target,
                uid,
                gid,
            } => {
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
                w.bytes(target);
                w.u32(*uid);
                w.u32(*gid);
            }
            Op::Unlink { view, parent, name } | Op::Rmdir { view, parent, name } => {
                w.u32(*view);
                w.u64(*parent);
                w.bytes(name);
            }
            Op::Rename {
                view,
                old_parent,
                old_name,
                new_parent,
                new_name,
                flags,
            } => {
                w.u32(*view);
                w.u64(*old_parent);
                w.bytes(old_name);
                w.u64(*new_parent);
                w.bytes(new_name);
                w.u32(*flags);
            }
            Op::ReadDir {
                view,
                parent,
                offset,
                limit,
            } => {
                w.u32(*view);
                w.u64(*parent);
                w.u64(*offset);
                w.u32(*limit);
            }
            Op::RefLookup { data_ref } => w.bytes(data_ref),
            Op::RefUp { data_ref, bytes } | Op::RefDown { data_ref, bytes } => {
                w.bytes(data_ref);
                w.u64(*bytes);
            }
            Op::LockQuery { view, ino } => {
                w.u32(*view);
                w.u64(*ino);
            }
            Op::LockSet {
                view,
                ino,
                owner,
                verb,
                kind,
                lease_expiry,
            } => {
                w.u32(*view);
                w.u64(*ino);
                w.bytes(owner);
                w.u8(*verb);
                w.u8(*kind);
                w.i64(*lease_expiry);
            }
            Op::KeyCreate { key } => key.encode(w),
            Op::KeyGet { id } => w.bytes(id),
            Op::KeyUpdate {
                id,
                verb,
                ts,
                new_hash,
                new_hint,
                ro,
            } => {
                w.bytes(id);
                w.u8(*verb);
                w.i64(*ts);
                w.bytes(new_hash);
                w.bytes(new_hint);
                w.bool(*ro);
            }
            Op::KeyCheck { id, verb, hash, now_ms } => {
                w.bytes(id);
                w.u8(*verb);
                w.bytes(hash);
                w.i64(*now_ms);
            }
            Op::AuditAppend { row } => row.encode(w),
            Op::DefPut { def } => def.encode(w),
            Op::DefGet { name } | Op::DefDel { name } => w.bytes(name),
            Op::DefList { kind } => w.bytes(kind),
            Op::PlaceSet { view, instance } => {
                w.bytes(view);
                w.bytes(instance);
            }
            Op::PlaceGet { view } => w.bytes(view),
        }
    }

    pub fn decode(r: &mut Rd) -> Result<Op> {
        let tag = r.u16()?;
        let op = match tag {
            0x0001 => Op::Ping,
            0x0002 => Op::Stats,
            0x0003 => Op::Checkpoint,
            0x0004 => Op::Shutdown,
            0x0010 => Op::CreateView { name: r.bytes()? },
            0x0011 => Op::DropView { name: r.bytes()? },
            0x0012 => Op::ListViews,
            0x0020 => Op::Lookup {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
            },
            0x0021 => Op::Getattr {
                view: r.u32()?,
                ino: r.u64()?,
            },
            0x0022 => Op::Setattr {
                view: r.u32()?,
                ino: r.u64()?,
                valid: r.u32()?,
                mode: r.u32()?,
                uid: r.u32()?,
                gid: r.u32()?,
                size: r.u64()?,
                atime: r.i64()?,
                mtime: r.i64()?,
                data_ref: r.bytes()?,
                ref_bytes: r.u64()?,
            },
            0x0023 => Op::Create {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
                mode: r.u32()?,
                uid: r.u32()?,
                gid: r.u32()?,
            },
            0x0024 => Op::Mkdir {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
                mode: r.u32()?,
                uid: r.u32()?,
                gid: r.u32()?,
            },
            0x0025 => Op::Symlink {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
                target: r.bytes()?,
                uid: r.u32()?,
                gid: r.u32()?,
            },
            0x0026 => Op::Readlink {
                view: r.u32()?,
                ino: r.u64()?,
            },
            0x0027 => Op::Unlink {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
            },
            0x0028 => Op::Rmdir {
                view: r.u32()?,
                parent: r.u64()?,
                name: r.bytes()?,
            },
            0x0029 => Op::Rename {
                view: r.u32()?,
                old_parent: r.u64()?,
                old_name: r.bytes()?,
                new_parent: r.u64()?,
                new_name: r.bytes()?,
                flags: r.u32()?,
            },
            0x002a => Op::ReadDir {
                view: r.u32()?,
                parent: r.u64()?,
                offset: r.u64()?,
                limit: r.u32()?,
            },
            0x0030 => Op::RefLookup { data_ref: r.bytes()? },
            0x0031 => Op::RefUp {
                data_ref: r.bytes()?,
                bytes: r.u64()?,
            },
            0x0032 => Op::RefDown {
                data_ref: r.bytes()?,
                bytes: r.u64()?,
            },
            0x0033 => Op::LockQuery {
                view: r.u32()?,
                ino: r.u64()?,
            },
            0x0034 => Op::LockSet {
                view: r.u32()?,
                ino: r.u64()?,
                owner: r.bytes()?,
                verb: r.u8()?,
                kind: r.u8()?,
                lease_expiry: r.i64()?,
            },
            0x0040 => Op::KeyCreate {
                key: KeyRec::decode(r)?,
            },
            0x0041 => Op::KeyGet { id: r.bytes()? },
            0x0042 => Op::KeyList,
            0x0043 => Op::KeyUpdate {
                id: r.bytes()?,
                verb: r.u8()?,
                ts: r.i64()?,
                new_hash: r.bytes()?,
                new_hint: r.bytes()?,
                ro: r.bool()?,
            },
            0x0044 => Op::KeyCheck {
                id: r.bytes()?,
                verb: r.u8()?,
                hash: r.bytes()?,
                now_ms: r.i64()?,
            },
            0x0045 => Op::AuditAppend {
                row: AuditRec::decode(r)?,
            },
            0x0046 => Op::AuditList { limit: r.u32()? },
            0x0050 => Op::DefPut {
                def: DefRec::decode(r)?,
            },
            0x0051 => Op::DefGet { name: r.bytes()? },
            0x0052 => Op::DefList { kind: r.bytes()? },
            0x0053 => Op::DefDel { name: r.bytes()? },
            0x0060 => Op::PlaceSet {
                view: r.bytes()?,
                instance: r.bytes()?,
            },
            0x0061 => Op::PlaceGet { view: r.bytes()? },
            0x0062 => Op::PlaceList,
            x => return Err(Error::Codec(format!("unknown op tag 0x{x:04x}"))),
        };
        Ok(op)
    }
}

/// 一次应答。`status=0` 成功；否则 `errno` 有效。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Pong,
    Err { code: u16, msg: Vec<u8> },
    View {
        id: ViewId,
        root: Ino,
    },
    Views(Vec<(Vec<u8>, ViewId, Ino)>),
    Node(Node),
    /// GC 线索：一个内容对象**刚好失去最后一个持有者**。空串是成功应答的一部分
    /// （意为「没有对象需要删字节」），不是错误。目前由
    /// [`Op::Unlink`] / [`Op::Rmdir`] / [`Op::Rename`] 返回。
    NodeRef(DataRef),
    Dirents {
        inos: Vec<Ino>,
        kinds: Vec<u8>,
        names: Vec<Vec<u8>>,
        next: u64,
        end: bool,
    },
    Ref {
        count: u64,
        bytes: u64,
    },
    Locks(Vec<(Vec<u8>, u8, i64)>),
    Key(Box<KeyRec>),
    Keys(Vec<KeyRec>),
    Authed {
        label: Vec<u8>,
        ro: bool,
        view: Vec<u8>,
        owner: Vec<u8>,
    },
    Live(bool),
    Audits(Vec<AuditRec>),
    Def(Option<DefRec>),
    Defs(Vec<DefRec>),
    Place(Vec<u8>),
    Places(Vec<(Vec<u8>, Vec<u8>)>),
    Stats(Box<crate::engine::core::EngineStats>),
}

impl Reply {
    pub fn encode(&self, w: &mut W) {
        match self {
            Reply::Ok => w.u16(0x0100),
            Reply::Pong => w.u16(0x0101),
            Reply::Err { code, msg } => {
                w.u16(0x01ff);
                w.u16(*code);
                w.bytes(msg);
            }
            Reply::View { id, root } => {
                w.u16(0x0110);
                w.u32(*id);
                w.u64(*root);
            }
            Reply::Views(v) => {
                w.u16(0x0111);
                w.u32(v.len() as u32);
                for (name, id, root) in v {
                    w.bytes(name);
                    w.u32(*id);
                    w.u64(*root);
                }
            }
            Reply::Node(n) => {
                w.u16(0x0120);
                n.encode(w);
            }
            Reply::NodeRef(d) => {
                w.u16(0x0121);
                w.bytes(d);
            }
            Reply::Dirents {
                inos,
                kinds,
                names,
                next,
                end,
            } => {
                w.u16(0x0122);
                let n = inos.len();
                w.u32(n as u32);
                for i in 0..n {
                    w.u64(inos[i]);
                    w.u8(kinds[i]);
                    w.bytes(&names[i]);
                }
                w.u64(*next);
                w.bool(*end);
            }
            Reply::Ref { count, bytes } => {
                w.u16(0x0130);
                w.u64(*count);
                w.u64(*bytes);
            }
            Reply::Locks(v) => {
                w.u16(0x0131);
                w.u32(v.len() as u32);
                for (owner, kind, exp) in v {
                    w.bytes(owner);
                    w.u8(*kind);
                    w.i64(*exp);
                }
            }
            Reply::Key(k) => {
                w.u16(0x0140);
                k.encode(w);
            }
            Reply::Keys(v) => {
                w.u16(0x0141);
                w.u32(v.len() as u32);
                for k in v {
                    k.encode(w);
                }
            }
            Reply::Authed {
                label,
                ro,
                view,
                owner,
            } => {
                w.u16(0x0142);
                w.bytes(label);
                w.bool(*ro);
                w.bytes(view);
                w.bytes(owner);
            }
            Reply::Live(b) => {
                w.u16(0x0143);
                w.bool(*b);
            }
            Reply::Audits(v) => {
                w.u16(0x0144);
                w.u32(v.len() as u32);
                for a in v {
                    a.encode(w);
                }
            }
            Reply::Def(d) => {
                w.u16(0x0150);
                match d {
                    Some(x) => {
                        w.u8(1);
                        x.encode(w);
                    }
                    None => w.u8(0),
                }
            }
            Reply::Defs(v) => {
                w.u16(0x0151);
                w.u32(v.len() as u32);
                for d in v {
                    d.encode(w);
                }
            }
            Reply::Place(p) => {
                w.u16(0x0160);
                w.bytes(p);
            }
            Reply::Places(v) => {
                w.u16(0x0161);
                w.u32(v.len() as u32);
                for (view, inst) in v {
                    w.bytes(view);
                    w.bytes(inst);
                }
            }
            Reply::Stats(s) => {
                w.u16(0x0102);
                s.encode(w);
            }
        }
    }

    pub fn decode(r: &mut Rd) -> Result<Reply> {
        let tag = r.u16()?;
        let rep = match tag {
            0x0100 => Reply::Ok,
            0x0101 => Reply::Pong,
            0x0102 => Reply::Stats(Box::new(crate::engine::core::EngineStats::decode(r)?)),
            0x01ff => Reply::Err {
                code: r.u16()?,
                msg: r.bytes()?,
            },
            0x0110 => Reply::View {
                id: r.u32()?,
                root: r.u64()?,
            },
            0x0111 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(100_000));
                for _ in 0..n {
                    v.push((r.bytes()?, r.u32()?, r.u64()?));
                }
                Reply::Views(v)
            }
            0x0120 => Reply::Node(Node::decode(r)?),
            0x0121 => Reply::NodeRef(r.bytes()?),
            0x0122 => {
                let n = r.u32()? as usize;
                let mut inos = Vec::with_capacity(n.min(200_000));
                let mut kinds = Vec::with_capacity(n.min(200_000));
                let mut names = Vec::with_capacity(n.min(200_000));
                for _ in 0..n {
                    inos.push(r.u64()?);
                    kinds.push(r.u8()?);
                    names.push(r.bytes()?);
                }
                Reply::Dirents {
                    inos,
                    kinds,
                    names,
                    next: r.u64()?,
                    end: r.bool()?,
                }
            }
            0x0130 => Reply::Ref {
                count: r.u64()?,
                bytes: r.u64()?,
            },
            0x0131 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(100_000));
                for _ in 0..n {
                    v.push((r.bytes()?, r.u8()?, r.i64()?));
                }
                Reply::Locks(v)
            }
            0x0140 => Reply::Key(Box::new(KeyRec::decode(r)?)),
            0x0141 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(100_000));
                for _ in 0..n {
                    v.push(KeyRec::decode(r)?);
                }
                Reply::Keys(v)
            }
            0x0142 => Reply::Authed {
                label: r.bytes()?,
                ro: r.bool()?,
                view: r.bytes()?,
                owner: r.bytes()?,
            },
            0x0143 => Reply::Live(r.bool()?),
            0x0144 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(200_000));
                for _ in 0..n {
                    v.push(AuditRec::decode(r)?);
                }
                Reply::Audits(v)
            }
            0x0150 => {
                if r.u8()? == 1 {
                    Reply::Def(Some(DefRec::decode(r)?))
                } else {
                    Reply::Def(None)
                }
            }
            0x0151 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(100_000));
                for _ in 0..n {
                    v.push(DefRec::decode(r)?);
                }
                Reply::Defs(v)
            }
            0x0160 => Reply::Place(r.bytes()?),
            0x0161 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(100_000));
                for _ in 0..n {
                    v.push((r.bytes()?, r.bytes()?));
                }
                Reply::Places(v)
            }
            x => return Err(Error::Codec(format!("unknown reply tag 0x{x:04x}"))),
        };
        Ok(rep)
    }

    pub fn err(code: u16, msg: impl Into<String>) -> Reply {
        Reply::Err {
            code,
            msg: msg.into().into_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_codec_round_trip_all_tags() {
        let ops = vec![
            Op::Ping,
            Op::Stats,
            Op::Checkpoint,
            Op::Shutdown,
            Op::CreateView {
                name: b"alice".to_vec(),
            },
            Op::DropView {
                name: b"alice".to_vec(),
            },
            Op::ListViews,
            Op::Lookup {
                view: 3,
                parent: 1,
                name: b"f.txt".to_vec(),
            },
            Op::Getattr { view: 3, ino: 7 },
            Op::Readlink { view: 3, ino: 7 },
            Op::Setattr {
                view: 3,
                ino: 7,
                valid: SA_SIZE | SA_DATA_REF,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                size: 4096,
                atime: -5,
                mtime: 5,
                data_ref: b"obj-42".to_vec(),
                ref_bytes: 99,
            },
            Op::Create {
                view: 3,
                parent: 1,
                name: b"x".to_vec(),
                mode: 0o644,
                uid: 1,
                gid: 2,
            },
            Op::Mkdir {
                view: 3,
                parent: 1,
                name: b"d".to_vec(),
                mode: 0o755,
                uid: 1,
                gid: 2,
            },
            Op::Symlink {
                view: 3,
                parent: 1,
                name: b"s".to_vec(),
                target: b"/a/b".to_vec(),
                uid: 1,
                gid: 2,
            },
            Op::Unlink {
                view: 3,
                parent: 1,
                name: b"x".to_vec(),
            },
            Op::Rmdir {
                view: 3,
                parent: 1,
                name: b"d".to_vec(),
            },
            Op::Rename {
                view: 3,
                old_parent: 1,
                old_name: b"a".to_vec(),
                new_parent: 2,
                new_name: b"b".to_vec(),
                flags: 0,
            },
            Op::ReadDir {
                view: 3,
                parent: 1,
                offset: 10,
                limit: 100,
            },
            Op::RefLookup { data_ref: b"r8".to_vec() },
            Op::RefUp {
                data_ref: b"r8".to_vec(),
                bytes: 123,
            },
            Op::RefDown {
                data_ref: b"r8".to_vec(),
                bytes: 123,
            },
            Op::LockQuery { view: 3, ino: 5 },
            Op::LockSet {
                view: 3,
                ino: 5,
                owner: b"p1234".to_vec(),
                verb: 0,
                kind: 1,
                lease_expiry: -9,
            },
            Op::KeyCreate {
                key: sample_key(),
            },
            Op::KeyGet { id: b"abcd".to_vec() },
            Op::KeyList,
            Op::KeyUpdate {
                id: b"abcd".to_vec(),
                verb: 1,
                ts: 5,
                new_hash: b"h".to_vec(),
                new_hint: b"hh".to_vec(),
                ro: true,
            },
            Op::KeyCheck {
                id: b"abcd".to_vec(),
                verb: 0,
                hash: b"h".to_vec(),
                now_ms: 1,
            },
            Op::AuditAppend {
                row: AuditRec {
                    seq: 1,
                    ts: 2,
                    key_id: b"k".to_vec(),
                    method: b"m".to_vec(),
                    tool: b"t".to_vec(),
                    ok: true,
                    detail: b"d".to_vec(),
                },
            },
            Op::AuditList { limit: 5 },
            Op::DefPut {
                def: DefRec {
                    name: b"n".to_vec(),
                    kind: b"prompt".to_vec(),
                    payload: b"{}".to_vec(),
                    updated_at: 3,
                },
            },
            Op::DefGet { name: b"n".to_vec() },
            Op::DefList {
                kind: b"prompt".to_vec(),
            },
            Op::DefDel { name: b"n".to_vec() },
            Op::PlaceSet {
                view: b"v".to_vec(),
                instance: b"i".to_vec(),
            },
            Op::PlaceGet { view: b"v".to_vec() },
            Op::PlaceList,
        ];
        for op in ops {
            let mut w = W::new();
            op.encode(&mut w);
            let bytes = w.finish();
            let mut r = Rd::new(&bytes);
            let back = Op::decode(&mut r).expect("decode op");
            assert_eq!(op, back, "op tag 0x{:04x}", op.tag());
            assert_eq!(r.remaining(), 0, "trailing bytes for tag 0x{:04x}", op.tag());
        }
    }

    #[test]
    fn reply_codec_round_trip() {
        let replies = vec![
            Reply::Ok,
            Reply::Pong,
            Reply::Err {
                code: errno::ENOENT,
                msg: b"no such file".to_vec(),
            },
            Reply::View { id: 2, root: 1 },
            Reply::Views(vec![(b"alice".to_vec(), 1, 1)]),
            Reply::Node(Node {
                ino: 2,
                parent: 1,
                name: b"a".to_vec(),
                kind: Kind::Dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                size: 3,
                nlink: 2,
                atime: -1,
                mtime: -2,
                ctime: -3,
                data_ref: Vec::new(),
                link: Vec::new(),
            }),
            Reply::NodeRef(b"r9".to_vec()),
            Reply::Dirents {
                inos: vec![2, 3],
                kinds: vec![0, 1],
                names: vec![b"a".to_vec(), b"b".to_vec()],
                next: 4,
                end: false,
            },
            Reply::Ref {
                count: 5,
                bytes: 500,
            },
            Reply::Locks(vec![(b"o1".to_vec(), 1, 99)]),
            Reply::Key(Box::new(sample_key())),
            Reply::Keys(vec![sample_key()]),
            Reply::Authed {
                label: b"lbl".to_vec(),
                ro: true,
                view: b"v".to_vec(),
                owner: b"o".to_vec(),
            },
            Reply::Live(true),
            Reply::Audits(vec![AuditRec {
                seq: 1,
                ts: 2,
                key_id: b"k".to_vec(),
                method: b"m".to_vec(),
                tool: b"t".to_vec(),
                ok: false,
                detail: b"d".to_vec(),
            }]),
            Reply::Def(Some(DefRec {
                name: b"n".to_vec(),
                kind: b"k".to_vec(),
                payload: b"p".to_vec(),
                updated_at: 1,
            })),
            Reply::Def(None),
            Reply::Defs(vec![DefRec {
                name: b"n".to_vec(),
                kind: b"k".to_vec(),
                payload: b"p".to_vec(),
                updated_at: 1,
            }]),
            Reply::Place(b"inst".to_vec()),
            Reply::Places(vec![(b"v".to_vec(), b"i".to_vec())]),
        ];
        for rep in replies {
            let mut w = W::new();
            rep.encode(&mut w);
            let bytes = w.finish();
            let mut r = Rd::new(&bytes);
            let back = Reply::decode(&mut r).expect("decode reply");
            assert_eq!(rep, back, "reply mismatch");
            assert_eq!(r.remaining(), 0);
        }
    }

    fn sample_key() -> KeyRec {
        KeyRec {
            id: b"0123456789abcdef".to_vec(),
            label: b"analyst".to_vec(),
            secret_hash: b"deadbeef".to_vec(),
            created_at: 1_700_000_000_000,
            last_used_at: 0,
            revoked_at: 0,
            view: b"alice".to_vec(),
            ro: false,
            owner: b"team-a".to_vec(),
            note: Vec::new(),
            expires_at: 0,
            created_by: b"admin".to_vec(),
            secret_hint: b"abcd~ef01".to_vec(),
        }
    }

    #[test]
    fn kind_and_expired_boundaries() {
        assert_eq!(Kind::from_code(1).unwrap(), Kind::Dir);
        assert!(Kind::from_code(3).is_err());
        let mut k = sample_key();
        assert!(!k.revoked());
        assert!(!k.expired(1));
        k.revoked_at = 5;
        assert!(k.revoked());
        k.revoked_at = 0;
        k.expires_at = 100;
        assert!(k.expired(100));
        assert!(!k.expired(99));
    }
}
