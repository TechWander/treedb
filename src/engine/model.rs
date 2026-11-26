//! 引擎数据模型与协议语义类型。
//!
//! 概念对齐「把元数据存进关系库」的那张表形状（`pg_store` 这类实现），但去掉 SQL：
//! - `node` 表 → [`Node`]（inode）+ `dentries`（`(view,parent,name) → ino`，
//!   内存里按 `(view,parent)` 分两层存，见 [`crate::engine::mem::State`]）；
//! - `locks` 表 → [`LockRec`]（`(view,ino,owner) → kind,lease`）；
//! - 内容引用账本 → [`RefRec`]（`data_ref → count,size`，跨视图共享）；
//! - 通用表 → [`TableDef`] / [`RowRec`]：列名与语义由客户端定义，引擎不解释。
//!
//! 命名与列语义与关系库那一版逐字段对得上，客户端接入时上层映射是翻译，
//! 不是重新建模 —— 这是这套形状存在的原因，不是对某个具体客户端的契约。
//!
//! 这里曾经还有**按某个客户端照抄的四张专用表**（命名密钥 / 调用审计 / 视图
//! 定义 / 放置），2026-11-23 按 DESIGN §11 第 10 条（M4）整体删掉了：接入侧
//! 已经用通用表拼出等价语义并跑过端到端，内核不再保留业务表。

use crate::engine::codec::{Rd, W};
use crate::engine::sha256::DIGEST_LEN;
use crate::error::{Error, Result};

pub type ViewId = u32;
pub type Ino = u64;

/// 握手随机数的长度（字节），两个方向各一个（§4.4）。
///
/// 32 = 与 HMAC-SHA256 的输出同宽，也等于 256 位安全性 —— 挑战值的宽度低于
/// 摘要宽度就是自削（能碰撞到重放）。
pub const NONCE_LEN: usize = 32;
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

// ------------------------------------------------------------ 通用表（§11 T2）
//
// 内核只给「字节列 + 主键 + 约束」这一层：列名由客户端起，列的含义引擎不解释。
// 这一组类型存在的理由就是把「客户端的表形状」挡在内核之外 —— 引擎里再也没有
// 任何一张按某个具体业务照抄的专用表。

/// 通用表的一列定义。只有名字与约束位，**没有类型**：值一律是不透明字节，
/// 比较只做字节相等，排序只按主键字节序。
///
/// 不给类型是刻意的：类型（时间戳、布尔、金额）一进内核，客户端的语义就会
/// 顺着类型名漏进来，而引擎没法替所有人定义「什么是一个合法的过期时间」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: Vec<u8>,
    pub flags: u32,
}

/// 该列取值全表唯一：跨行撞值 → `EEXIST`。隐含建索引。
pub const COL_UNIQUE: u32 = 1 << 0;
/// 为该列建二级索引，允许 `RowList` / `RowCount` 按等值列举（非唯一）。
pub const COL_INDEX: u32 = 1 << 1;

impl ColumnDef {
    pub fn encode(&self, w: &mut W) {
        w.bytes(&self.name);
        w.u32(self.flags);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(ColumnDef {
            name: r.bytes()?,
            flags: r.u32()?,
        })
    }
    /// 是否需要维护二级索引（唯一列必然要）。
    pub fn indexed(&self) -> bool {
        self.flags & (COL_UNIQUE | COL_INDEX) != 0
    }
    pub fn unique(&self) -> bool {
        self.flags & COL_UNIQUE != 0
    }
}

/// 一张通用表的形状。表 id 由引擎单调分配（与 view id 同构）且**不进 def**：
/// 客户端只用名字寻址，id 只是 WAL 记录与内部索引里的键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub name: Vec<u8>,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    pub fn encode(&self, w: &mut W) {
        w.bytes(&self.name);
        w.u32(self.columns.len() as u32);
        for c in &self.columns {
            c.encode(w);
        }
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        let name = r.bytes()?;
        let n = r.u32()? as usize;
        if n > MAX_COLUMNS {
            return Err(Error::Codec(format!(
                "table has {n} columns, over {MAX_COLUMNS}"
            )));
        }
        let mut columns = Vec::with_capacity(n);
        for _ in 0..n {
            columns.push(ColumnDef::decode(r)?);
        }
        Ok(TableDef { name, columns })
    }
}

/// 一行：主键 + 与 def 等长的列值。
///
/// 空 `Vec<u8>` 是一列的合法取值，所以约束与条件判定不能拿「空字节」当
/// 「这一列没值」—— 要区分就得由客户端自己存一个非空哨兵。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRec {
    pub key: Vec<u8>,
    pub cols: Vec<Vec<u8>>,
}

impl RowRec {
    pub fn encode(&self, w: &mut W) {
        w.bytes(&self.key);
        w.u32(self.cols.len() as u32);
        for c in &self.cols {
            w.bytes(c);
        }
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        let key = r.bytes()?;
        let n = r.u32()? as usize;
        if n > MAX_COLUMNS {
            return Err(Error::Codec(format!(
                "row has {n} columns, over {MAX_COLUMNS}"
            )));
        }
        let mut cols = Vec::with_capacity(n);
        for _ in 0..n {
            cols.push(r.bytes()?);
        }
        Ok(RowRec { key, cols })
    }
}

/// 表名上限：与视图名那条 25 字节不同源（那条是对齐关系库既有约束留下的），
/// 通用表的名字由客户端起，引擎只给它一个上界和「非空」这条底线。
pub const TABLE_NAME_MAX: usize = 255;
/// 主键上限。索引扫描按 `(表, 列, 值, 主键)` 排，主键参与比较，长度直接换算成
/// 每次比较的字节数，所以这里比列值严一点；再长就该换定长摘要而不是放宽它。
pub const ROW_KEY_MAX: usize = 4096;
/// 一张表的列数上限。列是定长编码（按序号寻址），放宽等于把一行的
/// 元数据成本摊给每一次 `RowPut`。
pub const MAX_COLUMNS: usize = 64;
/// 一个应答里能列出的表数 / 行数上限。帧长（`MAX_FRAME`）已经给了硬上限，
/// 这一条是给解码侧的 `Vec::with_capacity` 用的：不能拿到一个敌意的计数
/// 就预分配百亿元素。超限直接拒帧，不截断。
pub const MAX_TABLE_ITEMS: usize = 1_000_000;

/// 一个 [`Op::Batch`] 最多装多少条子 op。
///
/// 上界不是为了「协议装不下」，而是两笔合在一起：含写的批要在写锁里
/// `clone` 一份 `State` 当预演副本，批越宽这次拷贝（以及锁的持有时间）越长；
/// 应答还要把每条的应答都带回去。64 条够「一次 rename + 若干条 setattr」这一档，
/// 更大的批量应该由调用方自己分轮 —— 分摊 fsync 本来就有 pipeline 那条路（§7.9），
/// `Batch` 多出来的是跨 op 的可见性与原子性，不是吞吐。
pub const MAX_BATCH_OPS: usize = 64;

/// `RowList` / `RowCount` / `RowRange` / `RowRangeDelete` 的 `column` 保留值：按主键序
/// 遍历全表，`value` 忽略。
///
/// **它和 [`COND_NONE`] 是同一个数**，这不是疏忽，而是 `cond_col` 那一档根本不该写它：
/// 引擎的条件闸门是 `if cond_col != COND_NONE`（`apply.rs`），所以把 `ROW_PK` 当条件列
/// 递进来会走「无条件」那一条分支，`cond_val` 整个被忽略 —— 一句本该 CAS 的写会静默
/// 变成裸写。主键 CAS 本身也是空命题（行就是按主键寻址的，比较 `key == key` 恒真），
/// 因此这条不打算修成一种新语义，而是在 [`crate::cli`] 那一侧直接拒掉（DESIGN §5.4）。
pub const ROW_PK: u32 = u32::MAX;
/// `RowPut` / `RowDelete` 的 `cond_col` 保留值：不带条件。
///
/// 只能取这一个值 —— 见 [`ROW_PK`] 末段那条同值警告。
pub const COND_NONE: u32 = u32::MAX;

/// `RowPut` 的 flags。两者同时给是客户端的错，按 `EINVAL` 拒。
pub const PUT_IF_ABSENT: u32 = 1 << 0;
pub const PUT_IF_EXISTS: u32 = 1 << 1;
/// `RowDelete` 的 flags：不给则删不存在的行幂等成功。
pub const DEL_IF_EXISTS: u32 = 1 << 0;

/// `WalFetch` 的 `hint_off` 保留值：没有提示，服务端自己按 seq 定位。
///
/// 提示只在「那个偏移上确实坐着 `want_seq`」时才用，所以丢了不致命（§6.6）。
pub const WAL_HINT_NONE: u64 = u64::MAX;

/// 一次 `WalFetch` 能要的记录负载上限，与一次 `SnapFetch` 的块长上限。
///
/// 这两个数存在的理由是帧长（[`crate::engine::MAX_FRAME`] = 8 MiB）：回包是把
/// 记录/字节直接编进一个帧的，不设上限就是拿一个请求把服务端的内存吃穿。
/// 留出一半给帧头与数组计数，所以上限 4 MiB 而不是 8 MiB。
pub const WAL_FETCH_MAX: u32 = 4 << 20;
pub const SNAP_CHUNK_MAX: u32 = 4 << 20;

/// 一个 `WalBatch` 里的记录条数上限，作用与 [`MAX_TABLE_ITEMS`] 相同：只给
/// 解码侧的 `Vec::with_capacity` 一个界，不拿敌意计数直接预分配巨大数组。
///
/// 取值不是拍的：一帧之内每条记录至少占 4 字节长度位 + 13 字节负载（最小的
/// `Log` 是 `DelNode`：tag + view + ino），所以 [`WAL_FETCH_MAX`] 封顶的真实供
/// 应最多 4 MiB / 17 B ≈ 25 万条。取 `1 << 18` = 262_144 略高于它 —— 这一条
/// 不该挡掉任何合法应答，只挡编出来的计数。越界拒帧，不截断。
pub const MAX_FETCH_RECORDS: usize = 262_144;

/// `ReadDirPlus` 带回的属性：inode 里除 ino/kind/name 之外的那几列。
///
/// 为什么不直接复用 [`Node`]：ino/kind/name 在平行数组里已经各有一份，随每条
/// 再拷一遍就是白付三份字节（`data_ref` 还是大头）；`parent` 同理多余（这一页
/// 就是同一个 parent 的孩子），`link`（symlink 目标）要内容得走 `Readlink`。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeAttr {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub nlink: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
    pub data_ref: DataRef,
}

impl NodeAttr {
    pub fn of(n: &Node) -> Self {
        NodeAttr {
            mode: n.mode,
            uid: n.uid,
            gid: n.gid,
            size: n.size,
            nlink: n.nlink,
            atime: n.atime,
            mtime: n.mtime,
            ctime: n.ctime,
            data_ref: n.data_ref.clone(),
        }
    }
    pub fn encode(&self, w: &mut W) {
        w.u32(self.mode);
        w.u32(self.uid);
        w.u32(self.gid);
        w.u64(self.size);
        w.u32(self.nlink);
        w.i64(self.atime);
        w.i64(self.mtime);
        w.i64(self.ctime);
        w.bytes(&self.data_ref);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(NodeAttr {
            mode: r.u32()?,
            uid: r.u32()?,
            gid: r.u32()?,
            size: r.u64()?,
            nlink: r.u32()?,
            atime: r.i64()?,
            mtime: r.i64()?,
            ctime: r.i64()?,
            data_ref: r.bytes()?,
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

/// 定长字节段的解码口：长度不对就是坏帧，不当「短一点也行」。
///
/// 握手那两处（随机数与 MAC）必须定长：少字节意味着摘要输入被截，多出来的
/// 字节会被当成下一个字段 —— 两种都是「静默降级」的形状，所以在入口一次拦住。
fn fixed_bytes<const N: usize>(r: &mut Rd) -> Result<[u8; N]> {
    let b = r.bytes_bounded(N as u32)?;
    if b.len() != N {
        return Err(Error::Codec(format!(
            "fixed field: want {N} bytes, got {}",
            b.len()
        )));
    }
    let mut a = [0u8; N];
    a.copy_from_slice(&b);
    Ok(a)
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
    /// 给已存在的 inode 再加一个目录项（POSIX `link(2)`）。
    ///
    /// `ino` 必须是本视图内的**非目录**节点（目录回 `EPERM`，与 POSIX 一致）；
    /// 名字已存在回 `EEXIST`，不做 upsert —— `link(2)` 没有「替换」语义。
    /// 成功后两个名字指向同一个 inode、`nlink` +1，内容对象（`data_ref`）不复制：
    /// 这是与治理面「显示别名」（只改解析路径）和零拷贝导入（数据面共享字节）
    /// 都不同的第三件事（§5.2）。
    ///
    /// 不带 `uid`/`gid`：`link(2)` 不改属主也不改任何 inode 属性，唯一变化是
    /// 多一条 dentry 与 `nlink` +1。给一个用不上的身份字段只会让调用方以为
    /// 它在做权限判断。
    Link {
        view: ViewId,
        ino: Ino,
        parent: Ino,
        name: Vec<u8>,
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

    // 0x0040–0x0062 这一段曾是四组专用控制面 op（`Key*`/`Audit*`/`Def*`/
    // `Place*`），2026-11-23 按 §11 第 10 条（M4）整体删除。tag 不回收：
    // 旧客户端发过来会落到 `decode` 的 `unknown op tag`，所以线协议主版本
    // 抬到了 7（§4.1 的判据：删除既有帧不是「纯追加」）。

    // ---- 通用表：列名与语义由客户端定义，引擎不解释值 ----
    /// 建表。重名 → `EEXIST`；无列/超列数上限/列名空或重复 → `EINVAL`。
    TableCreate {
        def: TableDef,
    },
    /// 删表，级联丢掉它的全部行与二级索引。表不存在 → `ENOENT`。
    TableDrop {
        name: Vec<u8>,
    },
    /// 读一张表的形状（没有则 `Reply::Table(None)`，不是错误）。
    TableGet {
        name: Vec<u8>,
    },
    TableList,
    /// 给已有表**追加一列**（只能追加到末尾；不能改名、删列或改已有列的标志）。
    ///
    /// 三条语义：① 旧行按新宽度补齐，补出来的列值是**空字节** —— 空串本来就是
    /// 一个合法列值，所以引擎不承诺「能区分补出来的空值与显式写入的空值」；
    /// ② 整次变更只产一条 `Log::AddColumn`（一条 WAL 记录），所以崩溃切片上
    /// 「新形状 + 全部旧行都变宽」与「整次没发生」二选一；③ 快照格式与
    /// `SNAP_VERSION` 不动 —— 旧行变宽是行里多一个字节串，不是新的表结构。
    ///
    /// 拒绝四类：表不存在 `ENOENT`；列名空或超长 `EINVAL`/`ENAMETOOLONG`、
    /// 与已有列重名 `EINVAL`；已经 64 列 `EINVAL`；`COL_UNIQUE` 一律 `EINVAL` ——
    /// 引擎没有回填值，非空表上已有的空值会立刻把唯一约束踩破（要唯一列就在
    /// 建表时一次给全）。
    TableAddColumn {
        table: Vec<u8>,
        column: ColumnDef,
    },
    /// 写一行：键不存在则插入，存在则**整体覆盖**（不给列 = 该列被清成空字节）。
    /// 列数与 def 不符 → `EINVAL`；唯一列撞值 → `EEXIST`；条件不命中 → `EAGAIN`。
    RowPut {
        table: Vec<u8>,
        key: Vec<u8>,
        cols: Vec<Vec<u8>>,
        /// [`PUT_IF_ABSENT`] / [`PUT_IF_EXISTS`]。
        flags: u32,
        /// 与 `cond_val` 一起构成 CAS：要求现存行这一列恰等于给定字节。
        cond_col: u32,
        cond_val: Vec<u8>,
    },
    RowGet {
        table: Vec<u8>,
        key: Vec<u8>,
    },
    /// 按 `column` 等值列举；`column == `[`ROW_PK`] 时按主键序遍历全表、`value` 忽略。
    ///
    /// `from` 是「上一页最后一行的主键」（不含本身），空 = 从头。不用「已返回若
    /// 干条」那种序号游标：BTreeMap 的 `skip(n)` 是 O(n)，翻到第 k 页就把
    /// 前 k 页重走一遍；换成键游标后每页都从树上直接定位。
    /// 列没建索引 → `EINVAL`。
    RowList {
        table: Vec<u8>,
        column: u32,
        value: Vec<u8>,
        from: Vec<u8>,
        limit: u32,
    },
    /// 区间扫描：按 `column` 的字节序取 `[lo, hi)` 一段。`lo` / `hi` 空 = 该侧无界。
    ///
    /// 与 [`Op::RowList`] 只差「选哪一段」：那边是等值一点，这里是左闭右开的一段。
    /// `column == `[`ROW_PK`] 时区间落在**主键**上；否则必须是有索引的列，区间落在
    /// **列值**上，同一个值内仍按主键序。`lo > hi` 是空区间，不是错误。
    ///
    /// 游标 `after_*` 是「上一页最后一条」：`ROW_PK` 只看 `after_key`；索引列要连
    /// `after_val` 一起给 —— 同一个值可能跨页，只给主键回不去「这一页停在哪一段」。
    /// `after_key` 空 = 从区间头开始（主键不得为空，所以不会与「游标恰好是空键」撞车）。
    RowRange {
        table: Vec<u8>,
        column: u32,
        lo: Vec<u8>,
        hi: Vec<u8>,
        after_val: Vec<u8>,
        after_key: Vec<u8>,
        limit: u32,
    },
    /// 删一行。默认幂等（删不存在的行也成功），[`DEL_IF_EXISTS`] 则回 `ENOENT`。
    /// 条件不命中 → `EAGAIN`。
    RowDelete {
        table: Vec<u8>,
        key: Vec<u8>,
        flags: u32,
        cond_col: u32,
        cond_val: Vec<u8>,
    },
    /// 数行：按主键 = O(1)（表里存了多少行），按索引列 = O(命中数)。
    RowCount {
        table: Vec<u8>,
        column: u32,
        value: Vec<u8>,
    },

    // ---- 通用表：批量形状（§5.1「频繁替换/删除」那一档的专用口）----
    /// 清空一张表：名字、id 与形状（`def`）全留着，只把行与二级索引丢掉。
    ///
    /// 存在的理由是「整表替换」这个形状：逐行 `RowDelete` 要 N 条 op、N 条 WAL
    /// 记录，而这里**一条 op 一条记录**。与 [`Op::TableDrop`] 的区别是不换 id ——
    /// 换 id 意味着所有拿着 id 的人都要重新 `TableGet`，而清空不需要。
    ///
    /// 成本要说清：**一条记录 ≠ 常数时间**。apply 要逐行摘二级索引，O(行数)，
    /// 全在唯一提交线程上；十万行的清空会让那一轮停住相应的时长。想控制回合
    /// 时长就用 [`Op::RowRangeDelete`] 分批。
    ///
    /// 表不存在 → `ENOENT`。应答 [`Reply::Count`] = 被清掉的行数（本来就空 → 0）。
    TableClear {
        table: Vec<u8>,
    },
    /// 按区间删行：把 `column` 落在 `[lo, hi)` 内的行至多删掉 `max` 条。
    ///
    /// `column == `[`ROW_PK`] 时区间落在**主键**上，否则必须是有索引的列（判据与
    /// [`Op::RowRange`] 逐字一致：没建索引的列直接 `EINVAL`，不退化成全表扫）。
    /// `lo` / `hi` 空 = 该侧无界；`lo > hi` 是空区间而不是错误，删 0 行、**零日志**。
    ///
    /// `max = 0` = 不限，硬上限 [`MAX_TABLE_ITEMS`]（与列表/快照那道闸门同源）：
    /// 给它一个有限值是控制「一条 op 占住提交线程多久」的唯一手段。
    ///
    /// **一条 `Log::DelRange` = 一条 WAL 记录**，日志里记的是**区间而不是键清单**
    /// —— 写是全局单序的（§4.2），重放时从同一状态重新推出同一段，结果必然相同；
    /// 把键清单写进日志则会让一次删除的 WAL 体积正比于命中数，省下的往返又还回去。
    ///
    /// 应答 [`Reply::Count`] = 实际删掉的行数。成本 O(命中数)：翻译层为拿到这个数
    /// 扫一遍区间，apply 再扫一遍摘索引。
    RowRangeDelete {
        table: Vec<u8>,
        column: u32,
        lo: Vec<u8>,
        hi: Vec<u8>,
        max: u32,
    },
    /// 换表名（影子表切换）：把 `from` 那张表改叫 `to`。
    ///
    /// 默认**替换**：`to` 已存在时那张表连同它的全部行一起被丢 —— 这与文件
    /// `rename` 同形，也正是「新表顶替旧表」要的语义：先照着旧形状灌一张新表，
    /// 再一条 op 把它换到位。给 [`RENAME_NOREPLACE`] 则撞名回 `EEXIST`，什么都不动。
    ///
    /// **id 不变**：换的只是名字，索引跟着 `id` 走所以不用重建。`from` 不存在 →
    /// `ENOENT`；`from == to` 是空操作（零日志、回 `Reply::Ok`）。
    /// 一条 `Log::RenameTable` = 一条 WAL 记录，被替换掉的那张表由 apply 从状态里
    /// 现取（重放顺序一致，它的 id 不必进日志）。
    TableRename {
        from: Vec<u8>,
        to: Vec<u8>,
        /// 只用 [`RENAME_NOREPLACE`] 一位。
        flags: u32,
    },

    // ---- 跨 op 的原子批 ----
    /// 一批子 op：**要么全生效，要么一条都不生效**。
    ///
    /// 三个语义点：
    ///
    /// 1. **顺序可见**：子 op 按数组顺序执行，后一条能看到前一条的写。所以
    ///    「create 一个文件、再 `Setattr` 它」这种依赖前序结果的批合法 —— 这正是
    ///    它与「几条 op 拼成 pipeline 一起发」的区别：pipeline 只摊 fsync（§7.9），
    ///    既不提供跨 op 的可见性，也不提供原子性。
    /// 2. **崩溃原子**：整批的 `Log` 装进**一条** WAL 记录（`Log::Batch` 容器），
    ///    一条记录一个 CRC —— §6.2「坏尾整条丢弃」因此只可能整批丢，不可能只留
    ///    前几条。这比单 op 的多条 `Log` 更强：后者在同一个 fsync 里只是**运行期**
    ///    原子，崩溃切片上仍可能只落前几条。
    /// 3. **失败即整批不落**：任何一条回 [`Reply::Err`]，本 op 原样回那条的
    ///    `code`，`msg` 前缀写清是第几条；一条 `Log` 都不产，内存状态不动。
    ///
    /// 拒绝四类：空批、超过 [`MAX_BATCH_OPS`] 条、嵌套（子 op 不许再是 `Batch`，
    /// 解码与翻译两层各拦一道）、控制面 op（[`Op::is_control`] —— 它们要的不是
    /// `State`，而 `Checkpoint` 还会在批中间把 WAL 截掉，原子性就没了）。
    ///
    /// 成本：含写的批要在写锁里 `clone` 一份 `State` 当预演副本（`translate` 对
    /// 状态只读，预演只能落在副本上，失败时真状态一个字节没被动过）。批越宽、
    /// 状态越大，这一份拷贝越贵 —— 与「省几轮往返」是同一笔账的两端。
    Batch {
        ops: Vec<Op>,
    },

    // ---- readdir 带属性 ----
    /// 与 [`Op::ReadDir`] 同一份目录页，只是每条目录项把属性一起带回。
    ///
    /// 存在的唯一理由：不带属性的话，客户端要一页 N 条就得回头逐条 `Getattr`
    /// —— 往返次数从 1 变 N，目录列表在真实挂载上的成本全在那儿。
    ReadDirPlus {
        view: ViewId,
        parent: Ino,
        offset: u64,
        limit: u32,
    },

    // ---- 日志传送（§6.6）----
    /// 只读副本的拉记接口：把主上 `seq >= want_seq` 的 WAL 记录**原字节**交出来。
    ///
    /// 它不进只读工作池：要答它得碰 WAL 文件，而 `translate` 只拿得到 `&State`。
    /// 这与 `Stats` 同档 —— 属于控制面而不是可并行的数据面（见 [`Op::is_control`]）。
    WalFetch {
        want_seq: u64,
        /// 上一次交付返回的 `next_off`；没有就填 [`WAL_HINT_NONE`]。
        hint_off: u64,
        /// 本次要的记录负载上限（硬上限 [`WAL_FETCH_MAX`]，超了 `EINVAL`）。
        max_bytes: u32,
    },
    /// 分块读主的 `state.snap`。副本两处在用它：冷启动要追上主的 WAL 起点，
    /// 以及主 checkpoint 把自己缺的那段截掉之后的重同步。
    ///
    /// `len == 0` 是**探针**：不交字节，只答前缀 crc（[`Reply::SnapPrefix`]），
    /// 供换代时判断「已下的半份还能不能接着用」。这一档在旧主上只是空读。
    SnapFetch {
        offset: u64,
        /// 上限 [`SNAP_CHUNK_MAX`]，超了 `EINVAL`。`0` = 探针。
        len: u32,
    },

    // ---- 接入认证（§4.4）----
    /// 建连第一步：客户端报自己的线协议版本（wire tag `0x0092`）。
    ///
    /// 版本协商先于认证，也先于任何数据 op；服务端在收到这条之前一律 fail-closed。
    /// 它和认证握手一样由事件循环就地处置，不进引擎、不占提交线程。
    VersionBegin {
        client_proto: u16,
    },
    /// 握手第一步：客户端报自己的随机数（[`NONCE_LEN`] 字节）。
    ///
    /// 它**不进引擎**：不碰状态机、不占提交线程，在事件循环里就地处置
    /// （[`crate::net::auth`]）。服务端没启用认证时直接回 [`Reply::AuthOk`]
    /// 且 `authed = false` —— 所以一个「客户端总想握手」的形状对两种服务端
    /// 都成立，区别只在一次还是两次往返。
    AuthBegin {
        client_nonce: [u8; NONCE_LEN],
    },
    /// 握手第二步：客户端交回 `HMAC-SHA256`。只在上一拍收到
    /// [`Reply::Challenge`] 之后才有意义；MAC 算错了不会把连接打死，只是
    /// 回到「还没握手」那一步，可以重头再来（换个密钥重来就行）。
    AuthFinish {
        mac: [u8; DIGEST_LEN],
    },
    /// 握手的**可选**后续阶段：把这条连接之后的帧流整体加密（wire tag `0x0093`）。
    ///
    /// `want` 是客户端能接受的记录保护套件（见
    /// [`SECURE_SUITE_NONE`](crate::net::record::SECURE_SUITE_NONE) 与
    /// [`SECURE_SUITE_CHACHAPOLY1305`](crate::net::record::SECURE_SUITE_CHACHAPOLY1305)）。
    /// 服务端要么回 [`Reply::SecureOk`] 里同一个套件（接受），要么回
    /// `suite = 0`（拒绝，两侧继续走明文），要么完全不认识（旧服务端在
    /// `decode` 的兜底那里回 `EINVAL`）—— 三种都是合法结局，所以这一拍是可选的。
    ///
    /// 它只在认证已完成（`Stage::Ready`）之后才有意义：会话密钥从握手的两个
    /// 随机数派生，未握手的连接根本没有可派生的东西。它同样不进引擎。
    SecureBegin {
        want: u8,
    },
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
            // 0x0040–0x0062 留空：四组专用控制面 op 已删除（不回收 tag）。
            Op::TableCreate { .. } => 0x0070,
            Op::TableDrop { .. } => 0x0071,
            Op::TableGet { .. } => 0x0072,
            Op::TableList => 0x0073,
            Op::TableAddColumn { .. } => 0x007b,
            Op::RowPut { .. } => 0x0074,
            Op::RowGet { .. } => 0x0075,
            Op::RowList { .. } => 0x0076,
            Op::RowDelete { .. } => 0x0077,
            Op::RowCount { .. } => 0x0078,
            Op::RowRange { .. } => 0x0079,
            // 通用表那一段的最后一个空位 0x007a 给原子批（全局动作，不分段）。
            Op::Batch { .. } => 0x007a,
            // 三条批量 op 接在同一段的尾部（0x007c–0x007e，0x007b 已是追加列）。
            // 纯追加：旧客户端不发它们，旧服务端在 `decode` 的兜底那里回 `EINVAL`，
            // 两侧都没有静默分支；既有帧一个字节没动、`EngineStats` 也没加长，
            // 所以 `PROTOCOL_VERSION` 留在 7（§4.1 判据，第六例）。
            Op::TableClear { .. } => 0x007c,
            Op::RowRangeDelete { .. } => 0x007d,
            Op::TableRename { .. } => 0x007e,
            Op::ReadDirPlus { .. } => 0x002b,
            // 目录树那一段 0x0020–0x002b 已排满，`Link` 取紧接着的下一个空位。
            Op::Link { .. } => 0x002c,
            Op::WalFetch { .. } => 0x0080,
            Op::SnapFetch { .. } => 0x0081,
            Op::AuthBegin { .. } => 0x0090,
            Op::AuthFinish { .. } => 0x0091,
            Op::VersionBegin { .. } => 0x0092,
            // 0x0093 是握手阶段的下一个空位：只追加，不动既有帧的形状。
            // 但 `PROTOCOL_VERSION` 仍必须 +1 —— §4.1 的判据里「连接建立/握手的
            // 语义变了」这一条被踩中了：服务端现在会在 `AuthOk` 之后再切一次
            // 加密面，旧客户端不知道这一刀，两边会在同一段字节上得到不同的解释。
            Op::SecureBegin { .. } => 0x0093,
        }
    }

    /// 该 op 是否属于**控制面**：不改客户端可见状态，但答它需要的不是 `State`
    /// 而是 `Engine`（计数、WAL 文件、快照文件），因此既不能进只读工作池
    /// （[`is_readonly`](Op::is_readonly) 为 `false`），也不该在只读副本上被拒。
    ///
    /// 副本拒写的判据就是它：`!is_readonly() && !is_control()` 才是真的写。
    /// 分成三档而不是两档是因为两档下这个判据会连 `Stats` 一起拒掉 ——
    /// 而 `Stats` 恰恰是运维看副本落后多少的那个口，拒了它副本就没法监控。
    pub fn is_control(&self) -> bool {
        matches!(
            self,
            Op::Stats
                | Op::Checkpoint
                | Op::Shutdown
                | Op::WalFetch { .. }
                | Op::SnapFetch { .. }
                | Op::VersionBegin { .. }
                | Op::AuthBegin { .. }
                | Op::AuthFinish { .. }
                | Op::SecureBegin { .. }
        )
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
    /// 可并行的数据面读；`Checkpoint`/`Shutdown` 同理。`WalFetch`/`SnapFetch` 判
    /// `false` 是第三个理由：**它们需要的不是状态而是磁盘上的日志与快照**，
    /// `translate(&State, ..)` 根本拿不到。三档的划分见 [`Op::is_control`]。
    pub fn is_readonly(&self) -> bool {
        match self {
            // 批是**全只读时才只读**：一条写就把整批判进提交线程。这条递归
            // 让「纯读批」照旧吃 worker 并行与副本放行，而含写的批一定落回
            // 单线程路径 —— 与单 op 的判据同一个句号（[`Op::is_control`] 的
            // 三档划分不变，`Batch` 恒不是控制面）。
            Op::Batch { ops } => ops.iter().all(|o| o.is_readonly()),
            Op::Ping
            | Op::ListViews
            | Op::Lookup { .. }
            | Op::Getattr { .. }
            | Op::Readlink { .. }
            | Op::ReadDir { .. }
            | Op::ReadDirPlus { .. }
            | Op::RefLookup { .. }
            | Op::LockQuery { .. }
            | Op::TableGet { .. }
            | Op::TableList
            | Op::RowGet { .. }
            | Op::RowList { .. }
            | Op::RowRange { .. }
            | Op::RowCount { .. } => true,
            Op::Stats
            | Op::Checkpoint
            | Op::Shutdown
            | Op::CreateView { .. }
            | Op::DropView { .. }
            | Op::Setattr { .. }
            | Op::Create { .. }
            | Op::Mkdir { .. }
            | Op::Symlink { .. }
            | Op::Link { .. }
            | Op::Unlink { .. }
            | Op::Rmdir { .. }
            | Op::Rename { .. }
            | Op::RefUp { .. }
            | Op::RefDown { .. }
            | Op::LockSet { .. }
            | Op::TableCreate { .. }
            | Op::TableDrop { .. }
            | Op::TableAddColumn { .. }
            | Op::RowPut { .. }
            | Op::RowDelete { .. }
            | Op::TableClear { .. }
            | Op::RowRangeDelete { .. }
            | Op::TableRename { .. }
            | Op::WalFetch { .. }
            | Op::SnapFetch { .. }
            | Op::VersionBegin { .. }
            | Op::AuthBegin { .. }
            | Op::AuthFinish { .. }
            | Op::SecureBegin { .. } => false,
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
            | Op::TableList => {}
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
            Op::Link {
                view,
                ino,
                parent,
                name,
            } => {
                w.u32(*view);
                w.u64(*ino);
                w.u64(*parent);
                w.bytes(name);
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
            }
            | Op::ReadDirPlus {
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
            Op::TableCreate { def } => def.encode(w),
            Op::TableDrop { name } | Op::TableGet { name } => w.bytes(name),
            Op::TableAddColumn { table, column } => {
                w.bytes(table);
                column.encode(w);
            }
            Op::RowPut {
                table,
                key,
                cols,
                flags,
                cond_col,
                cond_val,
            } => {
                w.bytes(table);
                w.bytes(key);
                w.u32(cols.len() as u32);
                for c in cols {
                    w.bytes(c);
                }
                w.u32(*flags);
                w.u32(*cond_col);
                w.bytes(cond_val);
            }
            Op::RowGet { table, key } => {
                w.bytes(table);
                w.bytes(key);
            }
            Op::RowList {
                table,
                column,
                value,
                from,
                limit,
            } => {
                w.bytes(table);
                w.u32(*column);
                w.bytes(value);
                w.bytes(from);
                w.u32(*limit);
            }
            Op::RowRange {
                table,
                column,
                lo,
                hi,
                after_val,
                after_key,
                limit,
            } => {
                w.bytes(table);
                w.u32(*column);
                w.bytes(lo);
                w.bytes(hi);
                w.bytes(after_val);
                w.bytes(after_key);
                w.u32(*limit);
            }
            Op::RowDelete {
                table,
                key,
                flags,
                cond_col,
                cond_val,
            } => {
                w.bytes(table);
                w.bytes(key);
                w.u32(*flags);
                w.u32(*cond_col);
                w.bytes(cond_val);
            }
            Op::RowCount { table, column, value } => {
                w.bytes(table);
                w.u32(*column);
                w.bytes(value);
            }
            Op::TableClear { table } => w.bytes(table),
            Op::RowRangeDelete {
                table,
                column,
                lo,
                hi,
                max,
            } => {
                w.bytes(table);
                w.u32(*column);
                w.bytes(lo);
                w.bytes(hi);
                w.u32(*max);
            }
            Op::TableRename { from, to, flags } => {
                w.bytes(from);
                w.bytes(to);
                w.u32(*flags);
            }
            Op::Batch { ops } => {
                w.u32(ops.len() as u32);
                for o in ops {
                    o.encode(w);
                }
            }
            Op::WalFetch { want_seq, hint_off, max_bytes } => {
                w.u64(*want_seq);
                w.u64(*hint_off);
                w.u32(*max_bytes);
            }
            Op::SnapFetch { offset, len } => {
                w.u64(*offset);
                w.u32(*len);
            }
            Op::VersionBegin { client_proto } => w.u16(*client_proto),
            Op::AuthBegin { client_nonce } => w.bytes(client_nonce),
            Op::AuthFinish { mac } => w.bytes(mac),
            Op::SecureBegin { want } => w.u8(*want),
        }
    }

    pub fn decode(r: &mut Rd) -> Result<Op> {
        Self::decode_depth(r, 0)
    }

    /// `depth` = 已经套了几层 [`Op::Batch`]。子 op 不许再是 `Batch`，所以只有
    /// 0 与 1 两个取值。这道结构性拒绝放在解码里、而不是只放 `translate`：解码
    /// 递归没有上界的话，一条精心构造的深嵌套帧能让服务端在解帧阶段爆栈 ——
    /// 那是崩溃，连 `EINVAL` 都回不出去。
    fn decode_depth(r: &mut Rd, depth: u8) -> Result<Op> {
        let tag = r.u16()?;
        if depth > 0 && tag == 0x007a {
            return Err(Error::Codec("nested batch op is not allowed".into()));
        }
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
            0x002b => Op::ReadDirPlus {
                view: r.u32()?,
                parent: r.u64()?,
                offset: r.u64()?,
                limit: r.u32()?,
            },
            0x002c => Op::Link {
                view: r.u32()?,
                ino: r.u64()?,
                parent: r.u64()?,
                name: r.bytes()?,
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
            0x0070 => Op::TableCreate {
                def: TableDef::decode(r)?,
            },
            0x0071 => Op::TableDrop { name: r.bytes()? },
            0x0072 => Op::TableGet { name: r.bytes()? },
            0x0073 => Op::TableList,
            0x007b => Op::TableAddColumn {
                table: r.bytes()?,
                column: ColumnDef::decode(r)?,
            },
            0x0074 => {
                let table = r.bytes()?;
                let key = r.bytes()?;
                let n = r.u32()? as usize;
                if n > MAX_COLUMNS {
                    return Err(Error::Codec(format!(
                        "row has {n} columns, over {MAX_COLUMNS}"
                    )));
                }
                let mut cols = Vec::with_capacity(n);
                for _ in 0..n {
                    cols.push(r.bytes()?);
                }
                Op::RowPut {
                    table,
                    key,
                    cols,
                    flags: r.u32()?,
                    cond_col: r.u32()?,
                    cond_val: r.bytes()?,
                }
            }
            0x0075 => Op::RowGet {
                table: r.bytes()?,
                key: r.bytes()?,
            },
            0x0076 => Op::RowList {
                table: r.bytes()?,
                column: r.u32()?,
                value: r.bytes()?,
                from: r.bytes()?,
                limit: r.u32()?,
            },
            0x0077 => Op::RowDelete {
                table: r.bytes()?,
                key: r.bytes()?,
                flags: r.u32()?,
                cond_col: r.u32()?,
                cond_val: r.bytes()?,
            },
            0x0078 => Op::RowCount {
                table: r.bytes()?,
                column: r.u32()?,
                value: r.bytes()?,
            },
            0x0079 => Op::RowRange {
                table: r.bytes()?,
                column: r.u32()?,
                lo: r.bytes()?,
                hi: r.bytes()?,
                after_val: r.bytes()?,
                after_key: r.bytes()?,
                limit: r.u32()?,
            },
            0x007c => Op::TableClear {
                table: r.bytes()?,
            },
            0x007d => Op::RowRangeDelete {
                table: r.bytes()?,
                column: r.u32()?,
                lo: r.bytes()?,
                hi: r.bytes()?,
                max: r.u32()?,
            },
            0x007e => Op::TableRename {
                from: r.bytes()?,
                to: r.bytes()?,
                flags: r.u32()?,
            },
            // 条数上界在**解码**这里报错（Codec 错 = 断连），而不是等 `translate`
            // 回 EINVAL：`Vec::with_capacity(n)` 拿吃一个敌意计数就是内存放大，
            // 而正常客户端不会送超过 `MAX_BATCH_OPS` 的批（那一档由翻译层回
            // EINVAL，两条路对合法请求不交叉）。
            0x007a => {
                let n = r.u32()? as usize;
                if n > MAX_BATCH_OPS {
                    return Err(Error::Codec(format!(
                        "batch carries {n} ops, over MAX_BATCH_OPS={MAX_BATCH_OPS}"
                    )));
                }
                let mut ops = Vec::with_capacity(n);
                for _ in 0..n {
                    ops.push(Self::decode_depth(r, depth + 1)?);
                }
                Op::Batch { ops }
            }
            // 两个长度上限（`WAL_FETCH_MAX` / `SNAP_CHUNK_MAX`）**不在这里拦**：
            // 越界的解码会连带断连（解码失败在事件循环里就是 `drop_conn`），
            // 而「你要得太多」是个该回 EINVAL 的参数问题，不是协议坏了 ——
            // 拦住它的是 `Engine::exec` 的 `WalFetch`/`SnapFetch` 分支。
            0x0080 => Op::WalFetch {
                want_seq: r.u64()?,
                hint_off: r.u64()?,
                max_bytes: r.u32()?,
            },
            0x0081 => Op::SnapFetch {
                offset: r.u64()?,
                len: r.u32()?,
            },
            0x0090 => Op::AuthBegin {
                client_nonce: fixed_bytes(r)?,
            },
            0x0091 => Op::AuthFinish {
                mac: fixed_bytes(r)?,
            },
            0x0092 => Op::VersionBegin {
                client_proto: r.u16()?,
            },
            0x0093 => Op::SecureBegin { want: r.u8()? },
            x => return Err(Error::Codec(format!("unknown op tag 0x{x:04x}"))),
        };
        Ok(op)
    }
}

/// readdir 一页的名字缓冲：一块扁平字节 + 每项起始偏移。
///
/// 为什么不是 `Vec<Vec<u8>>`。micro 实测（`--items 32000`，钉单核，原始输出
/// `benches/results/micro_names_before.log`）把 readdir 的每条目录项定价定得很死：
/// limit 从 1 扫到 400，耗时线性
/// 220.6 → 482.7 → 2 293.5 → 4 280.5 → 17 038.1 ns，分配数恰好 5 → 14 → 54 → 104 → 404，
/// 即**每条 42 ns、每条 1 次 malloc**，占一页 100 条成本的 97%。名字本身只有
/// 十几字节，那 42 ns 几乎全是 glibc 的 malloc/free 对，不是拷贝。
///
/// 改成这个形状后，一页只付 2 次摊销分配（`blob` 与 `off` 各自翻倍增长）。
/// **线协议一个字节都没变** —— 编码仍按 `u32 len + 字节` 逐条写，见 encode。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Names {
    blob: Vec<u8>,
    /// 每项的起点，`off[len]` 是末尾哨兵，所以第 i 项 = `blob[off[i]..off[i+1]]`。
    off: Vec<u32>,
}

/// 必须手写：derive 出来的全零值 `off` 是空的，而 `len()` 算的是 `off.len() - 1`
/// （哨兵占一项），下溢后第一次 `get` 就越界。空表只能是 [`Names::new`] 那个形状。
impl Default for Names {
    fn default() -> Self {
        Self::new()
    }
}

impl Names {
    /// 空表。唯一入口是 [`with_capacity`](Names::with_capacity)，免得两处各自记
    /// 得补哨兵 —— 上一版就是 `with_capacity` 漏了那个 0，于是 `len()` 下溢、
    /// 编码侧一项都写不出来。
    pub fn new() -> Self {
        Names::with_capacity(0, 0)
    }

    /// 已知会装多少条时用这个：`entries` 条数、`bytes` 名字总长的估计。
    pub fn with_capacity(entries: usize, bytes: usize) -> Self {
        // 先把哨兵推进预分配好的 `off` 里，再返回 —— 两者都要，不能为了哨兵
        // 放弃容量（那一页 100 条就会退化成 7 次翻倍重分配）。
        let mut off = Vec::with_capacity(entries + 1);
        off.push(0);
        Names {
            blob: Vec::with_capacity(bytes),
            off,
        }
    }

    /// 从已有名字批量构造（测试与“手上已经是 `Vec<Vec<u8>>`”的调用方）。
    /// 热路径不要用，那等于把拷贝代价再拾回来。
    pub fn from_vec<V: AsRef<[u8]>>(v: &[V]) -> Self {
        let total = v.iter().map(|n| n.as_ref().len()).sum();
        let mut n = Self::with_capacity(v.len(), total);
        for name in v {
            n.push(name.as_ref());
        }
        n
    }

    pub fn push(&mut self, name: &[u8]) {
        self.blob.extend_from_slice(name);
        self.off.push(self.blob.len() as u32);
    }

    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        let (a, b) = (self.off[i] as usize, self.off[i + 1] as usize);
        &self.blob[a..b]
    }

    /// 条目数（不含末尾哨兵）。
    #[inline]
    pub fn len(&self) -> usize {
        self.off.len() - 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.off.len() == 1
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(move |i| self.get(i))
    }

    /// 交给需要拥有每条名字的调用方（FUSE 侧填 dirent 缓冲、单元测试断言）。
    pub fn to_vec(&self) -> Vec<Vec<u8>> {
        self.iter().map(|n| n.to_vec()).collect()
    }
}

/// 下标访问：`names[i] -> &[u8]`。扁平表示不该让调用方多背一个 `get` 的心智负担。
impl std::ops::Index<usize> for Names {
    type Output = [u8];
    #[inline]
    fn index(&self, i: usize) -> &[u8] {
        self.get(i)
    }
}

/// 借用迭代：`for name in &names` 逐条拿 `&[u8]`。不提供按值 `IntoIterator` ——
/// 那要么得把 blob 拆成一百个拥有块（本次优化想消掉的就是这个），要么得多写一个
/// 生命周期相关的 Item。把字段从 `Vec<Vec<u8>>` 换成 `Names` 后，下游原先按值
/// `zip(names)` 的写法要改成 `zip(&names)`；接线影响面见 DESIGN.md §10。
impl<'a> IntoIterator for &'a Names {
    type Item = &'a [u8];
    type IntoIter = Box<dyn Iterator<Item = &'a [u8]> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
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
        names: Names,
        next: u64,
        end: bool,
    },
    /// [`Op::ReadDirPlus`] 的应答：与 [`Reply::Dirents`] 同序的平行数组，多一个
    /// 与 `inos` 等长的属性数组。两个数组分开写是因为名字已经趟过一遍扁平缓冲，
    /// 把属性也摊进同一个循环只会多一次结构体拷贝。
    DirentsPlus {
        inos: Vec<Ino>,
        kinds: Vec<u8>,
        names: Names,
        attrs: Vec<NodeAttr>,
        next: u64,
        end: bool,
    },
    Ref {
        count: u64,
        bytes: u64,
    },
    Locks(Vec<(Vec<u8>, u8, i64)>),
    /// 一张通用表的形状：`None` = 没有这张表（不是错误，与 `TableGet` 同形）。
    Table(Option<TableDef>),
    Tables(Vec<TableDef>),
    /// 一行：`None` = 没有这行。`RowPut` 成功时回存进去的那一行（把成品
    /// 直接给回去，免得调用方再读一次）。
    Row(Option<RowRec>),
    /// `RowList` 的一页。`end = false` 意味着还有：下一页把本页最后一行的
    /// 键当 `from` 游标。
    Rows {
        rows: Vec<RowRec>,
        end: bool,
    },
    /// `RowCount`。
    Count(u64),
    /// 一批子应答：与 [`Op::Batch`] 的 `ops` **逐位对应、长度相等**。
    ///
    /// 一条子 op 失败时不会走到这里 —— 整批回一个 [`Reply::Err`]（`msg`
    /// 里带下标），所以拿到 `Batch` 就意味着每一条都成功了。
    ///
    /// 嵌套不存在：子应答不许再是 `Batch`（解码拦住，判据同 `Op` 那侧）。
    Batch { replies: Vec<Reply> },
    Stats(Box<crate::engine::core::EngineStats>),
    /// [`Op::WalFetch`] 的应答：`records` 是 WAL 记录的 payload **原字节**，按
    /// seq 升序且连续，`records[i]` 的 seq 就是 `base_seq + i`。
    ///
    /// `durable_seq` 是主上已落盘的最大 seq，副本的可读水位永远不能越过它 ——
    /// 越过就是让副本读到主还没确认的写，主一旦崩溃回滚，副本就撒了谎。
    /// `snap_seq` 是主当前快照的水位，`gap = want_seq <= snap_seq`：要的那条已经
    /// 被 checkpoint 截进快照，日志里没有，副本得整份重装快照（§6.6）。
    WalBatch {
        records: Vec<Vec<u8>>,
        base_seq: u64,
        /// 下次请求原样回送当 `hint_off`。
        next_off: u64,
        durable_seq: u64,
        snap_seq: u64,
        gap: bool,
        /// 主的 fencing token（§6.6 第 3 条）。副本用它挡被提掉权的旧主；
        /// 语义与 [`Reply::SnapChunk::epoch`] 同一个数（主的 `fence`）。
        epoch: u64,
    },
    /// [`Op::SnapFetch`] 的应答：`state.snap` 从 `offset` 起的一段。
    ///
    /// `total` 是整份快照的字节数（副本据此知道下完没有），`snap_seq` 是这份快照
    /// 的水位，起代际标签的作用：分块下载中途它变了，说明主的快照被 checkpoint
    /// 换过一代，手里这几块拼起来也不是同一份，只能整份重下。
    ///
    /// `epoch` 是主的 **fencing token**（§6.6 第 3 条）：单调递增的「谁有权写」
    /// 令牌，每次 `treedb-standby --promote` 加一。副本拿它与自己目录里的令牌比：
    /// 更小的主一律拒收（那是被提掉权的旧主），更大的主接受并把自己的令牌抬上去。
    /// 放在快照这条路上而不是只放在 `WalBatch` 上，因为「只有快照可拉」恰恰是副本
    /// 落后最多、最容易被旧主骗回去的时刻。
    SnapChunk {
        bytes: Vec<u8>,
        total: u64,
        snap_seq: u64,
        epoch: u64,
    },
    /// [`Op::SnapFetch`] 的**探针**应答：`len == 0` 时主不交字节，只答
    /// `state.snap` 前 `min(offset, total)` 字节的 crc32。
    ///
    /// 它只为一件事存在：换代之后判断「手里已经下的那半份还能不能接着用」。
    /// 两代快照出自同一个编码器，状态只挪了一点点时前缀往往逐字节相同 —— 但
    /// 协议**不承诺**公共前缀，所以判据只能是内容：副本拿自己已下前缀的 crc32
    /// 与它比，相同就截到 `min(offset,total)` 接着下，不同就从 0 重来。
    ///
    /// 为什么另起一个 tag 而不是给 [`Reply::SnapChunk`] 加字段：后者会让**既有**
    /// 帧变长，按 §4.1 的判据必须把 `PROTOCOL_VERSION` 抬到 7；而旧副本从不发
    /// `len == 0`（`--fetch-bytes 0` 启动时就拒），旧主收到 `len == 0` 回一个空
    /// `SnapChunk`，新副本认出这个答复就退回整份重下 —— 两个方向都安全，于是
    /// 版本留在 6。
    SnapPrefix {
        /// `state.snap` 前 `min(offset, total)` 字节的 crc32（IEEE，与快照尾巴
        /// 那个校验和同一个函数）。
        prefix_crc: u32,
        total: u64,
        snap_seq: u64,
        epoch: u64,
    },
    /// [`Op::VersionBegin`] 的应答：服务端确认双方线协议同代。
    ///
    /// 所有正常连接都必须先拿到这一条；认证路径随后还会在 [`Reply::Challenge`]
    /// 里再报一次版本，作为认证域内的交叉核对。
    VersionOk {
        server_proto: u16,
    },
    /// [`Op::AuthBegin`] 的应答：服务端挑战值。收到它就是「本实例启用了
    /// 认证，请交回 MAC」；`proto` 是服务端的线协议版本，与建连时的
    /// [`Reply::VersionOk`] 必须一致。
    Challenge {
        server_nonce: [u8; NONCE_LEN],
        proto: u16,
    },
    /// 握手完成。`authed = false` 意味着服务端**没有**启用认证（它只是
    /// 确认了「这里没事」）：带密钥的客户端收到它就是降级，必须自己报错，
    /// 不能当成成功（见 [`crate::net::auth`]）。
    ///
    /// `server_mac` 是服务端的反向证明，双向认证的其中一半：客户端据此能确认
    /// 对端真持有那份密钥。注意它证明的是「知道密钥」而不是「是当前那个主」
    /// —— 后者要 fencing token，还欠着（§11 第 4 条②）。
    AuthOk {
        server_mac: [u8; DIGEST_LEN],
        authed: bool,
    },
    /// [`Op::SecureBegin`] 的应答：实际达成的记录保护套件（wire tag `0x0193`）。
    ///
    /// `suite = 0` 是**明确的拒绝**而不是错误：服务端可以同意不加密，两侧继续
    /// 走明文，调用方要不要把降级当成失败是它自己的事（见
    /// [`crate::net::record`] 模块头第 3 条）。非零值必须等于请求的 `want`，
    /// 否则就是服务端在乱说话。
    ///
    /// 收到这一条之后（且 suite 非零），**两侧同时切到记录层**：从这里开始
    /// 线上不再有明文帧，包括这条应答自己的后续字节。
    SecureOk {
        suite: u8,
    },
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
                // 逐条写与旧的 `Vec<Vec<u8>>` 表示完全同序同长 —— 换内部表示不动线协议。
                for (i, name) in (0..n).zip(names.iter()) {
                    w.u64(inos[i]);
                    w.u8(kinds[i]);
                    w.bytes(name);
                }
                w.u64(*next);
                w.bool(*end);
            }
            Reply::DirentsPlus {
                inos,
                kinds,
                names,
                attrs,
                next,
                end,
            } => {
                w.u16(0x0123);
                let n = inos.len();
                w.u32(n as u32);
                for (i, name) in (0..n).zip(names.iter()) {
                    w.u64(inos[i]);
                    w.u8(kinds[i]);
                    w.bytes(name);
                    // 属性按同一下标紧挨名字写：读侧一个循环就能把四个平行数组
                    // 全填完，不用先趟完名字再回头找属性。
                    attrs[i].encode(w);
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
            Reply::Table(t) => {
                w.u16(0x0170);
                match t {
                    Some(x) => {
                        w.u8(1);
                        x.encode(w);
                    }
                    None => w.u8(0),
                }
            }
            Reply::Tables(v) => {
                w.u16(0x0171);
                w.u32(v.len() as u32);
                for t in v {
                    t.encode(w);
                }
            }
            Reply::Row(row) => {
                w.u16(0x0172);
                match row {
                    Some(x) => {
                        w.u8(1);
                        x.encode(w);
                    }
                    None => w.u8(0),
                }
            }
            Reply::Rows { rows, end } => {
                w.u16(0x0173);
                w.u32(rows.len() as u32);
                for row in rows {
                    row.encode(w);
                }
                w.bool(*end);
            }
            Reply::Count(n) => {
                w.u16(0x0174);
                w.u64(*n);
            }
            Reply::Batch { replies } => {
                w.u16(0x01a0);
                w.u32(replies.len() as u32);
                for r in replies {
                    r.encode(w);
                }
            }
            Reply::Stats(s) => {
                w.u16(0x0102);
                s.encode(w);
            }
            Reply::WalBatch {
                records,
                base_seq,
                next_off,
                durable_seq,
                snap_seq,
                gap,
                epoch,
            } => {
                w.u16(0x0180);
                w.u32(records.len() as u32);
                for rec in records {
                    w.bytes(rec);
                }
                w.u64(*base_seq);
                w.u64(*next_off);
                w.u64(*durable_seq);
                w.u64(*snap_seq);
                w.bool(*gap);
                w.u64(*epoch);
            }
            Reply::SnapChunk {
                bytes,
                total,
                snap_seq,
                epoch,
            } => {
                w.u16(0x0181);
                w.bytes(bytes);
                w.u64(*total);
                w.u64(*snap_seq);
                w.u64(*epoch);
            }
            Reply::SnapPrefix {
                prefix_crc,
                total,
                snap_seq,
                epoch,
            } => {
                w.u16(0x0182);
                w.u32(*prefix_crc);
                w.u64(*total);
                w.u64(*snap_seq);
                w.u64(*epoch);
            }
            Reply::VersionOk { server_proto } => {
                w.u16(0x0192);
                w.u16(*server_proto);
            }
            Reply::Challenge {
                server_nonce,
                proto,
            } => {
                w.u16(0x0190);
                w.bytes(server_nonce);
                w.u16(*proto);
            }
            Reply::AuthOk { server_mac, authed } => {
                w.u16(0x0191);
                w.bytes(server_mac);
                w.bool(*authed);
            }
            Reply::SecureOk { suite } => {
                w.u16(0x0193);
                w.u8(*suite);
            }
        }
    }

    pub fn decode(r: &mut Rd) -> Result<Reply> {
        Self::decode_depth(r, 0)
    }

    /// 与 [`Op::decode_depth`] 同一个理由：应答也是递归结构，给一个上界。
    fn decode_depth(r: &mut Rd, depth: u8) -> Result<Reply> {
        let tag = r.u16()?;
        if depth > 0 && tag == 0x01a0 {
            return Err(Error::Codec("nested batch reply is not allowed".into()));
        }
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
                let mut names = Names::new();
                for _ in 0..n {
                    inos.push(r.u64()?);
                    kinds.push(r.u8()?);
                    // `bytes_ref` 借自收到的帧，名字直接落进扁平缓冲 ——
                    // 解码侧也不再每条一次 `to_vec()`。
                    names.push(r.bytes_ref()?);
                }
                Reply::Dirents {
                    inos,
                    kinds,
                    names,
                    next: r.u64()?,
                    end: r.bool()?,
                }
            }
            0x0123 => {
                let n = r.u32()? as usize;
                let mut inos = Vec::with_capacity(n.min(200_000));
                let mut kinds = Vec::with_capacity(n.min(200_000));
                let mut names = Names::new();
                let mut attrs = Vec::with_capacity(n.min(200_000));
                for _ in 0..n {
                    inos.push(r.u64()?);
                    kinds.push(r.u8()?);
                    names.push(r.bytes_ref()?);
                    attrs.push(NodeAttr::decode(r)?);
                }
                Reply::DirentsPlus {
                    inos,
                    kinds,
                    names,
                    attrs,
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
            0x0170 => {
                if r.u8()? == 0 {
                    Reply::Table(None)
                } else {
                    Reply::Table(Some(TableDef::decode(r)?))
                }
            }
            0x0171 => {
                let n = r.u32()? as usize;
                if n > MAX_TABLE_ITEMS {
                    return Err(Error::Codec(format!(
                        "reply lists {n} tables, over {MAX_TABLE_ITEMS}"
                    )));
                }
                let mut v = Vec::with_capacity(n.min(4096));
                for _ in 0..n {
                    v.push(TableDef::decode(r)?);
                }
                Reply::Tables(v)
            }
            0x0172 => {
                if r.u8()? == 0 {
                    Reply::Row(None)
                } else {
                    Reply::Row(Some(RowRec::decode(r)?))
                }
            }
            0x0173 => {
                let n = r.u32()? as usize;
                if n > MAX_TABLE_ITEMS {
                    return Err(Error::Codec(format!(
                        "reply lists {n} rows, over {MAX_TABLE_ITEMS}"
                    )));
                }
                let mut rows = Vec::with_capacity(n.min(4096));
                for _ in 0..n {
                    rows.push(RowRec::decode(r)?);
                }
                Reply::Rows {
                    rows,
                    end: r.bool()?,
                }
            }
            0x0174 => Reply::Count(r.u64()?),
            0x01a0 => {
                let n = r.u32()? as usize;
                if n > MAX_BATCH_OPS {
                    return Err(Error::Codec(format!(
                        "batch reply carries {n} entries, over MAX_BATCH_OPS={MAX_BATCH_OPS}"
                    )));
                }
                let mut replies = Vec::with_capacity(n);
                for _ in 0..n {
                    replies.push(Self::decode_depth(r, depth + 1)?);
                }
                Reply::Batch { replies }
            }
            0x0180 => {
                let n = r.u32()? as usize;
                if n > MAX_FETCH_RECORDS {
                    return Err(Error::Codec(format!(
                        "reply carries {n} wal records, over {MAX_FETCH_RECORDS}"
                    )));
                }
                let mut records = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    // 单条按 WAL 那侧的 payload 上限拦：应答里的记录必须可能是
                    // 一条真记录，不然就是坏帧（帧长本身还另有 `MAX_FRAME` 兜底）。
                    records.push(r.bytes_bounded(crate::engine::wal::MAX_RECORD)?);
                }
                Reply::WalBatch {
                    records,
                    base_seq: r.u64()?,
                    next_off: r.u64()?,
                    durable_seq: r.u64()?,
                    snap_seq: r.u64()?,
                    gap: r.bool()?,
                    epoch: r.u64()?,
                }
            }
            0x0181 => Reply::SnapChunk {
                bytes: r.bytes_bounded(crate::engine::MAX_FRAME)?,
                total: r.u64()?,
                snap_seq: r.u64()?,
                epoch: r.u64()?,
            },
            0x0182 => Reply::SnapPrefix {
                prefix_crc: r.u32()?,
                total: r.u64()?,
                snap_seq: r.u64()?,
                epoch: r.u64()?,
            },
            0x0192 => Reply::VersionOk {
                server_proto: r.u16()?,
            },
            0x0190 => Reply::Challenge {
                server_nonce: fixed_bytes(r)?,
                proto: r.u16()?,
            },
            0x0191 => Reply::AuthOk {
                server_mac: fixed_bytes(r)?,
                authed: r.bool()?,
            },
            0x0193 => Reply::SecureOk { suite: r.u8()? },
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

    /// `Names` 的哨兵约定很容易被改错（`off` 比条目多一项），所以这里把
    /// 空表 / 零长名字 / 两种构造路径钉住。
    #[test]
    fn names_keeps_the_sentinel_shape() {
        let mut n = Names::default();
        assert!(n.is_empty());
        assert_eq!(n.len(), 0);

        n.push(b"alpha");
        n.push(b"");
        n.push(b"b");
        assert_eq!(n.len(), 3);
        assert!(!n.is_empty());
        // 零长名字（POSIX 不允许空名，但编码必须能表达）不能把偏移挤乱。
        assert_eq!(&n[0], b"alpha");
        assert_eq!(&n[1], b"");
        assert_eq!(&n[2], b"b");
        assert_eq!(
            n.iter().collect::<Vec<_>>(),
            vec![&b"alpha"[..], &b""[..], &b"b"[..]]
        );
        assert_eq!(n.to_vec(), vec![b"alpha".to_vec(), Vec::new(), b"b".to_vec()]);

        // `with_capacity` 走同一条 push 路径，结果必须与从零增长一致。
        let mut m = Names::with_capacity(3, 8);
        assert!(m.is_empty());
        for s in n.iter() {
            m.push(s);
        }
        assert_eq!(m, n);

        assert_eq!(Names::from_vec(&[b"a".to_vec(), b"bb".to_vec()]).len(), 2);
    }

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
            Op::Link {
                view: 3,
                ino: 7,
                parent: 1,
                name: b"s2".to_vec(),
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
            // ---- 通用表 ----
            Op::TableCreate {
                def: TableDef {
                    name: b"kv".to_vec(),
                    columns: vec![
                        ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
                        ColumnDef { name: b"b".to_vec(), flags: COL_INDEX },
                        ColumnDef { name: b"c".to_vec(), flags: 0 },
                    ],
                },
            },
            Op::TableDrop { name: b"kv".to_vec() },
            Op::TableGet { name: b"kv".to_vec() },
            Op::TableList,
            Op::TableAddColumn {
                table: b"kv".to_vec(),
                column: ColumnDef {
                    name: b"d".to_vec(),
                    flags: COL_INDEX,
                },
            },
            Op::RowPut {
                table: b"kv".to_vec(),
                key: b"k1".to_vec(),
                cols: vec![b"v1".to_vec(), Vec::new(), b"".to_vec()],
                flags: PUT_IF_ABSENT,
                cond_col: 0,
                cond_val: b"v0".to_vec(),
            },
            Op::RowGet { table: b"kv".to_vec(), key: b"k1".to_vec() },
            Op::RowList {
                table: b"kv".to_vec(),
                column: 0,
                value: b"v1".to_vec(),
                from: b"k0".to_vec(),
                limit: 100,
            },
            Op::RowRange {
                table: b"kv".to_vec(),
                column: 0,
                lo: b"v0".to_vec(),
                hi: b"v9".to_vec(),
                after_val: b"v1".to_vec(),
                after_key: b"k0".to_vec(),
                limit: 100,
            },
            Op::RowDelete {
                table: b"kv".to_vec(),
                key: b"k1".to_vec(),
                flags: DEL_IF_EXISTS,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
            Op::RowCount {
                table: b"kv".to_vec(),
                column: ROW_PK,
                value: Vec::new(),
            },
            Op::TableClear {
                table: b"kv".to_vec(),
            },
            Op::RowRangeDelete {
                table: b"kv".to_vec(),
                column: 0,
                lo: b"v0".to_vec(),
                hi: b"v9".to_vec(),
                max: 1000,
            },
            Op::TableRename {
                from: b"kv".to_vec(),
                to: b"kv2".to_vec(),
                flags: RENAME_NOREPLACE,
            },
            // ---- 原子批 ----
            // 子 op 故意混一条只读进去：编码/解码只关心形状，不为「只读还是写」分叉。
            Op::Batch {
                ops: vec![
                    Op::Mkdir {
                        view: 3,
                        parent: 1,
                        name: b"d".to_vec(),
                        mode: 0o755,
                        uid: 7,
                        gid: 8,
                    },
                    Op::Getattr { view: 3, ino: 9 },
                ],
            },
            Op::Batch { ops: Vec::new() },
            // ---- readdir 带属性 ----
            Op::ReadDirPlus {
                view: 3,
                parent: 1,
                offset: 10,
                limit: 100,
            },
            // ---- 日志传送 ----
            Op::WalFetch {
                want_seq: 7,
                hint_off: 4096,
                max_bytes: WAL_FETCH_MAX,
            },
            // 首轮拉取：没提示、不要记录，只为拿主的 durable_seq / snap_seq。
            Op::WalFetch {
                want_seq: 1,
                hint_off: WAL_HINT_NONE,
                max_bytes: 0,
            },
            Op::SnapFetch {
                offset: 0,
                len: SNAP_CHUNK_MAX,
            },
            Op::SnapFetch {
                offset: 1 << 40,
                len: 0,
            },
            Op::VersionBegin {
                client_proto: crate::engine::PROTOCOL_VERSION,
            },
            Op::AuthBegin {
                client_nonce: [0xa5; NONCE_LEN],
            },
            Op::AuthFinish {
                mac: [0x5a; DIGEST_LEN],
            },
            Op::SecureBegin {
                want: crate::net::record::SECURE_SUITE_CHACHAPOLY1305,
            },
            // 套件的「拒绝」那一支：0 = 不加密，形状上也是一条合法帧。
            Op::SecureBegin { want: 0 },
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

    /// 四组专用控制面 op 的 tag 已被整段删除（§11 第 10 条 M4）。旧客户端发过来
    /// 必须落进 `unknown op tag`，而不是碰巧解成另一条 op —— tag 不回收，这条测试
    /// 就是那段区间不再被任何人占用的可执行声明。
    #[test]
    fn removed_control_plane_tags_are_unknown() {
        for tag in [0x0040u16, 0x0041, 0x004d, 0x0062] {
            let mut w = W::new();
            w.u16(tag);
            let err = Op::decode(&mut Rd::new(&w.finish())).unwrap_err().to_string();
            assert!(err.contains("unknown op tag"), "0x{tag:04x}: {err}");
        }
        for tag in [0x0140u16, 0x0150, 0x0161] {
            let mut w = W::new();
            w.u16(tag);
            let err = Reply::decode(&mut Rd::new(&w.finish())).unwrap_err().to_string();
            assert!(err.contains("unknown reply tag"), "0x{tag:04x}: {err}");
        }
    }

    /// 版本与认证握手的定长字段在解码侧就是硬门槛：短一个字节是 `Err`，不是「少读一点」。
    /// 短的版本号会失去代际判据，短的挑战值 = 随机空间缩水，短的 MAC = 摘要被截，
    /// 所以宁可断连。（长的在 `bytes_bounded` 那一步就被拦掉。）
    #[test]
    fn handshake_fixed_fields_reject_wrong_length() {
        let mut w = W::new();
        w.u16(0x0090);
        w.bytes(&[9u8; NONCE_LEN - 1]);
        assert!(Op::decode(&mut Rd::new(&w.finish())).is_err());

        let mut w = W::new();
        w.u16(0x0090);
        w.bytes(&[9u8; NONCE_LEN + 1]);
        assert!(Op::decode(&mut Rd::new(&w.finish())).is_err());

        let mut w = W::new();
        w.u16(0x0091);
        w.bytes(&[7u8; DIGEST_LEN - 1]);
        assert!(Op::decode(&mut Rd::new(&w.finish())).is_err());

        let mut w = W::new();
        w.u16(0x0191);
        w.bytes(&[3u8; DIGEST_LEN - 1]);
        w.bool(true);
        assert!(Reply::decode(&mut Rd::new(&w.finish())).is_err());

        let mut w = W::new();
        w.u16(0x0092);
        w.u8(crate::engine::PROTOCOL_VERSION as u8);
        assert!(Op::decode(&mut Rd::new(&w.finish())).is_err());

        let mut w = W::new();
        w.u16(0x0192);
        w.u8(crate::engine::PROTOCOL_VERSION as u8);
        assert!(Reply::decode(&mut Rd::new(&w.finish())).is_err());

        // `SecureBegin` 的套件字段是定长一字节：少了就是「不知道要哪个套件」，
        // 不能当 0（= 不加密）默认掉 —— 那会把一条协商帧悄悄变成降级。
        let mut w = W::new();
        w.u16(0x0093);
        assert!(Op::decode(&mut Rd::new(&w.finish())).is_err());

        let mut w = W::new();
        w.u16(0x0193);
        assert!(Reply::decode(&mut Rd::new(&w.finish())).is_err());
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
                names: Names::from_vec(&[b"a".to_vec(), b"b".to_vec()]),
                next: 4,
                end: false,
            },
            Reply::Ref {
                count: 5,
                bytes: 500,
            },
            Reply::Locks(vec![(b"o1".to_vec(), 1, 99)]),
            Reply::Table(Some(TableDef {
                name: b"kv".to_vec(),
                columns: vec![
                    ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
                    ColumnDef { name: b"b".to_vec(), flags: 0 },
                ],
            })),
            Reply::Table(None),
            Reply::Tables(vec![TableDef {
                name: b"kv".to_vec(),
                columns: vec![ColumnDef { name: b"a".to_vec(), flags: COL_INDEX }],
            }]),
            Reply::Row(Some(RowRec {
                key: b"k1".to_vec(),
                cols: vec![b"v1".to_vec(), Vec::new()],
            })),
            Reply::Row(None),
            Reply::Rows {
                rows: vec![
                    RowRec { key: b"k1".to_vec(), cols: vec![b"v1".to_vec()] },
                    RowRec { key: b"k2".to_vec(), cols: vec![Vec::new()] },
                ],
                end: true,
            },
            Reply::Count(7),
            Reply::Batch {
                replies: vec![
                    Reply::Ok,
                    Reply::Err {
                        code: 22,
                        msg: b"x".to_vec(),
                    },
                    Reply::Pong,
                ],
            },
            Reply::DirentsPlus {
                inos: vec![2, 3],
                kinds: vec![0, 1],
                names: Names::from_vec(&[b"a".to_vec(), b"b".to_vec()]),
                attrs: vec![
                    NodeAttr::default(),
                    NodeAttr {
                        mode: 0o755,
                        uid: 1,
                        gid: 2,
                        size: 4096,
                        nlink: 2,
                        atime: -1,
                        mtime: -2,
                        ctime: -3,
                        data_ref: b"r9".to_vec(),
                    },
                ],
                next: 4,
                end: false,
            },
            // ---- 日志传送 ----
            Reply::WalBatch {
                records: vec![vec![0x01u8, 0x02, 0x03], Vec::new()],
                base_seq: 41,
                next_off: 96,
                durable_seq: 42,
                snap_seq: 40,
                gap: false,
                epoch: 3,
            },
            // 空交付也得能往返：这是空闲副本每一轮拿到的那个应答。
            Reply::WalBatch {
                records: Vec::new(),
                base_seq: 43,
                next_off: 96,
                durable_seq: 42,
                snap_seq: 40,
                gap: true,
                epoch: 0,
            },
            Reply::SnapChunk {
                bytes: vec![7; 64],
                total: 4096,
                snap_seq: 40,
                epoch: 3,
            },
            Reply::SnapChunk {
                bytes: Vec::new(),
                total: 0,
                snap_seq: 0,
                epoch: 0,
            },
            // 换代探针的应答：`len == 0` 的那条路。
            Reply::SnapPrefix {
                prefix_crc: 0xdead_beef,
                total: 4096,
                snap_seq: 40,
                epoch: 3,
            },
            Reply::VersionOk {
                server_proto: crate::engine::PROTOCOL_VERSION,
            },
            Reply::Challenge {
                server_nonce: [0x11; NONCE_LEN],
                proto: crate::engine::PROTOCOL_VERSION,
            },
            Reply::AuthOk {
                server_mac: [0x22; DIGEST_LEN],
                authed: true,
            },
            // 服务端没启用认证那一支：全零 MAC + `authed = false`。
            Reply::AuthOk {
                server_mac: [0u8; DIGEST_LEN],
                authed: false,
            },
            Reply::SecureOk {
                suite: crate::net::record::SECURE_SUITE_CHACHAPOLY1305,
            },
            // 服务端明确拒绝加密那一支。
            Reply::SecureOk { suite: 0 },
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

    /// 传送应答的两个护栏在解码侧的形状：条数超 `MAX_FETCH_RECORDS` 拒帧，
    /// 单条超 `MAX_RECORD` 拒帧。两者都必须是 `Err` 而不是 panic：解码失败在
    /// 事件循环里就是断连，而越界的 `with_capacity` 会先一步把内存吃掉。
    #[test]
    fn fetch_reply_bounds_are_enforced_on_decode() {
        // 一个合法的 `WalBatch` 编码前缀长什么样，这里手工编两个越界帧。
        let mut w = W::new();
        w.u16(0x0180);
        w.u32(MAX_FETCH_RECORDS as u32 + 1);
        let over_count = w.finish();
        let mut r = Rd::new(&over_count);
        assert!(
            Reply::decode(&mut r).is_err(),
            "条数越上限要拒帧，不能先分配再撑"
        );

        let mut w = W::new();
        w.u16(0x0180);
        w.u32(1);
        w.u32(crate::engine::wal::MAX_RECORD + 1);
        let over_len = w.finish();
        let mut r = Rd::new(&over_len);
        assert!(
            Reply::decode(&mut r).is_err(),
            "单条记录超 `MAX_RECORD` 要拒，不能按声明长度读进来"
        );

        // 反之 `SnapFetch` 的 `len` 上限**不在解码侧拦**（见 decode 处的注释）：
        // 越界得在 `Engine::exec` 回 `EINVAL`，不能让一个请求把连接敲掉。
        let mut w = W::new();
        Op::SnapFetch {
            offset: 0,
            len: SNAP_CHUNK_MAX + 1,
        }
        .encode(&mut w);
        let bytes = w.finish();
        let mut r = Rd::new(&bytes);
        assert_eq!(
            Op::decode(&mut r).unwrap(),
            Op::SnapFetch {
                offset: 0,
                len: SNAP_CHUNK_MAX + 1
            },
            "越上限的请求要能解出来，才能被回一个 EINVAL 而不是被断连"
        );
    }
}
