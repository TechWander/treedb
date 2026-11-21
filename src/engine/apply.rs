//! `Op` → `(Vec<Log>, Reply)` 的纯翻译层。
//!
//! 这是引擎唯一的 POSIX 语义所在地：读 op 直接查 [`State`] 出应答，
//! 写 op 先算出「提交后的行应该长什么样」（logs）并预生成应答，随后
//! 由 [`crate::engine::core::Engine`] 走 WAL → apply。翻译层对状态
//! **只读**，所以失败路径天然无需回滚——这是单写者流水线能成立的前提。
//!
//! 约定：`logs.is_empty()` 等价于「本 op 是只读的」。

use crate::engine::mem::{DENT_KIND_UNKNOWN, Log, State, TableMeta};
use std::collections::BTreeMap;
use std::ops::Bound;
use crate::engine::model::{
    errno, AuditRec, DataRef, DefRec, Ino, LockRec, Names, Node, NodeAttr, RefRec, Reply, RowRec,
    TableDef, ViewId, ViewMeta, Kind, Op, MAX_BATCH_OPS, MAX_COLUMNS, PUT_IF_ABSENT,
    PUT_IF_EXISTS,
    DEL_IF_EXISTS, COND_NONE, RENAME_NOREPLACE, ROW_KEY_MAX, ROW_PK, SA_ATIME, SA_DATA_REF,
    SA_GID, SA_MODE, SA_MTIME, SA_SIZE, SA_UID, TABLE_NAME_MAX,
};

/// 名字合法性：POSIX 255 字节上限 + 不允许 `/` 和空。
fn name_err(n: &[u8]) -> Option<Reply> {
    if n.is_empty() {
        Some(Reply::err(errno::ENOENT, "empty name"))
    } else if n.len() > 255 {
        Some(Reply::err(errno::ENAMETOOLONG, "name over 255 bytes"))
    } else if n.contains(&b'/') {
        Some(Reply::err(errno::EINVAL, "name contains '/'"))
    } else {
        None
    }
}

fn view_meta(s: &State, view: ViewId) -> Option<(&[u8], &ViewMeta)> {
    let name = s.view_by_id.get(&view)?;
    let meta = s.views.get(name.as_slice())?;
    Some((name.as_slice(), meta))
}

fn no_view(view: ViewId) -> Reply {
    Reply::err(errno::ENODEV, format!("no such view id {view}"))
}

/// 视图级前置检查：视图整个没了就回 ENODEV，别让 FUSE 把「视图被删」
/// 误判成「这个 inode 查不中」而只失效一个 dentry。
fn dead_view(s: &State, view: ViewId) -> bool {
    view_meta(s, view).is_none()
}

/// 通用表的 def 合法性。返回一条 `Reply::Err` 就代表不合法，调用方直接回。
///
/// 这里只查**形状**：名字非空、长度有上界、列数在 1..=上限、列名不重。
/// 列里存什么、怎么算合法值，引擎一概不管。
fn bad_def(def: &TableDef) -> Option<Reply> {
    if def.name.is_empty() {
        return Some(Reply::err(errno::EINVAL, "empty table name"));
    }
    if def.name.len() > TABLE_NAME_MAX {
        return Some(Reply::err(
            errno::ENAMETOOLONG,
            format!("table name over {TABLE_NAME_MAX} bytes"),
        ));
    }
    if def.columns.is_empty() || def.columns.len() > MAX_COLUMNS {
        return Some(Reply::err(
            errno::EINVAL,
            format!("table needs 1..={MAX_COLUMNS} columns"),
        ));
    }
    // 列名重复不破坏按序号寻址，但应答里两份同名列 = 客户端没法解释，
    // 所以宁可在建表时就拒。
    let mut seen: Vec<&[u8]> = Vec::with_capacity(def.columns.len());
    for c in &def.columns {
        if c.name.is_empty() {
            return Some(Reply::err(errno::EINVAL, "empty column name"));
        }
        if c.name.len() > TABLE_NAME_MAX {
            return Some(Reply::err(
                errno::ENAMETOOLONG,
                format!("column name over {TABLE_NAME_MAX} bytes"),
            ));
        }
        if seen.contains(&c.name.as_slice()) {
            return Some(Reply::err(
                errno::EINVAL,
                format!("duplicate column name {}", String::from_utf8_lossy(&c.name)),
            ));
        }
        seen.push(&c.name);
    }
    None
}

fn no_table(name: &[u8]) -> Reply {
    Reply::err(
        errno::ENOENT,
        format!("no such table {}", String::from_utf8_lossy(name)),
    )
}

/// `column` 能不能用来列举：[`ROW_PK`] 恒可，否则必须是建了索引的那一列。
fn bad_column(t: &TableMeta, column: u32) -> Option<Reply> {
    if column == ROW_PK {
        return None;
    }
    match t.def.columns.get(column as usize) {
        None => Some(Reply::err(
            errno::EINVAL,
            format!("table {} has no column {column}", String::from_utf8_lossy(&t.def.name)),
        )),
        Some(c) if !c.indexed() => Some(Reply::err(
            errno::EINVAL,
            format!(
                "column {} is not indexed",
                String::from_utf8_lossy(&c.name)
            ),
        )),
        Some(_) => None,
    }
}

/// CAS 条件不命中就返回一条 `EAGAIN`。调用方必须先把 `cond_col` 的范围
/// 检查做完，这里只比较值。
///
/// 行不存在也算不命中：「我以为它是这个值」这句话对一行不存在的行不成立，
/// 让客户端拿到 `ENOENT` 还是 `EAGAIN` 都会误导重试策略。
fn cond_miss(row: Option<&RowRec>, cond_col: u32, cond_val: &[u8]) -> Option<Reply> {
    if cond_col == COND_NONE {
        return None;
    }
    let Some(r) = row else {
        return Some(Reply::err(errno::EAGAIN, "condition needs an existing row"));
    };
    match r.cols.get(cond_col as usize) {
        Some(v) if v == cond_val => None,
        _ => Some(Reply::err(errno::EAGAIN, "condition not met")),
    }
}

fn node_of(s: &State, view: ViewId, ino: Ino) -> Option<Node> {
    s.nodes.get(&(view, ino)).cloned()
}

/// 只要 kind，不要整个 inode。readdir 一页 100 条，走 [`node_of`] 就是 100 次
/// `Node` 克隆（含 `data_ref`/`link` 两个 Vec）—— micro 量到这一项占了一页
/// 计算成本的八成。
fn kind_of(s: &State, view: ViewId, ino: Ino) -> u8 {
    s.nodes.get(&(view, ino)).map(|n| n.kind.code()).unwrap_or(0)
}

/// `child` 是否落在 `anc` 的子树内（含自身）。rename 用来挡目录成环。
fn is_in_subtree(s: &State, view: ViewId, anc: Ino, child: Ino) -> bool {
    let mut cur = child;
    for _ in 0..64 {
        if cur == anc {
            return true;
        }
        match s.nodes.get(&(view, cur)) {
            // 根节点 parent 为 0，走到 0 即出树。
            Some(n) if n.parent == cur || n.parent == 0 => return false,
            Some(n) => cur = n.parent,
            None => return false,
        }
    }
    false
}

fn dir_empty(s: &State, view: ViewId, ino: Ino) -> bool {
    s.dir_empty(view, ino)
}

/// 分配一个视图内唯一 ino，并把 ViewMeta 的游标推进（这条 PutView 必须
/// 和 PutNode 同批进 WAL，否则重启后 ino 会被复用）。
fn alloc_ino(name: &[u8], meta: &ViewMeta) -> (Ino, Log) {
    let ino = meta.next_ino;
    let mut m = meta.clone();
    m.next_ino = ino + 1;
    (
        ino,
        Log::PutView {
            name: name.to_vec(),
            meta: m,
        },
    )
}

fn new_node(ino: Ino, parent: Ino, name: &[u8], kind: Kind, mode: u32, uid: u32, gid: u32, now: i64) -> Node {
    Node {
        ino,
        parent,
        name: name.to_vec(),
        kind,
        mode,
        uid,
        gid,
        size: 0,
        nlink: if kind == Kind::Dir { 2 } else { 1 },
        atime: now,
        mtime: now,
        ctime: now,
        data_ref: Vec::new(),
        link: Vec::new(),
    }
}

/// 一个 op 内对 `refs` 账本的累计增减。
///
/// 必须汇总后再产出 `Log::PutRef`：日志存的是**整行绝对值**，同一
/// data_ref 逐笔产出两行会让前一行被后一行覆盖——而「换 ref + 同时
/// 改 size」正好会命中同一行两次。
#[derive(Default)]
struct RefBook {
    deltas: BTreeMap<DataRef, (i64, i64)>,
}

impl RefBook {
    fn up(&mut self, d: &DataRef, bytes: u64) {
        let e = self.deltas.entry(d.clone()).or_insert((0, 0));
        e.0 += 1;
        e.1 += bytes as i64;
    }
    fn down(&mut self, d: &DataRef, bytes: u64) {
        let e = self.deltas.entry(d.clone()).or_insert((0, 0));
        e.0 -= 1;
        e.1 -= bytes as i64;
    }
    /// 计数不变、只改字节（持有者的 size 变了）。
    fn resize(&mut self, d: &DataRef, old: u64, new: u64) {
        let e = self.deltas.entry(d.clone()).or_insert((0, 0));
        e.1 += new as i64 - old as i64;
    }
    fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }
    /// 基于 `s` 里的当前行算出新行。返回日志与每行的新值（给 Reply 用）。
    fn finish(&self, s: &State) -> (Vec<Log>, BTreeMap<DataRef, RefRec>) {
        let mut logs = Vec::with_capacity(self.deltas.len());
        let mut out = BTreeMap::new();
        for (d, (dc, db)) in &self.deltas {
            let cur = s.refs.get(d).copied().unwrap_or(RefRec { count: 0, bytes: 0 });
            let count = (cur.count as i64 + dc).max(0) as u64;
            // 最后一个持有者离开 → 整行被 `State::apply` 删掉，字节额也必须
            // 归零；否则回给客户端的 `(0, 非 0)` 是一个查不到的幻影值。
            let bytes = if count == 0 { 0 } else { (cur.bytes as i64 + db).max(0) as u64 };
            let rec = RefRec { count, bytes };
            out.insert(d.clone(), rec);
            logs.push(Log::PutRef { data_ref: d.clone(), rec });
        }
        (logs, out)
    }
}

/// 把 `node` 的 data_ref 从旧值切到新值，增减记到 `book`。
///
/// 记账口径：一笔持有的字节额 = 持有者当时的 `size`。所以旧 ref 减
/// `prev_size`（本次 setattr 之前的大小），新 ref 加切换后的 `node.size`。
/// 不变量 `refs[d].bytes == Σ 持有 d 的 inode.size` 因此始终成立。
fn swap_data_ref(node: &mut Node, book: &mut RefBook, prev_size: u64, new_ref: DataRef) {
    if node.data_ref == new_ref {
        return;
    }
    if !node.data_ref.is_empty() {
        book.down(&node.data_ref, prev_size);
    }
    if !new_ref.is_empty() {
        book.up(&new_ref, node.size);
    }
    node.data_ref = new_ref;
}

/// `book` 落账后，`d` 这个内容对象是否刚好失去最后一个持有者。
///
/// 是就把它原样带回（调用方可以删字节），否就回空串。这个判定必须跟删
/// dentry 落在**同一个 op** 里：让客户端自己 `Unlink` 后再 `RefLookup` 一次，
/// 中间那个窗口里另一个持有者可以重新引用同一个键，于是 GC 删掉还在用的字节。
fn last_holder(rows: &BTreeMap<DataRef, RefRec>, d: &DataRef) -> DataRef {
    if d.is_empty() {
        return Vec::new();
    }
    match rows.get(d) {
        Some(r) if r.count == 0 => d.clone(),
        _ => Vec::new(),
    }
}

/// [`Op::Batch`] 的翻译：整批要么产出**一条**容器日志，要么一条都不产。
///
/// 预演必须落在 `State` 的副本上：本层对状态只读，而批内第 i 条要看到前 i 条
/// 的效果 —— 唯一的办法是把前面那几条 `apply` 进一份副本。全只读的批不碰副本
/// （没有 `Log` 可 apply），也就不付这次拷贝。
///
/// 失败时真状态一个字节没被动过：副本是局部变量，直接丢掉 —— 这是「不需要
/// undo 日志」的全部原因，也是这条路径能在 `translate` 这种纯函数里成立的前提。
/// 代价则很具体：含写的批在写锁里拷一份 `State`，批越宽、状态越大越贵。
fn batch(s: &State, now: i64, ops: &[Op]) -> (Vec<Log>, Reply) {
    if ops.is_empty() {
        return (vec![], Reply::err(errno::EINVAL, "empty batch"));
    }
    // 解码层已拦过一道（那里报 Codec 错 = 断连，因为要先防内存放大）；
    // 这一道管的是代码里结构化的构造，以及给客户端一个可读的 EINVAL。
    if ops.len() > MAX_BATCH_OPS {
        return (
            vec![],
            Reply::err(
                errno::EINVAL,
                format!(
                    "batch carries {} ops, over MAX_BATCH_OPS={MAX_BATCH_OPS}",
                    ops.len()
                ),
            ),
        );
    }
    for (i, o) in ops.iter().enumerate() {
        if o.is_control() {
            return (
                vec![],
                Reply::err(
                    errno::EINVAL,
                    format!("batch op #{i} is a control-plane op (see Op::is_control)"),
                ),
            );
        }
        if matches!(o, Op::Batch { .. }) {
            return (
                vec![],
                Reply::err(errno::EINVAL, format!("batch op #{i} is itself a batch")),
            );
        }
    }
    let mut replies: Vec<Reply> = Vec::with_capacity(ops.len());
    // 纯读批：没有副作用，直接在真状态上逐条翻译，不付拷贝。
    if ops.iter().all(|o| o.is_readonly()) {
        for (i, o) in ops.iter().enumerate() {
            let (_, r) = translate(s, now, o);
            if matches!(r, Reply::Err { .. }) {
                return (vec![], batch_fail(i, r));
            }
            replies.push(r);
        }
        return (vec![], Reply::Batch { replies });
    }
    // 含写的批：预演落在副本上。整批共用一个 `now` —— 同一次提交里的几条
    // `Setattr` 应该看到同一个时间戳；逐条各取一次会让「同一时刻」在毫秒边界上
    // 碎开，也会让同一批重放两次得到不同结果。
    let mut scratch = s.clone();
    let mut logs: Vec<Log> = Vec::new();
    for (i, o) in ops.iter().enumerate() {
        let (ls, r) = translate(&scratch, now, o);
        if matches!(r, Reply::Err { .. }) {
            return (vec![], batch_fail(i, r));
        }
        // 单向自检：只读 op 产日志就说明 `is_readonly` 判错了（反向不成立：
        // 幂等的 `RowDelete` 不存在的行也是「非只读、零日志」）。
        if !ls.is_empty() && o.is_readonly() {
            return (
                vec![],
                Reply::err(
                    errno::EIO,
                    format!("batch op #{i} produced logs but is classified readonly"),
                ),
            );
        }
        for l in &ls {
            scratch.apply(l);
        }
        logs.extend(ls);
        replies.push(r);
    }
    // 整批都没有效果（全部幂等空转）：不产容器，也就不占一条 WAL 记录。
    if logs.is_empty() {
        return (vec![], Reply::Batch { replies });
    }
    (vec![Log::Batch { logs }], Reply::Batch { replies })
}

/// 把子 op 的 errno 抬成整批的失败，`msg` 前缀写清是第几条。
///
/// `code` 原样搬：调用方拿到的还是那个 `ENOENT` / `EEXIST`，而不是一个模糊的
/// 「批失败」—— 能不能重试、要不要换个名字，都靠这个码。
fn batch_fail(i: usize, r: Reply) -> Reply {
    match r {
        Reply::Err { code, msg } => {
            let mut m = format!("batch op #{i}: ").into_bytes();
            m.extend_from_slice(&msg);
            Reply::Err { code, msg: m }
        }
        other => other,
    }
}

/// 翻译一个 op。`now` 是引擎时钟（毫秒），用于 ctime 与租约判定。
pub fn translate(s: &State, now: i64, op: &Op) -> (Vec<Log>, Reply) {
    match op {
        // ---- 视图生命周期 ----
        Op::CreateView { name } => {
            if let Some(r) = name_err(name) {
                return (vec![], r);
            }
            if s.views.contains_key(name) {
                return (vec![], Reply::err(errno::EEXIST, "view exists"));
            }
            let id = s.next_view_id;
            let meta = ViewMeta {
                id,
                root: 1,
                next_ino: 2,
            };
            let root = new_node(1, 0, name, Kind::Dir, 0o755, 0, 0, now);
            (
                vec![
                    Log::PutView {
                        name: name.clone(),
                        meta: meta.clone(),
                    },
                    Log::PutNode { view: id, node: root },
                ],
                Reply::View { id, root: 1 },
            )
        }
        Op::DropView { name } => match s.views.get(name) {
            None => (vec![], Reply::err(errno::ENODEV, "no such view")),
            Some(m) => {
                let id = m.id;
                (
                    vec![Log::PurgeView {
                        name: name.clone(),
                        view: id,
                    }],
                    Reply::Ok,
                )
            }
        },
        Op::ListViews => {
            let v = s
                .views
                .iter()
                .map(|(n, m)| (n.clone(), m.id, m.root))
                .collect();
            (vec![], Reply::Views(v))
        }

        // ---- 目录树：只读 ----
        Op::Lookup { view, parent, name } => {
            let (_, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            let _ = &meta;
            if let Some(r) = name_err(name) {
                return (vec![], r);
            }
            let ino = match s.dentry(*view, *parent, name) {
                Some(d) => d.ino,
                None => return (vec![], Reply::err(errno::ENOENT, "lookup")),
            };
            match node_of(s, *view, ino) {
                Some(n) => (vec![], Reply::Node(n)),
                None => (vec![], Reply::err(errno::ESTALE, "dangling dentry")),
            }
        }
        Op::Getattr { view, ino } => {
            if dead_view(s, *view) {
                return (vec![], no_view(*view));
            }
            match node_of(s, *view, *ino) {
                Some(n) => (vec![], Reply::Node(n)),
                None => (vec![], Reply::err(errno::ENOENT, "no such inode")),
            }
        }
        Op::Readlink { view, ino } => {
            if dead_view(s, *view) {
                return (vec![], no_view(*view));
            }
            match node_of(s, *view, *ino) {
                Some(n) if n.kind == Kind::Symlink => (vec![], Reply::Node(n)),
                Some(_) => (vec![], Reply::err(errno::EINVAL, "not a symlink")),
                None => (vec![], Reply::err(errno::ENOENT, "no such inode")),
            }
        }
        Op::ReadDir {
            view,
            parent,
            offset,
            limit,
        } => {
            let (_, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            if let Some(n) = node_of(s, *view, *parent) {
                if n.kind != Kind::Dir {
                    return (vec![], Reply::err(errno::ENOTDIR, "not a directory"));
                }
            } else if *parent != meta.root {
                return (vec![], Reply::err(errno::ENOENT, "no such directory"));
            }
            // offset 是「已返回的条目数」：直接在内层表上 skip，
            // 不先把整目录物化（千级目录也不能每页拷一份）。
            let lim = if *limit == 0 { 1000 } else { *limit as usize };
            let off = *offset as usize;
            let dir = match s.dentries.get(&(*view, *parent)) {
                Some(m) => m,
                None => {
                    return (
                        vec![],
                        Reply::Dirents {
                            inos: Vec::new(),
                            kinds: Vec::new(),
                            names: Names::new(),
                            next: *offset,
                            end: true,
                        },
                    )
                }
            };
            let mut inos = Vec::with_capacity(lim.min(4096));
            let mut kinds = Vec::with_capacity(lim.min(4096));
            // 名字进扁平缓冲，32 B/条是个不需要预知的保守估计 —— 真正的收益
            // 是不再每条一次 malloc（micro 定价 42 ns/条，占这页成本 97%）。
            let mut names = Names::with_capacity(lim.min(4096), lim.min(4096) * 32);
            let mut it = dir.iter().skip(off);
            for (name, d) in it.by_ref().take(lim) {
                // kind 存在 dentry 里，这一页不用回 `nodes` 表下钻（那是
                // 一页 100 次 cache miss）；只有日志顺序异常导致 kind 未知时
                // 才对那一条回表。
                let kind = if d.kind == DENT_KIND_UNKNOWN {
                    kind_of(s, *view, d.ino)
                } else {
                    d.kind
                };
                inos.push(d.ino);
                kinds.push(kind);
                names.push(name);
            }
            let next = (off + inos.len()) as u64;
            // `end` 要说真话：整页恰好装满时再探一项就知道后面还有没有，
            // 不能让调用方多跑一页空 readdir 才发现到底（O(1) 换语义确定）。
            let end = it.next().is_none();
            (vec![], Reply::Dirents { inos, kinds, names, next, end })
        }
        Op::ReadDirPlus {
            view,
            parent,
            offset,
            limit,
        } => {
            // 目录页本身与 `ReadDir` 一模一样；多的只是每条一次 `nodes` 下钻。
            // 这一刀省的不是服务端的时间，是客户端那一页 N 次往返。
            let (_, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            if let Some(n) = node_of(s, *view, *parent) {
                if n.kind != Kind::Dir {
                    return (vec![], Reply::err(errno::ENOTDIR, "not a directory"));
                }
            } else if *parent != meta.root {
                return (vec![], Reply::err(errno::ENOENT, "no such directory"));
            }
            let lim = if *limit == 0 { 1000 } else { *limit as usize };
            let off = *offset as usize;
            let dir = match s.dentries.get(&(*view, *parent)) {
                Some(m) => m,
                None => {
                    return (
                        vec![],
                        Reply::DirentsPlus {
                            inos: Vec::new(),
                            kinds: Vec::new(),
                            names: Names::new(),
                            attrs: Vec::new(),
                            next: *offset,
                            end: true,
                        },
                    )
                }
            };
            let mut inos = Vec::with_capacity(lim.min(4096));
            let mut kinds = Vec::with_capacity(lim.min(4096));
            let mut attrs = Vec::with_capacity(lim.min(4096));
            let mut names = Names::with_capacity(lim.min(4096), lim.min(4096) * 32);
            let mut it = dir.iter().skip(off);
            for (name, d) in it.by_ref().take(lim) {
                let node = s.nodes.get(&(*view, d.ino));
                let kind = match node {
                    Some(n) => n.kind.code(),
                    None => {
                        if d.kind == DENT_KIND_UNKNOWN {
                            kind_of(s, *view, d.ino)
                        } else {
                            d.kind
                        }
                    }
                };
                inos.push(d.ino);
                kinds.push(kind);
                names.push(name);
                // inode 缺失（日志顺序异常）时回零值而不是丢这一条：四个平行
                // 数组必须同长，少一项就是整页错位，那比一个零属性糟得多。
                attrs.push(node.map(NodeAttr::of).unwrap_or_default());
            }
            let next = (off + inos.len()) as u64;
            let end = it.next().is_none();
            (
                vec![],
                Reply::DirentsPlus {
                    inos,
                    kinds,
                    names,
                    attrs,
                    next,
                    end,
                },
            )
        }
        Op::LockQuery { view, ino } => {
            let v: Vec<(Vec<u8>, u8, i64)> = s
                .locks
                .range((
                    Bound::Included((*view, *ino, Vec::new())),
                    Bound::Unbounded,
                ))
                .take_while(move |k| k.0 .0 == *view && k.0 .1 == *ino)
                .filter(|(_, r)| r.lease_expiry == 0 || r.lease_expiry > now)
                .map(|((_, _, owner), r)| (owner.clone(), r.kind, r.lease_expiry))
                .collect();
            (vec![], Reply::Locks(v))
        }
        Op::RefLookup { data_ref } => match s.refs.get(data_ref) {
            Some(r) => (vec![], Reply::Ref { count: r.count, bytes: r.bytes }),
            None => (vec![], Reply::Ref { count: 0, bytes: 0 }),
        },

        // ---- 目录树：写 ----
        Op::Create {
            view,
            parent,
            name,
            ..
        }
        | Op::Mkdir {
            view,
            parent,
            name,
            ..
        }
        | Op::Symlink {
            view,
            parent,
            name,
            ..
        } => {
            let (uid, gid) = match op {
                Op::Create { uid, gid, .. } | Op::Mkdir { uid, gid, .. } => (*uid, *gid),
                Op::Symlink { uid, gid, .. } => (*uid, *gid),
                _ => unreachable!(),
            };
            let (kind, mode, link) = match op {
                Op::Create { mode, .. } => (Kind::File, *mode, Vec::new()),
                Op::Mkdir { mode, .. } => (Kind::Dir, *mode, Vec::new()),
                Op::Symlink { target, .. } => (Kind::Symlink, 0o777, target.clone()),
                _ => unreachable!(),
            };
            if let Some(r) = name_err(name) {
                return (vec![], r);
            }
            let (vname, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            let mut logs = Vec::new();
            let mut book = RefBook::default();
            // Create/Mkdir 遇同名 EEXIST；Symlink 走 upsert：同名**非目录**项被
            // 原子替换，目录项不覆盖（空 → EISDIR，非空 → ENOTEMPTY）。
            //
            // 这里**不是** `symlink(2)` 的语义（它遇任何已存在项都回 EEXIST）：
            // 覆盖是给调用方的 create-or-replace 入口留的，删掉它就得多一次
            // Unlink+Symlink 两步，中间留一个“名字不存在”的窗口。目录不
            // 覆盖则是硬约束：替换一个目录意味着递归删掉它的孩子。
            if let Some(d) = s.dentry(*view, *parent, name) {
                if kind != Kind::Symlink {
                    return (vec![], Reply::err(errno::EEXIST, "file exists"));
                }
                let old_ino = d.ino;
                let old = match node_of(s, *view, old_ino) {
                    Some(o) => o,
                    None => return (vec![], Reply::err(errno::ESTALE, "dangling dentry")),
                };
                if old.kind == Kind::Dir {
                    return (
                        vec![],
                        Reply::err(
                            if dir_empty(s, *view, old_ino) {
                                errno::EISDIR
                            } else {
                                errno::ENOTEMPTY
                            },
                            "target is a directory",
                        ),
                    );
                }
                // 旧项如果是被 `Op::Link` 共享的 inode（nlink > 1），这里只摘
                // 一个名字：另一些名字还指着它，删 node / 下内容引用会让那些
                // 文件读到旧字节之外的东西（甚至把对象真删掉）。
                if old.nlink <= 1 {
                    logs.push(Log::DelNode {
                        view: *view,
                        ino: old_ino,
                    });
                    if !old.data_ref.is_empty() {
                        book.down(&old.data_ref, old.size);
                    }
                } else {
                    let mut o = old.clone();
                    o.nlink -= 1;
                    o.ctime = now;
                    logs.push(Log::PutNode {
                        view: *view,
                        node: o,
                    });
                }
            }
            let parent_node = match node_of(s, *view, *parent) {
                Some(p) if p.kind == Kind::Dir => p,
                Some(_) => return (vec![], Reply::err(errno::ENOTDIR, "parent is not a dir")),
                None => return (vec![], Reply::err(errno::ENOENT, "no such parent")),
            };
            let (ino, bump) = alloc_ino(vname, meta);
            let mut node = new_node(ino, *parent, name, kind, mode, uid, gid, now);
            node.link = link;
            if kind == Kind::Symlink {
                node.size = node.link.len() as u64;
            }
            logs.push(bump);
            logs.push(Log::PutNode {
                view: *view,
                node: node.clone(),
            });
            logs.push(Log::PutDentry {
                view: *view,
                parent: *parent,
                name: name.clone(),
                ino,
            });
            // 子目录让父目录 nlink +1（`.` 的那一票）。
            if kind == Kind::Dir {
                let mut p = parent_node.clone();
                p.nlink += 1;
                p.ctime = now;
                logs.push(Log::PutNode {
                    view: *view,
                    node: p,
                });
            }
            logs.extend(book.finish(s).0);
            (logs, Reply::Node(node))
        }
        Op::Link {
            view,
            ino,
            parent,
            name,
        } => {
            if let Some(r) = name_err(name) {
                return (vec![], r);
            }
            if view_meta(s, *view).is_none() {
                return (vec![], no_view(*view));
            }
            // 父目录先验：“路径前缀不是目录” 优先于 “源 inode 不存在”，与
            // unlink/rename 同一条口径。
            match node_of(s, *view, *parent) {
                Some(p) if p.kind == Kind::Dir => {}
                Some(_) => return (vec![], Reply::err(errno::ENOTDIR, "parent is not a dir")),
                None => return (vec![], Reply::err(errno::ENOENT, "no such parent")),
            }
            let mut node = match node_of(s, *view, *ino) {
                Some(n) => n,
                None => return (vec![], Reply::err(errno::ENOENT, "no such inode")),
            };
            // POSIX link(2)：源是目录回 EPERM（视图根本来也是目录）。不做“只允许
            // root 链接目录”的例外：本引擎没有 uid 0 的概念，能写这棵树的人已经
            // 越过权限面了，假装限制了反而会让人以为这里有一道门。
            if node.kind == Kind::Dir {
                return (vec![], Reply::err(errno::EPERM, "cannot link a directory"));
            }
            // 包括 `ln f f`：POSIX 也把它算 EEXIST，而不是 no-op。
            if s.dentry(*view, *parent, name).is_some() {
                return (vec![], Reply::err(errno::EEXIST, "file exists"));
            }
            // 不分配 ino、不碰 data_ref：两个名字就是同一个 inode，只多一票引用。
            // `node.parent`/`node.name` 保留第一个名字的位置（它们只是冗余的
            // “主名”，readdir 走 dentries，不读这两个字段）。
            node.nlink += 1;
            node.ctime = now;
            let logs = vec![
                Log::PutNode {
                    view: *view,
                    node: node.clone(),
                },
                Log::PutDentry {
                    view: *view,
                    parent: *parent,
                    name: name.clone(),
                    ino: *ino,
                },
            ];
            (logs, Reply::Node(node))
        }
        Op::Unlink { view, parent, name } | Op::Rmdir { view, parent, name } => {
            let want_dir = matches!(op, Op::Rmdir { .. });
            let (_, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            if let Some(r) = name_err(name) {
                return (vec![], r);
            }
            // 先验父目录再查 dentry：与 `Rename` 同一条口径。POSIX 里「路径
            // 前缀不是目录」（ENOTDIR）优先于「名字查不到」（ENOENT），
            // 以前这里直接查 dentry，`unlink(file/child)` 回的是 ENOENT ——
            // 同一个坏父 inode，rename 与 unlink 给出两个不同的 errno。
            match node_of(s, *view, *parent) {
                Some(d) if d.kind == Kind::Dir => {}
                Some(_) => return (vec![], Reply::err(errno::ENOTDIR, "parent is not a dir")),
                None => return (vec![], Reply::err(errno::ENOENT, "no such parent")),
            }
            let ino = match s.dentry(*view, *parent, name) {
                Some(d) => d.ino,
                None => return (vec![], Reply::err(errno::ENOENT, "no such entry")),
            };
            let node = match node_of(s, *view, ino) {
                Some(n) => n,
                None => return (vec![], Reply::err(errno::ESTALE, "dangling dentry")),
            };
            if want_dir && node.kind != Kind::Dir {
                return (vec![], Reply::err(errno::ENOTDIR, "not a directory"));
            }
            if !want_dir && node.kind == Kind::Dir {
                return (vec![], Reply::err(errno::EISDIR, "is a directory"));
            }
            if node.ino == meta.root {
                return (vec![], Reply::err(errno::EBUSY, "cannot remove view root"));
            }
            if want_dir && !dir_empty(s, *view, ino) {
                return (vec![], Reply::err(errno::ENOTEMPTY, "directory not empty"));
            }
            // 摘一条名字与删 inode 是两件事（`Op::Link` 之后前者不再蕴含后
            // 者）：nlink > 1 时只减一票、内容对象仍被其余名字持有，应答里的
            // 空键也保证调用方不会把它当孤儿回收。
            //
            // 目录不跑这条路：`Op::Link` 拒目录，目录的 nlink 是「. + 子目录
            // 数」，与引用计数不是同一个量。
            let last = want_dir || node.nlink <= 1;
            let mut logs = vec![Log::DelDentry {
                view: *view,
                parent: *parent,
                name: name.clone(),
            }];
            if last {
                logs.push(Log::DelNode {
                    view: *view,
                    ino,
                });
            } else {
                let mut n = node.clone();
                n.nlink -= 1;
                n.ctime = now;
                logs.push(Log::PutNode {
                    view: *view,
                    node: n,
                });
            }
            let mut book = RefBook::default();
            if last && !node.data_ref.is_empty() {
                book.down(&node.data_ref, node.size);
            }
            if want_dir {
                if let Some(mut p) = node_of(s, *view, *parent) {
                    p.nlink = p.nlink.saturating_sub(1);
                    p.ctime = now;
                    logs.push(Log::PutNode {
                        view: *view,
                        node: p,
                    });
                }
            }
            // 失去最后一个持有者的键直接装在应答里（见 `last_holder`）。
            let (l, rows) = book.finish(s);
            logs.extend(l);
            let freed = last_holder(&rows, &node.data_ref);
            (logs, Reply::NodeRef(freed))
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
            let mut node = match node_of(s, *view, *ino) {
                Some(n) => n,
                None => {
                    return (vec![], if dead_view(s, *view) { no_view(*view) } else { Reply::err(errno::ENOENT, "no such inode") })
                }
            };
            let mut book = RefBook::default();
            // 账本口径：一笔持有 = 持有者当时的 size，故先记住旧值。
            let (prev_size, prev_ref) = (node.size, node.data_ref.clone());
            if *valid & SA_MODE != 0 {
                node.mode = *mode;
            }
            if *valid & SA_UID != 0 {
                node.uid = *uid;
            }
            if *valid & SA_GID != 0 {
                node.gid = *gid;
            }
            if *valid & SA_SIZE != 0 {
                if node.kind == Kind::Dir {
                    return (vec![], Reply::err(errno::EISDIR, "cannot size a dir"));
                }
                node.size = *size;
            }
            if *valid & SA_ATIME != 0 {
                node.atime = *atime;
            }
            if *valid & SA_MTIME != 0 {
                node.mtime = *mtime;
            }
            if *valid & SA_DATA_REF != 0 {
                // 内容对象提交：data-server 知道对象多大，若没同时给
                // SA_SIZE 就把 ref_bytes 当成本次提交后的 size，否则
                // size 与账本会从此刻起永久分叉。
                if *ref_bytes != 0 && *valid & SA_SIZE == 0 && node.kind != Kind::Dir {
                    node.size = *ref_bytes;
                }
                swap_data_ref(&mut node, &mut book, prev_size, data_ref.clone());
            }
            // 只改 size、ref 未变（换 ref 时已按新 size 记账）：补差额。
            if *valid & SA_SIZE != 0
                && !node.data_ref.is_empty()
                && node.data_ref == prev_ref
                && node.size != prev_size
            {
                book.resize(&node.data_ref, prev_size, node.size);
            }
            node.ctime = now;
            let mut logs = Vec::new();
            if !book.is_empty() {
                // PutRef 在前：同一批里它不依赖 PutNode，但反过来读日志的
                // 人期望先看到账本变动再看到 inode 快照。
                logs.extend(book.finish(s).0);
            }
            logs.push(Log::PutNode {
                view: *view,
                node: node.clone(),
            });
            (logs, Reply::Node(node))
        }
        Op::Rename {
            view,
            old_parent,
            old_name,
            new_parent,
            new_name,
            flags,
        } => {
            let (_, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            if let Some(r) = name_err(old_name).or_else(|| name_err(new_name)) {
                return (vec![], r);
            }
            // 先验两个父目录：POSIX 里「路径前缀不是目录」（ENOTDIR）优先于
            // 「名字查不到」（ENOENT），反过来先查 dentry 会把 ENOTDIR 掉。
            for p in [*old_parent, *new_parent] {
                match node_of(s, *view, p) {
                    Some(d) if d.kind == Kind::Dir => {}
                    Some(_) => return (vec![], Reply::err(errno::ENOTDIR, "parent not a dir")),
                    None => return (vec![], Reply::err(errno::ENOENT, "no such parent")),
                }
            }
            let ino = match s.dentry(*view, *old_parent, old_name) {
                Some(d) => d.ino,
                None => return (vec![], Reply::err(errno::ENOENT, "no such entry")),
            };
            let mut node = match node_of(s, *view, ino) {
                Some(n) => n,
                None => return (vec![], Reply::err(errno::ESTALE, "dangling dentry")),
            };
            if ino == meta.root {
                return (vec![], Reply::err(errno::EBUSY, "cannot rename view root"));
            }
            if node.kind == Kind::Dir && is_in_subtree(s, *view, ino, *new_parent) {
                return (vec![], Reply::err(errno::EINVAL, "dir into own subtree"));
            }
            if *old_parent == *new_parent && old_name == new_name {
                return (vec![], Reply::NodeRef(Vec::new())); // POSIX: rename 到自身是 no-op
            }
            // `RENAME_NOREPLACE`：目标已存在就要失败，而不是静默覆盖。这一位
            // 只能在服务端看：客户端先 `Lookup` 再 `Rename` 不是同一个回事，
            // 两者之间落进来的同名目标会被覆盖掉。
            let noreplace = flags & RENAME_NOREPLACE != 0;
            let mut logs = Vec::new();
            let mut book = RefBook::default();
            // 被覆盖目标的内容键：只有它失去最后一个持有者时才需要带回给 GC。
            let mut over: DataRef = Vec::new();
            if let Some(t) = s.dentry(*view, *new_parent, new_name) {
                let tino = t.ino;
                if noreplace {
                    return (vec![], Reply::err(errno::EEXIST, "target exists and NOREPLACE"));
                }
                let t = match node_of(s, *view, tino) {
                    Some(t) => t,
                    None => return (vec![], Reply::err(errno::ESTALE, "dangling target")),
                };
                // 源与目标其实是同一个 inode 的两个名字：POSIX 要求成功且什么都不
                // 做。硬链接之前这不可能，现在必须显式挡 —— 否则「被覆盖项只减
                // 一票」与末尾那份按旧 `nlink` 写的 `PutNode` 会打架，留下一个
                // 没有第二个名字却 `nlink = 2` 的 inode（整个 inode 从此泄漏）。
                if tino == ino {
                    return (vec![], Reply::NodeRef(Vec::new()));
                }
                if t.kind == Kind::Dir && node.kind != Kind::Dir {
                    return (vec![], Reply::err(errno::ENOTDIR, "target is a dir"));
                }
                if t.kind != Kind::Dir && node.kind == Kind::Dir {
                    return (vec![], Reply::err(errno::EISDIR, "target is not a dir"));
                }
                if t.kind == Kind::Dir && !dir_empty(s, *view, tino) {
                    return (vec![], Reply::err(errno::ENOTEMPTY, "target dir not empty"));
                }
                if t.ino == meta.root {
                    return (vec![], Reply::err(errno::EBUSY, "target is view root"));
                }
                // 目录无双链接（`Op::Link` 拒目录），它的 nlink 是「. + 子目录数」；
                // 非目录则看 nlink：多链接 inode 被覆盖时只减一票。
                if t.kind == Kind::Dir || t.nlink <= 1 {
                    logs.push(Log::DelNode {
                        view: *view,
                        ino: tino,
                    });
                    if !t.data_ref.is_empty() {
                        book.down(&t.data_ref, t.size);
                        over = t.data_ref.clone();
                    }
                } else {
                    let mut o = t.clone();
                    o.nlink -= 1;
                    o.ctime = now;
                    logs.push(Log::PutNode {
                        view: *view,
                        node: o,
                    });
                }
            }
            node.parent = *new_parent;
            node.name = new_name.clone();
            node.ctime = now;
            logs.extend([
                Log::DelDentry {
                    view: *view,
                    parent: *old_parent,
                    name: old_name.clone(),
                },
                Log::PutDentry {
                    view: *view,
                    parent: *new_parent,
                    name: new_name.clone(),
                    ino,
                },
                Log::PutNode {
                    view: *view,
                    node: node.clone(),
                },
            ]);
            if node.kind == Kind::Dir {
                if *old_parent != *new_parent {
                    for p in [*old_parent, *new_parent] {
                        if let Some(mut d) = node_of(s, *view, p) {
                            d.nlink = if p == *old_parent {
                                d.nlink.saturating_sub(1)
                            } else {
                                d.nlink + 1
                            };
                            logs.push(Log::PutNode {
                                view: *view,
                                node: d,
                            });
                        }
                    }
                }
            }
            let (l, rows) = book.finish(s);
            logs.extend(l);
            let freed = last_holder(&rows, &over);
            (logs, Reply::NodeRef(freed))
        }
        Op::RefUp { data_ref, bytes } => {
            let mut book = RefBook::default();
            book.up(data_ref, *bytes);
            let (logs, rows) = book.finish(s);
            let r = rows.get(data_ref).copied().unwrap_or(RefRec { count: 0, bytes: 0 });
            (logs, Reply::Ref { count: r.count, bytes: r.bytes })
        }
        Op::RefDown { data_ref, bytes } => {
            // 管理面/GC 显式减账：bytes 由调用方给，用来纠正历史估算。
            let mut book = RefBook::default();
            book.down(data_ref, *bytes);
            let (logs, rows) = book.finish(s);
            let r = rows.get(data_ref).copied().unwrap_or(RefRec { count: 0, bytes: 0 });
            (logs, Reply::Ref { count: r.count, bytes: r.bytes })
        }

        // ---- 锁 ----
        Op::LockSet {
            view,
            ino,
            owner,
            verb,
            kind,
            lease_expiry,
        } => {
            if dead_view(s, *view) {
                return (vec![], no_view(*view));
            }
            if node_of(s, *view, *ino).is_none() {
                return (vec![], Reply::err(errno::ENOENT, "no such inode"));
            }
            if *verb == 1 {
                return (
                    vec![Log::DelLock {
                        view: *view,
                        ino: *ino,
                        owner: owner.clone(),
                    }],
                    Reply::Ok,
                );
            }
            (
                vec![Log::PutLock {
                    view: *view,
                    ino: *ino,
                    owner: owner.clone(),
                    rec: LockRec {
                        kind: *kind,
                        lease_expiry: *lease_expiry,
                    },
                }],
                Reply::Ok,
            )
        }

        // ---- 控制面：只读 ----
        Op::KeyGet { id } => match s.keys.get(id) {
            Some(k) => (vec![], Reply::Key(Box::new(k.clone()))),
            None => (vec![], Reply::err(errno::ENOENT, "no such key")),
        },
        Op::KeyList => (vec![], Reply::Keys(s.keys.values().cloned().collect())),
        Op::KeyCheck { id, verb, hash, now_ms } => {
            let k = match s.keys.get(id) {
                Some(k) => k,
                // 不存在与口令不符回同一个错，避免 id 枚举 oracle。
                None => return (vec![], Reply::err(errno::EADV, "bad key")),
            };
            if k.revoked() || k.expired(*now_ms) || k.secret_hash != *hash {
                return (vec![], Reply::err(errno::EADV, "bad key"));
            }
            let rep = if *verb == 1 {
                Reply::Live(true)
            } else {
                Reply::Authed {
                    label: k.label.clone(),
                    ro: k.ro,
                    view: k.view.clone(),
                    owner: k.owner.clone(),
                }
            };
            (vec![], rep)
        }
        Op::AuditList { limit } => {
            let n = if *limit == 0 { 100 } else { *limit as usize };
            let v = s
                .audit
                .values()
                .rev()
                .take(n)
                .cloned()
                .collect::<Vec<AuditRec>>();
            (vec![], Reply::Audits(v))
        }
        Op::DefGet { name } => (
            vec![],
            Reply::Def(s.defs.get(name).cloned()),
        ),
        Op::DefList { kind } => {
            let v = s
                .defs
                .values()
                .filter(|d| kind.is_empty() || &d.kind == kind)
                .cloned()
                .collect::<Vec<DefRec>>();
            (vec![], Reply::Defs(v))
        }
        Op::PlaceGet { view } => match s.placement.get(view) {
            Some(i) => (vec![], Reply::Place(i.clone())),
            None => (vec![], Reply::err(errno::ENOENT, "not placed")),
        },
        Op::PlaceList => (
            vec![],
            Reply::Places(
                s.placement
                    .iter()
                    .map(|(v, i)| (v.clone(), i.clone()))
                    .collect(),
            ),
        ),

        // ---- 通用表：只读 ----
        Op::TableGet { name } => (
            vec![],
            Reply::Table(s.tables.get(name).map(|t| t.def.clone())),
        ),
        Op::TableList => (
            vec![],
            Reply::Tables(s.tables.values().map(|t| t.def.clone()).collect()),
        ),
        Op::RowGet { table, key } => match s.tables.get(table) {
            None => (vec![], no_table(table)),
            Some(t) => (
                vec![],
                Reply::Row(s.rows.get(&t.id).and_then(|m| m.get(key)).cloned()),
            ),
        },
        Op::RowList {
            table,
            column,
            value,
            from,
            limit,
        } => {
            let Some(t) = s.tables.get(table) else {
                return (vec![], no_table(table));
            };
            if let Some(r) = bad_column(t, *column) {
                return (vec![], r);
            }
            let lim = if *limit == 0 { 1000 } else { *limit as usize };
            let (rows, end) = if *column == ROW_PK {
                s.row_page(t.id, from, lim)
            } else {
                s.index_page(t.id, *column, value, from, lim)
            };
            (vec![], Reply::Rows { rows, end })
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
            let Some(t) = s.tables.get(table) else {
                return (vec![], no_table(table));
            };
            if let Some(r) = bad_column(t, *column) {
                return (vec![], r);
            }
            let lim = if *limit == 0 { 1000 } else { *limit as usize };
            let (rows, end) = if *column == ROW_PK {
                // 主键区间只要主键游标：主键唯一，一条主键就定死了停在哪。
                s.row_range(t.id, lo, hi, after_key, lim)
            } else {
                // 索引列要连值一起给：同一个值可能跨页，只拿主键回不到「这一页
                // 停在哪一段」。空主键 = 从区间头开始（主键不得为空，不歧义）。
                let cursor = if after_key.is_empty() {
                    None
                } else {
                    Some((after_val.as_slice(), after_key.as_slice()))
                };
                s.index_range(t.id, *column, lo, hi, cursor, lim)
            };
            (vec![], Reply::Rows { rows, end })
        }
        Op::RowCount { table, column, value } => {
            let Some(t) = s.tables.get(table) else {
                return (vec![], no_table(table));
            };
            if let Some(r) = bad_column(t, *column) {
                return (vec![], r);
            }
            // 主键计数 O(1)；按索引列数 = 扫那一段，O(命中数)。没有计数器：
            // 任何「每列一个总数」都要在写侧维护，而写侧的列值本来就在动。
            let n = if *column == ROW_PK {
                s.rows.get(&t.id).map(|m| m.len()).unwrap_or(0)
            } else {
                s.index_keys(t.id, *column, value).len()
            };
            (vec![], Reply::Count(n as u64))
        }

        // ---- 控制面：写 ----
        Op::KeyCreate { key } => {
            if s.keys.contains_key(&key.id) {
                return (vec![], Reply::err(errno::EEXIST, "key id exists"));
            }
            (
                vec![Log::PutKey { key: key.clone() }],
                Reply::Key(Box::new(key.clone())),
            )
        }
        Op::KeyUpdate {
            id,
            verb,
            ts,
            new_hash,
            new_hint,
            ro,
        } => {
            let mut k = match s.keys.get(id) {
                Some(k) => k.clone(),
                None => return (vec![], Reply::err(errno::ENOENT, "no such key")),
            };
            match *verb {
                0 => k.revoked_at = *ts,
                1 => {
                    k.secret_hash = new_hash.clone();
                    k.secret_hint = new_hint.clone();
                }
                2 => k.last_used_at = *ts,
                3 => k.ro = *ro,
                x => return (vec![], Reply::err(errno::EINVAL, format!("bad key verb {x}"))),
            }
            (vec![Log::PutKey { key: k.clone() }], Reply::Key(Box::new(k)))
        }
        Op::AuditAppend { row } => {
            let mut r = row.clone();
            if r.seq == 0 {
                // 0 是「请分配」的约定值，所以自增号不能从 0 发：否则写进
                // WAL 的行与请求里的「未分配」同值，重放时会再分一次号。
                r.seq = s.next_audit_seq.max(1);
            }
            (vec![Log::PutAudit { row: r }], Reply::Ok)
        }
        Op::DefPut { def } => {
            let mut d = def.clone();
            if d.updated_at == 0 {
                d.updated_at = now;
            }
            (vec![Log::PutDef { def: d }], Reply::Ok)
        }
        Op::DefDel { name } => {
            if !s.defs.contains_key(name) {
                return (vec![], Reply::err(errno::ENOENT, "no such def"));
            }
            (vec![Log::DelDef { name: name.clone() }], Reply::Ok)
        }
        Op::PlaceSet { view, instance } => (
            vec![Log::PutPlace {
                view: view.clone(),
                instance: instance.clone(),
            }],
            Reply::Ok,
        ),

        // ---- 通用表：写 ----
        Op::TableCreate { def } => {
            if let Some(r) = bad_def(def) {
                return (vec![], r);
            }
            if s.tables.contains_key(&def.name) {
                return (
                    vec![],
                    Reply::err(
                        errno::EEXIST,
                        format!("table {} exists", String::from_utf8_lossy(&def.name)),
                    ),
                );
            }
            // id 在这里分配而不是在 `State::apply` 里：应答与日志必须拿到同一个
            // id，而 translate 对状态只读（那正是单写者流水线能成立的前提）。
            let id = s.next_table_id;
            (
                vec![Log::PutTable {
                    id,
                    def: def.clone(),
                }],
                Reply::Table(Some(def.clone())),
            )
        }
        Op::TableDrop { name } => match s.tables.get(name) {
            None => (vec![], no_table(name)),
            Some(t) => (
                vec![Log::DelTable {
                    id: t.id,
                    name: name.clone(),
                }],
                Reply::Ok,
            ),
        },
        Op::RowPut {
            table,
            key,
            cols,
            flags,
            cond_col,
            cond_val,
        } => {
            let Some(t) = s.tables.get(table) else {
                return (vec![], no_table(table));
            };
            if key.is_empty() || key.len() > ROW_KEY_MAX {
                return (
                    vec![],
                    Reply::err(errno::EINVAL, format!("row key must be 1..={ROW_KEY_MAX} bytes")),
                );
            }
            if cols.len() != t.def.columns.len() {
                return (
                    vec![],
                    Reply::err(
                        errno::EINVAL,
                        format!(
                            "row has {} columns, table {} has {}",
                            cols.len(),
                            String::from_utf8_lossy(&t.def.name),
                            t.def.columns.len()
                        ),
                    ),
                );
            }
            if *flags & (PUT_IF_ABSENT | PUT_IF_EXISTS) == (PUT_IF_ABSENT | PUT_IF_EXISTS) {
                return (
                    vec![],
                    Reply::err(errno::EINVAL, "PUT_IF_ABSENT and PUT_IF_EXISTS contradict"),
                );
            }
            if *cond_col != COND_NONE && t.def.columns.get(*cond_col as usize).is_none() {
                return (
                    vec![],
                    Reply::err(errno::EINVAL, format!("condition names no such column {cond_col}")),
                );
            }
            let old = s.rows.get(&t.id).and_then(|m| m.get(key));
            if *flags & PUT_IF_ABSENT != 0 && old.is_some() {
                return (vec![], Reply::err(errno::EEXIST, "row exists"));
            }
            if *flags & PUT_IF_EXISTS != 0 && old.is_none() {
                return (vec![], Reply::err(errno::ENOENT, "no such row"));
            }
            if let Some(r) = cond_miss(old, *cond_col, cond_val) {
                return (vec![], r);
            }
            // 唯一约束：撞值的必须是这一行自己（覆盖写不算撞）。不看 `cols` 而
            // 先看索引：索引上「相同值的不同主键」就是要拒的那种碰撞。
            for (i, c) in t.def.columns.iter().enumerate() {
                if !c.unique() {
                    continue;
                }
                for k in s.index_keys(t.id, i as u32, &cols[i]) {
                    if k != *key {
                        return (
                            vec![],
                            Reply::err(
                                errno::EEXIST,
                                format!(
                                    "column {} must stay unique",
                                    String::from_utf8_lossy(&c.name)
                                ),
                            ),
                        );
                    }
                }
            }
            let row = RowRec {
                key: key.clone(),
                cols: cols.clone(),
            };
            (
                vec![Log::PutRow {
                    table: t.id,
                    row: row.clone(),
                }],
                Reply::Row(Some(row)),
            )
        }
        Op::RowDelete {
            table,
            key,
            flags,
            cond_col,
            cond_val,
        } => {
            let Some(t) = s.tables.get(table) else {
                return (vec![], no_table(table));
            };
            if *cond_col != COND_NONE && t.def.columns.get(*cond_col as usize).is_none() {
                return (
                    vec![],
                    Reply::err(errno::EINVAL, format!("condition names no such column {cond_col}")),
                );
            }
            let old = s.rows.get(&t.id).and_then(|m| m.get(key));
            if old.is_none() {
                // 幂等是默认：客户端重发一个已经成功的删除不该看到错误。
                if *flags & DEL_IF_EXISTS != 0 {
                    return (vec![], Reply::err(errno::ENOENT, "no such row"));
                }
                return (vec![], Reply::Ok);
            }
            if let Some(r) = cond_miss(old, *cond_col, cond_val) {
                return (vec![], r);
            }
            (
                vec![Log::DelRow {
                    table: t.id,
                    key: key.clone(),
                }],
                Reply::Ok,
            )
        }

        // ---- 跨 op 的原子批 ----
        Op::Batch { ops } => batch(s, now, ops),

        // 由 Engine 直接应答的控制 op 不落到这里。
        Op::Ping
        | Op::Stats
        | Op::Checkpoint
        | Op::Shutdown
        | Op::WalFetch { .. }
        | Op::SnapFetch { .. } => (vec![], Reply::Pong),

        // 握手 op 在事件循环里就被处置掉了（§4.4）：它既不读状态也不写状态，
        // 进引擎没有任何意义。落到这里说明分派漏了一层 —— 大声报错而不是
        // 像上面那组一样回个看上去正常的 Pong。
        Op::AuthBegin { .. } | Op::AuthFinish { .. } => (
            vec![],
            Reply::err(errno::EINVAL, "handshake op reached the engine (§4.4)"),
        ),
    }
}
