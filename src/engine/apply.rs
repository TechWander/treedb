//! `Op` → `(Vec<Log>, Reply)` 的纯翻译层。
//!
//! 这是引擎唯一的 POSIX 语义所在地：读 op 直接查 [`State`] 出应答，
//! 写 op 先算出「提交后的行应该长什么样」（logs）并预生成应答，随后
//! 由 [`crate::engine::core::Engine`] 走 WAL → apply。翻译层对状态
//! **只读**，所以失败路径天然无需回滚——这是单写者流水线能成立的前提。
//!
//! 约定：`logs.is_empty()` 等价于「本 op 是只读的」。

use crate::engine::mem::{Log, State};
use std::collections::BTreeMap;
use std::ops::Bound;
use crate::engine::model::{
    errno, AuditRec, DataRef, DefRec, Ino, LockRec, Node, RefRec, Reply, ViewId, ViewMeta, Kind,
    Op, RENAME_NOREPLACE, SA_ATIME, SA_DATA_REF, SA_GID, SA_MODE, SA_MTIME, SA_SIZE, SA_UID,
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

fn view_meta(s: &State, view: ViewId) -> Option<(Vec<u8>, ViewMeta)> {
    let name = s.view_by_id.get(&view)?.clone();
    let meta = s.views.get(&name)?.clone();
    Some((name, meta))
}

fn no_view(view: ViewId) -> Reply {
    Reply::err(errno::ENODEV, format!("no such view id {view}"))
}

/// 视图级前置检查：视图整个没了就回 ENODEV，别让 FUSE 把「视图被删」
/// 误判成「这个 inode 查不中」而只失效一个 dentry。
fn dead_view(s: &State, view: ViewId) -> bool {
    view_meta(s, view).is_none()
}

fn node_of(s: &State, view: ViewId, ino: Ino) -> Option<Node> {
    s.nodes.get(&(view, ino)).cloned()
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
            let ino = match s.dentries.get(&(*view, *parent, name.clone())) {
                Some(i) => *i,
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
            // offset 是「已返回的条目数」：直接在 BTreeMap 迭代器上 skip，
            // 不先把整目录物化（千级目录也不能每页拷一份）。
            let lim = if *limit == 0 { 1000 } else { *limit as usize };
            let off = *offset as usize;
            let mut inos = Vec::with_capacity(lim.min(4096));
            let mut kinds = Vec::with_capacity(lim.min(4096));
            let mut names = Vec::with_capacity(lim.min(4096));
            let mut it = s
                .dentries
                .range((
                    Bound::Included((*view, *parent, Vec::new())),
                    Bound::Unbounded,
                ))
                .take_while(move |x| x.0 .0 == *view && x.0 .1 == *parent)
                .skip(off);
            for (k, ino) in it.by_ref().take(lim) {
                let kind = node_of(s, *view, *ino).map(|x| x.kind.code()).unwrap_or(0);
                inos.push(*ino);
                kinds.push(kind);
                names.push(k.2.clone());
            }
            let next = (off + inos.len()) as u64;
            // `end` 要说真话：整页恰好装满时再探一项就知道后面还有没有，
            // 不能让调用方多跑一页空 readdir 才发现到底（O(1) 换语义确定）。
            let end = it.next().is_none();
            (vec![], Reply::Dirents { inos, kinds, names, next, end })
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
            // Create/Mkdir 遇同名 EEXIST；Symlink 按 POSIX 覆盖旧项。
            if let Some(old_ino) = s.dentries.get(&(*view, *parent, name.clone())) {
                if kind != Kind::Symlink {
                    return (vec![], Reply::err(errno::EEXIST, "file exists"));
                }
                let old = match node_of(s, *view, *old_ino) {
                    Some(o) => o,
                    None => return (vec![], Reply::err(errno::ESTALE, "dangling dentry")),
                };
                if old.kind == Kind::Dir {
                    return (
                        vec![],
                        Reply::err(
                            if dir_empty(s, *view, *old_ino) {
                                errno::EISDIR
                            } else {
                                errno::ENOTEMPTY
                            },
                            "target is a directory",
                        ),
                    );
                }
                logs.push(Log::DelNode {
                    view: *view,
                    ino: *old_ino,
                });
                if !old.data_ref.is_empty() {
                    book.down(&old.data_ref, old.size);
                }
            }
            let parent_node = match node_of(s, *view, *parent) {
                Some(p) if p.kind == Kind::Dir => p,
                Some(_) => return (vec![], Reply::err(errno::ENOTDIR, "parent is not a dir")),
                None => return (vec![], Reply::err(errno::ENOENT, "no such parent")),
            };
            let (ino, bump) = alloc_ino(&vname, &meta);
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
        Op::Unlink { view, parent, name } | Op::Rmdir { view, parent, name } => {
            let want_dir = matches!(op, Op::Rmdir { .. });
            let (_, meta) = match view_meta(s, *view) {
                Some(x) => x,
                None => return (vec![], no_view(*view)),
            };
            if let Some(r) = name_err(name) {
                return (vec![], r);
            }
            let ino = match s.dentries.get(&(*view, *parent, name.clone())) {
                Some(i) => *i,
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
            let mut logs = vec![
                Log::DelDentry {
                    view: *view,
                    parent: *parent,
                    name: name.clone(),
                },
                Log::DelNode {
                    view: *view,
                    ino,
                },
            ];
            // 唯一一条链接断开 = 内容对象少一次引用（grovefs 的 data_ref
            // 由 inode 持有，没有额外硬链接，故 unlink 必然减引用）。
            let mut book = RefBook::default();
            if !node.data_ref.is_empty() {
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
            let ino = match s.dentries.get(&(*view, *old_parent, old_name.clone())) {
                Some(i) => *i,
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
            if let Some(tino) = s.dentries.get(&(*view, *new_parent, new_name.clone())) {
                if noreplace {
                    return (vec![], Reply::err(errno::EEXIST, "target exists and NOREPLACE"));
                }
                let t = match node_of(s, *view, *tino) {
                    Some(t) => t,
                    None => return (vec![], Reply::err(errno::ESTALE, "dangling target")),
                };
                if t.kind == Kind::Dir && node.kind != Kind::Dir {
                    return (vec![], Reply::err(errno::ENOTDIR, "target is a dir"));
                }
                if t.kind != Kind::Dir && node.kind == Kind::Dir {
                    return (vec![], Reply::err(errno::EISDIR, "target is not a dir"));
                }
                if t.kind == Kind::Dir && !dir_empty(s, *view, *tino) {
                    return (vec![], Reply::err(errno::ENOTEMPTY, "target dir not empty"));
                }
                if t.ino == meta.root {
                    return (vec![], Reply::err(errno::EBUSY, "target is view root"));
                }
                logs.push(Log::DelNode {
                    view: *view,
                    ino: *tino,
                });
                if !t.data_ref.is_empty() {
                    book.down(&t.data_ref, t.size);
                    over = t.data_ref.clone();
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

        // 由 Engine 直接应答的控制 op 不落到这里。
        Op::Ping | Op::Stats | Op::Checkpoint | Op::Shutdown => (vec![], Reply::Pong),
    }
}
