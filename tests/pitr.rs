//! WAL 归档 + 时间点恢复（PITR）验收，DESIGN §6.11。
//!
//! 六条主张，一条一个用例：
//!
//! 1. **归档布局与字节**：每次 checkpoint 在归档目录留下 `wal-<from>-<to>.wal`
//!    （与源 WAL 的前缀**逐字节相同**，不是重新编码）与 `snap-<from>.snap`；
//!    空目录的第一次 checkpoint 没有快照可归档，这是合法的。
//! 2. **回到中间点**：目标落在两次 checkpoint 之间时，锚点是归档里的旧快照、
//!    记录来自归档段；目标落在最后一次 checkpoint 之后时，锚点是数据目录的
//!    `state.snap`、记录来自活 WAL。两条路径重放出来的状态都与当时的内存状态
//!    逐字段相等，且恢复出来的目录能继续写。
//! 3. **缺口不拼接**：中途关掉归档跑过几次 checkpoint，归档里就有一个洞；
//!    越过它必须报缺口并指出缺的是哪一条，而不是把两边的历史接起来。
//! 4. **越过末尾**：目标比现有历史更远时报「history ends at」，不产出半份快照。
//! 5. **目标非空**：`--out` 里有东西就拒，一个字节不动（与 `treedb-load` 同口径）。
//! 6. **归档目录不能是数据目录**：开局就拒，理由是两个布局会互相污染。
//! 7. **批量形状的三条日志也走这条路**（v0.7.0 的 `Log` 25/26/27）：它们要能从
//!    归档段里重放出来（恢复中间点时 `from_wal` 必须为 0 —— 走活 WAL 就测不到
//!    归档），且等价判据仍是与冷 dump `cmp` 逐字节相同，不看行数。
//!
//! 跑法：`cargo test --release --test pitr`

use std::path::PathBuf;
use tempfile::TempDir;
use treedb::engine::core::WAL_FILE;
use treedb::engine::model::{ColumnDef, TableDef, COL_INDEX, COND_NONE, ROW_PK};
use treedb::engine::{dump_dir, pitr_restore, plan_pitr, Archive, Durability, Engine, Op, Reply, Wal};

// ---------------------------------------------------------------- 驱动脚手架

struct Fx {
    tmp: TempDir,
}

impl Fx {
    fn new(tag: &str) -> Fx {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(tmp.path().join(tag)).unwrap();
        Fx { tmp }
    }
    fn dir(&self) -> PathBuf {
        self.tmp.path().join("data")
    }
    fn archive(&self) -> PathBuf {
        self.tmp.path().join("archive")
    }
    fn out(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }
    /// 开归档的引擎（测试里永远用 Always：`checkpoint` 的水位就是已 fsync 的水位）。
    fn open(&self) -> Engine {
        Engine::open_with_archive(&self.dir(), Durability::Always, Some(self.archive()))
            .expect("engine open with archive")
    }
    /// 关归档的引擎：模拟「归档开关被关掉跑了一段」。
    fn open_plain(&self) -> Engine {
        Engine::open(&self.dir(), Durability::Always).expect("engine open")
    }
}

fn ok(e: &mut Engine, op: &Op) -> Reply {
    match e.submit(op).unwrap_or_else(|err| panic!("{op:?} → {err}")) {
        Reply::Err { code, msg } => panic!(
            "{op:?} → unexpected errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        r => r,
    }
}

/// 建一个视图，回 (view id, root ino)。
fn mk_view(e: &mut Engine, name: &str) -> (u32, u64) {
    match ok(e, &Op::CreateView { name: name.into() }) {
        Reply::View { id, root } => (id, root),
        other => panic!("CreateView → {other:?}"),
    }
}

/// 一个 `Create` 正好一条日志，于是「写一个文件 = 水位 +1」，用例里可以推。
fn mk_file(e: &mut Engine, view: u32, parent: u64, name: &str) -> u64 {
    match ok(
        e,
        &Op::Create {
            view,
            parent,
            name: name.into(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ) {
        Reply::Node(n) => n.ino,
        other => panic!("Create → {other:?}"),
    }
}

fn seqs(recs: &[(u64, treedb::engine::Log)]) -> Vec<u64> {
    recs.iter().map(|(s, _)| *s).collect()
}

// ---------------------------------------------------------------- 1. 归档布局

#[test]
fn checkpoint_archives_the_frames_and_the_outgoing_snapshot() {
    let fx = Fx::new("arch-layout");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    for i in 0..3 {
        mk_file(&mut e, view, root, &format!("f{i}"));
    }
    // 第一次 checkpoint：源目录还没有 `state.snap`（空状态不需要文件代表）。
    let wal_before = std::fs::read(fx.dir().join(WAL_FILE)).unwrap();
    let wrote = e.apply_seq() - 1;
    let s1 = e.checkpoint().unwrap();
    assert_eq!(s1, wrote, "checkpoint 的水位就是最后一个已 fsync 的 seq");

    let ar = Archive::new(fx.archive());
    let segs = ar.segments().unwrap();
    assert_eq!(segs.len(), 1, "第一次 checkpoint 只产一段");
    assert_eq!((segs[0].0, segs[0].1), (0, s1));
    assert_eq!(
        segs[0].2.file_name().unwrap().to_str().unwrap(),
        format!("wal-{:020}-{:020}.wal", 0, s1),
        "名字是定宽 20 位的 wal-<from>-<to>.wal"
    );
    assert!(
        ar.snapshots().unwrap().is_empty(),
        "空目录的第一次 checkpoint 没有快照可归档"
    );

    // 段与源 WAL 的前缀逐字节相同：归档搬的是帧，不是重新编码的记录。
    let seg_bytes = std::fs::read(&segs[0].2).unwrap();
    assert!(wal_before.starts_with(&seg_bytes), "归档段必须是源 WAL 的前缀");
    assert!(
        wal_before[seg_bytes.len()..].iter().all(|b| *b == 0),
        "前缀之后应当只剩预分配空洞"
    );
    assert_eq!(
        seqs(&Wal::read_records(&segs[0].2).unwrap()),
        (1..=s1).collect::<Vec<_>>()
    );

    // 再写两条，第二次 checkpoint：多一段 + 一份水位 s1 的快照。
    mk_file(&mut e, view, root, "g0");
    mk_file(&mut e, view, root, "g1");
    let s2 = e.checkpoint().unwrap();
    assert_eq!(s2, s1 + 2 * 3, "每个 Create 三条记录");
    let segs = ar.segments().unwrap();
    assert_eq!(segs.len(), 2);
    assert_eq!((segs[1].0, segs[1].1), (s1, s2));
    assert_eq!(
        segs[1].2.file_name().unwrap().to_str().unwrap(),
        format!("wal-{s1:020}-{s2:020}.wal")
    );
    let snaps = ar.snapshots().unwrap();
    assert_eq!(snaps.len(), 1, "只归档「要被覆盖掉的那一份」快照");
    assert_eq!(snaps[0].0, s1);
    assert_eq!(
        snaps[0].1.file_name().unwrap().to_str().unwrap(),
        format!("snap-{s1:020}.snap")
    );
    assert_eq!(
        seqs(&Wal::read_records(&segs[1].2).unwrap()),
        (s1 + 1..=s2).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------- 1b. 后台快照

/// 自动档（旁路快照）也归档，而且必须是「先拷旧快照再覆盖」——那条线程自己写
/// `state.snap`，顺序错了就会把水位 `from` 那份字节永远抹掉。
#[test]
fn background_checkpoint_archives_before_overwriting_the_snapshot() {
    let fx = Fx::new("arch-bg");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let s1 = e.checkpoint().unwrap();
    mk_file(&mut e, view, root, "b");
    let to = e.apply_seq() - 1;
    let want = e.state_snapshot();

    assert!(e.checkpoint_start().unwrap(), "新水位在前，应当真派出去");
    e.checkpoint_wait().unwrap();
    assert_eq!(e.snap_seq(), to);

    let ar = Archive::new(fx.archive());
    let snaps: Vec<u64> = ar.snapshots().unwrap().into_iter().map(|(s, _)| s).collect();
    assert_eq!(snaps, vec![s1], "归档里必须多出水位 s1 那一份");
    let segs = ar.segments().unwrap();
    let last = segs.last().unwrap();
    assert_eq!((last.0, last.1), (s1, to), "后台线程拷的是 (snap_seq, upto]");

    // 归档的那一份真能当锚点用：回到 s1 零条重放。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), s1).unwrap();
    assert_eq!(plan.anchor, s1);
    assert_eq!(plan.records(), 0);
    drop(e);
    let out = fx.out("from-bg");
    pitr_restore(plan, &out).unwrap();
    let r = Engine::open(&out, Durability::Always).unwrap();
    assert_eq!(r.snap_seq(), s1);

    // 后台快照做完之后的水位同样可恢复（锚点换成数据目录那份快照）。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), to).unwrap();
    assert_eq!(plan.state(), &want);
}

// ---------------------------------------------------------------- 2. 回到中间点
#[test]
fn pitr_restores_the_state_at_the_target_seq() {
    let fx = Fx::new("pitr-mid");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let s1 = e.checkpoint().unwrap(); // = 2，归档里有 (0, 2]、没有快照

    mk_file(&mut e, view, root, "b");
    let mid = e.apply_seq() - 1; // 落在两次 checkpoint 之间
    let want_mid = e.state_snapshot();

    mk_file(&mut e, view, root, "c");
    let s2 = e.checkpoint().unwrap(); // = 4，归档里多 (2, 4] 与 snap-2
    mk_file(&mut e, view, root, "d");
    let want_tail = e.state_snapshot();
    let tail = e.apply_seq() - 1; // 落在最后一次 checkpoint 之后，只有活 WAL 有
    drop(e);

    // (a) 中间点：锚点取自归档快照，记录取自归档段。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), mid).unwrap();
    assert_eq!(plan.anchor, s1, "锚点应是归档里的 snap-{s1}");
    assert_eq!((plan.from_archive, plan.from_wal), (mid - s1, 0));
    assert_eq!(plan.segments, 1);
    assert_eq!(plan.state(), &want_mid);
    let out = fx.out("restored-mid");
    pitr_restore(plan, &out).unwrap();

    let mut r = Engine::open(&out, Durability::Always).unwrap();
    assert_eq!(r.snap_seq(), mid, "恢复出来的目录水位就是 --to-seq");
    assert_eq!(r.state_snapshot(), want_mid, "状态必须逐字段相等");
    // 恢复出来的目录能继续用：视图还在，写接着走。
    let ino = mk_file(&mut r, view, root, "e");
    assert!(ino >= 2);
    assert_eq!(r.snap_seq(), mid);

    // (b) 尾巴：锚点取自数据目录的 state.snap（水位 s2），记录取自活 WAL。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), tail).unwrap();
    assert_eq!(plan.anchor, s2);
    assert_eq!((plan.from_archive, plan.from_wal), (0, tail - s2));
    assert_eq!(plan.state(), &want_tail);
    let out = fx.out("restored-tail");
    pitr_restore(plan, &out).unwrap();
    let r = Engine::open(&out, Durability::Always).unwrap();
    assert_eq!(r.snap_seq(), tail);
    assert_eq!(r.state_snapshot(), want_tail);

    // (c) 目标正好落在某个快照水位上：零条重放也是合法的一次恢复。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), s2).unwrap();
    assert_eq!(plan.anchor, s2);
    assert_eq!(plan.records(), 0);
    let out = fx.out("restored-exact");
    pitr_restore(plan, &out).unwrap();
    let r = Engine::open(&out, Durability::Always).unwrap();
    assert_eq!(r.snap_seq(), s2);
}

// ---------------------------------------------------------------- 3. 缺口

/// 中途关掉归档跑了一段：那几次 checkpoint 的记录只在快照里活过一次，
/// 归档完全不知道它们存在。此后越过那个洞的请求必须停在洞的左边。
#[test]
fn pitr_stops_where_the_archive_was_turned_off() {
    let fx = Fx::new("pitr-gap");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let s1 = e.checkpoint().unwrap(); // 归档只有 (0, s1]
    drop(e);

    // 关掉归档跑一段：这两次 checkpoint 的记录只进了当时的数据目录快照，
    // 后来的 checkpoint 一覆盖，那两份快照就没了。
    let mut e = fx.open_plain();
    mk_file(&mut e, view, root, "b");
    let s2 = e.checkpoint().unwrap();
    mk_file(&mut e, view, root, "c");
    let s3 = e.checkpoint().unwrap();
    drop(e);

    // 再开归档：从这里往后又有了（而且它会归档水位 s3 的那份快照）。
    let mut e = fx.open();
    mk_file(&mut e, view, root, "d");
    let s4 = e.checkpoint().unwrap();
    drop(e);
    assert!(s1 < s2 && s2 < s3 && s3 < s4, "{s1} {s2} {s3} {s4}");

    // 到 s1 为止仍然可恢复（锚点是空状态，记录全在归档的第一段里）。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), s1).unwrap();
    assert_eq!(plan.anchor, 0);
    assert_eq!(plan.records(), s1);

    // 洞中间：历史到 s1 为止就断了，不拿后面的历史冒充连续。
    let err = plan_pitr(&fx.dir(), &fx.archive(), s1 + 1)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!("history ends at {s1}")),
        "报错要说清历史到哪儿为止，不能只说「不行」：{err}"
    );

    // 洞的另一边（s3 之后）有 s3 那份快照做锚点，于是又能恢复了 ——
    // 这是快照把洞补上的合法情形，不是「把两边的历史接起来」。
    // 目标取 s4 - 1：s4 那份快照在数据目录里，恰好会把这条路径掩盖掉。
    let plan = plan_pitr(&fx.dir(), &fx.archive(), s4 - 1).unwrap();
    assert_eq!(plan.anchor, s3);
    assert_eq!(plan.records(), s4 - 1 - s3);
}

/// 归档里丢了一段（快照副本与那一段段文件一起没了）：必须在缺口处报错并指出
/// 下一个实际存在的 seq，而不是跳过它继续重放 —— 那会悄悄产出另一份历史。
#[test]
fn pitr_reports_a_gap_when_a_record_is_missing() {
    let fx = Fx::new("pitr-lost");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let s1 = e.checkpoint().unwrap();
    mk_file(&mut e, view, root, "b");
    let s2 = e.checkpoint().unwrap();
    mk_file(&mut e, view, root, "c");
    let s3 = e.checkpoint().unwrap();
    mk_file(&mut e, view, root, "d");
    let s4 = e.checkpoint().unwrap();
    drop(e);
    assert!(s1 < s2 && s2 < s3 && s3 < s4, "{s1} {s2} {s3} {s4}");

    // 模拟「归档坏了」：快照副本全丢，中间那一段也丢。于是锚点只能退到 0，
    // 而 (s1, s2] 这段记录谁都没有 —— 洞里后面却还有 s2 + 1 以后的记录。
    let ar = Archive::new(fx.archive());
    let snaps = ar.snapshots().unwrap();
    assert!(!snaps.is_empty(), "至少归档过一份快照");
    for (_, p) in &snaps {
        std::fs::remove_file(p).unwrap();
    }
    let segs = ar.segments().unwrap();
    let victim = segs
        .iter()
        .find(|(from, to, _)| *from == s1 && *to == s2)
        .map(|(_, _, p)| p.clone())
        .expect("要删的就是 (s1, s2] 这一段");
    std::fs::remove_file(&victim).unwrap();

    // 目标 s3 < s4，所以数据目录最新的那份快照（s4）也当不了锚点。
    let err = plan_pitr(&fx.dir(), &fx.archive(), s3)
        .unwrap_err()
        .to_string();
    assert!(err.contains(&format!("no record {}", s1 + 1)), "{err}");
    assert!(
        err.contains(&format!("next one present is {}", s2 + 1)),
        "报错要指出下一个实际存在的 seq：{err}"
    );
}

// ---------------------------------------------------------------- 4. 越过末尾

#[test]
fn pitr_rejects_a_point_past_the_end() {
    let fx = Fx::new("pitr-past");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let s1 = e.checkpoint().unwrap();
    drop(e);

    let err = plan_pitr(&fx.dir(), &fx.archive(), s1 + 100)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!("history ends at {s1}")),
        "报错要指出历史到哪儿为止：{err}"
    );
}

// ---------------------------------------------------------------- 5. 目标非空

#[test]
fn pitr_refuses_to_write_into_a_non_empty_directory() {
    let fx = Fx::new("pitr-nonempty");
    let mut e = fx.open();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let s1 = e.checkpoint().unwrap();
    drop(e);

    let out = fx.out("target");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("keep.me"), b"x").unwrap();

    let plan = plan_pitr(&fx.dir(), &fx.archive(), s1).unwrap();
    let err = pitr_restore(plan, &out).unwrap_err().to_string();
    assert!(err.contains("refusing to restore into"), "{err}");
    assert_eq!(
        std::fs::read(out.join("keep.me")).unwrap(),
        b"x",
        "拒绝发生在写之前，目标目录必须原样未动"
    );
    // 只允许多出本工具自己的 `.lock`：它是拿目录锁时的正常副产物。
    let extra: Vec<String> = std::fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n != ".lock")
        .collect();
    assert_eq!(extra, vec!["keep.me".to_string()], "不能多出任何数据文件");
}

// ---------------------------------------------------------------- 6. 归档目录

#[test]
fn archive_dir_must_not_be_the_data_dir() {
    let fx = Fx::new("arch-same");
    let err = match Engine::open_with_archive(&fx.dir(), Durability::Always, Some(fx.dir())) {
        Ok(_) => panic!("归档目录等于数据目录必须被拒"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("is the data directory"), "{err}");
}

// ---------------------------------------------------------------- 7. 不开归档

#[test]
fn without_archive_nothing_appears_next_to_the_data_dir() {
    let fx = Fx::new("arch-off");
    let mut e = Engine::open(&fx.dir(), Durability::Always).unwrap();
    let (view, root) = mk_view(&mut e, "v1");
    mk_file(&mut e, view, root, "a");
    let wrote = e.apply_seq() - 1;
    let s = e.checkpoint().unwrap();
    assert_eq!(s, wrote);
    assert!(e.archive_dir().is_none());
    // 数据目录里只有快照 + WAL + `.lock`：归档从不「顺手」写到别处。
    let mut names: Vec<String> = std::fs::read_dir(fx.dir())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, vec![".lock", "state.snap", "treedb.wal"]);
    assert!(!fx.archive().exists(), "没开归档就不该建归档目录");
}

// ------------------------------------------------ 7. 批量形状的三条日志

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn tb(name: &str, flags: u32) -> ColumnDef {
    ColumnDef {
        name: name.as_bytes().to_vec(),
        flags,
    }
}

/// 两张索引列的表：`TableClear` / `RowRangeDelete` 删的都是带索引的列，
/// 归档侧重放要连索引一起摘干净。
fn mk_table(e: &mut Engine, name: &str) {
    ok(
        e,
        &Op::TableCreate {
            def: TableDef {
                name: name.as_bytes().to_vec(),
                columns: vec![tb("a", COL_INDEX), tb("b", COL_INDEX)],
            },
        },
    );
}

fn put_row(e: &mut Engine, table: &str, key: &str) {
    ok(
        e,
        &Op::RowPut {
            table: table.as_bytes().to_vec(),
            key: key.as_bytes().to_vec(),
            cols: vec![key.as_bytes().to_vec(), b"v".to_vec()],
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    );
}

/// 主键序全表遍历（`column == ROW_PK` 时 `value` 忽略）。
fn keys(e: &mut Engine, table: &str) -> Vec<String> {
    match ok(
        e,
        &Op::RowList {
            table: table.as_bytes().to_vec(),
            column: ROW_PK,
            value: Vec::new(),
            from: Vec::new(),
            limit: 0,
        },
    ) {
        Reply::Rows { rows, .. } => rows
            .iter()
            .map(|r| String::from_utf8_lossy(&r.key).into_owned())
            .collect(),
        other => panic!("RowList → {other:?}"),
    }
}

/// 三条批量 op 各自只产**一条**日志 —— 这正是「一条记录不等于常数时间」那条
/// 边界（DESIGN §8 第 18 条）在归档侧的样子：恢复仍旧逐行 apply，所以归档/重放
/// 这条路径必须把 25/26/27 一起带上，而不是只支持行级 op。
#[test]
fn bulk_logs_replay_from_the_archive_segment() {
    let fx = Fx::new("pitr-bulk");
    let mut e = fx.open();
    mk_table(&mut e, "k");
    for k in ["k1", "k2", "k3", "k4"] {
        put_row(&mut e, "k", k);
    }
    let s1 = e.checkpoint().unwrap(); // (s1 之前) 的记录已归档，与下面三段无关

    ok(&mut e, &Op::TableClear { table: b("k") });
    let q_clear = e.apply_seq() - 1;
    let want_clear = e.state_snapshot();

    for k in ["k1", "k2", "k3", "k4"] {
        put_row(&mut e, "k", k); // 清空之后再灌一次，让范围删有东西可删
    }
    ok(
        &mut e,
        &Op::RowRangeDelete {
            table: b("k"),
            column: ROW_PK,
            lo: b("k2"),
            hi: b("k4"),
            max: 0,
        },
    );
    let q_range = e.apply_seq() - 1;
    let want_range = e.state_snapshot();

    ok(
        &mut e,
        &Op::TableRename {
            from: b("k"),
            to: b("k2"),
            flags: 0,
        },
    );
    let q_rename = e.apply_seq() - 1;
    let want_rename = e.state_snapshot();
    // checkpoint 的水位就是它覆盖的最后一条，所以要多写一行把它推到 q_rename 右边：
    // 三个点必须**严格落在两次 checkpoint 之间**，否则恢复会从锚点快照直取，
    // 归档段根本没被读。
    put_row(&mut e, "k2", "k9");
    let s2 = e.checkpoint().unwrap(); // 把 (s1, s2] 连 snap-s1 一起归档
    assert!(q_clear < s2 && q_rename < s2, "三个点都要落在两次 checkpoint 之间");
    drop(e);

    for (q, want, tag, tname, live) in [
        (q_clear, want_clear, "TableClear", "k", vec![]),
        (q_range, want_range, "RowRangeDelete", "k", vec!["k1", "k4"]),
        (q_rename, want_rename, "TableRename", "k2", vec!["k1", "k4"]),
    ] {
        let plan = plan_pitr(&fx.dir(), &fx.archive(), q).unwrap();
        assert_eq!(plan.anchor, s1, "{tag}: 锚点应是归档里的 snap-{s1}");
        assert_eq!(
            plan.from_wal, 0,
            "{tag}: 记录必须来自归档段，从活 WAL 取就没测到归档"
        );
        assert!(
            plan.from_archive > 0,
            "{tag}: 归档段里至少要有一条记录（同值消除不该把它省掉）"
        );
        assert_eq!(plan.state(), &want, "{tag}: 重放出的状态与当时内存状态不等");

        let out = fx.out(&format!("restored-{tag}"));
        pitr_restore(plan, &out).unwrap();
        let mut r = Engine::open(&out, Durability::Always).unwrap();
        assert_eq!(r.snap_seq(), q, "{tag}: 恢复目录的水位");
        assert_eq!(r.state_snapshot(), want, "{tag}: 恢复目录的状态");
        assert_eq!(keys(&mut r, tname), live, "{tag}: 恢复后表 {tname} 的行");
    }
}

/// §6.11 的等价判据不是「能起来」而是「与停服 dump 逐字节相同」。这条把
/// 三条批量日志全放进一次恢复的射程里（尾巴那形：锚点取数据目录快照、
/// 记录取活 WAL），拿 `cmp` 尺寸的判据钉住 —— 只看视图数 / 行数会放过
/// 字段级偏差（比如索引列或表的 `created_at` 没被日志带上）。
#[test]
fn bulk_pitr_restore_matches_a_cold_dump_byte_for_byte() {
    let fx = Fx::new("pitr-bulk-dump");
    let mut e = fx.open();
    mk_table(&mut e, "k");
    for k in ["a", "b", "c", "d", "e"] {
        put_row(&mut e, "k", k);
    }
    e.checkpoint().unwrap();
    ok(
        &mut e,
        &Op::RowRangeDelete {
            table: b("k"),
            column: ROW_PK,
            lo: b("b"),
            hi: b("e"),
            max: 0,
        },
    );
    ok(&mut e, &Op::TableClear { table: b("k") });
    mk_table(&mut e, "z");
    put_row(&mut e, "z", "q");
    ok(
        &mut e,
        &Op::TableRename {
            from: b("z"),
            to: b("y"),
            flags: 0,
        },
    );
    let tail = e.apply_seq() - 1;
    e.checkpoint().unwrap(); // 让源目录的 state.snap 正好落在 tail
    drop(e);

    let src = dump_dir(&fx.dir()).unwrap().encode();
    let plan = plan_pitr(&fx.dir(), &fx.archive(), tail).unwrap();
    assert_eq!(plan.from_archive, 0, "尾巴那形应只靠活 WAL");
    let out = fx.out("restored-dump");
    pitr_restore(plan, &out).unwrap();
    let got = dump_dir(&out).unwrap().encode();
    assert_eq!(
        src,
        got,
        "PITR 恢复到 {tail} 与停服冷 dump 不是逐字节相同（{} vs {} 字节）",
        src.len(),
        got.len()
    );
}
