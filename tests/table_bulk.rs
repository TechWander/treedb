//! 通用表「批量形状」（`Op::TableClear` / `Op::RowRangeDelete` /
//! `Op::TableRename`，DESIGN §5.1）加上 `Op::RowPut` 的同值消除的验收。
//!
//! 这一组要钉死六件事：
//!
//! 1. **一条记录**：清空、区间删、改名都只推进一个 seq。整个 op 存在的理由就是
//!    「把 N 条 op 压成一条」，压不成就是白做，所以每条用例都先量 seq。
//! 2. **区间删的 WAL 体积与命中数无关**：日志里存的是区间而不是键清单。这条用
//!    `log_payload` 直接比字节数 —— 不比时间，因为时间在这台机器上不可复现。
//! 3. **索引不留孤儿**：`row_index` 必须以 `rows` 为准同步摘掉。漏一处就等于
//!    永久泄漏（统计、快照、按值列举都会跟着多报）。
//! 4. **重放幂等 + 切片原子**：同一条日志 apply 两次等价于一次；把记录从
//!    payload 中间截断，重开后要么整个效果在、要么完全没发生。
//! 5. **改名不换 id，替换目标要级联**：影子表切换的全部价值在于「id 不变、
//!    行不用重灌」；而被顶掉的那张表如果留下行，就成了一张没有任何名字能
//!    指向它的孤儿表。
//! 6. **同值消除只在真的没变化时生效**：条件位（`PUT_IF_ABSENT` /
//!    `PUT_IF_EXISTS` / CAS）必须照旧拦住，新键必须照旧落盘 —— 省掉的只能是
//!    「写了也一样」的那一次。
//!
//! 跑法：`cargo test --test table_bulk`

use std::path::PathBuf;
use tempfile::TempDir;
use treedb::engine::core::WAL_FILE;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, RowRec, TableDef, COL_INDEX, COL_UNIQUE, COND_NONE, PUT_IF_ABSENT,
    RENAME_NOREPLACE, ROW_PK, TABLE_NAME_MAX,
};
use treedb::engine::wal::log_payload;
use treedb::engine::{dump_dir, restore_dump, Durability, Engine, Log, State};

// ---------------------------------------------------------------- 驱动脚手架

struct Dir {
    tmp: TempDir,
}

impl Dir {
    fn new(tag: &str) -> Dir {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(tmp.path().join(tag)).unwrap();
        Dir { tmp }
    }
    fn path(&self) -> PathBuf {
        self.tmp.path().join("treedb")
    }
    fn open(&self) -> Engine {
        Engine::open(&self.path(), Durability::Always).expect("engine open")
    }
    fn wal_path(&self) -> PathBuf {
        self.path().join(WAL_FILE)
    }
}

fn sub(e: &mut Engine, op: &Op) -> Reply {
    e.submit(op).unwrap_or_else(|err| panic!("{op:?} → submit error: {err}"))
}

fn ok(e: &mut Engine, op: &Op) -> Reply {
    match sub(e, op) {
        Reply::Err { code, msg } => panic!(
            "{op:?} → unexpected errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        r => r,
    }
}

fn bad(e: &mut Engine, op: &Op) -> (u16, String) {
    match sub(e, op) {
        Reply::Err { code, msg } => (code, String::from_utf8_lossy(&msg).into_owned()),
        other => panic!("{op:?} → expected Reply::Err, got {other:?}"),
    }
}

/// 三条新 op 都回 `Reply::Count`（与 `RowCount` 同一个 tag，不新增应答）。
fn count_reply(e: &mut Engine, op: &Op) -> u64 {
    match ok(e, op) {
        Reply::Count(n) => n,
        other => panic!("expected Reply::Count, got {other:?}"),
    }
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn col(name: &str, flags: u32) -> ColumnDef {
    ColumnDef {
        name: b(name),
        flags,
    }
}

fn def(name: &str, columns: Vec<ColumnDef>) -> TableDef {
    TableDef {
        name: b(name),
        columns,
    }
}

/// 一张 `a` 唯一 + `b` 带索引的两列表：两条索引列，正好用来验孤儿。
fn kv(name: &str) -> Op {
    Op::TableCreate {
        def: def(
            name,
            vec![col("a", COL_UNIQUE | COL_INDEX), col("b", COL_INDEX)],
        ),
    }
}

fn put(table: &str, key: &str, cols: &[&str]) -> Op {
    Op::RowPut {
        table: b(table),
        key: b(key),
        cols: cols.iter().map(|c| b(c)).collect(),
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn clear(table: &str) -> Op {
    Op::TableClear { table: b(table) }
}

fn delrange(table: &str, column: u32, lo: &str, hi: &str, max: u32) -> Op {
    Op::RowRangeDelete {
        table: b(table),
        column,
        lo: b(lo),
        hi: b(hi),
        max,
    }
}

fn rename(from: &str, to: &str, flags: u32) -> Op {
    Op::TableRename {
        from: b(from),
        to: b(to),
        flags,
    }
}

fn shape(e: &mut Engine, table: &str) -> Option<TableDef> {
    match ok(e, &Op::TableGet { name: b(table) }) {
        Reply::Table(d) => d,
        other => panic!("TableGet → {other:?}"),
    }
}

fn row(e: &mut Engine, table: &str, key: &str) -> Option<RowRec> {
    match ok(
        e,
        &Op::RowGet {
            table: b(table),
            key: b(key),
        },
    ) {
        Reply::Row(r) => r,
        other => panic!("RowGet → {other:?}"),
    }
}

fn keys(e: &mut Engine, table: &str, column: u32, value: &str) -> Vec<Vec<u8>> {
    match ok(
        e,
        &Op::RowList {
            table: b(table),
            column,
            value: b(value),
            from: Vec::new(),
            limit: 0,
        },
    ) {
        Reply::Rows { rows, .. } => rows.into_iter().map(|r| r.key).collect(),
        other => panic!("RowList → {other:?}"),
    }
}

fn count(e: &mut Engine, table: &str, column: u32, value: &str) -> u64 {
    match ok(
        e,
        &Op::RowCount {
            table: b(table),
            column,
            value: b(value),
        },
    ) {
        Reply::Count(n) => n,
        other => panic!("RowCount → {other:?}"),
    }
}

fn table_id(e: &Engine, name: &str) -> u32 {
    e.state_snapshot().tables[b(name).as_slice()].id
}

/// 状态里还挂着多少行 / 多少索引项，用来抓孤儿。
fn footprint(s: &State) -> (usize, usize, usize) {
    (
        s.tables.len(),
        s.rows.values().map(|m| m.len()).sum(),
        s.row_index.len(),
    )
}

/// 灌一张 `kv` 表，五行：`a` = `x1..x5`，`b` = `"1".."5"`，主键 `k1..k5`。
fn seed_five(e: &mut Engine, table: &str) {
    ok(e, &kv(table));
    for i in 1..=5 {
        ok(
            e,
            &put(table, &format!("k{i}"), &[&format!("x{i}"), &i.to_string()]),
        );
    }
}

// ---------------------------------------------------------------- 清空一张表

#[test]
fn table_clear_drops_rows_and_index_but_keeps_shape_and_id() {
    let dir = Dir::new("clear-basic");
    let mut e = dir.open();
    seed_five(&mut e, "kv");
    let id = table_id(&e, "kv");
    assert_eq!(e.state_snapshot().row_index.len(), 10, "两条索引列 × 五行");

    let seq = e.apply_seq();
    assert_eq!(count_reply(&mut e, &clear("kv")), 5, "应答是被清掉的行数");
    assert_eq!(e.apply_seq() - seq, 1, "清空只推进一个 seq");

    let s = e.state_snapshot();
    assert_eq!(s.tables[b("kv").as_slice()].id, id, "清空不许换 id");
    assert_eq!(s.tables[b("kv").as_slice()].def.columns.len(), 2, "形状也不许动");
    assert_eq!(
        s.rows.get(&id).map(|m| m.len()).unwrap_or(0),
        0,
        "行必须走干净"
    );
    assert_eq!(s.row_index.len(), 0, "索引项一条都不许留在状态里");

    // 本来就空 → 零日志、回 0（没发生的事不该占一条 WAL 记录）。
    let seq2 = e.apply_seq();
    assert_eq!(count_reply(&mut e, &clear("kv")), 0);
    assert_eq!(e.apply_seq(), seq2, "空表清空不产日志");

    // 表不存在 → ENOENT，而且不产日志。
    let (code, _) = bad(&mut e, &clear("nope"));
    assert_eq!(code, errno::ENOENT);
    assert_eq!(e.apply_seq(), seq2);

    // 清空后还能接着写：id 与 def 都在点上，索引也重新长回来。
    ok(&mut e, &put("kv", "n1", &["q", "9"]));
    assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("n1")]);
    assert_eq!(e.state_snapshot().row_index.len(), 2);
}

/// 帧格式见 DESIGN §6.1：`seq(8) crc(4) len(4) reserved(4)` + payload。
/// `len == 0` 是预分配空洞，扫描到此为止。
fn last_record(buf: &[u8]) -> Option<(usize, usize)> {
    const HDR: usize = 20;
    let mut off = 0usize;
    let mut last = None;
    while off + HDR <= buf.len() {
        let len = u32::from_le_bytes(buf[off + 12..off + 16].try_into().unwrap()) as usize;
        if len == 0 || off + HDR + len > buf.len() {
            break;
        }
        last = Some((off, HDR + len));
        off += HDR + len;
    }
    last
}

/// 把清空那条记录从 payload 中间截断：重开后五行原样还在。
///
/// 这一刀切的是**一条**记录，所以它同时也是「五行不是五条记录」的证明 ——
/// 若实现退回逐行 `DelRow`，同样这一刀只会切掉尾部几行，重放出来就是「表被
/// 清了一半」那种既非成功也非失败的中间态。
#[test]
fn table_clear_is_one_record_and_a_torn_tail_keeps_the_rows() {
    let dir = Dir::new("clear-torn");
    {
        let mut e = dir.open();
        seed_five(&mut e, "kv");
        let seq0 = e.apply_seq();
        count_reply(&mut e, &clear("kv"));
        assert_eq!(e.apply_seq() - seq0, 1);

        let bytes = std::fs::read(dir.wal_path()).unwrap();
        let (off, len) = last_record(&bytes).expect("WAL 里至少有建表 + 五行 + 清空");
        // 清空这条只有 `tag + table` = 5 字节 payload，切中点会落进帧头。留着
        // 完整帧头、只砍掉 payload 尾巴，才能验到「payload 不够 = 整条作废」。
        let cut = off + len - 1;
        assert!(cut > off + 20, "得真的切在 payload 里");
        std::fs::File::options()
            .write(true)
            .open(dir.wal_path())
            .unwrap()
            .set_len(cut as u64)
            .unwrap();
    }

    let mut e = dir.open();
    assert_eq!(shape(&mut e, "kv").unwrap().columns.len(), 2, "表本身在更早的记录里");
    assert_eq!(keys(&mut e, "kv", ROW_PK, "").len(), 5, "坏尾整条作废：五行必须都在");
    assert_eq!(e.state_snapshot().row_index.len(), 10);
}

// ---------------------------------------------------------------- 按区间删

#[test]
fn row_range_delete_follows_the_row_range_column_rule() {
    let dir = Dir::new("range-basic");
    let mut e = dir.open();
    seed_five(&mut e, "kv");
    let id = table_id(&e, "kv");

    // 主键区间 [k2, k4) → k2、k3。
    let seq = e.apply_seq();
    assert_eq!(count_reply(&mut e, &delrange("kv", ROW_PK, "k2", "k4", 0)), 2);
    assert_eq!(e.apply_seq() - seq, 1, "一次区间删只占一条记录");
    assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("k1"), b("k4"), b("k5")]);
    let s = e.state_snapshot();
    assert_eq!(
        s.rows.get(&id).map(|m| m.len()).unwrap_or(0),
        3,
        "rows 与应答必须数得出同一个数"
    );
    assert_eq!(s.row_index.len(), 6, "被删那两行的索引项要一起走");
    // 索引侧也真的看不见了：按值列举与计数都只剩存活的行。
    assert_eq!(keys(&mut e, "kv", 1, "2"), Vec::<Vec<u8>>::new());
    assert_eq!(count(&mut e, "kv", 1, "2"), 0);
    assert_eq!(keys(&mut e, "kv", 0, "x2"), Vec::<Vec<u8>>::new());

    // 索引列区间 [4, 5) → 只有 k4（值 4）。上界不含：值恰为 hi 的那一段整段排除。
    assert_eq!(count_reply(&mut e, &delrange("kv", 1, "4", "5", 0)), 1);
    assert_eq!(keys(&mut e, "kv", 1, "4"), Vec::<Vec<u8>>::new());
    assert_eq!(keys(&mut e, "kv", 1, "5"), vec![b("k5")]);

    // `max` 是限流旋钮：值区间 [1, 9) 里本来剩 k1、k5 两行，只许删一条时，
    // 按 (值, 主键) 序先命中的是 k1（值 "1"）。
    assert_eq!(count_reply(&mut e, &delrange("kv", 1, "1", "9", 1)), 1);
    assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("k5")]);
}

#[test]
fn row_range_delete_rejects_and_noops_without_touching_state() {
    let dir = Dir::new("range-bad");
    let mut e = dir.open();
    ok(&mut e, &kv("kv"));
    ok(
        &mut e,
        &Op::TableCreate {
            def: def("plain", vec![col("a", 0)]),
        },
    );
    seed_five(&mut e, "rows");
    let seq = e.apply_seq();
    let before = e.state_snapshot();

    // 表不存在 → ENOENT。
    assert_eq!(bad(&mut e, &delrange("nope", ROW_PK, "", "", 0)).0, errno::ENOENT);
    // 列号越界 → EINVAL。
    assert_eq!(bad(&mut e, &delrange("kv", 9, "", "", 0)).0, errno::EINVAL);
    // 没建索引的列 → EINVAL（不退化成全表扫，判据与 `Op::RowRange` 逐字相同）。
    assert_eq!(bad(&mut e, &delrange("plain", 0, "", "", 0)).0, errno::EINVAL);
    // 空区间：`lo > hi` 是空而不是错误；区间里没有行也是空。
    assert_eq!(count_reply(&mut e, &delrange("rows", 1, "9", "8", 0)), 0);
    assert_eq!(count_reply(&mut e, &delrange("rows", 1, "z", "", 0)), 0);
    assert_eq!(count_reply(&mut e, &delrange("kv", ROW_PK, "", "", 0)), 0);

    assert_eq!(e.apply_seq(), seq, "上面每一条都不该产日志");
    assert_eq!(e.state_snapshot(), before, "也不该动状态");
}

/// 区间删的全部价值是「WAL 体积与命中数无关」。这里比字节数而不是比时间：
/// 时间在这台盘上不可复现，字节数是格式本身的性质。
#[test]
fn a_range_delete_does_not_grow_with_the_number_of_hits() {
    let one = log_payload(&Log::DelRange {
        table: 7,
        column: ROW_PK,
        lo: b("k0"),
        hi: b("k9"),
        max: 0,
    });
    let per_row: Vec<Vec<u8>> = (0..1000)
        .map(|i| {
            log_payload(&Log::DelRow {
                table: 7,
                key: b(&format!("k{i}")),
            })
        })
        .collect();
    let many: usize = per_row.iter().map(|p| p.len()).sum();
    assert!(
        one.len() < 40,
        "区间记录只存区间：实际 {} 字节，应当与命中数无关",
        one.len()
    );
    assert!(
        many > one.len() * 500,
        "一千条 DelRow 共 {many} 字节，一条 DelRange 才 {} 字节：比值必须悬殊，\
         否则这个 op 没有存在的理由",
        one.len()
    );
}

/// 同一条 `Log::DelRange` 重放两次 = 一次。区间是就地重算的，第一次删完
/// 第二次就数不到任何行，于是 `rows` 与 `row_index` 都不会被二次伤害。
#[test]
fn bulk_logs_replay_idempotently() {
    let mut s = State::new();
    s.apply(&Log::PutTable {
        id: 1,
        def: def(
            "kv",
            vec![col("a", COL_UNIQUE | COL_INDEX), col("b", COL_INDEX)],
        ),
    });
    for i in 1..=5 {
        s.apply(&Log::PutRow {
            table: 1,
            row: RowRec {
                key: b(&format!("k{i}")),
                cols: vec![b(&format!("x{i}")), b(&i.to_string())],
            },
        });
    }

    let del = Log::DelRange {
        table: 1,
        column: ROW_PK,
        lo: b("k2"),
        hi: b("k5"),
        max: 0,
    };
    s.apply(&del);
    let once = s.clone();
    assert_eq!(s.rows[&1].len(), 2);
    assert_eq!(s.row_index.len(), 4, "剩下的两行 × 两条索引列");
    s.apply(&del);
    assert_eq!(s, once, "区间删重放第二次必须等价于一次");

    let clear = Log::ClearTable { table: 1 };
    s.apply(&clear);
    let emptied = s.clone();
    assert_eq!(footprint(&s), (1, 0, 0), "清空后表还在、行与索引都空");
    s.apply(&clear);
    assert_eq!(s, emptied, "清空重放第二次必须等价于一次");

    // 改名同理：第二次时 `from` 已经不指向那个 id 了。
    let rn = Log::RenameTable {
        id: 1,
        from: b("kv"),
        to: b("kv2"),
    };
    s.apply(&rn);
    let renamed = s.clone();
    assert_eq!(s.tables[b("kv2").as_slice()].id, 1);
    assert!(s.tables.get(b("kv").as_slice()).is_none());
    s.apply(&rn);
    assert_eq!(s, renamed, "改名重放第二次必须等价于一次");

    // 一条指向不存在表的日志不许 panic，也不许造出一张空表来。
    let mut t = State::new();
    t.apply(&Log::ClearTable { table: 42 });
    t.apply(&Log::DelRange {
        table: 42,
        column: ROW_PK,
        lo: Vec::new(),
        hi: Vec::new(),
        max: 0,
    });
    t.apply(&Log::RenameTable {
        id: 42,
        from: b("ghost"),
        to: b("here"),
    });
    assert_eq!(footprint(&t), (0, 0, 0), "坏日志只能什么都不做");
}

// ---------------------------------------------------------------- 换表名

#[test]
fn table_rename_keeps_the_id_and_cascades_the_replaced_table() {
    let dir = Dir::new("rename-replace");
    let mut e = dir.open();
    seed_five(&mut e, "old");
    seed_five(&mut e, "new");
    ok(&mut e, &put("new", "n1", &["z", "7"]));
    let old_id = table_id(&e, "old");
    let new_id = table_id(&e, "new");
    assert_ne!(old_id, new_id, "两张表本来各有各的 id");

    let seq = e.apply_seq();
    match ok(&mut e, &rename("old", "new", 0)) {
        Reply::Ok => {}
        other => panic!("TableRename → {other:?}"),
    }
    assert_eq!(e.apply_seq() - seq, 1, "改名只占一条记录");

    let s = e.state_snapshot();
    assert_eq!(footprint(&s), (1, 5, 10), "只剩一张表五行；被顶掉那六行必须级联走干净");
    assert_eq!(s.tables[b("new").as_slice()].id, old_id, "id 不随名字换");
    assert!(s.tables.get(b("old").as_slice()).is_none());
    assert!(!s.table_by_id.contains_key(&new_id), "被顶掉那张表不能留 id 映射");
    assert_eq!(s.table_by_id[&old_id], b("new"));

    // 顶替之后行直接可用，索引也在新名下认得（这正是影子表切换要的）。
    assert_eq!(keys(&mut e, "new", ROW_PK, "").len(), 5);
    assert_eq!(keys(&mut e, "new", 0, "x1"), vec![b("k1")]);
    assert!(
        row(&mut e, "new", "n1").is_none(),
        "被替换那张表的行不该还在"
    );

    // 改名之后再写：形状与 id 都在点上。
    ok(&mut e, &put("new", "k9", &["x9", "9"]));
    assert_eq!(count(&mut e, "new", ROW_PK, ""), 6);
}

#[test]
fn table_rename_boundaries() {
    let dir = Dir::new("rename-bad");
    let mut e = dir.open();
    seed_five(&mut e, "a1");
    seed_five(&mut e, "b1");
    let seq = e.apply_seq();
    let before = e.state_snapshot();

    // `RENAME_NOREPLACE` 撞名 → EEXIST，且一个字节都不动。
    let (code, _) = bad(&mut e, &rename("a1", "b1", RENAME_NOREPLACE));
    assert_eq!(code, errno::EEXIST);
    // 源不存在 → ENOENT。
    assert_eq!(bad(&mut e, &rename("nope", "x", 0)).0, errno::ENOENT);
    // 目标名空 / 超长，与 `bad_def` 同一道闸门。
    assert_eq!(bad(&mut e, &rename("a1", "", 0)).0, errno::EINVAL);
    assert_eq!(
        bad(
            &mut e,
            &rename("a1", &"n".repeat(TABLE_NAME_MAX + 1), 0)
        )
        .0,
        errno::ENAMETOOLONG
    );
    // `from == to` 是空操作：零日志，回 Ok。
    match ok(&mut e, &rename("a1", "a1", 0)) {
        Reply::Ok => {}
        other => panic!("自名改名 → {other:?}"),
    }
    assert_eq!(e.apply_seq(), seq, "上面每一条都不该产日志");
    assert_eq!(e.state_snapshot(), before, "也不该动状态");

    // 改名到空位：目标名不存在时不需要任何标志，正常换过去。
    let id = table_id(&e, "a1");
    ok(&mut e, &rename("a1", "a9", RENAME_NOREPLACE));
    assert_eq!(table_id(&e, "a9"), id);
    assert_eq!(footprint(&e.state_snapshot()), (2, 10, 20));
}

// ---------------------------------------------------------------- 同值消除

#[test]
fn same_value_rowput_writes_nothing() {
    let dir = Dir::new("elide");
    let mut e = dir.open();
    ok(&mut e, &kv("kv"));
    ok(&mut e, &put("kv", "k1", &["x", "1"]));
    let seq = e.apply_seq();
    assert_eq!(e.state_snapshot().row_index.len(), 2);

    // 逐字节相同的覆盖写：应答照旧是那一行，WAL 一个字都不写。
    let st0 = e.snapshot_stats();
    for _ in 0..3 {
        match ok(&mut e, &put("kv", "k1", &["x", "1"])) {
            Reply::Row(Some(r)) => assert_eq!(r.cols, vec![b("x"), b("1")]),
            other => panic!("RowPut → {other:?}"),
        }
    }
    assert_eq!(e.apply_seq(), seq, "同值覆盖写不落 WAL 记录");
    assert_eq!(e.state_snapshot().row_index.len(), 2, "也不该重排索引");
    // 可观测面（不加 `EngineStats` 字段的理由就在这一条）：`ops_total` 数的是
    // 进来的 op，`writes_total` 只在产出日志时 +1（core.rs 那条 `if !logs.is_empty()`）。
    // 两者的差就是被填掉的写，所以上线后拿这两个数相减就能量命中率，不必新开门。
    let st1 = e.snapshot_stats();
    assert_eq!(st1.ops_total - st0.ops_total, 3, "op 计数照旧走");
    assert_eq!(
        st1.writes_total, st0.writes_total,
        "被消除的写不计入 writes_total：这就是跳过写的那个计数器"
    );
    assert_eq!(st1.syncs_total, st0.syncs_total, "也就不该多一次 fsync");

    // 改一个字节就必须落。
    ok(&mut e, &put("kv", "k1", &["x", "2"]));
    assert_eq!(e.apply_seq(), seq + 1, "真正的变化照旧一条记录");

    // 条件位排在消除之前，所以它们照样拦住：同值 + `PUT_IF_ABSENT` 仍是 EEXIST。
    let (code, _) = bad(
        &mut e,
        &Op::RowPut {
            table: b("kv"),
            key: b("k1"),
            cols: vec![b("x"), b("2")],
            flags: PUT_IF_ABSENT,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    );
    assert_eq!(code, errno::EEXIST, "同值消除不许把「已存在」吃成成功");
    // CAS 不命中 → EAGAIN，且不改 seq。
    let (code, _) = bad(
        &mut e,
        &Op::RowPut {
            table: b("kv"),
            key: b("k1"),
            cols: vec![b("x"), b("2")],
            flags: 0,
            cond_col: 1,
            cond_val: b("no-such"),
        },
    );
    assert_eq!(code, errno::EAGAIN);
    assert_eq!(e.apply_seq(), seq + 1);
    // 同值 + CAS 命中 → 成功且零日志（这才是要省的那一次）。
    ok(
        &mut e,
        &Op::RowPut {
            table: b("kv"),
            key: b("k1"),
            cols: vec![b("x"), b("2")],
            flags: 0,
            cond_col: 1,
            cond_val: b("2"),
        },
    );
    assert_eq!(e.apply_seq(), seq + 1, "同值命中不该补一条记录");

    // 新键不是同值。
    ok(&mut e, &put("kv", "k2", &["y", "1"]));
    assert_eq!(e.apply_seq(), seq + 2);
}

/// 同值消除落进崩溃恢复路径也必须无痕：重开后看不见「被省掉的那几次」。
#[test]
fn elided_writes_leave_no_trace_across_restart() {
    let dir = Dir::new("elide-restart");
    let before: State;
    let seeded;
    {
        let mut e = dir.open();
        seed_five(&mut e, "kv");
        seeded = e.apply_seq();
        for _ in 0..50 {
            ok(&mut e, &put("kv", "k3", &["x3", "3"]));
        }
        // 一次纯同值轮：`commit_batch` 对空 payload 直接返回，seq 一步不走。
        assert_eq!(count_reply(&mut e, &delrange("kv", ROW_PK, "k9", "k9", 0)), 0);
        assert_eq!(e.apply_seq(), seeded, "五十次同值写与一次空区间删都没推水位");
        before = e.state_snapshot();
    }
    let mut e = dir.open();
    assert_eq!(e.state_snapshot(), before, "重开必须逐字段相等");
    // 落盘的记录只有「建表 + 五行」那六条：被省掉的写在 WAL 里不留痕。
    assert_eq!(e.apply_seq(), seeded);
    assert_eq!(keys(&mut e, "kv", ROW_PK, "").len(), 5);
}

// ---------------------------------------------------------------- 批与原子性

#[test]
fn batch_with_bulk_ops_is_one_record_and_atomic() {
    let dir = Dir::new("bulk-batch");
    {
        let mut e = dir.open();
        seed_five(&mut e, "kv");
        let seq = e.apply_seq();
        // 批内后一条看得见前一条的效果：先清空，再插一行。
        match ok(
            &mut e,
            &Op::Batch {
                ops: vec![clear("kv"), put("kv", "only", &["x", "1"])],
            },
        ) {
            Reply::Batch { replies } => {
                assert_eq!(replies.len(), 2);
                assert_eq!(replies[0], Reply::Count(5));
                assert!(matches!(replies[1], Reply::Row(Some(_))));
            }
            other => panic!("Batch → {other:?}"),
        }
        assert_eq!(e.apply_seq() - seq, 1, "整批只占一条记录");
        assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("only")]);

        // 批里混一条会失败的 op：整批不生效（连那条清空也不），也不产日志。
        let seq2 = e.apply_seq();
        let (code, _) = bad(
            &mut e,
            &Op::Batch {
                ops: vec![clear("kv"), rename("nope", "x", 0)],
            },
        );
        assert_eq!(code, errno::ENOENT);
        assert_eq!(e.apply_seq(), seq2, "失败的批一条记录都不写");
        assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("only")], "清空也没发生");

        // 截断这批的记录：两个效果一起退回。
        let bytes = std::fs::read(dir.wal_path()).unwrap();
        let (off, len) = last_record(&bytes).unwrap();
        std::fs::File::options()
            .write(true)
            .open(dir.wal_path())
            .unwrap()
            .set_len((off + len / 2) as u64)
            .unwrap();
    }
    let mut e = dir.open();
    assert_eq!(
        keys(&mut e, "kv", ROW_PK, "").len(),
        5,
        "批里那条 `only` 与那次清空必须一起丢"
    );
}

// ---------------------------------------------------------------- 快照与 dump

#[test]
fn bulk_ops_survive_checkpoint_and_dump_restore() {
    let src = Dir::new("bulk-snap-src");
    let before: State;
    {
        let mut e = src.open();
        seed_five(&mut e, "kv");
        count_reply(&mut e, &delrange("kv", 1, "1", "3", 0));
        ok(&mut e, &kv("shadow"));
        ok(&mut e, &put("shadow", "s1", &["s", "1"]));
        ok(&mut e, &rename("shadow", "kv", 0));
        count_reply(&mut e, &clear("kv"));
        ok(&mut e, &put("kv", "tail", &["t", "9"]));
        ok(&mut e, &Op::Checkpoint);
        assert_eq!(e.wal_len(), 0, "同步 checkpoint 之后 WAL 应为空");
        ok(&mut e, &put("kv", "after", &["a", "8"]));
        assert!(e.wal_len() > 0);
        before = e.state_snapshot();
    }

    // 重开：快照 + WAL 尾巴重放，逐字段相等（`row_index` 不在快照里，只能由
    // `rows` + def 重建，所以这条断言同时是「索引没漏」的闸门）。
    {
        let mut e = src.open();
        assert_eq!(e.state_snapshot(), before, "checkpoint + 重开必须逐字段相等");
        assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("after"), b("tail")]);
        // `shadow` 这个名字已经没了，`kv` 用的是它原来那个 id。
        assert!(shape(&mut e, "shadow").is_none());
        assert_eq!(count(&mut e, "kv", 1, "9"), 1);
    }

    let dumped = dump_dir(&src.path()).expect("dump");
    let dst = Dir::new("bulk-snap-dst");
    restore_dump(&dst.path(), &dumped).expect("restore");
    {
        let mut e = dst.open();
        assert_eq!(e.state_snapshot(), before, "dump → restore 必须逐字段相等");
        // 导出的是终态：清空与改名都不留痕，`logs` 里只有 Put* 那几条。
        assert!(
            !dumped
                .logs
                .iter()
                .any(|l| matches!(l, Log::ClearTable { .. } | Log::DelRange { .. } | Log::RenameTable { .. })),
            "纯形状变更不进 dump"
        );
        ok(&mut e, &rename("kv", "kv2", 0));
        assert_eq!(keys(&mut e, "kv2", ROW_PK, "").len(), 2);
    }
}
