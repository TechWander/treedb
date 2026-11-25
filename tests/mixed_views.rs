//! 万级混合视图档：`CreateView`/`Create` 的记录数公式，与「2 Lookup + 1 ReadDir +
//! 1 Create + 1 Unlink」成组并发时的净零账本。
//!
//! 这一档与 `delete_storm` 的分工：那个量具问「删完还剩什么」，这个问「读写混在
//! 一起长期跑，目录树会不会慢慢漂移」。三条口径缺一不可：
//!
//! 1. 记录数按 `src/engine/apply.rs` 的分支逐条核，不靠批大小反推：`CreateView`
//!    产 2 条（`PutView` + `PutNode(root)`），无 data_ref 的 `Create` 产 3 条
//!    （`bump` + `PutNode` + `PutDentry`），最后一个名字的 `Unlink` 产 2 条
//!    （`DelDentry` + `DelNode`）。所以一组五个 op 正好 5 条记录、2 次写。
//! 2. 视图覆盖是数出来的，不是算出来的：每组实际用的 view id 记进线程本地表，
//!    合并去重后必须等于 `min(N, G)`。轮转写错（比如所有连接都打 vids[0]）在账本
//!    上完全看不出来，只有这一条能揭穿。
//! 3. 净零要核到名字级：账本回到测量前只说明计数器没漏，还得证明临时名真的没了、
//!    预热的名字真的还在，否则「Create 与 Unlink 双双失败」也能凑出一个漂亮的
//!    账本差值。
//!
//! 默认规模 2000 视图 × 20 文件（40 000 条 dentry）。`TREEDB_MVIEWS_VIEWS` /
//! `TREEDB_MVIEWS_ROWS` 可缩小以快速过一遍。

use treedb::engine::core::EngineStats;
use treedb::engine::model::{errno, Op, Reply};
use treedb::net::client::Client;

mod common;
use common::Server;

/// 每个 pipeline 的 op 数。5 的倍数是硬要求：一组五个 op 不能被切进两批。
const PIPE: usize = 50;
const GROUP_OPS: usize = 5;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn view_name(i: u64) -> Vec<u8> {
    format!("mv{i:06}").into_bytes()
}

fn child(j: u64) -> Vec<u8> {
    format!("w{j:04}").into_bytes()
}

fn tmp_name(nonce: u64, g: u64) -> Vec<u8> {
    format!("m{nonce}-{g}").into_bytes()
}

fn stats(c: &mut Client) -> EngineStats {
    match c.call(&Op::Stats).expect("Stats") {
        Reply::Stats(s) => *s,
        other => panic!("Stats → {other:?}"),
    }
}

/// 账本七元组。相等就意味着「视图/节点/条目/锁/内容引用/表/行」一个都没漂。
fn ledger(s: &EngineStats) -> (u64, u64, u64, u64, u64, u64, u64) {
    (s.views, s.nodes, s.dentries, s.locks, s.refs, s.tables, s.rows)
}

/// 建 N 个视图并给每个视图灌 W 个文件，返回视图 id 表。
///
/// 建视图与建文件都走 pipeline：逐条一问一答时，光灌语料自己就要花掉比被测窗口
/// 更长的时间，那会把「万级视图」这一档变成「等磁盘」这一档。
fn seed_views(c: &mut Client, nviews: u64, w: u64) -> Vec<u32> {
    let mut vids: Vec<u32> = Vec::with_capacity(nviews as usize);
    for start in (0..nviews).step_by(500) {
        let end = (start + 500).min(nviews);
        let batch: Vec<Op> =
            (start..end).map(|i| Op::CreateView { name: view_name(i) }).collect();
        for (i, r) in (start..end).zip(c.pipeline(&batch).expect("CreateView pipeline")) {
            match r {
                Reply::View { id, root: 1 } => vids.push(id),
                other => panic!("CreateView #{i} → {other:?}"),
            }
        }
    }
    let mut made = 0u64;
    for &view in &vids {
        for start in (0..w).step_by(500) {
            let end = (start + 500).min(w);
            let batch: Vec<Op> = (start..end)
                .map(|j| Op::Create {
                    view,
                    parent: 1,
                    name: child(j),
                    mode: 0o644,
                    uid: 1,
                    gid: 1,
                })
                .collect();
            for r in c.pipeline(&batch).expect("Create pipeline") {
                if matches!(r, Reply::Node(_)) {
                    made += 1;
                } else {
                    panic!("Create → {r:?}");
                }
            }
        }
    }
    assert_eq!(made, nviews * w, "预热文件必须一个不落");
    vids
}

/// 一组五个 op：两次点查、一次翻页、一次建名、一次删同名。
fn group(nonce: u64, g: u64, view: u32, w: u64) -> Vec<Op> {
    let tmp = tmp_name(nonce, g);
    vec![
        Op::Lookup { view, parent: 1, name: child(g % w) },
        Op::Lookup { view, parent: 1, name: child(g / w.max(1) % w) },
        Op::ReadDir { view, parent: 1, offset: 0, limit: 20 },
        Op::Create { view, parent: 1, name: tmp.clone(), mode: 0o644, uid: 1, gid: 1 },
        Op::Unlink { view, parent: 1, name: tmp },
    ]
}

struct Ran {
    groups: u64,
    lookups: u64,
    dirs: u64,
    creates: u64,
    unlinks: u64,
    errs: u64,
    /// 每组实际打到的 view id，按线程收集后合并。覆盖数是这里数出来的。
    used: Vec<u32>,
}

/// 在 `conns` 条连接上跑 `total` 个组，组号从共享计数器领，视图按 `g % N` 轮转。
///
/// 每批正好一批一组时才有「组内 Create 先于 Unlink」这条序：含写的批整批交给
/// 唯一提交线程按序执行（只有全只读的批才分流到 worker），所以把五个 op 放进
/// 同一个 pipeline 就是有序的；把它们切成两批就未必。
fn run_groups(addr: &str, vids: &[u32], w: u64, nonce: u64, total: u64, conns: usize) -> Ran {
    let nviews = vids.len();
    let per = (total as usize + conns - 1) / conns;
    let mut handles = Vec::with_capacity(conns);
    for k in 0..conns {
        let a = addr.to_string();
        let vids = vids.to_vec();
        let lo = (k * per) as u64;
        let hi = (lo + per as u64).min(total);
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect(&a, 5000).expect("connect");
            let mut out = Ran {
                groups: 0,
                lookups: 0,
                dirs: 0,
                creates: 0,
                unlinks: 0,
                errs: 0,
                used: Vec::new(),
            };
            let mut g = lo;
            while g < hi {
                let end = (g + (PIPE / GROUP_OPS) as u64).min(hi);
                let mut batch: Vec<Op> = Vec::with_capacity((end - g) as usize * GROUP_OPS);
                for x in g..end {
                    let view = vids[x as usize % nviews];
                    out.used.push(view);
                    batch.extend(group(nonce, x, view, w));
                }
                match c.pipeline(&batch) {
                    Ok(rs) => {
                        if rs.len() != batch.len() {
                            out.errs += 1;
                            eprintln!("批 @组{g} 应答 {} 条，请求 {} 条", rs.len(), batch.len());
                            break;
                        }
                        let mut iter = rs.chunks(GROUP_OPS);
                        for (x, ops) in (g..end).zip(&mut iter) {
                            let ok = ops.len() == GROUP_OPS
                                && matches!(ops[0], Reply::Node(_))
                                && matches!(ops[1], Reply::Node(_))
                                && matches!(ops[2], Reply::Dirents { .. })
                                && matches!(ops[3], Reply::Node(_))
                                && matches!(ops[4], Reply::NodeRef(ref d) if d.is_empty());
                            if !ok {
                                out.errs += 1;
                                // 形状不符就是断言要抓的东西，但全错时不要刷屏。
                                if out.errs <= 5 {
                                    eprintln!("组 {x} 回包形状不符");
                                }
                                continue;
                            }
                            out.lookups += 2;
                            out.dirs += 1;
                            out.creates += 1;
                            out.unlinks += 1;
                            out.groups += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!("pipeline: {e}");
                        out.errs += (end - g) as u64;
                    }
                }
                g = end;
            }
            out
        }));
    }
    let mut sum = Ran { groups: 0, lookups: 0, dirs: 0, creates: 0, unlinks: 0, errs: 0, used: Vec::new() };
    for h in handles {
        let r = h.join().expect("mixed thread");
        sum.groups += r.groups;
        sum.lookups += r.lookups;
        sum.dirs += r.dirs;
        sum.creates += r.creates;
        sum.unlinks += r.unlinks;
        sum.errs += r.errs;
        sum.used.extend(r.used);
    }
    sum.used.sort_unstable();
    sum.used.dedup();
    sum
}

/// 头尾各至多 200 组：临时名必须 ENOENT，预热名必须还在。
fn check_names(c: &mut Client, vids: &[u32], nonce: u64, w: u64, groups: u64) -> (u64, u64) {
    let mut ids: Vec<u64> = (0..groups.min(200)).collect();
    ids.extend(groups.saturating_sub(200)..groups);
    ids.sort_unstable();
    ids.dedup();
    let (mut gone, mut kept) = (0u64, 0u64);
    for &g in &ids {
        let view = vids[g as usize % vids.len()];
        match c
            .call(&Op::Lookup { view, parent: 1, name: tmp_name(nonce, g) })
            .expect("lookup tmp")
        {
            Reply::Err { code, .. } if code == errno::ENOENT => gone += 1,
            other => panic!("临时名 m{nonce}-{g} 竟然存在：{other:?}"),
        }
        let want = child(g % w);
        match c.call(&Op::Lookup { view, parent: 1, name: want.clone() }).expect("lookup child") {
            Reply::Node(_) => kept += 1,
            other => {
                panic!("预热名 {} 竟然丢了：{other:?}", String::from_utf8_lossy(&want))
            }
        }
    }
    (gone, kept)
}

#[test]
fn seed_ledger_matches_the_documented_record_formula() {
    let nviews = env_u64("TREEDB_MVIEWS_VIEWS", 2000);
    let w = env_u64("TREEDB_MVIEWS_ROWS", 20);
    let srv = Server::start_extra("group", 4, &["--auto-checkpoint-mb", "0"]);
    let mut c = srv.client();
    let s0 = stats(&mut c);
    assert_eq!(ledger(&s0), (0, 0, 0, 0, 0, 0, 0), "这一档要求空库");
    let vids = seed_views(&mut c, nviews, w);
    let s1 = stats(&mut c);
    assert_eq!(
        ledger(&s1),
        (nviews, nviews * (w + 1), nviews * w, 0, 0, 0, 0),
        "views/nodes/dentries 必须正好是公式给出的形状"
    );
    assert_eq!(s1.seq - s0.seq, 2 * nviews + 3 * nviews * w, "记录数由 apply.rs 的分支给出");
    assert_eq!(s1.writes_total - s0.writes_total, nviews + nviews * w);
    // 视图 id 必须原样查回来，且每个视图的根目录一页就数满 W 项。
    match c.call(&Op::ListViews).expect("ListViews") {
        Reply::Views(v) => assert_eq!(v.len() as u64, nviews, "ListViews 数量不符"),
        other => panic!("ListViews → {other:?}"),
    }
    let mut paged = 0u64;
    for chunk in vids.chunks(200) {
        let batch: Vec<Op> = chunk
            .iter()
            .map(|&view| Op::ReadDir { view, parent: 1, offset: 0, limit: 1000 })
            .collect();
        for (&view, r) in chunk.iter().zip(c.pipeline(&batch).expect("ReadDir pipeline")) {
            match r {
                Reply::Dirents { names, next, end, .. } => {
                    assert_eq!((names.len() as u64, next, end), (w, w, true), "view={view} 翻页不像 {w} 项");
                    paged += 1;
                }
                other => panic!("ReadDir view={view} → {other:?}"),
            }
        }
    }
    assert_eq!(paged, nviews);
    srv.graceful_shutdown();
    println!(
        "mv_seed views={nviews} rows={w} records={} writes={} paged={paged} errors=0",
        s1.seq - s0.seq,
        s1.writes_total - s0.writes_total
    );
}

#[test]
fn one_mixed_group_costs_five_records_and_two_writes() {
    let srv = Server::start_extra("group", 1, &["--auto-checkpoint-mb", "0"]);
    let mut c = srv.client();
    let vids = seed_views(&mut c, 1, 20);
    let view = vids[0];
    let nonce = 7;
    let s = stats(&mut c);
    let seq = s.seq;
    let wr = s.writes_total;
    // 基线取在插入这一组之前。后面拿它和组跑完后的账本比，才是「净零」；
    // 和同一个 `e` 再读一次比只是自反式断言，测不出任何东西。
    let base = ledger(&s);
    let per_op = |c: &mut Client, op: Op, want_records: u64, want_writes: u64| {
        let before = stats(c);
        assert!(!matches!(c.call(&op).expect("op"), Reply::Err { .. }), "op {op:?} 被拒");
        let after = stats(c);
        assert_eq!(after.seq - before.seq, want_records, "op {op:?} 的记录数不符");
        assert_eq!(after.writes_total - before.writes_total, want_writes, "op {op:?} 的写计数不符");
    };
    // Lookup / ReadDir 一条记录都不产，也不计写；Create 三条、Unlink 两条。
    per_op(&mut c, Op::Lookup { view, parent: 1, name: child(3) }, 0, 0);
    per_op(&mut c, Op::ReadDir { view, parent: 1, offset: 0, limit: 20 }, 0, 0);
    per_op(
        &mut c,
        Op::Create { view, parent: 1, name: tmp_name(nonce, 0), mode: 0o644, uid: 1, gid: 1 },
        3,
        1,
    );
    per_op(&mut c, Op::Unlink { view, parent: 1, name: tmp_name(nonce, 0) }, 2, 1);
    let e = stats(&mut c);
    assert_eq!(e.seq - seq, 5, "一组五个 op 正好五条记录");
    assert_eq!(e.writes_total - wr, 2, "一组只有 Create 与 Unlink 计写");
    assert_eq!(ledger(&e), base, "单组跑完账本必须回到组开始前");
    srv.graceful_shutdown();
    println!("mv_group_formula records_per_group=5 writes_per_group=2 errors=0");
}

#[test]
fn mixed_groups_leave_the_ledger_exactly_where_it_started() {
    let nviews = env_u64("TREEDB_MVIEWS_MIX_VIEWS", 200);
    let w = env_u64("TREEDB_MVIEWS_ROWS", 20);
    let groups = env_u64("TREEDB_MVIEWS_GROUPS", 5000);
    let nonce = 11;
    let srv = Server::start_extra("group", 4, &["--auto-checkpoint-mb", "1"]);
    let mut c = srv.client();
    let vids = seed_views(&mut c, nviews, w);
    // 把灌种子可能触发的快照挪到测量窗口外。
    assert!(matches!(c.call(&Op::Checkpoint), Ok(Reply::Ok)));
    let s0 = stats(&mut c);
    let ran = run_groups(&srv.addr, &vids, w, nonce, groups, 1);
    let s1 = stats(&mut c);
    assert_eq!(ran.errs, 0, "混合组一条都不能失败");
    assert_eq!(ran.groups, groups);
    assert_eq!((ran.lookups, ran.dirs, ran.creates, ran.unlinks), (2 * groups, groups, groups, groups));
    assert_eq!(s1.seq - s0.seq, 5 * groups, "records 必须是 5G，读不能偷偷记账");
    assert_eq!(s1.writes_total - s0.writes_total, 2 * groups, "writes 必须是 2G");
    assert_eq!(ledger(&s1), ledger(&s0), "净零：账本必须逐项回到测量前");
    assert_eq!(ran.used.len() as u64, nviews.min(groups), "覆盖到的视图要数得出来");
    let (gone, kept) = check_names(&mut c, &vids, nonce, w, groups);
    assert_eq!((gone, kept), (groups.min(400), groups.min(400)), "名字级核对没跑完");

    // 重开一次：净零的终态必须由快照+WAL 重放原样撑住，临时名不能复活。
    let stopped = srv.graceful_shutdown();
    let srv = Server::restart_at_workers(stopped.tmp, stopped.dir, "group", 4);
    let mut c = srv.client();
    let r = stats(&mut c);
    assert_eq!(ledger(&r), ledger(&s0), "重开后账本要回到同一格");
    let (gone2, kept2) = check_names(&mut c, &vids, nonce, w, groups);
    assert_eq!((gone2, kept2), (groups.min(400), groups.min(400)), "重开后名字级核对失败");
    srv.graceful_shutdown();
    println!(
        "mv_netzero views={nviews} rows={w} groups={groups} records={} writes={} touched={} gone={gone} kept={kept} reopen_gone={gone2} errors=0",
        s1.seq - s0.seq,
        s1.writes_total - s0.writes_total,
        ran.used.len(),
    );
}

#[test]
fn concurrent_mixed_groups_stay_net_zero_across_connections() {
    let nviews = env_u64("TREEDB_MVIEWS_MIX_VIEWS", 200);
    let w = env_u64("TREEDB_MVIEWS_ROWS", 20);
    let conns = 8usize;
    let groups = env_u64("TREEDB_MVIEWS_CONC_GROUPS", 8000);
    let nonce = 23;
    let srv = Server::start_extra("group", 8, &["--auto-checkpoint-mb", "1"]);
    let mut c = srv.client();
    let vids = seed_views(&mut c, nviews, w);
    assert!(matches!(c.call(&Op::Checkpoint), Ok(Reply::Ok)));
    let s0 = stats(&mut c);
    let ran = run_groups(&srv.addr, &vids, w, nonce, groups, conns);
    let s1 = stats(&mut c);
    assert_eq!(ran.errs, 0, "八条连接并发下也不能有一条失败");
    assert_eq!(ran.groups, groups, "组数不能被连接切分吃掉");
    assert_eq!(s1.seq - s0.seq, 5 * groups);
    assert_eq!(s1.writes_total - s0.writes_total, 2 * groups);
    assert_eq!(ledger(&s1), ledger(&s0), "并发净零：账本必须逐项相等");
    assert_eq!(ran.used.len() as u64, nviews.min(groups), "每条连接都要真的遍历到全部视图");
    let (gone, kept) = check_names(&mut c, &vids, nonce, w, groups);
    assert_eq!((gone, kept), (groups.min(400), groups.min(400)));
    srv.graceful_shutdown();
    println!(
        "mv_concurrent views={nviews} conns={conns} groups={groups} records={} touched={} gone={gone} kept={kept} errors=0",
        s1.seq - s0.seq,
        ran.used.len()
    );
}

#[test]
fn view_rotation_covers_min_of_views_and_groups() {
    // 两组规模：G > N 时必须全覆盖，G < N 时覆盖数恰好等于 G。
    // 只断言「等于 N」会让轮转写错（例如恒取 vids[0]）在 G < N 的格子里蒙混过关。
    let w = 5u64;
    let srv = Server::start_extra("group", 2, &["--auto-checkpoint-mb", "0"]);
    let mut c = srv.client();
    let vids = seed_views(&mut c, 60, w);
    let more = run_groups(&srv.addr, &vids, w, 31, 500, 4);
    assert_eq!(more.errs, 0);
    assert_eq!(more.used.len(), 60, "组数多于视图数时必须全覆盖");
    let fewer = run_groups(&srv.addr, &vids, w, 37, 25, 4);
    assert_eq!(fewer.errs, 0);
    assert_eq!(fewer.groups, 25);
    assert_eq!(fewer.used.len(), 25, "组数少于视图数时覆盖数只能等于组数");
    let s = stats(&mut c);
    assert_eq!(ledger(&s), (60, 60 * (w + 1), 60 * w, 0, 0, 0, 0), "两组跑完账本仍是语料形状");
    srv.graceful_shutdown();
    println!("mv_rotation more_touched=60 fewer_touched=25 errors=0");
}
