//! `treedb-bench`：对运行中的 `treedb-server` 打负载并汇报吞吐/延迟。
//!
//! 三个轴分别对应验收目标：
//! - `--op ping` / `--op stat`：纯读路径（不进 WAL），看事件循环本身的上限。
//! - `--op create` / `--op lookup` / `--op readdir`：目录树常见 op。
//! - `--op row*`：通用表（§5.1）的七条路径 —— `rowinsert` 插入（树 + 主键索引）、
//!   `rowput` 覆盖已有行、`rowget` 主键点查、`rowlist` 主键序翻页、`rowidx` 二级
//!   索引等值列举、`rowrange` 主键区间扫描、`rowidxrange` 索引列值区间扫描、
//!   `rowcount` 按主键计数（O(1)，量的是分派地板）、`rowchurn` 插入+删除成对
//!   （树节点到零回收的那条路）。表形状固定为 `(tag 索引, payload)`，由
//!   [`setup_row_table`] 建一劳永逸。
//! - `--op write`：`Setattr` 提交 data_ref，会动 refs 账本，是最重的写。
//! - `--conns N`：只把连接数堆到 N（配合 `--hold`）观察服务端 RSS。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, TableDef, COL_INDEX, COND_NONE, PUT_IF_ABSENT, ROW_PK,
};
use treedb::net::client::Client;

static ERRORS: AtomicU64 = AtomicU64::new(0);

struct Args {
    addr: String,
    op: String,
    conns: usize,
    threads: usize,
    secs: u64,
    pipeline: usize,
    hold: bool,
    views: usize,
    /// `rowrange` 的区间宽度（键数）。小于页宽（100）时一页装完，大于页宽才会
    /// 走到「区间没走完」那一支。
    range: u64,
    /// `write` / `lock` 打的 inode。默认 1（根）；给一个中段 ino 就能造出
    /// 「只改正文中段某个节点」的换代负载（`benches/replica_redo.sh` 的 [mid]）。
    ino: u64,
    /// 视图名里的线程号。不给 = 用线程自己的编号（`bench{t}v{v}`，各线程互不
    /// 干扰）。给了 = 所有线程共用一个视图（`bench{N}v{v}`）—— 多线程把数据都
    /// 堆进同一棵子树，节点在快照里按 `(view, ino)` 排，于是「改中段 ino」真的
    /// 落在正文中段；不共用时每个视图只占正文的 1/视图数，中段 ino 也在很前面。
    view_tag: Option<u32>,
    /// 集群共享密钥：给了就每条连接先跑一次握手（§4.4）。`None` = 不握手，
    /// 与旧版逐字一致 —— 历史基线数字因此仍然可以直接对比。
    auth: Option<std::sync::Arc<treedb::net::auth::Key>>,
}

fn parse() -> Args {
    let a: Vec<String> = std::env::args().collect();
    let mut args = Args {
        addr: "127.0.0.1:7777".into(),
        op: "create".into(),
        conns: 8,
        threads: 8,
        secs: 5,
        pipeline: 1,
        hold: false,
        views: 8,
        range: 64,
        ino: 1,
        view_tag: None,
        auth: None,
    };
    let mut i = 1;
    while i < a.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            a.get(*i).cloned().unwrap_or_default()
        };
        match a[i].as_str() {
            "--addr" => args.addr = next(&mut i),
            "--op" => args.op = next(&mut i),
            "--conns" => args.conns = next(&mut i).parse().unwrap_or(8),
            "--threads" => args.threads = next(&mut i).parse().unwrap_or(8),
            "--secs" => args.secs = next(&mut i).parse().unwrap_or(5),
            "--pipeline" => args.pipeline = next(&mut i).parse().unwrap_or(1),
            "--views" => args.views = next(&mut i).parse().unwrap_or(8),
            "--range" => args.range = next(&mut i).parse().unwrap_or(64),
            "--ino" => args.ino = next(&mut i).parse().unwrap_or(1),
            "--view-tag" => args.view_tag = next(&mut i).parse().ok(),
            "--hold" => args.hold = true,
            "--auth-key-file" => {
                let p = next(&mut i);
                match treedb::net::auth::Key::load(std::path::Path::new(&p)) {
                    Ok(k) => args.auth = Some(std::sync::Arc::new(k)),
                    Err(e) => {
                        eprintln!("bad --auth-key-file: {e}");
                        std::process::exit(2);
                    }
                }
            }
            "-h" | "--help" => {
                println!("{HELP}");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg {other}\n{HELP}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    args
}

fn pct(mut v: Vec<u64>) -> (u64, u64, u64, u64) {
    if v.is_empty() {
        return (0, 0, 0, 0);
    }
    v.sort_unstable();
    let at = |q: f64| v[((v.len() as f64 - 1.0) * q) as usize];
    (at(0.50), at(0.90), at(0.99), *v.last().unwrap())
}

/// 本次进程的唯一运行标识：进程启动时的纳秒数低位。
fn nonce() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64 % 100_000)
        .unwrap_or(0)
}

// ---------------------------------------------------------------- 通用表档

/// 基准用的表名。固定一个而不带 nonce：行键里已经带了 nonce，而反复跑同一条
/// 基准要能命中上一轮已经预热好的那张表 —— 每次重建一遍等于把建表成本混进
/// 第一格。
const ROW_TABLE: &[u8] = b"benchkv";
/// 索引列下标。列序固定为 `(tag, payload)`，所以 tag 恒为 0。
const ROW_TAG: u32 = 0;

/// 基准表的形状：`tag` 可索引（`rowidx` 走它），`payload` 无约束。
///
/// 只有两列是刻意的：再加列只会把每行的字节数变大，而这里要量的是「表这条
/// 路径」的常数开销（定位 / 索引维护 / 帧编解码），不是某种真实业务的行宽。
fn row_table_def() -> TableDef {
    TableDef {
        name: ROW_TABLE.to_vec(),
        columns: vec![
            ColumnDef { name: b"tag".to_vec(), flags: COL_INDEX },
            ColumnDef { name: b"payload".to_vec(), flags: 0 },
        ],
    }
}

/// 建表（或确认已有的那张就是本轮的形状）。
///
/// 「已存在」不能默默放过：上一轮用别的列序建的表会让后续每一个 `RowPut` 都
/// `EINVAL`，而那时报出来的错在压测输出里只是一行 `errors=NNN`，查起来要绕
/// 一大圈。形状不同就在建连阶段直接退出并说清差异。
fn setup_row_table(addr: &str, auth: Option<&treedb::net::auth::Key>) {
    let mut c = Client::connect_auth(addr, 3000, auth).expect("connect for table setup");
    match c.call(&Op::TableCreate { def: row_table_def() }) {
        Ok(Reply::Table(Some(_))) => {}
        Ok(Reply::Err { code, .. }) if code == errno::EEXIST => {
            match c.call(&Op::TableGet { name: ROW_TABLE.to_vec() }) {
                Ok(Reply::Table(Some(d))) if d == row_table_def() => {}
                Ok(Reply::Table(Some(d))) => {
                    eprintln!(
                        "table {} already exists with a different shape ({} columns); \
                         drop it or use another --dir",
                        String::from_utf8_lossy(ROW_TABLE),
                        d.columns.len()
                    );
                    std::process::exit(2);
                }
                other => {
                    eprintln!("table get: {other:?}");
                    std::process::exit(1);
                }
            }
        }
        Ok(other) => {
            eprintln!("table create: {other:?}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("table create: {e}");
            std::process::exit(1);
        }
    }
}

/// 一行的主键：定宽零填充（`r000000`）。
///
/// 定宽是 `rowrange` 的需要：区间两端就是两个主键字符串，不定宽的话字节序
/// 与数字序不一致（`r10` 排在 `r2` 前面），「宽 64 个键的窗口」到底装了几行
/// 要看 span —— 而这一格量的正是「扫描 + 回包」的单位成本，窗宽必须可预测。
fn row_key(i: u64) -> Vec<u8> {
    format!("r{i:06}").into_bytes()
}

/// 一行的样子：键 + `(tag, payload)`。`tag` 只有 16 个取值，让索引列上每页
/// 都有多个命中（否则 `rowidx` 量到的全是「索引里只有一条」那条快路径）。
fn row_op(seq: u64, span: u64) -> Op {
    Op::RowPut {
        table: ROW_TABLE.to_vec(),
        key: row_key(seq % span),
        cols: vec![
            format!("t{}", seq % 16).into_bytes(),
            format!("p{seq}").into_bytes(),
        ],
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn main() {
    let args = parse();
    // 探活不走 `Client::ping_auth`：它只回 `bool`，能把“对端拒绝降级”与
    // “MAC 不对”与“端口没人听”归成同一个结果，而这三件事的下一步运维动作
    // 完全不同。这里自己跑一次握手 + Ping，把真实错误带进文案。
    match Client::connect_auth(&args.addr, 3000, args.auth.as_deref())
        .and_then(|mut c| c.call(&Op::Ping))
    {
        Ok(Reply::Pong) => {}
        Ok(other) => {
            eprintln!("unexpected reply from {}: {other:?}", args.addr);
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("server not usable at {}: {e}", args.addr);
            eprintln!(
                "  a server started with --auth-key-file needs the same key here:\n  \
                 no key    → EADV on every data op\n  \
                 wrong key → handshake mac fails\n  \
                 neither case silently downgrades to unauthenticated"
            );
            std::process::exit(1);
        }
    }

    if args.hold {
        // 连接轴：只把 N 条连接打开并保持，让外部脚本量服务端 RSS。
        //
        // 建连那一段单独报一个时间：它是「成批重连」场景里唯一多出来的开销
        // （每条连接多两次往返 + 两次 HMAC），而下面那个存活确认的 ping 循环
        // 与认证无关。两个数分开打，脚本才能把握手的代价从建连里剥出来。
        let t_conn = Instant::now();
        let mut cs: Vec<Client> = Vec::with_capacity(args.conns);
        for _ in 0..args.conns {
            match Client::connect_auth(&args.addr, 0, args.auth.as_deref()) {
                Ok(c) => cs.push(c),
                Err(e) => {
                    eprintln!("opened {} of {} before failure: {e}", cs.len(), args.conns);
                    break;
                }
            }
        }
        println!(
            "connected {} conns in {} ms",
            cs.len(),
            t_conn.elapsed().as_millis()
        );
        // 每条连接确认一次存活，保证 fd 真在服务端注册了。
        for c in cs.iter_mut() {
            let _ = c.call(&Op::Ping);
        }
        println!(
            "held {} connections for {} s",
            cs.len(),
            args.secs.max(30)
        );
        std::thread::sleep(Duration::from_secs(args.secs.max(30)));
        return;
    }

    // 通用表档：表是全局的（不在视图里），所以先把它建好再开线程 —— 让「建表」
    // 这件事发生在测量窗口之外，且八条线程不会抢着建同一张表。
    let row_axis = args.op.starts_with("row");
    if row_axis {
        setup_row_table(&args.addr, args.auth.as_deref());
    }

    // 测量窗口从**每个工作线程预热完成之后**才起算：把 deadline 在外层算好传
    // 进去，一旦预热（建视图 + 灌 500×views 个文件）比 secs 还长，整个循环会
    // 一次都不跑，量出 `0 ops/s` 这种假数字。
    let dur = Duration::from_secs(args.secs);
    let mut handles = Vec::new();
    for t in 0..args.threads {
        let a = args.addr.clone();
        let op = args.op.clone();
        let per_conn = (args.conns / args.threads).max(1);
        let pipeline = args.pipeline;
        let views = args.views;
        let range = args.range;
        let ino = args.ino;
        let view_tag = args.view_tag;
        let auth = args.auth.clone();
        let nonce = nonce();
        handles.push(std::thread::spawn(move || {
            let mut cs: Vec<Client> = Vec::with_capacity(per_conn);
            for _ in 0..per_conn {
                cs.push(Client::connect_auth(&a, 0, auth.as_deref()).expect("connect"));
            }
            // 每线程独占一批视图，避免跨线程名字冲突污染语义。
            //
            // 通用表档不建视图：表是全局的，`row*` 那几条 op 的 `view` 字段根本
            // 不存在。建 64 个视图只会把建视图的开销混进预热，同时让服务端多攒
            // 64 份视图状态 —— 而这一格要量的是表那条路径。
            let mut vids: Vec<u32> = Vec::with_capacity(if row_axis { 0 } else { views });
            let mut missing: Vec<String> = Vec::new();
            if !row_axis {
                for v in 0..views {
                    let tag = view_tag.unwrap_or(t as u32);
                    let name = format!("bench{tag}v{v}");
                    match cs[0].call(&Op::CreateView {
                        name: name.clone().into_bytes(),
                    }) {
                        Ok(Reply::View { id, .. }) => vids.push(id),
                        // 视图已在上一轮建过：这不是错误，压测脚本要能反复跑。
                        // 记住名字，稍后一次性 ListViews 把 id 查回来。
                        Ok(Reply::Err { code, .. }) if code == errno::EEXIST => {
                            missing.push(name);
                        }
                        Ok(other) => {
                            eprintln!("create view: {other:?}");
                            std::process::exit(1);
                        }
                        Err(e) => {
                            eprintln!("create view: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                if !missing.is_empty() {
                    let Ok(Reply::Views(all)) = cs[0].call(&Op::ListViews) else {
                        eprintln!("视图已存在但 ListViews 没答上来");
                        std::process::exit(1);
                    };
                    for name in &missing {
                        let hit = all.iter().find(|(n, _, _)| n.as_slice() == name.as_bytes());
                        match hit {
                            Some((_, id, _)) => vids.push(*id),
                            None => {
                                eprintln!("视图 {name} 报 EEXIST 却查不到");
                                std::process::exit(1);
                            }
                        }
                    }
                }
            }
            let mut lat: Vec<u64> = Vec::with_capacity(1 << 16);
            let mut seq: u64 = 0;
            let mut n: u64 = 0;
            let mut ci = 0usize;
            // lookup/readdir 得先有东西可查：预热若干文件，否则量的是“找
            // 不中”这条快路径，数字好看但没代表性。条数由 `TREEDB_BENCH_WARM`
            // 定（默认 500，与历史格子同形）：目录布局那类优化的收益随目录规模
            // 长，500 条全部活在 L2 里，根目录到叶子只三跳，分不出两种布局的差。
            //
            // 预热走 pipeline：逐条一问一答时，这一步自己就要几十秒（每条一次
            // fsync 往返），盖过被测区间；挨批送则只花 fsync 次数那么多时间。
            if op == "lookup" || op == "readdir" {
                const WARM: usize = 250;
                let span = warm_span();
                for &v in &vids {
                    for base in (0..span).step_by(WARM) {
                        let batch: Vec<Op> = (base..(base + WARM).min(span))
                            .map(|k| Op::Create {
                                view: v,
                                parent: 1,
                                name: format!("f{t}-{k}").into_bytes(),
                                mode: 0o644,
                                uid: 1000,
                                gid: 1000,
                            })
                            .collect();
                        // EEXIST = 上一轮已经预热过，正是要的状态，不能当失败。
                        for r in cs[0].pipeline(&batch).expect("warmup pipeline") {
                            if let Reply::Err { code, ref msg } = r {
                                assert!(
                                    code == errno::EEXIST,
                                    "warmup create 失败 code={code} msg={}",
                                    String::from_utf8_lossy(msg)
                                );
                            }
                        }
                    }
                }
            }
            // 通用表同理：`rowput` 要量的是覆盖、`rowget`/`rowlist`/`rowidx` 要量
            // 命中，都得先有行。`rowinsert` 与 `rowchurn` 自己造行（键里带 nonce），
            // 不预热 —— 给它们预热只会把「预热那 500 行」也算进索引规模里。
            if matches!(
                op.as_str(),
                "rowput" | "rowget" | "rowlist" | "rowidx" | "rowrange" | "rowidxrange" | "rowcount"
            ) {
                const WARM: u64 = 250;
                let span = warm_span() as u64;
                for base in (0..span).step_by(WARM as usize) {
                    let batch: Vec<Op> = (base..(base + WARM).min(span))
                        .map(|k| row_op(k, span))
                        .collect();
                    for r in cs[0].pipeline(&batch).expect("row warmup pipeline") {
                        if let Reply::Err { code, ref msg } = r {
                            panic!(
                                "row warmup 失败 code={code} msg={}",
                                String::from_utf8_lossy(msg)
                            );
                        }
                    }
                }
            }
            // 命中校验：四条「查」的 op 在空表上同样零错误 —— `RowGet` 未命中回的
            // 是 `Reply::Row(None)`、`RowList` 回空行集、`RowCount` 回 0，三者都
            // 不是 `Err`。于是一张空表能跑出「几百万 ops/s、errors=0」这种最好看
            // 也最没用的数字。预热刚写完 span 行，这里必须查得到；查不到就说明
            // 键的构造与预热不一致（改过 span 或键格式），宁可在这儿炸掉。
            if matches!(
                op.as_str(),
                "rowget" | "rowlist" | "rowidx" | "rowrange" | "rowidxrange" | "rowcount"
            ) {
                let (what, probe) = match op.as_str() {
                    "rowget" => (
                        "RowGet(r000000)",
                        Op::RowGet {
                            table: ROW_TABLE.to_vec(),
                            key: row_key(0),
                        },
                    ),
                    "rowlist" => (
                        "RowList(pk, limit=1)",
                        Op::RowList {
                            table: ROW_TABLE.to_vec(),
                            column: ROW_PK,
                            value: Vec::new(),
                            from: Vec::new(),
                            limit: 1,
                        },
                    ),
                    "rowidx" => (
                        "RowList(tag=t0, limit=1)",
                        Op::RowList {
                            table: ROW_TABLE.to_vec(),
                            column: ROW_TAG,
                            value: b"t0".to_vec(),
                            from: Vec::new(),
                            limit: 1,
                        },
                    ),
                    "rowidxrange" => (
                        "RowRange(tag [t0, t1), limit=1)",
                        Op::RowRange {
                            table: ROW_TABLE.to_vec(),
                            column: ROW_TAG,
                            lo: b"t0".to_vec(),
                            hi: b"t1".to_vec(),
                            after_val: Vec::new(),
                            after_key: Vec::new(),
                            limit: 1,
                        },
                    ),
                    "rowrange" => (
                        "RowRange(pk [r000000, r000001), limit=1)",
                        Op::RowRange {
                            table: ROW_TABLE.to_vec(),
                            column: ROW_PK,
                            lo: row_key(0),
                            hi: row_key(1),
                            after_val: Vec::new(),
                            after_key: Vec::new(),
                            limit: 1,
                        },
                    ),
                    _ => (
                        "RowCount(pk)",
                        Op::RowCount {
                            table: ROW_TABLE.to_vec(),
                            column: ROW_PK,
                            value: Vec::new(),
                        },
                    ),
                };
                let hit = match cs[0].call(&probe) {
                    Ok(Reply::Row(Some(_))) => true,
                    Ok(Reply::Rows { rows, .. }) => !rows.is_empty(),
                    Ok(Reply::Count(n)) => n > 0,
                    Ok(other) => {
                        eprintln!("{what}: unexpected reply {other:?}");
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("{what}: {e}");
                        std::process::exit(1);
                    }
                };
                // 不用 `assert!`：panic 只会在这个线程里炸，主线程再 `join`
                // 一次，看到的是不透明的 `Any { .. }`，真正的文案被冲掉。
                if !hit {
                    eprintln!("{what} 预热后仍查不到行：这一格会量成「空表快路径」");
                    std::process::exit(2);
                }
            }
            // 计时窗口只包被测循环。分母不能在 join 前取：那样“建 N 条连接 +
            // 预热 500 个文件”这段固定开销会被算进去，--secs 3 时 readdir/lookup
            // 那一格的分母实测被撑到 6.2 s，吞吐虚低一倍；而旧版 pgbench 因为
            // 预热在父线程做完，同一处只撑到 3.05 s —— 两边口径不一致，对比结果
            // 就不可信。故每线程自己记窗口，汇总时取最大值当分母（比取均值
            // 保守），treedb-pgbench 同步改成这个口径。
            let wstart = Instant::now();
            let deadline = wstart + dur;
            while Instant::now() < deadline {
                // 通用表档没有视图（`vids` 为空），传 0 给 build 即可 —— 那几条
                // op 的帧里没有 view 字段。
                let view = if vids.is_empty() {
                    0
                } else {
                    vids[(seq as usize) % vids.len()]
                };
                let batch = build(
                    &op,
                    view,
                    seq,
                    pipeline,
                    t,
                    nonce,
                    warm_span() as u64,
                    range,
                    ino,
                );
                let idx = ci % cs.len();
                ci += 1;
                let c = &mut cs[idx];
                let t0 = Instant::now();
                let reps = if pipeline == 1 {
                    c.call(&batch[0]).map(|r| vec![r])
                } else {
                    c.pipeline(&batch)
                };
                match reps {
                    Ok(rs) => {
                        let us = t0.elapsed().as_micros() as u64;
                        for r in &rs {
                            if matches!(r, Reply::Err { .. }) {
                                ERRORS.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        let per = us / rs.len() as u64;
                        for _ in 0..rs.len() {
                            lat.push(per);
                        }
                        n += rs.len() as u64;
                    }
                    Err(e) => {
                        ERRORS.fetch_add(1, Ordering::Relaxed);
                        eprintln!("round failed: {e}");
                        break;
                    }
                }
                seq += pipeline as u64;
            }
            (n, lat, wstart.elapsed())
        }));
    }

    let mut total = 0u64;
    let mut all: Vec<u64> = Vec::new();
    let mut span = Duration::ZERO;
    for h in handles {
        let (n, lat, s) = h.join().unwrap();
        total += n;
        all.extend(lat);
        if s > span {
            span = s;
        }
    }
    let secs = span.as_secs_f64();
    let (p50, p90, p99, max) = pct(all);
    println!(
        "op={op} conns={c} threads={t} pipeline={p} → {q:.0} ops/s over {s:.2}s \
         | lat us: p50={p50} p90={p90} p99={p99} max={max} | errors={e}",
        op = args.op,
        c = args.conns,
        t = args.threads,
        p = args.pipeline,
        q = total as f64 / secs,
        s = secs,
        e = ERRORS.load(Ordering::Relaxed),
    );
}

/// 预热条数 = 被测目录的规模。默认 500（历史格子的形），`TREEDB_BENCH_WARM`
/// 可拉到万级，用来分辨“只在宽目录上生效”的那类优化。
fn warm_span() -> usize {
    std::env::var("TREEDB_BENCH_WARM")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n| *n >= 100)
        .unwrap_or(500)
}

/// 造一批 op。名字带 `seq` 保证 create 唯一。
///
/// `nonce` 是本次进程的启动时间：没有它，同一数据目录上重跑 create/key 就
/// 全在撞 EEXIST，量到的是“已存在”这条早退路径，不是真的建项开销。
fn build(
    op: &str,
    view: u32,
    seq: u64,
    n: usize,
    t: usize,
    nonce: u64,
    span: u64,
    rw: u64,
    ino: u64,
) -> Vec<Op> {
    let mut v = Vec::with_capacity(n);
    for k in 0..n as u64 {
        let s = seq + k;
        v.push(match op {
            "ping" => Op::Ping,
            "stat" => Op::Stats,
            "lookup" => Op::Lookup {
                view,
                parent: 1,
                name: format!("f{}-{}", t, s % span).into_bytes(),
            },
            "readdir" => Op::ReadDir {
                view,
                parent: 1,
                offset: 0,
                limit: 100,
            },
            "write" => Op::Setattr {
                view,
                ino,
                valid: 1 << 6, // SA_DATA_REF
                mode: 0,
                uid: 0,
                gid: 0,
                size: 4096,
                atime: 0,
                mtime: 0,
                // 与 `treedb-pgbench` 的 `write` 同一条键（那边绑的是
                // `"1:4096:dev{}".format((seq+k) % 997)`）：两边都是 997 个不同的
                // 内容对象在轮转，账本行的热度一致，数字才可直接对拉。
                data_ref: format!("1:4096:dev{}", s % 997).into_bytes(),
                ref_bytes: 4096,
            },
            "lock" => Op::LockSet {
                view,
                ino,
                owner: format!("owner{t}").into_bytes(),
                verb: 0,
                kind: 1,
                lease_expiry: 0,
            },
            // ---- 通用表 ----
            // `rowinsert` 的键带 nonce：重跑时不撞上一轮的键（撞上就不是插入而是
            // 覆盖，量的将是另一条路径）。
            "rowinsert" => Op::RowPut {
                table: ROW_TABLE.to_vec(),
                key: format!("n{nonce}-{t}-{s}").into_bytes(),
                cols: vec![format!("t{}", s % 16).into_bytes(), b"p".to_vec()],
                flags: PUT_IF_ABSENT,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
            "rowput" => row_op(s, span),
            "rowget" => Op::RowGet {
                table: ROW_TABLE.to_vec(),
                key: row_key(s % span),
            },
            // 主键序第一页：`from` 空 = 从头。固定首页而不翻到底，是因为翻页的
            // 成本与「翻到第几页」无关（键游标直接从树上定位），而首页恰好是
            // `rowlist` 最常用的形状（挂载后第一次列目录）。
            "rowlist" => Op::RowList {
                table: ROW_TABLE.to_vec(),
                column: ROW_PK,
                value: Vec::new(),
                from: Vec::new(),
                limit: 100,
            },
            // 二级索引等值页：16 个 tag 轮转，每个 tag 在 500 行里约 31 行 ——
            // 既不是「只命中一条」的快路径，也不会大到把回包帧长测成瓶颈。
            "rowidx" => Op::RowList {
                table: ROW_TABLE.to_vec(),
                column: ROW_TAG,
                value: format!("t{}", s % 16).into_bytes(),
                from: Vec::new(),
                limit: 100,
            },
            "rowcount" => Op::RowCount {
                table: ROW_TABLE.to_vec(),
                column: ROW_PK,
                value: Vec::new(),
            },
            // 主键区间 `[lo, hi)`：窗口在整张表上滑着走，宽度 = `--range`，
            // 页宽固定 100。窗宽 < 100 时一页装完（`end=true`），> 100 才走到
            // 「区间没走完」那一支 —— 两档的差就是 `end` 判定与满页循环的价。
            //
            // `win` 夹到 `span - 1`：窗口与表一样宽时 `start` 只有一个取值，
            // 量到的就不再是「扫描一段」而是「扫全表」。
            "rowrange" => {
                let win = rw.min(span.saturating_sub(1)).max(1);
                let start = s % (span - win + 1);
                Op::RowRange {
                    table: ROW_TABLE.to_vec(),
                    column: ROW_PK,
                    lo: row_key(start),
                    hi: row_key(start + win),
                    after_val: Vec::new(),
                    after_key: Vec::new(),
                    limit: 100,
                }
            }
            // 索引列值区间 `[t0, t1)`：`t0` 这一段在 500 行里约 31 行，与
            // `rowidx` 的回包一样大 —— 两格差别只在「等值前缀」与「值区间 +
            // 逐行 take_while」这两条路径本身。不拿 `--range` 去切 tag：16 个
            // 值切不出「宽 64」这种窗口，硬切只会让两档语料不可比。
            "rowidxrange" => Op::RowRange {
                table: ROW_TABLE.to_vec(),
                column: ROW_TAG,
                lo: b"t0".to_vec(),
                hi: b"t1".to_vec(),
                after_val: Vec::new(),
                after_key: Vec::new(),
                limit: 100,
            },
            // 插入 + 删除成对：树每轮长一格又缩回去，量的是「Delete 把树节点
            // 减到零」那条路（包括索引侧的反向删除），而树规模恒定。
            //
            // 配对靠 s 的奇偶：偶数下 put、奇数下 del 同一个键 —— 当 pipeline=1
            // 时两轮一对，pipeline=N 时同一批里先 put 后 del，次序由协议 FIFO
            // 保证。所以报告的 ops/s 是 **put+del 之和**，配对速率是它的一半。
            "rowchurn" => {
                let key = format!("c{nonce}-{t}-{}", s / 2).into_bytes();
                if s % 2 == 0 {
                    Op::RowPut {
                        table: ROW_TABLE.to_vec(),
                        key,
                        cols: vec![b"tchurn".to_vec(), b"p".to_vec()],
                        flags: 0,
                        cond_col: COND_NONE,
                        cond_val: Vec::new(),
                    }
                } else {
                    Op::RowDelete {
                        table: ROW_TABLE.to_vec(),
                        key,
                        flags: 0,
                        cond_col: COND_NONE,
                        cond_val: Vec::new(),
                    }
                }
            }
            _ => Op::Create {
                view,
                parent: 1,
                name: format!("f{t}-{nonce}-{s}").into_bytes(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        });
    }
    v
}

const HELP: &str = "\
treedb-bench — 对 treedb-server 打负载

  --addr HOST:PORT   服务端地址（默认 127.0.0.1:7777）
  --op NAME          create|lookup|readdir|write|lock|ping|stat
                     |rowinsert|rowput|rowget|rowlist|rowidx|rowcount|rowchurn
                     |rowrange|rowidxrange
                     通用表档用固定形状的表 benchkv(tag 索引, payload)，首次运行
                     自动建表；rowchurn 是 put+del 成对发，ops/s 是两者之和、
                     配对速率是它的一半。rowrange 扫主键区间 [lo, hi)（窗宽
                     --range，页宽 100），rowidxrange 扫 tag 列的值区间 [t0, t1)
  --conns N          总连接数（默认 8）
  --threads N        工作线程数（默认 8）
  --pipeline N       每批请求数，1 = 一问一答（默认 1）
  --secs N           持续秒数（默认 5）
  --views N          每线程视图数（默认 8）
  --range N          rowrange 的区间宽度（默认 64）
  --ino N            write/lock 打的 inode（默认 1 = 根）。给一个中段 ino 就是
                     「只改正文中段某个节点」的换代负载（replica_redo.sh 的 [mid]）
  --view-tag N       视图名用 bench<N>v* 而不是 bench<线程号>v*：所有线程共用
                     一个视图。造数据时用它把整份快照压进一棵子树，中段 ino 才
                     真落在正文中段
  --hold             只建立 N 条连接并保持，用于量服务端 RSS
  --auth-key-file P  每条连接先用这把密钥跑一次握手（§4.4）。不给 = 不握手。
                     服务端必须开着同一把密钥，否则报错退出（客户端
                     fail-closed，不会静默降级成不认证）。与 --hold 合用
                     就是「建连风暴里握手占多少」的口径
";
