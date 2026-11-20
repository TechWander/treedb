//! 第三个对照臂：同样的元数据 op 打到 SQLite3，给 treedb / PG 两张表补上
//! 「同机、同盘、同操作」的第三条分母。
//!
//! 为什么要有它：PG 与 treedb 的对比里，「treedb 赢在哪」与「PG 输在哪」混在
//! 一起就分不清归因——PG 每次写要过 shared_buffers + WAL + 后台写进程，而
//! treedb 是一次 `write` + 一次 `fdatasync`。SQLite 的写路径（页缓存 + WAL +
//! `synchronous=FULL`）正好落在两者之间：它没有服务端进程与网络，但有真正的
//! 页管理与事务提交。三臂齐了，「引擎效率」与「进程模型」这两个因子才分得开。
//!
//! ## 口径（与 `treedb-pgbench` 逐项对齐，不对齐就没有可比性）
//!
//! - 数据集：每视图 500 个 `w-{k}`，视图表名 `node_v{v}`（PG 侧是 schema，
//!   这里是一张表：SQLite 没有 schema 命名空间，`PRAGMA` 的 `main.` 是库名）。
//! - 计时分母：每连接线程**连上并设完 PRAGMA 之后**才起算窗口，汇总取各线程
//!   窗口的最大值。父线程取 t0 会把建连算进分母（DESIGN §7.5 的那个教训）。
//! - ops 计数：返回**真正执行掉的语句数**。读 op 只跑 1 条时按 1 计，不按
//!   `pipeline` 计。
//! - `--pipeline N` = N 条语句塞进一个事务（与 PG 臂同一个定义：一次
//!   commit 摊给 N 个 op）。这与 `treedb-bench` 的「一次往返发 N 个请求」不是
//!   同一个机制：SQLite/PG 侧省的是 fsync 次数，treedb 侧省的是往返 + 服务端
//!   每轮的 fsync 次数，两边都摊到了同一笔账上，但实现归属不同，报告里要写明。
//! - 落盘强度：`journal_mode=WAL` + `synchronous=FULL`，即每次 commit 都等
//!   fsync，与 PG 的 `fsync=on` + `synchronous_commit=on`、treedb 的
//!   `--durability group` 同级。
//! - 写事务用 `BEGIN IMMEDIATE`：SQLite 的写锁是库级的，延迟事务在升级写锁
//!   时若撞上别人就立刻 `SQLITE_BUSY` 而不是等（防死锁），那会把「排队」量成
//!   「失败」。`busy_timeout=10000` + IMMEDIATE 让 64 连接的写档量到的是真
//!   排队时间，而不是一堆错误。
//!
//! 用法：
//! ```text
//! cargo build --release --features postgres-compat --bin treedb-sqlitebench
//! ./target/release/treedb-sqlitebench --path /tmp/x.db --op create --conns 8 --secs 3
//! ```

use rusqlite::{params, Connection, TransactionBehavior};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static ERRORS: AtomicU64 = AtomicU64::new(0);

struct Args {
    path: String,
    op: String,
    conns: usize,
    secs: u64,
    views: usize,
    pipeline: usize,
}

fn parse() -> Args {
    let a: Vec<String> = std::env::args().collect();
    let mut args = Args {
        path: "/tmp/treedb-sqlitebench.db".into(),
        op: "create".into(),
        conns: 8,
        secs: 5,
        views: 8,
        pipeline: 1,
    };
    let mut i = 1;
    while i < a.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            a.get(*i).cloned().unwrap_or_default()
        };
        match a[i].as_str() {
            "--path" => args.path = next(&mut i),
            "--op" => args.op = next(&mut i),
            "--conns" => args.conns = next(&mut i).parse().unwrap_or(8),
            "--secs" => args.secs = next(&mut i).parse().unwrap_or(5),
            "--views" => args.views = next(&mut i).parse().unwrap_or(8),
            "--pipeline" => args.pipeline = next(&mut i).parse().unwrap_or(1),
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

/// 视图号 → 表名。PG 侧是 `grovefs_v_<hex>` schema，这里是 `node_v{v}`。
fn table_for(v: usize) -> String {
    format!("node_v{v}")
}

/// 每连接都要设的 PRAGMA。**必须每条连接都设**：`journal_mode` 是库级（写进
/// 文件头），`synchronous` / `busy_timeout` / `cache_size` 是连接级，漏一条这
/// 个连接就是另一种落盘强度。
fn tune(c: &Connection) -> rusqlite::Result<()> {
    // 读回 journal_mode，落成 wal 才算数（老文件可能是 delete 模式）。
    let mode: String = c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(rusqlite::Error::InvalidQuery);
    }
    c.busy_timeout(Duration::from_millis(10_000))?;
    // FULL = 每次 commit 都 fdatasync（WAL 模式下是 fsync WAL）。
    c.pragma_update(None, "synchronous", "FULL")?;
    // 与 PG 的 shared_buffers=128MB 同一个量级（每连接 64 MiB 页缓存）。
    c.pragma_update(None, "cache_size", -65536i64)?;
    Ok(())
}

fn setup(c: &mut Connection, views: usize) {
    let t0 = Instant::now();
    for v in 0..views {
        let t = table_for(v);
        c.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {t} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                name TEXT NOT NULL,
                is_dir INTEGER NOT NULL DEFAULT 0,
                data_ref TEXT,
                link_target TEXT,
                mode INTEGER NOT NULL DEFAULT 0,
                uid INTEGER NOT NULL DEFAULT 0,
                gid INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER NOT NULL DEFAULT 0,
                atime INTEGER NOT NULL DEFAULT 0,
                ctime INTEGER NOT NULL DEFAULT 0,
                ino INTEGER NOT NULL DEFAULT 0
             );
             CREATE UNIQUE INDEX IF NOT EXISTS nn_{t} ON {t}(parent_id,name);
             CREATE INDEX IF NOT EXISTS np_{t} ON {t}(parent_id);"
        ))
        .unwrap_or_else(|e| panic!("setup {t}: {e}"));
    }
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS refs (
            data_ref TEXT PRIMARY KEY,
            holders INTEGER NOT NULL
         )",
    )
    .unwrap();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "setup {views} 个视图表耗时 {ms:.1} ms（每视图 {:.1} ms，不进 op QPS）",
        ms / views.max(1) as f64
    );
}

/// 预热：每视图 500 行，与 `treedb-bench` / `treedb-pgbench` 完全同一个数据集。
/// 名字统一 `w-{k}`、不带线程号：线程按 `t % views` 轮转表，混进线程号会让
/// `conns > views` 时查到没预热过的名字上。
fn warmup(c: &mut Connection, views: usize) {
    for v in 0..views {
        let t = table_for(v);
        let vals: Vec<String> = (0..500i64)
            .map(|k| format!("(1,'w-{k}',0,420,1000,1000,{k},{k},0,{k})"))
            .collect();
        c.execute(
            &format!(
                "INSERT OR IGNORE INTO {t}\
                 (parent_id,name,is_dir,mode,uid,gid,mtime,atime,ctime,ino) VALUES {}",
                vals.join(",")
            ),
            [],
        )
        .unwrap_or_else(|e| panic!("warmup {t}: {e}"));
    }
}

fn nonce() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64 % 1_000_000)
        .unwrap_or(0)
}

/// 一条 op（`n` = 一个事务里塞几条语句）。返回**真正执行掉的语句数**：外层按
/// 它计 ops，不能按 `n` 计（读类 op 只跑一条却按 batch 计数会虚高 `pipeline` 倍）。
fn one_op(
    c: &mut Connection,
    op: &str,
    table: &str,
    t: usize,
    seq: u64,
    n: usize,
    run: u64,
) -> rusqlite::Result<usize> {
    match op {
        "ping" => {
            let tx = c.transaction()?;
            for _ in 0..n {
                tx.execute("SELECT 1", [])?;
            }
            tx.commit()?;
            Ok(n)
        }
        "lookup" => {
            let s = format!(
                "SELECT id,name,is_dir,data_ref,mode,uid,gid FROM {table} \
                 WHERE parent_id=1 AND name=?1"
            );
            let tx = c.transaction()?;
            for k in 0..n as u64 {
                let name = format!("w-{}", (seq + k) % 500);
                let mut st = tx.prepare_cached(&s)?;
                let mut rows = st.query(params![name])?;
                // 必须真的 step 完：只 prepare 不 step 等于没跑这条查询。
                while rows.next()?.is_some() {}
            }
            tx.commit()?;
            Ok(n)
        }
        "readdir" => {
            let s = format!(
                "SELECT id,name,is_dir,data_ref,mode,uid,gid FROM {table} \
                 WHERE parent_id=1 ORDER BY name LIMIT 100"
            );
            let tx = c.transaction()?;
            for _ in 0..n {
                let mut st = tx.prepare_cached(&s)?;
                let mut rows = st.query([])?;
                while rows.next()?.is_some() {}
            }
            tx.commit()?;
            Ok(n)
        }
        "create" => {
            let s = format!(
                "INSERT INTO {table} \
                 (parent_id,name,is_dir,mode,uid,gid,mtime,atime,ctime,ino) \
                 VALUES (1,?1,0,420,1000,1000,?2,?2,0,?3)"
            );
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for k in 0..n as u64 {
                let name = format!("c{run}-{t}-{}", seq + k);
                let ts = (seq + k) as i64;
                tx.execute(&s, params![name, ts, ts])?;
            }
            tx.commit()?;
            Ok(n)
        }
        // 最重的写：改 inode 的 data_ref + 给共享账本记一个 holder，同一个事务。
        // ino 取预热那 500 行的 id（1..=500），保证 UPDATE 一定命中真实行。
        "write" => {
            let upd = format!("UPDATE {table} SET data_ref=?1, mtime=?2 WHERE id=?3");
            let ins = "INSERT INTO refs(data_ref,holders) VALUES (?1,1) \
                       ON CONFLICT (data_ref) DO UPDATE SET holders=holders+1";
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for k in 0..n as u64 {
                let d = format!("1:4096:dev{}", (seq + k) % 997);
                let ts = (seq + k) as i64;
                let ino = ((seq + k) % 500) as i64 + 1;
                tx.execute(&upd, params![d, ts, ino])?;
                tx.execute(ins, params![d])?;
            }
            // 一个逻辑 op = UPDATE + 账本一条，计 1：与 PG 臂同一口径。
            tx.commit()?;
            Ok(n)
        }
        other => unreachable!("unknown op {other}"),
    }
}

const OPS: &[&str] = &["ping", "lookup", "readdir", "create", "write"];

fn main() {
    let args = parse();
    if !OPS.contains(&args.op.as_str()) {
        eprintln!("op {} 不支持，可选：{OPS:?}", args.op);
        std::process::exit(2);
    }
    let run = nonce();

    let mut admin = Connection::open(&args.path).unwrap_or_else(|e| panic!("open {}: {e}", args.path));
    tune(&admin).unwrap();
    setup(&mut admin, args.views);
    warmup(&mut admin, args.views);
    // 预热完把页刷下去并做一次 checkpoint，否则测量窗口里那笔
    // 「WAL 从 0 开始涨」的活会算到第一个 cell 头上。三臂都做等价处理：
    // treedb 每格新建目录后由 bench 自己灌 500 条，PG 由 warmup 的提交落盘。
    let _: Result<(i64, i64, i64), _> =
        admin.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        });
    drop(admin);

    let mut handles = Vec::new();
    for t in 0..args.conns {
        let path = args.path.clone();
        let op = args.op.clone();
        let views = args.views;
        let secs = args.secs;
        handles.push(thread::spawn(move || {
            let mut c = Connection::open(&path).expect("open");
            tune(&c).expect("tune");
            let table = table_for(t % views);
            // 计时窗口从本线程连上并设完 PRAGMA 之后才起算：父线程取 t0 会把
            // 建连算进分母（DESIGN §7.5）。
            let wstart = Instant::now();
            let deadline = wstart + Duration::from_secs(secs);
            let mut lat: Vec<u64> = Vec::with_capacity(1 << 16);
            let mut n = 0u64;
            let mut seq = 0u64;
            while Instant::now() < deadline {
                let t0 = Instant::now();
                match one_op(&mut c, &op, &table, t, seq, args.pipeline, run) {
                    Ok(done) => {
                        let us = t0.elapsed().as_micros() as u64;
                        let per = us / done as u64;
                        for _ in 0..done {
                            lat.push(per);
                        }
                        n += done as u64;
                    }
                    Err(e) => {
                        // 主键冲突是记账问题（同一 run 内不该有），其余错误
                        // 必须计出来，否则 QPS 是假的。
                        let txt = format!("{e}");
                        if !txt.contains("UNIQUE constraint failed") {
                            ERRORS.fetch_add(1, Ordering::Relaxed);
                            eprintln!("op {op}: {txt}");
                        }
                    }
                }
                seq += args.pipeline as u64;
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
        "sqlite op={} conns={} pipeline={} → {:.0} ops/s over {:.2}s \
         | lat us: p50={} p90={} p99={} max={} | errors={}",
        args.op,
        args.conns,
        args.pipeline,
        total as f64 / secs,
        secs,
        p50,
        p90,
        p99,
        max,
        ERRORS.load(Ordering::Relaxed),
    );
    // 收尾把 WAL 并回主库，别让下一个 cell 背着上一格的 WAL 起跑
    // （测量窗口内不影响，但保留这个习惯能让手工复跑两次的结果可比）。
    if let Ok(c) = Connection::open(&args.path) {
        let _: Result<(i64, i64, i64), _> = c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        });
    }
}

const HELP: &str = "\
treedb-sqlitebench — 同样的元数据 op 打到 SQLite3（第二个母分母）

  --path PATH        库文件（每次测量请用全新文件；脚本负责删）
  --op NAME          ping|lookup|readdir|create|write
  --conns N          连接数 = 线程数（默认 8）
  --secs N           持续秒数（默认 5）
  --views N          预建视图表数（默认 8）
  --pipeline N       一个事务里塞 N 条语句（默认 1 = 每 op 一次 fsync）
";
