//! `treedb-pgbench`：把**同样的元数据操作**打到 PostgreSQL 上，给自研引擎的
//! 吞吐数字配一个同机、同操作语义的分母。
//!
//! 这不是"PG 不行"的表演，而是回答一个必须量化的问题：GroveFS 现在的路径
//! 就是 PG，换引擎的收益到底有多少落在 op QPS 上，多少只落在连接数上。
//!
//! SQL 刻意对齐 grovefs 的真实表结构（`node` / `grovefs_refs.holders` /
//! `grovefs_control.api_keys`，列名见 `src/schema.rs` 与 grovefs 的
//! `pg_store.rs` / `apikeys.rs`），并且**表名全限定**（`"grovefs_v_<hex>".node`）
//! 而不是 `SET LOCAL search_path`：后者要求每条连接按视图绑定 session 级
//! prepared statement，那正是 DESIGN §2.1 里被拒掉的那条路，混进对照只会让
//! 数字变成"池化策略之争"而不是"op 成本之争"。
//!
//! 事务边界对齐语义：
//! - `create` / `write` / `lock` / `key` 各包一个 `BEGIN…COMMIT`，COMMIT 默认
//!   `synchronous_commit=on`，即**每次都等 fsync** —— 与 treedb 的 `always` 档
//!   同级，比 `group` 档更保守。
//! - `lookup` / `readdir` 用只读事务（与 grovefs 读路径一致）。
//! - `--pipeline N` 在这里表示"N 条语句塞进一个事务"，即把一次 fsync 摊给 N
//!   个 op；treedb 侧对应的旋钮是服务端 group commit，不是客户端攒批。
//!
//! 用法（先起一个**私有**集群，见 `benches/pg_conn_cost.sh`）：
//! ```text
//! cargo build --release --bin treedb-pgbench
//! ./target/release/treedb-pgbench --uri postgresql://postgres@127.0.0.1:54390/postgres \
//!     --op create --conns 8 --secs 5
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static ERRORS: AtomicU64 = AtomicU64::new(0);

struct Args {
    uri: String,
    op: String,
    conns: usize,
    secs: u64,
    views: usize,
    pipeline: usize,
}

fn parse() -> Args {
    let a: Vec<String> = std::env::args().collect();
    let mut args = Args {
        uri: "postgresql://postgres@127.0.0.1:54390/postgres".into(),
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
            "--uri" => args.uri = next(&mut i),
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

/// 视图名 → schema 名，与 `treedb::schema::encode` 同一规则（hex 编码）。
fn schema_for(view: &str) -> String {
    let mut hex = String::with_capacity(view.len() * 2);
    for b in view.as_bytes() {
        hex.push_str(&format!("{b:02x}"));
    }
    format!("grovefs_v_{hex}")
}

/// 建出 `views` 个视图 schema。这是**挂载一个视图**在 PG 路径上的真实代价，
/// 单独计时打印，因为它每视图只发生一次，不进 op QPS。
fn setup(client: &mut postgres::Client, views: usize) -> Vec<String> {
    let mut tables: Vec<String> = Vec::with_capacity(views);
    let t0 = Instant::now();
    for v in 0..views {
        let s = schema_for(&format!("benchv{v}"));
        let q = format!("\"{s}\"");
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {q};
                 CREATE TABLE IF NOT EXISTS {q}.node (
                    id BIGSERIAL PRIMARY KEY,
                    parent_id BIGINT,
                    name TEXT NOT NULL,
                    is_dir BIGINT NOT NULL DEFAULT 0,
                    data_ref TEXT,
                    link_target TEXT,
                    mode BIGINT NOT NULL DEFAULT 0,
                    uid BIGINT NOT NULL DEFAULT 0,
                    gid BIGINT NOT NULL DEFAULT 0,
                    mtime BIGINT NOT NULL DEFAULT 0,
                    atime BIGINT NOT NULL DEFAULT 0,
                    ctime BIGINT NOT NULL DEFAULT 0,
                    ino BIGINT NOT NULL DEFAULT 0
                 );
                 CREATE UNIQUE INDEX IF NOT EXISTS nn_{s} ON {q}.node(parent_id, name);
                 CREATE INDEX IF NOT EXISTS np_{s} ON {q}.node(parent_id);
                 CREATE TABLE IF NOT EXISTS {q}.locks (
                    id BIGSERIAL PRIMARY KEY,
                    ino BIGINT NOT NULL,
                    owner TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    lease_expiry BIGINT NOT NULL
                 );
                 CREATE SCHEMA IF NOT EXISTS grovefs_refs;
                 CREATE TABLE IF NOT EXISTS grovefs_refs.holders (
                    data_ref TEXT PRIMARY KEY,
                    holders BIGINT NOT NULL
                 );
                 CREATE SCHEMA IF NOT EXISTS grovefs_control;
                 CREATE TABLE IF NOT EXISTS grovefs_control.api_keys (
                    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    id TEXT NOT NULL UNIQUE,
                    label TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_used_at TEXT,
                    revoked_at TEXT,
                    scope TEXT NOT NULL DEFAULT 'any',
                    view_name TEXT,
                    ro BOOLEAN NOT NULL DEFAULT false,
                    owner TEXT,
                    note TEXT,
                    expires_at TEXT,
                    created_by TEXT,
                    secret_hint TEXT
                 );"
            ))
            .unwrap_or_else(|e| panic!("setup {s}: {e}"));
        tables.push(q);
    }
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "setup {views} 个视图 schema 耗时 {ms:.1} ms（每视图 {:.1} ms，不进 op QPS）",
        ms / views.max(1) as f64
    );
    tables
}

/// `lookup` / `readdir` 的预热：每个视图落 500 个文件，和 treedb-bench 用完全
/// 一样的数据集，否则两边量的都是"查不中"这条快路径。
///
/// 名字不带线程号（统一 `w-{k}`）：线程按 `t % views` 轮转，一旦前缀混进线程
/// 号，`conns > views` 时 lookup 就会查到没预热过的名字上。
fn warmup(client: &mut postgres::Client, table: &str) {
    let vals: Vec<String> = (0..500i64)
        .map(|k| format!("(1, 'w-{k}', 0, 420, 1000, 1000, {k}, {k}, 0, {k})"))
        .collect();
    client
        .execute(
            &format!(
                "INSERT INTO {table}.node\
                 (parent_id,name,is_dir,mode,uid,gid,mtime,atime,ctime,ino) \
                 VALUES {} ON CONFLICT (parent_id,name) DO NOTHING",
                vals.join(",")
            ),
            &[],
        )
        .unwrap_or_else(|e| panic!("warmup {table}: {e}"));
    // 必须 ANALYZE：空表统计信息写着 rows≈0，灌完 500 行不刷新的话，
    // `WHERE parent_id=1 ORDER BY name LIMIT 100` 会被估成「命中 2 行」，
    // 于是走 Bitmap Heap Scan + top-N Sort（实测 1.5 ms）而不是
    // `nn_(parent_id,name)` 的有序索引扫描（~0.2 ms）。这不是 PG 慢，
    // 是量具没给它统计信息 —— 生产里 autovacuum 会自动 ANALYZE。
    // 2026-11-20 修：readdir 64/1 那一格因此从 67 k 变成 250 k ops/s。
    client
        .batch_execute(&format!("ANALYZE {table}.node"))
        .unwrap_or_else(|e| panic!("analyze {table}: {e}"));
}

/// 本次运行的唯一标识：同一集群上重跑时避开 `create` / `key` 的主键冲突。
fn nonce() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64 % 1_000_000)
        .unwrap_or(0)
}

/// 一次 op（`n` = 一个事务里塞几条语句）。
///
/// 返回值是**真正执行掉的语句数**：外层按它计 ops，不能按 `n` 计。之前
/// `ping`/`lookup`/`readdir` 只跑 1 条语句却按 `pipeline` 计数，pipeline=32
/// 那几格直接虚高 32 倍（量出过 14.7 M ops/s 的鬼数字）。
///
/// 事务用 `Client::transaction()` 的 RAII guard：出错时 guard 被丢弃 = 自动
/// ROLLBACK，不会把连接留在 aborted 状态污染下一个 op。
fn one_op(
    client: &mut postgres::Client,
    op: &str,
    table: &str,
    t: usize,
    seq: u64,
    n: usize,
    run: u64,
) -> Result<usize, postgres::Error> {
    // PG 侧 BIGINT 是 i64，u64 没有 ToSql 实现，数值参数一律先转 i64。
    let q = |v: u64| v as i64;
    let none = &[] as &[&(dyn postgres::types::ToSql + Sync)];
    match op {
        "ping" => {
            for _ in 0..n {
                client.query("SELECT 1", none)?;
            }
            Ok(n)
        }
        "lookup" => {
            let s = format!(
                "SELECT id,name,is_dir,data_ref,mode,uid,gid FROM {table}.node \
                 WHERE parent_id=1 AND name=$1"
            );
            let mut tx = client.transaction()?;
            for k in 0..n as u64 {
                let name = format!("w-{}", (seq + k) % 500);
                let params: &[&(dyn postgres::types::ToSql + Sync)] = &[&name];
                tx.query(&s, params)?;
            }
            tx.commit()?;
            Ok(n)
        }
        "readdir" => {
            let s = format!(
                "SELECT id,name,is_dir,data_ref,mode,uid,gid FROM {table}.node \
                 WHERE parent_id=1 ORDER BY name LIMIT 100"
            );
            let mut tx = client.transaction()?;
            for _ in 0..n {
                tx.query(&s, none)?;
            }
            tx.commit()?;
            Ok(n)
        }
        "create" => {
            let s = format!(
                "INSERT INTO {table}.node \
                 (parent_id,name,is_dir,mode,uid,gid,mtime,atime,ctime,ino) \
                 VALUES (1,$1,0,420,1000,1000,$2,$2,0,$3)"
            );
            let mut tx = client.transaction()?;
            for k in 0..n as u64 {
                let name = format!("c{run}-{t}-{}", seq + k);
                let ts = q(seq + k);
                let params: &[&(dyn postgres::types::ToSql + Sync)] = &[&name, &ts, &ts];
                tx.execute(&s, params)?;
            }
            tx.commit()?;
            Ok(n)
        }
        // 最重的写：改 inode 的 data_ref + 给共享账本记一个 holder，同一个事务。
        // ino 取 w- 预热那 500 行的 id（1..=500），保证 UPDATE 一定命中真实行。
        "write" => {
            let upd = format!("UPDATE {table}.node SET data_ref=$1, mtime=$2 WHERE id=$3");
            let ins = "INSERT INTO grovefs_refs.holders(data_ref,holders) VALUES ($1,1) \
                       ON CONFLICT (data_ref) DO UPDATE \
                       SET holders = grovefs_refs.holders.holders + 1";
            let mut tx = client.transaction()?;
            for k in 0..n as u64 {
                let d = format!("1:4096:dev{}", (seq + k) % 997);
                let ts = q(seq + k);
                let ino = ((seq + k) % 500) as i64 + 1;
                let params: &[&(dyn postgres::types::ToSql + Sync)] = &[&d, &ts, &ino];
                tx.execute(&upd, params)?;
                tx.execute(ins, &[&d])?;
            }
            // 一个逻辑 op = UPDATE + refs 记账两条语句，计 1 条：treedb 侧的
            // write 同样是一次 op 改两处，按“业务操作”而不是“SQL 语句”口径。
            tx.commit()?;
            Ok(n)
        }
        "lock" => {
            let s = format!(
                "INSERT INTO {table}.locks(ino,owner,kind,lease_expiry) \
                 VALUES (1,$1,'write',$2)"
            );
            let mut tx = client.transaction()?;
            for k in 0..n as u64 {
                let owner = format!("owner{t}-{run}-{}", seq + k);
                let ts = q(seq + k);
                let params: &[&(dyn postgres::types::ToSql + Sync)] = &[&owner, &ts];
                tx.execute(&s, params)?;
            }
            tx.commit()?;
            Ok(n)
        }
        "key" => {
            let ins = "INSERT INTO grovefs_control.api_keys \
                       (id,label,secret_hash,created_at,scope,ro) \
                       VALUES ($1,'bench',$2,'2026-01-01T00:00:00Z','any',false)";
            let mut tx = client.transaction()?;
            for k in 0..n as u64 {
                let id = format!("k{run}-{t}-{}", seq + k);
                let hash = format!("h{run}-{}", seq + k);
                tx.execute(ins, &[&id, &hash])?;
            }
            tx.commit()?;
            Ok(n)
        }
        other => unreachable!("unknown op {other}"),
    }
}

const OPS: &[&str] = &["ping", "lookup", "readdir", "create", "write", "lock", "key"];

fn main() {
    let args = parse();
    if !OPS.contains(&args.op.as_str()) {
        eprintln!("op {} 不支持，可选：{OPS:?}", args.op);
        std::process::exit(2);
    }
    let run = nonce();

    let mut admin = postgres::Client::connect(&args.uri, postgres::NoTls)
        .unwrap_or_else(|e| panic!("connect {}: {e}", args.uri));
    let tables = setup(&mut admin, args.views);
    for t in &tables {
        warmup(&mut admin, t);
    }
    drop(admin);

    let mut handles = Vec::new();
    for t in 0..args.conns {
        let uri = args.uri.clone();
        let op = args.op.clone();
        let tables = tables.clone();
        let secs = args.secs;
        handles.push(thread::spawn(move || {
            let mut client = postgres::Client::connect(&uri, postgres::NoTls).expect("connect");
            let table = tables[t % tables.len()].clone();
            // 计时窗口从本线程连上之后才起算：拿父线程算好的 deadline 的话，
            // 建连慢的线程一上来就少跑几秒，分母却仍按整段算。
            let wstart = Instant::now();
            let deadline = wstart + Duration::from_secs(secs);
            let mut lat: Vec<u64> = Vec::with_capacity(1 << 16);
            let mut n = 0u64;
            let mut seq = 0u64;
            while Instant::now() < deadline {
                let t0 = Instant::now();
                match one_op(&mut client, &op, &table, t, seq, args.pipeline, run) {
                    Ok(done) => {
                        let us = t0.elapsed().as_micros() as u64;
                        let per = us / done as u64;
                        for _ in 0..done {
                            lat.push(per);
                        }
                        n += done as u64;
                    }
                    Err(e) => {
                        // 重跑撞上本次 run 之外的主键是记账问题，不是被测路径失败；
                        // 其余错误必须计出来，否则 QPS 是假的。
                        let txt = format!("{e}");
                        if !txt.contains("duplicate key") {
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
    // 分母 = 各线程自身窗口的最大值，与 treedb-bench 同一套口径：在父线程取
    // t0 会把建连开销算进分母，两边必须一样量。
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
        "pg op={} conns={} pipeline={} → {:.0} ops/s over {:.2}s \
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
}

const HELP: &str = "\
treedb-pgbench — 同样的元数据 op 打到 PostgreSQL，给自研引擎装配母分母

  --uri URI          postgres URI（默认 postgresql://postgres@127.0.0.1:54390/postgres）
  --op NAME          ping|lookup|readdir|create|write|lock|key
  --conns N          连接数 = 线程数 = PG backend 数（默认 8）
  --secs N           持续秒数（默认 5）
  --views N          预建视图 schema 数（默认 8）
  --pipeline N       一个事务里塞 N 条语句（默认 1 = 每 op 一次 fsync）
";
