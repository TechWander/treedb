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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use treedb::engine::core::EngineStats;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, TableDef, COL_INDEX, COND_NONE, DEL_IF_EXISTS, PUT_IF_ABSENT,
    ROW_PK,
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
    /// 固定工作量档（`--bulk`）：不是「跑满 N 秒看 ops/s」，而是「删掉正好 N 行
    /// 看花了多少记录/字节/毫秒」。破坏性负载不能用时长口径量 —— 表删空之后
    /// 剩下的时间全在数空区间，报出来的 ops/s 反而最好看。
    bulk: String,
    /// 这一档的表规模。
    rows: u64,
    /// `range` 模式每批删多少行（0 = 1000）。
    batch: u64,
    /// `churn` 的删后重插轮数；每轮正好 2N 个 op。
    cycles: u64,
    /// 测量窗口内的 Stats 采样间隔；0 = 不采样（保留历史脚本的零 instrumentation）。
    sample_ms: u64,
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
        bulk: String::new(),
        rows: 200_000,
        batch: 1000,
        cycles: 3,
        sample_ms: 0,
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
            "--bulk" => args.bulk = next(&mut i),
            "--rows" => args.rows = next(&mut i).parse().unwrap_or(200_000),
            "--batch" => args.batch = next(&mut i).parse().unwrap_or(1000),
            "--cycles" => args.cycles = next(&mut i).parse().unwrap_or(3),
            "--sample-ms" => args.sample_ms = next(&mut i).parse().unwrap_or(0),
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

/// 基准用的默认表名。固定一个而不带 nonce：行键里已经带了 nonce，而反复跑同一条
/// 基准要能命中上一轮已经预热好的那张表 —— 每次重建一遍等于把建表成本混进
/// 第一格。
const ROW_TABLE: &[u8] = b"benchkv";

/// 本轮实际用的表名：默认 `benchkv`（与历史格子逐字一致），`TREEDB_BENCH_TABLE`
/// 可覆盖。
///
/// 为什么要能改：`benches/shard_write.sh` 的对照组要把多个 bench 进程压到
/// **同一个服务端**上，而键的唯一性只到 `(nonce, 线程, seq)` —— `nonce` 是启动
/// 纳秒 mod 100000，两个进程撞上就是成片的 `EEXIST`，那一格报的是错误而不是
/// 吞吐。各进程打自己的表名既把键空间隔开，也正好是「按表路由」这个分片方案
/// 的形状：单实例内多张表与多实例各一张表，服务端都只有一路提交线程。
fn row_table() -> &'static [u8] {
    static TBL: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    TBL.get_or_init(|| {
        std::env::var("TREEDB_BENCH_TABLE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.into_bytes())
            .unwrap_or_else(|| ROW_TABLE.to_vec())
    })
    .as_slice()
}

/// 索引列下标。列序固定为 `(tag, payload)`，所以 tag 恒为 0。
const ROW_TAG: u32 = 0;

/// 基准表的形状：`tag` 可索引（`rowidx` 走它），`payload` 无约束。
///
/// 只有两列是刻意的：再加列只会把每行的字节数变大，而这里要量的是「表这条
/// 路径」的常数开销（定位 / 索引维护 / 帧编解码），不是某种真实业务的行宽。
fn row_table_def() -> TableDef {
    TableDef {
        name: row_table().to_vec(),
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
            match c.call(&Op::TableGet { name: row_table().to_vec() }) {
                Ok(Reply::Table(Some(d))) if d == row_table_def() => {}
                Ok(Reply::Table(Some(d))) => {
                    eprintln!(
                        "table {} already exists with a different shape ({} columns); \
                         drop it or use another --dir",
                        String::from_utf8_lossy(row_table()),
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
        table: row_table().to_vec(),
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

// ---------------------------------------------------------------- 批量形状档

/// 批量档用的表名。与 `benchkv` 分开：那一档要常驻一张表给读轴用，而这一档
/// 每一轮都要把表删空，共用一张就会互相污染。
const BULK_TABLE: &[u8] = b"bulkkv";

/// 批量档的键：8 位补零，字节序 == 数字序。`range` 模式的分窗靠这一点才能保证
/// 「一个窗口正好装 `batch` 行」，否则量到的窗宽是个随机数。
fn bulk_key(i: u64) -> Vec<u8> {
    format!("b{i:08}").into_bytes()
}

/// 一行的内容只由 `i` 决定，与第几轮运行无关：`putsame` 要的就是「逐字节重推
/// 同一份值」，混一个 nonce 进去就把这条路径变成了一次真改动。
fn bulk_cols(i: u64) -> Vec<Vec<u8>> {
    vec![format!("t{}", i % 16).into_bytes(), format!("p{i}").into_bytes()]
}

fn bulk_put(i: u64, changed: bool) -> Op {
    let mut cols = bulk_cols(i);
    if changed {
        cols[1] = format!("p{i}x").into_bytes();
    }
    Op::RowPut {
        table: BULK_TABLE.to_vec(),
        key: bulk_key(i),
        cols,
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn bulk_delete(i: u64) -> Op {
    Op::RowDelete {
        table: BULK_TABLE.to_vec(),
        key: bulk_key(i),
        flags: DEL_IF_EXISTS,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

/// churn 第 `round` 轮重插的内容。轮号进 payload，保证每次都是真写而非同值消除。
fn bulk_churn_put(i: u64, round: u64) -> Op {
    let mut cols = bulk_cols(i);
    cols[1] = format!("p{i}c{round}").into_bytes();
    Op::RowPut {
        table: BULK_TABLE.to_vec(),
        key: bulk_key(i),
        cols,
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

/// 逐行核对 churn 的最后一轮内容；返回实际检查的行数。
///
/// 重启后只看 `rows == N` 不够：旧快照、半条尾巴或重插没落盘都可能留下相同的
/// 行数。这里连主键、索引列和带轮号的 payload 一起对。
fn verify_churn(c: &mut Client, n: u64, round: u64) -> Result<u64, String> {
    if rows_of(c) != n {
        return Err(format!("churn 表行数不是 {n}"));
    }
    let mut checked = 0u64;
    let mut got = 0u64;
    while got < n {
        let end = (got + 500).min(n);
        let batch: Vec<Op> = (got..end)
            .map(|i| Op::RowGet {
                table: BULK_TABLE.to_vec(),
                key: bulk_key(i),
            })
            .collect();
        let rs = c.pipeline(&batch).map_err(|e| format!("churn verify: {e}"))?;
        for (i, r) in (got..end).zip(rs) {
            match r {
                Reply::Row(Some(row)) => {
                    let mut want = bulk_cols(i);
                    want[1] = format!("p{i}c{round}").into_bytes();
                    if row.cols != want {
                        return Err(format!("第 {i} 行内容不是第 {round} 轮"));
                    }
                }
                other => return Err(format!("第 {i} 行读回 {other:?}")),
            }
            checked += 1;
        }
        got = end;
    }
    Ok(checked)
}

#[derive(Default)]
struct BulkPeak {
    samples: u64,
    wal_bytes: u64,
    wal_disk_bytes: u64,
    snap_bytes: u64,
}

impl BulkPeak {
    fn observe(&mut self, s: &EngineStats) {
        self.samples += 1;
        self.wal_bytes = self.wal_bytes.max(s.wal_bytes);
        self.wal_disk_bytes = self.wal_disk_bytes.max(s.wal_disk_bytes);
        self.snap_bytes = self.snap_bytes.max(s.snap_bytes);
    }
}

/// 只在固定工作量的测量窗口内采样。连接与握手在计时前完成，采样线程不碰 WAL。
struct BulkSampler {
    stop: std::sync::Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<BulkPeak>>,
}

fn start_bulk_sampler(args: &Args) -> Option<BulkSampler> {
    if args.sample_ms == 0 {
        return None;
    }
    let mut c = Client::connect_auth(&args.addr, 3000, args.auth.as_deref())
        .expect("bulk sampler conn");
    let interval = Duration::from_millis(args.sample_ms);
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let handle = std::thread::spawn(move || {
        let mut peak = BulkPeak::default();
        loop {
            peak.observe(&stats_of(&mut c));
            if flag.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(interval);
        }
        peak
    });
    Some(BulkSampler { stop, handle: Some(handle) })
}

impl BulkSampler {
    fn stop(&mut self) -> BulkPeak {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().expect("sampler already stopped").join().expect("bulk sampler")
    }
}

/// churn 的每个半轮是一道屏障：所有连接先把 N 行删完，才允许任何连接进入
/// 同一轮的重插；重插全部完成后再开下一轮。轮内仍按连续区间并行。
///
/// 若把 `2N × cycles` 一次性切开，后一轮删除会与前一轮重插跨连接乱序，
/// `DEL_IF_EXISTS` 就会真实地报 `ENOENT`，量到的也不再是删后重插。
fn run_churn_phases(
    args: &Args,
    phases: &[Vec<Op>],
    conns: usize,
    pipe: usize,
) -> (u64, u64, u64) {
    let widest = phases.iter().map(Vec::len).max().unwrap_or(0);
    let active = conns.min(widest.max(1));
    let mut clients: Vec<Client> = (0..active)
        .map(|_| {
            Client::connect_auth(&args.addr, 3000, args.auth.as_deref()).expect("churn conn")
        })
        .collect();
    let barrier = std::sync::Barrier::new(active);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(active);
        for (k, c) in clients.iter_mut().enumerate() {
            let barrier = &barrier;
            handles.push(scope.spawn(move || {
                let (mut done, mut reported, mut errs) = (0u64, 0u64, 0u64);
                for ops in phases {
                    barrier.wait();
                    let per = (ops.len() + active - 1) / active;
                    let mut i = (k * per).min(ops.len());
                    let hi = (i + per).min(ops.len());
                    while i < hi {
                        let end = (i + pipe).min(hi);
                        let batch: Vec<Op> = ops[i..end].to_vec();
                        match c.pipeline(&batch) {
                            Ok(rs) => {
                                for r in rs {
                                    match r {
                                        Reply::Count(v) => {
                                            done += 1;
                                            reported += v;
                                        }
                                        Reply::Ok | Reply::Row(_) | Reply::Table(_) => done += 1,
                                        Reply::Err { code, ref msg } => {
                                            errs += 1;
                                            eprintln!(
                                                "churn reply err {code} {}",
                                                String::from_utf8_lossy(msg)
                                            );
                                        }
                                        other => {
                                            errs += 1;
                                            eprintln!("churn reply shape {other:?}");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                errs += (end - i) as u64;
                                eprintln!("churn pipeline: {e}");
                            }
                        }
                        i = end;
                    }
                }
                (done, reported, errs)
            }));
        }
        let mut sum = (0u64, 0u64, 0u64);
        for h in handles {
            let (d, r, e) = h.join().expect("churn thread");
            sum.0 += d;
            sum.1 += r;
            sum.2 += e;
        }
        sum
    })
}

fn stats_of(c: &mut Client) -> EngineStats {
    match c.call(&Op::Stats).expect("stats") {
        Reply::Stats(s) => *s,
        other => panic!("Stats → {other:?}"),
    }
}

fn rows_of(c: &mut Client) -> u64 {
    match c
        .call(&Op::RowCount {
            table: BULK_TABLE.to_vec(),
            column: ROW_PK,
            value: Vec::new(),
        })
        .expect("rowcount")
    {
        Reply::Count(n) => n,
        other => panic!("RowCount → {other:?}"),
    }
}

/// 自动 checkpoint 之后写一条小尾巴：插入一个专用键再删掉它。
///
/// 终态与调用前逐字段相同，但 WAL 多两条记录；联合量具随后直接 SIGKILL，
/// 用来证明「快照 + 已 ack 尾巴」确实走完了恢复，而不是靠优雅停服替它收口。
fn run_tail(args: &Args) {
    let mut c = Client::connect_auth(&args.addr, 3000, args.auth.as_deref())
        .expect("tail control conn");
    let rows_before = rows_of(&mut c);
    let s0 = stats_of(&mut c);
    let ops = vec![
        Op::RowPut {
            table: BULK_TABLE.to_vec(),
            key: b"delete-storm-tail".to_vec(),
            cols: vec![b"tail".to_vec(), b"tail".to_vec()],
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
        Op::RowDelete {
            table: BULK_TABLE.to_vec(),
            key: b"delete-storm-tail".to_vec(),
            flags: DEL_IF_EXISTS,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    ];
    let rs = c.pipeline(&ops).expect("tail pipeline");
    if rs.len() != 2 {
        eprintln!("tail: 收到 {} 条应答，应为 2", rs.len());
        std::process::exit(1);
    }
    for r in rs {
        if let Reply::Err { code, ref msg } = r {
            eprintln!("tail: errno {code}: {}", String::from_utf8_lossy(msg));
            std::process::exit(1);
        }
    }
    let s1 = stats_of(&mut c);
    let rows_left = rows_of(&mut c);
    let records = s1.seq - s0.seq;
    let writes = s1.writes_total - s0.writes_total;
    if records != 2 || writes != 2 || rows_left != rows_before {
        eprintln!(
            "tail: records={records} writes={writes} rows={rows_before}→{rows_left}，应为 2/2/不变"
        );
        std::process::exit(1);
    }
    println!(
        "tail ops=2/2 records={records} writes={writes} wal_start_bytes={} wal_end_bytes={} rows_left={rows_left} errors=0",
        s0.wal_bytes, s1.wal_bytes
    );
}

const POSIX_VIEW: &[u8] = b"delete-storm-posix";

fn posix_file(i: u64) -> Vec<u8> {
    format!("f{i:08}").into_bytes()
}

fn posix_dir(i: u64) -> Vec<u8> {
    format!("d{i:08}").into_bytes()
}

fn posix_ref(i: u64) -> Vec<u8> {
    format!("ref{i:08}").into_bytes()
}

fn posix_create(view: u32, i: u64) -> Op {
    Op::Create {
        view,
        parent: 1,
        name: posix_file(i),
        mode: 0o644,
        uid: 1,
        gid: 1,
    }
}

fn posix_mkdir(view: u32, i: u64) -> Op {
    Op::Mkdir {
        view,
        parent: 1,
        name: posix_dir(i),
        mode: 0o755,
        uid: 1,
        gid: 1,
    }
}

fn posix_setref(view: u32, ino: u64, i: u64) -> Op {
    Op::Setattr {
        view,
        ino,
        valid: treedb::engine::model::SA_DATA_REF,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 0,
        atime: 0,
        mtime: 0,
        data_ref: posix_ref(i),
        ref_bytes: 4096,
    }
}

fn posix_unlink(view: u32, i: u64) -> Op {
    Op::Unlink {
        view,
        parent: 1,
        name: posix_file(i),
    }
}

fn posix_rmdir(view: u32, i: u64) -> Op {
    Op::Rmdir {
        view,
        parent: 1,
        name: posix_dir(i),
    }
}

fn posix_view(c: &mut Client) -> (u32, u64) {
    match c.call(&Op::ListViews).expect("ListViews") {
        Reply::Views(views) => {
            let (_, id, root) = views
                .into_iter()
                .find(|(name, _, _)| name.as_slice() == POSIX_VIEW)
                .unwrap_or_else(|| panic!("缺少视图 {}", String::from_utf8_lossy(POSIX_VIEW)));
            assert_eq!(root, 1, "POSIX 删除档固定打视图根");
            (id, root)
        }
        other => panic!("ListViews → {other:?}"),
    }
}

fn verify_posix_refs(c: &mut Client, n: u64) -> Result<u64, String> {
    let mut checked = 0u64;
    for start in (0..n).step_by(500) {
        let end = (start + 500).min(n);
        let batch: Vec<Op> = (start..end)
            .map(|i| Op::RefLookup { data_ref: posix_ref(i) })
            .collect();
        let rs = c.pipeline(&batch).map_err(|e| format!("RefLookup: {e}"))?;
        if rs.len() != batch.len() {
            return Err(format!("RefLookup 应答 {} 条，请求 {} 条", rs.len(), batch.len()));
        }
        for (i, r) in (start..end).zip(rs) {
            match r {
                Reply::Ref { count: 0, bytes: 0 } => checked += 1,
                other => return Err(format!("ref {i} 未归零：{other:?}")),
            }
        }
    }
    Ok(checked)
}

/// 造 POSIX 删除档的语料；刻意与删除测量分成两个服务端生命周期。
///
/// 这样 `posixdelete` 的 `worst_round_us` 只覆盖删除、自动 checkpoint 等待与停服，
/// 不会把造 2N 个 inode 的时间混进删除轮次。
fn run_posix_seed(args: &Args) {
    let n = args.rows;
    if n == 0 {
        eprintln!("posixseed 需要 --rows >= 1");
        std::process::exit(2);
    }
    let mut c = Client::connect_auth(&args.addr, 3000, args.auth.as_deref())
        .expect("posixseed control conn");
    match c.call(&Op::CreateView { name: POSIX_VIEW.to_vec() }).expect("CreateView") {
        Reply::View { id: view, root: 1 } => {
            let baseline = stats_of(&mut c);
            if (baseline.views, baseline.nodes, baseline.dentries, baseline.locks, baseline.refs,
                baseline.tables, baseline.rows) != (1, 1, 0, 0, 0, 0, 0)
            {
                eprintln!("posixseed 要求空库，实际 Stats={baseline:?}");
                std::process::exit(1);
            }
            let s0 = stats_of(&mut c);
            let mut inos: Vec<u64> = Vec::with_capacity(n as usize);
            for start in (0..n).step_by(500) {
                let end = (start + 500).min(n);
                let batch: Vec<Op> = (start..end).map(|i| posix_create(view, i)).collect();
                let rs = c.pipeline(&batch).expect("posix Create pipeline");
                if rs.len() != batch.len() {
                    eprintln!("posix Create 应答数不符");
                    std::process::exit(1);
                }
                for (i, r) in (start..end).zip(rs) {
                    match r {
                        Reply::Node(node) => inos.push(node.ino),
                        other => {
                            eprintln!("posix Create {i} → {other:?}");
                            std::process::exit(1);
                        }
                    }
                }
            }
            for start in (0..n).step_by(500) {
                let end = (start + 500).min(n);
                let batch: Vec<Op> = (start..end)
                    .map(|i| posix_setref(view, inos[i as usize], i))
                    .collect();
                for (i, r) in (start..end).zip(c.pipeline(&batch).expect("posix Setattr pipeline")) {
                    if !matches!(r, Reply::Node(_)) {
                        eprintln!("posix Setattr {i} → {r:?}");
                        std::process::exit(1);
                    }
                }
            }
            for start in (0..n).step_by(500) {
                let end = (start + 500).min(n);
                let batch: Vec<Op> = (start..end).map(|i| posix_mkdir(view, i)).collect();
                for (i, r) in (start..end).zip(c.pipeline(&batch).expect("posix Mkdir pipeline")) {
                    if !matches!(r, Reply::Node(_)) {
                        eprintln!("posix Mkdir {i} → {r:?}");
                        std::process::exit(1);
                    }
                }
            }
            let s1 = stats_of(&mut c);
            if (s1.views, s1.nodes, s1.dentries, s1.locks, s1.refs, s1.tables, s1.rows)
                != (1, 1 + 2 * n, 2 * n, 0, n, 0, 0)
            {
                eprintln!("posixseed 终态不符：{s1:?}");
                std::process::exit(1);
            }
            println!(
                "posixseed rows={n} ops={} records={} writes={} baseline_nodes={} nodes={} dentries={} refs={} errors=0",
                3 * n,
                s1.seq - s0.seq,
                s1.writes_total - s0.writes_total,
                baseline.nodes,
                s1.nodes,
                s1.dentries,
                s1.refs
            );
        }
        other => {
            eprintln!("CreateView → {other:?}");
            std::process::exit(1);
        }
    }
}

fn run_posix_verify(args: &Args) {
    let n = args.rows;
    let mut c = Client::connect_auth(&args.addr, 3000, args.auth.as_deref())
        .expect("posixverify control conn");
    let (view, _) = posix_view(&mut c);
    let s = stats_of(&mut c);
    if (s.views, s.nodes, s.dentries, s.locks, s.refs, s.tables, s.rows) != (1, 1, 0, 0, 0, 0, 0) {
        eprintln!("posixverify 账本未回到基线：{s:?}");
        std::process::exit(1);
    }
    match c.call(&Op::ReadDir { view, parent: 1, offset: 0, limit: 1000 }).expect("ReadDir") {
        Reply::Dirents { names, next, end, .. } if names.is_empty() && next == 0 && end => {}
        other => {
            eprintln!("posixverify 根目录不为空：{other:?}");
            std::process::exit(1);
        }
    }
    match verify_posix_refs(&mut c, n) {
        Ok(checked) => println!("posixverify rows={n} entries=0 checked_refs={checked} errors=0"),
        Err(e) => {
            eprintln!("posixverify: {e}");
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------- 万级混合视图档

/// 混合读写档每组固定五个 op：两次 `Lookup`、一次 `ReadDir`、一次 `Create` 加
/// 一次同名 `Unlink`。
///
/// 3 读 2 写是刻意的：读走 worker 读锁、不产 WAL 记录，写走唯一提交线程。纯读
/// 那一头 `--op lookup` / `--op readdir` 已经各自量过，纯写有 `--op create`；这一
/// 档要的是两者混在**同一条连接、同一批**里时的形状 —— 那才是客户端真实形态
/// （一次 `getattr` 打两个名字、一次 `ls`、一次 `touch` 后立刻 `rm`）。
const MIX_GROUP_OPS: usize = 5;

/// `mvseed` / `mvmix` 的视图名。刻意与压测档的 `bench<t>v<v>` 区分：那一档每线程
/// 独占一批视图，混合档要让所有连接打**同一批**视图，量的才是「多客户端并发遍历
/// 同一万级视图集合」，不是「各自数自己那几百个视图」。
fn mv_view(i: u64) -> Vec<u8> {
    format!("mv{i:06}").into_bytes()
}

/// 视图内预热出来的文件名；`Lookup` / `ReadDir` 只打这些既有名字，它们的应答
/// 形状因此与净零断言无关。
fn mv_child(j: u64) -> Vec<u8> {
    format!("w{j:04}").into_bytes()
}

/// `mvmix` 每组的临时名。`nonce` 只有五位（见 [`nonce`]），组号取满 u64 也才二十
/// 位，加上前缀与分隔符仍远在名字长度上限内；`m` 与预热的 `w` 不撞名。
fn mv_tmp(nonce: u64, g: u64) -> Vec<u8> {
    format!("m{nonce}-{g}").into_bytes()
}

/// 把上一档灌好的 N 个视图 id 查回来，并核对视图名就是 `mv000000` 起连号。
///
/// 不假设 id 从 1 递增：那是实现细节，`DropView` 之后就会留洞。这一档要的是
/// 「N 个不同视图」，洞不洞的与结论无关。名字形如 `mv*` 但后缀不是数字的（比如
/// 用户自己的 `mvold`）直接不计数，不拿它去碰后面的连号判定。
fn mv_views(c: &mut Client, nviews: u64) -> Vec<u32> {
    let all = match c.call(&Op::ListViews).expect("ListViews") {
        Reply::Views(v) => v,
        other => panic!("ListViews → {other:?}"),
    };
    let mut hits: Vec<(u64, u32)> = Vec::new();
    for (name, id, root) in &all {
        if !name.starts_with(b"mv") || *root != 1 {
            continue;
        }
        let idx = std::str::from_utf8(&name[2..]).ok().and_then(|s| s.parse::<u64>().ok());
        if let Some(idx) = idx {
            hits.push((idx, *id));
        }
    }
    hits.sort();
    let gapped = hits.iter().enumerate().any(|(k, (i, _))| *i != k as u64);
    if hits.len() as u64 != nviews || gapped {
        eprintln!(
            "需要 {nviews} 个 mv* 视图，实际查到 {} 个{}（ListViews 共 {} 条）",
            hits.len(),
            if gapped { "（编号不连续）" } else { "" },
            all.len()
        );
        std::process::exit(1);
    }
    hits.iter().map(|(_, id)| *id).collect()
}

/// 造万级混合视图档的语料：`--views` 个视图 × `--rows` 个文件。
///
/// 记录数不靠猜，按 `src/engine/apply.rs` 的分支逐条给出：`CreateView` 产
/// `PutView` + `PutNode(root)` 两条，`Create`（新名、无 data_ref）产 `bump` +
/// `PutNode` + `PutDentry` 三条，所以终态必须是 `views=N`、`nodes=N*(W+1)`、
/// `dentries=N*W`、`records=2N+3NW`。逐视图 `ReadDir` 是为了确认 dentry 数不是
/// 只对了计数器 —— 账本对得上而目录翻页少几项，一样是坏语料。
fn run_mv_seed(args: &Args) {
    let (nv, w) = (args.views as u64, args.rows);
    if nv == 0 || w == 0 {
        eprintln!("mvseed 需要 --views >= 1 且 --rows >= 1");
        std::process::exit(2);
    }
    if w > 1000 {
        eprintln!("mvseed 的 --rows 是每视图的文件数，上限 1000（一页 ReadDir 就能核完）");
        std::process::exit(2);
    }
    let mut c =
        Client::connect_auth(&args.addr, 3000, args.auth.as_deref()).expect("mvseed control conn");
    let s0 = stats_of(&mut c);
    if (s0.views, s0.nodes, s0.dentries, s0.locks, s0.refs, s0.tables, s0.rows)
        != (0, 0, 0, 0, 0, 0, 0)
    {
        eprintln!("mvseed 要求空库，实际 Stats={s0:?}");
        std::process::exit(1);
    }
    let t0 = Instant::now();
    let mut vids: Vec<u32> = Vec::with_capacity(nv as usize);
    for start in (0..nv).step_by(500) {
        let end = (start + 500).min(nv);
        let batch: Vec<Op> =
            (start..end).map(|i| Op::CreateView { name: mv_view(i) }).collect();
        let rs = c.pipeline(&batch).expect("mv CreateView pipeline");
        if rs.len() != batch.len() {
            eprintln!("mv CreateView 应答 {} 条，请求 {} 条", rs.len(), batch.len());
            std::process::exit(1);
        }
        for (i, r) in (start..end).zip(rs) {
            match r {
                Reply::View { id, root: 1 } => vids.push(id),
                other => {
                    eprintln!("CreateView #{i} → {other:?}");
                    std::process::exit(1);
                }
            }
        }
    }
    let mut created = 0u64;
    for &view in &vids {
        let mut start = 0u64;
        while start < w {
            let end = (start + 500).min(w);
            let batch: Vec<Op> = (start..end)
                .map(|j| Op::Create {
                    view,
                    parent: 1,
                    name: mv_child(j),
                    mode: 0o644,
                    uid: 1,
                    gid: 1,
                })
                .collect();
            let rs = c.pipeline(&batch).expect("mv Create pipeline");
            if rs.len() != batch.len() {
                eprintln!("mv Create 应答 {} 条，请求 {} 条", rs.len(), batch.len());
                std::process::exit(1);
            }
            for (j, r) in (start..end).zip(rs) {
                match r {
                    Reply::Node(_) => created += 1,
                    other => {
                        eprintln!("mv Create view={view} #{j} → {other:?}");
                        std::process::exit(1);
                    }
                }
            }
            start = end;
        }
    }
    let mut dirs_ok = 0u64;
    for chunk in vids.chunks(200) {
        let batch: Vec<Op> = chunk
            .iter()
            .map(|&view| Op::ReadDir { view, parent: 1, offset: 0, limit: 1000 })
            .collect();
        let rs = c.pipeline(&batch).expect("mv ReadDir pipeline");
        if rs.len() != batch.len() {
            eprintln!("mv ReadDir 应答 {} 条，请求 {} 条", rs.len(), batch.len());
            std::process::exit(1);
        }
        for (&view, r) in chunk.iter().zip(rs) {
            match r {
                Reply::Dirents { names, next, end, .. } if next == w && end && names.len() as u64 == w => {
                    dirs_ok += 1
                }
                other => {
                    eprintln!("mv ReadDir view={view} 的根目录不像 {w} 项：{other:?}");
                    std::process::exit(1);
                }
            }
        }
    }
    let time_ms = t0.elapsed().as_millis() as u64;
    let s1 = stats_of(&mut c);
    let records = s1.seq - s0.seq;
    let writes = s1.writes_total - s0.writes_total;
    if (s1.views, s1.nodes, s1.dentries, s1.locks, s1.refs, s1.tables, s1.rows)
        != (nv, nv * (w + 1), nv * w, 0, 0, 0, 0)
    {
        eprintln!("mvseed 终态不符（views={nv} rows={w}）：{s1:?}");
        std::process::exit(1);
    }
    println!(
        "mvseed views={nv} rows={w} ops={} created={created} dirs_ok={dirs_ok} \
         records={records} writes={writes} time_ms={} nodes={} dentries={} errors=0",
        nv * (w + 1),
        time_ms.max(1),
        s1.nodes,
        s1.dentries
    );
    // 视图 id 也要能原样查回来，否则 `mvmix` 会在一个「账本对、视图缺」的库上跑。
    if vids.len() as u64 != nv || mv_views(&mut c, nv).len() as u64 != nv {
        eprintln!("mvseed 建了 {} 个视图但查回来 {} 个", nv, vids.len());
        std::process::exit(1);
    }
    if records != 2 * nv + 3 * nv * w || writes != nv + nv * w {
        eprintln!(
            "mvseed: records={records} writes={writes}，应为 {}/{}",
            2 * nv + 3 * nv * w,
            nv + nv * w
        );
        std::process::exit(1);
    }
}

/// 核对临时名真的没了、预热的名字真的还在：取头尾各至多 200 组号，逐组两条
/// `Lookup`（临时名必须 ENOENT，预热名必须回 Node）。
///
/// 只读账本等于只信计数器。名字级核对才说明「净零」是真的净零 —— 尤其要排除
/// 「Create 失败、Unlink 也跟着失败，两边都没记账」这种自欺形状。
fn verify_mv_names(
    c: &mut Client,
    vids: &[u32],
    nonce: u64,
    w: u64,
    groups: u64,
) -> Result<(u64, u64), String> {
    let mut ids: Vec<u64> = (0..groups.min(200)).collect();
    ids.extend(groups.saturating_sub(200)..groups);
    ids.sort_unstable();
    ids.dedup();
    let (mut gone, mut kept) = (0u64, 0u64);
    for chunk in ids.chunks(100) {
        let batch: Vec<Op> = chunk
            .iter()
            .flat_map(|&g| {
                let view = vids[g as usize % vids.len()];
                [
                    Op::Lookup { view, parent: 1, name: mv_tmp(nonce, g) },
                    Op::Lookup { view, parent: 1, name: mv_child(g % w) },
                ]
            })
            .collect();
        let rs = c.pipeline(&batch).map_err(|e| format!("mv Lookup: {e}"))?;
        if rs.len() != batch.len() {
            return Err(format!("mv Lookup 应答 {} 条，请求 {} 条", rs.len(), batch.len()));
        }
        for (k, r) in rs.into_iter().enumerate() {
            let g = chunk[k / 2];
            if k % 2 == 0 {
                match r {
                    Reply::Err { code, .. } if code == errno::ENOENT => gone += 1,
                    other => return Err(format!("临时名 m{nonce}-{g} 还在：{other:?}")),
                }
            } else {
                match r {
                    Reply::Node(_) => kept += 1,
                    other => {
                        let name = mv_child(g % w);
                        let want = String::from_utf8_lossy(&name);
                        return Err(format!("预热名 {want} 丢了：{other:?}"));
                    }
                }
            }
        }
    }
    Ok((gone, kept))
}

/// 混合读写档：`--conns` 条连接在 `--secs` 秒里对 `--views` 个已灌好的视图反复跑
/// 「2 Lookup + 1 ReadDir + 1 Create + 1 Unlink」，每组正好一个 pipeline 批。
///
/// 三件事必须同时成立，缺一就说明这档量的不是混合读写：
/// 1. 账本在测量前后逐项相等（每组净零）。这依赖 `src/net/multi.rs` 的分派：
///    含写的批整批交给唯一提交线程按序执行（只有全只读的批才分流到 worker），
///    所以同组内的 `Create` 一定先于 `Unlink` 落地。
/// 2. `records = 5G`、`writes = 2G`：`Create` 三条、`Unlink` 两条（无 data_ref 且
///    nlink=1，走 `DelDentry` + `DelNode`），读不产记录也不计写。
/// 3. `views_touched` 由所有连接共享的位图真实统计，不由 `G % N` 反推。
///
/// 延迟口径与 `posixdelete` 相同：一个 pipeline 的往返微秒数除以实际回包数，逐
/// op 重复后求分位。它衡量的是批内摊还 RTT，不是 pipeline=1 的单 op 延迟。
fn run_mixed_views(args: &Args) {
    let nviews = args.views;
    let w = args.rows;
    let pipe = args.pipeline;
    if nviews == 0 || w == 0 {
        eprintln!("mvmix 需要 --views >= 1 且 --rows >= 1（先跑 --bulk mvseed）");
        std::process::exit(2);
    }
    if pipe == 0 || pipe % MIX_GROUP_OPS != 0 {
        eprintln!("mvmix 的 --pipeline 必须是 {MIX_GROUP_OPS} 的倍数 —— 一组五个 op 不能被切进两批");
        std::process::exit(2);
    }
    let conns = args.conns.max(1);
    let gpb = pipe / MIX_GROUP_OPS;
    let mut ctrl =
        Client::connect_auth(&args.addr, 3000, args.auth.as_deref()).expect("mvmix control conn");
    let vids = mv_views(&mut ctrl, nviews as u64);
    let s0 = stats_of(&mut ctrl);
    if (s0.views, s0.nodes, s0.dentries, s0.locks, s0.refs, s0.tables, s0.rows)
        != (nviews as u64, nviews as u64 * (w + 1), nviews as u64 * w, 0, 0, 0, 0)
    {
        eprintln!("mvmix 语料形状不符（views={nviews} rows={w}）：{s0:?}");
        std::process::exit(1);
    }
    let nonce = nonce();
    let want_page = (w as usize).min(20);
    let want_end = w <= 20;
    let next = Arc::new(AtomicU64::new(0));
    let touched: Arc<Vec<AtomicU64>> =
        Arc::new((0..(nviews + 63) / 64).map(|_| AtomicU64::new(0)).collect());
    let window = Duration::from_secs(args.secs.max(1));
    let mut peak = BulkPeak::default();
    peak.observe(&s0);
    let mut sampler = start_bulk_sampler(args);
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(conns);
    for _ in 0..conns {
        let a = args.addr.clone();
        let auth = args.auth.clone();
        let next = next.clone();
        let touched = touched.clone();
        let vids = vids.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect_auth(&a, 3000, auth.as_deref()).expect("mvmix conn");
            let (mut groups, mut errs) = (0u64, 0u64);
            let (mut lookups, mut dirs, mut creates, mut unlinks) = (0u64, 0u64, 0u64, 0u64);
            let mut lat = Vec::new();
            while t0.elapsed() < window {
                let g0 = next.fetch_add(gpb as u64, Ordering::Relaxed);
                let mut batch: Vec<Op> = Vec::with_capacity(pipe);
                for g in g0..g0 + gpb as u64 {
                    let slot = g as usize % nviews;
                    let view = vids[slot];
                    // 位图按 64 个视图一字打包：字号是 slot >> 6，不是 slot 本身。
                    touched[slot >> 6].fetch_or(1u64 << (slot & 63), Ordering::Relaxed);
                    batch.push(Op::Lookup { view, parent: 1, name: mv_child(g % w) });
                    batch.push(Op::Lookup { view, parent: 1, name: mv_child(g / w.max(1) % w) });
                    batch.push(Op::ReadDir { view, parent: 1, offset: 0, limit: 20 });
                    batch.push(Op::Create {
                        view,
                        parent: 1,
                        name: mv_tmp(nonce, g),
                        mode: 0o644,
                        uid: 1,
                        gid: 1,
                    });
                    batch.push(Op::Unlink { view, parent: 1, name: mv_tmp(nonce, g) });
                }
                let started = Instant::now();
                let ok = match c.pipeline(&batch) {
                    Ok(rs) => {
                        if rs.len() != batch.len() {
                            errs += (batch.len().max(rs.len()) - batch.len().min(rs.len())) as u64;
                            false
                        } else {
                            let us = started.elapsed().as_micros() as u64;
                            let per_op = us / rs.len().max(1) as u64;
                            for _ in 0..rs.len() {
                                lat.push(per_op);
                            }
                            for (k, r) in rs.into_iter().enumerate() {
                                let pos = k % MIX_GROUP_OPS;
                                let good = match (pos, r) {
                                    (0, Reply::Node(_)) | (1, Reply::Node(_)) => {
                                        lookups += 1;
                                        true
                                    }
                                    (3, Reply::Node(_)) => {
                                        creates += 1;
                                        true
                                    }
                                    (2, Reply::Dirents { names, end, .. }) => {
                                        dirs += 1;
                                        names.len() == want_page && end == want_end
                                    }
                                    (4, Reply::NodeRef(d)) => {
                                        unlinks += 1;
                                        d.is_empty()
                                    }
                                    (_, Reply::Err { code, ref msg }) => {
                                        eprintln!("mvmix reply err {code} {}", String::from_utf8_lossy(msg));
                                        false
                                    }
                                    (_, ref other) => {
                                        eprintln!("mvmix reply shape at pos {pos}: {other:?}");
                                        false
                                    }
                                };
                                if !good {
                                    errs += 1;
                                }
                            }
                            true
                        }
                    }
                    Err(e) => {
                        errs += (pipe * gpb) as u64;
                        eprintln!("mvmix pipeline: {e}");
                        false
                    }
                };
                if ok {
                    groups += gpb as u64;
                }
            }
            (groups, lookups, dirs, creates, unlinks, errs, lat)
        }));
    }
    let mut groups = 0u64;
    let mut lookups = 0u64;
    let mut dirs = 0u64;
    let mut creates = 0u64;
    let mut unlinks = 0u64;
    let mut errs = 0u64;
    let mut lat = Vec::new();
    for h in handles {
        let (g, l, d, cr, un, e, mut samples) = h.join().expect("mvmix thread");
        groups += g;
        lookups += l;
        dirs += d;
        creates += cr;
        unlinks += un;
        errs += e;
        lat.append(&mut samples);
    }
    let time_ms = t0.elapsed().as_millis() as u64;
    if let Some(s) = sampler.as_mut() {
        let sampled = s.stop();
        peak.samples += sampled.samples;
        peak.wal_bytes = peak.wal_bytes.max(sampled.wal_bytes);
        peak.wal_disk_bytes = peak.wal_disk_bytes.max(sampled.wal_disk_bytes);
        peak.snap_bytes = peak.snap_bytes.max(sampled.snap_bytes);
    }
    let reserved = next.load(Ordering::Relaxed);
    if errs != 0 {
        eprintln!("mvmix: errors={errs}，没跑完就不必再核对账本");
        std::process::exit(1);
    }
    if reserved != groups {
        eprintln!("mvmix: 领走 {reserved} 个组号、完成 {groups} 个，有批没走到底");
        std::process::exit(1);
    }
    let ops = groups * MIX_GROUP_OPS as u64;
    let mut touched_n = 0u64;
    for word in touched.iter() {
        let mut v = word.load(Ordering::Relaxed);
        while v != 0 {
            v &= v - 1;
            touched_n += 1;
        }
    }
    let s1 = stats_of(&mut ctrl);
    peak.observe(&s1);
    let records = s1.seq - s0.seq;
    let writes = s1.writes_total - s0.writes_total;
    let lat_samples = lat.len();
    let (p50, p90, p99, max) = pct(lat);
    let (gone, kept) = match verify_mv_names(&mut ctrl, &vids, nonce, w, groups) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("mvmix: {e}");
            std::process::exit(1);
        }
    };
    println!(
        "bulk mode=mvmix views={nviews} rows={w} conns={conns} pipeline={pipe} secs={} \
         time_ms={} groups={groups} ops={ops} records={records} writes={writes} syncs={} \
         lookups={lookups} readdirs={dirs} creates={creates} unlinks={unlinks} \
         lat_p50_us={p50} lat_p90_us={p90} lat_p99_us={p99} lat_max_us={max} lat_samples={lat_samples} \
         views_touched={touched_n} gone_ok={gone} kept_ok={kept} \
         wal_start_bytes={} wal_end_bytes={} wal_delta_bytes={} wal_peak_bytes={} \
         checkpoints_start={} checkpoints_end={} checkpoints={} \
         snap_seq_start={} snap_seq_end={} samples={} nodes_left={} dentries_left={} \
         locks_left={} refs_left={} errors={errs} ops_per_s={}",
        args.secs.max(1),
        time_ms.max(1),
        s1.syncs_total - s0.syncs_total,
        s0.wal_bytes,
        s1.wal_bytes,
        s1.wal_bytes.saturating_sub(s0.wal_bytes),
        peak.wal_bytes,
        s0.checkpoints,
        s1.checkpoints,
        s1.checkpoints.saturating_sub(s0.checkpoints),
        s0.snap_seq,
        s1.snap_seq,
        peak.samples,
        s1.nodes,
        s1.dentries,
        s1.locks,
        s1.refs,
        ops * 1000 / time_ms.max(1)
    );
    if lookups != 2 * groups || dirs != groups || creates != groups || unlinks != groups {
        eprintln!("mvmix: op 配比 {lookups}/{dirs}/{creates}/{unlinks}，应为 {}/{groups}/{groups}/{groups}", 2 * groups);
        std::process::exit(1);
    }
    if records != 5 * groups || writes != 2 * groups {
        eprintln!("mvmix: records={records} writes={writes}，应为 {}/{}", 5 * groups, 2 * groups);
        std::process::exit(1);
    }
    if lat_samples != ops as usize {
        eprintln!("mvmix: 延迟样本 {lat_samples}，应为 {ops}");
        std::process::exit(1);
    }
    if touched_n != (nviews as u64).min(groups) {
        eprintln!("mvmix: 实际触及 {touched_n} 个视图，应为 {}", (nviews as u64).min(groups));
        std::process::exit(1);
    }
    if gone != sampled_ids(groups) || kept != sampled_ids(groups) {
        eprintln!(
            "mvmix: 名字级核对 gone={gone} kept={kept}，应为 {}",
            sampled_ids(groups)
        );
        std::process::exit(1);
    }
    if (s1.views, s1.nodes, s1.dentries, s1.locks, s1.refs, s1.tables, s1.rows)
        != (s0.views, s0.nodes, s0.dentries, s0.locks, s0.refs, s0.tables, s0.rows)
    {
        eprintln!("mvmix: 账本没回到测量前 {s0:?} → {s1:?}");
        std::process::exit(1);
    }
    if groups == 0 {
        eprintln!("mvmix: 一个组都没跑完，这一格没有意义");
        std::process::exit(1);
    }
}

/// `verify_mv_names` 会核对的组数：头尾各至多 200 组，重叠部分去重后就是
/// `min(G, 400)`。
fn sampled_ids(groups: u64) -> u64 {
    groups.min(400)
}

/// 固定 2N 个 POSIX 删除 op：N 个 Unlink + N 个 Rmdir。
///
/// 每个文件持有唯一 data_ref，因此 Unlink 必须回非空 NodeRef，且账本从 N 行归零；
/// 目录必须回空 NodeRef。无锁语料下 Unlink/Rmdir 各产 3 条 WAL 记录，总计 6N。
/// 延迟口径与时长档相同：一个 pipeline 的往返微秒数除以实际回包数，逐 op 重复后
/// 求 p50/p99；它衡量的是批内摊还 RTT，不是 pipeline=1 的单 op 延迟。
fn run_posix_delete(args: &Args) {
    let n = args.rows;
    if n == 0 {
        eprintln!("posixdelete 需要 --rows >= 1");
        std::process::exit(2);
    }
    let mut ctrl = Client::connect_auth(&args.addr, 3000, args.auth.as_deref())
        .expect("posixdelete control conn");
    let (view, _) = posix_view(&mut ctrl);
    let s0 = stats_of(&mut ctrl);
    if (s0.views, s0.nodes, s0.dentries, s0.locks, s0.refs, s0.tables, s0.rows)
        != (1, 1 + 2 * n, 2 * n, 0, n, 0, 0)
    {
        eprintln!("posixdelete 语料形状不符：{s0:?}");
        std::process::exit(1);
    }
    let mut ops: Vec<Op> = Vec::with_capacity(2 * n as usize);
    ops.extend((0..n).map(|i| posix_unlink(view, i)));
    ops.extend((0..n).map(|i| posix_rmdir(view, i)));
    let ops = Arc::new(ops);
    let total = 2 * n;
    let conns = args.conns.max(1).min(total.max(1) as usize);
    let pipe = args.pipeline.max(1);
    let per = (total as usize + conns - 1) / conns;
    let mut peak = BulkPeak::default();
    peak.observe(&s0);
    let mut sampler = start_bulk_sampler(args);
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(conns);
    for k in 0..conns {
        let a = args.addr.clone();
        let auth = args.auth.clone();
        let ops = ops.clone();
        let lo = k * per;
        let hi = (lo + per).min(total as usize);
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect_auth(&a, 3000, auth.as_deref()).expect("posix conn");
            let (mut done, mut freed, mut errs) = (0u64, 0u64, 0u64);
            let mut lat = Vec::new();
            let mut i = lo;
            while i < hi {
                let end = (i + pipe).min(hi);
                let batch: Vec<Op> = ops[i..end].to_vec();
                let started = Instant::now();
                match c.pipeline(&batch) {
                    Ok(rs) => {
                        let us = started.elapsed().as_micros() as u64;
                        if rs.len() != batch.len() {
                            errs += (batch.len() - rs.len()) as u64;
                        }
                        let per_op = us / rs.len().max(1) as u64;
                        for _ in 0..rs.len() {
                            lat.push(per_op);
                        }
                        for r in rs {
                            match r {
                                Reply::NodeRef(r) => {
                                    done += 1;
                                    if !r.is_empty() {
                                        freed += 1;
                                    }
                                }
                                Reply::Err { code, ref msg } => {
                                    errs += 1;
                                    eprintln!("posix reply err {code} {}", String::from_utf8_lossy(msg));
                                }
                                other => {
                                    errs += 1;
                                    eprintln!("posix reply shape {other:?}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        errs += (end - i) as u64;
                        eprintln!("posix pipeline: {e}");
                    }
                }
                i = end;
            }
            (done, freed, errs, lat)
        }));
    }
    let mut done = 0u64;
    let mut freed = 0u64;
    let mut errs = 0u64;
    let mut lat = Vec::with_capacity(total as usize);
    for h in handles {
        let (d, f, e, mut samples) = h.join().expect("posix thread");
        done += d;
        freed += f;
        errs += e;
        lat.append(&mut samples);
    }
    let time_ms = t0.elapsed().as_millis() as u64;
    if let Some(s) = sampler.as_mut() {
        let sampled = s.stop();
        peak.samples += sampled.samples;
        peak.wal_bytes = peak.wal_bytes.max(sampled.wal_bytes);
        peak.wal_disk_bytes = peak.wal_disk_bytes.max(sampled.wal_disk_bytes);
        peak.snap_bytes = peak.snap_bytes.max(sampled.snap_bytes);
    }
    let s1 = stats_of(&mut ctrl);
    peak.observe(&s1);
    let records = s1.seq - s0.seq;
    let writes = s1.writes_total - s0.writes_total;
    if (s1.views, s1.nodes, s1.dentries, s1.locks, s1.refs, s1.tables, s1.rows)
        != (1, 1, 0, 0, 0, 0, 0)
    {
        eprintln!("posixdelete 后账本未回到基线：{s1:?}");
        std::process::exit(1);
    }
    let checked = match verify_posix_refs(&mut ctrl, n) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("posixdelete: {e}");
            std::process::exit(1);
        }
    };
    let lat_samples = lat.len();
    let (p50, p90, p99, max) = pct(lat);
    println!(
        "bulk mode=posixdelete rows={n} conns={conns} pipeline={pipe} sample_ms={} \
         time_ms={} ops={done}/{total} freed_refs={freed} records={records} writes={writes} syncs={} \
         lat_p50_us={p50} lat_p90_us={p90} lat_p99_us={p99} lat_max_us={max} lat_samples={lat_samples} \
         wal_bytes={} wal_start_bytes={} wal_end_bytes={} wal_peak_bytes={} \
         wal_disk_start_bytes={} wal_disk_end_bytes={} wal_disk_peak_bytes={} \
         checkpoints_start={} checkpoints_end={} checkpoints={} \
         snap_seq_start={} snap_seq_end={} snap_bytes_start={} snap_bytes_end={} \
         nodes_left={} dentries_left={} locks_left={} refs_left={} samples={} verify_refs={checked} \
         errors={errs} ops_per_s={}",
        args.sample_ms,
        time_ms.max(1),
        s1.syncs_total - s0.syncs_total,
        s1.wal_bytes.saturating_sub(s0.wal_bytes),
        s0.wal_bytes,
        s1.wal_bytes,
        peak.wal_bytes,
        s0.wal_disk_bytes,
        s1.wal_disk_bytes,
        peak.wal_disk_bytes,
        s0.checkpoints,
        s1.checkpoints,
        s1.checkpoints.saturating_sub(s0.checkpoints),
        s0.snap_seq,
        s1.snap_seq,
        s0.snap_bytes,
        s1.snap_bytes,
        s1.nodes,
        s1.dentries,
        s1.locks,
        s1.refs,
        peak.samples,
        done * 1000 / time_ms.max(1)
    );
    if errs != 0 || done != total || freed != n || checked != n || lat_samples != total as usize {
        eprintln!("posixdelete: op/NodeRef/ref 核对没跑完（done={done}/{total}, freed={freed}/{n}, checked={checked}/{n}, lat={lat_samples}, errors={errs}）");
        std::process::exit(1);
    }
    if records != 6 * n || writes != 2 * n {
        eprintln!("posixdelete: records={records} writes={writes}，应为 {}/{}", 6 * n, 2 * n);
        std::process::exit(1);
    }
}

/// 固定工作量档：灌 `--rows` 行 → 按 `--bulk` 指定的形状处理完 → 报时间、
/// 记录数、WAL 字节、fsync 次数与剩下的行数。
///
/// 七个负载模式：`rowdelete`（逐行删）、`range`（按区间删，批大小 `--batch`）、
/// `clear`（一条清空）、`rename`（建一张影子表再顶替）、`putsame`（逐字节相同的
/// 覆盖写）、`putchanged`（同量但每行真的变）、`churn`（同一键集删后重插 `--cycles`
/// 轮）。最后两个写档与 churn 都保证每一轮真的改值；同值消除的收益由 `putsame`
/// 与 `putchanged` 的 `records` / `syncs` / 时间之差给出。`verifychurn` 是重启后的
/// 独立只读验收，不重新灌数据。POSIX 三档（`posixseed` / `posixdelete` /
/// `posixverify`）与万级视图两档（`mvseed` / `mvmix`）在分派处就直接绕开整张表
/// 的铺垫，自己建自己的语料。
///
/// `records` 是服务端 `seq` 的差值，也就是「这些 op 总共写了多少条 WAL 记录」——
/// 它才是这一档的主指标：时间跟盘走，记录数是格式自己的性质。
fn run_bulk(args: &Args) {
    if args.bulk == "posixseed" {
        run_posix_seed(args);
        return;
    }
    if args.bulk == "posixdelete" {
        run_posix_delete(args);
        return;
    }
    if args.bulk == "posixverify" {
        run_posix_verify(args);
        return;
    }
    if args.bulk == "mvseed" {
        run_mv_seed(args);
        return;
    }
    if args.bulk == "mvmix" {
        run_mixed_views(args);
        return;
    }
    let n = args.rows;
    let bat = if args.batch == 0 { 1000 } else { args.batch };
    let mut ctrl =
        Client::connect_auth(&args.addr, 3000, args.auth.as_deref()).expect("bulk control conn");

    if args.bulk == "tail" {
        run_tail(args);
        return;
    }
    if args.bulk == "verifychurn" {
        if args.cycles == 0 {
            eprintln!("verifychurn 需要 --cycles >= 1");
            std::process::exit(2);
        }
        match verify_churn(&mut ctrl, n, args.cycles) {
            Ok(checked) => println!(
                "verifychurn rows={n} cycles={} checked={checked} errors=0",
                args.cycles
            ),
            Err(e) => {
                eprintln!("verifychurn: {e}");
                std::process::exit(1);
            }
        }
        return;
    }
    if args.bulk == "churn" && args.cycles == 0 {
        eprintln!("churn 需要 --cycles >= 1");
        std::process::exit(2);
    }

    // 每轮都从空表开始：否则第二轮量的就是「在已经空了的表上删」，那一档的
    // 数字非但不可比，还正好是最快最好看的那一支。
    let _ = ctrl.call(&Op::TableDrop { name: BULK_TABLE.to_vec() });
    let def = TableDef {
        name: BULK_TABLE.to_vec(),
        columns: vec![
            ColumnDef { name: b"tag".to_vec(), flags: COL_INDEX },
            ColumnDef { name: b"payload".to_vec(), flags: 0 },
        ],
    };
    match ctrl.call(&Op::TableCreate { def: def.clone() }) {
        Ok(Reply::Table(Some(_))) => {}
        Ok(other) => panic!("table create → {other:?}"),
        Err(e) => panic!("table create: {e}"),
    }

    // 灌数据在测量窗口外（分块 pipeline）。这一步自己也是数字：它是
    // 「逐行写」那一条的对照价。
    let tl = Instant::now();
    let s_load0 = stats_of(&mut ctrl);
    let mut got = 0u64;
    while got < n {
        let end = (got + 500).min(n);
        let batch: Vec<Op> = (got..end).map(|i| bulk_put(i, false)).collect();
        for r in ctrl.pipeline(&batch).expect("load pipeline") {
            if let Reply::Err { code, ref msg } = r {
                panic!("load 失败 code={code} msg={}", String::from_utf8_lossy(msg));
            }
        }
        got = end;
    }
    let load_ms = tl.elapsed().as_millis() as u64;
    let s_load1 = stats_of(&mut ctrl);
    if rows_of(&mut ctrl) != n {
        eprintln!("bulk: 灌了 {n} 行，表里只剩 {}", rows_of(&mut ctrl));
        std::process::exit(1);
    }

    // 构造这一档要跑的 op。churn 单独按半轮切 Phase，执行时有跨连接屏障。
    let mut ops: Vec<Op> = Vec::new();
    let mut churn_phases: Vec<Vec<Op>> = Vec::new();
    match args.bulk.as_str() {
        "rowdelete" => ops = (0..n).map(bulk_delete).collect(),
        "range" => {
            let mut start = 0u64;
            while start < n {
                let hi = (start + bat).min(n);
                ops.push(Op::RowRangeDelete {
                    table: BULK_TABLE.to_vec(),
                    column: ROW_PK,
                    lo: bulk_key(start),
                    hi: bulk_key(hi),
                    max: bat.min(u32::MAX as u64) as u32,
                });
                start = hi;
            }
        }
        "clear" => ops.push(Op::TableClear { table: BULK_TABLE.to_vec() }),
        // 影子表切换：先灌一张同形状的表（名字带 nonce），再一条 op 顶替。
        // 顶替那一刀只产一条记录；灌影子表的代价在 ops 里单列。
        "rename" => {
            let shadow = format!("bulkkv-{:x}", nonce()).into_bytes();
            let sdef = TableDef { name: shadow.clone(), ..def.clone() };
            match ctrl.call(&Op::TableCreate { def: sdef }) {
                Ok(Reply::Table(Some(_))) => {}
                other => panic!("shadow create → {other:?}"),
            }
            // 影子行的 payload 换一个标记前缀：两边内容相同时，剩下的行数、表名、
            // 记录数都对不出「看见的到底是不是新行」这件事，只在被顶替表还挂着
            // 的时候才露馅。收尾那一步要拿它做终态检查。
            let mut got = 0u64;
            while got < n {
                let end = (got + 500).min(n);
                let batch: Vec<Op> = (got..end)
                    .map(|i| Op::RowPut {
                        table: shadow.clone(),
                        key: bulk_key(i),
                        cols: vec![
                            format!("t{}", i % 16).into_bytes(),
                            format!("q{i}").into_bytes(),
                        ],
                        flags: 0,
                        cond_col: COND_NONE,
                        cond_val: Vec::new(),
                    })
                    .collect();
                for r in ctrl.pipeline(&batch).expect("shadow load") {
                    if let Reply::Err { code, ref msg } = r {
                        panic!("shadow load 失败 {code} {}", String::from_utf8_lossy(msg));
                    }
                }
                got = end;
            }
            // 影子表的灌入不计进这一档的时间：它属于调用方自己选的那条路
            // （要顶替就得先备好一张新表）。这里量的是「切换」这个动作。
            ops.push(Op::TableRename { from: shadow, to: BULK_TABLE.to_vec(), flags: 0 });
        }
        "putsame" => ops = (0..n).map(|i| bulk_put(i, false)).collect(),
        "putchanged" => ops = (0..n).map(|i| bulk_put(i, true)).collect(),
        "churn" => {
            for round in 1..=args.cycles {
                churn_phases.push((0..n).map(bulk_delete).collect());
                churn_phases.push((0..n).map(|i| bulk_churn_put(i, round)).collect());
            }
        }
        other => {
            eprintln!("unknown --bulk {other:?}\n{HELP}");
            std::process::exit(2);
        }
    }

    // 测量窗口。
    let conns = args.conns.max(1);
    let pipe = args.pipeline.max(1);
    let total = if args.bulk == "churn" {
        churn_phases.iter().map(Vec::len).sum()
    } else {
        ops.len()
    };
    let per = (total + conns - 1) / conns;
    let ops = std::sync::Arc::new(ops);
    // `rename` 只有一条 op：多开连接等于让七条线程去抢一个空区间。
    let conns = conns.min(total.max(1));
    let s0 = stats_of(&mut ctrl);
    let mut peak = BulkPeak::default();
    peak.observe(&s0);
    let mut sampler = start_bulk_sampler(args);
    let t0 = Instant::now();
    let (done, reported, errs) = if args.bulk == "churn" {
        run_churn_phases(args, &churn_phases, conns, pipe)
    } else {
        let mut handles = Vec::new();
        for k in 0..conns {
            let a = args.addr.clone();
            let auth = args.auth.clone();
            let ops = ops.clone();
            let lo = k * per;
            let hi = (lo + per).min(total);
            handles.push(std::thread::spawn(move || {
                let mut c = Client::connect_auth(&a, 3000, auth.as_deref()).expect("bulk conn");
                let (mut done, mut reported, mut errs) = (0u64, 0u64, 0u64);
                let mut i = lo;
                while i < hi {
                    let end = (i + pipe).min(hi);
                    let batch: Vec<Op> = ops[i..end].to_vec();
                    match c.pipeline(&batch) {
                        Ok(rs) => {
                            for r in rs {
                                match r {
                                    Reply::Count(v) => {
                                        done += 1;
                                        reported += v;
                                    }
                                    Reply::Ok | Reply::Row(_) | Reply::Table(_) => done += 1,
                                    Reply::Err { code, ref msg } => {
                                        errs += 1;
                                        eprintln!(
                                            "bulk reply err {code} {}",
                                            String::from_utf8_lossy(msg)
                                        );
                                    }
                                    other => {
                                        errs += 1;
                                        eprintln!("bulk reply shape {other:?}");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            errs += (end - i) as u64;
                            eprintln!("bulk pipeline: {e}");
                        }
                    }
                    i = end;
                }
                (done, reported, errs)
            }));
        }
        let mut sum = (0u64, 0u64, 0u64);
        for h in handles {
            let (d, r, e) = h.join().expect("bulk thread");
            sum.0 += d;
            sum.1 += r;
            sum.2 += e;
        }
        sum
    };
    let time_ms = t0.elapsed().as_millis() as u64;
    if let Some(s) = sampler.as_mut() {
        let sampled = s.stop();
        peak.samples += sampled.samples;
        peak.wal_bytes = peak.wal_bytes.max(sampled.wal_bytes);
        peak.wal_disk_bytes = peak.wal_disk_bytes.max(sampled.wal_disk_bytes);
        peak.snap_bytes = peak.snap_bytes.max(sampled.snap_bytes);
    }
    let s1 = stats_of(&mut ctrl);
    peak.observe(&s1);
    let left = rows_of(&mut ctrl);

    // 影子表那一档：被顶替的那张旧表必须消失，否则「旧行还在状态里」会被
    // `rows_left` 抓出来 —— 这正是这一档的回归闸门。
    let orphan = ctrl
        .call(&Op::TableList)
        .ok()
        .map(|r| match r {
            Reply::Tables(v) => v.iter().filter(|t| t.name.starts_with(b"bulkkv-")).count(),
            _ => usize::MAX,
        })
        .unwrap_or(usize::MAX);

    println!(
        "bulk mode={} rows={} batch={} cycles={} conns={} pipeline={} sample_ms={} \
         load_ms={} time_ms={} ops={}/{} reported={} records={} writes={} syncs={} \
         wal_bytes={} wal_start_bytes={} wal_end_bytes={} wal_peak_bytes={} \
         wal_disk_start_bytes={} wal_disk_end_bytes={} wal_disk_peak_bytes={} \
         checkpoints_start={} checkpoints_end={} checkpoints={} \
         snap_seq_start={} snap_seq_end={} snap_bytes_start={} snap_bytes_end={} \
         samples={} rows_left={} shadow_left={} errors={} rows_per_s={} ops_per_s={}",
        args.bulk,
        n,
        bat,
        args.cycles,
        conns,
        pipe,
        args.sample_ms,
        load_ms,
        time_ms.max(1),
        done,
        total,
        reported,
        s1.seq - s0.seq,
        s1.writes_total - s0.writes_total,
        s1.syncs_total - s0.syncs_total,
        s1.wal_bytes.saturating_sub(s0.wal_bytes),
        s0.wal_bytes,
        s1.wal_bytes,
        peak.wal_bytes,
        s0.wal_disk_bytes,
        s1.wal_disk_bytes,
        peak.wal_disk_bytes,
        s0.checkpoints,
        s1.checkpoints,
        s1.checkpoints.saturating_sub(s0.checkpoints),
        s0.snap_seq,
        s1.snap_seq,
        s0.snap_bytes,
        s1.snap_bytes,
        peak.samples,
        left,
        orphan,
        errs,
        n * 1000 / time_ms.max(1),
        done * 1000 / time_ms.max(1)
    );
    // 灌数据那一段的成本也报一次：逐行写与逐行删得用同一把尺子比。
    println!(
        "  load: records={} writes={} syncs={} ms={} rows_per_s={}",
        s_load1.seq - s_load0.seq,
        s_load1.writes_total - s_load0.writes_total,
        s_load1.syncs_total - s_load0.syncs_total,
        load_ms.max(1),
        n * 1000 / load_ms.max(1)
    );
    if errs != 0 || done != total as u64 {
        eprintln!("bulk: op 没跑完或出错（{done}/{total}，errors={errs}）");
        std::process::exit(1);
    }
    match args.bulk.as_str() {
        "rowdelete" | "range" | "clear" if left != 0 => {
            eprintln!("bulk: 删完还剩 {left} 行，这一档没到终态");
            std::process::exit(1);
        }
        // `rename` 的终态与删法相反：表里必须还是 N 行，而且每行都是影子表的
        // 内容 —— 顶替既要换掉旧行，又不能让表看起来空了。
        "rename" => {
            if left != n {
                eprintln!("bulk: rename 后 bulkkv 有 {left} 行，应为 {n}");
                std::process::exit(1);
            }
            let probe = Op::RowGet {
                table: BULK_TABLE.to_vec(),
                key: bulk_key(0),
            };
            match ctrl.call(&probe) {
                Ok(Reply::Row(Some(r))) if r.cols.get(1).map(|v| v.as_slice()) == Some(b"q0") => {}
                other => {
                    eprintln!("bulk: rename 后首行不是影子表的内容（{other:?}）");
                    std::process::exit(1);
                }
            }
        }
        _ => {}
    }
    if args.bulk == "rename" && orphan != 0 {
        eprintln!("bulk: 被顶替的影子表还在状态里（{orphan} 张）");
        std::process::exit(1);
    }
    if args.bulk == "churn" {
        let want = n.checked_mul(2).and_then(|v| v.checked_mul(args.cycles));
        if left != n || want != Some(s1.seq - s0.seq) || want != Some(s1.writes_total - s0.writes_total) {
            eprintln!("bulk: churn 终态/记录数不对（left={left}, seq={}, writes={}, want={want:?}）",
                s1.seq - s0.seq, s1.writes_total - s0.writes_total);
            std::process::exit(1);
        }
        if let Err(e) = verify_churn(&mut ctrl, n, args.cycles) {
            eprintln!("bulk: churn 内容验收失败：{e}");
            std::process::exit(1);
        }
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

    // 固定工作量档：与压测档互斥，跑完就走（不进下面那个时长循环）。
    if !args.bulk.is_empty() {
        run_bulk(&args);
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
                            table: row_table().to_vec(),
                            key: row_key(0),
                        },
                    ),
                    "rowlist" => (
                        "RowList(pk, limit=1)",
                        Op::RowList {
                            table: row_table().to_vec(),
                            column: ROW_PK,
                            value: Vec::new(),
                            from: Vec::new(),
                            limit: 1,
                        },
                    ),
                    "rowidx" => (
                        "RowList(tag=t0, limit=1)",
                        Op::RowList {
                            table: row_table().to_vec(),
                            column: ROW_TAG,
                            value: b"t0".to_vec(),
                            from: Vec::new(),
                            limit: 1,
                        },
                    ),
                    "rowidxrange" => (
                        "RowRange(tag [t0, t1), limit=1)",
                        Op::RowRange {
                            table: row_table().to_vec(),
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
                            table: row_table().to_vec(),
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
                            table: row_table().to_vec(),
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
                table: row_table().to_vec(),
                key: format!("n{nonce}-{t}-{s}").into_bytes(),
                cols: vec![format!("t{}", s % 16).into_bytes(), b"p".to_vec()],
                flags: PUT_IF_ABSENT,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
            "rowput" => row_op(s, span),
            "rowget" => Op::RowGet {
                table: row_table().to_vec(),
                key: row_key(s % span),
            },
            // 主键序第一页：`from` 空 = 从头。固定首页而不翻到底，是因为翻页的
            // 成本与「翻到第几页」无关（键游标直接从树上定位），而首页恰好是
            // `rowlist` 最常用的形状（挂载后第一次列目录）。
            "rowlist" => Op::RowList {
                table: row_table().to_vec(),
                column: ROW_PK,
                value: Vec::new(),
                from: Vec::new(),
                limit: 100,
            },
            // 二级索引等值页：16 个 tag 轮转，每个 tag 在 500 行里约 31 行 ——
            // 既不是「只命中一条」的快路径，也不会大到把回包帧长测成瓶颈。
            "rowidx" => Op::RowList {
                table: row_table().to_vec(),
                column: ROW_TAG,
                value: format!("t{}", s % 16).into_bytes(),
                from: Vec::new(),
                limit: 100,
            },
            "rowcount" => Op::RowCount {
                table: row_table().to_vec(),
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
                    table: row_table().to_vec(),
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
                table: row_table().to_vec(),
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
                        table: row_table().to_vec(),
                        key,
                        cols: vec![b"tchurn".to_vec(), b"p".to_vec()],
                        flags: 0,
                        cond_col: COND_NONE,
                        cond_val: Vec::new(),
                    }
                } else {
                    Op::RowDelete {
                        table: row_table().to_vec(),
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
  --views N          每线程视图数（默认 8）；mvseed/mvmix 档里是视图总数
  --range N          rowrange 的区间宽度（默认 64）
  --ino N            write/lock 打的 inode（默认 1 = 根）。给一个中段 ino 就是
                     「只改正文中段某个节点」的换代负载（replica_redo.sh 的 [mid]）
  --view-tag N       视图名用 bench<N>v* 而不是 bench<线程号>v*：所有线程共用
                     一个视图。造数据时用它把整份快照压进一棵子树，中段 ino 才
                     真落在正文中段
  --hold             只建立 N 条连接并保持，用于量服务端 RSS
  --bulk MODE        固定工作量档：灌 --rows 行再一次性处理完，
                     rowdelete|range|clear|rename|putsame|putchanged|churn。
                     churn 在同一键集上删后重插 --cycles 轮，每轮 2N 个 op；
                     verifychurn 只读核对重启后的最后一轮内容，不重新灌数据。
                     posixseed 造 N 个文件（各带唯一 data_ref）+ N 个空目录；
                     posixdelete 正好 N 个 Unlink + N 个 Rmdir，报账本收敛、
                     非空 NodeRef 数与 pipeline 摊还 p50/p99；posixverify 重开后逐 ref 核对。
                     mvseed 造 --views 个视图 × --rows 个文件（记录数 2N+3NW）；
                     mvmix 在那些视图上按「2 Lookup + 1 ReadDir + 1 Create + 1 Unlink」
                     成组并发 --secs 秒，每组一个 pipeline，跑完账本必须逐项回到
                     测量前，并报 records=5G / writes=2G / views_touched 与名字级核对。
                     tail 在自动 checkpoint 后插入并删除一个专用键，终态不变、
                     WAL 多两条记录，供 SIGKILL 后的快照+尾巴恢复验收。
                     报 time_ms / ops / records / writes / syncs / WAL 起止与峰值 /
                     checkpoint 起止 / rows_left；ops_per_s 的分母是实际完成 op 数
  --rows N           批量档的表规模（默认 200000）；mvseed/mvmix 档里是每视图的文件数
  --batch N          range 模式每批删多少行（默认 1000）
  --cycles N         churn 的删后重插轮数（默认 3，必须 >= 1）
  --sample-ms N      测量窗口内 Stats 采样间隔（默认 0 = 不采样）
  --auth-key-file P  每条连接先用这把密钥跑一次握手（§4.4）。不给 = 不握手。
                     服务端必须开着同一把密钥，否则报错退出（客户端
                     fail-closed，不会静默降级成不认证）。与 --hold 合用
                     就是「建连风暴里握手占多少」的口径

环境变量（只影响基准形状，不动线协议）：
  TREEDB_BENCH_WARM  预热条数 = 被测目录 / 表的规模（默认 500，最小 100）
  TREEDB_BENCH_TABLE 通用表档的表名（默认 benchkv）。同一服务端上并跑多个 bench
                     进程时必须各给一个，否则两边的 nonce 撞上就是一排 EEXIST
";
