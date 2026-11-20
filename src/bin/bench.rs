//! `treedb-bench`：对运行中的 `treedb-server` 打负载并汇报吞吐/延迟。
//!
//! 三个轴分别对应验收目标：
//! - `--op ping` / `--op stat`：纯读路径（不进 WAL），看事件循环本身的上限。
//! - `--op create` / `--op lookup` / `--op readdir`：目录树常见 op。
//! - `--op write`：`Setattr` 提交 data_ref，会动 refs 账本，是最重的写。
//! - `--conns N`：只把连接数堆到 N（配合 `--hold`）观察服务端 RSS。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use treedb::engine::model::{errno, KeyRec, Op, Reply};
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
        let auth = args.auth.clone();
        let nonce = nonce();
        handles.push(std::thread::spawn(move || {
            let mut cs: Vec<Client> = Vec::with_capacity(per_conn);
            for _ in 0..per_conn {
                cs.push(Client::connect_auth(&a, 0, auth.as_deref()).expect("connect"));
            }
            // 每线程独占一批视图，避免跨线程名字冲突污染语义。
            let mut vids: Vec<u32> = Vec::with_capacity(views);
            let mut missing: Vec<String> = Vec::new();
            for v in 0..views {
                let name = format!("bench{t}v{v}");
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
            // 计时窗口只包被测循环。分母不能在 join 前取：那样“建 N 条连接 +
            // 预热 500 个文件”这段固定开销会被算进去，--secs 3 时 readdir/lookup
            // 那一格的分母实测被撑到 6.2 s，吞吐虚低一倍；而旧版 pgbench 因为
            // 预热在父线程做完，同一处只撑到 3.05 s —— 两边口径不一致，对比结果
            // 就不可信。故每线程自己记窗口，汇总时取最大值当分母（比取均值
            // 保守），treedb-pgbench 同步改成这个口径。
            let wstart = Instant::now();
            let deadline = wstart + dur;
            while Instant::now() < deadline {
                let view = vids[(seq as usize) % vids.len()];
                let batch = build(&op, view, seq, pipeline, t, nonce, warm_span() as u64);
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
fn build(op: &str, view: u32, seq: u64, n: usize, t: usize, nonce: u64, span: u64) -> Vec<Op> {
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
                ino: 1,
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
                ino: 1,
                owner: format!("owner{t}").into_bytes(),
                verb: 0,
                kind: 1,
                lease_expiry: 0,
            },
            "key" => Op::KeyCreate {
                key: KeyRec {
                    id: format!("k{t}{nonce}{s}").into_bytes(),
                    label: b"bench".to_vec(),
                    secret_hash: format!("h{s}").into_bytes(),
                    created_at: 0,
                    last_used_at: 0,
                    revoked_at: 0,
                    view: Vec::new(),
                    ro: false,
                    owner: Vec::new(),
                    note: Vec::new(),
                    expires_at: 0,
                    created_by: Vec::new(),
                    secret_hint: Vec::new(),
                },
            },
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
  --op NAME          create|lookup|readdir|write|lock|key|ping|stat
  --conns N          总连接数（默认 8）
  --threads N        工作线程数（默认 8）
  --pipeline N       每批请求数，1 = 一问一答（默认 1）
  --secs N           持续秒数（默认 5）
  --views N          每线程视图数（默认 8）
  --hold             只建立 N 条连接并保持，用于量服务端 RSS
  --auth-key-file P  每条连接先用这把密钥跑一次握手（§4.4）。不给 = 不握手。
                     服务端必须开着同一把密钥，否则报错退出（客户端
                     fail-closed，不会静默降级成不认证）。与 --hold 合用
                     就是「建连风暴里握手占多少」的口径
";
