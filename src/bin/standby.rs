//! `treedb-standby`：一个只读副本实例（DESIGN.md §6.6）。
//!
//! ```text
//! treedb-standby --primary 10.0.0.1:7777 --dir /var/lib/treedb-replica \
//!                --listen 127.0.0.1:7778
//! ```
//!
//! 它就是一个把 `replication` 配上了的 [`serve`]：同一套 epoll 循环、同一套
//! 帧编解码、同一套 `Engine`，因此客户端连它和连主的协议体验完全一致 —— 区别
//! 只有两条：写会被 `EROFS` 挡在引擎层，以及每轮循环末尾多一次追平 tick。
//!
//! 启动顺序也因此不需要额外参数：目录里有什么就先用什么。冷启动（目录空）时
//! 它先向主要一份快照，追平之前对读的答案是"空库"而不是"报错"，这一点与主
//! 冷启动的语义一致，脚本里 `wait for port + read` 那种轮询不会看到 ECONNREFUSED
//! 之外的怪状态。
//!
//! 监控走 `treedb-server --listen <副本地址> --stats`：副本会多打一行
//! `replica = yes (...)`，里面有 lag、重装次数和失败次数。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use treedb::engine::PROTOCOL_VERSION;
use treedb::net::replicate::{Follower, ReplicaCfg};
use treedb::net::server::{serve, ServerConfig};

static STOP: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);

extern "C" fn on_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut primary: Option<String> = None;
    let mut dir = PathBuf::from("/var/lib/treedb-replica");
    let mut listen = "127.0.0.1:7778".to_string();
    let mut durability = "group".to_string();
    let mut interval_ms: u64 = 20;
    let mut fetch_bytes: u32 = 1 << 20;
    let mut timeout_ms: u64 = 5_000;
    let mut sync_ms: i32 = 20;
    let mut auto_ckpt_mb: u64 = 64;
    let mut backlog: i32 = 4096;
    let mut workers: usize = 0;
    let mut pid_file: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--primary" => primary = Some(next(&mut i)),
            "--dir" => dir = PathBuf::from(next(&mut i)),
            "--listen" => listen = next(&mut i),
            "--durability" => durability = next(&mut i),
            "--interval-ms" => interval_ms = next(&mut i).parse().unwrap_or(20),
            "--fetch-bytes" => fetch_bytes = next(&mut i).parse().unwrap_or(1 << 20),
            "--timeout-ms" => timeout_ms = next(&mut i).parse().unwrap_or(5_000),
            "--sync-ms" => sync_ms = next(&mut i).parse().unwrap_or(20),
            "--auto-checkpoint-mb" => auto_ckpt_mb = next(&mut i).parse().unwrap_or(64),
            "--backlog" => backlog = next(&mut i).parse().unwrap_or(4096),
            "--workers" => workers = next(&mut i).parse().unwrap_or(0),
            "--pid-file" => pid_file = Some(PathBuf::from(next(&mut i))),
            "-h" | "--help" => {
                println!("{}", HELP);
                return;
            }
            other => {
                eprintln!("unknown argument: {other}\n\n{HELP}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let Some(p) = primary else {
        eprintln!("--primary HOST:PORT is required (a replica without a primary is just a stale directory)\n\n{HELP}");
        std::process::exit(2);
    };
    let addr: SocketAddr = match listen.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("bad --listen {listen}: {e}");
            std::process::exit(2);
        }
    };
    let primary: SocketAddr = match p.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("bad --primary {p}: {e}");
            std::process::exit(2);
        }
    };
    if primary == addr {
        // 自己拉自己会在第一次 gap 上死循环（要的水位正是自己刚装的那份），
        // 而这个循环看起来只会表现为"lag 不动"。挡在启动前比挡在日志里好。
        eprintln!("--primary must not equal --listen: a replica cannot follow itself");
        std::process::exit(2);
    }

    let mut rcfg = ReplicaCfg::new(primary);
    rcfg.interval_ms = interval_ms;
    rcfg.max_bytes = fetch_bytes;
    rcfg.timeout_ms = timeout_ms;
    if let Err(e) = Follower::check_cfg(&rcfg) {
        eprintln!("{e}");
        std::process::exit(2);
    }

    if workers == 0 {
        workers = treedb::net::default_workers();
    }
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as libc::size_t);
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::size_t);
        // 主宕机时副本的拉取连接会写出 RST：SIGPIPE 不该打死一个还在答读的服务。
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    if let Some(pf) = &pid_file {
        let _ = std::fs::write(pf, std::process::id().to_string());
    }

    let mut cfg = ServerConfig::new(addr, dir.clone());
    cfg.durability = parse_durability(&durability);
    cfg.sync_ms = sync_ms;
    cfg.auto_checkpoint_bytes = auto_ckpt_mb * 1024 * 1024;
    cfg.backlog = backlog;
    cfg.workers = workers;
    cfg.replication = Some(rcfg.clone());

    eprintln!(
        "treedb-standby pid={} listen={} dir={} primary={} protocol={} interval={} ms fetch={} B workers={}",
        std::process::id(),
        addr,
        dir.display(),
        primary,
        PROTOCOL_VERSION,
        interval_ms,
        fetch_bytes,
        workers
    );
    match serve(&cfg, &STOP, &LIVE) {
        Ok(rep) => {
            eprintln!(
                "shutdown: recovered {} wal recs ({} ms, snap {}) | accepted {} peak {} | rounds {} worst {} us | ops {} writes {} syncs {} avg {} us | checkpoints {} | replica applied {} lag {} resyncs {} errors {}",
                rep.rec.wal_records,
                rep.rec.elapsed_ms,
                rep.rec.snap_seq,
                rep.accepted,
                rep.peak_conns,
                rep.rounds,
                rep.worst_round_us,
                rep.ops,
                rep.writes,
                rep.syncs,
                rep.avg_sync_us,
                rep.checkpoints,
                rep.replica_applied,
                rep.replica_lag,
                rep.replica_resyncs,
                rep.replica_errors
            );
        }
        Err(e) => {
            // 只有「主交来的快照比自己还旧」这一种会走到这里（见 net::replicate
            // 模块文档）：多半是连错了主。状态可能已经超前于磁盘，不能当正常退出。
            eprintln!("treedb-standby FATAL: {e}");
            std::process::exit(1);
        }
    }
}

fn parse_durability(s: &str) -> treedb::engine::Durability {
    use treedb::engine::Durability;
    match s {
        "always" => Durability::Always,
        "never" => Durability::Never,
        _ => Durability::Group,
    }
}

const HELP: &str = "\
treedb-standby — 只读副本（pull 式日志传送，DESIGN.md §6.6）

  --primary HOST:PORT        必填，主的地址。副本单向拉它，主不需要知道副本存在
  --dir PATH                 副本自己的数据目录，默认 /var/lib/treedb-replica。
                             必须与主的目录分开：各自有 WAL 与快照，落盘的字节
                             可以直接跟主的比对
  --listen HOST:PORT         对客户端的监听地址，默认 127.0.0.1:7778
  --interval-ms N            两次追平之间的最小间隔，默认 20。下载快照期间不限频
  --fetch-bytes N            一次往来的负载上限（字节），默认 1048576，上限看
                             协议里的 WAL_FETCH_MAX。追得慢就调大，但一次 tick
                             会占住副本的事件循环一轮
  --timeout-ms N             到主的连接与读写超时，默认 5000。事件循环里跑，
                             必须有上限，否则一个不答话的主会把读全钉死
  --durability NAME          group（默认）| always | never。副本的 WAL 记的是主
                             交来的记录，掉电后靠主的快照重同步也能回来，所以
                             这里换吞吐不丢正确性
  --sync-ms N                事件循环空闲唤醒间隔，默认 20
  --auto-checkpoint-mb N     副本 WAL 超此大小自动快照，默认 64（0 = 关闭）。
                             别关：副本的 WAL 只会因为追平而增长
  --backlog N                listen 接受队列长度，默认 4096
  --workers N                前端 worker 线程数，默认 0 = auto。只扩展读；写永远
                             被引擎以 EROFS 挡掉，不会到提交线程
  --pid-file PATH            启动后写入 pid

监控：treedb-server --listen <本副本地址> --stats，副本会多一行
  replica = yes (primary seq .. lag .. applied .. resyncs .. errors ..)
resyncs 持续上涨 = 追不上主（主 checkpoint 太快或 fetch-bytes 太小）；
errors 持续上涨 = 连不上主或主拒了请求，此时副本仍在用已有状态答读。
";
