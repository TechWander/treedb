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
//! `replica = yes (...)`，里面有 lag、重装次数和失败次数。本进程自己的 fencing
//! token 只在本副本的启动 banner 里（`fence=N`）—— `Stats` 的字段布局这一版
//! 刻意没动，免得再挖一个“旧客户端静默丢尾部字段”的坑（§4.1）。
//!
//! 同一个二进制还带一个**离线**动作：`treedb-standby --dir D --promote` 把 D 的
//! fencing token 抬一格后退出，不开服务（§6.6 第 3 条）。切主因此是两步：先停
//! 旧主，再 promote 副本目录，最后用 `treedb-server` 把新主开起来。为什么不做成
//! “就地变成可写服务端”：那样同一条命令行里 `--primary` 与提升的语义会互相
//! 矛盾，而且提升之后的第一件事往往是把旧主停掉，不该被隐式决定。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use treedb::engine::PROTOCOL_VERSION;
use treedb::net::auth::Key;
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
    let mut auth_key_file: Option<PathBuf> = None;
    let mut promote = false;

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
            "--interval-ms" => interval_ms = num("--interval-ms", &next(&mut i), interval_ms),
            "--fetch-bytes" => fetch_bytes = num("--fetch-bytes", &next(&mut i), fetch_bytes),
            "--timeout-ms" => timeout_ms = num("--timeout-ms", &next(&mut i), timeout_ms),
            "--sync-ms" => sync_ms = num("--sync-ms", &next(&mut i), sync_ms),
            "--auto-checkpoint-mb" => {
                auto_ckpt_mb = num("--auto-checkpoint-mb", &next(&mut i), auto_ckpt_mb)
            }
            "--backlog" => backlog = num("--backlog", &next(&mut i), backlog),
            "--workers" => workers = num("--workers", &next(&mut i), workers),
            "--pid-file" => pid_file = Some(PathBuf::from(next(&mut i))),
            "--auth-key-file" => auth_key_file = Some(PathBuf::from(next(&mut i))),
            "--promote" => promote = true,
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

    // `--promote`：把这份副本目录提成主（DESIGN.md §6.6 第 3 条）。
    //
    // 它**不起服务**，只改盘上的一个数然后退出：提升是运维动作，什么时候把
    // 新主开起来、客户端什么时候改地址，都不该被这个命令隐式决定。做成
    // “就地变成可写服务端”的话，`--primary` 这个参数在同一份命令里语义会
    // 自相矛盾（既追别人又当主），而且提升后的第一件事往往是把旧主停掉。
    if promote {
        if primary.is_some() {
            eprintln!("--promote does not follow anything: drop --primary (this dir becomes the primary)\n\n{HELP}");
            std::process::exit(2);
        }
        let durability = parse_durability(&durability);
        let mut eng = match treedb::engine::Engine::open(&dir, durability) {
            Ok(e) => e,
            Err(e) => {
                // 目录被一个活着的实例占着（另一个进程 / 另一个副本）：提升它会
                // 让两个进程对“谁是主”有两种答案。直接拒。
                eprintln!("treedb-standby --promote: cannot open {}: {e}", dir.display());
                std::process::exit(1);
            }
        };
        let was = eng.fence();
        let snap_seq = eng.recovery().snap_seq;
        let wal_records = eng.recovery().wal_records;
        match eng.promote() {
            Ok(now) => {
                println!(
                    "promoted {}: fence {was} -> {now} (snap_seq {snap_seq}, {wal_records} wal recs replayed)",
                    dir.display()
                );
                println!(
                    "this directory is now the highest-ranked copy; start a primary on it with\n  \
                     treedb-server --dir {} --listen HOST:PORT",
                    dir.display()
                );
                println!(
                    "any replica still pointed at the OLD primary will refuse its batches \
                     (its fence is {now}) until it is repointed here"
                );
                return;
            }
            Err(e) => {
                eprintln!("treedb-standby --promote: {e}");
                std::process::exit(1);
            }
        }
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

    // 一把密钥同时管两头：对主握手（它是主的客户端）、以及要自己的读者
    // 握手（它同时是个服务端）。拆成两个 flag 的话，“开了认证却把副本
    // 自己的端口默默留在开门状态”这种组合一定是被忘出来的，不是被设出来的。
    let auth: Option<std::sync::Arc<Key>> = match &auth_key_file {
        Some(p) => match Key::load(p) {
            Ok(k) => Some(std::sync::Arc::new(k)),
            Err(e) => {
                eprintln!("bad --auth-key-file: {e}");
                std::process::exit(2);
            }
        },
        None => None,
    };
    let auth_on = auth.is_some();

    let mut rcfg = ReplicaCfg::new(primary);
    rcfg.interval_ms = interval_ms;
    rcfg.max_bytes = fetch_bytes;
    rcfg.timeout_ms = timeout_ms;
    rcfg.auth = auth.clone();
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
    cfg.auth = auth;

    // 令牌要进 banner，但引擎在 `serve` 里才打开（`open` 会抢目录锁）。
    // 直接读那个小文件：它和引擎启动时读的是同一个函数，所以 banner 上
    // 这个数与随后 `Engine::open` 拿到的值不会不一致。读坏了就退出 ——
    // 这和引擎的选择一致（坏令牌不当 0 用），也避免了“banner 打 0、
    // 服务却起不来”这种看起来像 bug 的输出。
    let fence = match treedb::engine::read_fence(&dir) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("treedb-standby: {e}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "treedb-standby pid={} listen={} dir={} primary={} protocol={} interval={} ms fetch={} B workers={} auth={} fence={}",
        std::process::id(),
        addr,
        dir.display(),
        primary,
        PROTOCOL_VERSION,
        interval_ms,
        fetch_bytes,
        workers,
        if auth_on { "on" } else { "off" },
        fence
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

/// 数值参数：缺值/不是数都是启动错误。以前是 `.parse().unwrap_or(默认值)`，
/// 于是 `--workers abc` 会默默跑成 auto —— 副本的并行度不是运维以为的那个值，
/// 而日志里一个字都不会提。
fn num<T: std::str::FromStr>(flag: &str, raw: &str, default: T) -> T {
    if raw.is_empty() {
        return default;
    }
    match raw.parse() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("bad {flag} {raw:?}: expected a number\n\n{HELP}");
            std::process::exit(2);
        }
    }
}

fn parse_durability(s: &str) -> treedb::engine::Durability {
    // 与 treedb-server 共用引擎里的解析器：拼错不静默降级。
    use treedb::engine::Durability;
    match Durability::parse(s) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{e}\n\n{HELP}");
            std::process::exit(2);
        }
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
  --promote                  不开服务：只把本目录的 fencing token 抬到“见过的最大值
                             + 1”然后退出（§6.6 第 3 条）。跑完之后用
                             treedb-server --dir <同一个目录> 把新主开起来。
                             不要同时给 --primary：提升一个目录与“跟在谁后面”
                             是两件事，同时给会自相矛盾。
                             旧主如果还活着，它自己不会知道已被换掉，也不会停止对
                             客户端服务 —— fencing 只让副本拒收它的批，挡不住它的
                             读者。所以旧主必须真停掉（或者把它的客户端改指到新
                             主），而不是只靠抬令牌。
                             也别把新主的 fence 文件改小：那等于把旧主的权又开回来。
                             任何仍指着旧主的副本会拒收它的批（日志里报
                             `is fenced out`），需要改指到新主。
  --auth-key-file PATH       集群共享密钥（§4.4），一个 flag 管两头：对主握手，
                             并要求连到本副本的读者也握手。不给 = 不认证。
                             密钥必须与主的 `--auth-key-file` 是同一把，否则表
                             现是 lag 永远追不上（副本会指数退避重试并在日志里
                             报 unusable，不会默默降级）。

监控：treedb-server --listen <本副本地址> --stats，副本会多一行
  replica = yes (primary seq .. lag .. applied .. resyncs .. errors ..)
resyncs 持续上涨 = 追不上主（主 checkpoint 太快或 fetch-bytes 太小）；
errors 持续上涨 = 连不上主或主拒了请求，此时副本仍在用已有状态答读。
";
