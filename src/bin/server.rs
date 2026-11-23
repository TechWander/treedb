//! `treedb-server`：起一个元数据引擎实例，或对运行中的实例做状态查询。
//!
//! ```text
//! treedb-server --dir /data/treedb --listen 127.0.0.1:7777 [--durability group]
//! treedb-server --listen 127.0.0.1:7777 --stats          # 查询并退出
//! treedb-server --listen 127.0.0.1:7777 --shutdown       # 优雅停服
//! ```
//!
//! 信号：`SIGINT`/`SIGTERM` 走优雅退出路径（补 fsync + checkpoint 后返回），
//! 不是直接 kill —— 崩溃安全性由 `kill -9` 的验收测试专门覆盖。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use treedb::engine::{Durability, Op, Reply, PROTOCOL_VERSION};
use treedb::net::auth::Key;
use treedb::net::client::Client;
use treedb::net::server::{serve, ServerConfig};

static STOP: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);

extern "C" fn on_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn parse_durability(s: &str) -> Durability {
    // 解析器在引擎里（副本二进制共用同一份）：拼错不再是"静默降级成 group"，
    // 而是退出码 2。
    match Durability::parse(s) {
        Ok(d) => d,
        Err(e) => die(&e.to_string()),
    }
}

/// 参数错误一律退出码 2（POSIX 的 usage error），与运行期 FATAL 的 1 分开。
fn die(msg: &str) -> ! {
    eprintln!("{msg}\n\n{HELP}");
    std::process::exit(2);
}

/// 数值参数：缺值/不是数/越界都是启动错误。
/// 以前是 `.parse().unwrap_or(默认值)`，于是 `--workers abc` 会默默跑成 auto，
/// `--sync-ms 0` 与不给完全是两件事（前者关掉周期 sync），都不该靠猜。
fn num<T: std::str::FromStr>(flag: &str, raw: &str, default: T) -> T {
    if raw.is_empty() {
        return default;
    }
    match raw.parse() {
        Ok(v) => v,
        Err(_) => die(&format!("bad {flag} {raw:?}: expected a number")),
    }
}

fn print_stats(addr: &str, key: Option<&Key>, prometheus: bool) -> Result<(), String> {
    let mut c = Client::connect_auth(addr, 3000, key).map_err(|e| e.to_string())?;
    match c.call(&Op::Stats).map_err(|e| e.to_string())? {
        Reply::Stats(s) => {
            if prometheus {
                // 整个指标块一次写完：中间不能夹任何进度输出，否则抓取方会
                // 把提示文字当成解析失败的注释行。
                print!("{}", treedb::metrics::prometheus(&s));
                return Ok(());
            }
            println!("views      = {}", s.views);
            println!("nodes      = {}", s.nodes);
            println!("dentries   = {}", s.dentries);
            println!("locks      = {}", s.locks);
            println!("refs       = {}", s.refs);
            println!("tables     = {} (rows {})", s.tables, s.rows);
            println!("seq        = {} (snapshot {})", s.seq, s.snap_seq);
            // 两个长度各说一件事：逻辑长度决定 checkpoint 何时触发，磁盘长度
            // 决定这块盘被占了多少（预分配的空洞差一个数量级）。
            println!(
                "wal        = {} B logical, {} B on disk",
                s.wal_bytes, s.wal_disk_bytes
            );
            println!("snapshot   = {} B", s.snap_bytes);
            println!("protocol   = {}", s.proto_version);
            println!(
                "ops        = {} (writes {}, syncs {}, avg fsync {} us)",
                s.ops_total, s.writes_total, s.syncs_total,
                if s.syncs_total > 0 { s.sync_us_total / s.syncs_total } else { 0 }
            );
            println!("checkpoints= {}", s.checkpoints);
            // 只在副本上打印：主那边这几个数永远是 0，多打几行只会让运维
            // 以为“0 lag”是一种健康状态。
            if s.replica {
                println!(
                    "replica    = yes (primary seq {} lag {} applied {} resyncs {} errors {})",
                    s.replica_primary_seq, s.replica_lag, s.replica_applied,
                    s.replica_resyncs, s.replica_errors
                );
            }
            Ok(())
        }
        Reply::Err { code, msg } => Err(format!("server error {code}: {}", String::from_utf8_lossy(&msg))),
        other => Err(format!("unexpected reply {other:?}")),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir = PathBuf::from("/var/lib/treedb");
    let mut listen = "127.0.0.1:7777".to_string();
    let mut durability = "group".to_string();
    let mut sync_ms: i32 = 20;
    let mut auto_ckpt_mb: u64 = 64;
    let mut backlog: i32 = 4096;
    let mut max_conn: usize = 0;
    let mut lock_sweep_ms: i32 = 1000;
    let mut workers: usize = 0;
    let mut flush_pipeline = false;
    let mut stats_only = false;
    let mut shutdown_only = false;
    let mut pid_file: Option<PathBuf> = None;
    let mut auth_key_file: Option<PathBuf> = None;
    let mut archive: Option<PathBuf> = None;
    let mut format = "text".to_string();
    let mut format_given = false;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--dir" => dir = PathBuf::from(next(&mut i)),
            "--listen" => listen = next(&mut i),
            "--durability" => durability = next(&mut i),
            "--sync-ms" => sync_ms = num("--sync-ms", &next(&mut i), sync_ms),
            "--auto-checkpoint-mb" => {
                auto_ckpt_mb = num("--auto-checkpoint-mb", &next(&mut i), auto_ckpt_mb)
            }
            "--backlog" => backlog = num("--backlog", &next(&mut i), backlog),
            "--max-conn" => max_conn = num("--max-conn", &next(&mut i), max_conn),
            "--lock-sweep-ms" => {
                lock_sweep_ms = num("--lock-sweep-ms", &next(&mut i), lock_sweep_ms)
            }
            "--workers" => workers = num("--workers", &next(&mut i), workers),
            "--flush-pipeline" => flush_pipeline = true,
            "--pid-file" => pid_file = Some(PathBuf::from(next(&mut i))),
            "--auth-key-file" => auth_key_file = Some(PathBuf::from(next(&mut i))),
            "--archive-dir" => archive = Some(PathBuf::from(next(&mut i))),
            "--stats" => stats_only = true,
            "--format" => {
                format = next(&mut i);
                format_given = true;
            }
            "--shutdown" => shutdown_only = true,
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

    // 缺值（`--dir` 后面没东西）也是错误：`next()` 会回空串，不能让
    // `--dir` 空着跑成默认目录。
    if dir.as_os_str().is_empty() {
        die("empty --dir");
    }

    // 0 = auto，与 [`ServerConfig::new`] 同一个解析函数：两处各写一套
    // 默认值的话，嵌入式用法与两行命令拉起的实例性能特征就会不一。
    if workers == 0 {
        workers = treedb::net::default_workers();
    }

    // 密钥先于一切加载：一个被要它认证、却因为密钥文件读不到而默默降级成
    // 不认证的实例，比拒接启动坏得多。`--stats` / `--shutdown` 也要用它，
    // 所以放在那两个分支之前。
    let auth: Option<Arc<Key>> = match &auth_key_file {
        Some(p) => match Key::load(p) {
            Ok(k) => Some(Arc::new(k)),
            Err(e) => {
                eprintln!("bad --auth-key-file: {e}");
                std::process::exit(2);
            }
        },
        None => None,
    };
    let auth_on = auth.is_some();

    // `--format` 只影响 `--stats` 的输出形状。给了它却忘了 `--stats` 的话，
    // 用户以为自己在抓指标、实际上起了一个服务端 —— 那种误会必须在参数
    // 层就杀掉，不能等到“怎么一直不退出”才发现。
    let format_prom = match format.as_str() {
        "text" => false,
        "prometheus" => true,
        other => die(&format!("bad --format {other:?}: expected text or prometheus")),
    };
    if format_given && !stats_only {
        die("--format only applies to --stats");
    }

    if stats_only {
        match print_stats(&listen, auth.as_deref(), format_prom) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("stats: {e}");
                if format_prom {
                    // 抓取失败也得有一个样本，否则面板上是 no data（看起来像
                    // “监控没配好”）而不是 down（那才是真相）。退出码仍是 1。
                    print!("{}", treedb::metrics::down());
                }
                std::process::exit(1);
            }
        }
        return;
    }
    if shutdown_only {
        match Client::connect_auth(&listen, 3000, auth.as_deref())
            .and_then(|mut c| c.call(&Op::Shutdown))
        {
            Ok(Reply::Ok) => println!("shutdown acknowledged"),
            Ok(other) => eprintln!("unexpected reply: {other:?}"),
            Err(e) => eprintln!("shutdown: {e}"),
        }
        return;
    }

    let addr: SocketAddr = match listen.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("bad --listen {listen}: {e}");
            std::process::exit(2);
        }
    };
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as libc::size_t);
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::size_t);
        // 半写连接不要拿 SIGPIPE 打死服务。
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    if let Some(p) = &pid_file {
        let _ = std::fs::write(p, std::process::id().to_string());
    }

    let cfg = ServerConfig {
        listen: addr,
        dir: dir.clone(),
        durability: parse_durability(&durability),
        sync_ms,
        auto_checkpoint_bytes: auto_ckpt_mb * 1024 * 1024,
        backlog,
        max_conn,
        workers,
        flush_pipeline,
        // 这个二进制永远是主：副本入口是 `treedb-standby`，它把同一个
        // `serve` 配上 `replication` 就跑起来。两边各留一套 flag 解析的话，
        // 副本会拿到一份没人测过的默认值。
        replication: None,
        auth,
        archive: archive.clone(),
        lock_sweep_ms,
    };
    eprintln!(
        // 帧里没有版本位（§4.1），升级顺序又是硬约束，所以 banner 一开就把它摆出来：
        // 运维在两端对不上时第一眼就该看见。
        "treedb-server pid={} listen={} dir={} protocol={} durability={} backlog={} max_conn={} workers={} auth={} archive={}",
        std::process::id(),
        addr,
        dir.display(),
        PROTOCOL_VERSION,
        durability,
        backlog,
        if max_conn == 0 { "unlimited".to_string() } else { max_conn.to_string() },
        workers,
        if auth_on { "on" } else { "off" },
        match &archive {
            Some(a) => a.display().to_string(),
            None => "off".to_string(),
        }
    );
    match serve(&cfg, &STOP, &LIVE) {
        Ok(rep) => {
            eprintln!(
                "shutdown: recovered {} wal recs ({} ms, snap {}) | accepted {} refused {} peak {} | rounds {} worst {} us | ops {} writes {} syncs {} avg {} us | checkpoints {} | {} reads inline on {} workers, {} ops forwarded",
                rep.rec.wal_records,
                rep.rec.elapsed_ms,
                rep.rec.snap_seq,
                rep.accepted,
                rep.refused,
                rep.peak_conns,
                rep.rounds,
                rep.worst_round_us,
                rep.ops,
                rep.writes,
                rep.syncs,
                rep.avg_sync_us,
                rep.checkpoints,
                rep.reads_inline,
                rep.workers,
                rep.forwarded
            );
        }
        Err(e) => {
            // WAL fsync 失败等：状态可能已超前于磁盘，绝不能当成正常退出。
            eprintln!("treedb-server FATAL: {e}");
            std::process::exit(1);
        }
    }
}

const HELP: &str = "\
treedb-server — 元数据引擎服务端（POSIX 元数据语义，客户端无关）

  --dir PATH                 数据目录（WAL + 快照），默认 /var/lib/treedb
  --listen HOST:PORT         监听地址，默认 127.0.0.1:7777
  --durability NAME          group（默认，每轮一次 fsync）| always（与 group 同一个
                             落盘点：已 ack 的写必已落盘，不再按连接串 fsync）| never
  --backlog N                listen 接受队列长度，默认 4096（受 somaxconn 截断）
  --max-conn N               同时在册的连接数上限，默认 0 = 不限。超出的连接会被
                             接受后立刻关闭（客户端看到 EOF），并在服务端日志
                             限速打一行；总数在停服汇总的 refused 里
  --workers N                前端 worker 线程数，默认 0 = auto（按核数取
                             min(8, 核数/2)，至少 1）。=1 就是单线程事件
                             循环，也是全部历史基线数字的档。>1 时每条线程
                             自己 accept、自己收发，只读 op 就地并行算；写永远
                             由唯一提交线程定序，group commit 仍然跨连接合并成
                             一次 fsync。写多读少的负载可以显式收到 1：并行
                             只偷得到读，且每个 worker 要多占 3 个 fd。
  --sync-ms N                事件循环空闲唤醒间隔，默认 20
  --lock-sweep-ms N          过期租约回收的间隔，默认 1000（0 = 关）。读路径一直
                             把过期租约当不存在，但记录本身只在被覆盖或 Close 时
                             消失；这条周期任务把它真正删掉（Log::DelLock，参与
                             WAL 与副本收敛），否则 locks 只增不减、既占内存也
                             进快照。每次最多看 1024 条，一圈的成本摊在多轮上；
                             副本上这个旋钮无效。
  --flush-pipeline           写轴流水线（§11 第 8 条）：提交线程只 append，
                             fdatasync 交给专用线程，落盘回执到了再 ack。
                             默认关：实测在 fsync 1.3–1.6 ms 的盘上是净负收益
                             （多一次线程往返，而 fsync 期间并没有可重叠的活），
                             只在该盘 fsync 占比高、或单个 op 的 translate 很重时
                             才值得打开。打开后 ack 语义不变（仍严格晚于落盘）。
  --auto-checkpoint-mb N     WAL 超此大小自动快照，默认 64（0 = 关闭）
  --pid-file PATH            启动后写入 pid，便于测试脚本 kill -9
  --archive-dir PATH         开启 WAL 归档（§6.11）：每次 checkpoint 先把要抹掉的
                             那段日志与旧快照拷进这个目录，然后才动源目录。
                             给了它，`treedb-pitr` 才能回到任意一个已归档的水位；
                             不给 = 不归档，与以前逐字一致。归档失败会让 checkpoint
                             失败（历史不能静默丢），所以要给一个与数据目录不同的
                             可写目录，并保证磁盘余量。开关要一直开着：中间关过一段，
                             那几次 checkpoint 就会在归档里留一个洞。
  --auth-key-file PATH       启用接入认证（§4.4）：从文件读集群共享密钥，必须是
                             32 原始字节或 64 个十六进制字符（0/64 个零 = 没填的
                             模板，拒接）。不给这个 flag = 不认证，与 v4 逐字一致。
                             开了之后所有客户端（含 --stats / --shutdown）都得带
                             同一把密钥跑完握手才能发数据 op。
  --stats                    不启服务，连上去打印状态后退出（含表的行数、WAL 的
                             逻辑与磁盘两个长度、快照长度与协议版本）
  --format NAME              --stats 的输出形状：text（默认，逐行 key = value）|
                             prometheus（text exposition 0.0.4，直接喂监控；
                             连不上时输出一行 `treedb_up 0` 并以 1 退出）
  --shutdown                 通知运行中的实例优雅停服
";
