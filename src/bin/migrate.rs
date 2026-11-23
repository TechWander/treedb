//! `treedb-migrate`：把上一代快照布局的数据目录升到当前布局（DESIGN.md §6.5）。
//!
//! ```text
//! treedb-migrate --dir /var/lib/treedb              # 只看，一个字节都不写
//! treedb-migrate --dir /var/lib/treedb --apply      # 真改：先落备份，再原子重写
//! treedb-migrate --dir /var/lib/treedb --apply --force   # 连既有备份一起换掉
//! ```
//!
//! 存在的理由：`SNAP_VERSION` 每一次抬代（2 → 3 加通用表段、3 → 4 删四张专用表段）
//! 之后，上一代二进制跑过的数据目录在新二进制前**起不来** —— 版本号闸门拒收旧快照，
//! 而拒收比静默解错正确。这个工具就是那道闸门的合法出路：读旧布局 → 内存状态 →
//! 按新布局写回。当前的上一代是 v3；v2 及更早已经没有迁移路径。
//!
//! 三条硬规矩，都由 [`treedb::engine::core::migrate_snapshot`] 守着：
//!
//! 1. **WAL 不动一个字节**。所以已 ack 的写不会因为迁移而丢，回滚也只是把
//!    备份文件挪回去再跑旧二进制 —— 但这个回滚窗口只到新二进制**第一次
//!    checkpoint 为止**：checkpoint 会 `wal.reset()` 把上一代记录物理抹掉，此后
//!    新二进制已吸收的那些写就只存在新布局快照里，旧二进制读不到（`benches/rehearse_migrate.sh`
//!    把这两个窗口都量过）。
//! 2. **动手前先把输入看清**：版本不认识、正文与版本位不吻合、字节自检不等
//!    —— 任一条件不满足就直接失败，此时原文件还没被改过。
//! 3. **只认上一代**。两代之前的布局没有迁移路径，报错让人去翻历史版本，
//!    而不是攒一个「什么旧格式都敢读」的黑洞。v3 → v4 还多一道：v3 尾部那四张
//!    专用控制面表只要有一行就拒迁 —— 删表会丢数据，出路由旧二进制先把它们
//!    搬进通用表。
//!
//! 前提：**服务端必须先停**。本工具不 flock（服务端也没有持文件锁，锁不住
//! 任何东西），只在改写前后各读一次源文件比对 —— 有并发写者会在这里露馅，
//! 但那是哨兵不是互斥。

use std::path::PathBuf;
use treedb::engine::core::{migrate_snapshot, SNAP_BACKUP, SNAP_FILE};
use treedb::engine::{probe_snapshot, MigrationOutcome, SnapshotInfo, SNAP_LEGACY_VERSION};

const HELP: &str = "\
treedb-migrate —— 把旧布局的快照升到当前布局（只动 state.snap，不动 WAL）

用法：
  treedb-migrate --dir <数据目录>                 只报告，不写
  treedb-migrate --dir <数据目录> --apply         执行迁移
  treedb-migrate --dir <数据目录> --apply --force  执行并覆盖既有备份文件

退出码：0 = 无需迁移或迁移完成；1 = 出错（包括拒动）；10 = 需要迁移但没给 --apply。
执行前请先停掉服务端：本工具不做互斥。";

/// 体检报告。不带 `--apply` 时打一次，迁移后再打一次；字段名对齐
/// `treedb-server --stats` 的写法，方便运维把两份输出贴在一起看。
fn print_info(info: &SnapshotInfo) {
    println!("snapshot    = {} ({} B)", info.path.display(), info.bytes);
    println!(
        "version     = {} (this build writes {})",
        info.version, info.written_by_this_build
    );
    println!("seq         = {}", info.snap_seq);
    // 迁移不动 WAL，所以这一行在前后两次输出里必须一模一样 —— 它就是那条承诺
    // 的可验证据，不是装饰。
    println!("wal         = {} B on disk (untouched by migration)", info.wal_disk_bytes);
    if info.decodable {
        println!(
            "counts      = views {} nodes {} dentries {} locks {} refs {} tables {} rows {}",
            info.views, info.nodes, info.dentries, info.locks, info.refs, info.tables, info.rows
        );
    }
    if let Some(note) = &info.note {
        println!("note        = {note}");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir: Option<PathBuf> = None;
    let mut apply = false;
    let mut force = false;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--dir" => dir = Some(PathBuf::from(next(&mut i))),
            "--apply" => apply = true,
            "--force" => force = true,
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

    let Some(dir) = dir else {
        eprintln!("missing --dir <数据目录>\n\n{HELP}");
        std::process::exit(2);
    };

    let info = match probe_snapshot(&dir) {
        Ok(info) => info,
        Err(e) => {
            eprintln!("probe {}: {e}", dir.display());
            std::process::exit(1);
        }
    };
    println!("dir         = {}", dir.display());
    print_info(&info);

    if !info.present {
        // 没有快照 = 新目录，当前二进制直接就能开门；这不是错误，别给运维
        // 一个需要解释的失败码。
        println!("nothing to migrate: no snapshot in this directory yet");
        return;
    }

    if info.is_current() {
        println!("already on the current layout: nothing to do");
        return;
    }

    if !apply {
        if info.version == SNAP_LEGACY_VERSION && info.decodable {
            println!(
                "needs migration: run `treedb-migrate --dir {} --apply` (server stopped)",
                dir.display()
            );
            std::process::exit(10);
        }
        eprintln!(
            "refusing to migrate: {}",
            info.note.unwrap_or_else(|| format!("unknown snapshot version {}", info.version))
        );
        std::process::exit(1);
    }

    match migrate_snapshot(&dir, force) {
        Ok(outcome) => match outcome {
            treedb::engine::MigrationOutcome::Migrated {
                before,
                after,
                backup,
                seq,
                elapsed_ms,
            } => {
                println!(
                    "migrated    = {} -> {} ({} B -> {} B, seq {} preserved, {} ms)",
                    before.version, after.version, before.bytes, after.bytes, seq, elapsed_ms
                );
                println!("backup      = {}", backup.display());
                println!(
                    "wal         = untouched: {} B -> {} B",
                    before.wal_disk_bytes, after.wal_disk_bytes
                );
                println!(
                    "rollback    = stop the server, copy {SNAP_BACKUP} over {SNAP_FILE}, then \
                     run the older binary (the WAL was not touched). Lossless only until the new \
                     binary has checkpointed once: a checkpoint resets the WAL, so writes it had \
                     absorbed exist afterwards only in the rewritten snapshot."
                );
                println!("done        = the server can now start on this directory");
            }
            MigrationOutcome::AlreadyCurrent(info) => {
                println!("already on the current layout: nothing was written");
                print_info(&info);
            }
            MigrationOutcome::NoSnapshot(path) => {
                println!(
                    "nothing to migrate: no snapshot beside {} yet",
                    path.display()
                );
            }
        },
        Err(e) => {
            // 走到这里意味着要么什么都没改（自检/判断阶段失败），要么只留下了
            // 一份没被 rename 的 `.tmp` —— 后者不影响任何启动路径。
            eprintln!("migrate {}: {e}", dir.display());
            std::process::exit(1);
        }
    }
}
