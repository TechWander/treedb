//! `treedb-pitr`：把归档 + 活日志重放到指定水位，落成一个**新的**数据目录（§6.11）。
//!
//! ```text
//! treedb-pitr --dir /srv/treedb --archive /backup/treedb-wal --to-seq 918273 \
//!             --out /srv/treedb-0918
//! treedb-pitr --dir /srv/treedb --archive /backup/treedb-wal --to-seq 918273 --dry-run
//! ```
//!
//! 恢复口径与 `treedb-load` 完全一致：只在 `--out` 写 `state.snap`（水位 =
//! `--to-seq`），不写 WAL、不搬 `fence`。于是「快照之后没有任何写」是字面
//! 意思 —— 起来看到的正是那一刻，`--to-seq` 之后的写一条都不在里面。
//!
//! **三条前置条件**（不满足就报错，不猜）：
//!
//! - `--dir` 指向的数据目录必须已经停服（或是一份冷拷贝）：归档只覆盖到最后
//!   一次 checkpoint，`--to-seq` 落在它之后要靠活 WAL 补尾巴，而活 WAL 在被
//!   服务占着时会继续被写。
//! - `--to-seq` 必须落在「归档 + 活日志」的连续区间里。中间缺一条就报缺口并
//!   指出下一个实际存在的 seq —— 关掉归档期间做过的那几次 checkpoint 会在
//!   归档里留下一个洞。
//! - `--out` 必须不存在或只含本工具自己的 `.lock`：往有数据的目录里合并是
//!   另一种没有契约的操作。
//!
//! `--to-seq` 最好取某个 op **结束之后**的读数（`--stats` 的 `wal_seq`）：一条
//! op 可能产三条日志，目标落在组中间会得到「该 op 只生效了一半」的状态。WAL
//! 里没有组边界标记，这一层查不出来，所以不猜也不拦，只在这里说清楚。
//!
//! `--dry-run` 走的是**同一条**规划路径（同样的锚点选择、同样的缺口校验、
//! 同样的重放），只是最后一步不落盘：它证明的是「这份恢复会不会成」，不是
//! 「另一份账算得对不对」。

use std::path::PathBuf;
use std::time::Instant;
use treedb::engine::{pitr_restore, plan_pitr};

const HELP: &str = "\
treedb-pitr —— 时间点恢复（PITR）：把归档日志重放到指定水位，写成新数据目录

用法：
  treedb-pitr --dir <数据目录> --archive <归档目录> --to-seq N --out <新目录>
  treedb-pitr --dir <数据目录> --archive <归档目录> --to-seq N --dry-run

  --dir PATH        数据目录（WAL + 快照）。必须已停服，或是一份冷拷贝
  --archive PATH    归档目录（treedb-server --archive-dir 指的同一个）
  --to-seq N        目标水位：WAL 逻辑序号，与 --stats 里的 wal_seq 同一口径
  --out PATH        新数据目录（不存在，或只含本工具的 .lock）
  --dry-run         只规划并打印锚点/条数，一个字节不写

退出码：0 = 恢复完成或体检通过；1 = 出错（缺口、越界、目标非空/被占、快照坏）；
2 = 参数不全。
落点只有 state.snap（水位 = --to-seq）：不写 WAL、不搬 fence，起来就是那一刻。";

fn die(msg: &str) -> ! {
    eprintln!("{msg}\n\n{HELP}");
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir: Option<PathBuf> = None;
    let mut archive: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut to_seq: Option<u64> = None;
    let mut dry_run = false;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--dir" => dir = Some(PathBuf::from(next(&mut i))),
            "--archive" => archive = Some(PathBuf::from(next(&mut i))),
            "--out" => out = Some(PathBuf::from(next(&mut i))),
            "--to-seq" => {
                let raw = next(&mut i);
                match raw.parse::<u64>() {
                    Ok(n) => to_seq = Some(n),
                    Err(e) => die(&format!("bad --to-seq {raw:?}: {e}")),
                }
            }
            "--dry-run" => dry_run = true,
            "-h" | "--help" => {
                println!("{}", HELP);
                return;
            }
            other => die(&format!("unknown argument: {other}")),
        }
        i += 1;
    }

    let Some(dir) = dir.filter(|p| !p.as_os_str().is_empty()) else {
        die("missing --dir <数据目录>");
    };
    let Some(archive) = archive.filter(|p| !p.as_os_str().is_empty()) else {
        die("missing --archive <归档目录>");
    };
    let Some(to_seq) = to_seq else {
        die("missing --to-seq N");
    };
    if dry_run && out.is_some() {
        die("--dry-run cannot be combined with --out");
    }
    if !dry_run && out.is_none() {
        die("missing --out <新目录> (or pass --dry-run to only check the range)");
    }

    let t0 = Instant::now();
    // 规划与落盘分两步：dry-run 与真恢复走完全相同的锚点选择、缺口校验与重放，
    // 差别只在最后那次 write_snapshot。
    let plan = match plan_pitr(&dir, &archive, to_seq) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("pitr: {e}");
            std::process::exit(1);
        }
    };
    println!("dir      = {}", dir.display());
    println!("archive  = {}", archive.display());
    println!("anchor   = seq {} ({})", plan.anchor, plan.anchor_desc);
    println!("to-seq   = {}", plan.to_seq);
    println!(
        "records  = {} ({} from {} archive segment(s), {} from the live WAL; newest seen {})",
        plan.records(),
        plan.from_archive,
        plan.segments,
        plan.from_wal,
        plan.newest
    );
    let Some(out) = out else {
        println!("dry-run  = nothing was written");
        println!("elapsed  = {} ms", t0.elapsed().as_millis());
        return;
    };
    match pitr_restore(plan, &out) {
        Ok(path) => {
            println!("out      = {}", path.display());
            println!(
                "written  = state.snap only (seq {to_seq}, no WAL, no fence): the server can start \
                 on this directory"
            );
            println!("elapsed  = {} ms", t0.elapsed().as_millis());
        }
        Err(e) => {
            // 拒绝发生在写之前：目标目录要么原样未动，要么只多了空目录/`.lock`。
            eprintln!("pitr {}: {e}", out.display());
            std::process::exit(1);
        }
    }
}
