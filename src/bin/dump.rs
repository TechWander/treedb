//! `treedb-dump`：把一个数据目录导成自描述的 `.dump` 文件（DESIGN.md §6.7）。
//!
//! ```text
//! treedb-dump --dir /var/lib/treedb --out /backup/treedb.dump
//! treedb-dump --dir /var/lib/treedb --out - | gzip > /backup/treedb.dump.gz
//! ```
//!
//! 与 `treedb-migrate` 的分工：migrate 搬的是**内部布局**（同一台机器、同一个
//! 目录、换二进制），dump 搬的是**逻辑内容**（换机器、换目录、只依赖日志标签）。
//! 前者受 `SNAP_VERSION` 约束，后者只要两侧日志标签表一致就能读懂。
//!
//! 三条硬规矩，都由 [`treedb::engine::dump_dir`] 守着：
//!
//! 1. **要独占**：目录锁拿不到就失败。服务在跑时导出会读到「快照 + 正在增长
//!    的 WAL」这组可能自相矛盾的组合，宁可拒也不猜。
//! 2. **一个字节都不写源目录**：`--dir` 侧只读，`.dump` 落在 `--out` 上
//!    （`.tmp` → fsync → rename，半截文件不会顶着正式名字出现）。
//! 3. **导出是确定性的**：同一份状态导两次字节相同，所以 `dump → load → dump`
//!    可以拿字节相等当判据。
//!
//! 注意：dump 也会带上**未 durable** 的记录（与启动恢复同语义）—— 那是磁盘上
//! 真实存在的写，丢掉它们才是错。

use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use treedb::engine::dump_dir;

const HELP: &str = "\
treedb-dump —— 把一个 treedb 数据目录导成逻辑 dump 文件

用法：
  treedb-dump --dir <数据目录> --out <文件>   写文件（.tmp → fsync → rename）
  treedb-dump --dir <数据目录> --out -        写到标准输出（摘要走 stderr）

退出码：0 = 导出完成；1 = 出错（目录被占、读不出、写不进）；2 = 参数不全。
执行前请先停掉服务端：本工具要求目录独占。";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir: Option<PathBuf> = None;
    let mut out: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--dir" => dir = Some(PathBuf::from(next(&mut i))),
            "--out" => out = Some(next(&mut i)),
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

    let (Some(dir), Some(out)) = (dir, out) else {
        eprintln!("missing --dir and/or --out\n\n{HELP}");
        std::process::exit(2);
    };
    if out.is_empty() {
        eprintln!("--out needs a path (or `-` for stdout)\n\n{HELP}");
        std::process::exit(2);
    }

    let t0 = Instant::now();
    let dump = match dump_dir(&dir) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("dump {}: {e}", dir.display());
            std::process::exit(1);
        }
    };
    // 摘要先算好：它对 stdout 与 stderr 是一样的，只是落哪一路不同。
    let summary = dump.summary();

    if out == "-" {
        // 二进制走 stdout，人看的全部走 stderr —— 否则 `| gzip` 的管道里会
        // 混进一行摘要，解压出来的 dump 直接 CRC 不过。
        let buf = dump.encode();
        if let Err(e) = std::io::stdout().write_all(&buf) {
            eprintln!("write stdout: {e}");
            std::process::exit(1);
        }
        eprintln!("dir     = {}", dir.display());
        eprintln!("dump    = {}", summary);
        eprintln!("bytes   = {} (stdout)", buf.len());
        eprintln!("elapsed = {} ms", t0.elapsed().as_millis());
        return;
    }

    let path = PathBuf::from(&out);
    let bytes = match dump.write_file(&path) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("write {}: {e}", path.display());
            std::process::exit(1);
        }
    };
    println!("dir     = {}", dir.display());
    println!("dump    = {}", summary);
    println!("out     = {} ({} B)", path.display(), bytes);
    println!("elapsed = {} ms", t0.elapsed().as_millis());
}
