//! `treedb-load`：把 `treedb-dump` 产出的文件写成一个**新的**数据目录。
//!
//! ```text
//! treedb-load --in /backup/treedb.dump --dir /var/lib/treedb-new
//! treedb-load --in /backup/treedb.dump                 # 只体检，不写
//! gunzip -c /backup/treedb.dump.gz | treedb-load --in - --dir /srv/treedb
//! ```
//!
//! 落盘的只有 `state.snap`（水位 = dump 里的 seq），**不写 WAL**：导入后的
//! 目录里「快照之后没有任何写」是这句话的字面意思。于是恢复路径退化成一次
//! 快照读取 + 零条重放，`treedb-server` 起来看到的就是 dump 那一刻的状态。
//!
//! 拒绝的两种情况（都 fail-closed，都在动任何字节之前）：
//!
//! - **目标目录非空**（除本工具自己的 `.lock` 外）：往有数据的目录里合并是
//!   另一种没有契约的操作；想覆盖就先删或换个目录。
//! - **目标目录被服务占着**：目录锁拿不到就失败。
//!
//! 不搬 `fence` 令牌：那是「谁是当前主」的部署态，不是逻辑数据。从提升过的
//! 副本导出的 dump 载回来，按新主（fence 0）起步。
//!
//! 不带 `--dir` 时只解码 + 打规模，用来在恢复之前确认「这个文件里到底有什么」，
//! 全程不碰磁盘。

use std::io::Read;
use std::path::PathBuf;
use std::time::Instant;
use treedb::engine::{restore_dump, Dump};

const HELP: &str = "\
treedb-load —— 把 treedb-dump 的文件导入成一个新的数据目录

用法：
  treedb-load --in <文件> --dir <目标目录>   导入（目标须为空或不存在）
  treedb-load --in <文件>                    只体检：解码并打印规模，不写
  treedb-load --in - --dir <目标目录>        从标准输入读取

退出码：0 = 导入完成或体检通过；1 = 出错（CRC 不过、版本不认、目标非空/被占）；2 = 参数不全。
导入前请先停掉目标目录上的服务端：本工具要求目录独占。";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut input: Option<String> = None;
    let mut dir: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--in" => input = Some(next(&mut i)),
            "--dir" => dir = Some(PathBuf::from(next(&mut i))),
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

    let Some(input) = input.filter(|s| !s.is_empty()) else {
        eprintln!("missing --in <文件>\n\n{HELP}");
        std::process::exit(2);
    };

    let t0 = Instant::now();
    // 先完整解码再动磁盘：CRC/版本/条数任一不过，目标目录连 `.lock` 都不会多。
    let dump = if input == "-" {
        let mut raw = Vec::new();
        if let Err(e) = std::io::stdin().read_to_end(&mut raw) {
            eprintln!("read stdin: {e}");
            std::process::exit(1);
        }
        match Dump::decode(&raw) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("decode stdin: {e}");
                std::process::exit(1);
            }
        }
    } else {
        let path = PathBuf::from(&input);
        match Dump::read_file(&path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("read {}: {e}", path.display());
                std::process::exit(1);
            }
        }
    };
    let summary = dump.summary();
    println!("in      = {}", if input == "-" { "<stdin>".to_string() } else { input });

    let Some(dir) = dir else {
        // 体检档：只报告。运维靠这一步在覆盖/新建目录之前确认内容。
        println!("dump    = {}", summary);
        println!("dry-run = no --dir given: nothing was written");
        return;
    };

    match restore_dump(&dir, &dump) {
        Ok(()) => {
            println!("dir     = {}", dir.display());
            println!("dump    = {}", summary);
            println!(
                "written = state.snap only (no WAL): the server can start on this directory"
            );
            println!("elapsed = {} ms", t0.elapsed().as_millis());
        }
        Err(e) => {
            // 走到这里时目标目录要么原样未动（拒绝发生在写之前），要么只多了
            // 一个空目录/`.lock`；两种都不影响它原本的用途。
            eprintln!("load {}: {e}", dir.display());
            std::process::exit(1);
        }
    }
}
