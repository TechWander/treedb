//! `treedb-cli`：给人用的命令行 —— 通用表的全套 CRUD、引擎统计，以及既有离线工具的
//! 统一入口。
//!
//! 用法与全部旗标在 `treedb-cli --help`（正文与 `src/cli/mod.rs` 的 `HELP` 常量同一段，
//! 不在这里复述第二份）。这个文件只做三件事：拿参数、把 `i32` 交出去、给一个短说明。
//!
//! 为什么逻辑不在这里而在 `src/cli/`：集成测试要能在同一个进程里调
//! [`treedb::cli::main`] 拿到退出码与两段输出，`src/bin/*.rs` 里的代码不是库面，测不到。
//! 与 `server.rs`/`bench.rs` 那些入口的区别也正在于此 —— 它们的主逻辑本来就长在库里。

use std::io::Write;

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let rc = treedb::cli::main(&argv);
    // 先把两条流冲干净再退：`println!` 走的是带缓冲的 stdout，而 CLI 的输出经常被人
    // 接进管道。进程在缓冲落盘前 exit 会丢掉尾部几行，看起来像「命令没输出」。
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(rc);
}
