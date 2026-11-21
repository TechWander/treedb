//! `treedb-backup` 的在线性验收：**主一直在跑**，备份文件必须与它同源。
//!
//! 与 `dump_restore.rs` 的分工：那边验「离线目录 → 文件 → 目录」的语义与
//! 确定性；这边验「在线主 → 文件」，即两条只读 op（`SnapFetch` + `WalFetch`）
//! 拼出来的切面到底对不对、主有没有被打断。
//!
//! 三条判据：
//!
//! 1. **逐字段相等**：备份恢复出来的 `State` 与源目录停机后重开的 `State` 全等。
//!    这条最强，`assert_eq!` 会直接给出第一个不相等的字段名。
//! 2. **切面自洽**：备份文件的 `seq` 必须落在「备份开始前」与「备份结束后」两个
//!    `Stats.seq` 之间 —— 既不能凭空超前（那是主还没确认的写），也不能倒退。
//! 3. **恢复即可用**：`treedb-load` 出来的目录能被服务端起起来，快照水位等于
//!    dump 的 seq，且没有日志可重放。
//!
//! 跑法：`cargo test --release --test backup_online`

#[path = "common/mod.rs"]
mod common;

use common::Server;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use treedb::engine::model::{
    AuditRec, ColumnDef, DefRec, KeyRec, Node, Op, Reply, TableDef, COL_INDEX, COL_UNIQUE,
    COND_NONE, SA_DATA_REF,
};
use treedb::engine::{dump_dir, Durability, Dump, Engine, State};
use treedb::net::client::Client;

// ---------------------------------------------------------------- 脚手架

fn backup_bin() -> &'static str {
    env!("CARGO_BIN_EXE_treedb-backup")
}

fn load_bin() -> &'static str {
    env!("CARGO_BIN_EXE_treedb-load")
}

/// 起真二进制，返回（退出码，stdout+stderr）。退出码是写进运维脚本的契约。
fn cli(bin: &str, args: &[&str]) -> (i32, String) {
    let out = std::process::Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap_or(-1), text)
}

fn okc(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(Reply::Err { code, msg }) => panic!(
            "{op:?} → unexpected errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport error: {e}"),
    }
}

fn node(r: Reply) -> Node {
    match r {
        Reply::Node(n) => n,
        other => panic!("expected Reply::Node, got {other:?}"),
    }
}

fn stats(c: &mut Client) -> treedb::engine::core::EngineStats {
    match okc(c, &Op::Stats) {
        Reply::Stats(s) => *s,
        other => panic!("expected Reply::Stats, got {other:?}"),
    }
}

fn mk_view(c: &mut Client, name: &str) -> u32 {
    match okc(c, &Op::CreateView { name: name.into() }) {
        Reply::View { id, .. } => id,
        other => panic!("expected Reply::View, got {other:?}"),
    }
}

fn mk_file(c: &mut Client, view: u32, parent: u64, name: &str) -> Node {
    node(okc(
        c,
        &Op::Create {
            view,
            parent,
            name: name.into(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ))
}

fn mk_dir(c: &mut Client, view: u32, parent: u64, name: &str) -> Node {
    node(okc(
        c,
        &Op::Mkdir {
            view,
            parent,
            name: name.into(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ))
}

fn key(id: &str) -> KeyRec {
    KeyRec {
        id: id.into(),
        label: format!("label-{id}").into(),
        secret_hash: b"hash-1".to_vec(),
        created_at: 7,
        last_used_at: 9,
        revoked_at: 0,
        view: b"team".to_vec(),
        ro: false,
        owner: b"alice".to_vec(),
        note: b"n".to_vec(),
        expires_at: 0,
        created_by: b"root".to_vec(),
        secret_hint: b"h".to_vec(),
    }
}

/// 每张表都放一条：漏掉哪张，`assert_eq!(state)` 会给出字段名。
fn build_rich(c: &mut Client) -> (u32, u64) {
    let v = mk_view(c, "v");
    let d = mk_dir(c, v, 1, "d");
    let a = mk_file(c, v, 1, "a");
    let b = mk_file(c, v, d.ino, "b");
    for (ino, bytes) in [(a.ino, 4096u64), (b.ino, 1024)] {
        okc(
            c,
            &Op::Setattr {
                view: v,
                ino,
                valid: SA_DATA_REF,
                mode: 0,
                uid: 0,
                gid: 0,
                size: 0,
                atime: 0,
                mtime: 0,
                data_ref: b"1:4096:dev7".to_vec(),
                ref_bytes: bytes,
            },
        );
    }
    okc(
        c,
        &Op::Link {
            view: v,
            ino: a.ino,
            parent: d.ino,
            name: b"a-alias".to_vec(),
        },
    );
    okc(
        c,
        &Op::Symlink {
            view: v,
            parent: 1,
            name: b"l".to_vec(),
            target: b"a".to_vec(),
            uid: 1,
            gid: 1,
        },
    );
    okc(
        c,
        &Op::KeyCreate {
            key: key("k1"),
        },
    );
    okc(
        c,
        &Op::AuditAppend {
            row: AuditRec {
                seq: 0,
                ts: 5,
                key_id: b"k1".to_vec(),
                method: b"POST".to_vec(),
                tool: b"fs_ls".to_vec(),
                ok: true,
                detail: b"d".to_vec(),
            },
        },
    );
    okc(
        c,
        &Op::DefPut {
            def: DefRec {
                name: b"ls".to_vec(),
                kind: b"tool".to_vec(),
                payload: b"x".to_vec(),
                updated_at: 3,
            },
        },
    );
    okc(
        c,
        &Op::PlaceSet {
            view: b"v".to_vec(),
            instance: b"node-1".to_vec(),
        },
    );
    okc(
        c,
        &Op::TableCreate {
            def: TableDef {
                name: b"t1".to_vec(),
                columns: vec![
                    ColumnDef {
                        name: b"a".to_vec(),
                        flags: COL_UNIQUE,
                    },
                    ColumnDef {
                        name: b"b".to_vec(),
                        flags: COL_INDEX,
                    },
                ],
            },
        },
    );
    for (k, a1, b1) in [("r1", "a1", "x"), ("r2", "a2", "y")] {
        okc(
            c,
            &Op::RowPut {
                table: b"t1".to_vec(),
                key: k.as_bytes().to_vec(),
                cols: vec![a1.as_bytes().to_vec(), b1.as_bytes().to_vec()],
                flags: 0,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
        );
    }
    (v, a.ino)
}

// ---------------------------------------------------------------- 用例

/// 主静置时备份：这是最强的判据 —— 备份出来的东西必须**逐字段**等于源。
///
/// 「静置」只是不给新写，主本身一直在跑（备份前后都 `Ping` 得到应答），
/// 而且源目录的 `.lock` 始终被服务端占着 —— 这正是 offline dump 做不出来的
/// 那件事。
#[test]
fn online_backup_equals_the_instance_it_came_from() {
    let srv = Server::start_workers("group", 4);
    let mut c = srv.client();
    let (v, _ino) = build_rich(&mut c);
    // 让源是「快照 + 尾巴」两段都在：只测快照会漏掉 WalFetch 那半边。
    okc(&mut c, &Op::Checkpoint);
    mk_file(&mut c, v, 1, "after-snap");
    okc(
        &mut c,
        &Op::Unlink {
            view: v,
            parent: 1,
            name: b"l".to_vec(),
        },
    );
    let before = stats(&mut c);
    assert!(before.snap_seq > 0, "checkpoint 之后必须有快照：{before:?}");
    assert!(
        before.snap_seq < before.seq,
        "快照不能已经盖到尾巴，否则测不到 WalFetch：{before:?}"
    );

    let outdir = TempDir::new().expect("tempdir");
    let out = outdir.path().join("live.dump");
    let out_s = out.to_str().unwrap().to_string();
    let (code, text) = cli(backup_bin(), &["--primary", &srv.addr, "--out", &out_s]);
    assert_eq!(code, 0, "{text}");
    assert!(out.is_file(), "{text}");
    // 摘要里必须报出「快照水位 + 尾巴条数」，否则运维看不出切面在哪。
    assert!(
        text.contains(&format!("snapshot seq {} (", before.snap_seq)),
        "backup 摘要应报出快照水位 {}：{text}",
        before.snap_seq
    );
    assert!(text.contains(&format!("seq {})", before.seq)), "{text}");

    // 备份期间主一直在跑：这一条是「在线」的正面证据。
    assert!(Client::ping(&srv.addr), "备份结束后主必须还在服务");

    // 停机取证：源目录真正的状态（只为了拿一个可比较的对象，不是备份手段）。
    let stopped = srv.graceful_shutdown();
    let expect: State = {
        let e = Engine::open(&stopped.dir, Durability::Always).expect("reopen source");
        e.state_snapshot()
    };
    drop(stopped);

    // 备份文件导入新目录后，状态必须与源逐字段相等。
    let dst = TempDir::new().expect("tempdir");
    let dstdir = dst.path().join("treedb");
    let dst_s = dstdir.to_str().unwrap().to_string();
    let (code, text) = cli(load_bin(), &["--in", &out_s, "--dir", &dst_s]);
    assert_eq!(code, 0, "{text}");

    let e2 = Engine::open(&dstdir, Durability::Always).expect("open restored");
    assert_eq!(e2.state_snapshot(), expect, "备份必须与源逐字段相等");
    let rec = e2.recovery();
    assert_eq!(rec.snap_seq, before.seq, "恢复后的快照水位必须是 backup 的 seq");
    assert_eq!(rec.wal_records, 0, "导入不写 WAL");
    drop(e2);

    // 同一份状态导两次字节相同：备份必须走同一条确定性路径，不能夹带时间戳。
    let again = dump_dir(&dstdir).expect("re-dump restored");
    let orig = Dump::read_file(&out).expect("read backup");
    assert_eq!(again.encode(), orig.encode(), "backup → load → dump 必须逐字节相同");
}

/// 主在写、在 checkpoint 的时候备份：切面可以停在中途，但**必须自洽**。
///
/// 这里故意让写线程每隔一批就打一次 `Checkpoint` —— 主有机会把 backup 还没
/// 追上的那段截进快照，backup 因此会用 `--retries` 重来。这条用例不断言
/// 「一定重来过」（那是时钟决定的），只断言**无论重来几次，落盘的那一份都对**。
#[test]
fn an_online_backup_is_a_consistent_slice_while_the_primary_keeps_writing() {
    let srv = Server::start_workers("group", 4);
    let mut c = srv.client();
    let v = mk_view(&mut c, "live");
    for i in 0..200 {
        mk_file(&mut c, v, 1, &format!("pre-{i}"));
    }
    let t0 = stats(&mut c);

    let stop = Arc::new(AtomicBool::new(false));
    let addr = srv.addr.clone();
    let (tx, rx) = mpsc::channel::<(u64, u64, String)>();
    let writer = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut w = match Client::connect(&addr, 5_000) {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send((0, 1, format!("connect: {e}")));
                    return;
                }
            };
            let (mut ops, mut errs, mut last_err) = (0u64, 0u64, String::new());
            while !stop.load(Ordering::Relaxed) {
                let name = format!("live-{ops}");
                match w.call(&Op::Create {
                    view: v,
                    parent: 1,
                    name: name.into(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }) {
                    Ok(Reply::Err { code, msg }) => {
                        errs += 1;
                        last_err = format!("{code}: {}", String::from_utf8_lossy(&msg));
                        if errs > 8 {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        errs += 1;
                        last_err = format!("transport: {e}");
                        break;
                    }
                }
                ops += 1;
                // 每 400 条换一代快照：backup 的快照下载与 WAL 追赶都可能被撞上。
                if ops % 400 == 0 {
                    let _ = w.call(&Op::Checkpoint);
                }
            }
            let _ = tx.send((ops, errs, last_err));
        })
    };

    // 让写线程先跑一会儿，确保备份开始时主已经在动。
    std::thread::sleep(Duration::from_millis(80));
    let outdir = TempDir::new().expect("tempdir");
    let out = outdir.path().join("live.dump");
    let out_s = out.to_str().unwrap().to_string();
    let (code, text) = cli(
        backup_bin(),
        &["--primary", &srv.addr, "--out", &out_s, "--retries", "8"],
    );
    // 备份一返回就取主的水位：这是切面的上界。
    let t_after = stats(&mut c);

    stop.store(true, Ordering::Relaxed);
    let (ops, errs, last_err) = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("writer thread must report back");
    writer.join().expect("writer thread");

    assert_eq!(code, 0, "备份失败（主持续写入 + checkpoint 下）：{text}");
    assert_eq!(errs, 0, "写线程在备份期间报错 {errs} 次：{last_err}");
    assert!(ops > 0, "写线程一条都没写成功，用例前提不成立");

    let d = Dump::read_file(&out).expect("read backup");
    assert!(
        d.seq >= t0.seq,
        "备份切面 {} 早于备份开始前的水位 {}（丢写）",
        d.seq,
        t0.seq
    );
    assert!(
        d.seq <= t_after.seq,
        "备份切面 {} 越过备份结束后主的水位 {}（读到未确认的写）",
        d.seq,
        t_after.seq
    );
    let c0 = d.counts();
    assert!(
        c0.nodes as u64 >= t0.nodes,
        "备份里的 nodes {} 少于备份开始前的 {}（切面倒退）",
        c0.nodes,
        t0.nodes
    );

    // 自洽：落盘的那一份自己就是完整状态，不是「一半旧一半新」。
    let dst = TempDir::new().expect("tempdir");
    let dstdir = dst.path().join("treedb");
    let dst_s = dstdir.to_str().unwrap().to_string();
    let (code, text) = cli(load_bin(), &["--in", &out_s, "--dir", &dst_s]);
    assert_eq!(code, 0, "{text}");
    let e = Engine::open(&dstdir, Durability::Always).expect("open restored");
    assert_eq!(e.recovery().snap_seq, d.seq, "恢复后的水位必须等于切面 seq");
    assert_eq!(e.recovery().wal_records, 0);
    assert_eq!(e.state_snapshot(), d.state(), "备份内容与它自己的 dump 失配");
}

/// 参数与连接两档失败态的退出码：2 = 参数不全 / 参数非法；1 = 连不上或被拒。
#[test]
fn cli_exit_codes_are_the_scripting_contract() {
    let (code, text) = cli(backup_bin(), &["--out", "/tmp/never.dump"]);
    assert_eq!(code, 2, "缺 --primary 必须报 2：{text}");
    assert!(text.contains("--primary"), "{text}");

    let (code, text) = cli(backup_bin(), &["--primary", "127.0.0.1:1"]);
    assert_eq!(code, 2, "缺 --out 必须报 2：{text}");
    assert!(text.contains("--out"), "{text}");

    let (code, text) = cli(backup_bin(), &["--bogus"]);
    assert_eq!(code, 2, "未知参数必须报 2：{text}");

    let (code, text) = cli(
        backup_bin(),
        &["--primary", "127.0.0.1:1", "--out", "/tmp/never.dump", "--retries", "1", "--quiet"],
    );
    assert_eq!(code, 1, "连不上必须报 1：{text}");
    assert!(text.contains("connect"), "{text}");
    assert!(
        !std::path::Path::new("/tmp/never.dump").exists(),
        "失败不能留下半截文件"
    );

    // 重试循环本身要真的转满 `--retries` 轮：`gap`（主 checkpoint 追过了头）走的
    // 就是这条 `continue`，而它比连接失败难在测试里撞上，所以在这里钉住次数。
    let (code, text) = cli(
        backup_bin(),
        &["--primary", "127.0.0.1:1", "--out", "/tmp/never.dump", "--retries", "3"],
    );
    assert_eq!(code, 1, "{text}");
    assert_eq!(
        text.matches("attempt ").count(),
        3,
        "必须重试 3 轮：{text}"
    );
    assert!(text.contains("gave up after 3 attempts"), "{text}");
}
