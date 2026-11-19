//! `treedb-micro`：内嵌微基准 —— 不经网络、不写 WAL，只量状态机本身。
//!
//! 为什么需要它：吞吐矩阵（`benches/throughput_matrix.sh`）报的是端到端数字，
//! 里面混着 epoll 往返、syscall、fsync。想知道「readdir 一页为什么要 40 µs」，
//! 必须先把网络与磁盘从分母里摘掉。这里直接调 [`Engine::exec`]：它做
//! translate + apply，但不 fsync、不过 socket —— 剩下的就是纯计算与纯分配。
//!
//! 同时统计**每次 op 的堆分配次数与字节数**（`Counting` 分配器），因为这条路径
//! 上的成本几乎全是分配出来的：目录项 key 里夹 `Vec<u8>` 让每次 dentry 查找都要
//! 先复制一份名字，readdir 一页要给每条目录项单独 `clone()` 一次。
//!
//! 用法：
//!   target/release/treedb-micro [--items 32000] [--ops 100000] [--tag before]
//!
//! 注意：**它的绝对值不是承诺，两次构建之间的差值才是**。写类 op 会让目录边测
//! 边长（create 不配对 unlink），所以同一个 op 在不同 `--items` 下不是线性的。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use treedb::engine::core::{Durability, Engine};
use treedb::engine::model::{Op, Reply};

/// 计时窗口内才计数，避免把建库、造名字这些准备工作算进 op 成本。
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
        }
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) && new > l.size() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add((new - l.size()) as u64, Ordering::Relaxed);
        }
        System.realloc(p, l, new)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
        }
        System.alloc_zeroed(l)
    }
}

#[global_allocator]
static GA: Counting = Counting;

struct Args {
    items: usize,
    ops: usize,
    limit: usize,
    tag: String,
}

fn parse() -> Args {
    let mut a = Args {
        items: 32_000,
        ops: 100_000,
        limit: 100,
        tag: "run".into(),
    };
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    fn next(argv: &[String], i: usize) -> String {
        argv.get(i + 1).cloned().unwrap_or_default()
    }
    while i < argv.len() {
        match argv[i].as_str() {
            "--items" => a.items = next(&argv, i).parse().unwrap_or(a.items),
            "--ops" => a.ops = next(&argv, i).parse().unwrap_or(a.ops),
            "--limit" => a.limit = next(&argv, i).parse().unwrap_or(a.limit),
            "--tag" => a.tag = next(&argv, i),
            "--help" | "-h" => {
                println!(
                    "用法: treedb-micro [--items N] [--ops N] [--limit N] [--tag NAME]\n\
                     \x20 --items  预热进根目录的目录项数（默认 32000，与吞吐矩阵同形状）\n\
                     \x20 --ops    每个 op 的迭代次数（默认 100000）\n\
                     \x20 --limit  readdir 一页条数（默认 100，与 treedb-bench 一致）"
                );
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    a
}

/// 一次测量：跑 `iters` 次 `f`，返回 (ns/op, allocs/op, bytes/op)。
fn measure<B: FnMut()>(iters: usize, mut f: B) -> (f64, f64, f64) {
    // 先 warm 一轮：让分支预测、cache、分配器 bin 都进入稳态再计时。
    let warm = (iters / 10).max(1);
    for _ in 0..warm {
        f();
    }
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    COUNT.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let ns = t0.elapsed().as_nanos() as f64 / iters as f64;
    COUNT.store(false, Ordering::Relaxed);
    let al = ALLOCS.load(Ordering::Relaxed) as f64 / iters as f64;
    let by = BYTES.load(Ordering::Relaxed) as f64 / iters as f64;
    (ns, al, by)
}

fn main() {
    let a = parse();
    let dir = std::env::temp_dir().join(format!("treedb-micro-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create dir");
    // Never 档：exec() 本来就不 fsync，选它只是免得 open 时多付一次恢复读。
    let mut eng = Engine::open(&dir, Durability::Never).expect("open engine");

    let view = match eng.exec(&Op::CreateView {
        name: b"micro".to_vec(),
    }) {
        (_, Reply::View { id, .. }) => id,
        (_, r) => panic!("create view: {r:?}"),
    };

    // 预热：--items 条目录项挂在根下，形状与吞吐矩阵一致（短名、均匀分布）。
    let names: Vec<Vec<u8>> = (0..a.items)
        .map(|k| format!("f{k}").into_bytes())
        .collect();
    for n in &names {
        eng.exec(&Op::Create {
            view,
            parent: 1,
            name: n.clone(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
    }

    // 复用同一批 Op 对象：harness 自己不许产生分配，否则量到的是构造开销。
    let lookups: Vec<Op> = names
        .iter()
        .take(4096)
        .map(|n| Op::Lookup {
            view,
            parent: 1,
            name: n.clone(),
        })
        .collect();
    let misses: Vec<Op> = (0..4096u64)
        .map(|k| Op::Lookup {
            view,
            parent: 1,
            name: format!("nope{k}").into_bytes(),
        })
        .collect();
    let attrs: Vec<Op> = (0..4096u64)
        .map(|k| Op::Getattr {
            view,
            ino: 2 + (k % a.items as u64),
        })
        .collect();
    let dirs: Vec<Op> = (0..64u64)
        .map(|_| Op::ReadDir {
            view,
            parent: 1,
            offset: 0,
            limit: a.limit as u32,
        })
        .collect();
    let stamps: Vec<Op> = (0..4096u64)
        .map(|k| Op::Setattr {
            view,
            ino: 2 + (k % a.items as u64),
            valid: 1 << 6,
            mode: 0,
            uid: 0,
            gid: 0,
            size: 0,
            atime: 0,
            mtime: 0,
            data_ref: format!("blk{k}").into_bytes(),
            ref_bytes: 4096,
        })
        .collect();
    // create/unlink 用还没占用的名字段，避免撞上预热项的 EEXIST 早退路径。
    // 长度要带上 measure() 的 warm 轮（iters/10），否则下标会越界。
    let n_write = a.ops + a.ops / 10 + 2;
    let fresh: Vec<Op> = (a.items..a.items + n_write)
        .map(|k| Op::Create {
            view,
            parent: 1,
            name: format!("f{k}").into_bytes(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .collect();
    let gone: Vec<Op> = fresh
        .iter()
        .map(|op| match op {
            Op::Create { name, .. } => Op::Unlink {
                view,
                parent: 1,
                name: name.clone(),
            },
            _ => unreachable!(),
        })
        .collect();

    println!(
        "# treedb-micro tag={} items={} ops={} readdir_limit={}",
        a.tag, a.items, a.ops, a.limit
    );
    println!("{:<14} {:>9} {:>10} {:>10}", "op", "ns/op", "allocs/op", "bytes/op");
    // 用宏而不是闭包：`row` 要可变借 `eng`，而下面伸缩检查还要继续 `eng.exec`，
    // 一个 FnMut 变量会把后者挡在借用检查器外面。
    macro_rules! row {
        ($eng:expr, $name:expr) => {{
            let (ns, al, by) = match $name {
                "ping" => measure(a.ops, || {
                    $eng.exec(&Op::Ping);
                }),
                "lookup" => {
                    let mut i = 0usize;
                    measure(a.ops, || {
                        $eng.exec(&lookups[i & 4095]);
                        i += 1;
                    })
                }
                "lookup-miss" => {
                    let mut i = 0usize;
                    measure(a.ops, || {
                        $eng.exec(&misses[i & 4095]);
                        i += 1;
                    })
                }
                "getattr" => {
                    let mut i = 0usize;
                    measure(a.ops, || {
                        $eng.exec(&attrs[i & 4095]);
                        i += 1;
                    })
                }
                "readdir" => {
                    let mut i = 0usize;
                    measure(a.ops.min(20_000), || {
                        $eng.exec(&dirs[i & 63]);
                        i += 1;
                    })
                }
                "setattr" => {
                    let mut i = 0usize;
                    measure(a.ops, || {
                        $eng.exec(&stamps[i & 4095]);
                        i += 1;
                    })
                }
                "create" => {
                    let mut i = 0usize;
                    measure(a.ops, || {
                        $eng.exec(&fresh[i]);
                        i += 1;
                    })
                }
                "unlink" => {
                    let mut i = 0usize;
                    measure(a.ops, || {
                        $eng.exec(&gone[i]);
                        i += 1;
                    })
                }
                _ => (0.0, 0.0, 0.0),
            };
            println!("{:<14} {:>9.1} {:>10.2} {:>10.1}", $name, ns, al, by);
        }};
    }
    for name in [
        "ping",
        "lookup",
        "lookup-miss",
        "getattr",
        "readdir",
        "setattr",
        "create",
        "unlink",
    ] {
        row!(eng, name);
    }

    // 目录规模再翻两倍，看读 op 的 ns/op 怎么跟着长（BTreeMap 该是 log，
    // 若实测接近平方就是哪里在扫全表）。
    println!("# scaling check @ items={}", a.items * 3);
    for k in (a.items + n_write)..(a.items * 3 + n_write) {
        eng.exec(&Op::Create {
            view,
            parent: 1,
            name: format!("f{k}").into_bytes(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
    }
    for name in ["lookup", "readdir"] {
        row!(eng, name);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
