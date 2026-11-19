//! 并发能力验收：证明"每实例共享池"的四条核心承诺在真实负载下成立。
//!
//! 需要真实 PG 的用例通过 `#[ignore]` 隔离；跑法：
//!
//! ```sh
//! TREEDB_PG_URI=postgres://user:pw@127.0.0.1:5433/treedb_test \
//!     cargo test --test concurrency -- --include-ignored --test-threads=1
//! ```
//!
//! `--test-threads=1` 是必需的：每个用例各自开一条 `max=8..64` 的池，
//! 若并发跑合起来会把服务端 `max_connections` 顶满，制造假失败。
//!
//! 六个用例：
//!
//! 1. `dial_failures_do_not_leak_live` —— 无需 PG。32 线程 × 3 次对
//!    不可达 URI 的 checkout：全 Err、`live` 恒为 0、无死锁。
//! 2. `concurrent_control_txs_never_exceed_max` —— 32 线程抢 `max=6`
//!    的池：全部成功；后台采样器断言观测到的峰值 `live ≤ max`（若破
//!    就是 checkout 的 load→dial→fetch_add 三步没原子，抢位失败）。
//! 3. `many_views_share_one_pool_isolated` —— 64 视图 × 4 线程共享
//!    一条 `max=8` 的池。视图数 8× 池上限，验证数据不串、池不炸。
//! 4. `checkout_at_max_waits_not_fails` —— 6 线程抢 `max=2` 的慢事务
//!    池；全部成功但耗时证明排队发生。
//! 5. `reap_shrinks_back_to_warm` —— 8 并发撑到 max，回落后
//!    `idle_ttl` 到期 reap 缩回 warm。
//! 6. `keystore_concurrent_create_unique` —— 32 线程 KeyStore.create
//!    + authenticate 全通过；池与 SHA 生成路径都线程安全。
//! 7. `throughput_ref_reports_percentiles` —— 打印 p50/p95/p99 与
//!    ops/s；不做阈值断言（避免 CI 抖动）。
//!
//! **这些全部是 `postgres-compat` 过渡档的验收，不是元数据引擎本体的。**
//! 引擎自己的并发证据在 `net_protocol.rs`（同连接流内顺序、连接间不串扰）
//! 与 `read_scaling.rs`（多 worker 并行读与单线程逐条同答复、混打不插队、
//! acked 写崩后不丢）。那些不靠 feature，默认 `cargo test` 就跑。

// 没有 `postgres-compat` 时整个文件不参与编译：它 `use` 的 `treedb::pool` /
// `treedb::store` 在那个档下根本不存在。注意不要把“0 tests”当成“过了”。
#![cfg(feature = "postgres-compat")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use treedb::error::Error;
use treedb::pool::SharedInstancePool;
use treedb::PoolConfig;

/// 需要一个真实 PG。未设 `TREEDB_PG_URI` 直接 panic，避免"0 tests
/// 假绿"这一常见坑：见 memory《cargo test 与后台任务结果不可信信号》。
fn require_pg() -> String {
    std::env::var("TREEDB_PG_URI").expect(
        "TREEDB_PG_URI must be set to run ignored concurrency tests, e.g. \
         postgres://user:pw@127.0.0.1:5433/treedb_test",
    )
}

/// 每个用例独立的 schema 前缀，防止并行/串行重跑时数据撞车。
fn case_prefix(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // schema 名视图长度上限 25，`c{tag}{pid}{nanos_mod}` 保守取短。
    format!("c{tag}{}{:05}", std::process::id() % 1000, nanos % 100_000)
}

fn pool_for(uri: String, warm: usize, max: usize, idle_ttl_ms: u64, checkout_timeout_ms: u64) -> Arc<SharedInstancePool> {
    SharedInstancePool::new(
        uri,
        PoolConfig {
            warm,
            max,
            idle_ttl: Duration::from_millis(idle_ttl_ms),
            checkout_timeout: Duration::from_millis(checkout_timeout_ms),
            sweep_interval: Duration::from_secs(3600), // 手动 reap，不让 sweeper 干扰
            statement_timeout: Duration::from_secs(30),
        },
    )
}

/// 起一个后台采样线程，返回 (峰值 Arc, 停止 flag, JoinHandle)。
/// 采样窗口以 microsecond 精度自旋；1ms 内的 transient 也能被抓住。
fn start_live_sampler(
    pool: Arc<SharedInstancePool>,
) -> (Arc<AtomicUsize>, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let peak = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (p2, s2, pool2) = (Arc::clone(&peak), Arc::clone(&stop), Arc::clone(&pool));
    let h = std::thread::spawn(move || loop {
        if s2.load(Ordering::Relaxed) {
            break;
        }
        let n = pool2.live();
        p2.fetch_max(n, Ordering::Relaxed);
        std::hint::spin_loop();
    });
    (peak, stop, h)
}

// ---- 1. 无 PG：拨号失败路径不泄漏 live 计数 --------------------------------

#[test]
fn dial_failures_do_not_leak_live() {
    let pool = SharedInstancePool::new(
        "postgres://treedb-nonexistent.invalid:1/db".to_string(),
        PoolConfig {
            warm: 0,
            max: 8,
            checkout_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    );
    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for _ in 0..32 {
            let (pool, ok, err) = (Arc::clone(&pool), Arc::clone(&ok), Arc::clone(&err));
            s.spawn(move || {
                for _ in 0..3 {
                    match pool.begin_control_tx() {
                        Ok(_) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });
    assert_eq!(ok.load(Ordering::Relaxed), 0, "no dial should succeed");
    assert_eq!(err.load(Ordering::Relaxed), 32 * 3, "every attempt must Err");
    assert_eq!(pool.live(), 0, "failed dial must not bump live");
    assert_eq!(pool.idle(), 0);
}

// ---- 2. 上限：并发抢池绝不越 max -------------------------------------------

#[test]
#[ignore = "requires TREEDB_PG_URI"]
fn concurrent_control_txs_never_exceed_max() {
    let uri = require_pg();
    // warm=2, max=6：突发扩到 6 是允许的，7 就是破口。
    let pool = pool_for(uri, 2, 6, 60_000, 10_000);
    let (peak, stop, sampler) = start_live_sampler(Arc::clone(&pool));

    let threads = 32usize;
    let per_thread = 20usize;
    let mut hs = Vec::with_capacity(threads);
    for _ in 0..threads {
        let pool = Arc::clone(&pool);
        hs.push(std::thread::spawn(move || {
            for _ in 0..per_thread {
                let mut g = pool.begin_control_tx().expect("checkout");
                {
                    let mut tx = g.tx().expect("begin");
                    let _ = tx
                        .query_one("SELECT 1::int4", &[])
                        .expect("select 1");
                }
                g.commit().expect("commit");
            }
        }));
    }
    for h in hs {
        h.join().expect("no panic in worker");
    }

    stop.store(true, Ordering::Relaxed);
    sampler.join().expect("sampler");
    let observed = peak.load(Ordering::Relaxed);
    assert!(
        observed <= 6,
        "pool exceeded max: peak_live={observed} max=6 (checkout 的 load→dial→fetch_add 非原子)"
    );
    assert!(pool.idle() <= pool.live());
    assert!(pool.live() <= 6, "final live {} exceeds max", pool.live());
}

// ---- 3. 视图数 ≫ 池上限，数据互不串 ---------------------------------------

#[test]
#[ignore = "requires TREEDB_PG_URI"]
fn many_views_share_one_pool_isolated() {
    let uri = require_pg();
    let prefix = case_prefix("v");
    let n_views = 64usize;
    let pool = pool_for(uri, 2, 8, 60_000, 30_000);

    // 64 个短视图名：`{prefix}{k:02}`，总长 ≤ 25 字节（prefix 已限长）。
    let views: Vec<String> = (0..n_views)
        .map(|k| format!("{prefix}{k:02}"))
        .collect();
    for v in &views {
        assert!(v.len() <= treedb::schema::VIEW_NAME_MAX, "view {v} too long");
    }

    // 一次性建 64 schema + `ctbl`；SQL 显式带 schema 前缀，走控制面 tx。
    {
        let mut g = pool.begin_control_tx().expect("setup begin");
        let mut tx = g.tx().expect("setup tx");
        for v in &views {
            let s = treedb::schema::encode(v).unwrap();
            tx.batch(&format!("CREATE SCHEMA IF NOT EXISTS \"{s}\""))
                .expect("create schema");
            tx.batch(&format!(
                "CREATE TABLE IF NOT EXISTS \"{s}\".ctbl (id BIGINT PRIMARY KEY, owner TEXT NOT NULL)"
            ))
            .expect("create ctbl");
        }
        drop(tx);
        g.commit().expect("setup commit");
    }

    // 每个视图 4 线程 × 每线程 4 行；行 owner 是视图名本身。
    let (peak, stop, sampler) = start_live_sampler(Arc::clone(&pool));
    let mut hs = Vec::new();
    for (vi, view) in views.iter().enumerate() {
        for k in 0..4usize {
            let pool = Arc::clone(&pool);
            let view = view.clone();
            let base = (vi * 100 + k * 10) as i64;
            hs.push(std::thread::spawn(move || {
                for j in 0..4i64 {
                    let id = base + j;
                    let mut g = pool.begin_view_tx(&view).expect("view begin");
                    {
                        let mut tx = g.tx().expect("view tx");
                        let n = tx
                            .execute(
                                "INSERT INTO ctbl(id, owner) VALUES ($1, $2) \
                                 ON CONFLICT (id) DO NOTHING",
                                &[&id, &view],
                            )
                            .expect("insert");
                        assert!(n <= 1, "single-row insert affected {n}");
                    }
                    g.commit().expect("view commit");
                }
            }));
        }
    }
    for h in hs {
        h.join().expect("worker panic");
    }
    stop.store(true, Ordering::Relaxed);
    sampler.join().expect("sampler");

    // 视图数 256 × 池上限 8：峰值若破 8，就是 checkout 抢位漏。
    assert!(
        peak.load(Ordering::Relaxed) <= 8,
        "shared pool exceeded max under 64-view × 4-thread load (peak={})",
        peak.load(Ordering::Relaxed)
    );

    // 验证数据不串：每视图期望 16 行，且 owner 全 = 该视图名。
    for view in &views {
        let mut g = pool.begin_view_tx(view).expect("verify begin");
        let mut tx = g.tx().expect("verify tx");
        let (cnt, all_mine): (i64, bool) = tx
            .query_one(
                "SELECT count(*), bool_and(owner = $1) FROM ctbl",
                &[view],
            )
            .expect("verify query")
            .map(|r| (r.get(0), r.get::<_, Option<bool>>(1).unwrap_or(false)))
            .unwrap_or((0, false));
        drop(tx);
        g.commit().expect("verify commit");
        assert_eq!(cnt, 16, "view {view} row count");
        assert!(all_mine, "view {view} contains rows owned by another view");
    }
}

// ---- 4. 池满排队而非失败 ----------------------------------------------------

#[test]
#[ignore = "requires TREEDB_PG_URI"]
fn checkout_at_max_waits_not_fails() {
    let uri = require_pg();
    // max=2, 6 个并发慢事务，每个 pg_sleep 0.2s；checkout_timeout 给足 5s。
    let pool = pool_for(uri, 1, 2, 60_000, 5_000);
    let start = Instant::now();
    let mut hs = Vec::new();
    for _ in 0..6 {
        let pool = Arc::clone(&pool);
        hs.push(std::thread::spawn(move || {
            let mut g = pool.begin_control_tx().expect("begin");
            {
                let mut tx = g.tx().expect("tx");
                tx.query_one("SELECT pg_sleep(0.2)", &[])
                    .expect("sleep");
            }
            g.commit().expect("commit");
        }));
    }
    for h in hs {
        h.join().expect("worker panic");
    }
    let wall = start.elapsed();
    // 2 通道 × 6 事务 × 0.2s 串行下界 = 0.6s；再加 dial/网络开销。
    assert!(
        wall >= Duration::from_millis(550),
        "expected serialization through 2-slot pool, wall={wall:?}"
    );
    assert!(pool.live() <= 2);
}

// ---- 5. reap 缩回 warm -----------------------------------------------------

#[test]
#[ignore = "requires TREEDB_PG_URI"]
fn reap_shrinks_back_to_warm() {
    let uri = require_pg();
    let (warm, max) = (1usize, 8usize);
    let pool = pool_for(uri, warm, max, /*idle_ttl=*/ 150, 30_000);

    // 8 并发把池撑到 max。
    let mut hs = Vec::new();
    for _ in 0..max {
        let pool = Arc::clone(&pool);
        hs.push(std::thread::spawn(move || {
            let mut g = pool.begin_control_tx().expect("begin");
            {
                let mut tx = g.tx().expect("tx");
                tx.query_one("SELECT pg_sleep(0.15)", &[]).expect("sleep");
            }
            g.commit().expect("commit");
        }));
    }
    for h in hs {
        h.join().expect("worker");
    }
    let burst_live = pool.live();
    assert!(burst_live > warm, "burst should push live above warm (live={burst_live})");

    // 过 TTL 后 reap 应缩回 warm；150ms ttl + 一点缓冲。
    std::thread::sleep(Duration::from_millis(400));
    let gone = pool.reap();
    assert!(gone >= 1, "reap should reclaim at least one idle above warm");
    assert_eq!(
        pool.live(),
        warm,
        "after reap live must equal warm floor ({warm}); live={}",
        pool.live()
    );
}

// ---- 6. KeyStore 并发：32 create + 32 authenticate 全通过 -------------------

#[test]
#[ignore = "requires TREEDB_PG_URI"]
fn keystore_concurrent_create_unique() {
    use std::sync::atomic::AtomicBool as AB;
    use treedb::store::keys::KeyStore;

    let uri = require_pg();
    let prefix = case_prefix("k");
    // 独立 schema 隔离不同测试运行，避免累积。
    let pool = pool_for(uri, 2, 6, 60_000, 10_000);
    let reachable = Arc::new(AB::new(true));
    let store = Arc::new(
        KeyStore::new(Arc::clone(&pool), None, Arc::clone(&reachable), false)
            .expect("keystore init"),
    );

    // 用唯一前缀清理本用例历史行，让 count 检查可靠。
    {
        let mut g = pool.begin_control_tx().expect("cleanup begin");
        let mut tx = g.tx().expect("cleanup tx");
        tx.execute(
            "DELETE FROM grovefs_control.api_keys WHERE label LIKE $1",
            &[&format!("{prefix}%")],
        )
        .expect("cleanup");
        drop(tx);
        g.commit().expect("cleanup commit");
    }

    let n = 32usize;
    let mut hs = Vec::with_capacity(n);
    let mut records: Vec<Arc<std::sync::Mutex<Option<(String, String)>>>> =
        (0..n).map(|_| Arc::new(std::sync::Mutex::new(None))).collect();
    for i in 0..n {
        let store = Arc::clone(&store);
        let rec = Arc::clone(&records[i]);
        let label = format!("{prefix}{i}");
        hs.push(std::thread::spawn(move || {
            let r = store
                .create(&label, "any", None, Some(&label), None, None, None, false)
                .expect("create");
            let authed = store
                .authenticate(&r.id, &r.secret)
                .expect("authenticate own key");
            assert_eq!(authed.label, label);
            *rec.lock().unwrap() = Some((r.id, r.secret));
        }));
    }
    for h in hs {
        h.join().expect("keystore worker");
    }

    // 32 个 id 必须两两不同。
    let ids: Vec<String> = records
        .iter()
        .map(|r| r.lock().unwrap().as_ref().unwrap().0.clone())
        .collect();
    let uniq: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(uniq.len(), n, "concurrent create minted duplicate ids");

    // revoke 一把，再 authenticate 应 Denied。
    let victim = &ids[0];
    store.revoke(victim).expect("revoke");
    let sec = records[0].lock().unwrap().as_ref().unwrap().1.clone();
    let err = store
        .authenticate(victim, &sec)
        .expect_err("revoked must deny");
    assert!(matches!(err, Error::Denied(_)), "expected Denied, got {err:?}");

    assert!(pool.live() <= 6);
}

// ---- 7. 吞吐参考：不判阈值，只打印 ---------------------------------------

#[test]
#[ignore = "requires TREEDB_PG_URI"]
fn throughput_ref_reports_percentiles() {
    let uri = require_pg();
    let pool = pool_for(uri, 4, 16, 60_000, 10_000);

    let threads = 8usize;
    let per_thread = 500usize;
    let mut samples: Vec<Vec<f64>> = Vec::with_capacity(threads);
    let total_ops = threads * per_thread;

    let start = Instant::now();
    let mut hs = Vec::with_capacity(threads);
    for _ in 0..threads {
        let pool = Arc::clone(&pool);
        hs.push(std::thread::spawn(move || {
            let mut local: Vec<f64> = Vec::with_capacity(per_thread);
            for _ in 0..per_thread {
                let t0 = Instant::now();
                let mut g = pool.begin_control_tx().expect("begin");
                {
                    let mut tx = g.tx().expect("tx");
                    tx.query_one("SELECT 1::int4", &[]).expect("q");
                }
                g.commit().expect("commit");
                local.push(t0.elapsed().as_secs_f64() * 1e3);
            }
            local
        }));
    }
    for h in hs {
        samples.push(h.join().expect("worker"));
    }
    let wall = start.elapsed().as_secs_f64();

    let mut all: Vec<f64> = samples.into_iter().flatten().collect();
    assert_eq!(all.len(), total_ops);
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| all[((all.len() as f64 - 1.0) * q) as usize];
    println!(
        "treedb 共享池吞吐参考 (threads={threads}, max=16): \
         p50={:.2}ms p95={:.2}ms p99={:.2}ms ops/s={:.0} live={} idle={}",
        p(0.50),
        p(0.95),
        p(0.99),
        total_ops as f64 / wall,
        pool.live(),
        pool.idle(),
    );
}
