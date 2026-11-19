//! treedb 独立演示。
//!
//! 三种模式：
//! - `--offline`：只跑本地镜像 + 单测式演示，不需要 PG；用来在 CI / 无 PG
//!   环境验证镜像读写、schema 编解码、fallback 认证的整套流程。
//! - `--online <uri>`：连一份 PG 演示共享池、密钥 CRUD、审计缓冲。
//! - `--bench`：万级视图 × 单池的模拟压测，不真连 PG，只演示池容量与
//!   视图数解耦的算术效果。
//!
//! 用法：`cargo run --bin treedb-demo -- --offline`。

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use treedb::{
    persist::Mirror, pool::SharedPoolManager, store::audit::AuditRow, store::audit::AuditWriter,
    store::keys::KeyStore, PoolConfig, StoreConfig,
};

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let mode = argv
        .iter()
        .position(|a| a.starts_with("--"))
        .map(|i| argv[i].as_str())
        .unwrap_or("--offline");
    match mode {
        "--offline" => offline(),
        "--online" => {
            let uri = argv.get(3).cloned().unwrap_or_else(|| {
                eprintln!("usage: treedb-demo --online <postgres://...>");
                std::process::exit(2);
            });
            online(&uri);
        }
        "--bench" => bench(),
        other => {
            eprintln!("unknown mode: {other}");
            eprintln!("try: --offline | --online <uri> | --bench");
            std::process::exit(2);
        }
    }
}

fn offline() {
    println!("== treedb 离线演示 ==");

    // 1. 视图名 ↔ schema 编解码。
    for v in ["alice", "team-a", "队a"] {
        let s = treedb::schema_for_view(v).expect("valid");
        let back = treedb::schema::decode(&s).expect("decodes");
        println!("  {v:>8} ↔ {s} → {back}");
    }

    // 2. 本地镜像：建 store、写一行、fallback 认证。
    let dir = std::env::temp_dir().join(format!("treedb-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mirror = Arc::new(Mirror::open(&dir).expect("open mirror"));
    mirror
        .upsert_key(
            "deadbeefcafe1234",
            "analyst",
            &sha256_hex("c0ffee"),
            &treedb::persist::now_iso(),
            None,
            None,
            "any",
            Some("alice"),
            false,
            Some("alice"),
            None,
            None,
            None,
            Some("c0ff\u{2026}ffee"),
        )
        .expect("upsert");
    let out = mirror
        .authenticate_fallback("deadbeefcafe1234", &sha256_hex("c0ffee"), i64::MAX)
        .expect("query")
        .expect("row exists");
    println!("  fallback auth: {out:?}");

    // 3. 快照 + prune。
    let snap = mirror.snapshot().expect("snapshot");
    println!("  snapshot → {}", snap.display());
    let n = mirror.prune_snapshots(5).expect("prune");
    println!("  pruned {n} old snapshots");

    println!("  tmp: {}", dir.display());
    println!("  (leave dir for inspection; rm manually)");
}

fn online(uri: &str) {
    println!("== treedb 在线演示: {uri} ==");
    let cfg = PoolConfig::default();
    let mgr = Arc::new(SharedPoolManager::new(
        uri.to_string(),
        vec![("main".to_string(), uri.to_string())],
        cfg.clone(),
    ));
    let pool = mgr.pool_by_name("main").expect("main pool");
    let reachable = Arc::new(AtomicBool::new(true));
    let store = KeyStore::new(
        Arc::clone(&pool),
        None,
        Arc::clone(&reachable),
        false,
    )
    .expect("keystore");
    let audit = AuditWriter::new(
        Arc::clone(&pool),
        None,
        Arc::clone(&reachable),
        StoreConfig::default(),
    )
    .expect("audit writer");

    let rec = store
        .create("demo", "any", None, Some("demo"), None, None, None, false)
        .expect("create");
    println!("  minted id={} secret={} hint={}", rec.id, rec.secret, rec.hint);
    let authed = store
        .authenticate(&rec.id, &rec.secret)
        .expect("authenticate");
    println!("  authenticated as {:?}", authed);

    audit
        .push(AuditRow {
            ts: treedb::persist::now_iso(),
            key_id: Some(rec.id.clone()),
            key_label: Some("demo".into()),
            method: "tools/call".into(),
            tool: "view.list".into(),
            ok: true,
            detail: "n=0".into(),
        })
        .expect("push");
    let flushed = audit.flush_now().expect("flush");
    println!("  flushed {flushed} audit rows");

    store.revoke(&rec.id).expect("revoke");
    println!("  revoked {}", rec.id);
    println!("  pool live={} idle={}", pool.live(), pool.idle());
}

fn bench() {
    println!("== treedb 容量算术：万级视图 vs 每视图独立池 ==");
    // 三档配置对比：pg_store 默认档 / pg_store 弹性档 / treedb 共享池。
    let views: usize = 10_000;
    let max_conn: usize = 200;
    for (name, per_view, shared) in [
        ("pg_store default 8W+4R", 12usize, None),
        ("pg_store elastic  2W+1R", 3, None),
        ("treedb shared 16 warm / 64 max", 0, Some((16usize, 64usize))),
    ] {
        if let Some((warm, max)) = shared {
            println!("  {name}");
            println!("    稳态占用: {warm} 连接 (与视图数无关)");
            println!("    峰值上限: {max} 连接");
            println!(
                "    → 单 PG (max_connections={max_conn}) 支撑视图数: 无上限（并发受 max={max} 约束）"
            );
        } else {
            println!("  {name}");
            println!("    稳态占用: {} × {views} = {}", per_view, per_view * views);
            println!(
                "    → 单 PG (max_connections={max_conn}) 支撑视图数: {}",
                max_conn / per_view
            );
        }
    }
    println!("结论: 从每视图独立池→实例级共享池, 单实例承载量从 ~16 → 万级。");
}

fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let out = h.finalize();
    out.iter().map(|b| format!("{b:02x}")).collect()
}
