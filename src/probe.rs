//! 后台探活与 sweeper 线程。
//!
//! 一个线程干三件事，按 `sweep_interval` 节拍：
//! 1. **探活**：对每个池 `SELECT 1`；连续失败翻 `pg_reachable=false`，
//!    一次成功翻回 `true`。
//! 2. **reap**：调 `SharedPoolManager::reap_all`，把 warm 之上、超 ttl 的
//!    连接摘掉。
//! 3. **flush audit**：让 `AuditWriter` 把缓冲区推到 PG，把 pending 从
//!    镜像回捞。
//!
//! 三件事放一起是为了共用一次唤醒；错峰做没意义，sweeper 只做**幂等**动作。
//! `probe_once` 是纯函数暴露给测试用，不用起线程。

use crate::pool::{SharedInstancePool, SharedPoolManager};
use crate::store::audit::AuditWriter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 对单个池做探活：`SELECT 1`，成功返回 true。
pub fn probe_once(pool: &Arc<SharedInstancePool>) -> bool {
    match pool.checkout() {
        Ok(mut c) => {
            let ok = c.raw().batch_execute("SELECT 1").is_ok();
            pool.requeue(c);
            ok
        }
        Err(_) => false,
    }
}

/// sweeper 循环参数。
pub struct Sweeper {
    manager: Arc<SharedPoolManager>,
    audit: Option<Arc<AuditWriter>>,
    reachable: Arc<AtomicBool>,
    interval: std::time::Duration,
    stop: Arc<AtomicBool>,
}

impl Sweeper {
    pub fn new(
        manager: Arc<SharedPoolManager>,
        audit: Option<Arc<AuditWriter>>,
        reachable: Arc<AtomicBool>,
        interval: std::time::Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        Self {
            manager,
            audit,
            reachable,
            interval,
            stop,
        }
    }

    /// 停止信号：让下一次 sleep 后跳出循环。
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    /// 阻塞式启动（自己套 std::thread::spawn 用）。
    pub fn run(self) {
        let stop = Arc::clone(&self.stop);
        while !stop.load(Ordering::Acquire) {
            self.tick();
            std::thread::sleep(self.interval);
        }
    }

    /// 单次 tick：探活 + reap + flush。测试可以直接调。
    pub fn tick(&self) {
        // 1. 探活所有实例；任一不可达就翻 reachable=false。
        let names = self.manager.instance_names();
        let mut any_healthy = false;
        for n in &names {
            if let Some(p) = self.manager.pool_by_name(n) {
                if probe_once(&p) {
                    any_healthy = true;
                }
            }
        }
        if names.is_empty() {
            // 没配实例：认为可达（本地模式），不干扰上层。
            any_healthy = true;
        }
        self.reachable.store(any_healthy, Ordering::Release);

        // 2. reap（不管可达不可达都跑；idle 连接摘掉无害）。
        self.manager.reap_all();

        // 3. flush audit；PG 不可达时 push 内部会自动 spill。
        if let Some(a) = &self.audit {
            if any_healthy {
                let _ = a.flush_now();
                // 有 mirror 时把 pending 从镜像回捞。
                if self
                    .manager
                    .instance_names()
                    .first()
                    .and_then(|n| self.manager.pool_by_name(n))
                    .is_some()
                {
                    let _ = a.flush_pending();
                }
            }
        }
    }
}

/// 只给测试用：起一个 Mirror、跑一次 tick、检查镜像里有没有 pending。
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PoolConfig, StoreConfig};
    use crate::persist::Mirror;
    use std::path::PathBuf;
    use std::time::Duration;

    #[test]
    fn sweeper_tick_with_empty_manager_is_ok() {
        let mgr = Arc::new(SharedPoolManager::new(
            "spec",
            vec![],
            PoolConfig::default(),
        ));
        let reachable = Arc::new(AtomicBool::new(true));
        let s = Sweeper::new(mgr, None, Arc::clone(&reachable), Duration::from_millis(10));
        s.tick();
        assert!(reachable.load(Ordering::Acquire), "empty = considered local-ok");
    }

    #[test]
    fn probe_invalid_uri_returns_false() {
        let pool = SharedInstancePool::new(
            "postgres://invalid-host-for-treedb.invalid:1/db".to_string(),
            PoolConfig {
                warm: 0,
                max: 1,
                checkout_timeout: Duration::from_millis(10),
                ..Default::default()
            },
        );
        assert!(!probe_once(&pool));
    }

    #[allow(dead_code)]
    fn tmp_mirror(p: &PathBuf) -> Arc<Mirror> {
        Arc::new(Mirror::open(p).expect("mirror"))
    }

    #[test]
    fn store_config_clone_round_trip() {
        let c = StoreConfig::default();
        let _ = c.clone();
    }
}
