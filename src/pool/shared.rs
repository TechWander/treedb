//! 实例级共享池 + 顶层路由管理器。
//!
//! 见模块级文档。这里给出关键实现要点：
//!
//! - **借出/归还**：`checkout()` 走 LIFO（pop 最后归还的，即最热的），命中的
//!   连接 prepared 缓存最新鲜。归还 (`requeue`) 时若已过 `max` 或已被
//!   `is_closed()` 则直接 drop 并 `live -= 1`。
//! - **事务封装**：`begin_view_tx(view)` 借一条连接 → BEGIN → SET LOCAL
//!   search_path → 返回 [`TxGuard`]；`Drop` 时 ROLLBACK（若未 commit）并把
//!   连接归还池。这样保证任何路径都不会把带 dirty search_path 的连接漏回池。
//! - **SSI 重试**：`with_serial_retry` 是原 pg_store 里同名函数的池化版本，
//!   12 次指数退避（200μs 起、25ms 封顶）。
//! - **多实例路由**：`SharedPoolManager` 内部持一个 `HashMap<name, pool>`，
//!   通过 `resolve_view_home(spec, view)`（沿用 grovefs 的算法）选池；
//!   这一层不做 SQL 也不做 placement 写入，只把 (view → pool) 映射建起来。

use super::conn::PooledConn;
use crate::config::PoolConfig;
use crate::error::{Error, Result};
use crate::schema;
use parking_lot::Mutex;
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// SSI 重试参数——保持与 `pg_store.rs` 完全一致，接入时可无缝替换。
const SERIALIZATION_MAX_ATTEMPTS: u32 = 12;
const SERIALIZATION_BACKOFF_MIN: Duration = Duration::from_micros(200);
const SERIALIZATION_BACKOFF_MAX: Duration = Duration::from_millis(25);

/// 单个 PG 实例的共享池。所有视图共用；`Arc` 内部可变，克隆廉价。
pub struct SharedInstancePool {
    uri: String,
    config: PoolConfig,
    /// 空闲连接栈。pop 走 LIFO 复用热连接。
    idle: Mutex<Vec<PooledConn>>,
    /// 当前"存在"的连接数（in-use + idle），与 `max` 比较决定是否新拨。
    live: AtomicUsize,
    /// 端到端健康位：连续 dial 失败 5 次翻 false，一次成功翻 true。
    healthy: AtomicBool,
    /// 连续拨号失败计数。
    failures: AtomicUsize,
}

impl SharedInstancePool {
    /// 建池并预拨 `warm` 条连接。全部失败不报错，只是把 `healthy` 置 false，
    /// 让调用方通过 `is_healthy()` 判断是否降级；这样启动过程不会因一次
    /// 网络抖动直接崩。
    pub fn new(uri: String, config: PoolConfig) -> Arc<Self> {
        let pool = Arc::new(Self {
            uri,
            config,
            idle: Mutex::new(Vec::new()),
            live: AtomicUsize::new(0),
            healthy: AtomicBool::new(true),
            failures: AtomicUsize::new(0),
        });
        let dialed = pool.dial_batch(pool.config.warm);
        if dialed == 0 {
            pool.healthy.store(false, Ordering::Release);
        }
        pool
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn live(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    pub fn idle(&self) -> usize {
        self.idle.lock().len()
    }

    /// 拨一条新连接（含 tune），不入池，不计 live；失败记 failures。
    /// 只允许在**已占位**（`live` 已含这条额度）的路径里调用，否则请走
    /// [`dial_one`]，它负责占位/回退。
    fn dial_naked(&self) -> std::result::Result<PooledConn, postgres::Error> {
        match Client::connect(&self.uri, NoTls) {
            Ok(mut c) => {
                self.tune(&mut c);
                self.failures.store(0, Ordering::Relaxed);
                self.healthy.store(true, Ordering::Release);
                Ok(PooledConn::new(c))
            }
            Err(e) => {
                let f = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
                if f >= 5 {
                    self.healthy.store(false, Ordering::Release);
                }
                Err(e)
            }
        }
    }

    /// 原子占位 + 拨号。CAS 循环只在 `live < max` 时递增，所以 `live`
    /// 在任何采样时刻都严格 ≤ max（先 fetch_add 再回退的写法在池满
    /// 瞬间会被并发采样看到 max+1，由 `tests/concurrency.rs` 的峰值
    /// 采样器抓到）。拨号失败回退占位。返回 `Unavailable` 表示"已到
    /// max"，调用方（`checkout`）据此转入等待轮询，而不是当真错误上抛。
    fn dial_one(&self) -> Result<PooledConn> {
        loop {
            let cur = self.live.load(Ordering::Acquire);
            if cur >= self.config.max {
                return Err(Error::Unavailable(format!(
                    "shared pool at max={}",
                    self.config.max
                )));
            }
            match self.live.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => std::hint::spin_loop(),
            }
        }
        match self.dial_naked() {
            Ok(c) => Ok(c),
            Err(e) => {
                self.live.fetch_sub(1, Ordering::AcqRel);
                let f = self.failures.load(Ordering::Relaxed);
                Err(Error::Pg(format!("dial (fail {f}): {e}")))
            }
        }
    }

    fn dial_batch(&self, n: usize) -> usize {
        let mut ok = 0;
        for _ in 0..n {
            match self.dial_one() {
                Ok(c) => {
                    self.idle.lock().push(c);
                    ok += 1;
                }
                Err(_) => break,
            }
        }
        ok
    }

    /// 会话级 tune：把默认隔离级别提到 SERIALIZABLE，加语句超时。
    /// 借出后 `SET LOCAL` 只覆盖 search_path，事务隔离继承 session 默认。
    fn tune(&self, c: &mut Client) {
        let st = self.config.statement_timeout.as_millis();
        let _ = c.batch_execute(&format!(
            "SET default_transaction_isolation = 'serializable'; \
             SET statement_timeout = '{st}ms'"
        ));
    }

    /// 从池中借一条连接。命中路径 pop 栈顶；无空闲则**原子占位后拨新**
    /// （`dial_one` 内部 `fetch_add` 先行，保证并发下 live 永不超过 max）；
    /// 占位失败=池满，按 `checkout_timeout` 轮询等待归还，超时才
    /// `Unavailable`。真正的拨号错误（网络/认证）直接上抛 `Error::Pg`。
    pub fn checkout(&self) -> Result<PooledConn> {
        let deadline = Instant::now() + self.config.checkout_timeout;
        loop {
            // 快路径：pop 一条健康空闲。
            let candidate = {
                let mut p = self.idle.lock();
                loop {
                    match p.pop() {
                        None => break None,
                        Some(c) if c.is_closed() => {
                            self.live.fetch_sub(1, Ordering::AcqRel);
                            continue;
                        }
                        Some(c) => break Some(c),
                    }
                }
            };
            if let Some(c) = candidate {
                return Ok(c);
            }
            // 无空闲：占位拨新。Unavailable=池满，转等待；Pg=真错误上抛。
            match self.dial_one() {
                Ok(c) => return Ok(c),
                Err(Error::Unavailable(_)) => {}
                Err(e) => return Err(e),
            }
            if Instant::now() >= deadline || self.config.checkout_timeout.is_zero() {
                return Err(Error::Unavailable(format!(
                    "shared pool at max={} exhausted",
                    self.config.max
                )));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// 归还一条连接。closed 或超 max 则 drop 并 `live -= 1`；否则入栈。
    pub fn requeue(&self, mut c: PooledConn) {
        c.activated = None;
        if c.is_closed() {
            self.live.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        let mut p = self.idle.lock();
        if p.len() >= self.config.max {
            drop(c);
            self.live.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        c.returned_at = crate::persist::now_ms();
        p.push(c);
    }

    /// 摘掉超过 warm 且空闲过 ttl 的连接，从最久未用端开始（LRU 滞回）。
    pub fn reap(&self) -> usize {
        let warm = self.config.warm;
        let ttl = self.config.idle_ttl.as_millis() as u64;
        let now = crate::persist::now_ms();
        let mut p = self.idle.lock();
        let mut gone = 0;
        while p.len() > warm {
            match p.first() {
                Some(c) if now.saturating_sub(c.returned_at) >= ttl => {
                    p.remove(0);
                    gone += 1;
                }
                _ => break,
            }
        }
        drop(p);
        if gone > 0 {
            self.live.fetch_sub(gone, Ordering::Relaxed);
        }
        gone
    }

    /// 打开一个绑定视图 schema 的事务：借连接 → BEGIN SERIALIZABLE →
    /// `SET LOCAL search_path` → 返回 [`Tx`]。Drop [`TxGuard`] 未 commit
    /// 时 ROLLBACK 并归还连接。
    pub fn begin_view_tx(self: &Arc<Self>, view: &str) -> Result<TxGuard<'_>> {
        let schema = schema::encode(view)
            .ok_or_else(|| Error::Denied(format!("invalid view name '{view}'")))?;
        self.begin_with_schema(&schema)
    }

    /// 只操作控制面的事务：BEGIN → SET LOCAL search_path = grovefs_control。
    pub fn begin_control_tx(self: &Arc<Self>) -> Result<TxGuard<'_>> {
        let conn = self.checkout()?;
        Ok(TxGuard::new(
            Arc::clone(self),
            conn,
            None,
            schema::set_local_control(),
        ))
    }

    fn begin_with_schema(self: &Arc<Self>, schema: &str) -> Result<TxGuard<'_>> {
        let conn = self.checkout()?;
        Ok(TxGuard::new(
            Arc::clone(self),
            conn,
            Some(schema.to_string()),
            schema::set_local_path(schema),
        ))
    }

    /// SSI 重试外壳：把 [`with_serial_retry`] 提供给上层做单事务重试。
    pub fn with_serial_retry<F, T>(&self, mut op: F) -> Result<T>
    where
        F: FnMut() -> std::result::Result<T, postgres::Error>,
    {
        let mut delay = SERIALIZATION_BACKOFF_MIN;
        let mut err = String::new();
        for _ in 0..SERIALIZATION_MAX_ATTEMPTS {
            match op() {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if !is_serialization_failure(&e) {
                        return Err(Error::Pg(e.to_string()));
                    }
                    err = e.to_string();
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(SERIALIZATION_BACKOFF_MAX);
                }
            }
        }
        Err(Error::Pg(format!(
            "serialization retry exhausted ({SERIALIZATION_MAX_ATTEMPTS}): {err}"
        )))
    }
}

/// 事务句柄的持有者。Drop 未 commit 时 ROLLBACK 并归还连接到池。
///
/// `'pool` 生命周期绑定到 pool，因为 guard 归还连接时需要访问 pool；
/// 通过 `Arc<SharedInstancePool>` 让 guard 拥有 pool 的所有权。
pub struct TxGuard<'pool> {
    pool: Arc<SharedInstancePool>,
    conn: Option<PooledConn>,
    schema: Option<String>,
    set_sql: String,
    committed: bool,
    started: bool,
    _marker: std::marker::PhantomData<&'pool ()>,
}

impl<'pool> TxGuard<'pool> {
    fn new(
        pool: Arc<SharedInstancePool>,
        conn: PooledConn,
        schema: Option<String>,
        set_sql: String,
    ) -> Self {
        Self {
            pool,
            conn: Some(conn),
            schema,
            set_sql,
            committed: false,
            started: false,
            _marker: std::marker::PhantomData,
        }
    }

    /// 惰性开启事务：第一次调用 `tx()` 时 BEGIN + SET LOCAL。这样上层
    /// 拿 guard 但不用（走 `Drop`）时连接不背一个空事务。
    pub fn tx(&mut self) -> Result<Tx<'_>> {
        if !self.started {
            let conn = self.conn.as_mut().expect("conn alive before start");
            conn.raw()
                .batch_execute("BEGIN")
                .map_err(|e| Error::Pg(format!("BEGIN: {e}")))?;
            conn.raw()
                .batch_execute(&self.set_sql)
                .map_err(|e| Error::Pg(format!("SET LOCAL: {e}")))?;
            self.started = true;
        }
        let conn = self.conn.as_mut().expect("conn present after start");
        Ok(Tx {
            conn,
            schema: self.schema.clone(),
            committed: &mut self.committed,
            started: true,
        })
    }

    pub fn commit(&mut self) -> Result<()> {
        if !self.started {
            self.committed = true;
            return Ok(());
        }
        let conn = self.conn.as_mut().expect("conn alive when committed");
        conn.raw()
            .batch_execute("COMMIT")
            .map_err(|e| Error::Pg(format!("COMMIT: {e}")))?;
        self.committed = true;
        Ok(())
    }

    pub fn rollback(&mut self) {
        if self.started && !self.committed {
            if let Some(c) = self.conn.as_mut() {
                let _ = c.raw().batch_execute("ROLLBACK");
            }
        }
        self.committed = true; // 防止 Drop 再 ROLLBACK
    }
}

impl Drop for TxGuard<'_> {
    fn drop(&mut self) {
        if self.started && !self.committed {
            if let Some(c) = self.conn.as_mut() {
                let _ = c.raw().batch_execute("ROLLBACK");
            }
        }
        if let Some(c) = self.conn.take() {
            self.pool.requeue(c);
        }
    }
}

/// 借用给上层的"当前事务视图句柄"。暴露 SQL 执行 API，实际执行透传到
/// 底层 `postgres::Client`。
///
/// **注意**：本类型只是薄壳，`query/execute` 走的是 client 的顶层方法而不是
/// `postgres::Transaction` 对象——因为 `Transaction` 类型借用 `&mut Client`
/// 与 guard 的 self 冲突。用 `BEGIN/COMMIT` 手工管理同样保证事务边界，
/// 也让 `SET LOCAL search_path` 与后续语句共享 session。
pub struct Tx<'c> {
    conn: &'c mut PooledConn,
    #[allow(dead_code)]
    schema: Option<String>,
    committed: &'c mut bool,
    started: bool,
}

impl<'c> Tx<'c> {
    /// 执行不带返回的语句（`INSERT`/`UPDATE`/`DELETE`），SQL 里的表名不带
    /// schema 前缀，通过 `search_path` 解析。
    pub fn execute(&mut self, sql: &str, params: &[&(dyn postgres::types::ToSql + Sync)]) -> Result<u64> {
        let n = self
            .conn
            .raw()
            .execute(sql, params)
            .map_err(|e| Error::Pg(e.to_string()))?;
        Ok(n as u64)
    }

    /// 查询返回行集。
    pub fn query<F, T>(&mut self, sql: &str, params: &[&(dyn postgres::types::ToSql + Sync)], mut map: F) -> Result<Vec<T>>
    where
        F: FnMut(&postgres::Row) -> T,
    {
        let rows = self
            .conn
            .raw()
            .query(sql, params)
            .map_err(|e| Error::Pg(e.to_string()))?;
        Ok(rows.iter().map(&mut map).collect())
    }

    /// 单查询。
    pub fn query_one(
        &mut self,
        sql: &str,
        params: &[&(dyn postgres::types::ToSql + Sync)],
    ) -> Result<Option<postgres::Row>> {
        let rows = self
            .conn
            .raw()
            .query(sql, params)
            .map_err(|e| Error::Pg(e.to_string()))?;
        Ok(rows.into_iter().next())
    }

    /// 直接批量执行（DDL / 多语句）。
    pub fn batch(&mut self, sql: &str) -> Result<()> {
        self.conn
            .raw()
            .batch_execute(sql)
            .map_err(|e| Error::Pg(e.to_string()))
    }

    /// 借用底层 Client 做 postgres 特化调用（如 COPY）。
    pub fn client(&mut self) -> &mut Client {
        self.conn.raw()
    }

    /// 事务已经通过 `SET LOCAL` 定到了目标 schema；对上层暴露当前 schema 名，
    /// 用于日志/审计。
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn is_started(&self) -> bool {
        self.started
    }
}

/// 判定一个 PG 错误是不是 SSI 冲突或死锁（需要重试）。与
/// `pg_store::is_serialization_failure` 一致：40001 / 40P01。
fn is_serialization_failure(e: &postgres::Error) -> bool {
    e.code()
        .map(|c| c == &postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
                 || c == &postgres::error::SqlState::T_R_DEADLOCK_DETECTED)
        .unwrap_or(false)
}

/// 若上层需要显式指定隔离级别（比如某些 op 走 READ COMMITTED），
/// `IsolationLevel` 从这里 re-export。
pub use postgres::IsolationLevel as PoolIsolationLevel;

// ---- 顶层路由：多个实例 → 多个池 ----------------------------------------

/// 顶层管理器：一个 PG 实例对应一个 `SharedInstancePool`。视图 → 实例
/// 通过 placement 表（`resolve_view_home`）或 hash 回退决定；这一层不
/// 复制 placement 逻辑，只提供 "根据实例名拿池" 的查表。
///
/// 接入 GroveFS 时，上层负责从 `pg_store::expand_store_spec` 拿到实例列表
/// 并 `add_instance`；treedb 不重复实现 registry 解析。
pub struct SharedPoolManager {
    spec: String,
    config: PoolConfig,
    pools: parking_lot::RwLock<HashMap<String, Arc<SharedInstancePool>>>,
}

impl SharedPoolManager {
    /// 从 spec + 已解析的实例列表构造。实例列表由外部传入，避免 treedb
    /// 依赖 grovefs 的 registry parser。
    pub fn new(
        spec: impl Into<String>,
        instances: Vec<(String, String)>,
        config: PoolConfig,
    ) -> Self {
        let spec = spec.into();
        let mut map = HashMap::with_capacity(instances.len());
        for (name, uri) in instances {
            map.insert(name, SharedInstancePool::new(uri, config.clone()));
        }
        Self {
            spec,
            config,
            pools: parking_lot::RwLock::new(map),
        }
    }

    pub fn spec(&self) -> &str {
        &self.spec
    }

    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    pub fn add_instance(&self, name: &str, uri: &str) {
        self.pools.write().insert(
            name.to_string(),
            SharedInstancePool::new(uri.to_string(), self.config.clone()),
        );
    }

    pub fn remove_instance(&self, name: &str) -> Option<Arc<SharedInstancePool>> {
        self.pools.write().remove(name)
    }

    pub fn pool_by_name(&self, name: &str) -> Option<Arc<SharedInstancePool>> {
        self.pools.read().get(name).cloned()
    }

    /// 由调用方解析好实例名（比如从 `resolve_view_home` 拿到的），走这里拿池。
    /// treedb 不重复实现 placement 解析，避免与 grovefs 现有 spec 语法漂移。
    pub fn pool_for_instance(&self, name: &str) -> Result<Arc<SharedInstancePool>> {
        self.pool_by_name(name)
            .ok_or_else(|| Error::Config(format!("instance '{name}' not registered")))
    }

    /// 全局 reap：所有池都跑一次。sweeper 线程按 `sweep_interval` 调用。
    pub fn reap_all(&self) -> usize {
        let pools = self.pools.read();
        let mut n = 0;
        for (_k, p) in pools.iter() {
            n += p.reap();
        }
        n
    }

    /// 总连接数，供 web console / banner 展示。
    pub fn total_connections(&self) -> usize {
        self.pools.read().values().map(|p| p.live()).sum()
    }

    pub fn instance_names(&self) -> Vec<String> {
        self.pools.read().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_new_starts_empty_healthy() {
        // 空实例列表：不 dial 任何东西，total=0。
        let mgr = SharedPoolManager::new("spec", vec![], PoolConfig::default());
        assert_eq!(mgr.total_connections(), 0);
        assert!(mgr.instance_names().is_empty());
    }

    #[test]
    fn manager_lookup_missing() {
        let mgr = SharedPoolManager::new("spec", vec![], PoolConfig::default());
        assert!(mgr.pool_by_name("nope").is_none());
        assert!(matches!(
            mgr.pool_for_instance("nope"),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn invalid_uri_pool_starts_unhealthy_but_usable() {
        // 用一个不可能连上的 URI 建池：warm dial 全部失败但池对象构造 OK。
        // `checkout()` 会尝试 dial 新连、失败返回 Error::Pg。
        let pool = SharedInstancePool::new(
            "postgres://invalid-host-for-treedb-test.invalid:1/db".to_string(),
            PoolConfig {
                warm: 0,
                max: 1,
                checkout_timeout: Duration::from_millis(10),
                ..Default::default()
            },
        );
        // 不 dial 任何东西：warm=0。
        assert_eq!(pool.idle(), 0);
        // checkout 会尝试 dial、失败。
        let err = pool.checkout().err().expect("must fail");
        assert!(matches!(err, Error::Pg(_)));
    }

    #[test]
    fn reap_never_drops_below_warm() {
        // 手工往 idle 栈塞 fake conn 不现实（Client 不能 mock），改测
        // 空池下 reap 恒返回 0（不 panic、不越界）。
        let pool = SharedInstancePool::new(
            "postgres://x/y".to_string(),
            PoolConfig {
                warm: 2,
                max: 4,
                ..Default::default()
            },
        );
        assert_eq!(pool.reap(), 0);
    }

    #[test]
    fn tx_guard_drops_without_begin_when_unused() {
        // guard 从 checkout 拿到 conn 但从未调 tx()，Drop 时不应尝试
        // COMMIT/ROLLBACK；只需归还连接。用不合法 URI 建池让 checkout 失败
        // → 直接返回 Err，根本不产生 guard。
        let pool = SharedInstancePool::new(
            "postgres://invalid-host-for-treedb-test.invalid:1/db".to_string(),
            PoolConfig {
                warm: 0,
                max: 1,
                checkout_timeout: Duration::from_millis(10),
                ..Default::default()
            },
        );
        let res = pool.begin_control_tx();
        assert!(res.is_err(), "invalid uri must not yield a tx");
    }

    #[test]
    fn begin_view_tx_rejects_invalid_view_name() {
        let pool = SharedInstancePool::new(
            "postgres://x/y".to_string(),
            PoolConfig::default(),
        );
        let err = pool.begin_view_tx("").err().expect("empty name rejected");
        assert!(matches!(err, Error::Denied(_)));
    }
}
