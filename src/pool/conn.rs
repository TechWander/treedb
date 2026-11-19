//! 单条池化连接。
//!
//! 承载三件事：
//! 1. **底层 `postgres::Client`**：所有 PG 调用透传。
//! 2. **per-connection prepared 缓存**：只对**已带 schema 前缀**的语句
//!    （`grovefs_control.*` / `grovefs_refs.*`）启用，因为它们与 search_path
//!    无关、可跨视图复用。视图树一侧的语句在 `SET LOCAL search_path` 下
//!    是 schema-dependent 的，走 unnamed 每 tx 重解析——实测代价 45%，但
//!    换到「池与视图数解耦」这一结构收益；两条路线的取舍记录在 DESIGN.md
//!    §4，接入时由上层选择。
//! 3. **借出/归还时间戳**：用于 `SharedInstancePool::reap` 的 LRU 回收。

use postgres::{Client, Statement};
use std::collections::HashMap;

/// prepared 缓存上限：一条连接最多保留这么多条语句，超过后仍返回新 prepare
/// 的语句但不入缓存（避免无限增长）。与 pg_store 现有 STMT_CACHE_CAP 一致。
const STMT_CACHE_CAP: usize = 64;

/// 池化连接。所有字段私有，通过方法访问，避免调用方直接摸 Client 破坏
/// search_path 不变量。
pub struct PooledConn {
    pub(crate) client: Client,
    pub(crate) fixed_stmts: HashMap<String, Statement>,
    /// 上次归还池的时刻（毫秒 since epoch），reap 用。
    pub(crate) returned_at: u64,
    /// 借出周期内已激活的视图 schema；SET LOCAL 只在事务内有效，跨事务
    /// 归零。
    pub(crate) activated: Option<String>,
}

impl PooledConn {
    /// 从新拨号或已存的 `Client` 构造池化条目。`returned_at` 现在戳。
    pub fn new(client: Client) -> Self {
        Self {
            client,
            fixed_stmts: HashMap::new(),
            returned_at: crate::persist::now_ms(),
            activated: None,
        }
    }

    /// 底层 Client 是否已被服务端关闭（PG 重启 / idle timeout 触发）。
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// 借用底层 Client 做透传操作。只在 crate 内可见，防止外部直接跑
    /// 未 SET LOCAL 的语句。
    pub(crate) fn raw(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Prepare（或复用）一条**已带 schema 前缀**的 SQL。视图树操作**不能**
    /// 走这里，因为 prepared 会 pin 当时的 search_path；控制面/账本操作
    /// 都在 SQL 里写 `grovefs_control.xxx` / `grovefs_refs.xxx`，可以安全
    /// 跨视图缓存。
    ///
    /// 返回 `Statement`（克隆）——`postgres::Statement` 内部是 `Arc`，克隆
    /// 便宜；这样避免生命周期把调用方绑在 `&mut self` 上。
    pub(crate) fn prepare_fixed(&mut self, sql: &str) -> Result<Statement, postgres::Error> {
        if let Some(st) = self.fixed_stmts.get(sql) {
            return Ok(st.clone());
        }
        let st = self.client.prepare(sql)?;
        if self.fixed_stmts.len() < STMT_CACHE_CAP {
            self.fixed_stmts.insert(sql.to_string(), st.clone());
        }
        Ok(st)
    }
}
