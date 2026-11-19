//! 连接管理层。
//!
//! treedb 用**实例级共享池**替换 GroveFS 现有 per-view 池：一条物理 PG
//! backend 服务该实例上的所有视图，视图切换通过事务级 `SET LOCAL search_path`
//! 完成，池容量与视图数无关。
//!
//! 三个组件：
//! - [`conn::PooledConn`]：一条连接 + 会话内 prepared statement 缓存 + 借出
//!   时间戳；归还由 [`SharedInstancePool::requeue`] 决定保留还是丢弃。
//! - [`SharedInstancePool`]：单个 PG 实例的池，warm/max 语义与
//!   `pg_store::LaneBounds` 一致；额外承担 SSI 事务的 open + retry。
//! - [`SharedPoolManager`]：多实例路由，按 `resolve_view_home`（原 grovefs
//!   同名函数）把视图名映射到具体池。
//!
//! 接入 GroveFS 时 [`Tx`] 是要暴露给 `MetaProvider` 实现的顶层入口，
//! 让上层拿到一个"已经在目标视图 schema 里"的事务，写 SQL 不再关心 schema 前缀。

pub mod conn;
pub mod shared;

pub use conn::PooledConn;
pub use shared::{SharedInstancePool, SharedPoolManager, Tx, TxGuard};
