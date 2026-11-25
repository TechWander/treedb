//! treedb — 元数据引擎：单进程事件循环 + 自定义二进制协议 + WAL/checkpoint。
//!
//! ## 定位
//!
//! 这是一个**数据库项目**，不是某个客户端的一个模块。它针对的负载形状是「大量小
//! 对象 + 高频点查 + 目录遍历 + 万级常驻连接」：关系库在这个形状上要为每条连接
//! fork 一个 backend、为每个事务付一次 WAL flush，所以上下文里自己做一套。
//! 类型、语义、op 都按 POSIX 元数据定，不绑某一个客户端：`src/engine` 与 `src/net`
//! 里没有、也不该有某个具体客户端的业务语义（这条有测试守着，见 `tests/independence.rs`）。
//!
//! ## 三层能力
//!
//! 1. **引擎本体** `engine`：内存状态机 + CRC32 WAL + 快照 checkpoint + 崩溃重放；
//!    POSIX 语义的 create/lookup/getattr/setattr/rename/readdir（含一页直接带属性的
//!    readdirplus）与锁、内容引用账本。另有一面**通用表**：列为不透明字节，可标
//!    唯一/索引列，写可带条件与 CAS —— 接入方的表形用它表达，而不是把业务列名
//!    搬进内核（引擎不解释任何一列）。
//! 2. **连接层** `net`：默认单线程 `epoll` 事件循环（1 fd/conn，万级连接每连接
//!    半 KiB RSS）；`--workers N` 起 N 条前端并行读，写永远由唯一提交线程定序。
//! 3. **过渡档**（feature `postgres-compat`，**默认关**）：`pool` / `store` / `persist`
//!    / `schema` / `probe` —— PG 共享池 + SQLite 镜像 + `grovefs_control` 表形。它给
//!    GroveFS 现有的 PostgreSQL 栈做搬迁脚手架，不是引擎的一部分；关掉 feature 后
//!    依赖只剩 `libc` + `thiserror`。
//!
//! 选型判据、协议字节布局、语义边界与五轴实测数据全部在 `DESIGN.md`（本文档只留
//! 入口）。
//! ## 关键不变量（引擎本体）
//!
//! - **全局定序只有一个点**：所有写都由唯一提交线程排入 WAL，`workers > 1` 也不变；
//!   group commit 把一轮内所有连接的写合成一次 fsync。
//! - **已 ack 的写不会被崩溃吞掉**：重放快照 + 尾日志后一定重现；拿不准就报错，
//!   绝不静默当成「什么都没有」（见 `tests/crash_recovery.rs`）。
//! - **同连接应答严格按请求顺序**：协议没有 request id，就地并行读也不得插到前面
//!   那条写的答复之前（`tests/read_scaling.rs` 钉住）。
//!
//! ## 关键不变量（`postgres-compat` 过渡档）
//!
//! - **PG 是唯一权威**：镜像永远是被写入的一方，任何"以 PG 可用为前提"的路径
//!   都直接读 PG，不会因为镜像落后而遮蔽新写入。
//! - **降级只在完全不可达时**：`pg_reachable` 是全局原子位，由探活线程维护，
//!   只有位为 false 时 hot path 才走镜像。
//! - **视图树元数据不入镜像**：万级 schema 的数据量镜像不划算；镜像只放
//!   控制面（api_keys / mcp_calls / view_defs / view_placement）。
//! - **search_path 只在事务内 SET LOCAL**：跨事务不残留会话状态，任何
//!   借出/归还的中间时刻连接都是干净的。
//!
//! ## 与 GroveFS 现有代码的映射（`postgres-compat` 过渡档）
//!
//! 下面这些模块**不属于引擎本体**：它们是 grovefs 现有 PostgreSQL 栈的共享池/
//! 镜像形状，接入期留在这里，靠 `postgres-compat` feature 关着。feature 关掉
//! 时这些模块根本不参与编译，链接图里也就没有 PG 客户端和 SQLite：
//!
//! | treedb 模块 | grovefs 对应 | 迁移方式 |
//! |------------|--------------|----------|
//! | `pool::SharedInstancePool` | `pg_store::PgEngine` per-view 池 | 作为 `MetaStore::Pg` 的 sibling variant |
//! | `store::keys::KeyStore` | `apikeys::ApiKeyStore` (Backend::Pg 分支) | 直接替换 PG 分支实现 |
//! | `persist::Mirror` | `apikeys::ApiKeyStore` (Backend::Sqlite 分支) | 从"主存"降级为"备份" |
//! | `store::defs::DefStore` | `mcp::PgControlStore` view_defs 表 | 接口等价 |
//! | `schema` | `pg_store::schema_for` / `control::schema_name_for` | 保持字节一致，双端可解析同名 schema

pub mod config;
/// 人工操作面（`treedb-cli` 的实现）：通用表全套 CRUD、统计、既有离线工具的统一转发，
/// 外加一个自写的 REPL。放在这儿而不是只长在 `src/bin/` 里，是因为集成测试要能在同一
/// 个进程里跑完一条命令拿到退出码；它只走 [`net::Client`] 或 [`engine::Engine::submit`]
/// 两条既有路径，不新开语义（依赖门与 `tests/independence.rs` 一致：只用 libc + thiserror
/// 加标准库）。
pub mod cli;
pub mod engine;
pub mod error;
// `--stats --format prometheus` 的文本格式：纯函数 `EngineStats -> String`，
// 不碰事件循环，也不进线协议（指标里没有任何一个字节需要服务端特殊应答）。
pub mod metrics;
pub mod net;

// 过渡档：PG 共享池 + SQLite 镜像 + grovefs_control 表形。引擎本体
// （`engine` + `net`）不引用这里的任何东西，所以关掉 feature 后剩下的是一
// 个自足的数据库，而不是“少了一半功能的 grovefs 客户端”。
#[cfg(feature = "postgres-compat")]
pub mod persist;
#[cfg(feature = "postgres-compat")]
pub mod pool;
#[cfg(feature = "postgres-compat")]
pub mod probe;
#[cfg(feature = "postgres-compat")]
pub mod schema;
#[cfg(feature = "postgres-compat")]
pub mod store;

pub use config::{PoolConfig, StoreConfig};
pub use error::{Error, Result};
#[cfg(feature = "postgres-compat")]
pub use persist::Mirror;
#[cfg(feature = "postgres-compat")]
pub use pool::{SharedInstancePool, SharedPoolManager, Tx};
#[cfg(feature = "postgres-compat")]
pub use store::{AuditWriter, DefStore, KeyRecord, KeyStore, StoredKey};

/// 库内部通用工具：把 view name 编成 PG schema 名（`grovefs_v_<hex>`）。
/// 与 `grovefs::pg_store::schema_for` 保持字节一致，双端可互解。
#[cfg(feature = "postgres-compat")]
pub fn schema_for_view(view: &str) -> Option<String> {
    schema::encode(view)
}

/// crate 版本串（诊断横幅用）。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
