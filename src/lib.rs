//! treedb — GroveFS 元数据后端专用引擎。
//!
//! ## 定位
//!
//! 本 crate 是 GroveFS 现有 `pg_store.rs / control.rs / apikeys.rs` 三块存储职责的
//! **独立重写**，先把「实例级共享池 + 控制面镜像 + WAL 持久化容灾」三件事做到能在
//! 无 GroveFS 上下文的情况下独立编译、单测、示例运行，验证设计闭环后再作为
//! 第三种 `MetaStore` variant 或 `ApiKeyStore` 后端接入 GroveFS。
//!
//! ## 三层能力
//!
//! 1. **共享连接池** [`pool::SharedInstancePool`]：连接数与视图数解耦。
//!    单池固定 N 条连接（默认 warm=16/max=64），万级视图共用；用 `SET LOCAL
//!    search_path` 事务级切换目标视图 schema，SQL 文本去 schema 化。
//! 2. **控制面三表** [`store`]：API 密钥 / 审计日志 / 视图定义，双后端
//!    （PG + 本地 SQLite 镜像），PG 权威、镜像只读容灾。
//! 3. **持久化容灾** [`persist`]：PG 不可达时 `fallback_authenticate()`
//!    仍能在毫秒级完成校验；审计日志缓冲后异步 flush，不丢一行。
//!
//! ## 关键不变量
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
//! ## 与 GroveFS 现有代码的映射
//!
//! | treedb 模块 | grovefs 对应 | 迁移方式 |
//! |------------|--------------|----------|
//! | [`pool::SharedInstancePool`] | `pg_store::PgEngine` per-view 池 | 作为 `MetaStore::Pg` 的 sibling variant |
//! | [`store::keys::KeyStore`] | `apikeys::ApiKeyStore` (Backend::Pg 分支) | 直接替换 PG 分支实现 |
//! | [`persist::Mirror`] | `apikeys::ApiKeyStore` (Backend::Sqlite 分支) | 从"主存"降级为"备份" |
//! | [`store::defs::DefStore`] | `mcp::PgControlStore` view_defs 表 | 接口等价 |
//! | [`schema`] | `pg_store::schema_for` / `control::schema_name_for` | 保持字节一致，双端可解析同名 schema |

pub mod config;
pub mod engine;
pub mod error;
pub mod net;
pub mod persist;
pub mod pool;
pub mod probe;
pub mod schema;
pub mod store;

pub use config::{PoolConfig, StoreConfig};
pub use error::{Error, Result};
pub use persist::Mirror;
pub use pool::{SharedInstancePool, SharedPoolManager, Tx};
pub use store::{AuditWriter, DefStore, KeyRecord, KeyStore, StoredKey};

/// 库内部通用工具：把 view name 编成 PG schema 名（`grovefs_v_<hex>`）。
/// 与 `grovefs::pg_store::schema_for` 保持字节一致，双端可互解。
pub fn schema_for_view(view: &str) -> Option<String> {
    schema::encode(view)
}

/// crate 版本串（诊断横幅用）。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
