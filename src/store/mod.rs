//! 控制面存储层：API 密钥 + 审计 + 视图定义。
//!
//! treedb 把这三块独立出来，让上层（未来接入的 grovefs）用同一份实现拿到
//! 双后端 + 镜像容灾 + 审计缓冲。三块各自一个 Store：
//!
//! - [`keys::KeyStore`]：API 密钥 + 认证；PG 主 + Mirror fallback。
//! - [`audit::AuditWriter`]：审计日志；PG 主 + Mirror buffer，异步 flush。
//! - [`defs::DefStore`]：命名视图定义与放置表；PG 主 + Mirror cache。

pub mod audit;
pub mod defs;
pub mod keys;

pub use audit::AuditWriter;
pub use defs::DefStore;
pub use keys::{KeyRecord, KeyStore, StoredKey};
