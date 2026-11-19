//! 统一错误类型。
//!
//! treedb 面对三种失败面：PG 侧、SQLite 镜像侧、语义侧（视图名不合法、
//! 认证被拒）。全部收敛到一个 [`Error`] 枚举，让上层 `match` 得到穷尽覆盖，
//! 也让 `From<postgres::Error>` / `From<rusqlite::Error>` 让 `?` 自然冒泡。

/// 库内别名字面量：`Result<T, Error>`。
pub type Result<T> = std::result::Result<T, Error>;

/// treedb 全局错误枚举。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// PostgreSQL 侧失败：拨号、执行、事务、SSI 重试耗尽。
    #[error("postgres: {0}")]
    Pg(String),

    /// 本地 SQLite 镜像失败：文件不可写、DDL 错、WAL 恢复不完整。
    #[error("mirror sqlite: {0}")]
    Mirror(String),

    /// 语义错：视图名不合法、密钥摘要不匹配、认证被拒。
    #[error("denied: {0}")]
    Denied(String),

    /// 配置/环境错：spec 解析失败、注册表文件读不到。
    #[error("config: {0}")]
    Config(String),

    /// 池耗尽或降级：所有连接被借出且已到 max、控制面不可达。
    #[error("unavailable: {0}")]
    Unavailable(String),

    /// 字节流解析失败：帧截断、未知 tag、声明长度越界。
    /// 引擎的 WAL / 快照 / 线协议共用此错误面。
    #[error("codec: {0}")]
    Codec(String),

    /// 底层 IO：数据目录不可写、fsync 失败。
    #[error("io: {0}")]
    Io(String),
}

// 这两个 `From` 只为过渡档服务：`Error::Pg`/`Error::Mirror` 存的是字面量串，
// 引擎本体永远不构造它们。把它们跟 `postgres-compat` 绑在一起，而不是
// 把变体一起关掉 —— 免得下游已经写好的 `Error::Pg(_)` 分支在换 feature 时
// 突然编译不过。
#[cfg(feature = "postgres-compat")]
impl From<postgres::Error> for Error {
    fn from(e: postgres::Error) -> Self {
        Error::Pg(e.to_string())
    }
}

#[cfg(feature = "postgres-compat")]
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Mirror(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

/// 便捷：把字符串错误（比如 `pg_store` 现有 `Result<_, String>` 风格）转成
/// `Error::Config`，方便迁移期与外部 API 桥接。
impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Config(s)
    }
}
