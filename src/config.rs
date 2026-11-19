//! 全局配置：池/镜像/审计三档参数。
//!
//! 所有默认值都对应设计文档 §3.2 的实测点：
//! - `warm=16 / max=64`：万级视图单实例共享池，配合 `max_connections=200`
//!   的 PG 部署，留出 136 条给运维/其他服务；池内 SSI 并发写最多 64。
//! - `idle_ttl=60s`：与 `pg_store::PG_POOL_IDLE_SECS` 保持一致，突发过后再
//!   回落，避免反复拨号+tune+prepared 重解析。
//! - `sweep_interval=10s`：reap 与探活的合并节拍。
//! - `audit_batch=200`：单次 flush 的行数上限，够小保证延迟、够大 amortize
//!   PG 的 BEGIN/COMMIT 固定开销。

use std::time::Duration;

/// 共享池一侧的连接池参数。
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// 暖地板：池创建时预拨并永不回收。
    pub warm: usize,
    /// 保留上限：突发借出超过 warm 时最多能扩到 max。
    pub max: usize,
    /// 空闲多久后一条 warm 之上的连接被回收。
    pub idle_ttl: Duration,
    /// 借出等待超时（超出 max 时）；0 表示立即失败不排队。
    pub checkout_timeout: Duration,
    /// 探活/reap 的合并节拍。
    pub sweep_interval: Duration,
    /// 单次操作语句超时。
    pub statement_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            warm: 16,
            max: 64,
            idle_ttl: Duration::from_secs(60),
            checkout_timeout: Duration::from_millis(500),
            sweep_interval: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(30),
        }
    }
}

/// 控制面 store 的整体配置。
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// 共享池参数。
    pub pool: PoolConfig,
    /// 本地 SQLite 镜像目录。
    pub mirror_dir: std::path::PathBuf,
    /// 是否在 PG 写入成功后 write-through 到镜像（默认 true）。
    pub write_through: bool,
    /// 是否在 PG 不可达时允许走镜像做认证（默认 true）。
    pub fallback_enabled: bool,
    /// 审计缓冲行数上限，超出后强制 flush 或落镜像。
    pub audit_buffer_limit: usize,
    /// 单次 audit flush 批量大小。
    pub audit_batch: usize,
    /// 快照间隔（0 禁用）。
    pub snapshot_interval: Duration,
}

impl StoreConfig {
    /// 以镜像目录为默认根构造。PG 后端的 meta-server 常用形态。
    pub fn with_mirror_dir(dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            mirror_dir: dir.into(),
            ..Default::default()
        }
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            pool: PoolConfig::default(),
            mirror_dir: std::path::PathBuf::from("treedb_mirror"),
            write_through: true,
            fallback_enabled: true,
            audit_buffer_limit: 10_000,
            audit_batch: 200,
            snapshot_interval: Duration::from_secs(300),
        }
    }
}

/// `pg_store` 现有环境变量约定（`GROVEFS_PG_*`）在 treedb 里的对应读法：
/// 相同变量名、相同范围，方便迁移期两套实现看同一份配置。
pub fn pool_from_env(default: PoolConfig) -> PoolConfig {
    let read_usize = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<usize>().ok());
    let read_u64 = |k: &str| std::env::var(k).ok().and_then(|s| s.parse::<u64>().ok());
    let warm = read_usize("GROVEFS_PG_WRITE_POOL").unwrap_or(default.warm);
    let max = read_usize("GROVEFS_PG_WRITE_POOL_MAX").unwrap_or(default.max);
    let ttl_secs = read_u64("GROVEFS_PG_POOL_IDLE_SECS").unwrap_or(60u64);
    PoolConfig {
        warm: warm.min(64),
        max: max.max(warm).min(64),
        idle_ttl: Duration::from_secs(ttl_secs),
        ..default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_constrained() {
        let d = PoolConfig::default();
        assert!(d.warm <= d.max);
        assert!(d.warm > 0);
        assert!(d.max >= 16);
    }

    #[test]
    fn store_default_matches_design() {
        let s = StoreConfig::default();
        assert_eq!(s.audit_batch, 200);
        assert!(s.write_through);
        assert!(s.fallback_enabled);
    }

    #[test]
    fn pool_from_env_clamps_to_cap() {
        std::env::set_var("GROVEFS_PG_WRITE_POOL", "200");
        std::env::set_var("GROVEFS_PG_WRITE_POOL_MAX", "10");
        let p = pool_from_env(PoolConfig::default());
        assert_eq!(p.warm, 64, "warm must clamp to POOL_LANE_CAP");
        assert!(p.max >= p.warm, "max must be >= warm after resolve");
        std::env::remove_var("GROVEFS_PG_WRITE_POOL");
        std::env::remove_var("GROVEFS_PG_WRITE_POOL_MAX");
    }
}
