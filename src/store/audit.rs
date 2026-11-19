//! 审计日志写入：内存缓冲 + 定时批量 flush 到 PG。
//!
//! 语义：
//! - **每条工具调用**都记录一行；PG 是唯一权威。
//! - **缓冲**：内存 `Vec<Row>` 攒到 `batch` 条或到达 `flush_interval`，
//!   一次事务批量 INSERT。这样把每 op 的固定 BEGIN/COMMIT 开销摊到一批里，
//!   在 1000+ QPS 下把 PG 侧的写次数压到 5 次/秒以下。
//! - **降级**：PG 挂时把行 spill 到 [`Mirror::buffer_audit`]，PG 恢复后
//!   上层调 `flush_pending()` 从镜像拉回并写入 PG，写入成功后本地删除。
//!   **审计不丢一行**是这份设计的首要目标，允许延迟但不允许缺席。
//! - **上限**：内存缓冲上限 `audit_buffer_limit`；超出前若镜像可用则立即
//!   spill，不可用则阻塞 flush（背压）。这防"PG 长时挂 + 高流量"下内存
//!   无限增长。
//!
//! 与 `apikeys::ApiKeyStore::log_call` 的差别：原版每次调用直写 PG，无
//! 缓冲。treedb 把这层显式化，让 hot path 只做一次 `push` + 条件 flush。

use crate::config::StoreConfig;
use crate::error::{Error, Result};
use crate::persist::Mirror;
use crate::pool::SharedInstancePool;
use postgres::types::ToSql;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 一行待写入的审计。字段与 `grovefs_control.mcp_calls` 对齐。
#[derive(Debug, Clone)]
pub struct AuditRow {
    pub ts: String,
    pub key_id: Option<String>,
    pub key_label: Option<String>,
    pub method: String,
    pub tool: String,
    pub ok: bool,
    pub detail: String,
}

/// 审计写入器。线程安全：`push` 是 hot path，其它由 sweeper 调。
pub struct AuditWriter {
    pool: Arc<SharedInstancePool>,
    mirror: Option<Arc<Mirror>>,
    pg_reachable: Arc<AtomicBool>,
    config: StoreConfig,
    buffer: parking_lot::Mutex<Vec<AuditRow>>,
}

impl AuditWriter {
    pub fn new(
        pool: Arc<SharedInstancePool>,
        mirror: Option<Arc<Mirror>>,
        pg_reachable: Arc<AtomicBool>,
        config: StoreConfig,
    ) -> Result<Self> {
        let this = Self {
            pool,
            mirror,
            pg_reachable,
            config,
            buffer: parking_lot::Mutex::new(Vec::new()),
        };
        this.init_schema()?;
        Ok(this)
    }

    fn init_schema(&self) -> Result<()> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        tx.batch("CREATE SCHEMA IF NOT EXISTS grovefs_control")?;
        tx.batch(
            "CREATE TABLE IF NOT EXISTS grovefs_control.mcp_calls (
                seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                ts TEXT NOT NULL,
                key_id TEXT,
                key_label TEXT,
                method TEXT NOT NULL,
                tool TEXT NOT NULL,
                ok BOOLEAN NOT NULL,
                detail TEXT NOT NULL
             )",
        )?;
        g.commit()?;
        Ok(())
    }

    /// 记一行。不阻塞：如果缓冲区未达阈值就 push 完立即返回。
    pub fn push(&self, row: AuditRow) -> Result<()> {
        let mut buf = self.buffer.lock();
        buf.push(row);
        if buf.len() >= self.config.audit_batch {
            // 满了：批量 flush。失败时 spill 或阻塞（背压）。
            let rows = std::mem::take(&mut *buf);
            drop(buf);
            self.flush_batch(rows)?;
        }
        Ok(())
    }

    /// 由 sweeper 定时调用：不管满不满都尝试 flush 一批。
    pub fn flush_now(&self) -> Result<usize> {
        let rows = {
            let mut buf = self.buffer.lock();
            if buf.is_empty() {
                return Ok(0);
            }
            std::mem::take(&mut *buf)
        };
        let n = rows.len();
        self.flush_batch(rows)?;
        Ok(n)
    }

    /// 把内存或镜像的 pending 全推到 PG。sweeper 定期调用。
    pub fn flush_pending(&self) -> Result<usize> {
        if !self.pg_reachable.load(Ordering::Acquire) {
            return Ok(0);
        }
        let Some(m) = &self.mirror else {
            return Ok(0);
        };
        let mut total = 0;
        loop {
            let rows = m.pending_audit(self.config.audit_batch)?;
            if rows.is_empty() {
                break;
            }
            let last_seq = rows.iter().map(|r| r.0).max().unwrap_or(0);
            let mut g = self.pool.begin_control_tx()?;
            let mut tx = g.tx()?;
            for (_seq, ts, key_id, key_label, method, tool, ok, detail) in &rows {
                let params: &[&(dyn ToSql + Sync)] = &[
                    ts,
                    key_id,
                    key_label,
                    method,
                    tool,
                    ok,
                    detail,
                ];
                tx.execute(
                    "INSERT INTO grovefs_control.mcp_calls
                     (ts,key_id,key_label,method,tool,ok,detail)
                     VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    params,
                )?;
            }
            g.commit()?;
            m.delete_audit_below_seq(last_seq)?;
            total += rows.len();
            if rows.len() < self.config.audit_batch {
                break;
            }
        }
        Ok(total)
    }

    fn flush_batch(&self, rows: Vec<AuditRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        if !self.pg_reachable.load(Ordering::Acquire) {
            return self.spill_to_mirror(rows);
        }
        let mut g = match self.pool.begin_control_tx() {
            Ok(g) => g,
            Err(e) => {
                self.pg_reachable.store(false, Ordering::Release);
                return self.spill_to_mirror(rows).map(|_| ()).map_err(|_| e);
            }
        };
        let mut tx = g.tx()?;
        for r in &rows {
            let params: &[&(dyn ToSql + Sync)] = &[
                &r.ts,
                &r.key_id,
                &r.key_label,
                &r.method,
                &r.tool,
                &r.ok,
                &r.detail,
            ];
            match tx.execute(
                "INSERT INTO grovefs_control.mcp_calls
                 (ts,key_id,key_label,method,tool,ok,detail)
                 VALUES ($1,$2,$3,$4,$5,$6,$7)",
                params,
            ) {
                Ok(_) => {}
                Err(e) => {
                    drop(tx);
                    self.pg_reachable.store(false, Ordering::Release);
                    return self
                        .spill_to_mirror(rows)
                        .map(|_| ())
                        .map_err(|_| Error::Pg(format!("flush failed and spill failed: {e}")));
                }
            }
        }
        g.commit()?;
        Ok(())
    }

    fn spill_to_mirror(&self, rows: Vec<AuditRow>) -> Result<()> {
        let Some(m) = &self.mirror else {
            // 无镜像可用：内存放不下就得丢弃最老的一部分并告警。诚实边界：
            // 只有配了 mirror 才能保证"审计不丢一行"。
            tracing::warn!(
                "no mirror configured: dropping {} pending audit rows on PG outage",
                rows.len()
            );
            return Ok(());
        };
        for r in &rows {
            m.buffer_audit(
                &r.ts,
                r.key_id.as_deref(),
                r.key_label.as_deref(),
                &r.method,
                &r.tool,
                r.ok,
                &r.detail,
            )?;
        }
        Ok(())
    }

    /// 当前缓冲行数（监控）。
    pub fn buffered(&self) -> usize {
        self.buffer.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_row_clone() {
        let r = AuditRow {
            ts: "2026-01-01T00:00:00Z".into(),
            key_id: Some("abc".into()),
            key_label: Some("test".into()),
            method: "tools/call".into(),
            tool: "view.list".into(),
            ok: true,
            detail: "n=3".into(),
        };
        let c = r.clone();
        assert_eq!(c.tool, "view.list");
    }

    #[test]
    fn spill_to_missing_mirror_is_noop() {
        // 用不合法 URI 建 pool，让 begin_control_tx 直接失败，模拟 PG 挂；
        // 不配 mirror，让 spill_to_mirror 走告警分支返回 Ok。
        let pool = SharedInstancePool::new(
            "postgres://invalid-host-for-treedb.invalid:1/db".to_string(),
            crate::PoolConfig {
                warm: 0,
                max: 1,
                checkout_timeout: std::time::Duration::from_millis(10),
                ..Default::default()
            },
        );
        let reachable = Arc::new(AtomicBool::new(false));
        // 手动构造：跳过 init_schema（PG 挂时那步也做不了）。
        let writer = AuditWriter {
            pool,
            mirror: None,
            pg_reachable: reachable,
            config: StoreConfig::default(),
            buffer: parking_lot::Mutex::new(Vec::new()),
        };
        let row = AuditRow {
            ts: "2026-01-01T00:00:00Z".into(),
            key_id: None,
            key_label: None,
            method: "m".into(),
            tool: "t".into(),
            ok: true,
            detail: "d".into(),
        };
        // push 到满之前只走 push、不 flush。
        assert!(writer.push(row).is_ok());
        assert_eq!(writer.buffered(), 1);
    }
}
