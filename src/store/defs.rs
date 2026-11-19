//! 命名视图定义与放置表。
//!
//! - `grovefs_control.view_defs`：把「lens/alias/fork」的定义以 JSON payload
//!   存在这里；定义行是"这个视图名如何映射到基视图 + 规则"的权威记录。
//! - `grovefs_control.view_placement`：视图 → PG 实例名，`view create` /
//!   `view move` 是唯一改写入口。放置表让"加实例不影响旧视图"，
//!   `resolve_view_home` 的第一优先级。
//!
//! treedb 提供**读写两侧的封装**，接口与 grovefs 现有 `mcp::ControlStore`
//! 的 PG 分支等价，方便接入时替换实现。镜像侧只缓存 defs（低频读、
//! 高频解析）；placement 每次挂载都要读，且 PG 是唯一权威（改一次即生效），
//! 不做本地缓存以免"两处不同步"更难诊断。

use crate::error::{Error, Result};
use crate::persist::Mirror;
use crate::pool::SharedInstancePool;
use postgres::types::ToSql;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ViewDef {
    pub name: String,
    /// "derived" | "alias" | "fork"
    pub kind: String,
    pub payload: String,
    pub updated_at: String,
}

pub struct DefStore {
    pool: Arc<SharedInstancePool>,
    mirror: Option<Arc<Mirror>>,
}

impl DefStore {
    pub fn new(pool: Arc<SharedInstancePool>, mirror: Option<Arc<Mirror>>) -> Result<Self> {
        let this = Self { pool, mirror };
        this.init_schema()?;
        Ok(this)
    }

    fn init_schema(&self) -> Result<()> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        tx.batch("CREATE SCHEMA IF NOT EXISTS grovefs_control")?;
        tx.batch(
            "CREATE TABLE IF NOT EXISTS grovefs_control.view_defs (
                name TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
             )",
        )?;
        tx.batch(
            "CREATE TABLE IF NOT EXISTS grovefs_control.view_placement (
                view TEXT PRIMARY KEY,
                instance TEXT NOT NULL,
                updated_at TEXT NOT NULL
             )",
        )?;
        g.commit()?;
        Ok(())
    }

    pub fn put_def(&self, name: &str, kind: &str, payload: &str) -> Result<()> {
        let ts = crate::persist::now_iso();
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        tx.execute(
            "INSERT INTO grovefs_control.view_defs (name,kind,payload,updated_at)
             VALUES ($1,$2,$3,$4)
             ON CONFLICT (name) DO UPDATE
               SET kind=EXCLUDED.kind, payload=EXCLUDED.payload,
                   updated_at=EXCLUDED.updated_at",
            &[&name, &kind, &payload, &ts],
        )?;
        g.commit()?;
        if let Some(m) = &self.mirror {
            let _ = m.upsert_def(name, kind, payload);
        }
        Ok(())
    }

    pub fn get_def(&self, name: &str) -> Result<Option<ViewDef>> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let row = tx.query_one(
            "SELECT kind, payload, updated_at FROM grovefs_control.view_defs WHERE name=$1",
            &[&name],
        )?;
        let out = row.map(|r| ViewDef {
            name: name.to_string(),
            kind: r.get(0),
            payload: r.get(1),
            updated_at: r.get(2),
        });
        drop(tx);
        let _ = g.commit();
        Ok(out)
    }

    pub fn list_defs(&self) -> Result<Vec<ViewDef>> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let rows = tx.query(
            "SELECT name, kind, payload, updated_at
             FROM grovefs_control.view_defs ORDER BY name",
            &[] as &[&(dyn ToSql + Sync)],
            |r| ViewDef {
                name: r.get(0),
                kind: r.get(1),
                payload: r.get(2),
                updated_at: r.get(3),
            },
        )?;
        drop(tx);
        let _ = g.commit();
        Ok(rows)
    }

    pub fn remove_def(&self, name: &str) -> Result<()> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let n = tx.execute(
            "DELETE FROM grovefs_control.view_defs WHERE name=$1",
            &[&name],
        )?;
        g.commit()?;
        if n == 0 {
            return Err(Error::Denied(format!("no def named '{name}'")));
        }
        Ok(())
    }

    pub fn write_placement(&self, view: &str, instance: &str) -> Result<()> {
        let ts = crate::persist::now_iso();
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        tx.execute(
            "INSERT INTO grovefs_control.view_placement (view,instance,updated_at)
             VALUES ($1,$2,$3)
             ON CONFLICT (view) DO UPDATE
               SET instance=EXCLUDED.instance, updated_at=EXCLUDED.updated_at",
            &[&view, &instance, &ts],
        )?;
        g.commit()?;
        Ok(())
    }

    pub fn lookup_placement(&self, view: &str) -> Result<Option<String>> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let row = tx.query_one(
            "SELECT instance FROM grovefs_control.view_placement WHERE view=$1",
            &[&view],
        )?;
        let out = row.map(|r| r.get::<_, String>(0));
        drop(tx);
        let _ = g.commit();
        Ok(out)
    }

    pub fn forget_placement(&self, view: &str) -> Result<()> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        tx.execute(
            "DELETE FROM grovefs_control.view_placement WHERE view=$1",
            &[&view],
        )?;
        g.commit()?;
        Ok(())
    }

    pub fn all_placements(&self) -> Result<Vec<(String, String)>> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let rows = tx.query(
            "SELECT view, instance FROM grovefs_control.view_placement ORDER BY view",
            &[] as &[&(dyn ToSql + Sync)],
            |r| (r.get::<_, String>(0), r.get::<_, String>(1)),
        )?;
        drop(tx);
        let _ = g.commit();
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_def_debug_cloneable() {
        let d = ViewDef {
            name: "lens".into(),
            kind: "derived".into(),
            payload: "{}".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };
        let c = d.clone();
        assert_eq!(c.kind, "derived");
    }
}
