use super::protocol::Inbound;
use anyhow::{Result, ensure};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, sync::Mutex};

#[derive(Clone, Serialize, Deserialize)]
pub struct Delivery {
    pub event: Inbound,
    pub task_id: Option<String>,
    pub phase: String,
    pub chunks: Vec<String>,
    pub sent: usize,
    pub attempts: u32,
    pub next_attempt: i64,
    pub last_error: Option<String>,
}
pub struct Store(Mutex<Connection>);
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(version <= 1, "QQ database schema is newer than this binary");
        if version == 0 {
            db.execute_batch("BEGIN;
                CREATE TABLE deliveries(key TEXT PRIMARY KEY, phase TEXT NOT NULL, next_attempt INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX work ON deliveries(phase,next_attempt);
                CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                PRAGMA user_version=1; COMMIT;")?;
        }
        Ok(Self(Mutex::new(db)))
    }
    pub fn insert(&self, event: Inbound) -> Result<bool> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        if tx
            .query_row("SELECT 1 FROM deliveries WHERE key=?", [&event.key], |_| {
                Ok(())
            })
            .optional()?
            .is_some()
        {
            return Ok(false);
        }
        let count: i64 = tx.query_row("SELECT count(*) FROM deliveries WHERE phase IN ('pending','submitted','reply','sending')", [], |r| r.get(0))?;
        ensure!(count < 1000, "QQ inbox full (1000 active events)");
        let delivery = Delivery {
            event,
            task_id: None,
            phase: "pending".into(),
            chunks: vec![],
            sent: 0,
            attempts: 0,
            next_attempt: Utc::now().timestamp(),
            last_error: None,
        };
        tx.execute(
            "INSERT INTO deliveries VALUES(?,?,?,?)",
            params![
                delivery.event.key,
                delivery.phase,
                delivery.next_attempt,
                serde_json::to_string(&delivery)?
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }
    pub fn save(&self, delivery: &Delivery) -> Result<()> {
        self.0.lock().unwrap().execute(
            "UPDATE deliveries SET phase=?,next_attempt=?,payload=? WHERE key=?",
            params![
                delivery.phase,
                delivery.next_attempt,
                serde_json::to_string(delivery)?,
                delivery.event.key
            ],
        )?;
        Ok(())
    }
    pub fn due(&self, limit: u32) -> Result<Vec<Delivery>> {
        let db = self.0.lock().unwrap();
        let rows = db.prepare("SELECT payload FROM deliveries WHERE phase IN ('pending','submitted','reply') AND next_attempt<=? ORDER BY next_attempt,rowid LIMIT ?")?
            .query_map(params![Utc::now().timestamp(),limit], |r| r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }
    pub fn recover(&self) -> Result<usize> {
        let rows = {
            let db = self.0.lock().unwrap();
            db.prepare("SELECT payload FROM deliveries WHERE phase='sending'")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let count = rows.len();
        for row in rows {
            let mut delivery: Delivery = serde_json::from_str(&row)?;
            delivery.phase = "uncertain".into();
            delivery.last_error =
                Some("Service stopped during QQ send; not automatically replayed".into());
            self.save(&delivery)?;
        }
        Ok(count)
    }
    pub fn counts(&self) -> Result<BTreeMap<String, i64>> {
        let db = self.0.lock().unwrap();
        Ok(db
            .prepare("SELECT phase,count(*) FROM deliveries GROUP BY phase")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn summaries(&self) -> Result<Vec<serde_json::Value>> {
        let db = self.0.lock().unwrap();
        let rows = db
            .prepare("SELECT payload FROM deliveries ORDER BY rowid DESC LIMIT 100")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter().map(|s| {
            let d: Delivery = serde_json::from_str(&s)?;
            Ok(serde_json::json!({"event_key":d.event.key,"task_id":d.task_id,"phase":d.phase,"sent_chunks":d.sent,"total_chunks":d.chunks.len(),"attempts":d.attempts,"last_error":d.last_error}))
        }).collect()
    }
    pub fn retry(&self, key: &str) -> Result<()> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        let row: String =
            tx.query_row("SELECT payload FROM deliveries WHERE key=?", [key], |r| {
                r.get(0)
            })?;
        let mut delivery: Delivery = serde_json::from_str(&row)?;
        ensure!(
            matches!(delivery.phase.as_str(), "uncertain" | "dead"),
            "Only uncertain/dead events can be retried"
        );
        delivery.phase = if !delivery.chunks.is_empty() {
            "reply"
        } else if delivery.task_id.is_some() {
            "submitted"
        } else {
            "pending"
        }
        .into();
        delivery.attempts = 0;
        delivery.next_attempt = Utc::now().timestamp();
        delivery.last_error = None;
        tx.execute(
            "UPDATE deliveries SET phase=?,next_attempt=?,payload=? WHERE key=?",
            params![
                delivery.phase,
                delivery.next_attempt,
                serde_json::to_string(&delivery)?,
                key
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .query_row("SELECT value FROM metadata WHERE key=?", [key], |r| {
                r.get(0)
            })
            .optional()?)
    }
    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )?;
        Ok(())
    }
    pub fn clear_session(&self) -> Result<()> {
        self.0.lock().unwrap().execute(
            "DELETE FROM metadata WHERE key IN ('session_id','seq','ws_session')",
            [],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_dedup_and_uncertain_send_recovery() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("qq.db"))?;
        let event = Inbound {
            key: "qq:key".into(),
            channel: "qq:channel".into(),
            kind: "c2c".into(),
            target: "u".into(),
            user: "u".into(),
            message_id: "m".into(),
            text: "hello".into(),
            timestamp: Utc::now().timestamp(),
        };
        assert!(store.insert(event.clone())?);
        assert!(!store.insert(event)?);
        let mut delivery = store.due(10)?.remove(0);
        delivery.phase = "sending".into();
        store.save(&delivery)?;
        assert_eq!(store.recover()?, 1);
        assert!(store.due(10)?.is_empty());
        assert_eq!(store.counts()?["uncertain"], 1);
        store.retry("qq:key")?;
        assert_eq!(store.due(10)?[0].phase, "pending");
        Ok(())
    }
}
