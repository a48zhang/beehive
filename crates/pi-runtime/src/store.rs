use crate::schedule::Timing;
use anyhow::{Result, ensure};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Mutex};

pub use pi_channel_core::{id, protocol::Task};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub message: String,
    pub timing: Timing,
    pub next_run: Option<i64>,
    pub enabled: bool,
}

pub struct Store(Mutex<Connection>);

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(version <= 1, "Database schema is newer than this binary");
        if version == 0 {
            db.execute_batch("BEGIN;
                CREATE TABLE tasks(id TEXT PRIMARY KEY, channel TEXT NOT NULL, source TEXT NOT NULL,
                    message TEXT NOT NULL, status TEXT NOT NULL, result TEXT, error TEXT,
                    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, request_key TEXT UNIQUE);
                CREATE INDEX task_queue ON tasks(status, created_at);
                CREATE TABLE jobs(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE sessions(channel TEXT PRIMARY KEY, path TEXT NOT NULL);
                CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                PRAGMA user_version=1; COMMIT;")?;
        }
        Ok(Self(Mutex::new(db)))
    }

    pub fn recover(&self) -> Result<usize> {
        Ok(self.0.lock().unwrap().execute("UPDATE tasks SET status='interrupted', error='Runtime stopped during execution; not automatically replayed', updated_at=? WHERE status='running'", [Utc::now().timestamp()])?)
    }

    pub fn enqueue(
        &self,
        channel: &str,
        source: &str,
        message: &str,
        request_key: Option<&str>,
        coalesce: bool,
    ) -> Result<String> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        if let Some(key) = request_key
            && let Some((existing, old_channel, old_message)) = tx
                .query_row(
                    "SELECT id,channel,message FROM tasks WHERE request_key=?",
                    [key],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?
        {
            ensure!(
                channel == old_channel && message == old_message,
                "Idempotency key already used with different input"
            );
            return Ok(existing);
        }
        if coalesce
            && let Some(existing) = tx.query_row("SELECT id FROM tasks WHERE source=? AND status IN ('queued','running') LIMIT 1", [source], |r| r.get::<_, String>(0)).optional()? { return Ok(existing); }

        let count: i64 = tx.query_row(
            "SELECT count(*) FROM tasks WHERE status IN ('queued','running')",
            [],
            |r| r.get(0),
        )?;
        ensure!(count < 1000, "Queue full");
        let task = id();
        let now = Utc::now().timestamp();
        tx.execute(
            "INSERT INTO tasks VALUES(?,?,?,?,'queued',NULL,NULL,?,?,?)",
            params![task, channel, source, message, now, now, request_key],
        )?;
        tx.commit()?;
        Ok(task)
    }

    fn task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
        Ok(Task {
            id: row.get(0)?,
            channel: row.get(1)?,
            source: row.get(2)?,
            message: row.get(3)?,
            status: row.get(4)?,
            result: row.get(5)?,
            error: row.get(6)?,
            created_at: row.get(7)?,
            updated_at: row.get(8)?,
        })
    }

    pub fn claim(&self) -> Result<Option<Task>> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        let task = tx.query_row("SELECT * FROM tasks WHERE status='queued' ORDER BY CASE source WHEN 'message' THEN 0 WHEN 'heartbeat' THEN 2 ELSE 1 END, created_at, rowid LIMIT 1", [], Self::task_row).optional()?;
        if let Some(task) = &task {
            tx.execute(
                "UPDATE tasks SET status='running',updated_at=? WHERE id=?",
                params![Utc::now().timestamp(), task.id],
            )?;
        }
        tx.commit()?;
        Ok(task)
    }

    pub fn finish(
        &self,
        id: &str,
        status: &str,
        result: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        self.0.lock().unwrap().execute(
            "UPDATE tasks SET status=?,result=?,error=?,updated_at=? WHERE id=?",
            params![status, result, error, Utc::now().timestamp(), id],
        )?;
        Ok(())
    }

    pub fn task(&self, id: &str) -> Result<Option<Task>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .query_row("SELECT * FROM tasks WHERE id=?", [id], Self::task_row)
            .optional()?)
    }

    pub fn tasks(&self) -> Result<Vec<Task>> {
        let db = self.0.lock().unwrap();
        Ok(db
            .prepare("SELECT * FROM tasks ORDER BY created_at DESC, rowid DESC LIMIT 100")?
            .query_map([], Self::task_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn cancel_queued(&self, id: &str) -> Result<bool> {
        Ok(self.0.lock().unwrap().execute(
            "UPDATE tasks SET status='cancelled',updated_at=? WHERE id=? AND status='queued'",
            params![Utc::now().timestamp(), id],
        )? > 0)
    }

    pub fn jobs(&self) -> Result<Vec<Job>> {
        let db = self.0.lock().unwrap();
        let rows = db
            .prepare("SELECT payload FROM jobs ORDER BY rowid")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }

    pub fn add_job(&self, job: &Job) -> Result<()> {
        let db = self.0.lock().unwrap();
        let count: i64 = db.query_row("SELECT count(*) FROM jobs", [], |r| r.get(0))?;
        ensure!(count < 100, "At most 100 jobs are allowed");
        db.execute(
            "INSERT INTO jobs VALUES(?,?)",
            params![job.id, serde_json::to_string(job)?],
        )?;
        Ok(())
    }

    pub fn delete_job(&self, id: &str) -> Result<bool> {
        let db = self.0.lock().unwrap();
        let deleted = db.execute("DELETE FROM jobs WHERE id=?", [id])? > 0;
        if deleted {
            db.execute(
                "DELETE FROM sessions WHERE channel=?",
                [format!("job:{id}")],
            )?;
        }
        Ok(deleted)
    }

    // Claim a due occurrence and persist its successor atomically with enqueueing.
    pub fn dispatch_job(&self, previous: &Job, next: &Job) -> Result<()> {
        let mut db = self.0.lock().unwrap();
        let tx = db.transaction()?;
        let stored = tx
            .query_row("SELECT payload FROM jobs WHERE id=?", [&previous.id], |r| {
                r.get::<_, String>(0)
            })
            .optional()?;
        if stored.as_deref() != Some(&serde_json::to_string(previous)?) {
            return Ok(());
        }
        let source = format!("job:{}", previous.id);
        let active: i64 = tx.query_row(
            "SELECT count(*) FROM tasks WHERE source=? AND status IN ('queued','running')",
            [&source],
            |r| r.get(0),
        )?;
        let queued: i64 = tx.query_row(
            "SELECT count(*) FROM tasks WHERE status IN ('queued','running')",
            [],
            |r| r.get(0),
        )?;
        if active == 0 && queued < 1000 {
            let now = Utc::now().timestamp();
            tx.execute(
                "INSERT INTO tasks VALUES(?,?,?,?,'queued',NULL,NULL,?,?,NULL)",
                params![id(), source, source, previous.message, now, now],
            )?;
        }
        tx.execute(
            "UPDATE jobs SET payload=? WHERE id=?",
            params![serde_json::to_string(next)?, next.id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn session(&self, channel: &str) -> Result<Option<String>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .query_row(
                "SELECT path FROM sessions WHERE channel=?",
                [channel],
                |r| r.get(0),
            )
            .optional()?)
    }

    pub fn save_session(&self, channel: &str, path: &str) -> Result<()> {
        let db = self.0.lock().unwrap();
        db.execute("DELETE FROM sessions WHERE channel=?", [channel])?;
        db.execute(
            "DELETE FROM sessions WHERE rowid IN (SELECT rowid FROM sessions ORDER BY rowid DESC LIMIT -1 OFFSET 255)",
            [],
        )?;
        db.execute("INSERT INTO sessions VALUES(?,?)", params![channel, path])?;
        Ok(())
    }

    pub fn metadata(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .query_row("SELECT value FROM metadata WHERE key=?", [key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queue_recovery_and_idempotency() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = Store::open(&dir.path().join("test.db"))?;
        let a = db.enqueue("main", "message", "hello", Some("key"), false)?;
        assert_eq!(
            a,
            db.enqueue("main", "message", "hello", Some("key"), false)?
        );
        assert!(
            db.enqueue("other", "message", "hello", Some("key"), false)
                .is_err()
        );
        assert_eq!(db.claim()?.unwrap().id, a);
        assert_eq!(db.recover()?, 1);
        assert!(db.claim()?.is_none());
        assert_eq!(db.task(&a)?.unwrap().status, "interrupted");
        Ok(())
    }
    #[test]
    fn scheduled_occurrence_is_atomic_and_coalesced() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let db = Store::open(&dir.path().join("test.db"))?;
        let mut job = Job {
            id: id(),
            name: "test".into(),
            message: "check".into(),
            timing: Timing::Every { seconds: 10 },
            next_run: Some(0),
            enabled: true,
        };
        db.add_job(&job)?;
        db.save_session(&format!("job:{}", job.id), "/tmp/job.jsonl")?;
        let old = job.clone();
        job.next_run = Some(10);
        db.dispatch_job(&old, &job)?;
        db.dispatch_job(&old, &job)?;
        assert_eq!(db.tasks()?.len(), 1);
        let old = job.clone();
        job.next_run = Some(20);
        db.dispatch_job(&old, &job)?;
        assert_eq!(db.tasks()?.len(), 1);
        assert_eq!(db.jobs()?[0].next_run, Some(20));
        assert!(db.delete_job(&job.id)?);
        assert!(db.session(&format!("job:{}", job.id))?.is_none());
        Ok(())
    }
}
