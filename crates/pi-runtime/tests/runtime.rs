#![cfg(feature = "test-peer")]
use anyhow::{Result, bail, ensure};
use chrono::Utc;
use pi_runtime::{
    config::{Config, private_write},
    rpc::Rpc,
    store::Store,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::{Child, Command};

fn setup(home: &Path, port: u16) -> Result<Config> {
    std::fs::create_dir_all(home.join("sessions"))?;
    let config = Config {
        pi: PathBuf::from(env!("CARGO_BIN_EXE_pi-runtime-test-peer")),
        cwd: home.to_path_buf(),
        port,
        heartbeat_seconds: 10,
        task_timeout_seconds: 10,
        provider: None,
        model: None,
        thinking: "off".into(),
        tools: "read".into(),
    };
    private_write(&home.join("config.json"), &serde_json::to_vec(&config)?)?;
    private_write(
        &home.join("token"),
        b"test-token-at-least-32-characters-long",
    )?;
    private_write(
        &home.join("HEARTBEAT.md"),
        b"# Empty heartbeat\n<!-- no model call -->",
    )?;
    Ok(config)
}
async fn spawn(home: &Path) -> Result<Child> {
    Ok(Command::new(env!("CARGO_BIN_EXE_pi-runtime"))
        .arg("--home")
        .arg(home)
        .arg("serve")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?)
}
async fn stop(child: &mut Child) -> Result<()> {
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    let status = tokio::time::timeout(Duration::from_secs(12), child.wait()).await??;
    ensure!(status.success(), "Daemon shutdown failed: {status}");
    Ok(())
}
struct Http {
    client: reqwest::Client,
    base: String,
}
impl Http {
    async fn call(&self, method: &str, path: &str, data: Option<Value>) -> Result<Value> {
        let mut request = self
            .client
            .request(method.parse()?, format!("{}{path}", self.base))
            .bearer_auth("test-token-at-least-32-characters-long");
        if let Some(data) = data {
            request = request.json(&data);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        ensure!(status.is_success(), "{status}: {body}");
        Ok(serde_json::from_str(&body)?)
    }
    async fn ready(&self) -> Result<()> {
        for _ in 0..100 {
            if self.call("GET", "/ready", None).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        bail!("Daemon not ready")
    }
    async fn send(&self, message: &str, channel: &str) -> Result<String> {
        Ok(self
            .call(
                "POST",
                "/v1/messages",
                Some(json!({"message":message,"channel":channel})),
            )
            .await?["task_id"]
            .as_str()
            .unwrap()
            .to_owned())
    }
    async fn wait(&self, id: &str) -> Result<Value> {
        for _ in 0..150 {
            let task = self.call("GET", &format!("/v1/tasks/{id}"), None).await?;
            if !matches!(task["status"].as_str(), Some("queued" | "running")) {
                return Ok(task);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        bail!("Task did not settle: {id}")
    }
}

#[tokio::test]
async fn rpc_handles_early_events_errors_and_session_isolation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = setup(dir.path(), 8787)?;
    let store = Store::open(&dir.path().join("db"))?;
    let mut rpc = Rpc::start(&config, dir.path()).await?;
    for (message, channel, expected) in [
        ("你好\u{2028}world", "a", "1:你好\u{2028}world"),
        ("other", "b", "1:other"),
        ("again", "a", "2:again"),
        (
            "/handled",
            "a",
            "Prompt handled without starting an agent run.",
        ),
    ] {
        store.enqueue(channel, "message", message, None, false)?;
        let task = store.claim()?.unwrap();
        assert_eq!(rpc.run(&task, &store).await?, expected);
        store.finish(&task.id, "succeeded", Some(expected), None)?;
    }
    for message in ["error", "reject", "dialog"] {
        store.enqueue("a", "message", message, None, false)?;
        let task = store.claim()?.unwrap();
        assert!(rpc.run(&task, &store).await.is_err());
        store.finish(&task.id, "failed", None, Some("expected"))?;
    }
    rpc.stop().await;
    Ok(())
}

#[tokio::test]
async fn http_scheduling_heartbeat_and_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = socket.local_addr()?.port();
    drop(socket);
    setup(dir.path(), port)?;
    let http = Http {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()?,
        base: format!("http://127.0.0.1:{port}"),
    };
    let mut daemon = spawn(dir.path()).await?;
    http.ready().await?;
    assert_eq!(
        http.client
            .get(format!("{}/v1/status", http.base))
            .send()
            .await?
            .status(),
        401
    );
    assert_eq!(
        http.client
            .get(format!("{}/v1/status", http.base))
            .bearer_auth("test-token-at-least-32-characters-long")
            .header("origin", "http://evil.example")
            .send()
            .await?
            .status(),
        403
    );
    assert!(
        http.call("POST", "/v1/messages", Some(json!({"message":""})))
            .await
            .is_err()
    );
    assert!(
        http.call(
            "POST",
            "/v1/jobs",
            Some(json!({"name":"bad", "message":"x", "timing":{"kind":"every", "seconds":1}}))
        )
        .await
        .is_err()
    );
    let payload = json!({"message":"hello", "idempotency_key":"same"});
    let a = http
        .call("POST", "/v1/messages", Some(payload.clone()))
        .await?;
    let b = http.call("POST", "/v1/messages", Some(payload)).await?;
    assert_eq!(a, b);
    let task = http.wait(a["task_id"].as_str().unwrap()).await?;
    assert_eq!(task["status"], "succeeded");
    assert_eq!(task["result"], "1:hello");
    let error = http.send("error", "errors").await?;
    assert_eq!(http.wait(&error).await?["status"], "failed");
    let job = http.call("POST", "/v1/jobs", Some(json!({"name":"once", "message":"scheduled", "timing":{"kind":"at", "at":(Utc::now()+chrono::Duration::seconds(2)).to_rfc3339()}}))).await?;
    let mut found = false;
    for _ in 0..60 {
        let tasks = http.call("GET", "/v1/tasks", None).await?;
        if tasks.as_array().unwrap().iter().any(|task| {
            task["source"] == format!("job:{}", job["id"].as_str().unwrap())
                && task["status"] == "succeeded"
        }) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    ensure!(found, "One-shot job did not execute");
    assert_eq!(
        http.call("GET", "/v1/jobs", None).await?[0]["enabled"],
        false
    );
    let all = http.call("GET", "/v1/tasks", None).await?;
    assert!(
        !all.as_array()
            .unwrap()
            .iter()
            .any(|task| task["source"] == "heartbeat")
    );
    std::fs::write(
        dir.path().join("HEARTBEAT.md"),
        "# Heartbeat\n- [ ] report test status",
    )?;
    let mut heartbeat = false;
    for _ in 0..90 {
        let tasks = http.call("GET", "/v1/tasks", None).await?;
        if tasks
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["source"] == "heartbeat" && task["status"] == "succeeded")
        {
            heartbeat = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    ensure!(heartbeat, "Heartbeat did not execute");
    std::fs::write(dir.path().join("HEARTBEAT.md"), "# Empty")?;
    let slow = http.send("slow", "slow").await?;
    for _ in 0..50 {
        if http.call("GET", &format!("/v1/tasks/{slow}"), None).await?["status"] == "running" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let pending = http.send("after restart", "main").await?;
    stop(&mut daemon).await?;
    daemon = spawn(dir.path()).await?;
    http.ready().await?;
    assert_eq!(http.wait(&slow).await?["status"], "interrupted");
    let recovered = http.wait(&pending).await?;
    assert_eq!(recovered["status"], "succeeded");
    assert_eq!(recovered["result"], "2:after restart");
    // A stuck agent must time out without making the HTTP listener unresponsive.
    let timed_out = http.send("slow", "timeout").await?;
    assert_eq!(http.wait(&timed_out).await?["status"], "failed");
    let after_timeout = http.send("healthy again", "after-timeout").await?;
    assert_eq!(
        http.wait(&after_timeout).await?["result"],
        "1:healthy again"
    );
    // Exclusive daemon lock prevents duplicate schedulers.
    let mut duplicate = spawn(dir.path()).await?;
    assert!(
        !tokio::time::timeout(Duration::from_secs(3), duplicate.wait())
            .await??
            .success()
    );
    stop(&mut daemon).await?;
    Ok(())
}
