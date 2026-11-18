#![cfg(feature = "test-peer")]
use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use pi_channel_core::{message_client::MessageClient, secrets::private_write};
use pi_qq::config::{Config as QConfig, Credentials};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn test_binary(variable: &str, name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let target = if target.is_absolute() {
        target
    } else {
        root.join(target)
    };
    let path = std::env::var_os(variable)
        .map(PathBuf::from)
        .unwrap_or_else(|| target.join("debug").join(name));
    ensure!(
        path.is_file(),
        "Missing test binary {}; run ./scripts/test.sh from the workspace root",
        path.display()
    );
    Ok(path)
}
async fn task_count(client: &reqwest::Client, url: &str) -> Result<usize> {
    let tasks: Vec<pi_channel_core::protocol::Task> = client
        .get(url)
        .bearer_auth("native-qq-test-token-at-least-32-characters")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(tasks.len())
}
async fn spawn_runtime(home: &Path, port: u16) -> Result<Child> {
    std::fs::create_dir_all(home.join("sessions"))?;
    // Exercise runtime through its persisted config/RPC contract, not its Rust internals.
    let config = json!({
        "pi": test_binary("PI_RUNTIME_TEST_PEER_BIN", "pi-runtime-test-peer")?,
        "cwd": home, "port": port, "heartbeat_seconds": 1800,
        "task_timeout_seconds": 10, "provider": null, "model": null,
        "thinking": "off", "tools": "read"
    });
    private_write(&home.join("config.json"), &serde_json::to_vec(&config)?)?;
    private_write(
        &home.join("token"),
        b"native-qq-test-token-at-least-32-characters",
    )?;
    Ok(
        Command::new(test_binary("PI_RUNTIME_TEST_BIN", "pi-runtime")?)
            .arg("--home")
            .arg(home)
            .arg("serve")
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?,
    )
}
async fn stop(child: &mut Child) -> Result<()> {
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    ensure!(
        tokio::time::timeout(Duration::from_secs(12), child.wait())
            .await??
            .success(),
        "Child failed shutdown"
    );
    Ok(())
}
async fn runtime_ready(client: &MessageClient) -> Result<()> {
    for _ in 0..100 {
        if client
            .call("runtime.status", json!({}))
            .await
            .is_ok_and(|v| v["status"]["pi_ready"] == true)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("Runtime did not become ready")
}

#[tokio::test]
async fn json_rpc_contract_and_rest_idempotency_are_shared() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let port = port();
    let mut runtime = spawn_runtime(temp.path(), port).await?;
    let url = format!("http://127.0.0.1:{port}/rpc");
    let client = MessageClient::new(url.clone(), &temp.path().join("token"))?;
    runtime_ready(&client).await?;
    let http = reqwest::Client::new();
    let auth = "native-qq-test-token-at-least-32-characters";
    assert_eq!(http.post(&url).json(&json!({})).send().await?.status(), 401);
    let parsed: Value = http
        .post(&url)
        .bearer_auth(auth)
        .body("{bad")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(parsed["error"]["code"], -32700);
    for (request, code) in [
        (
            json!({"jsonrpc":"1.0","id":1,"method":"runtime.status"}),
            -32600,
        ),
        (json!({"jsonrpc":"2.0","id":"a","method":"unknown"}), -32601),
        (
            json!({"jsonrpc":"2.0","id":"a","method":"message.submit","params":{"message":""}}),
            -32602,
        ),
    ] {
        let data: Value = http
            .post(&url)
            .bearer_auth(auth)
            .json(&request)
            .send()
            .await?
            .json()
            .await?;
        assert_eq!(data["error"]["code"], code);
    }
    let id = client.submit("shared", "hello", "shared-key").await?;
    let rest: Value = http
        .post(format!("http://127.0.0.1:{port}/v1/messages"))
        .bearer_auth(auth)
        .json(&json!({"channel":"shared","message":"hello","idempotency_key":"shared-key"}))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(rest["task_id"], id);
    assert!(
        client
            .submit("shared", "changed input", "shared-key")
            .await
            .is_err()
    );
    let notification = http
        .post(&url)
        .bearer_auth(auth)
        .json(&json!({"jsonrpc":"2.0","method":"runtime.status"}))
        .send()
        .await?;
    assert_eq!(notification.status(), 204);
    stop(&mut runtime).await?;
    Ok(())
}

struct Mock {
    ws_url: String,
    tokens: AtomicUsize,
    identifies: AtomicUsize,
    resumes: AtomicUsize,
    sends: Mutex<Vec<Value>>,
}
async fn token(State(mock): State<Arc<Mock>>, Json(body): Json<Value>) -> Json<Value> {
    assert_eq!(body["clientSecret"], "fake-secret");
    let number = mock.tokens.fetch_add(1, Ordering::SeqCst) + 1;
    Json(json!({"access_token":format!("fake-token-{number}"),"expires_in":"7200"}))
}
async fn gateway(State(mock): State<Arc<Mock>>) -> Json<Value> {
    Json(json!({"url":mock.ws_url}))
}
async fn send(
    State(mock): State<Arc<Mock>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert!(body["msg_id"].is_string());
    assert_eq!(body["msg_seq"], 1);
    assert_eq!(body["msg_type"], 0);
    let auth = headers.get("authorization").unwrap().to_str().unwrap();
    let mut sends = mock.sends.lock().unwrap();
    sends.push(body.clone());
    if auth == "QQBot fake-token-1" {
        return (StatusCode::UNAUTHORIZED, Json(json!({"code":401})));
    }
    if body["msg_id"] == "uncertain" {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"code":500})));
    }
    (
        StatusCode::OK,
        Json(json!({"id":format!("sent-{}",sends.len())})),
    )
}
async fn ws_loop(listener: TcpListener, mock: Arc<Mock>) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let mut ws = accept_async(stream).await?;
        ws.send(Message::Text(
            json!({"op":10,"d":{"heartbeat_interval":1000}})
                .to_string()
                .into(),
        ))
        .await?;
        let auth: Value = serde_json::from_str(ws.next().await.unwrap()?.to_text()?)?;
        if auth["op"] == 2 {
            mock.identifies.fetch_add(1, Ordering::SeqCst);
            assert_eq!(auth["d"]["intents"], 1_u64 << 25);
            ws.send(Message::Text(
                json!({"op":0,"t":"READY","s":1,"d":{"session_id":"session-1"}})
                    .to_string()
                    .into(),
            ))
            .await?;
            let events = [
                ("stranger", "denied", 2),
                ("alice", "accepted", 3),
                ("alice", "accepted", 4),
            ];
            for (user, id, seq) in events {
                ws.send(Message::Text(json!({"op":0,"t":"C2C_MESSAGE_CREATE","s":seq,"d":{"id":id,"content":"hello","timestamp":(Utc::now()-chrono::Duration::minutes(30)).to_rfc3339(),"author":{"user_openid":user}}}).to_string().into())).await?;
            }
            ws.send(Message::Text(json!({"op":7,"d":null}).to_string().into()))
                .await?;
            continue;
        } else {
            mock.resumes.fetch_add(1, Ordering::SeqCst);
            assert_eq!(auth["op"], 6);
            assert_eq!(auth["d"]["session_id"], "session-1");
            assert!(auth["d"]["seq"].as_i64().unwrap() >= 4);
            ws.send(Message::Text(
                json!({"op":0,"t":"RESUMED","s":5,"d":{}})
                    .to_string()
                    .into(),
            ))
            .await?;
            // Replayed DM plus group traffic; only exact group+user allowlists may pass.
            ws.send(Message::Text(json!({"op":0,"t":"C2C_MESSAGE_CREATE","s":6,"d":{"id":"accepted","content":"hello","timestamp":Utc::now().to_rfc3339(),"author":{"user_openid":"alice"}}}).to_string().into())).await?;
            for (seq, group, user, id) in [
                (7, "blocked", "alice", "group-denied"),
                (8, "g1", "stranger", "user-denied"),
                (9, "g1", "alice", "group-ok"),
                (10, "g1", "alice", "uncertain"),
            ] {
                ws.send(Message::Text(json!({"op":0,"t":"GROUP_AT_MESSAGE_CREATE","s":seq,"d":{"id":id,"content":"<@!123> group hello","timestamp":Utc::now().to_rfc3339(),"group_openid":group,"author":{"member_openid":user}}}).to_string().into())).await?;
            }
        }
        while let Some(message) = ws.next().await {
            let message = match message {
                Ok(m) => m,
                Err(_) => break,
            };
            if message.is_close() {
                break;
            }
            if message.is_text() {
                let payload: Value = serde_json::from_str(message.to_text()?)?;
                if payload["op"] == 1 {
                    ws.send(Message::Text(json!({"op":11}).to_string().into()))
                        .await?;
                }
            }
        }
    }
}
async fn spawn_qq(home: &Path, base: &str) -> Result<Child> {
    Ok(Command::new(env!("CARGO_BIN_EXE_pi-qq"))
        .arg("--home")
        .arg(home)
        .arg("serve")
        .arg("--test-api-base")
        .arg(base)
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?)
}

#[tokio::test]
async fn native_qq_end_to_end_reconnect_dedup_outbox_and_restart() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let runtime_home = temp.path().join("runtime");
    std::fs::create_dir(&runtime_home)?;
    let runtime_port = port();
    let mut runtime = spawn_runtime(&runtime_home, runtime_port).await?;
    let rpc = MessageClient::new(
        format!("http://127.0.0.1:{runtime_port}/rpc"),
        &runtime_home.join("token"),
    )?;
    runtime_ready(&rpc).await?;
    let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
    let mock = Arc::new(Mock {
        ws_url: format!("ws://{}/", ws_listener.local_addr()?),
        tokens: AtomicUsize::new(0),
        identifies: AtomicUsize::new(0),
        resumes: AtomicUsize::new(0),
        sends: Mutex::new(vec![]),
    });
    let ws_task = tokio::spawn(ws_loop(ws_listener, mock.clone()));
    let api_listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", api_listener.local_addr()?);
    let app = Router::new()
        .route("/token", post(token))
        .route("/gateway", get(gateway))
        .route("/v2/users/{id}/messages", post(send))
        .route("/v2/groups/{id}/messages", post(send))
        .with_state(mock.clone());
    let api_task = tokio::spawn(async move {
        axum::serve(api_listener, app).await.unwrap();
    });
    let qq_home = temp.path().join("qq");
    std::fs::create_dir(&qq_home)?;
    let health_port = port();
    let cfg = QConfig {
        runtime_rpc_url: format!("http://127.0.0.1:{runtime_port}/rpc"),
        runtime_token_file: runtime_home.join("token"),
        credentials_file: qq_home.join("credentials.json"),
        health_port,
        allowed_users: vec!["alice".into()],
        allowed_groups: vec!["g1".into()],
        poll_interval_ms: 500,
        hermes_home: None,
    };
    private_write(&qq_home.join("config.json"), &serde_json::to_vec(&cfg)?)?;
    private_write(
        &qq_home.join("credentials.json"),
        &serde_json::to_vec(&Credentials {
            app_id: format!("{}", rand::random::<u64>()),
            client_secret: "fake-secret".into(),
        })?,
    )?;
    private_write(
        &qq_home.join("token"),
        b"adapter-health-token-at-least-32-characters",
    )?;
    let mut qq = spawn_qq(&qq_home, &base).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let status_url = format!("http://127.0.0.1:{health_port}/v1/status");
    let mut settled = false;
    for _ in 0..200 {
        if let Ok(response) = client
            .get(&status_url)
            .bearer_auth("adapter-health-token-at-least-32-characters")
            .send()
            .await
            && let Ok(status) = response.json::<Value>().await
            && status["deliveries"]["delivered"] == 2
            && status["deliveries"]["uncertain"] == 1
            && status["status"]["qq_ready"] == true
        {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ensure!(
        settled,
        "QQ pipeline did not settle; summaries: {:?}",
        pi_qq::store::Store::open(&qq_home.join("qq.sqlite3"))?.summaries()?
    );
    assert_eq!(client.get(&status_url).send().await?.status(), 401);
    assert_eq!(mock.identifies.load(Ordering::SeqCst), 1);
    assert!(mock.resumes.load(Ordering::SeqCst) >= 1);
    assert!(mock.tokens.load(Ordering::SeqCst) >= 2);
    let sends_before = mock.sends.lock().unwrap().len();
    assert_eq!(sends_before, 4); // one 401 + two successes + one uncertain 5xx
    let task_url = format!("http://127.0.0.1:{runtime_port}/v1/tasks");
    assert_eq!(task_count(&client, &task_url).await?, 3); // no stranger/blocked-group/duplicate calls
    stop(&mut qq).await?;
    qq = spawn_qq(&qq_home, &base).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(mock.sends.lock().unwrap().len(), sends_before);
    assert_eq!(task_count(&client, &task_url).await?, 3);
    assert!(mock.resumes.load(Ordering::SeqCst) >= 2);
    stop(&mut qq).await?;
    stop(&mut runtime).await?;
    ws_task.abort();
    api_task.abort();
    Ok(())
}
