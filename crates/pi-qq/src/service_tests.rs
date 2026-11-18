use super::*;
use crate::{config::Credentials, protocol::Inbound};
use pi_channel_core::secrets::private_write;

fn fixture() -> Result<(tempfile::TempDir, State, Delivery)> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        runtime_rpc_url: "http://127.0.0.1:1/rpc".into(),
        runtime_token_file: dir.path().join("token"),
        credentials_file: dir.path().join("credentials.json"),
        health_port: 8788,
        allowed_users: vec!["allowed".into()],
        allowed_groups: vec![],
        poll_interval_ms: 1000,
        hermes_home: None,
    };
    private_write(
        &config.runtime_token_file,
        b"test-token-at-least-32-characters-long",
    )?;
    let store = Store::open(&dir.path().join("qq.db"))?;
    store.insert(Inbound {
        key: "qq:old-event".into(),
        channel: "qq:channel".into(),
        kind: "c2c".into(),
        target: "allowed".into(),
        user: "allowed".into(),
        message_id: "old-message".into(),
        text: "long running task".into(),
        timestamp: Utc::now().timestamp() - 86400,
    })?;
    let mut delivery = store.due(1)?.remove(0);
    delivery.phase = "submitted".into();
    delivery.task_id = Some("a".repeat(32));
    store.save(&delivery)?;
    let runtime = MessageClient::new(config.runtime_rpc_url.clone(), &config.runtime_token_file)?;
    let state = State {
        config,
        store,
        api: Api::new(Credentials {
            app_id: "123".into(),
            client_secret: "fake-secret".into(),
        })?,
        runtime,
        stop: CancellationToken::new(),
        notify: Notify::new(),
        status: Mutex::default(),
        health_token: "unused-test-health-token".into(),
    };
    Ok((dir, state, delivery))
}

#[test]
fn repeated_errors_never_discard_a_delivery() -> Result<()> {
    let (_dir, _state, template) = fixture()?;
    for phase in ["pending", "submitted", "reply"] {
        let mut delivery = template.clone();
        delivery.phase = phase.into();
        for _ in 0..30 {
            defer(&mut delivery, "explicit rejection or unavailable service");
            assert_eq!(delivery.phase, phase);
            assert!(delivery.next_attempt <= Utc::now().timestamp() + 60);
        }
        assert_eq!(delivery.attempts, 30);
        assert_eq!(delivery.task_id, template.task_id);
    }
    Ok(())
}

#[tokio::test]
async fn old_submitted_messages_keep_polling_and_receive_completed_results() -> Result<()> {
    for task_status in ["running", "succeeded"] {
        let (_dir, mut state, delivery) = fixture()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        state.runtime = MessageClient::new(
            format!("http://{}/rpc", listener.local_addr()?),
            &state.config.runtime_token_file,
        )?;
        let status = task_status.to_string();
        let app = Router::new().route(
            "/rpc",
            axum::routing::post(move |Json(request): Json<serde_json::Value>| {
                let status = status.clone();
                async move {
                    assert_eq!(request["method"], "message.get"); // Never resubmit an existing task.
                    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":{
                        "id":"a".repeat(32),"channel":"message:qq:channel","source":"message",
                        "message":"long running task","status":status,"result":"completed result",
                        "error":null,"created_at":1,"updated_at":2
                    }}))
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        process(&state, delivery.clone()).await?;
        let updated = state.store.summaries()?.remove(0);
        assert_eq!(updated["task_id"], delivery.task_id.unwrap());
        if task_status == "running" {
            assert_eq!(updated["phase"], "submitted");
        } else {
            assert_eq!(updated["phase"], "reply");
            assert_eq!(updated["total_chunks"], 1);
        }
        assert!(!state.store.counts()?.contains_key("dead"));
        server.abort();
    }
    Ok(())
}

#[test]
fn legacy_config_and_stopped_deliveries_are_compatible() -> Result<()> {
    let (dir, state, mut delivery) = fixture()?;
    let mut document = serde_json::to_value(&state.config)?;
    document["reply_ttl_seconds"] = serde_json::json!(240);
    private_write(
        &dir.path().join("config.json"),
        &serde_json::to_vec(&document)?,
    )?;
    let loaded = Config::load(dir.path())?;
    assert!(
        serde_json::to_value(loaded)?
            .get("reply_ttl_seconds")
            .is_none()
    );
    delivery.phase = "dead".into();
    state.store.save(&delivery)?;
    assert_eq!(state.store.resume_legacy_deliveries()?, 1);
    let restored = state.store.due(1)?.remove(0);
    assert_eq!(restored.phase, "submitted");
    assert_eq!(restored.task_id, delivery.task_id);
    assert_eq!(restored.event.key, delivery.event.key);
    assert_eq!(state.store.resume_legacy_deliveries()?, 0);
    // Keep already-confirmed chunk progress when recovering legacy send failures.
    delivery.phase = "dead".into();
    delivery.chunks = vec!["sent".into(), "remaining".into()];
    delivery.sent = 1;
    state.store.save(&delivery)?;
    state.store.resume_legacy_deliveries()?;
    let restored = state.store.due(1)?.remove(0);
    assert_eq!(restored.phase, "reply");
    assert_eq!(restored.sent, 1);
    Ok(())
}
