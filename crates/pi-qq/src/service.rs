use super::{
    api::{Api, ApiError},
    config::Config,
    gateway, protocol,
    store::{Delivery, Store},
};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Request, State as WebState},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::Utc;
use fs2::FileExt;
use pi_channel_core::{message_client::MessageClient, secrets::token};
use serde::Serialize;
use serde_json::json;
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::{sync::Notify, task::JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Default, Clone, Serialize)]
pub struct Status {
    pub qq_ready: bool,
    pub runtime_ready: bool,
    pub reconnects: u64,
}
pub struct State {
    pub config: Config,
    pub store: Store,
    pub api: Api,
    pub runtime: MessageClient,
    pub stop: CancellationToken,
    pub notify: Notify,
    pub status: Mutex<Status>,
    health_token: String,
}
fn lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(path)?;
    file.try_lock_exclusive()
        .context("Another QQ adapter owns this data directory or app ID")?;
    Ok(file)
}
pub async fn serve(home: PathBuf) -> Result<()> {
    let config = Config::load(&home).map_err(|e| gateway::Fatal(e.to_string()))?;
    let api = Api::new(
        config
            .credentials()
            .map_err(|e| gateway::Fatal(e.to_string()))?,
    )?;
    serve_with_api(home, config, api).await
}
#[cfg(feature = "test-peer")]
pub async fn serve_test(home: PathBuf, api: Api) -> Result<()> {
    let config = Config::load(&home)?;
    serve_with_api(home, config, api).await
}
async fn serve_with_api(home: PathBuf, config: Config, api: Api) -> Result<()> {
    config
        .ensure_hermes_stopped(api.app_id())
        .map_err(|e| gateway::Fatal(e.to_string()))?;
    let _home_lock = lock(&home.join("service.lock"))?;
    let lock_dir = PathBuf::from(std::env::var_os("XDG_STATE_HOME").unwrap_or_else(|| {
        PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
            .join(".local/state")
            .into_os_string()
    }))
    .join("pi-qq");
    fs::create_dir_all(&lock_dir)?;
    let _app_lock = lock(&lock_dir.join(format!("{}.lock", protocol::hash(&[api.app_id()]))))?;
    let store = Store::open(&home.join("qq.sqlite3"))?;
    let recovered = store.recover()?;
    if recovered > 0 {
        tracing::warn!(recovered, "Interrupted QQ sends require manual review");
    }
    let runtime = MessageClient::new(config.runtime_rpc_url.clone(), &config.runtime_token_file)?;
    let health_token = token(&home)?;
    let listener =
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, config.health_port)).await?;
    let state = Arc::new(State {
        config,
        store,
        api,
        runtime,
        health_token,
        stop: CancellationToken::new(),
        notify: Notify::new(),
        status: Mutex::default(),
    });
    let mut tasks = JoinSet::new();
    tasks.spawn(gateway::run(state.clone()));
    tasks.spawn(worker(state.clone()));
    let http_state = state.clone();
    let cancel = state.stop.clone();
    tasks.spawn(async move {
        axum::serve(listener, router(http_state))
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await?;
        Ok(())
    });
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tracing::info!(
        port = state.config.health_port,
        allowed_users = state.config.allowed_users.len(),
        allowed_groups = state.config.allowed_groups.len(),
        "Native QQ service started (identifiers/secrets not logged)"
    );
    let result = tokio::select! {
        _=term.recv()=>Ok(()),
        _=tokio::signal::ctrl_c()=>Ok(()),
        joined=tasks.join_next()=>match joined { Some(Ok(Ok(()))) if state.stop.is_cancelled()=>Ok(()), Some(Ok(Err(error)))=>Err(error), other=>Err(anyhow::anyhow!("QQ service task exited: {other:?}")) },
    };
    state.stop.cancel();
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            tracing::error!(%error,"QQ service task failed during shutdown");
        }
    }
    state.store.recover()?;
    result
}

fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/ready", get(ready))
        .route("/v1/status", get(status))
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state)
}
async fn guard(WebState(state): WebState<Arc<State>>, request: Request, next: Next) -> Response {
    if request.uri().path() != "/health" {
        let value = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if !bool::from(value.as_bytes().ct_eq(state.health_token.as_bytes())) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"Bearer token required"})),
            )
                .into_response();
        }
        if request.headers().contains_key("origin") {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        axum::http::HeaderValue::from_static("nosniff"),
    );
    response
}
async fn ready(WebState(state): WebState<Arc<State>>) -> Response {
    let status = state.status.lock().unwrap().clone();
    let ready = status.qq_ready && status.runtime_ready;
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({"ready":ready})),
    )
        .into_response()
}
async fn status(WebState(state): WebState<Arc<State>>) -> Response {
    match state.store.counts() {
        Ok(counts) => {
            Json(json!({"status":state.status.lock().unwrap().clone(),"deliveries":counts}))
                .into_response()
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":"Store unavailable"})),
        )
            .into_response(),
    }
}

async fn worker(state: Arc<State>) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(state.config.poll_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_check = Instant::now() - Duration::from_secs(60);
    loop {
        tokio::select! {_=state.stop.cancelled()=>break,_=interval.tick()=>{},_=state.notify.notified()=>{}}
        if last_check.elapsed() >= Duration::from_secs(10) {
            let result = tokio::select! {_=state.stop.cancelled()=>break,result=state.runtime.status()=>result};
            state.status.lock().unwrap().runtime_ready =
                result.is_ok_and(|info| info.status.pi_ready);
            last_check = Instant::now();
        }
        for delivery in state.store.due(10)? {
            tokio::select! {_=state.stop.cancelled()=>return Ok(()),result=process(&state,delivery)=>result?}
        }
    }
    Ok(())
}

fn defer(delivery: &mut Delivery, error: impl ToString, permanent: bool) {
    delivery.attempts += 1;
    delivery.last_error = Some(error.to_string());
    if permanent || delivery.attempts >= 5 {
        delivery.phase = "dead".into();
    }
    delivery.next_attempt = Utc::now().timestamp() + (2_i64.pow(delivery.attempts.min(6))).min(60);
}
async fn process(state: &State, mut delivery: Delivery) -> Result<()> {
    let now = Utc::now().timestamp();
    if now - delivery.event.timestamp > state.config.reply_ttl_seconds {
        delivery.phase = "dead".into();
        delivery.last_error =
            Some("Reply TTL expired; the Pi task, if accepted, is NOT cancelled".into());
        state.store.save(&delivery)?;
        return Ok(());
    }
    let poll_seconds = state.config.poll_interval_ms.div_ceil(1000) as i64;
    match delivery.phase.as_str() {
        "pending" => match state
            .runtime
            .submit(
                &delivery.event.channel,
                &delivery.event.text,
                &delivery.event.key,
            )
            .await
        {
            Ok(id) => {
                delivery.task_id = Some(id);
                delivery.phase = "submitted".into();
                delivery.attempts = 0;
                delivery.next_attempt = now;
            }
            Err(error) => {
                state.status.lock().unwrap().runtime_ready = false;
                defer(&mut delivery, &error, !error.retryable());
            }
        },
        "submitted" => {
            let task_id = delivery
                .task_id
                .as_deref()
                .context("Submitted event without task ID")?;
            match state.runtime.get(task_id).await {
                Ok(task) => {
                    delivery.attempts = 0;
                    if matches!(task.status.as_str(), "queued" | "running") {
                        delivery.next_attempt = now + poll_seconds;
                    } else {
                        let reply = if task.status == "succeeded" {
                            task.result.unwrap_or_default()
                        } else {
                            format!(
                                "任务状态：{}。未自动重试；请在 pi-runtime 中检查任务 {}。",
                                task.status, task.id
                            )
                        };
                        delivery.chunks = protocol::chunks(&reply);
                        delivery.phase = "reply".into();
                        delivery.next_attempt = now;
                    }
                }
                Err(error) => {
                    state.status.lock().unwrap().runtime_ready = false;
                    defer(&mut delivery, &error, !error.retryable());
                }
            }
        }
        "reply" => {
            if delivery.sent >= delivery.chunks.len() {
                delivery.phase = "delivered".into();
                state.store.save(&delivery)?;
                return Ok(());
            }
            // Persist uncertainty BEFORE crossing the external side-effect boundary.
            delivery.phase = "sending".into();
            state.store.save(&delivery)?;
            match state.api.send(&delivery).await {
                Ok(()) => {
                    delivery.sent += 1;
                    delivery.attempts = 0;
                    delivery.last_error = None;
                    delivery.phase = if delivery.sent == delivery.chunks.len() {
                        "delivered"
                    } else {
                        "reply"
                    }
                    .into();
                    delivery.next_attempt = now + 1;
                }
                Err(ApiError::Uncertain) => {
                    delivery.phase = "uncertain".into();
                    delivery.last_error = Some(ApiError::Uncertain.to_string());
                }
                Err(error) => {
                    delivery.phase = "reply".into();
                    let permanent = matches!(error, ApiError::Rejected { .. });
                    defer(&mut delivery, error, permanent);
                }
            }
        }
        _ => anyhow::bail!("Invalid delivery state"),
    }
    if matches!(delivery.phase.as_str(), "dead" | "uncertain") {
        tracing::warn!(event_key=%delivery.event.key,phase=%delivery.phase,"QQ delivery needs operator review");
    }
    state.store.save(&delivery)?;
    Ok(())
}
