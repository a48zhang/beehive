use crate::{config::Config, rpc::Rpc, schedule::heartbeat_content, store::Store, web};
use anyhow::{Context, Result};
use chrono::Utc;
use fs2::FileExt;
use std::{
    fs::{self, OpenOptions},
    future::IntoFuture,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub use pi_channel_core::protocol::RuntimeStatus as Status;

pub struct State {
    pub config: Config,
    pub home: PathBuf,
    pub token: String,
    pub store: Store,
    pub notify: Notify,
    pub stop: CancellationToken,
    pub status: Mutex<Status>,
    pub rate: Mutex<(std::time::Instant, u32)>,
}

pub async fn serve(home: PathBuf) -> Result<()> {
    let config = Config::load(&home)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(home.join("runtime.lock"))?;
    lock.try_lock_exclusive()
        .context("Another pi-runtime daemon is already running")?;
    fs::create_dir_all(home.join("sessions"))?;
    let store = Store::open(&home.join("runtime.sqlite3"))?;
    let recovered = store.recover()?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "Marked previously running tasks interrupted; not replaying"
        );
    }
    let token = crate::config::token(&home)?;
    let state = Arc::new(State {
        config,
        home,
        token,
        store,
        notify: Notify::new(),
        stop: CancellationToken::new(),
        status: Mutex::default(),
        rate: Mutex::new((std::time::Instant::now(), 0)),
    });
    // Bind before starting any agent, so an occupied port fails without side effects.
    let listener =
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, state.config.port)).await?;
    let mut worker = tokio::spawn(worker(state.clone()));
    let mut ticker = tokio::spawn(ticker(state.clone()));
    let signal_state = state.clone();
    let signal = tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        signal_state.stop.cancel();
    });
    tracing::info!(port = state.config.port, "Pi runtime listening on loopback");
    let http_state = state.clone();
    let server = axum::serve(listener, web::router(state.clone()))
        .with_graceful_shutdown(async move {
            http_state.stop.cancelled().await;
        })
        .into_future();
    tokio::pin!(server);
    // A worker failure must not leave a superficially healthy but inert daemon.
    let (result, worker_finished, ticker_finished) = tokio::select! {
        result = &mut server => (result.map_err(anyhow::Error::from), false, false),
        result = &mut worker => {
            let result = if state.stop.is_cancelled() { result.map_err(anyhow::Error::from) }
                else { Err(anyhow::anyhow!("Worker exited unexpectedly: {result:?}")) };
            state.stop.cancel();
            (result, true, false)
        }
        result = &mut ticker => {
            let result = if state.stop.is_cancelled() { result.context("Scheduler panicked").and_then(|r| r) }
                else { Err(anyhow::anyhow!("Scheduler exited unexpectedly: {result:?}")) };
            state.stop.cancel();
            (result, false, true)
        }
    };
    state.stop.cancel();
    if !worker_finished {
        worker.await.context("Worker panicked")?;
    }
    if !ticker_finished {
        ticker.await??;
    }
    signal.abort();
    result
}

async fn worker(state: Arc<State>) {
    let mut rpc: Option<Rpc> = None;
    let mut backoff = 1;
    loop {
        if state.stop.is_cancelled() {
            break;
        }
        if rpc.is_none() {
            let started = tokio::select! {
                _ = state.stop.cancelled() => break,
                started = Rpc::start(&state.config, &state.home) => started,
            };
            match started {
                Ok(child) => {
                    rpc = Some(child);
                    backoff = 1;
                    state.status.lock().unwrap().pi_ready = true;
                }
                Err(error) => {
                    tracing::error!(%error, backoff, "Pi startup failed; retrying");
                    tokio::select! { _ = state.stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(backoff)) => {} }
                    backoff = (backoff * 2).min(60);
                    continue;
                }
            }
        }
        let task = match state.store.claim() {
            Ok(task) => task,
            Err(error) => {
                tracing::error!(%error, "Cannot claim task");
                state.stop.cancel();
                break;
            }
        };
        if let Some(task) = task {
            state.status.lock().unwrap().current_task = Some(task.id.clone());
            tracing::info!(task_id = %task.id, source = %task.source, "Task started");
            let result = tokio::select! {
                _ = state.stop.cancelled() => Err(anyhow::anyhow!("Runtime shutting down; execution may have been partial")),
                result = tokio::time::timeout(Duration::from_secs(state.config.task_timeout_seconds), rpc.as_mut().unwrap().run(&task, &state.store)) => result.context("Task execution timed out; execution may have been partial").and_then(|r| r),
            };
            let failed = result.is_err();
            let saved = match result {
                Ok(output) => state
                    .store
                    .finish(&task.id, "succeeded", Some(&output), None),
                Err(error) => {
                    let status = if state.stop.is_cancelled() {
                        "interrupted"
                    } else {
                        "failed"
                    };
                    state
                        .store
                        .finish(&task.id, status, None, Some(&error.to_string()))
                }
            };
            if let Err(error) = saved {
                tracing::error!(%error, "Cannot persist task outcome");
                state.stop.cancel();
            }
            tracing::info!(task_id = %task.id, failed, "Task finished");
            state.status.lock().unwrap().current_task = None;
            if failed {
                state.status.lock().unwrap().pi_ready = false;
                if let Some(mut child) = rpc.take() {
                    child.stop().await;
                }
            }
        } else {
            tokio::select! {
                _ = state.stop.cancelled() => break,
                _ = state.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    if let Err(error) = rpc.as_mut().unwrap().ping().await {
                        tracing::warn!(%error, "Pi health check failed; restarting");
                        state.status.lock().unwrap().pi_ready = false;
                        if let Some(mut child) = rpc.take() { child.stop().await; }
                    }
                }
            }
        }
    }
    state.status.lock().unwrap().pi_ready = false;
    if let Some(mut child) = rpc {
        child.stop().await;
    }
}

async fn ticker(state: Arc<State>) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = state.stop.cancelled() => break, _ = interval.tick() => {} }
        if let Err(error) = tick(&state) {
            tracing::error!(%error, "Scheduler tick failed");
        }
    }
    Ok(())
}

fn tick(state: &State) -> Result<()> {
    let now = Utc::now();
    for job in state.store.jobs()? {
        if job.enabled && job.next_run.is_some_and(|next| next <= now.timestamp()) {
            let mut next = job.clone();
            next.next_run = job.timing.next(now, false)?.map(|t| t.timestamp());
            next.enabled = next.next_run.is_some();
            state.store.dispatch_job(&job, &next)?;
            state.notify.notify_one();
        }
    }
    let last = state
        .store
        .metadata("heartbeat_tick")?
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    if now.timestamp() - last >= state.config.heartbeat_seconds as i64 {
        let path = state.home.join("HEARTBEAT.md");
        let content = if path.exists() {
            anyhow::ensure!(
                fs::metadata(&path)?.len() <= 16000,
                "HEARTBEAT.md exceeds 16000 bytes"
            );
            heartbeat_content(&fs::read_to_string(&path)?)
        } else {
            String::new()
        };
        if !content.is_empty() {
            let message = format!(
                "[Scheduled heartbeat {}]\nReview this user-owned checklist. Do only the listed work. If nothing needs attention, reply HEARTBEAT_OK.\n\n{}",
                now.to_rfc3339(),
                content
            );
            state
                .store
                .enqueue("heartbeat", "heartbeat", &message, None, true)?;
            state.notify.notify_one();
        }
        state
            .store
            .set_metadata("heartbeat_tick", &now.timestamp().to_string())?;
        state.status.lock().unwrap().last_heartbeat = Some(now.timestamp());
    }
    Ok(())
}
