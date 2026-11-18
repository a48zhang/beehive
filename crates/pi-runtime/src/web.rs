use crate::{
    daemon::State,
    schedule::Timing,
    store::{Job, id},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State as WebState, rejection::JsonRejection},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        tracing::error!(%error, "API operation failed");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal operation failed; see service logs".into(),
        )
    }
}
type ApiResult<T> = Result<T, ApiError>;
fn bad(message: impl ToString) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.to_string())
}
fn parse<T>(body: Result<Json<T>, JsonRejection>) -> ApiResult<T> {
    body.map(|Json(data)| data).map_err(|e| bad(e.body_text()))
}
fn validate(value: &str, name: &str, max: usize) -> ApiResult<()> {
    if value.trim().is_empty() || value.len() > max {
        return Err(bad(format!(
            "{name} must be nonempty and at most {max} bytes"
        )));
    }
    Ok(())
}

pub fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/ready", get(ready))
        .route("/v1/status", get(status))
        .route("/rpc", post(crate::message_rpc::handler))
        .route("/v1/messages", post(message))
        .route("/v1/tasks", get(tasks))
        .route("/v1/tasks/{id}", get(task).delete(cancel))
        .route("/v1/jobs", get(jobs).post(add_job))
        .route("/v1/jobs/{id}", delete(delete_job))
        .fallback(|| async { ApiError(StatusCode::NOT_FOUND, "Not found".into()) })
        .layer(DefaultBodyLimit::max(32768))
        .layer(middleware::from_fn_with_state(state.clone(), guard))
        .with_state(state)
}

async fn guard(WebState(state): WebState<Arc<State>>, request: Request, next: Next) -> Response {
    let request_id = id();
    let path = request.uri().path().to_string();
    let method = request.method().to_string();
    let started = std::time::Instant::now();
    let mut response = if path != "/health" {
        let credential = request
            .headers()
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
            .unwrap_or("");
        let valid: bool = credential.as_bytes().ct_eq(state.token.as_bytes()).into();
        if !valid {
            ApiError(StatusCode::UNAUTHORIZED, "Bearer token required".into()).into_response()
        } else if request.headers().contains_key("origin") {
            ApiError(
                StatusCode::FORBIDDEN,
                "Browser origins are not enabled".into(),
            )
            .into_response()
        } else if state.stop.is_cancelled() {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Runtime shutting down".into(),
            )
            .into_response()
        } else {
            next.run(request).await
        }
    } else {
        next.run(request).await
    };
    for (key, value) in [
        ("x-content-type-options", "nosniff"),
        ("cache-control", "no-store"),
        ("x-frame-options", "DENY"),
    ] {
        response
            .headers_mut()
            .insert(key, HeaderValue::from_static(value));
    }
    response
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());
    tracing::info!(%request_id, %method, %path, status = response.status().as_u16(), duration_ms = started.elapsed().as_millis(), "HTTP request");
    response
}

async fn ready(WebState(state): WebState<Arc<State>>) -> Response {
    let ready = state.status.lock().unwrap().pi_ready;
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
async fn status(WebState(state): WebState<Arc<State>>) -> Json<Value> {
    Json(serde_json::to_value(state.status.lock().unwrap().clone()).unwrap())
}

async fn message(
    WebState(state): WebState<Arc<State>>,
    body: Result<Json<crate::messages::Submit>, JsonRejection>,
) -> ApiResult<impl IntoResponse> {
    let input = parse(body)?;
    let id = crate::messages::submit(&state, input).map_err(bad)?;
    Ok((StatusCode::ACCEPTED, Json(json!({"task_id":id}))))
}
async fn tasks(WebState(state): WebState<Arc<State>>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(state.store.tasks()?)))
}
async fn task(
    WebState(state): WebState<Arc<State>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let task = state
        .store
        .task(&id)?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Task not found".into()))?;
    Ok(Json(json!(task)))
}
async fn cancel(
    WebState(state): WebState<Arc<State>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    if !state.store.cancel_queued(&id)? {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Only an existing queued task can be cancelled".into(),
        ));
    }
    Ok(Json(json!({"cancelled":true})))
}
async fn jobs(WebState(state): WebState<Arc<State>>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(state.store.jobs()?)))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewJob {
    name: String,
    message: String,
    timing: Timing,
}
async fn add_job(
    WebState(state): WebState<Arc<State>>,
    body: Result<Json<NewJob>, JsonRejection>,
) -> ApiResult<impl IntoResponse> {
    let input = parse(body)?;
    validate(&input.name, "name", 128)?;
    validate(&input.message, "message", 16000)?;
    let next = input.timing.next(Utc::now(), true).map_err(bad)?;
    let job = Job {
        id: id(),
        name: input.name,
        message: input.message,
        timing: input.timing,
        next_run: next.map(|date| date.timestamp()),
        enabled: true,
    };
    state.store.add_job(&job).map_err(bad)?;
    Ok((StatusCode::CREATED, Json(json!(job))))
}
async fn delete_job(
    WebState(state): WebState<Arc<State>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    if !state.store.delete_job(&id)? {
        return Err(ApiError(StatusCode::NOT_FOUND, "Job not found".into()));
    }
    Ok(Json(
        json!({"deleted":true, "note":"Already queued or running occurrences are not cancelled"}),
    ))
}
