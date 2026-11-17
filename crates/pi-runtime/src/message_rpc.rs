//! JSON-RPC 2.0 over authenticated HTTP. This is NOT Pi's stdin/stdout RPC.
use crate::{
    daemon::State,
    messages::{self, MessageError, Submit},
};
use axum::{
    Json,
    body::Bytes,
    extract::State as WebState,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use pi_channel_core::protocol::{self, TaskId};
use serde_json::{Value, json};
use std::sync::Arc;

fn error(id: Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":protocol::JSON_RPC_VERSION, "id":id, "error":{"code":code, "message":message}})
}

pub async fn handler(WebState(state): WebState<Arc<State>>, body: Bytes) -> Response {
    let request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return Json(error(Value::Null, -32700, "Parse error")).into_response(),
    };
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    if !request.is_object()
        || request["jsonrpc"] != protocol::JSON_RPC_VERSION
        || !request["method"].is_string()
        || (!id.is_null() && !id.is_string() && !id.is_number())
        || request
            .get("params")
            .is_some_and(|p| !p.is_object() && !p.is_array())
    {
        return Json(error(
            Value::Null,
            -32600,
            "Invalid Request (single requests only)",
        ))
        .into_response();
    }
    let notification = request.get("id").is_none();
    let method = request["method"].as_str().unwrap();
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    let result = dispatch(&state, method, params);
    if notification {
        return StatusCode::NO_CONTENT.into_response();
    }
    Json(match result {
        Ok(result) => json!({"jsonrpc":protocol::JSON_RPC_VERSION,"id":id,"result":result}),
        Err((code, message)) => error(id, code, &message),
    })
    .into_response()
}

type Fault = (i32, String);
fn invalid(error: impl ToString) -> Fault {
    (-32602, error.to_string())
}
fn internal(error: anyhow::Error) -> Fault {
    tracing::error!(%error, "Message RPC storage failure");
    (-32603, "Internal error".into())
}
fn task_id(params: Value) -> Result<String, Fault> {
    let input: TaskId = serde_json::from_value(params).map_err(invalid)?;
    input.validate().map_err(invalid)?;
    Ok(input.task_id)
}
fn dispatch(state: &State, method: &str, params: Value) -> Result<Value, Fault> {
    match method {
        protocol::SUBMIT => {
            let input: Submit = serde_json::from_value(params).map_err(invalid)?;
            let id = messages::submit(state, input).map_err(|e| match e {
                MessageError::Invalid(message) => invalid(message),
                MessageError::Rejected(message) => (-32000, message),
            })?;
            Ok(json!({"task_id":id}))
        }
        protocol::GET => {
            let id = task_id(params)?;
            state
                .store
                .task(&id)
                .map_err(internal)?
                .map(|task| json!(task))
                .ok_or((-32004, "Task not found".into()))
        }
        protocol::CANCEL => {
            let id = task_id(params)?;
            if !state.store.cancel_queued(&id).map_err(internal)? {
                return Err((-32009, "Only queued tasks can be cancelled".into()));
            }
            Ok(json!({"cancelled":true}))
        }
        protocol::STATUS => {
            if params != json!({}) {
                return Err(invalid("runtime.status takes no params"));
            }
            Ok(
                json!({"protocol_version":protocol::PROTOCOL_VERSION,"status":state.status.lock().unwrap().clone(),
                "methods":protocol::METHODS}),
            )
        }
        _ => Err((-32601, "Method not found".into())),
    }
}
