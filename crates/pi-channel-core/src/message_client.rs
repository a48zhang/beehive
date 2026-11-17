//! Typed client for the service's message JSON-RPC API.
use crate::{
    id,
    protocol::{self, RuntimeInfo, Submit, Submitted, Task, TaskId},
    secrets::read_token,
};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Runtime transport unavailable")]
    Transport,
    #[error("Runtime HTTP status {0}")]
    Http(u16),
    #[error("Runtime RPC error {0}")]
    Rpc(i64),
    #[error("Invalid runtime RPC response")]
    Protocol,
}
impl ClientError {
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Transport | Self::Http(429 | 500..=599))
            || matches!(self, Self::Rpc(-32000)) // full queue or conflicting idempotency key; retries are bounded
    }
}
#[derive(Clone)]
pub struct MessageClient {
    http: reqwest::Client,
    url: String,
    token: String,
}
impl MessageClient {
    pub fn new(url: String, token_file: &Path) -> anyhow::Result<Self> {
        let token = read_token(token_file)?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        Ok(Self { http, url, token })
    }
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, ClientError> {
        let id = id();
        let response = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&json!({"jsonrpc":protocol::JSON_RPC_VERSION,"id":id,"method":method,"params":params}))
            .send()
            .await
            .map_err(|_| ClientError::Transport)?;
        if !response.status().is_success() {
            return Err(ClientError::Http(response.status().as_u16()));
        }
        let data = bounded_json(response)
            .await
            .map_err(|_| ClientError::Protocol)?;
        if data["jsonrpc"] != protocol::JSON_RPC_VERSION || data["id"] != id {
            return Err(ClientError::Protocol);
        }
        if let Some(error) = data.get("error") {
            return Err(ClientError::Rpc(
                error["code"].as_i64().ok_or(ClientError::Protocol)?,
            ));
        }
        data.get("result").cloned().ok_or(ClientError::Protocol)
    }
    pub async fn submit(
        &self,
        channel: &str,
        message: &str,
        key: &str,
    ) -> Result<String, ClientError> {
        let value = self
            .call(
                protocol::SUBMIT,
                serde_json::to_value(Submit {
                    channel: channel.into(),
                    message: message.into(),
                    idempotency_key: Some(key.into()),
                })
                .map_err(|_| ClientError::Protocol)?,
            )
            .await?;
        let submitted: Submitted =
            serde_json::from_value(value).map_err(|_| ClientError::Protocol)?;
        Ok(submitted.task_id)
    }
    pub async fn get(&self, task_id: &str) -> Result<Task, ClientError> {
        let params = serde_json::to_value(TaskId {
            task_id: task_id.into(),
        })
        .map_err(|_| ClientError::Protocol)?;
        serde_json::from_value(self.call(protocol::GET, params).await?)
            .map_err(|_| ClientError::Protocol)
    }
    pub async fn status(&self) -> Result<RuntimeInfo, ClientError> {
        let info: RuntimeInfo =
            serde_json::from_value(self.call(protocol::STATUS, json!({})).await?)
                .map_err(|_| ClientError::Protocol)?;
        if info.protocol_version != protocol::PROTOCOL_VERSION {
            return Err(ClientError::Protocol);
        }
        Ok(info)
    }
}

pub async fn bounded_json(mut response: reqwest::Response) -> anyhow::Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            bytes.len() + chunk.len() <= 1024 * 1024,
            "HTTP response exceeds 1 MiB"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
