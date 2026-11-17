use super::{
    config::{API_BASE, Credentials, TOKEN_URL},
    store::Delivery,
};
use anyhow::{Result, ensure};
use pi_channel_core::message_client::bounded_json;
use serde_json::json;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("QQ request rejected: HTTP {status}, API code {code}")]
    Rejected { status: u16, code: i64 },
    #[error("QQ request was not sent or was rate-limited; retry later")]
    Retryable,
    #[error("QQ send outcome unknown; manual review required")]
    Uncertain,
}
struct Token {
    value: Option<String>,
    refresh_at: Instant,
}
pub struct Api {
    http: reqwest::Client,
    credentials: Credentials,
    api_base: String,
    token_url: String,
    token: Mutex<Token>,
    test: bool,
}
impl Api {
    pub fn new(credentials: Credentials) -> Result<Self> {
        Self::build(credentials, API_BASE.into(), TOKEN_URL.into(), false)
    }
    fn build(
        credentials: Credentials,
        api_base: String,
        token_url: String,
        test: bool,
    ) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .user_agent("PiQQ/0.2.0")
                .build()?,
            credentials,
            api_base,
            token_url,
            test,
            token: Mutex::new(Token {
                value: None,
                refresh_at: Instant::now(),
            }),
        })
    }
    #[cfg(feature = "test-peer")]
    pub fn for_test(credentials: Credentials, base: String) -> Result<Self> {
        let url = reqwest::Url::parse(&base)?;
        ensure!(
            url.scheme() == "http" && url.host_str() == Some("127.0.0.1"),
            "Tests only support loopback"
        );
        Self::build(credentials, base.clone(), format!("{base}/token"), true)
    }
    pub fn app_id(&self) -> &str {
        &self.credentials.app_id
    }
    pub async fn invalidate(&self) {
        self.token.lock().await.value = None;
    }
    pub async fn token(&self) -> Result<String, ApiError> {
        let mut token = self.token.lock().await;
        if token.value.is_some() && token.refresh_at > Instant::now() {
            return Ok(token.value.clone().unwrap());
        }
        let response = self.http.post(&self.token_url).json(&json!({"appId":self.credentials.app_id,"clientSecret":self.credentials.client_secret})).send().await.map_err(|_| ApiError::Retryable)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            return Err(if status == 429 || status >= 500 {
                ApiError::Retryable
            } else {
                ApiError::Rejected { status, code: 0 }
            });
        }
        let data = bounded_json(response)
            .await
            .map_err(|_| ApiError::Retryable)?;
        let value = data["access_token"]
            .as_str()
            .filter(|token| !token.is_empty() && token.len() <= 4096 && token.is_ascii())
            .ok_or(ApiError::Rejected {
                status: 200,
                code: 0,
            })?
            .to_string();
        let seconds = data["expires_in"]
            .as_u64()
            .or_else(|| data["expires_in"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(7200)
            .clamp(2, 86400);
        token.refresh_at = Instant::now() + Duration::from_secs(seconds.saturating_sub(60).max(1));
        token.value = Some(value.clone());
        tracing::info!("QQ access token refreshed");
        Ok(value)
    }
    pub fn validate_gateway(&self, gateway: &str) -> Result<()> {
        let url = reqwest::Url::parse(gateway)?;
        ensure!(
            url.username().is_empty() && url.password().is_none(),
            "Gateway URL must not contain credentials"
        );
        if self.test {
            ensure!(
                url.scheme() == "ws" && url.host_str() == Some("127.0.0.1"),
                "Test gateway must be loopback"
            );
        } else {
            let host = url.host_str().unwrap_or("");
            ensure!(
                url.scheme() == "wss" && (host == "qq.com" || host.ends_with(".qq.com")),
                "QQ gateway must be a secure qq.com endpoint"
            );
        }
        Ok(())
    }
    pub async fn gateway(&self) -> Result<String, ApiError> {
        for attempt in 0..2 {
            let token = self.token().await?;
            let response = self
                .http
                .get(format!("{}/gateway", self.api_base))
                .header("Authorization", format!("QQBot {token}"))
                .send()
                .await
                .map_err(|_| ApiError::Retryable)?;
            let status = response.status().as_u16();
            if status == 401 && attempt == 0 {
                self.invalidate().await;
                continue;
            }
            if !(200..300).contains(&status) {
                return Err(if status == 429 || status >= 500 {
                    ApiError::Retryable
                } else {
                    ApiError::Rejected { status, code: 0 }
                });
            }
            let data = bounded_json(response)
                .await
                .map_err(|_| ApiError::Retryable)?;
            let url = data["url"].as_str().ok_or(ApiError::Rejected {
                status: 200,
                code: 0,
            })?;
            self.validate_gateway(url).map_err(|_| ApiError::Rejected {
                status: 200,
                code: 0,
            })?;
            return Ok(url.into());
        }
        Err(ApiError::Retryable)
    }
    pub async fn send(&self, delivery: &Delivery) -> Result<(), ApiError> {
        let kind = if delivery.event.kind == "c2c" {
            "users"
        } else {
            "groups"
        };
        let url = format!(
            "{}/v2/{}/{}/messages",
            self.api_base, kind, delivery.event.target
        );
        for attempt in 0..2 {
            let token = self.token().await?;
            let response=self.http.post(&url).header("Authorization",format!("QQBot {token}"))
                .header("X-Union-Appid",&self.credentials.app_id)
                .json(&json!({"msg_type":0,"content":delivery.chunks[delivery.sent],"msg_id":delivery.event.message_id,"msg_seq":delivery.sent+1}))
                .send().await.map_err(|error| if error.is_connect() {ApiError::Retryable} else {ApiError::Uncertain})?;
            let status = response.status().as_u16();
            if status == 401 && attempt == 0 {
                self.invalidate().await;
                continue;
            }
            if status == 429 {
                return Err(ApiError::Retryable);
            }
            // A 5xx may have been returned after the send committed. Do not blind-retry.
            if status >= 500 {
                return Err(ApiError::Uncertain);
            }
            let data = bounded_json(response)
                .await
                .map_err(|_| ApiError::Uncertain)?;
            let code = data["code"].as_i64().unwrap_or(0);
            if !(200..300).contains(&status) || code != 0 {
                return Err(ApiError::Rejected { status, code });
            }
            if data["id"].as_str().is_none_or(str::is_empty) {
                return Err(ApiError::Uncertain);
            }
            return Ok(());
        }
        Err(ApiError::Retryable)
    }
}
