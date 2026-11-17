//! Wire DTOs used by both services. Keep field names/status strings backward compatible.
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const JSON_RPC_VERSION: &str = "2.0";
pub const PROTOCOL_VERSION: u32 = 1;
pub const SUBMIT: &str = "message.submit";
pub const GET: &str = "message.get";
pub const CANCEL: &str = "message.cancel";
pub const STATUS: &str = "runtime.status";
pub const METHODS: [&str; 4] = [SUBMIT, GET, CANCEL, STATUS];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub channel: String,
    pub source: String,
    pub message: String,
    pub status: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submit {
    pub message: String,
    #[serde(default = "default_channel")]
    pub channel: String,
    pub idempotency_key: Option<String>,
}
fn default_channel() -> String {
    "main".into()
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ValidationError(pub &'static str);
impl Submit {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.message.trim().is_empty() || self.message.len() > 16000 {
            return Err(ValidationError(
                "message must be nonempty and at most 16000 bytes",
            ));
        }
        if self.channel.is_empty()
            || self.channel.len() > 64
            || !self
                .channel
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
        {
            return Err(ValidationError(
                "channel must be 1..64 ASCII letters, digits, _ - . :",
            ));
        }
        if let Some(key) = &self.idempotency_key
            && (key.trim().is_empty() || key.len() > 128)
        {
            return Err(ValidationError(
                "idempotency_key must be nonempty and at most 128 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submitted {
    pub task_id: String,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskId {
    pub task_id: String,
}
impl TaskId {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.task_id.len() != 32 || !self.task_id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ValidationError("Invalid task_id"));
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct RuntimeStatus {
    pub pi_ready: bool,
    pub current_task: Option<String>,
    pub last_heartbeat: Option<i64>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct RuntimeInfo {
    pub protocol_version: u32,
    pub status: RuntimeStatus,
    pub methods: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_and_validation_preserve_the_wire_contract() {
        let mut submit: Submit = serde_json::from_str(r#"{"message":"hello"}"#).unwrap();
        assert_eq!(submit.channel, "main");
        assert!(submit.validate().is_ok());
        submit.channel = "not/a/channel".into();
        assert!(submit.validate().is_err());
        assert!(
            TaskId {
                task_id: crate::id()
            }
            .validate()
            .is_ok()
        );
        assert!(
            TaskId {
                task_id: "bad".into()
            }
            .validate()
            .is_err()
        );
        assert!(serde_json::from_str::<Submit>(r#"{"message":"x","unexpected":true}"#).is_err());
        assert_eq!(
            serde_json::to_value(Submitted {
                task_id: "id".into()
            })
            .unwrap(),
            serde_json::json!({"task_id":"id"})
        );
    }
}
