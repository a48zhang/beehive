//! Transport-neutral message submission shared by REST and JSON-RPC.
use crate::daemon::State;
pub use pi_channel_core::protocol::Submit;
use std::fmt;

#[derive(Debug)]
pub enum MessageError {
    Invalid(String),
    Rejected(String),
}
impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Rejected(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for MessageError {}

pub fn submit(state: &State, input: Submit) -> Result<String, MessageError> {
    input
        .validate()
        .map_err(|error| MessageError::Invalid(error.to_string()))?;
    let id = state
        .store
        .enqueue(
            &format!("message:{}", input.channel),
            "message",
            &input.message,
            input.idempotency_key.as_deref(),
            false,
        )
        .map_err(|error| MessageError::Rejected(error.to_string()))?;
    state.notify.notify_one();
    Ok(id)
}
