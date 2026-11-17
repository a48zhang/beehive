//! Stable channel/runtime contract and small process-independent service helpers.
//! No QQ transport, Pi process lifecycle, scheduler or database implementation lives here.
pub mod message_client;
pub mod paths;
pub mod protocol;
pub mod secrets;
pub mod systemd;

pub fn id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

pub fn init_logging() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
}
