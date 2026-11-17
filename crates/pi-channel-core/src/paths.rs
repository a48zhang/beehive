use anyhow::{Result, ensure};
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn data_home(service: &str) -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
        .join(".local/share")
        .join(service)
}
pub fn runtime_home() -> PathBuf {
    data_home("pi-runtime")
}

/// Discover the channel-facing endpoint, without depending on runtime business config.
/// The remaining config keys belong to pi-runtime and are intentionally ignored.
pub fn runtime_rpc_url(home: &Path) -> Result<String> {
    #[derive(Deserialize)]
    struct Endpoint {
        port: u16,
    }
    let endpoint: Endpoint = serde_json::from_slice(&fs::read(home.join("config.json"))?)?;
    ensure!(endpoint.port > 0, "Runtime port must be nonzero");
    Ok(format!("http://127.0.0.1:{}/rpc", endpoint.port))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_discovery_does_not_require_runtime_internals() -> Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(
            dir.path().join("config.json"),
            r#"{"port":8787,"provider":"irrelevant","future_key":1}"#,
        )?;
        assert_eq!(runtime_rpc_url(dir.path())?, "http://127.0.0.1:8787/rpc");
        fs::write(dir.path().join("config.json"), r#"{"port":0}"#)?;
        assert!(runtime_rpc_url(dir.path()).is_err());
        Ok(())
    }
}
