use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

pub const API_BASE: &str = "https://api.sgroup.qq.com";
pub const TOKEN_URL: &str = "https://bots.qq.com/app/getAppAccessToken";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub runtime_rpc_url: String,
    pub runtime_token_file: PathBuf,
    pub credentials_file: PathBuf,
    pub health_port: u16,
    pub allowed_users: Vec<String>,
    pub allowed_groups: Vec<String>,
    pub poll_interval_ms: u64,
    pub reply_ttl_seconds: i64,
    pub hermes_home: Option<PathBuf>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub app_id: String,
    pub client_secret: String,
}

pub fn default_home() -> PathBuf {
    pi_channel_core::paths::data_home("pi-qq")
}

impl Config {
    pub fn load(home: &Path) -> Result<Self> {
        let config: Self = serde_json::from_slice(&fs::read(home.join("config.json"))?)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.runtime_rpc_url)?;
        ensure!(
            url.scheme() == "http"
                && matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "::1"))
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/rpc",
            "runtime_rpc_url must be http://127.0.0.1:PORT/rpc (or IPv6 loopback)"
        );
        ensure!(self.health_port > 0, "health_port must be nonzero");
        ensure!(
            (500..=30000).contains(&self.poll_interval_ms),
            "poll_interval_ms must be 500..30000"
        );
        ensure!(
            (60..=3600).contains(&self.reply_ttl_seconds),
            "reply_ttl_seconds must be 60..3600"
        );
        ensure!(
            self.runtime_token_file.is_absolute() && self.credentials_file.is_absolute(),
            "Token/credential paths must be absolute"
        );
        for entry in self.allowed_users.iter().chain(self.allowed_groups.iter()) {
            ensure!(
                !entry.is_empty()
                    && entry.len() <= 256
                    && entry
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
                "Allowlist entries must be exact openids, not wildcards"
            );
        }
        Ok(())
    }
    pub fn credentials(&self) -> Result<Credentials> {
        let creds: Credentials = serde_json::from_slice(&fs::read(&self.credentials_file)?)?;
        ensure!(
            !creds.app_id.is_empty() && creds.app_id.bytes().all(|b| b.is_ascii_digit()),
            "Invalid QQ app ID"
        );
        ensure!(
            !creds.client_secret.trim().is_empty(),
            "QQ client secret is empty"
        );
        Ok(creds)
    }

    // Fail closed while the imported Hermes gateway still owns a live QQ channel.
    // This never stops Hermes or modifies its locks/configuration.
    pub fn ensure_hermes_stopped(&self, app_id: &str) -> Result<()> {
        let Some(home) = &self.hermes_home else {
            return Ok(());
        };
        let path = home.join("gateway_state.json");
        if !path.exists() {
            return Ok(());
        }
        let state: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
        let pid = state["pid"].as_i64().unwrap_or(0);
        let qq = &state["platforms"]["qqbot"];
        if pid <= 0 || pid > i32::MAX as i64 || qq.is_null() {
            return Ok(());
        }
        let live = !pid_absent(pid);
        // Hermes retains platform records across gateway restarts. A disconnected
        // record from a dead prior writer is not an adapter in the current, fully
        // started gateway. Never exempt a current writer's transient disconnect.
        let retired_record = state["gateway_state"] == "running"
            && qq["state"] == "disconnected"
            && qq["writer_pid"]
                .as_i64()
                .is_some_and(|writer| writer != pid && pid_absent(writer));
        let stopped =
            matches!(qq["state"].as_str(), Some("disabled" | "stopped")) || retired_record;
        if live && !stopped {
            let env = read_env(&home.join(".env"))?;
            // Unknown ownership must be resolved by the operator, not guessed.
            ensure!(
                env.get("QQ_APP_ID").is_some_and(|id| id != app_id),
                "Hermes still has a live QQ channel for this app (or ownership is unknown). Disable its QQ channel or stop Hermes before starting pi-qq"
            );
        }
        Ok(())
    }
}

fn pid_absent(pid: i64) -> bool {
    if pid <= 0 || pid > i32::MAX as i64 {
        return false;
    }
    let result = unsafe { libc::kill(pid as i32, 0) };
    result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

fn read_env(path: &Path) -> Result<std::collections::HashMap<String, String>> {
    Ok(dotenvy::from_path_iter(path)
        .context("Read Hermes .env without executing it")?
        .collect::<Result<_, _>>()?)
}

/// Explicit, local, one-time bootstrap. No Hermes module imports or network calls.
pub fn import_hermes(home: &Path) -> Result<(Credentials, Vec<String>)> {
    let env = read_env(&home.join(".env"))?;
    let creds = Credentials {
        app_id: env
            .get("QQ_APP_ID")
            .context("Hermes has no QQ_APP_ID")?
            .to_string(),
        client_secret: env
            .get("QQ_CLIENT_SECRET")
            .context("Hermes has no QQ_CLIENT_SECRET")?
            .to_string(),
    };
    let mut users = BTreeSet::new();
    if let Some(ids) = env.get("QQ_ALLOWED_USERS") {
        users.extend(
            ids.split([',', ' ', '\n'])
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        );
    }
    let approved = home.join("pairing/qqbot-approved.json");
    if approved.is_file() {
        let value: serde_json::Value = serde_json::from_slice(&fs::read(approved)?)?;
        let map = value
            .as_object()
            .context("Unknown Hermes pairing store format; configure allowed_users manually")?;
        // The inspected Hermes format is { openid: { approved_at, user_name } }.
        for (openid, metadata) in map {
            ensure!(metadata.is_object(), "Unknown Hermes approved-entry format");
            users.insert(openid.clone());
        }
    }
    Ok((creds, users.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reject_remote_runtime_and_wildcards() {
        let mut config = Config {
            runtime_rpc_url: "http://127.0.0.1:8787/rpc".into(),
            runtime_token_file: "/tmp/token".into(),
            credentials_file: "/tmp/creds".into(),
            health_port: 8788,
            allowed_users: vec![],
            allowed_groups: vec![],
            poll_interval_ms: 1000,
            reply_ttl_seconds: 240,
            hermes_home: None,
        };
        assert!(config.validate().is_ok());
        config.runtime_rpc_url = "http://evil.example/rpc".into();
        assert!(config.validate().is_err());
        config.runtime_rpc_url = "http://127.0.0.1:8787/rpc".into();
        config.allowed_users.push("*".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn import_is_read_only_and_active_hermes_is_rejected() -> Result<()> {
        let dir = tempfile::tempdir()?;
        fs::create_dir(dir.path().join("pairing"))?;
        let original = "QQ_APP_ID=123\nQQ_CLIENT_SECRET=fake-secret\nQQ_ALLOWED_USERS=alice\n";
        fs::write(dir.path().join(".env"), original)?;
        fs::write(
            dir.path().join("pairing/qqbot-approved.json"),
            r#"{"bob":{"approved_at":1,"user_name":"test"}}"#,
        )?;
        let (creds, users) = import_hermes(dir.path())?;
        assert_eq!(creds.app_id, "123");
        assert_eq!(users, vec!["alice", "bob"]);
        assert_eq!(fs::read_to_string(dir.path().join(".env"))?, original);
        let config = Config {
            runtime_rpc_url: "http://127.0.0.1:8787/rpc".into(),
            runtime_token_file: "/tmp/token".into(),
            credentials_file: "/tmp/credentials".into(),
            health_port: 8788,
            allowed_users: users,
            allowed_groups: vec![],
            poll_interval_ms: 1000,
            reply_ttl_seconds: 240,
            hermes_home: Some(dir.path().to_path_buf()),
        };
        fs::write(
            dir.path().join("gateway_state.json"),
            serde_json::to_vec(
                &serde_json::json!({"pid":std::process::id(),"platforms":{"qqbot":{"state":"connected"}}}),
            )?,
        )?;
        assert!(config.ensure_hermes_stopped("123").is_err());
        assert!(config.ensure_hermes_stopped("456").is_ok());
        let mut child = std::process::Command::new("true").spawn()?;
        let old_writer = child.id();
        child.wait()?;
        for (gateway_state, writer, allowed) in [
            ("running", old_writer, true),
            ("starting", old_writer, false),
            ("running", std::process::id(), false),
        ] {
            fs::write(
                dir.path().join("gateway_state.json"),
                serde_json::to_vec(&serde_json::json!({
                    "pid":std::process::id(),"gateway_state":gateway_state,
                    "platforms":{"qqbot":{"state":"disconnected","writer_pid":writer}}
                }))?,
            )?;
            assert_eq!(config.ensure_hermes_stopped("123").is_ok(), allowed);
        }
        Ok(())
    }
}
