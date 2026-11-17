use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub pi: PathBuf,
    pub cwd: PathBuf,
    pub port: u16,
    pub heartbeat_seconds: u64,
    pub task_timeout_seconds: u64,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub thinking: String,
    pub tools: String,
}

impl Config {
    pub fn load(home: &Path) -> Result<Self> {
        let config: Self = serde_json::from_slice(&fs::read(home.join("config.json"))?)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let config = self;
        ensure!(
            config.pi.is_absolute() && config.pi.is_file(),
            "pi must be an absolute executable path"
        );
        ensure!(
            config.cwd.is_absolute() && config.cwd.is_dir(),
            "cwd must be an existing absolute directory"
        );
        ensure!(config.port > 0, "port must be nonzero");
        ensure!(
            (10..=86400).contains(&config.heartbeat_seconds),
            "heartbeat_seconds must be 10..86400"
        );
        ensure!(
            (10..=86400).contains(&config.task_timeout_seconds),
            "task_timeout_seconds must be 10..86400"
        );
        ensure!(
            config.provider.is_some() == config.model.is_some(),
            "provider and model must be set together"
        );
        ensure!(
            ["off", "minimal", "low", "medium", "high", "xhigh", "max"]
                .contains(&config.thinking.as_str()),
            "invalid thinking level"
        );
        Ok(())
    }
}

pub use pi_channel_core::{
    paths::runtime_home as default_home,
    secrets::{private_write, token},
};
