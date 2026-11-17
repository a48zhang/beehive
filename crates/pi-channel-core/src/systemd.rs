//! Shared user-unit rendering/installation. Services supply only their own policy.
use crate::secrets::private_write;
use anyhow::{Context, Result, ensure};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub struct ServiceSpec<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub after: &'a [&'a str],
    pub wants: &'a [&'a str],
    pub stop_timeout_seconds: u32,
    pub prevent_restart_exit_status: Option<u8>,
    pub environment: Vec<String>,
}
fn quoted(value: &str, exec: bool) -> Result<String> {
    ensure!(!value.contains(['\n', '\r', '\0']), "Invalid systemd value");
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    Ok(format!(
        "\"{}\"",
        if exec {
            escaped.replace('$', "$$")
        } else {
            escaped
        }
    ))
}
fn exec_path(path: &Path) -> Result<String> {
    ensure!(
        path.is_absolute(),
        "Systemd executable/data paths must be absolute"
    );
    quoted(path.to_str().context("Non-UTF8 systemd path")?, true)
}
impl ServiceSpec<'_> {
    pub fn render(&self, binary: &Path, home: &Path) -> Result<String> {
        ensure!(
            !self.name.is_empty()
                && self
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
            "Invalid service name"
        );
        for value in std::iter::once(&self.description)
            .chain(self.after.iter())
            .chain(self.wants.iter())
        {
            ensure!(!value.contains(['\n', '\r', '\0']), "Invalid unit field");
        }
        let mut unit = format!(
            "# Managed by {} install-service\n[Unit]\nDescription={}\nAfter={}\nWants={}\n\n[Service]\nType=simple\nExecStart={} --home {} serve\nRestart=always\nRestartSec=5\nTimeoutStopSec={}\nKillMode=control-group\nUMask=0077\nNoNewPrivileges=true\n",
            self.name,
            self.description,
            self.after.join(" "),
            self.wants.join(" "),
            exec_path(binary)?,
            exec_path(home)?,
            self.stop_timeout_seconds
        );
        if let Some(code) = self.prevent_restart_exit_status {
            unit.push_str(&format!("RestartPreventExitStatus={code}\n"));
        }
        for value in &self.environment {
            unit.push_str(&format!("Environment={}\n", quoted(value, false)?));
        }
        unit.push_str("\n[Install]\nWantedBy=default.target\n");
        Ok(unit)
    }
    pub fn install(&self, home: &Path, start: bool) -> Result<PathBuf> {
        let binary = std::env::current_exe()?.canonicalize()?;
        let dir = PathBuf::from(std::env::var_os("HOME").context("HOME missing")?)
            .join(".config/systemd/user");
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.service", self.name));
        private_write(&path, self.render(&binary, home)?.as_bytes())
            .context("Unit exists or cannot be created; not overwritten")?;
        ensure!(
            Command::new("systemctl")
                .args(["--user", "daemon-reload"])
                .status()?
                .success(),
            "systemd reload failed"
        );
        if start {
            ensure!(
                Command::new("systemctl")
                    .args([
                        "--user",
                        "enable",
                        "--now",
                        &format!("{}.service", self.name)
                    ])
                    .status()?
                    .success(),
                "systemd enable/start failed"
            );
        }
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unit_policy_and_path_escaping_are_shared() -> Result<()> {
        let spec = ServiceSpec {
            name: "pi-qq",
            description: "QQ",
            after: &["pi-runtime.service"],
            wants: &["pi-runtime.service"],
            stop_timeout_seconds: 15,
            prevent_restart_exit_status: Some(78),
            environment: vec!["RUST_LOG=info".into()],
        };
        let unit = spec.render(
            Path::new("/opt/pi/bin/pi-qq"),
            Path::new("/home/user/a b%$"),
        )?;
        assert!(unit.contains("Restart=always\n"));
        assert!(unit.contains("RestartPreventExitStatus=78\n"));
        assert!(unit.contains("After=pi-runtime.service\n"));
        assert!(unit.contains("\"/home/user/a b%%$$\""));
        assert!(!unit.contains("WorkingDirectory="));
        assert!(unit.contains("Environment=\"RUST_LOG=info\""));
        assert!(
            spec.render(Path::new("relative"), Path::new("/tmp/home"))
                .is_err()
        );
        assert!(
            spec.render(Path::new("/bin/pi"), Path::new("/tmp/bad\nvalue"))
                .is_err()
        );
        Ok(())
    }
}
