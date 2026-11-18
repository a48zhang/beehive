use crate::{
    config::Config,
    store::{Store, Task, id},
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{os::unix::process::CommandExt, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
};

pub struct Rpc {
    child: Child,
    stdin: Option<ChildStdin>,
    events: mpsc::Receiver<Result<Value>>,
    reader: JoinHandle<()>,
    channel: Option<String>,
    healthy: bool,
}

impl Rpc {
    pub async fn start(config: &Config, home: &Path) -> Result<Self> {
        let mut command = Command::new(&config.pi);
        command.args(["--mode", "rpc", "--no-approve", "--no-extensions", "--extension", "builtin:mcp", "--extension", "builtin:codemode", "--no-prompt-templates", "--no-context-files"])
            .arg("--session-dir").arg(home.join("sessions"))
            .arg("--tools").arg(&config.tools)
            .arg("--thinking").arg(&config.thinking)
            .arg("--append-system-prompt").arg(format!(
                "You are running as an unattended local Pi runtime. Execute only the explicit task. Do not invent recurring work. Heartbeat checklists are in {}. For heartbeat requests, return HEARTBEAT_OK if nothing needs attention. Do not perform destructive actions or external sends without explicit user authorization. Never expose credentials in results.", home.join("HEARTBEAT.md").display()))
            .current_dir(&config.cwd).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true);
        if let (Some(provider), Some(model)) = (&config.provider, &config.model) {
            command
                .arg("--provider")
                .arg(provider)
                .arg("--model")
                .arg(model);
        }
        // A separate process group permits terminating tool subprocesses on timeout.
        command.as_std_mut().process_group(0);
        // Do not confuse the new child with this interactive Pi session.
        for name in [
            "PI_SESSION_FILE",
            "PI_SESSION_ID",
            "PI_CODING_AGENT_SESSION_DIR",
        ] {
            command.env_remove(name);
        }
        let mut child = command.spawn().context("Spawn Pi RPC")?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().context("Pi stdout")?;
        let (tx, events) = mpsc::channel(128);
        let reader = tokio::spawn(async move {
            let mut input = BufReader::new(stdout);
            loop {
                let mut bytes = Vec::new();
                match input.read_until(b'\n', &mut bytes).await {
                    Ok(0) => {
                        let _ = tx.send(Err(anyhow::anyhow!("Pi RPC stdout closed"))).await;
                        break;
                    }
                    Ok(_) => {
                        if bytes.len() > 16 * 1024 * 1024 {
                            let _ = tx
                                .send(Err(anyhow::anyhow!("Pi RPC record exceeds 16 MiB")))
                                .await;
                            break;
                        }
                        let event = serde_json::from_slice::<Value>(&bytes);
                        match event {
                            Ok(event) => {
                                let kind = event["type"].as_str().unwrap_or("");
                                let relevant = matches!(
                                    kind,
                                    "response"
                                        | "agent_settled"
                                        | "extension_ui_request"
                                        | "auto_retry_end"
                                ) || (kind == "message_end"
                                    && event["message"]["role"] == "assistant");
                                if relevant && tx.send(Ok(event)).await.is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = tx.send(Err(error.into())).await;
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error.into())).await;
                        break;
                    }
                }
            }
        });
        let mut rpc = Self {
            child,
            stdin,
            events,
            reader,
            channel: None,
            healthy: true,
        };
        if let Err(error) =
            tokio::time::timeout(Duration::from_secs(30), rpc.request("get_state", json!({})))
                .await
                .context("Pi startup timeout")
                .and_then(|r| r)
        {
            rpc.stop().await;
            return Err(error);
        }
        Ok(rpc)
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy
    }

    async fn send(&mut self, record: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        let stdin = self.stdin.as_mut().context("Pi stdin closed")?;
        stdin.write_all(&bytes).await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Value> {
        self.events
            .recv()
            .await
            .context("Pi RPC event reader stopped")?
    }

    async fn deny_dialog(&mut self, event: &Value) -> Result<bool> {
        if event["type"] == "extension_ui_request"
            && matches!(
                event["method"].as_str(),
                Some("select" | "confirm" | "input" | "editor")
            )
        {
            self.send(&json!({"type":"extension_ui_response", "id":event["id"], "cancelled":true}))
                .await?;
            tracing::warn!(method = ?event["method"], "Unattended UI dialog cancelled");
            return Ok(true);
        }
        Ok(false)
    }

    async fn request(&mut self, kind: &str, fields: Value) -> Result<Value> {
        let request_id = id();
        let mut record = fields;
        record["type"] = json!(kind);
        record["id"] = json!(request_id);
        self.send(&record).await?;
        loop {
            let event = self.receive().await?;
            self.deny_dialog(&event).await?;
            if event["type"] == "response" && event["id"] == request_id {
                ensure!(
                    event["success"] == true,
                    "Pi {} failed: {}",
                    kind,
                    event["error"]
                );
                return Ok(event["data"].clone());
            }
        }
    }

    pub async fn ping(&mut self) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.request("get_state", json!({})),
        )
        .await
        .context("Pi health timeout")??;
        Ok(())
    }

    async fn select_channel(&mut self, channel: &str, store: &Store) -> Result<()> {
        if self.channel.as_deref() == Some(channel) {
            return Ok(());
        }
        if let Some(path) = store.session(channel)? {
            let response = self
                .request("switch_session", json!({"sessionPath": path}))
                .await?;
            ensure!(response["cancelled"] != true, "Session switch cancelled");
        } else {
            let response = self.request("new_session", json!({})).await?;
            ensure!(response["cancelled"] != true, "New session cancelled");
            let state = self.request("get_state", json!({})).await?;
            store.save_session(
                channel,
                state["sessionFile"]
                    .as_str()
                    .context("Pi returned no session file")?,
            )?;
        }
        self.channel = Some(channel.to_string());
        Ok(())
    }

    pub async fn run(&mut self, task: &Task, store: &Store) -> Result<String> {
        self.healthy = false;
        self.select_channel(&task.channel, store).await?;
        let request_id = id();
        self.send(&json!({"type":"prompt", "id":request_id, "message":task.message}))
            .await?;
        let mut accepted = false;
        let mut settled = false;
        let mut aborted = false;
        let mut last_message = Value::Null;
        let mut blocked = false;
        let mut retry_error = None;
        loop {
            let event = self.receive().await?;
            blocked |= self.deny_dialog(&event).await?;
            match event["type"].as_str() {
                Some("response") if event["id"] == request_id => {
                    ensure!(
                        event["success"] == true,
                        "Prompt rejected: {}",
                        event["error"]
                    );
                    if event["data"]["disposition"] == "handled" {
                        self.healthy = true;
                        return Ok("Prompt handled without starting an agent run.".into());
                    }
                    accepted = true;
                }
                Some("message_end") => {
                    last_message = event["message"].clone();
                }
                Some("agent_settled") => {
                    settled = true;
                    aborted = event["aborted"] == true;
                }
                Some("auto_retry_end") if event["success"] == false => {
                    retry_error = Some(event["finalError"].to_string());
                }
                _ => {}
            }
            if accepted && settled {
                ensure!(!aborted, "Agent run aborted");
                self.healthy = true;
                ensure!(
                    !blocked,
                    "Agent requested interactive approval; denied in unattended mode"
                );
                if let Some(error) = retry_error {
                    bail!("Pi retries exhausted: {error}");
                }
                ensure!(
                    !matches!(
                        last_message["stopReason"].as_str(),
                        Some("error" | "aborted" | "length")
                    ),
                    "Pi incomplete or failed: {}",
                    last_message["errorMessage"]
                );
                let result = last_message["content"]
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter(|block| block["type"] == "text")
                            .filter_map(|block| block["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();
                // Bound SQLite result size while preserving valid UTF-8.
                let mut result = result;
                if result.len() > 256 * 1024 {
                    let mut end = 256 * 1024;
                    while !result.is_char_boundary(end) {
                        end -= 1;
                    }
                    result.truncate(end);
                    result.push_str("\n[Result truncated; full transcript in Pi session]");
                }
                return Ok(result);
            }
        }
    }

    pub async fn stop(&mut self) {
        self.stdin.take(); // EOF requests graceful Pi shutdown.
        let pid = self.child.id();
        if tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .is_err()
        {
            if let Some(pid) = pid {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGTERM);
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // Also remove orphaned tool children in this process group.
        if let Some(pid) = pid {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = self.child.wait().await;
        self.reader.abort();
    }
}
