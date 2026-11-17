use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use pi_channel_core::systemd::ServiceSpec;
use pi_runtime::{
    config::{Config, default_home, private_write, token},
    daemon,
    schedule::Timing,
    store::id,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Parser)]
#[command(
    version,
    about = "Small persistent Pi agent runtime: messages, heartbeat and scheduled tasks"
)]
struct Cli {
    #[arg(long, env = "PI_RUNTIME_HOME", default_value_os_t = default_home(), global = true)]
    home: PathBuf,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Init {
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long)]
        pi: Option<PathBuf>,
        #[arg(long, env = "PI_PROVIDER", requires = "model")]
        provider: Option<String>,
        #[arg(long, env = "PI_MODEL", requires = "provider")]
        model: Option<String>,
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },
    Serve,
    Status,
    Send {
        message: String,
        #[arg(long, default_value = "main")]
        channel: String,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        no_wait: bool,
        #[arg(long, default_value_t = 1800)]
        wait_seconds: u64,
    },
    Tasks,
    Task {
        id: String,
    },
    Cancel {
        id: String,
    },
    Jobs,
    #[command(group(clap::ArgGroup::new("timing").required(true).args(["every", "at", "cron"])))]
    AddJob {
        #[arg(long)]
        name: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        every: Option<u64>,
        #[arg(long)]
        at: Option<String>,
        #[arg(long)]
        cron: Option<String>,
        #[arg(long, default_value = "UTC")]
        timezone: String,
    },
    DeleteJob {
        id: String,
    },
    InstallService,
}

#[tokio::main]
async fn main() -> Result<()> {
    pi_channel_core::init_logging();
    let cli = Cli::parse();
    if let Commands::Init {
        cwd,
        pi,
        provider,
        model,
        port,
    } = cli.command
    {
        let cwd = cwd.unwrap_or(std::env::current_dir()?).canonicalize()?;
        let pi = pi
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".pi/agent/bin/pi")
            })
            .canonicalize()?;
        let cfg = Config {
            pi,
            cwd,
            port,
            provider,
            model,
            thinking: "medium".into(),
            tools: "read,bash,edit,write,codemode".into(),
            heartbeat_seconds: 1800,
            task_timeout_seconds: 900,
        };
        cfg.validate()?;
        ensure!(
            !cli.home.join("config.json").exists(),
            "Already initialized: {}",
            cli.home.display()
        );
        fs::create_dir_all(&cli.home)?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&cli.home, fs::Permissions::from_mode(0o700))?;
        private_write(
            &cli.home.join("config.json"),
            &serde_json::to_vec_pretty(&cfg)?,
        )?;
        private_write(
            &cli.home.join("token"),
            format!("{}{}\n", id(), id()).as_bytes(),
        )?;
        private_write(&cli.home.join("HEARTBEAT.md"), b"# Heartbeat\n\n<!-- Empty/comment-only files do not call the model.\nAdd an explicit checklist below to enable periodic work.\nCompleted '- [x]' items are ignored.\nExample: - [ ] Check the specified project for failing tests and report only; do not change files.\n-->\n")?;
        Config::load(&cli.home)?;
        println!("Initialized {}", cli.home.display());
        return Ok(());
    }
    let home = cli
        .home
        .canonicalize()
        .context("Initialize first: pi-runtime init")?;
    if matches!(cli.command, Commands::Serve) {
        return daemon::serve(home).await;
    }
    let config = Config::load(&home)?;
    if matches!(cli.command, Commands::InstallService) {
        return install_service(&home);
    }
    let client = Client {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?,
        url: format!("http://127.0.0.1:{}", config.port),
        token: token(&home)?,
    };
    match cli.command {
        Commands::Status => print_json(client.call("GET", "/v1/status", None).await?),
        Commands::Tasks => print_json(client.call("GET", "/v1/tasks", None).await?),
        Commands::Task { id } => print_json(
            client
                .call("GET", &format!("/v1/tasks/{}", safe_id(&id)?), None)
                .await?,
        ),
        Commands::Cancel { id } => print_json(
            client
                .call("DELETE", &format!("/v1/tasks/{}", safe_id(&id)?), None)
                .await?,
        ),
        Commands::Jobs => print_json(client.call("GET", "/v1/jobs", None).await?),
        Commands::DeleteJob { id } => print_json(
            client
                .call("DELETE", &format!("/v1/jobs/{}", safe_id(&id)?), None)
                .await?,
        ),
        Commands::AddJob {
            name,
            message,
            every,
            at,
            cron,
            timezone,
        } => {
            let timing = if let Some(seconds) = every {
                Timing::Every { seconds }
            } else if let Some(at) = at {
                Timing::At {
                    at: at
                        .parse()
                        .context("Use RFC3339, e.g. 2027-01-01T09:00:00+08:00")?,
                }
            } else {
                Timing::Cron {
                    expression: cron.context("Missing schedule")?,
                    timezone,
                }
            };
            print_json(
                client
                    .call(
                        "POST",
                        "/v1/jobs",
                        Some(json!({"name":name,"message":message,"timing":timing})),
                    )
                    .await?,
            );
        }
        Commands::Send {
            message,
            channel,
            key,
            no_wait,
            wait_seconds,
        } => {
            let created = client
                .call(
                    "POST",
                    "/v1/messages",
                    Some(json!({"message":message,"channel":channel,"idempotency_key":key})),
                )
                .await?;
            let id = created["task_id"].as_str().context("No task_id returned")?;
            if no_wait {
                print_json(created);
            } else {
                eprintln!("Task: {id}");
                let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);
                loop {
                    let task = client.call("GET", &format!("/v1/tasks/{id}"), None).await?;
                    match task["status"].as_str() {
                        Some("succeeded") => {
                            println!("{}", task["result"].as_str().unwrap_or(""));
                            break;
                        }
                        Some("failed" | "interrupted" | "cancelled") => {
                            bail!("Task {}: {}", task["status"], task["error"])
                        }
                        _ => {}
                    }
                    ensure!(
                        tokio::time::Instant::now() < deadline,
                        "Wait deadline exceeded; task is NOT cancelled. Inspect with: pi-runtime task {id}"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

struct Client {
    http: reqwest::Client,
    url: String,
    token: String,
}
impl Client {
    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self
            .http
            .request(method.parse()?, format!("{}{path}", self.url))
            .bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .context("Runtime unreachable; start pi-runtime serve or its systemd service")?;
        let status = response.status();
        let data = response.text().await?;
        ensure!(status.is_success(), "HTTP {status}: {data}");
        Ok(serde_json::from_str(&data)?)
    }
}
fn print_json(data: Value) {
    println!("{}", serde_json::to_string_pretty(&data).unwrap());
}
fn safe_id(id: &str) -> Result<&str> {
    ensure!(
        id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
        "Invalid task/job ID"
    );
    Ok(id)
}
fn install_service(home: &Path) -> Result<()> {
    let user_home = PathBuf::from(std::env::var_os("HOME").context("HOME missing")?);
    let spec = ServiceSpec {
        name: "pi-runtime",
        description: "Pi lightweight agent runtime",
        after: &["network-online.target"],
        wants: &["network-online.target"],
        stop_timeout_seconds: 20,
        prevent_restart_exit_status: None,
        environment: vec![
            format!(
                "PATH={}/.pi/agent/bin:/usr/local/bin:/usr/bin:/bin",
                user_home.display()
            ),
            "RUST_LOG=info".into(),
        ],
    };
    let path = spec.install(home, true)?;
    println!("Installed and started {}", path.display());
    Ok(())
}
