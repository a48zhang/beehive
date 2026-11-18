use anyhow::{Context, Result, ensure};
use clap::{Parser, Subcommand};
use pi_channel_core::{
    id,
    message_client::MessageClient,
    paths,
    secrets::{private_write, token},
    systemd::ServiceSpec,
};
use pi_qq::{
    config::{Config, Credentials, default_home, import_hermes},
    gateway::Fatal,
    service,
    store::Store,
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Parser)]
#[command(version, about = "Native QQ Bot v2 channel service for pi-runtime")]
struct Cli {
    #[arg(long,env="PI_QQ_HOME",default_value_os_t=default_home(),global=true)]
    home: PathBuf,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Init {
        #[arg(long,default_value_os_t=paths::runtime_home())]
        runtime_home: PathBuf,
        #[arg(long)]
        hermes_home: Option<PathBuf>,
        #[arg(long, default_value_t = 8788)]
        health_port: u16,
    },
    Serve {
        #[cfg(feature = "test-peer")]
        #[arg(long, hide = true)]
        test_api_base: Option<String>,
    },
    Check,
    Status,
    Deliveries,
    Retry {
        event_key: String,
        #[arg(long, required = true)]
        allow_duplicate: bool,
    },
    InstallService {
        #[arg(long)]
        start: bool,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    pi_channel_core::init_logging();
    let cli = Cli::parse();
    if let Commands::Init {
        runtime_home,
        hermes_home,
        health_port,
    } = cli.command
    {
        return init(&cli.home, &runtime_home, hermes_home, health_port);
    }
    let home = cli
        .home
        .canonicalize()
        .context("Initialize first: pi-qq init")?;
    if let Commands::Serve { .. } = &cli.command {
        #[cfg(feature = "test-peer")]
        if let Commands::Serve {
            test_api_base: Some(base),
        } = &cli.command
        {
            let config = Config::load(&home)?;
            let api = pi_qq::api::Api::for_test(config.credentials()?, base.clone())?;
            return service::serve_test(home, api).await;
        }
        let result = service::serve(home).await;
        if let Err(error) = &result
            && error.downcast_ref::<Fatal>().is_some()
        {
            eprintln!("{error:#}");
            std::process::exit(78);
        }
        return result;
    }
    let config = Config::load(&home)?;
    match cli.command {
        Commands::Check => {
            let creds = config.credentials()?;
            config.ensure_hermes_stopped(&creds.app_id)?;
            let client =
                MessageClient::new(config.runtime_rpc_url.clone(), &config.runtime_token_file)?;
            let status = client.call("runtime.status", json!({})).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &json!({"configuration":"valid","credentials":"configured (not tested with QQ)","hermes_conflict":false,"allowed_users":config.allowed_users.len(),"allowed_groups":config.allowed_groups.len(),"runtime":status})
                )?
            );
        }
        Commands::Status => {
            let response = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()?
                .get(format!("http://127.0.0.1:{}/v1/status", config.health_port))
                .bearer_auth(token(&home)?)
                .send()
                .await?
                .error_for_status()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response.json::<serde_json::Value>().await?)?
            );
        }
        Commands::Deliveries => {
            let store = Store::open(&home.join("qq.sqlite3"))?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &json!({"counts":store.counts()?,"recent":store.summaries()?})
                )?
            );
        }
        Commands::Retry {
            event_key,
            allow_duplicate,
        } => {
            ensure!(
                allow_duplicate,
                "Explicit duplicate-send acknowledgement required"
            );
            Store::open(&home.join("qq.sqlite3"))?.retry(&event_key)?;
            println!(
                "Requeued delivery; possible duplicate explicitly acknowledged. No new Pi task will be created if already submitted."
            );
        }
        Commands::InstallService { start } => install_service(&home, &config, start)?,
        _ => unreachable!(),
    }
    Ok(())
}

fn init(
    home: &Path,
    runtime_home: &Path,
    hermes_home: Option<PathBuf>,
    health_port: u16,
) -> Result<()> {
    ensure!(
        !home.join("config.json").exists(),
        "QQ service already initialized"
    );
    let runtime_home = runtime_home.canonicalize()?;
    let runtime_rpc_url = paths::runtime_rpc_url(&runtime_home)?;
    let home = if home.is_absolute() {
        home.to_path_buf()
    } else {
        std::env::current_dir()?.join(home)
    };
    let hermes_home = hermes_home.map(|home| home.canonicalize()).transpose()?;
    let (credentials, users) = if let Some(hermes) = &hermes_home {
        import_hermes(hermes)?
    } else {
        (
            Credentials {
                app_id: String::new(),
                client_secret: String::new(),
            },
            vec![],
        )
    };
    let config = Config {
        runtime_rpc_url,
        runtime_token_file: runtime_home.join("token"),
        credentials_file: home.join("credentials.json"),
        health_port,
        allowed_users: users,
        allowed_groups: vec![],
        poll_interval_ms: 1000,
        hermes_home,
    };
    config.validate()?;
    fs::create_dir_all(&home)?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700))?;
    private_write(
        &home.join("credentials.json"),
        &serde_json::to_vec_pretty(&credentials)?,
    )?;
    private_write(
        &home.join("token"),
        format!("{}{}\n", id(), id()).as_bytes(),
    )?;
    private_write(
        &home.join("config.json"),
        &serde_json::to_vec_pretty(&config)?,
    )?;
    println!(
        "Initialized {}. Imported {} exact user allowlist entries. Groups disabled. Service NOT started.",
        home.display(),
        config.allowed_users.len()
    );
    if credentials.app_id.is_empty() {
        println!("Fill credentials.json and allowed_users before starting.");
    }
    Ok(())
}
fn install_service(home: &Path, config: &Config, start: bool) -> Result<()> {
    if start {
        config.ensure_hermes_stopped(&config.credentials()?.app_id)?;
    }
    let spec = ServiceSpec {
        name: "pi-qq",
        description: "Native QQ channel for Pi runtime",
        after: &["network-online.target", "pi-runtime.service"],
        wants: &["network-online.target", "pi-runtime.service"],
        stop_timeout_seconds: 15,
        prevent_restart_exit_status: Some(78),
        environment: vec!["RUST_LOG=info".into()],
    };
    let path = spec.install(home, start)?;
    println!(
        "Installed {}. {}",
        path.display(),
        if start {
            "Started and enabled."
        } else {
            "NOT started or enabled; cut over Hermes before enabling."
        }
    );
    Ok(())
}
