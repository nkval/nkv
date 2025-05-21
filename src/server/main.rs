use dirs_next as dirs;
use nkv::flag_parser::FlagParser;
use nkv::srv;
use std::str::FromStr;
use std::{env, fs, path::PathBuf};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

use tracing::{error, info, Level};

const DEFAULT_URL: &str = "/tmp/nkv/nkv.sock";
const DEFAULT_LOG_DIR: &str = "logs";

const HELP_MESSAGE: &str = "nkv-server [OPTIONS]
Run the notify key-value (nkv) server.

OPTIONS:
  --dir <path-to-store-data>
      Specify the directory where data files will be stored.
      If not provided, it defaults to the user’s data directory under the `nkv` folder.

  --addr <path-to-socket>
      Define the address to listen for connections.
      Supports UNIX socket path.

  --logs <path-to-logs>
      Specify the directory where logs should be stored.
      Defaults to `./log` if not provided.

  --level <info|debug|trace>
      Set the logging level:
        - `info`  (default): Standard operational messages.
        - `debug` : More detailed debugging information.
        - `trace` : Maximum level of detailed logs.

  --help
      Display this help message and exit.

  --version
      Display nkv-server version and exit.";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let allowed_flags = vec![
        "dir".to_string(),
        "help".to_string(),
        "addr".to_string(),
        "logs".to_string(),
        "level".to_string(),
    ];
    let args: Vec<String> = env::args().collect();
    let flags = match FlagParser::new(args, Some(allowed_flags)) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            println!("nkv-server, version: {}", env!("CARGO_PKG_VERSION"));
            println!("{}", HELP_MESSAGE);
            return;
        }
    };

    if flags.get("version").is_some() {
        println!(env!("CARGO_PKG_VERSION"));
        return;
    }

    // and_then flattens Option<&Option<String>> to Option<String>
    // we only care if this particular flag is specified with a value
    // note that if user will specify --logs without value it'll be
    // defaulted to Level::INFO
    let level = match flags.get("level").and_then(|opt| opt.clone()) {
        None => Level::INFO,
        Some(lvl_str) => match Level::from_str(&lvl_str) {
            Ok(l) => l,
            Err(err) => {
                println!("error parsing log level: {}", err);
                println!("nkv-server, version: {}", env!("CARGO_PKG_VERSION"));
                println!("{}", HELP_MESSAGE);
                return;
            }
        },
    };

    let _guard = init_logging(level.clone(), flags.get("logs").and_then(|o| o.clone()));

    if flags.get("help").is_some() {
        println!("nkv-server, version: {}", env!("CARGO_PKG_VERSION"));
        println!("{}", HELP_MESSAGE);
        return;
    }

    let sock_path = match flags.get("addr") {
        Some(&Some(ref val)) => val.clone(),
        _ => DEFAULT_URL.to_string(),
    };

    if fs::metadata(&sock_path).is_ok() {
        fs::remove_file(&sock_path).expect("Failed to remove old socket");
    }

    let dir = match flags.get("dir") {
        Some(&Some(ref val)) => {
            fs::create_dir_all(&val).expect(&format!("Failed to create directory {}", &val));
            PathBuf::from(val)
        }
        _ => {
            let default_dir = dirs::data_dir().unwrap().join("nkv");
            if let Err(e) = fs::create_dir_all(&default_dir) {
                error!(
                    "failed to create default dir {}: {}",
                    default_dir.display(),
                    e
                );
                return;
            }
            default_dir
        }
    };

    info!("state will be saved to: {}", dir.display());

    // creates a task where it waits to serve threads
    let (mut srv, _cancel) = srv::Server::new(sock_path.to_string(), dir).await.unwrap();

    srv.serve().await;
}

fn init_logging(
    filter: tracing::Level,
    path: Option<String>,
) -> tracing_appender::non_blocking::WorkerGuard {
    let log_filter = EnvFilter::default().add_directive(filter.into());

    let log_dir = match path {
        Some(p) => p,
        None => DEFAULT_LOG_DIR.to_string(),
    };

    let file_appender = tracing_appender::rolling::daily(&log_dir, "nkv-server.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = fmt::Layer::new()
        .with_writer(file_writer)
        .with_ansi(false)
        .compact();

    let subscriber = Registry::default()
        .with(fmt::layer().with_filter(log_filter)) // this also adds stdout logging
        .with(file_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    info!(
        "log level is {} logs will be saved to: {}",
        filter, &log_dir
    );

    guard
}
