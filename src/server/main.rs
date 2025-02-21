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

const HELP_MESSAGE: &str = "nkv-server [--dir path-to-store-data] [--sock path-to-socket] [--logs path-to-logs] [--level info|debug|trace] [--help]
    run notify key value server
    --dir - specify where to store files, if not specified it will create temprorary directory
    --sock - specify where to create UNIX socket to listen to connections to
    --logs - specify folder where to store logs, if not specified will use ./log
    --level - speficy log level: info, debug or trace from smallest to maximum amount of information. info if not specified
    --help - display this message";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let allowed_flags = vec![
        "dir".to_string(),
        "help".to_string(),
        "sock".to_string(),
        "logs".to_string(),
        "level".to_string(),
    ];
    let args: Vec<String> = env::args().collect();
    let flags = match FlagParser::new(args, Some(allowed_flags)) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            println!("{}", HELP_MESSAGE);
            return;
        }
    };

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
                println!("{}", HELP_MESSAGE);
                return;
            }
        },
    };

    let _guard = init_logging(level.clone(), flags.get("logs").and_then(|o| o.clone()));

    if flags.get("help").is_some() {
        println!("{}", HELP_MESSAGE);
        return;
    }

    let sock_path = match flags.get("sock") {
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
