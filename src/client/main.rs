use std::env;

use nkv::flag_parser::FlagParser;
use nkv::request_msg::Message;
use nkv::NkvClient;
use std::time::Instant;

use nkv::request_msg::ServerResponse;
use nkv::trie::Trie;

use rustyline_async::SharedWriter;
use rustyline_async::{Readline, ReadlineEvent};
use std::io::Write;

use serde_json as json;

const DEFAULT_URL: &str = "/tmp/nkv/nkv.sock";
const HELP_MESSAGE: &str = "nkv-client [OPTIONS]
Run the notify key-value (nkv) client.

OPTIONS:
  --addr <path-to-socket>
      Specify the address of the server to connect to.
      Supports UNIX socket paths.

  --help
      Display this help message and exit.

  --version
      Print nkv-client version and exit.";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> tokio::io::Result<()> {
    let allowed_flags = vec!["help".to_string(), "addr".to_string()];
    let args: Vec<String> = env::args().collect();
    let flags = match FlagParser::new(args, Some(allowed_flags)) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            println!("nkv-client, version: {}", env!("CARGO_PKG_VERSION"));
            println!("{}", HELP_MESSAGE);
            return Ok(());
        }
    };

    if flags.get("help").is_some() {
        println!("nkv-client, version: {}", env!("CARGO_PKG_VERSION"));
        println!("{}", HELP_MESSAGE);
        return Ok(());
    }

    if flags.get("version").is_some() {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let sock_path = match flags.get("addr") {
        Some(&Some(ref val)) => val.clone(),
        _ => DEFAULT_URL.to_string(),
    };

    let mut client = NkvClient::new(&sock_path, "".to_string());
    let client_version = env!("CARGO_PKG_VERSION");

    match client.version().await? {
        ServerResponse::Version(server_version) => {
            if server_version.data != client_version {
                println!(
                    "Version missmatch! Server {}, client: {}",
                    server_version.data, client_version
                );
            }
        }
        _ => {
            println!("WARN: wrong response to version")
        }
    }

    let (mut rl, mut stdout) = Readline::new("\x1b[1;31m>>> \x1b[0m".into()).unwrap();

    rl.should_print_line_on(false, false);

    loop {
        tokio::select! {
            cmd = rl.readline() => match cmd {
                Ok(ReadlineEvent::Line(line)) => {
                    rl.add_history_entry(line.clone());
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if handle_command(parts, &mut client, &mut stdout).await {
                        break;
                    }
                }
                Ok(ReadlineEvent::Eof) => {
                    writeln!(stdout, "<EOF>")?;
                    break;
                }
                Ok(ReadlineEvent::Interrupted) => {
                    // writeln!(stdout, "^C")?;
                    continue;
                }
                Err(e) => {
                    writeln!(stdout, "Error: {e:?}")?;
                    break;
                }
            }
        }
    }
    rl.flush().unwrap();
    Ok(())
}

async fn handle_command(
    parts: Vec<&str>,
    client: &mut NkvClient,
    stdout: &mut SharedWriter,
) -> bool {
    let sw = stdout.clone();
    let print_update = Box::new(move |value: Message| {
        let _ = writeln!(&mut sw.clone(), "Received update:\n{:?}", value);
    });
    if let Some(command) = parts.get(0) {
        match &*command.to_lowercase() {
            "put" => {
                if let (Some(_key), Some(_value)) = (parts.get(1), parts.get(2)) {
                    let byte_slice: &[u8] = _value.as_bytes();
                    let boxed_bytes: Box<[u8]> = byte_slice.into();

                    let start = Instant::now();
                    let resp = client.put(_key.to_string(), boxed_bytes).await.unwrap();
                    let elapsed = start.elapsed();
                    let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
                } else {
                    let _ = writeln!(stdout, "PUT requires a key and a value");
                }
            }
            "get" => {
                if let Some(_key) = parts.get(1) {
                    let start = Instant::now();
                    let resp = client.get(_key.to_string()).await.unwrap();
                    let elapsed = start.elapsed();
                    let _ = writeln!(stdout, "Request took: {:.2?}", elapsed);
                    pretty_print_server_response(resp, stdout);
                } else {
                    let _ = writeln!(stdout, "GET requires a key");
                }
            }
            "trace" => {
                if let Some(_key) = parts.get(1) {
                    let start = Instant::now();
                    let resp = client.trace(_key.to_string()).await.unwrap();
                    let elapsed = start.elapsed();
                    let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
                } else {
                    let _ = writeln!(stdout, "TRACE requires a key");
                }
            }
            "tree" => {
                if let Some(_key) = parts.get(1) {
                    let start = Instant::now();
                    let resp = client.get(_key.to_string()).await.unwrap();
                    let elapsed = start.elapsed();
                    match resp {
                        ServerResponse::Base(resp) => {
                            let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
                        }
                        ServerResponse::Data(resp) => {
                            let mut trie = Trie::new();
                            for (key, val) in resp.data.iter() {
                                trie.insert(key, val);
                            }
                            let _ = writeln!(
                                stdout,
                                "Request took: {:.2?}\n{:?}\n{}",
                                elapsed, resp.base, trie
                            );
                        }
                        _ => {
                            let _ = writeln!(stdout, "Wrong response!");
                        }
                    };
                } else {
                    let _ = writeln!(stdout, "TREE requires a key");
                }
            }
            "delete" => {
                if let Some(_key) = parts.get(1) {
                    let start = Instant::now();
                    let resp = client.delete(_key.to_string()).await.unwrap();
                    let elapsed = start.elapsed();
                    let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
                } else {
                    let _ = writeln!(stdout, "DELETE requires a key");
                }
            }
            "subscribe" => {
                if let Some(_key) = parts.get(1) {
                    let start = Instant::now();
                    let resp = client
                        .subscribe(_key.to_string(), print_update.clone())
                        .await
                        .unwrap();
                    let elapsed = start.elapsed();
                    let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
                } else {
                    let _ = writeln!(stdout, "SUBSCRIBE requires a key");
                }
            }
            "unsubscribe" => {
                if let Some(key) = parts.get(1) {
                    let start = Instant::now();
                    let resp = client.unsubscribe(key.to_string()).await.unwrap();
                    let elapsed = start.elapsed();
                    let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
                } else {
                    let _ = writeln!(stdout, "SUBSCRIBE requires a key");
                }
            }
            "health" => {
                let start = Instant::now();
                let resp = client.health().await.unwrap();
                let elapsed = start.elapsed();
                let _ = writeln!(stdout, "Request took: {:.2?}\n{:?}", elapsed, resp);
            }
            "quit" => {
                return true;
            }
            "help" => {
                let _ = writeln!(stdout, "Commands:");
                let _ = writeln!(stdout, "PUT key value");
                let _ = writeln!(stdout, "GET key");
                let _ = writeln!(stdout, "TREE key");
                let _ = writeln!(stdout, "DELETE key");
                let _ = writeln!(stdout, "HELP");
                let _ = writeln!(stdout, "SUBSCRIBE key");
                let _ = writeln!(stdout, "QUIT");
            }
            &_ => {
                let _ = writeln!(stdout, "Unknown command");
            }
        }
    }
    return false;
}

fn pretty_print_server_response(d: ServerResponse, stdout: &mut SharedWriter) {
    // use stdout do directly send string, without keeping them in memory
    match d {
        ServerResponse::Base(_) | ServerResponse::Trace(_) | ServerResponse::Version(_) => {
            let _ = writeln!(stdout, "{}", d.to_string());
        }
        // only Data type from Get can contain JSON
        ServerResponse::Data(resp) => {
            let _ = writeln!(stdout, "{}", resp.base);
            for (k, v) in resp.data {
                let printable_string = match String::from_utf8(v.clone()) {
                    Ok(s) => s,
                    Err(_) => format!("{:?}", v),
                };
                let data_str = match json::from_str::<json::Value>(&printable_string) {
                    Ok(val) => json::to_string_pretty(&val).unwrap_or_else(|_| printable_string),
                    Err(_) => printable_string,
                };
                let _ = writeln!(stdout, "{} {}\n", k, data_str);
            }
        }
    }
}
