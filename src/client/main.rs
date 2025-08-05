use std::env;

use nkv::flag_parser::FlagParser;
use nkv::request_msg::Message;
use nkv::NkvClient;
use std::time::Instant;

use nkv::request_msg::ServerResponse;
use nkv::trie::Trie;

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

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
async fn main() -> Result<()> {
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

    let mut client = NkvClient::new(&sock_path, "nkv-client".to_string());

    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history!");
    }

    println!("Please enter the command words separated by whitespace, finish with a character return. Enter HELP for help:");
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(input) => {
                rl.add_history_entry(input.as_str())?;
                let input = input.trim();

                // Split the input on whitespace
                let parts: Vec<&str> = input.split_whitespace().collect();

                let print_update = Box::new(move |value: Message| {
                    println!("Recieved update:\n{}", value);
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
                                println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                            } else {
                                println!("PUT requires a key and a value");
                            }
                        }
                        "get" => {
                            if let Some(_key) = parts.get(1) {
                                let start = Instant::now();
                                let resp = client.get(_key.to_string()).await.unwrap();
                                let elapsed = start.elapsed();
                                println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                            } else {
                                println!("GET requires a key");
                            }
                        }
                        "trace" => {
                            if let Some(_key) = parts.get(1) {
                                let start = Instant::now();
                                let resp = client.trace(_key.to_string()).await.unwrap();
                                let elapsed = start.elapsed();
                                println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                            } else {
                                println!("TRACE requires a key");
                            }
                        }
                        "tree" => {
                            if let Some(_key) = parts.get(1) {
                                let start = Instant::now();
                                let resp = client.get(_key.to_string()).await.unwrap();
                                let elapsed = start.elapsed();
                                match resp {
                                    ServerResponse::Base(resp) => {
                                        println!("Request took: {:.2?}\n{:?}", elapsed, resp)
                                    }
                                    ServerResponse::Data(resp) => {
                                        let mut trie = Trie::new();
                                        for (key, val) in resp.data.iter() {
                                            trie.insert(key, val);
                                        }
                                        println!(
                                            "Request took: {:.2?}\n{:?}\n{}",
                                            elapsed, resp.base, trie
                                        )
                                    }
                                    _ => {
                                        println!("Wrong response!")
                                    }
                                };
                            } else {
                                println!("TREE requires a key");
                            }
                        }
                        "delete" => {
                            if let Some(_key) = parts.get(1) {
                                let start = Instant::now();
                                let resp = client.delete(_key.to_string()).await.unwrap();
                                let elapsed = start.elapsed();
                                println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                            } else {
                                println!("DELETE requires a key");
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
                                println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                            } else {
                                println!("SUBSCRIBE requires a key");
                            }
                        }
                        "unsubscribe" => {
                            if let Some(key) = parts.get(1) {
                                let start = Instant::now();
                                let resp = client.unsubscribe(key.to_string()).await.unwrap();
                                let elapsed = start.elapsed();
                                println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                            } else {
                                println!("SUBSCRIBE requires a key");
                            }
                        }
                        "health" => {
                            let start = Instant::now();
                            let resp = client.health().await.unwrap();
                            let elapsed = start.elapsed();
                            println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                        }
                        "quit" => {
                            break;
                        }
                        "help" => {
                            println!("Commands:");
                            println!("PUT key value");
                            println!("GET key");
                            println!("TREE key");
                            println!("DELETE key");
                            println!("HELP");
                            println!("SUBSCRIBE key");
                            println!("QUIT");
                        }
                        &_ => {
                            println!("Unknown command");
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history("history.txt")?;

    Ok(())
}
