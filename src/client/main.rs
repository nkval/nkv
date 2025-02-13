use std::env;
use std::io::{self, Write};

use nkv::flag_parser::FlagParser;
use nkv::request_msg::Message;
use nkv::NkvClient;
use std::time::Instant;

const DEFAULT_URL: &str = "/tmp/nkv/nkv.sock";
const HELP_MESSAGE: &str = "nkv-client [--sock path-to-socket] [--help]\
    run notify key value client\
    --sock - specify where to create UNIX socket to listen to connections to\
    --help - display this message";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let allowed_flags = vec!["help".to_string(), "sock".to_string()];
    let args: Vec<String> = env::args().collect();
    let flags = match FlagParser::new(args, Some(allowed_flags)) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            println!("{}", HELP_MESSAGE);
            return;
        }
    };

    if flags.get("help").is_some() {
        println!("{}", HELP_MESSAGE);
        return;
    }

    let sock_path = match flags.get("sock") {
        Some(&Some(ref val)) => val.clone(),
        _ => DEFAULT_URL.to_string(),
    };

    let mut client = NkvClient::new(&sock_path);

    println!("Please enter the command words separated by whitespace, finish with a character return. Enter HELP for help:");
    loop {
        let mut input = String::new();

        // Prompt the user for input
        io::stdout().flush().unwrap(); // Ensure the prompt is shown immediately

        // Read the input from the user
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        // Trim the input to remove any trailing newline characters
        let input = input.trim();

        // Split the input on whitespace
        let parts: Vec<&str> = input.split_whitespace().collect();

        let print_update = Box::new(move |value: Message| {
            println!("Recieved update:\n{}", value);
        });

        if let Some(command) = parts.get(0) {
            match *command {
                "PUT" => {
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
                "GET" => {
                    if let Some(_key) = parts.get(1) {
                        let start = Instant::now();
                        let resp = client.get(_key.to_string()).await.unwrap();
                        let elapsed = start.elapsed();
                        println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                    } else {
                        println!("GET requires a key");
                    }
                }
                "DELETE" => {
                    if let Some(_key) = parts.get(1) {
                        let start = Instant::now();
                        let resp = client.delete(_key.to_string()).await.unwrap();
                        let elapsed = start.elapsed();
                        println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                    } else {
                        println!("DELETE requires a key");
                    }
                }
                "SUBSCRIBE" => {
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
                "UNSUBSCRIBE" => {
                    if let Some(key) = parts.get(1) {
                        let start = Instant::now();
                        let resp = client.unsubscribe(key.to_string()).await.unwrap();
                        let elapsed = start.elapsed();
                        println!("Request took: {:.2?}\n{:?}", elapsed, resp);
                    } else {
                        println!("SUBSCRIBE requires a key");
                    }
                }
                "QUIT" => {
                    break;
                }
                "HELP" => {
                    println!("Commands:");
                    println!("PUT key value");
                    println!("GET key");
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
}
