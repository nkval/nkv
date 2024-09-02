use std::env;
use std::io::{self, Write};

use nkv::request_msg;
use nkv::NkvClient;

const DEFAULT_URL: &str = "localhost:4222";

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let url = if args.len() > 1 {
        &args[1]
    } else {
        DEFAULT_URL
    };
    let mut client = NkvClient::new(&url);

    loop {
        let mut input = String::new();

        // Prompt the user for input
        println!("Please enter the command words separated by whitespace, finish with a character return. Enter HELP for help:");
        io::stdout().flush().unwrap(); // Ensure the prompt is shown immediately

        // Read the input from the user
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        // Trim the input to remove any trailing newline characters
        let input = input.trim();

        // Split the input on whitespace
        let parts: Vec<&str> = input.split_whitespace().collect();

        if let Some(command) = parts.get(0) {
            match *command {
                "PUT" => {
                    if let (Some(_key), Some(_value)) = (parts.get(1), parts.get(2)) {
                        let byte_slice: &[u8] = _value.as_bytes();

                        // Convert the byte slice into a boxed slice
                        let boxed_bytes: Box<[u8]> = byte_slice.into();
                        let resp = client.put(_key.to_string(), boxed_bytes).await.unwrap();
                        assert_eq!(
                            resp,
                            request_msg::ServerResponse::Base(request_msg::BaseResp {
                                id: "0".to_string(),
                                status: http::StatusCode::OK,
                                message: "No Error".to_string(),
                            })
                        )
                    } else {
                        println!("PUT requires a key and a value");
                    }
                }
                "GET" => {
                    if let Some(_key) = parts.get(1) {
                        let resp = client.get(_key.to_string()).await.unwrap();
                        assert_eq!(
                            resp,
                            request_msg::ServerResponse::Base(request_msg::BaseResp {
                                id: "0".to_string(),
                                status: http::StatusCode::OK,
                                message: "No Error".to_string(),
                            })
                        )
                    } else {
                        println!("GET requires a key");
                    }
                }
                "DELETE" => {
                    if let Some(_key) = parts.get(1) {
                        let resp = client.delete(_key.to_string()).await.unwrap();
                        assert_eq!(
                            resp,
                            request_msg::ServerResponse::Base(request_msg::BaseResp {
                                id: "0".to_string(),
                                status: http::StatusCode::OK,
                                message: "No Error".to_string(),
                            })
                        )
                    } else {
                        println!("DELETE requires a key");
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
                    println!("QUIT");
                }
                &_ => {
                    println!("Unknown command");
                }
            }
        }
    }
}
